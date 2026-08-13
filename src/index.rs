//! The vector index: a flat array, scanned exhaustively.
//!
//! The previous build used qdrant-edge — HNSW, segments, a write-ahead log,
//! payload indexes — and paid 59 MB on disk for 1.2 MB of vectors, plus a beta
//! dependency whose API it had to wrap in a trait to contain.
//!
//! At this scale an approximate nearest-neighbour structure is not just
//! unnecessary, it is slower. 811 chunks × 384 dimensions is 311k
//! multiply-adds — well under a millisecond, no index to build, no index to
//! keep consistent, and the answer is exact. Even a 100k-chunk monorepo is
//! ~38M operations, which is a few milliseconds of scanning that
//! autovectorizes. HNSW would win somewhere past a million chunks; nothing
//! anyone points a personal code-search tool at is within two orders of
//! magnitude of that, and if it ever is, the shape to add is a coarse
//! centroid pre-filter in this file rather than a database.
//!
//! Two representations, for two jobs:
//!
//! - **In memory, int8.** Symmetric per-vector quantization: 388 bytes per
//!   chunk instead of 1536, so a 100k-chunk index is 39 MB resident rather
//!   than 153 MB, and the scan is bound by memory bandwidth so a quarter of
//!   the bytes is most of a quarter of the time.
//! - **On disk, f32.** The exact vector, used to rescore the shortlist the
//!   int8 scan produces.
//!
//! Rescoring is what makes the approximation invisible: the scan is allowed to
//! be slightly wrong about the order of the top few hundred, because the top
//! few hundred are then scored exactly and re-sorted. The result is identical
//! to a full f32 search, which removes a whole class of "why did this rank
//! differently today" questions.

use std::collections::HashMap;

/// Vectors are L2-normalized at embed time, so a dot product *is* cosine
/// similarity. Quantization is symmetric around zero, which suits that.
const LEVELS: f32 = 127.0;

struct Slot {
    chunk_id: i64,
    /// Interned source, so filtering is an integer compare rather than a
    /// string compare per row.
    source: u32,
    alive: bool,
}

pub struct Index {
    dimension: usize,
    /// Row-major, `dimension` values per row.
    codes: Vec<i8>,
    scales: Vec<f32>,
    slots: Vec<Slot>,
    /// Rows vacated by a delete, reused before the arena grows. Re-indexing
    /// one file in a loop is the common case, and it must not leak.
    free: Vec<u32>,
    by_document: HashMap<i64, Vec<u32>>,
    sources: Vec<String>,
    source_ids: HashMap<String, u32>,
    live: usize,
}

impl Index {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            codes: Vec::new(),
            scales: Vec::new(),
            slots: Vec::new(),
            free: Vec::new(),
            by_document: HashMap::new(),
            sources: Vec::new(),
            source_ids: HashMap::new(),
            live: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.live
    }

    /// Resident bytes. Reported by `status` so the memory claim this rewrite
    /// makes is checkable without a profiler.
    pub fn memory_bytes(&self) -> usize {
        self.codes.capacity()
            + self.scales.capacity() * 4
            + self.slots.capacity() * std::mem::size_of::<Slot>()
            + self.free.capacity() * 4
    }

    fn source_ordinal(&mut self, source_id: &str) -> u32 {
        if let Some(&ordinal) = self.source_ids.get(source_id) {
            return ordinal;
        }
        let ordinal = self.sources.len() as u32;
        self.sources.push(source_id.to_owned());
        self.source_ids.insert(source_id.to_owned(), ordinal);
        ordinal
    }

    /// Replace every row belonging to `document_id`.
    ///
    /// Replace rather than append: a re-indexed file whose chunks moved must
    /// not leave its previous chunks behind, and that is the same rule the
    /// database enforces with a `DELETE` inside the write transaction. The two
    /// stay in step because ingest calls both from one place.
    pub fn replace_document(
        &mut self,
        document_id: i64,
        source_id: &str,
        chunks: &[(i64, Vec<f32>)],
    ) {
        self.remove_document(document_id);
        if chunks.is_empty() {
            return;
        }
        let source = self.source_ordinal(source_id);
        let mut rows = Vec::with_capacity(chunks.len());
        for (chunk_id, vector) in chunks {
            rows.push(self.push_row(*chunk_id, source, vector));
        }
        self.by_document.insert(document_id, rows);
    }

    fn push_row(&mut self, chunk_id: i64, source: u32, vector: &[f32]) -> u32 {
        let (codes, scale) = quantize(vector, self.dimension);
        let row = match self.free.pop() {
            Some(row) => {
                let start = row as usize * self.dimension;
                self.codes[start..start + self.dimension].copy_from_slice(&codes);
                self.scales[row as usize] = scale;
                self.slots[row as usize] = Slot { chunk_id, source, alive: true };
                row
            }
            None => {
                let row = self.slots.len() as u32;
                self.codes.extend_from_slice(&codes);
                self.scales.push(scale);
                self.slots.push(Slot { chunk_id, source, alive: true });
                row
            }
        };
        self.live += 1;
        row
    }

    pub fn remove_document(&mut self, document_id: i64) {
        let Some(rows) = self.by_document.remove(&document_id) else {
            return;
        };
        for row in rows {
            let slot = &mut self.slots[row as usize];
            if slot.alive {
                slot.alive = false;
                self.live -= 1;
                self.free.push(row);
            }
        }
    }

    /// Approximate top-`want` by int8 score.
    ///
    /// Returns chunk ids for the caller to rescore exactly. Deliberately does
    /// not return a score: an int8 score is a shortlisting device, and letting
    /// one escape into a result would be showing a number that is nearly but
    /// not quite the one a rerun would show.
    pub fn shortlist(&self, query: &[f32], want: usize, source: Option<&str>) -> Vec<i64> {
        if want == 0 || self.live == 0 {
            return Vec::new();
        }
        let filter = match source {
            Some(id) => match self.source_ids.get(id) {
                Some(&ordinal) => Some(ordinal),
                // A source with no rows yet is not an error; it is a repository
                // whose first index has not finished. Empty results and a
                // progress event beats an error the picker has to explain.
                None => return Vec::new(),
            },
            None => None,
        };
        let (query_codes, query_scale) = quantize(query, self.dimension);

        let mut best = TopK::new(want);
        for (row, slot) in self.slots.iter().enumerate() {
            if !slot.alive || filter.is_some_and(|wanted| wanted != slot.source) {
                continue;
            }
            let start = row * self.dimension;
            let dot = dot_i8(&self.codes[start..start + self.dimension], &query_codes);
            best.offer(dot as f32 * self.scales[row] * query_scale, slot.chunk_id);
        }
        best.into_sorted_ids()
    }
}

/// Symmetric per-vector quantization to int8.
///
/// The scale is the largest magnitude in the vector, so the full int8 range is
/// used regardless of how peaked the vector is. Reconstructed error is bounded
/// by half a level — around 0.4% of the maximum component — which reorders
/// only near-ties, and near-ties are exactly what the f32 rescore then sorts
/// out.
fn quantize(vector: &[f32], dimension: usize) -> (Vec<i8>, f32) {
    let mut codes = vec![0i8; dimension];
    let peak = vector.iter().take(dimension).fold(0.0f32, |m, v| m.max(v.abs()));
    if peak == 0.0 {
        return (codes, 0.0);
    }
    let scale = peak / LEVELS;
    for (code, value) in codes.iter_mut().zip(vector) {
        *code = (value / scale).round().clamp(-LEVELS, LEVELS) as i8;
    }
    (codes, scale)
}

/// Integer dot product. Written as a plain loop over equal-length slices
/// because that is the shape LLVM reliably turns into SIMD; a zip with
/// bounds-checked indexing is not.
fn dot_i8(a: &[i8], b: &[i8]) -> i32 {
    debug_assert_eq!(a.len(), b.len());
    let mut sum = 0i32;
    for (x, y) in a.iter().zip(b) {
        sum += i32::from(*x) * i32::from(*y);
    }
    sum
}

/// Exact cosine similarity between L2-normalized vectors.
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// A bounded max-selection over a stream, kept as a sorted-on-demand vector
/// rather than a heap: `want` is in the hundreds and the comparison is one
/// float, so the constant factor of a heap costs more than it saves.
struct TopK {
    want: usize,
    items: Vec<(f32, i64)>,
    threshold: f32,
}

impl TopK {
    fn new(want: usize) -> Self {
        Self { want, items: Vec::with_capacity(want * 2), threshold: f32::NEG_INFINITY }
    }

    fn offer(&mut self, score: f32, id: i64) {
        if self.items.len() >= self.want && score <= self.threshold {
            return;
        }
        self.items.push((score, id));
        if self.items.len() >= self.want * 2 {
            self.trim();
        }
    }

    fn trim(&mut self) {
        self.items.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
        self.items.truncate(self.want);
        self.threshold = self.items.last().map_or(f32::NEG_INFINITY, |(score, _)| *score);
    }

    fn into_sorted_ids(mut self) -> Vec<i64> {
        self.trim();
        self.items.into_iter().map(|(_, id)| id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(values: &[f32]) -> Vec<f32> {
        let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
        values.iter().map(|v| v / norm).collect()
    }

    #[test]
    fn quantization_preserves_ranking_for_clearly_different_vectors() {
        let mut index = Index::new(4);
        index.replace_document(1, "s", &[(10, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        index.replace_document(2, "s", &[(20, unit(&[0.0, 1.0, 0.0, 0.0]))]);
        index.replace_document(3, "s", &[(30, unit(&[0.9, 0.1, 0.0, 0.0]))]);

        let hits = index.shortlist(&unit(&[1.0, 0.0, 0.0, 0.0]), 3, None);
        assert_eq!(hits[0], 10, "the exact match must rank first");
        assert_eq!(hits[1], 30, "the near match must rank second");
    }

    #[test]
    fn quantization_round_trips_within_half_a_level() {
        let original = unit(&[0.9, -0.4, 0.1, 0.02, -0.7, 0.33, 0.0, 0.5]);
        let (codes, scale) = quantize(&original, original.len());
        for (code, value) in codes.iter().zip(&original) {
            let restored = f32::from(*code) * scale;
            assert!(
                (restored - value).abs() <= scale / 2.0 + f32::EPSILON,
                "{value} restored as {restored}"
            );
        }
    }

    #[test]
    fn replacing_a_document_reuses_its_rows_instead_of_growing() {
        // Save a file in a loop and the arena must not grow without bound.
        let mut index = Index::new(4);
        let vectors = vec![(1, unit(&[1.0, 0.0, 0.0, 0.0])), (2, unit(&[0.0, 1.0, 0.0, 0.0]))];
        index.replace_document(1, "s", &vectors);
        let after_first = index.codes.len();
        for _ in 0..20 {
            index.replace_document(1, "s", &vectors);
        }
        assert_eq!(index.len(), 2);
        assert_eq!(index.codes.len(), after_first, "rows leaked on re-index");
    }

    #[test]
    fn a_removed_document_stops_matching() {
        let mut index = Index::new(4);
        index.replace_document(1, "s", &[(10, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        index.replace_document(2, "s", &[(20, unit(&[0.0, 1.0, 0.0, 0.0]))]);
        index.remove_document(1);
        let hits = index.shortlist(&unit(&[1.0, 0.0, 0.0, 0.0]), 10, None);
        assert_eq!(hits, vec![20], "a deleted document was still searchable");
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn a_shrinking_replace_drops_the_rows_it_no_longer_has() {
        let mut index = Index::new(4);
        index.replace_document(
            1,
            "s",
            &[(10, unit(&[1.0, 0.0, 0.0, 0.0])), (11, unit(&[0.0, 1.0, 0.0, 0.0]))],
        );
        index.replace_document(1, "s", &[(12, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        assert_eq!(index.len(), 1);
        assert_eq!(index.shortlist(&unit(&[0.0, 1.0, 0.0, 0.0]), 10, None), vec![12]);
    }

    #[test]
    fn the_source_filter_excludes_other_sources() {
        let mut index = Index::new(4);
        index.replace_document(1, "alpha", &[(10, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        index.replace_document(2, "beta", &[(20, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        let query = unit(&[1.0, 0.0, 0.0, 0.0]);
        assert_eq!(index.shortlist(&query, 10, Some("alpha")), vec![10]);
        assert_eq!(index.shortlist(&query, 10, None).len(), 2);
    }

    #[test]
    fn an_unknown_source_returns_nothing_rather_than_everything() {
        // A repository whose first index has not finished yet. Returning every
        // other source's hits would be worse than returning none.
        let mut index = Index::new(4);
        index.replace_document(1, "alpha", &[(10, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        assert!(index.shortlist(&unit(&[1.0, 0.0, 0.0, 0.0]), 10, Some("nope")).is_empty());
    }

    #[test]
    fn top_k_keeps_the_highest_scores_across_many_trims() {
        let mut top = TopK::new(3);
        for i in 0..1000 {
            top.offer(i as f32, i);
        }
        assert_eq!(top.into_sorted_ids(), vec![999, 998, 997]);
    }

    #[test]
    fn a_zero_vector_neither_panics_nor_wins() {
        let mut index = Index::new(4);
        index.replace_document(1, "s", &[(10, vec![0.0; 4])]);
        index.replace_document(2, "s", &[(20, unit(&[1.0, 0.0, 0.0, 0.0]))]);
        let hits = index.shortlist(&unit(&[1.0, 0.0, 0.0, 0.0]), 2, None);
        assert_eq!(hits[0], 20);
    }
}
