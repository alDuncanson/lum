//! Text → vectors, via ONNX Runtime.
//!
//! **Two sessions, not one.** ort's `Session::run` takes `&mut self`, so a
//! session cannot be shared lock-free — and a query that shares a lock with
//! bulk ingest waits behind a whole batch, at exactly the moment you search:
//! right after saving a file. One session is reserved for queries and one
//! belongs to ingest, so a keystroke never waits on indexing (measured: 9 ms
//! median under full indexing load). The ingest session is created when
//! indexing starts and dropped when the queue drains, so the weights it holds
//! are not resident between edits.
//!
//! **Batches are budgeted by padded tokens, not rows.** Activation memory
//! scales with rows × padded width, so a fixed row count makes a batch of
//! long chunks cost sixteen times a batch of short ones — and ONNX Runtime's
//! arena keeps the largest allocation it has ever served, so that one wide
//! batch sets resident memory for the life of the process. Sweeping the
//! budget on this repository: 8192 tokens/call peaks at 1229 MB and indexes
//! in 70 s; 1024 peaks at 748 MB in 51 s. Smaller is leaner *and* faster,
//! because attention is quadratic in the padded width and a wide batch spends
//! most of it on padding. Length-sorting before batching is what keeps the
//! padding small.
//!
//! Two negative results, recorded so nobody re-derives them:
//!
//! - **Dropping a session does not give its memory back.** ORT's CPU arena
//!   belongs to the environment, not the session; releasing the ingest
//!   session leaves RSS unchanged. `release_ingest` is still worth doing — it
//!   frees the weights and lets the allocator reuse the arena — but the token
//!   budget is what bounds the peak.
//! - **Thread count is not a memory knob.** Sweeping ORT's intra-op threads
//!   from 8 to 1 moved the peak by under 3% while making indexing 2.5×
//!   slower.
//!
//! Where that lands: ~350 MB with only the query session live, ~740 MB peak
//! while indexing, plateauing there across repeated full re-indexes rather
//! than creeping.

use std::path::Path;
use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use ort::memory::{AllocationDevice, AllocatorType, MemoryInfo, MemoryType};
use ort::session::builder::GraphOptimizationLevel;
use ort::session::Session;
use ort::value::Tensor;
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use crate::chunk::Chunk;
use crate::config::{Config, Model};
use crate::model::{self, ModelFiles};

/// bge-small's trained context. Longer input is truncated, which the chunker's
/// byte budget is sized to avoid in the first place.
const MAX_TOKENS: usize = 512;

/// Cheap token estimate for batching, from byte length.
///
/// Deliberately an estimate: knowing exactly would mean tokenizing everything
/// up front, and batching only needs to group similar lengths together, not to
/// be right. Code runs about three bytes per token, and the estimate is
/// clamped to the model's context because anything longer is truncated to it
/// anyway — so a 40 KB minified line is budgeted as 512 tokens, which is what
/// it will actually cost.
fn estimated_tokens(text: &str) -> usize {
    (text.len() / 3 + 2).min(MAX_TOKENS)
}

/// Text handed to the model for one chunk: a short context label, then the
/// chunk itself.
///
/// Neither prefix is stored — the row keeps `chunk.text` alone, so results
/// show code rather than a synthetic header. They exist purely so the vector
/// carries where the chunk came from.
///
/// People search with words that live in the path: "ingestion diagram",
/// "telescope plugin setup", "live activity tui". None of those could match
/// when only chunk text was embedded and the path was metadata. A dozen tokens
/// of path is cheap against a 512-token budget and is exactly what the query
/// is reaching for.
///
/// `chunk.context` is the same idea one level down: for markdown, the trail of
/// headings above the chunk, which is where a document says what a paragraph
/// is about without the paragraph repeating it.
pub fn passage_text(display_path: &str, uri: &str, chunk: &Chunk) -> String {
    let label = if display_path.is_empty() { derive_label(uri) } else { display_path.to_owned() };
    let mut out = String::with_capacity(label.len() + chunk.context.len() + chunk.text.len() + 2);
    for prefix in [label.as_str(), chunk.context.as_str()] {
        if !prefix.is_empty() {
            out.push_str(prefix);
            out.push('\n');
        }
    }
    out.push_str(&chunk.text);
    out
}

/// Last two path components — enough to be useful, short enough to leave out
/// the leading directories that are identical for every document and describe
/// nobody's query.
fn derive_label(uri: &str) -> String {
    let parts: Vec<&str> = uri.rsplit('/').take(2).collect();
    parts.into_iter().rev().collect::<Vec<_>>().join("/")
}

pub struct Embedder {
    model: Model,
    tokenizer: Tokenizer,
    onnx_path: std::path::PathBuf,
    batch: usize,
    token_budget: usize,
    query_threads: usize,
    ingest_threads: usize,
    /// Always resident. A query's latency is this session's inference time and
    /// nothing else.
    query: Mutex<Session>,
    /// Present only while there is indexing to do.
    ingest: Mutex<Option<Session>>,
}

impl Embedder {
    /// Resolve the model and open the query session.
    ///
    /// `on_download` fires only when files are actually missing, so a warm
    /// start does not announce a download it is not doing.
    pub fn load(config: &Config, on_download: impl FnOnce()) -> Result<Self> {
        let cache = config.models_dir();
        if !model::is_cached(&cache, config.model) {
            on_download();
        }
        let ModelFiles { onnx, tokenizer } = model::resolve(&cache, config.model)?;

        let mut tokenizer = Tokenizer::from_file(&tokenizer)
            .map_err(|e| anyhow!("loading tokenizer {}: {e}", tokenizer.display()))?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: MAX_TOKENS,
                ..Default::default()
            }))
            .map_err(|e| anyhow!("configuring truncation: {e}"))?;
        // BatchLongest, not Fixed: padding to 512 when the longest sequence is
        // 30 tokens means 94% of the compute — and of the activation memory —
        // is spent on `[PAD]`.
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            pad_id: 0,
            pad_token: "[PAD]".to_owned(),
            ..Default::default()
        }));

        // Cores are shared with whatever the user is actually doing. Indexing
        // is background work and takes half; a query is one short sequence
        // where more threads buy little.
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        let ingest_threads = config.embed_threads.unwrap_or((cores / 2).max(1));
        let query_threads = config.embed_threads.unwrap_or(2).min(cores);

        let query = open_session(&onnx, query_threads)?;
        Ok(Self {
            model: config.model,
            tokenizer,
            onnx_path: onnx,
            batch: config.embed_batch,
            token_budget: config.embed_token_budget,
            query_threads,
            ingest_threads,
            query: Mutex::new(query),
            ingest: Mutex::new(None),
        })
    }

    pub fn model(&self) -> Model {
        self.model
    }

    pub fn dimension(&self) -> usize {
        self.model.dimension()
    }

    /// Embed a search query.
    ///
    /// bge models are trained with asymmetric prefixes, so a query and the
    /// passage answering it land near each other rather than queries landing
    /// near queries.
    pub fn embed_query(&self, query: &str) -> Result<Vec<f32>> {
        let text = format!("query: {query}");
        let mut session = self.query.lock().expect("query session poisoned");
        let mut vectors = infer(&mut session, &self.tokenizer, &[text], self.dimension())?;
        vectors.pop().ok_or_else(|| anyhow!("model returned no vector for the query"))
    }

    /// Embed document chunks, reporting cumulative completion after each
    /// batch.
    ///
    /// The callback exists because this is the slow step and it is otherwise
    /// silent — one call can spend a minute inside the model with nothing
    /// observable happening. Per batch is the finest granularity available and
    /// a good unit anyway, since chunks are far more uniform in cost than
    /// documents.
    pub fn embed_passages(
        &self,
        texts: &[String],
        on_progress: &(dyn Fn(usize) + Sync),
    ) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let prefixed: Vec<String> = texts.iter().map(|t| format!("passage: {t}")).collect();

        // Length-sorted, token-budgeted batching.
        //
        // Padding is per batch, so grouping similar-length sequences keeps the
        // padded width near the real width. Budgeting by *tokens* rather than
        // rows then keeps the working set flat: activation memory scales with
        // rows × width, so a fixed row count means a batch of 512-token chunks
        // costs sixteen times a batch of 32-token ones. Sixteen long chunks in
        // one call is where the gigabytes came from.
        //
        // Sorting descending puts the widest batch first, so the peak is
        // established immediately rather than creeping up over a long index.
        let mut order: Vec<usize> = (0..prefixed.len()).collect();
        order.sort_by_key(|&i| std::cmp::Reverse(estimated_tokens(&prefixed[i])));

        let mut guard = self.ingest.lock().expect("ingest session poisoned");
        let session = match guard.as_mut() {
            Some(session) => session,
            None => {
                tracing::debug!("opening the ingest session");
                guard.insert(open_session(&self.onnx_path, self.ingest_threads)?)
            }
        };

        let mut batches: Vec<&[usize]> = Vec::new();
        let mut start = 0;
        while start < order.len() {
            // Sorted descending, so the first entry is the widest and sets the
            // padded width for the whole batch.
            let width = estimated_tokens(&prefixed[order[start]]).max(1);
            let rows = (self.token_budget / width).clamp(1, self.batch);
            let end = (start + rows).min(order.len());
            batches.push(&order[start..end]);
            start = end;
        }

        let mut out: Vec<Vec<f32>> = vec![Vec::new(); prefixed.len()];
        let mut done = 0;
        for batch in batches {
            let inputs: Vec<String> = batch.iter().map(|&i| prefixed[i].clone()).collect();
            let vectors = infer(session, &self.tokenizer, &inputs, self.dimension())?;
            for (&index, vector) in batch.iter().zip(vectors) {
                out[index] = vector;
            }
            done += batch.len();
            on_progress(done);
        }
        Ok(out)
    }

    /// Drop the ingest session, returning its arena to the OS.
    ///
    /// Called when the ingest queue drains. This is the whole of what the old
    /// build needed a second process, a supervisor, and a five-minute idle
    /// timer to accomplish, and unlike that version it costs queries nothing:
    /// the query session was never involved.
    pub fn release_ingest(&self) -> bool {
        let mut guard = self.ingest.lock().expect("ingest session poisoned");
        let had = guard.is_some();
        if had {
            tracing::debug!("releasing the ingest session");
        }
        *guard = None;
        had
    }

    pub fn ingest_loaded(&self) -> bool {
        self.ingest.lock().expect("ingest session poisoned").is_some()
    }

    pub fn batch_size(&self) -> usize {
        self.batch
    }

    pub fn threads(&self) -> (usize, usize) {
        (self.query_threads, self.ingest_threads)
    }
}

fn open_session(onnx: &Path, threads: usize) -> Result<Session> {
    // The plain device allocator rather than ONNX Runtime's arena.
    //
    // The arena grows to the largest allocation it has ever served and never
    // shrinks, and it is owned by the environment rather than the session, so
    // dropping a session does not give it back (measured: releasing the
    // ingest session leaves RSS unchanged). Going through the ordinary
    // allocator means inference buffers are freed when inference is done,
    // which is what makes "release the session when indexing stops" mean
    // anything at all.
    //
    // The cost is a malloc per intermediate tensor instead of a bump pointer.
    // Against ~40 ms of matrix multiplication per batch that does not register.
    let allocator =
        MemoryInfo::new(AllocationDevice::CPU, 0, AllocatorType::Device, MemoryType::Default)
            .context("describing the CPU allocator")?;

    Session::builder()
        .context("creating an ONNX session builder")?
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .context("setting the optimization level")?
        .with_intra_threads(threads)
        .context("setting intra-op threads")?
        .with_allocator(allocator)
        .context("selecting the non-arena allocator")?
        // With variable-length input, pre-planning allocations from the first
        // run's shapes is the other half of "grow to the widest thing ever
        // seen and keep it".
        .with_memory_pattern(false)
        .context("disabling memory pattern")?
        .commit_from_file(onnx)
        .with_context(|| format!("loading the ONNX model {}", onnx.display()))
}

/// One forward pass: tokenize, run, CLS-pool, normalize.
fn infer(
    session: &mut Session,
    tokenizer: &Tokenizer,
    texts: &[String],
    dimension: usize,
) -> Result<Vec<Vec<f32>>> {
    let encodings =
        tokenizer.encode_batch(texts.to_vec(), true).map_err(|e| anyhow!("tokenizing: {e}"))?;
    let rows = encodings.len();
    let width = encodings.first().map_or(0, |e| e.len());
    if rows == 0 || width == 0 {
        return Ok(vec![vec![0.0; dimension]; rows]);
    }

    let mut ids = Vec::with_capacity(rows * width);
    let mut mask = Vec::with_capacity(rows * width);
    let mut types = Vec::with_capacity(rows * width);
    for encoding in &encodings {
        // BatchLongest pads every row to the same width; a mismatch would mean
        // a ragged tensor, which ONNX cannot represent.
        debug_assert_eq!(encoding.len(), width);
        ids.extend(encoding.get_ids().iter().map(|&v| v as i64));
        mask.extend(encoding.get_attention_mask().iter().map(|&v| v as i64));
        types.extend(encoding.get_type_ids().iter().map(|&v| v as i64));
    }

    let shape = [rows, width];
    let outputs = session
        .run(ort::inputs! {
            "input_ids" => Tensor::from_array((shape, ids))?,
            "attention_mask" => Tensor::from_array((shape, mask))?,
            "token_type_ids" => Tensor::from_array((shape, types))?,
        })
        .context("running inference")?;
    let (_, hidden) = outputs
        .get("last_hidden_state")
        .ok_or_else(|| anyhow!("model produced no last_hidden_state"))?
        .try_extract_tensor::<f32>()
        .context("reading last_hidden_state")?;

    let expected = rows * width * dimension;
    if hidden.len() != expected {
        return Err(anyhow!(
            "last_hidden_state has {} values, expected {expected} ({rows}x{width}x{dimension})",
            hidden.len()
        ));
    }

    // bge pools by taking the [CLS] token — position 0 — rather than averaging
    // over the sequence. Mean pooling here would produce vectors that are
    // plausible, self-consistent, and measurably worse, which is the hardest
    // kind of wrong to notice.
    Ok((0..rows)
        .map(|row| {
            let start = row * width * dimension;
            normalize(&hidden[start..start + dimension])
        })
        .collect())
}

/// L2-normalize, so a dot product is cosine similarity and the index can skip
/// the division on every comparison.
fn normalize(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm == 0.0 {
        return vector.to_vec();
    }
    vector.iter().map(|v| v / norm).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(text: &str, context: &str) -> Chunk {
        Chunk { text: text.to_owned(), context: context.to_owned(), ..Default::default() }
    }

    #[test]
    fn passage_text_prefixes_the_display_path_without_storing_it() {
        assert_eq!(
            passage_text("docs/diagrams.md", "/abs/docs/diagrams.md", &chunk("flowchart LR", "")),
            "docs/diagrams.md\nflowchart LR"
        );
    }

    #[test]
    fn passage_text_prefixes_the_heading_trail_too() {
        // Path, then where in the document, then the chunk.
        assert_eq!(
            passage_text(
                "docs/diagrams.md",
                "/abs/docs/diagrams.md",
                &chunk("flowchart LR", "Boundaries > Reference")
            ),
            "docs/diagrams.md\nBoundaries > Reference\nflowchart LR"
        );
    }

    #[test]
    fn passage_text_falls_back_to_a_label_from_the_uri() {
        // Two components, so an absolute path does not spend tokens on
        // /Users/<name>/code, which is identical for every document.
        assert_eq!(
            passage_text("", "/Users/someone/code/lum/docs/diagrams.md", &chunk("x", "")),
            "docs/diagrams.md\nx"
        );
        assert_eq!(passage_text("", "", &chunk("just text", "")), "just text");
    }

    #[test]
    fn normalize_produces_unit_vectors() {
        let unit = normalize(&[3.0, 4.0]);
        assert!((unit[0] - 0.6).abs() < 1e-6, "{unit:?}");
        assert!((unit[1] - 0.8).abs() < 1e-6, "{unit:?}");
        let magnitude = unit.iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!((magnitude - 1.0).abs() < 1e-6);
    }

    #[test]
    fn normalize_leaves_a_zero_vector_alone() {
        // A chunk that tokenizes to nothing must not become NaN and poison
        // every comparison it takes part in.
        assert_eq!(normalize(&[0.0, 0.0]), vec![0.0, 0.0]);
    }
}
