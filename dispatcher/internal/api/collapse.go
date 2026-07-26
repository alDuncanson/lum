package api

import "github.com/alDuncanson/lum/dispatcher/internal/worker"

// Nearest-neighbour search returns chunks, and a question about one file is
// usually answered by several of them. Left alone, three chunks of the same
// file take three of the five slots anyone actually reads — measurably: on
// lum's own benchmark the answer to "picker entry formatting" fell out of the
// top ten behind three chunks of one unrelated file.
//
// Collapsing caps how many chunks any one document may contribute. It is
// applied here rather than in the worker because it is a policy about what
// `limit` means, and `limit` belongs to the public API. The worker's job is
// nearest neighbours; deciding that the sixth-best chunk is more useful than
// a third look at the same file is not.
const (
	// Chunks any one document may contribute, unless asked otherwise. Two
	// rather than one: a second hit in a large file is often a different
	// function and genuinely worth seeing, while a third rarely is.
	defaultPerFile = 2

	// Collapsing can only discard, so the search has to over-fetch to fill
	// `limit` afterwards. Four times covers the realistic worst case — every
	// result from a handful of files — without asking the worker for a
	// thousand neighbours to show ten.
	overfetch = 4

	// Ceiling on that over-fetch, so a large limit cannot turn into an
	// unbounded scan.
	maxFetch = 400
)

// fetchLimit is how many chunks to ask the worker for so that `limit` survive
// collapsing.
func fetchLimit(limit, perFile int) int {
	if perFile <= 0 {
		return limit
	}
	fetch := limit * overfetch
	if fetch > maxFetch {
		fetch = maxFetch
	}
	if fetch < limit {
		return limit
	}
	return fetch
}

// collapseByFile keeps at most perFile results per document, preserving the
// worker's ordering, and truncates to limit. perFile <= 0 disables collapsing
// and only the truncation applies.
//
// Results are ordered by score, so "the first perFile seen" is "the highest
// scoring perFile" — no re-sorting, and the file's rank stays where its best
// chunk put it.
func collapseByFile(results []worker.SearchResult, perFile, limit int) []worker.SearchResult {
	if limit > 0 && perFile <= 0 && len(results) > limit {
		return results[:limit]
	}
	if perFile <= 0 {
		return results
	}

	seen := make(map[string]int, len(results))
	kept := make([]worker.SearchResult, 0, min(len(results), max(limit, 0)))
	for _, result := range results {
		// DocumentID rather than URI: two sources may scan the same path, and
		// those are different documents with their own vectors.
		if seen[result.DocumentID] >= perFile {
			continue
		}
		seen[result.DocumentID]++
		kept = append(kept, result)
		if limit > 0 && len(kept) == limit {
			break
		}
	}
	return kept
}
