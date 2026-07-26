package api

import (
	"testing"

	"github.com/alDuncanson/lum/dispatcher/internal/worker"
)

func hits(documentIDs ...string) []worker.SearchResult {
	out := make([]worker.SearchResult, 0, len(documentIDs))
	for i, id := range documentIDs {
		out = append(out, worker.SearchResult{
			DocumentID: id,
			URI:        "/repo/" + id + ".go",
			ChunkIndex: uint32(i),
			// Descending, as the worker returns them.
			Score: float32(len(documentIDs) - i),
		})
	}
	return out
}

func documentIDs(results []worker.SearchResult) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, r.DocumentID)
	}
	return out
}

func equal(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestCollapseCapsChunksPerFile(t *testing.T) {
	// The case this exists for: one file taking the whole result list.
	got := collapseByFile(hits("a", "a", "a", "b", "c", "d"), 2, 5)
	equal(t, documentIDs(got), []string{"a", "a", "b", "c", "d"})
}

func TestCollapsePreservesWorkerOrdering(t *testing.T) {
	// A file's rank is set by its best chunk. Dropping a later chunk of an
	// earlier file must not promote the file above it.
	got := collapseByFile(hits("a", "b", "a", "c", "a", "d"), 1, 4)
	equal(t, documentIDs(got), []string{"a", "b", "c", "d"})
}

func TestCollapseStopsAtLimit(t *testing.T) {
	got := collapseByFile(hits("a", "b", "c", "d", "e"), 2, 3)
	equal(t, documentIDs(got), []string{"a", "b", "c"})
}

func TestCollapseDisabledStillTruncates(t *testing.T) {
	// per_file=0 asks for raw nearest neighbours, but `limit` still means
	// limit — the handler over-fetched to make collapsing possible.
	got := collapseByFile(hits("a", "a", "a", "a"), 0, 2)
	equal(t, documentIDs(got), []string{"a", "a"})
}

func TestCollapseKeepsFewerResultsThanLimitWhenThatIsAllThereIs(t *testing.T) {
	// A repository of two files cannot fill ten slots, and padding it with
	// duplicates would defeat the point.
	got := collapseByFile(hits("a", "a", "a", "b", "b", "b"), 2, 10)
	equal(t, documentIDs(got), []string{"a", "a", "b", "b"})
}

func TestCollapseSeparatesSourcesSharingAPath(t *testing.T) {
	// Two sources can scan the same directory. Same URI, different documents,
	// different vectors — collapsing on the path would hide one of them.
	results := hits("doc-a", "doc-b")
	results[0].URI = "/shared/main.go"
	results[1].URI = "/shared/main.go"
	got := collapseByFile(results, 1, 5)
	equal(t, documentIDs(got), []string{"doc-a", "doc-b"})
}

func TestFetchLimitOverfetchesOnlyWhenCollapsing(t *testing.T) {
	if got := fetchLimit(10, 0); got != 10 {
		t.Fatalf("collapsing off should fetch exactly the limit, got %d", got)
	}
	if got := fetchLimit(10, 2); got != 40 {
		t.Fatalf("got %d, want 40", got)
	}
	// The ceiling holds, and never below the limit itself.
	if got := fetchLimit(100, 2); got != maxFetch {
		t.Fatalf("got %d, want %d", got, maxFetch)
	}
	if got := fetchLimit(500, 2); got < 500 {
		t.Fatalf("fetch %d must not be below the limit 500", got)
	}
}

func TestIsTestPathFollowsNamingConventions(t *testing.T) {
	for _, uri := range []string{
		"/repo/internal/api/server_test.go",
		"/repo/pkg/thing_test.py",
		"/repo/src/App.test.tsx",
		"/repo/src/App.spec.js",
		"/repo/tests/integration.go",
		"/repo/__tests__/render.js",
		"/repo/spec/models/user_spec.rb",
		"/repo/pkg/test_helpers.py",
	} {
		if !isTestPath(uri) {
			t.Errorf("%s should read as a test", uri)
		}
	}
	for _, uri := range []string{
		"/repo/internal/api/server.go",
		// Rust keeps its tests inside the file they test, so there is no
		// path to recognize — and penalizing this one would penalize the
		// vector store.
		"/repo/worker/src/store/edge.rs",
		// "latest" ends in "test" and is not one.
		"/repo/internal/latest.go",
		"/repo/contest/main.go",
	} {
		if isTestPath(uri) {
			t.Errorf("%s should not read as a test", uri)
		}
	}
}

func TestDropTestsRemovesOnlyTests(t *testing.T) {
	results := hits("a", "b", "c")
	results[1].URI = "/repo/b_test.go"
	got := dropTests(results)
	equal(t, documentIDs(got), []string{"a", "c"})
}
