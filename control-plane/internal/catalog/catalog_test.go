package catalog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// openTestCatalog gives each test an isolated database in a temp dir
// that the test framework cleans up automatically.
func openTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestAddSourceIsIdempotent(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()

	first := Source{ID: "id-1", Type: "localdir", URI: "/tmp/docs", CreatedAt: time.Now().UTC()}
	got, created, err := c.AddSource(ctx, first)
	if err != nil || !created {
		t.Fatalf("first add: got created=%v err=%v, want created=true", created, err)
	}
	if got.ID != "id-1" {
		t.Fatalf("first add returned ID %q, want id-1", got.ID)
	}

	// Same URI again, different candidate ID: must return the existing
	// row untouched — `lum add ~/Documents` twice is not an error.
	second := Source{ID: "id-2", Type: "localdir", URI: "/tmp/docs", CreatedAt: time.Now().UTC()}
	got, created, err = c.AddSource(ctx, second)
	if err != nil || created {
		t.Fatalf("second add: got created=%v err=%v, want created=false", created, err)
	}
	if got.ID != "id-1" {
		t.Fatalf("second add returned ID %q, want existing id-1", got.ID)
	}
}

func TestDocumentLifecycle(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()

	src := Source{ID: "src-1", Type: "localdir", URI: "/tmp/docs", CreatedAt: time.Now().UTC()}
	if _, _, err := c.AddSource(ctx, src); err != nil {
		t.Fatal(err)
	}

	// Never-ingested documents report sql.ErrNoRows; the ingest worker
	// relies on this to distinguish "new" from "changed".
	if _, err := c.DocumentByURI(ctx, "src-1", "/tmp/docs/a.md"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown document: got %v, want sql.ErrNoRows", err)
	}

	doc := Document{
		ID: "doc-1", SourceID: "src-1", URI: "/tmp/docs/a.md",
		ContentHash: "hash-v1", ChunkCount: 3, IngestedAt: time.Now().UTC(),
	}
	if err := c.UpsertDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}

	// Re-ingest with new content: same row, updated hash and count.
	doc.ContentHash, doc.ChunkCount = "hash-v2", 5
	if err := c.UpsertDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got, err := c.DocumentByURI(ctx, "src-1", "/tmp/docs/a.md")
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentHash != "hash-v2" || got.ChunkCount != 5 {
		t.Fatalf("after upsert: hash=%q chunks=%d, want hash-v2/5", got.ContentHash, got.ChunkCount)
	}

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Sources != 1 || stats.Documents != 1 || stats.Chunks != 5 {
		t.Fatalf("stats = %+v, want 1 source, 1 document, 5 chunks", stats)
	}

	if err := c.DeleteDocument(ctx, "doc-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DocumentByURI(ctx, "src-1", "/tmp/docs/a.md"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("after delete: got %v, want sql.ErrNoRows", err)
	}
}

// TestDocumentIdentityIsScopedBySourceNotGloballyUnique guards against the
// #4 regression: an overlapping/nested source (e.g. ~/Documents and
// ~/Documents/notes registered separately) must not have its scan find
// and silently adopt another source's document row just because the URI
// happens to match.
func TestDocumentIdentityIsScopedBySourceNotGloballyUnique(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()

	for _, id := range []string{"src-a", "src-b"} {
		if _, _, err := c.AddSource(ctx, Source{ID: id, Type: "localdir", URI: "/tmp/" + id, CreatedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}

	const sharedURI = "/tmp/docs/shared.md"
	docA := Document{ID: "doc-a", SourceID: "src-a", URI: sharedURI, ContentHash: "hash-a", ChunkCount: 1, IngestedAt: time.Now().UTC()}
	docB := Document{ID: "doc-b", SourceID: "src-b", URI: sharedURI, ContentHash: "hash-b", ChunkCount: 2, IngestedAt: time.Now().UTC()}
	if err := c.UpsertDocument(ctx, docA); err != nil {
		t.Fatal(err)
	}
	if err := c.UpsertDocument(ctx, docB); err != nil {
		t.Fatal(err)
	}

	gotA, err := c.DocumentByURI(ctx, "src-a", sharedURI)
	if err != nil {
		t.Fatal(err)
	}
	if gotA.ID != "doc-a" || gotA.ContentHash != "hash-a" {
		t.Fatalf("DocumentByURI(src-a, ...) = %+v, want doc-a/hash-a untouched by src-b's row", gotA)
	}
	gotB, err := c.DocumentByURI(ctx, "src-b", sharedURI)
	if err != nil {
		t.Fatal(err)
	}
	if gotB.ID != "doc-b" || gotB.ContentHash != "hash-b" {
		t.Fatalf("DocumentByURI(src-b, ...) = %+v, want doc-b/hash-b untouched by src-a's row", gotB)
	}

	// Re-ingesting src-a's document must update only its own row.
	docA.ContentHash = "hash-a-v2"
	if err := c.UpsertDocument(ctx, docA); err != nil {
		t.Fatal(err)
	}
	if gotB, err := c.DocumentByURI(ctx, "src-b", sharedURI); err != nil || gotB.ContentHash != "hash-b" {
		t.Fatalf("src-b's document changed after src-a's re-ingest: %+v, err=%v", gotB, err)
	}

	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Documents != 2 {
		t.Fatalf("stats.Documents = %d, want 2 distinct rows for the shared URI", stats.Documents)
	}
}

func TestDeleteSourceRemovesRowAndCascadesDocuments(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()

	if _, _, err := c.AddSource(ctx, Source{ID: "src-1", Type: "localdir", URI: "/tmp/docs", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	doc := Document{ID: "doc-1", SourceID: "src-1", URI: "/tmp/docs/a.md", ContentHash: "h", ChunkCount: 1, IngestedAt: time.Now().UTC()}
	if err := c.UpsertDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RecordIngestFailure(ctx, IngestFailure{SourceID: "src-1", URI: "/tmp/docs/b.md", Error: "boom", FailedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}

	if err := c.DeleteSource(ctx, "src-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSource(ctx, "src-1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetSource after delete: got %v, want sql.ErrNoRows", err)
	}
	if _, err := c.DocumentByURI(ctx, "src-1", "/tmp/docs/a.md"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("document after source delete: got %v, want sql.ErrNoRows (cascade)", err)
	}
	failures, err := c.IngestFailuresBySource(ctx, "src-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 0 {
		t.Fatalf("ingest failures after source delete = %+v, want none (cascade)", failures)
	}
}

func TestIngestFailureLifecycle(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()
	src := Source{ID: "src-1", Type: "localdir", URI: "/tmp/docs", CreatedAt: time.Now().UTC()}
	if _, _, err := c.AddSource(ctx, src); err != nil {
		t.Fatal(err)
	}

	failedAt := time.Now().UTC()
	failure := IngestFailure{
		SourceID: src.ID, URI: "/tmp/docs/a.md", Error: "lumen unavailable", FailedAt: failedAt,
	}
	for want := 1; want <= 2; want++ {
		attempts, err := c.RecordIngestFailure(ctx, failure)
		if err != nil {
			t.Fatal(err)
		}
		if attempts != want {
			t.Fatalf("attempts = %d, want %d", attempts, want)
		}
	}

	failures, err := c.IngestFailuresBySource(ctx, src.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 || failures[0].Attempts != 2 || failures[0].Error != failure.Error {
		t.Fatalf("failures = %#v, want one two-attempt failure", failures)
	}
	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Failures != 1 {
		t.Fatalf("failure count = %d, want 1", stats.Failures)
	}

	if err := c.ClearIngestFailure(ctx, src.ID, failure.URI); err != nil {
		t.Fatal(err)
	}
	failures, err = c.IngestFailures(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 0 {
		t.Fatalf("failures after clear = %#v, want none", failures)
	}
}

func TestIngestFailuresAreNewestFirst(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()
	src := Source{ID: "src-1", Type: "localdir", URI: "/tmp/docs", CreatedAt: time.Now().UTC()}
	if _, _, err := c.AddSource(ctx, src); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, time.July, 21, 12, 0, 0, 100_000_000, time.UTC)
	for _, failure := range []IngestFailure{
		{SourceID: src.ID, URI: "/older", Error: "older", FailedAt: base},
		{SourceID: src.ID, URI: "/newer", Error: "newer", FailedAt: base.Add(10 * time.Millisecond)},
	} {
		if _, err := c.RecordIngestFailure(ctx, failure); err != nil {
			t.Fatal(err)
		}
	}
	failures, err := c.IngestFailures(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 2 || failures[0].URI != "/newer" || failures[1].URI != "/older" {
		t.Fatalf("failure order = %#v, want newer then older", failures)
	}
}

// TestMalformedTimestampsSurfaceAsErrors guards the #7 fix: a malformed
// timestamp (in practice only reachable via DB corruption, since every
// write goes through time.Format) must be reported, not silently
// swallowed into a zero-value time.Time.
func TestMalformedTimestampsSurfaceAsErrors(t *testing.T) {
	c := openTestCatalog(t)
	ctx := context.Background()

	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO sources (id, type, uri, created_at) VALUES (?, ?, ?, ?)`,
		"src-1", "localdir", "/tmp/docs", "not-a-timestamp",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetSource(ctx, "src-1"); err == nil {
		t.Fatal("GetSource with a malformed created_at returned no error")
	}

	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO documents (id, source_id, uri, content_hash, chunk_count, ingested_at) VALUES (?, ?, ?, ?, ?, ?)`,
		"doc-1", "src-1", "/tmp/docs/a.md", "hash", 1, "not-a-timestamp",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DocumentByURI(ctx, "src-1", "/tmp/docs/a.md"); err == nil {
		t.Fatal("DocumentByURI with a malformed ingested_at returned no error")
	}

	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO ingest_failures (source_id, uri, attempts, error, failed_at) VALUES (?, ?, ?, ?, ?)`,
		"src-1", "/tmp/docs/b.md", 1, "boom", "not-a-timestamp",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.IngestFailuresBySource(ctx, "src-1"); err == nil {
		t.Fatal("IngestFailuresBySource with a malformed failed_at returned no error")
	}
}
