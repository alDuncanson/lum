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
	if _, err := c.DocumentByURI(ctx, "/tmp/docs/a.md"); !errors.Is(err, sql.ErrNoRows) {
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
	got, err := c.DocumentByURI(ctx, "/tmp/docs/a.md")
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
	if _, err := c.DocumentByURI(ctx, "/tmp/docs/a.md"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("after delete: got %v, want sql.ErrNoRows", err)
	}
}
