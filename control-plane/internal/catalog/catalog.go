// Package catalog is the control plane's persistent memory: a SQLite
// database recording which sources are registered and which documents
// (with which content hashes and chunk counts) have been ingested.
//
// Division of state, systemwide:
//
//	catalog.db (here)     WHAT EXISTS — sources, documents, hashes, counts
//	vectors/ (data plane) WHAT IT MEANS — embeddings + chunk payloads
//
// Every fact lives in exactly one place. The vector store is never
// enumerated to answer "what have we ingested?"; the catalog is never
// asked "what matches this query?".
//
// SQLite is accessed via modernc.org/sqlite, a pure-Go driver — no cgo,
// so `go build` works anywhere without a C toolchain.
package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

const schema = `
CREATE TABLE IF NOT EXISTS sources (
	id         TEXT PRIMARY KEY,             -- UUID assigned at registration
	type       TEXT NOT NULL,                -- Source implementation, e.g. "localdir"
	uri        TEXT NOT NULL UNIQUE,         -- canonical location (absolute path)
	created_at TEXT NOT NULL                 -- RFC 3339
);

CREATE TABLE IF NOT EXISTS documents (
	id           TEXT PRIMARY KEY,           -- UUID; stable across re-ingests
	source_id    TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
	uri          TEXT NOT NULL UNIQUE,       -- e.g. absolute file path
	content_hash TEXT NOT NULL,              -- sha256 of content at last ingest
	chunk_count  INTEGER NOT NULL DEFAULT 0, -- points stored in the vector index
	ingested_at  TEXT NOT NULL               -- RFC 3339
);

CREATE INDEX IF NOT EXISTS documents_by_source ON documents(source_id);
`

// Source is a registered document location.
type Source struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	URI       string    `json:"uri"`
	CreatedAt time.Time `json:"created_at"`
}

// Document is one ingested document's bookkeeping row.
type Document struct {
	ID          string    `json:"id"`
	SourceID    string    `json:"source_id"`
	URI         string    `json:"uri"`
	ContentHash string    `json:"content_hash"`
	ChunkCount  uint32    `json:"chunk_count"`
	IngestedAt  time.Time `json:"ingested_at"`
}

// Catalog wraps the SQLite handle. Methods are safe for concurrent use
// (database/sql pools connections; SQLite serializes writers via WAL +
// busy timeout).
type Catalog struct {
	db *sql.DB
}

// Open creates/migrates the database at path.
func Open(path string) (*Catalog, error) {
	// WAL allows a reader (status/search handlers) while the ingest
	// worker writes; busy_timeout retries briefly instead of failing on
	// contention; foreign_keys makes the source→document cascade work.
	dsn := fmt.Sprintf(
		"file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)",
		path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening catalog: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating catalog schema: %w", err)
	}
	return &Catalog{db: db}, nil
}

func (c *Catalog) Close() error { return c.db.Close() }

// ---- sources ----

// AddSource inserts a source; returns the existing row unchanged if the
// URI is already registered (registration is idempotent).
func (c *Catalog) AddSource(ctx context.Context, s Source) (Source, bool, error) {
	existing, err := c.sourceByURI(ctx, s.URI)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Source{}, false, err
	}
	_, err = c.db.ExecContext(ctx,
		`INSERT INTO sources (id, type, uri, created_at) VALUES (?, ?, ?, ?)`,
		s.ID, s.Type, s.URI, s.CreatedAt.Format(time.RFC3339),
	)
	if err != nil {
		return Source{}, false, fmt.Errorf("inserting source: %w", err)
	}
	return s, true, nil
}

func (c *Catalog) sourceByURI(ctx context.Context, uri string) (Source, error) {
	row := c.db.QueryRowContext(ctx,
		`SELECT id, type, uri, created_at FROM sources WHERE uri = ?`, uri)
	return scanSource(row)
}

// GetSource fetches a source by ID.
func (c *Catalog) GetSource(ctx context.Context, id string) (Source, error) {
	row := c.db.QueryRowContext(ctx,
		`SELECT id, type, uri, created_at FROM sources WHERE id = ?`, id)
	return scanSource(row)
}

// ListSources returns all registered sources, oldest first.
func (c *Catalog) ListSources(ctx context.Context) ([]Source, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT id, type, uri, created_at FROM sources ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Source
	for rows.Next() {
		s, err := scanSource(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ---- documents ----

// DocumentByURI returns the bookkeeping row for a document, or
// sql.ErrNoRows if it has never been ingested.
func (c *Catalog) DocumentByURI(ctx context.Context, uri string) (Document, error) {
	row := c.db.QueryRowContext(ctx,
		`SELECT id, source_id, uri, content_hash, chunk_count, ingested_at
		 FROM documents WHERE uri = ?`, uri)
	return scanDocument(row)
}

// DocumentsBySource lists all documents belonging to a source.
func (c *Catalog) DocumentsBySource(ctx context.Context, sourceID string) ([]Document, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT id, source_id, uri, content_hash, chunk_count, ingested_at
		 FROM documents WHERE source_id = ?`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Document
	for rows.Next() {
		d, err := scanDocument(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// UpsertDocument records a successful ingest.
func (c *Catalog) UpsertDocument(ctx context.Context, d Document) error {
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO documents (id, source_id, uri, content_hash, chunk_count, ingested_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(uri) DO UPDATE SET
		   content_hash = excluded.content_hash,
		   chunk_count  = excluded.chunk_count,
		   ingested_at  = excluded.ingested_at`,
		d.ID, d.SourceID, d.URI, d.ContentHash, d.ChunkCount,
		d.IngestedAt.Format(time.RFC3339),
	)
	return err
}

// DeleteDocument removes a document's bookkeeping row (after its vectors
// have been removed from the data plane).
func (c *Catalog) DeleteDocument(ctx context.Context, id string) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM documents WHERE id = ?`, id)
	return err
}

// Stats summarizes catalog contents for `lum status`.
type Stats struct {
	Sources   int `json:"sources"`
	Documents int `json:"documents"`
	Chunks    int `json:"chunks"`
}

// Stats returns systemwide counts.
func (c *Catalog) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := c.db.QueryRowContext(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM sources),
		  (SELECT COUNT(*) FROM documents),
		  (SELECT COALESCE(SUM(chunk_count), 0) FROM documents)
	`).Scan(&s.Sources, &s.Documents, &s.Chunks)
	return s, err
}

// ---- row scanning helpers ----

type rowScanner interface{ Scan(dest ...any) error }

func scanSource(r rowScanner) (Source, error) {
	var s Source
	var created string
	if err := r.Scan(&s.ID, &s.Type, &s.URI, &created); err != nil {
		return Source{}, err
	}
	s.CreatedAt, _ = time.Parse(time.RFC3339, created)
	return s, nil
}

func scanDocument(r rowScanner) (Document, error) {
	var d Document
	var ingested string
	if err := r.Scan(&d.ID, &d.SourceID, &d.URI, &d.ContentHash, &d.ChunkCount, &ingested); err != nil {
		return Document{}, err
	}
	d.IngestedAt, _ = time.Parse(time.RFC3339, ingested)
	return d, nil
}
