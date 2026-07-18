// Package source defines lum's ingestion extension point: the Source
// interface, which turns "somewhere documents live" into a uniform
// stream of document references.
//
// Everything downstream — the catalog, the ingest worker, the data
// plane — is source-agnostic. Adding RSS feeds, IMAP mailboxes, or
// bookmark exports means writing one new Source implementation and
// registering it in registry.go; no other file changes.
package source

import "context"

// DocumentRef identifies one document as seen by a source during a
// scan. It is deliberately content-free: content is fetched lazily via
// Source.Read only for documents that are new or changed, so scanning a
// large, mostly-unchanged source stays cheap.
type DocumentRef struct {
	// URI uniquely identifies the document within lum (absolute file
	// path for local dirs; item URL for a future RSS source).
	URI string
	// MimeType tells the data plane which parser to use.
	MimeType string
	// ContentHash fingerprints the current content (sha256 hex). The
	// ingest worker compares it against the catalog to skip unchanged
	// documents — lum's change detection in one field.
	ContentHash string
}

// Source enumerates and fetches documents from one registered location.
//
// Implementations must be safe for repeated Scans; lum rescans sources
// to pick up changes (and, in a later milestone, watches them live).
type Source interface {
	// Type is the stable implementation name stored in the catalog,
	// e.g. "localdir". Used to reconstruct sources on daemon restart.
	Type() string

	// Scan enumerates every document currently visible. The returned
	// set is authoritative: catalog documents absent from it are
	// treated as deleted.
	Scan(ctx context.Context) ([]DocumentRef, error)

	// Read fetches the raw bytes of a document reported by Scan.
	Read(ctx context.Context, ref DocumentRef) ([]byte, error)
}
