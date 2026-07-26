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
	// MimeType tells the worker which parser to use.
	MimeType string
	// ContentHash is the authoritative sha256 hex of the current content,
	// or empty when the source could not determine it without reading the
	// document. Empty is the normal case for local files: reading every
	// file on every scan is the cost Fingerprint exists to avoid, so the
	// hash is computed once downstream, from the bytes the ingest runner
	// reads anyway.
	ContentHash string
	// Fingerprint is a cheap, source-defined stand-in for content identity
	// (for local files, size and mtime). Equal fingerprints mean "almost
	// certainly unchanged" and let a scan skip the document without
	// reading it; different fingerprints mean "read it and compare hashes",
	// never "re-embed it". Empty means the source offers no cheap check,
	// in which case ContentHash is the only signal.
	//
	// A fingerprint is never treated as proof of change, only of sameness.
	// That asymmetry is what makes it safe: the failure mode of a stale
	// fingerprint is one wasted read, not a missing document.
	Fingerprint string
	// DisplayPath is a short, human-meaningful label for the document —
	// the repository-relative path for local directories.
	//
	// It is prepended to each chunk before embedding, so that searches
	// using words from the path ("ingestion diagram", "telescope plugin
	// setup") have something to match. The source provides it because only
	// the source knows what a short label means for its own URI space: an
	// absolute filesystem path is the wrong thing to embed, and a future
	// RSS source would want an entry title rather than a path at all.
	DisplayPath string
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

// Watcher is an optional capability for sources that can report live changes.
// Notifications are hints only: consumers still run a full authoritative Scan.
type Watcher interface {
	Watch(ctx context.Context) (changes <-chan struct{}, failures <-chan error, err error)
}
