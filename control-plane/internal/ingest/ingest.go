// Package ingest is the event-driven heart of the control plane: it
// turns "a source changed" into catalog updates and data plane calls.
//
// Flow for one scan:
//
//	Source.Scan ──▶ diff vs catalog ──▶ per document:
//	                                      unchanged → skip
//	                                      new/changed → Read → dataplane.Ingest → catalog upsert
//	                                      vanished → dataplane.Delete → catalog delete
//
// Scans are queued on a channel and executed by a single background
// worker goroutine. Single, deliberately: ingestion is bottlenecked by
// the embedding model (CPU-bound in the data plane), so source-level
// parallelism would add contention and complexity for no throughput.
// The channel is the system's event bus in miniature — a broker like
// NATS could replace it someday without changing what flows through it.
package ingest

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/source"
)

// Ingestor owns the scan queue and executes scans.
type Ingestor struct {
	catalog *catalog.Catalog
	dp      *dataplane.Client
	queue   chan string // source IDs awaiting a scan
}

// New creates an Ingestor and starts its worker; cancel ctx to stop.
func New(ctx context.Context, cat *catalog.Catalog, dp *dataplane.Client) *Ingestor {
	ing := &Ingestor{
		catalog: cat,
		dp:      dp,
		queue:   make(chan string, 64),
	}
	go ing.worker(ctx)
	return ing
}

// EnqueueScan schedules a scan of the given source. Non-blocking; if
// the queue is full the scan is dropped with a warning (the next manual
// or startup scan will catch up — scans are idempotent).
func (i *Ingestor) EnqueueScan(sourceID string) {
	select {
	case i.queue <- sourceID:
	default:
		slog.Warn("scan queue full, dropping scan request", "source", sourceID)
	}
}

func (i *Ingestor) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case sourceID := <-i.queue:
			if err := i.scanSource(ctx, sourceID); err != nil {
				slog.Error("scan failed", "source", sourceID, "error", err)
			}
		}
	}
}

// scanSource performs one full reconciliation of a source against the
// catalog and vector index. Idempotent: running it twice in a row does
// no extra work the second time (hashes match, everything is skipped).
func (i *Ingestor) scanSource(ctx context.Context, sourceID string) error {
	started := time.Now()

	src, err := i.resolveSource(ctx, sourceID)
	if err != nil {
		return err
	}

	refs, err := src.Scan(ctx)
	if err != nil {
		return err
	}

	var ingested, unchanged, removed, failed int
	seen := make(map[string]bool, len(refs))

	for _, ref := range refs {
		seen[ref.URI] = true

		existing, err := i.catalog.DocumentByURI(ctx, ref.URI)
		switch {
		case err == nil && existing.ContentHash == ref.ContentHash:
			unchanged++ // change detection: hash match ⇒ skip entirely
			continue
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return err
		}

		// New document or changed content: run the pipeline. The
		// document keeps its UUID across re-ingests so its chunk point
		// IDs stay stable in the vector index.
		doc := existing
		if errors.Is(err, sql.ErrNoRows) {
			doc = catalog.Document{ID: uuid.NewString(), SourceID: sourceID, URI: ref.URI}
		}

		content, err := src.Read(ctx, ref)
		if err != nil {
			slog.Warn("read failed, skipping", "uri", ref.URI, "error", err)
			failed++
			continue
		}

		chunkCount, err := i.dp.IngestDocument(ctx,
			doc.ID, sourceID, ref.URI, ref.MimeType, content, doc.ChunkCount)
		if err != nil {
			slog.Warn("ingest failed, skipping", "uri", ref.URI, "error", err)
			failed++
			continue
		}

		doc.ContentHash = ref.ContentHash
		doc.ChunkCount = chunkCount
		doc.IngestedAt = time.Now().UTC()
		if err := i.catalog.UpsertDocument(ctx, doc); err != nil {
			return err
		}
		ingested++
	}

	// Anything in the catalog that the scan no longer sees was deleted
	// (or renamed) at the source; remove its vectors and its row.
	known, err := i.catalog.DocumentsBySource(ctx, sourceID)
	if err != nil {
		return err
	}
	for _, doc := range known {
		if seen[doc.URI] {
			continue
		}
		if err := i.dp.DeleteDocument(ctx, doc.ID, doc.ChunkCount); err != nil {
			slog.Warn("vector delete failed", "uri", doc.URI, "error", err)
			continue // keep the row; next scan retries the delete
		}
		if err := i.catalog.DeleteDocument(ctx, doc.ID); err != nil {
			return err
		}
		removed++
	}

	slog.Info("scan complete",
		"source", sourceID,
		"ingested", ingested,
		"unchanged", unchanged,
		"removed", removed,
		"failed", failed,
		"took", time.Since(started).Round(time.Millisecond),
	)
	return nil
}

// resolveSource loads a source row and reconstructs its implementation.
func (i *Ingestor) resolveSource(ctx context.Context, sourceID string) (source.Source, error) {
	row, err := i.catalog.GetSource(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	return source.FromCatalog(row.Type, row.URI)
}
