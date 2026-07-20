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
	"github.com/alDuncanson/lum/control-plane/internal/requestid"
	"github.com/alDuncanson/lum/control-plane/internal/source"
)

type scanRequest struct {
	sourceID  string
	requestID string
}

type pendingDocument struct {
	document    catalog.Document
	contentHash string
	input       dataplane.IngestBatchDocument
}

const (
	batchDocumentLimit = 128
	batchContentTarget = 4 * 1024 * 1024
	batchContentLimit  = 32 * 1024 * 1024
)

var documentIDNamespace = uuid.MustParse("9c4064c4-672b-4aa6-b3ba-bf18f0b94670")

// Ingestor owns the scan queue and executes scans.
type Ingestor struct {
	catalog *catalog.Catalog
	dp      *dataplane.Client
	queue   chan scanRequest
}

// New creates an Ingestor and starts its worker; cancel ctx to stop.
func New(ctx context.Context, cat *catalog.Catalog, dp *dataplane.Client) *Ingestor {
	ing := &Ingestor{
		catalog: cat,
		dp:      dp,
		queue:   make(chan scanRequest, 64),
	}
	go ing.worker(ctx)
	return ing
}

// EnqueueScan schedules a scan of the given source. Non-blocking; if
// the queue is full the scan is dropped with a warning (the next manual
// or startup scan will catch up — scans are idempotent).
func (i *Ingestor) EnqueueScan(ctx context.Context, sourceID string) {
	requestID := requestid.FromContext(ctx)
	if requestID == "" {
		_, requestID = requestid.New(ctx)
	}
	select {
	case i.queue <- scanRequest{sourceID: sourceID, requestID: requestID}:
	default:
		slog.Warn("scan queue full, dropping scan request",
			"request_id", requestID, "source", sourceID)
	}
}

func (i *Ingestor) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-i.queue:
			scanCtx := requestid.WithValue(ctx, req.requestID)
			if err := i.scanSource(scanCtx, req.sourceID); err != nil {
				slog.Error("scan failed", "request_id", req.requestID,
					"source", req.sourceID, "error", err)
			}
		}
	}
}

// scanSource performs one full reconciliation of a source against the
// catalog and vector index. Idempotent: running it twice in a row does
// no extra work the second time (hashes match, everything is skipped).
func (i *Ingestor) scanSource(ctx context.Context, sourceID string) error {
	started := time.Now()
	requestID := requestid.FromContext(ctx)

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
	var pending []pendingDocument
	pendingBytes := 0
	flushBatch := func() error {
		if len(pending) == 0 {
			return nil
		}
		inputs := make([]dataplane.IngestBatchDocument, len(pending))
		for index := range pending {
			inputs[index] = pending[index].input
		}
		results, err := i.dp.IngestBatch(ctx, inputs)
		if err != nil {
			slog.Warn("ingest batch failed, skipping documents",
				"request_id", requestID, "documents", len(pending), "error", err)
			failed += len(pending)
			pending = pending[:0]
			pendingBytes = 0
			return nil
		}
		for index, result := range results {
			item := pending[index]
			if result.Err != nil {
				slog.Warn("ingest failed, skipping",
					"request_id", requestID, "uri", item.document.URI, "error", result.Err)
				failed++
				continue
			}
			item.document.ContentHash = item.contentHash
			item.document.ChunkCount = result.ChunkCount
			item.document.IngestedAt = time.Now().UTC()
			if err := i.catalog.UpsertDocument(ctx, item.document); err != nil {
				return err
			}
			ingested++
		}
		pending = pending[:0]
		pendingBytes = 0
		return nil
	}

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
			doc = catalog.Document{
				ID:       uuid.NewSHA1(documentIDNamespace, []byte(sourceID+"\x00"+ref.URI)).String(),
				SourceID: sourceID,
				URI:      ref.URI,
			}
		}

		content, err := src.Read(ctx, ref)
		if err != nil {
			slog.Warn("read failed, skipping",
				"request_id", requestID, "uri", ref.URI, "error", err)
			failed++
			continue
		}

		if len(content) > batchContentLimit {
			slog.Warn("document exceeds 32 MiB ingest limit, skipping",
				"request_id", requestID, "uri", ref.URI, "bytes", len(content))
			failed++
			continue
		}
		if len(pending) > 0 && (len(pending) >= batchDocumentLimit || pendingBytes+len(content) > batchContentTarget) {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		pending = append(pending, pendingDocument{
			document:    doc,
			contentHash: ref.ContentHash,
			input: dataplane.IngestBatchDocument{
				DocumentID:         doc.ID,
				SourceID:           sourceID,
				URI:                ref.URI,
				MimeType:           ref.MimeType,
				Content:            content,
				PreviousChunkCount: doc.ChunkCount,
			},
		})
		pendingBytes += len(content)
		if pendingBytes > batchContentTarget {
			// Oversized-but-supported documents travel alone rather than
			// preventing the following small documents from batching.
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}
	if err := flushBatch(); err != nil {
		return err
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
			slog.Warn("vector delete failed",
				"request_id", requestID, "uri", doc.URI, "error", err)
			continue // keep the row; next scan retries the delete
		}
		if err := i.catalog.DeleteDocument(ctx, doc.ID); err != nil {
			return err
		}
		removed++
	}

	slog.Info("scan complete",
		"request_id", requestID,
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
