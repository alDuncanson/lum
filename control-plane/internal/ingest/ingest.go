// Package ingest is the event-driven heart of the control plane: it
// turns "a source changed" into catalog updates and data plane calls.
//
// A source scan is only the producer: it takes an authoritative snapshot,
// diffs it against the catalog, and emits document jobs. A single document
// worker reads, batches, embeds, and commits those jobs. Keeping documents as
// the execution unit gives retries and file watching a natural seam without
// introducing competing embedding workers.
package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
	"github.com/alDuncanson/lum/control-plane/internal/requestid"
	"github.com/alDuncanson/lum/control-plane/internal/source"
)

type scanRequest struct {
	sourceID  string
	requestID string
}

type documentJobKind uint8

const (
	jobUpsert documentJobKind = iota
	jobDelete
	jobScanComplete
)

type documentJob struct {
	kind     documentJobKind
	run      *scanRun
	source   source.Source
	ref      source.DocumentRef
	document catalog.Document
	err      error
}

type scanRun struct {
	sourceID  string
	requestID string
	started   time.Time
	done      chan struct{}

	ingested   int
	unchanged  int
	removed    int
	failed     int
	err        error
	retryAfter time.Duration
}

type pendingDocument struct {
	job         documentJob
	contentHash string
	input       dataplane.IngestBatchDocument
}

type debounceRequest struct {
	requestID string
	timer     *time.Timer
}

type retryRequest struct {
	timer *time.Timer
}

const (
	batchDocumentLimit = 128
	batchContentTarget = 4 * 1024 * 1024
	batchContentLimit  = 32 * 1024 * 1024
	debounceWindow     = time.Second
	retryLimit         = 3
	retryBaseDelay     = time.Second
)

var documentIDNamespace = uuid.MustParse("9c4064c4-672b-4aa6-b3ba-bf18f0b94670")

// Ingestor owns source reconciliation and document execution queues.
type Ingestor struct {
	ctx     context.Context
	catalog *catalog.Catalog
	dp      dataplane.DataPlane
	bus     *events.Bus

	scanReady chan struct{}
	jobs      chan documentJob

	mu             sync.Mutex
	pending        map[string]scanRequest
	scanOrder      []string
	debounced      map[string]*debounceRequest
	retries        map[string]*retryRequest
	watching       map[string]context.CancelFunc
	debounce       time.Duration
	retryBase      time.Duration
	watchFallback  time.Duration
	activeDocument string
	activeStage    string
}

// New creates an Ingestor and starts its planner and document worker; cancel
// ctx to stop both. bus may be nil, in which case no events are published.
func New(ctx context.Context, cat *catalog.Catalog, dp dataplane.DataPlane, bus *events.Bus) *Ingestor {
	ing := &Ingestor{
		ctx:           ctx,
		catalog:       cat,
		dp:            dp,
		bus:           bus,
		scanReady:     make(chan struct{}, 1),
		jobs:          make(chan documentJob, 256),
		pending:       make(map[string]scanRequest),
		debounced:     make(map[string]*debounceRequest),
		retries:       make(map[string]*retryRequest),
		watching:      make(map[string]context.CancelFunc),
		debounce:      debounceWindow,
		retryBase:     retryBaseDelay,
		watchFallback: 5 * time.Minute,
	}
	go ing.planner(ctx)
	go ing.documentWorker(ctx)
	go ing.stopDebouncers(ctx)
	return ing
}

// publish is a nil-safe wrapper so every call site can fire-and-forget.
func (i *Ingestor) publish(e events.Event) {
	if i.bus != nil {
		i.bus.Publish(e)
	}
}

// QueueDepth reports the source-level scan queue and the document-level
// job queue, for the periodic snapshot.
func (i *Ingestor) QueueDepth() (pendingScans, pendingDocuments int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.scanOrder), len(i.jobs)
}

// ActiveWork reports the document and pipeline stage the single document
// worker is currently handling, for the periodic snapshot. Both are empty
// when the worker is idle.
func (i *Ingestor) ActiveWork() (document, stage string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.activeDocument, i.activeStage
}

func (i *Ingestor) setActiveWork(document, stage string) {
	i.mu.Lock()
	i.activeDocument, i.activeStage = document, stage
	i.mu.Unlock()
}

// WatchSource starts live change detection when the source supports it.
// Watch failures fall back to periodic authoritative scans. The watch
// runs under its own cancelable context (child of the Ingestor's) so
// StopWatching can end it independently of the other sources — in
// particular, so DeleteSource doesn't leave a goroutine behind spamming
// failed-scan logs for a source_id that no longer exists.
func (i *Ingestor) WatchSource(sourceID string) {
	i.mu.Lock()
	if _, exists := i.watching[sourceID]; exists {
		i.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(i.ctx)
	i.watching[sourceID] = cancel
	i.mu.Unlock()
	go i.watchSource(ctx, sourceID)
}

// StopWatching ends a source's live-watch goroutine (and any periodic
// fallback scans it fell back to), if one is running. Safe to call even
// if none is.
func (i *Ingestor) StopWatching(sourceID string) {
	i.mu.Lock()
	cancel, exists := i.watching[sourceID]
	delete(i.watching, sourceID)
	i.mu.Unlock()
	if exists {
		cancel()
	}
}

// DeleteSource removes every document belonging to a source — its
// vectors via the data plane, then its catalog row — before removing the
// source itself, so ON DELETE CASCADE never races ahead of vector
// cleanup and orphans them as unreachable-but-still-searchable ghosts
// (#4). Blocks until every document is gone or ctx ends. If any document
// fails to delete, the source (and its remaining documents) are left in
// place for a retry, rather than leaving a half-deleted source with no
// way to finish cleanup.
func (i *Ingestor) DeleteSource(ctx context.Context, sourceID string) error {
	i.StopWatching(sourceID)
	i.cancelQueuedScan(sourceID)

	documents, err := i.catalog.DocumentsBySource(ctx, sourceID)
	if err != nil {
		return err
	}

	run := &scanRun{sourceID: sourceID, requestID: requestIDFrom(ctx), started: time.Now(), done: make(chan struct{})}
	for _, document := range documents {
		if !i.sendJob(ctx, documentJob{kind: jobDelete, run: run, document: document}) {
			return ctx.Err()
		}
	}
	if !i.sendJob(ctx, documentJob{kind: jobScanComplete, run: run}) {
		return ctx.Err()
	}
	select {
	case <-run.done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if run.err != nil {
		return run.err
	}
	if run.failed > 0 {
		return fmt.Errorf("%d of %d document(s) failed to delete; source not removed, retry the delete",
			run.failed, len(documents))
	}
	return i.catalog.DeleteSource(ctx, sourceID)
}

// cancelQueuedScan removes a source from the scan queue, its retry timer,
// and any pending debounce, so a delete doesn't race a scan that's about
// to start reconciling (and re-creating) the documents being removed. A
// scan already in progress is left to finish; it's a narrow, harmless
// race (see DeleteSource's doc comment).
func (i *Ingestor) cancelQueuedScan(sourceID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cancelRetryLocked(sourceID)
	if _, pending := i.pending[sourceID]; pending {
		delete(i.pending, sourceID)
		for index, id := range i.scanOrder {
			if id == sourceID {
				i.scanOrder = append(i.scanOrder[:index], i.scanOrder[index+1:]...)
				break
			}
		}
	}
	if request := i.debounced[sourceID]; request != nil {
		request.timer.Stop()
		delete(i.debounced, sourceID)
	}
}

func (i *Ingestor) watchSource(ctx context.Context, sourceID string) {
	src, err := i.resolveSource(ctx, sourceID)
	if err != nil {
		slog.Warn("cannot watch source; using periodic scans", "source", sourceID, "error", err)
		i.periodicScans(ctx, sourceID)
		return
	}
	watcher, ok := src.(source.Watcher)
	if !ok {
		return
	}
	changes, failures, err := watcher.Watch(ctx)
	if err != nil {
		slog.Warn("cannot watch source; using periodic scans", "source", sourceID, "error", err)
		i.periodicScans(ctx, sourceID)
		return
	}
	var fallback *time.Ticker
	var fallbackC <-chan time.Time
	defer func() {
		if fallback != nil {
			fallback.Stop()
		}
	}()
	startFallback := func(err error) {
		slog.Warn("filesystem watch degraded; using periodic scans", "source", sourceID, "error", err)
		i.EnqueueScan(ctx, sourceID)
		if fallback == nil {
			fallback = time.NewTicker(i.watchFallback)
			fallbackC = fallback.C
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-changes:
			if !ok {
				changes = nil
				startFallback(errors.New("filesystem watcher stopped"))
				continue
			}
			i.EnqueueDebouncedScan(ctx, sourceID)
		case err, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			startFallback(err)
		case <-fallbackC:
			i.EnqueueScan(ctx, sourceID)
		}
	}
}

func (i *Ingestor) periodicScans(ctx context.Context, sourceID string) {
	ticker := time.NewTicker(i.watchFallback)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.EnqueueScan(ctx, sourceID)
		}
	}
}

// EnqueueScan schedules an immediate authoritative scan. A source already
// waiting in the queue is coalesced into the pending request. Once planning
// starts it leaves the pending set, so a change arriving during a scan queues
// one follow-up reconciliation rather than being lost.
func (i *Ingestor) EnqueueScan(ctx context.Context, sourceID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cancelRetryLocked(sourceID)
	i.enqueueScanLocked(scanRequest{sourceID: sourceID, requestID: requestIDFrom(ctx)})
}

func (i *Ingestor) enqueueScanLocked(req scanRequest) {
	sourceID := req.sourceID
	if _, exists := i.pending[sourceID]; exists {
		return
	}
	i.pending[sourceID] = req
	i.scanOrder = append(i.scanOrder, sourceID)
	select {
	case i.scanReady <- struct{}{}:
	default:
	}
}

// EnqueueDebouncedScan coalesces noisy source-change notifications over a
// one-second quiet window. File watchers use this path; explicit API and
// startup scans remain immediate through EnqueueScan.
func (i *Ingestor) EnqueueDebouncedScan(ctx context.Context, sourceID string) {
	requestID := requestIDFrom(ctx)
	i.mu.Lock()
	if current := i.debounced[sourceID]; current != nil {
		current.timer.Stop()
	}
	request := &debounceRequest{requestID: requestID}
	request.timer = time.AfterFunc(i.debounce, func() {
		i.fireDebounced(sourceID, request)
	})
	i.debounced[sourceID] = request
	i.mu.Unlock()
}

func requestIDFrom(ctx context.Context) string {
	requestID := requestid.FromContext(ctx)
	if requestID == "" {
		_, requestID = requestid.New(ctx)
	}
	return requestID
}

func (i *Ingestor) fireDebounced(sourceID string, request *debounceRequest) {
	i.mu.Lock()
	if i.debounced[sourceID] != request {
		i.mu.Unlock()
		return
	}
	delete(i.debounced, sourceID)
	requestID := request.requestID
	i.mu.Unlock()

	if i.ctx.Err() == nil {
		i.EnqueueScan(requestid.WithValue(i.ctx, requestID), sourceID)
	}
}

func (i *Ingestor) stopDebouncers(ctx context.Context) {
	<-ctx.Done()
	i.mu.Lock()
	defer i.mu.Unlock()
	for sourceID, request := range i.debounced {
		request.timer.Stop()
		delete(i.debounced, sourceID)
	}
	for sourceID, request := range i.retries {
		request.timer.Stop()
		delete(i.retries, sourceID)
	}
}

func (i *Ingestor) planner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-i.scanReady:
		}

		for req, ok := i.nextScan(); ok; req, ok = i.nextScan() {
			run := &scanRun{
				sourceID: req.sourceID, requestID: req.requestID,
				started: time.Now(), done: make(chan struct{}),
			}
			i.publish(events.Event{
				Kind: events.KindScanStarted, RequestID: run.requestID, SourceID: run.sourceID,
			})
			scanCtx := requestid.WithValue(ctx, req.requestID)
			planErr := i.planScan(scanCtx, run)
			if !i.sendJob(ctx, documentJob{kind: jobScanComplete, run: run, err: planErr}) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-run.done:
			}
		}
	}
}

func (i *Ingestor) nextScan() (scanRequest, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.scanOrder) == 0 {
		return scanRequest{}, false
	}
	sourceID := i.scanOrder[0]
	i.scanOrder = i.scanOrder[1:]
	req := i.pending[sourceID]
	delete(i.pending, sourceID)
	return req, true
}

// planScan turns one authoritative source snapshot into document jobs. It
// queues deletes only after the complete snapshot has been obtained.
func (i *Ingestor) planScan(ctx context.Context, run *scanRun) error {
	src, err := i.resolveSource(ctx, run.sourceID)
	if err != nil {
		return err
	}
	refs, err := src.Scan(ctx)
	if err != nil {
		return err
	}

	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		seen[ref.URI] = true
		existing, err := i.catalog.DocumentByURI(ctx, run.sourceID, ref.URI)
		switch {
		case err == nil && existing.ContentHash == ref.ContentHash:
			if err := i.catalog.ClearIngestFailure(ctx, run.sourceID, ref.URI); err != nil {
				return err
			}
			run.unchanged++
			continue
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return err
		}

		doc := existing
		if errors.Is(err, sql.ErrNoRows) {
			doc = catalog.Document{
				ID:       uuid.NewSHA1(documentIDNamespace, []byte(run.sourceID+"\x00"+ref.URI)).String(),
				SourceID: run.sourceID,
				URI:      ref.URI,
			}
		}
		if !i.sendJob(ctx, documentJob{
			kind: jobUpsert, run: run, source: src, ref: ref, document: doc,
		}) {
			return ctx.Err()
		}
	}

	known, err := i.catalog.DocumentsBySource(ctx, run.sourceID)
	if err != nil {
		return err
	}
	knownURIs := make(map[string]bool, len(known))
	for _, doc := range known {
		knownURIs[doc.URI] = true
		if !seen[doc.URI] && !i.sendJob(ctx, documentJob{kind: jobDelete, run: run, document: doc}) {
			return ctx.Err()
		}
	}
	failures, err := i.catalog.IngestFailuresBySource(ctx, run.sourceID)
	if err != nil {
		return err
	}
	for _, failure := range failures {
		if !seen[failure.URI] && !knownURIs[failure.URI] {
			if err := i.catalog.ClearIngestFailure(ctx, run.sourceID, failure.URI); err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *Ingestor) sendJob(ctx context.Context, job documentJob) bool {
	select {
	case <-ctx.Done():
		return false
	case i.jobs <- job:
		if job.kind == jobUpsert || job.kind == jobDelete {
			i.publish(events.Event{
				Kind: events.KindDocumentQueued, RequestID: job.run.requestID,
				SourceID: job.run.sourceID, DocumentID: job.document.ID, URI: job.document.URI,
			})
		}
		return true
	}
}

func (i *Ingestor) documentWorker(ctx context.Context) {
	var pending []pendingDocument
	pendingBytes := 0

	flush := func() {
		if len(pending) == 0 {
			return
		}
		i.flushBatch(ctx, pending)
		pending = pending[:0]
		pendingBytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-i.jobs:
			if job.run.err != nil && job.kind != jobScanComplete {
				continue
			}
			switch job.kind {
			case jobUpsert:
				i.setActiveWork(job.document.URI, "reading")
				i.publish(events.Event{
					Kind: events.KindDocumentReading, RequestID: job.run.requestID,
					SourceID: job.run.sourceID, DocumentID: job.document.ID, URI: job.document.URI,
				})
				content, err := job.source.Read(ctx, job.ref)
				if err != nil {
					slog.Warn("read failed, skipping",
						"request_id", job.run.requestID, "uri", job.ref.URI, "error", err)
					i.failDocument(ctx, job, fmt.Errorf("read: %w", err))
					continue
				}
				if len(content) > batchContentLimit {
					err := fmt.Errorf("document exceeds 32 MiB ingest limit (%d bytes)", len(content))
					slog.Warn("document exceeds 32 MiB ingest limit, skipping",
						"request_id", job.run.requestID, "uri", job.ref.URI, "bytes", len(content))
					i.failDocument(ctx, job, err)
					continue
				}
				if len(pending) > 0 && (len(pending) >= batchDocumentLimit || pendingBytes+len(content) > batchContentTarget) {
					flush()
				}
				pending = append(pending, pendingDocument{
					job: job, contentHash: job.ref.ContentHash,
					input: dataplane.IngestBatchDocument{
						DocumentID: job.document.ID, SourceID: job.run.sourceID,
						URI: job.ref.URI, MimeType: job.ref.MimeType, Content: content,
					},
				})
				pendingBytes += len(content)
				if pendingBytes > batchContentTarget {
					flush()
				}
			case jobDelete:
				flush()
				i.setActiveWork(job.document.URI, "deleting")
				i.deleteDocument(ctx, job)
			case jobScanComplete:
				flush()
				if job.run.err == nil {
					job.run.err = job.err
				}
				i.finishScan(job.run)
				i.setActiveWork("", "")
			}
		}
	}
}

func (i *Ingestor) flushBatch(ctx context.Context, pending []pendingDocument) {
	inputs := make([]dataplane.IngestBatchDocument, len(pending))
	for index := range pending {
		inputs[index] = pending[index].input
	}
	run := pending[0].job.run
	batchCtx := requestid.WithValue(ctx, run.requestID)
	i.setActiveWork(fmt.Sprintf("%s (batch of %d)", pending[0].job.document.URI, len(pending)), "embedding")
	for _, item := range pending {
		i.publish(events.Event{
			Kind: events.KindDocumentEmbedding, RequestID: run.requestID,
			SourceID: run.sourceID, DocumentID: item.job.document.ID, URI: item.job.document.URI,
		})
	}
	results, err := i.dp.IngestBatch(batchCtx, inputs)
	if err != nil {
		slog.Warn("ingest batch failed, skipping documents",
			"request_id", run.requestID, "documents", len(pending), "error", err)
		for _, item := range pending {
			i.failDocument(batchCtx, item.job, err)
		}
		return
	}
	for index, result := range results {
		item := pending[index]
		if result.Err != nil {
			slog.Warn("ingest failed, skipping",
				"request_id", run.requestID, "uri", item.job.document.URI, "error", result.Err)
			i.failDocument(batchCtx, item.job, result.Err)
			continue
		}
		item.job.document.ContentHash = item.contentHash
		item.job.document.ChunkCount = result.ChunkCount
		item.job.document.IngestedAt = time.Now().UTC()
		if err := i.catalog.UpsertDocument(batchCtx, item.job.document); err != nil {
			run.err = err
			return
		}
		if err := i.catalog.ClearIngestFailure(batchCtx, run.sourceID, item.job.document.URI); err != nil {
			run.err = err
			return
		}
		run.ingested++
		i.publish(events.Event{
			Kind: events.KindDocumentIngested, RequestID: run.requestID,
			SourceID: run.sourceID, DocumentID: item.job.document.ID, URI: item.job.document.URI,
			ChunkCount: int(result.ChunkCount),
		})
	}
}

func (i *Ingestor) deleteDocument(ctx context.Context, job documentJob) {
	run := job.run
	jobCtx := requestid.WithValue(ctx, run.requestID)
	if err := i.dp.DeleteDocument(jobCtx, job.document.ID); err != nil {
		slog.Warn("vector delete failed",
			"request_id", run.requestID, "uri", job.document.URI, "error", err)
		i.failDocument(jobCtx, job, fmt.Errorf("delete: %w", err))
		return
	}
	if err := i.catalog.DeleteDocument(jobCtx, job.document.ID); err != nil {
		run.err = err
		return
	}
	if err := i.catalog.ClearIngestFailure(jobCtx, run.sourceID, job.document.URI); err != nil {
		run.err = err
		return
	}
	run.removed++
	i.publish(events.Event{
		Kind: events.KindDocumentDeleted, RequestID: run.requestID,
		SourceID: run.sourceID, DocumentID: job.document.ID, URI: job.document.URI,
	})
}

func (i *Ingestor) failDocument(ctx context.Context, job documentJob, failureErr error) {
	run := job.run
	run.failed++
	i.publish(events.Event{
		Kind: events.KindDocumentFailed, RequestID: run.requestID,
		SourceID: run.sourceID, DocumentID: job.document.ID, URI: job.document.URI,
		Error: failureErr.Error(),
	})
	attempts, err := i.catalog.RecordIngestFailure(ctx, catalog.IngestFailure{
		SourceID: run.sourceID,
		URI:      job.document.URI,
		Error:    failureErr.Error(),
		FailedAt: time.Now().UTC(),
	})
	if err != nil {
		run.err = err
		return
	}
	if attempts <= retryLimit {
		delay := i.retryBase << (attempts - 1)
		if delay > run.retryAfter {
			run.retryAfter = delay
		}
	}
}

func (i *Ingestor) finishScan(run *scanRun) {
	event := events.Event{
		Kind: events.KindScanFinished, RequestID: run.requestID, SourceID: run.sourceID,
		Ingested: run.ingested, Unchanged: run.unchanged, Removed: run.removed, Failed: run.failed,
		TookMS: time.Since(run.started).Milliseconds(),
	}
	if run.err != nil {
		event.Error = run.err.Error()
		slog.Error("scan failed", "request_id", run.requestID,
			"source", run.sourceID, "error", run.err)
	} else {
		slog.Info("scan complete",
			"request_id", run.requestID,
			"source", run.sourceID,
			"ingested", run.ingested,
			"unchanged", run.unchanged,
			"removed", run.removed,
			"failed", run.failed,
			"took", time.Since(run.started).Round(time.Millisecond),
		)
	}
	i.publish(event)
	i.replaceRetry(run)
	close(run.done)
}

func (i *Ingestor) replaceRetry(run *scanRun) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cancelRetryLocked(run.sourceID)
	if run.retryAfter == 0 {
		return
	}
	if _, pending := i.pending[run.sourceID]; pending {
		return
	}

	request := &retryRequest{}
	request.timer = time.AfterFunc(run.retryAfter, func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		if i.retries[run.sourceID] != request {
			return
		}
		delete(i.retries, run.sourceID)
		if i.ctx.Err() == nil {
			i.enqueueScanLocked(scanRequest{sourceID: run.sourceID, requestID: run.requestID})
		}
	})
	i.retries[run.sourceID] = request
}

func (i *Ingestor) cancelRetryLocked(sourceID string) {
	if request := i.retries[sourceID]; request != nil {
		request.timer.Stop()
		delete(i.retries, sourceID)
	}
}

// resolveSource loads a source row and reconstructs its implementation.
func (i *Ingestor) resolveSource(ctx context.Context, sourceID string) (source.Source, error) {
	row, err := i.catalog.GetSource(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	return source.FromCatalog(row.Type, row.URI)
}
