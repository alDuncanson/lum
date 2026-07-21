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
	dp      *dataplane.Client

	scanReady chan struct{}
	jobs      chan documentJob

	mu            sync.Mutex
	pending       map[string]scanRequest
	scanOrder     []string
	debounced     map[string]*debounceRequest
	retries       map[string]*retryRequest
	watching      map[string]struct{}
	debounce      time.Duration
	retryBase     time.Duration
	watchFallback time.Duration
}

// New creates an Ingestor and starts its planner and document worker; cancel
// ctx to stop both.
func New(ctx context.Context, cat *catalog.Catalog, dp *dataplane.Client) *Ingestor {
	ing := &Ingestor{
		ctx:           ctx,
		catalog:       cat,
		dp:            dp,
		scanReady:     make(chan struct{}, 1),
		jobs:          make(chan documentJob, 256),
		pending:       make(map[string]scanRequest),
		debounced:     make(map[string]*debounceRequest),
		retries:       make(map[string]*retryRequest),
		watching:      make(map[string]struct{}),
		debounce:      debounceWindow,
		retryBase:     retryBaseDelay,
		watchFallback: 5 * time.Minute,
	}
	go ing.planner(ctx)
	go ing.documentWorker(ctx)
	go ing.stopDebouncers(ctx)
	return ing
}

// WatchSource starts live change detection when the source supports it.
// Watch failures fall back to periodic authoritative scans.
func (i *Ingestor) WatchSource(sourceID string) {
	i.mu.Lock()
	if _, exists := i.watching[sourceID]; exists {
		i.mu.Unlock()
		return
	}
	i.watching[sourceID] = struct{}{}
	i.mu.Unlock()
	go i.watchSource(sourceID)
}

func (i *Ingestor) watchSource(sourceID string) {
	src, err := i.resolveSource(i.ctx, sourceID)
	if err != nil {
		slog.Warn("cannot watch source; using periodic scans", "source", sourceID, "error", err)
		i.periodicScans(sourceID)
		return
	}
	watcher, ok := src.(source.Watcher)
	if !ok {
		return
	}
	changes, failures, err := watcher.Watch(i.ctx)
	if err != nil {
		slog.Warn("cannot watch source; using periodic scans", "source", sourceID, "error", err)
		i.periodicScans(sourceID)
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
		i.EnqueueScan(i.ctx, sourceID)
		if fallback == nil {
			fallback = time.NewTicker(i.watchFallback)
			fallbackC = fallback.C
		}
	}
	for {
		select {
		case <-i.ctx.Done():
			return
		case _, ok := <-changes:
			if !ok {
				changes = nil
				startFallback(errors.New("filesystem watcher stopped"))
				continue
			}
			i.EnqueueDebouncedScan(i.ctx, sourceID)
		case err, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			startFallback(err)
		case <-fallbackC:
			i.EnqueueScan(i.ctx, sourceID)
		}
	}
}

func (i *Ingestor) periodicScans(sourceID string) {
	ticker := time.NewTicker(i.watchFallback)
	defer ticker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			i.EnqueueScan(i.ctx, sourceID)
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
		existing, err := i.catalog.DocumentByURI(ctx, ref.URI)
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
						PreviousChunkCount: job.document.ChunkCount,
					},
				})
				pendingBytes += len(content)
				if pendingBytes > batchContentTarget {
					flush()
				}
			case jobDelete:
				flush()
				i.deleteDocument(ctx, job)
			case jobScanComplete:
				flush()
				if job.run.err == nil {
					job.run.err = job.err
				}
				i.finishScan(job.run)
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
	}
}

func (i *Ingestor) deleteDocument(ctx context.Context, job documentJob) {
	run := job.run
	jobCtx := requestid.WithValue(ctx, run.requestID)
	if err := i.dp.DeleteDocument(jobCtx, job.document.ID, job.document.ChunkCount); err != nil {
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
}

func (i *Ingestor) failDocument(ctx context.Context, job documentJob, failureErr error) {
	run := job.run
	run.failed++
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
	if run.err != nil {
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
