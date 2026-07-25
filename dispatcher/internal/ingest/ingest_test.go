package ingest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alDuncanson/lum/dispatcher/internal/catalog"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
	"github.com/alDuncanson/lum/dispatcher/internal/source"
	"github.com/alDuncanson/lum/dispatcher/internal/worker"
)

// stubWorker is a minimal worker.Interface for exercising the
// ingest pipeline's event publishing without a real lum-worker process.
type stubWorker struct {
	failURIs      map[string]bool
	failDeleteIDs map[string]bool
}

type blockingDeleteWorker struct {
	stubWorker
	started chan string
	release <-chan struct{}
	err     error
}

func (s blockingDeleteWorker) DeleteDocument(ctx context.Context, documentID string) error {
	select {
	case s.started <- documentID:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.release:
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type changingSource struct{ content []byte }

func (changingSource) Type() string                                       { return "test" }
func (changingSource) Scan(context.Context) ([]source.DocumentRef, error) { return nil, nil }
func (s changingSource) Read(context.Context, source.DocumentRef) ([]byte, error) {
	return s.content, nil
}

func (stubWorker) Health(context.Context) (worker.HealthResult, error) {
	return worker.HealthResult{State: worker.StateReady}, nil
}
func (stubWorker) EnsureRunning() {}

func (s stubWorker) IngestBatch(_ context.Context, documents []worker.IngestBatchDocument) ([]worker.IngestBatchResult, error) {
	results := make([]worker.IngestBatchResult, len(documents))
	for index, doc := range documents {
		results[index].DocumentID = doc.DocumentID
		if s.failURIs[doc.URI] {
			results[index].Err = fmt.Errorf("stub: forced failure for %s", doc.URI)
			continue
		}
		results[index].ChunkCount = 1
	}
	return results, nil
}

func (s stubWorker) DeleteDocument(_ context.Context, documentID string) error {
	if s.failDeleteIDs[documentID] {
		return fmt.Errorf("stub: forced delete failure for %s", documentID)
	}
	return nil
}

func (stubWorker) Search(context.Context, string, uint32, string) ([]worker.SearchResult, error) {
	return nil, nil
}

func TestEnqueueScanDeduplicatesPendingSource(t *testing.T) {
	ing := queueOnlyIngestor(context.Background())

	ing.EnqueueScan(context.Background(), "source")
	ing.EnqueueScan(context.Background(), "source")
	if got := len(ing.scanOrder); got != 1 {
		t.Fatalf("pending scans = %d, want 1", got)
	}

	// Once a request starts planning, a new event must be allowed to queue
	// one follow-up scan so changes during an active scan are not lost.
	if _, ok := ing.nextScan(); !ok {
		t.Fatal("pending scan disappeared")
	}
	ing.EnqueueScan(context.Background(), "source")
	if got := len(ing.scanOrder); got != 1 {
		t.Fatalf("pending follow-up scans = %d, want 1", got)
	}
}

func TestInitialScanCanceledWaitDoesNotCancelAndSecondWaitJoins(t *testing.T) {
	ing := queueOnlyIngestor(context.Background()) // nil event bus is intentional
	ing.EnqueueInitialScan(context.Background(), "source")
	run, ok := ing.nextScan()
	if !ok || run.initial == nil {
		t.Fatal("initial scan was not associated with its queued run")
	}

	firstCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ing.WaitInitialScan(firstCtx, "source"); !errors.Is(err, context.Canceled) {
		t.Fatalf("first wait error = %v, want context canceled", err)
	}

	secondDone := make(chan error, 1)
	go func() { secondDone <- ing.WaitInitialScan(context.Background(), "source") }()
	select {
	case err := <-secondDone:
		t.Fatalf("second wait returned before scan completion: %v", err)
	default:
	}
	ing.finishScan(run)
	if err := <-secondDone; err != nil {
		t.Fatalf("second wait error = %v", err)
	}
}

func TestInitialScanCompletedAndEstablishedSourcesReturnImmediately(t *testing.T) {
	ing := queueOnlyIngestor(context.Background())
	if err := ing.WaitInitialScan(context.Background(), "established"); err != nil {
		t.Fatalf("established source wait error = %v", err)
	}
	ing.EnqueueInitialScan(context.Background(), "completed")
	run, ok := ing.nextScan()
	if !ok {
		t.Fatal("initial scan was not queued")
	}
	ing.finishScan(run)
	if err := ing.WaitInitialScan(context.Background(), "completed"); err != nil {
		t.Fatalf("completed source wait error = %v", err)
	}
}

func TestDeleteCompletesPendingInitialScanWaiter(t *testing.T) {
	ing := queueOnlyIngestor(context.Background())
	ing.EnqueueInitialScan(context.Background(), "source")

	_, attempt, owner := ing.beginDelete("source", "request")
	if !owner {
		t.Fatal("delete did not acquire gate")
	}
	if err := ing.WaitInitialScan(context.Background(), "source"); err == nil {
		t.Fatal("initial scan waiter returned nil after its pending scan was deleted")
	}
	ing.endDelete("source", attempt, nil)
}

func TestInitialScanWaiterReturnsWhenIngestorStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ing := queueOnlyIngestor(ctx)
	ing.EnqueueInitialScan(context.Background(), "source")
	cancel()

	if err := ing.WaitInitialScan(context.Background(), "source"); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context canceled", err)
	}
}

func TestEnqueueDebouncedScanResetsQuietWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ing := queueOnlyIngestor(ctx)
	ing.debounce = time.Hour

	ing.EnqueueDebouncedScan(context.Background(), "source")
	first := ing.debounced["source"]
	ing.EnqueueDebouncedScan(context.Background(), "source")
	second := ing.debounced["source"]
	if first == second {
		t.Fatal("debounce request was not replaced")
	}

	ing.fireDebounced("source", first)
	if got := len(ing.scanOrder); got != 0 {
		t.Fatalf("stale debounce callback queued %d scans", got)
	}
	ing.fireDebounced("source", second)
	if got := len(ing.scanOrder); got != 1 {
		t.Fatalf("current debounce callback queued %d scans, want 1", got)
	}
}

func TestPlanScanProducesDocumentJobsAndAuthoritativeDelete(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.md"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	created := time.Now().UTC()
	_, _, err = cat.AddSource(ctx, catalog.Source{
		ID: "source", Type: "localdir", URI: root, CreatedAt: created,
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := catalog.Document{
		ID: "stale", SourceID: "source", URI: filepath.Join(root, "gone.md"),
		ContentHash: "old", ChunkCount: 2, IngestedAt: created,
	}
	if err := cat.UpsertDocument(ctx, stale); err != nil {
		t.Fatal(err)
	}

	ing := &Ingestor{catalog: cat, jobs: make(chan documentJob, 8)}
	run := &scanRun{sourceID: "source", done: make(chan struct{})}
	if err := ing.planScan(ctx, run); err != nil {
		t.Fatal(err)
	}

	var upserts, deletes int
	for len(ing.jobs) > 0 {
		job := <-ing.jobs
		switch job.kind {
		case jobUpsert:
			upserts++
			if job.source == nil || job.ref.URI == "" || job.document.ID == "" {
				t.Fatalf("incomplete upsert job: %#v", job)
			}
		case jobDelete:
			deletes++
			if job.document.ID != stale.ID {
				t.Fatalf("deleted document = %q, want %q", job.document.ID, stale.ID)
			}
		}
	}
	if upserts != 2 || deletes != 1 {
		t.Fatalf("jobs: upserts=%d deletes=%d", upserts, deletes)
	}
}

func TestDocumentWorkerTerminatesRunAfterPlanningError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ing := &Ingestor{jobs: make(chan documentJob, 1)}
	run := &scanRun{sourceID: "source", requestID: "request", done: make(chan struct{})}
	wantErr := context.Canceled
	go ing.documentRunner(ctx)
	ing.jobs <- documentJob{kind: jobScanComplete, run: run, err: wantErr}

	select {
	case <-run.done:
		if run.err != wantErr {
			t.Fatalf("run error = %v, want %v", run.err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("document runner did not terminate failed run")
	}
}

func TestDocumentWorkerPersistsHashOfReadBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source")
	ing := &Ingestor{catalog: cat, dp: stubWorker{}, jobs: make(chan documentJob, 2)}
	run := &scanRun{sourceID: "source", requestID: "request", started: time.Now(), done: make(chan struct{})}
	content := []byte("bytes changed after scan")
	go ing.documentRunner(ctx)
	ing.jobs <- documentJob{
		kind: jobUpsert, run: run, source: changingSource{content: content},
		ref:      source.DocumentRef{URI: "/document", ContentHash: "scan-time-hash"},
		document: catalog.Document{ID: "document", SourceID: "source", URI: "/document"},
	}
	ing.jobs <- documentJob{kind: jobScanComplete, run: run}
	select {
	case <-run.done:
	case <-time.After(time.Second):
		t.Fatal("document run did not finish")
	}
	doc, err := cat.DocumentByURI(ctx, "source", "/document")
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(content))
	if doc.ContentHash != want {
		t.Fatalf("persisted hash = %q, want read-byte hash %q", doc.ContentHash, want)
	}
}

func TestDocumentFailurePersistsAttemptsAndCapsBackoff(t *testing.T) {
	ctx := context.Background()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	_, _, err = cat.AddSource(ctx, catalog.Source{
		ID: "source", Type: "localdir", URI: t.TempDir(), CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}

	ing := &Ingestor{catalog: cat, retryBase: 10 * time.Millisecond}
	run := &scanRun{sourceID: "source"}
	job := documentJob{run: run, document: catalog.Document{URI: "/failed.md"}}
	for range retryLimit + 1 {
		ing.failDocument(ctx, job, context.DeadlineExceeded)
	}
	if run.failed != retryLimit+1 {
		t.Fatalf("failed count = %d, want %d", run.failed, retryLimit+1)
	}
	wantBackoff := 10 * time.Millisecond << (retryLimit - 1)
	if run.retryAfter != wantBackoff {
		t.Fatalf("retry backoff = %s, want %s", run.retryAfter, wantBackoff)
	}
	failures, err := cat.IngestFailures(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 || failures[0].Attempts != retryLimit+1 {
		t.Fatalf("failures = %#v, want %d attempts", failures, retryLimit+1)
	}
}

func TestAcceptedScanSupersedesRetryTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ing := queueOnlyIngestor(ctx)
	run := &scanRun{
		sourceID: "source", requestID: "retry", retryAfter: time.Hour,
	}
	ing.replaceRetry(run)
	if ing.retries[run.sourceID] == nil {
		t.Fatal("retry timer was not scheduled")
	}

	ing.EnqueueScan(context.Background(), run.sourceID)
	if ing.retries[run.sourceID] != nil {
		t.Fatal("accepted scan did not supersede retry timer")
	}
	if len(ing.scanOrder) != 1 {
		t.Fatalf("pending scans = %d, want 1", len(ing.scanOrder))
	}

	// A failed run must not add another timer when a follow-up scan is
	// already pending, even when another run error was also observed.
	run.err = context.Canceled
	ing.replaceRetry(run)
	if ing.retries[run.sourceID] != nil {
		t.Fatal("retry timer scheduled alongside pending scan")
	}
}

func queueOnlyIngestor(ctx context.Context) *Ingestor {
	return &Ingestor{
		ctx: ctx, scanReady: make(chan struct{}, 1),
		pending: make(map[string]scanRequest), debounced: make(map[string]*debounceRequest),
		retries: make(map[string]*retryRequest), debounce: debounceWindow, retryBase: retryBaseDelay,
	}
}

// drainUntil consumes events from ch until one of kind is seen (returning
// the tally of every kind observed along the way) or the deadline passes.
func drainUntil(t *testing.T, ch <-chan events.Event, kind events.Kind, timeout time.Duration) map[events.Kind]int {
	t.Helper()
	seen := make(map[events.Kind]int)
	deadline := time.After(timeout)
	for {
		select {
		case e := <-ch:
			seen[e.Kind]++
			if e.Kind == kind {
				return seen
			}
		case <-deadline:
			t.Fatalf("did not observe %q within %s; events seen so far: %v", kind, timeout, seen)
			return nil
		}
	}
}

func TestScanPublishesFullDocumentLifecycleEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.md"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	if _, _, err := cat.AddSource(ctx, catalog.Source{
		ID: "source", Type: "localdir", URI: root, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	bus := events.NewBus(64)
	ch, _, unsubscribe := bus.Subscribe(64)
	defer unsubscribe()

	ing := New(ctx, cat, stubWorker{}, bus)
	ing.EnqueueScan(context.Background(), "source")

	seen := drainUntil(t, ch, events.KindScanFinished, 3*time.Second)
	for _, kind := range []events.Kind{
		events.KindScanStarted, events.KindDocumentQueued, events.KindDocumentReading,
		events.KindDocumentEmbedding, events.KindDocumentIngested, events.KindScanFinished,
	} {
		if seen[kind] == 0 {
			t.Errorf("expected at least one %q event, saw none (all seen: %v)", kind, seen)
		}
	}
}

func TestScanPublishesFailedAndDeletedEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := t.TempDir()
	failing := filepath.Join(root, "bad.md")
	deleting := filepath.Join(root, "gone.md")
	if err := os.WriteFile(failing, []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deleting, []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	if _, _, err := cat.AddSource(ctx, catalog.Source{
		ID: "source", Type: "localdir", URI: root, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	bus := events.NewBus(64)
	ch, _, unsubscribe := bus.Subscribe(64)
	defer unsubscribe()

	ing := New(ctx, cat, stubWorker{failURIs: map[string]bool{failing: true}}, bus)
	ing.EnqueueScan(context.Background(), "source")
	drainUntil(t, ch, events.KindScanFinished, 3*time.Second)

	// Remove the file that wasn't set up to fail, so the next authoritative
	// scan reconciles it as a deletion.
	if err := os.Remove(deleting); err != nil {
		t.Fatal(err)
	}
	ing.EnqueueScan(context.Background(), "source")
	seen := drainUntil(t, ch, events.KindScanFinished, 3*time.Second)
	for _, kind := range []events.Kind{events.KindDocumentFailed, events.KindDocumentDeleted} {
		if seen[kind] == 0 {
			t.Errorf("expected at least one %q event, saw none (all seen: %v)", kind, seen)
		}
	}
}

func TestBeginDeleteRemovesPendingRetryAndDebounce(t *testing.T) {
	ing := queueOnlyIngestor(context.Background())
	ing.EnqueueScan(context.Background(), "source")
	if len(ing.scanOrder) != 1 {
		t.Fatalf("pending scans = %d, want 1", len(ing.scanOrder))
	}

	_, attempt, owner := ing.beginDelete("source", "request")
	if !owner {
		t.Fatal("first delete did not acquire gate")
	}
	ing.endDelete("source", attempt, nil)
	if len(ing.scanOrder) != 0 || len(ing.pending) != 0 {
		t.Fatalf("scanOrder=%v pending=%v, want both empty after beginDelete", ing.scanOrder, ing.pending)
	}
	ing.WatchSource("source")
	ing.EnqueueScan(context.Background(), "source")
	if len(ing.watching) != 0 || len(ing.scanOrder) != 0 {
		t.Fatalf("successful deletion accepted stale startup work: watching=%v scanOrder=%v", ing.watching, ing.scanOrder)
	}
}

func newDeleteSourceTestCatalog(t *testing.T, sourceID string, documentIDs ...string) *catalog.Catalog {
	t.Helper()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	ctx := context.Background()
	if _, _, err := cat.AddSource(ctx, catalog.Source{
		ID: sourceID, Type: "localdir", URI: t.TempDir(), CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range documentIDs {
		if err := cat.UpsertDocument(ctx, catalog.Document{
			ID: id, SourceID: sourceID, URI: "/docs/" + id, ContentHash: "h", ChunkCount: 1, IngestedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return cat
}

func TestDeleteSourceRemovesVectorsCatalogRowsAndSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source", "doc-1", "doc-2")

	bus := events.NewBus(32)
	ch, _, unsubscribe := bus.Subscribe(32)
	defer unsubscribe()

	ing := New(ctx, cat, stubWorker{}, bus)
	if err := ing.DeleteSource(context.Background(), "source"); err != nil {
		t.Fatal(err)
	}

	if _, err := cat.GetSource(context.Background(), "source"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source after delete: got %v, want sql.ErrNoRows", err)
	}
	docs, err := cat.DocumentsBySource(context.Background(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 0 {
		t.Fatalf("documents after DeleteSource = %+v, want none", docs)
	}

	deleted := 0
drain:
	for {
		select {
		case e := <-ch:
			if e.Kind == events.KindDocumentDeleted {
				deleted++
			}
		default:
			break drain
		}
	}
	if deleted != 2 {
		t.Fatalf("document_deleted events published = %d, want 2", deleted)
	}
}

func TestDeleteSourceLeavesSourceInPlaceOnPartialFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source", "doc-ok", "doc-bad")

	ing := New(ctx, cat, stubWorker{failDeleteIDs: map[string]bool{"doc-bad": true}}, nil)
	if err := ing.DeleteSource(context.Background(), "source"); err == nil {
		t.Fatal("expected an error when one document fails to delete")
	}

	if _, err := cat.GetSource(context.Background(), "source"); err != nil {
		t.Fatalf("source should remain after a partial failure, got %v", err)
	}
	docs, err := cat.DocumentsBySource(context.Background(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 || docs[0].ID != "doc-bad" {
		t.Fatalf("documents after partial failure = %+v, want only doc-bad remaining", docs)
	}
}

func TestDeleteSourceStopsWatching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source")

	ing := New(ctx, cat, stubWorker{}, nil)
	ing.WatchSource("source")
	ing.mu.Lock()
	_, watching := ing.watching["source"]
	ing.mu.Unlock()
	if !watching {
		t.Fatal("WatchSource did not register a watch")
	}

	if err := ing.DeleteSource(context.Background(), "source"); err != nil {
		t.Fatal(err)
	}
	ing.mu.Lock()
	_, stillWatching := ing.watching["source"]
	ing.mu.Unlock()
	if stillWatching {
		t.Fatal("DeleteSource did not stop watching the deleted source")
	}
}

func TestDeleteSourceWaitsForActiveScanBeforeEnumeratingAndDeleting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source", "before-wait")
	started := make(chan string, 2)
	release := make(chan struct{})
	close(release)
	ing := New(ctx, cat, blockingDeleteWorker{started: started, release: release}, nil)
	active := &scanRun{sourceID: "source", done: make(chan struct{})}
	ing.mu.Lock()
	ing.sourceStateLocked("source").active = active
	ing.mu.Unlock()

	result := make(chan error, 1)
	go func() { result <- ing.DeleteSource(context.Background(), "source") }()
	select {
	case id := <-started:
		t.Fatalf("DeleteDocument(%q) started before the active scan completed", id)
	case <-time.After(100 * time.Millisecond):
	}
	// If enumeration happened before the wait, this document would not be
	// included in the deletion run and would prevent source removal.
	if err := cat.UpsertDocument(context.Background(), catalog.Document{
		ID: "during-wait", SourceID: "source", URI: "/docs/during-wait",
		ContentHash: "h", ChunkCount: 1, IngestedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	close(active.done)

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case id := <-started:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for deletes; saw %v", seen)
		}
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("DeleteSource did not finish")
	}
	if !seen["before-wait"] || !seen["during-wait"] {
		t.Fatalf("deleted documents = %v, want both pre-existing and scan-added documents", seen)
	}
}

func TestDeleteSourceOwnerCancellationKeepsGateAndDuplicateWaiter(t *testing.T) {
	ctx, cancelIngestor := context.WithCancel(context.Background())
	defer cancelIngestor()
	cat := newDeleteSourceTestCatalog(t, "source", "document")
	started := make(chan string, 1)
	release := make(chan struct{})
	ing := New(ctx, cat, blockingDeleteWorker{started: started, release: release}, nil)

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- ing.DeleteSource(ownerCtx, "source") }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("DeleteDocument did not start")
	}
	duplicateResult := make(chan error, 1)
	go func() { duplicateResult <- ing.DeleteSource(context.Background(), "source") }()
	cancelOwner()
	select {
	case err := <-ownerResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("owner error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled owner did not return")
	}

	ing.EnqueueScan(context.Background(), "source")
	ing.mu.Lock()
	deleting := ing.sourceStateLocked("source").deleting
	_, pending := ing.pending["source"]
	ing.mu.Unlock()
	if !deleting || pending {
		t.Fatalf("while blocked: deleting=%v pending scan=%v, want true/false", deleting, pending)
	}
	close(release)
	select {
	case err := <-duplicateResult:
		if err != nil {
			t.Fatalf("duplicate waiter error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate waiter did not receive final result")
	}
	if _, err := cat.GetSource(context.Background(), "source"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("source after background deletion: got %v, want sql.ErrNoRows", err)
	}
}

func TestWatchSourceWhileDeletingDoesNotRegisterWatcher(t *testing.T) {
	ing := queueOnlyIngestor(context.Background())
	ing.watching = make(map[string]context.CancelFunc)
	ing.mu.Lock()
	ing.sourceStateLocked("source").deleting = true
	ing.mu.Unlock()

	ing.WatchSource("source")
	ing.mu.Lock()
	_, watching := ing.watching["source"]
	ing.mu.Unlock()
	if watching {
		t.Fatal("WatchSource registered a watcher while deletion gate was held")
	}
}

func TestFailedDeleteRestoresWatcherAndReleasesScanGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source", "document")
	started := make(chan string, 1)
	release := make(chan struct{})
	ing := New(ctx, cat, blockingDeleteWorker{
		started: started, release: release, err: errors.New("delete failed"),
	}, nil)
	watchCanceled := make(chan struct{})
	ing.mu.Lock()
	ing.watching["source"] = func() { close(watchCanceled) }
	ing.mu.Unlock()

	result := make(chan error, 1)
	go func() { result <- ing.DeleteSource(context.Background(), "source") }()
	select {
	case <-watchCanceled:
	case <-time.After(time.Second):
		t.Fatal("existing watcher was not stopped")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("DeleteDocument did not start")
	}
	close(release)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("failed deletion returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("failed deletion did not return")
	}

	ing.mu.Lock()
	deleting := ing.sourceStateLocked("source").deleting
	_, watching := ing.watching["source"]
	ing.mu.Unlock()
	if deleting || !watching {
		t.Fatalf("after failure: deleting=%v watching=%v, want false/true", deleting, watching)
	}
	// Stop the planner so it cannot consume the accepted request before the
	// assertion below; cancellation does not change gate behavior.
	cancel()
	ing.EnqueueScan(context.Background(), "source")
	ing.mu.Lock()
	_, pending := ing.pending["source"]
	ing.mu.Unlock()
	if !pending {
		t.Fatal("scan gate remained held after failed deletion")
	}
}

func TestUnchangedPrefersContentHashOverFingerprint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing catalog.Document
		ref      source.DocumentRef
		want     bool
	}{{
		name:     "matching hash wins even when the fingerprint moved",
		existing: catalog.Document{ContentHash: "h", Fingerprint: "1:1"},
		ref:      source.DocumentRef{ContentHash: "h", Fingerprint: "2:2"},
		want:     true,
	}, {
		// The hash is authoritative: a matching fingerprint must not
		// override it, or a real edit that happened to preserve size and
		// mtime would be dismissed.
		name:     "differing hash loses to a matching fingerprint",
		existing: catalog.Document{ContentHash: "old", Fingerprint: "1:1"},
		ref:      source.DocumentRef{ContentHash: "new", Fingerprint: "1:1"},
		want:     false,
	}, {
		name:     "fingerprint decides when no hash was supplied",
		existing: catalog.Document{ContentHash: "h", Fingerprint: "1:1"},
		ref:      source.DocumentRef{Fingerprint: "1:1"},
		want:     true,
	}, {
		name:     "moved fingerprint never concludes unchanged",
		existing: catalog.Document{ContentHash: "h", Fingerprint: "1:1"},
		ref:      source.DocumentRef{Fingerprint: "2:2"},
		want:     false,
	}, {
		// Rows predating fingerprints have none stored. Two empty strings
		// must not read as a match, or such a document would never be
		// re-examined and could never acquire a fingerprint.
		name:     "two absent fingerprints do not match",
		existing: catalog.Document{ContentHash: "h"},
		ref:      source.DocumentRef{},
		want:     false,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := unchanged(tc.existing, tc.ref); got != tc.want {
				t.Errorf("unchanged() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A branch switch rewrites mtimes without changing bytes. That must cost one
// read and a fingerprint update — never a re-embed of the whole repository.
func TestTouchedButUnchangedDocumentRefreshesFingerprintWithoutReingesting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cat := newDeleteSourceTestCatalog(t, "source")
	content := []byte("unchanged bytes")
	hash := fmt.Sprintf("%x", sha256.Sum256(content))
	ingestedAt := time.Now().UTC().Truncate(time.Second)
	if err := cat.UpsertDocument(ctx, catalog.Document{
		ID: "document", SourceID: "source", URI: "/document",
		ContentHash: hash, Fingerprint: "10:100", ChunkCount: 7, IngestedAt: ingestedAt,
	}); err != nil {
		t.Fatal(err)
	}

	worker := &countingWorker{}
	ing := &Ingestor{catalog: cat, dp: worker, jobs: make(chan documentJob, 2)}
	run := &scanRun{sourceID: "source", requestID: "request", started: time.Now(), done: make(chan struct{})}
	existing, err := cat.DocumentByURI(ctx, "source", "/document")
	if err != nil {
		t.Fatal(err)
	}
	go ing.documentRunner(ctx)
	ing.jobs <- documentJob{
		kind: jobUpsert, run: run, source: changingSource{content: content},
		ref:      source.DocumentRef{URI: "/document", Fingerprint: "10:999"},
		document: existing,
	}
	ing.jobs <- documentJob{kind: jobScanComplete, run: run}
	select {
	case <-run.done:
	case <-time.After(2 * time.Second):
		t.Fatal("document run did not finish")
	}

	if n := worker.ingestBatches.Load(); n != 0 {
		t.Errorf("sent %d IngestBatch RPC(s), want 0 — identical content must not be re-embedded", n)
	}
	if run.ingested != 0 || run.unchanged != 1 {
		t.Errorf("run totals: ingested=%d unchanged=%d, want 0 and 1", run.ingested, run.unchanged)
	}
	doc, err := cat.DocumentByURI(ctx, "source", "/document")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Fingerprint != "10:999" {
		t.Errorf("fingerprint = %q, want the refreshed 10:999 so the next scan skips the read", doc.Fingerprint)
	}
	if doc.ChunkCount != 7 || doc.ContentHash != hash {
		t.Errorf("re-ingest bookkeeping was touched: chunks=%d hash=%q", doc.ChunkCount, doc.ContentHash)
	}
}

// countingWorker records whether the embedding path was reached at all.
type countingWorker struct {
	stubWorker
	ingestBatches atomic.Int32
}

func (c *countingWorker) IngestBatch(ctx context.Context, documents []worker.IngestBatchDocument) ([]worker.IngestBatchResult, error) {
	c.ingestBatches.Add(1)
	return c.stubWorker.IngestBatch(ctx, documents)
}
