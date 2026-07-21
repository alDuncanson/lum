package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
)

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
	go ing.documentWorker(ctx)
	ing.jobs <- documentJob{kind: jobScanComplete, run: run, err: wantErr}

	select {
	case <-run.done:
		if run.err != wantErr {
			t.Fatalf("run error = %v, want %v", run.err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("document worker did not terminate failed run")
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
