package api

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alDuncanson/lum/dispatcher/internal/catalog"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
	"github.com/alDuncanson/lum/dispatcher/internal/ingest"
	"github.com/alDuncanson/lum/dispatcher/internal/worker"
)

// stubWorker is a minimal worker.Interface for exercising the HTTP
// layer without a real lum-worker process.
type stubWorker struct{}

func (stubWorker) Health(context.Context) (worker.HealthResult, error) {
	return worker.HealthResult{State: worker.StateReady}, nil
}
func (stubWorker) EnsureRunning() {}

func (stubWorker) IngestBatch(context.Context, []worker.IngestBatchDocument) ([]worker.IngestBatchResult, error) {
	return nil, nil
}
func (stubWorker) DeleteDocument(context.Context, string) error { return nil }
func (stubWorker) Search(context.Context, string, uint32, string) ([]worker.SearchResult, error) {
	return nil, nil
}

func newTestServer(t *testing.T, bus *events.Bus) *Server {
	t.Helper()
	server, _ := newTestServerWithCatalog(t, bus)
	return server
}

func newTestServerWithCatalog(t *testing.T, bus *events.Bus) (*Server, *catalog.Catalog) {
	t.Helper()
	return newTestServerWithWorker(t, bus, stubWorker{})
}

func newTestServerWithWorker(t *testing.T, bus *events.Bus, dp worker.Interface) (*Server, *catalog.Catalog) {
	t.Helper()
	cat, err := catalog.Open(filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ing := ingest.New(ctx, cat, dp, bus)
	return New(cat, dp, ing, bus), cat
}

// lineChannel streams body's lines to a channel so tests can bound each
// read with a select/timeout instead of risking an indefinite Scan().
func lineChannel(body io.Reader) <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(body)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	return lines
}

// nextEvent reads SSE lines until a complete "event:"/"data:" pair is
// found, ignoring heartbeat comment lines in between.
func nextEvent(t *testing.T, lines <-chan string, timeout time.Duration) (kind, data string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("event stream closed unexpectedly")
			}
			if after, found := strings.CutPrefix(line, "event: "); found {
				kind = after
			}
			if after, found := strings.CutPrefix(line, "data: "); found {
				return kind, after
			}
		case <-deadline:
			t.Fatal("timed out waiting for next SSE event")
			return "", ""
		}
	}
}

func TestEventsStreamReplaysBacklogThenSnapshotThenLiveEvents(t *testing.T) {
	bus := events.NewBus(8)
	bus.Publish(events.Event{Kind: events.KindScanStarted, SourceID: "backlog-source"})

	server := newTestServer(t, bus)
	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	lines := lineChannel(resp.Body)

	kind, data := nextEvent(t, lines, 2*time.Second)
	if kind != string(events.KindScanStarted) || !strings.Contains(data, "backlog-source") {
		t.Fatalf("first event = %q %q, want replayed backlog scan_started", kind, data)
	}

	kind, _ = nextEvent(t, lines, 2*time.Second)
	if kind != string(events.KindSnapshot) {
		t.Fatalf("second event kind = %q, want a fresh connect-time snapshot", kind)
	}

	bus.Publish(events.Event{Kind: events.KindDocumentIngested, DocumentID: "live-doc"})
	kind, data = nextEvent(t, lines, 2*time.Second)
	if kind != string(events.KindDocumentIngested) || !strings.Contains(data, "live-doc") {
		t.Fatalf("third event = %q %q, want live document_ingested", kind, data)
	}
}

func TestEventsStreamTypeFilterExcludesOtherKinds(t *testing.T) {
	bus := events.NewBus(0)
	server := newTestServer(t, bus)
	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/events?types=document_ingested", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	lines := lineChannel(resp.Body)

	// The connect-time snapshot is filtered out too (types is a strict
	// allowlist), so scan_started published now must also never surface.
	bus.Publish(events.Event{Kind: events.KindScanStarted, SourceID: "filtered-out"})
	bus.Publish(events.Event{Kind: events.KindDocumentIngested, DocumentID: "filtered-in"})

	kind, data := nextEvent(t, lines, 2*time.Second)
	if kind != string(events.KindDocumentIngested) || !strings.Contains(data, "filtered-in") {
		t.Fatalf("first observed event = %q %q, want only the filtered-in document_ingested", kind, data)
	}
}

func TestEventsStreamHeartbeatSignalsActivityWhileOpen(t *testing.T) {
	original := eventsHeartbeatInterval
	eventsHeartbeatInterval = 30 * time.Millisecond
	t.Cleanup(func() { eventsHeartbeatInterval = original })

	bus := events.NewBus(0)
	server := newTestServer(t, bus)
	var activityCount atomic.Int32
	httpServer := httptest.NewServer(server.Handler(func() { activityCount.Add(1) }))
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	lines := lineChannel(resp.Body)
	nextEvent(t, lines, 2*time.Second) // drain the connect-time snapshot

	// onRequest already fired once for the initial request; a stream that
	// stays open must keep signaling activity so the idle timer (#13)
	// doesn't shut lumd down underneath a connected observability client.
	deadline := time.Now().Add(2 * time.Second)
	for activityCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("onRequest called %d times in 2s, want at least 2 (initial + a heartbeat)", activityCount.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEventsStreamReportsServiceUnavailableWithoutBus(t *testing.T) {
	server := newTestServer(t, nil)
	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	resp, err := http.Get(httpServer.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}

func TestDeleteSourceRemovesSourceAndReturns200(t *testing.T) {
	server, cat := newTestServerWithCatalog(t, nil)
	ctx := context.Background()
	if _, _, err := cat.AddSource(ctx, catalog.Source{
		ID: "source-1", Type: "localdir", URI: t.TempDir(), CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/v1/sources/source-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if _, err := cat.GetSource(ctx, "source-1"); err == nil {
		t.Fatal("source still present in catalog after DELETE")
	}
}

func TestDeleteSourceReturns404ForUnknownID(t *testing.T) {
	server := newTestServer(t, nil)
	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	req, err := http.NewRequest(http.MethodDelete, httpServer.URL+"/v1/sources/does-not-exist", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// capturingSearchWorker records the arguments handleSearch passes
// through, so a test can confirm the ?source= query param actually
// reaches Worker.Search rather than being silently dropped.
type capturingSearchWorker struct {
	stubWorker
	mu       sync.Mutex
	sourceID string
}

func (c *capturingSearchWorker) Search(_ context.Context, _ string, _ uint32, sourceID string) ([]worker.SearchResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sourceID = sourceID
	return nil, nil
}

func TestSearchPassesSourceQueryParamThrough(t *testing.T) {
	dp := &capturingSearchWorker{}
	server, _ := newTestServerWithWorker(t, nil, dp)
	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	resp, err := http.Get(httpServer.URL + "/v1/search?q=wild+yeast&source=source-42")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	dp.mu.Lock()
	defer dp.mu.Unlock()
	if dp.sourceID != "source-42" {
		t.Fatalf("sourceID passed to Worker.Search = %q, want source-42", dp.sourceID)
	}
}

func TestShutdownRespondsBeforeSignalingAndIsIdempotent(t *testing.T) {
	server := newTestServer(t, nil)
	httpServer := httptest.NewServer(server.Handler(nil))
	t.Cleanup(httpServer.Close)

	resp, err := http.Post(httpServer.URL+"/v1/shutdown", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	select {
	case <-server.ShutdownRequested():
	case <-time.After(time.Second):
		t.Fatal("ShutdownRequested channel was never signaled")
	}

	// A second request must not block trying to signal an already-full
	// channel (nothing has drained it, mirroring a slow main loop).
	resp2, err := http.Post(httpServer.URL+"/v1/shutdown", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("second request status = %d, want 202", resp2.StatusCode)
	}
}
