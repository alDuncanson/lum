package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/alDuncanson/lum/control-plane/internal/events"
)

func TestApplySnapshotUpdatesGaugeFields(t *testing.T) {
	var m topModel
	m.apply(events.Event{
		Kind: events.KindSnapshot, DataPlaneState: "ready", Detail: "model=x",
		Sources: 2, Documents: 5, Chunks: 40, IngestFailures: 1,
		PendingScans: 3, PendingDocuments: 7, ActiveDocument: "a.md", ActiveStage: "embedding",
	})
	if m.dataPlaneState != "ready" || m.sources != 2 || m.documents != 5 || m.chunks != 40 || m.ingestFailures != 1 {
		t.Fatalf("gauge fields not applied from snapshot: %+v", m)
	}
	if m.pendingScans != 3 || m.pendingDocs != 7 {
		t.Fatalf("queue depth not applied: %+v", m)
	}
	if m.activeDocument != "a.md" || m.activeStage != "embedding" {
		t.Fatalf("active work not applied: %+v", m)
	}
}

// TestApplyLiveDocumentEventsUpdateActiveAndPendingImmediately guards
// against relying solely on the periodic (2s) snapshot for "queue:" and
// "active:": a batch that starts and finishes well inside that window
// must still be visible, driven by the live document lifecycle events
// instead.
func TestApplyLiveDocumentEventsUpdateActiveAndPendingImmediately(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindDocumentQueued, URI: "a.md"})
	if m.pendingDocs != 1 {
		t.Fatalf("pendingDocs after one queued event = %d, want 1", m.pendingDocs)
	}
	m.apply(events.Event{Kind: events.KindDocumentReading, URI: "a.md"})
	if m.activeDocument != "a.md" || m.activeStage != "reading" {
		t.Fatalf("active work after reading = %q/%q, want a.md/reading", m.activeDocument, m.activeStage)
	}
	m.apply(events.Event{Kind: events.KindDocumentEmbedding, URI: "a.md"})
	if m.activeDocument != "a.md" || m.activeStage != "embedding" {
		t.Fatalf("active work after embedding = %q/%q, want a.md/embedding", m.activeDocument, m.activeStage)
	}
	m.apply(events.Event{Kind: events.KindDocumentIngested, URI: "a.md", ChunkCount: 2})
	if m.pendingDocs != 0 {
		t.Fatalf("pendingDocs after resolving the only in-flight document = %d, want 0", m.pendingDocs)
	}
	if m.activeDocument != "" || m.activeStage != "" {
		t.Fatalf("active work after the pipeline goes idle = %q/%q, want cleared", m.activeDocument, m.activeStage)
	}
}

func TestApplyLiveDocumentCountStaysAccurateAcrossOverlappingDocuments(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindDocumentQueued, URI: "a.md"})
	m.apply(events.Event{Kind: events.KindDocumentQueued, URI: "b.md"})
	if m.pendingDocs != 2 {
		t.Fatalf("pendingDocs after two queued events = %d, want 2", m.pendingDocs)
	}
	m.apply(events.Event{Kind: events.KindDocumentReading, URI: "b.md"})
	m.apply(events.Event{Kind: events.KindDocumentIngested, URI: "a.md"})
	if m.pendingDocs != 1 {
		t.Fatalf("pendingDocs after resolving one of two = %d, want 1", m.pendingDocs)
	}
	if m.activeDocument != "b.md" {
		t.Fatalf("active work = %q, want b.md preserved while it's still in flight", m.activeDocument)
	}
	m.apply(events.Event{Kind: events.KindDocumentFailed, URI: "b.md", Error: "boom"})
	if m.pendingDocs != 0 {
		t.Fatalf("pendingDocs after resolving both = %d, want 0", m.pendingDocs)
	}
	if m.activeDocument != "" {
		t.Fatalf("active work = %q, want cleared once nothing is in flight", m.activeDocument)
	}
}

func TestApplyOnceLiveActivitySeenSnapshotNoLongerOverwritesPendingOrActive(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindDocumentQueued, URI: "a.md"})
	m.apply(events.Event{Kind: events.KindDocumentReading, URI: "a.md"})

	// A lagging periodic snapshot published before this document resolved
	// must not stomp the live-tracked state with stale numbers.
	m.apply(events.Event{
		Kind: events.KindSnapshot, PendingDocuments: 0, ActiveDocument: "", ActiveStage: "",
	})
	if m.pendingDocs != 1 {
		t.Fatalf("pendingDocs after a stale snapshot = %d, want the live count of 1 preserved", m.pendingDocs)
	}
	if m.activeDocument != "a.md" || m.activeStage != "reading" {
		t.Fatalf("active work after a stale snapshot = %q/%q, want a.md/reading preserved", m.activeDocument, m.activeStage)
	}
}

func TestApplyDataPlaneStateChangedUpdatesStateOutsideSnapshot(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindDataPlaneStateChanged, FromState: "ready", DataPlaneState: "idle", Detail: "shed"})
	if m.dataPlaneState != "idle" || m.dataPlaneDetail != "shed" {
		t.Fatalf("state change not applied: %+v", m)
	}
	if len(m.recent) != 1 || m.recent[0].Kind != events.KindDataPlaneStateChanged {
		t.Fatalf("state change should appear in recent log: %+v", m.recent)
	}
}

func TestApplyAccumulatesIngestedAndFailedTotals(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindDocumentIngested, DocumentID: "d1", ChunkCount: 3})
	m.apply(events.Event{Kind: events.KindDocumentIngested, DocumentID: "d2", ChunkCount: 2})
	m.apply(events.Event{Kind: events.KindDocumentFailed, URI: "/bad.md", Error: "boom"})

	if m.ingestedTotal != 2 || m.chunksTotal != 5 || m.failedTotal != 1 {
		t.Fatalf("totals = ingested=%d chunks=%d failed=%d, want 2/5/1", m.ingestedTotal, m.chunksTotal, m.failedTotal)
	}
	if m.lastError != "/bad.md: boom" {
		t.Fatalf("lastError = %q, want the failed document's error", m.lastError)
	}
}

func TestApplyTracksLastBatchAndScanTimings(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindRPCCompleted, Transport: "grpc", Method: "IngestBatch", TookMS: 42})
	m.apply(events.Event{Kind: events.KindRPCCompleted, Transport: "http", Method: "GET /v1/status", TookMS: 999})
	m.apply(events.Event{Kind: events.KindScanFinished, SourceID: "s1", Ingested: 4, TookMS: 77})

	if m.lastBatchTookMS != 42 {
		t.Fatalf("lastBatchTookMS = %d, want 42 (http RPCs must not overwrite it)", m.lastBatchTookMS)
	}
	if m.lastScanTookMS != 77 {
		t.Fatalf("lastScanTookMS = %d, want 77", m.lastScanTookMS)
	}
}

func TestApplyRecentEventLogIsBoundedAndKeepsNewest(t *testing.T) {
	var m topModel
	for i := range topRecentEventsShown + 5 {
		m.apply(events.Event{Kind: events.KindScanStarted, SourceID: string(rune('a' + i))})
	}
	if len(m.recent) != topRecentEventsShown {
		t.Fatalf("recent log length = %d, want %d (bounded)", len(m.recent), topRecentEventsShown)
	}
	last := m.recent[len(m.recent)-1]
	if last.SourceID != string(rune('a'+topRecentEventsShown+4)) {
		t.Fatalf("recent log dropped the oldest entries incorrectly, last = %+v", last)
	}
}

func TestApplyIgnoresRPCCompletedAndSnapshotInRecentLog(t *testing.T) {
	var m topModel
	m.apply(events.Event{Kind: events.KindRPCCompleted, Transport: "http", Method: "GET /v1/status"})
	m.apply(events.Event{Kind: events.KindSnapshot})
	if len(m.recent) != 0 {
		t.Fatalf("recent log = %+v, want rpc_completed/snapshot excluded from the scrolling log", m.recent)
	}
}

func TestViewRendersWithoutPanicBeforeAndAfterEvents(t *testing.T) {
	m := newTopModel(nil)
	if out := m.View(); !strings.Contains(out, "lum top") {
		t.Fatalf("initial view = %q, want it to render a header", out)
	}
	m.apply(events.Event{
		Kind: events.KindSnapshot, DataPlaneState: "ready", Sources: 1, Documents: 2, Chunks: 3,
	})
	m.apply(events.Event{Kind: events.KindDocumentFailed, URI: "/bad.md", Error: "boom", Time: time.Now()})
	out := m.View()
	if !strings.Contains(out, "ready") || !strings.Contains(out, "boom") {
		t.Fatalf("view after events = %q, want data plane state and last error rendered", out)
	}
}

func TestViewShowsWarmingUpInsteadOfAbsurdRateOnFreshStart(t *testing.T) {
	m := newTopModel(nil)
	m.apply(events.Event{Kind: events.KindDocumentIngested, DocumentID: "d1", ChunkCount: 1})
	out := m.View()
	if !strings.Contains(out, "warming up") {
		t.Fatalf("view = %q, want a warming-up placeholder before minRateWindow elapses", out)
	}
	if strings.Contains(out, "docs/min") {
		t.Fatalf("view = %q, want no docs/min rate computed from a near-zero elapsed window", out)
	}

	m.start = time.Now().Add(-minRateWindow - time.Second)
	out = m.View()
	if !strings.Contains(out, "docs/min") {
		t.Fatalf("view = %q, want a real rate once minRateWindow has elapsed", out)
	}
}

func TestUpdateQuitsOnStreamClosedAndKeyPress(t *testing.T) {
	m := newTopModel(nil)
	updated, cmd := m.Update(topStreamClosedMsg{})
	tm := updated.(topModel)
	if !tm.quitting || cmd == nil {
		t.Fatal("stream closed must set quitting and return a quit command")
	}
}
