// Package events is lum's internal observability contract: a small
// in-memory pub/sub that system components publish to and any number of
// subscribers (the SSE endpoint, `lum top`, tests) consume from. The Bus
// carries one schema of events; there is no persistence, ring-buffered or
// otherwise durable — htop doesn't have history either, and neither does
// lum top.
package events

import (
	"sync"
	"time"
)

// Kind discriminates the one Event schema. Every event has Kind, Seq, and
// Time; the remaining fields are populated according to Kind and left
// zero otherwise, a tagged union shaped for easy JSON consumption from
// curl, not just Go.
type Kind string

const (
	// KindScanStarted/KindScanFinished bracket one source scan.
	KindScanStarted  Kind = "scan_started"
	KindScanFinished Kind = "scan_finished"

	// Per-document lifecycle, in order: queued, then either reading and
	// embedding lead to ingested, or either step leads to failed. Deletes
	// go straight from queued to deleted.
	KindDocumentQueued    Kind = "document_queued"
	KindDocumentReading   Kind = "document_reading"
	KindDocumentEmbedding Kind = "document_embedding"
	KindDocumentIngested  Kind = "document_ingested"
	KindDocumentFailed    Kind = "document_failed"
	KindDocumentDeleted   Kind = "document_deleted"

	// KindWorkerStateChanged fires on every lum-worker readiness transition
	// (ready, idle-shed, restarting, ...), observed from the periodic
	// snapshot loop polling worker.Manager.Health.
	KindWorkerStateChanged Kind = "worker_state_changed"

	// KindRPCCompleted covers whole-RPC latency for both transports: the
	// worker gRPC hop (Transport "grpc") and the public HTTP API
	// (Transport "http"). The dispatcher doesn't reach into lum-worker for
	// finer-grained visibility — from outside, the RPC's duration IS the
	// embedding phase for the batch it carried.
	KindRPCCompleted Kind = "rpc_completed"

	// KindSnapshot is a periodic point-in-time gauge reading, for
	// late-joining subscribers and anything that wants a heartbeat rather
	// than tracking discrete events itself.
	KindSnapshot Kind = "snapshot"
)

// AllKinds is every kind the bus publishes, so a client can discover what
// `?types=` accepts rather than guessing at strings. Keep it in step with
// the constants above; TestAllKindsIsComplete fails if it drifts.
var AllKinds = []Kind{
	KindScanStarted,
	KindScanFinished,
	KindDocumentQueued,
	KindDocumentReading,
	KindDocumentEmbedding,
	KindDocumentIngested,
	KindDocumentFailed,
	KindDocumentDeleted,
	KindWorkerStateChanged,
	KindRPCCompleted,
	KindSnapshot,
}

// Event is the one message shape published on the Bus.
type Event struct {
	Seq       uint64    `json:"seq"`
	Kind      Kind      `json:"kind"`
	Time      time.Time `json:"time"`
	RequestID string    `json:"request_id,omitempty"`

	// Document/source lifecycle events.
	SourceID   string `json:"source_id,omitempty"`
	DocumentID string `json:"document_id,omitempty"`
	URI        string `json:"uri,omitempty"`
	ChunkCount int    `json:"chunk_count,omitempty"`
	Error      string `json:"error,omitempty"`

	// scan_finished.
	Ingested  int `json:"ingested,omitempty"`
	Unchanged int `json:"unchanged,omitempty"`
	Removed   int `json:"removed,omitempty"`
	Failed    int `json:"failed,omitempty"`

	// rpc_completed.
	Transport string `json:"transport,omitempty"` // "http" or "grpc"
	Method    string `json:"method,omitempty"`
	Code      string `json:"code,omitempty"`
	TookMS    int64  `json:"took_ms,omitempty"`

	// worker_state_changed and snapshot.
	WorkerState string `json:"worker_state,omitempty"`
	FromState   string `json:"from_state,omitempty"`
	Detail      string `json:"detail,omitempty"`

	// snapshot.
	PendingScans     int    `json:"pending_scans,omitempty"`
	PendingDocuments int    `json:"pending_documents,omitempty"`
	ActiveDocument   string `json:"active_document,omitempty"`
	ActiveStage      string `json:"active_stage,omitempty"`
	Sources          int    `json:"sources,omitempty"`
	Documents        int    `json:"documents,omitempty"`
	Chunks           int    `json:"chunks,omitempty"`
	IngestFailures   int    `json:"ingest_failures,omitempty"`
}

// Bus is an in-memory pub/sub with a ring buffer for late joiners.
// Publish never blocks on a slow subscriber: this is best-effort
// observability, not a durable log, so a full subscriber channel drops
// the event rather than stalling the publisher (the ingest pipeline,
// most of the time).
type Bus struct {
	mu      sync.Mutex
	seq     uint64
	ring    []Event
	ringPos int
	ringLen int
	subs    map[int]chan Event
	nextSub int
}

// NewBus creates a Bus retaining up to ringSize recent events for replay
// to new subscribers. ringSize <= 0 disables replay (live events only).
func NewBus(ringSize int) *Bus {
	b := &Bus{subs: make(map[int]chan Event)}
	if ringSize > 0 {
		b.ring = make([]Event, ringSize)
	}
	return b
}

// Publish assigns the next sequence number and timestamp (if unset),
// records the event in the ring buffer, and fans it out to current
// subscribers.
func (b *Bus) Publish(e Event) {
	b.mu.Lock()
	b.seq++
	e.Seq = b.seq
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	if len(b.ring) > 0 {
		b.ring[b.ringPos] = e
		b.ringPos = (b.ringPos + 1) % len(b.ring)
		if b.ringLen < len(b.ring) {
			b.ringLen++
		}
	}
	subs := make([]chan Event, 0, len(b.subs))
	for _, ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// Subscribe registers a new subscriber and returns its channel, the
// current ring-buffer backlog (oldest first) for replay, and an
// unsubscribe func the caller must call when done listening.
func (b *Bus) Subscribe(bufferSize int) (ch <-chan Event, backlog []Event, unsubscribe func()) {
	b.mu.Lock()
	id := b.nextSub
	b.nextSub++
	subCh := make(chan Event, bufferSize)
	b.subs[id] = subCh
	backlog = b.replayLocked()
	b.mu.Unlock()

	return subCh, backlog, func() {
		b.mu.Lock()
		delete(b.subs, id)
		b.mu.Unlock()
	}
}

func (b *Bus) replayLocked() []Event {
	if b.ringLen == 0 {
		return nil
	}
	out := make([]Event, b.ringLen)
	start := (b.ringPos - b.ringLen + len(b.ring)) % len(b.ring)
	for i := 0; i < b.ringLen; i++ {
		out[i] = b.ring[(start+i)%len(b.ring)]
	}
	return out
}
