package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/status"

	"github.com/alDuncanson/lum/dispatcher/internal/events"
	"github.com/alDuncanson/lum/dispatcher/internal/requestid"
)

// Worker is what the rest of the dispatcher needs from the data
// plane: Client provides it directly (already running, externally owned);
// Manager provides it with idle shedding and lazy respawn layered on top.
// Health must be side-effect-free — status polling must never itself keep
// the worker warm — so EnsureRunning is the one call sites that
// perform real work (add source, scan, search) use to signal "I actually
// need this."
type Interface interface {
	Health(ctx context.Context) (HealthResult, error)
	EnsureRunning()
	IngestBatch(ctx context.Context, documents []IngestBatchDocument) ([]IngestBatchResult, error)
	DeleteDocument(ctx context.Context, documentID string) error
	Search(ctx context.Context, query string, limit uint32, sourceID string) ([]SearchResult, error)
}

// Manager owns lum-worker's lifecycle for as long as lumd runs: an idle timer
// stops it to reclaim the hundreds of MB the ONNX model and qdrant-edge
// hold resident, and any call that needs real work respawns it lazily. An
// unexpected crash is treated the same as a deliberate shed, since the
// worker is stateless between calls either way (see supervisor.go).
type Manager struct {
	idleTimeout    time.Duration
	startupTimeout time.Duration
	spawn          func() (*Supervisor, error)
	dial           func() (*Client, error)
	bus            *events.Bus

	mu           sync.Mutex
	sup          *Supervisor
	client       *Client
	spawning     bool
	stopped      bool
	lastActivity time.Time
	inFlight     int
	// absence records why no worker is running, so Health can tell a
	// deliberate shed from a crash. Both recover identically; only the
	// explanation differs, and the explanation is the whole problem when
	// something is wrong.
	absence      absenceCause
	absenceError error

	// forward cancels the progress subscription tied to the current worker
	// process. Each respawn gets a new one, because the stream belongs to
	// the process, not to the Manager.
	forward context.CancelFunc

	done chan struct{}
}

// absenceCause explains why m.sup is nil.
type absenceCause uint8

const (
	// absenceNone: a worker is running, or none has been stopped yet.
	absenceNone absenceCause = iota
	// absenceShed: stopped on purpose by the idle timer.
	absenceShed
	// absenceExited: the process ended on its own, or never started.
	absenceExited
)

// NewManager wraps an already-spawned, already-dialed lum-worker. idleTimeout
// <= 0 disables shedding (lum-worker stays up for the process lifetime, as
// before this feature). bus may be nil, in which case no events are
// published.
func NewManager(
	workerPath, dataDir, socketPath, embeddingModel string,
	idleTimeout, startupTimeout time.Duration,
	sup *Supervisor, client *Client,
	bus *events.Bus,
) *Manager {
	m := &Manager{
		idleTimeout: idleTimeout, startupTimeout: startupTimeout,
		spawn: func() (*Supervisor, error) { return Spawn(workerPath, dataDir, socketPath, embeddingModel) },
		dial:  func() (*Client, error) { return Dial(socketPath) },
		bus:   bus,
		sup:   sup, client: client, lastActivity: time.Now(),
		done: make(chan struct{}),
	}
	if idleTimeout > 0 {
		go m.idleLoop()
	}
	m.subscribeProgress(client)
	return m
}

// subscribeProgress republishes the worker's progress onto the event bus.
//
// Re-established per worker process: the stream dies with the process, and a
// shed-then-respawn is a new process. Failure is ignored on purpose — a
// worker without the RPC (or one that drops the stream) costs progress
// updates, never correctness, and nothing downstream waits on them.
func (m *Manager) subscribeProgress(client *Client) {
	if m.bus == nil || client == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.forward = cancel
	go func() {
		stream, err := client.Events(ctx)
		if err != nil {
			// Not fatal and not worth retrying here: WaitForReady already
			// covers "not up yet", so an error means the peer has no such
			// RPC or the context ended.
			slog.Debug("worker progress unavailable", "error", err)
			return
		}
		// Ends when the worker exits or this subscription is cancelled. A
		// respawn gets a fresh one, so there is nothing to reconnect.
		for progress := range stream {
			m.bus.Publish(events.Event{
				Kind: events.KindWorkerProgress, RequestID: progress.RequestID,
				Phase: progress.Phase, Done: progress.Done, Total: progress.Total,
				Unit: progress.Unit, Detail: progress.Detail,
			})
		}
	}()
}

// stopProgressLocked ends the subscription belonging to a worker that is
// going away. Caller holds m.mu.
func (m *Manager) stopProgressLocked() {
	if m.forward != nil {
		m.forward()
		m.forward = nil
	}
}

// Close stops any running lum-worker and prevents further respawns. Safe to
// call once during daemon shutdown; safe to call concurrently with an
// in-flight respawn or idle shed.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return nil
	}
	m.stopped = true
	sup, client := m.sup, m.client
	m.sup, m.client = nil, nil
	m.stopProgressLocked()
	m.mu.Unlock()

	close(m.done)
	if sup != nil {
		sup.Stop()
	}
	if client != nil {
		return client.Close()
	}
	return nil
}

// WaitReady blocks until lum-worker is ready, respawning it first if it was
// shed or has crashed. Intended for the daemon's startup handshake.
func (m *Manager) WaitReady(ctx context.Context) error {
	_, err := m.awaitReady(ctx)
	return err
}

// Health reports current state without side effects: it never triggers a
// respawn, so monitoring or polling /v1/status cannot itself keep the data
// plane warm. A shed or crashed lum-worker reports StateIdle (or StateStarting
// if a respawn triggered elsewhere is already in flight).
func (m *Manager) Health(ctx context.Context) (HealthResult, error) {
	m.mu.Lock()
	m.clearIfExitedLocked()
	client, spawning := m.client, m.spawning
	absence, absenceError := m.absence, m.absenceError
	m.mu.Unlock()

	if client == nil {
		if spawning {
			return HealthResult{State: StateStarting, Detail: "worker starting"}, nil
		}
		if absence == absenceExited {
			detail := "worker is not running and did not stop on purpose"
			if absenceError != nil {
				detail = "worker " + absenceError.Error()
			}
			return HealthResult{State: StateCrashed, Detail: detail}, nil
		}
		return HealthResult{State: StateIdle, Detail: "worker shed while idle to save memory; will restart on next request"}, nil
	}
	return client.Health(ctx)
}

// EnsureRunning kicks off a respawn if lum-worker isn't running and isn't
// already being started. It returns immediately; callers observe progress
// through Health, the same way CLI on-demand daemon startup works (#13).
func (m *Manager) EnsureRunning() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clearIfExitedLocked()
	m.triggerRespawnLocked()
}

func (m *Manager) IngestBatch(ctx context.Context, documents []IngestBatchDocument) ([]IngestBatchResult, error) {
	client, err := m.awaitReady(ctx)
	if err != nil {
		return nil, err
	}
	m.beginOp()
	defer m.endOp()
	started := time.Now()
	results, err := client.IngestBatch(ctx, documents)
	m.recordRPC(ctx, "IngestBatch", started, err)
	return results, err
}

func (m *Manager) DeleteDocument(ctx context.Context, documentID string) error {
	client, err := m.awaitReady(ctx)
	if err != nil {
		return err
	}
	m.beginOp()
	defer m.endOp()
	started := time.Now()
	err = client.DeleteDocument(ctx, documentID)
	m.recordRPC(ctx, "DeleteDocument", started, err)
	return err
}

func (m *Manager) Search(ctx context.Context, query string, limit uint32, sourceID string) ([]SearchResult, error) {
	client, err := m.awaitReady(ctx)
	if err != nil {
		return nil, err
	}
	m.beginOp()
	defer m.endOp()
	started := time.Now()
	results, err := client.Search(ctx, query, limit, sourceID)
	m.recordRPC(ctx, "Search", started, err)
	return results, err
}

// recordRPC publishes whole-RPC latency for the worker hop. This
// stands in for a gRPC client interceptor: from the dispatcher's side
// of the hop, an IngestBatch call's duration IS the embedding phase for
// the batch it carried (see the ingest_started/embedding events).
func (m *Manager) recordRPC(ctx context.Context, method string, started time.Time, err error) {
	if m.bus == nil {
		return
	}
	m.bus.Publish(events.Event{
		Kind: events.KindRPCCompleted, RequestID: requestid.FromContext(ctx),
		Transport: "grpc", Method: method, Code: status.Code(err).String(), TookMS: time.Since(started).Milliseconds(),
	})
}

// awaitReady is the one path real work goes through: ensure a respawn is
// under way, wait for the process to exist, then wait for it to report
// ready. Bounded by startupTimeout so a broken respawn cannot hang a
// caller forever.
func (m *Manager) awaitReady(ctx context.Context) (*Client, error) {
	m.mu.Lock()
	m.clearIfExitedLocked()
	client := m.client
	if client == nil {
		m.triggerRespawnLocked()
	}
	m.mu.Unlock()

	if client == nil {
		waitCtx, cancel := context.WithTimeout(ctx, m.startupTimeout)
		defer cancel()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for client == nil {
			select {
			case <-waitCtx.Done():
				return nil, fmt.Errorf("worker did not restart in time: %w", waitCtx.Err())
			case <-ticker.C:
			}
			m.mu.Lock()
			client = m.client
			// A respawn that failed outright will never produce a client, so
			// surface its error now rather than polling out the timeout.
			if client == nil && !m.spawning && m.absence == absenceExited && m.absenceError != nil {
				err := m.absenceError
				m.mu.Unlock()
				return nil, fmt.Errorf("worker %w", err)
			}
			m.mu.Unlock()
		}
	}

	m.mu.Lock()
	sup := m.sup
	m.mu.Unlock()

	readyCtx, cancel := context.WithTimeout(ctx, m.startupTimeout)
	defer cancel()
	// A worker that exits during startup — a bad socket path, a corrupt
	// index, a missing model — leaves nothing listening on the socket, and
	// polling it for the full startup timeout is how a knowable failure
	// turned into five minutes of silence. Give up as soon as the process is
	// gone and report why it went.
	if sup != nil {
		stopWatching := make(chan struct{})
		defer close(stopWatching)
		go func() {
			select {
			case <-sup.Done():
				cancel()
			case <-stopWatching:
			}
		}()
	}
	if err := client.WaitReady(readyCtx); err != nil {
		if sup != nil && sup.Exited() {
			return nil, fmt.Errorf("worker %w", exitReason(sup.ExitError()))
		}
		return nil, fmt.Errorf("waiting for worker readiness: %w", err)
	}
	m.mu.Lock()
	m.lastActivity = time.Now()
	m.mu.Unlock()
	return client, nil
}

func (m *Manager) beginOp() {
	m.mu.Lock()
	m.inFlight++
	m.lastActivity = time.Now()
	m.mu.Unlock()
}

func (m *Manager) endOp() {
	m.mu.Lock()
	m.inFlight--
	m.lastActivity = time.Now()
	m.mu.Unlock()
}

// clearIfExitedLocked treats an unexpectedly dead lum-worker the same as a
// deliberate idle shed: the next request respawns it. Caller holds m.mu.
func (m *Manager) clearIfExitedLocked() {
	if m.sup != nil && m.sup.Exited() {
		if m.client != nil {
			_ = m.client.Close()
		}
		m.absence, m.absenceError = absenceExited, exitReason(m.sup.ExitError())
		m.sup, m.client = nil, nil
		m.stopProgressLocked()
	}
}

// exitReason turns a child exit status into something a person reads in
// `lum status`. A nil status still means the worker vanished unexpectedly:
// nothing asked it to stop, so a clean exit is its own kind of wrong.
func exitReason(err error) error {
	if err == nil {
		return errors.New("exited unexpectedly; see the daemon log")
	}
	return fmt.Errorf("exited: %w; see the daemon log", err)
}

// triggerRespawnLocked starts a new lum-worker in the background if one isn't
// already running or being started. Caller holds m.mu.
func (m *Manager) triggerRespawnLocked() {
	if m.stopped || m.sup != nil || m.spawning {
		return
	}
	m.spawning = true
	go m.respawn()
}

func (m *Manager) respawn() {
	sup, err := m.spawn()
	var client *Client
	if err == nil {
		client, err = m.dial()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.spawning = false
	if m.stopped {
		// Close() raced this respawn; tear down what we just started rather
		// than leaking it or handing it to a manager that's shutting down.
		if sup != nil {
			go sup.Stop()
		}
		if client != nil {
			_ = client.Close()
		}
		return
	}
	if err != nil {
		slog.Error("worker respawn failed", "error", err)
		// Report a failed start as a crash, not an idle shed, and keep the
		// reason. The next request still retries; meanwhile /v1/status says
		// what went wrong instead of claiming this saved memory on purpose.
		m.absence = absenceExited
		m.absenceError = fmt.Errorf("could not be started: %w", err)
		return
	}
	m.sup, m.client = sup, client
	m.absence, m.absenceError = absenceNone, nil
	m.lastActivity = time.Now()
	m.subscribeProgress(client)
}

// idleLoop periodically sheds lum-worker once idleTimeout has elapsed since the
// last real RPC. Health checks and status polling are deliberately not
// activity (see Health's doc comment), so this fires even under sustained
// monitoring traffic.
func (m *Manager) idleLoop() {
	interval := m.idleTimeout / 4
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.shedIfIdle()
		}
	}
}

func (m *Manager) shedIfIdle() {
	m.mu.Lock()
	m.clearIfExitedLocked()
	if m.stopped || m.sup == nil || m.inFlight > 0 || time.Since(m.lastActivity) < m.idleTimeout {
		m.mu.Unlock()
		return
	}
	sup, client := m.sup, m.client
	m.sup, m.client = nil, nil
	m.absence, m.absenceError = absenceShed, nil
	m.stopProgressLocked()
	m.mu.Unlock()

	slog.Info("worker idle timeout reached; shedding to reclaim memory", "timeout", m.idleTimeout)
	sup.Stop()
	_ = client.Close()
}
