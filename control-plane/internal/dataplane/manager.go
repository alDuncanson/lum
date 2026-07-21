package dataplane

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DataPlane is what the rest of the control plane needs from the data
// plane: Client provides it directly (already running, externally owned);
// Manager provides it with idle shedding and lazy respawn layered on top.
// Health must be side-effect-free — status polling must never itself keep
// the data plane warm — so EnsureRunning is the one call sites that
// perform real work (add source, scan, search) use to signal "I actually
// need this."
type DataPlane interface {
	Health(ctx context.Context) (HealthResult, error)
	EnsureRunning()
	IngestBatch(ctx context.Context, documents []IngestBatchDocument) ([]IngestBatchResult, error)
	DeleteDocument(ctx context.Context, documentID string, chunkCount uint32) error
	Search(ctx context.Context, query string, limit uint32) ([]SearchResult, error)
}

// Manager owns lumen's lifecycle for as long as lumd runs: an idle timer
// stops it to reclaim the hundreds of MB the ONNX model and qdrant-edge
// hold resident, and any call that needs real work respawns it lazily. An
// unexpected crash is treated the same as a deliberate shed, since the
// data plane is stateless between calls either way (see supervisor.go).
type Manager struct {
	idleTimeout    time.Duration
	startupTimeout time.Duration
	spawn          func() (*Supervisor, error)
	dial           func() (*Client, error)

	mu           sync.Mutex
	sup          *Supervisor
	client       *Client
	spawning     bool
	stopped      bool
	lastActivity time.Time
	inFlight     int

	done chan struct{}
}

// NewManager wraps an already-spawned, already-dialed lumen. idleTimeout
// <= 0 disables shedding (lumen stays up for the process lifetime, as
// before this feature).
func NewManager(
	lumenPath, dataDir, socketPath, embeddingModel string,
	idleTimeout, startupTimeout time.Duration,
	sup *Supervisor, client *Client,
) *Manager {
	m := &Manager{
		idleTimeout: idleTimeout, startupTimeout: startupTimeout,
		spawn: func() (*Supervisor, error) { return Spawn(lumenPath, dataDir, socketPath, embeddingModel) },
		dial:  func() (*Client, error) { return Dial(socketPath) },
		sup:   sup, client: client, lastActivity: time.Now(),
		done: make(chan struct{}),
	}
	if idleTimeout > 0 {
		go m.idleLoop()
	}
	return m
}

// Close stops any running lumen and prevents further respawns. Safe to
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

// WaitReady blocks until lumen is ready, respawning it first if it was
// shed or has crashed. Intended for the daemon's startup handshake.
func (m *Manager) WaitReady(ctx context.Context) error {
	_, err := m.awaitReady(ctx)
	return err
}

// Health reports current state without side effects: it never triggers a
// respawn, so monitoring or polling /v1/status cannot itself keep the data
// plane warm. A shed or crashed lumen reports StateIdle (or StateStarting
// if a respawn triggered elsewhere is already in flight).
func (m *Manager) Health(ctx context.Context) (HealthResult, error) {
	m.mu.Lock()
	m.clearIfExitedLocked()
	client, spawning := m.client, m.spawning
	m.mu.Unlock()

	if client == nil {
		if spawning {
			return HealthResult{State: StateStarting, Detail: "data plane restarting after idle shed"}, nil
		}
		return HealthResult{State: StateIdle, Detail: "data plane shed while idle to save memory; will restart on next request"}, nil
	}
	return client.Health(ctx)
}

// EnsureRunning kicks off a respawn if lumen isn't running and isn't
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
	return client.IngestBatch(ctx, documents)
}

func (m *Manager) DeleteDocument(ctx context.Context, documentID string, chunkCount uint32) error {
	client, err := m.awaitReady(ctx)
	if err != nil {
		return err
	}
	m.beginOp()
	defer m.endOp()
	return client.DeleteDocument(ctx, documentID, chunkCount)
}

func (m *Manager) Search(ctx context.Context, query string, limit uint32) ([]SearchResult, error) {
	client, err := m.awaitReady(ctx)
	if err != nil {
		return nil, err
	}
	m.beginOp()
	defer m.endOp()
	return client.Search(ctx, query, limit)
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
				return nil, fmt.Errorf("data plane did not restart in time: %w", waitCtx.Err())
			case <-ticker.C:
			}
			m.mu.Lock()
			client = m.client
			m.mu.Unlock()
		}
	}

	readyCtx, cancel := context.WithTimeout(ctx, m.startupTimeout)
	defer cancel()
	if err := client.WaitReady(readyCtx); err != nil {
		return nil, fmt.Errorf("waiting for data plane readiness: %w", err)
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

// clearIfExitedLocked treats an unexpectedly dead lumen the same as a
// deliberate idle shed: the next request respawns it. Caller holds m.mu.
func (m *Manager) clearIfExitedLocked() {
	if m.sup != nil && m.sup.Exited() {
		if m.client != nil {
			_ = m.client.Close()
		}
		m.sup, m.client = nil, nil
	}
}

// triggerRespawnLocked starts a new lumen in the background if one isn't
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
		slog.Error("data plane respawn failed", "error", err)
		return // Health keeps reporting idle; the next request retries.
	}
	m.sup, m.client = sup, client
	m.lastActivity = time.Now()
}

// idleLoop periodically sheds lumen once idleTimeout has elapsed since the
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
	m.mu.Unlock()

	slog.Info("data plane idle timeout reached; shedding to reclaim memory", "timeout", m.idleTimeout)
	sup.Stop()
	_ = client.Close()
}
