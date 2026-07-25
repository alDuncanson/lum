package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/alDuncanson/lum/dispatcher/internal/events"
	lumv1 "github.com/alDuncanson/lum/dispatcher/internal/gen/lum/v1"
)

// managerDeps builds injectable spawn/dial functions for Manager tests. Each
// spawn() starts a real, inert child process (so Supervisor.Stop/Exited
// behave exactly as in production) while every dial() connects to one fixed,
// test-controlled gRPC health server, decoupling process lifecycle from
// readiness state so both can be driven independently.
func managerDeps(t *testing.T, service lumv1.WorkerServer) (spawn func() (*Supervisor, error), dial func() (*Client, error), spawnCount *atomic.Int32) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "lum-worker")
	script := "#!/bin/sh\ntrap '' INT\ncat >/dev/null\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	listener := listenUnix(t)
	server := grpc.NewServer()
	lumv1.RegisterWorkerServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	var count atomic.Int32
	spawn = func() (*Supervisor, error) {
		count.Add(1)
		return Spawn(bin, t.TempDir(), listener.Addr().String(), "standard")
	}
	dial = func() (*Client, error) { return Dial(listener.Addr().String()) }
	return spawn, dial, &count
}

func readyHealth() *healthWorker {
	return &healthWorker{response: &lumv1.HealthResponse{
		Ready: true, ContractVersion: ContractVersion, State: lumv1.ReadinessState_READINESS_STATE_READY,
	}}
}

// searchableWorker adds a working Search to healthWorker so tests can
// drive a real RPC (not just Health) through the Manager without caring
// about search semantics themselves.
type searchableWorker struct {
	*healthWorker
}

func (searchableWorker) Search(context.Context, *lumv1.SearchRequest) (*lumv1.SearchResponse, error) {
	return &lumv1.SearchResponse{}, nil
}

func newTestManager(spawn func() (*Supervisor, error), dial func() (*Client, error), idleTimeout time.Duration) *Manager {
	return &Manager{
		idleTimeout: idleTimeout, startupTimeout: 3 * time.Second,
		spawn: spawn, dial: dial,
		lastActivity: time.Now(),
		done:         make(chan struct{}),
	}
}

func TestManagerShedsAfterIdleTimeoutAndReportsIdleState(t *testing.T) {
	spawn, dial, spawnCount := managerDeps(t, readyHealth())
	sup, err := spawn()
	if err != nil {
		t.Fatal(err)
	}
	client, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	m := newTestManager(spawn, dial, 100*time.Millisecond)
	m.sup, m.client = sup, client
	t.Cleanup(func() { _ = m.Close() })
	go m.idleLoop()

	deadline := time.Now().Add(2 * time.Second)
	for {
		health, err := m.Health(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if health.State == StateIdle {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker was not shed within deadline; last state %q", health.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count after shed = %d, want 1 (shedding must not itself spawn)", got)
	}
}

func TestManagerHealthNeverTriggersRespawn(t *testing.T) {
	spawn, dial, spawnCount := managerDeps(t, readyHealth())
	m := newTestManager(spawn, dial, 0) // idle shedding disabled; starts shed
	t.Cleanup(func() { _ = m.Close() })

	for range 5 {
		health, err := m.Health(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if health.State != StateIdle {
			t.Fatalf("state = %q, want idle", health.State)
		}
	}
	if got := spawnCount.Load(); got != 0 {
		t.Fatalf("spawn count after repeated Health() = %d, want 0", got)
	}
}

func TestManagerEnsureRunningRespawnsAndBecomesReady(t *testing.T) {
	spawn, dial, spawnCount := managerDeps(t, readyHealth())
	m := newTestManager(spawn, dial, 0)
	t.Cleanup(func() { _ = m.Close() })

	m.EnsureRunning()

	deadline := time.Now().Add(3 * time.Second)
	for {
		health, err := m.Health(context.Background())
		if err == nil && health.State == StateReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker never became ready; last state %+v", health)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count = %d, want 1", got)
	}
}

func TestManagerConcurrentRealOpsCoalesceOntoOneRespawn(t *testing.T) {
	spawn, dial, spawnCount := managerDeps(t, searchableWorker{readyHealth()})
	m := newTestManager(spawn, dial, 0)
	t.Cleanup(func() { _ = m.Close() })

	errs := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := m.Search(context.Background(), "q", 10, "")
			errs <- err
		}()
	}
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count for 3 concurrent Search calls = %d, want 1", got)
	}
}

func TestManagerReportsCrashDistinctlyAndRespawnsOnNextRequest(t *testing.T) {
	// A supervisor whose process has already exited must recover exactly
	// like a deliberate shed — the next real request respawns it rather
	// than talking to a dead connection — while reporting a state that says
	// what actually happened.
	crashedBin := filepath.Join(t.TempDir(), "lum-worker")
	if err := os.WriteFile(crashedBin, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	crashedSup, err := Spawn(crashedBin, t.TempDir(), "unused", "standard")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !crashedSup.Exited() {
		if time.Now().After(deadline) {
			t.Fatal("crashed supervisor never reaped")
		}
		time.Sleep(10 * time.Millisecond)
	}

	spawn, dial, spawnCount := managerDeps(t, searchableWorker{readyHealth()})
	m := newTestManager(spawn, dial, 0)
	m.sup = crashedSup // simulate: manager still thinks lum-worker is running
	t.Cleanup(func() { _ = m.Close() })

	health, err := m.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.State != StateCrashed {
		t.Fatalf("state after detecting crash = %q, want crashed (not idle: nothing asked it to stop)", health.State)
	}
	if !strings.Contains(health.Detail, "exit") {
		t.Errorf("detail = %q, want the exit status so the cause is not a guess", health.Detail)
	}

	if _, err := m.Search(context.Background(), "q", 10, ""); err != nil {
		t.Fatal(err)
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count after crash recovery = %d, want 1", got)
	}
}

func TestManagerCloseStopsRunningWorkerAndPreventsRespawn(t *testing.T) {
	spawn, dial, spawnCount := managerDeps(t, readyHealth())
	sup, err := spawn()
	if err != nil {
		t.Fatal(err)
	}
	client, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	m := newTestManager(spawn, dial, 0)
	m.sup, m.client = sup, client

	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !sup.Exited() {
		if time.Now().After(deadline) {
			t.Fatal("Close did not stop the supervised process")
		}
		time.Sleep(10 * time.Millisecond)
	}

	m.EnsureRunning()
	time.Sleep(100 * time.Millisecond)
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count after Close+EnsureRunning = %d, want 1 (no respawn once stopped)", got)
	}
	health, _ := m.Health(context.Background())
	if health.State != StateIdle {
		t.Fatalf("state after Close = %q, want idle", health.State)
	}
}

func TestManagerPublishesRPCCompletedEventsForRealOps(t *testing.T) {
	spawn, dial, _ := managerDeps(t, searchableWorker{readyHealth()})
	m := newTestManager(spawn, dial, 0)
	bus := events.NewBus(8)
	m.bus = bus
	t.Cleanup(func() { _ = m.Close() })

	ch, _, unsubscribe := bus.Subscribe(8)
	defer unsubscribe()

	if _, err := m.Search(context.Background(), "q", 10, ""); err != nil {
		t.Fatal(err)
	}

	select {
	case e := <-ch:
		if e.Kind != events.KindRPCCompleted || e.Transport != "grpc" || e.Method != "Search" || e.Code != "OK" {
			t.Fatalf("unexpected event: %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no rpc_completed event published for Search")
	}
}

func TestManagerAwaitReadyTimesOutWhenRespawnNeverBecomesReady(t *testing.T) {
	stuck := &healthWorker{response: &lumv1.HealthResponse{
		ContractVersion: ContractVersion, State: lumv1.ReadinessState_READINESS_STATE_STARTING,
	}}
	spawn, dial, _ := managerDeps(t, stuck)
	m := newTestManager(spawn, dial, 0)
	m.startupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { _ = m.Close() })

	_, err := m.Search(context.Background(), "q", 10, "")
	if err == nil {
		t.Fatal("expected timeout error waiting for a worker stuck starting")
	}
}

// The failure that motivated splitting crashed from idle: a worker that
// exits during startup leaves nothing listening on the socket, and polling
// it for the full startup timeout is how a knowable problem became five
// minutes of silence.
func TestAwaitReadyGivesUpWhenTheWorkerExitsDuringStartup(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "lum-worker")
	// Exits without ever binding the socket, like a bad --grpc-socket.
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	socket := filepath.Join(shortSocketDir(t), "w.sock")

	m := &Manager{
		startupTimeout: 30 * time.Second, // long enough that polling it out would hang the test
		spawn:          func() (*Supervisor, error) { return Spawn(bin, dataDir, socket, "standard") },
		dial:           func() (*Client, error) { return Dial(socket) },
		done:           make(chan struct{}),
	}
	t.Cleanup(func() { _ = m.Close() })

	started := time.Now()
	_, err := m.awaitReady(context.Background())
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("awaitReady() = nil error for a worker that exited during startup")
	}
	if elapsed > 10*time.Second {
		t.Errorf("took %s to notice the worker was gone; the point is not to wait out startupTimeout", elapsed)
	}
	if !strings.Contains(err.Error(), "exit") {
		t.Errorf("error = %q, want the exit status rather than a bare timeout", err)
	}

	// And the state that follows says it crashed, not that memory was saved.
	health, healthErr := m.Health(context.Background())
	if healthErr != nil {
		t.Fatal(healthErr)
	}
	if health.State != StateCrashed {
		t.Errorf("state = %q, want crashed", health.State)
	}
}

// A spawn that fails outright (missing binary) must report as crashed too,
// rather than as a deliberate shed.
func TestRespawnFailureReportsCrashedWithTheReason(t *testing.T) {
	m := &Manager{
		startupTimeout: 2 * time.Second,
		spawn:          func() (*Supervisor, error) { return nil, errors.New("lum-worker binary not found") },
		dial:           func() (*Client, error) { return nil, errors.New("unreachable") },
		done:           make(chan struct{}),
	}
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.awaitReady(context.Background()); err == nil {
		t.Fatal("awaitReady() = nil error when the worker cannot be spawned")
	}
	health, err := m.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.State != StateCrashed {
		t.Fatalf("state = %q, want crashed", health.State)
	}
	if !strings.Contains(health.Detail, "binary not found") {
		t.Errorf("detail = %q, want the spawn error", health.Detail)
	}
}

// A deliberate shed must keep saying so — the whole point is that the two
// are now distinguishable, not that everything became a crash.
func TestIdleShedStillReportsIdle(t *testing.T) {
	spawn, dial, _ := managerDeps(t, readyHealth())
	sup, err := spawn()
	if err != nil {
		t.Fatal(err)
	}
	client, err := dial()
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{
		idleTimeout: time.Nanosecond, startupTimeout: 2 * time.Second,
		spawn: spawn, dial: dial, sup: sup, client: client,
		lastActivity: time.Now().Add(-time.Hour), done: make(chan struct{}),
	}
	t.Cleanup(func() { _ = m.Close() })

	m.shedIfIdle()
	health, err := m.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.State != StateIdle {
		t.Fatalf("state after an idle shed = %q, want idle", health.State)
	}
}

// Unix socket paths are length-limited and t.TempDir() is already long on
// macOS; see config.Validate.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "lumw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
