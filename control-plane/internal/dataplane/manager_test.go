package dataplane

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/alDuncanson/lum/control-plane/internal/events"
	lumv1 "github.com/alDuncanson/lum/control-plane/internal/gen/lum/v1"
)

// managerDeps builds injectable spawn/dial functions for Manager tests. Each
// spawn() starts a real, inert child process (so Supervisor.Stop/Exited
// behave exactly as in production) while every dial() connects to one fixed,
// test-controlled gRPC health server, decoupling process lifecycle from
// readiness state so both can be driven independently.
func managerDeps(t *testing.T, service lumv1.DataPlaneServer) (spawn func() (*Supervisor, error), dial func() (*Client, error), spawnCount *atomic.Int32) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "lumen")
	script := "#!/bin/sh\ntrap '' INT\ncat >/dev/null\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	listener := listenUnix(t)
	server := grpc.NewServer()
	lumv1.RegisterDataPlaneServer(server, service)
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

func readyHealth() *healthDataPlane {
	return &healthDataPlane{response: &lumv1.HealthResponse{
		Ready: true, ContractVersion: ContractVersion, State: lumv1.ReadinessState_READINESS_STATE_READY,
	}}
}

// searchableDataPlane adds a working Search to healthDataPlane so tests can
// drive a real RPC (not just Health) through the Manager without caring
// about search semantics themselves.
type searchableDataPlane struct {
	*healthDataPlane
}

func (searchableDataPlane) Search(context.Context, *lumv1.SearchRequest) (*lumv1.SearchResponse, error) {
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
			t.Fatalf("data plane was not shed within deadline; last state %q", health.State)
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
			t.Fatalf("data plane never became ready; last state %+v", health)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count = %d, want 1", got)
	}
}

func TestManagerConcurrentRealOpsCoalesceOntoOneRespawn(t *testing.T) {
	spawn, dial, spawnCount := managerDeps(t, searchableDataPlane{readyHealth()})
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

func TestManagerTreatsCrashLikeIdleShedAndRespawnsOnNextRequest(t *testing.T) {
	// A supervisor whose process has already exited (crashed) must be
	// treated the same as a deliberate shed: the next real request
	// respawns it rather than talking to a dead connection.
	crashedBin := filepath.Join(t.TempDir(), "lumen")
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

	spawn, dial, spawnCount := managerDeps(t, searchableDataPlane{readyHealth()})
	m := newTestManager(spawn, dial, 0)
	m.sup = crashedSup // simulate: manager still thinks lumen is running
	t.Cleanup(func() { _ = m.Close() })

	health, err := m.Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.State != StateIdle {
		t.Fatalf("state after detecting crash = %q, want idle (not yet respawning)", health.State)
	}

	if _, err := m.Search(context.Background(), "q", 10, ""); err != nil {
		t.Fatal(err)
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count after crash recovery = %d, want 1", got)
	}
}

func TestManagerCloseStopsRunningLumenAndPreventsRespawn(t *testing.T) {
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
	spawn, dial, _ := managerDeps(t, searchableDataPlane{readyHealth()})
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
	stuck := &healthDataPlane{response: &lumv1.HealthResponse{
		ContractVersion: ContractVersion, State: lumv1.ReadinessState_READINESS_STATE_STARTING,
	}}
	spawn, dial, _ := managerDeps(t, stuck)
	m := newTestManager(spawn, dial, 0)
	m.startupTimeout = 200 * time.Millisecond
	t.Cleanup(func() { _ = m.Close() })

	_, err := m.Search(context.Background(), "q", 10, "")
	if err == nil {
		t.Fatal("expected timeout error waiting for a data plane stuck starting")
	}
}
