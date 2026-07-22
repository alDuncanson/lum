package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alDuncanson/lum/control-plane/internal/config"
	"github.com/alDuncanson/lum/control-plane/internal/events"
)

func TestConcurrentClientsSpawnDaemonOnce(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	_ = reserved.Close()

	cfg := config.Config{DataDir: t.TempDir(), HTTPAddr: addr, StartupTimeout: 2 * time.Second}
	var spawnCount atomic.Int32
	var server *http.Server
	t.Cleanup(func() {
		if server != nil {
			_ = server.Close()
		}
	})
	spawn := func(config.Config) error {
		spawnCount.Add(1)
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]string{"data_plane": "ready"})
		})
		mux.HandleFunc("GET /v1/sources", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "[]")
		})
		server = &http.Server{Handler: mux}
		go func() { _ = server.Serve(listener) }()
		return nil
	}
	newClient := func() *Client {
		return &Client{base: cfg.BaseURL(), cfg: cfg, httpClient: http.DefaultClient, spawn: spawn}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := newClient().ListSources(context.Background())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("spawn count = %d, want 1", got)
	}
}

func TestServiceUnavailableWaitsAndReplaysRequestBody(t *testing.T) {
	var ready atomic.Bool
	var addCalls atomic.Int32
	var bodiesMu sync.Mutex
	var bodies []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		state := "starting"
		if ready.Load() {
			state = "ready"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"data_plane": state})
	})
	mux.HandleFunc("POST /v1/sources", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodiesMu.Lock()
		bodies = append(bodies, string(body))
		bodiesMu.Unlock()
		if addCalls.Add(1) == 1 {
			ready.Store(true)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"data plane is starting"}`)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"source":{"id":"source","type":"localdir","uri":"/docs"},"created":true}`)
	})
	server := &http.Server{Handler: mux}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	cfg := config.Config{
		DataDir: t.TempDir(), HTTPAddr: listener.Addr().String(), StartupTimeout: 2 * time.Second,
	}
	client := &Client{
		base: cfg.BaseURL(), cfg: cfg, httpClient: http.DefaultClient,
		spawn: func(config.Config) error {
			t.Fatal("spawn called while daemon was already running")
			return nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.AddSource(ctx, "/docs"); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatalf("request bodies were not replayed: %#v", bodies)
	}
}

func TestStartupFailureTimesOutWithLogPath(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	_ = reserved.Close()
	cfg := config.Config{
		DataDir: t.TempDir(), HTTPAddr: addr, StartupTimeout: 50 * time.Millisecond,
	}
	client := &Client{
		base: cfg.BaseURL(), cfg: cfg, httpClient: http.DefaultClient,
		spawn: func(config.Config) error { return nil },
	}
	_, err = client.ListSources(context.Background())
	if err == nil || !strings.Contains(err.Error(), cfg.DaemonLogPath()) {
		t.Fatalf("error = %v, want bounded startup failure with log path", err)
	}
}

func TestEventsParsesSSEFramesInOrderWithoutSpawning(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "event: scan_started\ndata: {\"seq\":1,\"kind\":\"scan_started\",\"source_id\":\"abc\"}\n\n")
		flusher.Flush()
		fmt.Fprint(w, ": heartbeat\n\n")
		flusher.Flush()
		fmt.Fprint(w, "event: document_ingested\ndata: {\"seq\":2,\"kind\":\"document_ingested\",\"document_id\":\"doc1\"}\n\n")
		flusher.Flush()
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	cfg := config.Config{DataDir: t.TempDir()}
	client := &Client{
		base: server.URL, cfg: cfg, httpClient: http.DefaultClient,
		spawn: func(config.Config) error {
			t.Fatal("spawn called even though the daemon was already reachable")
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, err := client.Events(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	first := <-ch
	if first.Kind != events.KindScanStarted || first.SourceID != "abc" {
		t.Fatalf("first event = %+v, want scan_started/abc", first)
	}
	second := <-ch
	if second.Kind != events.KindDocumentIngested || second.DocumentID != "doc1" {
		t.Fatalf("second event = %+v, want document_ingested/doc1", second)
	}
}

func TestEventsTriggersDaemonSpawnOnConnectionRefused(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	_ = reserved.Close()

	cfg := config.Config{DataDir: t.TempDir(), HTTPAddr: addr, StartupTimeout: 2 * time.Second}
	var server *http.Server
	t.Cleanup(func() {
		if server != nil {
			_ = server.Close()
		}
	})
	spawn := func(config.Config) error {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]string{"data_plane": "ready"})
		})
		mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, _ *http.Request) {
			flusher := w.(http.Flusher)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "event: snapshot\ndata: {\"seq\":1,\"kind\":\"snapshot\",\"sources\":3}\n\n")
			flusher.Flush()
		})
		server = &http.Server{Handler: mux}
		go func() { _ = server.Serve(listener) }()
		return nil
	}
	client := &Client{base: cfg.BaseURL(), cfg: cfg, httpClient: http.DefaultClient, spawn: spawn}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, err := client.Events(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	e := <-ch
	if e.Kind != events.KindSnapshot || e.Sources != 3 {
		t.Fatalf("event = %+v, want a snapshot with sources=3", e)
	}
}

func TestEventsAppliesTypesFilterAsQueryParam(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	cfg := config.Config{DataDir: t.TempDir()}
	client := &Client{
		base: server.URL, cfg: cfg, httpClient: http.DefaultClient,
		spawn: func(config.Config) error {
			t.Fatal("spawn called even though the daemon was already reachable")
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := client.Events(ctx, []string{"document_ingested", "document_failed"})
	if err != nil {
		t.Fatal(err)
	}
	for range ch {
	} // drain until the (empty) stream closes

	if want := "types=document_ingested%2Cdocument_failed"; gotQuery != want {
		t.Fatalf("query = %q, want %q", gotQuery, want)
	}
}

func TestStopReturnsErrNoDaemonRunningWhenNothingIsListening(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserved.Addr().String()
	_ = reserved.Close()

	cfg := config.Config{DataDir: t.TempDir(), HTTPAddr: addr}
	client := &Client{
		base: cfg.BaseURL(), cfg: cfg, httpClient: http.DefaultClient,
		spawn: func(config.Config) error {
			t.Fatal("Stop must never trigger the on-demand auto-spawn (#13)")
			return nil
		},
	}

	if err := client.Stop(context.Background()); !errors.Is(err, ErrNoDaemonRunning) {
		t.Fatalf("err = %v, want ErrNoDaemonRunning", err)
	}
}

// TestStopWaitsForTheDaemonLockToActuallyRelease reproduces, by hand, a
// real race found while testing this feature live: the HTTP port can
// stop answering well before the process has actually exited (listener
// Close() runs before dp.Close()/cat.Close() in the shutdown sequence),
// so Stop must confirm the daemon.lock flock is released -- the same
// authoritative signal the on-demand daemon spawn (#13) already relies
// on -- not just that the port went quiet.
func TestStopWaitsForTheDaemonLockToActuallyRelease(t *testing.T) {
	cfg := config.Config{DataDir: t.TempDir()}

	// Simulate the real daemon: it holds daemon.lock for its entire
	// lifetime and only releases it once fully shut down.
	lockFile, err := os.OpenFile(cfg.DaemonLockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})

	const shutdownDelay = 150 * time.Millisecond
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/shutdown", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		// The HTTP response lands immediately; the lock (standing in for
		// the rest of the real shutdown sequence) releases later.
		go func() {
			time.Sleep(shutdownDelay)
			_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
			close(released)
		}()
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	cfg.HTTPAddr = listener.Addr().String()
	client := &Client{
		base: cfg.BaseURL(), cfg: cfg, httpClient: http.DefaultClient,
		spawn: func(config.Config) error {
			t.Fatal("Stop must never trigger the on-demand auto-spawn (#13)")
			return nil
		},
	}

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Stop(ctx); err != nil {
		t.Fatalf("Stop returned %v, want nil once the daemon lock is released", err)
	}
	if elapsed := time.Since(started); elapsed < shutdownDelay {
		t.Fatalf("Stop returned after %v, want it to wait out the %v lock release", elapsed, shutdownDelay)
	}

	// Wait for the background goroutine to fully finish (not just for its
	// Flock call to land) before touching lockFile ourselves, so closing
	// it here can't race with that goroutine's own use of the fd.
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("background lock-release goroutine never finished")
	}
	_ = lockFile.Close()
}
