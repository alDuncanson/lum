package apiclient

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alDuncanson/lum/control-plane/internal/config"
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
