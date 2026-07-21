package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"

	"github.com/alDuncanson/lum/control-plane/internal/api"
	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/config"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
	"github.com/alDuncanson/lum/control-plane/internal/ingest"
)

// eventBusRingSize bounds how many recent events a late subscriber (a
// fresh `lum top` or /v1/events connection) gets replayed on connect.
const eventBusRingSize = 512

// serveCmd runs the daemon. Startup order matters and reads top to
// bottom in run(): data dir → catalog → spawn data plane → ingest/API →
// asynchronous readiness and startup scans. Shutdown is via defers.
func serveCmd() *cobra.Command {
	var lumenPath string
	var embeddingModel string

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the lum daemon (control plane + supervised data plane)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.Load()
			if lumenPath != "" {
				cfg.LumenPath = lumenPath
			}
			if cmd.Flags().Changed("embedding-model") {
				cfg.EmbeddingModel = embeddingModel
			}
			return run(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVar(&lumenPath, "lumen", "",
		"path to the lumen binary (default: auto-discover)")
	cmd.Flags().StringVar(&embeddingModel, "embedding-model", config.DefaultEmbeddingModel,
		"embedding model: standard or quantized (changing requires re-ingest)")
	return cmd
}

func run(ctx context.Context, cfg config.Config) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if cfg.EmbeddingModel != "standard" && cfg.EmbeddingModel != "quantized" {
		return fmt.Errorf("invalid embedding model %q: must be standard or quantized", cfg.EmbeddingModel)
	}

	// Root context cancelled by SIGINT/SIGTERM; everything hangs off it.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	daemonCtx, cancelDaemon := context.WithCancel(ctx)
	defer cancelDaemon()

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	if err := os.Chmod(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("securing data dir: %w", err)
	}
	daemonLock, err := acquireDaemonLock(ctx, cfg.DaemonLockPath())
	if err != nil {
		return err
	}
	// This defer is registered before every owned resource so the lifetime
	// lock is released only after lumen, gRPC, catalog, and HTTP clean up.
	defer daemonLock.Close()

	cat, err := catalog.Open(cfg.CatalogPath())
	if err != nil {
		return err
	}
	defer cat.Close()

	// The data plane is a child process, not a service the user runs.
	socketPath := cfg.GRPCSocketPath()
	sup, err := dataplane.Spawn(cfg.LumenPath, cfg.DataDir, socketPath, cfg.EmbeddingModel)
	if err != nil {
		return err
	}
	rawClient, err := dataplane.Dial(socketPath)
	if err != nil {
		sup.Stop()
		return err
	}
	// The event bus is lum's one observability contract (#19): the ingest
	// pipeline, the data-plane RPC hop, and the HTTP API all publish to it;
	// the SSE endpoint and `lum top` are just renderers of it.
	bus := events.NewBus(eventBusRingSize)

	// Manager takes over lumen's lifecycle from here: idle shedding to
	// reclaim the model's memory, and lazy respawn on the next request
	// (see dataplane/manager.go).
	dp := dataplane.NewManager(
		cfg.LumenPath, cfg.DataDir, socketPath, cfg.EmbeddingModel,
		cfg.DataPlaneIdleTimeout, cfg.StartupTimeout,
		sup, rawClient, bus,
	)
	defer dp.Close()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("binding HTTP API: %w", err)
	}
	defer listener.Close()

	// Do not start ingestion workers until the public API is guaranteed to
	// have its socket. In particular, a bind failure must start no scans.
	ingestor := ingest.New(daemonCtx, cat, dp, bus)
	go runSnapshotLoop(daemonCtx, bus, cat, ingestor, dp)
	activityCh := make(chan struct{}, 1)
	recordActivity := func() {
		select {
		case activityCh <- struct{}{}:
		default:
		}
	}
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api.New(cat, dp, ingestor, bus).Handler(recordActivity),
	}
	defer server.Close()
	errCh := make(chan error, 1)
	slog.Info("lum API listening", "addr", "http://"+listener.Addr().String())
	go func() {
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	readyErrCh := make(chan error, 1)
	go func() {
		slog.Info("waiting for data plane", "socket", socketPath)
		if err := dp.WaitReady(daemonCtx); err != nil {
			select {
			case readyErrCh <- err:
			case <-daemonCtx.Done():
			}
			return
		}
		slog.Info("data plane ready")
		// Only readiness starts watches and startup reconciliation.
		if sources, err := cat.ListSources(daemonCtx); err != nil {
			select {
			case readyErrCh <- err:
			case <-daemonCtx.Done():
			}
		} else {
			for _, source := range sources {
				ingestor.WatchSource(source.ID)
				ingestor.EnqueueScan(daemonCtx, source.ID)
			}
		}
	}()

	idleTimer := time.NewTimer(cfg.IdleTimeout)
	defer idleTimer.Stop()
	var runErr error
waitForExit:
	for {
		select {
		case err := <-errCh:
			runErr = err
			break waitForExit
		case err := <-readyErrCh:
			if !(errors.Is(err, context.Canceled) && ctx.Err() != nil) {
				runErr = err
			}
			break waitForExit
		case <-ctx.Done():
			break waitForExit
		case <-activityCh:
			resetTimer(idleTimer, cfg.IdleTimeout)
		case <-idleTimer.C:
			// Prefer activity when it raced the deadline rather than shutting
			// down underneath a request that has just arrived.
			select {
			case <-activityCh:
				idleTimer.Reset(cfg.IdleTimeout)
			default:
				slog.Info("idle timeout reached", "timeout", cfg.IdleTimeout)
				break waitForExit
			}
		}
	}
	// Stop workers before closing the catalog or data-plane client. Every exit
	// path shares this ordering, including HTTP and contract errors.
	cancelDaemon()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	return runErr
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(timeout)
}

func acquireDaemonLock(ctx context.Context, path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening daemon lock: %w", err)
	}
	for {
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			return file, nil
		} else if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = file.Close()
			return nil, fmt.Errorf("locking daemon lifetime: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
