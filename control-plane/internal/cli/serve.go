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

	"github.com/alDuncanson/lum/control-plane/internal/api"
	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/config"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/ingest"
)

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

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	cat, err := catalog.Open(cfg.CatalogPath())
	if err != nil {
		return err
	}
	defer cat.Close()

	// The data plane is a child process, not a service the user runs.
	sup, err := dataplane.Spawn(cfg.LumenPath, cfg.DataDir, cfg.GRPCAddr, cfg.EmbeddingModel)
	if err != nil {
		return err
	}
	defer sup.Stop()

	dp, err := dataplane.Dial(cfg.GRPCAddr)
	if err != nil {
		return err
	}
	defer dp.Close()

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("binding HTTP API: %w", err)
	}
	defer listener.Close()

	// Do not start ingestion workers until the public API is guaranteed to
	// have its socket. In particular, a bind failure must start no scans.
	ingestor := ingest.New(daemonCtx, cat, dp)
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api.New(cat, dp, ingestor).Handler(),
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
		slog.Info("waiting for data plane", "addr", cfg.GRPCAddr)
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

	var runErr error
	select {
	case err := <-errCh:
		runErr = err
	case err := <-readyErrCh:
		if !(errors.Is(err, context.Canceled) && ctx.Err() != nil) {
			runErr = err
		}
	case <-ctx.Done():
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
