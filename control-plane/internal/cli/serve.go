package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
// bottom in run(): data dir → catalog → spawn data plane → wait ready →
// ingest worker → HTTP API. Shutdown happens in reverse via defers.
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

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	cat, err := catalog.Open(cfg.CatalogPath())
	if err != nil {
		return err
	}
	defer cat.Close()

	// The data plane is a child process, not a service the user runs:
	// spawn it, then block until its gRPC port answers. First run can
	// take a while (embedding model download), hence the long timeout.
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

	slog.Info("waiting for data plane", "addr", cfg.GRPCAddr)
	if err := dp.WaitReady(ctx, 5*time.Minute); err != nil {
		return err
	}
	slog.Info("data plane ready")

	ingestor := ingest.New(ctx, cat, dp)

	// Rescan every known source on startup: catches changes made while
	// the daemon was down. Cheap when nothing changed (hash skips).
	if sources, err := cat.ListSources(ctx); err == nil {
		for _, s := range sources {
			ingestor.WatchSource(s.ID)
			ingestor.EnqueueScan(ctx, s.ID)
		}
	}

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api.New(cat, dp, ingestor).Handler(),
	}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("lum API listening", "addr", "http://"+cfg.HTTPAddr)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	}
}
