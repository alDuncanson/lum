// Package config centralizes every tunable of the dispatcher.
//
// lum is local-only: the HTTP API defaults to loopback and the private
// worker hop uses a Unix socket under the data directory. Configuration
// comes from environment variables (LUM_*) with sensible defaults; `lum
// serve` also exposes the important ones as flags.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// maxSocketPathLen is the longest path a Unix domain socket address can
// carry on this platform. sun_path is a fixed-size array in
// sockaddr_un — 104 bytes on Darwin, 108 on Linux — and the path must be
// NUL-terminated inside it, so one byte is reserved. Deriving the number
// from x/sys/unix keeps it right on both rather than hardcoding one and
// being quietly wrong on the other.
var maxSocketPathLen = len(unix.RawSockaddrUnix{}.Path) - 1

const (
	// DefaultHTTPAddr is where lumd serves its REST API and where the
	// CLI expects to find it.
	DefaultHTTPAddr = "127.0.0.1:7420"
	// DefaultIdleTimeout keeps an interactive session warm without leaving
	// auto-started daemons running indefinitely.
	DefaultIdleTimeout = 15 * time.Minute
	// DefaultStartupTimeout bounds model download and initialization so a
	// broken detached daemon cannot hang every waiting client forever.
	DefaultStartupTimeout = 5 * time.Minute
	// DefaultEmbeddingModel preserves the original full-precision model.
	// "quantized" is available as an opt-in CPU-throughput tradeoff.
	DefaultEmbeddingModel = "standard"
	// DefaultWorkerIdleTimeout sheds the memory-heavy lum-worker child after
	// this long without an ingest/search RPC, independent of and shorter
	// than lumd's own DefaultIdleTimeout. lum-worker respawns lazily on the next
	// request that needs it.
	DefaultWorkerIdleTimeout = 5 * time.Minute
)

// Config holds the resolved settings for a lumd process.
type Config struct {
	// DataDir is the root for all persistent state: catalog.db (SQLite),
	// vectors/ (qdrant-edge index), models/ (embedding model cache).
	// Deleting this directory resets lum completely.
	DataDir string
	// HTTPAddr is the REST API listen address.
	HTTPAddr string
	// IdleTimeout stops the daemon after this long without an HTTP request.
	IdleTimeout time.Duration
	// WorkerIdleTimeout stops the supervised lum-worker process after this
	// long without an ingest/search RPC, independent of IdleTimeout. Health
	// checks and status polling do not count as activity, so monitoring
	// lum doesn't itself keep the worker warm.
	WorkerIdleTimeout time.Duration
	// StartupTimeout bounds on-demand daemon readiness waits.
	StartupTimeout time.Duration
	// WorkerPath optionally pins the lum-worker binary location. Empty means
	// auto-discover (next to the lum executable, then $PATH).
	WorkerPath string
	// EmbeddingModel selects the standard or quantized bge-small model.
	// Changing it requires clearing the existing catalog and vector index.
	EmbeddingModel string
}

// Load builds a Config from environment variables and defaults.
func Load() Config {
	return Config{
		DataDir:           envOr("LUM_DATA_DIR", filepath.Join(homeDir(), ".lum")),
		HTTPAddr:          envOr("LUM_HTTP_ADDR", DefaultHTTPAddr),
		IdleTimeout:       DefaultIdleTimeout,
		WorkerIdleTimeout: envDurationOr("LUM_WORKER_IDLE_TIMEOUT", DefaultWorkerIdleTimeout),
		StartupTimeout:    DefaultStartupTimeout,
		WorkerPath:        os.Getenv("LUM_WORKER_PATH"),
		EmbeddingModel:    envOr("LUM_EMBEDDING_MODEL", DefaultEmbeddingModel),
	}
}

// Validate rejects configuration that cannot possibly work, before
// anything is started. Both entry points call it: `lum serve` so it fails
// with one clear line instead of spawning a worker that exits, and the
// on-demand client spawn so a CLI command reports the problem immediately
// rather than starting a daemon that dies and then polling /v1/status for
// the full startup timeout.
func (c Config) Validate() error {
	if err := c.validateSocketPath(); err != nil {
		return err
	}
	if c.EmbeddingModel != "standard" && c.EmbeddingModel != "quantized" {
		return fmt.Errorf("invalid embedding model %q: must be standard or quantized", c.EmbeddingModel)
	}
	return nil
}

// validateSocketPath catches a data directory too deep to hold the worker
// socket. Without this the failure surfaces as the worker exiting with a
// bare "path must be shorter than SUN_LEN" in daemon.log, while the
// dispatcher reports the worker as idle — the state it also uses for a
// deliberate memory shed, so nothing points at the real cause.
func (c Config) validateSocketPath() error {
	socket := c.GRPCSocketPath()
	// Dial resolves the socket to an absolute path, so that is the length
	// the kernel actually sees; a relative DataDir is only ever longer.
	if abs, err := filepath.Abs(socket); err == nil {
		socket = abs
	}
	if len(socket) > maxSocketPathLen {
		return fmt.Errorf(
			"data directory path is too long: the worker socket %q needs %d bytes, but a Unix domain socket address holds at most %d on this platform; point LUM_DATA_DIR at a shorter path",
			socket, len(socket), maxSocketPathLen,
		)
	}
	return nil
}

// BaseURL is the HTTP API root used by CLI clients.
func (c Config) BaseURL() string {
	return "http://" + c.HTTPAddr
}

// CatalogPath is the SQLite database file.
func (c Config) CatalogPath() string {
	return filepath.Join(c.DataDir, "catalog.db")
}

// GRPCSocketPath is the private inter-plane transport. Keeping it under the
// data directory gives each lum instance its own endpoint and relies on the
// directory's owner-only permissions for access control.
func (c Config) GRPCSocketPath() string {
	return filepath.Join(c.DataDir, "lum-worker.sock")
}

func (c Config) DaemonLockPath() string {
	return filepath.Join(c.DataDir, "daemon.lock")
}

func (c Config) DaemonStartLockPath() string {
	return filepath.Join(c.DataDir, "daemon-start.lock")
}

func (c Config) DaemonLogPath() string {
	return filepath.Join(c.DataDir, "daemon.log")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		// No home directory (rare; containers). Fall back to CWD so we
		// still function rather than crashing at startup.
		return "."
	}
	return home
}
