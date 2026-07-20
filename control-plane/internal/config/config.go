// Package config centralizes every tunable of the control plane.
//
// lum is local-only: both listen addresses default to loopback and
// nothing in the codebase should ever bind 0.0.0.0. Configuration comes
// from environment variables (LUM_*) with sensible defaults; `lum serve`
// also exposes the important ones as flags.
package config

import (
	"os"
	"path/filepath"
)

const (
	// DefaultHTTPAddr is where lumd serves its REST API and where the
	// CLI expects to find it.
	DefaultHTTPAddr = "127.0.0.1:7420"
	// DefaultGRPCAddr is where the data plane (lumen) serves gRPC.
	DefaultGRPCAddr = "127.0.0.1:7421"
	// DefaultEmbeddingModel preserves the original full-precision model.
	// "quantized" is available as an opt-in CPU-throughput tradeoff.
	DefaultEmbeddingModel = "standard"
)

// Config holds the resolved settings for a lumd process.
type Config struct {
	// DataDir is the root for all persistent state: catalog.db (SQLite),
	// vectors/ (qdrant-edge index), models/ (embedding model cache).
	// Deleting this directory resets lum completely.
	DataDir string
	// HTTPAddr is the REST API listen address.
	HTTPAddr string
	// GRPCAddr is the address lumen is told to listen on and lumd
	// connects to.
	GRPCAddr string
	// LumenPath optionally pins the lumen binary location. Empty means
	// auto-discover (next to the lum executable, then $PATH).
	LumenPath string
	// EmbeddingModel selects the standard or quantized bge-small model.
	// Changing it requires clearing the existing catalog and vector index.
	EmbeddingModel string
}

// Load builds a Config from environment variables and defaults.
func Load() Config {
	return Config{
		DataDir:        envOr("LUM_DATA_DIR", filepath.Join(homeDir(), ".lum")),
		HTTPAddr:       envOr("LUM_HTTP_ADDR", DefaultHTTPAddr),
		GRPCAddr:       envOr("LUM_GRPC_ADDR", DefaultGRPCAddr),
		LumenPath:      os.Getenv("LUM_LUMEN_PATH"),
		EmbeddingModel: envOr("LUM_EMBEDDING_MODEL", DefaultEmbeddingModel),
	}
}

// BaseURL is the HTTP API root used by CLI clients.
func (c Config) BaseURL() string {
	return "http://" + c.HTTPAddr
}

// CatalogPath is the SQLite database file.
func (c Config) CatalogPath() string {
	return filepath.Join(c.DataDir, "catalog.db")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
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
