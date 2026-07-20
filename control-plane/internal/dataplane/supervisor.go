// Package dataplane manages the control plane's relationship with the
// Rust data plane (lumen): running the process and talking gRPC to it.
package dataplane

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Supervisor runs lumen as a child process for the lifetime of lumd.
//
// This is what makes `lum serve` the only command a user ever starts:
// the data plane is an implementation detail, not a second service to
// operate. Unexpected exits are reaped and logged; automatic respawn
// with backoff is a planned improvement (see docs/architecture.md).
type Supervisor struct {
	cmd  *exec.Cmd
	done chan struct{}
}

// Spawn locates the lumen binary, starts it pointed at our data dir and
// gRPC address, and returns once the process is running (readiness is
// verified separately by dialing gRPC; see Client.WaitReady).
//
// Deliberately exec.Command, NOT exec.CommandContext: CommandContext
// SIGKILLs the child the moment the context cancels, which on shutdown
// would race (and beat) our graceful Stop() and cost qdrant-edge its
// final flush. Lifetime is managed explicitly by Stop() instead.
func Spawn(explicitPath, dataDir, grpcAddr, embeddingModel string) (*Supervisor, error) {
	bin, err := findLumen(explicitPath)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(bin,
		"--grpc-addr", grpcAddr,
		"--data-dir", dataDir,
		"--embedding-model", embeddingModel,
	)
	// lumen logs to stderr; surface it through our own stderr so
	// `lum serve` shows one merged, timestamped stream.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting lumen (%s): %w", bin, err)
	}
	slog.Info("data plane spawned", "binary", bin, "pid", cmd.Process.Pid, "embedding_model", embeddingModel)

	s := &Supervisor{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		if err != nil {
			slog.Error("data plane exited", "pid", cmd.Process.Pid, "error", err)
		} else {
			slog.Info("data plane exited", "pid", cmd.Process.Pid)
		}
		close(s.done)
	}()
	return s, nil
}

// Stop terminates lumen gracefully (SIGINT lets qdrant-edge flush),
// escalating to SIGKILL after a timeout.
func (s *Supervisor) Stop() {
	if s.cmd.Process == nil {
		return
	}
	select {
	case <-s.done:
		return
	default:
	}

	_ = s.cmd.Process.Signal(os.Interrupt)
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		slog.Warn("data plane did not exit in time; killing")
		_ = s.cmd.Process.Kill()
		<-s.done
	}
	slog.Info("data plane stopped")
}

// findLumen resolves the lumen binary path, in priority order:
//  1. explicit path (LUM_LUMEN_PATH / --lumen flag),
//  2. next to the lum executable (how `make build` lays out bin/),
//  3. $PATH.
func findLumen(explicit string) (string, error) {
	if explicit != "" {
		if _, err := os.Stat(explicit); err != nil {
			return "", fmt.Errorf("lumen binary not found at %q: %w", explicit, err)
		}
		return explicit, nil
	}
	if self, err := os.Executable(); err == nil {
		sibling := filepath.Join(filepath.Dir(self), "lumen")
		if _, err := os.Stat(sibling); err == nil {
			return sibling, nil
		}
	}
	if fromPath, err := exec.LookPath("lumen"); err == nil {
		return fromPath, nil
	}
	return "", fmt.Errorf(
		"lumen binary not found: build it with `make build` (looked next to lum and in $PATH; override with LUM_LUMEN_PATH)")
}
