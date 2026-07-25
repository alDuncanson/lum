package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alDuncanson/lum/dispatcher/internal/config"
	"github.com/alDuncanson/lum/dispatcher/internal/worker"
)

const daemonPollInterval = 250 * time.Millisecond

func (c *Client) ensureDaemon(ctx context.Context) error {
	if !isLoopbackAddress(c.cfg.HTTPAddr) {
		return fmt.Errorf("refusing to auto-start daemon on non-loopback address %q", c.cfg.HTTPAddr)
	}
	timeout := c.cfg.StartupTimeout
	if timeout <= 0 {
		timeout = config.DefaultStartupTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := os.MkdirAll(c.cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	if err := os.Chmod(c.cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("securing data dir: %w", err)
	}

	lock, err := os.OpenFile(c.cfg.DaemonStartLockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("opening daemon lock: %w", err)
	}
	defer lock.Close()
	if err := acquireLock(ctx, lock); err != nil {
		return fmt.Errorf("coordinating daemon startup (see %s): %w", c.cfg.DaemonLogPath(), err)
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)

	state, _, err := c.daemonStatus(ctx)
	if err != nil {
		if !isConnectionRefused(err) {
			return fmt.Errorf("checking lum daemon status: %w", err)
		}
		if err := c.spawn(c.cfg); err != nil {
			return fmt.Errorf("starting lum daemon: %w", err)
		}
	} else if state == string(worker.StateReady) {
		return nil
	}

	// Unavailable includes the short interval before lum-worker binds its socket,
	// so keep polling just like the daemon's own readiness monitor.
	return c.waitReady(ctx)
}

func acquireLock(ctx context.Context, file *os.File) error {
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return fmt.Errorf("locking daemon startup: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(daemonPollInterval):
		}
	}
}

func (c *Client) waitReady(ctx context.Context) error {
	ticker := time.NewTicker(daemonPollInterval)
	defer ticker.Stop()
	var lastState, lastDetail string
	for {
		state, detail, err := c.daemonStatus(ctx)
		if err == nil && state == string(worker.StateReady) {
			return nil
		}
		if err == nil {
			lastState, lastDetail = state, detail
		}
		if err != nil && !isConnectionRefused(err) {
			return fmt.Errorf("waiting for lum daemon readiness: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"waiting for lum daemon readiness (last state %q: %s; see %s): %w",
				lastState, lastDetail, c.cfg.DaemonLogPath(), ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func (c *Client) daemonStatus(ctx context.Context) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/status", nil)
	if err != nil {
		return "", "", err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", "", fmt.Errorf("status endpoint returned %s", resp.Status)
	}
	var status struct {
		Worker string `json:"worker"`
		Detail string `json:"detail"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return "", "", err
	}
	return status.Worker, status.Detail, nil
}

func isConnectionRefused(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && errors.Is(opErr, syscall.ECONNREFUSED)
}

func isLoopbackAddress(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

func spawnDaemon(cfg config.Config) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(cfg.DaemonLogPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening daemon log: %w", err)
	}
	defer logFile.Close()
	if err := logFile.Chmod(0o600); err != nil {
		return fmt.Errorf("securing daemon log: %w", err)
	}

	cmd := exec.Command(executable, "serve")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}
