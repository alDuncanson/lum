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

// ensureDaemon starts the daemon if needed and waits for the worker to be
// ready, which is what a command that is about to search or ingest needs.
func (c *Client) ensureDaemon(ctx context.Context) error {
	return c.ensureDaemonUp(ctx, true)
}

// ensureDaemonListening starts the daemon if needed but returns as soon as
// the HTTP API answers, without waiting for the worker.
//
// /v1/events does not touch the worker, and waiting for readiness would
// make the one client whose job is to watch startup the one client that
// cannot see it: the model download and the first scan would both be over
// before it subscribed. That is exactly the silence this endpoint exists
// to fill.
func (c *Client) ensureDaemonListening(ctx context.Context) error {
	return c.ensureDaemonUp(ctx, false)
}

func (c *Client) ensureDaemonUp(ctx context.Context, requireWorker bool) error {
	if !isLoopbackAddress(c.cfg.HTTPAddr) {
		return fmt.Errorf("refusing to auto-start daemon on non-loopback address %q", c.cfg.HTTPAddr)
	}
	// Fail before spawning: a daemon that cannot start would otherwise leave
	// this call polling /v1/status until the startup timeout expires.
	if err := c.cfg.Validate(); err != nil {
		return err
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
	} else if !requireWorker || state == string(worker.StateReady) {
		// Any successful status response means the API is up, which is all a
		// listening-only caller needs.
		return nil
	}

	if !requireWorker {
		return c.waitListening(ctx)
	}
	// Unavailable includes the short interval before lum-worker binds its socket,
	// so keep polling just like the daemon's own readiness monitor.
	return c.waitReady(ctx)
}

// waitListening blocks until the daemon answers /v1/status at all, whatever
// it says about the worker.
func (c *Client) waitListening(ctx context.Context) error {
	ticker := time.NewTicker(daemonPollInterval)
	defer ticker.Stop()
	startedWaiting := time.Now()
	for {
		if _, _, err := c.daemonStatus(ctx); err == nil {
			return nil
		} else if !isConnectionRefused(err) {
			return fmt.Errorf("waiting for the lum daemon (see %s): %w", c.cfg.DaemonLogPath(), err)
		}
		// Same reasoning as waitReady: a free daemon.lock past the startup
		// grace proves the daemon is gone rather than slow.
		if time.Since(startedWaiting) > daemonStartGrace {
			if gone, lockErr := tryAcquireAndRelease(c.cfg.DaemonLockPath()); lockErr == nil && gone {
				return fmt.Errorf("the lum daemon exited during startup; see %s", c.cfg.DaemonLogPath())
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for the lum daemon (see %s): %w", c.cfg.DaemonLogPath(), ctx.Err())
		case <-ticker.C:
		}
	}
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

// daemonStartGrace is how long a just-spawned daemon is given to take
// daemon.lock before a free lock is read as "it is not running". The daemon
// acquires it within milliseconds of exec, well before opening the catalog
// or binding the API, so this is generous.
const daemonStartGrace = 2 * time.Second

func (c *Client) waitReady(ctx context.Context) error {
	ticker := time.NewTicker(daemonPollInterval)
	defer ticker.Stop()
	var lastState, lastDetail string
	startedWaiting := time.Now()
	for {
		state, detail, err := c.daemonStatus(ctx)
		if err == nil && state == string(worker.StateReady) {
			return nil
		}
		// Crashed is terminal for this wait: the worker is not coming up on
		// its own, and the daemon has already said why. Polling on would
		// just replace a specific error with a timeout.
		if err == nil && state == string(worker.StateCrashed) {
			return fmt.Errorf("%s (see %s)", detail, c.cfg.DaemonLogPath())
		}
		if err == nil {
			lastState, lastDetail = state, detail
		}
		if err != nil && !isConnectionRefused(err) {
			// Name the log too: the reason is in that file, not here.
			return fmt.Errorf("waiting for lum daemon readiness (see %s): %w", c.cfg.DaemonLogPath(), err)
		}
		// A refused connection normally means "not listening yet". But the
		// daemon holds daemon.lock for its entire lifetime, so a lock we can
		// take means there is no daemon at all — it exited, and waiting the
		// rest of the startup timeout would only turn its logged reason into
		// a bare deadline. A daemon whose worker cannot start exits in
		// milliseconds, which is precisely when this fires.
		if isConnectionRefused(err) && time.Since(startedWaiting) > daemonStartGrace {
			if gone, lockErr := tryAcquireAndRelease(c.cfg.DaemonLockPath()); lockErr == nil && gone {
				return fmt.Errorf(
					"the lum daemon exited during startup; see %s", c.cfg.DaemonLogPath())
			}
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
