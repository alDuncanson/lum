// Package apiclient is the Go client for lumd's REST API.
//
// Both the CLI commands (internal/cli) and the MCP server
// (internal/mcpserver) go through this package, which keeps two
// invariants:
//
//  1. The REST API stays the single front door — no client gets a
//     privileged side channel into the catalog or the worker.
//  2. The API's request/response shapes are defined once, here, instead
//     of being re-declared inline by every caller.
package apiclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/alDuncanson/lum/dispatcher/internal/apiv1"
	"github.com/alDuncanson/lum/dispatcher/internal/config"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
)

// ErrNoDaemonRunning means Stop found nothing listening. Unlike every
// other Client method, Stop must never trigger the on-demand auto-spawn
// (#13) on a refused connection — spawning a daemon just to tell it to
// stop would be absurd.
var ErrNoDaemonRunning = errors.New("no lum daemon is running")

// Client talks to a running lumd over loopback HTTP.
type Client struct {
	base       string
	cfg        config.Config
	httpClient *http.Client
	spawn      func(config.Config) error
}

// New builds a client pointed at the configured daemon address
// (LUM_HTTP_ADDR, default 127.0.0.1:7420).
func New() *Client {
	cfg := config.Load()
	return &Client{
		base:       cfg.BaseURL(),
		cfg:        cfg,
		httpClient: http.DefaultClient,
		spawn:      spawnDaemon,
	}
}

// ---- typed API methods ----

// AddSourceResult is the response of POST /v1/sources.
type AddSourceResult = apiv1.AddSourceResponse

// AddSource registers a source URI and queues an initial scan.
func (c *Client) AddSource(ctx context.Context, uri string) (AddSourceResult, error) {
	var out AddSourceResult
	err := c.call(ctx, "POST", "/v1/sources", apiv1.AddSourceRequest{URI: uri}, &out)
	return out, err
}

// EnsureSource registers a source if needed and waits for any tracked initial
// scan attempt to finish.
func (c *Client) EnsureSource(ctx context.Context, uri string) (AddSourceResult, error) {
	var out AddSourceResult
	err := c.call(ctx, "POST", "/v1/sources?wait=initial", apiv1.AddSourceRequest{URI: uri}, &out)
	return out, err
}

// ListSources returns every registered source.
func (c *Client) ListSources(ctx context.Context) ([]apiv1.Source, error) {
	var out []apiv1.Source
	err := c.call(ctx, "GET", "/v1/sources", nil, &out)
	return out, err
}

// Search runs a semantic query and returns the nearest chunks. sourceID
// restricts results to one source; empty means all sources.
func (c *Client) Search(ctx context.Context, query string, limit int, sourceID string) ([]apiv1.SearchResult, error) {
	var out apiv1.SearchEnvelope
	path := fmt.Sprintf("/v1/search?q=%s&limit=%d", url.QueryEscape(query), limit)
	if sourceID != "" {
		path += "&source=" + url.QueryEscape(sourceID)
	}
	err := c.call(ctx, "GET", path, nil, &out)
	return out.Results, err
}

// Status is the response of GET /v1/status.
type Status = apiv1.Status

// Status reports daemon health, worker health, and index counts.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var out Status
	err := c.call(ctx, "GET", "/v1/status", nil, &out)
	return out, err
}

// Stop requests a graceful shutdown of the running daemon and waits for
// it to actually exit, so a caller that gets a nil error back knows every
// file (catalog, vector index, lum-worker's socket) is fully released, not
// just that the HTTP port stopped answering. Returns ErrNoDaemonRunning
// if nothing was listening in the first place.
func (c *Client) Stop(ctx context.Context) error {
	statusCode, status, _, err := c.do(ctx, http.MethodPost, "/v1/shutdown", nil)
	if isConnectionRefused(err) {
		return ErrNoDaemonRunning
	}
	if err != nil {
		return fmt.Errorf("requesting shutdown: %w", err)
	}
	if statusCode >= 400 {
		return fmt.Errorf("shutdown request returned %s", status)
	}
	return c.waitForDaemonLockToRelease(ctx)
}

// waitForDaemonLockToRelease blocks until the previous daemon has fully
// released daemon.lock, which it holds for its complete lifetime (see
// acquireDaemonLock in cli/serve.go) and which the OS releases the
// instant the process exits, even on a crash. This is deliberately not
// "wait until the HTTP port stops answering": listener.Close() runs
// before dp.Close() (stops lum-worker) and cat.Close() in the shutdown
// sequence, so the port can go quiet while the process is still mid
// cleanup — confirmed by hand: PID still alive nearly a second after
// /v1/status started refusing connections. The lock is the same
// authoritative "fully gone" signal the on-demand daemon spawn (#13)
// already relies on to know a replacement can safely start.
func (c *Client) waitForDaemonLockToRelease(ctx context.Context) error {
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	path := c.cfg.DaemonLockPath()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if released, err := tryAcquireAndRelease(path); err != nil {
			return fmt.Errorf("checking daemon lock: %w", err)
		} else if released {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("daemon did not fully exit within 15s of the shutdown request")
		case <-ticker.C:
		}
	}
}

func tryAcquireAndRelease(path string) (bool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false, err
	}
	defer file.Close()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return false, nil
		}
		return false, err
	}
	_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return true, nil
}

// Events opens a long-lived connection to GET /v1/events (SSE), starting
// the daemon on demand exactly like every other command. types optionally
// narrows the stream to specific event Kinds (server-side ?types= filter);
// nil subscribes to everything. The returned channel is closed when ctx
// is cancelled or the connection ends; malformed frames are dropped
// rather than surfaced, since a stream is inherently best-effort.
func (c *Client) Events(ctx context.Context, types []string) (<-chan events.Event, error) {
	path := "/v1/events"
	if len(types) > 0 {
		path += "?types=" + url.QueryEscape(strings.Join(types, ","))
	}

	statusCode, status, body, err := c.doStream(ctx, path)
	if isConnectionRefused(err) {
		if err := c.ensureDaemon(ctx); err != nil {
			return nil, err
		}
		statusCode, status, body, err = c.doStream(ctx, path)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot reach the lum daemon at %s: %w", c.base, err)
	}
	if statusCode >= 400 {
		defer body.Close()
		raw, _ := io.ReadAll(io.LimitReader(body, 4096))
		var apiErr struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error != "" {
			return nil, fmt.Errorf("%s", apiErr.Error)
		}
		return nil, fmt.Errorf("events endpoint returned %s", status)
	}

	out := make(chan events.Event, 64)
	go func() {
		defer close(out)
		defer body.Close()
		scanner := bufio.NewScanner(body)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			data, ok := strings.CutPrefix(scanner.Text(), "data: ")
			if !ok {
				continue // event: lines, heartbeat comments, blank separators
			}
			var e events.Event
			if err := json.Unmarshal([]byte(data), &e); err != nil {
				continue
			}
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// doStream is like do but returns the live response body for streaming
// instead of reading it fully; the caller owns closing it.
func (c *Client) doStream(ctx context.Context, path string) (int, string, io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return 0, "", nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	return resp.StatusCode, resp.Status, resp.Body, nil
}

// ---- transport ----

// call performs one JSON request/response round trip. `out` may be nil.
func (c *Client) call(ctx context.Context, method, path string, body, out any) error {
	var requestBody []byte
	if body != nil {
		var err error
		requestBody, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}

	statusCode, status, raw, err := c.do(ctx, method, path, requestBody)
	if isConnectionRefused(err) || statusCode == http.StatusServiceUnavailable {
		if err := c.ensureDaemon(ctx); err != nil {
			return err
		}
		statusCode, status, raw, err = c.do(ctx, method, path, requestBody)
	}
	if err != nil {
		return fmt.Errorf("cannot reach the lum daemon at %s: %w", c.base, err)
	}
	if statusCode >= 400 {
		// The API returns {"error": "..."} for failures; surface it.
		var apiErr struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error != "" {
			return fmt.Errorf("%s", apiErr.Error)
		}
		return fmt.Errorf("API returned %s", status)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (c *Client) do(
	ctx context.Context,
	method, path string,
	body []byte,
) (int, string, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return 0, "", nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Status, raw, err
}
