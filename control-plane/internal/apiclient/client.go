// Package apiclient is the Go client for lumd's REST API.
//
// Both the CLI commands (internal/cli) and the MCP server
// (internal/mcpserver) go through this package, which keeps two
// invariants:
//
//  1. The REST API stays the single front door — no client gets a
//     privileged side channel into the catalog or the data plane.
//  2. The API's request/response shapes are defined once, here, instead
//     of being re-declared inline by every caller.
package apiclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/config"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
)

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
type AddSourceResult struct {
	Source  catalog.Source `json:"source"`
	Created bool           `json:"created"` // false if the URI was already registered
}

// AddSource registers a source URI and queues an initial scan.
func (c *Client) AddSource(ctx context.Context, uri string) (AddSourceResult, error) {
	var out AddSourceResult
	err := c.call(ctx, "POST", "/v1/sources", map[string]string{"uri": uri}, &out)
	return out, err
}

// ListSources returns every registered source.
func (c *Client) ListSources(ctx context.Context) ([]catalog.Source, error) {
	var out []catalog.Source
	err := c.call(ctx, "GET", "/v1/sources", nil, &out)
	return out, err
}

// Search runs a semantic query and returns the nearest chunks. sourceID
// restricts results to one source; empty means all sources.
func (c *Client) Search(ctx context.Context, query string, limit int, sourceID string) ([]dataplane.SearchResult, error) {
	var out struct {
		Results []dataplane.SearchResult `json:"results"`
	}
	path := fmt.Sprintf("/v1/search?q=%s&limit=%d", url.QueryEscape(query), limit)
	if sourceID != "" {
		path += "&source=" + url.QueryEscape(sourceID)
	}
	err := c.call(ctx, "GET", path, nil, &out)
	return out.Results, err
}

// Status is the response of GET /v1/status.
type Status struct {
	Daemon    string                  `json:"daemon"`
	DataPlane string                  `json:"data_plane"`
	Detail    string                  `json:"detail"`
	Stats     catalog.Stats           `json:"stats"`
	Failures  []catalog.IngestFailure `json:"failures"`
}

// Status reports daemon health, data plane health, and index counts.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var out Status
	err := c.call(ctx, "GET", "/v1/status", nil, &out)
	return out, err
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
