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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/config"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
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

// Search runs a semantic query and returns the nearest chunks.
func (c *Client) Search(ctx context.Context, query string, limit int) ([]dataplane.SearchResult, error) {
	var out struct {
		Results []dataplane.SearchResult `json:"results"`
	}
	path := fmt.Sprintf("/v1/search?q=%s&limit=%d", url.QueryEscape(query), limit)
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
