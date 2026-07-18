// Package mcpserver exposes lum to local AI agents over the Model
// Context Protocol (https://modelcontextprotocol.io).
//
// It is deliberately thin: every tool is a typed wrapper around one
// REST endpoint via internal/apiclient, so MCP clients see exactly the
// same system the CLI and curl see. If a capability isn't in the REST
// API, it can't be an MCP tool — that pressure keeps the API complete.
//
// Transport is stdio: the agent (Claude Desktop, Amp, etc.) spawns
// `lum mcp` as a child process and speaks JSON-RPC over stdin/stdout.
// That means this process must never print to stdout itself; all
// diagnostics go to stderr. The daemon (`lum serve`) must already be
// running — `lum mcp` is a client of it, not a second daemon.
package mcpserver

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/alDuncanson/lum/control-plane/internal/apiclient"
	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
)

// Run serves MCP over stdio until the client disconnects or ctx is
// cancelled.
func Run(ctx context.Context, version string) error {
	api := apiclient.New()

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "lum",
		Title:   "lum — local semantic search",
		Version: version,
	}, nil)

	// mcp.AddTool infers JSON Schemas for inputs/outputs from the
	// struct tags below, validates incoming arguments against them,
	// and marshals our typed outputs into structured tool results.
	mcp.AddTool(server, &mcp.Tool{
		Name: "search",
		Description: "Semantic search across all documents lum has indexed. " +
			"Returns the best-matching text chunks with their source file URIs " +
			"and similarity scores (higher is closer).",
	}, searchTool(api))

	mcp.AddTool(server, &mcp.Tool{
		Name: "add_source",
		Description: "Register a new source for lum to index. Currently local " +
			"directories (absolute paths or ~ paths). Indexing runs in the " +
			"background; use the status tool to watch document counts.",
	}, addSourceTool(api))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_sources",
		Description: "List every source lum is indexing.",
	}, listSourcesTool(api))

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Health of the lum daemon and data plane, plus index " +
			"statistics (source, document, and chunk counts).",
	}, statusTool(api))

	return server.Run(ctx, &mcp.StdioTransport{})
}

// ---- search ----

type searchInput struct {
	Query string `json:"query" jsonschema:"the natural-language search query"`
	Limit int    `json:"limit,omitempty" jsonschema:"maximum results to return (1-100, default 10)"`
}

type searchOutput struct {
	Results []dataplane.SearchResult `json:"results"`
}

func searchTool(api *apiclient.Client) mcp.ToolHandlerFor[searchInput, searchOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in searchInput) (
		*mcp.CallToolResult, searchOutput, error,
	) {
		limit := in.Limit
		if limit == 0 {
			limit = 10
		}
		results, err := api.Search(ctx, in.Query, limit)
		if err != nil {
			return nil, searchOutput{}, err
		}
		if results == nil {
			results = []dataplane.SearchResult{}
		}
		return nil, searchOutput{Results: results}, nil
	}
}

// ---- add_source ----

type addSourceInput struct {
	URI string `json:"uri" jsonschema:"the source to index, e.g. a local directory path like ~/Documents"`
}

type addSourceOutput struct {
	Source  catalog.Source `json:"source"`
	Created bool           `json:"created"`
	Message string         `json:"message"`
}

func addSourceTool(api *apiclient.Client) mcp.ToolHandlerFor[addSourceInput, addSourceOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in addSourceInput) (
		*mcp.CallToolResult, addSourceOutput, error,
	) {
		res, err := api.AddSource(ctx, in.URI)
		if err != nil {
			return nil, addSourceOutput{}, err
		}
		msg := "source already registered; a rescan has been queued"
		if res.Created {
			msg = "source added; indexing runs in the background"
		}
		return nil, addSourceOutput{Source: res.Source, Created: res.Created, Message: msg}, nil
	}
}

// ---- list_sources ----

type listSourcesInput struct{}

type listSourcesOutput struct {
	Sources []catalog.Source `json:"sources"`
}

func listSourcesTool(api *apiclient.Client) mcp.ToolHandlerFor[listSourcesInput, listSourcesOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ listSourcesInput) (
		*mcp.CallToolResult, listSourcesOutput, error,
	) {
		sources, err := api.ListSources(ctx)
		if err != nil {
			return nil, listSourcesOutput{}, err
		}
		if sources == nil {
			sources = []catalog.Source{}
		}
		return nil, listSourcesOutput{Sources: sources}, nil
	}
}

// ---- status ----

type statusInput struct{}

type statusOutput struct {
	Daemon    string `json:"daemon"`
	DataPlane string `json:"data_plane"`
	Detail    string `json:"detail,omitempty"`
	Sources   int    `json:"sources"`
	Documents int    `json:"documents"`
	Chunks    int    `json:"chunks"`
}

func statusTool(api *apiclient.Client) mcp.ToolHandlerFor[statusInput, statusOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, _ statusInput) (
		*mcp.CallToolResult, statusOutput, error,
	) {
		st, err := api.Status(ctx)
		if err != nil {
			return nil, statusOutput{}, fmt.Errorf("%w (start it with `lum serve`)", err)
		}
		return nil, statusOutput{
			Daemon:    st.Daemon,
			DataPlane: st.DataPlane,
			Detail:    st.Detail,
			Sources:   st.Stats.Sources,
			Documents: st.Stats.Documents,
			Chunks:    st.Stats.Chunks,
		}, nil
	}
}
