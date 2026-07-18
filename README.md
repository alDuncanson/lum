# lum

Local-first, local-**only** semantic search infrastructure. lum ingests
documents from registered sources (local directories today, RSS feeds and
more tomorrow), embeds them on your machine, and serves semantic retrieval
over a CLI, a REST API, and MCP for local agents.

No cloud, no API keys, no Docker, no services to operate. Two binaries and
a data directory.

```
$ lum serve                    # terminal 1: the daemon
$ lum add ~/Documents          # terminal 2: register a source
$ lum search "that note about sourdough hydration"
 1. 0.744  /Users/al/Documents/baking.md (chunk 0)
    # Sourdough basics  A sourdough starter is a living culture …
```

## Why this exists

lum is a portfolio/learning project with a deliberate shape: a **Go
control plane** and a **Rust data plane**, split the way real
infrastructure systems split them — orchestration in one process,
compute in another, a versioned gRPC contract between them.

```
   you ──▶ lum (CLI) ─┐          ┌─ agents ──▶ `lum mcp` (MCP/stdio)
                      │ HTTP     │
                      ▼          ▼
        ┌─────────────────────────────────┐
        │   lumd — Go control plane       │   `lum serve`
        │   sources · scans · catalog     │
        │   SQLite (~/.lum/catalog.db)    │
        └───────────────┬─────────────────┘
                        │ gRPC (proto/lum/v1) — lumen is a
                        ▼ supervised child process of lumd
        ┌─────────────────────────────────┐
        │   lumen — Rust data plane       │
        │   parse → chunk → embed         │
        │   fastembed (local ONNX model)  │
        │   qdrant-edge (embedded index)  │
        └─────────────────────────────────┘
```

See [docs/architecture.md](docs/architecture.md) for the full design and
the reasoning behind each decision.

## Requirements

- Go ≥ 1.26
- Rust (stable)

That's the whole list. Protobuf codegen needs no `protoc` (buf for Go,
protox for Rust); the vector store is embedded (qdrant-edge); the
embedding model (BAAI/bge-small-en-v1.5, ~70 MB) downloads automatically
on first run and is cached in the data dir.

## Build and run

```sh
make build        # builds bin/lum (Go) and bin/lumen (Rust)
./bin/lum serve   # starts the daemon; spawns + supervises lumen
```

Then, from another terminal:

```sh
./bin/lum add ~/Documents      # register + index a directory
./bin/lum status               # daemon health and index counts
./bin/lum sources              # list registered sources
./bin/lum search "..."         # semantic search
```

The same API the CLI uses is available to anything else:

```sh
curl 'localhost:7420/v1/search?q=wild+yeast&limit=3'
```

Indexed file types: `.txt`, `.md` (see `internal/source/localdir.go` and
`data-plane/src/pipeline/parser.rs` for how to add more).

## MCP (local agents)

`lum mcp` serves the [Model Context Protocol](https://modelcontextprotocol.io)
over stdio, exposing four tools: `search`, `add_source`, `list_sources`,
and `status`. Each one is a thin wrapper around the REST API, so agents
see exactly the same system you do — and the daemon must be running.

Configure your MCP client (Claude Desktop, Amp, ...) to spawn it:

```json
{
  "mcpServers": {
    "lum": {
      "command": "/path/to/bin/lum",
      "args": ["mcp"]
    }
  }
}
```

## State

Everything lives under `~/.lum` (override with `LUM_DATA_DIR`):

```
~/.lum/
├── catalog.db   # SQLite: sources, documents, hashes, chunk counts
├── models/      # embedding model cache (auto-downloaded)
└── vectors/     # qdrant-edge index (embeddings + chunk payloads)
```

Delete the directory to reset lum completely.

## Development

```sh
make test         # Go + Rust unit tests
make proto        # regenerate Go code after editing proto/ (output is committed)
make run          # build + serve
```

Ports: HTTP API on `127.0.0.1:7420`, data plane gRPC on `127.0.0.1:7421`
(`LUM_HTTP_ADDR` / `LUM_GRPC_ADDR` to change). Everything binds loopback
only, by design.

## Roadmap

- [x] Sources: local directories, with hash-based change detection and
      delete reconciliation on rescan
- [x] MCP server (`lum mcp`) so local agents can search and add sources
- [ ] Live watching (fsnotify) instead of manual rescans
- [ ] Second parser (PDF or HTML) to exercise the parser seam
- [ ] RSS source to exercise the source seam
