# lum

Local-first, local-**only** semantic search infrastructure. lum ingests
documents from registered sources (local directories today, RSS feeds and
more tomorrow), embeds them on your machine, and serves semantic retrieval
over a CLI, a REST API, and MCP for local agents.

No cloud, no API keys, no Docker, no services to operate. Two binaries and
a data directory.

```
$ lum add ~/Documents          # starts the daemon on demand and registers a source
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
        │   lumd — Go control plane       │   auto-started on demand
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
./bin/lum add ~/Documents      # register + index a directory
./bin/lum status               # daemon health and index counts
./bin/lum sources              # list registered sources
./bin/lum search "..."         # semantic search
```

The same API the CLI uses is available to anything else:

```sh
curl 'localhost:7420/v1/search?q=wild+yeast&limit=3'
```

Watch the pipeline as it works via Server-Sent Events — scans, per-document
lifecycle, data-plane readiness, and a periodic snapshot (queue depth,
current document, index totals):

```sh
curl -N localhost:7420/v1/events                    # everything, live
curl -N 'localhost:7420/v1/events?types=document_ingested,document_failed'
```

Indexed file types: `.txt`, `.md` (see `internal/source/localdir.go` and
`data-plane/src/pipeline/parser.rs` for how to add more).

## MCP (local agents)

`lum mcp` serves the [Model Context Protocol](https://modelcontextprotocol.io)
over stdio, exposing four tools: `search`, `add_source`, `list_sources`,
and `status`. Each one is a thin wrapper around the REST API, so agents
see exactly the same system you do. The first tool call starts the daemon if
needed.

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
├── catalog.db   # SQLite: sources, documents, hashes, chunk counts, ingest failures
├── daemon.log   # detached daemon and data-plane logs
├── daemon.lock  # held for the daemon's complete lifetime
├── daemon-start.lock  # coordinates concurrent on-demand starts
├── lumen.sock   # private gRPC hop to the supervised data plane
├── models/      # embedding model cache (auto-downloaded)
└── vectors/     # qdrant-edge index (embeddings + chunk payloads)
```

Delete the directory to reset lum completely.

## Development

lum targets Unix-like systems because its private inter-process transport is a
Unix domain socket.

```sh
make test         # Go + Rust unit tests
make proto        # regenerate Go code after editing proto/ (output is committed)
make run          # build + serve
```

The HTTP API listens on `127.0.0.1:7420` (`LUM_HTTP_ADDR` to change). The
private data-plane gRPC hop uses `lumen.sock` under the owner-only data
directory; it does not open a TCP port. CLI and MCP requests start the daemon
automatically, and it exits after 15 minutes without an HTTP request. `lum
serve` remains available for foreground debugging.

lumen (the data plane) has its own, shorter idle lifetime nested inside
lumd's: after 5 minutes without an ingest/search RPC (`LUM_DATAPLANE_IDLE_TIMEOUT`
to change), lumd stops it to release the ONNX model and qdrant-edge's
resident memory, and respawns it lazily on the next request that needs it
(`lum status` reports `data plane: idle` in the meantime; polling status
never itself wakes it). An unexpected lumen crash is treated the same way —
the next request respawns it rather than failing forever.

The full-precision embedding model remains the default. For higher CPU
throughput with a small retrieval-quality tradeoff, start the daemon with
`lum serve --embedding-model quantized` or `LUM_EMBEDDING_MODEL=quantized`.
The models produce incompatible vectors: when changing models, remove
`catalog.db`, `vectors/`, and `vectors.manifest.json` from the data directory,
then add sources again to fully re-ingest them. Keep `models/` to avoid
re-downloading cached model files.

## Roadmap

- [x] Sources: local directories, with hash-based change detection and
      delete reconciliation on rescan
- [x] MCP server (`lum mcp`) so local agents can search and add sources
- [x] Live watching (fsnotify), with manual/startup rescans as a correctness backstop
- [ ] Second parser (PDF or HTML) to exercise the parser seam
- [ ] RSS source to exercise the source seam
