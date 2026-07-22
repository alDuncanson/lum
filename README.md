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

## Install

lum uses a Nix flake to pin Go, Rust, protobuf tooling, and native build
dependencies. With flakes enabled:

```sh
nix profile install github:alDuncanson/lum
```

The embedding model (BAAI/bge-small-en-v1.5, ~70 MB) downloads automatically
on first run and is cached in the data dir. There are no containers, API keys,
or services to configure.

## Build and run

```sh
nix build                         # result/bin/lum + result/bin/lumen
nix run . -- add ~/Documents
nix run . -- status
nix run . -- search "..."
nix run . -- top
nix run . -- stop
```

The flake also exposes `.#lum` and `.#lumen` as separate packages for release
artifacts, plus `.#lum-full` (the default) with both binaries side-by-side.
Normal users should install the combined package so the Go control plane can
discover its supervised Rust data plane.

The same API the CLI uses is available to anything else:

```sh
curl 'localhost:7420/v1/search?q=wild+yeast&limit=3'
curl 'localhost:7420/v1/search?q=wild+yeast&source=<source-id>'  # restrict to one source
curl -X DELETE localhost:7420/v1/sources/<source-id>             # remove a source + its vectors
```

Watch the pipeline as it works via Server-Sent Events — scans, per-document
lifecycle, data-plane readiness, and a periodic snapshot (queue depth,
current document, index totals):

```sh
curl -N localhost:7420/v1/events                    # everything, live
curl -N 'localhost:7420/v1/events?types=document_ingested,document_failed'
```

Indexed files include Markdown and common Go, Rust, Lua, Nix, Python,
JavaScript/TypeScript, JVM, C/C++, shell, SQL, configuration, and web source
extensions. Directory sources honor nested `.gitignore` files, including
negation rules.

## Neovim / Telescope

The repository includes a Telescope extension that discovers the current Git
root, registers it idempotently, indexes it, and searches it without separate
`lum add` setup. Search results carry source line ranges, so selection and
preview jump to the matching code.

Install the combined binary package, add this repository to Neovim's runtime
path with your plugin manager, then load the extension:

```lua
require("telescope").load_extension("lum")
vim.keymap.set("n", "<leader>fs", function()
  require("telescope").extensions.lum.lum()
end)
```

Equivalent command: `:Telescope lum`. Optional configuration:

```lua
require("telescope").setup({
  extensions = {
    lum = { executable = "lum", limit = 50, debounce_ms = 200 },
  },
})
```

The picker invokes `lum search --root <workspace> --jsonl`; it does not access
the catalog, vector files, REST API, or private gRPC service directly.

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

`lum stop`, then delete the directory, resets lum completely. Deleting it
*without* stopping the daemon first doesn't actually reset anything: on
Unix, a running process keeps its open files (the catalog, the vector
index) alive by inode even after their directory entry is gone, so the
daemon keeps serving the old state until it actually exits — `lum status`
and `lum search` will look completely unaffected by the deletion. `lum
stop` waits for the daemon to fully release everything (not just for its
HTTP port to stop answering) before returning, so it's always safe to
delete the directory immediately afterward.

## Development

lum targets Unix-like systems because its private inter-process transport is a
Unix domain socket.

```sh
nix develop                   # pinned Go 1.26 and Rust 1.97.1 shell
go test ./...                 # from control-plane/
cargo test                    # from data-plane/
buf generate                  # regenerate committed Go protobufs
nix flake check               # all flake checks
nix run . -- serve            # build and run in the foreground
```

For repeatable local performance measurements, build the combined package and
run the isolated benchmark harness against a representative workspace:

```sh
nix build
bash scripts/benchmark.sh --lum ./result/bin/lum --model-cache ~/.lum/models .
```

It emits JSON with cold-start latency, initial index-and-search time, warm
query latency (min/median/p95/max), indexed document/chunk counts, and a
resident-memory snapshot for both processes. The harness uses a temporary
`LUM_DATA_DIR`, so it cannot alter the normal catalog or vector index; passing
an existing model cache only avoids measuring the one-time model download.
Results are machine- and corpus-specific, so the repository records the
procedure rather than a misleading fixed benchmark number. Use `--runs`,
`--query`, or `--addr` to adjust the run.

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
- [x] Common source-code formats with nested `.gitignore` support
- [x] Line-aware semantic results and a Telescope picker
- [ ] Second parser (PDF or HTML) to exercise the parser seam
- [ ] RSS source to exercise the source seam
