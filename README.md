# lum

Lum is local semantic code search for repositories. Point it at a repository,
search by meaning from the CLI or Neovim, and jump to the matching line range.
Source code, embeddings, and the index stay on your machine.

```sh
lum search --root ~/code/my-project "where are retries handled?"
```

`--root` discovers and idempotently registers the repository, so there is no
separate setup step. Lum indexes Markdown and common Go, Rust, Lua, Nix,
Python, JavaScript/TypeScript, JVM, C/C++, shell, SQL, configuration, and web
source extensions. It honors nested `.gitignore` files (including negation
rules), watches the repository for changes, and returns inclusive, 1-based
source line ranges with each result.

Lum is one product with a multi-process implementation: the `lum` CLI and Go
coordinator own the public interfaces and repository lifecycle, while a private
Rust worker performs parsing, embedding, and vector search. The coordinator
starts and supervises that worker; users do not install, configure, or run two
services.

## Install

With [Nix](https://nixos.org/) flakes enabled:

```sh
nix profile install github:alDuncanson/lum
```

This installs the complete wrapped product. Nix and Home Manager configurations
can select the flake's `lum-nvim` output to add the Neovim/Telescope integration.
The embedding model (BAAI/bge-small-en-v1.5, about 70 MB) downloads on first use
and is then cached locally. First use requires network access for that download;
subsequent use is local. No containers, API keys, or services to operate are
required.

For example, with this repository available as a flake input named `lum`:

```nix
home.packages = [ inputs.lum.packages.${pkgs.system}.lum ];
programs.neovim.plugins = [
  pkgs.vimPlugins.telescope-nvim
  inputs.lum.packages.${pkgs.system}.lum-nvim
];
```

## CLI

Search a repository directly:

```sh
lum search --root . "where is daemon startup coordinated?"
lum search --root . --json "daemon startup"   # one JSON document
lum search --root . --jsonl "daemon startup"  # one result per line
```

Repositories may also be managed explicitly:

```sh
lum add ~/code/my-project
lum status
lum top                         # live indexing activity
lum stop
```

CLI and MCP requests start Lum on demand. It keeps registered repositories
current with recursive file watching; startup and periodic full scans provide a
correctness backstop if watch delivery fails.

## Neovim / Telescope

The Telescope extension in `lua/` discovers the current Git root, registers it
idempotently, and invokes `lum search --root <repo> --jsonl`. It uses the line
provenance in each result to preview and open the matching code. It never reads
Lum's database, vector index, or private worker protocol directly.

Install the `lum-nvim` Nix output (or add this repository's `lua/` directory to
Neovim's runtime path with your plugin manager), then load the extension:

```lua
require("telescope").load_extension("lum")
vim.keymap.set("n", "<leader>fs", function()
  require("telescope").extensions.lum.lum()
end)
```

Run it with the mapping or `:Telescope lum`. Optional configuration:

```lua
require("telescope").setup({
  extensions = {
    lum = { executable = "lum", limit = 50, debounce_ms = 200 },
  },
})
```

## REST, events, and MCP

The CLI is a client of the loopback REST API. For example:

```sh
curl 'localhost:7420/v1/search?q=retry+backoff&limit=3'
curl 'localhost:7420/v1/search?q=retry+backoff&source=<source-id>'
curl -X DELETE localhost:7420/v1/sources/<source-id>
```

Server-Sent Events expose scans, document lifecycle, worker readiness, queue
depth, current work, and index totals:

```sh
curl -N localhost:7420/v1/events
curl -N 'localhost:7420/v1/events?types=document_ingested,document_failed'
```

`lum mcp` serves MCP over stdio with `search`, `add_source`, `list_sources`, and
`status` tools. Each delegates to the same REST API, and the first tool call
starts Lum if needed:

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

## How it runs

```text
CLI / Telescope / MCP / curl
              │ REST (MCP uses stdio, then REST)
              ▼
      lum — Go coordinator
      repository scans · watching · catalog · public API
              │ private gRPC over ~/.lum/lumen.sock
              ▼
      supervised Rust worker
      parse → line-aware chunks → embed → vector search
```

The HTTP API listens on `127.0.0.1:7420` (`LUM_HTTP_ADDR` overrides it).
The private worker opens no TCP port. The coordinator exits after 15 minutes
without an HTTP request. Its worker is shed after 5 minutes without an
ingest/search RPC to release model and index memory, then respawned lazily; a
worker crash is handled the same way. `lum serve` runs the product in the
foreground for debugging. See [docs/architecture.md](docs/architecture.md) for
the detailed design and [docs/diagrams.md](docs/diagrams.md) for data flow,
architecture, and protocol-boundary diagrams.

## State and reset

Everything lives under `~/.lum` (override with `LUM_DATA_DIR`):

```text
~/.lum/
├── catalog.db          # repositories, documents, hashes, chunk counts, failures
├── daemon.log          # detached coordinator and worker logs
├── daemon.lock         # held for the coordinator's complete lifetime
├── daemon-start.lock   # coordinates concurrent on-demand starts
├── lumen.sock          # private coordinator-to-worker gRPC socket
├── models/             # downloaded embedding model cache
└── vectors/            # qdrant-edge index and chunk payloads
```

To reset Lum, run `lum stop` before deleting the directory. `lum stop` waits
until all files are released; deleting state while Lum is running does not reset
open SQLite/vector files on Unix.

The full-precision model is the default. For higher CPU throughput with a small
retrieval-quality tradeoff, use `lum serve --embedding-model quantized` or set
`LUM_EMBEDDING_MODEL=quantized`. The models produce incompatible vectors. When
switching, stop Lum and remove `catalog.db`, `vectors/`, and
`vectors.manifest.json`, then re-index; keep `models/` to avoid another download.

## Development and benchmarks

Lum targets Unix-like systems because its private transport is a Unix domain
socket.

```sh
nix develop                    # pinned Go and Rust development shell
(cd control-plane && go test ./...)
(cd data-plane && cargo test)
buf generate                   # regenerate committed Go protobufs
nix flake check
nix run . -- serve             # build and run in the foreground
```

For repeatable local measurements:

```sh
nix build
bash scripts/benchmark.sh --lum ./result/bin/lum --model-cache ~/.lum/models .
```

The harness emits JSON with cold-start latency, initial index-and-search time,
warm query latency (min/median/p95/max), indexed document/chunk counts, and a
resident-memory snapshot of both internal processes. It uses a temporary
`LUM_DATA_DIR`; an existing model cache only excludes the one-time download.
Results depend on machine and repository, so the project records the procedure
rather than a fixed headline number. Use `--runs`, `--query`, or `--addr` to
adjust it.
