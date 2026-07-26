# lum

Lum is a local semantic code-search engine, with a Telescope integration for
Neovim. Point it at a repository, search by meaning instead of by pattern, and
jump to the matching line range. Your code, the embeddings, and the index never
leave the machine.

```sh
lum search --root ~/code/my-project "where are retries handled?"
```

## Install

With [Nix](https://nixos.org/) flakes enabled:

```sh
nix profile install github:alDuncanson/lum
```

That is the whole install — no containers, no API keys, no service to operate.
The embedding model (BAAI/bge-small-en-v1.5, about 70 MB) downloads on first use
and is cached locally; everything after that is offline.

With this repository as a flake input named `lum`:

```nix
home.packages = [ inputs.lum.packages.${pkgs.system}.lum ];
programs.neovim.plugins = [
  pkgs.vimPlugins.telescope-nvim
  inputs.lum.packages.${pkgs.system}.lum-nvim
];
```

Flake outputs: `lum` (the product), `lum-nvim` (the Neovim plugin), and
`lum-worker` (exposed for debugging; `lum` already pins its own).

## Search

```sh
lum search --root . "where is daemon startup coordinated?"
lum search --root . --json  "daemon startup"   # one JSON document
lum search --root . --jsonl "daemon startup"   # one result per line
```

`--root` discovers and idempotently registers the repository, so there is no
separate setup step. Lum indexes Markdown and common source extensions across
~20 languages plus configuration and web formats. It honors nested `.gitignore`
files including negation rules, watches the repository for changes, and returns
inclusive, 1-based line ranges with every result.

Repositories can also be managed explicitly:

```sh
lum add ~/code/my-project
lum status
lum top                         # live indexing activity
lum stop
```

Lum starts on demand — the first search, tool call, or `curl` brings it up. It
keeps registered repositories current with recursive file watching; startup and
periodic full scans are the correctness backstop if watch delivery fails.

## Neovim / Telescope

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
    lum = {
      executable = "lum",
      limit = 50,
      debounce_ms = 200,
      notify = false,          -- see below
      index_on_open = false,   -- see below
    },
  },
})
```

### Knowing what it is doing

`notify = true` reports indexing activity through `vim.notify`, the way an LSP
reports progress — so whichever notification plugin you use renders it:

```text
downloading the embedding model (~70 MB, first run only)
embedding model ready
64 indexed in 42.1s
could not index src/huge.json: document exceeds 32 MiB ingest limit
```

It exists because the first index is otherwise silent, and on a cold start it
is the one time lum makes you wait. Off by default: subscribing starts the
daemon, and opening Neovim should not.

Routine events stay quiet on purpose — a warm rescan that changed nothing, idle
shedding, the respawn after it. A channel that reports non-events is a channel
you learn to ignore. Tune or replace the rules:

```lua
notify = {
  min_scan_ms = 750,           -- ignore faster scans that changed nothing
  types = { "scan_finished" }, -- narrow the subscription (filtered server-side)
  opts = { title = "lum", timeout = 4000 }, -- passed to vim.notify
  on_event = function(event)   -- or take the raw stream and do your own thing
    vim.print(event)
  end,
}
```

Silence is the normal state. A second Neovim session on an unchanged
repository reports nothing at all: the model is cached and the rescan finds no
work, so there is nothing to say. Set `min_scan_ms = 0` to hear about every
scan including the empty ones.

### Indexing before you ask

`index_on_open = true` registers and indexes the current Git repository when
Neovim starts, instead of when the picker first opens.

The picker registers its repository through `lum search --root`, which blocks
until that repository's *first* index finishes — a model download plus a full
embed on a cold repository. Telescope respawns the search on every keystroke,
so typing during that window actively restarts the wait and the picker just
sits empty. Indexing at open moves the work off the critical path, the way an
LSP attaches when you open a file rather than when you first ask it something.

Off by default because it starts a background daemon in every Neovim session,
including ones where you never search. Worth turning on if you use lum
regularly. It does nothing outside a Git repository, and on an already-indexed
one it costs a path lookup and a rescan of unchanged files.

Nothing here is Neovim-specific. `lum events` streams the same thing as
newline-delimited JSON for any consumer:

```sh
lum events --kinds                        # what can be subscribed to
lum events --types scan_finished          # filtered server-side
lum events --no-replay | jq -r .kind      # only what happens from now on
```

The extension discovers the current Git root, registers it, and runs
`lum search --root <repo> --jsonl`, using the line provenance in each result to
preview and open the matching code. Install the `lum-nvim` flake output, or put
this repository's `lua/` directory on Neovim's runtime path with your plugin
manager.

Notably, it is an ordinary CLI client. It reads no database, no vector index, and
no private socket — which is the same deal every other integration gets.

## Built to be extended

Lum is API-first, and that is a constraint rather than a slogan: the CLI is a
pure client of the REST API, so a capability that is not reachable over HTTP
cannot exist in the CLI either. Every integration is peer to every other one.

```sh
curl 'localhost:7420/v1/search?q=retry+backoff&limit=3'
curl 'localhost:7420/v1/search?q=retry+backoff&source=<source-id>'
curl -X DELETE localhost:7420/v1/sources/<source-id>
```

Server-Sent Events expose scans, document lifecycle, worker readiness, queue
depth, current work, and index totals — no client library required:

```sh
curl -N localhost:7420/v1/events
curl -N 'localhost:7420/v1/events?types=document_ingested,document_failed'
```

`lum mcp` serves the Model Context Protocol over stdio with `search`,
`add_source`, `list_sources`, and `status` tools, each delegating to that same
REST API:

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

Internally, the extension points are interfaces rather than forks: a new file
format is a `Parser`, a new chunking strategy is a `Chunker`, a new embedding
model is an `Embedder`, a different index is a `VectorStore`, and a new thing to
index is a `Source`. Adding one is an implementation, not an architecture change.

## How it runs

```text
CLI · Telescope · MCP · curl
              │ REST + SSE over loopback (MCP speaks stdio, then REST)
              ▼
      lum — the dispatcher
      repository scans · watching · catalog · public API
              │ private gRPC over ~/.lum/lum-worker.sock
              ▼
      lum-worker — the worker
      parse → line-aware chunks → embed → vector search
```

Two processes, one product. The dispatcher owns orchestration and every public
interface; the worker owns parsing, embedding, and vector search. The dispatcher
starts and supervises the worker, so there is never a second thing to install or
run.

The HTTP API listens on `127.0.0.1:7420` (`LUM_HTTP_ADDR` overrides it) and the
worker opens no TCP port at all. The dispatcher exits after 15 minutes without a
request. Its worker is shed after 5 minutes without an ingest or search, which
releases the model and index memory, then respawned on demand; a worker crash is
handled the same way. `lum serve` runs everything in the foreground for
debugging.

See [docs/architecture.md](docs/architecture.md) for the design and
[docs/diagrams.md](docs/diagrams.md) for data flow, architecture, and
protocol-boundary diagrams.

## State and reset

Everything lives under `~/.lum` (override with `LUM_DATA_DIR`):

```text
~/.lum/
├── catalog.db              # repositories, documents, hashes, chunk counts, failures
├── daemon.log              # detached logs from both processes
├── daemon.lock             # held for the dispatcher's complete lifetime
├── daemon-start.lock       # coordinates concurrent on-demand starts
├── lum-worker.sock         # private dispatcher-to-worker gRPC socket
├── models/                 # downloaded embedding model cache
├── vectors/                # qdrant-edge index and chunk payloads
└── vectors.manifest.json   # model and dimension the index was built with
```

Run `lum stop` before deleting that directory. `lum stop` waits until every file
is released; on Unix, deleting state while Lum is running does not reset the open
SQLite and vector files.

`LUM_DATA_DIR` has to be shallow enough to hold the worker socket: a Unix domain
socket address caps out at 104 bytes on macOS and 108 on Linux, path included.
Lum checks this at startup and tells you if the path is too deep, so the default
`~/.lum` and anything comparable is fine.

The full-precision model is the default. For higher CPU throughput at a small
retrieval-quality cost, use `lum serve --embedding-model quantized` or set
`LUM_EMBEDDING_MODEL=quantized`. The two produce incompatible vectors, so when
switching, stop Lum and remove `catalog.db`, `vectors/`, and
`vectors.manifest.json`, then re-index — keep `models/` to avoid another
download.

## Development

Lum targets Unix-like systems; its private transport is a Unix domain socket.

```sh
nix develop                      # pinned toolchains for both processes
(cd dispatcher && go test ./...)
(cd worker && cargo test)
nix develop -c buf generate      # regenerate the committed protobuf stubs
nix flake check                  # build and test everything
nix run . -- serve               # build and run in the foreground
```

### Measuring retrieval

```sh
nix run .#eval                 # score lum against eval/queries.yaml
nix run .#eval -- --fresh      # wipe the eval index first
```

Changes to parsing, chunking, or the embedding model are hard to judge by
looking at results, so [eval/](eval/README.md) scores them: recall@k, MRR,
whether the returned chunk is the part you wanted, and how much
of the top five is duplicate files.

`proto/lum/v1/worker.proto` is the contract between the two processes. The
dispatcher's stubs are generated by `buf` and committed, so a plain `go build`
needs no codegen step; the worker generates its own at build time.

### Neovim loop

```sh
nix run .#nvim                   # build, then open Neovim on the local plugin
nix run .#nvim -- --user-config  # ... using your own Neovim config instead
nix develop .#nvim               # or get a shell first, then run: lum-nvim-dev
```

Either one rebuilds the dispatcher from the working tree, takes the worker
prebuilt from Nix (it compiles slowly and is rarely what you are changing), and
opens Neovim with Telescope plus this repository's `lua/` on the runtimepath —
so plugin edits need no rebuild at all, just a restart. `<leader>fs` opens the
picker; `:LumRoot <dir>` searches somewhere else. See [dev/nvim.lua](dev/nvim.lua)
for the config it uses, which is deliberately minimal rather than yours — when
a result looks wrong, the only variable should be lum.

`--user-config` starts your own Neovim instead, with the working-tree lum
attached: your plugins, your notification handler, your keymaps. Use it to see
the integration the way you actually use it; use the isolated config to debug
lum itself, where no other plugin can be at fault.

It needs Telescope in your configuration. If yours lazy-loads it, open Telescope
once and run `:LumAttach` — attaching at startup cannot work when the plugin
does not exist yet.

It runs on `127.0.0.1:7421` with its data in `/tmp/lum-dev`, so a dev session
never collides with an installed lum on the default port or touches a real
index. The shell exports the same settings, so `lum` typed at the prompt and
`lum` invoked from the picker are the same binary against the same index.
