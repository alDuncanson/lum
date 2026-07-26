# lum outside Neovim

Lum is API-first, and that is a constraint rather than a slogan: the CLI is a
pure client of the REST API, so a capability that is not reachable over HTTP
cannot exist in the CLI either. Every integration is peer to every other one.

## Searching

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

At most two results come from any one file, so a single large file cannot fill
the list. `--per-file 1` gives one result per file, `--per-file 0` returns raw
nearest neighbours, and `--no-tests` omits test files. Those defaults are
measured, not guessed — see [../eval/README.md](../eval/README.md).

Repositories can also be managed explicitly:

```sh
lum add ~/code/my-project
lum status
lum top                         # live indexing activity
lum stop
```

Lum starts on demand — the first search, tool call, or `curl` brings it up. It

## HTTP and events

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

Internally the extension points are interfaces rather than forks: a new file
format is a `Parser`, a new chunking strategy is a `Chunker`, a new embedding
model is an `Embedder`, a different index is a `VectorStore`, and a new thing
to index is a `Source`. Adding one is an implementation, not an architecture
change.

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

See [docs/architecture.md](architecture.md) for the design and
[docs/diagrams.md](diagrams.md) for data flow, architecture, and
protocol-boundary diagrams.

## ## State and reset

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
