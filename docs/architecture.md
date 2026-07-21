# lum architecture

This document explains how lum is put together and, more importantly,
*why*. It is written to be read alongside the code; every section names
the files it describes.

## Design constraints

These were chosen up front and drive everything else:

1. **Local-only.** Not "local-first with a cloud story" — local, period.
   Every listener binds loopback. This deletes entire problem classes
   (auth, TLS, multi-tenancy) and buys a zero-dependency UX.
2. **Two planes, two languages.** A Go control plane for orchestration
   (concurrency, servers, tooling — Go's home turf) and a Rust data
   plane for compute (parsing, embedding, vector search — where
   performance and memory control matter).
3. **API-first.** The CLI is a pure client of the REST API. If a feature
   isn't reachable over HTTP, the CLI can't have it — which keeps the
   API honest and makes every client (CLI, MCP, curl, a future TUI)
   an equal.
4. **Interface-driven extension points.** New source types, parsers,
   chunkers, embedders, and vector stores are new implementations of
   existing interfaces, never architectural changes.
5. **No runtime dependencies.** No Docker, no protoc, no model API keys.
   Two toolchains to build; two binaries to run.

## The planes

### Control plane — `control-plane/` (Go, binary: `lum`)

Owns *what and when*: which sources exist, what documents they contained
at last scan, what changed, what to (re)ingest, and the public API.

| Package | Responsibility |
|---|---|
| `internal/cli` | cobra commands; `serve` runs the daemon, the rest are HTTP clients |
| `internal/config` | data dir + addresses, env overrides |
| `internal/api` | REST endpoints (the system's only front door) |
| `internal/apiclient` | typed Go client for the REST API (shared by cli and mcpserver) |
| `internal/mcpserver` | MCP stdio server: four tools, each a thin wrapper over the REST API |
| `internal/source` | the `Source` interface, URI → implementation dispatch, `localdir` |
| `internal/catalog` | SQLite bookkeeping: sources, documents, hashes, chunk counts, ingest failures |
| `internal/ingest` | scan planner + document worker: diff source state, batch jobs, retry failures |
| `internal/dataplane` | lumen child-process supervisor + typed gRPC client wrapper |
| `internal/gen` | generated proto code (committed, so `go build` just works) |

### Data plane — `data-plane/` (Rust, binary: `lumen`)

Owns *bytes and math*: parse → chunk → embed → store/search. Spawned by
`lum serve` as a child process; users never run it directly.

| Module | Responsibility |
|---|---|
| `service.rs` | gRPC glue: compose pipeline, hop to blocking threads, map errors |
| `pipeline/parser.rs` | `Parser` trait + registry (plain text, markdown) |
| `pipeline/chunker.rs` | `Chunker` trait + word-window implementation |
| `pipeline/embedder.rs` | `Embedder` trait + fastembed (bge-small-en-v1.5) |
| `store/` | `VectorStore` trait + qdrant-edge implementation |

The data plane is **stateless between calls**: every RPC carries all
context it needs (document IDs, previous chunk counts). It can be killed
and restarted at any time without coordination — its only persistent
artifacts are the vector index and the model cache.

## The contract — `proto/lum/v1/dataplane.proto`

Five RPCs: `Health`, `IngestDocument`, `IngestBatch`, `DeleteDocument`, and `Search`. The
narrowness is the point: features above this line (MCP, and planned RSS
sources and file watching) reuse these RPCs untouched — MCP shipped
without changing a single one.

Codegen without protoc:
- Go: `make proto` → buf + protoc-gen-go(-grpc), output committed.
- Rust: `build.rs` → protox at build time, nothing committed.

## Data ownership: one home per fact

```
catalog.db  (control plane)   WHAT EXISTS   sources, documents, content
                                            hashes, chunk counts, failures
vectors/    (data plane)      WHAT IT MEANS embeddings + chunk payloads
```

The catalog is never asked "what matches this query?"; the vector store
is never enumerated to answer "what have we ingested?". Search results
are self-describing because chunk payloads carry their provenance
(document id, source id, uri, text).

### The chunk-count trick

Vector points get **deterministic IDs**: `UUIDv5(namespace, "{document_id}/{chunk_index}")`.
The catalog records how many chunks each document produced. Together
these mean:

- re-ingesting overwrites points in place (same IDs),
- deleting needs no filtered queries — the control plane knows every
  point ID a document owns from `(document_id, chunk_count)` alone,
- a shrinking document leaves no stale tail (old range deleted first).

This is why the data plane needs no bookkeeping of its own.

### Durability ordering

`EdgeStore` flushes qdrant-edge **before** acking an ingest, because the
control plane writes its catalog row (hash + chunk count) after the ack.
If vectors could be lost after that row is written, the next scan would
see a matching hash, skip the document forever, and search would
silently miss it. Rule: durability before bookkeeping.

Relatedly, the supervisor uses `exec.Command`, not `exec.CommandContext`
— CommandContext SIGKILLs the child on context cancellation, racing the
graceful shutdown that lets qdrant-edge flush. (Found the hard way; see
the comment in `supervisor.go`.)

## Ingestion flow

```
lum add ~/Documents
  └▶ POST /v1/sources ──▶ catalog row ──▶ enqueue scan ──▶ 202
                                             │
                              scan planner (deduped by source)
                                             ▼
                    Source.Scan → refs (uri, mime, content hash)
                                             │
                       diff against catalog by uri + hash
                       ├─ unchanged → skip (the common case)
                       ├─ new/changed ─┐
                       └─ vanished ────┴▶ document job queue
                                             │
                                  one document worker
                                  ├─ Read → gRPC IngestBatch
                                  │          └▶ parse→chunk→embed→upsert
                                  │    → catalog upsert (hash, count)
                                  └─ gRPC DeleteDocument → catalog delete
```

Scans are idempotent and cheap when nothing changed, which makes the
recovery story trivial: rescan everything on daemon startup.

Pending scans are deduplicated by source. Explicit and startup scans run
immediately; fsnotify change notifications have a one-second debounce path.
Local directory watches are recursive (new directories are added dynamically),
skip the same hidden trees as scans, and degrade to five-minute full rescans if
OS watch limits or event delivery fail. A planner turns each authoritative
source snapshot into document upsert/delete jobs. Those jobs are consumed by one worker — single
because ingestion throughput is bounded by the embedding model anyway — and
small documents are combined into cross-document batches. The job channel is
the "event bus" in miniature; a real broker could replace it without changing
what flows through it.

Read, ingest, and delete failures are persisted per `(source_id, uri)` and
retried by scheduling another source reconciliation after 1s, 2s, and 4s.
Already-successful documents hash-skip during those retries. The failure is
cleared on success (or when a never-indexed document disappears); exhausted
failures remain visible through `/v1/status` and `lum status`.

On daemon startup, lumd starts its HTTP API immediately while lumen loads its
model and vector store. `/v1/status` reports `data_plane` as `starting`,
`downloading-model`, `ready`, or `unavailable`. Startup watches and scans begin
only after readiness; scan-triggering API calls return 503 while loading, so a
temporary startup state is not persisted as an ingestion failure.

## Search flow

```
lum search "..." ──▶ GET /v1/search ──▶ gRPC Search
                                          └▶ embed("query: ...") → qdrant-edge
                                             nearest-neighbor (cosine) → hits
```

Chunks are embedded as `"passage: ..."` and queries as `"query: ..."` —
the asymmetric-prefix convention bge models are trained with.

## MCP — `internal/mcpserver`

`lum mcp` speaks the Model Context Protocol over stdio: an agent spawns
it as a child process and calls its tools (`search`, `add_source`,
`list_sources`, `status`) via JSON-RPC on stdin/stdout.

```
agent ──spawns──▶ lum mcp ──HTTP──▶ lumd (must already be running)
        stdio (JSON-RPC)
```

Two design points worth copying:

- **It's a client, not a daemon.** Every tool delegates to the REST API
  through `internal/apiclient` — the same typed client the CLI uses.
  The MCP process holds no state, opens no database, and touches no
  gRPC; kill it freely.
- **Typed tools, inferred schemas.** The official Go SDK
  (`modelcontextprotocol/go-sdk`) derives each tool's JSON Schema from
  plain Go structs, validates arguments before our handlers run, and
  returns outputs as structured content. The handler bodies are ~10
  lines each.

One stdio rule: the process must never print to stdout (that's the
protocol channel); diagnostics go to stderr.

## Key dependency choices

| Choice | Over | Because |
|---|---|---|
| qdrant-edge (embedded) | Qdrant server in Docker | zero services; "SQLite for vectors"; beta risk fenced behind the `VectorStore` trait |
| fastembed / ONNX | Ollama or API embeddings | in-process, auto-downloaded, offline after first run |
| SQLite via modernc (pure Go) | mattn/go-sqlite3 (cgo) | `go build` works without a C toolchain |
| channel + worker | NATS/Kafka | right-sized for local-only; interface allows upgrading |
| REST between CLI and daemon | gRPC everywhere | curl-ability; gRPC learning happens on the inter-plane hop |

## Known gaps (deliberate, ordered)

1. **Supervisor doesn't respawn** — if lumen crashes, requests fail until
   `lum serve` is restarted. Restart-with-backoff is a contained exercise
   in `supervisor.go`.
2. **Catalog/vector drift after a hard crash** is prevented by flush
   ordering, but there's no `lum verify` to audit/repair the invariant.
3. **Scan status is coarse** — `lum status` shows counts, not per-scan
   progress. A jobs table in the catalog would fix this.
