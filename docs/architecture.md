# lum architecture

This document explains how lum is put together and, more importantly,
*why*. It is written to be read alongside the code; every section names
the files it describes.

## Design constraints

These were chosen up front and drive everything else:

1. **Local-only.** Not "local-first with a cloud story" — local, period.
   The public API binds loopback and the private inter-plane hop uses an
   owner-only Unix socket. This deletes entire problem classes (auth, TLS,
   multi-tenancy) and buys a zero-dependency UX.
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
| `internal/cli` | cobra commands; `serve` runs the daemon, the rest are auto-starting HTTP clients |
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
context it needs (document IDs, source IDs, content). It can be killed
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

### Chunk identity and deletion

Vector points get **deterministic IDs**: `UUIDv5(namespace, "{document_id}/{chunk_index}")`,
so re-ingesting the same document_id overwrites points in place instead
of writing new ones — idempotent, and no needless churn.

Deletion (a shrinking re-ingest's stale tail, or an explicit
`DeleteDocument`) is a **filtered delete** on a payload index over
`document_id` (`qdrant-edge`'s `DeletePointsByFilter`), not a chunk
count. Earlier, the wire contract carried `previous_chunk_count` /
`chunk_count` so the data plane could derive exactly which point IDs to
remove — a physical detail of the vector store's layout leaking across
the plane boundary, and a cross-plane invariant ("catalog chunk_count
must exactly match points on disk") that could drift after a hard
crash. The filtered delete removes that invariant entirely: the control
plane no longer tracks or echoes back a count for this to work (#3).

### Document identity and source deletion

Documents are keyed on `(source_id, uri)`, not `uri` alone: nothing
prevents registering two sources whose scans see the same path (an
overlapping directory, or later a source type that reuses another's
URI space), and a globally-unique `uri` let the second source's scan
find and silently adopt the first source's document row —
misattributed provenance with no error raised. Scoping identity to the
source that produced it makes that impossible; each source's rows are
independent even at an identical URI (#4).

`DELETE /v1/sources/{id}` walks every document the source owns, deletes
its vectors from the data plane, *then* deletes its catalog row —
deliberately in that order and synchronously, unlike the fire-and-forget
`POST /v1/sources`. The schema's `ON DELETE CASCADE` only ever cleans up
catalog rows; deleting the source row first, before vectors are
confirmed gone, would orphan them as unreachable-but-still-searchable
ghosts. If any document fails to delete, the source and its remaining
documents are left in place (not partially cleaned up) so the client
gets a clear error and can retry, rather than a 200 papering over
lingering vectors.

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

The control plane and lumen communicate over `lumen.sock` inside the 0700 data
directory, avoiding a fixed private port and preventing other local users from
bypassing the HTTP API. lumen also watches a pipe on stdin and shuts down when
the control plane disappears, including after an ungraceful parent exit.

## Search flow

```
lum search "..." ──▶ GET /v1/search ──▶ gRPC Search
                                          └▶ embed("query: ...") → qdrant-edge
                                             nearest-neighbor (cosine) → hits
```

Chunks are embedded as `"passage: ..."` and queries as `"query: ..."` —
the asymmetric-prefix convention bge models are trained with. An
optional `?source=<id>` restricts results to one source, applied as a
payload-indexed filter on `source_id` (the same mechanism `document_id`
filtering uses for deletes, see below) (#7).

## MCP — `internal/mcpserver`

`lum mcp` speaks the Model Context Protocol over stdio: an agent spawns
it as a child process and calls its tools (`search`, `add_source`,
`list_sources`, `status`) via JSON-RPC on stdin/stdout.

```
agent ──spawns──▶ lum mcp ──HTTP──▶ lumd (started on first tool call)
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

## On-demand daemon lifecycle

CLI commands and MCP tools first try the loopback HTTP API. A refused
connection takes an exclusive `daemon-start.lock` flock, rechecks the API, and
starts a detached `lum serve` only when still needed. This makes concurrent
commands converge on one daemon. The daemon itself holds `daemon.lock` until
all resources finish shutting down, preventing a replacement from overlapping
the old lumen process. Calls poll `/v1/status` through model startup before
replaying the original request; waits are bounded at five minutes and detached
output goes to `daemon.log`.

Every HTTP request resets a 15-minute idle timer. When it expires, the normal
ordered shutdown path stops ingestion, drains HTTP, and gracefully terminates
lumen. The next client request starts a fresh daemon.

`lum stop` (`POST /v1/shutdown`) ends the daemon on demand rather than
waiting out the idle timer, going through the identical ordered path: the
handler responds 202 before signaling, so the client always gets an
answer, then the daemon's main loop selects on that signal exactly like
an OS SIGINT or the idle timer firing. `apiclient.Client.Stop` then waits
for `daemon.lock` to actually release before returning — deliberately
*not* for the HTTP port to stop answering, since `listener.Close()` runs
before `dp.Close()` (stops lumen) and `cat.Close()` in the shutdown
sequence, so the port can go quiet while the process is still mid
cleanup (confirmed by hand: the PID stayed alive nearly a second after
`/v1/status` started refusing connections). The flock is the same
authoritative "fully gone" signal the on-demand spawn above already
relies on before starting a replacement. `Stop` never triggers that
auto-spawn on a refused connection, unlike every other command — nothing
listening just means nothing to stop.

Deleting the data directory is only a full reset once the daemon has
actually exited: on Unix, `rm -rf` only removes the directory entry, and
a still-running process keeps its open files (catalog, vector index)
alive by inode regardless, so it keeps serving the old state and
`lum status`/`lum search` look completely unaffected by the deletion
until the process holding them exits.

## Data plane idle shedding

lumen carries its own, independent (and shorter) idle lifetime nested
inside lumd's: `dataplane.Manager` tracks the last ingest/search RPC and, by
default, gracefully stops lumen after 5 minutes without one — reclaiming the
hundreds of MB the ONNX model and qdrant-edge hold resident, the sleeper
benefit of splitting the two planes into separate processes in the first
place. Deliberately excluded from "activity": `/v1/status` and other health
checks, so monitoring lum doesn't itself keep the model warm.

A request that actually needs the data plane (add source, scan, search)
respawns it lazily and waits for readiness, exactly mirroring the on-demand
daemon flow above one level down: `lum status` reports `data plane: idle`
while shed, `starting`/`downloading-model` while the respawn is in flight,
then `ready`. An unexpected lumen crash is handled identically — Manager
treats a dead supervisor the same as a deliberate shed, so the next request
respawns it rather than failing until `lum serve` is restarted by hand.

## Internal event bus

`internal/events.Bus` is lum's one observability contract: a small
in-memory pub/sub that system components publish to and any number of
subscribers consume from, with a ring buffer (512 events by default) so a
late-joining subscriber gets recent context. There's no persistence —
htop doesn't have history either.

One flat `Event` schema (a tagged union discriminated by `Kind`) covers
two kinds of message:

- **Discrete events**: `scan_started`/`scan_finished` bracket a source
  scan; `document_queued → document_reading → document_embedding →
  document_ingested`/`document_failed` trace one document through the
  pipeline (`document_deleted` for the delete path); `dataplane_state_changed`
  fires on every lumen readiness transition; `rpc_completed` covers
  whole-RPC latency for both the data-plane gRPC hop and the public HTTP
  API.
- **Periodic snapshots** (`kind: snapshot`, every 2s): scan queue depth,
  the document worker's current document and stage, data plane state,
  and index totals — a heartbeat for anything that wants a gauge reading
  rather than tracking discrete events itself.

Every event carries the request ID (#11) so it correlates with logs. The
control plane doesn't reach into lumen for finer-grained visibility: it
knows when it sent an IngestBatch RPC and when it returned, and treats
that duration as the "embedding" phase from outside, the same opaque
treatment of the gRPC boundary described above. This bus is a
prerequisite for `lum top`.

## Events endpoint (SSE)

`GET /v1/events` exposes the bus above the process boundary as
Server-Sent Events: one-directional, plain HTTP, so `curl -N
localhost:7420/v1/events` works with zero client-side dependencies — the
same API-first rule that keeps every other endpoint curl-able. On
connect, the handler replays the ring buffer, publishes one fresh
snapshot (so a new subscriber doesn't wait up to 2s for the next tick),
then streams live; an optional `?types=k1,k2` filter narrows which
`Kind`s are sent. A `: heartbeat` comment every 15s keeps the connection
alive through idle proxies and, deliberately, counts as activity against
lumd's own idle timer (#13) — a connected observability client is real
activity, not just the request that opened the stream.

## `lum top`

A bubbletea TUI over `GET /v1/events` (`internal/cli/top.go`), added
through `internal/apiclient` exactly like every other command — a pure
REST client with no privileged side channel into the daemon. It starts
the daemon on demand on connection refused, the same as any other CLI
command (#13).

The renderer is deliberately dumb: `topModel.apply` folds one `Event` at
a time into display state with a single `switch` on `Kind`, and the
rates it shows (docs/min, chunks/min) are `ingestedTotal / elapsed` — the
same arithmetic a `curl -N | jq` user watching the raw stream could do.
No semantics live in the TUI that aren't already in the event schema
(#19).

## Key dependency choices

| Choice | Over | Because |
|---|---|---|
| qdrant-edge (embedded) | Qdrant server in Docker | zero services; "SQLite for vectors"; beta risk fenced behind the `VectorStore` trait |
| fastembed / ONNX | Ollama or API embeddings | in-process, auto-downloaded, offline after first run |
| SQLite via modernc (pure Go) | mattn/go-sqlite3 (cgo) | `go build` works without a C toolchain |
| channel + worker | NATS/Kafka | right-sized for local-only; interface allows upgrading |
| REST between CLI and daemon | gRPC everywhere | curl-ability; gRPC learning happens on the inter-plane hop |

## Hardening nits (#7)

Small fixes from an architecture review:

- **Swallowed `time.Parse` errors** in catalog row scanners now propagate
  instead of silently producing a zero-value `time.Time` — in practice
  only reachable via DB corruption, since every write goes through
  `time.Format`, but a corrupted row should error, not look like it was
  created at the Unix epoch.
- **`WalkDir` now logs** (`slog.Warn`) when it skips an unreadable path
  during a scan instead of silently continuing. One bad permission still
  doesn't hide every document, but now it's diagnosable instead of an
  invisible gap in the index.
- **gRPC error taxonomy**: `run_blocking` (data-plane) mapped every
  pipeline error to `Status::internal`, including "no parser registered
  for MIME type," which is the caller's fault, not lumen's. A small
  `InvalidArgument` marker error (downcast from the `anyhow::Error`) lets
  it choose `Status::invalid_argument` instead — a first step toward
  distinguishing retryable from permanent failures, not a full
  taxonomy.
- **`SearchRequest` gained a `source_id` filter** (see Search flow above)
  — the first field a real user asks for.

## Known gaps (deliberate, ordered)

1. **Catalog/vector drift after a hard crash** is prevented by flush
   ordering, but there's no `lum verify` to audit/repair the invariant.
2. **Scan status is coarse** — `lum status` shows counts, not per-scan
   progress. A jobs table in the catalog would fix this.
