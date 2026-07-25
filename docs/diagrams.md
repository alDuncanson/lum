# lum — data flow and architecture diagrams

Companion to [architecture.md](architecture.md). That document explains *why*
the design is what it is; this one shows *what runs where, what data crosses
each boundary, and in what representation*. Every diagram names the files it
describes so it can be checked against the code.

Read in order:

1. [Boundaries and protocols](#1-boundaries-and-protocols) — the map
2. [Process lifecycle](#2-process-lifecycle) — who starts whom, and when they die
3. [Request flows](#3-request-flows) — CLI, Neovim, MCP, curl, `lum top`
4. [Ingestion data flow](#4-ingestion-data-flow) — bytes → vectors
5. [Data representations](#5-data-representations) — the transformation ladder
6. [State machines](#6-state-machines) — readiness, retries, idle nesting
7. [Deletion and ordering invariants](#7-deletion-and-ordering-invariants)
8. [State ownership and identity](#8-state-ownership-and-identity)
9. [Observations on the current state](#9-observations-on-the-current-state)

---

## 1. Boundaries and protocols

Two processes, four client surfaces, five distinct wire protocols. Everything
below the REST line is private implementation.

```mermaid
flowchart TB
    subgraph surfaces["Client surfaces"]
        NVIM["Neovim + Telescope<br/>lua/lum/telescope.lua"]
        CLI["lum CLI<br/>add · search · sources · status · top · stop"]
        AGENT["AI agent<br/>Claude Code · Claude Desktop · Amp"]
        CURL["curl / any HTTP client"]
    end

    MCPSRV["lum mcp<br/>internal/mcpserver<br/>stateless adapter"]

    subgraph lumd["lum serve — the dispatcher"]
        API["internal/api<br/>REST + SSE"]
        INGEST["internal/ingest<br/>scan planner + 1 document runner"]
        SRC["internal/source<br/>LocalDir scan · watch · read"]
        CAT["internal/catalog<br/>SQLite bookkeeping"]
        BUS["internal/events.Bus<br/>in-memory pub/sub, ring 512"]
        MGR["internal/worker<br/>Manager + Supervisor + Client"]
    end

    subgraph wkproc["lum-worker — the worker (private child process)"]
        SVC["service.rs<br/>tonic gRPC server"]
        PIPE["pipeline/<br/>parser → chunker → embedder"]
        STORE["store/edge.rs<br/>qdrant-edge shard"]
    end

    REPO[("repository files<br/>read-only")]
    DB[("~/.lum/catalog.db<br/>SQLite WAL")]
    VEC[("~/.lum/vectors/<br/>+ vectors.manifest.json")]
    MODELS[("~/.lum/models/<br/>ONNX model cache")]
    LOCKS[("~/.lum/daemon.lock<br/>daemon-start.lock<br/>daemon.log")]

    NVIM -->|"A. process spawn: argv in, JSONL on stdout"| CLI
    AGENT -->|"B. JSON-RPC 2.0 over stdio"| MCPSRV
    CLI -->|"C. HTTP/1.1 + JSON, loopback TCP 127.0.0.1:7420"| API
    MCPSRV -->|"C. same REST API via internal/apiclient"| API
    CURL -->|"C. HTTP/1.1 + JSON / text-event-stream"| API

    API --> INGEST
    API --> CAT
    API --> MGR
    API -->|"subscribe + publish"| BUS
    INGEST --> SRC
    INGEST --> CAT
    INGEST -->|"publish"| BUS
    INGEST --> MGR
    MGR -->|"publish rpc_completed"| BUS

    SRC -->|"E. filesystem: WalkDir + read + sha256, fsnotify"| REPO
    CAT -->|"F. SQL over database/sql, modernc pure-Go driver"| DB

    MGR -->|"D1. gRPC / HTTP-2 / protobuf over AF_UNIX ~/.lum/lum-worker.sock (0600)"| SVC
    MGR -->|"D2. process control: fork+exec, stdin pipe as liveness, SIGINT then SIGKILL"| wkproc
    MGR -.->|"D3. child stdout/stderr inherited → daemon.log"| LOCKS

    SVC --> PIPE
    PIPE --> STORE
    PIPE -->|"G. read/download once, then offline"| MODELS
    STORE -->|"H. embedded library calls + flush to disk"| VEC

    lumd -->|"flock for lifetime + start coordination"| LOCKS
```

### Boundary reference

| # | Boundary | Transport | Encoding | What crosses | Direction |
|---|---|---|---|---|---|
| A | Telescope → CLI | `vim.system`-style job spawn via Telescope `finders.new_job` | argv in; newline-delimited JSON on stdout | `--root <repo> --jsonl --limit N -- <prompt>` out; one `SearchResult` per line back | one-shot, per keystroke batch |
| B | Agent → `lum mcp` | stdio pipes | JSON-RPC 2.0 (MCP), schemas inferred from Go structs | 4 tools: `search`, `add_source`, `list_sources`, `status` | bidirectional, long-lived |
| C | Any client → daemon | loopback TCP, `127.0.0.1:7420` (`LUM_HTTP_ADDR`) | JSON request/response; `text/event-stream` for `/v1/events` | the entire public API | request/response + one long-lived stream |
| D1 | Dispatcher → lum-worker | Unix domain socket `~/.lum/lum-worker.sock`, mode 0600 in a 0700 dir | gRPC over HTTP/2, protobuf (`proto/lum/v1/worker.proto`) | 5 RPCs; `x-request-id` metadata on every call | request/response + one client-streaming RPC |
| D2 | Dispatcher → lum-worker | POSIX process control | — | `--grpc-socket`, `--data-dir`, `--embedding-model` argv; stdin pipe held open as a parent-liveness signal (EOF ⇒ exit); `SIGINT`, escalating to `SIGKILL` after 10s | out only |
| D3 | lum-worker → dispatcher | inherited stdout/stderr | `tracing` text lines | logs, merged into `lum serve`'s stream or `daemon.log` | out only |
| E | Dispatcher → repository | filesystem | raw bytes | `WalkDir` + `os.ReadFile` + SHA-256; `fsnotify` inotify/FSEvents watches | read only, never writes |
| F | Dispatcher → catalog | in-process `database/sql` | SQL, `modernc.org/sqlite` (pure Go, no cgo) | sources, documents, hashes, chunk counts, ingest failures | read/write |
| G | lum-worker → model cache | filesystem + HTTPS on first run only | ONNX | `BAAI/bge-small-en-v1.5` (~70 MB) or the `-Q` quantized variant | download once, then read only |
| H | lum-worker → vector index | in-process library (qdrant-edge) | qdrant-edge storage files + a sidecar `vectors.manifest.json` | 384-dim vectors + self-describing chunk payloads | read/write, `flush()` before every ack |

Three properties follow from this table and are worth stating out loud:

- **No client has a privileged side channel.** Telescope shells out to the CLI;
  the CLI and MCP both go through `internal/apiclient`; nothing but the daemon
  opens `catalog.db`, `vectors/`, or `lum-worker.sock`.
- **Nothing binds a network interface except loopback.** The private hop is a
  filesystem socket, so it is reachable only by the owning Unix user, and no
  fixed private port can collide.
- **The narrow contract is load-bearing.** Repository discovery, `.gitignore`,
  watching, debouncing, retries, output formats, Telescope, and MCP all live
  *above* the 5-RPC line. File formats and chunking live entirely *below* it.

---

## 2. Process lifecycle

### 2.1 On-demand daemon start

Every command except `lum stop` and `lum serve` follows this path. `lum stop`
deliberately never spawns — nothing listening means nothing to stop.

```mermaid
sequenceDiagram
    autonumber
    participant C as "any lum client command"
    participant L as "daemon-start.lock (flock)"
    participant D as "lum serve (daemon)"
    participant W as "lum-worker (worker)"

    C->>D: HTTP request
    D--xC: ECONNREFUSED
    Note over C: apiclient.call sees connection refused<br/>(or a 503) and calls ensureDaemon
    C->>L: flock LOCK_EX (poll 250ms, bounded 5 min)
    L-->>C: acquired
    C->>D: GET /v1/status (recheck under the lock)
    D--xC: still refused — we really are the starter
    C->>D: fork+exec "lum serve", setsid, stdout/stderr → daemon.log
    activate D
    D->>D: mkdir 0700 data dir, then flock daemon.lock for the full lifetime
    D->>D: catalog.Open (schema + identity migration)
    D->>W: Spawn lum-worker (argv + stdin liveness pipe)
    activate W
    D->>W: Dial unix://~/.lum/lum-worker.sock (lazy)
    D->>D: bind 127.0.0.1:7420, serve HTTP immediately
    W->>W: bind socket first, then load model on a detached OS thread
    loop until ready or 5 min
        C->>D: GET /v1/status
        D->>W: gRPC Health
        W-->>D: state = starting / downloading-model / ready
        D-->>C: {"worker": "..."}
    end
    W-->>D: ready
    D->>D: for each source: WatchSource + EnqueueScan (startup reconciliation)
    C->>L: flock LOCK_UN
    C->>D: replay the original request
    D-->>C: response
    deactivate W
    deactivate D
```

Key detail: lum-worker **binds its socket before it loads the model**, so `Health`
is answerable during a multi-minute first-run download. And the dispatcher
serves HTTP before lum-worker is ready, so `lum status` works throughout startup.

### 2.2 Startup and shutdown ordering

Ordering here is not incidental — each arrow prevents a specific failure.

```mermaid
flowchart LR
    subgraph up["Startup — cli/serve.go run(), top to bottom"]
        U1["1. data dir 0700"] --> U2["2. flock daemon.lock<br/>(held for the whole lifetime)"]
        U2 --> U3["3. catalog.Open"]
        U3 --> U4["4. Spawn lum-worker"]
        U4 --> U5["5. Dial gRPC (lazy)"]
        U5 --> U6["6. events.Bus"]
        U6 --> U7["7. worker.Manager"]
        U7 --> U8["8. net.Listen — must succeed<br/>before any scan starts"]
        U8 --> U9["9. ingest.New + snapshot loop"]
        U9 --> U10["10. serve HTTP"]
        U10 --> U11["11. async: WaitReady →<br/>watch + scan every source"]
    end

    subgraph down["Shutdown — reverse, via defers"]
        D1["cancelDaemon(): stop planner,<br/>document runner, watchers, debouncers"] --> D2["server.Shutdown (5s drain)"]
        D2 --> D3["defer server.Close / listener.Close"]
        D3 --> D4["dp.Close(): SIGINT lum-worker,<br/>wait ≤10s, else SIGKILL"]
        D4 --> D5["cat.Close()"]
        D5 --> D6["release daemon.lock — the only<br/>authoritative 'fully gone' signal"]
    end

    U11 -.->|"idle timer · SIGINT/SIGTERM · POST /v1/shutdown · HTTP error"| D1
```

Two consequences that clients depend on:

- The HTTP port stops answering at step **D3**, but the process lives through
  **D6**. `apiclient.Client.Stop` therefore waits on `daemon.lock`, not on the
  port. Same signal the on-demand spawn checks before starting a replacement.
- `rm -rf ~/.lum` while the daemon runs is **not** a reset: on Unix the open
  inodes survive the directory entry, so the daemon keeps serving the old
  catalog and index. `lum stop` first.

---

## 3. Request flows

### 3.1 `lum search --root <repo> "query"` — the primary path

```mermaid
sequenceDiagram
    autonumber
    participant U as user
    participant C as "lum search (cobra)"
    participant A as "internal/apiclient"
    participant API as "internal/api"
    participant I as "internal/ingest"
    participant M as "worker.Manager"
    participant W as lum-worker
    participant V as "qdrant-edge"

    U->>C: lum search --root . "where are retries handled?"
    Note over C: PreRunE rejects --json with --jsonl,<br/>and --root with --source
    C->>A: EnsureSource(root)
    A->>API: POST /v1/sources?wait=initial<br/>{"uri": "/abs/path"}
    API->>API: source.Resolve → expand ~, Abs, EvalSymlinks, Clean
    API->>API: catalog.AddSource under sourceMu (idempotent on canonical uri)
    alt newly created
        API->>M: EnsureRunning() (fire and forget)
        API->>I: EnqueueInitialScan + WatchSource
        API->>I: WaitInitialScan — blocks until the first full scan finishes
        Note over I,W: full ingest of the repository happens here<br/>(see section 4)
    else already registered
        Note over API,I: no tracked attempt — returns immediately.<br/>Freshness comes from the watcher / startup rescan, not this call.
    end
    API-->>A: 202 {"source": {...}, "created": bool}
    A-->>C: source id

    C->>A: Search(query, limit, sourceID)
    A->>API: GET /v1/search?q=...&limit=10&source=SOURCE_ID
    API->>API: validate q non-empty, limit in [1,100]
    API->>M: Search(ctx, query, limit, sourceID)
    M->>M: awaitReady — respawn lum-worker if shed/crashed, wait ≤5 min
    M->>W: gRPC Search{query, limit, source_id} + x-request-id
    W->>W: embed_query("query: " + text) → 384-dim f32
    W->>V: nearest-neighbour, cosine, WithPayload, optional source_id filter
    V-->>W: scored points
    W-->>M: repeated SearchResult (document_id, source_id, uri,<br/>chunk_index, score, text, start_line, end_line)
    M->>M: publish rpc_completed{transport: grpc, method: Search}
    M-->>API: []worker.SearchResult
    API-->>A: 200 {"query": ..., "results": [...]}
    A-->>C: []apiv1.SearchResult
    C-->>U: human table / --json envelope / --jsonl lines
```

The chunk **payload is the whole answer** — no join back to the catalog, no
re-read of the file. That is what makes `--jsonl` sufficient for Telescope and
`curl` sufficient for a human.

### 3.2 Neovim / Telescope

Telescope holds no lum-specific state. It is a per-keystroke process spawner
over the CLI, and the debounce is a `sleep` in front of `exec`.

```mermaid
sequenceDiagram
    autonumber
    participant U as user
    participant T as "telescope picker<br/>lua/lum/telescope.lua"
    participant SH as "sh -c debounce wrapper"
    participant C as "lum search --jsonl"
    participant D as "lum daemon"

    U->>T: :Telescope lum
    T->>T: workspace_root: opts.root, else vim.fs.root(buf, ".git"), else cwd
    T->>T: clamp limit to 1..100, convert debounce_ms to seconds
    loop on every prompt change
        T-)SH: kill previous job, spawn new one
        activate SH
        Note over SH: the prompt arrives as a positional parameter,<br/>never as shell source
        SH->>SH: sleep DEBOUNCE
        SH->>C: exec lum search --root ROOT --jsonl --limit N -- PROMPT
        activate C
        C->>D: POST /v1/sources?wait=initial, then GET /v1/search
        D-->>C: results
        C-->>SH: one JSON object per line on stdout
        deactivate C
        SH-->>T: stdout stream
        deactivate SH
        loop per line
            T->>T: make_entry: vim.json.decode
            T->>T: result_path: file:// → fname, other scheme → drop,<br/>relative → joinpath(root)
            T->>T: require path + start_line + score + text, else drop the entry
            T->>T: display "relpath:lnum  score  snippet"
        end
    end
    U->>T: Enter
    T->>U: qflist previewer opens path at lnum (end_lnum carried too)
```

The debounce is a `sleep` in front of an `exec`, which is why the job Telescope
spawns is a shell rather than `lum` directly:

```sh
sh -c 'sleep "$1"; shift; exec "$@"' telescope-lum <seconds> \
  lum search --root <repo> --jsonl --limit <n> -- <prompt>
```

Everything after the script is a positional parameter, so a prompt containing
`$(...)`, backticks, or `;` is never interpreted as shell source. `exec`
replaces the shell, so Telescope's kill of the previous job reaches `lum`
itself and not an orphaned wrapper.

Other notes worth knowing when debugging the picker:

- The sorter is `sorters.highlighter_only` — ordering is **entirely** the
  server's cosine ranking; Telescope only highlights. Fuzzy re-sorting would
  fight the embedding.
- Because each keystroke respawns `lum search`, each keystroke also re-runs
  `POST /v1/sources?wait=initial`. That is cheap once registered (a canonical
  URI lookup) but it is a real round trip per query.
- `result_path` drops any non-`file://` scheme, so a future remote source type
  will not produce broken jump targets.

### 3.3 MCP

```mermaid
sequenceDiagram
    autonumber
    participant AG as "agent (Claude Code, …)"
    participant M as "lum mcp"
    participant A as "internal/apiclient"
    participant D as "lum daemon"

    AG->>M: spawn child process, args ["mcp"]
    AG->>M: JSON-RPC initialize / tools/list (stdin)
    M-->>AG: 4 tools with schemas inferred from Go structs (stdout)
    Note over M: stdout is the protocol channel —<br/>all diagnostics go to stderr, never stdout
    AG->>M: tools/call {"name":"search","arguments":{"query":"...","limit":5}}
    M->>A: api.Search(...)
    A->>D: GET /v1/search (starting the daemon on demand if needed)
    D-->>A: results
    A-->>M: []apiv1.SearchResult
    M-->>AG: structured tool result, validated against the output schema
```

`lum mcp` opens no database, holds no state, and never touches gRPC. It can be
killed freely. The design constraint is deliberate: a capability that is not in
the REST API cannot become an MCP tool.

### 3.4 `lum top` and `GET /v1/events`

```mermaid
sequenceDiagram
    autonumber
    participant T as "lum top (bubbletea)"
    participant A as "apiclient.Events"
    participant API as "handleEvents"
    participant B as "events.Bus"
    participant SNAP as "snapshot.Build"

    T->>A: Events(ctx, nil)
    A->>API: GET /v1/events  (Accept text/event-stream)
    API->>B: Subscribe(64) → channel + ring backlog
    API-->>A: 200, Content-Type text/event-stream
    API->>A: replay ring buffer (up to 512 events, oldest first)
    API->>B: Publish(snapshot.Build(...))  so a new subscriber<br/>does not wait up to 2s for the next tick
    loop live
        B-->>API: Event
        API->>A: SSE frame — "event: KIND" then "data: JSON"
        A->>T: decoded events.Event on a Go channel
        T->>T: topModel.apply — one switch on Kind, no derived semantics
    end
    loop every 15s
        API->>API: onRequest() — a connected observer counts as<br/>daemon activity, but NOT as worker activity
        API->>A: ": heartbeat\n\n"
    end
```

Publishers into the bus, and what each contributes:

```mermaid
flowchart LR
    HTTP["api.withRequestID<br/>every HTTP request"] -->|"rpc_completed transport=http"| BUS[("events.Bus<br/>ring 512")]
    GRPC["worker.Manager.recordRPC"] -->|"rpc_completed transport=grpc"| BUS
    PLAN["ingest planner"] -->|"scan_started / scan_finished"| BUS
    WORK["ingest document runner"] -->|"document_queued → reading →<br/>embedding → ingested / failed / deleted"| BUS
    LOOP["cli.runSnapshotLoop<br/>every 2s"] -->|"snapshot"| BUS
    LOOP -->|"worker_state_changed on transition"| BUS

    BUS --> SSE["GET /v1/events<br/>optional ?types=k1,k2"]
    SSE --> TOP["lum top"]
    SSE --> JQ["curl -N | jq"]
```

The dispatcher never reaches inside lum-worker for finer-grained progress. From
outside, an `IngestBatch` RPC's duration *is* the embedding phase for the batch
it carried — the same opacity the gRPC boundary has everywhere else.

---

## 4. Ingestion data flow

### 4.1 Level 0 — stores and flows

```mermaid
flowchart LR
    REPO[("repository files")]

    subgraph cp["dispatcher"]
        SCAN["LocalDir.Scan<br/>WalkDir + .gitignore + ext allow-list + sha256"]
        DIFF["planScan<br/>diff snapshot against catalog"]
        Q(["documentJob channel<br/>buffered 256"])
        WORKER["documentRunner<br/>read · batch · commit"]
    end

    CATDB[("catalog.db<br/>documents(source_id, uri, content_hash, chunk_count)")]

    subgraph dp["worker"]
        PARSE["ParserRegistry.parse"]
        CHUNK["WordWindowChunker"]
        EMBED["FastEmbedder.embed_passages"]
        UPSERT["EdgeStore.upsert_document + flush"]
    end

    VECDB[("vectors/<br/>points: id=UUIDv5(document_id/chunk_index)<br/>payload: provenance + text + line range")]

    REPO -->|"file bytes"| SCAN
    SCAN -->|"[]DocumentRef{uri, mime, sha256}"| DIFF
    CATDB -->|"known documents + hashes"| DIFF
    DIFF -->|"upsert / delete jobs"| Q
    Q --> WORKER
    REPO -->|"os.ReadFile — only for new/changed"| WORKER
    WORKER -->|"gRPC IngestBatch: header + 256 KiB frames"| PARSE
    PARSE -->|"ParsedText{text, starting_line}"| CHUNK
    CHUNK -->|"[]Chunk{index, text, start_line, end_line}"| EMBED
    EMBED -->|"Vec<Vec<f32>> 384-dim"| UPSERT
    UPSERT --> VECDB
    UPSERT -->|"chunk_count per document, in request order"| WORKER
    WORKER -->|"UpsertDocument(hash, chunk_count, ingested_at)<br/>after the ack — durability before bookkeeping"| CATDB
```

The single most important arrow is the last one: the catalog row is written
*after* qdrant-edge has flushed. Reverse it and a crash makes the next scan see
a matching hash, skip the document forever, and search silently miss it.

### 4.2 Planner and worker internals

```mermaid
flowchart TB
    subgraph triggers["Scan triggers"]
        T1["POST /v1/sources (created)<br/>EnqueueInitialScan"]
        T2["POST /v1/sources/{id}/scan<br/>EnqueueScan — immediate"]
        T3["daemon startup after readiness<br/>EnqueueScan per source"]
        T4["fsnotify change<br/>EnqueueDebouncedScan — 1s quiet window"]
        T5["watch degraded / unwatchable source<br/>5 min ticker"]
        T6["ingest failure backoff<br/>1s, 2s, 4s (retryLimit 3)"]
    end

    T1 & T2 & T3 & T4 & T5 & T6 --> PEND["pending map + scanOrder slice<br/>deduplicated by source_id"]
    PEND --> PLANNER["planner goroutine<br/>one scan at a time, FIFO"]

    PLANNER --> RESOLVE["resolveSource: catalog row → source.FromCatalog"]
    RESOLVE --> SNAPSHOT["Source.Scan → authoritative []DocumentRef"]
    SNAPSHOT --> LOOP{"per ref: catalog lookup<br/>by (source_id, uri)"}
    LOOP -->|"hash matches"| SKIP["unchanged++ · ClearIngestFailure · skip"]
    LOOP -->|"no row"| NEW["mint document id =<br/>UUIDv5(ns, source_id + NUL + uri)"]
    LOOP -->|"hash differs"| CHANGED["reuse existing document id"]
    NEW & CHANGED --> JOB(["jobUpsert"])
    SNAPSHOT --> VANISH["catalog rows not in the snapshot"]
    VANISH --> DELJOB(["jobDelete"])
    SNAPSHOT --> STALEFAIL["failures for URIs neither seen nor known<br/>→ ClearIngestFailure"]
    JOB & DELJOB --> CHAN(["jobs channel, cap 256"])
    PLANNER --> DONE(["jobScanComplete: the barrier"])
    DONE --> CHAN

    CHAN --> WORKER["documentRunner — exactly one"]
    WORKER --> RD["jobUpsert: setActiveWork reading → Source.Read"]
    RD --> LIMIT{"len > 32 MiB?"}
    LIMIT -->|"yes"| FAIL["failDocument"]
    LIMIT -->|"no"| ACC["accumulate: sha256 + IngestBatchDocument"]
    ACC --> FLUSHQ{"flush?<br/>128 docs, or 4 MiB target exceeded"}
    FLUSHQ -->|"yes"| FLUSH["flushBatch → gRPC IngestBatch"]
    WORKER --> DEL["jobDelete: flush first, then DeleteDocument"]
    WORKER --> FIN["jobScanComplete: flush, finishScan,<br/>publish scan_finished, schedule retry"]
```

Why a single document runner: throughput is bounded by the embedding model,
so competing
workers would fight over cores rather than add parallelism. The job channel is
"the event bus in miniature" — a real broker could replace it without changing
what flows through it.

Why the `jobScanComplete` barrier: the planner emits deletes only after the
*complete* snapshot is in hand, and then waits on `run.done` before starting
the next scan. A partial snapshot must never be allowed to look like "these
files vanished".

### 4.3 `IngestBatch` wire framing

The one streaming RPC. Framing exists to stay under gRPC's per-message ceiling
while keeping many small files cheap.

```mermaid
sequenceDiagram
    autonumber
    participant CP as "worker.Client.IngestBatch"
    participant DP as "service.rs ingest_batch"

    CP->>DP: metadata x-request-id (appended manually —<br/>the unary interceptor does not cover streams)
    loop per document, ≤128 per batch
        CP->>DP: Frame::Document{document_id, source_id, uri, mime_type, content_length}
        DP->>DP: reject empty ids, duplicate document_id, >32 MiB declared total
        loop content
            CP->>DP: Frame::Content — exactly 256 KiB, final frame is the exact remainder
            DP->>DP: reject any frame that is not min(remaining, 256 KiB)
        end
        CP->>DP: Frame::EndDocument
        DP->>DP: reject if bytes received ≠ content_length
    end
    CP->>DP: CloseSend
    Note over DP: validation completes before the store is touched at all
    DP->>DP: spawn_blocking: parse + chunk EVERY document first
    DP->>DP: guards: 64 MiB parsed text, 16384 chunks/doc, 32768 chunks/batch
    DP->>DP: embed all chunks in one pass, sub-batched at 64
    DP->>DP: per document: delete_document(filter) then upsert_document, then flush
    DP-->>CP: IngestBatchResponse{documents[]} — one result per header, in request order
    CP->>CP: verify count and per-index document_id match, else fail the whole batch
```

Two design choices visible here:

- **Parse and chunk everything before mutating the store.** An embedding
  failure therefore leaves every previously indexed document intact.
- **Per-document outcomes, not batch-level failure.** A parse error on one file
  becomes an `IngestBatchDocumentFailure{stage, message}` and the rest of the
  batch still commits. Stages are `PARSE`, `RESOURCE_LIMIT`, `STORE`. A
  transport-level error, by contrast, fails all documents in the batch, each
  recorded as its own retryable ingest failure.
- If lum-worker answers `Unimplemented`, the client silently falls back to
  per-document unary `IngestDocument` calls.

---

## 5. Data representations

The same file content wears eight different shapes between disk and screen.

| # | Where | Type | Shape | Key transformation applied |
|---|---|---|---|---|
| 1 | repository | `[]byte` on disk | file bytes | — |
| 2 | `LocalDir.Scan` | `source.DocumentRef` | `{URI, MimeType, ContentHash}` | extension → MIME lookup; SHA-256 of full content; `.gitignore` and hidden-dir exclusion. Deliberately content-free so unchanged files cost no read downstream. |
| 3 | `ingest.planScan` | `catalog.Document` + `documentJob` | `{ID, SourceID, URI}` + job kind | hash diff against `(source_id, uri)`; document ID minted as `UUIDv5(ns, source_id + "\x00" + uri)` — stable across re-ingests |
| 4 | `documentRunner` | `worker.IngestBatchDocument` | `{DocumentID, SourceID, URI, MimeType, Content}` | lazy `os.ReadFile`; 32 MiB reject; SHA-256 recomputed on the bytes actually sent; batched by count and byte target |
| 5 | gRPC wire | protobuf frames | header + N×256 KiB content + end | length-declared framing; `x-request-id` metadata |
| 6 | `pipeline/parser.rs` | `ParsedText` | `{text: String, starting_line: u32}` | UTF-8 lossy decode; markdown strips YAML front matter and advances `starting_line` so line provenance survives |
| 7 | `pipeline/chunker.rs` | `Vec<Chunk>` | `{index, text, start_line, end_line}` | 220-word window, 40-word overlap, whitespace-normalized; per-word line tracking yields inclusive 1-based ranges |
| 8 | `pipeline/embedder.rs` | `Vec<Vec<f32>>` | 384-dim, cosine-normalized | `"passage: " + text` prefix; sub-batched at 64 so an interactive query can take the model mutex between batches |
| 9 | `store/edge.rs` | qdrant-edge `PointStruct` | id `UUIDv5(ns, "{document_id}/{chunk_index}")`, payload `{document_id, source_id, uri, chunk_index, text, start_line, end_line}` | keyword payload indexes on `document_id` and `source_id`; `flush()` before ack |
| 10 | search return | `store::Hit` → `pb::SearchResult` → `worker.SearchResult` → `apiv1.SearchResult` | same 8 fields all the way up | payload round-tripped through `serde_json::Value` so lum-worker depends only on the serialized shape, not qdrant-edge internals. Missing `start_line`/`end_line` degrade to `0` = unknown, for points written by an older worker. |
| 11 | client output | text / JSON / JSONL / Lua table | `uri:start_line  score  snippet` | Telescope maps `uri`→path, `start_line`→`lnum`, `end_line`→`end_lnum` |

Query text takes a much shorter path: `"query: " + text` → 384-dim vector →
cosine ANN. The asymmetric `passage:`/`query:` prefixing is what bge models are
trained with, so it is not cosmetic.

### Request-ID propagation

One correlation ID stitches logs and events across all three languages.

```mermaid
flowchart LR
    H["HTTP middleware<br/>requestid.New — always mints fresh,<br/>an inbound header is ignored"] --> RH["response header x-request-id"]
    H --> CTX["context value"]
    CTX --> SCAN["scanRun.requestID<br/>survives the async hop into the planner"]
    SCAN --> EV["every events.Event.RequestID"]
    SCAN --> GRPC["gRPC metadata x-request-id<br/>unary interceptor + manual on IngestBatch"]
    GRPC --> RS["lum-worker tracing::info!(request_id, ...)"]
    CTX --> GLOG["slog 'HTTP request' / 'worker RPC'"]
```

Background work with no HTTP origin (watch-triggered scans, retries) mints its
own ID via `requestIDFrom`, so nothing is ever uncorrelated.

---

## 6. State machines

### 6.1 Worker readiness

Four states come from lum-worker over gRPC; the last two are synthesized by the
dispatcher and never reported by lum-worker itself.

```mermaid
stateDiagram-v2
    direction LR
    state "starting" as S
    state "downloading-model" as D
    state "ready" as R
    state "unavailable" as U
    state "idle — synthesized by Manager" as I
    state "crashed — synthesized by Manager" as X

    [*] --> S: Spawn + bind socket
    S --> D: initialize() begins on a detached thread
    D --> R: model loaded + shard open + manifest validated
    D --> U: init failed (e.g. manifest/model mismatch)
    R --> I: 5 min with no ingest/search RPC → Supervisor.Stop
    R --> X: process exited on its own (Supervisor.Exited)
    S --> X: never started, or exited during startup
    D --> X: exited while loading the model
    I --> S: EnsureRunning / awaitReady → respawn
    X --> S: EnsureRunning / awaitReady → respawn
    U --> S: respawn on next request
    R --> [*]: daemon shutdown

    note right of R
        Health is side-effect-free.
        GET /v1/status never respawns
        and never counts as activity.
    end note

    note right of X
        Recovery is identical to idle:
        the next request respawns.
        Only the explanation differs,
        and detail carries the exit
        status or spawn error.
    end note
```

`Client.Health` also fails closed on a **contract version mismatch**: if
lum-worker's `contract_version` is not `"2"`, the result is `unavailable` with a
fatal `ContractMismatchError` rather than an optimistic "ready". That guards
against a mixed-build pair where a stale chunk-count field would silently leave
orphaned vectors behind.

### 6.2 Ingest failure and retry

```mermaid
stateDiagram-v2
    state "not indexed" as N
    state "indexed (hash + chunk_count in catalog)" as OK
    state "failed, attempts=n" as F
    state "exhausted, visible in /v1/status" as X

    [*] --> N
    N --> OK: read + IngestBatch + upsert succeed
    N --> F: read error / >32 MiB / parse / store / RPC error
    OK --> F: re-ingest of a changed file fails
    F --> F: source rescan after 1s, 2s, 4s<br/>(already-good documents hash-skip)
    F --> OK: a later attempt succeeds → ClearIngestFailure
    F --> X: attempts > 3 — no further automatic retry
    F --> [*]: file disappeared before it was ever indexed<br/>→ ClearIngestFailure
    X --> OK: any later scan that succeeds clears it
    OK --> [*]: file deleted → vectors removed, then catalog row
```

Failures are keyed `(source_id, uri)` and the retry is scheduled as *another
source reconciliation*, not a per-document retry — which is why it is cheap:
every already-successful document hash-skips during the retry scan. The backoff
is per-scan: `run.retryAfter` takes the **maximum** delay any document in that
scan asked for.

### 6.3 Two nested idle lifetimes

This is the payoff for splitting the processes at all, and the asymmetry in
what counts as activity is deliberate.

```mermaid
flowchart TB
    subgraph outer["lumd — 15 min idle timeout (config.DefaultIdleTimeout)"]
        direction TB
        OA["Resets on: ANY HTTP request<br/>including GET /v1/status<br/>including each 15s SSE heartbeat"]
        subgraph inner["lum-worker — 5 min idle timeout (LUM_WORKER_IDLE_TIMEOUT)"]
            IA["Resets on: IngestBatch · DeleteDocument · Search only"]
            IB["Does NOT reset on: Health, /v1/status, snapshots, SSE"]
        end
    end

    OA -->|"expires → ordered shutdown; next client request<br/>starts a fresh daemon"| OFF(["no lum processes"])
    IA -->|"expires → SIGINT lum-worker, reclaim hundreds of MB<br/>of ONNX model + index"| SHED(["worker: idle"])
    SHED -->|"next add/scan/search: lazy respawn,<br/>awaitReady ≤5 min"| IA
```

Concretely: leaving `lum top` open forever keeps the daemon alive forever (a
connected observer is real activity), but still lets the memory-heavy worker be
shed after 5 idle minutes. Monitoring lum does not keep the model warm.

---

## 7. Deletion and ordering invariants

```mermaid
sequenceDiagram
    autonumber
    participant CL as client
    participant API as "handleDeleteSource"
    participant I as "ingest.DeleteSource"
    participant W as "documentRunner"
    participant DP as lum-worker
    participant CAT as catalog

    CL->>API: DELETE /v1/sources/{id}
    API->>CAT: GetSource — 404 if unknown
    API->>I: DeleteSource(ctx, id) — synchronous, unlike add/scan
    I->>I: beginDelete: mark deleting, cancel watch,<br/>cancel retry, drop pending + debounced scans
    I->>I: wait for any active scan run to finish
    I->>CAT: DocumentsBySource
    loop per document
        I->>W: jobDelete
        W->>DP: gRPC DeleteDocument{document_id}
        DP->>DP: DeletePointsByFilter(document_id == …) then flush
        DP-->>W: ok
        W->>CAT: DeleteDocument(id)
    end
    I->>W: jobScanComplete (barrier)
    alt every document deleted
        I->>CAT: DeleteSource — ON DELETE CASCADE cleans remaining rows
        API-->>CL: 200 {"status": "deleted"}
    else any failure
        Note over I: source and remaining documents are left in place —<br/>not partially cleaned up. Watch is restored.
        API-->>CL: 500 "N of M document(s) failed to delete, retry the delete"
    end
```

The ordering is the invariant: **vectors first, catalog row last.** `ON DELETE
CASCADE` only ever touches catalog rows, never the vector store, so deleting
the source row first would orphan its vectors as unreachable-but-still-
searchable ghosts.

The same rule appears in three places, always the same shape:

| Operation | Must happen first | Must happen second | Failure if reversed |
|---|---|---|---|
| ingest a document | qdrant-edge `flush()` | catalog `UpsertDocument(hash, chunk_count)` | crash loses vectors; next scan hash-skips forever; search silently misses the file |
| delete a document | `DeleteDocument` RPC | catalog `DeleteDocument` | orphaned, still-searchable points with no owning row |
| delete a source | every document's vectors | catalog `DeleteSource` | cascade wipes the rows that were the only way to find the vectors |
| shrinking re-ingest | `delete_document` filtered delete | `upsert_document` of the new, shorter chunk set | stale trailing chunks from the previous, longer version |

And one non-obvious ordering rule in the supervisor: `exec.Command`, **never**
`exec.CommandContext`. CommandContext SIGKILLs the child on context
cancellation, which races and beats the graceful stop that lets qdrant-edge
take its final flush.

---

## 8. State ownership and identity

```mermaid
flowchart TB
    subgraph cpown["Dispatcher owns WHAT EXISTS"]
        S1["sources(id, type, uri UNIQUE, created_at)"]
        S2["documents(id, source_id, uri, content_hash,<br/>chunk_count, ingested_at)<br/>UNIQUE(source_id, uri)"]
        S3["ingest_failures(source_id, uri, attempts, error, failed_at)<br/>PK(source_id, uri)"]
    end

    subgraph dpown["Worker owns WHAT IT MEANS"]
        V1["one point per chunk<br/>384-dim vector + self-describing payload"]
        V2["keyword payload indexes: document_id, source_id"]
        V3["vectors.manifest.json — {model, dimension}"]
    end

    subgraph eph["Ephemeral, no persistence"]
        E1["events ring buffer (512)"]
        E2["scan queue · job channel · debounce/retry timers"]
        E3["initial-scan attempts (daemon-lifetime only)"]
    end

    S2 -.->|"document_id is the only shared key"| V1
    S1 -.->|"source_id is the only shared key"| V1
```

The two stores are never asked each other's question: the catalog is never
queried for "what matches this query?", and the vector index is never
enumerated to answer "what have we ingested?". Every fact has exactly one home,
and `document_id` / `source_id` are the only values that cross.

### Identity and keys

| Identity | Derivation | Why it is shaped that way |
|---|---|---|
| source id | random `uuid.NewString()` at registration | opaque; the canonical `uri` carries the `UNIQUE` constraint that makes registration idempotent |
| canonical source URI | `~` expansion → `filepath.Abs` → `EvalSymlinks` → `Clean` | the same directory reached by two paths must not register twice |
| document id | `UUIDv5(ns, source_id + "\x00" + uri)` | deterministic, so re-registering a repository re-derives the same IDs and re-ingest overwrites in place |
| document uniqueness | `UNIQUE(source_id, uri)`, **not** `uri` alone | a globally unique `uri` let a second overlapping source's scan find and silently adopt the first source's rows — misattributed provenance with no error raised |
| vector point id | `UUIDv5(ns, "{document_id}/{chunk_index}")` | re-ingest overwrites points in place rather than churning new ones |
| vector deletion | filtered delete on the `document_id` payload index | removes the cross-process invariant "catalog `chunk_count` must exactly match points on disk", which could drift after a hard crash |
| index compatibility | `vectors.manifest.json` `{model, dimension}`, plus an explicit legacy identity for pre-manifest indexes | standard and quantized bge produce incompatible vectors; a mismatch must be a startup error, not silently degraded recall |
| contract compatibility | `ContractVersion = "2"` checked on every `Health` | a mixed-build pair could leave stale chunks behind on a shrinking re-ingest |

---

## 9. Observations on the current state

What the "repository semantic search" pivot has and has not reached. Nothing
here is a bug report — these are the places where the product framing and the
implementation are not yet the same shape.

### Fully pivoted

- **Product surface.** `lum search --root <repo>` with idempotent discovery and
  registration means there is no setup step, and Telescope reaches it through
  the same CLI path. README, `docs/architecture.md`, `cli.Root`'s help text,
  and the flake's `lum` / `lum-nvim` outputs all describe one product.
- **Repository-shaped scanning.** Nested `.gitignore` with negation rules,
  hidden-tree exclusion applied consistently by both scans and watches,
  recursive watching with dynamic directory addition, and a 5-minute full
  rescan backstop when watch delivery degrades.
- **Code-shaped results.** Inclusive 1-based line ranges are tracked through
  parse → chunk → payload → gRPC → REST → CLI, never reconstructed by clients.
  That is what makes the Telescope jump-to-line and previewer work, and it is
  the single most repository-specific thing in the pipeline.
- **Packaging as one product.** The Nix wrapper pins `LUM_WORKER_PATH` to a
  store path, so the private worker is not a PATH lookup or a second install.

### Still prose-shaped underneath

These are the real quality ceiling for code search, in rough order of impact:

1. **The chunker is a prose chunker.** `WordWindowChunker` is a 220-word /
   40-word-overlap sliding window with whitespace normalization. Applied to
   source code it splits mid-function, mid-struct, and mid-expression, and
   collapses the indentation that carries much of code's structure. Line
   provenance is exact, but the *boundaries* are word-count driven. A
   syntax-aware chunker (tree-sitter, or even a brace/blank-line heuristic) is
   the highest-leverage change available and, by design, touches one file.
2. **No code parsers exist.** `localdir.go` maps ~50 extensions to MIME types
   like `text/x-go` and `text/x-rust`, but `ParserRegistry::with_defaults`
   registers only `MarkdownParser` and `PlainTextParser` — and `PlainTextParser`
   claims everything under `text/`. So every one of those MIME types resolves
   to lossy UTF-8 decode. The MIME mapping currently gates *which files are
   indexed*, not *how they are understood*. That is a defensible v1, but the
   type information is being computed and then discarded.
3. **The embedding model is trained on English prose.** `bge-small-en-v1.5`
   with `passage:`/`query:` prefixes is a good general retrieval model and a
   great local-first trade, but it is not code-trained. Natural-language →
   code queries ("where are retries handled?") are exactly the asymmetric case
   a code-trained embedder is built for.
4. **Directory exclusion depends on `.gitignore`.** `hiddenDirectory` only
   skips dot-prefixed names, and the comment in `localdir.go` about
   "node_modules-style caches" is not actually implemented. In a repository
   that does not gitignore `node_modules/`, `target/`, `vendor/`, or `dist/`,
   Lum will walk, hash, and index them.
5. **`Scan` reads and SHA-256s every indexable file, every time.** That runs on
   startup, after each debounced watch event, on the 5-minute fallback ticker,
   and on each retry. The code already names the fix (size+mtime pre-filter,
   hash only on suspicion) and it becomes noticeable, not theoretical, on a
   large repository.
6. **No result post-processing.** Results are raw cosine-ranked chunks. There
   is no dedup or collapse by file, no expansion to an enclosing symbol, and no
   re-ranking. Because chunks overlap by 40 words, adjacent chunks of the same
   function can plausibly occupy several of the top 10 slots.

### Freshness semantics worth being explicit about

`--root` guarantees *registration*, not *currency*:

- If the source is newly created, `POST /v1/sources?wait=initial` blocks for
  the entire first scan — correct, but on a large repository the first
  `lum search --root` (or first Telescope keystroke) is a full-index wait.
- If the source already exists from a previous daemon, `WaitInitialScan`
  returns immediately, because the tracked attempt only lives for the daemon
  that created it. Freshness then depends on the startup rescan and the
  watcher, both of which are asynchronous — so a query issued seconds after
  daemon start can legitimately see stale results with no signal that it did.
  A per-source "last completed scan" field in `/v1/status` would make this
  observable; today only aggregate counts are exposed.

### Documentation drift found while diagramming

- **Fixed.** `architecture.md` described a 503 gate on scan-triggering
  endpoints for the "worker still loading" case. That gate
  (`requireDataPlaneReady`) was real but was removed in `e3c8e4a`, replaced by
  `EnsureRunning()` plus a blocking `Manager.awaitReady` one level down; the
  prose had not been updated. The described *behavior* — a transient startup
  state never becoming a persisted ingestion failure — was correct throughout.
  The only 503 the API emits today is `GET /v1/events` with a nil bus.
- The README's `~/.lum/` tree omits `vectors.manifest.json`, which the
  model-switching paragraph further down does mention.

### Known gaps, carried forward from architecture.md

1. No `lum verify` to audit or repair catalog/vector drift after a hard crash.
   Flush ordering prevents it; nothing detects it.
2. Scan status is coarse — counts, not per-scan progress. A jobs table in the
   catalog would fix it, and would also fix the freshness-signal gap above.
