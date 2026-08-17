# lum architecture

Lum is local semantic code search: point it at a repository, type what you
mean, and jump to the matching line range. Everything — the code, the
embeddings, the index — stays on the machine.

It runs as **one process**: a daemon that owns the index and answers queries,
started on demand by the first command that needs it and gone fifteen minutes
after the last. Clients — the CLI, the Neovim picker, `lum mcp`, anything with
`socat` — all speak the same newline-delimited JSON over one Unix socket.

```
CLI / Neovim / MCP / socat
        │  NDJSON over lum.sock
        ▼
   lum serve ──► engine: scan → parse → chunk → embed → store
        │                                          │
        ▼                                          ▼
   index.rs (vectors, in memory)  ◄──rebuilt──  lum.db (SQLite)
```

## Design constraints

1. **Local-only.** One Unix socket inside a 0700 directory. No port, no auth,
   no TLS, no service to operate.
2. **A keystroke is the latency budget.** The Neovim picker sends a query per
   keystroke, so anything that can make a query wait on indexing is a bug, not
   a tuning problem.
3. **Memory is accounted for.** `lum status` reports its own resident size;
   the daemon should be cheap enough to forget about.
4. **Extension points are interfaces.** New file formats, chunking strategies,
   and languages are new implementations, never architectural changes.
5. **Nothing to install but lum.** First use downloads the embedding model
   (~130 MB, cached); everything after that is offline.

## The pieces

| Module | Responsibility |
|---|---|
| `main.rs`, `cli.rs` | clap commands; `serve` runs the daemon, everything else is a socket client |
| `config.rs` | data dir, socket path, the knobs that bound inference memory |
| `wire.rs` | the protocol: request, reply, event |
| `server.rs` | socket listener, per-connection framing, idle shutdown |
| `client.rs` | socket client, on-demand daemon spawn, lock-based liveness |
| `engine.rs` | sources, scans, planning, ingest, search |
| `db.rs` | one SQLite file — sources, documents, chunks, vectors |
| `index.rs` | in-memory int8 vectors, exhaustive scan, exact rescore |
| `embed.rs` | ONNX Runtime sessions, tokenization, batching, pooling |
| `model.rs` | model download and cache resolution |
| `scan.rs`, `watch.rs`, `mime.rs` | walking, change detection, file typing |
| `parse.rs`, `chunk/`, `language.rs` | bytes → text → chunks, tree-sitter |
| `events.rs` | the broadcast bus every observer reads |
| `render.rs`, `top.rs` | terminal output and the live TUI |
| `mcp.rs` | MCP stdio server, four tools, all socket clients |

## The protocol

Newline-delimited JSON. Every message is one object on one line: a **reply**
carries `id`, matching the request that asked; an **event** carries `event`
and arrives unsolicited after a `subscribe`.

```text
→ {"id":1,"op":"search","q":"retry backoff","limit":10}
← {"id":1,"ok":{"results":[…]}}
← {"event":"progress","phase":"embedding","done":48,"total":96}
```

Chosen because every client can speak it with nothing added: Neovim has
`vim.json` and `vim.uv`, a shell has `jq` and `socat`, and any editor with
async I/O is a couple of hundred lines from a working client
(`lua/lum/client.lua` is the reference). Requests on one connection are
answered concurrently, which is what lets the picker hold a single connection
for a whole session. The full operation and event tables are in
[cli.md](cli.md).

## One database

Sources, documents, chunks, and the vectors themselves live in one SQLite
file. A document, its chunks, and their vectors are written in a single
transaction and removed by a foreign key, so the bookkeeping and the
searchable vectors cannot disagree — there is no write ordering to get right
and no cross-store invariant to audit. Two connections in WAL mode let a query
read while an ingest transaction writes.

The in-memory index is derived state, rebuilt from the database at startup
(489 chunks load in ~120 ms). There is no second artifact to persist, keep in
step, or migrate.

## Search

A flat array, scanned exhaustively — no approximate index. A thousand chunks ×
384 dimensions is a few hundred thousand multiply-adds, well under a
millisecond; even a 100k-chunk monorepo is a few milliseconds of scanning that
autovectorizes. An ANN structure starts winning somewhere past a million
chunks ([#29](https://github.com/alDuncanson/lum/issues/29) sketches the shape
if that day comes).

Two representations, for two jobs:

- **In memory, int8.** Symmetric per-vector quantization: 388 bytes per chunk
  instead of 1536, and the scan is memory-bandwidth-bound so a quarter of the
  bytes is most of a quarter of the time.
- **On disk, f32.** The exact vector, used to rescore the shortlist.

Rescoring makes the quantization invisible: the int8 scan may misorder
near-ties, so the top few hundred are re-scored exactly and re-sorted, and the
result is identical to a full f32 search. Chunk *text* stays on disk and is
fetched only for results, so an index costs its vectors and nothing else
resident.

## Memory

Inference is where the memory is, and the knob that controls it is
`LUM_EMBED_TOKEN_BUDGET`: padded tokens per inference call. Activation memory
scales with rows × padded width, and ONNX Runtime's arena keeps the largest
allocation it has ever served — so the widest batch ever run sets resident
memory for the life of the process. Measured on this repository:

| tokens per call | peak RSS | full index |
|---|---|---|
| 8192 | 1229 MB | 70 s |
| 4096 | 1096 MB | 64 s |
| 2048 | 779 MB | 59 s |
| **1024 (default)** | **748 MB** | **51 s** |

Smaller is both leaner *and* faster — attention is quadratic in the padded
width, and a wide batch spends most of it on padding. Chunks are length-sorted
before batching so the budget stays tight.

Two measured non-knobs, recorded in `embed.rs` so nobody re-derives them:
dropping an ONNX session does not return its arena (it belongs to the
environment, not the session), and thread count moves peak memory by under 3%
while costing 2.5× in indexing speed.

Steady state: ~350 MB with only the query session live; ~740 MB peak while
indexing, plateauing there across repeated re-indexes.

## Two inference sessions

`ort`'s `Session::run` takes `&mut self`, so a session cannot be shared
lock-free — and a query that shares a lock with bulk ingest waits behind a
whole batch, at exactly the moment you search: right after saving a file. So
there are two sessions: one reserved for queries, one belonging to ingest.
A keystroke never waits on indexing — 9 ms median under full indexing load.
The ingest session is dropped when the queue drains, so its weights are not
resident between edits.

## Ingestion

```
lum add ~/code/thing
  └▶ register source ──▶ queue scan ──▶ reply
                             │
              walk with gitignore → refs (uri, mime, size+mtime fingerprint)
                             │
              diff against the database
              ├─ fingerprint unchanged → skip        (the common case)
              ├─ moved → read, hash → unchanged? skip
              ├─ new bytes ──▶ parse → chunk → embed → one transaction
              └─ vanished ──▶ delete (chunks cascade)
```

Scans are stat-only in the common case; only files whose fingerprint moved are
read and hashed (BLAKE3). Files written within the last two seconds are hashed
regardless — a write landing in the same mtime tick with the same size is
git's "racily clean" problem, and it is real on coarse-mtime filesystems.

Scans are idempotent and cheap when nothing changed — a warm rescan of this
repository is ~1 ms — which makes recovery trivial (rescan everything on
startup) and lets the file watcher be approximate: a missed event costs
staleness until the five-minute fallback rescan, never correctness. Saving one
file re-embeds only that file's chunks, ~180 ms from save to searchable.

Failures are recorded per document and retried with 1s/2s/4s backoff. A
failure the input caused — a format with no parser — is recorded as permanent
and skipped by later scans instead of rediscovered forever.

Ingest runs as plain blocking code on its own OS thread; the async runtime
never parks a worker inside ONNX.

## Lifecycle

Commands connect to the socket; a refused connection takes an exclusive
`daemon-start.lock`, rechecks, and spawns a detached `lum serve` only if still
needed, so concurrent commands converge on one daemon.

The daemon holds `daemon.lock` for its entire lifetime, and that lock is the
authoritative liveness signal — deliberately not "the socket answers", because
the listener closes before the database does during shutdown. `lum stop` waits
on the lock; a client watching a failed startup detects the free lock in
milliseconds instead of waiting out a timeout.

Every request resets a 15-minute idle timer; an open event subscription counts
as activity. Restart is cheap because the index reloads from SQLite — under
half a second from `shutting down` to answering queries.

## Events

A broadcast bus with a 512-event ring buffer. Every observer — the CLI
spinner, the Neovim progress bridge, `lum top`, a `socat | jq` pipeline —
reads the same stream and differs only in which kinds it subscribes to.
Nothing computes a number the stream does not carry: `top`'s docs/min is
`indexed / elapsed`, arithmetic anyone watching the raw stream could do.

## Neovim

The picker is a custom async Telescope finder over one socket held for the
session. A query asks for what is indexed *now* and renders it; progress for
the rest arrives on the same connection and is emitted as LSP `$/progress`
from an in-process client that attaches to no buffer — so whatever renders
rust-analyzer's progress renders lum's. A superseded keystroke is cancelled
before its request is sent, not ignored after it answers.

## Extending lum

The seams, from smallest to largest:

- **A file format** is a `Parser` in `parse.rs` plus an extension mapping in
  `mime.rs`.
- **A language** is a tree-sitter grammar dependency and a match arm in
  `language.rs`. Anything without a grammar still indexes, chunked by word
  window.
- **A chunking strategy** is a `Chunker` in `chunk/`.
- **An editor** is a socket client. The protocol is small enough to speak from
  anything with async I/O; `lua/lum/client.lua` is a complete example.

Direction, tracked as issues:

- [#27](https://github.com/alDuncanson/lum/issues/27) — Windows, via named
  pipes behind a transport seam
- [#28](https://github.com/alDuncanson/lum/issues/28) — speak LSP, so editor
  integrations stop being hand-written
- [#29](https://github.com/alDuncanson/lum/issues/29) — a centroid pre-filter
  for the vector scan, if an index ever outgrows the flat scan
- [#30](https://github.com/alDuncanson/lum/issues/30) — hybrid search: BM25
  over the same chunks, fused with the vector ranking

## Known gaps (deliberate, ordered)

1. **A crash in native inference takes the daemon with it.** The cost of one
   process. Recovery is a reconnect, since every client starts a daemon on
   demand — but it is a real tradeoff, not a free one.
2. **The arena is never returned to the OS.** Peak is bounded and plateaus,
   but a process that has indexed once keeps ~700 MB until it idles out.
   Fixing it properly needs `enable_cpu_mem_arena=false`, which `ort` does not
   yet expose.
3. **Scan progress is coarse.** `lum status` shows the current document and
   queue depths, not a per-scan percentage.
