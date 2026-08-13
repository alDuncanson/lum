# lum architecture

Lum's product boundary is local semantic code search for a repository. Users
interact through `lum search --root <repo>`, the Neovim/Telescope extension, a
Unix socket, or MCP. It runs as **one process**: a daemon that owns the index
and answers queries, started on demand and stopped when idle.

This document explains that design and, more importantly, *why* — including
where the previous design was right, where it was wrong, and which of the
things that sounded like they would help actually did.

See [diagrams.md](diagrams.md) for the same system drawn out.

## The previous design, and what it cost

Until v0.2 lum ran as two processes: a Go **dispatcher** owning orchestration
and every public interface, and a Rust **worker** owning parsing, embedding,
and vector search. They spoke gRPC over a Unix socket; clients spoke HTTP to
the dispatcher. The split was justified on three grounds, and it is worth being
specific about how each held up, because two of them did not.

**"It buys memory."** The worker peaked at 4507 MB — measured, on an index of
811 chunks whose vectors are 1.2 MB — so the dispatcher killed it after five
minutes idle and respawned it on demand. That machinery worked. But the four
gigabytes were never the price of doing inference; they were the price of one
tuning decision: batches of 64 sequences padded to 512 tokens, meeting an arena
allocator that keeps the largest allocation it ever made. Budgeting batches by
padded tokens instead of by rows brings the peak to 740 MB in one process, with
no supervisor, no readiness state machine, and no respawn path. The second
process was solving a problem the batch size created.

**"Crash isolation."** Real, and genuinely lost. A segfault in native inference
code now takes the daemon with it rather than just the worker. The mitigation
is that the daemon is cheap to restart and every client already starts one on
demand, so the observable cost is a reconnect. That is a smaller loss than it
sounds, but it is a loss, and it is the honest entry on this side of the ledger.

**"Two kinds of work, two languages."** True, and the learning goal behind the
whole project. It was achieved. Keeping the split afterwards cost daily
usability for a benefit already banked.

What the split cost, concretely, was most of the code. Of ~6600 non-generated
Go lines, roughly 1500 were the actual work — catalog, walk, watch, planner,
queue, retries. The rest existed because there was a boundary: a supervisor, a
six-state readiness machine with two "why is it absent" causes, an idle shed
with lazy respawn, a gRPC client wrapper, a streaming batch protocol with six
size limits, a contract version, `daemon.lock` plus `daemon-start.lock`, 503
replay logic, and an SSE bridge to carry worker progress back across. Plus 4800
lines of tests holding it in place, a protobuf contract, a code generator, and
its committed output.

It also cost the thing lum is for. Telescope spawned a process per keystroke —
`sh -c 'sleep 0.2; exec lum search …'` — so typing cost 290 ms per character
against 7 ms of actual search, and 1300 ms while indexing, because queries and
ingest shared one mutex around the embedding model.

## Design constraints

1. **Local-only.** Not "local-first with a cloud story" — local, period. One
   Unix socket inside a 0700 directory. No port, no auth, no TLS, no
   multi-tenancy.
2. **One process.** Everything below is downstream of this.
3. **Interactive latency is the budget.** A keystroke in the picker is the
   design target, not a batch job. Anything that can make a query wait for
   indexing is a bug, not a tuning problem.
4. **Interface-driven extension points.** New parsers, chunkers, and languages
   are new implementations, never architectural changes.
5. **Nothing to operate.** No Docker, no protoc, no API keys, no second
   executable. First use needs network access to download the model.

## The modules

| Module | Responsibility |
|---|---|
| `main.rs`, `cli.rs` | clap commands; `serve` runs the daemon, everything else is a socket client |
| `config.rs` | data dir, socket path, the knobs that decide how much memory inference may want |
| `wire.rs` | the protocol: request, reply, event |
| `server.rs` | socket listener, per-connection framing, idle shutdown |
| `client.rs` | socket client, on-demand daemon spawn, lock-based liveness |
| `engine.rs` | the whole pipeline: sources, scans, planning, ingest, search |
| `db.rs` | one SQLite file — sources, documents, chunks, vectors |
| `index.rs` | in-memory int8 vectors, exhaustive scan, exact rescore |
| `embed.rs` | ONNX Runtime sessions, tokenization, batching, pooling |
| `model.rs` | model download and cache resolution |
| `scan.rs`, `watch.rs`, `mime.rs` | walking, change detection, file typing |
| `parse.rs`, `chunk/`, `language.rs` | bytes → text → chunks, tree-sitter |
| `events.rs` | the broadcast bus every observer reads |
| `render.rs`, `top.rs` | terminal output and the live TUI |
| `mcp.rs` | MCP stdio server, four tools, all socket clients |

## The protocol — `wire.rs`

Newline-delimited JSON over one Unix socket. Every message is one JSON object
on one line, and there are exactly two kinds: a **reply** carries `id`, matching
the request that asked for it; an **event** carries `event` and arrives
unsolicited after a `subscribe`.

```text
→ {"id":1,"op":"search","q":"retry backoff","limit":10}
← {"id":1,"ok":{"results":[…]}}
← {"event":"progress","phase":"embedding","done":48,"total":96}
```

This replaced HTTP+JSON on one hop and gRPC+protobuf on another. The reason is
not that JSON is better than protobuf; it is that the two clients that matter
can speak this with nothing added. Neovim has `vim.json` and `vim.uv`; a shell
has `jq` and `socat`. The previous design's API-first rule — "if a feature is
not reachable over HTTP, the CLI cannot have it" — was a good rule that
produced a bad outcome, because the editor's hot path had to reach HTTP through
a process spawn.

Requests on one connection are handled concurrently and answered independently,
which is what lets the picker keep one connection for a session and fire a
query per keystroke down it.

## One database — `db.rs`

Sources, documents, chunks, and the vectors themselves, in one SQLite file.

The previous design split "what exists" (a SQLite catalog) from "what it means"
(a qdrant-edge index), and then spent real effort keeping them agreeing:
deterministic point IDs, filtered deletes, a flush ordered before the catalog
write, a rule named *durability before bookkeeping*, and a known gap for the
drift that could still happen after a hard crash.

All of that was the cost of two stores. With one, a document and its chunks and
their vectors are written in a single transaction and removed by a foreign key.
There is no ordering to get right, no invariant to audit, and no `lum verify` to
write. It is also 10× smaller on disk: 59 MB became 6 MB, because the old store
was 27 MB of segments and 32 MB of write-ahead log around 1.2 MB of vectors.

Two connections, not one. WAL lets a reader proceed while a writer holds the
write lock, so a query does not queue behind an ingest transaction.

## Search — `index.rs`

A flat array, scanned exhaustively. No HNSW, no segments, no second store.

At this scale an approximate structure is not merely unnecessary, it is slower:
811 chunks × 384 dimensions is 311k multiply-adds. Even a 100k-chunk monorepo is
~38M operations, a few milliseconds that autovectorize. HNSW starts winning
somewhere past a million chunks, which is two orders of magnitude beyond what
anyone points a personal code-search tool at.

Two representations, for two jobs:

- **In memory, int8.** Symmetric per-vector quantization: 388 bytes per chunk
  rather than 1536, so 100k chunks is 39 MB resident rather than 153 MB.
- **On disk, f32.** The exact vector, used to rescore the shortlist.

Rescoring is what makes the approximation invisible. The int8 scan may get the
order of near-ties slightly wrong, so the top few hundred are re-scored in f32
and re-sorted, and the result is identical to a full exact search. Chunk *text*
stays on disk and is fetched only for results, so an index costs its vectors and
nothing else in resident memory.

## Memory: what worked and what did not

The rewrite's headline number, and the one that most invited wrong conclusions.
Recorded here because all three of these sounded equally plausible beforehand.

**Token-budgeted batching worked.** Activation memory scales with rows × padded
width, so a fixed row count makes a batch of long chunks cost sixteen times a
batch of short ones — and it is the long batch that sets the arena's high-water
mark forever. Budgeting by padded tokens holds that product flat. Sweeping it on
this repository:

| tokens per call | peak RSS | full index |
|---|---|---|
| 8192 | 1229 MB | 70 s |
| 4096 | 1096 MB | 64 s |
| 2048 | 779 MB | 59 s |
| 1024 | **748 MB** | **51 s** |

Smaller is both leaner *and* faster, because attention is quadratic in the
padded width and a wide batch spends most of it on padding. Length-sorting
before batching is what makes the budget tight, since padding is per batch.

**Dropping the session did not work.** ORT's CPU arena belongs to the
environment, not the session, so releasing the ingest session left RSS
unchanged at 1.2 GB. `release_ingest` still runs — it frees the model weights
and lets the allocator reuse the arena — but it is not why peak memory came
down, and the version of this document that claimed otherwise was wrong.

**Thread count is not a memory knob.** Sweeping ORT's intra-op threads from 8 to
1 moved the peak by under 3% and made indexing 2.5× slower. It is a speed and
politeness knob; indexing gets half the machine's cores because it is background
work competing with whatever the user is actually doing.

Where it lands: ~350 MB with only the query session live, peaking around 740 MB
while indexing and plateauing there across repeated full re-indexes rather than
creeping.

## Concurrency: two sessions — `embed.rs`

The previous build shared one `Mutex<TextEmbedding>` between queries and bulk
ingest. A 64-passage batch held it for 735 ms, so a keystroke in the picker
queued behind one — and you index right after saving a file, which is exactly
when you search.

ort's `Session::run` takes `&mut self`, so a session cannot be shared lock-free.
Rather than tune the lock, there are **two sessions**: one reserved for queries,
one for ingest. A query never waits for indexing because it never touches the
session indexing is using. Measured at 9 ms median under full indexing load,
against 1300 ms before.

The ingest session is created when indexing starts and dropped when the queue
drains, so its weights are not resident during the long stretches when nothing
is being indexed.

## Ingestion — `engine.rs`

```
lum add ~/code/thing
  └▶ register source ──▶ queue scan ──▶ reply
                             │
                    scan (deduped by source)
                             ▼
              walk with gitignore → refs (uri, mime, fingerprint)
                             │
              diff against the database by fingerprint, then hash
              ├─ unchanged → skip (the common case)
              ├─ new/changed ─┐
              └─ vanished ────┴▶ batch → parse → chunk → embed → one transaction
```

Scans are stat-only in the common case: a scan reports a size+mtime fingerprint
and only the files whose fingerprint moved are read and hashed. Hashing the
whole tree on every scan would make watching a large repository cost a full read
of it per save.

Anything written within two seconds is hashed anyway. That is git's "racily
clean" problem: a file modified again inside the same mtime tick, with no change
in size, has an identical fingerprint and its edit is invisible.

Scans are idempotent and cheap when nothing changed, which makes recovery
trivial — rescan everything on startup — and makes it safe for the watcher to be
approximate. `notify` watches recursively, so the previous build's tree-of-
watches bookkeeping is gone. A watch that fails degrades to a five-minute
fallback rescan: latency, not correctness.

Read, parse, and store failures are recorded per document and retried with 1s,
2s, 4s backoff. A failure the input caused rather than lum — a PDF, a file with
no parser — is recorded as *permanent* and skipped by later scans, instead of
being rediscovered three more times per scan.

Ingest runs on its own OS thread as plain synchronous code, because every step
of it is blocking work and expressing that as blocking code avoids parking a
runtime worker inside ONNX.

## Lifecycle

Commands connect to the socket. A refused connection takes an exclusive
`daemon-start.lock`, rechecks, and spawns a detached `lum serve` only if still
needed, so concurrent commands converge on one daemon.

The daemon holds `daemon.lock` for its entire lifetime. A lock that can be taken
is the authoritative "it is fully gone" signal — deliberately not "the socket
stopped answering", because the listener closes before the database and the
model do, so the socket can go quiet while the process is still mid-cleanup.
`lum stop` waits on the lock for that reason, and `serve` takes it before doing
anything else so a failed startup is detectable in milliseconds rather than by
waiting out a timeout.

Every request resets a 15-minute idle timer; an open event subscription counts
as activity, because something is watching this daemon work.

## Events — `events.rs`

A broadcast bus with a 512-event ring buffer. Every observer reads the same
stream: the CLI spinner, the Neovim progress bridge, the Neovim notifier, and
`lum top` differ only in which kinds they subscribe to and what they draw.
Nothing computes a number the stream does not carry — `top`'s docs/min is
`indexed / elapsed`, arithmetic a `jq` user could do.

One flat schema discriminated by `event`, rather than a type per kind:
consumers are a Lua table lookup and a `match`, and a union of twelve shapes
would make both worse for a payload nobody stores.

## Neovim

The picker is a custom async Telescope finder over one socket held for the
session (`lua/lum/client.lua`), rather than `new_job` spawning a process per
keystroke. Two things follow that the old shape could not do:

- **A query does not wait for indexing.** It asks for what is indexed now and
  renders it, while progress for the rest arrives on the same connection. The
  old picker blocked on the first full index, showing nothing, and Telescope
  restarted that wait on every keystroke.
- **A superseded keystroke is cancelled**, not merely ignored. Debouncing is a
  `vim.uv` timer that gets reset, so the request is never sent — where the old
  one was `sleep 0.2` inside a shell that had already been spawned.

Progress is emitted as LSP `$/progress` from an in-process client that attaches
to no buffer (`lua/lum/lsp.lua`). Whatever renders rust-analyzer's progress
renders lum's, in the same place and style. That module is unchanged by this
rewrite: it was transport-agnostic and correct.

## Key dependency choices

| Choice | Over | Because |
|---|---|---|
| one SQLite file | SQLite + qdrant-edge | no cross-store invariant to maintain; 10× smaller on disk |
| flat int8 scan + f32 rescore | HNSW | exact, no index to build, faster below ~1M chunks |
| `ort` directly | fastembed | control over sessions, batching, and the allocator — the whole memory fix |
| `ignore` | a hand-rolled matcher | ripgrep's walker: nested gitignores, negations, already correct |
| NDJSON over a Unix socket | HTTP + gRPC | both clients speak it with nothing added; no codegen |
| `notify` recursive watch | fsnotify + manual recursion | deletes the subtree bookkeeping entirely |

## Known gaps (deliberate, ordered)

1. **A crash in native inference takes the daemon with it.** The cost the
   two-process design was paying for. Recovery is a reconnect, since every
   client already starts a daemon on demand.
2. **The arena is never returned to the OS.** Peak is bounded and plateaus, but
   a process that has indexed once keeps ~700 MB until it idles out. Fixing it
   properly needs `enable_cpu_mem_arena=false`, which `ort` does not expose.
3. **Scan progress is coarse.** `lum status` shows the current document and a
   queue depth, not a per-scan percentage.
4. **No hybrid search.** BM25 alongside the vector scan would be cheap at this
   size and would fix the queries the eval still misses, which are mostly ones
   where the exact identifier is in the file and the phrasing is not.
