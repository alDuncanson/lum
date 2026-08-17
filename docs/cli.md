# The CLI, the socket, and MCP

One binary. `lum serve` is the daemon; every other subcommand is a client of
its socket, and starts it if nothing is listening.

## Commands

```sh
lum search <query...>     # semantic search
lum add <path>            # register a directory and index it
lum remove <id | path>    # unregister and delete everything indexed from it
lum sources               # list registered directories
lum scan <id | path>      # rescan now
lum status                # state, counts, memory, work in flight
lum top                   # live activity
lum stop                  # stop the daemon
lum serve                 # run the daemon in the foreground
lum mcp                   # speak MCP on stdin/stdout
lum reindex --force       # discard every embedding and index again
```

### search

```sh
lum search --root ~/code/thing "retry backoff"
```

`--root` registers the directory if it is new, waits for its first index, and
restricts results to it. That is what makes lum need no setup: the first search
in a repository is also the command that starts indexing it.

| flag | default | |
|---|---|---|
| `--limit N` | 10 | maximum results |
| `--root PATH` | | ensure and search only this workspace |
| `--source ID` | | restrict to one registered source |
| `--per-file N` | 2 | chunks any one file may contribute; `0` returns raw nearest neighbours |
| `--no-tests` | off | omit test files |
| `--json` | | one JSON envelope |
| `--jsonl` | | one JSON result per line |
| `-q`, `--quiet` | | no progress on stderr |

`--per-file` exists because nearest-neighbour search returns chunks, and a
question about one file is usually answered by several of them. Left alone,
three chunks of the same file take three of the five slots anyone reads.

`--no-tests` is all-or-nothing on purpose. Tests describe the feature they
exercise in prose-like assertion names, so they outrank implementations for
several queries — but scaling test scores down by 0.95, 0.9, 0.8 and 0 made
every retrieval metric monotonically worse once the fixture contained queries
looking *for* a test, which people do. See [eval/README.md](../eval/README.md).

Progress is drawn on stderr, only when stderr is a terminal and `TERM` is not
`dumb`. Piping to `jq` gets clean JSON.

## The socket

`$LUM_DATA_DIR/lum.sock`, inside a 0700 directory. Newline-delimited JSON: one
object per line, in both directions. (On Windows — experimental, no release
binaries yet — the endpoint is the named pipe `\\.\pipe\lum-<data-dir>`
instead; everything else is identical.)

A line with `id` is a reply to the request that carried that `id`. A line with
`event` is an unsolicited event, sent after `subscribe`. Requests may be
pipelined and are answered independently.

```sh
# Everything the CLI does, without the CLI.
printf '{"id":1,"op":"search","q":"retry backoff","limit":5}\n' \
  | socat - UNIX-CONNECT:$HOME/.lum/lum.sock | jq
```

### Operations

| `op` | fields | |
|---|---|---|
| `ping` | | liveness, without touching the model or the index |
| `search` | `q`, `limit`, `root`, `source`, `per_file`, `exclude_tests`, `wait` | |
| `add_source` | `uri`, `wait` | |
| `remove_source` | `source` | an id or a path |
| `list_sources` | | |
| `scan` | `source` | queue a rescan |
| `status` | | |
| `subscribe` | `kinds`, `replay` | stream events on this connection |
| `shutdown` | | |

`wait` is the interesting one. The CLI sets it, because `lum search --root .`
on a cold repository promising results it does not have would be a lie. The
Neovim picker does not: it wants whatever is indexed *this keystroke*, and
watches progress events on the same connection to know more is coming.

Note that `remove_source` and `scan` take `source`, not `id` — `id` belongs to
the envelope, and a field of that name inside an operation is silently
shadowed by it.

### Events

`subscribe` with `kinds: []` for all of them, or name the ones you want.
`replay: true` replays a 512-event ring buffer, which is right for a display
(`lum top`) and wrong for anything that reacts to events, since it would
announce work that finished before you connected.

| `event` | |
|---|---|
| `state` | `starting` → `downloading-model` → `ready`, or `failed` |
| `scan_started`, `scan_finished`, `scan_failed` | brackets one source scan |
| `doc_indexed`, `doc_deleted`, `doc_failed` | one document |
| `progress` | `phase`, `done`, `total`, `unit` — the part that moves |
| `snapshot` | every 2s: counts, queue depths, resident bytes |
| `request` | whole-request latency, for `lum top` |

```sh
# Watch indexing from a shell.
printf '{"id":1,"op":"subscribe","kinds":["progress","doc_indexed"]}\n' \
  | socat - UNIX-CONNECT:$HOME/.lum/lum.sock | jq -c
```

## MCP

`lum mcp` speaks the Model Context Protocol over stdio: an agent spawns it and
calls its tools over JSON-RPC. Four tools — `search`, `add_source`,
`list_sources`, `status`.

It is a client, not a daemon. Every tool goes through the same socket the CLI
uses; the MCP process holds no state, opens no database, and loads no model.
Kill it freely.

```json
{ "mcpServers": { "lum": { "command": "lum", "args": ["mcp"] } } }
```

One stdio rule: it never writes to stdout except protocol messages, because
stdout *is* the channel. Diagnostics go to stderr.

## Where state lives

Everything is under `~/.lum` (or `$LUM_DATA_DIR`), and deleting it resets lum
completely.

```
lum.db              sources, documents, chunks, vectors  (plus -wal, -shm)
models/             the embedding model, in HuggingFace cache layout
lum.sock            the socket
daemon.lock         held for the daemon's lifetime
daemon-start.lock   held while deciding whether to spawn
daemon.log          the daemon's stderr
```

Deleting the directory is a full reset only once the daemon has exited. On
Unix, `rm -rf` removes the directory entry but a running process keeps its open
files alive by inode, so it keeps serving the old index and `lum status` looks
unaffected until it exits. `lum stop` first.

Upgrading from lum 0.1: its `catalog.db`, `vectors/` and `lum-worker.sock`
are left in place and reported once at startup. They are yours to delete; lum
will not remove an index it did not write.

## Configuration

| variable | default | |
|---|---|---|
| `LUM_DATA_DIR` | `~/.lum` | everything lum persists |
| `LUM_IDLE_TIMEOUT` | `15m` | daemon exits after this long idle |
| `LUM_STARTUP_TIMEOUT` | `5m` | bounds a client's wait, including the first download |
| `LUM_EMBED_TOKEN_BUDGET` | `1024` | padded tokens per inference call — the memory knob |
| `LUM_EMBED_BATCH_SIZE` | `16` | ceiling on rows per call |
| `LUM_EMBED_THREADS` | half the cores | inference threads; a speed knob, not a memory one |
| `LUM_EXCLUDE_DIRS` | `node_modules,vendor,target,__pycache__` | replaces the list; set empty to disable |
| `LUM_EMBEDDING_MODEL` | `standard` | or `quantized`; changing it requires `lum reindex` |
| `LUM_LOG` | `lum=info,warn` | tracing filter |

Durations accept `500ms`, `90s`, `5m`, `2h`.

`LUM_EMBED_TOKEN_BUDGET` is the one worth knowing about. It bounds padded
tokens per inference call, which is what activation memory actually scales
with; smaller is both leaner and faster, up to a point. The measurements are
in [architecture.md](architecture.md#memory).
