# lum in Neovim

A Telescope extension. It discovers the current Git root, registers it, and
runs `lum search --root <repo> --jsonl`, using the line range in each result to
preview and open the matching code.

It is an ordinary CLI client: no database, no vector index, no private socket —
the same deal every other integration gets.

Install the `lum-nvim` flake output, or add the repository with your plugin
manager.

## Getting the binary

The plugin is Lua; lum is two native binaries, so they arrive separately.

If `lum` is on your `PATH` — a Nix profile, a release you unpacked, a build
from source — the plugin uses it and there is nothing else to do. Otherwise
run `:LumInstall`, which downloads the release for your platform into
`stdpath("data")/lum/<version>/` and verifies it against the published
`SHA256SUMS` before unpacking. `:LumInstall!` re-downloads.

It is a command rather than something that happens on startup: an editor
plugin that quietly fetches ninety megabytes of executable the first time you
open a file is not a thing lum should do. The picker names the command when
the binary is missing, so it is one message and one command.

The version it fetches is pinned in `lua/lum/install.lua` rather than resolved
as "latest", because a plugin and a binary that shipped together are a tested
pair. `nix flake check` fails if that pin disagrees with the flake.

For an internal mirror, set `LUM_RELEASE_BASE_URL` to a location serving the
same archive and `SHA256SUMS` names.


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

## Knowing what it is doing

`notify = true` reports what lum is doing. Off by default: subscribing starts
the daemon, and opening Neovim should not.

Progress is reported as LSP `$/progress`, the same way rust-analyzer reports
indexing. Whatever renders a language server's progress renders lum's — noice,
fidget, snacks — in the same corner, in the same style, stacked alongside it
rather than on top of it:

```text
▕█████████░░░░░▏ embedding 256/443 chunks (57%)  ⠹ indexing lum
▕█████████████░▏ storing 57/69 documents (82%)   ⠼ indexing lum
✔ indexing lum
```

Two operations, so two progress tokens, exactly as rust-analyzer separates
"Roots Scanned" from "Indexing": `lum/model` for the one-time model download
and `lum/index` for the work itself.

lum is not a language server, and does not pretend to be one — it registers an
in-process LSP client that implements `initialize`, `shutdown`, and nothing
else, attaches to no buffer, and exists to emit progress. The tradeoff worth
knowing: `vim.lsp.get_clients()` will list a client called `lum`. Nothing
buffer-scoped sees it, which is what statusline components use.

Anything that draws its own window instead is competing for the same cells as
your notifier, and no zindex settles that — whichever wins hides the other.
lum tried it, and it covered rust-analyzer. Speaking the protocol every
notifier already understands is the fix.

With nothing listening for `LspProgress` there is nothing to see, since core
Neovim records progress but displays none of it. In that case lum falls back
to drawing one line, bottom right — better than silence, and the reason
`mode = "auto"` asks before choosing:

```lua
notify = { progress = { mode = "window" } }   -- or "lsp", or "auto" (default)
```

A successful index produces no notifications at all. Discrete events go
through `vim.notify`, where your notifier renders and persists them:

```text
worker crashed: exited: exit status 3
could not index src/huge.json: document exceeds 32 MiB ingest limit
```

The split is deliberate: `vim.notify` is built for discrete messages, and
progress is a status display. It is why fidget exists separately from
nvim-notify.

Each phase counts whatever unit it actually advances in. Embedding counts
chunks rather than files on purpose: the worker embeds a whole batch at once,
so no file finishes until they all do, but chunks complete steadily throughout
— and they are far more uniform in cost than files, so the bar moves smoothly
instead of lurching. The percentage tracks the current phase rather than the
whole scan, because the phase is the only thing that reports a denominator.

Errors stay on screen until dismissed. Progress stays while it runs. Routine
information times out. Nothing is said about a warm rescan that changed
nothing, idle shedding, or the respawn after it — a channel that reports
non-events is one you learn to ignore.

```lua
notify = {
  verbose = false,     -- add per-document failures, no-op scans, worker churn
  progress = true,     -- false leaves only notifications; a table configures
                       -- it: { mode = "auto" | "lsp" | "window" } plus, for
                       -- the window fallback, anchor / row_offset /
                       -- col_offset / zindex / border / winblend
  min_scan_ms = 750,   -- stay quiet about faster scans that changed nothing
  summary_ms = 4000,   -- how long the completion summary lingers
  timeouts = { info = 4000, warn = 10000, error = false },  -- false = sticky
  opts = { title = "lum" },       -- merged into the vim.notify opts table
  on_event = function(event)      -- or take the raw stream instead
    vim.print(event)
  end,
}
```

## Indexing before you ask

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

## Nothing here is Neovim-specific

`lum events` streams the same thing as newline-delimited JSON, for any
consumer:

```sh
lum events --kinds                        # what can be subscribed to
lum events --types scan_finished          # filtered server-side
lum events --no-replay | jq -r .kind      # only what happens from now on
```
