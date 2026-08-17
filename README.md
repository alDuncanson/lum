# lum

> [!WARNING]
> **Highly experimental. Fully vibed. Use at your own risk.**
>
> This is a personal project, written end to end with AI assistance. Nobody
> has run it in production, or for very long. Expect rough edges, breaking
> changes without notice, and decisions that get reversed once something is
> actually measured.
>
> It reads your repository and never writes to it — everything lum creates
> lives under `~/.lum`, and deleting that directory undoes it completely.

Lum is a local semantic code-search engine with a Telescope integration for
Neovim. Point it at a repository, search by meaning instead of by pattern, and
jump to the matching line range. Your code, the embeddings, and the index never
leave the machine.

```sh
$ lum search --root ~/code/lum "where is daemon startup coordinated?"
 1. 0.784  /home/you/code/lum/src/client.rs:165 (chunk 8)
     // Start a daemon, unless someone else is already doing it. Concurrent
     commands converge on one daemon by taking an exclusive lock, rechecking…
```

Nothing to configure, no API keys, no service to operate. One binary. The
embedding model (BAAI/bge-small-en-v1.5) downloads on first use and is cached;
everything after that is offline.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/alDuncanson/lum/main/install.sh | sh
```

Apple Silicon macOS, and Linux on arm64 or x86_64. Downloads the release for
your platform, verifies it against the published `SHA256SUMS`, and installs to
`~/.local/bin`. Nothing else to install: ONNX Runtime is statically linked.

Intel macOS is not supported: lum's inference dependency publishes no prebuilt
ONNX Runtime for it, so there is nothing to link against. Linux needs glibc
2.38 or newer (Ubuntu 24.04, Debian 13, Fedora 39), for the same reason — the
vendored runtime sets the floor.

**Nix**, as a flake input:

```nix
{
  inputs.lum.url = "github:alDuncanson/lum";

  # ... then, wherever you build your packages:
  environment.systemPackages = [ inputs.lum.packages.${pkgs.system}.lum ];
  # or with home-manager:
  home.packages = [ inputs.lum.packages.${pkgs.system}.lum ];
}
```

Or try it without installing anything:

```sh
nix run github:alDuncanson/lum -- search --root . "retry backoff"
```

**Neovim** — add the plugin and run `:LumInstall` once, which does the same
download into Neovim's data directory:

```lua
{ "alDuncanson/lum", dependencies = { "nvim-telescope/telescope.nvim" } }
```

**Prebuilt archives** are on the
[releases page](https://github.com/alDuncanson/lum/releases) if you would
rather not pipe a script to a shell. Unpack and put `lum` anywhere on your
`PATH`.

**From source**, with Rust 1.90+:

```sh
git clone https://github.com/alDuncanson/lum && cd lum
cargo build --release
cp target/release/lum ~/.local/bin/
```

## Quick start

```sh
lum search --root ~/code/my-project "retry backoff"
```

That is the whole setup: `--root` registers the repository, indexes it, and
keeps it current with file watching. Lum starts on demand and stops itself when
idle. The first run downloads the model and embeds the whole repository, and
reports both while it does:

```text
⠇ downloading the embedding model (~130 MB, first run)
⠙ embedding ▕██████████░░░░▏ 64/89 chunks
```

That line is on stderr and only when stderr is a terminal, so piping to `jq`
gets clean JSON. `-q` silences it.

```sh
lum status                      # state, counts, memory, work in flight
lum top                         # live indexing activity
lum remove ~/code/my-project    # unregister and delete its vectors
lum stop
```

## Neovim

```lua
require("telescope").load_extension("lum")
vim.keymap.set("n", "<leader>fs", function()
  require("telescope").extensions.lum.lum()
end)
```

With Nix, as a flake input named `lum`:

```nix
home.packages = [ inputs.lum.packages.${pkgs.system}.lum ];
programs.neovim.plugins = [
  pkgs.vimPlugins.telescope-nvim
  inputs.lum.packages.${pkgs.system}.lum-nvim
];
```

The picker holds one socket open for the session and sends a query per
keystroke down it, so typing costs a write and a read rather than a process
spawn. It also does not block on a first index: a cold repository shows results
as they arrive, with the rest reported as LSP `$/progress` — so whatever
already renders rust-analyzer's progress renders lum's. See
[docs/neovim.md](docs/neovim.md).

## Why results are whole functions

Go, Rust, Python, Nix, Lua and Markdown are parsed with tree-sitter and split
where the language says one thing ends and the next begins, so a result is a
declaration with its doc comment, or a section under its heading, rather than
the last half of one and the first half of the next.

That claim is measured rather than asserted. `nix run .#eval` scores lum
against 54 search phrases with known answers, and every retrieval change in
this repository had to move those numbers — including the two that made things
worse and were reverted.

## What it costs to run

Measured on an M-series Mac against this repository (113 documents, 1119
chunks):

| | |
|---|---|
| a search, warm | 5 ms on an open socket, 10 ms via the CLI |
| a keystroke in the Telescope picker | ~5 ms |
| a search while indexing is running | ~9 ms |
| save a file → re-embedded and searchable | ~180 ms |
| resident memory | ~350 MB idle, ~740 MB peak while indexing |
| the index on disk | 6 MB |

Queries never wait on indexing: the daemon keeps a dedicated inference session
for search, so saving a file costs the picker nothing.

## Documentation

- [docs/neovim.md](docs/neovim.md) — the Telescope extension, progress
  reporting, and indexing before you ask
- [docs/cli.md](docs/cli.md) — the CLI, the socket protocol, MCP, and where
  state lives
- [docs/architecture.md](docs/architecture.md) — how lum works, why it is
  shaped that way, and where it is going
- [docs/diagrams.md](docs/diagrams.md) — data flow, boundaries, and lifecycle
- [eval/README.md](eval/README.md) — how retrieval is measured, and what has
  and has not worked
- [docs/development.md](docs/development.md) — dev shell, the Neovim loop, and
  running the benchmark

## License

[MIT](LICENSE)
