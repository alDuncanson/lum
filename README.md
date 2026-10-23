# lum

[![CI](https://github.com/alDuncanson/lum/actions/workflows/ci.yml/badge.svg)](https://github.com/alDuncanson/lum/actions/workflows/ci.yml)
[![Release](https://github.com/alDuncanson/lum/actions/workflows/release.yml/badge.svg)](https://github.com/alDuncanson/lum/actions/workflows/release.yml)
[![Latest release](https://img.shields.io/github/v/release/alDuncanson/lum?label=latest&color=blue)](https://github.com/alDuncanson/lum/releases/latest)
[![License: MIT](https://img.shields.io/github/license/alDuncanson/lum)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/alDuncanson/lum)](https://github.com/alDuncanson/lum/stargazers)

Semantic code search for Neovim. Local, offline, one binary.

lum indexes a repository into embeddings and lets you search it by meaning:
type `retry backoff` or `where is daemon startup coordinated` and jump to the
code that does it, whether or not those words appear in it. It runs entirely
on your machine, downloads a small embedding model on first use, and never
sends your code anywhere. Search from a Telescope picker, from the command
line, or over MCP from a coding agent.

```sh
$ lum search --root ~/code/lum "where is daemon startup coordinated"
 1. 0.784  /home/you/code/lum/src/client.rs:165 (chunk 8)
     // Start a daemon, unless someone else is already doing it. Concurrent
     commands converge on one daemon by taking an exclusive lock, rechecking…
```

> [!WARNING]
> Experimental, and written end to end with AI assistance. lum only reads
> your repository; everything it creates lives under `~/.lum`, and deleting
> that directory undoes it completely.

## Features

- Searches by meaning, and slots in exact-identifier matches so a name you
  half remember still surfaces
- Returns whole functions and sections, not fragments: Go, Rust, Python, Nix,
  Lua, and Markdown are split at syntax boundaries with tree-sitter
- Indexes on first search and keeps itself current by watching for changes;
  a saved file is searchable again in about 180 ms
- Answers a keystroke in a few milliseconds, even while indexing
- Starts on demand and exits when idle; nothing to configure, no API keys, no
  service to run
- Speaks [MCP](https://modelcontextprotocol.io), so Claude Code and other
  agents can search your repositories the same way you do
- Measured, not asserted: every retrieval change is scored in CI against 54
  queries with known answers

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/alDuncanson/lum/main/install.sh | sh
```

Apple Silicon macOS, and Linux on arm64 or x86_64. The script downloads the
release for your platform, verifies it against the published `SHA256SUMS`,
and installs to `~/.local/bin`. Prebuilt archives are on the
[releases page](https://github.com/alDuncanson/lum/releases) if you would
rather not pipe a script into a shell.

Or with Nix, as a flake input:

```nix
inputs.lum.url = "github:alDuncanson/lum";

# then wherever you declare packages
home.packages = [ inputs.lum.packages.${pkgs.system}.lum ];
```

Or from source, with Rust 1.90 or newer:

```sh
cargo install --locked --git https://github.com/alDuncanson/lum
```

Intel macOS is not supported, because the inference runtime lum links
publishes no prebuilt library for it. Linux needs glibc 2.38 or newer. Windows
support is experimental and has no release binaries yet.

## Quick start

```sh
lum search --root ~/code/my-project "retry backoff"
```

That is the whole setup. The first run downloads the embedding model, indexes
the repository, and reports both as it goes; later runs answer in
milliseconds. `--root` registers the repository, so it stays indexed and
watched from then on.

```sh
lum status                      # state, counts, memory, work in flight
lum top                         # live indexing activity
lum remove ~/code/my-project    # unregister and delete its index
lum stop                        # stop the daemon (it also exits when idle)
```

## Neovim

```lua
-- lazy.nvim
{ "alDuncanson/lum", dependencies = { "nvim-telescope/telescope.nvim" } }
```

```lua
require("telescope").load_extension("lum")
vim.keymap.set("n", "<leader>fs", function()
  require("telescope").extensions.lum.lum()
end)
```

Open the picker with the mapping or `:Telescope lum`. If `lum` is not on your
`PATH`, run `:LumInstall` once to fetch the matching release into Neovim's
data directory. Results arrive as you type, and indexing progress renders
wherever your language server's progress does.

With Nix, the flake also packages the plugin:

```nix
programs.neovim.plugins = [
  pkgs.vimPlugins.telescope-nvim
  inputs.lum.packages.${pkgs.system}.lum-nvim
];
```

See [docs/neovim.md](docs/neovim.md) for configuration and progress
reporting.

## Coding agents

`lum mcp` is an MCP server over stdio with four tools: `search`,
`add_source`, `list_sources`, and `status`.

```json
{ "mcpServers": { "lum": { "command": "lum", "args": ["mcp"] } } }
```

## How it works

One process owns everything. A daemon starts on the first command that needs
it, indexes into a single SQLite file, holds the vectors in memory as int8,
and answers newline-delimited JSON over a local socket. Every client, including
the Neovim picker and the MCP server, speaks that same protocol. Retrieval is
an exhaustive vector scan rescored exactly, with BM25 keyword hits slotted in
below the semantic head. The whole design, with the measurements behind each
decision, is in [docs/architecture.md](docs/architecture.md).

## Documentation

- [docs/neovim.md](docs/neovim.md): the Telescope extension, progress
  reporting, and indexing before you ask
- [docs/cli.md](docs/cli.md): every command, the socket protocol, MCP, and
  where state lives
- [docs/architecture.md](docs/architecture.md): how lum works and why it is
  shaped that way
- [eval/README.md](eval/README.md): how retrieval is measured, and what has
  and has not worked
- [docs/development.md](docs/development.md): the dev shell, the Neovim loop,
  running the benchmark, and cutting a release

## License

[MIT](LICENSE)
