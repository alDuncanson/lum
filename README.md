# lum

Lum is a local semantic code-search engine with a Telescope integration for
Neovim. Point it at a repository, search by meaning instead of by pattern, and
jump to the matching line range. Your code, the embeddings, and the index never
leave the machine.

```sh
$ lum search --root ~/code/lum "where is daemon startup coordinated?"
 1. 0.806  /home/you/code/lum/dispatcher/internal/worker/manager.go:421 (chunk 14)
     // idleLoop periodically sheds lum-worker once idleTimeout has elapsed
     since the // last real RPC. Health checks and status polling are…
```

Nothing to configure, no API keys, no service to operate. The embedding model
(BAAI/bge-small-en-v1.5, about 70 MB) downloads on first use and is cached;
everything after that is offline.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/alDuncanson/lum/main/install.sh | sh
```

macOS and Linux, arm64 and x86_64. Downloads the release for your platform,
verifies it against the published `SHA256SUMS`, and installs to
`~/.local/bin`. Nothing else to install: ONNX Runtime is statically linked.

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
rather not pipe a script to a shell. Unpack and put both files in the same
directory on your `PATH` — `lum` finds `lum-worker` beside itself.

**From source**, with Go 1.26+ and Rust 1.97+:

```sh
git clone https://github.com/alDuncanson/lum && cd lum
(cd worker && cargo build --release)
(cd dispatcher && go build -o ../bin/lum ./cmd/lum)
cp worker/target/release/lum-worker bin/
```

## Quick start

```sh
lum search --root ~/code/my-project "retry backoff"
```

That is the whole setup: `--root` registers the repository, indexes it, and
keeps it current with file watching. Lum starts on demand and stops itself when
idle.

```sh
lum status                      # daemon, worker, counts, work in flight
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

Indexing progress is reported as LSP `$/progress`, so whatever already renders
rust-analyzer's progress renders lum's. See [docs/neovim.md](docs/neovim.md).

## Why results are whole functions

Go, Rust, Python, Nix, Lua and Markdown are parsed with tree-sitter and split
where the language says one thing ends and the next begins, so a result is a
declaration with its doc comment, or a section under its heading, rather than
the last half of one and the first half of the next.

That claim is measured rather than asserted. `nix run .#eval` scores lum
against 45 search phrases with known answers, and every retrieval change in
this repository had to move those numbers — including the two that made things
worse and were reverted.

## Documentation

- [docs/neovim.md](docs/neovim.md) — the Telescope extension, progress
  reporting, and indexing before you ask
- [docs/cli.md](docs/cli.md) — CLI, REST, Server-Sent Events, MCP, and where
  state lives
- [docs/architecture.md](docs/architecture.md) — the two-process design, and
  why
- [docs/diagrams.md](docs/diagrams.md) — data flow, protocol boundaries, and
  lifecycle state machines
- [eval/README.md](eval/README.md) — how retrieval is measured, and what has
  and has not worked
- [docs/development.md](docs/development.md) — dev shell, the Neovim loop, and
  running the benchmark

## License

[MIT](LICENSE)
