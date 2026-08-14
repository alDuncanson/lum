# Developing lum

Lum targets Unix-like systems; it listens on a Unix domain socket.

```sh
nix develop                      # pinned Rust toolchain
cargo test                       # fast and hermetic — no model, no index
cargo clippy --all-targets
nix flake check                  # build, tests, clippy, fmt, plugin
nix run . -- serve               # build and run in the foreground
```

One crate at the repository root; `cargo build` produces `lum`.

ONNX Runtime arrives one of two ways, and the difference matters. By default
(the `vendored-onnxruntime` feature) `ort` downloads and statically links its
own build — what `cargo build` and releases use, so the binary runs on
machines without Nix. The Nix package instead builds with
`--no-default-features` and links nixpkgs' copy through `ORT_LIB_LOCATION`,
because a sandboxed build has no network. `scripts/build-release.sh` unsets
those variables so a developer's environment cannot leak a dynamic link into a
release, and fails the build if the binary is not self-contained.

## Measuring retrieval

```sh
nix run .#eval                 # score lum against eval/queries.yaml
nix run .#eval -- --fresh      # wipe the eval index first
```

Changes to parsing, chunking, or the embedding model are hard to judge by
looking at results, so [eval/](../eval/README.md) scores them: recall@k, MRR,
and whether the returned chunk is the part you wanted.

## Neovim loop

```sh
nix run .#nvim                    # build, then open Neovim on the local plugin
nix run .#nvim -- --user-config   # ... using your own Neovim config instead
nix run .#nvim -- --fresh         # ... on an empty index (keeps the model)
nix run .#nvim -- --fresh-model   # ... and re-download the model too
nix develop .#nvim                # or get a shell first: lum-nvim-dev
```

Flags combine in any order. `--fresh` keeps `models/`, because re-fetching the
model on every iteration is slow for no benefit; `--fresh-model` exercises the
download and the notifications around it.

Either mode rebuilds lum from the working tree with `cargo build --release`
(seconds, incrementally) and opens Neovim with Telescope plus this
repository's `lua/` on the runtimepath — so plugin edits need no rebuild at
all, just a restart. `<leader>fs` opens the picker; `:LumRoot <dir>` searches
somewhere else. The isolated config is [dev/nvim.lua](../dev/nvim.lua),
deliberately minimal rather than yours: when a result looks wrong, the only
variable should be lum.

`--user-config` starts your own Neovim instead, with the working-tree lum
attached: your plugins, your notification handler, your keymaps. It needs
Telescope in your configuration; if yours lazy-loads it, open Telescope once
and run `:LumAttach`.

Both modes use `/tmp/lum-dev` as the data directory, so a dev session never
serves from — or pollutes — a real index, and never collides with an installed
lum on the default socket. The shell exports the same settings, so `lum` typed
at the prompt and `lum` invoked from the picker are the same binary against
the same index.

## Cutting a release

`.github/workflows/release.yml` builds three targets on native runners —
`darwin-arm64`, `linux-x86_64`, `linux-arm64` — and publishes tarballs plus a
`SHA256SUMS` file. There is no Intel macOS target: `ort` publishes no prebuilt
ONNX Runtime for it.

```sh
gh workflow run release.yml -f version=0.2.0   # dry run: builds, publishes nothing
git tag v0.2.0 && git push --tags              # the real thing
```

Three things have to agree before a tag is worth pushing, and two are checked
automatically:

- `flake.nix` `version` — the release job refuses a tag that disagrees.
- `lua/lum/install.lua` `M.version` — the `plugin-version` flake check refuses
  a tree where it drifts, because `:LumInstall` would fetch an archive that
  does not exist.
- The Linux runners are `ubuntu-24.04`, and the prebuilt ONNX Runtime sets the
  real floor: glibc 2.38+ (Ubuntu 24.04, Debian 13, Fedora 39).

The workflow deliberately does not build with Nix, though the flake is the
source of truth everywhere else. A binary built against `/nix/store` libraries
will not start on a machine without Nix — the job checks for that and fails
rather than shipping it.
