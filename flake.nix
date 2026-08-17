{
  description = "Reproducible builds and development environment for lum";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, crane, rust-overlay }:
    flake-utils.lib.eachSystem [
      "aarch64-darwin"
      "x86_64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ] (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
        lib = pkgs.lib;
        version = "0.2.0";
        rustToolchain = pkgs.rust-bin.stable."1.90.0".default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # One crate at the repository root, so this is Crane's ordinary path:
        # no nested manifest, no protobuf contract to keep in the source tree,
        # no second package to wrap around the first.
        #
        # A denylist, not an allowlist. The allowlist this replaced named `src`
        # and the manifests, and silently omitted `rustfmt.toml` — so the
        # sandboxed fmt check ran with rustfmt's defaults and failed on 164
        # diffs that `cargo fmt` calls clean. Anything else it forgot would have
        # failed the same quiet way, and `tests/` needs `eval/` while the eval
        # fixture asserts against paths all over the repository, so the set of
        # things that must be present is closer to "everything" than to a list
        # worth maintaining.
        #
        # The cost is that editing a doc rebuilds the crate. It does not rebuild
        # the dependencies — `buildDepsOnly` is keyed on the lockfile — so that
        # is ~30 s, not the ONNX Runtime build.
        rustSrc = lib.cleanSourceWith {
          src = lib.cleanSource ./.;
          filter = path: type:
            let rel = lib.removePrefix "${toString ./.}/" (toString path);
            in !(rel == "target" || lib.hasPrefix "target/" rel
              || rel == "result" || lib.hasPrefix "result/" rel
              || rel == "dist" || lib.hasPrefix "dist/" rel);
        };
        rustArgs = {
          pname = "lum";
          inherit version;
          src = rustSrc;
          strictDeps = true;
          buildInputs = [ pkgs.onnxruntime ];
          # Link against nixpkgs' ONNX Runtime rather than letting ort download
          # its own: a fixed-output derivation cannot reach the network, and a
          # vendored copy would be a second unmanaged dependency in a build
          # whose entire point is being managed.
          ORT_LIB_LOCATION = "${lib.getLib pkgs.onnxruntime}/lib";
          ORT_PREFER_DYNAMIC_LINK = "1";
          # Drops the `vendored-onnxruntime` default feature, and with it ort's
          # downloader — which is `ureq` + `native-tls`, so on Linux it wants
          # openssl and pkg-config in the sandbox to fetch a file this build
          # does not fetch. Applies to every crane invocation below so they all
          # share one set of artifacts.
          cargoExtraArgs = "--no-default-features";
        };
        cargoArtifacts = craneLib.buildDepsOnly rustArgs;
        lum = craneLib.buildPackage (rustArgs // {
          inherit cargoArtifacts;
          doCheck = false; # run separately as a check, so a failure names itself
        });

        nvimSrc = lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            let rel = lib.removePrefix "${toString ./.}/" (toString path);
            in rel == "lua" || lib.hasPrefix "lua/" rel
               || rel == "plugin" || lib.hasPrefix "plugin/" rel;
        };
        lum-nvim = pkgs.vimUtils.buildVimPlugin {
          pname = "lum-nvim";
          inherit version;
          src = nvimSrc;
          dependencies = [ pkgs.vimPlugins.telescope-nvim ];
        };

        # Shared by the Neovim loop and the eval harness: build from the
        # working tree, into a data directory that is not your real index.
        #
        # `cargo build` rather than a Nix build on purpose. A Nix rebuild of
        # this crate is minutes because of ONNX Runtime; an incremental cargo
        # build of a Lua-adjacent change is seconds, and the whole value of a
        # dev loop is the length of the loop.
        devPreamble = dataDir: ''
          root=$(git rev-parse --show-toplevel)

          # A dedicated data directory, so a dev session never serves from — or
          # pollutes — a real index. Short path: the socket lives here and Unix
          # socket addresses are length-limited.
          export LUM_DATA_DIR="''${LUM_DATA_DIR:-${dataDir}}"
          # Deliberately no ORT_LIB_LOCATION here. Setting it would relink
          # against nixpkgs' onnxruntime, forcing a full rebuild of ort-sys on
          # the first dev launch and running a different runtime version than
          # a release does. The sandboxed Nix package sets it because it has no
          # network; a dev loop does.

          fresh=0
          freshModel=0
          userConfig=0
          args=()
          for arg in "$@"; do
            case "$arg" in
              --fresh) fresh=1 ;;
              --fresh-model) fresh=1; freshModel=1 ;;
              --user-config) userConfig=1 ;;
              *) args+=("$arg") ;;
            esac
          done

          echo "building lum from $root ..."
          (cd "$root" && cargo build --release)
          export PATH="$root/target/release:$PATH"

          # A dev daemon left over from the previous launch still holds the
          # socket, and every command talks to whatever answers — so without
          # this, the build that just finished would not be the one under test.
          # It also holds the database open, which is why --fresh has to stop it
          # before deleting anything.
          lum stop >/dev/null 2>&1 || true
          if [ "$freshModel" = "1" ]; then
            echo "clearing $LUM_DATA_DIR including the model (~133 MB will download again)"
            rm -rf "''${LUM_DATA_DIR:?}"
          elif [ "$fresh" = "1" ]; then
            # models/ survives. It never changes, and re-fetching it on every
            # iteration makes the loop slow for no benefit; --fresh-model is
            # there for exercising the download.
            echo "clearing the index in $LUM_DATA_DIR (keeping the model; --fresh-model to re-download)"
            if [ -d "''${LUM_DATA_DIR:?}" ]; then
              find "''${LUM_DATA_DIR:?}" -mindepth 1 -maxdepth 1 ! -name models -exec rm -rf {} +
            fi
          fi
          mkdir -p "$LUM_DATA_DIR"
        '';

        # The Neovim development loop, as one command.
        lum-nvim-dev = pkgs.writeShellApplication {
          name = "lum-nvim-dev";
          # Deliberately no neovim here. writeShellApplication puts
          # runtimeInputs first on PATH, so including it would shadow the
          # user's own Neovim — and --user-config exists precisely to run
          # theirs, with their plugins and their notification handler. The
          # isolated mode references the pinned one by store path instead.
          runtimeInputs = [ pkgs.git rustToolchain ];
          text = (devPreamble "/tmp/lum-dev") + ''
            echo "lum:  $(command -v lum)"
            echo "data: $LUM_DATA_DIR"

            export LUM_DEV_REPO="$root"
            export LUM_DEV_TELESCOPE="${pkgs.vimPlugins.telescope-nvim}"
            export LUM_DEV_PLENARY="${pkgs.vimPlugins.plenary-nvim}"

            # --user-config runs your own Neovim — your plugins, your
            # notification handler — with the working-tree lum attached,
            # instead of the isolated config in dev/nvim.lua. Useful once you
            # want to see the integration the way you actually use it; the
            # isolated config stays the better place to debug lum itself, since
            # it has no other plugins to blame.
            if [ "$userConfig" = "1" ]; then
              if ! command -v nvim >/dev/null 2>&1; then
                echo "--user-config runs your own Neovim, but no nvim is on PATH." >&2
                echo "Run lum-nvim-dev without --user-config to use the pinned one." >&2
                exit 1
              fi
              echo "nvim: $(command -v nvim) (your configuration)"
              # --cmd runs before init so the runtimepath is in place for
              # configs that load the extension themselves. attach.lua sets it
              # again at VimEnter, because plugin managers rewrite runtimepath
              # and would otherwise drop it.
              exec nvim --cmd "set runtimepath^=$root" \
                -c "lua dofile('$root/dev/attach.lua')" ''${args[@]+"''${args[@]}"}
            fi
            echo "nvim: ${pkgs.neovim}/bin/nvim (isolated config)"
            exec ${pkgs.neovim}/bin/nvim -u "$root/dev/nvim.lua" ''${args[@]+"''${args[@]}"}
          '';
        };

        # Retrieval evaluation against eval/queries.yaml.
        #
        # Its own data directory, so a measurement never depends on — or
        # disturbs — whatever is in your real index. --fresh clears it, which is
        # what you want across a chunker or model change, since those invalidate
        # every existing vector.
        lum-eval = pkgs.writeShellApplication {
          name = "lum-eval";
          runtimeInputs = [ pkgs.git rustToolchain ];
          text = (devPreamble "/tmp/lum-eval") + ''
            # Keep eval/ out of the index. queries.yaml contains the phrases
            # verbatim, so indexing it made the fixture the best match for its
            # own queries — it appeared in the top five for half of them,
            # measuring the benchmark against itself. The first four mirror the
            # built-in defaults, which this replaces.
            export LUM_EXCLUDE_DIRS="node_modules,vendor,target,__pycache__,eval"

            cd "$root"
            cargo test --release --test eval -- --nocapture --ignored ''${args[@]+"''${args[@]}"}
          '';
        };
      in {
        packages = {
          inherit lum lum-nvim lum-nvim-dev lum-eval;
          default = lum;
        };

        apps = let
          lumApp = flake-utils.lib.mkApp { drv = lum; exePath = "/bin/lum"; };
        in {
          lum = lumApp;
          default = lumApp;
          # `nix run .#nvim` — the Neovim loop without entering a shell first.
          nvim = flake-utils.lib.mkApp {
            drv = lum-nvim-dev;
            exePath = "/bin/lum-nvim-dev";
          };
          # `nix run .#eval` — measure retrieval against eval/queries.yaml.
          eval = flake-utils.lib.mkApp {
            drv = lum-eval;
            exePath = "/bin/lum-eval";
          };
        };

        checks = {
          rust-tests = craneLib.cargoTest (rustArgs // { inherit cargoArtifacts; });
          clippy = craneLib.cargoClippy (rustArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });
          formatting = craneLib.cargoFmt { inherit (rustArgs) pname version src; };
          nvim-plugin = lum-nvim;
          packaged-version = pkgs.runCommand "lum-packaged-version-${version}" { } ''
            test "$(${lum}/bin/lum --version)" = "lum ${version}"
            touch $out
          '';

          # lua/lum/install.lua pins the release `:LumInstall` downloads. If it
          # drifts from the version here, that command fetches an archive that
          # either does not exist or was built from different code — and the
          # failure lands on a stranger's first use of the plugin, not here.
          plugin-version = pkgs.runCommand "lum-plugin-version-${version}" { } ''
            pinned=$(${pkgs.gnugrep}/bin/grep -oE 'M\.version = "[0-9]+\.[0-9]+\.[0-9]+"' \
              ${./lua/lum/install.lua} | ${pkgs.gnugrep}/bin/grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
            if [ "$pinned" != "${version}" ]; then
              echo "lua/lum/install.lua pins $pinned but flake.nix declares ${version}" >&2
              exit 1
            fi
            touch $out
          '';
        };

        devShells = {
          default = pkgs.mkShell {
            packages = [ rustToolchain pkgs.curl pkgs.perl ];
            shellHook = ''
              echo "lum dev shell. Neovim loop: nix develop .#nvim   (or nix run .#nvim)"
            '';
          };

          # Kept separate from the default shell so building lum does not pull
          # in Neovim and Telescope for people who never touch the plugin.
          #
          # The hook exports the same environment lum-nvim-dev uses, so `lum`
          # run directly in this shell and `lum` run from inside Neovim are the
          # same binary against the same index. Without that, a session would
          # have two different lums depending on where you typed it. No neovim
          # in packages: it would shadow the user's own, which --user-config
          # needs. lum-nvim-dev pins the isolated one by store path.
          nvim = pkgs.mkShell {
            packages = [ rustToolchain pkgs.git lum-nvim-dev ];
            shellHook = ''
              export LUM_DATA_DIR="''${LUM_DATA_DIR:-/tmp/lum-dev}"
              if root=$(git rev-parse --show-toplevel 2>/dev/null); then
                export PATH="$root/target/release:$PATH"
              fi
              echo "lum Neovim dev shell."
              echo "  lum-nvim-dev   build lum and open Neovim with the local plugin"
              echo "  data $LUM_DATA_DIR"
            '';
          };
        };
      });
}
