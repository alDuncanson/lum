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
        version = "0.1.0";
        rustToolchain = pkgs.rust-bin.stable."1.97.1".default;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # The worker build script reads ../proto, so retain both the Cargo
        # project and the shared protocol contract in Crane's source tree.
        rustSrc = lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            let rel = lib.removePrefix "${toString ./.}/" (toString path);
            in rel == "worker" || lib.hasPrefix "worker/" rel
              || rel == "proto" || lib.hasPrefix "proto/" rel;
        };
        rustArgs = {
          pname = "lum-worker";
          inherit version;
          src = rustSrc;
          cargoLock = ./worker/Cargo.lock;
          # Crane installs the lockfile's dependency set through its vendored
          # fixed-output derivation. It stages that lock at the source root, so
          # Cargo cannot also use --locked with this nested manifest path.
          cargoExtraArgs = "--manifest-path worker/Cargo.toml";
          # Keep build and install paths stable even though Cargo is driven
          # through a nested manifest.
          CARGO_TARGET_DIR = "target";
          strictDeps = true;
          buildInputs = [ pkgs.onnxruntime ];
          ORT_LIB_LOCATION = "${lib.getLib pkgs.onnxruntime}/lib";
          ORT_PREFER_DYNAMIC_LINK = "1";
        };
        lum-worker = craneLib.buildPackage (rustArgs // {
          # Dummification does not understand this nested manifest and would
          # compile lum-worker itself in the dependency derivation. A single build
          # is both faster and substantially smaller on disk.
          cargoArtifacts = null;
          doCheck = false;
          # Crane's default installer asks Cargo for root-manifest metadata;
          # this repository deliberately keeps the manifest in worker/.
          doNotPostBuildInstallCargoBinaries = true;
          installPhaseCommand = ''
            mkdir -p $out/bin
            cp target/release/lum-worker $out/bin/lum-worker
          '';
        });

        lum-unwrapped = pkgs.buildGoModule {
          pname = "lum";
          inherit version;
          src = ./dispatcher;
          vendorHash = "sha256-e02pNlwlTXjnK9Kg9TKI73nUqDuHD75ZlaU39jaaT9k=";
          subPackages = [ "cmd/lum" ];
          ldflags = [
            "-X github.com/alDuncanson/lum/dispatcher/internal/version.Value=${version}"
          ];
        };

        # Lum is one product even though it runs as two processes. Pin the
        # worker by store path instead of relying on symlink resolution or the
        # caller's PATH.
        lum = pkgs.runCommand "lum-${version}" {
          nativeBuildInputs = [ pkgs.makeWrapper ];
        } ''
          mkdir -p $out/bin
          makeWrapper ${lum-unwrapped}/bin/lum $out/bin/lum \
            --set-default LUM_WORKER_PATH ${lum-worker}/bin/lum-worker
        '';

        nvimSrc = lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            let rel = lib.removePrefix "${toString ./.}/" (toString path);
            in rel == "lua" || lib.hasPrefix "lua/" rel;
        };
        lum-nvim = pkgs.vimUtils.buildVimPlugin {
          pname = "lum-nvim";
          inherit version;
          src = nvimSrc;
          dependencies = [ pkgs.vimPlugins.telescope-nvim ];
        };

        # The Neovim development loop, as one command.
        #
        # The split matters for iteration speed: the dispatcher is rebuilt
        # from the working tree on every launch (seconds), while the worker
        # comes prebuilt from Nix because compiling it takes minutes and it
        # is rarely what you are changing. Lua comes from the working tree
        # too, via the runtimepath, so nothing about the plugin needs a
        # rebuild at all.
        lum-nvim-dev = pkgs.writeShellApplication {
          name = "lum-nvim-dev";
          # Deliberately no neovim here. writeShellApplication puts
          # runtimeInputs first on PATH, so including it would shadow the
          # user's own Neovim — and --user-config exists precisely to run
          # theirs, with their plugins and their notification handler. The
          # isolated mode references the pinned one by store path instead.
          runtimeInputs = [ pkgs.git pkgs.go_1_26 ];
          text = ''
            root=$(git rev-parse --show-toplevel)

            # A dedicated port and data directory, so a dev session is never
            # served by an installed lum holding the default port, and never
            # pollutes a real index. Short path: the worker socket lives here
            # and Unix socket addresses are length-limited.
            export LUM_HTTP_ADDR="''${LUM_HTTP_ADDR:-127.0.0.1:7421}"
            export LUM_DATA_DIR="''${LUM_DATA_DIR:-/tmp/lum-dev}"
            export LUM_WORKER_PATH="''${LUM_WORKER_PATH:-${lum-worker}/bin/lum-worker}"

            echo "building the dispatcher from $root/dispatcher ..."
            mkdir -p "$root/bin"
            (cd "$root/dispatcher" && go build -o "$root/bin/lum" ./cmd/lum)
            export PATH="$root/bin:$PATH"

            # A dev daemon left over from the previous launch still holds the
            # port, and the CLI talks to whatever answers — so without this,
            # the build that just finished would not be the one under test.
            lum stop >/dev/null 2>&1 || true

            echo "lum:    $(command -v lum)"
            echo "worker: $LUM_WORKER_PATH"
            echo "data:   $LUM_DATA_DIR   api: $LUM_HTTP_ADDR"

            export LUM_DEV_REPO="$root"
            export LUM_DEV_TELESCOPE="${pkgs.vimPlugins.telescope-nvim}"
            export LUM_DEV_PLENARY="${pkgs.vimPlugins.plenary-nvim}"

            # --user-config runs your own Neovim — your plugins, your
            # notification handler — with the working-tree lum attached,
            # instead of the isolated config in dev/nvim.lua. Useful once you
            # want to see the integration the way you actually use it; the
            # isolated config stays the better place to debug lum itself,
            # since it has no other plugins to blame.
            if [ "''${1:-}" = "--user-config" ]; then
              shift
              if ! command -v nvim >/dev/null 2>&1; then
                echo "--user-config runs your own Neovim, but no nvim is on PATH." >&2
                echo "Run lum-nvim-dev without --user-config to use the pinned one." >&2
                exit 1
              fi
              echo "nvim:   $(command -v nvim) (your configuration)"
              # --cmd runs before init so the runtimepath is in place for
              # configs that load the extension themselves. attach.lua sets it
              # again at VimEnter, because plugin managers rewrite
              # runtimepath and would otherwise drop it.
              exec nvim --cmd "set runtimepath^=$root" \
                -c "lua dofile('$root/dev/attach.lua')" "$@"
            fi
            echo "nvim:   ${pkgs.neovim}/bin/nvim (isolated config)"
            exec ${pkgs.neovim}/bin/nvim -u "$root/dev/nvim.lua" "$@"
          '';
        };
        # Retrieval evaluation. Same split as the Neovim loop: dispatcher
        # from the working tree, worker prebuilt.
        #
        # It runs against its own data directory so a measurement never
        # depends on, or disturbs, whatever is in your real index. --fresh
        # clears it, which is what you want across a chunker or model change
        # since those invalidate every existing vector.
        lum-eval = pkgs.writeShellApplication {
          name = "lum-eval";
          runtimeInputs = [ pkgs.git pkgs.go_1_26 ];
          text = ''
            root=$(git rev-parse --show-toplevel)

            export LUM_HTTP_ADDR="''${LUM_HTTP_ADDR:-127.0.0.1:7422}"
            export LUM_DATA_DIR="''${LUM_DATA_DIR:-/tmp/lum-eval}"
            export LUM_WORKER_PATH="''${LUM_WORKER_PATH:-${lum-worker}/bin/lum-worker}"

            # Keep eval/ out of the index. questions.yaml contains the
            # questions verbatim, so indexing it made the fixture the best
            # match for its own queries — it appeared in the top five for
            # half of them, measuring the benchmark against itself. The
            # first four mirror the built-in defaults, which this replaces.
            export LUM_EXCLUDE_DIRS="node_modules,vendor,target,__pycache__,eval"

            fresh=0
            args=()
            for arg in "$@"; do
              case "$arg" in
                --fresh) fresh=1 ;;
                *) args+=("$arg") ;;
              esac
            done

            mkdir -p "$root/bin"
            (cd "$root/dispatcher" && go build -o "$root/bin/lum" ./cmd/lum)
            export PATH="$root/bin:$PATH"

            # Stop first either way: a running daemon holds the old binary,
            # and on --fresh it also holds the index files open, so deleting
            # the directory underneath it would not actually reset anything.
            lum stop >/dev/null 2>&1 || true
            if [ "$fresh" = "1" ]; then
              echo "clearing $LUM_DATA_DIR (full re-index)"
              rm -rf "''${LUM_DATA_DIR:?}"
            fi
            mkdir -p "$LUM_DATA_DIR"

            # Start the daemon here rather than letting the test trigger the
            # on-demand spawn. That path re-execs os.Executable(), which under
            # `go test` is the test binary — so it would try to run the test
            # as a daemon and funnel its output into daemon.log.
            lum serve >>"$LUM_DATA_DIR/daemon.log" 2>&1 &
            serve=$!
            # shellcheck disable=SC2317
            cleanup() { lum stop >/dev/null 2>&1 || true; wait "$serve" 2>/dev/null || true; }
            trap cleanup EXIT

            host="''${LUM_HTTP_ADDR%:*}"
            port="''${LUM_HTTP_ADDR##*:}"
            for _ in $(seq 1 100); do
              if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3>&- ; break; fi
              if ! kill -0 "$serve" 2>/dev/null; then
                echo "lum serve exited during startup; see $LUM_DATA_DIR/daemon.log" >&2
                exit 1
              fi
              sleep 0.2
            done

            cd "$root/dispatcher"
            go test -tags eval -v -count=1 -timeout 30m ./internal/eval/ "''${args[@]}"
          '';
        };
      in {
        packages = {
          inherit lum lum-worker lum-nvim lum-nvim-dev lum-eval;
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
          # `nix run .#eval` — measure retrieval against eval/questions.yaml.
          eval = flake-utils.lib.mkApp {
            drv = lum-eval;
            exePath = "/bin/lum-eval";
          };
        };

        checks = {
          go-tests = lum-unwrapped.overrideAttrs (_: { subPackages = [ "..." ]; });
          rust-tests = craneLib.cargoTest (rustArgs // { cargoArtifacts = null; });
          nvim-plugin = lum-nvim;
          packaged-version = pkgs.runCommand "lum-packaged-version-${version}" { } ''
            test "$(${lum}/bin/lum version --json)" = '${builtins.toJSON { inherit version; }}'
            touch $out
          '';
        };

        devShells = {
          default = pkgs.mkShell {
            packages = [
              pkgs.go_1_26
              rustToolchain
              pkgs.cargo
              pkgs.buf
              pkgs.curl
              pkgs.perl
              pkgs.protobuf
              pkgs.protoc-gen-go
              pkgs.protoc-gen-go-grpc
            ];
            shellHook = ''
              echo "lum dev shell. Neovim loop: nix develop .#nvim   (or nix run .#nvim)"
            '';
          };

          # Kept separate from the default shell so building lum does not pull
          # in Neovim and Telescope for people who never touch the plugin.
          #
          # The hook exports the same environment lum-nvim-dev uses, so `lum`
          # run directly in this shell and `lum` run from inside Neovim are
          # the same binary against the same index. Without that, a session
          # would have two different lums depending on where you typed it.
          # No neovim in packages: it would shadow the user's own, which
          # --user-config needs. lum-nvim-dev pins the isolated one by store
          # path.
          nvim = pkgs.mkShell {
            packages = [ pkgs.go_1_26 pkgs.git lum-nvim-dev ];
            shellHook = ''
              export LUM_HTTP_ADDR="''${LUM_HTTP_ADDR:-127.0.0.1:7421}"
              export LUM_DATA_DIR="''${LUM_DATA_DIR:-/tmp/lum-dev}"
              export LUM_WORKER_PATH="''${LUM_WORKER_PATH:-${lum-worker}/bin/lum-worker}"
              if root=$(git rev-parse --show-toplevel 2>/dev/null); then
                export PATH="$root/bin:$PATH"
              fi
              echo "lum Neovim dev shell."
              echo "  lum-nvim-dev   build the dispatcher and open Neovim with the local plugin"
              echo "  data $LUM_DATA_DIR   api $LUM_HTTP_ADDR"
            '';
          };
        };
      });
}
