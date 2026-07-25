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

        # The Rust build script reads ../proto, so retain both the Cargo
        # project and the shared protocol contract in Crane's source tree.
        rustSrc = lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            let rel = lib.removePrefix "${toString ./.}/" (toString path);
            in rel == "data-plane" || lib.hasPrefix "data-plane/" rel
              || rel == "proto" || lib.hasPrefix "proto/" rel;
        };
        rustArgs = {
          pname = "lumen";
          inherit version;
          src = rustSrc;
          cargoLock = ./data-plane/Cargo.lock;
          # Crane installs the lockfile's dependency set through its vendored
          # fixed-output derivation. It stages that lock at the source root, so
          # Cargo cannot also use --locked with this nested manifest path.
          cargoExtraArgs = "--manifest-path data-plane/Cargo.toml";
          # Keep build and install paths stable even though Cargo is driven
          # through a nested manifest.
          CARGO_TARGET_DIR = "target";
          strictDeps = true;
          buildInputs = [ pkgs.onnxruntime ];
          ORT_LIB_LOCATION = "${lib.getLib pkgs.onnxruntime}/lib";
          ORT_PREFER_DYNAMIC_LINK = "1";
        };
        lumen = craneLib.buildPackage (rustArgs // {
          # Dummification does not understand this nested manifest and would
          # compile lumen itself in the dependency derivation. A single build
          # is both faster and substantially smaller on disk.
          cargoArtifacts = null;
          doCheck = false;
          # Crane's default installer asks Cargo for root-manifest metadata;
          # this repository deliberately keeps the manifest in data-plane.
          doNotPostBuildInstallCargoBinaries = true;
          installPhaseCommand = ''
            mkdir -p $out/bin
            cp target/release/lumen $out/bin/lumen
          '';
        });

        lum-unwrapped = pkgs.buildGoModule {
          pname = "lum";
          inherit version;
          src = ./control-plane;
          vendorHash = "sha256-e02pNlwlTXjnK9Kg9TKI73nUqDuHD75ZlaU39jaaT9k=";
          subPackages = [ "cmd/lum" ];
          ldflags = [
            "-X github.com/alDuncanson/lum/control-plane/internal/version.Value=${version}"
          ];
        };

        # Lum is one product even though its implementation uses a private
        # Rust worker. Pin that worker by store path instead of relying on
        # symlink resolution or the caller's PATH.
        lum = pkgs.runCommand "lum-${version}" {
          nativeBuildInputs = [ pkgs.makeWrapper ];
        } ''
          mkdir -p $out/bin
          makeWrapper ${lum-unwrapped}/bin/lum $out/bin/lum \
            --set-default LUM_LUMEN_PATH ${lumen}/bin/lumen
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
      in {
        packages = {
          inherit lum lum-nvim;
          default = lum;
        };

        apps = let
          lumApp = flake-utils.lib.mkApp { drv = lum; exePath = "/bin/lum"; };
        in {
          lum = lumApp;
          default = lumApp;
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

        devShells.default = pkgs.mkShell {
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
        };
      });
}
