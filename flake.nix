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
          version = "0.1.0";
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

        lum = pkgs.buildGoModule {
          pname = "lum";
          version = "0.1.0";
          src = ./control-plane;
          vendorHash = "sha256-e02pNlwlTXjnK9Kg9TKI73nUqDuHD75ZlaU39jaaT9k=";
          subPackages = [ "cmd/lum" ];
        };

        lum-full = pkgs.symlinkJoin {
          name = "lum-full-0.1.0";
          paths = [ lum lumen ];
        };
      in {
        packages = {
          inherit lum lumen lum-full;
          default = lum-full;
        };

        apps = {
          lum = flake-utils.lib.mkApp { drv = lum; };
          lumen = flake-utils.lib.mkApp { drv = lumen; };
          default = flake-utils.lib.mkApp { drv = lum-full; exePath = "/bin/lum"; };
        };

        checks = {
          go-tests = lum.overrideAttrs (_: { subPackages = [ "..." ]; });
          rust-tests = craneLib.cargoTest (rustArgs // { cargoArtifacts = null; });
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
