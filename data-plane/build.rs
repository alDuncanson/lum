//! Build script: generate Rust gRPC code from the shared proto contract.
//!
//! We use `protox` (a pure-Rust protobuf compiler) instead of shelling out
//! to `protoc`, so `cargo build` is the only step needed. The generated
//! code lands in `OUT_DIR` and is pulled into the crate by
//! `tonic::include_proto!("lum.v1")` (see src/main.rs).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Rebuild if the contract changes.
    println!("cargo:rerun-if-changed=../proto/lum/v1/dataplane.proto");

    let file_descriptors = protox::compile(["lum/v1/dataplane.proto"], ["../proto"])?;

    tonic_prost_build::configure()
        // lumen is a server only; the Go control plane is the client.
        .build_client(false)
        .compile_fds(file_descriptors)?;

    Ok(())
}
