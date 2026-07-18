//! lumen — lum's data plane.
//!
//! This binary is spawned and supervised by the Go control plane (`lum
//! serve`); you normally never run it by hand. It owns the compute-heavy,
//! byte-level half of the system:
//!
//! ```text
//!   IngestDocument:  bytes ─▶ Parser ─▶ Chunker ─▶ Embedder ─▶ VectorStore
//!   Search:          query ─────────────────────▶ Embedder ─▶ VectorStore
//! ```
//!
//! Each stage is a trait (see `pipeline/` and `store/`), so swapping the
//! chunking strategy, embedding model, or vector store is a matter of
//! providing another implementation — the gRPC service only knows the
//! traits.
//!
//! State: lumen keeps *no* orchestration state. Which documents exist,
//! their hashes, their chunk counts — all of that lives in the control
//! plane's SQLite catalog. lumen's only persistent data is the vector
//! index (qdrant-edge files) and the embedding model cache, both under
//! the data dir passed via `--data-dir`.

mod pipeline;
mod service;
mod store;

/// Generated protobuf/gRPC code for the `lum.v1` package.
///
/// `build.rs` compiles `proto/lum/v1/dataplane.proto` into `OUT_DIR`;
/// this macro pulls it into the crate. The Go control plane generates
/// its client from the same file — one contract, two languages.
pub mod pb {
    tonic::include_proto!("lum.v1");
}

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use crate::pb::data_plane_server::DataPlaneServer;
use crate::service::DataPlaneService;

#[derive(Parser, Debug)]
#[command(name = "lumen", about = "lum data plane (spawned by `lum serve`)")]
struct Args {
    /// Address to serve gRPC on. Localhost only — lum is local-only by
    /// design; nothing here should ever bind a public interface.
    #[arg(long, default_value = "127.0.0.1:7421")]
    grpc_addr: SocketAddr,

    /// Root data directory (the control plane passes its own, typically
    /// ~/.lum). lumen uses <data-dir>/models for the embedding model
    /// cache and <data-dir>/vectors for the qdrant-edge index.
    #[arg(long)]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lumen=info".into()),
        )
        .init();

    let args = Args::parse();

    // Initialization is deliberately synchronous and happens *before* we
    // start listening: on first run this downloads the embedding model
    // (~70 MB) and creates the vector index. The control plane treats
    // "gRPC port accepting connections" as readiness, so by the time a
    // request can arrive, everything is loaded.
    tracing::info!(data_dir = %args.data_dir.display(), "initializing pipeline");
    let service = DataPlaneService::initialize(&args.data_dir)?;
    tracing::info!(addr = %args.grpc_addr, "lumen ready, serving gRPC");

    tonic::transport::Server::builder()
        .add_service(DataPlaneServer::new(service))
        .serve_with_shutdown(args.grpc_addr, async {
            // Shut down cleanly on ctrl-c or when the control plane
            // terminates us; qdrant-edge flushes on drop.
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await?;

    Ok(())
}
