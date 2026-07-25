//! lum-worker — lum's indexing and retrieval worker.
//!
//! This binary is spawned and supervised by the dispatcher (`lum
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
//! State: lum-worker keeps *no* orchestration state. Which documents exist,
//! their hashes, their chunk counts — all of that lives in the control
//! plane's SQLite catalog. lum-worker's only persistent data is the vector
//! index (qdrant-edge files) and the embedding model cache, both under
//! the data dir passed via `--data-dir`.

mod pipeline;
mod service;
mod store;

/// Generated protobuf/gRPC code for the `lum.v1` package.
///
/// `build.rs` compiles `proto/lum/v1/worker.proto` into `OUT_DIR`;
/// this macro pulls it into the crate. The dispatcher generates
/// its client from the same file — one contract, two languages.
pub mod pb {
    tonic::include_proto!("lum.v1");
}

use std::path::PathBuf;

use clap::Parser;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;

use crate::pb::worker_server::WorkerServer;
use crate::pipeline::EmbeddingModelChoice;
use crate::service::WorkerService;

#[derive(Parser, Debug)]
#[command(name = "lum-worker", about = "lum worker (spawned by `lum serve`)")]
struct Args {
    /// Unix socket used for the private gRPC connection from lum.
    #[arg(long)]
    grpc_socket: PathBuf,

    /// Root data directory (the dispatcher passes its own, typically
    /// ~/.lum). lum-worker uses <data-dir>/models for the embedding model
    /// cache and <data-dir>/vectors for the qdrant-edge index.
    #[arg(long)]
    data_dir: PathBuf,

    /// Embedding model variant. Quantized is faster on CPU with a small
    /// retrieval-quality tradeoff and requires a separate vector index.
    #[arg(long, value_enum, default_value_t)]
    embedding_model: EmbeddingModelChoice,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lum-worker=info".into()),
        )
        .init();

    let args = Args::parse();

    // Expose lifecycle health immediately while initialization proceeds on
    // the blocking pool. All other RPCs remain unavailable until ready.
    tracing::info!(
        data_dir = %args.data_dir.display(),
        embedding_model = ?args.embedding_model,
        "initializing pipeline"
    );
    // Bind before scheduling any blocking initialization so Health is
    // reachable even when a first-run model download takes minutes.
    let (listener, _socket_guard) = bind_socket(&args.grpc_socket).await?;
    let incoming = UnixListenerStream::new(listener);
    let service = WorkerService::starting();
    let initializer = service.clone();
    let data_dir = args.data_dir.clone();
    let embedding_model = args.embedding_model;
    // fastembed does not provide cooperative cancellation. A detached OS
    // thread lets Tokio and the server shut down promptly during a blocked
    // download; process exit then terminates the initializer.
    std::thread::spawn(move || {
        initializer.initialize(data_dir, embedding_model);
    });
    tracing::info!(socket = %args.grpc_socket.display(), "lum-worker serving gRPC");

    tonic::transport::Server::builder()
        .add_service(WorkerServer::new(service))
        .serve_with_incoming_shutdown(incoming, async {
            // The stdin pipe closes if the dispatcher is SIGKILLed, while
            // ctrl-c covers its normal supervised shutdown path.
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = wait_for_parent_exit() => {}
            }
            tracing::info!("shutdown signal received");
        })
        .await?;

    Ok(())
}

struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;

        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.dev() == self.device && metadata.ino() == self.inode {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

async fn bind_socket(path: &std::path::Path) -> anyhow::Result<(UnixListener, SocketGuard)> {
    loop {
        match UnixListener::bind(path) {
            Ok(listener) => return secure_socket(path, listener),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                match UnixStream::connect(path).await {
                    Ok(_) => {
                        anyhow::bail!("worker socket {} is already in use", path.display())
                    }
                    Err(connect_error)
                        if connect_error.kind() == std::io::ErrorKind::ConnectionRefused =>
                    {
                        use std::os::unix::fs::FileTypeExt;

                        if !std::fs::symlink_metadata(path)?.file_type().is_socket() {
                            return Err(error.into());
                        }
                        let stale_path = path.with_extension(format!(
                            "stale-{}-{}",
                            std::process::id(),
                            uuid::Uuid::new_v4()
                        ));
                        match std::fs::rename(path, &stale_path) {
                            Ok(()) => {
                                let result = UnixListener::bind(path)
                                    .map_err(anyhow::Error::from)
                                    .and_then(|listener| secure_socket(path, listener));
                                let _ = std::fs::remove_file(stale_path);
                                return result;
                            }
                            Err(rename_error)
                                if rename_error.kind() == std::io::ErrorKind::NotFound =>
                            {
                                continue;
                            }
                            Err(rename_error) => return Err(rename_error.into()),
                        }
                    }
                    Err(connect_error) => return Err(connect_error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn secure_socket(
    path: &std::path::Path,
    listener: UnixListener,
) -> anyhow::Result<(UnixListener, SocketGuard)> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok((
        listener,
        SocketGuard {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        },
    ))
}

async fn wait_for_parent_exit() {
    let mut input = Vec::new();
    let _ = tokio::io::stdin().read_to_end(&mut input).await;
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn socket_path() -> PathBuf {
        PathBuf::from("/tmp").join(format!("lum-worker-{}.sock", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn socket_is_private_and_replaces_only_stale_socket() {
        let path = socket_path();
        let (listener, guard) = bind_socket(&path).await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        assert!(
            bind_socket(&path).await.is_err(),
            "active socket was replaced"
        );
        drop(listener);
        std::mem::forget(guard); // Simulate SIGKILL leaving the socket path.

        let (replacement, _replacement_guard) = bind_socket(&path).await.unwrap();
        drop(replacement);
    }

    #[tokio::test]
    async fn existing_regular_file_is_not_removed() {
        let path = socket_path();
        std::fs::write(&path, "keep").unwrap();

        assert!(bind_socket(&path).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep");
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn concurrent_stale_recovery_has_one_winner() {
        let path = socket_path();
        let (listener, guard) = bind_socket(&path).await.unwrap();
        drop(listener);
        std::mem::forget(guard);

        let (first, second) = tokio::join!(bind_socket(&path), bind_socket(&path));
        let (winner, loser) = if first.is_ok() {
            (first.unwrap(), second)
        } else {
            (second.unwrap(), first)
        };
        assert!(loser.is_err());
        UnixStream::connect(&path).await.unwrap();
        drop(winner);
    }
}
