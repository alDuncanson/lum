//! lum — local semantic code search, in one process.

mod chunk;
mod cli;
mod client;
mod config;
mod db;
mod embed;
mod engine;
mod events;
mod index;
mod language;
mod mcp;
mod mime;
mod model;
mod parse;
mod render;
mod scan;
mod server;
mod sys;
mod top;
mod watch;
mod wire;

use anyhow::Result;
use clap::Parser;

fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    // Logs go to stderr, which for the daemon is `daemon.log` and for `mcp` is
    // the only stream that is not the protocol. Quiet by default: a search is
    // not an occasion for output nobody asked for.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LUM_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("lum=info,warn")),
        )
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();

    // A small, fixed pool. The daemon's concurrency is a handful of tasks and
    // one ingest thread; the actual parallelism lives inside ONNX Runtime,
    // which has its own pool and does not benefit from competing with a
    // work-stealing scheduler sized to the machine.
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()?;

    runtime.block_on(async {
        if let Err(error) = cli::run(cli).await {
            eprintln!("lum: {error:#}");
            std::process::exit(1);
        }
        Ok(())
    })
}
