//! The command line. Every subcommand except `serve` is a socket client.

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use serde_json::json;

use crate::client::{self, Client, NotRunning};
use crate::config::Config;
use crate::render::{self, Progress};
use crate::wire::{AddSourceResponse, SearchResponse, Source, Status};

#[derive(Parser)]
#[command(
    name = "lum",
    version,
    about = "Local semantic code search",
    long_about = "Local semantic code search. Point it at a repository, search by meaning \
                  instead of by pattern, and jump to the matching line range. Your code, the \
                  embeddings, and the index never leave the machine."
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon in the foreground (normally started on demand)
    Serve,

    /// Semantic search across everything lum has indexed
    Search {
        /// The query. Everything after `--` is taken literally.
        #[arg(required = true, num_args = 1..)]
        query: Vec<String>,
        #[arg(long, default_value_t = 10, help = "maximum results to return")]
        limit: usize,
        #[arg(long, help = "restrict results to one source id (see `lum sources`)")]
        source: Option<String>,
        #[arg(long, help = "ensure and search only this workspace")]
        root: Option<String>,
        #[arg(long, help = "emit a compact JSON search envelope")]
        json: bool,
        #[arg(long, help = "emit one compact JSON result per line")]
        jsonl: bool,
        #[arg(
            long,
            default_value_t = 2,
            help = "chunks any one file may contribute; 0 returns raw nearest neighbours"
        )]
        per_file: usize,
        #[arg(long, help = "omit test files from results")]
        no_tests: bool,
        #[arg(short, long, help = "do not report indexing progress on stderr")]
        quiet: bool,
    },

    /// Register a directory and start indexing it
    Add {
        path: String,
        #[arg(long, help = "block until the first index finishes")]
        wait: bool,
        #[arg(short, long)]
        quiet: bool,
    },

    /// Unregister a source and delete everything indexed from it
    #[command(alias = "rm")]
    Remove {
        /// A source id, or the path you added
        target: String,
        #[arg(short, long)]
        quiet: bool,
    },

    /// List registered sources
    Sources,

    /// Rescan a source now
    Scan { target: String },

    /// Show daemon health and index statistics
    Status,

    /// Stop the running daemon, if any
    Stop,

    /// Live view of what lum is doing
    Top,

    /// Speak the Model Context Protocol on stdin/stdout
    Mcp,

    /// Discard every embedding and index again from scratch
    ///
    /// Needed after changing the embedding model: vectors from two models are
    /// not comparable, so they cannot share an index.
    Reindex {
        #[arg(long, help = "do not ask for confirmation")]
        force: bool,
    },
}

pub async fn run(cli: Cli) -> Result<()> {
    let config = Config::load()?;
    match cli.command {
        Command::Serve => crate::server::serve(config).await,
        Command::Search {
            query,
            limit,
            source,
            root,
            json: json_output,
            jsonl,
            per_file,
            no_tests,
            quiet,
        } => {
            if json_output && jsonl {
                bail!("--json and --jsonl are mutually exclusive");
            }
            if root.is_some() && source.is_some() {
                bail!("--root and --source cannot be combined");
            }
            search(
                &config,
                query.join(" "),
                limit,
                source,
                root,
                json_output,
                jsonl,
                per_file,
                no_tests,
                quiet,
            )
            .await
        }
        Command::Add { path, wait, quiet } => add(&config, path, wait, quiet).await,
        Command::Remove { target, quiet } => remove(&config, target, quiet).await,
        Command::Sources => sources(&config).await,
        Command::Scan { target } => scan(&config, target).await,
        Command::Status => status(&config).await,
        Command::Stop => stop(&config).await,
        Command::Top => crate::top::run(&config).await,
        Command::Mcp => crate::mcp::run(&config).await,
        Command::Reindex { force } => reindex(&config, force).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn search(
    config: &Config,
    query: String,
    limit: usize,
    source: Option<String>,
    root: Option<String>,
    json_output: bool,
    jsonl: bool,
    per_file: usize,
    no_tests: bool,
    quiet: bool,
) -> Result<()> {
    let client = Client::connect(config).await?;

    // The wait worth reporting: `--root` blocks until that workspace's first
    // index finishes, which on a cold start is a model download plus a full
    // embed. The picker deliberately does not wait — see wire::SearchRequest.
    let progress = if root.is_some() { Some(Progress::start(&client, quiet).await) } else { None };

    let response: SearchResponse = client
        .call(json!({
            "op": "search",
            "q": query,
            "limit": limit,
            "source": source,
            "root": root,
            "per_file": per_file,
            "exclude_tests": no_tests,
            "wait": true,
        }))
        .await?;

    if let Some(progress) = progress {
        progress.finish().await;
    }

    if json_output {
        println!("{}", serde_json::to_string(&response)?);
    } else if jsonl {
        for result in &response.results {
            println!("{}", serde_json::to_string(result)?);
        }
    } else {
        render::print_human(&response.results);
    }
    Ok(())
}

async fn add(config: &Config, path: String, wait: bool, quiet: bool) -> Result<()> {
    let client = Client::connect(config).await?;
    let progress = Progress::start(&client, quiet).await;
    let response: AddSourceResponse =
        client.call(json!({"op": "add_source", "uri": path, "wait": wait})).await?;
    progress.finish().await;

    if response.created {
        println!("added {}", response.source.uri);
    } else {
        println!("source {} already registered", response.source.uri);
    }
    if wait {
        println!(
            "indexed {} into {}",
            render::plural(response.source.documents as u64, "document"),
            render::plural(response.source.chunks as u64, "chunk")
        );
    } else {
        println!("indexing in the background — check progress with `lum status`");
    }
    Ok(())
}

async fn remove(config: &Config, target: String, quiet: bool) -> Result<()> {
    let client = Client::connect(config).await?;
    let progress = Progress::start(&client, quiet).await;
    let response: serde_json::Value =
        client.call(json!({"op": "remove_source", "source": target})).await?;
    progress.finish().await;
    let uri = response.get("uri").and_then(|v| v.as_str()).unwrap_or(&target);
    println!("removed {uri} and everything indexed from it");
    Ok(())
}

async fn sources(config: &Config) -> Result<()> {
    let client = Client::connect(config).await?;
    let sources: Vec<Source> = client.call(json!({"op": "list_sources"})).await?;
    if sources.is_empty() {
        println!("no sources yet — add one with `lum add <directory>`");
        return Ok(());
    }
    for source in sources {
        println!(
            "{}  {:>6} docs  {:>7} chunks  {}",
            source.id, source.documents, source.chunks, source.uri
        );
    }
    Ok(())
}

async fn scan(config: &Config, target: String) -> Result<()> {
    let client = Client::connect(config).await?;
    let _: serde_json::Value = client.call(json!({"op": "scan", "source": target})).await?;
    println!("scan queued — check progress with `lum status`");
    Ok(())
}

async fn status(config: &Config) -> Result<()> {
    let client = Client::connect(config).await?;
    let status: Status = client.call(json!({"op": "status"})).await?;
    render::print_status(&status);
    Ok(())
}

async fn stop(config: &Config) -> Result<()> {
    // Never auto-spawns: there being nothing to stop is success, and starting
    // a daemon in order to tell it to stop would be absurd.
    let client = match Client::connect_existing(config).await {
        Ok(client) => client,
        Err(error) if error.downcast_ref::<NotRunning>().is_some() => {
            println!("no lum daemon is running");
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let _: serde_json::Value = client.call(json!({"op": "shutdown"})).await?;
    // Wait for the lock, not for the socket: the listener closes before the
    // database and the model do, so a quiet socket does not mean it is gone.
    client::wait_until_stopped(config, std::time::Duration::from_secs(30)).await?;
    println!("daemon stopped");
    Ok(())
}

async fn reindex(config: &Config, force: bool) -> Result<()> {
    if !force {
        eprintln!(
            "This discards every embedding and indexes again from scratch, which costs \
             about as long as the first index did.\nRe-run with --force to proceed."
        );
        return Ok(());
    }
    // Done with the daemon stopped, against the file directly: the daemon may
    // be refusing to start precisely because of the model mismatch this fixes.
    if let Ok(client) = Client::connect_existing(config).await {
        let _: serde_json::Value = client.call(json!({"op": "shutdown"})).await?;
        client::wait_until_stopped(config, std::time::Duration::from_secs(30)).await?;
    }
    let kept = crate::db::reset_index(&config.db_path(), config.model)?;
    println!("cleared the index; {} kept", render::plural(kept as u64, "source"));

    let client = Client::connect(config).await?;
    let progress = Progress::start(&client, false).await;
    let sources: Vec<Source> = client.call(json!({"op": "list_sources"})).await?;
    for source in &sources {
        let _: AddSourceResponse =
            client.call(json!({"op": "add_source", "uri": source.uri, "wait": true})).await?;
    }
    progress.finish().await;
    let status: Status = client.call(json!({"op": "status"})).await?;
    println!(
        "indexed {} into {}",
        render::plural(status.documents as u64, "document"),
        render::plural(status.chunks as u64, "chunk")
    );
    Ok(())
}
