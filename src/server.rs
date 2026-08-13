//! The daemon: one Unix socket, one process.
//!
//! Requests on a connection are handled concurrently, so a client is never
//! blocked behind its own slow call. That matters for exactly one client: the
//! Neovim picker keeps a single connection open for a session and fires a
//! query per keystroke down it, and the answer to keystroke 4 must not wait on
//! keystroke 3's.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::engine::Engine;
use crate::events::Event;
use crate::wire::{Op, Reply, Request};

/// Outgoing lines per connection. A subscriber that stops reading fills this
/// and is disconnected rather than being allowed to stall the bus.
const WRITE_QUEUE: usize = 512;

struct Activity {
    last: Mutex<Instant>,
    subscribers: AtomicUsize,
}

impl Activity {
    fn touch(&self) {
        *self.last.lock().unwrap() = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last.lock().unwrap().elapsed()
    }
}

pub async fn serve(config: Config) -> Result<()> {
    config.ensure_data_dir()?;

    // Held for the whole lifetime. A lock that can be taken is proof the
    // daemon is fully gone, which is what makes on-demand spawn safe and what
    // `stop` waits on — the socket going quiet is not the same thing, because
    // the listener closes before the database does.
    let lock = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(config.lock_path())
        .context("opening the daemon lock")?;
    if lock.try_lock().is_err() {
        anyhow::bail!("another lum daemon is already running");
    }

    for stale in config.stale_paths() {
        tracing::warn!(
            path = %stale.display(),
            "left over from a previous lum version; safe to delete once you are happy with this one"
        );
    }

    let socket = config.socket_path();
    // A socket file outliving its process is normal after a hard kill, and
    // bind fails on an existing path regardless of whether anyone is behind
    // it. The daemon lock above is what actually proves nobody is.
    let _ = std::fs::remove_file(&socket);
    let listener =
        UnixListener::bind(&socket).with_context(|| format!("binding {}", socket.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    }

    let engine = Engine::new(config.clone())?;
    engine.start_background();

    let activity =
        Arc::new(Activity { last: Mutex::new(Instant::now()), subscribers: AtomicUsize::new(0) });

    tracing::info!(socket = %socket.display(), "lum daemon listening");

    let idle_timeout = config.idle_timeout;
    let shutdown = Arc::clone(&engine.shutdown);
    let idle_activity = Arc::clone(&activity);
    let idle_shutdown = Arc::clone(&shutdown);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval((idle_timeout / 4).max(Duration::from_secs(1)));
        loop {
            ticker.tick().await;
            // A connected subscriber is real activity, not just the request
            // that opened the stream: something is watching this daemon work.
            if idle_activity.subscribers.load(Ordering::Relaxed) > 0 {
                idle_activity.touch();
                continue;
            }
            if idle_activity.idle_for() >= idle_timeout {
                tracing::info!(?idle_timeout, "idle; shutting down");
                idle_shutdown.notify_one();
                return;
            }
        }
    });

    let mut sigterm = signal_stream()?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let engine = Arc::clone(&engine);
                        let activity = Arc::clone(&activity);
                        tokio::spawn(async move {
                            if let Err(error) = handle(stream, engine, activity).await {
                                tracing::debug!(%error, "connection ended");
                            }
                        });
                    }
                    Err(error) => tracing::warn!(%error, "accept failed"),
                }
            }
            _ = shutdown.notified() => break,
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
        }
    }

    tracing::info!("shutting down");
    drop(listener);
    let _ = std::fs::remove_file(&socket);
    // Drop the engine (and with it the database connections and any loaded
    // model) before releasing the lock, so a replacement daemon starting the
    // instant the lock frees never overlaps this one's files.
    drop(engine);
    let _ = lock.unlock();
    Ok(())
}

#[cfg(unix)]
fn signal_stream() -> Result<tokio::signal::unix::Signal> {
    use tokio::signal::unix::{signal, SignalKind};
    Ok(signal(SignalKind::terminate())?)
}

async fn handle(stream: UnixStream, engine: Arc<Engine>, activity: Arc<Activity>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let (out, mut outbox) = mpsc::channel::<String>(WRITE_QUEUE);

    let writer_task = tokio::spawn(async move {
        while let Some(line) = outbox.recv().await {
            if writer.write_all(line.as_bytes()).await.is_err() {
                return;
            }
            if writer.write_all(b"\n").await.is_err() {
                return;
            }
        }
    });

    let mut lines = BufReader::new(reader).lines();
    let mut subscribed = false;
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        activity.touch();

        let request: Request = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                // Recover the id from the raw line so the reply reaches
                // whoever sent it. A caller that gets no answer cannot tell a
                // rejected request from a wedged daemon.
                let id = crate::wire::id_of(&line).unwrap_or(0);
                let _ = out.send(serde_json::to_string(&Reply::error(id, error))?).await;
                continue;
            }
        };
        let id = request.id.unwrap_or(0);

        if let Op::Subscribe(subscribe) = &request.op {
            if !subscribed {
                subscribed = true;
                activity.subscribers.fetch_add(1, Ordering::Relaxed);
            }
            spawn_subscription(&engine, subscribe.clone(), out.clone());
            let _ = out
                .send(serde_json::to_string(&Reply::ok(
                    id,
                    serde_json::json!({"subscribed": true}),
                ))?)
                .await;
            continue;
        }

        // One task per request, so a search that has to wait for a first index
        // does not hold up the next line on the same connection.
        let engine = Arc::clone(&engine);
        let out = out.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let (method, result) = dispatch(&engine, request.op).await;
            let reply = match result {
                Ok(value) => Reply::ok(id, value),
                Err(error) => Reply::error(id, format!("{error:#}")),
            };
            if let Ok(line) = serde_json::to_string(&reply) {
                let _ = out.send(line).await;
            }
            let mut event = Event::new("request");
            event.method = Some(method);
            event.took_ms = Some(started.elapsed().as_millis() as u64);
            engine.bus.publish(event);
        });
    }

    if subscribed {
        activity.subscribers.fetch_sub(1, Ordering::Relaxed);
    }
    drop(out);
    let _ = writer_task.await;
    Ok(())
}

fn spawn_subscription(
    engine: &Arc<Engine>,
    request: crate::wire::SubscribeRequest,
    out: mpsc::Sender<String>,
) {
    let wanted: Option<std::collections::HashSet<String>> =
        if request.kinds.is_empty() { None } else { Some(request.kinds.iter().cloned().collect()) };
    let mut receiver = engine.bus.subscribe();
    let backlog = if request.replay { engine.bus.backlog() } else { Vec::new() };

    tokio::spawn(async move {
        let encode = |event: &Event| -> Option<String> {
            if wanted.as_ref().is_some_and(|kinds| !kinds.contains(event.event)) {
                return None;
            }
            serde_json::to_string(event).ok()
        };
        for event in backlog {
            if let Some(line) = encode(&event) {
                if out.send(line).await.is_err() {
                    return;
                }
            }
        }
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    if let Some(line) = encode(&event) {
                        if out.send(line).await.is_err() {
                            return;
                        }
                    }
                }
                // Lagged: this subscriber missed events. Dropping them is the
                // point of a bounded channel — a slow reader must never be
                // able to stall the indexer to deliver a progress update.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::debug!(missed, "subscriber lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
}

async fn dispatch(engine: &Arc<Engine>, op: Op) -> (&'static str, Result<serde_json::Value>) {
    match op {
        Op::Ping => ("ping", Ok(serde_json::json!({"pong": true}))),
        Op::Search(request) => {
            ("search", engine.search(request).await.and_then(|r| Ok(serde_json::to_value(r)?)))
        }
        Op::AddSource(request) => ("add_source", add_source(engine, request).await),
        Op::RemoveSource { source } => (
            "remove_source",
            engine.remove_source(&source).map(|uri| serde_json::json!({ "uri": uri })),
        ),
        Op::ListSources => {
            ("list_sources", engine.list_sources().and_then(|s| Ok(serde_json::to_value(s)?)))
        }
        Op::Scan { source } => (
            "scan",
            engine.resolve_source(&source).map(|source| {
                engine.request_scan(&source.id);
                serde_json::json!({"queued": true})
            }),
        ),
        Op::Status => ("status", engine.status().and_then(|s| Ok(serde_json::to_value(s)?))),
        Op::Subscribe(_) => ("subscribe", Ok(serde_json::json!({"subscribed": true}))),
        Op::Shutdown => {
            // notify_one, not notify_waiters: it stores a permit when nobody
            // is registered yet, so a shutdown arriving while the accept loop
            // is between select iterations is not silently dropped.
            engine.shutdown.notify_one();
            ("shutdown", Ok(serde_json::json!({"stopping": true})))
        }
    }
}

async fn add_source(
    engine: &Arc<Engine>,
    request: crate::wire::AddSourceRequest,
) -> Result<serde_json::Value> {
    let (source, created) = engine.ensure_source(&request.uri)?;
    if request.wait {
        engine.wait_initial(&source.id).await?;
    }
    let (documents, chunks) = engine.source_stats(&source.id)?;
    Ok(serde_json::to_value(crate::wire::AddSourceResponse {
        source: crate::wire::Source { id: source.id, uri: source.uri, documents, chunks },
        created,
    })?)
}
