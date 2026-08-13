//! Talking to the daemon, and starting it if it is not there.
//!
//! One connection multiplexes everything: replies are matched to requests by
//! `id`, and events arrive on the same socket between them. That is what lets
//! a command subscribe to progress *and* make the request it is reporting
//! progress for without opening a second connection — and, more importantly,
//! what lets the Neovim picker hold one connection for a whole session.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::config::Config;

/// Nothing is listening. Distinguished from every other failure because for
/// `stop` it means success — there being nothing to stop is not an error, and
/// spawning a daemon in order to tell it to stop would be absurd.
#[derive(Debug)]
pub struct NotRunning;

impl std::fmt::Display for NotRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no lum daemon is running")
    }
}

impl std::error::Error for NotRunning {}

/// Requests awaiting a reply, keyed by the id they were sent with. The reader
/// task resolves them; a dropped connection fails all of them at once.
type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>;

pub struct Client {
    outbox: mpsc::Sender<String>,
    pending: Pending,
    events: broadcast::Sender<Value>,
    next_id: AtomicU64,
}

impl Client {
    /// Connect, starting the daemon if nothing is listening.
    pub async fn connect(config: &Config) -> Result<Self> {
        match UnixStream::connect(config.socket_path()).await {
            Ok(stream) => Ok(Self::wrap(stream)),
            Err(error) if is_unavailable(&error) => {
                ensure_daemon(config).await?;
                let stream = UnixStream::connect(config.socket_path())
                    .await
                    .context("connecting to the daemon we just started")?;
                Ok(Self::wrap(stream))
            }
            Err(error) => Err(error).context("connecting to the lum daemon"),
        }
    }

    /// Connect only if a daemon is already running.
    pub async fn connect_existing(config: &Config) -> Result<Self> {
        match UnixStream::connect(config.socket_path()).await {
            Ok(stream) => Ok(Self::wrap(stream)),
            Err(error) if is_unavailable(&error) => Err(NotRunning.into()),
            Err(error) => Err(error).context("connecting to the lum daemon"),
        }
    }

    fn wrap(stream: UnixStream) -> Self {
        let (reader, mut writer) = stream.into_split();
        let (outbox, mut queue) = mpsc::channel::<String>(64);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (events, _) = broadcast::channel(1024);

        tokio::spawn(async move {
            while let Some(line) = queue.recv().await {
                if writer.write_all(line.as_bytes()).await.is_err() {
                    return;
                }
                if writer.write_all(b"\n").await.is_err() {
                    return;
                }
            }
        });

        let inbox = Arc::clone(&pending);
        let broadcaster = events.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                match message.get("id").and_then(Value::as_u64) {
                    Some(id) => {
                        if let Some(waiter) = inbox.lock().unwrap().remove(&id) {
                            let result = match message.get("error").and_then(Value::as_str) {
                                Some(error) => Err(error.to_owned()),
                                None => Ok(message.get("ok").cloned().unwrap_or(Value::Null)),
                            };
                            let _ = waiter.send(result);
                        }
                    }
                    // No id: an event. No subscribers is normal.
                    None => {
                        let _ = broadcaster.send(message);
                    }
                }
            }
            // The daemon went away. Fail every waiter rather than leaving them
            // to time out one by one with no explanation.
            for (_, waiter) in inbox.lock().unwrap().drain() {
                let _ = waiter.send(Err("the lum daemon closed the connection".to_owned()));
            }
        });

        Self { outbox, pending, events, next_id: AtomicU64::new(1) }
    }

    pub async fn call<T: DeserializeOwned>(&self, mut request: Value) -> Result<T> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        request["id"] = Value::from(id);

        self.outbox
            .send(serde_json::to_string(&request)?)
            .await
            .map_err(|_| anyhow!("the lum daemon closed the connection"))?;

        match rx.await {
            Ok(Ok(value)) => Ok(serde_json::from_value(value)?),
            Ok(Err(error)) => bail!(error),
            Err(_) => bail!("the lum daemon closed the connection"),
        }
    }

    /// Subscribe to events. Returns a receiver of raw event objects; callers
    /// read the fields they care about, which keeps the client from having to
    /// know every event shape.
    pub async fn subscribe(
        &self,
        kinds: &[&str],
        replay: bool,
    ) -> Result<broadcast::Receiver<Value>> {
        let receiver = self.events.subscribe();
        let _: Value = self
            .call(serde_json::json!({
                "op": "subscribe",
                "kinds": kinds,
                "replay": replay,
            }))
            .await?;
        Ok(receiver)
    }
}

fn is_unavailable(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::AddrNotAvailable
    )
}

/// Start a daemon, unless someone else is already doing it.
///
/// Concurrent commands converge on one daemon by taking an exclusive lock,
/// rechecking, and only then spawning. Without that, opening two shells and
/// searching in both races two daemons onto the same database.
async fn ensure_daemon(config: &Config) -> Result<()> {
    config.ensure_data_dir()?;
    let lock_path = config.start_lock_path();
    let lock = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("opening {}", lock_path.display()))?;

    let deadline = Instant::now() + config.startup_timeout;
    while lock.try_lock().is_err() {
        if Instant::now() >= deadline {
            bail!("timed out waiting for another lum command to start the daemon");
        }
        // Someone else is starting it; it may already be up.
        if UnixStream::connect(config.socket_path()).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let result = spawn_and_wait(config).await;
    let _ = lock.unlock();
    result
}

async fn spawn_and_wait(config: &Config) -> Result<()> {
    // Recheck under the lock: whoever held it before us may have finished the
    // job while we waited.
    if UnixStream::connect(config.socket_path()).await.is_ok() {
        return Ok(());
    }

    spawn_daemon(config)?;

    // `serve` takes the daemon lock before it does anything else, so after a
    // short grace period a free lock means the process is gone. Watching for
    // that turns "it failed to start" into a definite answer in milliseconds
    // instead of waiting out the full startup timeout to report a deadline.
    let grace = Instant::now() + Duration::from_millis(500);
    let deadline = Instant::now() + config.startup_timeout;
    loop {
        if UnixStream::connect(config.socket_path()).await.is_ok() {
            return Ok(());
        }
        if Instant::now() > grace && daemon_lock_is_free(&config.lock_path()) {
            bail!("the lum daemon exited during startup; see {}", config.log_path().display());
        }
        if Instant::now() >= deadline {
            bail!(
                "the lum daemon did not start within {:?}; see {}",
                config.startup_timeout,
                config.log_path().display()
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn spawn_daemon(config: &Config) -> Result<()> {
    let executable = std::env::current_exe().context("locating the lum executable")?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(config.log_path())
        .context("opening the daemon log")?;

    let mut command = std::process::Command::new(executable);
    command
        .arg("serve")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(log.try_clone()?))
        .stderr(std::process::Stdio::from(log));
    #[cfg(unix)]
    {
        // Its own process group, so closing the terminal that happened to
        // start it does not take the daemon with it.
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command.spawn().context("starting the lum daemon")?;
    Ok(())
}

/// Whether the daemon lock can be taken, which is the authoritative "it is
/// fully gone" signal.
///
/// Deliberately not "the socket stopped answering": the listener closes before
/// the database and the model do, so the socket can go quiet while the process
/// is still mid-cleanup.
pub fn daemon_lock_is_free(path: &Path) -> bool {
    let Ok(file) = std::fs::OpenOptions::new().write(true).create(true).truncate(false).open(path)
    else {
        return false;
    };
    match file.try_lock() {
        Ok(()) => {
            let _ = file.unlock();
            true
        }
        Err(_) => false,
    }
}

/// Wait for a stopping daemon to actually be gone.
pub async fn wait_until_stopped(config: &Config, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !daemon_lock_is_free(&config.lock_path()) {
        if Instant::now() >= deadline {
            bail!("the daemon did not shut down within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}
