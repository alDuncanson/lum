//! Every tunable, resolved once from the environment.
//!
//! lum is local-only and single-process, so there is no address to bind, no
//! port to collide, and no second executable to locate. What remains is a
//! data directory, a socket inside it, and the handful of knobs that decide
//! how much memory inference is allowed to want.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Result};

/// Longest path a Unix socket address can carry. `sun_path` is a fixed array
/// in `sockaddr_un` — 104 bytes on Darwin, 108 on Linux — and the path must
/// be NUL-terminated inside it, so one byte is reserved. Taking the smaller
/// of the two is right for a check that only ever needs to be conservative.
#[cfg(target_os = "macos")]
const MAX_SOCKET_PATH: usize = 103;
#[cfg(not(target_os = "macos"))]
const MAX_SOCKET_PATH: usize = 107;

/// How long the daemon stays up after the last request.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Bounds a client's wait for a daemon that is starting, including a first
/// run's model download.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Ceiling on rows per inference call.
///
/// A cap rather than a target: `DEFAULT_TOKEN_BUDGET` is what normally decides
/// the batch, and this only binds for very short chunks, where 16 rows of 30
/// tokens is already a well-shaped call and more would add latency to the
/// progress reporting without adding throughput.
pub const DEFAULT_EMBED_BATCH: usize = 16;

/// Padded tokens per inference call — the knob that sets peak memory.
///
/// Measured on this repository: 8192 peaks at 1229 MB and takes 70 s, 4096 at
/// 1096 MB and 64 s, 2048 at 779 MB and 59 s, 1024 at 748 MB and 51 s. Smaller
/// is both leaner and faster, because attention is quadratic in the padded
/// width and a wide batch spends most of it on padding. Below 1024 the curve
/// flattens, and a full-length chunk would stop sharing a call with anything.
///
/// The row count and the thread count are *not* memory knobs; see the note in
/// `embed`, which records what sweeping them actually did.
pub const DEFAULT_TOKEN_BUDGET: usize = 1024;

/// Directories skipped by name wherever they appear, independent of
/// `.gitignore`.
///
/// Honoring `.gitignore` covers well-kept repositories but is not a safety
/// net: a repository that forgets to ignore its dependency tree makes lum
/// walk, read, and embed thousands of files nobody wants to search, and
/// `node_modules` in particular is mostly indexable extensions.
///
/// Deliberately short, and limited to names that are generated or vendored by
/// universal convention. Riskier candidates (build, dist, out, bin) are left
/// off: a repository can plausibly keep real sources there, and silently
/// skipping sources is worse than indexing junk. `LUM_EXCLUDE_DIRS` replaces
/// this list for anyone who disagrees.
const DEFAULT_EXCLUDE_DIRS: [&str; 4] = ["node_modules", "vendor", "target", "__pycache__"];

/// Which bge-small variant to embed with. The quantized model has the same
/// dimensions but produces different vectors, so each carries a distinct
/// identity in the index manifest and switching requires a re-index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Model {
    Standard,
    Quantized,
}

impl Model {
    /// Identity recorded in the database. Changing embedder output without
    /// changing this string would silently mix incomparable vectors.
    pub fn name(self) -> &'static str {
        match self {
            Self::Standard => "BAAI/bge-small-en-v1.5",
            Self::Quantized => "Qdrant/bge-small-en-v1.5-onnx-Q",
        }
    }

    /// HuggingFace repository and the ONNX file within it. Both variants are
    /// published by Xenova in the layout `hf-hub` caches, so an existing
    /// `~/.lum/models` download is reused instead of repeated.
    pub fn repo(self) -> (&'static str, &'static str) {
        match self {
            Self::Standard => ("Xenova/bge-small-en-v1.5", "onnx/model.onnx"),
            Self::Quantized => ("Xenova/bge-small-en-v1.5", "onnx/model_quantized.onnx"),
        }
    }

    pub fn dimension(self) -> usize {
        384
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "standard" => Ok(Self::Standard),
            "quantized" => Ok(Self::Quantized),
            other => bail!("invalid embedding model {other:?}: must be standard or quantized"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Root for all persistent state: `lum.db` and `models/`. Deleting this
    /// directory resets lum completely.
    pub data_dir: PathBuf,
    pub idle_timeout: Duration,
    pub startup_timeout: Duration,
    pub embed_batch: usize,
    /// Padded tokens per inference call — the knob that actually sets peak
    /// memory. See `embed::TOKEN_BUDGET`.
    pub embed_token_budget: usize,
    /// Threads ONNX Runtime may use inside one inference call.
    ///
    /// Purely a speed and politeness knob, despite the folklore: measured,
    /// dropping from 8 threads to 1 changed peak memory by under 3% and made
    /// indexing 2.5× slower. `None` gives indexing half the machine's cores
    /// and queries two, on the grounds that indexing is background work
    /// competing with whatever the user is actually doing.
    pub embed_threads: Option<usize>,
    pub exclude_dirs: HashSet<String>,
    pub model: Model,
}

impl Config {
    pub fn load() -> Result<Self> {
        let data_dir = match std::env::var_os("LUM_DATA_DIR") {
            Some(dir) => PathBuf::from(dir),
            // No home directory (rare; containers) still functions rather
            // than failing at startup.
            None => dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".lum"),
        };
        let config = Self {
            data_dir,
            idle_timeout: env_duration("LUM_IDLE_TIMEOUT")?.unwrap_or(DEFAULT_IDLE_TIMEOUT),
            startup_timeout: env_duration("LUM_STARTUP_TIMEOUT")?
                .unwrap_or(DEFAULT_STARTUP_TIMEOUT),
            embed_batch: env_usize("LUM_EMBED_BATCH_SIZE")?.unwrap_or(DEFAULT_EMBED_BATCH),
            embed_token_budget: env_usize("LUM_EMBED_TOKEN_BUDGET")?
                .unwrap_or(DEFAULT_TOKEN_BUDGET),
            embed_threads: env_usize("LUM_EMBED_THREADS")?,
            exclude_dirs: exclude_dirs(),
            model: match std::env::var("LUM_EMBEDDING_MODEL") {
                Ok(raw) => Model::parse(&raw)?,
                Err(_) => Model::Standard,
            },
        };
        config.validate()?;
        Ok(config)
    }

    /// Rejects configuration that cannot possibly work, before anything is
    /// started. Both entry points call it: `serve` so it fails with one clear
    /// line, and the on-demand spawn so a command reports the problem
    /// immediately rather than starting a daemon that dies and then waiting
    /// out the startup timeout.
    fn validate(&self) -> Result<()> {
        let socket = self.socket_path();
        let socket = std::path::absolute(&socket).unwrap_or(socket);
        if socket.as_os_str().len() > MAX_SOCKET_PATH {
            bail!(
                "data directory path is too long: the socket {} needs {} bytes, but a Unix \
                 domain socket address holds at most {MAX_SOCKET_PATH} on this platform; \
                 point LUM_DATA_DIR at a shorter path",
                socket.display(),
                socket.as_os_str().len(),
            );
        }
        if self.embed_batch == 0 {
            bail!("LUM_EMBED_BATCH_SIZE must be at least 1");
        }
        if self.embed_token_budget < 512 {
            bail!("LUM_EMBED_TOKEN_BUDGET must be at least 512, one full-length chunk");
        }
        Ok(())
    }

    /// The one database: sources, documents, chunks, and vectors.
    pub fn db_path(&self) -> PathBuf {
        self.data_dir.join("lum.db")
    }

    /// The one socket. Inside the 0700 data directory rather than on a port,
    /// so access control is the directory's and no other local user can reach
    /// it.
    pub fn socket_path(&self) -> PathBuf {
        self.data_dir.join("lum.sock")
    }

    /// Held by the daemon for its entire lifetime. A lock that can be taken
    /// is proof the daemon is fully gone — which is what makes replacing it
    /// safe, and what `stop` waits on.
    pub fn lock_path(&self) -> PathBuf {
        self.data_dir.join("daemon.lock")
    }

    /// Taken while deciding whether to spawn, so concurrent commands
    /// converge on one daemon instead of racing to start several.
    pub fn start_lock_path(&self) -> PathBuf {
        self.data_dir.join("daemon-start.lock")
    }

    pub fn log_path(&self) -> PathBuf {
        self.data_dir.join("daemon.log")
    }

    pub fn models_dir(&self) -> PathBuf {
        self.data_dir.join("models")
    }

    /// Leftovers from lum 0.1's data layout. Reported once at startup rather
    /// than deleted: they are the user's data, and an index is expensive
    /// enough to rebuild that removing it silently would be rude.
    pub fn stale_paths(&self) -> Vec<PathBuf> {
        [
            self.data_dir.join("catalog.db"),
            self.data_dir.join("vectors"),
            self.data_dir.join("vectors.manifest.json"),
            self.data_dir.join("lum-worker.sock"),
        ]
        .into_iter()
        .filter(|path| path.exists())
        .collect()
    }

    pub fn ensure_data_dir(&self) -> Result<()> {
        ensure_private_dir(&self.data_dir)
    }
}

/// Create the directory owner-only. The socket's access control is the
/// directory's, so the mode is load-bearing rather than tidiness.
fn ensure_private_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(path)?.permissions();
        if perms.mode() & 0o777 != 0o700 {
            perms.set_mode(0o700);
            std::fs::set_permissions(path, perms)?;
        }
    }
    Ok(())
}

/// `LUM_EXCLUDE_DIRS` replaces the defaults rather than extending them, so
/// the effective list is always exactly what the variable says. Set but empty
/// disables name-based exclusion entirely, leaving only `.gitignore` and
/// hidden directories — which is a legitimate thing to want, and distinct
/// from unset.
fn exclude_dirs() -> HashSet<String> {
    match std::env::var("LUM_EXCLUDE_DIRS") {
        Ok(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect(),
        Err(_) => DEFAULT_EXCLUDE_DIRS.iter().map(|s| (*s).to_owned()).collect(),
    }
}

fn env_usize(key: &str) -> Result<Option<usize>> {
    match std::env::var(key) {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(value) => Ok(Some(value)),
            Err(_) => bail!("{key} must be a positive integer, got {raw:?}"),
        },
        Err(_) => Ok(None),
    }
}

/// Accepts durations the way people write them in shell profiles: `500ms`,
/// `90s`, `5m`, `2h`.
fn env_duration(key: &str) -> Result<Option<Duration>> {
    let Ok(raw) = std::env::var(key) else {
        return Ok(None);
    };
    let raw = raw.trim();
    let (value, unit) =
        raw.split_at(raw.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(raw.len()));
    let value: f64 = value
        .parse()
        .map_err(|_| anyhow::anyhow!("{key} must be a duration like 5m or 90s, got {raw:?}"))?;
    let seconds = match unit {
        "" | "s" => value,
        "ms" => value / 1000.0,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        other => bail!("{key} has unknown duration unit {other:?}; use ms, s, m, or h"),
    };
    Ok(Some(Duration::from_secs_f64(seconds)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_variants_have_distinct_identities() {
        assert_ne!(Model::Standard.name(), Model::Quantized.name());
        assert_eq!(Model::Standard.dimension(), Model::Quantized.dimension());
    }

    #[test]
    fn unknown_model_is_rejected_by_name() {
        let error = Model::parse("tiny").unwrap_err().to_string();
        assert!(error.contains("standard or quantized"), "{error}");
    }

    #[test]
    fn durations_accept_go_style_units() {
        // A profile that says 5m must never start meaning five seconds.
        for (raw, expected) in
            [("90s", 90.0), ("5m", 300.0), ("2h", 7200.0), ("500ms", 0.5), ("30", 30.0)]
        {
            std::env::set_var("LUM_TEST_DURATION", raw);
            assert_eq!(
                env_duration("LUM_TEST_DURATION").unwrap().unwrap().as_secs_f64(),
                expected,
                "{raw}"
            );
        }
        std::env::set_var("LUM_TEST_DURATION", "5 parsecs");
        assert!(env_duration("LUM_TEST_DURATION").is_err());
        std::env::remove_var("LUM_TEST_DURATION");
    }

    #[test]
    fn a_too_long_data_dir_is_reported_as_such() {
        // Otherwise this surfaces as a bind failure mentioning SUN_LEN, which
        // explains nothing about the setting that caused it.
        let config = Config {
            data_dir: PathBuf::from("/".to_owned() + &"d".repeat(200)),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            embed_batch: DEFAULT_EMBED_BATCH,
            embed_token_budget: DEFAULT_TOKEN_BUDGET,
            embed_threads: None,
            exclude_dirs: HashSet::new(),
            model: Model::Standard,
        };
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("LUM_DATA_DIR"), "{error}");
    }
}
