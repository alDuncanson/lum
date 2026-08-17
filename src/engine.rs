//! The engine: sources, scans, ingest, and search, in one address space.
//!
//! Everything between "a file changed" and "here are your results" happens
//! here: diff a directory against what is indexed, embed what changed, store
//! it, and keep answering queries while that is still happening.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::{mpsc, watch, Notify};

use crate::chunk::{Chunk, Chunker, SyntaxChunker};
use crate::config::Config;
use crate::db::{ChunkRow, Db};
use crate::embed::{self, Embedder};
use crate::events::{Bus, Event};
use crate::index::{dot_f32, Index};
use crate::mime;
use crate::parse::{InvalidArgument, ParserRegistry};
use crate::scan::{self, FileRef};
use crate::sys;
use crate::watch as fswatch;
use crate::wire;

/// Documents per embedding batch, and the byte budget that usually binds
/// first. Small documents are combined so one inference call covers many
/// files; a 4 MB budget is roughly a thousand chunks, which at 16 per
/// inference call is a manageable unit of work to report progress against.
const BATCH_DOCUMENTS: usize = 128;
const BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Collapsing can only discard, so a search over-fetches to fill `limit`
/// afterwards. Four times covers the realistic worst case — every result from
/// a handful of files — without asking for a thousand neighbours to show ten.
const OVERFETCH: usize = 4;
const MAX_FETCH: usize = 400;

/// How many extra candidates the int8 scan hands to the exact rescore.
/// Quantization only reorders near-ties, so double the shortlist is far more
/// headroom than the error needs.
const RESCORE_FACTOR: usize = 2;
const MAX_RESCORE: usize = 1000;

const RETRY_LIMIT: i64 = 3;
const DEBOUNCE: Duration = Duration::from_millis(1000);
/// Watching is an optimization; this is the authority. A missed event costs
/// staleness until this fires, never correctness.
const FALLBACK_RESCAN: Duration = Duration::from_secs(5 * 60);

pub enum EmbedderState {
    Starting,
    Downloading,
    Ready(Arc<Embedder>),
    Failed(String),
}

impl EmbedderState {
    fn name(&self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Downloading => "downloading-model",
            Self::Ready(_) => "ready",
            Self::Failed(_) => "failed",
        }
    }

    fn detail(&self) -> String {
        match self {
            Self::Starting => "loading".to_owned(),
            Self::Downloading => "downloading the embedding model (~130 MB, first run)".to_owned(),
            Self::Ready(embedder) => {
                let (query, ingest) = embedder.threads();
                format!(
                    "model={} batch={} threads={query}/{ingest}",
                    embedder.model().name(),
                    embedder.batch_size()
                )
            }
            Self::Failed(error) => error.clone(),
        }
    }
}

#[derive(Default)]
struct Activity {
    document: String,
    stage: &'static str,
    pending_documents: usize,
}

#[derive(Default)]
struct SourceRuntime {
    /// Whether a first full scan has ever completed for this source. What
    /// `add --wait` and `search --root` (with `wait`) block on.
    initial_done: bool,
}

pub struct Engine {
    pub config: Config,
    pub bus: Arc<Bus>,
    db: Arc<Db>,
    index: Arc<RwLock<Index>>,
    embedder: Arc<RwLock<EmbedderState>>,
    parsers: ParserRegistry,
    chunker: SyntaxChunker,

    scan_tx: mpsc::UnboundedSender<String>,
    queued: Mutex<HashSet<String>>,
    sources: Mutex<HashMap<String, SourceRuntime>>,
    watches: Mutex<HashMap<String, fswatch::Watch>>,
    generation: watch::Sender<u64>,
    activity: Mutex<Activity>,

    ready: Arc<Notify>,
    pub shutdown: Arc<Notify>,
    started: Instant,
}

impl Engine {
    pub fn new(config: Config) -> Result<Arc<Self>> {
        config.ensure_data_dir()?;
        let db = Arc::new(Db::open(&config.db_path(), config.model)?);
        let index = Arc::new(RwLock::new(Index::new(config.model.dimension())));
        let (scan_tx, scan_rx) = mpsc::unbounded_channel();
        let (generation, _) = watch::channel(0);

        let engine = Arc::new(Self {
            bus: Arc::new(Bus::new()),
            db,
            index,
            embedder: Arc::new(RwLock::new(EmbedderState::Starting)),
            parsers: ParserRegistry::with_defaults(),
            chunker: SyntaxChunker::default(),
            scan_tx,
            queued: Mutex::new(HashSet::new()),
            sources: Mutex::new(HashMap::new()),
            watches: Mutex::new(HashMap::new()),
            generation,
            activity: Mutex::new(Activity::default()),
            ready: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            started: Instant::now(),
            config,
        });

        // Ingest is a plain synchronous loop on its own OS thread rather than
        // an async task. Every step of it — reading, parsing, inference,
        // writing — is blocking work, and expressing that as blocking code
        // means no `spawn_blocking` at every stage and no chance of parking a
        // runtime worker inside ONNX.
        let ingest = Arc::clone(&engine);
        std::thread::Builder::new()
            .name("lum-ingest".into())
            .spawn(move || ingest.ingest_loop(scan_rx))
            .context("starting the ingest thread")?;

        Ok(engine)
    }

    // ---- startup ----

    /// Load the model, build the index, then start watching and scanning.
    ///
    /// Runs after the socket is already accepting, so a client connecting
    /// during a first-run model download gets `state: downloading-model`
    /// rather than a refused connection it has to interpret.
    pub fn start_background(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            if let Err(error) = engine.initialize() {
                tracing::error!(?error, "startup failed");
                let message = format!("{error:#}");
                *engine.embedder.write().unwrap() = EmbedderState::Failed(message.clone());
                engine.bus.publish(Event::state("failed", message));
                engine.ready.notify_waiters();
            }
        });

        let ticker = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                ticker.publish_snapshot();
            }
        });
    }

    fn initialize(self: &Arc<Self>) -> Result<()> {
        let bus = Arc::clone(&self.bus);
        let state = Arc::clone(&self.embedder);
        let embedder = Embedder::load(&self.config, || {
            *state.write().unwrap() = EmbedderState::Downloading;
            bus.publish(Event::state(
                "downloading-model",
                "downloading the embedding model (~130 MB, first run)",
            ));
        })?;
        let embedder = Arc::new(embedder);

        // The index is rebuilt from the database rather than persisted
        // separately, so there is no second artifact to keep in step and no
        // format to migrate. A hundred thousand chunks is a 150 MB sequential
        // read, once.
        let rows = self.db.all_vectors()?;
        {
            let mut index = self.index.write().unwrap();
            let mut by_document: HashMap<i64, DocumentVectors> = HashMap::new();
            for row in rows {
                by_document
                    .entry(row.document_id)
                    .or_insert_with(|| (row.source_id.clone(), Vec::new()))
                    .1
                    .push((row.chunk_id, row.vector));
            }
            for (document_id, (source_id, mut chunks)) in by_document {
                chunks.sort_by_key(|(id, _)| *id);
                index.replace_document(document_id, &source_id, &chunks);
            }
        }

        let loaded = self.index.read().unwrap().len();
        *self.embedder.write().unwrap() = EmbedderState::Ready(Arc::clone(&embedder));
        let detail = self.embedder.read().unwrap().detail();
        tracing::info!(chunks = loaded, "index loaded");
        self.bus.publish(Event::state("ready", detail));
        self.ready.notify_waiters();

        // Recovery is a rescan of everything: scans are idempotent and cheap
        // when nothing changed, which is what makes that acceptable as the
        // entire startup story.
        for source in self.db.list_sources()? {
            self.start_watch(&source.id, &source.uri);
            self.request_scan(&source.id);
        }
        Ok(())
    }

    // ---- readiness ----

    fn embedder_now(&self) -> Option<Arc<Embedder>> {
        match &*self.embedder.read().unwrap() {
            EmbedderState::Ready(embedder) => Some(Arc::clone(embedder)),
            _ => None,
        }
    }

    /// Wait for the model on a thread that is allowed to block.
    fn embedder_blocking(&self) -> Result<Arc<Embedder>> {
        let deadline = Instant::now() + self.config.startup_timeout;
        loop {
            match &*self.embedder.read().unwrap() {
                EmbedderState::Ready(embedder) => return Ok(Arc::clone(embedder)),
                EmbedderState::Failed(error) => bail!("{error}"),
                _ => {}
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for the embedding model to load");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Wait until the model is loaded, or fail with the reason it will not be.
    ///
    /// Bounded by `startup_timeout`. A failed load is terminal and reported
    /// immediately rather than waited out.
    async fn embedder(&self) -> Result<Arc<Embedder>> {
        let deadline = Instant::now() + self.config.startup_timeout;
        loop {
            {
                let state = self.embedder.read().unwrap();
                match &*state {
                    EmbedderState::Ready(embedder) => return Ok(Arc::clone(embedder)),
                    EmbedderState::Failed(error) => bail!("{error}"),
                    _ => {}
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!("timed out waiting for the embedding model to load");
            }
            let notified = self.ready.notified();
            // Re-check after registering, so a transition between the check
            // above and this await cannot be missed.
            match &*self.embedder.read().unwrap() {
                EmbedderState::Ready(embedder) => return Ok(Arc::clone(embedder)),
                EmbedderState::Failed(error) => bail!("{error}"),
                _ => {}
            }
            let _ = tokio::time::timeout(remaining.min(Duration::from_millis(250)), notified).await;
        }
    }

    // ---- sources ----

    /// Register a source, or return the one already covering this path.
    pub fn ensure_source(&self, uri: &str) -> Result<(crate::db::Source, bool)> {
        let canonical = canonicalize(uri)?;
        let existing = self.db.list_sources()?;
        if let Some(reason) = nesting_conflict(&canonical, &existing) {
            bail!(reason);
        }
        let (source, created) = self.db.add_source(&canonical)?;
        if created {
            self.sources.lock().unwrap().insert(source.id.clone(), SourceRuntime::default());
            self.start_watch(&source.id, &source.uri);
            self.request_scan(&source.id);
        }
        Ok((source, created))
    }

    pub fn list_sources(&self) -> Result<Vec<wire::Source>> {
        self.db
            .list_sources()?
            .into_iter()
            .map(|source| {
                let (documents, chunks) = self.db.source_stats(&source.id)?;
                Ok(wire::Source { id: source.id, uri: source.uri, documents, chunks })
            })
            .collect()
    }

    pub fn source_stats(&self, source_id: &str) -> Result<(i64, i64)> {
        self.db.source_stats(source_id)
    }

    /// Accepts a source id or a path, because the id is a UUID nobody has
    /// memorized and the path is what they typed to `add`.
    pub fn resolve_source(&self, wanted: &str) -> Result<crate::db::Source> {
        if let Some(source) = self.db.source_by_id(wanted)? {
            return Ok(source);
        }
        let sources = self.db.list_sources()?;
        if let Ok(canonical) = canonicalize(wanted) {
            if let Some(source) = sources.iter().find(|s| s.uri == canonical) {
                return Ok(source.clone());
            }
        }
        if sources.is_empty() {
            bail!("no sources are registered; add one with `lum add <directory>`");
        }
        let known: Vec<&str> = sources.iter().map(|s| s.uri.as_str()).collect();
        bail!("no source matches {wanted:?}; registered: {}", known.join(", "))
    }

    pub fn remove_source(&self, wanted: &str) -> Result<String> {
        let source = self.resolve_source(wanted)?;
        self.watches.lock().unwrap().remove(&source.id);
        let documents = self.db.delete_source(&source.id)?;
        {
            let mut index = self.index.write().unwrap();
            for document in documents {
                index.remove_document(document);
            }
        }
        self.sources.lock().unwrap().remove(&source.id);
        Ok(source.uri)
    }

    fn start_watch(&self, source_id: &str, uri: &str) {
        let root = PathBuf::from(uri);
        let (tx, mut rx) = mpsc::channel::<()>(1);
        let watcher = match fswatch::start(&root, self.config.exclude_dirs.clone(), move || {
            // try_send, not send: the channel holds one slot and a full one
            // already means "there is a change to look at".
            let _ = tx.try_send(());
        }) {
            Ok(watcher) => Some(watcher),
            Err(error) => {
                // Degraded, not broken: the fallback ticker below still
                // reconciles, just at five-minute latency.
                tracing::warn!(%error, uri, "watching unavailable; falling back to periodic scans");
                None
            }
        };
        if let Some(watcher) = watcher {
            self.watches.lock().unwrap().insert(source_id.to_owned(), watcher);
        }

        let scan_tx = self.scan_tx.clone();
        let queued = source_id.to_owned();
        let engine_id = source_id.to_owned();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    change = rx.recv() => {
                        if change.is_none() {
                            // The watch was dropped: the source is gone.
                            return;
                        }
                        // Debounce. A save triggers several events, and a
                        // branch switch triggers thousands.
                        tokio::time::sleep(DEBOUNCE).await;
                        while rx.try_recv().is_ok() {}
                    }
                    _ = tokio::time::sleep(FALLBACK_RESCAN) => {}
                }
                if scan_tx.send(queued.clone()).is_err() {
                    tracing::debug!(source = engine_id, "ingest stopped; ending watch");
                    return;
                }
            }
        });
    }

    // ---- scanning ----

    /// Queue a scan, deduplicated by source.
    pub fn request_scan(&self, source_id: &str) {
        let _ = self.scan_tx.send(source_id.to_owned());
    }

    /// Block until this source has completed a full scan at least once.
    pub async fn wait_initial(&self, source_id: &str) -> Result<()> {
        let deadline = Instant::now() + self.config.startup_timeout;
        let mut generation = self.generation.subscribe();
        loop {
            if self.sources.lock().unwrap().get(source_id).is_some_and(|state| state.initial_done) {
                return Ok(());
            }
            if let EmbedderState::Failed(error) = &*self.embedder.read().unwrap() {
                bail!("{error}");
            }
            if Instant::now() >= deadline {
                bail!("timed out waiting for the first index of this workspace");
            }
            let _ = tokio::time::timeout(Duration::from_millis(500), generation.changed()).await;
        }
    }

    // ---- search ----

    pub async fn search(
        self: &Arc<Self>,
        request: wire::SearchRequest,
    ) -> Result<wire::SearchResponse> {
        if request.q.trim().is_empty() {
            bail!("query must not be empty");
        }
        if request.root.is_some() && request.source.is_some() {
            bail!("root and source cannot be combined");
        }

        let filter = match (&request.root, &request.source) {
            (Some(root), _) => {
                let (source, _) = self.ensure_source(root)?;
                if request.wait {
                    self.wait_initial(&source.id).await?;
                }
                Some(source.id)
            }
            (None, Some(id)) => Some(self.resolve_source(id)?.id),
            (None, None) => None,
        };

        let embedder = self.embedder().await?;
        let fetch = fetch_limit(request.limit, request.per_file);
        let rescore = (fetch * RESCORE_FACTOR).clamp(fetch, MAX_RESCORE);

        // Embedding and the index scan are both CPU-bound, so they run
        // together off the runtime rather than each paying a hop.
        let index = Arc::clone(&self.index);
        let query = request.q.clone();
        let source = filter.clone();
        let (vector, ids) = tokio::task::spawn_blocking(move || -> Result<(Vec<f32>, Vec<i64>)> {
            let vector = embedder.embed_query(&query)?;
            let ids = index.read().unwrap().shortlist(&vector, rescore, source.as_deref());
            Ok((vector, ids))
        })
        .await??;

        // Exact rescore. The shortlist is int8 and may have the order of
        // near-ties slightly wrong; scoring the shortlist in f32 makes the
        // result identical to a full exact search.
        let mut payloads = self.db.chunk_payloads(&ids)?;
        let mut scored: Vec<(f32, crate::db::ChunkPayload)> = payloads
            .drain(..)
            .map(|payload| (dot_f32(&vector, &payload.vector), payload))
            .collect();
        scored.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));

        let mut results = Vec::with_capacity(request.limit);
        let mut per_file: HashMap<String, usize> = HashMap::new();
        for (score, payload) in scored {
            if request.exclude_tests && mime::is_test_path(&payload.uri) {
                continue;
            }
            if request.per_file > 0 {
                // Keyed on the document's URI within its source: two sources
                // scanning the same path are genuinely two documents.
                let key = format!("{}\u{0}{}", payload.source_id, payload.uri);
                let seen = per_file.entry(key).or_insert(0);
                if *seen >= request.per_file {
                    continue;
                }
                *seen += 1;
            }
            results.push(wire::SearchResult {
                uri: payload.uri,
                path: payload.path,
                source_id: payload.source_id,
                chunk_index: payload.chunk_index,
                score,
                text: payload.text,
                start_line: payload.start_line,
                end_line: payload.end_line,
            });
            if results.len() >= request.limit {
                break;
            }
        }
        Ok(wire::SearchResponse { query: request.q, results })
    }

    // ---- status ----

    pub fn status(&self) -> Result<wire::Status> {
        let stats = self.db.stats()?;
        let state = self.embedder.read().unwrap();
        let activity = self.activity.lock().unwrap();
        Ok(wire::Status {
            state: state.name().to_owned(),
            detail: state.detail(),
            sources: stats.sources,
            documents: stats.documents,
            chunks: stats.chunks,
            rss_bytes: sys::resident_bytes(),
            index_bytes: self.db.size_bytes(),
            vector_memory_bytes: self.index.read().unwrap().memory_bytes() as u64,
            ingest_session: matches!(&*state, EmbedderState::Ready(e) if e.ingest_loaded()),
            pending_scans: self.queued.lock().unwrap().len(),
            pending_documents: activity.pending_documents,
            active_document: activity.document.clone(),
            active_stage: activity.stage.to_owned(),
            failures: self
                .db
                .failures()?
                .into_iter()
                .map(|f| wire::Failure { uri: f.uri, attempts: f.attempts, error: f.error })
                .collect(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_seconds: self.started.elapsed().as_secs(),
        })
    }

    fn publish_snapshot(&self) {
        let Ok(stats) = self.db.stats() else { return };
        let activity = self.activity.lock().unwrap();
        let mut event = Event::new("snapshot");
        event.state = Some(self.embedder.read().unwrap().name());
        event.pending_scans = Some(self.queued.lock().unwrap().len() as u64);
        event.pending_documents = Some(activity.pending_documents as u64);
        event.sources = Some(stats.sources as u64);
        event.documents = Some(stats.documents as u64);
        event.total_chunks = Some(stats.chunks as u64);
        event.rss_bytes = Some(sys::resident_bytes());
        if !activity.document.is_empty() {
            event.path = Some(activity.document.clone());
            event.phase = Some(activity.stage);
        }
        drop(activity);
        self.bus.publish(event);
    }

    fn set_activity(&self, document: &str, stage: &'static str, pending: usize) {
        let mut activity = self.activity.lock().unwrap();
        activity.document = document.to_owned();
        activity.stage = stage;
        activity.pending_documents = pending;
    }

    // ---- ingest ----

    fn ingest_loop(self: Arc<Self>, mut scans: mpsc::UnboundedReceiver<String>) {
        while let Some(source_id) = scans.blocking_recv() {
            // Deduplicate: a burst of watch events and a startup scan for the
            // same source is one scan, not five.
            if !self.queued.lock().unwrap().insert(source_id.clone()) {
                continue;
            }
            // Drain any further requests for the same source that arrived
            // while we were waiting, so they collapse into this run.
            let result = self.run_scan(&source_id);
            self.queued.lock().unwrap().remove(&source_id);

            if let Err(error) = result {
                tracing::warn!(source = source_id, %error, "scan failed");
                let mut event = Event::new("scan_failed");
                event.source = Some(source_id.clone());
                event.error = Some(format!("{error:#}"));
                self.bus.publish(event);
            }

            self.sources.lock().unwrap().entry(source_id.clone()).or_default().initial_done = true;
            self.generation.send_modify(|generation| *generation += 1);

            // Nothing left to index: hand the ingest session's arena back.
            // This is the whole of the old idle-shedding feature, at session
            // granularity instead of process granularity, and queries never
            // noticed either way because they use a different session.
            if self.queued.lock().unwrap().is_empty() {
                if let Some(embedder) = self.embedder_now() {
                    if embedder.release_ingest() {
                        tracing::info!(
                            rss = sys::human_bytes(sys::resident_bytes()),
                            "ingest idle; released the inference session"
                        );
                    }
                }
                self.set_activity("", "", 0);
            }
        }
    }

    fn run_scan(self: &Arc<Self>, source_id: &str) -> Result<()> {
        let Some(source) = self.db.source_by_id(source_id)? else {
            return Ok(()); // removed while queued
        };
        // Wait rather than defer. A scan queued before the model finishes
        // loading — which is every `lum add` on a cold start — must not be
        // dropped: `initialize` only queues scans for the sources that existed
        // when it ran, so nothing would ever pick this one up and `--wait`
        // would return "0 documents" having indexed nothing.
        //
        // Blocking is free here: this is the ingest thread's own thread, and
        // there is nothing for it to do until the model exists.
        let embedder = self.embedder_blocking()?;

        let started = Instant::now();
        let root = PathBuf::from(&source.uri);
        if !root.is_dir() {
            bail!("source directory {} is gone", source.uri);
        }

        let refs = scan::scan(&root, &self.config.exclude_dirs)?;
        let mut known = self.db.document_states(&source.id)?;
        let permanent: HashSet<String> =
            self.db.permanent_failures(&source.id)?.into_iter().collect();

        let mut event = Event::new("scan_started");
        event.source = Some(source.id.clone());
        event.path = Some(source.uri.clone());
        self.bus.publish(event);

        // Plan: what changed, what vanished.
        let mut pending: Vec<PendingDocument> = Vec::new();
        let mut unchanged = 0u64;
        for file in refs {
            let previous = known.remove(&file.uri);
            if let Some(previous) = &previous {
                if previous.fingerprint == file.fingerprint && file.content_hash.is_none() {
                    unchanged += 1;
                    continue;
                }
            }
            if permanent.contains(&file.uri) {
                // A file that will never parse does not need rediscovering
                // three more times per scan.
                continue;
            }
            pending.push(PendingDocument { file, previous_hash: previous.map(|p| p.content_hash) });
        }
        let removed: Vec<String> = known.into_keys().collect();

        let mut indexed = 0u64;
        let mut failed = 0u64;

        for uri in &removed {
            match self.db.delete_document(&source.id, uri) {
                Ok(Some(document_id)) => {
                    self.index.write().unwrap().remove_document(document_id);
                    let mut event = Event::new("doc_deleted");
                    event.source = Some(source.id.clone());
                    event.path = Some(display_path(&root, uri));
                    self.bus.publish(event);
                }
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, uri, "deleting a vanished document"),
            }
            let _ = self.db.clear_failure(&source.id, uri);
        }

        let total = pending.len();
        let mut queue = pending.into_iter();
        let mut remaining = total;
        loop {
            let batch = collect_batch(&mut queue, &mut remaining);
            if batch.is_empty() {
                break;
            }
            let (ok, bad) = self.ingest_batch(&source, batch, &embedder, remaining)?;
            indexed += ok;
            failed += bad;
        }

        let elapsed = started.elapsed();
        let mut event = Event::new("scan_finished");
        event.source = Some(source.id.clone());
        event.path = Some(source.uri.clone());
        event.indexed = Some(indexed);
        event.removed = Some(removed.len() as u64);
        event.unchanged = Some(unchanged);
        event.failed = Some(failed);
        event.took_ms = Some(elapsed.as_millis() as u64);
        self.bus.publish(event);
        tracing::info!(
            source = source.id,
            indexed,
            removed = removed.len(),
            unchanged,
            failed,
            took_ms = elapsed.as_millis() as u64,
            "scan complete"
        );

        if failed > 0 {
            self.schedule_retry(&source.id);
        }
        Ok(())
    }

    /// Read, parse, chunk, embed, and store one batch.
    ///
    /// Every document is parsed and chunked before anything is embedded, and
    /// every document is stored only after the whole batch embeds. A failure
    /// in the middle therefore leaves the previous generation of each document
    /// intact rather than half-replaced.
    fn ingest_batch(
        self: &Arc<Self>,
        source: &crate::db::Source,
        batch: Vec<PendingDocument>,
        embedder: &Embedder,
        remaining: usize,
    ) -> Result<(u64, u64)> {
        let mut prepared: Vec<PreparedDocument> = Vec::with_capacity(batch.len());
        let mut texts: Vec<String> = Vec::new();
        let mut failed = 0u64;

        for item in batch {
            let file = item.file;
            self.set_activity(&file.path, "reading", remaining);

            let bytes = match std::fs::read(&file.uri) {
                Ok(bytes) => bytes,
                Err(error) => {
                    // A file that vanished between the scan and now is not a
                    // failure; it is a delete we will see on the next scan.
                    if error.kind() == std::io::ErrorKind::NotFound {
                        continue;
                    }
                    failed += self.fail_document(source, &file, &error.to_string(), false);
                    continue;
                }
            };
            let hash = scan::hash_bytes(&bytes);
            if item.previous_hash.as_deref() == Some(hash.as_str()) {
                // The fingerprint moved but the bytes did not — a touch, or a
                // checkout that rewrote an identical file. Nothing to embed.
                continue;
            }

            self.set_activity(&file.path, "parsing", remaining);
            let parsed = match self.parsers.parse(file.mime, &bytes) {
                Ok(parsed) => parsed,
                Err(error) => {
                    let permanent = error.downcast_ref::<InvalidArgument>().is_some();
                    failed += self.fail_document(source, &file, &format!("{error:#}"), permanent);
                    continue;
                }
            };
            let chunks = self.chunker.chunk(&parsed);
            tracing::debug!(
                path = file.path,
                language = parsed.language.map_or("text", crate::language::Language::name),
                chunks = chunks.len(),
                "chunked"
            );
            if chunks.is_empty() {
                // An empty or whitespace-only file. Record it as indexed so
                // its hash is stored and it stops being rescanned.
                self.store_document(source, &file, &hash, &[], &[])?;
                continue;
            }
            texts.extend(
                chunks.iter().map(|chunk| embed::passage_text(&file.path, &file.uri, chunk)),
            );
            prepared.push(PreparedDocument { file, hash, chunks });
        }

        if prepared.is_empty() {
            return Ok((0, failed));
        }

        let total = texts.len() as u64;
        let bus = Arc::clone(&self.bus);
        let label = prepared.first().map(|d| d.file.path.clone());
        self.set_activity(label.as_deref().unwrap_or(""), "embedding", remaining);
        bus.publish(Event::progress("embedding", 0, total, "chunks", label.clone()));

        let vectors = embedder.embed_passages(&texts, &|done| {
            bus.publish(Event::progress("embedding", done as u64, total, "chunks", label.clone()));
        })?;
        if vectors.len() != texts.len() {
            bail!("embedder returned {} vectors for {} chunks", vectors.len(), texts.len());
        }

        let mut vectors = vectors.into_iter();
        let mut indexed = 0u64;
        for document in prepared {
            let taken: Vec<Vec<f32>> = vectors.by_ref().take(document.chunks.len()).collect();
            self.set_activity(&document.file.path, "storing", remaining);
            self.store_document(source, &document.file, &document.hash, &document.chunks, &taken)?;

            let mut event = Event::new("doc_indexed");
            event.source = Some(source.id.clone());
            event.path = Some(document.file.path.clone());
            event.chunks = Some(document.chunks.len() as u64);
            self.bus.publish(event);
            indexed += 1;
        }
        Ok((indexed, failed))
    }

    fn store_document(
        &self,
        source: &crate::db::Source,
        file: &FileRef,
        hash: &str,
        chunks: &[Chunk],
        vectors: &[Vec<f32>],
    ) -> Result<()> {
        let rows: Vec<ChunkRow<'_>> = chunks
            .iter()
            .zip(vectors)
            .map(|(chunk, vector)| ChunkRow {
                index: chunk.index,
                start_line: chunk.start_line,
                end_line: chunk.end_line,
                text: &chunk.text,
                vector,
            })
            .collect();
        let (document_id, chunk_ids) = self.db.upsert_document(
            crate::db::DocumentWrite {
                source_id: &source.id,
                uri: &file.uri,
                path: &file.path,
                mime: file.mime,
                fingerprint: &file.fingerprint,
                content_hash: hash,
            },
            &rows,
        )?;

        // The index mirrors what the transaction just committed. Both are
        // replaced wholesale for this document, so neither can retain a chunk
        // the other dropped.
        let entries: Vec<(i64, Vec<f32>)> =
            chunk_ids.into_iter().zip(vectors.iter().cloned()).collect();
        self.index.write().unwrap().replace_document(document_id, &source.id, &entries);
        let _ = self.db.clear_failure(&source.id, &file.uri);
        Ok(())
    }

    fn fail_document(
        &self,
        source: &crate::db::Source,
        file: &FileRef,
        error: &str,
        permanent: bool,
    ) -> u64 {
        match self.db.record_failure(&source.id, &file.uri, error, permanent) {
            Ok(attempts) => {
                tracing::warn!(uri = file.uri, attempts, permanent, error, "indexing failed");
            }
            Err(error) => tracing::error!(%error, "recording an ingest failure"),
        }
        let mut event = Event::new("doc_failed");
        event.source = Some(source.id.clone());
        event.path = Some(file.path.clone());
        event.error = Some(error.to_owned());
        self.bus.publish(event);
        1
    }

    /// Re-run the source after a backoff, so a transient failure — a file
    /// locked by another process, a full disk that emptied — recovers without
    /// waiting for the next edit.
    fn schedule_retry(self: &Arc<Self>, source_id: &str) {
        let attempts = self
            .db
            .failures()
            .map(|failures| failures.iter().map(|f| f.attempts).max().unwrap_or(1))
            .unwrap_or(1);
        if attempts > RETRY_LIMIT {
            return;
        }
        let delay = Duration::from_secs(1 << (attempts.max(1) - 1).min(3) as u64);
        let scan_tx = self.scan_tx.clone();
        let source_id = source_id.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = scan_tx.send(source_id);
        });
    }
}

/// A document's source and its (chunk id, vector) pairs, while the index is
/// being rebuilt from the database at startup.
type DocumentVectors = (String, Vec<(i64, Vec<f32>)>);

struct PendingDocument {
    file: FileRef,
    previous_hash: Option<String>,
}

struct PreparedDocument {
    file: FileRef,
    hash: String,
    chunks: Vec<Chunk>,
}

/// Take documents until either limit is reached. Small files are combined so
/// one inference call covers many of them; a single large file is its own
/// batch.
fn collect_batch(
    queue: &mut impl Iterator<Item = PendingDocument>,
    remaining: &mut usize,
) -> Vec<PendingDocument> {
    let mut batch = Vec::new();
    let mut bytes = 0usize;
    for item in queue.by_ref() {
        bytes += item
            .file
            .fingerprint
            .split(':')
            .next()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        batch.push(item);
        *remaining = remaining.saturating_sub(1);
        if batch.len() >= BATCH_DOCUMENTS || bytes >= BATCH_BYTES {
            break;
        }
    }
    batch
}

/// How many chunks to shortlist so that `limit` survive collapsing.
fn fetch_limit(limit: usize, per_file: usize) -> usize {
    if per_file == 0 {
        return limit;
    }
    (limit * OVERFETCH).min(MAX_FETCH).max(limit)
}

fn display_path(root: &Path, uri: &str) -> String {
    Path::new(uri).strip_prefix(root).unwrap_or(Path::new(uri)).to_string_lossy().into_owned()
}

fn canonicalize(uri: &str) -> Result<String> {
    let expanded = if let Some(rest) = uri.strip_prefix("~/") {
        dirs::home_dir().ok_or_else(|| anyhow!("cannot expand ~: no home directory"))?.join(rest)
    } else if uri == "~" {
        dirs::home_dir().ok_or_else(|| anyhow!("cannot expand ~: no home directory"))?
    } else {
        PathBuf::from(uri)
    };
    let absolute = std::path::absolute(&expanded)
        .with_context(|| format!("resolving {}", expanded.display()))?;
    let resolved = absolute.canonicalize().unwrap_or(absolute);
    if !resolved.is_dir() {
        bail!("{} is not a directory", resolved.display());
    }
    Ok(resolved.to_string_lossy().into_owned())
}

/// Registering a directory inside — or containing — one already registered
/// indexes the overlap twice.
///
/// Documents are keyed on (source, uri) deliberately, so two sources seeing
/// the same path own independent rows. The consequence is that the same file
/// gets embedded twice, stored twice, and returned twice, under two documents
/// that collapsing cannot merge because they really are two documents.
///
/// So it is refused rather than warned about: a warning on a command whose
/// output is two lines and scrolls past is a warning nobody reads, and the
/// duplication is silent afterwards and annoying to diagnose.
fn nesting_conflict(candidate: &str, existing: &[crate::db::Source]) -> Option<String> {
    let candidate = Path::new(candidate);
    for source in existing {
        let registered = Path::new(&source.uri);
        if registered == candidate {
            // Not a conflict: re-adding the same directory is how `--root`
            // registers idempotently on every search.
            return None;
        }
        if candidate.starts_with(registered) {
            return Some(format!(
                "{} is already indexed as part of {}; search it with `lum search --root {}`, \
                 or remove the parent first",
                candidate.display(),
                registered.display(),
                candidate.display()
            ));
        }
        if registered.starts_with(candidate) {
            return Some(format!(
                "{} contains {}, which is already indexed; remove it first with `lum remove {}`",
                candidate.display(),
                registered.display(),
                registered.display()
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Source;

    fn source(uri: &str) -> Source {
        Source { id: uri.to_owned(), uri: uri.to_owned() }
    }

    #[test]
    fn readding_the_same_directory_is_not_a_conflict() {
        // `search --root` does this on literally every query.
        assert!(nesting_conflict("/a/b", &[source("/a/b")]).is_none());
    }

    #[test]
    fn a_child_of_a_registered_source_is_refused_with_the_command_that_fixes_it() {
        let reason = nesting_conflict("/a/b/c", &[source("/a/b")]).expect("should conflict");
        assert!(reason.contains("already indexed as part of"), "{reason}");
        assert!(reason.contains("lum search --root"), "{reason}");
    }

    #[test]
    fn a_parent_of_a_registered_source_is_refused_too() {
        let reason = nesting_conflict("/a", &[source("/a/b")]).expect("should conflict");
        assert!(reason.contains("lum remove /a/b"), "{reason}");
    }

    #[test]
    fn sibling_directories_with_a_shared_prefix_do_not_conflict() {
        // /a/bc is not inside /a/b. Comparing whole components rather than
        // string prefixes is what makes that true.
        assert!(nesting_conflict("/a/bc", &[source("/a/b")]).is_none());
    }

    #[test]
    fn overfetch_is_bounded_and_never_below_the_limit() {
        assert_eq!(fetch_limit(10, 2), 40);
        assert_eq!(fetch_limit(10, 0), 10, "collapsing off means no over-fetch");
        assert_eq!(fetch_limit(200, 2), MAX_FETCH, "a large limit must not become a full scan");
        assert_eq!(fetch_limit(500, 2), 500, "over-fetch must never return fewer than asked");
    }

    #[test]
    fn a_batch_stops_at_the_document_count() {
        let mut remaining = 200;
        let mut queue = (0..200).map(|i| PendingDocument {
            file: FileRef {
                uri: format!("/r/{i}.rs"),
                path: format!("{i}.rs"),
                mime: "text/x-rust",
                fingerprint: "10:0".into(),
                content_hash: None,
            },
            previous_hash: None,
        });
        assert_eq!(collect_batch(&mut queue, &mut remaining).len(), BATCH_DOCUMENTS);
        assert_eq!(remaining, 200 - BATCH_DOCUMENTS);
    }

    #[test]
    fn a_batch_stops_at_the_byte_budget() {
        // One big file is its own batch, so a 40 MB generated file does not
        // drag 127 others into an inference call with it.
        let mut remaining = 10;
        let mut queue = (0..10).map(|i| PendingDocument {
            file: FileRef {
                uri: format!("/r/{i}.rs"),
                path: format!("{i}.rs"),
                mime: "text/x-rust",
                fingerprint: format!("{}:0", BATCH_BYTES),
                content_hash: None,
            },
            previous_hash: None,
        });
        assert_eq!(collect_batch(&mut queue, &mut remaining).len(), 1);
    }
}
