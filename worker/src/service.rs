//! The gRPC service: glue between the proto contract and the pipeline.
//!
//! This layer should stay thin. Its jobs are exactly:
//!   1. compose the pipeline stages (parser → chunker → embedder → store),
//!   2. move work onto the blocking thread pool (every stage is
//!      CPU/disk-bound, and tonic handlers run on the async runtime),
//!   3. translate anyhow errors into gRPC statuses.
//!
//! Anything smarter — retries, scheduling, what to ingest when — belongs
//! in the dispatcher.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::pb;
use crate::pipeline::{
    Chunk, Chunker, Embedder, EmbeddingModelChoice, FastEmbedder, InvalidArgument, Language,
    ParserRegistry, SyntaxChunker,
};
use crate::store::{edge::EdgeStore, DocumentMeta, VectorStore};

fn search_result_from_hit(hit: crate::store::Hit) -> pb::SearchResult {
    pb::SearchResult {
        document_id: hit.document_id,
        source_id: hit.source_id,
        uri: hit.uri,
        chunk_index: hit.chunk_index,
        score: hit.score,
        text: hit.text,
        start_line: hit.start_line,
        end_line: hit.end_line,
    }
}

/// Must match the dispatcher's worker.ContractVersion.
const CONTRACT_VERSION: &str = "2";
const MAX_BATCH_DOCUMENTS: usize = 128;
const MAX_CONTENT_FRAME_BYTES: usize = 256 * 1024;
const MAX_BATCH_CONTENT_BYTES: usize = 32 * 1024 * 1024;
const MAX_PARSED_TEXT_BYTES: usize = 64 * 1024 * 1024;
const MAX_DOCUMENT_CHUNKS: usize = 16_384;
const MAX_BATCH_CHUNKS: usize = 32_768;

struct BatchDocument {
    header: pb::IngestBatchDocumentHeader,
    content: Vec<u8>,
}

struct PreparedDocument {
    request_index: usize,
    document: BatchDocument,
    chunks: Vec<Chunk>,
}

fn log_request<T>(request: &Request<T>, rpc: &'static str) -> String {
    let request_id = request
        .metadata()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("missing")
        .to_owned();
    tracing::info!(request_id, rpc, "gRPC request");
    request_id
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn batch_failure(
    document_id: String,
    stage: pb::IngestBatchFailureStage,
    message: String,
) -> pb::IngestBatchDocumentResult {
    pb::IngestBatchDocumentResult {
        document_id,
        outcome: Some(pb::ingest_batch_document_result::Outcome::Failure(
            pb::IngestBatchDocumentFailure {
                stage: stage.into(),
                message,
            },
        )),
    }
}

/// Text handed to the embedder for one chunk: a short context label followed
/// by the chunk itself.
///
/// The label is *not* stored — the payload keeps `chunk.text` alone, so
/// results still show code rather than a synthetic header. It exists purely
/// so the vector carries where the chunk came from.
///
/// People search with words that live in the path: "ingestion diagram",
/// "telescope plugin setup", "live activity tui". None of those could match
/// before, because only chunk text was ever embedded and the URI was
/// payload-only. A dozen tokens of path is cheap against bge-small's 512
/// budget and is exactly the context the query is reaching for.
///
/// `chunk.context` is the same idea one level down: for markdown, the trail
/// of headings above the chunk, which is where a document says what a
/// paragraph is about without the paragraph repeating it.
///
/// Falls back to deriving a label from the URI when the dispatcher sends
/// none, so an older peer degrades to something reasonable rather than to
/// nothing.
fn embed_text(display_path: &str, uri: &str, chunk: &Chunk) -> String {
    let label = if display_path.is_empty() {
        derive_label(uri)
    } else {
        display_path.to_owned()
    };
    let mut out = String::with_capacity(label.len() + chunk.context.len() + chunk.text.len() + 2);
    for prefix in [label.as_str(), chunk.context.as_str()] {
        if !prefix.is_empty() {
            out.push_str(prefix);
            out.push('\n');
        }
    }
    out.push_str(&chunk.text);
    out
}

/// Last two path components of a URI — enough to be useful, short enough to
/// leave out the parts of an absolute path that are identical for every
/// document and describe nobody's query.
/// The grammar that chunked a document, for the ingest log. "text" means the
/// word-window fallback, which is the answer to "why did this file chunk
/// oddly" often enough to be worth a field.
fn language_label(mime_type: &str) -> &'static str {
    Language::from_mime(mime_type).map_or("text", Language::name)
}

fn derive_label(uri: &str) -> String {
    let parts: Vec<&str> = uri.rsplit('/').take(2).collect();
    parts.into_iter().rev().collect::<Vec<_>>().join("/")
}

/// Shared, immutable pipeline. Wrapped in `Arc` so request handlers can
/// move a cheap clone onto blocking threads.
struct Pipeline {
    parsers: ParserRegistry,
    chunker: Box<dyn Chunker>,
    embedder: Box<dyn Embedder>,
    store: Box<dyn VectorStore>,
}

/// How many progress events a subscriber may fall behind before the oldest
/// are dropped. Progress is advisory: losing an update costs a stale bar for
/// a moment, whereas blocking the embedder to deliver one would trade the
/// work itself for a report about the work.
const PROGRESS_CHANNEL_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct WorkerService {
    state: Arc<RwLock<ServiceState>>,
    progress: broadcast::Sender<pb::WorkerEvent>,
}

/// Reports progress from inside blocking pipeline work.
///
/// Cloned into the blocking closure, so it must be cheap and usable off the
/// async runtime — `broadcast::Sender::send` is both, and returns an error
/// only when nobody is listening, which is not a problem worth handling.
#[derive(Clone)]
struct Progress {
    sender: broadcast::Sender<pb::WorkerEvent>,
    request_id: String,
}

impl Progress {
    fn report(&self, phase: &str, done: usize, total: usize, unit: &str, detail: &str) {
        let _ = self.sender.send(pb::WorkerEvent {
            request_id: self.request_id.clone(),
            phase: phase.to_owned(),
            done: done as u64,
            total: total as u64,
            unit: unit.to_owned(),
            detail: detail.to_owned(),
        });
    }
}

struct ServiceState {
    phase: pb::ReadinessState,
    detail: String,
    pipeline: Option<Arc<Pipeline>>,
}

impl WorkerService {
    pub fn starting() -> Self {
        let (progress, _) = broadcast::channel(PROGRESS_CHANNEL_CAPACITY);
        Self {
            state: Arc::new(RwLock::new(ServiceState {
                phase: pb::ReadinessState::Starting,
                detail: "starting".to_owned(),
                pipeline: None,
            })),
            progress,
        }
    }

    fn progress_for(&self, request_id: &str) -> Progress {
        Progress {
            sender: self.progress.clone(),
            request_id: request_id.to_owned(),
        }
    }

    /// Initialize the pipeline without holding the shared state lock.
    pub fn initialize(&self, data_dir: PathBuf, embedding_model: EmbeddingModelChoice) {
        {
            let mut state = self.state.write().expect("service state poisoned");
            state.phase = pb::ReadinessState::DownloadingModel;
            state.detail = "initializing embedding model and vector store".to_owned();
        }
        let result = (|| -> anyhow::Result<Pipeline> {
            let embedder = FastEmbedder::initialize(&data_dir.join("models"), embedding_model)?;
            let store = EdgeStore::open(
                &data_dir.join("vectors"),
                embedder.model_name(),
                embedder.dimension(),
            )?;
            Ok(Pipeline {
                parsers: ParserRegistry::with_defaults(),
                chunker: Box::new(SyntaxChunker::default()),
                embedder: Box::new(embedder),
                store: Box::new(store),
            })
        })();
        let mut state = self.state.write().expect("service state poisoned");
        match result {
            Ok(pipeline) => {
                state.detail = format!("model={}", pipeline.embedder.model_name());
                state.pipeline = Some(Arc::new(pipeline));
                state.phase = pb::ReadinessState::Ready;
            }
            Err(error) => {
                state.detail = format!("{error:#}");
                state.phase = pb::ReadinessState::Unavailable;
            }
        }
    }

    fn pipeline(&self) -> Result<Arc<Pipeline>, Status> {
        self.state
            .read()
            .expect("service state poisoned")
            .pipeline
            .clone()
            .ok_or_else(|| Status::unavailable("worker is not ready"))
    }

    /// Run `f` on the blocking thread pool with a handle to the pipeline.
    async fn run_blocking<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Pipeline) -> anyhow::Result<T> + Send + 'static,
    {
        let pipeline = self.pipeline()?;
        tokio::task::spawn_blocking(move || f(&pipeline))
            .await
            .map_err(|e| Status::internal(format!("worker panicked: {e}")))?
            .map_err(pipeline_error_status)
    }
}

/// Distinguishes the caller's fault (bad input, e.g. an unsupported MIME
/// type) from a genuine internal failure, rather than mapping every
/// pipeline error to `Status::internal` regardless of cause (#7).
fn pipeline_error_status(error: anyhow::Error) -> Status {
    if let Some(invalid) = error.downcast_ref::<InvalidArgument>() {
        return Status::invalid_argument(invalid.to_string());
    }
    Status::internal(format!("{error:#}"))
}

#[tonic::async_trait]
impl pb::worker_server::Worker for WorkerService {
    async fn health(
        &self,
        request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthResponse>, Status> {
        let _ = log_request(&request, "Health");
        let state = self.state.read().expect("service state poisoned");
        Ok(Response::new(pb::HealthResponse {
            ready: state.phase == pb::ReadinessState::Ready,
            detail: state.detail.clone(),
            contract_version: CONTRACT_VERSION.to_owned(),
            state: state.phase.into(),
        }))
    }

    async fn ingest_document(
        &self,
        request: Request<pb::IngestDocumentRequest>,
    ) -> Result<Response<pb::IngestDocumentResponse>, Status> {
        let request_id = log_request(&request, "IngestDocument");
        let _ = self.pipeline()?;
        let progress = self.progress_for(&request_id);
        let req = request.into_inner();
        let chunk_count = self
            .run_blocking(move |p| {
                let total_started = Instant::now();
                let content_bytes = req.content.len();

                let started = Instant::now();
                let text = p.parsers.parse(&req.mime_type, &req.content)?;
                let parse_duration = started.elapsed();

                let started = Instant::now();
                let chunks = p.chunker.chunk(&text);
                let chunk_duration = started.elapsed();

                // Drop the previous generation of points first, so a
                // shrinking document leaves no stale tail behind. A
                // filtered delete on document_id needs no chunk count (#3).
                let started = Instant::now();
                p.store.delete_document(&req.document_id)?;
                let delete_duration = started.elapsed();

                if chunks.is_empty() {
                    tracing::info!(
                        request_id,
                        document_id = req.document_id,
                        content_bytes,
                        chunks = 0,
                        batch_size = 0,
                        parse_ms = milliseconds(parse_duration),
                        chunk_ms = milliseconds(chunk_duration),
                        delete_ms = milliseconds(delete_duration),
                        embed_ms = 0.0,
                        upsert_ms = 0.0,
                        total_ms = milliseconds(total_started.elapsed()),
                        "ingest pipeline complete"
                    );
                    return Ok(0); // empty file: nothing to index
                }

                let texts: Vec<String> = chunks
                    .iter()
                    .map(|c| embed_text(&req.display_path, &req.uri, c))
                    .collect();
                let batch_size = texts.len();
                let started = Instant::now();
                let total_chunks = texts.len();
                let vectors = p.embedder.embed_passages_with_progress(&texts, &|done| {
                    progress.report("embedding", done, total_chunks, "chunks", &req.display_path);
                })?;
                let embed_duration = started.elapsed();

                let started = Instant::now();
                p.store.upsert_document(
                    DocumentMeta {
                        document_id: &req.document_id,
                        source_id: &req.source_id,
                        uri: &req.uri,
                    },
                    &chunks,
                    vectors,
                )?;
                let upsert_duration = started.elapsed();
                tracing::info!(
                    request_id,
                    document_id = req.document_id,
                    language = language_label(&req.mime_type),
                    content_bytes,
                    chunks = chunks.len(),
                    batch_size,
                    parse_ms = milliseconds(parse_duration),
                    chunk_ms = milliseconds(chunk_duration),
                    delete_ms = milliseconds(delete_duration),
                    embed_ms = milliseconds(embed_duration),
                    upsert_ms = milliseconds(upsert_duration),
                    total_ms = milliseconds(total_started.elapsed()),
                    "ingest pipeline complete"
                );
                Ok(chunks.len() as u32)
            })
            .await?;

        Ok(Response::new(pb::IngestDocumentResponse { chunk_count }))
    }

    async fn ingest_batch(
        &self,
        request: Request<tonic::Streaming<pb::IngestBatchRequest>>,
    ) -> Result<Response<pb::IngestBatchResponse>, Status> {
        let request_id = log_request(&request, "IngestBatch");
        let pipeline = self.pipeline()?;
        let progress = self.progress_for(&request_id);
        let mut stream = request.into_inner();
        let mut documents = Vec::new();
        let mut current: Option<BatchDocument> = None;
        let mut document_ids = HashSet::new();
        let mut declared_content_bytes = 0usize;

        while let Some(message) = stream.message().await? {
            let frame = message
                .frame
                .ok_or_else(|| Status::invalid_argument("ingest batch frame is empty"))?;
            match frame {
                pb::ingest_batch_request::Frame::Document(header) => {
                    if current.is_some() {
                        return Err(Status::invalid_argument(
                            "document header received before previous document ended",
                        ));
                    }
                    if documents.len() >= MAX_BATCH_DOCUMENTS {
                        return Err(Status::resource_exhausted("too many documents in batch"));
                    }
                    if header.document_id.is_empty()
                        || header.source_id.is_empty()
                        || header.uri.is_empty()
                        || header.mime_type.is_empty()
                    {
                        return Err(Status::invalid_argument(
                            "document_id, source_id, uri, and mime_type are required",
                        ));
                    }
                    if !document_ids.insert(header.document_id.clone()) {
                        return Err(Status::invalid_argument("duplicate document_id in batch"));
                    }
                    let content_length = usize::try_from(header.content_length)
                        .map_err(|_| Status::resource_exhausted("document content is too large"))?;
                    declared_content_bytes = declared_content_bytes
                        .checked_add(content_length)
                        .ok_or_else(|| Status::resource_exhausted("batch content is too large"))?;
                    if content_length > MAX_BATCH_CONTENT_BYTES
                        || declared_content_bytes > MAX_BATCH_CONTENT_BYTES
                    {
                        return Err(Status::resource_exhausted(
                            "batch content exceeds 32 MiB limit",
                        ));
                    }
                    current = Some(BatchDocument {
                        header,
                        content: Vec::with_capacity(content_length),
                    });
                }
                pb::ingest_batch_request::Frame::Content(content) => {
                    let document = current.as_mut().ok_or_else(|| {
                        Status::invalid_argument("content received without a document header")
                    })?;
                    let remaining =
                        document.header.content_length as usize - document.content.len();
                    let expected_length = remaining.min(MAX_CONTENT_FRAME_BYTES);
                    if content.len() != expected_length {
                        return Err(Status::invalid_argument(
                            "content frame length does not match required 256 KiB framing",
                        ));
                    }
                    if document.content.len() + content.len()
                        > document.header.content_length as usize
                    {
                        return Err(Status::invalid_argument(
                            "document content exceeds declared length",
                        ));
                    }
                    document.content.extend_from_slice(&content);
                }
                pb::ingest_batch_request::Frame::EndDocument(_) => {
                    let document = current.take().ok_or_else(|| {
                        Status::invalid_argument("end_document received without a document header")
                    })?;
                    if document.content.len() != document.header.content_length as usize {
                        return Err(Status::invalid_argument(
                            "document content length does not match header",
                        ));
                    }
                    documents.push(document);
                }
            }
        }
        if current.is_some() {
            return Err(Status::invalid_argument(
                "ingest batch ended before end_document frame",
            ));
        }
        if documents.is_empty() {
            return Err(Status::invalid_argument(
                "ingest batch contains no documents",
            ));
        }

        let results = tokio::task::spawn_blocking(move || {
            let total_started = Instant::now();
            let mut parse_duration = Duration::ZERO;
            let mut chunk_duration = Duration::ZERO;
            let mut store_duration = Duration::ZERO;
            let mut prepared = Vec::with_capacity(documents.len());
            let mut results = vec![None; documents.len()];
            let mut texts = Vec::new();
            let mut parsed_text_bytes = 0usize;

            for (request_index, document) in documents.into_iter().enumerate() {
                let started = Instant::now();
                let text = match pipeline
                    .parsers
                    .parse(&document.header.mime_type, &document.content)
                {
                    Ok(text) => text,
                    Err(error) => {
                        parse_duration += started.elapsed();
                        results[request_index] = Some(batch_failure(
                            document.header.document_id,
                            pb::IngestBatchFailureStage::Parse,
                            format!("{error:#}"),
                        ));
                        continue;
                    }
                };
                parse_duration += started.elapsed();
                parsed_text_bytes = parsed_text_bytes
                    .checked_add(text.text.len())
                    .ok_or_else(|| Status::resource_exhausted("parsed text is too large"))?;
                if parsed_text_bytes > MAX_PARSED_TEXT_BYTES {
                    return Err(Status::resource_exhausted(
                        "batch parsed text exceeds 64 MiB limit",
                    ));
                }

                let started = Instant::now();
                let chunks = pipeline.chunker.chunk(&text);
                chunk_duration += started.elapsed();
                if chunks.len() > MAX_DOCUMENT_CHUNKS {
                    results[request_index] = Some(batch_failure(
                        document.header.document_id,
                        pb::IngestBatchFailureStage::ResourceLimit,
                        format!("document produced more than {MAX_DOCUMENT_CHUNKS} chunks"),
                    ));
                    continue;
                }
                if texts.len() + chunks.len() > MAX_BATCH_CHUNKS {
                    return Err(Status::resource_exhausted(
                        "batch produced more than 32768 chunks",
                    ));
                }
                texts.extend(
                    chunks
                        .iter()
                        .map(|chunk| embed_text(&document.header.display_path, &document.header.uri, chunk)),
                );
                prepared.push(PreparedDocument {
                    request_index,
                    document,
                    chunks,
                });
            }

            // Complete every parse/chunk before mutating the store. An
            // embedding failure therefore leaves every old document intact.
            let started = Instant::now();
            let total_chunks = texts.len();
            progress.report("embedding", 0, total_chunks, "chunks", "");
            let vectors = pipeline
                .embedder
                .embed_passages_with_progress(&texts, &|done| {
                    progress.report("embedding", done, total_chunks, "chunks", "");
                })
                .map_err(|error| Status::internal(format!("embedding batch: {error:#}")))?;
            let embed_duration = started.elapsed();
            if vectors.len() != texts.len() {
                return Err(Status::internal(
                    "embedding count does not match chunk count",
                ));
            }

            let total_chunks = texts.len();
            let mut vectors = vectors.into_iter();
            let total_documents = prepared.len();
            let mut stored = 0usize;
            for prepared_document in prepared {
                let PreparedDocument {
                    request_index,
                    document,
                    chunks,
                } = prepared_document;
                let document_id = document.header.document_id.clone();
                let document_vectors: Vec<Vec<f32>> = vectors.by_ref().take(chunks.len()).collect();
                let started = Instant::now();
                let store_result = pipeline
                    .store
                    .delete_document(&document.header.document_id)
                    .and_then(|()| {
                        if chunks.is_empty() {
                            Ok(())
                        } else {
                            pipeline.store.upsert_document(
                                DocumentMeta {
                                    document_id: &document.header.document_id,
                                    source_id: &document.header.source_id,
                                    uri: &document.header.uri,
                                },
                                &chunks,
                                document_vectors,
                            )
                        }
                    });
                store_duration += started.elapsed();
                stored += 1;
                progress.report(
                    "storing",
                    stored,
                    total_documents,
                    "documents",
                    &document.header.display_path,
                );
                results[request_index] = Some(match store_result {
                    Ok(()) => pb::IngestBatchDocumentResult {
                        document_id,
                        outcome: Some(pb::ingest_batch_document_result::Outcome::Success(
                            pb::IngestBatchDocumentSuccess {
                                chunk_count: chunks.len() as u32,
                            },
                        )),
                    },
                    Err(error) => batch_failure(
                        document_id,
                        pb::IngestBatchFailureStage::Store,
                        format!("{error:#}"),
                    ),
                });
            }

            let results: Vec<_> = results
                .into_iter()
                .map(|result| result.expect("every batch document has an outcome"))
                .collect();
            let failed = results
                .iter()
                .filter(|result| {
                    matches!(
                        result.outcome,
                        Some(pb::ingest_batch_document_result::Outcome::Failure(_))
                    )
                })
                .count();
            tracing::info!(
                request_id,
                documents = results.len(),
                succeeded = results.len() - failed,
                failed,
                content_bytes = declared_content_bytes,
                chunks = total_chunks,
                max_embed_batch_size = crate::pipeline::embedder::PASSAGE_BATCH_SIZE,
                parse_ms = milliseconds(parse_duration),
                chunk_ms = milliseconds(chunk_duration),
                embed_ms = milliseconds(embed_duration),
                store_ms = milliseconds(store_duration),
                total_ms = milliseconds(total_started.elapsed()),
                "ingest batch complete"
            );
            Ok(results)
        })
        .await
        .map_err(|error| Status::internal(format!("worker panicked: {error}")))??;

        Ok(Response::new(pb::IngestBatchResponse {
            documents: results,
        }))
    }

    async fn delete_document(
        &self,
        request: Request<pb::DeleteDocumentRequest>,
    ) -> Result<Response<pb::DeleteDocumentResponse>, Status> {
        let _ = log_request(&request, "DeleteDocument");
        let _ = self.pipeline()?;
        let req = request.into_inner();
        self.run_blocking(move |p| p.store.delete_document(&req.document_id))
            .await?;
        Ok(Response::new(pb::DeleteDocumentResponse {}))
    }

    type EventsStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<pb::WorkerEvent, Status>> + Send + 'static>,
    >;

    async fn events(
        &self,
        request: Request<pb::WorkerEventsRequest>,
    ) -> Result<Response<Self::EventsStream>, Status> {
        let _ = log_request(&request, "Events");
        // Subscribing from here means only events published after this point
        // are delivered — correct for progress, which is worthless late.
        let receiver = self.progress.subscribe();
        let stream = BroadcastStream::new(receiver)
            // A lagged subscriber has missed updates. Skipping them is the
            // whole reason the channel is bounded: a slow reader must not be
            // able to stall the embedder.
            .filter_map(|event| event.ok().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::EventsStream))
    }

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        let _ = log_request(&request, "Search");
        let _ = self.pipeline()?;
        let req = request.into_inner();
        if req.query.trim().is_empty() {
            return Err(Status::invalid_argument("query must not be empty"));
        }
        let limit = if req.limit == 0 {
            10
        } else {
            req.limit as usize
        };

        let results = self
            .run_blocking(move |p| {
                let vector = p.embedder.embed_query(&req.query)?;
                let source_filter = (!req.source_id.is_empty()).then_some(req.source_id.as_str());
                let hits = p.store.search(vector, limit, source_filter)?;
                Ok(hits
                    .into_iter()
                    .map(search_result_from_hit)
                    .collect::<Vec<_>>())
            })
            .await?;

        Ok(Response::new(pb::SearchResponse { results }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::worker_server::Worker;

    #[tokio::test]
    async fn starting_service_reports_state_and_rejects_work() {
        let service = WorkerService::starting();
        let health = service
            .health(Request::new(pb::HealthRequest {}))
            .await
            .expect("health succeeds")
            .into_inner();
        assert!(!health.ready);
        assert_eq!(health.state, pb::ReadinessState::Starting as i32);
        assert_eq!(health.contract_version, CONTRACT_VERSION);

        let error = service
            .search(Request::new(pb::SearchRequest {
                query: "test".to_owned(),
                limit: 1,
                source_id: String::new(),
            }))
            .await
            .expect_err("search is unavailable before initialization");
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn pipeline_error_status_reports_invalid_argument_for_bad_input() {
        let error = anyhow::Error::new(InvalidArgument(
            "no parser registered for MIME type \"application/pdf\"".to_owned(),
        ));
        let status = pipeline_error_status(error);
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("application/pdf"));
    }

    #[test]
    fn pipeline_error_status_falls_back_to_internal_for_other_failures() {
        let status = pipeline_error_status(anyhow::anyhow!("store flush failed"));
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    fn chunk(text: &str, context: &str) -> Chunk {
        Chunk {
            text: text.to_owned(),
            context: context.to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn embed_text_prefixes_the_display_path_without_storing_it() {
        let embedded = embed_text(
            "docs/diagrams.md",
            "/abs/docs/diagrams.md",
            &chunk("flowchart LR", ""),
        );
        assert_eq!(embedded, "docs/diagrams.md\nflowchart LR");
    }

    #[test]
    fn embed_text_prefixes_the_heading_trail_too() {
        // Path, then where in the document, then the chunk. Neither prefix is
        // stored; both are what the query is reaching for.
        let embedded = embed_text(
            "docs/diagrams.md",
            "/abs/docs/diagrams.md",
            &chunk("flowchart LR", "Boundaries and protocols > Boundary reference"),
        );
        assert_eq!(
            embedded,
            "docs/diagrams.md\nBoundaries and protocols > Boundary reference\nflowchart LR"
        );
    }

    #[test]
    fn embed_text_derives_a_label_when_the_dispatcher_sends_none() {
        // An older dispatcher sends no display_path; a short suffix of the
        // URI beats embedding nothing, and beats embedding /Users/<name>/...
        let embedded = embed_text(
            "",
            "/Users/someone/code/lum/docs/diagrams.md",
            &chunk("flowchart LR", ""),
        );
        assert_eq!(embedded, "docs/diagrams.md\nflowchart LR");
    }

    #[test]
    fn embed_text_falls_back_to_bare_chunk_text() {
        assert_eq!(embed_text("", "", &chunk("just text", "")), "just text");
    }

    #[test]
    fn search_result_preserves_hit_line_range() {
        let result = search_result_from_hit(crate::store::Hit {
            document_id: "document".to_owned(),
            source_id: "source".to_owned(),
            uri: "/note.md".to_owned(),
            chunk_index: 2,
            score: 0.75,
            text: "result text".to_owned(),
            start_line: 12,
            end_line: 17,
        });
        assert_eq!((result.start_line, result.end_line), (12, 17));
    }
}
