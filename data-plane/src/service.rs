//! The gRPC service: glue between the proto contract and the pipeline.
//!
//! This layer should stay thin. Its jobs are exactly:
//!   1. compose the pipeline stages (parser → chunker → embedder → store),
//!   2. move work onto the blocking thread pool (every stage is
//!      CPU/disk-bound, and tonic handlers run on the async runtime),
//!   3. translate anyhow errors into gRPC statuses.
//!
//! Anything smarter — retries, scheduling, what to ingest when — belongs
//! in the control plane.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

use crate::pb;
use crate::pipeline::{Chunk, Chunker, Embedder, FastEmbedder, ParserRegistry, WordWindowChunker};
use crate::store::{edge::EdgeStore, DocumentMeta, VectorStore};

/// Must match the control plane's dataplane.ContractVersion.
const CONTRACT_VERSION: &str = "1";
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

/// Shared, immutable pipeline. Wrapped in `Arc` so request handlers can
/// move a cheap clone onto blocking threads.
struct Pipeline {
    parsers: ParserRegistry,
    chunker: Box<dyn Chunker>,
    embedder: Box<dyn Embedder>,
    store: Box<dyn VectorStore>,
}

pub struct DataPlaneService {
    pipeline: Arc<Pipeline>,
}

impl DataPlaneService {
    /// Build the default pipeline. Blocking: downloads the embedding
    /// model on first run and opens/creates the vector index.
    pub fn initialize(data_dir: &Path) -> anyhow::Result<Self> {
        let embedder = FastEmbedder::initialize(&data_dir.join("models"))?;
        let store = EdgeStore::open(
            &data_dir.join("vectors"),
            embedder.model_name(),
            embedder.dimension(),
        )?;
        Ok(Self {
            pipeline: Arc::new(Pipeline {
                parsers: ParserRegistry::with_defaults(),
                chunker: Box::new(WordWindowChunker::default()),
                embedder: Box::new(embedder),
                store: Box::new(store),
            }),
        })
    }

    /// Run `f` on the blocking thread pool with a handle to the pipeline.
    async fn run_blocking<T, F>(&self, f: F) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce(&Pipeline) -> anyhow::Result<T> + Send + 'static,
    {
        let pipeline = Arc::clone(&self.pipeline);
        tokio::task::spawn_blocking(move || f(&pipeline))
            .await
            .map_err(|e| Status::internal(format!("worker panicked: {e}")))?
            .map_err(|e| Status::internal(format!("{e:#}")))
    }
}

#[tonic::async_trait]
impl pb::data_plane_server::DataPlane for DataPlaneService {
    async fn health(
        &self,
        request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthResponse>, Status> {
        let _ = log_request(&request, "Health");
        // Initialization completes before the server starts listening,
        // so reachable implies ready.
        Ok(Response::new(pb::HealthResponse {
            ready: true,
            detail: format!("model={}", self.pipeline.embedder.model_name()),
            contract_version: CONTRACT_VERSION.to_owned(),
        }))
    }

    async fn ingest_document(
        &self,
        request: Request<pb::IngestDocumentRequest>,
    ) -> Result<Response<pb::IngestDocumentResponse>, Status> {
        let request_id = log_request(&request, "IngestDocument");
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
                // shrinking document leaves no stale tail behind.
                let started = Instant::now();
                p.store
                    .delete_document(&req.document_id, req.previous_chunk_count)?;
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

                let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
                let batch_size = texts.len();
                let started = Instant::now();
                let vectors = p.embedder.embed_passages(&texts)?;
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

        let pipeline = Arc::clone(&self.pipeline);
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
                    .checked_add(text.len())
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
                texts.extend(chunks.iter().map(|chunk| chunk.text.clone()));
                prepared.push(PreparedDocument {
                    request_index,
                    document,
                    chunks,
                });
            }

            // Complete every parse/chunk before mutating the store. An
            // embedding failure therefore leaves every old document intact.
            let started = Instant::now();
            let vectors = pipeline
                .embedder
                .embed_passages(&texts)
                .map_err(|error| Status::internal(format!("embedding batch: {error:#}")))?;
            let embed_duration = started.elapsed();
            if vectors.len() != texts.len() {
                return Err(Status::internal(
                    "embedding count does not match chunk count",
                ));
            }

            let total_chunks = texts.len();
            let mut vectors = vectors.into_iter();
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
                    .delete_document(
                        &document.header.document_id,
                        document.header.previous_chunk_count,
                    )
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
        let req = request.into_inner();
        self.run_blocking(move |p| p.store.delete_document(&req.document_id, req.chunk_count))
            .await?;
        Ok(Response::new(pb::DeleteDocumentResponse {}))
    }

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
        let _ = log_request(&request, "Search");
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
                let hits = p.store.search(vector, limit)?;
                Ok(hits
                    .into_iter()
                    .map(|h| pb::SearchResult {
                        document_id: h.document_id,
                        source_id: h.source_id,
                        uri: h.uri,
                        chunk_index: h.chunk_index,
                        score: h.score,
                        text: h.text,
                    })
                    .collect::<Vec<_>>())
            })
            .await?;

        Ok(Response::new(pb::SearchResponse { results }))
    }
}
