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

use std::path::Path;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::pb;
use crate::pipeline::{Chunker, Embedder, FastEmbedder, ParserRegistry, WordWindowChunker};
use crate::store::{edge::EdgeStore, DocumentMeta, VectorStore};

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
        let store = EdgeStore::open(&data_dir.join("vectors"), embedder.dimension())?;
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
        _request: Request<pb::HealthRequest>,
    ) -> Result<Response<pb::HealthResponse>, Status> {
        // Initialization completes before the server starts listening,
        // so reachable implies ready.
        Ok(Response::new(pb::HealthResponse {
            ready: true,
            detail: format!("model={}", self.pipeline.embedder.model_name()),
        }))
    }

    async fn ingest_document(
        &self,
        request: Request<pb::IngestDocumentRequest>,
    ) -> Result<Response<pb::IngestDocumentResponse>, Status> {
        let req = request.into_inner();
        let chunk_count = self
            .run_blocking(move |p| {
                let text = p.parsers.parse(&req.mime_type, &req.content)?;
                let chunks = p.chunker.chunk(&text);

                // Drop the previous generation of points first, so a
                // shrinking document leaves no stale tail behind.
                p.store
                    .delete_document(&req.document_id, req.previous_chunk_count)?;

                if chunks.is_empty() {
                    return Ok(0); // empty file: nothing to index
                }

                let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
                let vectors = p.embedder.embed_passages(&texts)?;
                p.store.upsert_document(
                    DocumentMeta {
                        document_id: &req.document_id,
                        source_id: &req.source_id,
                        uri: &req.uri,
                    },
                    &chunks,
                    vectors,
                )?;
                Ok(chunks.len() as u32)
            })
            .await?;

        Ok(Response::new(pb::IngestDocumentResponse { chunk_count }))
    }

    async fn delete_document(
        &self,
        request: Request<pb::DeleteDocumentRequest>,
    ) -> Result<Response<pb::DeleteDocumentResponse>, Status> {
        let req = request.into_inner();
        self.run_blocking(move |p| p.store.delete_document(&req.document_id, req.chunk_count))
            .await?;
        Ok(Response::new(pb::DeleteDocumentResponse {}))
    }

    async fn search(
        &self,
        request: Request<pb::SearchRequest>,
    ) -> Result<Response<pb::SearchResponse>, Status> {
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
