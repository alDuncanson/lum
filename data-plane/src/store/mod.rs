//! Vector storage: where embedded chunks live and get searched.
//!
//! The [`VectorStore`] trait is the seam that isolates the rest of lumen
//! from any particular vector database. Today the only implementation is
//! [`edge::EdgeStore`] (qdrant-edge, embedded in-process). If lum ever
//! outgrows it — or qdrant-edge's beta API shifts — a client for a
//! Qdrant *server* can implement this same trait and nothing upstream
//! changes.

pub mod edge;

use anyhow::Result;

use crate::pipeline::Chunk;

/// Identity + provenance of a document, stored alongside every chunk so
/// search results are self-describing (no join back to the control
/// plane needed to display a hit).
pub struct DocumentMeta<'a> {
    pub document_id: &'a str,
    pub source_id: &'a str,
    pub uri: &'a str,
}

/// One search result: a chunk plus its provenance and similarity score.
#[derive(Debug)]
pub struct Hit {
    pub document_id: String,
    pub source_id: String,
    pub uri: String,
    pub chunk_index: u32,
    pub score: f32,
    pub text: String,
}

/// Storage + retrieval for embedded chunks.
///
/// All methods are synchronous: implementations do local disk I/O and
/// CPU work. The gRPC layer calls them from tokio's blocking thread
/// pool (`spawn_blocking`), keeping the async runtime responsive.
pub trait VectorStore: Send + Sync {
    /// Store one vector point per chunk. `chunks` and `vectors` are
    /// parallel slices. Point IDs are derived from (document_id,
    /// chunk_index), so re-upserting a document overwrites in place.
    fn upsert_document(
        &self,
        meta: DocumentMeta<'_>,
        chunks: &[Chunk],
        vectors: Vec<Vec<f32>>,
    ) -> Result<()>;

    /// Remove the points for chunk indices `0..chunk_count`.
    fn delete_document(&self, document_id: &str, chunk_count: u32) -> Result<()>;

    /// Nearest-neighbor search over all stored chunks.
    fn search(&self, query_vector: Vec<f32>, limit: usize) -> Result<Vec<Hit>>;
}
