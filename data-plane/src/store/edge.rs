//! [`VectorStore`] implementation on qdrant-edge.
//!
//! qdrant-edge is the Qdrant project's embedded engine: the same storage
//! and search core as the Qdrant server, but linked into our process and
//! persisting to a local directory — "SQLite for vector search". No
//! service to run, nothing to deploy.
//!
//! Caveat: qdrant-edge is beta and its API is lower-level than the
//! server's (an "Edge Shard" rather than named collections). All contact
//! with it is confined to this file on purpose.

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use qdrant_edge::{
    Distance, EdgeConfig, EdgeShard, EdgeVectorParams, NamedQuery, PointId, PointInsertOperations,
    PointOperations, PointStruct, QueryEnum, QueryRequest, ScoringQuery, UpdateOperation,
    WithPayloadInterface, WithVector, DEFAULT_VECTOR_NAME,
};
use serde_json::json;
use uuid::Uuid;

use super::{DocumentMeta, Hit, VectorStore};
use crate::pipeline::Chunk;

/// Fixed namespace for deriving chunk point UUIDs (UUIDv5 = SHA-1 of
/// namespace + name). Any random-but-constant value works; changing it
/// would orphan every existing point, so: don't.
const POINT_NAMESPACE: Uuid = Uuid::from_u128(0x9f8b_4a1e_6c75_4d2a_b03e_517f_22c8_d941);

/// Deterministic point ID for a chunk.
///
/// This is the trick that lets the whole system avoid filtered deletes
/// and vector-store-side bookkeeping: knowing only (document_id,
/// chunk_count) — which the control plane's catalog tracks — we can name
/// every point a document owns, for overwrite and for deletion.
fn point_uuid(document_id: &str, chunk_index: u32) -> Uuid {
    Uuid::new_v5(
        &POINT_NAMESPACE,
        format!("{document_id}/{chunk_index}").as_bytes(),
    )
}

pub struct EdgeStore {
    shard: EdgeShard,
}

impl EdgeStore {
    /// Open (or create) the index directory. `dimension` must match the
    /// embedder's output size — it is baked into the index at creation.
    pub fn open(path: &Path, dimension: usize) -> Result<Self> {
        std::fs::create_dir_all(path)
            .with_context(|| format!("creating vector dir {}", path.display()))?;

        // Cosine distance suits normalized sentence embeddings; scores
        // come back in [0, 1] where 1 is an exact semantic match.
        let config = EdgeConfig::builder()
            .on_disk_payload(false) // chunk payloads are small; keep in RAM
            .vector(
                DEFAULT_VECTOR_NAME,
                EdgeVectorParams::builder(dimension, Distance::Cosine).build(),
            )
            .build();

        // `load` opens an existing shard or initializes an empty one.
        let shard = EdgeShard::load(path, Some(config))
            .map_err(|e| anyhow!("opening qdrant-edge shard: {e}"))?;
        Ok(Self { shard })
    }
}

impl VectorStore for EdgeStore {
    fn upsert_document(
        &self,
        meta: DocumentMeta<'_>,
        chunks: &[Chunk],
        vectors: Vec<Vec<f32>>,
    ) -> Result<()> {
        assert_eq!(chunks.len(), vectors.len(), "chunk/vector count mismatch");

        let points = chunks
            .iter()
            .zip(vectors)
            .map(|(chunk, vector)| {
                PointStruct::new(
                    PointId::Uuid(point_uuid(meta.document_id, chunk.index)),
                    vector,
                    // The payload makes every point self-describing;
                    // search results are built from it alone.
                    json!({
                        "document_id": meta.document_id,
                        "source_id": meta.source_id,
                        "uri": meta.uri,
                        "chunk_index": chunk.index,
                        "text": chunk.text,
                    }),
                )
                .into()
            })
            .collect();

        self.shard
            .update(UpdateOperation::PointOperation(
                PointOperations::UpsertPoints(PointInsertOperations::PointsList(points)),
            ))
            .map_err(|e| anyhow!("qdrant-edge upsert: {e}"))?;

        // Flush before acking: once the control plane records this
        // document as ingested (hash + chunk_count in its catalog), a
        // crash must not be able to lose the vectors — the catalog
        // would then skip the document forever ("unchanged") while
        // search silently misses it. Durability before bookkeeping.
        self.shard.flush();
        Ok(())
    }

    fn delete_document(&self, document_id: &str, chunk_count: u32) -> Result<()> {
        if chunk_count == 0 {
            return Ok(());
        }
        let ids = (0..chunk_count)
            .map(|i| PointId::Uuid(point_uuid(document_id, i)))
            .collect();
        self.shard
            .update(UpdateOperation::PointOperation(
                PointOperations::DeletePoints { ids },
            ))
            .map_err(|e| anyhow!("qdrant-edge delete: {e}"))?;
        // Same durability-before-bookkeeping rule as upsert_document.
        self.shard.flush();
        Ok(())
    }

    fn search(&self, query_vector: Vec<f32>, limit: usize) -> Result<Vec<Hit>> {
        let points = self
            .shard
            .query(QueryRequest {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                    query: query_vector.into(),
                    using: None, // the default (only) vector
                }))),
                filter: None,
                score_threshold: None,
                limit,
                offset: 0,
                params: None,
                with_vector: WithVector::Bool(false),
                with_payload: WithPayloadInterface::Bool(true),
            })
            .map_err(|e| anyhow!("qdrant-edge query: {e}"))?;

        Ok(points
            .into_iter()
            .filter_map(|point| {
                // Round-trip the payload through serde_json::Value so we
                // depend only on its (stable) serialized shape, not on
                // qdrant-edge's internal Payload type.
                let payload = serde_json::to_value(point.payload.as_ref()?).ok()?;
                Some(Hit {
                    document_id: payload.get("document_id")?.as_str()?.to_owned(),
                    source_id: payload.get("source_id")?.as_str()?.to_owned(),
                    uri: payload.get("uri")?.as_str()?.to_owned(),
                    chunk_index: payload.get("chunk_index")?.as_u64()? as u32,
                    score: point.score,
                    text: payload.get("text")?.as_str()?.to_owned(),
                })
            })
            .collect())
    }
}
