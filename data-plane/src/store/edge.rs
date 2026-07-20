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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

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

// Every index created before manifests were introduced used this model.
// Keeping that legacy identity explicit makes the one-time backfill safe:
// a future binary cannot bless old vectors as belonging to a new model.
const LEGACY_MODEL: &str = "BAAI/bge-small-en-v1.5";
const LEGACY_DIMENSION: usize = 384;

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
    pub fn open(path: &Path, model: &str, dimension: usize) -> Result<Self> {
        let has_manifest = validate_manifest(path, model, dimension)?;
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
        // Install only after qdrant has validated/opened the shard. A failed
        // model upgrade must never leave the old index mislabeled.
        if !has_manifest {
            write_manifest(path, model, dimension)?;
        }
        Ok(Self { shard })
    }
}

fn manifest_path(path: &Path) -> PathBuf {
    // Keep our metadata beside the qdrant-edge directory rather than
    // introducing an application-owned file into its storage layout.
    path.with_extension("manifest.json")
}

fn validate_manifest(path: &Path, model: &str, dimension: usize) -> Result<bool> {
    let manifest_path = manifest_path(path);
    if manifest_path.exists() {
        let bytes = std::fs::read(&manifest_path)
            .with_context(|| format!("reading index manifest {}", manifest_path.display()))?;
        let manifest: serde_json::Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing index manifest {}", manifest_path.display()))?;
        let stored_model = manifest.get("model").and_then(|v| v.as_str());
        let stored_dimension = manifest.get("dimension").and_then(|v| v.as_u64());
        if stored_model != Some(model) || stored_dimension != Some(dimension as u64) {
            return Err(anyhow!(
                "embedding model does not match the existing index (index: model={}, dimension={}; binary: model={model}, dimension={dimension}); clear the vector index and catalog under {} and re-add sources to re-ingest",
                stored_model.unwrap_or("unknown"),
                stored_dimension.map_or_else(|| "unknown".to_owned(), |v| v.to_string()),
                path.parent().unwrap_or(path).display(),
            ));
        }
        return Ok(true);
    }

    let has_legacy_index = if path.exists() {
        path.read_dir()
            .with_context(|| format!("reading vector dir {}", path.display()))?
            .next()
            .transpose()?
            .is_some()
    } else {
        false
    };
    if has_legacy_index && (model != LEGACY_MODEL || dimension != LEGACY_DIMENSION) {
        return Err(anyhow!(
            "existing index predates model manifests and was created with model={LEGACY_MODEL}, dimension={LEGACY_DIMENSION}; binary uses model={model}, dimension={dimension}; clear the vector index and catalog under {} and re-add sources to re-ingest",
            path.parent().unwrap_or(path).display(),
        ));
    }
    Ok(false)
}

fn write_manifest(path: &Path, model: &str, dimension: usize) -> Result<()> {
    let manifest_path = manifest_path(path);
    let manifest = serde_json::to_vec_pretty(&json!({
        "model": model,
        "dimension": dimension,
    }))?;
    let temporary_path = temporary_manifest_path(&manifest_path);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary_path)
        .with_context(|| format!("creating index manifest {}", temporary_path.display()))?;
    file.write_all(&manifest)
        .with_context(|| format!("writing index manifest {}", temporary_path.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing index manifest {}", temporary_path.display()))?;
    std::fs::rename(&temporary_path, &manifest_path)
        .with_context(|| format!("installing index manifest {}", manifest_path.display()))?;
    sync_parent(&manifest_path)?;
    Ok(())
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::File::open(parent)
            .with_context(|| format!("opening index manifest directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("syncing index manifest directory {}", parent.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<()> {
    Ok(())
}

fn temporary_manifest_path(manifest_path: &Path) -> PathBuf {
    manifest_path.with_extension(format!("json.tmp.{}", std::process::id()))
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static NEXT_PATH: AtomicUsize = AtomicUsize::new(0);

    fn test_index_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "lum-manifest-test-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        ))
    }

    #[test]
    fn manifest_rejects_a_different_model() {
        let path = test_index_path();
        std::fs::create_dir_all(&path).unwrap();
        write_manifest(&path, "model-a", 384).unwrap();
        assert!(validate_manifest(&path, "model-a", 384).unwrap());

        let error = validate_manifest(&path, "model-b", 768).unwrap_err();
        assert!(error.to_string().contains("does not match"));

        std::fs::remove_dir_all(&path).unwrap();
        std::fs::remove_file(manifest_path(&path)).unwrap();
    }

    #[test]
    fn legacy_index_cannot_be_claimed_by_a_different_model() {
        let path = test_index_path();
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("legacy-index-file"), b"index").unwrap();

        assert!(!validate_manifest(&path, LEGACY_MODEL, LEGACY_DIMENSION).unwrap());
        let error = validate_manifest(&path, "new-model", LEGACY_DIMENSION).unwrap_err();
        assert!(error.to_string().contains("predates model manifests"));
        assert!(!manifest_path(&path).exists());

        std::fs::remove_dir_all(path).unwrap();
    }
}
