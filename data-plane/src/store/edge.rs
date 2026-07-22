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
    Condition, CreateIndex, Distance, EdgeConfig, EdgeShard, EdgeVectorParams, FieldCondition,
    FieldIndexOperations, Filter, Match, NamedQuery, PayloadFieldSchema, PayloadSchemaType,
    PointId, PointInsertOperations, PointOperations, PointStruct, QueryEnum, QueryRequest,
    ScoringQuery, UpdateOperation, ValueVariants, WithPayloadInterface, WithVector,
    DEFAULT_VECTOR_NAME,
};
use serde_json::json;
use uuid::Uuid;

use super::{DocumentMeta, Hit, VectorStore};
use crate::pipeline::Chunk;

/// Fixed namespace for deriving chunk point UUIDs (UUIDv5 = SHA-1 of
/// namespace + name). Any random-but-constant value works; changing it
/// would orphan every existing point, so: don't.
const POINT_NAMESPACE: Uuid = Uuid::from_u128(0x9f8b_4a1e_6c75_4d2a_b03e_517f_22c8_d941);

/// Payload field indexed so every point belonging to a document can be
/// located and removed by a filtered delete, without the control plane
/// tracking or echoing back a chunk count (#3).
const DOCUMENT_ID_FIELD: &str = "document_id";

/// Payload field indexed so search can be restricted to one source (#7).
const SOURCE_ID_FIELD: &str = "source_id";

// Every index created before manifests were introduced used this model.
// Keeping that legacy identity explicit makes the one-time backfill safe:
// a future binary cannot bless old vectors as belonging to a new model.
const LEGACY_MODEL: &str = "BAAI/bge-small-en-v1.5";
const LEGACY_DIMENSION: usize = 384;

/// Deterministic point ID for a chunk (document_id, chunk_index). Kept
/// even though deletion no longer derives IDs from it (see
/// `document_id_filter`): re-ingesting the same document_id at the same
/// chunk count still overwrites points in place instead of writing new
/// ones, which is idempotent and avoids needless churn.
fn point_uuid(document_id: &str, chunk_index: u32) -> Uuid {
    Uuid::new_v5(
        &POINT_NAMESPACE,
        format!("{document_id}/{chunk_index}").as_bytes(),
    )
}

/// A filter matching every point whose payload field `field` equals
/// `value` — used both for document deletion (#3) and source-scoped
/// search (#7).
fn field_equals_filter(field: &str, value: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        field.parse().expect("field name is a valid JsonPath"),
        Match::new_value(ValueVariants::String(value.to_owned())),
    )))
}

/// A filter matching every point stored for one document, used for
/// deletion instead of deriving point IDs from a chunk count.
fn document_id_filter(document_id: &str) -> Filter {
    field_equals_filter(DOCUMENT_ID_FIELD, document_id)
}

fn payload_u32(payload: &serde_json::Value, field: &str) -> u32 {
    payload
        .get(field)
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(0)
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

        // Idempotent: creating an already-existing index is a documented
        // no-op upstream, so this runs unconditionally on every open.
        create_keyword_index(&shard, DOCUMENT_ID_FIELD)?;
        create_keyword_index(&shard, SOURCE_ID_FIELD)?;

        Ok(Self { shard })
    }
}

fn create_keyword_index(shard: &EdgeShard, field: &str) -> Result<()> {
    shard
        .update(UpdateOperation::FieldIndexOperation(
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name: field.parse().expect("field name is a valid JsonPath"),
                field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
            }),
        ))
        .map_err(|e| anyhow!("creating {field} payload index: {e}"))?;
    Ok(())
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
                        "start_line": chunk.start_line,
                        "end_line": chunk.end_line,
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

    fn delete_document(&self, document_id: &str) -> Result<()> {
        self.shard
            .update(UpdateOperation::PointOperation(
                PointOperations::DeletePointsByFilter(document_id_filter(document_id)),
            ))
            .map_err(|e| anyhow!("qdrant-edge delete: {e}"))?;
        // Same durability-before-bookkeeping rule as upsert_document.
        self.shard.flush();
        Ok(())
    }

    fn search(
        &self,
        query_vector: Vec<f32>,
        limit: usize,
        source_id: Option<&str>,
    ) -> Result<Vec<Hit>> {
        let filter = source_id.map(|id| field_equals_filter(SOURCE_ID_FIELD, id));
        let points = self
            .shard
            .query(QueryRequest {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
                    query: query_vector.into(),
                    using: None, // the default (only) vector
                }))),
                filter,
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
                    start_line: payload_u32(&payload, "start_line"),
                    end_line: payload_u32(&payload, "end_line"),
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
    fn payloads_without_line_ranges_degrade_to_unknown() {
        let old_payload = json!({ "document_id": "old", "text": "legacy" });
        assert_eq!(payload_u32(&old_payload, "start_line"), 0);
        assert_eq!(payload_u32(&old_payload, "end_line"), 0);
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

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
        let _ = std::fs::remove_file(manifest_path(path));
    }

    #[test]
    fn delete_document_removes_only_that_documents_points() {
        let path = test_index_path();
        let store = EdgeStore::open(&path, "test-model", 4).unwrap();

        store
            .upsert_document(
                DocumentMeta {
                    document_id: "doc-a",
                    source_id: "s",
                    uri: "/a",
                },
                &[
                    Chunk {
                        index: 0,
                        text: "a0".to_owned(),
                        start_line: 1,
                        end_line: 1,
                    },
                    Chunk {
                        index: 1,
                        text: "a1".to_owned(),
                        start_line: 2,
                        end_line: 3,
                    },
                ],
                vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]],
            )
            .unwrap();
        store
            .upsert_document(
                DocumentMeta {
                    document_id: "doc-b",
                    source_id: "s",
                    uri: "/b",
                },
                &[Chunk {
                    index: 0,
                    text: "b0".to_owned(),
                    start_line: 4,
                    end_line: 4,
                }],
                vec![vec![0.0, 0.0, 1.0, 0.0]],
            )
            .unwrap();

        let before = store.search(vec![1.0, 0.0, 0.0, 0.0], 10, None).unwrap();
        assert_eq!(
            before.len(),
            3,
            "expected both documents' points before delete"
        );

        store.delete_document("doc-a").unwrap();

        let after = store.search(vec![1.0, 0.0, 0.0, 0.0], 10, None).unwrap();
        assert_eq!(
            after.iter().map(|hit| hit.document_id.as_str()).collect::<Vec<_>>(),
            vec!["doc-b"],
            "delete_document must remove exactly the target document's points, leaving others intact"
        );

        drop(store); // flush/close before the directory is removed
        cleanup(&path);
    }

    #[test]
    fn delete_document_on_a_document_with_no_points_is_a_no_op() {
        let path = test_index_path();
        let store = EdgeStore::open(&path, "test-model", 4).unwrap();

        // Filtered delete on a document_id that never existed must succeed
        // quietly, matching the old chunk_count == 0 short-circuit.
        store.delete_document("never-ingested").unwrap();

        drop(store); // flush/close before the directory is removed
        cleanup(&path);
    }

    #[test]
    fn reingesting_a_shrunk_document_leaves_no_stale_tail() {
        let path = test_index_path();
        let store = EdgeStore::open(&path, "test-model", 4).unwrap();
        let meta = || DocumentMeta {
            document_id: "doc",
            source_id: "s",
            uri: "/doc",
        };

        store
            .upsert_document(
                meta(),
                &[
                    Chunk {
                        index: 0,
                        text: "v1-chunk0".to_owned(),
                        start_line: 1,
                        end_line: 1,
                    },
                    Chunk {
                        index: 1,
                        text: "v1-chunk1".to_owned(),
                        start_line: 2,
                        end_line: 2,
                    },
                ],
                vec![vec![1.0, 0.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]],
            )
            .unwrap();
        assert_eq!(
            store
                .search(vec![1.0, 0.0, 0.0, 0.0], 10, None)
                .unwrap()
                .len(),
            2
        );

        // Mirrors service.rs's ingest path: delete every existing point for
        // the document before upserting the new, shorter chunk set.
        store.delete_document("doc").unwrap();
        store
            .upsert_document(
                meta(),
                &[Chunk {
                    index: 0,
                    text: "v2-chunk0".to_owned(),
                    start_line: 3,
                    end_line: 5,
                }],
                vec![vec![1.0, 0.0, 0.0, 0.0]],
            )
            .unwrap();

        let after = store.search(vec![1.0, 0.0, 0.0, 0.0], 10, None).unwrap();
        assert_eq!(
            after.len(),
            1,
            "shrinking re-ingest must leave no stale trailing chunk"
        );
        assert_eq!(after[0].text, "v2-chunk0");
        assert_eq!((after[0].start_line, after[0].end_line), (3, 5));

        drop(store); // flush/close before the directory is removed
        cleanup(&path);
    }

    #[test]
    fn search_restricts_results_to_the_given_source_when_filtered() {
        let path = test_index_path();
        let store = EdgeStore::open(&path, "test-model", 4).unwrap();

        store
            .upsert_document(
                DocumentMeta {
                    document_id: "doc-a",
                    source_id: "source-a",
                    uri: "/a",
                },
                &[Chunk {
                    index: 0,
                    text: "a0".to_owned(),
                    start_line: 1,
                    end_line: 1,
                }],
                vec![vec![1.0, 0.0, 0.0, 0.0]],
            )
            .unwrap();
        store
            .upsert_document(
                DocumentMeta {
                    document_id: "doc-b",
                    source_id: "source-b",
                    uri: "/b",
                },
                &[Chunk {
                    index: 0,
                    text: "b0".to_owned(),
                    start_line: 1,
                    end_line: 1,
                }],
                vec![vec![1.0, 0.0, 0.0, 0.0]],
            )
            .unwrap();

        let unfiltered = store.search(vec![1.0, 0.0, 0.0, 0.0], 10, None).unwrap();
        assert_eq!(
            unfiltered.len(),
            2,
            "no filter should return both sources' points"
        );

        let filtered = store
            .search(vec![1.0, 0.0, 0.0, 0.0], 10, Some("source-a"))
            .unwrap();
        assert_eq!(
            filtered
                .iter()
                .map(|hit| hit.source_id.as_str())
                .collect::<Vec<_>>(),
            vec!["source-a"],
            "filtered search must return only the requested source's points"
        );

        drop(store);
        cleanup(&path);
    }
}
