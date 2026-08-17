//! One SQLite file: sources, documents, chunks, and the vectors themselves.
//!
//! One store on purpose. A document, its chunks, and their vectors are written
//! in a single transaction and deleted by a foreign key, so the bookkeeping
//! and the searchable vectors cannot disagree: there is no write ordering to
//! get right, no cross-store invariant to audit, and no `lum verify` to
//! write.
//!
//! Two connections, not one. WAL lets a reader proceed while a writer holds
//! the write lock, so a query does not queue behind an ingest transaction —
//! the same head-of-line problem the embedder solves with two sessions, one
//! layer down.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

use crate::config::Model;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
  id         TEXT PRIMARY KEY,
  uri        TEXT NOT NULL UNIQUE,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
  id           INTEGER PRIMARY KEY,
  source_id    TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  uri          TEXT NOT NULL,
  path         TEXT NOT NULL,
  mime         TEXT NOT NULL,
  fingerprint  TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  indexed_at   INTEGER NOT NULL,
  UNIQUE (source_id, uri)
);

-- Chunks carry their own vector. Deleting a document deletes its chunks, and
-- with them every vector that document contributed; that is the whole of the
-- consistency story.
CREATE TABLE IF NOT EXISTS chunks (
  id          INTEGER PRIMARY KEY,
  document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  idx         INTEGER NOT NULL,
  start_line  INTEGER NOT NULL,
  end_line    INTEGER NOT NULL,
  text        TEXT NOT NULL,
  vector      BLOB NOT NULL,
  UNIQUE (document_id, idx)
);
CREATE INDEX IF NOT EXISTS chunks_by_document ON chunks(document_id);

CREATE TABLE IF NOT EXISTS failures (
  source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  uri       TEXT NOT NULL,
  attempts  INTEGER NOT NULL,
  error     TEXT NOT NULL,
  permanent INTEGER NOT NULL DEFAULT 0,
  failed_at INTEGER NOT NULL,
  PRIMARY KEY (source_id, uri)
);

-- Keyword search over the same chunks the vectors describe, for the queries
-- an embedding misses: the exact identifier is in the file but nothing about
-- the phrasing is semantically near it. External content, so the text is
-- stored once — the FTS table holds only the inverted index.
--
-- Kept in sync by triggers rather than by application code, so every path
-- that touches chunks — including the ON DELETE CASCADE from documents and
-- sources — maintains it without knowing it exists. Chunks are only ever
-- inserted and deleted (an upsert is DELETE then INSERT), so there is no
-- UPDATE trigger to forget.
CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
  text,
  content='chunks',
  content_rowid='id',
  tokenize='unicode61'
);
CREATE TRIGGER IF NOT EXISTS chunks_fts_insert AFTER INSERT ON chunks BEGIN
  INSERT INTO chunks_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS chunks_fts_delete AFTER DELETE ON chunks BEGIN
  INSERT INTO chunks_fts(chunks_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;
"#;

#[derive(Debug, Clone)]
pub struct Source {
    pub id: String,
    pub uri: String,
}

/// What the catalog remembers about a document, for diffing against a scan.
#[derive(Debug, Clone)]
pub struct DocumentState {
    pub fingerprint: String,
    pub content_hash: String,
}

/// Everything identifying a document, so writing one is not eight positional
/// arguments in an order only the call site knows.
pub struct DocumentWrite<'a> {
    pub source_id: &'a str,
    pub uri: &'a str,
    /// Path relative to the source root.
    pub path: &'a str,
    pub mime: &'a str,
    pub fingerprint: &'a str,
    pub content_hash: &'a str,
}

/// A chunk ready to be written, with its vector.
pub struct ChunkRow<'a> {
    pub index: u32,
    pub start_line: u32,
    pub end_line: u32,
    pub text: &'a str,
    pub vector: &'a [f32],
}

/// One chunk's payload, hydrated for a search result.
#[derive(Debug, Clone)]
pub struct ChunkPayload {
    /// The chunk's rowid — the same id the vector index and the keyword index
    /// rank by, which is what lets rank fusion match payloads back up.
    pub chunk_id: i64,
    pub uri: String,
    pub path: String,
    pub source_id: String,
    pub chunk_index: u32,
    pub start_line: u32,
    pub end_line: u32,
    pub text: String,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub sources: i64,
    pub documents: i64,
    pub chunks: i64,
}

#[derive(Debug, Clone)]
pub struct Failure {
    pub uri: String,
    pub attempts: i64,
    pub error: String,
}

/// Every vector in the index, as loaded at startup.
pub struct IndexRow {
    pub chunk_id: i64,
    pub document_id: i64,
    pub source_id: String,
    pub vector: Vec<f32>,
}

pub struct Db {
    write: Mutex<Connection>,
    read: Mutex<Connection>,
    path: PathBuf,
}

impl Db {
    pub fn open(path: &Path, model: Model) -> Result<Self> {
        let write =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        configure(&write)?;
        write.execute_batch(SCHEMA).context("creating the schema")?;
        check_identity(&write, model)?;
        backfill_fts(&write)?;

        let read = Connection::open(path)
            .with_context(|| format!("opening {} for reading", path.display()))?;
        configure(&read)?;

        Ok(Self { write: Mutex::new(write), read: Mutex::new(read), path: path.to_path_buf() })
    }

    /// Bytes on disk, including the write-ahead log. Reported by `status`
    /// because it is the number this rewrite most wants to be held to.
    pub fn size_bytes(&self) -> u64 {
        let mut total = 0;
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.clone().into_os_string();
            path.push(suffix);
            if let Ok(meta) = std::fs::metadata(PathBuf::from(path)) {
                total += meta.len();
            }
        }
        total
    }

    // ---- sources ----

    pub fn list_sources(&self) -> Result<Vec<Source>> {
        let conn = self.read.lock().unwrap();
        let mut statement = conn.prepare("SELECT id, uri FROM sources ORDER BY created_at")?;
        let rows = statement
            .query_map([], |row| Ok(Source { id: row.get(0)?, uri: row.get(1)? }))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Register a source, or return the existing one. The `bool` is whether it
    /// was created, which is what makes `add` idempotent and lets
    /// `search --root` register on every call without re-indexing.
    pub fn add_source(&self, uri: &str) -> Result<(Source, bool)> {
        let conn = self.write.lock().unwrap();
        if let Some(source) = lookup_by_uri(&conn, uri)? {
            return Ok((source, false));
        }
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO sources (id, uri, created_at) VALUES (?1, ?2, ?3)",
            params![id, uri, now()],
        )?;
        Ok((Source { id, uri: uri.to_owned() }, true))
    }

    pub fn source_by_id(&self, id: &str) -> Result<Option<Source>> {
        let conn = self.read.lock().unwrap();
        Ok(conn
            .query_row("SELECT id, uri FROM sources WHERE id = ?1", [id], |row| {
                Ok(Source { id: row.get(0)?, uri: row.get(1)? })
            })
            .optional()?)
    }

    /// Delete a source and everything indexed from it.
    ///
    /// One statement. The cascade removes its documents, their chunks, and
    /// with them every vector — which is why this needs no ordering rule and
    /// cannot half-succeed.
    pub fn delete_source(&self, id: &str) -> Result<Vec<i64>> {
        let mut conn = self.write.lock().unwrap();
        let transaction = conn.transaction()?;
        let documents: Vec<i64> = transaction
            .prepare("SELECT id FROM documents WHERE source_id = ?1")?
            .query_map([id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        transaction.execute("DELETE FROM sources WHERE id = ?1", [id])?;
        transaction.commit()?;
        Ok(documents)
    }

    // ---- documents ----

    /// Everything the catalog knows about one source's documents, keyed by
    /// URI, for diffing a scan against.
    pub fn document_states(&self, source_id: &str) -> Result<HashMap<String, DocumentState>> {
        let conn = self.read.lock().unwrap();
        let mut statement = conn
            .prepare("SELECT uri, fingerprint, content_hash FROM documents WHERE source_id = ?1")?;
        let rows = statement.query_map([source_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                DocumentState { fingerprint: row.get(1)?, content_hash: row.get(2)? },
            ))
        })?;
        let mut out = HashMap::new();
        for row in rows {
            let (uri, state) = row?;
            out.insert(uri, state);
        }
        Ok(out)
    }

    /// Write a document and its chunks as one unit.
    ///
    /// Returns the document id and the chunk ids, so the in-memory index can
    /// be updated to match without re-reading anything.
    pub fn upsert_document(
        &self,
        document: DocumentWrite<'_>,
        chunks: &[ChunkRow<'_>],
    ) -> Result<(i64, Vec<i64>)> {
        let DocumentWrite { source_id, uri, path, mime, fingerprint, content_hash } = document;
        let mut conn = self.write.lock().unwrap();
        let transaction = conn.transaction()?;

        transaction.execute(
            "INSERT INTO documents (source_id, uri, path, mime, fingerprint, content_hash, indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (source_id, uri) DO UPDATE SET
               path = excluded.path,
               mime = excluded.mime,
               fingerprint = excluded.fingerprint,
               content_hash = excluded.content_hash,
               indexed_at = excluded.indexed_at",
            params![source_id, uri, path, mime, fingerprint, content_hash, now()],
        )?;
        let document_id: i64 = transaction.query_row(
            "SELECT id FROM documents WHERE source_id = ?1 AND uri = ?2",
            params![source_id, uri],
            |row| row.get(0),
        )?;

        // Replace rather than merge. A document that shrank must not keep the
        // tail of its previous self, and comparing chunk-by-chunk to find out
        // costs more than rewriting a few rows.
        transaction.execute("DELETE FROM chunks WHERE document_id = ?1", [document_id])?;

        let mut ids = Vec::with_capacity(chunks.len());
        {
            let mut insert = transaction.prepare(
                "INSERT INTO chunks (document_id, idx, start_line, end_line, text, vector)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for chunk in chunks {
                insert.execute(params![
                    document_id,
                    chunk.index,
                    chunk.start_line,
                    chunk.end_line,
                    chunk.text,
                    encode_vector(chunk.vector),
                ])?;
                ids.push(transaction.last_insert_rowid());
            }
        }
        transaction.commit()?;
        Ok((document_id, ids))
    }

    /// Remove one document. Returns its id if it existed.
    pub fn delete_document(&self, source_id: &str, uri: &str) -> Result<Option<i64>> {
        let conn = self.write.lock().unwrap();
        let id: Option<i64> = conn
            .query_row(
                "SELECT id FROM documents WHERE source_id = ?1 AND uri = ?2",
                params![source_id, uri],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(id) = id {
            conn.execute("DELETE FROM documents WHERE id = ?1", [id])?;
        }
        Ok(id)
    }

    // ---- reads for search ----

    /// Hydrate chunks by id, in no particular order.
    ///
    /// Text lives on disk rather than in the index, so the resident cost of an
    /// index is its vectors alone. Fetching a few hundred rows by primary key
    /// out of SQLite's page cache does not measure.
    pub fn chunk_payloads(&self, ids: &[i64]) -> Result<Vec<ChunkPayload>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.read.lock().unwrap();
        let placeholders = std::iter::repeat_n("?", ids.len()).collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT c.id, d.uri, d.path, d.source_id, c.idx, c.start_line, c.end_line, c.text, \
             c.vector
             FROM chunks c JOIN documents d ON d.id = c.document_id
             WHERE c.id IN ({placeholders})"
        );
        let mut statement = conn.prepare(&sql)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(ids), |row| {
                Ok(ChunkPayload {
                    chunk_id: row.get(0)?,
                    uri: row.get(1)?,
                    path: row.get(2)?,
                    source_id: row.get(3)?,
                    chunk_index: row.get(4)?,
                    start_line: row.get(5)?,
                    end_line: row.get(6)?,
                    text: row.get(7)?,
                    vector: decode_vector(&row.get::<_, Vec<u8>>(8)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Chunk ids ranked by BM25 against `match_query(query)`, best first.
    ///
    /// Empty when the query has no searchable terms. The ranking is fused
    /// with the vector ranking in `engine::search`; neither side is shown to
    /// the user as a score, so BM25's scale never needs calibrating.
    pub fn keyword_search(
        &self,
        query: &str,
        limit: usize,
        source_id: Option<&str>,
    ) -> Result<Vec<i64>> {
        let Some(match_expr) = match_query(query) else {
            return Ok(Vec::new());
        };
        let conn = self.read.lock().unwrap();
        // bm25() is a rank where smaller is better, hence ascending.
        let mut rows = Vec::with_capacity(limit);
        match source_id {
            Some(source) => {
                let mut statement = conn.prepare(
                    "SELECT f.rowid FROM chunks_fts f
                     JOIN chunks c ON c.id = f.rowid
                     JOIN documents d ON d.id = c.document_id
                     WHERE chunks_fts MATCH ?1 AND d.source_id = ?2
                     ORDER BY bm25(chunks_fts) LIMIT ?3",
                )?;
                let ids = statement
                    .query_map(params![match_expr, source, limit as i64], |row| {
                        row.get::<_, i64>(0)
                    })?;
                for id in ids {
                    rows.push(id?);
                }
            }
            None => {
                let mut statement = conn.prepare(
                    "SELECT rowid FROM chunks_fts
                     WHERE chunks_fts MATCH ?1
                     ORDER BY bm25(chunks_fts) LIMIT ?2",
                )?;
                let ids = statement
                    .query_map(params![match_expr, limit as i64], |row| row.get::<_, i64>(0))?;
                for id in ids {
                    rows.push(id?);
                }
            }
        }
        Ok(rows)
    }

    /// Every vector, for building the in-memory index at startup.
    pub fn all_vectors(&self) -> Result<Vec<IndexRow>> {
        let conn = self.read.lock().unwrap();
        let mut statement = conn.prepare(
            "SELECT c.id, c.document_id, d.source_id, c.vector
             FROM chunks c JOIN documents d ON d.id = c.document_id",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok(IndexRow {
                    chunk_id: row.get(0)?,
                    document_id: row.get(1)?,
                    source_id: row.get(2)?,
                    vector: decode_vector(&row.get::<_, Vec<u8>>(3)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn stats(&self) -> Result<Stats> {
        let conn = self.read.lock().unwrap();
        Ok(Stats {
            sources: conn.query_row("SELECT COUNT(*) FROM sources", [], |r| r.get(0))?,
            documents: conn.query_row("SELECT COUNT(*) FROM documents", [], |r| r.get(0))?,
            chunks: conn.query_row("SELECT COUNT(*) FROM chunks", [], |r| r.get(0))?,
        })
    }

    pub fn source_stats(&self, source_id: &str) -> Result<(i64, i64)> {
        let conn = self.read.lock().unwrap();
        let documents = conn.query_row(
            "SELECT COUNT(*) FROM documents WHERE source_id = ?1",
            [source_id],
            |r| r.get(0),
        )?;
        let chunks = conn.query_row(
            "SELECT COUNT(*) FROM chunks c JOIN documents d ON d.id = c.document_id
             WHERE d.source_id = ?1",
            [source_id],
            |r| r.get(0),
        )?;
        Ok((documents, chunks))
    }

    // ---- failures ----

    pub fn record_failure(
        &self,
        source_id: &str,
        uri: &str,
        error: &str,
        permanent: bool,
    ) -> Result<i64> {
        let conn = self.write.lock().unwrap();
        conn.execute(
            "INSERT INTO failures (source_id, uri, attempts, error, permanent, failed_at)
             VALUES (?1, ?2, 1, ?3, ?4, ?5)
             ON CONFLICT (source_id, uri) DO UPDATE SET
               attempts = failures.attempts + 1,
               error = excluded.error,
               permanent = excluded.permanent,
               failed_at = excluded.failed_at",
            params![source_id, uri, error, permanent as i64, now()],
        )?;
        Ok(conn.query_row(
            "SELECT attempts FROM failures WHERE source_id = ?1 AND uri = ?2",
            params![source_id, uri],
            |row| row.get(0),
        )?)
    }

    pub fn clear_failure(&self, source_id: &str, uri: &str) -> Result<()> {
        let conn = self.write.lock().unwrap();
        conn.execute(
            "DELETE FROM failures WHERE source_id = ?1 AND uri = ?2",
            params![source_id, uri],
        )?;
        Ok(())
    }

    pub fn failures(&self) -> Result<Vec<Failure>> {
        let conn = self.read.lock().unwrap();
        let mut statement =
            conn.prepare("SELECT uri, attempts, error FROM failures ORDER BY failed_at DESC")?;
        let rows = statement
            .query_map([], |row| {
                Ok(Failure { uri: row.get(0)?, attempts: row.get(1)?, error: row.get(2)? })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// URIs whose failure is permanent, so a retry can skip them rather than
    /// rediscovering three more times that a PDF is still a PDF.
    pub fn permanent_failures(&self, source_id: &str) -> Result<Vec<String>> {
        let conn = self.read.lock().unwrap();
        let mut statement =
            conn.prepare("SELECT uri FROM failures WHERE source_id = ?1 AND permanent = 1")?;
        let rows = statement
            .query_map([source_id], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

/// Discard every document and vector, keeping the registered sources, and
/// stamp the index with `model`.
///
/// Deliberately a free function that opens the file directly and skips the
/// identity check: the reason to run it is usually that the check is refusing
/// to open the database at all.
pub fn reset_index(path: &Path, model: Model) -> Result<usize> {
    let mut conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    configure(&conn)?;
    conn.execute_batch(SCHEMA)?;

    let transaction = conn.transaction()?;
    // Cascades into chunks, taking every vector with it.
    transaction.execute("DELETE FROM documents", [])?;
    transaction.execute("DELETE FROM failures", [])?;
    transaction.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('model', ?1), ('dimension', ?2)",
        params![model.name(), model.dimension().to_string()],
    )?;
    let sources: i64 =
        transaction.query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))?;
    transaction.commit()?;

    // Give the space back rather than leaving a file sized for the index we
    // just deleted.
    conn.execute_batch("VACUUM")?;
    Ok(sources as usize)
}

fn lookup_by_uri(conn: &Connection, uri: &str) -> Result<Option<Source>> {
    Ok(conn
        .query_row("SELECT id, uri FROM sources WHERE uri = ?1", [uri], |row| {
            Ok(Source { id: row.get(0)?, uri: row.get(1)? })
        })
        .optional()?)
}

fn configure(conn: &Connection) -> Result<()> {
    // WAL is what lets a search read while an ingest writes. NORMAL trades a
    // fsync per commit for one per checkpoint; the exposure is losing the last
    // transaction on power loss, and since a document's row and its vectors
    // are that one transaction, what is lost is a document looking un-indexed
    // — which the next scan fixes by re-indexing it.
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5_000)?;
    // 64 MiB of page cache and mmap. The index is read once at startup and
    // then mostly by primary key, so this is about making that startup read
    // sequential rather than about steady state.
    conn.pragma_update(None, "cache_size", -65_536)?;
    conn.pragma_update(None, "mmap_size", 268_435_456_i64)?;
    Ok(())
}

/// Vectors from one model are not comparable with another's, and mixing them
/// produces results that are wrong without ever looking wrong. Recorded on
/// first write and enforced on every open.
fn check_identity(conn: &Connection, model: Model) -> Result<()> {
    let stored: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = 'model'", [], |row| row.get(0))
        .optional()?;
    let dimension: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = 'dimension'", [], |row| row.get(0))
        .optional()?;

    match (stored, dimension) {
        (Some(name), Some(dim)) if name != model.name() || dim != model.dimension().to_string() => {
            bail!(
                "this index was built with model={name} dimension={dim}, but lum is configured \
                 for model={} dimension={}; re-index with `lum reindex`, or set \
                 LUM_EMBEDDING_MODEL back",
                model.name(),
                model.dimension()
            );
        }
        (Some(_), Some(_)) => Ok(()),
        _ => {
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('model', ?1), ('dimension', ?2)",
                params![model.name(), model.dimension().to_string()],
            )?;
            Ok(())
        }
    }
}

/// Rebuild the FTS index if it disagrees with the chunks table — an index
/// created before the FTS table existed, or one recovered from a crash
/// mid-write.
///
/// Detection is FTS5's own `integrity-check` command, which compares the
/// inverted index against the content table and reports SQLITE_CORRUPT on
/// any mismatch. Counting rows cannot do this job: on an external-content
/// table, `count(*)` is answered from the content table, so the two counts
/// agree by construction even when the index is empty. `rebuild` then
/// re-reads every row, so afterwards they agree in fact.
fn backfill_fts(conn: &Connection) -> Result<()> {
    let consistent = conn
        .execute("INSERT INTO chunks_fts(chunks_fts, rank) VALUES ('integrity-check', 1)", [])
        .is_ok();
    if !consistent {
        tracing::info!("keyword index disagrees with chunks; rebuilding");
        conn.execute("INSERT INTO chunks_fts(chunks_fts) VALUES ('rebuild')", [])
            .context("rebuilding the keyword index")?;
    }
    Ok(())
}

/// A user query as an FTS5 MATCH expression: terms OR-ed, each quoted.
///
/// OR rather than FTS5's default AND, because a search phrase is a
/// description, not a conjunction — "indexable file extensions" should rank a
/// chunk containing `EXTENSIONS` even though no chunk contains all three
/// words. BM25 already rewards matching more of them.
///
/// Quoting each term is what makes user input safe to pass to MATCH: FTS5
/// treats `-`, `.`, `NEAR`, and unbalanced quotes as syntax, and a query is
/// never syntax.
fn match_query(query: &str) -> Option<String> {
    let terms: Vec<String> = query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| format!("\"{t}\""))
        .collect();
    if terms.is_empty() {
        return None;
    }
    Some(terms.join(" OR "))
}

fn encode_vector(vector: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn decode_vector(bytes: &[u8]) -> Vec<f32> {
    bytes.chunks_exact(4).map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]])).collect()
}

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> (tempfile::TempDir, Db) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("lum.db"), Model::Standard).unwrap();
        (dir, db)
    }

    fn doc<'a>(source_id: &'a str, uri: &'a str, hash: &'a str) -> DocumentWrite<'a> {
        DocumentWrite {
            source_id,
            uri,
            path: uri.rsplit('/').next().unwrap_or(uri),
            mime: "text/x-rust",
            fingerprint: "1:1",
            content_hash: hash,
        }
    }

    fn chunks<'a>(texts: &'a [&'a str], vectors: &'a [Vec<f32>]) -> Vec<ChunkRow<'a>> {
        texts
            .iter()
            .zip(vectors)
            .enumerate()
            .map(|(i, (text, vector))| ChunkRow {
                index: i as u32,
                start_line: i as u32 + 1,
                end_line: i as u32 + 1,
                text,
                vector,
            })
            .collect()
    }

    #[test]
    fn vectors_survive_a_round_trip_exactly() {
        // Approximation belongs in the index's scan, not in storage.
        let original = vec![0.5f32, -0.25, 1.0, f32::MIN_POSITIVE];
        assert_eq!(decode_vector(&encode_vector(&original)), original);
    }

    #[test]
    fn adding_the_same_uri_twice_returns_the_same_source() {
        // `search --root` re-registers on every call; that must be free.
        let (_dir, db) = db();
        let (first, created) = db.add_source("/repo").unwrap();
        assert!(created);
        let (second, created) = db.add_source("/repo").unwrap();
        assert!(!created);
        assert_eq!(first.id, second.id);
    }

    #[test]
    fn deleting_a_source_takes_its_chunks_and_vectors_with_it() {
        // The invariant the old two-store design had to maintain by hand, and
        // could still lose after a hard crash.
        let (_dir, db) = db();
        let (source, _) = db.add_source("/repo").unwrap();
        let vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        db.upsert_document(
            doc(&source.id, "/repo/a.rs", "hash"),
            &chunks(&["one", "two"], &vectors),
        )
        .unwrap();
        assert_eq!(db.stats().unwrap().chunks, 2);

        db.delete_source(&source.id).unwrap();
        let stats = db.stats().unwrap();
        assert_eq!((stats.sources, stats.documents, stats.chunks), (0, 0, 0));
        assert!(db.all_vectors().unwrap().is_empty(), "vectors outlived their source");
    }

    #[test]
    fn a_shrinking_reingest_leaves_no_stale_tail() {
        let (_dir, db) = db();
        let (source, _) = db.add_source("/repo").unwrap();
        let three = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]];
        db.upsert_document(doc(&source.id, "/repo/a.rs", "v1"), &chunks(&["a", "b", "c"], &three))
            .unwrap();
        assert_eq!(db.stats().unwrap().chunks, 3);

        let one = vec![vec![1.0, 0.0]];
        db.upsert_document(doc(&source.id, "/repo/a.rs", "v2"), &chunks(&["only"], &one)).unwrap();
        assert_eq!(db.stats().unwrap().chunks, 1, "the previous tail survived");
        assert_eq!(db.stats().unwrap().documents, 1, "the document was duplicated");
    }

    #[test]
    fn two_sources_at_the_same_path_keep_separate_documents() {
        // Documents are keyed on (source_id, uri) rather than uri alone, so a
        // second source scanning the same path cannot adopt the first's rows
        // and silently misattribute their provenance.
        let (_dir, db) = db();
        let (a, _) = db.add_source("/a").unwrap();
        let (b, _) = db.add_source("/b").unwrap();
        let vectors = vec![vec![1.0, 0.0]];
        for source in [&a, &b] {
            db.upsert_document(doc(&source.id, "/shared/f.rs", "h"), &chunks(&["x"], &vectors))
                .unwrap();
        }
        assert_eq!(db.stats().unwrap().documents, 2);
        db.delete_source(&a.id).unwrap();
        assert_eq!(db.stats().unwrap().documents, 1, "deleting one source took the other's row");
    }

    #[test]
    fn a_mismatched_model_is_refused_rather_than_mixed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lum.db");
        drop(Db::open(&path, Model::Standard).unwrap());
        let error = match Db::open(&path, Model::Quantized) {
            Ok(_) => panic!("an index built with another model must be refused, not mixed"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("re-index"), "{error}");
    }

    #[test]
    fn failures_count_attempts_and_clear_on_success() {
        let (_dir, db) = db();
        let (source, _) = db.add_source("/repo").unwrap();
        assert_eq!(db.record_failure(&source.id, "/repo/a.rs", "boom", false).unwrap(), 1);
        assert_eq!(db.record_failure(&source.id, "/repo/a.rs", "boom", false).unwrap(), 2);
        assert_eq!(db.failures().unwrap().len(), 1);
        db.clear_failure(&source.id, "/repo/a.rs").unwrap();
        assert!(db.failures().unwrap().is_empty());
    }

    #[test]
    fn match_query_quotes_terms_and_ors_them() {
        assert_eq!(match_query("retry backoff"), Some("\"retry\" OR \"backoff\"".to_owned()));
        // Punctuation is FTS5 syntax; a query is never syntax. Splitting on
        // non-alphanumerics also splits snake_case the way the tokenizer does.
        assert_eq!(
            match_query("what's chunks_fts.rowid - NEAR?"),
            Some("\"what\" OR \"s\" OR \"chunks\" OR \"fts\" OR \"rowid\" OR \"NEAR\"".to_owned())
        );
        assert_eq!(match_query("  …—  "), None, "no searchable terms means no query");
    }

    #[test]
    fn keyword_search_finds_the_exact_identifier_the_embedding_misses() {
        let (_dir, db) = db();
        let (source, _) = db.add_source("/repo").unwrap();
        let vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0]];
        db.upsert_document(
            doc(&source.id, "/repo/mime.rs", "h1"),
            &chunks(
                &["const EXTENSIONS: &[(&str, &str)] = &[(\"txt\", ..)]", "fn for_path()"],
                &vectors,
            ),
        )
        .unwrap();
        db.upsert_document(
            doc(&source.id, "/repo/other.rs", "h2"),
            &chunks(&["fn unrelated() {}", "struct Nothing;"], &vectors),
        )
        .unwrap();

        let hits = db.keyword_search("indexable file extensions", 10, None).unwrap();
        assert!(!hits.is_empty(), "the literal EXTENSIONS table must match");
        let payloads = db.chunk_payloads(&hits).unwrap();
        assert!(
            payloads.iter().any(|p| p.text.contains("EXTENSIONS")),
            "expected the chunk containing the identifier"
        );
    }

    #[test]
    fn keyword_search_respects_the_source_filter() {
        let (_dir, db) = db();
        let (a, _) = db.add_source("/a").unwrap();
        let (b, _) = db.add_source("/b").unwrap();
        let vectors = vec![vec![1.0, 0.0]];
        db.upsert_document(doc(&a.id, "/a/f.rs", "h"), &chunks(&["needle in a"], &vectors))
            .unwrap();
        db.upsert_document(doc(&b.id, "/b/f.rs", "h"), &chunks(&["needle in b"], &vectors))
            .unwrap();
        let hits = db.keyword_search("needle", 10, Some(&a.id)).unwrap();
        let payloads = db.chunk_payloads(&hits).unwrap();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].source_id, a.id);
    }

    #[test]
    fn the_fts_index_follows_every_path_that_deletes_chunks() {
        // The design leans on triggers firing for *cascaded* deletes — a
        // source removal never touches chunks directly. If SQLite did not
        // fire the delete trigger on cascade, keyword search would keep
        // returning ghosts of deleted sources forever.
        let (_dir, db) = db();
        let (source, _) = db.add_source("/repo").unwrap();
        let vectors = vec![vec![1.0, 0.0]];
        db.upsert_document(doc(&source.id, "/repo/a.rs", "h"), &chunks(&["ghostword"], &vectors))
            .unwrap();
        assert_eq!(db.keyword_search("ghostword", 10, None).unwrap().len(), 1);

        // Path 1: shrinking re-ingest (explicit DELETE inside the upsert).
        db.upsert_document(doc(&source.id, "/repo/a.rs", "h2"), &chunks(&["replaced"], &vectors))
            .unwrap();
        assert!(db.keyword_search("ghostword", 10, None).unwrap().is_empty());
        assert_eq!(db.keyword_search("replaced", 10, None).unwrap().len(), 1);

        // Path 2: document delete.
        db.delete_document(&source.id, "/repo/a.rs").unwrap();
        assert!(db.keyword_search("replaced", 10, None).unwrap().is_empty());

        // Path 3: source delete — the cascade.
        db.upsert_document(doc(&source.id, "/repo/b.rs", "h"), &chunks(&["cascaded"], &vectors))
            .unwrap();
        db.delete_source(&source.id).unwrap();
        assert!(
            db.keyword_search("cascaded", 10, None).unwrap().is_empty(),
            "a cascaded delete must remove chunks from the keyword index too"
        );
    }

    #[test]
    fn an_index_created_before_fts_existed_is_backfilled_on_open() {
        // Simulates upgrading: rows in chunks the triggers never saw. The
        // count mismatch is detected on open and the index rebuilt.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lum.db");
        {
            let db = Db::open(&path, Model::Standard).unwrap();
            let (source, _) = db.add_source("/repo").unwrap();
            let vectors = vec![vec![1.0, 0.0]];
            db.upsert_document(
                doc(&source.id, "/repo/a.rs", "h"),
                &chunks(&["backfillword"], &vectors),
            )
            .unwrap();
            // Wipe the FTS side only, as if it had never existed.
            let conn = db.write.lock().unwrap();
            conn.execute("INSERT INTO chunks_fts(chunks_fts) VALUES ('delete-all')", []).unwrap();
            drop(conn);
            assert!(db.keyword_search("backfillword", 10, None).unwrap().is_empty());
        }
        let db = Db::open(&path, Model::Standard).unwrap();
        assert_eq!(
            db.keyword_search("backfillword", 10, None).unwrap().len(),
            1,
            "reopening must notice the mismatch and rebuild"
        );
    }

    #[test]
    fn permanent_failures_are_listed_separately_from_retryable_ones() {
        let (_dir, db) = db();
        let (source, _) = db.add_source("/repo").unwrap();
        db.record_failure(&source.id, "/repo/a.pdf", "no parser", true).unwrap();
        db.record_failure(&source.id, "/repo/b.rs", "disk", false).unwrap();
        assert_eq!(db.permanent_failures(&source.id).unwrap(), vec!["/repo/a.pdf"]);
        assert_eq!(db.failures().unwrap().len(), 2, "both stay visible in status");
    }
}
