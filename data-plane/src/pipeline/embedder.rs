//! Embedders: text → dense vectors.
//!
//! lum runs inference locally via [fastembed](https://github.com/Anush008/fastembed-rs)
//! (ONNX Runtime under the hood). The model is downloaded once into
//! `<data-dir>/models` on first run; afterwards everything works offline.

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};

/// Text-to-vector interface.
///
/// Passages (document chunks) and queries are separate methods because
/// many retrieval models — including our default — are trained with
/// asymmetric prefixes: the query "how do I X" and a passage answering
/// it should land near each other, not queries near queries.
pub trait Embedder: Send + Sync {
    /// Embed document chunks (batched).
    fn embed_passages(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    /// Embed a search query.
    fn embed_query(&self, query: &str) -> Result<Vec<f32>>;

    /// Output vector dimension; the vector store's index is created with
    /// this size, so it must be constant for the lifetime of an index.
    fn dimension(&self) -> usize;

    /// Human-readable model identifier (reported via Health).
    fn model_name(&self) -> &'static str;
}

/// Default embedder: BAAI/bge-small-en-v1.5 — small (~70 MB), fast on
/// CPU, and solidly mid-pack on retrieval benchmarks. A good trade for a
/// local-first tool. Swapping models means changing this file only, but
/// note: a new model (or dimension) requires re-ingesting everything,
/// because old and new vectors aren't comparable.
pub struct FastEmbedder {
    /// fastembed's `embed` takes `&mut self` (it reuses internal
    /// buffers), so we serialize access with a Mutex. Embedding is
    /// CPU-bound anyway; parallel calls would fight over cores, not
    /// help. Requests already run on the blocking thread pool.
    model: Mutex<TextEmbedding>,
}

impl FastEmbedder {
    pub fn initialize(cache_dir: &Path) -> Result<Self> {
        let model = TextEmbedding::try_new(
            TextInitOptions::new(EmbeddingModel::BGESmallENV15)
                .with_cache_dir(cache_dir.to_path_buf())
                .with_show_download_progress(true),
        )
        .context("initializing embedding model (first run downloads ~70 MB)")?;
        Ok(Self {
            model: Mutex::new(model),
        })
    }
}

impl Embedder for FastEmbedder {
    fn embed_passages(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        // bge models are trained with "passage: "/"query: " prefixes;
        // using them measurably improves retrieval quality.
        let prefixed: Vec<String> = texts.iter().map(|t| format!("passage: {t}")).collect();
        let mut model = self.model.lock().expect("embedder mutex poisoned");
        let embeddings = model.embed(prefixed, None)?;
        Ok(embeddings)
    }

    fn embed_query(&self, query: &str) -> Result<Vec<f32>> {
        let mut model = self.model.lock().expect("embedder mutex poisoned");
        let mut embeddings = model.embed(vec![format!("query: {query}")], None)?;
        embeddings
            .pop()
            .context("embedding model returned no vector for query")
    }

    fn dimension(&self) -> usize {
        384 // fixed output size of bge-small-en-v1.5
    }

    fn model_name(&self) -> &'static str {
        "BAAI/bge-small-en-v1.5"
    }
}
