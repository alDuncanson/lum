//! Embedders: text → dense vectors.
//!
//! lum runs inference locally via [fastembed](https://github.com/Anush008/fastembed-rs)
//! (ONNX Runtime under the hood). The model is downloaded once into
//! `<data-dir>/models` on first run; afterwards everything works offline.

use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::ValueEnum;
use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};

/// Bounds one passage inference call so a waiting interactive query gets
/// an opportunity to acquire the model between bulk-ingest batches.
pub const PASSAGE_BATCH_SIZE: usize = 64;

/// Supported bge-small variants. The quantized model keeps the same
/// dimensions but produces different vectors, so each has a distinct
/// manifest identity.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum EmbeddingModelChoice {
    #[default]
    Standard,
    Quantized,
}

impl EmbeddingModelChoice {
    fn fastembed_model(self) -> EmbeddingModel {
        match self {
            Self::Standard => EmbeddingModel::BGESmallENV15,
            Self::Quantized => EmbeddingModel::BGESmallENV15Q,
        }
    }

    fn model_name(self) -> &'static str {
        match self {
            Self::Standard => "BAAI/bge-small-en-v1.5",
            Self::Quantized => "Qdrant/bge-small-en-v1.5-onnx-Q",
        }
    }
}

/// Text-to-vector interface.
///
/// Passages (document chunks) and queries are separate methods because
/// many retrieval models — including our default — are trained with
/// asymmetric prefixes: the query "how do I X" and a passage answering
/// it should land near each other, not queries near queries.
pub trait Embedder: Send + Sync {
    /// Embed document chunks (batched), reporting cumulative completion after each
    /// internal batch.
    ///
    /// The callback exists because this is the slow step and it is
    /// otherwise silent: one call can spend a minute inside the model with
    /// nothing observable happening. Reporting per batch is the finest
    /// granularity available — a single `model.embed` is atomic — and it is
    /// a good unit anyway, since chunks are far more uniform in cost than
    /// documents.
    fn embed_passages_with_progress(
        &self,
        texts: &[String],
        on_progress: &(dyn Fn(usize) + Sync),
    ) -> Result<Vec<Vec<f32>>>;

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
    model_name: &'static str,
}

impl FastEmbedder {
    pub fn initialize(cache_dir: &Path, choice: EmbeddingModelChoice) -> Result<Self> {
        let model = TextEmbedding::try_new(
            TextInitOptions::new(choice.fastembed_model())
                .with_cache_dir(cache_dir.to_path_buf())
                .with_show_download_progress(true),
        )
        .with_context(|| format!("initializing embedding model {}", choice.model_name()))?;
        Ok(Self {
            model: Mutex::new(model),
            model_name: choice.model_name(),
        })
    }
}

impl Embedder for FastEmbedder {
    fn embed_passages_with_progress(
        &self,
        texts: &[String],
        on_progress: &(dyn Fn(usize) + Sync),
    ) -> Result<Vec<Vec<f32>>> {
        let mut embeddings = Vec::with_capacity(texts.len());
        for batch in texts.chunks(PASSAGE_BATCH_SIZE) {
            // bge models are trained with "passage: "/"query: " prefixes;
            // using them measurably improves retrieval quality.
            let prefixed: Vec<String> = batch
                .iter()
                .map(|text| format!("passage: {text}"))
                .collect();
            let waiting = Instant::now();
            let mut model = self.model.lock().expect("embedder mutex poisoned");
            let lock_wait = waiting.elapsed();
            let started = Instant::now();
            let batch_embeddings = model.embed(prefixed, Some(batch.len()))?;
            let embed_duration = started.elapsed();
            drop(model); // let queries interleave before the next sub-batch
            tracing::info!(
                batch_size = batch.len(),
                embed_lock_wait_ms = lock_wait.as_secs_f64() * 1_000.0,
                embed_ms = embed_duration.as_secs_f64() * 1_000.0,
                "embedding passage batch complete"
            );
            embeddings.extend(batch_embeddings);
            on_progress(embeddings.len());
        }
        Ok(embeddings)
    }

    fn embed_query(&self, query: &str) -> Result<Vec<f32>> {
        let waiting = Instant::now();
        let mut model = self.model.lock().expect("embedder mutex poisoned");
        let lock_wait = waiting.elapsed();
        let started = Instant::now();
        let mut embeddings = model.embed(vec![format!("query: {query}")], Some(1))?;
        let embed_duration = started.elapsed();
        tracing::info!(
            embed_lock_wait_ms = lock_wait.as_secs_f64() * 1_000.0,
            embed_ms = embed_duration.as_secs_f64() * 1_000.0,
            "embedding query complete"
        );
        embeddings
            .pop()
            .context("embedding model returned no vector for query")
    }

    fn dimension(&self) -> usize {
        384 // fixed output size of bge-small-en-v1.5
    }

    fn model_name(&self) -> &'static str {
        self.model_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_choices_have_distinct_manifest_identities() {
        assert_eq!(
            EmbeddingModelChoice::Standard.fastembed_model(),
            EmbeddingModel::BGESmallENV15
        );
        assert_eq!(
            EmbeddingModelChoice::Quantized.fastembed_model(),
            EmbeddingModel::BGESmallENV15Q
        );
        assert_ne!(
            EmbeddingModelChoice::Standard.model_name(),
            EmbeddingModelChoice::Quantized.model_name()
        );
    }
}
