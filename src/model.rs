//! Getting the model onto disk.
//!
//! Two files — the ONNX graph and its tokenizer — cached under
//! `<data-dir>/models` in HuggingFace's layout, so a cache populated by an
//! earlier lum release (or any other hf-hub tool) is reused rather than
//! re-downloaded.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use hf_hub::api::sync::ApiBuilder;

use crate::config::Model;

pub struct ModelFiles {
    pub onnx: PathBuf,
    pub tokenizer: PathBuf,
}

/// Whether every file is already cached.
///
/// Checked before resolving so the daemon can announce `downloading-model`
/// only when it is actually about to download ~130 MB, rather than on every
/// start. The distinction matters: that state is what the CLI spinner and the
/// Neovim progress bridge use to explain a wait that would otherwise look
/// like a hang.
pub fn is_cached(cache_dir: &Path, model: Model) -> bool {
    let (repo, onnx) = model.repo();
    let root = cache_dir.join(format!("models--{}", repo.replace('/', "--")));
    let Ok(refs) = std::fs::read_to_string(root.join("refs/main")) else {
        return false;
    };
    let snapshot = root.join("snapshots").join(refs.trim());
    snapshot.join(onnx).exists() && snapshot.join("tokenizer.json").exists()
}

/// Resolve both files, downloading whatever is missing.
///
/// Blocking, and deliberately so: it runs once, on a blocking thread, before
/// the daemon reports itself ready.
pub fn resolve(cache_dir: &Path, model: Model) -> Result<ModelFiles> {
    std::fs::create_dir_all(cache_dir)
        .with_context(|| format!("creating model cache {}", cache_dir.display()))?;
    let (repo_id, onnx_file) = model.repo();

    // Progress is reported through the event bus as a state, not by hf-hub
    // drawing its own bar: the daemon's stderr is a log file, and a progress
    // bar in a log file is neither progress nor a log.
    let api = ApiBuilder::new()
        .with_cache_dir(cache_dir.to_path_buf())
        .with_progress(false)
        .build()
        .context("initializing the model downloader")?;
    let repo = api.model(repo_id.to_owned());

    let onnx = repo.get(onnx_file).with_context(|| {
        format!("downloading {repo_id}/{onnx_file}; lum needs network access on first run")
    })?;
    let tokenizer = repo
        .get("tokenizer.json")
        .with_context(|| format!("downloading {repo_id}/tokenizer.json"))?;

    Ok(ModelFiles { onnx, tokenizer })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_cache_is_not_mistaken_for_a_populated_one() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_cached(dir.path(), Model::Standard));
    }

    #[test]
    fn a_dangling_ref_without_its_snapshot_is_not_cached() {
        // A download interrupted between writing refs/main and completing the
        // blob would otherwise look complete and fail later, at session load,
        // with a confusing ONNX parse error instead of a re-download.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("models--Xenova--bge-small-en-v1.5");
        std::fs::create_dir_all(root.join("refs")).unwrap();
        std::fs::write(root.join("refs/main"), "deadbeef").unwrap();
        assert!(!is_cached(dir.path(), Model::Standard));
    }
}
