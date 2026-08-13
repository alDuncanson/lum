//! Live change detection.
//!
//! `notify` watches recursively on every platform lum supports, which deletes
//! the tree-of-watches bookkeeping the previous build needed: adding each new
//! subdirectory as it appeared, removing whole subtrees on delete, re-adding
//! everything when a `.gitignore` changed, and a special case for renames
//! being unreliable on one platform.
//!
//! Watching is an optimization, never the source of truth. A scan is
//! authoritative and cheap when nothing changed, so a watch that fails or
//! misses an event costs latency until the fallback ticker fires, not
//! correctness. That is what makes it safe for this filter to be approximate.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as _};

use crate::mime;

/// Dropping this stops the watch.
pub struct Watch {
    _watcher: RecommendedWatcher,
}

/// Watch `root`, calling `on_change` for events that could affect the index.
///
/// `on_change` runs on the watcher's own thread and must be cheap and
/// non-blocking — in practice a `try_send` onto a channel the caller debounces.
pub fn start(
    root: &Path,
    exclude: HashSet<String>,
    on_change: impl Fn() + Send + 'static,
) -> Result<Watch> {
    let root = root.to_path_buf();
    let filter_root = root.clone();

    let mut watcher = notify::recommended_watcher(move |result: notify::Result<Event>| {
        let Ok(event) = result else {
            // A watch error means the OS stopped telling us things. The
            // fallback rescan is what covers it; the caller learns by not
            // hearing from us, which is exactly what a ticker is for.
            return;
        };
        if !matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_))
        {
            return;
        }
        if event.paths.iter().any(|path| relevant(path, &filter_root, &exclude)) {
            on_change();
        }
    })
    .context("creating a filesystem watcher")?;

    watcher
        .watch(&root, RecursiveMode::Recursive)
        .with_context(|| format!("watching {}", root.display()))?;
    Ok(Watch { _watcher: watcher })
}

/// Whether a changed path could change the index.
///
/// Errs toward relevant. A false positive costs one scan that finds nothing
/// changed — which is the cheap path — while a false negative leaves the index
/// stale until the fallback ticker.
fn relevant(path: &Path, root: &Path, exclude: &HashSet<String>) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    // Skipped for the same reason scans skip them: the two must agree, or a
    // watch produces events that reconcile to nothing on every save.
    for component in relative.components() {
        let Some(name) = component.as_os_str().to_str() else {
            continue;
        };
        if name.starts_with('.') && name != ".gitignore" {
            return false;
        }
        if exclude.contains(name) {
            return false;
        }
    }
    if path.file_name().is_some_and(|name| name == ".gitignore") {
        return true;
    }
    // A path with no extension is usually a directory being created or
    // removed, which changes what a scan would find.
    mime::for_path(path).is_some() || path.extension().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn exclude() -> HashSet<String> {
        ["node_modules", "target"].iter().map(|s| (*s).to_owned()).collect()
    }

    fn check(relative: &str) -> bool {
        let root = PathBuf::from("/repo");
        relevant(&root.join(relative), &root, &exclude())
    }

    #[test]
    fn source_files_are_relevant() {
        assert!(check("src/main.rs"));
        assert!(check("docs/architecture.md"));
    }

    #[test]
    fn unindexable_files_are_not() {
        assert!(!check("assets/logo.png"));
        assert!(!check("LICENSE.bin"));
    }

    #[test]
    fn gitignore_changes_are_relevant_despite_the_leading_dot() {
        // Editing it changes which files a scan would find, so it has to wake
        // one — and it is the one dotfile that must survive the hidden filter.
        assert!(check(".gitignore"));
        assert!(check("src/.gitignore"));
    }

    #[test]
    fn hidden_and_excluded_trees_are_ignored() {
        // Git writes constantly during ordinary work. Waking a scan on every
        // one of those writes would mean a rescan per command.
        assert!(!check(".git/index"));
        assert!(!check(".git/refs/heads/main.rs"));
        assert!(!check("node_modules/dep/index.js"));
        assert!(!check("target/debug/thing.rs"));
    }

    #[test]
    fn extensionless_paths_are_treated_as_directories_and_kept() {
        // A new directory changes what a scan finds, and the event that
        // announces it names the directory, not the files inside it.
        assert!(check("src/newmodule"));
    }

    #[test]
    fn paths_outside_the_root_are_ignored() {
        let root = PathBuf::from("/repo");
        assert!(!relevant(Path::new("/elsewhere/a.rs"), &root, &exclude()));
    }
}
