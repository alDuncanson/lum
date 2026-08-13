//! Walking a repository and deciding what changed.
//!
//! Scans are frequent — at startup, after every debounced watch event, on a
//! fallback ticker, and on every retry — so the common case is deliberately
//! stat-only. A scan reports a size+mtime fingerprint and leaves the content
//! hash empty; ingest reads and hashes only the files whose fingerprint moved.
//! Hashing the whole tree on every scan would make watching a large repository
//! cost a full read of it per keystroke-triggered save.
//!
//! Gitignore handling is the `ignore` crate's, which is ripgrep's walker:
//! per-directory `.gitignore` files, nested rules, negations, global excludes,
//! and `.git/info/exclude`, all already correct. It replaced a hand-rolled
//! matcher and a hand-rolled recursive walk.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use ignore::WalkBuilder;

use crate::mime;

/// How recently a file must have been written for its fingerprint to be
/// considered untrustworthy.
///
/// The hole in a size+mtime fingerprint is what git calls "racily clean": a
/// file modified again inside the same mtime tick we observed it in, with no
/// change in size, has an identical fingerprint and its edit is invisible.
/// Some filesystems keep only whole-second mtimes, which makes the window wide
/// enough to hit in practice — an editor writing twice in a second, or a
/// scripted in-place substitution that preserves length.
///
/// So anything written this recently is hashed now, and the fingerprint is
/// trusted only for files that have been quiet longer than any plausible mtime
/// granularity. The cost is bounded by how many files changed in the last
/// couple of seconds, which is approximately zero on every scan except the one
/// right after an edit.
const RACE_WINDOW: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct FileRef {
    /// Absolute path.
    pub uri: String,
    /// Path relative to the source root: what a person calls the file, and
    /// what they are likely to type part of when searching for it. The
    /// absolute path is deliberately not embedded — `/Users/<name>/code` adds
    /// tokens identical for every document that describe nobody's query.
    pub path: String,
    pub mime: &'static str,
    pub fingerprint: String,
    /// Present only when the fingerprint could not be trusted.
    pub content_hash: Option<String>,
}

/// Walk `root`, returning a ref for every indexable file.
pub fn scan(root: &Path, exclude: &HashSet<String>) -> Result<Vec<FileRef>> {
    let root = root.to_path_buf();
    let excluded = exclude.clone();
    let filter_root = root.clone();

    let walker = WalkBuilder::new(&root)
        // Hidden trees (.git, .obsidian) are never indexed, and the same
        // decision has to be made by the watcher or the two disagree: a
        // directory watched but not scanned produces events that reconcile to
        // nothing, and one scanned but not watched goes stale.
        .hidden(true)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .parents(true)
        .require_git(false)
        .follow_links(false)
        .filter_entry(move |entry| {
            // The root itself is never excluded: `lum add ~/code/vendor`
            // should index that directory, and the name only means "generated"
            // relative to a repository containing it.
            if entry.path() == filter_root {
                return true;
            }
            if !entry.file_type().is_some_and(|t| t.is_dir()) {
                return true;
            }
            !entry.file_name().to_str().is_some_and(|name| excluded.contains(name))
        })
        .build();

    let mut refs = Vec::new();
    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                // One unreadable path must not hide every document — but it
                // should be diagnosable rather than an invisible gap.
                tracing::warn!(%error, "skipping unreadable path during scan");
                continue;
            }
        };
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        let path = entry.path();
        let Some(mime) = mime::for_path(path) else {
            continue;
        };
        // Racing deletes are normal during a scan; skip rather than fail.
        let Ok(metadata) = entry.metadata() else {
            continue;
        };

        let modified = metadata.modified().ok();
        let content_hash = match modified {
            Some(time)
                if SystemTime::now().duration_since(time).unwrap_or_default() < RACE_WINDOW =>
            {
                hash_file(path).ok()
            }
            _ => None,
        };

        refs.push(FileRef {
            uri: path.to_string_lossy().into_owned(),
            path: path.strip_prefix(&root).unwrap_or(path).to_string_lossy().into_owned(),
            mime,
            fingerprint: fingerprint(metadata.len(), modified),
            content_hash,
        });
    }
    Ok(refs)
}

/// Size plus mtime in nanoseconds. Both come from the stat the walk already
/// performed, so producing it costs nothing beyond formatting.
///
/// Size is included because mtime alone is the weaker signal: a write landing
/// in the same mtime tick is common, whereas one that also preserves length is
/// much less so.
fn fingerprint(size: u64, modified: Option<SystemTime>) -> String {
    let nanos =
        modified.and_then(|time| time.duration_since(UNIX_EPOCH).ok()).map_or(0, |d| d.as_nanos());
    format!("{size}:{nanos}")
}

/// BLAKE3 rather than SHA-256: this is a change detector, not a signature, and
/// it runs over every changed file on every scan.
pub fn hash_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(hash_bytes(&bytes))
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(root: &Path, relative: &str, contents: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, contents).unwrap();
    }

    fn paths(refs: &[FileRef]) -> Vec<String> {
        let mut out: Vec<String> = refs.iter().map(|r| r.path.clone()).collect();
        out.sort();
        out
    }

    #[test]
    fn only_indexable_extensions_are_returned() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.rs", "fn main() {}");
        write(dir.path(), "b.md", "# hi");
        write(dir.path(), "c.png", "not text");
        write(dir.path(), "Makefile", "all:");
        let refs = scan(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(paths(&refs), vec!["a.rs", "b.md"]);
    }

    #[test]
    fn gitignore_is_honored_including_nested_files_and_negations() {
        let dir = tempfile::tempdir().unwrap();
        // require_git(false) means the rules apply without an actual repo.
        write(dir.path(), ".gitignore", "generated/\n*.tmp.rs\n!keep.tmp.rs\n");
        write(dir.path(), "src/main.rs", "fn main() {}");
        write(dir.path(), "generated/big.rs", "// generated");
        write(dir.path(), "src/scratch.tmp.rs", "// scratch");
        write(dir.path(), "src/keep.tmp.rs", "// keep");
        write(dir.path(), "src/.gitignore", "deeper.rs\n");
        write(dir.path(), "src/deeper.rs", "// deeper");

        assert_eq!(
            paths(&scan(dir.path(), &HashSet::new()).unwrap()),
            vec!["src/keep.tmp.rs", "src/main.rs"]
        );
    }

    #[test]
    fn hidden_and_excluded_directories_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "keep.rs", "");
        write(dir.path(), ".git/config.rs", "");
        write(dir.path(), ".obsidian/notes.md", "");
        write(dir.path(), "node_modules/dep/index.js", "");
        write(dir.path(), "target/debug/build.rs", "");
        let exclude: HashSet<String> =
            ["node_modules", "target"].iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(paths(&scan(dir.path(), &exclude).unwrap()), vec!["keep.rs"]);
    }

    #[test]
    fn the_root_is_indexed_even_when_its_name_is_excluded() {
        // `lum add ~/code/vendor` means index that directory. The name only
        // means "generated" relative to a repository containing it.
        let dir = tempfile::tempdir().unwrap();
        let vendor = dir.path().join("vendor");
        fs::create_dir_all(&vendor).unwrap();
        write(&vendor, "lib.rs", "pub fn f() {}");
        let exclude: HashSet<String> = ["vendor"].iter().map(|s| (*s).to_owned()).collect();
        assert_eq!(paths(&scan(&vendor, &exclude).unwrap()), vec!["lib.rs"]);
    }

    #[test]
    fn display_paths_are_relative_to_the_root() {
        // These are embedded with every chunk, so an absolute path here would
        // spend tokens on directories identical for every document.
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "deep/nested/file.rs", "fn f() {}");
        let refs = scan(dir.path(), &HashSet::new()).unwrap();
        assert_eq!(refs[0].path, "deep/nested/file.rs");
        assert!(refs[0].uri.ends_with("deep/nested/file.rs"));
        assert!(refs[0].uri.starts_with('/'));
    }

    #[test]
    fn a_freshly_written_file_is_hashed_rather_than_trusted() {
        // The racily-clean window: this file was written microseconds ago, so
        // its fingerprint cannot be trusted and the scan pays for the read.
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "new.rs", "fn f() {}");
        let refs = scan(dir.path(), &HashSet::new()).unwrap();
        assert!(refs[0].content_hash.is_some(), "a just-written file must be hashed");
    }

    #[test]
    fn fingerprints_change_with_size_and_with_mtime() {
        let epoch = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_ne!(fingerprint(10, Some(epoch)), fingerprint(11, Some(epoch)));
        assert_ne!(
            fingerprint(10, Some(epoch)),
            fingerprint(10, Some(epoch + Duration::from_nanos(1)))
        );
    }
}
