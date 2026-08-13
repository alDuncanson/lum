//! What a file is, and whether it is a test.
//!
//! Both are decided from the path alone, deliberately. Sniffing content would
//! mean reading every file in the tree on every scan, which is exactly the
//! cost a scan exists to avoid.

use std::path::Path;

/// Extensions lum indexes, and the MIME type each maps to.
///
/// Adding a format is one line here, plus a `Parser` in `parse.rs` if the
/// MIME type is new. A grammar in `language.rs` is optional: a file with no
/// grammar chunks by word window rather than going unindexed.
const EXTENSIONS: &[(&str, &str)] = &[
    ("txt", "text/plain"),
    ("text", "text/plain"),
    ("md", "text/markdown"),
    ("markdown", "text/markdown"),
    ("go", "text/x-go"),
    ("rs", "text/x-rust"),
    ("lua", "text/x-lua"),
    ("nix", "text/x-nix"),
    ("py", "text/x-python"),
    ("pyi", "text/x-python"),
    ("js", "text/javascript"),
    ("mjs", "text/javascript"),
    ("cjs", "text/javascript"),
    ("jsx", "text/jsx"),
    ("ts", "text/typescript"),
    ("mts", "text/typescript"),
    ("cts", "text/typescript"),
    ("tsx", "text/tsx"),
    ("java", "text/x-java-source"),
    ("kt", "text/x-kotlin"),
    ("kts", "text/x-kotlin"),
    ("c", "text/x-c"),
    ("h", "text/x-c"),
    ("cc", "text/x-c++"),
    ("cpp", "text/x-c++"),
    ("cxx", "text/x-c++"),
    ("hh", "text/x-c++"),
    ("hpp", "text/x-c++"),
    ("hxx", "text/x-c++"),
    ("cs", "text/x-csharp"),
    ("rb", "text/x-ruby"),
    ("php", "text/x-php"),
    ("swift", "text/x-swift"),
    ("scala", "text/x-scala"),
    ("sc", "text/x-scala"),
    ("sh", "text/x-shellscript"),
    ("bash", "text/x-shellscript"),
    ("zsh", "text/x-shellscript"),
    ("fish", "text/x-shellscript"),
    ("sql", "text/x-sql"),
    ("yaml", "text/yaml"),
    ("yml", "text/yaml"),
    ("toml", "text/x-toml"),
    ("json", "text/json"),
    ("jsonc", "text/json"),
    ("html", "text/html"),
    ("htm", "text/html"),
    ("css", "text/css"),
    ("scss", "text/x-scss"),
    ("sass", "text/x-sass"),
    ("less", "text/x-less"),
    ("proto", "text/x-protobuf"),
    ("xml", "text/xml"),
    ("svg", "text/xml"),
];

/// The MIME type for a path, or `None` if lum does not index it.
pub fn for_path(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    EXTENSIONS.iter().find(|(candidate, _)| *candidate == extension).map(|(_, mime)| *mime)
}

/// Naming conventions, not a language feature.
///
/// Rust deliberately has no entry: its tests live in a `#[cfg(test)] mod
/// tests` inside the file they test, so there is no path to recognize and
/// excluding `src/index.rs` would exclude the index.
const TEST_SUFFIXES: &[&str] = &[
    "_test.go",
    "_test.py",
    "_test.rs",
    "_test.ts",
    "_test.js",
    ".test.ts",
    ".test.tsx",
    ".test.js",
    ".test.jsx",
    ".spec.ts",
    ".spec.tsx",
    ".spec.js",
    ".spec.jsx",
    "_spec.rb",
    "_spec.lua",
    "_test.exs",
];

const TEST_SEGMENTS: &[&str] = &["/test/", "/tests/", "/__tests__/", "/spec/"];

/// Whether a path says it holds tests.
///
/// Tests describe the feature they exercise, repeatedly, in prose-like
/// assertion names and short focused functions — close to a description of
/// what scores well in an embedding search. So they outrank implementations.
///
/// Down-weighting them is the obvious fix and it is wrong: scaling test
/// scores by 0.95, 0.9, 0.8 and 0 made every retrieval metric monotonically
/// worse once the fixture contained queries that were *looking* for a test,
/// which people do. What survives is the preference, not the prior —
/// `exclude_tests` drops them entirely, off by default. There is no partial
/// setting because there is no evidence any partial setting is good.
pub fn is_test_path(uri: &str) -> bool {
    let lower = uri.to_ascii_lowercase();
    if TEST_SUFFIXES.iter().any(|suffix| lower.ends_with(suffix)) {
        return true;
    }
    let base = lower.rsplit('/').next().unwrap_or(&lower);
    if base.starts_with("test_") {
        return true;
    }
    TEST_SEGMENTS.iter().any(|segment| lower.contains(segment))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn extensions_map_case_insensitively() {
        assert_eq!(for_path(&PathBuf::from("a/b.rs")), Some("text/x-rust"));
        assert_eq!(for_path(&PathBuf::from("a/B.MD")), Some("text/markdown"));
        assert_eq!(for_path(&PathBuf::from("a/b.png")), None);
        assert_eq!(for_path(&PathBuf::from("Makefile")), None);
    }

    #[test]
    fn test_paths_are_recognized_by_convention() {
        for path in [
            "/repo/internal/api/server_test.go",
            "/repo/src/thing.test.ts",
            "/repo/tests/integration.py",
            "/repo/spec/models_spec.rb",
            "/repo/pkg/test_helpers.py",
        ] {
            assert!(is_test_path(path), "{path}");
        }
    }

    #[test]
    fn rust_inline_tests_do_not_make_their_file_a_test_file() {
        // The whole reason there is no "_test.rs"-style rule for Rust source:
        // excluding these would exclude the implementation too.
        assert!(!is_test_path("/repo/src/index.rs"));
        assert!(!is_test_path("/repo/src/store/edge.rs"));
    }

    #[test]
    fn substrings_do_not_count_as_segments() {
        // "latest/" contains "test" but is not a test directory, and
        // "contest.go" is not "_test.go".
        assert!(!is_test_path("/repo/latest/thing.go"));
        assert!(!is_test_path("/repo/pkg/contest.go"));
    }
}
