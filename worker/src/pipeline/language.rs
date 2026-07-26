//! Which grammar, if any, applies to a document.
//!
//! The dispatcher already decides what a file is — it maps extensions to
//! MIME types in `source/localdir.go` — so language detection here is a
//! lookup, not a guess. A MIME type with no grammar is not an error: those
//! documents chunk by word window, exactly as they did before.
//!
//! Adding a language is two lines here plus a dependency. The grammars are
//! chosen by what this repository and its neighbours are written in; the
//! list is meant to grow.

use tree_sitter::Language as Grammar;

/// A language lum can parse into a syntax tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Language {
    Go,
    Rust,
    Python,
    Nix,
    Lua,
}

impl Language {
    /// The language for a MIME type, or `None` when lum has no grammar for
    /// it. `None` is the common case — markdown, YAML, JSON and plain text
    /// all land here — and means "chunk it as prose".
    pub fn from_mime(mime_type: &str) -> Option<Self> {
        // Strip any `; charset=` parameter: the dispatcher does not send
        // one today, but a MIME type is allowed to carry it.
        let base = mime_type
            .split(';')
            .next()
            .unwrap_or(mime_type)
            .trim()
            .to_ascii_lowercase();
        Some(match base.as_str() {
            "text/x-go" => Self::Go,
            "text/x-rust" => Self::Rust,
            "text/x-python" => Self::Python,
            "text/x-nix" => Self::Nix,
            "text/x-lua" => Self::Lua,
            _ => return None,
        })
    }

    /// The compiled tree-sitter grammar.
    pub fn grammar(self) -> Grammar {
        match self {
            Self::Go => tree_sitter_go::LANGUAGE,
            Self::Rust => tree_sitter_rust::LANGUAGE,
            Self::Python => tree_sitter_python::LANGUAGE,
            Self::Nix => tree_sitter_nix::LANGUAGE,
            Self::Lua => tree_sitter_lua::LANGUAGE,
        }
        .into()
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Go => "go",
            Self::Rust => "rust",
            Self::Python => "python",
            Self::Nix => "nix",
            Self::Lua => "lua",
        }
    }
}

/// Every language, so a test can assert that each grammar actually loads.
#[cfg(test)]
pub const ALL: [Language; 5] = [
    Language::Go,
    Language::Rust,
    Language::Python,
    Language::Nix,
    Language::Lua,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_mime_types_map_to_grammars() {
        assert_eq!(Language::from_mime("text/x-go"), Some(Language::Go));
        assert_eq!(
            Language::from_mime("text/x-python; charset=utf-8"),
            Some(Language::Python)
        );
    }

    #[test]
    fn prose_has_no_grammar() {
        // Not an error: these chunk by word window.
        assert_eq!(Language::from_mime("text/markdown"), None);
        assert_eq!(Language::from_mime("text/plain"), None);
        assert_eq!(Language::from_mime("text/yaml"), None);
    }

    #[test]
    fn every_grammar_loads() {
        // A grammar compiled against an incompatible tree-sitter ABI fails
        // here rather than silently degrading every file of that language
        // to the word-window fallback at runtime.
        for language in ALL {
            let mut parser = tree_sitter::Parser::new();
            parser
                .set_language(&language.grammar())
                .unwrap_or_else(|e| panic!("{} grammar rejected: {e}", language.name()));
        }
    }
}
