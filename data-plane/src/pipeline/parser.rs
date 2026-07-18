//! Parsers: raw document bytes → plain text.
//!
//! The control plane sends every document's raw bytes plus a MIME type;
//! the [`ParserRegistry`] picks the first [`Parser`] that claims support.
//! To add a format (PDF, HTML, EPUB, ...), implement `Parser` and add it
//! to [`ParserRegistry::with_defaults`] — nothing else in the system
//! changes.

use anyhow::{bail, Result};

/// One document format handler.
///
/// Implementations must be cheap to construct and stateless; the same
/// instance is shared across all requests (hence `Send + Sync`).
pub trait Parser: Send + Sync {
    /// Whether this parser can handle the given MIME type.
    fn supports(&self, mime_type: &str) -> bool;

    /// Extract the human-readable text that should be indexed.
    fn parse(&self, content: &[u8]) -> Result<String>;
}

/// Ordered collection of parsers. Order matters: the first parser whose
/// `supports` returns true wins, so put specific parsers (markdown)
/// before catch-alls (plain text).
pub struct ParserRegistry {
    parsers: Vec<Box<dyn Parser>>,
}

impl ParserRegistry {
    /// The default set of parsers shipped with lum.
    pub fn with_defaults() -> Self {
        Self {
            parsers: vec![Box::new(MarkdownParser), Box::new(PlainTextParser)],
        }
    }

    /// Parse `content` using the first parser that supports `mime_type`.
    pub fn parse(&self, mime_type: &str, content: &[u8]) -> Result<String> {
        let Some(parser) = self.parsers.iter().find(|p| p.supports(mime_type)) else {
            bail!("no parser registered for MIME type {mime_type:?}");
        };
        parser.parse(content)
    }
}

/// Catch-all for `text/*`: decode as UTF-8, replacing invalid bytes
/// rather than failing — a lossy index entry beats no index entry.
struct PlainTextParser;

impl Parser for PlainTextParser {
    fn supports(&self, mime_type: &str) -> bool {
        mime_type.starts_with("text/")
    }

    fn parse(&self, content: &[u8]) -> Result<String> {
        Ok(String::from_utf8_lossy(content).into_owned())
    }
}

/// Markdown-specific parsing. Currently its only extra behavior over
/// plain text is stripping YAML front matter (the `---`-fenced metadata
/// block many note-taking tools prepend), which would otherwise pollute
/// embeddings with tags/dates. Extending this to strip code fences or
/// convert links is a good exercise.
struct MarkdownParser;

impl Parser for MarkdownParser {
    fn supports(&self, mime_type: &str) -> bool {
        mime_type == "text/markdown"
    }

    fn parse(&self, content: &[u8]) -> Result<String> {
        let text = String::from_utf8_lossy(content);
        Ok(strip_front_matter(&text).to_owned())
    }
}

/// If `text` begins with a `---` fenced YAML block, return the content
/// after the closing fence; otherwise return the input unchanged.
fn strip_front_matter(text: &str) -> &str {
    let Some(rest) = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
    else {
        return text;
    };
    // Walk line by line looking for the closing fence.
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            return &rest[offset + line.len()..];
        }
        offset += line.len();
    }
    // No closing fence: treat the whole thing as content, not metadata.
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_prefers_specific_parser() {
        let registry = ParserRegistry::with_defaults();
        let md = b"---\ntags: [a, b]\n---\n# Hello";
        assert_eq!(registry.parse("text/markdown", md).unwrap(), "# Hello");
        // Plain text keeps the front matter verbatim.
        assert!(registry.parse("text/plain", md).unwrap().starts_with("---"));
    }

    #[test]
    fn unknown_mime_is_an_error() {
        let registry = ParserRegistry::with_defaults();
        assert!(registry.parse("application/pdf", b"%PDF").is_err());
    }

    #[test]
    fn front_matter_without_close_is_kept() {
        assert_eq!(strip_front_matter("---\nunclosed"), "---\nunclosed");
    }
}
