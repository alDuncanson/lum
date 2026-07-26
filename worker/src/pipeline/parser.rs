//! Parsers: raw document bytes → plain text.
//!
//! The dispatcher sends every document's raw bytes plus a MIME type;
//! the [`ParserRegistry`] picks the first [`Parser`] that claims support.
//! To add a format (PDF, HTML, EPUB, ...), implement `Parser` and add it
//! to [`ParserRegistry::with_defaults`] — nothing else in the system
//! changes.

use super::language::Language;
use anyhow::Result;
use std::fmt;

/// Text extracted from a document, together with its first original
/// source line. Parsers that remove a prefix must adjust `starting_line`.
///
/// `language` is stamped by the registry rather than by the parser, because
/// it describes the *document*, not the extraction: markdown and Go both
/// come out of a parser as plain text, and only the chunker cares which one
/// it was looking at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedText {
    pub text: String,
    pub starting_line: u32,
    pub language: Option<Language>,
}

impl ParsedText {
    /// Extracted text with no language attached. Parsers use this; the
    /// registry fills in the language afterwards.
    pub fn new(text: impl Into<String>, starting_line: u32) -> Self {
        Self {
            text: text.into(),
            starting_line,
            language: None,
        }
    }
}

/// Marks an error as the caller's fault (unsupported/invalid input), not
/// an internal failure. `service::run_blocking` downcasts for this to
/// return `Status::invalid_argument` instead of a blanket
/// `Status::internal` — the first step toward a real error taxonomy so
/// retry logic can eventually distinguish permanent from transient
/// failures (#7).
#[derive(Debug)]
pub struct InvalidArgument(pub String);

impl fmt::Display for InvalidArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for InvalidArgument {}

/// One document format handler.
///
/// Implementations must be cheap to construct and stateless; the same
/// instance is shared across all requests (hence `Send + Sync`).
pub trait Parser: Send + Sync {
    /// Whether this parser can handle the given MIME type.
    fn supports(&self, mime_type: &str) -> bool;

    /// Extract the human-readable text that should be indexed.
    fn parse(&self, content: &[u8]) -> Result<ParsedText>;
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
    pub fn parse(&self, mime_type: &str, content: &[u8]) -> Result<ParsedText> {
        let Some(parser) = self.parsers.iter().find(|p| p.supports(mime_type)) else {
            return Err(InvalidArgument(format!(
                "no parser registered for MIME type {mime_type:?}"
            ))
            .into());
        };
        let mut parsed = parser.parse(content)?;
        parsed.language = Language::from_mime(mime_type);
        Ok(parsed)
    }
}

/// Catch-all for `text/*`: decode as UTF-8, replacing invalid bytes
/// rather than failing — a lossy index entry beats no index entry.
struct PlainTextParser;

impl Parser for PlainTextParser {
    fn supports(&self, mime_type: &str) -> bool {
        mime_type.starts_with("text/")
    }

    fn parse(&self, content: &[u8]) -> Result<ParsedText> {
        Ok(ParsedText::new(String::from_utf8_lossy(content), 1))
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

    fn parse(&self, content: &[u8]) -> Result<ParsedText> {
        let text = String::from_utf8_lossy(content);
        let (text, starting_line) = strip_front_matter(&text);
        Ok(ParsedText::new(text, starting_line))
    }
}

/// If `text` begins with a `---` fenced YAML block, return the content
/// after the closing fence; otherwise return the input unchanged.
fn strip_front_matter(text: &str) -> (&str, u32) {
    let Some(rest) = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
    else {
        return (text, 1);
    };
    // Walk line by line looking for the closing fence.
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_end() == "---" {
            let body = &rest[offset + line.len()..];
            let removed_len = text.len() - body.len();
            return (
                body,
                1 + text[..removed_len].bytes().filter(|b| *b == b'\n').count() as u32,
            );
        }
        offset += line.len();
    }
    // No closing fence: treat the whole thing as content, not metadata.
    (text, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_prefers_specific_parser() {
        let registry = ParserRegistry::with_defaults();
        let md = b"---\ntags: [a, b]\n---\n# Hello";
        assert_eq!(
            registry.parse("text/markdown", md).unwrap(),
            ParsedText {
                language: Some(Language::Markdown),
                ..ParsedText::new("# Hello", 4)
            }
        );
        // Plain text keeps the front matter verbatim.
        assert!(registry
            .parse("text/plain", md)
            .unwrap()
            .text
            .starts_with("---"));
    }

    #[test]
    fn unknown_mime_is_an_error() {
        let registry = ParserRegistry::with_defaults();
        assert!(registry.parse("application/pdf", b"%PDF").is_err());
    }

    #[test]
    fn unknown_mime_error_is_an_invalid_argument_not_a_generic_failure() {
        let registry = ParserRegistry::with_defaults();
        let error = registry.parse("application/pdf", b"%PDF").unwrap_err();
        let invalid = error
            .downcast_ref::<InvalidArgument>()
            .expect("unsupported MIME type must be reported as InvalidArgument (#7)");
        assert!(invalid.0.contains("application/pdf"));
    }

    #[test]
    fn registry_stamps_the_language_for_code() {
        let registry = ParserRegistry::with_defaults();
        assert_eq!(
            registry
                .parse("text/x-go", b"package main")
                .unwrap()
                .language,
            Some(Language::Go)
        );
        // Prose parses fine and simply carries no grammar.
        assert_eq!(registry.parse("text/plain", b"hi").unwrap().language, None);
    }

    #[test]
    fn front_matter_without_close_is_kept() {
        assert_eq!(strip_front_matter("---\nunclosed"), ("---\nunclosed", 1));
    }

    #[test]
    fn markdown_front_matter_preserves_crlf_line_provenance() {
        let parsed = ParserRegistry::with_defaults()
            .parse(
                "text/markdown",
                b"---\r\ntitle: x\r\n---\r\nfirst\r\nsecond",
            )
            .unwrap();
        assert_eq!(parsed.text, "first\r\nsecond");
        assert_eq!(parsed.starting_line, 4);
    }
}
