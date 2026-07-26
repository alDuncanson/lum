//! Chunkers: parsed text → pieces suitable for embedding.
//!
//! Why chunk at all? Embedding models have a token budget (bge-small: 512
//! tokens) and, more importantly, one vector per *whole document* averages
//! away the details a query is actually looking for. Smaller pieces give
//! sharper matches.
//!
//! Two strategies, picked per document by [`SyntaxChunker`]:
//!
//! - Code in a language lum has a grammar for splits at syntax boundaries,
//!   so a chunk is a function or a type rather than the tail of one and the
//!   head of the next.
//! - Everything else — markdown, YAML, plain text, and any language without
//!   a grammar — falls back to [`WordWindowChunker`], a sliding window with
//!   overlap.

mod syntax;
mod word_window;

pub use syntax::SyntaxChunker;
pub use word_window::WordWindowChunker;

use super::parser::ParsedText;

/// One piece of a document. `index` is the chunk's position (0-based)
/// and is part of the vector point identity — see `store::point_uuid`.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub index: u32,
    pub text: String,
    pub start_line: u32,
    pub end_line: u32,
}

/// Strategy for splitting text. Implementations must be deterministic:
/// the same text must always yield the same chunks, because re-ingests
/// rely on chunk indices being stable to overwrite stale points.
pub trait Chunker: Send + Sync {
    fn chunk(&self, parsed: &ParsedText) -> Vec<Chunk>;
}

#[cfg(test)]
pub(crate) fn parsed(text: &str, starting_line: u32) -> ParsedText {
    ParsedText::new(text, starting_line)
}
