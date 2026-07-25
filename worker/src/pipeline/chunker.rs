//! Chunkers: plain text → overlapping windows suitable for embedding.
//!
//! Why chunk at all? Embedding models have a token budget (bge-small:
//! 512 tokens) and, more importantly, one vector per *whole document*
//! averages away the details a query is actually looking for. Smaller
//! pieces give sharper matches; overlap keeps sentences that straddle a
//! boundary findable from either side.

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

/// A simple, robust baseline: a sliding window of words.
///
/// Not sentence-aware and not token-exact — 220 words comfortably fits
/// bge-small's 512-token budget for English. Replacing this with a
/// sentence- or heading-aware chunker is a natural upgrade that requires
/// no changes outside this file.
pub struct WordWindowChunker {
    /// Words per chunk.
    pub window: usize,
    /// Words shared between consecutive chunks.
    pub overlap: usize,
}

impl Default for WordWindowChunker {
    fn default() -> Self {
        Self {
            window: 220,
            overlap: 40,
        }
    }
}

impl Chunker for WordWindowChunker {
    fn chunk(&self, parsed: &ParsedText) -> Vec<Chunk> {
        // split_whitespace also normalizes runs of spaces/newlines,
        // which is fine: layout carries little semantic signal.
        let mut line = parsed.starting_line;
        let mut words = Vec::new();
        let mut word_start = None;
        for (offset, ch) in parsed.text.char_indices() {
            if ch.is_whitespace() {
                if let Some((start, start_line)) = word_start.take() {
                    words.push((&parsed.text[start..offset], start_line, line));
                }
                if ch == '\n' {
                    line += 1;
                }
            } else if word_start.is_none() {
                word_start = Some((offset, line));
            }
        }
        if let Some((start, start_line)) = word_start {
            words.push((&parsed.text[start..], start_line, line));
        }
        if words.is_empty() {
            return Vec::new();
        }

        let step = self.window.saturating_sub(self.overlap).max(1);
        let mut chunks = Vec::new();
        let mut start = 0;
        loop {
            let end = (start + self.window).min(words.len());
            chunks.push(Chunk {
                index: chunks.len() as u32,
                text: words[start..end]
                    .iter()
                    .map(|(word, _, _)| *word)
                    .collect::<Vec<_>>()
                    .join(" "),
                start_line: words[start].1,
                end_line: words[end - 1].2,
            });
            if end == words.len() {
                break;
            }
            start += step;
        }
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_chunk() {
        let chunks = WordWindowChunker::default().chunk(&parsed("hello world", 1));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "hello world");
    }

    #[test]
    fn empty_text_yields_no_chunks() {
        assert!(WordWindowChunker::default()
            .chunk(&parsed("  \n\t ", 1))
            .is_empty());
    }

    #[test]
    fn windows_overlap() {
        let chunker = WordWindowChunker {
            window: 4,
            overlap: 2,
        };
        let text = "a b c d e f g h";
        let chunks = chunker.chunk(&parsed(text, 1));
        assert_eq!(chunks[0].text, "a b c d");
        assert_eq!(chunks[1].text, "c d e f");
        assert_eq!(chunks.last().unwrap().text.split(' ').last(), Some("h"));
        // Indices are consecutive from zero — the store depends on this.
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.index as usize, i);
        }
    }

    fn parsed(text: &str, starting_line: u32) -> ParsedText {
        ParsedText {
            text: text.to_owned(),
            starting_line,
        }
    }

    #[test]
    fn ranges_follow_actual_word_lines_across_overlapping_windows() {
        let chunker = WordWindowChunker {
            window: 3,
            overlap: 1,
        };
        let chunks = chunker.chunk(&parsed("one two\nthree\n\nfour five\nsix", 7));
        assert_eq!(
            chunks.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
            vec!["one two three", "three four five", "five six"]
        );
        assert_eq!(
            chunks
                .iter()
                .map(|c| (c.start_line, c.end_line))
                .collect::<Vec<_>>(),
            vec![(7, 8), (8, 10), (10, 11)]
        );
    }
}
