//! Syntax-aware chunking: split code where the language says one thing ends
//! and the next begins.
//!
//! A word window has no idea what it is cutting. It will end a chunk halfway
//! through a function and open the next one with the rest of that function
//! plus the doc comment of the one after it, so neither chunk is *about*
//! anything in particular. Embedding averages over whatever it is given;
//! give it half of two functions and you get a vector near neither.
//!
//! The strategy is total coverage by descent. Start at the root and take the
//! largest nodes that fit the budget; a node too big to fit is replaced by
//! its children, and the text between children (braces, operators, blank
//! lines) is kept as its own span so nothing is dropped. The resulting spans
//! tile the file end to end, and are then merged back up to the budget so
//! chunks are as large as they can be without straddling a boundary that
//! matters.
//!
//! Two details earn their complexity:
//!
//! - **Leading text travels with what it introduces.** A doc comment is a
//!   *sibling* of the declaration in every grammar here, and a markdown
//!   heading is a sibling of the prose under it, so greedy merging would
//!   happily end a chunk on the comment or heading and start the next one on
//!   the thing it was introducing. When a merge has to break, any run of
//!   leading text at the tail of the finished chunk moves to the front of the
//!   new one instead.
//! - **The budget is bytes, not tokens.** bge-small truncates at 512 tokens
//!   and code runs about three characters per token, so 1200 bytes leaves
//!   room for the path prefix (see `service::embed_text`) without truncating.
//!
//! Anything without a grammar — markdown, YAML, plain text — falls through to
//! [`WordWindowChunker`], as does any file the parser chokes on. A missing
//! grammar must never mean a missing document.

use super::{Chunk, Chunker, WordWindowChunker};
use crate::pipeline::language::Language;
use crate::pipeline::parser::ParsedText;
use tree_sitter::{Node, Parser};

/// Largest chunk, in bytes of source. See the module docs.
const DEFAULT_MAX_BYTES: usize = 1200;

/// How deep the descent will go before giving up and splitting by line.
/// Generated code and minified data can nest far past anything a person
/// writes, and the recursion is bounded here rather than by the stack.
const MAX_DEPTH: usize = 48;

/// Splits code at syntax boundaries, everything else at word boundaries.
pub struct SyntaxChunker {
    pub max_bytes: usize,
    fallback: WordWindowChunker,
}

impl Default for SyntaxChunker {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_BYTES,
            fallback: WordWindowChunker::default(),
        }
    }
}

impl Chunker for SyntaxChunker {
    fn chunk(&self, parsed: &ParsedText) -> Vec<Chunk> {
        match parsed.language.and_then(|l| self.chunk_syntax(parsed, l)) {
            Some(chunks) if !chunks.is_empty() => chunks,
            _ => self.fallback.chunk(parsed),
        }
    }
}

/// What a span holds, which decides how it may be merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Body,
    /// Text that introduces what comes after it: a doc comment, a heading.
    /// Never the tail of a chunk if it can be the head of the next one.
    Lead,
    /// Whitespace only. Never starts a chunk, never interrupts a run of
    /// leads — a blank line between a doc comment and its function must not
    /// make them look unrelated.
    Blank,
}

#[derive(Debug, Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
    kind: Kind,
}

impl SyntaxChunker {
    fn chunk_syntax(&self, parsed: &ParsedText, language: Language) -> Option<Vec<Chunk>> {
        let src = parsed.text.as_bytes();
        let mut parser = Parser::new();
        parser.set_language(&language.grammar()).ok()?;
        let tree = parser.parse(src, None)?;
        let root = tree.root_node();

        // The root need not span the whole file — leading and trailing
        // whitespace can fall outside it — and coverage has to be total or
        // text goes missing from the index.
        let mut spans = Vec::new();
        self.push_text(0, root.start_byte(), src, &mut spans);
        self.descend(root, src, 0, &mut spans);
        self.push_text(root.end_byte(), src.len(), src, &mut spans);

        let spans = self.coalesce_leads(&spans, src);
        let headings = language
            .uses_heading_context()
            .then(|| HeadingTrail::build(root, src));
        Some(self.assemble(parsed, &spans, headings.as_ref()))
    }

    /// Fold a run of consecutive leads into one span.
    ///
    /// Every grammar here makes each `//` line its own node, so a ten-line
    /// doc comment arrives as ten siblings and greedy merging will happily
    /// break in the middle of it — which strands a fragment of a sentence as
    /// its own chunk and leaves the rest heading a chunk it does not
    /// introduce. Treating the run as one unit means it either travels with
    /// what follows or stands alone, but never splits. Nested markdown
    /// headings (`## Four` immediately above `### Four point one`) merge for
    /// the same reason.
    ///
    /// A blank line ends a run: that is what a blank line is for.
    fn coalesce_leads(&self, spans: &[Span], src: &[u8]) -> Vec<Span> {
        let mut out: Vec<Span> = Vec::with_capacity(spans.len());
        for span in spans {
            if span.kind == Kind::Lead {
                let joinable = match out.last() {
                    Some(prev) if prev.kind == Kind::Lead => Some(out.len() - 1),
                    // One line break between them, so they are consecutive
                    // lines of the same comment block.
                    Some(prev) if prev.kind == Kind::Blank && newlines(src, prev) <= 1 => out
                        .len()
                        .checked_sub(2)
                        .filter(|i| out[*i].kind == Kind::Lead),
                    _ => None,
                };
                if let Some(at) = joinable {
                    if span.end - out[at].start <= self.max_bytes {
                        // Extending over the gap keeps coverage total even
                        // though the blank span itself is dropped.
                        out.truncate(at + 1);
                        out[at].end = span.end;
                        continue;
                    }
                }
            }
            out.push(*span);
        }
        out
    }

    /// Emit spans covering `node` exactly, descending only where a node is
    /// too large to stand on its own.
    fn descend(&self, node: Node, src: &[u8], depth: usize, out: &mut Vec<Span>) {
        let (start, end) = (node.start_byte(), node.end_byte());
        if end <= start {
            return;
        }
        if end - start <= self.max_bytes {
            out.push(Span {
                start,
                end,
                kind: classify(&src[start..end], leads(node)),
            });
            return;
        }
        if depth >= MAX_DEPTH || node.child_count() == 0 {
            // Nothing left to split on syntactically: a giant string
            // literal, a data table, a minified line.
            self.split_lines(start, end, src, out);
            return;
        }

        let mut cursor = start;
        let mut walker = node.walk();
        for child in node.children(&mut walker) {
            // The gap before this child: punctuation and layout that belongs
            // to no child but is part of the file.
            self.push_text(cursor, child.start_byte(), src, out);
            self.descend(child, src, depth + 1, out);
            cursor = cursor.max(child.end_byte());
        }
        self.push_text(cursor, end, src, out);
    }

    /// Record `[start, end)` as one span, or several if it exceeds the
    /// budget on its own.
    fn push_text(&self, start: usize, end: usize, src: &[u8], out: &mut Vec<Span>) {
        if end <= start {
            return;
        }
        if end - start <= self.max_bytes {
            out.push(Span {
                start,
                end,
                kind: classify(&src[start..end], false),
            });
        } else {
            self.split_lines(start, end, src, out);
        }
    }

    /// Last resort: cut at line boundaries, and inside a line if even one
    /// line will not fit.
    fn split_lines(&self, start: usize, end: usize, src: &[u8], out: &mut Vec<Span>) {
        let mut chunk_start = start;
        let mut fitted = start; // end of the last line known to fit
        let mut cursor = start;

        while cursor < end {
            let line_end = match src[cursor..end].iter().position(|b| *b == b'\n') {
                Some(offset) => cursor + offset + 1,
                None => end,
            };
            if line_end - chunk_start > self.max_bytes {
                if fitted > chunk_start {
                    out.push(Span {
                        start: chunk_start,
                        end: fitted,
                        kind: classify(&src[chunk_start..fitted], false),
                    });
                    chunk_start = fitted;
                }
                // One line longer than the whole budget: cut it up.
                while line_end - chunk_start > self.max_bytes {
                    let boundary = char_boundary(src, chunk_start + self.max_bytes);
                    if boundary <= chunk_start {
                        break;
                    }
                    out.push(Span {
                        start: chunk_start,
                        end: boundary,
                        kind: classify(&src[chunk_start..boundary], false),
                    });
                    chunk_start = boundary;
                }
            }
            fitted = line_end;
            cursor = line_end;
        }
        self.push_text(chunk_start, end, src, out);
    }

    /// Merge spans back up to the budget, then turn each run into a chunk.
    fn assemble(
        &self,
        parsed: &ParsedText,
        spans: &[Span],
        headings: Option<&HeadingTrail>,
    ) -> Vec<Chunk> {
        let text = &parsed.text;
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        let mut current: Option<(usize, usize)> = None;
        // Start of the run of comments at the tail of `current`, if any.
        let mut trailing_lead: Option<usize> = None;

        for span in spans {
            if span.end <= span.start {
                continue;
            }
            match current {
                // Leading blank space belongs to no chunk.
                None if span.kind == Kind::Blank => continue,
                None => current = Some((span.start, span.end)),
                Some((start, end)) if span.end - start <= self.max_bytes => {
                    current = Some((start, span.end.max(end)))
                }
                Some((start, end)) => {
                    // A break, so decide what happens to any run of leading
                    // text at the tail of the chunk being closed. Leaving it
                    // there is always wrong: it introduces what comes next.
                    match trailing_lead {
                        Some(c) if c > start && span.end - c <= self.max_bytes => {
                            // It fits with what follows. One chunk, comment
                            // first — the shape the descent is here to get.
                            ranges.push((start, c));
                            current = Some((c, span.end));
                        }
                        Some(c) if c > start => {
                            // It does not fit. Give it its own chunk rather
                            // than leaving it as the tail of something it has
                            // nothing to do with: alone it still reads as a
                            // description of what is below it, and its line
                            // range still points there.
                            ranges.push((start, c));
                            ranges.push((c, span.start));
                            current = Some((span.start, span.end));
                        }
                        _ => {
                            ranges.push((start, end));
                            current = Some((end, span.end));
                        }
                    }
                    trailing_lead = None;
                }
            }
            match span.kind {
                Kind::Lead => {
                    trailing_lead.get_or_insert(span.start);
                }
                Kind::Body => trailing_lead = None,
                Kind::Blank => {}
            }
        }
        if let Some(range) = current {
            ranges.push(range);
        }

        let newlines = newline_offsets(text);
        ranges
            .into_iter()
            .filter_map(|(start, end)| {
                let raw = text.get(start..end)?;
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return None;
                }
                let from = start + (raw.len() - raw.trim_start().len());
                Some((from, from + trimmed.len(), trimmed.to_owned()))
            })
            .enumerate()
            .map(|(index, (from, to, text))| Chunk {
                index: index as u32,
                start_line: parsed.starting_line + line_at(&newlines, from),
                end_line: parsed.starting_line + line_at(&newlines, to.saturating_sub(1)),
                context: headings.map(|h| h.at(from, to)).unwrap_or_default(),
                text,
            })
            .collect()
    }
}

/// Whether a node introduces what follows it: a comment in any of the
/// spellings the grammars use (`comment`, `line_comment`, `block_comment`,
/// `doc_comment`), or a markdown heading.
fn leads(node: Node) -> bool {
    let kind = node.kind();
    kind.contains("comment") || kind.contains("heading")
}

/// The heading trail in force at any point in a markdown document.
///
/// "protocol boundaries table" should find the table under
/// `## 1. Boundaries and protocols`, but the paragraph holding it may not
/// contain either word — the document said it once, in a heading, three
/// screens up. Prepending the trail before embedding puts it back, the same
/// way the repository path is prepended in `service::embed_text`.
struct HeadingTrail {
    entries: Vec<Entry>,
}

struct Entry {
    start: usize,
    text: String,
    /// The full trail ending at this heading.
    trail: String,
}

impl HeadingTrail {
    fn build(root: Node, src: &[u8]) -> Self {
        let mut headings = Vec::new();
        collect_headings(root, src, &mut headings);
        headings.sort_by_key(|(start, _, _)| *start);

        // A document with exactly one top-level heading has a *title*, and
        // the title says what the path already says. Carrying it on every
        // chunk measurably hurt: it made whole documents more competitive for
        // queries whose answer is code, and docs describe code. A document
        // with several top-level headings has sections rather than a title,
        // and those stay. See eval/README.md.
        let title = headings
            .iter()
            .map(|(_, level, _)| *level)
            .min()
            .filter(|top| headings.iter().filter(|(_, level, _)| level == top).count() == 1);

        let mut entries = Vec::with_capacity(headings.len());
        let mut open: Vec<(usize, String)> = Vec::new();
        for (start, level, text) in headings {
            // A heading closes every heading at or below its own level: `##`
            // ends the `###` before it and the previous `##` too.
            while open.last().is_some_and(|(open_level, _)| *open_level >= level) {
                open.pop();
            }
            open.push((level, text));
            let trail = open
                .iter()
                .filter(|(level, _)| Some(*level) != title)
                .map(|(_, text)| text.as_str())
                .collect::<Vec<_>>()
                .join(" > ");
            entries.push(Entry {
                start,
                text: open.last().map(|(_, text)| text.clone()).unwrap_or_default(),
                trail,
            });
        }
        Self { entries }
    }

    /// The trail in force over `[from, to)`, minus the headings the chunk
    /// renders itself — those are already in its text, and spending the token
    /// budget to say them twice makes a chunk look more like itself.
    ///
    /// "Renders itself" is decided by byte offset rather than by searching the
    /// text for the heading: a document titled "lum" would otherwise have that
    /// heading stripped from every chunk that so much as mentions the word.
    fn at(&self, from: usize, to: usize) -> String {
        let trail = match self.entries.partition_point(|entry| entry.start <= from) {
            0 => return String::new(),
            index => &self.entries[index - 1].trail,
        };
        let own: Vec<&str> = self
            .entries
            .iter()
            .filter(|entry| entry.start >= from && entry.start < to)
            .map(|entry| entry.text.as_str())
            .collect();
        trail
            .split(" > ")
            .filter(|heading| !own.contains(heading))
            .collect::<Vec<_>>()
            .join(" > ")
    }
}

fn collect_headings(node: Node, src: &[u8], out: &mut Vec<(usize, usize, String)>) {
    if node.kind().contains("heading") {
        if let Some((level, text)) = heading(node, src) {
            out.push((node.start_byte(), level, text));
        }
        return;
    }
    let mut walker = node.walk();
    for child in node.children(&mut walker) {
        collect_headings(child, src, out);
    }
}

/// A heading's level and text, for both markdown spellings: `## Title` and a
/// line underlined with `===` or `---`.
fn heading(node: Node, src: &[u8]) -> Option<(usize, String)> {
    let raw = std::str::from_utf8(src.get(node.start_byte()..node.end_byte())?).ok()?;
    let first = raw.lines().next()?.trim();
    let (level, text) = if first.starts_with('#') {
        let hashes = first.chars().take_while(|c| *c == '#').count();
        (hashes, first[hashes..].trim().trim_end_matches('#').trim())
    } else {
        let underline = raw.lines().nth(1)?.trim();
        (if underline.starts_with('=') { 1 } else { 2 }, first)
    };
    if text.is_empty() || level == 0 || level > 6 {
        return None;
    }
    Some((level, text.to_owned()))
}

fn newlines(src: &[u8], span: &Span) -> usize {
    src[span.start..span.end]
        .iter()
        .filter(|b| **b == b'\n')
        .count()
}

fn classify(bytes: &[u8], lead: bool) -> Kind {
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        Kind::Blank
    } else if lead {
        Kind::Lead
    } else {
        Kind::Body
    }
}

/// The largest UTF-8 character boundary at or before `index`.
fn char_boundary(src: &[u8], index: usize) -> usize {
    let mut index = index.min(src.len());
    while index > 0 && index < src.len() && (src[index] & 0xC0) == 0x80 {
        index -= 1;
    }
    index
}

fn newline_offsets(text: &str) -> Vec<usize> {
    text.bytes()
        .enumerate()
        .filter(|(_, b)| *b == b'\n')
        .map(|(i, _)| i)
        .collect()
}

/// 0-based line containing `byte`.
fn line_at(newlines: &[usize], byte: usize) -> u32 {
    newlines.partition_point(|offset| *offset < byte) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::chunker::parsed;

    fn code(text: &str, language: Language) -> ParsedText {
        let mut p = parsed(text, 1);
        p.language = Some(language);
        p
    }

    const GO_FILE: &str = r#"package ingest

import "context"

// Runner drives one document through the pipeline.
type Runner struct {
	client Client
}

// Run reads the document, chunks it, and hands the chunks to the worker.
// It returns the number of chunks written.
func (r *Runner) Run(ctx context.Context, uri string) (int, error) {
	content, err := os.ReadFile(uri)
	if err != nil {
		return 0, err
	}
	return r.client.Ingest(ctx, uri, content)
}
"#;

    #[test]
    fn small_file_is_one_chunk() {
        let chunks = SyntaxChunker::default().chunk(&code(GO_FILE, Language::Go));
        assert_eq!(chunks.len(), 1, "{GO_FILE} is well under the budget");
        assert!(chunks[0].text.starts_with("package ingest"));
        assert_eq!(chunks[0].start_line, 1);
    }

    #[test]
    fn declarations_split_at_their_own_boundaries() {
        // A budget small enough to force a split within this file: the cut
        // must land between declarations, not inside one.
        let chunker = SyntaxChunker {
            max_bytes: 200,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(GO_FILE, Language::Go));
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            let opens = chunk.text.matches("func ").count();
            if opens > 0 {
                assert!(
                    chunk.text.contains("func (r *Runner) Run"),
                    "a chunk containing a func keyword should hold the whole signature: {:?}",
                    chunk.text
                );
            }
        }
    }

    #[test]
    fn doc_comments_stay_with_what_they_document() {
        // The reason this chunker exists. 400 bytes cannot hold the whole
        // file but can hold `// Run ...` together with the function it
        // describes, so the break must fall before the comment.
        let chunker = SyntaxChunker {
            max_bytes: 400,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(GO_FILE, Language::Go));
        assert!(chunks.len() > 1, "the file should not fit in one chunk");
        let with_run = chunks
            .iter()
            .find(|c| c.text.contains("func (r *Runner) Run"))
            .expect("the Run function must appear in some chunk");
        assert!(
            with_run.text.starts_with("// Run reads the document"),
            "doc comment was orphaned from its function: {:?}",
            with_run.text
        );
    }

    #[test]
    fn a_doc_comment_too_big_to_join_its_declaration_is_not_left_behind() {
        // At 200 bytes the comment and the function cannot share a chunk, so
        // the comment gets one of its own. What must never happen is the
        // comment ending up appended to the *preceding* declaration, which is
        // both wrong and the greedy default.
        let chunker = SyntaxChunker {
            max_bytes: 200,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(GO_FILE, Language::Go));
        let with_comment = chunks
            .iter()
            .find(|c| c.text.contains("// Run reads the document"))
            .expect("the comment must survive somewhere");
        assert!(
            !with_comment.text.contains("type Runner struct"),
            "doc comment was absorbed by the declaration above it: {:?}",
            with_comment.text
        );
    }

    #[test]
    fn chunks_tile_the_file_without_loss_or_duplication() {
        // The property that makes descent safe: every non-whitespace byte of
        // the source appears exactly once, in order, across the chunks. A
        // chunker that silently drops a function is worse than a crude one.
        for max_bytes in [80, 200, 512, 1200] {
            let chunker = SyntaxChunker {
                max_bytes,
                ..Default::default()
            };
            for (text, language) in samples() {
                let chunks = chunker.chunk(&code(text, language));
                let rejoined: String = chunks
                    .iter()
                    .flat_map(|c| c.text.chars())
                    .filter(|c| !c.is_whitespace())
                    .collect();
                let expected: String = text.chars().filter(|c| !c.is_whitespace()).collect();
                assert_eq!(
                    rejoined,
                    expected,
                    "{} at max_bytes={max_bytes} lost or duplicated text",
                    language.name()
                );
                for (i, chunk) in chunks.iter().enumerate() {
                    assert_eq!(chunk.index as usize, i, "indices must be consecutive");
                    assert!(chunk.start_line <= chunk.end_line);
                }
            }
        }
    }

    #[test]
    fn line_numbers_point_at_the_declaration() {
        let chunker = SyntaxChunker {
            max_bytes: 200,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(GO_FILE, Language::Go));
        let with_run = chunks
            .iter()
            .find(|c| c.text.contains("// Run reads the document"))
            .unwrap();
        let expected = GO_FILE
            .lines()
            .position(|l| l.starts_with("// Run reads"))
            .unwrap() as u32
            + 1;
        assert_eq!(with_run.start_line, expected);
    }

    #[test]
    fn starting_line_offsets_are_carried_through() {
        let mut input = code(GO_FILE, Language::Go);
        input.starting_line = 10;
        let chunks = SyntaxChunker::default().chunk(&input);
        assert_eq!(chunks[0].start_line, 10);
    }

    #[test]
    fn documents_without_a_grammar_fall_back_to_word_windows() {
        let prose = "one two three four five six seven";
        let chunks = SyntaxChunker::default().chunk(&parsed(prose, 1));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, prose);
    }

    #[test]
    fn broken_code_still_produces_chunks() {
        // tree-sitter is error-tolerant, but if a grammar ever returned
        // nothing this must degrade to prose chunking rather than to an
        // unindexed file.
        let chunks =
            SyntaxChunker::default().chunk(&code("func ( { { { unterminated", Language::Go));
        assert!(!chunks.is_empty());
        assert!(chunks[0].text.contains("unterminated"));
    }

    #[test]
    fn empty_and_whitespace_only_files_yield_nothing() {
        assert!(SyntaxChunker::default()
            .chunk(&code("", Language::Go))
            .is_empty());
        assert!(SyntaxChunker::default()
            .chunk(&code("\n\n  \t\n", Language::Go))
            .is_empty());
    }

    #[test]
    fn a_single_oversized_line_is_split_at_char_boundaries() {
        // Non-ASCII on purpose: cutting a multi-byte character in half would
        // panic on the string slice.
        let long = format!("x := \"{}\"\n", "é".repeat(4000));
        let chunks = SyntaxChunker::default().chunk(&code(&long, Language::Go));
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.text.len() <= 1200 + 8);
        }
    }

    #[test]
    fn chunking_is_deterministic() {
        // Re-ingest overwrites points by chunk index, so unstable output
        // would leave stale vectors behind.
        let chunker = SyntaxChunker::default();
        for (text, language) in samples() {
            let first = chunker.chunk(&code(text, language));
            assert_eq!(first, chunker.chunk(&code(text, language)));
        }
    }

    const MARKDOWN: &str = r#"# lum

Intro prose.

## Ingestion data flow

How a file becomes a vector.

### Level 0

Stores and flows.

## Deletion

Ordering invariants.

Setext still works
------------------

Trailing prose.
"#;

    #[test]
    fn markdown_chunks_carry_the_heading_trail() {
        let chunker = SyntaxChunker {
            max_bytes: 60,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(MARKDOWN, Language::Markdown));
        let level0 = chunks
            .iter()
            .find(|c| c.text.contains("Stores and flows"))
            .expect("the Level 0 body must be somewhere");
        // "### Level 0" opens this chunk, so it is filtered as the chunk's
        // own heading; what the text cannot say for itself is what remains.
        assert_eq!(level0.context, "Ingestion data flow");
    }

    #[test]
    fn the_document_title_is_not_in_the_trail() {
        // "# lum" repeats what the path says, and paying for it on every
        // chunk of the file made docs outrank the code they describe.
        for chunk in SyntaxChunker::default().chunk(&code(MARKDOWN, Language::Markdown)) {
            assert!(!chunk.context.contains("lum"), "{:?}", chunk.context);
        }
    }

    #[test]
    fn a_document_with_no_single_title_keeps_every_heading() {
        // Two top-level headings are sections, not a title.
        let text = "## First\n\nalpha prose that runs on for a while here.\n\n\
                    ## Second\n\nbeta prose that also runs on for a while.\n";
        let chunker = SyntaxChunker {
            max_bytes: 50,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(text, Language::Markdown));
        let beta = chunks.iter().find(|c| c.text.starts_with("beta")).unwrap();
        assert_eq!(beta.context, "Second");
    }

    #[test]
    fn a_sibling_heading_closes_the_one_before_it() {
        let chunker = SyntaxChunker {
            max_bytes: 60,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(MARKDOWN, Language::Markdown));
        let deletion = chunks
            .iter()
            .find(|c| c.text.contains("Ordering invariants"))
            .unwrap();
        // "## Deletion" ends "## Ingestion data flow" and its "### Level 0".
        assert!(!deletion.context.contains("Ingestion"), "{:?}", deletion.context);
        assert!(!deletion.context.contains("Level 0"), "{:?}", deletion.context);
    }

    #[test]
    fn setext_headings_count_too() {
        let text = "# Top\n\nintro\n\nUnderlined heading\n------------------\n\nA paragraph \
                    long enough that it cannot share a chunk with the heading above it, \
                    whatever the budget.\n";
        let chunker = SyntaxChunker {
            max_bytes: 80,
            ..Default::default()
        };
        let chunks = chunker.chunk(&code(text, Language::Markdown));
        let body = chunks
            .iter()
            .find(|c| c.text.starts_with("A paragraph"))
            .expect("the paragraph should be its own chunk");
        // "# Top" is the title and drops out; the setext heading stays.
        assert_eq!(body.context, "Underlined heading");
    }

    #[test]
    fn a_heading_the_chunk_shows_is_not_also_context() {
        let chunks = SyntaxChunker::default().chunk(&code(MARKDOWN, Language::Markdown));
        let first = &chunks[0];
        assert!(first.text.starts_with("# lum"));
        assert_eq!(first.context, "", "the chunk already shows every heading it is under");
    }

    #[test]
    fn a_chunk_opening_with_its_own_heading_does_not_repeat_it() {
        // The trail is there to supply what the text does not say. Saying it
        // twice spends the token budget to make the chunk look like itself.
        let chunks = SyntaxChunker::default().chunk(&code(MARKDOWN, Language::Markdown));
        for chunk in &chunks {
            for heading in chunk.context.split(" > ").filter(|h| !h.is_empty()) {
                assert!(
                    !chunk.text.contains(heading),
                    "{heading:?} is in both the context and the text of {:?}",
                    chunk.text
                );
            }
        }
    }

    #[test]
    fn hashes_inside_a_fence_are_not_headings() {
        // The reason this uses a grammar rather than a line scanner: shell
        // comments in a fenced block look exactly like ATX headings.
        let text = "# Real\n\nprose\n\n```sh\n# not a heading\nlum add .\n```\n\nmore prose\n";
        let chunker = SyntaxChunker {
            max_bytes: 40,
            ..Default::default()
        };
        for chunk in chunker.chunk(&code(text, Language::Markdown)) {
            assert!(
                !chunk.context.contains("not a heading"),
                "fenced comment became a heading: {:?}",
                chunk.context
            );
        }
    }

    #[test]
    fn code_carries_no_heading_context() {
        for chunk in SyntaxChunker::default().chunk(&code(GO_FILE, Language::Go)) {
            assert_eq!(chunk.context, "");
        }
    }

    fn samples() -> Vec<(&'static str, Language)> {
        vec![
            (GO_FILE, Language::Go),
            (
                r#"//! A worker.
use std::fmt;

/// Splits text.
pub struct Chunker {
    pub window: usize,
}

impl Chunker {
    /// Split it.
    pub fn chunk(&self, text: &str) -> Vec<String> {
        text.split(' ').map(str::to_owned).collect()
    }
}
"#,
                Language::Rust,
            ),
            (
                "# a module\nimport os\n\n\ndef read(path):\n    \"\"\"Read a file.\"\"\"\n    with open(path) as handle:\n        return handle.read()\n",
                Language::Python,
            ),
            (
                "{ pkgs, ... }:\n# the dev shell\npkgs.mkShell {\n  packages = [ pkgs.go pkgs.cargo ];\n}\n",
                Language::Nix,
            ),
            (
                "local M = {}\n\n--- Show a line.\nfunction M.show(text)\n  return text\nend\n\nreturn M\n",
                Language::Lua,
            ),
            (MARKDOWN, Language::Markdown),
        ]
    }
}


