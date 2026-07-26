//! The ingestion pipeline's three stages, each behind a trait.
//!
//! ```text
//!   raw bytes ─▶ Parser ─▶ plain text ─▶ Chunker ─▶ chunks ─▶ Embedder ─▶ vectors
//! ```
//!
//! Traits are the extension points of the worker: a new file format
//! is a new `Parser`, a smarter splitting strategy is a new `Chunker`,
//! a different model is a new `Embedder`. The gRPC service composes
//! whatever implementations it is given (see `service.rs`), so none of
//! these additions touch the service or the proto contract.

pub mod chunker;
pub mod embedder;
pub mod language;
pub mod parser;

pub use chunker::{Chunk, Chunker, SyntaxChunker};
pub use language::Language;
pub use embedder::{Embedder, EmbeddingModelChoice, FastEmbedder};
pub use parser::{InvalidArgument, ParserRegistry};
