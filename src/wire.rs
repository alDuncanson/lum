//! The protocol: newline-delimited JSON over one Unix socket.
//!
//! Every message is a single JSON object on a single line, and there are
//! exactly two kinds. A reply carries `id`, matching the `id` of the request
//! that asked for it. An event carries `event`, and arrives unsolicited after
//! a `subscribe`. That is the whole framing:
//!
//! ```text
//! → {"id":1,"op":"search","q":"retry backoff","limit":10}
//! ← {"id":1,"ok":{"results":[…]}}
//! ← {"event":"progress","phase":"embedding","done":48,"total":96}
//! ```
//!
//! It is deliberately something every client can speak with nothing added:
//! Neovim has `vim.json` and `vim.uv`, a shell has `jq` and `socat`, and any
//! editor with async I/O is a couple of hundred lines from a working client.
//!
//! Requests on one connection may be pipelined and are answered
//! independently, so a client is never blocked behind its own slow call —
//! which is what lets the Neovim picker keep a single connection open for a
//! session and fire a query per keystroke down it.

use serde::{Deserialize, Serialize};

/// Requests are internally tagged by `op`, so a message is self-describing
/// and reads the same in a log, in `jq`, and in the Lua client.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Liveness. Answered without touching the model, the index, or the
    /// database, so a client can distinguish "daemon is up" from "daemon is
    /// ready" without either question being expensive.
    Ping,
    Search(SearchRequest),
    AddSource(AddSourceRequest),
    /// `source` rather than `id`: the envelope owns `id`, and a flattened
    /// variant that reuses the name loses to it silently — the request id
    /// overwrites the target, and the caller waits forever for a reply
    /// addressed to a request that failed to parse.
    RemoveSource {
        /// A source id, or a path — the id is a UUID nobody has memorized
        /// and the path is what they typed to `add`.
        source: String,
    },
    ListSources,
    Scan {
        source: String,
    },
    Status,
    Subscribe(SubscribeRequest),
    Shutdown,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchRequest {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Restrict to one source id. Mutually exclusive with `root`.
    #[serde(default)]
    pub source: Option<String>,
    /// Ensure this workspace is registered, then search only it. This is what
    /// makes `lum search --root .` and the Neovim picker need no prior `add`.
    #[serde(default)]
    pub root: Option<String>,
    /// Chunks any one file may contribute. `0` returns raw nearest
    /// neighbours.
    #[serde(default = "default_per_file")]
    pub per_file: usize,
    #[serde(default)]
    pub exclude_tests: bool,
    /// Whether `root` blocks until that workspace's first index finishes.
    ///
    /// The CLI sets this, because `lum search --root .` on a cold repository
    /// promising results it does not have would be a lie. The picker does
    /// not: it wants whatever is indexed *now*, this keystroke, and watches
    /// the progress events on the same connection to know more is coming.
    #[serde(default)]
    pub wait: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AddSourceRequest {
    pub uri: String,
    /// Block until the first index completes.
    #[serde(default)]
    pub wait: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscribeRequest {
    /// Only these event kinds. Empty means all of them.
    #[serde(default)]
    pub kinds: Vec<String>,
    /// Replay the ring buffer on connect.
    ///
    /// `top` wants it, so a freshly opened TUI has immediate context. A
    /// client that *reacts* to events rather than displaying them — the
    /// Neovim notifier — does not, or it announces work that finished long
    /// before it connected.
    #[serde(default)]
    pub replay: bool,
}

fn default_limit() -> usize {
    10
}

/// Two rather than one: a second hit in a large file is often a different
/// function and genuinely worth seeing, while a third rarely is.
fn default_per_file() -> usize {
    2
}

/// An incoming line. `id` is echoed on the reply; a request without one is
/// fire-and-forget.
#[derive(Debug, Clone, Deserialize)]
pub struct Request {
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(flatten)]
    pub op: Op,
}

/// The request id of a raw line, if it has one.
///
/// Used to address the reply to a line that failed to deserialize. Without it,
/// a request the server could not parse produces a reply nobody is waiting for
/// and the caller hangs until its connection drops — which reads exactly like
/// the daemon being wedged, and sent me looking in the wrong place twice.
pub fn id_of(line: &str) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(line).ok()?.get("id")?.as_u64()
}

/// An outgoing line. Exactly one of `ok` or `error` is present.
#[derive(Debug, Clone, Serialize)]
pub struct Reply {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Reply {
    pub fn ok(id: u64, value: serde_json::Value) -> Self {
        Self { id, ok: Some(value), error: None }
    }

    pub fn error(id: u64, message: impl std::fmt::Display) -> Self {
        Self { id, ok: None, error: Some(message.to_string()) }
    }
}

// ---- payloads ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub uri: String,
    /// Path relative to the source root: what a person calls the file.
    pub path: String,
    pub source_id: String,
    pub chunk_index: u32,
    pub score: f32,
    pub text: String,
    /// Inclusive, 1-based. Stored at ingest rather than reconstructed by
    /// clients, so the picker, the CLI, and `jq` all agree.
    pub start_line: u32,
    pub end_line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub query: String,
    pub results: Vec<SearchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    pub uri: String,
    pub documents: i64,
    pub chunks: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddSourceResponse {
    pub source: Source,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    pub uri: String,
    pub attempts: i64,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    /// `starting` | `downloading-model` | `ready` | `failed`
    pub state: String,
    pub detail: String,
    pub sources: i64,
    pub documents: i64,
    pub chunks: i64,
    /// Resident set size in bytes, so `lum status` can answer the question
    /// this rewrite exists to answer without anyone reaching for `ps`.
    pub rss_bytes: u64,
    pub index_bytes: u64,
    /// Resident bytes held by the searchable vectors themselves.
    pub vector_memory_bytes: u64,
    /// Whether the ingest inference session is currently loaded. It is
    /// released when indexing goes idle, so its memory is not held between
    /// edits.
    pub ingest_session: bool,
    pub pending_scans: usize,
    pub pending_documents: usize,
    pub active_document: String,
    pub active_stage: String,
    pub failures: Vec<Failure>,
    pub version: String,
    pub uptime_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minimal_search_fills_in_every_default() {
        // The picker sends three fields; everything else has to have a
        // sensible answer or each client reinvents one.
        let request: Request =
            serde_json::from_str(r#"{"id":7,"op":"search","q":"retry backoff"}"#).unwrap();
        assert_eq!(request.id, Some(7));
        let Op::Search(search) = request.op else {
            panic!("expected a search");
        };
        assert_eq!(search.limit, 10);
        assert_eq!(search.per_file, 2);
        assert!(!search.exclude_tests);
        assert!(!search.wait, "a search must not block on indexing unless asked");
    }

    #[test]
    fn replies_carry_exactly_one_of_ok_or_error() {
        let ok = serde_json::to_string(&Reply::ok(1, serde_json::json!({"a": 1}))).unwrap();
        assert!(ok.contains("\"ok\""), "{ok}");
        assert!(!ok.contains("error"), "{ok}");

        let err = serde_json::to_string(&Reply::error(1, "nope")).unwrap();
        assert!(err.contains("\"error\":\"nope\""), "{err}");
        assert!(!err.contains("\"ok\""), "{err}");
    }

    #[test]
    fn every_message_stays_on_one_line() {
        // The framing *is* the newline. A pretty-printed payload anywhere in
        // here would desynchronize every reader.
        let reply = Reply::ok(
            1,
            serde_json::to_value(SearchResponse {
                query: "x".into(),
                results: vec![SearchResult {
                    uri: "/a/b.rs".into(),
                    path: "b.rs".into(),
                    source_id: "s".into(),
                    chunk_index: 0,
                    score: 0.5,
                    text: "fn main() {\n    // a newline inside a payload\n}".into(),
                    start_line: 1,
                    end_line: 3,
                }],
            })
            .unwrap(),
        );
        let line = serde_json::to_string(&reply).unwrap();
        assert!(!line.contains('\n'), "{line}");
    }

    #[test]
    fn unknown_ops_are_rejected_rather_than_ignored() {
        assert!(serde_json::from_str::<Request>(r#"{"id":1,"op":"drop_everything"}"#).is_err());
    }

    #[test]
    fn no_op_field_collides_with_the_envelope_id() {
        // `Op` is flattened into `Request`, so a variant field named `id` is
        // shadowed by the envelope's request id: the client stamps its id over
        // the caller's argument, the op fails to deserialize, and the reply is
        // addressed to a request nobody is waiting on. `lum scan` and
        // `lum remove` both hung on exactly this.
        for line in [
            r#"{"id":7,"op":"scan","source":"/repo"}"#,
            r#"{"id":7,"op":"remove_source","source":"/repo"}"#,
        ] {
            let request: Request =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("{line}: {e}"));
            assert_eq!(request.id, Some(7));
            let target = match request.op {
                Op::Scan { source } | Op::RemoveSource { source } => source,
                other => panic!("unexpected op {other:?}"),
            };
            assert_eq!(target, "/repo", "the envelope id overwrote the target");
        }
    }

    #[test]
    fn a_malformed_line_still_yields_an_addressable_id() {
        // Whatever else is wrong with a request, the reply has to reach the
        // caller — otherwise a typo in one field is indistinguishable from a
        // hang.
        assert_eq!(id_of(r#"{"id":42,"op":"nonsense"}"#), Some(42));
        assert_eq!(id_of("{not json at all"), None);
    }
}
