//! The Model Context Protocol, over stdio.
//!
//! An agent spawns `lum mcp` and calls its tools over JSON-RPC on
//! stdin/stdout. Like every other interface here it is a socket client and
//! holds no state: it opens no database, loads no model, and can be killed
//! freely.
//!
//! One stdio rule: never write to stdout except protocol messages. Diagnostics
//! go to stderr, because stdout *is* the channel.

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::client::Client;
use crate::config::Config;
use crate::wire::{AddSourceResponse, SearchResponse, Source, Status};

/// Versions this server understands. The negotiated version is the client's if
/// we know it, and our newest otherwise — a client that asks for something
/// unknown gets told what it will actually get rather than a silent mismatch.
const SUPPORTED: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

pub async fn run(config: &Config) -> Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = stdin.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            continue;
        };
        let id = request.get("id").cloned();

        // A notification has no id and takes no response, ever — replying to
        // one is a protocol error, not a harmless extra.
        if id.is_none() {
            continue;
        }

        let params = request.get("params").cloned().unwrap_or(Value::Null);
        let response = match method {
            "initialize" => Ok(initialize(&params)),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(tools()),
            "tools/call" => call(config, &params).await,
            other => Err(anyhow::anyhow!("unknown method {other}")),
        };

        let message = match response {
            Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
            Err(error) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32603, "message": format!("{error:#}")}
            }),
        };
        stdout.write_all(serde_json::to_string(&message)?.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}

fn initialize(params: &Value) -> Value {
    let requested = params.get("protocolVersion").and_then(Value::as_str).unwrap_or("");
    let version = if SUPPORTED.contains(&requested) { requested } else { SUPPORTED[0] };
    json!({
        "protocolVersion": version,
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "lum", "version": env!("CARGO_PKG_VERSION")},
    })
}

fn tools() -> Value {
    json!({"tools": [
        {
            "name": "search",
            "description": "Search indexed code and documents by meaning. Returns matching \
                            chunks with their file path and line range.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "What to look for, in plain language"},
                    "root": {"type": "string", "description": "Repository to search; registers and indexes it if new"},
                    "limit": {"type": "integer", "description": "Maximum results (default 10)"},
                    "exclude_tests": {"type": "boolean", "description": "Omit test files"}
                },
                "required": ["query"]
            }
        },
        {
            "name": "add_source",
            "description": "Register a directory and index it.",
            "inputSchema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }
        },
        {
            "name": "list_sources",
            "description": "List indexed directories with their document and chunk counts.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "status",
            "description": "Report index size, memory use, and any indexing failures.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    ]})
}

async fn call(config: &Config, params: &Value) -> Result<Value> {
    let name = params.get("name").and_then(Value::as_str).unwrap_or_default();
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
    let client = Client::connect(config).await?;

    let (text, structured) = match name {
        "search" => {
            let response: SearchResponse = client
                .call(json!({
                    "op": "search",
                    "q": arguments.get("query").and_then(Value::as_str).unwrap_or_default(),
                    "limit": arguments.get("limit").and_then(Value::as_u64).unwrap_or(10),
                    "root": arguments.get("root"),
                    "exclude_tests": arguments.get("exclude_tests").and_then(Value::as_bool).unwrap_or(false),
                    // An agent asking a question wants the answer that exists
                    // now, not to block on a first index it did not ask for.
                    "wait": arguments.get("root").is_some(),
                }))
                .await?;
            let rendered = if response.results.is_empty() {
                "No matches.".to_owned()
            } else {
                response
                    .results
                    .iter()
                    .map(|r| {
                        format!(
                            "{}:{}-{} (score {:.3})\n{}",
                            r.path, r.start_line, r.end_line, r.score, r.text
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n\n")
            };
            (rendered, serde_json::to_value(&response)?)
        }
        "add_source" => {
            let response: AddSourceResponse = client
                .call(json!({
                    "op": "add_source",
                    "uri": arguments.get("path").and_then(Value::as_str).unwrap_or_default(),
                    "wait": true,
                }))
                .await?;
            (
                format!(
                    "Indexed {} — {} documents, {} chunks.",
                    response.source.uri, response.source.documents, response.source.chunks
                ),
                serde_json::to_value(&response)?,
            )
        }
        "list_sources" => {
            let sources: Vec<Source> = client.call(json!({"op": "list_sources"})).await?;
            let rendered = if sources.is_empty() {
                "No sources registered.".to_owned()
            } else {
                sources
                    .iter()
                    .map(|s| format!("{} ({} documents, {} chunks)", s.uri, s.documents, s.chunks))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            (rendered, serde_json::to_value(&sources)?)
        }
        "status" => {
            let status: Status = client.call(json!({"op": "status"})).await?;
            (
                format!(
                    "{} — {} sources, {} documents, {} chunks, {} resident.",
                    status.state,
                    status.sources,
                    status.documents,
                    status.chunks,
                    crate::sys::human_bytes(status.rss_bytes)
                ),
                serde_json::to_value(&status)?,
            )
        }
        other => anyhow::bail!("unknown tool {other}"),
    };

    Ok(json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_known_protocol_version_is_echoed_back() {
        let result = initialize(&json!({"protocolVersion": "2024-11-05"}));
        assert_eq!(result["protocolVersion"], "2024-11-05");
    }

    #[test]
    fn an_unknown_protocol_version_gets_our_newest_rather_than_silence() {
        let result = initialize(&json!({"protocolVersion": "1999-01-01"}));
        assert_eq!(result["protocolVersion"], SUPPORTED[0]);
    }

    #[test]
    fn every_advertised_tool_has_a_schema() {
        let tools = tools();
        let list = tools["tools"].as_array().unwrap();
        assert_eq!(list.len(), 4);
        for tool in list {
            assert!(tool["name"].is_string(), "{tool}");
            assert!(tool["description"].is_string(), "{tool}");
            assert_eq!(tool["inputSchema"]["type"], "object", "{tool}");
        }
    }
}
