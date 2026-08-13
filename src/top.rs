//! `lum top` — a live view of what the daemon is doing.
//!
//! Deliberately dumb: it folds one event at a time into display state with a
//! single match on the kind, and the rates it shows are `indexed / elapsed`.
//! No semantics live here that are not already in the event stream, so
//! `lum top` and a `socat … | jq` pipeline can never disagree about what
//! happened.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::Result;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Paragraph};
use ratatui::Frame;
use serde_json::Value;

use crate::client::Client;
use crate::config::Config;
use crate::sys::human_bytes;
use crate::wire::Status;

const LOG_LINES: usize = 200;

#[derive(Default)]
struct Model {
    state: String,
    detail: String,
    sources: u64,
    documents: u64,
    chunks: u64,
    rss: u64,
    index_bytes: u64,
    pending_scans: u64,
    pending_documents: u64,
    active: String,
    stage: String,
    phase: String,
    done: u64,
    total: u64,
    unit: String,
    indexed: u64,
    failed: u64,
    indexed_chunks: u64,
    log: VecDeque<String>,
}

impl Model {
    fn apply(&mut self, event: &Value) {
        let kind = event.get("event").and_then(Value::as_str).unwrap_or_default();
        let text =
            |key: &str| event.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
        let number = |key: &str| event.get(key).and_then(Value::as_u64).unwrap_or_default();

        match kind {
            "state" => {
                self.state = text("state");
                self.detail = text("detail");
                self.note(format!("state: {} {}", self.state, self.detail));
            }
            "snapshot" => {
                self.state = text("state");
                self.sources = number("sources");
                self.documents = number("documents");
                self.chunks = number("total_chunks");
                self.rss = number("rss_bytes");
                self.pending_scans = number("pending_scans");
                self.pending_documents = number("pending_documents");
                self.active = text("path");
                self.stage = text("phase");
            }
            "progress" => {
                self.phase = text("phase");
                self.done = number("done");
                self.total = number("total");
                self.unit = text("unit");
            }
            "scan_started" => self.note(format!("scan started  {}", text("path"))),
            "scan_finished" => {
                self.phase.clear();
                self.done = 0;
                self.total = 0;
                self.note(format!(
                    "scan finished {}  indexed {} removed {} unchanged {} failed {} in {}ms",
                    text("path"),
                    number("indexed"),
                    number("removed"),
                    number("unchanged"),
                    number("failed"),
                    number("took_ms"),
                ));
            }
            "doc_indexed" => {
                self.indexed += 1;
                self.indexed_chunks += number("chunks");
                self.note(format!("indexed  {}  ({} chunks)", text("path"), number("chunks")));
            }
            "doc_deleted" => self.note(format!("deleted  {}", text("path"))),
            "doc_failed" => {
                self.failed += 1;
                self.note(format!("FAILED   {}  {}", text("path"), text("error")));
            }
            "scan_failed" => self.note(format!("SCAN FAILED  {}", text("error"))),
            "request" => self.note(format!("{:<14} {}ms", text("method"), number("took_ms"))),
            _ => {}
        }
    }

    fn note(&mut self, line: String) {
        if self.log.len() == LOG_LINES {
            self.log.pop_front();
        }
        self.log.push_back(line);
    }
}

pub async fn run(config: &Config) -> Result<()> {
    let client = Client::connect(config).await?;
    // Replay, unlike the notifier: a freshly opened view should show the
    // indexing already in flight rather than an empty screen until the next
    // tick.
    let mut events = client.subscribe(&[], true).await?;

    // Seed from a status call so the counters are populated before the first
    // snapshot tick rather than reading zero for two seconds.
    let mut model = Model::default();
    if let Ok(status) = client.call::<Status>(serde_json::json!({"op": "status"})).await {
        model.state = status.state;
        model.detail = status.detail;
        model.sources = status.sources as u64;
        model.documents = status.documents as u64;
        model.chunks = status.chunks as u64;
        model.rss = status.rss_bytes;
        model.index_bytes = status.index_bytes;
    }

    let mut terminal = ratatui::init();
    let started = Instant::now();
    let result = loop {
        if let Err(error) = terminal.draw(|frame| ui(frame, &model, started)) {
            break Err(error.into());
        }
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => model.apply(&event),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break Ok(()),
            },
            quit = tokio::task::spawn_blocking(pressed_quit) => {
                match quit {
                    Ok(Ok(true)) => break Ok(()),
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => break Err(error),
                    Err(error) => break Err(error.into()),
                }
            }
        }
    };
    ratatui::restore();
    result
}

/// Poll for a quit key. Bounded so the caller's select loop keeps turning and
/// events still render while nobody is typing.
fn pressed_quit() -> Result<bool> {
    use crossterm::event::{self, Event, KeyCode, KeyModifiers};
    if !event::poll(Duration::from_millis(200))? {
        return Ok(false);
    }
    match event::read()? {
        Event::Key(key) => Ok(matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))),
        _ => Ok(false),
    }
}

fn ui(frame: &mut Frame, model: &Model, started: Instant) {
    let areas =
        Layout::vertical([Constraint::Length(4), Constraint::Length(3), Constraint::Min(5)])
            .split(frame.area());

    let elapsed = started.elapsed().as_secs_f64().max(1.0);
    let per_minute = |count: u64| count as f64 / elapsed * 60.0;

    let header = vec![
        Line::from(vec![
            Span::styled("state ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                &model.state,
                Style::default()
                    .fg(match model.state.as_str() {
                        "ready" => Color::Green,
                        "failed" => Color::Red,
                        _ => Color::Yellow,
                    })
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("   memory ", Style::default().fg(Color::DarkGray)),
            Span::raw(human_bytes(model.rss)),
            Span::styled("   index ", Style::default().fg(Color::DarkGray)),
            Span::raw(human_bytes(model.index_bytes)),
        ]),
        Line::from(vec![
            Span::styled("sources ", Style::default().fg(Color::DarkGray)),
            Span::raw(model.sources.to_string()),
            Span::styled("   documents ", Style::default().fg(Color::DarkGray)),
            Span::raw(model.documents.to_string()),
            Span::styled("   chunks ", Style::default().fg(Color::DarkGray)),
            Span::raw(model.chunks.to_string()),
            Span::styled("   queued ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{} docs / {} scans", model.pending_documents, model.pending_scans)),
        ]),
        Line::from(vec![
            Span::styled("rate ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{:.1} docs/min  {:.0} chunks/min   failed {}",
                per_minute(model.indexed),
                per_minute(model.indexed_chunks),
                model.failed
            )),
        ]),
    ];
    frame.render_widget(
        Paragraph::new(header).block(Block::default().borders(Borders::ALL).title(" lum ")),
        areas[0],
    );

    let ratio = if model.total > 0 {
        (model.done as f64 / model.total as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let label = if model.total > 0 {
        format!("{} {}/{} {}", model.phase, model.done, model.total, model.unit)
    } else if !model.active.is_empty() {
        format!("{} {}", model.stage, model.active)
    } else {
        "idle".to_owned()
    };
    frame.render_widget(
        Gauge::default()
            .block(Block::default().borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::Cyan))
            .ratio(ratio)
            .label(label),
        areas[1],
    );

    let height = areas[2].height.saturating_sub(2) as usize;
    let lines: Vec<ListItem> = model
        .log
        .iter()
        .rev()
        .take(height)
        .map(|line| {
            let style = if line.starts_with("FAILED") || line.starts_with("SCAN FAILED") {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };
            ListItem::new(Line::styled(line.clone(), style))
        })
        .collect();
    frame.render_widget(
        List::new(lines)
            .block(Block::default().borders(Borders::ALL).title(" events — q to quit ")),
        areas[2],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn counters_come_from_snapshots_rather_than_being_accumulated() {
        // The daemon owns the totals; `top` must not keep a second running
        // count that can drift from them.
        let mut model = Model::default();
        model.apply(&json!({
            "event": "snapshot", "state": "ready",
            "sources": 1, "documents": 87, "total_chunks": 811, "rss_bytes": 45_000_000_u64
        }));
        assert_eq!((model.documents, model.chunks), (87, 811));
    }

    #[test]
    fn failures_are_recorded_and_marked() {
        let mut model = Model::default();
        model.apply(&json!({"event": "doc_failed", "path": "a.rs", "error": "boom"}));
        assert_eq!(model.failed, 1);
        assert!(model.log.back().unwrap().starts_with("FAILED"));
    }

    #[test]
    fn a_finished_scan_empties_the_gauge() {
        let mut model = Model::default();
        model.apply(&json!({"event": "progress", "phase": "embedding", "done": 3, "total": 9}));
        assert_eq!(model.total, 9);
        model.apply(&json!({"event": "scan_finished", "path": "/r", "indexed": 3}));
        assert_eq!(model.total, 0, "the gauge kept describing finished work");
    }

    #[test]
    fn the_log_is_bounded() {
        let mut model = Model::default();
        for _ in 0..LOG_LINES + 50 {
            model.apply(&json!({"event": "doc_indexed", "path": "a.rs", "chunks": 1}));
        }
        assert_eq!(model.log.len(), LOG_LINES);
        assert_eq!(model.indexed, (LOG_LINES + 50) as u64, "counts must outlive the log window");
    }
}
