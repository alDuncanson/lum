//! Terminal output: search results, and the progress line for commands that
//! have to wait.
//!
//! Two rules keep the progress line from breaking anything:
//!
//! - It writes to stderr, never stdout. `--json` and `--jsonl` are parsed by
//!   other programs, and a spinner in that stream would corrupt it.
//! - It draws only to a terminal. Redirected into a file or a pipe, a progress
//!   bar is noise, and `\r` animation is worse than noise.

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::broadcast;

use crate::client::Client;
use crate::sys::human_bytes;
use crate::wire::{SearchResult, Status};

const FRAMES: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const BAR_WIDTH: usize = 14;

/// What the event stream has said so far, folded into one line.
#[derive(Default)]
struct Activity {
    state: String,
    phase: String,
    done: u64,
    total: u64,
    unit: String,
    indexed: u64,
    failed: u64,
    pending: u64,
}

impl Activity {
    fn apply(&mut self, event: &Value) {
        let kind = event.get("event").and_then(Value::as_str).unwrap_or_default();
        let text =
            |key: &str| event.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
        let number = |key: &str| event.get(key).and_then(Value::as_u64).unwrap_or_default();

        match kind {
            "state" | "snapshot" => {
                let state = text("state");
                if !state.is_empty() {
                    self.state = state;
                }
                if kind == "snapshot" {
                    self.pending = number("pending_documents");
                }
            }
            "progress" => {
                self.phase = text("phase");
                self.done = number("done");
                self.total = number("total");
                self.unit = text("unit");
            }
            "doc_indexed" => self.indexed += 1,
            "doc_failed" => self.failed += 1,
            "scan_finished" => {
                // Whatever is still on screen describes work that has finished.
                self.phase.clear();
                self.done = 0;
                self.total = 0;
                self.pending = 0;
            }
            _ => {}
        }
    }

    /// The line, or empty when there is nothing worth saying.
    fn line(&self) -> String {
        let mut text = if self.state == "downloading-model" {
            // The one that takes minutes on a first run, and the one most
            // likely to be read as a hang.
            "downloading the embedding model (~70 MB, first run)".to_owned()
        } else if !self.phase.is_empty() && self.total > 0 {
            format!(
                "{} {} {}/{} {}",
                self.phase,
                bar(self.done, self.total),
                self.done,
                self.total,
                self.unit
            )
        } else if !self.phase.is_empty() {
            self.phase.clone()
        } else if self.pending > 0 {
            format!("indexing {}", plural(self.pending, "file"))
        } else if self.indexed > 0 {
            format!("indexed {}", plural(self.indexed, "file"))
        } else if self.state == "starting" {
            "starting".to_owned()
        } else {
            return String::new();
        };
        if self.failed > 0 {
            text.push_str(&format!(" · {} failed", plural(self.failed, "file")));
        }
        text
    }
}

fn bar(done: u64, total: u64) -> String {
    if total == 0 {
        return String::new();
    }
    let filled = ((done as f64 / total as f64) * BAR_WIDTH as f64) as usize;
    let filled = filled.min(BAR_WIDTH);
    format!("▕{}{}▏", "█".repeat(filled), "░".repeat(BAR_WIDTH - filled))
}

pub fn plural(count: u64, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// Draws lum's activity on stderr until the returned handle is stopped.
pub struct Progress {
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Progress {
    /// Safe to call when nothing will be drawn; stopping is idempotent.
    pub async fn start(client: &Client, quiet: bool) -> Self {
        if quiet || !enabled() {
            return Self { stop: Arc::new(AtomicBool::new(true)), task: None };
        }
        // No replay: this reports on work about to happen, and announcing what
        // finished before the command started would be a lie about it.
        let Ok(events) = client
            .subscribe(
                &["state", "snapshot", "progress", "doc_indexed", "doc_failed", "scan_finished"],
                false,
            )
            .await
        else {
            // Progress is advisory. A command that cannot subscribe should
            // still run, silently, rather than fail over the absence of a
            // spinner.
            return Self { stop: Arc::new(AtomicBool::new(true)), task: None };
        };

        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(draw(events, Arc::clone(&stop)));
        Self { stop, task: Some(task) }
    }

    pub async fn finish(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

async fn draw(mut events: broadcast::Receiver<Value>, stop: Arc<AtomicBool>) {
    let mut activity = Activity::default();
    let mut frame = 0;
    let mut drawn = false;
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => activity.apply(&event),
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = ticker.tick() => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let line = activity.line();
                if !line.is_empty() {
                    // \r to the start, then erase to end of line: without the
                    // erase a shorter line leaves the tail of a longer one.
                    eprint!("\r\u{1b}[K{} {}", FRAMES[frame], line);
                    let _ = std::io::stderr().flush();
                    drawn = true;
                    frame = (frame + 1) % FRAMES.len();
                }
            }
        }
    }
    if drawn {
        eprint!("\r\u{1b}[K");
        let _ = std::io::stderr().flush();
    }
}

fn enabled() -> bool {
    if !std::io::stderr().is_terminal() {
        return false;
    }
    // TERM=dumb means "no cursor control", which is exactly what this needs.
    !matches!(std::env::var("TERM").as_deref(), Ok("") | Ok("dumb") | Err(_))
}

// ---- results ----

pub fn print_human(results: &[SearchResult]) {
    if results.is_empty() {
        println!("no results");
        return;
    }
    for (rank, result) in results.iter().enumerate() {
        let location = if result.start_line > 0 {
            format!("{}:{}", result.uri, result.start_line)
        } else {
            result.uri.clone()
        };
        println!("{:2}. {:.3}  {location} (chunk {})", rank + 1, result.score, result.chunk_index);
        println!("    {}\n", snippet(&result.text, 240));
    }
}

/// Trim chunk text to one displayable line.
fn snippet(text: &str, max: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max {
        return collapsed;
    }
    // Cut on a character boundary: chunk text is source, and source is full of
    // comments containing characters a byte slice would cut in half.
    let cut: String = collapsed.chars().take(max).collect();
    format!("{cut}…")
}

pub fn print_status(status: &Status) {
    println!("state:       {}", status.state);
    if !status.detail.is_empty() && status.detail != status.state {
        println!("             {}", status.detail);
    }
    println!("sources:     {}", status.sources);
    println!("documents:   {}", status.documents);
    println!("chunks:      {}", status.chunks);
    println!("memory:      {}", human_bytes(status.rss_bytes));
    println!(
        "index:       {} on disk, {} of vectors resident",
        human_bytes(status.index_bytes),
        human_bytes(status.vector_memory_bytes)
    );
    if status.ingest_session {
        println!("             indexing session loaded; released when indexing goes idle");
    }
    println!("uptime:      {}", duration(status.uptime_seconds));

    let mut activity = Vec::new();
    if !status.active_document.is_empty() {
        let stage =
            if status.active_stage.is_empty() { "working on" } else { &status.active_stage };
        activity.push(format!("{stage} {}", status.active_document));
    }
    if status.pending_documents > 0 {
        activity.push(format!("{} queued", plural(status.pending_documents as u64, "document")));
    }
    if status.pending_scans > 0 {
        activity.push(format!("{} queued", plural(status.pending_scans as u64, "scan")));
    }
    if !activity.is_empty() {
        println!("indexing:    {}", activity.join(", "));
    }
    if !status.failures.is_empty() {
        println!("failures:    {}", status.failures.len());
        for failure in &status.failures {
            println!("  {} (attempts: {}): {}", failure.uri, failure.attempts, failure.error);
        }
    }
}

fn duration(seconds: u64) -> String {
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3599 => format!("{}m{}s", seconds / 60, seconds % 60),
        _ => format!("{}h{}m", seconds / 3600, (seconds % 3600) / 60),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_model_download_outranks_everything_else() {
        // It is the longest wait and the one most likely to be read as a hang,
        // so it must not be hidden behind a chunk counter.
        let mut activity = Activity::default();
        activity.apply(&json!({"event": "state", "state": "downloading-model"}));
        activity.apply(&json!({"event": "progress", "phase": "embedding", "done": 1, "total": 2}));
        assert!(activity.line().starts_with("downloading the embedding model"));
    }

    #[test]
    fn progress_renders_a_bar_with_counts() {
        let mut activity = Activity::default();
        activity.apply(&json!({"event": "state", "state": "ready"}));
        activity.apply(&json!({
            "event": "progress", "phase": "embedding",
            "done": 48, "total": 96, "unit": "chunks"
        }));
        let line = activity.line();
        assert!(line.contains("embedding"), "{line}");
        assert!(line.contains("48/96 chunks"), "{line}");
        assert!(line.contains('█') && line.contains('░'), "{line}");
    }

    #[test]
    fn a_finished_scan_clears_the_bar_it_was_drawing() {
        // Otherwise the last frame sits there describing work that is over.
        let mut activity = Activity::default();
        activity.apply(&json!({"event": "progress", "phase": "embedding", "done": 5, "total": 5}));
        activity.apply(&json!({"event": "scan_finished", "indexed": 5}));
        assert!(!activity.line().contains("embedding"));
    }

    #[test]
    fn failures_are_appended_rather_than_replacing_the_line() {
        let mut activity = Activity::default();
        activity.apply(&json!({"event": "progress", "phase": "embedding", "done": 1, "total": 4, "unit": "chunks"}));
        activity.apply(&json!({"event": "doc_failed"}));
        let line = activity.line();
        assert!(line.contains("embedding"), "{line}");
        assert!(line.ends_with("· 1 file failed"), "{line}");
    }

    #[test]
    fn nothing_to_say_produces_no_line() {
        // An idle daemon must not leave a spinner on screen forever.
        assert_eq!(Activity::default().line(), "");
    }

    #[test]
    fn the_bar_never_overflows_its_width() {
        // A done count above total is not supposed to happen, but a panic or a
        // 200-character line would be a bad way to find out.
        assert_eq!(bar(200, 100).chars().filter(|c| *c == '█').count(), BAR_WIDTH);
    }

    #[test]
    fn snippets_cut_on_character_boundaries() {
        let text = "é".repeat(400);
        let cut = snippet(&text, 240);
        assert_eq!(cut.chars().count(), 241, "240 characters plus the ellipsis");
        assert!(cut.ends_with('…'));
    }

    #[test]
    fn snippets_collapse_whitespace_to_one_line() {
        assert_eq!(snippet("fn main() {\n    let x = 1;\n}", 240), "fn main() { let x = 1; }");
    }
}
