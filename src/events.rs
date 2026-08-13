//! The one observability contract: a broadcast bus with a ring buffer.
//!
//! Every consumer reads the same stream. The CLI spinner, the Neovim progress
//! bridge, the Neovim notifier, and `lum top` differ only in which kinds they
//! subscribe to and what they draw. Nothing has a privileged side channel, and
//! nothing computes a number the stream does not already carry — `top`'s
//! docs/min is `indexed / elapsed`, which is arithmetic a `jq` user watching
//! the raw stream could do.
//!
//! One flat schema, discriminated by `event`, rather than a per-kind type.
//! That is deliberate: consumers are a Lua table lookup and a `match`, and a
//! union of twelve shapes would make both worse for no gain in a payload
//! nobody stores.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::Serialize;
use tokio::sync::broadcast;

/// How much history a late subscriber gets. `lum top` opened after an index
/// started should show that index; it does not need yesterday's.
const RING_CAPACITY: usize = 512;

/// Bounded, so a subscriber that stops reading cannot make the indexer wait
/// for it. Lagging drops the oldest, which for progress is the right loss:
/// a stale bar for a moment beats trading the work for a report about it.
const CHANNEL_CAPACITY: usize = 256;

/// One event. Fields absent from a given kind are omitted from the wire form,
/// so a `progress` line carries no nulls for the snapshot counters.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Event {
    pub event: &'static str,
    pub seq: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Display path — relative to the source root, which is what a person
    /// calls the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<&'static str>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub took_ms: Option<u64>,

    // scan_finished
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unchanged: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued: Option<u64>,

    // snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_scans: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_chunks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<&'static str>,
}

impl Event {
    pub fn new(kind: &'static str) -> Self {
        Self { event: kind, ..Default::default() }
    }

    pub fn state(state: &'static str, detail: impl Into<String>) -> Self {
        Self { state: Some(state), detail: Some(detail.into()), ..Self::new("state") }
    }

    pub fn progress(
        phase: &'static str,
        done: u64,
        total: u64,
        unit: &'static str,
        path: Option<String>,
    ) -> Self {
        Self {
            phase: Some(phase),
            done: Some(done),
            total: Some(total),
            unit: Some(unit),
            path,
            ..Self::new("progress")
        }
    }
}

pub struct Bus {
    sender: broadcast::Sender<Event>,
    ring: Mutex<VecDeque<Event>>,
    seq: AtomicU64,
}

impl Bus {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            sender,
            ring: Mutex::new(VecDeque::with_capacity(RING_CAPACITY)),
            seq: AtomicU64::new(0),
        }
    }

    /// Stamp and publish. Never fails and never blocks: with no subscribers
    /// `send` returns an error that is not a problem worth handling, and the
    /// channel is bounded so a slow reader lags rather than backpressures.
    pub fn publish(&self, mut event: Event) {
        event.seq = self.seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut ring = self.ring.lock().expect("event ring poisoned");
            if ring.len() == RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(event.clone());
        }
        let _ = self.sender.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    pub fn backlog(&self) -> Vec<Event> {
        self.ring.lock().expect("event ring poisoned").iter().cloned().collect()
    }
}

impl Default for Bus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_fields_stay_off_the_wire() {
        // A progress line carrying twenty nulls would triple the bytes of the
        // most frequent event for nothing.
        let line =
            serde_json::to_string(&Event::progress("embedding", 4, 8, "chunks", None)).unwrap();
        assert!(!line.contains("null"), "{line}");
        assert!(line.contains("\"event\":\"progress\""), "{line}");
        assert!(!line.contains("pending_scans"), "{line}");
    }

    #[test]
    fn sequence_numbers_are_monotonic_across_kinds() {
        let bus = Bus::new();
        bus.publish(Event::state("ready", "warm"));
        bus.publish(Event::progress("embedding", 1, 2, "chunks", None));
        let seqs: Vec<u64> = bus.backlog().iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![0, 1]);
    }

    #[test]
    fn the_ring_keeps_the_most_recent_and_drops_the_oldest() {
        let bus = Bus::new();
        for _ in 0..RING_CAPACITY + 10 {
            bus.publish(Event::new("doc_indexed"));
        }
        let backlog = bus.backlog();
        assert_eq!(backlog.len(), RING_CAPACITY);
        assert_eq!(backlog.first().unwrap().seq, 10, "the ring dropped from the wrong end");
    }

    #[test]
    fn publishing_with_no_subscribers_is_not_an_error() {
        // Every publish site is fire-and-forget; a daemon nobody is watching
        // must not accumulate failures.
        let bus = Bus::new();
        bus.publish(Event::new("scan_started"));
        assert_eq!(bus.backlog().len(), 1);
    }

    #[test]
    fn a_late_subscriber_sees_only_what_comes_next() {
        let bus = Bus::new();
        bus.publish(Event::new("before"));
        let mut receiver = bus.subscribe();
        bus.publish(Event::new("after"));
        assert_eq!(receiver.try_recv().unwrap().event, "after");
        assert!(receiver.try_recv().is_err(), "the backlog leaked into the live stream");
    }
}
