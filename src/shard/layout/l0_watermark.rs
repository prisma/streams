//! The shard log's L0 watermark (the owner's fail-safe of 2026-10-30): a
//! flush into a tree holding `l0_max_ssts` L0 SSTs stalls every write of
//! the shard (SlateDB), settles and the control plane included. Two marks
//! below that line keep the shard clear of it:
//!
//! - **yield** (half the line): the queue collector starts no pass, so the
//!   scheduler has the compaction slot to itself and drains the L0s;
//! - **high** (three quarters of the line): appends and queue sends get a
//!   retryable 503 `shard_l0_backpressure` with a `Retry-After`; reads,
//!   consumer pulls, settles, deletes and the control plane stay admitted.
//!   The shed releases at **low** (half the line): hysteresis.
//!
//! The marks are derived from the shard log's `l0_max_ssts` (the process's
//! line, `backpressure::l0_line`, set at boot; an engine may override it in
//! tests). The decision is precomputed: each engine's tick reads its
//! in-memory manifest (the most L0s any tree holds) once a second and
//! stores the count and the shed flag; the request path reads one atomic.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

#[cfg(test)]
mod tests;

/// How often an engine reads its manifest's L0 count.
pub(crate) const TICK: Duration = Duration::from_secs(1);

/// The seconds a shed write is told to wait (`Retry-After`).
pub(crate) const RETRY_AFTER_SECS: u64 = 2;

/// The marks of a line of `line` L0s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Marks {
    pub(crate) line: usize,
    /// The collector yields at or above it.
    pub(crate) yield_at: usize,
    /// The shed engages at or above it.
    pub(crate) high: usize,
    /// The shed releases below it.
    pub(crate) low: usize,
}

impl Marks {
    pub(crate) fn of(line: usize) -> Self {
        let line = line.max(4);
        Self {
            line,
            yield_at: line / 2,
            high: line * 3 / 4,
            low: line / 2,
        }
    }
}

/// The shed's next state from `count` L0s: engage at `high`, hold until the
/// count is back below `low`.
pub(crate) fn next_shed(shed: bool, count: usize, m: Marks) -> bool {
    if shed {
        count >= m.low
    } else {
        count >= m.high
    }
}

/// One engine's watermark state (part of `LayoutState`).
#[derive(Default)]
pub(crate) struct L0Watermark {
    /// An engine's own line (tests); 0 reads the process line.
    line: AtomicUsize,
    count: AtomicUsize,
    shed: AtomicBool,
    /// Whether the tick ever read the manifest (no decision before it).
    read: AtomicBool,
    pub(crate) engages: AtomicU64,
    pub(crate) sheds: AtomicU64,
    pub(crate) yields: AtomicU64,
    pub(crate) max_seen: AtomicUsize,
}

impl L0Watermark {
    pub(crate) fn marks(&self) -> Marks {
        let own = self.line.load(Ordering::Relaxed);
        Marks::of(if own == 0 {
            crate::backpressure::l0_line()
        } else {
            own
        })
    }

    /// Tests: this engine's line.
    #[cfg(test)]
    pub(crate) fn set_line(&self, line: usize) {
        self.line.store(line, Ordering::Relaxed);
    }

    /// Record a tick's count and re-decide the shed.
    pub(crate) fn observe(&self, count: usize) {
        let m = self.marks();
        self.count.store(count, Ordering::Relaxed);
        self.max_seen.fetch_max(count, Ordering::Relaxed);
        self.read.store(true, Ordering::Relaxed);
        let was = self.shed.load(Ordering::Relaxed);
        let now = next_shed(was, count, m);
        if now != was {
            self.shed.store(now, Ordering::Relaxed);
            if now {
                self.engages.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    l0 = count,
                    high = m.high,
                    line = m.line,
                    "L0 watermark shed ENGAGED: appends and queue sends answer a retryable 503 until the L0s drain below {}",
                    m.low
                );
            } else {
                tracing::info!(l0 = count, low = m.low, "L0 watermark shed released");
            }
        }
    }

    /// Whether appends and queue sends are shed now.
    pub(crate) fn shedding(&self) -> bool {
        self.shed.load(Ordering::Relaxed)
    }

    /// Whether the queue collector yields now (and counts it).
    pub(crate) fn collector_yields(&self) -> bool {
        let yields = self.read.load(Ordering::Relaxed)
            && self.count.load(Ordering::Relaxed) >= self.marks().yield_at;
        if yields {
            self.yields.fetch_add(1, Ordering::Relaxed);
        }
        yields
    }

    pub(crate) fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let m = self.marks();
        let n = |a: &AtomicU64| a.load(Ordering::Relaxed);
        serde_json::json!({
            "l0": self.count(),
            "max_seen": self.max_seen.load(Ordering::Relaxed),
            "line": m.line,
            "yield_at": m.yield_at,
            "high": m.high,
            "low": m.low,
            "shedding": self.shedding(),
            "engages": n(&self.engages),
            "sheds": n(&self.sheds),
            "collector_yields": n(&self.yields),
        })
    }
}

/// One engine's watermark as the operator surfaces read it.
pub(crate) struct L0Gauge {
    pub(crate) count: usize,
    pub(crate) shedding: bool,
    /// Writes shed since the engine opened.
    pub(crate) sheds: u64,
    /// Collector passes not started since the engine opened.
    pub(crate) yields: u64,
}

/// The most L0 SSTs any tree of `manifest` holds (the default tree and
/// every segment's).
pub(crate) fn l0_count(manifest: &slatedb::VersionedManifest) -> usize {
    manifest
        .segments()
        .iter()
        .map(|s| s.l0().len())
        .chain(std::iter::once(manifest.l0().len()))
        .max()
        .unwrap_or(0)
}

impl crate::shard::ShardEngine {
    /// The watermark's tick: the manifest's L0 count once a second until
    /// the engine closes.
    pub(in crate::shard) async fn l0_watch_loop(self: std::sync::Arc<Self>) {
        loop {
            self.layout.l0.observe(l0_count(&self.db.manifest()));
            tokio::select! {
                () = self.closed() => return,
                () = tokio::time::sleep(TICK) => {}
            }
        }
    }

    /// Tests: this engine's L0 line, in place of the process's.
    #[cfg(test)]
    pub(crate) fn set_l0_line(&self, line: usize) {
        self.layout.l0.set_line(line);
    }

    /// Tests: the marks this engine runs with.
    #[cfg(test)]
    pub(crate) fn l0_marks(&self) -> Marks {
        self.layout.l0.marks()
    }

    /// Whether this shard sheds a write (an append or a queue send) now:
    /// the seconds it is told to wait when it does (counted).
    pub(crate) fn shed_l0_write(&self) -> Option<u64> {
        if !self.layout.l0.shedding() {
            return None;
        }
        self.layout.l0.sheds.fetch_add(1, Ordering::Relaxed);
        Some(RETRY_AFTER_SECS)
    }

    /// The watermark's gauges (`GET /v1/debug/timings`, each shard's `l0`).
    pub(crate) fn l0_json(&self) -> serde_json::Value {
        self.layout.l0.json()
    }

    /// The watermark's gauge (`/operator/data.json`, `/v1/debug/load`).
    pub(crate) fn l0_gauge(&self) -> L0Gauge {
        let l0 = &self.layout.l0;
        L0Gauge {
            count: l0.count(),
            shedding: l0.shedding(),
            sheds: l0.sheds.load(Ordering::Relaxed),
            yields: l0.yields.load(Ordering::Relaxed),
        }
    }
}
