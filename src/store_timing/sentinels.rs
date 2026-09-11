//! Process-lifetime scheduler sentinels; never retain runtime service state.
use super::observations::{now_ms, percentile_index};
use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Run-13 discriminator: excursions are class-agnostic at low CPU and low
/// outbound concurrency (v18 killed the burst/handshake theory), so either
/// the vCPU itself stalls (host steal / descheduling) or the network path
/// queues. Two 10 ms-cadence sentinels tell them apart: a raw OS thread
/// (immune to our event loop) and a tokio task (subject to it), plus
/// /proc/stat steal ticks. Drift spikes on BOTH sentinels co-timed with
/// excursions → the VM stalled; tokio-only → our loop starved; neither →
/// the network path is the queue.
struct DriftRings {
    thread: Mutex<VecDeque<(u64, u32)>>, // (ts_ms, drift_us)
    tokio: Mutex<VecDeque<(u64, u32)>>,
    steal: Mutex<VecDeque<(u64, u64, u64)>>, // (ts_ms, steal_ticks, total_ticks)
}

fn drift() -> &'static DriftRings {
    static D: OnceLock<DriftRings> = OnceLock::new();
    D.get_or_init(|| DriftRings {
        thread: Mutex::new(VecDeque::with_capacity(4096)),
        tokio: Mutex::new(VecDeque::with_capacity(4096)),
        steal: Mutex::new(VecDeque::with_capacity(64)),
    })
}

#[expect(
    clippy::unwrap_used,
    reason = "process scheduler drift rings; poison may follow an interrupted sample update; recovery would publish partial scheduler measurements"
)]
fn push_drift(ring: &Mutex<VecDeque<(u64, u32)>>, drift_us: u32) {
    let mut r = ring.lock().unwrap();
    if r.len() >= 4096 {
        r.pop_front();
    }
    r.push_back((now_ms(), drift_us));
}

/// Read (steal_ticks, total_ticks) from /proc/stat's aggregate cpu line.
fn read_steal() -> Option<(u64, u64)> {
    let s = std::fs::read_to_string("/proc/stat").ok()?;
    let line = s.lines().next()?;
    let f: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    if f.len() < 8 {
        return None;
    }
    Some((f[7], f.iter().sum()))
}

/// Spawn both sentinels; call once at startup.
/// WP-15 / PR 6.1-A: process-lifetime INSTRUMENTATION — one OS thread
/// and one runtime task measuring scheduler drift for the whole
/// process. The documented exception to task supervision: it measures
/// the process, not a runtime, holds no runtime state and has nothing
/// to release, so it is not a child of any supervisor.
#[expect(
    clippy::disallowed_methods,
    reason = "process-lifetime scheduler instrumentation; the existing OS thread and Tokio sentinel retain no service state; attaching them to a runtime supervisor would measure a different lifetime"
)]
pub(crate) fn spawn_sentinels() {
    std::thread::Builder::new()
        .name("drift-sentinel".into())
        .spawn(|| {
            loop {
                let t0 = Instant::now();
                std::thread::sleep(std::time::Duration::from_millis(10));
                let over = t0.elapsed().as_micros().saturating_sub(10_000);
                push_drift(&drift().thread, u32::try_from(over).unwrap_or(u32::MAX));
            }
        })
        .ok();
    tokio::spawn(async {
        let mut ticks: u32 = 0;
        loop {
            let t0 = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let over = t0.elapsed().as_micros().saturating_sub(10_000);
            push_drift(&drift().tokio, u32::try_from(over).unwrap_or(u32::MAX));
            ticks += 1;
            if ticks.is_multiple_of(100) {
                // ~1 s cadence: cumulative steal/total ticks for window deltas
                sample_steal();
            }
        }
    });
}

#[expect(
    clippy::unwrap_used,
    reason = "process scheduler drift rings; poison may follow an interrupted sample update; recovery would publish partial scheduler measurements"
)]
fn drift_stats(ring: &Mutex<VecDeque<(u64, u32)>>, cutoff: u64) -> serde_json::Value {
    let mut v: Vec<u32> = ring
        .lock()
        .unwrap()
        .iter()
        .filter(|(ts, _)| *ts >= cutoff)
        .map(|(_, d)| *d)
        .collect();
    v.sort_unstable();
    let over50 = v.iter().filter(|d| **d >= 50_000).count();
    let idx99 = percentile_index(v.len(), 99);
    serde_json::json!({
        "n": v.len(),
        "p50_us": v.get(v.len() / 2).copied().unwrap_or(0),
        "p99_us": v.get(idx99).copied().unwrap_or(0),
        "max_us": v.last().copied().unwrap_or(0),
        "over_50ms": over50,
    })
}

pub(super) struct Snapshot {
    pub(super) thread: serde_json::Value,
    pub(super) tokio: serde_json::Value,
    pub(super) steal_pct: f64,
}

#[expect(
    clippy::unwrap_used,
    reason = "process scheduler drift rings; poison may follow an interrupted sample update; recovery would publish partial scheduler measurements"
)]
pub(super) fn snapshot(cutoff: u64) -> Snapshot {
    // steal% over the window: delta of the two cumulative tick samples
    // bracketing the cutoff
    let steal_pct = {
        let r = drift().steal.lock().unwrap();
        let inside: Vec<_> = r.iter().filter(|(ts, _, _)| *ts >= cutoff).collect();
        match (inside.first(), inside.last()) {
            (Some((_, s0, t0)), Some((_, s1, t1))) if t1 > t0 => {
                ((s1 - s0) as f64 / (t1 - t0) as f64 * 1000.0).round() / 10.0
            }
            _ => -1.0,
        }
    };
    Snapshot {
        thread: drift_stats(&drift().thread, cutoff),
        tokio: drift_stats(&drift().tokio, cutoff),
        steal_pct,
    }
}

#[expect(
    clippy::unwrap_used,
    reason = "process scheduler steal ring; poison may follow an interrupted sample update; recovery would publish partial scheduler measurements"
)]
fn sample_steal() {
    if let Some((st, tot)) = read_steal() {
        let mut r = drift().steal.lock().unwrap();
        if r.len() >= 64 {
            r.pop_front();
        }
        r.push_back((now_ms(), st, tot));
    }
}

#[cfg(test)]
mod tests;
