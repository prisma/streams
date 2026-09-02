//! Admission control (WP-02 / PR 6-B): the instance's request-admission
//! gates — global in-flight, the pre-auth survival bound, RSS pressure,
//! per-stream concurrency, the live-subscription budget, maintenance
//! backpressure — and every counter they keep, extracted from
//! `http::AppState`. Counters are PRIVATE: request paths hold RAII
//! tickets and ask typed questions; operator surfaces read one
//! immutable snapshot. One controller per runtime, no statics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::backpressure::GlobalLatch;

/// Bound on distinct streams tracked by the per-stream admission map:
/// the map stays proportional to concurrently-active streams and can
/// never grow without bound (fail OPEN at the bound, never leak).
const STREAM_INFLIGHT_MAX_TRACKED: usize = 65_536;

/// The pre-auth survival bound is this many times the ordinary cap: a
/// process at 4x its admission cap is defending its sockets, not
/// answering capacity questions (Round-13 review).
const SURVIVAL_MULTIPLIER: i64 = 4;

/// The live-subscription budget: the EFFECTIVE cap the runtime enforces
/// (resolved against the descriptor ceiling at boot) and the CONFIGURED
/// cap it came from — exported side by side so a clamp is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionCapacity {
    pub effective: u64,
    pub configured: u64,
}

/// What a controller is built from. Bootstrap takes these from the
/// proven configuration and the capacity preflight; a rig passes its
/// own — never a configuration graph, never another owner.
#[derive(Debug, Clone)]
pub struct AdmissionKnobs {
    /// Ordinary in-flight cap for writes (0 = off).
    pub max_inflight: i64,
    /// Per-stream concurrency cap (0 = off).
    pub per_stream_cap: i64,
    /// RSS write-shed line in MiB (0 = off).
    pub rss_shed_mb: u64,
    /// Per-project memory-pressure threshold (0 = off).
    pub project_memory_pressure_bytes: u64,
    /// Hysteresis release point, percent of the threshold (clamped 1..=100).
    pub project_memory_release_pct: u64,
    pub subscriptions: SubscriptionCapacity,
    /// Per-RECORD payload ceiling, independent of the request-body
    /// ceiling (0 = unlimited, the dev posture).
    pub record_ceiling_bytes: usize,
}

/// A write refused by the global gates. The transport decides the wire
/// shape (and the tarpit); the controller only decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteRefusal {
    /// Over the ordinary in-flight cap.
    Overloaded,
    /// Sampled RSS plus reserved absorber bytes is over the shed line.
    MemoryPressure,
}

/// The stream is at its concurrency cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRefusal;

/// The instance is at its live-subscription cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionRefusal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShedSnapshot {
    /// Sum of the two global mechanisms (inflight + rss) plus survival.
    pub total: u64,
    pub inflight: u64,
    pub survival: u64,
    pub rss: u64,
    pub stream: u64,
    pub wedge: u64,
}

/// An immutable reading of the controller for operator surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    pub inflight: i64,
    pub inflight_peak: i64,
    pub max_inflight: i64,
    pub per_stream_cap: i64,
    pub streams_tracked: usize,
    pub shed: ShedSnapshot,
    pub rss_mb: u64,
    pub rss_shed_mb: u64,
    pub project_memory_pressure_bytes: u64,
    pub sse_connections: u64,
    pub sse_effective_max: u64,
    pub sse_configured_max: u64,
    pub fleet_ops: u64,
}

#[derive(Clone)]
pub struct AdmissionController {
    inner: Arc<Inner>,
}

struct Inner {
    inflight: AtomicI64,
    inflight_peak: AtomicI64,
    /// Runtime-tunable (operator endpoint) — hence atomic, not a knob.
    max_inflight: AtomicI64,
    per_stream_cap: i64,
    streams: Mutex<HashMap<[u8; 16], i64>>,
    rss_shed_mb: u64,
    /// Sampled RSS in MiB (the bootstrap sampler writes it every 250 ms;
    /// a /proc read per request would be silly).
    rss_mb: AtomicU64,
    project_memory_pressure_bytes: AtomicU64,
    project_memory_release_pct: u64,
    shed_total: AtomicU64,
    shed_inflight: AtomicU64,
    shed_survival: AtomicU64,
    shed_rss: AtomicU64,
    shed_stream: AtomicU64,
    shed_wedge: AtomicU64,
    subscriptions: SubscriptionCapacity,
    sse_connections: AtomicU64,
    maintenance: GlobalLatch,
    /// Successful /v1/stream/* requests, the fleet load vector (§4.2).
    fleet_ops: AtomicU64,
    record_ceiling: std::sync::atomic::AtomicUsize,
}

/// RAII in-flight ticket: the count drops on response AND on
/// cancel/panic (the guard rides the handler's future).
pub struct InflightTicket {
    ctl: AdmissionController,
    current: i64,
}

impl InflightTicket {
    /// The in-flight count as of this admission (including this one).
    pub fn current(&self) -> i64 {
        self.current
    }
}

impl Drop for InflightTicket {
    fn drop(&mut self) {
        self.ctl.inner.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// RAII per-stream slot; entries leave the map at zero.
pub struct StreamSlot {
    ctl: AdmissionController,
    hash: [u8; 16],
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        let mut m = self.ctl.inner.streams.lock().unwrap();
        if let Some(v) = m.get_mut(&self.hash) {
            *v -= 1;
            if *v <= 0 {
                m.remove(&self.hash);
            }
        }
    }
}

/// RAII live-subscription slot (#267): held by the response stream's
/// body — dropping the body releases it.
pub struct SubscriptionTicket {
    ctl: AdmissionController,
}

impl Drop for SubscriptionTicket {
    fn drop(&mut self) {
        self.ctl
            .inner
            .sse_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl AdmissionController {
    pub fn new(knobs: AdmissionKnobs) -> Self {
        Self {
            inner: Arc::new(Inner {
                inflight: AtomicI64::new(0),
                inflight_peak: AtomicI64::new(0),
                max_inflight: AtomicI64::new(knobs.max_inflight),
                per_stream_cap: knobs.per_stream_cap,
                streams: Mutex::new(HashMap::new()),
                rss_shed_mb: knobs.rss_shed_mb,
                rss_mb: AtomicU64::new(0),
                project_memory_pressure_bytes: AtomicU64::new(knobs.project_memory_pressure_bytes),
                project_memory_release_pct: knobs.project_memory_release_pct.clamp(1, 100),
                shed_total: AtomicU64::new(0),
                shed_inflight: AtomicU64::new(0),
                shed_survival: AtomicU64::new(0),
                shed_rss: AtomicU64::new(0),
                shed_stream: AtomicU64::new(0),
                shed_wedge: AtomicU64::new(0),
                subscriptions: knobs.subscriptions,
                sse_connections: AtomicU64::new(0),
                maintenance: GlobalLatch::new(),
                fleet_ops: AtomicU64::new(0),
                record_ceiling: std::sync::atomic::AtomicUsize::new(knobs.record_ceiling_bytes),
            }),
        }
    }

    // ---- global in-flight -------------------------------------------

    /// Count a request in (and record the peak). Held for the whole
    /// request; dropped on every exit path.
    pub fn enter(&self) -> InflightTicket {
        let current = self.inner.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner
            .inflight_peak
            .fetch_max(current, Ordering::Relaxed);
        InflightTicket {
            ctl: self.clone(),
            current,
        }
    }

    /// The PRE-AUTH survival bound: only an absolute multiple of the
    /// ordinary cap, only on stream paths, no capacity answer — a
    /// process this far over its cap is defending its sockets. Counts
    /// the shed when it refuses.
    pub fn survival_refused(&self, current: i64, stream_path: bool) -> bool {
        let cap = self.max_inflight();
        let refuse = cap > 0 && stream_path && current > cap.saturating_mul(SURVIVAL_MULTIPLIER);
        if refuse {
            self.inner.shed_total.fetch_add(1, Ordering::Relaxed);
            self.inner.shed_survival.fetch_add(1, Ordering::Relaxed);
        }
        refuse
    }

    /// The ordinary in-flight gate for WRITES, after authentication
    /// (Round-13). Reads are never shed here (R24-B: shedding reads
    /// hides the instance from its own operators).
    pub fn admit_write_inflight(&self) -> Result<(), WriteRefusal> {
        let cap = self.max_inflight();
        if cap > 0 && self.inner.inflight.load(Ordering::Relaxed) > cap {
            self.inner.shed_total.fetch_add(1, Ordering::Relaxed);
            self.inner.shed_inflight.fetch_add(1, Ordering::Relaxed);
            return Err(WriteRefusal::Overloaded);
        }
        Ok(())
    }

    /// The RSS write-shed (R25-E): sampled RSS PLUS `reserved_bytes` (the
    /// absorber's reservation) against the shed line, so the line moves
    /// BEFORE the memory does.
    pub fn admit_write_memory(&self, reserved_bytes: u64) -> Result<(), WriteRefusal> {
        let line = self.inner.rss_shed_mb;
        if line > 0 && crate::history::memory_pressure_mb(self.rss_mb(), reserved_bytes) > line {
            self.inner.shed_total.fetch_add(1, Ordering::Relaxed);
            self.inner.shed_rss.fetch_add(1, Ordering::Relaxed);
            return Err(WriteRefusal::MemoryPressure);
        }
        Ok(())
    }

    pub fn max_inflight(&self) -> i64 {
        self.inner.max_inflight.load(Ordering::Relaxed)
    }

    /// Rigs tune the ordinary cap live (an operator surface can adopt
    /// this the day one exists; until then it is a test hook).
    #[cfg(test)]
    pub fn set_max_inflight(&self, cap: i64) {
        self.inner.max_inflight.store(cap, Ordering::Relaxed);
    }

    /// The current in-flight count and the peak since the last swap
    /// (the fleet heartbeat and the load page reset the peak).
    pub fn swap_peak(&self) -> (i64, i64) {
        let now = self.inner.inflight.load(Ordering::Relaxed);
        let peak = self.inner.inflight_peak.swap(now, Ordering::Relaxed);
        (now, peak)
    }

    // ---- per-stream ----------------------------------------------------

    /// Acquire a per-stream slot: `None` when the limiter is off or the
    /// map is at its bound (admit untracked, never leak); `Err` at the
    /// stream's cap (counted).
    pub fn stream_slot(&self, hash: [u8; 16]) -> Result<Option<StreamSlot>, StreamRefusal> {
        let cap = self.inner.per_stream_cap;
        if cap <= 0 {
            return Ok(None);
        }
        let mut m = self.inner.streams.lock().unwrap();
        match m.get_mut(&hash) {
            Some(v) => {
                if *v >= cap {
                    drop(m);
                    self.inner.shed_stream.fetch_add(1, Ordering::Relaxed);
                    return Err(StreamRefusal);
                }
                *v += 1;
            }
            None => {
                if m.len() >= STREAM_INFLIGHT_MAX_TRACKED {
                    return Ok(None);
                }
                m.insert(hash, 1);
            }
        }
        drop(m);
        Ok(Some(StreamSlot {
            ctl: self.clone(),
            hash,
        }))
    }

    // ---- subscriptions -------------------------------------------------

    /// Acquire a live-subscription slot against the instance budget, so
    /// subscriber memory exhausts SUBSCRIPTION capacity, not the shared
    /// RSS line that sheds unrelated appends. 0 = unlimited.
    pub fn subscribe(&self) -> Result<SubscriptionTicket, SubscriptionRefusal> {
        let cap = self.inner.subscriptions.effective;
        let cur = self.inner.sse_connections.fetch_add(1, Ordering::Relaxed) + 1;
        if cap > 0 && cur > cap {
            self.inner.sse_connections.fetch_sub(1, Ordering::Relaxed);
            return Err(SubscriptionRefusal);
        }
        Ok(SubscriptionTicket { ctl: self.clone() })
    }

    // ---- memory, maintenance, load vector -------------------------------

    pub fn record_rss_mb(&self, mb: u64) {
        self.inner.rss_mb.store(mb, Ordering::Relaxed);
    }

    pub fn rss_mb(&self) -> u64 {
        self.inner.rss_mb.load(Ordering::Relaxed)
    }

    pub fn rss_shed_mb(&self) -> u64 {
        self.inner.rss_shed_mb
    }

    pub fn project_memory_pressure_bytes(&self) -> u64 {
        self.inner
            .project_memory_pressure_bytes
            .load(Ordering::Relaxed)
    }

    /// Rigs move the per-project pressure threshold live (same status
    /// as `set_max_inflight`).
    #[cfg(test)]
    pub fn set_project_memory_pressure_bytes(&self, bytes: u64) {
        self.inner
            .project_memory_pressure_bytes
            .store(bytes, Ordering::Relaxed);
    }

    pub fn project_memory_release_pct(&self) -> u64 {
        self.inner.project_memory_release_pct
    }

    /// The maintenance-backpressure latch (R23-1): re-evaluated by the
    /// sampler tick, consulted by the append path.
    pub fn maintenance(&self) -> &GlobalLatch {
        &self.inner.maintenance
    }

    /// A wedge refusal (stalled durability pipeline) was answered.
    pub fn note_wedge_shed(&self) {
        self.inner.shed_wedge.fetch_add(1, Ordering::Relaxed);
    }

    /// One successful /v1/stream/* request toward the fleet load vector.
    pub fn note_fleet_op(&self) {
        self.inner.fleet_ops.fetch_add(1, Ordering::Relaxed);
    }

    pub fn fleet_ops(&self) -> u64 {
        self.inner.fleet_ops.load(Ordering::Relaxed)
    }

    /// The per-record payload ceiling (0 = unlimited).
    pub fn record_ceiling(&self) -> usize {
        self.inner.record_ceiling.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn set_record_ceiling(&self, bytes: usize) {
        self.inner.record_ceiling.store(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        let i = &self.inner;
        let ord = Ordering::Relaxed;
        AdmissionSnapshot {
            inflight: i.inflight.load(ord),
            inflight_peak: i.inflight_peak.load(ord),
            max_inflight: i.max_inflight.load(ord),
            per_stream_cap: i.per_stream_cap,
            streams_tracked: i.streams.lock().unwrap().len(),
            shed: ShedSnapshot {
                total: i.shed_total.load(ord),
                inflight: i.shed_inflight.load(ord),
                survival: i.shed_survival.load(ord),
                rss: i.shed_rss.load(ord),
                stream: i.shed_stream.load(ord),
                wedge: i.shed_wedge.load(ord),
            },
            rss_mb: i.rss_mb.load(ord),
            rss_shed_mb: i.rss_shed_mb,
            project_memory_pressure_bytes: i.project_memory_pressure_bytes.load(ord),
            sse_connections: i.sse_connections.load(ord),
            sse_effective_max: i.subscriptions.effective,
            sse_configured_max: i.subscriptions.configured,
            fleet_ops: i.fleet_ops.load(ord),
        }
    }

    /// Rigs simulate load without holding tickets (the inflight
    /// admission tests push the count past the cap directly).
    #[cfg(test)]
    pub fn add_inflight_for_test(&self, n: i64) {
        self.inner.inflight.fetch_add(n, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctl(max_inflight: i64, per_stream: i64, rss_shed_mb: u64, sse: u64) -> AdmissionController {
        AdmissionController::new(AdmissionKnobs {
            max_inflight,
            per_stream_cap: per_stream,
            rss_shed_mb,
            project_memory_pressure_bytes: 0,
            project_memory_release_pct: 0,
            subscriptions: SubscriptionCapacity {
                effective: sse,
                configured: sse,
            },
            record_ceiling_bytes: 0,
        })
    }

    /// Tickets are RAII: the count follows the ticket's lifetime and the
    /// peak survives the drop until swapped.
    #[test]
    fn inflight_tickets_are_raii_and_record_the_peak() {
        let c = ctl(10, 0, 0, 0);
        let a = c.enter();
        let b = c.enter();
        assert_eq!((a.current(), b.current()), (1, 2));
        assert_eq!(c.snapshot().inflight, 2);
        drop(a);
        assert_eq!(c.snapshot().inflight, 1);
        assert_eq!(c.swap_peak(), (1, 2), "the peak is the high-water mark");
        drop(b);
        assert_eq!(
            c.swap_peak(),
            (0, 1),
            "swap resets the peak to the current count"
        );
    }

    /// The survival bound refuses exactly above 4x the cap, on stream
    /// paths only, and counts under `survival`; the ordinary gate refuses
    /// above the cap and counts under `inflight`; both feed `total`.
    #[test]
    fn survival_and_ordinary_gates_are_distinct_mechanisms() {
        let c = ctl(4, 0, 0, 0);
        assert!(!c.survival_refused(16, true), "at 4x: not over");
        assert!(c.survival_refused(17, true), "over 4x on a stream path");
        assert!(!c.survival_refused(17, false), "never on non-stream paths");
        let off = ctl(0, 0, 0, 0);
        assert!(!off.survival_refused(1_000, true), "cap 0 = off");
        c.add_inflight_for_test(5);
        assert_eq!(c.admit_write_inflight(), Err(WriteRefusal::Overloaded));
        c.set_max_inflight(8);
        assert_eq!(c.admit_write_inflight(), Ok(()));
        let s = c.snapshot().shed;
        assert_eq!((s.total, s.survival, s.inflight), (2, 1, 1), "{s:?}");
    }

    /// The RSS gate reads sampled RSS PLUS the reserved bytes against the
    /// line; 0 = off.
    #[test]
    fn rss_gate_counts_reserved_bytes() {
        let c = ctl(0, 0, 100, 0);
        c.record_rss_mb(90);
        assert_eq!(c.admit_write_memory(0), Ok(()));
        assert_eq!(
            c.admit_write_memory(20 * 1024 * 1024),
            Err(WriteRefusal::MemoryPressure),
            "90 MiB sampled + 20 MiB reserved crosses the 100 MiB line"
        );
        assert_eq!(c.snapshot().shed.rss, 1);
        let off = ctl(0, 0, 0, 0);
        off.record_rss_mb(10_000);
        assert_eq!(off.admit_write_memory(u64::MAX / 4), Ok(()));
    }

    /// Per-stream slots: bounded per stream, released at zero, untracked
    /// when the limiter is off.
    #[test]
    fn stream_slots_are_bounded_and_released() {
        let c = ctl(0, 2, 0, 0);
        let h = [7u8; 16];
        let a = c.stream_slot(h).unwrap().expect("tracked");
        let _b = c.stream_slot(h).unwrap().expect("tracked");
        assert!(matches!(c.stream_slot(h), Err(StreamRefusal)));
        assert_eq!(c.snapshot().streams_tracked, 1);
        assert_eq!(c.snapshot().shed.stream, 1);
        drop(a);
        assert!(
            c.stream_slot(h).unwrap().is_some(),
            "a released slot is reusable"
        );
        let off = ctl(0, 0, 0, 0);
        assert!(
            off.stream_slot(h).unwrap().is_none(),
            "limiter off = untracked"
        );
        assert_eq!(off.snapshot().streams_tracked, 0);
    }

    /// The subscription budget: 0 = unlimited; over the cap refuses and
    /// leaves the count exactly where it was; tickets release on drop.
    #[test]
    fn subscription_budget_is_exact() {
        let c = ctl(0, 0, 0, 2);
        let a = c.subscribe().expect("1 of 2");
        let b = c.subscribe().expect("2 of 2");
        assert!(c.subscribe().is_err());
        assert_eq!(c.snapshot().sse_connections, 2, "a refusal never counts");
        drop(a);
        let _c2 = c.subscribe().expect("released slot");
        drop(b);
        assert_eq!(c.snapshot().sse_connections, 1);
        let unlimited = ctl(0, 0, 0, 0);
        let _t: Vec<_> = (0..1_000).map(|_| unlimited.subscribe().unwrap()).collect();
        assert_eq!(unlimited.snapshot().sse_connections, 1_000);
    }

    /// The snapshot mirrors every counter and the knobs, including the
    /// clamped release percentage and the load vector.
    #[test]
    fn snapshot_mirrors_the_controller() {
        let c = AdmissionController::new(AdmissionKnobs {
            max_inflight: 3,
            per_stream_cap: 1,
            rss_shed_mb: 7,
            project_memory_pressure_bytes: 11,
            project_memory_release_pct: 500,
            subscriptions: SubscriptionCapacity {
                effective: 5,
                configured: 9,
            },
            record_ceiling_bytes: 4_096,
        });
        assert_eq!(c.project_memory_release_pct(), 100, "clamped");
        c.note_fleet_op();
        c.note_wedge_shed();
        c.set_project_memory_pressure_bytes(13);
        let s = c.snapshot();
        assert_eq!((s.max_inflight, s.per_stream_cap, s.rss_shed_mb), (3, 1, 7));
        assert_eq!(s.project_memory_pressure_bytes, 13);
        assert_eq!((s.sse_effective_max, s.sse_configured_max), (5, 9));
        assert_eq!((s.fleet_ops, s.shed.wedge), (1, 1));
        assert_eq!(c.record_ceiling(), 4_096);
        c.set_record_ceiling(0);
        assert_eq!(c.record_ceiling(), 0);
        assert_eq!(c.maintenance().engaged(), None);
    }
}
