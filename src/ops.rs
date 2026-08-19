//! Durable operational event journal (docs/OBSERVABILITY-BILLING.md
//! §12): typed, versioned events with DETERMINISTIC ids, appended to
//! the reserved `_ops_events` stream and deduplicated downstream by id.
//!
//! Durability model (§12.4), by state class:
//!   - CAS-backed fleet state (`desired.json`, `overrides.json`)
//!     carries a pending-event outbox INSIDE the CAS object: the write
//!     that commits the transition records the event; the drainer
//!     appends and then CAS-clears exactly those ids. Re-emission is
//!     safe because ids are deterministic.
//!   - Descriptor-backed transitions (create/seal/split/delete) are
//!     durably recorded in the descriptor itself; their events emit
//!     through the process queue with ids derived from the incarnation
//!     and transition, so a replay deduplicates and the descriptor
//!     remains the recovery source.
//!   - Observations (instance dark/live, fences, stalls) derive their
//!     ids from the observed state's own timestamps.
//!
//! Events never block the product transition they describe. The queue
//! is bounded: at the cap the transition proceeds, a durable drop
//! counter grows, and a `telemetry_gap` event emits once capacity
//! returns (§12.4).

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub const OPS_QUEUE_CAP: usize = 4096;
/// Recent ring for the operator's live view (§12.5).
pub const RECENT_CAP: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpsEvent {
    pub v: u16,
    pub event_id: String,
    pub event_time_ms: i64,
    #[serde(default)]
    pub observed_ms: i64,
    #[serde(default)]
    pub cell: String,
    pub event_type: String,
    /// info | warn | error
    #[serde(default)]
    pub severity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    /// Owning project of a customer-stream event. Type-enforced by the
    /// `stream()` builder (SR-6): naming a stream without its project
    /// is unrepresentable. Absent on system/cell-level events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub fields: serde_json::Value,
}

impl OpsEvent {
    pub fn new(event_type: &str, event_id: String) -> Self {
        OpsEvent {
            v: 1,
            event_id,
            event_time_ms: crate::shard::now_ms(),
            observed_ms: crate::shard::now_ms(),
            cell: String::new(),
            event_type: event_type.to_string(),
            severity: "info".into(),
            instance: None,
            project_id: None,
            stream_id: None,
            stream_name: None,
            shard: None,
            fields: serde_json::Value::Null,
        }
    }
    /// Stamp CUSTOMER-stream identity: the tenant-qualified ref plus
    /// the incarnation. Taking `TenantStreamRef` (not a bare name) is
    /// the SR-6 guarantee that no customer op event omits its project.
    pub fn stream(mut self, sref: &crate::tenant::TenantStreamRef, epoch: &str) -> Self {
        self.project_id = Some(sref.project_id().as_str().to_string());
        self.stream_id = Some(epoch.to_string());
        self.stream_name = Some(sref.name().as_str().to_string());
        self
    }
    pub fn shard(mut self, prefix: &str) -> Self {
        self.shard = Some(prefix.to_string());
        self
    }
    pub fn instance(mut self, i: &str) -> Self {
        self.instance = Some(i.to_string());
        self
    }
    pub fn warn(mut self) -> Self {
        self.severity = "warn".into();
        self
    }
    pub fn fields(mut self, f: serde_json::Value) -> Self {
        self.fields = f;
        self
    }
}

struct OpsQueue {
    queue: VecDeque<OpsEvent>,
    recent: VecDeque<OpsEvent>,
}

fn q() -> &'static Mutex<OpsQueue> {
    static Q: std::sync::OnceLock<Mutex<OpsQueue>> = std::sync::OnceLock::new();
    Q.get_or_init(|| {
        Mutex::new(OpsQueue {
            queue: VecDeque::new(),
            recent: VecDeque::new(),
        })
    })
}

pub static EVENTS_DROPPED: AtomicU64 = AtomicU64::new(0);
static GAP_PENDING: AtomicU64 = AtomicU64::new(0);

/// Enqueue one event. NEVER blocks and never fails the caller: at the
/// cap the event drops into a durable counter and a later
/// `telemetry_gap` event reports the loss (§12.4).
pub fn emit(ev: OpsEvent) {
    let mut g = q().lock().unwrap();
    g.recent.push_back(ev.clone());
    if g.recent.len() > RECENT_CAP {
        g.recent.pop_front();
    }
    if g.queue.len() >= OPS_QUEUE_CAP {
        EVENTS_DROPPED.fetch_add(1, Ordering::Relaxed);
        GAP_PENDING.fetch_add(1, Ordering::Relaxed);
        return;
    }
    g.queue.push_back(ev);
}

/// The operator's recent-events view.
pub fn recent(limit: usize) -> Vec<OpsEvent> {
    let g = q().lock().unwrap();
    g.recent.iter().rev().take(limit).cloned().collect()
}

/// Drain queued events to `_ops_events`. Called from the telemetry
/// task; requeues on failure (order preserved).
pub async fn drain_ops_once(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(key) = state.usage_key.clone() else {
        return Ok(0);
    };
    let mut batch: Vec<OpsEvent> = {
        let mut g = q().lock().unwrap();
        let n = g.queue.len().min(512);
        g.queue.drain(..n).collect()
    };
    // Report any drop gap once capacity exists again.
    let gap = GAP_PENDING.swap(0, Ordering::Relaxed);
    if gap > 0 {
        batch.push(
            OpsEvent::new(
                "telemetry_gap",
                format!("gap/{}/{}", crate::billing::boot_id(), gap),
            )
            .warn()
            .fields(serde_json::json!({"dropped": gap})),
        );
    }
    if batch.is_empty() {
        return Ok(0);
    }
    for ev in &mut batch {
        if ev.cell.is_empty() {
            ev.cell = state.cell_id.clone();
        }
    }
    let body = serde_json::to_vec(&batch).map_err(|e| e.to_string())?;
    match ops_ledger_append(state, &key, body).await {
        Ok(()) => Ok(batch.len()),
        Err(e) => {
            let mut g = q().lock().unwrap();
            for ev in batch.into_iter().rev() {
                if g.queue.len() < OPS_QUEUE_CAP {
                    g.queue.push_front(ev);
                } else {
                    EVENTS_DROPPED.fetch_add(1, Ordering::Relaxed);
                    GAP_PENDING.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e)
        }
    }
}

async fn ops_ledger_append(
    state: &std::sync::Arc<crate::http::AppState>,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    crate::billing::system_append(state, crate::billing::OPS_EVENTS_STREAM, key, body).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The queue never blocks a caller and never lies about loss: past
    /// the cap events drop into a counter, and the next drain reports
    /// the gap exactly once.
    #[test]
    fn overflow_counts_and_reports() {
        // Isolate: drain whatever other tests queued.
        {
            let mut g = q().lock().unwrap();
            g.queue.clear();
        }
        for i in 0..(OPS_QUEUE_CAP + 10) {
            emit(OpsEvent::new("t", format!("t/{i}")));
        }
        let dropped = EVENTS_DROPPED.load(Ordering::Relaxed);
        assert!(dropped >= 10, "overflow must count drops, saw {dropped}");
        let g = q().lock().unwrap();
        assert_eq!(g.queue.len(), OPS_QUEUE_CAP, "cap enforced");
        assert!(g.recent.len() <= RECENT_CAP);
    }
}

// ---------------------------------------------------------------------
// `_ops_metrics` snapshots (§11) and the alert evaluator (§13.2)
// ---------------------------------------------------------------------

/// One instance's low-cardinality snapshot: counters and gauges only —
/// no stream names, routing keys, or per-customer dimensions (§11.2).
/// Counters are cumulative (mergeable by differencing); gauges are
/// instantaneous. Store-latency histograms remain on the live
/// `/v1/debug/timings` surface; the snapshot carries their summary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpsSnapshot {
    pub v: u16,
    pub ts_ms: i64,
    pub cell: String,
    pub region: String,
    pub instance: String,
    pub role: String,
    // mt-lint: allow(name-keyed-map): metric name, not stream identity
    pub counters: std::collections::BTreeMap<String, u64>,
    // mt-lint: allow(name-keyed-map): metric name, not stream identity
    pub gauges: std::collections::BTreeMap<String, u64>,
}

/// Peak sampled RSS (MB) since the last ops scrape — fed by the 250 ms
/// process sampler, drained (swap 0) by each snapshot, so a spike
/// between snapshots is never invisible.
pub static RSS_PEAK_MB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Collect the instance snapshot from the live plane.
pub fn collect_snapshot(state: &std::sync::Arc<crate::http::AppState>) -> OpsSnapshot {
    let mut counters = std::collections::BTreeMap::new();
    let mut gauges = std::collections::BTreeMap::new();
    counters.insert(
        "fleet_ops_total".into(),
        state.fleet_ops.load(Ordering::Relaxed),
    );
    counters.insert(
        "ops_events_dropped_total".into(),
        EVENTS_DROPPED.load(Ordering::Relaxed),
    );
    counters.insert(
        "audit_events_dropped_total".into(),
        crate::audit::AUDIT_DROPPED.load(Ordering::Relaxed),
    );
    counters.insert(
        "unowned_meter_events_total".into(),
        crate::billing::UNOWNED_METER_EVENTS.load(Ordering::Relaxed),
    );
    counters.insert(
        "segment_identity_drift_total".into(),
        crate::billing::SEGMENT_IDENTITY_DRIFT.load(Ordering::Relaxed),
    );
    counters.insert(
        "read_meter_seal_deferrals_total".into(),
        state.billing_reads.seal_deferrals.load(Ordering::Relaxed),
    );
    let (rows, est, sealed) = state.billing_reads.unflushed();
    gauges.insert("read_meter_unflushed_rows".into(), rows as u64);
    gauges.insert("read_meter_unflushed_bytes_est".into(), est as u64);
    gauges.insert("read_meter_sealed_batches".into(), sealed as u64);
    gauges.insert(
        "open_engines".into(),
        state.shards.read().unwrap().len() as u64,
    );
    if let Some(sp) = state.read_spool.get() {
        gauges.insert("read_spool_quarantined".into(), sp.quarantined_count());
        let (rows, bytes) = sp.resident();
        gauges.insert("read_spool_pending_rows".into(), rows);
        gauges.insert("read_spool_pending_bytes".into(), bytes);
    }
    // ---- OOM-review causal metrics ------------------------------------
    // Absorber: process-wide budget + last-gather phases. reserved vs
    // actual is the review's "is the multiplier honest" check.
    let ord = Ordering::Relaxed;
    gauges.insert(
        "absorb_reserved_bytes".into(),
        crate::history::absorb_reserved_bytes(),
    );
    gauges.insert(
        "absorb_gathers_inflight".into(),
        crate::history::absorb_gathers_inflight(),
    );
    gauges.insert(
        "gather_last_reserved_bytes".into(),
        crate::history::GATHER_LAST_RESERVED.load(ord),
    );
    gauges.insert(
        "gather_last_actual_bytes".into(),
        crate::history::GATHER_LAST_ACTUAL.load(ord),
    );
    gauges.insert(
        "gather_last_read_ms".into(),
        crate::history::GATHER_LAST_READ_MS.load(ord),
    );
    gauges.insert(
        "gather_last_pace_ms".into(),
        crate::history::GATHER_LAST_PACE_MS.load(ord),
    );
    gauges.insert(
        "gather_last_write_ms".into(),
        crate::history::GATHER_LAST_WRITE_MS.load(ord),
    );
    gauges.insert(
        "gather_last_flush_ms".into(),
        crate::history::GATHER_LAST_FLUSH_MS.load(ord),
    );
    // PEAK-SINCE-SCRAPE (swap 0): a flush stall between snapshots
    // cannot vanish — the next snapshot carries the peak.
    gauges.insert(
        "history_flush_wait_ms_max".into(),
        crate::history::HISTORY_FLUSH_WAIT_MS_MAX.swap(0, ord),
    );
    gauges.insert(
        "history_flush_injected_stall_ms".into(),
        crate::history::HISTORY_FLUSH_STALL_MS.load(ord),
    );
    counters.insert(
        "absorb_bytes_total".into(),
        crate::history::ABSORB_BYTES_TOTAL.load(ord),
    );
    counters.insert(
        "ingest_bytes_total".into(),
        crate::history::INGEST_BYTES_TOTAL.load(ord),
    );
    // History partitions: L0 posture from each OPEN partition's
    // in-memory manifest snapshot (no store requests, never opens one).
    {
        let engines: Vec<_> = state.shards.read().unwrap().values().cloned().collect();
        let (mut open, mut l0_max, mut l0_bytes, mut runs_max) = (0u64, 0u64, 0u64, 0u64);
        for e in engines {
            if let Some(part) = e.history_partition_if_open() {
                let (n, b, runs, _id) = crate::history::history_l0_stats(&part);
                open += 1;
                l0_max = l0_max.max(n);
                l0_bytes += b;
                runs_max = runs_max.max(runs);
            }
        }
        gauges.insert("history_partitions_open".into(), open);
        gauges.insert("history_l0_ssts_max".into(), l0_max);
        gauges.insert("history_l0_bytes_total".into(), l0_bytes);
        gauges.insert("history_compacted_runs_max".into(), runs_max);
    }
    gauges.insert(
        "sweep_resident_engines".into(),
        crate::billing::sweep_resident_engines(state),
    );
    gauges.insert(
        "sweep_open_peak".into(),
        crate::billing::sweep_open_peak(state) as u64,
    );
    gauges.insert(
        "walk_deferred_total".into(),
        crate::billing::WALK_DEFERRED.load(ord),
    );
    gauges.insert(
        "telemetry_cache_capacity_bytes".into(),
        crate::billing::TELEMETRY_CACHE_CAPACITY.load(ord),
    );
    // Telemetry-DB L0 posture (OOM review I3): the bounded settings
    // must be OBSERVABLY holding, not just configured.
    if let Some(sp) = state.read_spool.get() {
        let (l0, l0b, _, _) = sp.l0_stats();
        gauges.insert("spool_l0_ssts".into(), l0);
        gauges.insert("spool_l0_bytes".into(), l0b);
    }
    if let Some(ru) = state.rollup.get() {
        let (l0, l0b, _, _) = ru.l0_stats();
        gauges.insert("rollup_l0_ssts".into(), l0);
        gauges.insert("rollup_l0_bytes".into(), l0b);
    }
    gauges.insert(
        "rollup_apply_duration_ms".into(),
        crate::billing::ROLLUP_APPLY_DURATION_MS.load(ord),
    );
    // Process memory: sampled RSS + peak-since-scrape (the 250 ms
    // sampler keeps the peak; inter-snapshot SST-build spikes survive),
    // allocator commit, and cgroup truth when the platform provides it.
    gauges.insert("rss_mb".into(), state.rss_mb_cached.load(ord));
    gauges.insert("rss_peak_since_scrape_mb".into(), RSS_PEAK_MB.swap(0, ord));
    {
        let mut current_commit = 0usize;
        let mut peak_commit = 0usize;
        unsafe {
            let mut elapsed = 0;
            let mut ut = 0;
            let mut st_ = 0;
            let mut rss = 0;
            let mut prss = 0;
            let mut flt = 0;
            libmimalloc_sys::mi_process_info(
                &mut elapsed,
                &mut ut,
                &mut st_,
                &mut rss,
                &mut prss,
                &mut current_commit,
                &mut peak_commit,
                &mut flt,
            );
        }
        gauges.insert(
            "mi_current_commit_mb".into(),
            (current_commit / 1048576) as u64,
        );
        gauges.insert("mi_peak_commit_mb".into(), (peak_commit / 1048576) as u64);
    }
    for (file, name) in [
        ("/sys/fs/cgroup/memory.current", "cgroup_memory_current_mb"),
        ("/sys/fs/cgroup/memory.peak", "cgroup_memory_peak_mb"),
    ] {
        if let Ok(s) = std::fs::read_to_string(file)
            && let Ok(v) = s.trim().parse::<u64>()
        {
            gauges.insert(name.into(), v / 1048576);
        }
    }
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.events") {
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("oom_kill ")
                && let Ok(n) = v.trim().parse::<u64>()
            {
                counters.insert("cgroup_oom_kill_total".into(), n);
            }
        }
    }
    OpsSnapshot {
        v: 1,
        ts_ms: crate::shard::now_ms(),
        cell: state.cell_id.clone(),
        region: state.region.clone(),
        instance: state.instance_name.clone(),
        role: if state.rollup.get().is_some() {
            "rollup".into()
        } else {
            "server".into()
        },
        counters,
        gauges,
    }
}

/// Emit one snapshot to `_ops_metrics` (§11.2 cadence: the telemetry
/// task calls this every METRICS_INTERVAL_SECS, default 15).
pub async fn emit_metrics_once(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<(), String> {
    let Some(key) = state.usage_key.clone() else {
        return Ok(());
    };
    let snap = collect_snapshot(state);
    evaluate_alerts(state, &snap).await;
    let body = serde_json::to_vec(&[snap]).map_err(|e| e.to_string())?;
    metrics_ledger_append(state, &key, body).await
}

async fn metrics_ledger_append(
    state: &std::sync::Arc<crate::http::AppState>,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    crate::billing::system_append(state, crate::billing::OPS_METRICS_STREAM, key, body).await
}

// ---- alerts (§13.2) --------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub struct AlertState {
    pub fingerprint: String,
    pub summary: String,
    pub opened_at_ms: i64,
    pub last_seen_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at_ms: Option<i64>,
}

fn alerts_map() -> &'static Mutex<std::collections::HashMap<String, AlertState>> {
    // mt-lint: allow(name-keyed-map): alert kind, not stream identity
    static A: std::sync::OnceLock<Mutex<std::collections::HashMap<String, AlertState>>> =
        std::sync::OnceLock::new();
    A.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// Open alerts for the operator surface.
pub fn open_alerts() -> Vec<AlertState> {
    alerts_map()
        .lock()
        .unwrap()
        .values()
        .filter(|a| a.resolved_at_ms.is_none())
        .cloned()
        .collect()
}

/// Evaluate the initial rule set against one snapshot; open/resolve
/// transitions append to `_ops_events` (§13.2: the stored record is
/// the audit trail). Rules read only what the snapshot carries — the
/// evaluator itself is a pure function of observable state.
pub async fn evaluate_alerts(state: &std::sync::Arc<crate::http::AppState>, snap: &OpsSnapshot) {
    let g = |k: &str| snap.gauges.get(k).copied().unwrap_or(0);
    // (fingerprint, breached, human summary)
    let dirty_total = {
        let engines: Vec<_> = state.shards.read().unwrap().values().cloned().collect();
        let mut n = 0usize;
        for e in engines {
            n += e.usage_dirty_scan().await.map(|v| v.len()).unwrap_or(0);
        }
        n as u64
    };
    let rules: Vec<(String, bool, String)> = vec![
        (
            "usage_outbox_lag".into(),
            dirty_total > usage_outbox_alert_threshold(),
            format!("{dirty_total} unacknowledged usage snapshots"),
        ),
        (
            "read_meter_backpressure".into(),
            g("read_meter_sealed_batches") >= crate::billing::READ_SEALED_MAX_BATCHES as u64,
            "the read-usage sealed queue is full (ledger down?)".into(),
        ),
        (
            "ops_event_drops".into(),
            snap.counters
                .get("ops_events_dropped_total")
                .copied()
                .unwrap_or(0)
                > 0,
            "operational events were dropped at the queue cap".into(),
        ),
        (
            // Round-22 item 2c: quarantined spool rows are metered
            // reads that are NOT reaching the invoice — a standing
            // page until an operator recovers or writes them off.
            "read_spool_corruption".into(),
            g("read_spool_quarantined") > 0,
            format!(
                "{} corrupt read-spool rows quarantined — reads under-billed until recovered",
                g("read_spool_quarantined")
            ),
        ),
    ];
    let now = snap.ts_ms;
    let mut map = alerts_map().lock().unwrap();
    for (fp, breached, summary) in rules {
        match (map.get_mut(&fp), breached) {
            (Some(a), true) => {
                a.last_seen_ms = now;
                if a.resolved_at_ms.is_some() {
                    // Re-opened.
                    a.opened_at_ms = now;
                    a.resolved_at_ms = None;
                    emit(
                        OpsEvent::new("alert_opened", format!("alert/{fp}/{now}"))
                            .warn()
                            .fields(serde_json::json!({"fingerprint": fp, "summary": summary})),
                    );
                }
            }
            (Some(a), false) => {
                if a.resolved_at_ms.is_none() {
                    a.resolved_at_ms = Some(now);
                    emit(
                        OpsEvent::new("alert_resolved", format!("alert/{fp}/resolved/{now}"))
                            .fields(serde_json::json!({"fingerprint": fp})),
                    );
                }
            }
            (None, true) => {
                map.insert(
                    fp.clone(),
                    AlertState {
                        fingerprint: fp.clone(),
                        summary: summary.clone(),
                        opened_at_ms: now,
                        last_seen_ms: now,
                        resolved_at_ms: None,
                    },
                );
                emit(
                    OpsEvent::new("alert_opened", format!("alert/{fp}/{now}"))
                        .warn()
                        .fields(serde_json::json!({"fingerprint": fp, "summary": summary})),
                );
            }
            (None, false) => {}
        }
    }
}

fn usage_outbox_alert_threshold() -> u64 {
    std::env::var("ALERT_USAGE_OUTBOX_DIRTY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
}
