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

// Own the removed events across every await. Dropping the drain future,
// serialization failure, and append failure all restore the same event IDs.
struct PendingOps<'a> {
    queue: &'a Mutex<OpsQueue>,
    dropped: &'a AtomicU64,
    gap: &'a AtomicU64,
    events: Vec<OpsEvent>,
}
impl<'a> PendingOps<'a> {
    fn take(queue: &'a Mutex<OpsQueue>, dropped: &'a AtomicU64, gap: &'a AtomicU64) -> Self {
        let events = {
            let mut guard = queue.lock().unwrap();
            let len = guard.queue.len().min(512);
            guard.queue.drain(..len).collect()
        };
        Self {
            queue,
            dropped,
            gap,
            events,
        }
    }
}
impl Drop for PendingOps<'_> {
    fn drop(&mut self) {
        if self.events.is_empty() {
            return;
        }
        let mut queue = self.queue.lock().unwrap();
        for event in self.events.drain(..).rev() {
            if queue.queue.len() < OPS_QUEUE_CAP {
                queue.queue.push_front(event);
            } else {
                let represented_gap = (event.event_type == "telemetry_gap")
                    .then(|| {
                        event
                            .fields
                            .get("dropped")
                            .and_then(serde_json::Value::as_u64)
                    })
                    .flatten();
                if let Some(count) = represented_gap {
                    // The represented events were counted at their first loss.
                    self.gap.fetch_add(count, Ordering::Relaxed);
                } else {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    self.gap.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
async fn persist_ops_batch<F, Fut>(mut batch: PendingOps<'_>, append: F) -> Result<usize, String>
where
    F: FnOnce(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let body = serde_json::to_vec(&batch.events).map_err(|error| error.to_string())?;
    append(body).await?;
    let count = batch.events.len();
    batch.events.clear(); // The durable append now owns these events.
    Ok(count)
}

/// Drain queued events to `_ops_events`. Called from the telemetry
/// task; requeues on failure (order preserved).
pub async fn drain_ops_once(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(key) = state.billing.usage_key() else {
        return Ok(0);
    };
    let mut batch = PendingOps::take(q(), &EVENTS_DROPPED, &GAP_PENDING);
    // Report any drop gap once capacity exists again.
    let gap = GAP_PENDING.swap(0, Ordering::Relaxed);
    if gap > 0 {
        batch.events.push(
            OpsEvent::new(
                "telemetry_gap",
                format!("gap/{}/{}", state.runtime.identity.boot_id, gap),
            )
            .warn()
            .fields(serde_json::json!({ "dropped": gap })),
        );
    }
    if batch.events.is_empty() {
        return Ok(0);
    }
    for ev in &mut batch.events {
        if ev.cell.is_empty() {
            ev.cell = state.deployment.cell_id().as_str().to_string();
        }
    }
    persist_ops_batch(batch, |body| ops_ledger_append(state, &key, body)).await
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
    counters.insert("fleet_ops_total".into(), state.admission.fleet_ops());
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
        state.billing.read_seal_deferrals(),
    );
    let (rows, est, sealed) = state.billing.unflushed_reads();
    gauges.insert("read_meter_unflushed_rows".into(), rows as u64);
    gauges.insert("read_meter_unflushed_bytes_est".into(), est as u64);
    gauges.insert("read_meter_sealed_batches".into(), sealed as u64);
    gauges.insert("open_engines".into(), state.shards.open_count() as u64);
    if let Some(sp) = state.billing.read_spool_stats() {
        gauges.insert("read_spool_quarantined".into(), sp.quarantined);
        gauges.insert("read_spool_pending_rows".into(), sp.pending_rows);
        gauges.insert("read_spool_pending_bytes".into(), sp.pending_bytes);
    }
    // ---- OOM-review causal metrics ------------------------------------
    // Absorber: process-wide budget + last-gather phases. reserved vs
    // actual is the review's "is the multiplier honest" check.
    let ord = Ordering::Relaxed;
    gauges.insert(
        "absorb_reserved_bytes".into(),
        state.runtime.history.budget.reserved_bytes(),
    );
    gauges.insert(
        "absorb_gathers_inflight".into(),
        state.runtime.history.budget.inflight(),
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
        let engines: Vec<_> = state.shards.engines();
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
    if let Some(sp) = state.billing.read_spool_stats() {
        gauges.insert("spool_l0_ssts".into(), sp.l0.0);
        gauges.insert("spool_l0_bytes".into(), sp.l0.1);
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
    gauges.insert("rss_mb".into(), state.admission.rss_mb());
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
        cell: state.deployment.cell_id().as_str().to_string(),
        region: state.deployment.region().to_string(),
        instance: state.ownership.instance().to_string(),
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
    let Some(key) = state.billing.usage_key() else {
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
    let threshold = usage_outbox_alert_threshold(&state.config.billing);
    let (dirty_total, debt_unknown) = {
        let mut n = 0u64;
        let mut unknown = false;
        'engines: for engine in state.shards.engines() {
            let mut after = None;
            loop {
                if n > threshold {
                    break 'engines;
                }
                if n >= 4096 {
                    unknown = true;
                    break 'engines;
                }
                let limit = (threshold.saturating_add(1).saturating_sub(n))
                    .min(256)
                    .min(4096 - n) as usize;
                match engine.usage_dirty_page(after, limit).await {
                    Ok((rows, more)) => {
                        n += rows.len() as u64;
                        after = rows.last().map(|(hash, _)| *hash);
                        if !more {
                            break;
                        }
                        if after.is_none() {
                            unknown = true;
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "usage debt unreadable; alert remains open");
                        unknown = true;
                        break;
                    }
                }
            }
        }
        (n, unknown)
    };
    let rules: Vec<(String, bool, String)> = vec![
        (
            "usage_outbox_lag".into(),
            dirty_total > threshold || debt_unknown,
            format!(
                "at least {dirty_total} unacknowledged usage snapshots (discovery incomplete={debt_unknown})"
            ),
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
                            .fields(serde_json::json!({ "fingerprint": fp })),
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

fn usage_outbox_alert_threshold(cfg: &crate::config::BillingConfig) -> u64 {
    cfg.alert_usage_outbox_dirty
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    fn event(id: &str) -> OpsEvent {
        OpsEvent::new("event", id.into())
    }
    fn local_queue() -> Mutex<OpsQueue> {
        Mutex::new(OpsQueue {
            queue: VecDeque::new(),
            recent: VecDeque::new(),
        })
    }
    #[tokio::test]
    async fn cancelled_ops_append_restores_batch_order_and_retry_ids() {
        let queue = local_queue();
        let dropped = AtomicU64::new(0);
        let gap = AtomicU64::new(0);
        queue
            .lock()
            .unwrap()
            .queue
            .extend([event("first"), event("second")]);
        let entered = tokio::sync::Notify::new();
        let entered_sink = &entered;
        let mut drain = Box::pin(persist_ops_batch(
            PendingOps::take(&queue, &dropped, &gap),
            |body| async move {
                let sent: Vec<OpsEvent> = serde_json::from_slice(&body).unwrap();
                assert_eq!(
                    sent.iter()
                        .map(|event| event.event_id.as_str())
                        .collect::<Vec<_>>(),
                    ["first", "second"]
                );
                entered_sink.notify_one();
                std::future::pending::<Result<(), String>>().await
            },
        ));
        tokio::select! {
            _ = &mut drain => panic!("held append cannot finish"),
            _ = entered.notified() => {}
        }
        assert!(
            queue.lock().unwrap().queue.is_empty(),
            "entered the real batch sink after dequeue"
        );
        queue.lock().unwrap().queue.push_back(event("newer"));
        drop(drain); // Cooperative cancellation drops the actual owned batch.
        let ids = || {
            queue
                .lock()
                .unwrap()
                .queue
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(), ["first", "second", "newer"]);
        let failed = persist_ops_batch(PendingOps::take(&queue, &dropped, &gap), |_| async {
            Err("retry".into())
        })
        .await;
        assert!(failed.is_err());
        assert_eq!(ids(), ["first", "second", "newer"]);
        assert_eq!(
            persist_ops_batch(PendingOps::take(&queue, &dropped, &gap), |_| async {
                Ok(())
            })
            .await
            .unwrap(),
            3
        );
        assert!(ids().is_empty(), "durable success must disarm requeue");
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        assert_eq!(gap.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn cancelled_ops_batch_overflow_preserves_full_gap_magnitude() {
        let queue = local_queue();
        let dropped = AtomicU64::new(0);
        let gap = AtomicU64::new(0);
        queue.lock().unwrap().queue.push_back(event("pending"));
        let mut batch = PendingOps::take(&queue, &dropped, &gap);
        batch.events.push(
            OpsEvent::new("telemetry_gap", "gap-id".into())
                .fields(serde_json::json!({"dropped": 17})),
        );
        queue
            .lock()
            .unwrap()
            .queue
            .extend((0..OPS_QUEUE_CAP).map(|_| event("newer")));
        drop(batch);
        assert_eq!(queue.lock().unwrap().queue.len(), OPS_QUEUE_CAP);
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            1,
            "only the newly lost event is a new drop"
        );
        assert_eq!(
            gap.load(Ordering::Relaxed),
            18,
            "restore the prior 17-event gap plus the new loss"
        );
    }
}
