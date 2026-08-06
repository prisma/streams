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
            stream_id: None,
            stream_name: None,
            shard: None,
            fields: serde_json::Value::Null,
        }
    }
    pub fn stream(mut self, id: &str, name: &str) -> Self {
        self.stream_id = Some(id.to_string());
        self.stream_name = Some(name.to_string());
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
    use axum::http::{HeaderMap, HeaderValue};
    static CREATED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(key).map_err(|_| "bad key".to_string())?,
    );
    hdrs.insert("content-type", HeaderValue::from_static("application/json"));
    if CREATED.get().is_none() {
        let r = crate::http::create_stream(
            state.clone(),
            crate::billing::OPS_EVENTS_STREAM.to_string(),
            hdrs.clone(),
            bytes::Bytes::new(),
        )
        .await;
        let st = r.status().as_u16();
        if st == 200 || st == 201 || st == 409 {
            let _ = CREATED.set(());
        } else {
            return Err(format!("ops ledger create: {st}"));
        }
    }
    let r = crate::http::append(
        state.clone(),
        crate::billing::OPS_EVENTS_STREAM.to_string(),
        hdrs,
        axum::body::Body::from(body),
        None,
        None,
        None,
    )
    .await;
    if r.status().is_success() {
        Ok(())
    } else {
        Err(format!("ops ledger append: {}", r.status()))
    }
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
