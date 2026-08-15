//! Durable denial journal (docs/MULTITENANCY.md §10.4): enforce-mode
//! authorization and quota refusals append to the reserved
//! `_audit_events` system stream, so a security review reconstructs
//! who was denied what, when, on which cell — without trusting
//! instance logs.
//!
//! Same discipline as `_ops_events` (src/ops.rs): a bounded process
//! queue that NEVER blocks or fails the refused request, a durable
//! drop counter, and a gap event once capacity returns. The journal
//! records only facts: `project_id` is filled exclusively from a
//! VERIFIED principal — an unverified token's claims never reach the
//! journal as identity.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub const AUDIT_QUEUE_CAP: usize = 4096;

/// Response extension attached where a refusal is CLASSIFIED (the
/// auth/quota response builders), read once where the response leaves
/// the product surface. Traveling inside the Response keeps every
/// intermediate signature unchanged.
#[derive(Clone, Debug)]
pub struct DenialTag {
    pub code: &'static str,
    /// Verified principal's project, when one existed at the denial.
    pub project: Option<String>,
}

/// Attach the denial class to a refusal response.
pub fn tag(mut resp: axum::response::Response, code: &'static str) -> axum::response::Response {
    resp.extensions_mut().insert(DenialTag {
        code,
        project: None,
    });
    resp
}

/// Fill the VERIFIED project on an already-tagged refusal (a no-op on
/// untagged responses, so call sites never need to know the class).
pub fn tag_project(
    mut resp: axum::response::Response,
    project: &crate::tenant::ProjectId,
) -> axum::response::Response {
    if let Some(t) = resp.extensions_mut().get_mut::<DenialTag>() {
        t.project = Some(project.as_str().to_string());
    }
    resp
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub v: u16,
    pub event_id: String,
    pub event_time_ms: i64,
    #[serde(default)]
    pub cell: String,
    /// Denial class: the auth error kind or quota refusal code.
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub route: String,
    pub method: String,
    pub status: u16,
}

static SEQ: AtomicU64 = AtomicU64::new(0);
pub static AUDIT_DROPPED: AtomicU64 = AtomicU64::new(0);
static GAP_PENDING: AtomicU64 = AtomicU64::new(0);

fn q() -> &'static Mutex<VecDeque<AuditEvent>> {
    static Q: std::sync::OnceLock<Mutex<VecDeque<AuditEvent>>> = std::sync::OnceLock::new();
    Q.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// Observe a product-surface response on its way out. Enforce-mode
/// denials (a `DenialTag` extension) journal; everything else is a
/// no-op. Never blocks and never alters the response.
pub fn observe_denial(
    state: &crate::http::AppState,
    route: &str,
    method: &axum::http::Method,
    resp: &axum::response::Response,
) {
    if state.auth.mode != crate::auth::AuthMode::Enforce {
        return;
    }
    let Some(t) = resp.extensions().get::<DenialTag>() else {
        return;
    };
    let ev = AuditEvent {
        v: 1,
        event_id: format!(
            "deny/{}/{}",
            crate::billing::boot_id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ),
        event_time_ms: crate::shard::now_ms(),
        cell: state.cell_id.clone(),
        code: t.code.to_string(),
        project_id: t.project.clone(),
        route: route.to_string(),
        method: method.to_string(),
        status: resp.status().as_u16(),
    };
    let mut g = q().lock().unwrap();
    if g.len() >= AUDIT_QUEUE_CAP {
        AUDIT_DROPPED.fetch_add(1, Ordering::Relaxed);
        GAP_PENDING.fetch_add(1, Ordering::Relaxed);
        return;
    }
    g.push_back(ev);
}

/// Drain queued denials to `_audit_events`. Called from the telemetry
/// task; requeues on failure (order preserved).
pub async fn drain_audit_once(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(key) = state.usage_key.clone() else {
        return Ok(0);
    };
    let mut batch: Vec<AuditEvent> = {
        let mut g = q().lock().unwrap();
        let n = g.len().min(512);
        g.drain(..n).collect()
    };
    let gap = GAP_PENDING.swap(0, Ordering::Relaxed);
    if gap > 0 {
        batch.push(AuditEvent {
            v: 1,
            event_id: format!("deny-gap/{}/{}", crate::billing::boot_id(), gap),
            event_time_ms: crate::shard::now_ms(),
            cell: state.cell_id.clone(),
            code: "audit_gap".into(),
            project_id: None,
            route: String::new(),
            method: String::new(),
            status: 0,
        });
    }
    if batch.is_empty() {
        return Ok(0);
    }
    let body = serde_json::to_vec(&batch).map_err(|e| e.to_string())?;
    match crate::billing::system_append(state, crate::billing::AUDIT_EVENTS_STREAM, &key, body)
        .await
    {
        Ok(()) => Ok(batch.len()),
        Err(e) => {
            let mut g = q().lock().unwrap();
            for ev in batch.into_iter().rev() {
                if g.len() < AUDIT_QUEUE_CAP {
                    g.push_front(ev);
                } else {
                    AUDIT_DROPPED.fetch_add(1, Ordering::Relaxed);
                    GAP_PENDING.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e)
        }
    }
}
