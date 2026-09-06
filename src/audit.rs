//! Durable denial journal (docs/MULTITENANCY.md §10.4): enforce-mode
//! authorization and quota refusals append to the reserved
//! `_audit_events` system stream, so a security review reconstructs
//! who was denied what, when, on which cell — without trusting
//! instance logs.
//!
//! Same discipline as `_ops_events` (src/ops.rs): a bounded runtime
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
/// Fill-only-if-absent: the entry wrapper calls this with the gate's
/// principal for EVERY outgoing response, and it must never clobber a
/// classifier's more specific attribution.
pub fn tag_project(
    mut resp: axum::response::Response,
    project: &crate::tenant::ProjectId,
) -> axum::response::Response {
    if let Some(t) = resp.extensions_mut().get_mut::<DenialTag>()
        && t.project.is_none()
    {
        t.project = Some(project.as_str().to_string());
    }
    resp
}

/// Journaled route strings are bounded: the route is attacker-sized
/// (wildcard path remainder), and an unbounded copy would let an
/// unauthenticated denial storm pin queue memory and oversize drain
/// batches past the append body limit (a permanent drain wedge).
const ROUTE_MAX: usize = 256;

fn bounded_route(route: &str) -> String {
    if route.len() <= ROUTE_MAX {
        return route.to_string();
    }
    let mut end = ROUTE_MAX;
    while !route.is_char_boundary(end) {
        end -= 1;
    }
    route[..end].to_string()
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
    /// Gap markers only: how many denials were dropped at the queue
    /// cap. Carried as a FIELD (never only in the id) so a requeue
    /// overflow can restore the magnitude to the pending counter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped: Option<u64>,
}

/// A denial journal belongs to one server runtime and its durable cell ledger.
#[derive(Default)]
pub struct AuditJournal {
    sequence: AtomicU64,
    dropped: AtomicU64,
    gap: AtomicU64,
    queue: Mutex<VecDeque<AuditEvent>>,
}
impl AuditJournal {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
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
    let journal = &state.runtime.audit;
    if state.auth.mode != crate::auth::AuthMode::Enforce {
        return;
    }
    let Some(t) = resp.extensions().get::<DenialTag>() else {
        return;
    };
    // No usage key = no drain will EVER run: count the loss instead of
    // pinning the queue at cap for the runtime lifetime. The boot path
    // warns loudly that enforce without a usage key voids the journal.
    if state.billing.usage_key().is_none() {
        journal.dropped.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let ev = AuditEvent {
        v: 1,
        event_id: format!(
            "deny/{}/{}",
            state.runtime.identity.boot_id,
            journal.sequence.fetch_add(1, Ordering::Relaxed)
        ),
        event_time_ms: state.runtime.clock.now().ms(),
        cell: state.deployment.cell_id().as_str().to_string(),
        code: t.code.to_string(),
        project_id: t.project.clone(),
        route: bounded_route(route),
        method: method.to_string(),
        status: resp.status().as_u16(),
        dropped: None,
    };
    let mut g = journal.queue.lock().unwrap();
    if g.len() >= AUDIT_QUEUE_CAP {
        journal.dropped.fetch_add(1, Ordering::Relaxed);
        journal.gap.fetch_add(1, Ordering::Relaxed);
        return;
    }
    g.push_back(ev);
}

// Own the removed events across every await. Dropping the drain future,
// serialization failure, and append failure all restore the same event IDs.
struct PendingAudit<'a> {
    queue: &'a Mutex<VecDeque<AuditEvent>>,
    dropped: &'a AtomicU64,
    gap: &'a AtomicU64,
    events: Vec<AuditEvent>,
}
impl<'a> PendingAudit<'a> {
    fn take(
        queue: &'a Mutex<VecDeque<AuditEvent>>,
        dropped: &'a AtomicU64,
        gap: &'a AtomicU64,
    ) -> Self {
        let events = {
            let mut guard = queue.lock().unwrap();
            let len = guard.len().min(512);
            guard.drain(..len).collect()
        };
        Self {
            queue,
            dropped,
            gap,
            events,
        }
    }
}
impl Drop for PendingAudit<'_> {
    fn drop(&mut self) {
        if self.events.is_empty() {
            return;
        }
        let mut queue = self.queue.lock().unwrap();
        for event in self.events.drain(..).rev() {
            if queue.len() < AUDIT_QUEUE_CAP {
                queue.push_front(event);
            } else {
                let represented_gap = event.dropped;
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
async fn persist_audit_batch<F, Fut>(
    mut batch: PendingAudit<'_>,
    append: F,
) -> Result<usize, String>
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

/// Drain queued denials to `_audit_events`. Called from the telemetry
/// task; requeues on failure (order preserved).
pub async fn drain_audit_once(
    state: &std::sync::Arc<crate::http::AppState>,
) -> Result<usize, String> {
    let Some(key) = state.billing.usage_key() else {
        return Ok(0);
    };
    let journal = &state.runtime.audit;
    let mut batch = PendingAudit::take(&journal.queue, &journal.dropped, &journal.gap);
    let gap = journal.gap.swap(0, Ordering::Relaxed);
    if gap > 0 {
        // The id uses the shared SEQ, never the drop count: two gap
        // episodes with equal counts must not collide under the
        // mandatory dedupe-by-id, and the magnitude rides in the
        // `dropped` field so a requeue overflow can restore it.
        batch.events.push(AuditEvent {
            v: 1,
            event_id: format!(
                "deny-gap/{}/{}",
                state.runtime.identity.boot_id,
                journal.sequence.fetch_add(1, Ordering::Relaxed)
            ),
            event_time_ms: state.runtime.clock.now().ms(),
            cell: state.deployment.cell_id().as_str().to_string(),
            code: "audit_gap".into(),
            project_id: None,
            route: String::new(),
            method: String::new(),
            status: 0,
            dropped: Some(gap),
        });
    }
    if batch.events.is_empty() {
        return Ok(0);
    }
    persist_audit_batch(batch, |body| {
        crate::billing::system_append(state, crate::billing::AUDIT_EVENTS_STREAM, &key, body)
    })
    .await
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    fn event(id: &str) -> AuditEvent {
        AuditEvent {
            v: 1,
            event_id: id.into(),
            event_time_ms: 1,
            cell: "cell-test".into(),
            code: "denied".into(),
            project_id: None,
            route: String::new(),
            method: "GET".into(),
            status: 403,
            dropped: None,
        }
    }
    fn local_queue() -> Mutex<VecDeque<AuditEvent>> {
        Mutex::new(VecDeque::new())
    }
    #[tokio::test]
    async fn cancelled_audit_append_restores_batch_order_and_retry_ids() {
        let queue = local_queue();
        let dropped = AtomicU64::new(0);
        let gap = AtomicU64::new(0);
        queue
            .lock()
            .unwrap()
            .extend([event("first"), event("second")]);
        let entered = tokio::sync::Notify::new();
        let entered_sink = &entered;
        let mut drain = Box::pin(persist_audit_batch(
            PendingAudit::take(&queue, &dropped, &gap),
            |body| async move {
                let sent: Vec<AuditEvent> = serde_json::from_slice(&body).unwrap();
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
            queue.lock().unwrap().is_empty(),
            "entered the real batch sink after dequeue"
        );
        queue.lock().unwrap().push_back(event("newer"));
        drop(drain); // Cooperative cancellation drops the actual owned batch.
        let ids = || {
            queue
                .lock()
                .unwrap()
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(), ["first", "second", "newer"]);
        let failed = persist_audit_batch(PendingAudit::take(&queue, &dropped, &gap), |_| async {
            Err("retry".into())
        })
        .await;
        assert!(failed.is_err());
        assert_eq!(ids(), ["first", "second", "newer"]);
        assert_eq!(
            persist_audit_batch(PendingAudit::take(&queue, &dropped, &gap), |_| async {
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
    fn cancelled_audit_batch_overflow_preserves_full_gap_magnitude() {
        let queue = local_queue();
        let dropped = AtomicU64::new(0);
        let gap = AtomicU64::new(0);
        queue.lock().unwrap().push_back(event("pending"));
        let mut batch = PendingAudit::take(&queue, &dropped, &gap);
        batch.events.push({
            let mut event = event("gap-id");
            event.dropped = Some(17);
            event
        });
        queue
            .lock()
            .unwrap()
            .extend((0..AUDIT_QUEUE_CAP).map(|_| event("newer")));
        drop(batch);
        assert_eq!(queue.lock().unwrap().len(), AUDIT_QUEUE_CAP);
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
