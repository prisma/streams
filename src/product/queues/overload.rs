//! Queue requests under the cell's overload gates (layout 6A package 1,
//! DESIGN-6A §4.1): on a cell that streams and queues share, a queue
//! request meets the gates a product request meets, so a queue flood is
//! shed where an append flood is and never sheds its neighbours instead.
//!
//! Fair shedding (`admission::fair_shed`, edge change #184): once its
//! project is admitted, a verified queue request binds its project's share
//! and is judged by the project's share of the cell's recently offered
//! requests, which project admission already counts. Over the RSS shed
//! line or at the in-flight cap a blamed project's every queue request is
//! refused (`read_memory::blamed_refusal`), its body read and dropped
//! first, as a product request's is.
//!
//! The project memory line (round 13 and shared cells H3): a queue write
//! (a `POST`: a send, a claim, a settle or a verb) is refused while its
//! project's estimated pressure is over its line, as an append is, and
//! every queue body is charged to its project's buffered-body pressure
//! while it is buffered and until the queue answers (`admission::body`).
//!
//! The RSS write shed (R25-E, edge changes #183 and #184): over the shed
//! line a send or a settle is refused before its body is read, as an
//! append is; claims, the other verbs and the reads are not. And at the
//! in-flight write cap a send or a settle is refused once its body is
//! buffered, as an append is (`admit_write_inflight`).

use axum::body::Body;
use axum::http::Method;
use axum::response::Response;
use bytes::Bytes;

use super::super::append_error::render_product_append_error;
use super::super::read_memory::{SHED_DISCARD_WAIT, blamed_refusal, memory_pressure};
use super::super::{AppState, project_memory_gate};
use super::Resource;
use crate::admission::body::BodyRefusal;
use crate::application::append::{AppendCode, AppendFailure, FailureClass as AppendFailureClass};
use crate::application::queues::{FailureClass, SettleInput, SettleVerb, fail};
use crate::auth::RequestPrincipal;
use crate::quota::BufferedBodyGuard;

/// Bind the served request's project share and judge its load, once its
/// project is admitted.
pub(super) fn judge(state: &AppState, principal: Option<&RequestPrincipal>) {
    crate::admission::park::bind_principal(&state.quotas, principal);
    crate::admission::park::judge_load(state.config.cli.project_share_k);
}

/// The request's body, buffered whole up to the request-body limit and
/// charged to its project while the guard is held, or the refusal to
/// answer: before the body, a blamed project's, a write (`POST`) of a
/// project over its memory line (429 `project_memory_pressure`, as an
/// append's) or a send or settle the RSS write gate sheds (`rss_shed`),
/// each with the body read and dropped first; while buffering,
/// 429 `project_memory_pressure` when the body would take its project past
/// the line, or 413 `too_large`; once buffered, a send or settle the
/// in-flight write gate refuses (`inflight_refused`).
pub(super) async fn body(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    resource: &Resource,
    incoming: Body,
) -> Result<(Bytes, Option<BufferedBodyGuard>), Box<Response>> {
    let limit = state.config.cli.max_request_body_bytes;
    let refusal = blamed_refusal(state, principal).or_else(|| {
        (method == Method::POST)
            .then(|| project_memory_gate(state, principal))
            .flatten()
    });
    let refusal = match refusal {
        Some(refusal) => Some(refusal),
        None => rss_shed(state, resource).await,
    };
    if let Some(refusal) = refusal {
        let bound = u64::try_from(limit).unwrap_or(u64::MAX);
        crate::admission::body::discard_within(incoming, bound, SHED_DISCARD_WAIT).await;
        return Err(Box::new(refusal));
    }
    let buffered = crate::admission::body::buffer_within(
        incoming,
        limit,
        principal.and_then(|p| state.quotas.pressure_handle(&p.project_id)),
        (state.admission).project_memory_line(super::super::read_memory::weight_of(principal)),
    )
    .await
    .map_err(|refusal| {
        Box::new(match refusal {
            BodyRefusal::TooLarge => {
                super::failure_response(fail(FailureClass::TooLarge, "too_large", "body too large"))
            }
            BodyRefusal::MemoryPressure => memory_pressure(principal.map(|p| &p.project_id)),
        })
    })?;
    let refusal = match extend_shed(state, resource, &buffered.0).await {
        Some(refusal) => Some(refusal),
        None => inflight_refused(state, resource).await,
    };
    match refusal {
        Some(refusal) => Err(Box::new(refusal)),
        None => Ok(buffered),
    }
}

/// The in-flight write gate's refusal of a send or a settle, as an append
/// meets it once its body is buffered (`AdmissionController::
/// admit_write_inflight`, edge changes #79 and #185: 429 `rate_limited`,
/// Retry-After 1, after a 25 ms tarpit; a spared project passes up to the
/// cap x 5/4).
async fn inflight_refused(state: &AppState, resource: &Resource) -> Option<Response> {
    if !writes_like_append(resource) || state.admission.admit_write_inflight().is_ok() {
        return None;
    }
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let failure = AppendFailure::new(
        AppendFailureClass::Capacity,
        AppendCode::Overloaded,
        "instance at admission capacity; retry",
    )
    .retry(1);
    Some(render_product_append_error(failure))
}

/// Whether the request writes as an append does: a send (one message or a
/// batch) or a settle.
fn writes_like_append(resource: &Resource) -> bool {
    matches!(
        resource,
        Resource::Messages | Resource::MessagesBatch | Resource::GroupVerb("settle")
    )
}

/// The RSS write gate's refusal before the body, as an append meets it
/// (`application::append::memory_gate`, edge changes #183 and #184: 429
/// `rate_limited`, Retry-After 2, after the gate's tarpit; a spared project
/// passes up to the hard line): of a send, and of a settle only past the
/// hard line (`admit_lease_ending_write`, #191 as ruled), its extends
/// judged once its body is read (`extend_shed`).
async fn rss_shed(state: &AppState, resource: &Resource) -> Option<Response> {
    if *resource == Resource::GroupVerb("settle") {
        let reserved = state.runtime.history.budget.reserved_bytes();
        state.admission.admit_lease_ending_write(reserved).err()?;
        return Some(memory_refusal().await);
    }
    if !writes_like_append(resource) {
        return None;
    }
    let gate = crate::application::append::memory_gate(&state.admission, &state.runtime.history);
    let failure = gate.await.err()?;
    Some(render_product_append_error(failure))
}

/// The RSS write gate's refusal of a settle that extends a lease, once its
/// body is read: an extend keeps its lease and is a write as an append is,
/// under the append's rule; a settle that only ends leases passed before
/// its body. A body that does not parse is the handler's 400.
async fn extend_shed(state: &AppState, resource: &Resource, body: &Bytes) -> Option<Response> {
    if *resource != Resource::GroupVerb("settle") {
        return None;
    }
    let input = serde_json::from_slice::<SettleInput>(body).ok()?;
    if !input.ops.iter().any(|op| op.op == SettleVerb::Extend) {
        return None;
    }
    let gate = crate::application::append::memory_gate(&state.admission, &state.runtime.history);
    let failure = gate.await.err()?;
    Some(render_product_append_error(failure))
}

/// The RSS shed's answer, as `application::append::memory_gate` renders it
/// after its tarpit: 429 `rate_limited`, Retry-After 2.
async fn memory_refusal() -> Response {
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let failure = AppendFailure::new(
        AppendFailureClass::Capacity,
        AppendCode::Overloaded,
        "instance memory pressure; retry",
    )
    .retry(2);
    render_product_append_error(failure)
}
