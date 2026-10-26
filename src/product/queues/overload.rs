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

use axum::body::Body;
use axum::http::Method;
use axum::response::Response;
use bytes::Bytes;

use super::super::read_memory::{SHED_DISCARD_WAIT, blamed_refusal, memory_pressure};
use super::super::{AppState, project_memory_gate};
use crate::admission::body::BodyRefusal;
use crate::application::queues::{FailureClass, fail};
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
/// answer: before the body, a blamed project's or a write (`POST`) of a
/// project over its memory line (429 `project_memory_pressure`, as an
/// append's), each with the body read and dropped first; while buffering,
/// 429 `project_memory_pressure` when the body would take its project past
/// the line, or 413 `too_large`.
pub(super) async fn body(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    incoming: Body,
) -> Result<(Bytes, Option<BufferedBodyGuard>), Box<Response>> {
    let limit = state.config.cli.max_request_body_bytes;
    let refusal = blamed_refusal(state, principal).or_else(|| {
        (method == Method::POST)
            .then(|| project_memory_gate(state, principal))
            .flatten()
    });
    if let Some(refusal) = refusal {
        let bound = u64::try_from(limit).unwrap_or(u64::MAX);
        crate::admission::body::discard_within(incoming, bound, SHED_DISCARD_WAIT).await;
        return Err(Box::new(refusal));
    }
    crate::admission::body::buffer_within(
        incoming,
        limit,
        principal.and_then(|p| state.quotas.pressure_handle(&p.project_id)),
        state.admission.project_memory_pressure_bytes(),
    )
    .await
    .map_err(|refusal| {
        Box::new(match refusal {
            BodyRefusal::TooLarge => {
                super::failure_response(fail(FailureClass::TooLarge, "too_large", "body too large"))
            }
            BodyRefusal::MemoryPressure => memory_pressure(principal.map(|p| &p.project_id)),
        })
    })
}
