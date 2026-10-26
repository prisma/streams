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

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;

use super::super::AppState;
use super::super::read_memory::{SHED_DISCARD_WAIT, blamed_refusal};
use crate::application::queues::{FailureClass, fail};
use crate::auth::RequestPrincipal;

/// Bind the served request's project share and judge its load, once its
/// project is admitted.
pub(super) fn judge(state: &AppState, principal: Option<&RequestPrincipal>) {
    crate::admission::park::bind_principal(&state.quotas, principal);
    crate::admission::park::judge_load(state.config.cli.project_share_k);
}

/// The request's body, buffered whole up to the request-body limit, or the
/// refusal to answer: a blamed project's (its body read and dropped
/// first) or 413 `too_large`.
pub(super) async fn body(
    state: &AppState,
    principal: Option<&RequestPrincipal>,
    incoming: Body,
) -> Result<Bytes, Box<Response>> {
    let limit = state.config.cli.max_request_body_bytes;
    if let Some(refusal) = blamed_refusal(state, principal) {
        let bound = u64::try_from(limit).unwrap_or(u64::MAX);
        crate::admission::body::discard_within(incoming, bound, SHED_DISCARD_WAIT).await;
        return Err(Box::new(refusal));
    }
    axum::body::to_bytes(incoming, limit).await.map_err(|_| {
        let too_large = fail(FailureClass::TooLarge, "too_large", "body too large");
        Box::new(super::failure_response(too_large))
    })
}
