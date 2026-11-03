//! The ownership a create meets before its project is admitted (tuning
//! P2 item 3a, edge change pending). A create's stream lives on the shard
//! its stream route maps to, so whether this instance may create it is
//! known from the ring without a store read
//! (`CreationService::foreign_create`). A create of a shard the ring gives
//! another instance is answered here, `409 not_ring_owner` with
//! `Streams-Replay-To`, before project admission: the forwarding layer
//! sends it to the owner (`http::forward`) and the owner admits the
//! project, so a fleet tracks and debits a project on its owner alone.
//! Only creates are decided here: any other request may address a segment
//! of a split stream that this instance owns, which only the descriptor
//! says.

use std::sync::Arc;

use axum::http::{Method, StatusCode};
use axum::response::Response;

use super::{AppState, ProductOperation, classify_route, perr, project_admission, strip_verb};
use crate::application::creation::CreationError;
use crate::auth::RequestPrincipal;
use crate::quota::QuotaGuard;

/// Project admission for a product request on `path`, after a create of a
/// shard another instance owns is refused without it.
pub(crate) fn admit(
    state: &Arc<AppState>,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    path: &str,
) -> Result<Option<QuotaGuard>, Box<Response>> {
    if let Some(refusal) = foreign_create(state, principal, method, path) {
        return Err(Box::new(refusal));
    }
    project_admission(state, principal).map_err(Box::new)
}

/// The refusal of a product create (a create or a fork of the collection
/// `path` names) whose shard the ring gives another instance, rendered as
/// the create renders it; `None` for every other request, and without a
/// verified project.
fn foreign_create(
    state: &Arc<AppState>,
    principal: Option<&RequestPrincipal>,
    method: &Method,
    path: &str,
) -> Option<Response> {
    let p = principal?;
    let route = classify_route(path).ok()?;
    let create = ProductOperation::resolve(&route, strip_verb(path).1, method);
    let (Some(ProductOperation::Create), super::ProductRoute::Collection { name }) =
        (create, route)
    else {
        return None;
    };
    let error = state
        .creation_service()
        // mt-lint: allow(stream-ref-construction): ingress — the product create's collection name qualified with the verified principal's project, as the product entry resolves its path
        .foreign_create(&p.project_id.stream_ref(&name))?;
    Some(creation_error(error, Some(p)))
}

/// A creation failure on the product surface: the product error shape, the
/// owner to replay to, and the project tag.
pub(super) fn creation_error(
    error: CreationError,
    principal: Option<&RequestPrincipal>,
) -> Response {
    use crate::application::creation::CreationFailure as F;
    let status = match error.kind {
        F::Invalid => StatusCode::BAD_REQUEST,
        F::Conflict => StatusCode::CONFLICT,
        F::Missing => StatusCode::NOT_FOUND,
        F::Gone => StatusCode::GONE,
        F::WrongKey => StatusCode::FORBIDDEN,
        F::Storage => StatusCode::INTERNAL_SERVER_ERROR,
        F::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        F::Overloaded => StatusCode::TOO_MANY_REQUESTS,
        F::Ambiguous => StatusCode::REQUEST_TIMEOUT,
        F::Opening => StatusCode::SERVICE_UNAVAILABLE,
    };
    let retryable = matches!(
        error.kind,
        F::Storage | F::Overloaded | F::Ambiguous | F::Opening
    );
    let mut response = perr(status, error.code, &error.message, None, retryable);
    if error.kind == F::WrongKey {
        response = crate::audit::tag(response, "wrong_key");
    }
    if let Some(owner) = error.owner.and_then(|v| v.parse().ok()) {
        response.headers_mut().insert("streams-replay-to", owner);
    }
    if let Some(p) = principal {
        response = crate::audit::tag_project(response, &p.project_id);
    }
    response
}
