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
use crate::quota::{QuotaGuard, QuotaRefusal};

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

impl AppState {
    /// Project admission of verified principal `p`, under the quotas it
    /// verified against (`project_admission`): its request counted, its
    /// project tracked in a tracker this instance's CONTAINER_TRACKER_MAX
    /// sizes (tuning item 3b).
    pub(super) fn admit_project(&self, p: &RequestPrincipal) -> Result<QuotaGuard, QuotaRefusal> {
        let quotas = self.quotas.sized_to(self.config.cli.project_tracker_max);
        quotas.admit_counted(&p.project_id, &p.quotas, crate::shard::now_ms())
    }
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
        F::Opening | F::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    let retryable = matches!(
        error.kind,
        F::Storage | F::Overloaded | F::Ambiguous | F::Opening
    );
    let mut response = if error.kind == F::Unavailable {
        // A failed registry read is answered as the product's own descriptor
        // reads answer it (edge changes #58, #64 and #242).
        super::internal::descriptor_unavailable()
    } else {
        perr(status, error.code, &error.message, None, retryable)
    };
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

#[cfg(test)]
mod tests {
    use super::creation_error;
    use crate::application::creation::{CreationError, CreationFailure};

    /// A wrong-key create is tagged `wrong_key` for the audit journal, as
    /// the product create tagged it before its rendering moved here; any
    /// other creation failure is not tagged by this rendering.
    #[test]
    fn only_a_wrong_key_create_is_tagged_wrong_key() {
        let failure = |kind, code| CreationError {
            kind,
            code,
            message: "m".into(),
            owner: None,
            retry_after: None,
        };
        let tag = |r: &axum::response::Response| {
            r.extensions()
                .get::<crate::audit::DenialTag>()
                .map(|t| t.code)
        };
        let wrong = creation_error(failure(CreationFailure::WrongKey, "wrong_key"), None);
        let conflict = creation_error(failure(CreationFailure::Conflict, "config_mismatch"), None);
        assert_eq!(
            (wrong.status().as_u16(), tag(&wrong)),
            (403, Some("wrong_key"))
        );
        assert_eq!((conflict.status().as_u16(), tag(&conflict)), (409, None));
    }

    /// Edge change #242: a product create whose registry read failed answers
    /// 503 `temporarily_unavailable`, retryable, `Retry-After: 1`, tagged
    /// with no project when there is none, as a failed descriptor read.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_failed_registry_read_answers_as_a_failed_descriptor_read() {
        let error = CreationError {
            kind: CreationFailure::Unavailable,
            code: "registry_unavailable",
            message: "store".into(),
            owner: None,
            retry_after: Some(1),
        };
        let response = creation_error(error, None);
        let status = response.status().as_u16();
        let retry = response.headers().get("retry-after").cloned();
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let want: serde_json::Value = serde_json::from_str(
            r#"{"error": {"code": "temporarily_unavailable",
                "message": "stream metadata could not be read; retry", "retryable": true}}"#,
        )
        .unwrap();
        assert_eq!(
            (status, retry, body),
            (503, Some("1".parse().unwrap()), want)
        );
    }
}
