//! The product append's error answer: the stable product error shape
//! (`perr`) over the typed append contract's failure, for a single append
//! and `records:batch` (WIRE-MATRIX §2.7, §2.8) and a `:seal`'s final record
//! (§2.5). The raw surface renders the same failure as its own envelope.
use axum::http::StatusCode;
use axum::response::Response;
use serde_json::json;

use crate::application::append::AppendFailure;

use super::answers::retry_unavailable;
use super::{append_failure_status, capacity_refused, perr};

pub(super) fn render_product_append_error(error: AppendFailure) -> Response {
    use crate::application::append::{AppendCode as C, FailureClass as F};
    if let Some(refusal) = error.capacity_refusal() {
        return capacity_refused(refusal);
    }
    let status = append_failure_status(&error);
    let (code, message, details, retryable) = match error.code {
        C::NotOwner => (
            "not_stream_owner",
            "another instance owns the target segment; retry through the router",
            None,
            true,
        ),
        C::ProducerGap => (
            "producer_gap",
            "producer sequence gap",
            Some(json!({"expected":error.expected(),"received":error.received()})),
            false,
        ),
        C::ProducerStale => (
            "stale_producer_epoch",
            "producer epoch is stale",
            Some(json!({"currentEpoch":error.producer_epoch()})),
            false,
        ),
        C::ProducerSequenceReused => (
            "producer_sequence_reused",
            "same producer sequence with a different request",
            None,
            false,
        ),
        C::ProducerEpochSeq => (
            "producer_epoch_must_start_at_zero",
            "a new producer epoch must start at sequence 0",
            None,
            false,
        ),
        C::StreamClosed => ("sealed", "collection is sealed", None, false),
        C::MaintenanceBackpressure => (
            "maintenance_backpressure",
            "maintenance backlog exceeds its bound; retry after it drains",
            None,
            true,
        ),
        C::ShardMoving if error.outcome_unknown() => (
            "outcome_unknown",
            "the shard moved while this append was in flight; it may or may not have been written; retry only if a second copy is acceptable, or append through a producer",
            None,
            false,
        ),
        C::ContentTypeMismatch => (
            "content_type_mismatch",
            "content type mismatch",
            None,
            false,
        ),
        _ => match error.class {
            F::Missing => ("not_found", "stream not found", None, false),
            F::Denied => ("stale_or_wrong_credentials", "forbidden", None, false),
            F::Conflict => (
                "conflict",
                "producer or configuration conflict",
                None,
                false,
            ),
            F::Invalid if status == StatusCode::PAYLOAD_TOO_LARGE => (
                "body_too_large",
                "request body exceeds the limit",
                None,
                false,
            ),
            F::Capacity => ("rate_limited", "admission or rate limit", None, true),
            F::Unavailable => ("temporarily_unavailable", "retry shortly", None, true),
            _ => ("append_failed", "append failed", None, false),
        },
    };
    let mut r = retry_unavailable(code, perr(status, code, message, details, retryable));
    if let Some(retry) = error.retry_after {
        r.headers_mut()
            .insert("retry-after", axum::http::HeaderValue::from(retry));
    }
    if let Some(owner) = error.owner
        && let Ok(owner) = axum::http::HeaderValue::from_str(&owner)
    {
        r.headers_mut().insert("streams-replay-to", owner);
    }
    if code == "stale_or_wrong_credentials" {
        r = crate::audit::tag(r, "stale_or_wrong_credentials");
    }
    r
}
