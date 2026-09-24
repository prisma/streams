//! The product append body contract (external review §5).

use super::perr;
use crate::usage::CapacityRefusal;
use axum::{http::StatusCode, response::Response};

/// The ONE product spelling of the permanent per-stream capacity refusal:
/// 413 `payload_too_large` with its limit as `details {dimension, capacity,
/// requested}`, not retryable and with no `retry-after`, for append, batch
/// and the seal's final record alike. `body_too_large` stays the transport
/// body ceiling's code.
pub(super) fn capacity_refused(refusal: &CapacityRefusal) -> Response {
    let details = serde_json::json!({
        "dimension": refusal.dimension,
        "capacity": refusal.capacity,
        "requested": refusal.requested,
    });
    perr(
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        &refusal.to_string(),
        Some(details),
        false,
    )
}
