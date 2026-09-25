//! The product append body contract (external review §5): the committer
//! wire body, the record count the core will find in it and its permanent
//! capacity verdict are decided here, before the project's append-volume
//! debit and any lifecycle write.

use super::perr;
use crate::registry::StreamDesc;
use crate::usage::{CapacityRefusal, UsageService};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;

const MAX_BATCH_RECORDS: usize = 10_000;

/// A well-formed product append body, measured exactly as the append core
/// will measure it.
pub(super) struct AppendBody {
    /// The committer wire body: a single JSON value travels as `[value]`.
    pub(super) wire: Bytes,
    /// The records the core counts in `wire`.
    pub(super) count: usize,
    /// The core's own permanent verdict on `wire` and `count`: no fresh
    /// per-stream bucket admits them.
    pub(super) over_capacity: Option<CapacityRefusal>,
}

/// Whether the request names a producer. Its duplicate is recognized before
/// any later validation refusal (Stage 4 §5), so the core, not the handler,
/// refuses it over capacity; one header of the trio is enough, since a
/// partial trio is the core's 400 `invalid_producer`.
pub(super) fn names_a_producer(headers: &HeaderMap) -> bool {
    ["producer-id", "producer-epoch", "producer-seq"]
        .iter()
        .any(|h| headers.contains_key(*h))
}

/// The body's shape (Stage 4 §2.3, §5) and its capacity verdict, from the
/// same `usage` owner the core consults.
pub(super) fn parse_append_body(
    usage: &UsageService,
    desc: &StreamDesc,
    body: &Bytes,
    batch: bool,
) -> Result<AppendBody, Box<Response>> {
    let is_json = desc.is_json();
    if batch && !is_json {
        // Spec Stage 4 §2.3: no framed byte-batch format is standardized.
        return Err(Box::new(perr(
            StatusCode::METHOD_NOT_ALLOWED,
            "batch_unsupported_format",
            "records:batch requires a JSON stream",
            None,
            false,
        )));
    }
    // Validation order (Stage 4 §5): JSON syntax and batch shape are
    // checked BEFORE enqueue; the shared path handles producer
    // duplicate recognition ahead of later-validation rejections.
    let (wire, count): (Bytes, usize) = if is_json {
        if batch {
            let elems: Vec<&serde_json::value::RawValue> = match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(e) => {
                    return Err(Box::new(perr(
                        StatusCode::BAD_REQUEST,
                        "invalid_body",
                        &format!("batch must be a JSON array: {e}"),
                        None,
                        false,
                    )));
                }
            };
            if elems.is_empty() {
                return Err(Box::new(perr(
                    StatusCode::BAD_REQUEST,
                    "empty_batch",
                    "appendMany requires at least one record",
                    None,
                    false,
                )));
            }
            if elems.len() > MAX_BATCH_RECORDS {
                return Err(Box::new(perr(
                    StatusCode::BAD_REQUEST,
                    "batch_too_large",
                    "appendMany accepts at most 10,000 records",
                    None,
                    false,
                )));
            }
            (body.clone(), elems.len())
        } else {
            if serde_json::from_slice::<&serde_json::value::RawValue>(body).is_err() {
                return Err(Box::new(perr(
                    StatusCode::BAD_REQUEST,
                    "invalid_body",
                    "append requires one JSON value",
                    None,
                    false,
                )));
            }
            // [value]: one-level flattening stores exactly one message,
            // preserving array-valued records (retains a body slice; no
            // DOM reserialization).
            let mut w = Vec::with_capacity(body.len() + 2);
            w.push(b'[');
            w.extend_from_slice(body);
            w.push(b']');
            (Bytes::from(w), 1)
        }
    } else {
        if body.is_empty() {
            return Err(Box::new(perr(
                StatusCode::BAD_REQUEST,
                "empty_body",
                "append requires a non-empty body",
                None,
                false,
            )));
        }
        (body.clone(), 1)
    };
    // Capacity measures the bytes stored, as the core and its bucket do. The
    // wire form bounds them from above, so only a body over capacity on the
    // wire is measured exactly, by the owner that stores it.
    let over_capacity = usage
        .permanently_unadmittable(wire.len() as u64, count as u64)
        .and_then(|_| {
            let (records, _) = crate::application::append::stored_records(desc, &wire, usize::MAX);
            let stored = records.iter().map(|record| record.len() as u64).sum();
            usage.permanently_unadmittable(stored, count as u64)
        });
    Ok(AppendBody {
        wire,
        count,
        over_capacity,
    })
}

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
