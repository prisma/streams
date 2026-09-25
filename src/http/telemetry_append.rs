//! The owner-side receiver of the fleet's system-stream relay (round-21
//! blocker 5). The sender is `billing::system_append`; both halves run
//! `billing::append_local`, so a refusal here is the owner's own typed
//! decision.

use super::{
    AppState, InternalOperation, creation_error_response, err_resp, fleet_operation_authorized,
    internal_unauthorized, raw_key, render_append,
};
use crate::application::append::{AppendCode, FailureClass, fail};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use std::sync::Arc;

/// Fleet-internal telemetry append (round-21 blocker 5): the OWNER-side
/// target for system-stream relays. Fleet credential only; reserved
/// names only; creates the stream lazily with the carried system key.
pub(super) async fn internal_telemetry_append(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if !fleet_operation_authorized(&state, &headers, InternalOperation::TelemetryAppend) {
        return internal_unauthorized();
    }
    if !crate::billing::is_reserved_stream(&name) {
        return err_resp(
            StatusCode::FORBIDDEN,
            "not_system_stream",
            "telemetry-append accepts only reserved system streams",
        );
    }
    // Stage 7 review fix: the RECEIVER addresses the reserved stream
    // under the SYSTEM project — the identity the sender appended toward
    // and every reader (system_read, rollup_step) reads; the deployment
    // tenant would put relayed batches in a stream nobody reads.
    let Some(key) = raw_key(&headers, &state) else {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Stream-Encryption-Key required",
        );
    };
    let Ok(canonical) = crate::tenant::CanonicalStreamName::new(&name) else {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_name",
            "not a canonical stream name",
        );
    };
    let sref = crate::tenant::TenantStreamRef::new(crate::tenant::system_project(), canonical);
    // R25-E: authenticate before reading a byte. The body is read only
    // after every header-level refusal, under the configured limit the
    // local append path enforces and the senders budget their batches
    // against - never axum's implicit 2 MiB for a `Bytes` extractor.
    let limit = state.config.cli.max_request_body_bytes;
    let Ok(body) = axum::body::to_bytes(body, limit).await else {
        return render_append(fail(
            FailureClass::Invalid,
            AppendCode::TooLarge,
            "body too large",
        ));
    };
    // The same typed local path the sender took: a refusal here is the
    // owner's own decision (ownership included), never a status guess.
    match crate::billing::append_local(&state, sref, key, body).await {
        Ok(out) => render_append(Ok(out)),
        Err(crate::billing::LocalFailure::Key(m)) => {
            err_resp(StatusCode::BAD_REQUEST, "invalid_key", &m)
        }
        Err(crate::billing::LocalFailure::Append(e)) => render_append(Err(e)),
        Err(crate::billing::LocalFailure::Create(e)) => creation_error_response(e),
    }
}
