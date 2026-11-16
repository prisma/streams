//! `/health` and `/readyz`: whether this instance should be routed to (R23-5 / R24-E; the
//! router's comment in `http.rs` says which questions the two answer).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use super::AppState;

/// Health: in BILLING_MODE=required an instance is NOT ready until
/// its billing prerequisites hold (round-22 item 10) — the read spool
/// is open and, on a rollup owner, the rollup DB is open. Both are
/// opened synchronously at startup, so a 503 here means startup-order
/// bugs or a lost OnceLock, and the platform should not route yet.
pub(super) async fn health_axum(State(state): State<Arc<AppState>>) -> Response {
    if let Some(reason) = state.tasks.readiness_reason() {
        return (StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }
    // Review item 6: in shadow/enforce an instance is NOT ready until
    // every auth feed has published an INITIAL snapshot — routing
    // traffic to a cell that would fail-closed (or shadow-count
    // nothing but noise) helps nobody.
    if state.auth.mode != crate::auth::AuthMode::Off {
        let unpublished = state.auth.unpublished_feeds();
        if !unpublished.is_empty() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("auth feeds not yet published: {}", unpublished.join(", ")),
            )
                .into_response();
        }
    }
    // A process that has never opened a shard cannot serve a single
    // append; answering `ok` keeps it in the load balancer forever
    // (CHAOS-2). Report unready so rollouts halt and traffic drains.
    if let Some(reason) = state.shards.unready_reason() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("shard storage unavailable: {reason}"),
        )
            .into_response();
    }
    // A fleet instance before its first view opens nothing (edge change #263): routing to it
    // only meets that refusal. The heartbeat's withdrawal and the unready watchdog do not read
    // this: a lone member that withdrew would never be named by a view.
    if state.ownership.awaits_view() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "fleet view not yet published",
        )
            .into_response();
    }
    if state.config.cli.billing_required() {
        let spool_ok = state.billing.read_spool_open();
        let rollup_ok = !state.config.cli.runs_rollup() || state.rollup.installed();
        if !spool_ok || !rollup_ok {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("billing not ready (spool={spool_ok}, rollup={rollup_ok})"),
            )
                .into_response();
        }
    }
    // R28: identity headers so the campaign verifier can compare the
    // running build against its manifest without auth (body stays "ok"
    // for existing probes).
    (
        [
            ("x-streams-git", env!("STREAMS_GIT_COMMIT")),
            ("x-streams-build-unix", env!("STREAMS_BUILD_UNIX")),
            ("x-streams-boot-id", state.runtime.identity.boot_id.as_str()),
        ],
        "ok",
    )
        .into_response()
}
