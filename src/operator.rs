//! Cell-wide operator dashboard at /operator, behind the deployment bearer
//! (SR-5, `operator_gate`): no unauthenticated metadata on this listener.
//!
//! The payload REMAINS restricted to operational metadata: instance load
//! vectors, shard-prefix bit strings, admission/shed counters, per-op store
//! latency, the last ops snapshot, auth-feed ages and counts, and the
//! runbook. It must never include stream names, customer identifiers,
//! tokens, keys, or signed URLs.
//!
//! Every section degrades independently: a failed object-store read renders
//! as `null` in /operator/data.json and "unavailable" in the page, never an
//! error page — the dashboard is most needed exactly when parts of the cell
//! are unhealthy.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::http::AppState;

/// Compiled-in copies: the binary is deployed without the repo checkout, so
/// the dashboard carries its own page and runbook.
const PAGE: &str = include_str!("operator.html");
const RUNBOOK: &str = include_str!("../RUNBOOK.md");

/// SR-5 (Søren review): the operator surface is authenticated in
/// every mode — no unauthenticated metadata endpoint rides the
/// customer-facing listener. The deployment bearer is the interim
/// operator credential until §14.2 platform operator identity lands.
fn operator_gate(
    state: &crate::http::AppState,
    headers: &axum::http::HeaderMap,
) -> Option<Response> {
    if crate::http::authorized(state, headers) {
        return None;
    }
    Some(
        (
            axum::http::StatusCode::UNAUTHORIZED,
            [("content-type", "text/plain")],
            "operator bearer required",
        )
            .into_response(),
    )
}

pub(crate) async fn page(
    State(state): State<Arc<crate::http::AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    (
        [
            ("content-type", "text/html; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        PAGE,
    )
        .into_response()
}

pub(crate) async fn runbook(
    State(state): State<Arc<crate::http::AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    (
        [
            ("content-type", "text/markdown; charset=utf-8"),
            ("cache-control", "max-age=300"),
        ],
        RUNBOOK,
    )
        .into_response()
}

pub(crate) async fn data(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    let now_ms = crate::shard::now_ms();

    // PR 6.1.1-C: the operator reads the cell through the repository —
    // one per-runtime authority, no second path to the store.
    let (heartbeats, desired) = state.fleet.operator_snapshot().await;

    let adm = state.admission.snapshot();
    let (read_memory, read_memory_capacity) = state.admission.read_memory();
    let shard_opens = state.shards.open_stats();
    let local = json!({
        "instance": state.ownership.instance(),
        "open_shards": state.shards.open_count(),
        "ring_active": state.ownership.ring_active(),
        "inflight": adm.inflight,
        "inflight_peak": adm.inflight_peak,
        "parked": state.admission.parked(),
        "read_memory_bytes": read_memory,
        "read_memory_capacity_bytes": read_memory_capacity,
        "admit_shed": adm.shed.total,
        "stream_shed": adm.shed.stream,
        "wedge_shed": adm.shed.wedge,
        "admit_max_inflight": adm.max_inflight,
        "admit_max_inflight_per_stream": adm.per_stream_cap,
        "rss_mb": crate::fleet::rss_bytes() as f64 / 1048576.0,
        "rss_shed_mb": adm.rss_shed_mb,
        "admit_shed_rss": adm.shed.rss,
        "admit_shed_rss_blamed": adm.shed.blamed,
        "admit_shed_inflight_blamed": adm.shed.blamed_inflight,
        "admit_shed_inflight": adm.shed.inflight,
        "admit_shed_survival": adm.shed.survival,
        "maintenance_backpressure": state.admission.maintenance_stats_json(),
        "sse_connections": adm.sse_connections,
        "sse_max_connections": adm.sse_effective_max,
        "usage_overflow_admits": state.runtime.usage.overflow_stats().0,
        // per-op-class store latency, sentinels, steal — non-destructive read
        "store": crate::store_timing::snapshot(60, false, &state.runtime.store_io, &shard_opens),
    });

    // The boot's origin (the cell watch's row 4): the boot id it replaced and
    // whether that boot learned of it, so a planned replacement is told from a
    // crash. Operator surface only.
    let origin = state
        .shards
        .supersession()
        .lineage(&state.data_store, state.runtime.clock.now().ms())
        .await;

    axum::Json(json!({
        "ts_ms": now_ms,
        "boot": {
            "id": state.runtime.identity.boot_id.as_str(),
            "predecessor": origin.predecessor,
            "predecessorSuperseded": origin.predecessor_superseded,
        },
        "local": local,
        // The last telemetry tick's snapshot (null before the first one),
        // cached: re-collecting here would drain the peak gauges.
        "ops": state.runtime.ops.last_snapshot().as_deref(),
        "feeds": state.auth.feed_json(now_ms / 1000),
        "fleet": {
            "heartbeats": heartbeats,
            "desired": desired,
        },
    }))
    .into_response()
}
