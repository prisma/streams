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

use axum::extract::{Path, State};
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

/// The operator surface's routes, every one behind `operator_gate`.
pub(crate) fn routes() -> axum::Router<Arc<AppState>> {
    use axum::routing::{get, post};
    axum::Router::new()
        .route("/operator", get(page))
        .route("/operator/data.json", get(data))
        .route("/operator/runbook", get(runbook))
        .route(
            "/operator/shards/{prefix}/repair-indexes",
            post(repair_indexes),
        )
        .route("/operator/topology.json", get(topology_view))
        .route("/operator/topology/split", post(topology_split))
        .route("/operator/topology/merge", post(topology_merge))
}

/// `GET /operator/topology.json` (edge change #251, S4-DESIGN §11): `{version, shards: [{prefix,
/// home, owner, born_version, state}], intent: null | {kind, op_id, sources, children, state,
/// started_ms, lease_until_ms, outcome}}`, the intent's state derived from its durable witnesses
/// (`topology::view`).
pub(crate) async fn topology_view(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    let body =
        crate::topology::view::topology(&state.shards, &state.ownership, &state.data_store).await;
    ([("cache-control", "no-store")], axum::Json(body)).into_response()
}

/// The split route's body.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SplitBody {
    prefix: String,
}

/// `POST /operator/topology/split` `{"prefix"}` (edge change #251, S4-DESIGN §11): starts the split
/// of the leaf `prefix` (`""` names the root shard) on this instance's topology loop and answers once
/// it started, while the split runs on. 202 `{op_id, version}`; 400 `bad_request`; 404
/// `unknown_prefix`; 409 `topology_busy {reason: "intent" | "override" | "fleet"}`, `instance_full {resident,
/// max}` or `not_ring_owner {owner}`; 422 `not_splittable`; 503 `topology_unavailable`.
pub(crate) async fn topology_split(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    use crate::topology::split::Refused;
    use axum::http::StatusCode;
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    let Ok(SplitBody { prefix }) = serde_json::from_slice(&body) else {
        let body = json!({"error": "bad_request", "message": "expected {\"prefix\": \"<bits>\"}"});
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    };
    let (status, body) = match state.shards.topology_ops().split(prefix.clone()).await {
        Ok(started) => (
            StatusCode::ACCEPTED,
            json!({"op_id": started.op_id, "version": started.version}),
        ),
        Err(Refused::UnknownPrefix) => (
            StatusCode::NOT_FOUND,
            json!({"error": "unknown_prefix", "prefix": prefix}),
        ),
        Err(Refused::Busy(reason)) => (
            StatusCode::CONFLICT,
            json!({"error": "topology_busy", "reason": reason}),
        ),
        Err(Refused::NotOwner(owner)) => (
            StatusCode::CONFLICT,
            json!({"error": "not_ring_owner", "owner": owner}),
        ),
        Err(Refused::InstanceFull { resident, max }) => (
            StatusCode::CONFLICT,
            json!({"error": "instance_full", "resident": resident, "max": max}),
        ),
        Err(Refused::NotSplittable(why)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": "not_splittable", "message": why}),
        ),
        Err(Refused::Unavailable(why) | Refused::RolledBack(why) | Refused::Unfinished(why)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "topology_unavailable", "message": why}),
        ),
    };
    (status, axum::Json(body)).into_response()
}

/// `POST /operator/topology/merge` `{"prefix"}` (edge change #251, S6-DESIGN §7): starts the merge
/// of `prefix`'s halves `<prefix>0` and `<prefix>1` into `prefix` on this instance's topology loop,
/// answered once it started, while the merge runs on. 202 `{op_id, version}`; 400 `bad_request`;
/// 404 `unknown_prefix`; 409 `topology_busy {reason: "intent" | "override" | "fleet"}`,
/// `instance_full {resident, max}` or `not_ring_owner {owner}`; 422 `not_mergeable`; 503
/// `topology_unavailable`.
pub(crate) async fn topology_merge(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    use crate::topology::merge::Refused;
    use axum::http::StatusCode;
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    let Ok(SplitBody { prefix }) = serde_json::from_slice(&body) else {
        let body = json!({"error": "bad_request", "message": "expected {\"prefix\": \"<bits>\"}"});
        return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
    };
    let (status, body) = match state.shards.topology_ops().merge(prefix.clone()).await {
        Ok(started) => (
            StatusCode::ACCEPTED,
            json!({"op_id": started.op_id, "version": started.version}),
        ),
        Err(Refused::UnknownPrefix) => (
            StatusCode::NOT_FOUND,
            json!({"error": "unknown_prefix", "prefix": prefix}),
        ),
        Err(Refused::Busy(reason)) => (
            StatusCode::CONFLICT,
            json!({"error": "topology_busy", "reason": reason}),
        ),
        Err(Refused::InstanceFull { resident, max }) => (
            StatusCode::CONFLICT,
            json!({"error": "instance_full", "resident": resident, "max": max}),
        ),
        Err(Refused::NotOwner(owner)) => (
            StatusCode::CONFLICT,
            json!({"error": "not_ring_owner", "owner": owner}),
        ),
        Err(Refused::NotMergeable(why)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"error": "not_mergeable", "message": why}),
        ),
        Err(Refused::Unavailable(why) | Refused::RolledBack(why)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "topology_unavailable", "message": why}),
        ),
    };
    (status, axum::Json(body)).into_response()
}

/// `POST /operator/shards/{prefix}/repair-indexes` (edge change #245; `root` names the root shard):
/// marks the shard's log for an index repair and retires its engine on this instance, so the next
/// touch reopens it and that one open recomputes the unsealed and usage-dirty indexes from every
/// tail before it serves (S3a §4.1). 202 `{prefix, repair: "next_open"}`; 404
/// `shard_not_open_here` when this instance has no engine open for the shard; 503
/// `repair_marker_unwritten` when the marker's write fails (nothing retired).
pub(crate) async fn repair_indexes(
    State(state): State<Arc<AppState>>,
    Path(prefix): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    use axum::http::StatusCode;
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    let shard = if prefix == "root" {
        String::new()
    } else {
        prefix.clone()
    };
    let open = state.shards.engines_by_prefix();
    let Some((_, engine)) = open.into_iter().find(|(p, _)| *p == shard) else {
        let body = json!({"error": "shard_not_open_here", "prefix": prefix});
        return (StatusCode::NOT_FOUND, axum::Json(body)).into_response();
    };
    if let Err(error) = crate::shard::open_index::mark_for_repair(&engine.db).await {
        let body = json!({"error": "repair_marker_unwritten", "message": error.to_string()});
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    }
    let reason = crate::shard_directory::RetirementReason::IndexRepair;
    state.shards.retire(&shard, reason, |_, _| true);
    let body = json!({"prefix": prefix, "repair": "next_open"});
    (StatusCode::ACCEPTED, axum::Json(body)).into_response()
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
        // Seal-index rows (directory, pointer, orphan) the local engines'
        // opens or folds skipped as undecodable (edge #222; cell-watch row 30).
        "sealed_index_corrupt_rows": state.shards.engines().iter().map(|e| e.sealed_index_corrupt_rows()).sum::<u64>(),
        // Physical reclamation (layout 6A package 6, edge #230): decrements
        // refused as inexact (cell-watch row 31), and dead incarnations not
        // yet purged and reclaimed with the oldest one's lateness past its
        // due instant (row 32).
        "gc_inexact_decrements": state.shards.engines().iter().map(|e| e.gc_inexact()).sum::<u64>(),
        "gc_unreclaimed_incarnations": state.shards.engines().iter().map(|e| e.gc_reclaim_lag().0).sum::<u64>(),
        "gc_oldest_unreclaimed_late_secs": state.shards.engines().iter().map(|e| e.gc_reclaim_lag().1).max().unwrap_or(0),
        // The shard logs' L0 watermark (edge #234; cell-watch row 33):
        // the most L0s any local shard log holds against the line, the
        // shards shedding appends and queue sends now, the writes shed and
        // the collector passes not started since each engine opened.
        "l0_max": state.shards.engines().iter().map(|e| e.l0_gauge().count).max().unwrap_or(0),
        "l0_line": crate::backpressure::l0_line(),
        "l0_shards_shedding": state.shards.engines().iter().filter(|e| e.l0_gauge().shedding).count(),
        "l0_write_sheds": state.shards.engines().iter().map(|e| e.l0_gauge().sheds).sum::<u64>(),
        "l0_collector_yields": state.shards.engines().iter().map(|e| e.l0_gauge().yields).sum::<u64>(),
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
