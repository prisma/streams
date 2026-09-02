//! Cell-wide operator dashboard, served UNSECURED at /operator (explicit
//! product decision: on-call must see the cell without credentials).
//!
//! The surface is bearer-gated (SR-5); the payload REMAINS restricted to
//! operational metadata: instance load vectors, shard-prefix bit strings,
//! admission/shed counters, per-op store latency, and the runbook. It must
//! never include stream names, customer identifiers, tokens, keys, or
//! signed URLs.
//!
//! Every section degrades independently: a failed object-store read renders
//! as `null` in /operator/data.json and "unavailable" in the page, never an
//! error page — the dashboard is most needed exactly when parts of the cell
//! are unhealthy.

use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::{Value, json};

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

pub async fn page(
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

pub async fn runbook(
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

async fn read_json(store: &Arc<dyn ObjectStore>, path: &str) -> Option<Value> {
    let result = store.get(&ObjPath::from(path)).await.ok()?;
    let raw = result.bytes().await.ok()?;
    serde_json::from_slice(&raw).ok()
}

/// List fleet/<instance>.json heartbeats directly — slate's cell view is
/// the raw heartbeat set (2 s cadence, <10 s = live).
async fn read_heartbeats(store: &Arc<dyn ObjectStore>) -> Option<Vec<Value>> {
    let mut names: Vec<ObjPath> = Vec::new();
    let mut listing = store.list(Some(&ObjPath::from("fleet")));
    while let Some(meta) = listing.next().await {
        let meta = meta.ok()?;
        let loc = meta.location.as_ref();
        if loc.ends_with(".json") && !loc.ends_with("desired.json") {
            names.push(meta.location.clone());
        }
        if names.len() >= 64 {
            break; // a cell is bounded; a runaway listing is not our job
        }
    }
    let mut out = Vec::new();
    for name in names {
        let Ok(result) = store.get(&name).await else {
            continue;
        };
        let Ok(raw) = result.bytes().await else {
            continue;
        };
        if let Ok(v) = serde_json::from_slice::<Value>(&raw) {
            out.push(v);
        }
    }
    Some(out)
}

pub async fn data(State(state): State<Arc<AppState>>, headers: axum::http::HeaderMap) -> Response {
    if let Some(r) = operator_gate(&state, &headers) {
        return r;
    }
    let now_ms = crate::shard::now_ms();

    let (heartbeats, desired) = match &state.fleet_store {
        Some(fs) => (
            read_heartbeats(fs).await,
            read_json(fs, "fleet/desired.json").await,
        ),
        None => (None, None),
    };

    let local = json!({
        "instance": state.ownership.instance(),
        "open_shards": state.shards.open_count(),
        "ring_active": state.ownership.ring_active(),
        "inflight": state.inflight.load(std::sync::atomic::Ordering::Relaxed),
        "inflight_peak": state.inflight_peak.load(std::sync::atomic::Ordering::Relaxed),
        "admit_shed": state.admit_shed.load(std::sync::atomic::Ordering::Relaxed),
        "stream_shed": state.stream_shed.load(std::sync::atomic::Ordering::Relaxed),
        "wedge_shed": state.wedge_shed.load(std::sync::atomic::Ordering::Relaxed),
        "admit_max_inflight": state
            .admit_max_inflight
            .load(std::sync::atomic::Ordering::Relaxed),
        "admit_max_inflight_per_stream": state.admit_max_inflight_per_stream,
        "rss_mb": crate::fleet::rss_bytes() as f64 / 1048576.0,
        "rss_shed_mb": state.admit_rss_shed_mb,
        // per-op-class store latency, sentinels, steal — non-destructive read
        "store": crate::store_timing::snapshot(60, false),
    });

    axum::Json(json!({
        "ts_ms": now_ms,
        "local": local,
        "fleet": {
            "heartbeats": heartbeats,
            "desired": desired,
        },
    }))
    .into_response()
}
