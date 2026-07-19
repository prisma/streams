//! Cell-wide operator dashboard, served UNSECURED at /operator (explicit
//! product decision: on-call must see the cell without credentials).
//!
//! Because the route is unauthenticated, the payload is restricted to
//! operational metadata: instance load vectors, shard-prefix bit strings,
//! component health, alert catalog, and the runbook. It must never include
//! stream names, customer identifiers, tokens, keys, or signed URLs.
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
/// the dashboard carries its own alert catalog and runbook.
const PAGE: &str = include_str!("operator.html");
const ALERT_RULES: &str = include_str!("../ops/prometheus-alerts.json");
const RUNBOOK: &str = include_str!("../RUNBOOK.md");

pub async fn page() -> Response {
    (
        [
            ("content-type", "text/html; charset=utf-8"),
            ("cache-control", "no-store"),
        ],
        PAGE,
    )
        .into_response()
}

pub async fn runbook() -> Response {
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

/// List fleet/<instance>.json heartbeats directly — the fallback cell view
/// when the aggregator has not published (bootstrap, lease outage).
async fn read_heartbeats(store: &Arc<dyn ObjectStore>) -> Option<Vec<Value>> {
    let mut names: Vec<ObjPath> = Vec::new();
    let mut listing = store.list(Some(&ObjPath::from("fleet")));
    while let Some(meta) = listing.next().await {
        let meta = meta.ok()?;
        if meta.location.as_ref().ends_with(".json") {
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

/// Active split/merge intents under split-intents/ on the shard store.
async fn read_reconfigurations(store: &Arc<dyn ObjectStore>) -> Option<Vec<Value>> {
    let mut out = Vec::new();
    let mut listing = store.list(Some(&ObjPath::from("split-intents")));
    while let Some(meta) = listing.next().await {
        let meta = meta.ok()?;
        if out.len() >= 32 {
            break;
        }
        let modified = meta.last_modified.timestamp_millis();
        let shard = crate::reconfiguration::prefix_from_fence_path(&meta.location)
            .unwrap_or_else(|_| meta.location.as_ref().to_string());
        let kind = match store.get(&meta.location).await {
            Ok(result) => match result.bytes().await {
                Ok(raw) => match crate::reconfiguration::decode_fence(&raw) {
                    Ok(crate::reconfiguration::FenceDocument::Split) => "split",
                    Ok(crate::reconfiguration::FenceDocument::Merge(_)) => "merge",
                    Ok(crate::reconfiguration::FenceDocument::Released(_)) => "released",
                    Ok(crate::reconfiguration::FenceDocument::ReleasedSplit(_)) => "released-split",
                    Err(_) => "undecodable",
                },
                Err(_) => "unreadable",
            },
            Err(_) => "unreadable",
        }
        .to_string();
        out.push(json!({"shard": shard, "kind": kind, "modified_ms": modified}));
    }
    Some(out)
}

pub async fn data(State(state): State<Arc<AppState>>) -> Response {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    // Cell view: prefer the epoch-fenced aggregate; fall back to raw
    // heartbeats so the dashboard still works while the aggregator is dark.
    let (aggregate, heartbeats, desired, routers) = match &state.operator_fleet_store {
        Some(fs) => {
            let aggregate = read_json(fs, "fleet.json").await;
            let heartbeats = if aggregate.is_none() {
                read_heartbeats(fs).await
            } else {
                None
            };
            let desired = read_json(fs, "desired.json").await;
            let mut routers = Vec::new();
            let mut listing = fs.list(Some(&ObjPath::from("routers")));
            while let Some(Ok(meta)) = listing.next().await {
                if routers.len() >= 16 {
                    break;
                }
                if let Some(v) = read_json(fs, meta.location.as_ref()).await {
                    routers.push(v);
                }
            }
            (aggregate, heartbeats, desired, Some(routers))
        }
        None => (None, None, None, None),
    };

    let reconfigurations = read_reconfigurations(&state.shard_store).await;

    let backup = state.backup.as_ref().map(|b| {
        let health = b.health();
        json!({
            "ready": b.ready(),
            "snapshot": health.snapshot,
            "recovery_scrub": health.recovery_scrub,
            "primary_scrub": health.primary_scrub,
        })
    });

    let components = json!({
        "topology": state.topology_ready.load(std::sync::atomic::Ordering::Acquire),
        "cells": state.cells_ready.load(std::sync::atomic::Ordering::Acquire),
        "fleet": state.fleet_ready.load(std::sync::atomic::Ordering::Acquire),
        "split": state.split_ready.load(std::sync::atomic::Ordering::Acquire),
        "merge": state.merge_ready.load(std::sync::atomic::Ordering::Acquire),
    });

    let local = json!({
        "instance": state.instance_name,
        "cell_id": state.cell_id,
        "release_id": state.fleet_capabilities.release_id,
        "topology_version": state.topology_version.load(std::sync::atomic::Ordering::Acquire),
        "open_shards": state.shards.read().unwrap().len(),
        "splitting": state.splitting_prefixes.read().unwrap().len(),
        "ring_active": state.ring_active.read().unwrap().clone(),
        "inflight": state.inflight.load(std::sync::atomic::Ordering::Relaxed),
        "inflight_peak": state.inflight_peak.load(std::sync::atomic::Ordering::Relaxed),
        "admit_shed": state.admit_shed.load(std::sync::atomic::Ordering::Relaxed),
        "admit_max_inflight": state.admit_max_inflight,
        "rss_mb": state.rss_mb_cached.load(std::sync::atomic::Ordering::Relaxed),
        "rss_shed_mb": state.admit_rss_shed_mb,
        // per-op-class store latency, sentinels, steal — non-destructive read
        "store": crate::store_timing::snapshot(60, false),
    });

    let alerts: Value = serde_json::from_str(ALERT_RULES).unwrap_or(Value::Null);

    axum::Json(json!({
        "ts_ms": now_ms,
        "local": local,
        "components": components,
        "backup": backup,
        "fleet": {
            "aggregate": aggregate,
            "heartbeats_fallback": heartbeats,
            "desired": desired,
            "routers": routers,
        },
        "reconfigurations": reconfigurations,
        "alert_rules": alerts,
    }))
    .into_response()
}
