#![cfg(test)]

use super::{AppState, Stats, handle};
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{Method, StatusCode};
use axum::response::Response;
use futures_util::FutureExt;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(super) fn state(latency: Duration) -> Arc<AppState> {
    Arc::new(AppState {
        latency,
        discard_substr: None,
        objects: Mutex::new(BTreeMap::new()),
        uploads: Mutex::new(HashMap::new()),
        etag_counter: AtomicU64::new(1),
        upload_counter: AtomicU64::new(1),
        stats: Stats::default(),
    })
}

async fn request(state: &Arc<AppState>, method: Method, uri: &str, body: &'static str) -> Response {
    let request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::from(body))
        .unwrap();
    handle(State(state.clone()), request).await
}

async fn json_body(response: Response) -> serde_json::Value {
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

#[test]
fn detailed_cost_ledger_preserves_classes_statuses_and_tiers() {
    let stats = Stats::default();
    let empty = HashMap::new();
    for _ in 0..2 {
        stats.record(&Method::PUT, "shards/00/wal/1", &empty, StatusCode::OK);
    }
    for status in [200, 304, 404, 412, 403, 500] {
        stats.record(
            &Method::GET,
            "shards/00/wal/1",
            &empty,
            StatusCode::from_u16(status).unwrap(),
        );
    }
    stats.record(&Method::HEAD, "registry/doc", &empty, StatusCode::OK);
    stats.record(
        &Method::DELETE,
        "shards/00/wal/1",
        &empty,
        StatusCode::NO_CONTENT,
    );
    stats.record(
        &Method::POST,
        "",
        &HashMap::from([("delete".into(), "".into())]),
        StatusCode::OK,
    );
    stats.record(
        &Method::POST,
        "shards/00/history2/compacted/1.sst",
        &HashMap::from([("uploads".into(), "".into())]),
        StatusCode::OK,
    );
    stats.record(
        &Method::GET,
        "",
        &HashMap::from([("prefix".into(), "shards/00/history2/".into())]),
        StatusCode::OK,
    );
    assert_eq!(
        stats.detailed_snapshot(),
        json!({
            "cells": {
                "shard/wal/put": {"2xx": 2},
                "shard/wal/get": {"2xx": 1, "304": 1, "404": 1, "412": 1, "4xx": 1, "5xx": 1},
                "shard/wal/delete": {"2xx": 1},
                "hist/sst/multipart": {"2xx": 1},
                "hist/meta/list": {"2xx": 1},
                "registry/meta/head": {"2xx": 1},
                "other/meta/delete": {"2xx": 1}
            },
            "by_tier": {
                "shard": {"class_a": 2, "class_b": 1, "free": 6},
                "hist": {"class_a": 2, "class_b": 0, "free": 0},
                "registry": {"class_a": 0, "class_b": 1, "free": 0},
                "other": {"class_a": 0, "class_b": 0, "free": 1}
            },
            "total": {"class_a": 4, "class_b": 2, "free": 7}
        })
    );
}

#[tokio::test]
async fn storage_operations_feed_both_snapshots_and_current_object_census() {
    let state = state(Duration::ZERO);
    assert_eq!(
        request(&state, Method::PUT, "/b/shards/00/wal/1", "first")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(
            &state,
            Method::PUT,
            "/b/shards/00/history2/compacted/1.sst",
            "second"
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(&state, Method::GET, "/b/shards/00/wal/1", "")
            .await
            .status(),
        StatusCode::OK
    );
    let before = json_body(request(&state, Method::GET, "/_s3lite/stats2", "").await).await;
    assert_eq!(
        before["live_objects"],
        json!({"shard/wal": 1, "hist/sst": 1})
    );
    assert_eq!(
        request(&state, Method::DELETE, "/b/shards/00/wal/1", "")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    let detailed = json_body(request(&state, Method::GET, "/_s3lite/stats2", "").await).await;
    assert_eq!(detailed["live_objects"], json!({"hist/sst": 1}));
    assert_eq!(
        detailed["total"],
        json!({"class_a": 2, "class_b": 1, "free": 1})
    );
    let basic = json_body(request(&state, Method::GET, "/_s3lite/stats", "").await).await;
    assert_eq!(
        basic,
        json!({"put": 2, "get": 1, "head": 0, "delete": 1, "list": 0,
        "multipart": 0, "put_bytes": 11, "get_bytes": 5, "objects": 1})
    );
}

#[tokio::test]
async fn observation_endpoints_neither_wait_for_latency_nor_charge_requests() {
    let state = state(Duration::from_secs(3600));
    for path in ["/_s3lite/stats", "/_s3lite/stats2"] {
        let response = request(&state, Method::GET, path, "")
            .now_or_never()
            .expect("observations must complete without a timer");
        let body = json_body(response).await;
        assert!(body.is_object());
    }
    assert_eq!(
        state.stats.detailed_snapshot(),
        json!({"cells": {}, "by_tier": {}, "total": {"class_a": 0, "class_b": 0, "free": 0}})
    );
}

#[test]
fn poisoned_ledger_cannot_publish_or_accept_cost_observations() {
    let stats = Stats::default();
    let _poison = std::panic::catch_unwind(|| {
        let _held = stats.detailed.lock().unwrap();
        panic!("interrupt ledger update");
    });
    assert!(std::panic::catch_unwind(|| stats.detailed_snapshot()).is_err());
    assert!(
        std::panic::catch_unwind(|| stats.record(
            &Method::GET,
            "key",
            &HashMap::new(),
            StatusCode::OK
        ))
        .is_err()
    );
}
