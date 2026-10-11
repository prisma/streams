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
        put_bytes_per_sec: AtomicU64::new(0),
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
                "shard": {"class_a": 2, "class_b": 2, "free": 5},
                "hist": {"class_a": 2, "class_b": 0, "free": 0},
                "registry": {"class_a": 0, "class_b": 1, "free": 0},
                "other": {"class_a": 0, "class_b": 0, "free": 1}
            },
            "total": {"class_a": 4, "class_b": 3, "free": 6}
        })
    );
}

/// A stream descriptor lives under the registry
/// (`registry/v4/projects/<hex>/streams/<hex>.json`, src/registry.rs), so
/// its reads, writes and catalog LISTs are the registry tier, not history,
/// although the key contains `streams/`.
#[test]
fn registry_descriptors_are_the_registry_tier() {
    let stats = Stats::default();
    let empty = HashMap::new();
    let descriptor = "registry/v4/projects/70726f6a/streams/6f72.json";
    stats.record(&Method::PUT, descriptor, &empty, StatusCode::OK);
    stats.record(&Method::GET, descriptor, &empty, StatusCode::NOT_MODIFIED);
    stats.record(
        &Method::GET,
        "",
        &HashMap::from([(
            "prefix".into(),
            "registry/v4/projects/70726f6a/streams/".into(),
        )]),
        StatusCode::OK,
    );
    assert_eq!(
        stats.detailed_snapshot()["by_tier"],
        json!({"registry": {"class_a": 2, "class_b": 0, "free": 1}})
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
async fn live_bytes_census_sums_current_object_lengths_per_tier_and_kind() {
    let state = state(Duration::ZERO);
    let monthly = "/b/p/telemetry/usage-monthly/acct/proj/0a/2026-10.json";
    let part = "/b/p/shards/00/history2/compacted/2.sst";
    let steps = [
        (Method::PUT, "/b/p/shards/00/wal/1", "first", StatusCode::OK),
        (
            Method::PUT,
            "/b/p/shards/00/wal/2",
            "doomed-bytes",
            StatusCode::OK,
        ),
        (
            Method::PUT,
            "/b/p/shards/00/history2/compacted/1.sst",
            "second",
            StatusCode::OK,
        ),
        (
            Method::PUT,
            "/b/p/telemetry/usage-rollup/v2/p0/wal/1.sst",
            "abc",
            StatusCode::OK,
        ),
        (Method::PUT, monthly, "{}", StatusCode::OK),
        // An overwrite replaces the object's bytes; it adds none.
        (Method::PUT, monthly, "{\"n\":12}", StatusCode::OK),
        (
            Method::DELETE,
            "/b/p/shards/00/wal/2",
            "",
            StatusCode::NO_CONTENT,
        ),
        (Method::POST, &format!("{part}?uploads"), "", StatusCode::OK),
        (
            Method::PUT,
            &format!("{part}?partNumber=1&uploadId=u1"),
            "part-one-",
            StatusCode::OK,
        ),
        (
            Method::PUT,
            &format!("{part}?partNumber=2&uploadId=u1"),
            "two",
            StatusCode::OK,
        ),
        (
            Method::POST,
            &format!("{part}?uploadId=u1"),
            "",
            StatusCode::OK,
        ),
        (
            Method::GET,
            "/b?list-type=2&prefix=p%2Ftelemetry%2Fusage-rollup%2Fv2%2Fp0%2Fwal%2F",
            "",
            StatusCode::OK,
        ),
    ];
    for (method, uri, body, status) in steps {
        assert_eq!(
            request(&state, method, uri, body).await.status(),
            status,
            "{uri}"
        );
    }
    let stats2 = json_body(request(&state, Method::GET, "/_s3lite/stats2", "").await).await;
    assert_eq!(
        stats2["live_objects"],
        json!({"hist/sst": 2, "shard/wal": 1, "telemetry/meta": 1, "telemetry/wal": 1})
    );
    assert_eq!(stats2["cells"]["telemetry/meta/list"], json!({"2xx": 1}));
    assert_eq!(
        stats2["live_bytes"],
        json!({
            "cells": {
                "hist/sst": {"objects": 2, "bytes": 18},
                "shard/wal": {"objects": 1, "bytes": 5},
                "telemetry/meta": {"objects": 1, "bytes": 8},
                "telemetry/wal": {"objects": 1, "bytes": 3}
            },
            "total": {"objects": 5, "bytes": 34}
        })
    );
}

#[tokio::test]
async fn live_bytes_counts_a_discarded_body_at_its_original_length() {
    let state = AppState::new(Duration::ZERO, Some("compacted".into()));
    let key = "/b/p/shards/00/history2/compacted/9.sst";
    let put = request(&state, Method::PUT, key, "discarded-body").await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(
        state.live_bytes(),
        json!({
            "cells": {"hist/sst": {"objects": 1, "bytes": 14}},
            "total": {"objects": 1, "bytes": 14}
        })
    );
}

#[tokio::test]
async fn open_multipart_parts_are_stored_and_sent_bytes_until_completed_or_aborted() {
    let state = state(Duration::ZERO);
    let done = "/b/p/shards/00/history2/compacted/3.sst";
    let dropped = "/b/p/shards/00/compacted/4.sst";
    let steps = [
        (Method::POST, format!("{done}?uploads")),
        (Method::PUT, format!("{done}?partNumber=1&uploadId=u1")),
        (Method::PUT, format!("{done}?partNumber=2&uploadId=u1")),
        (Method::POST, format!("{dropped}?uploads")),
        (Method::PUT, format!("{dropped}?partNumber=1&uploadId=u2")),
    ];
    for ((method, uri), body) in steps.into_iter().zip(["", "part-one-", "two", "", "abcd"]) {
        assert_eq!(
            request(&state, method, &uri, body).await.status(),
            StatusCode::OK
        );
    }
    let put_bytes = |state: &Arc<AppState>| state.stats.snapshot(0)["put_bytes"].clone();
    assert_eq!(
        state.live_bytes(),
        json!({
            "cells": {
                "hist/in_progress_multipart": {"objects": 1, "bytes": 12},
                "shard/in_progress_multipart": {"objects": 1, "bytes": 4}
            },
            "total": {"objects": 2, "bytes": 16}
        })
    );
    assert_eq!(put_bytes(&state), json!(16));
    let complete = format!("{done}?uploadId=u1");
    let abort = format!("{dropped}?uploadId=u2");
    let complete = request(&state, Method::POST, &complete, "").await;
    assert_eq!(complete.status(), StatusCode::OK);
    let abort = request(&state, Method::DELETE, &abort, "").await;
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        state.live_bytes(),
        json!({
            "cells": {"hist/sst": {"objects": 1, "bytes": 12}},
            "total": {"objects": 1, "bytes": 12}
        })
    );
    // Completion assembles parts already sent; the aborted part was sent too.
    assert_eq!(put_bytes(&state), json!(16));
}

#[test]
fn poisoned_upload_map_reports_no_partial_live_bytes() {
    let state = state(Duration::ZERO);
    let _poison = std::panic::catch_unwind(|| {
        let _held = state.uploads.lock().unwrap();
        panic!("interrupt upload update");
    });
    assert_eq!(state.live_bytes(), json!({"poisoned": true}));
}

#[test]
fn poisoned_object_map_reports_no_partial_live_bytes() {
    let state = state(Duration::ZERO);
    let _poison = std::panic::catch_unwind(|| {
        let _held = state.objects.lock().unwrap();
        panic!("interrupt object update");
    });
    assert_eq!(state.live_bytes(), json!({"poisoned": true}));
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

/// L6 spike: every path that adds or removes census bytes books them in the
/// byte ledger under the census locks, so the per-cell PUT bytes sum to the
/// global counter and live bytes equal put minus out, per tier and in total.
#[tokio::test]
async fn byte_ledger_reconciles_with_the_global_counter_and_the_live_census() {
    let state = state(Duration::ZERO);
    let seg = "/b/p/shards/00/hseg/0000000000000001/0000000000000001";
    let part = "/b/p/shards/00/history2/compacted/2.sst";
    let gone = "/b/p/shards/00/compacted/9.sst";
    let steps: [(Method, String, &'static str, u16); 14] = [
        (Method::PUT, "/b/p/shards/00/wal/1".into(), "first", 200),
        (Method::PUT, seg.into(), "segment-body", 200),
        // a conditional create that loses: sent, rejected
        (Method::PUT, seg.into(), "xx", 200),
        (Method::PUT, "/b/p/shards/00/wal/1".into(), "overwrite!", 200),
        (Method::DELETE, "/b/p/shards/00/wal/1".into(), "", 204),
        (Method::POST, format!("{part}?uploads"), "", 200),
        (Method::PUT, format!("{part}?partNumber=1&uploadId=u1"), "p1-old", 200),
        (Method::PUT, format!("{part}?partNumber=1&uploadId=u1"), "p1", 200),
        (Method::PUT, format!("{part}?partNumber=2&uploadId=u1"), "p2", 200),
        (Method::POST, format!("{part}?uploadId=u1"), "", 200),
        (Method::PUT, format!("{part}?partNumber=3&uploadId=u1"), "late", 404),
        (Method::POST, format!("{gone}?uploads"), "", 200),
        (Method::PUT, format!("{gone}?partNumber=1&uploadId=u2"), "aborted", 200),
        (Method::DELETE, format!("{gone}?uploadId=u2"), "", 204),
    ];
    for (method, uri, body, status) in steps {
        let mut req = axum::http::Request::builder().method(method).uri(&uri);
        if body == "xx" {
            req = req.header("if-none-match", "*");
        }
        let response = handle(State(state.clone()), req.body(Body::from(body)).unwrap()).await;
        let want = if body == "xx" { 412 } else { status };
        assert_eq!(response.status().as_u16(), want, "{uri}");
    }
    let stats2 = json_body(request(&state, Method::GET, "/_s3lite/stats2", "").await).await;
    let put = &stats2["put_bytes"];
    // 5 + 12 + 2 + 10 + 6 + 2 + 2 + 4 + 7
    assert_eq!(put["total"], json!(50));
    assert_eq!(put["cells_sum"], json!(50));
    assert_eq!(put["cells"]["hseg/seg/put"], json!(14));
    assert_eq!(put["cells"]["shard/wal/put"], json!(15));
    assert_eq!(put["cells"]["hist/sst/multipart"], json!(14));
    assert_eq!(put["cells"]["shard/sst/multipart"], json!(7));
    assert_eq!(
        put["out"]["hseg/seg"],
        json!({"rejected": 2, "replaced": 0, "deleted": 0, "aborted": 0})
    );
    assert_eq!(
        put["out"]["shard/wal"],
        json!({"rejected": 0, "replaced": 5, "deleted": 10, "aborted": 0})
    );
    assert_eq!(
        put["out"]["hist/sst"],
        json!({"rejected": 4, "replaced": 6, "deleted": 0, "aborted": 0})
    );
    assert_eq!(
        put["out"]["shard/sst"],
        json!({"rejected": 0, "replaced": 0, "deleted": 0, "aborted": 7})
    );
    assert_eq!(
        put["check"],
        json!({"cells_sum_eq_total": true, "live_eq_put_minus_out": true, "tiers_reconcile": true})
    );
    assert_eq!(stats2["live_bytes"]["total"], json!({"objects": 2, "bytes": 16}));
    assert_eq!(stats2["live_bytes"]["cells"]["hseg/seg"], json!({"objects": 1, "bytes": 12}));
    assert_eq!(stats2["cells"]["hseg/seg/put"], json!({"2xx": 1, "412": 1}));
    let basic = json_body(request(&state, Method::GET, "/_s3lite/stats", "").await).await;
    assert_eq!(basic["put_bytes"], json!(50));
}

/// L6 spike (review B4): a PUT body pays len / cap seconds on top of the
/// latency; uncapped it pays nothing.
#[tokio::test]
async fn put_bandwidth_cap_delays_a_body_by_its_length() {
    let body = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
    let state = state(Duration::ZERO);
    state.set_put_bytes_per_sec(1000);
    let t0 = std::time::Instant::now();
    let put = request(&state, Method::PUT, "/b/p/shards/00/wal/1", body).await;
    assert_eq!(put.status(), StatusCode::OK);
    assert!(t0.elapsed() >= Duration::from_millis(100), "{:?}", t0.elapsed());
    state.set_put_bytes_per_sec(0);
    let t1 = std::time::Instant::now();
    let put = request(&state, Method::PUT, "/b/p/shards/00/wal/2", body).await;
    assert_eq!(put.status(), StatusCode::OK);
    assert!(t1.elapsed() < Duration::from_millis(100), "{:?}", t1.elapsed());
}
