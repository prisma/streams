//! Runtime-local denial/ops journals, alerts and HTTP body ceilings.
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use std::sync::Arc;
use std::time::Duration;

async fn journal(state: &Arc<crate::http::AppState>, stream: &str) -> Vec<serde_json::Value> {
    let key = state.billing.usage_key().unwrap();
    let (body, _) = crate::billing::system_read(state, stream, &key, None)
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_runtime_journals_and_alerts_do_not_cross_drain_or_resolve() {
    let mut states = Vec::new();
    for incarnation in [1, 2] {
        let auth = Arc::new(
            crate::auth::AuthService::new(
                crate::auth::AuthMode::Enforce,
                "https://auth.prisma.io".into(),
                "test-cell",
            )
            .unwrap(),
        );
        let rig = http_rig_build(
            mem(),
            RigRuntime::incarnation(incarnation),
            HttpRigOptions {
                auth_service: Some(auth),
                ..Default::default()
            },
        )
        .await;
        states.push(rig.state);
    }
    for (index, state) in states.iter().enumerate() {
        state
            .runtime
            .ops
            .emit(crate::ops::OpsEvent::new("owned", format!("owner-{index}")));
        let refusal = crate::audit::tag(
            axum::response::IntoResponse::into_response(axum::http::StatusCode::UNAUTHORIZED),
            "unauthorized",
        );
        crate::audit::observe_denial(
            state,
            &format!("route-{index}"),
            &axum::http::Method::GET,
            &refusal,
        );
    }
    // B drains first while A has queued events. Each ledger still receives
    // exactly its own event, not whatever arrived first process-wide.
    for index in [1, 0] {
        let state = &states[index];
        assert_eq!(crate::ops::drain_ops_once(state).await.unwrap(), 1);
        assert_eq!(crate::audit::drain_audit_once(state).await.unwrap(), 1);
        let ops = journal(state, crate::billing::OPS_EVENTS_STREAM).await;
        let audit = journal(state, crate::billing::AUDIT_EVENTS_STREAM).await;
        assert_eq!(ops.len(), 1);
        assert_eq!(audit.len(), 1);
        assert_eq!(ops[0]["event_id"], format!("owner-{index}"));
        assert_eq!(audit[0]["route"], format!("route-{index}"));
        assert!(
            audit[0]["event_id"]
                .as_str()
                .unwrap()
                .contains(&state.runtime.identity.boot_id)
        );
        assert!(audit[0]["project_id"].is_null());
        assert_eq!(state.runtime.ops.recent(10).len(), 1);
    }
    let a = &states[0];
    let b = &states[1];
    let mut breached = crate::ops::collect_snapshot(a);
    breached
        .counters
        .insert("ops_events_dropped_total".into(), 1);
    crate::ops::evaluate_alerts(a, &breached).await;
    assert!(
        a.runtime
            .ops
            .open_alerts()
            .iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    );
    assert!(b.runtime.ops.open_alerts().is_empty());
    crate::ops::evaluate_alerts(b, &crate::ops::collect_snapshot(b)).await;
    assert!(
        a.runtime
            .ops
            .open_alerts()
            .iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    );
    assert_eq!(
        b.runtime.ops.recent(10).len(),
        1,
        "B cannot emit A's resolution"
    );
    crate::ops::evaluate_alerts(a, &crate::ops::collect_snapshot(a)).await;
    assert!(
        !a.runtime
            .ops
            .open_alerts()
            .iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    );
    engine_shutdown(a).await;
    engine_shutdown(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_runtime_body_limits_apply_to_each_collector_and_preflight() {
    let mut rigs = Vec::new();
    for (incarnation, limit) in [(1, 64 * 1024), (2, 128 * 1024)] {
        rigs.push(
            http_rig_build(
                mem(),
                RigRuntime::incarnation(incarnation),
                HttpRigOptions {
                    max_request_body_bytes: Some(limit),
                    ..Default::default()
                },
            )
            .await,
        );
    }
    let payload = bytes::Bytes::from(vec![b'x'; 96 * 1024]);
    for (index, rig) in rigs.iter().enumerate() {
        let raw = [
            ("stream-encryption-key", PRISMA_KEY),
            ("content-type", "application/octet-stream"),
        ];
        let product = [
            ("prisma-encryption-key", PRISMA_KEY),
            ("content-type", "application/octet-stream"),
        ];
        assert_eq!(
            preq(rig.addr, "PUT", "/v1/stream/raw", &raw, b"").await.0,
            201
        );
        assert_eq!(
            preq(
                rig.addr,
                "PUT",
                "/v1/streams/product",
                &product,
                br#"{"format":{"kind":"bytes"}}"#
            )
            .await
            .0,
            201
        );
        let raw_status = preq(rig.addr, "POST", "/v1/stream/raw", &raw, &payload)
            .await
            .0;
        let product_status = preq(
            rig.addr,
            "POST",
            "/v1/streams/product/records",
            &product,
            &payload,
        )
        .await
        .0;
        let create_status = preq(rig.addr, "PUT", "/v1/stream/initial", &raw, &payload)
            .await
            .0;
        if index == 0 {
            assert_eq!((raw_status, product_status, create_status), (413, 413, 413));
        } else {
            assert!(matches!(raw_status, 200 | 204));
            assert_eq!((product_status, create_status), (200, 201));
        }
        // No Content-Length: exercise the incremental raw collector separately
        // from the declared-length rejection above.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("stream-encryption-key", PRISMA_KEY.parse().unwrap());
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        let chunks = [
            Ok::<_, std::io::Error>(payload.slice(..48 * 1024)),
            Ok(payload.slice(48 * 1024..)),
        ];
        let result = crate::http::append_typed(
            rig.state.clone(),
            rig.state.deployment.raw_adapter_sref("raw"),
            headers,
            axum::body::Body::from_stream(futures_util::stream::iter(chunks)),
            None,
            None,
            None,
        )
        .await;
        if index == 0 {
            assert_eq!(
                result.unwrap_err().code,
                crate::application::append::AppendCode::TooLarge
            );
        } else {
            assert!(result.is_ok());
        }
    }
    for rig in rigs {
        engine_shutdown(&rig.state).await;
    }
}

/// S2: the operator surface serves the ops snapshot the last telemetry
/// tick collected, cached and not re-collected per request (re-collecting
/// would swap the snapshot's drained peaks to 0 under the ledger), beside
/// the feed ages, the shed breakdown, the maintenance latch and the
/// overflow admits the cell watch alerts on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_operator_data_serves_the_cached_ops_snapshot_without_recollecting() {
    let auth = Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            auth: Some("operator-bearer".into()),
            auth_service: Some(auth),
            ..Default::default()
        },
    )
    .await;
    crate::ops::emit_metrics_once(&rig.state).await.unwrap();
    let bearer = [("authorization", "Bearer operator-bearer")];
    let mut bodies = Vec::new();
    for _ in 0..2 {
        let (status, _, body) = hreq(rig.addr, "GET", "/operator/data.json", &bearer, b"").await;
        assert_eq!(status, 200);
        bodies.push(serde_json::from_slice::<serde_json::Value>(&body).unwrap());
    }
    let body = &bodies[0];
    assert!(body["ops"].is_object(), "operator ops: {}", body["ops"]);
    assert_eq!(
        bodies[0]["ops"], bodies[1]["ops"],
        "re-collected per request"
    );
    assert!(body["ops"]["gauges"]["rss_peak_since_scrape_mb"].is_u64());
    assert!(body["feeds"]["policies"]["reservedDropped"].is_u64());
    let local = &body["local"];
    assert!(local["admit_shed_rss"].is_u64());
    assert!(local["maintenance_backpressure"]["appends_shed"].is_u64());
    assert!(local["sse_max_connections"].is_u64());
    assert!(local["usage_overflow_admits"].is_u64());
    assert_eq!(
        local["sealed_index_corrupt_rows"], 0,
        "no seal-index row skipped (cell-watch row 30)"
    );
    engine_shutdown(&rig.state).await;
}

/// A metrics batch reaches `_ops_metrics` as exactly the points it carries,
/// in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r10_a_metrics_batch_reaches_the_ops_metrics_journal_in_order() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let mut points = vec![crate::ops::collect_snapshot(&rig.state); 2];
    points[0].ts_ms = 1;
    points[1].ts_ms = 2;
    crate::ops::append_metrics(&rig.state, &points)
        .await
        .unwrap();
    let key = rig.state.billing.usage_key().unwrap();
    let read =
        crate::billing::system_read(&rig.state, crate::billing::OPS_METRICS_STREAM, &key, None);
    let stored = read.await.unwrap().map(|(body, _)| body.to_vec());
    let sent = serde_json::to_vec(&points).unwrap();
    assert_eq!(
        stored.map(String::from_utf8),
        Some(String::from_utf8(sent)),
        "the journal holds the batch's points, in order"
    );
    engine_shutdown(&rig.state).await;
}

/// The points `_ops_metrics` holds, oldest first.
async fn ops_metrics_points(state: &Arc<crate::http::AppState>) -> Vec<crate::ops::OpsSnapshot> {
    let key = state.billing.usage_key().unwrap();
    let read = crate::billing::system_read(state, crate::billing::OPS_METRICS_STREAM, &key, None);
    match read.await.unwrap() {
        Some((body, _)) if !body.is_empty() => serde_json::from_slice(&body).unwrap(),
        _ => Vec::new(),
    }
}

/// Idles the rig for `secs` seconds of the paused tokio clock, moving its
/// manual clock along a second at a time: the drain loop ticks on tokio's
/// timer and measures its cadences on the runtime clock, so both must pass.
async fn idle_seconds(rig: &HttpRig, secs: u64) {
    for _ in 0..secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        rig.clock.advance(Duration::from_secs(1));
    }
}

/// The store's PUTs so far: (WAL objects, every class).
fn puts(store: &FaultStore) -> (u64, u64) {
    let classes = [ObjClass::Manifest, ObjClass::Sst, ObjClass::Other];
    let wal = store.count(StoreOp::Put, ObjClass::Wal);
    let rest: u64 = classes.map(|c| store.count(StoreOp::Put, c)).iter().sum();
    (wal, wal + rest)
}

/// Stops a rig that ran on the paused clock. The engines' close completes
/// on work that clock does not drive, so the teardown's deadlines run on
/// the real clock.
async fn stop_paused_rig(rig: HttpRig) {
    tokio::time::resume();
    rig.shutdown().await;
}

/// The idle cost of the ops snapshot (the cost report of 2026-10-24, lever
/// 5): on an otherwise idle cell each `_ops_metrics` point is an append,
/// which on this rig costs about 8 PUTs (2 of them WAL objects, the rest
/// SSTs, manifests and other objects). At the defaults (a point every 60 s, checked by the
/// 8 s drain round) an idle server stores a point at 0 s, 64 s, ... 576 s:
/// exactly 10 in ten minutes, not the 38 of a 15 s cadence (one every
/// 16 s). Between 10 s and 600 s this rig measured 22 WAL PUTs and 116
/// PUTs in all at 60 s, against 78 and 340 at 15 s.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_idle_server_stores_one_ops_metrics_point_a_minute() {
    let store = FaultStore::uniform(mem(), 1601, FaultPlan::CLEAN);
    let data: Arc<dyn object_store::ObjectStore> = store.clone();
    let rig = http_rig_build(data, RigRuntime::first(), HttpRigOptions::default()).await;
    crate::billing::spawn_telemetry(rig.state.clone(), &rig.tasks);
    idle_seconds(&rig, 10).await; // past the first round's writes
    let (wal_before, all_before) = puts(&store);
    idle_seconds(&rig, 590).await;
    let points = ops_metrics_points(&rig.state).await.len();
    assert_eq!(
        points, 10,
        "ten idle minutes store one point a minute (0 s, 64 s, ... 576 s), not one every 16 s"
    );
    let (wal, all) = puts(&store);
    let idle = (wal - wal_before, all - all_before);
    assert!(
        idle.0 <= 30 && idle.1 <= 160,
        "the idle window's (WAL, all) PUTs: {idle:?}"
    );
    stop_paused_rig(rig).await;
}

/// Alert latency is the 15 s cadence's, not the minute's: between points
/// the alert rules run every 16 s (at the default 8 s drain cadence) on a
/// snapshot of their own, cached for `/operator/data.json` and not stored.
/// An ops event dropped at the queue cap after the 16 s round opens
/// `ops_event_drops` at the 32 s round, not at the 24 s round (which runs
/// no rules) nor at the next point (64 s), while `_ops_metrics` still holds
/// the first point alone; the 64 s point then carries the drop.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn alerts_run_every_16_s_between_minute_points() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    crate::billing::spawn_telemetry(state.clone(), &rig.tasks);
    let drops = |s: &crate::ops::OpsSnapshot| s.counters["ops_events_dropped_total"];
    let alerted = || {
        let open = state.runtime.ops.open_alerts();
        open.iter()
            .any(|alert| alert.fingerprint == "ops_event_drops")
    };
    idle_seconds(&rig, 20).await; // rounds at 0 s (a point), 8 s, 16 s (the rules)
    let mut flood = 0;
    while state.runtime.ops.dropped() == 0 {
        let event = crate::ops::OpsEvent::new("flood", format!("flood-{flood}"));
        state.runtime.ops.emit(event);
        flood += 1;
    }
    idle_seconds(&rig, 5).await; // the 24 s round drains events and runs no rules
    assert!(!alerted(), "the 24 s round runs no alert rules");
    idle_seconds(&rig, 8).await;
    assert!(alerted(), "the drop opens its alert at the 32 s round");
    let cached = state.runtime.ops.last_snapshot().unwrap();
    assert_eq!(drops(&cached), 1, "the rules' snapshot is the one cached");
    assert!(
        cached.gauges.contains_key("rss_peak_since_scrape_mb"),
        "the drained RSS peak is collected"
    );
    let stored =
        |points: Vec<crate::ops::OpsSnapshot>| points.iter().map(drops).collect::<Vec<_>>();
    assert_eq!(
        stored(ops_metrics_points(&state).await),
        [0],
        "and not stored"
    );
    idle_seconds(&rig, 32).await; // the 64 s round takes the second point
    assert_eq!(stored(ops_metrics_points(&state).await), [0, 1]);
    stop_paused_rig(rig).await;
}

/// `carry_peaks` raises the gauge a collection drains (the RSS peak since
/// the last scrape) to the larger of the two snapshots' (adding it when the
/// later snapshot lacks it) and leaves every other gauge as the later
/// snapshot collected it.
#[test]
fn carry_peaks_raises_only_the_drained_peak_gauges() {
    let snapshot = |gauges: &[(&str, u64)]| crate::ops::OpsSnapshot {
        v: 1,
        ts_ms: 0,
        cell: String::new(),
        region: String::new(),
        instance: String::new(),
        role: "server".into(),
        counters: std::collections::BTreeMap::new(),
        gauges: gauges.iter().map(|(k, v)| ((*k).to_string(), *v)).collect(),
    };
    let peak = "rss_peak_since_scrape_mb";
    let earlier = snapshot(&[(peak, 900), ("rss_mb", 900)]);
    let mut later = snapshot(&[(peak, 300), ("rss_mb", 300)]);
    later.carry_peaks(&earlier);
    let raised = snapshot(&[(peak, 900), ("rss_mb", 300)]);
    assert_eq!(later.gauges, raised.gauges);
    let mut bare = snapshot(&[]);
    bare.carry_peaks(&earlier);
    assert_eq!(bare.gauges, snapshot(&[(peak, 900)]).gauges);
}
