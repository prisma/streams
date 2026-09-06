//! Admission maintenance.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{
    HttpRigOptions, http_rig, http_rig_build, http_rig_named, http_rig_opts,
};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{mem, open_engine, skey};
use crate::dst::{FaultPlan, FaultStore, Outcome, Workload};
use object_store::ObjectStore;
use std::sync::Arc;

/// A rig whose shard OPENER parks on the given lock right before
/// maintenance restoration (R26-5). Its only use is a RESTART phase
/// over a store another rig wrote, so the incarnation is explicit.
async fn http_rig_park(
    store: Arc<dyn ObjectStore>,
    park: Arc<tokio::sync::Mutex<()>>,
    runtime: RigRuntime,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    http_rig_build(
        store,
        runtime,
        HttpRigOptions {
            open_park: Some(park),
            ..Default::default()
        },
    )
    .await
    .parts()
}

/// R25-D: per-shard isolation at the admission mechanism — one engine
/// over its bound sheds; a sibling under it admits. This is the
/// split-children property at the level where it is decided: admit()
/// consults only the resolved engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overloaded_engine_sheds_while_sibling_admits() {
    let store = mem();
    let hot = open_engine(store.clone(), "dst-iso-hot").await;
    let calm = open_engine(store.clone(), "dst-iso-calm").await;
    let key = skey();
    let cov = FaultStore::uniform(mem(), 1, FaultPlan::new(0, 0, 0)).coverage();
    let w = Workload::new(cov);
    let out = w
        .attempt_with_deadline(&hot, [24u8; 16], &key, "k", &"h".repeat(8192), None, None)
        .await;
    assert!(matches!(out, Outcome::Acked { .. }));

    let limits = crate::backpressure::Limits {
        unabsorbed_bytes_shard: 1024, // hot is over; calm (0) is under
        absorb_lag_secs: 100,
        release_pct: 75,
        ..Default::default()
    };
    // R27-1: admit() composes TWO independent machines — the caller's
    // global latch and the engine's shard latch. The latch is a per-
    // instance value now, so this test contaminates nothing.
    let global = crate::backpressure::GlobalLatch::new();
    assert_eq!(
        crate::backpressure::admit(&hot, &global, &limits),
        Some(crate::backpressure::Cause::ShardBytes),
        "the over-bound engine must shed"
    );
    assert_eq!(
        crate::backpressure::admit(&calm, &global, &limits),
        None,
        "a sibling engine under its own bound must admit"
    );

    // R27-1 regression: engage the GLOBAL latch (lag) while hot is
    // ALSO over its shard bound — the simultaneous-cause shape that
    // used to store ShardBytes in the single global slot, which the
    // read side filtered to None, silently disabling the lag bound.
    // Now: the offending engine sheds with the GLOBAL cause reported,
    // and the unrelated calm engine sheds too.
    global.apply(
        &crate::backpressure::Snapshot {
            unabsorbed_bytes_instance: 0,
            absorb_lag_secs: 101,
        },
        &limits,
    );
    assert_eq!(
        crate::backpressure::admit(&hot, &global, &limits),
        Some(crate::backpressure::Cause::LagSecs),
        "global cause must not be masked on the offending engine"
    );
    assert_eq!(
        crate::backpressure::admit(&calm, &global, &limits),
        Some(crate::backpressure::Cause::LagSecs),
        "an unrelated local shard must shed under a global lag violation"
    );
    // Global clears; the shard machine keeps its own verdict — and the
    // shard machine cannot pin the global one.
    assert!(!global.apply(&crate::backpressure::Snapshot::default(), &limits));
    assert_eq!(
        crate::backpressure::admit(&hot, &global, &limits),
        Some(crate::backpressure::Cause::ShardBytes),
        "shard latch persists independently of the global release"
    );
    assert_eq!(crate::backpressure::admit(&calm, &global, &limits), None);

    // Hysteresis on the engine latch: once hot drains below release, it
    // readmits.
    hot.publish_maintenance(crate::shard::ShardMaintenance::default());
    assert_eq!(crate::backpressure::admit(&hot, &global, &limits), None);
    assert!(
        !hot.maintenance_shard_shed
            .load(std::sync::atomic::Ordering::Relaxed),
        "latch must clear when the backlog drains below release"
    );
    hot.begin_close();
    calm.begin_close();
}

/// R25-D: the stall clock. Continuous durable progress keeps the
/// no-progress age at bay; zero progress trips the lag bound.
#[test]
fn progress_clock_trips_only_without_progress() {
    use crate::backpressure::{Limits, Snapshot, next_state};
    let l = Limits {
        absorb_lag_secs: 60,
        release_pct: 75,
        ..Default::default()
    };
    // Progress every tick: apply_delta refreshes last_progress_ms, so
    // the derived stall stays near zero and never trips.
    let mut m = crate::shard::ShardMaintenance::default();
    let mut now = 0i64;
    for _ in 0..100 {
        now += 10_000;
        m = m.apply_delta(1_000, 500, now).unwrap();
        let stall = m.no_progress_secs(now + 5_000);
        assert!(stall <= 5, "stall {stall}s despite continuous progress");
        let snap = Snapshot {
            absorb_lag_secs: stall,
            ..Default::default()
        };
        assert_eq!(next_state(false, &snap, &l), (false, None));
    }
    // No progress: the stall grows past the bound and trips.
    let stall = m.no_progress_secs(now + 120_000);
    assert!(stall > 60);
    let snap = Snapshot {
        absorb_lag_secs: stall,
        ..Default::default()
    };
    assert!(matches!(
        next_state(false, &snap, &l),
        (true, Some(crate::backpressure::Cause::LagSecs))
    ));
}

/// R25-D: reserved system streams are never shed — the classifier the
/// append-core skip consults. Overload recovery must not deadlock on
/// its own system-of-record writes.
#[test]
fn reserved_system_streams_are_recognized() {
    for name in ["_usage", "_ops_events", "_ops_metrics"] {
        assert!(
            crate::billing::is_reserved_stream(name),
            "{name} must be reserved (and therefore never shed)"
        );
    }
    assert!(!crate::billing::is_reserved_stream("customers/acme"));
}

// ---- R26-5: the maintenance gate through the PRODUCTION surfaces ----
// The R25-D coverage proved the local primitive (backpressure::admit on
// two engines); these five prove the ROUTES — raw wildcard names,
// product split children, ownership replay, open-time restoration, and
// the reserved-stream skip — through real HTTP against real engines.

/// Inflate an engine's published ledger far over the default per-shard
/// bound (256 MiB) so its latch engages on the next admission check.
/// Per-engine state: no other test's engine is affected.
fn inflate_ledger(engine: &crate::shard::ShardEngine) {
    engine.publish_maintenance(crate::shard::ShardMaintenance {
        version: 1,
        unabsorbed_frame_bytes: 300 * 1024 * 1024,
        backlog_started_ms: 1,
        last_progress_ms: 1,
    });
}

/// R26-5a: a RAW append to a hierarchical wildcard name receives the
/// typed refusal — 503, code `maintenance_backpressure`, retry-after —
/// while reads stay admitted, and recovery readmits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_hierarchical_append_sheds_typed_503_under_backlog() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let name = "acme/prod/orders";
    let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &ct,
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "baseline append through the wildcard route, got {st}"
    );

    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(name))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let engine = state
        .engine_for_scaler(&seg.shard_route)
        .await
        .expect("engine");
    inflate_ledger(&engine);
    let (st, hdrs, body) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &ct,
        br#"[{"n":2}]"#,
    )
    .await;
    assert_eq!(st, 503, "over-bound shard must shed the raw append");
    let body = String::from_utf8_lossy(&body).to_string();
    assert!(
        body.contains("maintenance_backpressure"),
        "typed code required, got: {body}"
    );
    assert_eq!(
        hdrs.get("retry-after").map(String::as_str),
        Some("5"),
        "shed must carry a retry hint"
    );
    // Reads stay admitted — shedding a consumer would stop the drain.
    let (st, _, _) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(st, 200, "reads must not shed");

    engine.publish_maintenance(crate::shard::ShardMaintenance::default());
    let (st, _, _) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &ct,
        br#"[{"n":3}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "drained shard must readmit, got {st}"
    );
}

/// R26-5b: after a product split, child A's engine over the bound sheds
/// ONLY child A's keys; the sibling child keeps accepting. The per-shard
/// latch is the multitenant failure boundary — through the product
/// append route, not admit() called by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_child_sheds_while_sibling_child_admits() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig_opts(
        store,
        vec!["00".into(), "01".into(), "02".into(), "03".into()],
        crate::shard::ShardConfig::default(),
    )
    .await;
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/shed-split",
        &[("content-type", "application/json")],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201);
    let keys = ["ga", "gb", "gc", "gd", "ge", "gf", "gg", "gh"];
    for k in &keys {
        let body = format!("{{\"k\":\"{k}\"}}");
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/shed-split/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            body.as_bytes(),
        )
        .await;
        assert!(st == 200 || st == 204);
    }
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("shed-split"),
            0,
            0x8000_0000_0000_0000
        )
        .await,
        "split executes"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("shed-split"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("shed-split"))
        .await
        .unwrap()
        .unwrap();
    let map = desc.segments.as_ref().expect("map");
    let live: Vec<_> = map.segments.iter().filter(|s| s.is_live()).collect();
    assert_eq!(live.len(), 2);
    let r0 = desc.segment_route(live[0]);
    let r1 = desc.segment_route(live[1]);
    let e0 = state.engine_for_scaler(&r0).await.expect("engine 0");
    let e1 = state.engine_for_scaler(&r1).await.expect("engine 1");
    assert!(!Arc::ptr_eq(&e0, &e1), "children on distinct engines");
    // One key per child.
    let key_for = |route: [u8; 16]| {
        keys.iter()
            .find(|k| {
                let seg = desc.resolve_segment(k);
                desc.segment_route_by_id(seg.seg_id).unwrap() == route
            })
            .copied()
            .expect("a key routing to this child")
    };
    let (ka, kb) = (key_for(r0), key_for(r1));

    inflate_ledger(&e0);
    let (st, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/shed-split/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", ka),
        ],
        format!("{{\"k\":\"{ka}\"}}").as_bytes(),
    )
    .await;
    assert_eq!(st, 503, "child A must shed");
    assert!(
        String::from_utf8_lossy(&body).contains("maintenance_backpressure"),
        "the product surface must keep the typed code"
    );
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/shed-split/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", kb),
        ],
        format!("{{\"k\":\"{kb}\"}}").as_bytes(),
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "sibling child must keep admitting, got {st}"
    );

    e0.publish_maintenance(crate::shard::ShardMaintenance::default());
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/shed-split/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", ka),
        ],
        format!("{{\"k\":\"{ka}\"}}").as_bytes(),
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "drained child must readmit, got {st}"
    );
}

/// R26-5c: ownership replay OUTRANKS a locally latched engine. When the
/// ring reassigns the shard, a request must get 409 + Streams-Replay-To
/// pointing at the owner — never a stale 503 about a backlog that now
/// belongs to someone else. (R25-C placed admission after engine_for
/// precisely for this; here is the route-level proof.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_replay_wins_over_a_latched_local_engine() {
    let store = mem();
    let (state, addr) = http_rig_named(store, "inst-a").await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/replay-x", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/replay-x", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);

    // Latch the local engine.
    let name = "replay-x";
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(name))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let engine = state
        .engine_for_scaler(&seg.shard_route)
        .await
        .expect("engine");
    inflate_ledger(&engine);
    let (st, _, body) = hreq(addr, "POST", "/v1/stream/replay-x", &ct, br#"[{"n":2}]"#).await;
    assert_eq!(st, 503);
    assert!(String::from_utf8_lossy(&body).contains("maintenance_backpressure"));

    // The ring moves the shard to inst-b. The SAME request must now be
    // redirected — the resident latched engine yields, it does not
    // answer with its own backlog.
    state.ownership.set_ring_active(vec!["inst-b".to_string()]);
    let (st, hdrs, body) = hreq(addr, "POST", "/v1/stream/replay-x", &ct, br#"[{"n":3}]"#).await;
    assert_eq!(st, 409, "non-owner must redirect, got {st}");
    let body = String::from_utf8_lossy(&body).to_string();
    assert!(body.contains("not_ring_owner"), "got: {body}");
    assert_eq!(
        hdrs.get("streams-replay-to").map(String::as_str),
        Some("inst-b"),
        "replay target must name the owner"
    );
    assert!(
        !body.contains("maintenance_backpressure"),
        "a stale local latch must never answer for a shard we do not own"
    );
}

/// R26-5d: the first request CANNOT pass while backlog restoration is
/// parked — and when restoration completes, admission sees the restored
/// ledger, not a default. Rig 1 persists an over-bound durable row;
/// rig 2 opens the same namespace with restoration parked: the request
/// stays unanswered while parked, then gets the typed 503 from the
/// restored state. At no point does an append slip through against an
/// unknown backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_request_waits_for_restoration_then_sees_the_restored_ledger() {
    let store = mem();
    let ct = [("content-type", "application/json")];

    // Rig 1: create the stream, then persist a fat durable row through
    // the engine's own DB and hand the namespace over.
    let (state1, addr1) = http_rig(store.clone()).await;
    let (st, _, _) = hreq(addr1, "PUT", "/v1/stream/restore-x", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr1, "POST", "/v1/stream/restore-x", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);
    let desc = state1
        .registry
        .get(&state1.deployment.raw_adapter_sref("restore-x"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let prefix = state1.shards.prefix_for(&seg.shard_route);
    let engine1 = state1
        .engine_for_scaler(&seg.shard_route)
        .await
        .expect("engine");
    let fat = crate::shard::ShardMaintenance {
        version: 99,
        unabsorbed_frame_bytes: 300 * 1024 * 1024,
        backlog_started_ms: 1,
        last_progress_ms: 1,
    };
    let mut wb = slatedb::WriteBatch::new();
    wb.put(
        crate::shard::shard_maint_key(),
        crate::shard::encode_shard_maint(&fat),
    );
    engine1
        .db
        .write_with_options(wb, &slatedb::config::WriteOptions::default())
        .await
        .unwrap();
    engine1.db.flush().await.unwrap();
    state1.shards.retire(
        &prefix,
        crate::shard_directory::RetirementReason::Shutdown,
        |_, _| true,
    );
    state1.shards.clear_holdoff(&prefix); // the fixture reopens at once
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Rig 2: fresh gate over the same store, restoration parked.
    let park = Arc::new(tokio::sync::Mutex::new(()));
    let held = park.clone().lock_owned().await;
    let (_state2, addr2) = http_rig_park(store, park.clone(), RigRuntime::incarnation(1)).await;
    let req = tokio::spawn(async move {
        hreq(addr2, "POST", "/v1/stream/restore-x", &ct, br#"[{"n":2}]"#).await
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(
        !req.is_finished(),
        "a request must not be answered while the durable backlog is unrestored"
    );
    drop(held);
    let (st, _, body) = req.await.unwrap();
    assert_eq!(st, 503, "restored over-bound ledger must shed, got {st}");
    assert!(
        String::from_utf8_lossy(&body).contains("maintenance_backpressure"),
        "admission must have used the RESTORED ledger"
    );
}

/// R26-5e: reserved system streams stay writable THROUGH THE APPEND
/// PATH while the same engine sheds customers. Overload recovery cannot
/// deadlock on its own system-of-record writes — proved via the
/// fleet-internal telemetry-append route, which funnels into the same
/// append_core as everything else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserved_streams_append_through_a_latched_engine() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/cust-r", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/cust-r", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);

    let name = "cust-r";
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(name))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let engine = state
        .engine_for_scaler(&seg.shard_route)
        .await
        .expect("engine");
    inflate_ledger(&engine);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/cust-r", &ct, br#"[{"n":2}]"#).await;
    assert_eq!(st, 503, "customer stream on the latched engine must shed");

    // The system stream lands on the SAME latched engine (single
    // prefix) and must still be admitted.
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_usage",
        &[
            ("content-type", "application/json"),
            ("authorization", "Bearer dst-internal-token"),
        ],
        br#"[{"ev":"probe"}]"#,
    )
    .await;
    let body = String::from_utf8_lossy(&body).to_string();
    assert!(
        !body.contains("maintenance_backpressure"),
        "reserved stream shed by the maintenance gate: {body}"
    );
    assert!(
        st == 200 || st == 204,
        "system-of-record write must pass the latch, got {st}: {body}"
    );
}

/// R26-7: /v1/debug/load carries what a campaign needs to attribute a
/// plateau — the exact cumulative frame-byte totals and the ordinary
/// limiter's refusals BY CODE — and the per-stream limiter's refusal
/// actually increments its own counter, distinct from maintenance shed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_load_reports_typed_limiter_and_frame_totals() {
    let store = mem();
    let (_state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/load-t", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/load-t", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);

    let load = |body: &[u8]| serde_json::from_slice::<serde_json::Value>(body).unwrap();
    let (st, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    assert_eq!(st, 200);
    let before = load(&body);
    let m = &before["maintenance_shards"];
    assert!(
        m["ingest_frame_bytes_total"].as_u64().unwrap() >= 1,
        "cumulative committed frame bytes must be exported"
    );
    assert!(m["absorbed_frame_bytes_total"].is_u64());
    let rl_before = before["rate_limit_refusals"]["limit_records_per_sec"]
        .as_u64()
        .expect("per-code refusal counters must be exported");

    // One request over the record-bucket CAPACITY (5,000/s x 2 s burst)
    // trips the ordinary limiter — the refusal must carry its own code
    // and count under its own counter, never the maintenance one.
    let over: Vec<serde_json::Value> = (0..10_001).map(|n| serde_json::json!({ "n": n })).collect();
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/stream/load-t",
        &ct,
        serde_json::to_vec(&over).unwrap().as_slice(),
    )
    .await;
    assert_eq!(st, 429, "over-capacity record burst must 429");
    let refusal = String::from_utf8_lossy(&body).to_string();
    assert!(
        refusal.contains("limit_records_per_sec"),
        "the limiter must name itself: {refusal}"
    );
    assert!(
        !refusal.contains("maintenance_backpressure"),
        "a limiter refusal must not masquerade as maintenance shed"
    );
    let (_, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    let after = load(&body);
    assert!(
        after["rate_limit_refusals"]["limit_records_per_sec"]
            .as_u64()
            .unwrap()
            > rl_before,
        "the refusal must count under its own code"
    );
    // (appends_shed equality is deliberately NOT asserted: the counter
    // is process-global and the R26-5 shed gates run in this same
    // parallel suite; `before` pins only the rl_before baseline.)
    let _ = &before;
}
