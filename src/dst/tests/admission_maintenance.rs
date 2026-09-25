//! Admission maintenance.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{HttpRigOptions, http_rig, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::{eventually, mem, open_engine, skey};
use crate::dst::{FaultPlan, FaultStore, Outcome, Workload};
use crate::shard::{encode_shard_maint, shard_maint_key};
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

const BACKLOG_LIMIT: u64 = 1024;

/// The HTTP routes use the real configured maintenance bound. Absorption is
/// paused only while arranging real acknowledged records, never by replacing
/// the ledger that the committer and absorber must reconcile.
async fn backlog_rig(
    mut options: HttpRigOptions,
) -> (Arc<crate::http::AppState>, std::net::SocketAddr) {
    let mut admission = crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
    .admission;
    admission.unabsorbed_bytes_shard = BACKLOG_LIMIT;
    options.admission = Some(admission);
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    rig.state
        .runtime
        .history
        .paused
        .store(true, std::sync::atomic::Ordering::Relaxed);
    rig.parts()
}

async fn append_raw_backlog(addr: std::net::SocketAddr, name: &str) {
    let body = format!("[{{\"padding\":\"{}\"}}]", "x".repeat(2048));
    let (status, _, body) = hreq(
        addr,
        "POST",
        &format!("/v1/stream/{name}"),
        &[("content-type", "application/json")],
        body.as_bytes(),
    )
    .await;
    assert!(
        status == 200 || status == 204,
        "backlog append failed: {status} {body:?}"
    );
}

async fn drain_backlog(state: &crate::http::AppState, engine: &crate::shard::ShardEngine) {
    state
        .runtime
        .history
        .paused
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while engine.maintenance_snapshot().unabsorbed_frame_bytes > BACKLOG_LIMIT * 75 / 100 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "real absorption did not release backlog: {:?}",
            engine.maintenance_snapshot()
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// R26-5a: a RAW append to a hierarchical wildcard name receives the
/// typed refusal — 503, code `maintenance_backpressure`, retry-after —
/// while reads stay admitted, and recovery readmits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_hierarchical_append_sheds_typed_503_under_backlog() {
    let (state, addr) = backlog_rig(HttpRigOptions::default()).await;
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
    append_raw_backlog(addr, name).await;
    assert!(engine.maintenance_snapshot().unabsorbed_frame_bytes > BACKLOG_LIMIT);
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

    drain_backlog(&state, &engine).await;
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
struct AdmissionChild {
    key: &'static str,
    engine: Arc<crate::shard::ShardEngine>,
}
struct SplitAdmission {
    state: Arc<crate::http::AppState>,
    addr: std::net::SocketAddr,
    first: AdmissionChild,
    second: AdmissionChild,
}
async fn split_admission() -> SplitAdmission {
    let (state, addr) = backlog_rig(HttpRigOptions {
        prefixes: vec!["00".into(), "01".into(), "02".into(), "03".into()],
        ..Default::default()
    })
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

    SplitAdmission {
        state,
        addr,
        first: AdmissionChild {
            key: ka,
            engine: e0,
        },
        second: AdmissionChild {
            key: kb,
            engine: e1,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_child_sheds_while_sibling_child_admits() {
    let _l = gap_lock().lock().await;
    let SplitAdmission {
        state,
        addr,
        first: AdmissionChild {
            key: ka,
            engine: e0,
        },
        second: AdmissionChild {
            key: kb,
            engine: e1,
        },
    } = split_admission().await;
    let body = format!("{{\"k\":\"{ka}\",\"padding\":\"{}\"}}", "x".repeat(2048));
    let (st, _, response_body) = preq(
        addr,
        "POST",
        "/v1/streams/shed-split/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", ka),
        ],
        body.as_bytes(),
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "backlog append failed: {st} {response_body:?}"
    );
    assert!(e0.maintenance_snapshot().unabsorbed_frame_bytes > BACKLOG_LIMIT);
    assert!(e1.maintenance_snapshot().unabsorbed_frame_bytes < BACKLOG_LIMIT);
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

    drain_backlog(&state, &e0).await;
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
    assert!(
        st == 200 || st == 204,
        "drained child must readmit, got {st}: {}; maintenance={:?}",
        String::from_utf8_lossy(&body),
        e0.maintenance_snapshot()
    );
}

/// R26-5c: ownership replay OUTRANKS a locally latched engine. When the
/// ring reassigns the shard, a request must get 409 + Streams-Replay-To
/// pointing at the owner — never a stale 503 about a backlog that now
/// belongs to someone else. (R25-C placed admission after engine_for
/// precisely for this; here is the route-level proof.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_replay_wins_over_a_latched_local_engine() {
    let (state, addr) = backlog_rig(HttpRigOptions {
        instance: Some("inst-a".into()),
        ..Default::default()
    })
    .await;
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
    append_raw_backlog(addr, name).await;
    assert!(engine.maintenance_snapshot().unabsorbed_frame_bytes > BACKLOG_LIMIT);
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

/// Rig 1 of R26-5d: after restore-x's first append settles (which rewrites the
/// maintenance row), persist an over-bound row and hand the shard over.
async fn hand_over_an_over_bound_row(store: Arc<dyn ObjectStore>) -> String {
    let ct = [("content-type", "application/json")];
    let (state1, addr1) = http_rig(store).await;
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
    let settled = || engine1.maintenance_snapshot().unabsorbed_frame_bytes == 0;
    eventually("the append's settlement", settled).await;
    let fat = crate::shard::ShardMaintenance {
        version: 99,
        unabsorbed_frame_bytes: 300 * 1024 * 1024,
        backlog_started_ms: 1,
        last_progress_ms: 1,
    };
    let mut wb = slatedb::WriteBatch::new();
    wb.put(shard_maint_key(), encode_shard_maint(&fat));
    engine1.db.write(wb).await.unwrap();
    engine1.db.flush().await.unwrap();
    state1.shards.retire(
        &prefix,
        crate::shard_directory::RetirementReason::Shutdown,
        |_, _| true,
    );
    state1.shards.clear_holdoff(&prefix); // the fixture reopens at once
    eventually("engine 1 closing", || engine1.is_closed()).await;
    prefix
}

/// R26-5d: the first request CANNOT pass while backlog restoration is
/// parked, and then sees the restored ledger, not a default. Rig 1 persists
/// an over-bound durable row; rig 2 reopens the namespace with restoration
/// parked: the request stays unanswered, then gets the restored state's 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_request_waits_for_restoration_then_sees_the_restored_ledger() {
    let store = mem();
    let ct = [("content-type", "application/json")];
    let prefix = hand_over_an_over_bound_row(store.clone()).await;

    // Rig 2: fresh gate over the same store, restoration parked.
    let park = Arc::new(tokio::sync::Mutex::new(()));
    let held = park.clone().lock_owned().await;
    let (state2, addr2) = http_rig_park(store, park.clone(), RigRuntime::incarnation(1)).await;
    #[expect(
        clippy::disallowed_methods,
        reason = "Restoration fixture owns its blocked HTTP request; the handle is retained and joined after releasing the opener; synchronous execution cannot verify that the request stays pending"
    )]
    let req = tokio::spawn(async move {
        hreq(addr2, "POST", "/v1/stream/restore-x", &ct, br#"[{"n":2}]"#).await
    });
    let shards = &state2.shards;
    let opening = || shards.describe_for_test(&prefix).contains("inflight=true");
    eventually("the request's shard open", opening).await;
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
    let (state, addr) = backlog_rig(HttpRigOptions::default()).await;
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
    append_raw_backlog(addr, name).await;
    assert!(engine.maintenance_snapshot().unabsorbed_frame_bytes > BACKLOG_LIMIT);
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

/// `n` one-field JSON records: the smallest batch the raw surface parses.
fn records_body(n: usize) -> Vec<u8> {
    let items: Vec<String> = (0..n).map(|i| format!("{{\"n\":{i}}}")).collect();
    format!("[{}]", items.join(",")).into_bytes()
}

/// R26-7: /v1/debug/load carries what a campaign needs to attribute a
/// plateau — the exact cumulative frame-byte totals and the ordinary
/// limiter's refusals BY CODE — and the per-stream limiter's refusal
/// actually increments its own counter, distinct from maintenance shed.
/// The limiter is tripped by a DRAINED bucket under a manual clock: a
/// request that fits a fresh bucket but not the current one is the
/// transient refusal the limiter exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_load_reports_typed_limiter_and_frame_totals() {
    let usage = Arc::new(crate::usage::UsageService::new(
        &crate::config::AdmissionConfig::default(),
        Arc::new(crate::runtime::ManualClock::at(0)),
    ));
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                shared_usage: Some(usage),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    // One panicked connection reported on THIS runtime's task record.
    rig.tasks.record_connection_panic();
    let (_state, addr) = rig.parts();
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/load-t", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/load-t", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204);

    let load = |body: &[u8]| serde_json::from_slice::<serde_json::Value>(body).unwrap();
    let (st, _, body) = hreq(addr, "GET", "/v1/debug/load", &[], b"").await;
    assert_eq!(st, 200);
    let before = load(&body);
    assert_eq!(
        before["tasks"]["connection_panics"], 1,
        "item 37: the runtime's own panicked connections reach /v1/debug/load: {before}"
    );
    let m = &before["maintenance_shards"];
    assert!(
        m["ingest_frame_bytes_total"].as_u64().unwrap() >= 1,
        "cumulative committed frame bytes must be exported"
    );
    assert!(m["absorbed_frame_bytes_total"].is_u64());
    let rl_before = before["rate_limit_refusals"]["limit_records_per_sec"]
        .as_u64()
        .expect("per-code refusal counters must be exported");

    // Two bursts of 6,000 records against a 10,000-record bucket that
    // never refills: the first is admitted (9,999 -> 3,999 left after the
    // one-record probe), the second is refused TRANSIENTLY — it would fit
    // a fresh bucket — by the ordinary limiter, under its own code and
    // counter, never the maintenance gate.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/load-t", &ct, &records_body(6_000)).await;
    assert!(st == 200 || st == 204);
    let (st, headers, body) =
        hreq(addr, "POST", "/v1/stream/load-t", &ct, &records_body(6_000)).await;
    assert_eq!(st, 429, "a drained record bucket must 429");
    assert!(
        headers.contains_key("retry-after"),
        "a transient refusal names its wait: {headers:?}"
    );
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

/// A content append with more records than the record bucket can EVER hold
/// is a permanent 413 with no Retry-After: a 429 would name a wait no wait
/// can honour (review item 25). The refusal leaves nothing behind: the
/// stream stays writable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_capacity_record_count_is_a_permanent_413_not_a_429() {
    let (_state, addr) = http_rig(mem()).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/cap-recs", &ct, b"").await;
    assert!(st == 200 || st == 201);
    // 10,001 records: one more than LIMIT_RECS_PER_SEC x LIMIT_BURST_SECS.
    let (st, headers, body) = hreq(
        addr,
        "POST",
        "/v1/stream/cap-recs",
        &ct,
        &records_body(10_001),
    )
    .await;
    let body = String::from_utf8_lossy(&body).to_string();
    assert_eq!(
        st, 413,
        "10,001 records never fit a 10,000-record bucket: {body}"
    );
    assert!(body.contains("payload_too_large"), "{body}");
    assert!(
        body.contains("of 10001 records exceeds the per-stream ingest capacity of 10000 records"),
        "the refusal names its limit: {body}"
    );
    assert_eq!(
        headers.get("retry-after"),
        None,
        "a permanent refusal names no wait"
    );
    let (st, _, body) = hreq(addr, "POST", "/v1/stream/cap-recs", &ct, &records_body(1)).await;
    assert!(
        st == 200 || st == 204,
        "{st}: {}",
        String::from_utf8_lossy(&body)
    );
}

/// The product batch surface reaches the same owner: with a record bucket
/// smaller than MAX_BATCH_RECORDS, a batch larger than a fresh bucket is a
/// permanent 413 (one spelling on both surfaces, `payload_too_large`, with
/// its limit in `details`), and exactly the capacity is still admitted
/// afterwards because the refusal consumed nothing (the rig's usage clock
/// never refills between the two).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn product_batch_over_record_capacity_is_413_without_retry_after() {
    let usage = Arc::new(crate::usage::UsageService::new(
        &crate::config::AdmissionConfig {
            limit_recs_per_sec: 50.0, // x LIMIT_BURST_SECS 2 = 100 records
            ..Default::default()
        },
        Arc::new(crate::runtime::ManualClock::at(0)),
    ));
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                shared_usage: Some(usage),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        rig.addr,
        "PUT",
        "/v1/streams/cap-batch",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, headers, body) = preq(
        rig.addr,
        "POST",
        "/v1/streams/cap-batch/records:batch",
        &key,
        &records_body(101),
    )
    .await;
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(st, 413, "101 records never fit a 100-record bucket: {body}");
    let (code, d) = (body["error"]["code"].as_str(), &body["error"]["details"]);
    assert_eq!(
        (
            code,
            d["dimension"].as_str(),
            d["capacity"].as_u64(),
            d["requested"].as_u64()
        ),
        (
            Some("payload_too_large"),
            Some("records"),
            Some(100),
            Some(101)
        ),
        "{body}"
    );
    assert_eq!(headers.get("retry-after"), None);
    let (st, _, body) = preq(
        rig.addr,
        "POST",
        "/v1/streams/cap-batch/records:batch",
        &key,
        &records_body(100),
    )
    .await;
    assert_eq!(
        st,
        200,
        "exactly the capacity fits a fresh bucket: {}",
        String::from_utf8_lossy(&body)
    );
}

/// A deferred producer verdict outranks the capacity check: the shard
/// still answers the duplicate/invalid producer request as it always did,
/// so an over-capacity body with a deferred verdict is that verdict (400
/// invalid_body for a missing content type), never a 413.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_producer_verdict_outranks_the_capacity_refusal() {
    let usage = Arc::new(crate::usage::UsageService::new(
        &crate::config::AdmissionConfig {
            limit_bytes_per_sec: 50.0, // x LIMIT_BURST_SECS 2 = 100 bytes
            ..Default::default()
        },
        Arc::new(crate::runtime::ManualClock::at(0)),
    ));
    let (_state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                shared_usage: Some(usage),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .parts();
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/cap-deferred", &ct, b"").await;
    assert!(st == 200 || st == 201);
    // Producer headers, NO content type, a 101-byte body over the 100-byte cap.
    let producer = [
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/stream/cap-deferred",
        &producer,
        &[b'x'; 101],
    )
    .await;
    let body = String::from_utf8_lossy(&body).to_string();
    assert_eq!(st, 400, "the deferred verdict answers first: {body}");
    assert!(body.contains("invalid_body"), "{body}");
}

/// External review §5 on the product surface: the handler refuses a body no
/// fresh bucket admits before the key is checked (413, not 403), except a
/// producer request, whose duplicate is recognized before any later
/// validation refusal (Stage 4 §5): the core decides it, and its deferred
/// verdict (a record over the ceiling) outranks the stored-bytes 413.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_product_capacity_413_precedes_the_key_but_not_a_producer_verdict() {
    let usage = Arc::new(crate::usage::UsageService::new(
        &crate::config::AdmissionConfig {
            limit_bytes_per_sec: 50.0, // x LIMIT_BURST_SECS 2 = 100 bytes
            ..Default::default()
        },
        Arc::new(crate::runtime::ManualClock::at(0)),
    ));
    let shard = crate::shard::ShardConfig {
        shared_usage: Some(usage),
        ..Default::default()
    };
    let options = HttpRigOptions {
        shard,
        ..Default::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let key = ("prisma-encryption-key", PRISMA_KEY);
    let json = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(
        preq(rig.addr, "PUT", "/v1/streams/cap-order", &[key], json)
            .await
            .0,
        201
    );
    // 1 + 102 stored bytes: over the 100-byte bucket.
    let body = format!("[1,\"{}\"]", "x".repeat(100));
    let path = "/v1/streams/cap-order/records:batch";
    let wrong = (
        "prisma-encryption-key",
        "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=",
    );
    let (st, _, b) = preq(rig.addr, "POST", path, &[wrong], body.as_bytes()).await;
    let b = String::from_utf8_lossy(&b);
    assert_eq!(st, 413, "the handler's 413 comes before the key: {b}");
    rig.state.admission.set_record_ceiling(50);
    let producer = [
        key,
        ("producer-id", "p"),
        ("producer-epoch", "0"),
        ("producer-seq", "0"),
    ];
    let (st, _, b) = preq(rig.addr, "POST", path, &producer, body.as_bytes()).await;
    let b = String::from_utf8_lossy(&b);
    assert_eq!(
        st, 400,
        "a producer's deferred verdict outranks the 413: {b}"
    );
}
