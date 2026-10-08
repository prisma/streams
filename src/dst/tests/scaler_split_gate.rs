//! The split gate (the owner's decision of 2026-10-07, NEXT-WORK §14.6;
//! edge change #119): while the server runs in a fleet, that is once the
//! fleet loop has published a ring of any size, the scaler's controller
//! declines every split. A hot stream stays one segment on its owner and
//! meets its per-stream limit with a 429. A split would put the high child
//! on another shard, which is another server's now or once the ring grows,
//! where consumer pulls, producer lanes, watches and merges do not follow it
//! yet. A server with no fleet ring (fleet off) still splits.
//!
//! Each instance has two one-bit shards over the shared store. The stream's
//! shard is `inst-a`'s and the other is `inst-b`'s while `inst-b` is active,
//! so the split's high child would be `inst-b`'s.
use super::fixture_http::{HttpRig, HttpRigOptions, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::scaler3::controller::{Controller, Decision, PassReport};
use std::sync::Arc;
use std::time::Duration;

const NAME: &str = "gate-hot";
const PATH: &str = "/v1/stream/gate-hot";
const TEXT: [(&str, &str); 1] = [("content-type", "text/plain")];

/// The load-weighted median of [`feed_heat`]: the end of the segment's first
/// sixty-fourth, as `scaler_loop` establishes.
const SPLIT_AT: u64 = u64::MAX / 64;

/// The per-stream limit: ten bytes a second with one second of burst, on a
/// usage clock that never advances, so the bucket holds ten bytes in all.
fn admission() -> crate::config::AdmissionConfig {
    crate::config::AdmissionConfig {
        limit_bytes_per_sec: 10.0,
        limit_reqs_per_sec: 0.0,
        limit_recs_per_sec: 0.0,
        limit_burst_secs: 1.0,
        ..Default::default()
    }
}

/// One instance of the cell, named `name`, under its own process incarnation.
async fn instance(store: Arc<dyn object_store::ObjectStore>, name: &str, n: u64) -> HttpRig {
    let usage = Arc::new(crate::usage::UsageService::new(
        &admission(),
        Arc::new(crate::runtime::ManualClock::at(0)),
    ));
    let options = HttpRigOptions {
        instance: Some(name.to_owned()),
        prefixes: vec!["0".to_owned(), "1".to_owned()],
        admission: Some(admission()),
        shard: crate::shard::ShardConfig {
            shared_usage: Some(usage),
            ..Default::default()
        },
        ..Default::default()
    };
    http_rig_build(store, RigRuntime::incarnation(n), options).await
}

/// Publish the ring `members` on `rig`: the stream's shard is `inst-a`'s,
/// the other shard `inst-b`'s while `inst-b` is active.
fn ring(rig: &HttpRig, members: &[&str]) {
    let sref = rig.state.deployment.raw_adapter_sref(NAME);
    let route = crate::crypto::RouteHash::for_stream(&sref).0;
    let home = crate::registry::shard_for_hash(rig.state.shards.prefixes(), &route);
    let other = if home == "0" { "1" } else { "0" };
    let ownership = &rig.state.ownership;
    ownership.set_ring_active(members.iter().map(|m| (*m).to_owned()).collect());
    ownership.set_override(&home, "inst-a");
    ownership.set_override(other, "inst-b");
}

/// Heat the stream's one segment as the append path feeds it, far above the
/// limit: 600 GB under one routing key at the segment's first point and
/// 400 GB under another at three quarters, so it splits at [`SPLIT_AT`].
fn feed_heat(state: &crate::http::AppState, desc: &crate::registry::StreamDesc) {
    let parent = desc.resolve_segment("");
    for (point, key, bytes) in [
        (1, 1, 600_000_000_000),
        (u64::MAX / 4 * 3, 2, 400_000_000_000),
    ] {
        let route = crate::registry::SegRoute {
            point,
            key_hash: crate::crypto::RoutingKeyHash([key; 16]),
            ..parent.clone()
        };
        state.runtime.scaler.note_append(desc, &route, bytes, 1);
    }
}

/// Create the stream on `rig`, spend its bucket with one ten-byte append,
/// and heat it until the scaler's second hot evaluation decides the split.
async fn hot_stream(rig: &HttpRig) -> Decision {
    assert_eq!(hreq(rig.addr, "PUT", PATH, &TEXT, b"").await.0, 201);
    let appended = hreq(rig.addr, "POST", PATH, &TEXT, b"0123456789").await;
    assert_eq!(appended.0, 204, "the first ten bytes fit the bucket");
    let sref = rig.state.deployment.raw_adapter_sref(NAME);
    let desc = rig.state.registry.get(&sref).await.unwrap().unwrap();
    feed_heat(&rig.state, &desc);
    let scaler = &rig.state.runtime.scaler;
    assert_eq!(scaler.evaluate(), (vec![], vec![]), "one hot evaluation");
    let epoch = desc.stream_epoch.clone();
    assert_eq!(
        scaler.evaluate(),
        (vec![(sref.clone(), epoch.clone(), 0, SPLIT_AT)], vec![]),
        "the second hot evaluation decides the split"
    );
    Decision::Split(sref, epoch, 0, SPLIT_AT)
}

/// One controller pass over `work`, supervised as the scaler loop's pass is.
async fn one_pass(state: &crate::http::AppState, work: Decision) -> PassReport {
    let mut controller = Controller::new(state.topology_service(), state.runtime.ops.clone(), 0);
    controller.enqueue(work);
    let (send, receive) = tokio::sync::oneshot::channel();
    let tasks = crate::tasks::TaskSupervisor::new();
    tasks
        .spawn(
            "scaler-pass",
            crate::tasks::Policy::Critical,
            move |cancel| async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                send.send(controller.pass(&cancel, deadline).await).ok();
                crate::tasks::TaskResult::Done
            },
        )
        .unwrap();
    let report = receive.await.unwrap();
    let stopped = tasks.shutdown(Duration::from_millis(300)).await;
    assert!(stopped.aborted.is_empty(), "{stopped:?}");
    report
}

/// The stream's stored segment map, as (segment, live) pairs; `None` while
/// the stream is unsplit and stores no map.
async fn segments(state: &crate::http::AppState) -> Option<Vec<(u32, bool)>> {
    let sref = state.deployment.raw_adapter_sref(NAME);
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let map = desc.segments.as_ref()?;
    Some(
        map.segments
            .iter()
            .map(|s| (s.seg_id, s.is_live()))
            .collect(),
    )
}

/// The `split_committed` events `state`'s controller emitted.
fn splits_committed(state: &crate::http::AppState) -> usize {
    let recent = state.runtime.ops.recent(crate::ops::RECENT_CAP);
    recent
        .iter()
        .filter(|event| event.event_type == "split_committed")
        .count()
}

/// On a ring of two active servers the controller declines the scaler's
/// split: the stream keeps its one segment and no split is committed, and
/// once its bucket is spent its next append is refused 429 with the bytes
/// limit's code and nothing of it is stored. The other server holds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_stream_on_a_two_server_ring_never_splits_and_meets_its_limit() {
    let store = mem();
    let a = instance(store.clone(), "inst-a", 1).await;
    let b = instance(store, "inst-b", 2).await;
    ring(&a, &["inst-a", "inst-b"]);
    ring(&b, &["inst-a", "inst-b"]);
    let split = hot_stream(&a).await;
    let report = one_pass(&a.state, split).await;
    assert_eq!(
        (report.attempted, report.completed, report.deferred),
        (1, 0, 0),
        "the controller declines the split on a two-server ring, and keeps no debt"
    );
    assert_eq!(segments(&a.state).await, None, "the stream stays unsplit");
    assert_eq!(splits_committed(&a.state), 0);
    let (status, headers, body) = hreq(a.addr, "POST", PATH, &TEXT, b"x").await;
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        (status, headers.get("retry-after").map(String::as_str)),
        (429, Some("1"))
    );
    assert_eq!(body["error"]["code"], "limit_bytes_per_sec", "{body}");
    let (status, _, stored) = hreq(a.addr, "GET", &format!("{PATH}?offset=-1"), &[], b"").await;
    assert_eq!((status, stored.as_slice()), (200, &b"0123456789"[..]));
    assert_eq!(
        b.state.shards.open_count(),
        0,
        "inst-b serves nothing of it"
    );
    b.shutdown().await;
    a.shutdown().await;
}

/// The servers that own the stream's live segments under `state`'s ring:
/// the stream's own owner while it is unsplit, else one per live segment.
async fn live_owners(state: &crate::http::AppState) -> Vec<String> {
    let sref = state.deployment.raw_adapter_sref(NAME);
    state.registry.invalidate(&sref);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let routes: Vec<[u8; 16]> = match desc.segments.as_ref() {
        None => vec![desc.route_hash().0],
        Some(map) => map
            .segments
            .iter()
            .filter(|s| s.is_live())
            .map(|s| desc.segment_route(s))
            .collect(),
    };
    routes
        .iter()
        .map(|route| {
            let prefix = crate::registry::shard_for_hash(state.shards.prefixes(), route);
            state.ownership.effective_owner(&prefix).unwrap_or_default()
        })
        .collect()
}

/// On a ring of one active server (a fleet of one, as an autoscaled fleet
/// idles, a peer drains or goes dark) the controller declines the split
/// too: a child on the other shard would be another server's once the ring
/// grows. When `inst-b` joins, the whole stream is still `inst-a`'s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_stream_on_a_one_server_ring_never_splits_so_a_grown_ring_keeps_it_on_its_owner() {
    let store = mem();
    let a = instance(store.clone(), "inst-a", 1).await;
    let b = instance(store, "inst-b", 2).await;
    ring(&a, &["inst-a"]);
    ring(&b, &["inst-a"]);
    let split = hot_stream(&a).await;
    let report = one_pass(&a.state, split).await;
    ring(&a, &["inst-a", "inst-b"]);
    ring(&b, &["inst-a", "inst-b"]);
    assert_eq!(
        (
            (report.attempted, report.completed, report.deferred),
            live_owners(&a.state).await
        ),
        ((1, 0, 0), vec!["inst-a".to_owned()]),
        "the controller declines the split on a one-server ring and keeps no debt, so the grown ring leaves the whole stream on its owner"
    );
    assert_eq!(segments(&a.state).await, None, "the stream stays unsplit");
    assert_eq!(splits_committed(&a.state), 0);
    b.shutdown().await;
    a.shutdown().await;
}

/// A server with no fleet ring (fleet off, the launch shape) still splits
/// the hot stream at the scaler's median: the parent is sealed and both
/// children are live, all on the one server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hot_stream_on_a_server_without_a_fleet_ring_still_splits() {
    let a = instance(mem(), "inst-a", 1).await;
    let split = hot_stream(&a).await;
    let report = one_pass(&a.state, split).await;
    assert_eq!(
        (report.attempted, report.completed, report.deferred),
        (1, 1, 0)
    );
    assert_eq!(
        segments(&a.state).await,
        Some(vec![(0, false), (1, true), (2, true)])
    );
    assert_eq!(splits_committed(&a.state), 1);
    a.shutdown().await;
}
