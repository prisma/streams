#![cfg(test)]
//! A runtime that stops while a topology operation's start is in flight (GATE-DECISION-meas-full-
//! forward-wedge-2026-11-19, Addendum 22; the `topology_colocation` shutdown flake). The loop that
//! starts operations (`topology::ops::run`) awaits a start's opens and its writes without watching
//! its cancellation, so a stop waits the supervisor's grace and aborts it. One rig over the leaves
//! `0` and `1` merges `""` with a store operation of the start held, on each side of the start's
//! first durable write (the home CAS): the open of a half, before it, and the intent's PUT, after
//! it. The stop must end every task within its grace, the request held beside it must be answered,
//! and a fresh boot over the same store must find no live intent, the stored topology as it was
//! (a reserved home aside), and both halves serving every record.

use super::billing_closure_debts::{JSON, raw_put, stored};
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::topology_policy::store_topology;
use crate::dst::fault_store::{FaultProfile, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use object_store::ObjectStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// How long a step (an open, a hold engaging, a held request's answer) may take.
const BOUND: Duration = Duration::from_secs(20);

/// The stored topology: its version, its leaves at their homes and its next home.
type Stored = (u64, Vec<(String, u32)>, u32);

/// A rig named `inst-a` over the leaves `0` and `1` on `data`.
async fn rig(data: Arc<dyn ObjectStore>, runtime: RigRuntime) -> HttpRig {
    let options = HttpRigOptions {
        instance: Some("inst-a".into()),
        prefixes: vec!["0".into(), "1".into()],
        ..Default::default()
    };
    http_rig_build(data, runtime, options).await
}

/// `rig`'s topology loop, under a supervisor of the test's own, with room for seven shards.
fn ops(rig: &HttpRig) -> crate::tasks::TaskSupervisor {
    let tasks = crate::tasks::TaskSupervisor::new();
    let parts = crate::topology::ops::Parts {
        dir: rig.state.shards.clone(),
        fleet: rig.state.fleet.clone(),
        ownership: rig.state.ownership.clone(),
        move_cooldown_ms: 600_000,
        clock: rig.state.runtime.clock.clone(),
        owner: "inst-a".to_string(),
        shards_max: 7,
    };
    crate::topology::ops::start(parts, &tasks);
    tasks
}

async fn stop(rig: HttpRig) {
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// A stream created at `rig` whose route lands in `leaf`: its path.
async fn stream_in(rig: &HttpRig, leaf: &str) -> String {
    for n in 0..200 {
        let name = format!("cancel-{leaf}-{n}");
        raw_put(rig.addr, &name, &[JSON], b"").await;
        let desc = stored(&rig.state, &rig.state.deployment.raw_adapter_sref(&name)).await;
        if rig
            .state
            .shards
            .prefix_for(&desc.resolve_segment("").shard_route)
            == leaf
        {
            return format!("/v1/stream/{name}");
        }
    }
    panic!("no stream lands in {leaf}");
}

/// Record `n` appended to `path` at `addr`: the answer's status.
async fn append(addr: std::net::SocketAddr, path: &str, n: u32) -> u16 {
    let body = format!(r#"[{{"n":{n}}}]"#);
    hreq(addr, "POST", path, &[JSON], body.as_bytes()).await.0
}

/// The records of `path`, read at `addr`.
async fn records(addr: std::net::SocketAddr, path: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", path, &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

async fn stored_topology(data: &Arc<dyn ObjectStore>) -> Stored {
    let (topology, _) = crate::topology::store::read(&**data).await.unwrap();
    let leaves = topology.shards.leaves.into_iter().collect();
    (topology.version, leaves, topology.shards.next_home)
}

/// The cell as boot leaves it, with one stream in each leaf holding record 1; its rig stopped.
async fn seeded(data: &Arc<dyn ObjectStore>) -> [String; 2] {
    let first = rig(data.clone(), RigRuntime::first()).await;
    store_topology(&first).await;
    let paths = [stream_in(&first, "0").await, stream_in(&first, "1").await];
    for path in &paths {
        assert_eq!(append(first.addr, path, 1).await, 204, "{path}");
    }
    stop(first).await;
    paths
}

/// The records `{"n": n}` of each of `ns`, as a read answers them.
fn holding(ns: &[u32]) -> serde_json::Value {
    let each: Vec<String> = ns.iter().map(|n| format!(r#"{{"n":{n}}}"#)).collect();
    serde_json::from_str(&format!("[{}]", each.join(","))).unwrap()
}

/// Wait, bounded, until a hold has parked an operation.
async fn engaged(counter: &AtomicU64) {
    let t0 = tokio::time::Instant::now();
    while counter.load(Ordering::SeqCst) == 0 {
        assert!(t0.elapsed() < BOUND, "the hold engages");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// What a fresh boot over `data` finds: whether a live intent holds the cell, the stored topology,
/// and each stream's records once record 3 is appended to it.
async fn after_a_fresh_boot(
    data: &Arc<dyn ObjectStore>,
    paths: &[String; 2],
) -> (bool, Stored, Vec<serde_json::Value>) {
    let fresh = rig(data.clone(), RigRuntime::incarnation(2)).await;
    let intent = crate::topology::intent::read(&**data).await.unwrap();
    let live = intent.is_some_and(|held| !held.intent.is_terminal());
    let topology = stored_topology(data).await;
    let mut read = Vec::new();
    for path in paths {
        assert_eq!(append(fresh.addr, path, 3).await, 204, "{path}");
        read.push(records(fresh.addr, path).await);
    }
    stop(fresh).await;
    (live, topology, read)
}

/// The merge of `""` on a fresh boot over `data`, run to its end: the stored topology it published,
/// and each stream's records read from the merged leaf.
async fn merged_on_a_fresh_boot(
    data: &Arc<dyn ObjectStore>,
    paths: &[String; 2],
) -> (Stored, Vec<serde_json::Value>) {
    let fresh = rig(data.clone(), RigRuntime::incarnation(3)).await;
    let tasks = ops(&fresh);
    let started = fresh.state.shards.topology_ops().merge(String::new()).await;
    assert!(started.is_ok(), "{started:?}");
    let t0 = tokio::time::Instant::now();
    loop {
        let intent = crate::topology::intent::read(&**data).await.unwrap();
        if intent.is_some_and(|held| held.intent.is_terminal()) {
            break;
        }
        assert!(t0.elapsed() < BOUND, "the merge ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let topology = stored_topology(data).await;
    let mut read = Vec::new();
    for path in paths {
        read.push(records(fresh.addr, path).await);
    }
    let report = tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    stop(fresh).await;
    (topology, read)
}

/// The merge of `""` started on `cell`'s topology loop with `hold` engaged at its start, a request
/// to `parked` sent beside it, and the loop stopped with a grace of 3 s: the tasks the stop
/// aborted, and the held request's status once the hold is released.
async fn stopped_mid_start(
    cell: &HttpRig,
    fault: &FaultStore,
    hold: &AtomicU64,
    parked: &str,
) -> (Vec<String>, u16) {
    let tasks = ops(cell);
    let handle = cell.state.shards.topology_ops();
    let merge = tokio::time::timeout(2 * BOUND, handle.merge(String::new()));
    let stopped = async {
        engaged(hold).await;
        let request = tokio::time::timeout(2 * BOUND, append(cell.addr, parked, 2));
        let stop = async {
            let report = tasks.shutdown(Duration::from_secs(3)).await;
            fault.release_hold();
            report
        };
        futures_util::future::join(request, stop).await
    };
    let (ended, (answered, report)) = futures_util::future::join(merge, stopped).await;
    assert!(ended.is_ok(), "the merge's asker is answered");
    let status = answered.expect("the held request is answered");
    (
        report.aborted.iter().map(ToString::to_string).collect(),
        status,
    )
}

/// **A stop while a merge's start opens a half ends every task, and leaves the cell whole** (before
/// the first durable write). The open of `1`'s log is held at its manifest read, the merge's
/// resolve of `1` and an append to `1` both wait on it, and the loop is stopped: no task is aborted
/// (red: `topology-ops`, at the grace), the append is answered `204` once the open lands, and a
/// fresh boot finds no intent, the topology as stored, and every record of both streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_stop_while_a_merges_start_opens_a_half_ends_its_tasks_and_leaves_the_cell_whole() {
    let data = mem();
    let paths = seeded(&data).await;
    let before = stored_topology(&data).await;
    let fault = FaultStore::new(data.clone(), 0x22A, FaultProfile::clean());
    let hold = fault.hold_class_under(StoreOp::Get, ObjClass::Manifest, "shards/1", u64::MAX);
    let cell = rig(fault.clone(), RigRuntime::incarnation(1)).await;
    let (aborted, answered) = stopped_mid_start(&cell, &fault, &hold, &paths[1]).await;
    stop(cell).await;
    let after = after_a_fresh_boot(&data, &paths).await;
    assert_eq!(
        (aborted, answered, after),
        (
            vec![],
            204,
            (false, before, vec![holding(&[1, 3]), holding(&[1, 2, 3])])
        )
    );
}

/// **A stop while a merge's start writes its intent ends every task, and leaves the cell whole**
/// (after the first durable write). The home CAS lands and the intent's PUT is held, an append to
/// `0` is sent beside it, and the loop is stopped: no task is aborted (red: `topology-ops`), the
/// append is answered `204`, and a fresh boot finds no intent (the PUT was never sent), the
/// topology at its version and leaves with the one home reserved (`next_home` advanced, no leaf
/// routed to it), and every record of both streams. The next merge then takes the following home
/// and publishes: the merged leaf `""` at that home, and every record read from it (Addendum 22
/// follow-up (a)).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_stop_while_a_merges_start_writes_its_intent_ends_its_tasks_and_leaves_the_cell_whole() {
    let data = mem();
    let paths = seeded(&data).await;
    let (version, leaves, next_home) = stored_topology(&data).await;
    let fault = FaultStore::new(data.clone(), 0x22B, FaultProfile::clean());
    let hold = fault.hold_class_under(
        StoreOp::Put,
        ObjClass::Other,
        "topology/intent.json",
        u64::MAX,
    );
    let cell = rig(fault.clone(), RigRuntime::incarnation(1)).await;
    let (aborted, answered) = stopped_mid_start(&cell, &fault, &hold, &paths[0]).await;
    stop(cell).await;
    let after = after_a_fresh_boot(&data, &paths).await;
    let reserved = (version, leaves, next_home + 1);
    let both = vec![holding(&[1, 2, 3]), holding(&[1, 3])];
    let merged = merged_on_a_fresh_boot(&data, &paths).await;
    let following = (
        version + 1,
        vec![(String::new(), next_home + 1)],
        next_home + 2,
    );
    assert_eq!(
        (aborted, answered, after, merged),
        (
            vec![],
            204,
            (false, reserved, both.clone()),
            (following, both)
        )
    );
}
