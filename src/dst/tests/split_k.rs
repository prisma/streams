#![cfg(test)]
//! The split end to end on a serving rig (S4-DESIGN §6, K-SPLIT-DECISION-2026-11-09 §5): two
//! streams, one in each half of the leaf `00`, sealed into one segment object (one `'K'` row naming
//! both), then `00` split into `000` and `001`. Each child serves its stream back byte-exact
//! through the row it was copied (keeping only its range's entries), and the children's `'R'` rows
//! for the object sum to the parent's.

use super::billing_closure_debts::{JSON, engine_of, raw_put, stored};
use super::fixture_http::{engine_shutdown, http_rig_at};
use super::fixture_requests::hreq;
use super::fixture_storage::mem;
use crate::spill::object::ObjectId;
use std::collections::BTreeMap;
use std::sync::Arc;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;

/// A stream created in the half `half` of the leaf `00` (its route's third bit): its name.
async fn stream_in_half(state: &State, addr: Addr, half: char, taken: &mut usize) -> String {
    loop {
        *taken += 1;
        assert!(*taken < 400, "no stream lands in 00{half}");
        let name = format!("k-{taken}");
        raw_put(addr, &name, &[JSON], b"").await;
        let desc = stored(state, &state.deployment.raw_adapter_sref(&name)).await;
        let route = desc.resolve_segment("").shard_route;
        let third = (route[0] >> 5) & 1;
        if state.shards.prefix_for(&route) == "00" && third == u8::from(half == '1') {
            return name;
        }
    }
}

/// The records of `name`, read back.
async fn records(addr: Addr, name: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// Each object's `'R'` row: its live chunks and bytes.
async fn refcounts(engine: &crate::shard::ShardEngine) -> BTreeMap<ObjectId, (u64, u64)> {
    let rows = engine.refcount_rows().await.unwrap();
    rows.into_iter()
        .filter_map(|(id, r)| Some((id, r.map(|r| (r.live_chunks, r.live_bytes))?)))
        .collect()
}

/// A rig whose stored topology is its own, with a stream in each half of the leaf `00` holding two
/// records each, and the leaf's engine.
struct Cell {
    state: State,
    addr: Addr,
    low: String,
    high: String,
    parent: Arc<crate::shard::ShardEngine>,
}

/// One record of `n`, padded.
fn body(n: u32) -> String {
    format!(r#"[{{"n":{n},"pad":"{}"}}]"#, "x".repeat(64))
}

/// An append of `{"n": n}` to `name`: its status and the next offset it reports.
async fn append(addr: Addr, name: &str, n: u32) -> (u16, Option<String>) {
    let path = format!("/v1/stream/{name}");
    let (status, headers, _) = hreq(addr, "POST", &path, &[JSON], body(n).as_bytes()).await;
    (status, headers.get("stream-next-offset").cloned())
}

async fn cell() -> Cell {
    cell_over(mem()).await
}

/// `cell`, over `store`.
async fn cell_over(store: Arc<dyn object_store::ObjectStore>) -> Cell {
    cell_at(store, super::fixture_runtime::RigRuntime::first()).await
}

/// `cell_over`, on `runtime` (whose manual clock the test may drive).
async fn cell_at(
    store: Arc<dyn object_store::ObjectStore>,
    runtime: super::fixture_runtime::RigRuntime,
) -> Cell {
    let (state, addr) = http_rig_at(store.clone(), runtime).await;
    // The cell's stored topology, as boot leaves it: the rig's own.
    let topology = crate::topology::model::Topology {
        version: 1,
        shards: state.shards.prefixes().shards.clone(),
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    object_store::ObjectStoreExt::put(&*store, &"topology.json".into(), raw.into())
        .await
        .unwrap();
    let mut taken = 0;
    let low = stream_in_half(&state, addr, '0', &mut taken).await;
    let high = stream_in_half(&state, addr, '1', &mut taken).await;
    for (name, n) in [(&low, 1), (&high, 2), (&low, 3), (&high, 4)] {
        assert_eq!(append(addr, name, n).await.0, 204, "append to {name}");
    }
    let desc = stored(&state, &state.deployment.raw_adapter_sref(&low)).await;
    let parent = engine_of(&state, &desc, 0).await;
    assert_eq!(parent.prefix, "00");
    Cell {
        state,
        addr,
        low,
        high,
        parent,
    }
}

/// **A split's children each serve their stream back byte-exact, and their counts sum to the
/// parent's.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_serves_each_stream_from_its_child_and_the_counts_sum() {
    let Cell {
        state,
        addr,
        low,
        high,
        parent,
    } = cell().await;
    parent.seal_requested().await.expect("seal");
    let before = refcounts(&parent).await;
    let shared: Vec<ObjectId> = before
        .iter()
        .filter(|(_, (chunks, _))| *chunks == 2)
        .map(|(id, _)| *id)
        .collect();
    // One seal holds both streams' chunks in one object; the sealer may have sealed the first two
    // records on its own too, in an object of its own that both streams also share.
    assert!(
        !shared.is_empty(),
        "an object holds both streams' chunks: {before:?}"
    );
    let want = (records(addr, &low).await, records(addr, &high).await);

    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-k", "inst-a", &*clock)
            .await
            .expect("the split publishes");
    assert_eq!(published.version, 2);
    let prefixes: Vec<&str> = published
        .children
        .iter()
        .map(|(p, _, _)| p.as_str())
        .collect();
    assert_eq!(prefixes, ["000", "001"]);
    assert!(parent.is_closed(), "the parent is retired");
    let admin =
        slatedb::admin::AdminBuilder::new(parent.log_path(), parent.data_store().clone()).build();
    let left = admin.list_checkpoints(Some("topo-op-k")).await.unwrap();
    assert!(
        left.is_empty(),
        "the pause's checkpoint is deleted: {left:?}"
    );

    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    let mut summed: BTreeMap<ObjectId, (u64, u64)> = BTreeMap::new();
    for (_, _, child) in &published.children {
        for (id, (chunks, bytes)) in refcounts(child).await {
            let at = summed.entry(id).or_default();
            (at.0, at.1) = (at.0 + chunks, at.1 + bytes);
        }
    }
    for id in &shared {
        assert_eq!(
            summed.get(id),
            before.get(id),
            "the children sum to the parent: {id:?}"
        );
    }
    for (_, _, child) in &published.children {
        let counted = refcounts(child).await;
        let chunks: Vec<_> = shared
            .iter()
            .map(|id| counted.get(id).map(|c| c.0))
            .collect();
        assert_eq!(chunks, vec![Some(1); shared.len()], "{}", child.prefix);
    }
    engine_shutdown(&state).await;
}

/// **A split whose clone fails after its checkpoint rolls back, leaving nothing of its children.**
/// A manifest that does not decode already sits at the first child's path, so its clone fails inside
/// the pause while the other's may succeed: the split refuses `RolledBack`, its intent ends
/// `RolledBack`, the parent holds exactly the checkpoints it held before (no `topo-<op_id>`, no
/// clone's final checkpoint), neither child path holds a file (S4-DESIGN §6.4's takeover GC), and
/// the parent still serves both streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_that_rolls_back_leaves_nothing_of_its_children() {
    let store = mem();
    let Cell {
        state,
        addr,
        low,
        high,
        parent,
    } = cell_over(store.clone()).await;
    let want = (records(addr, &low).await, records(addr, &high).await);
    let admin =
        slatedb::admin::AdminBuilder::new(parent.log_path(), parent.data_store().clone()).build();
    let held = admin.list_checkpoints(None).await.unwrap();
    let home = state.shards.prefixes().shards.next_home;
    let path = crate::topology::model::born_home_path("000", home);
    let junk = format!("{path}/manifest/00000000000000000001.manifest");
    object_store::ObjectStoreExt::put(&*store, &junk.as_str().into(), b"junk".to_vec().into())
        .await
        .unwrap();

    let clock = state.runtime.clock.clone();
    let refused = crate::topology::split::split(&state.shards, &parent, "op-r", "inst-a", &*clock)
        .await
        .err()
        .expect("the split rolls back");
    assert!(
        matches!(&refused, crate::topology::split::Refused::RolledBack(why) if why.contains(&path)),
        "{refused}"
    );
    let ended = crate::topology::intent::read(&*store)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ended.intent.state,
        crate::topology::intent::State::RolledBack
    );
    let left = admin.list_checkpoints(Some("topo-op-r")).await.unwrap();
    assert!(
        left.is_empty(),
        "the rollback deletes the checkpoint: {left:?}"
    );
    let ids = |all: Vec<slatedb::Checkpoint>| all.into_iter().map(|c| c.id).collect::<Vec<_>>();
    assert_eq!(
        ids(admin.list_checkpoints(None).await.unwrap()),
        ids(held),
        "no clone's final checkpoint is left on the parent"
    );
    for (prefix, home) in [("000", home), ("001", home + 1)] {
        let path = crate::topology::model::born_home_path(prefix, home);
        let files = object_paths(&*store, &path).await;
        assert!(files.is_empty(), "{path} holds {files:?}");
    }
    assert!(!parent.is_closed(), "the parent serves on");
    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    engine_shutdown(&state).await;
}

/// Every object under `prefix` in `store`.
async fn object_paths(store: &dyn object_store::ObjectStore, prefix: &str) -> Vec<String> {
    use futures_util::TryStreamExt;
    let prefix = object_store::path::Path::from(prefix);
    let all: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
    all.into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// Every reference list in `store` (S6-DESIGN §2.3): its path and ETag.
async fn lists(store: &dyn object_store::ObjectStore) -> BTreeMap<String, Option<String>> {
    use futures_util::TryStreamExt;
    let all: Vec<_> = store.list(None).try_collect().await.unwrap();
    all.into_iter()
        .filter(|meta| meta.location.as_ref().contains("/lineage/refs/"))
        .map(|meta| (meta.location.to_string(), meta.e_tag))
        .collect()
}

/// **The pause PUTs no list when nothing adds a holding** (S4-DESIGN §7.3). The split pauses at its
/// park, with the lists Preparing PUT for the sealed object in place; released, it publishes, and
/// every list is the one Preparing PUT (its ETag unchanged), with no `-d` list beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_pause_puts_no_list_when_nothing_adds_a_holding() {
    let store = mem();
    let Cell { state, parent, .. } = cell_over(store.clone()).await;
    parent.seal_requested().await.expect("seal");
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    parent.gc_state().gate.pause_at_park(point);
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, "op-d", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        let prepared = lists(&*store).await;
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
        prepared
    };
    let (published, prepared) = tokio::join!(split, control);
    published.expect("the split publishes");
    assert_eq!(prepared.len(), 2, "one list per child: {prepared:?}");
    assert_eq!(lists(&*store).await, prepared);
    engine_shutdown(&state).await;
}

/// **An append the split held is acked by its child, at its exact offset.** The split pauses once
/// its barrier parked; an append to the lower stream then waits in the parent's queue. Released, the
/// split publishes and re-routes it: the append is answered 204 by the child, the stream's next
/// offset moves past it, and the stream reads its three records in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_append_the_split_held_is_acked_by_its_child_at_its_exact_offset() {
    let Cell {
        state,
        addr,
        low,
        parent,
        ..
    } = cell().await;
    let (before, _) = records_and_next(addr, &low).await;
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    parent.gc_state().gate.pause_at_park(point);
    let clock = state.runtime.clock.clone();
    let split = crate::topology::split::split(&state.shards, &parent, "op-held", "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        let release = async {
            until_queued(&parent).await;
            assert!(go_tx.send(()).is_ok(), "the split waits for go");
        };
        futures_util::future::join(append(addr, &low, 9), release)
            .await
            .0
    };
    let (published, (status, next)) = futures_util::future::join(split, control).await;
    let published = published.expect("the split publishes");
    assert_eq!(published.version, 2);
    assert_eq!(status, 204, "the held append is acked");
    let (after, read_next) = records_and_next(addr, &low).await;
    let mut want = before.as_array().cloned().unwrap_or_default();
    want.push(serde_json::from_str::<serde_json::Value>(&body(9)).unwrap()[0].clone());
    assert_eq!(
        after,
        serde_json::Value::Array(want),
        "the held record, last"
    );
    assert!(next.is_some(), "the ack reports its offset");
    assert_eq!(next, read_next, "acked at the offset the child serves next");
    engine_shutdown(&state).await;
}

/// The records of `name` and the next offset its read reports.
async fn records_and_next(addr: Addr, name: &str) -> (serde_json::Value, Option<String>) {
    let (status, headers, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    let records = serde_json::from_slice(&body).unwrap_or_default();
    (records, headers.get("stream-next-offset").cloned())
}

/// Wait until `engine`'s committer holds an operation in its queue, at most 5 s.
async fn until_queued(engine: &crate::shard::ShardEngine) {
    let t0 = std::time::Instant::now();
    while engine.queued_ops() == 0 {
        assert!(t0.elapsed() < std::time::Duration::from_secs(5), "held");
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}

/// The sentinel rows of `tag` in `engine`'s log, keys only.
async fn tagged(engine: &crate::shard::ShardEngine, tag: u8) -> Vec<Vec<u8>> {
    let prefix = crate::spill::runindex::sentinel_prefix(tag);
    let mut scan = engine.db.scan_prefix(&prefix[..], ..).await.unwrap();
    let mut keys = Vec::new();
    while let Some(kv) = scan.next().await.unwrap() {
        keys.push(kv.key.to_vec());
    }
    keys
}

/// **The Preparing fold leaves no parent `'K'` row to copy** (K-SPLIT-DECISION-2026-11-09 §5's second
/// case): with sealed `'K'` rows in the parent's log, the split's Preparing fold folds them into the
/// run index, so neither child's seed holds a `'K'` row and both hold the parent's `'I'` rows; both
/// streams read back as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_preparing_fold_leaves_the_children_no_parent_k_rows() {
    let Cell {
        state,
        addr,
        low,
        high,
        parent,
    } = cell().await;
    parent.seal_requested().await.expect("seal");
    use crate::keyspace::tag::{DIRECTORY, POINTER};
    assert!(
        !tagged(&parent, DIRECTORY).await.is_empty(),
        "sealed 'K' rows to fold"
    );
    let want = (records(addr, &low).await, records(addr, &high).await);
    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-fold", "inst-a", &*clock)
            .await
            .expect("the split publishes");
    let pointers = tagged(&published.children[0].2, POINTER).await;
    assert!(!pointers.is_empty(), "the fold's 'I' rows");
    for (prefix, _, child) in &published.children {
        assert!(
            tagged(child, DIRECTORY).await.is_empty(),
            "{prefix}: no 'K' row"
        );
        assert_eq!(
            tagged(child, POINTER).await,
            pointers,
            "{prefix}: the parent's 'I' rows"
        );
    }
    assert_eq!(
        (records(addr, &low).await, records(addr, &high).await),
        want
    );
    engine_shutdown(&state).await;
}

/// The split's pause, its barrier's park to its release to the children, with every store request
/// taking `ms`: printed in milliseconds and round trips, and held to the 2 s hold
/// (`shard::transaction::barrier::HOLD`). Timing-sensitive: its figures are quoted only from a run
/// under the machine's lock.
async fn pause_at(ms: u64) {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    const HOLD: std::time::Duration = std::time::Duration::from_secs(2);
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (ms, ms),
        ..FaultPlan::CLEAN
    };
    let store: Arc<dyn object_store::ObjectStore> = FaultStore::uniform(mem(), 0x5411, plan);
    let Cell { state, parent, .. } = cell_over(store).await;
    parent.seal_requested().await.expect("seal");
    let clock = state.runtime.clock.clone();
    let published =
        crate::topology::split::split(&state.shards, &parent, "op-pause", "inst-a", &*clock)
            .await
            .expect("the split publishes");
    let pause = published.pause;
    let rtt = pause.as_millis() / u128::from(ms);
    eprintln!(
        "PAUSE {} ms, about {rtt} round trips of {ms} ms (hold: {HOLD:?})",
        pause.as_millis()
    );
    assert!(pause < HOLD, "{pause:?}");
    engine_shutdown(&state).await;
}

/// **The pause on a store 10 ms away stays within the 2 s hold** (S4-DESIGN §9.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_pause_on_a_store_ten_ms_away_stays_within_the_hold() {
    pause_at(10).await;
}

/// **The pause on a store 25 ms away (Tigris' 20-30 ms) stays within the 2 s hold.** A measurement
/// for a run under the machine's lock (the lead, 2026-11-10); ignored in the suite, whose load would
/// stretch it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[ignore = "a timing measurement, run under the machine's lock"]
async fn the_pause_on_a_store_twenty_five_ms_away_stays_within_the_hold() {
    pause_at(25).await;
}

/// The split of `cell`'s parent as `op_id`, paused at its park while `at_park` runs.
async fn split_paused_while<F: std::future::Future<Output = ()>>(
    cell: &Cell,
    op_id: &str,
    at_park: F,
) -> Result<crate::topology::split::Published, crate::topology::split::Refused> {
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    cell.parent.gc_state().gate.pause_at_park(point);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, op_id, "inst-a", &*clock);
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        at_park.await;
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
    };
    let (split, ()) = tokio::join!(split, control);
    split
}

/// The intent once its ETag moved from `etag`, within 5 s.
async fn renewal_of(
    store: &dyn object_store::ObjectStore,
    etag: &str,
) -> crate::topology::intent::Held {
    let t0 = std::time::Instant::now();
    loop {
        let now = crate::topology::intent::read(store).await.unwrap().unwrap();
        if now.etag != etag {
            return now;
        }
        assert!(t0.elapsed() < std::time::Duration::from_secs(5), "renewed");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// **A split renews its intent while it runs** (S4-DESIGN §4.2). Paused at its park, the split's
/// clock advances past a renewal period (3.3 s): its intent is renewed (a new ETag, at the same
/// epoch and owner), and released, the split publishes and ends its intent `Reclaimed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_renews_its_intent_while_it_runs() {
    use crate::topology::intent;
    let store = mem();
    let runtime = super::fixture_runtime::RigRuntime::first();
    let clock = runtime.clock.clone();
    let cell = cell_at(store.clone(), runtime).await;
    let at_park = async {
        let before = intent::read(&*store).await.unwrap().unwrap();
        clock.advance(std::time::Duration::from_secs(4));
        let now = renewal_of(&*store, &before.etag).await;
        assert_eq!(now.intent.lease_epoch, before.intent.lease_epoch);
        assert_eq!(now.intent.lease_owner, before.intent.lease_owner);
        assert!(now.intent.lease_until_ms > before.intent.lease_until_ms);
    };
    split_paused_while(&cell, "op-renew", at_park)
        .await
        .expect("the split publishes");
    let ended = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(ended.state, intent::State::Reclaimed);
    engine_shutdown(&cell.state).await;
}

/// **A split whose lease a claimant took rolls back before `'T'`** (S4-DESIGN §4.3). Paused at its
/// park, its intent is claimed (`lease_epoch` 2, another owner) and its clock passes the lease
/// (11 s): released, the split refuses `RolledBack` naming the lost lease, its parent holds no
/// `'T'` and still serves both streams, neither child path holds a file, and the intent stays the
/// claimant's (the actor's terminal write loses).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_whose_lease_a_claimant_took_rolls_back_before_t() {
    use crate::topology::intent;
    let store = mem();
    let runtime = super::fixture_runtime::RigRuntime::first();
    let clock = runtime.clock.clone();
    let cell = cell_at(store.clone(), runtime).await;
    let want = (
        records(cell.addr, &cell.low).await,
        records(cell.addr, &cell.high).await,
    );
    let home = cell.state.shards.prefixes().shards.next_home;
    let at_park = async {
        let held = intent::read(&*store).await.unwrap().unwrap();
        let mut claimed = held.intent.clone();
        claimed.lease_owner = "opener:claimant".to_string();
        claimed.lease_epoch += 1;
        intent::replace(&*store, &held, claimed).await.unwrap();
        clock.advance(std::time::Duration::from_secs(12));
    };
    let refused = split_paused_while(&cell, "op-lost", at_park)
        .await
        .err()
        .expect("the split rolls back");
    assert!(
        matches!(&refused, crate::topology::split::Refused::RolledBack(why) if why.contains("lease")),
        "{refused}"
    );
    let retired = cell
        .parent
        .db
        .get(crate::topology::fence::retired_key())
        .await
        .unwrap();
    assert!(retired.is_none(), "no 'T'");
    for (prefix, home) in [("000", home), ("001", home + 1)] {
        let path = crate::topology::model::born_home_path(prefix, home);
        let files = object_paths(&*store, &path).await;
        assert!(files.is_empty(), "{path} holds {files:?}");
    }
    let left = intent::read(&*store).await.unwrap().unwrap().intent;
    assert_eq!(
        (left.state, left.lease_epoch),
        (intent::State::Preparing, 2)
    );
    assert!(!cell.parent.is_closed(), "the parent serves on");
    assert_eq!(
        (
            records(cell.addr, &cell.low).await,
            records(cell.addr, &cell.high).await
        ),
        want
    );
    engine_shutdown(&cell.state).await;
}

/// `cell`'s topology loop (`topology::ops`), started under a supervisor of its own, its instance
/// holding at most `shards_max` shards.
fn start_ops(cell: &Cell, shards_max: u32) -> crate::tasks::TaskSupervisor {
    start_ops_on(&cell.state, shards_max)
}

/// `start_ops`, on `state`'s rig; the fleet's move cooldown is 600 s.
fn start_ops_on(state: &State, shards_max: u32) -> crate::tasks::TaskSupervisor {
    let tasks = crate::tasks::TaskSupervisor::new();
    let parts = crate::topology::ops::Parts {
        dir: state.shards.clone(),
        ownership: state.ownership.clone(),
        fleet: state.fleet.clone(),
        move_cooldown_ms: 600_000,
        clock: state.runtime.clock.clone(),
        owner: "inst-a".to_string(),
        shards_max,
    };
    crate::topology::ops::start(parts, &tasks);
    tasks
}

/// A `POST` of `body` to the split route: its status and JSON body.
async fn post_split(addr: Addr, body: &str) -> (u16, serde_json::Value) {
    let route = "/operator/topology/split";
    let (status, _, body) = hreq(addr, "POST", route, &[JSON], body.as_bytes()).await;
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

/// **The split route starts a split that runs on to its publish** (S4-DESIGN §11, edge #251). The
/// route answers 202 with the operation and the version it splits from at once; the split then
/// publishes version 2 (`last_op` the operation), its intent ends `Reclaimed`, and each child
/// serves its stream back byte-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_split_route_starts_a_split_that_runs_on_to_its_publish() {
    let cell = cell().await;
    let want = (
        records(cell.addr, &cell.low).await,
        records(cell.addr, &cell.high).await,
    );
    let tasks = start_ops(&cell, 7);
    let (status, started) = post_split(cell.addr, r#"{"prefix":"00"}"#).await;
    assert_eq!(status, 202, "{started}");
    assert_eq!(started["version"], 1);
    let op_id = started["op_id"].as_str().unwrap().to_string();
    let store = cell.state.data_store.clone();
    let t0 = std::time::Instant::now();
    let ended = loop {
        let held = crate::topology::intent::read(&*store)
            .await
            .unwrap()
            .unwrap();
        if held.intent.is_terminal() {
            break held.intent;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(20),
            "the split ends"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(
        (ended.op_id.as_str(), ended.state),
        (op_id.as_str(), crate::topology::intent::State::Reclaimed)
    );
    let (stored, _) = crate::topology::store::read(&*store).await.unwrap();
    assert_eq!(stored.version, 2);
    assert_eq!(stored.last_op.map(|op| op.op_id), Some(op_id));
    assert_eq!(
        (
            records(cell.addr, &cell.low).await,
            records(cell.addr, &cell.high).await
        ),
        want
    );
    let view = topology_view(cell.addr).await;
    let leaves: Vec<(String, serde_json::Value)> = view["shards"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["prefix"].as_str().is_some_and(|p| p.starts_with("00")))
        .map(|s| {
            (
                s["prefix"].as_str().unwrap().to_string(),
                s["born_version"].clone(),
            )
        })
        .collect();
    assert_eq!(
        leaves,
        [("000".to_string(), 2.into()), ("001".to_string(), 2.into())]
    );
    assert_eq!(view["version"], 2);
    assert_eq!(view["intent"]["state"], "Reclaimed");
    assert_eq!(view["intent"]["outcome"]["result"], "published");
    tasks.cancel();
    engine_shutdown(&cell.state).await;
}

/// **The split route's refusals** (S4-DESIGN §11, edge #251). Before its loop runs: 503
/// `topology_unavailable`. Then: a body that is not `{"prefix"}`, 400 `bad_request`; no leaf, 404
/// `unknown_prefix`; an override on the leaf, 409 `topology_busy {reason: "override"}`; a live
/// intent, 409 `topology_busy {reason: "intent"}`. The merge route answers 501 `not_implemented`.
/// Nothing was split: the stored topology stays at version 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_split_routes_refusals() {
    let cell = cell().await;
    let code = |answer: &(u16, serde_json::Value)| {
        (
            answer.0,
            answer.1["error"].as_str().unwrap_or_default().to_string(),
        )
    };
    let leaf = r#"{"prefix":"00"}"#;
    assert_eq!(
        code(&post_split(cell.addr, leaf).await),
        (503, "topology_unavailable".into())
    );
    let tasks = start_ops(&cell, 7);
    assert_eq!(
        code(&post_split(cell.addr, r#"{"leaf":"00"}"#).await),
        (400, "bad_request".into())
    );
    assert_eq!(
        code(&post_split(cell.addr, r#"{"prefix":"0110"}"#).await),
        (404, "unknown_prefix".into())
    );
    let overrides = std::collections::HashMap::from([("00".to_string(), "inst-b".to_string())]);
    cell.state.ownership.set_overrides(overrides);
    let refused = post_split(cell.addr, leaf).await;
    assert_eq!(
        (code(&refused), refused.1["reason"].clone()),
        ((409, "topology_busy".into()), "override".into())
    );
    cell.state
        .ownership
        .set_overrides(std::collections::HashMap::new());
    let live = crate::topology::intent::Intent {
        kind: "split".to_string(),
        op_id: "op-other".to_string(),
        sources: vec![("1".to_string(), 9)],
        children: vec![90, 91],
        version_seen: 1,
        state: crate::topology::intent::State::Preparing,
        lease_owner: "inst-b".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    crate::topology::intent::create(&*cell.state.data_store, &live)
        .await
        .unwrap();
    let refused = post_split(cell.addr, leaf).await;
    assert_eq!(
        (code(&refused), refused.1["reason"].clone()),
        ((409, "topology_busy".into()), "intent".into())
    );
    let (status, _, _) = hreq(
        cell.addr,
        "POST",
        "/operator/topology/merge",
        &[JSON],
        b"{}",
    )
    .await;
    assert_eq!(status, 501);
    let (stored, _) = crate::topology::store::read(&*cell.state.data_store)
        .await
        .unwrap();
    assert_eq!(stored.version, 1);
    tasks.cancel();
    engine_shutdown(&cell.state).await;
}

/// **A split that would fill its instance past `SHARDS_PER_INSTANCE_MAX` is refused** (S4-DESIGN
/// §10, edge #251). With a ceiling of 1, the leaf's split answers 409 `instance_full` naming the
/// shards it would leave resident (those open here, plus one) and the ceiling; nothing is split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_that_would_fill_its_instance_is_refused() {
    let cell = cell().await;
    let tasks = start_ops(&cell, 1);
    let (status, refused) = post_split(cell.addr, r#"{"prefix":"00"}"#).await;
    let resident = cell.state.shards.open_count() + 1;
    assert_eq!(
        (status, refused),
        (
            409,
            serde_json::json!({"error": "instance_full", "resident": resident, "max": 1})
        )
    );
    let (stored, _) = crate::topology::store::read(&*cell.state.data_store)
        .await
        .unwrap();
    assert_eq!(stored.version, 1);
    assert!(
        crate::topology::intent::read(&*cell.state.data_store)
            .await
            .unwrap()
            .is_none()
    );
    tasks.cancel();
    engine_shutdown(&cell.state).await;
}

/// **A split waits for the fleet to settle** (S4-DESIGN §10.1, edge #251). On a fleet cell whose
/// published count is 10 s old, the split route answers 409 `topology_busy {reason: "fleet"}`; with
/// the count 61 s old but an override (of another prefix) published 10 s ago, the same; with the
/// override past the 600 s move cooldown too, the fleet no longer refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_waits_for_the_fleet_to_settle() {
    use super::fixture_http::{HttpRigOptions, http_rig_build};
    let fleet = mem();
    let rig = http_rig_build(
        mem(),
        super::fixture_runtime::RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    let tasks = start_ops_on(&rig.state, 7);
    let leaf = rig
        .state
        .shards
        .prefixes()
        .shards
        .leaves
        .keys()
        .next()
        .cloned()
        .unwrap();
    let body = serde_json::json!({ "prefix": leaf }).to_string();
    let put = |path: &'static str, doc: serde_json::Value| {
        let fleet = fleet.clone();
        async move {
            let raw = serde_json::to_vec(&doc).unwrap();
            object_store::ObjectStoreExt::put(&*fleet, &path.into(), raw.into())
                .await
                .unwrap();
        }
    };
    let now = crate::shard::now_ms();
    let desired = |age_ms: i64| serde_json::json!({"count": 1, "reason": "t", "epoch": 1, "computed_at_ms": now - age_ms});
    let overrides = |age_ms: i64| serde_json::json!({"entries": {"1111": {"to": "streams-1", "ms": now - age_ms}}});
    let reason = |answer: (u16, serde_json::Value)| answer.1["reason"].as_str().map(str::to_string);
    put("fleet/desired.json", desired(10_000)).await;
    assert_eq!(
        reason(post_split(rig.addr, &body).await),
        Some("fleet".into()),
        "a young count"
    );
    put("fleet/desired.json", desired(61_000)).await;
    put("fleet/overrides.json", overrides(10_000)).await;
    assert_eq!(
        reason(post_split(rig.addr, &body).await),
        Some("fleet".into()),
        "a young override"
    );
    put("fleet/overrides.json", overrides(601_000)).await;
    let settled = post_split(rig.addr, &body).await;
    assert_ne!(
        reason(settled.clone()),
        Some("fleet".into()),
        "settled: {settled:?}"
    );
    tasks.cancel();
    engine_shutdown(&rig.state).await;
}

/// `GET /operator/topology.json`'s body.
async fn topology_view(addr: Addr) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", "/operator/topology.json", &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

/// **The topology view derives a live intent's state from its witnesses** (S4-DESIGN §6.1, §11,
/// edge #251). With no intent the leaf `00` serves and the intent is null. A live split of `00` no
/// actor runs is then given each witness in turn, and the view names the newest: `Preparing`
/// (none, the leaf `splitting`), `Quiesced` (the pause's checkpoint), `ChildrenReady` (both
/// children's logs initialized), `SourcesRetired` (the parent's `'T'`), `Published` (the stored
/// topology at the next version naming the operation).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_topology_view_derives_a_live_intents_state_from_its_witnesses() {
    use crate::topology::intent;
    let cell = cell().await;
    let store = cell.state.data_store.clone();
    let view = topology_view(cell.addr).await;
    assert_eq!(
        (view["version"].clone(), view["intent"].clone()),
        (1.into(), serde_json::Value::Null)
    );
    let leaf = |view: &serde_json::Value| {
        let shards = view["shards"].as_array().unwrap().clone();
        shards.into_iter().find(|s| s["prefix"] == "00").unwrap()
    };
    assert_eq!(leaf(&view)["state"], "serving");
    let home = cell.state.shards.prefixes().shards.leaves["00"];
    let live = intent::Intent {
        kind: "split".to_string(),
        op_id: "op-view".to_string(),
        sources: vec![("00".to_string(), home)],
        children: vec![90, 91],
        version_seen: 1,
        state: intent::State::Preparing,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    intent::create(&*store, &live).await.unwrap();
    let state = |view: serde_json::Value| view["intent"]["state"].as_str().unwrap().to_string();
    let view = topology_view(cell.addr).await;
    assert_eq!(leaf(&view)["state"], "splitting");
    assert_eq!(state(view), "Preparing");
    let options = slatedb::config::CheckpointOptions {
        lifetime: Some(std::time::Duration::from_secs(3600)),
        source: None,
        name: Some("topo-op-view".to_string()),
    };
    let scope = slatedb::config::CheckpointScope::All;
    cell.parent
        .db
        .create_checkpoint(scope, &options)
        .await
        .unwrap();
    assert_eq!(state(topology_view(cell.addr).await), "Quiesced");
    for (prefix, home) in [("000", 90), ("001", 91)] {
        let path = crate::topology::model::born_home_path(prefix, home);
        let db = slatedb::Db::open(path.as_str(), store.clone())
            .await
            .unwrap();
        db.close().await.unwrap();
    }
    assert_eq!(state(topology_view(cell.addr).await), "ChildrenReady");
    let retired = crate::topology::fence::Retired {
        version: 2,
        op_id: "op-view".to_string(),
        children: vec![("000".to_string(), 90), ("001".to_string(), 91)],
    };
    let row = serde_json::to_vec(&retired).unwrap();
    cell.parent
        .db
        .put(crate::topology::fence::retired_key(), row)
        .await
        .unwrap();
    assert_eq!(state(topology_view(cell.addr).await), "SourcesRetired");
    let (stored, etag) = crate::topology::store::read(&*store).await.unwrap();
    let mut reserved = stored;
    reserved.shards.next_home = 92;
    let published = reserved.split("00", [90, 91], "op-view").unwrap();
    crate::topology::store::replace(&*store, &etag, &published)
        .await
        .unwrap();
    assert_eq!(state(topology_view(cell.addr).await), "Published");
    engine_shutdown(&cell.state).await;
}
