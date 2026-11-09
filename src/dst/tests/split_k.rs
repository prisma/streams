#![cfg(test)]
//! The split end to end on a serving rig (S4-DESIGN §6, K-SPLIT-DECISION-2026-11-09 §5): two
//! streams, one in each half of the leaf `00`, sealed into one segment object (one `'K'` row naming
//! both), then `00` split into `000` and `001`. Each child serves its stream back byte-exact
//! through the row it was copied (keeping only its range's entries), and the children's `'R'` rows
//! for the object sum to the parent's.

use super::billing_closure_debts::{JSON, engine_of, raw_put, stored};
use super::fixture_http::{engine_shutdown, http_rig};
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
    let store = mem();
    let (state, addr) = http_rig(store.clone()).await;
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
