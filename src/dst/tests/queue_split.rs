#![cfg(test)]
//! Native queues across a split (S4-DESIGN §6, S3b-DESIGN §7.3 F7, part 2):
//! a queue in the leaf `00` of split_k's rig, its messages sealed into the
//! parent's segment objects, then `00` split. The trim pass is the gc's
//! writer of the queue's refcounts: everything it decides it reads from
//! durable rows, so a batch a split waits out is redone by the child from
//! its own cloned rows, and the child's counts end exact.

use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};
use super::split_k::{Cell, cell};
use crate::spill::object::ObjectId;

type Addr = std::net::SocketAddr;
type Engine = Arc<crate::shard::ShardEngine>;

/// A queue route with the stream key; `Null` sends no body.
async fn q(addr: Addr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let raw = if body.is_null() {
        Vec::new()
    } else {
        body.to_string().into_bytes()
    };
    let key = [("streams-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(addr, method, path, &key, &raw).await;
    let v = serde_json::from_slice(&b).unwrap_or(Value::Null);
    (st, v)
}

/// A queue created in the leaf `00`: its name, its hash and its route.
async fn queue_in_00(cell: &Cell) -> (String, [u8; 16], [u8; 16]) {
    for n in 0..400 {
        let name = format!("qs-{n}");
        let (st, v) = q(cell.addr, "PUT", &format!("/v1/queues/{name}"), Value::Null).await;
        assert_eq!(st, 201, "{v}");
        let sref = crate::tenant::ProjectId::new("proj-test")
            .unwrap()
            .stream_ref(&name);
        let desc = cell.state.registry.get(&sref).await.unwrap().unwrap();
        let seg = desc.resolve_segment("");
        if cell.state.shards.prefix_for(&seg.shard_route) == "00" {
            return (name, seg.identity, seg.shard_route);
        }
    }
    panic!("no queue lands in 00");
}

/// Each object's live chunks, by its `'R'` row.
async fn counts(engine: &crate::shard::ShardEngine) -> BTreeMap<ObjectId, u64> {
    let rows = engine.refcount_rows().await.unwrap();
    rows.into_iter()
        .filter_map(|(id, r)| Some((id, r?.live_chunks)))
        .collect()
}

/// Three messages sent, sealed, leased and acked: nothing holds the
/// queue's chunks any more, and hints name them.
async fn three_acked(cell: &Cell, name: &str) {
    for n in 0..3 {
        let path = format!("/v1/queues/{name}/messages");
        let (st, v) = q(cell.addr, "POST", &path, json!({ "body": { "n": n } })).await;
        assert_eq!(st, 201, "{v}");
    }
    cell.parent.seal_requested().await.expect("seal");
    let claim = json!({ "max": 3, "leaseMs": 60_000, "waitMs": 3_000 });
    let path = format!("/v1/queues/{name}/groups/default:claim");
    let (st, v) = q(cell.addr, "POST", &path, claim).await;
    assert_eq!(
        (st, v["messages"].as_array().map(Vec::len)),
        (200, Some(3)),
        "{v}"
    );
    let ops: Vec<Value> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| json!({ "token": m["token"], "op": "ack" }))
        .collect();
    let path = format!("/v1/queues/{name}/groups/default:settle");
    let (st, v) = q(cell.addr, "POST", &path, json!({ "ops": ops })).await;
    assert_eq!(st, 200, "{v}");
}

/// Wait until `engine`'s gc holds `refs` (a pass in progress), at most 5 s.
async fn until_refs_held(engine: &crate::shard::ShardEngine) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.gc_state().refs.try_lock().is_ok() {
        assert!(Instant::now() < deadline, "no pass took refs");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// **A trim batch a split waits out is redone by the child, exactly once.**
/// The split pauses at its park, holding the parent's gate exclusively; a
/// trim pass of the parent then reads its rows and waits at the gate with
/// its batch. Released, the split publishes and retires the parent, and
/// the parked batch lands in the retired parent's log or fails with it:
/// the children never see it. The child that holds the queue trims from
/// its own cloned hints and counts: every object's count falls by exactly
/// the queue's chunks in it, a second pass gives nothing back, and the
/// other child has nothing of the queue to trim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_trim_batch_a_split_waits_out_is_redone_by_the_child_exactly_once() {
    let cell = cell().await;
    let quiet = |e: &Engine| e.gc_state().configure(|s| s.auto = false);
    quiet(&cell.parent);
    let (name, hash, route) = queue_in_00(&cell).await;
    three_acked(&cell, &name).await;
    let chunks = cell.parent.live_chunks(hash).await.unwrap();
    assert!(!chunks.is_empty(), "the queue's pages are sealed");
    let mut queue_chunks: BTreeMap<ObjectId, u64> = BTreeMap::new();
    for c in &chunks {
        *queue_chunks.entry(c.object).or_default() += 1;
    }
    let (reached, reached_rx) = tokio::sync::oneshot::channel();
    let (go_tx, go) = tokio::sync::oneshot::channel();
    let point = crate::topology::gate::PausePoint { reached, go };
    cell.parent.gc_state().gate.pause_at_park(point);
    let clock = cell.state.runtime.clock.clone();
    let split =
        crate::topology::split::split(&cell.state.shards, &cell.parent, "op-qs", "inst-a", &*clock);
    let (start_tx, start) = tokio::sync::oneshot::channel::<()>();
    let done = std::sync::atomic::AtomicBool::new(false);
    let trim = async {
        start.await.expect("the control starts the trim");
        let r = cell.parent.trim_pass().await;
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        r
    };
    let control = async {
        reached_rx.await.expect("the split paused at its park");
        assert!(start_tx.send(()).is_ok());
        until_refs_held(&cell.parent).await;
        // Its reads are in memory: it is at the gate well within this.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let waiting = !done.load(std::sync::atomic::Ordering::SeqCst);
        assert!(waiting, "the trim's batch waits at the gate");
        assert!(go_tx.send(()).is_ok(), "the split waits for go");
    };
    let (published, parked, ()) = tokio::join!(split, trim, control);
    let published = published.expect("the split publishes");
    assert!(cell.parent.is_closed(), "the parent is retired");
    let Ok(child) = cell.state.engine_for_quiet(&route).await else {
        panic!("this instance owns the queue's child");
    };
    let other = published
        .children
        .iter()
        .map(|(_, _, e)| e.clone())
        .find(|e| e.prefix != child.prefix)
        .expect("the other child");
    for e in [&child, &other] {
        quiet(e);
    }
    let before = counts(&child).await;
    let report = child.trim_pass().await.unwrap();
    assert_eq!(report.released, chunks.len() as u64, "parked: {parked:?}");
    let after = counts(&child).await;
    for (id, n) in &queue_chunks {
        let left = before[id] - n;
        assert_eq!(
            after.get(id).copied().unwrap_or(0),
            left,
            "{id:?}: the queue's {n} chunk(s) given back once"
        );
    }
    assert_eq!(child.trim_pass().await.unwrap().released, 0, "once");
    assert_eq!(
        other.trim_pass().await.unwrap().released,
        0,
        "nothing of the queue"
    );
    let (st, v) = q(cell.addr, "GET", &format!("/v1/queues/{name}"), Value::Null).await;
    assert_eq!((st, &v["stats"]["queuedMessages"]), (200, &json!(0)), "{v}");
    engine_shutdown(&cell.state).await;
}

/// One claim of up to `max` from `name`: each message's body `n`, attempt and token.
async fn claim(cell: &Cell, name: &str, max: u32) -> Vec<(Value, Value, Value)> {
    let body = json!({ "max": max, "leaseMs": 60_000, "waitMs": 3_000 });
    let path = format!("/v1/queues/{name}/groups/default:claim");
    let (st, v) = q(cell.addr, "POST", &path, body).await;
    assert_eq!(st, 200, "{v}");
    v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["body"]["n"].clone(),
                m["attempt"].clone(),
                m["token"].clone(),
            )
        })
        .collect()
}

/// Ack `tokens`: each settle's outcome.
async fn ack(cell: &Cell, name: &str, tokens: &[Value]) -> Vec<Value> {
    let ops: Vec<Value> = tokens
        .iter()
        .map(|t| json!({ "token": t, "op": "ack" }))
        .collect();
    let path = format!("/v1/queues/{name}/groups/default:settle");
    let (st, v) = q(cell.addr, "POST", &path, json!({ "ops": ops })).await;
    assert_eq!(st, 200, "{v}");
    v["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["outcome"].clone())
        .collect()
}

/// Send `{"n": n}` for each of `ns` to `name`.
async fn send(cell: &Cell, name: &str, ns: std::ops::Range<u32>) {
    for n in ns {
        let path = format!("/v1/queues/{name}/messages");
        let (st, v) = q(cell.addr, "POST", &path, json!({ "body": { "n": n } })).await;
        assert_eq!(st, 201, "{v}");
    }
}

/// The queue of the split-then-merge DST, before the split: m0-m3 sealed,
/// m0 acked, m1 leased (its token returned), m4 and m5 sent with the
/// sealer paused, so their pages are unsealed.
async fn mixed_queue(cell: &Cell, name: &str) -> Value {
    send(cell, name, 0..4).await;
    cell.parent.seal_requested().await.expect("seal");
    let leased = claim(cell, name, 2).await;
    assert_eq!(
        leased.iter().map(|l| l.0.clone()).collect::<Vec<_>>(),
        [json!(0), json!(1)]
    );
    assert_eq!(ack(cell, name, &[leased[0].2.clone()]).await, [json!("ok")]);
    cell.parent.pause_seal(true);
    send(cell, name, 4..6).await;
    leased[1].2.clone()
}

/// **A queue split, then merged, delivers every message exactly as before,
/// and gives back every chunk once.** Its leaf `00` holds sealed chunks,
/// an open delivery and unsealed pages when it splits into `000` and
/// `001`, which then merge back into a fresh `00`. The merged leaf claims
/// exactly the four untaken messages, in order, each a first attempt; the
/// delivery leased before the split settles with its own token; and once
/// everything is acked and sealed, the merged leaf's trim gives back each of
/// the queue's chunks: each object's count falls by exactly its queue
/// chunks, and a second pass gives nothing back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_queue_split_then_merged_delivers_every_message_as_before_and_gives_back_each_chunk_once()
{
    let cell = cell().await;
    let quiet = |e: &Engine| e.gc_state().configure(|s| s.auto = false);
    quiet(&cell.parent);
    let (name, hash, _) = queue_in_00(&cell).await;
    let held = mixed_queue(&cell, &name).await;
    assert!(
        cell.parent.unsealed_page_for_tests(&hash, 4).is_some(),
        "m4 is unsealed at the split"
    );
    super::merge_k::split_00(&cell.state, &cell.parent).await;
    let halves = super::merge_k::halves_of_00(&cell.state).await;
    let clock = cell.state.runtime.clock.clone();
    let merged = crate::topology::merge::merge(
        &cell.state.shards,
        [&halves[0], &halves[1]],
        "op-qm",
        "inst-a",
        &*clock,
    )
    .await
    .expect("the merge publishes");
    let m = merged.merged.2.clone().expect("the merged leaf opened");
    quiet(&m);
    let taken = claim(&cell, &name, 10).await;
    let got: Vec<(Value, Value)> = taken.iter().map(|t| (t.0.clone(), t.1.clone())).collect();
    let want: Vec<(Value, Value)> = (2..6).map(|n| (json!(n), json!(1))).collect();
    assert_eq!(
        got, want,
        "the four untaken messages, in order, first attempts"
    );
    assert_eq!(
        ack(&cell, &name, &[held]).await,
        [json!("ok")],
        "m1's lease held"
    );
    let tokens: Vec<Value> = taken.iter().map(|t| t.2.clone()).collect();
    assert_eq!(ack(&cell, &name, &tokens).await, vec![json!("ok"); 4]);
    m.seal_requested().await.expect("seal");
    let chunks = m.live_chunks(hash).await.unwrap();
    let mut queue_chunks: BTreeMap<ObjectId, u64> = BTreeMap::new();
    for c in &chunks {
        *queue_chunks.entry(c.object).or_default() += 1;
    }
    let before = counts(&m).await;
    assert_eq!(m.trim_pass().await.unwrap().released, chunks.len() as u64);
    let after = counts(&m).await;
    for (id, n) in &queue_chunks {
        let left = before[id] - n;
        assert_eq!(after.get(id).copied().unwrap_or(0), left, "{id:?}");
    }
    assert_eq!(m.trim_pass().await.unwrap().released, 0, "once");
    let (st, v) = q(cell.addr, "GET", &format!("/v1/queues/{name}"), Value::Null).await;
    assert_eq!((st, &v["stats"]["queuedMessages"]), (200, &json!(0)), "{v}");
    engine_shutdown(&cell.state).await;
}
