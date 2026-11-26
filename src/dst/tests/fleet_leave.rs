#![cfg(test)]
//! The leave (§22 step 6; GATE-DECISION-s22-addendum-2026-11-19 C9-C11, GATE-DECISION-s22-join-
//! 2026-11-20 Addendum 41 B as Addenda 44 and 60 amend it). A member beyond the count hands every
//! leaf its view gives it off giver first while its peers still count it, seals its gate once it
//! holds nothing and releases itself in one CAS: a scale-in fences no writer (E5's scale-in class
//! gone).

use super::fixture_http::{HttpRig, engine_shutdown};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_handoff::{
    BOUND, LANES, LEAVES, Members, Tally, appender, balance_member, stored_ns, stream_in,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// A cell of `streams-1` and `streams-2` at a count of two over one data store 20 ms away, the
/// four [`LEAVES`] with no override (the ring gives `00` and `01` to `streams-1`, `10` and `11` to
/// `streams-2`), both views naming both and giving `10` and `11` to `streams-2`: the fleet store
/// and the two members.
async fn leave_cell() -> (Arc<dyn ObjectStore>, HttpRig, HttpRig) {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
    let fleet = mem();
    put(&fleet, "fleet/desired.json", desired(2, 1)).await;
    let plan = FaultPlan {
        latency_pct: 100,
        latency_ms: (20, 20),
        ..FaultPlan::CLEAN
    };
    let data: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 0x1EA, plan);
    let a = balance_member(&fleet, &data, "streams-1", RigRuntime::first()).await;
    let t0 = Instant::now();
    while fleet
        .head(&Path::from("fleet/streams-1.json"))
        .await
        .is_err()
    {
        assert!(t0.elapsed() < BOUND, "streams-1 beats");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let b = balance_member(&fleet, &data, "streams-2", RigRuntime::incarnation(1)).await;
    let urls = serde_json::json!({
        "streams-1": format!("http://{}", a.addr),
        "streams-2": format!("http://{}", b.addr),
    });
    put(&fleet, "fleet/urls.json", urls.to_string()).await;
    let t0 = Instant::now();
    while [&a, &b]
        .iter()
        .any(|rig| rig.state.ownership.ring_active().len() < 2)
    {
        assert!(t0.elapsed() < BOUND, "both views name both members");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Setup only: `streams-1` pinned `10` and `11` for `streams-2`'s join, and return-home moves
    // them to `streams-2` once it has joined; a test starts once both views give them to it.
    let theirs = |rig: &HttpRig| {
        ["10", "11"]
            .iter()
            .all(|leaf| rig.state.ownership.effective_owner(leaf).as_deref() == Some("streams-2"))
    };
    while !theirs(&a) || !theirs(&b) {
        assert!(t0.elapsed() < BOUND, "10 and 11 return to streams-2");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (fleet, a, b)
}

async fn put(fleet: &Arc<dyn ObjectStore>, path: &'static str, body: String) {
    fleet
        .put(&Path::from(path), PutPayload::from(body))
        .await
        .unwrap();
}

/// desired.json at `count` and `epoch`, as the controller publishes it.
fn desired(count: u64, epoch: u64) -> String {
    format!(r#"{{"count":{count},"epoch":{epoch},"reason":"test","computed_at_ms":0}}"#)
}

/// The members the stored overrides document lists, by name.
async fn listed(fleet: &Arc<dyn ObjectStore>) -> Option<Vec<String>> {
    let got = fleet.get(&Path::from("fleet/overrides.json")).await.ok()?;
    let doc: crate::fleet::Overrides = serde_json::from_slice(&got.bytes().await.ok()?).ok()?;
    doc.members
        .map(|members| members.into_iter().map(|member| member.name).collect())
}

/// Wait, bounded by `within`, until the document lists `streams-1` alone and both views name it
/// alone.
async fn left(fleet: &Arc<dyn ObjectStore>, rigs: [&HttpRig; 2], within: Duration) -> Duration {
    let t0 = Instant::now();
    let one = vec!["streams-1".to_string()];
    loop {
        let rings = rigs.map(|rig| rig.state.ownership.ring_active());
        let members = listed(fleet).await;
        if members.as_ref() == Some(&one) && rings.iter().all(|ring| *ring == one) {
            return t0.elapsed();
        }
        assert!(
            t0.elapsed() < within,
            "the leave ends: {members:?} {rings:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// No writer fenced on either member, by either cause (G6): a move's own late close, and a fence
/// ahead of a move; and nothing given up by a leave's fallback.
fn assert_no_fence(rigs: [&HttpRig; 2]) {
    for rig in rigs {
        let state = &rig.state;
        let standing = state.fleet.standing();
        let fenced = (
            standing.handoffs_fenced(),
            state.shards.fenced_ahead(),
            standing.leave().evacuated(),
        );
        let name = state.ownership.instance();
        assert_eq!(fenced, (0, 0, 0), "{name}: no writer fenced or given up");
    }
}

async fn stopped(rigs: [HttpRig; 2]) {
    for rig in rigs {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// **A scale-in moves its shards without fencing** (Addendum 41 B item 13; NEXT-WORK's name). A
/// stream in each of [`leave_cell`]'s leaves takes appends at both members; the count drops to
/// one. `streams-2`, beyond it, hands `10` and `11` off giver first, seals and releases itself:
/// the document then lists `streams-1` alone and both views name it alone. Neither member counts
/// a fenced writer or a given-up shard, no append ends refused, and every acked append is stored
/// once. Before the leave the interim's removal half unlisted `streams-2` in `streams-1`'s pin
/// CAS, and `streams-1`'s opens fenced `streams-2`'s writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_scale_in_moves_its_shards_without_fencing() {
    let (fleet, a, b) = leave_cell().await;
    let mut taken = 0;
    let mut streams = Vec::new();
    for leaf in LEAVES {
        streams.push(stream_in(&a.state, a.addr, leaf, &mut taken).await);
    }
    let members: Members = [("streams-1", a.addr), ("streams-2", b.addr)];
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Mutex::new(Tally::default()));
    let next = Arc::new(AtomicU32::new(0));
    let appenders = streams.iter().flat_map(|stream| {
        let (stop, tally, next) = (&stop, &tally, &next);
        let lane = move |lane| {
            let ends = (stop.clone(), tally.clone());
            appender(members, stream.clone(), lane, next.clone(), ends)
        };
        (0..LANES).map(lane)
    });
    let scaled_in = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        put(&fleet, "fleet/desired.json", desired(1, 2)).await;
        let took = left(&fleet, [&a, &b], BOUND).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.store(true, Ordering::SeqCst);
        took
    };
    let (_, took) =
        futures_util::future::join(futures_util::future::join_all(appenders), scaled_in).await;
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    eprintln!(
        "LEAVE scale-in took={took:?} replayed={} refused={:?}",
        tally.replayed, tally.refused
    );
    assert_no_fence([&a, &b]);
    assert_eq!(
        tally.refused,
        BTreeMap::new(),
        "no append is refused across the leave"
    );
    for stream in &streams {
        let mut acked: Vec<u32> = tally.acked[stream].iter().map(|ack| ack.n).collect();
        acked.sort_unstable();
        assert_eq!(
            stored_ns(a.addr, stream).await,
            acked,
            "{stream}: stored = acked"
        );
    }
    assert!(
        b.state.shards.held_prefixes().is_empty(),
        "streams-2 holds nothing"
    );
    stopped([a, b]).await;
}

/// `10` open on `streams-2`, its committer held behind an operation it took (as the drain's G3
/// test), so a move of it cannot park within its bound and ends `NotStarted` on every try: the
/// engine and the hold, which `streams-2` keeps until the guard drops.
async fn held_on(rig: &HttpRig) -> Arc<crate::shard::ShardEngine> {
    let opened = rig
        .state
        .shards
        .open_or_wait("10", Duration::from_secs(5))
        .await;
    let crate::sharddir::OpenOutcome::Ready(engine) = opened else {
        panic!("10 opens on {}", rig.state.ownership.instance());
    };
    engine
}

/// An operation `engine`'s committer takes while `held` keeps it from committing.
fn queued_behind(engine: &crate::shard::ShardEngine) {
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let close = crate::shard::CloseReq {
        hash: [3; 16],
        generation: Some(1),
        resp: tx,
    };
    engine.try_close(close).unwrap();
}

/// **A leaver past its phase releases, and what it gave up is counted once** (Addendum 41 B,
/// Addendum 60 §1). `streams-2` holds `10` with its committer held, so every move of it ends
/// `NotStarted`. The count drops to one: past `LEAVE_PHASE` `streams-2` falls back, gives `10` up
/// (one in `leave_evacuated`, one writer fenced ahead of a move) and releases; the document lists
/// `streams-1` alone. `streams-1` then opens `10`, and the fence of `streams-2`'s writer that
/// lands after is not counted again: `fenced_ahead` rose by one and no more. The leave's seal
/// stays while `10` is resident there and lifts once it is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaver_past_its_phase_releases_and_what_it_gave_up_is_counted_once() {
    let (fleet, a, b) = leave_cell().await;
    let engine = held_on(&b).await;
    let held = engine.test_hold_commit().await;
    queued_behind(&engine);
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    let phase = Duration::from_secs(32);
    let took = left(&fleet, [&a, &b], phase + BOUND).await;
    let gave_up = (
        b.state.fleet.standing().leave().evacuated(),
        b.state.shards.fenced_ahead(),
    );
    eprintln!("LEAVE fallback took={took:?} gave_up={gave_up:?}");
    assert!(took >= phase, "the phase ran out first: {took:?}");
    assert_eq!(gave_up, (1, 1), "10 given up, counted once in each");
    assert!(b.state.shards.leave_sealed(), "sealed while 10 is resident");
    let opened = a
        .state
        .shards
        .open_or_wait("10", Duration::from_secs(5))
        .await;
    assert!(
        matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
        "streams-1 takes 10"
    );
    drop(held);
    assert_eq!(engine.await_terminated(BOUND).await, Ok(()));
    let t0 = Instant::now();
    while b.state.shards.leave_sealed() {
        assert!(t0.elapsed() < BOUND, "the seal lifts once nothing is held");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        b.state.shards.fenced_ahead(),
        1,
        "no more after the peers' fences"
    );
    stopped([a, b]).await;
}

/// **A count that comes back mid-leave ends the leave and the leaver keeps serving** (the draft's
/// test 5). `streams-2` holds `10` with its committer held, so its leave cannot finish. The count
/// drops to one, and once `streams-2` is leaving it comes back to two: `streams-2` is no longer
/// leaving, its gate is not sealed, the document lists both, and once the hold is released an
/// append to `10` through `streams-2` is acked; no writer is fenced or given up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_count_that_comes_back_mid_leave_ends_it_and_the_leaver_keeps_serving() {
    let (fleet, a, b) = leave_cell().await;
    let engine = held_on(&b).await;
    let held = engine.test_hold_commit().await;
    queued_behind(&engine);
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    let leave = b.state.fleet.standing().leave();
    let t0 = Instant::now();
    while !leave.leaving() {
        assert!(t0.elapsed() < BOUND, "streams-2 leaves");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    put(&fleet, "fleet/desired.json", desired(2, 3)).await;
    while leave.leaving() {
        assert!(t0.elapsed() < BOUND, "the leave ends with the count");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(held);
    let both = Some(vec!["streams-1".to_string(), "streams-2".to_string()]);
    assert_eq!(listed(&fleet).await, both, "both stay listed");
    assert!(!b.state.shards.leave_sealed(), "never sealed");
    let mut taken = 0;
    let stream = stream_in(&b.state, b.addr, "10", &mut taken).await;
    let path = format!("/v1/stream/{stream}");
    let json = [super::billing_closure_debts::JSON];
    let (status, _, body) =
        super::fixture_requests::hreq(b.addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
    assert_eq!(status, 204, "{}", String::from_utf8_lossy(&body));
    assert_no_fence([&a, &b]);
    stopped([a, b]).await;
}

/// **A member removed while alive counts it and joins again** (C14, the draft §5). A write
/// removes `streams-2`'s listing, as C14 would for a member it judged dead: `streams-2`'s next
/// pass finds itself unlisted with no release of its own, counts one `removed_while_listed`, and,
/// within the count, joins again, so the document lists both once more. No writer is fenced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_removed_while_alive_counts_it_and_joins_again() {
    let (fleet, a, b) = leave_cell().await;
    let t0 = Instant::now();
    loop {
        let (mut doc, version) = a.state.fleet.read_overrides().await.unwrap();
        if let Some(members) = doc.members.as_mut() {
            members.retain(|member| member.name != "streams-2");
        }
        let raw = serde_json::to_vec(&doc).unwrap();
        let document = crate::fleet::FleetDocument::Overrides;
        if a.state.fleet.replace_document(document, raw, version).await {
            break;
        }
        assert!(t0.elapsed() < BOUND, "the removal is written");
    }
    let dead = b.state.fleet.standing().dead();
    let both = Some(vec!["streams-1".to_string(), "streams-2".to_string()]);
    while dead.removed_while_listed() == 0 || listed(&fleet).await != both {
        assert!(
            t0.elapsed() < BOUND,
            "streams-2 counts its removal and joins again"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(dead.removed_while_listed(), 1);
    assert_no_fence([&a, &b]);
    stopped([a, b]).await;
}
