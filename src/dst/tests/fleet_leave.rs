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
/// `streams-2`), both views naming both and giving `10` and `11` to `streams-2`: the fleet and
/// data stores and the two members.
async fn leave_cell() -> (Stores, HttpRig, HttpRig) {
    leave_cell_over(mem()).await
}

/// [`leave_cell`] over the fleet store `fleet`.
async fn leave_cell_over(fleet: Arc<dyn ObjectStore>) -> (Stores, HttpRig, HttpRig) {
    use crate::dst::fault_store::{FaultPlan, FaultStore};
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
    ((fleet, data), a, b)
}

/// A cell's fleet store and its data store.
type Stores = (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>);

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
pub(super) async fn listed(fleet: &Arc<dyn ObjectStore>) -> Option<Vec<String>> {
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
    let ((fleet, _), a, b) = leave_cell().await;
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
    let ((fleet, _), a, b) = leave_cell().await;
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
    let ((fleet, _), a, b) = leave_cell().await;
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
    let ((fleet, _), a, b) = leave_cell().await;
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

/// How long the tick takes to read and act: one period.
const TICK: Duration = Duration::from_secs(2);

/// A gone member: its name, and its last beat's age and whether it was draining, if it wrote one.
pub(super) type Gone = (&'static str, Option<(i64, bool)>);

/// A cell of `streams-1` alone at a count of two whose document also lists each of `gone` with
/// the boot id `gone-1`, a process that is gone: its heartbeat file written once, already `age_ms`
/// old and draining or not, as its last beat was, or none (a boot stamped and killed before its
/// first beat). The fleet store and `streams-1`, its tick running.
pub(super) async fn gone_cell(gone: &[Gone]) -> (Arc<dyn ObjectStore>, HttpRig) {
    let (fleet, data) = (mem(), mem());
    put(&fleet, "fleet/desired.json", desired(1, 1)).await;
    let a = balance_member(&fleet, &data, "streams-1", RigRuntime::first()).await;
    let t0 = Instant::now();
    while listed(&fleet).await != Some(vec!["streams-1".to_string()]) {
        assert!(t0.elapsed() < BOUND, "streams-1 lists itself");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (name, beat) in gone {
        let Some((age_ms, draining)) = beat else {
            continue;
        };
        let ts_ms = crate::shard::now_ms() - age_ms;
        let body = format!(
            r#"{{"instance":"{name}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":{draining},"boot_id":"gone-1"}}"#
        );
        let path = Path::from(format!("fleet/{name}.json"));
        fleet.put(&path, PutPayload::from(body)).await.unwrap();
    }
    loop {
        let (mut doc, version) = a.state.fleet.read_overrides().await.unwrap();
        let members = doc.members.get_or_insert_with(Vec::new);
        members.extend(gone.iter().map(|(name, _)| crate::fleet::Member {
            name: (*name).to_string(),
            boot_id: Some("gone-1".to_string()),
        }));
        let raw = serde_json::to_vec(&doc).unwrap();
        let document = crate::fleet::FleetDocument::Overrides;
        if a.state.fleet.replace_document(document, raw, version).await {
            break;
        }
        assert!(t0.elapsed() < BOUND, "the gone members are listed");
    }
    put(&fleet, "fleet/desired.json", desired(2, 2)).await;
    (fleet, a)
}

/// Wait, bounded by `within`, until the document lists exactly `names`; while it still lists
/// `before` instead, every sample must, until `held` has passed.
pub(super) async fn listed_after(
    fleet: &Arc<dyn ObjectStore>,
    (before, names): (&[&str], &[&str]),
    (held, within): (Duration, Duration),
) -> Duration {
    let as_names = |list: &[&str]| Some(list.iter().map(|name| (*name).to_string()).collect());
    let t0 = Instant::now();
    loop {
        let now = listed(fleet).await;
        if t0.elapsed() < held {
            assert_eq!(now, as_names(before), "still listed at {:?}", t0.elapsed());
        } else if now == as_names(names) {
            return t0.elapsed();
        }
        assert!(
            t0.elapsed() < within,
            "listed {now:?} at {:?}",
            t0.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **C14's first arm: a listed member whose last beat is old is removed at the bound, not
/// before** (Addendum 41 B items 6 and 7; Addendum 61). `streams-2`'s last beat of its listed
/// boot was written 6 s short of `PROGRESS_DEADLINE` plus `RING_LIVENESS` (169 s) and it is gone:
/// it stays listed until a tick before the bound, then `streams-1`'s pin pass removes it, within
/// the count as beyond it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_listed_member_whose_last_beat_is_old_is_removed_at_the_bound_not_before() {
    let short = Duration::from_secs(6);
    let age = crate::fleet::DEAD_MS - i64::try_from(short.as_millis()).unwrap();
    let (fleet, a) = gone_cell(&[("streams-2", Some((age, false)))]).await;
    let took = listed_after(
        &fleet,
        (&["streams-1", "streams-2"], &["streams-1"]),
        (short - TICK, short + BOUND),
    )
    .await;
    eprintln!("LEAVE dead stale removed after {took:?}");
    super::fleet_handoff_cases::stop(a).await;
}

/// **A drained member stays listed until C14's draining arm removes it** (Addendum 61, the stopgap's
/// own behaviour until the drain's half). `streams-2` and `streams-3` are gone, their last beats
/// 6 s short of `RING_LIVENESS` (30 s) old; `streams-2`'s was draining. Both stay listed until a
/// tick before the bound; then `streams-2` alone is removed, and `streams-3`, whose beat states
/// no end, stays listed: its arm is the 169 s one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_drained_member_stays_listed_until_the_draining_arm_removes_it() {
    let short = Duration::from_secs(6);
    let age = 30_000 - i64::try_from(short.as_millis()).unwrap();
    let gone = [
        ("streams-2", Some((age, true))),
        ("streams-3", Some((age, false))),
    ];
    let (fleet, a) = gone_cell(&gone).await;
    let took = listed_after(
        &fleet,
        (
            &["streams-1", "streams-2", "streams-3"],
            &["streams-1", "streams-3"],
        ),
        (short - TICK, short + BOUND),
    )
    .await;
    eprintln!("LEAVE dead draining removed after {took:?}");
    tokio::time::sleep(TICK * 3).await;
    let kept = Some(vec!["streams-1".to_string(), "streams-3".to_string()]);
    assert_eq!(
        listed(&fleet).await,
        kept,
        "a stale beat that states no end stays listed"
    );
    super::fleet_handoff_cases::stop(a).await;
}

/// The scale-in that releases `streams-2` from [`leave_cell`]: the count drops to one and the
/// test waits for the leave to end.
async fn released(fleet: &Arc<dyn ObjectStore>, rigs: [&HttpRig; 2]) {
    put(fleet, "fleet/desired.json", desired(1, 2)).await;
    left(fleet, rigs, BOUND).await;
}

/// **A count that comes back after the release: the leaver joins again as a joiner** (Addendum
/// 41 B item 12). `streams-2` has left; the count returns to two. `streams-2`, unlisted and
/// within the count, joins over `streams-1`'s pins with nothing uncovered (no join past
/// `JOIN_PHASE`), the document lists both and both views name both; no writer is fenced or given
/// up on either member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_count_back_after_the_release_joins_the_leaver_again_as_a_joiner() {
    let ((fleet, _), a, b) = leave_cell().await;
    released(&fleet, [&a, &b]).await;
    put(&fleet, "fleet/desired.json", desired(2, 3)).await;
    let both = vec!["streams-1".to_string(), "streams-2".to_string()];
    let t0 = Instant::now();
    loop {
        let rings = [&a, &b].map(|rig| rig.state.ownership.ring_active());
        if listed(&fleet).await.as_ref() == Some(&both) && rings.iter().all(|ring| *ring == both) {
            break;
        }
        assert!(t0.elapsed() < BOUND, "streams-2 joins again: {rings:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        b.state.fleet.standing().joins_unpinned(),
        0,
        "joined covered"
    );
    assert_no_fence([&a, &b]);
    stopped([a, b]).await;
}

/// **A leaver with no live member within the count keeps serving** (Addendum 41 B item 3). Once
/// `streams-1` is stopped and out of `streams-2`'s ring, the count drops to one: `streams-2` is
/// alone and beyond it, with no successor,
/// so its phase's clock does not run. It stays listed past `LEAVE_PHASE` with nothing given up.
/// When `streams-1` boots again, `streams-2` hands off to it and releases itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaver_with_no_live_member_within_the_count_keeps_serving_until_one_comes() {
    let ((fleet, data), a, b) = leave_cell().await;
    super::fleet_handoff_cases::stop(a).await;
    // Until its last beat is stale `streams-1` is still a successor (R1d), as for a drain.
    let t0 = Instant::now();
    while b.state.ownership.ring_active() != vec!["streams-2".to_string()] {
        assert!(
            t0.elapsed() < BOUND * 2,
            "streams-1 leaves streams-2's ring"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    let leave = b.state.fleet.standing().leave();
    while !leave.leaving() {
        assert!(t0.elapsed() < BOUND, "streams-2 leaves");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(32) + TICK * 3).await;
    let both = Some(vec!["streams-1".to_string(), "streams-2".to_string()]);
    assert_eq!(
        listed(&fleet).await,
        both,
        "no successor: the leave does not release"
    );
    assert_eq!(leave.evacuated(), 0, "nothing given up");
    let a = balance_member(&fleet, &data, "streams-1", RigRuntime::incarnation(2)).await;
    let urls = serde_json::json!({
        "streams-1": format!("http://{}", a.addr),
        "streams-2": format!("http://{}", b.addr),
    });
    put(&fleet, "fleet/urls.json", urls.to_string()).await;
    left(&fleet, [&a, &b], BOUND * 2).await;
    assert_eq!(leave.evacuated(), 0, "handed off, not given up");
    stopped([a, b]).await;
}

/// **A drain during a leave moves each shard once** (the draft's test 6; Addendum 67 §2).
/// `streams-2` holds `10` and `11` with `10`'s committer held, so its leave's look waits on `10`'s
/// move with `11` still to ask for; the count drops to one, and once it is leaving `streams-2`
/// drains, the hold released only once the drain has begun. From the drain's start the leave asks
/// for no further move (the drain moves its shards itself, G5), the drain ends `HandedOff`, the
/// next pass ends the leave, `streams-1` holds every leaf, and no writer is fenced or given up on
/// either member. Its controls (the look's loop without its recheck, and `handing_off` without its
/// drain) ask for `11`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_drain_during_a_leave_moves_each_shard_once() {
    let ((fleet, _), a, b) = leave_cell().await;
    let engine = held_on(&b).await;
    let held = engine.test_hold_commit().await;
    queued_behind(&engine);
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    let standing = b.state.fleet.standing();
    let t0 = Instant::now();
    while !standing.leave().leaving() || standing.leave().asked() == 0 {
        assert!(
            t0.elapsed() < BOUND,
            "streams-2 leaves and asks for its first move"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let drained = crate::fleet::drain::drain(&b.state, "streams-2", crate::fleet::drain::BUDGET);
    let begun = async {
        while !standing.drain_begun() {
            assert!(t0.elapsed() < BOUND * 2, "the drain begins");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let asked = standing.leave().asked();
        drop(held);
        asked
    };
    let (outcome, asked) = tokio::join!(tokio::time::timeout(BOUND, drained), begun);
    let outcome = outcome.expect("the drain ends");
    assert_eq!(outcome, crate::fleet::drain::DrainOutcome::HandedOff);
    // The leave is the last pass's verdict: the next pass, a drain begun, ends it.
    while standing.leave().leaving() {
        assert!(t0.elapsed() < BOUND * 3, "a drain ends the leave");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        standing.leave().asked(),
        asked,
        "the leave asks for no move once the drain has begun"
    );
    assert!(
        b.state.shards.held_prefixes().is_empty(),
        "streams-2 holds nothing"
    );
    assert_no_fence([&a, &b]);
    stopped([a, b]).await;
}

/// **A late request at a sealed leaver opens nothing** (C10, G2's case for the leave; the draft's
/// test 4). `streams-2` holds `10` by a stream's creation; the count drops to one and it leaves.
/// Its view publications are held, each after its pass, until the pass whose CAS released it:
/// between that release and its publication its view is set back by hand to one that gives it
/// `10`, as a stale view routes. An append to the stream in `10` through `streams-2` is answered
/// `503 shard_closing`, `Retry-After: 1`, and `10` is not open there. Its control
/// (`nc_release_unsealed`, a release before the seal) opens it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_late_request_at_a_sealed_leaver_opens_nothing() {
    let ((fleet, _), a, b) = leave_cell().await;
    let mut taken = 0;
    let stream = stream_in(&b.state, b.addr, "10", &mut taken).await;
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    let t0 = Instant::now();
    let go = loop {
        let (reached, reached_rx) = tokio::sync::oneshot::channel();
        let (go, held) = tokio::sync::oneshot::channel();
        let point = crate::topology::gate::PausePoint { reached, go: held };
        b.state.fleet.standing().pause_view_publication(point);
        let paused = tokio::time::timeout(BOUND, reached_rx).await;
        assert!(matches!(paused, Ok(Ok(()))), "streams-2's pass is held");
        let mine = listed(&fleet)
            .await
            .is_some_and(|names| names.contains(&"streams-2".into()));
        if !mine {
            break go;
        }
        go.send(()).ok();
        assert!(t0.elapsed() < BOUND, "streams-2 releases itself");
    };
    let stale = vec!["streams-1".to_string(), "streams-2".to_string()];
    b.state
        .ownership
        .set_view(stale, std::collections::HashMap::new());
    let owner = b.state.ownership.effective_owner("10");
    assert_eq!(
        owner.as_deref(),
        Some("streams-2"),
        "a stale view gives 10 here"
    );
    let path = format!("/v1/stream/{stream}");
    let json = [super::billing_closure_debts::JSON];
    let (status, headers, body) =
        super::fixture_requests::hreq(b.addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
    let retry = headers.get("retry-after").map(String::as_str);
    let answer = String::from_utf8_lossy(&body).into_owned();
    assert_eq!((status, retry), (503, Some("1")), "{answer}");
    assert!(
        !b.state.shards.is_open("10"),
        "nothing opened at the sealed leaver"
    );
    go.send(()).ok();
    stopped([a, b]).await;
}

/// The overrides document's requests that landed in `trace`: its GETs and its PUTs.
fn overrides_requests(trace: &crate::dst::trace_store::TraceStore) -> (usize, usize) {
    use crate::dst::StoreOp;
    use crate::dst::trace_store::TraceOutcome;
    let events = trace.events();
    let landed = |op: StoreOp| {
        let of = |event: &&crate::dst::trace_store::StoreTraceEvent| {
            event.op == op
                && event.path == "fleet/overrides.json"
                && matches!(event.outcome, TraceOutcome::Ok)
        };
        events.iter().filter(of).count()
    };
    (landed(StoreOp::Get), landed(StoreOp::Put))
}

/// **An idle fleet with a listed leaver writes nothing and reads at its tick's pace** (C16;
/// Addendum 41 B item 12). `streams-2` is a leaver with no successor (`streams-1` stopped and
/// out of its ring), so it stands listed. Over ten periods its document is never written, and
/// it reads it at most twice a period: its tick's pass and its leave's look.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_idle_fleet_with_a_listed_leaver_writes_nothing_and_reads_at_its_ticks_pace() {
    let trace = crate::dst::trace_store::TraceStore::verbatim(mem());
    let ((fleet, _), a, b) = leave_cell_over(trace.clone()).await;
    super::fleet_handoff_cases::stop(a).await;
    let t0 = Instant::now();
    while b.state.ownership.ring_active() != vec!["streams-2".to_string()] {
        assert!(
            t0.elapsed() < BOUND * 2,
            "streams-1 leaves streams-2's ring"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    while !b.state.fleet.standing().leave().leaving() {
        assert!(t0.elapsed() < BOUND * 3, "streams-2 leaves");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(TICK).await;
    let before = overrides_requests(&trace);
    let periods: u32 = 10;
    tokio::time::sleep(TICK * periods).await;
    let after = overrides_requests(&trace);
    let (gets, puts) = (after.0 - before.0, after.1 - before.1);
    eprintln!("LEAVE idle periods={periods} overrides gets={gets} puts={puts}");
    assert_eq!(puts, 0, "an idle leave writes nothing");
    let pace = usize::try_from(periods * 2 + 2).unwrap();
    assert!(gets <= pace, "{gets} reads in {periods} periods");
    super::fleet_handoff_cases::stop(b).await;
}

/// The cell of three: `streams-1`, `streams-2` and `streams-3` at a count of three over one data
/// store 20 ms away, the four [`LEAVES`], every view naming all three and the cell settled.
async fn three_cell() -> (Arc<dyn ObjectStore>, [HttpRig; 3]) {
    let (fleet, data) = (mem(), mem());
    put(&fleet, "fleet/desired.json", desired(3, 1)).await;
    let mut rigs = Vec::new();
    for (n, name) in ["streams-1", "streams-2", "streams-3"]
        .into_iter()
        .enumerate()
    {
        let runtime = RigRuntime::incarnation(u64::try_from(n).unwrap());
        let rig = balance_member(&fleet, &data, name, runtime).await;
        let t0 = Instant::now();
        let beat = Path::from(format!("fleet/{name}.json"));
        while fleet.head(&beat).await.is_err() {
            assert!(t0.elapsed() < BOUND, "{name} beats");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        rigs.push(rig);
    }
    let urls: serde_json::Map<String, serde_json::Value> = rigs
        .iter()
        .map(|rig| {
            (
                rig.state.ownership.instance().to_string(),
                format!("http://{}", rig.addr).into(),
            )
        })
        .collect();
    put(
        &fleet,
        "fleet/urls.json",
        serde_json::Value::Object(urls).to_string(),
    )
    .await;
    let refs: Vec<&HttpRig> = rigs.iter().collect();
    super::fixture_members::settled(&fleet, &refs, BOUND * 2).await;
    let Ok(rigs) = <[HttpRig; 3]>::try_from(rigs) else {
        panic!("three rigs");
    };
    (fleet, rigs)
}

/// **A leave from three to one hands off only within the count** (C11 for the leave; the draft's
/// test 3, at three). The count drops to one: `streams-2` and `streams-3` both leave, and each
/// hands off only to `streams-1`, never to the other leaver: no leaf opens on a leaver that it
/// did not hold when the count dropped. The document then lists `streams-1` alone, and no writer
/// is fenced or given up on any member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leave_from_three_to_one_hands_off_only_within_the_count() {
    let (fleet, [a, b, c]) = three_cell().await;
    let open_on = |rig: &HttpRig| -> Vec<bool> {
        LEAVES
            .iter()
            .map(|leaf| rig.state.shards.is_open(leaf))
            .collect()
    };
    let start = [open_on(&b), open_on(&c)];
    put(&fleet, "fleet/desired.json", desired(1, 2)).await;
    let one = Some(vec!["streams-1".to_string()]);
    let t0 = Instant::now();
    while listed(&fleet).await != one {
        for (leaver, was) in [&b, &c].iter().zip(&start) {
            let arrived = open_on(leaver)
                .iter()
                .zip(was)
                .any(|(now, then)| *now && !then);
            assert!(
                !arrived,
                "{}: a leaf arrived at a leaver",
                leaver.state.ownership.instance()
            );
        }
        assert!(
            t0.elapsed() < BOUND * 2,
            "both leave: {:?}",
            listed(&fleet).await
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for rig in [&a, &b, &c] {
        let standing = rig.state.fleet.standing();
        let fenced = (
            standing.handoffs_fenced(),
            rig.state.shards.fenced_ahead(),
            standing.leave().evacuated(),
        );
        assert_eq!(fenced, (0, 0, 0), "{}", rig.state.ownership.instance());
    }
    for rig in [a, b, c] {
        super::fleet_handoff_cases::stop(rig).await;
    }
}
