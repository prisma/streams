#![cfg(test)]
//! A planned drain is a sequence of planned moves, then an announcement (F6;
//! GATE-DECISION-f6-prep-rows-rev2-2026-11-17, addendum 2026-11-18 "drain fences ahead of the
//! handoff", G1-G10). The drainer hands every shard off giver first while its peers still count
//! it, seals its open gate once it holds nothing, and only then announces `draining`; no leaf
//! leaves it but through a move's hold, and no writer is fenced.

use super::fixture_http::{HttpRig, engine_shutdown};
use super::fleet_handoff::{
    BOUND, HOLD, LANES, LEAVES, Members, Tally, appender, balance_cell, balance_member, stored_ns,
    stream_in,
};
use super::fleet_handoff_cases::ErrorLines;
use crate::fleet::drain::{BUDGET, DrainOutcome, drain};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// Watch `drainer`'s leaves every 5 ms until `stop`: each time a leaf it served stops being open
/// with no move holding it, then or at the sample before (a fence, a yield or a retirement, not
/// a handoff), and each time a leaf it did not serve opens on it (a move towards the drainer).
/// The departures without a hold, and the arrivals per leaf.
async fn watch_leaves(
    drainer: &HttpRig,
    stop: &AtomicBool,
) -> (Vec<String>, BTreeMap<&'static str, u32>) {
    let shards = &drainer.state.shards;
    let sample = || LEAVES.map(|leaf| (shards.is_open(leaf), shards.handing_off(leaf)));
    let (mut before, mut departed, mut arrived) = (sample(), Vec::new(), BTreeMap::new());
    let t0 = Instant::now();
    while !stop.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let now = sample();
        for (leaf, ((was_open, was_held), (open, held))) in
            LEAVES.iter().zip(before.iter().zip(&now))
        {
            if *was_open && !open && !was_held && !held {
                departed.push(format!("{leaf} at {:?}", t0.elapsed()));
            }
            if !was_open && *open {
                *arrived.entry(*leaf).or_default() += 1;
            }
        }
        before = now;
    }
    (departed, arrived)
}

/// `a`, the cell's drainer, drains while a stream in each of its leaves takes appends at both
/// members; the drain's outcome and length, what the appenders saw, and what the leaf watch saw.
async fn drained_under_appends(
    a: &HttpRig,
    b: &HttpRig,
    streams: &[String],
) -> (
    (DrainOutcome, Duration),
    Tally,
    (Vec<String>, BTreeMap<&'static str, u32>),
) {
    let members: Members = [("streams-1", a.addr), ("streams-2", b.addr)];
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Mutex::new(Tally::default()));
    let next = Arc::new(AtomicU32::new(0));
    let appenders = streams.iter().flat_map(|stream| {
        let (stop, tally, next) = (&stop, &tally, &next);
        let lane = move |lane| {
            appender(
                members,
                stream.clone(),
                lane,
                next.clone(),
                (stop.clone(), tally.clone()),
            )
        };
        (0..LANES).map(lane)
    });
    let drained = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let t0 = Instant::now();
        let outcome = drained_within(&a.state, BOUND).await;
        let took = t0.elapsed();
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.store(true, Ordering::SeqCst);
        (outcome, took)
    };
    let watched = watch_leaves(a, &stop);
    let appended = futures_util::future::join_all(appenders);
    let joined = futures_util::future::join3(appended, drained, watched).await;
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    (joined.1, tally, joined.2)
}

/// No writer fenced on either member, by either cause (G6): a move's own late close, and a fence
/// ahead of a move.
fn assert_no_fence(rigs: [&HttpRig; 2]) {
    for rig in rigs {
        let state = &rig.state;
        let fenced = (
            state.fleet.standing().handoffs_fenced(),
            state.shards.fenced_ahead(),
        );
        assert_eq!(
            fenced,
            (0, 0),
            "{}: no writer fenced",
            state.ownership.instance()
        );
    }
}

/// Each stream's giver's last ack is followed by its successor's first within `HOLD`, and every
/// acked append of it is stored once on the successor `b`.
async fn assert_handed_and_stored(tally: &Tally, streams: &[String], b: &HttpRig) {
    for stream in streams {
        let acks = &tally.acked[stream];
        let by = |member: &'static str| {
            acks.iter()
                .filter(move |ack| ack.by == member)
                .map(|ack| ack.at)
        };
        let (Some(last), Some(first)) = (by("streams-1").max(), by("streams-2").min()) else {
            panic!("both members acked {stream}");
        };
        let gap = first.saturating_duration_since(last);
        eprintln!("HANDOFF drain {stream} gap={gap:?}");
        assert!(
            gap < HOLD,
            "{stream}: giver's last ack to target's first: {gap:?}"
        );
        let mut acked: Vec<u32> = acks.iter().map(|ack| ack.n).collect();
        acked.sort_unstable();
        assert_eq!(
            stored_ns(b.addr, stream).await,
            acked,
            "{stream}: stored = acked"
        );
    }
}

async fn stopped(rigs: [HttpRig; 2]) {
    for rig in rigs {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// **A drain hands every shard off before it announces, and fences no writer** (the planned
/// drain, F6; the drain addendum G1, G6, G7, G10). [`balance_cell`]'s `streams-1` holds all four
/// leaves; a stream in each takes appends at both members while `streams-1` drains. The drain
/// ends `HandedOff` in well under its budget; no leaf leaves `streams-1` but through a move's hold
/// (none is fenced, yielded or retired ahead of its handoff); neither member counts a fenced
/// writer, by either cause; no append ends refused, on a `409` after its replays or a `503
/// shard_moving` alike; each stream's successor's first ack follows the giver's last within
/// `HOLD`; and every acked append is stored once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_drain_of_several_shards_hands_each_off_within_its_budget() {
    let errors = ErrorLines::install();
    let (a, b) = balance_cell().await;
    let mut taken = 0;
    let mut streams = Vec::new();
    for leaf in LEAVES {
        streams.push(stream_in(&a.state, a.addr, leaf, &mut taken).await);
    }
    let ((outcome, took), tally, (departed, arrived)) =
        drained_under_appends(&a, &b, &streams).await;
    eprintln!("HANDOFF drain took={took:?} outcome={outcome:?} arrived={arrived:?}");
    eprintln!(
        "HANDOFF drain replayed={} refused={:?}",
        tally.replayed, tally.refused
    );
    errors.report();
    assert_eq!(outcome, DrainOutcome::HandedOff);
    assert!(took < BOUND, "four handoffs, one after another: {took:?}");
    assert!(
        departed.is_empty(),
        "a leaf left the drainer without a hold: {departed:?}"
    );
    assert!(
        a.state.shards.held_prefixes().is_empty(),
        "streams-1 holds nothing"
    );
    assert_no_fence([&a, &b]);
    assert_eq!(
        tally.refused,
        BTreeMap::new(),
        "no append is refused across the drain"
    );
    assert_handed_and_stored(&tally, &streams, &b).await;
    stopped([a, b]).await;
}

/// **A drainer sealed against opens answers a late request to retry** (G2). `streams-1` holds
/// `00` (a stream's creation opened it), so the seal is refused; once every shard it holds has
/// closed and settled the seal holds, and from then on nothing opens here: the gate answers `shard_closing` with
/// `Retry-After: 1`, and so does an append that resolves `00` here (`503`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_drainer_sealed_against_opens_answers_a_late_request_to_retry() {
    use super::fixture_storage::mem;
    let first = super::fixture_runtime::RigRuntime::first();
    let rig = super::fleet_handoff_cases::member(&mem(), mem(), "streams-1", first, false).await;
    let name = super::fleet_handoff_cases::stream_in_00(&rig).await;
    let shards = &rig.state.shards;
    assert!(!shards.seal_for_drain(), "a resident holds the gate open");
    let reason = crate::shard_directory::RetirementReason::Shutdown;
    for prefix in shards.held_prefixes() {
        let retired = shards.retire(&prefix, reason, |_, _| true);
        let closed = matches!(retired, crate::shard_directory::RetireOutcome::Retired(..));
        assert!(closed, "{prefix} retires");
    }
    let t0 = Instant::now();
    while !shards.seal_for_drain() {
        assert!(t0.elapsed() < Duration::from_secs(5), "every close settles");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let opened = shards.open_or_wait("00", Duration::from_secs(1)).await;
    let closing = matches!(
        &opened,
        crate::sharddir::OpenOutcome::Wait {
            code: "shard_closing",
            retry_after_secs: 1
        }
    );
    assert!(closing, "the gate answers shard_closing, Retry-After 1");
    let path = format!("/v1/stream/{name}");
    let json = [super::billing_closure_debts::JSON];
    let (status, headers, body) =
        super::fixture_requests::hreq(rig.addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
    let retry = headers.get("retry-after").map(String::as_str);
    assert_eq!(
        (status, retry),
        (503, Some("1")),
        "{}",
        String::from_utf8_lossy(&body)
    );
    assert!(!shards.is_open("00"), "nothing opened here");
    super::fleet_handoff_cases::stop(rig).await;
}

/// **A drain whose handoff phase runs out announces anyway, and the fence of what it still holds
/// is counted** (G3, G6). `10`'s committer is held behind an operation it took, so its move cannot
/// park within its bound and ends `NotStarted` on every try; once the handoff phase has passed,
/// the drain stops opening, gives `10` up (counted once on `streams-1`, as a writer taken ahead of
/// a move) and announces; `10` is then taken by fence or by the drainer's own yield, and the drain
/// ends `HandedOff`. The drainer's gate is stopped: an open of any leaf there is turned away.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_drain_whose_handoff_phase_runs_out_announces_and_counts_the_fence() {
    let (a, b) = balance_cell().await;
    let opened = a
        .state
        .shards
        .open_or_wait("10", Duration::from_secs(5))
        .await;
    let crate::sharddir::OpenOutcome::Ready(engine) = opened else {
        panic!("10 opens on streams-1");
    };
    let held = engine.test_hold_commit().await;
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let close = crate::shard::CloseReq {
        hash: [3; 16],
        generation: Some(1),
        resp: tx,
    };
    engine.try_close(close).unwrap();
    let standing = a.state.fleet.standing();
    let ended = AtomicBool::new(false);
    let drained = async {
        let outcome = drained_within(&a.state, Duration::from_secs(32) + BOUND / 3).await;
        ended.store(true, Ordering::SeqCst);
        outcome
    };
    // Held until the drain announces, or ends without announcing.
    let release = async {
        while !standing.draining() && !ended.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        drop(held);
    };
    let t0 = Instant::now();
    let (outcome, ()) = futures_util::future::join(drained, release).await;
    eprintln!("HANDOFF drain took={:?} outcome={outcome:?}", t0.elapsed());
    assert_eq!(outcome, DrainOutcome::HandedOff);
    assert!(
        t0.elapsed() >= Duration::from_secs(32),
        "the handoff phase ran out first"
    );
    assert_eq!(
        a.state.shards.fenced_ahead(),
        1,
        "10 given up, counted once"
    );
    let reopened = a
        .state
        .shards
        .open_or_wait("00", Duration::from_secs(1))
        .await;
    assert!(
        matches!(
            reopened,
            crate::sharddir::OpenOutcome::Wait {
                code: "shard_closing",
                ..
            }
        ),
        "the drainer opens nothing more"
    );
    stopped([a, b]).await;
}

/// `streams-1`'s drain on `state` with the production budget, ending within `within`, the bound of
/// the test that runs it: a drain still going then fails by assertion.
async fn drained_within(state: &crate::http::AppState, within: Duration) -> DrainOutcome {
    let drained = drain(state, "streams-1", BUDGET);
    tokio::time::timeout(within, drained)
        .await
        .expect("the drain ends within the test's bound")
}

/// Once `successor` holds a leaf the drain handed it, move that leaf back to the unannounced
/// drainer through `successor`'s own handoff, giver first, as its return-home or count balance
/// would (G4); the leaf.
async fn moved_back(successor: &HttpRig) -> &'static str {
    let shards = &successor.state.shards;
    let t0 = Instant::now();
    let leaf = loop {
        if let Some(leaf) = LEAVES.iter().find(|leaf| shards.is_open(leaf)) {
            break *leaf;
        }
        assert!(t0.elapsed() < BOUND, "the drain hands a leaf to streams-2");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let back = crate::fleet::handoff::Change::To("streams-1".to_string());
    let moved = crate::fleet::handoff::hand_off(&successor.state.fleet, leaf, back, false).await;
    assert_eq!(
        moved,
        crate::fleet::handoff::Outcome::Moved,
        "{leaf} moves back"
    );
    leaf
}

/// Hold `leaf`'s committer on `rig` behind an operation it took, for `long`.
async fn held_for(rig: &HttpRig, leaf: &str, long: Duration) {
    let opened = rig
        .state
        .shards
        .open_or_wait(leaf, Duration::from_secs(5))
        .await;
    let crate::sharddir::OpenOutcome::Ready(engine) = opened else {
        panic!("{leaf} opens");
    };
    let held = engine.test_hold_commit().await;
    let (tx, _rx) = tokio::sync::oneshot::channel();
    let close = crate::shard::CloseReq {
        hash: [3; 16],
        generation: Some(1),
        resp: tx,
    };
    engine.try_close(close).unwrap();
    tokio::time::sleep(long).await;
    drop(held);
}

/// **Moves towards an unannounced drainer are handed off again, each leaf at most once** (G4). As
/// the drain DST above, with `10`'s committer held for 6 s so the handoff phase stays open: once
/// `streams-2` holds a leaf the drain handed it, `streams-2` moves it back to the unannounced
/// drainer, giver first, as its return-home or count balance may, and the drain hands it off
/// again. The drain still ends
/// `HandedOff`, no leaf moves to the drainer more than once, none leaves it but through a hold,
/// no writer is fenced and no append ends refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn moves_towards_an_unannounced_drainer_are_handed_off_again() {
    let errors = ErrorLines::install();
    let (a, b) = balance_cell().await;
    let mut taken = 0;
    let mut streams = Vec::new();
    for leaf in LEAVES {
        streams.push(stream_in(&a.state, a.addr, leaf, &mut taken).await);
    }
    // `10`'s committer held for its first 6 s keeps the phase open while a leaf moves back.
    let held = held_for(&a, "10", Duration::from_secs(6));
    let drained = drained_under_appends(&a, &b, &streams);
    let returns = futures_util::future::join(moved_back(&b), held);
    let (((outcome, took), tally, (departed, arrived)), (back, ())) =
        futures_util::future::join(drained, returns).await;
    eprintln!("HANDOFF drain took={took:?} outcome={outcome:?} arrived={arrived:?}");
    eprintln!(
        "HANDOFF drain replayed={} refused={:?}",
        tally.replayed, tally.refused
    );
    errors.report();
    assert_eq!(outcome, DrainOutcome::HandedOff);
    assert_eq!(
        arrived.get(back),
        Some(&1),
        "{back} moved back to the drainer once"
    );
    assert!(
        arrived.values().all(|n| *n <= 1),
        "a leaf moved to the drainer twice: {arrived:?}"
    );
    assert!(
        departed.is_empty(),
        "a leaf left the drainer without a hold: {departed:?}"
    );
    assert_no_fence([&a, &b]);
    assert_eq!(
        tally.refused,
        BTreeMap::new(),
        "no append is refused across the drain"
    );
    stopped([a, b]).await;
}

/// A cell of three members over [`LEAVES`] with no overrides, each started once the one before it
/// beats, and every view naming all three.
async fn three_member_cell() -> [HttpRig; 3] {
    use object_store::{ObjectStoreExt, PutPayload, path::Path};
    let fleet = super::fixture_storage::mem();
    let desired = r#"{"count":3,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    fleet
        .put(
            &Path::from("fleet/desired.json"),
            PutPayload::from_static(desired.as_bytes()),
        )
        .await
        .unwrap();
    let data = super::fixture_storage::mem();
    let mut rigs = Vec::new();
    for (n, name) in ["streams-1", "streams-2", "streams-3"]
        .into_iter()
        .enumerate()
    {
        let runtime = super::fixture_runtime::RigRuntime::incarnation(u64::try_from(n).unwrap());
        rigs.push(balance_member(&fleet, &data, name, runtime).await);
        let beat = Path::from(format!("fleet/{name}.json"));
        let t0 = Instant::now();
        while fleet.head(&beat).await.is_err() {
            assert!(t0.elapsed() < BOUND, "{name} beats");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    let urls = serde_json::Value::Object(urls).to_string();
    fleet
        .put(&Path::from("fleet/urls.json"), PutPayload::from(urls))
        .await
        .unwrap();
    let t0 = Instant::now();
    while rigs
        .iter()
        .any(|rig| rig.state.ownership.ring_active().len() < 3)
    {
        assert!(t0.elapsed() < BOUND, "every view names all three members");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rigs.try_into().unwrap_or_else(|_| unreachable!())
}

/// A leaf the ring of all three gives `streams-1` and the ring without it gives `streams-2`.
fn drained_to_the_second() -> &'static str {
    let all = ["streams-1", "streams-2", "streams-3"].map(String::from);
    let rest = &all[1..];
    for leaf in LEAVES {
        let pick = |members: &[String]| crate::ownership::ring_pick(leaf, members);
        if pick(&all) == 0 && pick(rest) == 0 {
            return leaf;
        }
    }
    panic!("no leaf streams-1 holds and streams-2 inherits")
}

/// **A peer whose view the drain overtook is answered after one replay at most** (G10). A cell of
/// three: `streams-1` drains the leaf it holds to `streams-2` and announces, and `streams-2`
/// prunes the entry the move wrote (a no-op once `streams-1` left the ring; aged past the return
/// window here, as its passing would). `streams-3`'s view is then set back to what it was before
/// the drain, as if its tick had not run since: an append through it, naming `streams-1`, is acked
/// after at most one `409` and one replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_peer_whose_view_the_drain_overtook_is_answered_after_one_replay() {
    let [a, b, c] = three_member_cell().await;
    let leaf = drained_to_the_second();
    let mut taken = 0;
    let stream = stream_in(&a.state, a.addr, leaf, &mut taken).await;
    assert_eq!(
        drained_within(&a.state, BOUND).await,
        DrainOutcome::HandedOff
    );
    aged_past_the_window(&b.state, leaf).await;
    let t0 = Instant::now();
    while a
        .state
        .fleet
        .read_overrides()
        .await
        .unwrap()
        .0
        .entries
        .contains_key(leaf)
    {
        assert!(
            t0.elapsed() < BOUND,
            "streams-2 prunes the entry naming itself"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // streams-3's view as it stood before the drain, as if its tick had not run since: all three
    // members, no entry for the leaf.
    let before = ["streams-1", "streams-2", "streams-3"]
        .map(String::from)
        .to_vec();
    c.state
        .ownership
        .set_view(before, std::collections::HashMap::new());
    let stale = c.state.ownership.effective_owner(leaf);
    assert_eq!(
        stale.as_deref(),
        Some("streams-1"),
        "streams-3 still names streams-1"
    );
    let members = [
        ("streams-1", a.addr),
        ("streams-2", b.addr),
        ("streams-3", c.addr),
    ];
    let (status, by, replays) = appended_through(members, c.addr, &stream).await;
    assert_eq!(
        (status, replays <= 1),
        (204, true),
        "acked by {by} after {replays} replays"
    );
    for rig in [a, b, c] {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// Move `leaf`'s entry back past the return window, through the overrides CAS, as the window's
/// passing would: return-home then prunes it once it is a no-op.
async fn aged_past_the_window(state: &crate::http::AppState, leaf: &str) {
    let window_ms = i64::try_from(state.config.fleet.rebalance_return_secs).unwrap() * 1000;
    let t0 = Instant::now();
    loop {
        let (mut doc, version) = state.fleet.read_overrides().await.unwrap();
        let entry = doc.entries.get_mut(leaf).expect("the move's entry");
        entry.ms -= window_ms + 60_000;
        let raw = serde_json::to_vec(&doc).unwrap();
        let doc = crate::fleet::FleetDocument::Overrides;
        if state.fleet.replace_document(doc, raw, version).await {
            return;
        }
        assert!(t0.elapsed() < BOUND, "the aged document is written");
    }
}

/// One append to `stream` sent to `first`, replaying each `409` to the member it names, as the SDK
/// does, at most three times: its final status, who acked it and how many replays it took.
async fn appended_through(
    members: [(&'static str, std::net::SocketAddr); 3],
    first: std::net::SocketAddr,
    stream: &str,
) -> (u16, String, u32) {
    let path = format!("/v1/stream/{stream}");
    let json = [super::billing_closure_debts::JSON];
    let (mut addr, mut replays) = (first, 0);
    loop {
        let (status, headers, _) =
            super::fixture_requests::hreq(addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
        let owner = headers
            .get("streams-replay-to")
            .cloned()
            .unwrap_or_default();
        let next = members.iter().find(|(name, _)| *name == owner);
        match next {
            Some((_, to)) if status == 409 && replays < 3 => (addr, replays) = (*to, replays + 1),
            _ => {
                return (
                    status,
                    headers
                        .get("streams-forwarded-to")
                        .cloned()
                        .unwrap_or(owner),
                    replays,
                );
            }
        }
    }
}

/// **A drainer killed mid-phase leaves every record exact** (G7, "an interrupted drain"). With
/// `10`'s committer held so the drain cannot finish, `streams-1` is killed once it has handed a
/// leaf to `streams-2`: the handed-off leaves stay with `streams-2` under the entries the moves
/// wrote, the rest are taken over once `streams-1`'s beats go stale, and every stream holds exactly
/// the records acked before the kill and the one appended after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_drainer_killed_mid_phase_leaves_every_record_exact() {
    let (a, b) = balance_cell().await;
    let mut taken = 0;
    let mut streams = Vec::new();
    for leaf in LEAVES {
        let stream = stream_in(&a.state, a.addr, leaf, &mut taken).await;
        for n in 0..3 {
            let path = format!("/v1/stream/{stream}");
            let body = format!(r#"[{{"n":{n}}}]"#);
            let json = [super::billing_closure_debts::JSON];
            let (status, _, _) =
                super::fixture_requests::hreq(a.addr, "POST", &path, &json, body.as_bytes()).await;
            assert_eq!(status, 204, "{stream} {n}");
        }
        streams.push(stream);
    }
    let held = held_for(&a, "10", Duration::from_secs(120));
    let handed = async {
        let t0 = Instant::now();
        while !LEAVES.iter().any(|leaf| b.state.shards.is_open(leaf)) {
            assert!(t0.elapsed() < BOUND, "a leaf is handed off");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let drained = drain(&a.state, "streams-1", BUDGET);
    let killed = futures_util::future::select(
        Box::pin(handed),
        Box::pin(futures_util::future::join(drained, held)),
    )
    .await;
    assert!(
        matches!(killed, futures_util::future::Either::Left(_)),
        "killed mid-phase"
    );
    let moved: Vec<&str> = LEAVES
        .iter()
        .copied()
        .filter(|leaf| b.state.shards.is_open(leaf))
        .collect();
    let report = a.tasks.shutdown(Duration::ZERO).await;
    eprintln!("HANDOFF killed streams-1 after {moved:?}: {report:?}");
    let t0 = Instant::now();
    while b.state.ownership.ring_active() != ["streams-2"] {
        assert!(
            t0.elapsed() < Duration::from_secs(90),
            "streams-1's beats go stale"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let entries = b.state.fleet.read_overrides().await.unwrap().0.entries;
    for leaf in &moved {
        let named = entries.get(*leaf).map(|entry| entry.to.as_str());
        assert!(
            matches!(named, None | Some("streams-2")),
            "{leaf} stays with streams-2: {named:?}"
        );
    }
    for stream in &streams {
        let path = format!("/v1/stream/{stream}");
        let json = [super::billing_closure_debts::JSON];
        let (status, _, body) =
            super::fixture_requests::hreq(b.addr, "POST", &path, &json, br#"[{"n":3}]"#).await;
        assert_eq!(status, 204, "{stream}: {}", String::from_utf8_lossy(&body));
        assert_eq!(
            stored_ns(b.addr, stream).await,
            [0, 1, 2, 3],
            "{stream}: exactly its records"
        );
    }
    let report = b.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&b.state).await;
}

/// Until `drainer` announces its drain: whether a beat of its said `withdrawn` before the first
/// that said `draining` (the beat it publishes, as its peers read it).
async fn withdrawn_before_announced(drainer: &crate::http::AppState) -> Option<String> {
    let me = drainer.ownership.instance().to_string();
    let t0 = Instant::now();
    loop {
        assert!(
            t0.elapsed() < BOUND,
            "a beat of the drainer says draining or withdrawn"
        );
        let beats = drainer.fleet.peek_heartbeat_set().await.unwrap_or_default();
        if let Some(beat) = beats.iter().find(|beat| beat.instance == me) {
            if beat.draining {
                return None;
            }
            if let Some(withdrawn) = &beat.withdrawn {
                return Some(withdrawn.clone());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// **A drain a signal starts hands every shard off before its beat withdraws it** (the
/// ratification's X1, D1). Production's shape: a signal loop requests the stop, and the stop runs
/// the drain before any loop is cancelled. [`balance_cell`]'s `streams-1` holds four leaves under
/// appends at `streams-2`, with `10`'s committer held 6 s so the handoff phase outlasts a beat
/// and a tick. No beat of `streams-1` says `withdrawn` before its first beat says `draining`;
/// no leaf leaves it but through a hold (none yields at its own tick); no writer is fenced; no
/// append ends refused; and the drain the stop ran is recorded `HandedOff`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_signalled_drain_hands_every_shard_off_before_its_beat_withdraws_it() {
    let errors = ErrorLines::install();
    let (a, b) = balance_cell().await;
    let mut taken = 0;
    let mut streams = Vec::new();
    for leaf in LEAVES {
        streams.push(stream_in(&a.state, a.addr, leaf, &mut taken).await);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let tally = Arc::new(Mutex::new(Tally::default()));
    let next = Arc::new(AtomicU32::new(0));
    // Appends enter at streams-2 only, so none meets streams-1's listener as its stop closes it.
    let members: Members = [("streams-2", b.addr), ("streams-1", a.addr)];
    let appenders = streams.iter().map(|stream| {
        appender(
            members,
            stream.clone(),
            0,
            next.clone(),
            (stop.clone(), tally.clone()),
        )
    });
    let held = held_for(&a, "10", Duration::from_secs(6));
    super::fleet_drain::signal_loop(&a);
    let standing = a.state.fleet.standing();
    let announced = async {
        let withdrawn = withdrawn_before_announced(&a.state).await;
        let t0 = Instant::now();
        while !standing.draining() {
            assert!(t0.elapsed() < BOUND, "the drain announces");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.store(true, Ordering::SeqCst);
        withdrawn
    };
    let watched = watch_leaves(&a, &stop);
    let appended = futures_util::future::join_all(appenders);
    let (_, withdrawn, (departed, _), ()) =
        futures_util::future::join4(appended, announced, watched, held).await;
    let t0 = Instant::now();
    while standing.drain_outcome().is_none() {
        assert!(t0.elapsed() < BOUND, "the stop's drain ends");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let tally = std::mem::take(&mut *tally.lock().unwrap());
    eprintln!(
        "HANDOFF signalled drain withdrawn={withdrawn:?} departed={departed:?} refused={:?}",
        tally.refused
    );
    errors.report();
    assert_eq!(
        withdrawn, None,
        "streams-1 withdrew before it announced its drain"
    );
    assert!(
        departed.is_empty(),
        "a leaf left the drainer without a hold: {departed:?}"
    );
    assert_no_fence([&a, &b]);
    assert_eq!(
        tally.refused,
        BTreeMap::new(),
        "no append is refused across the drain"
    );
    assert_eq!(standing.drain_outcome(), Some(DrainOutcome::HandedOff));
    for stream in &streams {
        let mut acked: Vec<u32> = tally.acked[stream].iter().map(|ack| ack.n).collect();
        acked.sort_unstable();
        assert_eq!(
            stored_ns(b.addr, stream).await,
            acked,
            "{stream}: stored = acked"
        );
    }
    stopped([a, b]).await;
}
