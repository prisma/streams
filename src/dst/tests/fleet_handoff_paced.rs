#![cfg(test)]
//! **A planned move's held waiters wait for a forward place** (Addendum 18 of
//! GATE-DECISION-s22-join-2026-11-20, an amendment to edge #266's E7). A pile larger than the
//! giver's forward places parks on a planned move's hold: the move's write of the overrides is held
//! while 300 appends to ten streams of `00` arrive at the giver, `streams-1`, on a cell whose
//! in-flight cap is 512 (128 forward places). Released by the move, every one of them is owned by
//! the target, `streams-2`, and must be forwarded there. Before the amendment the 128 that found a
//! place were forwarded and the rest answered `409 not_stream_owner` at once; now each waits, in
//! order, for a place a finished forward frees, at most a second after its release
//! (`admission::forwards`). A second case holds the target's answers past that second, so the
//! waiters past it are answered `409` as before, none hangs, and none is acked twice.

use super::billing_closure_debts::JSON;
use super::fixture_http::HttpRig;
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::fleet_handoff::stored_ns;
use super::fleet_handoff_cases::{engaged, member, stop, stream_in_00_as};
use crate::dst::fault_store::{FaultPlan, FaultStore};
use crate::dst::{ObjClass, StoreOp};
use crate::fleet::handoff::{Change, Outcome, hand_off};
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

/// The appends piled on the hold: more than the giver's 128 forward places.
const PILE: usize = 300;
/// The streams of `00` they append to.
const STREAMS: usize = 10;
/// The cell's in-flight cap: a quarter of it are the forward places.
const CAP: i64 = 512;
/// How long the move's write of the overrides may be held before its bound (400 ms) ends the move
/// kept: the pile must be parked on the hold within it.
const PARKED_WITHIN: Duration = Duration::from_millis(300);

/// A ring of `streams-1`, holding `00` open, and `streams-2`, each naming the other as its peer,
/// on one data store and one fleet store, both fault stores; the in-flight cap `CAP` on both.
struct Ring {
    giver: HttpRig,
    target: HttpRig,
    fleet: Arc<FaultStore>,
}

async fn ring() -> Ring {
    let data: Arc<dyn ObjectStore> = FaultStore::uniform(mem(), 0xA18D, FaultPlan::CLEAN);
    let fleet = FaultStore::uniform(mem(), 0xA18F, FaultPlan::CLEAN);
    let giver = member(&data, fleet.clone(), "streams-1", RigRuntime::first(), true).await;
    let target = member(
        &data,
        fleet.clone(),
        "streams-2",
        RigRuntime::incarnation(1),
        false,
    )
    .await;
    giver
        .state
        .peer
        .set_peer("streams-2", &format!("http://{}", target.addr));
    target
        .state
        .peer
        .set_peer("streams-1", &format!("http://{}", giver.addr));
    for rig in [&giver, &target] {
        rig.state.admission.set_max_inflight(CAP);
    }
    Ring {
        giver,
        target,
        fleet,
    }
}

/// One append of the pile: its number, the stream it went to, its status, and when it was answered.
type Answered = (u32, String, u16, Instant);

/// The move of `00` to `streams-2`, its overrides write held until the pile of `PILE` appends to
/// `streams` has parked on the hold (or `PARKED_WITHIN` passed), then released; `armed` runs once
/// the write is held, before the pile. The move's end, the appends' answers in order, the giver's
/// forwards waiting at their peak, and the time of the release.
async fn held_through_a_move(
    ring: &Ring,
    streams: &[String],
    armed: impl FnOnce(),
) -> (Outcome, Vec<Answered>, i64, Instant) {
    let overrides =
        ring.fleet
            .hold_class_under(StoreOp::Put, ObjClass::Fleet, "fleet/overrides", 1);
    let moved = hand_off(
        &ring.giver.state.fleet,
        "00",
        Change::To("streams-2".to_string()),
        false,
    );
    let (done, peak) = (AtomicBool::new(false), AtomicI64::new(0));
    let released = std::sync::Mutex::new(None);
    let pile = async {
        engaged(&overrides).await;
        armed();
        let addr = ring.giver.addr;
        let sends = (0..PILE).map(|k| {
            let name = streams[k % STREAMS].clone();
            async move {
                let n = u32::try_from(k).unwrap();
                let body = format!(r#"[{{"n":{n}}}]"#);
                let path = format!("/v1/stream/{name}");
                let (status, _, _) = hreq(addr, "POST", &path, &[JSON], body.as_bytes()).await;
                (n, name, status, Instant::now())
            }
        });
        let answers = futures_util::future::join_all(sends).await;
        done.store(true, Ordering::Relaxed);
        answers
    };
    let control = async {
        engaged(&overrides).await;
        let held = Instant::now();
        let admission = &ring.giver.state.admission;
        let whole = i64::try_from(PILE).unwrap();
        while admission.waiting_places() < whole && held.elapsed() < PARKED_WITHIN {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let parked = admission.waiting_places();
        ring.fleet.release_hold();
        *released.lock().unwrap() = Some(Instant::now());
        while !done.load(Ordering::Relaxed) {
            peak.fetch_max(admission.forwards_waiting(), Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        parked
    };
    let (moved, answers, parked) = futures_util::future::join3(moved, pile, control).await;
    assert!(
        parked >= i64::try_from(PILE).unwrap(),
        "the pile parks on the hold before the overrides' bound: {parked} waiting on it"
    );
    let at = released.lock().unwrap().expect("the write was released");
    (moved, answers, peak.load(Ordering::Relaxed), at)
}

/// `answers` by status.
fn by_status(answers: &[Answered]) -> BTreeMap<u16, usize> {
    let mut by = BTreeMap::new();
    for (_, _, status, _) in answers {
        *by.entry(*status).or_insert(0) += 1;
    }
    by
}

/// When the pile was answered after the release at `at`, for a failure's message: the first
/// answer, the last `204` and the last answer, in ms.
fn drained(answers: &[Answered], at: Instant) -> String {
    let ms = |answer: &Answered| answer.3.saturating_duration_since(at).as_millis();
    let first = answers.iter().map(ms).min();
    let last_acked = answers
        .iter()
        .filter(|answer| answer.2 == 204)
        .map(ms)
        .max();
    let last = answers.iter().map(ms).max();
    format!(
        "answered after the release: first {first:?}, last 204 {last_acked:?}, last {last:?} ms"
    )
}

/// Each stream of `streams` holds exactly the appends `answers` acked to it, each once.
async fn stored_as_acked(ring: &Ring, streams: &[String], answers: &[Answered]) {
    for name in streams {
        let mut acked: Vec<u32> = answers
            .iter()
            .filter(|(_, stream, status, _)| stream == name && *status == 204)
            .map(|(n, _, _, _)| *n)
            .collect();
        acked.sort_unstable();
        let mut stored = stored_ns(ring.target.addr, name).await;
        stored.sort_unstable();
        assert_eq!(stored, acked, "{name}: stored equals acked, each once");
    }
}

/// The ten streams of `00` the pile appends to, under this file's own stems (the failpoints are
/// process-wide).
async fn streams_in_00(giver: &HttpRig, stem: &str) -> Vec<String> {
    let mut names = Vec::with_capacity(STREAMS);
    for k in 0..STREAMS {
        names.push(stream_in_00_as(giver, &format!("{stem}{k}")).await);
    }
    names
}

/// **A planned move's held waiters are forwarded, none answered `409` for want of a place.** A
/// pile of 300 appends, more than the giver's 128 forward places, parks on the move's hold; the
/// move ends moved. Asserted, by counts: no held waiter is answered `409`; every append is acked
/// (`204`) and stored once at the target, so the target's write gate shed nothing; the giver's
/// forwards waiting at once never pass 128. Red before the amendment: the waiters that found no
/// place were answered `409` at the release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_moves_held_waiters_wait_for_a_forward_place() {
    let ring = ring().await;
    let streams = streams_in_00(&ring.giver, "paced-w").await;
    let (moved, answers, peak, released) = held_through_a_move(&ring, &streams, || {}).await;
    eprintln!("PACED_DRAIN {}", drained(&answers, released));
    assert_eq!(moved, Outcome::Moved);
    assert_eq!(
        by_status(&answers),
        BTreeMap::from([(204, PILE)]),
        "no held waiter is answered 409: every append of the pile acked; {}",
        drained(&answers, released)
    );
    assert!(
        peak <= CAP / 4,
        "the giver's forwards waiting at once: {peak}"
    );
    stored_as_acked(&ring, &streams, &answers).await;
    stop(ring.target).await;
    stop(ring.giver).await;
}

/// **Held waiters past their second are answered `409`, none hangs, none is acked twice.** The
/// target's appends of the pile's streams are parked after their resolve for 1.5 s, so the 128
/// forwards that found places hold them past the waiters' bound. Asserted: every append is
/// answered within 5 s of the release, `204` or `409`; at most 128 are acked and the rest refused;
/// the giver's forwards waiting at once never pass 128; and each stream holds exactly its acked
/// appends, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn held_waiters_past_their_bound_are_answered_409_and_none_twice() {
    let ring = ring().await;
    let streams = streams_in_00(&ring.giver, "paced-b").await;
    let arm = || {
        for name in &streams {
            crate::failpoints::park_append_after_resolve(name);
        }
    };
    let unpark = async {
        tokio::time::sleep(Duration::from_millis(1_800)).await;
        for name in &streams {
            crate::failpoints::release_append_after_resolve(name);
        }
    };
    let ((moved, answers, peak, released), ()) =
        futures_util::future::join(held_through_a_move(&ring, &streams, arm), unpark).await;
    let answered = released.elapsed();
    assert_eq!(moved, Outcome::Moved);
    let by = by_status(&answers);
    let acked = by.get(&204).copied().unwrap_or(0);
    let refused = by.get(&409).copied().unwrap_or(0);
    assert_eq!(
        acked + refused,
        PILE,
        "every append answered 204 or 409: {by:?}"
    );
    assert!(acked <= usize::try_from(CAP / 4).unwrap(), "{by:?}");
    assert!(
        refused >= PILE - usize::try_from(CAP / 4).unwrap(),
        "{by:?}"
    );
    assert!(
        answered < Duration::from_secs(5),
        "none hangs: {answered:?}"
    );
    assert!(
        peak <= CAP / 4,
        "the giver's forwards waiting at once: {peak}"
    );
    stored_as_acked(&ring, &streams, &answers).await;
    stop(ring.target).await;
    stop(ring.giver).await;
}
