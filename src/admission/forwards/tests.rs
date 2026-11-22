#![cfg(test)]
//! The forward places (edge change #235) and a planned move's held waiters' wait for one
//! (Addendum 18 of GATE-DECISION-s22-join-2026-11-20).

use super::{AdmissionController, ForwardTicket, RELEASE_WAIT, ReleaseTally};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

fn controller(max_inflight: i64) -> AdmissionController {
    AdmissionController::new(crate::admission::AdmissionKnobs {
        max_inflight,
        per_stream_cap: 0,
        rss_shed_mb: 0,
        project_memory_pressure_bytes: 0,
        project_memory_release_pct: 0,
        subscriptions: crate::admission::SubscriptionCapacity {
            effective: 0,
            configured: 0,
        },
        record_ceiling_bytes: 0,
    })
}

/// How many forwards one controller lets wait at once.
fn places(ctl: &AdmissionController) -> Vec<ForwardTicket> {
    std::iter::from_fn(|| ctl.enter_forward())
        .take(1_000)
        .collect()
}

/// A quarter of the in-flight cap, at least one, and 128 with no cap;
/// a place is free again once its forward has its answer.
#[test]
fn forwards_wait_within_a_quarter_of_the_in_flight_cap() {
    let counts = [512, 8, 7, 3, 1, 0].map(|cap| places(&controller(cap)).len());
    assert_eq!(counts, [128, 2, 1, 1, 1, 128]);
    let ctl = controller(8);
    let mut held = places(&ctl);
    assert!(ctl.enter_forward().is_none());
    held.pop();
    assert!(ctl.enter_forward().is_some(), "a returned place is free");
    assert_eq!(ctl.forwards_waiting(), 1);
}

/// What a served request waited on before its forward.
#[derive(Clone, Copy)]
enum Waited {
    /// Nothing.
    Nothing,
    /// An open (`park::waits_on_owner`), not a move.
    Open,
    /// The planned move holding the prefix.
    Move(&'static str),
}

/// A served request on `ctl` that waited as `waited`, then asks for a forward place: whether it
/// got one, and how long the ask took.
async fn forwarded(ctl: &AdmissionController, waited: Waited) -> (bool, Duration) {
    let took = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&took);
    let asked = ctl.clone();
    let handler = async move {
        match waited {
            Waited::Nothing => {}
            Waited::Open => crate::admission::park::waits_on_owner(),
            Waited::Move(prefix) => crate::admission::park::waits_on_move(prefix),
        }
        let began = Instant::now();
        let place = asked.forward_place().await;
        *seen.lock().unwrap() = Some((place.is_some(), began.elapsed()));
        axum::response::Response::new(axum::body::Body::empty())
    };
    ctl.enter(true).serve(false, handler).await;
    let answered = took.lock().unwrap().take();
    answered.expect("the handler ran")
}

/// The counts `tally`'s line will report, once its last holder drops it.
fn reported(tally: &ReleaseTally) -> Arc<Mutex<Option<[u64; 5]>>> {
    Arc::clone(&tally.reported)
}

/// The live tally of the move that held `prefix`, if any waiter of it needed a forward.
fn tally_of(ctl: &AdmissionController, prefix: &str) -> Option<Arc<ReleaseTally>> {
    let tallies = ctl.inner.forwards.tallies.lock().unwrap();
    tallies.get(prefix).and_then(std::sync::Weak::upgrade)
}

/// Freed places go to the held waiters in the order they waited, never one place more than the
/// cap: with both places of a cap of 8 held, two waiters each get the place the next finished
/// forward hands over, the first waiter the first; a third, never handed one, is refused at its
/// bound and leaves the count where the places put it.
#[tokio::test(crate = "crate::test_rt")]
async fn freed_places_go_to_the_waiters_in_order_within_the_cap() {
    let ctl = controller(8);
    let mut held = places(&ctl);
    let tally = ReleaseTally::new("00");
    let order = Mutex::new(Vec::new());
    let wait = |n: u32| {
        let (ctl, order, tally) = (&ctl, &order, &tally);
        async move {
            let place = ctl.waited_place(tally).await;
            order.lock().unwrap().push((n, place.is_some()));
            place
        }
    };
    let hand_over = async {
        tokio::task::yield_now().await;
        assert_eq!(
            ctl.forwards_waiting(),
            2,
            "the waiters take no place of their own"
        );
        drop(held.pop());
        tokio::task::yield_now().await;
        drop(held.pop());
    };
    let (first, second, third, ()) =
        futures_util::future::join4(wait(1), wait(2), wait(3), hand_over).await;
    assert!(first.is_some() && second.is_some() && third.is_none());
    assert_eq!(*order.lock().unwrap(), [(1, true), (2, true), (3, false)]);
    assert_eq!(
        ctl.forwards_waiting(),
        2,
        "two places, handed over, never three"
    );
    drop((first, second));
    assert_eq!(ctl.forwards_waiting(), 0, "every place is returned");
    let [released, placed, waited, refused, longest] = tally.counts();
    assert_eq!([released, placed, waited, refused], [0, 0, 3, 0]);
    assert!(
        longest >= 1_000,
        "the third waited out its bound: {longest}"
    );
}

/// Only a request that waited on a planned move's hold, holding a waiting place, waits for a
/// place, and at most `RELEASE_WAIT`: with the one place of a cap of 4 held, a request that waited
/// on nothing and one that waited on an open are refused at once; a move's waiter is placed when
/// the place frees; a second, never handed one, is refused at its bound. The first move's tally
/// reports its waiter, placed after its wait, and the second move's waiter prunes it from the map.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn only_a_moves_waiter_waits_for_a_place_and_within_its_bound() {
    let ctl = controller(4);
    let held = ctl.enter_forward().expect("the one place");
    let (nothing, open) = futures_util::future::join(
        forwarded(&ctl, Waited::Nothing),
        forwarded(&ctl, Waited::Open),
    )
    .await;
    for (what, (placed, took)) in [("nothing", nothing), ("an open", open)] {
        assert!(
            !placed && took < Duration::from_millis(100),
            "{what}: {took:?}"
        );
    }
    assert!(
        tally_of(&ctl, "00").is_none(),
        "no waiter of a move asked yet"
    );
    let freed = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let line = tally_of(&ctl, "00").map(|tally| reported(&tally));
        drop(held);
        line
    };
    let ((placed, took), line) =
        futures_util::future::join(forwarded(&ctl, Waited::Move("00")), freed).await;
    assert!(placed && took >= Duration::from_millis(90), "{took:?}");
    assert!(
        tally_of(&ctl, "00").is_none(),
        "the tally goes with its last waiter"
    );
    let line = line.expect("the waiting request made the move's tally");
    assert_eq!(
        line.lock().unwrap().map(|c| c[..4].to_vec()),
        Some(vec![1, 1, 1, 0])
    );
    let _again = ctl.enter_forward().expect("the place, free again");
    let (placed, took) = forwarded(&ctl, Waited::Move("01")).await;
    assert!(!placed, "{took:?}");
    assert!(
        took >= RELEASE_WAIT && took < RELEASE_WAIT + Duration::from_millis(500),
        "{took:?}"
    );
    let kept: Vec<String> = ctl
        .inner
        .forwards
        .tallies
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        kept,
        ["01"],
        "the next move's waiter prunes the gone move's tally"
    );
}

/// **A held waiter whose client goes away still counts, and the last to leave writes the line**
/// (Addendum 18's tally): two waiters of one move wait for the one place of a cap of 4; the first
/// one's client goes away before it is placed; the place frees and goes to the second. The line is
/// written when the second, the last to leave, is answered: two released, one placed, both waits,
/// none refused, the longest wait at least the first one's 100 ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_waiter_that_goes_away_still_counts_and_the_last_to_leave_writes_the_line() {
    let ctl = controller(4);
    let held = ctl.enter_forward().expect("the one place");
    let gone = tokio::time::timeout(
        Duration::from_millis(100),
        forwarded(&ctl, Waited::Move("01")),
    );
    let stays = forwarded(&ctl, Waited::Move("01"));
    let free = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let line = tally_of(&ctl, "01").map(|tally| reported(&tally));
        assert!(
            line.as_ref()
                .is_some_and(|line| line.lock().unwrap().is_none()),
            "the waiter still waiting keeps the line unwritten"
        );
        drop(held);
        line
    };
    let (gone, (placed, _), line) = futures_util::future::join3(gone, stays, free).await;
    assert!(gone.is_err(), "the first waiter's client went away");
    assert!(placed, "the second waiter took the freed place");
    let line = line.expect("the move's tally");
    let counts = line
        .lock()
        .unwrap()
        .expect("the last to leave wrote the line");
    assert_eq!(counts[..4], [2, 1, 2, 0], "{counts:?}");
    assert!(counts[4] >= 100, "{counts:?}");
}

/// **A placed waiter keeps the move's tally until it is answered**, so the waiters of one hold write
/// one line: the first of a move's waiters is placed at once and holds its answer; the tally lives
/// on after its place was given, a second waiter of the move joins it, and the line, written once
/// both are answered, reports both released and both placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_placed_waiter_keeps_the_moves_tally_until_it_is_answered() {
    let ctl = controller(4);
    let (answer, answered) = tokio::sync::oneshot::channel::<()>();
    let (placed, was_placed) = tokio::sync::oneshot::channel::<bool>();
    let (asked, serving) = (ctl.clone(), ctl.clone());
    let first = async move {
        let handler = async move {
            crate::admission::park::waits_on_move("02");
            let place = asked.forward_place().await;
            placed.send(place.is_some()).unwrap();
            drop(place);
            drop(answered.await);
            axum::response::Response::new(axum::body::Body::empty())
        };
        serving.enter(true).serve(false, handler).await;
    };
    let ctl = &ctl;
    let then = async {
        assert!(was_placed.await.unwrap(), "the first is placed at once");
        let line = reported(&tally_of(ctl, "02").expect("the first, unanswered, keeps the tally"));
        let (second, _) = forwarded(ctl, Waited::Move("02")).await;
        assert!(second, "the second is placed");
        assert!(
            line.lock().unwrap().is_none(),
            "the first is not answered yet"
        );
        answer.send(()).unwrap();
        line
    };
    let ((), line) = futures_util::future::join(first, then).await;
    let counts = line
        .lock()
        .unwrap()
        .expect("the last to leave wrote the line");
    assert_eq!(counts[..4], [2, 2, 0, 0], "{counts:?}");
}

/// **A hold with no waiter to forward writes no line**: a request waited on the move holding `10`
/// and was answered without a forward (the move was kept, so it is served here); no tally exists,
/// so no line is written.
#[tokio::test(crate = "crate::test_rt")]
async fn a_hold_with_no_waiter_to_forward_writes_no_line() {
    let ctl = controller(4);
    let handler = async {
        crate::admission::park::waits_on_move("10");
        axum::response::Response::new(axum::body::Body::empty())
    };
    ctl.enter(true).serve(false, handler).await;
    assert!(
        ctl.inner.forwards.tallies.lock().unwrap().is_empty(),
        "no tally, so no line"
    );
}
