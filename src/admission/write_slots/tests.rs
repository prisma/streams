use super::Slot;
use crate::admission::AdmissionController;

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

/// At a line of 4 beside 2 requests counted from their arrival, gated requests join the count at
/// the gate until it holds 4 and every later one is refused and takes nothing: 6 meet it, 2 pass,
/// 4 are refused. A refused request holds nothing; an answered one returns its place.
#[test]
fn the_gate_takes_a_place_in_one_step_with_its_check() {
    let c = controller(4);
    let counted: Vec<_> = (0..2).map(|_| Slot::new(&c, false)).collect();
    let gated: Vec<_> = (0..6).map(|_| Slot::new(&c, true)).collect();
    assert_eq!(
        c.judged(),
        2,
        "a gated request is not counted before its gate"
    );
    let passed: Vec<bool> = gated.iter().map(|slot| slot.meet_gate(4)).collect();
    assert_eq!(passed, [true, true, false, false, false, false]);
    assert_eq!(c.judged(), 4);
    assert!(
        gated[0].meet_gate(4),
        "a counted request is judged where it stands"
    );
    assert_eq!(c.judged(), 4, "and is not counted twice");
    drop(gated);
    assert_eq!(c.judged(), 2, "the passed ones return their places");
    drop(counted);
    assert_eq!(c.judged(), 0);
}

/// A request holding a place among those waiting on a move or an open is out of the judged
/// count, whether the count held it before it waited or takes it at its gate afterwards; it
/// returns both when it is answered.
#[test]
fn a_waiting_request_is_out_of_the_judged_count_until_it_is_answered() {
    let c = controller(4);
    let before = Slot::new(&c, false);
    before.waits();
    assert_eq!(c.judged(), 0, "counted, then waiting");
    let after = Slot::new(&c, true);
    after.waits();
    assert!(
        after.meet_gate(0),
        "waiting, then counted at its gate: judged 0 of 0"
    );
    assert_eq!(c.judged(), 0);
    drop((before, after));
    assert_eq!(c.judged(), 0);
    assert_eq!(
        c.inner
            .judged
            .waiting
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// A gated request forwarded to its stream's owner is counted from the forward, beside the
/// requests the gate then judges: with 3 forwards at a line of 4, one gated request passes.
#[test]
fn a_forwarded_write_is_counted_from_its_forward() {
    let c = controller(4);
    let forwarded: Vec<_> = (0..3).map(|_| Slot::new(&c, true)).collect();
    forwarded.iter().for_each(Slot::leaves_by_another_door);
    forwarded[0].leaves_by_another_door();
    assert_eq!(c.judged(), 3, "each forward counts once");
    let gated: Vec<_> = (0..3).map(|_| Slot::new(&c, true)).collect();
    let passed = gated.iter().filter(|slot| slot.meet_gate(4)).count();
    assert_eq!(passed, 1);
    drop((forwarded, gated));
    assert_eq!(c.judged(), 0);
}

/// Through the served request: a gated request meets the write gate at the cap, beside the
/// requests counted from their arrival, and is refused past it as the shed counts; outside a
/// served request the gate only judges.
#[tokio::test(crate = "crate::test_rt")]
async fn a_served_gated_request_meets_the_gate_at_the_cap() {
    let c = controller(2);
    c.add_inflight_for_test(1);
    let first = c.enter(true).serve(true, async {
        assert_eq!(c.admit_write_inflight(), Ok(()), "1 counted, then 2 of 2");
        axum::response::IntoResponse::into_response(())
    });
    drop(first.await);
    let refused = c.enter(true).serve(true, async {
        c.add_inflight_for_test(1);
        assert!(
            c.admit_write_inflight().is_err(),
            "3 of 2: refused, nothing taken"
        );
        assert_eq!(c.judged(), 2);
        axum::response::IntoResponse::into_response(())
    });
    drop(refused.await);
    assert_eq!(c.snapshot().shed.inflight, 1);
    assert!(
        c.admit_write_inflight().is_ok(),
        "outside a served request: judged 2 of 2"
    );
}

/// Fair shedding's check of the write gate's count (`judged_over_cap`) counts the served request
/// itself only when the count does not hold it yet: at a cap of 2, one request counted beside a
/// served read (counted from its arrival) is at the cap, not over it; beside a gated write before
/// its gate it is too (the write adds itself); one more counted beside either is over.
#[tokio::test(crate = "crate::test_rt")]
async fn the_blamed_check_counts_the_request_itself_only_when_the_count_does_not_hold_it() {
    let c = controller(2);
    c.add_inflight_for_test(1);
    let read = c.enter(true).serve(false, async {
        assert_eq!(
            c.judged_over_cap(),
            None,
            "a counted read at the cap: 2 of 2"
        );
        c.add_inflight_for_test(1);
        assert_eq!(c.judged_over_cap(), Some((3, 2)), "one more: 3 of 2");
        c.add_inflight_for_test(-1);
        axum::response::IntoResponse::into_response(())
    });
    drop(read.await);
    let write = c.enter(true).serve(true, async {
        assert_eq!(
            c.judged_over_cap(),
            None,
            "a gated write before its gate: 1 + itself of 2"
        );
        c.add_inflight_for_test(1);
        assert_eq!(
            c.judged_over_cap(),
            Some((3, 2)),
            "one more: 2 + itself of 2"
        );
        c.add_inflight_for_test(-1);
        axum::response::IntoResponse::into_response(())
    });
    drop(write.await);
}

/// A served gated request that begins a forward (`park::forwarding`) is counted from then, once,
/// and leaves the count when it is answered.
#[tokio::test(crate = "crate::test_rt")]
async fn a_served_request_that_begins_a_forward_is_counted_until_it_is_answered() {
    let c = controller(4);
    let forwarded = c.enter(true).serve(true, async {
        assert_eq!(c.judged(), 0, "a gated write before its gate");
        crate::admission::park::forwarding();
        crate::admission::park::forwarding();
        assert_eq!(c.judged(), 1, "counted from its forward, once");
        axum::response::IntoResponse::into_response(())
    });
    drop(forwarded.await);
    assert_eq!(c.judged(), 0, "answered: out of the count");
}
