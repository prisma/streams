//! Index floors and the roster's lower bound (docs/QUEUES-DESIGN.md §6.8):
//! scans start at each time-ordered index's first live entry, and the
//! roster parks at the earliest deadline the state row's floors allow, so
//! an active queue is swept when something can be due, not every second.
#![cfg(test)]
use super::{HASH, claim, clocked, msg, op, roster, send_op, set_clock, settle, state, swept};
use crate::native_queue::rows::Floors;
use crate::native_queue::{DAY_MS, HOUR_MS, NqOp, QueueConfig, SettleKind, keys};

const RETENTION: i64 = 4 * DAY_MS;
const LEASE: i64 = 30_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_due_retry_waiting_for_a_claim_does_not_delay_an_earlier_lease_expiry() {
    let (e, db, _absorb, c) = clocked("nq-floor-lease-first", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    let d = claim(&e, 2).await;
    let now = SettleKind::Retry { delay_ms: Some(0) };
    op(&e, settle(vec![(d[1].offset, d[1].lease_gen, now)]))
        .await
        .unwrap();
    set_clock(&db, c + 1).await;
    // The retry is due and waits for a claim; the lease of offset 0 expires
    // at c + 30 s, and the sweep must look then, not a minute on.
    assert_eq!(swept(&e).await.0, Some(c + LEASE));
    assert_eq!(roster(&db).await, Some(c + LEASE));
    set_clock(&db, c + LEASE).await;
    swept(&e).await;
    let s = state(&db).await;
    assert_eq!((s.leased, s.retrying), (0, 2), "the lease expired on time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claims_and_acks_leave_a_parked_roster_where_the_sweep_put_it() {
    let (e, db, _absorb, c) = clocked("nq-floor-quiet", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    claim(&e, 1).await;
    assert_eq!(swept(&e).await.0, Some(c + LEASE));
    for i in 1..3_i64 {
        set_clock(&db, c + i * 1_000).await;
        let d = claim(&e, 1).await;
        op(
            &e,
            settle(vec![(d[0].offset, d[0].lease_gen, SettleKind::Ack)]),
        )
        .await
        .unwrap();
        assert_eq!(
            roster(&db).await,
            Some(c + LEASE),
            "a later lease and its ack leave the roster alone"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn floors_fall_with_every_put_and_rise_with_the_scans_that_find_the_heads() {
    let (e, db, _absorb, c) = clocked("nq-floor-exact", QueueConfig::default()).await;
    assert_eq!(state(&db).await.floors, Floors::EMPTY, "a new queue");
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    assert_eq!(state(&db).await.floors.avail, c);
    let d = claim(&e, 1).await;
    assert_eq!(state(&db).await.floors.leases, c + LEASE);
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    assert_eq!(
        state(&db).await.floors.leases,
        c + LEASE,
        "a delete leaves the floor low"
    );
    set_clock(&db, c + 1).await;
    assert_eq!(swept(&e).await.0, Some(c + RETENTION), "offset 1 untaken");
    let f = state(&db).await.floors;
    assert_eq!(
        (f.leases, f.avail),
        (i64::MAX, c),
        "the sweep raised the lease floor past the acked lease"
    );
    let retry = SettleKind::Retry {
        delay_ms: Some(HOUR_MS),
    };
    let d = claim(&e, 1).await;
    op(&e, settle(vec![(1, d[0].lease_gen, retry)]))
        .await
        .unwrap();
    let f = state(&db).await.floors;
    assert_eq!(
        (f.retries, f.retry_cycle),
        (c + 1 + HOUR_MS, c + RETENTION),
        "a retry lowers its due floor and its cycle floor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_retry_is_swept_only_at_its_cycle_deadline() {
    let (e, db, _absorb, c) = clocked("nq-floor-cycle", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim(&e, 1).await;
    let retry = SettleKind::Retry {
        delay_ms: Some(HOUR_MS),
    };
    op(&e, settle(vec![(0, d[0].lease_gen, retry)]))
        .await
        .unwrap();
    set_clock(&db, c + 2 * HOUR_MS).await;
    // Due since an hour and unclaimed: only a claim moves it, so the sweep
    // parks at its cycle deadline (its retention end).
    assert_eq!(swept(&e).await.0, Some(c + RETENTION));
    assert_eq!(state(&db).await.retrying, 1);
    set_clock(&db, c + RETENTION - 1).await;
    swept(&e).await;
    assert_eq!(state(&db).await.failed, 0, "a millisecond early: kept");
    set_clock(&db, c + RETENTION).await;
    swept(&e).await;
    let s = state(&db).await;
    assert_eq!(
        (s.retrying, s.failed, s.floors.retry_cycle),
        (0, 1, i64::MAX)
    );
    assert!(
        db.get(keys::roster(&HASH)).await.unwrap().is_some(),
        "the failed message keeps the queue on the roster"
    );
    op(&e, NqOp::Get).await.unwrap();
}
