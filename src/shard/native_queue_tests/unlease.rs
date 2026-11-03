//! What the server does when a leased message cannot reach its consumer
//! (S3b-DESIGN §6.5, review F1): an envelope that will not open is a
//! property of the message, which fails alone as `unreadable`; a transient
//! failure (a refused read hold, a reply that could not be sent) gives the
//! lease back without spending the customer's attempts or releases, at
//! most eight times a delivery, so no page can hold a queue's claims
//! forever.
#![cfg(test)]
use super::{HASH, claim, clocked, msg, op, send_op, settle, state};
use crate::native_queue::rows::{Cause, DState, DeliveryRow};
use crate::native_queue::{INTERNAL_UNLEASES, NqOut, QueueConfig, SettleKind, SettleOutcome, keys};

async fn delivery(db: &slatedb::Db, off: u64) -> DeliveryRow {
    let raw = db
        .get(keys::by_offset(&HASH, keys::DELIVERY, off))
        .await
        .unwrap()
        .unwrap();
    DeliveryRow::decode(&raw).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_internal_unlease_gives_the_lease_back_and_spends_no_budget() {
    let (e, db, c) = clocked("nq-unlease", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim(&e, 1).await;
    let NqOut::Settled(r) = op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Unlease)]))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(r[0].outcome, SettleOutcome::Ok);
    let row = delivery(&db, 0).await;
    assert_eq!(
        (row.state, row.attempts, row.releases, row.internal_unleases, row.due_at),
        (DState::Retrying, 0, 0, 1, c),
        "due again at once, its attempt given back, its releases untouched"
    );
    let again = claim(&e, 1).await;
    assert_eq!((again[0].offset, again[0].attempt), (0, 1), "attempt 1 again");
    let s = state(&db).await;
    assert_eq!(
        (s.leased, s.retrying, s.counters.retried, s.counters.internal_unleases),
        (1, 0, 0, 1),
        "not a retry the customer made"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_eighth_internal_unlease_fails_the_delivery_as_unreadable() {
    let (e, db, _c) = clocked("nq-unlease-limit", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    for n in 1..INTERNAL_UNLEASES {
        let d = claim(&e, 1).await;
        op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Unlease)]))
            .await
            .unwrap();
        assert_eq!(delivery(&db, 0).await.internal_unleases, n);
    }
    let d = claim(&e, 1).await;
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Unlease)]))
        .await
        .unwrap();
    let row = delivery(&db, 0).await;
    assert_eq!(
        (row.state, row.cause, row.code.as_deref(), row.attempts),
        (
            DState::Failed,
            Some(Cause::Unreadable),
            Some("internal_unlease_limit"),
            1
        )
    );
    assert!(claim(&e, 1).await.is_empty(), "the queue is not held by it");
    let s = state(&db).await;
    assert_eq!((s.failed, s.queued_messages, s.counters.unreadable), (1, 0, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_unreadable_settle_fails_the_delivery_alone_and_spends_no_budget() {
    let (e, db, _c) = clocked("nq-unreadable", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    let d = claim(&e, 2).await;
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Unreadable)]))
        .await
        .unwrap();
    let row = delivery(&db, 0).await;
    assert_eq!(
        (row.state, row.cause, row.code.as_deref(), row.attempts, row.releases),
        (DState::Failed, Some(Cause::Unreadable), Some("unreadable"), 1, 0)
    );
    assert_eq!(delivery(&db, 1).await.state, DState::Leased, "the other stays");
    let s = state(&db).await;
    assert_eq!(
        (s.failed, s.leased, s.counters.unreadable, s.counters.failed),
        (1, 1, 1, 1)
    );
}
