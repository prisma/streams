//! An operator's reads and actions on a queue (docs/QUEUES-DESIGN.md §6.7, §8.2) where they meet
//! the lane: peek and message read an untaken message from its page, a cancelled one is neither
//! listed nor read nor cancelled a second time, and a delete step records the pages it deletes as
//! collection debt.
#![cfg(test)]
use super::{HASH, claim, clocked, msg, op, send_op, state, stored};
use crate::native_queue::{DeliveryState, NqOp, NqOut, NqRefusal, QueueConfig, keys};

/// Four messages sent at the queue's clock: 0 is leased, so the frontier is at 1; 2 is cancelled
/// untaken, then cancelled again. A peek lists 0 by its delivery and 1 and 3 from the lane, and a
/// message read answers 3 from its page and 2 as reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_lanes_untaken_messages_are_peeked_and_read_and_a_cancelled_one_is_cancelled_once() {
    let (e, db, c) = clocked("nq-admin-lane", QueueConfig::default()).await;
    let sent = vec![msg("a"), msg("b"), msg("c"), msg("d")];
    op(&e, send_op(sent)).await.unwrap();
    let taken = claim(&e, 1).await;
    assert_eq!(taken.iter().map(|v| v.offset).collect::<Vec<_>>(), [0]);
    let cancel = || NqOp::Cancel { offsets: vec![2] };
    assert_eq!(op(&e, cancel()).await.unwrap().count(), 1);
    let again = op(&e, cancel()).await.unwrap().count();
    assert_eq!(again, 0, "a cancelled message is cancelled once");
    let s = state(&db).await;
    let counts = (s.frontier, s.queued_messages, s.counters.cancelled);
    assert_eq!(counts, (1, 3, 1));
    let peek = NqOp::Peek {
        after: None,
        limit: 10,
    };
    let NqOut::Deliveries { items, next } = op(&e, peek).await.unwrap() else {
        panic!("a peek answers deliveries")
    };
    let listed: Vec<_> = items.iter().map(|v| (v.offset, v.state)).collect();
    let (leased, pending) = (DeliveryState::Leased, DeliveryState::Pending);
    assert_eq!(listed, [(0, leased), (1, pending), (3, pending)]);
    assert_eq!(next, None, "a page short of its limit has no cursor");
    let three = (3, stored(&db, 3).await, c);
    let last = &items[2];
    assert_eq!(
        (last.offset, u64::from(last.bytes), last.available_at),
        three
    );
    let NqOut::Message(v) = op(&e, NqOp::Message { offset: 3 }).await.unwrap() else {
        panic!("an untaken message is read from its page")
    };
    let read = (v.offset, u64::from(v.bytes), v.available_at);
    assert_eq!((read, v.state), (three, DeliveryState::Pending));
    let cancelled = op(&e, NqOp::Message { offset: 2 }).await;
    assert_eq!(cancelled.unwrap_err(), NqRefusal::Reclaimed);
}

/// A delete step deletes the pages first, and each deleted page's stored bytes become collection
/// debt in the state row that every step but the last writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_delete_step_records_the_pages_it_deletes_as_collection_debt() {
    let (e, db, _c) = clocked("nq-admin-delete-debt", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("bb"), msg("ccc")]))
        .await
        .unwrap();
    let mut pages = 0;
    for offset in 0..3 {
        pages += stored(&db, offset).await;
    }
    assert_eq!(state(&db).await.debt_bytes, 0);
    op(&e, NqOp::BeginDelete).await.unwrap();
    let step = op(&e, NqOp::DeleteStep { max_rows: 3 }).await.unwrap();
    assert!(matches!(step, NqOut::DeleteStepped { remaining: true }));
    for offset in 0..3 {
        let page = db.get(keys::page(&HASH, offset)).await.unwrap();
        assert!(page.is_none(), "page {offset} is deleted first");
    }
    let debt = state(&db).await.debt_bytes;
    assert_eq!(debt, pages, "the deleted pages are the step's debt");
}
