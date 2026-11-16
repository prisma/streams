//! An operator's reads and actions on a queue (docs/QUEUES-DESIGN.md §6.7, §8.2) where they meet
//! the lane: peek and message read an untaken message from its page, a cancelled one is neither
//! listed nor read nor cancelled a second time, and a queue created again starts its frontier
//! past the earlier incarnation's offsets.
#![cfg(test)]
use super::{claim, clocked, create, engine, msg, op, send_op, state, stored};
use crate::native_queue::{DeliveryState, NqOp, NqOut, NqRefusal, QueueConfig};

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
        prepared: Vec::new(),
    };
    let NqOut::Deliveries { items, next } = op(&e, peek).await.unwrap() else {
        panic!("a peek answers deliveries")
    };
    let listed: Vec<_> = items.iter().map(|v| (v.offset, v.state)).collect();
    let (leased, pending) = (DeliveryState::Leased, DeliveryState::Pending);
    assert_eq!(listed, [(0, leased), (1, pending), (3, pending)]);
    assert_eq!(next, None, "a page short of its limit has no cursor");
    let three = (3, stored(&e, 3), c);
    let last = &items[2];
    assert_eq!(
        (last.offset, u64::from(last.bytes), last.available_at),
        three
    );
    let by_id = |offset| NqOp::Message {
        offset,
        prepared: Vec::new(),
    };
    let NqOut::Message(v) = op(&e, by_id(3)).await.unwrap() else {
        panic!("an untaken message is read from its page")
    };
    let read = (v.offset, u64::from(v.bytes), v.available_at);
    assert_eq!((read, v.state), (three, DeliveryState::Pending));
    let cancelled = op(&e, by_id(2)).await;
    assert_eq!(cancelled.unwrap_err(), NqRefusal::Reclaimed);
}

/// A queue created again on the stream an earlier incarnation used starts its frontier at the
/// stream's next offset: the earlier incarnation's offsets are behind it, so a cancel naming one
/// cancels nothing and the new queue's one message stays queued and is the one claimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_queue_created_again_starts_its_frontier_past_the_earlier_incarnations_offsets() {
    let (e, db) = engine("nq-recreate").await;
    create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    op(&e, NqOp::BeginDelete).await.unwrap();
    let mut done = false;
    for _ in 0..100 {
        if let NqOut::DeleteStepped { remaining: false } =
            op(&e, NqOp::DeleteStep { max_rows: 7 }).await.unwrap()
        {
            done = true;
            break;
        }
    }
    assert!(done, "the delete never finished");
    create(&e, QueueConfig::default()).await;
    assert_eq!(
        state(&db).await.frontier,
        3,
        "the frontier starts at the stream's next offset"
    );
    op(&e, send_op(vec![msg("d")])).await.unwrap();
    let cancelled = op(&e, NqOp::Cancel { offsets: vec![1] }).await.unwrap();
    assert_eq!(
        cancelled.count(),
        0,
        "an earlier incarnation's offset cancels nothing"
    );
    let s = state(&db).await;
    assert_eq!(
        (s.queued_messages, s.counters.cancelled, s.frontier),
        (1, 0, 3)
    );
    let d = claim(&e, 5).await;
    assert_eq!(d.iter().map(|v| v.offset).collect::<Vec<_>>(), vec![3]);
}
