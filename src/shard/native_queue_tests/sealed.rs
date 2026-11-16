//! A native queue's sealed lane (S3b-DESIGN §6.2-§6.3): once a seal takes
//! its pages the committer holds none of them, and every walk of the lane -
//! the sweeper's, the pending view's, a read by id - is served from the
//! pages its caller read outside the committer, never from a store read in
//! it.
#![cfg(test)]
use super::{HASH, clocked, msg, op, send_op, set_clock, state};
use crate::native_queue::{NqOp, NqOut, NqRefusal, PrepareFrom, PreparedPage, QueueConfig};
use crate::shard::ShardEngine;

/// Seal the queue stream's pages: none stays in the committer's memory.
async fn seal(e: &std::sync::Arc<ShardEngine>) {
    e.seal_once(crate::shard::seal::Trigger::Requested)
        .await
        .unwrap();
    assert!(e.unsealed_page_for_tests(&HASH, 0).is_none(), "sealed");
}

/// The offsets a page of a view shows, and its cursor.
fn shown(out: NqOut) -> (Vec<u64>, Option<u64>) {
    match out {
        NqOut::Deliveries { items, next } => (items.iter().map(|v| v.offset).collect(), next),
        other => panic!("a view answers deliveries: {other:?}"),
    }
}

fn offsets(pages: &[PreparedPage]) -> Vec<u64> {
    pages.iter().map(|p| p.0).collect()
}

/// A sealed backlog no consumer claims still expires: its sweep alone
/// answers `lane_sealed` and passes nothing unread, and the sweeper's
/// second sweep, with the pages it read, expires the lane to its end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_sealed_lane_expires_through_the_pages_its_sweeper_read() {
    let (e, db, c) = clocked("nq-sealed-sweep", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    seal(&e).await;
    let retention = state(&db).await.cfg.retention_ms;
    set_clock(&db, c + retention + 1).await;
    let alone = op(
        &e,
        NqOp::Sweep {
            prepared: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(
            alone,
            NqOut::Swept {
                lane_sealed: true,
                ..
            }
        ),
        "{alone:?}"
    );
    assert_eq!(state(&db).await.frontier, 0, "nothing passed unread");
    let swept = e.sweep_queue(HASH).await.unwrap();
    assert!(
        matches!(
            swept,
            NqOut::Swept {
                lane_sealed: false,
                ..
            }
        ),
        "{swept:?}"
    );
    let st = state(&db).await;
    assert_eq!(
        (
            st.frontier,
            st.queued_messages,
            st.queued_bytes,
            st.counters.expired,
            st.lane_expire_at
        ),
        (2, 0, 0, 2, 0)
    );
}

/// The pending view and a read by id of sealed untaken messages: with
/// nothing prepared the view shows none of them and a read by id answers
/// reclaimed; with the pages prepared both show them, each message's facts
/// from its own page. A prepared window with a gap is shown only up to the
/// gap, so a view never steps over a message. A preparation starts at the
/// frontier: a read by id of a message behind it reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_sealed_untaken_message_is_shown_from_the_pages_its_reader_prepared() {
    let (e, _db, _c) = clocked("nq-sealed-views", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    seal(&e).await;
    let peek = |prepared| NqOp::Peek {
        after: None,
        limit: 10,
        prepared,
    };
    assert_eq!(
        shown(op(&e, peek(Vec::new())).await.unwrap()),
        (vec![], None)
    );
    let (pages, through) = e
        .prepare_queue_pages(HASH, PrepareFrom::From(0), usize::MAX)
        .await
        .unwrap();
    assert_eq!((offsets(&pages), through), (vec![0, 1, 2], 3));
    assert_eq!(
        shown(op(&e, peek(pages.clone())).await.unwrap()),
        (vec![0, 1, 2], None)
    );
    let gapped = vec![pages[0].clone(), pages[2].clone()];
    assert_eq!(shown(op(&e, peek(gapped)).await.unwrap()), (vec![0], None));
    let message = |offset, prepared| NqOp::Message { offset, prepared };
    assert!(matches!(
        op(&e, message(1, Vec::new())).await,
        Err(NqRefusal::Reclaimed)
    ));
    let (one, _) = e
        .prepare_queue_pages(HASH, PrepareFrom::At(1), 1)
        .await
        .unwrap();
    assert_eq!(offsets(&one), [1], "one page, however small the budget");
    let NqOut::Message(v) = op(&e, message(1, one)).await.unwrap() else {
        panic!("a read by id answers its message");
    };
    assert_eq!(
        (v.offset, usize::try_from(v.bytes).unwrap()),
        (1, pages[1].1.len())
    );
    let claim = NqOp::Claim {
        max: 2,
        max_bytes: 1 << 20,
        lease_ms: 30_000,
        prepared: pages,
    };
    let NqOut::Claimed { deliveries, .. } = op(&e, claim).await.unwrap() else {
        panic!("a claim answers deliveries");
    };
    assert_eq!(deliveries.len(), 2, "0 and 1 leased: the frontier is at 2");
    let behind = e.prepare_queue_pages(HASH, PrepareFrom::At(1), 1).await;
    assert_eq!(behind.unwrap(), (Vec::new(), 3));
    let (from_frontier, _) = e
        .prepare_queue_pages(HASH, PrepareFrom::From(0), usize::MAX)
        .await
        .unwrap();
    assert_eq!(offsets(&from_frontier), [2]);
}

/// The families of the queue's rows, in key order.
async fn queue_rows(db: &slatedb::Db) -> Vec<u8> {
    let from = crate::keyspace::queue_row(&HASH, 0);
    let mut to = from.clone();
    *to.last_mut().unwrap() += 1;
    let mut scan = db.scan(from..to).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = scan.next().await.unwrap() {
        out.push(kv.key[crate::native_queue::keys::FAMILY_AT]);
    }
    out
}

/// An untaken immediate message costs no queue row (S3b-DESIGN §3.4,
/// §15.2 step 1): after 1,000 sends the queue's rows are its state row and
/// its tail, the messages' pages in its stream's pending row; after their
/// seal the same two rows and no pending row, every page read back from
/// the segments in offset order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_thousand_untaken_immediate_messages_cost_no_queue_row() {
    let (e, db, _c) = clocked("nq-sealed-rows", QueueConfig::default()).await;
    for round in 0..10 {
        let batch = (0..100).map(|n| msg(&format!("m{round}-{n}"))).collect();
        op(&e, send_op(batch)).await.unwrap();
    }
    let two = vec![crate::native_queue::keys::STATE, b't'];
    let pending = crate::spill::pending::pending_key(&HASH);
    assert_eq!(queue_rows(&db).await, two);
    assert!(db.get(&pending).await.unwrap().is_some(), "the pages");
    let st = state(&db).await;
    assert_eq!((st.queued_messages, st.frontier), (1_000, 0));
    seal(&e).await;
    assert_eq!(queue_rows(&db).await, two);
    assert_eq!(db.get(&pending).await.unwrap(), None, "all sealed");
    let (pages, through) = e
        .prepare_queue_pages(HASH, PrepareFrom::Frontier, usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        (offsets(&pages), through),
        ((0..1_000).collect::<Vec<_>>(), 1_000)
    );
}
