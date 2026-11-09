//! The shard's categorised queue-state bound (S3b-DESIGN §8.2-§8.4; edge
//! change #246): the committer keeps the shard's and each container's bytes
//! per category from the queues' exact gauges and puts them as `'N'` rows
//! in the batch that moves them; at a category's line a container over its
//! share, and within it a queue over its share, leases nothing new
//! (active, failed; the reply says `limited`) or has its scheduled or
//! fresh-keyed sends refused (delayed, dedup). Nothing else is refused.
#![cfg(test)]
use super::{CONTAINER, HASH, claim, clocked, msg, op, send_op, settle, state};
use crate::native_queue::{NqOp, NqOut, NqRefusal, QueueConfig, SendMessage, SettleKind, keys};
use crate::shard::ShardEngine;
use crate::shard::layout::queue_gauges::{self, QueueGauges};

/// A second queue, of another container.
const OTHER: [u8; 16] = [4; 16];
const OTHER_CONTAINER: [u8; 16] = [6; 16];
/// A bound whose slices are 300, 200, 200, 200 and 100 bytes.
const SMALL_BOUND: u64 = 1_000;

fn scheduled(body: &str, at: i64) -> SendMessage {
    SendMessage {
        deliver_at: Some(at),
        ..msg(body)
    }
}

fn keyed(body: &str, tag: u8) -> SendMessage {
    SendMessage {
        dedup_tag: Some([tag; 16]),
        ..msg(body)
    }
}

async fn op_on(e: &ShardEngine, hash: [u8; 16], op: NqOp) -> Result<NqOut, NqRefusal> {
    tokio::time::timeout(super::REPLY_WITHIN, e.submit_native_queue(hash, op))
        .await
        .expect("the committer never answered")
}

/// Queue `OTHER` of `OTHER_CONTAINER` beside the test's own.
async fn other_queue(e: &ShardEngine) {
    let create = NqOp::Create {
        cfg: QueueConfig::default(),
        container: OTHER_CONTAINER,
    };
    let out = op_on(e, OTHER, create).await.unwrap();
    assert!(matches!(out, NqOut::Config { created: true, .. }));
}

/// A claim's deliveries and its `limited` flag.
async fn claim_on(e: &ShardEngine, hash: [u8; 16], max: usize) -> (Vec<u64>, bool) {
    let claim = NqOp::Claim {
        max,
        max_bytes: 1 << 20,
        lease_ms: 30_000,
    };
    match op_on(e, hash, claim).await.unwrap() {
        NqOut::Claimed {
            deliveries,
            limited,
            ..
        } => (deliveries.iter().map(|d| d.offset).collect(), limited),
        other => panic!("claim answered {other:?}"),
    }
}

/// The persisted `'N'` rows, read back as an engine's first queue
/// operation reads them.
async fn persisted(db: &slatedb::Db) -> QueueGauges {
    let (from, to) = queue_gauges::key_range();
    let mut it = db.scan(from..to).await.unwrap();
    let mut rows = Vec::new();
    while let Some(kv) = it.next().await.unwrap() {
        rows.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    let g = QueueGauges::default();
    g.load(&rows);
    g
}

/// The stored bytes of the queue's range rows, as the shard log holds
/// them (key, value and the row overhead).
async fn range_rows(db: &slatedb::Db) -> u64 {
    let from = keys::family(&HASH, keys::RANGE);
    let mut to = from.clone();
    *to.last_mut().unwrap() += 1;
    let mut it = db.scan(from..to).await.unwrap();
    let mut bytes = 0;
    while let Some(kv) = it.next().await.unwrap() {
        bytes += (kv.key.len() + kv.value.len()) as u64 + queue_gauges::ROW_OVERHEAD;
    }
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_gauges_count_every_category_exactly_and_ride_the_batch() {
    let (e, db, c) = clocked("nq-slices-count", QueueConfig::default()).await;
    let later = c + 60_000;
    let batch = vec![
        scheduled("s0", later),
        scheduled("s1", later),
        keyed("k0", 1),
        keyed("k1", 2),
        keyed("k2", 3),
        msg("m"),
    ];
    op(&e, send_op(batch)).await.unwrap();
    let d = claim(&e, 2).await;
    assert_eq!((d[0].offset, d[1].offset), (2, 3), "the lane's first two");
    op(&e, settle(vec![(2, d[0].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    let s = state(&db).await;
    assert_eq!(
        (s.leased, s.failed, s.scheduled, s.dedup_entries),
        (1, 1, 2, 3)
    );
    assert!(s.inline_bytes > 0, "both scheduled pages ride inline");
    let ranges = range_rows(&db).await;
    assert!(ranges > 0);
    assert_eq!(s.range_bytes, ranges, "the range rows' stored bytes, exact");
    let want = [
        queue_gauges::W_ACTIVE,
        queue_gauges::W_FAILED,
        2 * queue_gauges::W_DELAYED + s.inline_bytes,
        3 * queue_gauges::W_DEDUP,
        ranges,
    ];
    assert_eq!(queue_gauges::categories(&s), want);
    assert_eq!(e.layout.queue_gauges.shard(), want, "the shard's sums");
    let read = persisted(&db).await;
    assert_eq!(read.shard(), want, "the 'N' rows carry them");
    // The ack and the discard take the active and failed bytes out again.
    op(&e, settle(vec![(3, d[1].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    op(&e, NqOp::Discard { offsets: vec![2] }).await.unwrap();
    let s = state(&db).await;
    let after = e.layout.queue_gauges.shard();
    assert_eq!((after[0], after[1]), (0, 0));
    assert_eq!(after, queue_gauges::categories(&s));
    assert_eq!(persisted(&db).await.shard(), after);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_over_its_projects_share_at_the_active_line_leases_nothing_and_says_limited() {
    let (e, db, _c) = clocked("nq-slices-active", QueueConfig::default()).await;
    e.layout.queue_gauges.set_bound_for_tests(SMALL_BOUND);
    other_queue(&e).await;
    let batch = (0..4).map(|n| msg(&format!("m{n}"))).collect();
    op(&e, send_op(batch)).await.unwrap();
    op_on(&e, OTHER, send_op(vec![msg("o0"), msg("o1")]))
        .await
        .unwrap();
    let d = claim(&e, 2).await;
    assert_eq!(
        e.layout.queue_gauges.shard()[0],
        448,
        "two leases: over 300"
    );
    let before = state(&db).await;
    assert_eq!(
        claim_on(&e, HASH, 2).await,
        (vec![], true),
        "448 of a 300 share"
    );
    assert_eq!(state(&db).await, before, "the limited claim stored nothing");
    // The other container holds nothing active: its claim is served.
    assert_eq!(claim_on(&e, OTHER, 1).await, (vec![0], false));
    // Now 672 at the line between two containers with active state, a share
    // of 150 each: the other at 224 is over it too, and so is this one.
    assert_eq!(claim_on(&e, OTHER, 1).await, (vec![], true));
    assert_eq!(claim_on(&e, HASH, 1).await, (vec![], true));
    // Its acks take the shard under the line; both lease again.
    let acks = d.iter().map(|v| (v.offset, v.lease_gen, SettleKind::Ack));
    op(&e, settle(acks.collect())).await.unwrap();
    assert_eq!(e.layout.queue_gauges.shard()[0], 224, "the other's one");
    assert_eq!(claim_on(&e, HASH, 2).await, (vec![2, 3], false));
    assert_eq!(e.layout.queue_gauges.shard()[0], 672);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_over_its_share_at_the_failed_line_says_limited_and_a_fail_is_taken() {
    let (e, db, _c) = clocked("nq-slices-failed", QueueConfig::default()).await;
    e.layout.queue_gauges.set_bound_for_tests(SMALL_BOUND);
    let batch = (0..3).map(|n| msg(&format!("m{n}"))).collect();
    op(&e, send_op(batch)).await.unwrap();
    let d = claim(&e, 2).await;
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    // 296 failed bytes over the 200 slice (224 active, under its 300).
    assert_eq!(e.layout.queue_gauges.shard()[..2], [224, 296]);
    assert_eq!(claim_on(&e, HASH, 1).await, (vec![], true));
    // The open lease's fail is never refused by it.
    op(&e, settle(vec![(1, d[1].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    let s = state(&db).await;
    assert_eq!((s.leased, s.failed), (0, 2));
    op(
        &e,
        NqOp::Discard {
            offsets: vec![0, 1],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        claim_on(&e, HASH, 1).await,
        (vec![2], false),
        "under the line"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_scheduled_send_over_its_share_at_the_delayed_line_is_refused_and_an_immediate_one_never()
{
    let (e, db, c) = clocked("nq-slices-delayed", QueueConfig::default()).await;
    e.layout.queue_gauges.set_bound_for_tests(SMALL_BOUND);
    let later = c + 60_000;
    op(
        &e,
        send_op(vec![scheduled("s0", later), scheduled("s1", later)]),
    )
    .await
    .unwrap();
    assert!(
        e.layout.queue_gauges.shard()[2] >= 320,
        "over the 200 slice"
    );
    let before = state(&db).await;
    let mixed = vec![msg("now"), scheduled("s2", later)];
    assert_eq!(
        op(&e, send_op(mixed)).await.unwrap_err(),
        NqRefusal::ShardQueueStateFull,
        "refused whole"
    );
    assert_eq!(state(&db).await, before, "nothing stored");
    let NqOut::Sent(sent) = op(&e, send_op(vec![msg("now")])).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        sent[0].offset, 2,
        "an immediate send is never refused by it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_fresh_keyed_send_over_its_share_at_the_dedup_line_is_refused_and_a_duplicate_never() {
    let (e, db, _c) = clocked("nq-slices-dedup", QueueConfig::default()).await;
    e.layout.queue_gauges.set_bound_for_tests(SMALL_BOUND);
    other_queue(&e).await;
    op(&e, send_op(vec![keyed("a", 1), keyed("b", 2)]))
        .await
        .unwrap();
    assert_eq!(e.layout.queue_gauges.shard()[3], 336, "over the 200 slice");
    let before = state(&db).await;
    assert_eq!(
        op(&e, send_op(vec![keyed("c", 3)])).await.unwrap_err(),
        NqRefusal::ShardQueueStateFull
    );
    assert_eq!(state(&db).await.dedup_entries, before.dedup_entries);
    let NqOut::Sent(dup) = op(&e, send_op(vec![keyed("a", 1), msg("plain")]))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        dup.iter()
            .map(|s| (s.offset, s.duplicate))
            .collect::<Vec<_>>(),
        vec![(0, true), (2, false)],
        "a duplicate and a send without a key are served"
    );
    // The other container holds no dedup state: its fresh key is served.
    let other = op_on(&e, OTHER, send_op(vec![keyed("x", 9)])).await;
    assert!(matches!(other, Ok(NqOut::Sent(_))));
    assert_eq!(e.layout.queue_gauges.shard()[3], 504);
    // Two containers with dedup state: shares of 100; this one at 336 is
    // over, the other at 168 too, both refused now.
    assert_eq!(
        op_on(&e, OTHER, send_op(vec![keyed("y", 8)]))
            .await
            .unwrap_err(),
        NqRefusal::ShardQueueStateFull
    );
    let s = state(&db).await;
    assert_eq!((s.container, s.dedup_entries), (CONTAINER, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_new_engine_reads_the_gauges_from_their_rows_at_its_first_queue_operation() {
    let (e, _db, _c) = clocked("nq-slices-reload", QueueConfig::default()).await;
    op(&e, send_op(vec![keyed("a", 1), keyed("b", 2)]))
        .await
        .unwrap();
    let gauges = &e.layout.queue_gauges;
    gauges.unload_for_tests();
    assert!(!gauges.loaded());
    assert!(claim(&e, 1).await.len() == 1);
    assert!(gauges.loaded(), "read by the next queue operation");
    assert_eq!(gauges.shard()[3], 336, "the persisted sums, not zero");
    op(&e, send_op(vec![keyed("c", 3)])).await.unwrap();
    assert_eq!(gauges.shard()[3], 504, "moved from them");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_group_whose_batch_was_not_written_leaves_the_sums_as_the_rows_hold_them() {
    let (e, db, _c) = clocked("nq-slices-unwritten", QueueConfig::default()).await;
    op(&e, send_op(vec![keyed("a", 1), keyed("b", 2)]))
        .await
        .unwrap();
    op(&e, NqOp::Get).await.unwrap();
    let gauges = &e.layout.queue_gauges;
    assert_eq!(
        gauges.staged_generation(),
        None,
        "the send's group settled as written"
    );
    // As a group that staged a move and was rejected: nothing written.
    gauges.apply("test", CONTAINER, ([0; 5], [0, 0, 0, 1_000, 0]));
    assert_eq!(gauges.shard()[3], 1_336);
    op(&e, NqOp::Get).await.unwrap();
    assert_eq!(
        gauges.shard()[3],
        336,
        "dropped: the shard log never held it"
    );
    op(&e, send_op(vec![keyed("c", 3)])).await.unwrap();
    assert_eq!(gauges.shard()[3], 504);
    op(&e, NqOp::Get).await.unwrap();
    assert_eq!(persisted(&db).await.shard()[3], 504, "the rows agree");
}
