//! Index floors and the roster's lower bound (docs/QUEUES-DESIGN.md §6.8):
//! scans start at each time-ordered index's first live entry, and the
//! roster parks at the earliest deadline the state row's floors allow, so
//! an active queue is swept when something can be due, not every second.
#![cfg(test)]
use tokio::sync::oneshot;

use super::{
    HASH, REPLY_WITHIN, claim, clocked, msg, op, roster, send_op, set_clock, settle, state, swept,
};
use crate::native_queue::rows::{Floors, MetaRow, encode_avail};
use crate::native_queue::{
    DAY_MS, DeliveryView, HOUR_MS, NqOp, NqOut, NqResult, QueueConfig, SendMessage, SettleKind,
    keys,
};
use crate::shard::{CommitOp, ShardEngine};

const RETENTION: i64 = 4 * DAY_MS;
const LEASE: i64 = 30_000;

/// A message sent to be due at `at`.
fn due_at(body: &str, at: i64) -> SendMessage {
    SendMessage {
        deliver_at: Some(at),
        ..msg(body)
    }
}

fn offsets(d: &[DeliveryView]) -> Vec<u64> {
    d.iter().map(|v| v.offset).collect()
}

fn claim_op(max: usize, max_bytes: u64) -> NqOp {
    NqOp::Claim {
        max,
        max_bytes,
        lease_ms: LEASE,
    }
}

fn deliveries(out: NqResult) -> Vec<DeliveryView> {
    match out {
        Ok(NqOut::Claimed { deliveries, .. }) => deliveries,
        other => panic!("claim answered {other:?}"),
    }
}

/// Commits `first` and then a claim of up to `max` as one commit group, so
/// the claim reads the rows `first` staged; returns the claim's deliveries.
async fn claim_after(e: &ShardEngine, first: NqOp, max: usize) -> Vec<DeliveryView> {
    let (first_tx, first_rx) = oneshot::channel();
    let (claim_tx, claim_rx) = oneshot::channel();
    let ops = vec![
        CommitOp::NativeQueue {
            hash: HASH,
            op: first,
            resp: first_tx,
        },
        CommitOp::NativeQueue {
            hash: HASH,
            op: claim_op(max, 1 << 20),
            resp: claim_tx,
        },
    ];
    e.commit_group(ops).await;
    let reply = |rx: oneshot::Receiver<NqResult>| async move {
        tokio::time::timeout(REPLY_WITHIN, rx)
            .await
            .expect("the group never answered")
            .expect("the group dropped its reply")
    };
    reply(first_rx).await.unwrap();
    deliveries(reply(claim_rx).await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_that_takes_the_due_work_raises_the_floor_to_the_next_entry() {
    let (e, db, c) = clocked("nq-floor-next", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), due_at("b", c + 60_000)]))
        .await
        .unwrap();
    let d = claim(&e, 10).await;
    assert_eq!(offsets(&d), vec![0]);
    let f = state(&db).await.floors;
    assert_eq!(
        (f.avail, f.retries),
        (c + 60_000, i64::MAX),
        "the claim took the due entry and stopped at the next one"
    );
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    set_clock(&db, c + 59_999).await;
    assert!(claim(&e, 10).await.is_empty(), "a millisecond early");
    set_clock(&db, c + 60_000).await;
    assert_eq!(
        offsets(&claim(&e, 10).await),
        vec![1],
        "due exactly at the floor"
    );
    assert_eq!(state(&db).await.floors.avail, i64::MAX, "nothing untaken");
    assert_eq!(
        swept(&e).await.0,
        Some(c + 60_000 + LEASE),
        "the lease of offset 1 is the only deadline"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_reads_no_availability_entry_below_a_future_floor() {
    let (e, db, c) = clocked("nq-floor-skip", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), due_at("b", c + 60_000)]))
        .await
        .unwrap();
    assert_eq!(offsets(&claim(&e, 10).await), vec![0]);
    // An entry below the floor breaks invariant F on purpose: only a claim
    // that reads the index can find it.
    let key = keys::by_time(&HASH, keys::AVAIL, c + 1_000, 7);
    let planted = encode_avail(
        &MetaRow {
            enqueued_at: c,
            available_at: c + 1_000,
            retention_end: c + RETENTION,
            hard_expires_at: c + 2 * RETENTION,
            bytes: 1,
            open: 0,
            cancelled: false,
            taken: false,
            dedup_tag: None,
        },
        &[],
    );
    db.put(&key, &planted).await.unwrap();
    set_clock(&db, c + 30_000).await;
    assert!(
        claim(&e, 10).await.is_empty(),
        "the claim read the index below its floor"
    );
    assert_eq!(db.get(&key).await.unwrap().as_deref(), Some(&planted[..]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_put_below_a_raised_floor_lowers_it_and_the_next_claim_takes_it() {
    let (e, db, c) = clocked("nq-floor-lowered", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), due_at("b", c + 60_000)]))
        .await
        .unwrap();
    claim(&e, 10).await;
    set_clock(&db, c + 1_000).await;
    op(&e, send_op(vec![msg("c")])).await.unwrap();
    assert_eq!(
        state(&db).await.floors.avail,
        c + 1_000,
        "the send lowered it"
    );
    let d = claim(&e, 10).await;
    assert_eq!(offsets(&d), vec![2]);
    assert_eq!(state(&db).await.floors.avail, c + 60_000);
    let now = SettleKind::Retry { delay_ms: Some(0) };
    op(&e, settle(vec![(2, d[0].lease_gen, now)]))
        .await
        .unwrap();
    assert_eq!(
        state(&db).await.floors.retries,
        c + 1_000,
        "the retry lowered it"
    );
    let d = claim(&e, 10).await;
    assert_eq!((offsets(&d), d[0].attempt), (vec![2], 2));
    let f = state(&db).await.floors;
    assert_eq!((f.avail, f.retries), (c + 60_000, i64::MAX));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_cut_short_leaves_the_floor_at_the_first_entry_it_did_not_take() {
    let (e, db, c) = clocked("nq-floor-cut", QueueConfig::default()).await;
    for i in 0..4_i64 {
        set_clock(&db, c + i).await;
        op(&e, send_op(vec![msg(&format!("{i:0>100}"))]))
            .await
            .unwrap();
    }
    let mut held = claim(&e, 2).await;
    assert_eq!(offsets(&held), vec![0, 1]);
    assert_eq!(state(&db).await.floors.avail, c + 2, "stopped by max");
    let d = deliveries(op(&e, claim_op(10, 150)).await);
    assert_eq!(offsets(&d), vec![2]);
    assert_eq!(state(&db).await.floors.avail, c + 3, "stopped by bytes");
    held.extend(d);
    held.extend(claim(&e, 10).await);
    assert_eq!(state(&db).await.floors.avail, i64::MAX);
    // The retry index, one retry due each millisecond from c + 4.
    for (i, v) in (4..).zip(&held) {
        set_clock(&db, c + i).await;
        let now = SettleKind::Retry { delay_ms: Some(0) };
        op(&e, settle(vec![(v.offset, v.lease_gen, now)]))
            .await
            .unwrap();
    }
    assert_eq!(offsets(&claim(&e, 1).await), vec![0]);
    assert_eq!(state(&db).await.floors.retries, c + 5, "stopped by max");
    assert_eq!(offsets(&claim(&e, 10).await), vec![1, 2, 3]);
    assert_eq!(state(&db).await.floors.retries, i64::MAX);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_raises_the_floor_past_an_entry_its_group_cancelled() {
    let (e, db, c) = clocked("nq-floor-cancel", QueueConfig::default()).await;
    let later = vec![msg("a"), due_at("b", c + 60_000), due_at("c", c + 120_000)];
    op(&e, send_op(later)).await.unwrap();
    let d = claim_after(&e, NqOp::Cancel { offsets: vec![1] }, 10).await;
    assert_eq!(offsets(&d), vec![0]);
    assert_eq!(
        state(&db).await.floors.avail,
        c + 120_000,
        "past the cancelled entry to the next live one"
    );
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    assert_eq!(
        swept(&e).await.0,
        Some(c + 120_000 + RETENTION),
        "offset 2 keeps the queue parked at its retention end"
    );
    set_clock(&db, c + 119_999).await;
    assert!(claim(&e, 10).await.is_empty());
    set_clock(&db, c + 120_000).await;
    assert_eq!(offsets(&claim(&e, 10).await), vec![2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_raises_the_floor_to_a_later_entry_its_group_sent() {
    let (e, db, c) = clocked("nq-floor-staged-send", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim_after(&e, send_op(vec![due_at("b", c + 60_000)]), 10).await;
    assert_eq!(offsets(&d), vec![0]);
    assert_eq!(state(&db).await.floors.avail, c + 60_000);
    set_clock(&db, c + 60_000).await;
    assert_eq!(offsets(&claim(&e, 10).await), vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_raises_the_retry_floor_past_a_retry_its_group_cancelled() {
    let (e, db, c) = clocked("nq-floor-retry-cancel", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    let d = claim(&e, 3).await;
    let retry = |ms| SettleKind::Retry { delay_ms: Some(ms) };
    let items = d
        .iter()
        .zip([0, 60_000, 120_000])
        .map(|(v, ms)| (v.offset, v.lease_gen, retry(ms)))
        .collect();
    op(&e, settle(items)).await.unwrap();
    assert_eq!(state(&db).await.floors.retries, c);
    let d = claim_after(&e, NqOp::Cancel { offsets: vec![1] }, 10).await;
    assert_eq!((offsets(&d), d[0].attempt), (vec![0], 2));
    assert_eq!(
        state(&db).await.floors.retries,
        c + 120_000,
        "past the cancelled retry to the next live one"
    );
    set_clock(&db, c + 119_999).await;
    assert!(claim(&e, 10).await.is_empty());
    set_clock(&db, c + 120_000).await;
    assert_eq!(offsets(&claim(&e, 10).await), vec![2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_cut_short_by_expired_entries_never_passes_an_entry_it_did_not_read() {
    let (e, db, c) = clocked("nq-floor-expired", QueueConfig::default()).await;
    op(
        &e,
        send_op((0..25).map(|i| msg(&format!("m{i}"))).collect()),
    )
    .await
    .unwrap();
    set_clock(&db, c + RETENTION).await;
    // A claim of one reads 18 stored entries, and one more for the send its
    // group staged: all 19 have expired, and the send lies past them.
    let d = claim_after(&e, send_op(vec![msg("late")]), 1).await;
    assert!(d.is_empty(), "the claim passed entries it never read");
    let s = state(&db).await;
    assert_eq!((s.counters.expired, s.floors.avail), (19, c));
    assert_eq!(offsets(&claim(&e, 1).await), vec![25]);
    let s = state(&db).await;
    assert_eq!((s.counters.expired, s.floors.avail), (25, i64::MAX));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_due_retry_waiting_for_a_claim_does_not_delay_an_earlier_lease_expiry() {
    let (e, db, c) = clocked("nq-floor-lease-first", QueueConfig::default()).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn claims_and_acks_leave_a_parked_roster_where_the_sweep_put_it() {
    let (e, db, c) = clocked("nq-floor-quiet", QueueConfig::default()).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn floors_fall_with_every_put_and_rise_with_the_scans_that_find_the_heads() {
    let (e, db, c) = clocked("nq-floor-exact", QueueConfig::default()).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_waiting_retry_is_swept_only_at_its_cycle_deadline() {
    let (e, db, c) = clocked("nq-floor-cycle", QueueConfig::default()).await;
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
