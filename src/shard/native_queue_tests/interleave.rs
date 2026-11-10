//! S3b-DESIGN §12's DST 3 on page rows (part 1): delayed, immediate,
//! cancelled and retried messages interleaved across two restarts and an
//! expiry, every step's rows exact. A restart is a fresh engine on the
//! same shard log: the queue's state, range and delivery rows load from
//! the store, nothing from memory.
#![cfg(test)]
use std::sync::Arc;

use super::{HASH, claim, clocked, msg, op, send_op, set_clock, settle, state, stored};
use crate::native_queue::range::RangeRow;
use crate::native_queue::{NqOp, NqOut, QueueConfig, SendMessage, SettleKind, keys};
use crate::shard::{ShardConfig, ShardEngine};

fn scheduled(body: &str, at: i64) -> SendMessage {
    SendMessage {
        deliver_at: Some(at),
        ..msg(body)
    }
}

/// A fresh engine on the same shard log.
fn restart(name: &str, db: &Arc<slatedb::Db>, old: Arc<ShardEngine>) -> Arc<ShardEngine> {
    drop(old);
    ShardEngine::start(
        name.to_owned(),
        db.clone(),
        Arc::new(object_store::memory::InMemory::new()),
        ShardConfig::default(),
        None,
        Default::default(),
    )
}

/// Every row under the queue's prefix, as (family, offset or time) pairs:
/// the pages, scheduled index, scheduled-time, delivery and range rows by
/// their first suffix field; the state row and the tail by family alone.
async fn rows(db: &slatedb::Db) -> Vec<(u8, u64)> {
    let from = crate::keyspace::queue_row(&HASH, 17);
    let mut to = from.clone();
    *to.last_mut().unwrap() += 1;
    let mut scan = db.scan(from..to).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = scan.next().await.unwrap() {
        let fam = kv.key[keys::FAMILY_AT];
        let first = kv
            .key
            .get(keys::FAMILY_AT + 1..keys::FAMILY_AT + 9)
            .map_or(0, |b| u64::from_be_bytes(b.try_into().unwrap()));
        out.push((fam, first));
    }
    out
}

async fn range(db: &slatedb::Db) -> Option<RangeRow> {
    db.get(keys::range(&HASH, 0))
        .await
        .unwrap()
        .map(|raw| RangeRow::decode(&raw).unwrap())
}

/// The offsets a range row's bitmap holds, among the six sent.
fn held(bits: &crate::native_queue::range::Bits) -> Vec<u64> {
    (0..6).filter(|o| bits.contains(*o)).collect()
}

const NAME: &str = "nq-interleave";

/// The scenario's shard log, its clock origin and what the phases learn.
struct Run {
    db: Arc<slatedb::Db>,
    c: i64,
    /// The stored bytes of the six pages, and of the cancelled one.
    bytes: (u64, u64),
}

/// 0 immediate, 1 scheduled at +10 s, 2 and 3 immediate, 4 scheduled at
/// +5 s, 5 immediate; then 3 is cancelled untaken: its bytes stay queued
/// until the frontier passes it.
async fn send_and_cancel(e: &ShardEngine, run: &mut Run) {
    let c = run.c;
    let sent = vec![
        msg("a"),
        scheduled("b", c + 10_000),
        msg("c"),
        msg("d"),
        scheduled("e", c + 5_000),
        msg("f"),
    ];
    op(e, send_op(sent)).await.unwrap();
    for o in 0..6 {
        run.bytes.0 += stored(&run.db, o).await;
    }
    run.bytes.1 = stored(&run.db, 3).await;
    op(e, NqOp::Cancel { offsets: vec![3] }).await.unwrap();
    let st = state(&run.db).await;
    assert_eq!(
        (
            st.frontier,
            st.queued_messages,
            st.queued_bytes,
            st.scheduled
        ),
        (0, 5, run.bytes.0, 2)
    );
}

/// At +0 s the lane leases 0, passes 1 (scheduled, unresolved: its page
/// stays) and leases 2; 0 is retried a second later. Returns 2's lease
/// generation.
async fn first_claim(e: &ShardEngine, run: &Run) -> u32 {
    let leased = claim(e, 2).await;
    assert_eq!(leased.iter().map(|v| v.offset).collect::<Vec<_>>(), [0, 2]);
    let retry = SettleKind::Retry {
        delay_ms: Some(1_000),
    };
    op(e, settle(vec![(0, leased[0].lease_gen, retry)]))
        .await
        .unwrap();
    let r = range(&run.db).await.unwrap();
    assert_eq!(
        (
            r.open,
            r.sched,
            held(&r.skip),
            held(&r.cancel),
            held(&r.sbits)
        ),
        (2, 2, vec![4], vec![3], vec![1, 4])
    );
    assert_eq!(state(&run.db).await.frontier, 3);
    leased[1].lease_gen
}

/// After a restart, at +6 s: the lane passes 3 (cancelled: its page and
/// bytes go) and 4 (scheduled), and leases 5; then 0's retry (due +1 s)
/// and 4's scheduled entry (due +5 s), in (due, offset) order. Returns the
/// leases, each with its generation.
async fn second_claim(e: &ShardEngine, run: &Run) -> Vec<(u64, u32)> {
    let c = run.c;
    set_clock(&run.db, c + 6_000).await;
    let leased = claim(e, 3).await;
    assert_eq!(
        leased
            .iter()
            .map(|v| (v.offset, v.attempt))
            .collect::<Vec<_>>(),
        [(5, 1), (0, 2), (4, 1)]
    );
    let st = state(&run.db).await;
    assert_eq!(
        (
            st.frontier,
            st.lane_expire_at,
            st.queued_messages,
            st.queued_bytes
        ),
        (6, 0, 5, run.bytes.0 - run.bytes.1),
        "every immediate offset is behind the frontier; the cancelled bytes left at its pass"
    );
    let r = range(&run.db).await.unwrap();
    assert_eq!(
        (
            r.open,
            r.sched,
            held(&r.skip),
            held(&r.cancel),
            held(&r.sbits)
        ),
        (4, 2, vec![], vec![], vec![1, 4]),
        "four open deliveries; the scheduled bits stay until their messages end"
    );
    let at = |ms: i64| u64::try_from(ms).unwrap();
    let (a, d, l, m, p) = (
        keys::AVAIL,
        keys::DELIVERY,
        keys::LEASES,
        keys::SCHEDULED_AT,
        keys::PAGE,
    );
    assert_eq!(
        rows(&run.db).await,
        [
            (a, at(c + 5_000)),
            (a, at(c + 10_000)),
            (d, 0),
            (d, 2),
            (d, 4),
            (d, 5),
            (l, at(c + 30_000)),
            (l, at(c + 36_000)),
            (l, at(c + 36_000)),
            (l, at(c + 36_000)),
            (m, 1),
            (m, 4),
            (p, 0),
            (p, 1),
            (p, 2),
            (p, 4),
            (p, 5),
            (keys::STATE, 0),
            (b't', 0),
            (keys::RANGE, 0),
        ],
        "exact rows after the second claim: no row for 3, no retry for 0"
    );
    leased.iter().map(|v| (v.offset, v.lease_gen)).collect()
}

/// After the acks and a second restart, the clock passes 1's retention
/// end: the sweep takes it into the gap, and only the state row and the
/// tail remain.
async fn expiry(e: &ShardEngine, run: &Run) {
    let retention = state(&run.db).await.cfg.retention_ms;
    set_clock(&run.db, run.c + 10_000 + retention).await;
    let swept = op(e, NqOp::Sweep).await.unwrap();
    assert!(matches!(swept, NqOut::Swept { .. }), "{swept:?}");
    let st = state(&run.db).await;
    assert_eq!(
        (
            st.queued_messages,
            st.queued_bytes,
            st.scheduled,
            st.gap_count,
            st.counters.expired,
            st.counters.acked,
            st.counters.cancelled
        ),
        (0, 0, 0, 1, 1, 4, 1)
    );
    assert_eq!(range(&run.db).await, None, "the empty range row is deleted");
    assert_eq!(rows(&run.db).await, [(keys::STATE, 0), (b't', 0)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn delayed_immediate_cancelled_and_retried_messages_interleave_across_restarts_and_an_expiry()
{
    let (e, db, c) = clocked(NAME, QueueConfig::default()).await;
    let mut run = Run {
        db,
        c,
        bytes: (0, 0),
    };
    send_and_cancel(&e, &mut run).await;
    let two = first_claim(&e, &run).await;
    let e = restart(NAME, &run.db, e);
    let leased = second_claim(&e, &run).await;
    let mut acks: Vec<(u64, u32, SettleKind)> = leased
        .into_iter()
        .map(|(o, g)| (o, g, SettleKind::Ack))
        .collect();
    acks.push((2, two, SettleKind::Ack));
    op(&e, settle(acks)).await.unwrap();
    let e = restart(NAME, &run.db, e);
    expiry(&e, &run).await;
}
