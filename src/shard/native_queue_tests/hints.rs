//! The committer's trim hints (S3b-DESIGN §3.4, §7.3; part 2 step 2): a
//! group that seals a queue's pages, moves its frontier over sealed pages
//! or resolves a reference at a sealed offset puts one hint row naming
//! exactly those offsets, under the next sequence; offsets the committer
//! still holds are named by no hint until their seal. Every row is
//! asserted exactly.
#![cfg(test)]
use std::sync::Arc;

use super::{HASH, clocked, msg, op, send_op, settle};
use crate::native_queue::{NqOp, NqOut, PrepareFrom, QueueConfig, SendMessage, SettleKind, hints};
use crate::shard::{ShardConfig, ShardEngine};

/// Every hint row: its sequence, its queue and its ranges.
type Row = (u64, [u8; 16], Vec<(u64, u64)>);

async fn hint_rows(db: &slatedb::Db) -> Vec<Row> {
    let (from, to) = hints::key_range();
    let mut scan = db.scan(from..to).await.unwrap();
    let mut out = Vec::new();
    while let Some(kv) = scan.next().await.unwrap() {
        let (gseq, hash) = hints::parse_key(&kv.key).expect("a hint key");
        out.push((gseq, hash, hints::decode(&kv.value).unwrap()));
    }
    out
}

/// The rows after the first `seen`.
async fn new_rows(db: &slatedb::Db, seen: usize) -> Vec<Row> {
    hint_rows(db).await.split_off(seen)
}

async fn seal(e: &Arc<ShardEngine>) {
    e.seal_once(crate::shard::seal::Trigger::Requested)
        .await
        .unwrap();
}

/// A claim of up to `max` with the lane's sealed pages prepared: the
/// leases' offsets and generations.
async fn claim_sealed(e: &ShardEngine, max: usize) -> Vec<(u64, u32)> {
    let (prepared, _) = e
        .prepare_queue_pages(HASH, PrepareFrom::Frontier, usize::MAX)
        .await
        .unwrap();
    let claim = NqOp::Claim {
        max,
        max_bytes: 1 << 20,
        lease_ms: 30_000,
        prepared,
    };
    let NqOut::Claimed { deliveries, .. } = op(e, claim).await.unwrap() else {
        panic!("a claim answers deliveries");
    };
    deliveries.iter().map(|d| (d.offset, d.lease_gen)).collect()
}

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

/// Unsealed offsets are named by no hint; the seal names `[0, 3)`; a
/// claim over sealed pages names the offsets it passed; an ack of a
/// sealed message names it; a claim across the boundary names only the
/// sealed part, and an ack of an unsealed message nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_group_names_exactly_the_sealed_offsets_it_touched_in_one_hint_row() {
    let (e, db, _c) = clocked("nq-hints", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    let leased = super::claim(&e, 1).await;
    let ack = |o: u64, g: u32| settle(vec![(o, g, SettleKind::Ack)]);
    op(&e, ack(0, leased[0].lease_gen)).await.unwrap();
    assert_eq!(hint_rows(&db).await, [], "nothing sealed: no hint");
    seal(&e).await;
    let sealed = hint_rows(&db).await;
    assert_eq!(
        sealed
            .iter()
            .map(|r| (r.1, r.2.clone()))
            .collect::<Vec<_>>(),
        [(HASH, vec![(0, 3)])],
        "the seal names its pages"
    );
    let g = sealed[0].0;
    let two = claim_sealed(&e, 1).await;
    assert_eq!(two.iter().map(|l| l.0).collect::<Vec<_>>(), [1]);
    assert_eq!(new_rows(&db, 1).await, [(g + 1, HASH, vec![(1, 2)])]);
    op(&e, ack(1, two[0].1)).await.unwrap();
    assert_eq!(new_rows(&db, 2).await, [(g + 2, HASH, vec![(1, 2)])]);
    op(&e, send_op(vec![msg("d")])).await.unwrap();
    let across = claim_sealed(&e, 2).await;
    assert_eq!(across.iter().map(|l| l.0).collect::<Vec<_>>(), [2, 3]);
    assert_eq!(
        new_rows(&db, 3).await,
        [(g + 3, HASH, vec![(2, 3)])],
        "only the sealed offset is named"
    );
    op(&e, ack(3, across[1].1)).await.unwrap();
    assert_eq!(new_rows(&db, 4).await, [], "an unsealed offset: none");
}

/// A scheduled message that ends unresolved names its own offset: a
/// cancel of a sealed scheduled message behind an untaken one puts one
/// hint of exactly `[1, 2)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_cancelled_sealed_scheduled_message_names_its_offset() {
    let (e, db, c) = clocked("nq-hints-scheduled", QueueConfig::default()).await;
    let later = SendMessage {
        deliver_at: Some(c + 60_000),
        ..msg("later")
    };
    op(&e, send_op(vec![msg("a"), later])).await.unwrap();
    seal(&e).await;
    let seen = hint_rows(&db).await.len();
    op(&e, NqOp::Cancel { offsets: vec![1] }).await.unwrap();
    let named: Vec<Vec<(u64, u64)>> = new_rows(&db, seen).await.into_iter().map(|r| r.2).collect();
    assert_eq!(named, [vec![(1, 2)]]);
}

/// The sequence is seeded once per engine, above the last hint key and
/// the trim's cursor: a restarted engine's first hint follows the last
/// one; with every hint consumed, it follows the cursor's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_restarted_engine_seeds_its_hint_sequence_above_the_last_hint_and_the_cursor() {
    const NAME: &str = "nq-hints-seed";
    let (e, db, _c) = clocked(NAME, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    seal(&e).await;
    let g = hint_rows(&db).await[0].0;
    // The restarted engines read no sealed page: the leases are taken
    // first, and only acks follow.
    let leased = claim_sealed(&e, 2).await;
    assert_eq!(new_rows(&db, 1).await, [(g + 1, HASH, vec![(0, 2)])]);
    let ack = |i: usize| settle(vec![(leased[i].0, leased[i].1, SettleKind::Ack)]);
    let e = restart(NAME, &db, e);
    op(&e, ack(0)).await.unwrap();
    assert_eq!(new_rows(&db, 2).await, [(g + 2, HASH, vec![(0, 1)])]);
    for (gseq, hash, _) in hint_rows(&db).await {
        db.delete(hints::key(gseq, &hash)).await.unwrap();
    }
    db.put(hints::cursor_key(), hints::key(50, &HASH))
        .await
        .unwrap();
    let e = restart(NAME, &db, e);
    op(&e, ack(1)).await.unwrap();
    assert_eq!(hint_rows(&db).await, [(51, HASH, vec![(1, 2)])]);
}
