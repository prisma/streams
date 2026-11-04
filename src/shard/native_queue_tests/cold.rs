//! A commit group's cold native queues read what their operations need
//! together before the group stages (`transaction::native_queue::warm`),
//! not one after another on the shard's one serial commit path: with every
//! read of a kind held open, a group's queues have as many of them in
//! flight at once as the group loads cold streams together. A cell of tiny
//! queues each touched once an hour is cold at every touch (tuning plan
//! 2026-11-03, item 4), so each read staged inline held the whole shard
//! for a store round trip.
#![cfg(test)]
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::{REPLY_WITHIN, engine, msg, send_op};
use crate::native_queue::rows::StateRow;
use crate::native_queue::{
    DeliveryView, NqOp, NqOut, NqResult, QueueConfig, SettleItem, SettleKind, SettleOutcome, Token,
    keys,
};
use crate::shard::test_support::{
    HeldReads, hold_native_index_reads, hold_native_row_reads, hold_native_state_reads,
};
use crate::shard::{CommitOp, ShardEngine};

/// Queues in one group: more than the group loads together.
const QUEUES: u8 = 80;
/// Cold streams a group loads together (`prepare::COLD_LOADS`).
const TOGETHER: usize = 64;

/// Queue `n`'s storage hash.
fn hash(n: u8) -> [u8; 16] {
    [0x40 + n; 16]
}

async fn on(e: &ShardEngine, hash: [u8; 16], op: NqOp) -> NqOut {
    tokio::time::timeout(REPLY_WITHIN, e.submit_native_queue(hash, op))
        .await
        .expect("the committer never answered")
        .expect("the operation succeeded")
}

/// `QUEUES` queues, each holding one sent message.
async fn queues_with_a_message(e: &ShardEngine) {
    for n in 0..QUEUES {
        let cfg = QueueConfig::default();
        on(e, hash(n), NqOp::Create { cfg }).await;
        on(e, hash(n), send_op(vec![msg("m")])).await;
    }
}

fn claim_one() -> NqOp {
    NqOp::Claim {
        max: 1,
        max_bytes: 1 << 20,
        lease_ms: 30_000,
    }
}

/// Run one operation per queue as one commit group while `held` holds its
/// reads open: the most of them in flight at once, and each queue's reply.
async fn held_group(
    e: &ShardEngine,
    held: &Arc<HeldReads>,
    ops: Vec<NqOp>,
) -> (usize, Vec<NqResult>) {
    let (ops, replies): (Vec<_>, Vec<_>) = ops
        .into_iter()
        .zip(0..QUEUES)
        .map(|(op, n)| {
            let (resp, reply) = oneshot::channel();
            (
                CommitOp::NativeQueue {
                    hash: hash(n),
                    op,
                    resp,
                },
                reply,
            )
        })
        .unzip();
    let group = e.commit_group(ops);
    let watch = async {
        let deadline = Instant::now() + Duration::from_secs(2);
        while held.in_flight() < TOGETHER && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let together = held.most_in_flight();
        held.release();
        together
    };
    let ((), together) = futures_util::future::join(group, watch).await;
    let mut answers = Vec::new();
    for reply in replies {
        answers.push(reply.await.expect("the group answered"));
    }
    (together, answers)
}

/// The one delivery each queue's claim leased: its first message, first
/// attempt.
fn leased(answers: Vec<NqResult>) -> Vec<DeliveryView> {
    answers
        .into_iter()
        .map(|answer| match answer {
            Ok(NqOut::Claimed { mut deliveries, .. }) => {
                assert_eq!(deliveries.len(), 1, "one message leased");
                deliveries.remove(0)
            }
            other => panic!("claim answered {other:?}"),
        })
        .inspect(|d| assert_eq!((d.offset, d.attempt, d.lease_gen), (0, 1, 1)))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_group_loads_its_cold_queues_state_rows_together() {
    let (e, _db) = engine("nq-cold-state").await;
    queues_with_a_message(&e).await;
    let held = hold_native_state_reads("nq-cold-state");
    let claims = (0..QUEUES).map(|_| claim_one()).collect();
    let (together, answers) = held_group(&e, &held, claims).await;
    assert_eq!(together, TOGETHER, "the queues' state rows load together");
    assert_eq!(leased(answers).len(), usize::from(QUEUES));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_group_walks_its_cold_queues_indexes_together() {
    let (e, _db) = engine("nq-cold-index").await;
    queues_with_a_message(&e).await;
    let held = hold_native_index_reads("nq-cold-index");
    let claims = (0..QUEUES).map(|_| claim_one()).collect();
    let (together, answers) = held_group(&e, &held, claims).await;
    assert_eq!(together, TOGETHER, "the claims' index walks run together");
    assert_eq!(leased(answers).len(), usize::from(QUEUES));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_group_reads_its_cold_settles_rows_together() {
    let (e, db) = engine("nq-cold-rows").await;
    queues_with_a_message(&e).await;
    let mut tokens = Vec::new();
    for n in 0..QUEUES {
        let NqOut::Claimed { deliveries, .. } = on(&e, hash(n), claim_one()).await else {
            panic!("a claim answers deliveries");
        };
        tokens.push(Token {
            offset: deliveries[0].offset,
            lease_gen: deliveries[0].lease_gen,
        });
    }
    let held = hold_native_row_reads("nq-cold-rows");
    let acks = tokens
        .into_iter()
        .map(|token| NqOp::Settle {
            items: vec![SettleItem {
                token,
                kind: SettleKind::Ack,
                code: None,
            }],
        })
        .collect();
    let (together, answers) = held_group(&e, &held, acks).await;
    assert_eq!(together, TOGETHER, "the settles' rows load together");
    for answer in answers {
        let Ok(NqOut::Settled(results)) = answer else {
            panic!("a settle answers results");
        };
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].outcome, SettleOutcome::Ok);
    }
    for n in 0..QUEUES {
        let raw = db.get(keys::state(&hash(n))).await.unwrap().unwrap();
        let s = StateRow::decode(&raw).unwrap();
        assert_eq!(
            (s.queued_messages, s.leased, s.counters.acked),
            (0, 0, 1),
            "queue {n} acked its message"
        );
    }
}

/// A group of sweeps (the roster brings each queue with collection debt
/// back an hour after its touch, all of them cold) walks their indexes
/// together, and each sweep leaves its untaken message where it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_group_sweeps_its_cold_queues_together() {
    let (e, db) = engine("nq-cold-sweep").await;
    queues_with_a_message(&e).await;
    let held = hold_native_index_reads("nq-cold-sweep");
    let sweeps = (0..QUEUES).map(|_| NqOp::Sweep).collect();
    let (together, answers) = held_group(&e, &held, sweeps).await;
    assert_eq!(together, TOGETHER, "the sweeps' index walks run together");
    for answer in answers {
        assert!(
            matches!(answer, Ok(NqOut::Swept { wake: None, .. })),
            "{answer:?}"
        );
    }
    for n in 0..QUEUES {
        let raw = db.get(keys::state(&hash(n))).await.unwrap().unwrap();
        let s = StateRow::decode(&raw).unwrap();
        assert_eq!(
            (s.queued_messages, s.leased),
            (1, 0),
            "queue {n} keeps its message"
        );
    }
}
