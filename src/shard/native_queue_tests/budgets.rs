//! Budgets, listings, claims and settles (docs/QUEUES-DESIGN.md §6-§8) on a
//! queue whose clock the test sets: each limit is probed at its exact
//! boundary and each count and stored row is asserted by value.
#![cfg(test)]
use super::{
    HASH, claim, clocked, create, engine, msg, op, send_op, set_clock, settle, state, stored,
};
use crate::native_queue::rows::{DState, DeliveryRow};
use crate::native_queue::{
    DAY_MS, HOUR_MS, NqOp, NqOut, NqRefusal, QueueConfig, SendMessage, SettleKind, keys,
};

const RETENTION: i64 = 4 * DAY_MS;

async fn delivery(db: &slatedb::Db, offset: u64) -> DeliveryRow {
    let raw = db.get(keys::by_offset(&HASH, keys::DELIVERY, offset)).await;
    DeliveryRow::decode(&raw.unwrap().expect("a delivery row")).unwrap()
}

/// Claim `n` messages and fail each explicitly, in one settle.
async fn fail_claimed(e: &super::ShardEngine, n: usize) {
    let d = claim(e, n).await;
    assert_eq!(d.len(), n);
    let items = d.iter().map(|v| (v.offset, v.lease_gen, SettleKind::Fail));
    op(e, settle(items.collect())).await.unwrap();
}

/// Offsets and next cursor of a listing.
async fn page(e: &super::ShardEngine, listing: NqOp) -> (Vec<u64>, Option<u64>) {
    match op(e, listing).await.unwrap() {
        NqOut::Deliveries { items, next } => (items.iter().map(|v| v.offset).collect(), next),
        other => panic!("a listing answered {other:?}"),
    }
}

/// Set the queue's byte budget, its other settings as they are.
async fn set_max_bytes(e: &super::ShardEngine, max_queued_bytes: u64) {
    let cfg = QueueConfig::default();
    let update = NqOp::Update {
        group: cfg.group,
        max_queued_bytes,
        max_queued_messages: cfg.max_queued_messages,
    };
    op(e, update).await.unwrap();
}

async fn redrive(e: &super::ShardEngine, offset: u64) -> (u64, u64, bool) {
    let redrive = NqOp::Redrive {
        offsets: vec![offset],
        all: false,
        limit: 0,
    };
    match op(e, redrive).await.unwrap() {
        NqOut::Redriven {
            redriven,
            remaining,
            capacity,
        } => (redriven, remaining, capacity),
        other => panic!("redrive answered {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_queue_seeds_its_jitter_from_its_hash_and_a_change_advances_its_clock() {
    let (e, db) = engine("nq-jitter-clock").await;
    create(&e, QueueConfig::default()).await;
    let created = state(&db).await;
    assert_eq!(created.jitter, u64::from_le_bytes([9; 8]) | 1);
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    assert!(state(&db).await.clock > created.clock, "the clock advanced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_send_fills_the_byte_budget_exactly_and_no_further() {
    let (e, db, c) = clocked("nq-send-bytes", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("abc")])).await.unwrap();
    op(&e, send_op(vec![msg("1234567")])).await.unwrap();
    // The budget counts stored pages: set it to exactly the two sent.
    let full = stored(&db, 0).await + stored(&db, 1).await;
    set_max_bytes(&e, full).await;
    let s = state(&db).await;
    assert_eq!((s.queued_bytes, s.counters.sent), (full, 2));
    let tail = db
        .get(crate::shard::queue_tail_key(&HASH))
        .await
        .unwrap()
        .unwrap();
    let tail = super::decode_tail_for_tests(&tail).unwrap();
    assert_eq!((tail.next, tail.logical, tail.ts), (2, 10, c));
    let appended = e.stats_appended.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(appended, 2, "each message is one appended record");
    assert_eq!(
        op(&e, send_op(vec![msg("x")])).await.unwrap_err(),
        NqRefusal::QueueFull
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_send_fills_the_dedup_budget_and_schedules_up_to_retention_exactly() {
    let cfg = QueueConfig {
        max_dedup_entries: 2,
        ..QueueConfig::default()
    };
    let (e, _db, c) = clocked("nq-send-dedup", cfg).await;
    let tagged = |tag: u8| SendMessage {
        dedup_tag: Some([tag; 16]),
        ..msg("m")
    };
    op(&e, send_op(vec![tagged(1)])).await.unwrap();
    op(&e, send_op(vec![tagged(2)])).await.unwrap();
    assert_eq!(
        op(&e, send_op(vec![tagged(3)])).await.unwrap_err(),
        NqRefusal::DedupFull
    );
    let at = |deliver_at: i64| SendMessage {
        deliver_at: Some(deliver_at),
        ..msg("later")
    };
    let NqOut::Sent(sent) = op(&e, send_op(vec![at(c + RETENTION)])).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        sent[0].available_at,
        c + RETENTION,
        "retention away is allowed"
    );
    assert_eq!(
        op(&e, send_op(vec![at(c + RETENTION + 1)]))
            .await
            .unwrap_err(),
        NqRefusal::DeliverAtBeyondRetention
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_redrive_reserves_a_message_slot_and_stops_at_a_full_queue() {
    let cfg = QueueConfig {
        max_queued_messages: 2,
        ..QueueConfig::default()
    };
    let (e, db, _c) = clocked("nq-redrive-count", cfg).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    fail_claimed(&e, 2).await;
    op(&e, send_op(vec![msg("c")])).await.unwrap();
    assert_eq!(
        redrive(&e, 0).await,
        (1, 1, false),
        "the second slot is free"
    );
    assert_eq!(state(&db).await.queued_messages, 2);
    assert_eq!(redrive(&e, 1).await, (0, 1, true), "a full queue refuses");
    assert_eq!(state(&db).await.counters.redriven, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_redrive_fills_the_byte_budget_exactly() {
    let (e, db, _c) = clocked("nq-redrive-bytes", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("1234567"), msg("abc")]))
        .await
        .unwrap();
    let (first, second) = (stored(&db, 0).await, stored(&db, 1).await);
    set_max_bytes(&e, first + second).await;
    let d = claim(&e, 1).await;
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    assert_eq!(state(&db).await.queued_bytes, second);
    assert_eq!(redrive(&e, 0).await, (1, 0, false), "both pages fit exactly");
    let s = state(&db).await;
    assert_eq!((s.queued_messages, s.queued_bytes), (2, first + second));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn failed_and_peeked_messages_page_by_offset_with_a_cursor_only_on_a_full_page() {
    let (e, db, _c) = clocked("nq-pages", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    fail_claimed(&e, 3).await;
    assert_eq!(state(&db).await.counters.failed, 3);
    let failed = |after| NqOp::ListFailed { after, limit: 2 };
    assert_eq!(page(&e, failed(None)).await, (vec![0, 1], Some(1)));
    assert_eq!(page(&e, failed(Some(1))).await, (vec![2], None));
    let peek = |after| NqOp::Peek { after, limit: 2 };
    assert_eq!(page(&e, peek(None)).await, (vec![0, 1], Some(1)));
    assert_eq!(page(&e, peek(Some(1))).await, (vec![2], None));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_takes_due_retries_within_the_byte_limit_and_never_early_ones() {
    let (e, db, _c) = clocked("nq-claim-retries", QueueConfig::default()).await;
    let body = |n: usize| msg(&"x".repeat(n));
    op(&e, send_op(vec![body(100), body(100), body(200)]))
        .await
        .unwrap();
    // One small page fits `one`; both small ones fit `two` exactly; the
    // large one is bigger than `one`.
    let one = stored(&db, 0).await;
    let two = one + stored(&db, 1).await;
    assert!(stored(&db, 2).await > one);
    let take = |max_bytes: u64| NqOp::Claim {
        max: 10,
        max_bytes,
        lease_ms: 30_000,
    };
    let offsets = |o: NqOut| match o {
        NqOut::Claimed { deliveries, .. } => deliveries
            .iter()
            .map(|v| (v.offset, v.lease_gen))
            .collect::<Vec<_>>(),
        other => panic!("claim answered {other:?}"),
    };
    let retry = |got: &[(u64, u32)], delay_ms: i64| {
        let kind = SettleKind::Retry {
            delay_ms: Some(delay_ms),
        };
        settle(got.iter().map(|(o, g)| (*o, *g, kind)).collect())
    };
    let all = offsets(op(&e, take(1 << 20)).await.unwrap());
    op(&e, retry(&all, 0)).await.unwrap();
    let got = offsets(op(&e, take(one)).await.unwrap());
    assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), vec![0]);
    op(&e, retry(&got, 0)).await.unwrap();
    let got = offsets(op(&e, take(two)).await.unwrap());
    assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), vec![0, 1]);
    op(&e, retry(&got, 10)).await.unwrap();
    // Only the large retry is due: alone, it passes a smaller limit.
    let got = offsets(op(&e, take(one)).await.unwrap());
    assert_eq!(got.iter().map(|g| g.0).collect::<Vec<_>>(), vec![2]);
    op(&e, retry(&got, 10)).await.unwrap();
    assert!(offsets(op(&e, take(1 << 20)).await.unwrap()).is_empty());
}

/// The offsets a claim of up to ten messages within `max_bytes` leases.
async fn claim_within(e: &super::ShardEngine, max_bytes: u64) -> Vec<u64> {
    let take = NqOp::Claim {
        max: 10,
        max_bytes,
        lease_ms: 30_000,
    };
    match op(e, take).await.unwrap() {
        NqOut::Claimed { deliveries, .. } => deliveries.iter().map(|v| v.offset).collect(),
        other => panic!("claim answered {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_leases_a_first_untaken_message_larger_than_its_byte_limit_alone() {
    let (e, db, _c) = clocked("nq-claim-first-big", QueueConfig::default()).await;
    let body = |n: usize| msg(&"x".repeat(n));
    op(&e, send_op(vec![body(300), body(1)])).await.unwrap();
    let limit = stored(&db, 0).await - 1;
    assert_eq!(
        claim_within(&e, limit).await,
        vec![0],
        "one message is always leased, and it fills the limit"
    );
    assert_eq!(claim_within(&e, limit).await, vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_of_untaken_messages_fills_its_byte_limit_exactly_and_no_further() {
    let (e, db, _c) = clocked("nq-claim-new-bytes", QueueConfig::default()).await;
    let body = |n: usize| msg(&"x".repeat(n));
    let sizes = [100, 100, 100, 99, 2];
    op(&e, send_op(sizes.into_iter().map(body).collect()))
        .await
        .unwrap();
    let limit = stored(&db, 0).await + stored(&db, 1).await;
    assert_eq!(
        stored(&db, 2).await + stored(&db, 3).await,
        limit - 1,
        "the next two pages are one byte short of the limit"
    );
    assert_eq!(claim_within(&e, limit).await, vec![0, 1], "exactly the limit");
    assert_eq!(
        claim_within(&e, limit).await,
        vec![2, 3],
        "a byte short; the third would pass the limit"
    );
    assert_eq!(claim_within(&e, limit).await, vec![4], "the frontier stayed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_claim_takes_the_message_at_the_frontier_and_skips_one_past_its_retention_end() {
    let (e, db, c) = clocked("nq-claim-meta", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    assert_eq!(claim(&e, 1).await.len(), 1);
    assert_eq!(delivery(&db, 0).await.state, DState::Leased);
    assert_eq!(state(&db).await.frontier, 1);
    // The second message's retention ends: a claim skips it into the gap.
    set_clock(&db, c + RETENTION).await;
    assert!(claim(&e, 1).await.is_empty());
    let s = state(&db).await;
    assert_eq!(
        (s.gap_count, s.counters.expired, s.queued_messages, s.frontier),
        (1, 1, 1, 2)
    );
    let page = crate::native_queue::keys::page(&HASH, 1);
    assert!(
        db.get(page).await.unwrap().is_none(),
        "the expired message's page is deleted"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn settles_discard_and_retry_up_to_the_cycle_deadline_exactly() {
    let (e, db, c) = clocked("nq-settle-retry", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    let d = claim(&e, 2).await;
    let retry = SettleKind::Retry {
        delay_ms: Some(RETENTION),
    };
    op(
        &e,
        settle(vec![
            (0, d[0].lease_gen, SettleKind::Discard),
            (1, d[1].lease_gen, retry),
        ]),
    )
    .await
    .unwrap();
    let s = state(&db).await;
    assert_eq!(
        (
            s.counters.discarded,
            s.counters.retried,
            s.retrying,
            s.failed
        ),
        (1, 1, 1, 0),
        "a retry due at the cycle deadline is still a retry"
    );
    let row = delivery(&db, 1).await;
    assert_eq!((row.state, row.due_at), (DState::Retrying, c + RETENTION));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_policy_retry_waits_its_jittered_backoff_and_fails_once_attempts_are_spent() {
    let mut cfg = QueueConfig::default();
    cfg.group.max_attempts = 2;
    cfg.group.backoff.initial_ms = 10_000;
    let (e, db, c) = clocked("nq-policy-retry", cfg.clone()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let mut s = state(&db).await;
    let delay = crate::native_queue::backoff_ms(&cfg.group.backoff, 1, s.next_jitter());
    assert!(delay > 0, "the jittered backoff of this queue is {delay}");
    let d = claim(&e, 1).await;
    let policy = SettleKind::Retry { delay_ms: None };
    op(&e, settle(vec![(0, d[0].lease_gen, policy)]))
        .await
        .unwrap();
    assert_eq!(delivery(&db, 0).await.due_at, c + delay);
    set_clock(&db, c + delay).await;
    let d = claim(&e, 1).await;
    assert_eq!(d[0].attempt, 2);
    op(&e, settle(vec![(0, d[0].lease_gen, policy)]))
        .await
        .unwrap();
    let row = delivery(&db, 0).await;
    assert_eq!(row.state, DState::Failed);
    assert_eq!(row.cause.map(|c| c.name()), Some("exhausted"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn debt_ages_from_its_first_deletion_and_a_pass_clears_what_it_captured() {
    let (e, db, c) = clocked("nq-debt", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("bb")])).await.unwrap();
    // An ack deletes the page and the rows: the page's bytes and the row
    // charge.
    let a = stored(&db, 0).await + 256;
    let bb = stored(&db, 1).await + 256;
    let d = claim(&e, 2).await;
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    let first = state(&db).await;
    assert_eq!((first.debt_bytes, first.debt_oldest_at), (a, c));
    set_clock(&db, c + 1_000).await;
    op(&e, settle(vec![(1, d[1].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    let both = state(&db).await;
    assert_eq!((both.debt_bytes, both.debt_oldest_at), (a + bb, c));
    // A pass that captured only the first deletion leaves the second, aged
    // from no earlier than the capture.
    let partial = NqOp::ClearDebt {
        generation: first.debt_generation,
        bytes: a,
        captured_at: c + 500,
    };
    assert_eq!(op(&e, partial).await.unwrap().count(), bb);
    let s = state(&db).await;
    assert_eq!((s.debt_bytes, s.debt_oldest_at), (bb, c + 500));
    let whole = NqOp::ClearDebt {
        generation: both.debt_generation,
        bytes: bb,
        captured_at: c + 2_000,
    };
    assert_eq!(op(&e, whole).await.unwrap().count(), 0);
    let s = state(&db).await;
    assert_eq!((s.debt_bytes, s.debt_oldest_at), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_retry_scheduled_later_is_not_claimed_before_its_time_nor_failed_by_a_sweep() {
    let (e, db, c) = clocked("nq-retry-later", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim(&e, 1).await;
    let later = SettleKind::Retry {
        delay_ms: Some(HOUR_MS),
    };
    op(&e, settle(vec![(0, d[0].lease_gen, later)]))
        .await
        .unwrap();
    assert!(claim(&e, 1).await.is_empty(), "an hour early");
    set_clock(&db, c + HOUR_MS).await;
    // Due but inside its cycle: the sweep leaves it for a claim.
    op(&e, NqOp::Sweep).await.unwrap();
    let s = state(&db).await;
    assert_eq!((s.retrying, s.failed), (1, 0));
    assert_eq!(claim(&e, 1).await[0].attempt, 2);
}
