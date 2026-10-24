//! The roster and the maintenance sweep (docs/QUEUES-DESIGN.md §6.8, §8,
//! §9.2) on a queue whose clock the test sets: every deadline, roster value,
//! wake-up and expiry is asserted at its exact millisecond.
#![cfg(test)]
use super::{
    HASH, claim, clocked, edit_state, msg, op, roster, send_op, set_clock, settle, state, swept,
};
use crate::native_queue::rows::StateRow;
use crate::native_queue::{
    DAY_MS, HOUR_MS, NqOp, NqOut, QueueConfig, SendMessage, SettleKind, keys,
};

const RETENTION: i64 = 4 * DAY_MS;

/// One pending count, named, and how to set it alone.
type Pending = (&'static str, fn(&mut StateRow));

fn clear_counts(s: &mut StateRow) {
    s.queued_messages = 0;
    s.leased = 0;
    s.retrying = 0;
    s.failed = 0;
    s.dedup_entries = 0;
    s.deleting = false;
    s.debt_bytes = 0;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_roster_holds_a_queue_while_any_one_count_or_its_debt_is_pending() {
    let (e, db, _absorb, c) = clocked("nq-roster-counts", QueueConfig::default()).await;
    assert_eq!(
        roster(&db).await,
        None,
        "a new empty queue is off the roster"
    );
    let cases: [Pending; 7] = [
        ("queued", |s| s.queued_messages = 1),
        ("leased", |s| s.leased = 1),
        ("retrying", |s| s.retrying = 1),
        ("failed", |s| s.failed = 1),
        ("dedup", |s| s.dedup_entries = 1),
        ("deleting", |s| s.deleting = true),
        ("debt", |s| s.debt_bytes = 1),
    ];
    for (what, set) in cases {
        edit_state(&db, set).await;
        // A state change looks again now; BeginDelete is what a deleting
        // queue still accepts.
        let change = if what == "deleting" {
            NqOp::BeginDelete
        } else {
            NqOp::Pause { paused: false }
        };
        op(&e, change).await.unwrap();
        // No deadline floor accounts for a count set by hand: the queue is
        // looked at within the hour.
        assert_eq!(
            roster(&db).await,
            Some(c + HOUR_MS),
            "{what} alone keeps the queue"
        );
        edit_state(&db, clear_counts).await;
        op(&e, NqOp::Pause { paused: false }).await.unwrap();
        assert_eq!(roster(&db).await, None, "nothing pending after {what}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_that_moves_nothing_leaves_the_state_row_untouched() {
    // On the wall clock: a sweep that moved something would advance the
    // queue clock to now.
    let (e, db, _absorb) = super::engine("nq-idle-sweep").await;
    super::create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let before = db.get(keys::state(&HASH)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    op(&e, NqOp::Sweep).await.unwrap();
    assert_eq!(db.get(keys::state(&HASH)).await.unwrap(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_reports_the_exact_next_deadline_and_parks_the_roster_there() {
    let (e, db, _absorb, c) = clocked("nq-next-deadline", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    assert_eq!(
        swept(&e).await.0,
        Some(c + RETENTION),
        "untaken: retention end"
    );
    assert_eq!(roster(&db).await, Some(c + RETENTION));
    op(&e, NqOp::Get).await.unwrap();
    assert_eq!(
        roster(&db).await,
        Some(c + RETENTION),
        "a read leaves the roster where the sweep parked it"
    );
    let d = claim(&e, 1).await;
    assert_eq!(
        swept(&e).await.0,
        Some(c + 30_000),
        "leased: lease deadline"
    );
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    // Only debt remains: nothing time moves, so the roster looks in an hour.
    assert_eq!(swept(&e).await.0, Some(c + HOUR_MS));
    assert_eq!(roster(&db).await, Some(c + HOUR_MS));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscribed_queue_with_nothing_due_looks_again_in_a_minute() {
    let (e, db, _absorb, c) = clocked("nq-next-notify", QueueConfig::default()).await;
    op(
        &e,
        NqOp::SetWakeup {
            url: Some("https://example.com/w".into()),
            name: "q".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(swept(&e).await.0, None, "no message, no notification time");
    let later = SendMessage {
        deliver_at: Some(c + HOUR_MS),
        ..msg("later")
    };
    op(&e, send_op(vec![later])).await.unwrap();
    let (next, wake) = swept(&e).await;
    assert!(wake.is_none(), "nothing due, nothing sent");
    // Never notified and never claimed: a notification may go from the
    // epoch's first second on, which is long past; with nothing claimable
    // it waits a minute.
    assert_eq!(next, Some(c + 60_000));
    assert_eq!(roster(&db).await, Some(c + 60_000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wake_up_fires_at_its_backoff_exactly_and_doubles_it_while_unanswered() {
    let (e, db, _absorb, c) = clocked("nq-wake-backoff", QueueConfig::default()).await;
    let url = Some("https://example.com/w".to_owned());
    let subscribe = NqOp::SetWakeup {
        url,
        name: "q".into(),
    };
    op(&e, subscribe).await.unwrap();
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    set_clock(&db, c + 500).await;
    let (next, wake) = swept(&e).await;
    let w = wake.expect("due work wakes the receiver");
    assert_eq!(
        (w.name.as_str(), w.due, w.version, w.oldest_due_age_ms),
        ("q", 2, 1, 500)
    );
    assert_eq!(next, Some(c + 1_500), "the next notification may go 1 s on");
    let s = state(&db).await;
    assert_eq!((s.last_notified_at, s.notify_backoff_ms), (c + 500, 1_000));
    set_clock(&db, c + 1_499).await;
    assert!(swept(&e).await.1.is_none(), "a millisecond early: nothing");
    set_clock(&db, c + 1_500).await;
    let w = swept(&e).await.1.expect("at the backoff exactly, again");
    assert_eq!(w.oldest_due_age_ms, 1_500);
    let s = state(&db).await;
    assert_eq!(
        (s.last_notified_at, s.notify_backoff_ms),
        (c + 1_500, 2_000)
    );
    // A useful claim in the notification's millisecond answers it: the
    // next one waits the base second again.
    assert_eq!(claim(&e, 1).await.len(), 1);
    set_clock(&db, c + 2_500).await;
    assert!(swept(&e).await.1.is_some(), "the rest is due a second on");
    assert_eq!(state(&db).await.notify_backoff_ms, 1_000);
    op(&e, NqOp::Pause { paused: true }).await.unwrap();
    set_clock(&db, c + 10_000).await;
    assert!(swept(&e).await.1.is_none(), "a paused queue wakes nobody");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_untaken_message_enters_the_gap_at_its_retention_end_exactly() {
    let (e, db, _absorb, c) = clocked("nq-untaken-expiry", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    set_clock(&db, c + RETENTION - 1).await;
    assert_eq!(swept(&e).await.0, Some(c + RETENTION));
    assert_eq!(state(&db).await.gap_count, 0, "a millisecond early: kept");
    set_clock(&db, c + RETENTION).await;
    assert_eq!(
        swept(&e).await.0,
        Some(c + RETENTION + HOUR_MS),
        "only debt is left"
    );
    let s = state(&db).await;
    assert_eq!(
        (s.gap_count, s.counters.expired, s.queued_messages),
        (1, 1, 0)
    );
    let meta = keys::by_offset(&HASH, keys::META, 0);
    assert!(!has(&db, meta).await, "the expired message is deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_message_is_discarded_at_its_failure_expiry_exactly() {
    let (e, db, _absorb, c) = clocked("nq-failure-expiry", QueueConfig::default()).await;
    let expiry = c + QueueConfig::default().failure_retention_ms;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim(&e, 1).await;
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    set_clock(&db, expiry - 1).await;
    assert_eq!(swept(&e).await.0, Some(expiry));
    assert_eq!(state(&db).await.failed, 1, "a millisecond early: kept");
    set_clock(&db, expiry).await;
    assert_eq!(
        swept(&e).await.0,
        Some(expiry + HOUR_MS),
        "only debt is left"
    );
    let s = state(&db).await;
    assert_eq!((s.failed, s.counters.discarded), (0, 1));
    let meta = keys::by_offset(&HASH, keys::META, 0);
    assert!(!has(&db, meta).await, "the discarded message is deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dedup_entry_is_deleted_at_its_window_end_exactly_and_a_younger_one_stays() {
    let (e, db, _absorb, c) = clocked("nq-dedup-expiry", QueueConfig::default()).await;
    let tagged = |tag: u8| SendMessage {
        dedup_tag: Some([tag; 16]),
        ..msg("m")
    };
    op(&e, send_op(vec![tagged(1)])).await.unwrap();
    set_clock(&db, c + 1_000).await;
    op(&e, send_op(vec![tagged(2)])).await.unwrap();
    set_clock(&db, c + DAY_MS).await;
    assert_eq!(
        swept(&e).await.0,
        Some(c + DAY_MS + 1_000),
        "the younger entry's window end is next"
    );
    assert_eq!(state(&db).await.dedup_entries, 1);
    assert!(!has(&db, keys::dedup(&HASH, &[1; 16])).await);
    assert!(!has(&db, keys::dedup_expiry(&HASH, c + DAY_MS, &[1; 16])).await);
    assert!(has(&db, keys::dedup(&HASH, &[2; 16])).await);
    assert!(has(&db, keys::dedup_expiry(&HASH, c + DAY_MS + 1_000, &[2; 16])).await);
}

async fn has(db: &slatedb::Db, key: Vec<u8>) -> bool {
    db.get(key).await.unwrap().is_some()
}

/// Each failed message's offset and failure cause, in offset order.
async fn failures(e: &super::ShardEngine) -> Vec<(u64, &'static str)> {
    let list = NqOp::ListFailed {
        after: None,
        limit: 10,
    };
    let NqOut::Deliveries { items, .. } = op(e, list).await.unwrap() else {
        panic!("a listing answered otherwise")
    };
    let cause = |v: &crate::native_queue::DeliveryView| v.failure.as_ref().map(|f| f.cause);
    items
        .iter()
        .map(|v| (v.offset, cause(v).unwrap_or("none")))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_due_at_its_cycle_deadline_fails_in_a_sweep_at_that_millisecond() {
    let (e, db, _absorb, c) = clocked("nq-retry-due-at-cycle", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim(&e, 1).await;
    let at_deadline = SettleKind::Retry {
        delay_ms: Some(RETENTION),
    };
    op(&e, settle(vec![(0, d[0].lease_gen, at_deadline)]))
        .await
        .unwrap();
    let f = state(&db).await.floors;
    assert_eq!(
        (f.retries, f.retry_cycle),
        (c + RETENTION, c + RETENTION),
        "due exactly at its cycle deadline"
    );
    set_clock(&db, c + RETENTION).await;
    swept(&e).await;
    let s = state(&db).await;
    assert_eq!(
        (s.retrying, s.failed, s.floors.retry_cycle),
        (0, 1, i64::MAX)
    );
    assert_eq!(failures(&e).await, vec![(0, "retention")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_that_runs_after_a_retrys_cycle_deadline_still_fails_it() {
    let (e, db, _absorb, c) = clocked("nq-retry-late-sweep", QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = claim(&e, 1).await;
    let later = SettleKind::Retry {
        delay_ms: Some(HOUR_MS),
    };
    op(&e, settle(vec![(0, d[0].lease_gen, later)]))
        .await
        .unwrap();
    assert_eq!(state(&db).await.floors.retry_cycle, c + RETENTION);
    // The sweeper comes five seconds late (a restart, a busy shard).
    set_clock(&db, c + RETENTION + 5_000).await;
    swept(&e).await;
    let s = state(&db).await;
    assert_eq!(
        (s.retrying, s.failed, s.floors.retry_cycle),
        (0, 1, i64::MAX)
    );
    assert_eq!(failures(&e).await, vec![(0, "retention")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_that_fills_its_step_looks_again_in_a_second() {
    let mut cfg = QueueConfig::default();
    cfg.group.max_batch = 1_000;
    let (e, db, _absorb, c) = clocked("nq-sweep-step", cfg).await;
    let n = crate::native_queue::SWEEP_STEP;
    op(&e, send_op((0..n).map(|_| msg("m")).collect()))
        .await
        .unwrap();
    let d = claim(&e, n).await;
    assert_eq!(d.len(), n);
    let retry = SettleKind::Retry {
        delay_ms: Some(HOUR_MS),
    };
    op(
        &e,
        settle(d.iter().map(|v| (v.offset, v.lease_gen, retry)).collect()),
    )
    .await
    .unwrap();
    // Every retry is still waiting at its cycle deadline: one full step
    // fails them all, and more may be due.
    set_clock(&db, c + RETENTION).await;
    assert_eq!(swept(&e).await.0, Some(c + RETENTION + 1_000));
    let s = state(&db).await;
    assert_eq!((s.retrying, s.failed), (0, u64::try_from(n).unwrap()));
    let NqOut::Deliveries { items, .. } = op(
        &e,
        NqOp::ListFailed {
            after: None,
            limit: 1,
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(items[0].failure.as_ref().unwrap().cause, "retention");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_and_retention_sweeps_that_fill_their_step_look_again_in_a_second() {
    let (e, db, _absorb, c) = clocked("nq-sweep-steps", QueueConfig::default()).await;
    let n = crate::native_queue::SWEEP_STEP;
    let tagged = (0..n).map(|i| SendMessage {
        dedup_tag: Some(u128::try_from(i).unwrap().to_be_bytes()),
        ..msg("m")
    });
    op(&e, send_op(tagged.collect())).await.unwrap();
    set_clock(&db, c + DAY_MS).await;
    assert_eq!(swept(&e).await.0, Some(c + DAY_MS + 1_000), "dedup step");
    assert_eq!(state(&db).await.dedup_entries, 0);
    set_clock(&db, c + RETENTION).await;
    let untaken = swept(&e).await.0;
    assert_eq!(untaken, Some(c + RETENTION + 1_000), "untaken step");
    assert_eq!(state(&db).await.gap_count, u64::try_from(n).unwrap());
}
