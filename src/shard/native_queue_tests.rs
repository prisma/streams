//! Native queue engine (docs/QUEUES-DESIGN.md §5-§8) through the real
//! committer over an in-memory store: every assertion reads the replies and
//! the stored rows the operations left.
#![cfg(test)]
use super::*;
use crate::native_queue::rows::StateRow;
use crate::native_queue::{
    NqOp, NqOut, NqRefusal, QueueConfig, SendMessage, SettleItem, SettleKind, SettleOutcome, Token,
    keys,
};

const HASH: [u8; 16] = [9; 16];

/// The engine, its database, and the absorber receiver the test must keep:
/// a dropped receiver closes the engine.
async fn engine(name: &str) -> (Arc<ShardEngine>, Arc<Db>, mpsc::Receiver<AbsorbSignal>) {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder(name, store.clone()).build().await.unwrap());
    let (tx, rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        name.to_owned(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    (engine, db, rx)
}

async fn op(e: &ShardEngine, op: NqOp) -> Result<NqOut, NqRefusal> {
    e.submit_native_queue(HASH, op).await
}

fn msg(body: &str) -> SendMessage {
    SendMessage {
        envelope: bytes::Bytes::from(body.to_owned()),
        deliver_at: None,
        dedup_tag: None,
    }
}

fn send_op(msgs: Vec<SendMessage>) -> NqOp {
    NqOp::Send {
        messages: msgs,
        subkey: [3; 32],
        key_version: 1,
    }
}

async fn create(e: &ShardEngine, cfg: QueueConfig) {
    let out = op(e, NqOp::Create { cfg }).await.unwrap();
    assert!(matches!(out, NqOut::Config { created: true, .. }));
}

async fn state(db: &Db) -> StateRow {
    StateRow::decode(&db.get(keys::state(&HASH)).await.unwrap().unwrap()).unwrap()
}

async fn claim(e: &ShardEngine, max: usize) -> Vec<crate::native_queue::DeliveryView> {
    match op(
        e,
        NqOp::Claim {
            max,
            max_bytes: 1 << 20,
            lease_ms: 30_000,
        },
    )
    .await
    .unwrap()
    {
        NqOut::Claimed { deliveries, .. } => deliveries,
        other => panic!("claim answered {other:?}"),
    }
}

fn settle(items: Vec<(u64, u32, SettleKind)>) -> NqOp {
    NqOp::Settle {
        items: items
            .into_iter()
            .map(|(offset, lease_gen, kind)| SettleItem {
                token: Token { offset, lease_gen },
                kind,
                code: None,
            })
            .collect(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sent_message_is_claimed_acked_and_its_rows_and_page_are_deleted() {
    let (e, db, _absorb) = engine("nq-ack").await;
    create(&e, QueueConfig::default()).await;
    let NqOut::Sent(sent) = op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        sent.iter().map(|s| s.offset).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let got = claim(&e, 2).await;
    assert_eq!(
        got.iter()
            .map(|d| (d.offset, d.attempt, d.lease_gen))
            .collect::<Vec<_>>(),
        vec![(0, 1, 1), (1, 1, 1)]
    );
    let NqOut::Settled(r) = op(
        &e,
        settle(vec![
            (0, 1, SettleKind::Ack),
            (1, 1, SettleKind::Retry { delay_ms: Some(0) }),
        ]),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert!(r.iter().all(|r| r.outcome == SettleOutcome::Ok));
    // The acked message's page and rows are gone; the others stay.
    assert!(
        db.get(crate::crypto_page::shard_page_key(&HASH, 0))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.get(keys::by_offset(&HASH, keys::META, 0))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.get(crate::crypto_page::shard_page_key(&HASH, 2))
            .await
            .unwrap()
            .is_some()
    );
    // The retried message comes back with its second attempt, beside the third.
    let again = claim(&e, 10).await;
    let mut seen: Vec<_> = again.iter().map(|d| (d.offset, d.attempt)).collect();
    seen.sort_unstable();
    assert_eq!(seen, vec![(1, 2), (2, 1)]);
    // A replay of the first settle is stale and changes nothing.
    let NqOut::Settled(r) = op(
        &e,
        settle(vec![(0, 1, SettleKind::Ack), (1, 1, SettleKind::Ack)]),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert!(r.iter().all(|r| r.outcome == SettleOutcome::Stale));
    let s = state(&db).await;
    assert_eq!(
        (
            s.queued_messages,
            s.leased,
            s.counters.acked,
            s.counters.stale
        ),
        (2, 2, 1, 2)
    );
    assert!(
        s.debt_bytes > 0,
        "the deleting batch records collection debt"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_tail_is_absorbed_in_place_and_never_marked_for_absorption() {
    let (e, db, _absorb) = engine("nq-tail").await;
    create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    let tail = decode_tail_for_tests(&db.get(tail_key(&HASH)).await.unwrap().unwrap()).unwrap();
    assert_eq!(
        (
            tail.next,
            tail.absorbed,
            tail.unabsorbed_bytes,
            tail.trim_safe_to
        ),
        (2, 2, 0, 0)
    );
    assert!(
        db.get(dirty_key(&HASH)).await.unwrap().is_none(),
        "no dirty mark: the absorber never sees a queue"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_claim_writes_nothing() {
    let (e, db, _absorb) = engine("nq-empty").await;
    create(&e, QueueConfig::default()).await;
    let before = db.get(keys::state(&HASH)).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert!(claim(&e, 10).await.is_empty());
    assert_eq!(db.get(keys::state(&HASH)).await.unwrap(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_duplicate_dedup_key_returns_the_original_and_the_budget_refuses_past_it() {
    let (e, _db, _absorb) = engine("nq-dedup").await;
    let cfg = QueueConfig {
        max_queued_messages: 2,
        ..QueueConfig::default()
    };
    create(&e, cfg).await;
    let tagged = |b: &str| SendMessage {
        dedup_tag: Some([5; 16]),
        ..msg(b)
    };
    let NqOut::Sent(first) = op(&e, send_op(vec![tagged("x")])).await.unwrap() else {
        panic!()
    };
    let NqOut::Sent(dup) = op(&e, send_op(vec![tagged("y")])).await.unwrap() else {
        panic!()
    };
    assert_eq!((dup[0].offset, dup[0].duplicate), (first[0].offset, true));
    op(&e, send_op(vec![msg("b")])).await.unwrap();
    assert_eq!(
        op(&e, send_op(vec![msg("c")])).await.unwrap_err(),
        NqRefusal::QueueFull
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_attempts_fail_keylessly_and_redrive_starts_a_new_cycle() {
    let (e, db, _absorb) = engine("nq-fail").await;
    let mut cfg = QueueConfig::default();
    cfg.group.max_attempts = 2;
    create(&e, cfg).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    for lg in [1u32, 3] {
        let d = claim(&e, 1).await;
        assert_eq!(d[0].lease_gen, lg);
        op(
            &e,
            settle(vec![(0, lg, SettleKind::Retry { delay_ms: Some(0) })]),
        )
        .await
        .unwrap();
    }
    let NqOut::Deliveries { items, .. } = op(
        &e,
        NqOp::ListFailed {
            after: None,
            limit: 10,
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].failure.as_ref().unwrap().cause, "exhausted");
    assert_eq!(
        state(&db).await.queued_messages,
        0,
        "a failed message leaves the admission budget"
    );
    let NqOut::Redriven { redriven, .. } = op(
        &e,
        NqOp::Redrive {
            offsets: vec![0],
            all: false,
            limit: 0,
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(redriven, 1);
    let d = claim(&e, 1).await;
    assert_eq!((d[0].offset, d[0].cycle, d[0].attempt), (0, 2, 1));
    op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap();
    let s = state(&db).await;
    assert_eq!(
        (s.failed, s.queued_messages, s.counters.redriven),
        (0, 0, 1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_lease_is_moved_by_the_sweep_and_a_late_extend_is_refused() {
    let (e, db, _absorb) = engine("nq-lease").await;
    create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let d = match op(
        &e,
        NqOp::Claim {
            max: 1,
            max_bytes: 1 << 20,
            lease_ms: 1_000,
        },
    )
    .await
    .unwrap()
    {
        NqOut::Claimed { deliveries, .. } => deliveries,
        _ => panic!(),
    };
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let NqOut::Settled(r) = op(
        &e,
        settle(vec![(
            0,
            d[0].lease_gen,
            SettleKind::Extend { lease_ms: 5_000 },
        )]),
    )
    .await
    .unwrap() else {
        panic!()
    };
    // Refused either way: `expired` while the lease still stands past its
    // deadline, `stale` once the roster-driven sweeper has moved it (it
    // runs on its own and wins under load, as on CI's runners).
    assert!(
        matches!(r[0].outcome, SettleOutcome::Expired | SettleOutcome::Stale),
        "a late extend is refused: {:?}",
        r[0].outcome
    );
    op(&e, NqOp::Sweep).await.unwrap();
    let s = state(&db).await;
    assert_eq!((s.leased, s.retrying), (0, 1));
    let NqOut::Settled(r) = op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        r[0].outcome,
        SettleOutcome::Stale,
        "the swept lease's token no longer settles"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_queue_leaves_no_row_and_no_page() {
    let (e, db, _absorb) = engine("nq-delete").await;
    create(&e, QueueConfig::default()).await;
    op(
        &e,
        send_op((0..20).map(|i| msg(&format!("m{i}"))).collect()),
    )
    .await
    .unwrap();
    claim(&e, 5).await;
    op(&e, NqOp::BeginDelete).await.unwrap();
    assert_eq!(
        op(&e, send_op(vec![msg("late")])).await.unwrap_err(),
        NqRefusal::Deleting
    );
    // 20 messages own about 100 rows: 7 a step finishes well inside 100
    // steps, and a step that never finishes fails instead of hanging.
    let mut done = false;
    for _ in 0..100 {
        match op(&e, NqOp::DeleteStep { max_rows: 7 }).await.unwrap() {
            NqOut::DeleteStepped { remaining: true } => {}
            NqOut::DeleteStepped { remaining: false } => {
                done = true;
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(done, "the delete never finished");
    let mut prefix = HASH.to_vec();
    prefix.push(keys::TAG);
    let mut rows = db.scan_prefix(&prefix[..], ..).await.unwrap();
    assert!(rows.next().await.unwrap().is_none(), "no queue row remains");
    for off in 0..20u64 {
        assert!(
            db.get(crate::crypto_page::shard_page_key(&HASH, off))
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(db.get(keys::roster(&HASH)).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_roster_driven_sweeper_moves_an_expired_lease_without_any_request() {
    let (e, db, _absorb) = engine("nq-sweeper").await;
    create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    match op(
        &e,
        NqOp::Claim {
            max: 1,
            max_bytes: 1 << 20,
            lease_ms: 1_000,
        },
    )
    .await
    .unwrap()
    {
        NqOut::Claimed { deliveries, .. } => assert_eq!(deliveries.len(), 1),
        other => panic!("{other:?}"),
    }
    assert!(
        db.get(keys::roster(&HASH)).await.unwrap().is_some(),
        "a queue with work is on the roster"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let s = state(&db).await;
        if s.leased == 0 && s.retrying == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the sweeper never moved the expired lease: {s:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscribed_queue_with_due_work_wakes_its_receiver_with_backoff_until_a_useful_claim() {
    let (e, db, _absorb) = engine("nq-wake").await;
    create(&e, QueueConfig::default()).await;
    let wake = |o: NqOut| match o {
        NqOut::Swept { wake, .. } => wake,
        other => panic!("{other:?}"),
    };
    op(
        &e,
        NqOp::SetWakeup {
            url: Some("https://example.com/w".into()),
            name: "q".into(),
        },
    )
    .await
    .unwrap();
    assert!(
        wake(op(&e, NqOp::Sweep).await.unwrap()).is_none(),
        "nothing due, nothing sent"
    );
    op(&e, send_op(vec![msg("a"), msg("b")])).await.unwrap();
    // The explicit sweep or the roster-driven sweeper, whichever runs
    // first, sends the first notification; then the backoff holds.
    let explicit = wake(op(&e, NqOp::Sweep).await.unwrap());
    let s = state(&db).await;
    assert!(s.last_notified_at > 0, "due work wakes the receiver: {s:?}");
    if let Some(first) = explicit {
        assert_eq!((first.name.as_str(), first.due, first.version), ("q", 2, 1));
    }
    assert!(
        wake(op(&e, NqOp::Sweep).await.unwrap()).is_none(),
        "within the backoff, no second notification"
    );
    // A useful claim resets the backoff; the remaining due message wakes again after it.
    assert_eq!(claim(&e, 1).await.len(), 1);
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    // The explicit sweep or the roster-driven sweeper sends it: either way
    // the durable notification time passes the useful claim.
    op(&e, NqOp::Sweep).await.unwrap();
    let s = state(&db).await;
    assert!(
        s.last_notified_at > s.last_useful_claim_at,
        "a drained receiver with backlog is woken again: {s:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_deletes_untaken_and_held_messages_and_discard_deletes_only_failed_ones() {
    let (e, db, _absorb) = engine("nq-cancel").await;
    let mut cfg = QueueConfig::default();
    cfg.group.max_attempts = 1;
    create(&e, cfg).await;
    op(&e, send_op(vec![msg("a"), msg("b"), msg("c")]))
        .await
        .unwrap();
    let held = claim(&e, 1).await;
    assert_eq!(
        op(
            &e,
            NqOp::Cancel {
                offsets: vec![0, 1, 99]
            }
        )
        .await
        .unwrap()
        .count(),
        2
    );
    let NqOut::Settled(r) = op(&e, settle(vec![(0, held[0].lease_gen, SettleKind::Ack)]))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        r[0].outcome,
        SettleOutcome::Stale,
        "a cancelled delivery's holder is stale"
    );
    let d = claim(&e, 5).await;
    assert_eq!(d.iter().map(|v| v.offset).collect::<Vec<_>>(), vec![2]);
    // Discard refuses a leased message and deletes a failed one.
    assert_eq!(
        op(&e, NqOp::Discard { offsets: vec![2] })
            .await
            .unwrap()
            .count(),
        0
    );
    op(&e, settle(vec![(2, d[0].lease_gen, SettleKind::Fail)]))
        .await
        .unwrap();
    assert_eq!(
        op(&e, NqOp::Discard { offsets: vec![2] })
            .await
            .unwrap()
            .count(),
        1
    );
    let s = state(&db).await;
    assert_eq!(
        (
            s.queued_messages,
            s.failed,
            s.leased,
            s.counters.cancelled,
            s.counters.discarded
        ),
        (0, 0, 0, 2, 1)
    );
    for off in 0..3u64 {
        assert!(
            db.get(keys::by_offset(&HASH, keys::META, off))
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_gives_the_attempt_back_twice_then_counts_and_extend_moves_the_deadline() {
    let (e, db, _absorb) = engine("nq-release").await;
    create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    let mut attempts = Vec::new();
    for _ in 0..3 {
        // A release is due a second later.
        tokio::time::sleep(std::time::Duration::from_millis(1_050)).await;
        let d = claim(&e, 1).await;
        attempts.push(d[0].attempt);
        op(&e, settle(vec![(0, d[0].lease_gen, SettleKind::Release)]))
            .await
            .unwrap();
    }
    assert_eq!(
        attempts,
        vec![1, 1, 1],
        "the first two releases give the attempt back"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_050)).await;
    let s = state(&db).await;
    assert_eq!(
        (s.retrying, s.counters.retried),
        (1, 1),
        "the third release counts as a retry"
    );
    let mut d = Vec::new();
    for _ in 0..50 {
        d = claim(&e, 1).await;
        if !d.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(!d.is_empty(), "the retried message never came back");
    assert_eq!(d[0].attempt, 2);
    let before = d[0].lease_deadline;
    let NqOut::Settled(r) = op(
        &e,
        settle(vec![(
            0,
            d[0].lease_gen,
            SettleKind::Extend { lease_ms: 120_000 },
        )]),
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(r[0].outcome, SettleOutcome::Ok);
    let NqOut::Message(v) = op(&e, NqOp::Message { offset: 0 }).await.unwrap() else {
        panic!()
    };
    assert!(
        v.lease_deadline > before + 60_000,
        "the extend moved the deadline"
    );
    assert_eq!(v.lease_gen, d[0].lease_gen, "an extend keeps the lease");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicit_retry_past_retention_fails_and_a_scheduled_message_waits_for_its_time() {
    let (e, db, _absorb) = engine("nq-retention").await;
    let cfg = QueueConfig {
        retention_ms: crate::native_queue::HOUR_MS,
        dedup_window_ms: 0,
        ..QueueConfig::default()
    };
    create(&e, cfg).await;
    let later = now_ms() + 1_500;
    let scheduled = SendMessage {
        deliver_at: Some(later),
        ..msg("later")
    };
    op(&e, send_op(vec![msg("now"), scheduled])).await.unwrap();
    let d = claim(&e, 10).await;
    assert_eq!(
        d.iter().map(|v| v.offset).collect::<Vec<_>>(),
        vec![0],
        "the scheduled one is not due"
    );
    let two_hours = 2 * crate::native_queue::HOUR_MS;
    op(
        &e,
        settle(vec![(
            0,
            d[0].lease_gen,
            SettleKind::Retry {
                delay_ms: Some(two_hours),
            },
        )]),
    )
    .await
    .unwrap();
    let NqOut::Deliveries { items, .. } = op(
        &e,
        NqOp::ListFailed {
            after: None,
            limit: 10,
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert_eq!(
        items[0].failure.as_ref().unwrap().cause,
        "retention",
        "an explicit delay is never shortened"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_600)).await;
    let d = claim(&e, 10).await;
    assert_eq!(d.iter().map(|v| v.offset).collect::<Vec<_>>(), vec![1]);
    assert_eq!(state(&db).await.failed, 1);
    let far = SendMessage {
        deliver_at: Some(now_ms() + two_hours),
        ..msg("far")
    };
    assert_eq!(
        op(&e, send_op(vec![far])).await.unwrap_err(),
        NqRefusal::DeliverAtBeyondRetention
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_claim_respects_bytes_in_flight_and_pause() {
    let (e, db, _absorb) = engine("nq-limits").await;
    let mut cfg = QueueConfig::default();
    cfg.group.max_in_flight = 3;
    create(&e, cfg).await;
    op(
        &e,
        send_op((0..6).map(|i| msg(&format!("{i:0>100}"))).collect()),
    )
    .await
    .unwrap();
    let small = match op(
        &e,
        NqOp::Claim {
            max: 10,
            max_bytes: 150,
            lease_ms: 30_000,
        },
    )
    .await
    .unwrap()
    {
        NqOut::Claimed { deliveries, .. } => deliveries,
        _ => panic!(),
    };
    assert_eq!(
        small.len(),
        1,
        "a byte limit still admits one message, and no second past it"
    );
    assert_eq!(
        claim(&e, 10).await.len(),
        2,
        "maxInFlight 3 leaves room for two more"
    );
    assert!(claim(&e, 10).await.is_empty());
    op(&e, NqOp::Pause { paused: true }).await.unwrap();
    match op(
        &e,
        NqOp::Claim {
            max: 10,
            max_bytes: 1 << 20,
            lease_ms: 30_000,
        },
    )
    .await
    .unwrap()
    {
        NqOut::Claimed { deliveries, paused } => assert!(deliveries.is_empty() && paused),
        _ => panic!(),
    }
    assert!(state(&db).await.paused);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_with_nothing_due_parks_the_roster_at_the_next_deadline() {
    let (e, db, _absorb) = engine("nq-roster").await;
    create(&e, QueueConfig::default()).await;
    op(&e, send_op(vec![msg("a")])).await.unwrap();
    op(&e, NqOp::Sweep).await.unwrap();
    let raw = db
        .get(keys::roster(&HASH))
        .await
        .unwrap()
        .expect("a queue with a message is on the roster");
    let at = i64::from_be_bytes(raw[..8].try_into().unwrap());
    // The only deadline is the untaken message's retention end (4 days):
    // the sweeper must not look at this queue again every second.
    assert!(
        at > now_ms() + 3 * crate::native_queue::DAY_MS,
        "roster deadline {at} is not the retention end"
    );
}
