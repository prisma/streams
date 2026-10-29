//! The shard's unsealed ledger: its delta rule, its rebuild from the stored
//! tails, and the seal commits that retire it exactly (layout 6).
#![cfg(test)]
use super::*;
use crate::shard::seal::test_hooks::Hold;
use crate::shard::seal::{SealAdvance, SealCommit, SealError, Trigger};
use crate::spill::pending::{parse_pages, pending_key};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// R25-A: the delta rule. Retirement past the ledger is an ERROR —
/// clamping would hide the exact unit-divergence class this type
/// exists to prevent.
#[test]
fn apply_delta_is_checked_and_tracks_progress() {
    let m = ShardMaintenance::default();
    let m = m.apply_delta(1000, 0, 5_000).unwrap();
    assert_eq!(m.unabsorbed_frame_bytes, 1000);
    assert_eq!(m.backlog_started_ms, 5_000);
    assert_eq!(m.last_progress_ms, 5_000);
    assert_eq!(m.version, 1);

    // Later append: the backlog-start clock must NOT restart.
    let m = m.apply_delta(500, 0, 9_000).unwrap();
    assert_eq!(m.backlog_started_ms, 5_000, "backlog start must not reset");
    assert_eq!(m.last_progress_ms, 5_000, "an append is not progress");

    // Retirement refreshes the PROGRESS clock — the stall signal is
    // "time since durable progress", not "age of oldest record",
    // which stays permanently old under continuous traffic.
    let m = m.apply_delta(0, 600, 12_000).unwrap();
    assert_eq!(m.unabsorbed_frame_bytes, 900);
    assert_eq!(m.last_progress_ms, 12_000);
    assert_eq!(m.no_progress_secs(20_000), 8);

    // Full drain retires both clocks.
    let m = m.apply_delta(0, 900, 15_000).unwrap();
    assert_eq!(m.unabsorbed_frame_bytes, 0);
    assert_eq!(m.backlog_started_ms, 0);
    assert_eq!(m.no_progress_secs(99_000), 0);

    // Over-retirement is a loud error, never a silent clamp.
    assert!(
        ShardMaintenance::default().apply_delta(10, 11, 1).is_err(),
        "retiring more than exists must fail"
    );
}

/// The stall signal reads zero unless there is both a backlog and a
/// progress clock: a stored row from before any progress cannot prove
/// its age (the opener starts its clock), and an empty ledger has
/// nothing to stall.
#[test]
fn no_progress_needs_a_backlog_and_a_clock() {
    let unclocked = ShardMaintenance {
        unabsorbed_frame_bytes: 10,
        ..ShardMaintenance::default()
    };
    assert_eq!(unclocked.no_progress_secs(99_000), 0);
    let drained = ShardMaintenance {
        last_progress_ms: 5_000,
        ..ShardMaintenance::default()
    };
    assert_eq!(drained.no_progress_secs(99_000), 0);
    let stalled = ShardMaintenance {
        unabsorbed_frame_bytes: 10,
        last_progress_ms: 5_000,
        ..ShardMaintenance::default()
    };
    assert_eq!(stalled.no_progress_secs(99_000), 94);
}

/// KANI-047: the typed refusals keep the words the commit failure and
/// the logs carried while they were `anyhow` errors.
#[test]
fn maintenance_refusals_keep_their_words() {
    assert_eq!(
        ShardMaintenance::default()
            .apply_delta(10, 11, 1)
            .unwrap_err()
            .to_string(),
        "maintenance retirement exceeds backlog: retire=11 available=10"
    );
    let full = ShardMaintenance {
        unabsorbed_frame_bytes: u64::MAX,
        ..ShardMaintenance::default()
    };
    assert_eq!(
        full.apply_delta(1, 0, 1).unwrap_err().to_string(),
        "maintenance byte overflow"
    );
}

/// Layout 6: the ledger lives in memory only, and the open rebuilds it by
/// summing the stored stream tails' unsealed bytes. Tails holding 300 and
/// 400 unsealed bytes rebuild to 700 with both clocks started; a log with
/// no tail rebuilds to a zero ledger with no clock; a tail that does not
/// decode, or whose boundary is past its next offset, is left out (its
/// stream fails alone at its handle load), and a queue's tail is not a
/// stream's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_or_rebuild_covers_present_missing_and_corrupt() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let unsealed = |bytes: u64| {
        encode_tail(&TailFields {
            next: 10,
            absorbed: 4,
            unabsorbed_bytes: bytes,
            ..Default::default()
        })
    };

    // 1. Present tails: the rebuild sums their unsealed bytes.
    let db = crate::keyspace::db_builder("m1/shard", store.clone())
        .build()
        .await
        .unwrap();
    let mut wb = WriteBatch::new();
    wb.put(tail_key(&[1; 16]), unsealed(300));
    wb.put(tail_key(&[2; 16]), unsealed(400));
    let sealed = TailFields {
        next: 10,
        absorbed: 10,
        ..Default::default()
    };
    wb.put(tail_key(&[3; 16]), encode_tail(&sealed));
    db.write(wb).await.unwrap();
    let before = now_ms();
    let got = load_or_rebuild_maintenance(&db).await.unwrap();
    assert_eq!(got.unabsorbed_frame_bytes, 700, "rebuild sums tail gauges");
    for clock in [got.backlog_started_ms, got.last_progress_ms] {
        assert!(
            (before..=now_ms()).contains(&clock),
            "a rebuilt backlog starts both clocks: {got:?}"
        );
    }
    db.close().await.unwrap();

    // 2. No tail: nothing unsealed, no clock.
    let db = crate::keyspace::db_builder("m2/shard", store.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(
        load_or_rebuild_maintenance(&db).await.unwrap(),
        ShardMaintenance {
            version: 1,
            ..ShardMaintenance::default()
        }
    );
    db.close().await.unwrap();

    // 3. Corrupt tails are left out; a queue tail is not counted.
    let db = crate::keyspace::db_builder("m3/shard", store.clone())
        .build()
        .await
        .unwrap();
    let ahead = TailFields {
        next: 10,
        absorbed: 11,
        unabsorbed_bytes: 999,
        ..Default::default()
    };
    let mut wb = WriteBatch::new();
    wb.put(tail_key(&[1; 16]), unsealed(300));
    wb.put(tail_key(&[4; 16]), b"broken tail");
    wb.put(tail_key(&[5; 16]), encode_tail(&ahead));
    wb.put(queue_tail_key(&[6; 16]), unsealed(50));
    db.write(wb).await.unwrap();
    let got = load_or_rebuild_maintenance(&db).await.unwrap();
    assert_eq!(got.unabsorbed_frame_bytes, 300);
    db.close().await.unwrap();
}

// ---- seal retirement (release hold: the capacity run's one-off 500). A
// boundary advance that re-covered bytes an earlier one retired used to
// retire them again; once the stream ledger was smaller than the double
// count, the whole commit group was refused as "maintenance accounting
// diverged" and every append in it answered 500. In layout 6 the advance is
// a seal's commit, staged against the live tail: one whose snapshot no
// longer matches is refused whole and retires nothing.

type Answer = oneshot::Receiver<Result<AppendAck, AppendErr>>;
const ANSWER_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// An engine over a fault store whose WAL puts a test can hold, its sealer
/// open and driven only by the test (`seal_once`).
async fn rig(name: &str) -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>) {
    let store = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        0x52,
        crate::dst::FaultPlan::new(0, 0, 0),
    );
    let db = crate::keyspace::db_builder(name, store.clone() as Arc<dyn object_store::ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let engine = ShardEngine::start(
        name.into(),
        Arc::new(db),
        store.clone(),
        ShardConfig::default(),
        None,
        Default::default(),
    );
    engine.configure_seal(|s| s.auto = false);
    engine.seal_ready().await.unwrap();
    (engine, store)
}

/// One 100-byte record for `hash`; the receiver is its answer.
fn append_op(hash: [u8; 16]) -> (AppendReq, Answer) {
    let key = crate::crypto::StreamKey([7; 32]);
    let (resp, answer) = oneshot::channel();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: hash,
        entries: vec![Bytes::from(vec![0x5a; 100])],
        usage: crate::usage::counters(&hash),
        routing_key: String::new(),
        key_hash: [7; 16],
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: crate::crypto::derive_subkey(&key, &hash, "", 0),
        ts_hint_ms: None,
        seq: None,
        bytes: 100,
        finish: AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp,
    };
    (req, answer)
}

/// Append one record through the committer queue and wait for its answer.
async fn append(engine: &ShardEngine, hash: [u8; 16]) -> Result<AppendAck, AppendErr> {
    let (req, answer) = append_op(hash);
    assert!(engine.try_enqueue(req).is_ok(), "enqueue");
    tokio::time::timeout(ANSWER_WITHIN, answer)
        .await
        .unwrap()
        .unwrap()
}

/// Stored bytes of the stream's one-record pages [from, upto), every one of
/// which its pending row must hold.
async fn stored_bytes(engine: &ShardEngine, hash: &[u8; 16], from: u64, upto: u64) -> u64 {
    let raw = engine
        .db
        .get(pending_key(hash))
        .await
        .unwrap()
        .unwrap_or_default();
    let pages: Vec<(u64, Bytes)> = parse_pages(&raw)
        .unwrap()
        .into_iter()
        .filter(|(last, _)| (from..upto).contains(last))
        .collect();
    assert_eq!(
        pages.len() as u64,
        upto - from,
        "pages [{from}, {upto}) are not all pending"
    );
    pages.iter().map(|(_, page)| page.len() as u64).sum()
}

async fn applied(engine: &ShardEngine, hash: [u8; 16]) -> TailFields {
    let handle = engine.stream_handle(hash).await.unwrap();
    let state = handle.state.lock().unwrap();
    state.applied.clone()
}

/// The object a hand-built seal names: none was PUT.
fn made_up_object(engine: &ShardEngine) -> crate::spill::object::ObjectId {
    crate::spill::object::ObjectId {
        epoch: engine.writer_epoch,
        seq: u64::MAX,
    }
}

/// A seal's commit of `hash`'s `[from, upto)` (`bytes` sealed) as a seal
/// would submit it from a snapshot the test makes up, and the receiver of
/// its refusal.
fn seal_op(
    engine: &ShardEngine,
    hash: [u8; 16],
    window: std::ops::Range<u64>,
    bytes: u64,
) -> (CommitOp, oneshot::Receiver<String>) {
    let id = made_up_object(engine);
    let (resp, refusal) = oneshot::channel();
    let commit = SealCommit {
        dir_key: crate::spill::directory::directory_key(id),
        dir_value: Vec::new(),
        ref_key: crate::spill::directory::refcount_key(id),
        ref_value: Vec::new(),
        streams: vec![SealAdvance {
            hash,
            from: window.start,
            upto: window.end,
            bytes,
            snapshot_remainder: Vec::new(),
        }],
        resp,
    };
    (CommitOp::Seal(Box::new(commit)), refusal)
}

/// Commits a stale seal of `hash` beside one append in one group: the
/// append commits, and the seal is refused whole, writing no row of its
/// own.
async fn refused_beside_an_append(
    engine: &ShardEngine,
    hash: [u8; 16],
    stale: (CommitOp, oneshot::Receiver<String>),
) {
    let (seal, refusal) = stale;
    let (req, reply) = append_op(hash);
    engine.commit_group(vec![seal, CommitOp::Append(req)]).await;
    let answer = tokio::time::timeout(ANSWER_WITHIN, reply)
        .await
        .unwrap()
        .unwrap();
    assert!(
        answer.is_ok(),
        "an append co-grouped with a stale seal was refused: {answer:?}"
    );
    let why = refusal.await.expect("a refused seal is answered");
    assert!(why.contains("moved since its snapshot"), "{why}");
    let id = made_up_object(engine);
    for key in [
        crate::spill::directory::directory_key(id),
        crate::spill::directory::refcount_key(id),
    ] {
        assert!(
            engine.db.get(key).await.unwrap().is_none(),
            "a refused seal wrote its row"
        );
    }
}

/// Committer layer: a seal over [0, 4) commits; then, in one group, a seal
/// over [0, 8) whose snapshot predates it (it overlaps the boundary) and an
/// append. The stale seal is refused whole, retiring nothing, and the
/// append commits; the ledger then holds exactly the bytes of
/// [absorbed, next). The stream's next seal from the boundary retires
/// exactly what it seals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_advance_that_overlaps_the_boundary_retires_nothing_and_fails_no_append() {
    let (engine, _store) = rig("overlap-advance").await;
    let h = [0x2a; 16];
    for _ in 0..4 {
        append(&engine, h).await.unwrap();
    }
    let b04 = stored_bytes(&engine, &h, 0, 4).await;
    let first = engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!((first.pages, first.page_bytes), (4, b04));
    for _ in 4..8 {
        append(&engine, h).await.unwrap();
    }
    let b48 = stored_bytes(&engine, &h, 4, 8).await;
    let stale = seal_op(&engine, h, 0..8, b04 + b48);
    refused_beside_an_append(&engine, h, stale).await;
    let b8 = stored_bytes(&engine, &h, 8, 9).await;
    let tail = applied(&engine, h).await;
    assert_eq!((tail.absorbed, tail.next), (4, 9));
    assert_eq!(
        tail.unabsorbed_bytes,
        b48 + b8,
        "the stream ledger is not [4, 9)"
    );
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(ledger, b48 + b8, "the shard ledger is not the stream's");
    let next = engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!((next.pages, next.page_bytes), (5, b48 + b8));
    append(&engine, h).await.unwrap();
    let b9 = stored_bytes(&engine, &h, 9, 10).await;
    let tail = applied(&engine, h).await;
    assert_eq!(
        tail.absorbed, 9,
        "the seal from the boundary did not retire"
    );
    assert_eq!(
        tail.unabsorbed_bytes, b9,
        "the stream ledger is not [9, 10)"
    );
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(ledger, b9, "the shard ledger is not the stream's");
    engine.begin_close();
}

/// Committer layer: a seal that starts past the boundary would leave
/// [0, 1) unretired forever (a phantom backlog); it is refused whole, the
/// boundary stays 0 and the append beside it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_advance_that_skips_offsets_is_dropped_whole() {
    let (engine, _store) = rig("skip-advance").await;
    let h2 = [0x2b; 16];
    for _ in 0..2 {
        append(&engine, h2).await.unwrap();
    }
    let b1 = stored_bytes(&engine, &h2, 1, 2).await;
    let skipping = seal_op(&engine, h2, 1..2, b1);
    refused_beside_an_append(&engine, h2, skipping).await;
    let tail = applied(&engine, h2).await;
    assert_eq!(
        (tail.absorbed, tail.next),
        (0, 3),
        "a seal that skips offsets moved the boundary"
    );
    let all = stored_bytes(&engine, &h2, 0, 3).await;
    assert_eq!(
        tail.unabsorbed_bytes, all,
        "the stream ledger is not [0, 3)"
    );
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(ledger, all, "the shard ledger is not the stream's");
    engine.begin_close();
}

async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + ANSWER_WITHIN;
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "{what} never happened"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}

/// Settlement: a seal whose commit is applied but not yet durable has not
/// settled: `seal_once` returns only once durable dispatch has published
/// the stream's new boundary and ledger (step 7a), so nothing that follows
/// a seal (its memtable flush) runs ahead of its durability.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receipted_advance_settles_only_after_its_group_is_durable() {
    let (engine, store) = rig("settle-durable").await;
    let h = [0x2c; 16];
    for _ in 0..4 {
        append(&engine, h).await.unwrap();
    }
    let b04 = stored_bytes(&engine, &h, 0, 4).await;
    let handle = engine.stream_handle(h).await.unwrap();
    let before_commit = Hold::new();
    *engine.layout.seal.hooks.before_commit.lock().unwrap() = Some(before_commit.clone());
    let settled = AtomicBool::new(false);
    let seal = async {
        let report = engine.seal_once(Trigger::Requested).await;
        settled.store(true, Ordering::SeqCst);
        let durable = handle.state.lock().unwrap().durable.clone();
        (report, durable)
    };
    let checks = async {
        wait_for("the seal reached its commit", || {
            before_commit.entered.load(Ordering::Acquire) > 0
        })
        .await;
        let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
        before_commit.release();
        wait_for("the seal applied behind a held WAL put", || {
            engaged.load(Ordering::SeqCst) >= 1
                && handle.state.lock().unwrap().applied.absorbed == 4
        })
        .await;
        assert!(
            !settled.load(Ordering::SeqCst),
            "a seal settled before its group was durable"
        );
        assert_eq!(handle.state.lock().unwrap().durable.absorbed, 0);
        store.release_hold();
    };
    let ((report, durable), ()) = tokio::join!(seal, checks);
    assert_eq!(report.unwrap().page_bytes, b04);
    assert_eq!(
        (durable.absorbed, durable.unabsorbed_bytes),
        (4, 0),
        "a seal settled before its boundary and ledger were published"
    );
    assert_eq!(engine.maintenance_snapshot().unabsorbed_frame_bytes, 0);
    engine.begin_close();
}

/// Settlement: a seal whose group is refused settles with the refusal:
/// `seal_once` fails, and the boundary, the ledgers and the pending pages
/// do not move; the next seal takes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_advance_settles_with_its_group() {
    let (engine, _store) = rig("settle-refused").await;
    let h = [0x2d; 16];
    for _ in 0..4 {
        append(&engine, h).await.unwrap();
    }
    let b04 = stored_bytes(&engine, &h, 0, 4).await;
    let counters = &engine.layout.seal.counters;
    let failures = counters.failures.load(Ordering::Relaxed);
    engine.fail_next_seal_group();
    let refused = engine.seal_once(Trigger::Requested).await;
    assert!(matches!(refused, Err(SealError::Store(_))), "{refused:?}");
    assert_eq!(engine.group_failures_tripped(), 1);
    assert_eq!(counters.failures.load(Ordering::Relaxed), failures + 1);
    let tail = applied(&engine, h).await;
    assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (0, b04));
    assert_eq!(engine.maintenance_snapshot().unabsorbed_frame_bytes, b04);
    assert_eq!(
        stored_bytes(&engine, &h, 0, 4).await,
        b04,
        "the pending row moved"
    );
    let next = engine.seal_once(Trigger::Requested).await.unwrap();
    assert_eq!((next.pages, next.page_bytes), (4, b04));
    let tail = applied(&engine, h).await;
    assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (4, 0));
    engine.begin_close();
}
