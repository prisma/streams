//! Maintenance rows: load-or-rebuild across present, missing and corrupt
//! state, and the absorbed-boundary retirement that keeps the ledger exact.
#![cfg(test)]
use super::*;
use std::sync::Arc;

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

/// R25-A/R26-4: the codec round-trips v2; the R24 16-byte row is
/// classified LEGACY — its payload-unit value is never surfaced as
/// frame bytes (it can under- OR overstate, and understatement makes
/// the first exact retirement read as over-retirement forever).
#[test]
fn codec_roundtrips_v2_and_refuses_v1_values() {
    let m = ShardMaintenance {
        version: 7,
        unabsorbed_frame_bytes: 123_456,
        backlog_started_ms: 111,
        last_progress_ms: 222,
    };
    let got = decode_shard_maint(&encode_shard_maint(&m)).unwrap();
    assert_eq!(got, m);

    // R24 layout: [bytes u64][oldest_ms i64], 16 untagged bytes.
    let mut v1 = [0u8; 16];
    v1[..8].copy_from_slice(&987_654u64.to_le_bytes());
    v1[8..].copy_from_slice(&42i64.to_le_bytes());
    assert!(
        matches!(
            decode_shard_maint_row(&v1),
            Ok(ShardMaintRow::LegacyPayloadUnit)
        ),
        "16-byte row must classify as legacy"
    );
    assert!(
        decode_shard_maint(&v1).is_err(),
        "the strict decode must never surface a payload-unit value"
    );

    assert!(
        decode_shard_maint(&[0u8; 7]).is_err(),
        "corrupt row must error"
    );
    let mut bad = [0u8; 40];
    bad[0] = 99;
    assert!(
        decode_shard_maint(&bad).is_err(),
        "unknown version must error"
    );
}

/// R25-A: load semantics against a real DB — present row loads (with
/// the progress clock initialized for v1 rows), missing row rebuilds
/// from the dirty index + tails and PERSISTS the rebuilt row.
#[expect(
    clippy::too_many_lines,
    reason = "load_or_rebuild_covers_present_missing_and_corrupt; the scenario pins the present, missing and corrupt maintenance rows through one engine in one order; helper phases would hide which state each rebuild observed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_or_rebuild_covers_present_missing_and_corrupt() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());

    // 1. Present v2 row: loads exactly.
    let db = Db::builder("m1/shard", store.clone())
        .build()
        .await
        .unwrap();
    let m = ShardMaintenance {
        version: 3,
        unabsorbed_frame_bytes: 555,
        backlog_started_ms: 10,
        last_progress_ms: 20,
    };
    let mut wb = WriteBatch::new();
    wb.put(shard_maint_key(), encode_shard_maint(&m));
    db.write_with_options(wb, &WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(load_or_rebuild_maintenance(&db).await.unwrap(), m);
    db.close().await.unwrap();

    // 2. Present v1 row (payload-unit 777): the value is IGNORED and
    // the ledger is rebuilt from the exact tails (R26-4). The
    // persisted replacement is a v2 row.
    let db = Db::builder("m2/shard", store.clone())
        .build()
        .await
        .unwrap();
    let mut v1 = [0u8; 16];
    v1[..8].copy_from_slice(&777u64.to_le_bytes());
    let h0 = [9u8; 16];
    let mut wb = WriteBatch::new();
    wb.put(shard_maint_key(), v1);
    wb.put(
        tail_key(&h0),
        encode_tail(&TailFields {
            next: 5,
            absorbed: 2,
            unabsorbed_bytes: 300,
            ..Default::default()
        }),
    );
    wb.put(
        dirty_key(&h0),
        dirty_value(&StreamMaintenance {
            absorbed: 2,
            next: 5,
            ..Default::default()
        }),
    );
    db.write_with_options(wb, &WriteOptions::default())
        .await
        .unwrap();
    let got = load_or_rebuild_maintenance(&db).await.unwrap();
    assert_eq!(
        got.unabsorbed_frame_bytes, 300,
        "legacy value must be rebuilt from tails, never converted"
    );
    assert!(
        got.last_progress_ms > 0,
        "rebuilt backlog must start the stall clock"
    );
    let raw = db
        .get(shard_maint_key())
        .await
        .unwrap()
        .expect("row replaced");
    assert_eq!(raw.len(), 40, "the legacy row must be replaced by v2");
    db.close().await.unwrap();

    // 3. Missing row: rebuild from dirty index + tails, then persist.
    let db = Db::builder("m3/shard", store.clone())
        .build()
        .await
        .unwrap();
    let h1 = [1u8; 16];
    let h2 = [2u8; 16];
    let mut wb = WriteBatch::new();
    for (h, bytes) in [(h1, 300u64), (h2, 400u64)] {
        let t = TailFields {
            next: 10,
            absorbed: 4,
            unabsorbed_bytes: bytes,
            ..Default::default()
        };
        wb.put(tail_key(&h), encode_tail(&t));
        wb.put(
            dirty_key(&h),
            dirty_value(&StreamMaintenance {
                absorbed: 4,
                next: 10,
                ..Default::default()
            }),
        );
    }
    db.write_with_options(wb, &WriteOptions::default())
        .await
        .unwrap();
    let got = load_or_rebuild_maintenance(&db).await.unwrap();
    assert_eq!(got.unabsorbed_frame_bytes, 700, "rebuild sums tail gauges");
    // And it persisted: a second load takes the row path.
    let raw = db
        .get(shard_maint_key())
        .await
        .unwrap()
        .expect("row persisted");
    assert_eq!(
        decode_shard_maint(&raw).unwrap().unabsorbed_frame_bytes,
        700
    );
    db.close().await.unwrap();

    // 4. Corrupt row: an engine-open FAILURE, never zero backlog.
    let db = Db::builder("m4/shard", store.clone())
        .build()
        .await
        .unwrap();
    let mut wb = WriteBatch::new();
    wb.put(shard_maint_key(), vec![9u8; 11]);
    db.write_with_options(wb, &WriteOptions::default())
        .await
        .unwrap();
    assert!(
        load_or_rebuild_maintenance(&db).await.is_err(),
        "corrupt maintenance row must fail the open"
    );
    db.close().await.unwrap();
}

// ---- absorbed-boundary retirement (release hold: the capacity run's
// one-off 500). An absorbed advance that re-covers bytes an earlier
// advance retired used to retire them again; once the stream ledger was
// smaller than the double count, the whole commit group was refused as
// "maintenance accounting diverged" and every append in it answered 500.

type Answer = oneshot::Receiver<Result<AppendAck, AppendErr>>;
const ANSWER_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// An engine over a fault store whose WAL puts a test can hold.
async fn rig(name: &str) -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>) {
    let store = crate::dst::FaultStore::uniform(
        Arc::new(object_store::memory::InMemory::new()),
        0x52,
        crate::dst::FaultPlan::new(0, 0, 0),
    );
    let db = Db::builder(name, store.clone() as Arc<dyn object_store::ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let (tx, _signals) = mpsc::channel(1);
    let engine = ShardEngine::start(
        name.into(),
        Arc::new(db),
        store.clone(),
        ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
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

/// Stored frame bytes of the stream's records [from, upto).
async fn stored_bytes(engine: &ShardEngine, hash: &[u8; 16], from: u64, upto: u64) -> u64 {
    let mut total = 0;
    for offset in from..upto {
        let row = engine.db.get(record_key(hash, offset)).await.unwrap();
        total += row.unwrap().len() as u64;
    }
    total
}

async fn applied(engine: &ShardEngine, hash: [u8; 16]) -> TailFields {
    let handle = engine.stream_handle(hash).await.unwrap();
    let state = handle.state.lock().unwrap();
    state.applied.clone()
}

/// The advance under test as the committer stages it: `len` bytes copied
/// from offset `from`, up to `upto`.
fn advance(hash: [u8; 16], from: u64, upto: u64, len: u64) -> CommitOp {
    CommitOp::Absorbed {
        hash,
        upto,
        bytes: CopiedBytes::new(from, len),
        v2: true,
    }
}

/// The same advance through the committer queue, as one gather's batch.
async fn submit_advance(engine: &ShardEngine, hash: [u8; 16], from: u64, upto: u64, len: u64) {
    let copied = CopiedBytes::new(from, len);
    engine
        .submit_absorbed_batch_v2(vec![(hash, upto, copied)])
        .await;
}

/// A trim step drains what an advance's budget left behind: the advance
/// trims at most `max_trim_per_op` records below the old boundary, the rest
/// stays trim debt, and each TrimTick moves `trimmed` on by the budget up to
/// `trim_safe_to` and never past it (KANI-046's bound at both call sites).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trim_tick_drains_what_an_advances_budget_left() {
    let (engine, _store) = rig("trim-debt").await;
    let cfg = ShardConfig {
        max_trim_per_op: 1,
        ..ShardConfig::default()
    };
    let h = [0x2e; 16];
    for _ in 0..8 {
        append(&engine, h).await.unwrap();
    }
    let b04 = stored_bytes(&engine, &h, 0, 4).await;
    let b48 = stored_bytes(&engine, &h, 4, 8).await;
    engine.commit_group(vec![advance(h, 0, 4, b04)], &cfg).await;
    engine.commit_group(vec![advance(h, 4, 8, b48)], &cfg).await;
    let tail = applied(&engine, h).await;
    let frontiers = (tail.absorbed, tail.trim_safe_to, tail.trimmed);
    assert_eq!(frontiers, (8, 4, 1), "the advance trims one record behind");
    for trimmed in [2, 3, 4, 4] {
        engine.commit_group(vec![CommitOp::TrimTick], &cfg).await;
        assert_eq!(applied(&engine, h).await.trimmed, trimmed);
    }
    assert!(engine.db.get(record_key(&h, 3)).await.unwrap().is_none());
    assert!(engine.db.get(record_key(&h, 4)).await.unwrap().is_some());
}

/// Committer layer: in one group, an advance over [0, 4) and a second
/// over [0, 8) that re-covers it. The first retires exactly, the second
/// is dropped whole, and the append beside them commits; the ledger then
/// holds exactly the bytes of [absorbed, next). The stream's next advance
/// from the boundary retires the rest and trims one advance behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_advance_that_overlaps_the_boundary_retires_nothing_and_fails_no_append() {
    let (engine, _store) = rig("overlap-advance").await;
    let cfg = ShardConfig::default();
    let h = [0x2a; 16];
    for _ in 0..8 {
        append(&engine, h).await.unwrap();
    }
    let (b04, b48) = (
        stored_bytes(&engine, &h, 0, 4).await,
        stored_bytes(&engine, &h, 4, 8).await,
    );
    let (req, reply) = append_op(h);
    let group = vec![
        advance(h, 0, 4, b04),
        advance(h, 0, 8, b04 + b48),
        CommitOp::Append(req),
    ];
    engine.commit_group(group, &cfg).await;
    let answer = tokio::time::timeout(ANSWER_WITHIN, reply)
        .await
        .unwrap()
        .unwrap();
    assert!(
        answer.is_ok(),
        "an append co-grouped with an overlapping advance was refused: {answer:?}"
    );
    let b8 = stored_bytes(&engine, &h, 8, 9).await;
    let tail = applied(&engine, h).await;
    assert_eq!((tail.absorbed, tail.history_v2, tail.trimmed), (4, true, 0));
    assert_eq!(
        tail.unabsorbed_bytes,
        b48 + b8,
        "the stream ledger is not [4, 9)"
    );
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(ledger, b48 + b8, "the shard ledger is not the stream's");
    // A duplicate at the boundary raises no trim target.
    engine.commit_group(vec![advance(h, 4, 4, 0)], &cfg).await;
    let tail = applied(&engine, h).await;
    assert_eq!(
        (tail.absorbed, tail.trim_safe_to),
        (4, 0),
        "a duplicate moved the tail"
    );
    submit_advance(&engine, h, 4, 8, b48).await;
    append(&engine, h).await.unwrap();
    let b9 = stored_bytes(&engine, &h, 9, 10).await;
    let tail = applied(&engine, h).await;
    assert_eq!(
        tail.absorbed, 8,
        "the advance from the boundary did not retire"
    );
    assert_eq!((tail.trim_safe_to, tail.trimmed), (4, 4));
    assert!(engine.db.get(record_key(&h, 0)).await.unwrap().is_none());
    assert_eq!(engine.trim_deletes_total.load(Ordering::Relaxed), 4);
    assert_eq!(
        tail.unabsorbed_bytes,
        b8 + b9,
        "the stream ledger is not [8, 10)"
    );
    let ledger = engine.maintenance_snapshot().unabsorbed_frame_bytes;
    assert_eq!(ledger, b8 + b9, "the shard ledger is not the stream's");
    engine.begin_close();
}

/// Committer layer: an advance that starts past the boundary would leave
/// [0, 1) unretired forever (a phantom backlog); it is dropped whole, the
/// layout stays unsealed and the append beside it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_advance_that_skips_offsets_is_dropped_whole() {
    let (engine, _store) = rig("skip-advance").await;
    let h2 = [0x2b; 16];
    for _ in 0..2 {
        append(&engine, h2).await.unwrap();
    }
    let b1 = stored_bytes(&engine, &h2, 1, 2).await;
    let (req, reply) = append_op(h2);
    let group = vec![advance(h2, 1, 2, b1), CommitOp::Append(req)];
    engine.commit_group(group, &ShardConfig::default()).await;
    let answer = tokio::time::timeout(ANSWER_WITHIN, reply)
        .await
        .unwrap()
        .unwrap();
    assert!(
        answer.is_ok(),
        "an append beside a skipping advance was refused: {answer:?}"
    );
    let tail = applied(&engine, h2).await;
    assert_eq!(
        (tail.absorbed, tail.history_v2),
        (0, false),
        "an advance that skips offsets moved the boundary"
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

/// Settlement: the group that retired a receipted advance holds its
/// receipt while the group is applied but not yet durable, and drops it
/// once durable dispatch has published the tail. Until then the absorber
/// may not roll the stream's lane mark back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receipted_advance_settles_only_after_its_group_is_durable() {
    let (engine, store) = rig("settle-durable").await;
    let h = [0x2c; 16];
    for _ in 0..4 {
        append(&engine, h).await.unwrap();
    }
    let b04 = stored_bytes(&engine, &h, 0, 4).await;
    let handle = engine.stream_handle(h).await.unwrap();
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let submissions = Arc::new(Submissions::default());
    let copied = CopiedBytes::new(0, b04).receipted(submissions.submit(&h));
    engine.submit_absorbed_batch_v2(vec![(h, 4, copied)]).await;
    wait_for("the advance applied behind a held WAL put", || {
        engaged.load(Ordering::SeqCst) >= 1 && handle.state.lock().unwrap().applied.absorbed == 4
    })
    .await;
    assert!(
        !submissions.settled(&h),
        "an advance settled before its group was durable"
    );
    assert_eq!(handle.state.lock().unwrap().durable.absorbed, 0);
    store.release_hold();
    wait_for("the durable advance settled", || submissions.settled(&h)).await;
    let published = handle.state.lock().unwrap().durable.absorbed;
    assert_eq!(
        published, 4,
        "an advance settled before its boundary was published"
    );
    engine.begin_close();
}

/// Settlement: a refused group drops its advance's receipt with the
/// refusal, and the boundary does not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_advance_settles_with_its_group() {
    let (engine, _store) = rig("settle-refused").await;
    let h = [0x2d; 16];
    for _ in 0..4 {
        append(&engine, h).await.unwrap();
    }
    let b04 = stored_bytes(&engine, &h, 0, 4).await;
    engine.fail_next_absorbed_group();
    let submissions = Arc::new(Submissions::default());
    let copied = CopiedBytes::new(0, b04).receipted(submissions.submit(&h));
    engine.submit_absorbed_batch_v2(vec![(h, 4, copied)]).await;
    wait_for("the group refused", || engine.group_failures_tripped() >= 1).await;
    wait_for("the refused advance settled", || submissions.settled(&h)).await;
    let tail = applied(&engine, h).await;
    assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (0, b04));
    engine.begin_close();
}
