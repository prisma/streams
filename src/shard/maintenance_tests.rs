//! Maintenance rows: load-or-rebuild across present, missing and corrupt state.
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

type Reply = oneshot::Receiver<Result<AppendAck, AppendErr>>;

/// `n` untagged records appended to `hash` in one request.
fn records(hash: [u8; 16], n: usize) -> (AppendReq, Reply) {
    let (resp, reply) = oneshot::channel();
    let entries: Vec<Bytes> = (0..n)
        .map(|i| Bytes::from(format!("record-{i}-{}", "x".repeat(i * 8))))
        .collect();
    let req = AppendReq {
        hash,
        route: [0; 16],
        enqueued_at: std::time::Instant::now(),
        bytes: entries.iter().map(Bytes::len).sum(),
        entries,
        routing_key: "lane".into(),
        key_hash: [7; 16],
        producer_lineage: vec![],
        key_version: 1,
        subkey: [1; 32],
        ts_hint_ms: None,
        seq: None,
        finish: AppendFinish::Open,
        billing: None,
        seal_gen: None,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        usage: Arc::new(Default::default()),
        resp,
    };
    (req, reply)
}

/// Waits until the stream's boundary is `upto` and both the stream ledger
/// and the published shard ledger owe exactly `owed` frame bytes.
async fn settled(engine: &ShardEngine, hash: &[u8; 16], upto: u64, owed: u64) {
    let mut seen = None;
    let reached = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let tail = engine.tail_fields(hash).await.unwrap().unwrap();
            let shard = engine.maintenance_snapshot().unabsorbed_frame_bytes;
            seen = Some((tail.absorbed, tail.unabsorbed_bytes, shard));
            if seen == Some((upto, owed, owed)) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "(absorbed, stream ledger, shard ledger) = {seen:?}, expected ({upto}, {owed}, {owed})"
    );
}

/// TLA-016-F1, through the single-stream absorbed submit: an advance moves
/// the absorbed boundary and retires what it covers from the stream and
/// shard ledgers. An aligned chunk retires the count it reports without a
/// read; one that does not start at the boundary retires the stored bytes
/// of the range it advances over, read back from the flushed SST; the same
/// bogus count on an aligned chunk is past the ledger, so it refuses its
/// whole group, the rider append included, and moves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submitted_absorbed_advances_retire_exactly_what_they_cover() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Db::builder("absorb-submit", store.clone())
        .build()
        .await
        .unwrap();
    let (tx, _signals) = mpsc::channel(16);
    let engine = ShardEngine::start(
        "absorb-submit".into(),
        Arc::new(db),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let hash = [41; 16];
    let (append, reply) = records(hash, 4);
    engine.try_enqueue(append).unwrap();
    assert_eq!(reply.await.unwrap().unwrap().next_offset, 4);
    let mut frames = Vec::new();
    for offset in 0..4 {
        let row = engine.db.get(record_key(&hash, offset)).await.unwrap();
        frames.push(row.unwrap().len() as u64);
    }
    let owed = |from: usize| frames[from..].iter().sum::<u64>();
    settled(&engine, &hash, 0, owed(0)).await;

    engine.submit_absorbed(hash, 0, 1, frames[0]).await;
    settled(&engine, &hash, 1, owed(1)).await;
    // The records now live in an SST, as they do by the time a real
    // absorber reports, and the chunk [2, 3) at boundary 1 carries a count
    // no chunk copied.
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    engine.db.flush_with_options(memtable).await.unwrap();
    engine.submit_absorbed(hash, 2, 3, 999_999).await;
    settled(&engine, &hash, 3, owed(3)).await;

    let hold = engine.test_hold_commit().await;
    let (rider, refused) = records(hash, 1);
    engine.try_enqueue(rider).unwrap();
    engine.submit_absorbed(hash, 3, 4, 999_999).await;
    drop(hold);
    let refused = refused.await.unwrap();
    assert!(
        refused.is_err(),
        "a group retiring past the ledger acked its rider: {refused:?}"
    );
    let tail = engine.tail_fields(&hash).await.unwrap().unwrap();
    assert_eq!(
        (tail.absorbed, tail.next),
        (3, 4),
        "the refused group moved"
    );
    settled(&engine, &hash, 3, owed(3)).await;

    engine.submit_absorbed(hash, 3, 4, frames[3]).await;
    settled(&engine, &hash, 4, 0).await;
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}
