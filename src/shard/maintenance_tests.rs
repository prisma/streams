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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "load_or_rebuild_covers_present_missing_and_corrupt; the scenario pins the present, missing and corrupt maintenance rows through one engine in one order; helper phases would hide which state each rebuild observed"
)]
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
