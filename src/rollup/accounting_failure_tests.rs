#![cfg(test)]

use super::{
    K_CURSOR, SegmentState, UsageRollup, k_month, k_name, k_project, k_source, month_start_ms,
    read_faults,
};
use crate::billing::UsageEnvelope;
use crate::billing::{ReadBatch, ReadRow, UsagePayload};
use slatedb::Db;
use std::sync::Arc;

fn batch(seq: u64) -> UsageEnvelope {
    UsageEnvelope {
        v: 1,
        event_id: format!("read/boot/{seq}"),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::ReadBatch(ReadBatch {
            source: crate::billing::MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "boot".into(),
            },
            seq,
            from_ms: month_start_ms(2026, 7),
            to_ms: month_start_ms(2026, 7) + 100,
            rows: vec![ReadRow {
                identity: crate::billing::BillingIdentity {
                    account_id: "a".into(),
                    project_id: "p".into(),
                    stream_id: "s".into(),
                    stream_name: "orders".into(),
                },
                read_payload_bytes: 101,
                read_records: 3,
                read_operations: 2,
                queue_operations: 4,
                append_requests: 5,
            }],
        }),
    }
}

async fn snapshot(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = vec![];
    let mut iter = db.scan(..).await.unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        rows.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r14_required_read_failures_leave_every_row_and_checkpoint_unchanged() {
    let db = Arc::new(
        Db::builder("r14-reads", Arc::new(object_store::memory::InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let r = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    r.apply_page(&[batch(0)], "c0").await.unwrap();
    let before = snapshot(&db).await;
    for key in [
        k_source("boot"),
        k_month("2026-07", "a", "p", "s"),
        k_name("2026-07", "a", "p", "orders"),
        k_project("2026-07", "a", "p"),
    ] {
        read_faults()
            .lock()
            .unwrap()
            .insert((Arc::as_ptr(&db) as usize, key.clone()));
        assert!(r.apply_page(&[batch(1)], "c1").await.is_err());
        assert_eq!(snapshot(&db).await, before, "key={key:?}");
    }
    read_faults()
        .lock()
        .unwrap()
        .insert((Arc::as_ptr(&db) as usize, K_CURSOR.to_vec()));
    assert!(r.cursor().await.is_err());
    assert_eq!(snapshot(&db).await, before);
    r.apply_page(&[batch(1)], "c1").await.unwrap();
    let after = snapshot(&db).await;
    r.apply_page(&[batch(1)], "c1").await.unwrap();
    assert_eq!(snapshot(&db).await, after, "replay must be exact");
    assert_eq!(
        r.month_row("2026-07", "a", "p", "s")
            .await
            .unwrap()
            .unwrap()
            .read_payload_bytes,
        202
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r14_corrupt_watermarks_rows_and_close_keys_block_progress() {
    let db = Arc::new(
        Db::builder(
            "r14-corruption",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let r = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    r.apply_page(&[batch(0)], "c0").await.unwrap();
    for len in [0, 1, 2, 3, 4, 5, 6, 7, 9] {
        db.put(k_source("boot"), vec![0; len]).await.unwrap();
        let before = snapshot(&db).await;
        assert!(r.apply_page(&[batch(1)], "c1").await.is_err());
        assert_eq!(snapshot(&db).await, before);
    }
    db.put(k_source("boot"), 0u64.to_le_bytes()).await.unwrap();
    db.put(k_month("2026-07", "a", "p", "s"), b"malformed json")
        .await
        .unwrap();
    let before = snapshot(&db).await;
    assert!(r.apply_page(&[batch(1)], "c1").await.is_err());
    assert!(r.close_month(2026, 7, 0).await.is_err());
    assert_eq!(snapshot(&db).await, before);
    db.put(
        b"segment/a/p/s/not-a-number",
        serde_json::to_vec(&SegmentState {
            account_id: "a".into(),
            ..Default::default()
        })
        .unwrap(),
    )
    .await
    .unwrap();
    let before = snapshot(&db).await;
    assert!(r.close_month(2026, 7, 0).await.is_err());
    assert_eq!(snapshot(&db).await, before);
    db.close().await.unwrap();
}

#[test]
fn decimal_validation_preserves_nested_and_legacy_zero_rules() {
    let decode = |text: &str| super::decode_json::<serde_json::Value>(text.as_bytes());
    assert!(
        decode(
            r#"{"rows":[{"storage_byte_ms":""},{"storage_byte_ms_delta":"-1"}],"unrelated":"text"}"#
        )
        .is_ok()
    );
    for invalid in [
        r#"{"rows":[{"storage_byte_ms":"-1"}]}"#,
        r#"{"storage_byte_ms":1}"#,
        r#"{"storage_byte_ms":"bad"}"#,
        r#"{"storage_byte_ms_delta":"170141183460469231731687303715884105728"}"#,
    ] {
        assert!(
            decode(invalid).is_err(),
            "accepted invalid accounting data: {invalid}"
        );
    }
}

#[test]
fn provisional_storage_handles_the_full_signed_clock_range() {
    let end = month_start_ms(2026, 8);
    let mut row = super::MonthRow {
        segments: [(
            0,
            super::SegMonth {
                storage_byte_ms: "7".into(),
                gauge_bytes: 3,
                accounted_through_ms: i64::MIN,
                ..Default::default()
            },
        )]
        .into(),
        ..Default::default()
    };
    let expected = 7 + u128::from(end.abs_diff(i64::MIN)) * 3;
    assert_eq!(
        row.storage_byte_ms_provisional("2026-07", i64::MAX),
        expected
    );
    row.segments.get_mut(&0).unwrap().final_seen = true;
    assert_eq!(row.storage_byte_ms_provisional("2026-07", i64::MAX), 7);
    row.segments.get_mut(&0).unwrap().final_seen = false;
    row.segments.get_mut(&0).unwrap().accounted_through_ms = end;
    assert_eq!(row.storage_byte_ms_provisional("2026-07", i64::MAX), 7);
}
