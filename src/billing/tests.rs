//! Billing owner fixtures: spool settings, storage-clock month splits and
//! the drain round's crash interleavings.
#![cfg(test)]
use super::*;

/// The no-environment knob posture for DB-open plumbing (spool
/// settings come from the owned config since WP-01 PR 3.1).
fn test_cfg() -> crate::config::ServerConfig {
    crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
}

#[test]
fn month_math_round_trips() {
    // Epoch is 1970-01.
    assert_eq!(utc_year_month(0), (1970, 1));
    assert_eq!(month_start_ms(1970, 1), 0);
    // A known instant: 2026-08-06 ≈ 1786-billion ms.
    let (y, m) = utc_year_month(1_786_000_000_000);
    assert_eq!((y, m), (2026, 8));
    // Boundary consistency: the first ms of every month maps back,
    // and the ms before it maps to the previous month.
    let mut cur = (2025, 12u32);
    for _ in 0..15 {
        let start = month_start_ms(cur.0, cur.1);
        assert_eq!(utc_year_month(start), cur, "start of {cur:?}");
        let (py, pm) = utc_year_month(start - 1);
        assert_eq!(next_month(py, pm), cur, "instant before {cur:?}");
        cur = next_month(cur.0, cur.1);
    }
    assert_eq!(parse_month("2026-08"), Some((2026, 8)));
    assert_eq!(parse_month("2026-13"), None);
    assert_eq!(parse_month("junk"), None);
}

#[test]
fn storage_clock_splits_at_month_boundaries() {
    let mut m = SegmentBillingMetaV1 {
        owned_frame_bytes_current: 1000,
        ..Default::default()
    };
    // Clock starts mid-July 2026.
    let jul = month_start_ms(2026, 7) + 86_400_000;
    m.advance_storage_clock(jul, |_| panic!("no close on start"));
    assert_eq!((m.month_year, m.month_month), (2026, 7));
    // Advance into August: July closes with exactly the byte-time
    // up to the boundary.
    let aug_start = month_start_ms(2026, 8);
    let into_aug = aug_start + 3_600_000;
    let mut closed = Vec::new();
    m.advance_storage_clock(into_aug, |c| closed.push(c.clone()));
    assert_eq!(closed.len(), 1);
    let jul_final = &closed[0];
    assert_eq!((jul_final.month_year, jul_final.month_month), (2026, 7));
    let expect_jul = (aug_start - jul) as u128 * 1000;
    assert_eq!(jul_final.month_byte_ms(), expect_jul);
    // The live row is now August with exactly one hour integrated.
    assert_eq!((m.month_year, m.month_month), (2026, 8));
    assert_eq!(m.month_byte_ms(), 3_600_000u128 * 1000);
    // Idle multi-month jump closes every intervening month.
    let mut closes = Vec::new();
    m.advance_storage_clock(month_start_ms(2026, 11) + 5, |c| {
        closes.push(month_str(c.month_year, c.month_month))
    });
    assert_eq!(closes, vec!["2026-08", "2026-09", "2026-10"]);
}

#[test]
fn reserved_namespace_is_the_underscore_prefix() {
    assert!(is_reserved_stream(USAGE_STREAM));
    assert!(is_reserved_stream(OPS_METRICS_STREAM));
    assert!(is_reserved_stream(OPS_EVENTS_STREAM));
    assert!(is_reserved_stream(AUDIT_EVENTS_STREAM));
    assert!(is_reserved_stream("_future_system_thing"));
    assert!(!is_reserved_stream("orders"));
    assert!(!is_reserved_stream("customers/_acme")); // only the leading segment
}

#[test]
fn snapshot_event_ids_are_deterministic() {
    let id = BillingIdentity {
        account_id: "a".into(),
        project_id: "p".into(),
        stream_id: "11".repeat(8),
        stream_name: "orders".into(),
    };
    let s = SegmentSnapshot {
        identity: id,
        segment_id: 3,
        usage_version: 42,
        month: "2026-08".into(),
        month_final: false,
        ingest_payload_bytes_month: 0,
        ingest_records_month: 0,
        owned_frame_bytes_current: 0,
        storage_byte_ms_month: "0".into(),
        storage_accounted_through_ms: 0,
        retained_by_forks: false,
    };
    assert_eq!(s.deterministic_event_id(), s.deterministic_event_id());
    let mut f = s.clone();
    f.month_final = true;
    assert_ne!(s.deterministic_event_id(), f.deterministic_event_id());
}

fn test_batch(seq: u64) -> ReadBatch {
    ReadBatch {
        source: MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "b".into(),
        },
        seq,
        from_ms: 0,
        to_ms: 1000,
        rows: vec![ReadRow {
            identity: BillingIdentity {
                account_id: "a".into(),
                project_id: "p".into(),
                stream_id: "22".repeat(8),
                stream_name: "orders".into(),
            },
            read_payload_bytes: seq + 1,
            read_records: 1,
            read_operations: 1,
            queue_operations: 0,
            append_requests: 0,
        }],
    }
}

/// Round-22 item 2a + OOM review item 5: a drain round persists as
/// ONE all-or-nothing WriteBatch (one flush). On a store fault the
/// WHOLE drained set requeues in order — nothing is dropped and
/// nothing is half-durable; on success the whole round lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spool_fault_requeues_the_whole_round() {
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let spool = ReadSpool::open(store, "", "t-remainder", &test_cfg())
        .await
        .unwrap();
    let acc = ReadUsageAccumulator::new(MeterSource {
        cell: "c".into(),
        instance: "i".into(),
        boot: "b".into(),
    });
    acc.requeue(vec![test_batch(0), test_batch(1), test_batch(2)]);
    // Fault the round's single batched write.
    spool
        .fail_after
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let err = spool_sealed(&acc, &spool, 10).await.unwrap_err();
    assert!(err.contains("read spool persist"), "{err}");
    assert!(
        spool.pending(10).await.unwrap().is_empty(),
        "all-or-nothing: a failed round leaves NOTHING half-durable"
    );
    let back = acc.drain_sealed(10);
    assert_eq!(
        back.iter().map(|b| b.seq).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "the whole round requeued in original order"
    );
    // Heal the store: the next round lands all three batches in one
    // WriteBatch, and the accumulator is empty.
    acc.requeue(back);
    spool
        .fail_after
        .store(-1, std::sync::atomic::Ordering::SeqCst);
    spool_sealed(&acc, &spool, 10).await.unwrap();
    let spooled = spool.pending(10).await.unwrap();
    assert_eq!(
        spooled.iter().map(|(_, b)| b.seq).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(acc.drain_sealed(10).is_empty());
}

/// Round-22 item 2c: a corrupt spool row is quarantined (moved,
/// preserved, counted — the count survives reopen), never
/// silently skipped, and never blocks the healthy rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spool_quarantines_corrupt_rows() {
    let store: std::sync::Arc<dyn object_store::ObjectStore> =
        std::sync::Arc::new(object_store::memory::InMemory::new());
    let spool = ReadSpool::open(store.clone(), "", "t-quarantine", &test_cfg())
        .await
        .unwrap();
    spool.persist(&test_batch(7)).await.unwrap();
    spool
        .put_raw(b"rb/\x00garbage", b"{not json")
        .await
        .unwrap();
    let ok = spool.pending(10).await.unwrap();
    assert_eq!(ok.len(), 1, "healthy row still drains");
    assert_eq!(ok[0].1.seq, 7);
    assert_eq!(spool.quarantined_count(), 1);
    assert_eq!(spool.quarantine_rows().await.len(), 1, "row preserved");
    // Idempotent: the moved row is not re-quarantined.
    spool.pending(10).await.unwrap();
    assert_eq!(spool.quarantined_count(), 1);
    // The alert condition survives a restart.
    drop(spool);
    let reopened = ReadSpool::open(store, "", "t-quarantine", &test_cfg())
        .await
        .unwrap();
    assert_eq!(reopened.quarantined_count(), 1);
    assert_eq!(reopened.pending(10).await.unwrap().len(), 1);
}
