#![cfg(test)]

use super::{MonthRow, UsageRollup, month_start_ms};
use crate::billing::{BillingIdentity, MeterSource, ReadRow, UsageEnvelope, UsagePayload};
use crate::billing::{ReadBatch, SegmentSnapshot};
use slatedb::WriteBatch;
use std::sync::Arc;

/// Restore the shared test clock before releasing exclusive access, including
/// unwinding through a failed assertion.
struct ClockGuard {
    _exclusive: tokio::sync::RwLockWriteGuard<'static, ()>,
}

impl ClockGuard {
    async fn at(now_ms: i64) -> Self {
        let exclusive = crate::billing::billing_clock_lock().write().await;
        crate::billing::BILLING_CLOCK_OVERRIDE.store(now_ms, std::sync::atomic::Ordering::Relaxed);
        Self {
            _exclusive: exclusive,
        }
    }
}

impl Drop for ClockGuard {
    fn drop(&mut self) {
        crate::billing::BILLING_CLOCK_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Read the pending artifact view at each close boundary, preserving its
/// production query limit and surfacing repository failures as test failures.
async fn pending_storage(rollup: &UsageRollup, month: &str) -> Option<u128> {
    rollup
        .pending_artifacts(64)
        .await
        .unwrap()
        .into_iter()
        .find(|(_, artifact_month, ..)| artifact_month == month)
        .map(|(.., row)| row.storage_byte_ms())
}

fn mem_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// The no-environment knob posture for the rollup DB open (settings
/// come from the owned config since WP-01 PR 3.1).
fn test_cfg() -> crate::config::ServerConfig {
    crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
}

fn id() -> BillingIdentity {
    BillingIdentity {
        account_id: "acct".into(),
        project_id: "proj".into(),
        stream_id: "aa".repeat(8),
        stream_name: "orders".into(),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "rollup snapshot fixture; version, month, counters, gauge and finality are independent scenario inputs; an options bag would hide the assertion dimensions"
)]
fn snap(
    version: u64,
    month: &str,
    bytes: u64,
    byte_ms: u128,
    gauge: u64,
    final_: bool,
) -> UsageEnvelope {
    let s = SegmentSnapshot {
        identity: id(),
        segment_id: 0,
        usage_version: version,
        month: month.into(),
        month_final: final_,
        ingest_payload_bytes_month: bytes,
        ingest_records_month: bytes / 10,
        owned_frame_bytes_current: gauge,
        storage_byte_ms_month: byte_ms.to_string(),
        storage_accounted_through_ms: crate::billing::month_start_ms(2026, 7) + 1_000_000,
        retained_by_forks: false,
    };
    UsageEnvelope {
        v: 1,
        event_id: s.deterministic_event_id(),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::SegmentSnapshot(s),
    }
}

/// Round-22 item 3: THREE segments of one stream, idle months —
/// every month bills the SUM of all three (nothing last-put-wins),
/// and name/project aggregates match.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_segment_idle_carry_sums_all_segments() {
    let _clock = ClockGuard::at(crate::billing::month_start_ms(2026, 12)).await;
    let r = UsageRollup::open(mem_store(), "mseg", &test_cfg())
        .await
        .unwrap();
    let jul15 = crate::billing::month_start_ms(2026, 7) + 14 * 86_400_000;
    // Three segments, gauges 100/200/300, all accounted through Jul 15.
    for (seg, gauge) in [(0u32, 100u64), (1, 200), (2, 300)] {
        let s = SegmentSnapshot {
            identity: id(),
            segment_id: seg,
            usage_version: 1,
            month: "2026-07".into(),
            month_final: false,
            ingest_payload_bytes_month: 10,
            ingest_records_month: 1,
            owned_frame_bytes_current: gauge,
            storage_byte_ms_month: "0".into(),
            storage_accounted_through_ms: jul15,
            retained_by_forks: false,
        };
        let env = UsageEnvelope {
            v: 1,
            event_id: s.deterministic_event_id(),
            event_time_ms: jul15,
            emitted_ms: jul15,
            cell: "c".into(),
            payload: UsagePayload::SegmentSnapshot(s),
        };
        r.apply_page(&[env], &format!("c{seg}")).await.unwrap();
    }
    let day = 86_400_000u128;
    let total_gauge = 600u128;
    // July: 17 idle days x 600 B across the three segments.
    assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 1);
    let jul = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(jul.storage_byte_ms(), 17 * day * total_gauge);
    // August, fully idle: 31 days x 600 — the multi-segment carry
    // must SUM, not last-put-win.
    assert_eq!(r.close_month(2026, 8, 0).await.unwrap(), 1);
    let aug = r
        .month_row("2026-08", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aug.storage_byte_ms(), 31 * day * total_gauge);
    assert_eq!(aug.segments.len(), 3, "every segment carried");
    let agg = r
        .project_row("2026-08", "acct", "proj")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(agg.storage_byte_ms, (31 * day * total_gauge).to_string());
    let name = r
        .name_row("2026-08", "acct", "proj", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name.storage_byte_ms, (31 * day * total_gauge).to_string());
}

/// Round-22 item 4: a rollover's month-final and live snapshots
/// share a usage version; whichever applies second must still
/// advance the global segment state, and the NEXT month's carry
/// uses the NEW gauge exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_version_live_snapshot_advances_segment_state() {
    let _clock = ClockGuard::at(crate::billing::month_start_ms(2026, 11)).await;
    let r = UsageRollup::open(mem_store(), "svtie", &test_cfg())
        .await
        .unwrap();
    let aug1 = crate::billing::month_start_ms(2026, 8);
    // Month-FINAL for July at version 5 (accounted through Aug 1,
    // OLD gauge 100), then the LIVE August snapshot at the SAME
    // version with the NEW gauge 400 accounted through Aug 2.
    let mk = |month: &str, final_: bool, gauge: u64, through: i64, byte_ms: u128| {
        let s = SegmentSnapshot {
            identity: id(),
            segment_id: 0,
            usage_version: 5,
            month: month.into(),
            month_final: final_,
            ingest_payload_bytes_month: 0,
            ingest_records_month: 0,
            owned_frame_bytes_current: gauge,
            storage_byte_ms_month: byte_ms.to_string(),
            storage_accounted_through_ms: through,
            retained_by_forks: false,
        };
        UsageEnvelope {
            v: 1,
            event_id: s.deterministic_event_id(),
            event_time_ms: through,
            emitted_ms: through,
            cell: "c".into(),
            payload: UsagePayload::SegmentSnapshot(s),
        }
    };
    r.apply_page(
        &[
            mk("2026-07", true, 100, aug1, 999),
            mk("2026-08", false, 400, aug1 + 86_400_000, 34_560_000_000),
        ],
        "c1",
    )
    .await
    .unwrap();
    let st = &r
        .stream_segment_states("acct", "proj", &id().stream_id)
        .await
        .unwrap()[0];
    assert_eq!(
        st.owned_frame_bytes_current, 400,
        "the live snapshot must win the same-version tie"
    );
    assert_eq!(st.storage_accounted_through_ms, aug1 + 86_400_000);
    // Close August: gauge 400 from Aug 2 to Sep 1 (30 more days).
    assert_eq!(r.close_month(2026, 8, 0).await.unwrap(), 1);
    let aug = r
        .month_row("2026-08", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    let day = 86_400_000u128;
    assert_eq!(
        aug.storage_byte_ms(),
        34_560_000_000 + 30 * day * 400,
        "the month close must extrapolate from the NEW gauge"
    );
}

/// Round-22 item 5: integer month allocation is exact — the sum of
/// per-month allocations equals the original for every dimension,
/// across two AND four month spans.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_batch_month_split_is_exact() {
    let r = UsageRollup::open(mem_store(), "split", &test_cfg())
        .await
        .unwrap();
    let aug1 = crate::billing::month_start_ms(2026, 8);
    // One byte, one record, one op split dead-center on a boundary:
    // must land as 0+1 or 1+0, never 1+1.
    let rb = ReadBatch {
        source: MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "bx".into(),
        },
        seq: 0,
        from_ms: aug1 - 5_000,
        to_ms: aug1 + 5_000,
        rows: vec![ReadRow {
            identity: id(),
            read_payload_bytes: 1,
            read_records: 1,
            read_operations: 1,
            queue_operations: 0,
            append_requests: 0,
        }],
    };
    let env = UsageEnvelope {
        v: 1,
        event_id: "read/bx/0".into(),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::ReadBatch(rb),
    };
    r.apply_page(&[env], "c1").await.unwrap();
    let jul = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap_or_default();
    let aug = r
        .month_row("2026-08", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap_or_default();
    assert_eq!(
        jul.read_payload_bytes + aug.read_payload_bytes,
        1,
        "one byte bills exactly once"
    );
    assert_eq!(jul.read_records + aug.read_records, 1);
    // A four-month batch (long outage) covers the INTERIOR months.
    let rb2 = ReadBatch {
        source: MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "by".into(),
        },
        seq: 0,
        from_ms: crate::billing::month_start_ms(2026, 1) + 10,
        to_ms: crate::billing::month_start_ms(2026, 4) + 10,
        rows: vec![ReadRow {
            identity: id(),
            read_payload_bytes: 1_000_003,
            read_records: 77,
            read_operations: 13,
            queue_operations: 5,
            append_requests: 9,
        }],
    };
    let env2 = UsageEnvelope {
        v: 1,
        event_id: "read/by/0".into(),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::ReadBatch(rb2),
    };
    r.apply_page(&[env2], "c2").await.unwrap();
    let mut sums = [0u64; 5];
    for m in ["2026-01", "2026-02", "2026-03", "2026-04"] {
        let row = r
            .month_row(m, "acct", "proj", &id().stream_id)
            .await
            .unwrap()
            .unwrap_or_default();
        sums[0] += row.read_payload_bytes;
        sums[1] += row.read_records;
        sums[2] += row.read_operations;
        sums[3] += row.queue_operations;
        sums[4] += row.append_requests;
        if m == "2026-02" || m == "2026-03" {
            assert!(row.read_payload_bytes > 0, "interior month {m} covered");
        }
    }
    assert_eq!(
        sums,
        [1_000_003, 77, 13, 5, 9],
        "sum preserved per dimension"
    );
}

/// Round-22 item 8: a rollup that was down across several
/// boundaries closes every overdue month IN ORDER from the
/// persisted oldest-unclosed marker, and the marker survives so
/// the next tick starts where this one stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missed_months_catch_up_in_order() {
    let _clock = ClockGuard::at(crate::billing::month_start_ms(2026, 12)).await;
    let r = UsageRollup::open(mem_store(), "catchup", &test_cfg())
        .await
        .unwrap();
    let jul15 = crate::billing::month_start_ms(2026, 7) + 14 * 86_400_000;
    let s = SegmentSnapshot {
        identity: id(),
        segment_id: 0,
        usage_version: 1,
        month: "2026-07".into(),
        month_final: false,
        ingest_payload_bytes_month: 10,
        ingest_records_month: 1,
        owned_frame_bytes_current: 100,
        storage_byte_ms_month: "0".into(),
        storage_accounted_through_ms: jul15,
        retained_by_forks: false,
    };
    let env = UsageEnvelope {
        v: 1,
        event_id: s.deterministic_event_id(),
        event_time_ms: jul15,
        emitted_ms: jul15,
        cell: "c".into(),
        payload: UsagePayload::SegmentSnapshot(s),
    };
    r.apply_page(&[env], "c1").await.unwrap();
    // The rollup "was down" July..November: one catch-up call at
    // Dec 1 closes Jul, Aug, Sep, Oct, Nov — in that order.
    let closed = r.close_months_due(0).await.unwrap();
    let months: Vec<&str> = closed.iter().map(|(m, _)| m.as_str()).collect();
    assert_eq!(
        months,
        vec!["2026-07", "2026-08", "2026-09", "2026-10", "2026-11"],
        "oldest first, no gaps"
    );
    assert!(
        closed.iter().all(|(_, n)| *n == 1),
        "every month closed the stream"
    );
    let day = 86_400_000u128;
    let nov = r
        .month_row("2026-11", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        nov.storage_byte_ms(),
        30 * day * 100,
        "idle November billed"
    );
    assert_eq!(
        r.pending_artifacts(64).await.unwrap().len(),
        5,
        "one artifact per closed month"
    );
    // Marker advanced: nothing further due.
    assert!(r.close_months_due(0).await.unwrap().is_empty());
}

/// Round-21 blocker 2, the reviewer's exact scenario: one July
/// write, then TOTAL silence. July, August and September must each
/// accrue the correct storage byte-time, each produce one artifact,
/// and the gauge must never read as zero. Re-closing any month is a
/// no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_retained_months_accrue_storage() {
    // Months close only after their boundary on the TRUSTED clock —
    // inject December so July-September are all closeable.
    let _clock = ClockGuard::at(crate::billing::month_start_ms(2026, 12)).await;
    let r = UsageRollup::open(mem_store(), "idle", &test_cfg())
        .await
        .unwrap();
    let jul15 = crate::billing::month_start_ms(2026, 7) + 14 * 86_400_000;
    // Live snapshot: gauge 100 B, accounted through Jul 15.
    let mut s0 = match snap(1, "2026-07", 500, 0, 100, false).payload {
        UsagePayload::SegmentSnapshot(x) => x,
        _ => unreachable!(),
    };
    s0.storage_accounted_through_ms = jul15;
    let env = UsageEnvelope {
        v: 1,
        event_id: s0.deterministic_event_id(),
        event_time_ms: jul15,
        emitted_ms: jul15,
        cell: "c".into(),
        payload: UsagePayload::SegmentSnapshot(s0),
    };
    r.apply_page(&[env], "c1").await.unwrap();

    let day = 86_400_000u128;
    // Close July: Jul 15 -> Aug 1 = 17 idle days at 100 B.
    assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 1);
    assert_eq!(pending_storage(&r, "2026-07").await, Some(17 * day * 100));
    // Close August: a FULLY idle month — no row existed until the
    // carry pass synthesized it. 31 days at 100 B.
    assert_eq!(
        r.close_month(2026, 8, 0).await.unwrap(),
        1,
        "an idle month must still close its stream"
    );
    assert_eq!(pending_storage(&r, "2026-08").await, Some(31 * day * 100));
    // September: 30 days.
    assert_eq!(r.close_month(2026, 9, 0).await.unwrap(), 1);
    assert_eq!(pending_storage(&r, "2026-09").await, Some(30 * day * 100));
    // The durable segment state still knows the gauge.
    let states = r
        .stream_segment_states("acct", "proj", &id().stream_id)
        .await
        .unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].owned_frame_bytes_current, 100);
    // Replays are no-ops.
    assert_eq!(r.close_month(2026, 8, 0).await.unwrap(), 0);
    // Aggregates carried the idle storage too.
    let aug_proj = r
        .project_row("2026-08", "acct", "proj")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aug_proj.storage_byte_ms, (31 * day * 100).to_string());
}

/// Snapshots are ABSOLUTE; aggregates absorb them as deltas, and a
/// replay applies as zero. Month close extrapolates idle gauges to
/// the boundary, finalizes, and emits exactly one artifact per
/// stream; a second close is a no-op.
#[expect(
    clippy::too_many_lines,
    reason = "rollup invoice lifecycle fixture; one ordered scenario follows frozen totals through late corrections and artifact publication; splitting it would duplicate durable state and weaken the cross-phase assertions"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollup_applies_deltas_and_closes_months() {
    let r = UsageRollup::open(mem_store(), "t1", &test_cfg())
        .await
        .unwrap();
    // Version 1: 100 bytes, byte-ms 5000. Version 2: 250 bytes,
    // byte-ms 9000 (absolute). Replay of version 2.
    r.apply_page(&[snap(1, "2026-07", 100, 5000, 40, false)], "c1")
        .await
        .unwrap();
    r.apply_page(&[snap(2, "2026-07", 250, 9000, 40, false)], "c2")
        .await
        .unwrap();
    r.apply_page(&[snap(2, "2026-07", 250, 9000, 40, false)], "c3")
        .await
        .unwrap();
    let row = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.ingest_bytes(), 250, "absolute, not summed");
    assert_eq!(row.storage_byte_ms(), 9000);
    let proj = r
        .project_row("2026-07", "acct", "proj")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proj.ingest_bytes, 250, "aggregate absorbed deltas once");
    assert_eq!(proj.storage_byte_ms, "9000");
    let name = r
        .name_row("2026-07", "acct", "proj", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name.incarnations, vec![id().stream_id]);

    // A read batch, then its duplicate: applied once.
    let rb = ReadBatch {
        source: MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "b1".into(),
        },
        seq: 0,
        from_ms: crate::billing::month_start_ms(2026, 7),
        to_ms: crate::billing::month_start_ms(2026, 7) + 5,
        rows: vec![ReadRow {
            identity: id(),
            read_payload_bytes: 77,
            read_records: 3,
            read_operations: 2,
            queue_operations: 1,
            append_requests: 4,
        }],
    };
    let env = UsageEnvelope {
        v: 1,
        event_id: "read/b1/0".into(),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::ReadBatch(rb),
    };
    r.apply_page(std::slice::from_ref(&env), "c4")
        .await
        .unwrap();
    r.apply_page(std::slice::from_ref(&env), "c5")
        .await
        .unwrap();
    let row = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.read_payload_bytes, 77, "source-seq dedupe");
    assert_eq!(row.append_requests, 4);

    // Close July (grace 0): the idle gauge extrapolates from
    // accounted_through to the boundary, the row finalizes, one
    // artifact per stream.
    let n = r.close_month(2026, 7, 0).await.unwrap();
    assert_eq!(n, 1);
    let artifacts = r.pending_artifacts(64).await.unwrap();
    assert_eq!(artifacts.len(), 1);
    let boundary = month_start_ms(2026, 8);
    let through = crate::billing::month_start_ms(2026, 7) + 1_000_000;
    let expect = 9000u128 + u128::try_from(boundary - through).unwrap() * 40;
    assert_eq!(
        artifacts[0].4.storage_byte_ms(),
        expect,
        "idle extrapolation to the boundary"
    );
    let closed = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert!(closed.finalized_at_ms.is_some());
    // Second close: nothing left to do.
    assert_eq!(r.close_month(2026, 7, 0).await.unwrap(), 0);

    // A correction after close appends explicitly; the base row is
    // never silently rewritten (§9.5).
    let corr = UsageEnvelope {
        v: 1,
        event_id: "corr/1".into(),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::UsageCorrection(crate::billing::UsageCorrection {
            identity: id(),
            month: "2026-07".into(),
            reason: "late read batch".into(),
            correction_id: String::new(), // filled from the envelope
            correction_version: 0,
            source_event_id: String::new(),
            created_at_ms: 0,
            ingest_payload_bytes_delta: 0,
            ingest_records_delta: 0,
            read_payload_bytes_delta: 12,
            read_records_delta: 0,
            read_operations_delta: 0,
            queue_operations_delta: 0,
            append_requests_delta: 0,
            storage_byte_ms_delta: "0".into(),
        }),
    };
    r.apply_page(&[corr], "c6").await.unwrap();
    let corrected = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(corrected.corrections.len(), 1);
    assert_eq!(
        corrected.read_payload_bytes, 77,
        "the base number is untouched; the correction is explicit"
    );

    // Round-21 blocker 8: LATE DATA after finalization converts to
    // corrections automatically — no manual envelope needed.
    r.apply_page(&[snap(3, "2026-07", 300, 10_000, 40, false)], "c7")
        .await
        .unwrap();
    let after_snap = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    let frozen = after_snap.frozen.clone().expect("frozen at finalization");
    assert_eq!(
        frozen.ingest_bytes, 250,
        "the frozen invoice base never moves"
    );
    assert_eq!(after_snap.corrections.len(), 2);
    let reconciliation = r.reconcile_month("2026-07").await.unwrap();
    assert!(
        reconciliation.ok,
        "late snapshot must reconcile: {reconciliation:?}"
    );
    let c = &after_snap.corrections[1];
    assert_eq!(c.ingest_payload_bytes_delta, 50);
    // Storage corrects ZERO here: finalization already extrapolated
    // this segment's byte-time past the late snapshot's absolute —
    // the late value is not additive news (and negative corrections
    // are deliberately not synthesized).
    assert_eq!(c.storage_byte_ms_delta, "0");
    // Replaying the same late snapshot corrects ZERO more times.
    r.apply_page(&[snap(3, "2026-07", 300, 10_000, 40, false)], "c8")
        .await
        .unwrap();
    let replay = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.corrections.len(), 2, "late replay corrected twice");

    // ...and a late READ batch corrects too.
    let late_rb = ReadBatch {
        source: MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "b2".into(),
        },
        seq: 0,
        from_ms: crate::billing::month_start_ms(2026, 7) + 10,
        to_ms: crate::billing::month_start_ms(2026, 7) + 20,
        rows: vec![ReadRow {
            identity: id(),
            read_payload_bytes: 9,
            read_records: 1,
            read_operations: 1,
            queue_operations: 0,
            append_requests: 0,
        }],
    };
    let late_env = UsageEnvelope {
        v: 1,
        event_id: "read/b2/0".into(),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::ReadBatch(late_rb),
    };
    r.apply_page(&[late_env], "c9").await.unwrap();
    let after_read = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_read.read_payload_bytes, 77, "base reads untouched");
    assert_eq!(after_read.corrections.len(), 3);
    assert_eq!(after_read.corrections[2].read_payload_bytes_delta, 9);

    // Two-phase publication (blocker 7): pending rows survive until
    // published with Create, the retry is idempotent, and the
    // pending queue empties.
    let store = mem_store();
    let n = crate::billing::publish_artifacts(&r, &store, "t1")
        .await
        .unwrap();
    assert_eq!(n, 4, "1 monthly + 3 correction artifacts");
    assert!(r.pending_artifacts(64).await.unwrap().is_empty());
    assert!(
        r.pending_correction_artifacts(64).await.unwrap().is_empty(),
        "correction artifacts drained"
    );
    assert_eq!(
        crate::billing::publish_artifacts(&r, &store, "t1")
            .await
            .unwrap(),
        0,
        "re-publication finds nothing pending"
    );
    use object_store::ObjectStoreExt;
    let path = object_store::path::Path::from(format!(
        "t1/telemetry/usage-monthly/acct/proj/{}/2026-07.json",
        id().stream_id
    ));
    let got = store.get(&path).await.expect("artifact object exists");
    let body = got.bytes().await.unwrap();
    let art: MonthRow = serde_json::from_slice(&body).unwrap();
    assert!(art.frozen.is_some(), "the artifact is the frozen row");

    // Round-22 item 8: corrections carry full provenance, the
    // materialized sums cover every dimension, aggregates carry
    // the same sums, effective = frozen + corrections, and each
    // correction is its own immutable object.
    let row = r
        .month_row("2026-07", "acct", "proj", &id().stream_id)
        .await
        .unwrap()
        .unwrap();
    for c in &row.corrections {
        assert!(!c.correction_id.is_empty(), "correction_id set");
        assert_eq!(c.correction_version, 1);
        assert!(!c.source_event_id.is_empty(), "source event recorded");
    }
    assert_eq!(row.corr.read_payload_bytes_delta, 12 + 9);
    assert_eq!(row.corr.ingest_payload_bytes_delta, 50);
    assert_eq!(row.corr.count, 3);
    let eff = row.effective();
    let frozen_b = row.frozen.as_ref().unwrap().read_payload_bytes;
    assert_eq!(
        eff["readPayloadBytes"].as_u64().unwrap(),
        frozen_b + 12 + 9,
        "effective = frozen base + corrections"
    );
    let agg = r
        .project_row("2026-07", "acct", "proj")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        agg.corr.read_payload_bytes_delta,
        12 + 9,
        "project aggregate carries correction sums"
    );
    let cpath = object_store::path::Path::from(format!(
        "t1/telemetry/usage-monthly/acct/proj/{}/2026-07.corrections/corr~1.json",
        id().stream_id
    ));
    let cbody = store
        .get(&cpath)
        .await
        .expect("correction artifact")
        .bytes()
        .await
        .unwrap();
    let cart: crate::billing::UsageCorrection = serde_json::from_slice(&cbody).unwrap();
    assert_eq!(cart.read_payload_bytes_delta, 12);

    // Content verification (round-22 item 8a): an AlreadyExists
    // whose bytes DIFFER is a mismatch — never marked published.
    let clash =
        object_store::path::Path::from("t2/telemetry/usage-monthly/acct/proj/x/2026-01.json");
    store
        .put(&clash, object_store::PutPayload::from(b"tampered".to_vec()))
        .await
        .unwrap();
    // Manufacture a pending row aimed at that path.
    {
        let mut wb = WriteBatch::new();
        wb.put(
            b"artifact-pending/2026-01/acct/proj/x",
            serde_json::to_vec(&MonthRow::default()).unwrap(),
        );
        r.db.write(wb).await.unwrap();
    }
    let before = crate::billing::ARTIFACT_MISMATCHES.load(std::sync::atomic::Ordering::Relaxed);
    let n2 = crate::billing::publish_artifacts(&r, &store, "t2")
        .await
        .unwrap();
    assert_eq!(n2, 0, "mismatched artifact must NOT publish");
    assert_eq!(
        crate::billing::ARTIFACT_MISMATCHES.load(std::sync::atomic::Ordering::Relaxed),
        before + 1,
        "mismatch counted for the alert"
    );
    assert_eq!(
        r.pending_artifacts(64).await.unwrap().len(),
        1,
        "row stays pending for the operator"
    );
}
