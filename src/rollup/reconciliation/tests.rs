#![cfg(test)]

use super::{ReconcileReport, Reconciliation};
use crate::rollup::{AggRow, CorrTotals, FrozenTotals, MonthRow, SegMonth};

fn comparison() -> Reconciliation {
    Reconciliation {
        report: ReconcileReport {
            month: "2026-07".into(),
            ..Default::default()
        },
        computed: Default::default(),
    }
}

#[test]
fn frozen_base_and_corrections_match_served_meters() {
    let row = MonthRow {
        segments: [(
            0,
            SegMonth {
                ingest_bytes: 300,
                ingest_records: 30,
                ..Default::default()
            },
        )]
        .into(),
        frozen: Some(FrozenTotals {
            ingest_bytes: 250,
            ingest_records: 25,
            read_payload_bytes: 12,
            read_records: 3,
            read_operations: 2,
            queue_operations: 4,
            append_requests: 5,
            ..Default::default()
        }),
        corr: CorrTotals {
            ingest_payload_bytes_delta: 50,
            ingest_records_delta: 5,
            read_payload_bytes_delta: -2,
            read_records_delta: 1,
            read_operations_delta: 2,
            queue_operations_delta: -1,
            append_requests_delta: 3,
            ..Default::default()
        },
        ..Default::default()
    };
    let aggregate = AggRow {
        ingest_bytes: 250,
        ingest_records: 25,
        read_payload_bytes: 12,
        read_records: 3,
        read_operations: 2,
        queue_operations: 4,
        append_requests: 5,
        corr: row.corr.clone(),
        ..Default::default()
    };
    let mut check = comparison();
    check.stream(b"month/2026-07/a/s", &serde_json::to_vec(&row).unwrap());
    check.project(
        b"container/2026-07/a",
        &serde_json::to_vec(&aggregate).unwrap(),
    );
    assert!(check.finish().ok);
    assert_eq!(
        row.effective(),
        serde_json::json!({"ingestPayloadBytes":300,
        "ingestRecords":30,"readPayloadBytes":10,"readRecords":4,
        "readOperations":4,"queueOperations":3,"appendRequests":8,
        "storageByteSeconds":"0","correctionCount":0})
    );
}

#[test]
fn container_qualified_comparison_reports_missing_and_disagreeing_rows() {
    let row = MonthRow {
        read_payload_bytes: 5,
        ..Default::default()
    };
    let mut check = comparison();
    for container in ["a", "b"] {
        check.stream(
            format!("month/2026-07/{container}/s").as_bytes(),
            &serde_json::to_vec(&row).unwrap(),
        );
    }
    check.project(
        b"container/2026-07/a",
        &serde_json::to_vec(&AggRow {
            read_payload_bytes: 5,
            ..Default::default()
        })
        .unwrap(),
    );
    check.project(
        b"container/2026-07/c",
        &serde_json::to_vec(&AggRow::default()).unwrap(),
    );
    let report = check.finish();
    assert!(!report.ok);
    assert_eq!(report.stream_rows, 2);
    assert_eq!(report.projects, 2);
    assert_eq!(
        report.mismatches,
        [
            "aggregate without stream rows: c",
            "stream rows without an aggregate: b"
        ]
    );

    let mut check = comparison();
    check.stream(b"month/2026-07/a/s", &serde_json::to_vec(&row).unwrap());
    check.project(
        b"container/2026-07/a",
        &serde_json::to_vec(&AggRow::default()).unwrap(),
    );
    let report = check.finish();
    assert_eq!(report.mismatches.len(), 1);
    assert!(report.mismatches[0].starts_with("totals disagree for a:"));
}

#[test]
fn malformed_keys_and_rows_remain_reported() {
    let mut check = comparison();
    check.stream(b"month/2026-07", b"{}");
    check.stream(b"month/2026-07/a/s", b"[");
    check.project(b"container/2026-07", b"{}");
    check.project(b"container/2026-07/a", b"[");
    let report = check.finish();
    assert!(!report.ok);
    assert_eq!(report.stream_rows, 0);
    assert_eq!(report.projects, 0);
    assert_eq!(
        report.mismatches,
        [
            "unparseable month key: month/2026-07",
            "undecodable month row: month/2026-07/a/s",
            "unparseable container key: container/2026-07",
            "undecodable container aggregate: container/2026-07/a"
        ]
    );
}

#[test]
fn signed_corrections_saturate_at_the_invoice_bounds() {
    use crate::rollup::{eff_u64, eff_u128};
    assert_eq!(eff_u64(1, i64::MIN), 0);
    assert_eq!(eff_u64(u64::MAX, 1), u64::MAX);
    assert_eq!(eff_u64(15, -5), 10);
    assert_eq!(eff_u64(15, 5), 20);
    assert_eq!(eff_u128(1, &i128::MIN.to_string()), 0);
    assert_eq!(eff_u128(u128::MAX, "1"), u128::MAX);
    assert_eq!(eff_u128(15, "-5"), 10);
    assert_eq!(eff_u128(15, "5"), 20);
    assert_eq!(eff_u128(15, ""), 15);
}
