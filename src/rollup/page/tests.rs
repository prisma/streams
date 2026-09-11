use crate::billing::{
    BillingIdentity, MeterSource, ReadBatch, ReadRow, UsageEnvelope, UsagePayload, month_start_ms,
};
use crate::rollup::{UsageRollup, k_month, k_name, k_project, k_source, read_faults};
use slatedb::Db;
use std::sync::Arc;

fn batch(seq: u64, stream_id: &str) -> UsageEnvelope {
    UsageEnvelope {
        v: 1,
        event_id: format!("read/boot/{seq}"),
        event_time_ms: 0,
        emitted_ms: 0,
        cell: "c".into(),
        payload: UsagePayload::ReadBatch(ReadBatch {
            source: MeterSource {
                cell: "c".into(),
                instance: "i".into(),
                boot: "boot".into(),
            },
            seq,
            from_ms: month_start_ms(2026, 7),
            to_ms: month_start_ms(2026, 7) + 100,
            rows: vec![ReadRow {
                identity: BillingIdentity {
                    account_id: "a".into(),
                    project_id: "p".into(),
                    stream_id: stream_id.into(),
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

async fn database_rows(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut scan = db.scan(..).await.unwrap();
    while let Some(row) = scan.next().await.unwrap() {
        rows.push((row.key.to_vec(), row.value.to_vec()));
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_page_shares_dedupe_month_and_aggregate_state() {
    let db = Arc::new(
        Db::builder(
            "page-shared",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let rollup = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    let envelopes = [
        batch(0, "first"),
        batch(0, "first"),
        batch(1, "second"),
        batch(2, "first"),
    ];
    rollup.apply_page(&envelopes, "all-three").await.unwrap();
    assert_eq!(rollup.cursor().await.unwrap().as_deref(), Some("all-three"));
    let first = rollup
        .month_row("2026-07", "a", "p", "first")
        .await
        .unwrap()
        .unwrap();
    let second = rollup
        .month_row("2026-07", "a", "p", "second")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (first.read_payload_bytes, second.read_payload_bytes),
        (202, 101)
    );
    let name = rollup
        .name_row("2026-07", "a", "p", "orders")
        .await
        .unwrap()
        .unwrap();
    let project = rollup
        .project_row("2026-07", "a", "p")
        .await
        .unwrap()
        .unwrap();
    for row in [&name, &project] {
        assert_eq!(
            (
                row.read_payload_bytes,
                row.read_records,
                row.read_operations,
                row.queue_operations,
                row.append_requests
            ),
            (303, 9, 6, 12, 15)
        );
    }
    assert_eq!(name.incarnations, ["first", "second"]);
    assert!(project.incarnations.is_empty());
    let before = database_rows(&db).await;
    rollup.apply_page(&envelopes, "all-three").await.unwrap();
    assert_eq!(database_rows(&db).await, before);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_later_envelope_failure_discards_all_earlier_staged_rows() {
    let db = Arc::new(
        Db::builder(
            "page-abandon",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let rollup = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    rollup
        .apply_page(&[batch(0, "original")], "before")
        .await
        .unwrap();
    let before = database_rows(&db).await;
    // Each key belongs to the second envelope, after the first has staged a
    // source floor, month, name and project. None may escape the failed page.
    let mut later = batch(0, "late");
    let UsagePayload::ReadBatch(ref mut read) = later.payload else {
        unreachable!()
    };
    read.source.boot = "later-boot".into();
    read.rows[0].identity.project_id = "later-project".into();
    for key in [
        k_source("later-boot"),
        k_month("2026-07", "a", "later-project", "late"),
        k_name("2026-07", "a", "later-project", "orders"),
        k_project("2026-07", "a", "later-project"),
    ] {
        read_faults()
            .lock()
            .unwrap()
            .insert((Arc::as_ptr(&db) as usize, key));
        assert!(
            rollup
                .apply_page(&[batch(1, "original"), later.clone()], "after")
                .await
                .is_err()
        );
        assert_eq!(database_rows(&db).await, before);
    }
    rollup
        .apply_page(&[batch(1, "original"), later], "after")
        .await
        .unwrap();
    assert_eq!(rollup.cursor().await.unwrap().as_deref(), Some("after"));
    assert_eq!(
        rollup
            .month_row("2026-07", "a", "p", "original")
            .await
            .unwrap()
            .unwrap()
            .read_payload_bytes,
        202
    );
    db.close().await.unwrap();
}
