#![cfg(test)]

use super::{SegmentState, UsageRollup, close_scan_range, k_segment, month_start_ms, read_faults};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r23_seek_boundaries_are_exclusive_and_prefix_bounded() {
    let db = Db::builder(
        "r23-boundary",
        Arc::new(object_store::memory::InMemory::new()),
    )
    .build()
    .await
    .unwrap();
    for key in [
        "month/2026-06/a",
        "month/2026-07/a",
        "month/2026-07/b",
        "month/2026-08/a",
    ] {
        db.put(key, b"row").await.unwrap();
    }
    let prefix = b"month/2026-07/";
    let mut it = db
        .scan_prefix(
            prefix,
            close_scan_range(prefix, Some(b"month/2026-07/a")).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        it.next().await.unwrap().unwrap().key.as_ref(),
        b"month/2026-07/b"
    );
    assert!(it.next().await.unwrap().is_none());
    assert!(close_scan_range(prefix, Some(b"month/2026-06/a")).is_err());
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r23_close_visits_scale_linearly_across_chunk_restart() {
    for n in [1001usize, 2002] {
        let db = Arc::new(
            Db::builder(
                format!("r23-{n}"),
                Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
            .unwrap(),
        );
        let mut wb = WriteBatch::new();
        for segment in 0..n {
            wb.put(
                k_segment("a", "p", "s", u32::try_from(segment).unwrap()),
                serde_json::to_vec(&SegmentState {
                    account_id: "a".into(),
                    stream_name: "orders".into(),
                    owned_frame_bytes_current: 1,
                    storage_accounted_through_ms: month_start_ms(2026, 7),
                    ..Default::default()
                })
                .unwrap(),
            );
        }
        db.write(wb).await.unwrap();
        let first = UsageRollup {
            db: db.clone(),
            close_rows_visited: Default::default(),
        };
        read_faults().lock().unwrap().insert((
            Arc::as_ptr(&db) as usize,
            b"stop-after-close-chunk".to_vec(),
        ));
        assert!(first.close_month(2026, 7, 0).await.is_err());
        let visited = first.close_rows_visited();
        assert_eq!(visited, 1000);
        drop(first);
        let reopened = UsageRollup {
            db: db.clone(),
            close_rows_visited: Default::default(),
        };
        assert_eq!(reopened.close_month(2026, 7, 0).await.unwrap(), 1);
        assert_eq!(
            visited + reopened.close_rows_visited(),
            u64::try_from(n).unwrap() + 1,
            "one visit per segment plus one shared monthly row"
        );
        let month = reopened
            .month_row("2026-07", "a", "p", "s")
            .await
            .unwrap()
            .unwrap();
        let span = u128::try_from(month_start_ms(2026, 8) - month_start_ms(2026, 7)).unwrap();
        assert_eq!(month.storage_byte_ms(), span * n as u128);
        assert_eq!(month.segments.len(), n);
        db.close().await.unwrap();
    }
}
