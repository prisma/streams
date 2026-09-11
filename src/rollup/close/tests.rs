#![cfg(test)]

use super::MonthClose;
use crate::rollup::{SegmentState, UsageRollup};
use slatedb::{Db, WriteBatch};
use std::sync::Arc;

#[tokio::test]
async fn byte_bound_pages_resume_exclusively_and_always_make_progress() {
    let db = Arc::new(
        Db::builder(
            "close-byte-cap",
            Arc::new(object_store::memory::InMemory::new()),
        )
        .build()
        .await
        .unwrap(),
    );
    let mut batch = WriteBatch::new();
    let state = SegmentState {
        stream_name: "x".repeat(600_000),
        ..Default::default()
    };
    let value = serde_json::to_vec(&state).unwrap();
    for key in [b"segment/a", b"segment/b", b"segment/c"] {
        batch.put(key, value.clone());
    }
    db.write(batch).await.unwrap();
    let rollup = UsageRollup {
        db: db.clone(),
        close_rows_visited: Default::default(),
    };
    let close = MonthClose {
        rollup: &rollup,
        month: "2026-07".into(),
        start: 0,
        boundary: 1,
        now: 2,
    };
    let first = close
        .read_page::<SegmentState>(b"segment/", None)
        .await
        .unwrap();
    assert_eq!(
        first.len(),
        2,
        "the byte cap must stop before the third row"
    );
    assert_eq!(first[1].0, b"segment/b");
    let last = close
        .read_page::<SegmentState>(b"segment/", Some(&first[1].0))
        .await
        .unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].0, b"segment/c");
    assert!(
        close
            .read_page::<SegmentState>(b"segment/", Some(&last[0].0))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(rollup.close_rows_visited(), 3);
    let oversized = SegmentState {
        stream_name: "x".repeat(1_100_000),
        ..Default::default()
    };
    db.put(b"segment/z", serde_json::to_vec(&oversized).unwrap())
        .await
        .unwrap();
    let oversized_page = close
        .read_page::<SegmentState>(b"segment/", Some(&last[0].0))
        .await
        .unwrap();
    assert_eq!(
        oversized_page.len(),
        1,
        "a single oversized row must still advance"
    );
    assert_eq!(oversized_page[0].0, b"segment/z");
    db.close().await.unwrap();
}
