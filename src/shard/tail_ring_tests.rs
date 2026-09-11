//! The durable-tail ring's eviction bookkeeping across a gap reset.
use crate::shard::{ShardConfig, ShardEngine, ShardMaintenance, StreamHandle};
use bytes::Bytes;
use slatedb::Db;
use std::sync::Arc;

async fn engine(tail_ring_bytes: usize) -> Arc<ShardEngine> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("tail-ring-tests", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    ShardEngine::start(
        "tail-ring-tests".into(),
        db,
        store,
        ShardConfig {
            tail_ring_bytes,
            ..Default::default()
        },
        tx,
        None,
        ShardMaintenance::default(),
    )
}

/// `n` opaque frames of `size` bytes at consecutive offsets from `from`.
fn frames(from: u64, n: u64, size: usize) -> Vec<(u64, Bytes)> {
    (from..from + n)
        .map(|offset| (offset, Bytes::from(vec![0u8; size])))
        .collect()
}

fn batch_starts(handle: &StreamHandle) -> Vec<u64> {
    handle
        .ring
        .lock()
        .unwrap()
        .batches
        .iter()
        .map(|b| b.first)
        .collect()
}

/// A gap reset drops the reset stream's stale batches and forgets only
/// ITS eviction slot: the other streams keep theirs, so budget pressure
/// still evicts them in publish order while the reset stream's later
/// batches survive behind them.
#[tokio::test]
async fn gap_reset_keeps_the_other_streams_eviction_slots() {
    let engine = engine(3 * 1024).await;
    let a = engine.stream_handle([1; 16]).await.unwrap();
    let b = engine.stream_handle([2; 16]).await.unwrap();
    engine.ring_publish(&a, &frames(0, 1, 1024));
    engine.ring_publish(&b, &frames(0, 1, 1024));
    // A's next batch skips offsets 1..5: its stale batch is dropped and its
    // slot re-queued behind B's.
    engine.ring_publish(&a, &frames(5, 1, 1024));
    assert_eq!(batch_starts(&a), vec![5]);
    assert_eq!(engine.ring_resident_bytes(), 2 * 1024);
    // Two more frames overrun the budget by one: the slot at the front of
    // the queue, B's, is evicted; A's batches are untouched.
    engine.ring_publish(&a, &frames(6, 2, 1024));
    assert_eq!(
        batch_starts(&a),
        vec![5, 6],
        "the reset stream keeps its later batches"
    );
    assert!(
        batch_starts(&b).is_empty(),
        "the other stream's slot was evicted first"
    );
    assert_eq!(engine.ring_resident_bytes(), 3 * 1024);
}
