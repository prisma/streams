//! The durable-tail ring's eviction bookkeeping across a gap reset, and the
//! pages a ring read inspects.
use crate::crypto::{StreamKey, derive_subkey};
use crate::crypto_page::{PageCipher, PageLane, stamped};
use crate::shard::{RingBatch, RingScan, ShardConfig, ShardEngine, ShardMaintenance, StreamHandle};
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

/// A batch of `n` opaque one-record pages of `size` bytes at consecutive
/// offsets from `from`.
fn frames(from: u64, n: u64, size: usize) -> RingBatch {
    let mut batch = RingBatch::default();
    for offset in from..from + n {
        batch.push_page(offset, offset, Bytes::from(vec![0u8; size]));
    }
    batch
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

/// The stored page of the one record at `offset`, as the shard log seals it.
fn sealed_page(offset: u64) -> Bytes {
    let subkey = derive_subkey(&StreamKey([7; 32]), &[9; 16], "", 1);
    let lane = PageLane {
        key_version: 1,
        routing_key: "",
    };
    let page = PageCipher::new(&subkey, &[8; 16])
        .seal(&lane, offset, &stamped(0, &[b"record"]))
        .unwrap();
    Bytes::from(page.bytes)
}

/// A ring read inspects no batch that starts at its window's end: the
/// window [0, 2) is served from its own batch although the next batch, at
/// 2, holds a page that fails admission. The control: the window [0, 3),
/// which reaches that page, is refused, so the served window never
/// admitted it.
#[tokio::test]
async fn a_ring_read_inspects_no_batch_from_its_window_end() {
    let engine = engine(1 << 20).await;
    let handle = engine.stream_handle([3; 16]).await.unwrap();
    let mut window = RingBatch::default();
    window.push_page(0, 0, sealed_page(0));
    window.push_page(1, 1, sealed_page(1));
    engine.ring_publish(&handle, &window);
    let mut beyond = RingBatch::default();
    beyond.push_page(2, 2, Bytes::from_static(b"broken"));
    engine.ring_publish(&handle, &beyond);
    handle.state.lock().unwrap().durable.next = 3;
    let scan = |to| RingScan {
        from: 0,
        to,
        max_bytes: usize::MAX,
    };
    let hit = engine.ring_read(&handle, scan(2), None).unwrap();
    let served: Vec<(u64, u64)> = hit.frames.iter().map(|s| (s.first(), s.last())).collect();
    assert_eq!(served, vec![(0, 0), (1, 1)]);
    assert_eq!(hit.last_offset, Some(1));
    assert!(engine.ring_read(&handle, scan(3), None).is_none());
}
