//! Item 35: a stream whose stored row fails admission backs off alone;
//! the lane-mates gathered beside it retire with the same flush.
use super::{Absorber, PendingAbsorb};
use crate::history::{AbsorberConfig, absorber_channel};
use crate::shard::{AppendFinish, AppendReq, ShardConfig, ShardEngine, record_key};
use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::WriteBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn append_one(engine: &ShardEngine, hash: [u8; 16]) {
    let (reply, ack) = tokio::sync::oneshot::channel();
    let request = AppendReq {
        usage: Default::default(),
        hash,
        route: hash,
        enqueued_at: Instant::now(),
        entries: vec![Bytes::from_static(b"lane isolation payload")],
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 1,
        subkey: [7; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 22,
        finish: AppendFinish::Open,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        seal_gen: None,
        billing: None,
        resp: reply,
    };
    assert!(engine.try_enqueue(request).is_ok(), "enqueue");
    ack.await.unwrap().unwrap();
}

async fn overwrite_first_row(engine: &ShardEngine, hash: [u8; 16]) {
    let mut batch = WriteBatch::new();
    batch.put(record_key(&hash, 0), b"invalid frame");
    let written = engine.db.write(batch).await.unwrap();
    written.await_durable().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_row_backs_off_only_its_stream() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let settings = slatedb::config::Settings {
        flush_interval: Some(Duration::from_millis(5)),
        ..Default::default()
    };
    let db = slatedb::Db::builder("item35-lane", store.clone())
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    let (absorb_tx, _absorb_rx) = absorber_channel();
    let shard_cfg = ShardConfig {
        tail_ring_bytes: 0,
        ..Default::default()
    };
    let engine = ShardEngine::start(
        "item35-lane".into(),
        Arc::new(db),
        store.clone(),
        shard_cfg,
        absorb_tx,
        None,
        Default::default(),
    );
    let (a, bad, c) = ([0xC1; 16], [0xC2; 16], [0xC3; 16]);
    for hash in [a, bad, c] {
        append_one(&engine, hash).await;
    }
    overwrite_first_row(&engine, bad).await;
    let cfg = AbsorberConfig::default();
    let tick = cfg.tick;
    let absorber = Absorber::new(engine.clone(), cfg);
    let now = Instant::now();
    let entry = || PendingAbsorb {
        bytes: 1,
        since: now,
        failures: 0,
        retry_after: None,
    };
    let mut pending: HashMap<_, _> = [a, bad, c].into_iter().map(|h| (h, entry())).collect();
    absorber.gather_due(&mut pending, now, &[a, bad, c]).await;
    assert!(
        !pending.contains_key(&a) && !pending.contains_key(&c),
        "a lane-mate of a corrupt row must retire with the flush that absorbed it"
    );
    let backoff = |p: &PendingAbsorb| (p.failures, p.retry_after);
    assert_eq!(
        pending.get(&bad).map(backoff),
        Some((1, Some(now + tick * 2))),
        "the corrupt stream backs off alone"
    );
    // Alone in its lane, it fails again and its backoff doubles.
    let later = now + tick * 2;
    absorber.gather_due(&mut pending, later, &[bad]).await;
    assert_eq!(
        pending.get(&bad).map(backoff),
        Some((2, Some(later + tick * 4))),
        "its backoff doubles on its next failure"
    );
    engine.begin_close();
}
