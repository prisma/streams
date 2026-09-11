//! R09: dirty-index discovery pages progress without exceeding the pending capacity.
#![cfg(test)]
use super::*;
use slatedb::WriteBatch;

#[test]
fn r09_hot_prefix_cannot_starve_other_due_streams() {
    let now = Instant::now();
    let cfg = AbsorberConfig::default();
    let pending: HashMap<_, _> = (0..6u8)
        .map(|id| {
            (
                [id; 16],
                PendingAbsorb {
                    bytes: u64::MAX - id as u64,
                    since: now,
                    failures: 0,
                    retry_after: None,
                },
            )
        })
        .collect();
    let first: Vec<_> = due_streams(&pending, &cfg, now, None)
        .into_iter()
        .take(3)
        .map(|(hash, _)| hash)
        .collect();
    let second: Vec<_> = due_streams(&pending, &cfg, now, first.last().copied())
        .into_iter()
        .take(3)
        .map(|(hash, _)| hash)
        .collect();
    assert_eq!(first, vec![[0; 16], [1; 16], [2; 16]]);
    assert_eq!(second, vec![[3; 16], [4; 16], [5; 16]]);
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "r09_discovery_pages_progress_without_exceeding_pending_capacity; the fixture's stream ids are small loop counters far inside every width they convert to; checked conversions would only restate the loop bounds"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r09_discovery_pages_progress_without_exceeding_pending_capacity; the fixture closes its database best effort once the assertions are done; a handled close would only restate the teardown"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r09_discovery_pages_progress_without_exceeding_pending_capacity() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r09-history", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let mut batch = WriteBatch::new();
    for id in 0..260u64 {
        let mut hash = [0; 16];
        hash[..8].copy_from_slice(&id.to_be_bytes());
        let marker = crate::shard::dirty_value_for_tests(&crate::shard::StreamMaintenance {
            next: 1,
            unabsorbed_bytes: 64,
            ..Default::default()
        });
        let width = [16, 24, 32][id as usize % 3];
        batch.put(crate::shard::dirty_key(&hash), &marker[..width]);
        batch.put(
            crate::shard::tail_key(&hash),
            crate::shard::encode_tail_for_tests(&crate::shard::TailFields {
                next: 1,
                unabsorbed_bytes: 64,
                route: [1; 16],
                ..Default::default()
            }),
        );
    }
    db.write(batch).await.unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let engine = ShardEngine::start(
        "r09-history".into(),
        db.clone(),
        store.clone(),
        crate::shard::ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let absorber = Absorber::new(
        store,
        engine.clone(),
        Arc::new(KeyCache::default()),
        AbsorberConfig::default(),
    );
    let mut pending = HashMap::new();
    assert_eq!(
        absorber.seed_from_dirty_index(&mut pending).await.unwrap(),
        DISCOVERY_PAGE_STREAMS
    );
    assert_eq!(pending.len(), DISCOVERY_PAGE_STREAMS);
    assert!(absorber.discovery_after.lock().unwrap().is_some());
    assert_eq!(
        absorber.seed_from_dirty_index(&mut pending).await.unwrap(),
        4
    );
    assert_eq!(pending.len(), 260);
    assert!(absorber.discovery_after.lock().unwrap().is_none());
    pending.clear();
    for id in 0..MAX_PENDING_STREAMS {
        let mut hash = [255; 16];
        hash[..8].copy_from_slice(&(id as u64).to_be_bytes());
        pending.insert(
            hash,
            PendingAbsorb {
                bytes: 1,
                since: Instant::now(),
                failures: 0,
                retry_after: None,
            },
        );
    }
    absorber.seed_from_dirty_index(&mut pending).await.unwrap();
    assert_eq!(pending.len(), MAX_PENDING_STREAMS);
    engine.begin_close();
    let _ = db.close().await;
}
