//! R09: dirty and final pages are bounded and partial acks preserve debt.
#![cfg(test)]
use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt; the scenario pins one ordered sequence of dirty and final pages and a partial acknowledgement; helper phases would hide which page each bound is checked against"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r09-outbox", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r09-outbox".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let mut batch = WriteBatch::new();
    for id in 0..130u64 {
        let mut hash = [0; 16];
        hash[..8].copy_from_slice(&id.to_be_bytes());
        batch.put(crate::billing::usage_dirty_key(&hash), 8u64.to_le_bytes());
    }
    let hash = [0; 16];
    let meta = crate::billing::SegmentBillingMetaV1 {
        v: 1,
        stream_id: "s".into(),
        usage_version: 8,
        month_year: 2026,
        month_month: 7,
        ..Default::default()
    };
    batch.put(
        crate::billing::billing_meta_key(&hash),
        serde_json::to_vec(&meta).unwrap(),
    );
    for n in 0..35u32 {
        batch.put(
            crate::billing::usage_month_final_key(&hash, 2020 + (n / 12) as i32, n % 12 + 1),
            serde_json::to_vec(&meta.to_snapshot(true)).unwrap(),
        );
    }
    db.write(batch).await.unwrap();
    assert!(engine.has_billing_debt().await.unwrap());
    let (first, more) = engine.usage_dirty_page(None, 64).await.unwrap();
    assert!(more);
    assert_eq!(first.len(), 64);
    let (second, more) = engine
        .usage_dirty_page(Some(first.last().unwrap().0), 64)
        .await
        .unwrap();
    assert!(more);
    assert_eq!(second.len(), 64);
    assert!(first.last().unwrap().0 < second[0].0);
    let (last, more) = engine
        .usage_dirty_page(Some(second.last().unwrap().0), 64)
        .await
        .unwrap();
    assert!(!more);
    assert_eq!(last.len(), 2);
    let (finals, more) = engine.usage_month_finals_page(hash, 32).await.unwrap();
    assert!(more);
    assert_eq!(finals.len(), 32);
    engine
        .commit_group(
            vec![CommitOp::UsageAck {
                hash,
                scope: UsageAckScope::FinalRowsOnly,
                month_final_keys: finals.into_iter().map(|(key, _)| key).collect(),
            }],
            &ShardConfig::default(),
        )
        .await;
    let (remaining, more) = engine.usage_month_finals_page(hash, 32).await.unwrap();
    assert!(!more);
    assert_eq!(remaining.len(), 3);
    assert_eq!(
        db.get(crate::billing::usage_dirty_key(&hash))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        &8u64.to_le_bytes()
    );
    engine
        .commit_group(
            vec![CommitOp::UsageAck {
                hash,
                scope: UsageAckScope::ThroughVersion(8),
                month_final_keys: remaining.into_iter().map(|(key, _)| key).collect(),
            }],
            &ShardConfig::default(),
        )
        .await;
    assert!(
        db.get(crate::billing::usage_dirty_key(&hash))
            .await
            .unwrap()
            .is_none()
    );
    engine.begin_close();
    let _ = db.close().await;
}
