//! R09: dirty and final pages are bounded and partial acks preserve debt.
#![cfg(test)]
use super::*;

#[expect(
    clippy::let_underscore_must_use,
    reason = "r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        crate::keyspace::db_builder("r09-outbox", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let meta = crate::billing::SegmentBillingMetaV1 {
        v: 1,
        stream_id: "s".into(),
        usage_version: 8,
        month_year: 2026,
        month_month: 7,
        ..Default::default()
    };
    let mut batch = WriteBatch::new();
    for id in 0..130u64 {
        let mut hash = [0; 16];
        hash[..8].copy_from_slice(&id.to_be_bytes());
        seed_billing(&mut batch, &hash, &meta, &TailFields::default());
    }
    let hash = [0; 16];
    for n in 0..35u32 {
        batch.put(
            crate::billing::usage_month_final_key(&hash, 2020 + (n / 12) as i32, n % 12 + 1),
            serde_json::to_vec(&meta.to_snapshot(true)).unwrap(),
        );
    }
    db.write(batch).await.unwrap();
    let engine = ShardEngine::start(
        "r09-outbox".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        None,
        Default::default(),
    );
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
        .commit_group(vec![CommitOp::UsageAck {
            hash,
            scope: UsageAckScope::FinalRowsOnly,
            month_final_keys: finals.into_iter().map(|(key, _)| key).collect(),
        }])
        .await;
    let (remaining, more) = engine.usage_month_finals_page(hash, 32).await.unwrap();
    assert!(!more);
    assert_eq!(remaining.len(), 3);
    assert_eq!(usage_dirty_version(&engine, hash).await, Some(8));
    engine
        .commit_group(vec![CommitOp::UsageAck {
            hash,
            scope: UsageAckScope::ThroughVersion(8),
            month_final_keys: remaining.into_iter().map(|(key, _)| key).collect(),
        }])
        .await;
    assert_eq!(usage_dirty_version(&engine, hash).await, None);
    engine.begin_close();
    let _ = db.close().await;
}

/// The usage debt index follows a stream's month finals (S3a-DESIGN rev 3 §2.5): an ack keeps the
/// stream on it while another final remains, even one written after the drain read its page, and
/// the ack of the last takes it off, so the debt probe answers no only when no final is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn the_usage_debt_index_follows_the_finals() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        crate::keyspace::db_builder("debt-index", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let hash = [0x5a; 16];
    let finals = [7, 8, 9].map(|m| crate::billing::usage_month_final_key(&hash, 2026, m));
    let mut batch = WriteBatch::new();
    for key in &finals[..2] {
        batch.put(key, b"{}");
    }
    batch.put(crate::billing::usage_debt_key(&hash), b"");
    db.write(batch).await.unwrap();
    let engine = ShardEngine::start(
        "debt-index".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        None,
        Default::default(),
    );
    let ack = |keys: &[Vec<u8>]| CommitOp::UsageAck {
        hash,
        scope: UsageAckScope::FinalRowsOnly,
        month_final_keys: keys.to_vec(),
    };
    assert_eq!(engine.usage_debt_index().await.unwrap(), [hash]);
    engine.commit_group(vec![ack(&finals[..1])]).await;
    assert_eq!(
        engine.usage_debt_index().await.unwrap(),
        [hash],
        "July's ack: August remains"
    );
    // September closes after the drain read its page of August alone.
    let mut late = WriteBatch::new();
    late.put(&finals[2], b"{}");
    late.put(crate::billing::usage_debt_key(&hash), b"");
    db.write(late).await.unwrap();
    engine.commit_group(vec![ack(&finals[1..2])]).await;
    assert_eq!(
        engine.usage_debt_index().await.unwrap(),
        [hash],
        "August's ack: September remains"
    );
    assert!(engine.has_billing_debt().await.unwrap());
    engine.commit_group(vec![ack(&finals[2..])]).await;
    assert_eq!(
        engine.usage_debt_index().await.unwrap(),
        Vec::<[u8; 16]>::new()
    );
    assert!(engine.usage_month_finals().await.unwrap().is_empty());
    assert!(
        !engine.has_billing_debt().await.unwrap(),
        "no final left: no debt"
    );
    engine.begin_close();
}
