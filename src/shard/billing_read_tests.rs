//! R13: failed accounting reads preserve the group and the newer dirty version.
#![cfg(test)]
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "r13_failed_accounting_reads_preserve_group_and_newer_dirty_version; the scenario pins one ordered sequence of a failed accounting read, the preserved group and the newer dirty version; helper phases would hide which step each assertion observes"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r13_failed_accounting_reads_preserve_group_and_newer_dirty_version; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
async fn r13_failed_accounting_reads_preserve_group_and_newer_dirty_version() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("r13", store.clone()).build().await.unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r13".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let hash = [13; 16];
    let meta = crate::billing::SegmentBillingMetaV1 {
        v: 1,
        stream_id: "existing".into(),
        usage_version: 8,
        ingest_payload_bytes_total: 923,
        owned_frame_bytes_current: 876,
        ..Default::default()
    };
    let encoded = serde_json::to_vec(&meta).unwrap();
    let dirty = crate::billing::usage_dirty_key(&hash);
    let key = crate::billing::billing_meta_key(&hash);
    let final_key = crate::billing::usage_month_final_key(&hash, 2026, 7);
    for corrupt in [false, true] {
        let before = if corrupt {
            b"invalid financial state".to_vec()
        } else {
            encoded.clone()
        };
        let mut wb = WriteBatch::new();
        wb.put(key.clone(), before.clone());
        wb.put(dirty.clone(), 8u64.to_le_bytes());
        wb.put(final_key.clone(), b"owed snapshot");
        wb.put(record_key(&hash, 0), b"retained record");
        db.write(wb).await.unwrap();
        for action in 0..3 {
            if !corrupt {
                billing_read_faults()
                    .lock()
                    .unwrap()
                    .insert("r13".into(), ());
            }
            let accounting = match action {
                0 => CommitOp::UsageAck {
                    hash,
                    scope: UsageAckScope::ThroughVersion(7),
                    month_final_keys: vec![final_key.clone()],
                },
                1 => CommitOp::BillingClose {
                    hash,
                    close_ms: 1000,
                },
                _ => CommitOp::BillingRetained {
                    hash,
                    retained: true,
                },
            };
            let (tx, rx) = oneshot::channel();
            engine
                .commit_group(
                    vec![
                        CommitOp::Queue {
                            hash,
                            op: crate::queue::QueueOp::ConfigGet {
                                consumer: "c".into(),
                            },
                            resp: tx,
                        },
                        accounting,
                    ],
                    &ShardConfig::default(),
                )
                .await;
            assert!(
                rx.await.unwrap().is_err(),
                "no group success on required read failure"
            );
            assert_eq!(db.get(&key).await.unwrap().unwrap().as_ref(), &before);
            assert_eq!(
                db.get(&dirty).await.unwrap().unwrap().as_ref(),
                &8u64.to_le_bytes()
            );
            assert_eq!(
                db.get(&final_key).await.unwrap().unwrap().as_ref(),
                b"owed snapshot"
            );
            assert_eq!(
                db.get(record_key(&hash, 0))
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"retained record"
            );
        }
    }
    db.put(&key, encoded).await.unwrap();
    engine
        .commit_group(
            vec![CommitOp::UsageAck {
                hash,
                scope: UsageAckScope::ThroughVersion(7),
                month_final_keys: vec![],
            }],
            &ShardConfig::default(),
        )
        .await;
    assert_eq!(
        db.get(&dirty).await.unwrap().unwrap().as_ref(),
        &8u64.to_le_bytes()
    );
    engine.begin_close();
    let _ = db.close().await;
}
