//! R03: queue refusal from a staged generation shares the group failure and dispatch.
#![cfg(test)]
use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "r03_queue_refusal_from_staged_generation_shares_group_failure_and_dispatch; the scenario pins one ordered sequence of the staged generation, the refusal, the group failure and its dispatch; helper phases would hide which step each assertion observes"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r03_queue_refusal_from_staged_generation_shares_group_failure_and_dispatch; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r03_queue_refusal_from_staged_generation_shares_group_failure_and_dispatch() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r03-queue-refusal", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r03-queue-refusal".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let hash = [37; 16];
    for fail in [true, false] {
        let (created_tx, mut created) = oneshot::channel();
        let (conflict_tx, mut conflict) = oneshot::channel();
        let (close_tx, mut closed) = oneshot::channel();
        if fail {
            engine.fail_next_group_for(hash);
        }
        let dispatch = engine.test_hold_dispatch().await;
        engine
            .commit_group(
                vec![
                    CommitOp::Queue {
                        hash,
                        op: crate::queue::QueueOp::ConfigPut {
                            consumer: "c".into(),
                            cfg: Default::default(),
                        },
                        resp: created_tx,
                    },
                    CommitOp::Queue {
                        hash,
                        op: crate::queue::QueueOp::ConfigLifecycle {
                            consumer: "c".into(),
                            expect_gen: 2,
                            deleting: true,
                        },
                        resp: conflict_tx,
                    },
                    CommitOp::Close(CloseReq {
                        hash,
                        generation: None,
                        resp: close_tx,
                    }),
                ],
                &ShardConfig::default(),
            )
            .await;
        if fail {
            // The conflict was derived from the uncommitted ConfigPut.
            // If the group fails, the consumer generation never existed.
            assert!(
                created
                    .await
                    .unwrap()
                    .unwrap_err()
                    .contains("group write failed")
            );
            assert!(
                conflict
                    .await
                    .unwrap()
                    .unwrap_err()
                    .contains("group write failed")
            );
            assert!(matches!(closed.await.unwrap(), Err(AppendErr::Internal(_))));
            assert!(
                db.get(crate::queue::config_key(&hash, "c"))
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert!(matches!(
                created.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                conflict.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                closed.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            drop(dispatch);
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(10), created)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok()
            );
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(10), conflict)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err()
                    .contains("consumer_generation_conflict")
            );
            assert!(closed.await.unwrap().unwrap().closed);
            continue;
        }
    }
    engine.begin_close();
    let _ = db.close().await;
}
