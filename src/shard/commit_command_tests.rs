//! R03: close and fence commands wait for durability and dispatch together.
#![cfg(test)]
use super::*;

#[expect(
    clippy::too_many_lines,
    reason = "r03_close_and_fence_wait_for_write_remote_durability_and_dispatch; the scenario pins one ordered sequence of the close, the fence, remote durability and dispatch; helper phases would hide which step each assertion observes"
)]
#[expect(
    clippy::let_underscore_must_use,
    reason = "r03_close_and_fence_wait_for_write_remote_durability_and_dispatch; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r03_close_and_fence_wait_for_write_remote_durability_and_dispatch() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        303,
        crate::dst::FaultProfile::clean(),
    );
    let db = Arc::new(
        Db::builder("r03-barriers", store.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r03-barriers".into(),
        db.clone(),
        store.clone(),
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let hash = [3; 16];
    let handle = engine.stream_handle(hash).await.unwrap();
    let commit_gate = engine.test_hold_commit().await;
    let dispatch_gate = engine.test_hold_dispatch().await;
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (ctx, mut close) = oneshot::channel();
    let (ftx, mut fence) = oneshot::channel();
    engine
        .try_close(CloseReq {
            hash,
            generation: Some(1),
            resp: ctx,
        })
        .unwrap();
    engine
        .try_seal_fence(SealFenceReq {
            hash,
            generation: 2,
            resp: ftx,
        })
        .unwrap();
    assert!(matches!(
        close.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(
        !handle.state.lock().unwrap().applied.closed,
        "nothing applied before write gate"
    );
    drop(commit_gate);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engaged.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // SlateDB's flush tick can PUT the group's WAL between its append and
    // its publication, so the crossing is awaited rather than asserted at
    // the instant the PUT engages.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !handle.state.lock().unwrap().applied.closed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("group has crossed local write acceptance");
    let remote = slatedb::config::ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    };
    assert!(
        db.get_with_options(tail_key(&hash), &remote)
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        close.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        fence.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    store.release_hold();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while db
            .get_with_options(tail_key(&hash), &remote)
            .await
            .unwrap()
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(close.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "remote durability alone does not bypass dispatch"
    );
    assert!(matches!(
        fence.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(dispatch_gate);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(10), close)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .closed
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(10), fence)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .closed
    );
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let _ = db.close().await;
    let reopened = Db::builder("r03-barriers", store).build().await.unwrap();
    assert!(
        stored_tail(&reopened.get(tail_key(&hash)).await.unwrap().unwrap())
            .unwrap()
            .closed
    );
    reopened.close().await.unwrap();
}

#[expect(
    clippy::let_underscore_must_use,
    reason = "r03_failed_group_discards_close_and_fence_effects_together; the fixture ignores a delivery or join result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r03_failed_group_discards_close_and_fence_effects_together() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        Db::builder("r03-reject", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r03-reject".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let hash = [4; 16];
    let (ctx, close) = oneshot::channel();
    let (ftx, fence) = oneshot::channel();
    engine.fail_next_group_for(hash);
    engine
        .commit_group(
            vec![
                CommitOp::Close(CloseReq {
                    hash,
                    generation: Some(1),
                    resp: ctx,
                }),
                CommitOp::SealFence(SealFenceReq {
                    hash,
                    generation: 2,
                    resp: ftx,
                }),
            ],
            &ShardConfig::default(),
        )
        .await;
    assert!(close.await.unwrap().is_err());
    assert!(fence.await.unwrap().is_err());
    assert!(db.get(tail_key(&hash)).await.unwrap().is_none());
    assert!(
        !engine
            .stream_handle(hash)
            .await
            .unwrap()
            .state
            .lock()
            .unwrap()
            .applied
            .closed
    );
    engine.begin_close();
    let _ = db.close().await;
}
