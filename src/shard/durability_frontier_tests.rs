//! Distinct remote-write and dispatch mechanisms for prior-group close retries.
use super::{
    CloseReq, SealFenceReq, ShardConfig, ShardEngine, ShardMaintenance, stored_tail, tail_key,
};
use slatedb::{Db, config::DurabilityLevel};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, oneshot};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r24_prior_group_close_retry_and_fence_wait_on_actual_remote_frontier() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2405,
        crate::dst::FaultProfile::clean(),
    );
    let db = Arc::new(
        Db::builder("r24-prior-frontier", store.clone())
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
        "r24-prior-frontier".into(),
        db.clone(),
        store.clone(),
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let identity = [24; 16];
    let handle = engine.stream_handle(identity).await.unwrap();
    let commit = engine.test_hold_commit().await;
    let dispatch = engine.test_hold_dispatch().await;
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (first_tx, mut first) = oneshot::channel();
    engine
        .try_close(CloseReq {
            hash: identity,
            generation: Some(1),
            resp: first_tx,
        })
        .unwrap();
    drop(commit);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engaged.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        handle.state.lock().unwrap().applied.closed,
        "first group has crossed local write acceptance"
    );
    let remote = slatedb::config::ReadOptions {
        durability_filter: DurabilityLevel::Remote,
        ..Default::default()
    };
    assert!(
        db.get_with_options(tail_key(&identity), &remote)
            .await
            .unwrap()
            .is_none(),
        "actual WAL object-store PUT remains blocked"
    );

    // These operations cannot share the first group: its applied close and
    // blocked object-store write were observed before they were submitted.
    let (retry_tx, mut retry) = oneshot::channel();
    let (fence_tx, mut fence) = oneshot::channel();
    engine
        .try_close(CloseReq {
            hash: identity,
            generation: Some(1),
            resp: retry_tx,
        })
        .unwrap();
    engine
        .try_seal_fence(SealFenceReq {
            hash: identity,
            generation: 2,
            resp: fence_tx,
        })
        .unwrap();
    assert_eq!(engine.appends_enqueued(), 3);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engine.seal_fences.lock().unwrap().get(&identity).copied() != Some(2) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for reply in [&mut first, &mut retry, &mut fence] {
        assert!(
            matches!(reply.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "applied closed state cannot release a success before remote durability"
        );
    }
    assert!(
        db.get_with_options(tail_key(&identity), &remote)
            .await
            .unwrap()
            .is_none()
    );
    store.release_hold();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while db
            .get_with_options(tail_key(&identity), &remote)
            .await
            .unwrap()
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for reply in [&mut first, &mut retry, &mut fence] {
        assert!(
            matches!(reply.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "remote durability and response dispatch are independent gates"
        );
    }
    drop(dispatch);
    for reply in [first, retry, fence] {
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(10), reply)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .closed
        );
    }
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let reopened = Db::builder("r24-prior-frontier", store)
        .build()
        .await
        .unwrap();
    assert!(
        stored_tail(&reopened.get(tail_key(&identity)).await.unwrap().unwrap())
            .unwrap()
            .closed,
        "reopened durable state agrees with all three replies"
    );
    reopened.close().await.unwrap();
}
