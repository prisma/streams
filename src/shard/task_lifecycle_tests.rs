//! R17-A owns real engine roles and a storage operation held after entry.
use super::{CloseReq, ShardConfig, ShardEngine, ShardMaintenance};
use slatedb::Db;
use std::{sync::Arc, sync::atomic::Ordering, time::Duration};
use tokio::sync::{mpsc, oneshot};

async fn fixture() -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>) {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        1717,
        crate::dst::FaultProfile::clean(),
    );
    let db = Arc::new(
        Db::builder("r17a-engine", store.clone())
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(Duration::from_secs(600)),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    );
    let (tx, rx) = mpsc::channel(16);
    let engine = ShardEngine::start(
        "r17a-engine".into(),
        db,
        store.clone(),
        ShardConfig {
            wal_group_commit: true,
            wal_flush_gap: Duration::ZERO,
            ..Default::default()
        },
        tx,
        None,
        ShardMaintenance::default(),
    );
    crate::history::Absorber::start_owned(
        store.clone(),
        engine.clone(),
        Arc::new(crate::history::KeyCache::default()),
        crate::history::AbsorberConfig::default(),
        rx,
    );
    (engine, store)
}

async fn held_wal() -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>) {
    let (engine, store) = fixture().await;
    let entered = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (reply, _result) = oneshot::channel();
    let handle = engine.stream_handle([17; 16]).await.unwrap();
    engine
        .try_close(CloseReq {
            hash: [17; 16],
            generation: None,
            resp: reply,
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while entered.load(Ordering::SeqCst) == 0 || !handle.state.lock().unwrap().applied.closed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        handle.state.lock().unwrap().applied.closed,
        "the real committer applied the group before its WAL PUT entered the barrier"
    );
    (engine, store)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_shutdown_timeout_keeps_authority_over_the_held_wal_task() {
    let (engine, store) = held_wal().await;
    engine.begin_close();
    let first = engine.await_terminated(Duration::from_millis(5)).await;
    let second = engine.await_terminated(Duration::from_millis(5)).await;
    store.release_hold();
    let finished = engine.await_terminated(Duration::from_secs(10)).await;
    let _ = engine.db.close().await;
    assert!(first.is_err(), "held operation must not be reported joined");
    assert!(
        second.is_err(),
        "a timed-out observer must not manufacture an empty completed task set"
    );
    assert!(
        finished.is_ok(),
        "after releasing storage, all owned tasks must actually join: {finished:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_cancelling_a_shutdown_observer_cannot_detach_engine_tasks() {
    let (engine, store) = held_wal().await;
    engine.begin_close();
    let mut waiting = Box::pin(engine.await_terminated(Duration::from_secs(30)));
    assert!(
        futures_util::poll!(waiting.as_mut()).is_pending(),
        "observer must enter the actual held join"
    );
    drop(waiting);
    let still_waiting = engine.await_terminated(Duration::from_millis(5)).await;
    store.release_hold();
    let finished = engine.await_terminated(Duration::from_secs(10)).await;
    let _ = engine.db.close().await;
    assert!(
        still_waiting.is_err(),
        "dropping an observer must not lose join authority"
    );
    assert!(finished.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_unexpected_required_engine_role_exit_fences_serving() {
    for role in ["committer", "acker", "pump", "flush-ticker", "absorber"] {
        let (engine, _store) = fixture().await;
        let stopped = engine.test_abort_task(role);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !stopped.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let refused = engine.is_closed();
        engine.begin_close();
        let _ = engine.await_terminated(Duration::from_secs(5)).await;
        let _ = engine.db.close().await;
        assert!(refused, "unexpected {role} exit left the engine available");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_runtime_shutdown_retains_held_engine_and_refuses_replacement() {
    let (engine, store) = held_wal().await;
    let opened = engine.clone();
    let directory = crate::shard_directory::ShardDirectory::new(
        vec![engine.prefix.clone()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: Duration::from_secs(10),
            open_wait: Duration::from_secs(1),
        },
        |_| {
            Box::new(move |_, _| {
                let opened = opened.clone();
                Box::pin(async move { Ok(opened) })
            })
        },
    );
    assert!(matches!(
        directory
            .open_or_wait(&engine.prefix, Duration::from_secs(1))
            .await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    let mut cancelled = Box::pin(directory.shutdown(Duration::from_secs(30)));
    assert!(futures_util::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    assert!(directory.shutdown(Duration::from_millis(5)).await.is_err());
    directory.clear_holdoff(&engine.prefix);
    assert!(matches!(
        directory
            .open_or_wait(&engine.prefix, Duration::from_millis(5))
            .await,
        crate::sharddir::OpenOutcome::Wait {
            code: "shard_closing",
            ..
        }
    ));
    assert!(!engine.termination_complete());
    store.release_hold();
    directory.shutdown(Duration::from_secs(10)).await.unwrap();
    directory.shutdown(Duration::from_secs(1)).await.unwrap();
    assert!(engine.termination_complete());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_cancelled_history_opener_stays_owned_until_late_store_close() {
    let (engine, store) = fixture().await;
    let entered = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Manifest, 1);
    let waiting = engine.clone();
    let request = tokio::spawn(async move { waiting.history_partition().await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    assert!(matches!(request.await, Err(error) if error.is_cancelled()));
    engine.begin_close();
    assert!(
        engine
            .await_terminated(Duration::from_millis(5))
            .await
            .is_err()
    );
    assert!(
        engine
            .await_terminated(Duration::from_millis(5))
            .await
            .is_err()
    );
    store.release_hold();
    engine
        .await_terminated(Duration::from_secs(10))
        .await
        .unwrap();
    let history = engine
        .history_partition_if_open()
        .expect("late open remained owned");
    assert_eq!(
        history.get(b"probe").await.unwrap_err().kind(),
        slatedb::ErrorKind::Closed(slatedb::CloseReason::Clean)
    );
    assert!(engine.history_partition().await.is_err());
    assert!(engine.termination_complete());
}
