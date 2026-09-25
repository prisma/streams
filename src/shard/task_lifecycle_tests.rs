//! R17-A owns real engine roles and a storage operation held after entry.
use super::{CloseReq, ShardConfig, ShardEngine, ShardMaintenance};
use slatedb::Db;
use std::{sync::Arc, sync::atomic::Ordering, time::Duration};
use tokio::sync::{mpsc, oneshot};

async fn fixture() -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>) {
    fixture_named("r17a-engine", 1717).await
}

/// An engine on its own clean store under `prefix`, with its absorber.
async fn fixture_named(prefix: &str, seed: u64) -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>) {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        seed,
        crate::dst::FaultProfile::clean(),
    );
    let db = Arc::new(
        Db::builder(prefix.to_string(), store.clone())
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
        prefix.into(),
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
        engine.clone(),
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

#[expect(
    clippy::let_underscore_must_use,
    reason = "r17a_shutdown_timeout_keeps_authority_over_the_held_wal_task; the fixture closes the database on the way out; a failed close leaves nothing the assertions depend on"
)]
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

#[expect(
    clippy::let_underscore_must_use,
    reason = "r17a_cancelling_a_shutdown_observer_cannot_detach_engine_tasks; the fixture closes the database on the way out; a failed close leaves nothing the assertions depend on"
)]
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

#[expect(
    clippy::let_underscore_must_use,
    reason = "r17a_unexpected_required_engine_role_exit_fences_serving; the fixture waits out termination and closes the database on the way out; a failed wait or close leaves nothing the assertion depends on"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "r17a_unexpected_required_engine_role_exit_fences_serving; the fixture nests the exit wait inside the timeout that bounds it inside the test; flattening it would separate the wait from the bound it must respect"
)]
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

#[expect(
    clippy::excessive_nesting,
    reason = "r17a_runtime_shutdown_retains_held_engine_and_refuses_replacement; the fixture nests the scripted opener inside the factory closure inside the rig it builds; flattening it would separate the opener from the rig that installs it"
)]
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

#[expect(
    clippy::disallowed_methods,
    reason = "r17a_cancelled_history_opener_stays_owned_until_late_store_close; the fixture spawns the request it then cancels while the opener is held; a supervised spawn would tie the fixture's teardown to a supervisor it never builds"
)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r17a_fenced_final_flush_releases_only_after_the_owned_close_joins() {
    let (engine, store) = fixture().await;
    let hash = [19; 16];
    let (reply, result) = oneshot::channel();
    engine
        .try_close(CloseReq {
            hash,
            generation: None,
            resp: reply,
        })
        .unwrap();
    result.await.unwrap().unwrap();
    let entered = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Sst, 1);
    engine.begin_close();
    tokio::time::timeout(Duration::from_secs(10), async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // A different process takes ownership while the OLD owner's final flush
    // is held. Its persisted fencing epoch makes that flush return Fenced.
    let replacement = Db::builder("r17a-engine", store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(Duration::from_secs(600)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let held = engine.await_terminated(Duration::from_millis(5)).await;
    assert!(!engine.termination_complete());
    store.release_hold();
    let joined = engine.await_terminated(Duration::from_secs(10)).await;
    let persisted = replacement.get(&super::tail_key(&hash)).await.unwrap();
    replacement.close().await.unwrap();
    assert!(
        held.is_err(),
        "a fenced but held close still owns its tasks"
    );
    assert!(
        persisted.is_some(),
        "the replacement recovers the durable tail"
    );
    assert!(
        joined.is_ok(),
        "the pinned backend returns Fenced only after its close joins: {joined:?}"
    );
    assert!(engine.termination_complete());
    assert!(engine.shutdown_handle().failure().is_none());
    assert!(engine.required_task_failure().is_none());
}

/// A directory whose one prefix is served by `engine`.
fn serving(engine: &Arc<ShardEngine>) -> crate::shard_directory::ShardDirectory {
    let opened = engine.clone();
    crate::shard_directory::ShardDirectory::new(
        vec![engine.prefix.clone()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: Duration::from_secs(10),
            open_wait: Duration::from_secs(1),
        },
        move |_| Box::new(move |_, _| Box::pin(std::future::ready(Ok(opened.clone())))),
    )
}

/// Item 38: a failed storage close is final, so the directory stop answers
/// at once, with the report that names it, and the owner keeps its fence
/// and readiness failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_storage_close_ends_the_directory_stop_at_once() {
    let (engine, _store) = fixture().await;
    let directory = serving(&engine);
    assert!(matches!(
        directory
            .open_or_wait(&engine.prefix, Duration::from_secs(1))
            .await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    engine
        .tasks
        .begin_failed_close_for_test("scripted close failure");
    let stopped = tokio::time::timeout(
        Duration::from_secs(5),
        directory.shutdown(Duration::from_secs(30)),
    )
    .await
    .expect("a failed close is final: the directory stop must not wait out its grace");
    let error = stopped.unwrap_err();
    assert!(
        error.contains("storage-close: Failed(\"scripted close failure\")"),
        "{error}"
    );
    assert!(
        directory
            .unready_reason()
            .unwrap()
            .contains("scripted close failure"),
        "the owner keeps the readiness failure"
    );
    assert!(
        !engine.termination_complete(),
        "a failed close proves no termination"
    );
    drop(engine.db.close().await);
}

/// Item 38: a directory stop that reaches its deadline counts the engine
/// still closing as pending, and its joined reports hold only closes that
/// settled (a failed one is never dropped beside a pending one): an engine
/// whose close is still running has no report yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_directory_stop_past_its_grace_carries_the_joined_reports() {
    let (engine, store) = held_wal().await;
    let directory = serving(&engine);
    assert!(matches!(
        directory
            .open_or_wait(&engine.prefix, Duration::from_secs(1))
            .await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    let stopped = tokio::time::timeout(
        Duration::from_secs(5),
        directory.shutdown(Duration::from_millis(5)),
    )
    .await
    .expect("the stop is bounded by its grace");
    assert_eq!(
        stopped.unwrap_err(),
        "shutdown ongoing or failed: 0 opens, 1 engines; owners retained; joined reports: []"
    );
    store.release_hold();
    tokio::time::timeout(
        Duration::from_secs(15),
        directory.shutdown(Duration::from_secs(10)),
    )
    .await
    .expect("the released close completes within its grace")
    .unwrap();
    assert!(engine.termination_complete());
}

/// Item 38: at the deadline, a close that failed beside one still running
/// is carried in the joined reports: the directory stop names the failure
/// and counts the running engine as pending (review of da06acad: T9 alone
/// no longer pinned this, since its only engine is still closing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_close_beside_a_pending_one_is_reported_at_the_deadline() {
    let (held, store) = held_wal().await;
    let (failed, _) = fixture_named("r17a-failed", 1718).await;
    let engines: std::collections::HashMap<String, Arc<ShardEngine>> = [
        (held.prefix.clone(), held.clone()),
        (failed.prefix.clone(), failed.clone()),
    ]
    .into_iter()
    .collect();
    let directory = crate::shard_directory::ShardDirectory::new(
        vec![held.prefix.clone(), failed.prefix.clone()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: Duration::from_secs(10),
            open_wait: Duration::from_secs(1),
        },
        move |_| {
            Box::new(move |prefix: String, _| {
                let engine = engines.get(&prefix).cloned().unwrap();
                Box::pin(std::future::ready(Ok(engine)))
            })
        },
    );
    for prefix in [&held.prefix, &failed.prefix] {
        assert!(matches!(
            directory.open_or_wait(prefix, Duration::from_secs(1)).await,
            crate::sharddir::OpenOutcome::Ready(_)
        ));
    }
    failed
        .tasks
        .begin_failed_close_for_test("scripted close failure");
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        directory.shutdown(Duration::from_millis(200)),
    )
    .await
    .expect("the stop is bounded by its grace")
    .unwrap_err();
    store.release_hold();
    drop(failed.db.close().await);
    drop(
        tokio::time::timeout(
            Duration::from_secs(15),
            directory.shutdown(Duration::from_secs(10)),
        )
        .await,
    );
    assert!(error.contains("1 engines; owners retained"), "{error}");
    assert!(
        error.contains("scripted close failure"),
        "the failed close was dropped beside the pending one: {error}"
    );
}
