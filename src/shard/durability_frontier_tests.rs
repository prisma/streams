//! Distinct remote-write and dispatch mechanisms for prior-group close retries.
use super::{
    CloseReq, SealFenceReq, ShardConfig, ShardEngine, ShardMaintenance, stored_tail, tail_key,
};
use slatedb::{Db, config::DurabilityLevel};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, oneshot};

#[expect(
    clippy::too_many_lines,
    reason = "r24_prior_group_close_retry_and_fence_wait_on_actual_remote_frontier; the fixture stages the prior group, the close retry and the fence wait against one remote frontier; splitting it would separate the stages from the frontier they wait on"
)]
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
    // SlateDB's flush tick can PUT the group's WAL between its append and
    // its publication, so the crossing is awaited rather than asserted at
    // the instant the PUT engages.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !handle.state.lock().unwrap().applied.closed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first group has crossed local write acceptance");
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
    // They must share the second: a retry alone changes nothing, and the
    // fence's durable row waits behind the same blocked WAL, so the commit
    // gate parks the committer until both are queued.
    let commit = engine.test_hold_commit().await;
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
    drop(commit);
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

/// TLA-002-F1: a seal fence outlives the engine that recorded it. A
/// replacement engine on the same storage reloads the durable fence row, so
/// a close carrying the superseded generation is refused there as well,
/// while the fenced generation still closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seal_fence_survives_engine_replacement() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2406,
        crate::dst::FaultProfile::clean(),
    );
    let identity = [26; 16];
    let open = || async {
        let db = Db::builder("fence-replacement", store.clone())
            .build()
            .await
            .unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let engine = ShardEngine::start(
            "fence-replacement".into(),
            Arc::new(db),
            store.clone(),
            ShardConfig::default(),
            tx,
            None,
            ShardMaintenance::default(),
        );
        (engine, _rx)
    };
    let (engine, _first_rx) = open().await;
    let (fence_tx, fenced) = oneshot::channel();
    engine
        .try_seal_fence(SealFenceReq {
            hash: identity,
            generation: 2,
            resp: fence_tx,
        })
        .unwrap();
    assert!(fenced.await.unwrap().is_ok(), "the fence is durable");
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();

    let (replacement, _second_rx) = open().await;
    let close = |generation| {
        let (resp, reply) = oneshot::channel();
        replacement
            .try_close(CloseReq {
                hash: identity,
                generation: Some(generation),
                resp,
            })
            .unwrap();
        reply
    };
    assert!(
        matches!(
            close(1).await.unwrap(),
            Err(super::AppendErr::SealSuperseded)
        ),
        "the replacement forgot the fence"
    );
    assert!(close(2).await.unwrap().unwrap().closed);
    replacement.begin_close();
    replacement
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}

/// A shard engine at `prefix` over `store` whose WAL flushes every 5 ms.
async fn fence_engine(
    prefix: &str,
    store: &Arc<crate::dst::FaultStore>,
) -> (Arc<ShardEngine>, mpsc::Receiver<super::AbsorbSignal>) {
    let db = Db::builder(prefix, store.clone())
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        prefix.into(),
        Arc::new(db),
        store.clone(),
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    (engine, rx)
}

/// TLA-002-F2: `SealSuperseded` releases the refused handler's claim, so a
/// refusal decided from a fence staged in its own group waits for that
/// group's durability. While the fence's WAL write is held the superseded
/// close has no answer; the engine then retires with the fence never
/// durable, and the close is answered `Moved`, which keeps the claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_superseded_close_waits_for_its_fence_to_be_durable() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2407,
        crate::dst::FaultProfile::clean(),
    );
    let (engine, _rx) = fence_engine("fence-barrier", &store).await;
    let identity = [27; 16];
    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let commit = engine.test_hold_commit().await;
    let (fence_tx, fence) = oneshot::channel();
    let (stale_tx, mut stale) = oneshot::channel();
    engine
        .try_seal_fence(SealFenceReq {
            hash: identity,
            generation: 2,
            resp: fence_tx,
        })
        .unwrap();
    engine
        .try_close(CloseReq {
            hash: identity,
            generation: Some(1),
            resp: stale_tx,
        })
        .unwrap();
    drop(commit);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engaged.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the fence's group reaches its WAL write");
    let early = stale.try_recv();
    assert!(
        matches!(early, Err(oneshot::error::TryRecvError::Empty)),
        "a superseded close was answered before its fence was durable: {early:?}"
    );
    engine.begin_close();
    let late = stale.await.unwrap();
    assert!(
        matches!(late, Err(super::AppendErr::Moved)),
        "a fence that never became durable left a definitive refusal: {late:?}"
    );
    assert!(matches!(fence.await.unwrap(), Err(super::AppendErr::Moved)));
    store.release_hold();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}

/// TLA-002-F2: a commit group that is never written leaves no fence row, so
/// neither its own superseded close nor a later one may be refused on the
/// strength of the engine cache it raised.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_fence_group_refuses_nothing() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2408,
        crate::dst::FaultProfile::clean(),
    );
    let (engine, _rx) = fence_engine("fence-lost", &store).await;
    let identity = [28; 16];
    let close = |generation| {
        let (resp, reply) = oneshot::channel();
        engine
            .try_close(CloseReq {
                hash: identity,
                generation: Some(generation),
                resp,
            })
            .unwrap();
        reply
    };
    let commit = engine.test_hold_commit().await;
    let (fence_tx, fence) = oneshot::channel();
    engine
        .try_seal_fence(SealFenceReq {
            hash: identity,
            generation: 2,
            resp: fence_tx,
        })
        .unwrap();
    let stale = close(1);
    engine.fail_next_group_for(identity);
    drop(commit);
    assert!(matches!(
        fence.await.unwrap(),
        Err(super::AppendErr::Internal(_))
    ));
    let refused = stale.await.unwrap();
    assert!(
        matches!(refused, Err(super::AppendErr::Internal(_))),
        "a failed fence group refused its close definitively: {refused:?}"
    );
    let later = close(1).await.unwrap();
    assert!(
        engine
            .db
            .get(super::seal_fence_key(&identity))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        later.as_ref().is_ok_and(|ack| ack.closed),
        "a fence no group wrote refused a later close: {later:?}"
    );
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}
