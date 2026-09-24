//! Distinct remote-write and dispatch mechanisms for prior-group close retries.
use super::{
    AppendAck, AppendErr, AppendFinish, AppendReq, CloseReq, SealFenceReq, ShardConfig,
    ShardEngine, ShardMaintenance, stored_tail, tail_key,
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

type Reply = oneshot::Receiver<Result<AppendAck, AppendErr>>;

/// A one-record append to `hash` that finishes as `finish`, carrying the
/// seal-claim generation `seal_gen` (None for an untagged append).
fn seal_append(hash: [u8; 16], finish: AppendFinish, seal_gen: Option<u64>) -> (AppendReq, Reply) {
    let (resp, reply) = oneshot::channel();
    let req = AppendReq {
        hash,
        route: [0; 16],
        enqueued_at: std::time::Instant::now(),
        entries: vec![bytes::Bytes::from_static(b"final")],
        routing_key: "lane".into(),
        key_hash: [7; 16],
        producer_lineage: vec![],
        key_version: 1,
        subkey: [1; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 5,
        finish,
        billing: None,
        seal_gen,
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        usage: Arc::new(Default::default()),
        resp,
    };
    (req, reply)
}

/// TLA-002-F1: a seal fence is scoped to the segment it fences. Fencing one
/// segment neither refuses a lower-generation claim-authorized close of a
/// sibling in the engine that raised it, nor, in a replacement engine that
/// reads each segment's fence from its durable row, a close of a segment no
/// fence ever named; the fenced segment itself still refuses its superseded
/// generation there, tagged or not, while its untagged ordinary appends
/// remain no seal decision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seal_fence_refuses_only_its_own_segment() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2409,
        crate::dst::FaultProfile::clean(),
    );
    let (fenced, sibling, cold) = ([29; 16], [30; 16], [31; 16]);
    let (engine, _rx) = fence_engine("fence-scoped", &store).await;
    let (fence_tx, fence) = oneshot::channel();
    engine
        .try_seal_fence(SealFenceReq {
            hash: fenced,
            generation: 3,
            resp: fence_tx,
        })
        .unwrap();
    assert!(fence.await.unwrap().is_ok(), "the fence is durable");
    let (append, reply) = seal_append(sibling, AppendFinish::Close, Some(1));
    engine.try_enqueue(append).unwrap();
    let closed = reply.await.unwrap();
    assert!(
        closed.as_ref().is_ok_and(|ack| ack.closed),
        "a fence on another segment refused this close: {closed:?}"
    );
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();

    let (replacement, _second_rx) = fence_engine("fence-scoped", &store).await;
    let submit = |hash, finish, seal_gen| {
        let (append, reply) = seal_append(hash, finish, seal_gen);
        replacement.try_enqueue(append).unwrap();
        reply
    };
    let unfenced = submit(cold, AppendFinish::Close, Some(1)).await.unwrap();
    assert!(
        unfenced.as_ref().is_ok_and(|ack| ack.closed),
        "a replacement read another segment's fence row: {unfenced:?}"
    );
    let ordinary = submit(fenced, AppendFinish::Open, None).await.unwrap();
    assert!(
        ordinary.as_ref().is_ok_and(|ack| !ack.closed),
        "an untagged ordinary append was decided by the fence: {ordinary:?}"
    );
    for seal_gen in [Some(2), None] {
        let stale = submit(fenced, AppendFinish::Close, seal_gen).await.unwrap();
        assert!(
            matches!(stale, Err(AppendErr::SealSuperseded)),
            "the replacement admitted a close below the fence ({seal_gen:?}): {stale:?}"
        );
    }
    let current = submit(fenced, AppendFinish::Close, Some(3)).await.unwrap();
    assert!(current.is_ok_and(|ack| ack.closed));
    replacement.begin_close();
    replacement
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}

/// Waits until `engine`'s Remote-durable `(absorbed, history_v2)` for
/// `hash` is `expected`.
async fn durable_absorbed_reaches(engine: &ShardEngine, hash: &[u8; 16], expected: (u64, bool)) {
    let mut seen = None;
    let reached = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while seen != Some(expected) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            seen = Some(engine.durable_absorbed(hash).await.unwrap());
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "durable boundary {seen:?}, expected {expected:?}"
    );
}

/// TLA-018-F1: a durable read observes only the Remote-durable shard log.
/// While an absorbed advance, and the trims it enables, are applied but
/// their WAL write is held, the applied boundary has moved, but the durable
/// boundary still names the previous advance and a durable scan still sees
/// the rows those trims deleted; once the write lands, the durable boundary
/// catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_reads_see_only_the_remote_frontier() {
    let store = crate::dst::FaultStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        2410,
        crate::dst::FaultProfile::clean(),
    );
    let (engine, _rx) = fence_engine("durable-view", &store).await;
    let hash = [32; 16];
    let (mut append, reply) = seal_append(hash, AppendFinish::Open, None);
    append.entries = (0..4u8)
        .map(|i| bytes::Bytes::from(vec![i; 16 + usize::from(i)]))
        .collect();
    engine.try_enqueue(append).unwrap();
    assert_eq!(reply.await.unwrap().unwrap().next_offset, 4);
    let mut frames = Vec::new();
    for offset in 0..4 {
        let row = engine.db.get(super::record_key(&hash, offset)).await;
        frames.push(row.unwrap().unwrap().len() as u64);
    }
    engine
        .submit_absorbed(hash, 0, 2, frames[0] + frames[1])
        .await;
    durable_absorbed_reaches(&engine, &hash, (2, false)).await;

    let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    engine
        .submit_absorbed(hash, 2, 4, frames[2] + frames[3])
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engine
            .visible_absorbed(&hash, super::Deliver::Applied)
            .await
            .unwrap()
            != (4, false)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the advance is applied");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engaged.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the advance's WAL write is parked");
    assert_eq!(
        engine.durable_absorbed(&hash).await.unwrap(),
        (2, false),
        "a durable read adopted an advance that is only applied"
    );
    let handle = engine.stream_handle(hash).await.unwrap();
    let durable = super::record::read_frames_until(
        &engine,
        &handle,
        0,
        u64::MAX,
        None,
        usize::MAX,
        super::Deliver::Durable,
    )
    .await
    .unwrap();
    let offsets: Vec<u64> = durable
        .frames
        .iter()
        .map(|frame| frame.view().header.offset)
        .collect();
    assert_eq!(
        offsets,
        [0, 1, 2, 3],
        "a durable scan saw trims that are only applied"
    );
    store.release_hold();
    durable_absorbed_reaches(&engine, &hash, (4, false)).await;
    engine.begin_close();
    engine
        .await_terminated(std::time::Duration::from_secs(5))
        .await
        .unwrap();
}
