//! R09-A cancels an owned resume while its actual parent WAL close is held.
use super::fixture_http::{HttpRigOptions, cold_absorber, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp};
use std::{sync::atomic::Ordering, time::Duration};

#[expect(
    clippy::too_many_lines,
    reason = "topology debt scenario; cancelling the job, measuring the exact debt and opening a fresh read owner form one causal sequence; helper phases would hide which owner inherited the debt"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09a_cancelled_topology_job_leaves_exact_debt_for_a_fresh_read_owner() {
    let store = FaultStore::new(mem(), 911, FaultProfile::clean());
    let opts = || HttpRigOptions {
        absorber: Some(cold_absorber()),
        ..Default::default()
    };
    let rig = http_rig_build(store.clone(), RigRuntime::first(), opts()).await;
    let headers = [("prisma-encryption-key", PRISMA_KEY)];
    assert_eq!(
        preq(
            rig.addr,
            "PUT",
            "/v1/streams/resume-work",
            &headers,
            br#"{"format":{"kind":"json"}}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        preq(
            rig.addr,
            "POST",
            "/v1/streams/resume-work/records",
            &headers,
            br#"{"value":1}"#
        )
        .await
        .0,
        200
    );
    let sref = rig.state.deployment.raw_adapter_sref("resume-work");
    rig.state
        .registry
        .cas_update(&sref, |desc| {
            let mut map = crate::segmap::SegmentMap::initial("", crate::shard::now_ms());
            map.pending = Some(crate::segmap::PendingTransition {
                kind: "split".into(),
                segs: vec![0],
                split_at: 1 << 63,
                started_ms: crate::shard::now_ms(),
                seal_gen: 1,
            });
            desc.seal_gen_counter = 1;
            desc.segments = Some(map);
            true
        })
        .await
        .unwrap();
    let pending = rig.state.registry.get(&sref).await.unwrap().unwrap();
    let engine = rig.state.shards.engines().into_iter().next().unwrap();
    let handle = engine
        .stream_handle(pending.dynamic_segment_identity(0))
        .await
        .unwrap();
    let entered = store.hold_class(StoreOp::Put, ObjClass::Wal, 1);
    let reads = rig.state.read_service();
    let first = reads.topology.schedule(&pending).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while entered.load(Ordering::SeqCst) == 0 || !handle.state.lock().unwrap().applied.closed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut observer = Box::pin(first.wait());
    assert!(futures_util::poll!(observer.as_mut()).is_pending());
    drop(observer);
    for _ in 0..200 {
        let _ = reads.topology.schedule(&pending).unwrap();
    }
    assert_eq!(
        rig.state.runtime.request_work.counts().2,
        1,
        "same incarnation resumes coalesce"
    );
    rig.tasks.shutdown(Duration::from_secs(1)).await;
    assert_eq!(rig.state.runtime.request_work.counts().2, 0);
    rig.state.registry.invalidate(&sref);
    let after_cancel = rig.state.registry.get(&sref).await.unwrap().unwrap();
    assert_eq!(
        after_cancel.segments, pending.segments,
        "cancellation must not publish successors or erase intent"
    );
    assert!(
        rig.state
            .shards
            .shutdown(Duration::from_millis(5))
            .await
            .is_err(),
        "the engine still owns the durability-ambiguous close"
    );
    store.release_hold();
    rig.state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();

    let fresh = http_rig_build(store, RigRuntime::incarnation(1), opts()).await;
    // This ordinary read rediscovers the persisted pending intent and admits
    // the resume through the fresh runtime's bounded owner.
    assert_eq!(
        preq(
            fresh.addr,
            "GET",
            "/v1/streams/resume-work/records",
            &headers,
            b""
        )
        .await
        .0,
        200
    );
    let completed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fresh.state.registry.invalidate(&sref);
            let desc = fresh.state.registry.get(&sref).await.unwrap().unwrap();
            if desc
                .segments
                .as_ref()
                .is_some_and(|map| map.pending.is_none())
            {
                break desc;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let map = completed.segments.as_ref().unwrap();
    assert_eq!(map.segments.len(), 3);
    assert_eq!(map.segments[0].sealed_next_offset, Some(1));
    assert_eq!(map.segments[0].successors.len(), 2);
    fresh
        .state
        .read_service()
        .topology
        .schedule(&completed)
        .unwrap()
        .wait()
        .await
        .unwrap();
    fresh.state.registry.invalidate(&sref);
    assert_eq!(
        fresh
            .state
            .registry
            .get(&sref)
            .await
            .unwrap()
            .unwrap()
            .segments,
        completed.segments,
        "an idempotent retry cannot mint another successor pair"
    );
    fresh.tasks.shutdown(Duration::from_secs(1)).await;
    fresh
        .state
        .shards
        .shutdown(Duration::from_secs(10))
        .await
        .unwrap();
}
