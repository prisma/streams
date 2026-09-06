//! The actual scaler iteration preserves durable intent while its pass is held.
use super::fixture_failpoints::{FailpointGuard, gap_lock};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq};
use super::fixture_storage::mem;
use crate::scaler3::controller::{Controller, Decision};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_scaler_cancellation_and_deadlines_preserve_intent_and_rotate_work() {
    let _lock = gap_lock().lock().await;
    let (state, addr) = http_rig(mem()).await;
    let mut decisions = Vec::new();
    for name in ["cancel-scale", "healthy-scale"] {
        assert_eq!(
            hreq(
                addr,
                "PUT",
                &format!("/v1/stream/{name}"),
                &[("stream-encryption-key", PRISMA_KEY)],
                b""
            )
            .await
            .0,
            201
        );
        let sref = state.deployment.raw_adapter_sref(name);
        let desc = state.registry.get(&sref).await.unwrap().unwrap();
        decisions.push(Decision::Split(
            sref,
            desc.stream_epoch.clone(),
            0,
            0x8000_0000_0000_0000,
        ));
    }
    crate::failpoints::arm_scaler_before_publish("cancel-scale");
    let _guard = FailpointGuard("cancel-scale".into());
    let parked =
        crate::failpoints::parked(crate::failpoints::Fp::ScalerBeforePublish, "cancel-scale");
    let mut controller = Controller::new(state.topology_service(), state.runtime.ops.clone(), 0);
    controller.enqueue(decisions[0].clone());
    let tasks = crate::tasks::TaskSupervisor::new();
    tasks
        .spawn(
            "scaler-pass",
            crate::tasks::Policy::Critical,
            move |cancel| async move {
                let report = controller
                    .pass(
                        &cancel,
                        tokio::time::Instant::now() + Duration::from_secs(30),
                    )
                    .await;
                assert!(report.cancelled);
                assert_eq!(report.deferred, 1);
                crate::tasks::TaskResult::Done
            },
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while crate::failpoints::parked(crate::failpoints::Fp::ScalerBeforePublish, "cancel-scale")
            == parked
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real durable parent close must precede cancellation");
    let stopped = tasks.shutdown(Duration::from_millis(300)).await;
    assert!(
        stopped.aborted.is_empty(),
        "cooperative completion required: {stopped:?}"
    );
    assert_eq!(stopped.outcomes[0].1, crate::tasks::TaskOutcome::Finished);
    let sref = state.deployment.raw_adapter_sref("cancel-scale");
    state.registry.invalidate(&sref);
    let before = state.registry.get(&sref).await.unwrap().unwrap();
    let pending = before.segments.as_ref().unwrap().pending.clone().unwrap();
    assert_eq!(
        before
            .segments
            .as_ref()
            .unwrap()
            .segments
            .iter()
            .filter(|s| s.is_live())
            .count(),
        1
    );
    assert!(
        !crate::application::topology::resume_fenced(
            &state.topology_service(),
            &sref,
            &"ff".repeat(16)
        )
        .await,
        "old heat must not resume a different incarnation"
    );
    let mut retry = Controller::new(state.topology_service(), state.runtime.ops.clone(), 0);
    retry.enqueue(decisions[0].clone());
    retry.enqueue(decisions[1].clone());
    let (send, receive) = tokio::sync::oneshot::channel();
    let next = crate::tasks::TaskSupervisor::new();
    next.spawn(
        "scaler-retry",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let first = retry
                .pass(
                    &cancel,
                    tokio::time::Instant::now() + Duration::from_millis(200),
                )
                .await;
            assert_eq!(first.attempted, 1);
            assert_eq!(first.deferred, 2);
            let second = retry
                .pass(
                    &cancel,
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await;
            assert_eq!(
                second.completed, 1,
                "the healthy second stream must pass the held first stream"
            );
            assert_eq!(second.deferred, 1);
            send.send(retry).ok();
            crate::tasks::TaskResult::Done
        },
    )
    .unwrap();
    let mut retry = receive.await.unwrap();
    assert!(
        next.shutdown(Duration::from_millis(300))
            .await
            .aborted
            .is_empty()
    );
    state.registry.invalidate(&sref);
    let held = state.registry.get(&sref).await.unwrap().unwrap();
    assert_eq!(
        held.segments
            .as_ref()
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .seal_gen,
        pending.seal_gen
    );
    crate::failpoints::release_scaler_before_publish("cancel-scale");
    let finish = crate::tasks::TaskSupervisor::new();
    let (send, receive) = tokio::sync::oneshot::channel();
    finish
        .spawn(
            "scaler-finish",
            crate::tasks::Policy::Critical,
            move |cancel| async move {
                send.send(
                    retry
                        .pass(
                            &cancel,
                            tokio::time::Instant::now() + Duration::from_secs(5),
                        )
                        .await,
                )
                .ok();
                crate::tasks::TaskResult::Done
            },
        )
        .unwrap();
    let report = receive.await.unwrap();
    assert_eq!((report.completed, report.deferred), (1, 0));
    assert!(
        finish
            .shutdown(Duration::from_millis(300))
            .await
            .aborted
            .is_empty()
    );
    state.registry.invalidate(&sref);
    let final_desc = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(final_desc.segments.as_ref().unwrap().pending.is_none());
    assert_eq!(final_desc.stream_epoch, before.stream_epoch);
    assert_eq!(
        final_desc
            .segments
            .as_ref()
            .unwrap()
            .segments
            .iter()
            .filter(|s| s.is_live())
            .count(),
        2
    );
    engine_shutdown(&state).await;
}
