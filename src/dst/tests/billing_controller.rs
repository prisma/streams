//! Bounded billing passes and cancellation at entered storage operations.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq};
use super::fixture_storage::mem;
use crate::dst::{FaultPlan, FaultProfile, FaultStore, ObjClass, StoreOp};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_active_telemetry_cancels_entered_storage_and_preserves_debt() {
    // A nonzero latency plan selects the instrumented listing path; the
    // explicit hold below, rather than elapsed sleep, proves LIST entry.
    let catalog_store = FaultStore::new(
        mem(),
        909,
        FaultProfile::clean().with_op_class(
            StoreOp::List,
            ObjClass::Other,
            FaultPlan {
                latency_pct: 100,
                latency_ms: (0, 0),
                ..FaultPlan::CLEAN
            },
        ),
    );
    let (state, addr) = http_rig(catalog_store.clone()).await;
    assert_eq!(
        hreq(
            addr,
            "PUT",
            "/v1/stream/cancel-seed",
            &[("stream-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await
        .0,
        201
    );
    let spool_store = FaultStore::uniform(mem(), 910, FaultPlan::CLEAN);
    let spool = Arc::new(
        crate::billing::ReadSpool::open(spool_store.clone(), "", "r09-cancel", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.billing.install_read_spool(spool.clone()).is_ok());
    let batch = crate::billing::ReadBatch {
        source: crate::billing::MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "cancel-boot".into(),
        },
        seq: 73,
        from_ms: 1,
        to_ms: 2,
        rows: vec![],
    };
    let expected = serde_json::to_vec(&batch).unwrap();
    state.billing.requeue_reads(vec![batch]);
    let write_entered = spool_store.hold_class(StoreOp::Put, ObjClass::Sst, u64::MAX);
    let list_entered = catalog_store.hold_class(StoreOp::List, ObjClass::Other, u64::MAX);
    let tasks = crate::tasks::TaskSupervisor::new();
    crate::billing::spawn_telemetry(state.clone(), &tasks);
    tokio::time::timeout(Duration::from_secs(3), async {
        while write_entered.load(Ordering::SeqCst) == 0 || list_entered.load(Ordering::SeqCst) == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both real storage operations must be entered before cancellation");
    assert_eq!(state.billing.unflushed_reads().2, 0, "drain owns the batch");
    let report = tasks.shutdown(Duration::from_millis(300)).await;
    assert!(
        report.aborted.is_empty(),
        "active passes must stop cooperatively: {report:?}"
    );
    assert_eq!(report.outcomes.len(), 2);
    assert!(
        report
            .outcomes
            .iter()
            .all(|(_, outcome)| *outcome == crate::tasks::TaskOutcome::Finished)
    );
    let recovered = state.billing.drain_sealed_reads(10);
    assert_eq!(recovered.len(), 1);
    assert_eq!(serde_json::to_vec(&recovered[0]).unwrap(), expected);
    assert_eq!(
        state.billing.sweep_walk_cursor(),
        None,
        "incomplete page cannot advance"
    );
    // The accepted-but-unflushed spool write may finish after cancellation.
    // Retry carries exactly the same source/sequence identity for dedupe.
    spool_store.release_hold();
    catalog_store.release_hold();
    state.billing.requeue_reads(recovered);
    state.billing.spool_sealed_reads(10).await.unwrap();
    let durable = spool.pending(10).await.unwrap();
    assert!(!durable.is_empty());
    for (_, batch) in durable {
        assert_eq!(serde_json::to_vec(&batch).unwrap(), expected);
    }
    assert!(state.billing.drain_sealed_reads(1).is_empty());
    spool.close_for_tests().await;
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r09_tombstone_walk_advances_one_catalog_page_per_pass() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(
        hreq(
            addr,
            "PUT",
            "/v1/stream/bounded-000",
            &[("stream-encryption-key", PRISMA_KEY)],
            b"",
        )
        .await
        .0,
        201
    );
    let seed = state
        .registry
        .get(&state.deployment.raw_adapter_sref("bounded-000"))
        .await
        .unwrap()
        .unwrap();
    for index in 1..513 {
        let mut row = seed.to_persisted();
        row.name = format!("bounded-{index:03}");
        assert!(state.registry.create(row).await.unwrap().0);
    }
    for expected in [Some("bounded-255"), Some("bounded-511"), None] {
        crate::billing::tombstone_walk(&state).await;
        assert_eq!(state.billing.sweep_walk_cursor().as_deref(), expected);
    }
    engine_shutdown(&state).await;
}
