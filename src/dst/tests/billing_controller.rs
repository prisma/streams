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

fn cancellation_read_envelope(at_ms: i64) -> crate::billing::UsageEnvelope {
    use crate::billing::{
        BillingIdentity, MeterSource, ReadBatch, ReadRow, UsageEnvelope, UsagePayload,
    };
    UsageEnvelope {
        v: 1,
        event_id: "read/cancel-rollup/1".into(),
        event_time_ms: at_ms,
        emitted_ms: at_ms,
        cell: "test".into(),
        payload: UsagePayload::ReadBatch(ReadBatch {
            source: MeterSource {
                cell: "test".into(),
                instance: "rollup-cancel".into(),
                boot: "cancel-rollup".into(),
            },
            seq: 1,
            from_ms: at_ms,
            to_ms: at_ms + 1,
            rows: vec![ReadRow {
                identity: BillingIdentity {
                    account_id: "acct".into(),
                    project_id: "proj".into(),
                    stream_id: "ab".repeat(8),
                    stream_name: "orders".into(),
                },
                read_payload_bytes: 73,
                read_records: 1,
                read_operations: 1,
                queue_operations: 0,
                append_requests: 0,
            }],
        }),
    }
}

async fn stop_entered_rollup(
    tasks: &crate::tasks::TaskSupervisor,
    entered: &std::sync::atomic::AtomicU64,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real rollup storage operation must be entered before cancellation");
    let report = tasks.shutdown(Duration::from_millis(300)).await;
    assert!(
        report.aborted.is_empty(),
        "rollup must stop cooperatively: {report:?}"
    );
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].1, crate::tasks::TaskOutcome::Finished);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_active_rollup_cancels_entered_read_and_replays_ledger_once() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let source_store = FaultStore::uniform(mem(), 911, FaultPlan::CLEAN);
    let (state, _) = http_rig(source_store.clone()).await;
    let now = crate::billing::billing_now_ms();
    let (year, month) = crate::billing::utc_year_month(now);
    let month = crate::billing::month_str(year, month);
    let envelope = cancellation_read_envelope(now);
    let key = state.billing.usage_key().unwrap();
    crate::billing::system_append(
        &state,
        crate::billing::USAGE_STREAM,
        &key,
        serde_json::to_vec(std::slice::from_ref(&envelope)).unwrap(),
    )
    .await
    .unwrap();
    let before = crate::billing::system_read(&state, crate::billing::USAGE_STREAM, &key, None)
        .await
        .unwrap()
        .unwrap();
    let rollup = Arc::new(
        crate::rollup::UsageRollup::open(mem(), "", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.rollup.install(rollup.clone()).is_ok());
    state
        .registry
        .invalidate(&crate::tenant::system_project().stream_ref(crate::billing::USAGE_STREAM));
    let entered = source_store.hold_class(StoreOp::Get, ObjClass::Other, u64::MAX);
    let tasks = crate::tasks::TaskSupervisor::new();
    crate::billing::spawn_rollup(state.clone(), String::new(), &tasks);
    stop_entered_rollup(&tasks, &entered).await;
    assert_eq!(rollup.cursor().await.unwrap(), None);
    assert!(
        rollup
            .month_row(&month, "acct", "proj", &"ab".repeat(8))
            .await
            .unwrap()
            .is_none()
    );
    source_store.release_hold();
    assert_eq!(crate::billing::rollup_step(&state).await.unwrap(), 1);
    assert_eq!(
        rollup.cursor().await.unwrap().as_deref(),
        Some(before.1.as_str())
    );
    // The authoritative ledger remains intact; an ambiguous apply can replay
    // its original source identity without adding the financial delta twice.
    let after = crate::billing::system_read(&state, crate::billing::USAGE_STREAM, &key, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, before);
    rollup.apply_page(&[envelope], &after.1).await.unwrap();
    let row = rollup
        .month_row(&month, "acct", "proj", &"ab".repeat(8))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.read_payload_bytes, 73);
    assert_eq!(row.read_records, 1);
    assert_eq!(row.read_operations, 1);
    rollup.db.close().await.unwrap();
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r09_active_rollup_cancels_entered_publication_and_retains_artifact_debt() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let publication_store = FaultStore::uniform(mem(), 912, FaultPlan::CLEAN);
    let (state, _) = http_rig(publication_store.clone()).await;
    let rollup = Arc::new(
        crate::rollup::UsageRollup::open(mem(), "", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.rollup.install(rollup.clone()).is_ok());
    let (year, month) = crate::billing::utc_year_month(crate::billing::billing_now_ms());
    let (year, month) = if month == 1 {
        (year - 1, 12)
    } else {
        (year, month - 1)
    };
    let envelope = cancellation_read_envelope(crate::billing::month_start_ms(year, month) + 10);
    rollup.apply_page(&[envelope], "source-page").await.unwrap();
    assert_eq!(rollup.close_month(year, month, 0).await.unwrap(), 1);
    rollup.db.flush().await.unwrap();
    let before = rollup.pending_artifacts(64).await.unwrap();
    assert_eq!(before.len(), 1);
    let expected = serde_json::to_vec(&before[0].4).unwrap();
    let entered = publication_store.hold_class(StoreOp::Put, ObjClass::Other, u64::MAX);
    let tasks = crate::tasks::TaskSupervisor::new();
    crate::billing::spawn_rollup(state.clone(), String::new(), &tasks);
    stop_entered_rollup(&tasks, &entered).await;
    let after = rollup.pending_artifacts(64).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(serde_json::to_vec(&after[0].4).unwrap(), expected);
    assert_eq!(
        rollup.cursor().await.unwrap().as_deref(),
        Some("source-page")
    );
    publication_store.release_hold();
    assert_eq!(
        crate::billing::publish_artifacts(&rollup, &state.data_store, "")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        crate::billing::publish_artifacts(&rollup, &state.data_store, "")
            .await
            .unwrap(),
        0
    );
    assert!(rollup.pending_artifacts(64).await.unwrap().is_empty());
    use object_store::ObjectStoreExt;
    let (_, month, project, stream_id, _) = &before[0];
    let path = object_store::path::Path::from(format!(
        "telemetry/usage-monthly/{project}/{stream_id}/{month}.json"
    ));
    let published = state
        .data_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(published.as_ref(), expected);
    rollup.db.close().await.unwrap();
    engine_shutdown(&state).await;
}
