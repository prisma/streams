//! Bounded billing passes and cancellation at entered storage operations.

use super::fixture_auth::{auth_rig, mint_token, rig_append, rig_create};
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::{FaultPlan, FaultProfile, FaultStore, ObjClass, StoreOp};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

#[expect(
    clippy::too_many_lines,
    reason = "r09 active telemetry cancellation; the entered holds, the stop and the release-and-retry must stay one visible sequence so the stop's side is evident for every batch; a helper phase would hide which side of the stop a batch was on"
)]
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
    let rig = http_rig_build(
        catalog_store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let state = rig.state.clone();
    let addr = rig.addr;
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
    // Item 26: a stop runs one terminal round; it parks on the same held
    // PUT and is cut after one drain cadence, so the grace sits above the
    // cadence and the stop still needs no abort (R09).
    let cadence = Duration::from_secs(state.config.billing.telemetry_drain_secs);
    let stop = std::time::Instant::now();
    let report = tasks.shutdown(Duration::from_secs(5)).await;
    assert!(
        report.aborted.is_empty(),
        "active passes must stop cooperatively: {report:?}"
    );
    assert!(
        stop.elapsed() >= cadence,
        "the terminal round must be attempted and given one cadence, not {:?}",
        stop.elapsed()
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
    rig.shutdown().await;
}

/// How many `_ops_metrics` snapshots the rig has emitted: the rig's first
/// drain round ends with one, so this is the mark that the loop has left
/// its round and is waiting between ticks.
async fn ops_metrics_records(state: &Arc<crate::http::AppState>) -> usize {
    let key = state.billing.usage_key().unwrap();
    match crate::billing::system_read(state, crate::billing::OPS_METRICS_STREAM, &key, None)
        .await
        .unwrap()
    {
        None => 0,
        Some((body, _)) if body.is_empty() => 0,
        Some((body, _)) => serde_json::from_slice::<Vec<serde_json::Value>>(&body)
            .unwrap()
            .len(),
    }
}

/// The read rows the `_usage` ledger holds, as (identity, (payload bytes,
/// records, operations)).
async fn usage_read_rows(
    state: &Arc<crate::http::AppState>,
) -> Vec<(crate::billing::BillingIdentity, (u64, u64, u64))> {
    let key = state.billing.usage_key().unwrap();
    let (body, _) = crate::billing::system_read(state, crate::billing::USAGE_STREAM, &key, None)
        .await
        .unwrap()
        .expect("_usage exists after the terminal round");
    let envelopes: Vec<crate::billing::UsageEnvelope> = serde_json::from_slice(&body).unwrap();
    envelopes
        .iter()
        .filter_map(|e| match &e.payload {
            crate::billing::UsagePayload::ReadBatch(b) => Some(&b.rows),
            crate::billing::UsagePayload::SegmentSnapshot(_)
            | crate::billing::UsagePayload::StreamLifecycle(_)
            | crate::billing::UsagePayload::UsageCorrection(_) => None,
        })
        .flatten()
        .map(|row| {
            (
                row.identity.clone(),
                (
                    row.read_payload_bytes,
                    row.read_records,
                    row.read_operations,
                ),
            )
        })
        .collect()
}

/// Review item 26: a graceful stop owes the ledger the read window the
/// cadence had not reached yet (OBSERVABILITY-BILLING §2.3, §7.4). The
/// loop is stopped between ticks, on the rig's own supervisor, so every
/// other supervised loop is cancelled in the same instant as in
/// production; the window younger than the flush interval must still be
/// sealed, spooled and appended before the loop reports itself finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_stop_seals_and_drains_the_active_read_window() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    let spool = Arc::new(
        crate::billing::ReadSpool::open(state.data_store.clone(), "", "stop-drain", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.billing.install_read_spool(spool.clone()).is_ok());
    crate::billing::spawn_telemetry(state.clone(), &rig.tasks);
    tokio::time::timeout(Duration::from_secs(3), async {
        while ops_metrics_records(&state).await != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first round ends with the metrics emission");
    let identity = crate::billing::BillingIdentity {
        account_id: "acct".into(),
        project_id: "proj".into(),
        stream_id: "ab".repeat(8),
        stream_name: "orders".into(),
    };
    state.billing.meter_read(
        &identity,
        crate::billing::RowDelta {
            read_payload_bytes: 4096,
            read_records: 3,
            read_operations: 1,
            ..Default::default()
        },
    );
    assert_eq!(
        state.billing.unflushed_reads(),
        (1, 150, 0),
        "the window is younger than the flush interval: nothing seals before the stop"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(5)).await;
    assert!(
        report.aborted.is_empty(),
        "the terminal round must finish inside the grace: {report:?}"
    );
    for name in ["telemetry-drain", "telemetry-outbox-sweep"] {
        assert!(
            report
                .outcomes
                .iter()
                .any(|(n, o)| *n == name && *o == crate::tasks::TaskOutcome::Finished),
            "{name} must report itself finished: {report:?}"
        );
    }
    assert_eq!(
        state.billing.unflushed_reads(),
        (0, 0, 0),
        "a graceful stop left read usage behind"
    );
    assert!(
        spool.pending(10).await.unwrap().is_empty(),
        "the ledger acknowledged the batch, so the spool released it"
    );
    assert_eq!(
        usage_read_rows(&state).await,
        vec![(identity, (4096, 3, 1))],
        "exactly the one metered row reached the ledger"
    );
    spool.close_for_tests().await;
    rig.shutdown().await;
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
    // The cursor is the last consumed descriptor KEY: the walk spans every
    // project in the cell, so a bare name no longer identifies a position.
    let tenant = crate::crypto::hex(state.deployment.deployment_tenant().as_bytes());
    let key = |name: &str| {
        let name = crate::crypto::hex(name.as_bytes());
        format!("registry/v4/projects/{tenant}/streams/{name}.json")
    };
    for expected in [Some(key("bounded-255")), Some(key("bounded-511")), None] {
        crate::billing::tombstone_walk(&state).await;
        assert_eq!(state.billing.sweep_walk_cursor(), expected);
    }
    engine_shutdown(&state).await;
}

/// Bug 5 (enforce-mode cells): terminal-closure reconciliation is CELL-wide.
/// A project that is not the deployment tenant owns three streams whose usage
/// rows are acked CLEAN; one is then tombstoned with no delete-time close (a
/// foreign owner, or a crash between the tombstone and the committer op) and
/// one simply expires (nothing closes at expiry). Nothing dirties those rows
/// again, so the walk is their only closer: it must zero both gauges and leave
/// the live stream's alone. The walk used to page only the deployment tenant's
/// catalog, so both gauges stayed nonzero and the rollup carried them monthly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_walk_closes_clean_terminal_rows_of_every_project() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (_svc, state, addr) = auth_rig("proj-walk", "ws_walk", &["c_walk"], None).await;
    let bearer = mint_token("c_walk", "proj-walk", "ws_walk", 1, 1, "walk-1", 600);
    let project = crate::tenant::ProjectId::new("proj-walk").unwrap();
    assert_ne!(&project, state.deployment.deployment_tenant());
    let mut rows = Vec::new();
    for name in ["walk-deleted", "walk-expired", "walk-live"] {
        rig_create(addr, name, &bearer).await;
        assert_eq!(rig_append(addr, name, &bearer, r#"{"n":1}"#).await, 200);
        let sref = project.stream_ref(name);
        let desc = state.registry.get(&sref).await.unwrap().unwrap();
        rows.push((sref, desc.resolve_segment("").identity));
    }
    let engine = state
        .shards
        .open("00")
        .expect("the rig's one shard is open");
    // Ack every row CLEAN while its stream is alive: from here on the
    // dirty-path reconciler never revisits them, only the walk can.
    let mut clean = false;
    for _ in 0..200 {
        crate::billing::drain_once(&state).await.expect("drain");
        let dirty = engine.usage_dirty_scan().await.unwrap();
        clean = rows
            .iter()
            .all(|(_, identity)| dirty.iter().all(|(hash, _)| hash != identity));
        if clean {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        clean,
        "rows must be acked clean before the streams turn terminal"
    );
    for (sref, identity) in &rows {
        let meta = engine.billing_meta(*identity).await.unwrap();
        assert!(
            meta.owned_frame_bytes_current > 0,
            "{sref}: no gauge to leak"
        );
    }
    // Terminal WITHOUT a close: the tombstone as delete_lifecycle writes it
    // (stamp in the same write) minus submit_billing_closes, and an expiry.
    let closed_at = crate::billing::billing_now_ms();
    let tombstoned = state
        .registry
        .cas_update(&rows[0].0, |d| {
            d.deleted = true;
            d.logical_close_ms = Some(closed_at);
            true
        })
        .await
        .unwrap();
    let lapsed = state
        .registry
        .cas_update(&rows[1].0, |d| {
            d.expires_at_ms = Some(closed_at);
            true
        })
        .await
        .unwrap();
    assert!(tombstoned && lapsed);
    let submits = crate::billing::WALK_CLOSE_SUBMITS.load(Ordering::Relaxed);
    // The close is a committer op: walk, then poll the durable gauges.
    let mut closed = false;
    for _ in 0..50 {
        crate::billing::tombstone_walk(&state).await;
        let mut open = 0u64;
        for (_, identity) in &rows[..2] {
            let meta = engine.billing_meta(*identity).await;
            open += meta.map_or(0, |m| m.owned_frame_bytes_current);
        }
        if open == 0 {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        closed,
        "a clean terminal row outside the deployment tenant kept its storage gauge; \
         the rollup would carry it onto every later month"
    );
    assert!(
        crate::billing::WALK_CLOSE_SUBMITS.load(Ordering::Relaxed) >= submits + 2,
        "the WALK is what closed them"
    );
    let live = engine.billing_meta(rows[2].1).await.unwrap();
    assert!(
        live.owned_frame_bytes_current > 0,
        "a live stream's gauge is not the walk's to close"
    );
    engine_shutdown(&state).await;
}

/// Owner decision (second external review, a billing release blocker):
/// recreating a name over an incarnation that expired while idle must not
/// erase what that incarnation's storage still owes. Its row is acked
/// CLEAN, it expires, and the name is recreated before the walk reaches it,
/// so the walk never sees it again. Red before the fix: the old gauge stayed
/// open through every sweep (the rollup carried it monthly). Now the
/// recreation records a closure debt before it replaces the descriptor, the
/// settlement pass closes the old gauge at its persisted expiry and then
/// removes the debt, and the new incarnation's gauge is untouched. Also:
/// a debt a recreation wrote before losing its race to a renewal (the
/// incarnation is still stored and live) is dropped, never acted on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreation_over_an_idle_expired_incarnation_still_closes_its_storage() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (_svc, state, addr) = auth_rig("proj-recreate", "ws_rec", &["c_rec"], None).await;
    let bearer = mint_token("c_rec", "proj-recreate", "ws_rec", 1, 1, "rec-1", 600);
    let project = crate::tenant::ProjectId::new("proj-recreate").unwrap();
    let mut rows = Vec::new();
    for name in ["idle", "renewed"] {
        rig_create(addr, name, &bearer).await;
        assert_eq!(rig_append(addr, name, &bearer, r#"{"n":1}"#).await, 200);
        let desc = state
            .registry
            .get(&project.stream_ref(name))
            .await
            .unwrap()
            .unwrap();
        rows.push(desc);
    }
    let prefix = |d: &crate::registry::StreamDesc| {
        state.shards.prefix_for(&d.segment_route_by_id(0).unwrap())
    };
    assert_eq!(prefix(&rows[0]), prefix(&rows[1]), "the rig's one shard");
    let engine = state
        .shards
        .open(&prefix(&rows[0]))
        .expect("the stream's shard is open");
    let identity = |d: &crate::registry::StreamDesc| d.resolve_segment("").identity;
    let mut clean = false;
    for _ in 0..200 {
        crate::billing::drain_once(&state).await.expect("drain");
        let dirty = engine.usage_dirty_scan().await.unwrap();
        clean = rows
            .iter()
            .all(|d| dirty.iter().all(|(hash, _)| *hash != identity(d)));
        if clean {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(clean, "rows must be acked clean first");
    let open = |meta: Option<crate::billing::SegmentBillingMetaV1>| {
        meta.map_or(0, |m| m.owned_frame_bytes_current)
    };
    assert!(
        open(engine.billing_meta(identity(&rows[0])).await) > 0,
        "no gauge to leak"
    );

    // Idle expiry, then the name is recreated before any walk.
    let expired_at = crate::billing::billing_now_ms();
    let sref = project.stream_ref("idle");
    assert!(
        state
            .registry
            .cas_update(&sref, |d| {
                d.expires_at_ms = Some(expired_at);
                true
            })
            .await
            .unwrap()
    );
    state.registry.invalidate(&sref);
    rig_create(addr, "idle", &bearer).await;
    let fresh = state.registry.get(&sref).await.unwrap().unwrap();
    assert_ne!(
        fresh.stream_epoch, rows[0].stream_epoch,
        "a new incarnation"
    );
    assert_eq!(rig_append(addr, "idle", &bearer, r#"{"n":2}"#).await, 200);

    // A recreation that lost to a renewal: its debt names a live incarnation.
    state.registry.record_replaced(&rows[1]).await.unwrap();

    let mut closed = false;
    for _ in 0..50 {
        crate::billing::tombstone_walk(&state).await;
        crate::billing::replaced::settle_replaced(&state).await;
        if open(engine.billing_meta(identity(&rows[0])).await) == 0 {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(
        closed,
        "the replaced incarnation kept its storage gauge; month close would carry it"
    );
    // Its debt settles on the next pass, and the spurious one is gone.
    let mut debts = usize::MAX;
    for _ in 0..20 {
        crate::billing::replaced::settle_replaced(&state).await;
        debts = state.registry.replaced_page(None, 64).await.unwrap().len();
        if debts == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(debts, 0, "every debt settles or is dropped");
    for (what, desc) in [
        ("the new incarnation", &fresh),
        ("the renewed stream", &rows[1]),
    ] {
        assert!(
            open(engine.billing_meta(identity(desc)).await) > 0,
            "{what}'s gauge is not the debt's to close"
        );
    }
    engine_shutdown(&state).await;
}

/// Whether `identity`'s storage gauge reads zero within ~400 ms: a close is
/// a committer op, so it lands shortly after it is submitted.
async fn gauge_closes(engine: &crate::shard::ShardEngine, identity: [u8; 16]) -> bool {
    for _ in 0..20 {
        let meta = engine.billing_meta(identity).await;
        if meta.map_or(0, |m| m.owned_frame_bytes_current) == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// The settlement pass examines 64 debts per sweep. Debts that keep
/// waiting (their incarnation is still stored and dead, so the walk closes
/// it) used to occupy those 64 slots on every sweep, because each pass
/// listed from the start: a debt sorted after them never settled. Red
/// before the cursor: the replaced incarnation behind 65 waiting debts kept
/// its gauge through every pass. Now each pass resumes after the last debt
/// it finished and wraps at the end, so it is reached on the next sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_closure_debt_pass_reaches_a_debt_behind_waiting_ones() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let (_svc, state, addr) = auth_rig("proj-starve", "ws_starve", &["c_st"], None).await;
    let bearer = mint_token("c_st", "proj-starve", "ws_starve", 1, 1, "st-1", 600);
    let project = crate::tenant::ProjectId::new("proj-starve").unwrap();
    rig_create(addr, "z-idle", &bearer).await;
    assert_eq!(rig_append(addr, "z-idle", &bearer, r#"{"n":1}"#).await, 200);
    let sref = project.stream_ref("z-idle");
    let old = state.registry.get(&sref).await.unwrap().unwrap();
    // 65 dead incarnations whose debts sort first and keep waiting.
    let expired_at = crate::billing::billing_now_ms();
    for index in 0..65 {
        let mut row = old.to_persisted();
        row.name = format!("a-{index:03}");
        row.expires_at_ms = Some(expired_at);
        assert!(state.registry.create(row).await.unwrap().0);
        let dead = state
            .registry
            .get(&project.stream_ref(&format!("a-{index:03}")))
            .await;
        state
            .registry
            .record_replaced(&dead.unwrap().unwrap())
            .await
            .unwrap();
    }
    let engine = state
        .shards
        .open(
            &state
                .shards
                .prefix_for(&old.segment_route_by_id(0).unwrap()),
        )
        .expect("the stream's shard is open");
    let identity = old.resolve_segment("").identity;
    let mut clean = false;
    for _ in 0..200 {
        crate::billing::drain_once(&state).await.expect("drain");
        let dirty = engine.usage_dirty_scan().await.unwrap();
        clean = dirty.iter().all(|(hash, _)| *hash != identity);
        if clean {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(clean, "the row must be acked clean first");
    assert!(
        engine
            .billing_meta(identity)
            .await
            .is_some_and(|m| m.owned_frame_bytes_current > 0)
    );
    assert!(
        state
            .registry
            .cas_update(&sref, |d| {
                d.expires_at_ms = Some(expired_at);
                true
            })
            .await
            .unwrap()
    );
    state.registry.invalidate(&sref);
    rig_create(addr, "z-idle", &bearer).await;

    let mut closed = false;
    for _ in 0..6 {
        crate::billing::replaced::settle_replaced(&state).await;
        closed = gauge_closes(&engine, identity).await;
        if closed {
            break;
        }
    }
    assert!(closed, "the debt behind 65 waiting ones was never reached");
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

/// The snapshot retirement helper cannot be a terminal boundary: a pending
/// opener is absent from the snapshot. The real gate must retain that opener
/// until it finishes, then close it without publishing a new resident.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn billing_terminal_shutdown_waits_for_a_late_open() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let park = Arc::new(tokio::sync::Mutex::new(()));
    let held = park.lock().await;
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            open_park: Some(park.clone()),
            ..Default::default()
        },
    )
    .await;
    // Complete the worker milestone first, so the poll below observes only
    // the pending shard open rather than scheduling the server cancellation.
    let report = rig.tasks.shutdown(Duration::from_secs(5)).await;
    assert!(report.aborted.is_empty());
    assert!(matches!(
        rig.state.shards.open_or_wait("00", Duration::ZERO).await,
        crate::sharddir::OpenOutcome::Wait { .. }
    ));
    assert_eq!(rig.state.shards.open_count(), 0);
    let mut shutdown = std::pin::pin!(rig.shutdown());
    let pending = shutdown
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending();
    // Release even when the assertion fails, so the regression leaves no
    // permanently held fixture operation behind.
    drop(held);
    assert!(
        pending,
        "terminal shutdown reported success before the held open finished"
    );
    shutdown.await;
    assert_eq!(rig.state.shards.open_count(), 0);
    assert!(matches!(
        rig.state.shards.open_or_wait("00", Duration::ZERO).await,
        crate::sharddir::OpenOutcome::Wait {
            code: "shard_closing",
            ..
        }
    ));
}

async fn rollup_rows(db: &slatedb::Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut iter = db.scan(..).await.unwrap();
    while let Some(kv) = iter.next().await.unwrap() {
        rows.push((kv.key.to_vec(), kv.value.to_vec()));
    }
    rows
}

/// Review item 49: an unreadable `_ops_metrics` checkpoint fails the ops
/// step. It never reads as the start of the ledger, which would merge
/// every snapshot into the minute tier a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger() {
    let (state, _) = http_rig(mem()).await;
    let rollup = Arc::new(
        crate::rollup::UsageRollup::open(mem(), "", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.rollup.install(rollup.clone()).is_ok());
    crate::ops::emit_metrics_once(&state).await.expect("emit");
    let mut absorbed = 0;
    for _ in 0..20 {
        let n = crate::billing::ops_rollup_step(&state)
            .await
            .expect("ops rollup");
        absorbed += n;
        if n == 0 {
            break;
        }
    }
    assert!(absorbed >= 1, "the rig's snapshot reached the minute tier");
    let before = rollup_rows(&rollup.db).await;
    assert!(
        before
            .iter()
            .any(|(key, _)| key.as_slice() == crate::rollup::K_OPS_CURSOR),
        "the drain committed its checkpoint"
    );
    crate::rollup::read_faults().lock().unwrap().insert((
        Arc::as_ptr(&rollup.db) as usize,
        crate::rollup::K_OPS_CURSOR.to_vec(),
    ));
    let step = crate::billing::ops_rollup_step(&state).await;
    assert_eq!(
        step,
        Err("injected rollup repository read failure".to_string()),
        "an unreadable checkpoint fails the step; it never restarts the ledger"
    );
    assert_eq!(
        rollup_rows(&rollup.db).await,
        before,
        "a failed required read moves no row and no checkpoint"
    );
    assert_eq!(
        crate::billing::ops_rollup_step(&state)
            .await
            .expect("ops rollup"),
        0,
        "the next step resumes at the committed checkpoint"
    );
    assert_eq!(rollup_rows(&rollup.db).await, before);
    rollup.db.close().await.unwrap();
    engine_shutdown(&state).await;
}
