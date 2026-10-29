//! The usage drain's bounds: how many of a shard's dirty rows one drain
//! round takes, and how long a graceful stop gives the round it owes (the
//! owner's decision of 2026-10-08, T9); and a read batch's size, which one
//! `_usage` record bounds.

use super::fixture_http::{
    HttpRigOptions, cold_sealer, http_rig_build, install_read_spool, install_rollup,
};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::dst::{FaultPlan, FaultStore, ObjClass, StoreOp};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

/// T9(a), edge change #121: one drain round takes up to 256 of each
/// shard's dirty rows (64 before), so a shard that dirties N segments
/// within one cadence has every snapshot in `_usage` after ceil(N / 256)
/// rounds, not ceil(N / 64). 400 streams with one append each over the
/// rig's shards, one of which holds more than 256 of them: the first
/// round emits exactly min(rows, 256) snapshots of each shard and, once
/// its acknowledgements apply, the second round the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_drain_round_takes_up_to_256_dirty_rows_of_each_shard() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    // The sealer is not the subject (and the rig's fast one keeps a
    // shard of this many segments from closing at the rig's stop).
    let options = HttpRigOptions {
        seal: Some(cold_sealer),
        ..HttpRigOptions::default()
    };
    let rig = http_rig_build(mem(), RigRuntime::first(), options).await;
    let json = [("content-type", "application/json")];
    for n in 0..400 {
        let path = format!("/v1/stream/page-{n}");
        assert_eq!(hreq(rig.addr, "PUT", &path, &json, b"").await.0, 201);
        let appended = hreq(rig.addr, "POST", &path, &json, br#"[{"n":1}]"#).await;
        assert_eq!(appended.0, 204);
    }
    let dirty = dirty_rows(&rig.state).await;
    assert_eq!(dirty.iter().sum::<usize>(), 400);
    assert!(dirty.iter().any(|rows| *rows > 256), "{dirty:?}");
    let first: usize = dirty.iter().map(|rows| (*rows).min(256)).sum();
    let rest: usize = dirty.iter().map(|rows| rows.saturating_sub(256)).sum();
    // The appends' read-side rows filled one bounded read batch, which
    // rides the first round; the rest of the window is younger than 10 s.
    let sealed_reads = rig.state.billing.unflushed_reads().2;
    assert_eq!(sealed_reads, 1);
    assert_eq!(
        crate::billing::drain_once(&rig.state).await,
        Ok(first + sealed_reads),
        "one round takes up to 256 dirty rows of each shard {dirty:?}"
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while dirty_rows(&rig.state).await.iter().sum::<usize>() != rest {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the first round's acknowledgements apply");
    assert_eq!(crate::billing::drain_once(&rig.state).await, Ok(rest));
    rig.shutdown().await;
}

/// T9(b), edge change #122: a graceful stop cuts a terminal drain round
/// that cannot finish after min(cadence, 5 s), not one cadence: at the
/// default 8 s cadence the stop of a drain parked on a held spool write
/// ends cooperatively after 5 s, so a cadence of 10 s or more no longer
/// reaches the supervisor's 10 s grace either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_graceful_stop_cuts_a_wedged_terminal_round_at_five_seconds() {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    let spool_store = FaultStore::uniform(mem(), 911, FaultPlan::CLEAN);
    let spool = Arc::new(
        crate::billing::ReadSpool::open(spool_store.clone(), "", "t9b-stop", &state.config)
            .await
            .unwrap(),
    );
    assert!(state.billing.install_read_spool(spool.clone()).is_ok());
    state.billing.requeue_reads(vec![crate::billing::ReadBatch {
        source: crate::billing::MeterSource {
            cell: "c".into(),
            instance: "i".into(),
            boot: "t9b-boot".into(),
        },
        seq: 1,
        from_ms: 1,
        to_ms: 2,
        rows: vec![],
    }]);
    let entered = spool_store.hold_class(StoreOp::Put, ObjClass::Sst, u64::MAX);
    let tasks = crate::tasks::TaskSupervisor::new();
    crate::billing::spawn_telemetry(state.clone(), &tasks);
    tokio::time::timeout(Duration::from_secs(3), async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the drain enters the held spool write");
    let stop = std::time::Instant::now();
    let report = tasks.shutdown(Duration::from_secs(10)).await;
    let took = stop.elapsed();
    assert!(
        report.aborted.is_empty(),
        "the stop needs no abort: {report:?}"
    );
    assert!(
        took >= Duration::from_secs(5) && took < Duration::from_secs(6),
        "the terminal round is cut at min(cadence 8 s, 5 s): the stop took {took:?}"
    );
    spool_store.release_hold();
    spool.close_for_tests().await;
    rig.shutdown().await;
}

/// A project whose reads touch 1,000 streams within one 10 s window: every
/// sealed read batch is one record of the drain's `_usage` append, so each
/// must fit the per-record ceiling (131,072 bytes on the rig and in the
/// release profile). A batch over it was refused, and the drain retried the
/// same spooled batch forever: no usage of any project on the cell reached
/// the ledger again. The drain empties the spool and the rollup bills every
/// stream's reads exactly once, and another project's reads of the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_of_a_thousand_streams_in_one_window_bill_once_each() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let rig = billed_rig("billcap-1k").await;
    let state = rig.state.clone();
    let mut bystander = audit_identity(0);
    bystander.project_id = "proj-bystander".into();
    state.billing.meter_read(&bystander, audit_delta());
    for i in 0..1_000 {
        state.billing.meter_read(&audit_identity(i), audit_delta());
    }
    state.billing.reads().seal_if_aged(0);
    drain_until_spool_empty(&state).await;
    assert_billed_once(&state, 1_000).await;
    let (year, month) = crate::billing::utc_year_month(crate::shard::now_ms());
    let month = crate::billing::month_str(year, month);
    let rollup = state.rollup.get().unwrap();
    let (account, project) = (&bystander.account_id, &bystander.project_id);
    let row = rollup.month_row(&month, account, project, &bystander.stream_id);
    let row = row
        .await
        .unwrap()
        .expect("the other project's reads are billed");
    assert_eq!((row.read_operations, row.read_payload_bytes), (3, 1 << 20));
    rig.shutdown().await;
}

/// A read batch already in the spool over the record ceiling (sealed by a
/// binary that sealed at an estimated 1 MiB) drains too, and a second
/// emission of it, as after a lost spool delete, bills nothing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_spooled_read_batch_drains_and_bills_once() {
    let _clock = crate::billing::billing_clock_lock().read().await;
    let rig = billed_rig("billcap-legacy").await;
    let state = rig.state.clone();
    let now = crate::shard::now_ms();
    let legacy = crate::billing::ReadBatch {
        source: crate::billing::MeterSource {
            cell: "cell".into(),
            instance: "billcap-legacy".into(),
            boot: "legacy-boot".into(),
        },
        seq: 7,
        from_ms: now - 1_000,
        to_ms: now,
        rows: (0..2_000)
            .map(|i| crate::billing::ReadRow {
                identity: audit_identity(i),
                read_payload_bytes: 1 << 20,
                read_records: 1 << 10,
                read_operations: 3,
                queue_operations: 0,
                append_requests: 0,
            })
            .collect(),
    };
    assert!(serde_json::to_vec(&legacy).unwrap().len() > 131_072);
    let spool = state.billing.read_spool().unwrap().clone();
    for _ in 0..2 {
        spool.persist(&legacy).await.unwrap();
        drain_until_spool_empty(&state).await;
    }
    assert_billed_once(&state, 2_000).await;
    rig.shutdown().await;
}

async fn billed_rig(instance: &str) -> super::fixture_http::HttpRig {
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let state = &rig.state;
    // The production record ceiling (the CLI default and the release profile).
    state.admission.set_record_ceiling(131_072);
    let rollup = crate::rollup::UsageRollup::open(mem(), "", &state.config);
    install_rollup(state, rollup.await.unwrap()).await;
    let spool = crate::billing::ReadSpool::open(mem(), "", instance, &state.config);
    install_read_spool(state, spool.await.unwrap());
    rig
}

fn audit_identity(i: u32) -> crate::billing::BillingIdentity {
    crate::billing::BillingIdentity {
        account_id: "acct_billcap".into(),
        project_id: "proj-billcap".into(),
        stream_id: format!("{i:032x}"),
        stream_name: format!("audit/s{i:05}"),
    }
}

fn audit_delta() -> crate::billing::RowDelta {
    crate::billing::RowDelta {
        read_payload_bytes: 1 << 20,
        read_records: 1 << 10,
        read_operations: 3,
        ..Default::default()
    }
}

/// Drains until the read spool is empty; every round must be accepted.
async fn drain_until_spool_empty(state: &std::sync::Arc<crate::http::AppState>) {
    let spool = state.billing.read_spool().unwrap().clone();
    for _ in 0..20 {
        let round = crate::billing::drain_once(state).await;
        assert!(round.is_ok(), "the usage drain was refused: {round:?}");
        if spool.depth().await == 0 && state.billing.unflushed_reads().2 == 0 {
            return;
        }
    }
    panic!("the read spool still holds {} batches", spool.depth().await);
}

/// The rollup consumes `_usage`, and each of the first `streams` audit
/// streams carries exactly one window's reads.
async fn assert_billed_once(state: &std::sync::Arc<crate::http::AppState>, streams: u32) {
    while crate::billing::rollup_step(state).await.expect("rollup") > 0 {}
    let (year, month) = crate::billing::utc_year_month(crate::shard::now_ms());
    let month = crate::billing::month_str(year, month);
    let rollup = state.rollup.get().unwrap();
    for i in 0..streams {
        let id = audit_identity(i);
        let row = rollup
            .month_row(&month, &id.account_id, &id.project_id, &id.stream_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("stream {i} has no month row"));
        let billed = (
            row.read_payload_bytes,
            row.read_records,
            row.read_operations,
        );
        assert_eq!(billed, (1 << 20, 1 << 10, 3), "stream {i}");
    }
}

/// Each open shard's dirty rows, for the shards that hold any.
async fn dirty_rows(state: &crate::http::AppState) -> Vec<usize> {
    let mut dirty = Vec::new();
    for engine in state.shards.engines() {
        let rows = engine.usage_dirty_scan().await.unwrap().len();
        if rows > 0 {
            dirty.push(rows);
        }
    }
    dirty
}
