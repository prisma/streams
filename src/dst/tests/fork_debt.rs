//! Fork-debt backfill and observability: tombstones older than the fork-debt
//! index are found and repaired without a client DELETE, the one-time
//! backfill resumes across a restart, and a debt that outlives its
//! reconciler circles raises `fork_debt_stale`.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{HttpRigOptions, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::Arc;
use std::time::Duration;

type State = Arc<crate::http::AppState>;
type Ref = crate::tenant::TenantStreamRef;

/// A Ready fork `kid` of `src`, then the state a DELETE of `kid` left behind
/// on a binary older than the fork-debt index: a tombstone that still owes
/// its source, and no marker. With `soft_source` the customer then deletes
/// the source, which the unreleased reference retains; otherwise the
/// reference is removed from the live source first, so the debt's release is
/// inconclusive and stays pending.
async fn plant_debt(
    state: &State,
    addr: std::net::SocketAddr,
    (src, kid): (&str, &str),
    soft_source: bool,
) -> (Ref, Ref) {
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(
        addr,
        "PUT",
        &format!("/v1/stream/{src}"),
        &ct,
        br#"[{"n":0}]"#,
    )
    .await;
    assert!(st == 200 || st == 201, "source: {st}");
    let (_, h, _) = hreq(addr, "GET", &format!("/v1/stream/{src}"), &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let fork = [
        ("content-type", "application/json"),
        ("stream-forked-from", src),
        ("stream-fork-offset", boundary.as_str()),
    ];
    let (st, _, _) = hreq(addr, "PUT", &format!("/v1/stream/{kid}"), &fork, b"").await;
    assert!(st == 200 || st == 201, "fork: {st}");
    let (src_ref, kid_ref) = (
        state.deployment.raw_adapter_sref(src),
        state.deployment.raw_adapter_sref(kid),
    );
    let debt = |d: &mut crate::registry::PersistedDescriptor| {
        d.deleted = true;
        d.parent_ref_pending = true;
        true
    };
    state.registry.cas_update(&kid_ref, debt).await.unwrap();
    if soft_source {
        let (st, _, _) = hreq(addr, "DELETE", &format!("/v1/stream/{src}"), &[], b"").await;
        assert!(st == 204 || st == 200, "source delete: {st}");
    } else {
        let unref = |d: &mut crate::registry::PersistedDescriptor| {
            d.fork_children.clear();
            true
        };
        state.registry.cas_update(&src_ref, unref).await.unwrap();
    }
    state.registry.invalidate(&src_ref);
    state.registry.invalidate(&kid_ref);
    (src_ref, kid_ref)
}

async fn desc(state: &State, sref: &Ref) -> crate::registry::StreamDesc {
    state.registry.invalidate(sref);
    state.registry.get(sref).await.unwrap().unwrap()
}

async fn indexed(state: &State) -> usize {
    let page = state.registry.fork_debt_page(None, 1000).await.unwrap();
    page.debts.len()
}

fn gauge(state: &State, name: &str) -> Option<u64> {
    let gauges = state
        .runtime
        .fork_debt
        .exported(std::collections::BTreeMap::new());
    gauges.get(name).copied()
}

/// Wait up to ten seconds for the source to be released and tombstoned and
/// the index to be empty.
async fn released(state: &State, src_ref: &Ref) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let src = desc(state, src_ref).await;
        if src.deleted && src.fork_children.is_empty() && indexed(state).await == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pre-index debt still pins its source: {src:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A debt-bearing tombstone written before the fork-debt index existed has
/// no marker. The reconciler's one-time backfill finds it, and the source it
/// pins is released and tombstoned without any client DELETE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tombstone_older_than_the_index_is_backfilled_and_released() {
    let _serial = gap_lock().lock().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let (src_ref, kid_ref) = plant_debt(&state, addr, ("frk30src", "frk30kid"), true).await;
    assert!(desc(&state, &src_ref).await.soft_deleted);
    assert_eq!(
        indexed(&state).await,
        0,
        "the planted debt must predate the index"
    );

    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        Duration::from_millis(50),
    );
    released(&state, &src_ref).await;
    assert!(!desc(&state, &kid_ref).await.parent_ref_pending);
    assert_eq!(gauge(&state, "fork_debt_backfill_complete"), Some(1));
    rig.shutdown().await;
}

/// A pre-index debt tombstone whose child name is recreated before the
/// backfill reaches it: the recreation overwrites the tombstone and the debt
/// it carried, and the backfill then finds only the new, live incarnation.
/// The recreation must index the overwritten debt first, so the reconciler
/// pays it from the marker and the source it pinned is tombstoned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_name_keeps_the_debt_of_the_unindexed_tombstone_it_replaced() {
    let _serial = gap_lock().lock().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let (src_ref, kid_ref) = plant_debt(&state, addr, ("frk33src", "frk33kid"), true).await;
    let owed = desc(&state, &kid_ref).await.stream_epoch.clone();
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk33kid", &ct, b"").await;
    assert!(st == 200 || st == 201, "recreate: {st}");
    let kid = desc(&state, &kid_ref).await;
    assert!(
        kid.stream_epoch != owed && !kid.deleted && !kid.parent_ref_pending,
        "the name holds a new incarnation: {kid:?}"
    );
    assert!(desc(&state, &src_ref).await.soft_deleted);

    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        Duration::from_millis(50),
    );
    released(&state, &src_ref).await;
    assert_eq!(gauge(&state, "fork_debt_backfill_complete"), Some(1));
    rig.shutdown().await;
}

/// The backfill resumes from its durable progress after a restart: the next
/// process indexes only what the first had not yet walked, then completes,
/// and its reconciler releases both sources.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_backfill_resumes_after_a_restart() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    // Catalog order is name order: frk31a* sorts before frk31b*.
    let (first, _) = plant_debt(&state, addr, ("frk31asrc", "frk31akid"), true).await;
    let (second, _) = plant_debt(&state, addr, ("frk31bsrc", "frk31bkid"), true).await;
    // One descriptor per step until the first debt is indexed; then stop.
    use crate::registry::fork_debt::BackfillStep;
    let mut steps = 0;
    loop {
        steps += 1;
        assert!(steps < 16, "the backfill never reached the first debt");
        match state.registry.backfill_fork_debt(1).await.unwrap() {
            BackfillStep::Advanced { indexed: 1 } => break,
            BackfillStep::Advanced { indexed: 0 } => {}
            other => panic!("unexpected backfill step {other:?}"),
        }
    }
    assert_eq!(indexed(&state).await, 1);
    rig.shutdown().await;

    let rig = http_rig_build(store, RigRuntime::incarnation(1), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    assert_eq!(
        state.registry.backfill_fork_debt(1000).await.unwrap(),
        BackfillStep::Advanced { indexed: 1 },
        "the restarted backfill walked from the beginning, or missed the second debt"
    );
    assert_eq!(
        state.registry.backfill_fork_debt(1000).await.unwrap(),
        BackfillStep::Complete
    );
    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        Duration::from_millis(50),
    );
    released(&state, &first).await;
    released(&state, &second).await;
    rig.shutdown().await;
}

/// A debt whose release stays inconclusive outlives three reconciler
/// circles: the ops snapshot reports it as pending with its age, and the
/// `fork_debt_stale` alert opens. Paying it resolves the alert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_debt_that_outlives_its_circles_raises_the_stale_alert() {
    let _serial = gap_lock().lock().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let (src_ref, _) = plant_debt(&state, addr, ("frk32src", "frk32kid"), false).await;
    assert_eq!(
        gauge(&state, "fork_debt_pending"),
        None,
        "no reconciler, no gauges"
    );
    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        Duration::from_millis(50),
    );
    let stale = |state: &State| {
        let after = gauge(state, "fork_debt_stale_after_ms").unwrap_or(u64::MAX);
        gauge(state, "fork_debt_pending") == Some(1)
            && gauge(state, "fork_debt_oldest_pending_age_ms").unwrap_or(0) > after
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !stale(&state) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the debt never went stale"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(gauge(&state, "fork_debt_stale_after_ms"), Some(150));
    let alert_open = |state: &State| {
        state
            .runtime
            .ops
            .open_alerts()
            .iter()
            .any(|a| a.fingerprint == "fork_debt_stale")
    };
    crate::ops::evaluate_alerts(&state, &crate::ops::collect_snapshot(&state)).await;
    assert!(alert_open(&state), "the stale debt raised no alert");

    // The source is deleted: a hard-deleted source holds no references, so
    // the debt's release becomes conclusive and the reconciler pays it.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk32src", &[], b"").await;
    assert!(st == 204 || st == 200, "source delete: {st}");
    released(&state, &src_ref).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while gauge(&state, "fork_debt_pending") != Some(0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the gauge kept the paid debt"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    crate::ops::evaluate_alerts(&state, &crate::ops::collect_snapshot(&state)).await;
    assert!(!alert_open(&state), "the paid debt kept the alert open");
    rig.shutdown().await;
}
