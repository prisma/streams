//! Fork cleanup.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;

// ---------------------------------------------------------------
// Round 13: the fork-reference saga survives a creator crash, and
// queue settlement joins the phantom audit.
// ---------------------------------------------------------------

/// FRK-013, crash variant: the creator dies with the source reference
/// freshly installed (parked forever at the post-install point). The
/// child's tombstone RETAINED its debt — because its earlier release
/// found the reference absent on a live source, which is not
/// conclusive — so the ordinary retry of the child DELETE removes the
/// late-installed reference and frees the source. No resumed creator,
/// no repair tool: the retry the client already owns is the repair.
#[expect(
    clippy::disallowed_methods,
    reason = "crashed creator fixture; the parked creator is released and joined before shutdown; its outcome is irrelevant because the world is repaired around it, but it must park concurrently at the crash point"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_creators_late_reference_is_repaired_by_delete_retry() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk13c", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/frk13c", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();

    // Park the creator BEFORE the install (child pre-check passed),
    // delete the child, then let the install land and park the creator
    // AFTER it — the simulated crash point.
    let pre = crate::failpoints::parked(crate::failpoints::Fp::ForkBeforeSourceRef, "frk13kid");
    let post = crate::failpoints::parked(crate::failpoints::Fp::ForkAfterSourceRef, "frk13kid");
    crate::failpoints::park_fork_before_source_ref("frk13kid");
    crate::failpoints::park_fork_after_source_ref("frk13kid");
    let b2 = boundary.clone();
    let creator = tokio::spawn(async move {
        hreq(
            addr,
            "PUT",
            "/v1/stream/frk13kid",
            &[
                ("content-type", "application/json"),
                ("stream-forked-from", "frk13c"),
                ("stream-fork-offset", &b2),
            ],
            b"",
        )
        .await
    });
    while crate::failpoints::parked(crate::failpoints::Fp::ForkBeforeSourceRef, "frk13kid") <= pre {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // Child dies in the stamp-to-install window; its tombstone keeps
    // the debt (the reference is absent on a live source — not
    // conclusive).
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk13kid", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk13kid"));
    let tomb = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk13kid"))
        .await
        .unwrap()
        .unwrap();
    assert!(tomb.deleted, "no tombstone");
    assert!(
        tomb.parent_ref_pending,
        "the tombstone cleared its debt on an inconclusive release"
    );

    // The install lands; the creator "crashes" (stays parked forever).
    crate::failpoints::release_fork_before_source_ref("frk13kid");
    while crate::failpoints::parked(crate::failpoints::Fp::ForkAfterSourceRef, "frk13kid") <= post {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk13c"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk13c"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        src.fork_children.len(),
        1,
        "the late install did not land — the crash window is not being tested"
    );

    // THE REPAIR: the ordinary retry of the child DELETE. Its retained
    // debt retries the release, finds the late reference, removes it.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk13kid", &[], b"").await;
    assert!(
        st == 204 || st == 200 || st == 404 || st == 410,
        "retry delete: {st}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk13c"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk13c"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        src.fork_children.is_empty(),
        "the crashed creator's reference survived the delete retry: {:?}",
        src.fork_children
    );
    // And the source hard-deletes cleanly.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk13c", &[], b"").await;
    assert!(st == 204 || st == 200, "source delete: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk13c"));
    let gone = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk13c"))
        .await
        .unwrap();
    assert!(
        gone.is_none() || gone.as_ref().is_some_and(|d| d.deleted && !d.soft_deleted),
        "the source was retained by the crashed creator's reference: {gone:?}"
    );
    // Unpark the "dead" creator so shutdown is clean; its outcome is
    // irrelevant — the world has already been repaired around it.
    crate::failpoints::release_fork_after_source_ref("frk13kid");
    creator.await.expect("the released creator completed");
    engine_shutdown(&state).await;
}

/// A delayed fork-reference release must not act on a RECREATED source:
/// its incarnation fence means a release against a replaced source is
/// conclusive (nothing pinned) rather than evaluating that stranger's
/// lifecycle. Driven at the registry level with the real helper.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_fork_release_does_not_touch_a_recreated_source() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk14src", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk14src"));
    let old_epoch = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk14src"))
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();

    // Recreate the source name at a new incarnation: DELETE (childless,
    // so a clean hard delete) then PUT.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk14src", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk14src", &ct, br#"[{"n":9}]"#).await;
    assert!(st == 200 || st == 201, "recreate: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk14src"));
    let fresh = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk14src"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(fresh.stream_epoch, old_epoch);

    // Put the REPLACEMENT into a soft-deleted-with-one-child state, so
    // a release that ignored the epoch fence WOULD tombstone it.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("frk14src"), |d| {
            d.soft_deleted = true;
            d.fork_children = vec!["ghost".into()];
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk14src"));

    // A release carrying the OLD source epoch is conclusive and leaves
    // the replacement untouched — not soft-deleted, not tombstoned.
    let conclusive = crate::http::release_fork_ref_for_test(
        &state,
        state.deployment.raw_adapter_sref("frk14src"),
        "ghost",
        &old_epoch,
    )
    .await
    .unwrap();
    assert!(conclusive, "a stale release should be conclusive");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk14src"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk14src"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.deleted, "a stale release tombstoned the replacement");
    assert_eq!(
        d.fork_children,
        vec!["ghost".to_string()],
        "a stale release mutated the replacement's children"
    );
    assert_eq!(d.stream_epoch, fresh.stream_epoch);
    engine_shutdown(&state).await;
}

/// Wait, boundedly, until a request of `name` arrives at `fp` beyond the
/// `past` arrivals counted before the schedule started.
async fn park_reached(fp: crate::failpoints::Fp, name: &str, past: usize) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while crate::failpoints::parked(fp, name) <= past {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{name} never reached {fp:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// TLA-019-F4, success variant: the same crash window as
/// `a_crashed_creators_late_reference_is_repaired_by_delete_retry`, but the
/// client's DELETE already returned success, so nothing tells it to repeat
/// it. The source is then deleted and soft-retained by the phantom
/// reference. The service itself must release that reference and tombstone
/// the source within a bounded time, with no second DELETE of the child.
#[expect(
    clippy::disallowed_methods,
    reason = "crashed creator fixture; the parked creator is released and joined before shutdown; it must park concurrently at the crash point while the service repairs the world around it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_creators_late_reference_is_released_without_a_client_retry() {
    let _serial = gap_lock().lock().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk19src", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/frk19src", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let src_ref = state.deployment.raw_adapter_sref("frk19src");
    let kid_ref = state.deployment.raw_adapter_sref("frk19kid");

    let pre = crate::failpoints::parked(crate::failpoints::Fp::ForkBeforeSourceRef, "frk19kid");
    let post = crate::failpoints::parked(crate::failpoints::Fp::ForkAfterSourceRef, "frk19kid");
    crate::failpoints::park_fork_before_source_ref("frk19kid");
    crate::failpoints::park_fork_after_source_ref("frk19kid");
    let creator = tokio::spawn(async move {
        hreq(
            addr,
            "PUT",
            "/v1/stream/frk19kid",
            &[
                ("content-type", "application/json"),
                ("stream-forked-from", "frk19src"),
                ("stream-fork-offset", &boundary),
            ],
            b"",
        )
        .await
    });
    park_reached(crate::failpoints::Fp::ForkBeforeSourceRef, "frk19kid", pre).await;
    // The child's DELETE succeeds while the creator sits between its
    // pre-check and the install: the in-request release is inconclusive.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk19kid", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    state.registry.invalidate(&kid_ref);
    let tomb = state.registry.get(&kid_ref).await.unwrap().unwrap();
    assert!(
        tomb.deleted && tomb.parent_ref_pending,
        "no retained debt: {tomb:?}"
    );

    // The late install lands; the creator "crashes" before its post-check.
    crate::failpoints::release_fork_before_source_ref("frk19kid");
    park_reached(crate::failpoints::Fp::ForkAfterSourceRef, "frk19kid", post).await;
    state.registry.invalidate(&src_ref);
    let src = state.registry.get(&src_ref).await.unwrap().unwrap();
    assert_eq!(src.fork_children.len(), 1, "the late install did not land");

    // The customer deletes the source: the phantom reference retains it.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk19src", &[], b"").await;
    assert!(st == 204 || st == 200, "source delete: {st}");
    state.registry.invalidate(&src_ref);
    let src = state.registry.get(&src_ref).await.unwrap().unwrap();
    assert!(
        src.soft_deleted && !src.deleted,
        "source not retained: {src:?}"
    );

    // No second DELETE of the child: the service owns the repair. Its
    // reconciler starts here, as in production but after the staged
    // schedule, so no pass can repair the window before it is observed.
    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        std::time::Duration::from_millis(50),
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let settled = loop {
        state.registry.invalidate(&src_ref);
        let src = state.registry.get(&src_ref).await.unwrap().unwrap();
        if src.fork_children.is_empty() && src.deleted {
            break src;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the phantom reference still pins the soft-deleted source: {src:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert!(
        !settled.soft_deleted,
        "the source is not a tombstone: {settled:?}"
    );
    state.registry.invalidate(&kid_ref);
    let tomb = state.registry.get(&kid_ref).await.unwrap().unwrap();
    assert!(
        !tomb.parent_ref_pending,
        "the child's debt survived: {tomb:?}"
    );

    crate::failpoints::release_fork_after_source_ref("frk19kid");
    creator.await.expect("the released creator completed");
    rig.shutdown().await;
}

async fn fork_children(
    state: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> Vec<String> {
    state.registry.invalidate(sref);
    state
        .registry
        .get(sref)
        .await
        .unwrap()
        .unwrap()
        .fork_children
        .clone()
}

/// Whether the stream's current descriptor still owes its fork source.
async fn owes_parent(
    state: &std::sync::Arc<crate::http::AppState>,
    sref: &crate::tenant::TenantStreamRef,
) -> bool {
    state.registry.invalidate(sref);
    state
        .registry
        .get(sref)
        .await
        .unwrap()
        .unwrap()
        .parent_ref_pending
}

async fn indexed_debts(state: &std::sync::Arc<crate::http::AppState>) -> usize {
    state
        .registry
        .fork_debt_page(None, 1000)
        .await
        .unwrap()
        .debts
        .len()
}

/// The reconciler against a creator that is still alive. It runs while the
/// creator sits before its install (inconclusive: it must keep the debt), and
/// again while the creator sits between its late install and its post-check,
/// after the child's name was recreated by a second fork of the same source.
/// It releases only the dead incarnation's reference, never the new child's,
/// and the live creator's own post-check still refuses its create.
#[expect(
    clippy::disallowed_methods,
    reason = "live creator race fixture; both parked creator handles are released and joined before shutdown; they must park concurrently while reconciler passes interleave with them"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reconciler_never_releases_a_reference_a_live_creator_or_new_child_holds() {
    let _serial = gap_lock().lock().await;
    let rig = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default()).await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let service = state.creation_service();
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk20src", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/frk20src", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let src_ref = state.deployment.raw_adapter_sref("frk20src");
    let kid_ref = state.deployment.raw_adapter_sref("frk20kid");
    let fork = move || {
        let boundary = boundary.clone();
        tokio::spawn(async move {
            hreq(
                addr,
                "PUT",
                "/v1/stream/frk20kid",
                &[
                    ("content-type", "application/json"),
                    ("stream-forked-from", "frk20src"),
                    ("stream-fork-offset", &boundary),
                ],
                b"",
            )
            .await
            .0
        })
    };
    let (pre, post) = (
        crate::failpoints::parked(crate::failpoints::Fp::ForkBeforeSourceRef, "frk20kid"),
        crate::failpoints::parked(crate::failpoints::Fp::ForkAfterSourceRef, "frk20kid"),
    );
    crate::failpoints::park_fork_before_source_ref("frk20kid");
    crate::failpoints::park_fork_after_source_ref("frk20kid");
    let first = fork();
    park_reached(crate::failpoints::Fp::ForkBeforeSourceRef, "frk20kid", pre).await;
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk20kid", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    state.registry.invalidate(&kid_ref);
    let dead = state.registry.get(&kid_ref).await.unwrap().unwrap();
    assert!(dead.deleted && dead.parent_ref_pending);

    // Before the install: absent on the live source is inconclusive.
    let (pass, _) = service.reconcile_fork_debt(None).await.unwrap();
    assert_eq!((pass.settled, pass.pending), (0, 1), "{pass:?}");
    assert!(owes_parent(&state, &kid_ref).await);
    assert_eq!(indexed_debts(&state).await, 1);

    // The first creator installs and parks before its post-check; a second
    // fork recreates the child's name and parks after its own install.
    crate::failpoints::release_fork_before_source_ref("frk20kid");
    park_reached(crate::failpoints::Fp::ForkAfterSourceRef, "frk20kid", post).await;
    assert_eq!(
        fork_children(&state, &src_ref).await,
        vec![dead.stream_epoch.clone()]
    );
    let second = fork();
    park_reached(
        crate::failpoints::Fp::ForkAfterSourceRef,
        "frk20kid",
        post + 1,
    )
    .await;
    state.registry.invalidate(&kid_ref);
    let fresh = state.registry.get(&kid_ref).await.unwrap().unwrap();
    assert_ne!(
        fresh.stream_epoch, dead.stream_epoch,
        "the name was not recreated"
    );
    assert_eq!(fork_children(&state, &src_ref).await.len(), 2);

    // The dead incarnation's reference goes; the new child's stays.
    let (pass, _) = service.reconcile_fork_debt(None).await.unwrap();
    assert_eq!(pass.settled, 1, "{pass:?}");
    assert_eq!(
        fork_children(&state, &src_ref).await,
        vec![fresh.stream_epoch.clone()]
    );
    assert_eq!(indexed_debts(&state).await, 0);

    // Both creators finish: the dead one's post-check refuses its create,
    // the live one's fork is served.
    crate::failpoints::release_fork_after_source_ref("frk20kid");
    let (first, second) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!(first, 409, "the dead incarnation's creator was not refused");
    assert!(
        second == 200 || second == 201,
        "the live fork failed: {second}"
    );
    assert_eq!(
        fork_children(&state, &src_ref).await,
        vec![fresh.stream_epoch.clone()]
    );
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/frk20kid", &[], b"").await;
    assert_eq!(st, 200, "the live fork is not readable");
    let (pass, _) = service.reconcile_fork_debt(None).await.unwrap();
    assert_eq!(
        (pass.settled, pass.pending, pass.deferred),
        (0, 0, 0),
        "{pass:?}"
    );
    assert_eq!(fork_children(&state, &src_ref).await.len(), 1);
    rig.shutdown().await;
}

/// A Ready fork `frk21kid` of `frk21src` whose DELETE indexed its debt,
/// wrote its tombstone and died before its release; the customer then
/// deleted the source, which the unreleased reference retains.
async fn stage_crashed_child_delete(
    state: &std::sync::Arc<crate::http::AppState>,
    addr: std::net::SocketAddr,
) -> (
    crate::tenant::TenantStreamRef,
    crate::tenant::TenantStreamRef,
    String,
) {
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk21src", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/frk21src", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/frk21kid",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "frk21src"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "fork: {st}");
    let src_ref = state.deployment.raw_adapter_sref("frk21src");
    let kid_ref = state.deployment.raw_adapter_sref("frk21kid");

    state.registry.invalidate(&kid_ref);
    let kid = state.registry.get(&kid_ref).await.unwrap().unwrap();
    state.registry.record_fork_debt(&kid).await.unwrap();
    state
        .registry
        .cas_update(&kid_ref, |d| {
            d.deleted = true;
            d.parent_ref_pending = true;
            true
        })
        .await
        .unwrap();
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk21src", &[], b"").await;
    assert!(st == 204 || st == 200, "source delete: {st}");
    state.registry.invalidate(&src_ref);
    assert!(
        state
            .registry
            .get(&src_ref)
            .await
            .unwrap()
            .unwrap()
            .soft_deleted
    );

    (src_ref, kid_ref, kid.stream_epoch.clone())
}

/// Restart safety: the reconciler is cancelled by a process shutdown in the
/// middle of a pass (parked inside the release, after its epoch check), with
/// the debt of a child DELETE that died after its tombstone write. Nothing
/// durable changes, and a reconciler in the next process over the same store
/// completes the repair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_reconcile_pass_is_completed_after_a_restart() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let rig = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions::default(),
    )
    .await;
    let (state, addr) = (rig.state.clone(), rig.addr);
    let (src_ref, kid_ref, kid_epoch) = stage_crashed_child_delete(&state, addr).await;
    let parked =
        crate::failpoints::parked(crate::failpoints::Fp::ReleaseAfterEpochCheck, "frk21src");
    crate::failpoints::park_release_after_epoch_check("frk21src");
    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        std::time::Duration::from_millis(50),
    );
    park_reached(
        crate::failpoints::Fp::ReleaseAfterEpochCheck,
        "frk21src",
        parked,
    )
    .await;
    // The process stops mid-pass; the reconciler observes cancellation.
    rig.shutdown().await;
    crate::failpoints::release_release_after_epoch_check("frk21src");
    state.registry.invalidate(&src_ref);
    let src = state.registry.get(&src_ref).await.unwrap().unwrap();
    assert!(src.soft_deleted && src.fork_children == vec![kid_epoch.clone()]);
    assert!(owes_parent(&state, &kid_ref).await);
    assert_eq!(
        indexed_debts(&state).await,
        1,
        "the interrupted pass lost its marker"
    );

    let rig = http_rig_build(store, RigRuntime::incarnation(1), HttpRigOptions::default()).await;
    let state = rig.state.clone();
    crate::application::creation::spawn_fork_debt_reconciler(
        state.creation_service(),
        &rig.tasks,
        std::time::Duration::from_millis(50),
    );
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        state.registry.invalidate(&src_ref);
        let src = state.registry.get(&src_ref).await.unwrap().unwrap();
        if src.deleted && src.fork_children.is_empty() && indexed_debts(&state).await == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restarted reconciler did not finish the repair: {src:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!owes_parent(&state, &kid_ref).await);
    rig.shutdown().await;
}
