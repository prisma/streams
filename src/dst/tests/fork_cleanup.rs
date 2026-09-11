//! Fork cleanup.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::hreq;
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
