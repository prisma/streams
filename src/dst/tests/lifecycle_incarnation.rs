//! Lifecycle incarnation.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// Round 15: the ABA window INSIDE release_fork_ref. The release
/// validates epoch A, then parks (the failpoint models a slow
/// cleanup); meanwhile A is hard-deleted and the name recreated as B,
/// conditioned so an unfenced mutation would visibly change it (same
/// fork id among its children, soft-deleted, childless-after-removal
/// => tombstone). The resumed release must carry its SNAPSHOT's epoch
/// into the mutation — B stays byte-for-byte untouched, and the stale
/// release reports CONCLUSIVE so the debt converges.
#[expect(
    clippy::disallowed_methods,
    reason = "fork release fixture; the parked release is released and joined before its verdict is examined; running it inline cannot park it between its epoch check and its mutation"
)]
#[expect(
    clippy::too_many_lines,
    reason = "fork release scenario; parking the release, deleting and recreating underneath it and checking the replacement form one causal sequence; helper phases would hide which incarnation the release touched"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_parked_across_recreation_cannot_touch_the_replacement() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("content-type", "application/json"),
    ];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/frk15src",
        &ct,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk15src"));
    let epoch_a = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk15src"))
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();

    // The in-flight release, parked between its snapshot (epoch A
    // validated) and its mutation.
    crate::failpoints::park_release_after_epoch_check("frk15src");
    let before_parked =
        crate::failpoints::parked(crate::failpoints::Fp::ReleaseAfterEpochCheck, "frk15src");
    let st2 = state.clone();
    let ea = epoch_a.clone();
    let rel = tokio::spawn(async move {
        crate::http::release_fork_ref_for_test(
            &st2,
            st2.deployment.raw_adapter_sref("frk15src"),
            "frk15-ghost-fork",
            &ea,
        )
        .await
    });
    while crate::failpoints::parked(crate::failpoints::Fp::ReleaseAfterEpochCheck, "frk15src")
        == before_parked
    {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    // A dies; the name is reborn as B...
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/frk15src", &ct, b"").await;
    assert!(st == 200 || st == 204, "delete A: {st}");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/frk15src",
        &ct,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "recreate as B");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk15src"));
    let fresh = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk15src"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(fresh.stream_epoch, epoch_a);

    // ...conditioned so an UNFENCED release would visibly mutate it:
    // it holds the very fork id the stale release carries, and is
    // soft-deleted, so removing that child would tombstone it.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("frk15src"), |d| {
            d.soft_deleted = true;
            d.fork_children = vec!["frk15-ghost-fork".into()];
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk15src"));
    let b_before = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk15src"))
        .await
        .unwrap()
        .unwrap();
    let b_bytes_before = serde_json::to_vec(&b_before).unwrap();

    // Resume the stale release.
    crate::failpoints::release_release_after_epoch_check("frk15src");
    let conclusive = rel.await.unwrap().expect("release must not error");
    assert!(
        conclusive,
        "a release for a dead incarnation must be conclusive so its debt converges"
    );

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk15src"));
    let b_after = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk15src"))
        .await
        .unwrap()
        .unwrap();
    let b_bytes_after = serde_json::to_vec(&b_after).unwrap();
    assert_eq!(
        b_bytes_before, b_bytes_after,
        "the parked release mutated the replacement incarnation"
    );
    engine_shutdown(&state).await;
}

// ---------------------------------------------------------------
// ABA: a name outlives its contents. Every in-flight lifecycle
// operation is issued against ONE incarnation, and a liveness check
// ("is this descriptor still alive?") cannot tell "still mine" from
// "deleted and recreated while I was parked". Each of these parks a
// real operation in its real window, deletes the stream, recreates it,
// and requires the parked operation to decline rather than apply its
// decision to a stranger.
// ---------------------------------------------------------------

/// Create → delete → recreate, with BOTH creators in the readiness
/// window and carrying the SAME request bytes. Only the incarnation
/// distinguishes them, which is the point: a check on liveness, or on
/// "is an initialization with my request hash outstanding", cannot
/// tell them apart. The stale creator must decline, and — the
/// observable that makes this more than a tautology — the LIVE creator
/// must still succeed. Without the fence the stale one clears the
/// replacement's claim, publishing it as ready before its records land
/// and leaving its rightful creator to fail.
#[expect(
    clippy::disallowed_methods,
    reason = "parked creation fixture; both creators are released and joined before their verdicts are compared; both must be parked in the same readiness window at once"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_create_never_publishes_readiness_for_a_later_incarnation() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let body = br#"[{"n":1}]"#;

    let before = crate::failpoints::parked(crate::failpoints::Fp::CreateBeforeReady, "abacreate");
    crate::failpoints::park_create_before_ready("abacreate");
    let stale =
        tokio::spawn(async move { hreq(addr, "PUT", "/v1/stream/abacreate", &ct, body).await });
    let first_epoch = parked_epoch(
        &state,
        crate::failpoints::Fp::CreateBeforeReady,
        "abacreate",
        before,
    )
    .await;
    assert!(
        !first_epoch.is_empty(),
        "the first creator never reached its window"
    );

    // Delete it out from under that creator, then create the same name
    // again from the same bytes. The replacement parks in the same
    // window, so both are in flight at once.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/abacreate", &[], b"").await;
    assert!(
        st == 204 || st == 200 || st == 404 || st == 410,
        "delete: {st}"
    );
    let live =
        tokio::spawn(async move { hreq(addr, "PUT", "/v1/stream/abacreate", &ct, body).await });
    let second_epoch = parked_epoch(
        &state,
        crate::failpoints::Fp::CreateBeforeReady,
        "abacreate",
        before + 1,
    )
    .await;
    assert!(
        !second_epoch.is_empty(),
        "the replacement never reached the window"
    );
    assert_ne!(second_epoch, first_epoch, "the incarnation never changed");

    // Release both.
    crate::failpoints::release_create_before_ready("abacreate");
    let (stale_st, _, sb) = stale.await.unwrap();
    let (live_st, _, lb) = live.await.unwrap();
    assert!(
        stale_st != 200 && stale_st != 201,
        "the stale creator reported success across incarnations: {stale_st} {}",
        String::from_utf8_lossy(&sb)
    );
    assert!(
        live_st == 200 || live_st == 201,
        "the rightful creator was defeated by a stale one: {live_st} {}",
        String::from_utf8_lossy(&lb)
    );

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abacreate"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abacreate"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.stream_epoch, second_epoch,
        "the surviving incarnation is not the live one"
    );
    assert!(d.init.is_none(), "the replacement was left initializing");
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/abacreate", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs,
        vec![serde_json::json!({"n": 1})],
        "the replacement lost its content: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// The epoch of `name` once more operations than `threshold` are parked at
/// `fp`; empty if no operation reaches that window.
async fn parked_epoch(
    state: &crate::http::AppState,
    fp: crate::failpoints::Fp,
    name: &str,
    threshold: usize,
) -> String {
    for _ in 0..300 {
        if crate::failpoints::parked(fp, name) <= threshold {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            continue;
        }
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref(name));
        return match state
            .registry
            .get(&state.deployment.raw_adapter_sref(name))
            .await
        {
            Ok(Some(d)) => d.stream_epoch.clone(),
            _ => String::new(),
        };
    }
    String::new()
}

/// Seal → delete → recreate. A seal is issued against the incarnation
/// the caller asked to close; applying it to a replacement closes a
/// collection nobody asked to close.
#[expect(
    clippy::too_many_lines,
    reason = "seal incarnation scenario; the parked seal, the delete and recreate underneath it and the replacement's open state form one causal sequence; helper phases would hide which incarnation the seal closed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_in_flight_never_closes_a_later_incarnation() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/abaseal", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // Crash a close between its intent and its records, so a real
    // Sealing claim exists and is owed a record.
    crate::failpoints::stop_after_seal_intent("abaseal");
    let body = br#"[{"fin":1}]"#;
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/abaseal",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert_eq!(st, 503, "the failpoint did not stop the close");
    crate::failpoints::stop_after_seal_intent_off("abaseal");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abaseal"));
    let old = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abaseal"))
        .await
        .unwrap()
        .unwrap();
    assert!(old.sealing.as_ref().is_some_and(|sl| sl.owes_final()));

    // Delete and recreate: the intent belongs to an incarnation that no
    // longer exists.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/abaseal", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/abaseal", &ct, br#"[{"n":9}]"#).await;
    assert!(st == 200 || st == 201, "recreate: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abaseal"));
    let fresh = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abaseal"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        fresh.stream_epoch, old.stream_epoch,
        "the incarnation never changed"
    );
    assert!(
        fresh.sealing.is_none(),
        "the replacement inherited a seal claim"
    );
    assert!(!fresh.sealed, "the replacement was born sealed");

    // A seal request that resolved its descriptor BEFORE the delete now
    // reaches its claim. The collection under that name is alive, open
    // and topologically quiet — every check but one says go — so the
    // incarnation is the only thing standing between a stranger's seal
    // and a collection nobody asked to close.
    let claim = crate::product::enter_sealing_cas(
        &state,
        &state.deployment.raw_adapter_sref("abaseal"),
        "stale-op",
        &crate::registry::SealIntent::Empty,
        &old.stream_epoch,
    )
    .await
    .unwrap();
    assert!(
        matches!(claim, crate::product::EnterSeal::Missing),
        "a seal issued against a dead incarnation claimed its replacement: {claim:?}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abaseal"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abaseal"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none(),
        "a stale seal installed its intent: {:?}",
        d.sealing
    );
    assert!(!d.sealed, "a stale seal closed the replacement");
    assert_eq!(
        d.stream_epoch, fresh.stream_epoch,
        "the incarnation changed again"
    );

    // The replacement's own seal still works.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/abaseal",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        b"",
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "the replacement could not be sealed: {st}"
    );
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/abaseal", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs[0],
        serde_json::json!({"n": 9}),
        "content crossed incarnations: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// Delete → recreate → the parked delete resumes. It must not delete
/// the replacement: the caller asked to delete what was there when they
/// asked, and a name is not an identity.
#[expect(
    clippy::disallowed_methods,
    reason = "parked deletion fixture; the parked deleter is released and joined before the replacement is examined; running it inline cannot park it before its decision"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_delete_never_removes_a_later_incarnation() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let sref = state.deployment.raw_adapter_sref("abadel");
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/abadel", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state.registry.invalidate(&sref);
    let first = state
        .registry
        .get(&sref)
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();

    // Park a delete just before it decides.
    let dbefore = crate::failpoints::parked(crate::failpoints::Fp::DeleteBeforeDecision, "abadel");
    crate::failpoints::park_delete_before_decision("abadel");
    let deleter =
        tokio::spawn(async move { hreq(addr, "DELETE", "/v1/stream/abadel", &[], b"").await });
    while crate::failpoints::parked(crate::failpoints::Fp::DeleteBeforeDecision, "abadel")
        <= dbefore
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // Delete and recreate underneath it, through a SECOND delete that
    // is not parked (the failpoint is armed for the parked one only —
    // release it, let the first finish, then rebuild).
    crate::failpoints::release_delete_before_decision("abadel");
    let (st, _, _) = deleter.await.unwrap();
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/abadel", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 201, "recreate: {st}");
    state.registry.invalidate(&sref);
    let second = state.registry.get(&sref).await.unwrap().unwrap();
    assert_ne!(second.stream_epoch, first, "the incarnation never changed");
    assert!(
        !second.deleted && !second.soft_deleted,
        "the replacement was born deleted"
    );

    // A delete decision issued against the FIRST incarnation is exactly
    // what the fence has to decline. Drive it directly: the request
    // path has already resolved its descriptor by this point, so this
    // is the same CAS the parked deleter would have run.
    let outcome = state
        .registry
        .mutate_incarnation(&sref, &first, |current| {
            let mut next = current.to_persisted();
            next.deleted = true;
            crate::registry::Mutation::Write(next, ())
        })
        .await;
    assert!(
        matches!(
            outcome,
            Ok(crate::registry::MutationResult::IncarnationChanged)
        ),
        "a stale delete decision was not fenced: {outcome:?}"
    );
    state.registry.invalidate(&sref);
    let d = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(
        !d.deleted,
        "the replacement was deleted by a stale decision"
    );
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/abadel", &[], b"").await;
    assert_eq!(st, 200, "the replacement is gone");
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs,
        vec![serde_json::json!({"n": 1})],
        "content crossed incarnations: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// Fork parentage across an ABA of the SOURCE. The fork id is stamped
/// on the child by a CAS; if the child is deleted and recreated while
/// that stamp is in flight, the stamp must not land on the replacement
/// and pin a source it was never forked from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_stamp_never_lands_on_a_later_incarnation() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/abasrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/abasrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/abachild",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "abasrc"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert!(st == 200 || st == 201, "fork: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abachild"));
    let forked = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abachild"))
        .await
        .unwrap()
        .unwrap();
    let stale_epoch = forked.stream_epoch.clone();
    assert!(forked.forked_from.is_some(), "the child has no parentage");

    // Replace the child: an ordinary collection under the same name,
    // with no parent at all.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/abachild", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/abachild", &ct, br#"[{"n":7}]"#).await;
    assert!(st == 200 || st == 201, "recreate: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abachild"));
    let fresh = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abachild"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        fresh.stream_epoch, stale_epoch,
        "the incarnation never changed"
    );
    assert!(
        fresh.forked_from.is_none(),
        "the replacement inherited parentage"
    );

    // A write decided against the deleted fork, replayed: it installs
    // that fork's own valid parentage, and carries the epoch it was
    // decided against, so the production fence declines it.
    let outcome = state
        .registry
        .mutate_incarnation(
            &state.deployment.raw_adapter_sref("abachild"),
            &stale_epoch,
            |current| {
                let mut next = current.to_persisted();
                next.forked_from = forked.forked_from.clone();
                crate::registry::Mutation::Write(next, ())
            },
        )
        .await;
    assert!(
        matches!(
            outcome,
            Ok(crate::registry::MutationResult::IncarnationChanged)
        ),
        "a stale fork stamp was not fenced: {outcome:?}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("abachild"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("abachild"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.forked_from.is_none(),
        "the replacement was pinned to a stranger"
    );
    engine_shutdown(&state).await;
}

/// Readiness is published by a CAS that REFUSES deleted descriptors, so
/// an `Ok(false)` there meant "not published", not "ready". Ignoring it
/// made creation answer 201 for a stream that no longer existed — and
/// for a fork, the source stayed pinned by a child that was never
/// published. Driven through the real create path with a failpoint in
/// the window the audit named: after the source reference is installed,
/// before readiness.
#[expect(
    clippy::disallowed_methods,
    reason = "parked fork creation fixture; the creator is released and joined before its status is checked; it must be parked before readiness while the source is deleted"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creation_does_not_report_success_after_a_concurrent_delete() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/racesrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/racesrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();

    // Park the fork creation just before it publishes readiness.
    crate::failpoints::park_create_before_ready("racechild");
    let creator = {
        let b = boundary.clone();
        tokio::spawn(async move {
            hreq(
                addr,
                "PUT",
                "/v1/stream/racechild",
                &[
                    ("content-type", "application/json"),
                    ("stream-forked-from", "racesrc"),
                    ("stream-fork-offset", &b),
                ],
                b"",
            )
            .await
        })
    };
    // Wait until the child descriptor exists and the source has been
    // pinned — i.e. the creator really is in the window.
    let mut pinned = false;
    for _ in 0..200 {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref("racesrc"));
        if let Ok(Some(d)) = state
            .registry
            .get(&state.deployment.raw_adapter_sref("racesrc"))
            .await
            && !d.fork_children.is_empty()
        {
            pinned = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(pinned, "the creator never reached the readiness window");

    // Delete the child out from under it.
    let (dst, _, _) = hreq(addr, "DELETE", "/v1/stream/racechild", &[], b"").await;
    assert!(
        dst == 204 || dst == 200 || dst == 404 || dst == 410,
        "delete: {dst}"
    );

    crate::failpoints::release_create_before_ready("racechild");
    let (st, _, b) = creator.await.unwrap();
    assert!(
        st != 201 && st != 200,
        "creation reported success for a deleted target: {st} {}",
        String::from_utf8_lossy(&b)
    );
    // …and the source is not left pinned by a child that never existed.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("racesrc"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("racesrc"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        src.fork_children.is_empty(),
        "source pinned by an unpublished child: {:?}",
        src.fork_children
    );
    engine_shutdown(&state).await;
}

/// A raw PUT may replace a dead name only when nothing reads through it, and
/// only the STORED descriptor can say so: the snapshot the request judged first
/// can be up to a cache TTL old and predate a fork another instance anchored.
/// The recreate CAS ignored `fork_children`, so an expired source whose fork
/// was anchored after the snapshot got a fresh epoch, and every read of the
/// fork then failed as "a different incarnation".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raw_recreate_never_replaces_an_expired_source_its_forks_read() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let src = state.deployment.raw_adapter_sref("keptsrc");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keptsrc", &ct, br#"[{"n":0}]"#).await;
    assert_eq!(st, 201);
    // What an instance that never saw the fork holds.
    let unforked = state.registry.get(&src).await.unwrap().unwrap();
    let fork = [
        ("content-type", "application/json"),
        ("stream-forked-from", "keptsrc"),
    ];
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/keptchild", &fork, b"").await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    // The source expires while its fork still reads through it.
    let expired = crate::shard::now_ms() - 1;
    state
        .registry
        .cas_update_retry(&src, |d| {
            d.expires_at_ms = Some(expired);
            true
        })
        .await
        .unwrap();
    let retained = state.registry.get(&src).await.unwrap().unwrap();
    assert_eq!(retained.fork_children.len(), 1, "the fork pins its source");
    let mut stale = unforked.to_persisted();
    stale.expires_at_ms = Some(expired);
    let stale = crate::registry::StreamDesc::try_from(stale).unwrap();
    state.registry.test_poison_cache(&src, stale);

    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/keptsrc", &ct, b"").await;
    assert_eq!(
        st,
        409,
        "a source retained for its fork was recreated: {}",
        String::from_utf8_lossy(&b)
    );
    let refusal: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(refusal["error"]["code"], "gone");
    state.registry.invalidate(&src);
    let after = state.registry.get(&src).await.unwrap().unwrap();
    assert_eq!(after.stream_epoch, retained.stream_epoch, "epoch replaced");
    assert_eq!(
        after.fork_children, retained.fork_children,
        "reference lost"
    );
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/keptchild", &[], b"").await;
    assert_eq!(
        st,
        200,
        "the fork lost its source: {}",
        String::from_utf8_lossy(&b)
    );
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "inherited prefix: {recs:?}");
    // Append and delete answer the same retained verdict.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/keptsrc", &ct, br#"[{"n":1}]"#).await;
    assert_eq!(st, 410, "append to a source retained for its fork");
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/keptsrc", &[], b"").await;
    assert_eq!(st, 410, "delete of a source retained for its fork");
    // Control: an expired name nothing reads through is still replaced.
    let free = state.deployment.raw_adapter_sref("keptfree");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keptfree", &ct, b"").await;
    assert_eq!(st, 201);
    let lapsed = state.registry.get(&free).await.unwrap().unwrap();
    state
        .registry
        .cas_update_retry(&free, |d| {
            d.expires_at_ms = Some(expired);
            true
        })
        .await
        .unwrap();
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/keptfree", &ct, b"").await;
    assert_eq!(st, 201, "an expired name without forks is recreated");
    let fresh = state.registry.get(&free).await.unwrap().unwrap();
    assert_ne!(fresh.stream_epoch, lapsed.stream_epoch);
    engine_shutdown(&state).await;
}

/// A declined recreate CAS is not proof the name is live. The CAS declines a
/// soft-deleted winner too, and the declined arm compared every winner as a
/// live stream: a raw PUT whose snapshot was the name's previous tombstone
/// answered 200 for a name that was deleted and is retained for its fork.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raw_put_behind_a_stale_tombstone_never_revives_a_deleted_source() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let src = state.deployment.raw_adapter_sref("heldsrc");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/heldsrc", &ct, br#"[{"n":0}]"#).await;
    assert_eq!(st, 201);
    let fork = [
        ("content-type", "application/json"),
        ("stream-forked-from", "heldsrc"),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/heldchild", &fork, b"").await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/heldsrc", &[], b"").await;
    assert!(
        st == 200 || st == 204,
        "the fork retains the deleted source: {st}"
    );
    // This instance still holds the tombstone of the name's previous incarnation.
    let mut stale = state
        .registry
        .get(&src)
        .await
        .unwrap()
        .unwrap()
        .to_persisted();
    stale.soft_deleted = false;
    stale.deleted = true;
    stale.fork_children.clear();
    stale.stream_epoch = "00000000000000000000000000000001".into();
    let stale = crate::registry::StreamDesc::try_from(stale).unwrap();
    state.registry.test_poison_cache(&src, stale);

    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/heldsrc", &ct, b"").await;
    assert_eq!(
        st,
        409,
        "a PUT was answered for a deleted name: {}",
        String::from_utf8_lossy(&b)
    );
    let refusal: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(refusal["error"]["code"], "gone");
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/heldsrc", &[], b"").await;
    assert_eq!(st, 410, "the name stays retained for its fork");
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/heldchild", &[], b"").await;
    assert_eq!(st, 200, "the fork still reads through its source");
    // Control: a LIVE winner behind the same stale tombstone is compared,
    // exactly like an idempotent PUT.
    let live = state.deployment.raw_adapter_sref("heldlive");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/heldlive", &ct, b"").await;
    assert_eq!(st, 201);
    let mut stale = state
        .registry
        .get(&live)
        .await
        .unwrap()
        .unwrap()
        .to_persisted();
    stale.deleted = true;
    stale.stream_epoch = "00000000000000000000000000000001".into();
    let stale = crate::registry::StreamDesc::try_from(stale).unwrap();
    state.registry.test_poison_cache(&live, stale);
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/heldlive", &ct, b"").await;
    assert_eq!(st, 200, "a live winner is an idempotent create");
    engine_shutdown(&state).await;
}
