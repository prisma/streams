//! Fork lifecycle.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// The raw route is the DEFAULT-key stream — including through a fork.
/// Stitched reads passed no key filter at all, so a raw fork of a
/// collection that product clients had written keyed records to
/// replayed every one of them through the standards surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_forks_show_only_the_default_key() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/forkiso", &ct, br#"[{"raw":0}]"#).await;
    assert!(st == 200 || st == 201);
    // Product traffic on other routing keys, interleaved with raw.
    for i in 0..3 {
        for k in ["ka", "kb"] {
            let body = format!("{{\"k\":\"{k}\",\"n\":{i}}}");
            let (st, _, _) = preq(
                addr,
                "POST",
                "/v1/streams/forkiso/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                body.as_bytes(),
            )
            .await;
            assert_eq!(st, 200);
        }
        let body = format!("[{{\"raw\":{}}}]", i + 1);
        let (st, _, _) = hreq(addr, "POST", "/v1/stream/forkiso", &ct, body.as_bytes()).await;
        assert!(st == 200 || st == 204);
    }
    // Fork it at the tail and read the fork through the raw route.
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/forkiso", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, b) = hreq(
        addr,
        "PUT",
        "/v1/stream/forkiso-child",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "forkiso"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    for name in ["forkiso", "forkiso-child"] {
        let (st, _, b) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
        assert_eq!(st, 200);
        let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
        assert!(
            recs.iter().all(|r| r.get("k").is_none()),
            "{name} leaked another routing key: {recs:?}"
        );
        assert_eq!(recs.len(), 4, "{name} default-key records: {recs:?}");
    }
    engine_shutdown(&state).await;
}

/// Deleting a fork tombstones the child and then releases the parent's
/// reference. A crash in between left the parent pinning data for a
/// fork that no longer exists — and a retry bounced off the
/// already-dead check before it could finish the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_half_deleted_fork_finishes_its_cleanup() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dsrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/dsrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/dchild",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "dsrc"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201);
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dsrc"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(src.fork_children.len(), 1, "the source holds the reference");

    // Simulate the crash: tombstone the child with the debt recorded,
    // exactly as delete_lifecycle writes it before releasing.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("dchild"), |d| {
            d.deleted = true;
            d.parent_ref_pending = true;
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dchild"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dsrc"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(src.fork_children.len(), 1, "the reference is still leaked");

    // A retried DELETE finishes the cleanup rather than bouncing.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/dchild", &[], b"").await;
    assert!(st == 404 || st == 410 || st == 204, "retry delete: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dsrc"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dsrc"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        src.fork_children.is_empty(),
        "the parent still pins a dead fork: {:?}",
        src.fork_children
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dchild"));
    let child = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dchild"))
        .await
        .unwrap()
        .unwrap();
    assert!(!child.parent_ref_pending, "the debt is settled");
    engine_shutdown(&state).await;
}

/// A fork initialization is claimed against ONE source incarnation. The
/// creation hash omitted the source epoch, so a retry against a
/// recreated source hashed identically and resumed the original
/// initialization — reference installed on the new incarnation while
/// the child still recorded the old one, which stitched reads only
/// discover later as an epoch mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_initialization_is_bound_to_its_source_incarnation() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fsrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let epoch_a = state
        .registry
        .get(&state.deployment.raw_adapter_sref("fsrc"))
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();

    // A child initialization claimed against incarnation A.
    let fh = [
        ("content-type", "application/json"),
        ("stream-forked-from", "fsrc"),
        ("stream-fork-offset", boundary.as_str()),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fchild", &fh, b"").await;
    assert_eq!(st, 201);
    // Reopen it as an in-flight initialization, as a crash would leave it.
    // The REAL request hash the server computes for this fork against
    // incarnation A. Planting an arbitrary string would make the retry
    // conflict on the hash alone and prove nothing about the epoch.
    let hash_against_a = crate::http::create_request_hash(
        "application/json",
        None,
        None,
        false,
        b"",
        Some(&crate::registry::ForkRef {
            source: "fsrc".into(),
            source_epoch: epoch_a.clone(),
            fork_offset: 1,
            fork_sub: 0,
            fork_id: String::new(),
        }),
    );
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("fchild"), |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: hash_against_a.clone(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fchild"));

    // The source becomes a DIFFERENT incarnation. A delete+recreate is
    // the way that happens in the field, but a source pinned by a fork
    // soft-deletes and refuses recreation, so the incarnation is moved
    // directly here — the identity rule under test is about the epoch,
    // not about how it changed.
    let epoch_b = format!("{:032x}", 0xfeed_beefu64);
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("fsrc"), |d| {
            d.stream_epoch = epoch_b.clone();
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fsrc"));
    assert_ne!(epoch_a, epoch_b, "the source must be a new incarnation");

    // Retrying the same fork request must NOT quietly resume the
    // initialization that was claimed against the old incarnation.
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/fchild", &fh, b"").await;
    assert!(
        st == 409 || st == 403,
        "a fork of a RECREATED source resumed the old initialization: {st} {}",
        String::from_utf8_lossy(&b)
    );
    // The hash the SAME request computes against incarnation B must
    // differ from the one recorded against A — that difference is the
    // mechanism under test, not the conflict above.
    let hash_against_b = crate::http::create_request_hash(
        "application/json",
        None,
        None,
        false,
        b"",
        Some(&crate::registry::ForkRef {
            source: "fsrc".into(),
            source_epoch: epoch_b.clone(),
            fork_offset: 1,
            fork_sub: 0,
            fork_id: String::new(),
        }),
    );
    assert_ne!(
        hash_against_a, hash_against_b,
        "the creation hash ignores the source incarnation"
    );
    let child = state
        .registry
        .get(&state.deployment.raw_adapter_sref("fchild"))
        .await
        .unwrap()
        .unwrap();
    if let Some(f) = &child.forked_from {
        assert_eq!(
            f.source_epoch, epoch_a,
            "the child's recorded parentage changed incarnation"
        );
    }
    engine_shutdown(&state).await;
}

/// Three generations, A <- B <- C, with B already soft-deleted. Deleting
/// C hard-deletes B, which then owes A a release. A crash in between
/// used to strand that reference forever: B is dead, so its CAS refuses,
/// and a retried delete of C reported success having released nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_fork_cascade_can_be_resumed() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/genA", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let fork_of = |src: &str, child: &str| {
        let src = src.to_string();
        let child = child.to_string();
        async move {
            let (_, h, _) = hreq(addr, "GET", &format!("/v1/stream/{src}"), &[], b"").await;
            let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
            let (st, _, b) = hreq(
                addr,
                "PUT",
                &format!("/v1/stream/{child}"),
                &[
                    ("content-type", "application/json"),
                    ("stream-forked-from", &src),
                    ("stream-fork-offset", &boundary),
                ],
                b"",
            )
            .await;
            assert_eq!(st, 201, "fork {child}: {}", String::from_utf8_lossy(&b));
        }
    };
    fork_of("genA", "genB").await;
    fork_of("genB", "genC").await;
    // B is soft-deleted: alive only because C exists.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/genB", &[], b"").await;
    assert!(st == 204 || st == 200);
    let b = state
        .registry
        .get(&state.deployment.raw_adapter_sref("genB"))
        .await
        .unwrap()
        .unwrap();
    assert!(b.soft_deleted && !b.deleted, "B soft-deleted: {b:?}");

    // The crash, through the REAL cascade: deleting C makes B lose its
    // last child, so the production path tombstones B and records its
    // debt — and the failpoint stops it there, before A is released.
    // Nothing about the post-crash state is planted by hand.
    crate::failpoints::stop_after_tombstone("genB");
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/genC", &[], b"").await;
    assert!(st == 204 || st == 200, "delete C: {st}");
    crate::failpoints::stop_after_tombstone_off("genB");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("genB"));
    let bdesc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("genB"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        bdesc.deleted && bdesc.parent_ref_pending,
        "the cascade did not tombstone B with its debt in one write: {bdesc:?}"
    );
    let a = state
        .registry
        .get(&state.deployment.raw_adapter_sref("genA"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.fork_children.len(), 1, "A still pins B");

    // The ORIGINAL request is what a client retries — DELETE C, not the
    // hidden intermediate name. That retry must walk the cascade it
    // could not finish.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/genC", &[], b"").await;
    assert!(st == 404 || st == 410 || st == 204, "resume delete: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("genA"));
    let a = state
        .registry
        .get(&state.deployment.raw_adapter_sref("genA"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        a.fork_children.is_empty(),
        "A still pins a dead generation: {:?}",
        a.fork_children
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("genB"));
    let b = state
        .registry
        .get(&state.deployment.raw_adapter_sref("genB"))
        .await
        .unwrap()
        .unwrap();
    assert!(!b.parent_ref_pending, "the debt is settled");
    engine_shutdown(&state).await;
}

/// A first fork installing its reference must serialize against a
/// concurrent delete of the source. Deterministic, not timing-nudged:
/// the delete is parked immediately before it decides soft-versus-hard,
/// the fork install is allowed to complete, and only then is the delete
/// released. Deciding that from a descriptor read taken BEFORE the
/// write let both win — the fork installed its reference and the delete
/// tombstoned the source anyway, leaving a live fork anchored to a
/// hard-deleted parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_creation_and_source_deletion_serialize() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rsrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/rsrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();

    // Park the delete before its decision, then start it — and PROVE
    // it parked instead of assuming 80 ms was enough.
    let dbefore = crate::failpoints::parked(crate::failpoints::Fp::DeleteBeforeDecision, "rsrc");
    crate::failpoints::park_delete_before_decision("rsrc");
    let del = tokio::spawn(async move { hreq(addr, "DELETE", "/v1/stream/rsrc", &[], b"").await });
    while crate::failpoints::parked(crate::failpoints::Fp::DeleteBeforeDecision, "rsrc") <= dbefore
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    // With the delete held there, the fork installs its reference.
    let (fst, _, fb) = hreq(
        addr,
        "PUT",
        "/v1/stream/rchild",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "rsrc"),
            ("stream-fork-offset", &boundary),
        ],
        b"",
    )
    .await;
    assert_eq!(fst, 201, "fork install: {}", String::from_utf8_lossy(&fb));

    // Release the delete: it must now SEE the child.
    crate::failpoints::release_delete_before_decision("rsrc");
    let (dst, _, _) = del.await.unwrap();
    assert!(dst == 204 || dst == 200, "delete: {dst}");

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rsrc"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rsrc"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !src.deleted,
        "a live fork is anchored to a HARD-deleted source: {src:?}"
    );
    assert!(src.soft_deleted, "the source should be retained: {src:?}");
    assert_eq!(src.fork_children.len(), 1, "the child reference survived");
    // The child can still read its inherited data.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/rchild", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "inherited data unreadable: {recs:?}");
    engine_shutdown(&state).await;
}

/// A fork initialization must resume against a source that is being
/// RETAINED FOR IT. Creation installs the child's reference on the
/// source before publishing the child Ready; if the source is deleted
/// in that window it soft-deletes (data kept precisely for this child),
/// and the retry used to be refused with `fork_source_gone` — leaving
/// the child permanently Initializing over data preserved to serve it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_initialization_resumes_against_a_retained_source() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/retsrc", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/retsrc", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();
    let fh = [
        ("content-type", "application/json"),
        ("stream-forked-from", "retsrc"),
        ("stream-fork-offset", boundary.as_str()),
    ];
    // Create the child: its reference is now installed on the source.
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/retchild", &fh, b"").await;
    assert_eq!(st, 201);
    let real_hash = state
        .registry
        .get(&state.deployment.raw_adapter_sref("retchild"))
        .await
        .unwrap()
        .unwrap()
        .forked_from
        .clone()
        .unwrap();
    assert!(!real_hash.fork_id.is_empty(), "the child stamped its id");

    // Crash before Ready: the child is left Initializing, holding the
    // parentage it already installed.
    let hash = crate::http::create_request_hash(
        "application/json",
        None,
        None,
        false,
        b"",
        Some(&real_hash),
    );
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("retchild"), |d| {
            d.init = Some(crate::registry::InitState {
                request_hash: hash.clone(),
                key_fingerprint: d.key_fingerprint.clone(),
                claimed_ms: crate::shard::now_ms(),
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("retchild"));

    // Now the source is deleted. It has a child reference, so it is
    // RETAINED — soft-deleted, data intact, for this child.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/retsrc", &[], b"").await;
    assert!(st == 204 || st == 200);
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("retsrc"))
        .await
        .unwrap()
        .unwrap();
    assert!(src.soft_deleted && !src.deleted, "source retained: {src:?}");

    // The exact retry must complete the child, not refuse it.
    let (st, _, b) = hreq(addr, "PUT", "/v1/stream/retchild", &fh, b"").await;
    assert!(
        st == 200 || st == 201,
        "the child could not resume against its retained source: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("retchild"));
    let child = state
        .registry
        .get(&state.deployment.raw_adapter_sref("retchild"))
        .await
        .unwrap()
        .unwrap();
    assert!(child.init.is_none(), "the child is still Initializing");
    // …and it reads its inherited records.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/retchild", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "inherited data unreadable: {recs:?}");
    engine_shutdown(&state).await;
}

/// Pinned DS fork contract (regression net for the conformance suite):
/// stitched reads across the boundary, source independence, sub-offset
/// materialization, soft-delete 410s, and the reference cascade.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_lifecycle_and_stitched_reads() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    // Source: three records.
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/fsrc",
        &ct,
        br#"[{"n":0},{"n":1},{"n":2}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    // Fork at record 2 (server-returned tokens are opaque; use the
    // reference zero-literal + JSON sub semantics: 0 + sub 2).
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/ffork",
        &[
            ("content-type", "application/json"),
            ("stream-forked-from", "/v1/stream/fsrc"),
            ("stream-fork-offset", "0000000000000000_0000000000000000"),
            ("stream-fork-sub-offset", "2"),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 201);
    // Fork sees the inherited prefix only.
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/ffork", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 2, "{recs:?}");
    // Appends to fork and source are independent.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/ffork", &ct, br#"[{"f":1}]"#).await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/fsrc", &ct, br#"[{"s":9}]"#).await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/ffork", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(recs[2]["f"], 1, "fork's own append after the prefix");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 4, "source unaffected by the fork's append");

    // Soft-delete: the source with a live fork answers 410 directly,
    // the fork still reads; deleting the fork cascades the source away.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fsrc", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    assert_eq!(st, 410, "soft-deleted source is GONE, not missing");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fsrc", &ct, b"").await;
    assert_eq!(st, 409, "re-creation blocked while forks live");
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/ffork", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs.len(),
        3,
        "fork reads inherited data past the soft delete"
    );
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/ffork", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fsrc", &[], b"").await;
    assert_eq!(
        st, 404,
        "last fork's deletion cascades the source to gone-gone"
    );
    engine_shutdown(&state).await;
}

/// AUDIT P0: the fork lifecycle is idempotent and recoverable.
/// References are installed and released BY ID (a retried delete is a
/// no-op, not a double-release); a stale source incarnation is an
/// integrity error, not a silent cross-incarnation read; the product
/// create path cannot overwrite a retained source; and members of a
/// fork chain never split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_lifecycle_is_idempotent_and_epoch_checked() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let pk = [("prisma-encryption-key", PRISMA_KEY)];

    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/fk-src",
        &ct,
        br#"[{"n":0},{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    // Two identical fork PUTs (a replay): one reference, not two.
    let fh = [
        ("content-type", "application/json"),
        ("stream-forked-from", "/v1/stream/fk-src"),
        ("stream-fork-offset", "0000000000000000_0000000000000000"),
        ("stream-fork-sub-offset", "1"),
    ];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fk-a", &fh, b"").await;
    assert_eq!(st, 201);
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/fk-a", &fh, b"").await;
    assert!(st == 200 || st == 201, "idempotent fork PUT: {st}");
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("fk-src"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        src.fork_children.len(),
        1,
        "one reference per fork: {:?}",
        src.fork_children
    );
    let child = state
        .registry
        .get(&state.deployment.raw_adapter_sref("fk-a"))
        .await
        .unwrap()
        .unwrap();
    let fref = child.forked_from.as_ref().unwrap();
    assert!(!fref.fork_id.is_empty(), "the fork stamps its own id");
    assert_eq!(
        fref.source_epoch, src.stream_epoch,
        "source incarnation recorded"
    );

    // Neither member of the chain may split (stitched reads resolve one
    // segment per ancestor).
    assert!(
        !crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("fk-src"),
            0,
            0x8000_0000_0000_0000
        )
        .await,
        "a stream with live forks must not split"
    );
    assert!(
        !crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("fk-a"),
            0,
            0x8000_0000_0000_0000
        )
        .await,
        "a fork must not split"
    );

    // A stale source incarnation is an integrity error, never a silent
    // read of a recreated source.
    state
        .registry
        .cas_update_retry(&state.deployment.raw_adapter_sref("fk-a"), |d| {
            d.forked_from.as_mut().unwrap().source_epoch =
                "ffffffffffffffffffffffffffffffff".into();
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fk-a"));
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/fk-a", &[], b"").await;
    assert_eq!(
        st,
        500,
        "stale source epoch must fail loudly: {}",
        String::from_utf8_lossy(&b)
    );
    // Restore the true epoch and confirm the read works again.
    let true_epoch = src.stream_epoch.clone();
    state
        .registry
        .cas_update_retry(&state.deployment.raw_adapter_sref("fk-a"), |d| {
            d.forked_from.as_mut().unwrap().source_epoch = true_epoch.clone();
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fk-a"));
    let (st, _, b) = hreq(addr, "GET", "/v1/stream/fk-a", &[], b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "inherited prefix");

    // Soft-delete the source; the PRODUCT create path must not replace
    // a name that still backs a live fork (the raw path already blocks
    // it).
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fk-src", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/fk-src",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(
        st,
        409,
        "product create must not overwrite a retained source: {}",
        String::from_utf8_lossy(&b)
    );

    // A RETRIED delete of the fork releases exactly once; the cascade
    // then removes the retained source.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fk-a", &[], b"").await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/fk-a", &[], b"").await;
    assert!(st == 404 || st == 410 || st == 204, "retried delete: {st}");
    let (st, _, _) = hreq(addr, "GET", "/v1/stream/fk-src", &[], b"").await;
    assert_eq!(st, 404, "last fork released -> source cascades away");
    engine_shutdown(&state).await;
}
