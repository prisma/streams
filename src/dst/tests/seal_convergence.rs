//! Seal convergence.

use super::fixture_auth::{sr_rig, sr2_workload_jwt};
use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig, http_rig_named, http_rig_named_at};
use super::fixture_livefeed::{hub_rig_stream, hub_sse_collect};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::sync::Arc;

// ------------------------------------------------------------------
// Round-4 review, seal-500 ROOT CAUSE (red): the seal claim was the
// one bare single-shot descriptor CAS on the lifecycle path. Every
// other descriptor mutator (touch_ttl's slide, fork releases) retries
// its etag-precondition conflicts — their own comments document that
// they race "the close path's seal". When such a writer lands inside
// the claim's read-modify-write window, the seal's put fails its
// precondition ONCE and surfaced as an immediate 500 (~1/56 solo
// runs, load-sensitive). The conflict is benign: re-decide from a
// fresh read.
// ------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_benign_descriptor_conflict_never_fails_a_seal() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scx",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // The next descriptor put for this stream answers the exact error
    // shape a lost etag race produces. The claim CAS must absorb it.
    state.registry.fail_next_put("scx");
    let (st, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/scx:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(
        st,
        200,
        "a one-shot descriptor-write conflict must be retried, not answered 500: {}",
        String::from_utf8_lossy(&body)
    );
    // And the collection really is sealed (the retry COMPLETED the
    // transition, it did not swallow it).
    let (_, _, segs) = preq(addr, "GET", "/v1/segments/scx", &[], b"").await;
    let segs = String::from_utf8_lossy(&segs);
    // Appends are refused on a sealed stream — proof of terminal state.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/scx/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"n":1}"#,
    )
    .await;
    assert_ne!(st, 200, "sealed stream accepted an append:\n{segs}");
}

/// Observe collection terminality through the registry's bounded
/// descriptor-cache staleness window (see the convergence leg's
/// comment: a concurrent refresh that started before a CAS can
/// complete after it and transiently re-cache older bytes). Polls —
/// never weakens — the assertion: terminal Sealed MUST appear within
/// a few cache TTLs or the test fails.
async fn wait_seal_terminal(
    state: &Arc<crate::http::AppState>,
    name: &str,
) -> crate::registry::StreamDesc {
    let sref = state.deployment.raw_adapter_sref(name);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if let Ok(Some(d)) = state.registry.get(&sref).await
            && d.sealed
            && d.sealing.is_none()
        {
            return d;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "collection {name} never reached terminal Sealed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// ==================================================================
// DST expansion (round-4): the SEAL LIFECYCLE crash/resume matrix.
// The spec's rule — every multi-step operation is crashed at each
// boundary, and every fault-heavy run is followed by a liveness phase
// — applied to the transition that had the one unexplained
// intermittent. New failpoint StopBeforeSealedPublish pins the exact
// state between step 2 (segment closes durable) and step 3 (SEALED
// publication).
// ==================================================================

/// A seal interrupted AFTER every segment close is durable must be
/// resumable by a plain retry, and the subscriber protocol must hold:
/// exactly ONE sealed control, then EOF.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_interrupted_before_publication_resumes_on_retry() {
    let _serial = gap_lock().lock().await; // global failpoint registry
    let (state, addr, _promoter, mut sck) = hub_rig_stream("sbr").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"), "subscriber parks:\n{a}");

    // Crash boundary: closes durable, publication withheld.
    crate::failpoints::stop_before_sealed_publish("sbr");
    let (st, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/sbr:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    // The interrupted seal surfaces as RETRYABLE failure (500 +
    // retryable:true through product_seal_only's resumable mapping) —
    // the client contract is "retry", and the next leg does.
    let text = String::from_utf8_lossy(&body);
    assert!(
        st == 500 || st == 503,
        "the interruption must answer retryable failure: {st} {text}"
    );
    assert!(
        text.contains("seal is resumable"),
        "the failure must name its resumability: {text}"
    );

    // The durable prefix really landed: seg 0's CLOSE is durable in
    // its committer, while the collection is NOT yet Sealed.
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sbr"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_some() && !d.sealed,
        "interrupted seal must leave the collection Sealing (sealed={}, sealing={})",
        d.sealed,
        d.sealing.is_some()
    );
    let hash = crate::crypto::RouteHash::for_stream(&d.sref()).0;
    let engine = state.engine_for(&hash).await.unwrap();
    let handle = engine
        .stream_handle(d.resolve_segment("").identity)
        .await
        .unwrap();
    assert!(
        handle.state.lock().unwrap().durable.closed,
        "seg 0's close must be durable at the crash boundary"
    );
    // THE RESUME: a plain retry observes Sealing with closed segments,
    // renews the claim, re-closes idempotently, and publishes.
    crate::failpoints::stop_before_sealed_publish_off("sbr");
    let (st, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/sbr:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(
        st,
        200,
        "retry must complete the transition: {}",
        String::from_utf8_lossy(&body)
    );

    // Subscriber protocol: exactly one sealed control, then EOF.
    let (acc, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert_eq!(
        acc.matches("\"sealed\":true").count(),
        1,
        "exactly one sealed control across the interruption:\n{acc}"
    );
    assert!(eof, "EOF after the final control:\n{acc}");
}

/// Two concurrent plain seals on one open collection: both succeed or
/// one observes the other's terminal state — never a stuck Sealing,
/// never two terminal transitions, exactly one sealed control per
/// subscriber.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_plain_seals_serialize_into_one_terminal_transition() {
    let (state, addr, _promoter, mut sck) = hub_rig_stream("csr").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"), "subscriber parks:\n{a}");

    let mk = || {
        preq(
            addr,
            "POST",
            "/v1/streams/csr:seal",
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"{}",
        )
    };
    let (r1, r2) = tokio::join!(mk(), mk());
    // Each racing seal either drives/observes terminality (200) or
    // answers a RETRYABLE failure naming its resumability — never a
    // false success, never a stuck state.
    for (i, (st, _, body)) in [(1usize, r1), (2, r2)] {
        let text = String::from_utf8_lossy(&body);
        assert!(
            st == 200 || ((500..=504).contains(&st) && text.contains("resumable")),
            "concurrent seal #{i} answered {st}: {text}"
        );
    }
    // CONVERGENCE: one plain retry settles whatever the race left.
    let _ = preq(
        addr,
        "POST",
        "/v1/streams/csr:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    let d = wait_seal_terminal(&state, "csr").await;
    assert!(d.sealed && d.sealing.is_none(), "terminal Sealed state");
    // Terminal exactly once, observable: appends refused afterwards.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/csr/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"n":1}"#,
    )
    .await;
    assert_ne!(st, 200, "sealed stream accepted an append");
    let (acc, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert_eq!(
        acc.matches("\"sealed\":true").count(),
        1,
        "exactly one sealed control under concurrent seals:\n{acc}"
    );
    assert!(eof, "EOF after the final control:\n{acc}");
}

/// Liveness phase (DST spec: every fault-heavy run is followed by a
/// convergence requirement). The seal's segment-close commit group
/// fails THREE times in a row — the deterministic stand-in for store
/// write errors — and every faulted attempt must answer a RETRYABLE
/// resumable failure with entered-proof that the injected group
/// failure really fired; then one plain retry must converge to a
/// terminal Sealed collection with the subscriber protocol intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_converges_through_transient_commit_group_failures() {
    let (state, addr, _promoter, mut sck) = hub_rig_stream("scv").await;
    let (a, _) = hub_sse_collect(&mut sck, 8, |t| t.contains("upToDate")).await;
    assert!(a.contains("upToDate"), "subscriber parks:\n{a}");

    // Resolve the segment identity + engine the close will ride.
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("scv"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();
    let tripped0 = engine.group_failures_tripped();

    for round in 1..=3u32 {
        engine.fail_next_group_for(identity);
        let (st, _, body) = preq(
            addr,
            "POST",
            "/v1/streams/scv:seal",
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"{}",
        )
        .await;
        let text = String::from_utf8_lossy(&body);
        assert_eq!(
            engine.group_failures_tripped(),
            tripped0 + round as usize,
            "round {round}: the injected group failure must have fired \
             (entered-proof, not assumption)"
        );
        assert!(
            (500..=504).contains(&st),
            "round {round}: faulted seal must answer retryable failure: {st} {text}"
        );
        assert!(
            text.contains("resumable"),
            "round {round}: the failure must name its resumability: {text}"
        );
        // The collection stays mid-transition, never half-sealed.
        let d = state
            .registry
            .get(&state.deployment.raw_adapter_sref("scv"))
            .await
            .unwrap()
            .unwrap();
        assert!(!d.sealed, "round {round}: sealed on a failed attempt");
        assert!(d.sealing.is_some(), "round {round}: claim lost");
    }

    // CONVERGENCE: no more faults — the documented client contract is
    // "retry until sealed". Bounded retries, first one expected to do.
    let mut st_final = 0;
    let mut trace: Vec<String> = Vec::new();
    for r in 0..6u32 {
        // Rounds 0-2 faulted; 3-5 clean retries.
        if r < 3 {
            engine.fail_next_group_for(identity);
        } else {
            engine.fail_next_group_for([0u8; 16]); // never matches
        }
        let (st, _, body) = preq(
            addr,
            "POST",
            "/v1/streams/scv:seal",
            &[("prisma-encryption-key", PRISMA_KEY)],
            b"{}",
        )
        .await;
        let d_now = state
            .registry
            .get(&state.deployment.raw_adapter_sref("scv"))
            .await
            .unwrap()
            .unwrap();
        trace.push(format!(
            "r{r}: st={st} sealed={} sealing={} gen={} body={}",
            d_now.sealed,
            d_now.sealing.is_some(),
            d_now
                .sealing
                .as_ref()
                .map(|x| x.claim_generation)
                .unwrap_or(0),
            String::from_utf8_lossy(&body)
        ));
        st_final = st;
        if d_now.sealed && d_now.sealing.is_none() {
            break;
        }
    }
    assert_eq!(
        st_final,
        200,
        "the seal must converge after the faults; trace:\n{}",
        trace.join("\n")
    );
    // Terminality is verified through the registry's cached view, and
    // under load a parked DIRECT producer's genuine_closure refresh
    // (it wakes on the close commit, i.e. BEFORE publication) can
    // complete after the seal's own proof-read and transiently
    // re-cache the PRE-publication bytes over the sealed ones. The
    // server-side truth is sealed — the 200 was answered from a fresh
    // proof read — so observe through the bounded staleness window:
    let d = wait_seal_terminal(&state, "scv").await;
    assert!(d.sealed && d.sealing.is_none(), "terminal Sealed state");

    // Subscriber protocol intact across the whole fault-heavy run:
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/scv/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"n":1}"#,
    )
    .await;
    assert_ne!(st, 200, "sealed stream accepted an append");
    let (acc, eof) = hub_sse_collect(&mut sck, 15, |_| false).await;
    assert_eq!(
        acc.matches("\"sealed\":true").count(),
        1,
        "exactly one sealed control despite three interrupted attempts:\n{acc}"
    );
    assert!(eof, "EOF after the final control:\n{acc}");
}

// ==================================================================
// Follow-up review finding 2: CROSS-OWNER SEGMENT CLOSE. Collection
// sealing used to resolve each segment's engine LOCALLY only — a
// segment owned by another instance made seal_identity return None
// and the whole seal answered 500. Now: SegmentClose fleet operation,
// a target-bound internal receiver, and a coordinator that follows
// ONE ownership redirect.
// ==================================================================

/// The receiver: operation-scoped authz, target binding, idempotent
/// close. A segment-read token must NOT open it; a valid request
/// closes the local segment and a retry answers the same frozen
/// offset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_close_receiver_scopes_authorizes_and_is_idempotent() {
    let scopes = "streams.create streams.records.append streams.records.read";
    let (state, addr, _tok) = sr_rig("proj-scl", "ws_scl", "c_scl", "scl-1", scopes).await;
    let ct = ("content-type", "application/json");
    // Stage a raw stream (deployment tenant = the receiver's own
    // registry view) with one record.
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/scl", &[ct, fleet], br#"[{"i":0}]"#).await;
    assert!(st == 200 || st == 201, "stage: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("scl"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("scl"))
        .await
        .unwrap()
        .unwrap();
    let target = crate::product::InternalTarget::of(&desc, 0).unwrap();
    let th: Vec<(String, String)> = target
        .headers()
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();

    // Operation scoping: an empty-operations and a segment-read token
    // must both be refused BEFORE any target work.
    for ops in [&[][..], &["segment-read"][..]] {
        let now = crate::shard::now_ms() / 1000;
        let token = format!("Bearer {}", sr2_workload_jwt("scl-1", ops, now));
        let mut hdrs: Vec<(&str, &str)> =
            th.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        hdrs.push(("authorization", &token));
        let (st, _, body) = hreq(
            addr,
            "POST",
            "/v1/internal/segment-close/scl?seg_id=0&seal_gen=0",
            &hdrs,
            b"",
        )
        .await;
        assert_eq!(
            st,
            401,
            "segment-close must demand its exact operation ({ops:?}): {st} {}",
            String::from_utf8_lossy(&body)
        );
    }

    // A VALID close-token request closes the local segment...
    let now = crate::shard::now_ms() / 1000;
    let close_token = format!(
        "Bearer {}",
        sr2_workload_jwt("scl-1", &["segment-close"], now)
    );
    let mut hdrs: Vec<(&str, &str)> = th.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    hdrs.push(("authorization", &close_token));
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/internal/segment-close/scl?seg_id=0&seal_gen=0",
        &hdrs,
        b"",
    )
    .await;
    assert_eq!(
        st,
        200,
        "segment-close receiver: {}",
        String::from_utf8_lossy(&body)
    );
    // ...and IDEMPOTENTLY answers the same frozen offset on retry.
    let (st2, _, body2) = hreq(
        addr,
        "POST",
        "/v1/internal/segment-close/scl?seg_id=0&seal_gen=0",
        &hdrs,
        b"",
    )
    .await;
    assert_eq!(st2, 200);
    assert_eq!(body, body2, "retried close must answer the same offset");
    engine_shutdown(&state).await;
}

/// The COORDINATOR: when the owner ring says a segment belongs to
/// another instance, the seal relays there instead of failing. Rig B
/// is a full second instance over the SAME object store; rig A
/// overrides the shard's owner to B and carries B's peer URL. The
/// stream is created through A; the collection seal must complete
/// even though its segment close resolves foreign.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collection_seal_relays_segment_closes_to_the_foreign_owner() {
    let store = mem();
    let (state_a, _addr_a) = http_rig_named(store.clone(), "inst-a").await;
    let (state_b, addr_b) = http_rig_named_at(store, "inst-b", RigRuntime::incarnation(1)).await;
    let (st, _, _) = preq(
        _addr_a,
        "PUT",
        "/v1/streams/relay",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        _addr_a,
        "POST",
        "/v1/streams/relay/records",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);

    // A: the shard belongs to inst-b, reachable at addr_b.
    state_a.ownership.set_override("00", "inst-b");
    state_a.peer.set_peer("inst-b", &format!("http://{addr_b}"));

    // THE SEAL from A: seg 0's local resolution says WRONG OWNER and
    // must relay rather than answer "did not close".
    let (st, _, body) = preq(
        _addr_a,
        "POST",
        "/v1/streams/relay:seal",
        &[("prisma-encryption-key", PRISMA_KEY)],
        b"{}",
    )
    .await;
    assert_eq!(
        st,
        200,
        "cross-owner seal must complete: {}",
        String::from_utf8_lossy(&body)
    );
    // Terminal on the shared store, observable from B too.
    let d = wait_seal_terminal(&state_b, "relay").await;
    assert!(d.sealed && d.sealing.is_none());
    engine_shutdown(&state_a).await;
    engine_shutdown(&state_b).await;
}
