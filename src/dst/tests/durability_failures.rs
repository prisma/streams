//! Failed commit-group verdicts and exact retry recovery.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

// ---------------------------------------------------------------
// DST expansion, workstream 2 (L1-now): deterministic group-write
// failure. Everything a commit group promised fails together, and
// nothing answered from its staging can outlive it.
// ---------------------------------------------------------------

/// DUR-002: original and exact duplicate FORCED into one commit group
/// (hold-commit gate + enqueue counter as entered-proof), and the
/// group write fails: BOTH must receive the group failure. This is
/// the assertion the earlier order-independent version could not
/// make — and it kills the labeling hole where a premature duplicate
/// on one side passed the opposite side's check.
#[expect(
    clippy::disallowed_methods,
    reason = "failed duplicate regression; both requests are joined after the entered-counter proof and group failure; serial requests would not share the failed group"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_group_write_fails_its_duplicate_too() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dur002", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let desc = descriptor(&state, "dur002").await;
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    // Compose the group deterministically: hold the committer, land
    // both requests in the queue (counter-proof), arm, release.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let ph = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let r1 =
        tokio::spawn(
            async move { hreq(addr, "POST", "/v1/stream/dur002", &ph, br#"[{"a":1}]"#).await },
        );
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let r2 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur002",
            &[
                ("content-type", "application/json"),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"[{"a":1}]"#,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);

    let (s1, _, _) = r1.await.unwrap();
    let (s2, _, _) = r2.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 500 && s2 >= 500,
        "a promise outlived its failed group: {s1} {s2}"
    );
    // Recovery: the exact retry commits as the original, exactly once.
    let (s3, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dur002",
        &[
            ("content-type", "application/json"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"[{"a":1}]"#,
    )
    .await;
    assert!(s3 == 200 || s3 == 204, "recovery: {s3}");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/dur002", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    let payloads = recs.iter().filter(|r| r.get("a").is_some()).count();
    assert_eq!(payloads, 1, "exactly-once violated: {recs:?}");
    engine_shutdown(&state).await;
}

/// DUR-006: sequence REUSE (same tuple, different body) judged against
/// a row staged in the same failed group. Both requests forced into
/// one group; both must receive the group failure — never a 409 about
/// a row that was never written.
#[expect(
    clippy::disallowed_methods,
    reason = "failed reuse regression; both competing bodies are joined after the entered-counter proof; concurrent staging is the invariant under test"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reuse_verdict_dies_with_its_failed_group() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/dur006",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let desc = descriptor(&state, "dur006").await;
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    let producer = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let a = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/dur006/records",
            &producer,
            br#"{"x":1}"#,
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let b = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/dur006/records",
            &producer,
            br#"{"x":"DIFFERENT"}"#,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);
    let (s1, _, _) = a.await.unwrap();
    let (s2, _, _) = b.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 500 && s2 >= 500,
        "a reuse verdict (or its original) outlived the failed group: {s1} {s2}"
    );
    // Ground truth: the original retry commits; only THEN is reuse a
    // durable fact and the different-body request conflicts.
    let (s3, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/dur006/records",
        &producer,
        br#"{"x":1}"#,
    )
    .await;
    assert_eq!(s3, 200, "original retry: {s3}");
    let (s4, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/dur006/records",
        &producer,
        br#"{"x":"DIFFERENT"}"#,
    )
    .await;
    assert_eq!(s4, 409, "reuse with durable ground: {s4}");
    engine_shutdown(&state).await;
}

/// DUR-004: close-only and its exact retry in ONE failed group — both
/// fail, nothing publishes sealing, and the later plain close seals
/// exactly once.
#[expect(
    clippy::disallowed_methods,
    reason = "failed close regression; both closes are joined under the existing watchdog after group release; serialization would discard the shared-group retry case"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_group_fails_the_close_and_its_retry_together() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dur004", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let desc = descriptor(&state, "dur004").await;
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    let c1 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur004",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        )
        .await
    });
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let c2 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur004",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);
    // WATCHDOG, not a wait: this test wedged twice in full-suite runs
    // (commit pipeline asleep with both acks outstanding, all workers
    // parked; passes solo). Until that liveness hole is caught in the
    // act, convert an infinite hang into a red, diagnosable failure.
    let (r1, r2) = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        async { (c1.await.unwrap(), c2.await.unwrap()) },
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "LIVENESS WEDGE: closes never returned. enqueued_delta={} tripped={} —              the commit pipeline is asleep with acks outstanding (see task notes)",
            engine.appends_enqueued() - base,
            engine.group_failures_tripped()
        )
    });
    let (s1, _, _) = r1;
    let (s2, _, _) = r2;
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 400 && s2 >= 400,
        "a close (or its idempotent echo) outlived the failed group: {s1} {s2}"
    );
    let d = descriptor(&state, "dur004").await;
    assert!(!d.sealed, "sealing published off a failed close");
    // Recovery.
    let (s3, _, _) = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        hreq(
            addr,
            "POST",
            "/v1/stream/dur004",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        ),
    )
    .await
    .expect("LIVENESS WEDGE: the recovery close never returned");
    assert!(s3 == 200 || s3 == 204, "recovery close: {s3}");
    let d = descriptor(&state, "dur004").await;
    assert!(d.sealed && d.sealing.is_none());
    engine_shutdown(&state).await;
}

/// DUR-004 + SEL-021 in one deterministic shape: a close whose group
/// write FAILS, with a fence in flight. The fence must answer failure
/// or closed=false — NEVER closed=true off staging that died — the
/// close must not report success, its intent survives (a write error
/// is ambiguous), and the exact retry recovers the seal.
#[expect(
    clippy::disallowed_methods,
    reason = "failed close and fence regression; both tasks are joined after the forced shared group; the race between the durable intent and fence is the input"
)]
#[expect(
    clippy::too_many_lines,
    reason = "close-fence recovery regression; the durable intent, forced failed group and exact retry must share one incarnation and claim generation; separate cases would remove that causal history"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_in_a_failed_group_reports_failure_not_closed() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/sel021", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let desc = descriptor(&state, "sel021").await;
    let epoch = desc.stream_epoch.clone();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    // Deterministic order: park the close after its intent, arm the
    // failure, fence AT THE CLAIM'S OWN GENERATION (raising the fence
    // without superseding the close), then release.
    let before = crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "sel021");
    crate::failpoints::park_close_before_enqueue("sel021");
    let body = br#"[{"fin":1}]"#;
    let close = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/sel021",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            body,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "sel021") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "the close never parked");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel021"));
    let claim = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel021"))
        .await
        .unwrap()
        .unwrap()
        .sealing
        .clone()
        .expect("no claim installed");
    // FORCED same group: hold the committer, release the close into
    // the queue, land the fence behind it, arm, release. The fence and
    // the close now share one commit group by construction — the
    // co-residency SEL-021 demands.
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    crate::failpoints::release_close_before_enqueue("sel021");
    while engine.appends_enqueued() < base + 1 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let st_f = state.clone();
    let ep_f = epoch.clone();
    let g_f = claim.claim_generation;
    let fence = tokio::spawn(async move {
        crate::http::fence_segment_for_key(
            &st_f,
            &st_f.deployment.raw_adapter_sref("sel021"),
            &ep_f,
            "",
            g_f,
        )
        .await
    });
    while engine.appends_enqueued() < base + 2 {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    engine.fail_next_group_for(identity);
    drop(hold);
    let fence = fence.await.unwrap();
    // Failure with its group and closed=false are both honest results.
    assert!(
        !matches!(fence, Ok(true)),
        "the fence reported closed=true off a write that failed"
    );
    let (cs, _, _) = close.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        cs >= 400,
        "the close reported success for a failed write: {cs}"
    );
    // A write failure is AMBIGUOUS: the intent stays for the retry.
    let d = descriptor(&state, "sel021").await;
    assert!(
        d.sealing.as_ref().is_some_and(|sl| sl.owes_final()),
        "the ambiguous failure tore down the intent: {:?}",
        d.sealing
    );
    assert!(!d.sealed, "sealed off a failed write");
    // The exact retry recovers: renews the claim, lands the record,
    // seals the collection.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/sel021",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "the exact retry could not recover: {st} {}",
        String::from_utf8_lossy(&b)
    );
    let d = descriptor(&state, "sel021").await;
    assert!(d.sealed && d.sealing.is_none());
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/sel021", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    let fins = recs.iter().filter(|r| r.get("fin").is_some()).count();
    assert_eq!(
        fins, 1,
        "exactly one final after the failed write: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// DUR-008: a Stream-Seq conflict judged against a lane another
/// request staged is not a fact until that staging is durable. If the
/// verdict escaped a failed group, the client would hold a permanent
/// "sequence taken" for a sequence that never existed.
#[expect(
    clippy::disallowed_methods,
    reason = "stream-sequence regression; the first competing request is joined before the durable conflict probe; concurrent staging is required to test the verdict boundary"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_seq_verdict_is_grounded_in_durable_state() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json"), ("stream-seq", "s1")];
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/dur008",
        &[("content-type", "application/json")],
        br#"[{"n":0}]"#,
    )
    .await;
    assert!(st == 200 || st == 201);
    let desc = descriptor(&state, "dur008").await;
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    engine.fail_next_group_for(identity);
    let w1 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur008",
            &[("content-type", "application/json"), ("stream-seq", "s1")],
            br#"[{"a":1}]"#,
        )
        .await
    });
    let (s2, _, _) = hreq(addr, "POST", "/v1/stream/dur008", &ct, br#"[{"b":1}]"#).await;
    let (s1, _, _) = w1.await.unwrap();
    assert_eq!(
        engine.group_failures_tripped(),
        1,
        "the failpoint never fired"
    );
    assert!(
        s1 >= 500 || s2 >= 500,
        "nobody saw the failed write: {s1} {s2}"
    );
    // If either got the CONFLICT verdict, the sequence it was judged
    // against must actually be durable: an exact probe must still
    // conflict. A conflict with no durable ground is the bug.
    if s1 == 409 || s2 == 409 {
        let (sp, _, _) = hreq(
            addr,
            "POST",
            "/v1/stream/dur008",
            &[("content-type", "application/json"), ("stream-seq", "s1")],
            br#"[{"probe":1}]"#,
        )
        .await;
        assert_eq!(sp, 409, "a conflict verdict had no durable ground");
    }
    engine_shutdown(&state).await;
}

/// Each observation must invalidate before reading the same tenant-qualified descriptor.
async fn descriptor(state: &crate::http::AppState, name: &str) -> crate::registry::StreamDesc {
    let sref = state.deployment.raw_adapter_sref(name);
    state.registry.invalidate(&sref);
    state.registry.get(&sref).await.unwrap().unwrap()
}
