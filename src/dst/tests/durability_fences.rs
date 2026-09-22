//! Durability fences.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

// ---------------------------------------------------------------
// Round 9: the fence is a DURABILITY barrier, engine-resident, and
// only the newest reservation installs. Validation binds the epoch.
// ---------------------------------------------------------------

/// Entered-proof: the request under test reached the engine (its op is
/// enqueued, counted from `entered`) and is STILL pending after a grace
/// period. A bare sleep before `!is_finished()` proved only that the
/// runner was slow: a request that had not reached the engine yet passed
/// it vacuously, exactly where a durability regression would hide.
async fn held<T>(
    engine: &crate::shard::ShardEngine,
    entered: u64,
    task: &tokio::task::JoinHandle<T>,
    what: &str,
) {
    for _ in 0..500 {
        if engine.appends_enqueued() >= entered {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        engine.appends_enqueued() >= entered,
        "{what}: the request never reached the engine"
    );
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!task.is_finished(), "{what} concluded before durability");
}

/// The fence's closed-report is a fact about DURABLE state. While the
/// dispatch gate is held (writes applied, durability not yet
/// released), a takeover's fence must not answer — the observable is
/// that the old claim stays UNMARKED. Answering from staged state let
/// the takeover mark a final "committed" off a WriteBatch that could
/// still fail, and publish Sealed over a record that never existed.
#[expect(
    clippy::disallowed_methods,
    reason = "durability fence fixture; the held close and the competing takeover are proven to have entered the engine before their pending checks and are both joined after dispatch is released; running either inline would deadlock behind the held durability barrier"
)]
#[expect(
    clippy::too_many_lines,
    reason = "durability fence scenario; the held close, the entered-and-pending takeover observation and the durable postconditions describe one causal interleaving; splitting the phases into pass-through helpers would hide which state the fence answered from"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_waits_for_durability_before_reporting_closed() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dur9", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur9"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur9"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.stream_epoch.clone();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();

    // Hold durability dispatch, then send A's final-bearing close: its
    // intent lands (registry path), its write commits, but nothing is
    // released as durable.
    let guard = engine.test_hold_dispatch().await;
    let entered = engine.appends_enqueued();
    let a = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur9",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            br#"[{"fin":"a"}]"#,
        )
        .await
    });
    let mut a_claim = None;
    for _ in 0..300 {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref("dur9"));
        if let Ok(Some(d)) = state
            .registry
            .get(&state.deployment.raw_adapter_sref("dur9"))
            .await
            && let Some(sl) = d.sealing.clone()
        {
            a_claim = Some(sl);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let a_claim = a_claim.expect("A never published its intent");
    // A's append is enqueued and applied, never durable: A stays pending.
    held(&engine, entered + 1, &a, "A's final").await;

    // A's lease lapses; B begins a takeover with its own final.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("dur9"), |d| {
            if let Some(sl) = d.sealing.as_mut() {
                sl.claimed_ms -= crate::registry::SEAL_CLAIM_MS + 1_000;
                return true;
            }
            false
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur9"));
    let b_intent = crate::registry::SealIntent::Final {
        routing_key: String::new(),
        request_hash: "b-op-9".into(),
        final_committed: false,
    };
    let st2 = state.clone();
    let ep2 = epoch.clone();
    let b = tokio::spawn(async move {
        crate::product::claim_seal(
            &st2,
            &st2.deployment.raw_adapter_sref("dur9"),
            "b-op-9",
            &b_intent,
            &ep2,
        )
        .await
    });

    // While durability is held, the takeover MUST NOT have concluded
    // (its fence is queued behind A's write): the old claim stays
    // exactly as it was, same op, still unmarked.
    held(&engine, entered + 2, &b, "the takeover").await;
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur9"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "sealed before the record was durable");
    let sl = d.sealing.clone().expect("the claim vanished mid-takeover");
    assert_eq!(
        sl.operation_id, a_claim.operation_id,
        "the claim moved before durability"
    );
    assert!(
        sl.owes_final(),
        "the final was marked committed before it was durable: {:?}",
        sl.intent
    );

    // Release durability: A's close completes its own transition; B's
    // fence then reports closed=true and B completes/joins A's seal.
    drop(guard);
    let (st, _, body) = a.await.unwrap();
    assert!(
        st == 200 || st == 204,
        "A: {st} {}",
        String::from_utf8_lossy(&body)
    );
    let b_out = b.await.unwrap().unwrap();
    assert!(
        matches!(
            b_out,
            crate::product::EnterSeal::AlreadySealed | crate::product::EnterSeal::AlreadyCompleted
        ),
        "B did not defer to the close that won: {b_out:?}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur9"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed && d.sealing.is_none());
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/dur9", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    let fins = recs.iter().filter(|r| r.get("fin").is_some()).count();
    assert_eq!(fins, 1, "exactly one final: {recs:?}");
    engine_shutdown(&state).await;
}

/// Only the NEWEST reservation may install. Two takeovers can reserve
/// against the same lapsed claim; if the lower one installed, the live
/// claim's generation would sit below the higher fence.
#[expect(
    clippy::too_many_lines,
    reason = "takeover reservation scenario; two reservations against one lapsed claim and the install verdicts form one ordered sequence; separating them would hide the generation comparison being proved"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lower_takeover_reservation_cannot_install() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/race9", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    // A crashed final close leaves a lapsed claim.
    crate::failpoints::stop_after_seal_intent("race9");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/race9",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        br#"[{"fin":1}]"#,
    )
    .await;
    assert_eq!(st, 503);
    crate::failpoints::stop_after_seal_intent_off("race9");
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("race9"), |d| {
            if let Some(sl) = d.sealing.as_mut() {
                sl.claimed_ms -= crate::registry::SEAL_CLAIM_MS + 1_000;
                return true;
            }
            false
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("race9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("race9"))
        .await
        .unwrap()
        .unwrap();
    let epoch = d.stream_epoch.clone();
    let old = d.sealing.clone().unwrap();

    // Takeover A reserves (counter -> g_a) and fences, then STALLS.
    let mut g_a = 0;
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("race9"), |d| {
            d.seal_gen_counter += 1;
            g_a = d.seal_gen_counter;
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("race9"));
    let closed = crate::http::fence_segment_for_key(
        &state,
        &state.deployment.raw_adapter_sref("race9"),
        &epoch,
        "",
        g_a,
    )
    .await
    .unwrap();
    assert!(!closed);

    // Takeover B reserves the NEWER generation and fences.
    let mut g_b = 0;
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("race9"), |d| {
            d.seal_gen_counter += 1;
            g_b = d.seal_gen_counter;
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("race9"));
    assert!(g_b > g_a);
    let closed = crate::http::fence_segment_for_key(
        &state,
        &state.deployment.raw_adapter_sref("race9"),
        &epoch,
        "",
        g_b,
    )
    .await
    .unwrap();
    assert!(!closed);

    // A resumes and tries to install its LOWER reservation — through
    // the PRODUCTION install CAS. It must decline: the counter has
    // moved past it.
    let installed_a = crate::product::install_reserved_claim(
        &state,
        &state.deployment.raw_adapter_sref("race9"),
        &epoch,
        &old.operation_id,
        old.claim_generation,
        "takeover-a",
        &crate::registry::SealIntent::Empty,
        g_a,
    )
    .await
    .unwrap();
    assert!(
        !installed_a,
        "a lower reservation installed below a higher fence"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("race9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("race9"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.sealing.as_ref().map(|s| s.claim_generation),
        Some(old.claim_generation),
        "the old claim should still stand: {:?}",
        d.sealing
    );

    // The stream recovers through the REAL protocol: a plain :seal
    // takes the lapsed claim over at a generation ≥ every fence.
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(addr, "POST", "/v1/streams/race9:seal", &key, b"{}").await;
    assert!(
        st == 200 || st == 204,
        "recovery seal failed: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("race9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("race9"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed && d.sealing.is_none(), "not sealed after recovery");
    engine_shutdown(&state).await;
}

/// The fence survives handle eviction: it lives on the ENGINE, not on
/// the evictable StreamHandle, because a stale queued append carries
/// only the stream hash and would meet a freshly-reborn handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_survives_handle_eviction() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/evict9", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("evict9"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("evict9"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.stream_epoch.clone();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();

    // Raise the fence to 10, then evict every idle handle.
    let closed = crate::http::fence_segment_for_key(
        &state,
        &state.deployment.raw_adapter_sref("evict9"),
        &epoch,
        "",
        10,
    )
    .await
    .unwrap();
    assert!(!closed);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    engine.evict_idle_handles(std::time::Duration::from_millis(1), 0);

    // A stale generation-9 close arrives after the rebirth.
    let (st, b) = {
        // Drive it as a raw close whose claim generation is BELOW the
        // fence: enqueue directly, exactly like a queued survivor.
        let identity = desc.dynamic_segment_identity(seg.seg_id);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = crate::shard::AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash: identity,
            route,
            entries: vec![bytes::Bytes::from_static(b"{\"stale\":1}")],
            routing_key: String::new(),
            key_hash: crate::crypto::stream_hash(""),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey: [7u8; 32],
            ts_hint_ms: None,
            seq: None,
            bytes: 11,
            finish: crate::shard::AppendFinish::Close,
            seal_gen: Some(9),
            producer: None,
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            usage: crate::usage::counters(&identity),
            billing: None,
            resp: tx,
        };
        assert!(engine.try_enqueue(req).is_ok());
        match rx.await.unwrap() {
            Ok(_) => (200u16, String::new()),
            Err(e) => (409u16, format!("{e:?}")),
        }
    };
    assert!(
        b.is_empty() || b.contains("SealSuperseded"),
        "unexpected refusal: {b}"
    );
    assert_eq!(st, 409, "a stale close committed after handle eviction");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/evict9", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    assert_eq!(recs.len(), 1, "the stale write landed: {recs:?}");
    engine_shutdown(&state).await;
}

// ---------------------------------------------------------------
// Round 10: no successful answer whose truth depends on batch-local
// or applied state leaves before its durability barrier, and the
// product final append proves its whole execution token.
// ---------------------------------------------------------------

/// A producer DUPLICATE is a statement that the original write is
/// durable. While the durability dispatch is held, a duplicate of an
/// applied-but-unreleased write must stay pending — answering it
/// immediately let a retry observe "durably committed" for a record
/// whose group write could still fail. Same for the idempotent
/// close-only answer.
#[expect(
    clippy::disallowed_methods,
    reason = "durability barrier fixture; the original, its exact duplicate and both idempotent closes are joined after each release; the pending checks require the requests to run concurrently with the held dispatch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_successes_wait_for_durability() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dur10", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dur10"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dur10"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();

    // Hold durability dispatch; the ORIGINAL producer write commits
    // (applied) but is never released as durable.
    let guard = engine.test_hold_dispatch().await;
    let entered = engine.appends_enqueued();
    let ph = [
        ("content-type", "application/json"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let orig =
        tokio::spawn(
            async move { hreq(addr, "POST", "/v1/stream/dur10", &ph, br#"[{"n":1}]"#).await },
        );
    held(&engine, entered + 1, &orig, "the original").await;

    // The exact duplicate arrives. It must ALSO stay pending: its
    // truth is the original's durability.
    let dup = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur10",
            &[
                ("content-type", "application/json"),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"[{"n":1}]"#,
        )
        .await
    });
    held(&engine, entered + 2, &dup, "the duplicate").await;

    // Release: both answer, exactly once.
    drop(guard);
    let (st, _, _) = orig.await.unwrap();
    assert!(st == 200 || st == 204, "original: {st}");
    let (st, _, _) = dup.await.unwrap();
    assert_eq!(st, 204, "duplicate: {st}");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/dur10", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    assert_eq!(recs.len(), 2, "exactly once: {recs:?}");

    // Idempotent close-only: same contract, driven through the same
    // window. The ORIGINAL close is applied but held pre-durability;
    // its exact retry reads closed=true from applied state and must
    // stay pending behind the original's barrier.
    let guard = engine.test_hold_dispatch().await;
    let close1 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur10",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        )
        .await
    });
    held(&engine, entered + 3, &close1, "the close").await;
    let close2 = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/dur10",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            b"",
        )
        .await
    });
    held(&engine, entered + 4, &close2, "the idempotent close").await;
    drop(guard);
    let (st, _, _) = close1.await.unwrap();
    assert!(st == 200 || st == 204, "close: {st}");
    let (st, _, _) = close2.await.unwrap();
    assert!(st == 200 || st == 204, "idempotent close retry: {st}");
    engine_shutdown(&state).await;
}

/// A DEFINITIVE conflict is a statement about committed state. A
/// sequence-reuse verdict read from a producer row that another
/// request staged in the same (or an undispatched) group must wait
/// for that state's durability — answering early and losing the write
/// hands the client a permanent verdict about state that never
/// existed.
#[expect(
    clippy::disallowed_methods,
    reason = "definitive-conflict fixture; the original write and the reuse verdict are joined after dispatch is released; the conflict can only be observed pending while the judged state is held pre-durability"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_dependent_conflicts_wait_for_durability() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/conf11",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("conf11"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("conf11"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();

    // Original held pre-durability.
    let guard = engine.test_hold_dispatch().await;
    let entered = engine.appends_enqueued();
    let ph = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let orig = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/conf11/records",
            &ph,
            br#"{"x":1}"#,
        )
        .await
    });
    held(&engine, entered + 1, &orig, "the original").await;

    // Same tuple, DIFFERENT body: the product surface's reuse check
    // yields a definitive 409 — whose truth is the original's row.
    let reuse = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/conf11/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"{"x":"different"}"#,
        )
        .await
    });
    held(&engine, entered + 2, &reuse, "the reuse verdict").await;
    drop(guard);
    let (st, _, _) = orig.await.unwrap();
    assert_eq!(st, 200, "original");
    let (st, _, b) = reuse.await.unwrap();
    assert_eq!(
        st,
        409,
        "sequence reuse should be a conflict: {}",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}

/// The maintenance sweep must never weaken a fence: a fence has no
/// safe wall-clock expiry while the queue it protects has no maximum
/// residence. (The round-10 six-hour pruning was removed for exactly
/// this reason; this test pins the property against its return.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fence_outlives_the_maintenance_sweep() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/sweep11", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sweep11"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sweep11"))
        .await
        .unwrap()
        .unwrap();
    let epoch = desc.stream_epoch.clone();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let engine = state.engine_for(&route).await.unwrap();

    let closed = crate::http::fence_segment_for_key(
        &state,
        &state.deployment.raw_adapter_sref("sweep11"),
        &epoch,
        "",
        10,
    )
    .await
    .unwrap();
    assert!(!closed);
    // Run the SAME maintenance the production sweep runs — eviction —
    // then a stale generation-9 close must still be refused.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    engine.evict_idle_handles(std::time::Duration::from_millis(1), 0);
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = crate::shard::AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash: identity,
        route,
        entries: vec![bytes::Bytes::from_static(b"{\"stale\":1}")],
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: [7u8; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 11,
        finish: crate::shard::AppendFinish::Close,
        seal_gen: Some(9),
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        usage: crate::usage::counters(&identity),
        billing: None,
        resp: tx,
    };
    assert!(engine.try_enqueue(req).is_ok());
    let refused = matches!(
        rx.await.unwrap(),
        Err(crate::shard::AppendErr::SealSuperseded)
    );
    assert!(refused, "the sweep weakened the fence");
    engine_shutdown(&state).await;
}
