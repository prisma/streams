//! Seal recovery.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// A seal intent installed OVER a pending split deadlocks the
/// collection: phase B refuses to publish because the collection is
/// sealing, and the seal cannot finish because the transition never
/// clears. The intent CAS is the serialization point — it installs only
/// over a topologically quiet descriptor, resolving the transition
/// first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_never_installs_over_a_pending_transition() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/deadl",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for k in ["a", "b"] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/deadl/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!("{{\"k\":\"{k}\"}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // Park a split in phase B: pending is durable, the parent is sealed,
    // successors are not published.
    crate::failpoints::arm_scaler_before_publish("deadl");
    let split = {
        let st2 = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(
                &st2,
                &st2.deployment.raw_adapter_sref("deadl"),
                0,
                0x8000_0000_0000_0000,
            )
            .await
        })
    };
    let mut pending = false;
    for _ in 0..100 {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref("deadl"));
        if let Ok(Some(d)) = state
            .registry
            .get(&state.deployment.raw_adapter_sref("deadl"))
            .await
            && d.segments.as_ref().is_some_and(|m| m.pending.is_some())
        {
            pending = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(pending, "the split never published its intent");

    // A seal-with-final arriving now must NOT install its intent over
    // the pending transition. It either resolves it and seals, or it
    // refuses — never "sealing forever with pending work".
    let sealer = {
        let _st2 = state.clone();
        tokio::spawn(async move {
            preq(
                addr,
                "POST",
                "/v1/streams/deadl:seal",
                &[("prisma-encryption-key", PRISMA_KEY)],
                br#"{"final":{"done":true}}"#,
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    // While the split is still parked, the descriptor must never hold
    // BOTH a sealing intent and pending work.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("deadl"));
    if let Ok(Some(d)) = state
        .registry
        .get(&state.deployment.raw_adapter_sref("deadl"))
        .await
    {
        let both = d.sealing.is_some() && d.segments.as_ref().is_some_and(|m| m.pending.is_some());
        assert!(
            !both,
            "deadlock state: sealing over pending {:?}",
            d.segments
        );
    }
    crate::failpoints::release_scaler_before_publish("deadl");
    let _ = split.await;
    let (st, _, b) = sealer.await.unwrap();

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("deadl"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("deadl"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !(d.sealing.is_some() && d.segments.as_ref().is_some_and(|m| m.pending.is_some())),
        "ended deadlocked: sealing={:?} segments={:?}",
        d.sealing,
        d.segments
    );
    if st == 200 || st == 204 {
        // Success must mean terminal, not "in progress".
        assert!(
            d.sealed && d.sealing.is_none(),
            "reported success without reaching Sealed: {} {:?}",
            String::from_utf8_lossy(&b),
            d.sealing
        );
    } else {
        // A refusal is fine — but then it must be resumable, and a
        // retry after the transition settles must succeed.
        let (st2, _, b2) = preq(
            addr,
            "POST",
            "/v1/streams/deadl:seal",
            &key,
            br#"{"final":{"done":true}}"#,
        )
        .await;
        assert!(
            st2 == 200 || st2 == 204,
            "the seal did not become possible again: {st2} {}",
            String::from_utf8_lossy(&b2)
        );
    }
    engine_shutdown(&state).await;
}

/// A seal intent must never be published for a final record the append
/// path will always refuse — that leaves the collection sealing
/// forever, owing something undeliverable. Every deterministic refusal
/// is decided first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_impossible_final_never_publishes_an_intent() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/impossible",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let untouched = |label: &'static str| {
        let state = state.clone();
        async move {
            state
                .registry
                .invalidate(&state.deployment.raw_adapter_sref("impossible"));
            let d = state
                .registry
                .get(&state.deployment.raw_adapter_sref("impossible"))
                .await
                .unwrap()
                .unwrap();
            assert!(
                d.sealing.is_none() && !d.sealed,
                "{label} published a lifecycle intent: {:?}",
                d.sealing
            );
        }
    };

    // Routing key past the limit.
    let long = "k".repeat(2000);
    let body = format!(r#"{{"final":{{"x":1}},"routingKey":"{long}"}}"#);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/impossible:seal",
        &key,
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 400, "oversized routing key accepted");
    untouched("an oversized routing key").await;

    // Routing key that cannot travel as a header value.
    let body = "{\"final\":{\"x\":1},\"routingKey\":\"bad\\u0001key\"}";
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/impossible:seal",
        &key,
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 400, "control character in the routing key accepted");
    untouched("an untransmittable routing key").await;

    // A partial producer trio.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/impossible:seal",
        &[("prisma-encryption-key", PRISMA_KEY), ("producer-id", "p")],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 400, "partial producer headers accepted");
    untouched("a partial producer trio").await;

    // And a valid one still seals.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/impossible:seal",
        &key,
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    engine_shutdown(&state).await;
}

/// The SECOND crash boundary of a raw final close: the records are
/// durable and the segment is closed, but the transition was never
/// marked committed. An ordinary retry has to cross it. Without a
/// synthetic identity for a non-producer close, the retry reached the
/// committer's closed-stream check — which only forgives an empty
/// close-only — and was refused forever, leaving the collection Sealing
/// over records it already held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raw_final_close_resumes_after_its_records_are_durable() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawmark", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // Real close, real records, real segment close — stopped just before
    // the transition is marked committed.
    let closing = [
        ("content-type", "application/json"),
        ("stream-closed", "true"),
    ];
    let body = br#"[{"n":1},{"n":2}]"#;
    crate::failpoints::stop_before_mark_committed("rawmark");
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawmark", &closing, body).await;
    crate::failpoints::stop_before_mark_committed_off("rawmark");
    assert_eq!(st, 503, "the failpoint should have interrupted the close");

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawmark"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawmark"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "sealed despite the interruption");
    assert!(
        d.sealing.as_ref().is_some_and(|sl| sl.owes_final()),
        "the intent should still owe its final: {:?}",
        d.sealing
    );

    // The ordinary retry — same request, no producer, no private header
    // — must cross the boundary and finish the transition.
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/rawmark", &closing, body).await;
    assert!(
        st == 200 || st == 204,
        "the retry could not resume after the records were durable: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawmark"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawmark"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealed && d.sealing.is_none(),
        "not terminal: {:?}",
        d.sealing
    );

    // And the records landed EXACTLY once.
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/rawmark", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "records duplicated or lost: {recs:?}");
    engine_shutdown(&state).await;
}

/// A producer gap is a verdict about ORDERING, not about this request:
/// the predecessor may already be inside the server, so an exact retry
/// can still succeed and the intent must survive to be resumed.
///
/// It used to be treated as definitive, which lost the promised record
/// whenever the predecessor was merely late. Retaining it introduces
/// the opposite hazard — an operation that is simply gone holding the
/// collection Sealing forever — so recovery is a TIMEOUT: past
/// `SEAL_CLAIM_MS` another seal may take the claim over. Never a guess
/// about whether a verdict was terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordering_verdict_keeps_its_intent_until_the_claim_is_abandoned() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/refused",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // Complete, syntactically valid producer headers that the committer
    // rejects: a first sequence of 5 is a gap, and no retry can fix it.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/refused:seal",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", "p1"),
            ("producer-epoch", "1"),
            ("producer-seq", "5"),
        ],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(
        st >= 400,
        "a producer gap should have been refused: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("refused"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("refused"))
        .await
        .unwrap()
        .unwrap();
    let sl = d
        .sealing
        .clone()
        .expect("an ordering verdict tore down the intent its retry needs");
    assert!(sl.owes_final(), "the intent stopped owing its record");
    assert!(!d.sealed);

    // While that claim stands the collection IS sealing, so an ordinary
    // append is refused. That is the state machine working, not a brick
    // — the next assertion is what makes it recoverable.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/refused/records",
        &key,
        br#"{"ok":1}"#,
    )
    .await;
    assert_eq!(st, 409, "an ordinary append landed during Sealing: {st}");

    // A second seal cannot steal a live claim…
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/refused:seal",
        &key,
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 409, "a live claim was taken over");

    // …but an ABANDONED one is taken over, so no single bad request can
    // hold a collection Sealing forever.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("refused"), |d| {
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
        .invalidate(&state.deployment.raw_adapter_sref("refused"));
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/refused:seal",
        &key,
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "an abandoned claim was never released: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("refused"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("refused"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "the takeover did not reach Sealed");
    assert!(d.sealing.is_none(), "sealing state left behind");
    engine_shutdown(&state).await;
}

/// The two verdicts a RAW close can get from the committer, and the two
/// different things it must do with its intent.
///
/// A verdict about THIS REQUEST — a content-type mismatch, a malformed
/// body, a reused sequence — can never be changed by retrying it, so
/// the close takes its own intent back down rather than leave a
/// collection Sealing over a record nobody can deliver.
///
/// A verdict about ORDERING keeps the intent, because the predecessor
/// may still be admitted (see
/// `a_transient_producer_gap_does_not_lose_the_promised_record`), and
/// is released by the claim timeout instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_raw_close_does_not_strand_the_collection() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawrefuse", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A verdict about the request itself: the descriptor's content type
    // is JSON, and no retry of this close will change that.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/rawrefuse",
        &[("content-type", "text/plain"), ("stream-closed", "true")],
        b"not json",
    )
    .await;
    assert!(st >= 400, "a content-type mismatch was accepted: {st}");
    let _ = b;
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawrefuse"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawrefuse"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none() && !d.sealed,
        "a definitively refused raw close stranded the collection: {:?}",
        d.sealing
    );
    // The stream still works.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/rawrefuse", &ct, br#"[{"n":2}]"#).await;
    assert!(
        st == 200 || st == 204,
        "ordinary appends were bricked: {st}"
    );

    // An ordering verdict behaves the other way: sequence 5 from a
    // brand-new producer is a gap, and the intent stays for the retry.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawrefuse",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "5"),
        ],
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st >= 400, "a producer gap should be refused: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawrefuse"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawrefuse"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.as_ref().is_some_and(|sl| sl.owes_final()),
        "an ordering verdict discarded the intent its retry needs: {:?}",
        d.sealing
    );
    engine_shutdown(&state).await;
}

/// The reason an ordering verdict may not tear the intent down: the
/// missing predecessor can already be INSIDE the server, admitted while
/// the collection was still open and merely not yet committed. The old
/// behaviour answered the gap and discarded the intent in the same
/// breath, so the exact retry — the one request that could still
/// deliver the promised record — was refused as a new write and the
/// record was lost.
///
/// Driven through the real committer with the predecessor parked
/// between admission and enqueue, so the close genuinely observes the
/// gap and the predecessor genuinely lands afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transient_producer_gap_does_not_lose_the_promised_record() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/gapwin", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // The predecessor: admitted against an OPEN descriptor, then parked
    // before it reaches the queue.
    let before = crate::failpoints::parked(crate::failpoints::Fp::AppendBeforeEnqueue, "gapwin");
    crate::failpoints::park_append_before_enqueue("gapwin");
    let pre = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/gapwin",
            &[
                ("content-type", "application/json"),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            br#"[{"pre":1}]"#,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::AppendBeforeEnqueue, "gapwin") > before
        {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "the predecessor never reached the window");

    // The close carries sequence 1 and reaches the committer first, so
    // it observes a gap that is about to stop being true.
    let close_hdrs = [
        ("content-type", "application/json"),
        ("stream-closed", "true"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "1"),
    ];
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/gapwin",
        &close_hdrs,
        br#"[{"fin":1}]"#,
    )
    .await;
    assert_eq!(st, 409, "the close should have seen the gap: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("gapwin"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("gapwin"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.as_ref().is_some_and(|sl| sl.owes_final()),
        "the gap response cleared the final intent: {:?}",
        d.sealing
    );

    // Release the predecessor: it was admitted before the intent, so it
    // still lands.
    crate::failpoints::release_append_before_enqueue("gapwin");
    let (st, _, b) = pre.await.unwrap();
    assert!(
        st == 200 || st == 204,
        "the predecessor was lost: {st} {}",
        String::from_utf8_lossy(&b)
    );

    // Now the exact retry — same bytes, same producer coordination —
    // recognises its own intent and finishes the transition.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/gapwin",
        &close_hdrs,
        br#"[{"fin":1}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "the exact retry could not resume: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("gapwin"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("gapwin"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "the collection did not reach Sealed");
    assert!(d.sealing.is_none(), "sealing state left behind");

    let (_, _, b) = hreq(addr, "GET", "/v1/stream/gapwin", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    let finals = recs.iter().filter(|r| r.get("fin").is_some()).count();
    assert_eq!(
        finals, 1,
        "the promised record is not present exactly once: {recs:?}"
    );
    assert_eq!(recs.len(), 3, "records lost or duplicated: {recs:?}");
    engine_shutdown(&state).await;
}

/// AUDIT P0: sealing is a durable, resumable transition. A collection
/// stuck in Sealing (a crashed sealer) refuses ordinary appends on BOTH
/// surfaces, and the next seal request finishes the job. A sealed
/// descriptor is authoritative even if a segment engine has not
/// observed its close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_is_a_resumable_transition() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let pk = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealtx",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealtx/records",
        &pk,
        b"{\"n\":0}",
    )
    .await;
    assert_eq!(st, 200);

    // Plant a Sealing intent (a sealer that died before closing the
    // segments and publishing Sealed).
    state
        .registry
        .cas_update_retry(&state.deployment.raw_adapter_sref("sealtx"), |d| {
            d.seal_gen_counter += 1;
            d.sealing = Some(crate::registry::SealState {
                intent: crate::registry::SealIntent::Empty,
                operation_id: "op-1".into(),
                claimed_ms: crate::shard::now_ms(),
                claim_generation: d.seal_gen_counter,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sealtx"));

    // Ordinary appends are refused on both surfaces WHILE sealing.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealtx/records",
        &pk,
        b"{\"n\":1}",
    )
    .await;
    assert_eq!(st, 409, "product append during Sealing");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealtx",
        &[("content-type", "application/json")],
        br#"[{"n":2}]"#,
    )
    .await;
    assert_eq!(st, 409, "raw append during Sealing");
    // Metadata still reports NOT sealed: the transition has not
    // completed, so nothing may claim it has.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sealtx", &pk, b"").await;
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["sealed"], false, "Sealing is not Sealed");

    // Any seal request resumes and completes the transition.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealtx:seal", &pk, b"{}").await;
    assert!(st == 200 || st == 204);
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealtx"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "resumed seal publishes Sealed");
    assert!(d.sealing.is_none(), "the intent is cleared");
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealtx/records",
        &pk,
        b"{\"n\":3}",
    )
    .await;
    assert_eq!(st, 409, "sealed refuses appends");

    // A sealed DESCRIPTOR is authoritative even when a segment engine
    // has not observed the close (the audit's "physically open segment
    // accepted writes" case): plant sealed on a stream whose engine is
    // still open and prove both surfaces refuse.
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealauth",
        &pk,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .cas_update_retry(&state.deployment.raw_adapter_sref("sealauth"), |d| {
            d.sealed = true; // descriptor only; engines untouched
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sealauth"));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealauth/records",
        &pk,
        b"{\"n\":9}",
    )
    .await;
    assert_eq!(st, 409, "descriptor seal is authoritative (product)");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealauth",
        &[("content-type", "application/json")],
        br#"[{"n":9}]"#,
    )
    .await;
    assert_eq!(st, 409, "descriptor seal is authoritative (raw)");
    engine_shutdown(&state).await;
}
