//! Seal fencing.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

// ---------------------------------------------------------------
// Round 8: a seal claim is a LEASE, not a timestamp. These drive the
// generation fence end to end: the committer refusing a superseded
// write, the takeover protocol consulting the fence's closed-report,
// renewal protecting an active owner, and the incarnation fence
// carried through mark and publication.
// ---------------------------------------------------------------

/// The committer refuses a superseded close BEFORE writing anything.
/// A's final-bearing close publishes its intent and parks before its
/// write enters the queue; its claim ages out; B takes the claim over
/// (fence sees an undecided, unclosed segment). A's write then arrives
/// carrying the old generation — it must land nothing and close
/// nothing, or the collection ends up physically closed behind a
/// descriptor that says somebody else's seal is still working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_superseded_close_cannot_write_or_close() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lease1", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A: final-bearing close, parked after its intent, before enqueue.
    let before = crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "lease1");
    crate::failpoints::park_close_before_enqueue("lease1");
    let a = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/lease1",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            br#"[{"fin":"a"}]"#,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "lease1") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "A never reached the window");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lease1"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lease1"))
        .await
        .unwrap()
        .unwrap();
    let a_claim = d.sealing.clone().expect("A published no intent");
    assert!(a_claim.owes_final());

    // A's lease lapses.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("lease1"), |d| {
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
        .invalidate(&state.deployment.raw_adapter_sref("lease1"));

    // B claims it through the real takeover protocol (reserve, fence,
    // closed-report, install) — but appends nothing yet: the point is
    // to catch A's write in flight between B's install and B's own.
    let epoch = d.stream_epoch.clone();
    let b_intent = crate::registry::SealIntent::Final {
        routing_key: String::new(),
        request_hash: "b-op".into(),
        final_committed: false,
    };
    let claim = crate::product::claim_seal(
        &state,
        &state.deployment.raw_adapter_sref("lease1"),
        "b-op",
        &b_intent,
        &epoch,
    )
    .await
    .unwrap();
    let b_gen = match claim {
        crate::product::EnterSeal::Installed { generation } => generation,
        other => panic!("takeover did not install: {other:?}"),
    };
    assert!(b_gen > a_claim.claim_generation, "no fresh generation");

    // A's write proceeds — and must be refused by the fence.
    crate::failpoints::release_close_before_enqueue("lease1");
    let (st, _, b) = a.await.unwrap();
    assert!(
        st >= 400,
        "a superseded close was accepted: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lease1"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lease1"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "the superseded close sealed the collection");
    assert_eq!(
        d.sealing.as_ref().map(|s| s.operation_id.as_str()),
        Some("b-op"),
        "B's claim did not survive A's fenced write: {:?}",
        d.sealing
    );
    let (_, _, body) = hreq(addr, "GET", "/v1/stream/lease1", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        recs,
        vec![serde_json::json!({"n": 0})],
        "the fenced close left records behind: {recs:?}"
    );
    // The segment is still writable under B's claim: B's own final
    // (the exact request whose identity is the claim) can finish. Here
    // we just prove the segment did not close: an is_owed_final resume
    // by B's op would be the production path.
    engine_shutdown(&state).await;
}

/// The other race outcome: the old close's write COMMITTED before the
/// takeover fenced. The fence's closed-report says so, and the
/// takeover must complete the OLD operation's transition instead of
/// stealing the claim — its record is durable and must not be
/// stranded behind an unmarked intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_completes_an_old_close_that_won() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/lease2", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A commits its final and its close, then dies before the mark.
    crate::failpoints::stop_before_mark_committed("lease2");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/lease2",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        br#"[{"fin":"a"}]"#,
    )
    .await;
    assert_eq!(st, 503, "the failpoint did not stop A");
    crate::failpoints::stop_before_mark_committed_off("lease2");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lease2"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lease2"))
        .await
        .unwrap()
        .unwrap();
    let a_op = d.sealing.clone().expect("A's claim is gone").operation_id;

    // A's lease lapses; B arrives with a DIFFERENT final.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("lease2"), |d| {
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
        .invalidate(&state.deployment.raw_adapter_sref("lease2"));
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/lease2",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        br#"[{"fin":"b"}]"#,
    )
    .await;
    // B cannot succeed as itself — the collection sealed under A.
    assert!(st >= 400, "B's different final reported success: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("lease2"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("lease2"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "the takeover did not complete A's transition");
    assert_eq!(
        d.seal_op.as_deref(),
        Some(a_op.as_str()),
        "sealed under the wrong operation"
    );
    assert!(d.sealing.is_none());
    let (_, _, body) = hreq(addr, "GET", "/v1/stream/lease2", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let a_fin = recs
        .iter()
        .filter(|r| r.get("fin") == Some(&serde_json::json!("a")))
        .count();
    let b_fin = recs
        .iter()
        .filter(|r| r.get("fin") == Some(&serde_json::json!("b")))
        .count();
    assert_eq!(
        (a_fin, b_fin),
        (1, 0),
        "wrong final record survived: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// An exact retry RENEWS its lease — fresh timestamp, fresh
/// generation — so an actively retrying owner can neither be taken
/// over by wall clock nor fenced out by an aborted reservation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exact_retry_renews_its_lease() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/renew", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("renew"));
    let epoch = state
        .registry
        .get(&state.deployment.raw_adapter_sref("renew"))
        .await
        .unwrap()
        .unwrap()
        .stream_epoch
        .clone();

    let intent = crate::registry::SealIntent::Final {
        routing_key: String::new(),
        request_hash: "op-r".into(),
        final_committed: false,
    };
    let g1 = match crate::product::claim_seal(
        &state,
        &state.deployment.raw_adapter_sref("renew"),
        "op-r",
        &intent,
        &epoch,
    )
    .await
    .unwrap()
    {
        crate::product::EnterSeal::Installed { generation } => generation,
        o => panic!("{o:?}"),
    };
    // Lease ages…
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("renew"), |d| {
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
        .invalidate(&state.deployment.raw_adapter_sref("renew"));
    // …but the owner retries: renewed, and with a HIGHER generation.
    let g2 = match crate::product::claim_seal(
        &state,
        &state.deployment.raw_adapter_sref("renew"),
        "op-r",
        &intent,
        &epoch,
    )
    .await
    .unwrap()
    {
        crate::product::EnterSeal::AlreadyOurs { generation } => generation,
        o => panic!("renewal did not recognise its own claim: {o:?}"),
    };
    assert!(g2 > g1, "renewal did not re-allocate: {g1} -> {g2}");
    // A rival arriving NOW finds a fresh lease and is refused.
    let rival = crate::registry::SealIntent::Final {
        routing_key: String::new(),
        request_hash: "op-x".into(),
        final_committed: false,
    };
    let out = crate::product::claim_seal(
        &state,
        &state.deployment.raw_adapter_sref("renew"),
        "op-x",
        &rival,
        &epoch,
    )
    .await
    .unwrap();
    assert!(
        matches!(out, crate::product::EnterSeal::Conflicting(_)),
        "a renewed lease was taken over: {out:?}"
    );
    engine_shutdown(&state).await;
}

/// mark_final_committed and run_seal are fenced to the incarnation the
/// close was issued against. A close that committed its records on
/// incarnation 1, then lost the window between ack and mark to a
/// delete+recreate, must NOT proceed to claim and seal the
/// replacement — the exact ABA the round-7 tests could not reach
/// because they stopped at claim installation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_mark_never_seals_a_later_incarnation() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/markaba", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A: close with content, parked BETWEEN its acknowledged write and
    // the mark.
    let before = crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeMark, "markaba");
    crate::failpoints::park_close_before_mark("markaba");
    let a = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/markaba",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ],
            br#"[{"fin":1}]"#,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeMark, "markaba") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "A never reached the ack-to-mark window");

    // The incarnation vanishes and the name is reborn.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/markaba", &[], b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/markaba", &ct, br#"[{"n":9}]"#).await;
    assert!(st == 200 || st == 201, "recreate: {st}");

    // A resumes: mark must fail on the incarnation fence, and the
    // handler must NOT continue into run_seal.
    crate::failpoints::release_close_before_mark("markaba");
    let (st, _, b) = a.await.unwrap();
    assert!(
        st >= 500,
        "a close from a dead incarnation reported success: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("markaba"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("markaba"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "the resumed close sealed the replacement");
    assert!(
        d.sealing.is_none(),
        "the resumed close claimed the replacement: {:?}",
        d.sealing
    );
    let (_, _, body) = hreq(addr, "GET", "/v1/stream/markaba", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        recs,
        vec![serde_json::json!({"n": 9})],
        "replacement content: {recs:?}"
    );
    engine_shutdown(&state).await;
}

/// The operation identity covers the request's OWN content type. A
/// close with the wrong type but the same body and coordination is a
/// DIFFERENT operation: it must not join the valid close's intent,
/// collect the deferred ct-mismatch verdict, and tear down an intent
/// it never owned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_content_type_close_cannot_join_the_valid_intent() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/ctid", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A: valid close, parked pre-enqueue with a live intent.
    let before = crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "ctid");
    crate::failpoints::park_close_before_enqueue("ctid");
    let body = br#"[{"fin":1}]"#;
    let a = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/ctid",
            &[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
                ("producer-id", "p"),
                ("producer-epoch", "1"),
                ("producer-seq", "0"),
            ],
            body,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "ctid") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "A never parked");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("ctid"));
    let a_claim = state
        .registry
        .get(&state.deployment.raw_adapter_sref("ctid"))
        .await
        .unwrap()
        .unwrap()
        .sealing
        .clone()
        .expect("A published no intent");

    // B: same body, same producer trio, WRONG content type. Its
    // ct-mismatch verdict is deferred to the committer BECAUSE it
    // carries a producer — which is exactly why sharing an identity
    // was fatal: the deferred verdict is definitive.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/ctid",
        &[
            ("content-type", "text/plain"),
            ("stream-closed", "true"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        body,
    )
    .await;
    assert!(st >= 400, "the wrong-type close was accepted: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("ctid"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("ctid"))
        .await
        .unwrap()
        .unwrap();
    let now_claim = d.sealing.as_ref().expect("B tore down A's intent");
    assert_eq!(
        now_claim.operation_id, a_claim.operation_id,
        "the intent no longer belongs to A"
    );
    assert!(now_claim.owes_final(), "A's promise was cleared");

    // A completes untouched.
    crate::failpoints::release_close_before_enqueue("ctid");
    let (st, _, b) = a.await.unwrap();
    assert!(
        st == 200 || st == 204,
        "the valid close could not finish: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("ctid"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("ctid"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "A did not seal");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/ctid", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    assert_eq!(recs.len(), 2, "the promised record is missing: {recs:?}");
    engine_shutdown(&state).await;
}

/// A plain `:seal` really can recover a collection whose final-bearing
/// sealer died: the lapsed claim goes through the takeover protocol
/// (the old write is fenced first), and the old operation's late retry
/// finds the collection sealed by someone else — never resurrected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_only_takes_over_an_abandoned_final_claim() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rescue", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    // A crashed between intent and write.
    crate::failpoints::stop_after_seal_intent("rescue");
    let body = br#"[{"fin":1}]"#;
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rescue",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert_eq!(st, 503);
    crate::failpoints::stop_after_seal_intent_off("rescue");

    // While the lease is live, a plain :seal is refused…
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(addr, "POST", "/v1/streams/rescue:seal", &key, b"{}").await;
    assert_eq!(st, 409, "a live final claim was sealed over");

    // …and once it lapses, the SAME plain :seal recovers the stream.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("rescue"), |d| {
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
        .invalidate(&state.deployment.raw_adapter_sref("rescue"));
    let (st, _, b) = preq(addr, "POST", "/v1/streams/rescue:seal", &key, b"{}").await;
    assert!(
        st == 200 || st == 204,
        "seal-only could not recover an abandoned claim: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rescue"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rescue"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed && d.sealing.is_none());

    // The dead operation's exact retry is told the truth: sealed by
    // someone else, its record never landed.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rescue",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert!(st >= 400, "the dead sealer's retry was resurrected: {st}");
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/rescue", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    assert_eq!(recs, vec![serde_json::json!({"n": 0})], "{recs:?}");
    engine_shutdown(&state).await;
}
