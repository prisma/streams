//! Seal coordination.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// Seal-with-final through the SDK's own shape: NO caller producer
/// headers, so the seal relies on the server's synthetic producer
/// identity for idempotence. That identity travels as a request header
/// into the shared committer path, and a header value may not contain
/// control bytes — a NUL-delimited id silently failed to insert, the
/// final append lost its producer, and the just-entered Sealing state
/// then refused the very record it was sealing with (live 409
/// `sealed`). The sibling test above passes its own producer headers
/// and never reaches this branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_seal_final_needs_no_caller_producer() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealnp",
        &[("prisma-encryption-key", PRISMA_KEY)],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let hdrs = vec![("prisma-encryption-key", PRISMA_KEY)];
    let body = br#"{"final":{"type":"done"},"routingKey":"c1"}"#;
    let (st, _, b) = preq(addr, "POST", "/v1/streams/sealnp:seal", &hdrs, body).await;
    assert!(
        st == 200 || st == 204,
        "seal without caller producer: {} {}",
        st,
        String::from_utf8_lossy(&b)
    );
    // Replay: the synthetic identity dedups the final append, so the
    // record lands exactly once and the seal stays idempotent.
    let (st, _, _) = preq(addr, "POST", "/v1/streams/sealnp:seal", &hdrs, body).await;
    assert!(st == 200 || st == 204);
    let (st, h, b) = preq(
        addr,
        "GET",
        "/v1/streams/sealnp/records?routingKey=c1",
        &hdrs,
        b"",
    )
    .await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 1, "final record exactly once: {recs:?}");
    assert_eq!(recs[0]["type"], "done");
    assert_eq!(h.get("prisma-sealed").map(String::as_str), Some("true"));
    engine_shutdown(&state).await;
}

/// A seal that promised a final record owns the transition until that
/// record is durable. A plain `:seal` arriving after a crashed
/// seal-with-final used to close every segment and publish Sealed —
/// dropping the final record permanently, with both requests reporting
/// success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_plain_seal_cannot_finish_someone_elses_final() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealint",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // A seal-with-final that published its intent and then died.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("sealint"), |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: crate::product::seal_op_id_full(
                    &serde_json::json!({"done": true}),
                    "",
                    None,
                ),
                intent: crate::registry::SealIntent::Final {
                    routing_key: String::new(),
                    request_hash: crate::product::seal_op_id_full(
                        &serde_json::json!({"done": true}),
                        "",
                        None,
                    ),
                    final_committed: false,
                },
                claimed_ms: crate::shard::now_ms(),
                claim_generation: 1,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sealint"));

    // A plain seal must NOT complete it.
    let (st, _, b) = preq(addr, "POST", "/v1/streams/sealint:seal", &key, b"").await;
    assert_eq!(st, 409, "{}", String::from_utf8_lossy(&b));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealint"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "the collection must not be sealed yet");
    assert!(d.sealing.is_some(), "the intent survives the refusal");

    // A raw close must not either — it would close the segment first.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sealint",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        b"",
    )
    .await;
    assert!(
        st == 409 || st == 503,
        "raw close during a final seal: {st}"
    );
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealint"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed);

    // The owning operation finishes it: same final, same routing key.
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/sealint:seal",
        &key,
        br#"{"final":{"done":true}}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealint"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed && d.sealing.is_none(), "seal completes: {d:?}");
    engine_shutdown(&state).await;
}

/// Producer requests are admitted during Sealing so a RETRY can be
/// recognised and answered with its original result. That let a
/// genuinely new sequence through as well: the descriptor said Sealing
/// while a novel producer write landed. The refusal now rides with the
/// request and the committer applies it after duplicate detection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producers_cannot_write_new_records_while_sealing() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/prodseal", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let ph = |seq: u32| {
        vec![
            ("content-type", "application/json"),
            ("producer-id", "p1"),
            ("producer-epoch", "1"),
            (
                "producer-seq",
                Box::leak(seq.to_string().into_boxed_str()) as &str,
            ),
        ]
    };
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/prodseal", &ph(0), br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 204, "first producer append: {st}");

    // Enter Sealing without finishing (as a crashed seal would leave it).
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("prodseal"), |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: String::new(),
                intent: crate::registry::SealIntent::Empty,
                claimed_ms: crate::shard::now_ms(),
                claim_generation: 1,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("prodseal"));

    // The RETRY of seq 0 still dedups to success…
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/prodseal", &ph(0), br#"[{"n":0}]"#).await;
    assert!(
        st == 200 || st == 204,
        "a duplicate must still answer: {st}"
    );
    // …but a NEW sequence is refused.
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/prodseal", &ph(1), br#"[{"n":1}]"#).await;
    assert_eq!(
        st,
        409,
        "a new producer sequence landed during Sealing: {}",
        String::from_utf8_lossy(&b)
    );
    // And nothing was written.
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/prodseal", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap_or_default();
    assert_eq!(recs.len(), 1, "records after a refused write: {recs:?}");
    engine_shutdown(&state).await;
}

/// The scaler must not publish a new writable child while a seal is in
/// flight: the seal snapshots the live segments, so a successor created
/// after that snapshot outlives the seal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_transitions_are_fenced_by_sealing() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/fenced",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("fenced"), |d| {
            d.sealing = Some(crate::registry::SealState {
                operation_id: String::new(),
                intent: crate::registry::SealIntent::Empty,
                claimed_ms: crate::shard::now_ms(),
                claim_generation: 1,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fenced"));
    assert!(
        !crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("fenced"),
            0,
            0x8000_0000_0000_0000
        )
        .await,
        "a split started under Sealing"
    );
    // …and once sealed, still refused.
    {
        let ep = state
            .registry
            .get(&state.deployment.raw_adapter_sref("fenced"))
            .await
            .unwrap()
            .unwrap()
            .stream_epoch
            .clone();
        crate::product::run_seal(
            &state,
            &state.deployment.raw_adapter_sref("fenced"),
            None,
            &ep,
            None,
        )
        .await
        .unwrap();
    }
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("fenced"));
    assert!(
        !crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("fenced"),
            0,
            0x8000_0000_0000_0000
        )
        .await,
        "a split started under Sealed"
    );
    engine_shutdown(&state).await;
}

/// The exact interleaving the round-3 audit specified: a split that has
/// already published its intent and sealed the parent, parked before
/// publishing successors, while the collection seals underneath it.
/// Fencing only the START of a transition left this open — phase B
/// would resume and publish live children under a Sealed collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_split_cannot_publish_under_a_sealed_collection() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/parked",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for k in ["a", "b", "c", "d"] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/parked/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!("{{\"k\":\"{k}\"}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }

    // Park every resume between the parent seal and the successor CAS,
    // then start a split: it publishes pending, seals the parent, waits.
    crate::failpoints::arm_scaler_before_publish("parked");
    let split = {
        let st2 = state.clone();
        tokio::spawn(async move {
            crate::scaler3::execute_split(
                &st2,
                &st2.deployment.raw_adapter_sref("parked"),
                0,
                0x8000_0000_0000_0000,
            )
            .await
        })
    };
    // Wait for the intent to become durable.
    let mut pending = false;
    for _ in 0..100 {
        state
            .registry
            .invalidate(&state.deployment.raw_adapter_sref("parked"));
        if let Ok(Some(d)) = state
            .registry
            .get(&state.deployment.raw_adapter_sref("parked"))
            .await
            && d.segments.as_ref().is_some_and(|m| m.pending.is_some())
        {
            pending = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(pending, "the split never published its intent");

    // Seal the collection while the split is parked. The seal resolves
    // the transition first rather than snapshotting around it.
    let sealer = {
        let st2 = state.clone();
        tokio::spawn(async move {
            let ep = st2
                .registry
                .get(&st2.deployment.raw_adapter_sref("parked"))
                .await
                .unwrap()
                .unwrap()
                .stream_epoch
                .clone();
            crate::product::run_seal(
                &st2,
                &st2.deployment.raw_adapter_sref("parked"),
                None,
                &ep,
                None,
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    crate::failpoints::release_scaler_before_publish("parked");
    let _ = split.await;
    let seal_result = sealer.await.unwrap();

    // Whatever order they settled in, the end state must be coherent:
    // if the collection is sealed, nothing may be live and unclosed, and
    // no transition may be left dangling.
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("parked"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("parked"))
        .await
        .unwrap()
        .unwrap();
    if d.sealed {
        assert!(
            d.segments.as_ref().is_none_or(|m| m.pending.is_none()),
            "sealed with a transition still pending: {:?}",
            d.segments
        );
        // The invariant is behavioural, not representational: whatever
        // segments exist, NOTHING may write. Keys are checked across the
        // whole keyspace, since a successor published by the parked
        // split would own half of it.
        for k in ["late", "a", "b", "c", "d", "zz", "q7"] {
            let (st, _, b) = preq(
                addr,
                "POST",
                "/v1/streams/parked/records",
                &[
                    ("prisma-encryption-key", PRISMA_KEY),
                    ("prisma-routing-key", k),
                ],
                br#"{"late":1}"#,
            )
            .await;
            assert_eq!(
                st,
                409,
                "append on key {k} accepted after Sealed: {}",
                String::from_utf8_lossy(&b)
            );
        }
        // The raw (default-key) surface agrees.
        let (st, _, _) = hreq(
            addr,
            "POST",
            "/v1/stream/parked",
            &[("content-type", "application/json")],
            br#"[{"late":1}]"#,
        )
        .await;
        assert_eq!(st, 409, "raw append accepted after Sealed");
    } else {
        // The seal declined because the transition was in flight: that
        // is the other legal outcome, and it must say so.
        assert!(
            seal_result.is_err(),
            "unsealed but the seal reported success"
        );
    }
    engine_shutdown(&state).await;
}

/// Three seal-request defects the round-3 audit found by reading the
/// types: a PRESENT `null` final was indistinguishable from an absent
/// one, the operation identity concatenated record and routing key
/// without lengths, and the intent went durable before the key was
/// checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_requests_are_identified_and_validated_exactly() {
    const OTHER_KEY: &str = "CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=";
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let mk = |n: &str| {
        let n = n.to_string();
        async move {
            let (st, _, _) = preq(
                addr,
                "PUT",
                &format!("/v1/streams/{n}"),
                &[("prisma-encryption-key", PRISMA_KEY)],
                br#"{"format":{"kind":"json"}}"#,
            )
            .await;
            assert_eq!(st, 201);
        }
    };

    // 1. `{"final": null}` seals WITH a null record, not without one.
    mk("sealnull").await;
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/sealnull:seal",
        &key,
        br#"{"final":null}"#,
    )
    .await;
    assert!(st == 200 || st == 204, "{}", String::from_utf8_lossy(&b));
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sealnull/records", &key, b"").await;
    assert_eq!(st, 200);
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(
        recs,
        vec![serde_json::Value::Null],
        "a present null final was dropped"
    );

    // 2. Operation identity: the audit's collision pair. Concatenating
    //    record+key made {1,"23"} and {12,"3"} hash the same "123".
    let a = crate::product::seal_op_id_full(&serde_json::json!(1), "23", None);
    let b2 = crate::product::seal_op_id_full(&serde_json::json!(12), "3", None);
    assert_ne!(a, b2, "distinct seal requests share an operation id");
    // …and two attempts that differ ONLY in producer coordination are
    // different operations, so one cannot tear down the other's intent.
    let p1 = crate::product::seal_op_id_full(&serde_json::json!(1), "k", Some(("p", "1", "0")));
    let p2 = crate::product::seal_op_id_full(&serde_json::json!(1), "k", Some(("p", "1", "5")));
    let none = crate::product::seal_op_id_full(&serde_json::json!(1), "k", None);
    assert_ne!(p1, p2, "producer sequence is not part of the seal identity");
    assert_ne!(
        p1, none,
        "producer presence is not part of the seal identity"
    );

    // 3. A wrong key must not move the lifecycle at all.
    mk("sealkey").await;
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealkey:seal",
        &[("prisma-encryption-key", OTHER_KEY)],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 403, "wrong key accepted");
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealkey"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none() && !d.sealed,
        "a refused seal published an intent: {:?}",
        d.sealing
    );
    // …and a missing key likewise.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealkey:seal",
        &[],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert_eq!(st, 400);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sealkey"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealkey"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealing.is_none() && !d.sealed);
    // The right key still works afterwards.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sealkey:seal",
        &key,
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    engine_shutdown(&state).await;
}

/// A raw close that carries content promises those records. Publishing
/// an EMPTY seal intent for it meant a crash after the intent let a
/// later close-only finish the seal without them — and a request that
/// failed validation left the collection sealing forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raw_close_with_content_owes_its_records() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawfin", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // A malformed close must not touch the lifecycle.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawfin",
        &[("content-type", "text/plain"), ("stream-closed", "true")],
        b"not json",
    )
    .await;
    assert!(st >= 400, "malformed close accepted: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawfin"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawfin"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none() && !d.sealed,
        "a refused close published an intent: {:?}",
        d.sealing
    );

    // A valid close WITH content: the records land and the collection
    // seals as one operation.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/rawfin",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        br#"[{"n":1},{"n":2}]"#,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "close with content: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawfin"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawfin"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "the collection did not seal");
    assert!(d.sealing.is_none(), "sealing state left behind");
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/rawfin", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "the close's records are missing: {recs:?}");
    engine_shutdown(&state).await;
}

/// A raw close that carries content must survive a crash BETWEEN the
/// lifecycle intent and the append — recoverable by an ordinary retry
/// of the same request, with no private headers and no producer opt-in.
/// The old design recognised the owed record only by an `x-seal-final`
/// header that the product path inserted internally, so a real client's
/// retry was rejected as a new write and the collection stayed stuck
/// owing a record nobody could deliver. That header was also accepted
/// from the wire, which let any caller smuggle a record into a sealing
/// collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_raw_final_close_is_resumed_by_an_ordinary_retry() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/rawcrash", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    // The REAL crash point, driven through the real code: the close
    // publishes its Final intent and the process dies before the
    // append. Planting a hand-built SealState here would prove only
    // that the resume path reads a struct the test wrote — not that the
    // server writes the same identity the retry recomputes, which is
    // the whole contract under test.
    let body = br#"[{"n":1},{"n":2}]"#;
    crate::failpoints::stop_after_seal_intent("rawcrash");
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert_eq!(st, 503, "the failpoint did not stop the close");
    crate::failpoints::stop_after_seal_intent_off("rawcrash");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawcrash"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawcrash"))
        .await
        .unwrap()
        .unwrap();
    let sl = d.sealing.clone().expect("no intent survived the crash");
    assert!(
        sl.owes_final(),
        "the crash left no owed final: {:?}",
        sl.intent
    );
    let op = sl.operation_id.clone();
    assert!(!d.sealed, "the collection sealed without its record");

    // An UNRELATED write is refused while the collection owes its final.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &ct,
        br#"[{"other":1}]"#,
    )
    .await;
    assert_eq!(st, 409, "an unrelated write landed during Sealing");

    // …and so is a caller trying to assert the private authorization.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
            ("x-seal-final", op.as_str()),
        ],
        br#"[{"smuggled":1}]"#,
    )
    .await;
    assert_eq!(st, 400, "x-seal-final was accepted from the wire");

    // The ORDINARY retry — same request, no special headers — resumes.
    let (st, _, b) = hreq(
        addr,
        "POST",
        "/v1/stream/rawcrash",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        body,
    )
    .await;
    assert!(
        st == 200 || st == 204,
        "the exact retry could not resume: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("rawcrash"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("rawcrash"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed, "the collection did not reach Sealed");
    assert!(d.sealing.is_none(), "sealing state left behind");
    let (_, _, b) = hreq(addr, "GET", "/v1/stream/rawcrash", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&b).unwrap();
    assert_eq!(recs.len(), 3, "the promised records are missing: {recs:?}");
    engine_shutdown(&state).await;
}
