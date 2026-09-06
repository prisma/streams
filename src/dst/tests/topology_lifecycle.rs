//! Topology lifecycle.

use super::fixture_auth::sr_rig;
use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// Scaling decisions and TTL slides are incarnation-fenced like every
/// other name-scoped background mutation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_scaler_and_ttl_decisions_decline_after_recreate() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/stale1", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale1"));
    let old = state
        .registry
        .get(&state.deployment.raw_adapter_sref("stale1"))
        .await
        .unwrap()
        .unwrap();

    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/stale1", &[], b"").await;
    assert!(st == 204 || st == 200);
    let (st, _, _) = hreq(
        addr,
        "PUT",
        "/v1/stream/stale1",
        &[("content-type", "application/json"), ("stream-ttl", "60")],
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 201, "recreate: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale1"));
    let fresh = state
        .registry
        .get(&state.deployment.raw_adapter_sref("stale1"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(fresh.stream_epoch, old.stream_epoch);
    let fresh_exp = fresh.expires_at_ms;

    // A split decision computed from the OLD incarnation's map: every
    // structural guard passes on the replacement (fresh single segment
    // spans the full range), so the epoch is the only thing refusing.
    let did = crate::scaler3::execute_split_fenced(
        &state,
        &state.deployment.raw_adapter_sref("stale1"),
        &old.stream_epoch,
        0,
        0x8000_0000_0000_0000,
    )
    .await;
    assert!(!did, "a stale split decision was applied");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale1"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("stale1"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.segments.as_ref().is_none_or(|m| m.pending.is_none()),
        "a stale decision installed pending topology: {:?}",
        d.segments.as_ref().map(|m| &m.pending)
    );

    // A TTL slide spawned against the old incarnation: it computes a
    // huge target from the OLD descriptor's ttl and must not extend
    // the replacement.
    let mut fake_old = fresh.to_persisted();
    fake_old.stream_epoch = old.stream_epoch.clone();
    fake_old.ttl_secs = Some(3_600);
    fake_old.expires_at_ms = Some(crate::shard::now_ms() + 1_000);
    let fake_old = crate::registry::StreamDesc::try_from(fake_old).unwrap();
    crate::http::touch_ttl(&state, &fake_old);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale1"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("stale1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        d.expires_at_ms, fresh_exp,
        "a stale TTL slide extended the replacement"
    );
    engine_shutdown(&state).await;
}

/// Heat is incarnation-scoped: a recreated stream starts COLD, a
/// decision carries the epoch of the traffic that justified it, and
/// run_seal's terminal proof refuses to bless a different operation's
/// terminal state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaler_heat_and_terminal_proof_are_incarnation_scoped() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/heat9", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("heat9"));
    let old = state
        .registry
        .get(&state.deployment.raw_adapter_sref("heat9"))
        .await
        .unwrap()
        .unwrap();
    // Feed heavy, PLURAL heat under incarnation A: many distinct keys
    // (a single dominant key trips hot-key suppression, not a split)
    // at rates far above the default hot thresholds.
    for i in 0..32 {
        let sg = old.resolve_segment(&format!("k{i}"));
        for _ in 0..8 {
            state
                .runtime
                .scaler
                .note_append(&old, &sg, 50_000_000, 10_000);
        }
    }
    // Recreate; ONE feed under B resets the sketch cold.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/heat9", &[], b"").await;
    assert!(st == 204 || st == 200);
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/heat9", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 201);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("heat9"));
    let fresh = state
        .registry
        .get(&state.deployment.raw_adapter_sref("heat9"))
        .await
        .unwrap()
        .unwrap();
    let fseg = fresh.resolve_segment("");
    // One COLD feed under the replacement resets the sketch. Then
    // hammer the evaluator long enough for any surviving heat to build
    // the hot streak a split decision needs — with the reset in place,
    // nothing ever forms; without it, the dead incarnation's traffic
    // drives a decision within a few passes.
    state.runtime.scaler.note_append(&fresh, &fseg, 10, 1);
    for _ in 0..12 {
        let (decisions, _) = state.runtime.scaler.evaluate();
        assert!(
            !decisions
                .iter()
                .any(|(n, _, _, _)| n.name().as_str() == "heat9"),
            "a recreated stream inherited the old incarnation's heat: {decisions:?}"
        );
    }

    // Terminal proof: seal B under its own operation, then ask
    // run_seal to bless it as a DIFFERENT operation's completion.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/heat9",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
        ],
        b"",
    )
    .await;
    assert!(st == 200 || st == 204);
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("heat9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("heat9"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed);
    let err = crate::product::run_seal(
        &state,
        &state.deployment.raw_adapter_sref("heat9"),
        Some("someone-else".into()),
        &fresh.stream_epoch,
        None,
    )
    .await;
    assert!(
        err.is_err(),
        "run_seal blessed a different operation's terminal state"
    );
    engine_shutdown(&state).await;
}

/// SEL-019: merge phase B refuses to publish under a sealing
/// collection — the merge stays pending and resumable, and completes
/// once the seal claim clears. (The split half has been pinned since
/// round 3; this is its mirror.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_phase_b_declines_under_sealing() {
    let _l = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sel019",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for k in ["a", "b"] {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/sel019/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", k),
            ],
            format!("{{\"k\":\"{k}\"}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    assert!(
        crate::scaler3::execute_split(
            &state,
            &state.deployment.raw_adapter_sref("sel019"),
            0,
            0x8000_0000_0000_0000
        )
        .await,
        "split failed"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel019"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel019"))
        .await
        .unwrap()
        .unwrap();
    let live: Vec<u32> = d
        .segments
        .as_ref()
        .unwrap()
        .segments
        .iter()
        .filter(|s| s.is_live())
        .map(|s| s.seg_id)
        .collect();
    assert_eq!(live.len(), 2);

    // Park after durable parent closure. A stale phase-B executor must
    // decline even when another legal descriptor transition owns the stream.
    let pbefore = crate::failpoints::parked(crate::failpoints::Fp::ScalerBeforePublish, "sel019");
    crate::failpoints::arm_scaler_before_publish("sel019");
    let (st2, a, b) = (state.clone(), live[0], live[1]);
    let merge = tokio::spawn(async move {
        crate::scaler3::execute_merge(&st2, &st2.deployment.raw_adapter_sref("sel019"), a, b).await
    });
    // Entered-proof: the merge REACHED phase B (parents sealed, parked
    // before publication) before the seal claim is planted.
    while crate::failpoints::parked(crate::failpoints::Fp::ScalerBeforePublish, "sel019") <= pbefore
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel019"));
    let pending = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel019"))
        .await
        .unwrap()
        .unwrap()
        .segments
        .as_ref()
        .unwrap()
        .pending
        .clone();
    // The serving-domain boundary now refuses the original impossible
    // fixture (simultaneous topology and seal claims) before it can persist.
    let invalid = state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("sel019"), |d| {
            d.seal_gen_counter += 1;
            d.sealing = Some(crate::registry::SealState {
                operation_id: "seal-x".into(),
                intent: crate::registry::SealIntent::Empty,
                claimed_ms: crate::shard::now_ms(),
                claim_generation: d.seal_gen_counter,
            });
            true
        })
        .await;
    assert!(
        invalid.is_err(),
        "overlapping claims crossed the validated boundary"
    );
    // A legal competing state removes the pending marker while claiming.
    // The fixture retains the original intent to test its subsequent retry;
    // production sealing always resumes topology before installing its claim.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("sel019"), |d| {
            d.segments.as_mut().unwrap().pending = None;
            d.seal_gen_counter += 1;
            d.sealing = Some(crate::registry::SealState {
                operation_id: "seal-x".into(),
                intent: crate::registry::SealIntent::Empty,
                claimed_ms: crate::shard::now_ms(),
                claim_generation: d.seal_gen_counter,
            });
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel019"));
    crate::failpoints::release_scaler_before_publish("sel019");
    let done = merge.await.unwrap();
    assert!(!done, "merge published under a sealing collection");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel019"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel019"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_some()
            && d.segments.as_ref().is_some_and(|m| m.pending.is_none()
                && m.segments
                    .iter()
                    .filter(|segment| segment.is_live())
                    .count()
                    == 2),
        "stale phase B changed the competing claim or published its successor"
    );

    // Clear the claim; the merge resumes to completion.
    state
        .registry
        .cas_update(&state.deployment.raw_adapter_sref("sel019"), |d| {
            d.sealing = None;
            d.segments.as_mut().unwrap().pending = pending.clone();
            true
        })
        .await
        .unwrap();
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel019"));
    assert!(
        crate::scaler3::resume(&state, &state.deployment.raw_adapter_sref("sel019")).await,
        "resume failed"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel019"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel019"))
        .await
        .unwrap()
        .unwrap();
    let live_after = d
        .segments
        .as_ref()
        .unwrap()
        .segments
        .iter()
        .filter(|s| s.is_live())
        .count();
    assert_eq!(
        live_after, 1,
        "merge did not complete after the seal cleared"
    );
    engine_shutdown(&state).await;
}

/// SEL-027: two finals with the SAME bytes but different producer
/// coordination are different operations. The second must not join
/// the first's claim, and its refusal must not tear that claim down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_finals_with_different_coordination_do_not_share_a_claim() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/sel027", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);

    let before = crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "sel027");
    crate::failpoints::park_close_before_enqueue("sel027");
    let body = br#"[{"fin":1}]"#;
    let a = tokio::spawn(async move {
        hreq(
            addr,
            "POST",
            "/v1/stream/sel027",
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
        if crate::failpoints::parked(crate::failpoints::Fp::CloseBeforeEnqueue, "sel027") > before {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "A never parked");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel027"));
    let a_claim = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel027"))
        .await
        .unwrap()
        .unwrap()
        .sealing
        .clone()
        .expect("A published no claim");

    // B: same bytes, DIFFERENT sequence — a different operation.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/sel027",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "1"),
        ],
        body,
    )
    .await;
    assert_eq!(st, 409, "B should conflict with A's live claim: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel027"));
    let now_claim = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel027"))
        .await
        .unwrap()
        .unwrap()
        .sealing
        .clone()
        .expect("B's refusal tore down A's claim");
    assert_eq!(now_claim.operation_id, a_claim.operation_id);
    assert!(now_claim.owes_final(), "A's promise was cleared");

    crate::failpoints::release_close_before_enqueue("sel027");
    let (st, _, b) = a.await.unwrap();
    assert!(
        st == 200 || st == 204,
        "A: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sel027"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sel027"))
        .await
        .unwrap()
        .unwrap();
    assert!(d.sealed);
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/sel027", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    let fins = recs.iter().filter(|r| r.get("fin").is_some()).count();
    assert_eq!(fins, 1, "exactly one final: {recs:?}");
    engine_shutdown(&state).await;
}

/// FRK-013: the child is deleted in the stamp-to-source-ref window.
/// The late reference install must not pin the source to a child that
/// no longer exists: the creator fails, the source keeps no children,
/// and the source hard-deletes cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_deleted_before_the_source_ref_cannot_pin_the_source() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/frk13src", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (_, h, _) = hreq(addr, "GET", "/v1/stream/frk13src", &[], b"").await;
    let boundary = h.get("stream-next-offset").cloned().unwrap_or_default();

    let before =
        crate::failpoints::parked(crate::failpoints::Fp::ForkBeforeSourceRef, "frk13child");
    crate::failpoints::park_fork_before_source_ref("frk13child");
    let b2 = boundary.clone();
    let creator = tokio::spawn(async move {
        hreq(
            addr,
            "PUT",
            "/v1/stream/frk13child",
            &[
                ("content-type", "application/json"),
                ("stream-forked-from", "frk13src"),
                ("stream-fork-offset", &b2),
            ],
            b"",
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::ForkBeforeSourceRef, "frk13child")
            > before
        {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "the creator never reached the stamp-to-ref window");

    // Delete the half-made child while the reference is uninstalled.
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk13child", &[], b"").await;
    assert!(
        st == 204 || st == 200 || st == 404 || st == 410,
        "delete: {st}"
    );

    crate::failpoints::release_fork_before_source_ref("frk13child");
    let (cs, _, cb) = creator.await.unwrap();
    assert!(
        cs != 200 && cs != 201,
        "creation reported success for a deleted child: {cs} {}",
        String::from_utf8_lossy(&cb)
    );
    // The source holds no reference to the dead child…
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk13src"));
    let src = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk13src"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        src.fork_children.is_empty(),
        "a dead child pinned the source: {:?}",
        src.fork_children
    );
    // …and hard-deletes cleanly (a leaked ref would soft-delete it).
    let (st, _, _) = hreq(addr, "DELETE", "/v1/stream/frk13src", &[], b"").await;
    assert!(st == 204 || st == 200, "source delete: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("frk13src"));
    let gone = state
        .registry
        .get(&state.deployment.raw_adapter_sref("frk13src"))
        .await
        .unwrap();
    assert!(
        gone.is_none() || gone.as_ref().is_some_and(|d| d.deleted && !d.soft_deleted),
        "the source was retained by a leaked reference: {gone:?}"
    );
    engine_shutdown(&state).await;
}

/// Regression for the scaler conversion (Søren review, blocker 3): a
/// NON-DEPLOYMENT project's pending split resumes under its own ref.
/// Pre-sweep, resume(name) resolved the deployment tenant and the
/// pending transition never completed for any other project.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nondeployment_pending_split_resumes() {
    let scopes = "streams.create streams.records.append streams.records.read \
                  streams.metadata.read";
    let (state, addr, tok) = sr_rig("proj-spl", "ws_spl", "c_spl", "spl-1", scopes).await;
    let auth = ("authorization", tok.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/orders",
        &[ekey, auth],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let bref = crate::tenant::ProjectId::new("proj-spl")
        .unwrap()
        .stream_ref("orders");
    // Execute a full split on B's OWN collection.
    let done = crate::scaler3::execute_split(&state, &bref, 0, 0x8000_0000_0000_0000).await;
    assert!(done, "a non-deployment project's split must execute");
    state.registry.invalidate(&bref);
    let d = state.registry.get(&bref).await.unwrap().unwrap();
    let map = d.segments.as_ref().expect("split materialized a map");
    assert!(map.pending.is_none(), "transition completed: {map:?}");
    let live = map.segments.iter().filter(|s| s.is_live()).count();
    assert_eq!(live, 2, "two live children after the split: {map:?}");
    engine_shutdown(&state).await;
}
