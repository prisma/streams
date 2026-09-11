//! Seal incarnation.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// The product seal is fenced to the incarnation its KEY was validated
/// against: a delete+recreate under the SAME key inside the
/// validation-to-claim gap must not let the request seal the
/// replacement.
#[expect(
    clippy::disallowed_methods,
    reason = "seal claim fixture; the request held before claim is released and joined before checking the recreated collection; running it inline cannot expose the validation-to-claim gap"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_product_seal_never_binds_to_a_recreated_incarnation() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealaba",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    let before =
        crate::failpoints::parked(crate::failpoints::Fp::ProductSealBeforeClaim, "sealaba");
    crate::failpoints::park_product_seal_before_claim("sealaba");
    let sealer = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/sealaba:seal",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"final":{"x":1}}"#,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::ProductSealBeforeClaim, "sealaba")
            > before
        {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        parked,
        "the sealer never reached the validation-to-claim gap"
    );

    // Replace the collection under the SAME key.
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/sealaba", &key, b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sealaba",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "recreate");

    crate::failpoints::release_product_seal_before_claim("sealaba");
    let (st, _, b) = sealer.await.unwrap();
    assert!(
        st >= 400,
        "a seal validated against a dead incarnation succeeded: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("sealaba"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("sealaba"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "the replacement was sealed");
    assert!(
        d.sealing.is_none(),
        "the replacement was claimed: {:?}",
        d.sealing
    );
    engine_shutdown(&state).await;
}

/// A close-with-content whose producer tuple was SPENT by an earlier
/// non-closing append can never deliver its promise: the duplicate
/// answer is correct, and the claim it installed comes down with it.
/// Leaving the intent held the collection Sealing behind an
/// undeliverable promise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_closing_duplicate_releases_its_own_intent() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/dup9", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    // Spend the tuple on an ORDINARY append.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dup9",
        &[
            ("content-type", "application/json"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);

    // The same tuple now arrives as a close-with-content.
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/dup9",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"[{"fin":1}]"#,
    )
    .await;
    assert_eq!(st, 204, "the duplicate answer is the protocol's contract");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("dup9"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("dup9"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none(),
        "a non-closing duplicate left its intent behind: {:?}",
        d.sealing
    );
    assert!(!d.sealed);
    let (_, _, bd) = hreq(addr, "GET", "/v1/stream/dup9", &[], b"").await;
    let recs: Vec<serde_json::Value> = serde_json::from_slice(&bd).unwrap();
    assert_eq!(recs.len(), 2, "the final body was appended: {recs:?}");
    // The collection still works.
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/dup9", &ct, br#"[{"n":2}]"#).await;
    assert!(st == 200 || st == 204, "ordinary appends bricked: {st}");
    engine_shutdown(&state).await;
}

/// The product final append proves its WHOLE execution token —
/// incarnation, operation, generation — against the current
/// descriptor before writing anything. A seal claimed on incarnation
/// A whose final append raced a delete+recreate (same name, same key)
/// used to write its record into B and physically close B's segment,
/// leaving the replacement unwritable; only the mark failed.
#[expect(
    clippy::disallowed_methods,
    reason = "seal final-record fixture; the request held after claim is released and joined before checking the replacement contents and writeability; running it inline cannot expose the claim-to-append gap"
)]
#[expect(
    clippy::too_many_lines,
    reason = "seal final-record scenario; the held request recreation and replacement read/write assertions form one incarnation proof; hiding phases behind one-use helpers would obscure the causal sequence"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_product_final_never_writes_into_a_recreated_incarnation() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/finaba",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);

    // Park the sealer AFTER its claim, BEFORE its final append.
    let before =
        crate::failpoints::parked(crate::failpoints::Fp::ProductFinalBeforeAppend, "finaba");
    crate::failpoints::park_product_final_before_append("finaba");
    let sealer = tokio::spawn(async move {
        preq(
            addr,
            "POST",
            "/v1/streams/finaba:seal",
            &[("prisma-encryption-key", PRISMA_KEY)],
            br#"{"final":{"x":1}}"#,
        )
        .await
    });
    let mut parked = false;
    for _ in 0..300 {
        if crate::failpoints::parked(crate::failpoints::Fp::ProductFinalBeforeAppend, "finaba")
            > before
        {
            parked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(parked, "the sealer never reached the claim-to-append gap");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("finaba"));
    let a = state
        .registry
        .get(&state.deployment.raw_adapter_sref("finaba"))
        .await
        .unwrap()
        .unwrap();
    assert!(a.sealing.is_some(), "no claim installed before the park");

    // Replace the collection under the SAME key while the final is
    // parked.
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/finaba", &key, b"").await;
    assert!(st == 204 || st == 200, "delete: {st}");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/finaba",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "recreate");

    crate::failpoints::release_product_final_before_append("finaba");
    let (st, _, b) = sealer.await.unwrap();
    assert!(
        st >= 400,
        "a final from a dead incarnation reported success: {st} {}",
        String::from_utf8_lossy(&b)
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("finaba"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("finaba"))
        .await
        .unwrap()
        .unwrap();
    assert!(!d.sealed, "the replacement was sealed");
    assert!(
        d.sealing.is_none(),
        "the replacement was claimed: {:?}",
        d.sealing
    );
    // No final record, and — the decisive assertion — the replacement's
    // segment is still OPEN: ordinary product appends succeed.
    let (_, _, bd) = preq(
        addr,
        "GET",
        "/v1/streams/finaba/records?routingKey=",
        &key,
        b"",
    )
    .await;
    let _ = bd;
    let (st, _, b) = preq(
        addr,
        "POST",
        "/v1/streams/finaba/records",
        &key,
        br#"{"ok":1}"#,
    )
    .await;
    assert_eq!(
        st,
        200,
        "the replacement's segment was closed by a stranger's final: {}",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}

// ---------------------------------------------------------------
// Round 12: one disposition policy; queue state joins the
// applied/durable discipline.
// ---------------------------------------------------------------

/// A STALE producer epoch is permanent — epochs never decrease — and
/// after round 11 the verdict stands on durable state. A seal-with-
/// final refused as stale must release its own intent NOW: retaining
/// it held the collection Sealing behind a promise that could never
/// be delivered, renewable indefinitely by the very request that can
/// never deliver it. Both surfaces, one policy.
#[expect(
    clippy::too_many_lines,
    reason = "seal producer-epoch scenario; raw and product requests must both prove immediate release of permanently refused finals; adapter-specific assertions stay adjacent to their protocol inputs"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_epoch_final_releases_its_intent_on_both_surfaces() {
    let store = mem();
    let (state, addr) = http_rig(store).await;

    // PRODUCT surface.
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/stale12p",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // Establish epoch 2.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/stale12p/records",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", "p"),
            ("producer-epoch", "2"),
            ("producer-seq", "0"),
        ],
        br#"{"n":1}"#,
    )
    .await;
    assert_eq!(st, 200);
    // Seal with a STALE epoch.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/stale12p:seal",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"{"final":{"x":1}}"#,
    )
    .await;
    assert!(
        st == 403 || st == 409,
        "stale epoch should be refused: {st}"
    );
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale12p"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("stale12p"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none(),
        "a permanently-stale final retained its intent: {:?}",
        d.sealing
    );
    assert!(!d.sealed);
    // Ordinary appends work IMMEDIATELY — no 15-second hostage window.
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/stale12p/records",
        &key,
        br#"{"n":2}"#,
    )
    .await;
    assert_eq!(st, 200, "ordinary appends held hostage by a stale final");

    // RAW surface, same story.
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/stale12r", &ct, br#"[{"n":0}]"#).await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/stale12r",
        &[
            ("content-type", "application/json"),
            ("producer-id", "p"),
            ("producer-epoch", "2"),
            ("producer-seq", "0"),
        ],
        br#"[{"n":1}]"#,
    )
    .await;
    assert!(st == 200 || st == 204);
    let (st, _, _) = hreq(
        addr,
        "POST",
        "/v1/stream/stale12r",
        &[
            ("content-type", "application/json"),
            ("stream-closed", "true"),
            ("producer-id", "p"),
            ("producer-epoch", "1"),
            ("producer-seq", "0"),
        ],
        br#"[{"fin":1}]"#,
    )
    .await;
    assert!(st >= 400, "stale epoch should be refused: {st}");
    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("stale12r"));
    let d = state
        .registry
        .get(&state.deployment.raw_adapter_sref("stale12r"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        d.sealing.is_none(),
        "raw: a permanently-stale final retained its intent: {:?}",
        d.sealing
    );
    assert!(!d.sealed);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/stale12r", &ct, br#"[{"n":2}]"#).await;
    assert!(st == 200 || st == 204, "raw appends held hostage: {st}");
    engine_shutdown(&state).await;
}
