//! Consumer saga.

use super::fixture_failpoints::gap_lock;
use super::fixture_http::{engine_shutdown, http_rig, rig_in_seal_gap};
use super::fixture_requests::{PRISMA_KEY, consumer_version, preq};
use super::fixture_storage::mem;

/// consumer_version with an explicit encryption key (round-18 cross-
/// key scenarios).
async fn consumer_version_with(
    addr: std::net::SocketAddr,
    stream: &str,
    cname: &str,
    key: &str,
) -> String {
    let (st, h, _) = preq(
        addr,
        "GET",
        &format!("/v1/streams/{stream}/consumers/{cname}"),
        &[("prisma-encryption-key", key)],
        b"",
    )
    .await;
    assert_eq!(
        st, 200,
        "consumer_version_with: GET {stream}/{cname} -> {st}"
    );
    h.get("prisma-consumer-version")
        .expect("prisma-consumer-version header")
        .clone()
}

// ---- round 18: the saga proves completion, never assumes it ----------

/// **A registry refresh failure is retryable, never mistaken for
/// stable topology.** The saga parks before its post-sweep refresh; a
/// one-shot registry error is injected; the resumed saga must answer a
/// retryable 503 (`segment_map_unverified`) — NOT 204 — leaving the
/// record Deleting. The consumer_deleting conflict now carries the
/// deleting incarnation's version token (round-18 recovery path), and
/// a retry with it completes the saga.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saga_refresh_failure_is_retryable_not_stable() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18a",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18a/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201);
    let ver = consumer_version(addr, "qc18a", "c1").await;

    crate::failpoints::park_consumer_saga_before_refresh("qc18a");
    let a1 = addr;
    let v1 = ver.clone();
    let del = tokio::spawn(async move {
        preq(
            a1,
            "DELETE",
            "/v1/streams/qc18a/consumers/c1",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-consumer-version", v1.as_str()),
            ],
            b"",
        )
        .await
    });
    while crate::failpoints::parked(crate::failpoints::Fp::ConsumerSagaBeforeRefresh, "qc18a") == 0
    {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    state.registry.fail_next_get("qc18a");
    crate::failpoints::release_consumer_saga_before_refresh("qc18a");
    let (st, _, b) = del.await.unwrap();
    assert_eq!(
        st,
        503,
        "an unverifiable map must be retryable, got {st}: {}",
        String::from_utf8_lossy(&b)
    );
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "segment_map_unverified");

    // The record is still Deleting; the PUT conflict carries the token
    // (the public recovery path for a client that lost its copy).
    let (st, h, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18a/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 409);
    let recovered = h
        .get("prisma-consumer-version")
        .expect("consumer_deleting must carry the version token")
        .clone();
    assert_eq!(
        recovered, ver,
        "the conflict names the deleting incarnation"
    );

    // Resume with the recovered token: completes collection-wide.
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc18a/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", recovered.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18a/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation after the recovered deletion");
    engine_shutdown(&state).await;
}

/// **A collection vanishing mid-saga is idempotent success, decided by
/// the refresh outcome — not by reusing a stale map.** The saga parks
/// before its refresh; the stream is deleted (no recreation); the
/// resumed saga must observe Ok(None)/not-alive and answer 204.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saga_vanished_collection_is_idempotent_success() {
    let _serial = gap_lock().lock().await;
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18b",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18b/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201);
    let ver = consumer_version(addr, "qc18b", "c1").await;

    crate::failpoints::park_consumer_saga_before_refresh("qc18b");
    let a1 = addr;
    let v1 = ver.clone();
    let del = tokio::spawn(async move {
        preq(
            a1,
            "DELETE",
            "/v1/streams/qc18b/consumers/c1",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-consumer-version", v1.as_str()),
            ],
            b"",
        )
        .await
    });
    while crate::failpoints::parked(crate::failpoints::Fp::ConsumerSagaBeforeRefresh, "qc18b") == 0
    {
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/qc18b", &key, b"").await;
    assert!(st == 200 || st == 202 || st == 204, "stream delete: {st}");
    crate::failpoints::release_consumer_saga_before_refresh("qc18b");
    let (st, _, b) = del.await.unwrap();
    assert_eq!(
        st,
        204,
        "a vanished collection is idempotent success: {}",
        String::from_utf8_lossy(&b)
    );
    engine_shutdown(&state).await;
}

/// **The saga never finalizes under a pending topology transition.** A
/// consumer is deleted while its collection sits in the parked
/// seal-gap (split Phase A done, successors withheld): every sweep
/// round sees `pending`, so the saga must refuse the 204 and answer a
/// retryable 503 — a false 204 here would strand rows on children the
/// map has not published yet. Releasing the gap and retrying with the
/// SAME token completes collection-wide across parent AND children.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_saga_never_finalizes_under_a_pending_transition() {
    let _l = gap_lock().lock().await;
    let (state, addr, guard, split) = rig_in_seal_gap("qc18c", 2).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18c/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let ver = consumer_version(addr, "qc18c", "c1").await;

    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc18c/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(
        st,
        503,
        "finalizing under a pending transition, got {st}: {}",
        String::from_utf8_lossy(&b)
    );
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(v["error"]["code"], "segment_map_unstable");

    // Publish the successors, then the SAME token resumes and finishes.
    drop(guard);
    assert!(split.await.unwrap(), "split completes after release");
    let mut done = false;
    for _ in 0..40 {
        let (st, _, _) = preq(
            addr,
            "DELETE",
            "/v1/streams/qc18c/consumers/c1",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-consumer-version", ver.as_str()),
            ],
            b"",
        )
        .await;
        if st == 204 {
            done = true;
            break;
        }
        assert_eq!(st, 503, "only retryable outcomes while settling: {st}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(done, "the deletion never completed after the gap released");
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18c/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201, "recreation after the post-split deletion");
    engine_shutdown(&state).await;
}

/// **A stale DELETE after a cross-KEY recreation is the documented
/// no-touch 204, not 403.** The token's stream epoch is compared
/// before key validation: the old client holds the old key, and the
/// honest answer about its vanished target does not require the new
/// one. The replacement (new key) is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_delete_after_cross_key_recreation_is_untouched_204() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    const KEY2: &str = "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=";
    let k1 = [("prisma-encryption-key", PRISMA_KEY)];
    let k2 = [("prisma-encryption-key", KEY2)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18d",
        &k1,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18d/consumers/c1",
        &k1,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201);
    let ver1 = consumer_version(addr, "qc18d", "c1").await;

    // The collection dies and is reborn under a DIFFERENT key.
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/qc18d", &k1, b"").await;
    assert!(st == 200 || st == 202 || st == 204);
    let (st, _, b) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18d",
        &k2,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc18d/consumers/c1",
        &k2,
        br#"{"visibilityTimeoutMs":30000}"#,
    )
    .await;
    assert_eq!(st, 201, "replacement consumer under the new key");
    let ver2 = consumer_version_with(addr, "qc18d", "c1", KEY2).await;

    // The old client retries its consumer DELETE: old key, old token.
    let (st, _, b) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc18d/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver1.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(
        st,
        204,
        "stale cross-key retry must be no-touch 204, got {st}: {}",
        String::from_utf8_lossy(&b)
    );
    // The replacement is untouched.
    let ver2_after = consumer_version_with(addr, "qc18d", "c1", KEY2).await;
    assert_eq!(ver2, ver2_after, "the stale retry touched the replacement");
    // And a LIVE-target delete still requires the right key: same
    // epoch + wrong key is a real 403.
    let (st, _, _) = preq(
        addr,
        "DELETE",
        "/v1/streams/qc18d/consumers/c1",
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-consumer-version", ver2.as_str()),
        ],
        b"",
    )
    .await;
    assert_eq!(st, 403, "a live-target delete with the wrong key must 403");
    engine_shutdown(&state).await;
}

// ---------------------------------------------------------------
// Round 19 must-fix 3: consumer-generation fences must survive an
// OWNERSHIP MOVE, not just handle eviction.
// ---------------------------------------------------------------

/// The scenario the round-19 review specified. A generation-1 pull is
/// in flight; DELETE fences and cleans the segment on owner A; the
/// shard then moves to owner B, which opens a FRESH engine whose
/// in-memory fence map is empty. The resumed generation-1 receive must
/// still refuse, and must write no cursor, lease or ack row — the
/// parent deletion may already have answered 204.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_fence_survives_ownership_move() {
    let store = mem();
    let (state, addr) = http_rig(store).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc19",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    for i in 0..2 {
        let (st, _, _) = preq(
            addr,
            "POST",
            "/v1/streams/qc19/records",
            &[
                ("prisma-encryption-key", PRISMA_KEY),
                ("prisma-routing-key", &format!("k{i}")),
            ],
            format!("{{\"n\":{i}}}").as_bytes(),
        )
        .await;
        assert_eq!(st, 200);
    }
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc19/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc19/consumers/c1:pull",
        &key,
        br#"{"max": 2}"#,
    )
    .await;
    assert_eq!(st, 200);

    state
        .registry
        .invalidate(&state.deployment.raw_adapter_sref("qc19"));
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref("qc19"))
        .await
        .unwrap()
        .unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let engine = state.engine_for(&route).await.unwrap();

    // Owner A fences generation 1 and cleans the segment.
    let del = engine
        .submit_queue(
            identity,
            crate::queue::QueueOp::ConfigDeleteStep {
                consumer: "c1".into(),
                fence_below: 2,
                max_rows: 4096,
                max_bytes: 1 << 20,
            },
        )
        .await;
    assert!(matches!(
        del,
        Ok(crate::queue::QueueOut::DeleteStep { complete: true, .. })
    ));
    // The fence is DURABLE, not just resident.
    assert_eq!(
        engine.durable_consumer_fence(identity, "c1").await,
        Some(2),
        "the cleanup step must persist its fence"
    );

    // OWNERSHIP MOVES: the new owner opens a fresh engine with an empty
    // fence map. (Before this fix that map's emptiness read as "no
    // fence" and the receive below was ACCEPTED.)
    engine.forget_consumer_fences_for_test();
    let rows_before = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();

    let resumed = engine
        .submit_queue(
            identity,
            crate::queue::QueueOp::Receive {
                consumer: "c1".into(),
                cgen: 1,
                max: 2,
                visibility_ms: 30_000,
                max_deliveries: 3,
                keys: Default::default(),
                covered_to: 0,
            },
        )
        .await;
    let err = resumed.expect_err("a dead generation was accepted after the move");
    assert!(
        err.starts_with("consumer_generation_fenced"),
        "expected a fence refusal, got: {err}"
    );
    let rows_after = engine
        .count_consumer_state_rows(identity, "c1")
        .await
        .unwrap();
    assert_eq!(
        rows_after, rows_before,
        "the refused generation-1 receive still wrote delivery-state rows"
    );
    engine_shutdown(&state).await;
}
