//! Durable DLQ delivery must precede source settlement, including retry after failure.
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, preq};
use super::fixture_storage::mem;

#[expect(
    clippy::too_many_lines,
    reason = "dead-letter commit scenario; the failed commit, the retained source lease and the exact retry settling once form one causal sequence; helper phases would hide which step could settle twice"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r07_failed_dlq_commit_retains_source_lease_and_exact_retry_settles_once() {
    let (state, addr) = http_rig(mem()).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    for name in ["r07-source", "r07-target"] {
        let (status, _, body) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &key,
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    }
    assert_eq!(
        preq(
            addr,
            "POST",
            "/v1/streams/r07-source/records",
            &key,
            br#"{"poison":true}"#
        )
        .await
        .0,
        200
    );
    assert_eq!(
        preq(
            addr,
            "PUT",
            "/v1/streams/r07-source/consumers/work",
            &key,
            br#"{"maxAttempts":1,"deadLetterStream":"r07-target"}"#
        )
        .await
        .0,
        201
    );
    let (status, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/r07-source/consumers/work:pull",
        &key,
        b"{}",
    )
    .await;
    assert_eq!(status, 200);
    let pull: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let token = pull["messages"][0]["leaseToken"].as_str().unwrap();
    let retry = serde_json::json!({"retries":[{"leaseToken":token,"delayMs":0}]}).to_string();
    let source = state
        .registry
        .get(&state.deployment.raw_adapter_sref("r07-source"))
        .await
        .unwrap()
        .unwrap();
    let target = state
        .registry
        .get(&state.deployment.raw_adapter_sref("r07-target"))
        .await
        .unwrap()
        .unwrap();
    let source_segment = source.resolve_segment("");
    let target_segment = target.resolve_segment("");
    let source_engine = state.engine_for(&source_segment.shard_route).await.unwrap();
    let target_engine = state.engine_for(&target_segment.shard_route).await.unwrap();
    let stream_key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let lease = crate::product_cursor::LeaseToken::decode(
        token,
        &source.project_id,
        &stream_key,
        &source.epoch(),
    )
    .unwrap();
    let lease_key = crate::queue::lease_key(
        &source_segment.identity,
        "work",
        lease.consumer_gen,
        lease.msg.offset,
    );
    target_engine.fail_next_group_for(target_segment.identity);
    let (status, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/r07-source/consumers/work:settle",
        &key,
        retry.as_bytes(),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let failed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        failed["dlq"], 0,
        "a failed DLQ commit cannot be counted as settled"
    );
    assert_eq!(
        target_engine.group_failures_tripped(),
        1,
        "target failure must actually fire"
    );
    assert!(
        source_engine.db.get(&lease_key).await.unwrap().is_some(),
        "source lease survives failed DLQ append"
    );
    assert_eq!(
        source_engine
            .queue_cursor(source_segment.identity, "work", lease.consumer_gen)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        target_engine
            .stream_handle(target_segment.identity)
            .await
            .unwrap()
            .state
            .lock()
            .unwrap()
            .durable
            .next,
        0
    );

    let (status, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/r07-source/consumers/work:settle",
        &key,
        retry.as_bytes(),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let resumed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        resumed["dlq"], 1,
        "same lease retry completes the durable handoff"
    );
    assert!(source_engine.db.get(&lease_key).await.unwrap().is_none());
    assert_eq!(
        source_engine
            .queue_cursor(source_segment.identity, "work", lease.consumer_gen)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        target_engine
            .stream_handle(target_segment.identity)
            .await
            .unwrap()
            .state
            .lock()
            .unwrap()
            .durable
            .next,
        1
    );

    let (status, _, body) = preq(
        addr,
        "POST",
        "/v1/streams/r07-source/consumers/work:settle",
        &key,
        retry.as_bytes(),
    )
    .await;
    assert_eq!(status, 200);
    let duplicate: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(duplicate["stale"], 1);
    assert_eq!(duplicate["dlq"], 0);
    assert_eq!(
        target_engine
            .stream_handle(target_segment.identity)
            .await
            .unwrap()
            .state
            .lock()
            .unwrap()
            .durable
            .next,
        1,
        "stale source retries cannot duplicate the DLQ record"
    );
    engine_shutdown(&state).await;
}

const KEY: [(&str, &str); 1] = [("prisma-encryption-key", PRISMA_KEY)];

async fn append_keyed(addr: std::net::SocketAddr, stream: &str, routing_key: &str, body: &[u8]) {
    let (status, _, response) = preq(
        addr,
        "POST",
        &format!("/v1/streams/{stream}/records"),
        &[
            ("prisma-encryption-key", PRISMA_KEY),
            ("prisma-routing-key", routing_key),
        ],
        body,
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&response));
}

/// A JSON `source` and `target`, consumer `work` dead-lettering to `target`
/// after ONE attempt, and a record under routing key `p` whose only lease was
/// granted and has expired: the next Receive reports it poisoned. Returns
/// that expired lease's token.
async fn source_with_expired_poison(
    addr: std::net::SocketAddr,
    source: &str,
    target: &str,
) -> String {
    for name in [source, target] {
        let (status, _, body) = preq(
            addr,
            "PUT",
            &format!("/v1/streams/{name}"),
            &KEY,
            br#"{"format":{"kind":"json"}}"#,
        )
        .await;
        assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    }
    append_keyed(addr, source, "p", br#"{"poison":true}"#).await;
    let config = format!(r#"{{"maxAttempts":1,"deadLetterStream":"{target}"}}"#);
    let (status, _, body) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{source}/consumers/work"),
        &KEY,
        config.as_bytes(),
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let first = bounded_pull(addr, source, br#"{"visibilityMs":1000}"#).await;
    let messages = first["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "attempt one leases the record: {first}");
    let token = messages[0]["leaseToken"].as_str().unwrap().to_owned();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    token
}

/// One `:pull` on consumer `work`. A pull answers within its own `waitMs`
/// (here at most 200 ms); one still running after five seconds is the wedge
/// these scenarios exist to catch.
async fn bounded_pull(addr: std::net::SocketAddr, source: &str, doc: &[u8]) -> serde_json::Value {
    let path = format!("/v1/streams/{source}/consumers/work:pull");
    let (status, _, body) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        preq(addr, "POST", &path, &KEY, doc),
    )
    .await
    .expect("a pull answers by its deadline even when the dead-letter handoff is blocked");
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

/// A poisoned lease whose dead-letter handoff is blocked keeps only its own
/// key blocked. The pull still answers by its deadline, and the leases the
/// same Receive granted to other keys are delivered rather than dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_dead_letter_handoff_neither_wedges_the_pull_nor_drops_other_keys_leases() {
    let (state, addr) = http_rig(mem()).await;
    let token = source_with_expired_poison(addr, "b4-blocked", "b4-blocked-dlq").await;
    // The configured target dies and is reborn under the same name: an
    // incarnation the handoff must refuse for as long as it is configured.
    let (status, _, _) = preq(addr, "DELETE", "/v1/streams/b4-blocked-dlq", &KEY, b"").await;
    assert!(matches!(status, 200 | 202 | 204), "delete target: {status}");
    let (status, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/b4-blocked-dlq",
        &KEY,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    append_keyed(addr, "b4-blocked", "h", br#"{"healthy":true}"#).await;

    let first = bounded_pull(addr, "b4-blocked", b"{}").await;
    let messages = first["messages"].as_array().unwrap();
    assert_eq!(
        messages.len(),
        1,
        "the other key's lease is delivered: {first}"
    );
    assert_eq!(messages[0]["routingKey"], "h");
    assert_eq!(messages[0]["attempts"], 1);

    // `h` is now in flight and `p` is blocked, so nothing is leasable: the
    // pull owes an empty answer at its deadline, not another walk.
    let second = bounded_pull(addr, "b4-blocked", br#"{"waitMs":200}"#).await;
    assert!(
        second["messages"].as_array().unwrap().is_empty(),
        "{second}"
    );

    let source = state
        .registry
        .get(&state.deployment.raw_adapter_sref("b4-blocked"))
        .await
        .unwrap()
        .unwrap();
    let stream_key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let lease = crate::product_cursor::LeaseToken::decode(
        &token,
        &source.project_id,
        &stream_key,
        &source.epoch(),
    )
    .unwrap();
    let segment = source.resolve_segment("p");
    let engine = state.engine_for(&segment.shard_route).await.unwrap();
    let lease_key = crate::queue::lease_key(
        &segment.identity,
        "work",
        lease.consumer_gen,
        lease.msg.offset,
    );
    assert!(
        engine.db.get(&lease_key).await.unwrap().is_some(),
        "a blocked handoff retains the poisoned lease"
    );
    let target_ref = state.deployment.raw_adapter_sref("b4-blocked-dlq");
    state.registry.invalidate(&target_ref);
    let target = state.registry.get(&target_ref).await.unwrap().unwrap();
    let target_segment = target.resolve_segment("");
    let target_engine = state.engine_for(&target_segment.shard_route).await.unwrap();
    assert_eq!(
        target_engine
            .stream_handle(target_segment.identity)
            .await
            .unwrap()
            .state
            .lock()
            .unwrap()
            .durable
            .next,
        0,
        "the reborn target is not the approved incarnation and receives nothing"
    );
    engine_shutdown(&state).await;
}

/// A Receive that reports poison also grants leases to other keys. With the
/// handoff healthy those leases are still this pull's to deliver: dropping
/// them costs each record an attempt no consumer was ever given, and at
/// `maxAttempts = 1` dead-letters it undelivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_settled_dead_letter_handoff_still_delivers_the_same_receives_leases() {
    let (state, addr) = http_rig(mem()).await;
    source_with_expired_poison(addr, "b4-settled", "b4-settled-dlq").await;
    append_keyed(addr, "b4-settled", "h", br#"{"healthy":true}"#).await;

    let pull = bounded_pull(addr, "b4-settled", b"{}").await;
    let messages = pull["messages"].as_array().unwrap();
    assert_eq!(
        messages.len(),
        1,
        "the other key's lease is delivered: {pull}"
    );
    assert_eq!(messages[0]["routingKey"], "h");
    assert_eq!(messages[0]["attempts"], 1);

    let (status, _, body) =
        preq(addr, "GET", "/v1/streams/b4-settled-dlq/records", &KEY, b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let dead: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(dead.len(), 1, "the poison is handed off exactly once");
    assert_eq!(dead[0]["routingKey"], "p");
    engine_shutdown(&state).await;
}
