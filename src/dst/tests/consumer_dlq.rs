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
