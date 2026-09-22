//! Dead-letter handoff ordering and the lease window a settle keeps: a durable DLQ
//! append precedes source settlement, and a retry's delay or an extend's visibility
//! never writes a lease that cannot expire.
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

// ---- the lease window a settle keeps ---------------------------------

/// The lease window `:pull` has always kept, which a settle must keep too.
/// A literal on purpose: the oracle is independent of
/// `queue::MAX_LEASE_WINDOW_MS`, and these scenarios compile on a tree
/// without it.
const TWELVE_HOURS_MS: i64 = 12 * 3600 * 1000;

/// A JSON `stream` with one record under routing key `k` and consumer
/// `work` (three attempts, no dead letter), leased once by a pull. Returns
/// that lease's token; a retry or an extend keeps its generation, so the
/// same token settles every later window in a scenario.
async fn leased_once(addr: std::net::SocketAddr, stream: &str) -> String {
    let (status, _, body) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{stream}"),
        &KEY,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    append_keyed(addr, stream, "k", br#"{"n":0}"#).await;
    let (status, _, body) = preq(
        addr,
        "PUT",
        &format!("/v1/streams/{stream}/consumers/work"),
        &KEY,
        br#"{"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let first = bounded_pull(addr, stream, b"{}").await;
    let messages = first["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "attempt one leases the record: {first}");
    assert_eq!(messages[0]["attempts"], 1);
    messages[0]["leaseToken"].as_str().unwrap().to_owned()
}

/// One settle of `token` alone under `verb` (`retries` or `extends`) with
/// its window `field` (`delayMs` or `visibilityMs`) at `requested`, or left
/// out. Returns the reply and the wall-clock bracket the committer's `now`
/// fell in. Bounded like `bounded_pull`: a committer that died on the
/// request fails the scenario instead of wedging it.
async fn settle_window(
    addr: std::net::SocketAddr,
    stream: &str,
    token: &str,
    (verb, field, requested): (&str, &str, Option<u64>),
) -> (serde_json::Value, (i64, i64)) {
    let window = requested.map_or_else(String::new, |ms| format!(r#","{field}":{ms}"#));
    let doc = format!(r#"{{"{verb}":[{{"leaseToken":"{token}"{window}}}]}}"#);
    let path = format!("/v1/streams/{stream}/consumers/work:settle");
    let before = crate::shard::now_ms();
    let (status, _, body) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        preq(addr, "POST", &path, &KEY, doc.as_bytes()),
    )
    .await
    .expect("a settle answers within five seconds");
    let after = crate::shard::now_ms();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    (serde_json::from_slice(&body).unwrap(), (before, after))
}

/// The deadline the durable lease row behind `token` holds.
async fn lease_deadline_ms(state: &crate::http::AppState, stream: &str, token: &str) -> i64 {
    let desc = state
        .registry
        .get(&state.deployment.raw_adapter_sref(stream))
        .await
        .unwrap()
        .unwrap();
    let stream_key = crate::crypto::StreamKey::from_b64(PRISMA_KEY).unwrap();
    let lease = crate::product_cursor::LeaseToken::decode(
        token,
        &desc.project_id,
        &stream_key,
        &desc.epoch(),
    )
    .unwrap();
    let segment = desc.resolve_segment("k");
    let engine = state.engine_for(&segment.shard_route).await.unwrap();
    let key = crate::queue::lease_key(
        &segment.identity,
        "work",
        lease.consumer_gen,
        lease.msg.offset,
    );
    let row = engine
        .db
        .get(&key)
        .await
        .unwrap()
        .expect("a held lease keeps its row");
    crate::queue::decode_lease(&row)
        .expect("a lease row decodes")
        .deadline_ms
}

/// The committer stamps `deadline = now + window` at a `now` inside the
/// bracket, so the bound is exact on both sides.
fn assert_held(deadline_ms: i64, (before, after): (i64, i64), held_ms: i64, asked: &str) {
    assert!(
        (before + held_ms..=after + held_ms).contains(&deadline_ms),
        "{asked}: the lease row's deadline is {} ms past the settle, not {held_ms}",
        deadline_ms.saturating_sub(before)
    );
}

/// The longest `delayMs` a `u64` carries is held to twelve hours. Unbounded,
/// the committer's `now + delay as i64` wraps to `now - 1` and the very next
/// pull redelivers the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_asking_for_the_longest_delay_is_not_redelivered_at_once() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-retry-max").await;
    let (reply, bracket) = settle_window(
        addr,
        "lw-retry-max",
        &token,
        ("retries", "delayMs", Some(u64::MAX)),
    )
    .await;
    assert_eq!(reply["retried"], 1, "{reply}");
    let again = bounded_pull(addr, "lw-retry-max", b"{}").await;
    assert_eq!(
        again["messages"].as_array().unwrap().len(),
        0,
        "the longest delay a u64 carries redelivered the record at once: {again}"
    );
    let deadline = lease_deadline_ms(&state, "lw-retry-max", &token).await;
    assert_held(deadline, bracket, TWELVE_HOURS_MS, "delayMs u64::MAX");
    engine_shutdown(&state).await;
}

/// A retry's `delayMs` lands in the lease row as `now + delay`. Unbounded,
/// 9e18 ms never expires: the record is never redelivered, never reaches
/// `maxAttempts`, never dead-letters, and its key stays blocked. A settle
/// holds the delay to the twelve hours a pull already keeps and passes the
/// rest through unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_delay_beyond_the_lease_window_is_held_to_twelve_hours() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-retry").await;
    for (requested, held_ms) in [
        (Some(9_000_000_000_000_000_000), TWELVE_HOURS_MS),
        (Some(250), 250),
        (Some(0), 0),
        (None, 1_000),
    ] {
        let (reply, bracket) =
            settle_window(addr, "lw-retry", &token, ("retries", "delayMs", requested)).await;
        assert_eq!(reply["retried"], 1, "{requested:?}: {reply}");
        let deadline = lease_deadline_ms(&state, "lw-retry", &token).await;
        assert_held(
            deadline,
            bracket,
            held_ms,
            &format!("delayMs {requested:?}"),
        );
    }
    engine_shutdown(&state).await;
}

/// The longest `visibilityMs` a `u64` carries is held to twelve hours on an
/// extend too; unbounded it wraps and the next pull redelivers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_extend_asking_for_the_longest_visibility_is_not_redelivered_at_once() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-extend-max").await;
    let (reply, bracket) = settle_window(
        addr,
        "lw-extend-max",
        &token,
        ("extends", "visibilityMs", Some(u64::MAX)),
    )
    .await;
    assert_eq!(reply["extended"], 1, "{reply}");
    let again = bounded_pull(addr, "lw-extend-max", b"{}").await;
    assert_eq!(
        again["messages"].as_array().unwrap().len(),
        0,
        "the longest visibility a u64 carries redelivered the record at once: {again}"
    );
    let deadline = lease_deadline_ms(&state, "lw-extend-max", &token).await;
    assert_held(deadline, bracket, TWELVE_HOURS_MS, "visibilityMs u64::MAX");
    engine_shutdown(&state).await;
}

/// An extend's `visibilityMs` is held to the same one second to twelve hours
/// as a pull's, and defaults to the consumer's configured timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_extended_visibility_is_held_between_one_second_and_twelve_hours() {
    let (state, addr) = http_rig(mem()).await;
    let token = leased_once(addr, "lw-extend").await;
    for (requested, held_ms) in [
        (Some(9_000_000_000_000_000_000), TWELVE_HOURS_MS),
        (Some(0), 1_000),
        (Some(5_000), 5_000),
        (None, 30_000),
    ] {
        let (reply, bracket) = settle_window(
            addr,
            "lw-extend",
            &token,
            ("extends", "visibilityMs", requested),
        )
        .await;
        assert_eq!(reply["extended"], 1, "{requested:?}: {reply}");
        let deadline = lease_deadline_ms(&state, "lw-extend", &token).await;
        assert_held(
            deadline,
            bracket,
            held_ms,
            &format!("visibilityMs {requested:?}"),
        );
    }
    engine_shutdown(&state).await;
}

/// Control: a pull already keeps the window. Its clamp moves onto the shared
/// owner in this change and must keep answering the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pulled_visibility_beyond_the_lease_window_is_held_to_twelve_hours() {
    let (state, addr) = http_rig(mem()).await;
    let (status, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/lw-pull",
        &KEY,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    append_keyed(addr, "lw-pull", "k", br#"{"n":0}"#).await;
    let (status, _, body) = preq(
        addr,
        "PUT",
        "/v1/streams/lw-pull/consumers/work",
        &KEY,
        b"{}",
    )
    .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));
    let before = crate::shard::now_ms();
    let pulled = bounded_pull(addr, "lw-pull", br#"{"visibilityMs":9000000000000000000}"#).await;
    let after = crate::shard::now_ms();
    let token = pulled["messages"][0]["leaseToken"].as_str().unwrap();
    let deadline = lease_deadline_ms(&state, "lw-pull", token).await;
    assert_held(
        deadline,
        (before, after),
        TWELVE_HOURS_MS,
        "a pull's visibilityMs 9e18",
    );
    engine_shutdown(&state).await;
}
