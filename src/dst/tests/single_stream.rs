//! Edge #253: while a container's `max_streams` is 1 it holds one stream
//! or queue, named `default`. Any other name is refused `409
//! container_single_stream`, so a dead-letter stream cannot exist beside
//! it; a stream's `default` is recreated at once after a delete, and a
//! queue's answers `409 queue_deleting` until its cleanup is done.

use serde_json::Value;

use super::fixture_auth::{
    RIG_SCOPES, auth_rig, auth_rig_with, rig_policy, rig_publish_policy, rig_scoped_bearer,
};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

/// A request's status and error code (empty when it has none).
async fn call(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    bearer: &str,
    body: &str,
) -> (u16, String) {
    let headers = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("streams-encryption-key", PRISMA_KEY),
        ("authorization", bearer),
    ];
    let (st, _, b) = preq(addr, method, path, &headers, body.as_bytes()).await;
    let code = serde_json::from_slice::<Value>(&b)
        .ok()
        .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
        .unwrap_or_default();
    (st, code)
}

/// The rig's container at `max_streams` 1, of `kind`, at policy version 2.
fn single(
    project: &str,
    kind: crate::project_policy::ContainerKind,
) -> crate::project_policy::ProjectPolicy {
    let mut p = rig_policy(project, 1, 2);
    p.kind = kind;
    p.quotas.max_streams = 1;
    p
}

const JSON: &str = r#"{"format":{"kind":"json"}}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_single_stream_container_holds_only_its_default_stream() {
    let (svc, state, addr) = auth_rig("ct-one", &["c1"], None).await;
    let streams = crate::project_policy::ContainerKind::Streams;
    rig_publish_policy(&svc, single("ct-one", streams), 2).unwrap();
    let scopes = format!(
        "{RIG_SCOPES} streams.lifecycle.manage streams.consumers.configure streams.dlq.configure"
    );
    let bearer = rig_scoped_bearer(&svc, "ct-one", "c1", &scopes, 2);
    let refused = (409, "container_single_stream".to_string());
    assert_eq!(
        call(addr, "PUT", "/v1/streams/orders", &bearer, JSON).await,
        refused
    );
    assert_eq!(
        call(addr, "PUT", "/v1/streams/default", &bearer, JSON)
            .await
            .0,
        201
    );
    // A dead-letter stream would be a second stream: none can exist.
    assert_eq!(
        call(addr, "PUT", "/v1/streams/dlq", &bearer, JSON).await,
        refused
    );
    let config = r#"{"maxAttempts":1,"deadLetterStream":"dlq"}"#;
    let dlq = call(
        addr,
        "PUT",
        "/v1/streams/default/consumers/w",
        &bearer,
        config,
    )
    .await;
    assert_eq!(dlq, (400, "unknown_dead_letter_stream".to_string()));
    // Deleted, the stream's name is recreated at once.
    assert_eq!(
        call(addr, "DELETE", "/v1/streams/default", &bearer, "")
            .await
            .0,
        204
    );
    assert_eq!(
        call(addr, "PUT", "/v1/streams/default", &bearer, JSON)
            .await
            .0,
        201
    );
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_single_queue_container_holds_only_its_default_queue_and_waits_for_its_cleanup() {
    // A container's kind is fixed: this one is born a queues container.
    let mut first = single("ct-oneq", crate::project_policy::ContainerKind::Queues);
    first.project_policy_version = 1;
    let (svc, state, addr) = auth_rig_with(first, &["c1"], None).await;
    let scopes = format!("{RIG_SCOPES} streams.lifecycle.manage");
    let bearer = rig_scoped_bearer(&svc, "ct-oneq", "c1", &scopes, 2);
    let refused = (409, "container_single_stream".to_string());
    assert_eq!(
        call(addr, "PUT", "/v1/queues/jobs", &bearer, "{}").await,
        refused
    );
    assert_eq!(
        call(addr, "PUT", "/v1/queues/default", &bearer, "{}")
            .await
            .0,
        201
    );
    assert_eq!(
        call(addr, "DELETE", "/v1/queues/default", &bearer, "")
            .await
            .0,
        202
    );
    let mut again = call(addr, "PUT", "/v1/queues/default", &bearer, "{}").await;
    assert!(
        again == (409, "queue_deleting".to_string()) || again.0 == 201,
        "a create during the cleanup: {again:?}"
    );
    for _ in 0..100 {
        if again.0 == 201 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        again = call(addr, "PUT", "/v1/queues/default", &bearer, "{}").await;
    }
    assert_eq!(again.0, 201, "created again once the cleanup is done");
    engine_shutdown(&state).await;
}

/// A create that loses the create-only PUT judges the winner as a read
/// would: a stale cached absence (another instance created and deleted the
/// name since) meets a tombstone, which is recreated, never answered as
/// an existing stream. Product and raw creates alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_create_over_a_stale_absence_recreates_the_tombstone_it_meets() {
    let (state, addr) = http_rig(mem()).await;
    let product = state.deployment.deployment_tenant().stream_ref("default");
    assert_eq!(
        call(addr, "PUT", "/v1/streams/default", "", JSON).await.0,
        201
    );
    assert_eq!(
        call(addr, "DELETE", "/v1/streams/default", "", "").await.0,
        204
    );
    state.registry.test_poison_cache_absent(&product);
    assert_eq!(
        call(addr, "PUT", "/v1/streams/default", "", JSON).await.0,
        201
    );
    let alive = state.registry.get(&product).await.unwrap();
    assert!(
        alive.is_some_and(|d| crate::application::creation::desc_alive(&d)),
        "recreated"
    );

    let ct = [("content-type", "application/json")];
    assert_eq!(hreq(addr, "PUT", "/v1/stream/raw-x", &ct, b"").await.0, 201);
    assert_eq!(
        hreq(addr, "DELETE", "/v1/stream/raw-x", &[], b"").await.0,
        204
    );
    let raw = state.deployment.raw_adapter_sref("raw-x");
    state.registry.test_poison_cache_absent(&raw);
    assert_eq!(hreq(addr, "PUT", "/v1/stream/raw-x", &ct, b"").await.0, 201);
    let alive = state.registry.get(&raw).await.unwrap();
    assert!(
        alive.is_some_and(|d| crate::application::creation::desc_alive(&d)),
        "raw recreated"
    );
    engine_shutdown(&state).await;
}
