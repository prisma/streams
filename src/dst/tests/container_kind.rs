//! Edge #253: a container's kind decides what it creates. A streams
//! container creates streams and a queues container queues; anything
//! else is refused `409 container_kind_mismatch` before it is stored.

use serde_json::Value;

use super::fixture_auth::{RIG_SCOPES, auth_rig, rig_bearer, rig_policy, rig_publish_grants};
use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;

const QUEUE_TYPE: &str = "application/vnd.prisma.queue+json";

/// A create's status and error code (empty when it has none).
async fn put(addr: std::net::SocketAddr, path: &str, bearer: &str, body: &str) -> (u16, String) {
    let headers = [
        ("prisma-encryption-key", PRISMA_KEY),
        ("streams-encryption-key", PRISMA_KEY),
        ("authorization", bearer),
    ];
    let (st, _, b) = preq(addr, "PUT", path, &headers, body.as_bytes()).await;
    let code = serde_json::from_slice::<Value>(&b)
        .ok()
        .and_then(|v| v["error"]["code"].as_str().map(str::to_string))
        .unwrap_or_default();
    (st, code)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn each_kind_of_container_creates_only_its_own_kind() {
    let (svc, state, addr) = auth_rig("ct-s", &["c-s"], None).await;
    let streams = rig_bearer("ct-s", "c-s", RIG_SCOPES, 1);
    let json = r#"{"format":{"kind":"json"}}"#;
    let queue_typed = format!(r#"{{"format":{{"kind":"bytes","contentType":"{QUEUE_TYPE}"}}}}"#);
    let mismatch = (409, "container_kind_mismatch".to_string());
    assert_eq!(put(addr, "/v1/streams/orders", &streams, json).await.0, 201);
    assert_eq!(put(addr, "/v1/queues/jobs", &streams, "{}").await, mismatch);
    // A stream create cannot carry the queue type into any container.
    assert_eq!(
        put(addr, "/v1/streams/typed", &streams, &queue_typed).await,
        mismatch
    );

    let mut queues = rig_policy("ct-q", 1, 1);
    queues.kind = crate::project_policy::ContainerKind::Queues;
    let both = [("ct-s", rig_policy("ct-s", 1, 1)), ("ct-q", queues)];
    svc.publish_policies(crate::project_policy::PolicySnapshot {
        projects: both
            .into_iter()
            .map(|(id, p)| (crate::tenant::ProjectId::new(id).unwrap(), p))
            .collect(),
        fetched_at_unix: crate::shard::now_ms() / 1000,
        feed_version: 2,
    })
    .unwrap();
    let active = crate::project_policy::CredentialStatus::Active;
    rig_publish_grants(&svc, "ct-q", &[("c-q", active, 1)], 2).unwrap();
    let queues = rig_bearer("ct-q", "c-q", RIG_SCOPES, 1);
    assert_eq!(put(addr, "/v1/queues/jobs", &queues, "{}").await.0, 201);
    assert_eq!(
        put(addr, "/v1/streams/orders", &queues, json).await,
        mismatch
    );
    assert_eq!(
        put(addr, "/v1/streams/typed", &queues, &queue_typed).await,
        mismatch
    );
    engine_shutdown(&state).await;
}

/// Without a policy (the raw surface, auth off) no kind applies, but a
/// stream create still cannot make a queue's descriptor: only
/// `/v1/queues` creates one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn no_stream_create_makes_a_queue_descriptor() {
    let (state, addr) = http_rig(mem()).await;
    let raw = hreq(
        addr,
        "PUT",
        "/v1/stream/raw-q",
        &[("content-type", QUEUE_TYPE)],
        b"",
    )
    .await;
    let code = serde_json::from_slice::<Value>(&raw.2).unwrap_or_default()["error"]["code"].clone();
    assert_eq!(
        (raw.0, code.as_str()),
        (409, Some("container_kind_mismatch"))
    );
    let typed = format!(r#"{{"format":{{"kind":"bytes","contentType":"{QUEUE_TYPE}"}}}}"#);
    assert_eq!(
        put(addr, "/v1/streams/typed", "", &typed).await,
        (409, "container_kind_mismatch".to_string())
    );
    engine_shutdown(&state).await;
}
