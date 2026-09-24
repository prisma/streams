//! Operation counts (docs/OBSERVABILITY-BILLING.md §4.5 `append_requests`)
//! follow the typed outcome (item 28 step B). An accepted request is counted
//! once, against the incarnation it committed to, with no second descriptor
//! read between the outcome and the count; a refusal, and a request whose
//! handler never answered, count nothing.

use super::fixture_http::{engine_shutdown, http_rig};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_storage::mem;
use crate::failpoints::Fp;
use std::time::Duration;

const KEY: [(&str, &str); 1] = [("prisma-encryption-key", PRISMA_KEY)];
const JSON: [(&str, &str); 1] = [("content-type", "application/json")];

/// The active window's counters for one incarnation (`stream_id` is the
/// descriptor's `stream_epoch`); zero when nothing was counted. The rig
/// runs no telemetry loop, so the window is not rotated under a test.
fn counts(state: &crate::http::AppState, stream_id: &str) -> crate::billing::RowDelta {
    state
        .billing
        .reads()
        .snapshot_active()
        .into_iter()
        .find(|(id, _)| id.stream_id == stream_id)
        .map(|(_, delta)| delta)
        .unwrap_or_default()
}

/// The descriptor the registry resolves `name` to now.
async fn descriptor(state: &crate::http::AppState, name: &str) -> crate::registry::StreamDesc {
    let sref = state.deployment.raw_adapter_sref(name);
    state.registry.invalidate(&sref);
    state.registry.get(&sref).await.unwrap().unwrap()
}

/// Create (or recreate) a JSON product stream; its fresh descriptor.
async fn product_stream(
    state: &crate::http::AppState,
    addr: std::net::SocketAddr,
    name: &str,
) -> crate::registry::StreamDesc {
    let path = format!("/v1/streams/{name}");
    let (st, _, body) = preq(addr, "PUT", &path, &KEY, br#"{"format":{"kind":"json"}}"#).await;
    assert_eq!(st, 201, "create {name}: {}", String::from_utf8_lossy(&body));
    descriptor(state, name).await
}

/// POST to a product route with the stream key plus `extra`: the status,
/// the headers and the body as JSON (`Null` when it is not JSON).
async fn product_post(
    addr: std::net::SocketAddr,
    path: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> (
    u16,
    std::collections::HashMap<String, String>,
    serde_json::Value,
) {
    let mut headers = KEY.to_vec();
    headers.extend_from_slice(extra);
    let (status, head, body) = preq(addr, "POST", path, &headers, body).await;
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, head, json)
}

/// Park `name`'s next append at the committer enqueue, after every
/// descriptor read the append makes, and run `request` until it arrives
/// there. The caller changes the registry, releases and joins.
#[expect(
    clippy::disallowed_methods,
    reason = "operation-count fixture; the request is parked at the committer enqueue and joined by the caller after release; run inline it would block on its own failpoint"
)]
async fn park_append<T: Send + 'static>(
    name: &str,
    request: impl std::future::Future<Output = T> + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    let before = crate::failpoints::parked(Fp::AppendBeforeEnqueue, name);
    crate::failpoints::park_append_before_enqueue(name);
    let handle = tokio::spawn(request);
    let mut arrived = false;
    for _ in 0..300 {
        if crate::failpoints::parked(Fp::AppendBeforeEnqueue, name) > before {
            arrived = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        arrived,
        "{name}: the append never reached the committer enqueue"
    );
    handle
}

/// Red: a delete and recreate that lands between an append's commit and
/// its answer makes the NAME resolve to the successor. The count belongs
/// to the incarnation the append committed to, never to whatever the name
/// resolves to afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_straddling_a_recreate_counts_on_the_incarnation_it_committed_to() {
    let (state, addr) = http_rig(mem()).await;
    let committed = product_stream(&state, addr, "opcrecreate").await;
    let mut next = committed.to_persisted();
    let epoch = u128::from_str_radix(&committed.stream_epoch, 16).unwrap();
    next.stream_epoch = format!("{:032x}", epoch ^ 1);
    let successor = crate::registry::StreamDesc::try_from(next).unwrap();
    let path = "/v1/streams/opcrecreate/records";
    let append = park_append("opcrecreate", product_post(addr, path, &[], br#"{"n":1}"#)).await;
    // Exactly what the name resolves to once a recreate lands. A real
    // delete during the park would fence the parked write itself; the
    // defect depends only on the registry's answer.
    let sref = state.deployment.raw_adapter_sref("opcrecreate");
    state.registry.test_poison_cache(&sref, successor.clone());
    crate::failpoints::release_append_before_enqueue("opcrecreate");
    let (st, _, body) = append.await.unwrap();
    assert_eq!(st, 200, "{body}");
    assert_eq!(
        counts(&state, &successor.stream_epoch).append_requests,
        0,
        "an append committed to the old incarnation was counted on its successor"
    );
    assert_eq!(
        counts(&state, &committed.stream_epoch).append_requests,
        1,
        "the committed incarnation lost its append count"
    );
    // Non-vacuity: a read after the answer would have seen the successor.
    let named = state.registry.get(&sref).await.unwrap().unwrap();
    assert_eq!(named.stream_epoch, successor.stream_epoch);
    state.registry.invalidate(&sref);
    engine_shutdown(&state).await;
}

/// Red: the count is taken from the outcome, so a descriptor store that
/// fails right after the commit cannot lose it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_counts_when_its_descriptor_cannot_be_read_again() {
    let (state, addr) = http_rig(mem()).await;
    let desc = product_stream(&state, addr, "opcreread").await;
    let path = "/v1/streams/opcreread/records";
    let append = park_append("opcreread", product_post(addr, path, &[], br#"{"n":1}"#)).await;
    state.registry.fail_next_get("opcreread");
    crate::failpoints::release_append_before_enqueue("opcreread");
    let (st, _, body) = append.await.unwrap();
    assert_eq!(st, 200, "{body}");
    assert_eq!(
        counts(&state, &desc.stream_epoch).append_requests,
        1,
        "a failed descriptor re-read dropped a committed append's count"
    );
    // Non-vacuity: the one-shot fault is still armed, so nothing read the
    // descriptor between the outcome and the count.
    let sref = state.deployment.raw_adapter_sref("opcreread");
    assert!(
        state.registry.get(&sref).await.is_err(),
        "the metering path read the descriptor again"
    );
    engine_shutdown(&state).await;
}

/// Red, raw surface: the same outcome-held count for `POST /v1/stream`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_append_counts_when_its_descriptor_cannot_be_read_again() {
    let (state, addr) = http_rig(mem()).await;
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/opcrawreread", &JSON, b"").await;
    assert_eq!(st, 201);
    let epoch = descriptor(&state, "opcrawreread")
        .await
        .stream_epoch
        .clone();
    let path = "/v1/stream/opcrawreread";
    let append = park_append(
        "opcrawreread",
        hreq(addr, "POST", path, &JSON, br#"[{"n":1}]"#),
    )
    .await;
    state.registry.fail_next_get("opcrawreread");
    crate::failpoints::release_append_before_enqueue("opcrawreread");
    let (st, _, body) = append.await.unwrap();
    assert_eq!(st, 204, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        counts(&state, &epoch).append_requests,
        1,
        "a failed descriptor re-read dropped a committed append's count"
    );
    let sref = state.deployment.raw_adapter_sref("opcrawreread");
    assert!(
        state.registry.get(&sref).await.is_err(),
        "the metering path read the descriptor again"
    );
    engine_shutdown(&state).await;
}

/// Pin: the product append answer is JSON that is never cached, with
/// exactly the documented keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_product_append_answers_json_that_is_never_cached() {
    let (state, addr) = http_rig(mem()).await;
    product_stream(&state, addr, "opcwire").await;
    let path = "/v1/streams/opcwire/records";
    let (st, head, body) = product_post(addr, path, &[], br#"{"n":1}"#).await;
    assert_eq!(st, 200, "{body}");
    assert_eq!(
        head.get("content-type").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(
        head.get("cache-control").map(String::as_str),
        Some("no-store")
    );
    let mut keys: Vec<&str> = body
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["count", "cursor", "duplicate", "sealed"]);
    engine_shutdown(&state).await;
}

/// Pin: one count per accepted product request (a producer duplicate
/// included, a batch once), none for a refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_append_counts_follow_the_typed_outcome() {
    let (state, addr) = http_rig(mem()).await;
    let epoch = product_stream(&state, addr, "opcproduct")
        .await
        .stream_epoch
        .clone();
    let records = "/v1/streams/opcproduct/records";
    let appended = |n: u64| {
        let got = counts(&state, &epoch).append_requests;
        assert_eq!(got, n, "append_requests after step {n}");
    };
    assert_eq!(product_post(addr, records, &[], br#"{"n":1}"#).await.0, 200);
    appended(1);
    let batch = "/v1/streams/opcproduct/records:batch";
    let (st, _, body) = product_post(addr, batch, &[], br#"[{"n":2},{"n":3}]"#).await;
    assert_eq!((st, &body["count"]), (200, &serde_json::Value::from(2)));
    appended(2);
    let producer = [
        ("producer-id", "p1"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    let (st, _, body) = product_post(addr, records, &producer, br#"{"n":4}"#).await;
    assert_eq!(
        (st, &body["duplicate"]),
        (200, &serde_json::Value::from(false))
    );
    appended(3);
    let (st, _, body) = product_post(addr, records, &producer, br#"{"n":4}"#).await;
    assert_eq!(
        (st, &body["duplicate"]),
        (200, &serde_json::Value::from(true))
    );
    appended(4);
    let gap = [
        ("producer-id", "p1"),
        ("producer-epoch", "1"),
        ("producer-seq", "5"),
    ];
    assert_eq!(
        product_post(addr, records, &gap, br#"{"n":5}"#).await.0,
        409
    );
    assert_eq!(product_post(addr, records, &[], b"{").await.0, 400);
    assert_eq!(
        counts(&state, &epoch).append_requests,
        4,
        "a refusal was counted"
    );
    engine_shutdown(&state).await;
}

/// Pin: one count per accepted raw request (a producer duplicate and a
/// close included), none for a refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_append_counts_follow_the_typed_outcome() {
    let (state, addr) = http_rig(mem()).await;
    let path = "/v1/stream/opcrawcount";
    assert_eq!(hreq(addr, "PUT", path, &JSON, b"").await.0, 201);
    let epoch = descriptor(&state, "opcrawcount").await.stream_epoch.clone();
    let appended = |n: u64| {
        let got = counts(&state, &epoch).append_requests;
        assert_eq!(got, n, "append_requests after step {n}");
    };
    assert_eq!(
        hreq(addr, "POST", path, &JSON, br#"[{"n":1}]"#).await.0,
        204
    );
    appended(1);
    let producer = [
        ("content-type", "application/json"),
        ("producer-id", "p1"),
        ("producer-epoch", "1"),
        ("producer-seq", "0"),
    ];
    assert_eq!(
        hreq(addr, "POST", path, &producer, br#"[{"n":2}]"#).await.0,
        200
    );
    appended(2);
    assert_eq!(
        hreq(addr, "POST", path, &producer, br#"[{"n":2}]"#).await.0,
        204
    );
    appended(3);
    let invalid = [
        ("content-type", "application/json"),
        ("producer-id", "p1"),
        ("producer-epoch", "x"),
        ("producer-seq", "1"),
    ];
    assert_eq!(
        hreq(addr, "POST", path, &invalid, br#"[{"n":3}]"#).await.0,
        400
    );
    assert_eq!(
        counts(&state, &epoch).append_requests,
        3,
        "a refusal was counted"
    );
    let close = [
        ("content-type", "application/json"),
        ("stream-closed", "true"),
    ];
    assert_eq!(hreq(addr, "POST", path, &close, b"").await.0, 204);
    appended(4);
    engine_shutdown(&state).await;
}

/// Pin: an append that committed while its handler was dropped before the
/// answer is not counted (its bytes are the committer's to bill).
#[expect(
    clippy::disallowed_methods,
    reason = "abandoned-append fixture; the handler is aborted while durability dispatch is held and joined before release; run inline the test could not drop it between the commit and its answer"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_its_handler_never_answered_is_not_counted() {
    let (state, addr) = http_rig(mem()).await;
    let desc = product_stream(&state, addr, "opcgone").await;
    let route = desc.resolve_segment("").shard_route;
    let engine = state.engine_for(&route).await.unwrap();
    let held = engine.test_hold_dispatch().await;
    let entered = engine.appends_enqueued();
    let mut headers = axum::http::HeaderMap::new();
    let key = axum::http::HeaderValue::from_static(PRISMA_KEY);
    headers.insert("prisma-encryption-key", key);
    let handler = tokio::spawn(crate::product::product_entry(
        state.clone(),
        "opcgone/records".into(),
        axum::http::Method::POST,
        headers,
        String::new(),
        bytes::Bytes::from_static(br#"{"n":1}"#),
        crate::product::ProductAuthorization::Deployment,
    ));
    let mut enqueued = false;
    for _ in 0..500 {
        if engine.appends_enqueued() > entered {
            enqueued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(enqueued, "the append never reached the committer");
    handler.abort();
    let dropped = handler.await.unwrap_err();
    assert!(
        dropped.is_cancelled(),
        "the handler answered while dispatch was held"
    );
    drop(held);
    let mut visible = false;
    for _ in 0..300 {
        let (st, _, body) = hreq(addr, "GET", "/v1/stream/opcgone?offset=-1", &[], b"").await;
        if st == 200 && String::from_utf8_lossy(&body).contains(r#""n":1"#) {
            visible = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(visible, "the abandoned append never committed");
    assert_eq!(
        counts(&state, &desc.stream_epoch).append_requests,
        0,
        "an append its handler never answered was counted"
    );
    engine_shutdown(&state).await;
}

/// Pin: a recreated name is a new incarnation, and each incarnation's
/// appends are counted on its own identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_stream_counts_each_incarnation_on_its_own_identity() {
    let (state, addr) = http_rig(mem()).await;
    let records = "/v1/streams/opcreborn/records";
    let first = product_stream(&state, addr, "opcreborn").await;
    assert_eq!(product_post(addr, records, &[], br#"{"n":1}"#).await.0, 200);
    let (st, _, _) = preq(addr, "DELETE", "/v1/streams/opcreborn", &KEY, b"").await;
    assert!(st == 200 || st == 204, "delete: {st}");
    let second = product_stream(&state, addr, "opcreborn").await;
    assert_ne!(first.stream_epoch, second.stream_epoch);
    assert_eq!(product_post(addr, records, &[], br#"{"n":2}"#).await.0, 200);
    assert_eq!(counts(&state, &first.stream_epoch).append_requests, 1);
    assert_eq!(counts(&state, &second.stream_epoch).append_requests, 1);
    engine_shutdown(&state).await;
}
