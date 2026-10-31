//! Forwarding to a stream's owner: a data-plane request that reaches an
//! instance which does not own its stream's shard, and that the instance
//! answers `409 not_ring_owner`, is sent once to the owner, when the fleet has published the owner's URL, and answered with
//! the owner's response. A forwarded request is never forwarded again, and
//! without the owner's URL, or with the owner unreachable, the instance
//! answers its `409 not_ring_owner` as before.
//!
//! Two instances over one store: the ring gives the rig's shards to B, and
//! requests arrive at A.

use super::fixture_http::{engine_shutdown, http_rig_named, http_rig_named_at};
use super::fixture_livefeed::{hub_sse_collect, lf_connect};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use std::collections::HashMap;
use std::sync::Arc;

type State = Arc<crate::http::AppState>;
type Addr = std::net::SocketAddr;

const CT: (&str, &str) = ("content-type", "application/json");

struct Pair {
    a: State,
    addr_a: Addr,
    b: State,
    addr_b: Addr,
}

/// The ring {inst-a, inst-b} is active and gives both of the rig's shards
/// to `owner`: "00", and "" for a route outside it.
fn ring(state: &State, owner: &str) {
    let members = ["inst-a", "inst-b"].map(str::to_string).to_vec();
    state.ownership.set_ring_active(members);
    state.ownership.set_override("00", owner);
    state.ownership.set_override("", owner);
}

/// A receives the requests, B owns the shards; A knows B's URL.
async fn pair() -> Pair {
    let store = mem();
    let (a, addr_a) = http_rig_named(store.clone(), "inst-a").await;
    let (b, addr_b) = http_rig_named_at(store, "inst-b", RigRuntime::incarnation(1)).await;
    ring(&a, "inst-b");
    ring(&b, "inst-b");
    a.peer.set_peer("inst-b", &format!("http://{addr_b}"));
    Pair {
        a,
        addr_a,
        b,
        addr_b,
    }
}

async fn create(addr: Addr, name: &str) {
    let (status, _, body) = hreq(addr, "PUT", &format!("/v1/stream/{name}"), &[CT], b"").await;
    assert!(
        status == 200 || status == 201,
        "create {name}: {status} {}",
        String::from_utf8_lossy(&body)
    );
}

/// The records the owner holds for `name`, read at the owner itself.
async fn records_at(addr: Addr, name: &str) -> Vec<serde_json::Value> {
    let (status, _, body) = hreq(
        addr,
        "GET",
        &format!("/v1/stream/{name}"),
        &[("streams-forwarded", "test")],
        b"",
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).expect("a JSON array of records")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_non_owner_forwards_an_append_to_the_owner_once() {
    let p = pair().await;
    create(p.addr_b, "fwd1").await;

    let (status, headers, body) =
        hreq(p.addr_a, "POST", "/v1/stream/fwd1", &[CT], br#"[{"n":1}]"#).await;
    assert!(
        status == 200 || status == 204,
        "the owner answers the forwarded append: {status} {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        headers.get("streams-forwarded-to").map(String::as_str),
        Some("inst-b"),
        "the answer names the owner that served it"
    );
    assert_eq!(
        records_at(p.addr_b, "fwd1").await,
        vec![serde_json::json!({"n": 1})],
        "the owner holds the record exactly once"
    );

    // A non-owner already serves reads through the owner's segment-read
    // relay, so a read needs no forward and is answered here.
    let (status, _, body) = hreq(p.addr_a, "GET", "/v1/stream/fwd1", &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let read: Vec<serde_json::Value> = serde_json::from_slice(&body).expect("records");
    assert_eq!(read, vec![serde_json::json!({"n": 1})]);
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_non_owner_forwards_a_product_append_to_the_owner() {
    let p = pair().await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let format = br#"{"format":{"kind":"bytes"}}"#;
    let (status, _, body) = preq(p.addr_b, "PUT", "/v1/streams/fwdp", &key, format).await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));

    let (status, headers, body) =
        preq(p.addr_a, "POST", "/v1/streams/fwdp/records", &key, b"x").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-forwarded-to").map(String::as_str),
        Some("inst-b")
    );
    let (status, headers, body) =
        preq(p.addr_b, "POST", "/v1/streams/fwdp/records", &key, b"y").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-forwarded-to"),
        None,
        "the owner answers its own requests"
    );
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_non_owner_forwards_a_create_to_the_owner() {
    let p = pair().await;
    let (status, headers, body) = hreq(p.addr_a, "PUT", "/v1/stream/fwd0", &[CT], b"").await;
    assert!(
        status == 200 || status == 201,
        "the owner answers the forwarded create: {status} {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        headers.get("streams-forwarded-to").map(String::as_str),
        Some("inst-b")
    );
    assert!(
        records_at(p.addr_b, "fwd0").await.is_empty(),
        "the owner holds the new, empty stream"
    );
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_forwarded_request_is_answered_with_its_409_and_never_forwarded_again() {
    let p = pair().await;
    create(p.addr_b, "fwd2").await;

    let (status, headers, body) = hreq(
        p.addr_a,
        "POST",
        "/v1/stream/fwd2",
        &[CT, ("streams-forwarded", "inst-c")],
        br#"[{"n":1}]"#,
    )
    .await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("inst-b")
    );
    assert!(String::from_utf8_lossy(&body).contains("not_ring_owner"));
    assert!(
        records_at(p.addr_b, "fwd2").await.is_empty(),
        "a refused forward writes nothing"
    );
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn without_the_owners_url_a_non_owner_answers_409() {
    let p = pair().await;
    create(p.addr_b, "fwd3").await;
    p.a.peer.set_peers(HashMap::new());

    let (status, headers, body) =
        hreq(p.addr_a, "POST", "/v1/stream/fwd3", &[CT], br#"[{"n":1}]"#).await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("inst-b")
    );
    assert!(records_at(p.addr_b, "fwd3").await.is_empty());
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_unreachable_owner_leaves_the_non_owner_answering_409() {
    let p = pair().await;
    create(p.addr_b, "fwd4").await;
    let closed = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a port");
    let unreachable = closed.local_addr().expect("its address");
    drop(closed);
    p.a.peer
        .set_peer("inst-b", &format!("http://{unreachable}"));

    let (status, headers, body) =
        hreq(p.addr_a, "POST", "/v1/stream/fwd4", &[CT], br#"[{"n":1}]"#).await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("inst-b")
    );
    assert!(records_at(p.addr_b, "fwd4").await.is_empty());
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_body_read_past_the_kept_bound_keeps_its_409() {
    let p = pair().await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let format = br#"{"format":{"kind":"bytes"}}"#;
    let (status, _, body) = preq(p.addr_b, "PUT", "/v1/streams/fwdk", &key, format).await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));

    // Within the bound, the record is forwarded; one byte past it, the
    // handler has read what cannot be sent again.
    let within = vec![b'k'; 64 << 10];
    let (status, _, body) = preq(p.addr_a, "POST", "/v1/streams/fwdk/records", &key, &within).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let past = vec![b'k'; (64 << 10) + 1];
    let (status, headers, body) =
        preq(p.addr_a, "POST", "/v1/streams/fwdk/records", &key, &past).await;
    assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("inst-b")
    );
    engine_shutdown(&p.b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_non_owner_forwards_a_live_feed_connect_to_the_owner() {
    let p = pair().await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let format = br#"{"format":{"kind":"bytes"}}"#;
    let (status, _, body) = preq(p.addr_b, "PUT", "/v1/streams/fwdl", &key, format).await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&body));

    let mut feed = lf_connect(p.addr_a, "fwdl", "?cursor=beginning").await;
    let (head, _) = hub_sse_collect(&mut feed, 15, |t| t.contains("\r\n\r\n")).await;
    let head = head.to_ascii_lowercase();
    assert!(
        head.starts_with("http/1.1 200"),
        "the owner serves the feed:\n{head}"
    );
    assert!(
        head.contains("streams-forwarded-to: inst-b"),
        "a non-owner forwards the connect to the owner:\n{head}"
    );
    drop(feed);
    engine_shutdown(&p.b).await;
}
