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

/// An owner stand-in at `listener`: accept one connection, read the
/// request on it whole, and hand the connection back unanswered.
async fn accept_unanswered(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    let (mut socket, _) = listener.accept().await.expect("accept a forward");
    read_request(&mut socket).await;
    socket
}

/// A loopback listener for an owner stand-in, and its URL.
async fn owner_stand_in() -> (tokio::net::TcpListener, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let url = format!("http://{}", listener.local_addr().expect("its address"));
    (listener, url)
}

/// Read one request's head and its body, sized or chunked.
async fn read_request(socket: &mut tokio::net::TcpStream) {
    use tokio::io::AsyncReadExt;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let head = buf.windows(4).position(|w| w == b"\r\n\r\n");
        if let Some(end) = head {
            let text = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            let length = text
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok());
            let whole = match length {
                Some(n) => buf.len() >= end + 4 + n,
                None => !text.contains("chunked") || buf.ends_with(b"0\r\n\r\n"),
            };
            if whole {
                return;
            }
        }
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
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
    let (_, at_owner, _) = hreq(
        p.addr_b,
        "GET",
        "/v1/stream/fwd1",
        &[("streams-forwarded", "test")],
        b"",
    )
    .await;
    assert_eq!(
        headers.get("stream-next-offset"),
        at_owner.get("stream-next-offset"),
        "the owner's answer is relayed with its headers: the append's next offset"
    );
    assert!(at_owner.contains_key("stream-next-offset"));

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
        (
            headers.get("streams-forwarded-to").map(String::as_str),
            headers.get("content-type").map(String::as_str),
        ),
        (Some("inst-b"), Some("application/json")),
        "the owner's JSON answer is relayed with its content type"
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

/// The owner a non-owner forwards to may itself have lost the stream: B's
/// ring says C owns it. B answers the forwarded request with its own 409
/// and never forwards it again, and A relays that 409 as B answered it, so
/// the client sees the owner B names in `Streams-Replay-To` and replays
/// there; `Streams-Forwarded-To` names B, which answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_second_owner_bounce_is_relayed_with_its_replay_target() {
    let p = pair().await;
    create(p.addr_b, "fwd5").await;
    let members = ["inst-a", "inst-b", "inst-c"].map(str::to_string).to_vec();
    p.b.ownership.set_ring_active(members);
    p.b.ownership.set_override("00", "inst-c");
    p.b.ownership.set_override("", "inst-c");

    let (status, headers, body) =
        hreq(p.addr_a, "POST", "/v1/stream/fwd5", &[CT], br#"[{"n":1}]"#).await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(status, 409, "{body}");
    assert_eq!(
        (
            headers.get("streams-replay-to").map(String::as_str),
            headers.get("streams-forwarded-to").map(String::as_str),
            headers.get("content-type").map(String::as_str),
        ),
        (Some("inst-c"), Some("inst-b"), Some("application/json"))
    );
    assert!(
        body.contains("not_ring_owner") && body.contains("inst-c"),
        "{body}"
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

/// An owner that read the forwarded append and closed the connection
/// without answering may have applied it: the non-owner answers exactly
/// `503 forward_failed` with `Retry-After: 1`, not marked retryable, and
/// never the `409` with `Streams-Replay-To` a router would replay as a new
/// append. The owner stand-in read the request once: nothing retried it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn an_owner_that_drops_a_forwarded_request_unanswered_is_answered_503() {
    let p = pair().await;
    create(p.addr_b, "fwd6").await;
    let (owner, url) = owner_stand_in().await;
    p.a.peer.set_peer("inst-b", &url);

    let ((status, headers, body), ()) = tokio::join!(
        hreq(p.addr_a, "POST", "/v1/stream/fwd6", &[CT], br#"[{"n":1}]"#),
        async { drop(accept_unanswered(&owner).await) },
    );
    assert_eq!(
        (
            status,
            serde_json::from_slice::<serde_json::Value>(&body).expect("a JSON error")["error"]
                .clone(),
        ),
        (
            503,
            serde_json::json!({
                "code": "forward_failed",
                "message": "the stream's owner did not answer the forwarded request; retry",
            })
        )
    );
    assert_eq!(
        (
            headers.get("retry-after").map(String::as_str),
            headers.get("streams-replay-to"),
            headers.get("streams-forwarded-to"),
        ),
        (Some("1"), None, None)
    );
    let again = tokio::time::timeout(std::time::Duration::from_millis(200), owner.accept()).await;
    assert!(again.is_err(), "the request was sent once, never again");
    assert!(records_at(p.addr_b, "fwd6").await.is_empty());
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
