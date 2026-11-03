#![cfg(test)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::super::tests::{BOUND, DEADLINE, get, http2, response_head, serve, serve_app};

/// The connection preface of an HTTP/2 client with prior knowledge and its
/// empty SETTINGS frame.
const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0";

/// One HTTP/2 frame as the server wrote it.
#[derive(Debug, PartialEq, Eq)]
struct Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

const SETTINGS: u8 = 0x4;
const GOAWAY: u8 = 0x7;

/// A raw connection that has sent the client preface.
async fn raw_http2(addr: std::net::SocketAddr) -> TcpStream {
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(CLIENT_PREFACE).await.unwrap();
    sock
}

/// What the server wrote next.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    Frame(Frame),
    Closed,
    Silent,
}

/// The next frame the server writes, its close, or its silence for the
/// bound.
async fn next_frame(sock: &mut TcpStream) -> Next {
    let mut head = [0u8; 9];
    match tokio::time::timeout(BOUND, sock.read_exact(&mut head)).await {
        Err(_) => return Next::Silent,
        Ok(Ok(_)) => {}
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Next::Closed,
        Ok(Err(e)) => panic!("read: {e}"),
    }
    let len = usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
    let mut payload = vec![0u8; len];
    tokio::time::timeout(BOUND, sock.read_exact(&mut payload))
        .await
        .expect("the payload within the bound")
        .unwrap();
    Next::Frame(Frame {
        kind: head[3],
        flags: head[4],
        stream: u32::from_be_bytes([head[5], head[6], head[7], head[8]]) & 0x7fff_ffff,
        payload,
    })
}

/// Every frame the server writes until it closes or falls silent, at most
/// `most`, and what ended them.
async fn frames_until_close(sock: &mut TcpStream, most: usize) -> (Vec<Frame>, Next) {
    let mut frames = Vec::new();
    while frames.len() < most {
        match next_frame(sock).await {
            Next::Frame(frame) => frames.push(frame),
            end => return (frames, end),
        }
    }
    (frames, next_frame(sock).await)
}

/// One HTTP/2 connection the rig serves, its place certainly held: one
/// request on it was answered. The connection future is kept, unpolled,
/// so the socket stays open.
async fn served_http2(
    addr: std::net::SocketAddr,
) -> Option<(
    hyper::client::conn::http2::SendRequest<axum::body::Body>,
    impl std::future::Future + Unpin,
)> {
    let (mut send, mut conn) = http2(addr).await;
    let answer = tokio::time::timeout(
        BOUND,
        futures_util::future::select(Box::pin(send.send_request(get(addr, "/fast"))), &mut conn),
    )
    .await
    .expect("an answer or the end within the bound");
    match answer {
        futures_util::future::Either::Left((Ok(answer), _)) if answer.status() == 200 => {
            Some((send, conn))
        }
        _ => None,
    }
}

/// The 257th HTTP/2 connection on a listener is answered with an empty
/// SETTINGS frame and a GOAWAY that processed no stream, then closed;
/// HTTP/1.1 is served beside the full cap, and a closed HTTP/2 connection
/// frees its place for the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_http2_connection_past_the_cap_is_refused_with_goaway_before_any_stream() {
    let (addr, tasks) = serve(BOUND).await;
    let mut held = Vec::new();
    for n in 0..256 {
        let served = served_http2(addr).await;
        held.push(served.unwrap_or_else(|| panic!("connection {n} is served")));
    }
    let mut refused = raw_http2(addr).await;
    let written = frames_until_close(&mut refused, 8).await;
    let mut goaway = vec![0, 0, 0, 0, 0, 0, 0, 0x7];
    goaway.extend_from_slice(b"http2_connection_cap");
    assert_eq!(
        written,
        (
            vec![
                Frame {
                    kind: SETTINGS,
                    flags: 0,
                    stream: 0,
                    payload: Vec::new(),
                },
                Frame {
                    kind: GOAWAY,
                    flags: 0,
                    stream: 0,
                    payload: goaway,
                },
            ],
            Next::Closed
        ),
        "the 257th connection is refused, then closed"
    );
    // The server still reads the refused connection after its GOAWAY: what
    // the peer sends then is drained, not answered with a reset, which
    // would fail the second write.
    let ping = b"\0\0\x08\x06\0\0\0\0\0pingpong";
    let first = refused.write_all(ping).await.map_err(|e| e.kind());
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = refused.write_all(ping).await.map_err(|e| e.kind());
    assert_eq!((first, second), (Ok(()), Ok(())), "read during the linger");
    let mut h1 = TcpStream::connect(addr).await.unwrap();
    h1.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
        .await
        .unwrap();
    let head = response_head(&mut h1).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    drop(held.pop());
    let started = tokio::time::Instant::now();
    let mut next = None;
    while next.is_none() && started.elapsed() < BOUND {
        next = served_http2(addr).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(next.is_some(), "a closed connection's place is free");
    tasks.shutdown(Duration::from_secs(5)).await;
}

const WINDOW_UPDATE: u8 = 0x8;

/// An HTTP/2 connection's receive window is 4 MiB: the server raises it
/// from the protocol's 65,535 bytes with one WINDOW_UPDATE on the
/// connection, so a peer may send at most 4 MiB of request bodies the
/// handlers have not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_http2_connection_window_is_four_mib() {
    let (addr, tasks) = serve(BOUND).await;
    let mut sock = raw_http2(addr).await;
    let mut increments = Vec::new();
    // The server's SETTINGS, its ACK of the client's and the update.
    for _ in 0..3 {
        let Next::Frame(frame) = next_frame(&mut sock).await else {
            break;
        };
        if frame.kind == WINDOW_UPDATE && frame.stream == 0 {
            increments.push(u32::from_be_bytes(frame.payload[..4].try_into().unwrap()));
        }
    }
    assert_eq!(increments, vec![(4 << 20) - 65_535]);
    tasks.shutdown(Duration::from_secs(5)).await;
}

const HEADERS: u8 = 0x1;

/// The rig's routes for the idle bound: `/drip` answers at once and sends
/// its body over 2.5 deadlines, `/text` answers four bytes.
fn dripping() -> axum::Router {
    let drip = || async {
        let parts = futures_util::stream::unfold(0u8, |sent| async move {
            match sent {
                0 => Some((
                    Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"a")),
                    1,
                )),
                1 => {
                    tokio::time::sleep(DEADLINE * 5 / 2).await;
                    Some((Ok(axum::body::Bytes::from_static(b"b")), 2))
                }
                _ => None,
            }
        });
        axum::body::Body::from_stream(parts)
    };
    axum::Router::new()
        .route("/fast", axum::routing::get(|| async {}))
        .route("/drip", axum::routing::get(drip))
        .route("/text", axum::routing::get(|| async { "text" }))
}

/// An HTTP/2 connection that serves no stream for the head deadline is
/// shut down gracefully, one that never opened a stream and one whose
/// stream has ended alike; its peer sees a clean close, as an idle
/// HTTP/1.1 keep-alive closes at the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_http2_connection_with_no_stream_is_shut_down_at_the_deadline() {
    let (addr, tasks) = serve(DEADLINE).await;
    let started = tokio::time::Instant::now();
    let (_never, never) = http2(addr).await;
    let (mut send, mut served) = http2(addr).await;
    let answer = tokio::time::timeout(
        BOUND,
        futures_util::future::select(Box::pin(send.send_request(get(addr, "/fast"))), &mut served),
    )
    .await
    .expect("the answer within the bound");
    let futures_util::future::Either::Left((Ok(answer), _)) = answer else {
        panic!("the connection ended before its answer");
    };
    assert_eq!(answer.status(), 200);
    let (never, served) = futures_util::future::join(
        tokio::time::timeout(BOUND, never),
        tokio::time::timeout(BOUND, served),
    )
    .await;
    let held = started.elapsed();
    assert_eq!(
        (
            never.map_err(|_| "still open").map(|c| c.is_ok()),
            served.map_err(|_| "still open").map(|c| c.is_ok())
        ),
        (Ok(true), Ok(true)),
        "both connections are closed cleanly"
    );
    assert!(
        held >= DEADLINE / 2,
        "closed on sight, not at the deadline: {held:?}"
    );
    tasks.shutdown(Duration::from_secs(5)).await;
}

/// A stream counts until its response body is done: a response that
/// streams for 2.5 deadlines keeps its connection serving, which then
/// answers its next request, and closes a deadline after that one.
/// HTTP/1.1 responses keep their length on the way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_http2_connection_is_kept_while_a_response_streams_past_the_deadline() {
    let (addr, tasks) = serve_app(DEADLINE, dripping()).await;
    let (mut send, mut conn) = http2(addr).await;
    let mut drip = send.clone();
    let dripped = async move {
        let answer = drip.send_request(get(addr, "/drip")).await.unwrap();
        axum::body::to_bytes(axum::body::Body::new(answer.into_body()), 64)
            .await
            .unwrap()
    };
    let dripped = tokio::time::timeout(
        BOUND,
        futures_util::future::select(Box::pin(dripped), &mut conn),
    )
    .await
    .expect("the body within the bound");
    let futures_util::future::Either::Left((body, _)) = dripped else {
        panic!("the connection ended before the body");
    };
    assert_eq!(&body[..], b"ab");
    let next = tokio::time::timeout(
        BOUND,
        futures_util::future::select(Box::pin(send.send_request(get(addr, "/fast"))), &mut conn),
    )
    .await
    .expect("the answer within the bound");
    let futures_util::future::Either::Left((next, _)) = next else {
        panic!("the connection ended before its next answer");
    };
    assert_eq!(
        next.expect("served, not refused after a GOAWAY").status(),
        200
    );
    let idle = tokio::time::Instant::now();
    let closed = tokio::time::timeout(BOUND, conn).await;
    assert!(
        matches!(closed, Ok(Ok(()))),
        "closed cleanly within the bound: {closed:?}"
    );
    let held = idle.elapsed();
    assert!(
        held >= DEADLINE / 2,
        "closed on sight, not at the deadline: {held:?}"
    );
    let mut h1 = TcpStream::connect(addr).await.unwrap();
    h1.write_all(b"GET /text HTTP/1.1\r\nhost: rig\r\n\r\n")
        .await
        .unwrap();
    let head = response_head(&mut h1).await;
    assert!(
        head.to_ascii_lowercase().contains("content-length: 4\r\n"),
        "{head}"
    );
    tasks.shutdown(Duration::from_secs(5)).await;
}

/// A response with no body ends its stream with its HEADERS frame: the
/// count a stream keeps does not hide that its body is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn an_empty_http2_answer_ends_its_stream_with_its_headers() {
    let (addr, tasks) = serve(BOUND).await;
    let mut sock = raw_http2(addr).await;
    // HEADERS on stream 1, END_STREAM | END_HEADERS: GET http / (HPACK
    // static table entries 2, 6 and 4), which the rig answers 404.
    sock.write_all(b"\0\0\x03\x01\x05\0\0\0\x01\x82\x86\x84")
        .await
        .unwrap();
    let mut answer = None;
    for _ in 0..4 {
        let Next::Frame(frame) = next_frame(&mut sock).await else {
            break;
        };
        if frame.stream == 1 {
            answer = Some((frame.kind, frame.flags));
            break;
        }
    }
    assert_eq!(
        answer,
        Some((HEADERS, 0x5)),
        "HEADERS with END_STREAM and END_HEADERS"
    );
    tasks.shutdown(Duration::from_secs(5)).await;
}

/// A counted response keeps its body's length: hyper sets an HTTP/2
/// answer's content-length from the exact size hint, so the count a stream
/// keeps must not hide it.
#[test]
fn a_counted_body_keeps_its_exact_length() {
    use hyper::body::Body as _;
    let streams = super::Streams::new();
    let counted = super::Counting {
        body: axum::body::Body::from("hello"),
        _open: streams.open(),
    };
    assert_eq!(counted.size_hint().exact(), Some(5), "the body's length");
    assert!(!counted.is_end_stream(), "five bytes are still to come");
}
