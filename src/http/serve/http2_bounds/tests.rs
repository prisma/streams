#![cfg(test)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::super::tests::{BOUND, get, http2, response_head, serve};

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
