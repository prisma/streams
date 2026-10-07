#![cfg(test)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use super::{DrainFloor, DrainStream};
use crate::config::HttpConfig;

const KIB: usize = 1024;
const WINDOW: Duration = Duration::from_secs(10);

/// A socket under the production floor, and its client's end: the pipe
/// holds 4 KiB the client has not read.
fn socket() -> (DrainStream<DuplexStream>, DuplexStream) {
    let (server, client) = tokio::io::duplex(4 * KIB);
    (
        DrainStream::new(server, DrainFloor::of(&HttpConfig::default())),
        client,
    )
}

/// The client reads up to `chunk` bytes every second until it has `total`.
async fn read_every_second(client: &mut DuplexStream, chunk: usize, total: usize) {
    let mut buf = vec![0u8; chunk];
    let mut got = 0;
    while got < total {
        tokio::time::sleep(Duration::from_secs(1)).await;
        got += client.read(&mut buf).await.unwrap();
    }
}

/// `work`, failed rather than hung if it outlives an hour of paused time.
async fn bounded<F: std::future::Future>(work: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3600), work)
        .await
        .expect("finished within the bound")
}

#[test]
fn the_floor_is_16_kib_per_10_s() {
    let floor = DrainFloor::of(&HttpConfig::default());
    assert_eq!((floor.window, floor.min_bytes), (WINDOW, 16 * 1024));
}

/// A client that never reads is cut when the first window ends.
#[tokio::test(start_paused = true)]
async fn a_response_the_client_never_reads_fails_when_its_window_ends() {
    let (mut server, _client) = socket();
    let started = tokio::time::Instant::now();
    let err = bounded(server.write_all(&[1u8; 64 * KIB]))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), WINDOW);
}

/// A client reading 4 KiB a second (40 KiB a window) receives a 256 KiB
/// response whole, however long it takes.
#[tokio::test(start_paused = true)]
async fn a_client_draining_above_the_floor_receives_the_whole_response() {
    let (mut server, mut client) = socket();
    let started = tokio::time::Instant::now();
    let (written, ()) = bounded(futures_util::future::join(
        server.write_all(&[2u8; 256 * KIB]),
        read_every_second(&mut client, 4 * KIB, 256 * KIB),
    ))
    .await;
    written.unwrap();
    assert!(started.elapsed() >= Duration::from_secs(60));
}

/// A client reading 1 KiB a second (10 KiB a window) is cut when the first
/// window ends; one that met a window and then stopped is cut when the
/// next one ends.
#[tokio::test(start_paused = true)]
async fn a_client_draining_below_the_floor_is_cut_at_the_end_of_a_window() {
    let (mut server, mut client) = socket();
    let started = tokio::time::Instant::now();
    let written = Box::pin(server.write_all(&[3u8; 64 * KIB]));
    let reading = Box::pin(read_every_second(&mut client, KIB, usize::MAX));
    let err = match bounded(futures_util::future::select(written, reading)).await {
        futures_util::future::Either::Left((written, _)) => written.unwrap_err(),
        futures_util::future::Either::Right(_) => unreachable!("the client reads for ever"),
    };
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), WINDOW);

    let (mut server, mut client) = socket();
    let started = tokio::time::Instant::now();
    let reading = read_every_second(&mut client, 4 * KIB, 32 * KIB);
    let (written, ()) = bounded(futures_util::future::join(
        server.write_all(&[4u8; 256 * KIB]),
        reading,
    ))
    .await;
    assert_eq!(written.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(started.elapsed(), 2 * WINDOW);
}

/// A write the socket takes whole closes the window: a client that caught
/// up, went quiet for three windows and then read the next response 5 s
/// late is not cut for the old window.
#[tokio::test(start_paused = true)]
async fn a_client_that_catches_up_closes_its_window() {
    let (mut server, mut client) = socket();
    for _ in 0..2 {
        let late_reader = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let mut buf = vec![0u8; 8 * KIB];
            client.read_exact(&mut buf).await.unwrap();
        };
        let (written, ()) = bounded(futures_util::future::join(
            server.write_all(&[5u8; 8 * KIB]),
            late_reader,
        ))
        .await;
        written.unwrap();
        tokio::time::sleep(3 * WINDOW).await;
    }
}

/// The production serve loop with a 300 ms window: a client that sends a
/// request and never reads the 16 MiB response is disconnected and the
/// response body dropped; a client that reads it receives every byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_serve_loop_drops_a_body_its_client_never_reads() {
    use crate::tasks::{Policy, TaskResult, TaskSupervisor};
    const BODY: usize = 16 << 20;
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = dropped.clone();
    let app = axum::Router::new().route(
        "/big",
        axum::routing::get(move || {
            let body = flagged_body(Flag(flag.clone()), BODY);
            std::future::ready(([(axum::http::header::CONTENT_LENGTH, BODY)], body))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let http = HttpConfig {
        drain_window: Duration::from_millis(300),
        ..HttpConfig::default()
    };
    let tasks = TaskSupervisor::new();
    let serve_tasks = tasks.clone();
    tasks
        .spawn("drain-rig", Policy::Critical, move |_cancel| async move {
            super::super::super::serve_h1(listener, app, &http, serve_tasks)
                .await
                .ok();
            TaskResult::Done
        })
        .unwrap();
    let request = b"GET /big HTTP/1.1\r\nhost: rig\r\nconnection: close\r\n\r\n";

    let mut reader = tokio::net::TcpStream::connect(addr).await.unwrap();
    reader.write_all(request).await.unwrap();
    let mut received = Vec::new();
    reader.read_to_end(&mut received).await.unwrap();
    let head = received.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert_eq!(received.len() - head, BODY, "the reading client");
    assert!(dropped.swap(false, Ordering::SeqCst), "an ended body drops");

    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4 * 1024).unwrap();
    let mut silent = socket.connect(addr).await.unwrap();
    silent.write_all(request).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !dropped.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        dropped.load(Ordering::SeqCst),
        "the body a silent client holds is dropped"
    );
    drop(silent);
    tasks.shutdown(Duration::from_secs(5)).await;
}

/// A body of `len` bytes in 64 KiB chunks that holds `flag` until dropped.
fn flagged_body(flag: Flag, len: usize) -> axum::body::Body {
    let chunks = (0..len / (64 * KIB))
        .map(|_| Ok::<_, std::convert::Infallible>(bytes::Bytes::from(vec![6u8; 64 * KIB])));
    let guarded = futures_util::StreamExt::map(futures_util::stream::iter(chunks), move |chunk| {
        let _held = &flag;
        chunk
    });
    axum::body::Body::from_stream(guarded)
}

/// Sets its flag when dropped.
struct Flag(Arc<AtomicBool>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// An inner writer that takes vectored writes whole and records how many
/// slices each carried.
#[derive(Default)]
struct Vectored {
    slices: Vec<usize>,
}

impl tokio::io::AsyncWrite for Vectored {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.get_mut().slices.push(1);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.get_mut().slices.push(bufs.len());
        std::task::Poll::Ready(Ok(bufs.iter().map(|b| b.len()).sum()))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// The floor keeps the socket's own write shape: vectored writes reach it
/// as vectored writes (hyper keeps its queued write strategy), reads, the
/// shutdown that ends a response and the socket's options pass through.
#[tokio::test]
async fn the_floor_passes_the_sockets_own_operations_through() {
    let floor = DrainFloor::of(&HttpConfig::default());
    let mut vectored = DrainStream::new(Vectored::default(), floor);
    assert!(tokio::io::AsyncWrite::is_write_vectored(&vectored));
    let slices = [
        std::io::IoSlice::new(b"head"),
        std::io::IoSlice::new(b"body"),
    ];
    assert_eq!(vectored.write_vectored(&slices).await.unwrap(), 8);
    vectored.flush().await.unwrap();
    assert_eq!(vectored.inner.slices, vec![2]);

    let (mut server, mut client) = socket();
    client.write_all(b"request").await.unwrap();
    let mut got = [0u8; 7];
    server.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"request");
    server.write_all(b"response").await.unwrap();
    server.shutdown().await.unwrap();
    let mut answer = Vec::new();
    bounded(client.read_to_end(&mut answer)).await.unwrap();
    assert_eq!(answer, b"response", "a shutdown ends the client's read");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener = super::DrainListener::new(listener, &HttpConfig::default());
    let (_client, accepted) =
        futures_util::future::join(tokio::net::TcpStream::connect(addr), listener.accept()).await;
    let (socket, _) = accepted.unwrap();
    socket.set_nodelay(true).unwrap();
    assert!(socket.inner.nodelay().unwrap());
    socket.set_nodelay(false).unwrap();
    assert!(!socket.inner.nodelay().unwrap());
}
