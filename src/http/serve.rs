//! The h1 connection posture: what every accepted socket is served with.
//!
//! One builder, cloned per connection, so production and every rig serve
//! with the same bounded read buffer and the same request-head deadline
//! (`HttpConfig::h1_max_buf`, `HttpConfig::h1_header_timeout`).

use crate::config::HttpConfig;

/// The hyper builder every connection is served with; cloned per accept
/// (an `Arc` bump and a parser-config copy).
///
/// hyper 1.x documents a 30 s `header_read_timeout` default and drops it
/// silently unless the builder is also given a timer: `Time::check`
/// answers `None` for a defaulted duration with no timer, and its `warn!`
/// is compiled out without hyper's `tracing` feature (off in this build).
/// Without the deadline a socket that never sends a request head (a
/// suspended VM's corpse, a half-open NAT flow, a crashed client) holds
/// a descriptor and a task for ever: the L3a EMFILE wedge `raise_nofile`
/// widened but could not bound.
///
/// hyper arms the deadline when a connection first waits for a head and
/// again after every response, so it is also the idle keep-alive bound.
/// It never runs while a head is parsed, a body is read or a response is
/// written: long-polls and SSE sessions are untouched. The timer and the
/// deadline are set together: a configured deadline without a timer
/// panics inside `serve_connection`, on every connection.
///
/// `max_buf` bounds each READ chunk, not request body size (#269:
/// axum::serve's default hyper posture measured ~53 KB resident per
/// parked connection; the bounded buffer holds the same fleet at ~44 KB,
/// the floor now dominated by task/future/slab overhead).
pub(crate) fn h1_builder(http: &HttpConfig) -> hyper::server::conn::http1::Builder {
    let mut b = hyper::server::conn::http1::Builder::new();
    b.timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(http.h1_header_timeout)
        .max_buf_size(http.h1_max_buf);
    b
}

#[cfg(test)]
mod tests {
    use crate::config::HttpConfig;
    use crate::tasks::{Policy, TaskResult, TaskSupervisor};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// The rig deadline: short enough to observe, long enough that a
    /// loaded runner's scheduling gap never masquerades as idleness.
    const DEADLINE: Duration = Duration::from_secs(1);
    /// Every wait is bounded well past the deadline; a test that waits
    /// this long fails by assertion, it never hangs.
    const BOUND: Duration = Duration::from_secs(4);

    /// The production serve loop over a two-route app, on `deadline`.
    async fn serve(deadline: Duration) -> (std::net::SocketAddr, TaskSupervisor) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new()
            .route("/fast", axum::routing::get(|| async {}))
            .route(
                "/slow",
                axum::routing::get(move || async move {
                    tokio::time::sleep(deadline * 5 / 2).await;
                }),
            );
        let tasks = TaskSupervisor::new();
        let serve_tasks = tasks.clone();
        let http = HttpConfig {
            h1_header_timeout: deadline,
            ..HttpConfig::default()
        };
        tasks
            .spawn(
                "h1-posture-rig",
                Policy::Critical,
                move |_cancel| async move {
                    super::super::serve_h1(listener, app, &http, serve_tasks)
                        .await
                        .ok();
                    TaskResult::Done
                },
            )
            .unwrap();
        (addr, tasks)
    }

    /// Reads until the server closes. EOF and a reset both mean closed
    /// (a close with unread request bytes queued resets on macOS).
    async fn closed_within(sock: &mut TcpStream, bound: Duration) -> Result<(), String> {
        let mut buf = [0u8; 64];
        match tokio::time::timeout(bound, sock.read(&mut buf)).await {
            Ok(Ok(0)) => Ok(()),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => Ok(()),
            Ok(Ok(n)) => Err(format!("server wrote {n} bytes instead of closing")),
            Ok(Err(e)) => Err(format!("read: {e}")),
            Err(_) => Err(format!("still open after {bound:?}")),
        }
    }

    /// Reads one response head (the rig's handlers answer without a body).
    async fn response_head(sock: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            let n = tokio::time::timeout(BOUND, sock.read(&mut byte))
                .await
                .expect("response head within the bound")
                .unwrap();
            assert_ne!(n, 0, "closed before the response head completed");
            buf.push(byte[0]);
        }
        String::from_utf8(buf).unwrap()
    }

    /// A connection that never completes a request head is closed at the
    /// deadline: a descriptor and a task are never held for ever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn headless_connection_is_closed_at_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let started = tokio::time::Instant::now();
        let mut silent = TcpStream::connect(addr).await.unwrap();
        let mut partial = TcpStream::connect(addr).await.unwrap();
        partial
            .write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n")
            .await
            .unwrap();
        assert_eq!(
            closed_within(&mut silent, BOUND).await,
            Ok(()),
            "silent socket"
        );
        assert_eq!(
            closed_within(&mut partial, BOUND).await,
            Ok(()),
            "partial head"
        );
        let held = started.elapsed();
        assert!(
            held >= DEADLINE / 2,
            "closed on sight, not at the deadline: {held:?}"
        );
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// The deadline covers only the wait for a request head: a response
    /// that takes longer than the deadline to produce still arrives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_flight_response_outlives_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /slow HTTP/1.1\r\nhost: rig\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let head = response_head(&mut sock).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert_eq!(
            closed_within(&mut sock, BOUND).await,
            Ok(()),
            "connection: close"
        );
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// Between requests the same deadline is the idle keep-alive bound: a
    /// request inside it is served on the same connection, a connection
    /// idle past it is closed by the server.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_keep_alive_is_served_inside_and_closed_past_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        for _ in 0..2 {
            sock.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
                .await
                .unwrap();
            let head = response_head(&mut sock).await;
            assert!(head.starts_with("HTTP/1.1 200"), "{head}");
            tokio::time::sleep(DEADLINE / 10).await;
        }
        assert_eq!(
            closed_within(&mut sock, BOUND).await,
            Ok(()),
            "idle keep-alive"
        );
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// The validated floor is exactly the one hyper asserts: a buffer at
    /// the floor builds, one byte less panics. If hyper moves its private
    /// minimum either way, this fails before a config it refuses can boot.
    #[test]
    fn the_validated_buffer_floor_is_the_one_hyper_asserts() {
        let builds = |n: usize| {
            std::panic::catch_unwind(move || {
                drop(super::h1_builder(&HttpConfig {
                    h1_max_buf: n,
                    ..HttpConfig::default()
                }));
            })
            .is_ok()
        };
        assert!(
            builds(HttpConfig::MIN_H1_MAX_BUF),
            "hyper refuses the validated floor"
        );
        assert!(
            !builds(HttpConfig::MIN_H1_MAX_BUF - 1),
            "hyper accepts less than the validated floor"
        );
    }
}
