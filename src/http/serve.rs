//! The h1 serve loop and its connection posture: what every accepted socket is served with.
//!
//! One builder, cloned per connection, so production and every rig serve
//! with the same bounded read buffer and the same request-head deadline
//! (`HttpConfig::h1_max_buf`, `HttpConfig::h1_header_timeout`).

use crate::config::HttpConfig;

use super::{NOFILE_HARD, NOFILE_SOFT, raise_nofile, spawn_runtime_watchdog};

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

/// A reaped connection's verdict (item 37). hyper does not catch a panic in
/// the service or its response body, so a panicking handler unwinds the
/// connection task and its client sees only a closed socket: this
/// JoinError is the one place the server learns of it. A cancelled task
/// is the shutdown's own abort, not a fault.
fn reap(tasks: &crate::tasks::TaskSupervisor, joined: Result<(), tokio::task::JoinError>) {
    match joined {
        Err(error) if error.is_panic() => {
            tracing::error!("connection task panicked; its request got no response: {error}");
            tasks.record_connection_panic();
        }
        Ok(()) | Err(_) => {}
    }
}

/// #269: the one h1 serve entry — production and every test rig serve
/// through THIS function, so the suite exercises the real connection
/// path; what each connection is served with is `serve::h1_builder`, and
/// every response it serves passes `challenge`.
pub(crate) async fn serve_h1(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    http: &crate::config::HttpConfig,
    tasks: crate::tasks::TaskSupervisor,
) -> std::io::Result<()> {
    let app = app.layer(axum::middleware::map_response(challenge));
    serve_connections(listener, app, http, tasks).await
}

/// RFC 9110 §15.5.2: a 401 names how to authenticate. Every credential the
/// server takes, on the product, raw and operator surfaces, is a bearer
/// token (RFC 6750). A challenge a handler already set is kept.
async fn challenge(mut response: axum::response::Response) -> axum::response::Response {
    if response.status() == axum::http::StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .entry(axum::http::header::WWW_AUTHENTICATE)
            .or_insert(axum::http::HeaderValue::from_static(
                "Bearer realm=\"streams\"",
            ));
    }
    response
}

/// The accept loop behind `serve_h1`.
#[expect(
    clippy::disallowed_methods,
    clippy::let_underscore_must_use,
    reason = "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns, reaps (counting a panicked one) and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled connection results would restate what the JoinSet already owns"
)]
async fn serve_connections(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    http: &crate::config::HttpConfig,
    tasks: crate::tasks::TaskSupervisor,
) -> std::io::Result<()> {
    let svc = hyper_util::service::TowerToHyperService::new(app);
    let h1 = h1_builder(http);
    let limits = raise_nofile();
    let (soft, hard) = (
        limits.soft.map_or(0, |n| n.get()),
        limits.hard.map_or(0, |n| n.get()),
    );
    NOFILE_SOFT.store(soft, std::sync::atomic::Ordering::Relaxed);
    NOFILE_HARD.store(hard, std::sync::atomic::Ordering::Relaxed);
    tracing::info!("nofile soft={soft} hard={hard} (raised to hard at boot)");
    spawn_runtime_watchdog(&tasks);
    // PR 6.1-A: the accept loop OWNS its connections. A connection is
    // not request-scoped — keep-alives and live subscriptions outlive
    // any one request — so on cancellation the loop stops accepting,
    // releases the address, then aborts and JOINS every connection: a
    // runtime that has shut down has no socket left open, and a
    // replacement can bind the same address immediately.
    let cancel = tasks.cancellation();
    let mut conns = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((sock, _peer)) => {
                    let svc = svc.clone();
                    let h1 = h1.clone();
                    conns.spawn(async move {
                        let _ = sock.set_nodelay(true);
                        let io = hyper_util::rt::TokioIo::new(sock);
                        // Errors here are routine client behavior (resets,
                        // half-closed keep-alives, head deadlines), not
                        // server faults.
                        let _ = h1.serve_connection(io, svc).await;
                    });
                }
                Err(e) => {
                    // Transient accept errors (EMFILE bursts, aborted
                    // handshakes) must not kill the acceptor.
                    tracing::warn!("accept: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            },
            // Reap finished connections so the set never grows with
            // completed entries.
            Some(joined) = conns.join_next(), if !conns.is_empty() => reap(&tasks, joined),
        }
    }
    drop(listener);
    conns.abort_all();
    while let Some(joined) = conns.join_next().await {
        reap(&tasks, joined);
    }
    Ok(())
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

    /// A handler that panics mid-request: neither axum nor hyper catches the
    /// unwind, so it ends the connection task.
    async fn panicking_handler() -> &'static str {
        panic!("scripted handler panic")
    }

    /// The production serve loop over the rig's routes, on `deadline`.
    async fn serve(deadline: Duration) -> (std::net::SocketAddr, TaskSupervisor) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new()
            .route("/fast", axum::routing::get(|| async {}))
            .route("/hang", axum::routing::get(std::future::pending::<()>))
            .route("/panic", axum::routing::get(panicking_handler))
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

    /// Item 37 (A): a panicking handler ends its connection with no response,
    /// and the accept loop that reaps the task counts it on the runtime's task
    /// record exactly once. A connection the shutdown aborts mid-request is
    /// cancelled, not panicked, and is not counted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_handler_is_counted_once_and_an_aborted_connection_is_not() {
        let (addr, tasks) = serve(DEADLINE).await;
        // Served once (so it is certainly accepted), then parked in a handler
        // that never answers: only the shutdown's abort ends it.
        let mut hung = TcpStream::connect(addr).await.unwrap();
        hung.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        let head = response_head(&mut hung).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        hung.write_all(b"GET /hang HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        let mut panicked = TcpStream::connect(addr).await.unwrap();
        panicked
            .write_all(b"GET /panic HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        assert_eq!(
            closed_within(&mut panicked, BOUND).await,
            Ok(()),
            "a panicked request is answered by a closed socket"
        );
        tasks.shutdown(Duration::from_secs(5)).await;
        assert_eq!(
            closed_within(&mut hung, BOUND).await,
            Ok(()),
            "the shutdown aborts and joins the parked connection"
        );
        assert_eq!(
            tasks.monitor().connection_panics(),
            1,
            "one panicked connection is counted; the aborted one is not"
        );
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

    /// A 401 gains the bearer challenge; any other status does not, and a
    /// challenge a handler already set is kept.
    #[tokio::test]
    async fn only_a_401_is_challenged_and_an_existing_challenge_is_kept() {
        use axum::http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE};
        use axum::response::IntoResponse;
        let challenged = super::challenge(StatusCode::UNAUTHORIZED.into_response()).await;
        assert_eq!(
            challenged.headers().get(WWW_AUTHENTICATE),
            Some(&HeaderValue::from_static("Bearer realm=\"streams\""))
        );
        for status in [StatusCode::OK, StatusCode::FORBIDDEN, StatusCode::NOT_FOUND] {
            let response = super::challenge(status.into_response()).await;
            assert_eq!(response.headers().get(WWW_AUTHENTICATE), None, "{status}");
        }
        let mut own = StatusCode::UNAUTHORIZED.into_response();
        own.headers_mut()
            .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Basic"));
        let kept = super::challenge(own).await;
        assert_eq!(
            kept.headers().get(WWW_AUTHENTICATE),
            Some(&HeaderValue::from_static("Basic"))
        );
    }
}
