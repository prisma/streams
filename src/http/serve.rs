//! The serve loop and its connection posture: what every accepted socket is served with,
//! HTTP/1.1 or HTTP/2 with prior knowledge.
//!
//! One builder, cloned per connection, so production and every rig serve
//! with the same bounded read buffer and the same request-head deadline
//! (`HttpConfig::h1_max_buf`, `HttpConfig::h1_header_timeout`).

use crate::config::HttpConfig;

use super::{NOFILE_HARD, NOFILE_SOFT, raise_nofile, spawn_runtime_watchdog};

mod drain;
mod http2_bounds;

/// The hyper builder every connection is served with; cloned per accept
/// (an `Arc` bump and a parser-config copy). A connection that opens with
/// the HTTP/2 connection preface is served HTTP/2, any other HTTP/1.1, on
/// the one listener.
///
/// HTTP/1.1:
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
///
/// HTTP/2 (a platform that carries many clients' requests on a few
/// connections, as Habanero does to an instance that binds `http2`): at
/// most [`HTTP2_STREAMS`] requests at once per connection, and a
/// connection window of [`HTTP2_CONNECTION_WINDOW`]. h2 closes a connection
/// with GOAWAY ENHANCE_YOUR_CALM once the small DATA frames it holds unread
/// pass half the connection window; at hyper's default of 1 MiB, thousands
/// of small append and send bodies in flight pass it. A keep-alive ping
/// every `h1_header_timeout` closes a connection whose peer stopped
/// answering, as the head deadline closes a silent HTTP/1.1 socket.
pub(crate) fn connection_builder(
    http: &HttpConfig,
) -> hyper_util::server::conn::auto::Builder<hyper_util::rt::TokioExecutor> {
    let mut b = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    b.http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(http.h1_header_timeout)
        .max_buf_size(http.h1_max_buf);
    b.http2()
        .timer(hyper_util::rt::TokioTimer::new())
        .max_concurrent_streams(HTTP2_STREAMS)
        .initial_connection_window_size(HTTP2_CONNECTION_WINDOW)
        .keep_alive_interval(Some(http.h1_header_timeout))
        .keep_alive_timeout(http.h1_header_timeout);
    b
}

/// What each accepted connection is served with: the builder, the head
/// deadline its protocol is read under (`serve_one`), and the listener's
/// places for HTTP/2 connections.
#[derive(Clone)]
struct Posture {
    builder: hyper_util::server::conn::auto::Builder<hyper_util::rt::TokioExecutor>,
    head: std::time::Duration,
    http2: http2_bounds::Http2Connections,
}

impl Posture {
    fn of(http: &HttpConfig) -> Self {
        Self {
            builder: connection_builder(http),
            head: http.h1_header_timeout,
            http2: http2_bounds::Http2Connections::new(),
        }
    }
}

/// The most requests one HTTP/2 connection carries at once.
pub(crate) const HTTP2_STREAMS: u32 = 1_024;

/// The receive window of each HTTP/2 connection: request bodies its
/// handlers have not read, at most 4 MiB, and h2's budget for small DATA
/// frames held unread, half of it.
pub(crate) const HTTP2_CONNECTION_WINDOW: u32 = 4 << 20;

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

/// The accept queue every served listener listens with. Tokio (through
/// mio) binds with the standard library's 128, and a burst of more
/// simultaneous connects than the queue holds has the rest of its SYNs
/// dropped: each of those clients waits out the 1 s initial retransmit,
/// and longer while the burst lasts (the shared-cell flood test's 256
/// connects showed it on Linux). The kernel caps the request at
/// `net.core.somaxconn` (4,096 by default since Linux 5.4; macOS's
/// `kern.ipc.somaxconn` is 128).
const ACCEPT_BACKLOG: u32 = 1024;

/// `listener`'s own socket, listening again with `ACCEPT_BACKLOG`.
/// listen(2) on a listening socket changes only its queue length, so the
/// socket keeps its address, the options it was bound with
/// (`SO_REUSEADDR`) and every connection already queued. Tokio takes a
/// socket to configure only as an unconnected `std::net::TcpStream`; the
/// descriptor passes through one, owned throughout, and is registered
/// with the runtime again as a listener.
fn with_accept_backlog(
    listener: tokio::net::TcpListener,
) -> std::io::Result<tokio::net::TcpListener> {
    let socket = std::os::fd::OwnedFd::from(listener.into_std()?);
    tokio::net::TcpSocket::from_std_stream(std::net::TcpStream::from(socket)).listen(ACCEPT_BACKLOG)
}

/// #269: the one serve entry, HTTP/1.1 and HTTP/2 with prior knowledge on
/// one listener — production and every test rig serve through THIS
/// function, so the suite exercises the real connection path; what each
/// connection is served with is `serve::connection_builder` over
/// a socket under the drain floor (`drain`), and every response it serves
/// passes `challenge`. Production and every rig bind with Tokio's
/// default queue and hand the listener here, which listens again with
/// `ACCEPT_BACKLOG` before its first accept.
pub(crate) async fn serve_h1(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    http: &crate::config::HttpConfig,
    tasks: crate::tasks::TaskSupervisor,
) -> std::io::Result<()> {
    let listener = with_accept_backlog(listener)?;
    let app = app.layer(axum::middleware::map_response(challenge));
    serve_connections(drain::DrainListener::new(listener, http), app, http, tasks).await
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
    listener: drain::DrainListener,
    app: axum::Router,
    http: &crate::config::HttpConfig,
    tasks: crate::tasks::TaskSupervisor,
) -> std::io::Result<()> {
    let svc = hyper_util::service::TowerToHyperService::new(app);
    let posture = Posture::of(http);
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
    // releases the address, then drains and JOINS every connection
    // (`drain`): a runtime that has shut down has no socket left open,
    // and a replacement can bind the same address immediately.
    let cancel = tasks.cancellation();
    let mut conns = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((sock, _peer)) => {
                    let _ = sock.set_nodelay(true);
                    conns.spawn(serve_one(sock, svc.clone(), posture.clone(), cancel.clone()));
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
    drain(conns, http.stop_drain, &tasks).await;
    Ok(())
}

/// One accepted connection (#180): served until it ends, and once the stop
/// begins, until the request it is serving is answered. hyper's graceful
/// shutdown finishes the response in flight and takes no further request on
/// the connection; an idle keep-alive closes at once. Errors are routine
/// client behaviour (resets, half-closed keep-alives, head deadlines), not
/// server faults.
///
/// The connection's protocol is read from its first bytes under the head
/// deadline (`opening`), so a socket that never sends one is closed at the
/// deadline as before; an HTTP/1.1 connection's head deadline is what is
/// left of it. An HTTP/2 connection past the listener's cap is refused
/// (`http2_bounds::refuse`); one served holds its place until it ends.
async fn serve_one(
    sock: drain::DrainStream<tokio::net::TcpStream>,
    svc: hyper_util::service::TowerToHyperService<axum::Router>,
    posture: Posture,
    stop: crate::tasks::Cancellation,
) {
    let Posture {
        builder,
        head,
        http2,
    } = posture;
    let deadline = tokio::time::Instant::now() + head;
    let opened = tokio::select! {
        opened = tokio::time::timeout_at(deadline, opening(&sock)) => opened,
        () = stop.cancelled() => return,
    };
    let (builder, _place) = match opened {
        Ok(Ok(Opening::Http2)) => match http2.admit() {
            Some(place) => (builder.http2_only(), Some(place)),
            None => return http2_bounds::refuse(sock, deadline).await,
        },
        Ok(Ok(Opening::Http1)) => {
            let mut builder = builder;
            builder.http1().header_read_timeout(
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            );
            (builder.http1_only(), None)
        }
        Ok(Err(error)) => {
            tracing::debug!("connection ended before its first request: {error}");
            return;
        }
        Err(_elapsed) => return,
    };
    let mut conn = Box::pin(builder.serve_connection(hyper_util::rt::TokioIo::new(sock), svc));
    let served = tokio::select! {
        served = conn.as_mut() => served,
        () = stop.cancelled() => {
            conn.as_mut().graceful_shutdown();
            conn.await
        }
    };
    if let Err(error) = served {
        tracing::debug!("connection ended: {error}");
    }
}

/// The bytes an HTTP/2 connection with prior knowledge opens with.
const HTTP2_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// How long `opening` waits between looks at a preface that has arrived in
/// part: a look returns at once while the bytes it saw wait unread.
const PREFACE_LOOK: std::time::Duration = std::time::Duration::from_millis(5);

/// The protocol a connection opened with.
#[derive(Debug, PartialEq, Eq)]
enum Opening {
    Http1,
    Http2,
}

/// The protocol `sock` speaks, read from its first bytes without taking
/// them: HTTP/2 once the whole preface has arrived, HTTP/1.1 as soon as a
/// byte departs from it or the peer closes.
async fn opening(sock: &drain::DrainStream<tokio::net::TcpStream>) -> std::io::Result<Opening> {
    let mut seen = [0u8; HTTP2_PREFACE.len()];
    loop {
        let n = sock.peek(&mut seen).await?;
        if n == 0 || seen[..n] != HTTP2_PREFACE[..n] {
            return Ok(Opening::Http1);
        }
        if n == HTTP2_PREFACE.len() {
            return Ok(Opening::Http2);
        }
        tokio::time::sleep(PREFACE_LOOK).await;
    }
}

/// The stop's drain (#180): every connection began its graceful shutdown at
/// the stop (`serve_one`). Those still serving are joined for at most
/// `bound`, then the rest (an SSE session, a long-poll, a slow reader) are
/// aborted and joined, each reaped as the accept loop reaps it.
async fn drain(
    mut conns: tokio::task::JoinSet<()>,
    bound: std::time::Duration,
    tasks: &crate::tasks::TaskSupervisor,
) {
    let settled = tokio::time::timeout(bound, async {
        while let Some(joined) = conns.join_next().await {
            reap(tasks, joined);
        }
    })
    .await;
    if settled.is_err() {
        tracing::warn!(
            "stop: {} connection(s) still serving after {bound:?} are reset",
            conns.len()
        );
    }
    conns.abort_all();
    while let Some(joined) = conns.join_next().await {
        reap(tasks, joined);
    }
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
    pub(super) const DEADLINE: Duration = Duration::from_secs(1);
    /// Every wait is bounded well past the deadline; a test that waits
    /// this long fails by assertion, it never hangs.
    pub(super) const BOUND: Duration = Duration::from_secs(4);
    /// The rig's stop drain: past `/slow`'s answer, inside the 5 s grace
    /// the rigs stop their supervisor with.
    const DRAIN: Duration = Duration::from_secs(3);

    /// A handler that panics mid-request: neither axum nor hyper catches the
    /// unwind, so it ends the connection task.
    async fn panicking_handler() -> &'static str {
        panic!("scripted handler panic")
    }

    /// The production serve loop over the rig's routes, on `deadline`.
    pub(super) async fn serve(deadline: Duration) -> (std::net::SocketAddr, TaskSupervisor) {
        let app = axum::Router::new()
            .route("/fast", axum::routing::get(|| async {}))
            .route("/hang", axum::routing::get(std::future::pending::<()>))
            .route("/panic", axum::routing::get(panicking_handler))
            .route(
                "/hold",
                axum::routing::post(move |body: axum::body::Body| async move {
                    tokio::time::sleep(deadline / 2).await;
                    drop(body);
                }),
            )
            .route(
                "/slow",
                axum::routing::get(move || async move {
                    tokio::time::sleep(deadline * 5 / 2).await;
                }),
            );
        serve_app(deadline, app).await
    }

    /// The production serve loop over `app`, on `deadline`.
    pub(super) async fn serve_app(
        deadline: Duration,
        app: axum::Router,
    ) -> (std::net::SocketAddr, TaskSupervisor) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tasks = TaskSupervisor::new();
        let serve_tasks = tasks.clone();
        let http = HttpConfig {
            h1_header_timeout: deadline,
            stop_drain: DRAIN,
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
    pub(super) async fn closed_within(sock: &mut TcpStream, bound: Duration) -> Result<(), String> {
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
    pub(super) async fn response_head(sock: &mut TcpStream) -> String {
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn headless_connection_is_closed_at_the_deadline() {
        let (addr, tasks) = serve(DEADLINE).await;
        let started = tokio::time::Instant::now();
        let mut silent = TcpStream::connect(addr).await.unwrap();
        let mut partial = TcpStream::connect(addr).await.unwrap();
        partial
            .write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n")
            .await
            .unwrap();
        let mut preface = TcpStream::connect(addr).await.unwrap();
        preface.write_all(b"PRI * HTTP/2.0\r\n").await.unwrap();
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
        assert_eq!(
            closed_within(&mut preface, BOUND).await,
            Ok(()),
            "partial HTTP/2 preface"
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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

    /// #180: a request in flight when the stop begins is answered, not reset,
    /// and its connection then closes; the stop waits for it, inside the drain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn a_request_in_flight_at_the_stop_is_answered_before_its_connection_closes() {
        let (addr, tasks) = serve(DEADLINE).await;
        let mut sock = TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /slow HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(DEADLINE / 5).await;
        let started = tokio::time::Instant::now();
        let (report, head) = futures_util::future::join(
            tasks.shutdown(Duration::from_secs(5)),
            response_head(&mut sock),
        )
        .await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        assert_eq!(
            closed_within(&mut sock, BOUND).await,
            Ok(()),
            "the connection takes no request after the stop"
        );
        let stopped = started.elapsed();
        assert!(report.aborted.is_empty(), "{report:?}");
        assert!(
            stopped >= DEADLINE * 2,
            "the stop ended before the answer: {stopped:?}"
        );
    }

    /// #180: at the stop an idle keep-alive closes at once, and a request
    /// still running when the drain ends is reset then; the stop ends there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn the_stop_closes_idle_connections_at_once_and_resets_the_rest_at_the_drain() {
        let (addr, tasks) = serve(BOUND).await;
        let mut idle = TcpStream::connect(addr).await.unwrap();
        idle.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        let head = response_head(&mut idle).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let mut hung = TcpStream::connect(addr).await.unwrap();
        hung.write_all(b"GET /hang HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(DEADLINE / 5).await;
        let started = tokio::time::Instant::now();
        let stop = tasks.shutdown(Duration::from_secs(5));
        let watched = async {
            let idle_closed = closed_within(&mut idle, BOUND).await;
            let idle_at = started.elapsed();
            let hung_closed = closed_within(&mut hung, BOUND * 2).await;
            (idle_closed, idle_at, hung_closed, started.elapsed())
        };
        let (report, (idle_closed, idle_at, hung_closed, hung_at)) =
            futures_util::future::join(stop, watched).await;
        assert_eq!((idle_closed, hung_closed), (Ok(()), Ok(())));
        assert!(
            idle_at < DRAIN / 2,
            "the idle keep-alive closed at {idle_at:?}"
        );
        assert!(
            hung_at >= DRAIN,
            "the running request was reset at {hung_at:?}"
        );
        assert!(report.aborted.is_empty(), "{report:?}");
    }

    /// An HTTP/2 connection to the rig with prior knowledge, and the
    /// connection future to drive beside its requests; it ends when the
    /// server closes the connection.
    pub(super) async fn http2(
        addr: std::net::SocketAddr,
    ) -> (
        hyper::client::conn::http2::SendRequest<axum::body::Body>,
        hyper::client::conn::http2::Connection<
            hyper_util::rt::TokioIo<TcpStream>,
            axum::body::Body,
            hyper_util::rt::TokioExecutor,
        >,
    ) {
        let sock = TcpStream::connect(addr).await.unwrap();
        hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(sock),
        )
        .await
        .unwrap()
    }

    pub(super) fn get(
        addr: std::net::SocketAddr,
        path: &str,
    ) -> axum::http::Request<axum::body::Body> {
        axum::http::Request::get(format!("http://{addr}{path}"))
            .body(axum::body::Body::empty())
            .unwrap()
    }

    /// A connection that opens with the HTTP/2 preface is served HTTP/2 on
    /// the listener that serves HTTP/1.1, several requests at once on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn http2_with_prior_knowledge_is_served_beside_http1() {
        let (addr, tasks) = serve(DEADLINE).await;
        let mut h1 = TcpStream::connect(addr).await.unwrap();
        h1.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        let head = response_head(&mut h1).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let (send, conn) = http2(addr).await;
        let requests = futures_util::future::join_all(
            (0..8).map(|_| send.clone().send_request(get(addr, "/fast"))),
        );
        let answers = tokio::time::timeout(
            BOUND,
            futures_util::future::select(Box::pin(requests), conn),
        )
        .await
        .expect("the answers within the bound");
        let futures_util::future::Either::Left((answers, _conn)) = answers else {
            panic!("the connection ended before its answers");
        };
        for answer in answers {
            let answer = answer.unwrap();
            assert_eq!(answer.status(), 200);
            assert_eq!(answer.version(), axum::http::Version::HTTP_2);
        }
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// One-byte DATA frames a handler holds unread: h2 charges each 255
    /// bytes of a small-frame budget of half the connection window, and
    /// sends GOAWAY ENHANCE_YOUR_CALM past it. 8,192 of them cost 2,088,960
    /// bytes, inside the 2 MiB of [`super::HTTP2_CONNECTION_WINDOW`]'s half
    /// (8,224 frames) and past the half of any window under 4 MiB - 16 KiB.
    const SMALL_FRAMES: usize = 8_192;

    /// A connection carrying thousands of small body frames its handlers
    /// have not read yet keeps serving: the connection window sets h2's
    /// budget for them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn http2_holds_thousands_of_small_unread_body_frames() {
        let (addr, tasks) = serve(DEADLINE).await;
        let (mut send, conn) = http2(addr).await;
        let frames = futures_util::stream::iter(
            (0..SMALL_FRAMES)
                .map(|_| Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"x"))),
        );
        let request = axum::http::Request::post(format!("http://{addr}/hold"))
            .body(axum::body::Body::from_stream(frames))
            .unwrap();
        let answer = tokio::time::timeout(
            BOUND,
            futures_util::future::select(Box::pin(send.send_request(request)), conn),
        )
        .await
        .expect("the answer within the bound");
        let futures_util::future::Either::Left((answer, _conn)) = answer else {
            panic!("the connection ended before its answer");
        };
        assert_eq!(answer.expect("answered, not reset").status(), 200);
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// #180 over HTTP/2: a request in flight when the stop begins is
    /// answered, and the connection then closes; the stop waits for it,
    /// inside the drain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn the_stop_answers_an_http2_request_in_flight_then_closes_its_connection() {
        let (addr, tasks) = serve(DEADLINE).await;
        let (mut send, conn) = http2(addr).await;
        let client = futures_util::future::join(
            send.send_request(get(addr, "/slow")),
            tokio::time::timeout(BOUND * 2, conn),
        );
        let stop = async {
            tokio::time::sleep(DEADLINE / 5).await;
            let started = tokio::time::Instant::now();
            let report = tasks.shutdown(Duration::from_secs(5)).await;
            (report, started.elapsed())
        };
        let ((answer, closed), (report, stopped)) = futures_util::future::join(client, stop).await;
        let answer = answer.expect("the request in flight is answered, not reset");
        assert_eq!(answer.status(), 200);
        assert!(
            closed
                .expect("the connection closes after the answer")
                .is_ok(),
            "the server closes the connection cleanly"
        );
        assert!(report.aborted.is_empty(), "{report:?}");
        assert!(
            stopped >= DEADLINE * 2,
            "the stop ended before the answer: {stopped:?}"
        );
    }

    /// Simultaneous connects: four times the 128 the standard library, mio
    /// and Tokio bind with, and half of the queue `serve_h1` listens with.
    const BURST: usize = 512;

    /// The descriptors the burst holds at once, a client and an accepted
    /// socket for each connect, with room for the tests beside it.
    const BURST_FDS: u64 = 2 * BURST as u64 + 256;

    /// This process's soft `RLIMIT_NOFILE`, as `/proc/self/limits` states
    /// it; `None` when unlimited or unreadable.
    fn open_files_soft_limit() -> Option<u64> {
        let limits = std::fs::read_to_string("/proc/self/limits").ok()?;
        let line = limits.lines().find(|l| l.starts_with("Max open files"))?;
        line.split_whitespace().nth(3)?.parse().ok()
    }

    /// The status line a fresh connection's one request is answered with,
    /// or what ended the exchange first.
    async fn status_line(connected: std::io::Result<TcpStream>) -> String {
        let mut sock = match connected {
            Ok(sock) => sock,
            Err(e) => return format!("connect: {e}"),
        };
        let request = b"GET /fast HTTP/1.1\r\nhost: rig\r\nconnection: close\r\n\r\n";
        if let Err(e) = sock.write_all(request).await {
            return format!("write: {e}");
        }
        let mut response = Vec::new();
        match tokio::time::timeout(BOUND, sock.read_to_end(&mut response)).await {
            Ok(Ok(_)) => String::from_utf8_lossy(&response)
                .lines()
                .next()
                .unwrap_or("closed with no response")
                .to_string(),
            Ok(Err(e)) => format!("read: {e}"),
            Err(_) => format!("no response within {BOUND:?}"),
        }
    }

    /// A burst of `BURST` simultaneous connects is queued whole: every
    /// connect completes inside a second and every request on one is
    /// answered `200`. A connect the accept queue has no room for has its
    /// SYN dropped and completes no sooner than the 1 s initial retransmit,
    /// so a connect of a second or more is one the queue refused. The burst
    /// is sent in one poll of this single-threaded runtime, unconstrained so
    /// the cooperative budget never yields inside it: the accept loop takes
    /// none of it before its last SYN is sent, and the queue alone holds it.
    /// That holds on Linux, whose loopback completes each handshake inside
    /// the connect call. macOS completes it on its input thread, after the
    /// connect returns, so the accept loop drains the queue while the burst
    /// arrives and the test passes there on a 128 queue too (it measures
    /// nothing there; macOS also caps every backlog at `kern.ipc.somaxconn`,
    /// 128 by default). Under a soft `RLIMIT_NOFILE` too low for the burst
    /// (1,024 in a common Linux shell; the standard library raises it only
    /// on Apple targets) it skips with a message rather than failing on
    /// `connect: Too many open files`, which would read as a product fault.
    #[tokio::test(crate = "crate::test_rt")]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "only Linux's loopback queues a burst before the accept loop runs"
    )]
    async fn a_connect_burst_past_the_default_backlog_is_queued_whole() {
        if let Some(soft) = open_files_soft_limit()
            && soft < BURST_FDS
        {
            eprintln!(
                "skipped: the soft RLIMIT_NOFILE is {soft}, and the burst needs {BURST_FDS} \
                 descriptors; raise it (ulimit -n) to run this test"
            );
            return;
        }
        // The head deadline is not what this measures: BOUND keeps it away.
        let (addr, tasks) = serve(BOUND).await;
        // One request served first: the accept loop is running, on the
        // queue `serve_h1` listens with.
        let mut first = TcpStream::connect(addr).await.unwrap();
        first
            .write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n")
            .await
            .unwrap();
        let head = response_head(&mut first).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let started = std::time::Instant::now();
        let burst = (0..BURST).map(|_| async move {
            let connected = TcpStream::connect(addr).await;
            let took = started.elapsed();
            (took, status_line(connected).await)
        });
        let outcomes = tokio::task::unconstrained(futures_util::future::join_all(burst)).await;
        let slow: Vec<Duration> = outcomes
            .iter()
            .map(|(took, _)| *took)
            .filter(|took| *took >= Duration::from_secs(1))
            .collect();
        assert_eq!(
            slow.len(),
            0,
            "{} of {BURST} connects took a second or more, the slowest {:?}: the accept queue dropped their SYNs",
            slow.len(),
            slow.iter().max()
        );
        let mut answers = std::collections::BTreeMap::<&str, usize>::new();
        for (_, line) in &outcomes {
            *answers.entry(line.as_str()).or_default() += 1;
        }
        assert_eq!(
            answers,
            std::collections::BTreeMap::from([("HTTP/1.1 200 OK", BURST)]),
            "every connection of the burst is answered"
        );
        drop(first);
        tasks.shutdown(Duration::from_secs(5)).await;
    }

    /// Listening again keeps the socket: its address, and a connection
    /// queued on it before is the one its next accept takes. It does not
    /// check the queue's depth (macOS caps it at `kern.ipc.somaxconn`); the
    /// Linux burst test above does.
    #[tokio::test(crate = "crate::test_rt")]
    async fn listening_again_keeps_the_address_and_a_queued_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let queued = TcpStream::connect(addr).await.unwrap();
        let listener = super::with_accept_backlog(listener).unwrap();
        assert_eq!(listener.local_addr().unwrap(), addr);
        let (_, peer) = tokio::time::timeout(BOUND, listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(peer, queued.local_addr().unwrap());
    }

    /// The validated floor is exactly the one hyper asserts: a buffer at
    /// the floor builds, one byte less panics. If hyper moves its private
    /// minimum either way, this fails before a config it refuses can boot.
    #[test]
    fn the_validated_buffer_floor_is_the_one_hyper_asserts() {
        let builds = |n: usize| {
            std::panic::catch_unwind(move || {
                drop(super::connection_builder(&HttpConfig {
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
    #[tokio::test(crate = "crate::test_rt")]
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
