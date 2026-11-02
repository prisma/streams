//! The emulator's listener: every accepted connection is served HTTP/1.1,
//! or HTTP/2 when it opens with the connection preface, so a server started
//! with STORE_HTTP2=1 carries its requests in flight on one connection.

/// h2 closes a connection with GOAWAY ENHANCE_YOUR_CALM once the small DATA
/// frames it holds unread pass half the connection window. A fleet's WAL
/// bodies are small and many, and at hyper's 1 MiB default they passed it on
/// a busy store: 64 MiB holds 32 MiB of them.
pub(crate) const CONNECTION_WINDOW: u32 = 64 << 20;

/// Each HTTP/2 stream's receive window.
const STREAM_WINDOW: u32 = 4 << 20;

/// Serve `router` on every connection `listener` accepts, until accepting
/// fails. The listener owns each connection's task and reaps it when it
/// ends.
#[expect(
    clippy::disallowed_methods,
    reason = "serve; each accepted connection is served by a task the listener's own JoinSet owns, reaps as it ends and aborts when the listener returns; a supervised task per connection would restate what the JoinSet already owns for this stand-in store"
)]
pub(crate) async fn serve(
    listener: tokio::net::TcpListener,
    router: axum::Router,
) -> std::io::Result<()> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                connections.spawn(connection(socket, router.clone()));
            }
            Some(_ended) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

/// The builder each connection is served with. A store takes the resets of
/// requests its clients cancel under load, which h2 otherwise counts toward
/// GOAWAY at 20 streams reset before they were accepted.
pub(crate) fn builder() -> hyper_util::server::conn::auto::Builder<hyper_util::rt::TokioExecutor> {
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    builder
        .http2()
        .max_pending_accept_reset_streams(None)
        .max_local_error_reset_streams(None)
        .initial_connection_window_size(CONNECTION_WINDOW)
        .initial_stream_window_size(STREAM_WINDOW);
    builder
}

/// Serve one connection until it ends, and report how it ended if it
/// failed.
async fn connection(socket: tokio::net::TcpStream, router: axum::Router) {
    let service = hyper_util::service::TowerToHyperService::new(router);
    if let Err(error) = builder()
        .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
        .await
    {
        eprintln!("s3lite connection ended: {}", chain(&*error));
    }
}

/// An error with its whole chain of sources, since hyper's own text names
/// only the protocol.
fn chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}
