//! The bounds an HTTP/2 connection is served under beside its builder's
//! (`serve::connection_builder`), so every HTTP/2 resource is bounded per
//! instance (edge change #238):
//!
//! - at most [`HTTP2_CONNECTIONS`] HTTP/2 connections per listener; one past
//!   them is refused with GOAWAY before any stream ([`Http2Connections`],
//!   [`refuse`]);
//! - a connection that serves no stream for the head deadline is shut down
//!   gracefully, as an idle HTTP/1.1 keep-alive closes at that deadline
//!   ([`Streams`], [`stop_or_idle`]).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

/// The HTTP/2 connections one listener serves at once. A platform proxy
/// pools up to 250 per instance (Habanero's `--pool-connections`).
pub(crate) const HTTP2_CONNECTIONS: usize = 256;

/// The places for HTTP/2 connections on one listener, shared by its
/// connections; a connection holds its place while it is served.
#[derive(Clone)]
pub(super) struct Http2Connections(Arc<Semaphore>);

impl Http2Connections {
    pub(super) fn new() -> Self {
        Self(Arc::new(Semaphore::new(HTTP2_CONNECTIONS)))
    }

    /// A place for one more HTTP/2 connection, released when dropped;
    /// `None` while [`HTTP2_CONNECTIONS`] are served.
    pub(super) fn admit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.0).try_acquire_owned().ok()
    }
}

/// What a connection past the cap is answered with: the server's
/// connection preface, an empty SETTINGS frame (RFC 9113 §3.4), then a
/// GOAWAY that processed no stream (last stream 0) with `REFUSED_STREAM`
/// and the debug data `http2_connection_cap`, so the peer may retry every
/// request it sent on another connection.
const REFUSAL: &[u8] = b"\0\0\0\x04\0\0\0\0\0\
\0\0\x1c\x07\0\0\0\0\0\
\0\0\0\0\0\0\0\x07http2_connection_cap";

/// How long a refused connection is read after its GOAWAY, at most: what
/// the peer sent is drained until it closes, so the close does not reset
/// the connection before the peer has read the GOAWAY.
const REFUSAL_LINGER: Duration = Duration::from_secs(2);

/// Refuses an HTTP/2 connection past the cap: writes [`REFUSAL`], closes
/// the server's side and reads what the peer sent until it closes, for at
/// most [`REFUSAL_LINGER`] and never past the head `deadline`.
pub(super) async fn refuse<S: AsyncRead + AsyncWrite + Unpin>(
    mut sock: S,
    deadline: tokio::time::Instant,
) {
    let linger = deadline.min(tokio::time::Instant::now() + REFUSAL_LINGER);
    let refused = tokio::time::timeout_at(linger, async {
        sock.write_all(REFUSAL).await?;
        sock.shutdown().await?;
        let mut sink = [0u8; 4096];
        while sock.read(&mut sink).await? != 0 {}
        Ok::<(), std::io::Error>(())
    })
    .await;
    match refused {
        Ok(Ok(())) => tracing::debug!("refused an HTTP/2 connection past {HTTP2_CONNECTIONS}"),
        Ok(Err(error)) => tracing::debug!("a refused HTTP/2 connection ended: {error}"),
        Err(_elapsed) => tracing::debug!("a refused HTTP/2 connection was open at its linger"),
    }
}

/// The streams one connection is serving: a request counts from the call
/// of the service until its response body is done or dropped, so a
/// long-poll or an SSE session counts for as long as it is answered.
#[derive(Clone)]
pub(super) struct Streams(Arc<watch::Sender<usize>>);

impl Streams {
    pub(super) fn new() -> Self {
        Self(Arc::new(watch::Sender::new(0)))
    }

    fn open(&self) -> Open {
        self.0.send_modify(|n| *n = n.saturating_add(1));
        Open(self.clone())
    }

    /// Resolves once the connection has served no stream for `bound`
    /// without a break.
    async fn idle_for(&self, bound: Duration) {
        let mut seen = self.0.subscribe();
        loop {
            if seen.wait_for(|n| *n == 0).await.is_err() {
                return;
            }
            if tokio::time::timeout(bound, seen.changed()).await.is_err() {
                return;
            }
        }
    }
}

/// One stream being served; dropping it ends the stream's count.
struct Open(Streams);

impl Drop for Open {
    fn drop(&mut self) {
        (self.0).0.send_modify(|n| *n = n.saturating_sub(1));
    }
}

/// Resolves at the stop, or once an HTTP/2 connection (`idle` is its head
/// deadline) has served no stream for that long; an HTTP/1.1 connection
/// (`idle` is `None`) keeps hyper's own idle bound, the head deadline.
pub(super) async fn stop_or_idle(
    stop: &crate::tasks::Cancellation,
    streams: &Streams,
    idle: Option<Duration>,
) {
    let idle = async {
        match idle {
            Some(bound) => streams.idle_for(bound).await,
            None => std::future::pending().await,
        }
    };
    futures_util::future::select(Box::pin(stop.cancelled()), Box::pin(idle)).await;
}

/// The connection's service, counting each request it serves in
/// [`Streams`].
#[derive(Clone)]
pub(super) struct Counted<S> {
    inner: S,
    streams: Streams,
}

impl<S> Counted<S> {
    pub(super) fn new(inner: S, streams: &Streams) -> Self {
        Self {
            inner,
            streams: streams.clone(),
        }
    }
}

/// A served request's answer, still counted.
type Answer<B, E> = Pin<Box<dyn Future<Output = Result<hyper::Response<Counting<B>>, E>> + Send>>;

impl<S, B> hyper::service::Service<hyper::Request<hyper::body::Incoming>> for Counted<S>
where
    S: hyper::service::Service<
            hyper::Request<hyper::body::Incoming>,
            Response = hyper::Response<B>,
        >,
    S::Future: Send + 'static,
    S::Error: 'static,
    B: Send + 'static,
{
    type Response = hyper::Response<Counting<B>>;
    type Error = S::Error;
    type Future = Answer<B, S::Error>;

    fn call(&self, request: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        let open = self.streams.open();
        let answer = self.inner.call(request);
        Box::pin(async move {
            let response = answer.await?;
            Ok(response.map(|body| Counting { body, _open: open }))
        })
    }
}

/// A response body that keeps its stream counted until it is done or
/// dropped.
pub(super) struct Counting<B> {
    body: B,
    _open: Open,
}

impl<B: hyper::body::Body + Unpin> hyper::body::Body for Counting<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests;
