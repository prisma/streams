//! The bounds an HTTP/2 connection is served under beside its builder's
//! (`serve::connection_builder`), so every HTTP/2 resource is bounded per
//! instance (edge change #238): at most [`HTTP2_CONNECTIONS`] HTTP/2
//! connections per listener; one past them is refused with GOAWAY before
//! any stream ([`Http2Connections`], [`refuse`]).

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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

#[cfg(test)]
mod tests;
