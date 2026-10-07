//! The drain floor (shared cells H3): a client that stops reading a
//! response is disconnected, so the memory behind the response (hyper's
//! write buffer, the body and everything the body holds) is released
//! instead of pinned for as long as the client keeps the socket.
//!
//! hyper has no write deadline: its only deadline never runs while a
//! response is written (`h1_builder`), and once its write buffer is full
//! it stops polling the body, so nothing in the body can notice a client
//! that has stopped. The floor therefore sits under hyper, on the
//! connection's socket. A write the socket cannot take opens a window
//! (`HttpConfig::drain_window`); a write that takes everything offered
//! closes it. If, when a window ends, the client has accepted fewer than
//! `drain_min_bytes` during it, the write fails and hyper closes the
//! connection, dropping the response body. A client that keeps up, an
//! idle keep-alive, a parked long-poll and an idle SSE subscription never
//! open a window; an SSE client that stops reading also meets its
//! session's 10 s send timeout.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, Sleep};

use crate::config::HttpConfig;

/// The least a client must accept of a pending response per window.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DrainFloor {
    window: Duration,
    min_bytes: u64,
}

impl DrainFloor {
    pub(crate) fn of(http: &HttpConfig) -> Self {
        Self {
            window: http.drain_window,
            min_bytes: http.drain_min_bytes,
        }
    }
}

/// The serve loop's listener: every accepted socket is served under the
/// floor.
pub(crate) struct DrainListener {
    listener: TcpListener,
    floor: DrainFloor,
}

impl DrainListener {
    pub(crate) fn new(listener: TcpListener, http: &HttpConfig) -> Self {
        Self {
            listener,
            floor: DrainFloor::of(http),
        }
    }

    pub(crate) async fn accept(&self) -> io::Result<(DrainStream<TcpStream>, SocketAddr)> {
        let (socket, peer) = self.listener.accept().await?;
        Ok((DrainStream::new(socket, self.floor), peer))
    }
}

/// A window opened by a write the socket could not take.
struct Stall {
    since: Instant,
    accepted: u64,
}

impl Stall {
    fn now() -> Self {
        Self {
            since: Instant::now(),
            accepted: 0,
        }
    }
}

/// A connection's socket under the drain floor.
pub(crate) struct DrainStream<S> {
    inner: S,
    floor: DrainFloor,
    stall: Option<Stall>,
    /// Wakes the connection when the open window ends; made at the first
    /// stall, so a connection that never stalls holds no timer.
    timer: Option<Pin<Box<Sleep>>>,
}

impl DrainStream<TcpStream> {
    pub(crate) fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.inner.set_nodelay(nodelay)
    }
}

impl<S> DrainStream<S> {
    pub(crate) fn new(inner: S, floor: DrainFloor) -> Self {
        Self {
            inner,
            floor,
            stall: None,
            timer: None,
        }
    }

    /// The socket took `n` of the `offered` bytes: all of them closes the
    /// window, part of them counts toward it.
    fn took(&mut self, n: usize, offered: usize) {
        if n >= offered {
            self.stall = None;
        } else if let Some(stall) = &mut self.stall {
            stall.accepted = stall.accepted.saturating_add(n as u64);
        }
    }

    /// The socket took nothing: open a window, or judge the open one if it
    /// has ended (a window the client met opens the next one).
    fn stalled(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        loop {
            let stall = self.stall.get_or_insert_with(Stall::now);
            let end = stall.since + self.floor.window;
            let ended = Instant::now() >= end;
            if ended && stall.accepted < self.floor.min_bytes {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the client drained the response below the floor",
                )));
            }
            if ended {
                *stall = Stall::now();
                continue;
            }
            if self.wake_at(end, cx).is_pending() {
                return Poll::Pending;
            }
        }
    }

    /// Wake the connection at `end`; `Ready` once it has passed.
    fn wake_at(&mut self, end: Instant, cx: &mut Context<'_>) -> Poll<()> {
        let timer = self
            .timer
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(end)));
        if timer.deadline() != end {
            timer.as_mut().reset(end);
        }
        timer.as_mut().poll(cx)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for DrainStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for DrainStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                this.took(n, buf.len());
                Poll::Ready(Ok(n))
            }
            Poll::Pending => this.stalled(cx),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(n)) => {
                this.took(n, bufs.iter().map(|b| b.len()).sum());
                Poll::Ready(Ok(n))
            }
            Poll::Pending => this.stalled(cx),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests;
