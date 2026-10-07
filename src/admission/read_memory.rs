//! Read memory (shared cells H3): a page read reserves the bytes it may
//! materialize before it runs, and its response body holds the bytes it
//! rendered until the body ends.
//!
//! Two ledgers move together, both exact:
//! - the instance's read memory (`ReadMemory`), a quarter of the RSS shed
//!   line;
//! - its project's read bytes (`quota::read_reservation`), which its
//!   memory-pressure estimate counts and its memory line bounds.
//!
//! A read that does not fit in either ledger waits for reads and bodies to
//! release enough (`READ_MEMORY_WAIT`), then is refused retryably, so a
//! burst of a project's reads (a herd of long-polls re-arming) queues for
//! milliseconds and only memory held for seconds refuses. One read always
//! fits, so a page larger than a budget is served alone.
//!
//! A request's hold (`ReadHold`) lives in its request scope
//! (`admission::park`): it reserves `min(maxBytes, 8 MiB)` at admission,
//! releases the reservation while the request waits for data (a waiting
//! long-poll materializes nothing), takes it again before a woken wait
//! renders (`ReadHold::resume`, waiting for room until the wait's own
//! deadline, so a fan-out wake of one project's waits stays inside its
//! line), becomes the rendered page's exact size when the page renders,
//! and rides the response body from there (`ReadHold::attach`), released
//! when the body ends or is dropped: a client that never drains it holds
//! it until the connection's drain floor (`http::serve`) closes the
//! connection.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use hyper::body::{Frame, SizeHint};

use super::AdmissionController;
use crate::quota::read_reservation::ProjectReadBytes;

/// The instance's read memory is the RSS shed line divided by this: a
/// read burst alone stays inside the line from the certified 1 GiB
/// baseline (362 MB + 125 MiB < 500 MB).
const READ_MEMORY_SHARE_OF_SHED: u64 = 4;

/// How long a read waits for room under its project's line and in the
/// instance's read memory before it is refused (429
/// `project_memory_pressure` or 503 `read_memory_busy`, both retryable).
pub(crate) const READ_MEMORY_WAIT: Duration = Duration::from_secs(2);

/// A held body is served in frames of at most this many bytes, so it ends,
/// and its hold drops, only once hyper's write buffer (`h1_max_buf`) has
/// taken all but its last frames rather than when hyper takes the page.
const HELD_FRAME_BYTES: usize = 64 * 1024;

/// The instance's read-memory budget and what holds it.
pub(crate) struct ReadMemory {
    /// 0 = no budget.
    capacity: AtomicU64,
    held: AtomicU64,
    released: tokio::sync::Notify,
}

impl ReadMemory {
    pub(super) fn new(rss_shed_mb: u64) -> Self {
        Self {
            capacity: AtomicU64::new(
                rss_shed_mb.saturating_mul(1 << 20) / READ_MEMORY_SHARE_OF_SHED,
            ),
            held: AtomicU64::new(0),
            released: tokio::sync::Notify::new(),
        }
    }

    /// Take `bytes` if they fit, or if nothing is held.
    fn try_take(&self, bytes: u64) -> bool {
        let capacity = self.capacity.load(Ordering::Relaxed);
        let mut now = self.held.load(Ordering::Relaxed);
        loop {
            if capacity > 0 && now > 0 && now.saturating_add(bytes) > capacity {
                return false;
            }
            match self.held.compare_exchange_weak(
                now,
                now.saturating_add(bytes),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(moved) => now = moved,
            }
        }
    }

    fn charge(&self, bytes: u64) {
        self.held.fetch_add(bytes, Ordering::Relaxed);
    }

    fn release(&self, bytes: u64) {
        let mut now = self.held.load(Ordering::Relaxed);
        while let Err(moved) = self.held.compare_exchange_weak(
            now,
            now.saturating_sub(bytes),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            now = moved;
        }
        self.released.notify_waiters();
    }
}

impl AdmissionController {
    /// The instance's read memory: (held, capacity), capacity 0 = none.
    pub(crate) fn read_memory(&self) -> (u64, u64) {
        let memory = &self.inner.read_memory;
        (
            memory.held.load(Ordering::Relaxed),
            memory.capacity.load(Ordering::Relaxed),
        )
    }

    /// Rigs size the instance's read memory (the rig sheds no RSS).
    #[cfg(test)]
    pub(crate) fn set_read_memory_capacity(&self, bytes: u64) {
        self.inner
            .read_memory
            .capacity
            .store(bytes, Ordering::Relaxed);
    }
}

/// Why a read's reservation was refused after its wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadRefusal {
    /// Its project's read bytes would have passed the project's line.
    Project,
    /// The instance's read memory had no room.
    Instance,
}

impl ReadRefusal {
    /// A refusal at a project's line is one of the project's memory sheds.
    fn counted(self, project: Option<&ProjectReadBytes>) -> Self {
        if let (Self::Project, Some(project)) = (self, project) {
            project.refused();
        }
        self
    }
}

/// One request's read memory, in both ledgers.
pub(crate) struct ReadHold {
    ctl: AdmissionController,
    project: Option<ProjectReadBytes>,
    /// What admission reserved: what a woken wait takes again before it
    /// renders. 0 for a hold no admission reserved.
    budget: u64,
    bytes: AtomicU64,
    rendered: AtomicBool,
}

impl ReadHold {
    /// Reserve `bytes` for a page read in both ledgers: its project's,
    /// under the project's `line`, and the instance's. A read that does not
    /// fit waits up to `READ_MEMORY_WAIT` for reads and bodies to release
    /// enough, then is refused for the ledger that last refused it.
    pub(crate) async fn reserve(
        ctl: &AdmissionController,
        project: Option<ProjectReadBytes>,
        line: u64,
        bytes: u64,
    ) -> Result<Self, ReadRefusal> {
        let deadline = tokio::time::Instant::now() + READ_MEMORY_WAIT;
        Self::take_until(
            &ctl.inner.read_memory,
            project.as_ref(),
            line,
            bytes,
            deadline,
        )
        .await?;
        Ok(Self {
            ctl: ctl.clone(),
            project,
            budget: bytes,
            bytes: AtomicU64::new(bytes),
            rendered: AtomicBool::new(false),
        })
    }

    /// A woken wait is about to render: take what admission reserved
    /// again, under the project's line and the instance's read memory, as
    /// admission did, waiting for room until `deadline` (the wait's own).
    /// False when no room came by then: the wait must end without a page.
    /// A rendered hold, or one that already holds its budget, takes
    /// nothing.
    pub(crate) async fn resume(&self, deadline: tokio::time::Instant) -> bool {
        let short = self.budget.saturating_sub(self.bytes());
        if self.rendered.load(Ordering::Relaxed) || short == 0 {
            return true;
        }
        let line = self.ctl.project_memory_pressure_bytes();
        let memory = &self.ctl.inner.read_memory;
        let taken = Self::take_until(memory, self.project.as_ref(), line, short, deadline).await;
        if taken.is_ok() {
            self.bytes.fetch_add(short, Ordering::Relaxed);
        }
        taken.is_ok()
    }

    /// `take`, retried on every release until it fits or `deadline` has
    /// passed; refused for the ledger that refused it last.
    async fn take_until(
        memory: &ReadMemory,
        project: Option<&ProjectReadBytes>,
        line: u64,
        bytes: u64,
        deadline: tokio::time::Instant,
    ) -> Result<(), ReadRefusal> {
        if Self::take(memory, project, line, bytes).is_ok() {
            return Ok(());
        }
        loop {
            // Registered before the retry, so a release between the two is
            // never missed; every release in either ledger notifies.
            let mut released = Box::pin(memory.released.notified());
            released.as_mut().enable();
            let refusal = match Self::take(memory, project, line, bytes) {
                Ok(()) => return Ok(()),
                Err(refusal) => refusal,
            };
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return Err(refusal.counted(project));
            }
        }
    }

    /// Take `bytes` in both ledgers at once, or in neither.
    fn take(
        memory: &ReadMemory,
        project: Option<&ProjectReadBytes>,
        line: u64,
        bytes: u64,
    ) -> Result<(), ReadRefusal> {
        if let Some(project) = project
            && !project.try_reserve(bytes, line)
        {
            return Err(ReadRefusal::Project);
        }
        if memory.try_take(bytes) {
            return Ok(());
        }
        // Undone without a notification: one would wake this read's own
        // wait at once. A read the transient reservation kept out of the
        // project's line is woken by the next release, with this one.
        if let Some(project) = project {
            project.release(bytes);
        }
        Err(ReadRefusal::Instance)
    }

    /// A hold for a page no admission reserved (a consumer pull's batch):
    /// it holds nothing until the page renders.
    pub(crate) fn unreserved(ctl: &AdmissionController, project: Option<ProjectReadBytes>) -> Self {
        Self {
            ctl: ctl.clone(),
            project,
            budget: 0,
            bytes: AtomicU64::new(0),
            rendered: AtomicBool::new(true),
        }
    }

    /// The bytes this hold holds now.
    fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Move both ledgers from what this hold holds to `bytes`: a growth is
    /// charged (the bytes already exist), a shrink released.
    fn resize(&self, bytes: u64) {
        let before = self.bytes.swap(bytes, Ordering::Relaxed);
        if bytes > before {
            let grown = bytes - before;
            self.ctl.inner.read_memory.charge(grown);
            if let Some(project) = &self.project {
                project.charge(grown);
            }
        } else if before > bytes {
            // The project's first: the instance's release notifies the
            // reads waiting on either ledger.
            let shrunk = before - bytes;
            if let Some(project) = &self.project {
                project.release(shrunk);
            }
            self.ctl.inner.read_memory.release(shrunk);
        }
    }

    /// The request waits for data: nothing is materialized while it waits,
    /// so an unrendered reservation is released.
    pub(crate) fn unreserve(&self) {
        if !self.rendered.load(Ordering::Relaxed) {
            self.resize(0);
        }
    }

    /// A page of `served` bytes rendered: the first replaces the
    /// reservation with the page's exact size, a later one adds to it.
    pub(crate) fn settle(&self, served: u64) {
        if self.rendered.swap(true, Ordering::Relaxed) {
            self.resize(self.bytes().saturating_add(served));
        } else {
            self.resize(served);
        }
    }

    /// The response the request answered: a rendered page's hold rides its
    /// body until the body ends or is dropped; anything else releases the
    /// hold now (a refusal or a failure renders no page).
    pub(crate) fn attach(self, response: Response) -> Response {
        if !self.rendered.load(Ordering::Relaxed) || self.bytes() == 0 {
            return response;
        }
        let (parts, inner) = response.into_parts();
        let held = HeldBody {
            inner,
            pending: Bytes::new(),
            _hold: self,
        };
        Response::from_parts(parts, Body::new(held))
    }
}

impl Drop for ReadHold {
    fn drop(&mut self) {
        self.resize(0);
    }
}

/// A response body that holds its page's read memory until it ends.
struct HeldBody {
    inner: Body,
    /// The rest of the frame the inner body last yielded.
    pending: Bytes,
    _hold: ReadHold,
}

impl hyper::body::Body for HeldBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.get_mut();
        if this.pending.is_empty() {
            match Pin::new(&mut this.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => this.pending = data,
                    Err(other) => return Poll::Ready(Some(Ok(other))),
                },
                other => return other,
            }
        }
        let next = this.pending.len().min(HELD_FRAME_BYTES);
        Poll::Ready(Some(Ok(Frame::data(this.pending.split_to(next)))))
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_empty() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let inner = self.inner.size_hint();
        let pending = self.pending.len() as u64;
        match inner.exact() {
            Some(exact) => SizeHint::with_exact(exact + pending),
            None => {
                let mut hint = SizeHint::new();
                hint.set_lower(inner.lower() + pending);
                if let Some(upper) = inner.upper() {
                    hint.set_upper(upper + pending);
                }
                hint
            }
        }
    }
}

#[cfg(test)]
mod tests;
