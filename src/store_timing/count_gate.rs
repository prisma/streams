//! The instance-wide bound on store requests in flight (Addendum 17 of the forward-wedge ruling,
//! 2026-11-20): one pool of permits a request takes before it is dispatched and holds until the
//! call answers.
//!
//! A pile of distinct cold streams released together after a move-in open revalidated 1,550
//! descriptors at once, one store request and one new connection each, while every fan-out of its
//! own was bounded. The instance's total was bounded by nothing. These pools bound it: a request
//! waits for a permit inside the in-flight slot the instance counted it in, so a pile queues at the
//! store and the requests stay counted.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The permits SST transfers (class 2) and every other request (class 4: descriptors, the
/// catalog, topology, gc and compactions) share.
pub(crate) const GENERAL_PERMITS: usize = 256;

/// The permits the commit path and cluster liveness have of their own: the WAL (class 0), the
/// manifest (class 1), and the fleet and routers (class 3). They never wait behind the general
/// class.
pub(crate) const COMMIT_PERMITS: usize = 64;

/// A permit, held until it drops. `None` only if the pool's semaphore were closed, which nothing
/// does.
pub(super) type CountPermit = Option<OwnedSemaphorePermit>;

/// The wait for a permit: owned, so a listing's stream can hold it.
pub(super) type CountWait = Pin<Box<dyn Future<Output = CountPermit> + Send>>;

/// One pool of permits, with its highest use and its waits.
#[derive(Debug)]
pub(super) struct Pool {
    sem: Arc<Semaphore>,
    size: usize,
    peak: AtomicUsize,
    waits: AtomicU64,
}

impl Pool {
    pub(super) fn new(size: usize) -> Arc<Self> {
        Arc::new(Self {
            sem: Arc::new(Semaphore::new(size)),
            size,
            peak: AtomicUsize::new(0),
            waits: AtomicU64::new(0),
        })
    }

    /// Waits for one permit, in the order the waits began (tokio's semaphore is fair). A wait
    /// dropped before it is granted takes nothing; a permit dropped returns to the pool.
    pub(super) fn acquire(self: &Arc<Self>) -> CountWait {
        let pool = Arc::clone(self);
        Box::pin(async move {
            let permit = match Arc::clone(&pool.sem).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    pool.waits.fetch_add(1, Ordering::Relaxed);
                    Arc::clone(&pool.sem).acquire_owned().await.ok()
                }
            };
            let held = pool.size - pool.sem.available_permits();
            pool.peak.fetch_max(held, Ordering::Relaxed);
            permit
        })
    }

    /// `GET /v1/debug/store`'s view of the pool (edge change #270): its permits, the permits out
    /// now, the most out at once and the requests that waited for one since the process began.
    /// The sampler's swap (`swap_peak`) lowers the peak to what is out, as it does the outbound
    /// gauge's, so each sample's peak is the most since the last.
    pub(super) fn stats_json(&self, swap_peak: bool) -> serde_json::Value {
        let out = self.size - self.sem.available_permits();
        let peak = if swap_peak {
            self.peak.swap(out, Ordering::Relaxed)
        } else {
            self.peak.load(Ordering::Relaxed)
        };
        serde_json::json!({
            "permits": self.size,
            "out": out,
            "peak": peak,
            "waits": self.waits.load(Ordering::Relaxed),
        })
    }

    /// The most permits held at once, and how many requests waited for one.
    #[cfg(test)]
    pub(super) fn seen(&self) -> (usize, u64) {
        (
            self.peak.load(Ordering::Relaxed),
            self.waits.load(Ordering::Relaxed),
        )
    }
}

/// A listing or a delete stream that waits for its permit before it first polls `inner`, and
/// holds it until `inner`'s first item or its end: the call's own wait, never the rest of the
/// stream. Dropped, it returns the permit or abandons the wait.
pub(super) struct Gated<S> {
    wait: Option<CountWait>,
    held: CountPermit,
    inner: S,
}

impl<S> Gated<S> {
    pub(super) fn new(wait: CountWait, inner: S) -> Self {
        Self {
            wait: Some(wait),
            held: None,
            inner,
        }
    }
}

impl<S: Stream + Unpin> Stream for Gated<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        let this = self.get_mut();
        if let Some(wait) = this.wait.as_mut() {
            match wait.as_mut().poll(cx) {
                Poll::Ready(permit) => {
                    this.held = permit;
                    this.wait = None;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let polled = this.inner.poll_next_unpin(cx);
        if polled.is_ready() {
            this.held = None;
        }
        polled
    }
}

#[cfg(test)]
mod tests {
    use super::{Gated, Pool};
    use futures_util::StreamExt;
    use std::task::{Context, Poll};

    #[tokio::test(crate = "crate::test_rt")]
    async fn a_pool_waits_when_full_and_a_dropped_wait_or_permit_takes_nothing() {
        let pool = Pool::new(2);
        let first = pool.acquire().await;
        let second = pool.acquire().await;
        assert_eq!(pool.seen(), (2, 0));
        let mut third = pool.acquire();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            third.as_mut().poll(&mut cx).is_pending(),
            "a full pool waits"
        );
        assert_eq!(pool.seen(), (2, 1));
        drop(third);
        drop(first);
        assert_eq!(
            pool.sem.available_permits(),
            1,
            "an abandoned wait takes nothing"
        );
        let fourth = pool.acquire().await;
        assert!(fourth.is_some());
        drop((second, fourth));
        assert_eq!(pool.sem.available_permits(), 2, "every permit returns");
    }

    #[tokio::test(crate = "crate::test_rt")]
    async fn a_gated_stream_holds_its_permit_to_its_first_item_only() {
        let pool = Pool::new(1);
        let mut gated = Gated::new(pool.acquire(), futures_util::stream::iter([1, 2]));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert_eq!(gated.poll_next_unpin(&mut cx), Poll::Ready(Some(1)));
        assert_eq!(
            pool.sem.available_permits(),
            1,
            "released at the first item"
        );
        let blocker = pool.acquire().await;
        let mut waiting = Gated::new(pool.acquire(), futures_util::stream::iter([3]));
        assert!(
            waiting.poll_next_unpin(&mut cx).is_pending(),
            "it waits first"
        );
        drop(waiting);
        drop(blocker);
        assert_eq!(
            pool.sem.available_permits(),
            1,
            "a dropped stream takes nothing"
        );
        let mut empty = Gated::new(pool.acquire(), futures_util::stream::iter(Vec::<u8>::new()));
        assert_eq!(empty.poll_next_unpin(&mut cx), Poll::Ready(None));
        assert_eq!(pool.sem.available_permits(), 1, "released at the end");
        assert_eq!(gated.poll_next_unpin(&mut cx), Poll::Ready(Some(2)));
    }

    /// `GET /v1/debug/store`'s view of a pool (edge change #270): a read without the swap leaves
    /// the peak; the sampler's swap reads the most out since its last swap and lowers it to what
    /// is out now; waits count since the pool began and no read resets them.
    #[tokio::test(crate = "crate::test_rt")]
    async fn a_pools_stats_read_its_peak_since_the_last_swap_and_every_wait() {
        let pool = Pool::new(2);
        let first = pool.acquire().await;
        let second = pool.acquire().await;
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut third = pool.acquire();
        assert!(third.as_mut().poll(&mut cx).is_pending());
        drop(third);
        let stats = |swap| pool.stats_json(swap);
        let at =
            |out, peak| serde_json::json!({"permits": 2, "out": out, "peak": peak, "waits": 1});
        assert_eq!(stats(false), at(2, 2), "both out, one wait");
        drop((first, second));
        assert_eq!(
            stats(false),
            at(0, 2),
            "a read without the swap keeps the peak"
        );
        assert_eq!(
            stats(true),
            at(0, 2),
            "the swap reads the peak since the last swap"
        );
        assert_eq!(stats(false), at(0, 0), "and lowers it to what is out");
    }
}
