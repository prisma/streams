//! The forwards a non-owner holds while their owners answer (edge change
//! #235, `http::forward`). A forward is an active request at the non-owner
//! for as long as it waits, so an owner that never answers would hold every
//! request forwarded to it, up to the survival bound, and the non-owner's
//! own streams would be shed beside them. Forwards waiting at once are
//! capped at a quarter of the in-flight cap, so at most that share of the
//! non-owner's active requests waits on other instances; past it a forward
//! is not sent and the client gets the non-owner's `409` to replay.
//!
//! **A held waiter waits for a place** (Addendum 18 of GATE-DECISION-s22-join-2026-11-20, an
//! amendment to edge #266's E7). A request that waited on a planned move's hold holding a waiting
//! place (`park::waits_on_move`), and whose owner at the release is another member, is not answered
//! `409` for want of a place: it waits, first in first out, for the place a finished forward frees,
//! at most `RELEASE_WAIT`, and keeps its waiting place meanwhile. Past the bound, and for any request
//! that did not wait on a hold, no place is a `409` as before; so while held waiters queue, a request
//! that did not wait on the hold finds no free place and is answered `409`: the pile goes first. The
//! places stay a quarter of the cap: a freed place is handed to the oldest waiter, never added to.
//! Each hold whose released waiters needed a forward logs one `handoff_released` line once the last
//! of them is answered or gone (`ReleaseTally`).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use tokio::time::Instant;

use super::AdmissionController;

/// The forwards that may wait at once when no in-flight cap is set: a
/// quarter of 512.
const UNCAPPED: i64 = 128;

/// How long a held waiter waits for a forward place once its forward is due, at the move's release:
/// the move-release DST's pile of 667 drained through 128 places in 0.4 s (Addendum 18).
pub(crate) const RELEASE_WAIT: Duration = Duration::from_secs(1);

/// The tally each planned move's hold keeps, by the shard prefix it held.
type Tallies = HashMap<String, Weak<ReleaseTally>>;

/// One controller's forward places: how many forwards wait for their owners' answers now, the held
/// waiters waiting for a place in order (each handed the place a finished forward frees, so a freed
/// place never returns to the count while one waits), and each hold's tally while it lives.
#[derive(Default)]
pub(crate) struct Places {
    waiting: AtomicI64,
    queue: Mutex<VecDeque<tokio::sync::oneshot::Sender<()>>>,
    // mt-lint: allow(name-keyed-map): shard prefix -> the tally of the planned move that held it
    tallies: Mutex<Tallies>,
}

/// Each section of these locks moves one sender or one tally in or out and panics nowhere, so a
/// poisoned lock is as the last section left it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How one planned move's released waiters met the forward places: the waiters that needed a
/// forward, those placed, those that waited for a place, those refused at the bound, and the
/// longest wait. Each such waiter's request keeps it until it is answered or gone, so its line is
/// logged by whichever leaves last (`handoff_released`, beside the giver's `handoff` line, which is
/// logged at the release itself).
pub(crate) struct ReleaseTally {
    prefix: String,
    released: AtomicU64,
    placed: AtomicU64,
    waited: AtomicU64,
    refused: AtomicU64,
    longest_ms: AtomicU64,
    /// Tests only: the counts its line reported, once it was dropped.
    #[cfg(test)]
    reported: Arc<Mutex<Option<[u64; 5]>>>,
}

impl ReleaseTally {
    fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            released: AtomicU64::new(0),
            placed: AtomicU64::new(0),
            waited: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            longest_ms: AtomicU64::new(0),
            #[cfg(test)]
            reported: Arc::default(),
        }
    }

    /// The counts: released, placed, waited, refused, the longest wait in ms.
    fn counts(&self) -> [u64; 5] {
        [
            &self.released,
            &self.placed,
            &self.waited,
            &self.refused,
            &self.longest_ms,
        ]
        .map(|count| count.load(Ordering::Relaxed))
    }
}

impl Drop for ReleaseTally {
    /// The last waiter that needed a forward is answered or gone: one line.
    fn drop(&mut self) {
        let counts = self.counts();
        #[cfg(test)]
        {
            *lock(&self.reported) = Some(counts);
        }
        let [released, placed, waited, refused, longest_wait_ms] = counts;
        tracing::info!(
            shard = %self.prefix,
            released,
            placed,
            waited,
            refused,
            longest_wait_ms,
            "handoff_released"
        );
    }
}

/// A held waiter's wait for a place: its length lands in the tally however the wait ends, placed,
/// at its bound, or with its client gone.
struct Waiting<'a> {
    tally: &'a ReleaseTally,
    began: Instant,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let ms = u64::try_from(self.began.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.tally.longest_ms.fetch_max(ms, Ordering::Relaxed);
    }
}

/// RAII place of one forward waiting for its owner's answer.
pub(crate) struct ForwardTicket {
    ctl: AdmissionController,
}

impl Drop for ForwardTicket {
    /// The forward has its answer: its place goes to the oldest held waiter still waiting, and
    /// back to the count only when none waits.
    fn drop(&mut self) {
        let places = &self.ctl.inner.forwards;
        let mut queue = lock(&places.queue);
        while let Some(next) = queue.pop_front() {
            if next.send(()).is_ok() {
                return;
            }
        }
        places.waiting.fetch_sub(1, Ordering::Relaxed);
    }
}

impl AdmissionController {
    /// A place for one more forward waiting for its owner's answer: `None`
    /// when a quarter of the in-flight cap (at least one; 128 with no cap)
    /// already wait.
    pub(crate) fn enter_forward(&self) -> Option<ForwardTicket> {
        let cap = match self.max_inflight() {
            cap if cap > 0 => (cap / 4).max(1),
            _ => UNCAPPED,
        };
        let waiting = &self.inner.forwards.waiting;
        if waiting.fetch_add(1, Ordering::Relaxed) >= cap {
            waiting.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(ForwardTicket { ctl: self.clone() })
    }

    /// A place for the served request's forward: one free now; else, for a request that waited
    /// on a planned move's hold holding a waiting place (`park::waits_on_move`), the first one a
    /// finished forward frees within `RELEASE_WAIT`, in order; `None` past that bound, and at once
    /// for any other request. The place is given, or refused, at this one match.
    pub(crate) async fn forward_place(&self) -> Option<ForwardTicket> {
        let held = super::park::moved_from().map(|prefix| self.joined(&prefix));
        let place = match (self.enter_forward(), &held) {
            (Some(place), _) => Some(place),
            (None, Some(tally)) => self.waited_place(tally).await,
            (None, None) => None,
        };
        if let Some(tally) = &held {
            let counted = if place.is_some() {
                &tally.placed
            } else {
                &tally.refused
            };
            counted.fetch_add(1, Ordering::Relaxed);
        }
        place
    }

    /// The tally of the planned move that held `prefix`, joined by the served request: made by
    /// the first of its waiters to need a forward, kept by each until it is answered or gone, and
    /// counted once per request (each asks for one place, `http::forward::serve`).
    fn joined(&self, prefix: &str) -> Arc<ReleaseTally> {
        let tally = {
            let mut tallies = lock(&self.inner.forwards.tallies);
            tallies.retain(|_, tally| tally.strong_count() > 0);
            match tallies.get(prefix).and_then(Weak::upgrade) {
                Some(tally) => tally,
                None => {
                    let tally = Arc::new(ReleaseTally::new(prefix));
                    tallies.insert(prefix.to_string(), Arc::downgrade(&tally));
                    tally
                }
            }
        };
        super::park::keep_tally(Arc::clone(&tally));
        tally.released.fetch_add(1, Ordering::Relaxed);
        tally
    }

    /// A held waiter's place: the first free within `RELEASE_WAIT`, one free now or the next a
    /// finished forward hands over, in order. A place handed over just as the bound passes is kept,
    /// not lost; a waiter whose client goes away leaves its turn to the next.
    async fn waited_place(&self, tally: &ReleaseTally) -> Option<ForwardTicket> {
        tally.waited.fetch_add(1, Ordering::Relaxed);
        let _waiting = Waiting {
            tally,
            began: Instant::now(),
        };
        let mut handed = {
            let mut queue = lock(&self.inner.forwards.queue);
            if let Some(place) = self.enter_forward() {
                return Some(place);
            }
            let (sender, receiver) = tokio::sync::oneshot::channel();
            queue.push_back(sender);
            receiver
        };
        let placed = match tokio::time::timeout(RELEASE_WAIT, &mut handed).await {
            Ok(sent) => sent.is_ok(),
            Err(_) => {
                handed.close();
                handed.try_recv().is_ok()
            }
        };
        placed.then(|| ForwardTicket { ctl: self.clone() })
    }

    /// The forwards waiting for their owners' answers now.
    #[cfg(test)]
    pub(crate) fn forwards_waiting(&self) -> i64 {
        self.inner.forwards.waiting.load(Ordering::Relaxed)
    }

    /// The requests holding a waiting place now (`park::waits_on_owner`): a move's pile parked on
    /// its hold.
    #[cfg(test)]
    pub(crate) fn waiting_places(&self) -> i64 {
        self.inner.waiting.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;
