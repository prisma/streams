//! A planned move's hold in the directory (S5 addendum F6 rev 2, step 4;
//! GATE-DECISION-f6-prep-rows-rev2-2026-11-17 H1, H3, H5).
//!
//! From the moment a giver takes a prefix out of its serving map until the move ends, every
//! resolve of that prefix waits here instead of opening it: appends, reads, waiting reads, queue
//! pulls and a live session's re-resolve alike. The wait ends when the move does:
//! - **moved**: the overrides document and this instance's mirror name the new owner, so the
//!   waiter's ownership check answers `NotOwner` and the forward layer sends it there;
//! - **kept**: the move was dropped, and the waiter resolves as before, which may open the
//!   (flushed) shard here again;
//! - or at the hold's **deadline**, an absolute instant two seconds after the park (`HOLD`): a
//!   waiter, or a request that arrives after it while the mark still stands, is answered at once
//!   (`503 shard_moving`, `Retry-After: 1`; H3).
//!
//! A `Mark` is a guard: dropped before it ended (its handoff was cancelled or its task died),
//! it ends the hold as kept, so no prefix is left marked.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;
use tokio::time::Instant;

/// How a planned move ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum End {
    /// The new owner is named: a waiter's ownership check answers it.
    Moved,
    /// The move was dropped: a waiter resolves as it would have.
    Kept,
}

/// What a resolve found for its prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Waited {
    /// No move holds the prefix, or the one that did ended.
    Free,
    /// A move still holds it past its deadline (H3).
    Held,
}

/// The prefixes a planned move holds on this instance.
#[derive(Default)]
pub(crate) struct Handoffs {
    // mt-lint: allow(name-keyed-map): shard prefix -> the planned move holding it
    held: Mutex<HashMap<String, Hold>>,
    /// The serial the next hold takes.
    next: std::sync::atomic::AtomicU64,
}

/// One prefix's hold: its deadline and its end, once known.
struct Hold {
    until: Instant,
    end: watch::Receiver<Option<End>>,
    /// Distinguishes this hold from a later one of the same prefix, so a late drop removes only
    /// its own.
    serial: u64,
}

/// A planned move's hold on one prefix; it ends when [`Mark::end`] is called or it is dropped.
pub(crate) struct Mark {
    handoffs: Arc<Handoffs>,
    prefix: String,
    serial: u64,
    end: watch::Sender<Option<End>>,
}

impl Handoffs {
    /// Hold `prefix` until `until` at the latest; `None` while another move holds it.
    pub(crate) fn begin(self: &Arc<Self>, prefix: &str, until: Instant) -> Option<Mark> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if held
            .get(prefix)
            .is_some_and(|hold| hold.end.borrow().is_none())
        {
            return None;
        }
        let serial = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (end, rx) = watch::channel(None);
        let hold = Hold {
            until,
            end: rx,
            serial,
        };
        held.insert(prefix.to_string(), hold);
        Some(Mark {
            handoffs: Arc::clone(self),
            prefix: prefix.to_string(),
            serial,
            end,
        })
    }

    /// Wait for the move holding `prefix`, if one does, to end or to pass its deadline.
    pub(crate) async fn wait(&self, prefix: &str) -> Waited {
        let Some((until, mut end)) = self.hold(prefix) else {
            return Waited::Free;
        };
        let ended = end.wait_for(Option::is_some);
        match tokio::time::timeout_at(until, ended).await {
            Ok(_) => Waited::Free,
            Err(_) => Waited::Held,
        }
    }

    /// Whether a move holds `prefix` now (unended, whatever its deadline).
    pub(crate) fn holds(&self, prefix: &str) -> bool {
        self.hold(prefix).is_some()
    }

    /// The unended hold of `prefix`: its deadline and its end.
    fn hold(&self, prefix: &str) -> Option<(Instant, watch::Receiver<Option<End>>)> {
        let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        let hold = held.get(prefix)?;
        hold.end
            .borrow()
            .is_none()
            .then(|| (hold.until, hold.end.clone()))
    }
}

impl Mark {
    /// End the hold as `how`, waking every waiter.
    pub(crate) fn end(self, how: End) {
        self.finish(how);
    }

    fn finish(&self, how: End) {
        let mut held = self
            .handoffs
            .held
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if held
            .get(&self.prefix)
            .is_some_and(|hold| hold.serial == self.serial)
        {
            held.remove(&self.prefix);
        }
        drop(held);
        self.end.send_if_modified(|end| {
            let first = end.is_none();
            if first {
                *end = Some(how);
            }
            first
        });
    }
}

impl Drop for Mark {
    /// A hold whose move never ended it ends as kept (H1).
    fn drop(&mut self) {
        self.finish(End::Kept);
    }
}

#[cfg(test)]
mod tests {
    use super::{End, Handoffs, Waited};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test(crate = "crate::test_rt")]
    async fn a_waiter_is_released_by_the_end_and_a_drop_ends_the_hold_as_kept() {
        let handoffs = Arc::new(Handoffs::default());
        assert_eq!(handoffs.wait("0").await, Waited::Free, "no hold, no wait");
        let mark = handoffs
            .begin("0", Instant::now() + Duration::from_secs(5))
            .expect("the first hold");
        assert!(
            handoffs.begin("0", Instant::now()).is_none(),
            "one hold per prefix"
        );
        assert!(handoffs.holds("0") && !handoffs.holds("1"));
        let ended = Instant::now();
        let (waited, ()) = futures_util::future::join(handoffs.wait("0"), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            mark.end(End::Moved);
        })
        .await;
        assert_eq!(waited, Waited::Free, "the end releases the waiter");
        assert!(
            ended.elapsed() < Duration::from_secs(2),
            "{:?}",
            ended.elapsed()
        );
        assert!(!handoffs.holds("0"), "an ended hold is gone");
        let dropped = handoffs.begin("0", Instant::now() + Duration::from_secs(5));
        drop(dropped);
        assert!(!handoffs.holds("0"), "a dropped mark ends its hold");
    }

    #[tokio::test(crate = "crate::test_rt")]
    async fn the_deadline_is_absolute() {
        let handoffs = Arc::new(Handoffs::default());
        let until = Instant::now() + Duration::from_millis(100);
        let _mark = handoffs.begin("0", until).expect("a hold");
        assert_eq!(handoffs.wait("0").await, Waited::Held, "past its deadline");
        let late = Instant::now();
        assert_eq!(handoffs.wait("0").await, Waited::Held, "a late request");
        assert!(
            late.elapsed() < Duration::from_millis(50),
            "answered at once: {:?}",
            late.elapsed()
        );
    }
}
