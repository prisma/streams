//! The feed registry: incarnation-safe subscribe/retire for LiveFeeds.
//!
//! `subscribe()` is ATOMIC — lookup/create, subscriber increment and
//! handle return happen under the registry lock — and `unsubscribe()`
//! performs verify-pointer/decrement/evict under that SAME lock,
//! closing the last-out/new-join race (follow-up review finding 4).
//! Entering SHARED mode (the 1→2 transition) reserves this feed's ring
//! allowance from the process-global budget EXACTLY ONCE; failure
//! rejects the NEW subscriber with a typed capacity error instead of
//! silently dropping shared delivery (finding 6-mem redesign).

use super::feed::{FeedKey, LiveFeed};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Typed refusal when the process-global retention budget cannot host
/// another SHARED feed (maps to 503 subscription_capacity).
#[derive(Debug)]
pub(crate) struct CapacityRejected;

#[derive(Default)]
pub(crate) struct FeedRegistry {
    map: Mutex<HashMap<FeedKey, Arc<LiveFeed>>>,
}

/// RAII subscription handle: attach-on-create, detach-on-drop.
pub(crate) struct FeedSubscription {
    registry: Arc<FeedRegistry>,
    pub(crate) key: FeedKey,
    pub(crate) feed: Arc<LiveFeed>,
    /// Feed head CAPTURED under the subscribe lock (finding 8): Phase A
    /// catches up to exactly this bound, never the moving frontier.
    pub(crate) join_head: u64,
    /// Persistent version receiver for this session.
    pub(crate) ver_rx: tokio::sync::watch::Receiver<u64>,
}

impl FeedSubscription {
    pub(crate) fn feed(&self) -> Arc<LiveFeed> {
        self.feed.clone()
    }

    pub(crate) fn join_head(&self) -> u64 {
        self.join_head
    }

    pub(crate) fn version_rx(&self) -> tokio::sync::watch::Receiver<u64> {
        self.ver_rx.clone()
    }
}

impl Drop for FeedSubscription {
    fn drop(&mut self) {
        self.registry.unsubscribe(&self.key, &self.feed);
    }
}

impl FeedRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Atomic create-or-join + captured head/version, all under one
    /// lock hold.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        key: FeedKey,
        make: impl FnOnce() -> Arc<LiveFeed>,
    ) -> Result<FeedSubscription, CapacityRejected> {
        let mut map = self.map.lock().unwrap();
        let (feed, join_head, ver_rx): (Arc<LiveFeed>, u64, tokio::sync::watch::Receiver<u64>) =
            match map.get(&key) {
                Some(f) => {
                    let (head, rx) = f.subscribe_locked();
                    (f.clone(), head, rx)
                }
                None => {
                    let f = make();
                    let (head, rx) = f.subscribe_locked();
                    map.insert(key.clone(), f.clone());
                    (f, head, rx)
                }
            };
        // Finding 6-mem (redesign): the 1→2 transition reserves this
        // feed's ring allowance from the process-global budget EXACTLY
        // ONCE (enter_shared_locked is idempotent; 2→3+ and re-entry
        // after a solo dip cost nothing while the reservation is held).
        // Failure rejects THE NEW subscriber — never silently drops
        // shared delivery. The existing singleton continues normally.
        if feed.subscriber_count() == 2 && !feed.enter_shared_locked() {
            let remaining = feed.leave_locked();
            debug_assert_eq!(remaining, 1, "the pre-existing singleton remains");
            return Err(CapacityRejected);
        }
        Ok(FeedSubscription {
            registry: Arc::clone(self),
            key,
            feed,
            join_head,
            ver_rx,
        })
    }

    /// ATOMIC detach: verify pointer identity, decrement, remove ONLY
    /// when the POST-decrement count is zero — all under the same lock
    /// `subscribe` holds. The 2→1 transition deliberately KEEPS the
    /// retained ring: the survivor may still have unread batches, and
    /// clearing them would disconnect it as lagged for another
    /// subscriber's departure (follow-up review finding 4). Solo drives
    /// stop retaining new batches; the ring and its budget allowance
    /// are released when the feed is dropped at zero subscribers.
    fn unsubscribe(&self, key: &FeedKey, expected: &Arc<LiveFeed>) {
        let mut map = self.map.lock().unwrap();
        if !map.get(key).is_some_and(|f| Arc::ptr_eq(f, expected)) {
            return;
        }
        let remaining = expected.leave_locked();
        if remaining == 0 {
            map.remove(key);
        }
    }

    #[cfg(test)]
    pub(crate) fn len_for_test(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn feed_for_test(&self, key: &FeedKey) -> Option<Arc<LiveFeed>> {
        self.map.lock().unwrap().get(key).cloned()
    }
}

// ==================================================================
// Unit tests (follow-up review: lifecycle coverage). Deterministic
// registry-level shapes: eviction at zero, budget accounting across
// the shared transition, capacity rejection.
// ==================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sse::feed::FeedMemoryBudget;
    use crate::sse::feed::tests::FakeSource;

    const RING: usize = 4096;

    fn make_feed(key: FeedKey, budget: &Arc<FeedMemoryBudget>) -> Arc<LiveFeed> {
        let src = Arc::new(FakeSource::new(0, 8));
        LiveFeed::new_with_budget(key, src, RING, budget.clone())
    }

    fn key(n: u8) -> FeedKey {
        FeedKey::default_lane([n; 16])
    }

    /// Finding 1 (red): the last subscriber's drop EVICTS the feed;
    /// repeated connect/disconnect never grows the registry.
    #[test]
    fn last_subscriber_evicts_and_reconnect_does_not_grow() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        for round in 0..3 {
            let sub = registry
                .subscribe(key(1), || make_feed(key(1), &budget))
                .expect("subscribe");
            assert_eq!(registry.len_for_test(), 1, "round {round}: one live feed");
            assert_eq!(sub.feed.subscriber_count(), 1);
            drop(sub);
            assert_eq!(
                registry.len_for_test(),
                0,
                "round {round}: zero subscribers evicts the feed"
            );
        }
        assert_eq!(registry.len_for_test(), 0);
    }

    /// Finding 6-mem (red), through the registry: three subscribers
    /// reserve EXACTLY ONE allowance; final teardown returns the budget
    /// to zero.
    #[test]
    fn three_subscribers_one_allowance_released_at_zero() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(16 * RING as u64));
        let s1 = registry
            .subscribe(key(1), || make_feed(key(1), &budget))
            .expect("s1");
        assert_eq!(budget.reserved(), 0, "singletons reserve nothing");
        let s2 = registry
            .subscribe(key(1), || make_feed(key(1), &budget))
            .expect("s2");
        assert_eq!(
            budget.reserved(),
            RING as u64,
            "the 1->2 transition reserves one allowance"
        );
        let s3 = registry
            .subscribe(key(1), || make_feed(key(1), &budget))
            .expect("s3");
        assert_eq!(
            budget.reserved(),
            RING as u64,
            "the third subscriber reserves nothing further"
        );
        drop(s1);
        drop(s2);
        assert_eq!(
            budget.reserved(),
            RING as u64,
            "drop-to-one keeps the allowance (the survivor may still drain)"
        );
        assert_eq!(registry.len_for_test(), 1, "feed survives at one");
        drop(s3);
        assert_eq!(registry.len_for_test(), 0);
        assert_eq!(budget.reserved(), 0, "final teardown releases it");
    }

    /// Finding 6-mem (red): budget exhaustion rejects the SECOND
    /// subscriber with the typed error; the pre-existing singleton and
    /// the feed survive untouched.
    #[test]
    fn capacity_rejection_preserves_singleton_and_feed() {
        let registry = Arc::new(FeedRegistry::new());
        // Room for exactly ONE shared allowance.
        let budget = Arc::new(FeedMemoryBudget::new_for_test(RING as u64));
        let a1 = registry
            .subscribe(key(1), || make_feed(key(1), &budget))
            .expect("a1");
        let a2 = registry
            .subscribe(key(1), || make_feed(key(1), &budget))
            .expect("a2 enters shared mode");
        assert_eq!(budget.reserved(), RING as u64);

        let b1 = registry
            .subscribe(key(2), || make_feed(key(2), &budget))
            .expect("b1 singleton needs no allowance");
        let rejected = registry.subscribe(key(2), || make_feed(key(2), &budget));
        assert!(
            matches!(rejected, Err(CapacityRejected)),
            "the second subscriber on feed B must be rejected at capacity"
        );
        assert_eq!(
            b1.feed.subscriber_count(),
            1,
            "the rejected join rolled its attach back"
        );
        assert_eq!(registry.len_for_test(), 2, "both feeds remain resident");
        assert_eq!(budget.reserved(), RING as u64, "no half-reservation");

        drop(a1);
        drop(a2);
        drop(b1);
        assert_eq!(registry.len_for_test(), 0);
        assert_eq!(budget.reserved(), 0);
    }

    /// Zero-budget contract (docs/LIVE-FEED.md): singleton-only — the
    /// second subscriber to the same feed gets the capacity refusal.
    #[test]
    fn zero_budget_is_singleton_only() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(0));
        let s1 = registry
            .subscribe(key(1), || make_feed(key(1), &budget))
            .expect("singleton admitted at zero budget");
        let rejected = registry.subscribe(key(1), || make_feed(key(1), &budget));
        assert!(matches!(rejected, Err(CapacityRejected)));
        assert_eq!(s1.feed.subscriber_count(), 1);
        drop(s1);
        assert_eq!(registry.len_for_test(), 0);
    }

    /// The cap is EXACT: feeds racing to enter shared mode fill the
    /// budget allowance-for-allowance; the first feed past the cap is
    /// refused, and full teardown returns the budget to zero.
    #[test]
    fn many_feeds_fill_the_cap_exactly() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(4 * RING as u64));
        let mut subs = Vec::new();
        // 32 singletons: no reservations at all.
        for n in 0..32u8 {
            subs.push(
                registry
                    .subscribe(key(n), || make_feed(key(n), &budget))
                    .expect("singleton"),
            );
        }
        assert_eq!(budget.reserved(), 0);
        // Second subscribers: exactly FOUR fit.
        let mut shared = 0;
        for n in 0..32u8 {
            if let Ok(s2) = registry.subscribe(key(n), || make_feed(key(n), &budget)) {
                shared += 1;
                subs.push(s2);
            }
        }
        assert_eq!(shared, 4, "the cap admits exactly four shared feeds");
        assert_eq!(budget.reserved(), 4 * RING as u64);
        drop(subs);
        assert_eq!(registry.len_for_test(), 0);
        assert_eq!(budget.reserved(), 0, "budget returns exactly to zero");
    }
}
