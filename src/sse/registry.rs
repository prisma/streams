//! The feed registry: incarnation-safe subscribe/retire for LiveFeeds.
//!
//! `subscribe()` is ATOMIC — lookup/create, subscriber increment and
//! handle return happen under the registry lock — and `unsubscribe()`
//! performs verify-pointer/decrement/evict under that SAME lock,
//! closing the last-out/new-join race (follow-up review finding 4).
//! SHARED admission (the 1→2 transition) is validated BEFORE the
//! attach on static configuration (nonzero ring AND nonzero global
//! budget); retention itself reserves the ACTUAL retained bytes per
//! batch from the process-global budget (budget model B).

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
    /// Source generation CAPTURED under the SAME subscribe lock
    /// (review round 4: reading it after the fact reopens the
    /// construction race). Raw sessions take the typed fallback as
    /// soon as the feed's generation differs from this.
    pub(crate) join_gen: u64,
    /// Persistent source-generation receiver for this session.
    pub(crate) gen_rx: tokio::sync::watch::Receiver<u64>,
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

    pub(crate) fn join_gen(&self) -> u64 {
        self.join_gen
    }

    pub(crate) fn gen_rx(&self) -> tokio::sync::watch::Receiver<u64> {
        self.gen_rx.clone()
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
        reconcile: Option<&Arc<dyn crate::sse::feed::FeedSourceRead>>,
    ) -> Result<FeedSubscription, CapacityRejected> {
        let mut map = self.map.lock().unwrap();
        type Captured = (
            Arc<LiveFeed>,
            u64,
            tokio::sync::watch::Receiver<u64>,
            u64,
            tokio::sync::watch::Receiver<u64>,
        );
        let (feed, join_head, ver_rx, join_gen, gen_rx): Captured = match map.get(&key) {
            Some(f) => {
                // Stage 7A: reconcile the existing feed's source with
                // the REQUESTED one (built from the current descriptor)
                // BEFORE any join state is captured — a feed that
                // predates a topology change must not hand this
                // subscriber a stale frontier or generation.
                if let Some(requested) = reconcile {
                    f.reconcile_locked(requested);
                }
                // SHARED admission (follow-up review finding 1): the
                // 1→2 transition is validated BEFORE the attach, on
                // STATIC configuration (nonzero ring AND nonzero
                // global budget) — never after exposing a count the
                // memory posture cannot support. There is nothing to
                // roll back on refusal because nothing happened yet.
                if f.subscriber_count() == 1 && !f.can_share() {
                    crate::sse::auth::sse_stats::FEED_CAPACITY_REJECTED
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Err(CapacityRejected);
                }
                let (head, rx, generation, grx) = f.subscribe_locked();
                (f.clone(), head, rx, generation, grx)
            }
            None => {
                let f = make();
                let (head, rx, generation, grx) = f.subscribe_locked();
                map.insert(key.clone(), f.clone());
                (f, head, rx, generation, grx)
            }
        };
        Ok(FeedSubscription {
            registry: Arc::clone(self),
            key,
            feed,
            join_head,
            ver_rx,
            join_gen,
            gen_rx,
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
            // Round-11.1: teardown cancels every in-flight source
            // operation of the evicted feed (only the LAST leave —
            // one subscriber's disconnect never cancels shared work).
            expected.cancel.fire();
        }
    }

    /// Live feed count (observability: /v1/debug/load).
    pub(crate) fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Peek at an existing feed (no attach) — the raw compatibility
    /// gate inspects the current source generation BEFORE subscribing.
    pub(crate) fn feed_for(&self, key: &FeedKey) -> Option<Arc<LiveFeed>> {
        self.map.lock().unwrap().get(key).cloned()
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

    fn tpid() -> crate::tenant::ProjectId {
        crate::tenant::ProjectId::new("proj-feed-test").unwrap()
    }

    fn make_feed(key: FeedKey, budget: &Arc<FeedMemoryBudget>) -> Arc<LiveFeed> {
        let src = Arc::new(FakeSource::new(0, 8));
        LiveFeed::new_with_budget(key, src, RING, budget.clone(), tpid())
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
                .subscribe(key(1), || make_feed(key(1), &budget), None)
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

    /// Budget model B, through the registry: subscribers NEVER reserve —
    /// only retained batches do. Many subscribers on one feed cost
    /// nothing until a publication retains bytes.
    #[test]
    fn shared_subscribers_cost_nothing_until_retention() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(16 * RING as u64));
        let s1 = registry
            .subscribe(key(1), || make_feed(key(1), &budget), None)
            .expect("s1");
        let s2 = registry
            .subscribe(key(1), || make_feed(key(1), &budget), None)
            .expect("s2");
        let s3 = registry
            .subscribe(key(1), || make_feed(key(1), &budget), None)
            .expect("s3");
        assert_eq!(s1.feed.subscriber_count(), 3);
        assert_eq!(
            budget.reserved(),
            0,
            "no reservation exists before any retained batch (model B)"
        );
        drop(s1);
        drop(s2);
        assert_eq!(registry.len_for_test(), 1, "feed survives at one");
        drop(s3);
        assert_eq!(registry.len_for_test(), 0);
        assert_eq!(budget.reserved(), 0);
    }

    /// Finding 1 (red): the 1→2 admission check is STATIC and happens
    /// BEFORE the attach — a refusal leaves NO partial state: count
    /// untouched, feed resident, nothing reserved.
    #[test]
    fn shared_refusal_leaves_no_partial_state() {
        let registry = Arc::new(FeedRegistry::new());
        // Zero global budget: sharing is statically impossible.
        let budget = Arc::new(FeedMemoryBudget::new_for_test(0));
        let s1 = registry
            .subscribe(key(1), || make_feed(key(1), &budget), None)
            .expect("s1");
        for _ in 0..3 {
            let rejected = registry.subscribe(key(1), || make_feed(key(1), &budget), None);
            assert!(matches!(rejected, Err(CapacityRejected)));
        }
        assert_eq!(
            s1.feed.subscriber_count(),
            1,
            "repeated refusals never leak an attach"
        );
        assert_eq!(registry.len_for_test(), 1);
        assert_eq!(budget.reserved(), 0);
        drop(s1);
        assert_eq!(registry.len_for_test(), 0);
    }

    /// Zero-budget contract (docs/LIVE-FEED.md): singleton-only — the
    /// second subscriber to the same feed gets the capacity refusal.
    #[test]
    fn zero_budget_is_singleton_only() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(0));
        let s1 = registry
            .subscribe(key(1), || make_feed(key(1), &budget), None)
            .expect("singleton admitted at zero budget");
        let rejected = registry.subscribe(key(1), || make_feed(key(1), &budget), None);
        assert!(matches!(rejected, Err(CapacityRejected)));
        assert_eq!(s1.feed.subscriber_count(), 1);
        drop(s1);
        assert_eq!(registry.len_for_test(), 0);
    }

    /// Zero-RING contract (follow-up review finding 4): a zero-byte
    /// ring cannot hold any batch, so the second subscriber is refused
    /// BEFORE attach rather than admitted into instant lag.
    #[test]
    fn zero_ring_is_singleton_only() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
        let s1 = registry
            .subscribe(
                key(1),
                || {
                    let src = Arc::new(FakeSource::new(0, 8));
                    LiveFeed::new_with_budget(key(1), src, 0, budget.clone(), tpid())
                },
                None,
            )
            .expect("singleton admitted on a zero-ring feed");
        let rejected = registry.subscribe(
            key(1),
            || {
                let src = Arc::new(FakeSource::new(0, 8));
                LiveFeed::new_with_budget(key(1), src, 0, budget.clone(), tpid())
            },
            None,
        );
        assert!(matches!(rejected, Err(CapacityRejected)));
        assert_eq!(s1.feed.subscriber_count(), 1);
        drop(s1);
        assert_eq!(registry.len_for_test(), 0);
    }

    /// Model B capacity: shared admission is not rationed by feed —
    /// 32 feeds × 2 subscribers all attach; the process budget is
    /// consumed only by ACTUAL retained bytes.
    #[test]
    fn many_shared_feeds_share_one_budget() {
        let registry = Arc::new(FeedRegistry::new());
        let budget = Arc::new(FeedMemoryBudget::new_for_test(4 * RING as u64));
        let mut subs = Vec::new();
        for n in 0..32u8 {
            subs.push(
                registry
                    .subscribe(key(n), || make_feed(key(n), &budget), None)
                    .expect("singleton"),
            );
            subs.push(
                registry
                    .subscribe(key(n), || make_feed(key(n), &budget), None)
                    .expect("second subscriber — model B never rations feeds"),
            );
        }
        assert_eq!(registry.len_for_test(), 32);
        assert_eq!(budget.reserved(), 0, "nothing retained yet");
        drop(subs);
        assert_eq!(registry.len_for_test(), 0);
        assert_eq!(budget.reserved(), 0, "budget returns exactly to zero");
    }
}
