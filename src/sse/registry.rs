//! The feed registry: incarnation-safe subscribe/retire for LiveFeeds.
//!
//! `subscribe()` is ATOMIC — lookup/create, subscriber increment and
//! handle return happen under the registry lock — and `unsubscribe()`
//! performs verify-pointer/decrement/evict under that SAME lock,
//! closing the last-out/new-join race (follow-up review finding 4).
//! Entering SHARED mode additionally reserves this feed's ring
//! allowance from the process-global budget; failure rejects the NEW
//! subscriber with a typed capacity error instead of silently dropping
//! shared delivery (finding 6-mem).

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
        // Finding 6-mem (option A): entering SHARED mode requires one
        // exact reservation of this feed's ring allowance from the
        // process-global budget. Failure rejects the NEW subscriber —
        // never silently drops shared delivery. The existing singleton
        // continues normally.
        if feed.subscriber_count() >= 2 && !feed.reserve_shared_allowance() {
            feed.leave_locked();
            if feed.subscriber_count() == 0 {
                map.remove(&key);
            }
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

    /// ATOMIC detach: verify pointer identity, decrement, release
    /// retention at drop-to-one, remove ONLY when the post-decrement
    /// count is zero — all under the same lock `subscribe` holds.
    fn unsubscribe(&self, key: &FeedKey, expected: &Arc<LiveFeed>) {
        let mut map = self.map.lock().unwrap();
        if !map.get(key).is_some_and(|f| Arc::ptr_eq(f, expected)) {
            return;
        }
        let remaining = expected.leave_locked();
        if remaining == 1 {
            expected.clear_retention();
        }
        if remaining == 0 {
            map.remove(&key);
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
