//! The feed registry: incarnation-safe subscribe/retire for LiveFeeds.
//! `subscribe()` is ATOMIC — lookup/create, subscriber increment and
//! handle return all happen under the registry lock, closing the
//! last-out-eviction versus new-join race (follow-up review finding
//! 3). The returned `FeedSubscription` is an RAII guard: dropping it
//! decrements the count, clears shared retention when the crowd falls
//! to one, and removes the feed (by pointer identity) at zero.

use super::feed::{FeedKey, LiveFeed};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct FeedRegistry {
    map: Mutex<HashMap<FeedKey, Arc<LiveFeed>>>,
}

/// RAII subscription handle. Dropping it detaches the session from the
/// feed: decrement → clear-retention-at-one → remove-at-zero.
pub(crate) struct FeedSubscription {
    registry: Arc<FeedRegistry>,
    pub(crate) key: FeedKey,
    pub(crate) feed: Arc<LiveFeed>,
}

impl FeedSubscription {
    pub(crate) fn feed(&self) -> Arc<LiveFeed> {
        self.feed.clone()
    }
}

impl Drop for FeedSubscription {
    fn drop(&mut self) {
        let remaining = self.feed.leave();
        if remaining == 1 {
            // Shared → solo: retained batches would never be consumed
            // by anyone but the lone survivor driving fresh reads.
            self.feed.clear_retention();
        }
        if remaining == 0 {
            self.registry.remove_if_same(&self.key, &self.feed);
        }
    }
}

impl FeedRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Atomic subscribe: create-or-lookup AND subscriber increment
    /// happen under one lock hold, so a last-out eviction can never
    /// land between another session's lookup and its join.
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        key: FeedKey,
        make: impl FnOnce() -> Arc<LiveFeed>,
    ) -> FeedSubscription {
        let mut map = self.map.lock().unwrap();
        let feed = match map.get(&key) {
            Some(f) => f.clone(),
            None => {
                let f = make();
                map.insert(key.clone(), f.clone());
                f
            }
        };
        feed.join();
        FeedSubscription {
            registry: Arc::clone(self),
            key,
            feed,
        }
    }

    /// Remove ONLY if the entry is still this exact feed — cleanup from
    /// a replaced incarnation must never delete its successor.
    fn remove_if_same(&self, key: &FeedKey, expected: &Arc<LiveFeed>) {
        let mut map = self.map.lock().unwrap();
        if map
            .get(key)
            .is_some_and(|actual| Arc::ptr_eq(actual, expected))
        {
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
