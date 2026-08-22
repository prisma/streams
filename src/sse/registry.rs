//! The feed registry: incarnation-safe lookup/create/retire for
//! LiveFeeds (LIVE-FEED Stage 2). Keyed by `FeedKey` (segment
//! identity — epoch-bound), so a delete/recreate under the same name
//! lands on a fresh feed by construction. A feed whose last session
//! left is evicted lazily by the leaving session.

use super::feed::{FeedKey, LiveFeed};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct FeedRegistry {
    map: Mutex<HashMap<FeedKey, Arc<LiveFeed>>>,
}

impl FeedRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_or_create(
        &self,
        key: FeedKey,
        make: impl FnOnce() -> Arc<LiveFeed>,
    ) -> Arc<LiveFeed> {
        let mut map = self.map.lock().unwrap();
        match map.get(&key) {
            Some(f) => f.clone(),
            None => {
                let f = make();
                map.insert(key.clone(), f.clone());
                f
            }
        }
    }

    /// Called by a session on exit: retire the feed if it has no
    /// subscribers left (last-out cleanup; a racing join re-creates).
    pub(crate) fn evict_if_unsubscribed(&self, key: &FeedKey) {
        let mut map = self.map.lock().unwrap();
        if let Some(f) = map.get(key)
            && f.subscriber_count() == 0
        {
            map.remove(key);
        }
    }

    #[cfg(test)]
    pub(crate) fn len_for_test(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Test hook: the live feed for a key, if one exists.
    #[cfg(test)]
    pub(crate) fn feed_for_test(&self, key: &FeedKey) -> Option<Arc<LiveFeed>> {
        self.map.lock().unwrap().get(key).cloned()
    }
}
