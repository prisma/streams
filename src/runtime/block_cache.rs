//! One SlateDB block cache that can let go of everything it holds.
//!
//! foyer evicts only to admit an insert: it has no idle sweep. After a load a
//! full cache stays full, and its entries pin allocator pages well beyond
//! their own size (Q0: 162 MiB of cached blocks held 206-265 MiB of mimalloc
//! pages at idle). A release swaps in an empty generation, so the old one's
//! entries and hash tables are freed together when its last reader lets go.
//! foyer's own `clear()` would not do: it keeps the tables' capacity and its
//! usage counter, so a cleared cache believes it is still full.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A SlateDB block cache held as a replaceable generation of a foyer cache.
/// Every database that shares it shares the generation in force.
pub(crate) struct BlockCache {
    /// Captured once, so each generation is built alike and the host's CPU
    /// count (foyer's shard count) is read once.
    options: FoyerCacheOptions,
    current: ArcSwap<Generation>,
    releases: AtomicU64,
}

/// One generation: the foyer cache and the bytes inserted into it.
struct Generation {
    cache: FoyerCache,
    inserted: AtomicU64,
}

impl Generation {
    fn new(options: FoyerCacheOptions) -> Self {
        Self {
            cache: FoyerCache::new_with_opts(options),
            inserted: AtomicU64::new(0),
        }
    }
}

impl BlockCache {
    pub(crate) fn new(options: FoyerCacheOptions) -> Self {
        Self {
            options,
            current: ArcSwap::from_pointee(Generation::new(options)),
            releases: AtomicU64::new(0),
        }
    }

    /// A cache bounded at `bytes`, sharded by the host's CPU count (slatedb's
    /// default for its foyer cache).
    pub(crate) fn with_capacity(bytes: u64) -> Self {
        Self::new(FoyerCacheOptions {
            max_capacity: bytes,
            ..Default::default()
        })
    }

    /// Replaces the generation in force with an empty one of the full
    /// capacity and returns the bytes inserted into the generation it
    /// replaced. The old generation is freed when its last reader finishes:
    /// a fetch in flight holds its generation and completes on it. The next
    /// miss reads the store and fills the new generation.
    pub(crate) fn release(&self) -> u64 {
        let old = self.current.swap(Arc::new(Generation::new(self.options)));
        self.releases.fetch_add(1, Ordering::Relaxed);
        old.inserted.load(Ordering::Relaxed)
    }

    /// Bytes inserted into the generation in force, as foyer weighs them.
    /// Reads that miss fill a generation without inserting: this counts what
    /// writes (cache-on-write of flushed and compacted tables) put there.
    pub(crate) fn inserted_bytes(&self) -> u64 {
        self.current.load().inserted.load(Ordering::Relaxed)
    }

    /// How many times this cache has been released.
    pub(crate) fn releases(&self) -> u64 {
        self.releases.load(Ordering::Relaxed)
    }
}

/// Every call reads through the generation in force and holds it until the
/// call completes, across a fetch's load included.
#[async_trait]
impl DbCache for BlockCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.current.load_full().cache.get_block(key).await
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.current.load_full().cache.get_index(key).await
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.current.load_full().cache.get_filter(key).await
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, slatedb::Error> {
        self.current.load_full().cache.get_stats(key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        let generation = self.current.load_full();
        let bytes = value.size() as u64;
        generation.cache.insert(key, value).await;
        generation.inserted.fetch_add(bytes, Ordering::Relaxed);
    }

    async fn remove(&self, key: &CachedKey) {
        self.current.load_full().cache.remove(key).await;
    }

    fn entry_count(&self) -> u64 {
        self.current.load().cache.entry_count()
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.current
            .load_full()
            .cache
            .fetch_block(key, loader)
            .await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.current
            .load_full()
            .cache
            .fetch_index(key, loader)
            .await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.current
            .load_full()
            .cache
            .fetch_filter(key, loader)
            .await
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, slatedb::Error> {
        self.current
            .load_full()
            .cache
            .fetch_stats(key, loader)
            .await
    }
}

#[cfg(test)]
mod tests;
