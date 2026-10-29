//! The sealed-read caches (layout 6, DESIGN-6A §2.4, REVIEW-6A F15): one
//! process-wide byte budget each, least recently used out first.
//!
//! - **Index cache**: run-index footers and runs, by object path and byte
//!   offset, `INDEX_CACHE_BYTES`.
//! - **Chunk cache**: whole sealed chunks of at most `CHUNK_CACHE_MAX_ITEM`
//!   bytes, by segment object path and chunk offset, `CHUNK_CACHE_BYTES`.
//!
//! Both are budget lines under the 500 MB shed line (`spill::budget`). A
//! value larger than an eighth of its cache's budget is not kept, so one
//! large item never empties a cache. Every value is immutable (objects are
//! written once, `PutMode::Create`), so a cached value is never stale.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use bytes::Bytes;

/// The index cache's budget.
pub(crate) const INDEX_CACHE_BYTES: u64 = 16 << 20;
/// The chunk cache's budget.
pub(crate) const CHUNK_CACHE_BYTES: u64 = 32 << 20;
/// The largest chunk the chunk cache keeps.
pub(crate) const CHUNK_CACHE_MAX_ITEM: u64 = 1 << 20;

struct Slot<V> {
    value: V,
    size: u64,
    tick: u64,
}

struct Inner<V> {
    // mt-lint: allow(name-keyed-map): keys are an engine's cache scope and an immutable object's store path with a byte offset, never stream or tenant names.
    map: HashMap<(String, u64), Slot<V>>,
    order: BTreeMap<u64, (String, u64)>,
    tick: u64,
    bytes: u64,
}

/// A byte-budgeted LRU of immutable values keyed by (object path, offset).
pub(crate) struct ByteLru<V> {
    inner: Mutex<Inner<V>>,
    budget: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl<V: Clone> ByteLru<V> {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: BTreeMap::new(),
                tick: 0,
                bytes: 0,
            }),
            budget,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    // A poisoned cache holds whole entries only (each insert or touch
    // finishes its map and order updates before it can panic), so its
    // state is usable as it stands.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<V>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn get(&self, path: &str, offset: u64) -> Option<V> {
        let mut g = self.lock();
        let inner = &mut *g;
        inner.tick += 1;
        let tick = inner.tick;
        let Some(slot) = inner.map.get_mut(&(path.to_string(), offset)) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        inner.order.remove(&slot.tick);
        slot.tick = tick;
        inner.order.insert(tick, (path.to_string(), offset));
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(slot.value.clone())
    }

    /// Keep `value` (of `size` bytes) unless it is over an eighth of the
    /// budget; evict the least recently used until the cache fits.
    pub(crate) fn insert(&self, path: &str, offset: u64, value: V, size: u64) {
        if size > self.budget / 8 {
            return;
        }
        let mut g = self.lock();
        let inner = &mut *g;
        inner.tick += 1;
        let tick = inner.tick;
        let key = (path.to_string(), offset);
        if let Some(old) = inner.map.insert(key.clone(), Slot { value, size, tick }) {
            inner.order.remove(&old.tick);
            inner.bytes -= old.size;
        }
        inner.order.insert(tick, key);
        inner.bytes += size;
        while inner.bytes > self.budget {
            let Some((_, oldest)) = inner.order.pop_first() else {
                break;
            };
            if let Some(gone) = inner.map.remove(&oldest) {
                inner.bytes -= gone.size;
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.lock().bytes
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let n = |c: &AtomicU64| c.load(Ordering::Relaxed);
        serde_json::json!({
            "bytes": self.bytes(),
            "budget": self.budget,
            "hits": n(&self.hits),
            "misses": n(&self.misses),
            "evictions": n(&self.evictions),
        })
    }
}

/// One engine's share of the process caches: its keys carry this scope, so
/// two engines never meet each other's entries, even for one object path on
/// two stores (tests open many in-memory stores under the same shard paths;
/// a reopened engine starts cold, which costs one GET per object).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Scope(u64);

impl Default for Scope {
    fn default() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

impl Scope {
    /// The cache key of `path` within this scope.
    pub(crate) fn key(self, path: &str) -> String {
        format!("{}:{path}", self.0)
    }
}

/// A cached run-index footer or run (decoded).
#[derive(Clone)]
pub(crate) enum IndexValue {
    Footer(std::sync::Arc<Vec<super::runindex::format::FooterEntry>>),
    Run(std::sync::Arc<Vec<super::directory::DirEntry>>),
}

/// The process's index cache.
pub(crate) fn index_cache() -> &'static ByteLru<IndexValue> {
    static CACHE: std::sync::LazyLock<ByteLru<IndexValue>> =
        std::sync::LazyLock::new(|| ByteLru::new(INDEX_CACHE_BYTES));
    &CACHE
}

/// The process's chunk cache.
pub(crate) fn chunk_cache() -> &'static ByteLru<Bytes> {
    static CACHE: std::sync::LazyLock<ByteLru<Bytes>> =
        std::sync::LazyLock::new(|| ByteLru::new(CHUNK_CACHE_BYTES));
    &CACHE
}

/// Both caches' gauges (`GET /v1/debug/timings`).
pub(crate) fn json() -> serde_json::Value {
    serde_json::json!({
        "index_cache": index_cache().json(),
        "chunk_cache": chunk_cache().json(),
    })
}
