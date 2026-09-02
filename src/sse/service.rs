//! The live-feed service (WP-02 / PR 6-C): the instance's feed registry
//! and its memory budget, the per-feed ring allowance and the SSE
//! keep-alive cadence — extracted from `http::AppState`. Per runtime:
//! two rigs in one process never share a budget.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::feed::FeedMemoryBudget;
use super::registry::FeedRegistry;

#[derive(Clone)]
pub struct LiveFeedService {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Arc<FeedRegistry>,
    budget: Arc<FeedMemoryBudget>,
    ring_bytes: AtomicUsize,
    /// Operational cadence knob (fleet certification runs it at 500 ms
    /// to observe keep-alives inside short stall windows); the gated
    /// body clamps to its 50 ms floor.
    heartbeat_ms: AtomicU64,
}

/// What the debug surface shows about live feeds.
pub struct LiveFeedSnapshot {
    pub live_feeds: usize,
    pub reserved_bytes: u64,
    /// (project, reserved bytes, cap hits) — rows exist only while a
    /// project has live feeds.
    pub project_retention: Vec<(String, u64, u64)>,
}

impl LiveFeedService {
    pub fn from_config(cfg: &crate::config::SseConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                registry: Arc::new(FeedRegistry::new()),
                budget: Arc::new(FeedMemoryBudget::from_config(cfg)),
                ring_bytes: AtomicUsize::new(super::budget::feed_ring_bytes(cfg)),
                heartbeat_ms: AtomicU64::new(cfg.heartbeat_ms),
            }),
        }
    }

    pub fn registry(&self) -> &Arc<FeedRegistry> {
        &self.inner.registry
    }

    pub fn budget(&self) -> &Arc<FeedMemoryBudget> {
        &self.inner.budget
    }

    /// The ring allowance a feed reserves when it enters shared mode.
    pub fn ring_bytes(&self) -> usize {
        self.inner.ring_bytes.load(Ordering::Relaxed)
    }

    pub fn heartbeat_ms(&self) -> u64 {
        self.inner.heartbeat_ms.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn set_ring_bytes(&self, bytes: usize) {
        self.inner.ring_bytes.store(bytes, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn set_heartbeat_ms(&self, ms: u64) {
        self.inner.heartbeat_ms.store(ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> LiveFeedSnapshot {
        LiveFeedSnapshot {
            live_feeds: self.inner.registry.len(),
            reserved_bytes: self.inner.budget.reserved(),
            project_retention: self.inner.budget.project_rows(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two services from the same configuration are independent: a
    /// knob moved on one never reaches the other, and each starts with
    /// an empty registry and an untouched budget.
    #[test]
    fn services_are_per_runtime() {
        let cfg = crate::config::SseConfig::default();
        let a = LiveFeedService::from_config(&cfg);
        let b = LiveFeedService::from_config(&cfg);
        assert_eq!(a.heartbeat_ms(), cfg.heartbeat_ms);
        assert_eq!(a.ring_bytes(), super::super::budget::feed_ring_bytes(&cfg));
        a.set_heartbeat_ms(300);
        a.set_ring_bytes(1024);
        assert_eq!((a.heartbeat_ms(), a.ring_bytes()), (300, 1024));
        assert_eq!(b.heartbeat_ms(), cfg.heartbeat_ms, "b is untouched");
        let s = a.snapshot();
        assert_eq!((s.live_feeds, s.reserved_bytes), (0, 0));
        assert!(s.project_retention.is_empty());
        assert!(!Arc::ptr_eq(a.budget(), b.budget()));
    }
}
