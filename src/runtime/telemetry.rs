//! One runtime's shared telemetry cache and billing progress signals.

use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
};

pub(crate) struct TelemetryResources {
    /// The spool and rollup both pass this same handle to SlateDB.
    pub cache: Arc<FoyerCache>,
    capacity_bytes: u64,
    last_drain_ok_ms: AtomicI64,
    last_rollup_apply_ms: AtomicI64,
    rollup_apply_duration_ms: AtomicU64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TelemetryProgress {
    pub last_drain_ok_ms: i64,
    pub last_rollup_apply_ms: i64,
    pub rollup_apply_duration_ms: u64,
}

impl TelemetryResources {
    pub(crate) fn new(capacity_bytes: usize) -> Self {
        Self {
            cache: Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: capacity_bytes as u64,
                ..Default::default()
            })),
            capacity_bytes: capacity_bytes as u64,
            last_drain_ok_ms: AtomicI64::new(0),
            last_rollup_apply_ms: AtomicI64::new(0),
            rollup_apply_duration_ms: AtomicU64::new(0),
        }
    }

    /// Configured bound, observable before either telemetry DB opens.
    pub(crate) fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub(crate) fn drain_succeeded(&self, at: super::TrustedNow) {
        self.last_drain_ok_ms.store(at.ms(), Ordering::Relaxed);
    }

    pub(crate) fn rollup_applied(&self, at: super::TrustedNow, elapsed_ms: u64) {
        self.last_rollup_apply_ms.store(at.ms(), Ordering::Relaxed);
        self.rollup_apply_duration_ms
            .store(elapsed_ms, Ordering::Relaxed);
    }

    pub(crate) fn progress(&self) -> TelemetryProgress {
        TelemetryProgress {
            last_drain_ok_ms: self.last_drain_ok_ms.load(Ordering::Relaxed),
            last_rollup_apply_ms: self.last_rollup_apply_ms.load(Ordering::Relaxed),
            rollup_apply_duration_ms: self.rollup_apply_duration_ms.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{ManualClock, RuntimeCaps, SeededEntropy};

    fn config(bytes: usize) -> crate::config::ServerConfig {
        let mut config = crate::config::ServerConfig::load(
            crate::config::CliArgs::deterministic(),
            &crate::config::MapEnvironment::empty(),
        );
        config.billing.telemetry_cache_bytes = bytes;
        config
    }

    #[expect(
        clippy::excessive_nesting,
        reason = "r10_telemetry_cache_progress_and_database_lifetimes_are_runtime_owned; the fixture nests the release wait inside the timeout that bounds it inside the test; flattening it would separate the wait from the bound it must respect"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r10_telemetry_cache_progress_and_database_lifetimes_are_runtime_owned() {
        let clock = Arc::new(ManualClock::at(1000));
        let before = RuntimeCaps::with(clock.clone(), Arc::new(SeededEntropy::seeded(11)), "a");
        let accessed_early = before.telemetry.cache.clone();
        let old = Arc::downgrade(&accessed_early);
        before.telemetry.drain_succeeded(before.clock.now());
        let config_a = config(1024 * 1024);
        let config_b = config(3 * 1024 * 1024);
        let a = before.with_config(&config_a);
        let b = RuntimeCaps::with(clock, Arc::new(SeededEntropy::seeded(12)), "b")
            .with_config(&config_b);
        assert_eq!(a.telemetry.capacity_bytes(), 1024 * 1024);
        assert_eq!(b.telemetry.capacity_bytes(), 3 * 1024 * 1024);
        assert!(!Arc::ptr_eq(&a.telemetry.cache, &accessed_early));
        assert!(!Arc::ptr_eq(&a.telemetry.cache, &b.telemetry.cache));
        drop(accessed_early);
        assert!(
            old.upgrade().is_none(),
            "pre-configuration access leaves no global holder"
        );
        assert_eq!(a.telemetry.progress(), TelemetryProgress::default());
        a.telemetry.drain_succeeded(a.clock.now());
        a.telemetry.rollup_applied(a.clock.now(), 7);
        assert_eq!(a.telemetry.progress().last_drain_ok_ms, 1000);
        assert_eq!(a.telemetry.progress().last_rollup_apply_ms, 1000);
        assert_eq!(a.telemetry.progress().rollup_apply_duration_ms, 7);
        assert_eq!(
            b.telemetry.progress(),
            TelemetryProgress::default(),
            "a cannot heal b"
        );

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let weak_a = Arc::downgrade(&a.telemetry.cache);
        let weak_b = Arc::downgrade(&b.telemetry.cache);
        let spool_a = crate::billing::ReadSpool::open_with_cache(
            store.clone(),
            "a",
            "a",
            &config_a,
            a.telemetry.cache.clone(),
        )
        .await
        .unwrap();
        assert!(
            Arc::strong_count(&a.telemetry.cache) > 1,
            "spool uses a's cache"
        );
        let rollup_a = crate::rollup::UsageRollup::open_with_cache(
            store.clone(),
            "a",
            &config_a,
            a.telemetry.cache.clone(),
        )
        .await
        .unwrap();
        let spool_b = crate::billing::ReadSpool::open_with_cache(
            store.clone(),
            "b",
            "b",
            &config_b,
            b.telemetry.cache.clone(),
        )
        .await
        .unwrap();
        let rollup_b = crate::rollup::UsageRollup::open_with_cache(
            store,
            "b",
            &config_b,
            b.telemetry.cache.clone(),
        )
        .await
        .unwrap();
        drop((a, b));
        spool_a.close_for_tests().await;
        drop(spool_a);
        rollup_b.db.close().await.unwrap();
        drop(rollup_b);
        assert!(
            weak_a.upgrade().is_some(),
            "rollup retains a's shared cache"
        );
        assert!(weak_b.upgrade().is_some(), "spool retains b's shared cache");
        rollup_a.db.close().await.unwrap();
        drop(rollup_a);
        spool_b.close_for_tests().await;
        drop(spool_b);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while weak_a.upgrade().is_some() || weak_b.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed telemetry components release both caches");
    }
}
