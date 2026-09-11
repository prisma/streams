//! Shared store-I/O admission owned by one runtime, across all its stores.
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub(crate) struct StoreResources {
    concurrent: Option<tokio::sync::Semaphore>,
    bulk: Option<BulkGate>,
    nominal_get_bytes: u64,
}

impl StoreResources {
    pub(crate) fn new(config: &crate::config::StorageConfig) -> Self {
        Self {
            concurrent: (config.store_max_concurrent != 0)
                .then(|| tokio::sync::Semaphore::new(config.store_max_concurrent)),
            bulk: NonZeroU32::new(
                u32::try_from(config.bulk_inflight_max_bytes).unwrap_or(u32::MAX),
            )
            .map(BulkGate::new),
            nominal_get_bytes: config.bulk_nominal_get_bytes,
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "Runtime store admission owns a private semaphore that is never closed; a closed gate violates the owner invariant; silently bypassing it would remove the concurrency bound"
    )]
    pub(super) async fn permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        match &self.concurrent {
            Some(semaphore) => Some(
                semaphore
                    .acquire()
                    .await
                    .expect("runtime store gate stays open"),
            ),
            None => None,
        }
    }

    pub(super) async fn bulk_permit(&self, class: u8, bytes: u64) -> Option<BulkHold<'_>> {
        // WAL, manifest and fleet liveness never wait for bulk capacity.
        if class != 2 {
            return None;
        }
        Some(self.bulk.as_ref()?.acquire(bytes).await)
    }

    pub(super) fn nominal_get_bytes(&self) -> u64 {
        self.nominal_get_bytes
    }

    pub(crate) fn bulk_stats(&self) -> serde_json::Value {
        match &self.bulk {
            Some(gate) => gate.stats_json(),
            None => serde_json::json!({"cap_bytes": 0}),
        }
    }
}

/// R27-4: instance-wide byte bound on in-flight BULK store transfers.
///
/// SCOPE (R29 review): this is an SST LEAF-I/O OVERLAP LIMITER, not a
/// complete memory budget. It bounds bytes concurrently inside store
/// calls; it does NOT bound payloads already built and queued at the
/// gate, completed read-ahead buffers, compactor merge state, or
/// output builders — those are bounded by the compaction-worker
/// profile (COMPACT_* knobs) and task-count posture. The full 1 GiB
/// survival story is the COMBINATION, never this gate alone.
///
/// The SIN incompressible campaign OOM-killed (exit 137) with the
/// maintenance ledger healthy at 50-86 MB: RSS jumped ~250 MB in one
/// 5 s window exactly as concurrent store ops burst 14→22 (peak 53).
/// The latency-injected local repro shows the same wave. The driver is
/// SST-class transfers — flush + compaction across EVERY resident
/// SlateDB (4 shard DBs + history + telemetry + registry…) each buffer
/// MB-scale payloads, and per-DB compactor limits do not compose: at
/// WAN RTT every DB's compaction lives long enough to overlap all the
/// others', so the instance-wide buffered-byte peak scales with store
/// latency. This wrapper is the only point all DBs share, so the
/// shared runtime admission bound lives here.
///
/// Rules (deadlock-freedom): a permit is held ONLY across the leaf
/// await of the inner store call — never across stream consumption —
/// so every waiter is eventually satisfied by ops that complete on
/// pure network I/O. WAL/manifest/fleet classes NEVER wait (ack path
/// and cluster liveness); only sst-class ops are gated. An op larger
/// than the cap clamps to the whole cap (serializes, never starves).
#[derive(Debug)]
pub(super) struct BulkGate {
    sem: tokio::sync::Semaphore,
    cap: NonZeroU32,
    pub inflight_bytes: AtomicI64,
    /// High-water mark of concurrently held bytes (non-destructive).
    pub inflight_peak: AtomicI64,
    pub waits: std::sync::atomic::AtomicU64,
    pub wait_ms: std::sync::atomic::AtomicU64,
    /// Ops whose weight clamped to the whole cap (larger than the gate).
    pub oversized: std::sync::atomic::AtomicU64,
}

impl BulkGate {
    pub(super) fn new(cap_bytes: NonZeroU32) -> Self {
        BulkGate {
            sem: tokio::sync::Semaphore::new(cap_bytes.get() as usize),
            cap: cap_bytes,
            inflight_bytes: AtomicI64::new(0),
            inflight_peak: AtomicI64::new(0),
            waits: std::sync::atomic::AtomicU64::new(0),
            wait_ms: std::sync::atomic::AtomicU64::new(0),
            oversized: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Acquire `bytes` worth of the gate (clamped to the cap). Counts a
    /// wait only when the fast path fails, so steady-state overhead is
    /// one try_acquire. The returned hold decrements the inflight gauge
    /// and returns capacity on drop.
    #[expect(
        clippy::expect_used,
        reason = "Bulk transfer admission owns a private semaphore; it is never closed; returning an uncharged hold after closure would falsify the byte bound"
    )]
    pub(super) async fn acquire(&self, bytes: u64) -> BulkHold<'_> {
        if bytes > u64::from(self.cap.get()) {
            self.oversized.fetch_add(1, Ordering::Relaxed);
        }
        let w = u32::try_from(bytes)
            .unwrap_or(u32::MAX)
            .min(self.cap.get())
            .max(1);
        let p = match self.sem.try_acquire_many(w) {
            Ok(p) => p,
            Err(_) => {
                self.waits.fetch_add(1, Ordering::Relaxed);
                let t0 = Instant::now();
                // only errs on close; we never close it
                let p = self.sem.acquire_many(w).await.expect("gate never closed");
                self.wait_ms.fetch_add(
                    u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                p
            }
        };
        let now = self
            .inflight_bytes
            .fetch_add(i64::from(w), Ordering::Relaxed)
            + i64::from(w);
        self.inflight_peak.fetch_max(now, Ordering::Relaxed);
        BulkHold {
            _p: p,
            gate: self,
            w: i64::from(w),
        }
    }

    pub(super) fn stats_json(&self) -> serde_json::Value {
        serde_json::json!({
            "cap_bytes": self.cap.get(),
            "inflight_bytes": self.inflight_bytes.load(Ordering::Relaxed),
            "inflight_peak_bytes": self.inflight_peak.load(Ordering::Relaxed),
            "waits_total": self.waits.load(Ordering::Relaxed),
            "wait_ms_total": self.wait_ms.load(Ordering::Relaxed),
            "oversized_total": self.oversized.load(Ordering::Relaxed),
            // Gated class set is fixed by design: sst only (WAL,
            // manifest and fleet are ack/liveness paths and never
            // queue behind compaction).
            "classes": "sst",
        })
    }
}

/// RAII hold on gate capacity: semaphore permits + inflight gauge,
/// both returned on drop.
pub(super) struct BulkHold<'a> {
    _p: tokio::sync::SemaphorePermit<'a>,
    gate: &'a BulkGate,
    w: i64,
}

impl Drop for BulkHold<'_> {
    fn drop(&mut self) {
        self.gate
            .inflight_bytes
            .fetch_sub(self.w, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_timing::TimingStore;
    use object_store::{ObjectStore, PutOptions, memory::InMemory, path::Path};
    use std::future::Future;
    use std::sync::Arc;
    use std::task::Poll;

    #[tokio::test]
    async fn runtime_store_concurrency_is_shared_locally_and_independent_of_first_access() {
        let early = StoreResources::new(&crate::config::StorageConfig::default());
        assert!(early.permit().await.is_none());
        let mut config = crate::config::ServerConfig::load(
            crate::config::CliArgs::deterministic(),
            &crate::config::MapEnvironment::empty(),
        );
        config.storage.store_max_concurrent = 1;
        let a = crate::runtime::RuntimeCaps::production("store-a").with_config(&config);
        config.storage.store_max_concurrent = 2;
        let b = crate::runtime::RuntimeCaps::production("store-b").with_config(&config);
        let weak = Arc::downgrade(&a.store_io);
        let first = TimingStore::new(InMemory::new(), a.store_io.clone());
        let second = TimingStore::new(InMemory::new(), a.store_io.clone());
        let other = TimingStore::new(InMemory::new(), b.store_io.clone());
        assert!(Arc::ptr_eq(&first.resources, &second.resources));
        let held = first.resources.permit().await.unwrap();
        let held_other = b.store_io.permit().await.unwrap();
        let path = Path::from("manifest/test");
        let mut pending = Box::pin(second.put_opts(&path, vec![1u8].into(), PutOptions::default()));
        std::future::poll_fn(|cx| {
            assert!(
                pending.as_mut().poll(cx).is_pending(),
                "same-runtime store must share the full concurrency gate"
            );
            Poll::Ready(())
        })
        .await;
        other
            .put_opts(&path, vec![2u8].into(), PutOptions::default())
            .await
            .unwrap();
        drop(held_other);
        drop(held);
        pending.await.unwrap();
        assert_eq!(
            a.store_io.concurrent.as_ref().unwrap().available_permits(),
            1
        );
        assert_eq!(
            b.store_io.concurrent.as_ref().unwrap().available_permits(),
            2
        );
        drop((first, second, other, a, b));
        assert!(
            weak.upgrade().is_none(),
            "store admission state must retire with its owners"
        );
    }

    #[tokio::test]
    async fn runtime_bulk_capacity_cancellation_and_exempt_paths_keep_their_own_owner() {
        let a = Arc::new(StoreResources::new(&crate::config::StorageConfig {
            bulk_inflight_max_bytes: 4,
            bulk_nominal_get_bytes: 3,
            ..Default::default()
        }));
        let b = Arc::new(StoreResources::new(&crate::config::StorageConfig {
            bulk_inflight_max_bytes: 8,
            bulk_nominal_get_bytes: 6,
            ..Default::default()
        }));
        assert_eq!(a.nominal_get_bytes(), 3);
        assert_eq!(b.nominal_get_bytes(), 6);
        let store_a = TimingStore::new(InMemory::new(), a.clone());
        let store_b = TimingStore::new(InMemory::new(), b.clone());
        let held = a.bulk_permit(2, 4).await.unwrap();
        let path = Path::from("compacted/data.sst");
        let mut pending =
            Box::pin(store_a.put_opts(&path, vec![1u8; 4].into(), PutOptions::default()));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(pending); // Cancelling a waiter must not reserve bytes later.
        store_b
            .put_opts(&path, vec![2u8; 8].into(), PutOptions::default())
            .await
            .unwrap();
        store_a
            .put_opts(
                &Path::from("wal/data.sst"),
                vec![3u8; 8].into(),
                PutOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(a.bulk_stats()["inflight_bytes"], 4);
        assert_eq!(b.bulk_stats()["inflight_bytes"], 0);
        drop(held);
        assert_eq!(a.bulk_stats()["inflight_bytes"], 0);
        store_a
            .put_opts(&path, vec![4u8; 8].into(), PutOptions::default())
            .await
            .unwrap();
        assert_eq!(a.bulk_stats()["cap_bytes"], 4);
        assert_eq!(b.bulk_stats()["cap_bytes"], 8);
        assert_eq!(a.bulk_stats()["inflight_bytes"], 0);
    }

    #[tokio::test]
    async fn configured_bulk_bounds_and_zero_length_io_keep_exact_capacity() {
        let disabled = StoreResources::new(&crate::config::StorageConfig {
            bulk_inflight_max_bytes: 0,
            ..Default::default()
        });
        assert_eq!(disabled.bulk_stats()["cap_bytes"], 0);
        assert!(disabled.bulk_permit(2, u64::MAX).await.is_none());
        for (configured, expected) in [
            (1, 1),
            (u64::from(u32::MAX), u64::from(u32::MAX)),
            (u64::MAX, u64::from(u32::MAX)),
        ] {
            let resources = StoreResources::new(&crate::config::StorageConfig {
                bulk_inflight_max_bytes: configured,
                ..Default::default()
            });
            assert_eq!(resources.bulk_stats()["cap_bytes"], expected);
            let gate = resources.bulk.as_ref().unwrap();
            let full = gate.acquire(u64::MAX).await;
            assert_eq!(
                gate.inflight_bytes.load(Ordering::Relaxed),
                i64::try_from(expected).unwrap()
            );
            let mut zero = Box::pin(gate.acquire(0));
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(
                zero.as_mut().poll(&mut context).is_pending(),
                "zero-byte IO still reserves one slot"
            );
            drop(full);
            let held = zero.await;
            assert_eq!(gate.inflight_bytes.load(Ordering::Relaxed), 1);
            drop(held);
            assert_eq!(gate.inflight_bytes.load(Ordering::Relaxed), 0);
            assert_eq!(
                gate.sem.available_permits(),
                usize::try_from(expected).unwrap()
            );
        }
    }
}
