//! The maintenance gate (S4-DESIGN §5.1, §5.3): one async RwLock per engine that every shard-log
//! writer outside the committer takes shared, around exactly one batch, and that a split takes
//! exclusive to quiesce the parent.
//!
//! - **The writers:** fold, run-index merge, consolidation, the cleaner's copy, the collector's
//!   `'G'` deletes, purge reclaim and its row deletes, the orphan sweep's cursor, and
//!   `request_purge`. Each writes through `ShardEngine::write_index_batch`, which takes the gate
//!   shared for exactly that one batch. `request_purge` alone never waits for it: it tries the
//!   gate before `refs` and again for its batch, and answers `Busy` while a split holds it (its
//!   callers fall back to the tombstone walk's next pass, S6-DESIGN §7.1).
//! - **The lock order:** a writer that holds `refs` (`'R'`/`'H'`'s read-modify-write lock)
//!   takes the gate inside it, batch by batch; every other writer takes the gate alone. No
//!   writer takes it twice. A second shared acquire behind a queued exclusive one would never
//!   return. **The holder of the exclusive gate never takes `refs`:** the split reads `'R'` and
//!   `'H'` under the quiesce instead. So a writer that waits at the gate with `refs` held never
//!   blocks the split, and the order has no cycle. An object a writer PUT before its batch
//!   waited out a split is an orphan under the parent's home, for the sweep, like a barriered
//!   seal's.
//! - **"Collection off"** ([`MaintenanceGate::collection_off`]): while a split prepares, the
//!   passes that delete or rewrite objects stand down (the orphan sweep, consolidation, the
//!   cleaner, purge reclaim and the run-index merge). Seals, folds, trims and `request_purge`
//!   continue: their rows are sentinel rows the split's delta covers, or objects its lists cover.

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Default)]
pub(crate) struct MaintenanceGate {
    lock: RwLock<()>,
    collection_off: AtomicBool,
}

impl MaintenanceGate {
    /// One writer's hold: shared, around one batch.
    pub(crate) async fn shared(&self) -> RwLockReadGuard<'_, ()> {
        self.lock.read().await
    }

    /// The shared hold if it is free now: `None` while a split holds the gate or waits for it.
    pub(crate) fn try_shared(&self) -> Option<RwLockReadGuard<'_, ()>> {
        self.lock.try_read().ok()
    }

    /// Whether the collection passes stand down (a split prepares).
    pub(crate) fn collection_off(&self) -> bool {
        self.collection_off.load(Ordering::Acquire)
    }

    /// Tests until the split takes it (S4 step 11): the exclusive hold, within `deadline`.
    #[cfg(test)]
    pub(crate) async fn exclusive(
        &self,
        deadline: std::time::Duration,
    ) -> Option<tokio::sync::RwLockWriteGuard<'_, ()>> {
        tokio::time::timeout(deadline, self.lock.write()).await.ok()
    }

    /// Tests until the split sets it (S4 step 11): the collection passes stand down or resume.
    #[cfg(test)]
    pub(crate) fn set_collection_off(&self, off: bool) {
        self.collection_off.store(off, Ordering::Release);
    }
}
