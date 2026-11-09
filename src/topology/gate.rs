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
//! - **"Splitting"** ([`MaintenanceGate::splitting`]): set when a split's barrier holds the
//!   committer past its pause (`shard::transaction::barrier`); while it stands, every group
//!   the committer gathers is answered `503 shard_splitting`, never committed. Once the split
//!   published, the parent is **retired**: each append goes to the child engine this instance
//!   owns for its route ([`Reroute`]), and every other operation is answered as in the hold,
//!   which its handler turns into the new owner's `409` (`application::append::submit`).
//! - **The touched-queue mark** ([`MaintenanceGate::mark_touched`], S4-DESIGN §7.2): from the
//!   split's snapshot pass until the release, the committer records every queue whose state row
//!   a group writes, so the delta re-reads exactly those.
//! - **"Collection off"** ([`MaintenanceGate::collection_off`]): while a split prepares, the
//!   passes that delete or rewrite objects stand down (the orphan sweep, consolidation, the
//!   cleaner, purge reclaim and the run-index merge). Seals, folds, trims and `request_purge`
//!   continue: their rows are sentinel rows the split's delta covers, or objects its lists cover.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{RwLock, RwLockReadGuard};

use crate::shard::ShardEngine;

/// Where a retired parent's operations go (S4-DESIGN §6.2 step 4): for a route hash, the child
/// engine this instance owns, if any.
pub(crate) type Reroute = Arc<dyn Fn(&[u8; 16]) -> Option<Arc<ShardEngine>> + Send + Sync>;

#[derive(Default)]
pub(crate) struct MaintenanceGate {
    lock: RwLock<()>,
    collection_off: AtomicBool,
    splitting: AtomicBool,
    retired: Mutex<Option<Reroute>>,
    marking: AtomicBool,
    touched: Mutex<HashSet<[u8; 16]>>,
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

    /// Whether a split holds the engine past its pause (S4-DESIGN §8): the committer answers
    /// every group `Splitting`.
    pub(crate) fn splitting(&self) -> bool {
        self.splitting.load(Ordering::Acquire)
    }

    /// The committer's touched-queue mark (S4-DESIGN §7.2): while it is set, every queue whose
    /// state row a commit group writes. Bounded by the shard's distinct queues.
    pub(crate) fn mark_touched(&self, hash: &[u8; 16]) {
        if self.marking.load(Ordering::Acquire) {
            self.touched
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(*hash);
        }
    }

    /// Tests until the split's snapshot pass sets it (S4 step 11): the mark starts empty.
    #[cfg(test)]
    pub(crate) fn start_marking(&self) {
        self.touched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.marking.store(true, Ordering::Release);
    }

    /// Tests until the split's delta takes it (S4 step 10): the queues marked, and the mark's
    /// release.
    #[cfg(test)]
    pub(crate) fn take_touched(&self) -> HashSet<[u8; 16]> {
        self.marking.store(false, Ordering::Release);
        std::mem::take(&mut *self.touched.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The retired parent's reroute, once its split published (read only while `splitting`).
    pub(crate) fn reroute(&self) -> Option<Reroute> {
        self.retired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Tests until the split's barrier sets it (S4 step 11): the parent retired, its operations
    /// rerouted from now on.
    #[cfg(test)]
    pub(crate) fn set_retired(&self, reroute: Reroute) {
        *self.retired.lock().unwrap_or_else(PoisonError::into_inner) = Some(reroute);
        self.set_splitting(true);
    }

    /// Tests until the split's barrier sets it (S4 step 11): the hold passed its deadline, or
    /// the split released it.
    #[cfg(test)]
    pub(crate) fn set_splitting(&self, on: bool) {
        self.splitting.store(on, Ordering::Release);
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
