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
//! - **"Final"** ([`MaintenanceGate::is_final`]): from the `'T'` a split or merge wrote, the
//!   source's log is final. The operation sets it while it holds the gate exclusive, at the point
//!   from which it only rolls forward, and it is never cleared: a writer queued on the gate that
//!   gets it once the operation drops its hold writes no batch (`write_index_batch_held` refuses,
//!   `request_purge` answers `Busy`), so the source's rows stay exactly what its successors copied.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{RwLock, RwLockReadGuard};

use crate::shard::ShardEngine;

/// Where a retired parent's operations go (S4-DESIGN §6.2 step 4): for a route hash, the child
/// engine this instance owns, once open, if any. Asynchronous, so a split releases its barrier as
/// soon as its topology is installed and a held operation waits for its child's open instead.
pub(crate) type Reroute = Arc<
    dyn Fn([u8; 16]) -> futures_util::future::BoxFuture<'static, Option<Arc<ShardEngine>>>
        + Send
        + Sync,
>;

#[derive(Default)]
pub(crate) struct MaintenanceGate {
    lock: RwLock<()>,
    collection_off: AtomicBool,
    splitting: AtomicBool,
    parked: AtomicBool,
    retired: Mutex<Option<Reroute>>,
    /// The source wrote `'T'`: set once, never cleared.
    final_log: AtomicBool,
    marking: AtomicBool,
    touched: Mutex<HashSet<[u8; 16]>>,
    /// Tests: where a split pauses once its barrier parks (`pause_at_park`).
    #[cfg(test)]
    pause: Mutex<Option<PausePoint>>,
}

/// Tests: a split that reaches its pause answers `reached` and waits for `go`.
#[cfg(test)]
pub(crate) struct PausePoint {
    pub(crate) reached: tokio::sync::oneshot::Sender<()>,
    pub(crate) go: tokio::sync::oneshot::Receiver<()>,
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

    /// Tests: the next split of this engine stops once its barrier parked, until the test says go.
    #[cfg(test)]
    pub(crate) fn pause_at_park(&self, point: PausePoint) {
        *self.pause.lock().unwrap_or_else(PoisonError::into_inner) = Some(point);
    }

    /// Tests: the pause point a split takes, if a test set one.
    #[cfg(test)]
    pub(crate) fn take_pause(&self) -> Option<PausePoint> {
        self.pause
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
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

    /// The split's Preparing sets it: the mark starts empty.
    pub(crate) fn start_marking(&self) {
        self.touched
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.marking.store(true, Ordering::Release);
    }

    /// The split's delta takes it: the queues marked, and the mark's
    /// release.
    pub(crate) fn take_touched(&self) -> HashSet<[u8; 16]> {
        self.marking.store(false, Ordering::Release);
        std::mem::take(&mut *self.touched.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether a split's barrier holds the committer (from the park until a resume): such an
    /// engine is retired by its split after the release, never by an install of the topology that
    /// retires its prefix (`ShardDirectory::install`).
    pub(crate) fn parked(&self) -> bool {
        self.parked.load(Ordering::Acquire)
    }

    /// The split's barrier sets it.
    pub(crate) fn set_parked(&self, on: bool) {
        self.parked.store(on, Ordering::Release);
    }

    /// The retired parent's reroute, once its split published (read only while `splitting`).
    pub(crate) fn reroute(&self) -> Option<Reroute> {
        self.retired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The split's barrier sets it: the parent retired, its operations rerouted from now on.
    pub(crate) fn set_retired(&self, reroute: Reroute) {
        *self.retired.lock().unwrap_or_else(PoisonError::into_inner) = Some(reroute);
        self.set_splitting(true);
    }

    /// The hold passed its deadline, or the split released it.
    pub(crate) fn set_splitting(&self, on: bool) {
        self.splitting.store(on, Ordering::Release);
    }

    /// The split's exclusive hold, within `deadline`.
    pub(crate) async fn exclusive(
        &self,
        deadline: std::time::Duration,
    ) -> Option<tokio::sync::RwLockWriteGuard<'_, ()>> {
        tokio::time::timeout(deadline, self.lock.write()).await.ok()
    }

    /// The collection passes stand down or resume: while a split prepares (S4 step 11), and while
    /// an open is held by the fence (`topology::fence`).
    pub(crate) fn set_collection_off(&self, off: bool) {
        self.collection_off.store(off, Ordering::Release);
    }

    /// Whether the source's log is final: a split or merge wrote its `'T'` (see the module).
    pub(crate) fn is_final(&self) -> bool {
        self.final_log.load(Ordering::Acquire)
    }

    /// The operation that wrote `'T'` makes the log final, under its exclusive hold.
    pub(crate) fn set_final(&self) {
        self.final_log.store(true, Ordering::Release);
    }
}
