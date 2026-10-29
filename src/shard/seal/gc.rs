//! Physical reclamation (layout 6, DESIGN-6A §2.5, package 6): every stored
//! object is deleted once nothing references it, a dead stream's index and
//! rows are reclaimed, and objects a fenced or failed writer left behind are
//! swept. The rows and the liveness rule are `spill::gc`.
//!
//! - **Refcounts.** A segment object's `'R'` row counts its unreclaimed
//!   chunks. A seal's commit writes it; a consolidation decrements the
//!   chunks it supersedes; a purge's reclaim decrements a dead stream's
//!   chunks. Every read-modify-write of `'R'` rows runs under the engine's
//!   `refs` lock, from its reads until its batch is durable, so no two
//!   decrements interleave. A row reaching zero is deleted in the same batch
//!   that writes the object's `'G'` row. A merge writes a `'G'` row for each
//!   run-index object it merged away, in the batch that deletes its `'X'`.
//! - **Deletes** (`collect`). An object named by a `'G'` row is DELETEd once
//!   its grace passed (a read on another process, or a fenced owner's, is
//!   over by then) and no sealed read of this process that started before
//!   the object's zeroing is still running (`ReadPin`), then its `'G'` row
//!   goes. A read that meets a deleted object answers a retryable error,
//!   never a short success.
//! - **Purges** (`purge`). A dead incarnation's purge is asked for by the
//!   tombstone walk or the replaced-debt pass (`request_purge`); once its
//!   time came, the committer deletes the stream's tail and pending rows and
//!   marks it purged (a later load of the stream fails), then its chunks are
//!   decremented.
//! - **Orphans** (`sweep`). Objects with no row naming them, from a fenced
//!   opening, or from this one and no longer in flight, are deleted.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use slatedb::WriteBatch;

use crate::shard::ShardEngine;
use crate::spill::gc::{self, Decrements, Garbage, ObjectKind, Refcount};
use crate::spill::object::ObjectId;

pub(crate) mod purge;
mod sweep;

/// When the garbage collector runs and how long it waits.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GcSettings {
    /// An object whose last reference went is deleted this long after.
    pub(crate) grace: Duration,
    /// A dead incarnation's purge waits this long after its close: longer
    /// than any request admitted before the close may still run.
    pub(crate) purge_delay: Duration,
    /// How often the loop purges, reclaims and deletes.
    pub(crate) tick: Duration,
    /// How often the orphan sweep runs.
    pub(crate) sweep_every: Duration,
    /// How often the live and garbage totals are recounted (a scan of the
    /// `'R'` and `'G'` rows).
    pub(crate) totals_every: Duration,
    /// An object younger than this is never an orphan.
    pub(crate) orphan_age: Duration,
    /// LIST pages one sweep reads per prefix.
    pub(crate) sweep_pages: usize,
    /// Chunks one reclaim batch decrements, rows one purge batch deletes,
    /// and `'G'` rows one collect pass deletes (bounded batches, H5).
    pub(crate) reclaim_batch: usize,
    /// How often the sweep's cursor starts over.
    pub(crate) wrap_every: Duration,
    /// Whether the loop runs by itself.
    pub(crate) auto: bool,
}

impl Default for GcSettings {
    fn default() -> Self {
        Self {
            grace: Duration::from_secs(600),
            purge_delay: Duration::from_secs(600),
            tick: Duration::from_secs(30),
            sweep_every: Duration::from_secs(3600),
            totals_every: Duration::from_secs(3600),
            orphan_age: Duration::from_secs(3600),
            sweep_pages: 4,
            reclaim_batch: 2048,
            wrap_every: Duration::from_secs(24 * 3600),
            auto: true,
        }
    }
}

/// The collector's counters (`seal.gc` in the shard debug JSON).
#[derive(Default)]
pub(crate) struct GcCounters {
    pub(crate) purge_requests: AtomicU64,
    pub(crate) purges: AtomicU64,
    pub(crate) purges_refused: AtomicU64,
    pub(crate) reclaims: AtomicU64,
    pub(crate) reclaimed_chunks: AtomicU64,
    pub(crate) reclaimed_bytes: AtomicU64,
    pub(crate) superseded_chunks: AtomicU64,
    pub(crate) zeroed: AtomicU64,
    pub(crate) deleted: AtomicU64,
    pub(crate) deleted_bytes: AtomicU64,
    pub(crate) delete_errors: AtomicU64,
    pub(crate) pinned: AtomicU64,
    pub(crate) sweeps: AtomicU64,
    pub(crate) listed: AtomicU64,
    pub(crate) orphans: AtomicU64,
    pub(crate) orphan_bytes: AtomicU64,
    /// Decrements refused because an object's row is missing or would go
    /// below zero (an index entry no seal counted).
    pub(crate) inexact: AtomicU64,
    /// `'G'`, `'H'` or `'R'` rows that did not decode (left in place).
    pub(crate) corrupt_rows: AtomicU64,
}

/// The collector's state on one engine (part of `SealState`).
#[derive(Default)]
pub(crate) struct GcState {
    settings: Mutex<Option<GcSettings>>,
    /// Every read-modify-write of `'R'` and `'H'` rows, from its reads until
    /// its batch is durable.
    pub(crate) refs: tokio::sync::Mutex<()>,
    /// The pin clock: a read takes it as its start, a zeroing advances it.
    clock: AtomicU64,
    /// Running sealed reads by start, with how many started then.
    readers: Mutex<BTreeMap<u64, usize>>,
    /// Objects this opening zeroed, by the clock value at their zeroing: a
    /// read that started at or before it may hold them.
    zeroed: Mutex<BTreeMap<ObjectId, u64>>,
    /// Object sequences of this opening drawn by a seal, fold, merge or
    /// consolidation whose batch's outcome is not known yet.
    in_flight: Mutex<BTreeSet<u64>>,
    last_sweep: Mutex<Option<Instant>>,
    /// When the sweep's cursor last wrapped to the start (a fenced
    /// writer's late PUT may land below it).
    last_wrap: Mutex<Option<Instant>>,
    /// Streams this opening purged: a later op on one fails, even within
    /// the purge's own group (its rows are deleted only when the group
    /// writes). After a restart the durable `'H'` row says the same.
    purged: Mutex<BTreeSet<[u8; 16]>>,
    /// Dead incarnations whose purge or reclaim is not done, and the
    /// earliest instant one of them was due, at the last recount (0: none).
    unreclaimed: AtomicU64,
    oldest_due_ms: AtomicU64,
    /// The live and garbage totals at the last refresh (`gc_totals`).
    totals: Mutex<Option<serde_json::Value>>,
    last_totals: Mutex<Option<Instant>>,
    /// Signals that purge or garbage work may exist: a pass scans the
    /// `'H'` or `'G'` range only while its signal is up (both up at open),
    /// so an idle shard reads nothing. A request or a zeroing raises one; a
    /// pass that finds nothing left lowers it unless raised meanwhile.
    heads_signal: AtomicU64,
    garbage_signal: AtomicU64,
    /// Whether the shard log holds any `'H'` row (set at open, and by a
    /// purge request): until it does, folds and merges read none.
    heads_exist: std::sync::atomic::AtomicBool,
    /// When this opening's collector started: a `'G'` row found then is
    /// due no sooner than one grace later (a fenced owner's reads).
    opened_ms: AtomicU64,
    pub(crate) counters: GcCounters,
}

// The collector's locks guard plain maps whose every update is one insert
// or remove, so a poisoned lock's value is consistent as it stands.
pub(super) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl GcState {
    pub(crate) fn settings(&self) -> GcSettings {
        lock(&self.settings).unwrap_or_default()
    }

    /// Change the settings (tests).
    #[cfg(test)]
    pub(crate) fn configure(&self, change: impl FnOnce(&mut GcSettings)) {
        let mut s = lock(&self.settings);
        let mut now = s.unwrap_or_default();
        change(&mut now);
        *s = Some(now);
    }

    /// Pin every object zeroed from now on until the returned guard drops:
    /// a sealed read takes it before it looks at the directory or the
    /// run-index set.
    pub(crate) fn pin(&self) -> ReadPin<'_> {
        let mut readers = lock(&self.readers);
        let start = self.clock.load(Ordering::SeqCst);
        *readers.entry(start).or_default() += 1;
        ReadPin { gc: self, start }
    }

    /// The objects of `ids` are about to lose their last reference: until
    /// `note_zeroed`, any running read pins them.
    fn prepare_zeroed(&self, ids: &[ObjectId]) {
        self.garbage_signal.fetch_add(1, Ordering::SeqCst);
        let mut zeroed = lock(&self.zeroed);
        for id in ids {
            zeroed.insert(*id, u64::MAX);
        }
    }

    /// The objects of `ids`, just zeroed by a batch that is applied: a read
    /// that started before now may hold them.
    fn note_zeroed(&self, ids: &[ObjectId]) {
        if ids.is_empty() {
            return;
        }
        let at = self.clock.fetch_add(1, Ordering::SeqCst);
        let mut zeroed = lock(&self.zeroed);
        for id in ids {
            zeroed.insert(*id, at);
        }
        self.counters
            .zeroed
            .fetch_add(ids.len() as u64, Ordering::Relaxed);
    }

    /// Whether a running read may hold `id` (it started at or before the
    /// object's zeroing).
    fn pinned(&self, id: ObjectId) -> bool {
        let Some(at) = lock(&self.zeroed).get(&id).copied() else {
            return false;
        };
        lock(&self.readers)
            .first_key_value()
            .is_some_and(|(start, _)| *start <= at)
    }

    fn forget_zeroed(&self, id: ObjectId) {
        lock(&self.zeroed).remove(&id);
    }

    /// Whether any `'H'` row may exist.
    pub(crate) fn heads_exist(&self) -> bool {
        self.heads_exist.load(Ordering::SeqCst)
    }

    /// A purge request was written: the next pass scans the `'H'` range.
    pub(crate) fn signal_heads(&self) {
        self.heads_exist.store(true, Ordering::SeqCst);
        self.heads_signal.fetch_add(1, Ordering::SeqCst);
    }

    /// This opening's collector starts now (the sealer's open): both
    /// ranges are scanned at the first pass.
    pub(crate) fn note_opened(&self) {
        self.heads_signal.fetch_add(1, Ordering::SeqCst);
        self.garbage_signal.fetch_add(1, Ordering::SeqCst);
        self.opened_ms
            .store(crate::shard::now_ms().unsigned_abs(), Ordering::Relaxed);
    }

    /// Mark `hash` purged by this opening.
    pub(crate) fn note_purged(&self, hash: [u8; 16]) {
        lock(&self.purged).insert(hash);
    }

    /// Whether this opening purged `hash`.
    pub(crate) fn purged(&self, hash: &[u8; 16]) -> bool {
        lock(&self.purged).contains(hash)
    }

    /// Whether `seq` of this opening is drawn by an operation still running.
    pub(crate) fn in_flight(&self) -> BTreeSet<u64> {
        lock(&self.in_flight).clone()
    }

    /// Running reads (`seal.gc.readers`).
    pub(crate) fn readers(&self) -> usize {
        lock(&self.readers).values().sum()
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let c = &self.counters;
        let n = |a: &AtomicU64| a.load(Ordering::Relaxed);
        serde_json::json!({
            "purge_requests": n(&c.purge_requests),
            "purges": n(&c.purges),
            "purges_refused": n(&c.purges_refused),
            "reclaims": n(&c.reclaims),
            "reclaimed_chunks": n(&c.reclaimed_chunks),
            "reclaimed_bytes": n(&c.reclaimed_bytes),
            "superseded_chunks": n(&c.superseded_chunks),
            "zeroed": n(&c.zeroed),
            "deleted": n(&c.deleted),
            "deleted_bytes": n(&c.deleted_bytes),
            "delete_errors": n(&c.delete_errors),
            "pinned": n(&c.pinned),
            "sweeps": n(&c.sweeps),
            "listed": n(&c.listed),
            "orphans": n(&c.orphans),
            "orphan_bytes": n(&c.orphan_bytes),
            "inexact": n(&c.inexact),
            "corrupt_rows": n(&c.corrupt_rows),
            "readers": self.readers(),
            "unreclaimed_incarnations": self.unreclaimed.load(Ordering::Relaxed),
            "oldest_unreclaimed_due_ms": self.oldest_due_ms.load(Ordering::Relaxed),
            "in_flight": lock(&self.in_flight).len(),
            "stored": lock(&self.totals).clone(),
        })
    }
}

/// A running sealed read: objects zeroed after it started stay stored
/// until it ends.
pub(crate) struct ReadPin<'a> {
    gc: &'a GcState,
    start: u64,
}

impl Drop for ReadPin<'_> {
    fn drop(&mut self) {
        let mut readers = lock(&self.gc.readers);
        if let Some(n) = readers.get_mut(&self.start) {
            *n -= 1;
            if *n == 0 {
                readers.remove(&self.start);
            }
        }
    }
}

/// An object id drawn for a seal, fold, merge or consolidation, in flight
/// until dropped: the orphan sweep leaves its object alone meanwhile.
pub(crate) struct ObjectClaim<'a> {
    gc: &'a GcState,
    pub(crate) id: ObjectId,
}

impl Drop for ObjectClaim<'_> {
    fn drop(&mut self) {
        lock(&self.gc.in_flight).remove(&self.id.seq);
    }
}

/// Why a decrement batch was refused before it was written.
#[derive(Debug)]
pub(crate) enum GcError {
    Store(String),
    /// A decrement no `'R'` row can take: the batch is not written.
    Inexact(String),
    Index(String),
}

impl std::fmt::Display for GcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(e) => write!(f, "store: {e}"),
            Self::Inexact(e) => write!(f, "inexact refcount: {e}"),
            Self::Index(e) => write!(f, "index: {e}"),
        }
    }
}

impl ShardEngine {
    pub(crate) fn gc_state(&self) -> &GcState {
        &self.layout.seal.gc
    }

    /// The next object id of this opening (seals, folds, merges and
    /// consolidations share the sequence, so no two of them name the same
    /// key or path), in flight until the claim drops.
    pub(crate) fn claim_object(&self) -> ObjectClaim<'_> {
        let gc = self.gc_state();
        let mut in_flight = lock(&gc.in_flight);
        let id = ObjectId {
            epoch: self.writer_epoch,
            seq: self.layout.seal.next_object_seq(),
        };
        in_flight.insert(id.seq);
        ObjectClaim { gc, id }
    }

    /// A purged stream leaves the engine: its handle and its hold.
    pub(crate) fn forget_stream(&self, hash: &[u8; 16]) {
        if let Ok(mut map) = self.streams.lock() {
            map.remove(hash);
        }
        self.layout.forget_unsealed(hash);
    }

    /// Stage `decrements` of `'R'` rows into `batch` (the caller holds
    /// `refs`): each row less its decrement, or deleted with a `'G'` row
    /// when it reaches zero. Returns the objects zeroed. A row that is
    /// missing or would go below zero refuses the whole batch.
    pub(crate) async fn stage_decrements(
        &self,
        batch: &mut WriteBatch,
        decrements: &Decrements,
    ) -> Result<Vec<ObjectId>, GcError> {
        let due = crate::shard::now_ms()
            .unsigned_abs()
            .saturating_add(duration_ms(self.gc_state().settings().grace));
        let mut zeroed = Vec::new();
        for (id, by) in decrements {
            let key = gc::refcount_key(*id);
            let raw = self
                .db
                .get(&key)
                .await
                .map_err(|e| GcError::Store(e.to_string()))?;
            let current = raw.as_deref().and_then(gc::decode_refcount);
            let next = current.and_then(|r| gc::less(r, *by));
            let Some(next) = next.filter(|n| n.live_chunks > 0 || n.live_bytes == 0) else {
                self.gc_state()
                    .counters
                    .inexact
                    .fetch_add(1, Ordering::Relaxed);
                return Err(GcError::Inexact(format!(
                    "object {id:?}: row {current:?} cannot take {by:?}"
                )));
            };
            if next.live_chunks == 0 {
                batch.delete(&key);
                let g = Garbage {
                    kind: ObjectKind::Segment,
                    object_len: next.object_len,
                    due_ms: due,
                };
                batch.put(gc::garbage_key(*id), gc::encode_garbage(&g));
                zeroed.push(*id);
            } else {
                batch.put(&key, gc::encode_refcount(&next));
            }
        }
        Ok(zeroed)
    }

    /// Write a decrement batch (`write_index_batch`: durable, or the engine
    /// closes), then note its zeroed objects for the pins.
    pub(crate) async fn write_decrements(
        &self,
        batch: WriteBatch,
        zeroed: &[ObjectId],
    ) -> Result<(), GcError> {
        self.gc_state().prepare_zeroed(zeroed);
        self.write_index_batch(batch)
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        self.gc_state().note_zeroed(zeroed);
        Ok(())
    }

    /// Dead incarnations not yet purged and reclaimed at the last recount,
    /// and how long the oldest due one has waited past its due instant
    /// (`/operator/data.json`, cell-watch row 32).
    pub(crate) fn gc_reclaim_lag(&self) -> (u64, u64) {
        let gc = self.gc_state();
        let due = gc.oldest_due_ms.load(Ordering::Relaxed);
        let now = crate::shard::now_ms().unsigned_abs();
        let late = if due == 0 {
            0
        } else {
            now.saturating_sub(due) / 1000
        };
        (gc.unreclaimed.load(Ordering::Relaxed), late)
    }

    /// Decrements refused as inexact (`/operator/data.json`, cell-watch
    /// row 31): an index entry no `'R'` row counted.
    pub(crate) fn gc_inexact(&self) -> u64 {
        self.gc_state().counters.inexact.load(Ordering::Relaxed)
    }

    /// The `'G'` row a merge writes for a run-index object it merged away.
    pub(crate) fn retired_index_garbage(&self, object_len: u64) -> Vec<u8> {
        let due = crate::shard::now_ms()
            .unsigned_abs()
            .saturating_add(duration_ms(self.gc_state().settings().grace));
        gc::encode_garbage(&Garbage {
            kind: ObjectKind::RunIndex,
            object_len,
            due_ms: due,
        })
    }

    /// A merge is about to retire `ids`: any running read pins them.
    pub(crate) fn retiring_index(&self, ids: &[ObjectId]) {
        self.gc_state().prepare_zeroed(ids);
    }

    /// A merge's inputs left the set: reads that took the old set pin them.
    pub(crate) fn note_retired_index(&self, ids: &[ObjectId]) {
        self.gc_state().note_zeroed(ids);
    }

    /// The collector: once the sealer's open finished, each tick purges and
    /// reclaims what is due and deletes the garbage whose time came; every
    /// `sweep_every` it sweeps orphans.
    pub(in crate::shard) async fn gc_loop(self: Arc<Self>) {
        if self.seal_ready().await.is_err() {
            return;
        }
        let gc = self.gc_state();
        *lock(&gc.last_sweep) = Some(Instant::now());
        loop {
            let s = gc.settings();
            tokio::select! {
                () = self.closed() => {}
                () = tokio::time::sleep(s.tick) => {}
            }
            if self.is_closed() {
                return;
            }
            if !s.auto {
                continue;
            }
            self.until_closed(self.gc_pass()).await;
            let recount = lock(&gc.last_totals).is_none_or(|t| t.elapsed() >= s.totals_every);
            if recount {
                *lock(&gc.last_totals) = Some(Instant::now());
                self.until_closed(self.recount_totals()).await;
            }
            let due = lock(&gc.last_sweep).is_none_or(|t| t.elapsed() >= s.sweep_every);
            if due {
                *lock(&gc.last_sweep) = Some(Instant::now());
                self.until_closed(self.sweep_logged()).await;
            }
        }
    }

    async fn recount_totals(&self) {
        if let Ok(heads) = self.head_rows().await {
            let owing: Vec<u64> = heads
                .iter()
                .filter(|(_, h)| h.state == gc::HeadState::Requested || h.owes())
                .map(|(_, h)| h.not_before_ms)
                .collect();
            let gc = self.gc_state();
            gc.unreclaimed.store(owing.len() as u64, Ordering::Relaxed);
            gc.oldest_due_ms
                .store(owing.iter().copied().min().unwrap_or(0), Ordering::Relaxed);
        }
        match self.gc_totals().await {
            Ok(t) => *lock(&self.gc_state().totals) = Some(t),
            Err(e) => tracing::debug!(shard = %self.prefix, "gc totals not recounted: {e}"),
        }
    }

    async fn sweep_logged(&self) {
        if let Err(e) = self.sweep_orphans().await {
            tracing::warn!(shard = %self.prefix, "orphan sweep stopped: {e}");
        }
    }

    /// One pass: the purges and reclaims due, then the garbage due, each
    /// only while its signal is up.
    pub(crate) async fn gc_pass(&self) {
        let gc = self.gc_state();
        let heads = gc.heads_signal.load(Ordering::SeqCst);
        if heads > 0 {
            match self.purge_due().await {
                Ok(0) => lower(&gc.heads_signal, heads),
                Ok(_) => {}
                Err(e) => tracing::warn!(shard = %self.prefix, "purge pass stopped: {e}"),
            }
        }
        let garbage = gc.garbage_signal.load(Ordering::SeqCst);
        if garbage > 0 {
            match self.collect_counted().await {
                Ok((_, 0)) => lower(&gc.garbage_signal, garbage),
                Ok(_) => {}
                Err(e) => tracing::warn!(shard = %self.prefix, "garbage pass stopped: {e}"),
            }
        }
    }

    /// Delete every object whose `'G'` row is due and that no running read
    /// pins, then their `'G'` rows in one batch: how many were deleted
    /// (tests; the loop's pass reads `collect_counted`).
    #[cfg(test)]
    pub(crate) async fn collect(&self) -> Result<u64, GcError> {
        Ok(self.collect_counted().await?.0)
    }

    /// `collect`: (deleted, `'G'` rows left).
    async fn collect_counted(&self) -> Result<(u64, u64), GcError> {
        let gc = self.gc_state();
        let now = crate::shard::now_ms().unsigned_abs();
        let reopened = gc
            .opened_ms
            .load(Ordering::Relaxed)
            .saturating_add(duration_ms(gc.settings().grace));
        let mut done = Vec::new();
        let most = gc.settings().reclaim_batch.max(1);
        let rows = self.garbage_rows().await?;
        let total = rows.len() as u64;
        for (id, g) in rows {
            if done.len() >= most {
                break;
            }
            if g.due_ms.max(reopened) > now {
                continue;
            }
            if gc.pinned(id) {
                gc.counters.pinned.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let path = object_store::path::Path::from(self.object_path_of(g.kind, id));
            match object_store::ObjectStoreExt::delete(&*self.data_store, &path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    gc.counters.deleted.fetch_add(1, Ordering::Relaxed);
                    gc.counters
                        .deleted_bytes
                        .fetch_add(g.object_len, Ordering::Relaxed);
                    done.push(id);
                }
                Err(e) => {
                    gc.counters.delete_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(shard = %self.prefix, "garbage object {id:?} stays: {e}");
                }
            }
        }
        if done.is_empty() {
            return Ok((0, total));
        }
        let mut batch = WriteBatch::new();
        for id in &done {
            batch.delete(gc::garbage_key(*id));
        }
        self.write_index_batch(batch)
            .await
            .map_err(|e| GcError::Store(e.to_string()))?;
        for id in &done {
            gc.forget_zeroed(*id);
        }
        let deleted = done.len() as u64;
        Ok((deleted, total - deleted))
    }

    /// Every `'G'` row; one that does not decode is counted and left.
    pub(crate) async fn garbage_rows(&self) -> Result<Vec<(ObjectId, Garbage)>, GcError> {
        let store = |e: slatedb::Error| GcError::Store(e.to_string());
        let prefix = crate::spill::runindex::sentinel_prefix(gc::GARBAGE_TAG);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
        let mut out = Vec::new();
        while let Some(kv) = rows.next().await.map_err(store)? {
            let decoded = crate::spill::runindex::parse_sentinel_key(gc::GARBAGE_TAG, &kv.key)
                .and_then(|id| Some((id, gc::decode_garbage(&kv.value)?)));
            match decoded {
                Some(row) => out.push(row),
                None => {
                    self.gc_state()
                        .counters
                        .corrupt_rows
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(out)
    }

    /// Every `'R'` row, decoded (`None` for one that does not decode).
    pub(crate) async fn refcount_rows(&self) -> Result<Vec<(ObjectId, Option<Refcount>)>, GcError> {
        let store = |e: slatedb::Error| GcError::Store(e.to_string());
        let tag = crate::spill::directory::REFCOUNT_TAG;
        let prefix = crate::spill::runindex::sentinel_prefix(tag);
        let mut rows = self.db.scan_prefix(&prefix[..], ..).await.map_err(store)?;
        let mut out = Vec::new();
        while let Some(kv) = rows.next().await.map_err(store)? {
            if let Some(id) = crate::spill::runindex::parse_sentinel_key(tag, &kv.key) {
                out.push((id, gc::decode_refcount(&kv.value)));
            }
        }
        Ok(out)
    }

    /// An object's store path.
    pub(crate) fn object_path_of(&self, kind: ObjectKind, id: ObjectId) -> String {
        let shard = crate::sharddir::shard_db_path(&self.prefix);
        match kind {
            ObjectKind::Segment => crate::spill::object::object_path(&shard, id),
            ObjectKind::RunIndex => crate::spill::runindex::run_index_path(&shard, id),
        }
    }

    /// The live and garbage totals the `'R'` and `'G'` rows hold
    /// (`seal.gc.stored`): a scan, for the debug JSON and the soak.
    pub(crate) async fn gc_totals(&self) -> Result<serde_json::Value, GcError> {
        let refs = self.refcount_rows().await?;
        let garbage = self.garbage_rows().await?;
        let live: Vec<Refcount> = refs.iter().filter_map(|(_, r)| *r).collect();
        Ok(serde_json::json!({
            "live_objects": live.len(),
            "live_bytes": live.iter().map(|r| r.live_bytes).sum::<u64>(),
            "live_object_bytes": live.iter().map(|r| r.object_len).sum::<u64>(),
            "garbage_objects": garbage.len(),
            "garbage_bytes": garbage.iter().map(|(_, g)| g.object_len).sum::<u64>(),
        }))
    }
}

/// Lower a signal read as `seen`, unless something raised it since.
fn lower(signal: &AtomicU64, seen: u64) {
    if signal
        .compare_exchange(seen, 0, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        tracing::trace!("a gc signal was raised during its pass");
    }
}

pub(crate) fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
