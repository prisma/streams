//! The sealer (layout 6, DESIGN-6A §2.2 with REVIEW-6A F1/F2): it moves a
//! shard's unsealed pages out of the shard log into segment objects, so the
//! log's memtable never carries them into an L0.
//!
//! `ShardEngine::seal_once` seals one shard, one seal at a time:
//! 1. one awaited WAL flush, then a wait until durable dispatch published
//!    every group it covered, so each stream's durable frontier meets its
//!    applied one;
//! 2. a snapshot of every unsealed stream's durable pages
//!    `[durable.absorbed, durable.next)` from its handle's pending pages
//!    (in memory: a handle holding unsealed pages is never evicted, so a
//!    seal never reads the store);
//! 3. one object (`spill::object`) of chunks of at most
//!    `max_pages_per_chunk` pages, each with its routing-key summary;
//! 4. its PUT (`PutMode::Create`);
//! 5. its chunks published in the in-memory directory, before the commit
//!    can make any new boundary visible, so a reader that sees a boundary
//!    finds the chunks below it;
//! 6. one seal commit (`CommitOp::Seal`): the committer checks each stream
//!    against its live tail, moves its boundary, puts its pending row's
//!    remainder (every page above the boundary at that moment, REVIEW-6A
//!    F1) and stages the seal's `'K'` and `'R'` rows, in one group;
//! 7. (a) one awaited WAL flush and a wait until durable dispatch published
//!    each sealed stream's new boundary; (b) every `seals_per_flush` seals
//!    (or at the memtable trigger, a deadline, or a requested seal) one
//!    forced memtable flush: a later seal's remainder shadows the earlier
//!    seals' page operands, so that flush drops them all.
//!
//! Step 7a precedes **every** forced memtable flush (REVIEW-6A F2): the
//! deadlines', the idle release's and the queue collector's too. SlateDB's
//! memtable flush does not await its WAL, so without it the flush's
//! retention boundary misses the newest operands and their merge chains
//! reach the L0. A deadline flush that races a seal's commit (its 7a before
//! the commit, its memtable flush after) still carries that seal's pages
//! into an L0, where the remainder put shadows them: a storage cost until
//! compaction, never a data loss (TLA-016's successor, SealSpill).
//!
//! The loop (`seal_loop`) rebuilds the directory and loads every unsealed
//! stream at open, then evaluates the triggers every `tick` and when a
//! durable group wakes it. A trigger starts a seal; a deadline flushes the
//! memtable, seal or no seal (the pending rows' operands then reach the
//! L0, and the next seal's remainder put shadows them), waiting for a seal
//! in flight, whose own flush restarts the count, until twice the deadline. A failed seal
//! blocks the next for a backoff that doubles per failure.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use slatedb::config::{FlushOptions, FlushType};
use slatedb::db_stats;

use super::ShardEngine;
use crate::spill::directory::Directory;
use crate::spill::estimate::l0_estimate;
use crate::spill::metrics::ShardLogMetrics;

#[cfg(test)]
mod amplification_tests;
#[cfg(test)]
mod budget_tests;
mod consolidate;
#[cfg(test)]
mod consolidate_tests;
#[cfg(test)]
mod dispatch_tests;
pub(crate) mod fold;
#[cfg(test)]
mod fold_tests;
pub(crate) mod gc;
#[cfg(test)]
mod gc_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod memory_tests;
mod open;
pub(crate) mod read;
#[cfg(test)]
mod read_tests;
mod runs;
mod steps;
#[cfg(test)]
pub(crate) mod test_hooks;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod trigger_tests;

pub(crate) use steps::{SealAdvance, SealBatch, SealCommit, SealError, SealReport};

/// What starts a seal, and the deadlines that flush the memtable whatever
/// a seal does.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SealSettings {
    /// Seal at this L0 estimate of the memtable's writes since the last
    /// forced flush (SlateDB's own units, `spill::estimate`):
    /// `memtable_trigger` of the `L0_SST_SIZE_BYTES` at which SlateDB
    /// freezes the memtable by itself.
    pub(crate) memtable_bytes: u64,
    /// Seal at this many unsealed page bytes on the shard: the pages a
    /// seal takes are held in memory until it does.
    pub(crate) unsealed_bytes: u64,
    /// Seal at this many WAL objects since the last seal started.
    pub(crate) wal_objects_seal: u64,
    /// Flush the memtable after this many seals, or sooner at the memtable
    /// trigger, a deadline or a requested seal: each later seal's remainder
    /// put shadows the earlier seals' page operands, which the flush then
    /// drops instead of writing them to an L0 (S-gap step 2, k = 3).
    pub(crate) seals_per_flush: u64,
    /// Flush the memtable at this many, seal or no seal (the replay
    /// bound): a seal in flight is waited for (its own flush restarts the
    /// count) until twice this many.
    pub(crate) wal_objects_deadline: u64,
    /// Seal when the shard's oldest unsealed page is this old.
    pub(crate) age_seal: Duration,
    /// Flush the memtable when it has held an unsealed page this long (a
    /// seal in flight is waited for until twice this long).
    pub(crate) age_deadline: Duration,
    /// How often the triggers are evaluated besides a durable group's wake.
    pub(crate) tick: Duration,
    /// A chunk holds at most this many pages.
    pub(crate) max_pages_per_chunk: usize,
    /// One seal takes at most this many page bytes (its object's budget):
    /// a stream larger than what is left contributes a prefix of its pages
    /// (the first stream always at least one page), and the rest waits for
    /// the next seal, which follows at once (`Trigger::Deferred`).
    pub(crate) max_object_bytes: u64,
    /// One seal takes at most this many streams; the next seal starts after
    /// the last stream this one took, so none waits behind the others.
    pub(crate) max_streams: usize,
    /// Whether the loop seals and flushes by itself (tests that drive
    /// `seal_once` turn it off).
    pub(crate) auto: bool,
    /// A failed seal blocks new seals this long, doubling per failure up to
    /// `backoff_max`.
    pub(crate) backoff_min: Duration,
    pub(crate) backoff_max: Duration,
    /// The dispatch watchdog (`ShardEngine::watch_dispatch`): a durable
    /// group pending with no dispatch at all this long is warned about
    /// once (and counted)...
    pub(crate) dispatch_warn: Duration,
    /// ...and this long retires the engine.
    pub(crate) dispatch_stall: Duration,
}

/// The production dispatch watchdog: warn after 10 s, retire after 60 s
/// without any dispatch. A false retire on a shared cell is a whole-cell
/// outage, and a loaded small instance can starve the acker for seconds.
pub(crate) const DISPATCH_WARN: Duration = Duration::from_secs(10);
pub(crate) const DISPATCH_STALL: Duration = Duration::from_secs(60);
/// The test build's watchdog, so a test (or a mutant) whose dispatch
/// stalls fails within a second; every test rig's engine starts with it
/// (a test that parks dispatch on purpose sets its own).
#[cfg(test)]
pub(crate) const TEST_DISPATCH_WARN: Duration = Duration::from_millis(500);
#[cfg(test)]
pub(crate) const TEST_DISPATCH_STALL: Duration = Duration::from_secs(1);

impl Default for SealSettings {
    fn default() -> Self {
        Self {
            memtable_bytes: memtable_trigger(8 << 20),
            unsealed_bytes: 24 << 20,
            wal_objects_seal: 42,
            seals_per_flush: 3,
            wal_objects_deadline: 128,
            age_seal: Duration::from_secs(45),
            age_deadline: Duration::from_secs(60),
            tick: Duration::from_millis(250),
            max_pages_per_chunk: 8192,
            max_object_bytes: 64 << 20,
            max_streams: 1024,
            auto: true,
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(16),
            #[cfg(not(test))]
            dispatch_warn: DISPATCH_WARN,
            #[cfg(not(test))]
            dispatch_stall: DISPATCH_STALL,
            #[cfg(test)]
            dispatch_warn: TEST_DISPATCH_WARN,
            #[cfg(test)]
            dispatch_stall: TEST_DISPATCH_STALL,
        }
    }
}

/// The sealer's memtable trigger for a shard log whose SlateDB freezes its
/// memtable at `l0_sst_size_bytes`: three quarters of it, so a seal starts
/// before SlateDB's own freeze would carry the unsealed pages into an L0
/// (6 MiB at the 8 MiB default, 24 MiB at the shared cell's 32 MiB).
pub(crate) fn memtable_trigger(l0_sst_size_bytes: usize) -> u64 {
    u64::try_from(l0_sst_size_bytes / 4 * 3).unwrap_or(u64::MAX)
}

/// Why a seal started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trigger {
    Memtable,
    Unsealed,
    WalObjects,
    Age,
    /// The idle release, or a test.
    Requested,
    /// The previous seal left pages behind for its budget.
    Deferred,
}

/// Why the memtable was flushed without a seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Deadline {
    WalObjects,
    Age,
}

/// The sealer's counters (`GET /v1/debug/store`, tests).
#[derive(Default)]
pub(crate) struct SealCounters {
    pub(crate) seals: AtomicU64,
    pub(crate) empty_seals: AtomicU64,
    pub(crate) failures: AtomicU64,
    pub(crate) refused: AtomicU64,
    pub(crate) objects: AtomicU64,
    pub(crate) object_bytes: AtomicU64,
    pub(crate) page_bytes: AtomicU64,
    /// Bytes of the pending rows' remainders the seals put (the pages a
    /// seal left in the log, metered beside the object bytes).
    pub(crate) remainder_bytes: AtomicU64,
    pub(crate) deadline_flushes: AtomicU64,
    /// Deadline flushes that ran while a seal was in flight (one waited
    /// past twice its deadline): each may carry that seal's pages into an
    /// L0 until compaction (TLA-016-F5).
    pub(crate) deadline_flushes_during_seal: AtomicU64,
    /// Deadlines that found a seal in flight and waited for its flush.
    pub(crate) deadline_flushes_deferred: AtomicU64,
    pub(crate) flush_errors: AtomicU64,
    pub(crate) open_failures: AtomicU64,
    /// Streams a snapshot skipped because their unsealed pages did not
    /// tile their durable window (never, while unsealed handles stay
    /// resident).
    pub(crate) snapshot_gaps: AtomicU64,
    /// Streams the open skipped because their tail or pending row does not
    /// decode (each fails alone when loaded).
    pub(crate) corrupt_streams: AtomicU64,
    pub(crate) sealed_reads: AtomicU64,
    pub(crate) sealed_gets: AtomicU64,
    /// Chunks a keyed read skipped by their key summary, without a GET.
    pub(crate) summary_skips: AtomicU64,
    /// Chunks a keyed read fetched and found no page of its key in (the
    /// summary's false positives).
    pub(crate) summary_misses: AtomicU64,
    /// Durable dispatches attempted (the dispatch watchdog's progress
    /// signal; not served).
    pub(crate) dispatches: AtomicU64,
    /// Times a durable group stayed pending with no dispatch for the
    /// watchdog's warning threshold (each episode once).
    pub(crate) dispatch_stall_warnings: AtomicU64,
    /// Plans served from the chunk cache, without a GET.
    pub(crate) chunk_cache_hits: AtomicU64,
}

impl SealCounters {
    pub(crate) fn json(&self) -> serde_json::Value {
        let n = |c: &AtomicU64| c.load(Ordering::Relaxed);
        serde_json::json!({
            "seals": n(&self.seals),
            "empty_seals": n(&self.empty_seals),
            "failures": n(&self.failures),
            "refused": n(&self.refused),
            "objects": n(&self.objects),
            "object_bytes": n(&self.object_bytes),
            "page_bytes": n(&self.page_bytes),
            "remainder_bytes": n(&self.remainder_bytes),
            "deadline_flushes": n(&self.deadline_flushes),
            "deadline_flushes_during_seal": n(&self.deadline_flushes_during_seal),
            "deadline_flushes_deferred": n(&self.deadline_flushes_deferred),
            "flush_errors": n(&self.flush_errors),
            "open_failures": n(&self.open_failures),
            "snapshot_gaps": n(&self.snapshot_gaps),
            "corrupt_streams": n(&self.corrupt_streams),
            "sealed_reads": n(&self.sealed_reads),
            "sealed_gets": n(&self.sealed_gets),
            "summary_skips": n(&self.summary_skips),
            "summary_misses": n(&self.summary_misses),
            "dispatch_stall_warnings": n(&self.dispatch_stall_warnings),
            "chunk_cache_hits": n(&self.chunk_cache_hits),
        })
    }
}

/// The counters at the last forced memtable flush: the memtable estimate
/// and the WAL objects are deltas from here.
#[derive(Clone, Copy)]
struct Baseline {
    write_ops: u64,
    kv_bytes: u64,
    wal_objects: u64,
    pump_flushes: u64,
    at: Instant,
}

impl Default for Baseline {
    fn default() -> Self {
        Self {
            write_ops: 0,
            kv_bytes: 0,
            wal_objects: 0,
            pump_flushes: 0,
            at: Instant::now(),
        }
    }
}

/// The sealer's state on one engine (part of `LayoutState`).
#[derive(Default)]
pub(crate) struct SealState {
    settings: Mutex<SealSettings>,
    /// One seal at a time.
    serial: tokio::sync::Mutex<()>,
    /// One forced memtable flush at a time.
    flush_lock: tokio::sync::Mutex<()>,
    /// After a failed seal no seal starts before this; the wait doubles per
    /// consecutive failure.
    retry_after: Mutex<Option<(Instant, Duration)>>,
    next_seq: AtomicU64,
    /// Set by a seal its budget cut short: the loop seals again at its next
    /// evaluation, whatever the triggers say.
    deferred: std::sync::atomic::AtomicBool,
    /// The last stream a budget-capped seal took: the next snapshot starts
    /// after it.
    cursor: Mutex<Option<[u8; 16]>>,
    baseline: Mutex<Baseline>,
    /// The WAL objects since the last forced flush when the last seal
    /// started (0 at the flush): the seal trigger counts from there.
    wal_at_seal: AtomicU64,
    /// Seals since the last forced flush.
    seals_since_flush: AtomicU64,
    /// When the shard's oldest unsealed page was committed (or earlier);
    /// None while nothing is unsealed.
    oldest_unsealed: Mutex<Option<Instant>>,
    metrics: std::sync::OnceLock<Option<Arc<ShardLogMetrics>>>,
    wake: tokio::sync::Notify,
    /// The in-memory seal directory, rebuilt at open from the `'K'` rows.
    pub(crate) directory: RwLock<Directory>,
    /// Set once the open's rebuild finished; sealed reads wait for it.
    ready: tokio::sync::watch::Sender<bool>,
    pub(crate) counters: SealCounters,
    /// The fold and the run-index set (`fold`).
    pub(crate) fold: fold::FoldState,
    /// Physical reclamation (`gc`).
    pub(crate) gc: gc::GcState,
    #[cfg(test)]
    pub(crate) hooks: test_hooks::SealHooks,
}

impl SealState {
    /// Seal at the memtable trigger of a shard log that freezes its
    /// memtable at `freeze_bytes` (`memtable_trigger`).
    fn follow_freeze_bytes(&self, freeze_bytes: usize) {
        self.settings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .memtable_bytes = memtable_trigger(freeze_bytes);
    }

    /// The settings in force.
    #[expect(
        clippy::unwrap_used,
        reason = "SealState::settings; a poisoned settings lock may hold a half-written configuration; recovering it could seal or flush on a trigger nobody set, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    pub(crate) fn settings(&self) -> SealSettings {
        *self.settings.lock().unwrap()
    }

    /// Change the settings (tests, and the DST fixtures' cadences).
    #[cfg(test)]
    pub(crate) fn configure(&self, change: impl FnOnce(&mut SealSettings)) {
        change(&mut self.settings.lock().unwrap());
        self.wake.notify_one();
    }

    /// Turn the loop's own triggers and deadlines on or off.
    #[expect(
        clippy::unwrap_used,
        reason = "SealState::set_auto; a poisoned settings lock may hold a half-written configuration; recovering it could seal or flush on a trigger nobody set, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn set_auto(&self, auto: bool) {
        self.settings.lock().unwrap().auto = auto;
        self.wake.notify_one();
    }

    /// A durable group added unsealed pages.
    #[expect(
        clippy::unwrap_used,
        reason = "SealState::note_unsealed; a poisoned age lock may hold a half-written age; recovering it could let the age trigger and deadline miss an unsealed page, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    pub(crate) fn note_unsealed(&self) {
        self.oldest_unsealed
            .lock()
            .unwrap()
            .get_or_insert_with(Instant::now);
    }

    /// Wait until the open's rebuild of the directory finished.
    pub(crate) async fn wait_ready(&self) {
        let mut ready = self.ready.subscribe();
        if ready.wait_for(|r| *r).await.is_err() {
            tracing::debug!("the sealer's state went away before its open finished");
        }
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        let mut j = self.counters.json();
        if let Ok(d) = self.directory.read() {
            j["directory_entries"] = d.entries().into();
            j["directory_streams"] = d.streams().into();
            j["directory_bytes"] = d.bytes().into();
        }
        j["fold"] = self.fold.json();
        j["consolidation"] = self.fold.consolidator.json();
        j["caches"] = crate::spill::cache::json();
        j["attribution"] = crate::spill::metrics::attribution();
        j["gc"] = self.gc.json();
        j
    }

    /// The next object sequence of this opening (seals, folds, merges and
    /// consolidations share it).
    pub(crate) fn next_object_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[expect(
        clippy::unwrap_used,
        reason = "SealState::backing_off; a poisoned backoff lock may hold a half-written retry time; recovering it could start seals a failing store keeps refusing, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn backing_off(&self) -> bool {
        self.retry_after
            .lock()
            .unwrap()
            .is_some_and(|(at, _)| Instant::now().checked_duration_since(at).is_none())
    }

    /// Clear the backoff after a seal, or double it after a failure.
    #[expect(
        clippy::unwrap_used,
        reason = "SealState::after_seal; a poisoned backoff lock may hold a half-written retry time; recovering it could start seals a failing store keeps refusing, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn after_seal(&self, ok: bool) {
        let s = self.settings();
        let mut retry = self.retry_after.lock().unwrap();
        *retry = (!ok).then(|| {
            let wait = retry.map_or(s.backoff_min, |(_, w)| (w * 2).min(s.backoff_max));
            (Instant::now() + wait, wait)
        });
    }

    fn counter(&self, metric: &str) -> u64 {
        self.metrics
            .get()
            .and_then(Option::as_ref)
            .map_or(0, |m| m.counter(metric))
    }
}

/// The trigger dimensions now.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Measure {
    /// SlateDB's L0 estimate of the memtable's writes since the last forced
    /// flush (the unsealed page bytes for a log no builder gave metrics).
    pub(crate) memtable: u64,
    /// Unsealed page bytes on the shard.
    pub(crate) unsealed: u64,
    pub(crate) wal_objects: u64,
    /// Age of the shard's oldest unsealed page.
    pub(crate) oldest: Option<Duration>,
    /// How long the memtable has held an unsealed page.
    pub(crate) memtable_age: Option<Duration>,
}

impl Measure {
    fn trigger(&self, s: &SealSettings) -> Option<Trigger> {
        if self.memtable >= s.memtable_bytes {
            Some(Trigger::Memtable)
        } else if self.unsealed >= s.unsealed_bytes {
            Some(Trigger::Unsealed)
        } else if self.wal_objects >= s.wal_objects_seal {
            Some(Trigger::WalObjects)
        } else if self.oldest.is_some_and(|a| a >= s.age_seal) {
            Some(Trigger::Age)
        } else {
            None
        }
    }

    fn deadline(&self, s: &SealSettings) -> Option<Deadline> {
        if self.wal_objects >= s.wal_objects_deadline {
            Some(Deadline::WalObjects)
        } else if self.memtable_age.is_some_and(|a| a >= s.age_deadline) {
            Some(Deadline::Age)
        } else {
            None
        }
    }

    /// Past twice a deadline: the flush no longer waits for a seal.
    fn overdue(&self, s: &SealSettings) -> bool {
        self.wal_objects >= s.wal_objects_deadline.saturating_mul(2)
            || self.memtable_age.is_some_and(|a| a >= s.age_deadline * 2)
    }
}

/// How often the loop evicts idle handles.
const EVICT_EVERY: Duration = Duration::from_secs(5);

impl ShardEngine {
    fn seal_state(&self) -> &SealState {
        &self.layout.seal
    }

    /// The sealer: the open's rebuild (retried until it succeeds), then the
    /// triggers, one seal at a time, and idle handle eviction. The
    /// deadlines run beside it (`deadline_loop`), so a slow seal holds a
    /// deadline flush back by at most the deadline again.
    pub(super) async fn seal_loop(self: Arc<Self>) {
        let sp = self.seal_state();
        let metrics = sp.metrics.get_or_init(|| {
            crate::spill::metrics::of_log(&crate::sharddir::shard_db_path(&self.prefix))
        });
        if let Some(freeze) = metrics.as_ref().and_then(|m| m.freeze_bytes()) {
            sp.follow_freeze_bytes(freeze);
        }
        self.reset_baseline();
        let mut closed = self.close_tx.subscribe();
        let mut retry = Duration::from_millis(50);
        while let Err(e) = self.seal_open().await {
            sp.counters.open_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(shard = %self.prefix, "sealer open failed, retrying: {e}");
            tokio::select! {
                _ = closed.changed() => {}
                () = tokio::time::sleep(retry) => {}
            }
            if self.is_closed() {
                return;
            }
            retry = (retry * 2).min(Duration::from_secs(5));
        }
        sp.ready.send_replace(true);
        let mut evicted_at = Instant::now();
        let mut dispatch_seen = None;
        while self.seal_wait(&mut closed).await {
            if evicted_at.elapsed() >= EVICT_EVERY {
                evicted_at = Instant::now();
                self.evict_idle();
            }
            self.watch_dispatch(&mut dispatch_seen);
            let settings = sp.settings();
            if !settings.auto || sp.backing_off() {
                continue;
            }
            let deferred = sp
                .deferred
                .swap(false, Ordering::AcqRel)
                .then_some(Trigger::Deferred);
            if let Some(trigger) = deferred.or_else(|| self.trigger_measure().trigger(&settings)) {
                self.until_closed(self.seal_triggered(trigger)).await;
            }
        }
    }

    /// The deadlines: flush the memtable, seal or no seal. A seal in
    /// flight flushes at its end when a deadline is reached (step 7b), which
    /// restarts the count, so a deadline waits for it rather than flush
    /// mid-seal, until twice the deadline (a stuck seal never holds the
    /// replay bound back further).
    pub(super) async fn deadline_loop(self: Arc<Self>) {
        let sp = self.seal_state();
        sp.wait_ready().await;
        let mut closed = self.close_tx.subscribe();
        let mut deferring = false;
        while self.seal_wait(&mut closed).await {
            let settings = sp.settings();
            if !settings.auto {
                continue;
            }
            let measure = self.seal_measure();
            let Some(kind) = measure.deadline(&settings) else {
                deferring = false;
                continue;
            };
            if self.defer_deadline(&measure, &settings, &mut deferring) {
                continue;
            }
            self.until_closed(self.deadline_flush(kind)).await;
        }
    }

    /// Whether a reached deadline waits for the seal in flight (none past
    /// twice the deadline); the wait is counted once.
    fn defer_deadline(&self, measure: &Measure, s: &SealSettings, deferring: &mut bool) -> bool {
        let sp = self.seal_state();
        let defer = sp.serial.try_lock().is_err() && !measure.overdue(s);
        if defer && !*deferring {
            sp.counters
                .deadline_flushes_deferred
                .fetch_add(1, Ordering::Relaxed);
        }
        *deferring = defer;
        defer
    }

    /// `work` unless the engine closes first. A close leaves a seal or a
    /// flush where it is, as the workers' grace would abort it, instead of
    /// waiting for it: one parked in a forced memtable flush while the log
    /// is at its L0 cap waits for the compactor, and an abort at the grace
    /// fails the close. Every step boundary a seal can stop at is safe
    /// (the tiling DST), and the next owner's open seals what is left.
    async fn until_closed(&self, work: impl std::future::Future<Output = ()>) {
        tokio::select! {
            biased;
            () = self.closed() => {}
            () = work => {}
        }
    }

    /// The next tick or wake; false once the engine closes.
    async fn seal_wait(&self, closed: &mut tokio::sync::watch::Receiver<bool>) -> bool {
        let sp = self.seal_state();
        tokio::select! {
            _ = closed.changed() => {}
            () = tokio::time::sleep(sp.settings().tick) => {}
            () = sp.wake.notified() => {}
        }
        !self.is_closed()
    }

    /// One seal a trigger started.
    async fn seal_triggered(self: &Arc<Self>, trigger: Trigger) {
        let sp = self.seal_state();
        let result = self.seal_once(trigger).await;
        sp.after_seal(result.is_ok());
        match &result {
            Ok(r) => tracing::debug!(
                shard = %self.prefix,
                ?trigger,
                object_bytes = r.object_bytes,
                streams = r.streams,
                chunks = r.chunks,
                remainder_bytes = r.remainder_bytes,
                "sealed"
            ),
            Err(SealError::Closed) => {}
            Err(e) => tracing::warn!(shard = %self.prefix, ?trigger, "seal failed: {e}"),
        }
    }

    /// Evict idle handles (a handle holding unsealed pages is held by the
    /// unsealed set, so it is never idle).
    fn evict_idle(&self) {
        let idle = self.handle_idle_evict;
        if idle.is_zero() && self.handle_max_resident == 0 {
            return;
        }
        let evicted = self.evict_idle_handles(idle, self.handle_max_resident);
        if let Some(evicted) = std::num::NonZeroUsize::new(evicted) {
            tracing::debug!(shard = %self.prefix, "evicted {evicted} idle stream handles");
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::reset_baseline; a poisoned baseline lock may hold a half-written baseline; recovering it could put the memtable trigger and deadline on counters of another flush, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    fn reset_baseline(&self) {
        let sp = self.seal_state();
        *sp.baseline.lock().unwrap() = Baseline {
            write_ops: sp.counter(db_stats::WRITE_OPS),
            kv_bytes: sp.counter(db_stats::MEMTABLE_WRITE_BYTES),
            wal_objects: sp.counter(slatedb::wal_buffer_stats::WAL_BUFFER_FLUSHES),
            pump_flushes: self.pump_flushes.load(Ordering::Relaxed),
            at: Instant::now(),
        };
    }

    /// The trigger dimensions now. A log no builder gave metrics counts its
    /// memtable by unsealed page bytes and its WAL objects by the pump's
    /// flushes.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::seal_measure; a poisoned baseline or age lock may hold a half-written value; recovering it could let a trigger or deadline miss what the memtable holds, whereas panicking ends the sealer, a required engine task, whose exit retires the engine, and the next open rebuilds this state from the store"
    )]
    pub(crate) fn seal_measure(&self) -> Measure {
        let sp = self.seal_state();
        let b = *sp.baseline.lock().unwrap();
        let unsealed = self.maintenance_snapshot().unabsorbed_frame_bytes;
        let (memtable, wal_objects) = if sp.metrics.get().is_some_and(Option::is_some) {
            let ops = sp.counter(db_stats::WRITE_OPS).saturating_sub(b.write_ops);
            let kv = sp
                .counter(db_stats::MEMTABLE_WRITE_BYTES)
                .saturating_sub(b.kv_bytes);
            let wal = sp
                .counter(slatedb::wal_buffer_stats::WAL_BUFFER_FLUSHES)
                .saturating_sub(b.wal_objects);
            (l0_estimate(ops, kv), wal)
        } else {
            let pump = self.pump_flushes.load(Ordering::Relaxed);
            (unsealed, pump.saturating_sub(b.pump_flushes))
        };
        let oldest = *sp.oldest_unsealed.lock().unwrap();
        Measure {
            memtable,
            unsealed,
            wal_objects,
            oldest: oldest.map(|t| t.elapsed()),
            memtable_age: oldest.map(|t| t.max(b.at).elapsed()),
        }
    }

    /// The trigger dimensions with the WAL objects counted since the last
    /// seal started, not since the last flush (a seal flushes only every
    /// `seals_per_flush`).
    fn trigger_measure(&self) -> Measure {
        let m = self.seal_measure();
        let since = self.seal_state().wal_at_seal.load(Ordering::Relaxed);
        Measure {
            wal_objects: m.wal_objects.saturating_sub(since),
            ..m
        }
    }

    /// Whether the seal that just finished ends in a forced flush: every
    /// `seals_per_flush` seals, at the memtable trigger or a deadline, and
    /// for a seal another part of the process asked for (the idle release
    /// closes the engine after it, so its log must not need a replay).
    pub(super) fn flush_after_seal(&self, trigger: Trigger) -> bool {
        let sp = self.seal_state();
        let s = sp.settings();
        let seals = sp.seals_since_flush.fetch_add(1, Ordering::Relaxed) + 1;
        let m = self.seal_measure();
        trigger == Trigger::Requested
            || seals >= s.seals_per_flush
            || m.memtable >= s.memtable_bytes
            || m.deadline(&s).is_some()
    }

    /// A seal starts: the seal trigger counts WAL objects from here.
    pub(super) fn note_seal_start(&self) {
        let wal = self.seal_measure().wal_objects;
        self.seal_state().wal_at_seal.store(wal, Ordering::Relaxed);
    }

    /// One awaited WAL flush.
    async fn wal_flush(&self) -> Result<(), slatedb::Error> {
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
    }

    /// One forced memtable flush, after step 7a (an awaited WAL flush)
    /// unless the caller's seal already took it.
    async fn forced_flush(&self, with_7a: bool) -> Result<(), String> {
        let sp = self.seal_state();
        let _one_at_a_time = sp.flush_lock.lock().await;
        #[cfg(test)]
        let with_7a = sp.takes_7a(with_7a);
        if with_7a {
            self.wal_flush().await.map_err(|e| e.to_string())?;
        }
        self.reset_baseline();
        sp.wal_at_seal.store(0, Ordering::Relaxed);
        sp.seals_since_flush.store(0, Ordering::Relaxed);
        let flushed = self
            .db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .map_err(|e| e.to_string());
        if flushed.is_err() {
            sp.counters.flush_errors.fetch_add(1, Ordering::Relaxed);
        }
        flushed
    }

    /// A deadline: flush the memtable, seal or no seal.
    async fn deadline_flush(&self, kind: Deadline) {
        let sp = self.seal_state();
        sp.counters.deadline_flushes.fetch_add(1, Ordering::Relaxed);
        if sp.serial.try_lock().is_err() {
            sp.counters
                .deadline_flushes_during_seal
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Err(e) = self.forced_flush(true).await {
            tracing::warn!(shard = %self.prefix, ?kind, "deadline memtable flush failed: {e}");
        }
    }

    /// Change the sealer's settings (tests and the DST fixtures' cadences).
    #[cfg(test)]
    pub(crate) fn configure_seal(&self, change: impl FnOnce(&mut SealSettings)) {
        self.seal_state().configure(change);
    }

    /// Pause (`true`) or resume the sealer's own triggers and deadlines
    /// (`POST /v1/debug/seal-pause`); a seal in flight finishes, and a seal
    /// another part of the process asks for still runs.
    pub(crate) fn pause_seal(&self, paused: bool) {
        self.seal_state().set_auto(!paused);
    }

    /// Tests: the seal commits this engine's sealer handed to the committer.
    #[cfg(test)]
    pub(crate) fn seal_commits_sent(&self) -> u64 {
        self.seal_state().hooks.commits_sent.load(Ordering::Acquire)
    }

    /// Seal-index rows this engine skipped as undecodable (`/operator/data.json`).
    pub(crate) fn sealed_index_corrupt_rows(&self) -> u64 {
        self.seal_state()
            .fold
            .counters
            .corrupt_rows
            .load(Ordering::Relaxed)
    }

    /// The sealer's counters and directory size (`GET /v1/debug/timings`, each shard's `seal`).
    pub(crate) fn seal_json(&self) -> serde_json::Value {
        self.seal_state().json()
    }

    /// A seal's commit put `bytes` of remainder on a stream's pending row,
    /// metered beside the object bytes. The stream stays held without a
    /// hold here: it is in the unsealed set while it has pages, and only the
    /// sealer's release after this commit takes it out, once none is left.
    pub(crate) fn note_remainder(&self, bytes: u64) {
        self.seal_state()
            .counters
            .remainder_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// A seal another part of the process asks for (the idle release),
    /// once the sealer's open finished.
    pub(crate) async fn seal_requested(self: &Arc<Self>) -> Result<SealReport, SealError> {
        self.seal_ready()
            .await
            .map_err(|e| SealError::Store(e.to_string()))?;
        self.seal_once(Trigger::Requested).await
    }

    /// A memtable flush another part of the engine asks for (the queue
    /// collector): it takes step 7a like every forced flush.
    pub(crate) async fn flush_memtable(&self) -> Result<(), String> {
        self.forced_flush(true).await
    }
}
