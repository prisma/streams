//! L6 spike M3: the seal path behind the existing write path (design B
//! §1.2 steps 3-9, SPIKE-PLAN §3 M3 with review B1/B2/A1).
//!
//! `spill_once` seals one shard, serialized per shard:
//! 1. force one WAL flush;
//! 2. take `[absorbed, durable.next)` of every stream with unabsorbed pages
//!    from the ring, or seek its pages (`read_frames_range`) where the ring
//!    does not cover the window;
//! 3. build one object (`spill::object`), chunks of at most
//!    `max_pages_per_chunk` pages;
//! 4. PUT it with `PutMode::Create`;
//! 5. submit the advances as `AbsorbedBatch` groups (`submit_absorbed_batch_v2`)
//!    of at most one chunk per stream and `max_streams_per_group` streams,
//!    one group at a time, with each chunk's descriptor in the engine's side
//!    table so `CommitTransaction::expand` is unchanged;
//! 6. the committer's `Exact` retirement stages the index row and one
//!    tombstone per sealed page, with `trim_safe_to = upto` (zero lag);
//! 7. once the groups are applied: (a) `flush_with_options(Wal)`, awaited,
//!    which makes step 6 durable; (b) `flush_with_options(MemTable)`, whose
//!    L0 then keeps only the tombstone of a value and its tombstone.
//!    (b) alone is not enough: SlateDB's memtable flush requests a WAL flush
//!    but freezes without waiting for it (`batch_write.rs:347-360` at
//!    `0717cc1`), so its retention boundary (`flush.rs:201-209`, the durable
//!    seq) misses the tombstones and every value reaches the L0 (measured by
//!    `flushdrop`: 101% of the sealed bytes without (a)). These are the two
//!    forced WAL flushes per seal of rBn F4 (W2), steps 1 and 7a.
//!
//! `spill_loop` evaluates the triggers after each durable group and every
//! `tick`, and flushes the memtable at the deadlines whatever a seal does.
//! Throwaway code on the never-merged `l6-spike` branch.
#![allow(dead_code, reason = "L6 spike: throwaway sealer; some fields are read only by tests")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::{PutMode, PutOptions};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::db_stats;

use super::{CopiedBytes, ShardEngine, StreamHandle, Submissions, read_frames_range};
use crate::crypto_page::CheckedPage;
use crate::spill::estimate::l0_estimate;
use crate::spill::index::{IndexRow, index_key};
use crate::spill::object::{self, ChunkInput, ObjectError, ObjectId};
use crate::spill::settings::{Deadline, SpillSettings, Trigger, TriggerMode};
use crate::spill::stats::SpillStats;

pub(crate) mod read;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod tests;

/// A chunk advance's fate in the committer.
pub(crate) const OUTCOME_PENDING: u8 = 0;
pub(crate) const OUTCOME_EXACT: u8 = 1;
pub(crate) const OUTCOME_DETACHED: u8 = 2;
pub(crate) const OUTCOME_DIVERGED: u8 = 3;
/// Dropped before retirement (a duplicate, or a refused lane).
pub(crate) const OUTCOME_STALE: u8 = 4;

/// One sealed chunk as the committer stages it: its stream, its window, its
/// index row and its pages' last offsets (one tombstone each).
pub(crate) struct ChunkDescriptor {
    pub(crate) hash: [u8; 16],
    pub(crate) from: u64,
    pub(crate) upto: u64,
    pub(crate) index_key: Vec<u8>,
    pub(crate) index_value: Vec<u8>,
    pub(crate) page_lasts: Vec<u64>,
    outcome: AtomicU8,
}

impl ChunkDescriptor {
    pub(crate) fn set_outcome(&self, outcome: u8) {
        self.outcome.store(outcome, Ordering::Release);
    }

    pub(crate) fn outcome(&self) -> u8 {
        self.outcome.load(Ordering::Acquire)
    }
}

/// Where a test stops a seal (the tiling property's abort points): after
/// step 1, after step 4, after step 6 (every group applied, not yet
/// durable), after step 7a (durable and dispatched, before the memtable
/// flush).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SealStep {
    AfterWalFlush,
    AfterPut,
    AfterApplied,
    AfterDurable,
}

#[derive(Debug)]
pub(crate) enum SealError {
    Closed,
    Store(String),
    Encode(ObjectError),
    /// The committer refused or dropped an advance.
    Refused(String),
    Aborted(SealStep),
}

impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// What one seal did.
#[derive(Clone, Debug, Default)]
pub(crate) struct SealReport {
    pub(crate) object: Option<ObjectId>,
    pub(crate) object_bytes: u64,
    pub(crate) streams: usize,
    pub(crate) chunks: usize,
    pub(crate) pages: u64,
    pub(crate) page_bytes: u64,
    pub(crate) groups: usize,
    pub(crate) ring_fallbacks: usize,
    /// `l0_flush_bytes` delta around the step-7 flush (0 without metrics).
    pub(crate) l0_bytes: u64,
    /// `immutable_memtable_flushes` delta around the step-7 flush.
    pub(crate) imm_flushes: u64,
    /// Unabsorbed page bytes on the shard when the memtable was flushed.
    pub(crate) leak_bytes: u64,
    pub(crate) put_ms: u64,
    pub(crate) commit_ms: u64,
    pub(crate) flush_ms: u64,
    pub(crate) total_ms: u64,
    /// M5: step 1 (the awaited WAL flush and the catch-up), and the start
    /// of step 1 to the end of step 7b (the census's seal duration, which
    /// leaves out the wait for an earlier seal's advances to settle).
    pub(crate) wal_ms: u64,
    pub(crate) steps_ms: u64,
}

/// The counters' values at the last forced memtable flush: the memtable
/// dimension and the WAL objects are deltas from here.
#[derive(Clone, Copy)]
struct Baseline {
    write_ops: u64,
    kv_bytes: u64,
    wal_objects: u64,
    pump_flushes: u64,
    at: Instant,
}

/// The sealer's state on one engine (spill arm).
pub(crate) struct SpillState {
    pub(crate) settings: SpillSettings,
    /// M4: the triggers a DST fixture's absorber cadence put in place of
    /// `settings` (`ShardEngine::spill_adopt_absorber_cadence`).
    cadence: Mutex<Option<SpillSettings>>,
    serial: tokio::sync::Mutex<()>,
    /// Forced memtable flushes run one at a time, so each one's delta of
    /// `immutable_memtable_flushes` is its own (review A1).
    flush_lock: tokio::sync::Mutex<()>,
    running: AtomicBool,
    /// After a failed seal, no seal starts before this (deadlines still
    /// flush); the wait doubles per consecutive failure.
    retry_after: Mutex<Option<(Instant, Duration)>>,
    next_seq: AtomicU64,
    submissions: Arc<Submissions>,
    /// Streams with an advance that may not have settled yet.
    unsettled: Mutex<Vec<[u8; 16]>>,
    side: Mutex<HashMap<([u8; 16], u64), Arc<ChunkDescriptor>>>,
    baseline: Mutex<Baseline>,
    /// When the oldest unabsorbed page on the shard was committed (or a
    /// conservative earlier time); None when every stream is absorbed.
    oldest_unabsorbed: Mutex<Option<Instant>>,
    ring_refused_at_seal: AtomicU64,
    pub(crate) stats: Arc<SpillStats>,
    pub(crate) metrics: Option<Arc<crate::l6_metrics::ShardDbMetrics>>,
    wake: tokio::sync::Notify,
    #[cfg(test)]
    pub(crate) abort_at: Mutex<Option<SealStep>>,
    /// Tests: fail every object PUT, or hold it this long first.
    #[cfg(test)]
    pub(crate) fail_puts: AtomicBool,
    #[cfg(test)]
    pub(crate) put_delay: Mutex<Duration>,
    /// Tests (M4): scan index rows at this level whatever the read's
    /// visibility (the negative control reads at Remote).
    #[cfg(test)]
    pub(crate) index_durability_override: Mutex<Option<slatedb::config::DurabilityLevel>>,
    /// Tests (M4): park every sealed-range GET until released.
    #[cfg(test)]
    pub(crate) get_hold: Mutex<Option<Arc<read::GetHold>>>,
    /// Tests (M4): park a seal after its PUT, before its commit (step 5).
    #[cfg(test)]
    pub(crate) before_commit: Mutex<Option<Arc<read::GetHold>>>,
}

impl SpillState {
    pub(crate) fn new(settings: SpillSettings, db_path: &str) -> Arc<Self> {
        Arc::new(Self {
            settings,
            cadence: Mutex::new(None),
            serial: tokio::sync::Mutex::new(()),
            flush_lock: tokio::sync::Mutex::new(()),
            running: AtomicBool::new(false),
            retry_after: Mutex::new(None),
            next_seq: AtomicU64::new(1),
            submissions: Arc::new(Submissions::default()),
            unsettled: Mutex::new(Vec::new()),
            side: Mutex::new(HashMap::new()),
            baseline: Mutex::new(Baseline {
                write_ops: 0,
                kv_bytes: 0,
                wal_objects: 0,
                pump_flushes: 0,
                at: Instant::now(),
            }),
            oldest_unabsorbed: Mutex::new(None),
            ring_refused_at_seal: AtomicU64::new(0),
            stats: crate::spill::stats::for_db(db_path),
            metrics: crate::l6_metrics::get(db_path),
            wake: tokio::sync::Notify::new(),
            #[cfg(test)]
            abort_at: Mutex::new(None),
            #[cfg(test)]
            fail_puts: AtomicBool::new(false),
            #[cfg(test)]
            put_delay: Mutex::new(Duration::ZERO),
            #[cfg(test)]
            index_durability_override: Mutex::new(None),
            #[cfg(test)]
            get_hold: Mutex::new(None),
            #[cfg(test)]
            before_commit: Mutex::new(None),
        })
    }

    /// The triggers in force: a fixture's absorber cadence, or `settings`.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    pub(crate) fn current(&self) -> SpillSettings {
        self.cadence.lock().unwrap().unwrap_or(self.settings)
    }

    fn counter(&self, metric: &str) -> u64 {
        self.metrics.as_ref().map_or(0, |m| m.counter(metric))
    }

    #[cfg(test)]
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    fn check_abort(&self, step: SealStep) -> Result<(), SealError> {
        if *self.abort_at.lock().unwrap() == Some(step) {
            return Err(SealError::Aborted(step));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[expect(clippy::unnecessary_wraps, reason = "L6 spike: the test build stops seals here")]
    fn check_abort(&self, _step: SealStep) -> Result<(), SealError> {
        Ok(())
    }

    /// A durable group added unabsorbed pages.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    pub(crate) fn note_unabsorbed(&self) {
        let mut oldest = self.oldest_unabsorbed.lock().unwrap();
        if oldest.is_none() {
            *oldest = Some(Instant::now());
        }
    }

    pub(crate) fn wake(&self) {
        self.wake.notify_one();
    }

    /// Whether a failed seal's backoff still holds.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    fn backing_off(&self) -> bool {
        self.retry_after
            .lock()
            .unwrap()
            .is_some_and(|(at, _)| Instant::now() < at)
    }

    /// Reset the backoff after a seal, or double it (0.5 s to 16 s) after a
    /// failure.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    fn after_seal(&self, ok: bool) {
        let mut retry = self.retry_after.lock().unwrap();
        let s = self.current();
        *retry = if ok {
            None
        } else {
            let wait = retry.map_or(s.backoff_min, |(_, w)| (w * 2).min(s.backoff_max));
            Some((Instant::now() + wait, wait))
        };
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        self.stats.json()
    }
}

/// The trigger dimensions now.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Measure {
    /// SlateDB's L0 estimate of the memtable since the last forced flush.
    pub(crate) mem_estimate: u64,
    /// Unabsorbed page bytes on the shard.
    pub(crate) pages: u64,
    pub(crate) wal_objects: u64,
    /// Age of the oldest unabsorbed page.
    pub(crate) oldest_age: Option<Duration>,
    /// How long the memtable has held an unabsorbed page.
    pub(crate) memtable_age: Option<Duration>,
    pub(crate) ring: bool,
}

impl Measure {
    fn trigger(&self, s: &SpillSettings) -> Option<Trigger> {
        let (dimension, trigger) = match s.mode {
            TriggerMode::Memtable => (self.mem_estimate, Trigger::Memtable),
            TriggerMode::Pages => (self.pages, Trigger::Pages),
        };
        if dimension >= s.memtable_bytes {
            Some(trigger)
        } else if self.wal_objects >= s.wal_objects_seal {
            Some(Trigger::WalObjects)
        } else if self.oldest_age.is_some_and(|a| a >= s.age_seal) {
            Some(Trigger::Age)
        } else if self.ring {
            Some(Trigger::Ring)
        } else {
            None
        }
    }

    fn deadline(&self, s: &SpillSettings) -> Option<Deadline> {
        if self.wal_objects >= s.wal_objects_deadline {
            Some(Deadline::WalObjects)
        } else if self.memtable_age.is_some_and(|a| a >= s.age_deadline) {
            Some(Deadline::Age)
        } else {
            None
        }
    }
}

/// One stream's window to seal.
struct Window {
    handle: Arc<StreamHandle>,
    hash: [u8; 16],
    from: u64,
    /// (first offset, last offset, stored page, record timestamp) per page.
    pages: Vec<(u64, u64, Bytes, i64)>,
}

impl ShardEngine {
    /// The spill loop: triggers and deadlines (a required task on the
    /// spill arm).
    pub(super) async fn spill_loop(self: Arc<Self>, sp: Arc<SpillState>) {
        let mut closed = self.close_tx.subscribe();
        // M4: the seed retries until it succeeds (the absorber's startup
        // dirty scan does), and a stream it finds unabsorbed starts the
        // age clock: after a restart nothing else would (no durable group
        // notes it), and a quiet shard would never seal it.
        let mut seeded = false;
        let mut seed_wait = Duration::from_millis(50);
        let mut seed_at = Instant::now();
        self.spill_reset_baseline(&sp);
        loop {
            if !seeded && Instant::now() >= seed_at {
                match self.spill_seed_dirty().await {
                    Ok(unabsorbed) => {
                        seeded = true;
                        if unabsorbed {
                            sp.note_unabsorbed();
                        }
                    }
                    Err(e) => {
                        sp.stats.seed_failures.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(shard = %self.prefix, "L6 spill: dirty seed failed: {e}");
                        seed_at = Instant::now() + seed_wait;
                        seed_wait = (seed_wait * 2).min(Duration::from_secs(5));
                    }
                }
            }
            let settings = sp.current();
            tokio::select! {
                _ = closed.changed() => {
                    if *closed.borrow() {
                        return;
                    }
                }
                () = tokio::time::sleep(settings.tick) => {}
                () = sp.wake.notified() => {}
            }
            if self.is_closed() {
                return;
            }
            let settings = sp.current();
            if !settings.auto {
                continue;
            }
            let m = self.spill_measure(&sp);
            sp.stats.mem_estimate_last.store(m.mem_estimate, Ordering::Relaxed);
            sp.stats.wal_objects_last.store(m.wal_objects, Ordering::Relaxed);
            if let Some(kind) = m.deadline(&settings) {
                if sp.running.load(Ordering::Acquire) {
                    sp.stats.deadline_preempted_seal.fetch_add(1, Ordering::Relaxed);
                }
                self.spill_deadline_flush(&sp, kind).await;
                continue;
            }
            #[cfg(test)]
            if std::env::var("DST_L6_TRACE").is_ok_and(|v| v == "ticks") {
                eprintln!(
                    "L6TRACE {} {:?} running={} backoff={} m={m:?}",
                    self.prefix,
                    std::time::SystemTime::now(),
                    sp.running.load(Ordering::Acquire),
                    sp.backing_off()
                );
            }
            if sp.running.load(Ordering::Acquire) || sp.backing_off() {
                continue;
            }
            if let Some(trigger) = m.trigger(&settings) {
                #[cfg(test)]
                if std::env::var("DST_L6_TRACE").is_ok() {
                    eprintln!("L6TRACE {} seal start {trigger:?}", self.prefix);
                }
                sp.running.store(true, Ordering::Release);
                let engine = self.clone();
                let state = sp.clone();
                tokio::spawn(async move {
                    let result = engine.spill_once(trigger).await;
                    #[cfg(test)]
                    if std::env::var("DST_L6_TRACE").is_ok() {
                        eprintln!(
                            "L6TRACE {} {:?} seal {trigger:?} -> {:?}",
                            engine.prefix,
                            std::time::SystemTime::now(),
                            result.as_ref().map(|r| (r.pages, r.total_ms))
                        );
                    }
                    state.after_seal(result.is_ok());
                    // M5: one line per seal, so the census has every seal's
                    // durations, sizes and leak (the histograms only bucket).
                    if let Ok(r) = &result {
                        tracing::info!(
                            shard = %engine.prefix,
                            trigger = trigger.name(),
                            mem_estimate = m.mem_estimate,
                            unabsorbed = m.pages,
                            wal_objects = m.wal_objects,
                            object_bytes = r.object_bytes,
                            streams = r.streams,
                            chunks = r.chunks,
                            pages = r.pages,
                            page_bytes = r.page_bytes,
                            groups = r.groups,
                            ring_fallbacks = r.ring_fallbacks,
                            l0_bytes = r.l0_bytes,
                            imm_flushes = r.imm_flushes,
                            leak_bytes = r.leak_bytes,
                            wal_ms = r.wal_ms,
                            put_ms = r.put_ms,
                            commit_ms = r.commit_ms,
                            flush_ms = r.flush_ms,
                            steps_ms = r.steps_ms,
                            total_ms = r.total_ms,
                            "L6SEAL"
                        );
                    }
                    if let Err(e) = result
                        && !engine.is_closed()
                    {
                        tracing::warn!(shard = %engine.prefix, ?trigger, "L6 seal failed: {e}");
                    }
                    state.running.store(false, Ordering::Release);
                    state.wake();
                });
            }
        }
    }

    /// Make every stream the durable dirty index names resident, so seals
    /// see it (idle eviction keeps a handle with unabsorbed pages). Returns
    /// whether any had unabsorbed pages.
    async fn spill_seed_dirty(&self) -> anyhow::Result<bool> {
        let mut after = None;
        let mut unabsorbed = false;
        loop {
            let (page, more) = self.scan_dirty_streams_page(after, 1024).await?;
            for (hash, absorbed, next) in &page {
                if absorbed < next {
                    self.stream_handle(*hash).await?;
                    unabsorbed = true;
                }
            }
            after = page.last().map(|e| e.0);
            if !more || after.is_none() {
                return Ok(unabsorbed);
            }
        }
    }

    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    fn spill_reset_baseline(&self, sp: &SpillState) {
        *sp.baseline.lock().unwrap() = Baseline {
            write_ops: sp.counter(db_stats::WRITE_OPS),
            kv_bytes: sp.counter(db_stats::MEMTABLE_WRITE_BYTES),
            wal_objects: sp.counter(slatedb::wal_buffer_stats::WAL_BUFFER_FLUSHES),
            pump_flushes: self.pump_flushes.load(Ordering::Relaxed),
            at: Instant::now(),
        };
    }

    /// The trigger dimensions now. Without SlateDB's metrics (engines a
    /// test builds without a recorder) the memtable dimension falls back to
    /// page bytes and the WAL objects to the pump's flushes.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    pub(crate) fn spill_measure(&self, sp: &SpillState) -> Measure {
        let b = *sp.baseline.lock().unwrap();
        let pages = self.maintenance_snapshot().unabsorbed_frame_bytes;
        let (mem_estimate, wal_objects) = if sp.metrics.is_some() {
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
            (pages, pump.saturating_sub(b.pump_flushes))
        };
        let oldest = *sp.oldest_unabsorbed.lock().unwrap();
        let ring = self.ring_share.as_ref().is_some_and(|share| {
            let refused = share.refused.load(Ordering::Relaxed);
            pages > 0
                && (refused > sp.ring_refused_at_seal.load(Ordering::Relaxed)
                    || pages * 100 >= share.share_bytes().saturating_mul(sp.current().ring_pct))
        });
        Measure {
            mem_estimate,
            pages,
            wal_objects,
            oldest_age: oldest.map(|t| t.elapsed()),
            memtable_age: oldest.map(|t| t.max(b.at).elapsed()),
            ring,
        }
    }

    /// One forced memtable flush, attributed by the delta of SlateDB's
    /// `immutable_memtable_flushes` around it (review A1): at most one per
    /// call is ours; any excess is an own freeze the call carried out.
    /// Returns (attributed, l0 bytes, error).
    async fn spill_forced_flush(&self, sp: &SpillState) -> (u64, u64, Option<String>) {
        let _one_at_a_time = sp.flush_lock.lock().await;
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} forced flush: lock held, l0 {}", self.prefix, self.db.manifest().l0().len());
        }
        let measure = self.spill_measure(sp);
        sp.stats
            .mem_estimate_at_flush_max
            .fetch_max(measure.mem_estimate, Ordering::Relaxed);
        sp.stats
            .wal_objects_at_flush_max
            .fetch_max(measure.wal_objects, Ordering::Relaxed);
        let before = sp.counter(db_stats::IMMUTABLE_MEMTABLE_FLUSHES);
        let l0_before = sp.counter(db_stats::L0_FLUSH_BYTES);
        self.spill_reset_baseline(sp);
        let flushed = self
            .db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await;
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} forced flush: done {:?}", self.prefix, flushed.as_ref().err());
        }
        let delta = sp
            .counter(db_stats::IMMUTABLE_MEMTABLE_FLUSHES)
            .saturating_sub(before);
        if delta > 1 {
            sp.stats
                .forced_flush_excess
                .fetch_add(delta - 1, Ordering::Relaxed);
        }
        let l0 = sp.counter(db_stats::L0_FLUSH_BYTES).saturating_sub(l0_before);
        (delta.min(1), l0, flushed.err().map(|e| e.to_string()))
    }

    /// A deadline: flush the memtable, seal or no seal (degraded mode).
    pub(crate) async fn spill_deadline_flush(&self, sp: &SpillState, kind: Deadline) {
        sp.stats.deadline(kind);
        let m = self.spill_measure(sp);
        let (attributed, l0, err) = self.spill_forced_flush(sp).await;
        tracing::info!(
            shard = %self.prefix,
            ?kind,
            mem_estimate = m.mem_estimate,
            unabsorbed = m.pages,
            wal_objects = m.wal_objects,
            seal_running = sp.running.load(Ordering::Acquire),
            attributed,
            l0_bytes = l0,
            "L6DEADLINE"
        );
        sp.stats
            .deadline_flush_attributed
            .fetch_add(attributed, Ordering::Relaxed);
        if let Some(e) = err {
            sp.stats.deadline_flush_errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(shard = %self.prefix, ?kind, "L6 deadline flush failed: {e}");
        }
    }

    /// One seal of this shard (steps 1-7 above), serialized per shard.
    pub(crate) async fn spill_once(self: &Arc<Self>, trigger: Trigger) -> Result<SealReport, SealError> {
        let sp = self.spill.clone().ok_or(SealError::Closed)?;
        let _serial = sp.serial.lock().await;
        sp.stats.started(trigger);
        let t0 = Instant::now();
        let result = self.spill_steps(&sp, t0).await;
        match &result {
            Ok(report) => {
                sp.stats.seals_completed.fetch_add(1, Ordering::Relaxed);
                sp.stats.seal_ms(report.total_ms);
            }
            Err(SealError::Aborted(_)) => {
                sp.stats.seal_aborts.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                sp.stats.seal_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    async fn spill_steps(self: &Arc<Self>, sp: &Arc<SpillState>, t0: Instant) -> Result<SealReport, SealError> {
        // Every advance an earlier seal submitted has settled (durable, or
        // refused), so this seal's windows start at durable boundaries.
        self.spill_wait_settled(sp).await?;
        if let Some(share) = &self.ring_share {
            sp.ring_refused_at_seal
                .store(share.refused.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} step 1", self.prefix);
        }
        // (1)
        let t_steps = Instant::now();
        self.spill_flush_and_catch_up(sp).await?;
        let wal_ms = ms(t_steps.elapsed());
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} step 2", self.prefix);
        }
        sp.check_abort(SealStep::AfterWalFlush)?;
        // (2)
        let snapshot_at = Instant::now();
        let mut report = SealReport {
            wal_ms,
            ..SealReport::default()
        };
        let windows = self.spill_snapshot(sp, &mut report).await?;
        if windows.is_empty() {
            sp.stats.seals_empty.fetch_add(1, Ordering::Relaxed);
        } else {
            self.spill_seal_windows(sp, windows, &mut report).await?;
        }
        // (7a) The commit is applied; make it durable before the freeze.
        let t_flush = Instant::now();
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} step 7a", self.prefix);
        }
        self.spill_make_durable().await?;
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} step 7b", self.prefix);
        }
        #[cfg(test)]
        if *sp.abort_at.lock().unwrap() == Some(SealStep::AfterDurable) {
            self.spill_wait_settled(sp).await?;
        }
        sp.check_abort(SealStep::AfterDurable)?;
        // (7b) The L0 keeps the tombstones, the index rows, the newest
        // metadata, and the values committed after the snapshot (the leak).
        report.leak_bytes = self.maintenance_snapshot().unabsorbed_frame_bytes;
        sp.stats.spill_flushes.fetch_add(1, Ordering::Relaxed);
        let (attributed, l0, err) = self.spill_forced_flush(sp).await;
        report.imm_flushes = attributed;
        report.l0_bytes = l0;
        report.flush_ms = ms(t_flush.elapsed());
        report.steps_ms = ms(t_steps.elapsed());
        sp.stats.flush_ms(report.flush_ms);
        sp.stats
            .spill_flush_attributed
            .fetch_add(attributed, Ordering::Relaxed);
        sp.stats.spill_l0_bytes.fetch_add(l0, Ordering::Relaxed);
        sp.stats
            .leak_bytes
            .fetch_add(report.leak_bytes, Ordering::Relaxed);
        if let Some(e) = err {
            sp.stats.spill_flush_errors.fetch_add(1, Ordering::Relaxed);
            return Err(SealError::Store(e));
        }
        #[cfg(test)]
        if std::env::var("DST_L6_TRACE").is_ok() {
            eprintln!("L6TRACE {} final settle", self.prefix);
        }
        // The advances settle once durable; check they all landed.
        self.spill_wait_settled(sp).await?;
        self.spill_note_leftover(sp, snapshot_at);
        report.total_ms = ms(t0.elapsed());
        Ok(report)
    }

    /// Step 1: one awaited WAL flush, then a wait (at most 1 s) until durable
    /// dispatch has published every group the flush covered, so each
    /// stream's durable frontier meets its applied one when step 2 reads it
    /// (dB §1.2 step 4: "so that the applied and durable frontiers meet").
    /// Without the wait the snapshot misses whatever the acker has not
    /// dispatched yet, and those pages' values reach this seal's L0 (leak).
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    async fn spill_flush_and_catch_up(&self, sp: &SpillState) -> Result<(), SealError> {
        let target = self.in_flight.lock().unwrap().pending().last().map(|g| g.seq);
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .map_err(|e| SealError::Store(e.to_string()))?;
        let Some(target) = target else {
            return Ok(());
        };
        let until = Instant::now() + Duration::from_secs(1);
        loop {
            let behind = self
                .in_flight
                .lock()
                .unwrap()
                .pending()
                .first()
                .is_some_and(|g| g.seq <= target);
            if !behind {
                return Ok(());
            }
            if self.is_closed() {
                return Err(SealError::Closed);
            }
            if Instant::now() >= until {
                sp.stats.catch_up_timeouts.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Step 7a: one awaited WAL flush, so SlateDB's durable seq covers the
    /// seal's commit before step 7b freezes the memtable.
    async fn spill_make_durable(&self) -> Result<(), SealError> {
        self.db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::Wal,
            })
            .await
            .map_err(|e| SealError::Store(e.to_string()))
    }

    /// Steps 3-6 for a non-empty snapshot.
    async fn spill_seal_windows(
        self: &Arc<Self>,
        sp: &Arc<SpillState>,
        windows: Vec<Window>,
        report: &mut SealReport,
    ) -> Result<(), SealError> {
        // (3) One chunk per run of at most `max_pages_per_chunk` pages of a
        // stream; `rank` is the chunk's place in its stream.
        let max_pages = sp.settings.max_pages_per_chunk.max(1);
        let mut inputs = Vec::new();
        let mut placed: Vec<(usize, usize)> = Vec::new(); // (window, rank)
        for (w, window) in windows.iter().enumerate() {
            for (rank, run) in window.pages.chunks(max_pages).enumerate() {
                inputs.push(ChunkInput {
                    hash: window.hash,
                    first: run[0].0,
                    max_ts: run.iter().map(|p| p.3).max().unwrap_or(0),
                    pages: run.iter().map(|p| (p.1, p.2.clone())).collect(),
                });
                placed.push((w, rank));
            }
        }
        let encoded = object::encode(&inputs).map_err(SealError::Encode)?;
        let id = ObjectId {
            epoch: self.writer_epoch,
            seq: sp.next_seq.fetch_add(1, Ordering::Relaxed),
        };
        // (4)
        let path = object::object_path(&crate::sharddir::shard_db_path(&self.prefix), id);
        let t_put = Instant::now();
        #[cfg(test)]
        {
            let delay = *sp.put_delay.lock().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if sp.fail_puts.load(Ordering::Relaxed) {
                return Err(SealError::Store("injected PUT failure".into()));
            }
        }
        self.data_store
            .put_opts(
                &object_store::path::Path::from(path),
                encoded.payload(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| SealError::Store(e.to_string()))?;
        report.put_ms = ms(t_put.elapsed());
        report.object = Some(id);
        report.object_bytes = encoded.len as u64;
        report.streams = windows.len();
        report.chunks = inputs.len();
        sp.stats.put_ms(report.put_ms);
        sp.stats.objects_put.fetch_add(1, Ordering::Relaxed);
        sp.stats
            .object_bytes
            .fetch_add(encoded.len as u64, Ordering::Relaxed);
        sp.stats
            .chunks
            .fetch_add(inputs.len() as u64, Ordering::Relaxed);
        sp.check_abort(SealStep::AfterPut)?;
        #[cfg(test)]
        {
            let hold = sp.before_commit.lock().unwrap().clone();
            if let Some(hold) = hold {
                hold.entered.fetch_add(1, Ordering::AcqRel);
                let mut released = hold.release.subscribe();
                let _ = released.wait_for(|r| *r).await;
            }
        }
        // (5) Descriptors, grouped by rank, at most `max_streams_per_group`
        // streams a group.
        let mut by_rank: Vec<Vec<(Arc<StreamHandle>, Arc<ChunkDescriptor>, u64)>> = Vec::new();
        for ((input, chunk), (w, rank)) in inputs.iter().zip(&encoded.chunks).zip(&placed) {
            let row = IndexRow::for_chunk(id, chunk);
            let value = row
                .encode()
                .map_err(|e| SealError::Refused(format!("index row: {e:?}")))?;
            let bytes: u64 = input.pages.iter().map(|(_, p)| p.len() as u64).sum();
            sp.stats.chunk(u64::from(chunk.entry.len));
            report.pages += input.pages.len() as u64;
            report.page_bytes += bytes;
            let desc = Arc::new(ChunkDescriptor {
                hash: input.hash,
                from: chunk.entry.first,
                upto: chunk.entry.last + 1,
                index_key: index_key(&input.hash, chunk.entry.last),
                index_value: value,
                page_lasts: input.pages.iter().map(|(last, _)| *last).collect(),
                outcome: AtomicU8::new(OUTCOME_PENDING),
            });
            if by_rank.len() <= *rank {
                by_rank.resize_with(rank + 1, Vec::new);
            }
            by_rank[*rank].push((windows[*w].handle.clone(), desc, bytes));
        }
        sp.stats
            .pages_sealed
            .fetch_add(report.pages, Ordering::Relaxed);
        sp.stats
            .page_bytes_sealed
            .fetch_add(report.page_bytes, Ordering::Relaxed);
        let t_commit = Instant::now();
        for rank in by_rank {
            for group in rank.chunks(sp.settings.max_streams_per_group.max(1)) {
                self.spill_submit_group(sp, group).await?;
                report.groups += 1;
            }
        }
        report.commit_ms = ms(t_commit.elapsed());
        sp.stats.commit_ms(report.commit_ms);
        sp.check_abort(SealStep::AfterApplied)?;
        Ok(())
    }

    /// Submit one group of advances and wait until the committer applied
    /// it (step 6). Durability comes with step 7a.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    async fn spill_submit_group(
        &self,
        sp: &Arc<SpillState>,
        group: &[(Arc<StreamHandle>, Arc<ChunkDescriptor>, u64)],
    ) -> Result<(), SealError> {
        {
            let mut side = sp.side.lock().unwrap();
            for (_, d, _) in group {
                side.insert((d.hash, d.from), d.clone());
            }
        }
        let tombstones: u64 = group.iter().map(|(_, d, _)| d.page_lasts.len() as u64).sum();
        sp.stats.groups.fetch_add(1, Ordering::Relaxed);
        sp.stats
            .streams_per_group_max
            .fetch_max(group.len() as u64, Ordering::Relaxed);
        sp.stats
            .tombstones_per_group_max
            .fetch_max(tombstones, Ordering::Relaxed);
        let advances = group
            .iter()
            .map(|(_, d, bytes)| {
                let receipt = sp.submissions.submit(&d.hash);
                (d.hash, d.upto, CopiedBytes::new(d.from, *bytes).receipted(receipt))
            })
            .collect();
        sp.unsettled
            .lock()
            .unwrap()
            .extend(group.iter().map(|(_, d, _)| d.hash));
        self.submit_absorbed_batch_v2(advances).await;
        // Applied: the committer staged every advance and, for an Exact
        // one, published its boundary; a refused group settles instead.
        loop {
            let done = group.iter().all(|(h, d, _)| {
                let o = d.outcome();
                (o != OUTCOME_PENDING
                    && (o != OUTCOME_EXACT || h.state.lock().unwrap().applied.absorbed >= d.upto))
                    || sp.submissions.settled(&d.hash)
            });
            if done {
                break;
            }
            if self.is_closed() {
                return Err(SealError::Closed);
            }
            #[cfg(test)]
            if std::env::var("DST_L6_TRACE").is_ok() && rand::random::<u16>() < 256 {
                let outcomes: Vec<_> = group
                    .iter()
                    .map(|(h, d, _)| (d.outcome(), h.state.lock().unwrap().applied.absorbed, d.upto, sp.submissions.settled(&d.hash)))
                    .collect();
                eprintln!("L6TRACE {} submit_group waiting: {outcomes:?}", self.prefix);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for (h, d, _) in group {
            if d.outcome() != OUTCOME_EXACT || h.state.lock().unwrap().applied.absorbed < d.upto {
                // Drop whatever this seal left in the side table.
                let mut side = sp.side.lock().unwrap();
                for (_, d, _) in group {
                    side.remove(&(d.hash, d.from));
                }
                return Err(SealError::Refused(format!(
                    "advance of {} to {} was not applied (outcome {})",
                    crate::crypto::hex(&d.hash[..4]),
                    d.upto,
                    d.outcome()
                )));
            }
        }
        Ok(())
    }

    /// Wait until every advance submitted so far settled (durable dispatch
    /// or refusal).
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    async fn spill_wait_settled(&self, sp: &SpillState) -> Result<(), SealError> {
        loop {
            let pending = {
                let mut u = sp.unsettled.lock().unwrap();
                u.retain(|h| !sp.submissions.settled(h));
                u.len()
            };
            if pending == 0 {
                return Ok(());
            }
            if self.is_closed() {
                return Err(SealError::Closed);
            }
            #[cfg(test)]
            if std::env::var("DST_L6_TRACE").is_ok() && rand::random::<u16>() < 256 {
                eprintln!("L6TRACE {} wait_settled: {pending} unsettled", self.prefix);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Step 2: each stream's `[absorbed, durable.next)`, from the ring or a
    /// per-stream seek.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    async fn spill_snapshot(&self, sp: &SpillState, report: &mut SealReport) -> Result<Vec<Window>, SealError> {
        let handles: Vec<Arc<StreamHandle>> = self.streams.lock().unwrap().values().cloned().collect();
        let mut out = Vec::new();
        for handle in handles {
            let (from, upto) = {
                let st = handle.state.lock().unwrap();
                (st.durable.absorbed, st.durable.next)
            };
            if from >= upto {
                continue;
            }
            let pages = match ring_window(&handle, from, upto) {
                Some(pages) => pages,
                None => {
                    let pages = self.spill_seek(&handle, from, upto).await?;
                    report.ring_fallbacks += 1;
                    sp.stats.ring_fallbacks.fetch_add(1, Ordering::Relaxed);
                    sp.stats.ring_fallback_bytes.fetch_add(
                        pages.iter().map(|p| p.2.len() as u64).sum(),
                        Ordering::Relaxed,
                    );
                    pages
                }
            };
            if pages.is_empty() {
                continue;
            }
            out.push(Window {
                hash: handle.hash,
                handle,
                from,
                pages,
            });
        }
        Ok(out)
    }

    /// The fallback: the stream's pages in `[from, upto)` from the shard DB
    /// (a ring hit when the ring covers it after all). The window's ends
    /// are page edges, so every slice is a whole page; the pages must start
    /// at `from` and be dense.
    async fn spill_seek(
        &self,
        handle: &Arc<StreamHandle>,
        from: u64,
        upto: u64,
    ) -> Result<Vec<(u64, u64, Bytes, i64)>, SealError> {
        let read = read_frames_range(self, handle, from, upto, usize::MAX)
            .await
            .map_err(|e| SealError::Store(format!("{e:?}")))?;
        let mut pages = Vec::with_capacity(read.frames.len());
        let mut expected = from;
        for slice in read.frames.iter() {
            let page = slice.page();
            if slice.first() != page.first() || slice.last() != page.last() || page.first() != expected {
                return Err(SealError::Refused(format!(
                    "seek of {} from {from}: page [{}, {}] is not the next whole page",
                    crate::crypto::hex(&handle.hash[..4]),
                    page.first(),
                    page.last()
                )));
            }
            expected = page.last() + 1;
            pages.push((page.first(), page.last(), page.raw().clone(), page.ts_ms()));
        }
        Ok(pages)
    }

    /// After a seal: the shard's oldest unabsorbed page is at most as old as
    /// the snapshot if any stream is still unabsorbed.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    fn spill_note_leftover(&self, sp: &SpillState, snapshot_at: Instant) {
        *sp.oldest_unabsorbed.lock().unwrap() = None;
        let leftover = self.streams.lock().unwrap().values().any(|h| {
            let st = h.state.lock().unwrap();
            st.durable.absorbed < st.durable.next
        });
        if leftover {
            *sp.oldest_unabsorbed.lock().unwrap() = Some(snapshot_at);
        }
    }

    /// The committer's lookup: the descriptor of the chunk an advance of
    /// `hash` from `from` to `upto` carries (spill arm), taken out of the
    /// side table.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    pub(super) fn spill_take_descriptor(&self, hash: [u8; 16], from: u64, upto: u64) -> Option<Arc<ChunkDescriptor>> {
        let sp = self.spill.as_ref()?;
        let mut side = sp.side.lock().unwrap();
        match side.get(&(hash, from)) {
            Some(d) if d.upto == upto => side.remove(&(hash, from)),
            _ => None,
        }
    }

    pub(super) fn spill_stats(&self) -> Option<&SpillStats> {
        self.spill.as_ref().map(|sp| &*sp.stats)
    }

    /// M4: a merged read adopted a newer boundary after its tail scan met
    /// the hole a seal's trims left (spill arm only).
    pub(crate) fn spill_note_race_adoption(&self) {
        if let Some(s) = self.spill_stats() {
            s.reads.race_adoptions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// M4 (the DST fixture honours `L6_SPILL=1`): a fixture that starts an
    /// absorber on a spill-arm engine gets the sealer at the absorber's
    /// cadence instead: seal when the shard's unabsorbed page bytes reach
    /// `threshold_bytes` or its oldest unabsorbed page is `threshold_age`
    /// old, evaluated every `tick` and after each durable group, with no
    /// WAL-object trigger (an absorber has none) and a backoff of one tick
    /// to 1 s. A cold absorber (thresholds never reached) is a sealer that
    /// never seals. The deadlines and the ring trigger stay. Returns false
    /// on the base arm, where the caller starts the absorber.
    pub(crate) fn spill_adopt_absorber_cadence(
        &self,
        threshold_bytes: u64,
        threshold_age: Duration,
        tick: Duration,
    ) -> bool {
        let Some(sp) = &self.spill else {
            return false;
        };
        let base = sp.settings;
        let tick = tick.max(Duration::from_millis(1));
        let cadence = SpillSettings {
            mode: crate::spill::settings::TriggerMode::Pages,
            memtable_bytes: threshold_bytes.max(1),
            wal_objects_seal: u64::MAX,
            age_seal: threshold_age,
            age_deadline: base.age_deadline.max(threshold_age),
            tick,
            backoff_min: tick,
            backoff_max: Duration::from_secs(1).max(tick),
            auto: true,
            ..base
        };
        #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
        {
            *sp.cadence.lock().unwrap() = Some(cadence);
        }
        sp.stats.absorber_cadences.fetch_add(1, Ordering::Relaxed);
        sp.wake();
        true
    }

    /// The task a test fixture's `Absorber::start` returns on the spill arm:
    /// it holds the absorber cadence until aborted (the sealer then stops,
    /// as an aborted absorber stops absorbing) or the engine closes.
    pub(crate) fn spill_cadence_task(self: &Arc<Self>) -> Option<impl std::future::Future<Output = ()> + Send + 'static> {
        let sp = self.spill.clone()?;
        let mut closed = self.close_tx.subscribe();
        Some(async move {
            let _lease = CadenceLease(sp);
            while !*closed.borrow() {
                if closed.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}

/// Dropped with an aborted fixture absorber: the sealer stops sealing.
struct CadenceLease(Arc<SpillState>);

impl Drop for CadenceLease {
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
    fn drop(&mut self) {
        let sp = &self.0;
        let stopped = SpillSettings {
            auto: false,
            ..sp.current()
        };
        *sp.cadence.lock().unwrap() = Some(stopped);
        sp.wake();
    }
}

/// The ring's pages of `[from, upto)` as (first, last, page, ts), or None
/// when the ring does not hold them densely from `from` (refused batches,
/// a restart) or a page fails admission.
#[expect(clippy::unwrap_used, reason = "L6 spike: throwaway sealer")]
fn ring_window(handle: &StreamHandle, from: u64, upto: u64) -> Option<Vec<(u64, u64, Bytes, i64)>> {
    let ring = handle.ring.lock().unwrap();
    let mut pages = Vec::new();
    let mut expected = from;
    for batch in ring.batches.iter() {
        let mut first = batch.first;
        for (last, raw) in &batch.frames {
            let page_first = first;
            first = last + 1;
            if *last < from {
                continue;
            }
            if page_first >= upto {
                break;
            }
            if page_first != expected || *last >= upto {
                return None;
            }
            let ts = CheckedPage::admit(raw.clone(), *last).ok()?.ts_ms();
            pages.push((page_first, *last, raw.clone(), ts));
            expected = last + 1;
        }
    }
    (expected == upto).then_some(pages)
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
