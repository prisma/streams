//! A live queue's trim (S3b-DESIGN §4.3, §7.3): the gc retires the sealed
//! chunks of a queue that nothing references any more.
//!
//! **Why no read of queue state has to be fresh.** A queue's needed set
//! `N = {o < frontier : o referenced} ∪ [frontier, ∞)` only shrinks: the
//! frontier only moves on, a reference (an open delivery, an unresolved
//! scheduled message) is only ever created at or past the frontier, and a
//! resolved one never comes back. So any durable reading of `N` covers
//! every later `N`, and the gc may retire anything outside a durable
//! reading, with no lock against the committer, as long as it reads the
//! frontier first and the references after. This is the keystone of the
//! design.
//!
//! The pass (part 2 step 3) and the reads the cleaner makes through it
//! (`intervals`, `chunk_horizon`) live here; the rows it keeps are the
//! purge head's Trimming state (`spill::gc::HeadState::Trimming`) and the
//! queue's released intervals (`native_queue::intervals`).
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use slatedb::WriteBatch;

use super::GcError;
use crate::native_queue::hints::{self, HintRanges};
use crate::native_queue::intervals::{self, Interval, Released};
use crate::native_queue::keys;
use crate::native_queue::range::{RangeRow, SHIFT, SPAN};
use crate::native_queue::rows::StateRow;
use crate::shard::ShardEngine;
use crate::spill::directory::DirEntry;
use crate::spill::gc::{self as sgc, Decrements, Head, HeadState};

/// What one trim pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TrimReport {
    /// Hint rows consumed.
    pub(crate) hints: u64,
    /// Chunks examined for retirement.
    pub(crate) examined: u64,
    /// Chunks retired and decremented.
    pub(crate) released: u64,
}

/// A pass's staged work: its one batch and what goes in it.
struct Pass {
    batch: WriteBatch,
    decrements: Decrements,
    report: TrimReport,
    /// A Trimming row was written: folds read heads from now on.
    heads: bool,
    /// The chunks it may examine.
    chunks: u64,
}

/// The hints a pass consumes for one queue: their keys and their ranges.
type QueueHints = BTreeMap<[u8; 16], (Vec<Vec<u8>>, Vec<(u64, u64)>)>;

fn remote_read() -> slatedb::config::ReadOptions {
    slatedb::config::ReadOptions {
        durability_filter: slatedb::config::DurabilityLevel::Remote,
        ..Default::default()
    }
}

fn remote_scan() -> slatedb::config::ScanOptions {
    slatedb::config::ScanOptions {
        durability_filter: slatedb::config::DurabilityLevel::Remote,
        ..crate::native_queue::scan_options()
    }
}

fn store(e: impl std::fmt::Display) -> GcError {
    GcError::Store(e.to_string())
}

impl ShardEngine {
    /// Seed the shard's hint sequence from its log, once per engine: above
    /// the last hint key and the trim's cursor (`hints::seed`). The
    /// committer and the trim draw from it; the first seed stands.
    pub(crate) async fn seed_trim_seq(&self) -> Result<(), String> {
        let seq = &self.layout.trim_seq;
        if seq.load(Ordering::Acquire) != 0 {
            return Ok(());
        }
        let (from, to) = hints::key_range();
        let descending = slatedb::config::ScanOptions {
            order: slatedb::IterationOrder::Descending,
            ..crate::native_queue::scan_options()
        };
        let mut last = self
            .db
            .scan_with_options(from..to, &descending)
            .await
            .map_err(|e| e.to_string())?;
        let last = last.next().await.map_err(|e| e.to_string())?;
        let cursor = self
            .db
            .get(hints::cursor_key())
            .await
            .map_err(|e| e.to_string())?;
        let seed = hints::seed(last.as_ref().map(|kv| &kv.key[..]), cursor.as_deref())?;
        // Another seeder won already: its seed stands.
        if seq
            .compare_exchange(0, seed, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::trace!(shard = %self.prefix, "trim sequence already seeded");
        }
        Ok(())
    }

    /// The next hint key's sequence (after `seed_trim_seq`).
    pub(crate) fn next_trim_seq(&self) -> u64 {
        self.layout.trim_seq.fetch_add(1, Ordering::AcqRel)
    }

    /// One trim pass (S3b-DESIGN §7.3), under `refs`: the hints after the
    /// cursor, at most `trim_hints`, grouped by queue; for each queue its
    /// chunks the hints name (and the first one at its prefix), at most
    /// `trim_chunks` in all (both 4,096 by default), each retired when it lies behind the frontier
    /// and holds no reference; then one batch through `write_index_batch`
    /// (the gate shared around it alone): the decrements, each queue's
    /// Trimming row and interval changes, the consumed hints deleted, the
    /// chunks left unexamined hinted again, and the cursor. Everything it
    /// decides is read from durable rows, so a batch a split waited out and
    /// lost is redone by the children from their own rows.
    pub(crate) async fn trim_pass(&self) -> Result<TrimReport, GcError> {
        let gc = self.gc_state();
        let _refs = gc.refs.lock().await;
        let (queues, last) = self.trim_hints().await?;
        let Some(last) = last else {
            return Ok(TrimReport::default());
        };
        self.seed_trim_seq().await.map_err(store)?;
        let mut pass = Pass {
            batch: WriteBatch::new(),
            decrements: Decrements::new(),
            report: TrimReport::default(),
            heads: false,
            chunks: gc.settings().trim_chunks.max(1) as u64,
        };
        for (hash, (consumed, ranges)) in queues {
            pass.report.hints += consumed.len() as u64;
            for key in &consumed {
                pass.batch.delete(key);
            }
            self.trim_queue(hash, &ranges, &mut pass).await?;
        }
        pass.batch.put(hints::cursor_key(), &last);
        let zeroed = self
            .stage_decrements(&mut pass.batch, &pass.decrements)
            .await?;
        self.write_decrements(pass.batch, &zeroed).await?;
        if pass.heads {
            gc.signal_heads();
        }
        Ok(pass.report)
    }

    /// The hint rows after the cursor, at most `trim_hints`, by queue, and
    /// the last key read (`None`: no hint). A row that does not decode is
    /// counted and consumed: losing a hint leaks chunks, never a body.
    async fn trim_hints(&self) -> Result<(QueueHints, Option<Vec<u8>>), GcError> {
        let (mut from, to) = hints::key_range();
        if let Some(cursor) = self.db.get(hints::cursor_key()).await.map_err(store)? {
            from = [&cursor[..], &[0]].concat();
        }
        let mut rows = self
            .db
            .scan_with_options(from..to, &remote_scan())
            .await
            .map_err(store)?;
        let (mut out, mut last) = (QueueHints::new(), None);
        let (mut n, most) = (0, self.gc_state().settings().trim_hints.max(1));
        while n < most
            && let Some(kv) = rows.next().await.map_err(store)?
        {
            n += 1;
            last = Some(kv.key.to_vec());
            let Some((_, hash)) = hints::parse_key(&kv.key) else {
                self.note_corrupt_index_row("trim hint", &kv.key);
                continue;
            };
            let slot = out.entry(hash).or_default();
            slot.0.push(kv.key.to_vec());
            match hints::decode(&kv.value) {
                Ok(ranges) => slot.1.extend(ranges),
                Err(_) => self.note_corrupt_index_row("trim hint", &kv.key),
            }
        }
        Ok((out, last))
    }

    /// One queue's part of a pass: its frontier first, then its references
    /// chunk by chunk (§4.3); its rows staged into the pass.
    async fn trim_queue(
        &self,
        hash: [u8; 16],
        ranges: &[(u64, u64)],
        pass: &mut Pass,
    ) -> Result<(), GcError> {
        let state = self
            .db
            .get_with_options(keys::state(&hash), &remote_read())
            .await
            .map_err(store)?;
        let Some(state) = state
            .as_deref()
            .map(StateRow::decode)
            .transpose()
            .map_err(store)?
        else {
            return Ok(());
        };
        if state.deleting {
            return Ok(());
        }
        let frontier = state.frontier;
        let before = match self.head_row(&hash).await? {
            None => Released::new(0, &[]),
            Some(h) if h.state == HeadState::Trimming => {
                Released::new(h.reclaimed, &self.intervals(&hash).await.map_err(store)?)
            }
            // The purge owns a deleted queue's chunks.
            Some(_) => return Ok(()),
        };
        let mut released = before.clone();
        let mut again = HintRanges::default();
        for c in self.trim_candidates(hash, ranges, &before).await? {
            if pass.report.examined >= pass.chunks {
                again.add(c.first, c.last.saturating_add(1));
                continue;
            }
            pass.report.examined += 1;
            if c.last < frontier && !self.queue_refs_in(&hash, c.first, c.last).await? {
                sgc::decrement(&mut pass.decrements, &c);
                released.release(c.first, c.last);
                pass.report.released += 1;
            }
        }
        self.stage_trim_rows(hash, (&before, &released, frontier), &again, pass);
        Ok(())
    }

    /// A queue's chunks a pass looks at: those its hints name and the first
    /// one at its prefix, none released, none of a seal in flight (whose
    /// object counts nothing yet), in offset order.
    async fn trim_candidates(
        &self,
        hash: [u8; 16],
        ranges: &[(u64, u64)],
        released: &Released,
    ) -> Result<Vec<DirEntry>, GcError> {
        let in_flight = self.gc_state().in_flight();
        let live = self.live_chunks(hash).await?;
        Ok(live
            .into_iter()
            .filter(|e| e.object.epoch != self.writer_epoch || !in_flight.contains(&e.object.seq))
            .filter(|e| !released.holds(e.first, e.last))
            .filter(|e| {
                e.first == released.reclaimed
                    || ranges.iter().any(|&(a, b)| e.first < b && a <= e.last)
            })
            .collect())
    }

    /// Whether anything still references an offset of `[first, last]`: an
    /// open delivery, or an unresolved scheduled message's bit (§7.3).
    async fn queue_refs_in(&self, hash: &[u8; 16], first: u64, last: u64) -> Result<bool, GcError> {
        let from = keys::by_offset(hash, keys::DELIVERY, first);
        let to = keys::by_offset(hash, keys::DELIVERY, last.saturating_add(1));
        let mut open = self
            .db
            .scan_with_options(from..to, &remote_scan())
            .await
            .map_err(store)?;
        if open.next().await.map_err(store)?.is_some() {
            return Ok(true);
        }
        for index in (first >> SHIFT)..=(last >> SHIFT) {
            let raw = self
                .db
                .get_with_options(keys::by_offset(hash, keys::RANGE, index), &remote_read())
                .await
                .map_err(store)?;
            let Some(raw) = raw else {
                continue;
            };
            let row = RangeRow::decode(&raw).map_err(store)?;
            let span = (index << SHIFT)..(index << SHIFT).saturating_add(SPAN);
            let lo = first.max(span.start);
            let hi = last.min(span.end - 1);
            if (lo..=hi).any(|o| row.sbits.contains(o)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A queue's rows of the pass: its interval changes and Trimming row
    /// when it released anything, and a hint for the chunks it left.
    fn stage_trim_rows(
        &self,
        hash: [u8; 16],
        released: (&Released, &Released, u64),
        again: &HintRanges,
        pass: &mut Pass,
    ) {
        let (before, after, frontier) = released;
        if after != before {
            let (put, delete) = after.changes(before);
            for iv in put {
                pass.batch
                    .put(intervals::key(&hash, iv.last), intervals::encode(&iv));
            }
            for last in delete {
                pass.batch.delete(intervals::key(&hash, last));
            }
            let head = Head::trimming(frontier, after.reclaimed);
            pass.batch
                .put(sgc::head_key(&hash), sgc::encode_head(&head));
            pass.heads = true;
        }
        for row in again.below(u64::MAX).chunks(hints::MAX_RANGES) {
            let key = hints::key(self.next_trim_seq(), &hash);
            pass.batch.put(key, hints::encode(row));
        }
    }

    /// When nothing will hold the chunk `[first, last]` of the queue `hash`
    /// any more (S3b-DESIGN §7.4, the cleaner's horizon, epoch ms): the
    /// latest hard expiry of its open deliveries (retention end plus failure
    /// retention); else, for a chunk held only by scheduled messages, now
    /// plus twice the retention plus the failure retention; `None` when
    /// nothing holds it, or the queue is gone. Its caller is package 6's
    /// cleaner (part 2 step 4); until it lands, only tests call it.
    #[cfg(test)]
    pub(crate) async fn chunk_horizon(
        &self,
        hash: &[u8; 16],
        first: u64,
        last: u64,
    ) -> Result<Option<i64>, String> {
        let raw = self
            .db
            .get_with_options(keys::state(hash), &remote_read())
            .await
            .map_err(|e| e.to_string())?;
        let Some(state) = raw.as_deref().map(StateRow::decode).transpose()? else {
            return Ok(None);
        };
        let (retention, failure) = (state.cfg.retention_ms, state.cfg.failure_retention_ms);
        let from = keys::by_offset(hash, keys::DELIVERY, first);
        let to = keys::by_offset(hash, keys::DELIVERY, last.saturating_add(1));
        let mut rows = self
            .db
            .scan_with_options(from..to, &remote_scan())
            .await
            .map_err(|e| e.to_string())?;
        let mut horizon: Option<i64> = None;
        while let Some(kv) = rows.next().await.map_err(|e| e.to_string())? {
            let row = crate::native_queue::rows::DeliveryRow::decode(&kv.value)?;
            let hard = row.cycle_deadline.saturating_add(failure);
            horizon = Some(horizon.map_or(hard, |h| h.max(hard)));
        }
        if horizon.is_none()
            && self
                .queue_refs_in(hash, first, last)
                .await
                .map_err(|e| e.to_string())?
        {
            let now = crate::shard::now_ms();
            horizon = Some(
                now.saturating_add(retention.saturating_mul(2))
                    .saturating_add(failure),
            );
        }
        Ok(horizon)
    }

    /// A queue's released intervals as the shard log durably holds them
    /// (S3b-DESIGN §7.4: the fold's, the cleaner's and the purge's read).
    pub(crate) async fn intervals(&self, hash: &[u8; 16]) -> Result<Vec<Interval>, String> {
        let (from, to) = intervals::key_range(hash);
        let durable = slatedb::config::ScanOptions {
            durability_filter: slatedb::config::DurabilityLevel::Remote,
            ..crate::native_queue::scan_options()
        };
        let mut rows = self
            .db
            .scan_with_options(from..to, &durable)
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(kv) = rows.next().await.map_err(|e| e.to_string())? {
            out.push(intervals::decode(&kv.key, &kv.value)?);
        }
        Ok(out)
    }
}
