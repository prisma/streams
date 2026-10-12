//! The durable-tail ring: each stream's most recent published batches, held
//! under one engine-wide byte budget so live readers can chase offsets the
//! ring still covers without a canonical scan. A batch holds the group's
//! pages of one stream as (last offset, stored page); a read admits each
//! page it inspects again and slices it to the window.
use super::{FrameReadResult, RingBatch, ShardEngine, StreamHandle, TailRing, record};
use crate::crypto_page::CheckedPage;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// L6 spike (SPIKE-PLAN M2, review B5): the ring budget of a spill-arm
/// process. Every engine's share is `max(floor, min(cap, process ÷
/// shards))`, where `shards` counts the engines registered with the budget:
/// at 64 MiB / 8 MiB a shard keeps 8 MiB up to 8 shards, then the process
/// budget divides, and the floor is the minimum a shard keeps however many
/// shards share the process (the process budget is then overcommitted, by
/// design, and reported). A share is a CAP: one shard never takes the whole
/// process budget, and another shard's load never evicts or refuses its
/// pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RingSettings {
    pub(crate) process_bytes: u64,
    pub(crate) shard_cap_bytes: u64,
    pub(crate) shard_floor_bytes: u64,
}

pub(crate) struct RingBudget {
    settings: RingSettings,
    shards: AtomicU64,
    resident: AtomicU64,
    peak: AtomicU64,
}

impl RingBudget {
    pub(crate) fn new(settings: RingSettings) -> Arc<Self> {
        Arc::new(Self {
            settings,
            shards: AtomicU64::new(0),
            resident: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    pub(crate) fn settings(&self) -> RingSettings {
        self.settings
    }

    /// Each registered engine's share now.
    pub(crate) fn share(&self) -> u64 {
        let s = self.settings;
        let shards = self.shards.load(Ordering::Relaxed).max(1);
        s.shard_cap_bytes
            .min(s.process_bytes / shards)
            .max(s.shard_floor_bytes.min(s.shard_cap_bytes))
    }

    /// Ring bytes resident across every registered engine.
    pub(crate) fn resident(&self) -> u64 {
        self.resident.load(Ordering::Relaxed)
    }

    pub(crate) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    pub(crate) fn shards(&self) -> u64 {
        self.shards.load(Ordering::Relaxed)
    }

    pub(crate) fn register(self: &Arc<Self>) -> RingShare {
        self.shards.fetch_add(1, Ordering::Relaxed);
        RingShare {
            budget: self.clone(),
            resident: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            fifo: Mutex::new(VecDeque::new()),
            evicted_absorbed: AtomicU64::new(0),
            evicted_absorbed_bytes: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            refused_bytes: AtomicU64::new(0),
            refused_gap: AtomicU64::new(0),
            gap_resets: AtomicU64::new(0),
            evicted_unabsorbed: AtomicU64::new(0),
        }
    }
}

/// One engine's part of a [`RingBudget`]. Its batches leave the ring only
/// through `ring_evict_absorbed`, which takes batches whose every page is
/// below the stream's absorbed boundary; past its share the ring refuses a
/// batch instead (the seal then falls back to the per-stream seek).
pub(crate) struct RingShare {
    budget: Arc<RingBudget>,
    resident: AtomicU64,
    peak: AtomicU64,
    /// One entry per resident batch, in publish order.
    fifo: Mutex<VecDeque<Arc<StreamHandle>>>,
    pub(crate) evicted_absorbed: AtomicU64,
    pub(crate) evicted_absorbed_bytes: AtomicU64,
    /// Batches refused because the share held only unabsorbed pages...
    pub(crate) refused: AtomicU64,
    pub(crate) refused_bytes: AtomicU64,
    /// ...of which refused because an earlier batch of the same stream was
    /// refused and the ring still holds its unabsorbed pages.
    pub(crate) refused_gap: AtomicU64,
    /// Rings dropped whole at a gap because they held only absorbed pages.
    pub(crate) gap_resets: AtomicU64,
    /// L6 M3: batches that left the ring with a page at or above their
    /// stream's absorbed boundary (measured at the eviction; 0 by design).
    pub(crate) evicted_unabsorbed: AtomicU64,
}

impl RingShare {
    pub(crate) fn resident(&self) -> u64 {
        self.resident.load(Ordering::Relaxed)
    }

    pub(crate) fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// This engine's share of the process budget now.
    pub(crate) fn share_bytes(&self) -> u64 {
        self.budget.share()
    }

    fn admit(&self, bytes: u64) {
        let now = self.resident.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(now, Ordering::Relaxed);
        let total = self.budget.resident.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.budget.peak.fetch_max(total, Ordering::Relaxed);
    }

    fn release(&self, bytes: u64) {
        self.resident.fetch_sub(bytes, Ordering::Relaxed);
        self.budget.resident.fetch_sub(bytes, Ordering::Relaxed);
    }

    fn refuse(&self, bytes: u64, gap: bool) {
        self.refused.fetch_add(1, Ordering::Relaxed);
        self.refused_bytes.fetch_add(bytes, Ordering::Relaxed);
        if gap {
            self.refused_gap.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "share_bytes": self.budget.share(),
            "resident_bytes": self.resident(),
            "peak_bytes": self.peak(),
            "evicted_absorbed": self.evicted_absorbed.load(Ordering::Relaxed),
            "evicted_absorbed_bytes": self.evicted_absorbed_bytes.load(Ordering::Relaxed),
            "refused": self.refused.load(Ordering::Relaxed),
            "refused_bytes": self.refused_bytes.load(Ordering::Relaxed),
            "refused_gap": self.refused_gap.load(Ordering::Relaxed),
            "gap_resets": self.gap_resets.load(Ordering::Relaxed),
            "evicted_unabsorbed": self.evicted_unabsorbed.load(Ordering::Relaxed),
            "process_resident_bytes": self.budget.resident(),
            "process_peak_bytes": self.budget.peak(),
            "process_shards": self.budget.shards(),
            "settings": {
                "process_bytes": self.budget.settings.process_bytes,
                "shard_cap_bytes": self.budget.settings.shard_cap_bytes,
                "shard_floor_bytes": self.budget.settings.shard_floor_bytes,
            },
        })
    }
}

impl Drop for RingShare {
    fn drop(&mut self) {
        self.budget.shards.fetch_sub(1, Ordering::Relaxed);
        self.budget
            .resident
            .fetch_sub(self.resident.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// One durable scan window served from the ring: offsets `[from, to)` up to
/// `max_bytes` of stored frames, the same window the canonical scan honours.
#[derive(Clone, Copy)]
pub(crate) struct RingScan {
    pub(crate) from: u64,
    pub(crate) to: u64,
    pub(crate) max_bytes: usize,
}

impl ShardEngine {
    /// Publish one group's frames for one stream into its ring, then
    /// evict globally-oldest batches until the engine-wide budget is
    /// non-negative. FIFO mirrors publish order across streams, so its
    /// front IS the globally oldest batch.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::ring_publish; a poisoned ring or eviction FIFO may hold a half-published batch; recovering it could serve a batch that was never fully retained or evict the wrong stream"
    )]
    pub(super) fn ring_publish(&self, handle: &Arc<StreamHandle>, recs: &RingBatch) {
        if let Some(share) = &self.ring_share {
            self.ring_publish_spill(share, handle, recs);
            return;
        }
        let (first, bytes) = (recs.first, recs.bytes);
        {
            let mut ring = handle.ring.lock().unwrap();
            // A shard handoff replays through a fresh engine, so within
            // one engine offsets only grow. If a gap somehow appears
            // (defensive: absorber trim races ahead), reset rather than
            // serve a hole.
            if ring.ceil().is_some_and(|c| c != first) {
                let dropped = ring.bytes;
                ring.batches.clear();
                ring.bytes = 0;
                self.ring_budget
                    .fetch_add(dropped as i64, Ordering::Relaxed);
                let mut fifo = self.ring_fifo.lock().unwrap();
                fifo.retain(|h| !Arc::ptr_eq(h, handle));
            }
            ring.batches.push_back(recs.clone());
            ring.bytes += bytes;
        }
        self.ring_fifo.lock().unwrap().push_back(handle.clone());
        self.ring_published.fetch_add(1, Ordering::Relaxed);
        let after = self.ring_budget.fetch_sub(bytes as i64, Ordering::Relaxed) - bytes as i64;
        let resident = u64::try_from((self.ring_cfg_bytes as i64 - after).max(0)).unwrap_or(0);
        self.ring_peak_bytes.fetch_max(resident, Ordering::Relaxed);
        while self.ring_budget.load(Ordering::Relaxed) < 0 {
            let Some(victim) = self.ring_fifo.lock().unwrap().pop_front() else {
                break;
            };
            let mut ring = victim.ring.lock().unwrap();
            if let Some(b) = ring.batches.pop_front() {
                ring.bytes -= b.bytes;
                self.ring_budget
                    .fetch_add(b.bytes as i64, Ordering::Relaxed);
                self.ring_evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// L6 spike: publish under the engine's share of the process budget.
    /// Admission never evicts a page at or above its stream's absorbed
    /// boundary: past the share it evicts this engine's absorbed batches,
    /// oldest first, and refuses the batch if that does not make room.
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway ring budget")]
    fn ring_publish_spill(&self, share: &RingShare, handle: &Arc<StreamHandle>, recs: &RingBatch) {
        let bytes = recs.bytes as u64;
        let absorbed = handle.state.lock().unwrap().durable.absorbed;
        {
            let mut ring = handle.ring.lock().unwrap();
            if let Some(ceil) = ring.ceil()
                && ceil != recs.first
            {
                if ceil > absorbed {
                    // An earlier batch was refused and the ring still holds
                    // unabsorbed pages: keep them, refuse the batch.
                    drop(ring);
                    share.refuse(bytes, true);
                    return;
                }
                let (dropped, batches) = (ring.bytes as u64, ring.batches.len() as u64);
                let above = ring.batches.iter().filter(|b| b.next > absorbed).count() as u64;
                share.evicted_unabsorbed.fetch_add(above, Ordering::Relaxed);
                if let Some(stats) = self.spill_stats() {
                    stats.ring_evicted_unabsorbed.fetch_add(above, Ordering::Relaxed);
                }
                ring.batches.clear();
                ring.bytes = 0;
                drop(ring);
                share.release(dropped);
                share.evicted_absorbed.fetch_add(batches, Ordering::Relaxed);
                share.evicted_absorbed_bytes.fetch_add(dropped, Ordering::Relaxed);
                share.gap_resets.fetch_add(1, Ordering::Relaxed);
                self.ring_evicted.fetch_add(batches, Ordering::Relaxed);
                share
                    .fifo
                    .lock()
                    .unwrap()
                    .retain(|h| !Arc::ptr_eq(h, handle));
            }
        }
        let cap = share.budget.share();
        if share.resident() + bytes > cap {
            self.ring_evict_absorbed(share, cap.saturating_sub(bytes));
        }
        if share.resident() + bytes > cap {
            share.refuse(bytes, false);
            return;
        }
        {
            let mut ring = handle.ring.lock().unwrap();
            ring.batches.push_back(recs.clone());
            ring.bytes += recs.bytes;
        }
        share.fifo.lock().unwrap().push_back(handle.clone());
        share.admit(bytes);
        self.ring_published.fetch_add(1, Ordering::Relaxed);
        self.ring_peak_bytes.fetch_max(share.resident(), Ordering::Relaxed);
    }

    /// L6 spike: evict this engine's batches whose pages are all below
    /// their stream's absorbed boundary, oldest first, until its resident
    /// bytes are at most `target`. A stream whose oldest batch still holds
    /// an unabsorbed page keeps every batch (its later batches are newer).
    #[expect(clippy::unwrap_used, reason = "L6 spike: throwaway ring budget")]
    fn ring_evict_absorbed(&self, share: &RingShare, target: u64) {
        let mut fifo = share.fifo.lock().unwrap();
        let mut blocked: HashSet<usize> = HashSet::new();
        let mut i = 0;
        while share.resident() > target && i < fifo.len() {
            let h = fifo[i].clone();
            let key = Arc::as_ptr(&h) as usize;
            if blocked.contains(&key) {
                i += 1;
                continue;
            }
            let absorbed = h.state.lock().unwrap().durable.absorbed;
            let mut ring = h.ring.lock().unwrap();
            match ring.batches.front() {
                Some(b) if b.next <= absorbed => {
                    let b = ring.batches.pop_front().unwrap();
                    if b.next > absorbed {
                        share.evicted_unabsorbed.fetch_add(1, Ordering::Relaxed);
                        if let Some(stats) = self.spill_stats() {
                            stats.ring_evicted_unabsorbed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    ring.bytes -= b.bytes;
                    drop(ring);
                    fifo.remove(i);
                    share.release(b.bytes as u64);
                    share.evicted_absorbed.fetch_add(1, Ordering::Relaxed);
                    share
                        .evicted_absorbed_bytes
                        .fetch_add(b.bytes as u64, Ordering::Relaxed);
                    self.ring_evicted.fetch_add(1, Ordering::Relaxed);
                }
                Some(_) => {
                    blocked.insert(key);
                    i += 1;
                }
                None => {
                    drop(ring);
                    fifo.remove(i);
                }
            }
        }
    }

    /// L6 spike: batches the spill arm's ring refused (0 off the arm).
    pub(crate) fn l6_ring_refused(&self) -> u64 {
        self.ring_share
            .as_ref()
            .map_or(0, |s| s.refused.load(Ordering::Relaxed))
    }

    /// L6 spike M3: batches the spill arm's ring let go of with a page at or
    /// above their stream's absorbed boundary (0 by design).
    pub(crate) fn l6_ring_evicted_unabsorbed(&self) -> u64 {
        self.ring_share
            .as_ref()
            .map_or(0, |s| s.evicted_unabsorbed.load(Ordering::Relaxed))
    }

    /// L6 spike: the share's counters for `/v1/debug/store`.
    pub(crate) fn l6_ring_json(&self) -> serde_json::Value {
        self.ring_share
            .as_ref()
            .map_or(serde_json::Value::Null, RingShare::json)
    }

    pub(crate) fn ring_resident_bytes(&self) -> u64 {
        if let Some(share) = &self.ring_share {
            return share.resident();
        }
        u64::try_from(
            (self.ring_cfg_bytes as i64 - self.ring_budget.load(Ordering::Relaxed)).max(0),
        )
        .unwrap_or(0)
    }

    /// Serve `scan` from the stream's ring if the ring covers its start.
    /// Returns None when it does not (the caller falls back to the canonical
    /// scan). Mirrors the DB path's contract exactly: stop at `max_bytes`,
    /// end at `to`, last_offset is the consumed progress.
    ///
    /// #272: with a `selector` this is the ring read for FILTERED durable
    /// reads. The hub pump (and any keyed tail chaser) reads one routing-key
    /// lane; scanning the covered range once, decoding only frame HEADERS
    /// (payloads stay encrypted) and keeping the frames whose routing key
    /// matches lets keyed readers never rescan match-free ranges, because the
    /// CONSUMED offset covers non-matching frames too. The stored-byte budget
    /// also charges filtered misses; truncation reports the last scanned
    /// offset at the cut. Selection affects the returned frames only; every
    /// inspected row contributes to the coverage witness.
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::ring_read; a poisoned ring or tail state may hold a half-published batch or frontier; recovering it could serve frames that were never made durable"
    )]
    pub(crate) fn ring_read(
        &self,
        handle: &StreamHandle,
        scan: RingScan,
        selector: Option<&str>,
    ) -> Option<FrameReadResult> {
        let RingScan {
            from: scan_from,
            to: scan_to,
            max_bytes,
        } = scan;
        if !self.ring_enabled
            || handle.owner.as_ptr() != Arc::as_ptr(&self.db)
            || scan_from >= scan_to
            || scan_to > handle.state.lock().unwrap().durable.next
        {
            return None;
        }
        let ring = handle.ring.lock().unwrap();
        let (Some(floor), Some(ceil)) = (ring.floor(), ring.ceil()) else {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            self.ring_miss_empty.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if scan_from < floor || scan_to > ceil {
            self.ring_misses.fetch_add(1, Ordering::Relaxed);
            if scan_from < floor {
                self.ring_miss_below_floor.fetch_add(1, Ordering::Relaxed);
            }
            if scan_to > ceil {
                self.ring_miss_above_ceil.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        let (out, expected) = ring.serve(scan_from, scan_to, max_bytes, selector)?;
        Some(self.ring_hit(out, handle, scan_from, expected))
    }

    /// Records a served window: its coverage witness and the hit counter.
    fn ring_hit(
        &self,
        mut out: FrameReadResult,
        handle: &StreamHandle,
        scan_from: u64,
        expected: u64,
    ) -> FrameReadResult {
        out.coverage = Some(record::DurableRingCoverage::new(
            self,
            handle.hash,
            scan_from,
            expected,
        ));
        self.ring_hits.fetch_add(1, Ordering::Relaxed);
        out
    }
}

impl TailRing {
    /// The window `[from, to)` up to `max_bytes` of stored page bytes from
    /// the ring's pages, each admitted again and sliced to the window, and
    /// the offset after the last inspected record. None when the pages are
    /// not dense from `from` or one fails admission: the caller then scans
    /// the store. The page that reaches `max_bytes` is kept.
    fn serve(
        &self,
        from: u64,
        to: u64,
        max_bytes: usize,
        selector: Option<&str>,
    ) -> Option<(FrameReadResult, u64)> {
        let mut out = FrameReadResult::default();
        let mut total = 0usize;
        let mut expected = from;
        // Batches are contiguous and ordered, so the window is the covering
        // batches' pages from the one holding `from` up to the end.
        let pages = self
            .batches
            .iter()
            .filter(|b| b.next > from)
            .take_while(|b| b.first < to)
            .flat_map(|b| b.frames.iter())
            .filter(|(last, _)| *last >= from);
        for (last, raw) in pages {
            let page = CheckedPage::admit(raw.clone(), *last).ok()?;
            let Some(slice) = record::PageSlice::clip(page, from, to) else {
                break;
            };
            // Floor/ceiling alone cannot prove density after eviction or
            // malformed cached batch metadata. Every inspected page counts,
            // including filtered misses and a byte-limited final page.
            if slice.first() != expected {
                return None;
            }
            expected = slice.last().checked_add(1)?;
            total = total.saturating_add(slice.stored_len());
            // Consumed progress covers NON-matching pages too.
            out.last_offset = Some(slice.last());
            if selector.is_none_or(|key| key == slice.page().routing_key()) {
                out.frames.push(slice);
            }
            if total >= max_bytes {
                return Some((out, expected));
            }
        }
        (expected == to).then_some((out, expected))
    }
}

/// L6 spike: whether idle eviction must keep `h` (spill arm): its stream
/// has pages above its absorbed boundary, which the next seal takes from its
/// ring. A state lock held elsewhere keeps it too (eviction never waits).
pub(super) fn l6_holds_unabsorbed(h: &StreamHandle) -> bool {
    h.state
        .try_lock()
        .map_or(true, |st| st.durable.absorbed < st.durable.next)
}
