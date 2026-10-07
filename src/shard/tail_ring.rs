//! The durable-tail ring: each stream's most recent published batches, held
//! under one engine-wide byte budget so live readers can chase offsets the
//! ring still covers without a canonical scan. A batch holds the group's
//! pages of one stream as (last offset, stored page); a read admits each
//! page it inspects again and slices it to the window.
use super::{FrameReadResult, RingBatch, ShardEngine, StreamHandle, TailRing, record};
use crate::crypto_page::CheckedPage;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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

    pub(crate) fn ring_resident_bytes(&self) -> u64 {
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
