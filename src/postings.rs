//! Compact per-routing-key postings for the shared history partition
//! (docs/ROUTING-V3.md §3, replacing the full-frame covering index).
//!
//! The canonical encrypted frame is stored ONCE under
//! `<route16><inc16> 'r' <offset>`. Every routing key — including the
//! empty/default key — gets postings pages:
//!
//! ```text
//! <route16><inc16> 'p' <rk_hash16> <bucket_be8> <page_first_be8>
//!     -> PostingsPageV1 (self-describing header + varint runs; see
//!        `encode_page`)
//! ```
//!
//! A bucket is a fixed window of 65,536 segment-local offsets, so the
//! bucket holding any cursor is directly calculable — no predecessor
//! scan. Pages never span buckets (runs split at the boundary), and a
//! page's runs decode relative to its `page_first` offset. Gap byte
//! accounting is intra-page: the planner uses it to choose between
//! scanning exact runs, coalescing across a small gap, or reading one
//! envelope and filtering; page boundaries are natural span breaks.
//!
//! Pages ride the SAME history WriteBatch and flush as their canonical
//! rows — postings add no Class A request, no manifest update, no
//! database, no namespace, and no GC lifecycle of their own.
//!
//! A 128-bit `rk_hash` collision can only ADD candidate frames; the
//! read path verifies every frame against the exact routing-key bytes,
//! so another key's data is never returned.

use crate::crypto::{RouteHash, RoutingKeyHash, SegmentHash};

/// Segment-local offsets per postings bucket. Fixed so that
/// `bucket = offset / BUCKET_OFFSETS` is directly calculable.
/// A format constant, not an operator mode (spec §6.4).
pub const BUCKET_OFFSETS: u64 = 65_536;

/// Hard cap on one encoded page (spec §6.4). The builder splits a
/// bucket into multiple pages (distinct `page_first`) at this size.
pub const PAGE_MAX_ENCODED_BYTES: usize = 32 * 1024;

pub fn bucket_of(offset: u64) -> u64 {
    offset / BUCKET_OFFSETS
}

/// 16-byte routing-key hash (the postings key discriminator).
pub fn rk_hash(rk: &str) -> RoutingKeyHash {
    RoutingKeyHash::of(rk)
}

pub fn postings_key(
    route: RouteHash,
    inc: SegmentHash,
    rk_hash: &RoutingKeyHash,
    bucket: u64,
    page_first: u64,
) -> Vec<u8> {
    let mut k = Vec::with_capacity(65);
    k.extend_from_slice(&route.0);
    k.extend_from_slice(&inc.0);
    k.push(b'p');
    k.extend_from_slice(&rk_hash.0);
    k.extend_from_slice(&bucket.to_be_bytes());
    k.extend_from_slice(&page_first.to_be_bytes());
    k
}

/// Scan range covering every page of `rk_hash` whose bucket intersects
/// [from_offset, upto_offset). Buckets are big-endian directly after
/// the key hash, so one contiguous range covers them all.
pub fn postings_range(
    route: RouteHash,
    inc: SegmentHash,
    rk_hash: &RoutingKeyHash,
    from_offset: u64,
    upto_offset: u64,
) -> (Vec<u8>, Vec<u8>) {
    let b_lo = bucket_of(from_offset);
    // upto is exclusive; the last relevant bucket holds upto-1.
    let b_hi = bucket_of(upto_offset.saturating_sub(1).max(from_offset));
    let lo = postings_key(route, inc, rk_hash, b_lo, 0);
    let hi = postings_key(route, inc, rk_hash, b_hi, u64::MAX);
    (lo, hi)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingRun {
    pub gap_offsets: u64,
    pub record_count: u32,
    pub matching_frame_bytes: u64,
    pub gap_frame_bytes_before: u64,
}

fn put_varint(v: &mut Vec<u8>, mut x: u64) {
    loop {
        let b = (x & 0x7f) as u8;
        x >>= 7;
        if x == 0 {
            v.push(b);
            return;
        }
        v.push(b | 0x80);
    }
}

fn get_varint(v: &[u8], at: &mut usize) -> Option<u64> {
    let mut out: u64 = 0;
    let mut shift = 0u32;
    loop {
        let b = *v.get(*at)?;
        *at += 1;
        out |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Some(out);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// PostingsPageV1 (spec §6.4). Self-describing header + varint runs:
///
/// ```text
/// u8  version = 1
/// u8  codec   = 0 raw   (1 reserved: deterministic compress-if-smaller)
/// u64 first_offset            (LE; duplicates the key's page_first)
/// u64 last_offset_exclusive   (LE)
/// u32 run_count               (LE)
/// u64 matching_frame_bytes    (LE; page total)
/// runs: varint gap_offsets, record_count, matching_frame_bytes,
///       gap_frame_bytes_before
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub first_offset: u64,
    pub last_offset_exclusive: u64,
    pub matching_frame_bytes: u64,
    pub runs: Vec<PostingRun>,
}

pub fn encode_page(first_offset: u64, runs: &[PostingRun]) -> Vec<u8> {
    let mut v = Vec::with_capacity(32 + runs.len() * 6);
    v.push(1u8);
    v.push(0u8); // codec: raw
    let mut off = first_offset;
    let mut total = 0u64;
    for r in runs {
        off += r.gap_offsets + r.record_count as u64;
        total += r.matching_frame_bytes;
    }
    v.extend_from_slice(&first_offset.to_le_bytes());
    v.extend_from_slice(&off.to_le_bytes());
    v.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    v.extend_from_slice(&total.to_le_bytes());
    for r in runs {
        put_varint(&mut v, r.gap_offsets);
        put_varint(&mut v, r.record_count as u64);
        put_varint(&mut v, r.matching_frame_bytes);
        put_varint(&mut v, r.gap_frame_bytes_before);
    }
    v
}

pub fn decode_page(v: &[u8]) -> Option<Page> {
    if v.len() < 30 || v[0] != 1 || v[1] != 0 {
        return None;
    }
    let first_offset = u64::from_le_bytes(v[2..10].try_into().ok()?);
    let last_offset_exclusive = u64::from_le_bytes(v[10..18].try_into().ok()?);
    let n = u32::from_le_bytes(v[18..22].try_into().ok()?) as usize;
    let matching_frame_bytes = u64::from_le_bytes(v[22..30].try_into().ok()?);
    if n > BUCKET_OFFSETS as usize {
        return None;
    }
    let mut at = 30usize;
    let mut runs = Vec::with_capacity(n);
    for _ in 0..n {
        runs.push(PostingRun {
            gap_offsets: get_varint(v, &mut at)?,
            record_count: get_varint(v, &mut at)? as u32,
            matching_frame_bytes: get_varint(v, &mut at)?,
            gap_frame_bytes_before: get_varint(v, &mut at)?,
        });
    }
    // Posting monotonicity (spec §13.3): the header must agree with
    // the runs; disagreement is corruption, not a parse quirk.
    let mut off = first_offset;
    let mut total = 0u64;
    for r in &runs {
        off += r.gap_offsets + r.record_count as u64;
        total += r.matching_frame_bytes;
    }
    if off != last_offset_exclusive || total != matching_frame_bytes {
        return None;
    }
    Some(Page {
        first_offset,
        last_offset_exclusive,
        matching_frame_bytes,
        runs,
    })
}

/// One decoded run at an absolute offset position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbsRun {
    pub start: u64,
    pub count: u32,
    pub matching_bytes: u64,
    /// Stored bytes of the gap before this run — INTRA-PAGE truth from
    /// the codec, or GAP_UNKNOWN once pages are stitched (the bytes
    /// between two pages were never measured; the planner must not
    /// treat them as free).
    pub gap_bytes_before: u64,
}

/// Cross-page gap marker: the byte cost is unknown, so coalescing is
/// forbidden unless the offsets are literally contiguous.
pub const GAP_UNKNOWN: u64 = u64::MAX;

/// Append one page's decoded runs onto an accumulating list, marking
/// the seam: unless the page's first run continues EXACTLY at the
/// previous run's end, its gap byte cost was never measured and must
/// read as GAP_UNKNOWN. Without this the first run of each page
/// carries gap_bytes_before=0 and the planner coalesces arbitrarily
/// distant pages into one giant span (measured: a 40k-offset stream
/// scanned WHOLE for a 2-record key — 10,000x amplification).
pub fn append_page_runs(all: &mut Vec<AbsRun>, page: Vec<AbsRun>) {
    let prev_end = all.last().map(|r| r.start + r.count as u64);
    for (i, mut r) in page.into_iter().enumerate() {
        if i == 0 && prev_end != Some(r.start) {
            r.gap_bytes_before = GAP_UNKNOWN;
        }
        all.push(r);
    }
}

/// Decode a page into absolute runs. `page_first` (from the KEY) must
/// agree with the page header — a mismatch is corruption.
pub fn decode_page_abs(page_first: u64, v: &[u8]) -> Option<Vec<AbsRun>> {
    let page = decode_page(v)?;
    if page.first_offset != page_first {
        return None;
    }
    let runs = page.runs;
    let mut out = Vec::with_capacity(runs.len());
    let mut off = page_first;
    for r in runs {
        off += r.gap_offsets;
        out.push(AbsRun {
            start: off,
            count: r.record_count,
            matching_bytes: r.matching_frame_bytes,
            gap_bytes_before: r.gap_frame_bytes_before,
        });
        off += r.record_count as u64;
    }
    Some(out)
}

// ---- gather-side page builder ----------------------------------------

/// Builds every key's postings pages for ONE gather chunk (one stream's
/// contiguous frame walk, offsets strictly ascending). Emits pages
/// split at bucket boundaries; gap byte accounting is intra-page.
#[derive(Default)]
pub struct PageBuilder {
    /// Total stored frame bytes walked so far (all keys).
    walked_bytes: u64,
    keys: std::collections::HashMap<RoutingKeyHash, KeyAcc>,
}

struct KeyAcc {
    /// Open page: (bucket, page_first, runs).
    bucket: u64,
    page_first: u64,
    runs: Vec<PostingRun>,
    /// Open run tail: next expected offset and accumulated fields.
    run_next: u64,
    run_count: u32,
    run_bytes: u64,
    /// walked_bytes at the end of this key's most recent matching frame
    /// (== the end of its open/last run). The gap bytes of the NEXT run
    /// are `walk position at its first frame − this`.
    walked_at_run_end: u64,
    /// Finished pages ready to emit.
    done: Vec<(u64, u64, Vec<PostingRun>)>,
}

impl PageBuilder {
    /// Account one canonical frame at `offset` with stored size
    /// `frame_bytes` for routing-key hash `key`. MUST be called in
    /// strictly ascending offset order across the whole chunk.
    pub fn note_frame(&mut self, key: RoutingKeyHash, offset: u64, frame_bytes: u64) {
        let walked_before = self.walked_bytes;
        self.walked_bytes += frame_bytes;
        let bucket = bucket_of(offset);
        let acc = self.keys.entry(key).or_insert_with(|| KeyAcc {
            bucket,
            page_first: offset,
            runs: Vec::new(),
            run_next: offset,
            run_count: 0,
            run_bytes: 0,
            walked_at_run_end: walked_before,
            done: Vec::new(),
        });
        // Bucket boundary — or the encoded-size cap (spec §6.4, 32 KiB)
        // — closes the page; a fresh page opens at this offset (pages
        // never span buckets, and multiple pages per bucket sort by
        // their page_first key component).
        let page_full = acc.runs.len() * 12 + 40 >= PAGE_MAX_ENCODED_BYTES;
        if bucket != acc.bucket || (page_full && acc.run_count == 0) {
            Self::close_run(acc);
            if !acc.runs.is_empty() {
                acc.done
                    .push((acc.bucket, acc.page_first, std::mem::take(&mut acc.runs)));
            }
            acc.bucket = bucket;
            acc.page_first = offset;
            acc.run_next = offset;
            acc.walked_at_run_end = walked_before;
        }
        debug_assert!(offset >= acc.run_next || acc.run_count == 0);
        if offset != acc.run_next && acc.run_count > 0 {
            Self::close_run(acc);
        }
        if acc.run_count == 0 {
            // Opening a run: gaps are relative to the previous run's
            // end (page_first for the first run of a page).
            let gap_offsets = if acc.runs.is_empty() {
                offset - acc.page_first
            } else {
                offset - acc.run_next
            };
            let gap_bytes = walked_before.saturating_sub(acc.walked_at_run_end);
            acc.runs.push(PostingRun {
                gap_offsets,
                record_count: 0,
                matching_frame_bytes: 0,
                gap_frame_bytes_before: gap_bytes,
            });
            acc.run_next = offset;
        }
        acc.run_count += 1;
        acc.run_bytes += frame_bytes;
        acc.run_next = offset + 1;
        acc.walked_at_run_end = self.walked_bytes;
    }

    fn close_run(acc: &mut KeyAcc) {
        if acc.run_count == 0 {
            return;
        }
        let open = acc.runs.last_mut().expect("open run");
        open.record_count = acc.run_count;
        open.matching_frame_bytes = acc.run_bytes;
        acc.run_count = 0;
        acc.run_bytes = 0;
    }

    /// Emit every page: `(rk_hash, bucket, page_first, encoded_value)`,
    /// plus the total encoded postings bytes (the byte-ratio gate's
    /// numerator).
    pub fn finish(mut self) -> (Vec<(RoutingKeyHash, u64, u64, Vec<u8>)>, u64) {
        let mut out = Vec::new();
        let mut total = 0u64;
        for (key, mut acc) in self.keys.drain() {
            Self::close_run(&mut acc);
            if !acc.runs.is_empty() {
                acc.done
                    .push((acc.bucket, acc.page_first, std::mem::take(&mut acc.runs)));
            }
            for (bucket, first, runs) in acc.done {
                let v = encode_page(first, &runs);
                total += v.len() as u64;
                out.push((key, bucket, first, v));
            }
        }
        (out, total)
    }
}

// ---- read planner ------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct PlanCfg {
    pub max_spans: usize,
    pub max_gap_bytes: u64,
    pub max_scan_bytes: u64,
    /// Preferred scan/matching ratio: a gap coalesce beyond this opens
    /// a new span instead while span slots remain (spec §5).
    pub target_amplification: f64,
    /// HARD scan/matching ceiling for any single coalesce: storage
    /// savings must never silently shift into Class B read bytes (spec
    /// §5, review finding 6 — two 1 KiB records around a 60 KiB gap
    /// used to plan one ~62 KiB scan, ~31x).
    pub hard_amplification: f64,
}

impl Default for PlanCfg {
    fn default() -> Self {
        PlanCfg {
            max_spans: 8,
            max_gap_bytes: 64 * 1024,
            max_scan_bytes: 16 * 1024 * 1024,
            target_amplification: 2.0,
            hard_amplification: 4.0,
        }
    }
}

/// One canonical scan span the reader will issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u64,
    /// Exclusive end offset.
    pub end: u64,
    pub matching_bytes: u64,
    /// Estimated TOTAL stored bytes the scan will read (matching plus
    /// coalesced gaps) — the amplification denominator's counterpart.
    pub scan_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub spans: Vec<Span>,
    /// The read provably consumed matches up to here (exclusive): the
    /// caller's cursor advances to this even over match-free ranges.
    pub consumed_to: u64,
    /// Every run in the requested range was planned.
    pub complete: bool,
}

/// Plan bounded canonical spans over absolute runs (ascending, within
/// one requested range). Coalesces a following run into the current
/// span when the intervening gap is small in BYTES; otherwise opens a
/// new span. Stops at span/byte budgets with an honest partial.
pub fn plan_spans(runs: &[AbsRun], upto: u64, cfg: &PlanCfg) -> Plan {
    let mut plan = Plan {
        spans: Vec::new(),
        consumed_to: 0,
        complete: true,
    };
    let mut scan_total = 0u64;
    for r in runs {
        let r_end = r.start + r.count as u64;
        let r_bytes = r.matching_bytes;
        let contiguous = plan.spans.last().is_some_and(|s| s.end == r.start);
        // Amplification guard on gap coalescing: the combined span's
        // scan/matching ratio must stay under TARGET while span slots
        // remain (an exact new span is cheaper), and under HARD always —
        // never trade the storage win for Class B scan bytes.
        // Contiguous runs add no gap, so they can only improve the
        // ratio and always coalesce (budget permitting).
        let at_last_slot = plan.spans.len() >= cfg.max_spans;
        let amp_ok = |s: &Span| {
            let gap = if contiguous { 0 } else { r.gap_bytes_before };
            let scan = (s.scan_bytes + gap + r_bytes) as f64;
            let matching = (s.matching_bytes + r_bytes) as f64;
            let limit = if at_last_slot {
                cfg.hard_amplification
            } else {
                cfg.target_amplification
            };
            contiguous || scan <= matching * limit
        };
        match plan.spans.last_mut() {
            Some(s)
                if (contiguous
                    || (r.gap_bytes_before != GAP_UNKNOWN
                        && r.gap_bytes_before <= cfg.max_gap_bytes))
                    && scan_total + if contiguous { 0 } else { r.gap_bytes_before } + r_bytes
                        <= cfg.max_scan_bytes
                    && amp_ok(s) =>
            {
                // Coalesce into the open span (cheap, ratio-safe gap).
                s.end = r_end;
                s.matching_bytes += r_bytes;
                let gap = if contiguous { 0 } else { r.gap_bytes_before };
                s.scan_bytes += gap + r_bytes;
                scan_total += gap + r_bytes;
            }
            _ => {
                if plan.spans.len() >= cfg.max_spans {
                    plan.complete = false;
                    return plan;
                }
                if scan_total + r_bytes > cfg.max_scan_bytes {
                    // Budget exhausted BY this run. With prior spans the
                    // partial already made progress; with NONE, the first
                    // record must still be served (review blocker: a run
                    // fatter than the whole scan budget used to plan ZERO
                    // spans, so the read returned an empty page forever).
                    // Split the run to a bounded prefix — per-record
                    // estimate, never fewer than one record; the span
                    // executor's response budget truncates honestly if
                    // the estimate under-counts.
                    if plan.spans.is_empty() {
                        let per_rec = (r.matching_bytes / r.count.max(1) as u64).max(1);
                        let budget_recs = (cfg.max_scan_bytes / per_rec).clamp(1, r.count as u64);
                        let end = r.start + budget_recs;
                        let bytes = per_rec * budget_recs;
                        plan.spans.push(Span {
                            start: r.start,
                            end,
                            matching_bytes: bytes,
                            scan_bytes: bytes,
                        });
                        plan.consumed_to = end;
                    }
                    plan.complete = false;
                    return plan;
                }
                plan.spans.push(Span {
                    start: r.start,
                    end: r_end,
                    matching_bytes: r_bytes,
                    scan_bytes: r_bytes,
                });
                scan_total += r_bytes;
            }
        }
        plan.consumed_to = r_end;
    }
    // All runs planned: the whole requested range is consumed, even the
    // match-free tail past the last run.
    plan.consumed_to = plan.consumed_to.max(upto);
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(start: u64, count: u32, bytes: u64, gap_bytes: u64) -> AbsRun {
        AbsRun {
            start,
            count,
            matching_bytes: bytes,
            gap_bytes_before: gap_bytes,
        }
    }

    /// Review finding 6: tiny matches around a large-but-under-64KiB
    /// gap must NOT coalesce into one high-amplification scan — the
    /// hard 4x (and target 2x) ratio gates it.
    #[test]
    fn amplification_bound_refuses_expensive_gaps() {
        let cfg = PlanCfg::default();
        // The reviewer's example: 1 KiB + 60 KiB gap + 1 KiB = ~31x.
        let plan = plan_spans(
            &[run(0, 1, 1024, 0), run(100, 1, 1024, 60 * 1024)],
            101,
            &cfg,
        );
        assert_eq!(plan.spans.len(), 2, "exact spans, not one 62 KiB scan");
        assert!(plan.complete);
        for s in &plan.spans {
            assert!(
                (s.scan_bytes as f64) <= (s.matching_bytes as f64) * cfg.hard_amplification,
                "span amp {}/{} exceeds hard bound",
                s.scan_bytes,
                s.matching_bytes,
            );
        }

        // A cheap gap (ratio under target) still coalesces.
        let plan = plan_spans(&[run(0, 4, 4096, 0), run(100, 4, 4096, 2048)], 104, &cfg);
        assert_eq!(plan.spans.len(), 1, "cheap gap coalesces");
        assert!(plan.complete);
    }

    /// One tiny match per postings page across many pages: every span
    /// stays exact and the plan stays span-bounded with honest partials.
    #[test]
    fn fragmented_key_stays_exact_and_bounded() {
        let cfg = PlanCfg::default();
        let mut runs = Vec::new();
        for i in 0..12u64 {
            // 256 B matches separated by 32 KiB gaps: 129x if coalesced.
            runs.push(run(i * 1000, 1, 256, if i == 0 { 0 } else { 32 * 1024 }));
        }
        let plan = plan_spans(&runs, 12_000, &cfg);
        assert_eq!(plan.spans.len(), cfg.max_spans, "span-bounded");
        assert!(!plan.complete, "honest partial past the span budget");
        assert_eq!(
            plan.consumed_to,
            runs[cfg.max_spans - 1].start + 1,
            "cursor covers exactly the planned prefix"
        );
        for s in &plan.spans {
            assert_eq!(s.scan_bytes, s.matching_bytes, "every span exact");
        }
    }

    /// Contiguous runs never trip the amplification guard.
    #[test]
    fn contiguous_runs_coalesce_regardless_of_amp() {
        let cfg = PlanCfg::default();
        let plan = plan_spans(&[run(0, 2, 200, 0), run(2, 2, 200, GAP_UNKNOWN)], 4, &cfg);
        assert_eq!(plan.spans.len(), 1);
        assert!(plan.complete);
    }

    /// Review blocker: a first run fatter than the whole scan budget
    /// must still plan a bounded prefix — never zero spans.
    #[test]
    fn oversized_first_run_plans_bounded_prefix() {
        let cfg = PlanCfg {
            max_scan_bytes: 16 * 1024 * 1024,
            ..Default::default()
        };
        // One 32 MiB record: budget/record floor still allows exactly it.
        let plan = plan_spans(&[run(10, 1, 32 * 1024 * 1024, 0)], 11, &cfg);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!((plan.spans[0].start, plan.spans[0].end), (10, 11));
        assert!(!plan.complete);
        assert_eq!(plan.consumed_to, 11);

        // A 24 MiB contiguous run of 1 MiB records: ~16 records fit.
        let plan = plan_spans(&[run(0, 24, 24 * 1024 * 1024, 0)], 24, &cfg);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].start, 0);
        assert_eq!(plan.spans[0].end, 16, "budget/per-record prefix");
        assert!(!plan.complete);
        assert_eq!(plan.consumed_to, 16, "cursor advances to the prefix end");
    }

    /// A fat run AFTER planned spans returns an honest partial whose
    /// consumed_to covers the planned prefix only.
    #[test]
    fn oversized_mid_run_partial_keeps_progress() {
        let cfg = PlanCfg {
            max_scan_bytes: 1024,
            ..Default::default()
        };
        let plan = plan_spans(
            &[run(0, 2, 400, 0), run(100, 1, 10_000, GAP_UNKNOWN)],
            101,
            &cfg,
        );
        assert_eq!(plan.spans.len(), 1);
        assert!(!plan.complete);
        assert_eq!(plan.consumed_to, 2, "progress = what was actually planned");
    }

    /// No runs in the window: the plan consumes the whole match-free
    /// range (overall completion is the caller's provable_to gate).
    #[test]
    fn empty_window_consumes_range() {
        let plan = plan_spans(&[], 500, &PlanCfg::default());
        assert!(plan.spans.is_empty());
        assert!(plan.complete);
        assert_eq!(plan.consumed_to, 500);
    }

    #[test]
    fn varint_roundtrip_edges() {
        for x in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut v = Vec::new();
            put_varint(&mut v, x);
            let mut at = 0;
            assert_eq!(get_varint(&v, &mut at), Some(x));
            assert_eq!(at, v.len());
        }
    }

    #[test]
    fn page_codec_roundtrip() {
        let runs = vec![
            PostingRun {
                gap_offsets: 0,
                record_count: 3,
                matching_frame_bytes: 900,
                gap_frame_bytes_before: 0,
            },
            PostingRun {
                gap_offsets: 41,
                record_count: 1,
                matching_frame_bytes: 128,
                gap_frame_bytes_before: 17_000,
            },
        ];
        let v = encode_page(1000, &runs);
        let page = decode_page(&v).unwrap();
        assert_eq!(page.runs, runs);
        assert_eq!(page.first_offset, 1000);
        assert_eq!(page.last_offset_exclusive, 1000 + 3 + 41 + 1);
        assert_eq!(page.matching_frame_bytes, 1028);
        let abs = decode_page_abs(1000, &v).unwrap();
        assert_eq!(abs[0].start, 1000);
        assert_eq!(abs[1].start, 1000 + 3 + 41);
        // Key/header disagreement = corruption.
        assert!(decode_page_abs(999, &v).is_none());
        // Header/runs disagreement = corruption.
        let mut bad = v.clone();
        bad[10] ^= 1; // perturb last_offset_exclusive
        assert!(decode_page(&bad).is_none());
    }

    #[test]
    fn builder_splits_runs_and_buckets() {
        let ka = RoutingKeyHash([1u8; 16]);
        let kb = RoutingKeyHash([2u8; 16]);
        let mut b = PageBuilder::default();
        // Interleaved: a a b a  | gap |  a, then a crosses a bucket edge.
        b.note_frame(ka, 10, 100);
        b.note_frame(ka, 11, 100);
        b.note_frame(kb, 12, 50);
        b.note_frame(ka, 13, 100);
        b.note_frame(kb, 20, 60);
        let edge = BUCKET_OFFSETS;
        b.note_frame(ka, edge - 1, 100);
        b.note_frame(ka, edge, 100);
        let (pages, total) = b.finish();
        assert!(total > 0);
        let a_pages: Vec<_> = pages.iter().filter(|p| p.0 == ka).collect();
        assert_eq!(a_pages.len(), 2, "bucket edge must split the page");
        let p0 = a_pages.iter().find(|p| p.1 == 0).unwrap();
        let abs = decode_page_abs(p0.2, &p0.3).unwrap();
        // Runs for a in bucket 0: [10,12) at 10..11, [13,14), [edge-1,edge).
        assert_eq!(abs[0].start, 10);
        assert_eq!(abs[0].count, 2);
        assert_eq!(abs[1].start, 13);
        assert_eq!(abs[1].count, 1);
        // The gap before run [13]: frame kb@12 (50 bytes) sits between.
        assert_eq!(abs[1].gap_bytes_before, 50);
        assert_eq!(abs[2].start, edge - 1);
        let p1 = a_pages.iter().find(|p| p.1 == 1).unwrap();
        let abs1 = decode_page_abs(p1.2, &p1.3).unwrap();
        assert_eq!(abs1[0].start, edge);
        assert_eq!(abs1[0].count, 1);

        let b_pages: Vec<_> = pages.iter().filter(|p| p.0 == kb).collect();
        assert_eq!(b_pages.len(), 1);
        let abs_b = decode_page_abs(b_pages[0].2, &b_pages[0].3).unwrap();
        assert_eq!(abs_b[0].start, 12);
        assert_eq!(abs_b[1].start, 20);
        // Gap before b@20: frame a@13 (100 bytes).
        assert_eq!(abs_b[1].gap_bytes_before, 100);
    }

    #[test]
    fn cross_page_seams_never_coalesce() {
        let cfg = PlanCfg::default();
        // Two singleton runs from DIFFERENT pages, 20k offsets apart,
        // seam bytes unknown: must be two spans, never one giant scan.
        let runs = vec![
            run(5, 1, 1_000, 0),
            AbsRun {
                start: 20_005,
                count: 1,
                matching_bytes: 1_000,
                gap_bytes_before: GAP_UNKNOWN,
            },
        ];
        let plan = plan_spans(&runs, 40_000, &cfg);
        assert_eq!(plan.spans.len(), 2, "unknown seams must open a new span");
        assert_eq!(plan.spans[0].end, 6);
        assert_eq!(plan.spans[1].start, 20_005);
        // Truly contiguous across a seam still merges.
        let runs = vec![
            run(5, 1, 1_000, 0),
            AbsRun {
                start: 6,
                count: 1,
                matching_bytes: 1_000,
                gap_bytes_before: GAP_UNKNOWN,
            },
        ];
        let plan = plan_spans(&runs, 40_000, &cfg);
        assert_eq!(plan.spans.len(), 1, "contiguous offsets are one span");
    }

    #[test]
    fn planner_coalesces_cheap_gaps_and_bounds_spans() {
        let cfg = PlanCfg::default();
        // Two runs separated by a tiny gap coalesce into one span.
        let plan = plan_spans(&[run(0, 10, 4_000, 0), run(15, 5, 2_000, 1_000)], 100, &cfg);
        assert_eq!(plan.spans.len(), 1);
        assert_eq!(plan.spans[0].start, 0);
        assert_eq!(plan.spans[0].end, 20);
        assert!(plan.complete);
        assert_eq!(plan.consumed_to, 100, "match-free tail is consumed");

        // A big gap opens a second span.
        let plan = plan_spans(
            &[run(0, 10, 4_000, 0), run(50_000, 5, 2_000, 10_000_000)],
            60_000,
            &cfg,
        );
        assert_eq!(plan.spans.len(), 2);

        // Span budget: the ninth distinct run is deferred honestly.
        let mut runs9 = Vec::new();
        for i in 0..9u64 {
            runs9.push(run(i * 1_000_000, 1, 100, 10_000_000));
        }
        let plan = plan_spans(&runs9, 9_000_000, &cfg);
        assert_eq!(plan.spans.len(), 8);
        assert!(!plan.complete);
        assert_eq!(
            plan.consumed_to,
            7 * 1_000_000 + 1,
            "cursor resumes at the first unplanned run"
        );
    }
}
