#![warn(clippy::indexing_slicing, clippy::arithmetic_side_effects)]
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

#[path = "postings/validated.rs"]
mod validated;
pub(crate) use validated::{RunWindow, ValidatedRuns};

/// Segment-local offsets per postings bucket. Fixed so that
/// `bucket = offset / BUCKET_OFFSETS` is directly calculable.
/// A format constant, not an operator mode (spec §6.4).
pub(crate) const BUCKET_OFFSETS: u64 = 65_536;

/// Hard cap on one encoded page (spec §6.4). The builder splits a
/// bucket into multiple pages (distinct `page_first`) at this size.
pub(crate) const PAGE_MAX_ENCODED_BYTES: usize = 32 * 1024;

pub(crate) fn bucket_of(offset: u64) -> u64 {
    offset / BUCKET_OFFSETS
}

/// 16-byte routing-key hash (the postings key discriminator).
pub(crate) fn rk_hash(rk: &str) -> RoutingKeyHash {
    RoutingKeyHash::of(rk)
}

pub(crate) fn postings_key(
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
pub(crate) fn postings_range(
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
pub(crate) struct PostingRun {
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

fn get_varint(input: &mut &[u8]) -> Option<u64> {
    let mut out = 0u64;
    for shift in (0..=63).step_by(7) {
        let (&byte, rest) = input.split_first()?;
        *input = rest;
        if shift == 63 && byte & 0x7f > 1 {
            return None;
        }
        out |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(out);
        }
    }
    None
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
pub(crate) struct Page {
    pub first_offset: u64,
    pub last_offset_exclusive: u64,
    pub matching_frame_bytes: u64,
    pub runs: Vec<PostingRun>,
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "encode_page; a page holds at most PAGE_MAX_ENCODED_BYTES / 4 runs, so the run count fits the u32 header field; a checked conversion would add an error path no admitted page reaches"
)]
pub(crate) fn encode_page(first_offset: u64, runs: &[PostingRun]) -> Vec<u8> {
    let mut v = Vec::with_capacity(runs.len().saturating_mul(6).saturating_add(32));
    v.push(1u8);
    v.push(0u8); // codec: raw
    let mut off = first_offset;
    let mut total = 0u64;
    for r in runs {
        off = off
            .saturating_add(r.gap_offsets)
            .saturating_add(u64::from(r.record_count));
        total = total.saturating_add(r.matching_frame_bytes);
    }
    v.extend_from_slice(&first_offset.to_le_bytes());
    v.extend_from_slice(&off.to_le_bytes());
    v.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    v.extend_from_slice(&total.to_le_bytes());
    for r in runs {
        put_varint(&mut v, r.gap_offsets);
        put_varint(&mut v, u64::from(r.record_count));
        put_varint(&mut v, r.matching_frame_bytes);
        put_varint(&mut v, r.gap_frame_bytes_before);
    }
    v
}

/// Admit the untrusted allocation count before reserving decoded runs.
/// Each run needs at least four one-byte varints in the capped page body.
fn admitted_run_count(raw: &[u8; 4], encoded_bytes: usize) -> Option<usize> {
    let count = usize::try_from(u32::from_le_bytes(*raw)).ok()?;
    (count > 0 && count <= encoded_bytes / 4).then_some(count)
}

pub(crate) fn decode_page(v: &[u8]) -> Option<Page> {
    if v.len() > PAGE_MAX_ENCODED_BYTES {
        return None;
    }
    let ([version, codec], input) = v.split_first_chunk::<2>()?;
    if *version != 1 || *codec != 0 {
        return None;
    }
    let (first, input) = input.split_first_chunk::<8>()?;
    let (last, input) = input.split_first_chunk::<8>()?;
    let (count, input) = input.split_first_chunk::<4>()?;
    let (matching, mut input) = input.split_first_chunk::<8>()?;
    let first_offset = u64::from_le_bytes(*first);
    let last_offset_exclusive = u64::from_le_bytes(*last);
    let n = admitted_run_count(count, input.len())?;
    let matching_frame_bytes = u64::from_le_bytes(*matching);
    let mut runs = Vec::with_capacity(n);
    for _ in 0..n {
        runs.push(PostingRun {
            gap_offsets: get_varint(&mut input)?,
            record_count: u32::try_from(get_varint(&mut input)?).ok()?,
            matching_frame_bytes: get_varint(&mut input)?,
            gap_frame_bytes_before: get_varint(&mut input)?,
        });
    }
    // Posting monotonicity (spec §13.3): the header must agree with
    // the runs; disagreement is corruption, not a parse quirk.
    let mut off = first_offset;
    let mut total = 0u64;
    for r in &runs {
        if r.record_count == 0 || r.matching_frame_bytes == 0 {
            return None;
        }
        off = off
            .checked_add(r.gap_offsets)?
            .checked_add(u64::from(r.record_count))?;
        total = total.checked_add(r.matching_frame_bytes)?;
    }
    if !input.is_empty()
        || runs.first()?.gap_offsets != 0
        || off != last_offset_exclusive
        || total != matching_frame_bytes
    {
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
pub(crate) struct AbsRun {
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
pub(crate) const GAP_UNKNOWN: u64 = u64::MAX;

/// Append one page's decoded runs onto an accumulating list, marking
/// the seam: unless the page's first run continues EXACTLY at the
/// previous run's end, its gap byte cost was never measured and must
/// read as GAP_UNKNOWN. Without this the first run of each page
/// carries gap_bytes_before=0 and the planner coalesces arbitrarily
/// distant pages into one giant span (measured: a 40k-offset stream
/// scanned WHOLE for a 2-record key — 10,000x amplification).
pub(crate) fn append_page_runs(all: &mut Vec<AbsRun>, page: Vec<AbsRun>) -> Option<()> {
    validated::validate(&page)?;
    let prev_end = match all.last() {
        Some(r) => Some(r.start.checked_add(u64::from(r.count))?),
        None => None,
    };
    if prev_end
        .zip(page.first())
        .is_some_and(|(end, first)| end > first.start)
    {
        return None;
    }
    for (i, mut r) in page.into_iter().enumerate() {
        if i == 0 && prev_end != Some(r.start) {
            r.gap_bytes_before = GAP_UNKNOWN;
        }
        all.push(r);
    }
    Some(())
}

/// Decode a page into absolute runs. `page_first` (from the KEY) must
/// agree with the page header — a mismatch is corruption.
pub(crate) fn decode_page_abs(page_first: u64, v: &[u8]) -> Option<Vec<AbsRun>> {
    let page = decode_page(v)?;
    if page.first_offset != page_first {
        return None;
    }
    let runs = page.runs;
    let mut out = Vec::with_capacity(runs.len());
    let mut off = page_first;
    for r in runs {
        off = off.checked_add(r.gap_offsets)?;
        out.push(AbsRun {
            start: off,
            count: r.record_count,
            matching_bytes: r.matching_frame_bytes,
            gap_bytes_before: r.gap_frame_bytes_before,
        });
        off = off.checked_add(u64::from(r.record_count))?;
    }
    validated::validate(&out)?;
    Some(out)
}

/// Admit the entire stored key and its bucket/range, before publishing coverage.
pub(crate) fn decode_stored_page(
    route: RouteHash,
    inc: SegmentHash,
    kh: &RoutingKeyHash,
    key: &[u8],
    value: &[u8],
) -> Option<Vec<AbsRun>> {
    let (stored_route, input) = key.split_first_chunk::<16>()?;
    let (stored_inc, input) = input.split_first_chunk::<16>()?;
    let (kind, input) = input.split_first()?;
    let (stored_hash, input) = input.split_first_chunk::<16>()?;
    let (bucket, input) = input.split_first_chunk::<8>()?;
    let (first, rest) = input.split_first_chunk::<8>()?;
    if !rest.is_empty()
        || stored_route != &route.0
        || stored_inc != &inc.0
        || *kind != b'p'
        || stored_hash != &kh.0
    {
        return None;
    }
    let bucket = u64::from_be_bytes(*bucket);
    let first = u64::from_be_bytes(*first);
    let runs = decode_page_abs(first, value)?;
    let last = runs.last()?;
    let end = last.start.checked_add(u64::from(last.count))?;
    if bucket_of(first) != bucket || bucket_of(end.checked_sub(1)?) != bucket {
        return None;
    }
    Some(runs)
}

// ---- gather-side page builder ----------------------------------------

/// Builds every key's postings pages for ONE gather chunk (one stream's
/// contiguous frame walk, offsets strictly ascending). Emits pages
/// split at bucket boundaries; gap byte accounting is intra-page.
#[derive(Default)]
pub(crate) struct PageBuilder {
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

/// Every emitted page as `(rk_hash, bucket, page_first, encoded_value)`.
pub(crate) type Pages = Vec<(RoutingKeyHash, u64, u64, Vec<u8>)>;

impl PageBuilder {
    /// Account one canonical frame at `offset` with stored size
    /// `frame_bytes` for routing-key hash `key`. MUST be called in
    /// strictly ascending offset order across the whole chunk.
    pub(crate) fn note_frame(&mut self, key: RoutingKeyHash, offset: u64, frame_bytes: u64) {
        let walked_before = self.walked_bytes;
        self.walked_bytes = self.walked_bytes.saturating_add(frame_bytes);
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
        let page_full =
            acc.runs.len().saturating_mul(12).saturating_add(40) >= PAGE_MAX_ENCODED_BYTES;
        // A full page closes only where its open run ends, at a gap, so
        // the cap holds for keys whose runs never change bucket without
        // ever cutting one run in two. A key seen for the first time has
        // no runs yet, so its page is never full.
        let at_gap = offset != acc.run_next;
        if bucket != acc.bucket || (page_full && at_gap) {
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
                offset.saturating_sub(acc.page_first)
            } else {
                offset.saturating_sub(acc.run_next)
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
        acc.run_count = acc.run_count.saturating_add(1);
        acc.run_bytes = acc.run_bytes.saturating_add(frame_bytes);
        acc.run_next = offset.saturating_add(1);
        acc.walked_at_run_end = self.walked_bytes;
    }

    #[expect(
        clippy::expect_used,
        reason = "PageBuilder::close_run; a run is pushed the moment run_count leaves zero, so a non-zero count proves an open run; a fallible read would add a branch no counted run reaches"
    )]
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
    pub(crate) fn finish(mut self) -> (Pages, u64) {
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
                total = total.saturating_add(v.len() as u64);
                out.push((key, bucket, first, v));
            }
        }
        (out, total)
    }
}

// ---- read planner ------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanCfg {
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
pub(crate) struct Span {
    pub start: u64,
    /// Exclusive end offset.
    pub end: u64,
    pub matching_bytes: u64,
    /// Estimated TOTAL stored bytes the scan will read (matching plus
    /// coalesced gaps) — the amplification denominator's counterpart.
    pub scan_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Plan {
    pub spans: Vec<Span>,
    /// The read provably consumed matches up to here (exclusive): the
    /// caller's cursor advances to this even over match-free ranges.
    pub consumed_to: u64,
    /// Every run in the requested range was planned.
    pub complete: bool,
}

impl Plan {
    /// Budget exhausted BY a run. With prior spans the partial already made
    /// progress; with NONE, the first record must still be served (review
    /// blocker: a run fatter than the whole scan budget used to plan ZERO
    /// spans, so the read returned an empty page forever). Split the run to
    /// a bounded prefix — per-record estimate, never fewer than one record;
    /// the span executor's response budget truncates honestly if the
    /// estimate under-counts.
    fn serve_bounded_prefix(&mut self, r: &AbsRun, max_scan_bytes: u64) {
        if !self.spans.is_empty() {
            return;
        }
        // The run holds at least one record and per_rec is floored at one
        // byte, so neither division has a zero divisor.
        let per_rec = r
            .matching_bytes
            .checked_div(u64::from(r.count.max(1)))
            .unwrap_or(1)
            .max(1);
        let budget_recs = max_scan_bytes
            .checked_div(per_rec)
            .unwrap_or(1)
            .clamp(1, u64::from(r.count));
        let end = r.start.saturating_add(budget_recs);
        let bytes = per_rec.saturating_mul(budget_recs);
        self.spans.push(Span {
            start: r.start,
            end,
            matching_bytes: bytes,
            scan_bytes: bytes,
        });
        self.consumed_to = end;
    }
}

/// Plan bounded canonical spans over absolute runs (ascending, within
/// one requested range). Coalesces a following run into the current
/// span when the intervening gap is small in BYTES; otherwise opens a
/// new span. Stops at span/byte budgets with an honest partial.
#[cfg(test)]
pub(crate) fn plan_spans(runs: &[AbsRun], upto: u64, cfg: &PlanCfg) -> Plan {
    plan_spans_iter(runs.iter().copied(), upto, cfg)
}

pub(crate) fn plan_spans_iter(
    runs: impl IntoIterator<Item = AbsRun>,
    upto: u64,
    cfg: &PlanCfg,
) -> Plan {
    let mut plan = Plan {
        spans: Vec::new(),
        consumed_to: 0,
        complete: true,
    };
    let mut scan_total = 0u64;
    for r in runs {
        let r_end = r.start.saturating_add(u64::from(r.count));
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
            let scan = s.scan_bytes.saturating_add(gap).saturating_add(r_bytes) as f64;
            let matching = s.matching_bytes.saturating_add(r_bytes) as f64;
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
                    && scan_total
                        .saturating_add(if contiguous { 0 } else { r.gap_bytes_before })
                        .saturating_add(r_bytes)
                        <= cfg.max_scan_bytes
                    && amp_ok(s) =>
            {
                // Coalesce into the open span (cheap, ratio-safe gap).
                s.end = r_end;
                s.matching_bytes = s.matching_bytes.saturating_add(r_bytes);
                let gap = if contiguous { 0 } else { r.gap_bytes_before };
                s.scan_bytes = s.scan_bytes.saturating_add(gap).saturating_add(r_bytes);
                scan_total = scan_total.saturating_add(gap).saturating_add(r_bytes);
            }
            _ => {
                if plan.spans.len() >= cfg.max_spans {
                    plan.complete = false;
                    return plan;
                }
                if scan_total.saturating_add(r_bytes) > cfg.max_scan_bytes {
                    plan.serve_bounded_prefix(&r, cfg.max_scan_bytes);
                    plan.complete = false;
                    return plan;
                }
                plan.spans.push(Span {
                    start: r.start,
                    end: r_end,
                    matching_bytes: r_bytes,
                    scan_bytes: r_bytes,
                });
                scan_total = scan_total.saturating_add(r_bytes);
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
#[path = "postings/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "postings/codec_tests.rs"]
mod codec_tests;
