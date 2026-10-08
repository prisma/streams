//! History reads over layout 5 pages. The absorber copies each shard-log
//! page byte for byte into the shard's shared history partition under
//! `route16 ‖ inc16 ‖ 'g' ‖ last offset` (history's `'p'` rows are postings
//! pages). A page is keyed by its LAST offset, so a forward scan from the
//! key of `from` meets first the page that holds `from`; a page holds at
//! most `PAGE_MAX_RECORDS` records, so the page holding `to - 1` sits below
//! the key of `to - 1 + PAGE_MAX_RECORDS`, and the scan stops at the first
//! page that starts at or after `to`.
//!
//! Every row is admitted against its incarnation's history page prefix
//! before anything looks at it, then clipped to the window: a read may
//! start or end inside a page, and serves only the records of the window.
//! A keyed read skips another routing key's page from its clear header,
//! without decrypting it; its records still count as consumed progress.
//! Nothing here decrypts: the read path opens the pages it is handed.

use std::ops::Range;
use std::sync::Arc;

use slatedb::Db;
use slatedb::config::ScanOptions;

use super::postings_read::execute_postings_plan;
use super::{POSTINGS_CORRUPT, postings_scan_opts};
use crate::crypto::{RouteHash, SegmentHash};
use crate::crypto_page::{CheckedPage, PAGE_MAX_RECORDS, history_page_key, history_page_prefix};
use crate::shard::record::{PageSlice, PageSlices, RecordCorruption};

/// How far past `to - 1` a window's scan may have to read to meet the page
/// holding `to - 1`.
const PAGE_SPAN: u64 = PAGE_MAX_RECORDS as u64;

/// Scan options for history reads: without readahead, slatedb fetches one
/// (compressed, ~200B) block per sequential GET — thousands of round-trips
/// per page on a 25ms store. 2MB readahead turns that into a few large GETs.
fn hist_scan_opts() -> ScanOptions {
    ScanOptions {
        read_ahead_bytes: 2 * 1024 * 1024,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..Default::default()
    }
}

/// One forward scan over a stream incarnation's history pages, yielding
/// each page that holds a record of the window `[from, to)`, clipped to it,
/// in offset order. It ends at the first page that starts at or after `to`.
///
/// Every window a read scans lies below the absorbed boundary, where the
/// pages tile the offsets: each offset is in exactly one page. Admission
/// checks one page against its own key only, so the scan checks the set:
/// each page it meets, another routing key's included, must start at the
/// offset after the last one (the first at `from`), and the pages must
/// reach `to`. An overlap would serve offsets twice and a lost page would
/// pass its records off as consumed progress; both fail the read.
pub(super) struct PageScan {
    rows: Option<slatedb::DbIterator>,
    prefix: [u8; 33],
    from: u64,
    to: u64,
    /// The offset the next page met must serve first.
    due: u64,
}

impl PageScan {
    /// Open the scan of `window` under `opts`. An empty window reads nothing.
    pub(super) async fn open(
        part: &Db,
        route: RouteHash,
        inc: SegmentHash,
        window: Range<u64>,
        opts: &ScanOptions,
    ) -> Result<Self, slatedb::Error> {
        let Range {
            start: from,
            end: to,
        } = window;
        let rows = if from < to {
            let bound = to.saturating_sub(1).saturating_add(PAGE_SPAN);
            let range = history_page_key(route, inc, from)..history_page_key(route, inc, bound);
            Some(part.scan_with_options(range, opts).await?)
        } else {
            None
        };
        Ok(Self {
            rows,
            prefix: history_page_prefix(route, inc),
            from,
            to,
            due: from,
        })
    }

    /// The next page holding a record of the window, clipped to it; None
    /// once the scan meets a page past the window or runs out of rows. A row
    /// that fails admission against its key fails the read, and so do pages
    /// that do not tile the window.
    pub(super) async fn next(&mut self) -> anyhow::Result<Option<PageSlice>> {
        let Some(rows) = self.rows.as_mut() else {
            return Ok(None);
        };
        let slice = match rows.next().await? {
            Some(row) => {
                let page = CheckedPage::from_row(&row.key, &self.prefix, row.value)
                    .map_err(RecordCorruption::Page)?;
                PageSlice::clip(page, self.from, self.to)
            }
            None => None,
        };
        let Some(slice) = slice else {
            self.rows = None;
            self.reached_end()?;
            return Ok(None);
        };
        Ok(Some(self.follow(slice)?))
    }

    /// `slice` if it serves first the offset due, which it then moves past.
    fn follow(&mut self, slice: PageSlice) -> Result<PageSlice, RecordCorruption> {
        if slice.first() != self.due {
            let page = slice.page();
            return Err(RecordCorruption::Misplaced {
                due: self.due,
                first: page.first(),
                last: page.last(),
            });
        }
        // A slice ends before `to`, so the offset after it never saturates.
        self.due = slice.last().saturating_add(1);
        Ok(slice)
    }

    /// Whether the pages the scan met reached the window's end.
    fn reached_end(&self) -> Result<(), RecordCorruption> {
        if self.due == self.to {
            Ok(())
        } else {
            Err(RecordCorruption::Missing {
                due: self.due,
                to: self.to,
            })
        }
    }

    /// Every page of the window, or only those of `key_filter`'s routing key,
    /// until the stored bytes of the pages inspected reach `max_bytes`; the
    /// page that reaches them is kept, so the first page always fits. Returns
    /// the pages, the last record inspected (matching or not: the consumed
    /// progress) and whether the scan reached the window's end.
    async fn collect(
        mut self,
        key_filter: Option<&str>,
        max_bytes: usize,
    ) -> anyhow::Result<(PageSlices, Option<u64>, bool)> {
        let mut pages = PageSlices::default();
        let (mut last, mut total) = (None, 0usize);
        while let Some(slice) = self.next().await? {
            total = total.saturating_add(slice.stored_len());
            last = Some(slice.last());
            if key_filter.is_none_or(|key| key == slice.page().routing_key()) {
                pages.push(slice);
            }
            if total >= max_bytes {
                return Ok((pages, last, false));
            }
        }
        Ok((pages, last, true))
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "read_history2; a history read names its partition, route, segment, key and offset window separately as the planner produced them; a query struct would repeat the same fields at every call"
)]
pub(crate) async fn read_history2(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    from: u64,
    upto: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> anyhow::Result<(PageSlices, Option<u64>, bool)> {
    match key_filter {
        Some(rk) => read_history2_keyed(part, route, inc, rk, from, upto, max_bytes).await,
        None => read_history2_scan(part, route, inc, from, upto, max_bytes).await,
    }
}

/// Unfiltered canonical scan (whole-segment replay): every page of the
/// window, clipped to it — the canonical rows ARE the stream.
#[expect(
    clippy::too_many_arguments,
    reason = "read_history2_scan; a history read names its partition, route, segment, key and offset window separately as the planner produced them; a query struct would repeat the same fields at every call"
)]
pub(super) async fn read_history2_scan(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(PageSlices, Option<u64>, bool)> {
    let pages = PageScan::open(part, route, inc, from..upto, &hist_scan_opts()).await?;
    pages.collect(None, max_bytes).await
}

/// Keyed read through the postings planner (ROUTING-V3 §3/§5): decode
/// the key's offset runs for the requested range, plan bounded
/// canonical spans (<= 8 per response, gap-coalesced by BYTES, 16 MiB
/// scan cap), execute each span as ONE canonical range scan, and
/// verify every page against the exact routing-key bytes — a 128-bit
/// rk-hash collision can add candidates, never another key's data.
///
/// `last` advances to `consumed_to - 1` even when a planned range holds
/// no matches, so cursors move over provably match-free ranges. The
/// per-offset GET pattern is structurally impossible here: reads are
/// range scans only.
///
/// Ranges with ZERO postings pages are read as holding no matches (the
/// greenfield layout; the covering-index fallback was deleted, see
/// docs/ROUTING-V3.md). The reader cannot tell a page lost after it was
/// durable from an absent key, so H11's missing-postings clause rests on
/// storage assumptions today: docs/dst/DST-EXPANSION-SPEC.md §9.12.2
/// records the open obligation.
#[expect(
    clippy::too_many_arguments,
    reason = "read_history2_keyed; a history read names its partition, route, segment, key and offset window separately as the planner produced them; a query struct would repeat the same fields at every call"
)]
async fn read_history2_keyed(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(PageSlices, Option<u64>, bool)> {
    use std::sync::atomic::Ordering::Relaxed;
    if from >= upto {
        return Ok((PageSlices::default(), None, true));
    }
    let kh = crate::postings::rk_hash(rk);
    // 1. Collect this key's pages for every bucket the range touches.
    // Greenfield layout (spec §12.4, postings_from = 0): the postings
    // index is authoritative for the WHOLE absorbed range — zero pages
    // means the range provably holds no matches and the cursor advances
    // over it. A page that fails to decode (or disagrees with its key)
    // is corruption: never claim completeness over an unverified range;
    // fall back to ONE bounded canonical envelope scan of the requested
    // range, filtered by exact key bytes (spec §8.6), and count it.
    let (lo, hi) = crate::postings::postings_range(route, inc, &kh, from, upto);
    let mut runs: Vec<crate::postings::AbsRun> = Vec::new();
    let mut corrupt = false;
    {
        let mut iter = part
            .scan_with_options(lo..hi, &postings_scan_opts())
            .await?;
        while let Some(kv) = iter.next().await? {
            if crate::postings::decode_stored_page(route, inc, &kh, &kv.key, &kv.value)
                .and_then(|page| crate::postings::append_page_runs(&mut runs, page))
                .is_none()
            {
                corrupt = true;
                break;
            }
        }
    }
    let admitted = (!corrupt)
        .then(|| crate::postings::ValidatedRuns::new(runs))
        .flatten();
    let Some(runs) = admitted else {
        POSTINGS_CORRUPT.fetch_add(1, Relaxed);
        return read_history2_keyed_envelope(part, route, inc, rk, from, upto, max_bytes).await;
    };
    let window = crate::postings::RunWindow::new(runs, from, upto);
    execute_postings_plan(part, route, inc, rk, window, upto, upto, max_bytes).await
}

/// Keyed read through the DECODED SLICE CACHE (spec §7): the engine's
/// cache resolves the runs (hit, single-flight cold load, or forward
/// extension), then the shared planner/executor below serves them.
/// `provable_to < upto` (a load window that could not reach the whole
/// range) yields an honest partial at the proven boundary.
#[expect(
    clippy::too_many_arguments,
    reason = "read_history2_keyed_cached; a keyed history read names its partition, route, segment, key and offset window separately as the planner produced them; a query struct would repeat the same fields at every call"
)]
pub(crate) async fn read_history2_keyed_cached(
    cache: &Arc<crate::postings_cache::PostingsCache>,
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    absorbed: u64,
    max_bytes: usize,
) -> anyhow::Result<(PageSlices, Option<u64>, bool)> {
    use std::sync::atomic::Ordering::Relaxed;
    if from >= upto {
        return Ok((PageSlices::default(), None, true));
    }
    let kh = crate::postings::rk_hash(rk);
    match cache
        .runs_for(part, route, inc, kh, from, upto, absorbed)
        .await?
    {
        crate::postings_cache::CacheRuns::Corrupt => {
            POSTINGS_CORRUPT.fetch_add(1, Relaxed);
            read_history2_keyed_envelope(part, route, inc, rk, from, upto, max_bytes).await
        }
        crate::postings_cache::CacheRuns::Runs { runs, provable_to } => {
            execute_postings_plan(part, route, inc, rk, runs, provable_to, upto, max_bytes).await
        }
    }
}

/// Corruption envelope (spec §8.6): one bounded canonical scan of the
/// requested range, filtered by EXACT routing-key bytes. Never lies
/// about completeness — a byte-truncated envelope returns an honest
/// partial with a resume cursor.
#[expect(
    clippy::too_many_arguments,
    reason = "read_history2_keyed_envelope; a history read names its partition, route, segment, key and offset window separately as the planner produced them; a query struct would repeat the same fields at every call"
)]
pub(super) async fn read_history2_keyed_envelope(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    from: u64,
    upto: u64,
    max_bytes: usize,
) -> anyhow::Result<(PageSlices, Option<u64>, bool)> {
    let pages = PageScan::open(part, route, inc, from..upto, &hist_scan_opts()).await?;
    let (pages, mut last, completed) = pages.collect(Some(rk), max_bytes).await?;
    if completed {
        // The whole range was verified page by page.
        last = Some(last.map_or(upto - 1, |l| l.max(upto - 1)));
    }
    Ok((pages, last, completed))
}

#[cfg(test)]
mod tests;
