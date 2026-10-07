//! Bounded canonical span scan with exact routing-key selection, over the
//! history pages that hold the span's records (`page_read::PageScan`).
use super::*;
use crate::shard::record::PageSlice;
use page_read::PageScan;
pub(super) struct ResultPage {
    pub hits: Vec<PageSlice>,
    pub truncated: bool,
    pub last: Option<u64>,
    bytes: usize,
}
impl ResultPage {
    /// Inspect one page of the span. A page that would carry the result's
    /// stored bytes past `max_bytes` ends it (the first page always fits).
    /// Every inspected page is consumed progress; only `rk`'s pages are
    /// hits, and another key's page is skipped from its clear header.
    fn inspect(&mut self, page: PageSlice, rk: &str, max_bytes: usize) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        READ_FRAMES_SCANNED.fetch_add(1, Relaxed);
        if self.last.is_some() && self.bytes.saturating_add(page.stored_len()) > max_bytes {
            self.truncated = true;
            return false;
        }
        self.bytes += page.stored_len();
        self.last = Some(page.last());
        if page.page().routing_key() == rk {
            READ_FRAMES_MATCHED.fetch_add(1, Relaxed);
            self.hits.push(page);
        }
        true
    }
}
#[expect(
    clippy::too_many_arguments,
    reason = "read; the canonical span read takes the partition, route, incarnation, range, filter, budget and sink the gather planned separately; a request struct would restate the plan per span"
)]
pub(super) async fn read(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    span: crate::postings::Span,
    max_bytes: usize,
) -> anyhow::Result<ResultPage> {
    let mut result = ResultPage {
        hits: Vec::new(),
        truncated: false,
        last: None,
        bytes: 0,
    };
    let opts = slatedb::config::ScanOptions {
        read_ahead_bytes: (span.scan_bytes.saturating_mul(3) / 2).clamp(64 * 1024, 2 * 1024 * 1024)
            as usize,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..Default::default()
    };
    let mut pages = PageScan::open(part, route, inc, span.start..span.end, &opts).await?;
    while let Some(page) = pages.next().await? {
        if !result.inspect(page, rk, max_bytes) {
            return Ok(result);
        }
    }
    Ok(result)
}
