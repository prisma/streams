//! Bounded canonical span scan with exact routing-key selection.
use super::*;
use crate::shard::record::CheckedFrame;
pub(super) struct ResultPage {
    pub hits: Vec<(u64, CheckedFrame)>,
    pub truncated: bool,
    pub last: Option<u64>,
    bytes: usize,
}
impl ResultPage {
    fn inspect(&mut self, frame: CheckedFrame, rk: &str, max_bytes: usize) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        READ_FRAMES_SCANNED.fetch_add(1, Relaxed);
        if self.last.is_some() && self.bytes.saturating_add(frame.len()) > max_bytes {
            self.truncated = true;
            return false;
        }
        self.bytes += frame.len();
        let off = frame.view().header.offset;
        self.last = Some(off);
        if frame.view().header.routing_key == rk {
            READ_FRAMES_MATCHED.fetch_add(1, Relaxed);
            self.hits.push((off, frame));
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
    let prefix = hist2_record_key(route, inc, 0);
    let range = hist2_record_key(route, inc, span.start)..hist2_record_key(route, inc, span.end);
    let opts = slatedb::config::ScanOptions {
        read_ahead_bytes: (span.scan_bytes.saturating_mul(3) / 2).clamp(64 * 1024, 2 * 1024 * 1024)
            as usize,
        max_fetch_tasks: 2,
        cache_blocks: true,
        ..Default::default()
    };
    let mut iter = part.scan_with_options(range, &opts).await?;
    while let Some(kv) = iter.next().await? {
        let frame = CheckedFrame::from_row(&kv.key, &prefix[..33], kv.value)?;
        if !result.inspect(frame, rk, max_bytes) {
            return Ok(result);
        }
    }
    Ok(result)
}
