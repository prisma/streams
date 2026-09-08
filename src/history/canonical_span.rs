//! One scan/selection policy for cached and storage-backed canonical spans.
use super::*;
use crate::shard::record::CheckedFrame;
use span_cache::{Access, Scope};
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
#[allow(clippy::too_many_arguments)]
pub(super) async fn read(
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &str,
    span: crate::postings::Span,
    max_bytes: usize,
    scope: Option<&Arc<Scope>>,
    absorbed: u64,
) -> anyhow::Result<ResultPage> {
    let scope = scope.filter(|scope| scope.matches_read(part, route.0, inc.0));
    let mut result = ResultPage {
        hits: Vec::new(),
        truncated: false,
        last: None,
        bytes: 0,
    };
    let mut access = scope.map_or(Access::Bypass, |s| {
        s.acquire(span.start, span.end, absorbed, span.scan_bytes)
    });
    if let Access::Wait(waiter) = access {
        waiter.wait().await;
        // One coalesced wait; a cancelled/invalidated producer never forces an
        // unbounded retry loop. A second pending producer is a canonical bypass.
        access = scope.map_or(Access::Bypass, |s| {
            s.acquire(span.start, span.end, absorbed, span.scan_bytes)
        });
    }
    let mut capture = match access {
        Access::Hit(owner) => {
            for frame in owner.frames() {
                if !result.inspect(frame.clone(), rk, max_bytes) {
                    break;
                }
            }
            return Ok(result);
        }
        Access::Fill(capture) => Some(capture),
        Access::Wait(_) | Access::Bypass => None,
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
        if let Some(fill) = &mut capture
            && !fill.push(&frame)
        {
            capture = None;
        }
        if !result.inspect(frame, rk, max_bytes) {
            return Ok(result);
        }
    }
    if let Some(fill) = capture {
        fill.complete();
    }
    Ok(result)
}
