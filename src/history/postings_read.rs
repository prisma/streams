//! Synchronous postings planning followed by an owner-free scan future.
use super::{Db, READ_SPANS_MAX, RouteHash, SegmentHash, canonical_span};
use std::sync::Arc;

#[expect(
    clippy::too_many_arguments,
    reason = "history planner; explicit physical coordinates and separate request/proof bounds; an options bag would hide their distinct contracts"
)]
#[expect(
    clippy::excessive_nesting,
    reason = "postings executor; synchronous planning must end before constructing the scan future; separating scan locals into context helpers would obscure ordered early termination"
)]
pub(super) fn execute_postings_plan<'a>(
    part: &'a Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    rk: &'a str,
    window: crate::postings::RunWindow,
    provable_to: u64,
    upto: u64,
    max_bytes: usize,
) -> impl std::future::Future<
    Output = anyhow::Result<(Vec<crate::shard::record::CheckedFrame>, Option<u64>, bool)>,
> + 'a {
    use std::sync::atomic::Ordering::Relaxed;
    // 3. Plan bounded spans.
    let cfg = crate::postings::PlanCfg {
        max_scan_bytes: (max_bytes as u64).min(crate::postings::PlanCfg::default().max_scan_bytes),
        ..Default::default()
    };
    let plan = crate::postings::plan_spans_iter(window.iter(), provable_to, &cfg);
    // Planning is synchronous. Do not pin an evicted cached run array while
    // asynchronous canonical scans or a slow reader wait for their result.
    drop(window);
    async move {
        let mut spans_used = 0u64;
        let mut frames: Vec<crate::shard::record::CheckedFrame> = Vec::new();
        let mut last: Option<u64> = None;
        let mut total = 0usize;
        let mut truncated = false;
        // 4. Execute each span as one canonical range scan with exact-key
        // verification.
        // Spec §8.4: bounded-concurrency span execution (max 4 in flight),
        // results assembled in span order — cold multi-span reads pay
        // max(RTT), not sum(RTT). Serial execution measured 2x the covering
        // baseline's cold p50 on the two-span batch-1 shape.
        {
            use futures_util::StreamExt;
            let mut results = futures_util::stream::iter(plan.spans.iter().copied().map(|span| {
                let part = part.clone();
                async move {
                    let result =
                        canonical_span::read(&part, route, inc, rk, span, max_bytes).await?;
                    anyhow::Ok((span, result.hits, result.truncated, result.last))
                }
            }))
            .buffered(4);
            'spans: while let Some(res) = results.next().await {
                let (span, hits, span_trunc, span_last) = res?;
                spans_used += 1;
                for (off, raw) in hits {
                    total += raw.len();
                    frames.push(raw);
                    last = Some(off);
                    if total >= max_bytes {
                        truncated = true;
                        break 'spans;
                    }
                }
                if span_trunc {
                    if let Some(scanned) = span_last {
                        last = Some(last.map_or(scanned, |l| l.max(scanned)));
                    }
                    // The span stopped mid-run: retain its actual scanned
                    // position, including valid filtered misses. Later span
                    // results are discarded; the caller resumes here.
                    truncated = true;
                    break 'spans;
                }
                // The span is fully consumed even if nothing matched (hash
                // collisions or clipping estimates): the cursor may advance.
                last = Some(last.map_or(span.end - 1, |l| l.max(span.end - 1)));
            }
        }
        READ_SPANS_MAX.fetch_max(spans_used, Relaxed);
        if truncated {
            return Ok((frames, last, false));
        }
        // 5. Cursor semantics: a complete plan consumed everything the
        // index PROVED — including any match-free tail — so the caller's
        // next page starts there. Completion is relative to the full
        // request: an index window short of `upto` is an honest partial.
        last = Some(last.map_or(plan.consumed_to.saturating_sub(1), |l| {
            l.max(plan.consumed_to.saturating_sub(1))
        }));
        Ok((frames, last, plan.complete && provable_to >= upto))
    }
}
