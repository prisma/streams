//! The frontier's step at one page (S3b-DESIGN §4.2, §5.3): what the
//! frontier does at the page it stands at, from the range row holding the
//! page and the clock. Pure, so that Kani checks it (KANI-110).
use super::range::RangeRow;

/// What the frontier does at a page it reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AtFrontier {
    /// A scheduled offset: pass it; its page goes too when the scheduled
    /// message is already resolved.
    Scheduled { resolved: bool },
    /// Cancelled untaken: pass it, its page and its bytes go.
    Cancelled,
    /// Untaken past its retention end: into the gap.
    Expired,
    /// Untaken and live: lease it, or stop.
    Live,
}

/// The frontier's step at the page of `offset` sent at `ts` (its header's
/// time, an immediate message's `availableAt`), from its range row and the
/// clock `t`.
pub(crate) fn at_frontier(
    range: &RangeRow,
    page: (u64, i64),
    retention_ms: i64,
    t: i64,
) -> AtFrontier {
    let (offset, ts) = page;
    if range.skip.contains(offset) {
        AtFrontier::Scheduled {
            resolved: !range.sbits.contains(offset),
        }
    } else if range.cancel.contains(offset) {
        AtFrontier::Cancelled
    } else if ts.saturating_add(retention_ms) <= t {
        AtFrontier::Expired
    } else {
        AtFrontier::Live
    }
}

#[cfg(kani)]
mod proofs;
