//! What a durable catch-up read that advanced nothing owes its pass.
//!
//! `serve`'s catch-up pass reads privately below a fixed bound; its answer
//! to a read that moved nothing lives here so the pass meets every shape at
//! one exhaustive match, outside the pass's size and nesting exceptions.
use crate::sse::feed::SourceBatch;

/// What the pass does next after a read that advanced nothing.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Stall {
    /// A fatal span cutoff, counted and logged here: the session
    /// disconnects without a terminal control.
    Cutoff,
    /// The read failed and its wait passed: the same snapshot is read
    /// again at the same bound.
    Failed,
    /// The page advanced nothing: the pass hands the session to the live
    /// loop.
    NoProgress,
}

/// One owner for a stalled catch-up read's counters, log and wait.
pub(super) async fn stalled(read: anyhow::Result<SourceBatch>) -> Stall {
    // This source's spans are exhausted below the bound
    // (a swap happened mid-catch-up): the live loop
    // re-snapshots and, if the ring moved, re-catches-up
    // through the 'handoff path.
    let Err(e) = read else {
        return Stall::NoProgress;
    };
    // Round-11.2: fatal span outcomes disconnect
    // with the typed reason (no terminal) instead
    // of retrying forever.
    if let Some(cut) = e.downcast_ref::<crate::sse::source::FatalSpanCutoff>() {
        super::count_cutoff(cut.0);
        crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(reason = ?cut.0, "livefeed catch-up fatal cutoff");
        return Stall::Cutoff;
    }
    // Source failure mid-catch-up: bounded backoff,
    // then retry the SAME bound — never a hot loop
    // (finding 6 discipline applies here too).
    crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Stall::Failed
}
