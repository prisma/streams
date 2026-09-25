//! What a durable catch-up read that advanced nothing owes its pass.
//!
//! `serve`'s catch-up pass reads privately below a bound taken from the
//! feed head, so the frontier of every snapshot it reads is at or past the
//! bound: an empty page there is a hole the read could not explain yet,
//! never the snapshot's end. Nothing re-drives the pass but its own next
//! read (no version bump, no park), so each read that advanced nothing
//! waits here before the next one. The answer lives here so the pass meets
//! every shape at one exhaustive match, outside its size and nesting
//! exceptions.
use crate::sse::feed::{SourceBatch, SourceReadError};
use std::time::Duration;

/// One re-read per 100 ms bounds what a failing or hole-bearing store
/// costs a session in catch-up. The live phase backs off on its own
/// (`read_retry`): that retry shortens a park other wakes may end first,
/// and this pass has no park.
const RETRY: Duration = Duration::from_millis(100);

/// What the pass does next after a read that advanced nothing.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Stall {
    /// A fatal read cutoff, counted and logged here: the session
    /// disconnects without a terminal control.
    Cutoff,
    /// The read failed and its wait passed: the same snapshot is read
    /// again at the same bound.
    Failed,
    /// The page advanced nothing and its wait passed: the pass hands the
    /// session to the live loop, which serves a cursor the ring covers and
    /// sends one below the ring's floor back to catch-up at the current
    /// head, now at most once per wait.
    NoProgress,
}

/// One owner for a stalled catch-up read's counters, log and wait.
pub(super) async fn stalled(read: Result<SourceBatch, SourceReadError>) -> Stall {
    let cause = match read {
        Ok(page) => {
            crate::sse::auth::sse_stats::FEED_NO_PROGRESS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::debug!(
                cursor = page.scan_from,
                "livefeed catch-up page made no progress; handing over after a bounded wait"
            );
            tokio::time::sleep(RETRY).await;
            return Stall::NoProgress;
        }
        // Round-11.2: fatal read outcomes disconnect
        // with the typed reason (no terminal) instead
        // of retrying forever.
        Err(SourceReadError::Fatal(cut)) => {
            super::count_cutoff(cut);
            crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!(reason = ?cut, "livefeed catch-up fatal cutoff");
            return Stall::Cutoff;
        }
        Err(SourceReadError::Retryable(cause)) => cause,
    };
    // Source failure mid-catch-up: bounded backoff,
    // then retry the SAME bound — never a hot loop
    // (finding 6 discipline applies here too).
    crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tracing::debug!(
        error = %format_args!("{cause:#}"),
        "livefeed catch-up read failed; retrying the same bound after a bounded wait"
    );
    tokio::time::sleep(RETRY).await;
    Stall::Failed
}
