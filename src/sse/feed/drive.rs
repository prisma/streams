//! The permit-held drive and the feed's transition-retry task, moved
//! out of `feed.rs` verbatim.
use super::{DriveOutcome, InstallOutcome, Lifecycle, LiveFeed, SourceCutoff, SourceTransition};
use std::sync::Arc;
use std::sync::atomic::Ordering;

#[expect(
    clippy::unwrap_used,
    reason = "LiveFeed drive; a poisoned feed state may hold a partially published head, version or lifecycle; recovering it could re-read delivered records or settle a transition the feed already retired"
)]
impl LiveFeed {
    /// Round-11.1: ONE transient-transition retry scheduler per feed.
    /// A parked fan-out never creates a timer herd: the first session
    /// that observes an unresolved closed source arms one task; the
    /// task re-drives at 250 ms until the transition resolves, the
    /// subscribers leave, or the feed tears down. Resolution bumps
    /// the version, waking every parked session.
    #[expect(
        clippy::disallowed_methods,
        clippy::excessive_nesting,
        reason = "LiveFeed::schedule_transition_retry; the retry is a bare task the feed owns and cancels through its own flag, nesting the abandoned and superseded verdicts inside the wake it awaits; a supervised task would tie a feed-scoped retry to the runtime supervisor and flattening the verdicts would separate them from the wake"
    )]
    pub(crate) fn schedule_transition_retry(self: &Arc<Self>) {
        if self
            .retry_scheduled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        #[cfg(test)]
        self.retry_spawns.fetch_add(1, Ordering::Relaxed);
        let feed = self.clone();
        tokio::spawn(async move {
            // Bounded: 20 minutes of 250 ms attempts, far beyond any
            // real transition; sessions' own caps disconnect earlier.
            for _ in 0..4800u32 {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
                    _ = feed.cancel.cancelled() => break,
                }
                if feed.subscriber_count() == 0 || feed.cancel.is_fired() {
                    break;
                }
                match feed.drive_once().await {
                    Some(DriveOutcome::Idle)
                    | Some(DriveOutcome::NoProgress)
                    | Some(DriveOutcome::SourceFailed)
                    | None => {
                        let unresolved = feed.current_source().closed()
                            && matches!(feed.st.lock().unwrap().lifecycle, Lifecycle::Active);
                        if !unresolved {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            feed.retry_scheduled.store(false, Ordering::SeqCst);
        });
    }

    #[expect(
        clippy::excessive_nesting,
        clippy::let_underscore_must_use,
        reason = "LiveFeed::drive_under_permit; the drive nests the install and incompatibility verdicts inside the transition arm of the permit-held loop and re-publishes through a watch whose send fails only when every session is gone; flattening it or a handled send would separate the verdicts from the permit that serialises them"
    )]
    pub(super) async fn drive_under_permit(&self) -> DriveOutcome {
        let mut swap_attempts = 0u8;
        // Lifecycle outcomes are REPEATABLE (every parked session must
        // observe them on its own drive), but the version bump happens
        // only on the drive that performs the transition itself.
        let mut transitioned = false;
        let outcome = loop {
            let snap = self.source_snapshot();
            let src = snap.source.clone();
            let head = self.st.lock().unwrap().head;
            if head < src.frontier() {
                self.source_reads.fetch_add(1, Ordering::Relaxed);
                break self.read_and_publish(&src, head).await;
            }
            // Nothing durable beyond the head. A closed tail is either
            // a genuine collection close or a topology transition —
            // only the descriptor refresh (under THIS permit) decides
            // and installs the successor source (Stage 6.3).
            let lifecycle = self.st.lock().unwrap().lifecycle;
            match lifecycle {
                Lifecycle::Closed => break DriveOutcome::Closed,
                Lifecycle::Gone(reason) => break DriveOutcome::IncarnationClosed(reason),
                Lifecycle::Active => {}
            }
            if !src.closed() {
                break DriveOutcome::Idle;
            }
            match src.next_source().await {
                Ok(SourceTransition::NewSource(next)) => {
                    match self.install_source(next) {
                        // Validated continuation — installed by us, or
                        // already installed by a racing reconciliation
                        // (AlreadyCurrent is a LOST RACE, never an
                        // incarnation change): re-evaluate with the
                        // current source (its live tail may already
                        // have records for this head).
                        InstallOutcome::Installed | InstallOutcome::AlreadyCurrent => {
                            swap_attempts += 1;
                            if swap_attempts >= 4 {
                                break DriveOutcome::Idle;
                            }
                            continue;
                        }
                        // Incompatible topology: NOT a swap — sessions
                        // disconnect without a terminal control.
                        InstallOutcome::Incompatible => {
                            let mut st = self.st.lock().unwrap();
                            st.lifecycle = Lifecycle::Gone(SourceCutoff::IncompatibleTopology);
                            transitioned = true;
                            break DriveOutcome::IncarnationClosed(
                                SourceCutoff::IncompatibleTopology,
                            );
                        }
                    }
                }
                Ok(SourceTransition::GenuineClose) => {
                    let mut st = self.st.lock().unwrap();
                    st.lifecycle = Lifecycle::Closed;
                    transitioned = true;
                    break DriveOutcome::Closed;
                }
                Ok(SourceTransition::IncarnationChanged(reason)) => {
                    let mut st = self.st.lock().unwrap();
                    st.lifecycle = Lifecycle::Gone(reason);
                    transitioned = true;
                    break DriveOutcome::IncarnationClosed(reason);
                }
                Ok(SourceTransition::RetryLater) => break DriveOutcome::Idle,
                Err(_) => {
                    crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.fetch_add(1, Ordering::Relaxed);
                    break DriveOutcome::Idle;
                }
            }
        };
        // Bump the version EXACTLY when feed state actually changed
        // (findings 5+6): a delivery, a publication, a swap, or the
        // lifecycle transition ITSELF (its repeated observation is not
        // a change). Idle, no-progress and source failures changed
        // nothing — bumping would wake every parked session into
        // another immediate drive (the busy retry loop).
        let bump = match outcome {
            DriveOutcome::Solo { .. } | DriveOutcome::Published => true,
            DriveOutcome::Closed | DriveOutcome::IncarnationClosed(_) => transitioned,
            DriveOutcome::Idle
            | DriveOutcome::NoProgress
            | DriveOutcome::SourceFailed
            | DriveOutcome::Cancelled => false,
        };
        if bump {
            let ver = {
                let mut st = self.st.lock().unwrap();
                st.version += 1;
                st.version
            };
            crate::sse::auth::sse_stats::FEED_VERSION_BUMPS.fetch_add(1, Ordering::Relaxed);
            let _ = self.changed.send(ver);
        }
        outcome
    }
}
