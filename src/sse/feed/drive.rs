//! The permit-held drive: settling the tail and reading it are two
//! steps with two owners.
//!
//! `tail()` decides what lies at the head WITHOUT reading: it resolves
//! a closed tail (genuine close, successor install, incarnation
//! cutoff) and has no record-bearing outcome. Reading is a
//! SUBSCRIBER's act - a solo read hands its records to the caller and
//! moves the head past them - so only `drive_under_permit`, entered
//! through a session's `drive_once`, turns `Tail::Readable` into a
//! read. The feed's own retry task enters through
//! `transition_pending` and can reach `tail()` alone.
use super::{
    DriveOutcome, FeedSourceRead, InstallOutcome, Lifecycle, LiveFeed, SourceCutoff,
    SourceTransition,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// What the permit holder finds at the head BEFORE any read.
enum Tail {
    /// Durable records lie beyond the head; only a subscriber's drive
    /// may read them.
    Readable {
        src: Arc<dyn FeedSourceRead>,
        head: u64,
    },
    /// Open source, nothing beyond the head.
    Open,
    /// Closed source whose transition did not settle on this attempt:
    /// still in flight, the refresh failed, or one hold's swap budget
    /// ran out.
    Unresolved,
    Closed,
    Gone(SourceCutoff),
}

#[expect(
    clippy::unwrap_used,
    reason = "LiveFeed drive; a poisoned feed state may hold a partially published head, version or lifecycle; recovering it could re-read delivered records or settle a transition the feed already retired"
)]
impl LiveFeed {
    /// Round-11.1: ONE transient-transition retry scheduler per feed.
    /// A parked fan-out never creates a timer herd: the first session
    /// that observes an unresolved closed source arms one task; the
    /// task re-SETTLES at 250 ms - it never reads - until the
    /// transition settles, the subscribers leave, or the feed tears
    /// down.
    #[expect(
        clippy::disallowed_methods,
        clippy::excessive_nesting,
        reason = "LiveFeed::schedule_transition_retry; the retry is a bare task the feed owns and cancels through its own flag, nesting the abandoned and settled verdicts inside the wake it awaits; a supervised task would tie a feed-scoped retry to the runtime supervisor and flattening the verdicts would separate them from the wake"
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
                if !feed.transition_pending().await {
                    break;
                }
            }
            feed.retry_scheduled.store(false, Ordering::SeqCst);
        });
    }

    /// The retry task's ONLY entry: one permit-held attempt to settle
    /// the tail, never a read. `true` = tick again: the transition is
    /// still in flight, a subscriber holds the permit, or a closed
    /// tail still has records its subscribers have not read. A
    /// readable tail is announced on the version watch AFTER the
    /// permit is free: a session whose own drive lost the permit to
    /// this attempt parked expecting a publication, and no other wake
    /// is owed to it.
    ///
    /// The announcement REPEATS every tick while a closed tail stays
    /// unread, and that is deliberate, not an oversight to deduplicate.
    /// A closed source never fires its advance notification again, so
    /// a session whose read of that tail failed has parked with nothing
    /// else to wake it: this tick is the only retry of that read. It is
    /// bounded (250 ms, the loop's 4800 ticks, and each session's own
    /// retry cap), and it stops the moment a subscriber reads the tail.
    async fn transition_pending(&self) -> bool {
        let Some(permit) = self.acquire_permit() else {
            return true;
        };
        let tail = self.tail().await;
        drop(permit);
        match tail {
            Tail::Readable { src, .. } => {
                self.bump_version();
                src.closed()
            }
            Tail::Unresolved => true,
            Tail::Open | Tail::Closed | Tail::Gone(_) => false,
        }
    }

    /// The subscriber's drive: the only place a settled tail becomes
    /// a read.
    pub(super) async fn drive_under_permit(&self) -> DriveOutcome {
        let outcome = match self.tail().await {
            Tail::Readable { src, head } => {
                self.source_reads.fetch_add(1, Ordering::Relaxed);
                self.read_and_publish(&src, head).await
            }
            Tail::Open | Tail::Unresolved => DriveOutcome::Idle,
            Tail::Closed => DriveOutcome::Closed,
            Tail::Gone(reason) => DriveOutcome::IncarnationClosed(reason),
        };
        // A delivery or a publication changed feed state (findings
        // 5+6). A lifecycle transition bumped at the transition itself
        // (`retire`); its repeated observation, Idle, no-progress and
        // source failures changed nothing - bumping would wake every
        // parked session into another immediate drive.
        if matches!(outcome, DriveOutcome::Solo { .. } | DriveOutcome::Published) {
            self.bump_version();
        }
        outcome
    }

    /// Settle the tail under the permit. Nothing durable beyond the
    /// head means a closed tail is either a genuine collection close
    /// or a topology transition - only the descriptor refresh (under
    /// THIS permit) decides and installs the successor (Stage 6.3). A
    /// validated install - ours, or a racing reconciliation's
    /// (AlreadyCurrent is a LOST RACE, never an incarnation change) -
    /// re-evaluates against the current source, whose live tail may
    /// already have records for this head; four swaps in one hold is
    /// a storm the next attempt continues.
    async fn tail(&self) -> Tail {
        for _ in 0..4u8 {
            let src = self.source_snapshot().source;
            let (head, lifecycle) = {
                let st = self.st.lock().unwrap();
                (st.head, st.lifecycle)
            };
            if head < src.frontier() {
                return Tail::Readable { src, head };
            }
            match lifecycle {
                Lifecycle::Closed => return Tail::Closed,
                Lifecycle::Gone(reason) => return Tail::Gone(reason),
                Lifecycle::Active => {}
            }
            if !src.closed() {
                return Tail::Open;
            }
            let next = match src.next_source().await {
                Ok(SourceTransition::NewSource(next)) => next,
                Ok(SourceTransition::GenuineClose) => {
                    self.retire(Lifecycle::Closed);
                    return Tail::Closed;
                }
                Ok(SourceTransition::IncarnationChanged(reason)) => {
                    self.retire(Lifecycle::Gone(reason));
                    return Tail::Gone(reason);
                }
                Ok(SourceTransition::RetryLater) => return Tail::Unresolved,
                Err(_) => {
                    crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.fetch_add(1, Ordering::Relaxed);
                    return Tail::Unresolved;
                }
            };
            match self.install_source(next) {
                InstallOutcome::Installed | InstallOutcome::AlreadyCurrent => {}
                // Incompatible topology: NOT a swap - sessions
                // disconnect without a terminal control.
                InstallOutcome::Incompatible => {
                    self.retire(Lifecycle::Gone(SourceCutoff::IncompatibleTopology));
                    return Tail::Gone(SourceCutoff::IncompatibleTopology);
                }
            }
        }
        Tail::Unresolved
    }

    /// The lifecycle transition ITSELF is the state change: it bumps
    /// the version exactly once, here; its later observations do not.
    fn retire(&self, to: Lifecycle) {
        self.st.lock().unwrap().lifecycle = to;
        self.bump_version();
    }

    #[expect(
        clippy::let_underscore_must_use,
        reason = "LiveFeed::bump_version; the version watch fails to send only when every session is gone; a handled result would only restate that nobody waits"
    )]
    fn bump_version(&self) {
        let ver = {
            let mut st = self.st.lock().unwrap();
            st.version += 1;
            st.version
        };
        crate::sse::auth::sse_stats::FEED_VERSION_BUMPS.fetch_add(1, Ordering::Relaxed);
        let _ = self.changed.send(ver);
    }
}
