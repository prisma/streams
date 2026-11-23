//! The directory's side of a planned move (S5 addendum F6 rev 2, step 4;
//! GATE-DECISION-f6-prep-rows-rev2-2026-11-17 H5): a resolve waits on the move holding its prefix
//! (`sharddir::handoff`) before it reads the owner, and the move takes its resident out of the
//! serving map without the anti-flap holdoff, so a dropped move can reopen the shard at once.
//!
//! A served request that waits here, on a move holding its prefix or on an open this instance
//! must make or join, takes a place among the requests waiting on an owner and leaves the write
//! gate while it does (`admission::park::waits_on_owner`; §22 rev 2, K1-K2c).

use std::sync::Arc;

use tokio::time::Instant;

use super::{RetirementReason, ShardDirectory};
use crate::shard::ShardEngine;
use crate::sharddir::Retirement;
use crate::sharddir::handoff::Mark;

impl ShardDirectory {
    /// The owner `resolve` routes `prefix` to (`None`: this instance), read once the planned move
    /// holding `prefix`, if one does, ended or passed its hold. A served request that waits on that
    /// move, or that `resolve` then has to open `prefix` for, waits on its owner (module comment).
    pub(super) async fn owner_after_handoff(&self, prefix: &str) -> Option<String> {
        if self.inner.handoffs.holds(prefix) {
            crate::admission::park::waits_on_move(prefix);
        }
        self.inner.handoffs.wait(prefix).await;
        let owner = self.inner.ownership.foreign_owner(prefix);
        if owner.is_none() && !self.is_open(prefix) {
            crate::admission::park::waits_on_owner();
        }
        owner
    }

    /// Tests only: whether a planned move holds `prefix` now.
    #[cfg(test)]
    pub(crate) fn handing_off(&self, prefix: &str) -> bool {
        self.inner.handoffs.holds(prefix)
    }

    /// Step 4 of a planned move: hold `prefix` until `until` at the latest, and take its resident
    /// out of the serving map, open, with no holdoff and no strike (`RetirementReason::PlannedMove`;
    /// the gate keeps its shutdown handle, so a reopen here waits for its close). The hold and the
    /// resident, if there was one; `None` while another move holds `prefix`.
    pub(crate) fn begin_handoff(
        &self,
        prefix: &str,
        until: Instant,
    ) -> Option<(Mark, Option<Arc<ShardEngine>>)> {
        let mark = self.inner.handoffs.begin(prefix, until)?;
        let reason = RetirementReason::PlannedMove;
        let engine = match self.inner.gate.retire_resident(prefix, reason, |_, _| true) {
            Retirement::Retired(engine) => Some(engine),
            Retirement::Kept | Retirement::Absent => None,
        };
        Some((mark, engine))
    }

    /// Seal this instance against opens for a planned drain, if it holds nothing (the drain
    /// addendum, G1-G2, `sharddir::drain`). Whether it sealed.
    pub(crate) fn seal_for_drain(&self) -> bool {
        self.inner.gate.seal_for_drain()
    }

    /// Start no more opens, whatever this instance still holds: a planned drain whose handoff
    /// phase ran out (G3), before it announces.
    pub(crate) fn stop_opens(&self) {
        self.inner.gate.stop();
    }

    /// A drain whose handoff phase ran out gives up `held`, the shards it still holds: each is
    /// taken by fence or by this instance's own yield, not by a move, and counts as a writer
    /// fenced ahead of a move (G3, G6).
    pub(crate) fn note_evacuated(&self, held: &[String]) {
        for prefix in held {
            tracing::warn!(shard = %prefix, "planned drain: evacuated, not handed off");
            self.inner.handoffs.note_fenced();
        }
    }

    /// Writers fenced ahead of a planned move on this instance (G6).
    pub(crate) fn fenced_ahead(&self) -> u64 {
        self.inner.handoffs.fenced()
    }

    /// Tests: the shards this instance retired because their ownership moved, its ticks' yields
    /// (Addendum 27).
    #[cfg(test)]
    pub(crate) fn tick_yields(&self) -> u64 {
        self.inner.handoffs.yielded()
    }
}

/// An engine of `prefix` (`incarnation`) closed: count it as fenced ahead of a planned move when
/// its storage closed fenced while this instance's view named it the owner (G6). Judged before the
/// eviction drops the resident. Once the gate is stopped nothing is counted here: a drain's
/// evacuation counted what it gave up itself ([`ShardDirectory::note_evacuated`]).
pub(super) fn judge_fence(
    dir: &super::DirInner,
    prefix: &str,
    incarnation: crate::sharddir::EngineIncarnation,
) {
    let ahead = dir.ownership.foreign_owner(prefix).is_none() && !dir.gate.sealed();
    if ahead && dir.gate.resident_fenced(prefix, incarnation) {
        tracing::warn!(shard = %prefix, "a writer here was fenced ahead of a planned move");
        dir.handoffs.note_fenced();
    }
}
