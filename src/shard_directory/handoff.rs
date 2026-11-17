//! The directory's side of a planned move (S5 addendum F6 rev 2, step 4;
//! GATE-DECISION-f6-prep-rows-rev2-2026-11-17 H5): a resolve waits on the move holding its prefix
//! (`sharddir::handoff`) before it reads the owner, and the move takes its resident out of the
//! serving map without the anti-flap holdoff, so a dropped move can reopen the shard at once.

use std::sync::Arc;

use tokio::time::Instant;

use super::{RetirementReason, ShardDirectory};
use crate::shard::ShardEngine;
use crate::sharddir::Retirement;
use crate::sharddir::handoff::Mark;

impl ShardDirectory {
    /// The owner `resolve` routes `prefix` to (`None`: this instance), read once the planned move
    /// holding `prefix`, if one does, ended or passed its hold.
    pub(super) async fn owner_after_handoff(&self, prefix: &str) -> Option<String> {
        self.inner.handoffs.wait(prefix).await;
        self.inner.ownership.foreign_owner(prefix)
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
}
