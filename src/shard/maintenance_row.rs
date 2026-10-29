//! The maintenance ledger of one physical shard (R25-A, layout 6: in
//! memory only): the delta the committer applies to it and its age signal.
use super::ShardMaintenance;
/// Why a maintenance delta is refused. Typed rather than an `anyhow` error
/// so the delta stays checkable by Kani (KANI-047); `Display` keeps the
/// words the logs and tests read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceError {
    /// The backlog plus the added bytes pass `u64::MAX`.
    Overflow,
    /// A retirement larger than the backlog: the two sides of the
    /// accounting have diverged.
    OverRetirement { retire: u64, available: u64 },
}

impl std::fmt::Display for MaintenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => f.write_str("maintenance byte overflow"),
            Self::OverRetirement { retire, available } => write!(
                f,
                "maintenance retirement exceeds backlog: retire={retire} available={available}"
            ),
        }
    }
}

impl std::error::Error for MaintenanceError {}

impl ShardMaintenance {
    /// Apply a committed delta. Retiring more than exists is an ERROR,
    /// not a saturation: it means the two sides of the accounting have
    /// diverged, and clamping would hide exactly the class of unit bug
    /// this type exists to prevent.
    pub(crate) fn apply_delta(
        self,
        added_frame_bytes: u64,
        retired_frame_bytes: u64,
        now_ms: i64,
    ) -> Result<Self, MaintenanceError> {
        let available = self
            .unabsorbed_frame_bytes
            .checked_add(added_frame_bytes)
            .ok_or(MaintenanceError::Overflow)?;
        let next =
            available
                .checked_sub(retired_frame_bytes)
                .ok_or(MaintenanceError::OverRetirement {
                    retire: retired_frame_bytes,
                    available,
                })?;
        let mut out = self;
        out.version = out.version.saturating_add(1);
        out.unabsorbed_frame_bytes = next;
        if next == 0 {
            out.backlog_started_ms = 0;
            out.last_progress_ms = 0;
        } else {
            // A backlog out of an empty ledger: its bytes were just added.
            if self.unabsorbed_frame_bytes == 0 {
                out.backlog_started_ms = now_ms;
                out.last_progress_ms = now_ms;
            }
            if retired_frame_bytes > 0 {
                out.last_progress_ms = now_ms;
            }
        }
        Ok(out)
    }

    /// Seconds since maintenance last made durable progress, while a
    /// backlog is outstanding. Zero when there is nothing to do, and when
    /// the clock reads before the last progress; never overflows, whatever
    /// the two clocks read (KANI-047).
    pub(crate) fn no_progress_secs(self, now_ms: i64) -> u64 {
        if self.unabsorbed_frame_bytes == 0 || self.last_progress_ms <= 0 {
            0
        } else {
            u64::try_from(now_ms.saturating_sub(self.last_progress_ms)).unwrap_or(0) / 1_000
        }
    }
}

#[cfg(kani)]
mod proofs;
