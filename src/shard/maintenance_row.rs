//! The durable maintenance row of one physical shard (R25-A): its value,
//! the delta the committer applies to it, its age signal and its codec.
use super::ShardMaintenance;
/// Why a maintenance row or delta is refused. Typed rather than an
/// `anyhow` error so the codec and the delta stay checkable by Kani
/// (KANI-047); `Display` keeps the words the logs and tests read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceError {
    /// The backlog plus the added bytes pass `u64::MAX`.
    Overflow,
    /// A retirement larger than the backlog: the two sides of the
    /// accounting have diverged.
    OverRetirement { retire: u64, available: u64 },
    /// A stored row of a layout this build does not read.
    UnsupportedRow(usize),
}

impl std::fmt::Display for MaintenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => f.write_str("maintenance byte overflow"),
            Self::OverRetirement { retire, available } => write!(
                f,
                "maintenance retirement exceeds backlog: retire={retire} available={available}"
            ),
            Self::UnsupportedRow(len) => {
                write!(f, "unsupported shard maintenance row ({len} bytes)")
            }
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
            if self.unabsorbed_frame_bytes == 0 && added_frame_bytes > 0 {
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

const SHARD_MAINT_V2: u8 = 2;

pub(crate) fn encode_shard_maint(m: &ShardMaintenance) -> [u8; 40] {
    let mut v = [0u8; 40];
    v[0] = SHARD_MAINT_V2;
    v[8..16].copy_from_slice(&m.version.to_le_bytes());
    v[16..24].copy_from_slice(&m.unabsorbed_frame_bytes.to_le_bytes());
    v[24..32].copy_from_slice(&m.backlog_started_ms.to_le_bytes());
    v[32..40].copy_from_slice(&m.last_progress_ms.to_le_bytes());
    v
}

/// Row classification (R26-4). The R24 row was 16 untagged PAYLOAD-unit
/// bytes: on compressible data it overstates the frame backlog, but on
/// small incompressible frames the encoding overhead (headers, auth
/// tag) makes frames LARGER than payload, so it can also understate —
/// and an understated ledger makes the first exact retirement look like
/// over-retirement, which the checked accounting refuses forever. The
/// legacy value is therefore never trusted as frame bytes in either
/// direction: the opener rebuilds from the durable tails instead.
pub(crate) enum ShardMaintRow {
    Exact(ShardMaintenance),
    LegacyPayloadUnit,
}

pub(crate) fn decode_shard_maint_row(v: &[u8]) -> Result<ShardMaintRow, MaintenanceError> {
    if v.len() == 16 {
        return Ok(ShardMaintRow::LegacyPayloadUnit);
    }
    let row: &[u8; 40] = v
        .try_into()
        .map_err(|_| MaintenanceError::UnsupportedRow(v.len()))?;
    if row[0] != SHARD_MAINT_V2 {
        return Err(MaintenanceError::UnsupportedRow(v.len()));
    }
    let field = |at: usize| {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&row[at..at + 8]);
        bytes
    };
    Ok(ShardMaintRow::Exact(ShardMaintenance {
        version: u64::from_le_bytes(field(8)),
        unabsorbed_frame_bytes: u64::from_le_bytes(field(16)),
        backlog_started_ms: i64::from_le_bytes(field(24)),
        last_progress_ms: i64::from_le_bytes(field(32)),
    }))
}

/// Strict v2 decode: rows written by THIS build. A legacy 16-byte row
/// is an error here — callers that can meet one go through
/// `decode_shard_maint_row` and the rebuild path.
#[cfg(test)]
pub(crate) fn decode_shard_maint(v: &[u8]) -> anyhow::Result<ShardMaintenance> {
    match decode_shard_maint_row(v)? {
        ShardMaintRow::Exact(m) => Ok(m),
        ShardMaintRow::LegacyPayloadUnit => {
            anyhow::bail!("legacy payload-unit maintenance row; rebuild required")
        }
    }
}

#[cfg(kani)]
mod proofs;
