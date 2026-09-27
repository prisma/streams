//! Kani proofs for the maintenance summaries: KANI-047. The shard's row
//! (`encode_shard_maint`, `decode_shard_maint_row`) and each stream's dirty
//! row (`dirty_value`, `decode_dirty_value`) round-trip every value, decode
//! every supported layout exactly and refuse every other length or version
//! rather than reading it as zero debt; `apply_delta` moves the backlog by
//! exactly its added and retired bytes and refuses overflow and
//! over-retirement instead of clearing live debt; `no_progress_secs` never
//! overflows, whatever the two clocks read. Rows are symbolic bytes of every
//! length up to one past the longest layout; counters and clocks are full
//! width.
use super::{
    MaintenanceError, ShardMaintRow, ShardMaintenance, decode_shard_maint_row, encode_shard_maint,
};
use crate::shard::{StreamMaintenance, decode_dirty_value, dirty_value};

fn any_state() -> ShardMaintenance {
    ShardMaintenance {
        version: kani::any(),
        unabsorbed_frame_bytes: kani::any(),
        backlog_started_ms: kani::any(),
        last_progress_ms: kani::any(),
    }
}

/// KANI-047: the shard row round-trips every value; a 16-byte row is the
/// legacy payload-unit marker; every other length, and a 40-byte row of
/// another version, is refused.
#[kani::proof]
#[kani::unwind(50)]
fn kani_047_a_shard_row_decodes_exactly_or_is_refused() {
    let state = any_state();
    let row = encode_shard_maint(&state);
    assert!(
        matches!(decode_shard_maint_row(&row), Ok(ShardMaintRow::Exact(back)) if back == state),
        "a shard row round-trips"
    );
    let bytes: [u8; 41] = kani::any();
    let len: usize = kani::any_where(|len: &usize| *len <= 41);
    let stored = &bytes[..len];
    let decoded = decode_shard_maint_row(stored);
    match len {
        16 => assert!(
            matches!(decoded, Ok(ShardMaintRow::LegacyPayloadUnit)),
            "a 16-byte row is the legacy marker"
        ),
        40 if stored[0] == 2 => assert!(decoded.is_ok(), "a version-2 row decodes"),
        _ => assert!(
            matches!(decoded, Err(MaintenanceError::UnsupportedRow(refused)) if refused == len),
            "an unsupported row is refused"
        ),
    }
    kani::cover!(
        len == 40 && stored[0] != 2,
        "a 40-byte row of another version"
    );
}

/// KANI-047: a stream's dirty row round-trips every value; its 16- and
/// 24-byte legacy layouts decode their complete fields with the rest zero;
/// every other length is refused.
#[kani::proof]
#[kani::unwind(50)]
fn kani_047_a_dirty_row_decodes_exactly_or_is_refused() {
    let state = StreamMaintenance {
        absorbed: kani::any(),
        next: kani::any(),
        unabsorbed_bytes: kani::any(),
        oldest_unabsorbed_ms: kani::any(),
    };
    assert!(
        decode_dirty_value(&dirty_value(&state)) == Some(state),
        "a dirty row round-trips"
    );
    let bytes: [u8; 33] = kani::any();
    let len: usize = kani::any_where(|len: &usize| *len <= 33);
    let stored = &bytes[..len];
    let field = |at: usize| {
        let mut word = [0u8; 8];
        word.copy_from_slice(&bytes[at..at + 8]);
        word
    };
    let expected = matches!(len, 16 | 24 | 32).then(|| StreamMaintenance {
        absorbed: u64::from_le_bytes(field(0)),
        next: u64::from_le_bytes(field(8)),
        unabsorbed_bytes: if len >= 24 {
            u64::from_le_bytes(field(16))
        } else {
            0
        },
        oldest_unabsorbed_ms: if len == 32 {
            i64::from_le_bytes(field(24))
        } else {
            0
        },
    });
    assert!(
        decode_dirty_value(stored) == expected,
        "a dirty row decodes its complete fields or is refused"
    );
    kani::cover!(len == 24, "a 24-byte legacy row");
}

/// KANI-047: a delta moves the backlog by exactly its added and retired
/// bytes, bumps the version without wrapping, and clears the clocks when the
/// backlog empties; one that would overflow or retire more than the backlog
/// holds is refused, so no delta clears live debt.
#[kani::proof]
fn kani_047_a_delta_never_clears_live_debt() {
    let state = any_state();
    let (added, retired, now): (u64, u64, i64) = (kani::any(), kani::any(), kani::any());
    let available = state.unabsorbed_frame_bytes.checked_add(added);
    match state.apply_delta(added, retired, now) {
        Ok(next) => {
            assert!(
                available
                    .is_some_and(|a| a >= retired && next.unabsorbed_frame_bytes == a - retired),
                "a delta moves the backlog by exactly its bytes"
            );
            assert!(
                next.version == state.version.saturating_add(1),
                "a delta bumps the version without wrapping"
            );
            if next.unabsorbed_frame_bytes == 0 {
                assert!(
                    (next.backlog_started_ms, next.last_progress_ms) == (0, 0),
                    "an empty backlog clears its clocks"
                );
            }
        }
        Err(error) => {
            let expected = match available {
                None => MaintenanceError::Overflow,
                Some(available) => MaintenanceError::OverRetirement {
                    retire: retired,
                    available,
                },
            };
            assert!(
                available.is_none_or(|a| a < retired) && error == expected,
                "retiring more than the backlog holds, or overflowing it, is refused"
            );
        }
    }
    kani::cover!(
        available.is_some_and(|a| a == retired && a > 0),
        "a delta drains the whole backlog"
    );
}

/// KANI-047: the stall signal never overflows, whatever the two clocks
/// read, and is zero with no backlog or a clock before the last progress.
#[kani::proof]
fn kani_047_the_stall_signal_survives_clock_extremes() {
    let state = any_state();
    let now: i64 = kani::any();
    let secs = state.no_progress_secs(now);
    if state.unabsorbed_frame_bytes == 0 || now <= state.last_progress_ms {
        assert!(secs == 0, "no backlog or no elapsed time is no stall");
    }
    assert!(
        secs <= i64::MAX.unsigned_abs() / 1_000,
        "the stall signal is bounded"
    );
    kani::cover!(
        state.last_progress_ms > 0 && now == i64::MIN,
        "a clock at i64::MIN"
    );
    kani::cover!(
        state.unabsorbed_frame_bytes > 0 && state.last_progress_ms == 1 && now == i64::MAX,
        "the widest elapsed time"
    );
}
