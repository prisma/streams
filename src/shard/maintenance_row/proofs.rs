//! Kani proofs for the shard's maintenance ledger: KANI-047. Layout 6
//! keeps the ledger in memory only (rebuilt from the tails at open), so no
//! row codec is left: `apply_delta` moves the backlog by exactly its added
//! and retired bytes and refuses overflow and over-retirement instead of
//! clearing live debt; `no_progress_secs` never overflows, whatever the two
//! clocks read. Counters and clocks are full width.
use super::{MaintenanceError, ShardMaintenance};

fn any_state() -> ShardMaintenance {
    ShardMaintenance {
        version: kani::any(),
        unabsorbed_frame_bytes: kani::any(),
        backlog_started_ms: kani::any(),
        last_progress_ms: kani::any(),
    }
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
