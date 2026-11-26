#![cfg(test)]
//! The leave's phase clock (Addendum 41 B item 3, the draft §4).

use std::time::Duration;

use super::{LEAVE_PHASE, Leave};

/// The phase runs over leaving passes on this instance's monotonic clock, not while its clock is
/// held (no successor, or a held source of the live intent), and is over at `LEAVE_PHASE`, eight
/// moves at their bound; a pass that is not leaving ends the leave and the next starts afresh.
#[test]
fn the_leave_phase_runs_only_over_leaving_passes_whose_clock_runs() {
    assert_eq!(LEAVE_PHASE, Duration::from_secs(32));
    let leave = Leave::default();
    let start = tokio::time::Instant::now();
    let at = |secs| start + Duration::from_secs(secs);
    assert!(!leave.leaving());
    let passes = [
        leave.note_pass(Some(true), at(0)),
        leave.note_pass(Some(true), at(20)),
        leave.note_pass(Some(false), at(40)),
        leave.note_pass(Some(true), at(51)),
        leave.note_pass(Some(true), at(52)),
    ];
    assert_eq!(
        passes,
        [false, false, false, false, true],
        "20 s, held 20 s, then 12 s"
    );
    assert!(leave.leaving());
    assert!(!leave.note_pass(None, at(60)), "the count came back");
    assert!(!leave.leaving());
    let again = [
        leave.note_pass(Some(true), at(61)),
        leave.note_pass(Some(true), at(92)),
        leave.note_pass(Some(true), at(93)),
    ];
    assert_eq!(again, [false, false, true], "started afresh");
    assert_eq!(leave.evacuated(), 0);
}
