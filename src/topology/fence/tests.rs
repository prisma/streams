#![cfg(test)]
//! The fence's table (S4-DESIGN §6.3), row by row.

use super::{Source, Verdict, decide};

/// **Every row of the table.** No `'T'`: an unnamed or terminal intent serves, a live one holds, a
/// lapsed one rolls back. With `'T'`: a snapshot at or past its version is retired whatever the
/// intent; before it, a live intent holds and a lapsed or no intent rolls forward.
#[test]
fn the_fence_decides_each_row_of_its_table() {
    let rows = [
        (None, Source::Unnamed, 1, Verdict::Serve),
        (None, Source::Live, 1, Verdict::Hold),
        (None, Source::Lapsed, 1, Verdict::RollBack),
        (Some(2), Source::Lapsed, 1, Verdict::RollForward),
        (Some(2), Source::Lapsed, 2, Verdict::Retired),
        (Some(2), Source::Live, 1, Verdict::Hold),
        (Some(2), Source::Unnamed, 1, Verdict::RollForward),
        (Some(2), Source::Live, 2, Verdict::Retired),
        (Some(2), Source::Unnamed, 3, Verdict::Retired),
        (Some(2), Source::Live, 3, Verdict::Retired),
    ];
    for (retired, source, snapshot, verdict) in rows {
        assert_eq!(
            decide(retired, source, snapshot),
            verdict,
            "{retired:?} {source:?} {snapshot}"
        );
    }
}
