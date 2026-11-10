//! Kani proofs for a range row's bitmaps: KANI-109 (S3b-DESIGN §3.3, §4.2,
//! §11.3). The committer keeps a range's `skip`, `cancel` and `sbits` sets
//! and its counts with `set`, `clear`, `contains`, `count` and `is_empty`
//! alone; `sched == popcount(sbits)` (I6, checked by the codec) rests on
//! `count`. Over any three operations at any offsets of one row:
//! - the bitmap holds exactly the offsets whose last operation set them;
//! - an offset of the row that no operation touched is clear;
//! - `count` is the number of those offsets, and `is_empty` holds exactly
//!   when it is zero;
//! - a row is empty exactly when its counts are zero and its bitmaps empty.
use super::{Bits, RangeRow, index_of};

/// The state the last operation on `offset`'s bit left, if any.
fn last(ops: &[(u64, bool); 3], offset: u64) -> Option<bool> {
    let mut out = None;
    for (o, set) in ops {
        if *o == offset {
            out = Some(*set);
        }
    }
    out
}

#[kani::proof]
#[kani::unwind(65)]
fn kani_109_a_bitmap_holds_exactly_the_offsets_its_last_operations_set() {
    let row: u64 = kani::any();
    let ops: [(u64, bool); 3] = [
        (kani::any(), kani::any()),
        (kani::any(), kani::any()),
        (kani::any(), kani::any()),
    ];
    for (o, _) in &ops {
        kani::assume(index_of(*o) == row);
    }
    let mut bits = Bits::default();
    for (o, set) in &ops {
        if *set {
            bits.set(*o);
        } else {
            bits.clear(*o);
        }
    }
    for (o, _) in &ops {
        assert!(
            bits.contains(*o) == last(&ops, *o).unwrap_or(false),
            "the bitmap holds exactly the offsets its last operations set"
        );
    }
    let probe: u64 = kani::any();
    kani::assume(index_of(probe) == row && last(&ops, probe).is_none());
    assert!(
        !bits.contains(probe),
        "an untouched offset of the row is clear"
    );
    let mut held = 0u32;
    for (i, (o, _)) in ops.iter().enumerate() {
        let first = ops[..i].iter().all(|(p, _)| p != o);
        if first && last(&ops, *o) == Some(true) {
            held += 1;
        }
    }
    kani::cover!(held == 2, "two distinct offsets are held");
    kani::cover!(
        ops[0].0 == ops[1].0 && ops[0].1 && !ops[1].1,
        "a set offset is cleared again"
    );
    assert!(bits.count() == held, "count is the number of offsets held");
    assert!(
        bits.is_empty() == (held == 0),
        "is_empty holds exactly when count is zero"
    );
}

#[kani::proof]
#[kani::unwind(65)]
fn kani_109_a_range_row_is_empty_exactly_when_it_holds_nothing() {
    let mut row = RangeRow {
        open: kani::any(),
        sched: kani::any(),
        ..RangeRow::default()
    };
    let marked: (bool, bool, bool) = (kani::any(), kani::any(), kani::any());
    let offset: u64 = kani::any();
    if marked.0 {
        row.skip.set(offset);
    }
    if marked.1 {
        row.cancel.set(offset);
    }
    if marked.2 {
        row.sbits.set(offset);
    }
    let holds = row.open != 0 || row.sched != 0 || marked.0 || marked.1 || marked.2;
    assert!(
        row.is_empty() == !holds,
        "a row is empty exactly when it holds nothing"
    );
}
