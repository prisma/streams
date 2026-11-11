//! Kani proofs for the trim hints: KANI-111 (S3b-DESIGN §3.4, §11.3).
//! - A hint key parses back to exactly the sequence and queue it names,
//!   and a key under any other sentinel tag names nothing.
//! - One addition to any sorted, apart (neither overlapping nor touching)
//!   list of up to three non-empty ranges keeps them sorted, apart and
//!   non-empty, holds each range it had and the added one within one range,
//!   and starts and ends every range where one of those did. By induction
//!   from the empty list, after any additions (while at most three ranges
//!   are held before each) a queue's hint ranges cover every added offset
//!   and reach only the range-row spans the added ranges reach.
use super::{HintRanges, key, parse_key};

#[kani::proof]
#[kani::unwind(45)]
fn kani_111_a_hint_key_names_its_sequence_and_queue_and_no_other_tag_does() {
    let gseq: u64 = kani::any();
    let hash: [u8; 16] = kani::any();
    let k = key(gseq, &hash);
    assert!(
        parse_key(&k) == Some((gseq, hash)),
        "a hint key parses back to its sequence and queue"
    );
    let tag: u8 = kani::any();
    kani::assume(tag != super::TAG);
    let mut other = k;
    other[16] = tag;
    assert!(parse_key(&other).is_none(), "another tag names nothing");
}

/// Any `N` sorted, apart, non-empty ranges: what additions leave, by the
/// induction this harness's step proves.
fn any_ranges<const N: usize>() -> [(u64, u64); N] {
    let ranges: [(u64, u64); N] = kani::any();
    for i in 0..N {
        kani::assume(ranges[i].0 < ranges[i].1);
        if i > 0 {
            kani::assume(ranges[i - 1].1 < ranges[i].0);
        }
    }
    ranges
}

/// One addition of any non-empty range to `N` held ranges.
fn one_addition<const N: usize>() {
    let before = any_ranges::<N>();
    let (a, b): (u64, u64) = (kani::any(), kani::any());
    kani::assume(a < b);
    let mut h = HintRanges(before.to_vec());
    h.add(a, b);
    let after = &h.0;
    let input = |j: usize| if j < N { before[j] } else { (a, b) };
    // One nondeterministic index per property, not a loop over the result
    // (kani-ci-memory): what holds at any index holds at every index.
    let i: usize = kani::any();
    kani::assume(i <= N);
    if i + 1 < after.len() {
        assert!(after[i].1 < after[i + 1].0, "sorted and apart");
    }
    let j: usize = kani::any();
    kani::assume(j <= N);
    let (x, y) = input(j);
    assert!(
        after.iter().any(|&(p, q)| p <= x && y <= q),
        "every added offset is named"
    );
    assert!(after.len() <= N + 1, "no range is made from nothing");
    if let Some(&(p, q)) = after.get(i) {
        assert!(p < q, "no range is empty");
        assert!(
            (0..=N).any(|k| input(k).0 == p) && (0..=N).any(|k| input(k).1 == q),
            "no range reaches a span nothing added lies in"
        );
    }
}

#[kani::proof]
#[kani::unwind(6)]
fn kani_111_hint_ranges_stay_sorted_apart_and_cover_only_what_was_added() {
    one_addition::<0>();
    one_addition::<1>();
    one_addition::<2>();
    one_addition::<3>();
}
