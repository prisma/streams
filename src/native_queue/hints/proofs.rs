//! Kani proofs for the trim hints: KANI-111 (S3b-DESIGN §3.4, §11.3).
//! - A hint key parses back to exactly the sequence and queue it names,
//!   and a key under any other sentinel tag names nothing.
//! - After any three additions, a queue's hint ranges are sorted, apart
//!   (neither overlapping nor touching), cover every added offset, and
//!   reach only the range-row spans the added offsets lie in.
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

/// An added range: offsets below a bound that spans three range rows.
fn any_range() -> (u64, u64) {
    let a: u64 = kani::any();
    let b: u64 = kani::any();
    kani::assume(a < b && b <= 3 * super::super::range::SPAN);
    (a, b)
}

#[kani::proof]
#[kani::unwind(6)]
fn kani_111_hint_ranges_stay_sorted_apart_and_cover_only_what_was_added() {
    let added = [any_range(), any_range(), any_range()];
    let mut h = HintRanges::default();
    for (a, b) in added {
        h.add(a, b);
    }
    let ranges = h.below(u64::MAX);
    let span = |o: u64| o >> super::super::range::SHIFT;
    for w in ranges.windows(2) {
        assert!(w[0].1 < w[1].0, "sorted and apart");
    }
    for (a, b) in added {
        let covers = |o: u64| ranges.iter().any(|&(x, y)| x <= o && o < y);
        assert!(covers(a) && covers(b - 1), "every added offset is named");
    }
    for &(x, y) in &ranges {
        assert!(x < y, "no range is empty");
        let named = |s: u64| added.iter().any(|&(a, b)| span(a) <= s && s <= span(b - 1));
        assert!(
            named(span(x)) && named(span(y - 1)),
            "no range reaches a span nothing added lies in"
        );
    }
}
