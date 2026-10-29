//! Kani proofs for the run-index tiling: KANI-105. A sealed read fetches
//! the chunks `tile` picks from the in-memory directory and the run-index
//! objects together (`shard::seal::read`), and a fold or merge keeps only
//! the chunks `drop_covered` keeps (`shard::fold`). A pick that missed an
//! offset would serve a gap as a short read of records that exist; a stop
//! before an offset some chunk holds would end a read early forever (the
//! reader resumes from the same offset); a dropped chunk whose offsets no
//! kept chunk covers would lose sealed records from every later read.
//!
//! The harnesses take up to `N` spans over offsets below `D`, sorted as each
//! function's caller sorts them, and check every offset by brute force.
use super::{Span, drop_covered, tile};

const N: usize = 4;
const D: u64 = 8;

fn spans(len: usize) -> [Span; N] {
    let mut out = [Span { first: 0, last: 0 }; N];
    for s in out.iter_mut().take(len) {
        let first: u64 = kani::any_where(|f: &u64| *f < D);
        let last: u64 = kani::any_where(|l: &u64| *l < D && *l >= first);
        *s = Span { first, last };
    }
    out
}

fn holds(s: Span, offset: u64) -> bool {
    s.first <= offset && offset <= s.last
}

/// KANI-105 (a): `tile` picks a chain that holds every offset from `from`
/// to where it stops, each pick the best holder of its offset, and it stops
/// short of `to` only at `max` picks or at an offset no span holds.
#[kani::proof]
#[kani::unwind(7)]
fn kani_105_tile_covers_every_offset_until_none_holds_one() {
    let len = kani::any_where(|n: &usize| *n <= N);
    let all = spans(len);
    let s = &all[..len];
    kani::assume(s.windows(2).all(|w| w[0].first <= w[1].first));
    let from: u64 = kani::any_where(|f: &u64| *f < D);
    let to: u64 = kani::any_where(|t: &u64| *t <= D && *t > from);
    let max: usize = kani::any_where(|m: &usize| *m <= N + 1);
    let picks = tile(s, from, to, max);
    assert!(picks.len() <= max, "at most max picks");
    let mut due = from;
    for &p in &picks {
        assert!(p < len, "a pick names an input span");
        assert!(holds(s[p], due), "each pick holds the offset due");
        for (j, other) in s.iter().enumerate() {
            if j != p && holds(*other, due) {
                let worse = other.last < s[p].last
                    || (other.last == s[p].last && other.first >= s[p].first);
                assert!(worse, "each pick is the best holder of its offset");
            }
        }
        due = s[p].last + 1;
    }
    if due < to && picks.len() < max {
        assert!(
            !s.iter().any(|x| holds(*x, due)),
            "a read stops only where no span holds the offset"
        );
    }
    kani::cover!(picks.len() >= 3, "a chain of three picks");
    kani::cover!(
        due < to && picks.len() < max && !picks.is_empty(),
        "a stop at a gap"
    );
}

/// KANI-105 (b): `drop_covered` keeps spans whose first and last offsets
/// both increase strictly, and every offset an input span holds is held by
/// a kept one.
#[kani::proof]
#[kani::unwind(6)]
fn kani_105_drop_covered_keeps_every_offset() {
    let len = kani::any_where(|n: &usize| *n <= N);
    let all = spans(len);
    let s = &all[..len];
    kani::assume(
        s.windows(2).all(|w| {
            w[0].first < w[1].first || (w[0].first == w[1].first && w[0].last >= w[1].last)
        }),
    );
    let kept = drop_covered(s);
    for w in kept.windows(2) {
        assert!(w[0] < w[1], "kept in input order");
        assert!(
            s[w[0]].first < s[w[1]].first,
            "first offsets increase strictly"
        );
        assert!(
            s[w[0]].last < s[w[1]].last,
            "last offsets increase strictly"
        );
    }
    let offset: u64 = kani::any_where(|o: &u64| *o < D);
    if s.iter().any(|x| holds(*x, offset)) {
        assert!(
            kept.iter().any(|&k| k < len && holds(s[k], offset)),
            "every held offset stays held"
        );
    }
    kani::cover!(kept.len() < len, "a contained span dropped");
    kani::cover!(kept.len() == len && len == N, "nothing to drop");
}
