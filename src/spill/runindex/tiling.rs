//! How a stream's sealed chunks tile its offsets (layout 6, DESIGN-6A
//! §2.3-§2.4), as offset spans `[first, last]`, the two rules every reader,
//! fold and merge share:
//!
//! - `tile`: the chunks a read fetches. At each due offset it takes the
//!   chunk holding it with the largest last offset (the fewest GETs), the
//!   smallest first on a tie, the newest object's of two equal spans (a
//!   cleaner's copy: its original's object is released), and stops at the
//!   first offset no chunk holds.
//!   Every chunk that holds an offset holds the same pages for it (a
//!   consolidated chunk is a byte copy of whole chunks), so any choice is
//!   correct; the rule only picks the cheapest.
//! - `drop_covered`: the chunks a run-index object keeps. A chunk another
//!   one contains is dropped (a consolidated chunk covers the chunks it
//!   copied), so what is kept covers every offset the input covers, with
//!   first and last offsets both strictly increasing (the run format's
//!   order, `format::run_ordered`); of two equal spans the newest object's
//!   (`sort_for_drop` puts it first).
//!
//! KANI-105 proves both (`proofs`). Sealed chunks of one stream are
//! disjoint and a consolidation copies whole chunks `tile` picked, so the
//! spans stay laminar (any two nested or disjoint) and `tile`'s picks are
//! disjoint; neither rule needs that to be correct.

/// A chunk's offsets, `first..=last`, and its rank: its object's id
/// (`rank_of`), so of two chunks of one span the newer object's is the one
/// a copy supersedes the other with (`seal::gc::clean`); a span of skipped
/// offsets ranks lowest (0).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) first: u64,
    pub(crate) last: u64,
    pub(crate) rank: u128,
}

/// A chunk's rank: its object's (epoch, seq), newer larger.
pub(crate) fn rank_of(id: crate::spill::object::ObjectId) -> u128 {
    (u128::from(id.epoch) << 64) | u128::from(id.seq)
}

/// Whether `a` is the better pick than `b` for an offset both hold.
fn better(a: Span, b: Span) -> bool {
    a.last > b.last
        || (a.last == b.last && a.first < b.first)
        || (a.last == b.last && a.first == b.first && a.rank > b.rank)
}

/// The spans that tile `[from, to)`, by index into `spans` (sorted by
/// first offset), at most `max` of them: the first holds `from`, each next
/// one holds the offset after the previous one's last, each is the best
/// holder of its offset (largest last, then smallest first). It stops at
/// `to`, at `max` picks, or before the first offset no span holds.
pub(crate) fn tile(spans: &[Span], from: u64, to: u64, max: usize) -> Vec<usize> {
    let mut out = Vec::new();
    let mut due = from;
    let mut seen = spans.iter().copied().enumerate().peekable();
    let mut best: Option<(usize, Span)> = None;
    while due < to && out.len() < max {
        while let Some((i, s)) = seen.next_if(|(_, s)| s.first <= due) {
            if best.is_none_or(|(_, b)| better(s, b)) {
                best = Some((i, s));
            }
        }
        let Some((pick, span)) = best.filter(|(_, s)| s.last >= due) else {
            break;
        };
        out.push(pick);
        let Some(after) = span.last.checked_add(1) else {
            break;
        };
        due = after;
    }
    out
}

/// The spans no other span contains, by index into `spans` (sorted by
/// first offset ascending, then last descending): first and last both
/// strictly increasing, covering every offset `spans` covers. Of two equal
/// spans the first is kept.
pub(crate) fn drop_covered(spans: &[Span]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut reach: Option<u64> = None;
    for (i, s) in spans.iter().enumerate() {
        if reach.is_none_or(|r| s.last > r) {
            out.push(i);
            reach = Some(s.last);
        }
    }
    out
}

/// Sort `items` as `drop_covered` needs them: first ascending, then last
/// descending, then rank descending (of equal spans the newest first).
pub(crate) fn sort_for_drop<T>(items: &mut [T], span: impl Fn(&T) -> Span) {
    items.sort_by(|a, b| {
        let (a, b) = (span(a), span(b));
        a.first
            .cmp(&b.first)
            .then(b.last.cmp(&a.last))
            .then(b.rank.cmp(&a.rank))
    });
}

#[cfg(kani)]
mod proofs;
