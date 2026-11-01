//! The tiling rules against a brute-force reference over random spans,
//! and the shapes a fold and a consolidation produce.
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::tiling::{Span, drop_covered, sort_for_drop, tile};

fn s(first: u64, last: u64) -> Span {
    r(first, last, 0)
}

fn r(first: u64, last: u64, rank: u128) -> Span {
    Span { first, last, rank }
}

#[test]
fn a_consolidated_chunk_is_preferred_and_gaps_stop_the_tiling() {
    // Originals [0,9] [10,19] [20,29], a consolidation of the first two,
    // then a gap before [40,49].
    let spans = [s(0, 9), s(0, 19), s(10, 19), s(20, 29), s(40, 49)];
    assert_eq!(tile(&spans, 0, 100, 10), vec![1, 3]);
    assert_eq!(
        tile(&spans, 12, 100, 10),
        vec![1, 3],
        "a pick may start before its offset"
    );
    assert_eq!(tile(&spans, 30, 100, 10), Vec::<usize>::new());
    assert_eq!(tile(&spans, 40, 45, 10), vec![4]);
    assert_eq!(tile(&spans, 0, 100, 1), vec![1]);
    assert_eq!(
        tile(&spans, 0, 20, 10),
        vec![1],
        "a window ending where a pick ends takes nothing past it"
    );
}

#[test]
fn drop_covered_keeps_the_outermost_spans() {
    let mut spans = vec![s(10, 19), s(0, 9), s(0, 19), s(20, 29), s(0, 19)];
    sort_for_drop(&mut spans, |x| *x);
    let kept: Vec<Span> = drop_covered(&spans).into_iter().map(|i| spans[i]).collect();
    assert_eq!(kept, vec![s(0, 19), s(20, 29)]);
}

fn holds(x: Span, o: u64) -> bool {
    x.first <= o && o <= x.last
}

#[test]
fn both_rules_match_a_brute_force_reference() {
    let mut rng = StdRng::seed_from_u64(105);
    for _ in 0..20_000 {
        let n = rng.random_range(0..7);
        let mut spans: Vec<Span> = (0..n)
            .map(|_| {
                let first = rng.random_range(0..30u64);
                s(first, first + rng.random_range(0..10u64))
            })
            .collect();
        sort_for_drop(&mut spans, |x| *x);
        let from = rng.random_range(0..35u64);
        let to = from + rng.random_range(1..20u64);
        let picks = tile(&spans, from, to, 64);
        let mut due = from;
        for &p in &picks {
            let best = spans
                .iter()
                .filter(|x| holds(**x, due))
                .max_by(|a, b| a.last.cmp(&b.last).then(b.first.cmp(&a.first)))
                .copied();
            assert_eq!(Some(spans[p]), best, "{spans:?} from {from}");
            due = spans[p].last + 1;
        }
        assert!(
            due >= to || !spans.iter().any(|x| holds(*x, due)),
            "{spans:?} stopped at {due}"
        );
        let kept = drop_covered(&spans);
        for o in 0..45 {
            assert_eq!(
                spans.iter().any(|x| holds(*x, o)),
                kept.iter().any(|&k| holds(spans[k], o)),
                "{spans:?} offset {o}"
            );
        }
        for w in kept.windows(2) {
            assert!(spans[w[0]].first < spans[w[1]].first && spans[w[0]].last < spans[w[1]].last);
        }
    }
}

#[test]
fn of_identical_spans_the_first_is_picked() {
    let spans = [s(0, 9), s(0, 9), s(10, 19), s(10, 19)];
    assert_eq!(tile(&spans, 0, 20, 10), vec![0, 2]);
}

/// Of two chunks of one span (a cleaner's copy and the chunk it copied),
/// the newer object's is picked and kept, whatever order the index gave
/// them (`seal::gc::clean`).
#[test]
fn of_two_equal_spans_the_newer_objects_is_picked_and_kept() {
    for spans in [vec![r(0, 9, 1), r(0, 9, 2)], vec![r(0, 9, 2), r(0, 9, 1)]] {
        let picks = tile(&spans, 0, 10, 4);
        let picked: Vec<u128> = picks.iter().map(|&i| spans[i].rank).collect();
        assert_eq!(picked, [2], "tile of {spans:?}");
        let mut sorted = spans.clone();
        sort_for_drop(&mut sorted, |x| *x);
        let kept: Vec<u128> = drop_covered(&sorted)
            .iter()
            .map(|&i| sorted[i].rank)
            .collect();
        assert_eq!(kept, [2], "drop_covered of {spans:?}");
    }
}

/// A rank orders objects by (epoch, seq): a newer epoch outranks every seq
/// of an older one.
#[test]
fn a_rank_is_the_objects_epoch_then_seq() {
    use crate::spill::object::ObjectId;
    assert_eq!(
        super::tiling::rank_of(ObjectId { epoch: 2, seq: 3 }),
        (2u128 << 64) + 3
    );
    assert!(
        super::tiling::rank_of(ObjectId { epoch: 1, seq: 0 })
            > super::tiling::rank_of(ObjectId {
                epoch: 0,
                seq: u64::MAX
            })
    );
}

/// The rank breaks only a tie of both offsets: of two spans ending at one
/// offset, the one starting first is picked even when the other is newer.
#[test]
fn a_newer_span_starting_later_loses_to_the_one_starting_first() {
    let spans = [r(0, 9, 1), r(3, 9, 9)];
    assert_eq!(tile(&spans, 5, 10, 4), vec![0]);
}
