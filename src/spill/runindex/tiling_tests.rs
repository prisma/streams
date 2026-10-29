//! The tiling rules against a brute-force reference over random spans,
//! and the shapes a fold and a consolidation produce.
use rand::{Rng, SeedableRng, rngs::StdRng};

use super::tiling::{Span, drop_covered, sort_for_drop, tile};

fn s(first: u64, last: u64) -> Span {
    Span { first, last }
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
