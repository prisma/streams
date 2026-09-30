//! Where one evaluation's rate lines fall: a segment is hot strictly above
//! `hot_pct` of any limit and cold strictly under 5% of that line on every
//! limit, and neither between them; a split is chosen on exactly the
//! `hot_evals`-th consecutive hot evaluation; and a dominant key is an
//! unsplittable hot key only while the segment holds no other meaningful
//! load.
#![cfg(test)]

use super::{chosen, sketch, splittable, test_desc};
use crate::crypto::RoutingKeyHash;
use crate::scaler3::{ScalePolicy, SegSketch, State, evaluate_state};
use crate::usage::Limits;

/// Limits whose hot lines (750 B/s, 75 req/s, 750 rec/s) are exact in
/// floating point, so a rate can sit on one.
fn limits() -> Limits {
    Limits {
        bytes_per_sec: 1_000.0,
        reqs_per_sec: 100.0,
        recs_per_sec: 1_000.0,
        burst_secs: 1.0,
    }
}

/// A segment whose three rates read exactly `(bytes, reqs, recs)` at
/// `now`: fed once at `now`, so no decay applies, then set directly.
fn rates(now: i64, bytes: f64, reqs: f64, recs: f64) -> SegSketch {
    let mut sk = sketch("epoch", now, false, 1);
    sk.dist.bytes.rate = bytes;
    sk.dist.reqs.rate = reqs;
    sk.dist.recs.rate = recs;
    sk.hot_streak = 0;
    sk
}

/// One evaluation of a lone fresh segment at the given rates: its (cold,
/// hot) streaks afterwards.
fn streaks_after(bytes: f64, reqs: f64, recs: f64) -> (u32, u32) {
    let name = test_desc("lines").sref();
    let mut s = State::default();
    s.sketches
        .insert((name.clone(), 0), rates(1_000, bytes, reqs, recs));
    let (splits, merges) = evaluate_state(&mut s, 1_000, &ScalePolicy::default(), &limits());
    assert!(
        splits.is_empty() && merges.is_empty(),
        "{splits:?} {merges:?}"
    );
    let sk = &s.sketches[&(name, 0)];
    (sk.cold_streak, sk.hot_streak)
}

/// Each limit is judged on its own: a rate just under 5% of its hot line
/// is cold and one on that quiet line is not; between the lines the segment
/// is neither; on the hot line it is not hot and the next representable
/// rate above it is.
#[test]
fn a_segment_is_hot_strictly_above_the_hot_line_and_cold_strictly_under_the_quiet_line() {
    let pol = ScalePolicy::default();
    let lim = limits();
    let full = [lim.bytes_per_sec, lim.reqs_per_sec, lim.recs_per_sec];
    let hot = full.map(|limit| limit * pol.hot_pct);
    let quiet = full.map(|limit| limit * (pol.hot_pct * 0.05));
    assert_eq!(streaks_after(0.0, 0.0, 0.0), (1, 0), "silence is cold");
    for i in 0..3 {
        for (rate, want, what) in [
            (
                quiet[i].next_down(),
                (1, 0),
                "just under the quiet line: cold",
            ),
            (quiet[i], (0, 0), "on the quiet line: not cold"),
            (hot[i] / 2.0, (0, 0), "between the lines: neither"),
            (hot[i], (0, 0), "on the hot line: not hot"),
            (hot[i].next_up(), (0, 1), "just above the hot line: hot"),
            (full[i], (0, 1), "at the limit: hot"),
        ] {
            let mut r = [0.0; 3];
            r[i] = rate;
            assert_eq!(
                streaks_after(r[0], r[1], r[2]),
                want,
                "limit {i} {what} ({rate})"
            );
        }
    }
}

/// The `hot_evals`-th consecutive hot evaluation chooses the split, not one
/// later: under the default patience of two a fresh hot segment is held once
/// and split on the next evaluation, which restarts its streak; a patience
/// of one splits at once.
#[test]
fn a_split_is_chosen_on_exactly_the_hot_evals_th_consecutive_hot_evaluation() {
    let pol = ScalePolicy::default();
    assert_eq!(pol.hot_evals, 2);
    let name = test_desc("patient").sref();
    let fresh = |s: &mut State| {
        let mut sk = splittable("epoch", 1_000);
        sk.hot_streak = 0;
        s.sketches.insert((name.clone(), 0), sk);
    };
    let mut s = State::default();
    fresh(&mut s);
    let at = |s: &mut State, pol: &ScalePolicy| {
        chosen(&evaluate_state(s, 1_000, pol, crate::usage::limits()))
    };
    assert_eq!(at(&mut s, &pol), (vec![], vec![]), "held once");
    assert_eq!(s.sketches[&(name.clone(), 0)].hot_streak, 1);
    assert_eq!(at(&mut s, &pol), (vec!["patient".into()], vec![]));
    assert_eq!(
        s.sketches[&(name.clone(), 0)].hot_streak,
        0,
        "a chosen split restarts the streak"
    );
    let eager = ScalePolicy {
        hot_evals: 1,
        ..pol
    };
    let mut s = State::default();
    fresh(&mut s);
    assert_eq!(at(&mut s, &eager), (vec!["patient".into()], vec![]));
}

/// A hot segment fed the given `(point, key, bytes)` samples, past the
/// split patience.
fn segment(samples: &[(u64, u8, u64)]) -> SegSketch {
    let mut sk = sketch("epoch", 1_000, false, samples[0].1);
    for (point, key, bytes) in samples {
        sk.dist.note(1_000, *point, [*key; 16], *bytes, 1);
    }
    sk
}

/// A key holding over half a segment's load is an unsplittable hot key only
/// while nothing else there is worth a split: either a second key above 15%
/// of the load or eight distinct keys makes the segment plural, and it
/// splits at its load-weighted median instead of surfacing the key.
#[test]
fn a_dominant_key_is_a_hot_key_only_while_the_segment_holds_no_other_meaningful_load() {
    let name = test_desc("dominated").sref();
    let far = u64::MAX / 4 * 3;
    let split = (name.clone(), "epoch".to_string(), 0, u64::MAX / 64);
    let light: Vec<(u64, u8, u64)> = (10..34).map(|key| (far, key, 18_000_000_000)).collect();
    for (samples, want, what) in [
        (
            vec![(1, 1, 1_000_000_000_000)],
            (vec![], Some(RoutingKeyHash([1; 16]))),
            "one key: a hot key, no split",
        ),
        (
            vec![(1, 1, 600_000_000_000), (far, 2, 400_000_000_000)],
            (vec![split.clone()], None),
            "a second key above 15%: plural by keys",
        ),
        (
            [vec![(1, 1, 550_000_000_000)], light].concat(),
            (vec![split], None),
            "24 light distinct keys: plural by distinct count",
        ),
    ] {
        let mut s = State::default();
        s.sketches.insert((name.clone(), 0), segment(&samples));
        let (splits, merges) = evaluate_state(
            &mut s,
            1_000,
            &ScalePolicy::default(),
            crate::usage::limits(),
        );
        assert!(merges.is_empty(), "{what}: {merges:?}");
        assert_eq!((splits, s.hot_keys.get(&name).copied()), want, "{what}");
    }
}
