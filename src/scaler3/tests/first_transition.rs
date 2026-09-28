//! A stream's first split under a cooldown past the millisecond range (edge
//! change #45's follow-up). It sits beside the parent's cooldown tests
//! because src/scaler3.rs is at its 1,000-line ceiling.
#![cfg(test)]

use super::{chosen, cold_pair, splittable, test_desc};
use crate::scaler3::{SKETCH_IDLE_MS, ScalePolicy, State, evaluate_state};

/// SCALE_COOLDOWN_SECS=inf loads as i64::MAX and means "never re-scale": a
/// stream with no transition record has not split, so its first split is
/// never held, and once it has split it never splits again. A merge is never
/// chosen: its candidate has already split, and the controller refuses every
/// merge under inf by its segments' age, so choosing one would record a
/// transition that never ran and bar the stream's split for ever.
#[test]
fn an_infinite_cooldown_allows_a_first_split_and_never_a_merge() {
    let pol = ScalePolicy {
        cooldown_secs: i64::MAX,
        ..ScalePolicy::default()
    };
    let [hot, quiet] = ["first-split", "no-merge"].map(|n| test_desc(n).sref());
    let mut s = State::default();
    s.sketches
        .insert((hot.clone(), 0), splittable("epoch", 1_000));
    cold_pair(&mut s, &quiet, 1_000, 0);
    let at = |s: &mut State, now| chosen(&evaluate_state(s, now, &pol, crate::usage::limits()));
    assert_eq!(at(&mut s, 1_000), (vec!["first-split".into()], vec![]));
    assert_eq!(
        s.last_transition_ms.get(&(quiet.clone(), "epoch".into())),
        None,
        "a merge that was not chosen records nothing"
    );
    // Re-heated a hundred idle horizons later, far past the default 600 s
    // cooldown, the split stream does not split again: its record holds.
    let later = 1_000 + 100 * SKETCH_IDLE_MS;
    s.sketches
        .insert((hot.clone(), 0), splittable("epoch", later));
    cold_pair(&mut s, &quiet, later, 0);
    assert_eq!(at(&mut s, later), (vec![], vec![]));
    assert_eq!(
        s.last_transition_ms.get(&(hot, "epoch".into())),
        Some(&1_000)
    );
}
