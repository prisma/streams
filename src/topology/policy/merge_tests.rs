#![cfg(test)]

use super::tests::{READY, T0, cell, leaf, limits, merge, split, sustained, tick, usage};
use super::{Axis, Decision, Hold, Policy, SplitKind, View};

/// Two cold siblings `0` (on `a`) and `1` (on `b`) at `pa` and `pb` of their appends.
fn cold_pair(pa: u64, pb: u64) -> View {
    cell(vec![leaf("0", "a", pa), leaf("1", "b", pb)])
}

const QUIET: i64 = T0 + 21_600_000;

#[test]
fn cold_siblings_merge_after_the_full_quiet_sustain() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 14));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET - 1),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, QUIET), merge(""));
}

#[test]
fn siblings_at_the_merge_line_combined_never_merge() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 15));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        Decision::Hold(Hold::Steady)
    );
}

#[test]
fn siblings_merge_only_when_quiet_on_every_axis() {
    let mut view = cold_pair(1, 1);
    view.leaves[1].load.insert(Axis::Committer, usage(25));
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        Decision::Hold(Hold::Steady)
    );
}

#[test]
fn a_busy_tick_restarts_the_quiet_sustain() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 10));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.leaves[0].load.insert(Axis::Appends, usage(30));
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 1_000),
        Decision::Hold(Hold::Steady)
    );
    view.leaves[0].load.insert(Axis::Appends, usage(10));
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 2_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET + 1_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, QUIET + 2_000), merge(""));
}

#[test]
fn a_stale_beat_from_either_owner_restarts_the_quiet_sustain() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 10));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.now_ms = T0 + 60_000;
    view.leaves[1].beat_ms = Some(view.now_ms - 30_001);
    assert_eq!(
        policy.decide(&view, &limits()),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 61_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET + 60_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, QUIET + 61_000), merge(""));
}

#[test]
fn a_split_is_not_reversed_inside_the_anti_flap() {
    for (born, decision) in [
        (QUIET - 43_199_999, Decision::Hold(Hold::Steady)),
        (QUIET - 43_200_000, merge("")),
    ] {
        let mut view = cold_pair(10, 10);
        view.leaves[1].born_ms = born;
        let mut policy = Policy::default();
        assert_eq!(
            tick(&mut policy, &mut view, T0),
            Decision::Hold(Hold::Steady)
        );
        assert_eq!(tick(&mut policy, &mut view, QUIET), decision, "born {born}");
    }
}

#[test]
fn congestion_on_the_cell_blocks_a_merge() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 10));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.congested = true;
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        Decision::Hold(Hold::Congested)
    );
}

#[test]
fn a_recent_override_holds_a_merge() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 10));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.youngest_override_ms = Some(QUIET - 59_999);
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        Decision::Hold(Hold::OverrideRecent)
    );
}

#[test]
fn the_owner_of_the_zero_child_merges() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 10));
    view.me = "b".to_owned();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        Decision::Hold(Hold::PeerActs)
    );
}

#[test]
fn the_deepest_pair_merges_first() {
    let mut view = cell(vec![
        leaf("0000", "a", 1),
        leaf("0001", "a", 1),
        leaf("001", "a", 1),
        leaf("01", "a", 1),
        leaf("10", "a", 1),
        leaf("11", "a", 1),
    ]);
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, QUIET), merge("000"));
    view.leaves.splice(0..2, [leaf("000", "a", 1)]);
    // The merged leaf is young, so its pair waits: the next pair, `1`, merges.
    view.leaves[0].born_ms = QUIET;
    assert_eq!(tick(&mut policy, &mut view, QUIET + 60_000), merge("1"));
}

#[test]
fn a_split_goes_before_a_merge() {
    let mut view = cell(vec![
        leaf("00", "a", 1),
        leaf("01", "a", 1),
        leaf("10", "a", 1),
        leaf("11", "a", 70),
    ]);
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        split("11", SplitKind::Normal)
    );
}

#[test]
fn stale_cached_beats_block_a_merge_and_restart_its_sustain_but_never_block_a_split() {
    let (mut policy, mut view) = (Policy::default(), cold_pair(10, 10));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.beats_fresh = false;
    assert_eq!(
        tick(&mut policy, &mut view, QUIET),
        Decision::Hold(Hold::BeatsStale)
    );
    view.beats_fresh = true;
    assert_eq!(
        tick(&mut policy, &mut view, QUIET + 1),
        Decision::Hold(Hold::Steady)
    );

    let (mut policy, mut view) = sustained();
    view.beats_fresh = false;
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}
