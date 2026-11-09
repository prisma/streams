#![cfg(test)]

use super::{Axis, Decision, Hold, Instance, Leaf, Limits, Policy, SplitKind, Usage, View};
use std::collections::BTreeMap;

/// A clock far past every leaf's birth, so no leaf is young unless a test makes it so.
pub(super) const T0: i64 = 100_000_000_000;

/// rev 4 §4.3's table and Q4's seven shards.
pub(super) fn limits() -> Limits {
    Limits {
        split_line_pct: 70,
        split_sustain_ms: 600_000,
        emergency_line_pct: 90,
        emergency_sustain_ms: 45_000,
        merge_line_pct: 25,
        merge_sustain_ms: 21_600_000,
        reversal_min_ms: 43_200_000,
        cooldown_ms: 60_000,
        beat_stale_ms: 30_000,
        override_cooldown_ms: 60_000,
        shards_per_instance_max: 7,
    }
}

pub(super) fn usage(used: u64) -> Usage {
    Usage {
        used,
        safe: 100,
        top_entity: 0,
    }
}

/// A leaf owned by `owner` using `pct` of its appends capacity, born at 0.
pub(super) fn leaf(prefix: &str, owner: &str, pct: u64) -> Leaf {
    Leaf {
        prefix: prefix.to_owned(),
        born_ms: 0,
        owner: owner.to_owned(),
        beat_ms: Some(T0),
        load: BTreeMap::from([(Axis::Appends, usage(pct))]),
    }
}

fn instance(id: &str, resident_shards: u32) -> Instance {
    Instance {
        id: id.to_owned(),
        resident_shards,
        accepting: true,
    }
}

/// A settled cell of `leaves` run by `a`, with `b` beside it, each holding three shards.
pub(super) fn cell(leaves: Vec<Leaf>) -> View {
    View {
        now_ms: T0,
        me: "a".to_owned(),
        leaves,
        instances: vec![instance("a", 3), instance("b", 3)],
        split_demand: 0,
        beats_fresh: true,
        intent_live: false,
        last_op_end_ms: None,
        desired_computed_ms: Some(0),
        youngest_override_ms: None,
        congested: false,
    }
}

/// The decision at `now_ms`, every leaf's beat fresh at that instant.
pub(super) fn tick(policy: &mut Policy, view: &mut View, now_ms: i64) -> Decision {
    view.now_ms = now_ms;
    for leaf in &mut view.leaves {
        leaf.beat_ms = Some(now_ms);
    }
    policy.decide(view, &limits())
}

pub(super) fn split(prefix: &str, kind: SplitKind) -> Decision {
    Decision::Split {
        prefix: prefix.to_owned(),
        kind,
        doubling: false,
        fleet_axis: 0,
    }
}

/// A doubling's step on the 70% line.
fn doubling(prefix: &str) -> Decision {
    Decision::Split {
        prefix: prefix.to_owned(),
        kind: SplitKind::Normal,
        doubling: true,
        fleet_axis: 0,
    }
}

/// A split the fleet axis made ready at demand `d`.
fn fleet(prefix: &str, doubling: bool, d: u64) -> Decision {
    Decision::Split {
        prefix: prefix.to_owned(),
        kind: SplitKind::Normal,
        doubling,
        fleet_axis: d,
    }
}

pub(super) fn merge(parent: &str) -> Decision {
    Decision::Merge {
        parent: parent.to_owned(),
    }
}

/// Three leaves, `1` at `pct`: one of three over the line is a local split, not a doubling.
fn one_hot(pct: u64) -> View {
    cell(vec![
        leaf("00", "a", 10),
        leaf("01", "a", 10),
        leaf("1", "a", pct),
    ])
}

/// A view whose `1` has held 70% for the full sustain, ready to split at `T0 + 600 s`.
pub(super) fn sustained() -> (Policy, View) {
    let (mut policy, mut view) = (Policy::default(), one_hot(70));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    (policy, view)
}

pub(super) const READY: i64 = T0 + 600_000;

#[test]
fn a_leaf_at_the_split_line_splits_after_the_full_sustain_and_not_a_millisecond_before() {
    let (mut policy, mut view) = sustained();
    assert_eq!(
        tick(&mut policy, &mut view, READY - 1),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn a_leaf_just_under_the_split_line_never_splits() {
    let (mut policy, mut view) = (Policy::default(), one_hot(69));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 6_000_000),
        Decision::Hold(Hold::Steady)
    );
}

#[test]
fn a_dip_under_the_line_restarts_the_sustain() {
    let (mut policy, mut view) = sustained();
    view.leaves[2].load.insert(Axis::Appends, usage(69));
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 300_000),
        Decision::Hold(Hold::Steady)
    );
    view.leaves[2].load.insert(Axis::Appends, usage(70));
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 301_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 900_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 901_000),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn a_beat_older_than_the_stale_age_restarts_the_sustain_and_one_at_it_does_not() {
    let (mut policy, mut view) = sustained();
    view.now_ms = T0 + 300_000;
    view.leaves[2].beat_ms = Some(view.now_ms - 30_000);
    assert_eq!(
        policy.decide(&view, &limits()),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );

    let (mut policy, mut view) = sustained();
    view.now_ms = T0 + 300_000;
    view.leaves[2].beat_ms = Some(view.now_ms - 30_001);
    assert_eq!(
        policy.decide(&view, &limits()),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 1_199_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 1_200_000),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn a_leaf_no_beat_carries_never_splits() {
    let (mut policy, mut view) = sustained();
    view.now_ms = READY;
    view.leaves[2].beat_ms = None;
    assert_eq!(
        policy.decide(&view, &limits()),
        Decision::Hold(Hold::Steady)
    );
}

#[test]
fn a_leaf_at_the_emergency_line_splits_after_its_short_sustain() {
    let (mut policy, mut view) = (Policy::default(), one_hot(90));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 44_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 45_000),
        split("1", SplitKind::Emergency)
    );

    let (mut policy, mut view) = (Policy::default(), one_hot(89));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 45_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn the_most_loaded_axis_is_the_one_that_counts() {
    let mut view = one_hot(10);
    view.leaves[2].load.insert(
        Axis::Streams,
        Usage {
            used: 70_000,
            safe: 100_000,
            top_entity: 0,
        },
    );
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn an_axis_with_no_measured_capacity_is_no_constraint() {
    let mut view = one_hot(10);
    view.leaves[2].load.insert(
        Axis::Committer,
        Usage {
            used: 50,
            safe: 0,
            top_entity: 0,
        },
    );
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::Steady)
    );
}

#[test]
fn one_entity_carrying_half_of_the_binding_axis_holds_the_split() {
    for (top, decision) in [
        (40, Decision::Hold(Hold::HotEntity)),
        (39, split("1", SplitKind::Normal)),
    ] {
        let mut view = one_hot(80);
        view.leaves[2].load.insert(
            Axis::Appends,
            Usage {
                used: 80,
                safe: 100,
                top_entity: top,
            },
        );
        view.leaves[2].load.insert(
            Axis::Streams,
            Usage {
                used: 30,
                safe: 100,
                top_entity: 30,
            },
        );
        let mut policy = Policy::default();
        assert_eq!(
            tick(&mut policy, &mut view, T0),
            Decision::Hold(Hold::Steady)
        );
        assert_eq!(
            tick(&mut policy, &mut view, READY),
            decision,
            "top entity {top}"
        );
    }
}

#[test]
fn a_hot_entity_on_an_axis_that_does_not_bind_does_not_hold_the_split() {
    let mut view = one_hot(10);
    view.leaves[2].load.insert(
        Axis::Appends,
        Usage {
            used: 72,
            safe: 100,
            top_entity: 72,
        },
    );
    view.leaves[2].load.insert(
        Axis::Streams,
        Usage {
            used: 80,
            safe: 100,
            top_entity: 1,
        },
    );
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn a_24_bit_leaf_never_splits_and_a_23_bit_one_does() {
    for (bits, decision) in [
        (24, Decision::Hold(Hold::MaxDepth)),
        (23, split("", SplitKind::Normal)),
    ] {
        let deep = "1".repeat(bits);
        let mut view = cell(vec![
            leaf("0", "a", 10),
            leaf("10", "a", 10),
            leaf(&deep, "a", 75),
        ]);
        let mut policy = Policy::default();
        assert_eq!(
            tick(&mut policy, &mut view, T0),
            Decision::Hold(Hold::Steady)
        );
        let decision = match decision {
            Decision::Split { kind, .. } => split(&deep, kind),
            other => other,
        };
        assert_eq!(tick(&mut policy, &mut view, READY), decision, "{bits} bits");
    }
}

/// `n` six-bit leaves, or with `n` = 63 one five-bit leaf in place of a pair, `000001` hot.
fn wide(n: usize) -> View {
    let mut leaves: Vec<Leaf> = (0..64)
        .map(|i| leaf(&format!("{i:06b}"), "a", if i == 1 { 75 } else { 0 }))
        .collect();
    if n == 63 {
        leaves.splice(62..64, [leaf("11111", "a", 0)]);
    }
    cell(leaves)
}

#[test]
fn a_split_to_the_64th_leaf_goes_ahead_and_one_past_64_leaves_is_refused() {
    for (n, decision) in [
        (64, Decision::Hold(Hold::TopologyAtMax)),
        (63, split("000001", SplitKind::Normal)),
    ] {
        let (mut policy, mut view) = (Policy::default(), wide(n));
        assert_eq!(
            tick(&mut policy, &mut view, T0),
            Decision::Hold(Hold::Steady)
        );
        assert_eq!(tick(&mut policy, &mut view, READY), decision, "{n} leaves");
    }
}

#[test]
fn half_of_the_leaves_over_the_line_doubles_every_leaf_deepest_first() {
    let mut view = cell(vec![
        leaf("000", "a", 0),
        leaf("001", "a", 0),
        leaf("01", "a", 70),
        leaf("1", "a", 70),
    ]);
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, READY), doubling("000"));

    // The split published: the children are leaves, and the doubling does not split them.
    view.leaves
        .splice(0..1, [leaf("0000", "a", 0), leaf("0001", "a", 0)]);
    assert_eq!(tick(&mut policy, &mut view, READY + 1), doubling("001"));
    view.leaves
        .splice(2..3, [leaf("0010", "a", 0), leaf("0011", "a", 0)]);
    assert_eq!(tick(&mut policy, &mut view, READY + 2), doubling("01"));
    view.leaves
        .splice(4..5, [leaf("010", "a", 0), leaf("011", "a", 0)]);
    assert_eq!(tick(&mut policy, &mut view, READY + 3), doubling("1"));
    view.leaves
        .splice(6..7, [leaf("10", "a", 0), leaf("11", "a", 0)]);
    assert_eq!(
        tick(&mut policy, &mut view, READY + 4),
        Decision::Hold(Hold::Steady)
    );
}

#[test]
fn one_leaf_of_four_over_the_line_splits_alone() {
    let mut view = cell(vec![
        leaf("000", "a", 0),
        leaf("001", "a", 0),
        leaf("01", "a", 0),
        leaf("1", "a", 70),
    ]);
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn a_one_leaf_cell_over_the_line_doubles() {
    let mut view = cell(vec![leaf("", "a", 70)]);
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, READY), doubling(""));
}

#[test]
fn only_the_candidate_leafs_owner_acts() {
    let (mut policy, mut view) = sustained();
    view.leaves[2].owner = "b".to_owned();
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::PeerActs)
    );
}

#[test]
fn a_live_intent_holds_every_operation() {
    let (mut policy, mut view) = sustained();
    view.intent_live = true;
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::IntentLive)
    );
}

#[test]
fn the_next_operation_waits_the_cooldown_after_the_last_one_ended() {
    let (mut policy, mut view) = sustained();
    view.last_op_end_ms = Some(READY - 59_999);
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::Cooldown)
    );
    view.last_op_end_ms = Some(READY - 60_000);
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn an_operation_waits_for_the_fleets_desired_state_to_settle() {
    let (mut policy, mut view) = sustained();
    view.desired_computed_ms = Some(READY - 59_999);
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::FleetSettling)
    );
    view.desired_computed_ms = Some(READY - 60_000);
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
    view.desired_computed_ms = None;
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

#[test]
fn an_emergency_split_waits_for_a_recent_override_too() {
    let (mut policy, mut view) = (Policy::default(), one_hot(95));
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.youngest_override_ms = Some(T0 + 45_000 - 59_999);
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 45_000),
        Decision::Hold(Hold::OverrideRecent)
    );
    view.youngest_override_ms = Some(T0 + 45_000 - 60_000);
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 45_000),
        split("1", SplitKind::Emergency)
    );
}

#[test]
fn congestion_never_blocks_a_split() {
    let (mut policy, mut view) = sustained();
    view.congested = true;
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        split("1", SplitKind::Normal)
    );
}

/// The decision at `READY` for a ready split, after `edit` changed the instances.
fn reserve(edit: impl FnOnce(&mut View)) -> Decision {
    let (mut policy, mut view) = sustained();
    edit(&mut view);
    tick(&mut policy, &mut view, READY)
}

#[test]
fn a_split_needs_room_for_two_children_beside_the_parent_on_the_actor() {
    assert_eq!(
        reserve(|v| v.instances[0].resident_shards = 5),
        split("1", SplitKind::Normal)
    );
    assert_eq!(
        reserve(|v| v.instances[0].resident_shards = 6),
        Decision::AskInstance {
            resident: 6,
            max: 7
        }
    );
}

#[test]
fn a_split_needs_another_instance_with_room_for_one_child() {
    let ask = Decision::AskInstance {
        resident: 3,
        max: 7,
    };
    assert_eq!(
        reserve(|v| v.instances[1].resident_shards = 6),
        split("1", SplitKind::Normal)
    );
    assert_eq!(reserve(|v| v.instances[1].resident_shards = 7), ask);
    assert_eq!(reserve(|v| v.instances[1].accepting = false), ask);
    assert_eq!(reserve(|v| v.instances.truncate(1)), ask);
}

#[test]
fn a_shard_whose_resident_handles_reach_its_cap_splits_on_the_emergency_line() {
    let mut view = one_hot(10);
    view.leaves[2].load.insert(
        Axis::Streams,
        Usage {
            used: 8_192,
            safe: 8_192,
            top_entity: 0,
        },
    );
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 44_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 45_000),
        split("1", SplitKind::Emergency)
    );
}

#[test]
fn the_fleet_axis_doubles_a_one_leaf_cell_after_the_full_sustain_and_has_no_emergency_form() {
    let mut view = cell(vec![leaf("", "a", 10)]);
    view.split_demand = 3;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 45_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY - 1),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, READY), fleet("", true, 3));
}

#[test]
fn a_one_shard_cell_with_split_demand_three_reaches_four_shards_a_cooldown_apart() {
    let mut view = cell(vec![leaf("", "a", 10)]);
    view.split_demand = 3;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, READY), fleet("", true, 3));
    // Each split publishes and ends; the fleet's demand past the shards falls by one each time,
    // and the cell's sustain carries on across the operations.
    view.leaves = vec![leaf("0", "a", 10), leaf("1", "a", 10)];
    view.split_demand = 2;
    view.last_op_end_ms = Some(READY + 5_000);
    assert_eq!(
        tick(&mut policy, &mut view, READY + 64_999),
        Decision::Hold(Hold::Cooldown)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY + 65_000),
        fleet("0", true, 2)
    );
    view.leaves
        .splice(0..1, [leaf("00", "a", 10), leaf("01", "a", 10)]);
    view.split_demand = 1;
    view.last_op_end_ms = Some(READY + 70_000);
    assert_eq!(
        tick(&mut policy, &mut view, READY + 130_000),
        fleet("1", true, 2)
    );
    view.leaves
        .splice(2..3, [leaf("10", "a", 10), leaf("11", "a", 10)]);
    view.split_demand = 0;
    view.last_op_end_ms = Some(READY + 135_000);
    assert_eq!(
        tick(&mut policy, &mut view, READY + 195_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(view.leaves.len(), 4);
}

#[test]
fn a_fold_without_fleet_demand_restarts_the_fleet_sustain() {
    let mut view = cell(vec![leaf("", "a", 10)]);
    view.split_demand = 3;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.split_demand = 0;
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 300_000),
        Decision::Hold(Hold::Steady)
    );
    view.split_demand = 3;
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 301_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 900_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 901_000),
        fleet("", true, 3)
    );
}

#[test]
fn a_stale_fold_restarts_the_fleet_sustain() {
    let mut view = cell(vec![leaf("", "a", 10)]);
    view.split_demand = 3;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    view.beats_fresh = false;
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 300_000),
        Decision::Hold(Hold::BeatsStale)
    );
    view.beats_fresh = true;
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 301_000),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 900_999),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, T0 + 901_000),
        fleet("", true, 3)
    );
}

/// Four leaves at 30, 50, 40 and 20% after the fleet axis held `d` for its full sustain.
fn fleet_marked(d: u64) -> Decision {
    let mut view = cell(vec![
        leaf("00", "a", 30),
        leaf("01", "a", 50),
        leaf("10", "a", 40),
        leaf("11", "a", 20),
    ]);
    view.split_demand = d;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    tick(&mut policy, &mut view, READY)
}

#[test]
fn the_fleet_axis_marks_its_demands_worth_of_the_busiest_leaves() {
    assert_eq!(fleet_marked(1), fleet("01", false, 1));
    assert_eq!(fleet_marked(2), fleet("00", true, 2));
}

#[test]
fn the_fleet_axis_breaks_a_tie_deepest_first() {
    let mut view = cell(vec![
        leaf("0", "a", 40),
        leaf("10", "a", 40),
        leaf("11", "a", 10),
    ]);
    view.split_demand = 1;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(tick(&mut policy, &mut view, READY), fleet("10", false, 1));
}

#[test]
fn leaves_the_fleet_axis_marks_count_with_the_lines_toward_a_doubling() {
    for (d, decision) in [
        (0, split("00", SplitKind::Normal)),
        (1, fleet("00", false, 1)),
        (2, fleet("00", true, 2)),
    ] {
        let mut view = cell(vec![
            leaf("00", "a", 70),
            leaf("01", "a", 60),
            leaf("10", "a", 10),
            leaf("11", "a", 10),
        ]);
        view.split_demand = d;
        let mut policy = Policy::default();
        assert_eq!(
            tick(&mut policy, &mut view, T0),
            Decision::Hold(Hold::Steady)
        );
        assert_eq!(tick(&mut policy, &mut view, READY), decision, "d {d}");
    }
}

#[test]
fn a_hot_leaf_the_fleet_axis_marks_is_held() {
    let mut view = cell(vec![leaf("0", "a", 10), leaf("1", "a", 5)]);
    view.leaves[0].load.insert(
        Axis::Appends,
        Usage {
            used: 10,
            safe: 100,
            top_entity: 5,
        },
    );
    view.split_demand = 1;
    let mut policy = Policy::default();
    assert_eq!(
        tick(&mut policy, &mut view, T0),
        Decision::Hold(Hold::Steady)
    );
    assert_eq!(
        tick(&mut policy, &mut view, READY),
        Decision::Hold(Hold::HotEntity)
    );
}
