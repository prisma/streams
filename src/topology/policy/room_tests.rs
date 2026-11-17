#![cfg(test)]
//! A full actor's split and the instance with room for its shard
//! (GATE-DECISION-f2-convergence-2026-11-17): the pick (`room_for_split`) and the reservation that
//! lets the split go to S4 when another instance has room.

use super::tests::{reserve, split};
use super::{Decision, Instance, SplitKind, View};

/// GATE-DECISION-f2-convergence-2026-11-17: an actor without room for the children still splits
/// when another accepting instance has room for the shard and both children (resident + 3 <= max),
/// at `FLEET_MAX` too: S4 refuses it `instance_full` and the loop moves the shard there. With no
/// such instance it asks.
#[test]
fn a_full_actors_split_goes_ahead_when_another_instance_has_room_for_the_shard() {
    let with_peer = |peer: u32, accepting: bool, live: u32| {
        move |v: &mut View| {
            v.instances[0].resident_shards = 6;
            v.instances[1].resident_shards = peer;
            v.instances[1].accepting = accepting;
            v.live = live;
        }
    };
    let ask = Decision::AskInstance {
        resident: 6,
        max: 7,
    };
    assert_eq!(
        reserve(with_peer(3, true, 2)),
        split("1", SplitKind::Normal)
    );
    assert_eq!(
        reserve(with_peer(4, true, 2)),
        split("1", SplitKind::Normal),
        "4 + 3 <= 7"
    );
    assert_eq!(reserve(with_peer(5, true, 2)), ask, "5 + 3 > 7");
    assert_eq!(reserve(with_peer(3, false, 2)), ask, "not accepting");
    assert_eq!(
        reserve(with_peer(3, true, 4)),
        split("1", SplitKind::Normal),
        "at FLEET_MAX"
    );
}

fn room_view(instances: &[(&str, u32, bool)]) -> View {
    View {
        now_ms: 0,
        me: "streams-1".to_owned(),
        leaves: Vec::new(),
        instances: instances
            .iter()
            .map(|(id, resident_shards, accepting)| Instance {
                id: (*id).to_owned(),
                resident_shards: *resident_shards,
                accepting: *accepting,
            })
            .collect(),
        split_demand: 0,
        live: 0,
        beats_fresh: true,
        intent_live: false,
        last_op_end_ms: None,
        desired_computed_ms: None,
        youngest_override_ms: None,
        congested: false,
    }
}

/// **The target has room for the shard and its two children, accepts, is not this instance,
/// and holds the fewest shards; none gives no target.** With three shards an instance at most,
/// an instance holding 0 has room (0 + 3 <= 3) and one holding 1 has not; a non-accepting
/// empty instance is skipped; of two with room the one with fewer shards wins, then the first
/// by name; this instance is never its own target.
#[test]
fn the_target_is_the_accepting_instance_with_room_and_fewest_shards() {
    let pick = |instances: &[(&str, u32, bool)], max| {
        super::room_for_split(&room_view(instances), max, "streams-1")
    };
    assert_eq!(
        pick(&[("streams-1", 2, true), ("streams-2", 0, true)], 3).as_deref(),
        Some("streams-2")
    );
    assert_eq!(
        pick(&[("streams-1", 2, true), ("streams-2", 1, true)], 3),
        None,
        "1 + 3 > 3"
    );
    assert_eq!(
        pick(&[("streams-1", 2, true), ("streams-2", 0, false)], 3),
        None,
        "not accepting"
    );
    assert_eq!(pick(&[("streams-1", 0, true)], 3), None, "never itself");
    let three = [
        ("streams-1", 4, true),
        ("streams-3", 1, true),
        ("streams-2", 1, true),
        ("streams-4", 0, true),
    ];
    assert_eq!(
        pick(&three, 4).as_deref(),
        Some("streams-4"),
        "fewest shards"
    );
    assert_eq!(
        pick(&three[..3], 4).as_deref(),
        Some("streams-2"),
        "then by name"
    );
    assert_eq!(pick(&three[..3], 3), None, "none has room");
}
