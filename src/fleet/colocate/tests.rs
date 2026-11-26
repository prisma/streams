#![cfg(test)]
//! A co-location's parts: the target every holder computes alike, and each holder's step (it hands
//! its half to the target or waits; it never writes for a half the target already holds,
//! GATE-DECISION-s22-join-2026-11-20 Addendum 13 (1)).

use std::collections::BTreeMap;

use super::{Step, colocation_target, step};
use crate::topology::policy::{Instance, Leaf, View};

fn instance(id: &str, resident_shards: u32, accepting: bool) -> Instance {
    Instance {
        id: id.into(),
        resident_shards,
        accepting,
        within_count: true,
    }
}

fn leaf(prefix: &str, owner: &str) -> Leaf {
    Leaf {
        prefix: prefix.into(),
        born_ms: 0,
        owner: owner.into(),
        beat_ms: None,
        load: BTreeMap::new(),
    }
}

/// The pair `0` split over `a` (`00`) and `b` (`01`) as `me` sees it: `a` and `b` holding `na` and
/// `nb` shards (`b` accepting as `b_accepting`), and a third `c` when given.
fn view(me: &str, (na, nb): (u32, u32), c: Option<(u32, bool)>, b_accepting: bool) -> View {
    View {
        now_ms: 0,
        me: me.into(),
        instances: [instance("a", na, true), instance("b", nb, b_accepting)]
            .into_iter()
            .chain(c.map(|(n, ok)| instance("c", n, ok)))
            .collect(),
        leaves: vec![leaf("00", "a"), leaf("01", "b")],
        split_demand: 0,
        live: 3,
        beats_fresh: true,
        intent_live: false,
        last_op_end_ms: None,
        desired_computed_ms: None,
        youngest_override_ms: None,
        congested: false,
    }
}

fn halves() -> [String; 2] {
    ["00".to_string(), "01".to_string()]
}

/// A split pair's co-location target (`colocation_target`): the actor when it has room for the
/// other half and the merged leaf (+2), else the partner holding `…1` (+2) when it accepts, else
/// the accepting instance with the fewest shards that has room for both halves and the merged leaf
/// (+3); none when no instance has room.
#[test]
fn a_split_pairs_halves_go_to_the_first_instance_with_room() {
    let target = |v: &View| colocation_target(v, 3, &halves());
    let at = |pair, c, b_accepting| view("a", pair, c, b_accepting);
    assert_eq!(
        target(&at((1, 3), None, true)).as_deref(),
        Some("a"),
        "the actor has room"
    );
    assert_eq!(
        target(&at((2, 1), None, true)).as_deref(),
        Some("b"),
        "the partner has room"
    );
    assert_eq!(
        target(&at((2, 1), None, false)),
        None,
        "a partner that does not accept"
    );
    assert_eq!(
        target(&at((2, 2), Some((0, true)), true)).as_deref(),
        Some("c"),
        "a third with +3"
    );
    assert_eq!(
        target(&at((2, 2), Some((1, true)), true)),
        None,
        "a third with 1 + 3 > 3"
    );
    assert_eq!(
        target(&at((2, 2), Some((0, false)), true)),
        None,
        "a third that does not accept"
    );
    let mut holds_another = at((2, 2), Some((0, true)), true);
    holds_another.leaves.push(leaf("1", "c"));
    assert_eq!(
        target(&holds_another).as_deref(),
        Some("c"),
        "a third holding a leaf other than the half"
    );
}

/// **Both holders compute one target** (the F6 addendum's C2): for each layout above, the view of
/// `00`'s holder `a`, of `01`'s holder `b` and of the third `c` give the same target.
#[test]
fn a_split_pairs_target_is_the_same_from_either_holders_view() {
    let layouts = [
        ((1, 3), None, true),
        ((2, 1), None, true),
        ((2, 1), None, false),
        ((2, 2), Some((0, true)), true),
        ((2, 2), Some((1, true)), true),
    ];
    for (pair, c, b_accepting) in layouts {
        let targets: Vec<Option<String>> = ["a", "b", "c"]
            .iter()
            .map(|me| colocation_target(&view(me, pair, c, b_accepting), 3, &halves()))
            .collect();
        assert!(
            targets.iter().all(|t| *t == targets[0]),
            "{pair:?} {c:?}: {targets:?}"
        );
    }
}

/// `me`'s step for the pair `0` of `view(me, pair, c, true)`.
fn step_of(me: &str, pair: (u32, u32), c: Option<(u32, bool)>) -> Step {
    step(&view(me, pair, c, true), 3, "0")
}

fn hand(half: &str, to: &str) -> Step {
    Step::Hand {
        half: half.into(),
        to: to.into(),
    }
}

/// **The target writes nothing, and the other holder hands its half there at once**
/// (GATE-DECISION-s22-join-2026-11-20 Addendum 13 (1); Addendum 7's pin-then-hand ordering is
/// withdrawn for it): with the actor `a` the target, `a` waits (its view gives it `00`, by an entry
/// or by the ring) and `b` hands `01` to `a`; with the partner `b` the target, `b` waits and `a`
/// hands `00` to `b`.
#[test]
fn the_target_writes_nothing_and_the_other_holder_hands_its_half_at_once() {
    assert_eq!(step_of("a", (1, 3), None), Step::Wait);
    assert_eq!(step_of("b", (1, 3), None), hand("01", "a"));
    assert_eq!(step_of("b", (2, 1), None), Step::Wait);
    assert_eq!(step_of("a", (2, 1), None), hand("00", "b"));
}

/// **A third instance takes both halves, each handed by its holder at once**; with no instance
/// that has room neither holder writes and only the actor asks for one; an instance holding
/// neither half has nothing to do.
#[test]
fn a_third_target_takes_each_half_from_its_holder_and_no_room_writes_nothing() {
    let third = Some((0, true));
    assert_eq!(step_of("a", (2, 2), third), hand("00", "c"));
    assert_eq!(step_of("b", (2, 2), third), hand("01", "c"));
    assert_eq!(step_of("c", (2, 2), third), Step::Wait);
    assert_eq!(step_of("a", (2, 2), None), Step::NoRoom);
    assert_eq!(step_of("b", (2, 2), None), Step::Wait);
}

/// `view` with `ids` beyond the count (`Instance::within_count`).
fn beyond(mut view: View, ids: &[&str]) -> View {
    for instance in &mut view.instances {
        if ids.contains(&instance.id.as_str()) {
            instance.within_count = false;
        }
    }
    view
}

/// **A member beyond the count is no target, as the actor, the partner or another, and starts
/// none** (C11; GATE-DECISION-s22-join-2026-11-20 Addendum 41 B, item 10). Under a ceiling of 4:
/// the actor `a` with room but beyond leaves the target to the partner `b`; the partner with room
/// but beyond leaves it to `c`; `c` beyond leaves none. A holder beyond the count waits where it
/// would ask for room or hand its half.
#[test]
fn a_member_beyond_the_count_is_no_target_and_starts_none() {
    let target = |view: &View| colocation_target(view, 4, &halves());
    let actor_beyond = beyond(view("b", (2, 2), None, true), &["a"]);
    assert_eq!(target(&actor_beyond).as_deref(), Some("b"));
    let partner_beyond = beyond(view("a", (3, 2), Some((0, true)), true), &["b"]);
    assert_eq!(target(&partner_beyond).as_deref(), Some("c"));
    let other_beyond = beyond(view("a", (3, 3), Some((0, true)), true), &["c"]);
    assert_eq!(target(&other_beyond), None);
    let asks = beyond(view("a", (3, 3), None, true), &["a"]);
    assert_eq!(
        step(&asks, 4, "0"),
        Step::Wait,
        "the actor beyond asks for none"
    );
    let hands = beyond(view("b", (3, 3), Some((0, true)), true), &["b"]);
    assert_eq!(
        step(&hands, 4, "0"),
        Step::Wait,
        "the partner beyond hands none"
    );
    let unaffected = view("b", (3, 3), Some((0, true)), true);
    assert_eq!(step(&unaffected, 4, "0"), hand("01", "c"));
}
