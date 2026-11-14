#![cfg(test)]
//! The topology loop's pieces that need no rig (the loop itself: `dst_tests::split_k`).

use super::route_of;

/// **A prefix's first route hash is its bits, then zeros.** The root's is all zeros; `1` sets the
/// top bit; `01` the second; a 24-bit prefix fills three bytes.
#[test]
fn a_prefixs_first_route_is_its_bits_then_zeros() {
    assert_eq!(route_of(""), [0; 16]);
    assert_eq!(route_of("1")[0], 0x80);
    assert_eq!(route_of("01")[0], 0x40);
    let deep = route_of("101010101010101010101010");
    assert_eq!(deep[..4], [0xAA, 0xAA, 0xAA, 0x00]);
}

/// **A split fits while the parent and both children stay within the instance's maximum.** At
/// four open of six, it goes ahead; at five, it is refused `instance_full` with seven resident.
#[test]
fn a_split_fits_exactly_while_the_parent_and_both_children_stay_within_the_maximum() {
    assert!(super::room_for_split(4, 6).is_ok());
    match super::room_for_split(5, 6) {
        Err(super::Refused::InstanceFull { resident, max }) => assert_eq!((resident, max), (7, 6)),
        other => panic!("five open of six: {other:?}"),
    }
}

/// **The fleet settled once its count is at least the settle time old and no override is younger
/// than the move cooldown, both at the millisecond.**
#[test]
fn the_fleet_settles_at_its_settle_time_and_its_move_cooldown_exactly() {
    use super::{FLEET_SETTLE_MS, settled};
    let now = 1_000_000;
    let cooldown = 30_000;
    let none = std::iter::empty;
    assert!(settled(now, None, none(), cooldown), "no fleet documents");
    assert!(settled(now, Some(now - FLEET_SETTLE_MS), none(), cooldown));
    assert!(!settled(
        now,
        Some(now - FLEET_SETTLE_MS + 1),
        none(),
        cooldown
    ));
    assert!(settled(now, None, [now - cooldown].into_iter(), cooldown));
    assert!(!settled(
        now,
        None,
        [now - cooldown + 1].into_iter(),
        cooldown
    ));
    assert!(
        !settled(now, None, [0, now - cooldown + 1].into_iter(), cooldown),
        "any young override"
    );
}
