#![cfg(test)]
//! The merge's topology change (S6 §7.2 step 4): one fresh home reserved; the two halves replaced
//! by their parent prefix at it, born at the next version with both halves' homes as parents, both
//! retired at it; refused for halves that are not both leaves or a home that is not fresh.

use crate::topology::model::Topology;

/// A new cell's four shards `00` `01` `10` `11`, homes 0 to 3.
fn four() -> Topology {
    crate::config::validation::InitialShards::new(4)
        .expect("four shards")
        .topology(1 << 20)
}

#[test]
fn a_merge_replaces_two_halves_by_their_parent_at_a_fresh_home() {
    let (reserved, home) = four().reserve_home();
    assert_eq!(home, 4);
    assert_eq!(reserved.shards.next_home, 5);
    assert_eq!(reserved.version, 1, "a reservation routes nothing");
    let merged = reserved.merged("0", home, "op-m").unwrap();
    assert_eq!(merged.version, 2);
    let leaves: Vec<(&str, u32)> = merged
        .shards
        .leaves
        .iter()
        .map(|(p, h)| (p.as_str(), *h))
        .collect();
    assert_eq!(leaves, [("0", 4), ("10", 2), ("11", 3)]);
    assert_eq!(merged.shards.shards, ["0", "10", "11"]);
    let born = &merged.shards.homes[&4];
    assert_eq!(
        (born.prefix.as_str(), born.path.as_str()),
        ("0", "shards/0~4")
    );
    assert_eq!((born.parents.clone(), born.born_version), (vec![0, 1], 2));
    assert_eq!(merged.shards.homes[&0].retired_version, Some(2));
    assert_eq!(merged.shards.homes[&1].retired_version, Some(2));
    assert_eq!(merged.shards.homes[&2].retired_version, None);
    let op = merged.last_op.unwrap();
    assert_eq!((op.kind.as_str(), op.children), ("merge", vec![4]));
    assert_eq!(op.sources, [("00".to_string(), 0), ("01".to_string(), 1)]);
}

#[test]
fn a_merge_of_non_leaves_or_onto_a_used_home_is_refused() {
    let (reserved, home) = four().reserve_home();
    assert!(reserved.merged("1", 0, "op").is_err(), "home 0 is in use");
    assert!(
        reserved.merged("1", 9, "op").is_err(),
        "home 9 was never reserved"
    );
    let merged = reserved.merged("0", home, "op").unwrap();
    let (again, next) = merged.reserve_home();
    assert!(
        again.merged("0", next, "op").is_err(),
        "00 and 01 are no leaves now"
    );
}
