#![cfg(test)]
//! The split's shared pieces, and the split end to end (S4-DESIGN §6).

use std::ops::Bound;

use bytes::Bytes;

use super::{Lists, key_bounds, pause_lists, seed_rows, supplements};
use crate::keyspace::SENTINEL;
use crate::spill::gc::ObjectKind;
use crate::topology::sentinel::Counts;

fn at(bytes: [u8; 16]) -> Bytes {
    Bytes::copy_from_slice(&bytes)
}

/// **A child's keys run from its range's first hash to the next prefix's, never into the
/// sentinel.** `0` runs from the start to `0x80…`; `1` from `0x80…` to the sentinel; `10` from
/// `0x80…` to `0xC0…`.
#[test]
fn a_childs_keys_run_from_its_first_hash_to_the_next_prefixs() {
    let mut half = [0u8; 16];
    half[0] = 0x80;
    let mut quarter = [0u8; 16];
    quarter[0] = 0xC0;
    assert_eq!(
        key_bounds("0"),
        (Bound::Unbounded, Bound::Excluded(at(half)))
    );
    assert_eq!(
        key_bounds("1"),
        (Bound::Included(at(half)), Bound::Excluded(at(SENTINEL)))
    );
    assert_eq!(
        key_bounds("10"),
        (Bound::Included(at(half)), Bound::Excluded(at(quarter)))
    );
}

/// **A child's seed holds its routed rows, its counts and its own `'Y'`, one row per key, in
/// order.**
#[test]
fn a_childs_seed_holds_its_rows_counts_and_identity_in_order() {
    let routed = vec![(vec![0xFF; 17], vec![1]), (vec![0xFF; 17], vec![2])];
    let id = crate::spill::object::ObjectId {
        home: 1,
        epoch: 1,
        seq: 1,
    };
    let counts = Counts {
        refcounts: vec![(id, crate::spill::gc::Refcount::default())],
        orphans: vec![],
    };
    let seed = seed_rows(&routed, &counts, 9);
    let (y_key, y_value) = crate::shard::open_index::identity_row(9);
    assert!(
        seed.windows(2).all(|w| w[0].0 < w[1].0),
        "in order, one per key"
    );
    assert!(seed.contains(&(y_key, y_value)), "its own identity");
    assert!(
        seed.iter()
            .any(|(k, _)| *k == crate::spill::gc::refcount_key(id))
    );
    assert_eq!(
        seed.len(),
        3,
        "the routed key once, the count, the identity"
    );
}

/// **A split's future is `Send`**, so a supervised task (package 1's policy loop) can run it: it
/// holds nothing across an await that cannot cross threads. The check is the compiler's; the test
/// only names it.
#[test]
fn a_splits_future_is_send() {
    fn send<T: Send>(_: &T) {}
    fn check(
        dir: &crate::shard_directory::ShardDirectory,
        parent: &std::sync::Arc<crate::shard::ShardEngine>,
        clock: &dyn crate::runtime::Clock,
    ) {
        let split = super::split(dir, parent, "op", "owner", clock);
        send(&split);
    }
    let _checked: fn(
        &crate::shard_directory::ShardDirectory,
        &std::sync::Arc<crate::shard::ShardEngine>,
        &dyn crate::runtime::Clock,
    ) = check;
}

/// **The pause's `-d` lists hold exactly the holdings Preparing's lists lack** (S4-DESIGN §7.3).
/// Preparing listed a segment and a run index of home 0. By the pause a seal added a segment of
/// home 0 and a run index of home 3 appeared, and the listed run index is gone (a holding removed
/// is not supplemented). The `-d` lists hold the new segment under home 0 and the run index under
/// home 3; the same lists again add nothing.
#[test]
fn the_pauses_supplements_hold_exactly_what_preparing_lacks() {
    let (seg, run) = (ObjectKind::Segment, ObjectKind::RunIndex);
    let put = Lists::from([(0, vec![(seg, 1, 1), (run, 1, 2)])]);
    let now = Lists::from([(0, vec![(seg, 1, 1), (seg, 1, 3)]), (3, vec![(run, 2, 9)])]);
    let want = Lists::from([(0, vec![(seg, 1, 3)]), (3, vec![(run, 2, 9)])]);
    assert_eq!(supplements(&put, &now), want);
    assert_eq!(supplements(&now, &now), Lists::new());
}

/// **A home the child first holds in the pause gets its reference list, not a `-d` list** (S4-DESIGN
/// §7.3; S6's readers require a reference list for every ancestor home). Of the pause's additions
/// above, the segment of home 0 (which Preparing listed) goes to home 0's `-d` list, and the run
/// index of home 3 (which it did not) to home 3's reference list.
#[test]
fn a_home_first_held_in_the_pause_gets_its_reference_list() {
    let (seg, run) = (ObjectKind::Segment, ObjectKind::RunIndex);
    let put = Lists::from([(0, vec![(seg, 1, 1)])]);
    let added = Lists::from([(0, vec![(seg, 1, 3)]), (3, vec![(run, 2, 9)])]);
    let (first, more) = pause_lists(&put, added);
    assert_eq!(first, Lists::from([(3, vec![(run, 2, 9)])]));
    assert_eq!(more, Lists::from([(0, vec![(seg, 1, 3)])]));
    assert_eq!(
        pause_lists(&Lists::new(), Lists::new()),
        (Lists::new(), Lists::new())
    );
}
