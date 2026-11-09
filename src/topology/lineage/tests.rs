#![cfg(test)]
//! Which retired homes the lineage loop sweeps, by which leaf, reading which
//! holders: nothing before the version has aged here; every retired home that
//! is not a leaf; a list of a home that is not a leaf ignored; the smallest
//! holder sweeps, or the smallest leaf when none holds anything.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use super::{Job, LINEAGE_GRACE, plan};
use crate::runtime::{Clock, ManualClock, MonotonicNow};
use crate::topology::model::{Home, ShardSet, Snapshot};

fn home(path: &str, prefix: &str, retired: Option<u64>) -> Home {
    Home {
        path: path.into(),
        prefix: prefix.into(),
        parents: Vec::new(),
        born_version: 1,
        retired_version: retired,
    }
}

/// Home 0 (the root, retired by a split at version 2) with leaves "0" (home 1)
/// and "1" (home 2); home 3, an abandoned attempt, was never a leaf; home 4
/// ("00", retired too, by a later merge) left a second lineage.
fn snapshot(installed_at: Option<MonotonicNow>) -> Snapshot {
    let homes = BTreeMap::from([
        (0, home("shards/root", "", Some(2))),
        (1, home("shards/0~1", "0", None)),
        (2, home("shards/1~2", "1", None)),
        (4, home("shards/00~4", "00", Some(3))),
    ]);
    Snapshot {
        shards: ShardSet {
            version: 3,
            shards: vec!["0".into(), "1".into()],
            homes,
            leaves: BTreeMap::from([("0".into(), 1), ("1".into(), 2)]),
            next_home: 5,
        },
        etag: None,
        installed_at,
    }
}

#[test]
fn the_loop_sweeps_each_retired_home_by_its_smallest_holder_once_the_version_aged() {
    let clock = ManualClock::at(0);
    let at = clock.monotonic();
    let listed = BTreeMap::from([(0, BTreeSet::from([2, 1, 3])), (4, BTreeSet::new())]);

    assert_eq!(
        plan(&snapshot(None), &listed, at),
        [],
        "not yet installed here"
    );
    clock.advance_monotonic(LINEAGE_GRACE - Duration::from_millis(1));
    assert_eq!(
        plan(&snapshot(Some(at)), &listed, clock.monotonic()),
        [],
        "within the grace"
    );
    clock.advance_monotonic(Duration::from_millis(1));
    let jobs = plan(&snapshot(Some(at)), &listed, clock.monotonic());
    assert_eq!(
        jobs,
        [
            Job {
                home: 0,
                root: "shards/root".into(),
                by: "0".into(),
                holders: vec![1, 2],
            },
            Job {
                home: 4,
                root: "shards/00~4".into(),
                by: "0".into(),
                holders: vec![],
            },
        ],
        "home 3's list is ignored (never a leaf); home 4, held by none, is swept by the smallest leaf"
    );
}

/// The holders under a root are the homes named by its `lineage/refs/` keys;
/// release logs and other names are not holders.
#[tokio::test(crate = "crate::test_rt")]
async fn the_holders_under_a_root_are_its_reference_lists() {
    use object_store::ObjectStoreExt;
    let store = object_store::memory::InMemory::new();
    for path in [
        "shards/root/lineage/refs/2",
        "shards/root/lineage/refs/11",
        "shards/root/lineage/refs/x",
        "shards/root/lineage/rel/5/all",
        "shards/root/hseg/0000000000000001/0000000000000001",
        "shards/0~1/lineage/refs/7",
    ] {
        store
            .put(&path.into(), bytes::Bytes::from_static(b"x").into())
            .await
            .unwrap();
    }
    let homes = super::listed_under(&store, "shards/root").await.unwrap();
    assert_eq!(homes, BTreeSet::from([2, 11]));
}
