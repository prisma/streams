#![cfg(test)]
//! Where an engine finds objects once a split made it (S4 route A): its own under its log's path in
//! the topology, an ancestor's under that home's path, and any home the topology does not name by
//! S3a's static rule.

use std::sync::Arc;

use super::super::gc_tests::gc_rig;
use crate::topology::model::Snapshot;

/// **A child finds its own objects under its topology path and its parent's under the parent's.**
/// After a split of `1` into `10` (home 2) and `11` (home 3), an engine placed at `shards/10-2`
/// finds its own home's objects there and home 1's under `shards/1`; a home the topology does not
/// name keeps the static root.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_child_finds_its_own_and_its_parents_objects_through_the_topology() {
    let rig = gc_rig("roots").await;
    let topology = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let (reserved, homes) = topology.reserve_homes();
    let split = reserved.split("1", homes, "op").unwrap();
    let snapshot = Arc::new(Snapshot {
        shards: split.shards,
        etag: None,
        installed_at: None,
    });
    let own = rig.engine.home();
    let static_root = rig.engine.object_root(77);
    rig.engine.place(
        "shards/10-2".to_string(),
        Arc::new(move || Some(snapshot.clone())),
    );
    assert_eq!(rig.engine.log_path(), "shards/10-2");
    assert_eq!(rig.engine.object_root(own), "shards/10-2", "its own home");
    assert_eq!(rig.engine.object_root(1), "shards/1", "its parent's home");
    assert_eq!(
        rig.engine.object_root(77),
        static_root,
        "a home the topology does not name"
    );
    rig.engine.begin_close();
}
