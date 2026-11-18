#![cfg(test)]
//! Where an engine finds objects once a split made it (S4 route A): its own under its log's path in
//! the topology, an ancestor's under that home's path, and any home the topology does not name by
//! S3a's static rule.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use object_store::ObjectStore;

use super::super::gc_tests::gc_rig;
use crate::shard::ShardEngine;
use crate::topology::model::Snapshot;

/// **A child finds its own objects under its topology path and its parent's under the parent's.**
/// After a split of `1` into `10` (home 2) and `11` (home 3), an engine built placed at
/// `shards/10-2` (`ShardEngine::placed`) finds its own home's objects there and home 1's under
/// `shards/1`; a home the topology does not name keeps the static root.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_child_finds_its_own_and_its_parents_objects_through_the_topology() {
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
    let topology: crate::topology::fence::SnapshotFn = Arc::new(move || Some(snapshot.clone()));
    let rig = ShardEngine::placed("shards/10-2".to_string(), topology, gc_rig("roots")).await;
    let own = rig.engine.home();
    let static_root = crate::sharddir::object_root(77, rig.engine.prefix.len());
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

/// **A born home whose number is the one its prefix reads as is built where it lives.** The first
/// split of `10` in a four-shard cell gives `101` home 5, and `101` read as a number is 5: no fact
/// inside the engine tells this home from an initial one (GATE-DECISION-born-home-seal-root, ruling
/// 2). Opened through the directory's fenced open at `shards/101-5`, its engine answers that path,
/// its own root and its parent's (home 2, `shards/10`) already before its opener returns; the same
/// engine started outside any open answers the static `shards/101` and no parent root.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_born_home_numbered_as_its_prefix_reads_is_built_at_its_born_path() {
    let topology = crate::config::validation::InitialShards::new(4)
        .unwrap()
        .topology(1 << 20);
    let (reserved, homes) = topology.reserve_homes();
    let shards = reserved.split("10", homes, "op").unwrap().shards;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let seen = Arc::new(Mutex::new(None));
    let (opener_store, saw) = (store.clone(), seen.clone());
    let opener: crate::sharddir::OpenAt = Box::new(
        move |prefix: String, path: String, _: crate::sharddir::EngineIncarnation| {
            let (store, saw) = (opener_store.clone(), saw.clone());
            Box::pin(async move {
                let engine = born(&store, &prefix, &path).await;
                let found = roots(&engine).await;
                *saw.lock().unwrap() = Some(found);
                Ok(engine)
            })
        },
    );
    let timing = crate::shard_directory::OpenTiming {
        open_deadline: Duration::from_secs(60),
        open_wait: Duration::from_millis(500),
    };
    let ownership = crate::ownership::OwnershipService::new("");
    let dir = crate::shard_directory::ShardDirectory::new_at(shards, ownership, timing, |_| opener);
    let outcome = dir
        .gate_for_tests()
        .get_or_open("101", Duration::from_secs(10))
        .await;
    let crate::sharddir::OpenOutcome::Ready(served) = outcome else {
        panic!("101 opens");
    };
    let at = |log: &str, own: &str, parent: Option<&str>| {
        (log.to_string(), own.to_string(), parent.map(str::to_string))
    };
    let placed = at("shards/101-5", "shards/101-5", Some("shards/10"));
    let before = seen.lock().unwrap().clone();
    assert_eq!(before, Some(placed.clone()), "before its opener returned");
    assert_eq!(roots(&served).await, placed, "served");
    served.begin_close();
    let other: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let outside = born(&other, "101", "shards/101-5").await;
    let unplaced = at("shards/101", "shards/101", None);
    assert_eq!(roots(&outside).await, unplaced, "outside any open");
    outside.begin_close();
}

/// `engine`'s log path, its own home's root and its parent's (home 2), its home recorded first.
async fn roots(engine: &ShardEngine) -> (String, String, Option<String>) {
    let home = engine.record_home().await.unwrap();
    assert_eq!(home, 5, "the 'Y' row's home");
    (
        engine.log_path(),
        engine.object_root(home),
        engine.home_root(2),
    )
}

/// An engine started on a fresh log at `path` whose `'Y'` row names home 5.
async fn born(store: &Arc<dyn ObjectStore>, prefix: &str, path: &str) -> Arc<ShardEngine> {
    let db = crate::keyspace::db_builder(path, store.clone())
        .build()
        .await
        .unwrap();
    let (key, value) = crate::shard::open_index::identity_row(5);
    db.put(&key, &value).await.unwrap();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        None,
        maintenance,
    )
}
