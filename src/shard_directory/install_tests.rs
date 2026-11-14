#![cfg(test)]
//! `ShardDirectory::install` (S4): only a topology at least as new as the held one installs, its
//! install instant kept for the same version and taken for a newer one, and the engines of
//! prefixes that are no longer leaves retired, except a parent its split holds parked; and the
//! directory's one topology handle.

use std::sync::Arc;
use std::time::Duration;

use super::{OpenTiming, ShardDirectory};
use crate::runtime::Clock;
use crate::sharddir::{EngineIncarnation, OpenFn, OpenOutcome};
use crate::topology::model::Topology;

/// A cell's four shards `00` `01` `10` `11` at version 1.
fn four() -> Topology {
    crate::config::validation::InitialShards::new(4)
        .expect("four shards")
        .topology(1 << 20)
}

/// A directory over `topology` whose opener opens a real engine per prefix in one memory store.
fn directory(topology: &Topology) -> ShardDirectory {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let opener: OpenFn = Box::new(move |prefix: String, _inc: EngineIncarnation| {
        let store = store.clone();
        Box::pin(async move {
            let path = crate::sharddir::shard_db_path(&prefix);
            let db = crate::keyspace::db_builder(path.as_str(), store.clone())
                .build()
                .await?;
            let maintenance = crate::shard::load_or_rebuild_maintenance(&db).await?;
            Ok(crate::shard::ShardEngine::start(
                prefix,
                Arc::new(db),
                store,
                crate::shard::ShardConfig::default(),
                None,
                maintenance,
            ))
        })
    });
    let timing = OpenTiming {
        open_deadline: Duration::from_secs(10),
        open_wait: Duration::from_secs(10),
    };
    let ownership = crate::ownership::OwnershipService::new("");
    ShardDirectory::new(topology.shards.clone(), ownership, timing, |_| opener)
}

async fn opened(dir: &ShardDirectory, prefix: &str) -> Arc<crate::shard::ShardEngine> {
    match dir.open_or_wait(prefix, Duration::from_secs(10)).await {
        OpenOutcome::Ready(engine) => engine,
        _ => panic!("{prefix} did not open"),
    }
}

/// **A topology installs only at its version or a newer one; the instant is kept for the same
/// version; a gone leaf's engine is retired unless its split holds it parked.** Version 1 again
/// at t0 installs and takes t0. The merge of `0` at version 2, at t1, installs and takes t1, and
/// retires `00`, but not `01`, which a split's barrier holds parked. Version 2 again at t2 keeps
/// t1. Version 1 after it is refused and leaves version 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn install_takes_no_older_version_keeps_its_instant_and_retires_unparked_gone_leaves() {
    let clock = crate::runtime::ManualClock::at(0);
    let v1 = four();
    let dir = directory(&v1);
    let (left, right) = (opened(&dir, "00").await, opened(&dir, "01").await);
    let t0 = clock.monotonic();
    assert!(dir.install(v1.shards.clone(), None, t0), "the same version");
    assert_eq!(dir.prefixes().installed_at, Some(t0));
    let (reserved, home) = v1.reserve_home();
    let v2 = reserved.merged("0", home, "op-m").unwrap();
    right.gc_state().gate.set_parked(true);
    clock.advance_monotonic(Duration::from_secs(1));
    let t1 = clock.monotonic();
    assert!(dir.install(v2.shards.clone(), None, t1), "a newer version");
    assert_eq!(
        (dir.prefixes().shards.version, dir.prefixes().installed_at),
        (2, Some(t1))
    );
    assert!(!dir.is_open("00"), "a gone leaf is retired");
    assert!(dir.is_open("01"), "a parked parent waits for its split");
    clock.advance_monotonic(Duration::from_secs(1));
    assert!(dir.install(v2.shards.clone(), None, clock.monotonic()));
    assert_eq!(
        dir.prefixes().installed_at,
        Some(t1),
        "the same version keeps its instant"
    );
    assert!(
        !dir.install(v1.shards.clone(), None, clock.monotonic()),
        "an older version"
    );
    assert_eq!(dir.prefixes().shards.version, 2);
    right.gc_state().gate.set_parked(false);
    drop((left, right));
    drop(dir.shutdown(Duration::from_secs(5)).await);
}

/// The directory has one topology handle: every call returns it, so a loop attached to it
/// (`topology::ops::start`) is the one every route reaches.
#[test]
fn the_directory_hands_out_one_topology_handle() {
    let dir = directory(&four());
    assert!(std::ptr::eq(dir.topology_ops(), dir.topology_ops()));
}
