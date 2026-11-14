#![cfg(test)]
//! The fence's table (S4-DESIGN §6.3), row by row.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::{Source, Verdict, decide};
use crate::sharddir::{EngineIncarnation, OpenOutcome};

/// **Every row of the table.** No `'T'`: an unnamed or terminal intent serves, a live one holds, a
/// lapsed one rolls back. With `'T'`: a snapshot at or past its version is retired whatever the
/// intent; before it, a live intent holds and a lapsed or no intent rolls forward.
#[test]
fn the_fence_decides_each_row_of_its_table() {
    let rows = [
        (None, Source::Unnamed, 1, Verdict::Serve),
        (None, Source::Live, 1, Verdict::Hold),
        (None, Source::Lapsed, 1, Verdict::RollBack),
        (Some(2), Source::Lapsed, 1, Verdict::RollForward),
        (Some(2), Source::Lapsed, 2, Verdict::Retired),
        (Some(2), Source::Live, 1, Verdict::Hold),
        (Some(2), Source::Unnamed, 1, Verdict::RollForward),
        (Some(2), Source::Live, 2, Verdict::Retired),
        (Some(2), Source::Unnamed, 3, Verdict::Retired),
        (Some(2), Source::Live, 3, Verdict::Retired),
    ];
    for (retired, source, snapshot, verdict) in rows {
        assert_eq!(
            decide(retired, source, snapshot),
            verdict,
            "{retired:?} {source:?} {snapshot}"
        );
    }
}

/// **The fenced open fabricates no log** (#251 amendment R3-d): through the directory's gate, which
/// skips the by-prefix door's own check, an open of `10` under the leaves `0` and `1` fails
/// `not_a_leaf` before the log opener (which would create `shards/10`) is called.
#[tokio::test(crate = "crate::test_rt")]
async fn a_fenced_open_of_a_prefix_that_is_no_leaf_never_reaches_the_log() {
    let topology = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let handed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = handed.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let opener: crate::sharddir::OpenAt = Box::new(
        move |_prefix: String, path: String, _inc: EngineIncarnation| {
            c.fetch_add(1, Ordering::Relaxed);
            seen.lock().unwrap().push(path);
            Box::pin(std::future::ready(Err(anyhow::anyhow!("recorded"))))
        },
    );
    let dir = crate::shard_directory::ShardDirectory::new_at(
        topology.shards,
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: Duration::from_secs(60),
            open_wait: Duration::from_millis(500),
        },
        |_notifier| opener,
    );
    let outcome = dir
        .gate_for_tests()
        .get_or_open("10", Duration::from_secs(5))
        .await;
    let OpenOutcome::Failed(error) = outcome else {
        panic!("the fenced open of a prefix that is no leaf fails");
    };
    assert!(error.contains("not_a_leaf: 10"), "{error}");
    assert_eq!(
        calls.load(Ordering::Relaxed),
        0,
        "{:?}",
        handed.lock().unwrap()
    );
}

/// The `'T'` row's key is the sentinel, `FF` × 16, then `'T'`, at pinned bytes: a split's or
/// merge's actor writes it and every later open reads it.
#[test]
fn the_retired_row_is_keyed_at_pinned_bytes() {
    assert_eq!(
        crate::crypto::hex(&super::retired_key()),
        "ffffffffffffffffffffffffffffffff54"
    );
}

/// **A read of a log closed for any reason but a newer writer is no fence.** One read of the fence
/// on an engine whose log was closed fails as a store error, not as `Fenced`: only a newer
/// writer's fence makes the held open close and reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_read_of_a_closed_log_is_a_store_error_not_a_fence() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = crate::keyspace::db_builder("shards/0", store.clone())
        .build()
        .await
        .unwrap();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .unwrap();
    let engine = crate::shard::ShardEngine::start(
        "0".to_string(),
        Arc::new(db),
        store,
        crate::shard::ShardConfig::default(),
        None,
        maintenance,
    );
    engine.db.close().await.unwrap();
    let topology = crate::config::validation::InitialShards::new(2)
        .unwrap()
        .topology(1 << 20);
    let snapshot = crate::topology::model::Snapshot {
        shards: topology.shards,
        etag: None,
        installed_at: None,
    };
    match super::read(&engine, &snapshot).await {
        Err(super::ReadFailure::Other(error)) => assert!(error.starts_with("'T': "), "{error}"),
        Err(super::ReadFailure::Fenced) => panic!("a closed log read as a fence"),
        Ok(_) => panic!("a closed log read"),
    }
}

/// A held open's whole bound, its reopens after a fence included, is twice the fence's wait.
#[test]
fn a_held_open_is_bounded_by_twice_the_fence_wait() {
    assert_eq!(super::HELD_OPEN_BOUND, super::FENCE_WAIT * 2);
}
