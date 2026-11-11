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
