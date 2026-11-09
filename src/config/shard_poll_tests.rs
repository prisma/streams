#![cfg(test)]
//! Q5's condition (SCALING-DECISIONS): fencing is not on the manifest poll. A shard log's
//! owner learns it was fenced at its next write's CAS (ASM-SLATEDB-FENCE, TLA-011), so a
//! 30 s poll changes no correctness: the old owner's next write fails at once.

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use object_store::memory::InMemory;

use super::CliArgs;
use super::validation::shard_settings;

/// The shard logs' settings at a 30 s manifest poll.
fn polled_every_30_s() -> slatedb::config::Settings {
    let mut args = CliArgs::deterministic();
    args.manifest_poll_ms = 30_000;
    let settings = shard_settings(&args, &super::EngineConfig::default());
    assert_eq!(settings.manifest_poll_interval, Duration::from_secs(30));
    settings
}

/// Owner A writes; owner B opens the same log, which fences A; A's next write fails within a
/// second, never after the 30 s poll, and B writes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_fenced_shard_logs_next_write_fails_at_once_under_a_30_s_poll() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let settings = polled_every_30_s();
    let open = || {
        crate::keyspace::db_builder("0/shard", store.clone())
            .with_settings(settings.clone())
            .build()
    };
    let a = open().await.expect("owner A opens");
    a.put(b"k", b"a").await.expect("owner A writes");
    a.flush().await.expect("owner A flushes");
    let b = open().await.expect("owner B opens, fencing A");
    let started = Instant::now();
    let fenced = tokio::time::timeout(Duration::from_secs(5), async {
        a.put(b"k", b"a2").await?;
        a.flush().await
    })
    .await;
    let waited = started.elapsed();
    assert!(
        matches!(fenced, Ok(Err(_))),
        "the fenced owner's write fails: {fenced:?}"
    );
    assert!(
        waited < Duration::from_secs(1),
        "at once, not at the poll: {waited:?}"
    );
    b.put(b"k", b"b").await.expect("owner B writes");
    b.flush().await.expect("owner B flushes");
    let read = b.get(b"k").await.expect("owner B reads");
    assert_eq!(read.as_deref(), Some(&b"b"[..]));
}
