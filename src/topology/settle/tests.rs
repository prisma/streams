//! Preparing's settle wait on a log without a compactor, so its L0 stays as the test wrote it.

use super::settled_within;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;

/// The settle line these tests wait below.
const LINE: usize = 4;

/// A log over a fresh store without a compactor, holding `tables` L0 tables of one key each, then
/// `unflushed` more keys in its memtable.
async fn log_with(tables: usize, unflushed: usize) -> slatedb::Db {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let settings = slatedb::config::Settings {
        compactor_options: None,
        ..Default::default()
    };
    let db =
        crate::native_queue::compaction_guard::shard_log_builder("shards/0110100", store, settings)
            .build()
            .await
            .unwrap();
    let memtable = slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    };
    for n in 0..tables + unflushed {
        db.put(n.to_be_bytes(), b"v").await.unwrap();
        if n < tables {
            db.flush_with_options(memtable.clone()).await.unwrap();
        }
    }
    assert_eq!(db.manifest().l0().len(), tables, "the tables as written");
    db
}

/// A log below its line settles at once, its memtable flushed to one more L0 table first, so the
/// pause's checkpoint uploads only what arrives after Preparing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_log_below_its_line_settles_with_its_memtable_flushed() {
    let db = log_with(LINE - 2, 1).await;
    let l0 = || db.manifest().l0().len();
    let settled = settled_within(&db, l0, LINE, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(settled, LINE - 1, "the L0 it ended at");
}

/// A log at its line, with no compactor to bring it below, is unsettled once the bound passes,
/// and the refusal names the bound, the L0 length and the line.
#[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
async fn a_log_at_its_line_is_unsettled_past_the_bound() {
    let db = log_with(LINE, 0).await;
    let l0 = || db.manifest().l0().len();
    let started = std::time::Instant::now();
    let refused = settled_within(&db, l0, LINE, Duration::from_millis(200))
        .await
        .unwrap_err();
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "waited out the bound"
    );
    assert_eq!(
        refused,
        "the log did not settle within 200ms: 4 L0 tables, its line 4"
    );
}
