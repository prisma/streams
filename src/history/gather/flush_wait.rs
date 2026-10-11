//! A gather's flush that keeps its history partition's view of the
//! manifest current while it waits for an L0 slot.
//!
//! A history Db refuses to flush while its L0, as it last saw the
//! manifest, is at the cap (64), and it learns that compaction freed a
//! slot only when it writes its own manifest, which no flush can do at
//! the cap, or at its manifest poll (300 s, `history_settings`). So a
//! gather whose flush outlasts one absorber tick refreshes the manifest,
//! the poll's own path, and keeps refreshing until the flush returns: it
//! resumes within one refresh of compaction freeing a slot instead of at
//! the next poll. The flush's own result decides the gather; a refresh
//! that fails is logged and changes nothing.
use super::millis;
use crate::history::HISTORY_FLUSH_WAIT_MS_MAX;
use slatedb::Db;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// One absorber tick (`AbsorberConfig::default().tick`): a flush that
/// outlasts the tick that scheduled it is waiting for an L0 slot.
pub(super) const REFRESH_AFTER: Duration = Duration::from_secs(5);

/// `db.flush()`, refreshing the manifest every `refresh_after` while the
/// flush has not returned. Each refresh first raises
/// `HISTORY_FLUSH_WAIT_MS_MAX` to the wait so far, so a flush stalled at
/// the cap shows in the peak-since-scrape gauge while it waits, not only
/// once it ends.
pub(super) async fn flush_refreshing(
    db: &Db,
    refresh_after: Duration,
) -> Result<(), slatedb::Error> {
    let started = Instant::now();
    let mut flush = Box::pin(db.flush());
    loop {
        match tokio::time::timeout(refresh_after, &mut flush).await {
            Ok(flushed) => return flushed,
            Err(_still_waiting) => {
                HISTORY_FLUSH_WAIT_MS_MAX.fetch_max(millis(started.elapsed()), Ordering::Relaxed);
                if let Err(error) = db.refresh_manifest().await {
                    tracing::warn!(%error, "history manifest refresh while a flush waits failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::flush_refreshing;
    use crate::history::HISTORY_FLUSH_WAIT_MS_MAX;
    use slatedb::Db;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    /// A history partition's production settings with an L0 cap of 4 and a
    /// compactor that polls every 20 ms, over `store`, its L0 at the cap and
    /// a row in its memtable; `compactor` false runs no compactor, so no
    /// slot ever frees.
    async fn capped_partition(store: Arc<dyn object_store::ObjectStore>, compactor: bool) -> Db {
        let shard = crate::shard::ShardConfig::default();
        let worker = slatedb::config::CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(20),
            ..shard.compactor_options.worker.clone().unwrap_or_default()
        };
        let options = slatedb::config::CompactorOptions {
            poll_interval: Duration::from_millis(20),
            worker: Some(worker),
            ..shard.compactor_options.clone()
        };
        let settings = slatedb::config::Settings {
            l0_max_ssts: 4,
            l0_max_ssts_per_key: 4,
            compactor_options: compactor.then_some(options.clone()),
            ..crate::history::history2_settings(&shard.history, &options)
        };
        let db = Db::builder("capped", store)
            .with_settings(settings)
            .build()
            .await
            .unwrap();
        for n in 0..4u8 {
            db.put([n], b"row").await.unwrap();
            db.flush().await.unwrap();
        }
        assert_eq!(
            db.manifest().l0().len(),
            4,
            "the partition's L0 is at its cap"
        );
        db.put(b"waiting", b"row").await.unwrap();
        db
    }

    /// The absorber's stall at the history L0 cap: the compactor has merged
    /// the four L0s in the stored manifest, but the Db still sees four and
    /// would learn otherwise only at its 300 s poll, so a plain flush waits.
    /// A flush that refreshes after 200 ms lands within 3 s (the 5 s
    /// `REFRESH_AFTER` would not) and the Db's view has room again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_flush_waiting_at_the_l0_cap_resumes_at_the_refresh_not_the_poll() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = capped_partition(store.clone(), true).await;
        let admin = slatedb::admin::AdminBuilder::new("capped", store).build();
        let stored_l0 = async || admin.read_manifest(None).await.unwrap().unwrap().l0().len();
        let mut looks = 0;
        while stored_l0().await >= 4 && looks < 500 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            looks += 1;
        }
        assert!(
            stored_l0().await < 4,
            "the compactor never merged the four L0s"
        );
        assert_eq!(db.manifest().l0().len(), 4, "the Db has not seen the merge");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), db.flush())
                .await
                .is_err(),
            "a plain flush waits for the manifest poll"
        );
        let flushed = tokio::time::timeout(
            Duration::from_secs(3),
            flush_refreshing(&db, Duration::from_millis(200)),
        )
        .await;
        assert_eq!(flushed.map(|result| result.is_ok()), Ok(true));
        assert!(db.manifest().l0().len() < 4, "the refresh freed the slot");
        assert_eq!(
            db.get(b"waiting").await.unwrap().as_deref(),
            Some(&b"row"[..])
        );
        db.close().await.unwrap();
    }

    /// While a flush waits at the cap (no compactor, so it waits for good),
    /// `history_flush_wait_ms_max` rises with the wait at every refresh.
    /// The gauge is process-wide and an ops snapshot swaps it to 0, so the
    /// test keeps the highest value it sees across 20 looks of 25 ms each.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_waiting_flush_reports_its_wait_before_it_ends() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = capped_partition(store, false).await;
        let mut waiting = Box::pin(flush_refreshing(&db, Duration::from_millis(20)));
        let mut seen = 0;
        for _ in 0..20 {
            let looked = tokio::time::timeout(Duration::from_millis(25), &mut waiting).await;
            assert!(
                looked.is_err(),
                "no compactor frees a slot: the flush waits"
            );
            seen = seen.max(HISTORY_FLUSH_WAIT_MS_MAX.load(Ordering::Relaxed));
        }
        assert!(
            seen >= 250,
            "the wait reported while the flush waits: {seen} ms"
        );
        drop(waiting);
        let unflushed = slatedb::config::CloseOptions::default().with_flush_memtables(false);
        db.close_with_options(unflushed).await.unwrap();
    }
}
