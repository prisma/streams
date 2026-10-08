//! One history open belongs to the engine, not to the request awaiting it.
//! Shutdown joins a late open and closes its result before reporting completion.
use slatedb::Db;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

type OpenResult = Result<Arc<Db>, Arc<slatedb::Error>>;
struct Opening {
    result: watch::Receiver<Option<OpenResult>>,
    task: tokio::task::JoinHandle<()>,
}
#[derive(Default)]
struct State {
    stopping: bool,
    db: Option<Arc<Db>>,
    opening: Option<Opening>,
}
#[derive(Default)]
pub(super) struct HistoryPartition {
    state: Mutex<State>,
}
impl HistoryPartition {
    #[expect(
        clippy::unwrap_used,
        reason = "HistoryPartition::get; a poisoned partition state may hold a half-opened database or a stale stopping flag; recovering it could hand out a database that never finished opening or reopen one that is stopping"
    )]
    pub(super) fn get(&self) -> Option<Arc<Db>> {
        self.state.lock().unwrap().db.clone()
    }
    #[expect(
        clippy::unwrap_used,
        reason = "HistoryPartition::stop; a poisoned partition state may hold a half-opened database or a stale stopping flag; recovering it could hand out a database that never finished opening or reopen one that is stopping"
    )]
    pub(super) fn stop(&self) {
        self.state.lock().unwrap().stopping = true;
    }

    #[expect(
        clippy::unwrap_used,
        reason = "HistoryPartition::open; a poisoned partition state may hold a half-opened database or a stale stopping flag; recovering it could hand out a database that never finished opening or reopen one that is stopping"
    )]
    #[expect(
        clippy::expect_used,
        reason = "HistoryPartition::open; a finished open task is joinable now; a fallible join would add a branch no finished task reaches"
    )]
    #[expect(
        clippy::disallowed_methods,
        reason = "HistoryPartition::open; the opener outlives the request that started it and is joined through the partition's own state, so no request-scoped supervisor may own it; a supervised opener would tie the partition to one caller's lifetime"
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "HistoryPartition::open; the open nests the panic-caught build inside the spawned opener inside the state guard; flattening it would separate the result from the task that publishes it"
    )]
    #[expect(
        clippy::let_underscore_must_use,
        reason = "HistoryPartition::open; the result watch fails to send only when every waiter is gone; a handled result would only restate that nobody waits"
    )]
    pub(super) async fn open<F, Fut>(self: &Arc<Self>, build: F) -> Result<Arc<Db>, slatedb::Error>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Db, slatedb::Error>> + Send + 'static,
    {
        use futures_util::FutureExt;
        let mut result = {
            let mut state = self.state.lock().unwrap();
            if state.stopping {
                return Err(stopping());
            }
            if let Some(db) = &state.db {
                return Ok(db.clone());
            }
            // Failed attempts may retry, but never accumulate completed handles.
            // is_finished permits an immediate join; it is not itself success.
            if state
                .opening
                .as_ref()
                .is_some_and(|open| open.task.is_finished())
            {
                let old = state.opening.take().unwrap();
                old.task
                    .now_or_never()
                    .expect("finished open is joinable")
                    .map_err(|e| slatedb::Error::internal(format!("history opener: {e}")))?;
            }
            if state.opening.is_none() {
                let (tx, rx) = watch::channel(None);
                let owner = self.clone();
                let task = tokio::spawn(async move {
                    let result = match std::panic::AssertUnwindSafe(async move { build().await })
                        .catch_unwind()
                        .await
                    {
                        Ok(result) => result.map(Arc::new).map_err(Arc::new),
                        Err(_) => Err(Arc::new(slatedb::Error::internal(
                            "history opener panicked".into(),
                        ))),
                    };
                    if let Ok(db) = &result {
                        owner.state.lock().unwrap().db = Some(db.clone());
                    }
                    let _ = tx.send(Some(result));
                });
                state.opening = Some(Opening { result: rx, task });
            }
            state.opening.as_ref().unwrap().result.clone()
        };
        loop {
            if let Some(result) = result.borrow().clone() {
                if self.state.lock().unwrap().stopping {
                    return Err(stopping());
                }
                return result.map_err(copy_error);
            }
            result.changed().await.map_err(|_| {
                slatedb::Error::internal("history opener ended without a result".into())
            })?;
        }
    }

    /// Only the engine's retained shutdown driver calls this. In particular,
    /// no request cancellation can drop this join or an in-progress Db::close.
    #[expect(
        clippy::unwrap_used,
        reason = "HistoryPartition::close; a poisoned partition state may hold a half-opened database or a stale stopping flag; recovering it could hand out a database that never finished opening or reopen one that is stopping"
    )]
    pub(super) async fn close(&self) -> Result<(), String> {
        self.stop();
        let opening = self.state.lock().unwrap().opening.take();
        if let Some(opening) = opening {
            opening
                .task
                .await
                .map_err(|e| format!("history open join: {e}"))?;
        }
        self.close_opened().await
    }

    /// Closes the opened database without a final memtable flush. Nothing
    /// unflushed here was ever relied on: a gather writes, flushes, and only
    /// then submits its boundary, so a write whose flush never returned left
    /// its debt in the shard log for the next owner. A final flush could only
    /// wait: at the L0 cap for a slot the database learns of at its next
    /// manifest poll (300 s for a history database), or for good while
    /// compaction frees none, and the engine's close with it.
    async fn close_opened(&self) -> Result<(), String> {
        let Some(db) = self.get() else {
            return Ok(());
        };
        let unflushed = slatedb::config::CloseOptions::default().with_flush_memtables(false);
        let closed = db.close_with_options(unflushed).await;
        close_verdict(closed, db.status().close_reason)
    }
}

pub(super) async fn close_db(db: &Db) -> Result<(), String> {
    let closed = db.close().await;
    // Read only after the close returned: the pinned Db::close_with_options
    // has joined every Db task by then, so a failure one of them recorded has
    // also published its reason.
    close_verdict(closed, db.status().close_reason)
}
/// What a returned `Db::close` means for the engine's storage close. The
/// pinned `Db::close_with_options` awaits every shutdown/join before it
/// returns its saved final-flush result, so every arm judges a Db whose tasks
/// have ended; nothing is inferred from abandoning or retrying a pending close.
fn close_verdict(
    closed: Result<(), slatedb::Error>,
    reason: Option<slatedb::CloseReason>,
) -> Result<(), String> {
    match closed {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Clean) => Ok(()),
        // A new owner may fence the final flush; the awaited close still completed.
        Err(e) if e.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) => Ok(()),
        // The Db had recorded its own failure before this close could mark it
        // closed: the first result wins and only the winner publishes its
        // reason, so a reason other than Clean is never the close's. Db::close
        // skips the flush and answers Ok when it reads that reason; it read
        // none only because the failing task had not published it yet, and its
        // flush failed with that task's error. Either way every Db task has
        // been joined, nothing of this Db can write again, and the next open
        // recovers whatever prefix landed.
        Err(_) if reason.is_some_and(|recorded| recorded != slatedb::CloseReason::Clean) => Ok(()),
        // The close itself marked the Db closed (Clean), or no reason is
        // recorded: the final flush of a healthy Db failed.
        Err(e) => Err(e.to_string()),
    }
}
fn stopping() -> slatedb::Error {
    slatedb::Error::closed(
        "engine history is closing".into(),
        slatedb::CloseReason::Clean,
    )
}
// Sharing one attempt must retain the original typed fencing/error category.
fn copy_error(error: Arc<slatedb::Error>) -> slatedb::Error {
    let message = error.to_string();
    let copy = match error.kind() {
        slatedb::ErrorKind::Transaction => slatedb::Error::transaction(message),
        slatedb::ErrorKind::Closed(reason) => slatedb::Error::closed(message, reason),
        slatedb::ErrorKind::Unavailable => slatedb::Error::unavailable(message),
        slatedb::ErrorKind::Invalid => slatedb::Error::invalid(message),
        slatedb::ErrorKind::Data => slatedb::Error::data(message),
        _ => slatedb::Error::internal(message),
    };
    copy.with_source(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::{HistoryPartition, close_db, close_verdict};
    use slatedb::{CloseReason, Db, Error};
    use std::{sync::Arc, time::Duration};

    const REFUSED: &str = "Unavailable error: io error (oops)";
    const WAL_REFUSED: &str = "Unavailable error: wal unavailable (io error)";

    fn refused() -> Error {
        Error::unavailable("io error".into()).with_source(Box::new(std::io::Error::other("oops")))
    }

    /// A Db on its own in-memory store whose WAL is flushed only on request:
    /// it has no flush timer, whose first tick could otherwise flush, and
    /// fail, behind the test's back.
    async fn db(path: &str, failpoints: Arc<fail_parallel::FailPointRegistry>) -> Db {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        Db::builder(path, store)
            .with_settings(slatedb::config::Settings {
                flush_interval: None,
                ..Default::default()
            })
            .with_fp_registry(failpoints)
            .build()
            .await
            .unwrap()
    }

    /// The race's answer: the close returns the failing task's error, and
    /// the reason the Db recorded is that task's, not the close's.
    #[test]
    fn a_close_refused_by_a_db_that_had_already_failed_is_a_close() {
        for reason in [CloseReason::Panic, CloseReason::Fenced] {
            assert_eq!(
                close_verdict(Err(refused()), Some(reason)),
                Ok(()),
                "{reason:?}"
            );
        }
    }

    /// The close marked the Db closed itself, or nothing is recorded: the
    /// final flush of a healthy Db failed, and the close stays failed.
    #[test]
    fn a_healthy_db_whose_close_fails_stays_failed() {
        for reason in [Some(CloseReason::Clean), None] {
            assert_eq!(
                close_verdict(Err(refused()), reason),
                Err(REFUSED.to_string()),
                "{reason:?}"
            );
        }
        let panicked = Error::closed("background task panicked".into(), CloseReason::Panic);
        assert_eq!(
            close_verdict(Err(panicked), Some(CloseReason::Clean)),
            Err("Closed error: background task panicked".to_string())
        );
    }

    #[test]
    fn a_completed_clean_or_fenced_close_is_a_close_whatever_the_reason() {
        for reason in [None, Some(CloseReason::Clean), Some(CloseReason::Panic)] {
            assert_eq!(close_verdict(Ok(()), reason), Ok(()), "{reason:?}");
        }
        for kind in [CloseReason::Clean, CloseReason::Fenced] {
            for reason in [None, Some(CloseReason::Clean)] {
                let error = Error::closed("closed".into(), kind);
                assert_eq!(
                    close_verdict(Err(error), reason),
                    Ok(()),
                    "{kind:?} {reason:?}"
                );
            }
        }
    }

    /// A Db whose WAL write failed closed itself with its own reason; its
    /// close skips the flush, answers Ok and keeps that reason.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_close_of_a_db_that_failed_on_its_own_answers_ok_and_keeps_its_reason() {
        let failpoints = Arc::new(fail_parallel::FailPointRegistry::new());
        let db = db("close-verdict-failed", failpoints.clone()).await;
        db.put(b"k", b"v").await.unwrap();
        fail_parallel::cfg(failpoints, "write-wal-sst-io-error", "return").unwrap();
        assert!(db.flush().await.is_err(), "the WAL write fails");
        let mut status = db.subscribe();
        tokio::time::timeout(
            Duration::from_secs(10),
            status.wait_for(|s| s.close_reason.is_some()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db.status().close_reason, Some(CloseReason::Panic));
        assert_eq!(close_db(&db).await, Ok(()));
        assert_eq!(db.status().close_reason, Some(CloseReason::Panic));
    }

    /// A healthy Db whose final flush fails: the close had marked it closed,
    /// so the reason is Clean and the storage close is failed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_final_flush_on_a_healthy_db_leaves_the_reason_clean_and_the_close_failed() {
        let failpoints = Arc::new(fail_parallel::FailPointRegistry::new());
        let db = db("close-verdict-healthy", failpoints.clone()).await;
        db.put(b"k", b"v").await.unwrap();
        fail_parallel::cfg(failpoints, "write-wal-sst-io-error", "return").unwrap();
        assert_eq!(close_db(&db).await, Err(WAL_REFUSED.to_string()));
        assert_eq!(db.status().close_reason, Some(CloseReason::Clean));
    }

    /// A clean close records Clean, and a second close of that Db is a close.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_clean_close_records_clean() {
        let db = db(
            "close-verdict-clean",
            Arc::new(fail_parallel::FailPointRegistry::new()),
        )
        .await;
        db.put(b"k", b"v").await.unwrap();
        assert_eq!(close_db(&db).await, Ok(()));
        assert_eq!(db.status().close_reason, Some(CloseReason::Clean));
        assert_eq!(close_db(&db).await, Ok(()));
    }

    /// The shard-close hang of 2026-10-08: a partition whose L0 is at its cap
    /// as the Db last saw it, with a write in its memtable, closes at once.
    /// A final flush would wait for a slot the Db learns of at its next
    /// manifest poll (300 s for a history database), or never while
    /// compaction frees none. The Db closes Clean; the flushed rows reopen,
    /// and the unflushed one, which no flush ever acknowledged, does not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_partition_at_its_l0_cap_closes_without_waiting_for_a_slot() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let shard = crate::shard::ShardConfig::default();
        let settings = slatedb::config::Settings {
            l0_max_ssts: 2,
            l0_max_ssts_per_key: 2,
            ..crate::history::history2_settings(&shard.history, &shard.compactor_options)
        };
        let partition = Arc::new(HistoryPartition::default());
        let (opened, at) = (settings.clone(), store.clone());
        let db = partition
            .open(move || Db::builder("capped", at).with_settings(opened).build())
            .await
            .unwrap();
        for key in [b"flushed-1", b"flushed-2"] {
            db.put(key, b"row").await.unwrap();
            db.flush().await.unwrap();
        }
        assert_eq!(
            db.manifest().l0().len(),
            2,
            "the partition's L0 is at its cap"
        );
        db.put(b"unflushed", b"row").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), db.flush())
                .await
                .is_err(),
            "a flush waits for an L0 slot"
        );
        let closed = tokio::time::timeout(Duration::from_secs(10), partition.close()).await;
        assert_eq!(closed, Ok(Ok(())), "the close does not wait for a slot");
        assert_eq!(db.status().close_reason, Some(CloseReason::Clean));
        let reopened = Db::builder("capped", store)
            .with_settings(settings)
            .build()
            .await
            .unwrap();
        for key in [&b"flushed-1"[..], b"flushed-2"] {
            assert_eq!(
                reopened.get(key).await.unwrap().as_deref(),
                Some(&b"row"[..])
            );
        }
        assert_eq!(reopened.get(b"unflushed").await.unwrap(), None);
        reopened.close().await.unwrap();
    }
}
