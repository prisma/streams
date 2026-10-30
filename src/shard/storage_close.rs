//! The engine's storage close: the shard log closes with its final flush,
//! and what a returned close means.
use slatedb::Db;

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
#[cfg(test)]
mod tests {
    use super::{close_db, close_verdict};
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
    async fn a_failed_final_flush_on_a_healthy_db_leaves_the_reason_clean_and_the_close_failed() {
        let failpoints = Arc::new(fail_parallel::FailPointRegistry::new());
        let db = db("close-verdict-healthy", failpoints.clone()).await;
        db.put(b"k", b"v").await.unwrap();
        fail_parallel::cfg(failpoints, "write-wal-sst-io-error", "return").unwrap();
        assert_eq!(close_db(&db).await, Err(WAL_REFUSED.to_string()));
        assert_eq!(db.status().close_reason, Some(CloseReason::Clean));
    }

    /// A clean close records Clean, and a second close of that Db is a close.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2, crate = "crate::test_rt")]
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
}
