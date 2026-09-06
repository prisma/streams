//! Keep ownership of a database open when its awaiting controller is cancelled.

/// Run a database open on the SlateDB runtime so its background tasks use
/// those threads. The worker owns the result until successful handoff. An
/// abandoned open completes under that owner, which closes any resulting DB;
/// cancelling a caller never leaves a late writer running without an owner.
/// OpenGate additionally owns its pending opener across request cancellation.
pub async fn on_slatedb_rt<F>(fut: F) -> Result<slatedb::Db, slatedb::Error>
where
    F: std::future::Future<Output = Result<slatedb::Db, slatedb::Error>> + Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    super::slatedb_runtime().spawn(async move {
        let _ = tx.send(fut.await.map(DbHandoff::new));
    });
    rx.await
        .expect("slatedb-rt task dropped")
        .map(DbHandoff::claim)
}

/// The channel can accept a value just before its receiver is cancelled.
/// Ownership therefore travels with the queued value until the caller claims
/// it, rather than relying only on detecting a failed send.
struct DbHandoff(Option<slatedb::Db>);

impl DbHandoff {
    fn new(db: slatedb::Db) -> Self {
        Self(Some(db))
    }
    fn claim(mut self) -> slatedb::Db {
        self.0.take().expect("database handoff is claimed once")
    }
}

impl Drop for DbHandoff {
    fn drop(&mut self) {
        if let Some(db) = self.0.take() {
            super::slatedb_runtime().spawn(async move {
                if let Err(error) = db.close().await {
                    tracing::warn!(%error, "closing abandoned database open failed");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    async fn expect_closed(observer: &slatedb::Db) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while observer.get(b"probe").await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the abandoned handoff closes its completed database");
    }

    #[tokio::test]
    async fn abandoned_open_closes_its_late_database_and_live_handoff_stays_open() {
        let db = on_slatedb_rt(async {
            slatedb::Db::builder(
                "handoff-cancel",
                Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
        })
        .await
        .unwrap();
        let observer = db.clone();
        assert!(observer.get(b"probe").await.unwrap().is_none());
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let caller = tokio::spawn(on_slatedb_rt(async move {
            entered.send(()).unwrap();
            release_rx.await.unwrap();
            Ok(db)
        }));
        entered_rx.await.unwrap();
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        assert!(observer.get(b"probe").await.unwrap().is_none());
        release.send(()).unwrap();
        expect_closed(&observer).await;
    }

    #[tokio::test]
    async fn abandoning_an_already_queued_database_result_also_closes_it() {
        let db = on_slatedb_rt(async {
            slatedb::Db::builder(
                "handoff-queued",
                Arc::new(object_store::memory::InMemory::new()),
            )
            .build()
            .await
        })
        .await
        .unwrap();
        let observer = db.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        assert!(tx.send(DbHandoff::new(db)).is_ok());
        assert!(observer.get(b"probe").await.unwrap().is_none());
        drop(rx);
        expect_closed(&observer).await;
    }
}
