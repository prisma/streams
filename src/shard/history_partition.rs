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
        if let Some(db) = self.get() {
            close_db(&db).await?;
        }
        Ok(())
    }
}

pub(super) async fn close_db(db: &Db) -> Result<(), String> {
    match db.close().await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Clean) => Ok(()),
        // The pinned Db::close_with_options awaits every shutdown/join before
        // returning its saved final-flush result. A new owner may fence that
        // flush; the awaited close still completed. This is not inferred from
        // status(), nor from abandoning/retrying a pending close.
        Err(e) if e.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) => Ok(()),
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
