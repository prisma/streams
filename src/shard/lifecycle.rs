//! Required engine workers share the runtime supervisor's join authority.
use super::{ShardEngine, history_partition::HistoryPartition};
use crate::tasks::{Policy, TaskOutcome, TaskResult, TaskSupervisor};
use std::{
    future::Future,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

const WORKER_GRACE: Duration = Duration::from_secs(5);
#[derive(Default)]
pub(super) struct EngineTasks {
    supervisor: TaskSupervisor,
    failure: Mutex<Option<&'static str>>,
}
impl EngineTasks {
    #[expect(
        clippy::let_underscore_must_use,
        reason = "EngineTasks::required; the supervisor rejects a spawn only while it is stopping, when no required role is owed; a rejected role has nothing left to run"
    )]
    pub(super) fn required(
        &self,
        engine: &Arc<ShardEngine>,
        role: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) {
        let guard = RequiredExit {
            engine: Arc::downgrade(engine),
            role,
        };
        let _ = self
            .supervisor
            .spawn(role, Policy::Critical, |_| async move {
                let _guard = guard;
                future.await;
                TaskResult::Done
            });
    }
    #[expect(
        clippy::unwrap_used,
        reason = "EngineTasks::failure; a poisoned failure slot may hold a half-recorded role; recovering it could report a role that never failed or hide one that did"
    )]
    pub(super) fn failure(&self) -> Option<&'static str> {
        *self.failure.lock().unwrap()
    }
    #[expect(
        clippy::unwrap_used,
        reason = "EngineTasks::failed; a poisoned failure slot may hold a half-recorded role; recovering it could report a role that never failed or hide one that did"
    )]
    pub(super) fn failed(&self, role: &'static str) {
        self.failure.lock().unwrap().get_or_insert(role);
    }
    pub(super) fn begin_close(
        &self,
        db: Arc<slatedb::Db>,
        history: Arc<HistoryPartition>,
        prefix: String,
    ) {
        history.stop();
        self.supervisor
            .begin_shutdown_with(WORKER_GRACE, "storage-close", async move {
                // Close independently so one failure cannot orphan the other store.
                let shard = super::history_partition::close_db(&db).await;
                let history = history.close().await;
                match (shard, history) {
                    (Ok(()), Ok(())) => {
                        tracing::info!(shard = %prefix, "engine workers and stores terminated");
                        TaskResult::Done
                    }
                    (shard, history) => TaskResult::Failed(format!(
                        "{prefix}: shard={shard:?}, history={history:?}"
                    )),
                }
            });
    }
    pub(super) fn handle(&self) -> EngineShutdown {
        EngineShutdown(self.supervisor.clone())
    }
    #[cfg(test)]
    pub(super) async fn workers(&self, timeout: Duration) -> Result<(), String> {
        tokio::time::timeout(timeout, self.supervisor.workers_terminated())
            .await
            .map(|_| ())
            .map_err(|_| "engine workers still running; join authority retained".into())
    }
    #[cfg(test)]
    pub(super) fn task_handle_for_test(&self, role: &str) -> tokio::task::AbortHandle {
        self.supervisor.task_handle_for_test(role)
    }
    #[cfg(test)]
    pub(super) fn abort(&self, role: &str) -> tokio::task::AbortHandle {
        self.supervisor.abort_named(role)
    }
}
/// Retains join authority and its report, without retaining engine caches.
#[derive(Clone)]
pub(crate) struct EngineShutdown(TaskSupervisor);
impl EngineShutdown {
    pub(crate) fn failure(&self) -> Option<String> {
        self.0.incomplete_shutdown_failure()
    }
    pub(crate) fn terminated(&self) -> bool {
        self.0.monitor().phase() == Some(crate::tasks::Phase::Stopped)
    }
    pub(crate) async fn wait(&self, timeout: Duration) -> Result<(), String> {
        let report = tokio::time::timeout(timeout, self.0.observe_shutdown())
            .await
            .map_err(|_| "engine shutdown still running; join authority retained".to_string())?;
        let failures: Vec<_> = report
            .outcomes
            .iter()
            .filter(|(_, outcome)| !matches!(outcome, TaskOutcome::Finished))
            .map(|(role, outcome)| format!("{role}: {outcome:?}"))
            .collect();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}
struct RequiredExit {
    engine: Weak<ShardEngine>,
    role: &'static str,
}
impl Drop for RequiredExit {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.upgrade()
            && !engine.is_closed()
        {
            engine.tasks.failed(self.role);
            engine.begin_close();
        }
    }
}
