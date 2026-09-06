//! One retained shutdown driver: join workers, then finish owned resources.
//! Callers observe watches; neither a timeout nor cancellation owns handles.
use super::{
    Inner, Phase, ShutdownReport, Supervised, TaskId, TaskOutcome, TaskResult, TaskSupervisor,
};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};
type Completion = tokio::sync::watch::Receiver<Option<Arc<ShutdownReport>>>;
type Finalizer = (
    &'static str,
    Pin<Box<dyn Future<Output = TaskResult> + Send>>,
);

impl TaskSupervisor {
    /// A terminal error without proof of resource termination requires the
    /// owner's replacement fence and readiness failure to remain in place.
    pub(crate) fn incomplete_shutdown_failure(&self) -> Option<String> {
        let state = self.inner.state.lock().unwrap();
        if state.phase != Phase::ShuttingDown {
            return None;
        }
        let report = state.report.as_ref()?;
        Some(format!("resource shutdown failed: {:?}", report.outcomes))
    }
    /// Starts shutdown synchronously, including when a retirement caller will
    /// not wait. Finalizers may contain durability-ambiguous close operations:
    /// they stay owned until completion rather than being aborted at grace.
    pub(crate) fn begin_shutdown_with(
        &self,
        grace: Duration,
        label: &'static str,
        finalizer: impl Future<Output = TaskResult> + Send + 'static,
    ) {
        self.launch_shutdown(grace, Some((label, Box::pin(finalizer))));
    }

    fn launch_shutdown(&self, grace: Duration, finalizer: Option<Finalizer>) -> Completion {
        let mut st = self.inner.state.lock().unwrap();
        if let Some(rx) = &st.completion {
            return rx.clone();
        }
        st.phase = Phase::ShuttingDown;
        let tasks = std::mem::take(&mut st.tasks);
        let (tx, rx) = tokio::sync::watch::channel(None);
        st.completion = Some(rx.clone());
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let mut report = drive_shutdown(&inner, tasks, grace).await;
            inner.state.lock().unwrap().workers = Some(Arc::new(report.clone()));
            inner.workers_done.notify_waiters();
            let mut resources_closed = true;
            if let Some((label, finalizer)) = finalizer {
                use futures_util::FutureExt;
                let outcome = match std::panic::AssertUnwindSafe(finalizer).catch_unwind().await {
                    Ok(TaskResult::Done) => TaskOutcome::Finished,
                    Ok(TaskResult::Failed(error)) => TaskOutcome::Failed(error),
                    Err(_) => TaskOutcome::Panicked("resource finalizer panicked".into()),
                };
                resources_closed = matches!(outcome, TaskOutcome::Finished);
                report.outcomes.push((label, outcome));
            }
            let report = Arc::new(report);
            {
                let mut st = inner.state.lock().unwrap();
                // A failed/panicked close cannot prove resource termination.
                // Publish the failure, but keep the owner's replacement fence.
                if resources_closed {
                    st.phase = Phase::Stopped;
                }
                st.report = Some(report.clone());
            }
            let _ = tx.send(Some(report));
        });
        let _ = self.inner.cancel_tx.send(true);
        self.inner.workers_done.notify_waiters();
        rx
    }

    /// Each worker is aborted if needed and joined; the shared result is
    /// published only after the resource finalizer has also completed.
    pub async fn shutdown(&self, grace: Duration) -> ShutdownReport {
        self.launch_shutdown(grace, None);
        self.observe_shutdown().await
    }

    /// Observe an owner-started shutdown without racing it to install a driver
    /// lacking the owner's resource finalizer.
    pub(crate) async fn observe_shutdown(&self) -> ShutdownReport {
        let mut rx = loop {
            let notified = self.inner.workers_done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(rx) = self.inner.state.lock().unwrap().completion.clone() {
                break rx;
            }
            notified.await;
        };
        loop {
            if let Some(report) = rx.borrow().clone() {
                return (*report).clone();
            }
            if rx.changed().await.is_err() {
                // A missing driver cannot manufacture a clean empty report.
                return ShutdownReport {
                    outcomes: vec![(
                        "shutdown-driver",
                        TaskOutcome::Failed("driver ended without a terminal report".into()),
                    )],
                    aborted: vec![],
                };
            }
        }
    }

    /// A narrower milestone for owners that must distinguish released worker
    /// reservations from a still-running database close. Does not start work.
    #[cfg(test)]
    pub(crate) async fn workers_terminated(&self) -> ShutdownReport {
        loop {
            let notified = self.inner.workers_done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(report) = self.inner.state.lock().unwrap().workers.clone() {
                return (*report).clone();
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn abort_named(&self, role: &str) -> tokio::task::AbortHandle {
        let state = self.inner.state.lock().unwrap();
        let handle = state
            .tasks
            .values()
            .find(|task| task.name == role)
            .expect("registered role")
            .handle
            .abort_handle();
        handle.abort();
        handle
    }
}
/// The one shutdown sequence, owned by the driver task.
async fn drive_shutdown(
    inner: &Arc<Inner>,
    tasks: BTreeMap<TaskId, Supervised>,
    grace: Duration,
) -> ShutdownReport {
    let _ = inner.cancel_tx.send(true);
    let deadline = tokio::time::Instant::now() + grace;
    let mut outcomes: BTreeMap<TaskId, (&'static str, TaskOutcome)> = BTreeMap::new();
    let mut survivors: Vec<(TaskId, Supervised)> = Vec::new();
    for (id, mut t) in tasks {
        match tokio::time::timeout_at(deadline, &mut t.handle).await {
            Ok(joined) => {
                outcomes.insert(id, (t.name, classify(joined)));
            }
            Err(_elapsed) => {
                t.handle.abort();
                survivors.push((id, t));
            }
        }
    }
    let mut aborted: Vec<(TaskId, &'static str)> = Vec::new();
    for (id, mut t) in survivors {
        aborted.push((id, t.name));
        // Abort only REQUESTS cancellation; joining proves the future
        // was dropped, its destructors ran and its resources are gone.
        let joined = (&mut t.handle).await;
        outcomes.insert(id, (t.name, classify(joined)));
    }
    aborted.sort_by_key(|(id, _)| *id);
    ShutdownReport {
        outcomes: outcomes.into_values().collect(),
        aborted: aborted.into_iter().map(|(_, name)| name).collect(),
    }
}

fn classify(joined: Result<TaskResult, tokio::task::JoinError>) -> TaskOutcome {
    match joined {
        Ok(TaskResult::Done) => TaskOutcome::Finished,
        Ok(TaskResult::Failed(e)) => TaskOutcome::Failed(e),
        Err(e) if e.is_panic() => {
            let p = e.into_panic();
            let msg = p
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| p.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".to_string());
            TaskOutcome::Panicked(msg)
        }
        Err(_) => TaskOutcome::Cancelled,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn r17a_early_observer_cannot_replace_the_resource_finalizer() {
        let supervisor = TaskSupervisor::new();
        let mut observer = Box::pin(supervisor.observe_shutdown());
        assert!(futures_util::poll!(observer.as_mut()).is_pending());
        assert_eq!(supervisor.phase(), Phase::Running);
        let (release, held) = tokio::sync::oneshot::channel();
        supervisor.begin_shutdown_with(Duration::ZERO, "owned-resource", async move {
            held.await.unwrap();
            TaskResult::Done
        });
        drop(observer);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), supervisor.observe_shutdown())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        let first = supervisor.observe_shutdown().await;
        assert_eq!(
            first.outcomes,
            vec![("owned-resource", TaskOutcome::Finished)]
        );
        assert_eq!(supervisor.shutdown(Duration::ZERO).await, first);
    }

    #[tokio::test]
    async fn r17a_failed_resource_finalizer_retains_its_fence_and_failure() {
        let supervisor = TaskSupervisor::new();
        supervisor.begin_shutdown_with(Duration::ZERO, "owned-resource", async {
            panic!("close failed after an ambiguous storage operation");
        });
        let first = supervisor.observe_shutdown().await;
        assert!(matches!(
            &first.outcomes[..],
            [("owned-resource", TaskOutcome::Panicked(_))]
        ));
        assert_eq!(supervisor.phase(), Phase::ShuttingDown);
        assert!(
            supervisor
                .incomplete_shutdown_failure()
                .unwrap()
                .contains("owned-resource")
        );
        assert_eq!(supervisor.observe_shutdown().await, first);
    }
}
