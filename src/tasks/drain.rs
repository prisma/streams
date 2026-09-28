//! A planned drain: the phase a requested stop runs before it cancels
//! anything (item 40, the owner's decision). The runtime that has ownership
//! to hand off registers it (`fleet::start_configured`); a stop that a
//! critical exit requested, or one on a runtime without a drain, skips it.
//! While the drain runs every loop keeps running (the heartbeat, the fleet
//! tick, the server that hands requests on), so the runtime keeps its
//! liveness and its fencing until its ownership has moved.
use super::{
    Inner, Phase, Policy, ShutdownRequest, TaskMonitor, TaskResult, TaskState, TaskSupervisor,
    exits,
};
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// What a draining runtime's readiness answers.
pub(crate) const DRAINING: &str = "runtime draining";

/// A registered drain: what it runs, and its bound.
pub(super) struct StopPreface {
    budget: Duration,
    run: Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>,
}

impl TaskSupervisor {
    /// Registers the drain a requested stop runs first, bounded by
    /// `budget`. The first registration stands; returns whether this one
    /// did.
    pub(crate) fn set_stop_preface(
        &self,
        budget: Duration,
        run: impl FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    ) -> bool {
        let Ok(mut preface) = self.inner.preface.lock() else {
            return false;
        };
        if preface.is_some() {
            return false;
        }
        *preface = Some(StopPreface {
            budget,
            run: Box::new(run),
        });
        true
    }
}

impl TaskMonitor {
    /// Why this runtime does not serve, as its readiness answers it. While a
    /// planned drain runs, the runtime is draining: the critical loops that
    /// ended as its consequence (the signal loop that asked for it) are not
    /// failures, but any other critical loop that ended still is, and once
    /// the stop has begun the supervisor's own answer holds again.
    pub(crate) fn readiness_reason(&self) -> Option<String> {
        let Some(inner) = self.inner.upgrade() else {
            return self.unready_reason();
        };
        if !inner.draining() || inner.phase() != Phase::Running {
            return self.unready_reason();
        }
        let ended = inner
            .drain_ended
            .lock()
            .map(|ended| ended.clone())
            .unwrap_or_default();
        let failed = inner.snapshot().into_iter().find(|task| {
            task.policy == Policy::Critical
                && task.state == TaskState::Exited
                && !ended.contains(&task.name)
        });
        Some(failed.map_or_else(
            || DRAINING.into(),
            |task| format!("critical task terminated: {}", task.name),
        ))
    }
}

impl Inner {
    /// Whether a planned drain has begun.
    pub(super) fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// The critical loop `label` ended cooperatively during the drain.
    pub(super) fn ended_by_drain(&self, label: &'static str) {
        if let Ok(mut ended) = self.drain_ended.lock() {
            ended.push(label);
        }
    }

    /// Begins the registered drain, once: arms a process root's stop bound,
    /// extended by the drain's budget, then runs the drain as a supervised
    /// loop whose own end requests the stop, even if the drain panics.
    /// Returns whether the stop now follows a drain. A request while the
    /// drain runs does not wait for it (the stop follows at once), and a
    /// registry a panic poisoned runs no drain.
    pub(super) fn begin_drain(self: &Arc<Self>) -> bool {
        let taken = match self.preface.lock() {
            Ok(mut preface) => {
                let taken = preface.take();
                if taken.is_some() {
                    self.draining.store(true, Ordering::SeqCst);
                }
                taken
            }
            Err(_) => None,
        };
        let Some(StopPreface { budget, run }) = taken else {
            return false;
        };
        exits::arm_root_deadline(self, budget);
        let stop = ShutdownRequest {
            inner: Arc::downgrade(self),
        };
        let supervisor = TaskSupervisor {
            inner: Arc::clone(self),
        };
        supervisor
            .spawn("drain", Policy::Noncritical, move |cancel| async move {
                let drained = std::panic::AssertUnwindSafe(tokio::time::timeout(budget, run()))
                    .catch_unwind();
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    ended = drained => {
                        if ended.is_err() {
                            tracing::error!("the planned drain panicked; stopping without it");
                        }
                    }
                }
                stop.stop_now();
                TaskResult::Done
            })
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::tasks::{Cancellation, Policy, TaskOutcome, TaskResult, TaskSupervisor};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A loop that runs until the stop is requested.
    fn polite(supervisor: &TaskSupervisor, name: &'static str) {
        supervisor
            .spawn(name, Policy::Critical, |cancel| async move {
                cancel.cancelled().await;
                TaskResult::Done
            })
            .unwrap();
    }

    async fn stop_requested(supervisor: &TaskSupervisor, why: &str) {
        tokio::time::timeout(
            Duration::from_secs(5),
            supervisor.cancellation().cancelled(),
        )
        .await
        .expect(why);
    }

    /// A drain that reports whether the stop was already requested while it
    /// ran.
    async fn report_cancellation(
        cancellation: Cancellation,
        drained: tokio::sync::oneshot::Sender<bool>,
    ) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        drained.send(cancellation.is_cancelled()).unwrap();
    }

    #[tokio::test]
    async fn a_requested_stop_runs_the_registered_drain_before_it_cancels() {
        let supervisor = TaskSupervisor::new();
        polite(&supervisor, "http");
        let (drained, finished) = tokio::sync::oneshot::channel();
        let drain = report_cancellation(supervisor.cancellation(), drained);
        let registered =
            supervisor.set_stop_preface(Duration::from_secs(5), move || Box::pin(drain));
        assert!(registered);
        supervisor.shutdown_request().request();
        assert!(
            !supervisor.cancellation().is_cancelled(),
            "the stop waits for the drain"
        );
        let drained = tokio::time::timeout(Duration::from_secs(5), finished)
            .await
            .expect("a requested stop runs the registered drain");
        assert_eq!(drained, Ok(false), "every loop runs while the drain does");
        stop_requested(&supervisor, "the drain's end requests the stop").await;
        let report = supervisor.shutdown(Duration::from_secs(1)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        assert!(report.outcomes.contains(&("drain", TaskOutcome::Finished)));
        assert!(report.outcomes.contains(&("http", TaskOutcome::Finished)));
    }

    #[tokio::test]
    async fn a_drain_that_outlives_its_budget_is_cut_and_the_stop_follows() {
        let supervisor = TaskSupervisor::new();
        polite(&supervisor, "http");
        let registered = supervisor.set_stop_preface(Duration::from_millis(100), || {
            Box::pin(std::future::pending())
        });
        assert!(registered);
        let requested = tokio::time::Instant::now();
        supervisor.shutdown_request().request();
        stop_requested(&supervisor, "the budget ends the drain").await;
        assert!(requested.elapsed() >= Duration::from_millis(100));
        assert!(
            supervisor
                .shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
    }

    #[tokio::test]
    async fn the_first_drain_registered_is_the_one_that_runs_and_it_runs_once() {
        let supervisor = TaskSupervisor::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&runs);
        let first = supervisor.set_stop_preface(Duration::from_secs(1), move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(()))
        });
        let second = supervisor
            .set_stop_preface(Duration::from_secs(1), || Box::pin(std::future::pending()));
        assert!(first && !second);
        supervisor.shutdown_request().request();
        supervisor.shutdown_request().request();
        stop_requested(&supervisor, "the drain's end requests the stop").await;
        assert!(
            supervisor
                .shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_second_request_during_the_drain_stops_at_once() {
        let supervisor = TaskSupervisor::new();
        polite(&supervisor, "http");
        let registered = supervisor
            .set_stop_preface(Duration::from_secs(60), || Box::pin(std::future::pending()));
        assert!(registered);
        supervisor.shutdown_request().request();
        assert!(!supervisor.cancellation().is_cancelled());
        supervisor.shutdown_request().request();
        assert!(
            supervisor.cancellation().is_cancelled(),
            "a second request does not wait for the drain"
        );
        assert!(
            supervisor
                .shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_stop_with_no_drain_registered_cancels_at_once() {
        let supervisor = TaskSupervisor::new();
        supervisor.shutdown_request().request();
        assert!(supervisor.cancellation().is_cancelled());
    }

    /// The signal loop's shape (`bootstrap::run`): it requests the stop and
    /// ends.
    fn signal_loop(root: &TaskSupervisor) {
        let request = root.shutdown_request();
        root.spawn("signal", Policy::Critical, move |_| async move {
            request.request();
            TaskResult::Done
        })
        .unwrap();
    }

    /// A draining runtime's readiness says it drains. Its signal loop has
    /// ended (it asked for the drain), and `unready_reason` alone would name
    /// that loop as a terminated critical task.
    #[tokio::test]
    async fn a_draining_runtime_answers_readiness_as_draining() {
        let supervisor = TaskSupervisor::new();
        polite(&supervisor, "http");
        let monitor = supervisor.monitor();
        assert_eq!(monitor.readiness_reason(), None);
        let registered = supervisor
            .set_stop_preface(Duration::from_secs(60), || Box::pin(std::future::pending()));
        assert!(registered);
        signal_loop(&supervisor);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while monitor.unready_reason().is_none() && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            monitor.unready_reason().as_deref(),
            Some("critical task terminated: signal")
        );
        assert_eq!(monitor.readiness_reason().as_deref(), Some(super::DRAINING));
        assert!(
            supervisor
                .shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
    }

    /// A failing critical loop during a drain is still a failure on the
    /// readiness answer, and once the stop begins the supervisor's own
    /// answer holds again.
    #[tokio::test]
    async fn a_drain_does_not_mask_a_failure_or_the_stop() {
        let supervisor = TaskSupervisor::new();
        let monitor = supervisor.monitor();
        let registered = supervisor
            .set_stop_preface(Duration::from_secs(60), || Box::pin(std::future::pending()));
        assert!(registered);
        signal_loop(&supervisor);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while monitor.readiness_reason().is_none() && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(monitor.readiness_reason().as_deref(), Some(super::DRAINING));
        let failing = supervisor.spawn("fleet", Policy::Critical, |_| async {
            TaskResult::Failed("repository gone".into())
        });
        assert!(failing.is_ok());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while monitor.readiness_reason().as_deref() == Some(super::DRAINING)
            && tokio::time::Instant::now() < deadline
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            monitor.readiness_reason().as_deref(),
            Some("critical task terminated: fleet")
        );
        supervisor.shutdown_request().request();
        assert_eq!(
            monitor.readiness_reason().as_deref(),
            Some("runtime shutting down"),
            "a second request stops at once, and the stop outranks the drain"
        );
        assert!(
            supervisor
                .shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
    }

    /// A drain that panics still ends in the stop it was run for.
    #[tokio::test]
    async fn a_panicking_drain_still_stops_the_runtime() {
        let supervisor = TaskSupervisor::new();
        polite(&supervisor, "http");
        let drain = async {
            panic!("drain panicked");
        };
        let registered =
            supervisor.set_stop_preface(Duration::from_secs(60), move || Box::pin(drain));
        assert!(registered);
        supervisor.shutdown_request().request();
        stop_requested(&supervisor, "a panicking drain still requests the stop").await;
        assert!(
            supervisor
                .shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
    }

    /// On the process root: the drain's budget extends the stop's bound; the
    /// signal loop's own end (it requested the drain) does not stop the
    /// runtime, but a loop that fails during the drain does, at once.
    #[tokio::test]
    async fn on_a_process_root_only_a_failure_cuts_a_drain_short() {
        let root = TaskSupervisor::new().bounded_root(Duration::from_secs(86_400));
        let (_release, held) = tokio::sync::oneshot::channel::<()>();
        let drain = async move { held.await.unwrap_or_default() };
        let registered = root.set_stop_preface(Duration::from_secs(60), move || Box::pin(drain));
        assert!(registered);
        signal_loop(&root);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !root.inner.draining() && tokio::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(root.inner.armed.get(), Some(&Duration::from_secs(86_460)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !root.cancellation().is_cancelled(),
            "the signal loop's own end is the drain's consequence"
        );
        assert_eq!(root.stop_cause(), None);
        let failing = root.spawn("fleet", Policy::Critical, |_| async {
            TaskResult::Failed("repository gone".into())
        });
        assert!(failing.is_ok());
        stop_requested(
            &root,
            "a failure during the drain stops the runtime at once",
        )
        .await;
        assert_eq!(root.stop_cause().map(|exit| exit.name), Some("fleet"));
        assert!(
            root.shutdown(Duration::from_secs(1))
                .await
                .aborted
                .is_empty()
        );
    }
}
