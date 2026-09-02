//! Task supervision (WP-02 / PR 6-F — the first slice of WP-15 §7-9):
//! every long-lived loop a runtime spawns is a child of ONE supervisor
//! that owns its join handle, a cooperative cancellation handle, its
//! result and its failure policy. Shutdown is ordered and bounded:
//! cancel, wait a grace period, abort what did not stop. Request-scoped
//! child tasks are NOT supervised here — they belong to their request.
//! A deterministic rig terminates a simulated process through this
//! owner, so a restart is literal, not two process objects over one
//! store with the old accept loop still alive.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;

/// What losing the loop means for the runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// The runtime is not healthy without it (fleet loop, telemetry
    /// drain, the watchdogs): an unexpected exit is a critical failure
    /// (surfaced to readiness by WP-15's remaining slice).
    Critical,
    /// Loss degrades observability or hygiene only.
    Noncritical,
}

/// A cooperative cancellation handle: a loop awaits `cancelled()` in
/// its `select!`; clones observe the same signal.
#[derive(Clone)]
pub struct Cancellation {
    rx: tokio::sync::watch::Receiver<bool>,
}

impl Cancellation {
    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        loop {
            if *rx.borrow() {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskState {
    Running,
    /// Exited on its own while the supervisor was NOT shutting down.
    /// For a critical loop that is the failure the policy exists for.
    Exited,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskStatus {
    pub name: &'static str,
    pub policy: Policy,
    pub state: TaskState,
}

/// The outcome of an ordered shutdown, by task name.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Stopped inside the grace period (cooperatively or by finishing).
    pub finished: Vec<&'static str>,
    /// Still running at the deadline: aborted at its next await point.
    pub aborted: Vec<&'static str>,
    /// Had panicked.
    pub panicked: Vec<&'static str>,
}

impl ShutdownReport {
    #[cfg(test)]
    pub fn terminated(&self, name: &str) -> bool {
        self.finished.contains(&name)
            || self.aborted.contains(&name)
            || self.panicked.contains(&name)
    }
}

struct Supervised {
    name: &'static str,
    policy: Policy,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
pub struct TaskSupervisor {
    inner: Arc<Inner>,
}

struct Inner {
    cancel_tx: tokio::sync::watch::Sender<bool>,
    cancel: Cancellation,
    tasks: Mutex<Vec<Supervised>>,
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskSupervisor {
    pub fn new() -> Self {
        let (cancel_tx, rx) = tokio::sync::watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                cancel_tx,
                cancel: Cancellation { rx },
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    /// The handle a loop selects on to stop cooperatively.
    pub fn cancellation(&self) -> Cancellation {
        self.inner.cancel.clone()
    }

    /// Spawn a long-lived loop as a child of this supervisor. After
    /// `shutdown` nothing is spawned: a stopped runtime stays stopped.
    pub fn spawn(
        &self,
        label: &'static str,
        policy: Policy,
        fut: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        if self.inner.cancel.is_cancelled() {
            tracing::warn!(task = label, "not spawned: the supervisor is shut down");
            return;
        }
        let handle = tokio::spawn(fut);
        self.inner.tasks.lock().unwrap().push(Supervised {
            name: label,
            policy,
            handle,
        });
    }

    pub fn snapshot(&self) -> Vec<TaskStatus> {
        self.inner
            .tasks
            .lock()
            .unwrap()
            .iter()
            .map(|t| TaskStatus {
                name: t.name,
                policy: t.policy,
                state: if t.handle.is_finished() {
                    TaskState::Exited
                } else {
                    TaskState::Running
                },
            })
            .collect()
    }

    /// The first CRITICAL loop that exited while the runtime was not
    /// shutting down — the condition WP-15's readiness policy fails on.
    pub fn critical_failure(&self) -> Option<&'static str> {
        if self.inner.cancel.is_cancelled() {
            return None;
        }
        self.snapshot()
            .into_iter()
            .find(|t| t.policy == Policy::Critical && t.state == TaskState::Exited)
            .map(|t| t.name)
    }

    /// Ordered, bounded shutdown: signal cancellation, give every task
    /// `grace` to stop, abort the rest. Idempotent; a second call finds
    /// nothing to stop.
    pub async fn shutdown(&self, grace: Duration) -> ShutdownReport {
        let _ = self.inner.cancel_tx.send(true);
        let tasks: Vec<Supervised> = std::mem::take(&mut *self.inner.tasks.lock().unwrap());
        let mut report = ShutdownReport::default();
        let deadline = tokio::time::Instant::now() + grace;
        for mut t in tasks {
            match tokio::time::timeout_at(deadline, &mut t.handle).await {
                Ok(Ok(())) => report.finished.push(t.name),
                Ok(Err(e)) if e.is_panic() => report.panicked.push(t.name),
                Ok(Err(_)) => report.aborted.push(t.name),
                Err(_elapsed) => {
                    t.handle.abort();
                    report.aborted.push(t.name);
                }
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A cooperative loop stops inside the grace period; one that
    /// ignores cancellation is aborted at the deadline; the report names
    /// both, and a second shutdown has nothing left to stop.
    #[tokio::test]
    async fn shutdown_is_ordered_and_bounded() {
        let sup = TaskSupervisor::new();
        let cancel = sup.cancellation();
        let ticks = Arc::new(AtomicUsize::new(0));
        let t = ticks.clone();
        sup.spawn("polite", Policy::Critical, async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                        t.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        sup.spawn("stubborn", Policy::Noncritical, async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(ticks.load(Ordering::Relaxed) >= 2);
        assert_eq!(sup.snapshot().len(), 2);
        assert_eq!(sup.critical_failure(), None);
        let report = sup.shutdown(Duration::from_millis(200)).await;
        assert_eq!(report.finished, vec!["polite"]);
        assert_eq!(report.aborted, vec!["stubborn"]);
        assert!(report.terminated("polite") && report.terminated("stubborn"));
        let after = ticks.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(ticks.load(Ordering::Relaxed), after, "the loop is gone");
        assert_eq!(
            sup.shutdown(Duration::from_millis(10)).await,
            ShutdownReport::default()
        );
        sup.spawn("late", Policy::Noncritical, async {});
        assert!(
            sup.snapshot().is_empty(),
            "a shut-down supervisor spawns nothing"
        );
    }

    /// A critical loop that exits on its own is the failure the policy
    /// exists for; a noncritical exit is not; a panic is reported.
    #[tokio::test]
    async fn critical_exits_are_failures_and_panics_are_reported() {
        let sup = TaskSupervisor::new();
        sup.spawn("hygiene", Policy::Noncritical, async {});
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sup.critical_failure(), None);
        sup.spawn("acker", Policy::Critical, async {});
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sup.critical_failure(), Some("acker"));
        sup.spawn("boom", Policy::Critical, async { panic!("scripted") });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let report = sup.shutdown(Duration::from_millis(50)).await;
        assert!(report.panicked.contains(&"boom"), "{report:?}");
        assert!(report.finished.contains(&"acker") && report.finished.contains(&"hygiene"));
        assert_eq!(
            sup.critical_failure(),
            None,
            "after shutdown nothing is a failure"
        );
    }
}
