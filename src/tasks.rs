//! Task supervision (WP-02 / PR 6-F, corrected by PR 6.1-A — the first
//! slice of WP-15 §7-9): every long-lived loop a runtime spawns is a
//! child of ONE supervisor that owns its join handle, hands it the
//! cancellation it must observe, keeps its typed result and its failure
//! policy, and stops it in order. Registration and shutdown share one
//! phase-locked state, so a loop can never register after the drain;
//! a loop that ignores cancellation is aborted AND joined, so nothing
//! it owned outlives `shutdown`. Request-scoped child tasks are NOT
//! supervised here — they belong to their request (the HTTP accept
//! loop owns its connections itself, see `http::serve_h1`).
//!
//! A runtime hands its state a read-only [`TaskMonitor`], never the
//! supervisor: the supervisor owns the tasks, the tasks capture the
//! state, and a strong edge from the state back to the supervisor would
//! make a runtime that failed to start immortal.

mod shutdown;
/// The runtime's termination input, prepared before any task starts.
pub(crate) mod signal;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::task::JoinHandle;

/// What losing the loop means for the runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Policy {
    /// The runtime is not healthy without it (fleet loop, telemetry
    /// drain, the watchdogs): an unexpected exit is a critical failure
    /// (surfaced to readiness by WP-15's remaining slice).
    Critical,
    /// Loss degrades observability or hygiene only.
    Noncritical,
}

/// A cooperative cancellation handle: a loop awaits `cancelled()` at
/// every iteration boundary; clones observe the same signal.
#[derive(Clone)]
pub(crate) struct Cancellation {
    rx: tokio::sync::watch::Receiver<bool>,
}

impl Cancellation {
    pub(crate) fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    pub(crate) async fn cancelled(&self) {
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

/// How a supervised loop ended on its own terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TaskResult {
    /// Stopped cleanly (cancelled, or its work is complete).
    Done,
    /// Stopped because it could not continue.
    Failed(String),
}

/// Identity of one supervised task, in registration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TaskId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Running,
    ShuttingDown,
    Stopped,
}

/// Why a spawn was refused: the runtime is stopping or stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SpawnRejected {
    ShuttingDown,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TaskState {
    Running,
    /// Exited on its own while the supervisor was NOT shutting down.
    /// For a critical loop that is the failure the policy exists for.
    Exited,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TaskStatus {
    pub name: &'static str,
    pub policy: Policy,
    pub state: TaskState,
}

/// How each task ended, as observed by joining its handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TaskOutcome {
    Finished,
    Failed(String),
    /// Aborted at the deadline and joined: it is gone.
    Cancelled,
    Panicked(String),
}

/// The outcome of an ordered shutdown, by task name, in REGISTRATION
/// order (PR 6.1.1-A: keyed by `TaskId` and sorted once at completion,
/// so an immediate finisher no longer jumps ahead of a task that had to
/// be aborted). Every task listed has been JOINED: none of them is
/// running when the report exists, and every caller of `shutdown`
/// receives this same report.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ShutdownReport {
    pub outcomes: Vec<(&'static str, TaskOutcome)>,
    /// The subset that ignored cancellation past the grace and was
    /// aborted (then joined).
    pub aborted: Vec<&'static str>,
}

impl ShutdownReport {
    pub(crate) fn names(&self, want: fn(&TaskOutcome) -> bool) -> Vec<&'static str> {
        self.outcomes
            .iter()
            .filter(|(_, o)| want(o))
            .map(|(n, _)| *n)
            .collect()
    }

    pub(crate) fn finished(&self) -> Vec<&'static str> {
        self.names(|o| matches!(o, TaskOutcome::Finished))
    }

    pub(crate) fn panicked(&self) -> Vec<&'static str> {
        self.names(|o| matches!(o, TaskOutcome::Panicked(_)))
    }

    #[cfg(test)]
    pub(crate) fn terminated(&self, name: &str) -> bool {
        self.outcomes.iter().any(|(n, _)| *n == name)
    }
}

struct Supervised {
    name: &'static str,
    policy: Policy,
    handle: JoinHandle<TaskResult>,
}

struct SupervisorState {
    phase: Phase,
    next_id: u64,
    /// Only ever non-empty while `Running`: the shutdown driver takes
    /// the whole map in one step and owns it from then on.
    tasks: BTreeMap<TaskId, Supervised>,
    /// Present once a shutdown driver exists. Waiting on this is how
    /// EVERY caller observes the one shutdown — including callers that
    /// arrive after the driver started.
    completion: Option<tokio::sync::watch::Receiver<Option<Arc<ShutdownReport>>>>,
    /// The terminal report, once the driver has joined everything.
    report: Option<Arc<ShutdownReport>>,
    /// Workers are joined before resource finalization. This is observable
    /// separately from full termination and never claims stores are closed.
    workers: Option<Arc<ShutdownReport>>,
}

struct Inner {
    state: Mutex<SupervisorState>,
    cancel_tx: tokio::sync::watch::Sender<bool>,
    cancel: Cancellation,
    workers_done: tokio::sync::Notify,
}

impl Inner {
    #[expect(
        clippy::unwrap_used,
        reason = "Supervisor task snapshot; poisoned registration may have failed between assigning an ID and retaining its handle; returning a partial task list would hide incomplete ownership"
    )]
    fn snapshot(&self) -> Vec<TaskStatus> {
        self.state
            .lock()
            .unwrap()
            .tasks
            .values()
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

    #[expect(
        clippy::unwrap_used,
        reason = "Supervisor phase observation; poisoned registration or shutdown cannot prove a valid lifecycle phase; silent recovery could report a partially published transition"
    )]
    fn phase(&self) -> Phase {
        self.state.lock().unwrap().phase
    }

    /// The first CRITICAL loop that exited while the runtime was not
    /// shutting down — the condition WP-15's readiness policy fails on.
    fn critical_failure(&self) -> Option<&'static str> {
        if self.phase() != Phase::Running {
            return None;
        }
        self.snapshot()
            .into_iter()
            .find(|t| t.policy == Policy::Critical && t.state == TaskState::Exited)
            .map(|t| t.name)
    }
}

/// The owner of a runtime's long-lived loops.
#[derive(Clone)]
pub(crate) struct TaskSupervisor {
    inner: Arc<Inner>,
}

/// A weak handle that can only REQUEST the ordered shutdown — what a
/// signal handler needs. It keeps nothing alive.
#[derive(Clone)]
pub(crate) struct ShutdownRequest {
    inner: Weak<Inner>,
}

impl ShutdownRequest {
    pub(crate) fn request(&self) {
        if let Some(inner) = self.inner.upgrade() {
            TaskSupervisor { inner }.cancel();
        }
    }
}

/// A read-only view for health and debug surfaces. It holds no strong
/// reference: a runtime whose supervisor is gone reports nothing.
#[derive(Clone)]
pub(crate) struct TaskMonitor {
    inner: Weak<Inner>,
}

impl TaskMonitor {
    /// Serving requires a live supervisor in its running phase and all
    /// required loops still running. Recoverable errors inside a loop do
    /// not change this verdict; permanent task exit does.
    #[expect(
        clippy::unwrap_used,
        reason = "Supervisor readiness; poisoned lifecycle state is not evidence of a healthy runtime; returning a readiness result would conceal incomplete registration or shutdown"
    )]
    pub(crate) fn unready_reason(&self) -> Option<String> {
        let Some(inner) = self.inner.upgrade() else {
            return Some("runtime supervisor unavailable".into());
        };
        let state = inner.state.lock().unwrap();
        match state.phase {
            Phase::ShuttingDown => Some("runtime shutting down".into()),
            Phase::Stopped => Some("runtime stopped".into()),
            Phase::Running => state.tasks.values().find_map(|task| {
                (task.policy == Policy::Critical && task.handle.is_finished())
                    .then(|| format!("critical task terminated: {}", task.name))
            }),
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<TaskStatus> {
        self.inner
            .upgrade()
            .map(|i| i.snapshot())
            .unwrap_or_default()
    }

    pub(crate) fn critical_failure(&self) -> Option<&'static str> {
        self.inner.upgrade().and_then(|i| i.critical_failure())
    }

    pub(crate) fn phase(&self) -> Option<Phase> {
        self.inner.upgrade().map(|i| i.phase())
    }
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskSupervisor {
    pub(crate) fn new() -> Self {
        let (cancel_tx, rx) = tokio::sync::watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(SupervisorState {
                    phase: Phase::Running,
                    next_id: 0,
                    tasks: BTreeMap::new(),
                    completion: None,
                    report: None,
                    workers: None,
                }),
                cancel_tx,
                cancel: Cancellation { rx },
                workers_done: tokio::sync::Notify::new(),
            }),
        }
    }

    /// The handle a loop selects on to stop cooperatively (every
    /// supervised loop also receives it from `spawn`).
    pub(crate) fn cancellation(&self) -> Cancellation {
        self.inner.cancel.clone()
    }

    pub(crate) fn monitor(&self) -> TaskMonitor {
        TaskMonitor {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(crate) fn shutdown_request(&self) -> ShutdownRequest {
        ShutdownRequest {
            inner: Arc::downgrade(&self.inner),
        }
    }

    #[cfg(test)]
    pub(crate) fn phase(&self) -> Phase {
        self.inner.phase()
    }

    /// Spawn a long-lived loop as a child of this supervisor. The loop
    /// is BUILT with the cancellation it must observe, so no supervised
    /// loop can be written without one. Registration and the phase
    /// check are one atomic step: once shutdown has begun, nothing is
    /// spawned — a stopped runtime stays stopped.
    #[expect(
        clippy::unwrap_used,
        reason = "Supervisor registration; a poisoned phase may contain an incomplete task insertion; recovering and spawning again could leave a task outside the eventual drain"
    )]
    pub(crate) fn spawn<F, Fut>(
        &self,
        label: &'static str,
        policy: Policy,
        build: F,
    ) -> Result<TaskId, SpawnRejected>
    where
        F: FnOnce(Cancellation) -> Fut,
        Fut: std::future::Future<Output = TaskResult> + Send + 'static,
    {
        let mut st = self.inner.state.lock().unwrap();
        match st.phase {
            Phase::Running => {}
            Phase::ShuttingDown => return Err(SpawnRejected::ShuttingDown),
            Phase::Stopped => return Err(SpawnRejected::Stopped),
        }
        let id = TaskId(st.next_id);
        st.next_id += 1;
        #[expect(
            clippy::disallowed_methods,
            reason = "TaskSupervisor worker owner; the registration lock retains each handle before shutdown can take the map; spawning through another supervisor would recursively delegate this canonical owner"
        )]
        let handle = tokio::spawn(build(self.inner.cancel.clone()));
        st.tasks.insert(
            id,
            Supervised {
                name: label,
                policy,
                handle,
            },
        );
        Ok(id)
    }

    /// Request the ordered shutdown without waiting for it: the phase
    /// moves to `ShuttingDown` (no further spawns) and every loop sees
    /// cancellation. A signal handler's move; `shutdown` completes it.
    #[expect(
        clippy::unwrap_used,
        reason = "TaskSupervisor cancellation; a poisoned phase may reflect incomplete registration; silent recovery could abandon a task outside the shutdown drain"
    )]
    pub(crate) fn cancel(&self) {
        {
            let mut st = self.inner.state.lock().unwrap();
            if st.phase == Phase::Running {
                st.phase = Phase::ShuttingDown;
            }
        }
        self.inner.cancel_tx.send_replace(true);
    }
}

#[cfg(test)]
mod tests;
