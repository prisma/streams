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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
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

/// A cooperative cancellation handle: a loop awaits `cancelled()` at
/// every iteration boundary; clones observe the same signal.
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

/// How a supervised loop ended on its own terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskResult {
    /// Stopped cleanly (cancelled, or its work is complete).
    Done,
    /// Stopped because it could not continue.
    Failed(String),
}

/// Identity of one supervised task, in registration order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Running,
    ShuttingDown,
    Stopped,
}

/// Why a spawn was refused: the runtime is stopping or stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpawnRejected {
    ShuttingDown,
    Stopped,
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

/// How each task ended, as observed by joining its handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskOutcome {
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
pub struct ShutdownReport {
    pub outcomes: Vec<(&'static str, TaskOutcome)>,
    /// The subset that ignored cancellation past the grace and was
    /// aborted (then joined).
    pub aborted: Vec<&'static str>,
}

impl ShutdownReport {
    pub fn names(&self, want: fn(&TaskOutcome) -> bool) -> Vec<&'static str> {
        self.outcomes
            .iter()
            .filter(|(_, o)| want(o))
            .map(|(n, _)| *n)
            .collect()
    }

    pub fn finished(&self) -> Vec<&'static str> {
        self.names(|o| matches!(o, TaskOutcome::Finished))
    }

    pub fn panicked(&self) -> Vec<&'static str> {
        self.names(|o| matches!(o, TaskOutcome::Panicked(_)))
    }

    #[cfg(test)]
    pub fn terminated(&self, name: &str) -> bool {
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
}

struct Inner {
    state: Mutex<SupervisorState>,
    cancel_tx: tokio::sync::watch::Sender<bool>,
    cancel: Cancellation,
}

impl Inner {
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
pub struct TaskSupervisor {
    inner: Arc<Inner>,
}

/// A weak handle that can only REQUEST the ordered shutdown — what a
/// signal handler needs. It keeps nothing alive.
#[derive(Clone)]
pub struct ShutdownRequest {
    inner: Weak<Inner>,
}

impl ShutdownRequest {
    pub fn request(&self) {
        if let Some(inner) = self.inner.upgrade() {
            TaskSupervisor { inner }.cancel();
        }
    }
}

/// A read-only view for health and debug surfaces. It holds no strong
/// reference: a runtime whose supervisor is gone reports nothing.
#[derive(Clone)]
pub struct TaskMonitor {
    inner: Weak<Inner>,
}

impl TaskMonitor {
    pub fn snapshot(&self) -> Vec<TaskStatus> {
        self.inner
            .upgrade()
            .map(|i| i.snapshot())
            .unwrap_or_default()
    }

    pub fn critical_failure(&self) -> Option<&'static str> {
        self.inner.upgrade().and_then(|i| i.critical_failure())
    }

    pub fn phase(&self) -> Option<Phase> {
        self.inner.upgrade().map(|i| i.phase())
    }
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
                state: Mutex::new(SupervisorState {
                    phase: Phase::Running,
                    next_id: 0,
                    tasks: BTreeMap::new(),
                    completion: None,
                    report: None,
                }),
                cancel_tx,
                cancel: Cancellation { rx },
            }),
        }
    }

    /// The handle a loop selects on to stop cooperatively (every
    /// supervised loop also receives it from `spawn`).
    pub fn cancellation(&self) -> Cancellation {
        self.inner.cancel.clone()
    }

    pub fn monitor(&self) -> TaskMonitor {
        TaskMonitor {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub fn shutdown_request(&self) -> ShutdownRequest {
        ShutdownRequest {
            inner: Arc::downgrade(&self.inner),
        }
    }

    #[cfg(test)]
    pub fn phase(&self) -> Phase {
        self.inner.phase()
    }

    /// Spawn a long-lived loop as a child of this supervisor. The loop
    /// is BUILT with the cancellation it must observe, so no supervised
    /// loop can be written without one. Registration and the phase
    /// check are one atomic step: once shutdown has begun, nothing is
    /// spawned — a stopped runtime stays stopped.
    pub fn spawn<F, Fut>(
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
    pub fn cancel(&self) {
        {
            let mut st = self.inner.state.lock().unwrap();
            if st.phase == Phase::Running {
                st.phase = Phase::ShuttingDown;
            }
        }
        let _ = self.inner.cancel_tx.send(true);
    }

    /// Ordered, bounded shutdown: close registration, signal
    /// cancellation, give every task until one shared deadline, abort
    /// the survivors, then JOIN every task — aborted ones included — and
    /// record how each ended. When this returns, no supervised task is
    /// running.
    ///
    /// PR 6.1.1-A: shutdown is SINGLE-FLIGHT and CANCELLATION-SAFE. The
    /// first caller moves the task handles into one internally spawned
    /// driver whose lifetime does not depend on any caller; every caller
    /// (including this one) then awaits that driver's completion and
    /// receives the SAME terminal report. Dropping a waiting caller
    /// cannot detach the tasks, because the caller never owned them, and
    /// only the driver may declare the runtime `Stopped`.
    pub async fn shutdown(&self, grace: Duration) -> ShutdownReport {
        let mut rx = {
            let mut st = self.inner.state.lock().unwrap();
            if let Some(report) = st.report.clone() {
                return (*report).clone();
            }
            match st.completion.clone() {
                // A driver is already running: wait for it, do not start
                // a second one and never declare Stopped ourselves.
                Some(rx) => rx,
                None => {
                    st.phase = Phase::ShuttingDown;
                    let tasks = std::mem::take(&mut st.tasks);
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    st.completion = Some(rx.clone());
                    let inner = self.inner.clone();
                    // The DRIVER owns the handles from here. Spawned, so
                    // cancelling any waiting caller cannot strand them.
                    tokio::spawn(async move {
                        let report = Arc::new(drive_shutdown(&inner, tasks, grace).await);
                        {
                            let mut st = inner.state.lock().unwrap();
                            st.phase = Phase::Stopped;
                            st.report = Some(report.clone());
                        }
                        let _ = tx.send(Some(report));
                    });
                    rx
                }
            }
        };
        let _ = self.inner.cancel_tx.send(true);
        loop {
            if let Some(report) = rx.borrow().clone() {
                return (*report).clone();
            }
            if rx.changed().await.is_err() {
                // The driver's sender is gone without a report only if
                // the runtime is tearing down; report what state holds.
                return self
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .report
                    .clone()
                    .map(|r| (*r).clone())
                    .unwrap_or_default();
            }
        }
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A cooperative loop stops inside the grace period; one that
    /// ignores cancellation is aborted AND joined; the report names
    /// both; a second shutdown has nothing left to stop; nothing can be
    /// spawned afterwards.
    #[tokio::test]
    async fn shutdown_is_ordered_bounded_and_joins_everything() {
        let sup = TaskSupervisor::new();
        let ticks = Arc::new(AtomicUsize::new(0));
        let t = ticks.clone();
        sup.spawn("polite", Policy::Critical, move |cancel| async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return TaskResult::Done,
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {
                        t.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        })
        .unwrap();
        sup.spawn("stubborn", Policy::Noncritical, |_cancel| async move {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(ticks.load(Ordering::Relaxed) >= 2);
        assert_eq!(sup.monitor().snapshot().len(), 2);
        assert_eq!(sup.monitor().critical_failure(), None);
        assert_eq!(sup.phase(), Phase::Running);
        let report = sup.shutdown(Duration::from_millis(200)).await;
        assert_eq!(report.finished(), vec!["polite"]);
        assert_eq!(report.aborted, vec!["stubborn"]);
        assert_eq!(
            report.outcomes,
            vec![
                ("polite", TaskOutcome::Finished),
                ("stubborn", TaskOutcome::Cancelled)
            ]
        );
        assert_eq!(sup.phase(), Phase::Stopped);
        let after = ticks.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(ticks.load(Ordering::Relaxed), after, "the loop is gone");
        let again = sup.shutdown(Duration::from_millis(10)).await;
        assert_eq!(again, report, "every caller sees the same terminal report");
        assert_eq!(
            sup.spawn("late", Policy::Noncritical, |_| async { TaskResult::Done }),
            Err(SpawnRejected::Stopped)
        );
        assert!(sup.monitor().snapshot().is_empty());
    }

    /// Registration and shutdown share one phase-locked state: however
    /// many spawns race the drain, every spawn that succeeded is in the
    /// report (joined), every other one was refused, and no task is
    /// alive when shutdown returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registration_cannot_race_shutdown() {
        for _round in 0..20 {
            let sup = TaskSupervisor::new();
            let alive = Arc::new(AtomicUsize::new(0));
            let accepted = Arc::new(AtomicUsize::new(0));
            let refused = Arc::new(AtomicUsize::new(0));
            let mut spawners = Vec::new();
            for _ in 0..8 {
                let sup2 = sup.clone();
                let (alive, accepted, refused) = (alive.clone(), accepted.clone(), refused.clone());
                spawners.push(tokio::spawn(async move {
                    loop {
                        let alive2 = alive.clone();
                        match sup2.spawn("racer", Policy::Noncritical, move |cancel| async move {
                            alive2.fetch_add(1, Ordering::SeqCst);
                            cancel.cancelled().await;
                            alive2.fetch_sub(1, Ordering::SeqCst);
                            TaskResult::Done
                        }) {
                            Ok(_) => {
                                accepted.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(_) => {
                                refused.fetch_add(1, Ordering::SeqCst);
                                return;
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                }));
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
            let report = sup.shutdown(Duration::from_millis(500)).await;
            for s in spawners {
                s.await.unwrap();
            }
            assert_eq!(report.outcomes.len(), accepted.load(Ordering::SeqCst));
            assert!(report.aborted.is_empty(), "{report:?}");
            assert_eq!(alive.load(Ordering::SeqCst), 0, "a racer outlived shutdown");
            assert_eq!(
                refused.load(Ordering::SeqCst),
                8,
                "every spawner was refused once"
            );
            assert!(sup.monitor().snapshot().is_empty());
        }
    }

    /// An aborted task's resources are gone BEFORE shutdown returns: the
    /// drop probe inside the stubborn future has fired.
    #[tokio::test]
    async fn aborted_tasks_are_destroyed_before_shutdown_returns() {
        struct Probe(Arc<AtomicBool>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let sup = TaskSupervisor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = Probe(dropped.clone());
        sup.spawn("holder", Policy::Critical, move |_cancel| async move {
            let _probe = probe;
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!dropped.load(Ordering::SeqCst));
        let report = sup.shutdown(Duration::from_millis(20)).await;
        assert!(
            dropped.load(Ordering::SeqCst),
            "the probe must be dropped before return"
        );
        assert_eq!(report.outcomes, vec![("holder", TaskOutcome::Cancelled)]);
    }

    /// A critical loop that exits on its own is the failure the policy
    /// exists for; a noncritical exit is not; a panic and a typed
    /// failure are reported as such; the monitor sees the same facts
    /// without owning anything.
    #[tokio::test]
    async fn critical_exits_failures_and_panics_are_reported() {
        let sup = TaskSupervisor::new();
        let mon = sup.monitor();
        sup.spawn("hygiene", Policy::Noncritical, |_| async {
            TaskResult::Done
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mon.critical_failure(), None);
        sup.spawn("acker", Policy::Critical, |_| async { TaskResult::Done })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mon.critical_failure(), Some("acker"));
        sup.spawn("boom", Policy::Critical, |_| async { panic!("scripted") })
            .unwrap();
        sup.spawn("broken", Policy::Critical, |_| async {
            TaskResult::Failed("store gone".into())
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(mon.phase(), Some(Phase::Running));
        let report = sup.shutdown(Duration::from_millis(50)).await;
        assert_eq!(report.panicked(), vec!["boom"]);
        assert!(
            report
                .outcomes
                .contains(&("broken", TaskOutcome::Failed("store gone".into())))
        );
        assert!(report.finished().contains(&"acker") && report.finished().contains(&"hygiene"));
        assert_eq!(
            mon.critical_failure(),
            None,
            "after shutdown nothing is a failure"
        );
        assert_eq!(mon.phase(), Some(Phase::Stopped));
        drop(sup);
        assert_eq!(mon.phase(), None, "the monitor holds nothing alive");
    }

    /// `cancel` alone closes registration and signals every loop; the
    /// later `shutdown` completes the join.
    #[tokio::test]
    async fn cancel_closes_registration_before_the_join() {
        let sup = TaskSupervisor::new();
        let seen = Arc::new(AtomicBool::new(false));
        let s = seen.clone();
        sup.spawn("loop", Policy::Critical, move |cancel| async move {
            cancel.cancelled().await;
            s.store(true, Ordering::SeqCst);
            TaskResult::Done
        })
        .unwrap();
        sup.cancel();
        assert_eq!(sup.phase(), Phase::ShuttingDown);
        assert_eq!(
            sup.spawn("late", Policy::Noncritical, |_| async { TaskResult::Done }),
            Err(SpawnRejected::ShuttingDown)
        );
        let report = sup.shutdown(Duration::from_millis(100)).await;
        assert!(seen.load(Ordering::SeqCst));
        assert_eq!(report.finished(), vec!["loop"]);
    }

    /// PR 6.1.1-A: shutdown is SINGLE-FLIGHT. Two callers race it while
    /// a stubborn task is alive: both stay pending until that task has
    /// been aborted AND joined, both get the same report, and neither
    /// can declare the runtime stopped early.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_shutdowns_share_one_run_and_one_report() {
        let sup = TaskSupervisor::new();
        let alive = Arc::new(AtomicUsize::new(0));
        let a2 = alive.clone();
        sup.spawn("stubborn", Policy::Critical, move |_cancel| async move {
            a2.fetch_add(1, Ordering::SeqCst);
            let _guard = scopeguard(a2.clone());
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(alive.load(Ordering::SeqCst), 1);
        let s1 = sup.clone();
        let s2 = sup.clone();
        let mon = sup.monitor();
        let h1 = tokio::spawn(async move { s1.shutdown(Duration::from_millis(150)).await });
        let h2 = tokio::spawn(async move { s2.shutdown(Duration::from_millis(150)).await });
        // While the grace period runs, neither caller may have returned
        // and the monitor must NOT claim the runtime is stopped.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !h1.is_finished() && !h2.is_finished(),
            "callers wait for the drain"
        );
        assert_eq!(mon.phase(), Some(Phase::ShuttingDown));
        assert_eq!(alive.load(Ordering::SeqCst), 1, "the task is still running");
        let (r1, r2) = (h1.await.unwrap(), h2.await.unwrap());
        assert_eq!(r1, r2, "both callers receive the same terminal report");
        assert_eq!(r1.aborted, vec!["stubborn"]);
        assert_eq!(
            alive.load(Ordering::SeqCst),
            0,
            "joined, not merely aborted"
        );
        assert_eq!(mon.phase(), Some(Phase::Stopped));
    }

    /// PR 6.1.1-A: shutdown is CANCELLATION-SAFE. The first caller is
    /// dropped after the drain has begun; the handles belong to the
    /// driver, not to that future, so a later caller still waits for
    /// real termination and the task's resources are gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_a_waiting_shutdown_cannot_detach_the_tasks() {
        struct Probe(Arc<AtomicBool>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let sup = TaskSupervisor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = Probe(dropped.clone());
        sup.spawn("holder", Policy::Critical, move |_cancel| async move {
            let _probe = probe;
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Poll the first shutdown just long enough to start the drain,
        // then DROP it.
        let first = sup.shutdown(Duration::from_millis(120));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), first)
                .await
                .is_err(),
            "the drain is under way"
        );
        assert!(!dropped.load(Ordering::SeqCst), "still running");
        assert_eq!(sup.monitor().phase(), Some(Phase::ShuttingDown));
        // A later caller must still observe REAL termination.
        let report = sup.shutdown(Duration::from_millis(120)).await;
        assert!(
            dropped.load(Ordering::SeqCst),
            "the task was joined, not detached"
        );
        assert_eq!(report.aborted, vec!["holder"]);
        assert_eq!(sup.monitor().phase(), Some(Phase::Stopped));
    }

    /// PR 6.1.1-A: outcomes are in REGISTRATION order, whatever order
    /// the tasks happen to end in.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn outcomes_are_reported_in_registration_order() {
        let sup = TaskSupervisor::new();
        sup.spawn("first-stubborn", Policy::Noncritical, |_| async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        })
        .unwrap();
        sup.spawn("second-immediate", Policy::Noncritical, |_| async {
            TaskResult::Done
        })
        .unwrap();
        sup.spawn("third-polite", Policy::Noncritical, |cancel| async move {
            cancel.cancelled().await;
            TaskResult::Done
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let report = sup.shutdown(Duration::from_millis(80)).await;
        assert_eq!(
            report.outcomes.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec!["first-stubborn", "second-immediate", "third-polite"],
        );
        assert_eq!(report.aborted, vec!["first-stubborn"]);
    }

    /// A live counter guard: decrements when the task's future is
    /// dropped, so "joined" is distinguishable from "abort requested".
    fn scopeguard(alive: Arc<AtomicUsize>) -> impl Drop {
        struct G(Arc<AtomicUsize>);
        impl Drop for G {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        G(alive)
    }
}
