//! What a supervisor does when one of its loops ends (item 38).
//!
//! Every supervised loop ends through `ExitWatch`. Under most supervisors
//! that changes nothing: an engine closes itself through its required-exit
//! fence, and a test rig reports the exit as readiness. The process's own
//! supervisor (`TaskSupervisor::process_root`) answers for its critical
//! loops itself, because nothing else would ever stop a runtime that can no
//! longer serve: the first one that ends before any stop was requested, by
//! returning `Done`, returning `Failed` or panicking, is recorded as the
//! cause and requests the ordered stop. A loop that ends after a stop was
//! requested is that stop's consequence and appears only in the join
//! report. Every stop of a process root, this one or a termination
//! signal's, is bounded off the executor (`arm_stop_deadline`), and
//! `ordered_stop` fails the process with the cause once it has run.
//!
//! Limitation: a termination signal asks for its stop through the signal
//! task, which runs on the executor. A SIGTERM that arrives when every
//! executor worker is already blocked is never observed, so it arms no
//! bound, and the process waits for the platform's SIGKILL, as it did
//! before item 38. A critical exit's own stop, and a signal observed before
//! the executor wedged, are bounded.

use super::{Inner, Policy, TaskOutcome, TaskResult, TaskSupervisor, shutdown};
use futures_util::FutureExt;
use std::any::Any;
use std::future::Future;
use std::io::Write;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

/// The binary's bound on its ordered stop: the loops' join (10 s) and the
/// shards' close (10 s) that `bootstrap::run` grants, the runtime's own
/// teardown after `run` returns, and margin.
const PROCESS_STOP_DEADLINE: Duration = Duration::from_secs(30);

/// The first critical loop that ended while its process-root runtime had no
/// stop requested, and how it ended: the cause of the stop the supervisor
/// then requested, and of the process's failure exit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CriticalExit {
    pub name: &'static str,
    pub outcome: TaskOutcome,
}

impl std::fmt::Display for CriticalExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "critical task {} exited while the runtime was running: {:?}",
            self.name, self.outcome
        )
    }
}

/// Whether a stop has been requested, and the request itself: the runtime's
/// cancellation. Generic so the Loom model runs `stop_on_exit` on
/// instrumented primitives.
pub(super) trait StopFlag {
    fn requested(&self) -> bool;
    fn request(&self);
}

impl StopFlag for tokio::sync::watch::Sender<bool> {
    fn requested(&self) -> bool {
        *self.borrow()
    }
    fn request(&self) {
        self.send_replace(true);
    }
}

/// The once-only record of a stop's cause.
pub(super) trait CauseCell<T> {
    /// Records `cause` unless one is recorded; whether this call did.
    fn record(&self, cause: T) -> bool;
}

impl<T> CauseCell<T> for OnceLock<T> {
    fn record(&self, cause: T) -> bool {
        self.set(cause).is_ok()
    }
}

/// The process root's one stop transition. A critical exit that finds a
/// stop already requested is its consequence. Otherwise the first such exit
/// records itself as the cause and only then publishes the stop, so whoever
/// sees the stop sees its cause. Returns whether this exit is the cause.
pub(super) fn stop_on_exit<T>(flag: &impl StopFlag, cause: &impl CauseCell<T>, exit: T) -> bool {
    if flag.requested() {
        return false;
    }
    if !cause.record(exit) {
        return false;
    }
    flag.request();
    true
}

/// A loop's end, as its supervisor answers for it. Holds the supervisor
/// weakly: a loop never keeps its runtime alive.
pub(super) struct ExitWatch {
    supervisor: Weak<Inner>,
    label: &'static str,
    policy: Policy,
}

impl ExitWatch {
    pub(super) fn new(supervisor: &Arc<Inner>, label: &'static str, policy: Policy) -> Self {
        Self {
            supervisor: Arc::downgrade(supervisor),
            label,
            policy,
        }
    }

    /// Runs the loop to its end, answers for that end, then hands the join
    /// handle the same result, or resumes the same panic (the panic hook
    /// does not run twice).
    pub(super) async fn run(self, task: impl Future<Output = TaskResult>) -> TaskResult {
        let ended = std::panic::AssertUnwindSafe(task).catch_unwind().await;
        self.answer(&ended);
        match ended {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn answer(&self, ended: &Result<TaskResult, Box<dyn Any + Send>>) {
        if self.policy != Policy::Critical {
            return;
        }
        let Some(inner) = self.supervisor.upgrade() else {
            return;
        };
        let Some(&deadline) = inner.root.get() else {
            return;
        };
        let exit = CriticalExit {
            name: self.label,
            outcome: outcome_of(ended),
        };
        if stop_on_exit(&inner.cancel_tx, &inner.stop_cause, exit.clone()) {
            // Bounded first: the log line below writes to stdout, which can
            // block, and a published stop must never be left unbounded.
            arm_root_deadline(&inner);
            tracing::error!(
                task = self.label,
                "{exit}; requesting the ordered stop, bounded at {deadline:?}"
            );
        }
    }
}

/// How a loop ended, as the join report will classify it.
fn outcome_of(ended: &Result<TaskResult, Box<dyn Any + Send>>) -> TaskOutcome {
    match ended {
        Ok(TaskResult::Done) => TaskOutcome::Finished,
        Ok(TaskResult::Failed(error)) => TaskOutcome::Failed(error.clone()),
        Err(panic) => TaskOutcome::Panicked(shutdown::panic_message(&**panic)),
    }
}

/// A process root's stop is bounded once, by whichever asked first: its
/// critical exit, or a termination signal's request (owner decision D1).
/// Any other supervisor has no bound to arm.
pub(super) fn arm_root_deadline(inner: &Inner) {
    if let Some(&deadline) = inner.root.get() {
        // A critical exit records itself before it publishes the stop, so the
        // cause at arm time is the stop's own; a signal's request has none.
        let cause = inner.stop_cause.get().map(ToString::to_string);
        inner
            .stop_armed
            .call_once(|| arm_stop_deadline(deadline, cause));
    }
}

/// The bound on a process root's ordered stop, on a thread of its own: a
/// wedged executor cannot hold it. It is never disarmed, since a process
/// whose root was asked to stop is ending either way, and so it also bounds
/// the runtime's teardown after `run` returns, which otherwise waits for
/// every blocking task. A thread that cannot be started cannot bound the
/// stop, so the process ends at once instead (owner decision D3). Its
/// lines name the stop's cause, `cause` (a critical exit), or a signal's
/// request, so the bound's own record does not depend on the stdout log.
#[expect(
    clippy::disallowed_methods,
    reason = "arm_stop_deadline; the process root's bound on its ordered stop must run where a wedged executor cannot hold it, and it ends the process rather than being joined; a supervised task would share the executor it has to outlive"
)]
fn arm_stop_deadline(after: Duration, cause: Option<String>) {
    let cause = cause.unwrap_or_else(|| "a termination signal requested it".into());
    let at_deadline = format!(
        "stop deadline: a stop was requested {after:?} ago and has not finished \
         (cause: {cause}); exiting"
    );
    let armed = std::thread::Builder::new()
        .name("stop-deadline".into())
        .spawn(move || {
            std::thread::sleep(after);
            exit_failed(at_deadline);
        });
    if let Err(error) = armed {
        exit_failed(format!(
            "stop deadline: cannot bound the requested stop ({error}; cause: {cause}); \
             exiting now"
        ));
    }
}

/// How long the process waits for its last stderr line before it exits
/// anyway.
const LAST_LINE_WAIT: Duration = Duration::from_millis(250);

/// Ends the process with code 1, after writing `line` to stderr best effort:
/// the bound must not depend on its message. `eprintln!` panics when the
/// write fails (a closed pipe gives EPIPE), and a write blocks for good on a
/// full pipe nobody reads or behind another thread's held stderr lock; so
/// the line is written on a thread of its own and waited for at most
/// `LAST_LINE_WAIT`. With no thread to write it, the process ends silently.
#[expect(
    clippy::disallowed_methods,
    reason = "exit_failed; the process's last line is written on a thread of its own so a blocked stderr cannot hold the exit; the thread is never joined, since the process ends either way"
)]
fn exit_failed(line: String) -> ! {
    let (written, heard) = std::sync::mpsc::channel();
    let writer = std::thread::Builder::new()
        .name("stop-deadline-line".into())
        .spawn(move || {
            drop(writeln!(std::io::stderr(), "{line}"));
            drop(written.send(()));
        });
    if writer.is_ok() {
        drop(heard.recv_timeout(LAST_LINE_WAIT));
    }
    std::process::exit(1)
}

impl TaskSupervisor {
    /// The process's own supervisor: its first critical exit requests the
    /// ordered stop and fails the process, and every stop it is asked for is
    /// bounded at `PROCESS_STOP_DEADLINE` (item 38). Engines and test rigs
    /// keep `new`, whose owner answers for their loops.
    pub(crate) fn process_root(self) -> Self {
        self.bounded_root(PROCESS_STOP_DEADLINE)
    }

    /// A process root whose ordered stop is bounded at `after`.
    pub(super) fn bounded_root(self, after: Duration) -> Self {
        self.inner.root.get_or_init(|| after);
        self
    }

    /// Why a process root stopped its runtime on its own, if it did.
    pub(super) fn stop_cause(&self) -> Option<CriticalExit> {
        self.inner.stop_cause.get().cloned()
    }

    /// The process's ordered stop, once its accept loop has returned (PR 6-F
    /// / 6.1-A): every supervised loop is cancelled, joined and reported,
    /// then `resources` close (WP-15 §9 sequences admission, engines and
    /// stores ahead of this in its remaining slice). A stop the process root
    /// requested for a critical exit then fails with that cause; what the
    /// same stop failed to close is logged, since the cause outranks it.
    pub(crate) async fn ordered_stop(
        &self,
        grace: Duration,
        resources: impl Future<Output = Result<(), String>>,
    ) -> Result<(), String> {
        let report = self.shutdown(grace).await;
        tracing::info!(
            finished = ?report.finished(),
            failed = ?report.failed(),
            aborted = ?report.aborted,
            panicked = ?report.panicked(),
            "supervised loops stopped"
        );
        let closed = resources.await;
        let Some(cause) = self.stop_cause() else {
            return closed;
        };
        if let Err(error) = closed {
            tracing::error!("resources after the critical exit: {error}");
        }
        Err(cause.to_string())
    }
}

#[cfg(test)]
mod loom_tests;
#[cfg(test)]
mod tests;
