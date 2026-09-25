#![cfg(test)]

use super::{Phase, Policy, SpawnRejected, TaskOutcome, TaskResult, TaskSupervisor};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, TryLockError};
use std::{future::pending, sync::Arc, time::Duration};

#[tokio::test]
async fn readiness_changes_after_each_kind_of_critical_exit() {
    let outcomes: [fn() -> TaskResult; 3] = [
        || TaskResult::Done,
        || TaskResult::Failed("permanent failure".into()),
        || panic!("critical panic"),
    ];
    for outcome in outcomes {
        critical_exit_case(outcome).await;
    }
}

async fn critical_exit_case(outcome: fn() -> TaskResult) {
    let supervisor = TaskSupervisor::new();
    let monitor = supervisor.monitor();
    let (release, wait) = tokio::sync::oneshot::channel();
    supervisor
        .spawn("required", Policy::Critical, |_| async move {
            wait.await.unwrap();
            outcome()
        })
        .unwrap();
    assert_eq!(monitor.unready_reason(), None);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while monitor.unready_reason().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        monitor.unready_reason().as_deref(),
        Some("critical task terminated: required")
    );
    supervisor.shutdown(Duration::from_secs(1)).await;
    assert_eq!(
        monitor.critical_failure(),
        None,
        "shutdown is not an unexpected exit"
    );
    assert_eq!(monitor.unready_reason().as_deref(), Some("runtime stopped"));
}

#[tokio::test]
async fn readiness_ignores_best_effort_exit_and_recoverable_loop_errors() {
    let supervisor = TaskSupervisor::new();
    let monitor = supervisor.monitor();
    let (recovered, recovery) = tokio::sync::oneshot::channel();
    supervisor
        .spawn("refresh", Policy::Critical, |cancel| async move {
            let transient: Result<(), &str> = Err("temporary source outage");
            assert!(transient.is_err());
            recovered.send(()).unwrap();
            cancel.cancelled().await;
            TaskResult::Done
        })
        .unwrap();
    supervisor
        .spawn("best effort", Policy::Noncritical, |_| async {
            TaskResult::Failed("optional task stopped".into())
        })
        .unwrap();
    recovery.await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(monitor.unready_reason(), None);
    supervisor.cancel();
    assert_eq!(
        monitor.unready_reason().as_deref(),
        Some("runtime shutting down")
    );
    supervisor.shutdown(Duration::from_secs(1)).await;
    drop(supervisor);
    assert_eq!(
        monitor.unready_reason().as_deref(),
        Some("runtime supervisor unavailable")
    );
}

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
        pending().await
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
#[expect(
    clippy::disallowed_methods,
    reason = "Registration-race fixture; all eight spawners are joined and return only after supervisor refusal; supervising them would prevent the race under test"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_cannot_race_shutdown() {
    for _round in 0..20 {
        let sup = TaskSupervisor::new();
        let alive = Arc::new(AtomicUsize::new(0));
        let mut spawners = Vec::new();
        for _ in 0..8 {
            spawners.push(tokio::spawn(register_until_refused(
                sup.clone(),
                alive.clone(),
            )));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let report = sup.shutdown(Duration::from_millis(500)).await;
        let mut accepted = 0;
        for spawner in spawners {
            accepted += spawner.await.unwrap();
        }
        assert_eq!(report.outcomes.len(), accepted);
        assert!(report.aborted.is_empty(), "{report:?}");
        assert_eq!(alive.load(Ordering::SeqCst), 0, "a racer outlived shutdown");
        assert!(sup.monitor().snapshot().is_empty());
    }
}

/// An aborted task's resources are gone BEFORE shutdown returns: the
/// drop probe inside the stubborn future has fired.
#[tokio::test]
async fn aborted_tasks_are_destroyed_before_shutdown_returns() {
    let sup = TaskSupervisor::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropFlag(dropped.clone());
    sup.spawn("holder", Policy::Critical, move |_cancel| async move {
        let _probe = probe;
        pending().await
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
#[expect(
    clippy::disallowed_methods,
    reason = "Concurrent-shutdown fixture; both request tasks are joined and compared after the bounded worker drain; serial calls would not exercise concurrent observers"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdowns_share_one_run_and_one_report() {
    let sup = TaskSupervisor::new();
    let alive = Arc::new(AtomicUsize::new(0));
    let a2 = alive.clone();
    sup.spawn("stubborn", Policy::Critical, move |_cancel| async move {
        a2.fetch_add(1, Ordering::SeqCst);
        let _guard = scopeguard(a2);
        pending().await
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
    let sup = TaskSupervisor::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropFlag(dropped.clone());
    sup.spawn("holder", Policy::Critical, move |_cancel| async move {
        let _probe = probe;
        pending().await
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
        pending().await
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

/// Destruction, not an abort request, releases the fixture's resource.
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A spawner returns its accepted count only after shutdown refuses it.
/// Joining all eight proves every spawner reached that refusal boundary.
async fn register_until_refused(supervisor: TaskSupervisor, alive: Arc<AtomicUsize>) -> usize {
    let mut accepted = 0;
    loop {
        let active = alive.clone();
        if supervisor
            .spawn("racer", Policy::Noncritical, move |cancel| async move {
                active.fetch_add(1, Ordering::SeqCst);
                cancel.cancelled().await;
                active.fetch_sub(1, Ordering::SeqCst);
                TaskResult::Done
            })
            .is_err()
        {
            return accepted;
        }
        accepted += 1;
        tokio::task::yield_now().await;
    }
}

#[test]
fn cancellation_refuses_poisoned_registration_state() {
    let supervisor = TaskSupervisor::new();
    let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = supervisor.inner.state.lock().unwrap();
        panic!("registration interrupted while holding the phase lock");
    }));
    assert!(poison.is_err());
    let cancelled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| supervisor.cancel()));
    assert!(
        cancelled.is_err(),
        "poison cannot manufacture a completed cancellation"
    );
    assert!(!supervisor.cancellation().is_cancelled());
}

#[tokio::test]
async fn a_panicking_builder_poison_prevents_health_claims_and_later_spawns() {
    let supervisor = TaskSupervisor::new();
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        supervisor.spawn(
            "broken-builder",
            Policy::Critical,
            |_| -> std::future::Ready<TaskResult> {
                panic!("builder failed after the registration ID was assigned");
            },
        )
    }));
    assert!(rejected.is_err());
    assert!(supervisor.inner.state.is_poisoned());
    let observations: [fn(&TaskSupervisor); 4] = [
        |s| {
            let _snapshot = s.monitor().snapshot();
        },
        |s| {
            let _phase = s.monitor().phase();
        },
        |s| {
            let _ready = s.monitor().unready_reason();
        },
        |s| {
            let _failure = s.monitor().critical_failure();
        },
    ];
    for observe in observations {
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observe(&supervisor)))
                .is_err()
        );
    }
    let built = std::cell::Cell::new(false);
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        supervisor.spawn("after-poison", Policy::Critical, |_| {
            built.set(true);
            std::future::ready(TaskResult::Done)
        })
    }));
    assert!(rejected.is_err());
    assert!(!built.get(), "the later future must not be constructed");
    assert!(!supervisor.cancellation().is_cancelled());
}

/// Item 37: the accept loop's panicked connections are counted on the
/// runtime's task record, which a monitor reads while the supervisor
/// lives and reports as none once it is gone.
#[test]
fn panicked_connections_are_counted_on_the_monitor() {
    let supervisor = TaskSupervisor::new();
    let monitor = supervisor.monitor();
    assert_eq!(monitor.connection_panics(), 0);
    supervisor.record_connection_panic();
    supervisor.record_connection_panic();
    assert_eq!(monitor.connection_panics(), 2);
    drop(supervisor);
    assert_eq!(monitor.connection_panics(), 0);
}

/// F-G: a runtime that is shutting down refuses a new task by dropping its
/// future INSIDE `tokio::spawn`. An engine's required task carries a guard
/// whose drop closes the engine, which launches that supervisor's shutdown.
/// If `spawn` still held the registration lock at that moment, the drop
/// re-entered it on the same thread: the worker deadlocked and the runtime
/// drop (which joins every worker, with no timeout) never returned.
#[test]
fn a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_teardown_completes(runtime, 1, LateDrop::ShutsDown);
}

/// The same refusal on a multi-threaded runtime whose teardown drops many
/// supervisors' refused futures on several workers at once.
#[test]
fn a_multi_thread_teardown_cannot_deadlock_any_supervisor() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    assert_teardown_completes(runtime, 16, LateDrop::ShutsDown);
}

/// A refused future whose destructor spawns again: the nested spawn is
/// refused too, and its future is set aside and dropped outside the lock
/// the same way.
#[test]
fn a_refused_future_that_spawns_again_is_set_aside_again() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_teardown_completes(runtime, 1, LateDrop::SpawnsAgain);
}

/// What the late task's guard does when the closing runtime drops it.
#[derive(Clone, Copy)]
enum LateDrop {
    ShutsDown,
    SpawnsAgain,
}

/// Counts the refused futures that were dropped, not merely set aside.
struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct LateGuard(TaskSupervisor, LateDrop, Counted);
impl Drop for LateGuard {
    fn drop(&mut self) {
        match self.1 {
            LateDrop::ShutsDown => {
                self.0
                    .begin_shutdown_with(Duration::ZERO, "late-finalizer", async {
                        TaskResult::Done
                    });
            }
            LateDrop::SpawnsAgain => {
                let counted = Counted(self.2.0.clone());
                let nested = self
                    .0
                    .spawn("nested", Policy::Critical, move |_| async move {
                        let _counted = counted;
                        TaskResult::Done
                    });
                assert!(nested.is_ok(), "the supervisor itself is still running");
            }
        }
    }
}

struct SpawnsOnDrop(TaskSupervisor, LateDrop, Arc<AtomicUsize>);
impl Drop for SpawnsOnDrop {
    fn drop(&mut self) {
        let guard = LateGuard(self.0.clone(), self.1, Counted(self.2.clone()));
        let spawned = self.0.spawn("late", Policy::Critical, move |_| async move {
            let _guard = guard;
            TaskResult::Done
        });
        assert!(spawned.is_ok(), "the supervisor itself is still running");
    }
}

/// Drops `runtime` while `owners` tasks each hold a supervisor that spawns
/// from its own drop, and fails (instead of hanging) if the teardown does not
/// finish or leaves a refused future undropped.
fn assert_teardown_completes(runtime: tokio::runtime::Runtime, owners: usize, late: LateDrop) {
    let dropped = Arc::new(AtomicUsize::new(0));
    for _ in 0..owners {
        let owner = SpawnsOnDrop(TaskSupervisor::new(), late, dropped.clone());
        #[expect(
            clippy::disallowed_methods,
            reason = "F-G deadlock rig; the task exists only to be dropped by the runtime teardown under test; supervising it would put the supervisor under test in its own teardown path"
        )]
        runtime.spawn(async move {
            let _owner = owner;
            pending::<()>().await;
        });
    }
    runtime.block_on(tokio::task::yield_now());
    let (done, finished) = std::sync::mpsc::channel();
    // A deadlocked teardown never returns: it runs on its own thread so
    // the assertion below can report it instead of hanging the suite.
    #[expect(
        clippy::disallowed_methods,
        reason = "F-G deadlock rig; the runtime drop under test may never return, so it runs on a detached thread the bounded receive below observes; a runtime task cannot host the drop of its own runtime"
    )]
    std::thread::spawn(move || {
        drop(runtime);
        done.send(()).ok();
    });
    assert!(
        finished.recv_timeout(Duration::from_secs(10)).is_ok(),
        "runtime teardown deadlocked inside TaskSupervisor::spawn"
    );
    let per_owner = match late {
        LateDrop::ShutsDown => 1,
        LateDrop::SpawnsAgain => 2,
    };
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        owners * per_owner,
        "every refused future is dropped once its spawn has released the lock"
    );
}

/// F-G slot: a spawn call that unwinds (there is no runtime to spawn onto)
/// happens while its caller holds the registration lock. The future it
/// refused is not dropped inside the call, where its destructor could take
/// that lock, and the thread's slot still closes: an open slot would park
/// every later supervised drop on this thread instead of dropping it.
#[test]
fn a_spawn_that_unwinds_closes_its_slot_and_drops_nothing_under_the_lock() {
    struct TriesLock(Arc<Mutex<()>>, Arc<AtomicBool>);
    impl Drop for TriesLock {
        fn drop(&mut self) {
            if matches!(self.0.try_lock(), Err(TryLockError::WouldBlock)) {
                self.1.store(true, Ordering::SeqCst);
            }
        }
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let supervisor = TaskSupervisor::new();
    let later = Arc::new(AtomicBool::new(false));
    let flag = DropFlag(later.clone());
    runtime.block_on(async {
        let spawned = supervisor.spawn("later", Policy::Critical, move |_| async move {
            let _flag = flag;
            pending::<TaskResult>().await
        });
        assert!(spawned.is_ok());
        tokio::task::yield_now().await;
    });

    let lock = Arc::new(Mutex::new(()));
    let under_lock = Arc::new(AtomicBool::new(false));
    let probe = TriesLock(lock.clone(), under_lock.clone());
    let held = lock.lock().unwrap();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::refusal::spawn_set_aside(async move {
            let _probe = probe;
        })
    }));
    drop(held);
    assert!(unwound.is_err(), "there is no runtime to spawn onto");
    assert!(
        !under_lock.load(Ordering::SeqCst),
        "the unwound spawn dropped its refused future while the caller held its lock"
    );

    // The runtime's teardown drops the later task's future on this thread.
    drop(runtime);
    assert!(
        later.load(Ordering::SeqCst),
        "the unwound spawn left its slot open, so a later supervised drop was parked"
    );
}
