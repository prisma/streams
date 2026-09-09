#![cfg(test)]

use super::{Phase, Policy, SpawnRejected, TaskOutcome, TaskResult, TaskSupervisor};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
