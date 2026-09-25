#![cfg(test)]
//! Item 38: the process root answers for its critical loops, and bounds the
//! stop it is asked for off the executor.

use super::CriticalExit;
use crate::config::{Environment, ProcessEnvironment};
use crate::tasks::{Phase, Policy, TaskOutcome, TaskResult, TaskSupervisor};
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// A process root whose deadline cannot expire inside this suite: its
/// deadline thread sleeps a day, and the test binary's exit ends it.
fn test_root() -> TaskSupervisor {
    TaskSupervisor::new().bounded_root(Duration::from_secs(86_400))
}

/// Waits, bounded, for the stop a critical exit must request.
async fn stop_requested(supervisor: &TaskSupervisor, why: &str) {
    tokio::time::timeout(
        Duration::from_secs(5),
        supervisor.cancellation().cancelled(),
    )
    .await
    .expect(why);
}

/// A loop that ends only when the stop is requested.
fn polite(supervisor: &TaskSupervisor, name: &'static str) {
    supervisor
        .spawn(name, Policy::Critical, |cancel| async move {
            cancel.cancelled().await;
            TaskResult::Done
        })
        .unwrap();
}

/// How a scripted loop ends, and how its join report must read.
type Ending = (fn() -> TaskResult, TaskOutcome);

/// T1. A critical loop's end, however it ends (`Done` included), stops the
/// process root and names the loop, and the join report still carries the
/// same end.
#[tokio::test]
async fn a_critical_exit_stops_the_process_root_and_names_its_cause() {
    let cases: [Ending; 3] = [
        (|| TaskResult::Done, TaskOutcome::Finished),
        (
            || TaskResult::Failed("repository gone".into()),
            TaskOutcome::Failed("repository gone".into()),
        ),
        (
            || panic!("critical panic"),
            TaskOutcome::Panicked("critical panic".into()),
        ),
    ];
    for (end, outcome) in cases {
        let root = test_root();
        let (release, held) = tokio::sync::oneshot::channel();
        root.spawn("fleet", Policy::Critical, |_| async move {
            held.await.unwrap();
            end()
        })
        .unwrap();
        assert_eq!(root.stop_cause(), None);
        release.send(()).unwrap();
        stop_requested(
            &root,
            "a critical exit must request the process root's stop",
        )
        .await;
        let cause = CriticalExit {
            name: "fleet",
            outcome: outcome.clone(),
        };
        assert_eq!(root.stop_cause(), Some(cause.clone()));
        let report = root.shutdown(Duration::from_secs(1)).await;
        assert_eq!(report.outcomes, vec![("fleet", outcome)]);
        assert_eq!(
            root.stop_cause(),
            Some(cause),
            "the cause outlives the stop"
        );
    }
}

/// T2. Only an unrequested critical exit on the process root is a cause: a
/// noncritical end stops nothing, an owner's supervisor keeps its readiness
/// verdict, and a loop ending in answer to a requested stop is no cause.
#[tokio::test]
async fn only_an_unrequested_critical_exit_on_the_process_root_is_a_cause() {
    let root = test_root();
    root.spawn("hygiene", Policy::Noncritical, |_| async {
        TaskResult::Failed("optional".into())
    })
    .unwrap();
    let owned = TaskSupervisor::new();
    owned
        .spawn("committer", Policy::Critical, |_| async {
            TaskResult::Done
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while owned.monitor().critical_failure().is_none()
            || root.monitor().snapshot()[0].state == crate::tasks::TaskState::Running
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !root.cancellation().is_cancelled(),
        "a noncritical end stops nothing"
    );
    assert!(
        !owned.cancellation().is_cancelled(),
        "an owner answers for its loops"
    );
    assert_eq!((root.stop_cause(), owned.stop_cause()), (None, None));
    assert_eq!(
        owned.monitor().unready_reason().as_deref(),
        Some("critical task terminated: committer")
    );
    let requested = test_root();
    polite(&requested, "fleet");
    requested.shutdown_request().request();
    let report = requested.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.finished(), vec!["fleet"]);
    assert_eq!(
        requested.stop_cause(),
        None,
        "a requested stop has no critical cause"
    );
    root.shutdown(Duration::from_secs(1)).await;
    owned.shutdown(Duration::from_secs(1)).await;
}

/// T3. Held interleaving: the first critical exit is the cause; a critical
/// loop that ends because of the stop it requested, and a later request,
/// are its consequences. The join report names both failures.
#[tokio::test]
async fn the_first_critical_exit_is_the_cause_and_later_exits_its_consequences() {
    let root = test_root();
    let (release, held) = tokio::sync::oneshot::channel();
    root.spawn("fleet", Policy::Critical, |_| async move {
        held.await.unwrap();
        TaskResult::Failed("repository gone".into())
    })
    .unwrap();
    root.spawn("telemetry-drain", Policy::Critical, |cancel| async move {
        cancel.cancelled().await;
        TaskResult::Failed("stopped by the first".into())
    })
    .unwrap();
    release.send(()).unwrap();
    stop_requested(&root, "the first exit requests the stop").await;
    root.shutdown_request().request();
    let report = root.shutdown(Duration::from_secs(1)).await;
    assert_eq!(
        report.outcomes,
        vec![
            ("fleet", TaskOutcome::Failed("repository gone".into())),
            (
                "telemetry-drain",
                TaskOutcome::Failed("stopped by the first".into())
            ),
        ]
    );
    assert_eq!(
        report.failed(),
        vec![
            ("fleet", "repository gone"),
            ("telemetry-drain", "stopped by the first")
        ],
        "the ordered stop's join log names every failed loop and why"
    );
    assert_eq!(
        root.stop_cause(),
        Some(CriticalExit {
            name: "fleet",
            outcome: TaskOutcome::Failed("repository gone".into())
        })
    );
}

/// T4. The process's ordered stop: loops joined, then the resources, then
/// the verdict. A requested stop reports the resources' own result; a
/// critical exit fails the stop with its cause, which outranks a resource
/// failure.
#[tokio::test]
async fn the_ordered_stop_fails_with_the_critical_cause_after_the_whole_sequence() {
    let requested = test_root();
    polite(&requested, "fleet");
    requested.cancel();
    requested
        .ordered_stop(Duration::from_secs(1), std::future::ready(Ok(())))
        .await
        .unwrap();
    assert_eq!(requested.monitor().phase(), Some(Phase::Stopped));
    let refused = test_root();
    refused.cancel();
    assert_eq!(
        refused
            .ordered_stop(
                Duration::from_secs(1),
                std::future::ready(Err("close failed".into()))
            )
            .await,
        Err("close failed".to_string())
    );
    let failed = test_root();
    failed
        .spawn("fleet", Policy::Critical, |_| async {
            TaskResult::Failed("repository gone".into())
        })
        .unwrap();
    stop_requested(&failed, "the critical exit requests the stop").await;
    let stopped = failed
        .ordered_stop(
            Duration::from_secs(1),
            std::future::ready(Err("close failed".into())),
        )
        .await;
    assert_eq!(
        stopped,
        Err(
            "critical task fleet exited while the runtime was running: Failed(\"repository gone\")"
                .to_string()
        )
    );
    assert_eq!(
        failed.monitor().phase(),
        Some(Phase::Stopped),
        "the loops were joined first"
    );
}

/// T5. End to end through the real accept loop: under the process root a
/// critical loop's exit ends `serve_h1`, and the ordered stop then fails
/// naming the loop.
#[tokio::test]
async fn a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop() {
    let tasks = test_root();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tasks
        .spawn("fleet", Policy::Critical, |_| async { TaskResult::Done })
        .unwrap();
    let served = tokio::time::timeout(
        Duration::from_secs(5),
        crate::http::serve_h1(
            listener,
            axum::Router::new(),
            &crate::config::HttpConfig::default(),
            tasks.clone(),
        ),
    )
    .await
    .expect("a critical exit must end the accept loop");
    assert!(served.is_ok());
    let stopped = tokio::time::timeout(
        Duration::from_secs(5),
        tasks.ordered_stop(Duration::from_secs(1), std::future::ready(Ok(()))),
    )
    .await
    .expect("the ordered stop is bounded by its grace");
    assert_eq!(
        stopped,
        Err("critical task fleet exited while the runtime was running: Finished".to_string())
    );
}

const HELPER_MARKER: &str = "STREAMS_STOP_DEADLINE_HELPER";
const HELPER_TEST: &str = "tasks::exits::tests::stop_deadline_helper";
const ESCALATED: &str = "process root stopped";
const WEDGED: &str = "executor wedged";
/// The bounded root's escalation, logged with its cause (name and outcome).
const CAUSE_LOGGED: &str = "critical task fleet exited while the runtime was running: \
     Failed(\"gone\"); requesting the ordered stop, bounded at 200ms";
/// The deadline's own line, with the stop's cause: a signal's request here.
const EXPIRED: &str = "stop deadline: a stop was requested 200ms ago and has not finished \
     (cause: a termination signal requested it); exiting";
/// The deadline's line after a critical exit names that exit.
const EXPIRED_CAUSE: &str = "(cause: critical task fleet exited while the runtime was running: \
     Failed(\"gone\")); exiting";

/// Whether the parent keeps reading the helper's stderr, closes its read
/// end at once so that every later write to it fails (EPIPE), or keeps it
/// open, unread and full, so that every later write to it blocks.
#[derive(PartialEq)]
enum Stderr {
    Read,
    Closed,
    Full,
}

/// Kills and reaps the helper on every exit path.
struct Helper(Child);

impl Drop for Helper {
    fn drop(&mut self) {
        drop(self.0.kill());
        drop(self.0.wait());
    }
}

fn drained(helper: &mut Helper) -> String {
    let mut text = String::new();
    for pipe in [
        helper.0.stdout.take().map(|p| Box::new(p) as Box<dyn Read>),
        helper.0.stderr.take().map(|p| Box::new(p) as Box<dyn Read>),
    ]
    .into_iter()
    .flatten()
    {
        let mut pipe = pipe;
        drop(pipe.read_to_string(&mut text));
    }
    text
}

/// Runs `stop_deadline_helper` in a child process, whose stop is armed the
/// way `how` names, and waits up to 20 s for it to exit. A helper still
/// running then is killed BEFORE its pipes are drained: it holds their
/// write ends, so a drain would never return (skeptic C1).
async fn helper_exit(how: &str, stderr: Stderr) -> (Option<i32>, String) {
    let exe = std::env::current_exe().expect("test binary path");
    // Full: `yes` fills the pipe, then blocks while `reader` stays open
    // and unread, so every later write to it blocks too.
    let (reader, writer) = std::io::pipe().expect("a pipe for the helper's stderr");
    let (child_stderr, _filler) = if stderr == Stderr::Full {
        let filler = Command::new("yes")
            .stdout(Stdio::from(
                writer.try_clone().expect("the pipe's write end"),
            ))
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the pipe's filler");
        (Stdio::from(writer), Some(Helper(filler)))
    } else {
        (Stdio::piped(), None)
    };
    let child = Command::new(exe)
        .args([HELPER_TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env_clear()
        .env(HELPER_MARKER, how)
        .stdout(Stdio::piped())
        .stderr(child_stderr)
        .spawn()
        .expect("spawn the stop-deadline helper");
    let mut helper = Helper(child);
    if stderr == Stderr::Closed {
        drop(helper.0.stderr.take());
    }
    let exited = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(status) = helper.0.try_wait().expect("poll the helper") {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if exited.is_err() {
        drop(helper.0.kill());
        drop(helper.0.wait());
    }
    drop(reader);
    let transcript = drained(&mut helper);
    let status =
        exited.unwrap_or_else(|_| panic!("the deadline never ended the helper:\n{transcript}"));
    (status.code(), transcript)
}

/// T6. The bound runs in a CHILD process, since it ends its process: the
/// binary's own root answers a critical exit, and a root whose only executor
/// thread is then blocked for good still exits 1 at its deadline. Its
/// escalation logs the cause, name and outcome, before the executor wedges.
#[tokio::test]
async fn the_process_root_bounds_its_stop_off_the_executor() {
    let (code, transcript) = helper_exit("critical", Stderr::Read).await;
    assert_eq!(code, Some(1), "{transcript}");
    assert!(transcript.contains(ESCALATED), "{transcript}");
    assert!(transcript.contains(CAUSE_LOGGED), "{transcript}");
    assert!(transcript.contains(WEDGED), "{transcript}");
    assert!(transcript.contains(EXPIRED_CAUSE), "{transcript}");
}

/// T7. Owner decision D1: a termination signal's request arms the same
/// bound, so a requested stop whose executor is blocked for good also exits
/// 1 at its deadline.
#[tokio::test]
async fn a_requested_stop_of_the_process_root_is_bounded_off_the_executor() {
    let (code, transcript) = helper_exit("request", Stderr::Read).await;
    assert_eq!(code, Some(1), "{transcript}");
    assert!(!transcript.contains(ESCALATED), "{transcript}");
    assert!(transcript.contains(WEDGED), "{transcript}");
    assert!(transcript.contains(EXPIRED), "{transcript}");
}

/// T10. The bound does not depend on its message: with the helper's stderr
/// closed, the deadline's write fails (EPIPE) and the process still exits 1
/// at its deadline instead of losing the bound with the write.
#[tokio::test]
async fn the_stop_deadline_exits_even_when_stderr_is_closed() {
    let (code, transcript) = helper_exit("request", Stderr::Closed).await;
    assert_eq!(code, Some(1), "{transcript}");
    assert!(transcript.contains(WEDGED), "{transcript}");
}

/// T11. Nor does it wait for a message that cannot be written: with the
/// helper's stderr a full pipe nobody reads, the deadline's write blocks,
/// and the process still exits 1 shortly after its deadline.
#[tokio::test]
async fn the_stop_deadline_exits_even_when_stderr_is_full() {
    let (code, transcript) = helper_exit("request", Stderr::Full).await;
    assert_eq!(code, Some(1), "{transcript}");
    assert!(transcript.contains(WEDGED), "{transcript}");
}

/// Subject of `helper_exit`; inert unless the parent set the marker. With
/// "critical", the binary's own root first answers a critical exit, then a
/// 200 ms root stops for one; with "request", a 200 ms root is asked to stop
/// as the signal task asks. Either way the only executor thread then blocks.
/// The log goes to stdout, as the binary's does.
#[tokio::test]
async fn stop_deadline_helper() {
    let Some(how) = ProcessEnvironment.get(HELPER_MARKER) else {
        return;
    };
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .finish(),
    );
    let wedged = TaskSupervisor::new().bounded_root(Duration::from_millis(200));
    if how == "request" {
        wedged.shutdown_request().request();
    } else {
        let binary = TaskSupervisor::new().process_root();
        binary
            .spawn("fleet", Policy::Critical, |_| async { TaskResult::Done })
            .unwrap();
        stop_requested(&binary, "the binary's own root must answer a critical exit").await;
        println!("{ESCALATED}");
        wedged
            .spawn("fleet", Policy::Critical, |_| async {
                TaskResult::Failed("gone".into())
            })
            .unwrap();
    }
    stop_requested(&wedged, "the bounded root's stop must be requested").await;
    println!("{WEDGED}");
    let (_held, blocked) = std::sync::mpsc::channel::<()>();
    let never = blocked.recv();
    panic!("the stop deadline must end the process while its executor is blocked: {never:?}");
}
