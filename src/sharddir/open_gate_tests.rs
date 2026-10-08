//! `OpenGate`'s decisions, one per test: readiness, the holdoff at both of
//! its checks, a deadlined open and its reaper, a stale close, a stopped
//! gate, an engine refused when its open lands, and the unready watchdog's
//! spawn. Each test names the `cargo mutants` mutants of `src/sharddir.rs`
//! it kills (the nightly rotation of 2026-10-07 left them alive: the
//! `sharddir::` filter selected only the gate's success, failure and
//! retirement paths).
#![cfg(test)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use super::holdoff::Departure;
use super::{
    EngineIncarnation, HOLDOFF_BASE, HOLDOFF_CAP, OpenFn, OpenGate, OpenOutcome, PrefixGate,
    Retirement, arm_holdoff_locked, holdoff_for, spawn_unready_watchdog, unready_exit_after,
};
use crate::config::ShardRuntimeConfig;
use crate::runtime::{Clock, ManualClock};
use crate::shard::ShardEngine;
use crate::shard_directory::{OpenTiming, RetirementReason, ShardDirectory};
use crate::tasks::{Policy, TaskOutcome, TaskState, TaskStatus, TaskSupervisor};

const PREFIX: &str = "0";
/// Bounded: an open a test expects to finish fails by assertion, never hangs.
const OPEN_WAIT: Duration = Duration::from_secs(30);
const OPEN_DEADLINE: Duration = Duration::from_secs(60);
/// The deadline of the tests whose open never finishes on its own.
const SHORT_DEADLINE: Duration = Duration::from_millis(200);

/// What the opener does on each attempt.
#[derive(Clone, Copy)]
enum Script {
    /// A real engine at once.
    Open,
    /// A real engine once the rig releases it.
    Held,
    /// The first `n` attempts fail with "store unavailable"; later ones open.
    FailFirst(usize),
    /// A real engine that is already closing when its open lands.
    ClosedOnArrival,
}

/// A real engine over an in-memory store (`unwind::tests` keeps the same shape).
async fn open_engine(prefix: &str) -> Arc<ShardEngine> {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = slatedb::Db::builder(prefix, store.clone())
        .build()
        .await
        .expect("open db");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    ShardEngine::start(
        prefix.to_string(),
        Arc::new(db),
        store,
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        maintenance,
    )
}

async fn scripted_open(
    prefix: String,
    script: Script,
    attempt: usize,
    mut released: watch::Receiver<bool>,
) -> anyhow::Result<Arc<ShardEngine>> {
    if released.wait_for(|open| *open).await.is_err() {
        anyhow::bail!("the rig dropped its release");
    }
    match script {
        Script::FailFirst(n) if attempt < n => anyhow::bail!("store unavailable"),
        Script::ClosedOnArrival => {
            let engine = open_engine(&prefix).await;
            engine.begin_close();
            Ok(engine)
        }
        Script::Open | Script::Held | Script::FailFirst(_) => Ok(open_engine(&prefix).await),
    }
}

fn scripted_opener(
    script: Script,
    attempts: Arc<AtomicUsize>,
    released: watch::Receiver<bool>,
) -> OpenFn {
    Box::new(move |prefix: String, _incarnation: EngineIncarnation| {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(scripted_open(prefix, script, attempt, released.clone()))
    })
}

/// A gate over a scripted opener, with its attempt count and its release.
struct Rig {
    gate: OpenGate,
    attempts: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
}

impl Rig {
    fn new(script: Script, open_deadline: Duration) -> Self {
        let attempts = Arc::new(AtomicUsize::new(0));
        let (release, released) = watch::channel(!matches!(script, Script::Held));
        let opener = scripted_opener(script, attempts.clone(), released);
        let gate = OpenGate::new(Arc::new(RwLock::new(HashMap::new())), opener, open_deadline);
        Rig {
            gate,
            attempts,
            release,
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

/// An outcome as one comparable value.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Ready,
    Wait(&'static str, u64),
    Failed(String),
}

fn verdict(outcome: OpenOutcome) -> Verdict {
    match outcome {
        OpenOutcome::Ready(_) => Verdict::Ready,
        OpenOutcome::Wait {
            code,
            retry_after_secs,
        } => Verdict::Wait(code, retry_after_secs),
        OpenOutcome::Failed(error) => Verdict::Failed(error),
    }
}

fn ready(outcome: OpenOutcome) -> Arc<ShardEngine> {
    match outcome {
        OpenOutcome::Ready(engine) => engine,
        other => panic!("expected Ready, got {:?}", verdict(other)),
    }
}

/// The gate's open counters (`stats_json`) as one exact value.
#[derive(Debug, Default, PartialEq, Eq)]
struct Opens {
    started: u64,
    completed: u64,
    failed: u64,
    coalesced: u64,
    in_flight: i64,
    deadlined: u64,
    reaped: u64,
}

fn opens(gate: &OpenGate) -> Opens {
    let o = &gate.inner.opens;
    Opens {
        started: o.started.load(Ordering::Relaxed),
        completed: o.completed.load(Ordering::Relaxed),
        failed: o.failed.load(Ordering::Relaxed),
        coalesced: o.coalesced.load(Ordering::Relaxed),
        in_flight: o.in_flight.load(Ordering::Relaxed),
        deadlined: o.deadlined.load(Ordering::Relaxed),
        reaped: o.reaped.load(Ordering::Relaxed),
    }
}

/// The prefix's anti-flap ledger: strikes and the holdoff left.
fn ledger(gate: &OpenGate, prefix: &str) -> (u32, Option<Duration>) {
    let st = gate.inner.st.lock().unwrap();
    let g = st.get(prefix).expect("gate state for the prefix");
    let left = g
        .holdoff_until
        .map(|until| until.saturating_duration_since(Instant::now()));
    (g.strikes, left)
}

/// The holdoff's clock is `std::time`: moving its deadline back by the cap
/// stands in for the holdoff running out, and leaves the strikes alone.
fn run_out_holdoff(gate: &OpenGate, prefix: &str) {
    let mut st = gate.inner.st.lock().unwrap();
    let g = st.get_mut(prefix).expect("gate state for the prefix");
    let until = g.holdoff_until.expect("an armed holdoff");
    g.holdoff_until = Some(until - HOLDOFF_CAP);
}

/// Polls `done` every 5 ms; fails by assertion after 10 s.
async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..2_000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{what}: not within 10 s");
}

/// Kills `unready_exit_after -> Duration with Default::default()`: the window
/// is the configured seconds, and zero (the off switch) stays zero.
#[test]
fn the_unready_exit_window_is_the_configured_seconds() {
    let cfg = |unready_exit_after_secs| ShardRuntimeConfig {
        unready_exit_after_secs,
        ..ShardRuntimeConfig::default()
    };
    assert_eq!(unready_exit_after(&cfg(300)), Duration::from_secs(300));
    assert_eq!(unready_exit_after(&cfg(7)), Duration::from_secs(7));
    assert_eq!(unready_exit_after(&cfg(0)), Duration::ZERO);
}

/// Kills `spawn_unready_watchdog with ()`: an enabled watchdog is one
/// critical task named `unready-watchdog` that ends when its supervisor
/// stops; a zero window spawns nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_unready_watchdog_is_one_critical_task_and_only_when_enabled() {
    let (_release, released) = watch::channel(true);
    let opener = scripted_opener(Script::Open, Arc::new(AtomicUsize::new(0)), released);
    let directory = ShardDirectory::new(
        vec![PREFIX.to_string()],
        crate::ownership::OwnershipService::new(""),
        OpenTiming {
            open_deadline: OPEN_DEADLINE,
            open_wait: Duration::from_millis(50),
        },
        |_notifier| opener,
    );
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::at(0));
    let cfg = |unready_exit_after_secs| ShardRuntimeConfig {
        unready_exit_after_secs,
        ..ShardRuntimeConfig::default()
    };

    let disabled = TaskSupervisor::new();
    spawn_unready_watchdog(&cfg(0), clock.clone(), &disabled, directory.clone());
    assert_eq!(
        disabled.monitor().snapshot(),
        vec![],
        "0 disables the watchdog"
    );

    let enabled = TaskSupervisor::new();
    spawn_unready_watchdog(&cfg(300), clock, &enabled, directory);
    assert_eq!(
        enabled.monitor().snapshot(),
        vec![TaskStatus {
            name: "unready-watchdog",
            policy: Policy::Critical,
            state: TaskState::Running,
        }]
    );
    let report = enabled.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        report.outcomes,
        vec![("unready-watchdog", TaskOutcome::Finished)]
    );
    assert_eq!(report.aborted, Vec::<&str>::new(), "it obeys cancellation");
}

/// Kills the three replacements of `OpenGate::unready_reason` (`None`,
/// `Some(String::new())`, `Some("xyzzy".into())`): a gate is ready until three
/// distinct prefixes failed to open while none ever opened; then it names the
/// count and the last error; the first open that succeeds makes it ready.
#[tokio::test(flavor = "current_thread")]
async fn readiness_names_three_failed_prefixes_until_one_opens() {
    let rig = Rig::new(Script::FailFirst(3), OPEN_DEADLINE);
    assert_eq!(rig.gate.unready_reason(), None, "a fresh gate is ready");
    let failed = Verdict::Failed("store unavailable".into());
    for prefix in ["a", "b"] {
        assert_eq!(
            verdict(rig.gate.get_or_open(prefix, OPEN_WAIT).await),
            failed
        );
    }
    assert_eq!(
        rig.gate.unready_reason(),
        None,
        "two failed prefixes are no verdict"
    );
    assert_eq!(verdict(rig.gate.get_or_open("c", OPEN_WAIT).await), failed);
    assert_eq!(
        rig.gate.unready_reason().as_deref(),
        Some(
            "no shard has ever opened (3 distinct shards failed); last error: c: store unavailable"
        )
    );
    let engine = ready(rig.gate.get_or_open("d", OPEN_WAIT).await);
    assert_eq!(
        rig.gate.unready_reason(),
        None,
        "an open that succeeds heals readiness"
    );
    engine.begin_close();
}

/// Kills `+ with -` in `get_or_open`'s deadline arm, both `|| with &&` in its
/// closing check (`|| g.reaping`, `|| g.closing...`), and in
/// `shutdown_pending` both `(vec![], 0)` and `|| with &&`: an open past its
/// deadline fails its caller, strikes and arms the escalated holdoff, and its
/// reaper owns the prefix: a stop counts it as a pending open, and no second
/// open starts while it runs, even once the holdoff is gone.
#[tokio::test(flavor = "current_thread")]
async fn a_deadlined_open_strikes_and_its_reaper_holds_the_prefix() {
    let rig = Rig::new(Script::Held, SHORT_DEADLINE);
    assert_eq!(
        verdict(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await),
        Verdict::Failed("shard open exceeded 200ms".into())
    );
    let expected = Opens {
        started: 1,
        failed: 1,
        deadlined: 1,
        ..Opens::default()
    };
    assert_eq!(opens(&rig.gate), expected);
    let (strikes, left) = ledger(&rig.gate, PREFIX);
    assert_eq!(strikes, 1, "a deadlined open is a strike");
    let left = left.expect("a deadlined open arms the holdoff");
    // 6 s minus microseconds: a > 3 s stall between two adjacent statements
    // would be needed to read it at or below the base.
    assert!(
        left > HOLDOFF_BASE && left <= holdoff_for(1),
        "the escalated holdoff, got {left:?}"
    );
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!(
        (engines.len(), pending_opens),
        (0, 1),
        "the reaper's open is pending"
    );
    match verdict(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await) {
        Verdict::Wait("shard_moving", secs) => assert!((3..=5).contains(&secs), "{secs}"),
        other => panic!("expected the holdoff, got {other:?}"),
    }

    rig.gate.clear_holdoff(PREFIX);
    assert_eq!(
        verdict(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await),
        Verdict::Wait("shard_closing", 1),
        "the reaper still owns the prefix"
    );
    assert_eq!(rig.attempts(), 1, "no second open while the reaper runs");
    assert_eq!(opens(&rig.gate), expected);
}

/// Kills `shutdown_pending -> (vec![], 0)` from the engine side: the reaper
/// closes the engine a deadlined open produces late, a stop owes that close,
/// and the prefix reopens, as a new incarnation, only after it terminated.
#[tokio::test(flavor = "current_thread")]
async fn the_reaper_closes_a_late_engine_and_the_prefix_reopens_after_it() {
    let rig = Rig::new(Script::Held, SHORT_DEADLINE);
    assert_eq!(
        verdict(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await),
        Verdict::Failed("shard open exceeded 200ms".into())
    );
    rig.gate.clear_holdoff(PREFIX);
    rig.release.send_replace(true);
    eventually("the reaper closes the late engine", || {
        opens(&rig.gate).reaped == 1
    })
    .await;
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!(
        (engines.len(), pending_opens),
        (1, 0),
        "the late engine's close is owed, its open is not"
    );

    let engine = ready(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await);
    assert!(
        engines[0].terminated(),
        "the reopen waited for the late engine"
    );
    assert_eq!(
        rig.gate.resident_incarnation(PREFIX),
        Some(EngineIncarnation(1))
    );
    assert_eq!(
        opens(&rig.gate),
        Opens {
            started: 2,
            completed: 1,
            failed: 1,
            deadlined: 1,
            reaped: 1,
            ..Opens::default()
        }
    );
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!((engines.len(), pending_opens), (0, 0));
    engine.begin_close();
}

/// Kills `> with <` in `wait_retired` and `< with >` in `get_or_open`: a
/// holdoff that has run out turns no caller away at either check, and the
/// next caller opens at once.
#[tokio::test(flavor = "current_thread")]
async fn a_holdoff_that_ran_out_admits_the_next_open() {
    let rig = Rig::new(Script::FailFirst(1), OPEN_DEADLINE);
    assert_eq!(
        verdict(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await),
        Verdict::Failed("store unavailable".into())
    );
    assert_eq!(ledger(&rig.gate, PREFIX).0, 1, "a failed open is a strike");
    run_out_holdoff(&rig.gate, PREFIX);

    let engine = ready(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await);
    assert_eq!(
        rig.gate.resident_incarnation(PREFIX),
        Some(EngineIncarnation(1))
    );
    assert_eq!(
        opens(&rig.gate),
        Opens {
            started: 2,
            completed: 1,
            failed: 1,
            ..Opens::default()
        }
    );
    assert_eq!(
        ledger(&rig.gate, PREFIX),
        (1, None),
        "the open clears the holdoff; only a long life resets the strikes"
    );
    engine.begin_close();
}

/// Kills `> with ==` and `> with <` in `wait_retired`: an armed holdoff answers
/// `shard_moving` at once, before the caller would wait on the engine the
/// retirement left closing.
#[tokio::test(flavor = "current_thread")]
async fn an_armed_holdoff_answers_before_any_wait_on_the_retired_engine() {
    let rig = Rig::new(Script::Open, OPEN_DEADLINE);
    let engine = ready(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await);
    let retired = rig
        .gate
        .retire_resident(PREFIX, RetirementReason::Shutdown, |_, _| true);
    assert!(matches!(retired, Retirement::Retired(_)));
    // The retired engine's close has not begun: a caller that waited on it
    // would wait out its patience and leave with `shard_closing`.
    match verdict(
        rig.gate
            .get_or_open(PREFIX, Duration::from_millis(100))
            .await,
    ) {
        Verdict::Wait("shard_moving", secs) => assert!((1..=2).contains(&secs), "{secs}"),
        other => panic!("expected the base holdoff, got {other:?}"),
    }
    assert_eq!(rig.attempts(), 1);
    engine.begin_close();
}

/// Kills `< with ==` and `< with >` in `get_or_open`'s holdoff check: a holdoff
/// armed while a caller waited on the retiring engine still turns that caller
/// away once the engine terminated, and the caller starts no open.
#[tokio::test(flavor = "current_thread")]
async fn a_holdoff_armed_while_a_caller_waits_on_the_retiring_engine_turns_it_away() {
    let rig = Rig::new(Script::Open, OPEN_DEADLINE);
    let engine = ready(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await);
    let retired = rig
        .gate
        .retire_resident(PREFIX, RetirementReason::Shutdown, |_, _| true);
    assert!(matches!(retired, Retirement::Retired(_)));
    rig.gate.clear_holdoff(PREFIX);

    let mut caller = Box::pin(rig.gate.get_or_open(PREFIX, OPEN_WAIT));
    // One poll parks the caller on the retired engine's termination: its close
    // has not begun, and that wait is the only await before the decision.
    let parked = caller
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    assert!(
        parked.is_pending(),
        "the caller waits on the retiring engine"
    );
    // A departure judged meanwhile arms the holdoff, as `notify_closed` does.
    arm_holdoff_locked(
        &mut rig.gate.inner.st.lock().unwrap(),
        PREFIX,
        Departure::Died,
    );
    engine.begin_close();

    let outcome = tokio::time::timeout(OPEN_WAIT, caller)
        .await
        .expect("the caller answers once the engine terminated");
    match verdict(outcome) {
        Verdict::Wait("shard_moving", secs) => assert!((3..=5).contains(&secs), "{secs}"),
        other => panic!("expected the holdoff armed meanwhile, got {other:?}"),
    }
    assert!(engine.termination_complete());
    assert_eq!(rig.attempts(), 1, "the caller started no open");
    assert_eq!(rig.gate.resident_incarnation(PREFIX), None);
}

/// Kills the match guard `r.incarnation == incarnation` -> `true` in
/// `notify_closed`: the late close of a replaced incarnation evicts nothing
/// and arms nothing; the replacement keeps serving.
#[tokio::test(flavor = "current_thread")]
async fn a_late_close_of_a_replaced_incarnation_leaves_the_replacement_serving() {
    let rig = Rig::new(Script::Open, OPEN_DEADLINE);
    ready(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await);
    let first = EngineIncarnation(0);
    assert_eq!(rig.gate.resident_incarnation(PREFIX), Some(first));
    assert!(
        rig.gate.notify_closed(PREFIX, first),
        "the live incarnation's close evicts it"
    );
    rig.gate.clear_holdoff(PREFIX);
    let replacement = ready(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await);
    let second = EngineIncarnation(1);
    assert_eq!(rig.gate.resident_incarnation(PREFIX), Some(second));

    assert!(
        !rig.gate.notify_closed(PREFIX, first),
        "a late close of the replaced incarnation is stale"
    );
    assert_eq!(rig.gate.resident_incarnation(PREFIX), Some(second));
    assert!(!replacement.is_closed(), "the replacement was not closed");
    assert_eq!(
        ledger(&rig.gate, PREFIX),
        (0, None),
        "the stale close armed nothing"
    );
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!((engines.len(), pending_opens), (0, 0));
    replacement.begin_close();
}

/// Kills `OpenGate::stop with ()`, `|| with &&` in `publish_open`, and in
/// `shutdown_pending` both `(vec![], 0)` and `|| with &&`: an open in flight
/// is pending; a stopped gate starts no open; the open that lands after the
/// stop is refused, closed and owed to the stop, never installed.
#[tokio::test(flavor = "current_thread")]
async fn a_stopped_gate_starts_no_open_and_refuses_the_one_in_flight() {
    let rig = Rig::new(Script::Held, OPEN_DEADLINE);
    let patience = Duration::from_millis(20);
    assert_eq!(
        verdict(rig.gate.get_or_open(PREFIX, patience).await),
        Verdict::Wait("shard_opening", 3)
    );
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!(
        (engines.len(), pending_opens),
        (0, 1),
        "the open in flight is pending"
    );

    rig.gate.stop();
    for prefix in [PREFIX, "1"] {
        assert_eq!(
            verdict(rig.gate.get_or_open(prefix, patience).await),
            Verdict::Wait("shard_closing", 1),
            "{prefix}: a stopped gate turns every caller away"
        );
    }
    assert_eq!(rig.attempts(), 1, "a stopped gate starts no open");

    rig.release.send_replace(true);
    eventually("the open in flight lands", || {
        rig.gate.shutdown_pending().1 == 0
    })
    .await;
    assert_eq!(
        rig.gate.resident_incarnation(PREFIX),
        None,
        "refused, not installed"
    );
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!(
        (engines.len(), pending_opens),
        (1, 0),
        "its close is owed to the stop"
    );
    assert_eq!(engines[0].wait(OPEN_WAIT).await, Ok(()), "and completes");
    assert_eq!(
        opens(&rig.gate),
        Opens {
            started: 1,
            ..Opens::default()
        }
    );
}

/// Kills `|| with &&` in `publish_open` from its other side: an engine that
/// closed while its open ran is refused and closed, never served, and a stop
/// owes its close.
#[tokio::test(flavor = "current_thread")]
async fn an_engine_closed_before_its_open_lands_is_refused() {
    let rig = Rig::new(Script::ClosedOnArrival, OPEN_DEADLINE);
    assert_eq!(
        verdict(rig.gate.get_or_open(PREFIX, OPEN_WAIT).await),
        Verdict::Failed("engine closed or directory stopped during open".into())
    );
    assert_eq!(
        rig.gate.resident_incarnation(PREFIX),
        None,
        "refused, not installed"
    );
    let (engines, pending_opens) = rig.gate.shutdown_pending();
    assert_eq!(
        (engines.len(), pending_opens),
        (1, 0),
        "its close is owed to a stop"
    );
    assert_eq!(engines[0].wait(OPEN_WAIT).await, Ok(()));
    assert_eq!(
        opens(&rig.gate),
        Opens {
            started: 1,
            ..Opens::default()
        }
    );
}

/// Kills `< with <=`, `< with ==` and `< with >` in `PrefixGate::holdoff_verdict`,
/// the one holdoff check `get_or_open` and `wait_retired` both ask, and the
/// body's `None`: at an explicit instant the holdoff runs strictly before its
/// deadline, answering `shard_moving` with the whole seconds left and at
/// least one, and has run out AT the deadline. Before the shared verdict the
/// two checks read `Instant::now()` themselves, so no test could land on the
/// deadline: `< with <=` in `get_or_open` and `> with >=` in `wait_retired`
/// survived the nightly rotation of 2026-10-07.
#[test]
fn the_holdoff_turns_callers_away_until_its_deadline_and_not_at_it() {
    let base = Instant::now();
    let until = base + Duration::from_millis(2_500);
    let gate = PrefixGate {
        holdoff_until: Some(until),
        ..PrefixGate::default()
    };
    let at = |ms: u64| {
        gate.holdoff_verdict(base + Duration::from_millis(ms))
            .map(verdict)
    };
    let moving = |secs| Some(Verdict::Wait("shard_moving", secs));
    assert_eq!(
        [0, 1_499, 1_500, 2_499, 2_500, 2_501].map(at),
        [moving(2), moving(1), moving(1), moving(1), None, None],
        "2.5 s before, 1.001 s, 1 s, 1 ms, at the deadline, and after it"
    );
    assert_eq!(
        PrefixGate::default().holdoff_verdict(base).map(verdict),
        None,
        "no holdoff armed"
    );
}
