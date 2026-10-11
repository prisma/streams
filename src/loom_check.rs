//! A Loom model whose execution fails ends its test as an ordinary failure.
//!
//! loom 0.7.2 cannot unwind out of a model. When an execution panics, the
//! threads it spawned in the step that panicked are still queued, not
//! started; the scheduler drops them while the panic unwinds out of that
//! step, after it has left the execution, so the Loom objects they captured
//! panic again ("cannot access Loom execution state from outside a Loom
//! model", loom-0.7.2 src/rt/scheduler.rs:128). A panic in a destructor
//! during cleanup aborts the test binary: the assertion the model caught
//! reads as a crash, every test still running in that binary is lost, and
//! the mutation leg lists the mutant incomplete (a signal), not caught.
//!
//! `check` explores exactly the executions `Builder::check` explores, in
//! the same order, and runs each execution's body under `catch_unwind`
//! inside the execution, so a panic unwinds only the body, while the
//! execution's state is still there, and the threads it queued then run.
//! It keeps the first failure, lets that execution finish and ends the
//! exploration at the start of the next one, before its body has made
//! anything of Loom's; the test then fails with that first failure. The rest
//! of the failing execution runs on fresh schedule choices: loom replays an
//! execution's prefix from the previous one, which passed, and a model is a
//! function of its schedule, so the failure lies past that prefix (a model
//! that failed on anything else, a count of executions say, would have
//! loom replay a choice into a thread that has ended, and spin). A panic
//! on a spawned model thread, or loom's own (a deadlock, a leak), still
//! unwinds out of `Builder::check`: no model spawns from a spawned thread,
//! so nothing is queued when it does.
#![cfg(test)]

use loom::model::Builder;
use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Explores `body` under `model` as `model.check(body)` does; an execution
/// that fails fails the test with its panic message instead of aborting the
/// test binary.
pub(crate) fn check<F>(model: &Builder, body: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let failure = Arc::new(Mutex::new(None));
    let shared = Arc::clone(&failure);
    let explored = catch_unwind(AssertUnwindSafe(|| {
        model.check(move || execute(&body, &shared));
    }));
    let first = kept(&failure).take();
    if let Some(first) = first {
        panic!("a Loom execution failed: {first}");
    }
    if let Err(panic) = explored {
        resume_unwind(panic);
    }
}

/// One execution: ends the exploration when an earlier one failed, else
/// runs `body` and keeps its panic message if it is the first.
fn execute(body: &impl Fn(), failure: &Mutex<Option<String>>) {
    if kept(failure).is_some() {
        resume_unwind(Box::new("an earlier Loom execution failed"));
    }
    if let Err(panic) = catch_unwind(AssertUnwindSafe(body)) {
        kept(failure).get_or_insert_with(|| message(panic.as_ref()));
    }
}

/// The failure kept so far, also after a holder panicked.
fn kept(failure: &Mutex<Option<String>>) -> MutexGuard<'_, Option<String>> {
    failure.lock().unwrap_or_else(PoisonError::into_inner)
}

fn message(panic: &(dyn Any + Send)) -> String {
    match (panic.downcast_ref::<&str>(), panic.downcast_ref::<String>()) {
        (Some(text), _) => (*text).to_owned(),
        (None, Some(text)) => text.clone(),
        (None, None) => "a panic without a message".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::check;
    use loom::model::Builder;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    fn model() -> Builder {
        let mut model = Builder::new();
        model.max_threads = 3;
        model.preemption_bound = Some(2);
        model
    }

    type Seen = Arc<Mutex<Vec<usize>>>;

    /// The main thread races a writer on a Loom atomic, logs what it saw,
    /// then hands the atomic to a second thread. With `fail`, an execution
    /// in which the writer ran first fails right after that spawn: the
    /// second thread is still queued and holds the atomic, as the racers of
    /// `tasks::exits`' first model were.
    fn race(log: Seen, fail: bool) -> impl Fn() + Send + Sync + 'static {
        move || {
            let flag = loom::sync::Arc::new(loom::sync::atomic::AtomicUsize::new(0));
            let writer = flag.clone();
            let first = loom::thread::spawn(move || writer.store(1, Ordering::SeqCst));
            let seen = flag.load(Ordering::SeqCst);
            log.lock().unwrap().push(seen);
            let late = loom::thread::spawn(move || flag.load(Ordering::SeqCst));
            assert!(!fail || seen == 0, "the writer ran first");
            first.join().unwrap();
            assert!(seen <= late.join().unwrap());
        }
    }

    fn explored(run: impl FnOnce(Seen)) -> Vec<usize> {
        let log = Seen::default();
        run(log.clone());
        log.lock().unwrap().clone()
    }

    /// Exploration is `Builder::check`'s: the same executions, in order.
    #[test]
    fn quality_loom_a_passing_model_explores_what_builder_check_explores() {
        let plain = explored(|log| model().check(race(log, false)));
        let checked = explored(|log| check(&model(), race(log, false)));
        assert!(plain.contains(&0) && plain.contains(&1), "{plain:?}");
        assert_eq!(checked, plain);
    }

    /// The first failing execution fails the test, though a thread it
    /// spawned was still queued when it panicked (`Builder::check` aborts
    /// the test binary there), and no execution's body runs after it.
    #[test]
    fn quality_loom_a_failing_execution_fails_its_test_and_ends_the_exploration() {
        let plain = explored(|log| model().check(race(log, false)));
        let mut failure = None;
        let checked = explored(|log| {
            failure = std::panic::catch_unwind(|| check(&model(), race(log, true))).err();
        });
        let message = failure.expect("an execution in which the writer ran first fails");
        assert_eq!(
            message.downcast_ref::<String>().map(String::as_str),
            Some("a Loom execution failed: the writer ran first")
        );
        let failed = plain.iter().position(|seen| *seen == 1).unwrap();
        assert_eq!(checked, plain[..=failed]);
    }

    /// A spawned thread's failure unwinds out of the exploration as itself.
    #[test]
    #[should_panic(expected = "a spawned thread failed")]
    fn quality_loom_a_failing_spawned_thread_fails_its_test() {
        check(&model(), || {
            let thread = loom::thread::spawn(|| panic!("a spawned thread failed"));
            thread.join().unwrap();
        });
    }
}
