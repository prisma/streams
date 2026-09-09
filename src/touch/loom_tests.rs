//! Actual registration and retirement transitions under an instrumented mutex.
//! Two worker threads, preemption bound 2, at most 1,000 branches per execution;
//! no duration or permutation cutoff. Tokio delivery is checked after both joins,
//! not claimed as Loom-instrumented channel implementation.
#![cfg(test)]

use super::{Inner, WaitOutcome, WaitRegistration, WakeReason};
use loom::sync::{Arc, Mutex};

#[test]
fn quality_loom_retirement_cannot_leave_a_late_registered_waiter() {
    let mut model = loom::model::Builder::new();
    model.max_threads = 3;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(|| {
        let state = Arc::new(Mutex::new(Inner::default()));
        let waiter = state.clone();
        let waiter =
            loom::thread::spawn(move || waiter.lock().unwrap().register("epoch", "now", vec![7]));
        let retire = state.clone();
        let retire = loom::thread::spawn(move || retire.lock().unwrap().close());
        let registration = waiter.join().unwrap();
        retire.join().unwrap();
        match registration {
            WaitRegistration::Ready(outcome) => {
                assert!(matches!(outcome, WaitOutcome::Stale { .. }))
            }
            WaitRegistration::Pending(mut receiver) => {
                assert!(matches!(receiver.try_recv(), Ok(WakeReason::Closed)))
            }
        }
        let state = state.lock().unwrap();
        assert!(state.closed);
        assert!(state.waiters.is_empty());
        assert!(state.key_index.is_empty());
    });
}
