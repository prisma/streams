//! The process root's actual `stop_on_exit` transition on Loom primitives:
//! an atomic flag stands in for the runtime's cancellation watch and a Loom
//! mutex for the cause's `OnceLock`. At most three threads, preemption bound
//! 2, 1,000 branches per execution; no duration or permutation cutoff. The
//! supervisor, its tasks, the stop's arming and the deadline thread are not
//! modelled: `exits::tests` drives them.
//!
//! What this does and does not prove (skeptic C12). The "at most one cause"
//! property holds of the `Mutex<Option>` stand-in, which re-implements
//! `OnceLock::set`; production relies on `OnceLock`'s own guarantee. The
//! "a published stop carries its cause" property is the order of the two
//! writes inside `stop_on_exit`; production's `ordered_stop` does not rely
//! on it, because it reads the cause only after `shutdown` has joined the
//! claimant, so its happens-before comes from that join. Nor is a loop that
//! ends through a failed `Weak` upgrade (the scaler, once the router is
//! dropped after `serve_h1`'s cancelled return) modelled: it is a
//! consequence only transitively, through that cancelled return.
//!
//! The consequence rule (an exit that finds a stop requested never claims)
//! is L1's exit sequenced after the signal: a model whose exits only race
//! the signal passes without that rule, since the signalling thread's own
//! request makes "the stop is published" hold either way.
//!
//! The observer of a published stop runs on a thread of its own. Measured
//! with loom 0.7.2: with the observer on the model's main thread, whose read
//! of the flag precedes the exit thread's own read, the checker explored ONE
//! execution and passed with the two writes swapped (its reduction appears
//! to reorder a write only against the object's last access). With the
//! observer spawned, L1 explores 476 executions and L2 36. Controls:
//! swapping the writes fails L2 with "a stop without its cause"; deleting
//! the consequence check from `stop_on_exit` fails L1 with "an exit after
//! the signal claimed the cause" (without its sequenced exit, L1 explored 96
//! executions and passed).
#![cfg(test)]

use super::{CauseCell, StopFlag, stop_on_exit};
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::sync::{Arc, Mutex};

impl StopFlag for AtomicBool {
    fn requested(&self) -> bool {
        self.load(Ordering::SeqCst)
    }
    fn request(&self) {
        self.store(true, Ordering::SeqCst);
    }
}

impl<T> CauseCell<T> for Mutex<Option<T>> {
    fn record(&self, cause: T) -> bool {
        let mut slot = self.lock().unwrap();
        if slot.is_some() {
            return false;
        }
        *slot = Some(cause);
        true
    }
}

fn model() -> loom::model::Builder {
    let mut model = loom::model::Builder::new();
    model.max_threads = 3;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model
}

type Cells = (Arc<AtomicBool>, Arc<Mutex<Option<&'static str>>>);

fn cells() -> Cells {
    (Arc::new(AtomicBool::new(false)), Arc::new(Mutex::new(None)))
}

/// Item 38: two critical exits race a termination signal, and a third exit
/// ends after the signal, on the signalling thread. Whatever the
/// interleaving, at most one exit is the cause, the recorded cause is the
/// exit that claimed it, the stop is published once anything asked, and the
/// exit sequenced after the signal is its consequence: it never claims, even
/// when neither racer has claimed yet.
#[test]
fn quality_loom_one_cause_at_most_and_the_stop_always_published() {
    model().check(|| {
        let (flag, cause) = cells();
        let racers: Vec<_> = ["fleet", "telemetry-drain"]
            .into_iter()
            .map(|name| {
                let (flag, cause) = (flag.clone(), cause.clone());
                loom::thread::spawn(move || (name, stop_on_exit(&*flag, &*cause, name)))
            })
            .collect();
        flag.request();
        assert!(
            !stop_on_exit(&*flag, &*cause, "auth-refresher"),
            "an exit after the signal claimed the cause"
        );
        let claimed: Vec<_> = racers
            .into_iter()
            .map(|racer| racer.join().unwrap())
            .filter(|(_, claimed)| *claimed)
            .map(|(name, _)| name)
            .collect();
        assert!(claimed.len() <= 1, "two causes: {claimed:?}");
        assert_eq!(*cause.lock().unwrap(), claimed.first().copied());
        assert!(flag.requested());
    });
}

/// Whoever sees a stop an exit requested sees its cause: the cause is
/// recorded before the stop is published.
#[test]
fn quality_loom_a_published_exit_stop_carries_its_cause() {
    model().check(|| {
        let (flag, cause) = cells();
        let (f, c) = (flag.clone(), cause.clone());
        let exit = loom::thread::spawn(move || stop_on_exit(&*f, &*c, "fleet"));
        let observer = loom::thread::spawn(move || {
            if flag.requested() {
                assert_eq!(
                    *cause.lock().unwrap(),
                    Some("fleet"),
                    "a stop without its cause"
                );
            }
        });
        observer.join().unwrap();
        assert!(exit.join().unwrap());
    });
}
