//! The admission pin's actual `AdmissionCounters` transitions on Loom
//! atomics, under a Loom mutex standing in for the tracker lock. Two
//! threads, preemption bound 2, 1,000 branches per execution; no duration
//! or permutation cutoff. The map itself and `admit`'s rate bucket are
//! not modelled: `quota::tests` parks a real admit between its lookup and
//! its charge.
#![cfg(test)]

use super::{AdmissionCounters, CounterWord};
use loom::sync::atomic::AtomicU64;
use loom::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;

impl CounterWord for AtomicU64 {
    fn load(&self, order: Ordering) -> u64 {
        AtomicU64::load(self, order)
    }
    fn fetch_add(&self, value: u64, order: Ordering) -> u64 {
        AtomicU64::fetch_add(self, value, order)
    }
    fn fetch_sub(&self, value: u64, order: Ordering) -> u64 {
        AtomicU64::fetch_sub(self, value, order)
    }
}

type Entry = Arc<AdmissionCounters<AtomicU64>>;

/// External review §9: an admission pins its entry under the tracker
/// lock and charges it after the lock drops; its request keeps the pin; a
/// sweep evicts, under the lock, only an entry nothing holds. Whatever the
/// interleaving, an admission that found its entry still finds it tracked
/// once it has charged, and its release then leaves the entry idle.
#[test]
fn quality_loom_a_sweep_never_evicts_an_entry_mid_admission() {
    let mut model = loom::model::Builder::new();
    model.max_threads = 2;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(|| {
        let entry: Entry = Arc::new(AdmissionCounters {
            admitting: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
        });
        let tracker = Arc::new(Mutex::new(Some(entry)));
        let sweeper = tracker.clone();
        let sweep = loom::thread::spawn(move || {
            let mut tracked = sweeper.lock().unwrap();
            if tracked.as_ref().is_some_and(|e| !e.active()) {
                *tracked = None;
            }
        });
        let found = tracker.lock().unwrap().as_ref().map(|e| {
            e.pin();
            e.clone()
        });
        if let Some(entry) = found {
            entry.charge();
            sweep.join().unwrap();
            assert!(
                tracker
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|e| Arc::ptr_eq(e, &entry)),
                "a sweep evicted an entry whose admission had pinned it"
            );
            entry.discharge();
            entry.unpin();
            assert!(!entry.active(), "a released entry is left in use");
        } else {
            sweep.join().unwrap();
        }
    });
}
