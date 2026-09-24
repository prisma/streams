//! The absorbed-boundary retirement under Loom (release hold: the capacity
//! run's one-off 500). The committer runs the actual `retire_absorbed` on
//! an actual `TailFields` under a Loom mutex, behind `absorbed`'s
//! `upto > absorbed` guard, and publishes the durable boundary under a
//! second one after each advance, as `dispatch_durable` does after
//! staging. The absorber's steps are transcribed from `plan_read`
//! (`from = max(mark, durable)`), `raise_lane_marks` and the rescan rule
//! (drop a mark ahead of the durable boundary). Two threads, preemption
//! bound 2, 1,000 branches per execution; no duration or permutation
//! cutoff. Every loop is bounded: the committer thread pops at most the
//! advances the absorber queues, and what it did not reach is staged, and
//! healed, after the join in at most two passes.
#![cfg(test)]

use super::{AbsorbRetirement, CopiedBytes, retire_absorbed};
use crate::shard::TailFields;
use loom::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// Stored bytes of the stream's eight records, all distinct so a double
/// count never cancels out.
const SIZES: [u64; 8] = [11, 13, 17, 19, 23, 29, 31, 37];
const NEXT: u64 = 8;

fn stored(from: u64, upto: u64) -> u64 {
    (0u64..)
        .zip(SIZES)
        .filter(|(offset, _)| (from..upto).contains(offset))
        .map(|(_, bytes)| bytes)
        .sum()
}

type Retire = fn(&mut TailFields, u64, &CopiedBytes) -> AbsorbRetirement;

/// One advance the committer staged: what retiring it did (None for one
/// the guard skipped as not advancing) and the tail it left.
struct Staged {
    outcome: Option<AbsorbRetirement>,
    absorbed: u64,
    ledger: u64,
}

/// The shard as the model sees it: the committer's tail, the published
/// durable boundary and the committer queue.
struct Shard {
    tail: Mutex<TailFields>,
    durable: Mutex<u64>,
    queue: Mutex<VecDeque<(u64, CopiedBytes)>>,
}

impl Shard {
    fn new() -> Self {
        Self {
            tail: Mutex::new(TailFields {
                next: NEXT,
                unabsorbed_bytes: stored(0, NEXT),
                ..Default::default()
            }),
            durable: Mutex::new(0),
            queue: Mutex::new(VecDeque::new()),
        }
    }

    /// `plan_read` then `raise_lane_marks`: copy [max(mark, durable), upto)
    /// and queue it.
    fn gather(&self, mark: &mut u64, upto: u64) {
        let from = (*mark).max(*self.durable.lock().unwrap());
        if from < upto {
            let copied = CopiedBytes::new(from, stored(from, upto));
            self.queue.lock().unwrap().push_back((upto, copied));
            *mark = upto;
        }
    }

    /// The rescan rule: a mark ahead of the durable boundary is dropped.
    fn roll_back(&self, mark: &mut u64) {
        if *mark > *self.durable.lock().unwrap() {
            *mark = 0;
        }
    }

    /// Stage the oldest queued advance, then publish the durable boundary.
    fn commit_one(&self, retire: Retire) -> Option<Staged> {
        let (upto, copied) = self.queue.lock().unwrap().pop_front()?;
        let staged = {
            let mut tail = self.tail.lock().unwrap();
            let outcome = (upto > tail.absorbed).then(|| retire(&mut tail, upto, &copied));
            Staged {
                outcome,
                absorbed: tail.absorbed,
                ledger: tail.unabsorbed_bytes,
            }
        };
        *self.durable.lock().unwrap() = staged.absorbed;
        Some(staged)
    }
}

/// G1 copies [0, 4) and waits in the queue; the rescan rule then runs
/// ungated (today's absorber, and still the eviction prune), and G2
/// copies from `max(mark, durable)` to the end — racing a committer that
/// stages both. Returns every advance staged, in order.
fn rescan_during_an_inflight_advance(retire: Retire) -> (Arc<Shard>, u64, Vec<Staged>) {
    let shard = Arc::new(Shard::new());
    let mut mark = 0;
    shard.gather(&mut mark, 4);
    let committer = shard.clone();
    let thread = loom::thread::spawn(move || {
        (0..2)
            .filter_map(|_| committer.commit_one(retire))
            .collect::<Vec<_>>()
    });
    shard.roll_back(&mut mark);
    shard.gather(&mut mark, NEXT);
    let mut staged = thread.join().unwrap();
    staged.extend((0..2).filter_map(|_| shard.commit_one(retire)));
    (shard, mark, staged)
}

/// The retirement a pre-fix committer ran: whatever `upto > absorbed`
/// admits retires its whole `len`.
fn upto_only(tail: &mut TailFields, upto: u64, copied: &CopiedBytes) -> AbsorbRetirement {
    let Some(remaining) = tail.unabsorbed_bytes.checked_sub(copied.len) else {
        return AbsorbRetirement::Diverged;
    };
    tail.absorbed = upto.min(tail.next);
    tail.unabsorbed_bytes = remaining;
    AbsorbRetirement::Exact
}

fn explore(f: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.max_threads = 2;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(f);
}

/// Whatever the interleaving, no advance diverges, the ledger is exactly
/// the stored bytes of [absorbed, next) after every one, and a stranded
/// remainder heals exactly once the queue has drained.
#[test]
fn quality_loom_an_unconditional_rollback_never_retires_a_byte_twice() {
    explore(|| {
        let (shard, mut mark, staged) = rescan_during_an_inflight_advance(retire_absorbed);
        for step in &staged {
            assert_ne!(step.outcome, Some(AbsorbRetirement::Diverged));
            assert_eq!(
                step.ledger,
                stored(step.absorbed, NEXT),
                "a byte retired twice"
            );
        }
        for _ in 0..2 {
            if shard.tail.lock().unwrap().absorbed == NEXT {
                break;
            }
            shard.roll_back(&mut mark);
            shard.gather(&mut mark, NEXT);
            let healed = shard.commit_one(retire_absorbed).unwrap();
            assert_eq!(healed.outcome, Some(AbsorbRetirement::Exact));
        }
        let tail = shard.tail.lock().unwrap();
        assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (NEXT, 0));
    });
}

/// Non-vacuity: the same race under the pre-fix retirement reaches an
/// interleaving whose regathered overlap outruns the ledger.
#[test]
fn quality_loom_an_upto_only_retirement_counts_a_regathered_overlap_twice() {
    let diverged = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = diverged.clone();
    explore(move || {
        let (_, _, staged) = rescan_during_an_inflight_advance(upto_only);
        if staged
            .iter()
            .any(|step| step.outcome == Some(AbsorbRetirement::Diverged))
        {
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });
    assert!(
        diverged.load(std::sync::atomic::Ordering::SeqCst),
        "no interleaving retired the regathered overlap twice"
    );
}
