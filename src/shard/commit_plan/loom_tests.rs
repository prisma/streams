//! The absorbed-boundary retirement under Loom (release hold: the capacity
//! run's one-off 500). The committer runs the actual `retire_absorbed` on
//! an actual `TailFields` under a Loom mutex, behind `absorbed`'s
//! `upto > absorbed` guard, publishes the durable boundary under a second
//! one after each advance, as `dispatch_durable` does after staging, and
//! then drops the advance's actual `SubmitReceipt` on Loom atomics, as a
//! dispatched or refused group does. The absorber's steps are transcribed
//! from `plan_reads` (the settled-gated rollback), `plan_read`
//! (`from = max(mark, durable)`), `raise_lane_marks` and the old rescan
//! rule (drop a mark ahead of the durable boundary); it counts each
//! advance with the actual `Submissions::submit`. Two threads, preemption
//! bound 2, 1,000 branches per execution; no duration or permutation
//! cutoff. Every loop is bounded: the committer thread pops at most the
//! advances the absorber queues, and what it did not reach is staged, and
//! healed, after the join in at most two passes.
#![cfg(test)]

use super::{
    AbsorbRetirement, CopiedBytes, SettlementWord, Submissions, SubmitReceipt, retire_absorbed,
};
use crate::shard::TailFields;
use loom::sync::atomic::AtomicU64;
use loom::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

impl SettlementWord for AtomicU64 {
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

/// Two settlement buckets: the modelled stream sits in bucket 0, and a
/// stream in either bucket can keep an advance in flight beside it.
type Settlement = std::sync::Arc<Submissions<AtomicU64, 2>>;
type Receipt = SubmitReceipt<AtomicU64, 2>;
const STREAM: [u8; 16] = [0; 16];
const SAME_BUCKET: [u8; 16] = [2; 16];
const OTHER_BUCKET: [u8; 16] = [1; 16];

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
/// the guard skipped as not advancing, or one refused) and the tail it
/// left.
struct Staged {
    outcome: Option<AbsorbRetirement>,
    absorbed: u64,
    ledger: u64,
}

/// The shard as the model sees it: the committer's tail, the published
/// durable boundary, the committer queue (each advance with the receipt
/// it was submitted under), how many queued advances the committer will
/// refuse, and the absorber's settlement counters.
struct Shard {
    tail: Mutex<TailFields>,
    durable: Mutex<u64>,
    queue: Mutex<VecDeque<(u64, CopiedBytes, Receipt)>>,
    refusals: Mutex<u32>,
    settlement: Settlement,
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
            refusals: Mutex::new(0),
            settlement: Settlement::default(),
        }
    }

    /// `plan_read` then `raise_lane_marks`: copy [max(mark, durable), upto)
    /// and queue it, counted by `submit` first. Returns where it started.
    fn gather(&self, mark: &mut u64, upto: u64) -> Option<u64> {
        let from = (*mark).max(*self.durable.lock().unwrap());
        if from >= upto {
            return None;
        }
        let copied = CopiedBytes::new(from, stored(from, upto));
        let receipt = self.settlement.submit(&STREAM);
        self.queue
            .lock()
            .unwrap()
            .push_back((upto, copied, receipt));
        *mark = upto;
        Some(from)
    }

    /// The stranded-mark rule: a mark ahead of the durable boundary is
    /// dropped. `plan_reads` applies it only to a settled stream; the old
    /// rescan (and still the eviction prune) applied it unconditionally.
    fn roll_back(&self, mark: &mut u64) {
        if *mark > *self.durable.lock().unwrap() {
            *mark = 0;
        }
    }

    /// `plan_reads` then `plan_read`: the rollback runs only when `gated`.
    fn plan(&self, mark: &mut u64, upto: u64, gated: bool) -> Option<u64> {
        if gated {
            self.roll_back(mark);
        }
        self.gather(mark, upto)
    }

    /// Stage the oldest queued advance, then publish the durable boundary,
    /// then drop its receipt. A refused advance drops its receipt without
    /// staging or publishing anything.
    fn commit_one(&self, retire: Retire) -> Option<Staged> {
        let (upto, copied, receipt) = self.queue.lock().unwrap().pop_front()?;
        let refused = {
            let mut refusals = self.refusals.lock().unwrap();
            let refused = *refusals > 0;
            *refusals = refusals.saturating_sub(1);
            refused
        };
        let staged = {
            let mut tail = self.tail.lock().unwrap();
            let advancing = !refused && upto > tail.absorbed;
            let outcome = advancing.then(|| retire(&mut tail, upto, &copied));
            Staged {
                outcome,
                absorbed: tail.absorbed,
                ledger: tail.unabsorbed_bytes,
            }
        };
        if !refused {
            *self.durable.lock().unwrap() = staged.absorbed;
        }
        drop(receipt);
        Some(staged)
    }

    /// No advance diverged, and the ledger was exactly the stored bytes of
    /// [absorbed, next) after every one.
    fn assert_exact(staged: &[Staged]) {
        for step in staged {
            assert_ne!(step.outcome, Some(AbsorbRetirement::Diverged));
            assert_eq!(
                step.ledger,
                stored(step.absorbed, NEXT),
                "a byte retired twice"
            );
        }
    }

    /// Heal passes once the queue has drained and every receipt dropped, at
    /// most two: the settled stream's stranded mark rolls back and the
    /// remainder is gathered and retired exactly.
    fn heal(&self, mark: &mut u64) {
        for _ in 0..2 {
            if self.tail.lock().unwrap().absorbed == NEXT {
                break;
            }
            self.plan(mark, NEXT, self.settlement.settled(&STREAM));
            let healed = self.commit_one(retire_absorbed).unwrap();
            assert_eq!(healed.outcome, Some(AbsorbRetirement::Exact));
        }
        let tail = self.tail.lock().unwrap();
        assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (NEXT, 0));
    }
}

/// G1 copies [0, 4) and waits in the queue; the rescan rule then runs
/// ungated (the old absorber, and still the eviction prune), and G2
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
        Shard::assert_exact(&staged);
        shard.heal(&mut mark);
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

/// One regather race: G1's [0, 4) is queued (the committer refuses the
/// first `refusals` advances) while `beside` holds an advance in flight;
/// G2 then plans to the end with its rollback gated by `gate`, applied to
/// whether the stream's bucket read as settled, racing a committer that
/// stages both.
struct Race {
    shard: Arc<Shard>,
    mark: u64,
    settled_at_plan: bool,
    g2_from: Option<u64>,
    staged: Vec<Staged>,
}

fn regather_race(refusals: u32, beside: [u8; 16], gate: fn(bool) -> bool) -> Race {
    let shard = Arc::new(Shard::new());
    *shard.refusals.lock().unwrap() = refusals;
    let busy = shard.settlement.submit(&beside);
    let mut mark = 0;
    shard.gather(&mut mark, 4);
    let committer = shard.clone();
    let thread = loom::thread::spawn(move || {
        (0..2)
            .filter_map(|_| committer.commit_one(retire_absorbed))
            .collect::<Vec<_>>()
    });
    let settled_at_plan = shard.settlement.settled(&STREAM);
    let g2_from = shard.plan(&mut mark, NEXT, gate(settled_at_plan));
    let mut staged = thread.join().unwrap();
    staged.extend((0..2).filter_map(|_| shard.commit_one(retire_absorbed)));
    drop(busy);
    Race {
        shard,
        mark,
        settled_at_plan,
        g2_from,
        staged,
    }
}

fn settled_gate(settled: bool) -> bool {
    settled
}

fn unconditional_gate(_: bool) -> bool {
    true
}

/// The causal fix: while G1 can still land, its lane mark stays, so G2
/// starts at it and both advances retire exactly. No advance is ever
/// dropped, whatever the interleaving, with another bucket's advance in
/// flight beside it.
#[test]
fn quality_loom_an_unsettled_batch_keeps_its_lane_mark() {
    explore(|| {
        let race = regather_race(0, OTHER_BUCKET, settled_gate);
        Shard::assert_exact(&race.staged);
        for step in &race.staged {
            assert_ne!(step.outcome, Some(AbsorbRetirement::Detached));
        }
        assert_eq!(race.g2_from, Some(4), "G2 regathered below G1's mark");
        let tail = race.shard.tail.lock().unwrap();
        assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (NEXT, 0));
    });
}

/// Non-vacuity: without the settlement gate, some interleaving rolls the
/// in-flight mark back and G2's regather is dropped.
#[test]
fn quality_loom_an_unconditional_rollback_regathers_under_an_unsettled_batch() {
    let detached = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let seen = detached.clone();
    explore(move || {
        let race = regather_race(0, OTHER_BUCKET, unconditional_gate);
        if race
            .staged
            .iter()
            .any(|step| step.outcome == Some(AbsorbRetirement::Detached))
        {
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });
    assert!(
        detached.load(std::sync::atomic::Ordering::SeqCst),
        "no interleaving regathered under an unsettled batch"
    );
}

/// A refused G1 settles with its refusal. When that precedes G2's plan,
/// G2 rolls the mark back and regathers [0, 8) exactly although another
/// bucket's advance is in flight; otherwise G2 starts at the mark and is
/// dropped, and the settled heal regathers from the durable boundary.
/// Never a divergence, and the ledger is exact after every advance.
#[test]
fn quality_loom_a_refused_batch_is_regathered_from_the_durable_boundary() {
    explore(|| {
        let mut race = regather_race(1, OTHER_BUCKET, settled_gate);
        Shard::assert_exact(&race.staged);
        if race.settled_at_plan {
            assert_eq!(
                race.g2_from,
                Some(0),
                "a settled refusal was not rolled back"
            );
            let tail = race.shard.tail.lock().unwrap();
            assert_eq!((tail.absorbed, tail.unabsorbed_bytes), (NEXT, 0));
        }
        race.shard.heal(&mut race.mark);
    });
}

/// A stream sharing its bucket with an advance in flight only waits: the
/// refused G1's mark is never rolled back during the race, and the heal
/// rolls it back once the bucket settles.
#[test]
fn quality_loom_a_shared_bucket_only_delays_a_rollback() {
    explore(|| {
        let mut race = regather_race(1, SAME_BUCKET, settled_gate);
        assert!(!race.settled_at_plan, "a shared bucket read as settled");
        assert_eq!(race.g2_from, Some(4), "a busy bucket rolled a mark back");
        Shard::assert_exact(&race.staged);
        race.shard.heal(&mut race.mark);
    });
}
