#![cfg(test)]
//! **A wait for a planned move names why it gives up** (F6's `MoveWait`;
//! GATE-DECISION-s22-join-2026-11-20 Addendum 28). The handoff DSTs wait for one of the giver's
//! planned moves to land. The wait gives up as soon as the move cannot land, rather than at
//! [`BOUND`], and says why.

use super::super::fleet_birth::{Pair, age_the_children, split_with_a_held_append};
use super::BOUND;
use crate::fleet::Attempts;
use crate::fleet::handoff::Outcome;
use std::time::Duration;
use tokio::time::Instant;

/// Planned moves that ended (`Moved` or `Kept`) without the waited state, past which the wait
/// gives up: a CAS lost to another writer ends one move `Kept`; three in a row do not happen in
/// these rigs.
const ENDED: u32 = 3;

/// Planned moves that did not start, past which the wait gives up (Addendum 28): twice the most
/// an idle host showed in one wait (4, #181's 40 traces), and at 2 to 4 s a miss about what
/// [`BOUND`] holds.
const NOT_STARTED: u64 = 8;

/// A wait for one of `giver`'s planned moves, which fails as soon as the move cannot land
/// rather than at [`BOUND`], and says why: a giver whose fleet runs no handoff runner moves
/// nothing; a giver that has ended [`ENDED`] planned moves since the wait began, without the
/// waited state, hands off but lands nothing; and one whose [`NOT_STARTED`] moves did not start
/// (a park or a flush past its bound, a claim refused) names how many and the last one's step.
/// Every attempt the wait sees end is printed (`MOVEWAIT`). Without it, every handoff DST waited
/// out its 30 s bound under a runner or a commit that never lands, and together they outlasted the
/// mutation leg's limit; and one whose moves never started gave up at that bound saying only what
/// it waited for (#181's CI red). [`BOUND`] stays the backstop.
pub(super) struct MoveWait {
    t0: Instant,
    seen: Option<Instant>,
    ended: u32,
    /// The giver's attempts as the last poll saw them, and those that had not started when the
    /// wait began.
    attempts: Attempts,
    not_started_before: u64,
    /// The step the last attempt seen not starting stopped at.
    step: &'static str,
    /// How long the wait may take: [`BOUND`], or a test's own derived bound.
    bound: Duration,
}

impl MoveWait {
    pub(super) fn begin(giver: &crate::http::AppState) -> Self {
        Self::begin_within(giver, BOUND)
    }

    /// A wait bounded by `bound` rather than [`BOUND`].
    pub(super) fn begin_within(giver: &crate::http::AppState, bound: Duration) -> Self {
        let standing = giver.fleet.standing();
        let seen = standing.last_handoff().map(|(_, at)| at);
        let attempts = standing.attempts();
        Self {
            t0: Instant::now(),
            seen,
            ended: 0,
            attempts,
            not_started_before: attempts.not_started,
            step: "none",
            bound,
        }
    }

    /// One poll of the wait for `what`, which has not happened yet: it panics with its
    /// [`Self::verdict`].
    pub(super) fn pending(&mut self, giver: &crate::http::AppState, what: &str) {
        if let Some(failed) = self.verdict(giver, what) {
            panic!("{failed}");
        }
    }

    /// One poll of the wait for `what`: why it gives up now, or `None` while the move may land.
    fn verdict(&mut self, giver: &crate::http::AppState, what: &str) -> Option<String> {
        let standing = giver.fleet.standing();
        if !standing.runs_handoffs() {
            return Some(format!("{what}: the giver runs no handoff runner"));
        }
        let last = standing.last_handoff().map(|(_, at)| at);
        if last.is_some() && last != self.seen {
            self.seen = last;
            self.ended += 1;
        }
        if self.ended >= ENDED {
            return Some(format!(
                "{what}: {} planned moves ended, none landed",
                self.ended
            ));
        }
        let not_started = self.saw(standing.attempts(), what);
        if not_started >= NOT_STARTED {
            let step = self.step;
            return Some(format!(
                "{what}: {not_started} planned moves did not start, the last at step={step}"
            ));
        }
        (self.t0.elapsed() >= self.bound).then(|| what.to_string())
    }

    /// The giver's `attempts` now: each one ended since the last poll printed, and how many have
    /// not started since the wait began.
    fn saw(&mut self, attempts: Attempts, what: &str) -> u64 {
        if attempts.ended != self.attempts.ended
            && let Some((end, step)) = attempts.last
        {
            let new = attempts.ended - self.attempts.ended;
            eprintln!("MOVEWAIT {what}: {new} attempt(s) ended, the last {end:?} at step={step}");
            if end == Outcome::NotStarted {
                self.step = step;
            }
        }
        self.attempts = attempts;
        attempts.not_started - self.not_started_before
    }
}

/// **A wait names the planned moves that did not start** (Addendum 28, red first). T5's cell, its
/// children aged past the return window, with every flush under the park failing its bound (the
/// failpoint `Standing::miss_parked_flushes`): return-home's move of a child never starts. The
/// wait gives up at the eighth such move, inside its 30 s backstop, and names the step. Before, it
/// saw only the moves that ended, and gave up at the backstop with nothing but what it waited for
/// (#181's CI red, run 45041068006).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_wait_names_the_moves_that_did_not_start() {
    let pair = Pair::new().await;
    let (status, _) = split_with_a_held_append(&pair).await;
    assert_eq!(status, 204, "the held append");
    let a = &pair.a.state;
    a.fleet.standing().miss_parked_flushes(u64::MAX);
    age_the_children(a).await;
    let mut wait = MoveWait::begin(a);
    let failed = loop {
        if let Some(failed) = wait.verdict(a, "a child returns home") {
            break failed;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        failed,
        "a child returns home: 8 planned moves did not start, the last at step=flush"
    );
    assert!(wait.t0.elapsed() < BOUND, "inside the backstop");
    pair.shutdown().await;
}
