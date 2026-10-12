//! When the idle loop releases, as a pure function of its readings.
#![cfg(test)]

use super::{COLLECT_EVERY, IdlePolicy, QUIET, REFILL_FLOOR, RERELEASE_AFTER, Reading};
use crate::runtime::{Clock, ManualClock};
use std::time::Duration;

const TWO: Duration = Duration::from_secs(2);

/// Readings at chosen offsets from one origin, on a manual clock.
struct Readings {
    clock: ManualClock,
    elapsed: Duration,
}

impl Readings {
    fn new() -> Self {
        Self {
            clock: ManualClock::at(0),
            elapsed: Duration::ZERO,
        }
    }

    /// A quiet reading `at` from the origin, after `arrivals` arrivals.
    fn at(&mut self, at: Duration, arrivals: u64) -> Reading {
        self.clock.advance_monotonic(at - self.elapsed);
        self.elapsed = at;
        Reading {
            now: self.clock.monotonic(),
            arrivals,
            active: 0,
            unabsorbed: 0,
            refilled: 0,
        }
    }
}

/// The policy at the origin, with no arrival yet.
fn policy(readings: &mut Readings) -> IdlePolicy {
    IdlePolicy::new(&readings.at(Duration::ZERO, 0))
}

#[test]
fn nothing_is_released_before_the_quiet_period_and_a_release_comes_at_it() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    assert!(!policy.release_due(&readings.at(QUIET - TWO, 0)));
    assert!(policy.release_due(&readings.at(QUIET, 0)));
    assert!(
        !policy.release_due(&readings.at(QUIET + TWO, 0)),
        "one release per quiet period"
    );
}

#[test]
fn unabsorbed_bytes_hold_the_release_until_the_first_tick_they_are_gone() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    for (at, unabsorbed) in [(QUIET, 1), (QUIET + TWO, 5 << 20)] {
        let reading = Reading {
            unabsorbed,
            ..readings.at(at, 0)
        };
        assert!(!policy.release_due(&reading), "{unabsorbed} bytes owed");
    }
    assert!(policy.release_due(&readings.at(QUIET + 2 * TWO, 0)));
}

#[test]
fn an_active_request_holds_the_release() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    let reading = Reading {
        active: 1,
        ..readings.at(QUIET, 0)
    };
    assert!(!policy.release_due(&reading));
    assert!(policy.release_due(&readings.at(QUIET + TWO, 0)));
}

#[test]
fn a_data_plane_arrival_restarts_the_quiet_period() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    let arrived = Duration::from_secs(60);
    assert!(!policy.release_due(&readings.at(arrived, 1)));
    assert!(!policy.release_due(&readings.at(QUIET, 1)));
    assert!(!policy.release_due(&readings.at(arrived + QUIET - TWO, 1)));
    assert!(policy.release_due(&readings.at(arrived + QUIET, 1)));

    let again = arrived + QUIET + Duration::from_secs(10);
    assert!(!policy.release_due(&readings.at(again, 2)));
    assert!(
        policy.release_due(&readings.at(again + QUIET, 2)),
        "each quiet period releases once, refilled or not"
    );
}

#[test]
fn a_quiet_period_releases_again_only_once_refilled_past_the_floor_after_a_minute() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    assert!(policy.release_due(&readings.at(QUIET, 0)));
    let refilled = |reading: Reading, refilled| Reading {
        refilled,
        ..reading
    };
    let soon = readings.at(QUIET + RERELEASE_AFTER - TWO, 0);
    assert!(
        !policy.release_due(&refilled(soon, REFILL_FLOOR)),
        "too soon"
    );
    let due = readings.at(QUIET + RERELEASE_AFTER, 0);
    assert!(
        !policy.release_due(&refilled(due, REFILL_FLOOR - 1)),
        "below the floor"
    );
    assert!(policy.release_due(&refilled(due, REFILL_FLOOR)));
    let next = readings.at(QUIET + RERELEASE_AFTER + TWO, 0);
    assert!(
        !policy.release_due(&refilled(next, 64 * REFILL_FLOOR)),
        "a minute from the last release, not the first"
    );
}

#[test]
fn each_release_requests_collect_rounds_at_0_30_and_60_seconds() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    let quiet = readings.at(QUIET - TWO, 0);
    assert!(!policy.collect_due(quiet.now), "nothing released yet");
    let mut rounds_at = |policy: &mut IdlePolicy, at: Duration, due: bool| {
        let reading = readings.at(at, 0);
        if at == QUIET {
            assert!(policy.release_due(&reading));
        }
        assert_eq!(policy.collect_due(reading.now), due, "at {at:?}");
    };
    rounds_at(&mut policy, QUIET, true);
    rounds_at(&mut policy, QUIET + TWO, false);
    rounds_at(&mut policy, QUIET + COLLECT_EVERY - TWO, false);
    rounds_at(&mut policy, QUIET + COLLECT_EVERY, true);
    rounds_at(&mut policy, QUIET + COLLECT_EVERY, false);
    rounds_at(&mut policy, QUIET + 2 * COLLECT_EVERY - TWO, false);
    rounds_at(&mut policy, QUIET + 2 * COLLECT_EVERY, true);
    rounds_at(&mut policy, QUIET + 3 * COLLECT_EVERY, false);
    rounds_at(&mut policy, QUIET + 10 * COLLECT_EVERY, false);

    let again = QUIET + 11 * COLLECT_EVERY;
    assert!(!policy.release_due(&readings.at(again, 1)), "an arrival");
    let next = readings.at(again + QUIET, 1);
    assert!(policy.release_due(&next));
    assert!(
        policy.collect_due(next.now),
        "a new release starts new rounds"
    );
}

/// A release's collect rounds end with its quiet period: once requests
/// arrive again, no later round holds the workers that serve them.
#[test]
fn a_request_after_a_release_ends_its_collect_rounds() {
    let mut readings = Readings::new();
    let mut policy = policy(&mut readings);
    let released = readings.at(QUIET, 0);
    assert!(policy.release_due(&released));
    assert!(policy.collect_due(released.now), "round 0 at the release");
    let traffic = [
        (QUIET + TWO, 1),
        (QUIET + COLLECT_EVERY, 5),
        (QUIET + 2 * COLLECT_EVERY, 9),
    ];
    for (at, arrivals) in traffic {
        let reading = readings.at(at, arrivals);
        assert!(!policy.release_due(&reading), "{arrivals} arrivals");
        assert!(
            !policy.collect_due(reading.now),
            "a collect round was requested {:?} into resumed traffic",
            at - QUIET
        );
    }
}
