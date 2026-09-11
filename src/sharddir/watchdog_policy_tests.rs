//! The unready watchdog's expiry policy under wall-clock jumps.
#![cfg(test)]
use super::*;
use crate::runtime::{Clock, ManualClock};

const LIMIT: Duration = Duration::from_secs(300);

/// A large FORWARD wall-clock jump does not expire the watchdog:
/// only monotonic elapsed time counts.
#[test]
fn forward_wall_jump_does_not_expire() {
    let clock = ManualClock::at(0);
    let mut w = UnreadyWindow::default();
    assert!(matches!(
        w.observe(true, clock.monotonic(), LIMIT),
        WatchdogDecision::Waiting { .. }
    ));
    clock.jump_wall(3_600_000);
    assert_eq!(
        w.observe(true, clock.monotonic(), LIMIT),
        WatchdogDecision::Waiting {
            elapsed: Duration::ZERO
        }
    );
}

/// A BACKWARD wall-clock jump does not postpone expiry.
#[test]
fn backward_wall_jump_does_not_postpone_expiry() {
    let clock = ManualClock::at(0);
    let mut w = UnreadyWindow::default();
    w.observe(true, clock.monotonic(), LIMIT);
    clock.jump_wall(-86_400_000);
    clock.advance_monotonic(LIMIT);
    assert_eq!(
        w.observe(true, clock.monotonic(), LIMIT),
        WatchdogDecision::Expired { elapsed: LIMIT }
    );
}

/// Expiry lands EXACTLY when monotonic elapsed reaches the limit.
#[test]
fn expires_exactly_at_monotonic_limit() {
    let clock = ManualClock::at(0);
    let mut w = UnreadyWindow::default();
    w.observe(true, clock.monotonic(), LIMIT);
    clock.advance_monotonic(LIMIT - Duration::from_millis(1));
    assert!(matches!(
        w.observe(true, clock.monotonic(), LIMIT),
        WatchdogDecision::Waiting { .. }
    ));
    clock.advance_monotonic(Duration::from_millis(1));
    assert_eq!(
        w.observe(true, clock.monotonic(), LIMIT),
        WatchdogDecision::Expired { elapsed: LIMIT }
    );
}

/// Returning to ready clears the active window, and a later
/// unready period begins a FRESH window (elapsed restarts at zero).
#[test]
fn ready_clears_and_later_unready_starts_fresh() {
    let clock = ManualClock::at(0);
    let mut w = UnreadyWindow::default();
    w.observe(true, clock.monotonic(), LIMIT);
    clock.advance_monotonic(Duration::from_secs(200));
    assert_eq!(
        w.observe(false, clock.monotonic(), LIMIT),
        WatchdogDecision::Healthy
    );
    clock.advance_monotonic(Duration::from_secs(200));
    assert_eq!(
        w.observe(true, clock.monotonic(), LIMIT),
        WatchdogDecision::Waiting {
            elapsed: Duration::ZERO
        },
        "a fresh window, not 400s of accumulated unreadiness"
    );
}
