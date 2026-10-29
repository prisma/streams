#![cfg(test)]
//! The dispatch watchdog's look, on synthetic instants, and its production
//! thresholds.

use std::time::{Duration, Instant};

use super::{Verdict, Watch, dispatch_look};
use crate::shard::seal::{DISPATCH_STALL, DISPATCH_WARN, SealSettings};

fn limits() -> SealSettings {
    SealSettings {
        dispatch_warn: Duration::from_secs(10),
        dispatch_stall: Duration::from_secs(60),
        ..SealSettings::default()
    }
}

#[test]
fn production_warns_at_ten_seconds_and_retires_at_sixty() {
    assert_eq!(DISPATCH_WARN, Duration::from_secs(10));
    assert_eq!(DISPATCH_STALL, Duration::from_secs(60));
}

#[test]
fn one_group_with_no_dispatch_is_warned_once_then_stalled() {
    let s = limits();
    let t0 = Instant::now();
    let at = |secs: u64, ms: u64| t0 + Duration::from_secs(secs) + Duration::from_millis(ms);
    let (w, v) = dispatch_look(None, Some(7), 3, t0, &s);
    assert_eq!(v, Verdict::Quiet, "first seen");
    let (w, v) = dispatch_look(w, Some(7), 3, at(9, 999), &s);
    assert_eq!(v, Verdict::Quiet, "short of the warning");
    let (w, v) = dispatch_look(w, Some(7), 3, at(10, 0), &s);
    assert_eq!(v, Verdict::Warn, "warned at the threshold");
    let (w, v) = dispatch_look(w, Some(7), 3, at(30, 0), &s);
    assert_eq!(v, Verdict::Quiet, "warned once per episode");
    let (w, v) = dispatch_look(w, Some(7), 3, at(59, 999), &s);
    assert_eq!(v, Verdict::Quiet, "short of the stall");
    let (_, v) = dispatch_look(w, Some(7), 3, at(60, 0), &s);
    assert_eq!(v, Verdict::Stalled);
}

#[test]
fn any_progress_starts_the_episode_over() {
    let s = limits();
    let t0 = Instant::now();
    let late = t0 + Duration::from_secs(120);
    let seen = Some(Watch {
        seq: 7,
        since: t0,
        attempts: 3,
        warned: true,
    });
    let fresh = |seq, attempts| {
        Some(Watch {
            seq,
            since: late,
            attempts,
            warned: false,
        })
    };
    assert_eq!(
        dispatch_look(seen, Some(8), 3, late, &s),
        (fresh(8, 3), Verdict::Quiet),
        "another group: dispatch moved on"
    );
    assert_eq!(
        dispatch_look(seen, Some(7), 4, late, &s),
        (fresh(7, 4), Verdict::Quiet),
        "a dispatch was attempted"
    );
    assert_eq!(
        dispatch_look(seen, None, 3, late, &s),
        (None, Verdict::Quiet),
        "nothing due, or the gate held"
    );
}
