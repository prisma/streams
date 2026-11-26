#![cfg(test)]
//! C14's evidence table and R1c's watch (`fleet::dead`).

use std::time::Duration;

use super::{DEAD_MS, Dead};
use crate::fleet::Member;
use crate::fleet::planning::RING_LIVENESS_MS;
use crate::fleet::record::{Candidates, boot_of};

const NOW: i64 = 10_000_000;

fn member(name: &str, boot: &str) -> Member {
    Member {
        name: name.to_string(),
        boot_id: Some(boot.to_string()),
    }
}

/// `streams-1`'s pass over held beats `(name, boot, age_ms, draining)`.
fn reader(beats: &[(&str, &str, i64, bool)]) -> Candidates {
    let held = beats
        .iter()
        .map(|(name, boot, _, _)| ((*name).to_string(), (boot_of(boot), false)))
        .collect();
    let timed = beats
        .iter()
        .map(|(name, _, age, draining)| ((*name).to_string(), (NOW - age, *draining, *draining)))
        .collect();
    Candidates::held(2, "streams-1", held)
        .with_boot("a1")
        .with_beats(timed)
}

/// The first arm, by the listed boot's held beat (Addendum 41 B items 6 and 7): 169 s old, or
/// draining and 30 s old, is dead; a beat a millisecond younger is not; a beat of another boot is
/// no evidence of the first arm. One name a pass, the lowest ordinal; never the reader.
#[tokio::test(start_paused = true, crate = "crate::test_rt")]
async fn a_listed_boots_held_beat_is_dead_only_past_its_bound() {
    let at = (NOW, tokio::time::Instant::now());
    let verdict = |name: &str, boot: &str, age: i64, draining: bool| {
        let listed = [member("streams-1", "a1"), member(name, "b1")];
        let c = reader(&[("streams-1", "a1", age, false), (name, boot, age, draining)]);
        Dead::default().removal(&c, &listed, at)
    };
    let two = Some("streams-2".to_string());
    let table = [
        verdict("streams-2", "b1", DEAD_MS - 1, false),
        verdict("streams-2", "b1", DEAD_MS, false),
        verdict("streams-2", "b1", RING_LIVENESS_MS - 1, true),
        verdict("streams-2", "b1", RING_LIVENESS_MS, true),
        verdict("streams-2", "other", DEAD_MS * 2, true),
        verdict("streams-9", "b1", DEAD_MS, false),
    ];
    assert_eq!(
        table,
        [None, two.clone(), None, two, None, Some("streams-9".into())],
        "beyond the count alike"
    );
    let listed = [
        member("streams-1", "a1"),
        member("streams-3", "c1"),
        member("streams-2", "b1"),
    ];
    let c = reader(&[
        ("streams-1", "a1", DEAD_MS, false),
        ("streams-2", "b1", DEAD_MS, false),
        ("streams-3", "c1", DEAD_MS, false),
    ]);
    let one = Dead::default().removal(&c, &listed, at);
    assert_eq!(
        one.as_deref(),
        Some("streams-2"),
        "one, the lowest ordinal, never itself"
    );
}

/// R1c's second arm on this boot's monotonic clock (Addendum 41 B item 7): a listed boot with no
/// beat of it answered (absent, or another boot's) is removed once 169 s have passed since the
/// first such read, not before; a read that failed in between starts the clock over; a beat of
/// the listed boot answering ends the watch.
#[tokio::test(start_paused = true, crate = "crate::test_rt")]
async fn a_listed_boot_with_no_beat_answered_is_dead_only_after_its_bound_of_answered_reads() {
    let dead = Dead::default();
    let listed = [member("streams-1", "a1"), member("streams-2", "b1")];
    let absent = reader(&[("streams-1", "a1", 0, false)]);
    let other = reader(&[("streams-1", "a1", 0, false), ("streams-2", "b2", 0, false)]);
    let fresh = reader(&[("streams-1", "a1", 0, false), ("streams-2", "b1", 0, false)]);
    let bound = Duration::from_millis(DEAD_MS.cast_unsigned());
    let pass = |c: &Candidates| {
        dead.note_read();
        dead.note_answered();
        dead.removal(c, &listed, (NOW, tokio::time::Instant::now()))
    };
    assert_eq!(pass(&absent), None, "the first read");
    tokio::time::advance(bound - Duration::from_millis(1)).await;
    assert_eq!(pass(&other), None, "another boot's beat answers nothing");
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(pass(&absent).as_deref(), Some("streams-2"), "at the bound");

    assert_eq!(
        pass(&fresh),
        None,
        "a beat of the listed boot ends the watch"
    );
    assert_eq!(pass(&absent), None, "watched afresh");
    tokio::time::advance(bound - Duration::from_millis(1)).await;
    dead.note_read();
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(pass(&absent), None, "a failed read started the clock over");
    tokio::time::advance(bound).await;
    assert_eq!(pass(&absent).as_deref(), Some("streams-2"));
}
