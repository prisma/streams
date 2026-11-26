//! Dead names (C14; GATE-DECISION-tla049-r1-r3-2026-11-20 R1c; GATE-DECISION-s22-join-2026-11-20
//! Addendum 41 B items 6 and 7). With the interim's removal half gone (the leave), a listed name
//! leaves the document only by its own release or on evidence that it is dead, never by the count.
//!
//! A member's pass removes at most one listed name, in the CAS of its pins, within the count or
//! beyond it alike, on either arm of the evidence, judged against the **listed** boot:
//! - **a held beat of that boot** older than `PROGRESS_DEADLINE` plus `RING_LIVENESS` (169 s), or
//!   draining and older than `RING_LIVENESS` (Q2: every such removal is of a stale beat);
//! - **no beat of that boot answered** (absent, or another boot's) on every read for 169 s, on
//!   this boot's own monotonic clock from its first such read (R1c's second arm: a boot stamped
//!   and killed before its first beat). A read that failed restarts the clock.
//!
//! Never an unread beat, never a fresh beat of the listed boot, never the count. A member removed
//! while alive finds itself unlisted at its next pass with no release of its own: warned, counted
//! `removed_while_listed`, its engines yield with the view without it, and it is a joiner.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::Member;
use super::planning::{PROGRESS_DEADLINE_MS, RING_LIVENESS_MS};
use super::record::{Candidates, boot_of, ordinal_key};

/// How old a held beat of the listed boot, or how long no beat of it answered, before its name is
/// removed: `PROGRESS_DEADLINE` plus `RING_LIVENESS`, 169 s.
pub(super) const DEAD_MS: i64 = PROGRESS_DEADLINE_MS.cast_signed() + RING_LIVENESS_MS;

/// A listed name and boot with no beat of that boot answered, and this boot's first such read.
type Watched = ((String, u64), tokio::time::Instant);

/// R1c's watch and the reads it rests on, and the removals this boot suffered while alive.
#[derive(Debug, Default)]
pub(crate) struct Dead {
    /// Each listed name and boot with no beat of that boot answered, and this boot's first such
    /// read of it.
    unanswered: Mutex<Vec<Watched>>,
    /// The tick's heartbeat reads begun and answered, and the gap between them at the last pass.
    reads: AtomicU64,
    answered: AtomicU64,
    unanswered_reads: AtomicU64,
    removed_while_listed: AtomicU64,
}

impl Dead {
    /// The tick's heartbeat read has begun (`FleetRepository::read_heartbeat_set`).
    pub(crate) fn note_read(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
    }

    /// The tick's heartbeat read answered whole.
    pub(crate) fn note_answered(&self) {
        self.answered.fetch_add(1, Ordering::SeqCst);
    }

    /// This boot was removed while it was a decided member, with no release of its own.
    pub(super) fn note_removed_while_listed(&self) {
        self.removed_while_listed.fetch_add(1, Ordering::Relaxed);
    }

    /// Tests: the removals this boot suffered while alive (`removed_while_listed`).
    #[cfg(test)]
    pub(crate) fn removed_while_listed(&self) -> u64 {
        self.removed_while_listed.load(Ordering::Relaxed)
    }

    /// The one name of `listed` this pass may remove, in ordinal order, or None; `now_ms` is the
    /// reader's wall clock the beats' ages are judged on (as the ring's are), `now` its monotonic
    /// clock for R1c's watch.
    pub(super) fn removal(
        &self,
        candidates: &Candidates,
        listed: &[Member],
        (now_ms, now): (i64, tokio::time::Instant),
    ) -> Option<String> {
        let Ok(mut watch) = self.unanswered.lock() else {
            return None;
        };
        let gap = self.reads.load(Ordering::SeqCst) - self.answered.load(Ordering::SeqCst);
        if self.unanswered_reads.swap(gap, Ordering::SeqCst) != gap {
            // A read failed since the last pass: R1c's clock starts over.
            watch.clear();
        }
        let mut dead = Vec::new();
        let mut kept = Vec::new();
        let others = listed
            .iter()
            .filter(|member| member.name != candidates.me());
        for member in others {
            let listed_boot = member.boot_id.as_deref().map(boot_of);
            let held = candidates.timed_beat(&member.name);
            let of_listed = held.filter(|(boot, _, _)| listed_boot.is_none_or(|b| b == *boot));
            let gone = match of_listed {
                Some((_, ts_ms, draining)) => stale(now_ms - ts_ms, draining),
                None => {
                    let key = (member.name.clone(), listed_boot.unwrap_or(0));
                    let first = watched(&watch, &key).unwrap_or(now);
                    kept.push((key, first));
                    now.duration_since(first) >= dead_for()
                }
            };
            if gone {
                dead.push(member.name.clone());
            }
        }
        *watch = kept;
        dead.into_iter()
            .min_by(|a, b| ordinal_key(a).cmp(&ordinal_key(b)))
    }
}

/// Whether a held beat of the listed boot `age_ms` old is evidence of death (the first arm).
fn stale(age_ms: i64, draining: bool) -> bool {
    age_ms >= DEAD_MS || (draining && age_ms >= RING_LIVENESS_MS)
}

/// R1c's bound on this boot's own clock.
fn dead_for() -> Duration {
    Duration::from_millis(DEAD_MS.cast_unsigned())
}

/// When this boot first read `key` with no beat of its boot answered, if it is watched.
fn watched(watch: &[Watched], key: &(String, u64)) -> Option<tokio::time::Instant> {
    watch
        .iter()
        .find(|(seen, _)| seen == key)
        .map(|(_, first)| *first)
}

#[cfg(test)]
mod tests;
