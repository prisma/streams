//! The overrides document's version as this instance last published it, and when a peer's
//! version may make it read the document again (F6 step 8, D7; GATE-DECISION-f6-prep-rows-rev2
//! "Addendum 2026-11-17: relayed 409 refresh", R1-R3).
//!
//! A forward, or an owner's `409` relayed back, carries the sender's version
//! (`Streams-Overrides-Version`). When it differs from this instance's, this instance reads the
//! document once, so the target of a planned move learns it owns the shard from the move's first
//! request. Versions have no order, so the reads that learn nothing are bounded:
//! - an empty version (a sender that has read no document yet) triggers nothing (R3);
//! - a version that still differed after a read is remembered and triggers nothing again;
//! - after a read that left this instance's version unchanged, no header-triggered read starts
//!   for [`FLOOR`] (R2), whatever the version: at most ten unproductive reads a second.
//!
//! The fleet tick's and a planned move's own reads are not bounded here.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// How long a header-triggered read that learnt nothing holds the next one off (R2).
pub(super) const FLOOR: Duration = Duration::from_millis(100);

/// The version state behind [`Tags`]' lock.
#[derive(Debug, Default)]
struct State {
    /// The version this instance last published.
    tag: String,
    /// A peer's version that still differed after a read.
    stale: Option<String>,
    /// No header-triggered read starts before it (R2).
    floor: Option<Instant>,
    /// Header-triggered reads made (tests count them).
    reads: u64,
}

/// The overrides version this instance holds, and its header-triggered reads.
#[derive(Debug, Default)]
pub(crate) struct Tags(Mutex<State>);

impl Tags {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// This instance published the document at version `tag`.
    pub(super) fn set(&self, tag: String) {
        self.state().tag = tag;
    }

    /// The version this instance last published.
    pub(crate) fn tag(&self) -> String {
        self.state().tag.clone()
    }

    /// Whether a peer's version `seen` should make this instance read the document at `now`
    /// (R1 asks it again under the revalidation lock).
    pub(super) fn reads_for(&self, seen: &str, now: Instant) -> bool {
        let state = self.state();
        !seen.is_empty()
            && state.tag != seen
            && state.stale.as_deref() != Some(seen)
            && state.floor.is_none_or(|floor| now >= floor)
    }

    /// A header-triggered read for `seen` ended at `now` with `read` (`None`: it failed), when this
    /// instance held `before`. A read that left the version unchanged sets the floor (R2); a
    /// version it did not bring is remembered.
    pub(super) fn read_for(&self, seen: &str, before: &str, read: Option<&str>, now: Instant) {
        let mut state = self.state();
        state.reads += 1;
        if read.unwrap_or(before) == before {
            state.floor = Some(now + FLOOR);
        }
        if read != Some(seen) {
            state.stale = Some(seen.to_string());
        }
    }

    /// Tests: header-triggered reads made so far.
    #[cfg(test)]
    pub(crate) fn reads(&self) -> u64 {
        self.state().reads
    }
}

#[cfg(test)]
mod tests {
    use super::{FLOOR, Tags};
    use tokio::time::Instant;

    #[test]
    fn an_empty_or_held_version_reads_nothing_and_another_reads_once() {
        let tags = Tags::default();
        tags.set("v1".into());
        let now = Instant::now();
        assert!(!tags.reads_for("", now), "R3: an empty version triggers nothing");
        assert!(!tags.reads_for("v1", now), "the held version triggers nothing");
        assert!(tags.reads_for("v2", now), "another version reads");
        tags.read_for("v2", "v1", Some("v2"), now);
        tags.set("v2".into());
        assert!(!tags.reads_for("v2", now), "once read, it is held");
        assert!(tags.reads_for("v3", now), "a read that brought a version sets no floor");
    }

    #[test]
    fn a_read_that_learns_nothing_holds_every_version_off_for_the_floor() {
        let tags = Tags::default();
        tags.set("v1".into());
        let now = Instant::now();
        tags.read_for("stale-a", "v1", Some("v1"), now);
        assert!(!tags.reads_for("stale-a", now + FLOOR * 2), "a version that still differed");
        assert!(!tags.reads_for("stale-b", now), "R2: the floor holds another version off");
        assert!(tags.reads_for("stale-b", now + FLOOR), "and lets it through after the floor");
        tags.read_for("stale-b", "v1", None, now + FLOOR);
        assert!(!tags.reads_for("stale-a", now + FLOOR), "a failed read sets the floor too");
        assert_eq!(tags.reads(), 2);
    }
}
