//! A rolled-back split's backoff (GATE-DECISION-meas149-reshaping-2026-11-16 F1d). A split that
//! rolls back does not move the topology version, so the policy's cooldown anchor (a version
//! change) does not move either, and the next tick started the same split again: meas-149's run
//! parked its shard three times in 30 s. After a rollback the policy starts no split of that prefix
//! for one topology cooldown, then for twice the last wait, up to [`MAX_MS`]; a publish (a new
//! topology version) clears every wait.
//!
//! A rollback reaches the policy two ways: the split entry refuses `rolled_back` (its start
//! failed), or the split started and its actor ended the cell's intent `RolledBack` later, which
//! the policy reads on its next pass (counted once per operation).
//!
//! A merge that rolls back waits the same way (GATE-DECISION-s22-join-2026-11-20 Addendum 17: a
//! source that never settles must not make a merge every pass), under its own key
//! ([`merge_key`]), so a merge's wait and a split's never share a prefix's entry.

use std::collections::BTreeMap;

/// The longest wait before a prefix whose splits keep rolling back is split again.
pub(crate) const MAX_MS: i64 = 10 * 60 * 1_000;

/// The split entry's refusal of a split that rolled back.
const ROLLED_BACK: &str = "rolled_back";

/// The backoff's key for a merge into `parent`: apart from a split's of a leaf of that prefix.
pub(crate) fn merge_key(parent: &str) -> String {
    format!("merge {parent}")
}

/// The waits, per prefix.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    // mt-lint: allow(name-keyed-map): shard prefix (a leaf) -> (its wait, the time it ends)
    waits: BTreeMap<String, (i64, i64)>,
    /// The last rolled-back operation counted from the cell's intent, which every pass reads.
    counted: Option<String>,
}

impl Backoff {
    /// A split of `prefix` rolled back at `now_ms`: no new split of it for one `cooldown_ms`, or
    /// twice its last wait, at most [`MAX_MS`].
    pub(crate) fn rolled_back(&mut self, prefix: &str, now_ms: i64, cooldown_ms: i64) {
        let wait = self
            .waits
            .get(prefix)
            .map_or(cooldown_ms, |(last, _)| last.saturating_mul(2))
            .clamp(1, MAX_MS);
        self.waits
            .insert(prefix.to_owned(), (wait, now_ms.saturating_add(wait)));
    }

    /// The split or merge entry refused the operation under `prefix` (a split's prefix, or a
    /// merge's [`merge_key`]) with `code` at `now_ms`: a `rolled_back` refusal is a rollback
    /// ([`Self::rolled_back`]), any other refusal waits for nothing here.
    pub(crate) fn refused(&mut self, prefix: &str, code: &str, now_ms: i64, cooldown_ms: i64) {
        if code == ROLLED_BACK {
            self.rolled_back(prefix, now_ms, cooldown_ms);
        }
    }

    /// The cell's intent records that operation `op_id`, a split of `prefix` or a merge under its
    /// [`merge_key`], rolled back: [`Self::rolled_back`], once per operation.
    pub(crate) fn intent_rolled_back(
        &mut self,
        op_id: &str,
        prefix: &str,
        now_ms: i64,
        cooldown_ms: i64,
    ) {
        if self.counted.as_deref() == Some(op_id) {
            return;
        }
        self.counted = Some(op_id.to_owned());
        self.rolled_back(prefix, now_ms, cooldown_ms);
    }

    /// Whether the operation under `prefix` (a split's prefix, or a merge's [`merge_key`]) waits at
    /// `now_ms`.
    pub(crate) fn holds(&self, prefix: &str, now_ms: i64) -> bool {
        self.waits
            .get(prefix)
            .is_some_and(|(_, until)| now_ms < *until)
    }

    /// A publish moved the topology version: every wait ends.
    pub(crate) fn published(&mut self) {
        self.waits.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{Backoff, MAX_MS, merge_key};

    /// **A rolled-back merge waits under its own key** (GATE-DECISION-s22-join-2026-11-20
    /// Addendum 17): a merge into `0` that rolled back holds the next merge into `0` for one
    /// cooldown, then twice that, and neither holds a split of a leaf `0` nor is held by one.
    #[test]
    fn a_rolled_back_merge_waits_under_its_own_key() {
        let mut b = Backoff::default();
        let merge = merge_key("0");
        assert_eq!(merge, "merge 0");
        b.rolled_back(&merge, 0, 1_000);
        assert_eq!(
            (b.holds(&merge, 999), b.holds(&merge, 1_000)),
            (true, false)
        );
        assert!(!b.holds("0", 0), "a split of the leaf 0 waits for nothing");
        b.refused(&merge, "rolled_back", 1_000, 1_000);
        assert_eq!(
            (b.holds(&merge, 2_999), b.holds(&merge, 3_000)),
            (true, false)
        );
        b.rolled_back("1", 0, 1_000);
        assert!(
            !b.holds(&merge_key("1"), 0),
            "a split's wait holds no merge"
        );
    }

    /// **Each rollback of a prefix doubles its wait from one cooldown, up to ten minutes, and a
    /// publish ends it.** A cooldown of 1,000 ms: rolled back at 0, the prefix waits until 1,000
    /// (held at 999, not at 1,000); rolled back again at 1,000, until 3,000; then until 7,000.
    /// Another prefix waits for nothing. Waits stop doubling at ten minutes, and a publish clears
    /// every wait.
    #[test]
    fn each_rollback_doubles_the_wait_up_to_ten_minutes_and_a_publish_ends_it() {
        let mut b = Backoff::default();
        assert!(!b.holds("0", 0), "nothing rolled back");
        b.rolled_back("0", 0, 1_000);
        assert_eq!((b.holds("0", 999), b.holds("0", 1_000)), (true, false));
        assert!(!b.holds("1", 0), "another prefix");
        b.rolled_back("0", 1_000, 1_000);
        assert_eq!((b.holds("0", 2_999), b.holds("0", 3_000)), (true, false));
        b.rolled_back("0", 3_000, 1_000);
        assert_eq!((b.holds("0", 6_999), b.holds("0", 7_000)), (true, false));
        for _ in 0..20 {
            b.rolled_back("0", 7_000, 1_000);
        }
        let capped = 7_000 + MAX_MS;
        assert_eq!(
            (b.holds("0", capped - 1), b.holds("0", capped)),
            (true, false)
        );
        b.published();
        assert!(!b.holds("0", 7_000), "a publish ends the wait");
        b.rolled_back("0", 8_000, 1_000);
        assert_eq!(
            (b.holds("0", 8_999), b.holds("0", 9_000)),
            (true, false),
            "from one again"
        );
    }

    /// **The waits are exactly one cooldown, then each twice the last, up to exactly ten
    /// minutes.** A cooldown of 1,000 ms, rolled back at 0 again and again: 1 s, 2 s, 4 s, ...,
    /// 512 s, then 600 s (the cap, 600,000 ms, written out rather than read from [`MAX_MS`]) and
    /// 600 s again. Each wait is observed as held 1 ms before its end and not at it.
    #[test]
    fn the_waits_are_one_cooldown_doubled_up_to_exactly_ten_minutes() {
        let mut b = Backoff::default();
        let mut waits = Vec::new();
        for _ in 0..13 {
            b.rolled_back("0", 0, 1_000);
            let wait = (1..=700_000)
                .find(|t| !b.holds("0", *t))
                .expect("every wait ends within 700 s");
            assert!(b.holds("0", wait - 1), "held until {wait}");
            waits.push(wait);
        }
        assert_eq!(
            waits,
            [
                1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 512_000,
                600_000, 600_000, 600_000
            ]
        );
    }

    /// **Only a `rolled_back` refusal starts a wait.** The split entry's other refusals (an
    /// instance full, a busy cell, a peer's parent) wait for nothing here.
    #[test]
    fn only_a_rolled_back_refusal_starts_a_wait() {
        let mut b = Backoff::default();
        for code in [
            "instance_full",
            "topology_busy",
            "not_ring_owner",
            "unavailable",
        ] {
            b.refused("0", code, 0, 1_000);
            assert!(!b.holds("0", 0), "{code}");
        }
        b.refused("0", "rolled_back", 0, 1_000);
        assert_eq!((b.holds("0", 999), b.holds("0", 1_000)), (true, false));
    }

    /// **A rolled-back intent is counted once.** The policy reads the cell's terminal intent on
    /// every pass: the first read of operation `op-1` starts the wait, later reads of it add
    /// nothing, and the next operation's rollback doubles it.
    #[test]
    fn a_rolled_back_intent_is_counted_once() {
        let mut b = Backoff::default();
        b.intent_rolled_back("op-1", "0", 0, 1_000);
        b.intent_rolled_back("op-1", "0", 500, 1_000);
        b.intent_rolled_back("op-1", "0", 900, 1_000);
        assert_eq!((b.holds("0", 999), b.holds("0", 1_000)), (true, false));
        b.intent_rolled_back("op-2", "0", 1_000, 1_000);
        assert_eq!((b.holds("0", 2_999), b.holds("0", 3_000)), (true, false));
    }
}
