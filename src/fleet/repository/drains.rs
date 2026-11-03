//! The drain marker, `fleet/drains.json` (item 8): which runtimes have begun
//! a planned drain, by instance and boot.
//!
//! The fleet tick reads its peers' heartbeats on one tick in
//! `members::PEER_READ_TICKS`, but a planned drain waits until every peer's
//! published view has read one of its draining beats (`fleet::drain`), and
//! its bound counts one tick period for that read. A drain therefore names
//! itself here as it begins, beside its first draining beat, and every tick
//! revalidates this document: unchanged, it answers 304, which the store
//! does not bill. A tick that finds a runtime named here reads that
//! runtime's heartbeat at every tick until it has read a draining beat of
//! the named boot, so the drain's bound is unchanged. An entry names a boot:
//! one whose instance now beats under another boot is stale and is ignored,
//! and whether a member drains is only ever its own heartbeat's word.
//!
//! The document is created empty as the runtime joins, so a tick never pays
//! a billed 404 for it. An entry older than `KEEP_MS` is dropped by the next
//! announcement, so the document stays small whatever the fleet's history.
use serde::{Deserialize, Serialize};

/// An entry older than this is dropped by the next announcement: far past
/// any drain's bound.
const KEEP_MS: i64 = 3_600_000;
/// The entries an announcement keeps, newest first.
const MAX_ENTRIES: usize = 256;

/// The drain marker document.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Drains {
    #[serde(default)]
    pub(crate) entries: Vec<Announced>,
}

/// One runtime that began a planned drain.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Announced {
    pub(crate) instance: String,
    pub(crate) boot_id: String,
    /// When the drain began (the announcer's clock).
    pub(crate) at_ms: i64,
}

impl Drains {
    /// This document with `entry` announced: any earlier entry of its
    /// instance replaced, entries older than `KEEP_MS` before `now_ms`
    /// dropped, and at most `MAX_ENTRIES` kept, newest first.
    pub(crate) fn announcing(mut self, entry: Announced, now_ms: i64) -> Drains {
        self.entries
            .retain(|kept| kept.instance != entry.instance && now_ms - kept.at_ms <= KEEP_MS);
        self.entries.push(entry);
        self.entries
            .sort_by_key(|entry| std::cmp::Reverse(entry.at_ms));
        self.entries.truncate(MAX_ENTRIES);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{Announced, Drains, KEEP_MS, MAX_ENTRIES};

    fn entry(instance: &str, boot: &str, at_ms: i64) -> Announced {
        Announced {
            instance: instance.into(),
            boot_id: boot.into(),
            at_ms,
        }
    }

    #[test]
    fn an_announcement_replaces_its_instance_and_drops_old_entries() {
        let doc = Drains {
            entries: vec![entry("streams-2", "a", 10), entry("streams-3", "b", 0)],
        };
        let now = KEEP_MS + 5;
        let doc = doc.announcing(entry("streams-2", "c", now), now);
        assert_eq!(
            doc.entries,
            [entry("streams-2", "c", now)],
            "streams-2's earlier entry is replaced; streams-3's is older than the keep window"
        );
        let doc = doc.announcing(entry("streams-4", "d", now - 1), now);
        assert_eq!(
            doc.entries,
            [
                entry("streams-2", "c", now),
                entry("streams-4", "d", now - 1)
            ],
            "newest first"
        );
    }

    #[test]
    fn an_announcement_keeps_a_bounded_number_of_entries() {
        let mut doc = Drains::default();
        for n in 0..(MAX_ENTRIES as i64 + 10) {
            doc = doc.announcing(entry(&format!("streams-{n}"), "b", n), n);
        }
        assert_eq!(doc.entries.len(), MAX_ENTRIES);
        assert_eq!(doc.entries[0].at_ms, MAX_ENTRIES as i64 + 9);
    }
}
