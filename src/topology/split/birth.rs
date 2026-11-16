//! The split's children are born on its actor (GATE-DECISION-f2-children-on-actor-2026-11-16 A2,
//! the S5 addendum F2, edge change #265). A new leaf's owner is the ring's pick for its prefix, and
//! the ring keeps siblings together: in meas-149's run both halves of a split hashed to another
//! member, which opened them cold while the actor's held appends were refused. So, after `'T'` and
//! before the topology CAS, the actor writes one entry per child into `fleet/overrides.json` naming
//! itself, and mirrors them into its own view, both under the directory's revalidation lock (A1):
//! every member's install refreshes the overrides under the same lock before it installs the new
//! leaves, so no member serves a child by the ring.
//!
//! The entries carry no `rebalance_move` event: a birth is not a move, and the split's own event
//! names the children. A lost CAS is read again, three tries in all; after the third the split
//! publishes anyway (ring-born, as before F2) and says so at WARN. The step never fails the split.
//! A fleet-off cell writes nothing.

use crate::fleet::{FleetDocument, FleetRepository, OverrideEntry, Overrides};
use crate::ownership::OwnershipService;
use crate::shard_directory::ShardDirectory;

/// How many times the actor reads and CASes the overrides document before it gives up.
const TRIES: usize = 3;

/// What the actor writes its children's entries with: the fleet's documents, this instance's
/// ownership view and its name.
pub(crate) struct Birth {
    fleet: FleetRepository,
    ownership: OwnershipService,
    me: String,
}

impl Birth {
    pub(crate) fn new(fleet: FleetRepository, ownership: OwnershipService, me: String) -> Self {
        Self {
            fleet,
            ownership,
            me,
        }
    }

    /// Name this instance the owner of `children` in the overrides document and in its own view,
    /// under `dir`'s revalidation lock: whether the entries were written (`false` for a fleet-off
    /// cell, and after [`TRIES`] lost or unreadable attempts).
    pub(crate) async fn born(&self, dir: &ShardDirectory, children: &[&str], op_id: &str) -> bool {
        if !self.fleet.enabled() {
            return false;
        }
        let _one = dir.revalidation().lock().await;
        if !self.committed(children).await {
            tracing::warn!(
                op_id,
                ?children,
                "a split's children are born on the ring: the overrides CAS was lost {TRIES} times"
            );
            return false;
        }
        for child in children {
            self.ownership.set_override(child, &self.me);
        }
        true
    }

    /// Up to [`TRIES`] reads and CASes of the overrides document with `children`'s entries: whether
    /// one committed.
    async fn committed(&self, children: &[&str]) -> bool {
        for _ in 0..TRIES {
            if self.written(children).await {
                return true;
            }
        }
        false
    }

    /// One read of the overrides document and its CAS with `children`'s entries: whether it
    /// committed.
    async fn written(&self, children: &[&str]) -> bool {
        let Ok((overrides, version)) = self.fleet.read_overrides().await else {
            return false;
        };
        let born = with_children(overrides, children, &self.me, crate::shard::now_ms());
        let Ok(body) = serde_json::to_vec(&born) else {
            return false;
        };
        self.fleet
            .replace_document(FleetDocument::Overrides, body, version)
            .await
    }
}

/// `overrides` with an entry naming `me` at `now_ms` for each of `children`, over any entry that
/// names another instance, and no event.
fn with_children(mut overrides: Overrides, children: &[&str], me: &str, now_ms: i64) -> Overrides {
    for child in children {
        let entry = OverrideEntry {
            to: me.to_owned(),
            ms: now_ms,
        };
        overrides.entries.insert((*child).to_owned(), entry);
    }
    overrides
}

#[cfg(test)]
mod tests {
    use super::{Overrides, with_children};
    use crate::fleet::OverrideEntry;

    /// **A birth names the actor for both children, over another instance's entry, and adds no
    /// event.** An override of `0` to `streams-2` (a lag move's, with its event) and one of `9`
    /// stay as they were, except that `0` and `1` now name `streams-1` at the birth's instant.
    #[test]
    fn a_birth_names_the_actor_for_both_children_and_adds_no_event() {
        let entry = |to: &str, ms| OverrideEntry {
            to: to.to_string(),
            ms,
        };
        let mut before = Overrides::default();
        before.entries.insert("0".into(), entry("streams-2", 5));
        before.entries.insert("9".into(), entry("streams-3", 6));
        before.pending_events.push(crate::ops::OpsEvent::new(
            "rebalance_move",
            "move/0/0".to_string(),
        ));
        let born = with_children(before, &["0", "1"], "streams-1", 1_000);
        let mut entries: Vec<(String, String, i64)> = born
            .entries
            .iter()
            .map(|(p, e)| (p.clone(), e.to.clone(), e.ms))
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            [
                ("0".to_string(), "streams-1".to_string(), 1_000),
                ("1".to_string(), "streams-1".to_string(), 1_000),
                ("9".to_string(), "streams-3".to_string(), 6),
            ]
        );
        assert_eq!(born.pending_events.len(), 1, "the lag move's event only");
    }
}
