//! Overrides that name no leaf (edge #251 amendment R3's carry): a split parent's entry, or the
//! two a co-location leaves behind its merge, names a prefix the cell's topology retired. No
//! instance opens it (the directory opens only leaves, #251 amendment R3-d), but return-home may
//! never drop it (it drops an entry only when the ring's home for the
//! prefix can take one more shard), and every entry counts toward the document's 4,096-entry
//! bound, past which a publication is refused. The writers that already rewrite the document
//! (count balance's move, a merge's co-location) drop such entries as they write, and a beat that
//! finds one in its mirror drops them by CAS (`prune`). The authority is
//! the stored topology, the newest version: an entry for a prefix that is no leaf of it is stale
//! whatever any instance's installed topology says, and a child a newer version made is one of its
//! leaves. A cell without a stored topology, or one whose topology cannot be read, prunes nothing.
//!
//! A live split's children are not stale (GATE-DECISION-f2-children-on-actor-2026-11-16 A4): its
//! actor writes their entries before its topology CAS makes them leaves (`topology::split::birth`),
//! so an entry the intent keeps (`intent::Live::keeps`) is spared while the intent is live. The
//! intent is read only when an entry would be dropped, and an intent that cannot be read prunes
//! nothing.

use object_store::{ObjectStore, UpdateVersion};

use super::{FleetDocument, FleetRepository, Overrides};

/// `overrides` without the entries whose prefix is no leaf of the stored topology and no child the
/// live intent keeps (see the module); unchanged when the stored topology or the intent cannot be
/// read.
pub(crate) async fn pruned(mut overrides: Overrides, store: &dyn ObjectStore) -> Overrides {
    let Ok((stored, _)) = crate::topology::store::read(store).await else {
        return overrides;
    };
    let leaf = |prefix: &String| stored.shards.leaves.contains_key(prefix);
    if overrides.entries.keys().all(leaf) {
        return overrides;
    }
    let Ok(held) = crate::topology::intent::read(store).await else {
        return overrides;
    };
    let live = held
        .filter(|held| !held.intent.is_terminal())
        .map(|held| crate::topology::intent::Live::of(held.intent));
    let kept = |prefix: &String| live.as_ref().is_some_and(|live| live.keeps(prefix));
    overrides
        .entries
        .retain(|prefix, _| leaf(prefix) || kept(prefix));
    overrides
}

/// Drop the entries that name no stored leaf from `fleet/overrides.json` (#251 amendment R3-d):
/// read the document, prune it (`pruned`, by the stored topology), and CAS it back only if an
/// entry dropped. Whether it wrote: an unreadable document, nothing to drop or a lost CAS writes
/// nothing, and the caller's next beat looks again. No event is added.
pub(crate) async fn prune(fleet: &FleetRepository, store: &dyn ObjectStore) -> bool {
    let Ok((overrides, version)) = fleet.read_overrides().await else {
        return false;
    };
    write_pruned(fleet, store, overrides, version).await
}

/// `prune`'s write of `overrides`, read at `version`: the CAS of the pruned document when an entry
/// dropped.
async fn write_pruned(
    fleet: &FleetRepository,
    store: &dyn ObjectStore,
    overrides: Overrides,
    version: Option<UpdateVersion>,
) -> bool {
    let before = overrides.entries.len();
    let next = pruned(overrides, store).await;
    let dropped = before - next.entries.len();
    if dropped == 0 {
        return false;
    }
    tracing::info!(dropped, "overrides: dropped entries that name no leaf");
    let Ok(body) = serde_json::to_vec(&next) else {
        return false;
    };
    fleet
        .replace_document(FleetDocument::Overrides, body, version)
        .await
}

#[cfg(test)]
mod tests;
