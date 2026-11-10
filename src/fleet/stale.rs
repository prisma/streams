//! Overrides that name no leaf (edge #251 amendment R3's carry): a split parent's entry, or the
//! two a co-location leaves behind its merge, names a prefix the cell's topology retired. It moves
//! nothing, but return-home may never drop it (it drops an entry only when the ring's home for the
//! prefix can take one more shard), and every entry counts toward the document's 4,096-entry
//! bound, past which a publication is refused. The writers that already rewrite the document
//! (count balance's move, a merge's co-location) drop such entries as they write. The authority is
//! the stored topology, the newest version: an entry for a prefix that is no leaf of it is stale
//! whatever any instance's installed topology says, and a child a newer version made is one of its
//! leaves. A cell without a stored topology, or one whose topology cannot be read, prunes nothing.

use object_store::ObjectStore;

use super::Overrides;

/// `overrides` without the entries whose prefix is no leaf of the stored topology (see the
/// module); unchanged when the stored topology cannot be read.
pub(crate) async fn pruned(mut overrides: Overrides, store: &dyn ObjectStore) -> Overrides {
    let Ok((stored, _)) = crate::topology::store::read(store).await else {
        return overrides;
    };
    let before = overrides.entries.len();
    overrides
        .entries
        .retain(|prefix, _| stored.shards.leaves.contains_key(prefix));
    let dropped = before - overrides.entries.len();
    if dropped > 0 {
        tracing::info!(dropped, "overrides: dropped entries that name no leaf");
    }
    overrides
}

#[cfg(test)]
mod tests;
