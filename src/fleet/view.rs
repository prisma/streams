//! The fleet tick's ownership view, read and published under the shard directory's revalidation
//! lock (GATE-DECISION-f2-children-on-actor-2026-11-16 A1). Every topology install takes the same
//! lock and refreshes the overrides first, and a split's actor writes its children's entries under
//! it, so a tick whose overrides read was served before a split's entries cannot publish that older
//! view after an install that refreshed to the newer one.

use std::collections::HashMap;

use object_store::UpdateVersion;

use super::{AppState, Overrides};

/// Read `fleet/overrides.json` and publish it with `active` as the ownership view, and `peers` as
/// the peer map, under the revalidation lock: the overrides document and its version for the tick's
/// rebalancer. A failed read publishes nothing (the last good view is kept).
pub(super) async fn published(
    state: &AppState,
    active: &[String],
    peers: &HashMap<String, String>,
) -> anyhow::Result<(Overrides, Option<UpdateVersion>)> {
    let _one = state.shards.revalidation().lock().await;
    let (overrides, version) = state.fleet.read_overrides().await?;
    // Tests only: a tick held between its read and its publication, the lock still held.
    #[cfg(test)]
    if let Some(point) = state.fleet.standing().take_view_pause()
        && point.reached.send(()).is_ok()
    {
        drop(point.go.await);
    }
    let map: HashMap<String, String> = overrides
        .entries
        .iter()
        .map(|(prefix, entry)| (prefix.clone(), entry.to.clone()))
        .collect();
    // Publish only a complete successful authority read. This synchronous replacement cannot
    // expose a mixed ring/map.
    state.ownership.set_view(active.to_vec(), map);
    state.fleet.standing().set_overrides_tag(tag_of(version.as_ref()));
    state.peer.set_peers(peers.clone());
    state.fleet.standing().mark_progress();
    Ok((overrides, version))
}

/// Read `fleet/overrides.json` and publish its entries with the ring this instance last published,
/// under the revalidation lock: a planned move's mirror of the document it wrote (F6, step 7), and
/// a forward's refresh when its sender holds another version (step 8, D7). The version read, or
/// `None` when the read failed (nothing is published).
pub(crate) async fn refreshed(state: &AppState) -> Option<String> {
    let _one = state.shards.revalidation().lock().await;
    let (overrides, version) = state.fleet.read_overrides().await.ok()?;
    let map: HashMap<String, String> = overrides
        .entries
        .iter()
        .map(|(prefix, entry)| (prefix.clone(), entry.to.clone()))
        .collect();
    state.ownership.set_view(state.ownership.ring_active(), map);
    let tag = tag_of(version.as_ref());
    state.fleet.standing().set_overrides_tag(tag.clone());
    Some(tag)
}

/// A document version as the forward header carries it: its ETag, else its version, else empty.
pub(super) fn tag_of(version: Option<&UpdateVersion>) -> String {
    version
        .and_then(|v| v.e_tag.clone().or_else(|| v.version.clone()))
        .unwrap_or_default()
}
