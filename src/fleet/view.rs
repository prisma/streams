//! The fleet tick's ownership view, read and published under the shard directory's revalidation
//! lock (GATE-DECISION-f2-children-on-actor-2026-11-16 A1). Every topology install takes the same
//! lock and refreshes the overrides first, and a split's actor writes its children's entries under
//! it, so a tick whose overrides read was served before a split's entries cannot publish that older
//! view after an install that refreshed to the newer one.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use object_store::UpdateVersion;

use super::{AppState, Overrides};

/// Read `fleet/overrides.json` and publish it with its ring over this pass's `candidates` (§22 C1,
/// C2) as the ownership view, and `peers` as the peer map, under the revalidation lock: the
/// overrides document and its version for the tick's rebalancer. Between the read and the
/// publication the pass makes its one membership write (`join::pass`: a pin, a stamp, a restart's
/// decision or a join, C5), and the view is published from the document as written with no await
/// after the CAS's answer. A failed read publishes nothing (the last good view is kept).
pub(super) async fn published(
    state: &AppState,
    candidates: &super::record::Candidates,
    peers: &HashMap<String, String>,
) -> anyhow::Result<(Overrides, Option<UpdateVersion>)> {
    let _one = state.shards.revalidation().lock().await;
    let read = state.fleet.read_overrides().await?;
    let candidates = &candidates
        .clone()
        .with_boot(&state.runtime.identity.boot_id)
        .with_decided(state.fleet.standing().decided_member());
    let (overrides, version) = super::join::pass(state, candidates, read).await;
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
    // Tests only, a failpoint: a move of a leaf still open here is held (C3's control).
    #[cfg(test)]
    held_while_open(state, &map).await;
    // Publish only a complete successful authority read, its ring and entries from the one
    // document. This synchronous replacement cannot expose a mixed ring/map.
    let ring = candidates.ring(overrides.members.as_deref());
    state.fleet.standing().set_candidates(candidates);
    state.ownership.set_view(ring, map);
    state.fleet.standing().tags().set(tag_of(version.as_ref()));
    state.peer.set_peers(peers.clone());
    state.fleet.standing().mark_progress();
    // Publish, then lift a leave's seal (`fleet::leave`).
    super::leave::published(state);
    Ok((overrides, version))
}

/// Tests only, the failpoint `Standing::hold_moves_of_open_leaves`: when it is armed and `map`
/// gives elsewhere a leaf whose engine is open here, wait its hold before the view is published.
#[cfg(test)]
async fn held_while_open(state: &AppState, map: &HashMap<String, String>) {
    let Some(hold) = state.fleet.standing().moved_open_hold() else {
        return;
    };
    let me = state.config.cli.instance_name.as_str();
    let open_elsewhere = |(leaf, to): (&String, &String)| to != me && state.shards.is_open(leaf);
    if map.iter().any(open_elsewhere) {
        tracing::info!(
            ?hold,
            "failpoint: the publication of a move of a leaf open here is held"
        );
        tokio::time::sleep(hold).await;
    }
}

/// Publish `document`, one this runtime just wrote, whole: its entries with its own ring over the
/// tick's last candidates (§22 C2; before the first pass, the ring this instance last published).
/// The writes that named one entry beside the published ring (a split's birth, a co-location,
/// the lag move's evacuation) publish their document so (GATE-DECISION-s22-3b-step4-2026-11-21,
/// Addendum 1: the join ends `set_override`).
pub(crate) fn installed(
    fleet: &super::FleetRepository,
    ownership: &crate::ownership::OwnershipService,
    document: &Overrides,
) {
    let map: HashMap<String, String> = document
        .entries
        .iter()
        .map(|(prefix, entry)| (prefix.clone(), entry.to.clone()))
        .collect();
    let ring = fleet.standing().ring_of(document);
    ownership.set_view(ring.unwrap_or_else(|| ownership.ring_active()), map);
}

/// Read `fleet/overrides.json` and publish its entries with its own ring (§22 C2), under the
/// revalidation lock: a planned move's mirror of the document it wrote (F6, step 7), and
/// a forward's refresh when its sender holds another version (step 8, D7). The version read, or
/// `None` when the read failed (nothing is published).
pub(crate) async fn refreshed(state: &AppState) -> Option<String> {
    let _one = state.shards.revalidation().lock().await;
    published_here(state).await
}

/// A peer's overrides version `seen` (`Streams-Overrides-Version` on a forward or on a relayed
/// `409`) refreshes this instance's mirror when [`super::tags::Tags`] allows it: at most one read
/// for a burst that saw one new version (R1: asked again under the revalidation lock), none for
/// an empty one (R3), and none within the floor after a read that learnt nothing (R2).
pub(crate) async fn refresh_for(state: &AppState, seen: &str) {
    let tags = state.fleet.standing().tags();
    if !tags.reads_for(seen, tokio::time::Instant::now()) {
        return;
    }
    let _one = state.shards.revalidation().lock().await;
    if !tags.reads_for(seen, tokio::time::Instant::now()) {
        return;
    }
    let before = tags.tag();
    let read = published_here(state).await;
    tags.read_for(seen, &before, read.as_deref(), tokio::time::Instant::now());
}

/// Read the overrides document and publish it with its own ring over the tick's last candidates
/// (§22 C2; before the first pass, the ring this instance last published), the revalidation lock
/// held by the caller; the version read.
async fn published_here(state: &AppState) -> Option<String> {
    let (overrides, version) = state.fleet.read_overrides().await.ok()?;
    let map: HashMap<String, String> = overrides
        .entries
        .iter()
        .map(|(prefix, entry)| (prefix.clone(), entry.to.clone()))
        .collect();
    let ring = state.fleet.standing().ring_of(&overrides);
    state
        .ownership
        .set_view(ring.unwrap_or_else(|| state.ownership.ring_active()), map);
    let tag = tag_of(version.as_ref());
    state.fleet.standing().tags().set(tag.clone());
    Some(tag)
}

/// A document version as the forward header carries it: its ETag, else its version, else empty.
pub(super) fn tag_of(version: Option<&UpdateVersion>) -> String {
    version
        .and_then(|v| v.e_tag.clone().or_else(|| v.version.clone()))
        .unwrap_or_default()
}

/// Install this instance's overrides view on its peer client (F6's peer read hop, addendum
/// 2026-11-18): every fleet request carries the version this instance holds, and a peer's `409`
/// naming another version refreshes it through [`refresh_for`] before the asker's next step.
pub(crate) fn watch(state: &Arc<AppState>) {
    state
        .peer
        .watch_overrides(Arc::new(PeerView(Arc::downgrade(state))));
}

/// The peer client's view of this instance's overrides; it holds the state weakly, since the
/// state holds the peer client.
struct PeerView(Weak<AppState>);

impl crate::peer::OverridesView for PeerView {
    fn tag(&self) -> String {
        let tag = |state: Arc<AppState>| state.fleet.standing().tags().tag();
        self.0.upgrade().map(tag).unwrap_or_default()
    }

    fn refresh(&self, seen: String) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
        let state = self.0.clone();
        Box::pin(async move {
            if let Some(state) = state.upgrade() {
                refresh_for(&state, &seen).await;
            }
        })
    }
}
