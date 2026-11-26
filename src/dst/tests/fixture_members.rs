#![cfg(test)]
//! The membership of record in a rig (§22 step 5; GATE-DECISION-s22-join-2026-11-20 Addendum 3):
//! a cell's members arrive by the join, not by the count. A rig that asserts a layout, a ring or a
//! write count waits for the join to converge first ([`joined`]); a rig whose peer is a seeded beat
//! with no process behind it seeds that peer's listing as its join would have written it
//! ([`seed_joined`]). Setup only: no assertion of the tests that use these changes; the join's
//! wait also fails on a beat published before its boot's decision, which no correct boot writes
//! (Addendum 39).

use super::fixture_http::HttpRig;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// The stored overrides document, or an empty one.
async fn document(fleet: &Arc<dyn ObjectStore>) -> crate::fleet::Overrides {
    let Ok(got) = fleet.get(&Path::from("fleet/overrides.json")).await else {
        return crate::fleet::Overrides::default();
    };
    let body = got.bytes().await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// Whether `document` lists each of `rigs` with its own boot id.
fn listed(document: &crate::fleet::Overrides, rigs: &[&HttpRig]) -> bool {
    let listed = |rig: &&HttpRig| {
        let name = rig.state.config.cli.instance_name.as_str();
        let boot = rig.state.runtime.identity.boot_id.as_str();
        let members = document.members.iter().flatten();
        members
            .into_iter()
            .any(|member| member.name == name && member.boot_id.as_deref() == Some(boot))
    };
    rigs.iter().all(listed)
}

/// Whether `document` lists each of `rigs` with its own boot id, and holds no pin (an entry
/// stamped 0, `fleet::join::PIN_MS`): the join has converged and its pins have returned.
fn converged(document: &crate::fleet::Overrides, rigs: &[&HttpRig]) -> bool {
    let pinned = document.entries.values().any(|entry| entry.ms == 0);
    listed(document, rigs) && !pinned
}

/// Whether every one of `rigs` has published a view, with `document`'s entries as its overrides: a
/// view not yet published, or still holding a pin the document no longer has, describes a cell
/// that has not converged (the join ruling, Addendum 23 §2 (a)).
fn views_agree(document: &crate::fleet::Overrides, rigs: &[&HttpRig]) -> bool {
    let entries: std::collections::HashMap<String, String> = document
        .entries
        .iter()
        .map(|(leaf, entry)| (leaf.clone(), entry.to.clone()))
        .collect();
    let agrees = |rig: &&HttpRig| {
        let view = &rig.state.ownership;
        !view.ring_active().is_empty() && view.overrides() == entries
    };
    rigs.iter().all(agrees)
}

/// What `joined` waits on, for its panic: the document's members and entries, and each rig's ring
/// and entries.
fn report(document: &crate::fleet::Overrides, rigs: &[&HttpRig]) -> String {
    let views: Vec<_> = rigs
        .iter()
        .map(|rig| {
            let view = &rig.state.ownership;
            let mut entries: Vec<_> = view.overrides().into_iter().collect();
            entries.sort();
            (view.instance().to_string(), view.ring_active(), entries)
        })
        .collect();
    let mut entries: Vec<_> = document
        .entries
        .iter()
        .map(|(leaf, e)| (leaf, &e.to))
        .collect();
    entries.sort();
    format!(
        "converged={} members={:?} entries={entries:?} views={views:?}",
        converged(document, rigs),
        document.members
    )
}

/// The pins of `document` that `fleet::return_home_allowed` would release over `rig`'s view (the
/// join ruling, Addendum 25 (i)): an entry stamped 0 whose home over the ring is another member,
/// where that member's leaves and this one stay within the fair share of the cell's leaves over
/// the ring. A cell is settled when there is none: a pin the fair share keeps on its holder is a
/// placement, not a pending move.
fn releasable(document: &crate::fleet::Overrides, rig: &HttpRig) -> Vec<String> {
    let view = &rig.state.ownership;
    let ring = view.ring_active();
    let leaves = rig.state.shards.prefixes();
    let owned = |name: &str| {
        let held = |leaf: &&String| view.effective_owner(leaf).as_deref() == Some(name);
        leaves.iter().filter(held).count()
    };
    let released = |(leaf, entry): &(&String, &crate::fleet::OverrideEntry)| {
        let home = &ring[crate::ownership::ring_pick(leaf, &ring)];
        entry.ms == 0
            && *home != entry.to
            && crate::fleet::return_home_allowed(owned(home), 1, leaves.len(), ring.len())
    };
    let mut pins: Vec<String> = document
        .entries
        .iter()
        .filter(released)
        .map(|(leaf, _)| leaf.clone())
        .collect();
    pins.sort();
    pins
}

/// Whether every one of `rigs` rings every one of them: a view a join has not reached yet judges
/// the pins over the ring before it.
fn rings_whole(rigs: &[&HttpRig]) -> bool {
    let names: Vec<&str> = rigs
        .iter()
        .map(|rig| rig.state.ownership.instance())
        .collect();
    let whole = |rig: &&HttpRig| {
        let ring = rig.state.ownership.ring_active();
        names
            .iter()
            .all(|name| ring.iter().any(|member| member == name))
    };
    rigs.iter().all(whole)
}

/// The pins `fleet`'s document holds, as (leaf, holder), sorted.
pub(super) async fn pins(fleet: &Arc<dyn ObjectStore>) -> Vec<(String, String)> {
    let read = document(fleet).await;
    let pinned = read.entries.iter().filter(|(_, entry)| entry.ms == 0);
    let mut pins: Vec<_> = pinned
        .map(|(leaf, e)| (leaf.clone(), e.to.clone()))
        .collect();
    pins.sort();
    pins
}

/// Wait, within `bound`, until `fleet`'s document lists every one of `rigs` with its own boot id,
/// every rig's view publishes its entries and rings all of them, and no pin stands that
/// `fleet::return_home_allowed` would release (Addendum 25 (i): settled); the pins that stand. A
/// releasable pin that nothing releases fails here, by name.
pub(super) async fn settled(
    fleet: &Arc<dyn ObjectStore>,
    rigs: &[&HttpRig],
    bound: Duration,
) -> Vec<(String, String)> {
    let t0 = tokio::time::Instant::now();
    loop {
        let read = document(fleet).await;
        let open = rigs
            .first()
            .map(|rig| releasable(&read, rig))
            .unwrap_or_default();
        if listed(&read, rigs) && views_agree(&read, rigs) && rings_whole(rigs) && open.is_empty() {
            return pins(fleet).await;
        }
        assert!(
            t0.elapsed() < bound,
            "settled: releasable pins {open:?}; {}",
            report(&read, rigs)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait, within `bound`, until `fleet`'s document lists every one of `rigs` with its own boot id and
/// every rig's view publishes its entries, pins included: for a cell whose count names an ordinal
/// not yet booted, whose pins (C7) stand until it joins.
pub(super) async fn published(fleet: &Arc<dyn ObjectStore>, rigs: &[&HttpRig], bound: Duration) {
    let t0 = tokio::time::Instant::now();
    loop {
        let read = document(fleet).await;
        if listed(&read, rigs) && views_agree(&read, rigs) {
            return;
        }
        assert!(
            t0.elapsed() < bound,
            "the views publish: {}",
            report(&read, rigs)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Fails if `fleet` holds a beat of `rig`'s own boot while that boot is undecided (C13's verdict
/// not taken; GATE-DECISION-s22-join-2026-11-20 Addendum 39): the heartbeat takes the verdict
/// before it allows a beat (`fleet::heartbeat::before_the_first_beat`), so such a beat was
/// published before its decision. A restart's previous beat carries the previous boot's id and is
/// in the store before the new boot decides, so only this boot's id counts. The verdict is read
/// after the beat: one taken before the beat was published is seen.
async fn decided_before_beating(fleet: &Arc<dyn ObjectStore>, rig: &HttpRig) {
    let name = rig.state.config.cli.instance_name.as_str();
    let boot = rig.state.runtime.identity.boot_id.as_str();
    let Ok(got) = fleet.get(&Path::from(format!("fleet/{name}.json"))).await else {
        return;
    };
    let Ok(body) = got.bytes().await else {
        return;
    };
    let beat: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let own = beat["boot_id"].as_str() == Some(boot);
    assert!(
        !own || rig.state.fleet.standing().boot_verdict().is_some(),
        "{name} published a beat of its boot {boot} before deciding it"
    );
}

/// Wait, within `bound`, until `fleet`'s document lists every one of `rigs` with its own boot id
/// and holds no pin, and every rig's view publishes its entries, failing on a beat published
/// before its boot's decision ([`decided_before_beating`]).
pub(super) async fn joined(fleet: &Arc<dyn ObjectStore>, rigs: &[&HttpRig], bound: Duration) {
    let t0 = tokio::time::Instant::now();
    loop {
        for rig in rigs {
            decided_before_beating(fleet, rig).await;
        }
        own_beats_stamped(fleet, rigs).await;
        let read = document(fleet).await;
        if converged(&read, rigs) && views_agree(&read, rigs) {
            return;
        }
        assert!(
            t0.elapsed() < bound,
            "the join converges: {}",
            report(&read, rigs)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A beat `state`'s runtime published is stamped with its own clock less its tests' skew, within
/// the ring's liveness (GATE-DECISION-s22-join-2026-11-20 Addendum 65): a beat dated anywhere else
/// (at the epoch, say) leaves every view unformed, so a wait on one fails at once, by name, rather
/// than after its bound.
pub(super) fn own_stamp(state: &crate::http::AppState, ts_ms: i64) {
    let name = state.config.cli.instance_name.as_str();
    let skew = i64::try_from(state.fleet.standing().beat_skew()).unwrap_or(i64::MAX);
    let clock = crate::shard::now_ms().saturating_sub(skew);
    assert!(
        (clock - 30_000..=clock + 1_000).contains(&ts_ms),
        "{name}'s own beat is stamped {ts_ms}, not its clock less its test skew ({clock})"
    );
}

/// [`own_stamp`] for each of `rigs`' stored beat of its own boot, once it has one.
pub(super) async fn own_beats_stamped(fleet: &Arc<dyn ObjectStore>, rigs: &[&HttpRig]) {
    for rig in rigs {
        let name = rig.state.config.cli.instance_name.as_str();
        let Ok(got) = fleet.get(&Path::from(format!("fleet/{name}.json"))).await else {
            continue;
        };
        let Ok(body) = got.bytes().await else {
            continue;
        };
        let beat: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let boot = rig.state.runtime.identity.boot_id.as_str();
        if let (Some(ts_ms), Some(true)) = (
            beat["ts_ms"].as_i64(),
            beat["boot_id"].as_str().map(|own| own == boot),
        ) {
            own_stamp(&rig.state, ts_ms);
        }
    }
}

/// [`joined`] over every rig of `rigs`.
pub(super) async fn all_joined(fleet: &Arc<dyn ObjectStore>, rigs: &[HttpRig], bound: Duration) {
    let rigs: Vec<&HttpRig> = rigs.iter().collect();
    joined(fleet, &rigs, bound).await;
}

/// List `name` with `boot_id` in `fleet`'s document, as its join CAS would have, for a peer whose
/// beat is seeded with no process behind it (it never joins by itself). Entries and other members
/// are kept as read.
pub(super) async fn seed_joined(fleet: &Arc<dyn ObjectStore>, name: &str, boot_id: &str) {
    let mut next = document(fleet).await;
    let members = next.members.get_or_insert_with(Vec::new);
    members.retain(|member| member.name != name);
    members.push(crate::fleet::Member {
        name: name.to_string(),
        boot_id: Some(boot_id.to_string()),
    });
    // In the ordinal order, as a join CAS writes it (the join ruling, Addendum 23 §3).
    members.sort_by_key(|member| ordinal_index(&member.name));
    let body = serde_json::to_vec(&next).unwrap();
    let path = Path::from("fleet/overrides.json");
    fleet.put(&path, PutPayload::from(body)).await.unwrap();
}

/// `name`'s live beat with no process behind it, holding no shard, listed as its join would list it
/// (the join ruling, Addendum 3 (A)): a beat alone never joins, since only a join CAS adds a member
/// (Addendum 14), and the add half that put such a beat in the ring is gone.
pub(super) async fn joined_peer(fleet: &Arc<dyn ObjectStore>, name: &str) {
    let ts_ms = crate::shard::now_ms() + 600_000;
    let beat = format!(
        r#"{{"instance":"{name}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":false,"url":"https://{name}.invalid"}}"#
    );
    let path = Path::from(format!("fleet/{name}.json"));
    fleet.put(&path, PutPayload::from(beat)).await.unwrap();
    seed_joined(fleet, name, "").await;
}

/// List each of `members` with the boot id its runtime boots with, before any of them boots: a
/// cell whose join is not the test's subject starts converged (the join ruling, Addendum 23 §2
/// (b)). Never for a DST whose subject is the join's sequence. Entries are kept as read.
pub(super) async fn seed_members(
    fleet: &Arc<dyn ObjectStore>,
    members: &[(&str, &super::fixture_runtime::RigRuntime)],
) {
    for (name, runtime) in members {
        seed_joined(fleet, name, &runtime.caps.identity.boot_id).await;
    }
}

/// `name`'s ordinal index (`streams-<n>`), a name outside the pattern last.
fn ordinal_index(name: &str) -> u64 {
    let index = name
        .strip_prefix("streams-")
        .and_then(|index| index.parse().ok());
    index.unwrap_or(u64::MAX)
}
