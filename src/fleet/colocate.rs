//! A merge's co-location (S6-DESIGN §7.2 step 0; #261 B1 as amended by
//! GATE-DECISION-s22-join-2026-11-20 Addendum 7): a merge's two halves must be one instance's, and
//! a pair the fleet split across two instances (a count-balance move, say) is gathered on one
//! instance first. No instance writes an entry for a leaf it does not hold. Each holder computes
//! one target from its own view (`step`, in `colocation_target`'s order) in its own policy pass and
//! acts for its own half only: on the target it names its half for itself (`pin`); elsewhere it
//! hands its half there giver first (`fleet::handoff`), once the target, when that is the other
//! holder, has named its own half for itself. So both halves are overridden before the merge, and
//! count balance (it moves only a shard without an override) moves neither; the merge, once the
//! last move's cooldown has passed, finds both on one instance. After the merge the halves are no
//! leaves; the next beat that sees them, or the next write of the document by count balance or a
//! pin, drops their entries (`stale`).

use super::{FleetDocument, FleetRepository};
use crate::ownership::OwnershipService;
use crate::topology::policy::{Instance, View};

/// How a holder's pin of its own half ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Pinned {
    /// The document names the half for this instance, stamped now, with a `rebalance_move` event.
    Written,
    /// The document already named the half for this instance: nothing was written.
    Already,
    /// Another instance holds the half: nothing was written.
    NotHeld,
    /// The document was unreadable or the CAS was lost: nothing was written; the caller asks
    /// again on its next pass.
    Lost,
}

/// Name `half`, a leaf this instance holds, for this instance in one CAS of the overrides
/// document, the entries that name no leaf of the topology stored in `store` dropped (`stale`),
/// and mirror the entry here at once. A half another instance holds is never written.
pub(crate) async fn pin(
    fleet: &FleetRepository,
    ownership: &OwnershipService,
    store: &dyn object_store::ObjectStore,
    half: &str,
) -> Pinned {
    if !ownership.is_mine(half) {
        return Pinned::NotHeld;
    }
    let instance = ownership.instance();
    let Ok((overrides, version)) = fleet.read_overrides().await else {
        return Pinned::Lost;
    };
    if overrides
        .entries
        .get(half)
        .is_some_and(|entry| entry.to == instance)
    {
        return Pinned::Already;
    }
    let overrides = super::stale::pruned(overrides, store).await;
    let now_ms = crate::shard::now_ms();
    let next = super::count_balance::moved_document(overrides, half, instance, now_ms);
    let Ok(body) = serde_json::to_vec(&next) else {
        return Pinned::Lost;
    };
    if !fleet
        .replace_document(FleetDocument::Overrides, body, version)
        .await
    {
        return Pinned::Lost;
    }
    ownership.set_override(half, instance);
    Pinned::Written
}

/// What a holder of one half of a split pair does in its policy pass (see the module).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// No instance has room for the pair and the merged leaf, and this instance is the actor
    /// (`…0`'s holder): it asks the fleet for one.
    NoRoom,
    /// Nothing to write this pass: this instance holds neither half, or it is the partner and no
    /// instance has room, or its half is already named for it on the target, or the target holds
    /// the other half and has not named it for itself.
    Wait,
    /// The target is this instance: name its half for it (`pin`).
    Pin(String),
    /// Hand `half` to the target `to`, giver first.
    Hand { half: String, to: String },
}

/// This instance's step (`view.me`'s) for the pair under `parent` in `view`, under the ceiling
/// `max`, reading the entries of the overrides document as `ownership` mirrors them.
pub(crate) fn step(view: &View, max: u32, parent: &str, ownership: &OwnershipService) -> Step {
    let halves = [format!("{parent}0"), format!("{parent}1")];
    let [zero, one] = &halves;
    let (half, other) = if owner(view, zero) == Some(&view.me) {
        (zero, one)
    } else if owner(view, one) == Some(&view.me) {
        (one, zero)
    } else {
        return Step::Wait;
    };
    let Some(target) = colocation_target(view, max, &halves) else {
        return if half == zero {
            Step::NoRoom
        } else {
            Step::Wait
        };
    };
    let named = ownership.overrides();
    let named_for = |prefix: &str, to: &str| named.get(prefix).is_some_and(|at| at == to);
    if target == view.me {
        return if named_for(half, &target) {
            Step::Wait
        } else {
            Step::Pin(half.clone())
        };
    }
    if owner(view, other) == Some(&target) && !named_for(other, &target) {
        return Step::Wait;
    }
    Step::Hand {
        half: half.clone(),
        to: target,
    }
}

/// The owner of the leaf `prefix` in `view`.
fn owner<'a>(view: &'a View, prefix: &str) -> Option<&'a String> {
    view.leaves
        .iter()
        .find(|leaf| leaf.prefix == prefix)
        .map(|leaf| &leaf.owner)
}

/// Where a pair split across instances goes before its merge, as every holder computes it from its
/// view (#261 B1; the F6 addendum's C2): nothing in it is relative to the instance computing it.
/// The first of the actor (the owner of `halves[0]`, `…0`), the partner (the owner of `…1`) and
/// every other accepting instance, fewest shards first, then by name, with room under `max` for
/// what it would hold until the merge publishes. A holder takes the other half and the merged leaf
/// (its shards + 2); any other instance takes both halves and the merged leaf (+ 3). The partner
/// and the others must be accepting.
fn colocation_target(view: &View, max: u32, halves: &[String; 2]) -> Option<String> {
    let [zero, one] = halves;
    let actor = owner(view, zero)?;
    let partner = owner(view, one).filter(|partner| *partner != actor);
    let accepting = |id: &str| view.instances.iter().any(|i| i.id == id && i.accepting);
    let resident = |id: &str| {
        view.instances
            .iter()
            .find(|i| i.id == id)
            .map_or(0, |i| i.resident_shards)
    };
    let room = |id: &str, takes: u32| resident(id).saturating_add(takes) <= max;
    if room(actor.as_str(), 2) {
        return Some(actor.clone());
    }
    if let Some(partner) = partner.filter(|p| accepting(p.as_str()) && room(p.as_str(), 2)) {
        return Some(partner.clone());
    }
    let mut others: Vec<&Instance> = view
        .instances
        .iter()
        .filter(|i| i.accepting && i.id != *actor && owner(view, one) != Some(&i.id))
        .collect();
    others.sort_by_key(|i| (i.resident_shards, i.id.clone()));
    others
        .into_iter()
        .find(|i| room(&i.id, 3))
        .map(|i| i.id.clone())
}

#[cfg(test)]
mod tests;
