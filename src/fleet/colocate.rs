//! A merge's co-location (S6-DESIGN §7.2 step 0; #261 B1 as amended by
//! GATE-DECISION-s22-join-2026-11-20 Addenda 7 and 13): a merge's two halves must be one
//! instance's, and a pair the fleet split across two instances (a count-balance move, say) is
//! gathered on one instance first. No instance writes an entry for a leaf it does not hold. Each
//! holder computes one target from its own view (`step`, in `colocation_target`'s order) in its own
//! policy pass and acts for its own half only: elsewhere than the target it hands its half there
//! giver first (`fleet::handoff`); on the target it writes nothing, because its view already gives
//! it its own half, by an entry or by the ring (Addendum 13 (1): a pin there named the leaf's ring
//! home, a no-op that return-home drops). While the pair is its policy's merge candidate each
//! holder shields the halves it holds from its own count balance, lag move and return-home, in its
//! own process (`Standing::shield`, Addendum 13 (2)); nothing is written for that. The merge, once
//! the last move's cooldown has passed, finds both halves on one instance. After the merge the
//! halves are no leaves; the next beat that sees them, or the next write of the document, drops
//! their entries (`stale`).

use crate::topology::policy::{Instance, View};

/// What a holder of one half of a split pair does in its policy pass (see the module).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// No instance has room for the pair and the merged leaf, and this instance is the actor
    /// (`…0`'s holder): it asks the fleet for one.
    NoRoom,
    /// Nothing to write this pass: this instance holds neither half, or it is the partner and no
    /// instance has room, or it is the target (its view already gives it its own half).
    Wait,
    /// Hand `half` to the target `to`, giver first.
    Hand { half: String, to: String },
}

/// This instance's step (`view.me`'s) for the pair under `parent` in `view`, under the ceiling
/// `max`.
pub(crate) fn step(view: &View, max: u32, parent: &str) -> Step {
    let halves = [format!("{parent}0"), format!("{parent}1")];
    let [zero, one] = &halves;
    let (half, _) = if owner(view, zero) == Some(&view.me) {
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
    if target == view.me {
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
