//! A split refused for room moves its shard to an instance with room
//! (GATE-DECISION-f2-convergence-2026-11-17, option 2 amended). S4 refuses a split with
//! `instance_full` when this instance cannot hold the parent beside both children. An accepting
//! instance with room for the shard and its two children may still exist: then the shard moves
//! there giver first (`fleet::handoff`: parked, flushed and closed here, then one overrides CAS
//! with a `rebalance_move` among its events) and its new owner splits it once the move's cooldown has
//! passed (`override_recent` meanwhile). Only with no such instance does the policy ask the fleet
//! for one. A born child is never moved within a move cooldown of its birth: its young override
//! holds the split (`override_recent`) before the split is tried.

use super::super::policy::{Decision, Hold, View, room_for_split};
use super::{Cell, resident};
use crate::fleet::handoff::Outcome;

/// The split of `prefix` was refused `instance_full`: move `prefix` onto the instance with room
/// (`policy::room_for_split`) and hold `override_recent`; a move that was not made holds how it
/// ended (`held_for`); with no instance that has room, ask the fleet for one.
pub(crate) async fn moved_for_room(cell: &Cell, view: &View, prefix: &str) -> Decision {
    let max = cell.limits.shards_per_instance_max;
    let Some(target) = room_for_split(view, max, &cell.me) else {
        return Decision::AskInstance {
            resident: resident(view, &cell.me),
            max,
        };
    };
    // Giver first (F6): this instance holds `prefix` and hands it off before the overrides name
    // `target`.
    let change = crate::fleet::handoff::Change::To(target.clone());
    let outcome = crate::fleet::handoff::hand_off_paced(&cell.fleet, prefix, change, false).await;
    tracing::info!(
        prefix,
        %target,
        ?outcome,
        "topology split refused for room: the move to an instance with room ended"
    );
    Decision::Hold(held_for(outcome))
}

/// The hold a room move's end answers: the move's cooldown once made, else how it was not made. A
/// move that was not made is not steady: its leaf is over the split line and the next pass tries
/// again, so the decision an operator reads names the attempt's end.
fn held_for(outcome: Outcome) -> Hold {
    match outcome {
        Outcome::Moved => Hold::OverrideRecent,
        Outcome::Kept => Hold::MoveKept,
        Outcome::BackedOff => Hold::MoveBackedOff,
        // A room move is not evacuable, so `Evacuate` is not its end; it would be a missed bound.
        Outcome::NotStarted | Outcome::Evacuate => Hold::MoveNotStarted,
    }
}

#[cfg(test)]
mod tests {
    use super::{Hold, Outcome, held_for};

    /// Addendum 49: a room move that was not made is never reported `steady`; each end has its own
    /// hold.
    #[test]
    fn a_room_move_holds_by_how_it_ended() {
        let holds = [
            (Outcome::Moved, Hold::OverrideRecent),
            (Outcome::Kept, Hold::MoveKept),
            (Outcome::BackedOff, Hold::MoveBackedOff),
            (Outcome::NotStarted, Hold::MoveNotStarted),
            (Outcome::Evacuate, Hold::MoveNotStarted),
        ];
        for (outcome, hold) in holds {
            assert_eq!(held_for(outcome), hold, "{outcome:?}");
        }
    }
}
