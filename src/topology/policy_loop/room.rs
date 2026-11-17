//! A split refused for room moves its shard to an instance with room
//! (GATE-DECISION-f2-convergence-2026-11-17, option 2 amended). S4 refuses a split with
//! `instance_full` when this instance cannot hold the parent beside both children. An accepting
//! instance with room for the shard and its two children may still exist: then the shard moves
//! there by one overrides CAS (`fleet::colocate::colocate` with a one-prefix slice, a
//! `rebalance_move` among its events) and its new owner splits it once the move's cooldown has
//! passed (`override_recent` meanwhile). Only with no such instance does the policy ask the fleet
//! for one. A born child is never moved within a move cooldown of its birth: its young override
//! holds the split (`override_recent`) before the split is tried.

use super::super::policy::{Decision, Hold, View, room_for_split};
use super::{Cell, resident};

/// The split of `prefix` was refused `instance_full`: move `prefix` onto the instance with room
/// (`policy::room_for_split`) and hold `override_recent`; a lost CAS holds steady; with no instance that
/// has room, ask the fleet for one.
pub(super) async fn moved_for_room(cell: &Cell, view: &View, prefix: &str) -> Decision {
    let max = cell.limits.shards_per_instance_max;
    let Some(target) = room_for_split(view, max, &cell.me) else {
        return Decision::AskInstance {
            resident: resident(view, &cell.me),
            max,
        };
    };
    let moved = [prefix.to_string()];
    let committed = crate::fleet::colocate::colocate(
        &cell.fleet,
        &cell.ownership,
        &*cell.store,
        &moved,
        &target,
    )
    .await;
    if committed {
        tracing::info!(prefix, %target, "topology split refused for room: moving the shard to an instance with room");
        return Decision::Hold(Hold::OverrideRecent);
    }
    tracing::info!(
        prefix,
        "topology split refused for room: the overrides CAS was lost"
    );
    Decision::Hold(Hold::Steady)
}
