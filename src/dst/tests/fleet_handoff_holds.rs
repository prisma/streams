#![cfg(test)]
//! What the topology policy holds after a planned move it asked for was made or was not
//! (GATE-DECISION-s22-join-2026-11-20 Addenda 49 and 52): the room move and the co-location's hand.

use super::fleet_handoff_cases::{entry, giver, stop};

/// **A split refused for room holds `override_recent` when its shard moved, and `move_not_started`
/// when it did not** (`policy_loop::room::moved_for_room`). Two instances, `streams-2` with room
/// for `00` and its two children: with `00` claimed by another move the move is not made
/// (`move_not_started`); released, the move is made (`override_recent`) and the entry names
/// `streams-2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_room_move_holds_override_recent_only_when_it_moved() {
    use crate::topology::policy::{Decision, Hold};
    use crate::topology::policy_loop;
    let rig = giver(true).await;
    let state = &rig.state;
    let cell = policy_cell(state, 7);
    let view = two_instances(Vec::new());
    let engine = state.shards.open("00").expect("00 is open");
    assert!(
        engine.gc_state().gate.claim_move(),
        "another move claims 00"
    );
    let kept = policy_loop::room::moved_for_room(&cell, &view, "00").await;
    engine.gc_state().gate.release_move();
    drop(engine);
    let moved = policy_loop::room::moved_for_room(&cell, &view, "00").await;
    let not_started = Decision::Hold(Hold::MoveNotStarted);
    assert_eq!(
        (kept, moved),
        (not_started, Decision::Hold(Hold::OverrideRecent))
    );
    assert_eq!(entry(&rig, "00").await.as_deref(), Some("streams-2"));
    stop(rig).await;
}

/// **A half that was not handed for a co-location holds how the move ended** (`policy_loop::colocate`,
/// Addendum 52). `streams-1` holds `00` and has no room for the pair; `streams-2` holds `01` and has
/// room: the half's hand is not made while `00` is claimed (`move_not_started`), then is
/// (`override_recent`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_co_location_hand_holds_override_recent_only_when_it_moved() {
    use crate::topology::policy::{Decision, Hold};
    use crate::topology::policy_loop;
    let rig = giver(true).await;
    let state = &rig.state;
    let cell = policy_cell(state, 3);
    let view = two_instances(vec![leaf("00", "streams-1"), leaf("01", "streams-2")]);
    let engine = state.shards.open("00").expect("00 is open");
    assert!(
        engine.gc_state().gate.claim_move(),
        "another move claims 00"
    );
    let kept = policy_loop::colocate(&cell, &view, "0").await;
    engine.gc_state().gate.release_move();
    drop(engine);
    let moved = policy_loop::colocate(&cell, &view, "0").await;
    let not_started = Decision::Hold(Hold::MoveNotStarted);
    assert_eq!(
        (kept, moved),
        (not_started, Decision::Hold(Hold::OverrideRecent))
    );
    assert_eq!(entry(&rig, "00").await.as_deref(), Some("streams-2"));
    stop(rig).await;
}

fn leaf(prefix: &str, owner: &str) -> crate::topology::policy::Leaf {
    crate::topology::policy::Leaf {
        prefix: prefix.to_string(),
        born_ms: 0,
        owner: owner.to_string(),
        beat_ms: None,
        load: std::collections::BTreeMap::new(),
    }
}

/// `streams-1`'s policy cell over `state`, at most `max` shards an instance.
fn policy_cell(state: &crate::http::AppState, max: u32) -> crate::topology::policy_loop::Cell {
    use crate::topology::policy_loop;
    policy_loop::Cell {
        me: "streams-1".to_string(),
        fleet: state.fleet.clone(),
        shards: state.shards.clone(),
        ownership: state.ownership.clone(),
        store: state.data_store.clone(),
        limits: crate::topology::policy::Limits {
            shards_per_instance_max: max,
            ..policy_loop::limits(&state.config)
        },
        appends_ceiling: state.config.cli.shard_appends_ceiling,
        rss_shed_mb: 0,
        split: policy_loop::split_entry(state.shards.clone()),
        merge: policy_loop::merge_entry(state.shards.clone()),
    }
}

/// `streams-1`'s view of itself (two shards) and an empty accepting `streams-2`, over `leaves`.
fn two_instances(leaves: Vec<crate::topology::policy::Leaf>) -> crate::topology::policy::View {
    let instance = |id: &str, resident_shards| crate::topology::policy::Instance {
        id: id.to_string(),
        resident_shards,
        accepting: true,
    };
    crate::topology::policy::View {
        now_ms: 0,
        me: "streams-1".to_string(),
        leaves,
        instances: vec![instance("streams-1", 2), instance("streams-2", 0)],
        split_demand: 0,
        live: 0,
        beats_fresh: true,
        intent_live: false,
        last_op_end_ms: None,
        desired_computed_ms: None,
        youngest_override_ms: None,
        congested: false,
    }
}
