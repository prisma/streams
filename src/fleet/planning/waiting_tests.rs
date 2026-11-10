#![cfg(test)]
//! The planner's answer to a split that waits for an instance (S5; S4 §10.1 rule 4 as amended,
//! EDGE-261-RATIFICATION §2: `need ≤ shards + 1` while any member's split waits).

use super::Demand;
use super::Loads;
use super::demand_tests::{n, quiet, thresholds};
use std::time::Instant;

/// A member whose topology policy's split waits for an instance asks for one more at
/// once, and the shrink keeps it; the reason names its shards against the ceiling
/// (S5, Q4). A beat that says so folds its resident shards, the most of any member.
#[test]
fn a_split_waiting_for_an_instance_wants_one_more_and_names_the_ceiling() {
    let waiting = Loads {
        instance_full: Some(7),
        ..quiet(64)
    };
    let plan = Demand::default().plan(&waiting, &thresholds(), Instant::now());
    assert_eq!(n(&plan), (5, 5, 0));
    assert!(
        plan.reason.ends_with(" shards_per_instance=7/7 (5)"),
        "{}",
        plan.reason
    );
    let plan = Demand::default().plan(&quiet(64), &thresholds(), Instant::now());
    assert_eq!(n(&plan), (2, 3, 0));
    assert!(
        !plan.reason.contains("shards_per_instance"),
        "{}",
        plan.reason
    );
    // One shard on one instance: the instance the split waits for is the child's home, past
    // the clamp by the one shard the split adds and no further.
    let one = Loads {
        instance_full: Some(1),
        shards: 1,
        live: 1,
        ..Loads::default()
    };
    assert_eq!(
        n(&Demand::default().plan(&one, &thresholds(), Instant::now())),
        (2, 2, 1)
    );
    let two = Loads {
        instance_full: Some(2),
        shards: 2,
        live: 1,
        ..Loads::default()
    };
    assert_eq!(
        n(&Demand::default().plan(&two, &thresholds(), Instant::now())),
        (2, 2, 0)
    );
    let beat = |extra: &str| -> crate::fleet::Heartbeat {
        let document = format!(
            r#"{{"instance":"streams-1","ts_ms":1,"rps":0.0,"owned_shards":[],"draining":false{extra}}}"#
        );
        serde_json::from_str(&document).unwrap()
    };
    let mut loads = Loads::default();
    loads.fold(&beat(r#","shards_resident":3"#));
    assert_eq!(loads.instance_full, None);
    loads.fold(&beat(
        r#","split_refused":"instance_full","shards_resident":6"#,
    ));
    loads.fold(&beat(
        r#","split_refused":"instance_full","shards_resident":4"#,
    ));
    assert_eq!(loads.instance_full, Some(6));
}

/// A split held while the fleet settles keeps what it asked for and asks nothing more (#261
/// amendment): a member's `fleet_settling` blocks the shrink as a live intent does (the sentinel)
/// and leaves the grow and the split demand where the same beat without it puts them, so a
/// one-shard cell's second instance stays until the split can use it and no third is asked
/// for. `instance_full` still asks for one past the shards and no further.
#[test]
fn a_split_waiting_for_the_fleet_to_settle_keeps_the_count_and_asks_for_nothing() {
    let plan = |refused: &str, shards: u64| {
        let mut loads = Loads {
            shards,
            ..Loads::default()
        };
        let beat = format!(
            r#"{{"instance":"streams-1","ts_ms":1,"rps":0.0,"owned_shards":[""],"draining":false,"split_refused":"{refused}","shards_resident":1}}"#
        );
        loads.fold(&serde_json::from_str(&beat).unwrap());
        n(&Demand::default().plan(&loads, &thresholds(), Instant::now()))
    };
    for shards in [1, 3] {
        let (need, shrink, split) = plan("none", shards);
        assert!(shrink < thresholds().max, "{shards}: {shrink}");
        assert_eq!(
            plan("fleet_settling", shards),
            (need, thresholds().max, split),
            "{shards} shards"
        );
    }
    assert_eq!(plan("instance_full", 1), (2, 2, 1));
}
