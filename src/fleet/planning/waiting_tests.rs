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
