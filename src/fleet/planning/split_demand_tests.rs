#![cfg(test)]
//! The split demand counts load (#243 as amended, #261): the utilization dimension reads only the
//! CPU of members doing real work (≥ 5 rps), and a split's wait for an instance is not a
//! dimension of it. The count and the shrink still read all the CPU.

use super::Demand;
use super::Loads;
use super::demand_tests::{Numbers, n, thresholds};
use std::time::Instant;

/// One fresh member's beat at `rps` and `cpu_pct`, with `extra` JSON fields.
fn beat(rps: f64, cpu_pct: f64, extra: &str) -> crate::fleet::Heartbeat {
    let document = format!(
        r#"{{"instance":"streams-1","ts_ms":1,"rps":{rps:?},"cpu_pct":{cpu_pct:?},"owned_shards":[],"draining":false{extra}}}"#
    );
    serde_json::from_str(&document).unwrap()
}

/// The loads of `beats` (rps, cpu_pct) on `shards`, folded as the tick folds them.
fn folded(beats: &[(f64, f64)], shards: u64) -> Loads {
    let mut loads = Loads {
        shards,
        ..Loads::default()
    };
    for &(rps, cpu) in beats {
        loads.fold(&beat(rps, cpu, ""));
    }
    loads
}

fn plan(loads: &Loads) -> Numbers {
    n(&Demand::default().plan(loads, &thresholds(), Instant::now()))
}

/// Four members at 57 % of a core and no requests, on three shards (the convergence DST's quiet
/// half, `cores_used=2.28 util->4 ... shards=3 split->1 rps=0 live=4`): the CPU holds the
/// count at the shards and asks for no split. The same CPU at 40 rps each asks for one.
#[test]
fn cpu_no_request_explains_holds_the_count_and_asks_for_no_split() {
    let idle = folded(&[(0.0, 57.0); 4], 3);
    assert_eq!(plan(&idle), (3, 3, 0));
    let loaded = folded(&[(40.0, 57.0); 4], 3);
    assert_eq!(plan(&loaded), (3, 3, 1));
}

/// The gate's edge is 5 rps, inclusive: two members at 90 % on one shard want three for the
/// split at 5.0 rps each, two with one of them at 4.9, and none at 4.9 each.
#[test]
fn a_member_counts_toward_the_split_from_five_requests_a_second() {
    let split = |a: f64, b: f64| plan(&folded(&[(a, 90.0), (b, 90.0)], 1)).2;
    assert_eq!(split(5.0, 5.0), 2);
    assert_eq!(split(5.0, 4.9), 1);
    assert_eq!(split(4.9, 4.9), 0);
}

/// Only the loaded member's CPU counts: one at 40 rps and 60 % beside one at no requests and three
/// cores, on one shard, asks for no split.
#[test]
fn only_a_loaded_members_cpu_counts_toward_the_split() {
    let loads = folded(&[(40.0, 60.0), (0.0, 300.0)], 1);
    assert_eq!(plan(&loads), (1, 1, 0));
}

/// The dimensions with no load gate still reach the split at no requests: a filling cache, and
/// a sustained refusal streak.
#[test]
fn occupancy_and_refusals_still_ask_for_a_split_at_no_requests() {
    let mut caching = Loads {
        shards: 1,
        ..Loads::default()
    };
    caching.fold(&beat(0.0, 0.0, r#","cached_streams":200000"#));
    assert_eq!(plan(&caching), (1, 1, 4), "200,000 / (0.75 x 65,536) -> 5");
    let mut refusing = Loads {
        shards: 1,
        ..Loads::default()
    };
    refusing.fold(&beat(0.0, 0.0, r#","refusal_beats":3"#));
    let at = thresholds();
    let start = Instant::now();
    let mut demand = Demand::default();
    assert_eq!(n(&demand.plan(&refusing, &at, start)), (1, 1, 0));
    assert_eq!(
        n(&demand.plan(&refusing, &at, start + at.cpu_sustain)),
        (1, 1, 1)
    );
}
