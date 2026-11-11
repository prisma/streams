#![cfg(test)]
//! A split of a leaf the fleet's count balance moved (edge #251 amendment R3, the split's twin of
//! S6's B4): count balance places a leaf by an override in `fleet/overrides.json`, and return-home
//! drops it only once it ages past `rebalance_return_secs`. A split start that refused any
//! override held such a leaf unsplit that long under any load (S5's policy holding
//! `override_recent`), which under #149's ramp, where count balance moves leaves all the time,
//! stalls growth. An override is no refusal of its own: one placing the leaf elsewhere answers
//! `not_ring_owner` when the leaf resolves, one younger than the move cooldown is the fleet's wait
//! (`fleet_settled`), and one placing it here lets the split run here.

use super::fixture_http::engine_shutdown;
use super::topology_policy::{destination, leaves, lines, load, policy_rig, real_split, seeded};
use object_store::{ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// **A leaf count balance moved here splits here under sustained load.** A one-shard cell run by
/// `streams-1`, whose leaf `""` carries an aged override naming `streams-1` (as count balance's
/// move leaves it, past the move cooldown and short of return-home). Under appends past the split
/// line the policy splits it through S4's split into `0` and `1` within 30 s, under the override;
/// then a beat drops the parent's entry, which names no leaf (#251 amendment R3-d).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_leaf_the_balancer_moved_here_splits_here_under_load() {
    let fleet = seeded().await;
    destination(&fleet).await;
    let moved_ms = crate::shard::now_ms() - 120_000;
    let moved = format!(r#"{{"entries":{{"":{{"to":"streams-1","ms":{moved_ms}}}}}}}"#);
    fleet
        .put(&Path::from("fleet/overrides.json"), PutPayload::from(moved))
        .await
        .unwrap();
    let (rig, asked) = policy_rig(
        fleet.clone(),
        |cli| {
            cli.fleet_max = 1;
            lines(cli);
        },
        real_split,
    )
    .await;
    let topology = crate::topology::model::Topology {
        version: 1,
        shards: rig.state.shards.prefixes().shards.clone(),
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    rig.state
        .data_store
        .put(&Path::from("topology.json"), raw.into())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !rig.state.ownership.overrides().contains_key("") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the override reaches the ring"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let split = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while leaves(&rig.state).len() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        stop.store(true, Ordering::Relaxed);
    };
    tokio::join!(load(rig.addr, 20, stop.clone()), split);
    assert_eq!(
        leaves(&rig.state),
        ["0", "1"],
        "asked {:?}",
        asked.lock().unwrap()
    );
    assert_eq!(*asked.lock().unwrap(), [""]);
    // The parent's entry names no leaf once the split publishes: a beat drops it (#251 R3-d).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let parent_entry = || async {
        let (overrides, _) = rig.state.fleet.read_overrides().await.unwrap();
        overrides.entries.contains_key("")
    };
    while parent_entry().await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !parent_entry().await,
        "a beat drops the split parent's entry"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}
