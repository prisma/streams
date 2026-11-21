#![cfg(test)]
//! A co-location at the scout's settings (GATE-DECISION-s22-join-2026-11-20 Addendum 13): a 5 s
//! return window (`REBALANCE_RETURN_SECS=5`, the coloc-scout rig's posture) and the co-location
//! DSTs' move cooldown. Over three members the ring splits a sibling pair across two instances, so
//! the target holds its own half at its ring home, with no entry. The target writes nothing for
//! that half (on 379af787f it pinned it, a no-op entry that return-home dropped every window, and
//! the partner waited for it for ever). The partner hands its half giver first, and each holder
//! shields the halves it holds of its policy's merge candidate from its own count balance, lag move
//! and return-home until the merge: the handed half is not returned home, and count balance does
//! not move the target's half, though the target is over its fair share.

use super::fixture_http::HttpRig;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::topology_colocation::{
    MOVE_COOLDOWN_SECS, moved_onto_then_pruned, stored_leaves, unfenced, whole_and_shut,
};
use super::topology_policy::{
    On, document, lines, policy_rig_on, seeded, split_double, store_topology,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// The coloc-scout rig's return window.
const RETURN_SECS: u64 = 5;

/// The scout's fleet settings: its 5 s return window, and the co-location DSTs' move cooldown.
fn scout(fleet: &mut crate::config::FleetConfig) {
    fleet.rebalance_move_cooldown_secs = MOVE_COOLDOWN_SECS;
    fleet.rebalance_return_secs = RETURN_SECS;
}

/// The policy's lines with a 5 s merge sustain and a second's reversal, so the pair is a merge
/// candidate (and shielded) well inside count balance's first cooldown after boot, and six shards
/// an instance at most, so the actor is the target in both cases.
fn quick(cli: &mut crate::config::CliArgs) {
    lines(cli);
    cli.shard_merge_sustain_secs = 5;
    cli.shard_split_reversal_min_secs = 1;
    cli.shards_per_instance_max = 6;
}

/// `instance` over `data` on the scout's settings, its loop never splitting.
async fn member(
    fleet: &Arc<dyn ObjectStore>,
    data: &Arc<dyn ObjectStore>,
    (instance, runtime): (&'static str, RigRuntime),
    leaves: &[&str],
) -> HttpRig {
    let on = On {
        data: data.clone(),
        instance,
        runtime,
        split_sustain_ms: 1_000_000_000,
        fleet: scout,
    };
    let leaves = leaves.iter().map(|leaf| (*leaf).to_string()).collect();
    policy_rig_on(fleet.clone(), on, quick, split_double, leaves)
        .await
        .0
}

/// Three members over the stored topology `leaves`, with a settled desired count of three and no
/// overrides: every leaf is at its ring home.
async fn cell(leaves: &[&str]) -> (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>, [HttpRig; 3]) {
    let (fleet, data) = (seeded().await, mem());
    let a = member(&fleet, &data, ("streams-1", RigRuntime::first()), leaves).await;
    store_topology(&a).await;
    let settled = crate::shard::now_ms() - 120_000;
    let desired = format!(r#"{{"count":3,"epoch":2,"reason":"test","computed_at_ms":{settled}}}"#);
    let path = Path::from("fleet/desired.json");
    fleet.put(&path, PutPayload::from(desired)).await.unwrap();
    let b = member(
        &fleet,
        &data,
        ("streams-2", RigRuntime::incarnation(1)),
        leaves,
    )
    .await;
    let c = member(
        &fleet,
        &data,
        ("streams-3", RigRuntime::incarnation(2)),
        leaves,
    )
    .await;
    (fleet, data, [a, b, c])
}

/// What the overrides document showed for the pair `halves` until the merge published `parent`:
/// whether an entry ever named `home_half` (the target's half at its ring home), and every
/// `rebalance_return` event of a half (a return home). Sampled every 100 ms, within `bound`.
async fn watched(
    (fleet, data): (&Arc<dyn ObjectStore>, &Arc<dyn ObjectStore>),
    parent: &str,
    home_half: &str,
    bound: Duration,
) -> (bool, Vec<String>) {
    let halves = [format!("{parent}0"), format!("{parent}1")];
    let (mut home_named, mut returned) = (false, Vec::new());
    let t0 = tokio::time::Instant::now();
    loop {
        let doc = document(fleet, "overrides").await;
        home_named |= !doc["entries"][home_half].is_null();
        for event in doc["pending_events"].as_array().into_iter().flatten() {
            let shard = event["shard"].as_str().unwrap_or_default().to_string();
            let back = event["event_type"] == "rebalance_return";
            if back && halves.contains(&shard) && !returned.contains(&shard) {
                returned.push(shard);
            }
        }
        let leaves = stored_leaves(data).await;
        if leaves.contains(&parent.to_string()) && !leaves.contains(&halves[0]) {
            return (home_named, returned);
        }
        let decisions = [
            document(fleet, "streams-2").await["topology_decision"].clone(),
            document(fleet, "streams-3").await["topology_decision"].clone(),
        ];
        assert!(
            t0.elapsed() < bound,
            "the pair {parent} merges: {leaves:?}, decisions {decisions:?}, home named \
             {home_named}, returned {returned:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The bound on a co-location here: its three cooldowns, with the 5 s sustain and the setup.
fn bound() -> Duration {
    Duration::from_secs(3 * MOVE_COOLDOWN_SECS + 30)
}

/// **A pair whose target holds its half at its ring home is merged without a pin, its handed half
/// not returned home before the merge** (Addendum 13 (1) and (2)). Three members over the leaves
/// `0` and `1`, no overrides: the ring gives `0` to `streams-2`, the actor, and `1` to `streams-3`.
/// `streams-2` has room (1 + 2), so it is the target and writes nothing; `streams-3` hands `1` to it
/// giver first. With a 5 s return window `streams-2`'s return-home would hand `1` back to its ring
/// home, and its count balance (two shards against a share of one) would move `0`, each before the
/// merge's cooldown: its shield keeps both. Asserted: no entry ever names `0`; no half is returned
/// home before the merge; the merge publishes `""` within three cooldowns of the handoff; the
/// halves' writes are exactly the handoff of `1` to `streams-2`; no handoff was fenced. Red on
/// 379af787f: `streams-2` pins `0` every pass, return-home drops it, `streams-3` waits, no merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_target_holding_its_half_at_its_ring_home_merges_without_a_pin_or_a_return() {
    let (fleet, data, rigs) = cell(&["0", "1"]).await;
    let (home_named, returned) = watched((&fleet, &data), "", "0", bound()).await;
    assert_eq!(
        (home_named, returned),
        (false, Vec::<String>::new()),
        "(an entry named the ring-home half, the halves returned home before the merge)"
    );
    moved_onto_then_pruned(&fleet, "", &[("1", "streams-2")]).await;
    let [a, b, c] = rigs;
    unfenced(&[&a, &b, &c]);
    whole_and_shut(&[&a, &b, &c], &[]).await;
}

/// **Count balance does not move a target's half before the merge, though the target is over its
/// fair share** (Addendum 13 (2), its second case). Three members over `000 001 01 100 101 11`, no
/// overrides: the ring gives `000`, `101` and `11` to `streams-2` (three against a share of two),
/// `001` and `100` to `streams-3`, and `01` to `streams-1`. The deepest pair `00` is split:
/// `streams-2`, its actor, is the target (3 + 2 <= 6) and writes nothing; `streams-3` hands `001`
/// to it. `streams-2`'s count balance would move its first shard, `000`, to `streams-1` once its
/// first cooldown after boot has passed; its shield keeps it until the merge. Asserted as above for
/// the pair `00`: no entry names `000`, no half returned home, the merge of `00` within three
/// cooldowns, the halves' writes exactly the handoff of `001` (no count move of `000`), no fence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn count_balance_does_not_move_a_targets_half_before_the_merge() {
    let (fleet, data, rigs) = cell(&["000", "001", "01", "100", "101", "11"]).await;
    let (home_named, returned) = watched((&fleet, &data), "00", "000", bound()).await;
    assert_eq!(
        (home_named, returned),
        (false, Vec::<String>::new()),
        "(an entry named the ring-home half, the halves returned home before the merge)"
    );
    moved_onto_then_pruned(&fleet, "00", &[("001", "streams-2")]).await;
    let [a, b, c] = rigs;
    unfenced(&[&a, &b, &c]);
    whole_and_shut(&[&a, &b, &c], &[]).await;
}
