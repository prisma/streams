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

/// The partner's merge sustain in case 2: long enough that the target shields the pair, and its
/// count balance gets its chance, before the partner names the pair and hands its half.
const PARTNER_SUSTAIN_SECS: u64 = 60;

/// `quick`, with the partner's long merge sustain ([`PARTNER_SUSTAIN_SECS`]).
fn slow_partner(cli: &mut crate::config::CliArgs) {
    quick(cli);
    cli.shard_merge_sustain_secs = PARTNER_SUSTAIN_SECS;
}

/// `instance` over `data` on the scout's settings and `cli`, its loop never splitting.
async fn member(
    fleet: &Arc<dyn ObjectStore>,
    data: &Arc<dyn ObjectStore>,
    (instance, runtime): (&'static str, RigRuntime),
    leaves: &[&str],
    cli: fn(&mut crate::config::CliArgs),
) -> HttpRig {
    let on = On {
        data: data.clone(),
        instance,
        runtime,
        split_sustain_ms: 1_000_000_000,
        fleet: scout,
    };
    let leaves = leaves.iter().map(|leaf| (*leaf).to_string()).collect();
    policy_rig_on(fleet.clone(), on, cli, split_double, leaves)
        .await
        .0
}

/// Three members over the stored topology `leaves`, listed with their boot ids before they boot,
/// with a settled desired count of three and no overrides: every leaf is at its ring home.
/// `streams-3`, each case's partner, runs `partner`.
async fn cell(
    leaves: &[&str],
    partner: fn(&mut crate::config::CliArgs),
) -> (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>, [HttpRig; 3]) {
    let (fleet, data) = (seeded().await, mem());
    // The count is three before anyone boots: at `seeded`'s one, `streams-1`'s first pass would
    // unlist the other two as beyond the count, and they would join again with pins.
    let settled = crate::shard::now_ms() - 120_000;
    let desired = format!(r#"{{"count":3,"epoch":2,"reason":"test","computed_at_ms":{settled}}}"#);
    let path = Path::from("fleet/desired.json");
    fleet.put(&path, PutPayload::from(desired)).await.unwrap();
    let runtimes = [
        RigRuntime::first(),
        RigRuntime::incarnation(1),
        RigRuntime::incarnation(2),
    ];
    // Setup only (the join ruling, Addendum 23 §2 (b)): the three are listed with their boot ids
    // before they boot, so the cell starts converged and no join pin names a half.
    let listed: Vec<(&str, &RigRuntime)> = ["streams-1", "streams-2", "streams-3"]
        .into_iter()
        .zip(&runtimes)
        .collect();
    super::fixture_members::seed_members(&fleet, &listed).await;
    let [first, second, third] = runtimes;
    let a = member(&fleet, &data, ("streams-1", first), leaves, quick).await;
    store_topology(&a).await;
    let b = member(&fleet, &data, ("streams-2", second), leaves, quick).await;
    let c = member(&fleet, &data, ("streams-3", third), leaves, partner).await;
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
    let (fleet, data, rigs) = cell(&["0", "1"], quick).await;
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
/// `streams-2`, its actor, is the target (3 + 2 <= 6) and writes nothing; `streams-3`, the partner,
/// hands `001` to it once its own 60 s merge sustain has passed. Meanwhile `streams-2` shields the
/// pair, and once it does its count balance is let past its first hold (the rig's clock advanced
/// past one move cooldown; no override is young): it would move its first shard, `000`, to
/// `streams-1`, and its shield keeps it, so it moves its next, `101`, there. Asserted as above for
/// the pair `00`: no entry names `000`, no half returned home, the merge of `00` within three
/// cooldowns of the handoff, the halves' writes exactly the handoff of `001` (no count move of
/// `000`), no fence; and count balance's move of `101` to `streams-1`, so it ran and chose.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn count_balance_does_not_move_a_targets_half_before_the_merge() {
    let (fleet, data, rigs) = cell(&["000", "001", "01", "100", "101", "11"], slow_partner).await;
    let target = &rigs[1];
    let t0 = tokio::time::Instant::now();
    while !target.state.fleet.standing().shielded("000") {
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "the target shields the pair"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    target
        .clock
        .advance_monotonic(Duration::from_secs(MOVE_COOLDOWN_SECS + 1));
    let partner_bound = Duration::from_secs(PARTNER_SUSTAIN_SECS) + bound();
    let (home_named, returned) = watched((&fleet, &data), "00", "000", partner_bound).await;
    assert_eq!(
        (home_named, returned),
        (false, Vec::<String>::new()),
        "(an entry named the ring-home half, the halves returned home before the merge)"
    );
    // Count balance ran with the shield up and chose: its first movable shard, `101`, went to
    // `streams-1`. So `000` stayed because the shield kept it, not because count balance never ran.
    let events = document(&fleet, "overrides").await["pending_events"].clone();
    let counted = |event: &serde_json::Value| {
        let to = &event["fields"]["to"];
        event["event_type"] == "rebalance_move" && event["shard"] == "101" && to == "streams-1"
    };
    let moved_101 = events.as_array().into_iter().flatten().any(counted);
    assert!(
        moved_101,
        "count balance moved 101 with the shield up: {events}"
    );
    moved_onto_then_pruned(&fleet, "00", &[("001", "streams-2")]).await;
    let [a, b, c] = rigs;
    unfenced(&[&a, &b, &c]);
    whole_and_shut(&[&a, &b, &c], &[]).await;
}
