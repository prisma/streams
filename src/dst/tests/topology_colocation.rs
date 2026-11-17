#![cfg(test)]
//! The topology policy's merges across instances (S5 + S6 §7.2 step 0, edge #261 B1): a cold pair
//! the fleet split across two instances is moved onto one instance first, the merge's actor, its
//! partner or a third instance with room, and a merge with no room anywhere asks the fleet for an
//! instance. Each scenario runs two or three real instances over one data store and one fleet
//! store, on `topology_policy`'s rigs, and takes minutes (the move's cooldown among it).

use super::fixture_http::engine_shutdown;
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use super::topology_policy::{
    On, document, lines, policy_rig_on, real_split, records, seeded, served, split_double,
    store_topology,
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::time::Duration;

/// The leaves of the cell's stored topology.
async fn stored_leaves(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    let (stored, _) = crate::topology::store::read(&**store).await.unwrap();
    stored.shards.leaves.keys().cloned().collect()
}

/// **A cold pair split across two instances is co-located and merged into one stored leaf**
/// (S6-DESIGN §7.2 step 0, edge #261 B1). `streams-1` alone takes a record on each of 12
/// streams of the cell `0` + `1`; `streams-2` then beats, the ring gives it both halves, and a
/// move (count balance's, as an aged override) gives `1` back to `streams-1`. Both halves go
/// cold past the merge sustain. The merge's actor, `0`'s owner `streams-2`, cannot merge a half
/// another instance holds: it moves `1` to itself, waits out the move's cooldown, and merges. The stored topology ends as the one leaf `""`, its owner serves it, and every stream
/// reads back the same records.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_cold_pair_split_across_two_instances_is_colocated_and_merged() {
    let fleet = seeded().await;
    let data = mem();
    // The merge sustain outlasts the setup, so `streams-1` alone never merges the pair before
    // `streams-2` beats and the pair is split. Nothing splits (a loaded test host can push the
    // committer axis over the line; a split here would only be the double's, in one directory).
    const NEVER: i64 = 1_000_000_000;
    let cold = |cli: &mut crate::config::CliArgs| {
        lines(cli);
        cli.shard_merge_sustain_secs = 20;
        cli.shard_split_reversal_min_secs = 1;
    };
    let halves = vec!["0".to_string(), "1".to_string()];
    let one = On {
        data: data.clone(),
        instance: "streams-1",
        runtime: RigRuntime::first(),
        split_sustain_ms: NEVER,
    };
    let (a, _) = policy_rig_on(fleet.clone(), one, cold, split_double, halves.clone()).await;
    store_topology(&a).await;
    let json = [("content-type", "application/json")];
    let mut kept = Vec::new();
    for i in 0..12 {
        let path = format!("/v1/stream/pair-{i}");
        hreq(a.addr, "PUT", &path, &json, b"").await;
        let body = format!(r#"[{{"stream":{i}}}]"#);
        let (status, _, _) = hreq(a.addr, "POST", &path, &json, body.as_bytes()).await;
        assert_eq!(status, 204, "append to pair-{i}");
        kept.push((
            format!("pair-{i}"),
            records(a.addr, &format!("pair-{i}")).await,
        ));
    }
    // A count of two computed now: the shrink's hysteresis keeps the ring at two members for the
    // test (a count computed at 0 is old enough to shrink at the next tick).
    let now = crate::shard::now_ms();
    let desired = format!(r#"{{"count":2,"epoch":2,"reason":"test","computed_at_ms":{now}}}"#);
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(desired))
        .await
        .unwrap();
    let two = On {
        data: data.clone(),
        instance: "streams-2",
        runtime: RigRuntime::incarnation(1),
        split_sustain_ms: NEVER,
    };
    let (b, _) = policy_rig_on(fleet.clone(), two, cold, split_double, halves).await;
    for rig in [&a, &b] {
        rig.state
            .peer
            .set_peer("streams-1", &format!("http://{}", a.addr));
        rig.state
            .peer
            .set_peer("streams-2", &format!("http://{}", b.addr));
    }
    // The ring gives both halves to `streams-2` (rendezvous over FNV-1a keeps siblings
    // together); count balance's fair-share move split them, as this aged override records.
    let moved_ms = crate::shard::now_ms() - 120_000;
    let split_pair = format!(r#"{{"entries":{{"1":{{"to":"streams-1","ms":{moved_ms}}}}}}}"#);
    fleet
        .put(
            &Path::from("fleet/overrides.json"),
            PutPayload::from(split_pair),
        )
        .await
        .unwrap();
    let owner = |prefix: &str| a.state.ownership.effective_owner(prefix);
    let split = || {
        (owner("0"), owner("1")) == (Some("streams-2".to_string()), Some("streams-1".to_string()))
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !split() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(split(), "the pair is split: {:?}", (owner("0"), owner("1")));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    while stored_leaves(&data).await != [""] && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let decisions = (
        document(&fleet, "streams-1").await["topology_decision"].clone(),
        document(&fleet, "streams-2").await["topology_decision"].clone(),
    );
    assert_eq!(stored_leaves(&data).await, [""], "decisions {decisions:?}");
    moved_onto_then_pruned(&fleet, "", "streams-2").await;
    let root = owner("").expect("the ring owns the merged root");
    let serving = if root == "streams-1" { a.addr } else { b.addr };
    for (name, before) in &kept {
        assert_eq!(
            &served(serving, name).await,
            before,
            "{name} after the merge"
        );
    }
    for rig in [&a, &b] {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
    }
    engine_shutdown(&a.state).await;
    engine_shutdown(&b.state).await;
}

/// Both halves of `parent` were moved onto `to` first, then their entries pruned: of the overrides
/// document's `rebalance_move` events, those whose shard is `<parent>0` or `<parent>1` are exactly
/// `(<parent>0, to)` and `(<parent>1, to)` (filtered by shard: after the merge the parent has no
/// override and count balance may move it; nothing in the rig drains the events, whose drain runs
/// from the telemetry loop this rig never starts). The merge published `parent`, so neither half is
/// a leaf and a beat drops both entries (DESIGN-DECISION-c3-incomplete-rig-2026-11-11 Addendum 3;
/// #251 R3-d, C2): within 10 s the document names neither. An entry is never read after the publish
/// as a fact, and the merged leaf's owner is not asserted: it is the ring's pick, which #251 B1
/// records may move it off `to` (GATE-DECISION-colocation-prune-race-2026-11-17).
async fn moved_onto_then_pruned(fleet: &Arc<dyn ObjectStore>, parent: &str, to: &str) {
    let halves = [format!("{parent}0"), format!("{parent}1")];
    let moved = document(fleet, "overrides").await;
    let mut moves: Vec<(String, String)> = moved["pending_events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|event| event["event_type"] == "rebalance_move")
        .filter(|event| halves.iter().any(|half| event["shard"] == half.as_str()))
        .map(|event| {
            (
                event["shard"].as_str().unwrap_or_default().to_string(),
                event["fields"]["to"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )
        })
        .collect();
    moves.sort();
    let onto = halves.clone().map(|half| (half, to.to_string()));
    assert_eq!(moves, onto, "both halves moved onto {to} first: {moved}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let entries = document(fleet, "overrides").await["entries"].clone();
        if halves.iter().all(|half| entries[half.as_str()].is_null()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a beat drops the entries that name no leaf: {entries}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The records of `name` from whichever of `addrs` serves it (its shard's owner), within 20 s.
async fn served_by_one(addrs: &[std::net::SocketAddr], name: &str) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut answers = Vec::new();
        for addr in addrs {
            let (status, _, body) =
                hreq(*addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
            if status == 200 {
                return serde_json::from_slice(&body).unwrap_or_default();
            }
            answers.push((status, String::from_utf8_lossy(&body).into_owned()));
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{name} is served: {answers:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `n` streams `<name>-<i>` through `addr`, one record each: their names and records.
pub(super) async fn streams_with_a_record(
    addr: std::net::SocketAddr,
    name: &str,
    n: u32,
) -> Vec<(String, serde_json::Value)> {
    let json = [("content-type", "application/json")];
    let mut kept = Vec::new();
    for i in 0..n {
        let path = format!("/v1/stream/{name}-{i}");
        hreq(addr, "PUT", &path, &json, b"").await;
        let body = format!(r#"[{{"stream":{i}}}]"#);
        let (status, _, _) = hreq(addr, "POST", &path, &json, body.as_bytes()).await;
        assert_eq!(status, 204, "append to {name}-{i}");
        kept.push((
            format!("{name}-{i}"),
            records(addr, &format!("{name}-{i}")).await,
        ));
    }
    kept
}

/// A desired count of two computed now, so the shrink's hysteresis holds the ring at two
/// members for the test: the time it was written.
async fn count_of_two(fleet: &Arc<dyn ObjectStore>) -> i64 {
    let now = crate::shard::now_ms();
    let desired = format!(r#"{{"count":2,"epoch":2,"reason":"test","computed_at_ms":{now}}}"#);
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(desired))
        .await
        .unwrap();
    now
}

/// The rigs of the co-location DSTs: the policy's lines with a merge sustain of 20 s (longer than
/// the setup), a second's reversal, and three shards an instance at most.
fn full(cli: &mut crate::config::CliArgs) {
    lines(cli);
    cli.shard_merge_sustain_secs = 20;
    cli.shard_split_reversal_min_secs = 1;
    cli.shards_per_instance_max = 3;
}

/// `full`, with four shards an instance at most: two instances hold eight shards, so only a
/// merge's ask grows the fleet past two.
fn full_of_four(cli: &mut crate::config::CliArgs) {
    full(cli);
    cli.shards_per_instance_max = 4;
}

/// A rig of the co-location DSTs: `instance` over `data` with `cli`, its loop never splitting (a
/// loaded host can push the committer axis over the line; the split double would split one
/// directory only).
async fn full_rig(
    fleet: &Arc<dyn ObjectStore>,
    data: &Arc<dyn ObjectStore>,
    (instance, runtime): (&'static str, RigRuntime),
    cli: fn(&mut crate::config::CliArgs),
    leaves: Vec<String>,
) -> super::fixture_http::HttpRig {
    let on = On {
        data: data.clone(),
        instance,
        runtime,
        split_sustain_ms: 1_000_000_000,
    };
    policy_rig_on(fleet.clone(), on, cli, split_double, leaves)
        .await
        .0
}

/// Wait, within 30 s, until `rig` sees every leaf of `layout` owned as it says.
async fn laid_out(rig: &super::fixture_http::HttpRig, layout: &[(&str, &str)]) {
    let owner = |prefix: &str| rig.state.ownership.effective_owner(prefix);
    let done = || {
        layout
            .iter()
            .all(|(p, to)| owner(p).as_deref() == Some(*to))
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !done() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let owners: Vec<_> = layout.iter().map(|(p, _)| owner(p)).collect();
    assert!(done(), "the layout: {owners:?}");
}

/// Wait, within `secs`, until the stored topology has `parent` as a leaf and not its `…0`; whether
/// it did, and the instances' decisions when it did not.
async fn merged_within(
    data: &Arc<dyn ObjectStore>,
    fleet: &Arc<dyn ObjectStore>,
    parent: &str,
    secs: u64,
) -> Result<(), String> {
    let merged = || async {
        let leaves = stored_leaves(data).await;
        leaves.contains(&parent.to_string()) && !leaves.contains(&format!("{parent}0"))
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !merged().await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if merged().await {
        return Ok(());
    }
    let mut decisions = Vec::new();
    for name in ["streams-1", "streams-2", "streams-3"] {
        decisions.push(document(fleet, name).await["topology_decision"].clone());
    }
    Err(format!(
        "decisions {decisions:?}, leaves {:?}",
        stored_leaves(data).await
    ))
}

/// Every stream of `kept` reads back its records from whichever of `rigs` serves it, and the rigs
/// shut down cleanly.
async fn whole_and_shut(
    rigs: &[&super::fixture_http::HttpRig],
    kept: &[(String, serde_json::Value)],
) {
    let addrs: Vec<_> = rigs.iter().map(|rig| rig.addr).collect();
    for (name, before) in kept {
        assert_eq!(&served_by_one(&addrs, name).await, before, "{name}");
    }
    for rig in rigs {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// **A split pair whose actor is full is merged on its partner** (#261 B1, package 1's finding).
/// With `SHARDS_PER_INSTANCE_MAX` 3, the cell `00 01 10 11` sits as moves left it: `streams-1`
/// serves `00`, `10` and `11`, `streams-2` serves `01`. The pair `0` is cold. Its actor, `00`'s
/// owner `streams-1`, has no room for `01` and the merged leaf (3 + 2 > 3); its partner, `01`'s
/// owner `streams-2`, has (1 + 2 = 3). The actor moves both halves onto the partner, which then owns
/// `00`, is the actor, and merges: `0` is a leaf of the stored topology, both halves moved to
/// `streams-2` and their entries are then dropped, and every stream reads back the records it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_pair_whose_actor_is_full_is_merged_on_its_partner() {
    let (fleet, data) = (seeded().await, mem());
    let leaves = || ["00", "01", "10", "11"].map(str::to_string).to_vec();
    let a = full_rig(
        &fleet,
        &data,
        ("streams-1", RigRuntime::first()),
        full,
        leaves(),
    )
    .await;
    store_topology(&a).await;
    let kept = streams_with_a_record(a.addr, "full", 16).await;
    let now = count_of_two(&fleet).await;
    let b = full_rig(
        &fleet,
        &data,
        ("streams-2", RigRuntime::incarnation(1)),
        full,
        leaves(),
    )
    .await;
    // The ring gives `00 01` to streams-1 and `10 11` to streams-2; aged moves (count balance's)
    // left 01 on streams-2 and 10, 11 on streams-1.
    let layout = [
        ("00", "streams-1"),
        ("01", "streams-2"),
        ("10", "streams-1"),
        ("11", "streams-1"),
    ];
    pinned(&fleet, &layout[1..], now).await;
    laid_out(&a, &layout).await;
    merged_within(&data, &fleet, "0", 300).await.unwrap();
    moved_onto_then_pruned(&fleet, "0", "streams-2").await;
    whole_and_shut(&[&a, &b], &kept).await;
}

/// Every leaf of `layout` pinned to its instance by an override aged two minutes, as moves left them.
async fn pinned(fleet: &Arc<dyn ObjectStore>, layout: &[(&str, &str)], now: i64) {
    let ms = now - 120_000;
    let entries: serde_json::Map<String, serde_json::Value> = layout
        .iter()
        .map(|(prefix, to)| {
            (
                (*prefix).to_string(),
                serde_json::json!({"to": to, "ms": ms}),
            )
        })
        .collect();
    let doc = serde_json::json!({ "entries": entries }).to_string();
    fleet
        .put(&Path::from("fleet/overrides.json"), PutPayload::from(doc))
        .await
        .unwrap();
}

/// **A split pair with no room on either side asks for an instance and is merged there** (#261
/// B1). With `SHARDS_PER_INSTANCE_MAX` 4 and every leaf where moves pinned it, `streams-1` serves
/// `000 010 011 100` and `streams-2` serves `001 101 110 111`: both full, and two instances hold
/// every shard. The pair `00`, first in the policy's order, is cold. Neither its actor nor its
/// partner has room (4 + 2 > 4), and there is no third instance, so the merge asks the fleet for
/// one, as a split does: the actor's beat says `instance_full shards_per_instance=4/4`, and the
/// fleet's one-shard allowance grows the desired count past two. `streams-3` then beats, empty; it
/// has room for both halves and the merged leaf (0 + 3 within 4), takes both, is the actor, and
/// merges: `00` is a leaf of the stored topology, both halves moved to `streams-3` and their entries
/// are then dropped, and every stream reads back the records it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_pair_with_no_room_on_either_side_asks_for_an_instance_and_is_merged_there() {
    let (fleet, data) = (seeded().await, mem());
    // Eight leaves of one depth (a cell's first leaves take their homes from their bits), four on
    // each instance: the pairs `00` and `10` split across them, `01` and `11` whole.
    let layout = [
        ("000", "streams-1"),
        ("010", "streams-1"),
        ("011", "streams-1"),
        ("100", "streams-1"),
        ("001", "streams-2"),
        ("101", "streams-2"),
        ("110", "streams-2"),
        ("111", "streams-2"),
    ];
    // In bit order: a rig numbers a cell's first homes in the order it is given its leaves, and
    // a log it opens takes its home from its prefix's bits, so the two must agree.
    let leaves = || {
        let mut leaves = layout.map(|(prefix, _)| prefix.to_string()).to_vec();
        leaves.sort();
        leaves
    };
    let a = full_rig(
        &fleet,
        &data,
        ("streams-1", RigRuntime::first()),
        full_of_four,
        leaves(),
    )
    .await;
    store_topology(&a).await;
    let kept = streams_with_a_record(a.addr, "none", 16).await;
    let now = count_of_two(&fleet).await;
    let b = full_rig(
        &fleet,
        &data,
        ("streams-2", RigRuntime::incarnation(1)),
        full_of_four,
        leaves(),
    )
    .await;
    pinned(&fleet, &layout, now).await;
    laid_out(&a, &layout).await;

    // The ask is the actor's: its beat says why the merge waits (after the fleet's 60 s wait on
    // the count this test published); the count then grows.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut beat = document(&fleet, "streams-1").await;
    let asks = |beat: &serde_json::Value| {
        beat["topology_decision"]
            .as_str()
            .is_some_and(|d| d.starts_with("instance_full"))
    };
    while !asks(&beat) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        beat = document(&fleet, "streams-1").await;
    }
    assert_eq!(
        beat["topology_decision"], "instance_full shards_per_instance=4/4",
        "the merge asks for an instance: {beat}"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut desired = document(&fleet, "desired").await;
    while desired["count"].as_u64() < Some(3) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        desired = document(&fleet, "desired").await;
    }
    let count = desired["count"].as_u64();
    assert!(count >= Some(3), "the fleet grows for the merge: {desired}");
    let c = full_rig(
        &fleet,
        &data,
        ("streams-3", RigRuntime::incarnation(2)),
        full_of_four,
        leaves(),
    )
    .await;
    merged_within(&data, &fleet, "00", 300).await.unwrap();
    moved_onto_then_pruned(&fleet, "00", "streams-3").await;
    whole_and_shut(&[&a, &b, &c], &kept).await;
}

/// **Return-home skips a live intent's sources** (S4-DESIGN §4.4: the intent is a move reservation
/// that return-home and the rebalancer skip; Fable 5.2's B4-c). On two instances the ring gives
/// `00 01` to `streams-1` and `10 11` to `streams-2`; an override 400 s old (past
/// `REBALANCE_RETURN_SECS`) keeps `00` on `streams-2`, and the fair share admits its return home
/// (1 + 1 within 2). A merge's intent names `00` and `01` as its sources and stays live (renewed).
/// For as long as it is live, return-home leaves `00`'s override; once it ends, return-home
/// drops the override and `00` goes home.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn return_home_skips_a_live_intents_sources() {
    use crate::topology::intent::{self, Intent, State};
    let (fleet, data) = (seeded().await, mem());
    let leaves = || ["00", "01", "10", "11"].map(str::to_string).to_vec();
    let a = full_rig(
        &fleet,
        &data,
        ("streams-1", RigRuntime::first()),
        lines,
        leaves(),
    )
    .await;
    store_topology(&a).await;
    let now = count_of_two(&fleet).await;
    let b = full_rig(
        &fleet,
        &data,
        ("streams-2", RigRuntime::incarnation(1)),
        lines,
        leaves(),
    )
    .await;
    let snapshot = a.state.shards.prefixes();
    let live = Intent {
        kind: "merge".to_string(),
        op_id: "op-reserved".to_string(),
        sources: vec![
            ("00".to_string(), snapshot.shards.leaves["00"]),
            ("01".to_string(), snapshot.shards.leaves["01"]),
        ],
        children: vec![snapshot.shards.next_home],
        version_seen: snapshot.shards.version,
        state: State::Preparing,
        lease_owner: "streams-2".to_string(),
        lease_epoch: 1,
        lease_until_ms: i64::MAX,
        started_ms: 0,
        outcome: None,
    };
    let mut held = intent::create(&*data, &live).await.unwrap();
    // Both instances' beats have read the intent before the aged override lands.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let seen = || {
        [&a, &b].iter().all(|rig| {
            rig.state
                .fleet
                .standing()
                .topology_sources()
                .contains(&"00".to_string())
        })
    };
    while !seen() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(seen(), "both beats read the live intent");
    let moved = format!(
        r#"{{"entries":{{"00":{{"to":"streams-2","ms":{}}}}}}}"#,
        now - 400_000
    );
    fleet
        .put(&Path::from("fleet/overrides.json"), PutPayload::from(moved))
        .await
        .unwrap();
    laid_out(&a, &[("00", "streams-2"), ("01", "streams-1")]).await;
    // Renewed every second for 20 s (a live actor's lease), the override is never returned.
    let overridden =
        || async { document(&fleet, "overrides").await["entries"]["00"]["to"].clone() };
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut next = held.intent.clone();
        next.started_ms += 1;
        held = intent::replace(&*data, &held, next).await.unwrap();
        assert_eq!(
            overridden().await,
            "streams-2",
            "a live intent's source keeps its override"
        );
    }
    let mut ended = held.intent.clone();
    ended.state = State::RolledBack;
    intent::replace(&*data, &held, ended).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while overridden().await == "streams-2" && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        overridden().await.is_null(),
        "once the intent ended the override is returned home"
    );
    for rig in [&a, &b] {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        engine_shutdown(&rig.state).await;
    }
}

/// The rigs of the room DST: `full`'s three shards an instance, and no shrink for an hour, so a
/// desired count past S4's 60 s settle holds the ring at two members.
fn full_and_held(cli: &mut crate::config::CliArgs) {
    full(cli);
    cli.scale_in_secs = 3_600;
}

/// `n` streams whose routes land in the leaf `0` of `rig`'s cell, each created: their names.
async fn streams_in_0(rig: &super::fixture_http::HttpRig, n: usize) -> Vec<String> {
    use super::billing_closure_debts::{JSON, raw_put, stored};
    let mut names = Vec::new();
    for taken in 0..400 {
        let name = format!("room-{taken}");
        raw_put(rig.addr, &name, &[JSON], b"").await;
        let desc = stored(&rig.state, &rig.state.deployment.raw_adapter_sref(&name)).await;
        if rig
            .state
            .shards
            .prefix_for(&desc.resolve_segment("").shard_route)
            == "0"
        {
            names.push(name);
        }
        if names.len() == n {
            return names;
        }
    }
    panic!("no {n} streams land in 0");
}

/// The room DST's cell: the fleet and data stores, and `streams-1` and `streams-2`, peers of each
/// other, once `streams-1` holds both leaves by their aged entries.
async fn room_cell() -> (
    Arc<dyn ObjectStore>,
    Arc<dyn ObjectStore>,
    super::fixture_http::HttpRig,
    super::fixture_http::HttpRig,
) {
    let fleet = seeded().await;
    let data = mem();
    let settled = crate::shard::now_ms() - 120_000;
    let desired = format!(r#"{{"count":2,"epoch":2,"reason":"test","computed_at_ms":{settled}}}"#);
    let desired_path = Path::from("fleet/desired.json");
    fleet
        .put(&desired_path, PutPayload::from(desired))
        .await
        .unwrap();
    let pinned = format!(
        r#"{{"entries":{{"0":{{"to":"streams-1","ms":{settled}}},"1":{{"to":"streams-1","ms":{settled}}}}}}}"#
    );
    let overrides_path = Path::from("fleet/overrides.json");
    fleet
        .put(&overrides_path, PutPayload::from(pinned))
        .await
        .unwrap();
    let halves = vec!["0".to_string(), "1".to_string()];
    let on = |instance, runtime| On {
        data: data.clone(),
        instance,
        runtime,
        split_sustain_ms: 1_000,
    };
    let one = on("streams-1", RigRuntime::first());
    let (a, _) = policy_rig_on(
        fleet.clone(),
        one,
        full_and_held,
        real_split,
        halves.clone(),
    )
    .await;
    store_topology(&a).await;
    let two = on("streams-2", RigRuntime::incarnation(1));
    let (b, _) = policy_rig_on(fleet.clone(), two, full_and_held, real_split, halves).await;
    for rig in [&a, &b] {
        rig.state
            .peer
            .set_peer("streams-1", &format!("http://{}", a.addr));
        rig.state
            .peer
            .set_peer("streams-2", &format!("http://{}", b.addr));
    }
    laid_out(&a, &[("0", "streams-1"), ("1", "streams-1")]).await;
    (fleet, data, a, b)
}

/// What the room DST saw while it loaded `0` until the stored leaves were `00`, `01`, `1`: the
/// records acked per stream, the entries of `0` and `1` when `0` was first seen moved to
/// `streams-2` (read then: once `0` splits it is no leaf, and a stale prune may drop its entry), the
/// `instance_full` beats after the move, and each desired count in turn.
struct Loaded {
    acked: std::collections::BTreeMap<String, u64>,
    moved_entries: Option<(serde_json::Value, serde_json::Value)>,
    asked_after: usize,
    counts: Vec<Option<u64>>,
}

/// Append to `0`'s streams at `a` 20 a second until the split of `0` published (see `Loaded`).
async fn load_until_split(
    fleet: &Arc<dyn ObjectStore>,
    data: &Arc<dyn ObjectStore>,
    a: &super::fixture_http::HttpRig,
) -> Loaded {
    let names = streams_in_0(a, 8).await;
    let json = [("content-type", "application/json")];
    let mut acked: std::collections::BTreeMap<String, u64> = Default::default();
    let (mut n, mut moved, mut asked_after, mut counts) = (0_u64, false, Vec::new(), Vec::new());
    let mut moved_entries = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    while stored_leaves(data).await != ["00", "01", "1"] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "leaves {:?}, moved {moved}, desired counts {counts:?}, decision {}",
            stored_leaves(data).await,
            document(fleet, "streams-1").await["topology_decision"]
        );
        let name = &names[usize::try_from(n).unwrap() % names.len()];
        let body = format!(r#"[{{"n":{n}}}]"#);
        let (status, _, _) = hreq(
            a.addr,
            "POST",
            &format!("/v1/stream/{name}"),
            &json,
            body.as_bytes(),
        )
        .await;
        if status == 204 {
            *acked.entry(name.clone()).or_default() += 1;
        }
        n += 1;
        let count = document(fleet, "desired").await["count"].as_u64();
        if counts.last() != Some(&count) {
            counts.push(count);
        }
        let entries = document(fleet, "overrides").await["entries"].clone();
        if !moved && entries["0"]["to"] == "streams-2" {
            moved = true;
            moved_entries = Some((entries["0"]["to"].clone(), entries["1"]["to"].clone()));
        }
        let refused = document(fleet, "streams-1").await["split_refused"].clone();
        if moved && refused == "instance_full" {
            asked_after.push(refused);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Loaded {
        acked,
        moved_entries,
        asked_after: asked_after.len(),
        counts,
    }
}

/// **A split refused for room moves its shard to an instance with room**
/// (GATE-DECISION-f2-convergence-2026-11-17). Two members, three shards an instance at most:
/// `streams-1` holds both leaves `0` and `1` by entries older than the move cooldown, `streams-2`
/// none, and only `0` takes load over the split line. `streams-1`'s split of `0` is refused
/// `instance_full` (2 + 2 > 3); since `streams-2` has room for `0` and its two children, the policy
/// moves `0` there by one overrides CAS instead of asking for an instance. Exactly: when the move is
/// first seen, the entry of `0` names `streams-2` and `1`'s still names `streams-1`; the desired count stays two throughout;
/// no beat of `streams-1` says `instance_full` after the move; once the move's cooldown has passed
/// `streams-2` splits `0`, so the stored leaves are `00`, `01` and `1` with `00` and `01` on
/// `streams-2`; and every acked record reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_refused_for_room_moves_its_shard_to_an_instance_with_room() {
    let (fleet, data, a, b) = room_cell().await;
    let Loaded {
        acked,
        moved_entries,
        asked_after,
        counts,
    } = load_until_split(&fleet, &data, &a).await;
    let (to_0, to_1) = moved_entries.expect("0 moved to streams-2");
    let owner = |p: &str| b.state.ownership.effective_owner(p).unwrap_or_default();
    assert_eq!(
        (
            to_0.as_str(),
            to_1.as_str(),
            counts,
            asked_after,
            [owner("00"), owner("01")]
        ),
        (
            Some("streams-2"),
            Some("streams-1"),
            vec![Some(2)],
            0,
            ["streams-2", "streams-2"].map(str::to_string)
        ),
        "the move, the count, no ask after it, and the children's owner"
    );
    for (name, count) in &acked {
        let read = served(b.addr, name).await;
        let got = read.as_array().map_or(0, Vec::len);
        assert_eq!(
            u64::try_from(got).unwrap(),
            *count,
            "{name} reads back every acked record"
        );
    }
    for rig in [&a, &b] {
        let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
    }
    engine_shutdown(&a.state).await;
    engine_shutdown(&b.state).await;
}
