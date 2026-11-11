#![cfg(test)]
//! The fleet and the topology (S4-DESIGN §10.1): every member's heartbeat names the cell's live
//! topology intent, which the fleet's planner reads to hold off a scale-in while a split or merge
//! runs (`fleet::planning`'s shrink sentinel).

use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::topology::intent::{self, Intent, State};
use std::time::{Duration, Instant};

type App = std::sync::Arc<crate::http::AppState>;

/// How long a beat naming the intent's change may take: a few 2 s beat periods.
const BOUND: Duration = Duration::from_secs(10);

/// Wait for a heartbeat `streams-1` publishes after `since_ms` whose `topology_intent` is `want`
/// and that reserves `reserved` shards.
async fn beat_naming(state: &App, since_ms: i64, want: Option<&str>, reserved: u32) {
    let t0 = Instant::now();
    loop {
        let beats = state.fleet.peek_heartbeat_set().await.unwrap_or_default();
        let found = beats.into_iter().any(|b| {
            b.instance == "streams-1"
                && b.ts_ms > since_ms
                && b.topology_intent.as_deref() == want
                && b.reserved_shards == reserved
        });
        if found {
            return;
        }
        assert!(t0.elapsed() < BOUND, "no beat named {want:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **A member's heartbeat names the live topology intent until it ends, and reserves two shards
/// for a split it runs itself** (S4-DESIGN §4.4, §10.1). A fleet instance beats with no intent;
/// once another instance's live split intent is written its beats name the operation and reserve
/// nothing; once that intent names this instance as its actor they reserve two; once it is rolled
/// back they name none and reserve nothing again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_members_heartbeat_names_the_live_topology_intent_until_it_ends() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(mem()),
            instance: Some("streams-1".into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let state = &rig.state;
    beat_naming(state, 0, None, 0).await;
    let live = Intent {
        kind: "split".to_string(),
        op_id: "op-beat".to_string(),
        sources: vec![("00".to_string(), 0)],
        children: vec![90, 91],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-a".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    let held = intent::create(&*state.data_store, &live).await.unwrap();
    beat_naming(state, crate::shard::now_ms(), Some("op-beat"), 0).await;
    let mut ours = live;
    ours.lease_owner = "streams-1".to_string();
    let held = intent::replace(&*state.data_store, &held, ours.clone())
        .await
        .unwrap();
    beat_naming(state, crate::shard::now_ms(), Some("op-beat"), 2).await;
    let mut ended = ours;
    ended.state = State::RolledBack;
    intent::replace(&*state.data_store, &held, ended)
        .await
        .unwrap();
    beat_naming(state, crate::shard::now_ms(), None, 0).await;
    engine_shutdown(state).await;
}

/// **A source of the live topology intent is never the lag rebalancer's shard** (S4-DESIGN §4.4).
/// An instance serving one shard offers it as its most wedged; once its beat read an intent naming
/// that shard as a source, it offers none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_source_is_never_the_lag_rebalancers_shard() {
    let (state, _addr) = super::fixture_http::http_rig(mem()).await;
    let leaf = state
        .shards
        .prefixes()
        .shards
        .leaves
        .keys()
        .next()
        .cloned()
        .unwrap();
    let route = crate::topology::ops::route_of(&leaf);
    let adoption = crate::shard_directory::Adoption::Internal;
    state.shards.resolve(&route, adoption).await.unwrap();
    assert_eq!(state.fleet_pressure().0, leaf, "its one shard");
    state
        .fleet
        .standing()
        .set_topology_sources(vec![leaf.clone()]);
    assert_eq!(state.fleet_pressure().0, "", "a split's source");
    engine_shutdown(&state).await;
}

/// **Nor is it the lag move's most stalled shard** (S4-DESIGN §4.4): an instance holding one shard
/// gives it up to a lag move; once its beat read an intent naming that shard as a source, it gives
/// up none, stalled or wedged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_live_intents_source_is_never_the_lag_moves_shard() {
    let (state, _addr) = super::fixture_http::http_rig(mem()).await;
    let leaf = state
        .shards
        .prefixes()
        .shards
        .leaves
        .keys()
        .next()
        .cloned()
        .unwrap();
    let route = crate::topology::ops::route_of(&leaf);
    let adoption = crate::shard_directory::Adoption::Internal;
    state.shards.resolve(&route, adoption).await.unwrap();
    assert_eq!(state.lag_victim("", 0), Some(leaf.clone()), "its one shard");
    state
        .fleet
        .standing()
        .set_topology_sources(vec![leaf.clone()]);
    let (wedge_prefix, wedge_max_ms, _) = state.fleet_pressure();
    assert_eq!(
        state.lag_victim(&wedge_prefix, wedge_max_ms),
        None,
        "a live intent's source"
    );
    engine_shutdown(&state).await;
}

/// The aged overrides of the two halves a merge into `1` left behind, both naming `target`:
/// 120 s old, past the 60 s move cooldown and short of return-home's 300 s, so the tick keeps them.
async fn seed_stale_halves(fleet: &std::sync::Arc<dyn object_store::ObjectStore>, target: &str) {
    use object_store::ObjectStoreExt;
    let ms = crate::shard::now_ms() - 120_000;
    let entry = || crate::fleet::OverrideEntry {
        to: target.to_string(),
        ms,
    };
    let overrides = crate::fleet::Overrides {
        entries: [("10".to_string(), entry()), ("11".to_string(), entry())]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let raw = serde_json::to_vec(&overrides).unwrap();
    fleet
        .put(&"fleet/overrides.json".into(), raw.into())
        .await
        .unwrap();
    let desired = br#"{"count":1,"epoch":1,"reason":"seed","computed_at_ms":0}"#.to_vec();
    fleet
        .put(&"fleet/desired.json".into(), desired.into())
        .await
        .unwrap();
}

/// Whether any object of `state`'s data store lies under `root`.
async fn any_object_under(state: &App, root: &str) -> bool {
    use futures_util::StreamExt;
    let prefix = object_store::path::Path::from(root);
    state.data_store.list(Some(&prefix)).next().await.is_some()
}

/// The overrides document's keys, sorted.
async fn override_keys(state: &App) -> Vec<String> {
    let (overrides, _) = state.fleet.read_overrides().await.unwrap();
    let mut keys: Vec<String> = overrides.entries.into_keys().collect();
    keys.sort();
    keys
}

/// One case of the test below: the stale halves name `target`.
async fn stale_halves_open_nothing(target: &str) {
    let fleet = mem();
    seed_stale_halves(&fleet, target).await;
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            prefixes: ["00", "01", "1"].map(str::to_string).to_vec(),
            ..Default::default()
        },
    )
    .await;
    let state = &rig.state;
    let opened = state
        .shards
        .open_or_wait("00", Duration::from_secs(5))
        .await;
    assert!(
        matches!(opened, crate::sharddir::OpenOutcome::Ready(_)),
        "{target}: 00 opens"
    );
    assert!(crate::fleet::start_configured(state.clone(), &rig.tasks));
    let t0 = Instant::now();
    while state.ownership.ring_active().is_empty() {
        assert!(
            t0.elapsed() < BOUND,
            "{target}: the tick publishes the ring"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Four ticks, past the 3 s holdoff a retirement arms.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let mut held = state.shards.held_prefixes();
    held.sort();
    assert_eq!(held, ["00"], "{target}: only the leaf it opened is held");
    for root in ["shards/10/", "shards/11/"] {
        assert!(
            !any_object_under(state, root).await,
            "{target}: no log at {root}"
        );
    }
    // The stored topology, the prune's authority: a beat then drops both entries.
    let topology = crate::topology::model::Topology {
        version: 1,
        shards: state.shards.prefixes().shards.clone(),
        last_op: None,
        max_request_body_bytes: None,
    };
    let raw = serde_json::to_vec(&topology).unwrap();
    {
        use object_store::ObjectStoreExt;
        state
            .data_store
            .put(&"topology.json".into(), raw.into())
            .await
            .unwrap();
    }
    let t0 = Instant::now();
    while !override_keys(state).await.is_empty() && t0.elapsed() < BOUND {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        override_keys(state).await,
        Vec::<String>::new(),
        "{target}: a beat drops the entries that name no leaf"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(state).await;
}

/// **The fleet tick opens no override whose prefix is no leaf, and a beat drops such an entry**
/// (#251 amendment R3-d). The cell's leaves are `00`, `01` and `1`, and the overrides still name
/// `10` and `11`, the halves a merge into `1` left behind, aged past the move cooldown. In both
/// forms the ring's pick of each half is this instance: its target is this instance (the merge's
/// actor), or an instance outside the ring. Past the holdoff, the only engine held is the leaf
/// `00` it opened, and no log exists at `shards/10` or `shards/11`. Once the topology is stored
/// (the prune's authority), a beat drops both entries from `fleet/overrides.json`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_fleet_tick_opens_no_override_whose_prefix_is_no_leaf() {
    for target in ["streams-1", "streams-2"] {
        stale_halves_open_nothing(target).await;
    }
}
