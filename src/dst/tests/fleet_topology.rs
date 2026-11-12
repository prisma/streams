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

/// The objects of `state`'s data store under `root`, with their sizes, sorted: an open of a log
/// there (its fence, a new manifest) changes it.
async fn objects_under(state: &App, root: &str) -> Vec<(String, u64)> {
    use futures_util::TryStreamExt;
    let prefix = object_store::path::Path::from(root);
    let listed = state.data_store.list(Some(&prefix));
    let mut objects: Vec<(String, u64)> = listed
        .map_ok(|meta| (meta.location.to_string(), meta.size))
        .try_collect()
        .await
        .unwrap();
    objects.sort();
    objects
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

/// **A beat opens no live intent's source that another instance owns** (TLA-048 liveness:
/// `fleet::heartbeat::open_unopened_sources` opens only the sources its instance owns and holds
/// closed). The cell's leaves are `0` and `1`; the ring is `streams-1` (this rig) and `streams-2`
/// (a peer's beat), and an aged override gives `1` to `streams-2`; the fleet documents alone give
/// the view, so the first beat runs before any ring is known (Addendum 9: it opens nothing then).
/// A split intent names `1` as its one source, its lease lapsed under an actor that is
/// gone. Through beats on either side of 12 s on the rig's clock (past the lease, so an opener of
/// `1` would claim it), `streams-1` holds no engine for `1`, `shards/1`'s objects are exactly what
/// they were (an open would write there), and the intent is unclaimed: still `Preparing` at lease
/// epoch 1. An open of `1` here would fence the shard's owner and decide the owner's intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_beat_opens_no_live_intents_source_another_instance_owns() {
    let fleet = mem();
    seed_peer_owning_1(&fleet).await;
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            prefixes: ["0", "1"].map(str::to_string).to_vec(),
            ..Default::default()
        },
    )
    .await;
    let state = &rig.state;
    let home = state.shards.prefixes().shards.leaves["1"];
    let lapsed = Intent {
        kind: "split".to_string(),
        op_id: "op-elsewhere".to_string(),
        sources: vec![("1".to_string(), home)],
        children: vec![90, 91],
        version_seen: 1,
        state: State::Preparing,
        lease_owner: "inst-gone".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    intent::create(&*state.data_store, &lapsed).await.unwrap();
    let untouched = objects_under(state, "shards/1/").await;
    assert!(crate::fleet::start_configured(state.clone(), &rig.tasks));
    // Beats (a 2 s period) read the live intent; past the lease on the rig's clock an opener of
    // `1` would see it lapsed (`intent::Lapse`, LEASE + SKEW on its own clock) and claim it.
    tokio::time::sleep(Duration::from_secs(4)).await;
    rig.clock.advance(Duration::from_secs(12));
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        state.ownership.effective_owner("1").as_deref(),
        Some("streams-2")
    );
    let held = state.shards.held_prefixes();
    assert!(
        !held.contains(&"1".to_string()),
        "no engine for 1: {held:?}"
    );
    let objects = objects_under(state, "shards/1/").await;
    assert_eq!(objects, untouched, "1's log is untouched");
    let left = intent::read(&*state.data_store)
        .await
        .unwrap()
        .unwrap()
        .intent;
    let left = (left.state, left.lease_epoch, left.lease_owner);
    assert_eq!(
        left,
        (State::Preparing, 1, "inst-gone".to_string()),
        "unclaimed"
    );
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(state).await;
}

/// The fleet documents of a two-instance ring whose peer `streams-2` owns `1`: its beat, an aged
/// override of `1` to it (past the 60 s move cooldown), and a desired count of two.
async fn seed_peer_owning_1(fleet: &std::sync::Arc<dyn object_store::ObjectStore>) {
    use object_store::ObjectStoreExt;
    let now = crate::shard::now_ms();
    let peer = format!(
        r#"{{"instance":"streams-2","ts_ms":{},"rps":0.0,"cpu_pct":0.0,"inflight":0,"owned_shards":[],"draining":false,"url":"https://streams-2.invalid"}}"#,
        now + 60_000
    );
    let beat = object_store::path::Path::from("fleet/streams-2.json");
    fleet.put(&beat, peer.into_bytes().into()).await.unwrap();
    let moved = crate::fleet::OverrideEntry {
        to: "streams-2".to_string(),
        ms: now - 120_000,
    };
    let overrides = crate::fleet::Overrides {
        entries: [("1".to_string(), moved)].into_iter().collect(),
        ..Default::default()
    };
    let raw = serde_json::to_vec(&overrides).unwrap();
    fleet
        .put(&"fleet/overrides.json".into(), raw.into())
        .await
        .unwrap();
    let desired = br#"{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}"#.to_vec();
    fleet
        .put(&"fleet/desired.json".into(), desired.into())
        .await
        .unwrap();
}

/// **A beat opens a live intent's sources only once a ring is known** (DESIGN-DECISION-c3-incomplete-
/// rig Addendum 9). Before the fleet tick publishes a view, an instance counts every shard its own:
/// a direct call of the beat's open of a live intent's sources opens nothing then. With a view of
/// this instance alone, the same call opens the closed source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_beat_opens_a_live_intents_sources_only_once_a_ring_is_known() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(mem()),
            instance: Some("streams-1".into()),
            prefixes: ["0", "1"].map(str::to_string).to_vec(),
            ..Default::default()
        },
    )
    .await;
    let state = &rig.state;
    assert!(state.ownership.ring_active().is_empty(), "no view yet");
    let live = intent::Live {
        op_id: "op-direct".to_string(),
        kind: "split".to_string(),
        owner: "inst-gone".to_string(),
        sources: vec!["1".to_string()],
    };
    let before = state.shards.open_count();
    crate::fleet::open_unopened_sources(state, Some(&live)).await;
    assert_eq!(
        state.shards.open_count(),
        before,
        "nothing opens before a ring"
    );
    assert!(!state.shards.is_open("1"));
    let alone = vec!["streams-1".to_string()];
    state
        .ownership
        .set_view(alone, std::collections::HashMap::new());
    crate::fleet::open_unopened_sources(state, Some(&live)).await;
    assert!(
        state.shards.is_open("1"),
        "the owned source opens once the ring names this instance"
    );
    engine_shutdown(state).await;
}
