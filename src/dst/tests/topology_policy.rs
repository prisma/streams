#![cfg(test)]
//! The topology policy's loop over a live cell (S5): the beats the fleet tick reads carry each
//! shard's load, the loop decides on them, and a split it starts goes through the split entry.
//! Until S4's entry lands the test stands a double in: it publishes the split's topology into
//! the directory, as S4's split does on a cell without peers (S4 step 11).
use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::topology::model::{Home, ShardSet};
use crate::topology::policy_loop::{Cell, SplitFn};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// `set` with the leaf `prefix` split into its two children, one version on: the parent's home
/// retired, a home allocated for each child.
fn split_set(set: &ShardSet, prefix: &str) -> ShardSet {
    let version = set.version + 1;
    let parent = set.leaves[prefix];
    let mut next = set.clone();
    next.version = version;
    if let Some(home) = next.homes.get_mut(&parent) {
        home.retired_version = Some(version);
    }
    next.leaves.remove(prefix);
    let children = [format!("{prefix}0"), format!("{prefix}1")];
    for (home, child) in (set.next_home..).zip(&children) {
        let path = crate::topology::model::born_home_path(child, home);
        let parents = vec![parent];
        let born_version = version;
        next.homes.insert(
            home,
            Home {
                path,
                prefix: child.clone(),
                parents,
                born_version,
                retired_version: None,
            },
        );
        next.leaves.insert(child.clone(), home);
    }
    next.next_home = set.next_home + 2;
    let at = next.shards.iter().position(|leaf| leaf == prefix).unwrap();
    next.shards.splice(at..=at, children);
    next
}

/// Each child's log of the split `prefix` in `next`, seeded with its identity row as its clone
/// would be: an operation-born home's log without one is missing (`shard::check_identity`).
async fn seed_children(store: &Arc<dyn ObjectStore>, next: &ShardSet, prefix: &str) {
    for child in [format!("{prefix}0"), format!("{prefix}1")] {
        let home = next.leaves[&child];
        let db = crate::keyspace::db_builder(next.homes[&home].path.as_str(), store.clone())
            .build()
            .await
            .unwrap();
        let (key, value) = crate::shard::open_index::identity_row(home);
        db.put(&key, &value).await.unwrap();
        db.close().await.unwrap();
    }
}

/// The split entry's double: each split it is asked for, in order, its children's logs seeded and
/// its topology installed.
pub(super) fn split_double(
    state: &Arc<crate::http::AppState>,
    asked: Arc<Mutex<Vec<String>>>,
) -> SplitFn {
    let (shards, clock) = (state.shards.clone(), state.runtime.clock.clone());
    let store = state.data_store.clone();
    Arc::new(move |prefix: String| {
        let (shards, clock, store) = (shards.clone(), clock.clone(), store.clone());
        let asked = asked.clone();
        Box::pin(async move {
            let next = split_set(&shards.prefixes().shards, &prefix);
            seed_children(&store, &next, &prefix).await;
            assert!(
                shards.install(next, None, clock.monotonic()),
                "the split's topology installs"
            );
            asked.lock().unwrap().push(prefix);
            Ok(())
        })
    })
}

/// Appends to fresh streams at about `per_sec` a second, each to its own stream so the load
/// spreads over the hash space as the leaves split, until `stop`.
pub(super) async fn load(addr: std::net::SocketAddr, per_sec: u64, stop: Arc<AtomicBool>) {
    let pause = Duration::from_millis(1_000 / per_sec);
    let mut n = 0_u64;
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let path = format!("/v1/stream/tp-{n}");
        let json = [("content-type", "application/json")];
        let (created, _, _) = hreq(addr, "PUT", &path, &json, b"").await;
        if created == 200 || created == 201 {
            hreq(addr, "POST", &path, &json, br#"{"n":1}"#).await;
        }
        tokio::time::sleep(pause).await;
    }
}

/// The directory's leaves now.
pub(super) fn leaves(state: &crate::http::AppState) -> Vec<String> {
    state.shards.prefixes().iter().cloned().collect()
}

/// A one-shard cell whose appends run past the split line doubles to two shards and, both still
/// over it, to four, one split at a time and deepest first; at four the load is under the line
/// on every leaf and nothing more splits. The operations are exactly the three it takes, all on
/// the one instance a fleet of at most one has.
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_one_shard_cell_over_the_split_line_doubles_to_four_shards_and_stops() {
    let fleet = seeded().await;
    let (rig, asked) = policy_rig(
        fleet,
        |cli| {
            // One instance, and the fleet at its maximum of one: no instance can join, so each
            // split goes ahead on this one, both children beside the parent, with no destination
            // (EDGE-261-RATIFICATION §3).
            cli.fleet_max = 1;
            lines(cli);
        },
        split_double,
    )
    .await;

    let stop = Arc::new(AtomicBool::new(false));
    let generator = tokio::spawn(load(rig.addr, 20, stop.clone()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    while leaves(&rig.state).len() < 4 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        leaves(&rig.state),
        ["00", "01", "10", "11"],
        "asked {:?}",
        asked.lock().unwrap()
    );
    // Four leaves at about 5 appends a second each, under the line: the topology holds.
    tokio::time::sleep(Duration::from_secs(20)).await;
    stop.store(true, Ordering::Relaxed);
    generator.await.unwrap();
    assert_eq!(*asked.lock().unwrap(), ["", "0", "1"]);
    assert_eq!(leaves(&rig.state), ["00", "01", "10", "11"]);
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// The policy's lines for a rig: a second's cooldown, and 7 appends a second as the split line,
/// so 20 a second is over it on one leaf and on two, and under it on each of four. The fleet's own
/// policy loop, which splits through S4's entry, holds its sustains past any test, so only the
/// test's loop (`policy_rig`, its sustain one second) decides.
pub(super) fn lines(cli: &mut crate::config::CliArgs) {
    cli.shard_split_sustain_secs = 1_000_000;
    cli.shard_emergency_sustain_secs = 1_000_000;
    cli.shard_topology_cooldown_secs = 1;
    cli.shard_appends_ceiling = 10;
}

/// A one-shard cell run by `streams-1` with its fleet on `fleet`, and the policy's loop on it
/// every 200 ms through the split entry's double, whose asked splits it returns.
/// How a rig's policy loop splits: the double, or S4's split itself, each recording the splits
/// it was asked for.
type Entry = fn(&Arc<crate::http::AppState>, Arc<Mutex<Vec<String>>>) -> SplitFn;

pub(super) async fn policy_rig(
    fleet: Arc<dyn ObjectStore>,
    cli: fn(&mut crate::config::CliArgs),
    entry: Entry,
) -> (super::fixture_http::HttpRig, Arc<Mutex<Vec<String>>>) {
    policy_rig_at(fleet, cli, entry, vec![String::new()]).await
}

/// `policy_rig`, with the cell's leaves `prefixes`.
async fn policy_rig_at(
    fleet: Arc<dyn ObjectStore>,
    cli: fn(&mut crate::config::CliArgs),
    entry: Entry,
    prefixes: Vec<String>,
) -> (super::fixture_http::HttpRig, Arc<Mutex<Vec<String>>>) {
    let on = On {
        data: mem(),
        instance: "streams-1",
        runtime: RigRuntime::first(),
        split_sustain_ms: 1_000,
    };
    policy_rig_on(fleet, on, cli, entry, prefixes).await
}

/// Where a policy rig runs: its cell's data store, its instance and its process incarnation, and
/// the split sustain of the test's own loop (a second, unless a scenario must never split).
pub(super) struct On {
    pub(super) data: Arc<dyn ObjectStore>,
    pub(super) instance: &'static str,
    pub(super) runtime: RigRuntime,
    pub(super) split_sustain_ms: i64,
}

/// `policy_rig_at`, run as `on.instance` over `on.data`, so two instances can share a cell.
pub(super) async fn policy_rig_on(
    fleet: Arc<dyn ObjectStore>,
    on: On,
    cli: fn(&mut crate::config::CliArgs),
    entry: Entry,
    prefixes: Vec<String>,
) -> (super::fixture_http::HttpRig, Arc<Mutex<Vec<String>>>) {
    let rig = http_rig_build(
        on.data,
        on.runtime,
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some(on.instance.into()),
            cli,
            prefixes,
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let asked = Arc::new(Mutex::new(Vec::new()));
    let cell = Cell {
        me: on.instance.into(),
        fleet: rig.state.fleet.clone(),
        shards: rig.state.shards.clone(),
        ownership: rig.state.ownership.clone(),
        store: rig.state.data_store.clone(),
        limits: crate::topology::policy::Limits {
            split_sustain_ms: on.split_sustain_ms,
            ..crate::topology::policy_loop::limits(&rig.state.config)
        },
        appends_ceiling: rig.state.config.cli.shard_appends_ceiling,
        rss_shed_mb: 0,
        split: entry(&rig.state, asked.clone()),
        merge: crate::topology::policy_loop::merge_entry(rig.state.shards.clone()),
    };
    crate::topology::policy_loop::start(cell, Duration::from_millis(200), &rig.tasks);
    (rig, asked)
}

/// A fleet store seeded with a desired count of one.
pub(super) async fn seeded() -> Arc<dyn ObjectStore> {
    let fleet = mem();
    let seed = r#"{"count":1,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(seed))
        .await
        .unwrap();
    fleet
}

/// The fleet document `name` on `fleet`, `Null` while absent.
pub(super) async fn document(fleet: &Arc<dyn ObjectStore>, name: &str) -> serde_json::Value {
    match fleet.get(&Path::from(format!("fleet/{name}.json"))).await {
        Ok(got) => serde_json::from_slice(&got.bytes().await.unwrap()).unwrap(),
        Err(_) => serde_json::Value::Null,
    }
}

/// `streams-1`'s beat once `shows` holds of it, within 30 s.
async fn beat_until(
    fleet: &Arc<dyn ObjectStore>,
    shows: fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut beat = document(fleet, "streams-1").await;
    while !shows(&beat) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        beat = document(fleet, "streams-1").await;
    }
    beat
}

/// A split with no other instance to take a child asks the fleet for one: this instance's beat
/// says `split_refused: "instance_full"`, the desired count grows past the one shard with the
/// ceiling in its reason, and nothing splits. Once the instance asked for beats, the split goes,
/// and it is the only one.
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_split_with_no_destination_asks_for_an_instance_and_splits_once_one_beats() {
    let fleet = seeded().await;
    let (rig, asked) = policy_rig(fleet.clone(), lines, split_double).await;
    let stop = Arc::new(AtomicBool::new(false));
    let generator = tokio::spawn(load(rig.addr, 20, stop.clone()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut desired = document(&fleet, "desired").await;
    while desired["count"].as_u64() < Some(2) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        desired = document(&fleet, "desired").await;
    }
    let reason = desired["reason"].as_str().unwrap_or_default().to_owned();
    assert!(
        reason.ends_with(" shards_per_instance=1/7 (2)"),
        "{desired}"
    );
    // This instance's beat says why its split waits, in the decision's segments (E6). The
    // production loop beside the double's polls only every 10 s on its stub entry, so a beat
    // between them can carry its pass: the check waits for this loop's.
    let beat = beat_until(&fleet, |beat| beat["split_refused"] == "instance_full").await;
    assert_eq!(
        beat["topology_decision"], "instance_full shards_per_instance=1/7",
        "{beat}"
    );
    assert_eq!(beat["shards_resident"], 1, "{beat}");
    assert!(asked.lock().unwrap().is_empty());
    assert_eq!(leaves(&rig.state), [""]);

    // The instance the count asked for: `streams-2`, which the tick reads as soon as the count
    // names it. The ring keeps the one leaf on `streams-1` (rendezvous over "" picks it).
    peer(&fleet, "streams-2").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while leaves(&rig.state).len() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    generator.await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(*asked.lock().unwrap(), [""]);
    assert_eq!(leaves(&rig.state), ["0", "1"]);
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Another instance's live beat, outside the ring (no ordinal the count names), holding no
/// shard: the destination a split's reservation needs.
pub(super) async fn destination(fleet: &Arc<dyn ObjectStore>) {
    peer(fleet, "streams-9").await;
}

/// `instance`'s live beat, holding no shard.
async fn peer(fleet: &Arc<dyn ObjectStore>, instance: &str) {
    let ts_ms = crate::shard::now_ms() + 600_000;
    let beat = format!(
        r#"{{"instance":"{instance}","ts_ms":{ts_ms},"rps":0.0,"owned_shards":[],"draining":false,"url":"https://{instance}.invalid"}}"#
    );
    fleet
        .put(
            &Path::from(format!("fleet/{instance}.json")),
            PutPayload::from(beat),
        )
        .await
        .unwrap();
}

/// `state`'s topology intent, written where the cell keeps it.
async fn write_intent(store: &Arc<dyn ObjectStore>, state: crate::topology::intent::State) {
    use crate::topology::intent::{Intent, Outcome, State};
    let outcome = (state != State::Preparing).then(|| Outcome {
        result: "rolled_back".into(),
        reason: "test".into(),
    });
    let intent = Intent {
        kind: "split".into(),
        op_id: "op-elsewhere".into(),
        sources: vec![("1".into(), 9)],
        children: vec![10, 11],
        version_seen: 1,
        state,
        lease_owner: "streams-2".into(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome,
    };
    let body = serde_json::to_vec(&intent).unwrap();
    let path = Path::from("topology/intent.json");
    store.put(&path, PutPayload::from(body)).await.unwrap();
}

/// A live topology intent holds every operation (S4 §10.1): a leaf over the line does not split
/// while another operation's intent is Preparing, and the beat says why; once the intent is
/// terminal, the split goes.
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_live_intent_holds_the_split_until_it_is_terminal() {
    use crate::topology::intent::State;
    let fleet = seeded().await;
    destination(&fleet).await;
    let (rig, asked) = policy_rig(
        fleet.clone(),
        |cli| {
            cli.fleet_max = 1;
            lines(cli);
        },
        split_double,
    )
    .await;
    write_intent(&rig.state.data_store, State::Preparing).await;
    let stop = Arc::new(AtomicBool::new(false));
    let generator = tokio::spawn(load(rig.addr, 20, stop.clone()));
    let beat = beat_until(&fleet, |beat| {
        beat["topology_decision"] == "hold=intent_live"
    })
    .await;
    assert_eq!(beat["topology_decision"], "hold=intent_live", "{beat}");
    assert!(asked.lock().unwrap().is_empty());

    write_intent(&rig.state.data_store, State::RolledBack).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while asked.lock().unwrap().is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    generator.await.unwrap();
    assert_eq!(asked.lock().unwrap().first().map(String::as_str), Some(""));
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// S4's split behind the seam, recording each split that published.
pub(super) fn real_split(
    state: &Arc<crate::http::AppState>,
    asked: Arc<Mutex<Vec<String>>>,
) -> SplitFn {
    let entry = crate::topology::policy_loop::split_entry(state.shards.clone());
    Arc::new(move |prefix: String| {
        let (entry, asked) = (entry.clone(), asked.clone());
        Box::pin(async move {
            let published = entry(prefix.clone()).await;
            if published.is_ok() {
                asked.lock().unwrap().push(prefix);
            }
            published
        })
    })
}

/// The records of `name`, read back.
pub(super) async fn records(addr: std::net::SocketAddr, name: &str) -> serde_json::Value {
    let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
    assert_eq!(status, 200, "{name}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap_or_default()
}

/// The policy's split goes through S4's split end to end: a one-shard cell over the line splits
/// once into its two halves, and every stream written before the split reads back the same
/// records from the child that holds it.
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_policys_split_through_s4_keeps_every_stream_whole() {
    let fleet = seeded().await;
    destination(&fleet).await;
    let (rig, asked) = policy_rig(
        fleet.clone(),
        |cli| {
            cli.fleet_max = 1;
            lines(cli);
        },
        real_split,
    )
    .await;
    // The cell's stored topology, as boot leaves it: the one S4's split publishes over.
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
    let json = [("content-type", "application/json")];
    let mut kept = Vec::new();
    for i in 0..12 {
        let path = format!("/v1/stream/keep-{i}");
        hreq(rig.addr, "PUT", &path, &json, b"").await;
        for n in 0..3 {
            let body = format!(r#"[{{"stream":{i},"n":{n}}}]"#);
            let (status, _, _) = hreq(rig.addr, "POST", &path, &json, body.as_bytes()).await;
            assert_eq!(status, 204, "append to keep-{i}");
        }
        kept.push((
            format!("keep-{i}"),
            records(rig.addr, &format!("keep-{i}")).await,
        ));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let generator = tokio::spawn(load(rig.addr, 20, stop.clone()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while leaves(&rig.state).len() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    generator.await.unwrap();
    assert_eq!(
        leaves(&rig.state),
        ["0", "1"],
        "asked {:?}",
        asked.lock().unwrap()
    );
    assert_eq!(*asked.lock().unwrap(), [""]);
    for (name, before) in &kept {
        assert_eq!(
            &records(rig.addr, name).await,
            before,
            "{name} after the split"
        );
    }
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// Appends to one stream at about `per_sec` a second until `stop`.
async fn load_one(addr: std::net::SocketAddr, per_sec: u64, stop: Arc<AtomicBool>) {
    let pause = Duration::from_millis(1_000 / per_sec);
    let json = [("content-type", "application/json")];
    hreq(addr, "PUT", "/v1/stream/hot", &json, b"").await;
    while !stop.load(Ordering::Relaxed) {
        hreq(addr, "POST", "/v1/stream/hot", &json, br#"{"n":1}"#).await;
        tokio::time::sleep(pause).await;
    }
}

/// One stream carrying all of a shard's appends over the line is a hot entity: a split cannot
/// relieve it (one shard per entity), so the policy holds `hot_entity`, the beat says so, and
/// nothing splits (rev 4 §4.3; EDGE-261-RATIFICATION A3).
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn one_stream_carrying_a_shards_appends_holds_hot_entity_and_does_not_split() {
    let fleet = seeded().await;
    let (rig, asked) = policy_rig(
        fleet.clone(),
        |cli| {
            cli.fleet_max = 1;
            lines(cli);
        },
        split_double,
    )
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let generator = tokio::spawn(load_one(rig.addr, 20, stop.clone()));
    let beat = beat_until(&fleet, |beat| {
        beat["topology_decision"] == "hold=hot_entity"
    })
    .await;
    stop.store(true, Ordering::Relaxed);
    generator.await.unwrap();
    assert_eq!(beat["topology_decision"], "hold=hot_entity", "{beat}");
    let load = &beat["shard_loads"][0];
    assert!(
        load["top_stream_appends_per_s"].as_u64() > Some(0),
        "{beat}"
    );
    assert!(asked.lock().unwrap().is_empty());
    assert_eq!(leaves(&rig.state), [""]);
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// The cell's stored topology, as boot leaves it: the one S4's split and S6's merge publish over.
pub(super) async fn store_topology(rig: &super::fixture_http::HttpRig) {
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
}

/// **A sustained cold pair is merged into one stored leaf through S6's merge, and every stream
/// reads back whole** (S5 + S6 §7, edge #261). A cell of two leaves, `0` and `1`, takes a few
/// records on each and then goes quiet: both stay under the merge line past its sustain, so the
/// policy decides `merge parent=root`, and the merge entry merges them. The directory and the
/// stored topology end with the one leaf `""`, born at the next version at `shards/root-<h>`, and
/// every stream written before reads back the same records from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sustained_cold_pair_merges_into_one_stored_leaf_and_keeps_every_stream_whole() {
    let fleet = seeded().await;
    let (rig, asked) = policy_rig_at(
        fleet.clone(),
        |cli| {
            cli.fleet_max = 1;
            lines(cli);
            cli.shard_merge_sustain_secs = 1;
            cli.shard_split_reversal_min_secs = 1;
        },
        split_double,
        vec!["0".to_string(), "1".to_string()],
    )
    .await;
    store_topology(&rig).await;
    let json = [("content-type", "application/json")];
    let mut kept = Vec::new();
    for i in 0..12 {
        let path = format!("/v1/stream/cold-{i}");
        hreq(rig.addr, "PUT", &path, &json, b"").await;
        let body = format!(r#"[{{"stream":{i}}}]"#);
        let (status, _, _) = hreq(rig.addr, "POST", &path, &json, body.as_bytes()).await;
        assert_eq!(status, 204, "append to cold-{i}");
        kept.push((
            format!("cold-{i}"),
            records(rig.addr, &format!("cold-{i}")).await,
        ));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while leaves(&rig.state) != [""] && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let beat = document(&fleet, "streams-1").await;
    assert_eq!(
        leaves(&rig.state),
        [""],
        "decision {}",
        beat["topology_decision"]
    );
    assert!(asked.lock().unwrap().is_empty(), "nothing split");
    let (stored, _) = crate::topology::store::read(&*rig.state.data_store)
        .await
        .unwrap();
    assert_eq!(stored.version, 2);
    assert_eq!(stored.shards.leaves.keys().collect::<Vec<_>>(), [""]);
    let root = stored.shards.leaves[""];
    assert_eq!(
        stored.shards.homes[&root].path,
        format!("shards/root-{root}"),
        "the merged root's home"
    );
    for (name, before) in &kept {
        assert_eq!(
            &served(rig.addr, name).await,
            before,
            "{name} after the merge"
        );
    }
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// The records of `name` once its shard serves (a just-merged leaf may still be opening), within
/// 20 s.
pub(super) async fn served(addr: std::net::SocketAddr, name: &str) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (status, _, body) = hreq(addr, "GET", &format!("/v1/stream/{name}"), &[], b"").await;
        if status == 200 {
            return serde_json::from_slice(&body).unwrap_or_default();
        }
        assert!(
            status == 503 && tokio::time::Instant::now() < deadline,
            "{name}: {status} {}",
            String::from_utf8_lossy(&body)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A one-instance fleet cell of two leaves, `0` and `1`, with its stored topology, twelve streams
/// of one record each, and the policy never merging (its merge sustain is the default six hours).
async fn local_pair() -> (
    super::fixture_http::HttpRig,
    Vec<(String, serde_json::Value)>,
) {
    let fleet = seeded().await;
    let one = |cli: &mut crate::config::CliArgs| {
        cli.fleet_max = 1;
        lines(cli);
    };
    let halves = vec!["0".to_string(), "1".to_string()];
    let (rig, _) = policy_rig_at(fleet, one, split_double, halves).await;
    store_topology(&rig).await;
    let kept = super::topology_colocation::streams_with_a_record(rig.addr, "lost", 12).await;
    (rig, kept)
}

/// The cell's intent once it is terminal, within 45 s: lapse (10 s) and skew (1 s), a beat, the
/// fence's own lapse and claim, and slack.
async fn terminal_intent(store: &Arc<dyn ObjectStore>) -> crate::topology::intent::Intent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let held = crate::topology::intent::read(&**store)
            .await
            .unwrap()
            .unwrap();
        if held.intent.is_terminal() {
            return held.intent;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the intent ends with no request: still {:?} under {}",
            held.intent.state,
            held.intent.lease_owner
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Merge the root's halves through S6's entry, and wait for that merge's intent to end
/// `Reclaimed`: the cell is then the one leaf `""`.
async fn merges_after(rig: &super::fixture_http::HttpRig) {
    // A half still finishing its open answers 503, which an operator retries. A half the
    // claimant's maintenance open fenced mid-open re-enters the hold and reopens once the intent
    // ended, within a few rechecks (`fence::fenced`), not after the fence's whole wait.
    let t0 = tokio::time::Instant::now();
    let deadline = t0 + Duration::from_secs(10);
    let op_id = loop {
        match rig.state.shards.topology_ops().merge(String::new()).await {
            Ok(started) => break started.op_id,
            Err(crate::topology::merge::Refused::Unavailable(why))
                if tokio::time::Instant::now() < deadline =>
            {
                tracing::debug!(why, "the later merge waits for its halves to open");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(refused) => panic!("a later merge starts: {refused}"),
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let held = crate::topology::intent::read(&*rig.state.data_store)
            .await
            .unwrap()
            .unwrap();
        if held.intent.op_id == op_id && held.intent.is_terminal() {
            assert_eq!(
                held.intent.state,
                crate::topology::intent::State::Reclaimed,
                "the later merge: {:?}",
                held.intent
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the later merge ends"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// **A merge whose actor died while Preparing is rolled back with no request** (TLA-048
/// `liveness_local`). The cell's pair is on this one instance; the merge's intent is live, its
/// lease lapsed, its actor gone, and no engine of either half is open. No client request comes.
/// Each half's owner opens it from its heartbeat, so its open-path fence claims the lapsed intent
/// and rolls it back: `RolledBack` at the next lease epoch. A merge of the same pair then starts
/// and publishes, and every stream reads back the record it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_whose_actor_died_while_preparing_is_rolled_back_with_no_request() {
    use crate::topology::intent::{Intent, State};
    let (rig, kept) = local_pair().await;
    let snapshot = rig.state.shards.prefixes();
    let homes = [snapshot.shards.leaves["0"], snapshot.shards.leaves["1"]];
    let lost = Intent {
        kind: "merge".to_string(),
        op_id: "op-lost".to_string(),
        sources: vec![("0".to_string(), homes[0]), ("1".to_string(), homes[1])],
        children: vec![snapshot.shards.next_home],
        version_seen: snapshot.shards.version,
        state: State::Preparing,
        lease_owner: "inst-gone".to_string(),
        lease_epoch: 1,
        lease_until_ms: 0,
        started_ms: 0,
        outcome: None,
    };
    engine_shutdown(&rig.state).await;
    crate::topology::intent::create(&*rig.state.data_store, &lost)
        .await
        .unwrap();
    let ended = terminal_intent(&rig.state.data_store).await;
    assert_eq!(
        (ended.op_id.as_str(), ended.state),
        ("op-lost", State::RolledBack)
    );
    assert_eq!(ended.lease_epoch, 2, "claimed once");
    merges_after(&rig).await;
    for (name, before) in &kept {
        assert_eq!(&served(rig.addr, name).await, before, "{name}");
    }
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}

/// **A merge left `Unfinished` is rolled forward with no request** (TLA-048, the variant with
/// the actor alive). The publish of a merge of this instance's pair fails after both `'T'`s (the
/// stored topology unreadable at the park): the merge ends `Unfinished`, its dropped barriers
/// close both halves, and its intent stays live. The topology is readable again, and no client
/// request comes. Each half's owner opens it from its heartbeat; once the lease lapsed its fence
/// claims the intent and rolls the merge forward from `'T'`: `Reclaimed`, the stored topology the
/// one leaf `""`, and every stream reads back the record it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_merge_left_unfinished_is_rolled_forward_with_no_request() {
    use crate::topology::intent::State;
    let (rig, kept) = local_pair().await;
    let halves = ["0", "1"].map(|p| rig.state.shards.open(p).expect("the half is open here"));
    let store = rig.state.data_store.clone();
    let reserved = Arc::new(Mutex::new(None));
    let kept_doc = reserved.clone();
    let corrupt = || async move {
        let path = Path::from("topology.json");
        let doc = store.get(&path).await.unwrap().bytes().await.unwrap();
        *kept_doc.lock().unwrap() = Some(doc);
        store
            .put(&path, PutPayload::from_static(b"{"))
            .await
            .unwrap();
    };
    let merged =
        super::merge_finish_k::merge_paused_at_park(&rig.state, &halves, "op-u", corrupt).await;
    assert!(
        matches!(merged, Err(crate::topology::merge::Refused::Unfinished(_))),
        "the publish fails after 'T': {:?}",
        merged.as_ref().map(|published| published.version)
    );
    let doc = reserved
        .lock()
        .unwrap()
        .take()
        .expect("the topology was read at the park");
    rig.state
        .data_store
        .put(&Path::from("topology.json"), PutPayload::from(doc))
        .await
        .unwrap();
    let ended = terminal_intent(&rig.state.data_store).await;
    assert_eq!(
        (ended.op_id.as_str(), ended.state),
        ("op-u", State::Reclaimed)
    );
    let (stored, _) = crate::topology::store::read(&*rig.state.data_store)
        .await
        .unwrap();
    assert_eq!(stored.shards.leaves.keys().collect::<Vec<_>>(), [""]);
    for (name, before) in &kept {
        assert_eq!(&served(rig.addr, name).await, before, "{name}");
    }
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
}
