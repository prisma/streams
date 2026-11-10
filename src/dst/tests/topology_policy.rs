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

/// The split entry's double: each split it is asked for, in order, installed at once.
fn split_double(state: &Arc<crate::http::AppState>, asked: Arc<Mutex<Vec<String>>>) -> SplitFn {
    let (shards, clock) = (state.shards.clone(), state.runtime.clock.clone());
    Arc::new(move |prefix: String| {
        let next = split_set(&shards.prefixes().shards, &prefix);
        assert!(
            shards.install(next, None, clock.monotonic()),
            "the split's topology installs"
        );
        asked.lock().unwrap().push(prefix);
        Box::pin(async { Ok(()) })
    })
}

/// Appends to fresh streams at about `per_sec` a second, each to its own stream so the load
/// spreads over the hash space as the leaves split, until `stop`.
async fn load(addr: std::net::SocketAddr, per_sec: u64, stop: Arc<AtomicBool>) {
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
fn leaves(state: &crate::http::AppState) -> Vec<String> {
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
fn lines(cli: &mut crate::config::CliArgs) {
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

async fn policy_rig(
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
struct On {
    data: Arc<dyn ObjectStore>,
    instance: &'static str,
    runtime: RigRuntime,
    split_sustain_ms: i64,
}

/// `policy_rig_at`, run as `on.instance` over `on.data`, so two instances can share a cell.
async fn policy_rig_on(
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
async fn seeded() -> Arc<dyn ObjectStore> {
    let fleet = mem();
    let seed = r#"{"count":1,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(seed))
        .await
        .unwrap();
    fleet
}

/// The fleet document `name` on `fleet`, `Null` while absent.
async fn document(fleet: &Arc<dyn ObjectStore>, name: &str) -> serde_json::Value {
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
async fn destination(fleet: &Arc<dyn ObjectStore>) {
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
fn real_split(state: &Arc<crate::http::AppState>, asked: Arc<Mutex<Vec<String>>>) -> SplitFn {
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
async fn records(addr: std::net::SocketAddr, name: &str) -> serde_json::Value {
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
async fn store_topology(rig: &super::fixture_http::HttpRig) {
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
async fn served(addr: std::net::SocketAddr, name: &str) -> serde_json::Value {
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
    let moved = document(&fleet, "overrides").await;
    assert_eq!(
        (&moved["entries"]["0"]["to"], &moved["entries"]["1"]["to"]),
        (&"streams-2".into(), &"streams-2".into()),
        "the actor moved both halves to itself first: {moved}"
    );
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
