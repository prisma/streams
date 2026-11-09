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
        let path = format!("shards/{child}~{home}");
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
/// on every leaf and nothing more splits. The operations are exactly the three it takes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
async fn a_one_shard_cell_over_the_split_line_doubles_to_four_shards_and_stops() {
    let fleet = seeded().await;
    destination(&fleet).await;
    let (rig, asked) = policy_rig(fleet, |cli| {
        // One instance in the ring: the cell's leaves are all this one's.
        cli.fleet_max = 1;
        lines(cli);
    })
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

/// The policy's lines for a rig: a second's sustain and cooldown, and 7 appends a second as the
/// split line, so 20 a second is over it on one leaf and on two, and under it on each of four.
fn lines(cli: &mut crate::config::CliArgs) {
    cli.shard_split_sustain_secs = 1;
    cli.shard_topology_cooldown_secs = 1;
    cli.shard_appends_ceiling = 10;
}

/// A one-shard cell run by `streams-1` with its fleet on `fleet`, and the policy's loop on it
/// every 200 ms through the split entry's double, whose asked splits it returns.
async fn policy_rig(
    fleet: Arc<dyn ObjectStore>,
    cli: fn(&mut crate::config::CliArgs),
) -> (super::fixture_http::HttpRig, Arc<Mutex<Vec<String>>>) {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            cli,
            prefixes: vec![String::new()],
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
        me: "streams-1".into(),
        fleet: rig.state.fleet.clone(),
        shards: rig.state.shards.clone(),
        ownership: rig.state.ownership.clone(),
        limits: crate::topology::policy_loop::limits(&rig.state.config),
        appends_ceiling: rig.state.config.cli.shard_appends_ceiling,
        rss_shed_mb: 0,
        split: split_double(&rig.state, asked.clone()),
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
#[expect(
    clippy::disallowed_methods,
    reason = "policy DST; the load generator is stopped through the shared flag and joined before the topology is checked; it must run while the policy loop decides"
)]
async fn a_split_with_no_destination_asks_for_an_instance_and_splits_once_one_beats() {
    let fleet = seeded().await;
    let (rig, asked) = policy_rig(fleet.clone(), lines).await;
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
