//! Item 40, planned drain: a stopping instance hands its ownership off
//! while it keeps beating and keeps its fencing, and a drain that cannot
//! finish says so.
use super::fixture_http::{HttpRig, HttpRigOptions, engine_shutdown, http_rig_build};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use crate::fleet::drain::{DrainOutcome, drain};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// The coordination documents of a ring of `count`, no overrides.
async fn seed_ring(fleet: &Arc<dyn ObjectStore>, count: u64) {
    let desired = format!(r#"{{"count":{count},"epoch":1,"reason":"seed","computed_at_ms":0}}"#);
    for (path, body) in [
        ("fleet/desired.json", desired),
        ("fleet/overrides.json", r#"{"entries":{}}"#.to_string()),
    ] {
        fleet
            .put(&Path::from(path), PutPayload::from(body))
            .await
            .unwrap();
    }
}

/// `instance`, its fleet loop running, over the shared `data` and `fleet`
/// stores.
async fn start(
    data: &Arc<dyn ObjectStore>,
    fleet: &Arc<dyn ObjectStore>,
    incarnation: u64,
    instance: &str,
) -> HttpRig {
    let rig = http_rig_build(
        data.clone(),
        RigRuntime::incarnation(incarnation),
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some(instance.into()),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    rig
}

/// Polls `ready` until it holds or `budget` elapses.
async fn settled(budget: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    while !ready() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ready()
}

/// `instance`'s published heartbeat document.
async fn published(fleet: &Arc<dyn ObjectStore>, instance: &str) -> serde_json::Value {
    let path = Path::from(format!("fleet/{instance}.json"));
    let bytes = fleet.get(&path).await.unwrap().bytes().await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// A stream the ring gives `rig`, created there with one record.
async fn stream_owned_by(rig: &HttpRig) -> (String, String) {
    let ct = [("content-type", "application/json")];
    for index in 0..64 {
        let name = format!("drain/s{index}");
        let (status, _, _) = hreq(rig.addr, "PUT", &format!("/v1/stream/{name}"), &ct, b"").await;
        if status == 409 {
            continue;
        }
        assert!(status == 200 || status == 201, "create {name}: {status}");
        let (status, _, _) = hreq(
            rig.addr,
            "POST",
            &format!("/v1/stream/{name}"),
            &ct,
            br#"[{"n":1}]"#,
        )
        .await;
        assert!(status == 200 || status == 204, "append {name}: {status}");
        let held = rig.state.shards.held_prefixes();
        assert_eq!(held.len(), 1, "one shard holds the new stream: {held:?}");
        return (name, held[0].clone());
    }
    panic!("the ring gave this instance none of 64 streams");
}

/// The owner's multi-instance drain contract: the draining instance keeps
/// beating (it announces the drain; it never goes dark first), its peer
/// takes the shard over and serves the record it acknowledged while the
/// drain still runs (the peer's open fences the draining writer: fencing,
/// not heartbeat freshness, is what rules out two writers), and the outcome
/// is a handoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_drain_hands_every_shard_to_its_peer_before_the_stop() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let peer = start(&data, &fleet, 1, "streams-2").await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let converged = settled(Duration::from_secs(20), || {
        [&draining, &peer]
            .iter()
            .all(|rig| rig.state.ownership.ring_active() == both)
    })
    .await;
    assert!(converged, "both rings must hold both instances first");
    let (name, _) = stream_owned_by(&draining).await;

    let done = AtomicBool::new(false);
    let (outcome, (beats, served)) = tokio::join!(
        drain_then_mark(&draining, &done),
        watch(&done, &fleet, &peer, &name)
    );
    assert_eq!(outcome, DrainOutcome::HandedOff);
    assert!(
        !beats.is_empty(),
        "the draining instance must announce its drain while it runs"
    );
    assert!(served, "the peer serves the stream before the drain ends");
    assert!(draining.state.shards.held_prefixes().is_empty());
    assert_eq!(peer.state.ownership.ring_active(), ["streams-2"]);
    assert_eq!(draining.state.ownership.ring_active(), ["streams-2"]);
    let (status, _, body) = hreq(
        peer.addr,
        "GET",
        &format!("/v1/stream/{name}?offset=-1"),
        &[],
        b"",
    )
    .await;
    assert_eq!(status, 200, "the peer serves the handed-off stream");
    assert!(
        String::from_utf8_lossy(&body).contains(r#""n":1"#),
        "the record the draining instance acknowledged is the peer's to serve: {}",
        String::from_utf8_lossy(&body)
    );
    for rig in [&draining, &peer] {
        assert!(
            rig.tasks
                .shutdown(Duration::from_secs(5))
                .await
                .aborted
                .is_empty()
        );
        engine_shutdown(&rig.state).await;
    }
}

/// A drain whose peer never publishes a view without it runs out of budget
/// and says who still routes to it; it is never reported as a handoff.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drain_a_peer_never_acknowledges_times_out_and_names_it() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let document = format!(
        r#"{{"instance":"streams-2","ts_ms":{},"rps":0.0,"owned_shards":[],"draining":false,"seq":5}}"#,
        crate::shard::now_ms()
    );
    fleet
        .put(
            &Path::from("fleet/streams-2.json"),
            PutPayload::from(document),
        )
        .await
        .unwrap();
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(3)).await;
    let DrainOutcome::TimedOut { pending } = outcome else {
        panic!("a drain its peer never acknowledged must time out: {outcome:?}");
    };
    assert!(
        pending.contains(&"streams-2 has not read this instance's drain".to_string()),
        "{pending:?}"
    );
    assert!(
        draining
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&draining.state).await;
}

/// A peer of an earlier version publishes no beat sequence and would keep
/// routing to a draining instance, so no drain begins: the drain says so at
/// once and never announces itself, and the instance stops as it did before
/// drains existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_of_an_earlier_version_means_no_drain() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let document = format!(
        r#"{{"instance":"streams-2","ts_ms":{},"rps":0.0,"owned_shards":[],"draining":false}}"#,
        crate::shard::now_ms()
    );
    fleet
        .put(
            &Path::from("fleet/streams-2.json"),
            PutPayload::from(document),
        )
        .await
        .unwrap();
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(30)).await;
    assert_eq!(outcome, DrainOutcome::NoPeer);
    assert!(
        !draining.state.fleet.standing().draining(),
        "no drain is announced to a peer that would not honour it"
    );
    assert!(
        draining
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&draining.state).await;
}

/// A fleet of one has no peer to take its ownership: its drain says so at
/// once instead of waiting out its budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fleet_of_one_has_no_peer_to_hand_off_to() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 1).await;
    let alone = start(&data, &fleet, 0, "streams-1").await;
    let started = tokio::time::Instant::now();
    let outcome = drain(&alone.state, "streams-1", Duration::from_secs(30)).await;
    assert_eq!(outcome, DrainOutcome::NoPeer);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        alone
            .tasks
            .shutdown(Duration::from_secs(5))
            .await
            .aborted
            .is_empty()
    );
    engine_shutdown(&alone.state).await;
}

/// The wiring, in production's shape: a signal loop requests the stop and
/// ends, the runtime's drain runs before any loop is cancelled, the draining
/// heartbeat is published while the runtime still runs (it also says
/// withdrawn: the ended signal loop is a critical loop), its peer drops it,
/// and only then does the stop follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_requested_stop_drains_the_fleet_runtime_first() {
    let (data, fleet) = (mem(), mem());
    seed_ring(&fleet, 2).await;
    let draining = start(&data, &fleet, 0, "streams-1").await;
    let peer = start(&data, &fleet, 1, "streams-2").await;
    let both = ["streams-1".to_string(), "streams-2".to_string()];
    let converged = settled(Duration::from_secs(20), || {
        [&draining, &peer]
            .iter()
            .all(|rig| rig.state.ownership.ring_active() == both)
    })
    .await;
    assert!(converged, "both rings must hold both instances first");
    signal_loop(&draining);
    let cancellation = draining.tasks.cancellation();
    let mut announced_while_running = false;
    let mut withdrawn = serde_json::Value::Null;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !cancellation.is_cancelled() && tokio::time::Instant::now() < deadline {
        let beat = published(&fleet, "streams-1").await;
        if beat["draining"] == true {
            announced_while_running |= !cancellation.is_cancelled();
            withdrawn = beat["withdrawn"].clone();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        cancellation.is_cancelled(),
        "the drain's end requests the stop"
    );
    assert!(
        announced_while_running,
        "the drain is announced before any loop is cancelled"
    );
    assert_eq!(withdrawn, "critical task terminated: signal");
    assert_eq!(peer.state.ownership.ring_active(), ["streams-2"]);
    for rig in [&draining, &peer] {
        assert!(
            rig.tasks
                .shutdown(Duration::from_secs(5))
                .await
                .aborted
                .is_empty()
        );
        engine_shutdown(&rig.state).await;
    }
}

/// Drains `streams-1` with a 30 s budget, then marks `done`.
async fn drain_then_mark(draining: &HttpRig, done: &AtomicBool) -> DrainOutcome {
    let outcome = drain(&draining.state, "streams-1", Duration::from_secs(30)).await;
    done.store(true, Ordering::SeqCst);
    outcome
}

/// Until `done`: the draining beats `streams-1` publishes, and whether the
/// peer served `name` (reading it routes it there: the peer opens the shard
/// once its view names it owner).
async fn watch(
    done: &AtomicBool,
    fleet: &Arc<dyn ObjectStore>,
    peer: &HttpRig,
    name: &str,
) -> (std::collections::BTreeSet<Option<i64>>, bool) {
    let (mut beats, mut served) = (std::collections::BTreeSet::new(), false);
    let read = format!("/v1/stream/{name}?offset=-1");
    while !done.load(Ordering::SeqCst) {
        let beat = published(fleet, "streams-1").await;
        if beat["draining"] == true {
            beats.insert(beat["ts_ms"].as_i64());
        }
        served |=
            hreq(peer.addr, "GET", &read, &[], b"").await.0 == 200 && !done.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (beats, served)
}

/// The signal loop's shape (`bootstrap::run`): it requests the stop and
/// ends.
fn signal_loop(rig: &HttpRig) {
    let request = rig.tasks.shutdown_request();
    let spawned = rig.tasks.spawn(
        "signal",
        crate::tasks::Policy::Critical,
        move |_| async move {
            request.request();
            crate::tasks::TaskResult::Done
        },
    );
    assert!(spawned.is_ok());
}
