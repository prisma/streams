//! The count the fleet publishes from the cores its members report in use.
use super::super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig_build};
use super::super::fixture_runtime::RigRuntime;
use super::super::fixture_storage::mem;
use super::{desired_doc, peer_heartbeat};
use object_store::{ObjectStoreExt, PutPayload, path::Path};
use std::time::Duration;

/// The first document the controller publishes over a fleet seeded at
/// `count`. Two peers report 1.5 cores each and nothing in flight; this
/// instance adds what its own process uses, which no test controls, so the
/// document's reason is where the sum the tick planned with is read. The
/// topology has 16 shards, past every count these tests plan: the count is
/// clamped to the shards (item 6), which `fleet::planning` tests itself.
async fn first_publication(
    count: u64,
    cli: fn(&mut crate::config::CliArgs),
) -> crate::fleet::Desired {
    let fleet = mem();
    let seed = format!(r#"{{"count":{count},"epoch":1,"reason":"seed","computed_at_ms":0}}"#);
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(seed))
        .await
        .unwrap();
    for peer in ["streams-2", "streams-3"] {
        peer_heartbeat(&fleet, peer, 0, 150.0).await;
    }
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            cli,
            prefixes: (0..16).map(|shard| format!("{shard:02x}")).collect(),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    // A fleet that did not start fails here at once, not after its waits.
    let tasks = rig.tasks.monitor().snapshot();
    assert!(
        tasks.iter().any(|t| t.name == "fleet"),
        "the fleet tick runs"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let mut desired = desired_doc(&rig.state).await;
    while desired.epoch < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        desired = desired_doc(&rig.state).await;
    }
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(
        report.aborted.is_empty(),
        "fleet loop must cancel cooperatively: {report:?}"
    );
    engine_shutdown(&rig.state).await;
    desired
}

/// The cores in use a reason prints. It prints two decimals, so the sum the
/// tick divided lies within 0.005 of them.
fn cores_in_use(reason: &str) -> f64 {
    reason
        .strip_prefix("cores_used=")
        .and_then(|rest| rest.split(' ').next())
        .and_then(|printed| printed.parse().ok())
        .unwrap_or_else(|| panic!("the reason starts with the cores in use: {reason}"))
}

/// Whether `count` is `cores / util` rounded up, for the cores the reason
/// prints.
fn is_cores_over(desired: &crate::fleet::Desired, util: f64) -> bool {
    let cores = cores_in_use(&desired.reason);
    let count = f64::from(u32::try_from(desired.count).unwrap());
    ((cores - 0.005) / util).ceil() <= count && count <= ((cores + 0.005) / util).ceil()
}

/// Growth is planned on the scale-out target (75 %): three cores in use and
/// more want four instances and more. A controller that multiplied would
/// publish three, and one that took the remainder would leave the fleet of
/// one alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_fleet_grows_to_the_cores_in_use_over_the_scale_out_target() {
    let desired = first_publication(1, |cli| cli.fleet_max = 512).await;
    assert_eq!(
        desired.epoch, 2,
        "three cores in use scale a fleet of one out at the first tick: {desired:?}"
    );
    assert!(
        cores_in_use(&desired.reason) >= 3.0,
        "both peers' cores are counted: {desired:?}"
    );
    assert!(
        is_cores_over(&desired, 0.75),
        "the count is the cores in use over 0.75, rounded up: {desired:?}"
    );
    assert!(
        desired
            .reason
            .contains(&format!(" util->{} ", desired.count)),
        "the utilization dimension alone decides this count: {desired:?}"
    );
}

/// Shrinking is planned on the scale-in ceiling (50 %), which is below the
/// target so the fleet does not flap: three cores in use and more keep six
/// instances and more. A controller that multiplied would keep two, and one
/// that took the remainder would shrink to one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_fleet_shrinks_to_the_cores_in_use_over_the_scale_in_ceiling() {
    let desired = first_publication(512, |cli| {
        cli.fleet_max = 512;
        cli.scale_in_secs = 0;
    })
    .await;
    assert_eq!(
        desired.epoch, 2,
        "with no sustain window the first tick publishes the shrink: {desired:?}"
    );
    assert!(
        cores_in_use(&desired.reason) >= 3.0,
        "both peers' cores are counted: {desired:?}"
    );
    assert!(
        is_cores_over(&desired, 0.5),
        "the count is the cores in use over 0.5, rounded up: {desired:?}"
    );
}

/// A member that refuses reads for read memory on three beats in a row
/// counts as hot like a cpu-hot one (item 6): the 10M-on-8 run refused reads
/// for minutes while every instance's CPU stayed low and the fleet planned
/// nothing. Three members at about 30 % CPU (one core in use: utilization
/// wants one instance), one of them reporting the refusal streak: the fleet
/// wants one instance more than it has live, and its reason names the streak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_refusing_reads_three_beats_running_scales_the_fleet_out() {
    let fleet = mem();
    let seed = r#"{"count":3,"epoch":1,"reason":"seed","computed_at_ms":0}"#;
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(seed))
        .await
        .unwrap();
    let ts_ms = crate::shard::now_ms() + 60_000;
    for (peer, refusal_beats) in [("streams-2", 3), ("streams-3", 0)] {
        let beat = format!(
            r#"{{"instance":"{peer}","ts_ms":{ts_ms},"rps":40.0,"cpu_pct":30.0,"inflight":0,"owned_shards":[],"draining":false,"url":"https://{peer}.invalid","refusals":12,"refusal_beats":{refusal_beats}}}"#
        );
        fleet
            .put(
                &Path::from(format!("fleet/{peer}.json")),
                PutPayload::from(beat),
            )
            .await
            .unwrap();
    }
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet),
            instance: Some("streams-1".into()),
            cli: |cli| {
                cli.fleet_max = 64;
                cli.scale_cpu_sustain_secs = 0;
            },
            prefixes: (0..16).map(|shard| format!("{shard:02x}")).collect(),
            ..Default::default()
        },
    )
    .await;
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let mut desired = desired_doc(&rig.state).await;
    while desired.epoch < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        desired = desired_doc(&rig.state).await;
    }
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
    assert!(
        wanted_by(&desired, 4, " refusal_beats=3 ") && desired.reason.contains("MB (4) "),
        "three live members, one refusing reads: the hot dimension wants live + 1: {desired:?}"
    );
}

/// Whether the first publication's count is at least `wanted` and its reason
/// shows `marker`, the dimension that wanted it. This instance's own beat
/// carries what the test process's CPU uses, which no test controls, so its
/// utilization may want more on a loaded host; the dimension's own want is
/// what is asserted.
fn wanted_by(desired: &crate::fleet::Desired, wanted: u64, marker: &str) -> bool {
    desired.epoch == 2 && desired.count >= wanted && desired.reason.contains(marker)
}

/// One fleet tick's plan over a seeded count of `seed`, the peers `peers`
/// (name, rps, the extra heartbeat fields) at 30 % CPU each, this
/// instance's RSS shed line `shed_mb` (0 = off, as every rig's) and `shards`
/// shards; every sustain at 0, and the scale-in window held at its default.
/// Returns the desired document after the first publication, or as seeded
/// when 12 s of ticks publish nothing, and this instance's beat once it
/// carries a split demand (its last within the same 12 s otherwise).
async fn plan_once(
    seed: u64,
    peers: &[(&str, f64, &str)],
    shed_mb: u64,
    shards: usize,
) -> (crate::fleet::Desired, serde_json::Value) {
    let fleet = mem();
    let doc = format!(r#"{{"count":{seed},"epoch":1,"reason":"seed","computed_at_ms":0}}"#);
    fleet
        .put(&Path::from("fleet/desired.json"), PutPayload::from(doc))
        .await
        .unwrap();
    let ts_ms = crate::shard::now_ms() + 60_000;
    for (peer, rps, extra) in peers {
        let beat = format!(
            r#"{{"instance":"{peer}","ts_ms":{ts_ms},"rps":{rps},"cpu_pct":30.0,"inflight":0,"owned_shards":[],"draining":false,"url":"https://{peer}.invalid"{extra}}}"#
        );
        fleet
            .put(
                &Path::from(format!("fleet/{peer}.json")),
                PutPayload::from(beat),
            )
            .await
            .unwrap();
    }
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            fleet_store: Some(fleet.clone()),
            instance: Some("streams-1".into()),
            cli: |cli| {
                cli.fleet_max = 64;
                cli.scale_cpu_sustain_secs = 0;
                cli.scale_lat_sustain_secs = 0;
            },
            prefixes: (0..shards).map(|shard| format!("{shard:02x}")).collect(),
            ..Default::default()
        },
    )
    .await;
    rig.state.admission.set_rss_shed_mb(shed_mb);
    assert!(crate::fleet::start_configured(
        rig.state.clone(),
        &rig.tasks
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    let mut desired = desired_doc(&rig.state).await;
    while desired.epoch < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        desired = desired_doc(&rig.state).await;
    }
    let beat = loop {
        let beat: serde_json::Value = match fleet.get(&Path::from("fleet/streams-1.json")).await {
            Ok(got) => serde_json::from_slice(&got.bytes().await.unwrap()).unwrap(),
            Err(_) => serde_json::Value::Null,
        };
        if beat["split_demand"].as_u64().unwrap_or(0) > 0 || tokio::time::Instant::now() >= deadline
        {
            break beat;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let report = rig.tasks.shutdown(Duration::from_secs(3)).await;
    assert!(report.aborted.is_empty(), "{report:?}");
    engine_shutdown(&rig.state).await;
    (desired, beat)
}

/// A member whose RSS stays at 85 % of the shed line counts as hot (item 6,
/// the lead's ruling): live + 1. A fleet of idle members at 40 % each is not
/// full: RSS is judged per member, never summed, so it changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_member_near_its_rss_shed_line_scales_the_fleet_out_and_idle_ones_do_not() {
    let near = [
        ("streams-2", 40.0, r#","rss_mb":85000.0"#),
        ("streams-3", 40.0, r#","rss_mb":1000.0"#),
    ];
    let (desired, _) = plan_once(3, &near, 100_000, 16).await;
    assert!(
        wanted_by(&desired, 4, " rss_max=85000MB (4) "),
        "{desired:?}"
    );
    let idle = [
        ("streams-2", 40.0, r#","rss_mb":40000.0"#),
        ("streams-3", 40.0, r#","rss_mb":40000.0"#),
    ];
    let (desired, _) = plan_once(3, &idle, 100_000, 16).await;
    assert!(
        desired.epoch == 1 || desired.reason.contains(" rss_max=40000MB (0) "),
        "idle members at 40 % want nothing: {desired:?}"
    );
}

/// The fleet's filling resources ask for their occupancy whatever the rate:
/// descriptor-cache entries ceil(total / (0.75 × 65,536)) and tracked
/// projects ceil(total / (0.75 × 32,768)).
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn the_fleets_filling_caches_and_trackers_ask_for_their_occupancy() {
    let caching = [
        ("streams-2", 0.0, r#","cached_streams":100000"#),
        ("streams-3", 0.0, r#","cached_streams":100000"#),
    ];
    let (desired, _) = plan_once(3, &caching, 0, 16).await;
    // 200,000 / 49,152 = 4.07: five instances.
    assert!(
        wanted_by(&desired, 5, " cached=200000 projects=0 (5) "),
        "{desired:?}"
    );
    let tracking = [
        ("streams-2", 40.0, r#","tracked_projects":40000"#),
        ("streams-3", 40.0, r#","tracked_projects":40000"#),
    ];
    let (desired, _) = plan_once(3, &tracking, 0, 16).await;
    // 80,000 / 24,576 = 3.26: four instances.
    assert!(
        wanted_by(&desired, 4, " projects=80000 (4) "),
        "{desired:?}"
    );
}

/// A loaded member whose touch-path store reads wait past the latency line
/// (250 ms), sustained, asks for live + 1: a slow read is not an ack.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_sustained_store_read_wait_scales_the_fleet_out() {
    let slow = [
        ("streams-2", 40.0, r#","store_read_p50_ms":400"#),
        ("streams-3", 40.0, ""),
    ];
    let (desired, _) = plan_once(3, &slow, 0, 16).await;
    assert!(wanted_by(&desired, 4, " ack_p50=400ms (4) "), "{desired:?}");
}

/// The fleet never wants more instances than shards: on a one-shard cell a
/// filling cache keeps the count at one, and the demand past the shard,
/// within FLEET_MAX, is published on the beat as the split demand shard
/// split (item 7) reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4, crate = "crate::test_rt")]
async fn a_one_shard_cell_publishes_its_demand_as_split_demand() {
    let caching = [
        ("streams-2", 0.0, r#","cached_streams":100000"#),
        ("streams-3", 0.0, r#","cached_streams":100000"#),
    ];
    let (desired, beat) = plan_once(1, &caching, 0, 1).await;
    assert_eq!(
        (desired.epoch, desired.count),
        (1, 1),
        "one shard, one instance: {desired:?}"
    );
    let split = beat["split_demand"].as_u64().unwrap_or(0);
    assert!(split >= 4, "five wanted, one shard: {beat}");
}
