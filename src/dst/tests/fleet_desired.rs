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
/// document's reason is where the sum the tick planned with is read.
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
