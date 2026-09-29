#![cfg(test)]

use super::{RUN_WAS_INVOKED, absorber_config, run};
use std::sync::atomic::Ordering;
use std::time::Duration;

mod provider_contract;

#[test]
fn active_absorber_options_reach_the_absorber_configuration() {
    let mut base = crate::config::CliArgs::deterministic();
    base.absorb_bytes = 17;
    base.absorb_age_secs = 19;
    base.absorb_read_par = 3;
    let active = absorber_config(&base, 7 * 1024 * 1024);
    assert_eq!(
        active,
        crate::history::AbsorberConfig {
            threshold_bytes: 17,
            threshold_age: Duration::from_secs(19),
            gather_max_bytes: 7 * 1024 * 1024,
            gather_read_par: 3,
            ..Default::default()
        }
    );

    let mut tuned = base;
    tuned.absorb_bytes = 31;
    tuned.absorb_read_par = 5;
    let changed = absorber_config(&tuned, 5 * 1024 * 1024);
    assert_ne!(changed, active);
    assert_eq!(changed.threshold_bytes, 31);
    assert_eq!(changed.gather_max_bytes, 5 * 1024 * 1024);
    assert_eq!(changed.gather_read_par, 5);
}

#[test]
fn a_server_that_sets_nothing_absorbs_by_age_after_sixty_seconds() {
    let active = absorber_config(&crate::config::CliArgs::deterministic(), 8 * 1024 * 1024);
    assert_eq!(
        (active.threshold_age, active.threshold_bytes),
        (Duration::from_secs(60), 4 * 1024 * 1024)
    );
}

/// The admission controller of a server that sets nothing: the two caps as
/// `run` copies them from the parsed settings. The other gates are off.
fn default_admission() -> crate::admission::AdmissionController {
    let cli = crate::config::CliArgs::deterministic();
    crate::admission::AdmissionController::new(crate::admission::AdmissionKnobs {
        max_inflight: cli.admit_max_inflight,
        per_stream_cap: cli.admit_max_inflight_per_stream,
        rss_shed_mb: 0,
        project_memory_pressure_bytes: 0,
        project_memory_release_pct: 75,
        subscriptions: crate::admission::SubscriptionCapacity {
            effective: 0,
            configured: 0,
        },
        record_ceiling_bytes: 0,
    })
}

#[test]
fn a_server_that_sets_nothing_sheds_writes_above_512_in_flight() {
    let admission = default_admission();
    admission.add_inflight_for_test(512);
    assert_eq!(admission.admit_write_inflight(), Ok(()));
    admission.add_inflight_for_test(1);
    assert_eq!(
        admission.admit_write_inflight(),
        Err(crate::admission::WriteRefusal::Overloaded)
    );
    assert_eq!(
        (
            admission.survival_refused(2048, true),
            admission.survival_refused(2049, true),
            admission.survival_refused(2049, false),
        ),
        (false, true, false)
    );
    let seen = admission.snapshot();
    assert_eq!(
        (
            seen.max_inflight,
            seen.shed.total,
            seen.shed.inflight,
            seen.shed.survival
        ),
        (512, 2, 1, 1)
    );
}

#[test]
fn a_server_that_sets_nothing_admits_256_concurrent_appends_to_one_stream() {
    let admission = default_admission();
    let stream = [7u8; 16];
    let mut held: Vec<_> = (0..300)
        .filter_map(|_| admission.stream_slot(stream).ok().flatten())
        .collect();
    assert_eq!((held.len(), admission.snapshot().shed.stream), (256, 44));
    assert!(matches!(admission.stream_slot([8u8; 16]), Ok(Some(_))));
    drop(held.pop());
    held.extend(admission.stream_slot(stream).ok().flatten());
    assert_eq!(held.len(), 256);
    assert!(admission.stream_slot(stream).is_err());
    let seen = admission.snapshot();
    assert_eq!(
        (seen.per_stream_cap, seen.shed.stream, seen.shed.total),
        (256, 45, 0)
    );
}

#[tokio::test]
async fn process_bootstrap_cannot_be_an_empty_success() {
    let validated = crate::config::ServerConfig::load(
        crate::config::CliArgs::deterministic(),
        &crate::config::MapEnvironment::empty(),
    )
    .validate()
    .unwrap();
    RUN_WAS_INVOKED.store(true, Ordering::SeqCst);
    let error = run(validated).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("starts process infrastructure once")
    );
}
