#![cfg(test)]

use super::{RUN_WAS_INVOKED, absorber_config, run};
use std::sync::atomic::Ordering;
use std::time::Duration;

mod provider_contract;

#[test]
fn ignored_absorber_options_cannot_change_active_runtime_configuration() {
    let mut base = crate::config::CliArgs::deterministic();
    base.absorb_bytes = 17;
    base.absorb_age_secs = 19;
    base.absorb_pace_window_ms = 23;
    base.absorb_pace_ms = 29;
    base.absorb_read_par = 3;
    let active = absorber_config(&base, 7 * 1024 * 1024);
    assert_eq!(
        active,
        crate::history::AbsorberConfig {
            threshold_bytes: 17,
            threshold_age: Duration::from_secs(19),
            gather_max_bytes: 7 * 1024 * 1024,
            gather_pace_window: Duration::from_millis(23),
            gather_pace: Duration::from_millis(29),
            gather_read_par: 3,
            ..Default::default()
        }
    );

    let mut legacy = base.clone();
    legacy.absorb_pass_bytes = Some(1);
    legacy.absorb_concurrency = Some(99);
    legacy.absorb_small_bytes = Some(2);
    assert_eq!(absorber_config(&legacy, 7 * 1024 * 1024), active);

    let mut tuned = base;
    tuned.absorb_bytes = 31;
    tuned.absorb_read_par = 5;
    let changed = absorber_config(&tuned, 5 * 1024 * 1024);
    assert_ne!(changed, active);
    assert_eq!(changed.threshold_bytes, 31);
    assert_eq!(changed.gather_max_bytes, 5 * 1024 * 1024);
    assert_eq!(changed.gather_read_par, 5);
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
