#![cfg(test)]

use super::{RUN_WAS_INVOKED, absorber_config, run, shard_config};
use std::sync::atomic::Ordering;
use std::time::Duration;

mod provider_contract;
mod write_tier;

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

/// The settings-derived part of a shard engine's configuration as one
/// value: (pump, flush gap, post-ack gather), (gather-skip requests,
/// bytes), (trim per op, global trim budget), (handle idle eviction,
/// resident handles), and the tail ring bytes.
type ShardSettings = (
    (bool, Duration, Duration),
    (u32, u64),
    (u64, u64),
    (Duration, usize),
    usize,
);

fn shard_settings(config: &crate::shard::ShardConfig) -> ShardSettings {
    (
        (
            config.wal_group_commit,
            config.wal_flush_gap,
            config.wal_post_ack_gather,
        ),
        (config.wal_gather_skip_reqs, config.wal_gather_skip_bytes),
        (config.max_trim_per_op, config.trim_global_budget),
        (config.handle_idle_evict, config.handle_max_resident),
        config.tail_ring_bytes,
    )
}

/// Every shard engine opens on the WAL pipeline, the gather-skip constants,
/// the trims, the handle settings, the tail ring, the history settings and
/// the compactor options that the parsed settings decide, and on no shared
/// handle of its own (`run` adds those). This pins
/// what the review of edge record #77 found unpinned: that the three WAL
/// values reach the engine.
#[test]
fn the_parsed_shard_settings_reach_every_shard_engine() {
    let cli = crate::config::CliArgs::deterministic();
    let history = crate::config::HistoryConfig::default();
    let compactor = crate::config::EngineConfig::default().compactor_options();
    let active = shard_config(&cli, history.clone(), compactor.clone(), false);
    assert_eq!(
        shard_settings(&active),
        (
            (true, Duration::from_millis(100), Duration::from_millis(6)),
            (32, 1_048_576),
            (8_192, 65_536),
            (Duration::from_secs(600), 65_536),
            0,
        ),
        "a server that sets nothing runs the pump with a 100 ms gap and a 6 ms gather, \
         skipped once the next WAL holds 32 requests or 1 MiB"
    );
    assert!(
        active.shared_postings_cache.is_none()
            && active.shared_history.is_none()
            && active.shared_usage.is_none()
            && active.shared_ops.is_none(),
        "the shared handles are the runtime's, added by run"
    );
    assert_eq!(
        (active.history, active.compactor_options.poll_interval),
        (history, compactor.poll_interval)
    );

    let mut tuned = cli;
    tuned.wal_group_commit = 0;
    tuned.wal_flush_gap_ms = 0;
    tuned.flush_interval_ms = 25;
    tuned.wal_post_ack_gather_ms = 0;
    tuned.trim_per_op = 9;
    tuned.trim_global_budget = 11;
    tuned.handle_idle_evict_secs = 13;
    tuned.handle_max_resident = 15;
    tuned.tail_ring_bytes = 32 * 1024 * 1024;
    let history = crate::config::HistoryConfig {
        cache_bytes: 17,
        ..Default::default()
    };
    let compactor = slatedb::config::CompactorOptions {
        poll_interval: Duration::from_millis(19),
        ..crate::config::EngineConfig::default().compactor_options()
    };
    let changed = shard_config(&tuned, history.clone(), compactor, true);
    assert_eq!(
        shard_settings(&changed),
        (
            (false, Duration::from_millis(25), Duration::ZERO),
            (32, 1_048_576),
            (9, 11),
            (Duration::from_secs(13), 15),
            32 * 1024 * 1024,
        ),
        "the tick pipeline flushes on the flush interval, gathers nothing, and the skip \
         thresholds are the same constants"
    );
    assert_eq!(
        (changed.history, changed.compactor_options.poll_interval),
        (history, Duration::from_millis(19))
    );
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
