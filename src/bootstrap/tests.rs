#![cfg(test)]

use super::{RUN_WAS_INVOKED, run, shard_config};
use std::sync::atomic::Ordering;
use std::time::Duration;

mod provider_contract;
mod write_tier;

/// The settings-derived part of a shard engine's configuration as one
/// value: (pump, flush gap, post-ack gather), (gather-skip requests,
/// bytes), (handle idle eviction, resident handles), and the tail ring
/// bytes.
type ShardSettings = (
    (bool, Duration, Duration),
    (u32, u64),
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
        (config.handle_idle_evict, config.handle_max_resident),
        config.tail_ring_bytes,
    )
}

/// Every shard engine opens on the WAL pipeline, the gather-skip constants,
/// the handle settings and the tail ring that the parsed settings decide,
/// and on no shared handle of its own (`run` adds it). This pins
/// what the review of edge record #77 found unpinned: that the three WAL
/// values reach the engine.
#[test]
fn the_parsed_shard_settings_reach_every_shard_engine() {
    let cli = crate::config::CliArgs::deterministic();
    let active = shard_config(&cli);
    assert_eq!(
        shard_settings(&active),
        (
            (true, Duration::from_millis(100), Duration::from_millis(6)),
            (32, 1_048_576),
            (Duration::from_secs(600), 65_536),
            0,
        ),
        "a server that sets nothing runs the pump with a 100 ms gap and a 6 ms gather, \
         skipped once the next WAL holds 32 requests or 1 MiB"
    );
    assert!(
        active.shared_ops.is_none(),
        "the shared handle is the runtime's, added by run"
    );

    let mut tuned = cli;
    tuned.wal_group_commit = 0;
    tuned.wal_flush_gap_ms = 0;
    tuned.flush_interval_ms = 25;
    tuned.wal_post_ack_gather_ms = 0;
    tuned.handle_idle_evict_secs = 13;
    tuned.handle_max_resident = 15;
    tuned.tail_ring_bytes = 32 * 1024 * 1024;
    let changed = shard_config(&tuned);
    assert_eq!(
        shard_settings(&changed),
        (
            (false, Duration::from_millis(25), Duration::ZERO),
            (32, 1_048_576),
            (Duration::from_secs(13), 15),
            32 * 1024 * 1024,
        ),
        "the tick pipeline flushes on the flush interval, gathers nothing, and the skip \
         thresholds are the same constants"
    );
}

/// The shard log's `l0_max_ssts` is the line every engine's L0 watermark
/// derives its marks from (`shard::layout::l0_watermark`): the shard
/// configuration's build hands it to the process. (Another test's engine
/// sees 48 only while this one runs, which can only delay its shed.)
#[test]
fn the_shard_logs_l0_line_reaches_the_l0_watermark() {
    let cli = crate::config::CliArgs::deterministic();
    let mut tuned = cli.clone();
    tuned.l0_max_ssts = 48;
    shard_config(&tuned);
    assert_eq!(crate::backpressure::l0_line(), 48);
    shard_config(&cli);
    assert_eq!(crate::backpressure::l0_line(), 32, "the CLI default");
}

/// The sealer posture every engine of a server that sets nothing runs (no
/// sealer knob comes from the command line): a seal once 24 MiB is unsealed,
/// 42 WAL objects were written since the last seal or the oldest page is
/// 45 s old, the memtable flushed every third seal and at the 128-object or
/// 60 s deadline, evaluated every 250 ms.
#[test]
fn a_server_that_sets_nothing_seals_by_age_after_forty_five_seconds() {
    let shipped = crate::shard::seal::SealSettings::default();
    assert_eq!(
        (
            shipped.age_seal,
            shipped.age_deadline,
            shipped.unsealed_bytes,
            shipped.memtable_bytes,
            (
                shipped.wal_objects_seal,
                shipped.seals_per_flush,
                shipped.wal_objects_deadline,
            ),
            shipped.tick,
            shipped.auto,
        ),
        (
            Duration::from_secs(45),
            Duration::from_secs(60),
            24 * 1024 * 1024,
            6 * 1024 * 1024,
            (42, 3, 128),
            Duration::from_millis(250),
            true,
        )
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
            admission.survival_refused(2048, "/v1/stream/a"),
            admission.survival_refused(2049, "/v1/stream/a"),
            admission.survival_refused(2049, "/health"),
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

#[tokio::test(crate = "crate::test_rt")]
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
