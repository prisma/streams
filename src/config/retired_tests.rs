//! The settings that became constants or were removed with their
//! mechanism (edge records #80 to #85): every retired flag is refused on
//! argv, and every retired environment name is ignored. The v1 absorber
//! names that did nothing (edge record #74) are held in `config::tests`.
use super::tests::{load_with, run_helper_test, test_cli};
use super::{CliArgs, Environment, MapEnvironment, ProcessEnvironment, ServerConfig};
use clap::Parser;

/// Every value a retired name used to set, read where the server reads it:
/// the GC options and the L0 caps the shard databases open with, the
/// history sweep, and the gather thresholds `bootstrap::run` takes (the
/// constants of `EngineConfig`; no field carries them). The assumed
/// capacity of the scaler has no value to read: its field is gone with
/// the cap the boot line printed.
type RetiredValues = (
    ((Option<u64>, u64), (Option<u64>, u64), [Option<u64>; 3]),
    Option<u64>,
    (usize, usize),
    (u32, u64),
);

fn retired_values(config: &ServerConfig) -> RetiredValues {
    let settings = super::validation::shard_settings(&config.cli, &config.engine);
    let gc = settings.garbage_collector_options.unwrap_or_default();
    let secs = |o: Option<slatedb::config::GarbageCollectorDirectoryOptions>| {
        let o = o.unwrap_or_default();
        (o.interval.map(|d| d.as_secs()), o.min_age.as_secs())
    };
    (
        (
            secs(gc.wal_options),
            secs(gc.compactions_options),
            [
                gc.wal_fence_options,
                gc.manifest_options,
                gc.compacted_options,
            ]
            .map(|o| secs(o).0),
        ),
        config.history.gc_interval.map(|d| d.as_secs()),
        (settings.l0_max_ssts, settings.l0_max_ssts_per_key),
        (
            super::EngineConfig::WAL_GATHER_SKIP_REQS,
            super::EngineConfig::WAL_GATHER_SKIP_BYTES,
        ),
    )
}

/// Subject of `a_retired_name_in_the_environment_changes_nothing`: inert
/// unless the parent set the marker.
#[test]
fn retired_names_helper() {
    if ProcessEnvironment
        .get("STREAMS_RETIRED_NAMES_CHECK")
        .is_none()
    {
        return;
    }
    let cli =
        CliArgs::try_parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]).unwrap();
    let config = ServerConfig::load(cli, &ProcessEnvironment);
    assert_eq!(
        retired_values(&config),
        (
            ((Some(30), 60), (Some(30), 120), [Some(600); 3]),
            Some(600),
            (48, 48),
            (32, 1_048_576),
        ),
        "GC cadences and age floors, the per-key L0 cap and the gather skips are constants"
    );
}

/// The names that were settings and are constants now are not read: a
/// process that holds every one of them, in a cleared environment, runs the
/// values of a process that holds none. `L0_MAX_SSTS` stays a setting, and
/// the per-key cap follows it. The two names of the gather pacing that was
/// removed (edge record #81) were read by clap, so only this child process
/// can hold them: each carries a value clap refused while it parsed them.
#[test]
fn a_retired_name_in_the_environment_changes_nothing() {
    let out = run_helper_test(
        "config::retired_tests::retired_names_helper",
        &[
            ("STREAMS_RETIRED_NAMES_CHECK", "1"),
            ("WAL_GC_INTERVAL_SECS", "7"),
            ("WAL_GC_MIN_AGE_SECS", "1"),
            ("COMPACTIONS_GC_INTERVAL_SECS", "9"),
            ("COMPACTIONS_GC_MIN_AGE_SECS", "2"),
            ("GC_QUIET_INTERVAL_SECS", "0"),
            ("HISTORY_GC_INTERVAL_SECS", "0"),
            ("HISTORY_GC_MAX_INTERVAL_SECS", "42"),
            ("L0_MAX_SSTS", "48"),
            ("L0_MAX_SSTS_PER_KEY", "8"),
            ("WAL_GATHER_SKIP_REQS", "0"),
            ("WAL_GATHER_SKIP_BYTES", "7"),
            ("SCALE_RPS_CAPACITY", "150"),
            ("ABSORB_PACE_MS", "abc"),
            ("ABSORB_PACE_WINDOW_MS", "abc"),
        ],
    );
    assert!(
        out.status.success(),
        "a retired name changed a value:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("1 passed"),
        "retired names helper did not run"
    );
}

/// The flags of the retired settings are refused like any other unknown
/// argument, `--gc-max-interval-secs` (the alias retired before them) and
/// the two flags of the gather pacing that was removed (edge record #81)
/// and the flag of the scaler's assumed capacity (edge record #83) included.
#[test]
fn a_retired_flag_on_argv_is_refused() {
    const FLAGS: [&str; 15] = [
        "--wal-gc-interval-secs",
        "--wal-gc-min-age-secs",
        "--compactions-gc-interval-secs",
        "--compactions-gc-min-age-secs",
        "--gc-quiet-interval-secs",
        "--gc-max-interval-secs",
        "--l0-max-ssts-per-key",
        "--wal-gather-skip-reqs",
        "--wal-gather-skip-bytes",
        "--ops-bucket",
        "--shard-bucket",
        "--data-bucket",
        "--absorb-pace-ms",
        "--absorb-pace-window-ms",
        "--scale-rps-capacity",
    ];
    let parsed = FLAGS.map(|flag| {
        let argv = ["streams-slate", "--s3-endpoint", "http://e", flag, "1"];
        (
            flag,
            CliArgs::try_parse_from(argv)
                .map(|_| ())
                .map_err(|e| e.kind()),
        )
    });
    assert_eq!(
        parsed,
        FLAGS.map(|flag| (flag, Err(clap::error::ErrorKind::UnknownArgument)))
    );
}

/// The three names the overlay read are not read: the history sweep, the
/// page of a read woken by a wait and the h1 read buffer keep their values,
/// and a buffer value below hyper's floor cannot refuse the start.
#[test]
fn overlay_names_nothing_set_are_not_read() {
    let c = load_with(&[
        ("HISTORY_GC_INTERVAL_SECS", "0"),
        ("TAIL_MAX_BYTES", "4096"),
        ("SSE_H1_MAX_BUF", "4096"),
    ]);
    assert_eq!(
        (
            c.history.gc_interval.map(|d| d.as_secs()),
            c.http.tail_max_bytes,
            c.http.h1_max_buf,
            c.validate().map(|_| ()).map_err(|e| e.to_string()),
        ),
        (Some(600), 1024 * 1024, 64 * 1024, Ok(()))
    );
}

/// A name that was a setting without a flag, and whose mechanism is removed,
/// changes nothing in the loaded configuration: `STORE_MAX_CONCURRENT`, the
/// count cap on store operations (edge record #82), `HISTORY_COMPACTOR`,
/// the switch that turned the history compactor off (edge record #84),
/// `BILLING_METER`, the switch that turned ingest metering off (edge record
/// #85), and `FRAME_COMPRESS`, the per-record compression switch that
/// layout 5's pages replaced (they compress whenever that pays): neither of
/// the spellings that turned it on, nor "0", changes anything.
#[test]
fn retired_environment_names_change_nothing() {
    const NAMES: [(&str, &str); 6] = [
        ("STORE_MAX_CONCURRENT", "48"),
        ("HISTORY_COMPACTOR", "off"),
        ("BILLING_METER", "off"),
        ("FRAME_COMPRESS", "1"),
        ("FRAME_COMPRESS", "TrUe"),
        ("FRAME_COMPRESS", "0"),
    ];
    for (name, value) in NAMES {
        assert_eq!(load_with(&[(name, value)]), load_with(&[]), "{name}");
    }
}

/// Required billing cannot be started with ingest unmetered: under
/// `BILLING_MODE=required` a process that holds `BILLING_METER=off` loads
/// the configuration of a process that does not hold the name.
#[test]
fn required_billing_cannot_be_loaded_with_ingest_unmetered() {
    let required = |entries: &[(&str, &str)]| {
        let mut cli = test_cli();
        cli.billing_mode = "required".into();
        ServerConfig::load(cli, &MapEnvironment::from(entries.iter().copied()))
    };
    let held = required(&[("BILLING_METER", "off")]);
    assert!(held.cli.billing_required());
    assert_eq!(held, required(&[]));
}

/// The history databases of both layouts open with the embedded compactor,
/// on the worker options the engine resolved (one compaction at a time,
/// polled every 2.5 s, 32 MiB output SSTs), and with L0 caps of 64, whatever
/// `HISTORY_COMPACTOR` holds: read on the settings the builders receive, not
/// on the configuration value.
#[test]
fn the_history_compactor_cannot_be_switched_off() {
    let c = load_with(&[("HISTORY_COMPACTOR", "off")]);
    let workers = c.engine.compactor_options();
    let opened_with = |s: slatedb::config::Settings| {
        let compactor = s.compactor_options.map(|o| {
            (
                o.poll_interval.as_millis(),
                o.max_concurrent_compactions,
                o.worker.map(|w| w.max_sst_size),
            )
        });
        (s.l0_max_ssts, s.l0_max_ssts_per_key, compactor)
    };
    let expected = (64, 64, Some((2500, 1, Some(32 * 1024 * 1024))));
    assert_eq!(
        (
            opened_with(crate::history::history_settings(&c.history, &workers)),
            opened_with(crate::history::history2_settings(&c.history, &workers)),
        ),
        (expected, expected)
    );
}
