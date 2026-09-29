//! The settings that became constants: every retired flag is refused on
//! argv, and every retired environment name is ignored. One file names
//! them all (edge record #80).
use super::tests::{load_with, run_helper_test};
use super::{CliArgs, Environment, ProcessEnvironment, ServerConfig};
use clap::Parser;

/// Every value a retired name used to set, read where the server reads it:
/// the GC options and the L0 caps the shard databases open with, the
/// history sweep, and the gather thresholds `bootstrap::run` takes.
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
            config.cli.wal_gather_skip_reqs,
            config.cli.wal_gather_skip_bytes,
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
/// the per-key cap follows it.
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
/// included.
#[test]
fn a_retired_flag_on_argv_is_refused() {
    const FLAGS: [&str; 14] = [
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
