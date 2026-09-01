//! Config-parser tests. PR 3.2: NO test mutates process-wide
//! environment state, in-process `set_var`/`remove_var` are gone, and
//! ordinary tests cannot observe ambient Clap environment variables
//! either — `test_cli()` is the explicit [`CliArgs::deterministic`]
//! fixture, not a clap parse. The two tests that need the REAL
//! process environment (`process_environment_smoke_test`,
//! `cli_fixture_matches_scrubbed_parse`) run their subject in a
//! SUBPROCESS whose environment is established before it starts; the
//! `*_helper` tests are those subjects, gated on marker variables so
//! they are inert in the ordinary suite.

use super::*;
use clap::{CommandFactory, Parser};

pub(crate) fn test_cli() -> CliArgs {
    CliArgs::deterministic()
}

/// Spawn THIS test binary running exactly one helper test, with the
/// child's environment fully scrubbed and then seeded from `envs`.
/// `--test-threads=1`: the child runs ONE filtered test — it must not
/// spin up a full worker pool inside an already-parallel parent suite.
fn run_helper_test(test_filter: &str, envs: &[(&str, &str)]) -> std::process::Output {
    let exe = std::env::current_exe().expect("test binary path");
    let mut cmd = std::process::Command::new(exe);
    cmd.arg(test_filter)
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env_clear();
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn helper test subprocess")
}

/// Subject of `cli_fixture_matches_scrubbed_parse` — inert unless the
/// parent set the marker (an ordinary suite run must not compare the
/// fixture against a parse that can see arbitrary developer/CI env).
#[test]
fn cli_fixture_drift_helper() {
    if std::env::var("STREAMS_CLI_FIXTURE_DRIFT_CHECK").is_err() {
        return;
    }
    let parsed =
        CliArgs::try_parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]).unwrap();
    assert_eq!(
        parsed,
        CliArgs::deterministic(),
        "CliArgs::deterministic() drifted from the scrubbed-environment parse — \
         update the fixture to match the clap defaults"
    );
}

/// The deterministic fixture IS the scrubbed parse: proven in a
/// subprocess whose environment is cleared before clap runs, so no
/// ambient variable can leak into the comparison in either direction.
#[test]
fn cli_fixture_matches_scrubbed_parse() {
    let out = run_helper_test(
        "config::tests::cli_fixture_drift_helper",
        &[("STREAMS_CLI_FIXTURE_DRIFT_CHECK", "1")],
    );
    assert!(
        out.status.success(),
        "scrubbed parse != deterministic fixture:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("1 passed"),
        "helper did not run (filter typo would pass vacuously): {stdout}"
    );
}

fn load_with(entries: &[(&str, &str)]) -> ServerConfig {
    ServerConfig::load(test_cli(), &MapEnvironment::from(entries.iter().copied()))
}

#[test]
fn load_with_empty_environment_equals_knob_defaults() {
    let a = load_with(&[]);
    let b = ServerConfig::load(test_cli(), &MapEnvironment::empty());
    assert_eq!(a, b);
    // And a different CLI changes only the CLI segment — knob defaults
    // are environment-independent.
    let c = CliArgs::try_parse_from([
        "streams-slate",
        "--s3-endpoint",
        "http://127.0.0.1:1",
        "--flush-interval-ms",
        "99",
    ])
    .unwrap();
    let c = ServerConfig::load(c, &MapEnvironment::empty());
    assert_eq!(a.storage, c.storage);
    assert_eq!(a.billing, c.billing);
    assert_eq!(c.cli.flush_interval_ms, 99);
}

#[test]
fn default_values_are_pinned() {
    // The no-environment posture, knob by knob. Every literal here is
    // the pre-WP-01 default, moved not changed; a PR that edits one
    // must justify a configuration behavior change.
    let c = load_with(&[]);
    assert_eq!(c.storage.pool_idle_secs, 4);
    assert_eq!(c.storage.store_max_concurrent, 0);
    assert_eq!(c.storage.bulk_inflight_max_bytes, 0);
    assert_eq!(c.storage.bulk_nominal_get_bytes, 8 * 1024 * 1024);
    assert_eq!(c.engine.compactor_poll_ms, 2500);
    assert_eq!(c.engine.compactor_max_concurrent, 4);
    assert_eq!(c.engine.compact_max_subcompactions, 4);
    assert_eq!(c.engine.compact_max_fetch_tasks, 4);
    assert_eq!(c.engine.compact_bytes_to_fetch, 2 * 1024 * 1024);
    assert_eq!(c.engine.compact_max_sst_size, 256 * 1024 * 1024);
    assert_eq!(c.engine.slatedb_rt_threads, 2);
    assert_eq!(c.shard.open_deadline, std::time::Duration::from_secs(180));
    assert_eq!(c.shard.open_wait_ms, 10_000);
    assert_eq!(c.shard.unready_exit_after_secs, 300);
    assert!(!c.history.absorb_pause_initial);
    assert_eq!(c.history.absorb_global_budget_bytes, 4 * 1024 * 1024 * 1024); // cfg(test)
    assert_eq!(c.history.absorb_global_gathers, 64); // cfg(test)
    assert_eq!(c.history.cache_bytes, 32 * 1024 * 1024);
    assert!(!c.history.compactor_off);
    assert_eq!(
        c.history.gc_interval,
        Some(std::time::Duration::from_secs(600))
    );
    assert_eq!(c.postings.cache_bytes, 64 * 1024 * 1024);
    assert_eq!(c.sse.feed_ring_bytes, 1024 * 1024);
    assert_eq!(c.sse.feed_total_bytes, 16 * 1024 * 1024);
    assert_eq!(c.sse.feed_project_bytes_raw, None);
    assert_eq!(c.sse.heartbeat_ms, 15_000);
    assert_eq!(c.http.tail_max_bytes, 1024 * 1024);
    assert!(!c.http.debug_timing);
    assert!(!c.http.debug_exit);
    assert_eq!(c.http.binary_sha256, "unknown");
    assert_eq!(c.http.h1_max_buf, 64 * 1024);
    assert_eq!(c.billing.mode_env, None);
    assert!(c.billing.meter_enabled);
    assert_eq!(c.billing.rollup_env, None);
    assert_eq!(c.billing.path_prefix_env, None);
    assert_eq!(c.billing.outbox_sweep_secs, 300);
    assert_eq!(c.billing.telemetry_drain_secs, 2);
    assert_eq!(c.billing.metrics_interval_secs, 15);
    assert_eq!(c.billing.month_close_grace_ms, 86_400_000);
    assert_eq!(c.billing.telemetry_cache_bytes, 16 * 1024 * 1024);
    assert_eq!(c.billing.sweep_discovery_max, 8);
    assert_eq!(c.billing.sweep_maint_resident, 2);
    assert_eq!(c.billing.sweep_resident_quantum, 4);
    assert_eq!(c.billing.alert_usage_outbox_dirty, 1000);
    assert!(!c.fleet.allow_http_peers);
    assert_eq!(c.fleet.peer_domains_raw, None);
    assert_eq!(c.fleet.rebalance_lag_secs, 60);
    assert_eq!(c.fleet.rebalance_move_cooldown_secs, 60);
    assert_eq!(c.fleet.self_url, "");
    assert_eq!(c.fleet.fleet_min, 1);
    assert_eq!(c.fleet.rebalance_return_secs, 300);
    assert_eq!(c.scaler.eval_secs, 10);
    assert_eq!(c.scaler.rate_window_secs, 120.0);
    assert_eq!(c.scaler.hot_pct, 0.75);
    assert_eq!(c.scaler.cold_pct, 0.15);
    assert_eq!(c.scaler.hot_evals, 2);
    assert_eq!(c.scaler.cold_evals, 180);
    assert_eq!(c.scaler.cooldown_secs, 600);
    assert_eq!(c.scaler.max_segments, 64);
    assert_eq!(c.admission.unabsorbed_bytes_instance, 512 * 1024 * 1024);
    assert_eq!(c.admission.unabsorbed_bytes_shard, 256 * 1024 * 1024);
    assert_eq!(c.admission.absorb_lag_secs, 900);
    assert_eq!(c.admission.maint_release_pct, 75);
    assert_eq!(c.admission.limit_bytes_per_sec, 5_000_000.0);
    assert_eq!(c.admission.limit_reqs_per_sec, 1_000.0);
    assert_eq!(c.admission.limit_recs_per_sec, 5_000.0);
    assert_eq!(c.admission.limit_burst_secs, 2.0);
    assert!(!c.crypto.frame_compress);
    assert_eq!(c.runtime.memprofile_cert, None);
    assert_eq!(c.runtime.cert_sealed_publish_delay_ms_raw, None);
    assert_eq!(c.runtime.certification_mode, None);
}

#[test]
fn env_overlay_applies_with_legacy_parse_semantics() {
    let c = load_with(&[
        ("STORE_BULK_INFLIGHT_MAX_BYTES", "4096"),
        // One env name, two knobs, preserved divergence:
        ("COMPACT_MAX_SST_SIZE_BYTES", "123456"),
        ("SSE_FEED_RING_BYTES", "garbage"), // warn + default
        ("SSE_HEARTBEAT_MS", "0"),          // filtered -> default
        ("MAINT_BACKPRESSURE_RELEASE_PCT", "140"), // min(100)
        ("SWEEP_MAINT_RESIDENT", "0"),      // stored raw (boot check)
        ("HISTORY_GC_INTERVAL_SECS", "0"),  // 0 -> None
        ("HISTORY_GC_MAX_INTERVAL_SECS", "42"), // alias used only when the current name is unset
        ("FRAME_COMPRESS", "TrUe"),
        ("SCALE_HOT_PCT", "90.0"),
        ("BILLING_METER", "off"),
        ("FLEET_ALLOW_HTTP_PEERS", "1"),
    ]);
    assert_eq!(c.storage.bulk_inflight_max_bytes, 4096);
    assert_eq!(c.storage.bulk_nominal_get_bytes, 123456);
    assert_eq!(c.engine.compact_max_sst_size, 123456);
    assert_eq!(c.sse.feed_ring_bytes, 1024 * 1024);
    assert_eq!(c.sse.heartbeat_ms, 15_000);
    assert_eq!(c.admission.maint_release_pct, 100);
    assert_eq!(c.billing.sweep_maint_resident, 0); // raw; floored at the use site
    assert_eq!(c.history.gc_interval, None); // current name set to 0 wins
    assert!(c.crypto.frame_compress);
    assert_eq!(c.scaler.hot_pct, 0.9);
    assert!(!c.billing.meter_enabled);
    assert!(c.fleet.allow_http_peers);

    let c = load_with(&[("HISTORY_GC_MAX_INTERVAL_SECS", "42")]);
    assert_eq!(
        c.history.gc_interval,
        Some(std::time::Duration::from_secs(42))
    );
}

#[test]
fn redacted_summary_never_leaks_secret_channels() {
    // Sentinel secrets live ONLY in cli; the summary must exclude them
    // and every secret-named channel, at any value.
    let mut cli = test_cli();
    cli.secret_access_key = "sk-SENTINEL-7f3a".into();
    cli.access_key_id = "ak-SENTINEL-91bx".into();
    cli.auth_token = Some("tok-SENTINEL-q74m".into());
    cli.streams_cursor_key = Some("cur-SENTINEL-z20k".into());
    let cfg = ServerConfig::load(cli, &MapEnvironment::empty());
    let s = cfg.redacted_summary().to_string();
    for sentinel in [
        "sk-SENTINEL-7f3a",
        "ak-SENTINEL-91bx",
        "tok-SENTINEL-q74m",
        "cur-SENTINEL-z20k",
        "secret_access_key",
        "auth_token",
        "cursor_key",
        "fleet_internal_token",
        "conformance_default_key",
        "usage_stream_key",
        "password",
        "credential",
    ] {
        assert!(
            !s.to_lowercase().contains(&sentinel.to_lowercase()),
            "redacted summary leaks {sentinel}: {s}"
        );
    }
}

/// The complete CLI surface, pinned: every long flag, its env name and
/// its default. A PR that renames/rewires an option fails here first.
/// Table generated from clap's own argument registry.
#[test]
fn cli_surface_is_pinned() {
    let expected: &[(&str, &str, &str)] = &[
        ("listen", "", "127.0.0.1:8090"),
        ("s3-endpoint", "SLATE_S3_ENDPOINT", ""),
        ("bucket", "SLATE_S3_BUCKET", "streams"),
        ("ops-bucket", "", ""),
        ("shard-bucket", "", ""),
        ("data-bucket", "", ""),
        ("region", "SLATE_S3_REGION", "us-east-1"),
        ("access-key-id", "SLATE_S3_ACCESS_KEY_ID", "test"),
        ("secret-access-key", "SLATE_S3_SECRET_ACCESS_KEY", "test"),
        ("initial-shards", "INITIAL_SHARDS", ""),
        ("flush-interval-ms", "FLUSH_INTERVAL_MS", "25"),
        ("wal-group-commit", "WAL_GROUP_COMMIT", "0"),
        ("wal-flush-gap-ms", "WAL_FLUSH_GAP_MS", "0"),
        ("wal-post-ack-gather-ms", "WAL_POST_ACK_GATHER_MS", "0"),
        ("wal-gather-skip-reqs", "WAL_GATHER_SKIP_REQS", "32"),
        ("wal-gather-skip-bytes", "WAL_GATHER_SKIP_BYTES", "1048576"),
        ("tail-ring-bytes", "TAIL_RING_BYTES", "0"),
        ("l0-sst-size-bytes", "L0_SST_SIZE_BYTES", "8388608"),
        ("max-unflushed-bytes", "MAX_UNFLUSHED_BYTES", "16777216"),
        (
            "max-request-body-bytes",
            "MAX_REQUEST_BODY_BYTES",
            "33554432",
        ),
        ("l0-max-ssts", "L0_MAX_SSTS", "8"),
        ("l0-max-ssts-per-key", "L0_MAX_SSTS_PER_KEY", "0"),
        ("compactor-poll-ms", "COMPACTOR_POLL_MS", "2500"),
        ("compactor-max-concurrent", "COMPACTOR_MAX_CONCURRENT", "4"),
        ("wal-gc-interval-secs", "WAL_GC_INTERVAL_SECS", "30"),
        ("gc-quiet-interval-secs", "GC_QUIET_INTERVAL_SECS", "600"),
        ("wal-gc-min-age-secs", "WAL_GC_MIN_AGE_SECS", "60"),
        (
            "compactions-gc-interval-secs",
            "COMPACTIONS_GC_INTERVAL_SECS",
            "30",
        ),
        (
            "compactions-gc-min-age-secs",
            "COMPACTIONS_GC_MIN_AGE_SECS",
            "120",
        ),
        ("manifest-poll-ms", "MANIFEST_POLL_MS", "2000"),
        ("trim-per-op", "TRIM_PER_OP", "8192"),
        ("trim-global-budget", "TRIM_GLOBAL_BUDGET", "65536"),
        ("absorb-pass-bytes", "ABSORB_PASS_BYTES", "268435456"),
        ("absorb-bytes", "ABSORB_BYTES", "4194304"),
        ("absorb-age-secs", "ABSORB_AGE_SECS", "300"),
        ("absorb-concurrency", "ABSORB_CONCURRENCY", "6"),
        ("absorb-small-bytes", "ABSORB_SMALL_BYTES", "1048576"),
        ("handle-idle-evict-secs", "HANDLE_IDLE_EVICT_SECS", "600"),
        ("handle-max-resident", "HANDLE_MAX_RESIDENT", "65536"),
        (
            "absorb-gather-max-bytes",
            "ABSORB_GATHER_MAX_BYTES",
            "33554432",
        ),
        ("absorb-pace-window-ms", "ABSORB_PACE_WINDOW_MS", "50"),
        ("absorb-pace-ms", "ABSORB_PACE_MS", "0"),
        ("absorb-read-par", "ABSORB_READ_PAR", "8"),
        ("conformance-default-key", "", ""),
        ("auth-token", "AUTH_TOKEN", ""),
        ("streams-auth-mode", "STREAMS_AUTH_MODE", "off"),
        (
            "streams-auth-issuer",
            "STREAMS_AUTH_ISSUER",
            "https://auth.prisma.io",
        ),
        ("streams-auth-keys-file", "STREAMS_AUTH_KEYS_FILE", ""),
        ("streams-auth-policy-file", "STREAMS_AUTH_POLICY_FILE", ""),
        ("streams-auth-grants-file", "STREAMS_AUTH_GRANTS_FILE", ""),
        (
            "streams-auth-refresh-secs",
            "STREAMS_AUTH_REFRESH_SECS",
            "30",
        ),
        ("streams-cursor-key", "STREAMS_CURSOR_KEY", ""),
        ("fleet-internal-token", "FLEET_INTERNAL_TOKEN", ""),
        ("fleet-auth-mode", "FLEET_AUTH_MODE", "static"),
        ("workload-token-file", "WORKLOAD_TOKEN_FILE", ""),
        ("release-posture", "STREAMS_RELEASE_POSTURE", "false"),
        ("max-record-payload-bytes", "MAX_RECORD_PAYLOAD_BYTES", ""),
        ("account-id", "ACCOUNT_ID", "acct_local"),
        ("project-id", "PROJECT_ID", "proj_local"),
        ("cell-id", "CELL_ID", "local"),
        ("telemetry-region", "REGION", ""),
        ("usage-stream-key", "USAGE_STREAM_KEY", ""),
        ("billing-mode", "BILLING_MODE", "off"),
        ("rollup", "ROLLUP", "0"),
        ("instance-name", "INSTANCE_NAME", "streams"),
        ("path-prefix", "PATH_PREFIX", ""),
        ("fleet-prefix", "FLEET_PREFIX", ""),
        ("scale-rps-capacity", "SCALE_RPS_CAPACITY", "0"),
        ("scale-out-cpu-pct", "SCALE_OUT_CPU_PCT", "75"),
        ("scale-in-cpu-pct", "SCALE_IN_CPU_PCT", "50"),
        ("scale-cpu-sustain-secs", "SCALE_CPU_SUSTAIN_SECS", "20"),
        ("scale-edge-latency-ms", "SCALE_EDGE_LATENCY_MS", "1000"),
        (
            "project-memory-pressure-bytes",
            "PROJECT_MEMORY_PRESSURE_BYTES",
            "0",
        ),
        (
            "project-memory-release-pct",
            "PROJECT_MEMORY_RELEASE_PCT",
            "75",
        ),
        ("admit-rss-shed-mb", "ADMIT_RSS_SHED_MB", "600"),
        ("sse-max-connections", "SSE_MAX_CONNECTIONS", "10000"),
        (
            "admit-max-inflight-per-stream",
            "ADMIT_MAX_INFLIGHT_PER_STREAM",
            "64",
        ),
        ("admit-max-inflight", "ADMIT_MAX_INFLIGHT", "0"),
        ("scale-edge-slots", "SCALE_EDGE_SLOTS", "140"),
        ("shared-cache-bytes", "SHARED_CACHE_BYTES", "201326592"),
        ("scale-in-secs", "SCALE_IN_SECS", "60"),
        ("scale-latency-ms", "SCALE_LATENCY_MS", "250"),
        ("scale-lat-sustain-secs", "SCALE_LAT_SUSTAIN_SECS", "20"),
        ("fleet-max", "FLEET_MAX", "4"),
    ];
    let cmd = CliArgs::command();
    let mut actual: Vec<(String, String, String)> = cmd
        .get_arguments()
        .map(|a| {
            (
                a.get_long().unwrap_or("").to_string(),
                a.get_env()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                a.get_default_values()
                    .iter()
                    .map(|v| v.to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        })
        .collect();
    actual.sort();
    let mut want: Vec<(String, String, String)> = expected
        .iter()
        .map(|(f, e, d)| (f.to_string(), e.to_string(), d.to_string()))
        .collect();
    want.sort();
    assert_eq!(
        actual, want,
        "CLI surface drifted; a rename/default change is a product decision, not a refactor"
    );
}

#[test]
fn two_configurations_coexist_in_one_process() {
    let a = load_with(&[("FLEET_MIN", "3"), ("SSE_FEED_RING_BYTES", "1048576")]);
    let b = load_with(&[("FLEET_MIN", "9"), ("SSE_FEED_RING_BYTES", "2097152")]);
    assert_eq!(a.fleet.fleet_min, 3);
    assert_eq!(b.fleet.fleet_min, 9);
    assert_eq!(a.sse.feed_ring_bytes, 1_048_576);
    assert_eq!(b.sse.feed_ring_bytes, 2_097_152);
    // No shared slot: constructing b cannot disturb a.
    assert_eq!(a.fleet.fleet_min, 3);
}

/// Subject of `process_environment_smoke_test` — inert unless the
/// parent set the marker AND the value under test.
#[test]
fn process_environment_smoke_helper() {
    if std::env::var("STREAMS_PROCESS_ENV_SMOKE").is_err() {
        return;
    }
    let cfg = ServerConfig::load(test_cli(), &ProcessEnvironment);
    assert_eq!(
        cfg.fleet.fleet_min, 7,
        "ProcessEnvironment must observe the FLEET_MIN the parent set"
    );
}

/// The single process-environment smoke test: the loader really reads
/// the ambient environment through `ProcessEnvironment`. PR 3.2: the
/// subject runs in a SUBPROCESS whose environment is established
/// before it starts — the 2024-edition `set_var` unsafety is real (the
/// test runner's other threads may read the environment concurrently),
/// and RAII restoration never made in-process mutation safe, only
/// tidy. No in-process `set_var`/`remove_var` remains anywhere.
#[test]
fn process_environment_smoke_test() {
    let out = run_helper_test(
        "config::tests::process_environment_smoke_helper",
        &[("STREAMS_PROCESS_ENV_SMOKE", "1"), ("FLEET_MIN", "7")],
    );
    assert!(
        out.status.success(),
        "subprocess smoke failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // And the helper really ran (a filter typo would pass vacuously).
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("1 passed"), "helper did not run: {stdout}");
}
