//! Memory-profile certification and the resolved compaction profile —
//! the canonical configuration DIAGNOSTIC API (PR 4.1: moved out of
//! validation.rs, which had grown past budget; HTTP's debug surface and
//! validation both consume it from here, never through bootstrap).

use crate::config::notice::ConfigNotice;

/// Dedicated runtime for every SlateDB instance (shard logs, history DBs,
/// readers). SlateDB spawns its flusher / compactor / batch-writer on the
/// runtime that drives `build()`, and those tasks run CPU-bound SST builds
/// (block encode + zstd + AES block transform) inline in their polls — on
/// the request runtime a single 4-16 MB build holds a worker for 100s of
/// ms and can stall the runtime's timer/IO driver outright (sinmax run 12:
/// tokio timer p99 848 ms vs 3.6 ms for a raw OS thread on the same box).
/// On their own OS threads the kernel preempts them at timeslice
/// granularity instead, so the ack path pays milliseconds, not bursts.
/// R27-4 / R28 review: ONE resolved CompactorOptions for EVERY SlateDB
/// this process opens — shard DBs, telemetry, rollup, spool. The first
/// SIN fix missed the telemetry DBs because their Settings used
/// `..Default::default()`, silently reinstating the upstream worker
/// (concurrency 4, 4 subcompactions, 4x2 MiB read-ahead, 256 MiB
/// rolls) beside the bounded shard DBs. WP-01 PR 3: the knobs live in
/// `config::EngineConfig`, parsed once at startup; clap args mirror the
/// same env vars for --help discoverability.
pub fn resolved_compactor_options(
    engine: &crate::config::EngineConfig,
) -> slatedb::config::CompactorOptions {
    engine.compactor_options()
}

/// The resolved worker knobs as JSON (debug/load + startup log) and the
/// certification check: with MEMPROFILE_CERT=compute-1g the process
/// REFUSES to start unless the live resolved configuration matches the
/// certified survival profile — a deploy that drops one env var must
/// fail loudly at boot, not OOM at +28 minutes.
pub fn compactor_profile_json(cfg: &crate::config::ServerConfig) -> serde_json::Value {
    let co = cfg.engine.compactor_options();
    let w = co.worker.clone().unwrap_or_default();
    serde_json::json!({
        "max_concurrent_compactions": co.max_concurrent_compactions,
        "worker_max_concurrent_compactions": w.max_concurrent_compactions,
        "max_subcompactions": w.max_subcompactions,
        "max_fetch_tasks": w.max_fetch_tasks,
        "bytes_to_fetch": w.bytes_to_fetch,
        "max_sst_size": w.max_sst_size,
        "store_bulk_inflight_max_bytes": cfg.storage.bulk_inflight_max_bytes,
    })
}

/// Every production Settings family and the worker options it will
/// hand its DB builder. R29 release blocker: the certification used to
/// validate only the env helper, while history_settings() passed
/// UPSTREAM defaults to every history partition — the process logged
/// "certified" with the exact unsafe profile running. Certification
/// (and the structural test) now inspects what the builders receive.
pub fn production_settings_families(
    cfg: &crate::config::ServerConfig,
) -> Vec<(&'static str, Option<slatedb::config::CompactorOptions>)> {
    vec![
        ("shard", Some(resolved_compactor_options(&cfg.engine))),
        (
            "history_v1",
            crate::history::history_settings(&cfg.history, &cfg.engine.compactor_options())
                .compactor_options,
        ),
        (
            "history_v2",
            crate::history::history2_settings(&cfg.history, &cfg.engine.compactor_options())
                .compactor_options,
        ),
        (
            "telemetry",
            crate::billing::telemetry_settings(&cfg.billing, &cfg.engine.compactor_options())
                .compactor_options,
        ),
    ]
}

/// PR 3.2: pure — returns every certification mismatch instead of
/// calling `process::exit` from library code (the binary decides how to
/// terminate). Empty vec = certified (or certification not requested).
pub(crate) fn certified_memprofile_errors(
    cfg: &crate::config::ServerConfig,
    notices: &mut Vec<ConfigNotice>,
) -> Vec<String> {
    if cfg.runtime.memprofile_cert.as_deref() != Some("compute-1g") {
        return Vec::new();
    }
    let mut errors = Vec::new();
    let p = compactor_profile_json(cfg);
    let expect = serde_json::json!({
        "max_concurrent_compactions": 1,
        "worker_max_concurrent_compactions": 1,
        "max_subcompactions": 1,
        "max_fetch_tasks": 1,
        "bytes_to_fetch": 1048576,
        "max_sst_size": 33554432,
        "store_bulk_inflight_max_bytes": 33554432u64,
    });
    for (k, want) in expect.as_object().unwrap() {
        let got = &p[k];
        if got != want {
            errors.push(format!(
                "MEMPROFILE_CERT=compute-1g but {k}={got} (certified {want}) — \
                 the deploy dropped or overrode a survival knob; refusing to start"
            ));
        }
    }
    // The env helper matching the certificate is necessary but not
    // sufficient: every DB family's ACTUAL settings must carry the
    // same worker profile.
    let cert = cfg
        .engine
        .compactor_options()
        .worker
        .clone()
        .unwrap_or_default();
    for (family, co) in production_settings_families(cfg) {
        let Some(co) = co else {
            errors.push(format!(
                "MEMPROFILE_CERT: {family} settings disable the compactor"
            ));
            continue;
        };
        let w = co.worker.clone().unwrap_or_default();
        if w.max_subcompactions != cert.max_subcompactions
            || w.max_fetch_tasks != cert.max_fetch_tasks
            || w.bytes_to_fetch != cert.bytes_to_fetch
            || w.max_sst_size != cert.max_sst_size
            || w.max_concurrent_compactions != cert.max_concurrent_compactions
        {
            errors.push(format!(
                "MEMPROFILE_CERT: {family} settings carry a different \
                 compaction worker profile than the certified one; refusing to start"
            ));
        }
    }
    if errors.is_empty() {
        notices.push(ConfigNotice::MemoryProfileCertified {
            profile: p.to_string(),
        });
    }
    errors
}
