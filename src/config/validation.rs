//! Configuration validation (PR 3.2.1: moved out of the bootstrap
//! catch-all — bootstrap CONSUMES [`ValidatedServerConfig`]; this
//! module DEFINES what it means).
//!
//! [`crate::config::ServerConfig::validate`] is the two-stage
//! boundary's second stage: pure over the configuration value, it
//! collects EVERY problem and returns the proven
//! [`ValidatedServerConfig`] — the only type [`crate::bootstrap::run`]
//! accepts, so every configuration-dependent refusal happens before
//! any startup side effect by construction. The architectural rule
//! (review, PR 3.2.1): once `ValidatedServerConfig` exists, no code
//! after the boundary may validate, assert, panic, or error solely
//! because two configuration fields are inconsistent. OS-probe checks
//! (descriptor limits) live in `run()`'s named preflight instead.

use std::time::Duration;

use slatedb::config::Settings;

use crate::config::cli::CliArgs;

#[cfg(test)]
#[path = "validation_tests.rs"]
mod validation_tests;

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
pub(crate) fn certified_memprofile_errors(cfg: &crate::config::ServerConfig) -> Vec<String> {
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
        tracing::info!(profile = %p, "memory profile certified: compute-1g (all DB families)");
    }
    errors
}

pub(crate) fn shard_settings(args: &CliArgs, engine: &crate::config::EngineConfig) -> Settings {
    Settings {
        // With the group-commit pump on, SlateDB's internal timer is only
        // a failsafe for anything the pump misses (it should never fire
        // on a healthy shard) — stretch it well past the pump cadence.
        flush_interval: Some(Duration::from_millis(if args.wal_group_commit != 0 {
            args.flush_interval_ms.max(1000)
        } else {
            args.flush_interval_ms
        })),
        l0_sst_size_bytes: args.l0_sst_size_bytes,
        max_unflushed_bytes: args.max_unflushed_bytes,
        l0_max_ssts: args.l0_max_ssts,
        l0_max_ssts_per_key: if args.l0_max_ssts_per_key == 0 {
            args.l0_max_ssts
        } else {
            args.l0_max_ssts_per_key
        },
        // F1: `max_wal_flushes_before_l0_flush` has a 4096 validation floor
        // upstream, so the recovery window is bounded instead by the shard
        // engine's periodic explicit memtable->L0 flush (ShardEngine ticker).
        // D23: fencing correctness comes from CAS write failures, not polls.
        manifest_poll_interval: Duration::from_millis(args.manifest_poll_ms),
        compactor_options: Some(resolved_compactor_options(engine)),
        garbage_collector_options: {
            let mut gc = Settings::default()
                .garbage_collector_options
                .unwrap_or_default();
            // Upstream carries no quiet-backoff or listing reuse (it
            // declined slatedb#1991 in favor of #1993's 10-minute
            // default), so the fork-era economics come from STATIC
            // intervals instead: sweeps that used to back off toward
            // the --gc-quiet-interval-secs ceiling now simply run AT
            // that cadence. Reclamation latency is the trade, LIST
            // steady-state is preserved (COST-CAMPAIGN-2 addendum).
            let quiet = (args.gc_quiet_interval_secs > 0)
                .then(|| Duration::from_secs(args.gc_quiet_interval_secs));
            gc.wal_options = Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(args.wal_gc_interval_secs)),
                min_age: Duration::from_secs(args.wal_gc_min_age_secs),
                ..gc.wal_options.unwrap_or_default()
            });
            // Fence sweeps are dry-run and never delete their fence
            // candidates; every pass re-lists the WAL dir, so the quiet
            // cadence is the right one.
            gc.wal_fence_options = Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: quiet,
                ..gc.wal_fence_options.unwrap_or_default()
            });
            gc.compactions_options = Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(args.compactions_gc_interval_secs)),
                min_age: Duration::from_secs(args.compactions_gc_min_age_secs),
                ..gc.compactions_options.unwrap_or_default()
            });
            gc.manifest_options = Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: quiet,
                ..gc.manifest_options.unwrap_or_default()
            });
            gc.compacted_options = Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: quiet,
                ..gc.compacted_options.unwrap_or_default()
            });
            Some(gc)
        },
        ..Default::default()
    }
}

/// Round-10 review: the release posture requires an explicit
/// per-record payload ceiling whose WORST-CASE prepared SSE frame
/// fits the certified feed ring — otherwise one legal oversized
/// append forces every shared-feed subscriber into the reconnect
/// posture at once (O(subscribers) EOFs, TLS re-establishment burst,
/// durable read amplification).
/// Round-11.6: the seal-publication delay is a CERTIFICATION
/// instrument, never a production knob — a nonzero delay without
/// STREAMS_CERTIFICATION_MODE=1 refuses boot (fail-loud beats a
/// silently armed canary fault in production). PR 3.2: called from
/// `ServerConfig::validate`; the boot warning for a nonzero delay is
/// emitted by `run()`'s preflight.
///
/// Pure over its inputs (unit-testable without process-global env
/// mutation).
fn cert_sealed_publish_delay_from(delay: Option<&str>, mode: Option<&str>) -> anyhow::Result<u64> {
    let ms: u64 = match delay {
        Some(v) => v.parse().map_err(|_| {
            anyhow::anyhow!("STREAMS_CERT_SEALED_PUBLISH_DELAY_MS must be an integer")
        })?,
        None => 0,
    };
    if ms > 0 && mode != Some("1") {
        anyhow::bail!(
            "STREAMS_CERT_SEALED_PUBLISH_DELAY_MS is a certification instrument and \
             requires STREAMS_CERTIFICATION_MODE=1"
        );
    }
    Ok(ms)
}

fn validate_record_ceiling(
    sse: &crate::config::SseConfig,
    release_posture: bool,
    ceiling: Option<usize>,
) -> anyhow::Result<()> {
    if !release_posture {
        return Ok(());
    }
    let Some(ceiling) = ceiling else {
        anyhow::bail!(
            "STREAMS_RELEASE_POSTURE=1 requires MAX_RECORD_PAYLOAD_BYTES — one record \
             whose prepared SSE frame exceeds SSE_FEED_RING_BYTES would disconnect \
             every shared-feed subscriber at once (round-10 review)"
        );
    };
    // Round-10e review: zero means UNLIMITED on the request path — a
    // release configuration that looks set but silently disables the
    // mechanism is refused.
    if ceiling == 0 {
        anyhow::bail!(
            "MAX_RECORD_PAYLOAD_BYTES=0 is unlimited and is not permitted under \
             STREAMS_RELEASE_POSTURE=1"
        );
    }
    let ring = crate::sse::budget::feed_ring_bytes(sse);
    // CHECKED true worst-case bound (round-10e: text framing expands
    // ~6x, more than base64's 4/3; an absurd ceiling must fail here,
    // never wrap).
    let Some(worst) = crate::sse::feed::worst_prepared_charge(ceiling) else {
        anyhow::bail!(
            "MAX_RECORD_PAYLOAD_BYTES={ceiling}: the worst-case prepared-frame \
             calculation overflows — the ceiling is not a plausible record size"
        );
    };
    if worst > ring {
        anyhow::bail!(
            "MAX_RECORD_PAYLOAD_BYTES={ceiling}: worst-case prepared SSE frame is \
             {worst} bytes, exceeding SSE_FEED_RING_BYTES={ring} — raise the ring or \
             lower the record ceiling (release posture requires the frame to fit)"
        );
    }
    // Round-10e: one legal record must also fit BOTH retention caps,
    // and the project backstop must actually isolate: a zero project
    // cap disconnects every shared subscriber on first publication,
    // and a project cap at/above the cell ceiling disables the
    // isolation the backstop exists to provide. An unparseable
    // project-cap setting fails boot here (the dev path only warns).
    let global = crate::sse::budget::feed_total_cap(sse);
    let project = crate::sse::feed::configured_project_cap(sse, global)
        .map_err(|m| anyhow::anyhow!("{m} (STREAMS_RELEASE_POSTURE=1 refuses the fallback)"))?;
    if project == 0 {
        anyhow::bail!(
            "SSE_FEED_PROJECT_BYTES=0 admits shared subscribers and then disconnects \
             them on the first publication — not permitted under release posture"
        );
    }
    if project >= global {
        anyhow::bail!(
            "SSE_FEED_PROJECT_BYTES={project} >= SSE_FEED_TOTAL_BYTES={global}: one \
             project could consume the whole cell budget — the project backstop must \
             be strictly below the cell ceiling"
        );
    }
    if (worst as u64) > project {
        anyhow::bail!(
            "worst-case prepared record ({worst} bytes) exceeds \
             SSE_FEED_PROJECT_BYTES={project}: a single legal record could never be \
             retained by any project"
        );
    }
    if (worst as u64) > global {
        anyhow::bail!(
            "worst-case prepared record ({worst} bytes) exceeds \
             SSE_FEED_TOTAL_BYTES={global}: a single legal record could never be \
             retained at all"
        );
    }
    Ok(())
}

fn validate_fleet_auth(args: &CliArgs, fleet_mode: bool) -> anyhow::Result<()> {
    match args.fleet_auth_mode.as_str() {
        "static" => {
            if args.release_posture {
                anyhow::bail!(
                    "FLEET_AUTH_MODE=static is the bridge posture and is refused under \
                     STREAMS_RELEASE_POSTURE=1 — configure workload identity (§14.1)"
                );
            }
            tracing::warn!(
                "FLEET_AUTH_MODE=static: the shared bridge token is a NAMED legacy \
                 posture; the release posture requires workload identity (§14.1)"
            );
            if fleet_mode {
                match (&args.fleet_internal_token, &args.auth_token) {
                    (None, _) => anyhow::bail!(
                        "fleet mode (static) requires FLEET_INTERNAL_TOKEN (a credential \
                         distinct from AUTH_TOKEN) — /v1/internal/* must not be reachable \
                         with a customer bearer"
                    ),
                    (Some(t), _) if t.len() < 16 => {
                        anyhow::bail!("FLEET_INTERNAL_TOKEN must be at least 16 characters")
                    }
                    (Some(t), Some(a)) if t == a => anyhow::bail!(
                        "FLEET_INTERNAL_TOKEN must differ from AUTH_TOKEN — they are \
                         separate trust boundaries"
                    ),
                    _ => {}
                }
            }
        }
        "workload" => {
            if args.workload_token_file.is_none() {
                anyhow::bail!(
                    "FLEET_AUTH_MODE=workload requires WORKLOAD_TOKEN_FILE (the \
                     platform-rotated workload JWT)"
                );
            }
            if args.release_posture && args.fleet_internal_token.is_some() {
                anyhow::bail!(
                    "STREAMS_RELEASE_POSTURE=1 with FLEET_AUTH_MODE=workload must not \
                     carry FLEET_INTERNAL_TOKEN — the release posture has NO permanent \
                     shared credential (round-3 finding 1)"
                );
            }
        }
        other => anyhow::bail!("FLEET_AUTH_MODE must be static|workload, got {other:?}"),
    }
    if args.release_posture && args.streams_auth_mode != "enforce" {
        anyhow::bail!(
            "STREAMS_RELEASE_POSTURE=1 requires STREAMS_AUTH_MODE=enforce (got {:?})",
            args.streams_auth_mode
        );
    }
    Ok(())
}

/// Largest hub retention posture any field campaign exercised (the
/// 1-GiB ladder's 64-MiB rung). NOT the release-safe maximum for any
/// specific profile — that is per-profile below; a rung that produced
/// memory-pressure write shedding at ~505 hubs must not be the
/// release-safe ceiling for the tier it shed on.
pub(crate) const MAX_EXERCISED_HUB_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

/// Release-safe hub-retention MAXIMUM per memory profile. Follow-up
/// review finding 4: one process-global "largest ever exercised"
/// number must not govern every instance class. A larger tier defines
/// its own entry once certified there.
fn profile_feed_budget_max(profile: Option<&str>) -> u64 {
    match profile {
        // Round-12 re-derivation (docs/PERF-LIVEFEED.md): the LiveFeed
        // retention model reserves exact bytes per retained batch, and
        // the controlled 1-GiB study certified 64 MiB (10,000 parked
        // subscribers idle at 307 MB, peak 399 MB — 101 MB below the
        // shed line). The former 16 MiB ceiling was the deleted hub's
        // uncertainty envelope (its 64-MiB rung tripped RSS shed at
        // ~505 hubs because hub retention was NOT exactly accounted).
        Some("compute-1g") => 64 * 1024 * 1024,
        _ => MAX_EXERCISED_HUB_TOTAL_BYTES,
    }
}

/// Descriptors held OUTSIDE the SSE connection budget — storage
/// clients, peer HTTP pools, maintenance, stdio, listener. The clamp
/// keeps this much headroom below `nofile_hard`.
const FD_RESERVE: u64 = 1024;

/// Round-4 review: lock the safe 1-GiB defaults at boot. Pure over its
/// inputs so the suite can exercise every shape without touching
/// process environment or rlimits.
/// * `feed_total_env` — the raw SSE_FEED_TOTAL_BYTES value, when set.
/// * `sse_max_connections` — configured cap; CLAMPED in place to the
///   effective ceiling when it exceeds what the descriptor budget can
///   actually carry (clamp + emit, per the review's acceptable arm).
/// * `nofile_hard` — RLIMIT_NOFILE hard ceiling already raised to
///   (0 = unknown / non-unix: skip the fd clamp).
pub(crate) fn validate_release_capacity(
    release_posture: bool,
    profile: Option<&str>,
    feed_total_env: Option<&str>,
    sse_max_connections: &mut u64,
    nofile_hard: u64,
) -> anyhow::Result<()> {
    // The feed retention budget: refuse an explicit override above the
    // largest certified posture, and refuse a value that would
    // silently parse as the default (a typo'd byte count must not
    // masquerade as a tuned budget).
    if let Some(raw) = feed_total_env {
        let parsed: Option<u64> = raw.trim().parse().ok();
        match parsed {
            None => {
                anyhow::bail!(
                    "SSE_FEED_TOTAL_BYTES={raw:?} does not parse as a byte count \
                     (an unparseable value would silently fall back to the default)"
                );
            }
            Some(v) if v > profile_feed_budget_max(profile) => {
                let max = profile_feed_budget_max(profile);
                if release_posture {
                    anyhow::bail!(
                        "SSE_FEED_TOTAL_BYTES={v} exceeds the {max}-byte release-safe \
                         maximum for memory profile {:?} (the 1-GiB class certifies at \
                         16 MiB; 64 MiB tripped RSS shed at ~505 feeds)",
                        profile.unwrap_or("default")
                    );
                }
                tracing::warn!(
                    "SSE_FEED_TOTAL_BYTES={v} exceeds the {max}-byte release-safe \
                     maximum for memory profile {:?}",
                    profile.unwrap_or("default")
                );
            }
            Some(_) => {}
        }
    }
    // The descriptor ceiling: the configured SSE cap must fit under
    // nofile_hard with headroom for everything else the process holds.
    // Under the release posture: clamp and emit (the review's
    // acceptable arm) rather than refusing — a platform that lowers
    // the ceiling mid-fleet must not take the whole deployment down at
    // restart. Outside the release posture, warn only.
    //
    // Round-4 follow-up review, finding 1: the runtime reads cap 0 as
    // UNLIMITED, so a degraded descriptor ceiling (nofile_hard <=
    // reserve) must never clamp DOWN to it — and an explicit 0 must
    // never pass release validation. A degraded platform fails CLOSED
    // (refusal), not open.
    if release_posture && *sse_max_connections == 0 {
        anyhow::bail!(
            "SSE_MAX_CONNECTIONS=0 means unlimited; the release posture \
             requires a bounded subscription cap"
        );
    }
    if nofile_hard > 0 {
        if nofile_hard <= FD_RESERVE {
            if release_posture {
                anyhow::bail!(
                    "nofile_hard={nofile_hard} leaves no safe SSE connection capacity \
                     (a {FD_RESERVE}-descriptor reserve is required before any \
                     subscription budget)"
                );
            }
            tracing::warn!(
                "nofile_hard={nofile_hard} leaves no safe SSE connection capacity \
                 after the {FD_RESERVE}-descriptor reserve"
            );
        } else {
            let ceiling = nofile_hard - FD_RESERVE;
            if *sse_max_connections > ceiling {
                if release_posture {
                    tracing::warn!(
                        "SSE_MAX_CONNECTIONS={} exceeds what nofile_hard={nofile_hard} can carry \
                         with a {FD_RESERVE}-descriptor reserve; clamping the effective cap to {ceiling} \
                         (raise RLIMIT_NOFILE or lower SSE_MAX_CONNECTIONS)",
                        *sse_max_connections
                    );
                    *sse_max_connections = ceiling;
                } else {
                    tracing::warn!(
                        "SSE_MAX_CONNECTIONS={} exceeds what nofile_hard={nofile_hard} can carry \
                         with a {FD_RESERVE}-descriptor reserve; descriptor exhaustion wedges \
                         parked subscriptions (~1.5k seen in the field)",
                        *sse_max_connections
                    );
                }
            }
        }
    }
    Ok(())
}

/// Cross-knob validation of a SlateDB `Settings` before any engine opens.
///
/// CHAOS-2 (2026-08-09): SlateDB validates `max_unflushed_bytes >
/// l0_sst_size_bytes` at OPEN time, and a shard engine opens lazily on
/// first use. An invalid combination therefore boots cleanly, logs a
/// healthy memory budget, answers `/health` with `ok`, accepts stream
/// CREATION (the registry DB has its own valid settings) — and then
/// fails EVERY append with a 500, forever, with the only evidence a
/// `WARN` line per attempt. The shipped defaults were themselves
/// invalid (l0 32 MiB vs unflushed 16 MiB), so a bare `streams-slate`
/// with no environment was a permanently broken data plane that
/// reported itself healthy.
///
/// Refuse to start instead. This is deliberately fail-fast rather than
/// clamp-and-continue: the memory posture is an operator declaration
/// that the acceptance campaign verifies knob-for-knob against
/// `deploy/profiles/compute-1g.env`, so silently substituting a
/// different value would make that verification a lie. A crash-loop is
/// loud, greppable, and stops a bad rollout at the first instance.
pub(crate) fn validate_engine_settings(what: &str, s: &Settings) -> anyhow::Result<()> {
    if s.max_unflushed_bytes <= s.l0_sst_size_bytes {
        anyhow::bail!(
            "{what} settings are invalid: max_unflushed_bytes ({}) must be GREATER than \
             l0_sst_size_bytes ({}) — SlateDB rejects this at every engine open, which \
             would leave this process answering /health with `ok` while failing every \
             append. Raise MAX_UNFLUSHED_BYTES above L0_SST_SIZE_BYTES, or lower \
             L0_SST_SIZE_BYTES below it (the field-validated 1 GiB posture is \
             L0_SST_SIZE_BYTES=8388608 with MAX_UNFLUSHED_BYTES=16777216; see \
             deploy/profiles/compute-1g.env).",
            s.max_unflushed_bytes,
            s.l0_sst_size_bytes,
        );
    }
    if s.l0_sst_size_bytes == 0 {
        anyhow::bail!("{what} settings are invalid: l0_sst_size_bytes must be > 0");
    }
    if s.l0_max_ssts == 0 {
        anyhow::bail!("{what} settings are invalid: l0_max_ssts must be > 0");
    }
    Ok(())
}

/// Bounds for the effective request-body ceiling (PR 3.2.1: pure and
/// proven BEFORE any process-global initialization; the wire pin and
/// usable floor live in [`crate::protocol_pin`]). The runtime installer
/// (`http::install_max_body_bytes`) is infallible by design — this is
/// the only place the bounds are asserted.
pub(crate) fn validate_body_ceiling(v: usize) -> Result<(), String> {
    let pin = crate::protocol_pin::MAX_BODY_BYTES;
    let floor = crate::protocol_pin::MIN_BODY_BYTES;
    if v > pin {
        return Err(format!(
            "MAX_REQUEST_BODY_BYTES ({v}) exceeds the pinned protocol ceiling \
             ({pin}); the limit may only be lowered"
        ));
    }
    if v < floor {
        return Err(format!(
            "MAX_REQUEST_BODY_BYTES ({v}) is below the {floor}-byte floor"
        ));
    }
    Ok(())
}

/// A PROVEN effective initial shard count (PR 3.2.1): nonzero and a
/// power of two — the rendezvous layout's requirement, formerly an
/// assert deep inside fresh-topology initialization. The value is
/// already resolved against the fleet-mode default, so downstream code
/// consumes it without re-deriving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialShards(std::num::NonZeroUsize);

impl InitialShards {
    pub fn new(n: usize) -> Result<Self, String> {
        let nz = std::num::NonZeroUsize::new(n)
            .ok_or_else(|| "INITIAL_SHARDS must be >= 1".to_string())?;
        if !n.is_power_of_two() {
            return Err(format!(
                "INITIAL_SHARDS={n} must be a power of two (the rendezvous \
                 shard layout derives its bit-width from it)"
            ));
        }
        Ok(Self(nz))
    }

    pub fn get(&self) -> usize {
        self.0.get()
    }
}

/// Every configuration problem found by [`crate::config::ServerConfig::validate`]
/// — all of them, not just the first, so one boot attempt reports one
/// complete list (PR 3.2). Library code returns this; the binary decides
/// how to print it and what exit status to use.
#[derive(Debug)]
pub struct ConfigError {
    errors: Vec<String>,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "configuration invalid ({} problem(s)):",
            self.errors.len()
        )?;
        for e in &self.errors {
            writeln!(f, "  - {e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

/// A [`crate::config::ServerConfig`] whose invariants have been proven
/// (PR 3.2): the only way to construct one is
/// [`crate::config::ServerConfig::validate`], and [`run`] accepts only
/// this type — so validation is complete before any process-global
/// initialization, store opening, canary write, or task spawn, by
/// construction rather than by call-site discipline. Carries the values
/// validation had to derive anyway, so bootstrap never re-parses (and
/// can never disagree with what was validated).
pub struct ValidatedServerConfig {
    pub(crate) config: crate::config::ServerConfig,
    pub(crate) tenant: crate::tenant::ProjectId,
    pub(crate) cell_id: crate::tenant::CellId,
    pub(crate) auth_mode: crate::auth::AuthMode,
    pub(crate) catalog_cursor_key: Option<[u8; 32]>,
    pub(crate) cert_sealed_publish_delay_ms: u64,
    pub(crate) initial_shards: InitialShards,
}

impl ValidatedServerConfig {
    /// The proven configuration graph (read-only).
    pub fn config(&self) -> &crate::config::ServerConfig {
        &self.config
    }
}

impl crate::config::ServerConfig {
    /// Prove the parsed configuration internally consistent (PR 3.2).
    /// Pure over the configuration value: no environment reads, no
    /// stores, no spawns, no process termination — every problem is
    /// collected and returned. OS-resource checks that need a live
    /// probe (the `nofile` descriptor clamp) run in [`run`]'s preflight
    /// stage instead, before any process-global initialization.
    pub fn validate(self) -> Result<ValidatedServerConfig, ConfigError> {
        let mut errors = Vec::new();

        // R28: SWEEP_MAINT_RESIDENT=0 would silently starve every cold
        // debt class (the rotation would open and immediately close
        // each indebted engine, so no absorber lives long enough to
        // drain). The config stores the raw value; the billing adapter
        // floors at use.
        if self.billing.sweep_maint_resident == 0 {
            errors.push(
                "SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; \
                 set >= 1 or unset (default 2)"
                    .to_string(),
            );
        }
        // R28: a certified survival deploy must fail at boot, not OOM
        // at +28 min, if any memory knob was dropped or overridden.
        errors.extend(certified_memprofile_errors(&self));
        // A configuration SlateDB will reject at open time must stop
        // here, not turn into a permanently-500 data plane behind an
        // `ok` health check (CHAOS-2). Both engine tiers go through the
        // same check so a future edit to either cannot reintroduce the
        // hole.
        for (what, settings) in [
            ("shard", shard_settings(&self.cli, &self.engine)),
            (
                "history",
                crate::history::history_settings(&self.history, &self.engine.compactor_options()),
            ),
        ] {
            if let Err(e) = validate_engine_settings(what, &settings) {
                errors.push(format!("{e}"));
            }
        }
        // MULTITENANCY transition posture: the deployment tenant is
        // EXPLICIT, validated config — layout-4 paths and hashes derive
        // from it, so an invalid or reserved value refuses boot loudly
        // instead of writing a mis-keyed namespace.
        let tenant = match crate::tenant::ProjectId::new(&self.cli.project_id) {
            Ok(t) if t.is_system() => {
                errors.push("PROJECT_ID may not be the reserved system project".to_string());
                None
            }
            Ok(t) => Some(t),
            Err(e) => {
                errors.push(format!(
                    "PROJECT_ID {:?} is invalid: {e}",
                    self.cli.project_id
                ));
                None
            }
        };
        // §2: the telemetry cell identity — formerly re-validated (and
        // expect-ed) inside Registry::new; the proof now lives here and
        // Registry consumes the typed value.
        let cell_id = match crate::tenant::CellId::new(&self.cli.cell_id) {
            Ok(c) => Some(c),
            Err(e) => {
                errors.push(format!("CELL_ID {:?} is invalid: {e}", self.cli.cell_id));
                None
            }
        };
        // The effective body ceiling (pure bounds; the installer is
        // infallible). CHAOS-3: this value also sizes the absorber's
        // worst-frame reservation, so it must be right BEFORE any
        // process-global budget reads it.
        if let Err(e) = validate_body_ceiling(self.cli.max_request_body_bytes) {
            errors.push(e);
        }
        // The effective initial shard count, resolved against the
        // fleet-mode default and proven (nonzero power of two). The
        // coarse-topology warning moves here with it.
        let fleet_mode = self.cli.fleet_prefix.is_some() && self.cli.fleet_max > 1;
        let effective_shards = match self.cli.initial_shards {
            Some(n) => {
                if fleet_mode && n < 4 * self.cli.fleet_max as usize {
                    tracing::warn!(
                        "INITIAL_SHARDS={n} < 4×FLEET_MAX={}: a fresh topology this \
                         coarse draws unevenly under rendezvous and the rebalancer \
                         flaps against return-home; use >= {}",
                        self.cli.fleet_max,
                        (4 * self.cli.fleet_max as usize).next_power_of_two()
                    );
                }
                n
            }
            None if fleet_mode => (4 * self.cli.fleet_max as usize).next_power_of_two(),
            None => 1,
        };
        let initial_shards = match InitialShards::new(effective_shards) {
            Ok(s) => Some(s),
            Err(e) => {
                errors.push(e);
                None
            }
        };
        // Round-21: production billing must never silently attribute a
        // customer's traffic to the placeholder tenant — the PURE
        // billing-required prerequisites are proven here; the spool and
        // rollup OPENS (store I/O) stay in bootstrap.
        if self.cli.billing_mode == "required" {
            if self.cli.usage_stream_key.is_none() {
                errors.push(
                    "BILLING_MODE=required needs USAGE_STREAM_KEY — production \
                     billing refuses to run without the usage ledger (§14.1)"
                        .to_string(),
                );
            }
            if self.cli.account_id == "acct_local"
                || self.cli.project_id == "proj_local"
                || self.cli.cell_id == "local"
            {
                errors.push(
                    "BILLING_MODE=required needs explicit ACCOUNT_ID, PROJECT_ID \
                     and CELL_ID — refusing to bill production traffic to the \
                     local placeholders"
                        .to_string(),
                );
            }
        }
        // MULTITENANCY Stage 5: the auth service exists in every mode
        // (Off is inert).
        let auth_mode =
            match crate::auth::AuthMode::from_env(Some(self.cli.streams_auth_mode.as_str())) {
                Ok(m) => Some(m),
                Err(e) => {
                    errors.push(format!("{e}"));
                    None
                }
            };
        if auth_mode.is_some_and(|m| m != crate::auth::AuthMode::Off) {
            // Review item: the local placeholder tenant must never
            // reach a shadow/enforce deployment — proj_local silently
            // naming a real project's data is exactly the accident
            // this refuses.
            if self.cli.project_id == "proj_local" {
                errors.push(format!(
                    "STREAMS_AUTH_MODE={} requires an explicit non-default PROJECT_ID",
                    self.cli.streams_auth_mode
                ));
            }
            if !(self.cli.streams_auth_keys_file.is_some()
                && self.cli.streams_auth_policy_file.is_some()
                && self.cli.streams_auth_grants_file.is_some())
            {
                errors.push(format!(
                    "STREAMS_AUTH_MODE={} requires STREAMS_AUTH_KEYS_FILE, \
                     STREAMS_AUTH_POLICY_FILE and STREAMS_AUTH_GRANTS_FILE",
                    self.cli.streams_auth_mode
                ));
            }
            // The refresher cadence must clear the staleness window
            // with room for a failed fetch or two, or the cell
            // oscillates into fail-closed refusals on schedule.
            if (self.cli.streams_auth_refresh_secs as i64)
                > crate::auth::POLICY_STALENESS_MAX_SECS / 3
            {
                errors.push(format!(
                    "STREAMS_AUTH_REFRESH_SECS={} must be <= {} (a third of the \
                     {}s staleness window)",
                    self.cli.streams_auth_refresh_secs,
                    crate::auth::POLICY_STALENESS_MAX_SECS / 3,
                    crate::auth::POLICY_STALENESS_MAX_SECS
                ));
            }
        }
        let catalog_cursor_key: Option<[u8; 32]> = match &self.cli.streams_cursor_key {
            None => None,
            Some(b64) => {
                use base64::Engine;
                match base64::engine::general_purpose::STANDARD.decode(b64) {
                    Err(e) => {
                        errors.push(format!("STREAMS_CURSOR_KEY is not base64: {e}"));
                        None
                    }
                    Ok(raw) => match <[u8; 32]>::try_from(raw.as_slice()) {
                        Ok(k) => Some(k),
                        Err(_) => {
                            errors.push(
                                "STREAMS_CURSOR_KEY must decode to exactly 32 bytes".to_string(),
                            );
                            None
                        }
                    },
                }
            }
        };
        // FAIL CLOSED (round-19 security): fleet mode must not start
        // without its own internal credential.
        if let Err(e) = validate_fleet_auth(&self.cli, fleet_mode) {
            errors.push(format!("{e}"));
        }
        if let Err(e) = validate_record_ceiling(
            &self.sse,
            self.cli.release_posture,
            self.cli.max_record_payload_bytes,
        ) {
            errors.push(format!("{e}"));
        }
        // The pure half of the release-capacity posture: nofile_hard=0
        // means "no descriptor probe", so only the feed-budget and
        // bounded-cap checks run here. The live descriptor clamp is
        // run()'s preflight.
        {
            let mut cap_probe = self.cli.sse_max_connections;
            if let Err(e) = validate_release_capacity(
                self.cli.release_posture,
                self.runtime.memprofile_cert.as_deref(),
                self.sse.feed_total_bytes_raw.as_deref(),
                &mut cap_probe,
                0,
            ) {
                errors.push(format!("{e}"));
            }
        }
        // Round-11.6: the seal-publication delay is a CERTIFICATION
        // instrument, never a production knob.
        let cert_sealed_publish_delay_ms = match cert_sealed_publish_delay_from(
            self.runtime.cert_sealed_publish_delay_ms_raw.as_deref(),
            self.runtime.certification_mode.as_deref(),
        ) {
            Ok(ms) => Some(ms),
            Err(e) => {
                errors.push(format!("{e}"));
                None
            }
        };

        if !errors.is_empty() {
            return Err(ConfigError { errors });
        }
        Ok(ValidatedServerConfig {
            tenant: tenant.expect("no errors implies tenant parsed"),
            cell_id: cell_id.expect("no errors implies cell id parsed"),
            auth_mode: auth_mode.expect("no errors implies auth mode parsed"),
            catalog_cursor_key,
            cert_sealed_publish_delay_ms: cert_sealed_publish_delay_ms
                .expect("no errors implies delay parsed"),
            initial_shards: initial_shards.expect("no errors implies shards proven"),
            config: self,
        })
    }
}
