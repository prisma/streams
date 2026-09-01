//! Binary bootstrap: store opening, validation, service construction and
//! task startup (WP-01/PR 2: moved out of src/main.rs; PR 3.1: takes the
//! owned [`crate::config::ServerConfig`]). The binary calls exactly one
//! entry point: [`run`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::Db;
use slatedb::config::Settings;

use crate::config::CliArgs;
use crate::history::{Absorber, AbsorberConfig, KeyCache, absorber_channel};
use crate::http::AppState;
use crate::registry::{Registry, load_or_init_topology};
use crate::shard::{ShardConfig, ShardEngine};

impl crate::config::ServerConfig {
    fn raw_store(&self, bucket: &Option<String>) -> anyhow::Result<AmazonS3> {
        let bucket = bucket.as_deref().unwrap_or(&self.cli.bucket);
        AmazonS3Builder::new()
            .with_endpoint(&self.cli.s3_endpoint)
            .with_bucket_name(bucket)
            .with_region(&self.cli.region)
            .with_access_key_id(&self.cli.access_key_id)
            .with_secret_access_key(&self.cli.secret_access_key)
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            // Idle pooled connections die silently across scale-to-zero
            // snapshot/restore; expiring them just under the platform's 5 s
            // idle threshold means a restored image wakes with an empty
            // pool instead of dead sockets (EXPERIMENT-PILOT.md). The pool
            // is shared by every shard/stream on the instance, and manifest
            // polling keeps it warm whenever any shard is open — the cold
            // path only bites fully-idle instances. POOL_IDLE_SECS exists
            // so production fleets can lift this once the platform stops
            // killing idle flows (2026-07 plan); until then keep <5.
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(true) // ClientOptions REPLACES the builder's allow_http
                    .with_pool_idle_timeout(Duration::from_secs(self.storage.pool_idle_secs)),
            )
            // Records Tigris's Server-Timing (their internal ms) and
            // x-tigris-served-from per response → sp50/sp99 + served_from
            // in /v1/debug/store. wall − server = network path.
            .with_http_connector(crate::store_timing::SniffConnector)
            .build()
            .context("build s3 object store")
    }

    // TimingStore sits beneath PrefixStore so it times final, fully-prefixed
    // paths (O14a split: our pipeline vs egress path vs Tigris). All stores
    // share one global gauge — the egress budget is per instance.
    fn store_for(&self, bucket: &Option<String>) -> anyhow::Result<Arc<dyn ObjectStore>> {
        let s3 = crate::store_timing::TimingStore::new(self.raw_store(bucket)?);
        Ok(match &self.cli.path_prefix {
            Some(p) => Arc::new(object_store::prefix::PrefixStore::new(s3, p.as_str())),
            None => Arc::new(s3),
        })
    }

    /// Fleet-coordination store (heartbeats, desired.json): shared across
    /// instances, so prefixed by --fleet-prefix, not --path-prefix.
    fn fleet_store(&self) -> anyhow::Result<Option<Arc<dyn ObjectStore>>> {
        let Some(p) = &self.cli.fleet_prefix else {
            return Ok(None);
        };
        let s3 = crate::store_timing::TimingStore::new(self.raw_store(&None)?);
        Ok(Some(Arc::new(object_store::prefix::PrefixStore::new(
            s3,
            p.as_str(),
        ))))
    }
}

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

pub fn assert_certified_memprofile(cfg: &crate::config::ServerConfig) {
    if cfg.runtime.memprofile_cert.as_deref() != Some("compute-1g") {
        return;
    }
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
            eprintln!(
                "Error: MEMPROFILE_CERT=compute-1g but {k}={got} (certified {want}) — \
                 the deploy dropped or overrode a survival knob; refusing to start"
            );
            std::process::exit(1);
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
            eprintln!("Error: MEMPROFILE_CERT: {family} settings disable the compactor");
            std::process::exit(1);
        };
        let w = co.worker.clone().unwrap_or_default();
        if w.max_subcompactions != cert.max_subcompactions
            || w.max_fetch_tasks != cert.max_fetch_tasks
            || w.bytes_to_fetch != cert.bytes_to_fetch
            || w.max_sst_size != cert.max_sst_size
            || w.max_concurrent_compactions != cert.max_concurrent_compactions
        {
            eprintln!(
                "Error: MEMPROFILE_CERT: {family} settings carry a different \
                 compaction worker profile than the certified one; refusing to start"
            );
            std::process::exit(1);
        }
    }
    tracing::info!(profile = %p, "memory profile certified: compute-1g (all DB families)");
}

#[cfg(test)]
mod memprofile_tests {
    use clap::Parser;

    /// Structural: every production settings family hands its builder
    /// the ONE resolved worker profile — a family that regresses to
    /// `Settings::default().compactor_options` (history, R29) or
    /// `..Default::default()` (telemetry, R28) fails here.
    #[test]
    fn every_db_family_carries_the_resolved_compactor_profile() {
        let cfg = crate::config::ServerConfig::load(
            crate::config::CliArgs::parse_from([
                "streams-slate",
                "--s3-endpoint",
                "http://127.0.0.1:1",
            ]),
            &crate::config::MapEnvironment::empty(),
        );
        let cert = super::resolved_compactor_options(&cfg.engine)
            .worker
            .clone()
            .unwrap_or_default();
        for (family, co) in super::production_settings_families(&cfg) {
            let co = co.unwrap_or_else(|| panic!("{family}: compactor disabled"));
            let w = co.worker.clone().unwrap_or_default();
            assert_eq!(w.max_subcompactions, cert.max_subcompactions, "{family}");
            assert_eq!(w.max_fetch_tasks, cert.max_fetch_tasks, "{family}");
            assert_eq!(w.bytes_to_fetch, cert.bytes_to_fetch, "{family}");
            assert_eq!(w.max_sst_size, cert.max_sst_size, "{family}");
            assert_eq!(
                w.max_concurrent_compactions, cert.max_concurrent_compactions,
                "{family}"
            );
        }
    }
}

static SLATEDB_RT_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(2);

/// Composition-root call, before the first SlateDB opens: size the
/// dedicated runtime from the process configuration. Tests never call
/// this and get the default (2), matching the old env-unset default.
pub fn init_slatedb_runtime_threads(threads: usize) {
    SLATEDB_RT_THREADS.store(threads, std::sync::atomic::Ordering::Relaxed);
}

pub fn slatedb_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        let threads = SLATEDB_RT_THREADS.load(std::sync::atomic::Ordering::Relaxed);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("slatedb-rt")
            .enable_all()
            .build()
            .expect("build slatedb runtime")
    })
}

/// Run `fut` to completion on the SlateDB runtime. Used for every
/// `Db::builder(...).build()` / `DbReader` open so all slatedb-internal
/// tasks land on `slatedb_runtime()`'s threads.
pub async fn on_slatedb_rt<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    slatedb_runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.await.expect("slatedb-rt task dropped")
}

fn shard_settings(args: &CliArgs, engine: &crate::config::EngineConfig) -> Settings {
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
/// silently armed canary fault in production).
fn cert_sealed_publish_delay(cfg: &crate::config::RuntimeConfig) -> anyhow::Result<u64> {
    let ms = cert_sealed_publish_delay_from(
        cfg.cert_sealed_publish_delay_ms_raw.as_deref(),
        cfg.certification_mode.as_deref(),
    )?;
    if ms > 0 {
        tracing::warn!(ms, "CERTIFICATION MODE: sealed publication delayed");
    }
    Ok(ms)
}

/// Pure core of `cert_sealed_publish_delay` (unit-testable without
/// process-global env mutation).
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

#[cfg(test)]
mod config_validation_tests {
    use super::*;
    use clap::Parser;

    /// CHAOS-2: the shipped defaults must be openable. The old
    /// L0_SST_SIZE_BYTES default (32 MiB) exceeded the
    /// MAX_UNFLUSHED_BYTES default (16 MiB), so a bare `streams-slate`
    /// with no environment booted, reported `/health` ok, accepted
    /// stream creation, and then failed EVERY append with a 500 for as
    /// long as the process lived.
    /// SR3-1 (round-3 finding 1): the release posture carries NO
    /// permanent shared credential, validated GLOBALLY — the same
    /// rules whether or not fleet mode is on.
    #[test]
    fn release_posture_refuses_every_static_credential_shape() {
        let parse = |extra: &[&str]| {
            let mut v = vec!["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"];
            v.extend_from_slice(extra);
            // try_parse_from: a parse error must FAIL THE TEST, not
            // process::exit(2) the whole suite binary.
            CliArgs::try_parse_from(v).expect("test args must parse")
        };
        // Workload + release + a coexisting static token: refused.
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--streams-auth-mode",
            "enforce",
            "--release-posture",
            "--fleet-internal-token",
            "legacy-token-0123456789",
        ]);
        assert!(
            validate_fleet_auth(&a, false).is_err(),
            "release+workload must refuse a coexisting static token"
        );
        // Static mode under release: refused even single-instance.
        let a = parse(&[
            "--fleet-auth-mode",
            "static",
            "--streams-auth-mode",
            "enforce",
            "--release-posture",
            "--fleet-internal-token",
            "legacy-token-0123456789",
        ]);
        assert!(
            validate_fleet_auth(&a, false).is_err(),
            "release posture must refuse static mode without fleet mode too"
        );
        // Release workload posture without enforce: refused.
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--release-posture",
        ]);
        assert!(
            validate_fleet_auth(&a, false).is_err(),
            "release posture requires STREAMS_AUTH_MODE=enforce"
        );
        // The clean release shape passes, single-instance AND fleet.
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--streams-auth-mode",
            "enforce",
            "--release-posture",
        ]);
        assert!(validate_fleet_auth(&a, false).is_ok());
        assert!(validate_fleet_auth(&a, true).is_ok());
        // Non-release migration coexistence stays allowed (boot only;
        // the runtime gate still refuses the static bearer in
        // workload mode).
        let a = parse(&[
            "--fleet-auth-mode",
            "workload",
            "--workload-token-file",
            "/run/w.jwt",
            "--fleet-internal-token",
            "legacy-token-0123456789",
        ]);
        assert!(validate_fleet_auth(&a, false).is_ok());
        // Static fleet mode off-release keeps its existing rules.
        let a = parse(&["--fleet-internal-token", "legacy-token-0123456789"]);
        assert!(validate_fleet_auth(&a, true).is_ok());
        let a = parse(&[]);
        assert!(
            validate_fleet_auth(&a, true).is_err(),
            "static fleet mode still requires the token"
        );
    }

    /// Round-11.6: the seal-publication delay is a certification
    /// instrument — armed without STREAMS_CERTIFICATION_MODE=1 it
    /// refuses boot; unset and malformed shapes behave predictably.
    #[test]
    fn cert_sealed_publish_delay_is_gated_on_certification_mode() {
        assert_eq!(cert_sealed_publish_delay_from(None, None).unwrap(), 0);
        assert_eq!(cert_sealed_publish_delay_from(Some("0"), None).unwrap(), 0);
        assert!(cert_sealed_publish_delay_from(Some("500"), None).is_err());
        assert!(cert_sealed_publish_delay_from(Some("500"), Some("0")).is_err());
        assert_eq!(
            cert_sealed_publish_delay_from(Some("500"), Some("1")).unwrap(),
            500
        );
        assert!(cert_sealed_publish_delay_from(Some("abc"), Some("1")).is_err());
    }

    /// Round-10 review: the release posture requires a per-record
    /// payload ceiling whose worst-case prepared SSE frame fits the
    /// feed ring.
    #[test]
    fn release_posture_requires_a_ring_consistent_record_ceiling() {
        let sse = crate::config::SseConfig::default();
        // Off-release: no ceiling required.
        assert!(validate_record_ceiling(&sse, false, None).is_ok());
        // Release without a ceiling: refused.
        assert!(validate_record_ceiling(&sse, true, None).is_err());
        // Round-10e: ZERO is the unlimited sentinel — refused.
        assert!(validate_record_ceiling(&sse, true, Some(0)).is_err());
        // Round-10e: an overflow-inducing ceiling is refused, not
        // wrapped.
        assert!(validate_record_ceiling(&sse, true, Some(usize::MAX)).is_err());
        // Release with a ceiling whose frame exceeds the ring: refused.
        let ring = crate::sse::budget::feed_ring_bytes(&sse);
        assert!(validate_record_ceiling(&sse, true, Some(ring)).is_err());
        // Release with a fitting ceiling: accepted (an eighth of the
        // ring leaves headroom under the 6x worst-case text framing).
        assert!(validate_record_ceiling(&sse, true, Some(ring / 8)).is_ok());
        // The bound covers the TRUE worst framing (round-10e): a
        // newline-heavy text payload (6 bytes of SSE output per input
        // byte), lossy invalid UTF-8 (3 bytes per byte), JSON and
        // binary all stay under worst_prepared_charge.
        let bound = |n: usize| crate::sse::feed::worst_prepared_charge(n).expect("plausible size");
        let text_desc = {
            let mut d = crate::sse::feed::tests::test_desc("wcase");
            d.content_type = "text/plain".into();
            d
        };
        let newlines = vec![b'\n'; 1024];
        assert!(
            crate::sse::wire::sse_data_event(&text_desc, &newlines).len() <= bound(1024),
            "newline-heavy text must fit the worst-case bound"
        );
        let invalid = vec![0xFFu8; 1024];
        assert!(
            crate::sse::wire::sse_data_event(&text_desc, &invalid).len() <= bound(1024),
            "lossy invalid UTF-8 must fit the worst-case bound"
        );
        let bin_desc = {
            let mut d = crate::sse::feed::tests::test_desc("wcase2");
            d.content_type = "application/octet-stream".into();
            d
        };
        assert!(crate::sse::wire::sse_data_event(&bin_desc, &invalid).len() <= bound(1024));
        let json_desc = crate::sse::feed::tests::test_desc("wcase3");
        assert!(
            crate::sse::wire::sse_data_event(&json_desc, &newlines).len() <= bound(1024),
            "json framing must fit the worst-case bound"
        );
    }

    #[test]
    fn shipped_defaults_are_a_valid_engine_configuration() {
        let args = CliArgs::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]);
        validate_engine_settings(
            "shard",
            &shard_settings(&args, &crate::config::EngineConfig::default()),
        )
        .expect("default shard settings must open");
        let cfg = crate::config::ServerConfig::load(
            CliArgs::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]),
            &crate::config::MapEnvironment::empty(),
        );
        validate_engine_settings(
            "history",
            &crate::history::history_settings(&cfg.history, &cfg.engine.compactor_options()),
        )
        .expect("default history settings must open");
    }

    /// Follow-up review finding 4 (red): the release-safe hub-budget
    /// maximum is PROFILE-specific. The 64-MiB rung was exercised but
    /// produced RSS shed on the 1-GiB class, so it must not be that
    /// class's release-safe ceiling.
    #[test]
    fn hub_budget_maximum_is_profile_specific() {
        const THIRTY_TWO_MIB: &str = "33554432";
        // Round-12: the LiveFeed retention model is exactly accounted
        // (bounded reservation per retained batch, released on
        // eviction), and the perf study certified 64 MiB on the 1-GiB
        // class: 10,000 parked subscribers idle at 307 MB, peak
        // 399 MB, 101 MB below the shed line (docs/PERF-LIVEFEED.md
        // §3). The old 16 MiB ceiling was the HUB's uncertainty.
        let mut cap = 10_000;
        validate_release_capacity(
            true,
            Some("compute-1g"),
            Some("67108864"),
            &mut cap,
            u32::MAX as u64,
        )
        .unwrap();
        // Above the newly certified 64 MiB: still refused.
        let mut cap = 10_000;
        assert!(
            validate_release_capacity(
                true,
                Some("compute-1g"),
                Some("134217728"),
                &mut cap,
                u32::MAX as u64
            )
            .is_err(),
            "the 1-GiB profile must refuse a feed budget above its certified 64 MiB"
        );
        // Unknown/default profile + release + 32 MiB: allowed (largest
        // EXERCISED envelope until that tier certifies its own).
        let mut cap = 10_000;
        validate_release_capacity(true, None, Some(THIRTY_TWO_MIB), &mut cap, u32::MAX as u64)
            .unwrap();
        // Non-release warns only.
        let mut cap = 10_000;
        validate_release_capacity(
            false,
            Some("compute-1g"),
            Some(THIRTY_TWO_MIB),
            &mut cap,
            u32::MAX as u64,
        )
        .unwrap();
        // The certified posture lands everywhere.
        let mut cap = 10_000;
        validate_release_capacity(
            true,
            Some("compute-1g"),
            Some("16777216"),
            &mut cap,
            u32::MAX as u64,
        )
        .unwrap();
    }

    /// Round-4 follow-up review, finding 1 (red): the runtime reads
    /// SSE_MAX_CONNECTIONS=0 as UNLIMITED, so neither the validator's
    /// own clamp nor an explicit zero may ever produce it under the
    /// release posture. A degraded platform fails closed.
    #[test]
    fn release_capacity_never_turns_the_sse_gate_off() {
        // Explicit cap 0 + release posture: boot refusal.
        let mut cap = 0u64;
        assert!(
            validate_release_capacity(true, None, None, &mut cap, u32::MAX as u64).is_err(),
            "release posture must refuse an unlimited subscription budget"
        );
        // Non-release cap 0 remains allowed and untouched.
        let mut cap = 0u64;
        validate_release_capacity(false, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(cap, 0);
        // A degraded ceiling must not clamp DOWN to zero (=unlimited):
        // nofile_hard == FD_RESERVE refuses; below it refuses too.
        let mut cap = 10_000;
        assert!(validate_release_capacity(true, None, None, &mut cap, FD_RESERVE).is_err());
        let mut cap = 10_000;
        assert!(validate_release_capacity(true, None, None, &mut cap, FD_RESERVE - 1).is_err());
        // Non-release only warns.
        let mut cap = 10_000;
        validate_release_capacity(false, None, None, &mut cap, FD_RESERVE).unwrap();
        assert_eq!(cap, 10_000);
        // The first usable ceiling above the reserve clamps to it.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, FD_RESERVE + 1).unwrap();
        assert_eq!(cap, 1);
        // The observed Compute-class shape is unchanged.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(cap, 3_072);
    }

    /// Round-4 review: release-posture capacity validation — the hub
    /// budget stays inside the field-certified envelope, a typo'd byte
    /// count never silently becomes the default, and the SSE
    /// connection cap clamps to what nofile_hard can carry.
    #[test]
    fn release_capacity_validates_hub_budget_and_fd_ceiling() {
        // An explicit hub budget above the certified envelope: refused
        // under the release posture, warned outside it.
        let mut cap = 10_000u64;
        assert!(validate_release_capacity(true, None, Some("134217728"), &mut cap, 0).is_err());
        assert!(validate_release_capacity(false, None, Some("134217728"), &mut cap, 0).is_ok());
        // The certified postures pass (16 MiB default is implicit).
        let mut cap = 10_000;
        assert!(validate_release_capacity(true, None, Some("16777216"), &mut cap, 65_536).is_ok());
        assert!(validate_release_capacity(true, None, None, &mut cap, 65_536).is_ok());
        // A typo'd value must not silently become the default.
        assert!(validate_release_capacity(false, None, Some("16 MiB"), &mut cap, 0).is_err());
        // The Compute-class ceiling: hard 4,096 with a 1,024 reserve
        // clamps the configured 10k to 3,072 under the release posture.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(
            cap, 3_072,
            "effective cap must fit nofile_hard minus reserve"
        );
        // Outside the release posture the configured value stands.
        let mut cap = 10_000;
        validate_release_capacity(false, None, None, &mut cap, 4_096).unwrap();
        assert_eq!(cap, 10_000);
        // A generous ceiling leaves the cap alone.
        let mut cap = 10_000;
        validate_release_capacity(true, None, None, &mut cap, u32::MAX as u64).unwrap();
        assert_eq!(cap, 10_000);
    }

    /// CHAOS-3: the body ceiling is a capacity knob. Lowering it must
    /// shrink the absorber reservation that every gather holds against
    /// the shed line, and it must never be raisable above the pin.
    #[test]
    fn body_ceiling_sizes_the_absorber_reservation_and_only_lowers() {
        use crate::history::worst_frame_transient_for;
        let pinned = crate::http::MAX_BODY_BYTES;
        let at_pin = worst_frame_transient_for(pinned);
        assert!(
            at_pin > 96 * 1024 * 1024,
            "the pinned reservation is the ~96 MiB measured in the field, got {at_pin}"
        );
        let lowered = worst_frame_transient_for(1024 * 1024);
        assert!(
            lowered * 25 < at_pin,
            "a 1 MiB ceiling must shrink the reservation by more than 25x: \
             {lowered} vs {at_pin}"
        );

        // The live wiring reads the same rule, so the freed bytes are
        // real admission headroom and not a floor that clamps back.
        assert_eq!(
            crate::history::absorb_worst_frame_transient(),
            worst_frame_transient_for(crate::http::max_body_bytes())
        );
        assert_eq!(
            crate::history::floored_budget_capacity(0),
            crate::history::absorb_worst_frame_transient()
        );

        assert!(
            crate::http::set_max_body_bytes(pinned + 1).is_err(),
            "the protocol ceiling must not be raisable"
        );
        assert!(
            crate::http::set_max_body_bytes(1024).is_err(),
            "floor holds"
        );
        assert_eq!(
            crate::http::max_body_bytes(),
            pinned,
            "a rejected setting must not have taken effect"
        );
    }

    #[test]
    fn unflushed_at_or_below_l0_is_rejected_before_any_engine_opens() {
        let mut args =
            CliArgs::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]);
        args.l0_sst_size_bytes = 32 * 1024 * 1024;
        args.max_unflushed_bytes = 16 * 1024 * 1024;
        let err = validate_engine_settings(
            "shard",
            &shard_settings(&args, &crate::config::EngineConfig::default()),
        )
        .expect_err("l0 above unflushed must be refused at startup");
        let msg = format!("{err}");
        assert!(msg.contains("max_unflushed_bytes"), "unhelpful: {msg}");
        assert!(msg.contains("L0_SST_SIZE_BYTES"), "no remedy named: {msg}");

        // Equality is just as fatal as inversion — SlateDB requires a
        // strict inequality.
        args.max_unflushed_bytes = args.l0_sst_size_bytes;
        validate_engine_settings(
            "shard",
            &shard_settings(&args, &crate::config::EngineConfig::default()),
        )
        .expect_err("equal sizes must be refused too");
    }
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
fn validate_engine_settings(what: &str, s: &Settings) -> anyhow::Result<()> {
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

/// The server bootstrap: the composition root hands in ONE owned,
/// immutable [`crate::config::ServerConfig`]; this function constructs
/// stores, validates, builds the runtime owners and serves. Called from
/// the binary's `run` facade; tests drive owners directly, not this.
pub async fn run(mut config: crate::config::ServerConfig) -> anyhow::Result<()> {
    // R28: a certified survival deploy must fail at boot, not OOM at
    // +28 min, if any memory knob was dropped or overridden.
    assert_certified_memprofile(&config);
    tracing::info!(config = %config.redacted_summary(), "effective configuration (redacted)");
    tracing::info!(
        model = %crate::quota::pressure_model_json(),
        "project memory-pressure model (round-13; weights are code-versioned)"
    );
    init_slatedb_runtime_threads(config.engine.slatedb_rt_threads);
    // Process-global infrastructure sized once from the owned config
    // (WP-01 PR 3.1): the absorber budget, the shared caches (history,
    // telemetry, postings), the usage limits, the scaler policy, the
    // store egress gates, and the debug pause flag's INITIAL value.
    // Each holder documents why it is process-global; un-seeded tests
    // get the old defaults.
    crate::history::init_absorb_pause(config.history.absorb_pause_initial);
    crate::history::init_absorb_budget(&config.history);
    crate::history::init_history_cache(config.history.cache_bytes);
    crate::billing::init_telemetry_cache(config.billing.telemetry_cache_bytes);
    crate::postings_cache::init_postings_cache(config.postings.cache_bytes);
    crate::usage::init_limits(&config.admission);
    crate::scaler3::init_policy(&config.scaler);
    crate::store_timing::configure(&config.storage);

    // FIRST: the body ceiling sizes the absorber's worst-frame
    // reservation, which floors the process-wide budget. It must be
    // fixed before anything reads either (CHAOS-3).
    crate::http::set_max_body_bytes(config.cli.max_request_body_bytes)?;

    // Before anything opens a store: a configuration that SlateDB will
    // reject at open time must stop the process here, not turn into a
    // permanently-500 data plane behind an `ok` health check (CHAOS-2).
    // Both engine tiers go through the same check so a future edit to
    // either one cannot reintroduce the hole.
    validate_engine_settings("shard", &shard_settings(&config.cli, &config.engine))?;
    validate_engine_settings(
        "history",
        &crate::history::history_settings(&config.history, &config.engine.compactor_options()),
    )?;

    let ops_store = config.store_for(&config.cli.ops_bucket)?;
    let shard_store = config.store_for(&config.cli.shard_bucket)?;
    let data_store = config.store_for(&config.cli.data_bucket)?;

    // R23-5: a synchronous storage canary, BEFORE we bind.
    //
    // The /health readiness signal only fires for failures that reach a
    // shard open. A registry or control-plane storage failure refuses
    // requests earlier, so `shard_opens.started` stays 0 and readiness
    // stays silent — verified in the field by killing the object store
    // after boot. This closes that gap at the only moment it is cheap:
    // prove each bucket is usable, and refuse to start if it is not.
    //
    // Deliberately a write AND a read-back on every bucket we depend on.
    // Credentials that can read but not write are a real and silent
    // failure mode that would otherwise surface as a 500 per append
    // forever — which is the whole CHAOS-2 disease.
    let canary_prefix = config.cli.path_prefix.clone().unwrap_or_default();
    for (label, store) in [
        ("ops", ops_store.clone()),
        ("shard", shard_store.clone()),
        ("data", data_store.clone()),
    ] {
        let store: Arc<dyn ObjectStore> = store;
        // R24-E: the canary key must be unique per INSTANCE-INCARNATION,
        // not per PID. Firecracker VMs commonly start at the same pid, so
        // two instances sharing a namespace would collide on one object:
        // A puts, B puts, A deletes, B reads -> missing, and B refuses to
        // start for a store that is perfectly healthy.
        let probe = object_store::path::Path::from(format!(
            "{}_canary/{}-{}-{}",
            canary_prefix.trim_end_matches('/'),
            config.cli.instance_name.replace('/', "_"),
            std::process::id(),
            // A boot-unique nonce. No new dependency: the wall clock in
            // nanos plus the instance name already distinguishes two
            // VMs that happen to share a pid, and a create-only put
            // below turns any residual collision into a loud error
            // rather than a silent overwrite.
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        ));
        let payload = b"streams-startup-canary".to_vec();
        store
            .put_opts(
                &probe,
                object_store::PutPayload::from(payload.clone()),
                object_store::PutOptions::from(object_store::PutMode::Create),
            )
            .await
            .with_context(|| {
                format!(
                    "startup canary: cannot WRITE to the {label} bucket — this process \
                     would have booted, answered /health with ok, and failed every append"
                )
            })?;
        let got = store
            .get(&probe)
            .await
            .with_context(|| format!("startup canary: cannot READ BACK from the {label} bucket"))?
            .bytes()
            .await
            .with_context(|| format!("startup canary: {label} read-back body failed"))?;
        if got.as_ref() != payload.as_slice() {
            anyhow::bail!(
                "startup canary: {label} bucket returned {} bytes, expected {} — \
                 this store is not durable for this process",
                got.len(),
                payload.len()
            );
        }
        let _ = store.delete(&probe).await; // best effort
    }
    tracing::info!("startup canary: ops/shard/data buckets readable and writable");
    // R23-5: and if we ever DO end up unready with no shard ever opened,
    // exit rather than sit in rotation-limbo (see spawn_unready_watchdog).
    crate::sharddir::spawn_unready_watchdog(&config.shard);

    let registry = Registry::new(ops_store.clone(), &config.cli.cell_id);
    // MULTITENANCY transition posture: the deployment tenant is
    // EXPLICIT, validated config — layout-4 paths and hashes derive
    // from it, so an invalid or reserved value refuses boot loudly
    // instead of writing a mis-keyed namespace.
    let tenant = crate::tenant::ProjectId::new(&config.cli.project_id)
        .unwrap_or_else(|e| panic!("PROJECT_ID {:?} is invalid: {e}", config.cli.project_id));
    if tenant.is_system() {
        panic!("PROJECT_ID may not be the reserved system project");
    }
    // MULTITENANCY Stage 5: the auth service exists in every mode (Off
    // is inert); feeds are wired below once the runtime is up.
    let auth_mode = crate::auth::AuthMode::from_env(Some(config.cli.streams_auth_mode.as_str()))?;
    if auth_mode != crate::auth::AuthMode::Off {
        // Review item: the local placeholder tenant must never reach a
        // shadow/enforce deployment — those are the multi-tenant
        // postures, and proj_local silently naming a real project's
        // data is exactly the accident this refuses.
        anyhow::ensure!(
            config.cli.project_id != "proj_local",
            "STREAMS_AUTH_MODE={} requires an explicit non-default PROJECT_ID",
            config.cli.streams_auth_mode
        );
        anyhow::ensure!(
            config.cli.streams_auth_keys_file.is_some()
                && config.cli.streams_auth_policy_file.is_some()
                && config.cli.streams_auth_grants_file.is_some(),
            "STREAMS_AUTH_MODE={} requires STREAMS_AUTH_KEYS_FILE,              STREAMS_AUTH_POLICY_FILE and STREAMS_AUTH_GRANTS_FILE",
            config.cli.streams_auth_mode
        );
        // §10.4: the denial journal drains through the system ledger
        // key. Without one, enforce still refuses correctly but the
        // journal is VOID — denials are only counted, never durably
        // recorded. Loud at boot so a preview cell cannot mistake
        // itself for an audited one.
        if config.cli.usage_stream_key.is_none() {
            tracing::warn!(
                "STREAMS_AUTH_MODE={} without USAGE_STREAM_KEY: the _audit_events \
                 denial journal is DISABLED (denials appear only in \
                 audit_events_dropped_total)",
                config.cli.streams_auth_mode
            );
        }
        // The refresher cadence must clear the staleness window with
        // room for a failed fetch or two, or the cell oscillates into
        // fail-closed refusals on schedule.
        anyhow::ensure!(
            (config.cli.streams_auth_refresh_secs as i64)
                <= crate::auth::POLICY_STALENESS_MAX_SECS / 3,
            "STREAMS_AUTH_REFRESH_SECS={} must be <= {} (a third of the              {}s staleness window)",
            config.cli.streams_auth_refresh_secs,
            crate::auth::POLICY_STALENESS_MAX_SECS / 3,
            crate::auth::POLICY_STALENESS_MAX_SECS
        );
    }
    let catalog_cursor_key: Option<[u8; 32]> = match &config.cli.streams_cursor_key {
        None => None,
        Some(b64) => {
            use base64::Engine;
            let raw = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| anyhow::anyhow!("STREAMS_CURSOR_KEY is not base64: {e}"))?;
            Some(<[u8; 32]>::try_from(raw.as_slice()).map_err(|_| {
                anyhow::anyhow!("STREAMS_CURSOR_KEY must decode to exactly 32 bytes")
            })?)
        }
    };
    let auth_service = std::sync::Arc::new(crate::auth::AuthService::new(
        auth_mode,
        config.cli.streams_auth_issuer.clone(),
        &config.cli.cell_id,
    )?);
    // Only relevant when no topology exists yet; an existing topology wins.
    let fleet_mode = config.cli.fleet_prefix.is_some() && config.cli.fleet_max > 1;
    // FAIL CLOSED (round-19 security): the /v1/internal/* peer surface
    // can fence consumer generations and read segment state WITHOUT a
    // stream key. It therefore needs its own credential, distinct from
    // the customer account token, and fleet mode must not start without
    // one — a fleet that silently accepted the public bearer on those
    // routes would let any customer token corrupt any consumer.
    validate_fleet_auth(&config.cli, fleet_mode)?;
    validate_record_ceiling(
        &config.sse,
        config.cli.release_posture,
        config.cli.max_record_payload_bytes,
    )?;
    // Round-4 review: validate the capacity posture against the real
    // descriptor ceiling BEFORE any state is built — the SSE cap may
    // be clamped to what nofile_hard can actually carry.
    let (nofile_soft, nofile_hard) = crate::http::raise_nofile();
    tracing::info!(
        "nofile soft={nofile_soft} hard={nofile_hard} (raised to hard at boot); \
         feed retention budget={}B",
        crate::sse::budget::feed_total_cap(&config.sse)
    );
    validate_release_capacity(
        config.cli.release_posture,
        config.runtime.memprofile_cert.as_deref(),
        config.sse.feed_total_bytes_raw.as_deref(),
        &mut config.cli.sse_max_connections,
        nofile_hard,
    )?;
    let initial_shards = match config.cli.initial_shards {
        Some(n) => {
            if fleet_mode && n < 4 * config.cli.fleet_max as usize {
                tracing::warn!(
                    "INITIAL_SHARDS={n} < 4×FLEET_MAX={}: a fresh topology this coarse \
                     draws unevenly under rendezvous and the rebalancer flaps against \
                     return-home; use >= {}",
                    config.cli.fleet_max,
                    (4 * config.cli.fleet_max as usize).next_power_of_two()
                );
            }
            n
        }
        None if fleet_mode => (4 * config.cli.fleet_max as usize).next_power_of_two(),
        None => 1,
    };
    let topology = load_or_init_topology(
        &ops_store,
        initial_shards,
        config.cli.max_request_body_bytes,
    )
    .await
    .context("load topology")?;
    // R23-2: the body ceiling is a property of the NAMESPACE, not of the
    // process. The absorber sizes its worst-frame reservation from the
    // running setting, so starting against a namespace created with a
    // different ceiling would either under-reserve for records already
    // written — the exact under-reservation the process-wide budget
    // exists to prevent — or silently move the product limit customers
    // were told about. Refuse either way.
    //
    // A topology written before this field existed carries None; those
    // namespaces were created at the 32 MiB protocol pin and are held
    // to it.
    let stored_ceiling = topology
        .max_request_body_bytes
        .unwrap_or(crate::http::MAX_BODY_BYTES);
    if stored_ceiling != config.cli.max_request_body_bytes {
        anyhow::bail!(
            "MAX_REQUEST_BODY_BYTES is {} but this namespace was created with {} — \
             the ceiling sizes the absorber's worst-frame reservation, so changing it \
             on an existing namespace would under-reserve for records already written \
             (or silently move the published product limit). Set \
             MAX_REQUEST_BODY_BYTES={} to start against this namespace, or point \
             PATH_PREFIX at a fresh one.",
            config.cli.max_request_body_bytes,
            stored_ceiling,
            stored_ceiling,
        );
    }
    tracing::info!(
        "topology v{}: {} shard(s), body ceiling {} bytes (namespace-pinned)",
        topology.version,
        topology.shards.len(),
        stored_ceiling,
    );

    let keys = Arc::new(KeyCache::default());
    let touch = Arc::new(crate::touch::TouchRegistry::default());

    // Shards open lazily on first routed request (COMPUTE-SPEC §5.1):
    // opening fences the previous owner, so ownership follows routing.
    // A closed (fenced-away) shard is dropped from the serving map via
    // AppState::shard_closed through this weak back-reference.
    let state_slot: Arc<std::sync::OnceLock<std::sync::Weak<AppState>>> =
        Arc::new(std::sync::OnceLock::new());
    let opener = {
        let shard_store = shard_store.clone();
        let data_store = data_store.clone();
        let keys = keys.clone();
        let touch = touch.clone();
        let settings = shard_settings(&config.cli, &config.engine);
        // §1.1: one block cache for the whole process, not one per DB
        // (SlateDB default: 512 MB PER DB — a 16-shard 1 GB instance dies
        // by cache fill; the run 6/8 zombie generator).
        let shared_cache: Arc<slatedb::db_cache::foyer::FoyerCache> =
            Arc::new(slatedb::db_cache::foyer::FoyerCache::new_with_opts(
                slatedb::db_cache::foyer::FoyerCacheOptions {
                    max_capacity: config.cli.shared_cache_bytes,
                    ..Default::default()
                },
            ));
        let absorb_bytes = config.cli.absorb_bytes;
        let absorb_age = config.cli.absorb_age_secs;
        let absorb_pass_bytes = config.cli.absorb_pass_bytes;
        let absorb_concurrency = config.cli.absorb_concurrency;
        let absorb_pace_window_ms = config.cli.absorb_pace_window_ms;
        let absorb_pace_ms = config.cli.absorb_pace_ms;
        let absorb_read_par = config.cli.absorb_read_par;
        let absorb_small_bytes = config.cli.absorb_small_bytes;
        // Startup invariant (OOM disposition 2): the per-gather packing
        // cap must fit the process budget after the build multiplier,
        // or the envelope claim quietly breaks via reservation
        // clamping. Clamp the PACKING LIMIT (not the reservation) and
        // say so loudly.
        let absorb_gather_max_bytes = {
            let budget = crate::history::absorb_budget().capacity();
            let max_allowed = budget / crate::history::ABSORB_BUILD_MULTIPLIER;
            crate::history::RESOLVED_GATHER_PACKING_BYTES.store(
                crate::history::resolved_gather_packing_bytes(config.cli.absorb_gather_max_bytes),
                std::sync::atomic::Ordering::Relaxed,
            );
            if config.cli.absorb_gather_max_bytes > max_allowed {
                tracing::warn!(
                    "ABSORB_GATHER_MAX_BYTES {} x{} exceeds the process budget {} — \
                     clamping the gather packing limit to {}",
                    config.cli.absorb_gather_max_bytes,
                    crate::history::ABSORB_BUILD_MULTIPLIER,
                    budget,
                    max_allowed,
                );
                max_allowed
            } else {
                config.cli.absorb_gather_max_bytes
            }
        };
        let handle_idle_evict_secs = config.cli.handle_idle_evict_secs;
        let handle_max_resident = config.cli.handle_max_resident;
        let trim_per_op = config.cli.trim_per_op;
        let trim_global_budget = config.cli.trim_global_budget;
        let wal_group_commit = config.cli.wal_group_commit != 0;
        let wal_flush_gap = Duration::from_millis(if config.cli.wal_flush_gap_ms == 0 {
            config.cli.flush_interval_ms
        } else {
            config.cli.wal_flush_gap_ms
        });
        let wal_post_ack_gather = Duration::from_millis(config.cli.wal_post_ack_gather_ms);
        let wal_gather_skip_reqs = if config.cli.wal_gather_skip_reqs == 0 {
            u32::MAX
        } else {
            config.cli.wal_gather_skip_reqs
        };
        let wal_gather_skip_bytes = if config.cli.wal_gather_skip_bytes == 0 {
            u64::MAX
        } else {
            config.cli.wal_gather_skip_bytes
        };
        let tail_ring_bytes = config.cli.tail_ring_bytes;
        let state_slot = state_slot.clone();
        // Per-open inputs cloned out of the owned config: the Fn opener
        // runs once per shard open and cannot move fields out of its
        // captured variables, so it clones from these locals per call.
        let opener_history = config.history.clone();
        let opener_compactor = config.engine.compactor_options();
        let opener_frame_compress = config.crypto.frame_compress;
        crate::http::ShardOpener {
            open: Box::new(move |prefix: String| {
                let shard_store = shard_store.clone();
                let shared_cache = shared_cache.clone();
                let data_store = data_store.clone();
                let keys = keys.clone();
                let touch = touch.clone();
                let mut settings = settings.clone();
                // O14a: desynchronize WAL flush ticks across shards. 16
                // shards flushing on the same phase PUT in synchronized
                // bursts every interval; staggering by a per-shard offset
                // (base..1.5x base) spreads the PUTs across the window.
                if let Some(base) = settings.flush_interval {
                    let mut h: u32 = 2166136261;
                    for b in prefix.bytes() {
                        h ^= b as u32;
                        h = h.wrapping_mul(16777619);
                    }
                    let spread = (base.as_millis() as u64 / 2).max(1);
                    settings.flush_interval = Some(base + Duration::from_millis(h as u64 % spread));
                }
                let state_slot = state_slot.clone();
                let opener_history = opener_history.clone();
                let opener_compactor = opener_compactor.clone();
                Box::pin(async move {
                    let path = crate::sharddir::shard_db_path(&prefix);
                    tracing::info!("opening shard log {path} (lazy; fences prior owner)");
                    let db = {
                        let p2 = path.clone();
                        crate::bootstrap::on_slatedb_rt(async move {
                            Db::builder(p2.as_str(), shard_store)
                                .with_settings(settings)
                                .with_db_cache(shared_cache)
                                .build()
                                .await
                        })
                        .await
                        .with_context(|| format!("open shard log {path}"))?
                    };
                    let db = Arc::new(db);
                    // R25-A: load (or rebuild) the durable maintenance
                    // state SYNCHRONOUSLY, before the engine exists.
                    // Failure here is an engine-open failure — a shard
                    // whose backlog cannot be established must not
                    // serve, because "unknown" would be treated as
                    // "zero" by every admission decision after it.
                    let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
                        .await
                        .with_context(|| format!("load maintenance state for shard {prefix}"))?;
                    let (absorb_tx, absorb_rx) = absorber_channel();
                    let on_close = {
                        let touch = touch.clone();
                        let prefix = prefix.clone();
                        let state_slot = state_slot.clone();
                        Arc::new(move || {
                            touch.close_shard(&prefix);
                            if let Some(st) = state_slot.get().and_then(std::sync::Weak::upgrade) {
                                st.shard_closed(&prefix);
                            }
                        }) as Arc<dyn Fn() + Send + Sync>
                    };
                    let engine = ShardEngine::start(
                        prefix.clone(),
                        db,
                        data_store.clone(),
                        ShardConfig {
                            max_trim_per_op: trim_per_op,
                            trim_global_budget,
                            wal_group_commit,
                            wal_flush_gap,
                            wal_post_ack_gather,
                            wal_gather_skip_reqs,
                            wal_gather_skip_bytes,
                            tail_ring_bytes,
                            handle_idle_evict: Duration::from_secs(handle_idle_evict_secs),
                            handle_max_resident,
                            shared_postings_cache: Some(crate::postings_cache::process_cache()),
                            frame_compression: crate::crypto::FrameCompression::from_enabled(
                                opener_frame_compress,
                            ),
                            history: opener_history,
                            compactor_options: opener_compactor,
                            ..Default::default()
                        },
                        absorb_tx,
                        Some(on_close),
                        maintenance,
                    );
                    Absorber::start(
                        data_store,
                        engine.clone(),
                        keys,
                        AbsorberConfig {
                            threshold_bytes: absorb_bytes,
                            threshold_age: Duration::from_secs(absorb_age),
                            pass_bytes: absorb_pass_bytes,
                            concurrency: absorb_concurrency,
                            small_pass_bytes: absorb_small_bytes,
                            gather_max_bytes: absorb_gather_max_bytes,
                            gather_pace_window: Duration::from_millis(absorb_pace_window_ms),
                            gather_pace: Duration::from_millis(absorb_pace_ms),
                            gather_read_par: absorb_read_par,
                            ..Default::default()
                        },
                        absorb_rx,
                    );
                    Ok(engine)
                })
            }),
        }
    };

    let fleet_store_opt = config.fleet_store()?;
    let shards_map: std::sync::Arc<
        std::sync::RwLock<HashMap<String, Arc<crate::shard::ShardEngine>>>,
    > = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
    let gate =
        crate::sharddir::OpenGate::new(shards_map.clone(), opener.open, config.shard.open_deadline);
    let config = Arc::new(config);
    let state = Arc::new(AppState {
        config: config.clone(),
        registry,
        tenant,
        shard_prefixes: topology.shards.clone(),
        shards: shards_map,
        fleet_store: fleet_store_opt.clone(),
        gate,
        fleet_ops: std::sync::atomic::AtomicU64::new(0),
        inflight: std::sync::atomic::AtomicI64::new(0),
        inflight_peak: std::sync::atomic::AtomicI64::new(0),
        admit_max_inflight: std::sync::atomic::AtomicI64::new(config.cli.admit_max_inflight),
        admit_rss_shed_mb: config.cli.admit_rss_shed_mb,
        rss_mb_cached: std::sync::atomic::AtomicU64::new(0),
        admit_shed: std::sync::atomic::AtomicU64::new(0),
        admit_shed_inflight: std::sync::atomic::AtomicU64::new(0),
        admit_shed_survival: std::sync::atomic::AtomicU64::new(0),
        project_memory_pressure_bytes: std::sync::atomic::AtomicU64::new(
            config.cli.project_memory_pressure_bytes,
        ),
        project_memory_release_pct: config.cli.project_memory_release_pct.clamp(1, 100),
        admit_shed_rss: std::sync::atomic::AtomicU64::new(0),
        sse_max_connections: config.cli.sse_max_connections,
        sse_configured_max_connections: config.cli.sse_max_connections,
        live_feeds: Arc::new(crate::sse::registry::FeedRegistry::new()),
        feed_budget: Arc::new(crate::sse::feed::FeedMemoryBudget::from_config(&config.sse)),
        feed_ring_bytes: std::sync::atomic::AtomicUsize::new(crate::sse::budget::feed_ring_bytes(
            &config.sse,
        )),
        max_record_payload_bytes: std::sync::atomic::AtomicUsize::new(
            config.cli.max_record_payload_bytes.unwrap_or(0),
        ),
        cert_sealed_publish_delay_ms: std::sync::atomic::AtomicU64::new(
            cert_sealed_publish_delay(&config.runtime).unwrap_or_else(|e| panic!("{e}")),
        ),
        sse_heartbeat_ms: std::sync::atomic::AtomicU64::new(
            // Operational cadence knob (fleet certification runs it at
            // 500ms to observe keep-alives inside short stall windows);
            // GatedSseBody clamps to its 50ms floor.
            config.sse.heartbeat_ms,
        ),
        sse_connections: std::sync::atomic::AtomicU64::new(0),
        admit_max_inflight_per_stream: config.cli.admit_max_inflight_per_stream,
        stream_inflight: std::sync::Mutex::new(HashMap::new()),
        stream_shed: std::sync::atomic::AtomicU64::new(0),
        wedge_shed: std::sync::atomic::AtomicU64::new(0),
        maint_latch: crate::backpressure::GlobalLatch::new(),
        sweep_sched: crate::billing::SweepSched::default(),
        instance_name: config.cli.instance_name.clone(),
        ring_active: std::sync::RwLock::new(Vec::new()),
        ring_overrides: std::sync::RwLock::new(std::collections::HashMap::new()),
        peer_urls: std::sync::RwLock::new(std::collections::HashMap::new()),
        data_store,
        keys,
        touch,
        default_key: config.cli.conformance_default_key.clone(),
        auth_token: config.cli.auth_token.clone(),
        // Never empty: a standalone server still must be
        // distinguishable from the platform edge (round-19 MF4).
        origin_marker: if config.cli.instance_name.is_empty() {
            format!("streams/{}", env!("CARGO_PKG_VERSION"))
        } else {
            config.cli.instance_name.clone()
        },
        // SR3-1: the MODE determines the runtime credential state — in
        // workload mode the static token does not exist at runtime,
        // whatever the environment carried; in static mode no source
        // exists and relays use the bridge token.
        fleet_internal_token: if config.cli.fleet_auth_mode == "workload" {
            None
        } else {
            config.cli.fleet_internal_token.clone()
        },
        fleet_token_source: (config.cli.fleet_auth_mode == "workload")
            .then_some(config.cli.workload_token_file.as_ref())
            .flatten()
            .map(|path| {
                // Expiry-aware file cache: the platform rotates the file;
                // this re-reads when forced (peer 401) or within 30s of
                // the cached token's exp. The exp is read WITHOUT
                // verification — freshness scheduling only; peers verify.
                let path = path.clone();
                let cache: std::sync::Mutex<Option<(String, i64)>> = std::sync::Mutex::new(None);
                std::sync::Arc::new(move |force: bool| {
                    let now = chrono::Utc::now().timestamp();
                    let mut c = cache.lock().unwrap();
                    if !force
                        && let Some((tok, exp)) = c.as_ref()
                        && now < exp - 30
                    {
                        return Some(tok.clone());
                    }
                    let tok = std::fs::read_to_string(&path).ok()?.trim().to_string();
                    let exp = crate::auth::unverified_exp(&tok).unwrap_or(now);
                    *c = Some((tok.clone(), exp));
                    Some(tok)
                }) as crate::http::FleetTokenSource
            }),
        usage_key: config.cli.usage_stream_key.clone(),
        rollup: std::sync::OnceLock::new(),
        read_spool: std::sync::OnceLock::new(),
        billing_reads: Arc::new(crate::billing::ReadUsageAccumulator::new(
            crate::billing::MeterSource {
                cell: config.cli.cell_id.clone(),
                instance: config.cli.instance_name.clone(),
                boot: crate::billing::boot_id().to_string(),
            },
        )),
        account_id: config.cli.account_id.clone(),
        auth: auth_service.clone(),
        cell_id: config.cli.cell_id.clone(),
        region: config.cli.telemetry_region.clone(),
        quotas: crate::quota::QuotaRegistry::default(),
        catalog_cursor_key,
    });
    let _ = state_slot.set(Arc::downgrade(&state));
    // MULTITENANCY Stage 5: feed refresher — an immediate first fetch,
    // then a cadence well inside the staleness window (checked above).
    if auth_mode != crate::auth::AuthMode::Off {
        crate::auth_feed::spawn_refresher(
            auth_service.clone(),
            Box::new(crate::auth_feed::FileKeySource(
                config.cli.streams_auth_keys_file.clone().unwrap(),
            )),
            Box::new(crate::auth_feed::FilePolicySource(
                config.cli.streams_auth_policy_file.clone().unwrap(),
            )),
            Box::new(crate::auth_feed::FileGrantSource(
                config.cli.streams_auth_grants_file.clone().unwrap(),
            )),
            std::time::Duration::from_secs(config.cli.streams_auth_refresh_secs.max(1)),
        );
    }
    // Unified scaler (ROUTING-V3 §5): sketch-driven splits/merges.
    crate::scaler3::start(Arc::downgrade(&state));
    {
        // RSS sampler for the shed check (500 ms; /proc read per request
        // would be silly). Unconditional: this used to live inside the
        // fleet-mode block, which left ADMIT_RSS_SHED_MB comparing against
        // a frozen 0 in standalone mode — the shed was dead exactly where
        // the 2026-07-21 single-instance gate needed it (OOM at ~725 MB
        // with admit_shed=0).
        //
        // Purge-on-pressure: mimalloc only purges freed OS pages on
        // allocation-path ticks, so a process that goes IDLE after an
        // overload spike never purges — RSS stays frozen at the high
        // water and the shed 429s forever (the wedge liveness gate's
        // FAIL signature: byte-identical RSS for minutes, zero store
        // writes, zero backlog). When the sampler sees RSS above the
        // shed line it forces a collection (segments decommit;
        // purge_decommits defaults on) and re-measures, so retained-idle
        // memory can't masquerade as live pressure. Rate-limited; the
        // instance is already shedding writes when this runs.
        let st = state.clone();
        let shed_line_mb = config.cli.admit_rss_shed_mb;
        let bp_limits = crate::backpressure::Limits::from_config(&config.admission);
        tracing::info!(
            unabsorbed_instance = bp_limits.unabsorbed_bytes_instance,
            unabsorbed_shard = bp_limits.unabsorbed_bytes_shard,
            lag_secs = bp_limits.absorb_lag_secs,
            release_pct = bp_limits.release_pct,
            "maintenance backpressure bounds",
        );
        tokio::spawn(async move {
            let mut last_purge: Option<std::time::Instant> = None;
            let mut ticks: u64 = 0;
            loop {
                let mut mb = crate::fleet::rss_bytes() / 1048576;
                let purge_due = shed_line_mb > 0
                    && mb > shed_line_mb
                    && last_purge.is_none_or(|t| t.elapsed() >= Duration::from_secs(10));
                if purge_due {
                    let _ = tokio::task::spawn_blocking(|| unsafe {
                        libmimalloc_sys::mi_collect(true);
                    })
                    .await;
                    last_purge = Some(std::time::Instant::now());
                    mb = crate::fleet::rss_bytes() / 1048576;
                }
                st.rss_mb_cached
                    .store(mb, std::sync::atomic::Ordering::Relaxed);
                // Peak-since-scrape for the ops snapshot (OOM review I4):
                // 250 ms sampling, max-held until the scrape drains it.
                crate::ops::RSS_PEAK_MB.fetch_max(mb, std::sync::atomic::Ordering::Relaxed);
                // Maintenance backpressure re-evaluates on the same tick
                // (R23-1). Doing it here keeps the request path to a
                // single atomic read — walking the lag map per append
                // would put the overload on the hot path.
                if ticks.is_multiple_of(8) {
                    let snap = crate::backpressure::snapshot(&st);
                    st.maint_latch.apply(&snap, &bp_limits);
                }
                ticks = ticks.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }
    if let Some(fleet_store) = fleet_store_opt {
        crate::fleet::start(
            state.clone(),
            fleet_store,
            crate::fleet::FleetCfg {
                instance: config.cli.instance_name.clone(),
                capacity_rps: config.cli.scale_rps_capacity,
                edge_slots: config.cli.scale_edge_slots,
                target_util: (config.cli.scale_out_cpu_pct as f64 / 100.0).clamp(0.05, 0.95),
                scale_in_util: (config.cli.scale_in_cpu_pct as f64 / 100.0).clamp(0.05, 0.90),
                hot_cpu_pct: config.cli.scale_out_cpu_pct as f64,
                cpu_sustain: Duration::from_secs(config.cli.scale_cpu_sustain_secs),
                scale_in: Duration::from_secs(config.cli.scale_in_secs),
                latency_ms: config.cli.scale_latency_ms,
                edge_latency_ms: config.cli.scale_edge_latency_ms,
                latency_sustain: Duration::from_secs(config.cli.scale_lat_sustain_secs),
                max: config.cli.fleet_max,
            },
        );
        tracing::info!(
            "fleet coordination on (prefix={}, cap={} rps)",
            config.cli.fleet_prefix.as_deref().unwrap_or(""),
            config.cli.scale_rps_capacity
        );
    }
    // Telemetry pipeline (docs/OBSERVABILITY-BILLING.md): the drainer on
    // every instance; the rollup consumer where ROLLUP=1.
    if config.cli.billing_mode == "required" {
        if config.cli.usage_stream_key.is_none() {
            anyhow::bail!(
                "BILLING_MODE=required needs USAGE_STREAM_KEY — production \
                 billing refuses to run without the usage ledger (§14.1)"
            );
        }
        // Round-21: production billing must never silently attribute a
        // customer's traffic to the placeholder tenant.
        if config.cli.account_id == "acct_local"
            || config.cli.project_id == "proj_local"
            || config.cli.cell_id == "local"
        {
            anyhow::bail!(
                "BILLING_MODE=required needs explicit ACCOUNT_ID, PROJECT_ID \
                 and CELL_ID — refusing to bill production traffic to the \
                 local placeholders"
            );
        }
        // Round-22 items 2b/10: the read spool must be OPEN and
        // READABLE before this instance serves a single request —
        // required mode has no memory-only fallback window, so a spool
        // that cannot open (or whose rows cannot be scanned) is fatal.
        crate::billing::open_read_spool(&state).await.map_err(|e| {
            anyhow::anyhow!("BILLING_MODE=required: read spool must open before serving: {e}")
        })?;
        // ...and the rollup instance's database likewise: a rollup
        // owner that cannot open its DB must not serve (item 10).
        if config.cli.rollup == "1" {
            crate::billing::open_rollup(
                &state,
                &config.cli.path_prefix.clone().unwrap_or_default(),
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("BILLING_MODE=required: rollup DB must open before serving: {e}")
            })?;
        }
    }
    // ONE startup budget summary (OOM review): every fixed memory
    // bound in a single log line, plus a headroom warning when their
    // sum leaves less than 100 MiB below the shed line — posture
    // mistakes surface at boot, not at the kill line. WP-01: values come
    // from the installed AppConfig (identical parsing, once).
    {
        let cfg = &config;
        let shared = config.cli.shared_cache_bytes as usize;
        let history = cfg.history.cache_bytes;
        let postings = cfg.postings.cache_bytes;
        let telemetry = cfg.billing.telemetry_cache_bytes;
        let budget = crate::history::absorb_budget();
        let absorb_budget = budget.capacity();
        let gathers = budget.gather_slots();
        // Every gather reserves at least the worst-frame transient, so
        // the EFFECTIVE concurrency is the byte budget divided by that
        // floor — 1 under the 1-GiB profile regardless of configured
        // slots. Print both so nobody reads two slots as two-way.
        // R23-3: use the SHARED accounting so the log, the debug
        // surface, and the campaign verification cannot disagree. A
        // gather reserves max(packing x multiplier, worst_frame), not
        // the worst frame alone.
        crate::history::RESOLVED_GATHER_PACKING_BYTES.store(
            crate::history::resolved_gather_packing_bytes(config.cli.absorb_gather_max_bytes),
            std::sync::atomic::Ordering::Relaxed,
        );
        let per_gather = crate::history::per_gather_reservation_bytes();
        let effective_gathers = crate::history::effective_gather_concurrency();
        let rt_threads = cfg.engine.slatedb_rt_threads;
        let mib = |b: usize| b / (1024 * 1024);
        tracing::info!(
            "memory budget: caches shared={}MiB history={}MiB postings={}MiB telemetry={}MiB; unflushed/db={}MiB; absorb budget={}MiB (worst-frame build={}MiB, per-gather reservation={}MiB, configured gather slots={}, EFFECTIVE gather concurrency={}); slatedb rt threads={}; shed line={}MB (RSS + reserved absorber bytes)",
            mib(shared),
            mib(history),
            mib(postings),
            mib(telemetry),
            mib(config.cli.max_unflushed_bytes),
            mib(absorb_budget),
            mib(crate::history::absorb_worst_frame_transient()),
            mib(per_gather),
            gathers,
            effective_gathers,
            rt_threads,
            config.cli.admit_rss_shed_mb,
        );
        let _ = crate::history::RESOLVED_MEMORY_CONFIG.set(serde_json::json!({
            "gatherPackingLimitBytes": config.cli
                .absorb_gather_max_bytes
                .min(absorb_budget / crate::history::ABSORB_BUILD_MULTIPLIER),
            "absorbBudgetBytes": absorb_budget,
            "gatherSlots": gathers,
            "effectiveGatherConcurrency": effective_gathers,
            "slatedbRuntimeThreads": rt_threads,
            "sharedCacheBytes": shared,
            "historyCacheBytes": history,
            "postingsCacheBytes": postings,
            "telemetryCacheBytes": telemetry,
            "maxUnflushedBytes": config.cli.max_unflushed_bytes,
            "l0SstSizeBytes": config.cli.l0_sst_size_bytes,
            "l0MaxSsts": config.cli.l0_max_ssts,
            "shedLineMb": config.cli.admit_rss_shed_mb,
        }));
        let fixed_mb = mib(shared + history + postings + telemetry + absorb_budget) as u64;
        if config.cli.admit_rss_shed_mb > 0 && fixed_mb + 100 > config.cli.admit_rss_shed_mb {
            tracing::warn!(
                "fixed memory budgets ({fixed_mb} MiB) leave <100 MiB below the shed line                  ({} MB) — this posture does not fit the instance class",
                config.cli.admit_rss_shed_mb,
            );
        }
    }
    crate::billing::spawn_telemetry(state.clone());
    if config.cli.rollup == "1" {
        crate::billing::spawn_rollup(
            state.clone(),
            config.cli.path_prefix.clone().unwrap_or_default(),
        );
    }
    let app = crate::http::router(state);

    crate::store_timing::spawn_sentinels();

    let listener = tokio::net::TcpListener::bind(&config.cli.listen)
        .await
        .with_context(|| format!("bind {}", config.cli.listen))?;
    tracing::info!("streams-slate listening on {}", config.cli.listen);
    // #269: bounded h1 buffers — see http::serve_h1.
    let max_buf = config.http.h1_max_buf;
    crate::http::serve_h1(listener, app, max_buf).await?;
    Ok(())
}
