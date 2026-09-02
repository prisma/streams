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
use crate::config::notice::ConfigNotice;
use crate::config::profile::{certified_memprofile_errors, resolved_compactor_options};

#[cfg(test)]
#[path = "validation_tests.rs"]
mod validation_tests;

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

fn validate_fleet_auth(
    args: &CliArgs,
    fleet_mode: bool,
    notices: &mut Vec<ConfigNotice>,
) -> anyhow::Result<()> {
    match args.fleet_auth_mode.as_str() {
        "static" => {
            if args.release_posture {
                anyhow::bail!(
                    "FLEET_AUTH_MODE=static is the bridge posture and is refused under \
                     STREAMS_RELEASE_POSTURE=1 — configure workload identity (§14.1)"
                );
            }
            notices.push(ConfigNotice::FleetAuthStaticBridge);
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

/// The PROVEN configured subscription capacity (PR 4.1: the pure half
/// of the old `validate_release_capacity`, which mixed configured-limit
/// validation with OS-dependent resolution behind a `nofile_hard == 0`
/// sentinel and an in-place mutation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfiguredCapacity {
    sse_max_connections: u64,
}

/// The EFFECTIVE subscription capacity after the descriptor-budget
/// resolution — what the runtime installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveCapacity {
    pub sse_max_connections: u64,
}

/// Round-4 review: lock the safe 1-GiB defaults at boot — the PURE
/// checks over configured values (no OS probe): the feed retention
/// budget must stay inside the profile's release-safe maximum and must
/// parse exactly (a typo'd byte count must not masquerade as the
/// default), and the release posture must carry a BOUNDED subscription
/// cap (the runtime reads 0 as unlimited).
pub(crate) fn validate_configured_capacity(
    release_posture: bool,
    profile: Option<&str>,
    feed_total_env: Option<&str>,
    sse_max_connections: u64,
    notices: &mut Vec<ConfigNotice>,
) -> Result<ConfiguredCapacity, String> {
    if let Some(raw) = feed_total_env {
        let parsed: Option<u64> = raw.trim().parse().ok();
        match parsed {
            None => {
                return Err(format!(
                    "SSE_FEED_TOTAL_BYTES={raw:?} does not parse as a byte count \
                     (an unparseable value would silently fall back to the default)"
                ));
            }
            Some(v) if v > profile_feed_budget_max(profile) => {
                let max = profile_feed_budget_max(profile);
                if release_posture {
                    return Err(format!(
                        "SSE_FEED_TOTAL_BYTES={v} exceeds the {max}-byte release-safe \
                         maximum for memory profile {:?} (the 1-GiB class certifies at \
                         16 MiB; 64 MiB tripped RSS shed at ~505 feeds)",
                        profile.unwrap_or("default")
                    ));
                }
                notices.push(ConfigNotice::FeedBudgetAboveReleaseMax {
                    configured: v,
                    max,
                    profile: profile.unwrap_or("default").to_string(),
                });
            }
            Some(_) => {}
        }
    }
    // Round-4 follow-up review, finding 1: an explicit 0 must never
    // pass release validation (0 = unlimited on the request path).
    if release_posture && sse_max_connections == 0 {
        return Err(
            "SSE_MAX_CONNECTIONS=0 means unlimited; the release posture \
             requires a bounded subscription cap"
                .to_string(),
        );
    }
    Ok(ConfiguredCapacity {
        sse_max_connections,
    })
}

/// The OS-dependent half (bootstrap PREFLIGHT, after the descriptor
/// limit has been raised and probed): the configured cap must fit
/// under `nofile_hard` with headroom for everything else the process
/// holds. Under the release posture: clamp and notice (the review's
/// acceptable arm) rather than refusing — a platform that lowers the
/// ceiling mid-fleet must not take the whole deployment down at
/// restart. Outside the release posture: notice only. A degraded
/// ceiling (`nofile_hard <= reserve`) must never clamp DOWN to 0
/// (= unlimited): the release posture fails CLOSED. `nofile_hard == 0`
/// means the platform reported no ceiling (non-unix): nothing to
/// resolve against, the configured value stands.
pub(crate) fn resolve_effective_capacity(
    configured: ConfiguredCapacity,
    release_posture: bool,
    nofile_hard: u64,
    notices: &mut Vec<ConfigNotice>,
) -> Result<EffectiveCapacity, String> {
    let mut cap = configured.sse_max_connections;
    if nofile_hard > 0 {
        if nofile_hard <= FD_RESERVE {
            if release_posture {
                return Err(format!(
                    "nofile_hard={nofile_hard} leaves no safe SSE connection capacity \
                     (a {FD_RESERVE}-descriptor reserve is required before any \
                     subscription budget)"
                ));
            }
            notices.push(ConfigNotice::DescriptorReserveTight {
                nofile_hard,
                reserve: FD_RESERVE,
            });
        } else {
            let ceiling = nofile_hard - FD_RESERVE;
            if cap > ceiling {
                if release_posture {
                    notices.push(ConfigNotice::SseCapClamped {
                        configured: cap,
                        nofile_hard,
                        reserve: FD_RESERVE,
                        effective: ceiling,
                    });
                    cap = ceiling;
                } else {
                    notices.push(ConfigNotice::SseCapExceedsDescriptors {
                        configured: cap,
                        nofile_hard,
                        reserve: FD_RESERVE,
                    });
                }
            }
        }
    }
    Ok(EffectiveCapacity {
        sse_max_connections: cap,
    })
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
/// [`crate::config::ServerConfig::validate`], and [`crate::bootstrap::run`]
/// accepts only this type — so validation is complete before any
/// process-global initialization, store opening, canary write, or task
/// spawn, by construction rather than by call-site discipline. Every
/// field is PRIVATE (PR 4.1): no other crate module can forge one;
/// bootstrap takes the proven parts through
/// [`ValidatedServerConfig::into_bootstrap_parts`].
pub struct ValidatedServerConfig {
    config: crate::config::ServerConfig,
    tenant: crate::tenant::ProjectId,
    cell_id: crate::tenant::CellId,
    auth_mode: crate::auth::AuthMode,
    catalog_cursor_key: Option<[u8; 32]>,
    cert_sealed_publish_delay_ms: u64,
    initial_shards: InitialShards,
    configured_capacity: ConfiguredCapacity,
    notices: Vec<ConfigNotice>,
}

/// The proven values bootstrap consumes — obtainable ONLY from a
/// [`ValidatedServerConfig`].
pub(crate) struct BootstrapParts {
    pub(crate) config: crate::config::ServerConfig,
    pub(crate) tenant: crate::tenant::ProjectId,
    pub(crate) cell_id: crate::tenant::CellId,
    pub(crate) auth_mode: crate::auth::AuthMode,
    pub(crate) catalog_cursor_key: Option<[u8; 32]>,
    pub(crate) cert_sealed_publish_delay_ms: u64,
    pub(crate) initial_shards: InitialShards,
    pub(crate) configured_capacity: ConfiguredCapacity,
    pub(crate) notices: Vec<ConfigNotice>,
}

impl ValidatedServerConfig {
    /// The proven configuration graph (read-only).
    pub fn config(&self) -> &crate::config::ServerConfig {
        &self.config
    }

    pub(crate) fn into_bootstrap_parts(self) -> BootstrapParts {
        BootstrapParts {
            config: self.config,
            // mt-lint: allow(state-tenant-read): THE one consuming accessor of the validated boundary — it hands the PROVEN deployment tenant to bootstrap, which is the sanctioned adopter (PR 4.1)
            tenant: self.tenant,
            cell_id: self.cell_id,
            auth_mode: self.auth_mode,
            catalog_cursor_key: self.catalog_cursor_key,
            cert_sealed_publish_delay_ms: self.cert_sealed_publish_delay_ms,
            initial_shards: self.initial_shards,
            configured_capacity: self.configured_capacity,
            notices: self.notices,
        }
    }
}

/// Collector for one validation pass: every problem and every advisory.
#[derive(Default)]
struct Findings {
    errors: Vec<String>,
    notices: Vec<ConfigNotice>,
}

impl Findings {
    fn err(&mut self, e: impl Into<String>) {
        self.errors.push(e.into());
    }
}

impl crate::config::ServerConfig {
    /// Prove the parsed configuration internally consistent (PR 3.2).
    /// Pure over the configuration value: no environment reads, no
    /// stores, no spawns, no process termination, and (PR 4.1) NO
    /// LOGS — advisories are returned as typed notices. Every problem
    /// is collected and returned. OS-resource checks that need a live
    /// probe (the descriptor clamp) run in [`crate::bootstrap::run`]'s
    /// preflight through [`resolve_effective_capacity`].
    pub fn validate(self) -> Result<ValidatedServerConfig, ConfigError> {
        let mut f = Findings::default();
        self.validate_engine_and_profile(&mut f);
        let (tenant, cell_id) = self.validate_identity(&mut f);
        let initial_shards = self.validate_topology_and_ceilings(&mut f);
        self.validate_billing_prerequisites(&mut f);
        let (auth_mode, catalog_cursor_key) = self.validate_auth_and_keys(&mut f);
        let configured_capacity = self.validate_posture(&mut f);
        let cert_sealed_publish_delay_ms = self.validate_instruments(&mut f);

        if !f.errors.is_empty() {
            return Err(ConfigError { errors: f.errors });
        }
        Ok(ValidatedServerConfig {
            tenant: tenant.expect("no errors implies tenant parsed"),
            cell_id: cell_id.expect("no errors implies cell id parsed"),
            auth_mode: auth_mode.expect("no errors implies auth mode parsed"),
            catalog_cursor_key,
            cert_sealed_publish_delay_ms: cert_sealed_publish_delay_ms
                .expect("no errors implies delay parsed"),
            initial_shards: initial_shards.expect("no errors implies shards proven"),
            configured_capacity: configured_capacity.expect("no errors implies capacity proven"),
            notices: f.notices,
            config: self,
        })
    }

    /// R28 sweep residency + memory-profile certification + the
    /// engine settings SlateDB would reject at open time (CHAOS-2).
    fn validate_engine_and_profile(&self, f: &mut Findings) {
        // SWEEP_MAINT_RESIDENT=0 would silently starve every cold debt
        // class (the rotation would open and immediately close each
        // indebted engine, so no absorber lives long enough to drain).
        // The config stores the raw value; the billing adapter floors at
        // use.
        if self.billing.sweep_maint_resident == 0 {
            f.err(
                "SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; \
                 set >= 1 or unset (default 2)",
            );
        }
        // A certified survival deploy must fail at boot, not OOM at
        // +28 min, if any memory knob was dropped or overridden.
        let profile_errors = certified_memprofile_errors(self, &mut f.notices);
        f.errors.extend(profile_errors);
        // Both engine tiers go through the same check so a future edit
        // to either cannot reintroduce the permanently-500 hole.
        for (what, settings) in [
            ("shard", shard_settings(&self.cli, &self.engine)),
            (
                "history",
                crate::history::history_settings(&self.history, &self.engine.compactor_options()),
            ),
        ] {
            if let Err(e) = validate_engine_settings(what, &settings) {
                f.err(format!("{e}"));
            }
        }
    }

    /// The deployment tenant (layout-4 paths and hashes derive from
    /// it) and the telemetry cell identity (§2), both as typed values.
    fn validate_identity(
        &self,
        f: &mut Findings,
    ) -> (
        Option<crate::tenant::ProjectId>,
        Option<crate::tenant::CellId>,
    ) {
        let tenant = match crate::tenant::ProjectId::new(&self.cli.project_id) {
            Ok(t) if t.is_system() => {
                f.err("PROJECT_ID may not be the reserved system project");
                None
            }
            Ok(t) => Some(t),
            Err(e) => {
                f.err(format!(
                    "PROJECT_ID {:?} is invalid: {e}",
                    self.cli.project_id
                ));
                None
            }
        };
        let cell_id = match crate::tenant::CellId::new(&self.cli.cell_id) {
            Ok(c) => Some(c),
            Err(e) => {
                f.err(format!("CELL_ID {:?} is invalid: {e}", self.cli.cell_id));
                None
            }
        };
        (tenant, cell_id)
    }

    /// The effective body ceiling (CHAOS-3: it sizes the absorber's
    /// worst-frame reservation, so it must be right BEFORE any
    /// process-global budget reads it) and the effective initial shard
    /// count, resolved against the fleet-mode default and proven.
    fn validate_topology_and_ceilings(&self, f: &mut Findings) -> Option<InitialShards> {
        if let Err(e) = validate_body_ceiling(self.cli.max_request_body_bytes) {
            f.err(e);
        }
        let fleet_mode = self.fleet_mode();
        let effective_shards = match self.cli.initial_shards {
            Some(n) => {
                if fleet_mode && n < 4 * self.cli.fleet_max as usize {
                    f.notices.push(ConfigNotice::CoarseInitialShards {
                        configured: n,
                        fleet_max: self.cli.fleet_max,
                        suggested: (4 * self.cli.fleet_max as usize).next_power_of_two(),
                    });
                }
                n
            }
            None if fleet_mode => (4 * self.cli.fleet_max as usize).next_power_of_two(),
            None => 1,
        };
        match InitialShards::new(effective_shards) {
            Ok(s) => Some(s),
            Err(e) => {
                f.err(e);
                None
            }
        }
    }

    /// Round-21: production billing must never silently attribute a
    /// customer's traffic to the placeholder tenant — the PURE
    /// billing-required prerequisites; the spool and rollup OPENS
    /// (store I/O) stay in bootstrap.
    fn validate_billing_prerequisites(&self, f: &mut Findings) {
        if self.cli.billing_mode != "required" {
            return;
        }
        if self.cli.usage_stream_key.is_none() {
            f.err(
                "BILLING_MODE=required needs USAGE_STREAM_KEY — production \
                 billing refuses to run without the usage ledger (§14.1)",
            );
        }
        if self.cli.account_id == "acct_local"
            || self.cli.project_id == "proj_local"
            || self.cli.cell_id == "local"
        {
            f.err(
                "BILLING_MODE=required needs explicit ACCOUNT_ID, PROJECT_ID \
                 and CELL_ID — refusing to bill production traffic to the \
                 local placeholders",
            );
        }
    }

    /// MULTITENANCY Stage 5: the auth mode, its required files and
    /// refresh cadence, and the catalog cursor key.
    fn validate_auth_and_keys(
        &self,
        f: &mut Findings,
    ) -> (Option<crate::auth::AuthMode>, Option<[u8; 32]>) {
        let auth_mode =
            match crate::auth::AuthMode::from_env(Some(self.cli.streams_auth_mode.as_str())) {
                Ok(m) => Some(m),
                Err(e) => {
                    f.err(format!("{e}"));
                    None
                }
            };
        if auth_mode.is_some_and(|m| m != crate::auth::AuthMode::Off) {
            // The local placeholder tenant must never reach a
            // shadow/enforce deployment — proj_local silently naming a
            // real project's data is exactly the accident this refuses.
            if self.cli.project_id == "proj_local" {
                f.err(format!(
                    "STREAMS_AUTH_MODE={} requires an explicit non-default PROJECT_ID",
                    self.cli.streams_auth_mode
                ));
            }
            if !(self.cli.streams_auth_keys_file.is_some()
                && self.cli.streams_auth_policy_file.is_some()
                && self.cli.streams_auth_grants_file.is_some())
            {
                f.err(format!(
                    "STREAMS_AUTH_MODE={} requires STREAMS_AUTH_KEYS_FILE, \
                     STREAMS_AUTH_POLICY_FILE and STREAMS_AUTH_GRANTS_FILE",
                    self.cli.streams_auth_mode
                ));
            }
            // The refresher cadence must clear the staleness window with
            // room for a failed fetch or two, or the cell oscillates into
            // fail-closed refusals on schedule.
            if (self.cli.streams_auth_refresh_secs as i64)
                > crate::auth::POLICY_STALENESS_MAX_SECS / 3
            {
                f.err(format!(
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
                        f.err(format!("STREAMS_CURSOR_KEY is not base64: {e}"));
                        None
                    }
                    Ok(raw) => match <[u8; 32]>::try_from(raw.as_slice()) {
                        Ok(k) => Some(k),
                        Err(_) => {
                            f.err("STREAMS_CURSOR_KEY must decode to exactly 32 bytes");
                            None
                        }
                    },
                }
            }
        };
        (auth_mode, catalog_cursor_key)
    }

    /// The release posture: fleet-auth credentials (FAIL CLOSED,
    /// round-19), the per-record ceiling, and the configured
    /// subscription capacity.
    fn validate_posture(&self, f: &mut Findings) -> Option<ConfiguredCapacity> {
        if let Err(e) = validate_fleet_auth(&self.cli, self.fleet_mode(), &mut f.notices) {
            f.err(format!("{e}"));
        }
        if let Err(e) = validate_record_ceiling(
            &self.sse,
            self.cli.release_posture,
            self.cli.max_record_payload_bytes,
        ) {
            f.err(format!("{e}"));
        }
        match validate_configured_capacity(
            self.cli.release_posture,
            self.runtime.memprofile_cert.as_deref(),
            self.sse.feed_total_bytes_raw.as_deref(),
            self.cli.sse_max_connections,
            &mut f.notices,
        ) {
            Ok(c) => Some(c),
            Err(e) => {
                f.err(e);
                None
            }
        }
    }

    /// Round-11.6: the seal-publication delay is a CERTIFICATION
    /// instrument, never a production knob.
    fn validate_instruments(&self, f: &mut Findings) -> Option<u64> {
        match cert_sealed_publish_delay_from(
            self.runtime.cert_sealed_publish_delay_ms_raw.as_deref(),
            self.runtime.certification_mode.as_deref(),
        ) {
            Ok(ms) => Some(ms),
            Err(e) => {
                f.err(format!("{e}"));
                None
            }
        }
    }

    fn fleet_mode(&self) -> bool {
        self.cli.fleet_prefix.is_some() && self.cli.fleet_max > 1
    }
}
