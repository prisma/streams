//! The configuration model: `ServerConfig` and its sub-configs
//! (WP-01 PR 3.1 — the parsed, owned configuration graph).
//!
//! Rules of the model:
//!
//! - **Fidelity over elegance.** Each field preserves the exact parse
//!   expression, default and divergence of the site it replaced —
//!   including known quirks (see `BillingConfig::path_prefix_env` and the
//!   two readers of `COMPACT_MAX_SST_SIZE_BYTES` with different defaults).
//!   Semantic cleanup is separate work (WP-13/WP-14), not the refactor.
//! - **No secrets in the knob graph.** Key material, tokens and
//!   credentials live only in `cli` (the parsed command line); the
//!   environment-knob sub-configs carry tunables only.
//! - **Immutable after construction.** Built once at the composition
//!   root and handed to owners by reference/clone; there is no
//!   process-global slot. Runtime-mutable controls (e.g. the absorber
//!   pause flag) keep their own atomics; config only carries their
//!   initial value.

use crate::config::cli::CliArgs;
use std::time::Duration;

/// The complete server configuration: the parsed command line (`cli`)
/// plus every environment knob, parsed once. Owners receive this value
/// (or their narrow sub-config) at construction.
#[derive(Clone, Debug, PartialEq)]
pub struct ServerConfig {
    /// The parsed CLI surface (84 flags). Contains secret material
    /// (access keys, tokens) — never log it; `redacted_summary` excludes
    /// it entirely.
    pub cli: CliArgs,
    pub storage: StorageConfig,
    pub engine: EngineConfig,
    pub shard: ShardRuntimeConfig,
    pub history: HistoryConfig,
    pub postings: PostingsConfig,
    pub sse: SseConfig,
    pub http: HttpConfig,
    pub billing: BillingConfig,
    pub fleet: FleetConfig,
    pub scaler: ScaleConfig,
    pub admission: AdmissionConfig,
    pub crypto: CryptoConfig,
    pub runtime: RuntimeConfig,
}

/// Object-store construction + store_timing gates.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageConfig {
    /// POOL_IDLE_SECS, default 4. Idle pooled connections die silently
    /// across scale-to-zero snapshot/restore; keep under the platform's
    /// 5 s idle threshold (EXPERIMENT-PILOT.md).
    pub pool_idle_secs: u64,
    /// STORE_MAX_CONCURRENT, default 0 = off. Instance-wide cap on
    /// concurrent object-store ops (keeps a warm connection set).
    pub store_max_concurrent: usize,
    /// STORE_BULK_INFLIGHT_MAX_BYTES, default 0 = off. Readers: the
    /// bulk gate in store_timing (clamped to u32 at use) and the
    /// compactor profile JSON (raw u64).
    pub bulk_inflight_max_bytes: u64,
    /// store_timing's nominal weight for unknown-length GETs. Reads
    /// COMPACT_MAX_SST_SIZE_BYTES with default **8 MiB** — deliberately
    /// NOT unified with `EngineConfig::compact_max_sst_size` (default
    /// 256 MiB): same env name, two different knobs, preserved as-is.
    pub bulk_nominal_get_bytes: u64,
}

/// SlateDB engine knobs (one resolved compactor profile for EVERY
/// SlateDB this process opens — R27-4/R28).
#[derive(Clone, Debug, PartialEq)]
pub struct EngineConfig {
    /// COMPACTOR_POLL_MS, default `crate::DEFAULT_COMPACTOR_POLL_MS`.
    pub compactor_poll_ms: u64,
    /// COMPACTOR_MAX_CONCURRENT, default 4.
    pub compactor_max_concurrent: usize,
    /// COMPACT_MAX_SUBCOMPACTIONS, default 4.
    pub compact_max_subcompactions: usize,
    /// COMPACT_MAX_FETCH_TASKS, default 4.
    pub compact_max_fetch_tasks: usize,
    /// COMPACT_BYTES_TO_FETCH, default 2 MiB.
    pub compact_bytes_to_fetch: usize,
    /// COMPACT_MAX_SST_SIZE_BYTES, default 256 MiB (the compactor's
    /// reader — see `StorageConfig::bulk_nominal_get_bytes` for the
    /// other reader of the same env name).
    pub compact_max_sst_size: usize,
    /// SLATEDB_RT_THREADS, default 2. Worker threads of the dedicated
    /// SlateDB runtime.
    pub slatedb_rt_threads: usize,
}

impl EngineConfig {
    /// Build the resolved compactor options (previously the
    /// `resolved_compactor_options()` OnceLock in bootstrap.rs).
    pub fn compactor_options(&self) -> slatedb::config::CompactorOptions {
        let base = slatedb::config::CompactorOptions::default();
        let w0 = base.worker.clone().unwrap_or_default();
        let w = slatedb::config::CompactionWorkerOptions {
            max_concurrent_compactions: self.compactor_max_concurrent,
            max_subcompactions: self.compact_max_subcompactions,
            max_fetch_tasks: self.compact_max_fetch_tasks,
            bytes_to_fetch: self.compact_bytes_to_fetch,
            max_sst_size: self.compact_max_sst_size,
            ..w0
        };
        slatedb::config::CompactorOptions {
            poll_interval: Duration::from_millis(self.compactor_poll_ms),
            max_concurrent_compactions: self.compactor_max_concurrent,
            worker: Some(w),
            ..base
        }
    }
}

/// Shard-directory runtime knobs.
#[derive(Clone, Debug, PartialEq)]
pub struct ShardRuntimeConfig {
    /// SHARD_OPEN_DEADLINE_MS, default 180 s (`OpenGate` construction).
    pub open_deadline: Duration,
    /// SHARD_OPEN_WAIT_MS, default 10_000 — per-request open-gate wait.
    pub open_wait_ms: u64,
    /// UNREADY_EXIT_AFTER_SECS, default 300 (0 disables the watchdog).
    pub unready_exit_after_secs: u64,
}

/// History/absorber knobs (src/history.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct HistoryConfig {
    /// ABSORB_PAUSE == "1", default false. Only the INITIAL value of the
    /// runtime-mutable pause flag (the debug endpoint toggles the
    /// atomic at runtime).
    pub absorb_pause_initial: bool,
    /// ABSORB_GLOBAL_BUDGET_BYTES. Defaults: 4 GiB under cfg(test),
    /// 64 MiB otherwise (preserved exactly, including the test split);
    /// `floored_budget_capacity()` still raises it to the worst-frame
    /// floor at the use site.
    pub absorb_global_budget_bytes: usize,
    /// ABSORB_GLOBAL_GATHERS, max(1). Defaults: 64 under cfg(test),
    /// 2 otherwise.
    pub absorb_global_gathers: usize,
    /// HISTORY_CACHE_BYTES, default 32 MiB.
    pub cache_bytes: usize,
    /// HISTORY_COMPACTOR == "off", default false.
    pub compactor_off: bool,
    /// HISTORY_GC_INTERVAL_SECS (legacy alias
    /// HISTORY_GC_MAX_INTERVAL_SECS), default 600 s; 0 = None.
    pub gc_interval: Option<Duration>,
}

/// Postings cache (src/postings_cache.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct PostingsConfig {
    /// POSTINGS_CACHE_BYTES, default 64 MiB.
    pub cache_bytes: usize,
}

/// LiveFeed budgets and heartbeat (src/sse/).
#[derive(Clone, Debug, PartialEq)]
pub struct SseConfig {
    /// SSE_FEED_RING_BYTES, default 1 MiB; unparseable warns + default
    /// (same behavior, now at load time).
    pub feed_ring_bytes: usize,
    /// SSE_FEED_TOTAL_BYTES, default 16 MiB; unparseable warns + default.
    pub feed_total_bytes: u64,
    /// RAW SSE_FEED_TOTAL_BYTES string, for release-posture validation
    /// (`bootstrap::validate_release_capacity` refuses garbage outright;
    /// the warn-and-default above is only the lazy-reader contract).
    pub feed_total_bytes_raw: Option<String>,
    /// SSE_FEED_PROJECT_BYTES, RAW string. The strict parse stays at the
    /// use site (`sse::feed::configured_project_cap`) because release
    /// posture turns it into a hard boot error; default there = global/4.
    pub feed_project_bytes_raw: Option<String>,
    /// SSE_HEARTBEAT_MS, default 15_000; 0/unparseable = default.
    pub heartbeat_ms: u64,
}

/// HTTP-surface runtime knobs (src/http.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct HttpConfig {
    /// TAIL_MAX_BYTES, default 1 MiB; 0/unparseable = default.
    pub tail_max_bytes: usize,
    /// STREAMS_DEBUG_TIMING == "1", default false.
    pub debug_timing: bool,
    /// STREAMS_DEBUG_EXIT == "1", default false (forbidden).
    pub debug_exit: bool,
    /// APP_BINARY_SHA256, default "unknown" (debug endpoint payload).
    pub binary_sha256: String,
    /// SSE_H1_MAX_BUF, default 64 KiB — h1 connection buffer ceiling.
    pub h1_max_buf: usize,
}

/// Billing/telemetry/rollup knobs (src/billing.rs, src/ops.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct BillingConfig {
    /// RAW BILLING_MODE env value. `billing_required()` and the debug
    /// endpoint read the ENVIRONMENT today, NOT the clap field
    /// (`--billing-mode`, env BILLING_MODE) — a dual-channel quirk
    /// preserved exactly here. Consumers of the CLI value keep reading
    /// `args.billing_mode`. Scheduled for unification in WP-13.
    pub mode_env: Option<String>,
    /// BILLING_METER: metering on unless == "off" (per-append read).
    pub meter_enabled: bool,
    /// RAW ROLLUP env value. /health and /v1/debug/billing read the env
    /// directly today; `spawn_rollup` uses the clap field. Same
    /// dual-channel quirk as `mode_env`.
    pub rollup_env: Option<String>,
    /// RAW PATH_PREFIX env value. `open_read_spool` reads the env
    /// directly (NOT `--path-prefix`), while `spawn_rollup` uses the clap
    /// field — preserved as-is; WP-13 owns the unification.
    pub path_prefix_env: Option<String>,
    /// OUTBOX_SWEEP_SECS, default 300.
    pub outbox_sweep_secs: u64,
    /// TELEMETRY_DRAIN_SECS, default 2.
    pub telemetry_drain_secs: u64,
    /// METRICS_INTERVAL_SECS, default 15.
    pub metrics_interval_secs: u64,
    /// MONTH_CLOSE_GRACE_MS, default 86_400_000 (24 h).
    pub month_close_grace_ms: i64,
    /// TELEMETRY_CACHE_BYTES, default 16 MiB.
    pub telemetry_cache_bytes: usize,
    /// SWEEP_DISCOVERY_MAX, default 8 — per maintenance-sweep tick.
    pub sweep_discovery_max: usize,
    /// SWEEP_MAINT_RESIDENT, default 2, floored at 1. (The binary
    /// composition root refuses 0 outright at boot.)
    pub sweep_maint_resident: usize,
    /// SWEEP_RESIDENT_QUANTUM, default 4, floored at 1.
    pub sweep_resident_quantum: usize,
    /// ALERT_USAGE_OUTBOX_DIRTY, default 1000 (ops alert threshold).
    pub alert_usage_outbox_dirty: u64,
}

/// Fleet coordination knobs (src/fleet.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct FleetConfig {
    /// FLEET_ALLOW_HTTP_PEERS == "1", default false. Peer URL validation
    /// input (plaintext http only for local rigs/DST).
    pub allow_http_peers: bool,
    /// FLEET_PEER_DOMAINS, RAW string; split/trim/subdomain match stays
    /// at the use site (`fleet::valid_peer_url_with`).
    pub peer_domains_raw: Option<String>,
    /// REBALANCE_LAG_SECS, default 60.
    pub rebalance_lag_secs: u64,
    /// REBALANCE_MOVE_COOLDOWN_SECS, default 60.
    pub rebalance_move_cooldown_secs: u64,
    /// SELF_URL, default "" — heartbeat url field.
    pub self_url: String,
    /// FLEET_MIN, default 1, floored at 1.
    pub fleet_min: u64,
    /// REBALANCE_RETURN_SECS, default 300.
    pub rebalance_return_secs: u64,
}

/// Scaler policy knobs (src/scaler3.rs) — parsed as f64 by `envf`,
/// cast at use; the casts are preserved exactly.
#[derive(Clone, Debug, PartialEq)]
pub struct ScaleConfig {
    /// SCALE_EVAL_SECS, default 10.
    pub eval_secs: u64,
    /// SCALE_RATE_WINDOW_SECS, default 120.
    pub rate_window_secs: f64,
    /// SCALE_HOT_PCT, default 75 → stored /100 as 0.75.
    pub hot_pct: f64,
    /// SCALE_COLD_PCT, default 15 → /100.
    pub cold_pct: f64,
    /// SCALE_HOT_EVALS, default 2.
    pub hot_evals: u32,
    /// SCALE_COLD_EVALS, default 180.
    pub cold_evals: u32,
    /// SCALE_COOLDOWN_SECS, default 600.
    pub cooldown_secs: i64,
    /// MAX_SEGMENTS_PER_STREAM, default 64.
    pub max_segments: usize,
}

/// Admission/backpressure and per-shard usage token-bucket knobs
/// (src/backpressure.rs, src/usage.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct AdmissionConfig {
    /// MAX_UNABSORBED_BYTES_PER_INSTANCE, default 512 MiB.
    pub unabsorbed_bytes_instance: u64,
    /// MAX_UNABSORBED_BYTES_PER_SHARD, default 256 MiB.
    pub unabsorbed_bytes_shard: u64,
    /// MAX_ABSORB_LAG_SECS, default 900.
    pub absorb_lag_secs: u64,
    /// MAINT_BACKPRESSURE_RELEASE_PCT, default 75, capped at 100.
    pub maint_release_pct: u64,
    /// LIMIT_BYTES_PER_SEC, default 5_000_000.
    pub limit_bytes_per_sec: f64,
    /// LIMIT_REQS_PER_SEC, default 1_000.
    pub limit_reqs_per_sec: f64,
    /// LIMIT_RECS_PER_SEC, default 5_000.
    pub limit_recs_per_sec: f64,
    /// LIMIT_BURST_SECS, default 2.
    pub limit_burst_secs: f64,
}

/// Crypto framing knobs (src/crypto.rs).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CryptoConfig {
    /// FRAME_COMPRESS: "1" or case-insensitive "true", default false.
    pub frame_compress: bool,
}

/// Process/runtime identity and certification controls.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeConfig {
    /// MEMPROFILE_CERT, raw. Read by the binary's pre-runtime certified
    /// memprofile assertion and by release-capacity validation.
    pub memprofile_cert: Option<String>,
    /// STREAMS_CERT_SEALED_PUBLISH_DELAY_MS, raw. Parse + the "delay
    /// requires certification mode" bail stay in
    /// `bootstrap::cert_sealed_publish_delay_from`.
    pub cert_sealed_publish_delay_ms_raw: Option<String>,
    /// STREAMS_CERTIFICATION_MODE, raw (Some("1") enables cert knobs).
    pub certification_mode: Option<String>,
}

impl ServerConfig {
    /// The no-environment knob posture. `load()` overlays the
    /// environment on top of this, so `load(cli, empty_env)` is provably
    /// this value.
    pub(crate) fn with_knob_defaults(cli: CliArgs) -> Self {
        Self {
            cli,
            storage: Default::default(),
            engine: Default::default(),
            shard: Default::default(),
            history: Default::default(),
            postings: Default::default(),
            sse: Default::default(),
            http: Default::default(),
            billing: Default::default(),
            fleet: Default::default(),
            scaler: Default::default(),
            admission: Default::default(),
            crypto: Default::default(),
            runtime: Default::default(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            pool_idle_secs: 4,
            store_max_concurrent: 0,
            bulk_inflight_max_bytes: 0,
            bulk_nominal_get_bytes: 8 * 1024 * 1024,
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            compactor_poll_ms: crate::DEFAULT_COMPACTOR_POLL_MS,
            compactor_max_concurrent: 4,
            compact_max_subcompactions: 4,
            compact_max_fetch_tasks: 4,
            compact_bytes_to_fetch: 2 * 1024 * 1024,
            compact_max_sst_size: 256 * 1024 * 1024,
            slatedb_rt_threads: 2,
        }
    }
}

impl Default for ShardRuntimeConfig {
    fn default() -> Self {
        Self {
            open_deadline: Duration::from_secs(180),
            open_wait_ms: 10_000,
            unready_exit_after_secs: 300,
        }
    }
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            absorb_pause_initial: false,
            // The test/profile split is preserved from the old
            // history.rs OnceLock: tests get headroom, production
            // gets the field-validated 64 MiB / 2 gathers posture.
            absorb_global_budget_bytes: if cfg!(test) {
                4 * 1024 * 1024 * 1024
            } else {
                64 * 1024 * 1024
            },
            absorb_global_gathers: if cfg!(test) { 64 } else { 2 },
            cache_bytes: 32 * 1024 * 1024,
            compactor_off: false,
            gc_interval: Some(Duration::from_secs(600)),
        }
    }
}

impl Default for PostingsConfig {
    fn default() -> Self {
        Self {
            cache_bytes: 64 * 1024 * 1024,
        }
    }
}

impl Default for SseConfig {
    fn default() -> Self {
        Self {
            feed_ring_bytes: 1024 * 1024,
            feed_total_bytes: 16 * 1024 * 1024,
            feed_total_bytes_raw: None,
            feed_project_bytes_raw: None,
            heartbeat_ms: 15_000,
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            tail_max_bytes: 1024 * 1024,
            debug_timing: false,
            debug_exit: false,
            binary_sha256: "unknown".into(),
            h1_max_buf: 64 * 1024,
        }
    }
}

impl Default for BillingConfig {
    fn default() -> Self {
        Self {
            mode_env: None,
            meter_enabled: true,
            rollup_env: None,
            path_prefix_env: None,
            outbox_sweep_secs: 300,
            telemetry_drain_secs: 2,
            metrics_interval_secs: 15,
            month_close_grace_ms: 24 * 3_600_000,
            telemetry_cache_bytes: 16 * 1024 * 1024,
            sweep_discovery_max: 8,
            sweep_maint_resident: 2,
            sweep_resident_quantum: 4,
            alert_usage_outbox_dirty: 1000,
        }
    }
}

impl Default for FleetConfig {
    fn default() -> Self {
        Self {
            allow_http_peers: false,
            peer_domains_raw: None,
            rebalance_lag_secs: 60,
            rebalance_move_cooldown_secs: 60,
            self_url: String::new(),
            fleet_min: 1,
            rebalance_return_secs: 300,
        }
    }
}

impl Default for ScaleConfig {
    fn default() -> Self {
        Self {
            eval_secs: 10,
            rate_window_secs: 120.0,
            hot_pct: 75.0 / 100.0,
            cold_pct: 15.0 / 100.0,
            hot_evals: 2,
            cold_evals: 180,
            cooldown_secs: 600,
            max_segments: 64,
        }
    }
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            unabsorbed_bytes_instance: 512 * 1024 * 1024,
            unabsorbed_bytes_shard: 256 * 1024 * 1024,
            absorb_lag_secs: 900,
            maint_release_pct: 75,
            limit_bytes_per_sec: 5_000_000.0,
            limit_reqs_per_sec: 1_000.0,
            limit_recs_per_sec: 5_000.0,
            limit_burst_secs: 2.0,
        }
    }
}
