//! Centralized process configuration (WP-01 PR 3).
//!
//! Every environment read that used to be scattered across modules (52
//! sites in 15 modules, plus the bootstrap helpers) is parsed ONCE here,
//! at startup, into one immutable value. Modules read fields from
//! [`current()`] instead of calling `std::env::var` directly.
//!
//! Rules of the module:
//!
//! - **Fidelity over elegance.** Each field preserves the exact parse
//!   expression, default and divergence of the site it replaces —
//!   including known quirks (see `BillingConfig::path_prefix_env` and the
//!   two readers of `COMPACT_MAX_SST_SIZE_BYTES` with different defaults).
//!   Semantic cleanup is separate work (WP-13/WP-14), not this PR.
//! - **No secrets.** Key material, tokens and credentials live only in
//!   the CLI args (`crate::bootstrap::Args`); [`AppConfig`] is safe to
//!   log via [`AppConfig::redacted_summary`].
//! - **Immutable after install.** Production installs exactly once at
//!   bootstrap before any store opens. Runtime-mutable controls (e.g. the
//!   absorber pause flag) keep their own atomics; config only carries
//!   their initial value. Per-request/per-tick re-reads of process env
//!   were already de-facto frozen (nothing external can rewrite a
//!   running process's environment), so parsing once at startup is
//!   behavior-preserving; the two fleet tests that mutated env now drive
//!   a pure function instead (`fleet::valid_peer_url_with`).

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// The installed process configuration. Production installs the loaded
/// value once during bootstrap; tests that never install get
/// [`AppConfig::default()`] — which is exactly `load()` with an empty
/// environment, because `load()` is defined as `default()` + env overlay.
static CURRENT: std::sync::LazyLock<arc_swap::ArcSwap<AppConfig>> =
    std::sync::LazyLock::new(|| arc_swap::ArcSwap::from_pointee(AppConfig::default()));

/// Install the loaded configuration. Production calls this once, early
/// in bootstrap. Re-installs are permitted so a future TestConfigBuilder
/// can scope configs to rigs without mutating process env.
pub fn install(cfg: AppConfig) {
    CURRENT.store(Arc::new(cfg));
}

/// The current process configuration (installed value, or defaults).
pub fn current() -> Arc<AppConfig> {
    CURRENT.load_full()
}

fn env_parse<T: FromStr>(k: &str) -> Option<T> {
    std::env::var(k).ok().and_then(|v| v.parse().ok())
}

fn dur_ms<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(d.as_millis() as u64)
}

fn opt_dur_ms<S: serde::Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
    match d {
        Some(d) => s.serialize_u64(d.as_millis() as u64),
        None => s.serialize_none(),
    }
}

/// The one immutable configuration graph. Sub-configs are grouped by
/// consuming module. All fields derive `PartialEq` so tests can prove
/// `default()` == "no environment".
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct AppConfig {
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
        let mut co = slatedb::config::CompactorOptions::default();
        co.poll_interval = Duration::from_millis(self.compactor_poll_ms);
        co.max_concurrent_compactions = self.compactor_max_concurrent;
        let mut w = co.worker.take().unwrap_or_default();
        w.max_concurrent_compactions = self.compactor_max_concurrent;
        w.max_subcompactions = self.compact_max_subcompactions;
        w.max_fetch_tasks = self.compact_max_fetch_tasks;
        w.bytes_to_fetch = self.compact_bytes_to_fetch;
        w.max_sst_size = self.compact_max_sst_size;
        co.worker = Some(w);
        co
    }
}

/// Shard-directory runtime knobs.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ShardRuntimeConfig {
    /// SHARD_OPEN_DEADLINE_MS, default 180 s (`OpenGate` construction).
    #[serde(serialize_with = "dur_ms")]
    pub open_deadline: Duration,
    /// SHARD_OPEN_WAIT_MS, default 10_000 — per-request open-gate wait.
    pub open_wait_ms: u64,
    /// UNREADY_EXIT_AFTER_SECS, default 300 (0 disables the watchdog).
    pub unready_exit_after_secs: u64,
}

/// History/absorber knobs (src/history.rs).
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
    #[serde(serialize_with = "opt_dur_ms")]
    pub gc_interval: Option<Duration>,
}

/// Postings cache (src/postings_cache.rs).
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct PostingsConfig {
    /// POSTINGS_CACHE_BYTES, default 64 MiB.
    pub cache_bytes: usize,
}

/// LiveFeed budgets and heartbeat (src/sse/).
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct CryptoConfig {
    /// FRAME_COMPRESS: "1" or case-insensitive "true", default false.
    pub frame_compress: bool,
}

/// Process/runtime identity and certification controls.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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

impl AppConfig {
    /// Parse the process environment once. Defined as defaults + overlay
    /// so `AppConfig::default()` is provably the no-environment value.
    pub fn load() -> Self {
        let mut cfg = Self::default();
        cfg.overlay_env();
        cfg
    }

    fn overlay_env(&mut self) {
        // --- storage ---
        if let Some(v) = env_parse("POOL_IDLE_SECS") {
            self.storage.pool_idle_secs = v;
        }
        if let Some(v) = env_parse("STORE_MAX_CONCURRENT") {
            self.storage.store_max_concurrent = v;
        }
        if let Some(v) = env_parse("STORE_BULK_INFLIGHT_MAX_BYTES") {
            self.storage.bulk_inflight_max_bytes = v;
        }
        if let Some(v) = env_parse::<u64>("COMPACT_MAX_SST_SIZE_BYTES") {
            // One env name feeds BOTH knobs, with different defaults
            // (256 MiB compactor roll vs 8 MiB nominal GET weight) —
            // preserved divergence, do not "fix" here.
            self.storage.bulk_nominal_get_bytes = v;
        }
        if let Some(v) = env_parse::<usize>("COMPACT_MAX_SST_SIZE_BYTES") {
            self.engine.compact_max_sst_size = v;
        }

        // --- engine ---
        if let Some(v) = env_parse("COMPACTOR_POLL_MS") {
            self.engine.compactor_poll_ms = v;
        }
        if let Some(v) = env_parse("COMPACTOR_MAX_CONCURRENT") {
            self.engine.compactor_max_concurrent = v;
        }
        if let Some(v) = env_parse("COMPACT_MAX_SUBCOMPACTIONS") {
            self.engine.compact_max_subcompactions = v;
        }
        if let Some(v) = env_parse("COMPACT_MAX_FETCH_TASKS") {
            self.engine.compact_max_fetch_tasks = v;
        }
        if let Some(v) = env_parse("COMPACT_BYTES_TO_FETCH") {
            self.engine.compact_bytes_to_fetch = v;
        }
        if let Some(v) = env_parse("SLATEDB_RT_THREADS") {
            self.engine.slatedb_rt_threads = v;
        }

        // --- shard runtime ---
        if let Some(v) = env_parse::<u64>("SHARD_OPEN_DEADLINE_MS") {
            self.shard.open_deadline = Duration::from_millis(v);
        }
        if let Some(v) = env_parse("SHARD_OPEN_WAIT_MS") {
            self.shard.open_wait_ms = v;
        }
        if let Some(v) = env_parse("UNREADY_EXIT_AFTER_SECS") {
            self.shard.unready_exit_after_secs = v;
        }

        // --- history ---
        if std::env::var("ABSORB_PAUSE").ok().as_deref() == Some("1") {
            self.history.absorb_pause_initial = true;
        }
        if let Some(v) = env_parse("ABSORB_GLOBAL_BUDGET_BYTES") {
            self.history.absorb_global_budget_bytes = v;
        }
        if let Some(v) = env_parse::<usize>("ABSORB_GLOBAL_GATHERS") {
            self.history.absorb_global_gathers = v.max(1);
        }
        if let Some(v) = env_parse("HISTORY_CACHE_BYTES") {
            self.history.cache_bytes = v;
        }
        if std::env::var("HISTORY_COMPACTOR")
            .map(|v| v == "off")
            .unwrap_or(false)
        {
            self.history.compactor_off = true;
        }
        {
            // Current name first, legacy alias as fallback; 0 disables.
            let secs = std::env::var("HISTORY_GC_INTERVAL_SECS")
                .ok()
                .or_else(|| std::env::var("HISTORY_GC_MAX_INTERVAL_SECS").ok())
                .and_then(|v| v.parse::<u64>().ok());
            if let Some(secs) = secs {
                self.history.gc_interval = (secs > 0).then(|| Duration::from_secs(secs));
            }
        }

        // --- postings ---
        if let Some(v) = env_parse("POSTINGS_CACHE_BYTES") {
            self.postings.cache_bytes = v;
        }

        // --- sse ---
        if let Ok(raw) = std::env::var("SSE_FEED_RING_BYTES") {
            self.sse.feed_ring_bytes = raw.trim().parse().unwrap_or_else(|_| {
                tracing::warn!(
                    "SSE_FEED_RING_BYTES={raw:?} does not parse as a byte count; \
                     using the 1 MiB default"
                );
                1024 * 1024
            });
        }
        self.sse.feed_total_bytes_raw = std::env::var("SSE_FEED_TOTAL_BYTES").ok();
        if let Ok(raw) = std::env::var("SSE_FEED_TOTAL_BYTES") {
            self.sse.feed_total_bytes = raw.trim().parse().unwrap_or_else(|_| {
                tracing::warn!(
                    "SSE_FEED_TOTAL_BYTES={raw:?} does not parse as a byte count; \
                     using the 16 MiB default"
                );
                16 * 1024 * 1024
            });
        }
        self.sse.feed_project_bytes_raw = std::env::var("SSE_FEED_PROJECT_BYTES").ok();
        if let Some(v) = env_parse::<u64>("SSE_HEARTBEAT_MS").filter(|v| *v > 0) {
            self.sse.heartbeat_ms = v;
        }

        // --- http ---
        if let Some(v) = env_parse::<usize>("TAIL_MAX_BYTES").filter(|v| *v > 0) {
            self.http.tail_max_bytes = v;
        }
        self.http.debug_timing = std::env::var("STREAMS_DEBUG_TIMING").as_deref() == Ok("1");
        self.http.debug_exit = std::env::var("STREAMS_DEBUG_EXIT").as_deref() == Ok("1");
        if let Ok(v) = std::env::var("APP_BINARY_SHA256") {
            self.http.binary_sha256 = v;
        }
        if let Some(v) = env_parse("SSE_H1_MAX_BUF") {
            self.http.h1_max_buf = v;
        }

        // --- billing / telemetry / rollup ---
        self.billing.mode_env = std::env::var("BILLING_MODE").ok();
        self.billing.meter_enabled = std::env::var("BILLING_METER")
            .map(|v| v != "off")
            .unwrap_or(true);
        self.billing.rollup_env = std::env::var("ROLLUP").ok();
        self.billing.path_prefix_env = std::env::var("PATH_PREFIX").ok();
        if let Some(v) = env_parse("OUTBOX_SWEEP_SECS") {
            self.billing.outbox_sweep_secs = v;
        }
        if let Some(v) = env_parse("TELEMETRY_DRAIN_SECS") {
            self.billing.telemetry_drain_secs = v;
        }
        if let Some(v) = env_parse("METRICS_INTERVAL_SECS") {
            self.billing.metrics_interval_secs = v;
        }
        if let Some(v) = env_parse("MONTH_CLOSE_GRACE_MS") {
            self.billing.month_close_grace_ms = v;
        }
        if let Some(v) = env_parse("TELEMETRY_CACHE_BYTES") {
            self.billing.telemetry_cache_bytes = v;
        }
        if let Some(v) = env_parse("SWEEP_DISCOVERY_MAX") {
            self.billing.sweep_discovery_max = v;
        }
        if let Some(v) = env_parse::<usize>("SWEEP_MAINT_RESIDENT") {
            // Stored RAW: the binary refuses 0 at boot (0 would starve
            // cold-debt drain); the billing adapter floors at the use
            // site (`sweep_resident_budget`, max(1)).
            self.billing.sweep_maint_resident = v;
        }
        if let Some(v) = env_parse::<usize>("SWEEP_RESIDENT_QUANTUM") {
            // Raw here too; floored at the use site (max(1)).
            self.billing.sweep_resident_quantum = v;
        }
        if let Some(v) = env_parse("ALERT_USAGE_OUTBOX_DIRTY") {
            self.billing.alert_usage_outbox_dirty = v;
        }

        // --- fleet ---
        self.fleet.allow_http_peers =
            std::env::var("FLEET_ALLOW_HTTP_PEERS").ok().as_deref() == Some("1");
        self.fleet.peer_domains_raw = std::env::var("FLEET_PEER_DOMAINS").ok();
        if let Some(v) = env_parse("REBALANCE_LAG_SECS") {
            self.fleet.rebalance_lag_secs = v;
        }
        if let Some(v) = env_parse("REBALANCE_MOVE_COOLDOWN_SECS") {
            self.fleet.rebalance_move_cooldown_secs = v;
        }
        if let Ok(v) = std::env::var("SELF_URL") {
            self.fleet.self_url = v;
        }
        if let Some(v) = env_parse::<u64>("FLEET_MIN") {
            self.fleet.fleet_min = v.max(1);
        }
        if let Some(v) = env_parse("REBALANCE_RETURN_SECS") {
            self.fleet.rebalance_return_secs = v;
        }

        // --- scaler (envf: f64 parse, cast at the same points) ---
        let envf = |k: &str, d: f64| env_parse(k).unwrap_or(d);
        if std::env::var("SCALE_EVAL_SECS").is_ok() {
            self.scaler.eval_secs = envf("SCALE_EVAL_SECS", 10.0) as u64;
        }
        if std::env::var("SCALE_RATE_WINDOW_SECS").is_ok() {
            self.scaler.rate_window_secs = envf("SCALE_RATE_WINDOW_SECS", 120.0);
        }
        if std::env::var("SCALE_HOT_PCT").is_ok() {
            self.scaler.hot_pct = envf("SCALE_HOT_PCT", 75.0) / 100.0;
        }
        if std::env::var("SCALE_COLD_PCT").is_ok() {
            self.scaler.cold_pct = envf("SCALE_COLD_PCT", 15.0) / 100.0;
        }
        if std::env::var("SCALE_HOT_EVALS").is_ok() {
            self.scaler.hot_evals = envf("SCALE_HOT_EVALS", 2.0) as u32;
        }
        if std::env::var("SCALE_COLD_EVALS").is_ok() {
            self.scaler.cold_evals = envf("SCALE_COLD_EVALS", 180.0) as u32;
        }
        if std::env::var("SCALE_COOLDOWN_SECS").is_ok() {
            self.scaler.cooldown_secs = envf("SCALE_COOLDOWN_SECS", 600.0) as i64;
        }
        if std::env::var("MAX_SEGMENTS_PER_STREAM").is_ok() {
            self.scaler.max_segments = envf("MAX_SEGMENTS_PER_STREAM", 64.0) as usize;
        }

        // --- admission / usage limits ---
        if let Some(v) = env_parse("MAX_UNABSORBED_BYTES_PER_INSTANCE") {
            self.admission.unabsorbed_bytes_instance = v;
        }
        if let Some(v) = env_parse("MAX_UNABSORBED_BYTES_PER_SHARD") {
            self.admission.unabsorbed_bytes_shard = v;
        }
        if let Some(v) = env_parse("MAX_ABSORB_LAG_SECS") {
            self.admission.absorb_lag_secs = v;
        }
        if let Some(v) = env_parse::<u64>("MAINT_BACKPRESSURE_RELEASE_PCT") {
            self.admission.maint_release_pct = v.min(100);
        }
        if let Some(v) = env_parse("LIMIT_BYTES_PER_SEC") {
            self.admission.limit_bytes_per_sec = v;
        }
        if let Some(v) = env_parse("LIMIT_REQS_PER_SEC") {
            self.admission.limit_reqs_per_sec = v;
        }
        if let Some(v) = env_parse("LIMIT_RECS_PER_SEC") {
            self.admission.limit_recs_per_sec = v;
        }
        if let Some(v) = env_parse("LIMIT_BURST_SECS") {
            self.admission.limit_burst_secs = v;
        }

        // --- crypto ---
        self.crypto.frame_compress = std::env::var("FRAME_COMPRESS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        // --- runtime / certification ---
        self.runtime.memprofile_cert = std::env::var("MEMPROFILE_CERT").ok();
        self.runtime.cert_sealed_publish_delay_ms_raw =
            std::env::var("STREAMS_CERT_SEALED_PUBLISH_DELAY_MS").ok();
        self.runtime.certification_mode = std::env::var("STREAMS_CERTIFICATION_MODE").ok();
    }

    /// The effective configuration, safe to log: AppConfig never carries
    /// key material, tokens or credentials (those stay in the CLI args).
    pub fn redacted_summary(&self) -> serde_json::Value {
        serde_json::json!(self)
    }

    // serde::Serialize derives exist ONLY for this redacted diagnostics
    // payload. AppConfig contains no secrets by construction; a future
    // field that cannot be serialized (or should not be logged) must be
    // excluded explicitly at its declaration.
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                pool_idle_secs: 4,
                store_max_concurrent: 0,
                bulk_inflight_max_bytes: 0,
                bulk_nominal_get_bytes: 8 * 1024 * 1024,
            },
            engine: EngineConfig {
                compactor_poll_ms: crate::DEFAULT_COMPACTOR_POLL_MS,
                compactor_max_concurrent: 4,
                compact_max_subcompactions: 4,
                compact_max_fetch_tasks: 4,
                compact_bytes_to_fetch: 2 * 1024 * 1024,
                compact_max_sst_size: 256 * 1024 * 1024,
                slatedb_rt_threads: 2,
            },
            shard: ShardRuntimeConfig {
                open_deadline: Duration::from_secs(180),
                open_wait_ms: 10_000,
                unready_exit_after_secs: 300,
            },
            history: HistoryConfig {
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
            },
            postings: PostingsConfig {
                cache_bytes: 64 * 1024 * 1024,
            },
            sse: SseConfig {
                feed_ring_bytes: 1024 * 1024,
                feed_total_bytes: 16 * 1024 * 1024,
                feed_total_bytes_raw: None,
                feed_project_bytes_raw: None,
                heartbeat_ms: 15_000,
            },
            http: HttpConfig {
                tail_max_bytes: 1024 * 1024,
                debug_timing: false,
                debug_exit: false,
                binary_sha256: "unknown".into(),
                h1_max_buf: 64 * 1024,
            },
            billing: BillingConfig {
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
            },
            fleet: FleetConfig {
                allow_http_peers: false,
                peer_domains_raw: None,
                rebalance_lag_secs: 60,
                rebalance_move_cooldown_secs: 60,
                self_url: String::new(),
                fleet_min: 1,
                rebalance_return_secs: 300,
            },
            scaler: ScaleConfig {
                eval_secs: 10,
                rate_window_secs: 120.0,
                hot_pct: 75.0 / 100.0,
                cold_pct: 15.0 / 100.0,
                hot_evals: 2,
                cold_evals: 180,
                cooldown_secs: 600,
                max_segments: 64,
            },
            admission: AdmissionConfig {
                unabsorbed_bytes_instance: 512 * 1024 * 1024,
                unabsorbed_bytes_shard: 256 * 1024 * 1024,
                absorb_lag_secs: 900,
                maint_release_pct: 75,
                limit_bytes_per_sec: 5_000_000.0,
                limit_reqs_per_sec: 1_000.0,
                limit_recs_per_sec: 5_000.0,
                limit_burst_secs: 2.0,
            },
            crypto: CryptoConfig {
                frame_compress: false,
            },
            runtime: RuntimeConfig {
                memprofile_cert: None,
                cert_sealed_publish_delay_ms_raw: None,
                certification_mode: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    //! Config-parser tests are the ONLY tests allowed to mutate process
    //! environment, and they serialize on ENV_LOCK while doing so.
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Every environment variable the loader consumes. This is the
    /// registry the architecture report cross-checks: a direct env read
    /// outside this list (and the binaries' pre-runtime checks) is a
    /// WP-01 violation.
    const ENV_KNOBS: &[&str] = &[
        "POOL_IDLE_SECS",
        "STORE_MAX_CONCURRENT",
        "STORE_BULK_INFLIGHT_MAX_BYTES",
        "COMPACT_MAX_SST_SIZE_BYTES",
        "COMPACTOR_POLL_MS",
        "COMPACTOR_MAX_CONCURRENT",
        "COMPACT_MAX_SUBCOMPACTIONS",
        "COMPACT_MAX_FETCH_TASKS",
        "COMPACT_BYTES_TO_FETCH",
        "SLATEDB_RT_THREADS",
        "SHARD_OPEN_DEADLINE_MS",
        "SHARD_OPEN_WAIT_MS",
        "UNREADY_EXIT_AFTER_SECS",
        "ABSORB_PAUSE",
        "ABSORB_GLOBAL_BUDGET_BYTES",
        "ABSORB_GLOBAL_GATHERS",
        "HISTORY_CACHE_BYTES",
        "HISTORY_COMPACTOR",
        "HISTORY_GC_INTERVAL_SECS",
        "HISTORY_GC_MAX_INTERVAL_SECS",
        "POSTINGS_CACHE_BYTES",
        "SSE_FEED_RING_BYTES",
        "SSE_FEED_TOTAL_BYTES",
        "SSE_FEED_PROJECT_BYTES",
        "SSE_HEARTBEAT_MS",
        "TAIL_MAX_BYTES",
        "STREAMS_DEBUG_TIMING",
        "STREAMS_DEBUG_EXIT",
        "APP_BINARY_SHA256",
        "SSE_H1_MAX_BUF",
        "BILLING_MODE",
        "BILLING_METER",
        "ROLLUP",
        "PATH_PREFIX",
        "OUTBOX_SWEEP_SECS",
        "TELEMETRY_DRAIN_SECS",
        "METRICS_INTERVAL_SECS",
        "MONTH_CLOSE_GRACE_MS",
        "TELEMETRY_CACHE_BYTES",
        "SWEEP_DISCOVERY_MAX",
        "SWEEP_MAINT_RESIDENT",
        "SWEEP_RESIDENT_QUANTUM",
        "ALERT_USAGE_OUTBOX_DIRTY",
        "FLEET_ALLOW_HTTP_PEERS",
        "FLEET_PEER_DOMAINS",
        "REBALANCE_LAG_SECS",
        "REBALANCE_MOVE_COOLDOWN_SECS",
        "SELF_URL",
        "FLEET_MIN",
        "REBALANCE_RETURN_SECS",
        "SCALE_EVAL_SECS",
        "SCALE_RATE_WINDOW_SECS",
        "SCALE_HOT_PCT",
        "SCALE_COLD_PCT",
        "SCALE_HOT_EVALS",
        "SCALE_COLD_EVALS",
        "SCALE_COOLDOWN_SECS",
        "MAX_SEGMENTS_PER_STREAM",
        "MAX_UNABSORBED_BYTES_PER_INSTANCE",
        "MAX_UNABSORBED_BYTES_PER_SHARD",
        "MAX_ABSORB_LAG_SECS",
        "MAINT_BACKPRESSURE_RELEASE_PCT",
        "LIMIT_BYTES_PER_SEC",
        "LIMIT_REQS_PER_SEC",
        "LIMIT_RECS_PER_SEC",
        "LIMIT_BURST_SECS",
        "FRAME_COMPRESS",
        "MEMPROFILE_CERT",
        "STREAMS_CERT_SEALED_PUBLISH_DELAY_MS",
        "STREAMS_CERTIFICATION_MODE",
    ];

    /// Run `f` with every config knob removed from the environment,
    /// restoring whatever was there before (serialized).
    fn with_clean_env<R>(f: impl FnOnce() -> R) -> R {
        let _g = ENV_LOCK.lock().unwrap();
        let saved: Vec<(&str, Option<String>)> = ENV_KNOBS
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        for (k, _) in &saved {
            unsafe { std::env::remove_var(k) };
        }
        let r = f();
        for (k, v) in &saved {
            match v {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        r
    }

    #[test]
    fn default_equals_load_with_empty_environment() {
        with_clean_env(|| {
            assert_eq!(
                AppConfig::load(),
                AppConfig::default(),
                "load() with no config environment must equal default()"
            );
        });
    }

    #[test]
    fn default_values_are_pinned() {
        // The no-environment posture, knob by knob. Every literal here
        // is the pre-WP-01 default, moved not changed; a PR that edits
        // one must justify a configuration behavior change.
        let c = AppConfig::default();
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
        assert_eq!(c.shard.open_deadline, Duration::from_secs(180));
        assert_eq!(c.shard.open_wait_ms, 10_000);
        assert_eq!(c.shard.unready_exit_after_secs, 300);
        assert!(!c.history.absorb_pause_initial);
        assert_eq!(c.history.absorb_global_budget_bytes, 4 * 1024 * 1024 * 1024); // cfg(test)
        assert_eq!(c.history.absorb_global_gathers, 64); // cfg(test)
        assert_eq!(c.history.cache_bytes, 32 * 1024 * 1024);
        assert!(!c.history.compactor_off);
        assert_eq!(c.history.gc_interval, Some(Duration::from_secs(600)));
        assert_eq!(c.postings.cache_bytes, 64 * 1024 * 1024);
        assert_eq!(c.sse.feed_ring_bytes, 1024 * 1024);
        assert_eq!(c.sse.feed_total_bytes, 16 * 1024 * 1024);
        assert_eq!(c.sse.feed_total_bytes_raw, None);
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
        with_clean_env(|| {
            unsafe {
                std::env::set_var("STORE_BULK_INFLIGHT_MAX_BYTES", "4096");
                // One env name, two knobs, preserved divergence:
                std::env::set_var("COMPACT_MAX_SST_SIZE_BYTES", "123456");
                std::env::set_var("SSE_FEED_RING_BYTES", "garbage"); // warn + default
                std::env::set_var("SSE_HEARTBEAT_MS", "0"); // filtered -> default
                std::env::set_var("MAINT_BACKPRESSURE_RELEASE_PCT", "140"); // min(100)
                std::env::set_var("SWEEP_MAINT_RESIDENT", "0"); // stored raw (boot check)
                std::env::set_var("HISTORY_GC_INTERVAL_SECS", "0"); // 0 -> None
                std::env::set_var("HISTORY_GC_MAX_INTERVAL_SECS", "42"); // alias used only when
                // the current name is unset
                std::env::set_var("FRAME_COMPRESS", "TrUe");
                std::env::set_var("SCALE_HOT_PCT", "90.0");
                std::env::set_var("BILLING_METER", "off");
                std::env::set_var("FLEET_ALLOW_HTTP_PEERS", "1");
            }
            let c = AppConfig::load();
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
        });
        with_clean_env(|| {
            unsafe { std::env::set_var("HISTORY_GC_MAX_INTERVAL_SECS", "42") };
            let c = AppConfig::load();
            assert_eq!(c.history.gc_interval, Some(Duration::from_secs(42)));
        });
    }

    #[test]
    fn redacted_summary_contains_no_secret_channels() {
        with_clean_env(|| {
            let v = AppConfig::default().redacted_summary();
            let s = v.to_string();
            for forbidden in [
                "key",
                "token",
                "secret",
                "password",
                "credential",
                "STREAMS_CURSOR_KEY",
                "AUTH_TOKEN",
            ] {
                assert!(
                    !s.to_lowercase().contains(&forbidden.to_lowercase()),
                    "redacted summary must not mention {forbidden}: {s}"
                );
            }
        });
    }

    /// The complete CLI surface, pinned: every long flag, its env
    /// name and its default. A PR that renames/rewires an option fails
    /// here first. Table generated from clap's own argument registry.
    #[test]
    fn cli_surface_is_pinned() {
        use clap::CommandFactory;
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
        let cmd = crate::bootstrap::Args::command();
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
}
