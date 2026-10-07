//! The configuration model: `ServerConfig` and its sub-configs
//! (WP-01 PR 3.1 — the parsed, owned configuration graph).
//!
//! Rules of the model:
//!
//! - **Fidelity over elegance.** Each field preserves the exact parse
//!   expression, default and divergence of the site it replaced —
//!   including known quirks (see the two readers of
//!   `COMPACT_MAX_SST_SIZE_BYTES`, which share one default).
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
    /// The parsed CLI surface. Contains secret material
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
    /// STORE_BULK_INFLIGHT_MAX_BYTES, default 32 MiB (0 = off).
    /// Readers: the bulk gate in store_timing (clamped to u32 at use)
    /// and the compactor profile JSON (raw u64).
    pub bulk_inflight_max_bytes: u64,
    /// store_timing's nominal weight for unknown-length GETs. Reads
    /// COMPACT_MAX_SST_SIZE_BYTES, default 32 MiB: the same name and the
    /// same default as `EngineConfig::compact_max_sst_size`, in its own
    /// field because the gate takes a u64.
    pub bulk_nominal_get_bytes: u64,
}

/// SlateDB engine knobs (one resolved compactor profile for EVERY
/// SlateDB this process opens — R27-4/R28).
#[derive(Clone, Debug, PartialEq)]
pub struct EngineConfig {
    /// `--compactor-poll-ms` (env COMPACTOR_POLL_MS), default
    /// `crate::DEFAULT_COMPACTOR_POLL_MS`. Clap owns it: `with_knob_defaults`
    /// copies the resolved value so an argv override reaches every DB family.
    pub compactor_poll_ms: u64,
    /// `--compactor-max-concurrent` (env COMPACTOR_MAX_CONCURRENT),
    /// default 1, clap-owned like the poll interval. This and the four
    /// worker values below default to the certified 1 GiB posture
    /// (deploy/profiles/compute-1g.env), not to SlateDB's own 4 / 4 / 4 /
    /// 2 MiB / 256 MiB, under which a 32-input L0 merge stages about 1 GB.
    pub compactor_max_concurrent: usize,
    /// COMPACT_MAX_SUBCOMPACTIONS, default 1.
    pub compact_max_subcompactions: usize,
    /// COMPACT_MAX_FETCH_TASKS, default 1.
    pub compact_max_fetch_tasks: usize,
    /// COMPACT_BYTES_TO_FETCH, default 1 MiB.
    pub compact_bytes_to_fetch: usize,
    /// COMPACT_MAX_SST_SIZE_BYTES, default 32 MiB (the compactor's
    /// reader — see `StorageConfig::bulk_nominal_get_bytes` for the
    /// other reader of the same env name).
    pub compact_max_sst_size: usize,
    /// SLATEDB_RT_THREADS, default 4 (the 1 GiB profile's value). Worker
    /// threads of the dedicated SlateDB runtime.
    pub slatedb_rt_threads: usize,
}

impl EngineConfig {
    // The garbage-collection cadences and age floors of every shard
    // database. Constants: no deployment set them, and the two age floors
    // are safety margins, not tuning.
    /// WAL garbage-collection cadence. O14a finding: at 50 ms flush a
    /// loaded shard mints ~20 WAL SSTs/s; the upstream default retention
    /// (min_age 300 s, sweep 60 s) keeps thousands of objects per shard for
    /// GC to list and delete while sharing the same object store path as
    /// the ack-critical WAL PUTs. Tighter reaping keeps the WAL prefix
    /// small.
    pub(crate) const WAL_GC_INTERVAL: Duration = Duration::from_secs(30);
    /// Minimum WAL SST age before GC may delete it. Must cover the
    /// reopen/replay window (shard moves replay < ~1 s; 60 s is a generous
    /// safety factor at 5x fewer retained objects than the 300 s upstream
    /// default).
    pub(crate) const WAL_GC_MIN_AGE: Duration = Duration::from_secs(60);
    /// Compactions-log GC cadence. The compactions state is a versioned
    /// transactional object: every compactor state change mints another
    /// small `.compactions` file, and shard OPEN must page through the
    /// survivors — at cross-region latency that cost compounds into the
    /// slow-open class behind the eu-central-1 hang (docs/SOAK-REGIONS.md).
    /// Upstream defaults (60s interval / 300s min-age) retain minutes of
    /// churn; we reap harder, like WAL GC.
    pub(crate) const COMPACTIONS_GC_INTERVAL: Duration = Duration::from_secs(30);
    /// Min age before a superseded `.compactions` version may be reaped.
    /// Only versions BELOW the GC boundary die, so this is a safety floor
    /// against clock skew, not a retention feature.
    pub(crate) const COMPACTIONS_GC_MIN_AGE: Duration = Duration::from_secs(120);
    /// Static sweep interval for the quiet GC directories: manifest,
    /// compacted, and the WAL fence pass. Under the retired fork these
    /// backed off adaptively toward this same value as a CEILING; upstream
    /// SlateDB has no backoff (slatedb#1991 was declined for #1993), so the
    /// ceiling IS the cadence now: reclamation latency (bounded,
    /// storage-cheap) is traded for LIST steady-state.
    pub(crate) const GC_QUIET_INTERVAL: Duration = Duration::from_secs(600);

    // The thresholds of the post-acknowledgement gather window
    // (`--wal-post-ack-gather-ms`). Constants since edge record #80: the
    // window exists for SMALL next generations, so the pump skips it when
    // the next WAL already holds this many requests or bytes (at
    // saturation the window is a latency and throughput tax).
    /// Requests already committed-but-unflushed above which the gather
    /// window is skipped.
    pub(crate) const WAL_GATHER_SKIP_REQS: u32 = 32;
    /// Bytes already committed-but-unflushed above which the gather window
    /// is skipped (1 MiB).
    pub(crate) const WAL_GATHER_SKIP_BYTES: u64 = 1_048_576;

    /// SlateDB's own WAL flush timer on a shard log while the group-commit
    /// pump runs (owner decision of 2026-10-07, the write tier): a failsafe
    /// only. Every commit wakes the pump, so no acknowledgement waits for
    /// the timer; a tick that finds the WAL buffer non-empty (commits
    /// waiting out the pump's gap) writes them early as one more WAL
    /// object. At 1 s that was about 0.8 a second on every busy shard.
    pub(crate) const WAL_FAILSAFE_INTERVAL: Duration = Duration::from_secs(60);

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
    /// ABSORB_GLOBAL_BUDGET_BYTES, default 100,859,904: one worst-frame
    /// build at the 32 MiB body pin, (32 MiB + 64 KiB) x3. The runtime
    /// floors a smaller value at that build for the server's own ceiling
    /// (`HistoryResources::with_body_limit`).
    pub absorb_global_budget_bytes: usize,
    /// ABSORB_GLOBAL_GATHERS, max(1), default 1 (the 1 GiB profile's value).
    pub absorb_global_gathers: usize,
    /// HISTORY_CACHE_BYTES, default 32 MiB.
    pub cache_bytes: usize,
    /// GC sweep interval of every history database: 600 s. No environment
    /// name sets it; `None` (no sweeps) exists for tests that build the value.
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
    /// SSE_FEED_TOTAL_BYTES, default 64 MiB; unparseable warns + default.
    pub feed_total_bytes: u64,
    /// RAW SSE_FEED_TOTAL_BYTES string, for release-posture validation
    /// (`bootstrap::validate_release_capacity` refuses garbage outright;
    /// the warn-and-default above is only the lazy-reader contract).
    pub feed_total_bytes_raw: Option<String>,
    /// SSE_FEED_PROJECT_BYTES, RAW string. The strict parse stays at the
    /// use site (`sse::feed::configured_project_cap`) because release
    /// posture turns it into a hard boot error; default there = global/2.
    pub feed_project_bytes_raw: Option<String>,
    /// SSE_HEARTBEAT_MS, default 15_000; 0/unparseable = default.
    pub heartbeat_ms: u64,
}

/// HTTP-surface runtime knobs (src/http.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct HttpConfig {
    /// Page budget of a read woken by a long-poll wait: 1 MiB. No
    /// environment name sets it; tests vary it on the read command.
    pub tail_max_bytes: usize,
    /// STREAMS_DEBUG_TIMING == "1", default false.
    pub debug_timing: bool,
    /// STREAMS_DEBUG_EXIT == "1", default false (forbidden).
    pub debug_exit: bool,
    /// APP_BINARY_SHA256, default "unknown" (debug endpoint payload).
    pub binary_sha256: String,
    /// 64 KiB, at least `MIN_H1_MAX_BUF`; no environment name sets it — the h1
    /// read-buffer threshold hyper tests only after a head fails to parse:
    /// it limits memory per connection and does not bound header values.
    pub h1_max_buf: usize,
    /// SSE_H1_HEADER_TIMEOUT_MS, default 120_000 — the request-head and
    /// idle keep-alive deadline (`http::serve::h1_builder`); 0/unparseable =
    /// default. Never disabled: hyper without it holds a headless socket
    /// for ever.
    pub h1_header_timeout: std::time::Duration,
    /// The drain floor (shared cells H3, `http::serve::drain`): a
    /// connection whose client accepts fewer than `drain_min_bytes` of a
    /// pending response in a `drain_window` is closed. 10 s and 16 KiB; no
    /// environment name sets them, rigs shorten them.
    pub drain_window: std::time::Duration,
    pub drain_min_bytes: u64,
}

/// Billing/telemetry/rollup knobs (src/billing.rs, src/ops.rs).
#[derive(Clone, Debug, PartialEq)]
pub struct BillingConfig {
    /// OUTBOX_SWEEP_SECS, default 300.
    pub outbox_sweep_secs: u64,
    /// TELEMETRY_DRAIN_SECS, default 8 (the write tier; 2 before edge
    /// change #117): usage reaches `_usage`, and so the usage answers, up to
    /// one cadence after it is metered. Also bounds the terminal drain
    /// round a graceful stop runs; keep it below the 10 s supervisor
    /// grace, above it the supervisor's abort is the bound.
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
    /// SCALE_HOT_PCT, default 75 → stored /100 as 0.75. A segment is cold
    /// below 5% of this fraction of each limit.
    pub hot_pct: f64,
    /// SCALE_HOT_EVALS, default 2. A stream merges after every segment
    /// stayed cold for four times this many evaluations.
    pub hot_evals: u32,
    /// SCALE_COOLDOWN_SECS, default 600.
    pub cooldown_secs: i64,
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
    /// LIMIT_BYTES_PER_SEC, default 5_000_000 (finite, >= 0; 0 disables;
    /// an enabled bucket must hold >= 1 token — `validate()`).
    pub limit_bytes_per_sec: f64,
    /// LIMIT_REQS_PER_SEC, default 1_000 (same rule).
    pub limit_reqs_per_sec: f64,
    /// LIMIT_RECS_PER_SEC, default 5_000 (same rule).
    pub limit_recs_per_sec: f64,
    /// LIMIT_BURST_SECS, default 2 (finite, > 0).
    pub limit_burst_secs: f64,
}

/// Crypto framing: no setting since layout 5. Stored pages compress
/// whenever that pays, and `format=frames` responses never compress (every
/// frame is version 4), so no response length depends on how well a record
/// compresses. FRAME_COMPRESS is no longer read: a retired name, like the
/// names of edge records #80 to #85.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CryptoConfig {
    /// Always false: the wire's compression choice the frozen
    /// `render_raw_read` passes to `read_payload`, so `format=frames`
    /// answers version 4 frames. Nothing sets it; `bootstrap::run` also
    /// hands it to `shard_config`, which ignores it. It stays a field only
    /// because those frozen scopes read it, and goes with the next approved
    /// change to them.
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
    /// TOKIO_WORKERS; None = one per available core.
    pub tokio_workers: Option<usize>,
}

impl RuntimeConfig {
    /// The Tokio worker count: TOKIO_WORKERS, else one per available core,
    /// never below two. Run 13 measured ~230 ms p50 timer drift (vs 4 ms
    /// for a raw thread) from inline blocking work; on a 1-vCPU box one
    /// worker lets a single blocking poll freeze every future, durable
    /// acks included (O14a), so a second worker lets the OS timeslice
    /// around it.
    pub fn worker_threads(&self, available: Option<std::num::NonZeroUsize>) -> usize {
        self.tokio_workers
            .unwrap_or_else(|| available.map_or(1, std::num::NonZeroUsize::get))
            .max(2)
    }
}

impl ServerConfig {
    /// The no-environment knob posture over `cli` (whose compactor poll
    /// interval and compaction concurrency it carries). `load()` overlays
    /// the environment on top of this, so `load(cli, empty_env)` is
    /// provably this value.
    pub(crate) fn with_knob_defaults(cli: CliArgs) -> Self {
        let engine = EngineConfig {
            compactor_poll_ms: cli.compactor_poll_ms,
            compactor_max_concurrent: cli.compactor_max_concurrent,
            ..EngineConfig::default()
        };
        Self {
            cli,
            storage: Default::default(),
            engine,
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
            bulk_inflight_max_bytes: 32 * 1024 * 1024,
            bulk_nominal_get_bytes: 32 * 1024 * 1024,
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            compactor_poll_ms: crate::DEFAULT_COMPACTOR_POLL_MS,
            compactor_max_concurrent: 1,
            compact_max_subcompactions: 1,
            compact_max_fetch_tasks: 1,
            compact_bytes_to_fetch: 1024 * 1024,
            compact_max_sst_size: 32 * 1024 * 1024,
            slatedb_rt_threads: 4,
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
            // The field-validated posture in every build. Budgets are per
            // runtime, so a test that needs more headroom states it in its
            // own HistoryConfig; the default never forks on the build.
            absorb_global_budget_bytes: 100_859_904,
            absorb_global_gathers: 1,
            cache_bytes: 32 * 1024 * 1024,
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
            feed_total_bytes: 64 * 1024 * 1024,
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
            h1_header_timeout: std::time::Duration::from_secs(120),
            drain_window: std::time::Duration::from_secs(10),
            drain_min_bytes: 16 * 1024,
        }
    }
}

impl Default for BillingConfig {
    fn default() -> Self {
        Self {
            outbox_sweep_secs: 300,
            telemetry_drain_secs: 8,
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

impl BillingConfig {
    /// The longest a metered read stays in memory only, so what a hard
    /// process loss can drop (OBSERVABILITY-BILLING §7.4): its window seals
    /// at the first drain round after it is `READ_FLUSH_INTERVAL_MS` old,
    /// and that round spools it. The loop floors its cadence at 1 s.
    pub(crate) fn read_loss_window_secs(&self) -> u64 {
        let interval = (crate::billing::READ_FLUSH_INTERVAL_MS / 1000).unsigned_abs();
        interval.saturating_add(self.telemetry_drain_secs.max(1))
    }
}

impl FleetConfig {
    /// Bounds runtime ring allocation and every persisted membership document.
    pub const MAX_MEMBERS: u64 = 4096;
}

impl HttpConfig {
    /// hyper's `http1::Builder::max_buf_size` asserts at least this (its
    /// private `MINIMUM_MAX_BUFFER_SIZE`) inside `serve_h1`, after bootstrap
    /// has opened engines and spawned loops; validation refuses less
    /// before anything boots.
    pub const MIN_H1_MAX_BUF: usize = 8 * 1024;
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
            hot_evals: 2,
            cooldown_secs: 600,
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
