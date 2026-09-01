//! The command-line surface (WP-01 PR 3.1): parsed once by the binary
//! composition root, then owned by [`crate::config::ServerConfig`].
//!
//! Field order and doc text are the server's `--help` output — move
//! fields between sub-configs only with a matching pin update
//! (`config::tests::cli_surface_is_pinned`).

use clap::Parser;

#[derive(Parser, Debug, Clone, PartialEq)]
#[command(name = "streams-slate", about = "Durable Streams server on SlateDB")]
pub struct CliArgs {
    #[arg(long, default_value = "127.0.0.1:8090")]
    pub(crate) listen: String,

    /// S3-compatible endpoint (e.g. http://127.0.0.1:9500 or Tigris).
    #[arg(long, env = "SLATE_S3_ENDPOINT")]
    pub(crate) s3_endpoint: String,

    /// Default bucket; per-role buckets override it.
    #[arg(long, env = "SLATE_S3_BUCKET", default_value = "streams")]
    pub(crate) bucket: String,
    #[arg(long)]
    pub(crate) ops_bucket: Option<String>,
    #[arg(long)]
    pub(crate) shard_bucket: Option<String>,
    #[arg(long)]
    pub(crate) data_bucket: Option<String>,

    #[arg(long, env = "SLATE_S3_REGION", default_value = "us-east-1")]
    pub(crate) region: String,
    #[arg(long, env = "SLATE_S3_ACCESS_KEY_ID", default_value = "test")]
    pub(crate) access_key_id: String,
    #[arg(long, env = "SLATE_S3_SECRET_ACCESS_KEY", default_value = "test")]
    pub(crate) secret_access_key: String,

    /// Initial shard count (power of two) if no topology exists yet (D3).
    /// Unset = auto: 1 standalone, next_power_of_two(4 × FLEET_MAX) in
    /// fleet mode — a topology as coarse as the fleet gives rendezvous a
    /// permanently uneven draw and turns the rebalancer's override into a
    /// return-home tug-of-war (FLEET-CAMPAIGN.md: 4 shards over 4
    /// instances drew 1/1/2/0 and oscillated on a ~300 s period).
    #[arg(long, env = "INITIAL_SHARDS")]
    pub(crate) initial_shards: Option<usize>,

    /// Shard-log WAL flush interval (D22, amended). 5 ms minted WAL SSTs
    /// ~7× faster than SlateDB's WAL GC reaps them; the growing backlog
    /// degraded the per-DB durable watermark to ~0.3–1 s (EXPERIMENT-PILOT
    /// run 3). 25 ms keeps the ack floor ≈ flush + Tigris PUT ≈ 40 ms while
    /// cutting WAL-object churn 5×.
    #[arg(long, env = "FLUSH_INTERVAL_MS", default_value_t = 25)]
    pub(crate) flush_interval_ms: u64,

    /// Group-commit WAL flushing (1 = on). A per-shard pump flushes the
    /// WAL the moment the previous flush completes when commits are
    /// waiting, so under load the flush cadence self-clocks to the WAL
    /// PUT RTT instead of adding tick alignment (avg tick/2) on top of
    /// the serial-PUT queue. flush_interval_ms then only acts as the
    /// idle mint-rate floor (see --wal-flush-gap-ms) and SlateDB's own
    /// timer is stretched to a 1 s failsafe.
    #[arg(long, env = "WAL_GROUP_COMMIT", default_value_t = 0)]
    pub(crate) wal_group_commit: u8,

    /// Minimum start-to-start gap between pump flushes, ms. Bounds the
    /// WAL SST mint rate exactly like the old tick did (churn ceiling
    /// unchanged); irrelevant whenever the PUT RTT exceeds it. 0 = use
    /// flush_interval_ms.
    #[arg(long, env = "WAL_FLUSH_GAP_MS", default_value_t = 0)]
    pub(crate) wal_flush_gap_ms: u64,

    /// Post-ACK gather window, ms (0 = off). After a busy WAL flush the
    /// pump releases that flush's acknowledgements itself (explicit
    /// barrier), then waits this long before freezing the next WAL, so
    /// closed-loop producers' ack-triggered follow-ups join the next WAL
    /// instead of missing its freeze and paying a full extra PUT. Without
    /// it, append p50 at concurrency 2 measures ~2x concurrency 1.
    /// Suggested 4-8. Adds at most this many ms to a busy flush cycle;
    /// never delays an idle shard's first write.
    #[arg(long, env = "WAL_POST_ACK_GATHER_MS", default_value_t = 0)]
    pub(crate) wal_post_ack_gather_ms: u64,

    /// Skip the gather window when the next WAL already holds at least
    /// this many requests (the window exists for SMALL next generations;
    /// at saturation it is a tax). 0 = never skip.
    #[arg(long, env = "WAL_GATHER_SKIP_REQS", default_value_t = 32)]
    pub(crate) wal_gather_skip_reqs: u32,

    /// Byte-count sibling of --wal-gather-skip-reqs. 0 = never skip.
    #[arg(long, env = "WAL_GATHER_SKIP_BYTES", default_value_t = 1048576)]
    pub(crate) wal_gather_skip_bytes: u64,

    /// Durable-tail ring budget per shard engine, bytes (0 = off). Live
    /// tail reads (long-poll/SSE wakes, catch-up near the head) serve
    /// from an in-memory ring of recently-durable frames published at
    /// ack time, instead of scanning SlateDB. Suggested: 33554432 (32
    /// MiB) — several seconds of a maxed shard's traffic.
    #[arg(long, env = "TAIL_RING_BYTES", default_value_t = 0)]
    pub(crate) tail_ring_bytes: usize,

    /// Target L0 SST size per shard DB. MUST stay below
    /// --max-unflushed-bytes: SlateDB rejects the pair at engine-open
    /// time, and shard engines open lazily, so an invalid pair used to
    /// surface only as a permanent 500 per append (CHAOS-2). The old
    /// default here was 32 MiB against a 16 MiB unflushed cap, which
    /// made a bare `streams-slate` with no environment unbootable in
    /// exactly that silent way. 8 MiB is the field-validated 1 GiB
    /// posture (deploy/profiles/compute-1g.env).
    #[arg(long, env = "L0_SST_SIZE_BYTES", default_value_t = 8 * 1024 * 1024)]
    pub(crate) l0_sst_size_bytes: usize,

    /// Byte-backpressure cap per shard DB (§1.1). SlateDB's default is
    /// 512 MB — a byte-flood on a 1 GB instance OOMs before any request
    /// backpressure fires (bench finding, 2026-07-14).
    #[arg(long, env = "MAX_UNFLUSHED_BYTES", default_value_t = 16 * 1024 * 1024)]
    pub(crate) max_unflushed_bytes: usize,

    /// Effective request-body ceiling. May only LOWER the pinned 32 MiB
    /// protocol maximum, never raise it.
    ///
    /// This is a capacity knob as much as a validator: the absorber
    /// reserves (limit + overhead) × 3 against the admission shed line
    /// for every gather, because one legal oversized frame must be able
    /// to proceed alone. At the 32 MiB pin that is 96.2 MiB — 19% of the
    /// 1 GiB posture's 500 MB line — held while a gather runs, measured
    /// in Singapore against gathers averaging 6 MB of actual work
    /// (CHAOS-3). A deployment whose records are small should say so
    /// here and get the difference back as admitted traffic.
    #[arg(long, env = "MAX_REQUEST_BODY_BYTES", default_value_t = 32 * 1024 * 1024)]
    pub(crate) max_request_body_bytes: usize,

    /// L0 SST count that triggers write backpressure. More L0s = more burst
    /// headroom before compaction must catch up (throughput tuning).
    #[arg(long, env = "L0_MAX_SSTS", default_value_t = 8)]
    pub(crate) l0_max_ssts: usize,

    /// Per-key L0 overlap cap. A totally-ordered stream rewrites its meta
    /// row in every memtable, so every L0 overlaps on that key and the
    /// per-key cap — not l0_max_ssts — becomes the real dispatch gate
    /// (upstream default 8 stalled the flusher; bench finding 2026-07-14).
    /// 0 = follow l0_max_ssts.
    #[arg(long, env = "L0_MAX_SSTS_PER_KEY", default_value_t = 0)]
    pub(crate) l0_max_ssts_per_key: usize,

    /// WAL garbage-collection cadence (seconds). O14a finding: at 50 ms
    /// flush a loaded shard mints ~20 WAL SSTs/s; the upstream default
    /// retention (min_age 300 s, sweep 60 s) keeps thousands of objects
    /// per shard for GC to list and delete while sharing the same object
    /// store path as the ack-critical WAL PUTs. Tighter reaping keeps the
    /// WAL prefix small.
    /// Compactor scheduling poll (ms). Each tick probes the compactions
    /// log — a live Tigris 404 at ~200-240 ms internal (docs/
    /// TIGRIS-404-COST.md), so this is the largest idle-probe class:
    /// 500 ms across 4 shard DBs was 8 probes/s forever. Upstream default
    /// is 5000; the old deploy pin of 500 dated from double-digit-MB/s
    /// single-stream pushes, pre-limiter. At the enforced 5 MB/s/shard a
    /// 2.5 s scheduling gap bounds L0 accumulation to ~12.5 MB (~3 L0
    /// SSTs against L0_MAX 64) — drain continuity comes from concurrent
    /// compactions, not scheduling latency. Field-validated in soak10.
    #[arg(long, env = "COMPACTOR_POLL_MS", default_value_t = crate::DEFAULT_COMPACTOR_POLL_MS)]
    pub(crate) compactor_poll_ms: u64,

    /// Concurrent compactions (upstream default 4). Merges are object-I/O
    /// bound on Tigris, so extra concurrency overlaps GET/PUT latency.
    #[arg(long, env = "COMPACTOR_MAX_CONCURRENT", default_value_t = 4)]
    pub(crate) compactor_max_concurrent: usize,

    // R27-4 compaction-worker memory knobs (COMPACT_MAX_SUBCOMPACTIONS,
    // COMPACT_MAX_FETCH_TASKS, COMPACT_BYTES_TO_FETCH,
    // COMPACT_MAX_SST_SIZE_BYTES) are ENV-ONLY, read by
    // resolved_compactor_options() — the one source every DB family
    // shares. R29 review: clap mirrors here parsed but were never read,
    // so a CLI override silently did nothing; removed rather than
    // duplicating the plumbing.
    #[arg(long, env = "WAL_GC_INTERVAL_SECS", default_value_t = 30)]
    pub(crate) wal_gc_interval_secs: u64,

    /// Static sweep interval (seconds) for the quiet GC directories:
    /// manifest, compacted, and the WAL fence pass. Under the retired
    /// fork these backed off adaptively toward this same value as a
    /// CEILING; upstream SlateDB has no backoff (slatedb#1991 was
    /// declined for #1993), so the ceiling IS the cadence now. Raising
    /// it trades reclamation latency (bounded, storage-cheap) for LIST
    /// steady-state. (--gc-max-interval-secs kept as a flag alias.)
    #[arg(
        long,
        env = "GC_QUIET_INTERVAL_SECS",
        default_value_t = 600,
        alias = "gc-max-interval-secs"
    )]
    pub(crate) gc_quiet_interval_secs: u64,

    /// Minimum WAL SST age before GC may delete it (seconds). Must cover
    /// the reopen/replay window (shard moves replay < ~1 s; 60 s is a
    /// generous safety factor at 5x fewer retained objects than the
    /// 300 s upstream default).
    #[arg(long, env = "WAL_GC_MIN_AGE_SECS", default_value_t = 60)]
    pub(crate) wal_gc_min_age_secs: u64,

    /// Compactions-log GC cadence. The compactions state is a versioned
    /// transactional object: every compactor state change mints another
    /// small `.compactions` file, and shard OPEN must page through the
    /// survivors — at cross-region latency that cost compounds into the
    /// slow-open class behind the eu-central-1 hang (docs/SOAK-REGIONS.md).
    /// Upstream defaults (60s interval / 300s min-age) retain minutes of
    /// churn; we reap harder, like WAL GC.
    #[arg(long, env = "COMPACTIONS_GC_INTERVAL_SECS", default_value_t = 30)]
    pub(crate) compactions_gc_interval_secs: u64,

    /// Min age before a superseded `.compactions` version may be reaped.
    /// Only versions BELOW the GC boundary die, so this is a safety floor
    /// against clock skew, not a retention feature.
    #[arg(long, env = "COMPACTIONS_GC_MIN_AGE_SECS", default_value_t = 120)]
    pub(crate) compactions_gc_min_age_secs: u64,

    /// Manifest poll cadence (ms). This is ALSO how the memtable flusher
    /// learns that compaction freed L0 slots: with a long poll, dispatch
    /// stays gated on a stale L0 view for the whole interval while imm
    /// memtables pile into backpressure (bench finding 2026-07-14: 60 s
    /// poll → 14 s flush stalls). Idle-shard poll cost is ~1 probe-GET
    /// (a Tigris 404, ~200-240 ms internal) per interval; loaded shards
    /// need this at 1-2 s, which is why the idle-cost stretch stops at
    /// 2 s here instead of going longer (docs/TIGRIS-404-COST.md).
    #[arg(long, env = "MANIFEST_POLL_MS", default_value_t = crate::DEFAULT_MANIFEST_POLL_MS)]
    pub(crate) manifest_poll_ms: u64,

    /// Hot-log records deleted per stream per commit group. Trim must
    /// keep pace with ingest in steady state: at 50k records/s and ~1
    /// absorb pass per 5 s, the pass has to retire ~250k records or the
    /// hot DB grows without bound. Tombstones are ~30 B, so even the
    /// high setting is a few MB per batch. The GLOBAL per-commit bound
    /// across all streams is TRIM_GLOBAL_BUDGET.
    #[arg(long, env = "TRIM_PER_OP", default_value_t = 8_192)]
    pub(crate) trim_per_op: u64,

    /// GLOBAL cap on trim deletes per commit group, shared by every
    /// boundary advance and maintenance step in the group. This is what
    /// bounds a mature-fleet second absorption wave: without it one
    /// gather's AbsorbedBatch × TRIM_PER_OP could expand into tens of
    /// millions of deletes in a single WriteBatch (multi-GiB). Leftover
    /// work becomes trim debt, drained a budgeted slice per 5 s tick.
    #[arg(long, env = "TRIM_GLOBAL_BUDGET", default_value_t = 65_536)]
    pub(crate) trim_global_budget: u64,

    /// Plaintext bytes buffered per absorber pass (absorb_one holds a pass
    /// in memory; cap it well below the instance's RAM).
    #[arg(long, env = "ABSORB_PASS_BYTES", default_value_t = 256 * 1024 * 1024)]
    pub(crate) absorb_pass_bytes: u64,

    /// Absorber thresholds (§3.6 / D23).
    #[arg(long, env = "ABSORB_BYTES", default_value_t = 4 * 1024 * 1024)]
    pub(crate) absorb_bytes: u64,
    #[arg(long, env = "ABSORB_AGE_SECS", default_value_t = 300)]
    pub(crate) absorb_age_secs: u64,

    /// Concurrent small-lane absorb passes (1 = fully serial). Streams
    /// with ≤ absorb_small_bytes pending overlap their latency-bound
    /// per-stream passes; bigger streams keep the serial full-budget
    /// lane. The serial grind measured ~4.5 streams/s against wide
    /// backlogs (docs/COST-WIDE1.md §1); peak added memory is bounded by
    /// concurrency × absorb_small_bytes of plaintext.
    #[arg(long, env = "ABSORB_CONCURRENCY", default_value_t = 6)]
    pub(crate) absorb_concurrency: usize,
    #[arg(long, env = "ABSORB_SMALL_BYTES", default_value_t = 1024 * 1024)]
    pub(crate) absorb_small_bytes: u64,

    /// Evict resident per-stream handles idle at least this long
    /// (seconds; 0 = never). Handles reload from the shard DB on next
    /// touch; the durable dirty-stream index keeps unabsorbed evictees
    /// discoverable, so this only trades a tail-row read for memory.
    #[arg(long, env = "HANDLE_IDLE_EVICT_SECS", default_value_t = 600)]
    pub(crate) handle_idle_evict_secs: u64,

    /// Capacity cap on resident per-stream handles per shard (0 =
    /// uncapped). Time-based eviction alone lets a cardinality burst
    /// accumulate rate × idle-window handles; past this cap the ticker
    /// evicts oldest-touched unreferenced handles immediately.
    #[arg(long, env = "HANDLE_MAX_RESIDENT", default_value_t = 65_536)]
    pub(crate) handle_max_resident: usize,

    /// Aggregate byte budget for one shared-history gather WriteBatch
    /// (keys + frames, keyed index rows counted twice). Bounds absorber
    /// peak memory on small instances; streams that do not fit gather on
    /// later ticks. Default = the history DB's unflushed cap.
    #[arg(long, env = "ABSORB_GATHER_MAX_BYTES", default_value_t = 32 * 1024 * 1024)]
    pub(crate) absorb_gather_max_bytes: usize,

    /// Duty-cycle the gather read phase: whenever this much time has
    /// elapsed since the last park, the gather parks ABSORB_PACE_MS
    /// after the current read so append WAL writes queued behind the
    /// reads inside SlateDB drain. Bounds the absorber's append-latency
    /// impact at sparse-many-stream shapes (#266). ABSORB_PACE_MS=0
    /// disables; window 0 parks after every read.
    #[arg(long, env = "ABSORB_PACE_WINDOW_MS", default_value_t = 50)]
    pub(crate) absorb_pace_window_ms: u64,
    #[arg(long, env = "ABSORB_PACE_MS", default_value_t = 0)]
    pub(crate) absorb_pace_ms: u64,

    /// Concurrent per-stream frame reads within one absorber gather.
    /// Shrinks the read phase's wall time — the window during which
    /// append service dips (#266). 1 = serial.
    #[arg(long, env = "ABSORB_READ_PAR", default_value_t = 8)]
    pub(crate) absorb_read_par: usize,

    /// Conformance/dev only: use this stream key (base64url, 32 bytes) for
    /// requests that carry no Stream-Encryption-Key header. The upstream
    /// conformance suite cannot send custom headers.
    #[arg(long)]
    pub(crate) conformance_default_key: Option<String>,

    /// Require `Authorization: Bearer <token>` on all /v1/* requests.
    /// This is the CUSTOMER account token; it never authorizes
    /// /v1/internal/* (round-19: those routes fence consumer
    /// generations and read segment state without a stream key).
    #[arg(long, env = "AUTH_TOKEN")]
    pub(crate) auth_token: Option<String>,
    /// MULTITENANCY §7.2: off | shadow | enforce. Shadow verifies every
    /// product bearer through the customer pipeline and counts the
    /// outcome without touching responses. Enforce is refused at boot
    /// until the route-scope matrix lands (Stage 5b).
    #[arg(long, env = "STREAMS_AUTH_MODE", default_value = "off")]
    pub(crate) streams_auth_mode: String,
    #[arg(
        long,
        env = "STREAMS_AUTH_ISSUER",
        default_value = "https://auth.prisma.io"
    )]
    pub(crate) streams_auth_issuer: String,
    /// Operator-authored snapshot files (src/auth_feed.rs wire formats).
    /// All three are required when STREAMS_AUTH_MODE != off.
    #[arg(long, env = "STREAMS_AUTH_KEYS_FILE")]
    pub(crate) streams_auth_keys_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_POLICY_FILE")]
    pub(crate) streams_auth_policy_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_GRANTS_FILE")]
    pub(crate) streams_auth_grants_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_REFRESH_SECS", default_value_t = 30)]
    pub(crate) streams_auth_refresh_secs: u64,
    /// Base64 32-byte key signing catalog cursors (review item 3).
    /// Set the SAME value fleet-wide so page walks verify on any
    /// instance; optional on a single instance.
    #[arg(long, env = "STREAMS_CURSOR_KEY")]
    pub(crate) streams_cursor_key: Option<String>,

    /// Fleet-internal credential for /v1/internal/* peer RPCs. REQUIRED
    /// when fleet mode is on with FLEET_AUTH_MODE=static (startup
    /// refuses otherwise), MUST differ from --auth-token, and is never
    /// accepted on a product route.
    #[arg(long, env = "FLEET_INTERNAL_TOKEN")]
    pub(crate) fleet_internal_token: Option<String>,

    /// §14.1 (SR2): how this instance authenticates to PEERS.
    /// "static" = the shared bridge token (NAMED legacy posture;
    /// refused under STREAMS_RELEASE_POSTURE=1); "workload" =
    /// short-lived workload JWTs read from WORKLOAD_TOKEN_FILE (the
    /// platform rotates the file), attached to every relay and
    /// force-refreshed once on a peer 401.
    #[arg(long, env = "FLEET_AUTH_MODE", default_value = "static")]
    pub(crate) fleet_auth_mode: String,

    /// Path to the platform-rotated workload JWT (FLEET_AUTH_MODE=
    /// workload). Read lazily with an expiry-aware cache.
    #[arg(long, env = "WORKLOAD_TOKEN_FILE")]
    pub(crate) workload_token_file: Option<std::path::PathBuf>,

    /// Release posture: refuse boot configurations that are bridges,
    /// not GA shapes. Accepts 1/0/true/false — every runbook writes
    /// STREAMS_RELEASE_POSTURE=1 and a posture flag that fails to
    /// parse the documented form would refuse the SAFE configuration.
    #[arg(long, env = "STREAMS_RELEASE_POSTURE", default_value = "false", value_parser = parse_bool_flag)]
    pub(crate) release_posture: bool,

    /// Per-RECORD payload ceiling, independent of the request-body
    /// ceiling (round-10 review): a request may carry MANY records,
    /// but ONE record whose prepared SSE frame exceeds the certified
    /// feed ring turns a valid append into an O(subscribers)
    /// reconnect herd on a shared feed. Unset = unlimited (dev
    /// posture); the release posture REQUIRES a ring-consistent value.
    #[arg(long, env = "MAX_RECORD_PAYLOAD_BYTES")]
    pub(crate) max_record_payload_bytes: Option<usize>,

    /// Billing tenant boundary: the account every stream created on
    /// this deployment bills to (docs/OBSERVABILITY-BILLING.md §3.2).
    #[arg(long, env = "ACCOUNT_ID", default_value = "acct_local")]
    pub(crate) account_id: String,

    /// Billing tenant boundary: the project.
    #[arg(long, env = "PROJECT_ID", default_value = "proj_local")]
    pub(crate) project_id: String,

    /// Telemetry cell identity (one `_usage`/`_ops_*` set per cell).
    #[arg(long, env = "CELL_ID", default_value = "local")]
    pub(crate) cell_id: String,

    /// Region tag on telemetry sources (NOT the object-store region).
    #[arg(long, env = "REGION", default_value = "")]
    pub(crate) telemetry_region: String,

    /// System encryption key for the `_usage` ledger (§8.1). Unset =
    /// telemetry pipeline off. BILLING_MODE=required refuses to start
    /// without it (§14.1).
    #[arg(long, env = "USAGE_STREAM_KEY")]
    pub(crate) usage_stream_key: Option<String>,

    /// "required" makes readiness fail without the usage ledger key.
    #[arg(long, env = "BILLING_MODE", default_value = "off")]
    pub(crate) billing_mode: String,

    /// Run the usage rollup consumer + month closer on THIS instance.
    #[arg(long, env = "ROLLUP", default_value = "0")]
    pub(crate) rollup: String,

    /// Instance tag recorded in metrics records.
    #[arg(long, env = "INSTANCE_NAME", default_value = "streams")]
    pub(crate) instance_name: String,

    /// Key prefix inside the bucket(s): lets independent deployments share
    /// one bucket.
    #[arg(long, env = "PATH_PREFIX")]
    pub(crate) path_prefix: Option<String>,

    /// Fleet coordination prefix (COMPUTE-SPEC §2): heartbeats + desired.json
    /// live here, shared by all instances of the fleet. Enables the
    /// heartbeat/autoscale loop when set.
    #[arg(long, env = "FLEET_PREFIX")]
    pub(crate) fleet_prefix: Option<String>,

    /// Legacy assumed-capacity scaling dimension (req/s per instance).
    /// 0 disables it: measured CPU utilization (scale_out_cpu_pct) is the
    /// primary signal — capacity constants go stale whenever the engine
    /// changes speed (run 5 scaled out at ~5 % actual utilization).
    #[arg(long, env = "SCALE_RPS_CAPACITY", default_value_t = 0)]
    pub(crate) scale_rps_capacity: u64,

    /// Scale-out utilization target (percent of fleet maximum). Both the
    /// capacity dimension (ceil(cores_used/target)) and the hot-instance
    /// dimension use it: scaling triggers as the fleet nears this level.
    #[arg(long, env = "SCALE_OUT_CPU_PCT", default_value_t = 75)]
    pub(crate) scale_out_cpu_pct: u64,

    /// Scale-in utilization ceiling: shrink to N-1 only if projected
    /// post-shrink utilization stays below this (percent). Must sit well
    /// under scale_out_cpu_pct or the fleet flaps at the boundary.
    #[arg(long, env = "SCALE_IN_CPU_PCT", default_value_t = 50)]
    pub(crate) scale_in_cpu_pct: u64,

    /// How long a hot-instance CPU breach must persist before it scales
    /// the fleet (shard handoffs spike CPU briefly).
    #[arg(long, env = "SCALE_CPU_SUSTAIN_SECS", default_value_t = 20)]
    pub(crate) scale_cpu_sustain_secs: u64,

    /// Router-observed client-latency threshold (ms) for the edge scaling
    /// dimension; also blocks scale-in while breached.
    #[arg(long, env = "SCALE_EDGE_LATENCY_MS", default_value_t = 1000)]
    pub(crate) scale_edge_latency_ms: u64,

    /// RSS shed threshold (MB): 429 writes while RSS exceeds this.
    /// Docker phase 1: without it a 1 GB cgroup OOM-kills the instance at
    /// full throughput. MUST sit well below the platform kill line (the
    /// slate-codex A/B died at ~750 MB anon RSS on Prisma Compute with the
    /// shed configured at 800 — an unreachable guard protects nothing).
    /// Default 600 for the ~750 MB pilot instance class; 0 = off.
    /// Round-13: per-project memory-pressure high watermark in bytes
    /// (0 = the backstop is off; the profile pins a certified value).
    #[arg(long, env = "PROJECT_MEMORY_PRESSURE_BYTES", default_value_t = 0)]
    pub(crate) project_memory_pressure_bytes: u64,
    /// Hysteresis release point as a percentage of the high watermark.
    #[arg(long, env = "PROJECT_MEMORY_RELEASE_PCT", default_value_t = 75)]
    pub(crate) project_memory_release_pct: u64,
    #[arg(long, env = "ADMIT_RSS_SHED_MB", default_value_t = 600)]
    pub(crate) admit_rss_shed_mb: u64,

    /// Instance cap on live SSE subscriptions (#267): new subscriptions
    /// past the cap get a typed 503 subscription_capacity instead of
    /// subscriber RSS pushing UNRELATED appends over the write shed
    /// line. 0 = unlimited. Default 10k is the certification rung of
    /// the per-instance ladder (measured ~44 KB/parked conn after
    /// #269 => ~440 MB at the cap); raising it is a deliberate
    /// experimental posture, not part of default certification.
    #[arg(long, env = "SSE_MAX_CONNECTIONS", default_value_t = 10_000)]
    pub(crate) sse_max_connections: u64,

    /// Per-stream inflight append cap (0 = off): one hot stream cannot
    /// occupy every admission slot of its shard owner (scoped 429).
    #[arg(long, env = "ADMIT_MAX_INFLIGHT_PER_STREAM", default_value_t = 64)]
    pub(crate) admit_max_inflight_per_stream: i64,

    /// §12-lite admission backstop: shed /v1/stream requests with 429 +
    /// Retry-After beyond this many in flight (0 = off). Protects the
    /// durable path from queue collapse when offered load exceeds
    /// capacity; pairs with closed-loop clients honoring Retry-After.
    #[arg(long, env = "ADMIT_MAX_INFLIGHT", default_value_t = 0)]
    pub(crate) admit_max_inflight: i64,

    /// Measured per-instance ingress-concurrency capacity through the
    /// platform front door. Two-layer model (platform team investigation
    /// + our 6-source confirmation, 2026-07-15): each SOURCE Compute
    /// instance is egress-capped at ~48-50 outgoing requests; the
    /// DESTINATION front door admits ~145-150 concurrent aggregate (the
    /// earlier 48 calibration was the measuring instance's own egress
    /// cap). Scale-out begins at scale_out_cpu_pct% of this. 0 disables.
    #[arg(long, env = "SCALE_EDGE_SLOTS", default_value_t = 140)]
    pub(crate) scale_edge_slots: u64,

    /// ONE shared block cache across all shard DBs (§1.1). SlateDB's
    /// per-DB default is 512 MB — 16 shards × 512 MB on a 1 GB instance
    /// dies by cache fill in tens of minutes (the run 6/8 zombie
    /// generator; found 2026-07-15).
    #[arg(long, env = "SHARED_CACHE_BYTES", default_value_t = 192 * 1024 * 1024)]
    pub(crate) shared_cache_bytes: u64,

    /// Hysteresis: scale-in only after need has been below the current
    /// desired count for this long (pilot-scaled from the spec's 10 min).
    #[arg(long, env = "SCALE_IN_SECS", default_value_t = 60)]
    pub(crate) scale_in_secs: u64,

    /// Second scaling dimension (COMPUTE-SPEC §4.2): if any loaded live
    /// instance's ack p50 exceeds this, the fleet scales out even when
    /// rps alone wouldn't ask for it — a congested instance suppresses
    /// its own throughput signal (run-3 finding).
    #[arg(long, env = "SCALE_LATENCY_MS", default_value_t = 250)]
    pub(crate) scale_latency_ms: u64,

    /// The latency breach must persist this long before scaling (damps the
    /// transition-churn feedback observed in run 4).
    #[arg(long, env = "SCALE_LAT_SUSTAIN_SECS", default_value_t = 20)]
    pub(crate) scale_lat_sustain_secs: u64,

    /// Maximum fleet size (pilot: the four deployed services).
    #[arg(long, env = "FLEET_MAX", default_value_t = 4)]
    pub(crate) fleet_max: u64,
}

#[cfg(test)]
impl CliArgs {
    /// Hermetic test fixture (PR 3.2). Clap's `env = "..."` attributes
    /// make every `try_parse_from` observe the AMBIENT process
    /// environment for absent flags — a developer or CI variable could
    /// silently change what ordinary config tests parse. This value is
    /// every field written explicitly, equal to what
    /// `["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]`
    /// parses to in a SCRUBBED environment;
    /// `config::tests::cli_fixture_matches_scrubbed_parse` proves that
    /// equality in a cleared-environment subprocess, so a default
    /// change in the clap attributes cannot drift past this fixture.
    pub(crate) fn deterministic() -> Self {
        Self {
            listen: "127.0.0.1:8090".into(),
            s3_endpoint: "http://127.0.0.1:1".into(),
            bucket: "streams".into(),
            ops_bucket: None,
            shard_bucket: None,
            data_bucket: None,
            region: "us-east-1".into(),
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            initial_shards: None,
            flush_interval_ms: 25,
            wal_group_commit: 0,
            wal_flush_gap_ms: 0,
            wal_post_ack_gather_ms: 0,
            wal_gather_skip_reqs: 32,
            wal_gather_skip_bytes: 1_048_576,
            tail_ring_bytes: 0,
            l0_sst_size_bytes: 8 * 1024 * 1024,
            max_unflushed_bytes: 16 * 1024 * 1024,
            max_request_body_bytes: 32 * 1024 * 1024,
            l0_max_ssts: 8,
            l0_max_ssts_per_key: 0,
            compactor_poll_ms: crate::DEFAULT_COMPACTOR_POLL_MS,
            compactor_max_concurrent: 4,
            wal_gc_interval_secs: 30,
            gc_quiet_interval_secs: 600,
            wal_gc_min_age_secs: 60,
            compactions_gc_interval_secs: 30,
            compactions_gc_min_age_secs: 120,
            manifest_poll_ms: crate::DEFAULT_MANIFEST_POLL_MS,
            trim_per_op: 8_192,
            trim_global_budget: 65_536,
            absorb_pass_bytes: 256 * 1024 * 1024,
            absorb_bytes: 4 * 1024 * 1024,
            absorb_age_secs: 300,
            absorb_concurrency: 6,
            absorb_small_bytes: 1024 * 1024,
            handle_idle_evict_secs: 600,
            handle_max_resident: 65_536,
            absorb_gather_max_bytes: 32 * 1024 * 1024,
            absorb_pace_window_ms: 50,
            absorb_pace_ms: 0,
            absorb_read_par: 8,
            conformance_default_key: None,
            auth_token: None,
            streams_auth_mode: "off".into(),
            streams_auth_issuer: "https://auth.prisma.io".into(),
            streams_auth_keys_file: None,
            streams_auth_policy_file: None,
            streams_auth_grants_file: None,
            streams_auth_refresh_secs: 30,
            streams_cursor_key: None,
            fleet_internal_token: None,
            fleet_auth_mode: "static".into(),
            workload_token_file: None,
            release_posture: false,
            max_record_payload_bytes: None,
            account_id: "acct_local".into(),
            project_id: "proj_local".into(),
            cell_id: "local".into(),
            telemetry_region: "".into(),
            usage_stream_key: None,
            billing_mode: "off".into(),
            rollup: "0".into(),
            instance_name: "streams".into(),
            path_prefix: None,
            fleet_prefix: None,
            scale_rps_capacity: 0,
            scale_out_cpu_pct: 75,
            scale_in_cpu_pct: 50,
            scale_cpu_sustain_secs: 20,
            scale_edge_latency_ms: 1000,
            project_memory_pressure_bytes: 0,
            project_memory_release_pct: 75,
            admit_rss_shed_mb: 600,
            sse_max_connections: 10_000,
            admit_max_inflight_per_stream: 64,
            admit_max_inflight: 0,
            scale_edge_slots: 140,
            shared_cache_bytes: 192 * 1024 * 1024,
            scale_in_secs: 60,
            scale_latency_ms: 250,
            scale_lat_sustain_secs: 20,
            fleet_max: 4,
        }
    }
}

/// SR3-1: fleet-auth posture validation, extracted and GLOBAL. The
/// selected mode determines the runtime credential state (workload
/// mode discards any configured static token at construction — see
/// the AppState wiring), and the release posture is validated whether
/// or not fleet mode is on: a single-instance deployment mounts the
/// same raw and internal routes, so it gets the same rules.
fn parse_bool_flag(s: &str) -> Result<bool, String> {
    match s {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        other => Err(format!("expected 1/0/true/false, got {other:?}")),
    }
}
