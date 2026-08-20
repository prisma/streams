#![recursion_limit = "512"]

mod audit;
mod auth;
mod auth_feed;
mod backpressure;
mod billing;
mod crypto;
#[cfg(test)]
mod dst;
mod failpoints;
mod fleet;
mod history;
mod http;
mod livehub;
#[cfg(test)]
mod mt_lint;
mod offsets;
mod operator;
mod ops;
mod postings;
mod postings_cache;
mod product;
mod product_cursor;
mod project_policy;
mod protocol_pin;
mod queue;
mod quota;
mod registry;
mod rollup;
mod scaler3;
mod segmap;
mod shard;
mod sharddir;
mod sketch;
mod store_timing;
mod tenant;
mod touch;
mod touch_keys;
mod usage;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// musl's allocator fragments badly under this workload (docker phase 1:
// RSS 2x the accounted budgets); mimalloc keeps RSS near actual live set.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Context;
use clap::Parser;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::{ObjectStore, ObjectStoreExt};
use slatedb::Db;
use slatedb::config::Settings;

use crate::history::{Absorber, AbsorberConfig, KeyCache, absorber_channel};
use crate::http::AppState;
use crate::registry::{Registry, load_or_init_topology};
use crate::shard::{ShardConfig, ShardEngine};

/// Default metadata-poll cadences, shared with the DST idle-cost pin
/// (`idle_engine_store_traffic_is_bounded_by_the_poll_cadence`). Every
/// manifest/compactions poll is a live probe-GET against Tigris — a 404
/// that costs ~200-240 ms of Tigris-internal work (docs/TIGRIS-404-COST.md)
/// — so these cadences are a cost posture, not just a freshness knob.
/// Deploy scripts intentionally do NOT override them.
pub const DEFAULT_MANIFEST_POLL_MS: u64 = 2000;
pub const DEFAULT_COMPACTOR_POLL_MS: u64 = 2500;

#[derive(Parser, Debug)]
#[command(name = "streams-slate", about = "Durable Streams server on SlateDB")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8090")]
    listen: String,

    /// S3-compatible endpoint (e.g. http://127.0.0.1:9500 or Tigris).
    #[arg(long, env = "SLATE_S3_ENDPOINT")]
    s3_endpoint: String,

    /// Default bucket; per-role buckets override it.
    #[arg(long, env = "SLATE_S3_BUCKET", default_value = "streams")]
    bucket: String,
    #[arg(long)]
    ops_bucket: Option<String>,
    #[arg(long)]
    shard_bucket: Option<String>,
    #[arg(long)]
    data_bucket: Option<String>,

    #[arg(long, env = "SLATE_S3_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "SLATE_S3_ACCESS_KEY_ID", default_value = "test")]
    access_key_id: String,
    #[arg(long, env = "SLATE_S3_SECRET_ACCESS_KEY", default_value = "test")]
    secret_access_key: String,

    /// Initial shard count (power of two) if no topology exists yet (D3).
    /// Unset = auto: 1 standalone, next_power_of_two(4 × FLEET_MAX) in
    /// fleet mode — a topology as coarse as the fleet gives rendezvous a
    /// permanently uneven draw and turns the rebalancer's override into a
    /// return-home tug-of-war (FLEET-CAMPAIGN.md: 4 shards over 4
    /// instances drew 1/1/2/0 and oscillated on a ~300 s period).
    #[arg(long, env = "INITIAL_SHARDS")]
    initial_shards: Option<usize>,

    /// Shard-log WAL flush interval (D22, amended). 5 ms minted WAL SSTs
    /// ~7× faster than SlateDB's WAL GC reaps them; the growing backlog
    /// degraded the per-DB durable watermark to ~0.3–1 s (EXPERIMENT-PILOT
    /// run 3). 25 ms keeps the ack floor ≈ flush + Tigris PUT ≈ 40 ms while
    /// cutting WAL-object churn 5×.
    #[arg(long, env = "FLUSH_INTERVAL_MS", default_value_t = 25)]
    flush_interval_ms: u64,

    /// Group-commit WAL flushing (1 = on). A per-shard pump flushes the
    /// WAL the moment the previous flush completes when commits are
    /// waiting, so under load the flush cadence self-clocks to the WAL
    /// PUT RTT instead of adding tick alignment (avg tick/2) on top of
    /// the serial-PUT queue. flush_interval_ms then only acts as the
    /// idle mint-rate floor (see --wal-flush-gap-ms) and SlateDB's own
    /// timer is stretched to a 1 s failsafe.
    #[arg(long, env = "WAL_GROUP_COMMIT", default_value_t = 0)]
    wal_group_commit: u8,

    /// Minimum start-to-start gap between pump flushes, ms. Bounds the
    /// WAL SST mint rate exactly like the old tick did (churn ceiling
    /// unchanged); irrelevant whenever the PUT RTT exceeds it. 0 = use
    /// flush_interval_ms.
    #[arg(long, env = "WAL_FLUSH_GAP_MS", default_value_t = 0)]
    wal_flush_gap_ms: u64,

    /// Post-ACK gather window, ms (0 = off). After a busy WAL flush the
    /// pump releases that flush's acknowledgements itself (explicit
    /// barrier), then waits this long before freezing the next WAL, so
    /// closed-loop producers' ack-triggered follow-ups join the next WAL
    /// instead of missing its freeze and paying a full extra PUT. Without
    /// it, append p50 at concurrency 2 measures ~2x concurrency 1.
    /// Suggested 4-8. Adds at most this many ms to a busy flush cycle;
    /// never delays an idle shard's first write.
    #[arg(long, env = "WAL_POST_ACK_GATHER_MS", default_value_t = 0)]
    wal_post_ack_gather_ms: u64,

    /// Skip the gather window when the next WAL already holds at least
    /// this many requests (the window exists for SMALL next generations;
    /// at saturation it is a tax). 0 = never skip.
    #[arg(long, env = "WAL_GATHER_SKIP_REQS", default_value_t = 32)]
    wal_gather_skip_reqs: u32,

    /// Byte-count sibling of --wal-gather-skip-reqs. 0 = never skip.
    #[arg(long, env = "WAL_GATHER_SKIP_BYTES", default_value_t = 1048576)]
    wal_gather_skip_bytes: u64,

    /// Durable-tail ring budget per shard engine, bytes (0 = off). Live
    /// tail reads (long-poll/SSE wakes, catch-up near the head) serve
    /// from an in-memory ring of recently-durable frames published at
    /// ack time, instead of scanning SlateDB. Suggested: 33554432 (32
    /// MiB) — several seconds of a maxed shard's traffic.
    #[arg(long, env = "TAIL_RING_BYTES", default_value_t = 0)]
    tail_ring_bytes: usize,

    /// Target L0 SST size per shard DB. MUST stay below
    /// --max-unflushed-bytes: SlateDB rejects the pair at engine-open
    /// time, and shard engines open lazily, so an invalid pair used to
    /// surface only as a permanent 500 per append (CHAOS-2). The old
    /// default here was 32 MiB against a 16 MiB unflushed cap, which
    /// made a bare `streams-slate` with no environment unbootable in
    /// exactly that silent way. 8 MiB is the field-validated 1 GiB
    /// posture (deploy/profiles/compute-1g.env).
    #[arg(long, env = "L0_SST_SIZE_BYTES", default_value_t = 8 * 1024 * 1024)]
    l0_sst_size_bytes: usize,

    /// Byte-backpressure cap per shard DB (§1.1). SlateDB's default is
    /// 512 MB — a byte-flood on a 1 GB instance OOMs before any request
    /// backpressure fires (bench finding, 2026-07-14).
    #[arg(long, env = "MAX_UNFLUSHED_BYTES", default_value_t = 16 * 1024 * 1024)]
    max_unflushed_bytes: usize,

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
    max_request_body_bytes: usize,

    /// L0 SST count that triggers write backpressure. More L0s = more burst
    /// headroom before compaction must catch up (throughput tuning).
    #[arg(long, env = "L0_MAX_SSTS", default_value_t = 8)]
    l0_max_ssts: usize,

    /// Per-key L0 overlap cap. A totally-ordered stream rewrites its meta
    /// row in every memtable, so every L0 overlaps on that key and the
    /// per-key cap — not l0_max_ssts — becomes the real dispatch gate
    /// (upstream default 8 stalled the flusher; bench finding 2026-07-14).
    /// 0 = follow l0_max_ssts.
    #[arg(long, env = "L0_MAX_SSTS_PER_KEY", default_value_t = 0)]
    l0_max_ssts_per_key: usize,

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
    compactor_poll_ms: u64,

    /// Concurrent compactions (upstream default 4). Merges are object-I/O
    /// bound on Tigris, so extra concurrency overlaps GET/PUT latency.
    #[arg(long, env = "COMPACTOR_MAX_CONCURRENT", default_value_t = 4)]
    compactor_max_concurrent: usize,

    // R27-4 compaction-worker memory knobs (COMPACT_MAX_SUBCOMPACTIONS,
    // COMPACT_MAX_FETCH_TASKS, COMPACT_BYTES_TO_FETCH,
    // COMPACT_MAX_SST_SIZE_BYTES) are ENV-ONLY, read by
    // resolved_compactor_options() — the one source every DB family
    // shares. R29 review: clap mirrors here parsed but were never read,
    // so a CLI override silently did nothing; removed rather than
    // duplicating the plumbing.
    #[arg(long, env = "WAL_GC_INTERVAL_SECS", default_value_t = 30)]
    wal_gc_interval_secs: u64,

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
    gc_quiet_interval_secs: u64,

    /// Minimum WAL SST age before GC may delete it (seconds). Must cover
    /// the reopen/replay window (shard moves replay < ~1 s; 60 s is a
    /// generous safety factor at 5x fewer retained objects than the
    /// 300 s upstream default).
    #[arg(long, env = "WAL_GC_MIN_AGE_SECS", default_value_t = 60)]
    wal_gc_min_age_secs: u64,

    /// Compactions-log GC cadence. The compactions state is a versioned
    /// transactional object: every compactor state change mints another
    /// small `.compactions` file, and shard OPEN must page through the
    /// survivors — at cross-region latency that cost compounds into the
    /// slow-open class behind the eu-central-1 hang (docs/SOAK-REGIONS.md).
    /// Upstream defaults (60s interval / 300s min-age) retain minutes of
    /// churn; we reap harder, like WAL GC.
    #[arg(long, env = "COMPACTIONS_GC_INTERVAL_SECS", default_value_t = 30)]
    compactions_gc_interval_secs: u64,

    /// Min age before a superseded `.compactions` version may be reaped.
    /// Only versions BELOW the GC boundary die, so this is a safety floor
    /// against clock skew, not a retention feature.
    #[arg(long, env = "COMPACTIONS_GC_MIN_AGE_SECS", default_value_t = 120)]
    compactions_gc_min_age_secs: u64,

    /// Manifest poll cadence (ms). This is ALSO how the memtable flusher
    /// learns that compaction freed L0 slots: with a long poll, dispatch
    /// stays gated on a stale L0 view for the whole interval while imm
    /// memtables pile into backpressure (bench finding 2026-07-14: 60 s
    /// poll → 14 s flush stalls). Idle-shard poll cost is ~1 probe-GET
    /// (a Tigris 404, ~200-240 ms internal) per interval; loaded shards
    /// need this at 1-2 s, which is why the idle-cost stretch stops at
    /// 2 s here instead of going longer (docs/TIGRIS-404-COST.md).
    #[arg(long, env = "MANIFEST_POLL_MS", default_value_t = crate::DEFAULT_MANIFEST_POLL_MS)]
    manifest_poll_ms: u64,

    /// Hot-log records deleted per stream per commit group. Trim must
    /// keep pace with ingest in steady state: at 50k records/s and ~1
    /// absorb pass per 5 s, the pass has to retire ~250k records or the
    /// hot DB grows without bound. Tombstones are ~30 B, so even the
    /// high setting is a few MB per batch. The GLOBAL per-commit bound
    /// across all streams is TRIM_GLOBAL_BUDGET.
    #[arg(long, env = "TRIM_PER_OP", default_value_t = 8_192)]
    trim_per_op: u64,

    /// GLOBAL cap on trim deletes per commit group, shared by every
    /// boundary advance and maintenance step in the group. This is what
    /// bounds a mature-fleet second absorption wave: without it one
    /// gather's AbsorbedBatch × TRIM_PER_OP could expand into tens of
    /// millions of deletes in a single WriteBatch (multi-GiB). Leftover
    /// work becomes trim debt, drained a budgeted slice per 5 s tick.
    #[arg(long, env = "TRIM_GLOBAL_BUDGET", default_value_t = 65_536)]
    trim_global_budget: u64,

    /// Plaintext bytes buffered per absorber pass (absorb_one holds a pass
    /// in memory; cap it well below the instance's RAM).
    #[arg(long, env = "ABSORB_PASS_BYTES", default_value_t = 256 * 1024 * 1024)]
    absorb_pass_bytes: u64,

    /// Absorber thresholds (§3.6 / D23).
    #[arg(long, env = "ABSORB_BYTES", default_value_t = 4 * 1024 * 1024)]
    absorb_bytes: u64,
    #[arg(long, env = "ABSORB_AGE_SECS", default_value_t = 300)]
    absorb_age_secs: u64,

    /// Concurrent small-lane absorb passes (1 = fully serial). Streams
    /// with ≤ absorb_small_bytes pending overlap their latency-bound
    /// per-stream passes; bigger streams keep the serial full-budget
    /// lane. The serial grind measured ~4.5 streams/s against wide
    /// backlogs (docs/COST-WIDE1.md §1); peak added memory is bounded by
    /// concurrency × absorb_small_bytes of plaintext.
    #[arg(long, env = "ABSORB_CONCURRENCY", default_value_t = 6)]
    absorb_concurrency: usize,
    #[arg(long, env = "ABSORB_SMALL_BYTES", default_value_t = 1024 * 1024)]
    absorb_small_bytes: u64,

    /// Evict resident per-stream handles idle at least this long
    /// (seconds; 0 = never). Handles reload from the shard DB on next
    /// touch; the durable dirty-stream index keeps unabsorbed evictees
    /// discoverable, so this only trades a tail-row read for memory.
    #[arg(long, env = "HANDLE_IDLE_EVICT_SECS", default_value_t = 600)]
    handle_idle_evict_secs: u64,

    /// Capacity cap on resident per-stream handles per shard (0 =
    /// uncapped). Time-based eviction alone lets a cardinality burst
    /// accumulate rate × idle-window handles; past this cap the ticker
    /// evicts oldest-touched unreferenced handles immediately.
    #[arg(long, env = "HANDLE_MAX_RESIDENT", default_value_t = 65_536)]
    handle_max_resident: usize,

    /// Aggregate byte budget for one shared-history gather WriteBatch
    /// (keys + frames, keyed index rows counted twice). Bounds absorber
    /// peak memory on small instances; streams that do not fit gather on
    /// later ticks. Default = the history DB's unflushed cap.
    #[arg(long, env = "ABSORB_GATHER_MAX_BYTES", default_value_t = 32 * 1024 * 1024)]
    absorb_gather_max_bytes: usize,

    /// Duty-cycle the gather read phase: whenever this much time has
    /// elapsed since the last park, the gather parks ABSORB_PACE_MS
    /// after the current read so append WAL writes queued behind the
    /// reads inside SlateDB drain. Bounds the absorber's append-latency
    /// impact at sparse-many-stream shapes (#266). ABSORB_PACE_MS=0
    /// disables; window 0 parks after every read.
    #[arg(long, env = "ABSORB_PACE_WINDOW_MS", default_value_t = 50)]
    absorb_pace_window_ms: u64,
    #[arg(long, env = "ABSORB_PACE_MS", default_value_t = 0)]
    absorb_pace_ms: u64,

    /// Concurrent per-stream frame reads within one absorber gather.
    /// Shrinks the read phase's wall time — the window during which
    /// append service dips (#266). 1 = serial.
    #[arg(long, env = "ABSORB_READ_PAR", default_value_t = 8)]
    absorb_read_par: usize,

    /// Conformance/dev only: use this stream key (base64url, 32 bytes) for
    /// requests that carry no Stream-Encryption-Key header. The upstream
    /// conformance suite cannot send custom headers.
    #[arg(long)]
    conformance_default_key: Option<String>,

    /// Require `Authorization: Bearer <token>` on all /v1/* requests.
    /// This is the CUSTOMER account token; it never authorizes
    /// /v1/internal/* (round-19: those routes fence consumer
    /// generations and read segment state without a stream key).
    #[arg(long, env = "AUTH_TOKEN")]
    auth_token: Option<String>,
    /// MULTITENANCY §7.2: off | shadow | enforce. Shadow verifies every
    /// product bearer through the customer pipeline and counts the
    /// outcome without touching responses. Enforce is refused at boot
    /// until the route-scope matrix lands (Stage 5b).
    #[arg(long, env = "STREAMS_AUTH_MODE", default_value = "off")]
    streams_auth_mode: String,
    #[arg(
        long,
        env = "STREAMS_AUTH_ISSUER",
        default_value = "https://auth.prisma.io"
    )]
    streams_auth_issuer: String,
    /// Operator-authored snapshot files (src/auth_feed.rs wire formats).
    /// All three are required when STREAMS_AUTH_MODE != off.
    #[arg(long, env = "STREAMS_AUTH_KEYS_FILE")]
    streams_auth_keys_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_POLICY_FILE")]
    streams_auth_policy_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_GRANTS_FILE")]
    streams_auth_grants_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_REFRESH_SECS", default_value_t = 30)]
    streams_auth_refresh_secs: u64,
    /// Base64 32-byte key signing catalog cursors (review item 3).
    /// Set the SAME value fleet-wide so page walks verify on any
    /// instance; optional on a single instance.
    #[arg(long, env = "STREAMS_CURSOR_KEY")]
    streams_cursor_key: Option<String>,

    /// Fleet-internal credential for /v1/internal/* peer RPCs. REQUIRED
    /// when fleet mode is on with FLEET_AUTH_MODE=static (startup
    /// refuses otherwise), MUST differ from --auth-token, and is never
    /// accepted on a product route.
    #[arg(long, env = "FLEET_INTERNAL_TOKEN")]
    fleet_internal_token: Option<String>,

    /// §14.1 (SR2): how this instance authenticates to PEERS.
    /// "static" = the shared bridge token (NAMED legacy posture;
    /// refused under STREAMS_RELEASE_POSTURE=1); "workload" =
    /// short-lived workload JWTs read from WORKLOAD_TOKEN_FILE (the
    /// platform rotates the file), attached to every relay and
    /// force-refreshed once on a peer 401.
    #[arg(long, env = "FLEET_AUTH_MODE", default_value = "static")]
    fleet_auth_mode: String,

    /// Path to the platform-rotated workload JWT (FLEET_AUTH_MODE=
    /// workload). Read lazily with an expiry-aware cache.
    #[arg(long, env = "WORKLOAD_TOKEN_FILE")]
    workload_token_file: Option<std::path::PathBuf>,

    /// Release posture: refuse boot configurations that are bridges,
    /// not GA shapes. Accepts 1/0/true/false — every runbook writes
    /// STREAMS_RELEASE_POSTURE=1 and a posture flag that fails to
    /// parse the documented form would refuse the SAFE configuration.
    #[arg(long, env = "STREAMS_RELEASE_POSTURE", default_value = "false", value_parser = parse_bool_flag)]
    release_posture: bool,

    /// Billing tenant boundary: the account every stream created on
    /// this deployment bills to (docs/OBSERVABILITY-BILLING.md §3.2).
    #[arg(long, env = "ACCOUNT_ID", default_value = "acct_local")]
    account_id: String,

    /// Billing tenant boundary: the project.
    #[arg(long, env = "PROJECT_ID", default_value = "proj_local")]
    project_id: String,

    /// Telemetry cell identity (one `_usage`/`_ops_*` set per cell).
    #[arg(long, env = "CELL_ID", default_value = "local")]
    cell_id: String,

    /// Region tag on telemetry sources (NOT the object-store region).
    #[arg(long, env = "REGION", default_value = "")]
    telemetry_region: String,

    /// System encryption key for the `_usage` ledger (§8.1). Unset =
    /// telemetry pipeline off. BILLING_MODE=required refuses to start
    /// without it (§14.1).
    #[arg(long, env = "USAGE_STREAM_KEY")]
    usage_stream_key: Option<String>,

    /// "required" makes readiness fail without the usage ledger key.
    #[arg(long, env = "BILLING_MODE", default_value = "off")]
    billing_mode: String,

    /// Run the usage rollup consumer + month closer on THIS instance.
    #[arg(long, env = "ROLLUP", default_value = "0")]
    rollup: String,

    /// Instance tag recorded in metrics records.
    #[arg(long, env = "INSTANCE_NAME", default_value = "streams")]
    instance_name: String,

    /// Key prefix inside the bucket(s): lets independent deployments share
    /// one bucket.
    #[arg(long, env = "PATH_PREFIX")]
    path_prefix: Option<String>,

    /// Fleet coordination prefix (COMPUTE-SPEC §2): heartbeats + desired.json
    /// live here, shared by all instances of the fleet. Enables the
    /// heartbeat/autoscale loop when set.
    #[arg(long, env = "FLEET_PREFIX")]
    fleet_prefix: Option<String>,

    /// Legacy assumed-capacity scaling dimension (req/s per instance).
    /// 0 disables it: measured CPU utilization (scale_out_cpu_pct) is the
    /// primary signal — capacity constants go stale whenever the engine
    /// changes speed (run 5 scaled out at ~5 % actual utilization).
    #[arg(long, env = "SCALE_RPS_CAPACITY", default_value_t = 0)]
    scale_rps_capacity: u64,

    /// Scale-out utilization target (percent of fleet maximum). Both the
    /// capacity dimension (ceil(cores_used/target)) and the hot-instance
    /// dimension use it: scaling triggers as the fleet nears this level.
    #[arg(long, env = "SCALE_OUT_CPU_PCT", default_value_t = 75)]
    scale_out_cpu_pct: u64,

    /// Scale-in utilization ceiling: shrink to N-1 only if projected
    /// post-shrink utilization stays below this (percent). Must sit well
    /// under scale_out_cpu_pct or the fleet flaps at the boundary.
    #[arg(long, env = "SCALE_IN_CPU_PCT", default_value_t = 50)]
    scale_in_cpu_pct: u64,

    /// How long a hot-instance CPU breach must persist before it scales
    /// the fleet (shard handoffs spike CPU briefly).
    #[arg(long, env = "SCALE_CPU_SUSTAIN_SECS", default_value_t = 20)]
    scale_cpu_sustain_secs: u64,

    /// Router-observed client-latency threshold (ms) for the edge scaling
    /// dimension; also blocks scale-in while breached.
    #[arg(long, env = "SCALE_EDGE_LATENCY_MS", default_value_t = 1000)]
    scale_edge_latency_ms: u64,

    /// RSS shed threshold (MB): 429 writes while RSS exceeds this.
    /// Docker phase 1: without it a 1 GB cgroup OOM-kills the instance at
    /// full throughput. MUST sit well below the platform kill line (the
    /// slate-codex A/B died at ~750 MB anon RSS on Prisma Compute with the
    /// shed configured at 800 — an unreachable guard protects nothing).
    /// Default 600 for the ~750 MB pilot instance class; 0 = off.
    #[arg(long, env = "ADMIT_RSS_SHED_MB", default_value_t = 600)]
    admit_rss_shed_mb: u64,

    /// Instance cap on live SSE subscriptions (#267): new subscriptions
    /// past the cap get a typed 503 subscription_capacity instead of
    /// subscriber RSS pushing UNRELATED appends over the write shed
    /// line. 0 = unlimited. Default = the pre-Phase-1 measured-safe
    /// ceiling; raise after the idle-slope probe on the slimmed path.
    #[arg(long, env = "SSE_MAX_CONNECTIONS", default_value_t = 10_000)]
    sse_max_connections: u64,

    /// SSE Phase 2 live-hub fanout (#268): 1 = eligible product SSE
    /// connections ride per-stream shared hubs. Feature-gated pending
    /// the review-battery closure (tasks #270-#275).
    #[arg(long, env = "SSE_LIVE_HUB", default_value_t = 0)]
    sse_live_hub: u8,

    /// Per-stream inflight append cap (0 = off): one hot stream cannot
    /// occupy every admission slot of its shard owner (scoped 429).
    #[arg(long, env = "ADMIT_MAX_INFLIGHT_PER_STREAM", default_value_t = 64)]
    admit_max_inflight_per_stream: i64,

    /// §12-lite admission backstop: shed /v1/stream requests with 429 +
    /// Retry-After beyond this many in flight (0 = off). Protects the
    /// durable path from queue collapse when offered load exceeds
    /// capacity; pairs with closed-loop clients honoring Retry-After.
    #[arg(long, env = "ADMIT_MAX_INFLIGHT", default_value_t = 0)]
    admit_max_inflight: i64,

    /// Measured per-instance ingress-concurrency capacity through the
    /// platform front door. Two-layer model (platform team investigation
    /// + our 6-source confirmation, 2026-07-15): each SOURCE Compute
    /// instance is egress-capped at ~48-50 outgoing requests; the
    /// DESTINATION front door admits ~145-150 concurrent aggregate (the
    /// earlier 48 calibration was the measuring instance's own egress
    /// cap). Scale-out begins at scale_out_cpu_pct% of this. 0 disables.
    #[arg(long, env = "SCALE_EDGE_SLOTS", default_value_t = 140)]
    scale_edge_slots: u64,

    /// ONE shared block cache across all shard DBs (§1.1). SlateDB's
    /// per-DB default is 512 MB — 16 shards × 512 MB on a 1 GB instance
    /// dies by cache fill in tens of minutes (the run 6/8 zombie
    /// generator; found 2026-07-15).
    #[arg(long, env = "SHARED_CACHE_BYTES", default_value_t = 192 * 1024 * 1024)]
    shared_cache_bytes: u64,

    /// Hysteresis: scale-in only after need has been below the current
    /// desired count for this long (pilot-scaled from the spec's 10 min).
    #[arg(long, env = "SCALE_IN_SECS", default_value_t = 60)]
    scale_in_secs: u64,

    /// Second scaling dimension (COMPUTE-SPEC §4.2): if any loaded live
    /// instance's ack p50 exceeds this, the fleet scales out even when
    /// rps alone wouldn't ask for it — a congested instance suppresses
    /// its own throughput signal (run-3 finding).
    #[arg(long, env = "SCALE_LATENCY_MS", default_value_t = 250)]
    scale_latency_ms: u64,

    /// The latency breach must persist this long before scaling (damps the
    /// transition-churn feedback observed in run 4).
    #[arg(long, env = "SCALE_LAT_SUSTAIN_SECS", default_value_t = 20)]
    scale_lat_sustain_secs: u64,

    /// Maximum fleet size (pilot: the four deployed services).
    #[arg(long, env = "FLEET_MAX", default_value_t = 4)]
    fleet_max: u64,
}

impl Args {
    fn raw_store(&self, bucket: &Option<String>) -> anyhow::Result<AmazonS3> {
        let bucket = bucket.as_deref().unwrap_or(&self.bucket);
        AmazonS3Builder::new()
            .with_endpoint(&self.s3_endpoint)
            .with_bucket_name(bucket)
            .with_region(&self.region)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
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
                    .with_pool_idle_timeout(Duration::from_secs(
                        std::env::var("POOL_IDLE_SECS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(4),
                    )),
            )
            // Records Tigris's Server-Timing (their internal ms) and
            // x-tigris-served-from per response → sp50/sp99 + served_from
            // in /v1/debug/store. wall − server = network path.
            .with_http_connector(store_timing::SniffConnector)
            .build()
            .context("build s3 object store")
    }

    // TimingStore sits beneath PrefixStore so it times final, fully-prefixed
    // paths (O14a split: our pipeline vs egress path vs Tigris). All stores
    // share one global gauge — the egress budget is per instance.
    fn store_for(&self, bucket: &Option<String>) -> anyhow::Result<Arc<dyn ObjectStore>> {
        let s3 = crate::store_timing::TimingStore::new(self.raw_store(bucket)?);
        Ok(match &self.path_prefix {
            Some(p) => Arc::new(object_store::prefix::PrefixStore::new(s3, p.as_str())),
            None => Arc::new(s3),
        })
    }

    /// Fleet-coordination store (heartbeats, desired.json): shared across
    /// instances, so prefixed by --fleet-prefix, not --path-prefix.
    fn fleet_store(&self) -> anyhow::Result<Option<Arc<dyn ObjectStore>>> {
        let Some(p) = &self.fleet_prefix else {
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
/// rolls) beside the bounded shard DBs. Env-derived so the value is
/// identical however the process is driven; clap args mirror the same
/// env vars for --help discoverability.
pub fn resolved_compactor_options() -> &'static slatedb::config::CompactorOptions {
    static CO: std::sync::OnceLock<slatedb::config::CompactorOptions> = std::sync::OnceLock::new();
    CO.get_or_init(|| {
        fn env_usize(k: &str, d: usize) -> usize {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        }
        fn env_u64(k: &str, d: u64) -> u64 {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        }
        let conc = env_usize("COMPACTOR_MAX_CONCURRENT", 4);
        let mut co = slatedb::config::CompactorOptions::default();
        co.poll_interval = Duration::from_millis(env_u64(
            "COMPACTOR_POLL_MS",
            crate::DEFAULT_COMPACTOR_POLL_MS,
        ));
        co.max_concurrent_compactions = conc;
        let mut w = co.worker.take().unwrap_or_default();
        w.max_concurrent_compactions = conc;
        w.max_subcompactions = env_usize("COMPACT_MAX_SUBCOMPACTIONS", 4);
        w.max_fetch_tasks = env_usize("COMPACT_MAX_FETCH_TASKS", 4);
        w.bytes_to_fetch = env_usize("COMPACT_BYTES_TO_FETCH", 2 * 1024 * 1024);
        w.max_sst_size = env_usize("COMPACT_MAX_SST_SIZE_BYTES", 256 * 1024 * 1024);
        co.worker = Some(w);
        co
    })
}

/// The resolved worker knobs as JSON (debug/load + startup log) and the
/// certification check: with MEMPROFILE_CERT=compute-1g the process
/// REFUSES to start unless the live resolved configuration matches the
/// certified survival profile — a deploy that drops one env var must
/// fail loudly at boot, not OOM at +28 minutes.
pub fn compactor_profile_json() -> serde_json::Value {
    let co = resolved_compactor_options();
    let w = co.worker.clone().unwrap_or_default();
    serde_json::json!({
        "max_concurrent_compactions": co.max_concurrent_compactions,
        "worker_max_concurrent_compactions": w.max_concurrent_compactions,
        "max_subcompactions": w.max_subcompactions,
        "max_fetch_tasks": w.max_fetch_tasks,
        "bytes_to_fetch": w.bytes_to_fetch,
        "max_sst_size": w.max_sst_size,
        "store_bulk_inflight_max_bytes": std::env::var("STORE_BULK_INFLIGHT_MAX_BYTES")
            .ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0),
    })
}

/// Every production Settings family and the worker options it will
/// hand its DB builder. R29 release blocker: the certification used to
/// validate only the env helper, while history_settings() passed
/// UPSTREAM defaults to every history partition — the process logged
/// "certified" with the exact unsafe profile running. Certification
/// (and the structural test) now inspects what the builders receive.
pub fn production_settings_families()
-> Vec<(&'static str, Option<slatedb::config::CompactorOptions>)> {
    vec![
        ("shard", Some(resolved_compactor_options().clone())),
        (
            "history_v1",
            crate::history::history_settings().compactor_options,
        ),
        (
            "history_v2",
            crate::history::history2_settings().compactor_options,
        ),
        (
            "telemetry",
            crate::billing::telemetry_settings().compactor_options,
        ),
    ]
}

fn assert_certified_memprofile() {
    if std::env::var("MEMPROFILE_CERT").as_deref() != Ok("compute-1g") {
        return;
    }
    let p = compactor_profile_json();
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
    let cert = resolved_compactor_options()
        .worker
        .clone()
        .unwrap_or_default();
    for (family, co) in production_settings_families() {
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
    /// Structural: every production settings family hands its builder
    /// the ONE resolved worker profile — a family that regresses to
    /// `Settings::default().compactor_options` (history, R29) or
    /// `..Default::default()` (telemetry, R28) fails here.
    #[test]
    fn every_db_family_carries_the_resolved_compactor_profile() {
        let cert = crate::resolved_compactor_options()
            .worker
            .clone()
            .unwrap_or_default();
        for (family, co) in crate::production_settings_families() {
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

pub fn slatedb_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        let threads: usize = std::env::var("SLATEDB_RT_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2);
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

fn shard_settings(args: &Args) -> Settings {
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
        compactor_options: Some(resolved_compactor_options().clone()),
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

fn validate_fleet_auth(args: &Args, fleet_mode: bool) -> anyhow::Result<()> {
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
            Args::try_parse_from(v).expect("test args must parse")
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

    #[test]
    fn shipped_defaults_are_a_valid_engine_configuration() {
        let args = Args::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]);
        validate_engine_settings("shard", &shard_settings(&args))
            .expect("default shard settings must open");
        validate_engine_settings("history", &crate::history::history_settings())
            .expect("default history settings must open");
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
        let mut args = Args::parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]);
        args.l0_sst_size_bytes = 32 * 1024 * 1024;
        args.max_unflushed_bytes = 16 * 1024 * 1024;
        let err = validate_engine_settings("shard", &shard_settings(&args))
            .expect_err("l0 above unflushed must be refused at startup");
        let msg = format!("{err}");
        assert!(msg.contains("max_unflushed_bytes"), "unhelpful: {msg}");
        assert!(msg.contains("L0_SST_SIZE_BYTES"), "no remedy named: {msg}");

        // Equality is just as fatal as inversion — SlateDB requires a
        // strict inequality.
        args.max_unflushed_bytes = args.l0_sst_size_bytes;
        validate_engine_settings("shard", &shard_settings(&args))
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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .init();
    // R28: a certified survival deploy must fail at boot, not OOM at
    // +28 min, if any memory knob was dropped or overridden.
    assert_certified_memprofile();
    // R28: SWEEP_MAINT_RESIDENT=0 would silently starve every cold
    // debt class (the rotation would open and immediately close each
    // indebted engine, so no absorber lives long enough to drain).
    if std::env::var("SWEEP_MAINT_RESIDENT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        == Some(0)
    {
        eprintln!(
            "Error: SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; \
             set >= 1 or unset (default 2)"
        );
        std::process::exit(1);
    }
    // Run 13: tokio timer drift of ~230 ms p50 (vs 4 ms for a raw thread)
    // proved the event loop is starved by inline blocking work. On a 1-vCPU
    // box #[tokio::main] means ONE worker — a single blocking poll freezes
    // every future, including durable-watermark acks (O14a). A worker floor
    // of 2+ lets the OS timeslice around a blocked worker.
    let workers: usize = std::env::var("TOKIO_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .max(2);
    tracing::info!("tokio runtime: {workers} worker threads");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let args = Args::parse();

    // FIRST: the body ceiling sizes the absorber's worst-frame
    // reservation, which floors the process-wide budget. It must be
    // fixed before anything reads either (CHAOS-3).
    crate::http::set_max_body_bytes(args.max_request_body_bytes)?;

    // Before anything opens a store: a configuration that SlateDB will
    // reject at open time must stop the process here, not turn into a
    // permanently-500 data plane behind an `ok` health check (CHAOS-2).
    // Both engine tiers go through the same check so a future edit to
    // either one cannot reintroduce the hole.
    validate_engine_settings("shard", &shard_settings(&args))?;
    validate_engine_settings("history", &crate::history::history_settings())?;

    let ops_store = args.store_for(&args.ops_bucket)?;
    let shard_store = args.store_for(&args.shard_bucket)?;
    let data_store = args.store_for(&args.data_bucket)?;

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
    let canary_prefix = args.path_prefix.clone().unwrap_or_default();
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
            args.instance_name.replace('/', "_"),
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
    crate::sharddir::spawn_unready_watchdog();

    let registry = Registry::new(ops_store.clone(), &args.cell_id);
    // MULTITENANCY transition posture: the deployment tenant is
    // EXPLICIT, validated config — layout-4 paths and hashes derive
    // from it, so an invalid or reserved value refuses boot loudly
    // instead of writing a mis-keyed namespace.
    let tenant = crate::tenant::ProjectId::new(&args.project_id)
        .unwrap_or_else(|e| panic!("PROJECT_ID {:?} is invalid: {e}", args.project_id));
    if tenant.is_system() {
        panic!("PROJECT_ID may not be the reserved system project");
    }
    // MULTITENANCY Stage 5: the auth service exists in every mode (Off
    // is inert); feeds are wired below once the runtime is up.
    let auth_mode = crate::auth::AuthMode::from_env(Some(args.streams_auth_mode.as_str()))?;
    if auth_mode != crate::auth::AuthMode::Off {
        // Review item: the local placeholder tenant must never reach a
        // shadow/enforce deployment — those are the multi-tenant
        // postures, and proj_local silently naming a real project's
        // data is exactly the accident this refuses.
        anyhow::ensure!(
            args.project_id != "proj_local",
            "STREAMS_AUTH_MODE={} requires an explicit non-default PROJECT_ID",
            args.streams_auth_mode
        );
        anyhow::ensure!(
            args.streams_auth_keys_file.is_some()
                && args.streams_auth_policy_file.is_some()
                && args.streams_auth_grants_file.is_some(),
            "STREAMS_AUTH_MODE={} requires STREAMS_AUTH_KEYS_FILE,              STREAMS_AUTH_POLICY_FILE and STREAMS_AUTH_GRANTS_FILE",
            args.streams_auth_mode
        );
        // §10.4: the denial journal drains through the system ledger
        // key. Without one, enforce still refuses correctly but the
        // journal is VOID — denials are only counted, never durably
        // recorded. Loud at boot so a preview cell cannot mistake
        // itself for an audited one.
        if args.usage_stream_key.is_none() {
            tracing::warn!(
                "STREAMS_AUTH_MODE={} without USAGE_STREAM_KEY: the _audit_events \
                 denial journal is DISABLED (denials appear only in \
                 audit_events_dropped_total)",
                args.streams_auth_mode
            );
        }
        // The refresher cadence must clear the staleness window with
        // room for a failed fetch or two, or the cell oscillates into
        // fail-closed refusals on schedule.
        anyhow::ensure!(
            (args.streams_auth_refresh_secs as i64) <= crate::auth::POLICY_STALENESS_MAX_SECS / 3,
            "STREAMS_AUTH_REFRESH_SECS={} must be <= {} (a third of the              {}s staleness window)",
            args.streams_auth_refresh_secs,
            crate::auth::POLICY_STALENESS_MAX_SECS / 3,
            crate::auth::POLICY_STALENESS_MAX_SECS
        );
    }
    let catalog_cursor_key: Option<[u8; 32]> = match &args.streams_cursor_key {
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
        args.streams_auth_issuer.clone(),
        &args.cell_id,
    )?);
    // Only relevant when no topology exists yet; an existing topology wins.
    let fleet_mode = args.fleet_prefix.is_some() && args.fleet_max > 1;
    // FAIL CLOSED (round-19 security): the /v1/internal/* peer surface
    // can fence consumer generations and read segment state WITHOUT a
    // stream key. It therefore needs its own credential, distinct from
    // the customer account token, and fleet mode must not start without
    // one — a fleet that silently accepted the public bearer on those
    // routes would let any customer token corrupt any consumer.
    validate_fleet_auth(&args, fleet_mode)?;
    let initial_shards = match args.initial_shards {
        Some(n) => {
            if fleet_mode && n < 4 * args.fleet_max as usize {
                tracing::warn!(
                    "INITIAL_SHARDS={n} < 4×FLEET_MAX={}: a fresh topology this coarse \
                     draws unevenly under rendezvous and the rebalancer flaps against \
                     return-home; use >= {}",
                    args.fleet_max,
                    (4 * args.fleet_max as usize).next_power_of_two()
                );
            }
            n
        }
        None if fleet_mode => (4 * args.fleet_max as usize).next_power_of_two(),
        None => 1,
    };
    let topology = load_or_init_topology(&ops_store, initial_shards, args.max_request_body_bytes)
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
    if stored_ceiling != args.max_request_body_bytes {
        anyhow::bail!(
            "MAX_REQUEST_BODY_BYTES is {} but this namespace was created with {} — \
             the ceiling sizes the absorber's worst-frame reservation, so changing it \
             on an existing namespace would under-reserve for records already written \
             (or silently move the published product limit). Set \
             MAX_REQUEST_BODY_BYTES={} to start against this namespace, or point \
             PATH_PREFIX at a fresh one.",
            args.max_request_body_bytes,
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
        let settings = shard_settings(&args);
        // §1.1: one block cache for the whole process, not one per DB
        // (SlateDB default: 512 MB PER DB — a 16-shard 1 GB instance dies
        // by cache fill; the run 6/8 zombie generator).
        let shared_cache: Arc<slatedb::db_cache::foyer::FoyerCache> =
            Arc::new(slatedb::db_cache::foyer::FoyerCache::new_with_opts(
                slatedb::db_cache::foyer::FoyerCacheOptions {
                    max_capacity: args.shared_cache_bytes,
                    ..Default::default()
                },
            ));
        let absorb_bytes = args.absorb_bytes;
        let absorb_age = args.absorb_age_secs;
        let absorb_pass_bytes = args.absorb_pass_bytes;
        let absorb_concurrency = args.absorb_concurrency;
        let absorb_pace_window_ms = args.absorb_pace_window_ms;
        let absorb_pace_ms = args.absorb_pace_ms;
        let absorb_read_par = args.absorb_read_par;
        let absorb_small_bytes = args.absorb_small_bytes;
        // Startup invariant (OOM disposition 2): the per-gather packing
        // cap must fit the process budget after the build multiplier,
        // or the envelope claim quietly breaks via reservation
        // clamping. Clamp the PACKING LIMIT (not the reservation) and
        // say so loudly.
        let absorb_gather_max_bytes = {
            let budget = crate::history::absorb_budget().capacity();
            let max_allowed = budget / crate::history::ABSORB_BUILD_MULTIPLIER;
            crate::history::RESOLVED_GATHER_PACKING_BYTES.store(
                crate::history::resolved_gather_packing_bytes(args.absorb_gather_max_bytes),
                std::sync::atomic::Ordering::Relaxed,
            );
            if args.absorb_gather_max_bytes > max_allowed {
                tracing::warn!(
                    "ABSORB_GATHER_MAX_BYTES {} x{} exceeds the process budget {} — \
                     clamping the gather packing limit to {}",
                    args.absorb_gather_max_bytes,
                    crate::history::ABSORB_BUILD_MULTIPLIER,
                    budget,
                    max_allowed,
                );
                max_allowed
            } else {
                args.absorb_gather_max_bytes
            }
        };
        let handle_idle_evict_secs = args.handle_idle_evict_secs;
        let handle_max_resident = args.handle_max_resident;
        let trim_per_op = args.trim_per_op;
        let trim_global_budget = args.trim_global_budget;
        let wal_group_commit = args.wal_group_commit != 0;
        let wal_flush_gap = Duration::from_millis(if args.wal_flush_gap_ms == 0 {
            args.flush_interval_ms
        } else {
            args.wal_flush_gap_ms
        });
        let wal_post_ack_gather = Duration::from_millis(args.wal_post_ack_gather_ms);
        let wal_gather_skip_reqs = if args.wal_gather_skip_reqs == 0 {
            u32::MAX
        } else {
            args.wal_gather_skip_reqs
        };
        let wal_gather_skip_bytes = if args.wal_gather_skip_bytes == 0 {
            u64::MAX
        } else {
            args.wal_gather_skip_bytes
        };
        let tail_ring_bytes = args.tail_ring_bytes;
        let state_slot = state_slot.clone();
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
                Box::pin(async move {
                    let path = crate::sharddir::shard_db_path(&prefix);
                    tracing::info!("opening shard log {path} (lazy; fences prior owner)");
                    let db = {
                        let p2 = path.clone();
                        crate::on_slatedb_rt(async move {
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

    let fleet_store_opt = args.fleet_store()?;
    let shards_map: std::sync::Arc<
        std::sync::RwLock<HashMap<String, Arc<crate::shard::ShardEngine>>>,
    > = std::sync::Arc::new(std::sync::RwLock::new(HashMap::new()));
    let gate = crate::sharddir::OpenGate::new(shards_map.clone(), opener.open);
    let state = Arc::new(AppState {
        registry,
        tenant,
        shard_prefixes: topology.shards.clone(),
        shards: shards_map,
        fleet_store: fleet_store_opt.clone(),
        gate,
        fleet_ops: std::sync::atomic::AtomicU64::new(0),
        inflight: std::sync::atomic::AtomicI64::new(0),
        inflight_peak: std::sync::atomic::AtomicI64::new(0),
        admit_max_inflight: args.admit_max_inflight,
        admit_rss_shed_mb: args.admit_rss_shed_mb,
        rss_mb_cached: std::sync::atomic::AtomicU64::new(0),
        admit_shed: std::sync::atomic::AtomicU64::new(0),
        admit_shed_inflight: std::sync::atomic::AtomicU64::new(0),
        admit_shed_rss: std::sync::atomic::AtomicU64::new(0),
        sse_max_connections: args.sse_max_connections,
        sse_connections: std::sync::atomic::AtomicU64::new(0),
        live_hubs: crate::livehub::HubRegistry::new(),
        sse_live_hub: std::sync::atomic::AtomicBool::new(args.sse_live_hub == 1),
        hub_total: crate::livehub::hub_total_global(),
        hub_total_cap: std::sync::atomic::AtomicU64::new(crate::livehub::hub_total_cap()),
        admit_max_inflight_per_stream: args.admit_max_inflight_per_stream,
        stream_inflight: std::sync::Mutex::new(HashMap::new()),
        stream_shed: std::sync::atomic::AtomicU64::new(0),
        wedge_shed: std::sync::atomic::AtomicU64::new(0),
        maint_latch: crate::backpressure::GlobalLatch::new(),
        sweep_sched: crate::billing::SweepSched::default(),
        instance_name: args.instance_name.clone(),
        ring_active: std::sync::RwLock::new(Vec::new()),
        ring_overrides: std::sync::RwLock::new(std::collections::HashMap::new()),
        peer_urls: std::sync::RwLock::new(std::collections::HashMap::new()),
        data_store,
        keys,
        touch,
        default_key: args.conformance_default_key.clone(),
        auth_token: args.auth_token.clone(),
        // Never empty: a standalone server still must be
        // distinguishable from the platform edge (round-19 MF4).
        origin_marker: if args.instance_name.is_empty() {
            format!("streams/{}", env!("CARGO_PKG_VERSION"))
        } else {
            args.instance_name.clone()
        },
        // SR3-1: the MODE determines the runtime credential state — in
        // workload mode the static token does not exist at runtime,
        // whatever the environment carried; in static mode no source
        // exists and relays use the bridge token.
        fleet_internal_token: if args.fleet_auth_mode == "workload" {
            None
        } else {
            args.fleet_internal_token.clone()
        },
        fleet_token_source: (args.fleet_auth_mode == "workload")
            .then_some(args.workload_token_file.as_ref())
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
        usage_key: args.usage_stream_key.clone(),
        rollup: std::sync::OnceLock::new(),
        read_spool: std::sync::OnceLock::new(),
        billing_reads: Arc::new(crate::billing::ReadUsageAccumulator::new(
            crate::billing::MeterSource {
                cell: args.cell_id.clone(),
                instance: args.instance_name.clone(),
                boot: crate::billing::boot_id().to_string(),
            },
        )),
        account_id: args.account_id.clone(),
        auth: auth_service.clone(),
        cell_id: args.cell_id.clone(),
        region: args.telemetry_region.clone(),
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
                args.streams_auth_keys_file.clone().unwrap(),
            )),
            Box::new(crate::auth_feed::FilePolicySource(
                args.streams_auth_policy_file.clone().unwrap(),
            )),
            Box::new(crate::auth_feed::FileGrantSource(
                args.streams_auth_grants_file.clone().unwrap(),
            )),
            std::time::Duration::from_secs(args.streams_auth_refresh_secs.max(1)),
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
        let shed_line_mb = args.admit_rss_shed_mb;
        let bp_limits = crate::backpressure::Limits::from_env();
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
                instance: args.instance_name.clone(),
                capacity_rps: args.scale_rps_capacity,
                edge_slots: args.scale_edge_slots,
                target_util: (args.scale_out_cpu_pct as f64 / 100.0).clamp(0.05, 0.95),
                scale_in_util: (args.scale_in_cpu_pct as f64 / 100.0).clamp(0.05, 0.90),
                hot_cpu_pct: args.scale_out_cpu_pct as f64,
                cpu_sustain: Duration::from_secs(args.scale_cpu_sustain_secs),
                scale_in: Duration::from_secs(args.scale_in_secs),
                latency_ms: args.scale_latency_ms,
                edge_latency_ms: args.scale_edge_latency_ms,
                latency_sustain: Duration::from_secs(args.scale_lat_sustain_secs),
                max: args.fleet_max,
            },
        );
        tracing::info!(
            "fleet coordination on (prefix={}, cap={} rps)",
            args.fleet_prefix.as_deref().unwrap_or(""),
            args.scale_rps_capacity
        );
    }
    // Telemetry pipeline (docs/OBSERVABILITY-BILLING.md): the drainer on
    // every instance; the rollup consumer where ROLLUP=1.
    if args.billing_mode == "required" {
        if args.usage_stream_key.is_none() {
            anyhow::bail!(
                "BILLING_MODE=required needs USAGE_STREAM_KEY — production \
                 billing refuses to run without the usage ledger (§14.1)"
            );
        }
        // Round-21: production billing must never silently attribute a
        // customer's traffic to the placeholder tenant.
        if args.account_id == "acct_local"
            || args.project_id == "proj_local"
            || args.cell_id == "local"
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
        if args.rollup == "1" {
            crate::billing::open_rollup(&state, &args.path_prefix.clone().unwrap_or_default())
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "BILLING_MODE=required: rollup DB must open before serving: {e}"
                    )
                })?;
        }
    }
    // ONE startup budget summary (OOM review): every fixed memory
    // bound in a single log line, plus a headroom warning when their
    // sum leaves less than 100 MiB below the shed line — posture
    // mistakes surface at boot, not at the kill line. Env reads mirror
    // each knob's own default.
    {
        let genv = |k: &str, d: usize| -> usize {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        };
        let shared = args.shared_cache_bytes as usize;
        let history = genv("HISTORY_CACHE_BYTES", 32 * 1024 * 1024);
        let postings = genv("POSTINGS_CACHE_BYTES", 64 * 1024 * 1024);
        let telemetry = genv("TELEMETRY_CACHE_BYTES", 16 * 1024 * 1024);
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
            crate::history::resolved_gather_packing_bytes(args.absorb_gather_max_bytes),
            std::sync::atomic::Ordering::Relaxed,
        );
        let per_gather = crate::history::per_gather_reservation_bytes();
        let effective_gathers = crate::history::effective_gather_concurrency();
        let rt_threads = genv("SLATEDB_RT_THREADS", 2);
        let mib = |b: usize| b / (1024 * 1024);
        tracing::info!(
            "memory budget: caches shared={}MiB history={}MiB postings={}MiB telemetry={}MiB; unflushed/db={}MiB; absorb budget={}MiB (worst-frame build={}MiB, per-gather reservation={}MiB, configured gather slots={}, EFFECTIVE gather concurrency={}); slatedb rt threads={}; shed line={}MB (RSS + reserved absorber bytes)",
            mib(shared),
            mib(history),
            mib(postings),
            mib(telemetry),
            mib(args.max_unflushed_bytes),
            mib(absorb_budget),
            mib(crate::history::absorb_worst_frame_transient()),
            mib(per_gather),
            gathers,
            effective_gathers,
            rt_threads,
            args.admit_rss_shed_mb,
        );
        let _ = crate::history::RESOLVED_MEMORY_CONFIG.set(serde_json::json!({
            "gatherPackingLimitBytes": args
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
            "maxUnflushedBytes": args.max_unflushed_bytes,
            "l0SstSizeBytes": args.l0_sst_size_bytes,
            "l0MaxSsts": args.l0_max_ssts,
            "shedLineMb": args.admit_rss_shed_mb,
        }));
        let fixed_mb = mib(shared + history + postings + telemetry + absorb_budget) as u64;
        if args.admit_rss_shed_mb > 0 && fixed_mb + 100 > args.admit_rss_shed_mb {
            tracing::warn!(
                "fixed memory budgets ({fixed_mb} MiB) leave <100 MiB below the shed line                  ({} MB) — this posture does not fit the instance class",
                args.admit_rss_shed_mb,
            );
        }
    }
    crate::billing::spawn_telemetry(state.clone());
    if args.rollup == "1" {
        crate::billing::spawn_rollup(state.clone(), args.path_prefix.clone().unwrap_or_default());
    }
    let app = http::router(state);

    crate::store_timing::spawn_sentinels();

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    tracing::info!("streams-slate listening on {}", args.listen);
    axum::serve(listener, app).await?;
    Ok(())
}
