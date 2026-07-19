mod audit;
mod auth;
mod cell_move_fence;
mod fleet;
mod history;
mod http;
mod merge;
mod metrics;
mod offsets;
mod operator;
mod queue;
mod reconfiguration;
mod shard;
mod split;
mod store_timing;
mod telemetry;
mod touch;
mod touch_keys;

use streams_slate::{cells, crypto, registry};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

// musl's allocator fragments badly under this workload (docker phase 1:
// RSS 2x the accounted budgets); mimalloc keeps RSS near actual live set.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::Context;
use clap::Parser;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use slatedb::Db;
use slatedb::config::Settings;

use crate::history::{
    Absorber, AbsorberConfig, AbsorberStartup, HistoryBlockWriteFormat, KeyCache, absorber_channel,
};
use crate::http::AppState;
use crate::registry::{Registry, load_or_init_topology};
use crate::shard::{ShardConfig, ShardEngine};

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

    /// Optional independent provider/bucket for immutable recovery snapshots.
    #[arg(long, env = "BACKUP_S3_ENDPOINT")]
    backup_s3_endpoint: Option<String>,
    #[arg(long, env = "BACKUP_S3_BUCKET")]
    backup_s3_bucket: Option<String>,
    #[arg(long, env = "BACKUP_S3_REGION", default_value = "us-east-1")]
    backup_s3_region: String,
    #[arg(long, env = "BACKUP_S3_ACCESS_KEY_ID")]
    backup_s3_access_key_id: Option<String>,
    #[arg(long, env = "BACKUP_S3_SECRET_ACCESS_KEY")]
    backup_s3_secret_access_key: Option<String>,
    #[arg(long, env = "BACKUP_PATH_PREFIX")]
    backup_path_prefix: Option<String>,
    #[arg(long, env = "BACKUP_INTERVAL_SECS", default_value_t = 300)]
    backup_interval_secs: u64,
    /// Maximum acceptable age of the newest fully protected recovery point.
    #[arg(long, env = "BACKUP_RPO_BUDGET_SECS", default_value_t = 300)]
    backup_rpo_budget_secs: u64,
    /// Complete recovery points older than this are removed; unreferenced
    /// content blobs and abandoned partial generations are reclaimed with it.
    #[arg(long, env = "BACKUP_RETENTION_SECS", default_value_t = 7 * 24 * 60 * 60)]
    backup_retention_secs: u64,
    /// Expiry safety net for the SlateDB checkpoints that pin one backup's
    /// manifest/SST set. Successful runs delete them eagerly.
    #[arg(
        long,
        env = "BACKUP_CHECKPOINT_LIFETIME_SECS",
        default_value_t = 60 * 60
    )]
    backup_checkpoint_lifetime_secs: u64,
    #[arg(long, env = "BACKUP_SCRUB_INTERVAL_SECS", default_value_t = 60)]
    backup_scrub_interval_secs: u64,
    #[arg(long, env = "BACKUP_SCRUB_OBJECTS_PER_INTERVAL", default_value_t = 256)]
    backup_scrub_objects_per_interval: usize,
    /// Continuously parse live manifests and decode every referenced SlateDB
    /// SST/WAL through its checksummed reader. This is separate from recovery
    /// corpus scrubbing and shares the cell's fenced backup coordinator.
    #[arg(long, env = "PRIMARY_SCRUB_INTERVAL_SECS", default_value_t = 60)]
    primary_scrub_interval_secs: u64,
    #[arg(long, env = "PRIMARY_SCRUB_OBJECTS_PER_INTERVAL", default_value_t = 16)]
    primary_scrub_objects_per_interval: usize,
    #[arg(
        long,
        env = "PRIMARY_SCRUB_MAX_OBJECT_BYTES",
        default_value_t = 256 * 1024 * 1024
    )]
    primary_scrub_max_object_bytes: u64,
    /// Recovery-corpus write format. Use 2 for the read-first migration wave;
    /// flip to 3 only after every backup/restore binary reads format 3.
    #[arg(long, env = "BACKUP_WRITE_FORMAT", default_value_t = 3)]
    backup_write_format: u32,
    /// Release-mode guard: fail startup unless backup is configured.
    #[arg(long, env = "REQUIRE_BACKUP", default_value_t = false)]
    require_backup: bool,

    /// Independent incident/audit provider. Control events are acknowledged
    /// only after immutable writes to both the primary ops store and this
    /// mirror; sampled batches retry each side independently.
    #[arg(long, env = "AUDIT_MIRROR_S3_ENDPOINT")]
    audit_mirror_s3_endpoint: Option<String>,
    #[arg(long, env = "AUDIT_MIRROR_S3_BUCKET")]
    audit_mirror_s3_bucket: Option<String>,
    #[arg(long, env = "AUDIT_MIRROR_S3_REGION", default_value = "us-east-1")]
    audit_mirror_s3_region: String,
    #[arg(long, env = "AUDIT_MIRROR_S3_ACCESS_KEY_ID")]
    audit_mirror_s3_access_key_id: Option<String>,
    #[arg(long, env = "AUDIT_MIRROR_S3_SECRET_ACCESS_KEY")]
    audit_mirror_s3_secret_access_key: Option<String>,
    #[arg(long, env = "AUDIT_MIRROR_PATH_PREFIX")]
    audit_mirror_path_prefix: Option<String>,
    #[arg(long, env = "AUDIT_MIRROR_S3_ALLOW_HTTP", default_value_t = false)]
    audit_mirror_s3_allow_http: bool,
    #[arg(long, env = "REQUIRE_AUDIT_MIRROR", default_value_t = false)]
    require_audit_mirror: bool,
    #[arg(long, env = "AUDIT_SAMPLE_DENOMINATOR", default_value_t = 100)]
    audit_sample_denominator: u32,
    /// Full-fidelity read-only operator events share an immutable object for
    /// this long unless 256 events fill it first.
    #[arg(long, env = "AUDIT_OPERATOR_BATCH_INTERVAL_SECS", default_value_t = 60)]
    audit_operator_batch_interval_secs: u64,
    #[arg(
        long,
        env = "AUDIT_PRIMARY_RETENTION_SECS",
        default_value_t = 30 * 24 * 60 * 60
    )]
    audit_primary_retention_secs: u64,
    #[arg(
        long,
        env = "AUDIT_MIRROR_RETENTION_SECS",
        default_value_t = 365 * 24 * 60 * 60
    )]
    audit_mirror_retention_secs: u64,
    #[arg(long, env = "AUDIT_MAINTENANCE_INTERVAL_SECS", default_value_t = 300)]
    audit_maintenance_interval_secs: u64,
    #[arg(
        long,
        env = "AUDIT_MAINTENANCE_OBJECTS_PER_INTERVAL",
        default_value_t = 1_000
    )]
    audit_maintenance_objects_per_interval: usize,
    #[arg(
        long,
        env = "AUDIT_MAINTENANCE_MAX_OBJECT_BYTES",
        default_value_t = 8 * 1024 * 1024
    )]
    audit_maintenance_max_object_bytes: u64,

    #[arg(long, env = "SLATE_S3_REGION", default_value = "us-east-1")]
    region: String,
    #[arg(long, env = "SLATE_S3_ACCESS_KEY_ID", default_value = "test")]
    access_key_id: String,
    #[arg(long, env = "SLATE_S3_SECRET_ACCESS_KEY", default_value = "test")]
    secret_access_key: String,

    /// Overall object-store request timeout. Timeout errors are retried by the
    /// object-store client; the durable watermark cannot advance on timeout.
    #[arg(long, env = "S3_REQUEST_TIMEOUT_MS", default_value_t = 30_000)]
    s3_request_timeout_ms: u64,

    /// Initial shard count (power of two) if no topology exists yet (D3).
    #[arg(long, env = "INITIAL_SHARDS", default_value_t = 1)]
    initial_shards: usize,

    /// Calibrated sustained write ceiling for one shard on this deployment.
    /// When non-zero, an owner automatically splits a shard after it remains
    /// above 60% of this payload-byte rate for auto_split_sustain_secs.
    /// Zero is the explicit operator override that disables automatic split.
    #[arg(
        long,
        env = "SINGLE_SHARD_WRITE_CEILING_BYTES_PER_SEC",
        default_value_t = 0
    )]
    single_shard_write_ceiling_bytes_per_sec: u64,
    #[arg(long, env = "AUTO_SPLIT_SUSTAIN_SECS", default_value_t = 60)]
    auto_split_sustain_secs: u64,

    /// Merge sibling shards after their combined write rate remains at or
    /// below this percentage of the calibrated single-shard ceiling. The
    /// maximum of 20 keeps at least 3x hysteresis below split's 60% trigger;
    /// zero explicitly disables automatic merge.
    #[arg(long, env = "AUTO_MERGE_COLD_FRACTION_PCT", default_value_t = 10)]
    auto_merge_cold_fraction_pct: u64,
    #[arg(long, env = "AUTO_MERGE_SUSTAIN_SECS", default_value_t = 600)]
    auto_merge_sustain_secs: u64,

    /// Unreferenced `shards/splits/<operation>/` generations are retained
    /// this long before deletion. The topology and every active split intent
    /// are re-read immediately before GC. Zero is test-only.
    #[arg(long, env = "SPLIT_GC_RETENTION_SECS", default_value_t = 86_400)]
    split_gc_retention_secs: u64,
    #[arg(long, env = "SPLIT_GC_INTERVAL_SECS", default_value_t = 300)]
    split_gc_interval_secs: u64,

    /// Shard-log WAL flush interval (D22, amended). 5 ms minted WAL SSTs
    /// ~7× faster than SlateDB's WAL GC reaps them; the growing backlog
    /// degraded the per-DB durable watermark to ~0.3–1 s (EXPERIMENT-PILOT
    /// run 3). 25 ms keeps the ack floor ≈ flush + Tigris PUT ≈ 40 ms while
    /// cutting WAL-object churn 5×.
    #[arg(long, env = "FLUSH_INTERVAL_MS", default_value_t = 25)]
    flush_interval_ms: u64,

    #[arg(long, env = "L0_SST_SIZE_BYTES", default_value_t = 32 * 1024 * 1024)]
    l0_sst_size_bytes: usize,

    /// Byte-backpressure cap per shard DB (§1.1). SlateDB's default is
    /// 512 MB — a byte-flood on a 1 GB instance OOMs before any request
    /// backpressure fires (bench finding, 2026-07-14).
    #[arg(long, env = "MAX_UNFLUSHED_BYTES", default_value_t = 16 * 1024 * 1024)]
    max_unflushed_bytes: usize,

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
    #[arg(long, env = "WAL_GC_INTERVAL_SECS", default_value_t = 30)]
    wal_gc_interval_secs: u64,

    /// Minimum WAL SST age before GC may delete it (seconds). Must cover
    /// the reopen/replay window (shard moves replay < ~1 s; 60 s is a
    /// generous safety factor at 5x fewer retained objects than the
    /// 300 s upstream default).
    #[arg(long, env = "WAL_GC_MIN_AGE_SECS", default_value_t = 60)]
    wal_gc_min_age_secs: u64,

    /// Manifest poll cadence (ms). This is ALSO how the memtable flusher
    /// learns that compaction freed L0 slots: with a long poll, dispatch
    /// stays gated on a stale L0 view for the whole interval while imm
    /// memtables pile into backpressure (bench finding 2026-07-14: 60 s
    /// poll → 14 s flush stalls). Idle-shard poll cost is ~1 GET per
    /// interval; loaded shards need this at 1-2 s.
    #[arg(long, env = "MANIFEST_POLL_MS", default_value_t = 2000)]
    manifest_poll_ms: u64,

    /// Hot-log records deleted per Absorbed commit op. Trim must keep pace
    /// with ingest in steady state: at 50k records/s and ~1 absorb pass
    /// per 5 s, the pass has to retire ~250k records or the hot DB grows
    /// without bound. Tombstones are ~30 B, so even the high setting is a
    /// few MB per batch.
    #[arg(long, env = "TRIM_PER_OP", default_value_t = 8_192)]
    trim_per_op: u64,

    /// Plaintext bytes buffered per absorber pass (absorb_one holds a pass
    /// in memory; cap it well below the instance's RAM).
    #[arg(long, env = "ABSORB_PASS_BYTES", default_value_t = 256 * 1024 * 1024)]
    absorb_pass_bytes: u64,

    /// Absorber thresholds (§3.6 / D23).
    #[arg(long, env = "ABSORB_BYTES", default_value_t = 4 * 1024 * 1024)]
    absorb_bytes: u64,
    #[arg(long, env = "ABSORB_AGE_SECS", default_value_t = 300)]
    absorb_age_secs: u64,

    /// History block envelope writer. Readers always accept legacy v1 and
    /// incarnation-bound v2. Existing cells use 1 for the read-first wave,
    /// then flip to 2 after every serving binary has the v2 reader.
    #[arg(long, env = "HISTORY_BLOCK_WRITE_FORMAT", default_value_t = 2)]
    history_block_write_format: u8,

    /// Conformance/dev only: use this stream key (base64url, 32 bytes) for
    /// requests that carry no Stream-Encryption-Key header. The upstream
    /// conformance suite cannot send custom headers.
    #[arg(long)]
    conformance_default_key: Option<String>,

    /// Conformance accommodation: make headerless creates per-key with this
    /// many segments (power of two).
    #[arg(long)]
    conformance_ordering_segments: Option<u32>,

    /// Pilot compatibility token. Without JWKS this authenticates one legacy
    /// tenant; with JWKS it is accepted only on operator/debug endpoints.
    #[arg(long, env = "AUTH_TOKEN")]
    auth_token: Option<String>,

    /// Production authentication: asymmetric JWT verification keys. URL,
    /// issuer, and audience must be configured together.
    #[arg(long, env = "AUTH_JWKS_URL")]
    auth_jwks_url: Option<String>,
    /// Monotonic token-id denylist document:
    /// {"version":N,"revoked_token_ids":[...]}. Required in production.
    #[arg(long, env = "AUTH_REVOCATION_URL")]
    auth_revocation_url: Option<String>,
    #[arg(long, env = "AUTH_ISSUER")]
    auth_issuer: Option<String>,
    #[arg(long, env = "AUTH_AUDIENCE")]
    auth_audience: Option<String>,
    #[arg(long, env = "AUTH_JWKS_REFRESH_SECS", default_value_t = 600)]
    auth_jwks_refresh_secs: u64,
    #[arg(long, env = "AUTH_JWKS_MAX_STALE_SECS", default_value_t = 3600)]
    auth_jwks_max_stale_secs: u64,
    #[arg(long, env = "AUTH_REVOCATION_REFRESH_SECS", default_value_t = 60)]
    auth_revocation_refresh_secs: u64,
    #[arg(long, env = "AUTH_REVOCATION_MAX_STALE_SECS", default_value_t = 120)]
    auth_revocation_max_stale_secs: u64,

    /// Explicit development escape hatch. Production boot fails when no
    /// authentication mode is configured.
    #[arg(long, env = "ALLOW_INSECURE_NO_AUTH", default_value_t = false)]
    allow_insecure_no_auth: bool,

    /// Enable the internal `__metrics__` stream, encrypted with this key.
    #[arg(long, env = "METRICS_KEY")]
    metrics_key: Option<String>,

    /// Router URL for metrics appends: billing records are routed like any
    /// tenant write so the shard's ring owner serves them (no fence-fights).
    #[arg(long, env = "METRICS_LB_URL")]
    metrics_lb_url: Option<String>,
    /// Scoped service JWT granting create/append for __metrics__. In pilot
    /// mode AUTH_TOKEN is used when this is unset.
    #[arg(long, env = "METRICS_AUTH_TOKEN")]
    metrics_auth_token: Option<String>,
    /// Customer/sub claim of the scoped metrics service principal. Only this
    /// exact customer's `__metrics__` stream is excluded from self-metering.
    #[arg(long, env = "METRICS_CUSTOMER_ID")]
    metrics_customer_id: Option<String>,
    #[arg(long, env = "METRICS_EXPORT_INTERVAL_SECS", default_value_t = 15)]
    metrics_export_interval_secs: u64,
    #[arg(long, env = "REQUIRE_METRICS_EXPORT", default_value_t = false)]
    require_metrics_export: bool,

    /// Instance tag recorded in metrics records.
    #[arg(long, env = "INSTANCE_NAME", default_value = "streams")]
    instance_name: String,

    /// Key prefix inside the bucket(s): lets independent deployments share
    /// one bucket.
    #[arg(long, env = "PATH_PREFIX")]
    path_prefix: Option<String>,

    /// Managed cell identity. Enabling this mode requires PATH_PREFIX to be
    /// exactly cells/<id> and a separately credentialed global registry.
    #[arg(long, env = "CELL_ID")]
    cell_id: Option<String>,
    #[arg(long, env = "CELL_DIRECTORY_REFRESH_SECS", default_value_t = 60)]
    cell_directory_refresh_secs: u64,

    /// Global stream registry + cells.json. In legacy single-cell mode these
    /// default to the ordinary ops store. Managed cells require explicit,
    /// separately scoped credentials and a non-cell prefix.
    #[arg(long, env = "REGISTRY_S3_ENDPOINT")]
    registry_s3_endpoint: Option<String>,
    #[arg(long, env = "REGISTRY_S3_BUCKET")]
    registry_s3_bucket: Option<String>,
    #[arg(long, env = "REGISTRY_S3_REGION", default_value = "us-east-1")]
    registry_s3_region: String,
    #[arg(long, env = "REGISTRY_S3_ACCESS_KEY_ID")]
    registry_s3_access_key_id: Option<String>,
    #[arg(long, env = "REGISTRY_S3_SECRET_ACCESS_KEY")]
    registry_s3_secret_access_key: Option<String>,
    #[arg(long, env = "REGISTRY_S3_ALLOW_HTTP", default_value_t = false)]
    registry_s3_allow_http: bool,
    #[arg(long, env = "REGISTRY_PATH_PREFIX")]
    registry_path_prefix: Option<String>,

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
    /// full throughput. Set ~78 %% of instance RAM (spec §1.1 alarm).
    #[arg(long, env = "ADMIT_RSS_SHED_MB", default_value_t = 0)]
    admit_rss_shed_mb: u64,

    /// §12-lite admission backstop: shed /v1/stream requests with 429 +
    /// Retry-After beyond this many in flight (0 = off). Protects the
    /// durable path from queue collapse when offered load exceeds
    /// capacity; pairs with closed-loop clients honoring Retry-After.
    #[arg(long, env = "ADMIT_MAX_INFLIGHT", default_value_t = 0)]
    admit_max_inflight: i64,

    /// Hard per-customer concurrency share. Unlike the instance backstop,
    /// this remains enabled by default so one tenant cannot occupy every
    /// long-poll and write slot. 0 disables it for local benchmarking only.
    #[arg(long, env = "ADMIT_MAX_INFLIGHT_PER_CUSTOMER", default_value_t = 64)]
    admit_max_inflight_per_customer: usize,

    /// Per-customer ingress write-byte token rate and burst. These defaults
    /// bound noisy-neighbor memory/WAL pressure while remaining above the
    /// documented 50 MB/s hot-stream product limit. 0 rate disables.
    #[arg(
        long,
        env = "ADMIT_WRITE_BYTES_PER_SEC_PER_CUSTOMER",
        default_value_t = 64 * 1024 * 1024
    )]
    admit_write_bytes_per_sec_per_customer: u64,
    #[arg(
        long,
        env = "ADMIT_WRITE_BURST_BYTES_PER_CUSTOMER",
        default_value_t = 128 * 1024 * 1024
    )]
    admit_write_burst_bytes_per_customer: u64,

    /// Maximum concurrent long-poll, SSE, and queue-receive connections per
    /// customer. This is separate from the all-request in-flight ceiling.
    #[arg(
        long,
        env = "ADMIT_MAX_LIVE_CONNECTIONS_PER_CUSTOMER",
        default_value_t = 32
    )]
    admit_max_live_connections_per_customer: usize,

    /// Per-customer append request token rate and burst. A zero rate disables
    /// request-count admission while leaving the byte quota active.
    #[arg(
        long,
        env = "ADMIT_APPEND_REQUESTS_PER_SEC_PER_CUSTOMER",
        default_value_t = 10_000
    )]
    admit_append_requests_per_sec_per_customer: u64,
    #[arg(
        long,
        env = "ADMIT_APPEND_REQUEST_BURST_PER_CUSTOMER",
        default_value_t = 10_000
    )]
    admit_append_request_burst_per_customer: u64,

    /// Per-customer read request and egress-byte buckets. Response bytes are
    /// paced, including SSE, so an admitted 200 response is never torn down
    /// mid-stream merely because its next frame crosses the rate.
    #[arg(
        long,
        env = "ADMIT_READ_REQUESTS_PER_SEC_PER_CUSTOMER",
        default_value_t = 10_000
    )]
    admit_read_requests_per_sec_per_customer: u64,
    #[arg(
        long,
        env = "ADMIT_READ_REQUEST_BURST_PER_CUSTOMER",
        default_value_t = 10_000
    )]
    admit_read_request_burst_per_customer: u64,
    #[arg(
        long,
        env = "ADMIT_READ_BYTES_PER_SEC_PER_CUSTOMER",
        default_value_t = 128 * 1024 * 1024
    )]
    admit_read_bytes_per_sec_per_customer: u64,
    #[arg(
        long,
        env = "ADMIT_READ_BURST_BYTES_PER_CUSTOMER",
        default_value_t = 256 * 1024 * 1024
    )]
    admit_read_burst_bytes_per_customer: u64,

    /// Per-customer queue receive request bucket. Settlement calls do not
    /// consume this dimension.
    #[arg(
        long,
        env = "ADMIT_QUEUE_RECEIVES_PER_SEC_PER_CUSTOMER",
        default_value_t = 5_000
    )]
    admit_queue_receives_per_sec_per_customer: u64,
    #[arg(
        long,
        env = "ADMIT_QUEUE_RECEIVE_BURST_PER_CUSTOMER",
        default_value_t = 5_000
    )]
    admit_queue_receive_burst_per_customer: u64,

    /// Default provisioned append request/byte limits for one stream. Create
    /// headers can persist lower or higher values within the validated caps.
    #[arg(
        long,
        env = "ADMIT_APPEND_REQUESTS_PER_SEC_PER_STREAM",
        default_value_t = 5_000
    )]
    admit_append_requests_per_sec_per_stream: u64,
    #[arg(
        long,
        env = "ADMIT_APPEND_REQUEST_BURST_PER_STREAM",
        default_value_t = 5_000
    )]
    admit_append_request_burst_per_stream: u64,
    #[arg(
        long,
        env = "ADMIT_WRITE_BYTES_PER_SEC_PER_STREAM",
        default_value_t = 50 * 1024 * 1024
    )]
    admit_write_bytes_per_sec_per_stream: u64,
    #[arg(
        long,
        env = "ADMIT_WRITE_BURST_BYTES_PER_STREAM",
        default_value_t = 100 * 1024 * 1024
    )]
    admit_write_burst_bytes_per_stream: u64,

    /// Default relative commit share among streams belonging to one customer.
    /// The outer tenant round-robin remains equal regardless of this value.
    #[arg(long, env = "STREAM_COMMIT_WEIGHT", default_value_t = 1)]
    stream_commit_weight: u16,

    /// Measured per-instance ingress-concurrency capacity through the
    /// platform front door. Two-layer model (platform team investigation
    /// + our 6-source confirmation, 2026-07-15): each SOURCE Compute
    ///   instance is egress-capped at ~48-50 outgoing requests; the
    ///   DESTINATION front door admits ~145-150 concurrent aggregate (the
    ///   earlier 48 calibration was the measuring instance's own egress
    ///   cap). Scale-out begins at scale_out_cpu_pct% of this. 0 disables.
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
        anyhow::ensure!(
            (50..=300_000).contains(&self.s3_request_timeout_ms),
            "S3_REQUEST_TIMEOUT_MS must be between 50 and 300000"
        );
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
            // pool instead of dead sockets (EXPERIMENT-PILOT.md).
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(true) // ClientOptions REPLACES the builder's allow_http
                    .with_timeout(Duration::from_millis(self.s3_request_timeout_ms))
                    .with_pool_idle_timeout(Duration::from_secs(4)),
            )
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

    fn registry_store(&self) -> anyhow::Result<Arc<dyn ObjectStore>> {
        let store: Arc<dyn ObjectStore> = match &self.registry_s3_endpoint {
            Some(endpoint) => {
                let bucket = self
                    .registry_s3_bucket
                    .as_deref()
                    .context("REGISTRY_S3_BUCKET is required with REGISTRY_S3_ENDPOINT")?;
                let access_key = self
                    .registry_s3_access_key_id
                    .as_deref()
                    .context("REGISTRY_S3_ACCESS_KEY_ID is required")?;
                let secret_key = self
                    .registry_s3_secret_access_key
                    .as_deref()
                    .context("REGISTRY_S3_SECRET_ACCESS_KEY is required")?;
                Arc::new(crate::store_timing::TimingStore::new(
                    AmazonS3Builder::new()
                        .with_endpoint(endpoint)
                        .with_bucket_name(bucket)
                        .with_region(&self.registry_s3_region)
                        .with_access_key_id(access_key)
                        .with_secret_access_key(secret_key)
                        .with_allow_http(self.registry_s3_allow_http)
                        .with_conditional_put(S3ConditionalPut::ETagMatch)
                        .with_client_options(
                            object_store::ClientOptions::new()
                                .with_allow_http(self.registry_s3_allow_http)
                                .with_timeout(Duration::from_millis(self.s3_request_timeout_ms))
                                .with_pool_idle_timeout(Duration::from_secs(4)),
                        )
                        .build()
                        .context("build registry object store")?,
                ))
            }
            None => {
                anyhow::ensure!(
                    self.registry_s3_bucket.is_none()
                        && self.registry_s3_access_key_id.is_none()
                        && self.registry_s3_secret_access_key.is_none(),
                    "REGISTRY_S3_ENDPOINT is required when registry credentials are configured"
                );
                Arc::new(crate::store_timing::TimingStore::new(
                    self.raw_store(&self.ops_bucket)?,
                ))
            }
        };
        Ok(match &self.registry_path_prefix {
            Some(prefix) => Arc::new(object_store::prefix::PrefixStore::new(
                store,
                prefix.as_str(),
            )),
            None if self.registry_s3_endpoint.is_none() => return self.store_for(&self.ops_bucket),
            None => store,
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

    fn backup_store(&self) -> anyhow::Result<Option<Arc<dyn ObjectStore>>> {
        let Some(endpoint) = &self.backup_s3_endpoint else {
            anyhow::ensure!(
                self.backup_s3_bucket.is_none()
                    && self.backup_s3_access_key_id.is_none()
                    && self.backup_s3_secret_access_key.is_none()
                    && self.backup_path_prefix.is_none(),
                "BACKUP_S3_ENDPOINT is required when any backup setting is configured"
            );
            return Ok(None);
        };
        let bucket = self
            .backup_s3_bucket
            .as_deref()
            .context("BACKUP_S3_BUCKET is required with BACKUP_S3_ENDPOINT")?;
        let access_key = self
            .backup_s3_access_key_id
            .as_deref()
            .context("BACKUP_S3_ACCESS_KEY_ID is required")?;
        let secret_key = self
            .backup_s3_secret_access_key
            .as_deref()
            .context("BACKUP_S3_SECRET_ACCESS_KEY is required")?;
        let primary_buckets = [
            self.ops_bucket.as_deref().unwrap_or(&self.bucket),
            self.shard_bucket.as_deref().unwrap_or(&self.bucket),
            self.data_bucket.as_deref().unwrap_or(&self.bucket),
        ];
        anyhow::ensure!(
            endpoint.trim_end_matches('/') != self.s3_endpoint.trim_end_matches('/')
                || !primary_buckets.contains(&bucket),
            "backup destination must not be a primary source bucket"
        );
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(&self.backup_s3_region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(true)
                    .with_timeout(Duration::from_millis(self.s3_request_timeout_ms))
                    .with_pool_idle_timeout(Duration::from_secs(4)),
            )
            .build()
            .context("build backup object store")?;
        let store: Arc<dyn ObjectStore> = match &self.backup_path_prefix {
            Some(prefix) => Arc::new(object_store::prefix::PrefixStore::new(
                store,
                prefix.as_str(),
            )),
            None => Arc::new(store),
        };
        Ok(Some(store))
    }

    fn audit_mirror_store(&self) -> anyhow::Result<Option<Arc<dyn ObjectStore>>> {
        let Some(endpoint) = &self.audit_mirror_s3_endpoint else {
            anyhow::ensure!(
                self.audit_mirror_s3_bucket.is_none()
                    && self.audit_mirror_s3_access_key_id.is_none()
                    && self.audit_mirror_s3_secret_access_key.is_none()
                    && self.audit_mirror_path_prefix.is_none(),
                "AUDIT_MIRROR_S3_ENDPOINT is required when any audit mirror setting is configured"
            );
            return Ok(None);
        };
        let bucket = self
            .audit_mirror_s3_bucket
            .as_deref()
            .context("AUDIT_MIRROR_S3_BUCKET is required")?;
        let access_key = self
            .audit_mirror_s3_access_key_id
            .as_deref()
            .context("AUDIT_MIRROR_S3_ACCESS_KEY_ID is required")?;
        let secret_key = self
            .audit_mirror_s3_secret_access_key
            .as_deref()
            .context("AUDIT_MIRROR_S3_SECRET_ACCESS_KEY is required")?;
        let ops_bucket = self.ops_bucket.as_deref().unwrap_or(&self.bucket);
        anyhow::ensure!(
            endpoint.trim_end_matches('/') != self.s3_endpoint.trim_end_matches('/')
                || bucket != ops_bucket,
            "audit mirror must not be the primary ops bucket"
        );
        anyhow::ensure!(
            access_key != self.access_key_id,
            "audit mirror must use credentials independent from the primary store"
        );
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(&self.audit_mirror_s3_region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(self.audit_mirror_s3_allow_http)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(self.audit_mirror_s3_allow_http)
                    .with_timeout(Duration::from_millis(self.s3_request_timeout_ms))
                    .with_pool_idle_timeout(Duration::from_secs(4)),
            )
            .build()
            .context("build audit mirror object store")?;
        let store: Arc<dyn ObjectStore> = match &self.audit_mirror_path_prefix {
            Some(prefix) => Arc::new(object_store::prefix::PrefixStore::new(
                store,
                prefix.as_str(),
            )),
            None => Arc::new(store),
        };
        Ok(Some(store))
    }
}

fn shard_settings(args: &Args) -> Settings {
    Settings {
        flush_interval: Some(Duration::from_millis(args.flush_interval_ms)),
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
        garbage_collector_options: {
            let mut gc = Settings::default()
                .garbage_collector_options
                .unwrap_or_default();
            gc.wal_options = Some(slatedb::config::GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(args.wal_gc_interval_secs)),
                min_age: Duration::from_secs(args.wal_gc_min_age_secs),
                ..gc.wal_options.unwrap_or_default()
            });
            Some(gc)
        },
        ..Default::default()
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .init();
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

fn start_topology_watcher(state: Arc<AppState>, store: Arc<dyn ObjectStore>) {
    tokio::spawn(async move {
        let mut last_good = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match crate::registry::load_topology(&store).await {
                Ok(topology) => {
                    let current = state
                        .topology_version
                        .load(std::sync::atomic::Ordering::Acquire);
                    if topology.version < current {
                        tracing::error!(
                            current,
                            observed = topology.version,
                            "topology version regressed; retaining last-known-good trie"
                        );
                        state
                            .topology_ready
                            .store(false, std::sync::atomic::Ordering::Release);
                        continue;
                    }
                    if topology.version > current {
                        let live: std::collections::HashSet<&str> =
                            topology.shards.iter().map(String::as_str).collect();
                        *state.topology.write().unwrap() = topology.clone();
                        state
                            .topology_version
                            .store(topology.version, std::sync::atomic::Ordering::Release);
                        let retired: Vec<Arc<ShardEngine>> = {
                            let mut shards = state.shards.write().unwrap();
                            let retired_prefixes: Vec<_> = shards
                                .keys()
                                .filter(|prefix| !live.contains(prefix.as_str()))
                                .cloned()
                                .collect();
                            retired_prefixes
                                .into_iter()
                                .filter_map(|prefix| shards.remove(&prefix))
                                .collect()
                        };
                        for engine in retired {
                            engine.retire();
                        }
                        tracing::info!(
                            version = topology.version,
                            shards = topology.shards.len(),
                            "installed topology"
                        );
                    }
                    last_good = std::time::Instant::now();
                    state
                        .topology_ready
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                Err(error) => {
                    tracing::warn!("topology refresh failed: {error}");
                    if last_good.elapsed() > Duration::from_secs(30) {
                        state
                            .topology_ready
                            .store(false, std::sync::atomic::Ordering::Release);
                    }
                }
            }
        }
    });
}

fn start_cell_directory_watcher(
    state: Arc<AppState>,
    store: Arc<dyn ObjectStore>,
    refresh: Duration,
) {
    let Some(cell_id) = state.cell_id.clone() else {
        return;
    };
    tokio::spawn(async move {
        let mut last_good = std::time::Instant::now();
        let mut tick = tokio::time::interval(refresh);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Startup already loaded and validated generation 1+.
        tick.tick().await;
        loop {
            tick.tick().await;
            let result = match cells::load(&store).await {
                Ok(directory) => {
                    if directory.get(&cell_id).is_none() {
                        Err(format!("CELL_ID {cell_id} disappeared from cells.json"))
                    } else {
                        let current = state.cell_directory.read().unwrap().clone();
                        match current {
                            Some(current) if directory.generation < current.generation => {
                                Err("cells.json generation rolled back".to_string())
                            }
                            Some(current)
                                if directory.generation == current.generation
                                    && directory != current =>
                            {
                                Err("cells.json changed without a generation advance".to_string())
                            }
                            _ => Ok(directory),
                        }
                    }
                }
                Err(error) => Err(error.to_string()),
            };
            match result {
                Ok(directory) => {
                    let generation = directory.generation;
                    *state.cell_directory.write().unwrap() = Some(directory);
                    last_good = std::time::Instant::now();
                    state
                        .cells_ready
                        .store(true, std::sync::atomic::Ordering::Release);
                    tracing::debug!(generation, "validated cells.json");
                }
                Err(error) => {
                    tracing::warn!("cell directory refresh failed: {error}");
                    if last_good.elapsed() > refresh.saturating_mul(2) {
                        state
                            .cells_ready
                            .store(false, std::sync::atomic::Ordering::Release);
                    }
                }
            }
        }
    });
}

async fn async_main() -> anyhow::Result<()> {
    let args = Args::parse();
    let history_block_write_format =
        HistoryBlockWriteFormat::try_from(args.history_block_write_format)?;

    if let Some(cell_id) = &args.cell_id {
        anyhow::ensure!(cells::valid_cell_id(cell_id), "CELL_ID is invalid");
        let expected_prefix = cells::cell_prefix(cell_id);
        anyhow::ensure!(
            args.path_prefix.as_deref() == Some(expected_prefix.as_str()),
            "managed CELL_ID requires PATH_PREFIX=cells/<cell-id>"
        );
        anyhow::ensure!(
            args.registry_s3_endpoint.is_some()
                && args.registry_s3_bucket.is_some()
                && args.registry_s3_access_key_id.is_some()
                && args.registry_s3_secret_access_key.is_some(),
            "managed CELL_ID requires separately configured registry credentials"
        );
        anyhow::ensure!(
            args.registry_s3_access_key_id.as_deref() != Some(args.access_key_id.as_str()),
            "managed CELL_ID registry and cell data credentials must be distinct"
        );
        anyhow::ensure!(
            args.registry_path_prefix
                .as_deref()
                .is_some_and(|prefix| !prefix.is_empty() && !prefix.starts_with("cells/")),
            "managed CELL_ID requires a non-cell REGISTRY_PATH_PREFIX"
        );
        anyhow::ensure!(
            args.fleet_prefix.as_deref().is_none_or(|prefix| {
                prefix
                    .strip_prefix(&expected_prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
            }),
            "FLEET_PREFIX must be inside the configured cell prefix"
        );
        anyhow::ensure!(
            (5..=3_600).contains(&args.cell_directory_refresh_secs),
            "CELL_DIRECTORY_REFRESH_SECS must be between 5 and 3600"
        );
    }

    anyhow::ensure!(
        args.auto_merge_cold_fraction_pct <= 20,
        "AUTO_MERGE_COLD_FRACTION_PCT must be between 0 and 20"
    );
    anyhow::ensure!(
        args.admit_max_inflight_per_customer <= 1_000_000
            && args.admit_max_live_connections_per_customer <= 1_000_000,
        "per-customer connection limits must not exceed 1000000"
    );
    for (name, value) in [
        (
            "ADMIT_WRITE_BURST_BYTES_PER_CUSTOMER",
            args.admit_write_burst_bytes_per_customer,
        ),
        (
            "ADMIT_READ_BURST_BYTES_PER_CUSTOMER",
            args.admit_read_burst_bytes_per_customer,
        ),
    ] {
        anyhow::ensure!(
            (1..=1 << 50).contains(&value),
            "{name} must be between 1 and 2^50"
        );
    }
    anyhow::ensure!(
        args.admit_write_bytes_per_sec_per_customer <= 1 << 50
            && args.admit_read_bytes_per_sec_per_customer <= 1 << 50,
        "per-customer byte rates must not exceed 2^50"
    );
    for (name, value) in [
        (
            "ADMIT_APPEND_REQUEST_BURST_PER_CUSTOMER",
            args.admit_append_request_burst_per_customer,
        ),
        (
            "ADMIT_READ_REQUEST_BURST_PER_CUSTOMER",
            args.admit_read_request_burst_per_customer,
        ),
        (
            "ADMIT_QUEUE_RECEIVE_BURST_PER_CUSTOMER",
            args.admit_queue_receive_burst_per_customer,
        ),
    ] {
        anyhow::ensure!(
            (1..=1_000_000_000).contains(&value),
            "{name} must be between 1 and 1000000000"
        );
    }
    anyhow::ensure!(
        args.admit_append_requests_per_sec_per_customer <= 1_000_000_000
            && args.admit_read_requests_per_sec_per_customer <= 1_000_000_000
            && args.admit_queue_receives_per_sec_per_customer <= 1_000_000_000,
        "per-customer request rates must not exceed 1000000000"
    );
    anyhow::ensure!(
        args.admit_append_requests_per_sec_per_stream <= 1_000_000_000
            && (1..=1_000_000_000).contains(&args.admit_append_request_burst_per_stream),
        "per-stream append request rate/burst are out of range"
    );
    anyhow::ensure!(
        args.admit_write_bytes_per_sec_per_stream <= 1 << 50
            && (1..=1 << 50).contains(&args.admit_write_burst_bytes_per_stream),
        "per-stream write byte rate/burst are out of range"
    );
    anyhow::ensure!(
        (1..=100).contains(&args.stream_commit_weight),
        "STREAM_COMMIT_WEIGHT must be between 1 and 100"
    );
    if args.fleet_prefix.is_some() {
        anyhow::ensure!(
            (1..=64).contains(&args.fleet_max),
            "FLEET_MAX must be between 1 and 64"
        );
        anyhow::ensure!(
            crate::fleet::valid_instance_name(&args.instance_name),
            "INSTANCE_NAME in fleet mode must contain only ASCII letters, digits, '-' or '_' and be at most 128 bytes"
        );
        anyhow::ensure!(
            crate::fleet::fleet_ordinal(&args.instance_name)
                .is_some_and(|ordinal| ordinal <= args.fleet_max),
            "INSTANCE_NAME in fleet mode must be streams-N with 1 <= N <= FLEET_MAX"
        );
    }

    let authn = match (
        args.auth_jwks_url.clone(),
        args.auth_revocation_url.clone(),
        args.auth_issuer.clone(),
        args.auth_audience.clone(),
    ) {
        (Some(url), Some(revocation_url), Some(issuer), Some(audience)) => {
            crate::auth::Authenticator::jwks(crate::auth::JwksConfig {
                url,
                revocation_url,
                issuer,
                audience,
                refresh_interval: Duration::from_secs(args.auth_jwks_refresh_secs),
                max_stale: Duration::from_secs(args.auth_jwks_max_stale_secs),
                revocation_refresh_interval: Duration::from_secs(args.auth_revocation_refresh_secs),
                revocation_max_stale: Duration::from_secs(args.auth_revocation_max_stale_secs),
            })
            .await
            .context("initialize JWKS authentication")?
        }
        (None, None, None, None) => match args.auth_token.clone() {
            Some(token) => crate::auth::Authenticator::legacy(token),
            None if args.allow_insecure_no_auth || args.conformance_default_key.is_some() => {
                tracing::warn!("authentication disabled by explicit development/conformance mode");
                crate::auth::Authenticator::Disabled
            }
            None => anyhow::bail!(
                "authentication is required: configure AUTH_JWKS_URL/AUTH_REVOCATION_URL/ISSUER/AUDIENCE, \
                 AUTH_TOKEN for the pilot, or explicitly set ALLOW_INSECURE_NO_AUTH=true"
            ),
        },
        _ => anyhow::bail!(
            "AUTH_JWKS_URL, AUTH_REVOCATION_URL, AUTH_ISSUER, and AUTH_AUDIENCE must be set together"
        ),
    };
    anyhow::ensure!(
        args.cell_id.is_none() || authn.production_ready(),
        "managed CELL_ID mode requires production JWKS authentication"
    );
    anyhow::ensure!(
        !authn.production_ready() || args.auth_token.is_none(),
        "AUTH_TOKEN is pilot-only and must be unset when production JWKS authentication is configured"
    );
    if authn.production_ready()
        && args.metrics_key.is_some()
        && args.metrics_lb_url.is_some()
        && args.metrics_auth_token.is_none()
    {
        anyhow::bail!("the internal metrics flusher requires METRICS_AUTH_TOKEN in JWKS mode");
    }
    anyhow::ensure!(
        matches!(
            (
                &args.metrics_key,
                &args.metrics_lb_url,
                &args.metrics_customer_id
            ),
            (Some(_), Some(_), Some(_)) | (None, None, None)
        ),
        "METRICS_KEY, METRICS_LB_URL, and METRICS_CUSTOMER_ID must be configured together"
    );
    if let Some(customer) = &args.metrics_customer_id {
        anyhow::ensure!(
            !customer.is_empty()
                && customer.len() <= 128
                && customer.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
                }),
            "METRICS_CUSTOMER_ID is not a bounded customer identifier"
        );
    }
    anyhow::ensure!(
        (1..=3_600).contains(&args.metrics_export_interval_secs),
        "METRICS_EXPORT_INTERVAL_SECS must be between 1 and 3600"
    );
    if args.require_metrics_export && args.metrics_key.is_none() {
        anyhow::bail!("REQUIRE_METRICS_EXPORT=true but metrics export is not configured");
    }

    let ops_store = args.store_for(&args.ops_bucket)?;
    let shard_store = args.store_for(&args.shard_bucket)?;
    let data_store = args.store_for(&args.data_bucket)?;
    let registry_store = args.registry_store()?;
    let backup_store = args.backup_store()?;
    let audit_mirror = args.audit_mirror_store()?;
    if args.require_backup && backup_store.is_none() {
        anyhow::bail!("REQUIRE_BACKUP=true but BACKUP_S3_ENDPOINT is not configured");
    }
    if args.require_audit_mirror && audit_mirror.is_none() {
        anyhow::bail!("REQUIRE_AUDIT_MIRROR=true but AUDIT_MIRROR_S3_ENDPOINT is not configured");
    }
    anyhow::ensure!(
        (1..=1_000_000).contains(&args.audit_sample_denominator),
        "AUDIT_SAMPLE_DENOMINATOR must be between 1 and 1000000"
    );
    anyhow::ensure!(
        (1..=300).contains(&args.audit_operator_batch_interval_secs),
        "AUDIT_OPERATOR_BATCH_INTERVAL_SECS must be between 1 and 300"
    );
    anyhow::ensure!(
        (24 * 60 * 60..=365 * 24 * 60 * 60).contains(&args.audit_primary_retention_secs),
        "AUDIT_PRIMARY_RETENTION_SECS must be between one day and one year"
    );
    anyhow::ensure!(
        args.audit_mirror_retention_secs >= args.audit_primary_retention_secs
            && args.audit_mirror_retention_secs <= 7 * 365 * 24 * 60 * 60,
        "AUDIT_MIRROR_RETENTION_SECS must cover primary retention and be at most seven years"
    );
    anyhow::ensure!(
        (60..=24 * 60 * 60).contains(&args.audit_maintenance_interval_secs)
            && (1..=100_000).contains(&args.audit_maintenance_objects_per_interval)
            && (64 * 1024..=64 * 1024 * 1024).contains(&args.audit_maintenance_max_object_bytes),
        "audit maintenance interval, object count, or object size bound is invalid"
    );
    if backup_store.is_some() {
        anyhow::ensure!(
            crate::fleet::valid_instance_name(&args.instance_name),
            "INSTANCE_NAME must be a bounded ASCII coordinator identity when backup is enabled"
        );
        anyhow::ensure!(
            args.backup_interval_secs >= 60,
            "BACKUP_INTERVAL_SECS must be at least 60"
        );
        anyhow::ensure!(
            (60..=24 * 60 * 60).contains(&args.backup_rpo_budget_secs)
                && args.backup_interval_secs <= args.backup_rpo_budget_secs,
            "BACKUP_RPO_BUDGET_SECS must cover the snapshot interval and be at most one day"
        );
        anyhow::ensure!(
            args.backup_retention_secs >= args.backup_interval_secs.saturating_mul(2)
                && args.backup_retention_secs <= 365 * 24 * 60 * 60,
            "BACKUP_RETENTION_SECS must retain at least two intervals and at most one year"
        );
        anyhow::ensure!(
            args.backup_checkpoint_lifetime_secs >= args.backup_interval_secs.saturating_mul(2)
                && args.backup_checkpoint_lifetime_secs <= 24 * 60 * 60,
            "BACKUP_CHECKPOINT_LIFETIME_SECS must cover two intervals and at most one day"
        );
        anyhow::ensure!(
            (10..=24 * 60 * 60).contains(&args.backup_scrub_interval_secs)
                && (1..=100_000).contains(&args.backup_scrub_objects_per_interval),
            "backup scrub interval or batch is out of range"
        );
        anyhow::ensure!(
            (10..=24 * 60 * 60).contains(&args.primary_scrub_interval_secs)
                && (1..=100_000).contains(&args.primary_scrub_objects_per_interval)
                && (1024 * 1024..=1024 * 1024 * 1024)
                    .contains(&args.primary_scrub_max_object_bytes),
            "primary scrub interval, batch, or object bound is out of range"
        );
    }
    let backup_write_format =
        streams_slate::backup::BackupWriteFormat::try_from(args.backup_write_format)?;
    let fleet_capabilities = crate::fleet::FleetCapabilities::current(
        args.history_block_write_format,
        args.backup_write_format,
    )
    .map_err(anyhow::Error::msg)?;
    let backup_config = backup_store.map(|destination| {
        // A role bucket may be shared by all three logical stores. Snapshot
        // each physical keyspace once; the first role is the restore name.
        let mut seen_buckets = HashSet::new();
        // Prefer the shard role when physical buckets are shared so the
        // backup actor applies exact pinned-manifest filtering to that copy.
        // Restore still maps all logical roles sharing a bucket to one target.
        let shard_bucket = args.shard_bucket.as_deref().unwrap_or(&args.bucket);
        let ops_bucket = args.ops_bucket.as_deref().unwrap_or(&args.bucket);
        let data_bucket = args.data_bucket.as_deref().unwrap_or(&args.bucket);
        let history_source_role = if data_bucket == shard_bucket {
            "shard"
        } else if data_bucket == ops_bucket {
            "ops"
        } else {
            "data"
        };
        let sources = [
            ("shard", shard_bucket, shard_store.clone()),
            ("ops", ops_bucket, ops_store.clone()),
            ("data", data_bucket, data_store.clone()),
        ]
        .into_iter()
        .filter_map(|(role, bucket, store)| {
            seen_buckets
                .insert(bucket.to_string())
                .then_some(streams_slate::backup::BackupSource { role, store })
        })
        .collect();
        streams_slate::backup::BackupConfig {
            sources,
            destination,
            interval: Duration::from_secs(args.backup_interval_secs),
            rpo_budget: Duration::from_secs(args.backup_rpo_budget_secs),
            retention: Duration::from_secs(args.backup_retention_secs),
            scrub_interval: Duration::from_secs(args.backup_scrub_interval_secs),
            scrub_objects_per_interval: args.backup_scrub_objects_per_interval,
            primary_scrub_interval: Duration::from_secs(args.primary_scrub_interval_secs),
            primary_scrub_objects_per_interval: args.primary_scrub_objects_per_interval,
            primary_scrub_max_object_bytes: args.primary_scrub_max_object_bytes,
            pins: Some(streams_slate::backup::BackupPins {
                cell_id: args.cell_id.clone(),
                topology_store: ops_store.clone(),
                registry_store: registry_store.clone(),
                shard_store: shard_store.clone(),
                data_store: data_store.clone(),
                history_source_role,
                lifetime: Duration::from_secs(args.backup_checkpoint_lifetime_secs),
            }),
            coordinator: Some(streams_slate::backup::BackupCoordinator {
                store: ops_store.clone(),
                owner: args.instance_name.clone(),
            }),
            write_format: backup_write_format,
        }
    });

    let cell_directory = match &args.cell_id {
        Some(cell_id) => {
            let directory = cells::load(&registry_store)
                .await
                .context("load cells.json")?;
            let local = directory
                .get(cell_id)
                .with_context(|| format!("CELL_ID {cell_id} is absent from cells.json"))?;
            anyhow::ensure!(
                Some(local.ops_prefix.as_str()) == args.path_prefix.as_deref(),
                "cells.json ops_prefix does not match PATH_PREFIX"
            );
            Some(directory)
        }
        None => None,
    };
    let registry = Registry::new(registry_store.clone());
    let topology = load_or_init_topology(&ops_store, args.initial_shards)
        .await
        .context("load topology")?;
    tracing::info!(
        "topology v{}: {} shard(s)",
        topology.version,
        topology.shards.len()
    );
    // Do not allow readiness to be satisfied by an empty startup snapshot:
    // initialize and validate the control plane before the first actor tick.
    let backup = backup_config.map(streams_slate::backup::start);

    let keys = Arc::new(KeyCache::default());
    let touch = Arc::new(crate::touch::TouchRegistry::default());
    let audit = crate::audit::AuditLog::start_with_config(
        ops_store.clone(),
        &args.instance_name,
        crate::audit::AuditConfig {
            mirror: audit_mirror,
            sample_denominator: args.audit_sample_denominator,
            operator_batch_interval: Duration::from_secs(args.audit_operator_batch_interval_secs),
            primary_retention: Duration::from_secs(args.audit_primary_retention_secs),
            mirror_retention: Duration::from_secs(args.audit_mirror_retention_secs),
            maintenance_interval: Duration::from_secs(args.audit_maintenance_interval_secs),
            maintenance_objects_per_interval: args.audit_maintenance_objects_per_interval,
            maintenance_max_object_bytes: args.audit_maintenance_max_object_bytes,
        },
    );

    // Shards open lazily on first routed request (COMPUTE-SPEC §5.1):
    // opening fences the previous owner, so ownership follows routing.
    // A closed (fenced-away) shard is dropped from the serving map via
    // AppState::shard_closed through this weak back-reference.
    let state_slot: Arc<std::sync::OnceLock<std::sync::Weak<AppState>>> =
        Arc::new(std::sync::OnceLock::new());
    let telemetry = Arc::new(crate::telemetry::Telemetry::default());
    let opener = {
        let shard_store = shard_store.clone();
        let data_store = data_store.clone();
        let ops_store = ops_store.clone();
        let keys = keys.clone();
        let touch = touch.clone();
        let telemetry = telemetry.clone();
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
        let history_integrity_max_object_bytes = args.primary_scrub_max_object_bytes;
        let trim_per_op = args.trim_per_op;
        let state_slot = state_slot.clone();
        crate::http::ShardOpener {
            open: Box::new(move |prefix: String, path: String| {
                let shard_store = shard_store.clone();
                let shared_cache = shared_cache.clone();
                let data_store = data_store.clone();
                let ops_store = ops_store.clone();
                let keys = keys.clone();
                let touch = touch.clone();
                let telemetry = telemetry.clone();
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
                    tracing::info!("opening shard log {path} (lazy; fences prior owner)");
                    let db = Db::builder(path.as_str(), shard_store.clone())
                        .with_settings(settings)
                        .with_db_cache(shared_cache)
                        .build()
                        .await
                        .with_context(|| format!("open shard log {path}"))?;
                    let recovered_absorptions = ShardEngine::recover_pending_absorptions(&db)
                        .await
                        .with_context(|| {
                            format!("recover pending history absorption for {path}")
                        })?;
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
                        Arc::new(db),
                        ShardConfig {
                            max_trim_per_op: trim_per_op,
                            ..Default::default()
                        },
                        absorb_tx,
                        telemetry.clone(),
                        Some(on_close),
                        Some(shard_store.clone()),
                    );
                    Absorber::start(
                        data_store,
                        ops_store,
                        engine.clone(),
                        keys,
                        telemetry,
                        AbsorberConfig {
                            threshold_bytes: absorb_bytes,
                            threshold_age: Duration::from_secs(absorb_age),
                            pass_bytes: absorb_pass_bytes,
                            integrity_max_object_bytes: history_integrity_max_object_bytes,
                            history_block_write_format,
                            ..Default::default()
                        },
                        AbsorberStartup {
                            receiver: absorb_rx,
                            recovered: recovered_absorptions,
                        },
                    );
                    Ok(engine)
                })
            }),
        }
    };

    let state = Arc::new(AppState {
        registry,
        operator_fleet_store: args.fleet_store()?,
        cell_id: args.cell_id.clone(),
        cell_directory: std::sync::RwLock::new(cell_directory),
        cells_ready: std::sync::atomic::AtomicBool::new(true),
        topology: std::sync::RwLock::new(topology.clone()),
        topology_version: std::sync::atomic::AtomicU64::new(topology.version),
        topology_ready: std::sync::atomic::AtomicBool::new(true),
        splitting_prefixes: std::sync::RwLock::new(HashSet::new()),
        split_workers: std::sync::Mutex::new(HashSet::new()),
        split_ready: std::sync::atomic::AtomicBool::new(true),
        merge_ready: std::sync::atomic::AtomicBool::new(true),
        fleet_ready: std::sync::atomic::AtomicBool::new(args.fleet_prefix.is_none()),
        fleet_capabilities,
        shards: std::sync::RwLock::new(HashMap::new()),
        opener,
        open_lock: tokio::sync::Mutex::new(HashMap::new()),
        fleet_ops: std::sync::atomic::AtomicU64::new(0),
        inflight: std::sync::atomic::AtomicI64::new(0),
        inflight_peak: std::sync::atomic::AtomicI64::new(0),
        admit_max_inflight: args.admit_max_inflight,
        admit_rss_shed_mb: args.admit_rss_shed_mb,
        rss_mb_cached: std::sync::atomic::AtomicU64::new(0),
        admit_shed: std::sync::atomic::AtomicU64::new(0),
        tenant_admission: crate::http::TenantAdmission::new(crate::http::TenantAdmissionConfig {
            max_inflight: args.admit_max_inflight_per_customer,
            max_live_connections: args.admit_max_live_connections_per_customer,
            write_bytes_per_second: args.admit_write_bytes_per_sec_per_customer,
            write_burst_bytes: args.admit_write_burst_bytes_per_customer,
            append_requests_per_second: args.admit_append_requests_per_sec_per_customer,
            append_request_burst: args.admit_append_request_burst_per_customer,
            read_requests_per_second: args.admit_read_requests_per_sec_per_customer,
            read_request_burst: args.admit_read_request_burst_per_customer,
            read_bytes_per_second: args.admit_read_bytes_per_sec_per_customer,
            read_burst_bytes: args.admit_read_burst_bytes_per_customer,
            queue_receives_per_second: args.admit_queue_receives_per_sec_per_customer,
            queue_receive_burst: args.admit_queue_receive_burst_per_customer,
        }),
        stream_admission: crate::http::StreamAdmission::new(crate::http::StreamAdmissionConfig {
            append_requests_per_second: args.admit_append_requests_per_sec_per_stream,
            append_request_burst: args.admit_append_request_burst_per_stream,
            write_bytes_per_second: args.admit_write_bytes_per_sec_per_stream,
            write_burst_bytes: args.admit_write_burst_bytes_per_stream,
            commit_weight: args.stream_commit_weight,
        }),
        audit,
        backup,
        instance_name: args.instance_name.clone(),
        ring_active: std::sync::RwLock::new(Vec::new()),
        data_store,
        ops_store: ops_store.clone(),
        shard_store: shard_store.clone(),
        keys,
        touch,
        default_key: args.conformance_default_key.clone(),
        default_ordering: args
            .conformance_ordering_segments
            .map(|n| ("per-key".to_string(), n)),
        authn,
        metrics: Arc::new(crate::metrics::Metrics::default()),
        metrics_identity: args
            .metrics_customer_id
            .clone()
            .map(|customer| (customer, "__metrics__".to_string())),
        telemetry,
    });
    let _ = state_slot.set(Arc::downgrade(&state));
    start_cell_directory_watcher(
        state.clone(),
        registry_store,
        Duration::from_secs(args.cell_directory_refresh_secs),
    );
    crate::split::initialize(&state)
        .await
        .map_err(anyhow::Error::msg)
        .context("load split intents")?;
    crate::merge::initialize(&state)
        .await
        .map_err(anyhow::Error::msg)
        .context("load merge intents")?;
    crate::split::start(
        state.clone(),
        crate::split::AutoSplitConfig {
            single_shard_write_ceiling_bytes_per_sec: args.single_shard_write_ceiling_bytes_per_sec,
            sustain: Duration::from_secs(args.auto_split_sustain_secs.max(1)),
            gc_retention: Duration::from_secs(args.split_gc_retention_secs.min(365 * 24 * 60 * 60)),
            gc_interval: Duration::from_secs(args.split_gc_interval_secs.clamp(1, 24 * 60 * 60)),
        },
    );
    crate::merge::start(
        state.clone(),
        crate::merge::MergeConfig {
            gc_retention: Duration::from_secs(args.split_gc_retention_secs.min(365 * 24 * 60 * 60)),
            gc_interval: Duration::from_secs(args.split_gc_interval_secs.clamp(1, 24 * 60 * 60)),
            single_shard_write_ceiling_bytes_per_sec: args.single_shard_write_ceiling_bytes_per_sec,
            cold_fraction_pct: args.auto_merge_cold_fraction_pct,
            cold_sustain: Duration::from_secs(args.auto_merge_sustain_secs.max(1)),
            fleet_mode: args.fleet_prefix.is_some(),
        },
    );
    start_topology_watcher(state.clone(), ops_store.clone());
    if let (Some(mk), Some(lb)) = (args.metrics_key.clone(), args.metrics_lb_url.clone()) {
        state.metrics.configure_export();
        let st = state.clone();
        let instance = args.instance_name.clone();
        let export_interval = Duration::from_secs(args.metrics_export_interval_secs);
        let metrics_auth = args
            .metrics_auth_token
            .clone()
            .or_else(|| args.auth_token.clone())
            .unwrap_or_default();
        tokio::spawn(async move {
            crate::http::metrics_flusher(st, mk, instance, lb, metrics_auth, export_interval).await;
        });
    }
    if let Some(fleet_store) = args.fleet_store()? {
        {
            // RSS sampler for the shed check (500 ms; /proc read per
            // request would be silly).
            let st = state.clone();
            tokio::spawn(async move {
                loop {
                    st.rss_mb_cached.store(
                        crate::fleet::rss_bytes() / 1048576,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            });
        }
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
    let app = http::router(state);

    crate::store_timing::spawn_sentinels();

    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    tracing::info!("streams-slate listening on {}", args.listen);
    axum::serve(listener, app).await?;
    Ok(())
}
