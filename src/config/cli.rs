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

    /// The bucket of every role: ops, shard logs and data.
    #[arg(long, env = "SLATE_S3_BUCKET", default_value = "streams")]
    pub(crate) bucket: String,

    #[arg(long, env = "SLATE_S3_REGION", default_value = "us-east-1")]
    pub(crate) region: String,
    #[arg(long, env = "SLATE_S3_ACCESS_KEY_ID", default_value = "test")]
    pub(crate) access_key_id: String,
    #[arg(long, env = "SLATE_S3_SECRET_ACCESS_KEY", default_value = "test")]
    pub(crate) secret_access_key: String,

    /// Initial shard count (power of two) if no topology exists yet (D3).
    /// Unset = auto: 1 standalone, and in fleet mode the largest power of
    /// two at most FLEET_MAX. Every shard is its own WAL writer, so a
    /// fresh cell runs no more writers than servers (owner decision of
    /// 2026-10-07); a fleet that sets more than FLEET_MAX is warned.
    #[arg(long, env = "INITIAL_SHARDS")]
    pub(crate) initial_shards: Option<usize>,

    /// Shard-log WAL flush interval (D22, amended). 5 ms minted WAL SSTs
    /// ~7× faster than SlateDB's WAL GC reaps them; the growing backlog
    /// degraded the per-DB durable watermark to ~0.3–1 s (EXPERIMENT-PILOT
    /// run 3). 25 ms keeps the ack floor ≈ flush + Tigris PUT ≈ 40 ms while
    /// cutting WAL-object churn 5×. It is the flush cadence only with
    /// WAL_GROUP_COMMIT=0. With the group-commit pump (the default) it is
    /// read only as the gap when WAL_FLUSH_GAP_MS is 0; SlateDB's own
    /// timer is then a fixed 60 s failsafe.
    #[arg(long, env = "FLUSH_INTERVAL_MS", default_value_t = 25)]
    pub(crate) flush_interval_ms: u64,

    /// Group-commit WAL flushing (1 = on, the default; 0 = SlateDB's
    /// fixed flush tick). A per-shard pump flushes the WAL the moment the
    /// previous flush completes when commits are waiting, so under load
    /// the flush cadence self-clocks to the WAL PUT RTT instead of adding
    /// tick alignment (avg tick/2) on top of the serial-PUT queue. The
    /// idle mint-rate floor is --wal-flush-gap-ms (flush_interval_ms when
    /// that is 0) and SlateDB's own timer is a fixed 60 s failsafe.
    #[arg(long, env = "WAL_GROUP_COMMIT", default_value_t = 1)]
    pub(crate) wal_group_commit: u8,

    /// Minimum start-to-start gap between pump flushes, ms: the one write
    /// tier (owner decision of 2026-10-07). With the 1 ms herd-settle of a
    /// non-zero gather, a shard's pump flushes at most once per 101 ms,
    /// about 9.9 times a second. An append waits for the rest of the gap
    /// since its shard's last flush started, then the settle and one WAL
    /// write; on a shard that has not flushed for a gap, only the settle
    /// and the write. Irrelevant when the PUT RTT exceeds it. 0 = use
    /// flush_interval_ms.
    #[arg(long, env = "WAL_FLUSH_GAP_MS", default_value_t = 100)]
    pub(crate) wal_flush_gap_ms: u64,

    /// Post-ACK gather window, ms (0 = off). After a busy WAL flush the
    /// pump releases that flush's acknowledgements itself (explicit
    /// barrier), then waits this long before freezing the next WAL, so
    /// closed-loop producers' ack-triggered follow-ups join the next WAL
    /// instead of missing its freeze and paying a full extra PUT. Without
    /// it, append p50 at concurrency 2 measures ~2x concurrency 1.
    /// 6 is the soaked value (docs/SOAK5-REPORT.md). Adds at most this
    /// many ms to a busy flush cycle; an idle shard's first write pays
    /// only the 1 ms herd-settle before its flush, not the window.
    #[arg(long, env = "WAL_POST_ACK_GATHER_MS", default_value_t = 6)]
    pub(crate) wal_post_ack_gather_ms: u64,

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
    /// headroom before compaction must catch up; an L0 costs a stored
    /// object, not memory. 32 is the production posture
    /// (deploy/profiles/compute-1g.env); at 8 batch ingest stalled on
    /// backpressure while the compactor kept up.
    /// Also the per-key L0 overlap cap: an ordered stream rewrites its meta
    /// row in every memtable, so every L0 overlaps on that key.
    #[arg(long, env = "L0_MAX_SSTS", default_value_t = 32)]
    pub(crate) l0_max_ssts: usize,

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

    /// Concurrent compactions (upstream default 4; 1 is the certified
    /// 1 GiB posture). Merges are object-I/O bound on Tigris, so extra
    /// concurrency overlaps GET/PUT latency.
    #[arg(long, env = "COMPACTOR_MAX_CONCURRENT", default_value_t = 1)]
    pub(crate) compactor_max_concurrent: usize,

    // R27-4 compaction-worker memory knobs (COMPACT_MAX_SUBCOMPACTIONS,
    // COMPACT_MAX_FETCH_TASKS, COMPACT_BYTES_TO_FETCH,
    // COMPACT_MAX_SST_SIZE_BYTES) are ENV-ONLY, read by
    // resolved_compactor_options() — the one source every DB family
    // shares. R29 review: clap mirrors here parsed but were never read,
    // so a CLI override silently did nothing; removed rather than
    // duplicating the plumbing.
    //
    // The GC cadences and age floors are constants of `EngineConfig`
    // (30/60 s for the WAL, 30/120 s for the compactions log, 600 s for
    // the quiet directories): no argument sets them.
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

    /// Absorber thresholds (§3.6 / D23).
    #[arg(long, env = "ABSORB_BYTES", default_value_t = 4 * 1024 * 1024)]
    pub(crate) absorb_bytes: u64,
    /// Age threshold, seconds: a stream's unabsorbed tail is absorbed once
    /// it is this old, whatever its size. The default, 60, is the value the
    /// deployments run (300 until 2026-09-29).
    #[arg(long, env = "ABSORB_AGE_SECS", default_value_t = 60)]
    pub(crate) absorb_age_secs: u64,

    /// Evict resident per-stream handles idle at least this long
    /// (seconds; 0 = never, refused under STREAMS_AUTH_MODE=enforce, item
    /// 50). Handles reload from the shard DB on next touch; the durable
    /// dirty-stream index keeps unabsorbed evictees discoverable.
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
    /// later ticks. Default 8 MiB, the batch the 1 GiB posture runs.
    #[arg(long, env = "ABSORB_GATHER_MAX_BYTES", default_value_t = 8 * 1024 * 1024)]
    pub(crate) absorb_gather_max_bytes: usize,

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
    /// outcome without touching responses. Shadow and enforce refuse to
    /// start without an explicit PROJECT_ID and the three feed files below.
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
    ///
    /// A feed's age counts from the refresh pass that last read it
    /// successfully, so an unchanged file stays fresh while it stays
    /// readable and valid; whether its author has published a newer
    /// generation shows only as the policy and grant `feedVersion` on
    /// /v1/debug/auth.
    #[arg(long, env = "STREAMS_AUTH_KEYS_FILE")]
    pub(crate) streams_auth_keys_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_POLICY_FILE")]
    pub(crate) streams_auth_policy_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_GRANTS_FILE")]
    pub(crate) streams_auth_grants_file: Option<std::path::PathBuf>,
    #[arg(long, env = "STREAMS_AUTH_REFRESH_SECS", default_value_t = 30)]
    pub(crate) streams_auth_refresh_secs: u64,
    /// Pause (seconds, floored at 1) between circles of the fork-debt
    /// reconciler, which releases the source references that deleted forks
    /// still owe (TLA-019-F4). Same cadence as OUTBOX_SWEEP_SECS, the
    /// neighbouring tombstone walk; a backlog drains without waiting for it.
    #[arg(long, env = "FORK_DEBT_SWEEP_SECS", default_value_t = 300)]
    pub(crate) fork_debt_sweep_secs: u64,
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
    /// reconnect herd on a shared feed. Default 131,072, an eighth of
    /// the default ring. 0 = unlimited, which the release posture
    /// refuses, as it refuses a ceiling whose worst frame exceeds the ring.
    #[arg(long, env = "MAX_RECORD_PAYLOAD_BYTES", default_value = "131072")]
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

    /// Round-13: per-project memory-pressure high watermark in bytes
    /// (0 = off; deploy/profiles/shared-cell.env sets one, pending certification).
    #[arg(long, env = "PROJECT_MEMORY_PRESSURE_BYTES", default_value_t = 0)]
    pub(crate) project_memory_pressure_bytes: u64,
    /// Hysteresis release point as a percentage of the high watermark.
    #[arg(long, env = "PROJECT_MEMORY_RELEASE_PCT", default_value_t = 75)]
    pub(crate) project_memory_release_pct: u64,
    /// RSS shed threshold (MB): 429 writes while RSS exceeds this.
    /// Docker phase 1: without it a 1 GB cgroup OOM-kills the instance at
    /// full throughput. MUST sit well below the platform kill line (the
    /// slate-codex A/B died at ~750 MB anon RSS on Prisma Compute with the
    /// shed configured at 800 — an unreachable guard protects nothing).
    /// Counted in MiB, as sampled RSS plus the absorber's reserved bytes.
    /// Default 500, the certified 1 GiB posture
    /// (deploy/profiles/compute-1g.env). It assumes the default caches:
    /// the fixed budgets sum to 336 MiB and boot warns when they leave
    /// less than 100 below this line. 0 = off.
    #[arg(long, env = "ADMIT_RSS_SHED_MB", default_value_t = 500)]
    pub(crate) admit_rss_shed_mb: u64,

    /// Instance cap on live SSE subscriptions (#267): new subscriptions
    /// past the cap get a typed 503 subscription_capacity instead of
    /// subscriber RSS pushing UNRELATED appends over the write shed
    /// line. 0 = unlimited (refused under the release posture). Default
    /// 1,200 is the 1 GiB Compute class value: the public edge bounds
    /// service concurrency near 1.2-1.4k (bench/WORKLOAD-CERT-PLAN.md).
    /// A larger class sets its own cap once a ladder has measured it.
    #[arg(long, env = "SSE_MAX_CONNECTIONS", default_value_t = 1_200)]
    pub(crate) sse_max_connections: u64,

    /// Per-stream inflight append cap (0 = off): one hot stream cannot
    /// occupy every admission slot of its shard owner (scoped 429).
    /// Default 256, what the deployments set, half the default instance
    /// cap. Set it below ADMIT_MAX_INFLIGHT where that is lowered.
    #[arg(long, env = "ADMIT_MAX_INFLIGHT_PER_STREAM", default_value_t = 256)]
    pub(crate) admit_max_inflight_per_stream: i64,

    /// §12-lite admission backstop: beyond this many requests in flight an
    /// authenticated append gets 429 + Retry-After: 1 after a 25 ms tarpit
    /// (0 = off); reads, creates and deletes are not shed by the cap. Keeps
    /// the durable path from queue collapse under overload; clients must
    /// honor Retry-After. Default 512, what the deployments set. The count
    /// covers every request on every route, so parked long-polls use it up;
    /// above four times the cap every /v1/stream and /v1/streams request is
    /// refused with 503 before authentication.
    #[arg(long, env = "ADMIT_MAX_INFLIGHT", default_value_t = 512)]
    pub(crate) admit_max_inflight: i64,

    /// k, the number of projects this cell is shared by (shared-cells
    /// PLAN step 2, finding H1): each project's quota on every shared
    /// bound (the envelope below, ADMIT_MAX_INFLIGHT, the SSE cap, the
    /// per-stream maps, MAX_UNABSORBED_BYTES_PER_INSTANCE) is at most
    /// bound / k, and a 0 or missing quota takes exactly that. Default 1,
    /// a dedicated cell: quotas pass unchanged. At least 1.
    #[arg(long, env = "PROJECT_SHARE_K", default_value_t = 1,
          value_parser = clap::value_parser!(u64).range(1..))]
    pub(crate) project_share_k: u64,
    /// The requests per second this cell was measured to carry, divided
    /// by PROJECT_SHARE_K into each project's ceiling (0 = no shared bound).
    #[arg(long, env = "CELL_ENVELOPE_REQUESTS_PER_SEC", default_value_t = 0)]
    pub(crate) cell_envelope_requests_per_sec: u64,
    /// The appended payload bytes per second it was measured to carry
    /// (0 = no shared bound).
    #[arg(long, env = "CELL_ENVELOPE_APPEND_BYTES_PER_SEC", default_value_t = 0)]
    pub(crate) cell_envelope_append_bytes_per_sec: u64,
    /// The read payload bytes per second it was measured to carry
    /// (0 = no shared bound).
    #[arg(long, env = "CELL_ENVELOPE_READ_BYTES_PER_SEC", default_value_t = 0)]
    pub(crate) cell_envelope_read_bytes_per_sec: u64,

    /// Measured per-instance ingress-concurrency capacity through the
    /// platform front door. Two-layer model confirmed by the platform team
    /// and six independent sources (2026-07-15): each SOURCE Compute
    /// instance is egress-capped at ~48-50 outgoing requests; the
    /// DESTINATION front door admits ~145-150 concurrent aggregate (the
    /// earlier 48 calibration was the measuring instance's own egress
    /// cap). Scale-out begins at scale_out_cpu_pct% of this. 0 disables.
    #[arg(long, env = "SCALE_EDGE_SLOTS", default_value_t = 140)]
    pub(crate) scale_edge_slots: u64,

    /// ONE shared block cache across all shard DBs (§1.1). SlateDB's
    /// per-DB default is 512 MB — 16 shards × 512 MB on a 1 GB instance
    /// dies by cache fill in tens of minutes (the run 6/8 zombie
    /// generator; found 2026-07-15). 128 MiB is the 1 GiB posture
    /// (deploy/profiles/compute-1g.env); a larger instance class sets more.
    #[arg(long, env = "SHARED_CACHE_BYTES", default_value_t = 128 * 1024 * 1024)]
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
            region: "us-east-1".into(),
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            initial_shards: None,
            flush_interval_ms: 25,
            wal_group_commit: 1,
            wal_flush_gap_ms: 100,
            wal_post_ack_gather_ms: 6,
            tail_ring_bytes: 0,
            l0_sst_size_bytes: 8 * 1024 * 1024,
            max_unflushed_bytes: 16 * 1024 * 1024,
            max_request_body_bytes: 32 * 1024 * 1024,
            l0_max_ssts: 32,
            compactor_poll_ms: crate::DEFAULT_COMPACTOR_POLL_MS,
            compactor_max_concurrent: 1,
            manifest_poll_ms: crate::DEFAULT_MANIFEST_POLL_MS,
            trim_per_op: 8_192,
            trim_global_budget: 65_536,
            absorb_bytes: 4 * 1024 * 1024,
            absorb_age_secs: 60,
            handle_idle_evict_secs: 600,
            handle_max_resident: 65_536,
            absorb_gather_max_bytes: 8 * 1024 * 1024,
            absorb_read_par: 8,
            conformance_default_key: None,
            auth_token: None,
            streams_auth_mode: "off".into(),
            streams_auth_issuer: "https://auth.prisma.io".into(),
            streams_auth_keys_file: None,
            streams_auth_policy_file: None,
            streams_auth_grants_file: None,
            streams_auth_refresh_secs: 30,
            fork_debt_sweep_secs: 300,
            streams_cursor_key: None,
            fleet_internal_token: None,
            fleet_auth_mode: "static".into(),
            workload_token_file: None,
            release_posture: false,
            max_record_payload_bytes: Some(131_072),
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
            scale_out_cpu_pct: 75,
            scale_in_cpu_pct: 50,
            scale_cpu_sustain_secs: 20,
            scale_edge_latency_ms: 1000,
            project_memory_pressure_bytes: 0,
            project_memory_release_pct: 75,
            admit_rss_shed_mb: 500,
            sse_max_connections: 1_200,
            admit_max_inflight_per_stream: 256,
            admit_max_inflight: 512,
            project_share_k: 1,
            cell_envelope_requests_per_sec: 0,
            cell_envelope_append_bytes_per_sec: 0,
            cell_envelope_read_bytes_per_sec: 0,
            scale_edge_slots: 140,
            shared_cache_bytes: 128 * 1024 * 1024,
            scale_in_secs: 60,
            scale_latency_ms: 250,
            scale_lat_sustain_secs: 20,
            fleet_max: 4,
        }
    }
}

impl CliArgs {
    /// BILLING_MODE=required: production billing, where volatile fallbacks
    /// are refused and billing infrastructure failures are fatal at startup.
    /// Clap has already resolved `--billing-mode` over the variable and no
    /// consumer re-reads the environment, so validation, the drain, /health
    /// and /operator/billing.json agree with boot (item 32). Only the exact
    /// word `required` selects it.
    pub(crate) fn billing_required(&self) -> bool {
        self.billing_mode == "required"
    }

    /// ROLLUP=1: this instance runs the usage rollup consumer and month
    /// closer, so required-mode readiness waits for its rollup database.
    /// Resolved by clap like `billing_required`; only the exact word `1`.
    pub(crate) fn runs_rollup(&self) -> bool {
        self.rollup == "1"
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
