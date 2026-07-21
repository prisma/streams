//! HTTP surface (spec §3.4/§3.5): keyed appends, merged reads (history +
//! shard tail), ciphertext frames by default, server-side decryption for
//! `format=json`, long-poll tails.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, HttpBody};
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use object_store::ObjectStore;
use serde::Deserialize;
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::oneshot;

use crate::auth::{AuthError, Authenticator, Principal, Verb};
use crate::crypto::{
    FrameHeader, StreamKey, decode_frame, decrypt_frame, derive_subkey, encrypt_frame, hex,
};
use crate::history::{KeyCache, read_history};
use crate::offsets::Offset;
use crate::registry::{Registry, StorageHash, StreamDesc, Topology, shard_for_hash};
use crate::shard::{AppendErr, AppendReq, EnqueueError, ShardEngine, now_ms, read_frames};

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_READ_BYTES: usize = 8 * 1024 * 1024;
const MAX_FORK_MATERIALIZE_BYTES: usize = 32 * 1024 * 1024;
const MAX_ROUTING_KEY_BYTES: usize = 4 * 1024;
const APPEND_TIMEOUT: Duration = Duration::from_secs(10);
/// Must conclude BELOW the platform front door's 30 s proxy timeout: at
/// 30 s the edge kills the response as a 502 and the client sees a
/// transport error instead of a clean empty poll (measured 2026-07-20:
/// closed after 30.16 s http=502). 25 s restores the original margin.
const MAX_LONG_POLL: Duration = Duration::from_secs(25);
const OPERATOR_APPROVAL_HEADER: &str = "x-prisma-operator-approval";
const REQUEST_ID_HEADER: &str = "x-prisma-request-id";

#[derive(Clone)]
struct RequestId(String);

impl RequestId {
    fn generate() -> Self {
        Self(format!("{:032x}", rand::random::<u128>()))
    }
}

/// Everything needed to open a shard log on demand. Shards are opened
/// lazily on first routed request (COMPUTE-SPEC §5.1): opening fences the
/// previous owner, so ownership follows routing with no coordination.
pub type ShardOpenFuture =
    futures_util::future::BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>>;
pub type ShardOpenFn = dyn Fn(String, String) -> ShardOpenFuture + Send + Sync;

pub struct ShardOpener {
    pub open: Box<ShardOpenFn>,
}

pub struct AppState {
    pub registry: Registry,
    /// Fleet-prefixed store clone for the /operator dashboard's
    /// cell-wide reads (fleet.json, heartbeats, desired, routers).
    /// None when fleet mode is off.
    pub operator_fleet_store: Option<Arc<dyn ObjectStore>>,
    /// Managed multi-cell mode. `None` preserves the legacy single-cell
    /// contract; `Some` requires matching authoritative placement, with
    /// durable source-shard fences covering operator moves.
    pub cell_id: Option<String>,
    pub cell_directory: std::sync::RwLock<Option<crate::cells::CellDirectory>>,
    pub cells_ready: std::sync::atomic::AtomicBool,
    /// Last-known-good topology, replaced atomically by the topology watcher.
    /// Requests never observe a partially parsed or partially applied trie.
    pub topology: std::sync::RwLock<Topology>,
    pub topology_version: std::sync::atomic::AtomicU64,
    pub topology_ready: std::sync::atomic::AtomicBool,
    pub splitting_prefixes: std::sync::RwLock<HashSet<String>>,
    /// Process-local exclusion for split/merge reconcilers. Durable intent
    /// objects are the fleet-wide locks; this prevents an operator request
    /// and scanner from running the same operation concurrently here.
    pub split_workers: std::sync::Mutex<HashSet<String>>,
    pub split_ready: std::sync::atomic::AtomicBool,
    pub merge_ready: std::sync::atomic::AtomicBool,
    /// False while a configured fleet lacks a fresh, valid aggregate view.
    /// The last ring remains installed for fencing-safe outage behavior, but
    /// readiness must expose the degraded control plane.
    pub fleet_ready: std::sync::atomic::AtomicBool,
    /// Immutable build/runtime storage compatibility advertised in fleet
    /// heartbeats and the operator-only canary endpoint.
    pub fleet_capabilities: crate::fleet::FleetCapabilities,
    pub shards: std::sync::RwLock<HashMap<String, Arc<ShardEngine>>>,
    pub opener: ShardOpener,
    /// Serializes shard opens; also carries anti-flap state.
    pub open_lock: tokio::sync::Mutex<HashMap<String, ShardOpenGate>>,
    /// Counts /v1/stream/* requests for the fleet load vector (§4.2).
    pub fleet_ops: std::sync::atomic::AtomicU64,
    /// Concurrently in-flight HTTP requests (all routes) + windowed peak.
    /// THE direct measurement of admitted concurrency: the platform edge
    /// delivers a bounded number of concurrent requests per instance, and
    /// that bound — not CPU — was the run-6/8 per-instance ceiling. The
    /// fleet loop swaps the peak each heartbeat.
    pub inflight: std::sync::atomic::AtomicI64,
    pub inflight_peak: std::sync::atomic::AtomicI64,
    /// §12-lite admission backstop: /v1/stream requests beyond this many
    /// in flight are shed with 429 + Retry-After instead of queueing into
    /// latency collapse (runs 7-9: offered load past capacity turned into
    /// multi-second p50 and timeout churn; shedding holds goodput at
    /// capacity with bounded latency). 0 = off. Health/debug are exempt.
    pub admit_max_inflight: i64,
    /// RSS shed threshold (MB): writes are 429'd while resident memory
    /// exceeds this. Converts cgroup/instance OOM death (docker phase 1:
    /// RSS 218→1030 MB at full throughput, OOMKilled=true) into graceful
    /// backpressure. 0 = off. Sampled every 500 ms into rss_mb_cached.
    pub admit_rss_shed_mb: u64,
    pub rss_mb_cached: std::sync::atomic::AtomicU64,
    /// 429s issued by the admission backstop (observability).
    pub admit_shed: std::sync::atomic::AtomicU64,
    /// Per-customer concurrency isolation. Long polls count for their full
    /// lifetime, preventing one valid tenant from consuming every ingress
    /// slot or all shard-queue capacity on an instance.
    pub tenant_admission: Arc<TenantAdmission>,
    /// Per-stream provisioned append admission. This is a separate bounded
    /// state table so a tenant under its account quota cannot overrun one hot
    /// ordered stream or monopolize its shard.
    pub stream_admission: Arc<StreamAdmission>,
    pub audit: Arc<crate::audit::AuditLog>,
    pub backup: Option<Arc<streams_slate::backup::BackupStatus>>,
    /// This instance's name plus the ring's active instance set, updated by
    /// the fleet loop from desired.json + heartbeat liveness (a selected
    /// instance that has gone heartbeat-dark >30 s is dropped until it
    /// revives). Used for the R2 ring-ownership check: never open a shard
    /// the ring assigns elsewhere, even if a stale router sends it.
    /// Empty = check disabled (fleet mode off or bootstrapping).
    pub instance_name: String,
    pub ring_active: std::sync::RwLock<Vec<String>>,
    pub data_store: Arc<dyn ObjectStore>,
    pub ops_store: Arc<dyn ObjectStore>,
    pub shard_store: Arc<dyn ObjectStore>,
    pub keys: Arc<KeyCache>,
    pub touch: Arc<crate::touch::TouchRegistry>,
    /// Conformance/dev accommodation: used when a request carries no
    /// Stream-Encryption-Key header (the upstream conformance suite cannot
    /// send custom headers). Never set in production.
    pub default_key: Option<String>,
    /// Conformance accommodation: apply this ordering + segment count to
    /// streams created WITHOUT a Stream-Ordering header, so the upstream
    /// suite (which cannot send custom headers) exercises per-key streams.
    pub default_ordering: Option<(String, u32)>,
    /// Bearer token required on /v1/* when set (pilot authn).
    pub authn: Authenticator,
    pub metrics: Arc<crate::metrics::Metrics>,
    /// Exact internal billing stream identity; no user-selected name alone
    /// can opt out of metering or create an exporter feedback loop.
    pub metrics_identity: Option<(String, String)>,
    /// Fixed-label RED metrics for the external monitoring scrape path.
    pub telemetry: Arc<crate::telemetry::Telemetry>,
}

#[derive(Default)]
pub struct ShardOpenGate {
    closed_at: Option<std::time::Instant>,
    failures: u32,
    retry_at: Option<std::time::Instant>,
}

fn quarantine_delay(failures: u32) -> Duration {
    if failures < 3 {
        Duration::ZERO
    } else {
        Duration::from_secs(
            5u64.saturating_mul(1u64 << failures.saturating_sub(3).min(6))
                .min(300),
        )
    }
}

#[derive(Default)]
struct TenantAdmissionInner {
    customers: HashMap<String, TenantAdmissionState>,
}

struct TenantAdmissionState {
    inflight: usize,
    live_connections: usize,
    write_bytes: RateBucket,
    append_requests: RateBucket,
    read_requests: RateBucket,
    read_bytes: RateBucket,
    queue_receives: RateBucket,
    last_seen: std::time::Instant,
}

struct RateBucket {
    tokens: f64,
    rate: u64,
    burst: u64,
    reported_burst: u64,
    observed_scale: u64,
    last_refill: std::time::Instant,
}

impl RateBucket {
    fn new(
        rate: u64,
        burst: u64,
        reported_burst: u64,
        observed_scale: usize,
        now: std::time::Instant,
    ) -> Self {
        let burst = burst.max(1);
        Self {
            tokens: burst as f64,
            rate,
            burst,
            reported_burst,
            observed_scale: observed_scale as u64,
            last_refill: now,
        }
    }

    fn refill(&mut self, now: std::time::Instant) {
        if now < self.last_refill {
            return;
        }
        if self.rate > 0 {
            let refill = now.duration_since(self.last_refill).as_secs_f64() * self.rate as f64;
            self.tokens = (self.tokens + refill).min(self.burst as f64);
        }
        self.last_refill = now;
    }

    fn configure(
        &mut self,
        rate: u64,
        burst: u64,
        reported_burst: u64,
        observed_scale: usize,
        now: std::time::Instant,
    ) {
        let burst = burst.max(1);
        if self.rate == rate
            && self.burst == burst
            && self.reported_burst == reported_burst
            && self.observed_scale == observed_scale as u64
        {
            return;
        }
        self.refill(now);
        self.rate = rate;
        self.burst = burst;
        self.reported_burst = reported_burst;
        self.observed_scale = observed_scale as u64;
        self.tokens = self.tokens.min(burst as f64);
        self.last_refill = now;
    }

    fn try_charge(&mut self, amount: usize, now: std::time::Instant) -> Result<(), (u64, u64)> {
        if self.rate == 0 || amount == 0 {
            return Ok(());
        }
        self.refill(now);
        if amount as f64 > self.tokens {
            let consumed = self.burst as f64 - self.tokens;
            let observed = (consumed + amount as f64).ceil().min(u64::MAX as f64) as u64;
            return Err((
                self.reported_burst,
                observed.saturating_mul(self.observed_scale),
            ));
        }
        self.tokens -= amount as f64;
        Ok(())
    }

    /// Reserve bandwidth in a token bucket. Unlike request admission, egress
    /// waits for capacity so a response that has already sent 200 is not
    /// converted into an abrupt transport error halfway through a frame.
    fn reserve_delay(&mut self, amount: usize, now: std::time::Instant) -> Duration {
        if self.rate == 0 || amount == 0 {
            return Duration::ZERO;
        }
        self.refill(now);
        if amount as f64 <= self.tokens {
            self.tokens -= amount as f64;
            return Duration::ZERO;
        }
        let deficit = amount as f64 - self.tokens;
        self.tokens = 0.0;
        let base = self.last_refill.max(now);
        let delay = Duration::from_secs_f64(deficit / self.rate as f64);
        self.last_refill = base + delay;
        self.last_refill.saturating_duration_since(now)
    }

    fn is_full(&mut self, now: std::time::Instant) -> bool {
        self.refill(now);
        self.rate == 0 || self.tokens + f64::EPSILON >= self.burst as f64
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ThrottleReason {
    scope: &'static str,
    dimension: &'static str,
    limit: u64,
    observed: u64,
}

pub struct TenantAdmission {
    inner: Mutex<TenantAdmissionInner>,
    defaults: TenantAdmissionConfig,
    customer_capacity: usize,
}

#[derive(Clone, Copy)]
pub struct TenantAdmissionConfig {
    pub max_inflight: usize,
    pub max_live_connections: usize,
    pub write_bytes_per_second: u64,
    pub write_burst_bytes: u64,
    pub append_requests_per_second: u64,
    pub append_request_burst: u64,
    pub read_requests_per_second: u64,
    pub read_request_burst: u64,
    pub read_bytes_per_second: u64,
    pub read_burst_bytes: u64,
    pub queue_receives_per_second: u64,
    pub queue_receive_burst: u64,
}

#[derive(Clone, Copy)]
pub struct StreamAdmissionConfig {
    pub append_requests_per_second: u64,
    pub append_request_burst: u64,
    pub write_bytes_per_second: u64,
    pub write_burst_bytes: u64,
    pub commit_weight: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedStreamLimits {
    append_requests_per_second: u64,
    append_request_burst: u64,
    write_bytes_per_second: u64,
    write_burst_bytes: u64,
    commit_weight: u16,
}

struct StreamAdmissionState {
    append_requests: RateBucket,
    write_bytes: RateBucket,
    last_seen: std::time::Instant,
}

#[derive(Default)]
struct StreamAdmissionInner {
    streams: HashMap<crate::registry::StorageHash, StreamAdmissionState>,
}

pub struct StreamAdmission {
    inner: Mutex<StreamAdmissionInner>,
    defaults: StreamAdmissionConfig,
    capacity: usize,
}

impl StreamAdmission {
    pub fn new(defaults: StreamAdmissionConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(StreamAdmissionInner::default()),
            defaults,
            capacity: 100_000,
        })
    }

    fn resolve(&self, descriptor: &StreamDesc) -> ResolvedStreamLimits {
        ResolvedStreamLimits {
            append_requests_per_second: descriptor
                .append_requests_per_second
                .unwrap_or(self.defaults.append_requests_per_second),
            append_request_burst: descriptor
                .append_request_burst
                .unwrap_or(self.defaults.append_request_burst)
                .max(1),
            write_bytes_per_second: descriptor
                .write_bytes_per_second
                .unwrap_or(self.defaults.write_bytes_per_second),
            write_burst_bytes: descriptor
                .write_burst_bytes
                .unwrap_or(self.defaults.write_burst_bytes)
                .max(1),
            commit_weight: descriptor
                .commit_weight
                .unwrap_or(self.defaults.commit_weight)
                .clamp(1, 100),
        }
    }

    fn requested_limits(&self, headers: &HeaderMap) -> Result<ResolvedStreamLimits, &'static str> {
        fn value(
            headers: &HeaderMap,
            name: &'static str,
            default: u64,
            max: u64,
            zero_allowed: bool,
        ) -> Result<u64, &'static str> {
            let Some(raw) = hdr(headers, name) else {
                return Ok(default);
            };
            let parsed = raw
                .parse::<u64>()
                .ok()
                .filter(|value| *value <= max && (zero_allowed || *value > 0));
            parsed.ok_or(name)
        }

        let commit_weight = value(
            headers,
            "stream-commit-weight",
            self.defaults.commit_weight as u64,
            100,
            false,
        )? as u16;
        Ok(ResolvedStreamLimits {
            append_requests_per_second: value(
                headers,
                "stream-append-requests-per-second",
                self.defaults.append_requests_per_second,
                1_000_000_000,
                true,
            )?,
            append_request_burst: value(
                headers,
                "stream-append-request-burst",
                self.defaults.append_request_burst,
                1_000_000_000,
                false,
            )?,
            write_bytes_per_second: value(
                headers,
                "stream-write-bytes-per-second",
                self.defaults.write_bytes_per_second,
                1 << 50,
                true,
            )?,
            write_burst_bytes: value(
                headers,
                "stream-write-burst-bytes",
                self.defaults.write_burst_bytes,
                1 << 50,
                false,
            )?,
            commit_weight,
        })
    }

    fn state_key(descriptor: &StreamDesc) -> crate::registry::StorageHash {
        descriptor.storage_hash()
    }

    fn charge(
        &self,
        descriptor: &StreamDesc,
        requests: usize,
        bytes: usize,
    ) -> Result<(), ThrottleReason> {
        let limits = self.resolve(descriptor);
        let key = Self::state_key(descriptor);
        let now = std::time::Instant::now();
        let mut inner = self.inner.lock().unwrap();
        if !inner.streams.contains_key(&key) && inner.streams.len() >= self.capacity {
            let evict = inner
                .streams
                .iter_mut()
                .filter_map(|(key, state)| {
                    (state.append_requests.is_full(now) && state.write_bytes.is_full(now))
                        .then_some((*key, state.last_seen))
                })
                .min_by_key(|(_, last_seen)| *last_seen)
                .map(|(key, _)| key);
            let Some(evict) = evict else {
                return Err(ThrottleReason {
                    scope: "instance",
                    dimension: "stream_admission_states",
                    limit: self.capacity as u64,
                    observed: self.capacity.saturating_add(1) as u64,
                });
            };
            inner.streams.remove(&evict);
        }
        let state = inner
            .streams
            .entry(key)
            .or_insert_with(|| StreamAdmissionState {
                append_requests: RateBucket::new(
                    limits.append_requests_per_second,
                    limits.append_request_burst,
                    limits.append_request_burst,
                    1,
                    now,
                ),
                write_bytes: RateBucket::new(
                    limits.write_bytes_per_second,
                    limits.write_burst_bytes,
                    limits.write_burst_bytes,
                    1,
                    now,
                ),
                last_seen: now,
            });
        state.append_requests.configure(
            limits.append_requests_per_second,
            limits.append_request_burst,
            limits.append_request_burst,
            1,
            now,
        );
        state.write_bytes.configure(
            limits.write_bytes_per_second,
            limits.write_burst_bytes,
            limits.write_burst_bytes,
            1,
            now,
        );
        state.last_seen = now;
        state
            .append_requests
            .try_charge(requests, now)
            .map_err(|(limit, observed)| ThrottleReason {
                scope: "stream",
                dimension: "append_burst_requests",
                limit,
                observed,
            })?;
        state
            .write_bytes
            .try_charge(bytes, now)
            .map_err(|(limit, observed)| ThrottleReason {
                scope: "stream",
                dimension: "write_burst_bytes",
                limit,
                observed,
            })
    }

    fn charge_request(&self, descriptor: &StreamDesc) -> Result<(), ThrottleReason> {
        self.charge(descriptor, 1, 0)
    }

    fn charge_write(&self, descriptor: &StreamDesc, bytes: usize) -> Result<(), ThrottleReason> {
        self.charge(descriptor, 0, bytes)
    }
}

#[derive(Clone, Copy)]
enum RequestQuota {
    Append,
    Read,
    QueueReceive,
}

fn quota_share(limit: u64, active_instances: usize) -> u64 {
    if limit == 0 {
        0
    } else {
        limit.div_ceil(active_instances.max(1) as u64)
    }
}

fn concurrency_share(limit: usize, active_instances: usize) -> usize {
    if limit == 0 {
        0
    } else {
        limit.div_ceil(active_instances.max(1))
    }
}

impl TenantAdmission {
    pub fn new(defaults: TenantAdmissionConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(TenantAdmissionInner::default()),
            defaults,
            customer_capacity: 100_000,
        })
    }

    fn enter(
        self: &Arc<Self>,
        customer_id: &str,
        limits: &crate::registry::CustomerLimits,
        is_live: bool,
        active_instances: usize,
    ) -> Result<TenantAdmissionGuard, ThrottleReason> {
        let active_instances = active_instances.max(1);
        let max_inflight = limits.max_inflight.unwrap_or(self.defaults.max_inflight);
        let max_live_connections = limits
            .max_live_connections
            .unwrap_or(self.defaults.max_live_connections);
        let write_bytes_per_second = limits
            .write_bytes_per_second
            .unwrap_or(self.defaults.write_bytes_per_second);
        let write_burst_bytes = limits
            .write_burst_bytes
            .unwrap_or(self.defaults.write_burst_bytes)
            .max(1);
        let append_requests_per_second = limits
            .append_requests_per_second
            .unwrap_or(self.defaults.append_requests_per_second);
        let append_request_burst = limits
            .append_request_burst
            .unwrap_or(self.defaults.append_request_burst)
            .max(1);
        let read_requests_per_second = limits
            .read_requests_per_second
            .unwrap_or(self.defaults.read_requests_per_second);
        let read_request_burst = limits
            .read_request_burst
            .unwrap_or(self.defaults.read_request_burst)
            .max(1);
        let read_bytes_per_second = limits
            .read_bytes_per_second
            .unwrap_or(self.defaults.read_bytes_per_second);
        let read_burst_bytes = limits
            .read_burst_bytes
            .unwrap_or(self.defaults.read_burst_bytes)
            .max(1);
        let queue_receives_per_second = limits
            .queue_receives_per_second
            .unwrap_or(self.defaults.queue_receives_per_second);
        let queue_receive_burst = limits
            .queue_receive_burst
            .unwrap_or(self.defaults.queue_receive_burst)
            .max(1);
        let local_max_inflight = concurrency_share(max_inflight, active_instances);
        let local_max_live_connections = concurrency_share(max_live_connections, active_instances);
        let local_write_bytes_per_second = quota_share(write_bytes_per_second, active_instances);
        let local_write_burst_bytes = quota_share(write_burst_bytes, active_instances);
        let local_append_requests_per_second =
            quota_share(append_requests_per_second, active_instances);
        let local_append_request_burst = quota_share(append_request_burst, active_instances);
        let local_read_requests_per_second =
            quota_share(read_requests_per_second, active_instances);
        let local_read_request_burst = quota_share(read_request_burst, active_instances);
        let local_read_bytes_per_second = quota_share(read_bytes_per_second, active_instances);
        let local_read_burst_bytes = quota_share(read_burst_bytes, active_instances);
        let local_queue_receives_per_second =
            quota_share(queue_receives_per_second, active_instances);
        let local_queue_receive_burst = quota_share(queue_receive_burst, active_instances);
        let mut inner = self.inner.lock().unwrap();
        if !inner.customers.contains_key(customer_id)
            && inner.customers.len() >= self.customer_capacity
        {
            let Some(evict) = inner
                .customers
                .iter()
                .filter(|(_, state)| state.inflight == 0)
                .min_by_key(|(_, state)| state.last_seen)
                .map(|(customer, _)| customer.clone())
            else {
                return Err(ThrottleReason {
                    scope: "instance",
                    dimension: "customer_admission_states",
                    limit: self.customer_capacity as u64,
                    observed: self.customer_capacity.saturating_add(1) as u64,
                });
            };
            inner.customers.remove(&evict);
        }
        let now = std::time::Instant::now();
        let state = inner
            .customers
            .entry(customer_id.to_string())
            .or_insert_with(|| TenantAdmissionState {
                inflight: 0,
                live_connections: 0,
                write_bytes: RateBucket::new(
                    local_write_bytes_per_second,
                    local_write_burst_bytes,
                    write_burst_bytes,
                    active_instances,
                    now,
                ),
                append_requests: RateBucket::new(
                    local_append_requests_per_second,
                    local_append_request_burst,
                    append_request_burst,
                    active_instances,
                    now,
                ),
                read_requests: RateBucket::new(
                    local_read_requests_per_second,
                    local_read_request_burst,
                    read_request_burst,
                    active_instances,
                    now,
                ),
                read_bytes: RateBucket::new(
                    local_read_bytes_per_second,
                    local_read_burst_bytes,
                    read_burst_bytes,
                    active_instances,
                    now,
                ),
                queue_receives: RateBucket::new(
                    local_queue_receives_per_second,
                    local_queue_receive_burst,
                    queue_receive_burst,
                    active_instances,
                    now,
                ),
                last_seen: now,
            });
        state.write_bytes.configure(
            local_write_bytes_per_second,
            local_write_burst_bytes,
            write_burst_bytes,
            active_instances,
            now,
        );
        state.append_requests.configure(
            local_append_requests_per_second,
            local_append_request_burst,
            append_request_burst,
            active_instances,
            now,
        );
        state.read_requests.configure(
            local_read_requests_per_second,
            local_read_request_burst,
            read_request_burst,
            active_instances,
            now,
        );
        state.read_bytes.configure(
            local_read_bytes_per_second,
            local_read_burst_bytes,
            read_burst_bytes,
            active_instances,
            now,
        );
        state.queue_receives.configure(
            local_queue_receives_per_second,
            local_queue_receive_burst,
            queue_receive_burst,
            active_instances,
            now,
        );
        if local_max_inflight > 0 && state.inflight >= local_max_inflight {
            return Err(ThrottleReason {
                scope: "customer",
                dimension: "connections",
                limit: max_inflight as u64,
                observed: (state.inflight.saturating_add(1) as u64)
                    .saturating_mul(active_instances as u64),
            });
        }
        if is_live
            && local_max_live_connections > 0
            && state.live_connections >= local_max_live_connections
        {
            return Err(ThrottleReason {
                scope: "customer",
                dimension: "live_connections",
                limit: max_live_connections as u64,
                observed: (state.live_connections.saturating_add(1) as u64)
                    .saturating_mul(active_instances as u64),
            });
        }
        state.inflight += 1;
        if is_live {
            state.live_connections += 1;
        }
        state.last_seen = now;
        Ok(TenantAdmissionGuard {
            admission: Some(self.clone()),
            customer_id: customer_id.to_string(),
            is_live,
        })
    }

    fn charge_request(&self, customer_id: &str, quota: RequestQuota) -> Result<(), ThrottleReason> {
        let mut inner = self.inner.lock().unwrap();
        let Some(state) = inner.customers.get_mut(customer_id) else {
            return Err(ThrottleReason {
                scope: "instance",
                dimension: "customer_admission_states",
                limit: self.customer_capacity as u64,
                observed: self.customer_capacity.saturating_add(1) as u64,
            });
        };
        let (bucket, dimension) = match quota {
            RequestQuota::Append => (&mut state.append_requests, "append_burst_requests"),
            RequestQuota::Read => (&mut state.read_requests, "read_burst_requests"),
            RequestQuota::QueueReceive => {
                (&mut state.queue_receives, "queue_receive_burst_requests")
            }
        };
        let now = std::time::Instant::now();
        let result = bucket.try_charge(1, now);
        state.last_seen = now;
        result.map_err(|(limit, observed)| ThrottleReason {
            scope: "customer",
            dimension,
            limit,
            observed,
        })
    }

    fn charge_write(&self, customer_id: &str, bytes: usize) -> Result<(), ThrottleReason> {
        let mut inner = self.inner.lock().unwrap();
        let Some(state) = inner.customers.get_mut(customer_id) else {
            return if bytes == 0 {
                Ok(())
            } else {
                Err(ThrottleReason {
                    scope: "instance",
                    dimension: "customer_admission_states",
                    limit: self.customer_capacity as u64,
                    observed: self.customer_capacity.saturating_add(1) as u64,
                })
            };
        };
        let now = std::time::Instant::now();
        state.last_seen = now;
        state
            .write_bytes
            .try_charge(bytes, now)
            .map_err(|(limit, observed)| ThrottleReason {
                scope: "customer",
                dimension: "write_burst_bytes",
                limit,
                observed,
            })
    }

    fn reserve_read_delay(&self, customer_id: &str, bytes: usize) -> Duration {
        let mut inner = self.inner.lock().unwrap();
        let Some(state) = inner.customers.get_mut(customer_id) else {
            // The response guard pins its state, so absence is only possible
            // for an internal misuse. Do not turn that into unlimited egress.
            return Duration::from_secs(1);
        };
        let now = std::time::Instant::now();
        let delay = state.read_bytes.reserve_delay(bytes, now);
        state.last_seen = now;
        delay
    }
}

struct TenantAdmissionGuard {
    admission: Option<Arc<TenantAdmission>>,
    customer_id: String,
    is_live: bool,
}

impl Drop for TenantAdmissionGuard {
    fn drop(&mut self) {
        let Some(admission) = &self.admission else {
            return;
        };
        let mut inner = admission.inner.lock().unwrap();
        if let Some(state) = inner.customers.get_mut(&self.customer_id) {
            state.inflight = state.inflight.saturating_sub(1);
            if self.is_live {
                state.live_connections = state.live_connections.saturating_sub(1);
            }
            state.last_seen = std::time::Instant::now();
        }
    }
}

/// Owns a tenant admission guard until the response body is fully consumed or
/// abandoned. Handler futures complete as soon as they construct a response,
/// which is much earlier than an SSE or other streaming body closes.
struct TenantAdmissionBody {
    inner: Body,
    guard: TenantAdmissionGuard,
    meter_egress: bool,
    pending_frame: Option<http_body::Frame<Bytes>>,
    delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl HttpBody for TenantAdmissionBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if let Some(delay) = &mut this.delay {
            if delay.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            this.delay = None;
            return Poll::Ready(this.pending_frame.take().map(Ok));
        }

        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) if this.meter_egress => {
                let bytes = frame.data_ref().map_or(0, Bytes::len);
                let delay = this
                    .guard
                    .admission
                    .as_ref()
                    .map(|admission| admission.reserve_read_delay(&this.guard.customer_id, bytes))
                    .unwrap_or_default();
                if delay.is_zero() {
                    Poll::Ready(Some(Ok(frame)))
                } else {
                    this.pending_frame = Some(frame);
                    let mut sleep = Box::pin(tokio::time::sleep(delay));
                    let result = sleep.as_mut().poll(cx);
                    this.delay = Some(sleep);
                    debug_assert!(result.is_pending());
                    Poll::Pending
                }
            }
            result => result,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn hold_tenant_admission(
    response: Response,
    guard: TenantAdmissionGuard,
    meter_egress: bool,
) -> Response {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(TenantAdmissionBody {
            inner: body,
            guard,
            meter_egress,
            pending_frame: None,
            delay: None,
        }),
    )
}

async fn body_with_quota(
    body: Body,
    state: &Arc<AppState>,
    customer_id: &str,
    limit: usize,
    stream: Option<&StreamDesc>,
) -> Result<Bytes, Response> {
    let mut body_stream = body.into_data_stream();
    let mut buffered = BytesMut::new();
    while let Some(chunk) = body_stream.next().await {
        let chunk = chunk.map_err(|_| {
            err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_body",
                "failed to read body",
            )
        })?;
        if buffered.len().saturating_add(chunk.len()) > limit {
            return Err(err_resp(
                StatusCode::PAYLOAD_TOO_LARGE,
                "too_large",
                "body too large",
            ));
        }
        if let Err(reason) = state
            .tenant_admission
            .charge_write(customer_id, chunk.len())
        {
            state
                .admit_shed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(throttled_resp(reason, 1));
        }
        if let Some(stream) = stream
            && let Err(reason) = state.stream_admission.charge_write(stream, chunk.len())
        {
            state
                .admit_shed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(throttled_resp(reason, 1));
        }
        buffered.extend_from_slice(&chunk);
    }
    Ok(buffered.freeze())
}

async fn admit_customer(
    state: &Arc<AppState>,
    customer_id: &str,
    quota: Option<RequestQuota>,
    is_live: bool,
) -> Result<(TenantAdmissionGuard, crate::registry::CustomerLimits), Response> {
    let limits = state
        .registry
        .customer_limits(customer_id)
        .await
        .map_err(|error| {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "limits_unavailable",
                &format!("customer limits unavailable: {error}"),
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            response
        })?;
    let guard = state
        .tenant_admission
        .enter(
            customer_id,
            &limits,
            is_live,
            state.ring_active.read().unwrap().len().max(1),
        )
        .map_err(|reason| {
            state
                .admit_shed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            throttled_resp(reason, 1)
        })?;
    if let Some(quota) = quota
        && let Err(reason) = state.tenant_admission.charge_request(customer_id, quota)
    {
        state
            .admit_shed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Err(throttled_resp(reason, 1));
    }
    Ok((guard, limits))
}

fn authorization(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

#[allow(clippy::result_large_err)]
fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<Principal, Response> {
    state
        .authn
        .authenticate(authorization(headers))
        .map_err(|error| match error {
            AuthError::Unavailable => {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "auth_unavailable",
                    "authentication keys unavailable",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("5"),
                );
                response
            }
            AuthError::Missing | AuthError::Invalid => err_resp(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "valid bearer token required",
            ),
        })
}

fn forbidden() -> Response {
    err_resp(
        StatusCode::FORBIDDEN,
        "forbidden",
        "token does not grant this operation",
    )
}

impl AppState {
    fn should_meter(&self, customer: &str, stream: &str) -> bool {
        self.metrics_identity
            .as_ref()
            .is_none_or(|(internal_customer, internal_stream)| {
                customer != internal_customer || stream != internal_stream
            })
    }

    /// Shard engine for `hash`, opening the shard log on first use (which
    /// fences any previous owner). A shard that was just fenced away is
    /// held off for 3 s (anti-flap while the router converges) → 503.
    async fn engine_for(self: &Arc<Self>, hash: &[u8; 16]) -> Result<Arc<ShardEngine>, Response> {
        let (prefix, db_path) = {
            let topology = self.topology.read().unwrap();
            let prefix = shard_for_hash(&topology.shards, hash);
            let path = topology.db_path(&prefix);
            (prefix, path)
        };
        if self.splitting_prefixes.read().unwrap().contains(&prefix) {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_splitting",
                "shard is crossing a durable split barrier; retry",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            return Err(response);
        }
        // Check placement before consulting the local engine cache. Ring
        // changes must stop a former owner immediately; the old order let a
        // cached engine bypass this guard indefinitely.
        let active = self.ring_active.read().unwrap().clone();
        if !active.is_empty() && !self.instance_name.is_empty() {
            let owner = active[ring_pick(&prefix, &active)].clone();
            if owner != self.instance_name {
                let mut response = err_resp(
                    StatusCode::CONFLICT,
                    "not_ring_owner",
                    &format!("shard {prefix} belongs to {owner}"),
                );
                if let Ok(value) = axum::http::HeaderValue::from_str(&owner) {
                    response.headers_mut().insert("streams-replay-to", value);
                }
                return Err(response);
            }
        }
        let cached = { self.shards.read().unwrap().get(&prefix).cloned() };
        if let Some(engine) = cached {
            if let Err(error) = engine.prove_ownership().await {
                return Err(storage_err_resp(error));
            }
            return Ok(engine);
        }
        let mut lock = self.open_lock.lock().await;
        let raced = { self.shards.read().unwrap().get(&prefix).cloned() };
        if let Some(engine) = raced {
            drop(lock);
            if let Err(error) = engine.prove_ownership().await {
                return Err(storage_err_resp(error));
            }
            return Ok(engine); // raced: someone opened it while we waited
        }
        if let Some(gate) = lock.get(&prefix) {
            if gate
                .closed_at
                .is_some_and(|closed_at| closed_at.elapsed() < Duration::from_secs(3))
            {
                return Err(err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_moving",
                    "shard recently fenced away; retry",
                ));
            }
            if gate
                .retry_at
                .is_some_and(|retry_at| retry_at > std::time::Instant::now())
            {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_quarantined",
                    "shard open is quarantined after repeated failures; retry later",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                return Err(response);
            }
        }
        match (self.opener.open)(prefix.clone(), db_path).await {
            Ok(engine) => {
                lock.remove(&prefix);
                self.shards.write().unwrap().insert(prefix, engine.clone());
                Ok(engine)
            }
            Err(e) => {
                let gate = lock.entry(prefix.clone()).or_default();
                gate.failures = gate.failures.saturating_add(1);
                let delay = quarantine_delay(gate.failures);
                gate.retry_at = (!delay.is_zero()).then(|| std::time::Instant::now() + delay);
                if !delay.is_zero() {
                    tracing::error!(
                        shard = %prefix,
                        failures = gate.failures,
                        retry_after_s = delay.as_secs(),
                        "poison shard quarantined after open failure: {e:#}"
                    );
                }
                Err(err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_open",
                    &format!("open shard {prefix}: {e}"),
                ))
            }
        }
    }

    /// Called when a shard db closes (fenced by a new owner): drop it from
    /// the serving map and start the anti-flap holdoff.
    pub fn shard_closed(self: &Arc<Self>, prefix: &str) {
        self.shards.write().unwrap().remove(prefix);
        if let Ok(mut l) = self.open_lock.try_lock() {
            l.entry(prefix.to_string()).or_default().closed_at = Some(std::time::Instant::now());
        } else {
            let state = self.clone();
            let prefix = prefix.to_string();
            tokio::spawn(async move {
                state
                    .open_lock
                    .lock()
                    .await
                    .entry(prefix)
                    .or_default()
                    .closed_at = Some(std::time::Instant::now());
            });
        }
    }
}

/// RAII in-flight counter: decrements on response AND on cancel/panic.
struct InflightGuard(Arc<AppState>);
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0
            .inflight
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn track_inflight(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let cur = state
        .inflight
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    state
        .inflight_peak
        .fetch_max(cur, std::sync::atomic::Ordering::Relaxed);
    let _guard = InflightGuard(state.clone());
    let path_is_stream = req.uri().path().starts_with("/v1/stream");
    if state.admit_max_inflight > 0 && cur > state.admit_max_inflight && path_is_stream {
        state
            .admit_shed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Tarpit: a ~25 ms pause before the 429 bounds the reject rate a
        // non-compliant closed-loop client can generate (an instant 429
        // invites an instant retry — measured as a CPU-starving reject
        // storm). Compliant clients never see this path twice in a row.
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        return throttled_resp(
            ThrottleReason {
                scope: "instance",
                dimension: "connections",
                limit: state.admit_max_inflight as u64,
                observed: u64::try_from(cur).unwrap_or(u64::MAX),
            },
            1,
        );
    }
    // RSS shed: writes only — reads don't grow memtables, and rejecting
    // them would hide the instance from its own operators.
    if state.admit_rss_shed_mb > 0
        && path_is_stream
        && req.method() != axum::http::Method::GET
        && state
            .rss_mb_cached
            .load(std::sync::atomic::Ordering::Relaxed)
            > state.admit_rss_shed_mb
    {
        state
            .admit_shed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        return throttled_resp(
            ThrottleReason {
                scope: "instance",
                dimension: "memory_bytes",
                limit: state.admit_rss_shed_mb.saturating_mul(1024 * 1024),
                observed: state
                    .rss_mb_cached
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .saturating_mul(1024 * 1024),
            },
            2,
        );
    }
    next.run(req).await
}

async fn record_http_telemetry(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let operation = crate::telemetry::Telemetry::classify(req.method(), req.uri());
    let started = std::time::Instant::now();
    let response = next.run(req).await;
    state
        .telemetry
        .record(operation, response.status(), started.elapsed());
    response
}

async fn assign_request_id(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    // Never accept a caller-selected correlation identifier: audit IDs are
    // service assertions and must not collide or contain attacker text.
    req.headers_mut().remove(REQUEST_ID_HEADER);
    let request_id = RequestId::generate();
    req.extensions_mut().insert(request_id.clone());
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        axum::http::HeaderValue::from_str(&request_id.0)
            .expect("generated request ID is a valid header value"),
    );
    response
}

fn operator_unauthorized() -> Response {
    err_resp(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "operator token required",
    )
}

fn operator_approval_required() -> Response {
    err_resp(
        StatusCode::FORBIDDEN,
        "operator_approval_required",
        "production admin operations require a distinct second operator approval",
    )
}

struct OperatorApprovalRejection {
    response: Response,
    /// Present only when the second JWT passed signature, issuer, audience,
    /// lifetime, and revocation validation. It is safe to include in the
    /// denied-attempt audit even when it lacked operator authority or reused
    /// the primary person's identity.
    principal: Option<Principal>,
}

#[allow(clippy::result_large_err)]
fn authenticate_operator_approval(
    state: &AppState,
    headers: &HeaderMap,
    primary: &Principal,
) -> Result<Option<Principal>, OperatorApprovalRejection> {
    if !state.authn.production_ready() {
        return Ok(None);
    }
    let Some(authorization) = headers
        .get(OPERATOR_APPROVAL_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(OperatorApprovalRejection {
            response: operator_approval_required(),
            principal: None,
        });
    };
    let approval = match state.authn.authenticate(Some(authorization)) {
        Ok(principal) => principal,
        Err(AuthError::Unavailable) => {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth_unavailable",
                "authentication keys unavailable",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("5"),
            );
            return Err(OperatorApprovalRejection {
                response,
                principal: None,
            });
        }
        Err(AuthError::Missing | AuthError::Invalid) => {
            return Err(OperatorApprovalRejection {
                response: operator_approval_required(),
                principal: None,
            });
        }
    };
    if !approval.operator
        || approval.customer_id == primary.customer_id
        || approval.token_id == primary.token_id
    {
        return Err(OperatorApprovalRejection {
            response: operator_approval_required(),
            principal: Some(approval),
        });
    }
    Ok(Some(approval))
}

fn operator_audit_event(
    request_id: &RequestId,
    principal: &Principal,
    approval: Option<&Principal>,
    path: &str,
    method: &Method,
    status: StatusCode,
    duration: Duration,
) -> crate::audit::AuditEvent {
    crate::audit::AuditEvent {
        format_version: 1,
        request_id: request_id.0.clone(),
        timestamp_ms: now_ms(),
        customer_id: principal.customer_id.clone(),
        token_id: principal.token_id.clone(),
        approval_customer_id: approval.map(|principal| principal.customer_id.clone()),
        approval_token_id: approval.map(|principal| principal.token_id.clone()),
        stream: path.to_string(),
        method: method.to_string(),
        status: status.as_u16(),
        duration_us: duration.as_micros().min(u64::MAX as u128) as u64,
    }
}

fn audit_unavailable() -> Response {
    let mut response = err_resp(
        StatusCode::SERVICE_UNAVAILABLE,
        "audit_unavailable",
        "operation completed but its audit record could not be accepted; retry",
    );
    response.headers_mut().insert(
        header::RETRY_AFTER,
        axum::http::HeaderValue::from_static("1"),
    );
    response
}

/// Authenticate and audit the complete privileged HTTP surface in one route
/// layer. Debug reads are full fidelity but batched; state-changing admin
/// calls synchronously dual-write their result. Query strings are deliberately
/// excluded, while the bounded path retains an admin call's exact shard target.
async fn audit_operator_request(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let request_id = req
        .extensions()
        .get::<RequestId>()
        .cloned()
        .expect("request ID middleware must wrap operator routes");
    let principal = match authenticate(&state, req.headers()) {
        Ok(principal) if principal.operator => principal,
        Ok(_) => return operator_unauthorized(),
        Err(response) => return response,
    };
    let started = std::time::Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let durable = path.starts_with("/v1/admin/");
    let approval = if durable {
        match authenticate_operator_approval(&state, req.headers(), &principal) {
            Ok(approval) => approval,
            Err(rejection) => {
                let event = operator_audit_event(
                    &request_id,
                    &principal,
                    rejection.principal.as_ref(),
                    &path,
                    &method,
                    rejection.response.status(),
                    started.elapsed(),
                );
                if let Err(error) = state.audit.record_durable(&event).await {
                    tracing::error!(
                        customer_id = %principal.customer_id,
                        token_id = %principal.token_id,
                        resource = %path,
                        method = %method,
                        "denied admin request audit failed: {error}"
                    );
                } else {
                    tracing::warn!(
                        target: "streams_audit",
                        customer_id = %principal.customer_id,
                        token_id = %principal.token_id,
                        approval_customer_id = rejection.principal.as_ref().map(|principal| principal.customer_id.as_str()),
                        approval_token_id = rejection.principal.as_ref().map(|principal| principal.token_id.as_str()),
                        resource = %path,
                        method = %method,
                        status = rejection.response.status().as_u16(),
                        "denied admin request audit"
                    );
                }
                return rejection.response;
            }
        }
    } else {
        None
    };
    let response = next.run(req).await;
    let event = operator_audit_event(
        &request_id,
        &principal,
        approval.as_ref(),
        &path,
        &method,
        response.status(),
        started.elapsed(),
    );
    let audit_result = if durable {
        state.audit.record_durable(&event).await
    } else {
        state.audit.record_operator_read(event)
    };
    if let Err(error) = audit_result {
        tracing::error!(
            customer_id = %principal.customer_id,
            token_id = %principal.token_id,
            approval_customer_id = approval.as_ref().map(|principal| principal.customer_id.as_str()),
            approval_token_id = approval.as_ref().map(|principal| principal.token_id.as_str()),
            resource = %path,
            method = %method,
            durable,
            "privileged request audit failed: {error}"
        );
        if response.status().is_success() {
            return audit_unavailable();
        }
    } else {
        tracing::info!(
            target: "streams_audit",
            customer_id = %principal.customer_id,
            token_id = %principal.token_id,
            approval_customer_id = approval.as_ref().map(|principal| principal.customer_id.as_str()),
            approval_token_id = approval.as_ref().map(|principal| principal.token_id.as_str()),
            resource = %path,
            method = %method,
            status = response.status().as_u16(),
            duration_us = started.elapsed().as_micros() as u64,
            durable,
            "privileged request audit"
        );
    }
    response
}

/// Calibrated-latency endpoint for edge probes: holds the request for
/// ?ms= milliseconds doing no engine work. Lets a probe separate an
/// admitted-concurrency cap (rate = slots/latency) from a rate cap
/// (rate constant regardless of latency).
async fn debug_sleep(
    State(_state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let ms: u64 = q
        .get("ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
        .min(5_000);
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    "ok".into_response()
}

/// Live resource gauge for probes: in-flight now, peak since last call,
/// and RSS.
async fn debug_load(State(state): State<Arc<AppState>>) -> Response {
    let now = state.inflight.load(std::sync::atomic::Ordering::Relaxed);
    let peak = state
        .inflight_peak
        .swap(now, std::sync::atomic::Ordering::Relaxed);
    axum::Json(serde_json::json!({
        "inflight_now": now,
        "inflight_peak": peak,
        "rss_mb": crate::fleet::rss_bytes() as f64 / 1048576.0,
        "admit_shed": state.admit_shed.load(std::sync::atomic::Ordering::Relaxed),
        "audit_dropped": state.audit.dropped(),
    }))
    .into_response()
}

/// Bounded compatibility evidence for mixed-version deployment canaries.
/// This deliberately exposes no tenant identifiers, shard names, load, or
/// storage paths. A heartbeat produced by an old binary (or republished by an
/// old aggregator) appears with an all-zero capability envelope, allowing the
/// rollout judge to stop rather than infer compatibility from a release tag.
async fn debug_capabilities(State(state): State<Arc<AppState>>) -> Response {
    let mut fleet = crate::fleet::live_heartbeats();
    fleet.sort_unstable_by(|left, right| left.instance.cmp(&right.instance));
    let fleet: Vec<_> = fleet
        .into_iter()
        .map(|heartbeat| {
            json!({
                "instance": heartbeat.instance,
                "ts_ms": heartbeat.ts_ms,
                "draining": heartbeat.draining,
                "cell_move_protocol": heartbeat.cell_move_protocol,
                "capabilities": heartbeat.capabilities,
            })
        })
        .collect();
    axum::Json(json!({
        "format_version": 1,
        "observed_at_ms": now_ms(),
        "local": {
            "instance": state.instance_name,
            "cell_move_protocol": crate::cell_move_fence::PROTOCOL_VERSION,
            "capabilities": &state.fleet_capabilities,
        },
        "aggregate_ready": state.fleet_ready.load(std::sync::atomic::Ordering::Acquire),
        "fleet": fleet,
    }))
    .into_response()
}

/// Object-store client latency snapshot (O14a): per (op, path-class)
/// percentiles over ?window= seconds (default 60), the slow-op ring, and
/// the outbound in-flight gauge. ?swap=1 resets the peak (sampler only —
/// heartbeats read it non-destructively).
async fn debug_store(
    State(_state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    let window: u64 = q
        .get("window")
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
        .clamp(1, 300);
    let swap = q.get("swap").map(|v| v == "1").unwrap_or(false);
    axum::Json(crate::store_timing::snapshot(window, swap)).into_response()
}

fn metric_bool(out: &mut String, name: &str, labels: &str, value: bool) {
    out.push_str(name);
    out.push_str(labels);
    out.push(' ');
    out.push_str(if value { "1\n" } else { "0\n" });
}

fn render_operational_metrics(state: &AppState) -> String {
    let mut out = String::with_capacity(32 * 1024);
    let instance_hash = crate::crypto::hex(&crate::crypto::stream_hash(&state.instance_name));
    out.push_str("# HELP streams_instance_info Pseudonymous stable instance identity.\n");
    out.push_str("# TYPE streams_instance_info gauge\n");
    out.push_str(&format!(
        "streams_instance_info{{instance_hash=\"{instance_hash}\"}} 1\n"
    ));
    out.push_str("# HELP streams_fleet_active_instances Instances in the installed active ring.\n");
    out.push_str("# TYPE streams_fleet_active_instances gauge\n");
    out.push_str(&format!(
        "streams_fleet_active_instances {}\n",
        state.ring_active.read().unwrap().len().max(1)
    ));
    state.telemetry.render_openmetrics(&mut out);

    out.push_str(
        "# HELP streams_component_ready Whether a serving dependency is currently healthy.\n",
    );
    out.push_str("# TYPE streams_component_ready gauge\n");
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"auth\"}",
        state.authn.ready(),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"audit\"}",
        state.audit.ready(),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"absorber\"}",
        state.telemetry.absorber_healthy(),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"topology\"}",
        state
            .topology_ready
            .load(std::sync::atomic::Ordering::Acquire),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"split\"}",
        state.split_ready.load(std::sync::atomic::Ordering::Acquire),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"merge\"}",
        state.merge_ready.load(std::sync::atomic::Ordering::Acquire),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"fleet\"}",
        state.fleet_ready.load(std::sync::atomic::Ordering::Acquire),
    );
    metric_bool(
        &mut out,
        "streams_component_ready",
        "{component=\"cells\"}",
        state.cells_ready.load(std::sync::atomic::Ordering::Acquire),
    );

    out.push_str("# HELP streams_backup_configured Whether independent recovery is configured.\n");
    out.push_str("# TYPE streams_backup_configured gauge\n");
    metric_bool(
        &mut out,
        "streams_backup_configured",
        "",
        state.backup.is_some(),
    );
    out.push_str("# HELP streams_backup_component_ready Recovery point and scrub health.\n");
    out.push_str("# TYPE streams_backup_component_ready gauge\n");
    let backup = state.backup.as_ref().map(|status| status.health());
    for (component, ready) in [
        ("snapshot", backup.is_none_or(|health| health.snapshot)),
        (
            "recovery_scrub",
            backup.is_none_or(|health| health.recovery_scrub),
        ),
        (
            "primary_scrub",
            backup.is_none_or(|health| health.primary_scrub),
        ),
    ] {
        metric_bool(
            &mut out,
            "streams_backup_component_ready",
            &format!("{{component=\"{component}\"}}"),
            ready,
        );
    }
    out.push_str("# HELP streams_backup_recovery_point_age_seconds Conservative monotonic age of the newest fully protected recovery point.\n");
    out.push_str("# TYPE streams_backup_recovery_point_age_seconds gauge\n");
    match state
        .backup
        .as_ref()
        .and_then(|status| status.recovery_point_age())
    {
        Some(age) => out.push_str(&format!(
            "streams_backup_recovery_point_age_seconds {:.3}\n",
            age.as_secs_f64()
        )),
        None => out.push_str("streams_backup_recovery_point_age_seconds +Inf\n"),
    }
    out.push_str("# HELP streams_backup_rpo_budget_seconds Configured maximum protected recovery-point age.\n");
    out.push_str("# TYPE streams_backup_rpo_budget_seconds gauge\n");
    out.push_str(&format!(
        "streams_backup_rpo_budget_seconds {}\n",
        state
            .backup
            .as_ref()
            .map_or(0, |status| status.rpo_budget().as_secs())
    ));

    out.push_str(
        "# HELP streams_audit_dropped_total Sampled audit records dropped at the bounded queue.\n",
    );
    out.push_str("# TYPE streams_audit_dropped_total counter\n");
    out.push_str(&format!(
        "streams_audit_dropped_total {}\n",
        state.audit.dropped()
    ));
    out.push_str(
        "# HELP streams_audit_mirror_configured Whether audit events use an independent mirror.\n",
    );
    out.push_str("# TYPE streams_audit_mirror_configured gauge\n");
    metric_bool(
        &mut out,
        "streams_audit_mirror_configured",
        "",
        state.audit.mirror_configured(),
    );
    out.push_str("# HELP streams_billing_export_configured Whether the encrypted billing stream exporter is enabled.\n");
    out.push_str("# TYPE streams_billing_export_configured gauge\n");
    metric_bool(
        &mut out,
        "streams_billing_export_configured",
        "",
        state.metrics.export_configured(),
    );
    out.push_str(
        "# HELP streams_billing_export_healthy Whether the most recent export attempt succeeded.\n",
    );
    out.push_str("# TYPE streams_billing_export_healthy gauge\n");
    metric_bool(
        &mut out,
        "streams_billing_export_healthy",
        "",
        state.metrics.export_healthy(),
    );
    out.push_str("# HELP streams_billing_export_failures_total Failed retry-stable billing export attempts.\n");
    out.push_str("# TYPE streams_billing_export_failures_total counter\n");
    out.push_str(&format!(
        "streams_billing_export_failures_total {}\n",
        state.metrics.export_failures()
    ));
    out.push_str("# HELP streams_billing_dropped_series_total Billing series rejected at the cardinality bound.\n");
    out.push_str("# TYPE streams_billing_dropped_series_total counter\n");
    out.push_str(&format!(
        "streams_billing_dropped_series_total {}\n",
        state.metrics.dropped_series_total()
    ));
    out.push_str("# HELP streams_admission_shed_total Instance-level overload responses.\n");
    out.push_str("# TYPE streams_admission_shed_total counter\n");
    out.push_str(&format!(
        "streams_admission_shed_total {}\n",
        state.admit_shed.load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str("# HELP streams_inflight_requests Currently executing HTTP requests.\n");
    out.push_str("# TYPE streams_inflight_requests gauge\n");
    out.push_str(&format!(
        "streams_inflight_requests {}\n",
        state.inflight.load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP streams_process_resident_memory_bytes Resident memory observed by the process.\n",
    );
    out.push_str("# TYPE streams_process_resident_memory_bytes gauge\n");
    out.push_str(&format!(
        "streams_process_resident_memory_bytes {}\n",
        crate::fleet::rss_bytes()
    ));
    out.push_str("# HELP streams_process_cpu_seconds_total Process user and system CPU time.\n");
    out.push_str("# TYPE streams_process_cpu_seconds_total counter\n");
    out.push_str(&format!(
        "streams_process_cpu_seconds_total {:.6}\n",
        crate::fleet::cpu_time_secs()
    ));

    let (wal_p50_ms, wal_p99_ms, store_inflight, store_inflight_peak) =
        crate::store_timing::heartbeat_summary();
    out.push_str(
        "# HELP streams_wal_put_latency_seconds Recent object-store WAL PUT latency quantiles.\n",
    );
    out.push_str("# TYPE streams_wal_put_latency_seconds gauge\n");
    out.push_str(&format!(
        "streams_wal_put_latency_seconds{{quantile=\"0.50\"}} {:.6}\n",
        wal_p50_ms as f64 / 1_000.0
    ));
    out.push_str(&format!(
        "streams_wal_put_latency_seconds{{quantile=\"0.99\"}} {:.6}\n",
        wal_p99_ms as f64 / 1_000.0
    ));
    out.push_str("# HELP streams_object_store_inflight Outbound object-store operations.\n");
    out.push_str("# TYPE streams_object_store_inflight gauge\n");
    out.push_str(&format!(
        "streams_object_store_inflight{{kind=\"current\"}} {store_inflight}\n"
    ));
    out.push_str(&format!(
        "streams_object_store_inflight{{kind=\"peak\"}} {store_inflight_peak}\n"
    ));
    out.push_str(
        "# HELP streams_object_store_operations_total Finished outbound object-store operation attempts.\n",
    );
    out.push_str("# TYPE streams_object_store_operations_total counter\n");
    for (operation, class, total) in crate::store_timing::operation_totals() {
        out.push_str(&format!(
            "streams_object_store_operations_total{{operation=\"{operation}\",class=\"{class}\"}} {total}\n"
        ));
    }

    out.push_str("# HELP streams_open_shards Shard engines currently resident in this process.\n");
    out.push_str("# TYPE streams_open_shards gauge\n");
    let engines: Vec<(String, Arc<ShardEngine>)> = state
        .shards
        .read()
        .unwrap()
        .iter()
        .map(|(prefix, engine)| (prefix.clone(), engine.clone()))
        .collect();
    out.push_str(&format!("streams_open_shards {}\n", engines.len()));
    out.push_str("# HELP streams_shard_durable_wait_p99_seconds Recent per-shard remote durability wait p99.\n");
    out.push_str("# TYPE streams_shard_durable_wait_p99_seconds gauge\n");
    out.push_str("# HELP streams_shard_appended_records_total Records committed by the current shard writer.\n");
    out.push_str("# TYPE streams_shard_appended_records_total counter\n");
    out.push_str("# HELP streams_shard_l0_ssts Immutable L0 tables awaiting or participating in compaction.\n");
    out.push_str("# TYPE streams_shard_l0_ssts gauge\n");
    out.push_str("# HELP streams_shard_unflushed_wal_ssts Manifest-known WAL tables not yet covered by the replay watermark.\n");
    out.push_str("# TYPE streams_shard_unflushed_wal_ssts gauge\n");
    let cutoff = now_ms().saturating_sub(15_000);
    for (prefix, engine) in engines {
        let shard = if prefix.is_empty() {
            "root"
        } else {
            prefix.as_str()
        };
        let mut waits: Vec<u32> = engine
            .timings
            .lock()
            .unwrap()
            .iter()
            .filter(|sample| sample.ts_ms >= cutoff)
            .map(|sample| sample.durable_wait_us)
            .collect();
        waits.sort_unstable();
        let p99_us = if waits.is_empty() {
            0
        } else {
            let index = (waits.len() * 99).div_ceil(100).saturating_sub(1);
            waits[index]
        };
        out.push_str(&format!(
            "streams_shard_durable_wait_p99_seconds{{shard=\"{shard}\"}} {:.6}\n",
            p99_us as f64 / 1_000_000.0
        ));
        out.push_str(&format!(
            "streams_shard_appended_records_total{{shard=\"{shard}\"}} {}\n",
            engine
                .stats_appended
                .load(std::sync::atomic::Ordering::Relaxed)
        ));
        let manifest = engine.db.status().current_manifest;
        let l0_ssts = manifest.l0().len()
            + manifest
                .segments()
                .iter()
                .map(|segment| segment.l0().len())
                .sum::<usize>();
        let first_unflushed_wal = manifest.replay_after_wal_id().saturating_add(1);
        let unflushed_wal_ssts = manifest
            .next_wal_sst_id()
            .saturating_sub(first_unflushed_wal);
        out.push_str(&format!(
            "streams_shard_l0_ssts{{shard=\"{shard}\"}} {l0_ssts}\n"
        ));
        out.push_str(&format!(
            "streams_shard_unflushed_wal_ssts{{shard=\"{shard}\"}} {unflushed_wal_ssts}\n"
        ));
    }
    out.push_str("# EOF\n");
    out
}

async fn debug_metrics(State(state): State<Arc<AppState>>) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(render_operational_metrics(&state)))
        .unwrap()
}

async fn admin_split_shard(
    State(state): State<Arc<AppState>>,
    Path(parent): Path<String>,
) -> Response {
    let parent = if parent == "root" {
        String::new()
    } else {
        parent
    };
    match crate::split::request(state, parent).await {
        Ok(topology) => axum::Json(topology).into_response(),
        Err(error) if error.contains("current shard owner") => {
            err_resp(StatusCode::CONFLICT, "not_ring_owner", &error)
        }
        Err(error) if error.contains("not live") || error.contains("binary prefix") => {
            err_resp(StatusCode::BAD_REQUEST, "invalid_split", &error)
        }
        Err(error) => {
            let mut response =
                err_resp(StatusCode::SERVICE_UNAVAILABLE, "split_unavailable", &error);
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            response
        }
    }
}

async fn admin_merge_shards(
    State(state): State<Arc<AppState>>,
    Path(parent): Path<String>,
) -> Response {
    let parent = if parent == "root" {
        String::new()
    } else {
        parent
    };
    match crate::merge::request(state, parent).await {
        Ok(topology) => axum::Json(topology).into_response(),
        Err(error) if error.contains("current merge coordinator") => {
            err_resp(StatusCode::CONFLICT, "not_merge_coordinator", &error)
        }
        Err(error) if error.contains("live sibling") || error.contains("binary prefix") => {
            err_resp(StatusCode::BAD_REQUEST, "invalid_merge", &error)
        }
        Err(error) => {
            let mut response =
                err_resp(StatusCode::SERVICE_UNAVAILABLE, "merge_unavailable", &error);
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            response
        }
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    let operator_routes = Router::new()
        .route("/v1/debug/timings", get(debug_timings))
        .route("/v1/debug/load", get(debug_load))
        .route("/v1/debug/capabilities", get(debug_capabilities))
        .route("/v1/debug/store", get(debug_store))
        .route("/v1/debug/metrics", get(debug_metrics))
        .route("/v1/debug/sleep", get(debug_sleep))
        .route("/v1/admin/shards/{parent}/split", post(admin_split_shard))
        .route("/v1/admin/shards/{parent}/merge", post(admin_merge_shards))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            audit_operator_request,
        ));
    Router::new()
        .route("/health", get(health_ready))
        .route("/health/ready", get(health_ready))
        .route("/health/live", get(|| async { "ok" }))
        // Cell operator dashboard: deliberately unauthenticated (operator
        // decision 2026-07-18); payload is operational metadata only — no
        // stream names, tenant identifiers, tokens, or keys.
        .route("/operator", get(crate::operator::page))
        .route("/operator/data.json", get(crate::operator::data))
        .route("/operator/runbook", get(crate::operator::runbook))
        .route("/v1/streams", get(list_streams))
        .merge(operator_routes)
        .route("/v1/stream/{*name}", any(stream_entry))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            track_inflight,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            record_http_telemetry,
        ))
        .layer(axum::middleware::map_response(|mut resp: Response| async {
            resp.headers_mut().insert(
                "x-content-type-options",
                axum::http::HeaderValue::from_static("nosniff"),
            );
            resp
        }))
        .layer(axum::middleware::from_fn(assign_request_id))
        .with_state(state)
}

async fn health_ready(State(state): State<Arc<AppState>>) -> Response {
    if state.authn.ready()
        && state.audit.ready()
        && state.telemetry.absorber_healthy()
        && state.backup.as_ref().is_none_or(|backup| backup.ready())
        && state
            .topology_ready
            .load(std::sync::atomic::Ordering::Acquire)
        && state.split_ready.load(std::sync::atomic::Ordering::Acquire)
        && state.merge_ready.load(std::sync::atomic::Ordering::Acquire)
        && state.fleet_ready.load(std::sync::atomic::Ordering::Acquire)
        && state.cells_ready.load(std::sync::atomic::Ordering::Acquire)
    {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"ready":true}"#,
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            r#"{"ready":false}"#,
        )
            .into_response()
    }
}

/// Rendezvous over instance NAMES (FNV-1a, identical in the pilot LB) —
/// both sides compute the same shard→instance assignment from the same
/// inputs (COMPUTE-SPEC §2: "the live set is the assignment").
pub fn ring_pick(shard: &str, instances: &[String]) -> usize {
    let mut best = 0usize;
    let mut best_score = 0u32;
    for (i, name) in instances.iter().enumerate() {
        let key = format!("{shard} {name}");
        let mut h: u32 = 2166136261;
        for b in key.bytes() {
            h ^= b as u32;
            h = h.wrapping_mul(16777619);
        }
        if i == 0 || h > best_score {
            best_score = h;
            best = i;
        }
    }
    best
}

fn err_resp(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        json!({"error": {"code": code, "message": message}}).to_string(),
    )
        .into_response()
}

fn replay_to_cell(cell_id: &str) -> Response {
    let mut response = err_resp(
        StatusCode::CONFLICT,
        "not_cell_owner",
        &format!("stream is pinned to cell {cell_id}"),
    );
    if let Ok(value) = axum::http::HeaderValue::from_str(cell_id) {
        response
            .headers_mut()
            .insert("streams-replay-to-cell", value);
    }
    response
}

enum CellOwnershipError {
    Unassigned,
    Moving,
    Replay(String),
}

impl CellOwnershipError {
    fn into_response(self) -> Response {
        match self {
            Self::Unassigned => err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "cell_unassigned",
                "legacy descriptor has no cell placement; migrate it before enabling multi-cell serving",
            ),
            Self::Moving => {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cell_moving",
                    "stream is crossing a durable cell fence; retry",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                response
            }
            Self::Replay(cell_id) => replay_to_cell(&cell_id),
        }
    }
}

fn require_local_cell(state: &AppState, descriptor: &StreamDesc) -> Result<(), CellOwnershipError> {
    if descriptor.cell_move_in_progress() {
        return Err(CellOwnershipError::Moving);
    }
    match (state.cell_id.as_deref(), descriptor.cell.as_str()) {
        (None, "") => Ok(()),
        (Some(local), assigned) if local == assigned => Ok(()),
        (Some(_), "") => Err(CellOwnershipError::Unassigned),
        (_, assigned) => Err(CellOwnershipError::Replay(assigned.to_string())),
    }
}

async fn placement_for_create(
    state: &AppState,
    customer_id: &str,
    stream_name: &str,
) -> Result<String, Response> {
    let Some(local_cell) = state.cell_id.as_deref() else {
        return Ok(String::new());
    };
    if !state.cells_ready.load(std::sync::atomic::Ordering::Acquire) {
        return Err(err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "placement_unavailable",
            "cell directory is unavailable; retry",
        ));
    }
    let Some(directory) = state.cell_directory.read().unwrap().clone() else {
        return Err(err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "placement_unavailable",
            "cell directory is unavailable; retry",
        ));
    };
    let proposed = directory
        .select(customer_id, stream_name, &[])
        .map_err(|message| {
            err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "placement_unavailable",
                &message,
            )
        })?;
    let affinity = state
        .registry
        .get_or_create_customer_cell_affinity(customer_id, &proposed.cell_id)
        .await
        .map_err(|error| {
            err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "placement_unavailable",
                &error.to_string(),
            )
        })?;
    let selected = directory
        .select(customer_id, stream_name, &affinity.cells)
        .map_err(|message| {
            err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "placement_unavailable",
                &message,
            )
        })?;
    if selected.cell_id != local_cell {
        return Err(replay_to_cell(&selected.cell_id));
    }
    Ok(selected.cell_id.clone())
}

fn throttled_resp(reason: ThrottleReason, retry_after_seconds: u64) -> Response {
    let retry_after_seconds = retry_after_seconds.max(1);
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::CONTENT_TYPE, "application/json")],
        json!({
            "error": {
                "code": "throttled",
                "scope": reason.scope,
                "dimension": reason.dimension,
                "limit": reason.limit,
                "observed": reason.observed,
                "retry_after_ms": retry_after_seconds.saturating_mul(1_000),
            }
        })
        .to_string(),
    )
        .into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        axum::http::HeaderValue::from_str(&retry_after_seconds.to_string())
            .expect("positive integer is a valid Retry-After value"),
    );
    response
}

fn storage_err_resp(error: slatedb::Error) -> Response {
    if matches!(
        error.kind(),
        slatedb::ErrorKind::Unavailable | slatedb::ErrorKind::Closed(_)
    ) {
        let mut response = err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "stream storage is temporarily unavailable; retry",
        );
        response.headers_mut().insert(
            header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        response
    } else {
        err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &error.to_string(),
        )
    }
}

/// Commit-pipeline timing samples per shard: how long db.write took vs how
/// long the group then waited for the durable watermark. Diagnostic only.
async fn debug_timings(State(state): State<Arc<AppState>>) -> Response {
    let mut shards = serde_json::Map::new();
    let engines: Vec<(String, Arc<ShardEngine>)> = state
        .shards
        .read()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (prefix, eng) in &engines {
        let samples: Vec<_> = eng
            .timings
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(40)
            .map(|g| {
                json!({
                    "ts_ms": g.ts_ms,
                    "queue_wait_us": g.queue_wait_us,
                    "encode_us": g.encode_us,
                    "write_us": g.write_us,
                    "durable_wait_us": g.durable_wait_us,
                    "reqs": g.reqs,
                    "records": g.records,
                    "bytes": g.bytes,
                })
            })
            .collect();
        shards.insert(prefix.clone(), json!(samples));
    }
    (
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::Value::Object(shards).to_string(),
    )
        .into_response()
}

async fn list_streams(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let principal = match authenticate(&state, &headers) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if !principal.allows_verb(Verb::List) {
        return forbidden();
    }
    let (tenant_guard, _) = match admit_customer(
        &state,
        &principal.customer_id,
        Some(RequestQuota::Read),
        false,
    )
    .await
    {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let response = match state.registry.list(&principal.customer_id, 1000).await {
        Ok(streams) => {
            let body: Vec<_> = streams
                .iter()
                .filter(|descriptor| principal.allows_name(&descriptor.name))
                .map(|d| {
                    json!({
                        "name": d.name,
                        "profile": "generic",
                        "created_at_ms": d.created_ms,
                        "stream_epoch": d.stream_epoch,
                    })
                })
                .collect();
            (
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&body).unwrap(),
            )
                .into_response()
        }
        Err(e) => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &e.to_string(),
        ),
    };
    hold_tenant_admission(response, tenant_guard, true)
}

#[derive(Deserialize, Default)]
pub struct ReadParams {
    offset: Option<String>,
    format: Option<String>,
    live: Option<String>,
    timeout: Option<String>,
    key: Option<String>,
    // touch wait params
    cursor: Option<String>,
    sig: Option<String>,
}

async fn stream_entry(
    State(state): State<Arc<AppState>>,
    Extension(request_id): Extension<RequestId>,
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // Browser preflights carry no bearer token. Keep the response generic so
    // it exposes neither stream existence nor tenant identity.
    if method == Method::OPTIONS {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "GET,HEAD,PUT,POST,DELETE")
            .header("Access-Control-Allow-Headers", "*")
            .header("Access-Control-Max-Age", "600")
            .body(Body::empty())
            .unwrap();
    }
    let st = state.clone();
    let principal = match authenticate(&state, &headers) {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let started = std::time::Instant::now();
    let audit_customer = principal.customer_id.clone();
    let audit_token = principal.token_id.clone();
    let audit_name = name.clone();
    let audit_method = method.clone();
    let metered_name = name
        .split_once("/queue/")
        .or_else(|| name.split_once("/touch/"))
        .map(|(stream, _)| stream)
        .unwrap_or(&name)
        .to_string();
    let queue_receive = method == Method::POST
        && name
            .split_once("/queue/")
            .is_some_and(|(_, route)| route.ends_with("/receive"));
    let request_quota = if queue_receive {
        Some(RequestQuota::QueueReceive)
    } else if matches!(method, Method::GET | Method::HEAD) {
        Some(RequestQuota::Read)
    } else if method == Method::POST && !name.contains("/queue/") && !name.contains("/touch/") {
        Some(RequestQuota::Append)
    } else {
        None
    };
    let is_live = (method == Method::GET && params.live.is_some()) || queue_receive;
    let meter_egress = method == Method::GET || queue_receive;
    let (tenant_guard, limits) =
        match admit_customer(&state, &principal.customer_id, request_quota, is_live).await {
            Ok(admission) => admission,
            Err(response) => {
                if st.should_meter(&audit_customer, &metered_name) {
                    st.metrics.request(
                        &audit_customer,
                        &metered_name,
                        response.status(),
                        started.elapsed(),
                    );
                }
                return response;
            }
        };
    let resp = stream_entry_inner(
        State(state),
        Path(name),
        Query(params),
        method,
        headers,
        body,
        (principal, limits),
    )
    .await;
    // Only successful work counts toward the fleet load vector — otherwise
    // routing noise (409 replays, 404s) masquerades as demand and drives
    // the desired count up on garbage.
    if resp.status().is_success() {
        st.fleet_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let control_plane = matches!(audit_method, Method::PUT | Method::DELETE);
    let audit_event = crate::audit::AuditEvent {
        format_version: 1,
        request_id: request_id.0,
        timestamp_ms: now_ms(),
        customer_id: audit_customer.clone(),
        token_id: audit_token.clone(),
        approval_customer_id: None,
        approval_token_id: None,
        stream: audit_name.clone(),
        method: audit_method.to_string(),
        status: resp.status().as_u16(),
        duration_us: started.elapsed().as_micros().min(u64::MAX as u128) as u64,
    };
    let sample_data_plane = !control_plane && st.audit.should_sample();
    if control_plane {
        if let Err(error) = st.audit.record_durable(&audit_event).await {
            tracing::error!("durable control-plane audit failed: {error}");
            if resp.status().is_success() {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "audit_unavailable",
                    "operation completed but its audit record could not be persisted; retry",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                if st.should_meter(&audit_customer, &metered_name) {
                    st.metrics.request(
                        &audit_customer,
                        &metered_name,
                        response.status(),
                        started.elapsed(),
                    );
                }
                return hold_tenant_admission(response, tenant_guard, meter_egress);
            }
        }
    } else if sample_data_plane {
        st.audit.record_sampled(audit_event);
    }
    if control_plane || sample_data_plane {
        tracing::info!(
            target: "streams_audit",
            customer_id = %audit_customer,
            token_id = %audit_token,
            stream = %audit_name,
            method = %audit_method,
            status = resp.status().as_u16(),
            duration_us = started.elapsed().as_micros() as u64,
            "request audit"
        );
    }
    if st.should_meter(&audit_customer, &metered_name) {
        st.metrics.request(
            &audit_customer,
            &metered_name,
            resp.status(),
            started.elapsed(),
        );
    }
    hold_tenant_admission(resp, tenant_guard, meter_egress)
}

async fn stream_entry_inner(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    method: Method,
    headers: HeaderMap,
    body: Body,
    authz: (Principal, crate::registry::CustomerLimits),
) -> Response {
    let (principal, limits) = authz;
    let customer_id = principal.customer_id.clone();
    // Queue subresources: /v1/stream/<name>/queue/{consumer}/{receive,ack,extend}
    if let Some((stream, route)) = name.split_once("/queue/") {
        if !principal.allows(Verb::Queue, stream) {
            return forbidden();
        }
        return queue_entry(
            state,
            customer_id,
            stream.to_string(),
            route.to_string(),
            method,
            headers,
            body,
        )
        .await;
    }
    // Touch subresources: /v1/stream/<name>/touch/{meta,key/<hex>}
    if let Some((stream, route)) = name.split_once("/touch/") {
        if !principal.allows(Verb::Touch, stream) {
            return forbidden();
        }
        return touch_entry(
            state,
            customer_id,
            stream.to_string(),
            route.to_string(),
            method,
            headers,
            params,
        )
        .await;
    }
    let verb = match method {
        Method::PUT => Verb::Create,
        Method::POST => Verb::Append,
        Method::GET | Method::HEAD => Verb::Read,
        Method::DELETE => Verb::Delete,
        _ => {
            return err_resp(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "unsupported method",
            );
        }
    };
    if !principal.allows(verb, &name) {
        return forbidden();
    }
    match method {
        Method::PUT => {
            let body = match body_with_quota(body, &state, &customer_id, MAX_BODY_BYTES, None).await
            {
                Ok(b) => b,
                Err(response) => return response,
            };
            create_stream(state, customer_id, name, headers, body, limits).await
        }
        Method::POST => append(state, customer_id, name, headers, body).await,
        Method::GET => read(state, customer_id, name, params, headers, false).await,
        Method::HEAD => read(state, customer_id, name, params, headers, true).await,
        Method::DELETE => delete_stream(state, customer_id, name).await,
        _ => unreachable!("method checked above"),
    }
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(v) = s.strip_suffix("ms") {
        return v.parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(v) = s.strip_suffix('h') {
        return v.parse::<u64>().ok().map(|n| Duration::from_secs(n * 3600));
    }
    if let Some(v) = s.strip_suffix('m') {
        return v.parse::<u64>().ok().map(|n| Duration::from_secs(n * 60));
    }
    if let Some(v) = s.strip_suffix('s') {
        return v.parse::<u64>().ok().map(Duration::from_secs);
    }
    s.parse::<u64>().ok().map(Duration::from_secs)
}

/// Extract + validate the request's stream key against the descriptor.
enum KeyCheck {
    Ok(StreamKey, [u8; 16]),
    Missing,
    Wrong,
    BadDescriptor,
}

fn raw_key<'a>(headers: &'a HeaderMap, state: &'a AppState) -> Option<&'a str> {
    headers
        .get("stream-encryption-key")
        .and_then(|v| v.to_str().ok())
        .or(state.default_key.as_deref())
}

fn check_key(raw: Option<&str>, desc: &StreamDesc) -> KeyCheck {
    let Some(raw) = raw else {
        return KeyCheck::Missing;
    };
    let Ok(key) = StreamKey::from_b64(raw) else {
        return KeyCheck::Wrong;
    };
    let Some(epoch) = desc.epoch_bytes() else {
        return KeyCheck::BadDescriptor;
    };
    let fingerprint = key.fingerprint(&epoch);
    if !bool::from(
        fingerprint
            .as_bytes()
            .ct_eq(desc.key_fingerprint.as_bytes()),
    ) {
        return KeyCheck::Wrong;
    }
    KeyCheck::Ok(key, epoch)
}

fn key_version(headers: &HeaderMap) -> Result<u32, String> {
    match headers.get("stream-key-version") {
        None => Ok(0),
        Some(value) => {
            let value = value.to_str().map_err(|_| "invalid Stream-Key-Version")?;
            parse_uint_strict(value)
                .and_then(|version| u32::try_from(version).ok())
                .ok_or_else(|| "invalid Stream-Key-Version".to_string())
        }
    }
}

fn desc_alive(desc: &StreamDesc) -> bool {
    !desc.deleted && desc.expires_at_ms.map(|e| now_ms() < e).unwrap_or(true)
}

fn dead_stream_response(desc: &StreamDesc) -> Response {
    if !desc.fork_children.is_empty() {
        err_resp(
            StatusCode::GONE,
            "stream_soft_deleted",
            "stream is retained by one or more forks",
        )
    } else {
        err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found")
    }
}

async fn renew_sliding_ttl(state: &AppState, desc: &StreamDesc) -> Result<StreamDesc, Response> {
    if desc.ttl_secs.is_none() {
        return Ok(desc.clone());
    }
    match state
        .registry
        .renew_ttl(desc.owner(), &desc.name, &desc.stream_epoch)
        .await
    {
        Ok(Some(renewed)) if desc_alive(&renewed) && renewed.stream_epoch == desc.stream_epoch => {
            Ok(renewed)
        }
        Ok(_) => Err(err_resp(
            StatusCode::NOT_FOUND,
            "not_found",
            "stream expired during TTL renewal",
        )),
        Err(error) => {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "ttl_renewal_unavailable",
                &error.to_string(),
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            Err(response)
        }
    }
}

fn rand_epoch() -> [u8; 16] {
    use rand::RngCore;
    let mut e = [0u8; 16];
    rand::rng().fill_bytes(&mut e);
    e
}

/// Strict TTL grammar: canonical non-negative decimal only.
fn parse_ttl_strict(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.is_empty() || (b[0] == b'0' && b.len() > 1) || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Canonical non-negative integer for producer epoch/seq.
fn parse_uint_strict(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.is_empty() || !b.iter().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn hdr(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn want_close(headers: &HeaderMap) -> bool {
    hdr(headers, "stream-closed")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn tail_token(next: u64) -> String {
    if next == 0 {
        Offset::START
    } else {
        Offset(Some(next - 1))
    }
    .encode()
}

/// JSON append batching: top-level array = batch (one message per element);
/// any other JSON value = a single message. `allow_empty_array` is true only
/// for PUT bodies.
fn json_entries(body: &[u8], allow_empty_array: bool) -> Result<Vec<Bytes>, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "invalid JSON body".to_string())?;
    match v {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() && !allow_empty_array {
                return Err("empty JSON array".to_string());
            }
            Ok(arr
                .iter()
                .map(|e| Bytes::from(serde_json::to_vec(e).expect("json")))
                .collect())
        }
        other => Ok(vec![Bytes::from(serde_json::to_vec(&other).expect("json"))]),
    }
}

fn parse_producer(headers: &HeaderMap) -> Result<Option<crate::shard::ProducerReq>, String> {
    let id = hdr(headers, "producer-id");
    let epoch = hdr(headers, "producer-epoch");
    let seq = hdr(headers, "producer-seq");
    match (id, epoch, seq) {
        (None, None, None) => Ok(None),
        (Some(id), Some(e), Some(s)) => {
            if id.is_empty() || id.len() > 256 {
                return Err("Producer-Id must contain 1..=256 bytes".into());
            }
            if id.starts_with("__streams_internal/") {
                return Err("Producer-Id uses a reserved prefix".into());
            }
            let epoch = parse_uint_strict(&e).ok_or("invalid Producer-Epoch")?;
            let seq = parse_uint_strict(&s).ok_or("invalid Producer-Seq")?;
            Ok(Some(crate::shard::ProducerReq { id, epoch, seq }))
        }
        _ => Err("Producer-Id, Producer-Epoch and Producer-Seq must be sent together".into()),
    }
}

fn initial_request_hash(
    entries: &[Bytes],
    close: bool,
    fork_identity: Option<&str>,
) -> Option<String> {
    if entries.is_empty() && !close && fork_identity.is_none() {
        return None;
    }
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"streams-create-initial-v1\0");
    digest.update([u8::from(close)]);
    if let Some(identity) = fork_identity {
        digest.update((identity.len() as u64).to_be_bytes());
        digest.update(identity.as_bytes());
    } else {
        digest.update(0u64.to_be_bytes());
    }
    digest.update((entries.len() as u64).to_be_bytes());
    for entry in entries {
        digest.update((entry.len() as u64).to_be_bytes());
        digest.update(entry);
    }
    Some(hex(&digest.finalize()))
}

fn initial_config_matches(existing: &StreamDesc, requested: Option<&str>) -> bool {
    match (existing.initial_request_hash.as_deref(), requested) {
        (Some(stored), Some(requested)) => stored == requested,
        (None, Some(_)) => false,
        // Omitting the body on an idempotent PUT is allowed; it does not
        // re-run or alter a prior initial append.
        (_, None) => true,
    }
}

fn stream_config_matches(
    admission: &StreamAdmission,
    existing: &StreamDesc,
    requested: ResolvedStreamLimits,
) -> bool {
    admission.resolve(existing) == requested
}

#[allow(clippy::too_many_arguments)]
fn fresh_desc(
    customer_id: &str,
    cell: String,
    name: &str,
    key: &StreamKey,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    profile: Option<String>,
    touch_templates: Vec<crate::registry::PinnedTemplate>,
    ordering: Option<String>,
    segment_count: u32,
    initial_request_hash: Option<String>,
    stream_limits: ResolvedStreamLimits,
) -> StreamDesc {
    let epoch = rand_epoch();
    let (tt_fpr, sig_key) = if profile.as_deref() == Some("state-protocol") {
        let token = crate::crypto::touch_token(key, &epoch);
        let sk = crate::crypto::wait_sig_key(&token, &epoch);
        (
            Some(crate::crypto::touch_token_fingerprint(&token)),
            Some(hex(&sk)),
        )
    } else {
        (None, None)
    };
    StreamDesc {
        customer_id: customer_id.to_string(),
        cell,
        cell_move: None,
        name: name.to_string(),
        stream_epoch: hex(&epoch),
        key_fingerprint: key.fingerprint(&epoch),
        created_ms: now_ms(),
        expires_at_ms: ttl_secs
            .map(|t| now_ms() + (t as i64) * 1000)
            .or(expires_at_ms),
        deleted: false,
        profile,
        content_type,
        ttl_secs,
        ordering,
        segment_count,
        queue_max_deliveries: None,
        append_requests_per_second: Some(stream_limits.append_requests_per_second),
        append_request_burst: Some(stream_limits.append_request_burst),
        write_bytes_per_second: Some(stream_limits.write_bytes_per_second),
        write_burst_bytes: Some(stream_limits.write_burst_bytes),
        commit_weight: Some(stream_limits.commit_weight),
        touch_token_fingerprint: tt_fpr,
        touch_templates,
        touch_sig_key: sig_key,
        initial_request_hash,
        forked_from: None,
        fork_source_epoch: None,
        fork_offset: None,
        fork_sub_offset: None,
        fork_children: Vec::new(),
        fork_reference_registered: false,
    }
}

#[derive(Clone)]
struct ForkPlan {
    source_name: String,
    source_epoch: String,
    offset: String,
    sub_offset: Option<u64>,
    inherited: Vec<Bytes>,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
}

impl ForkPlan {
    fn identity(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}",
            self.source_name,
            self.source_epoch,
            self.offset,
            self.sub_offset.unwrap_or(0)
        )
    }
}

fn apply_fork(desc: &mut StreamDesc, plan: Option<&ForkPlan>) {
    if let Some(plan) = plan {
        desc.forked_from = Some(plan.source_name.clone());
        desc.fork_source_epoch = Some(plan.source_epoch.clone());
        desc.fork_offset = Some(plan.offset.clone());
        desc.fork_sub_offset = plan.sub_offset;
    }
}

fn fork_config_matches(desc: &StreamDesc, plan: Option<&ForkPlan>) -> bool {
    match plan {
        Some(plan) => {
            desc.forked_from.as_deref() == Some(plan.source_name.as_str())
                && desc.fork_source_epoch.as_deref() == Some(plan.source_epoch.as_str())
                && desc.fork_offset.as_deref() == Some(plan.offset.as_str())
                && desc.fork_sub_offset == plan.sub_offset
        }
        None => {
            desc.forked_from.is_none()
                && desc.fork_source_epoch.is_none()
                && desc.fork_offset.is_none()
                && desc.fork_sub_offset.is_none()
        }
    }
}

fn fork_source_name(raw: &str) -> Option<String> {
    let path = if raw.starts_with("http://") || raw.starts_with("https://") {
        reqwest::Url::parse(raw).ok()?.path().to_string()
    } else {
        raw.to_string()
    };
    let name = path.strip_prefix("/v1/stream/")?;
    if name.is_empty() || name.len() > 1024 {
        return None;
    }
    Some(name.to_string())
}

async fn prepare_fork(
    state: &Arc<AppState>,
    customer_id: &str,
    target_name: &str,
    headers: &HeaderMap,
) -> Result<Option<ForkPlan>, Response> {
    let source_header = hdr(headers, "stream-forked-from");
    if source_header.is_none()
        && (headers.contains_key("stream-fork-offset")
            || headers.contains_key("stream-fork-sub-offset"))
    {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_fork",
            "fork offset headers require Stream-Forked-From",
        ));
    }
    let Some(source_header) = source_header else {
        return Ok(None);
    };
    let Some(source_name) = fork_source_name(&source_header) else {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_fork",
            "invalid Stream-Forked-From",
        ));
    };
    if source_name == target_name {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_fork",
            "a stream cannot fork itself",
        ));
    }
    let source = match state.registry.get(customer_id, &source_name).await {
        Ok(Some(source)) if desc_alive(&source) => source,
        Ok(Some(_)) => {
            return Err(err_resp(
                StatusCode::CONFLICT,
                "fork_source_deleted",
                "fork source is deleted or expired",
            ));
        }
        Ok(None) => {
            return Err(err_resp(
                StatusCode::NOT_FOUND,
                "not_found",
                "fork source not found",
            ));
        }
        Err(error) => {
            return Err(err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &error.to_string(),
            ));
        }
    };
    require_local_cell(state, &source).map_err(CellOwnershipError::into_response)?;
    if source.is_per_key() {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_fork",
            "forking per-key streams is not supported",
        ));
    }
    let (key, epoch) = match check_key(raw_key(headers, state), &source) {
        KeyCheck::Ok(key, epoch) => (key, epoch),
        KeyCheck::Missing => {
            return Err(err_resp(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Stream-Encryption-Key required",
            ));
        }
        KeyCheck::Wrong => {
            return Err(err_resp(
                StatusCode::FORBIDDEN,
                "wrong_key",
                "fork source key mismatch",
            ));
        }
        KeyCheck::BadDescriptor => {
            return Err(err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "bad fork source descriptor",
            ));
        }
    };
    let engine = state.engine_for(&source.routing_hash()).await?;
    let handle = engine
        .stream_handle(source.storage_hash())
        .await
        .map_err(storage_err_resp)?;
    let end = handle.state.lock().unwrap().durable.next;
    let parsed_offset = match hdr(headers, "stream-fork-offset") {
        // The fork extension specification names the beginning with the
        // legacy two-word zero token, while ordinary stream offsets use the
        // canonical Crockford codec. Accept only this exact legacy sentinel.
        Some(raw) if raw == "0000000000000000_0000000000000000" => Offset::START,
        Some(raw) => Offset::parse(&raw).map_err(|message| {
            err_resp(StatusCode::BAD_REQUEST, "invalid_fork_offset", &message)
        })?,
        None => {
            if end == 0 {
                Offset::START
            } else {
                Offset(Some(end - 1))
            }
        }
    };
    let full_records = parsed_offset.scan_from();
    if full_records > end {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_fork_offset",
            "fork offset is beyond source tail",
        ));
    }
    let sub_offset = match hdr(headers, "stream-fork-sub-offset") {
        Some(raw) => {
            let parsed = parse_ttl_strict(&raw).ok_or_else(|| {
                err_resp(
                    StatusCode::BAD_REQUEST,
                    "invalid_fork_sub_offset",
                    "invalid Stream-Fork-Sub-Offset",
                )
            })?;
            (parsed > 0).then_some(parsed)
        }
        None => None,
    };
    let desired_records = if source.is_json() {
        full_records
            .checked_add(sub_offset.unwrap_or(0))
            .ok_or_else(|| {
                err_resp(
                    StatusCode::BAD_REQUEST,
                    "invalid_fork_sub_offset",
                    "fork sub-offset overflow",
                )
            })?
    } else {
        full_records + u64::from(sub_offset.is_some())
    };
    if desired_records > end {
        return Err(err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_fork_sub_offset",
            "fork sub-offset is beyond source data",
        ));
    }

    let mut inherited = Vec::new();
    let mut materialized_bytes = 0usize;
    let mut scan_from = 0u64;
    while scan_from < desired_records {
        let out = read_records(
            state,
            &key,
            &epoch,
            &handle,
            &engine,
            scan_from,
            None,
            MAX_READ_BYTES,
        )
        .await
        .map_err(|message| err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &message))?;
        if out.recs.is_empty() {
            return Err(err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "fork_source_unavailable",
                "fork source ended before requested boundary",
            ));
        }
        for record in out.recs {
            if record.off >= desired_records {
                break;
            }
            let payload = if let Some(sub_offset) = sub_offset
                && !source.is_json()
                && record.off == full_records
            {
                let take = usize::try_from(sub_offset).unwrap_or(usize::MAX);
                if take > record.payload.len() {
                    return Err(err_resp(
                        StatusCode::BAD_REQUEST,
                        "invalid_fork_sub_offset",
                        "binary fork sub-offset exceeds append length",
                    ));
                }
                record.payload.slice(..take)
            } else {
                record.payload
            };
            materialized_bytes =
                materialized_bytes
                    .checked_add(payload.len())
                    .ok_or_else(|| {
                        err_resp(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "fork_too_large",
                            "fork materialization size overflow",
                        )
                    })?;
            if materialized_bytes > MAX_FORK_MATERIALIZE_BYTES {
                return Err(err_resp(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "fork_too_large",
                    "fork materialization exceeds 32 MiB",
                ));
            }
            inherited.push(payload);
        }
        scan_from = out.last.map(|last| last + 1).unwrap_or(desired_records);
    }

    Ok(Some(ForkPlan {
        source_name,
        source_epoch: source.stream_epoch,
        offset: parsed_offset.encode(),
        sub_offset,
        inherited,
        content_type: source.content_type,
        ttl_secs: source.ttl_secs,
        expires_at_ms: source.expires_at_ms,
    }))
}

async fn create_stream(
    state: Arc<AppState>,
    customer_id: String,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    limits: crate::registry::CustomerLimits,
) -> Response {
    let lease = if limits.streams_count.is_some() {
        match state
            .registry
            .acquire_stream_quota_lease(&customer_id)
            .await
        {
            Ok(lease) => Some(lease),
            Err(error) => {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "quota_unavailable",
                    &format!("stream quota unavailable: {error}"),
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                return response;
            }
        }
    } else {
        None
    };
    let response = create_stream_with_quota(
        state.clone(),
        customer_id,
        name,
        headers,
        body,
        limits,
        lease.as_ref(),
    )
    .await;
    if let Some(lease) = &lease
        && let Err(error) = state.registry.release_stream_quota_lease(lease).await
    {
        tracing::error!("stream quota lease release failed: {error}");
    }
    response
}

async fn create_stream_with_quota(
    state: Arc<AppState>,
    customer_id: String,
    name: String,
    headers: HeaderMap,
    body: Bytes,
    limits: crate::registry::CustomerLimits,
    quota_lease: Option<&crate::registry::StreamQuotaLease>,
) -> Response {
    let Some(raw_key_str) = raw_key(&headers, &state) else {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Stream-Encryption-Key required",
        );
    };
    let key = match StreamKey::from_b64(raw_key_str) {
        Ok(k) => k,
        Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_key", &m),
    };
    let mut content_type =
        hdr(&headers, "content-type").unwrap_or_else(|| "application/octet-stream".to_string());
    let ttl_hdr = hdr(&headers, "stream-ttl");
    let exp_hdr = hdr(&headers, "stream-expires-at");
    if ttl_hdr.is_some() && exp_hdr.is_some() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "TTL and Expires-At together",
        );
    }
    let mut ttl_secs = match &ttl_hdr {
        Some(t) => match parse_ttl_strict(t) {
            Some(v) => Some(v),
            None => return err_resp(StatusCode::BAD_REQUEST, "invalid_ttl", "invalid Stream-TTL"),
        },
        None => None,
    };
    let mut expires_at_ms = match &exp_hdr {
        Some(e) => match chrono::DateTime::parse_from_rfc3339(e) {
            Ok(ts) => Some(ts.timestamp_millis()),
            Err(_) => {
                return err_resp(StatusCode::BAD_REQUEST, "invalid_request", "bad Expires-At");
            }
        },
        None => None,
    };
    let close = want_close(&headers);
    let profile = hdr(&headers, "stream-profile");
    if let Some(p) = &profile
        && !matches!(p.as_str(), "generic" | "state-protocol" | "queue")
    {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_profile",
            "unsupported profile",
        );
    }
    let stream_limits = match state.stream_admission.requested_limits(&headers) {
        Ok(limits) => limits,
        Err(name) => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_stream_limit",
                &format!("invalid {name}"),
            );
        }
    };
    // Opt-in per-key ordering (PER-KEY-ORDERING.md §2). Absent => total
    // order, byte-identical semantics to before this feature existed.
    let ordering = match hdr(&headers, "stream-ordering") {
        None => state.default_ordering.as_ref().map(|(o, _)| o.clone()),
        Some(v) if v.eq_ignore_ascii_case("total") => None,
        Some(v) if v.eq_ignore_ascii_case("per-key") => Some("per-key".to_string()),
        Some(_) => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_ordering",
                "ordering must be total or per-key",
            );
        }
    };
    let segment_count: u32 = match hdr(&headers, "stream-segments") {
        None => {
            if ordering.is_some() {
                state
                    .default_ordering
                    .as_ref()
                    .map(|(_, n)| *n)
                    .unwrap_or(2)
            } else {
                0
            }
        }
        Some(_) if ordering.is_none() => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Stream-Segments requires Stream-Ordering: per-key",
            );
        }
        Some(v) => match parse_ttl_strict(&v) {
            Some(n) if (1..=256).contains(&n) && (n as u32).is_power_of_two() => n as u32,
            _ => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "invalid_segments",
                    "Stream-Segments must be a power of two in 1..=256",
                );
            }
        },
    };
    if ordering.is_some()
        && matches!(
            hdr(&headers, "stream-profile").as_deref(),
            Some("state-protocol" | "queue")
        )
    {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "unsupported_combination",
            "this profile requires total ordering (v1)",
        );
    }
    let fork_plan = match prepare_fork(&state, &customer_id, &name, &headers).await {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    if let Some(plan) = &fork_plan {
        if hdr(&headers, "content-type").is_some()
            && crate::registry::media_type(&content_type)
                != crate::registry::media_type(&plan.content_type)
        {
            return err_resp(
                StatusCode::CONFLICT,
                "fork_content_type_mismatch",
                "fork content type must match source",
            );
        }
        if hdr(&headers, "content-type").is_none() {
            content_type = plan.content_type.clone();
        }
        if ttl_hdr.is_none() && exp_hdr.is_none() {
            ttl_secs = plan.ttl_secs;
            expires_at_ms = if plan.ttl_secs.is_some() {
                None
            } else {
                plan.expires_at_ms
            };
        }
        if profile
            .as_deref()
            .is_some_and(|profile| profile != "generic")
            || ordering.is_some()
        {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_fork",
                "forks require the generic total-order profile",
            );
        }
    }
    let mut touch_templates: Vec<crate::registry::PinnedTemplate> = Vec::new();
    if let Some(raw) = hdr(&headers, "stream-touch-templates") {
        if profile.as_deref() != Some("state-protocol") {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "templates need state-protocol",
            );
        }
        match serde_json::from_str(&raw) {
            Ok(list) => touch_templates = list,
            Err(e) => {
                return err_resp(StatusCode::BAD_REQUEST, "invalid_templates", &e.to_string());
            }
        }
        if touch_templates.len() > crate::touch::MAX_TEMPLATES_PER_STREAM
            || touch_templates
                .iter()
                .any(|t| t.entity.is_empty() || t.fields.is_empty() || t.fields.len() > 3)
        {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_templates",
                "bad template shape/caps",
            );
        }
        let mut per_entity: HashMap<&str, usize> = HashMap::new();
        for template in &touch_templates {
            let count = per_entity.entry(&template.entity).or_default();
            *count += 1;
            if *count > crate::touch::MAX_TEMPLATES_PER_ENTITY
                || template.entity.len() > 256
                || template
                    .fields
                    .iter()
                    .any(|field| field.is_empty() || field.len() > 256)
            {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "invalid_templates",
                    "template entity/field bounds exceeded",
                );
            }
        }
    }

    // Validate and canonicalize the initial body BEFORE publishing the
    // descriptor. Otherwise a malformed JSON create leaves behind a stream
    // that can only be repaired by delete/recreate.
    let body_entries: Vec<Bytes> = if body.is_empty() {
        Vec::new()
    } else if crate::registry::media_type(&content_type) == "application/json" {
        match json_entries(&body, true) {
            Ok(entries) => entries,
            Err(message) => {
                return err_resp(StatusCode::BAD_REQUEST, "invalid_json", &message);
            }
        }
    } else {
        vec![body.clone()]
    };
    let mut initial_entries = fork_plan
        .as_ref()
        .map(|plan| plan.inherited.clone())
        .unwrap_or_default();
    initial_entries.extend(body_entries);
    let fork_identity = fork_plan.as_ref().map(ForkPlan::identity);
    let requested_initial_hash =
        initial_request_hash(&initial_entries, close, fork_identity.as_deref());

    // Resolve existing.
    let existing = match state.registry.get(&customer_id, &name).await {
        Ok(v) => v,
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if let Some(descriptor) = &existing
        && let Err(response) = require_local_cell(&state, descriptor)
    {
        return response.into_response();
    }
    let consumes_stream_slot = existing
        .as_ref()
        .is_none_or(|descriptor| descriptor.deleted);
    if consumes_stream_slot && let Some(limit) = limits.streams_count {
        let observed = if limit == 0 {
            0
        } else {
            match state
                .registry
                .list(&customer_id, limit.saturating_add(1))
                .await
            {
                Ok(streams) => streams.len(),
                Err(error) => {
                    return err_resp(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "quota_unavailable",
                        &format!("stream count unavailable: {error}"),
                    );
                }
            }
        };
        if observed >= limit {
            return throttled_resp(
                ThrottleReason {
                    scope: "customer",
                    dimension: "streams_count",
                    limit: limit as u64,
                    observed: observed.saturating_add(1) as u64,
                },
                1,
            );
        }
        let Some(lease) = quota_lease else {
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "quota_unavailable",
                "stream quota lease missing",
            );
        };
        if let Err(error) = state.registry.verify_stream_quota_lease(lease).await {
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "quota_unavailable",
                &format!("stream quota lease lost: {error}"),
            );
        }
    }
    let (created, desc) = match existing {
        Some(d) if desc_alive(&d) => {
            // Idempotent PUT: config must match.
            let same_ct = crate::registry::media_type(&d.content_type)
                == crate::registry::media_type(&content_type)
                || hdr(&headers, "content-type").is_none();
            if !same_ct
                || d.ttl_secs != ttl_secs
                || d.ordering != ordering
                || (ordering.is_some() && d.segment_count != segment_count)
                || !stream_config_matches(&state.stream_admission, &d, stream_limits)
                || !fork_config_matches(&d, fork_plan.as_ref())
                || !initial_config_matches(&d, requested_initial_hash.as_deref())
            {
                return err_resp(
                    StatusCode::CONFLICT,
                    "config_mismatch",
                    "stream exists with different config",
                );
            }
            match check_key(raw_key(&headers, &state), &d) {
                KeyCheck::Ok(..) => {}
                KeyCheck::Wrong => {
                    return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch");
                }
                KeyCheck::Missing => {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "missing_key",
                        "Stream-Encryption-Key required",
                    );
                }
                KeyCheck::BadDescriptor => {
                    return err_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "bad descriptor",
                    );
                }
            }
            (false, d)
        }
        Some(dead) => {
            if !dead.fork_children.is_empty() {
                return err_resp(
                    StatusCode::CONFLICT,
                    "stream_soft_deleted",
                    "stream name is retained by one or more forks",
                );
            }
            if let Err(error) = state.registry.release_fork_chain(&customer_id, &dead).await {
                return err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "fork_reference_unavailable",
                    &error.to_string(),
                );
            }
            // Dead incarnation: recreate with a fresh epoch (fresh keyspace).
            let mut fresh = fresh_desc(
                &customer_id,
                state.cell_id.clone().unwrap_or_default(),
                &name,
                &key,
                content_type.clone(),
                ttl_secs,
                expires_at_ms,
                profile.clone(),
                touch_templates.clone(),
                ordering.clone(),
                segment_count,
                requested_initial_hash.clone(),
                stream_limits,
            );
            fresh.queue_max_deliveries =
                hdr(&headers, "stream-queue-max-deliveries").and_then(|v| v.parse().ok());
            apply_fork(&mut fresh, fork_plan.as_ref());
            if consumes_stream_slot
                && let Some(lease) = quota_lease
                && let Err(error) = state.registry.verify_stream_quota_lease(lease).await
            {
                return err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "quota_unavailable",
                    &format!("stream quota lease lost: {error}"),
                );
            }
            match state
                .registry
                .recreate(&customer_id, &name, &dead.stream_epoch, fresh)
                .await
            {
                Ok((true, d)) => (true, d),
                Ok((false, d)) => {
                    // A concurrent recreator won. Treat it exactly like an
                    // already-existing stream; never cache or use the losing
                    // request's key for the winner's incarnation.
                    if let Err(response) = require_local_cell(&state, &d) {
                        return response.into_response();
                    }
                    match check_key(raw_key(&headers, &state), &d) {
                        KeyCheck::Ok(..) => {}
                        KeyCheck::Wrong => {
                            return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch");
                        }
                        _ => {
                            return err_resp(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "internal",
                                "bad descriptor",
                            );
                        }
                    }
                    if crate::registry::media_type(&d.content_type)
                        != crate::registry::media_type(&content_type)
                        || d.ttl_secs != ttl_secs
                        || d.ordering != ordering
                        || (ordering.is_some() && d.segment_count != segment_count)
                        || !stream_config_matches(&state.stream_admission, &d, stream_limits)
                        || !fork_config_matches(&d, fork_plan.as_ref())
                        || !initial_config_matches(&d, requested_initial_hash.as_deref())
                    {
                        return err_resp(StatusCode::CONFLICT, "config_mismatch", "conflict");
                    }
                    (false, d)
                }
                Err(e) => {
                    return err_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &e.to_string(),
                    );
                }
            }
        }
        None => {
            let selected_cell = match placement_for_create(&state, &customer_id, &name).await {
                Ok(cell) => cell,
                Err(response) => return response,
            };
            let mut fresh = fresh_desc(
                &customer_id,
                selected_cell,
                &name,
                &key,
                content_type.clone(),
                ttl_secs,
                expires_at_ms,
                profile.clone(),
                touch_templates.clone(),
                ordering.clone(),
                segment_count,
                requested_initial_hash.clone(),
                stream_limits,
            );
            fresh.queue_max_deliveries =
                hdr(&headers, "stream-queue-max-deliveries").and_then(|v| v.parse().ok());
            apply_fork(&mut fresh, fork_plan.as_ref());
            if let Some(lease) = quota_lease
                && let Err(error) = state.registry.verify_stream_quota_lease(lease).await
            {
                return err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "quota_unavailable",
                    &format!("stream quota lease lost: {error}"),
                );
            }
            match state.registry.create(fresh).await {
                Ok((true, d)) => (true, d),
                Ok((false, d)) => {
                    // Raced: treat as idempotent-config path.
                    if let Err(response) = require_local_cell(&state, &d) {
                        return response.into_response();
                    }
                    match check_key(raw_key(&headers, &state), &d) {
                        KeyCheck::Ok(..) => {}
                        KeyCheck::Wrong => {
                            return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch");
                        }
                        _ => {
                            return err_resp(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "internal",
                                "bad descriptor",
                            );
                        }
                    }
                    if crate::registry::media_type(&d.content_type)
                        != crate::registry::media_type(&content_type)
                        || d.ttl_secs != ttl_secs
                        || d.ordering != ordering
                        || (ordering.is_some() && d.segment_count != segment_count)
                        || !stream_config_matches(&state.stream_admission, &d, stream_limits)
                        || !fork_config_matches(&d, fork_plan.as_ref())
                        || !initial_config_matches(&d, requested_initial_hash.as_deref())
                    {
                        return err_resp(StatusCode::CONFLICT, "config_mismatch", "conflict");
                    }
                    (false, d)
                }
                Err(e) => {
                    return err_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &e.to_string(),
                    );
                }
            }
        }
    };

    let hash = if desc.is_per_key() {
        desc.segment_hash(desc.segment_for(""))
    } else {
        desc.storage_hash()
    };
    let Some(epoch_bytes) = desc.epoch_bytes() else {
        return err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "bad descriptor",
        );
    };
    state.keys.put(hash, key.clone(), epoch_bytes);
    // Shard choice keys off the stream NAME hash (COMPUTE-SPEC R1) so the
    // router can compute placement without knowing the stream epoch; the
    // record keyspace keeps using storage/segment hashes.
    let engine = match state.engine_for(&desc.routing_hash()).await {
        Ok(e) => e,
        Err(r) => return r,
    };

    // The create-time initial append rides a reserved durable producer id on
    // EVERY matching retry. If the first response was lost after durability,
    // producer dedupe returns the original tail instead of appending twice;
    // if it failed before durability, the retry completes it.
    let mut next = {
        match engine.stream_handle(hash).await {
            Ok(h) => h.state.lock().unwrap().durable.next,
            Err(e) => return storage_err_resp(e),
        }
    };
    let mut closed_now = false;
    if requested_initial_hash.is_some() {
        let entries = initial_entries;
        let subkey = derive_subkey(&key, &epoch_bytes, "", 0);
        let bytes = entries.iter().map(|e| e.len()).sum();
        if let Err(reason) = state.stream_admission.charge_request(&desc) {
            state
                .admit_shed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return throttled_resp(reason, 1);
        }
        if let Err(reason) = state.stream_admission.charge_write(&desc, bytes) {
            state
                .admit_shed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return throttled_resp(reason, 1);
        }
        let (tx, rx) = oneshot::channel();
        let req = AppendReq {
            customer_id: customer_id.clone(),
            enqueued_at: std::time::Instant::now(),
            hash,
            fair_weight: desc.commit_weight.unwrap_or(1).clamp(1, 100),
            entries,
            routing_key: String::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes,
            close,
            producer: Some(crate::shard::ProducerReq {
                id: "__streams_internal/create".to_string(),
                epoch: 0,
                seq: 0,
            }),
            deferred_error: None,
            touch: None,
            resp: tx,
        };
        if let Err(error) = engine.try_enqueue(req) {
            return match error {
                EnqueueError::Full => throttled_resp(
                    ThrottleReason {
                        scope: "shard",
                        dimension: "queue_depth",
                        limit: engine.queue_limit() as u64,
                        observed: engine.queue_limit().saturating_add(1) as u64,
                    },
                    1,
                ),
                EnqueueError::ShardMoved => {
                    let mut response = err_resp(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "shard_moving",
                        "shard ownership changed; retry create",
                    );
                    response.headers_mut().insert(
                        header::RETRY_AFTER,
                        axum::http::HeaderValue::from_static("1"),
                    );
                    response
                }
            };
        }
        match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
            Ok(Ok(Ok(ack))) => {
                next = ack.next_offset;
                closed_now = ack.closed;
            }
            Ok(Ok(Err(AppendErr::ShardMoved))) => {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_moving",
                    "shard ownership changed; retry create",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                return response;
            }
            Ok(Ok(Err(AppendErr::Overloaded))) => {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "overloaded",
                    "active stream capacity exhausted; retry create",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                return response;
            }
            Ok(Ok(Err(AppendErr::Internal(message)))) => {
                return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &message);
            }
            Ok(Ok(Err(_))) => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "initial_append_conflict",
                    "initial create append conflicted; retry",
                );
            }
            Ok(Err(_)) => {
                return err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_moving",
                    "initial append responder closed; retry",
                );
            }
            Err(_) => {
                return err_resp(
                    StatusCode::REQUEST_TIMEOUT,
                    "append_timeout",
                    "initial body timed out; retry the same PUT",
                );
            }
        }
    }

    if let Some(plan) = &fork_plan
        && !desc.fork_reference_registered
    {
        match state
            .registry
            .add_fork_child(&customer_id, &plan.source_name, &plan.source_epoch, &name)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                if created {
                    let _ = state
                        .registry
                        .mark_deleted(&customer_id, &name, &desc.stream_epoch, &desc.cell)
                        .await;
                }
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_source_changed",
                    "fork source changed or was deleted during creation",
                );
            }
            Err(error) => {
                return err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "fork_reference_unavailable",
                    &error.to_string(),
                );
            }
        }
        if let Err(error) = state
            .registry
            .update(&customer_id, &name, |current| {
                if current.stream_epoch == desc.stream_epoch {
                    current.fork_reference_registered = true;
                }
            })
            .await
        {
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "fork_reference_unavailable",
                &error.to_string(),
            );
        }
    }

    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let mut resp = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, desc.content_type.clone())
        .header("Stream-Next-Offset", tail_token(next));
    if created {
        let host = hdr(&headers, "host").unwrap_or_else(|| "localhost".to_string());
        resp = resp.header(header::LOCATION, format!("http://{host}/v1/stream/{name}"));
    }
    if closed_now {
        resp = resp.header("Stream-Closed", "true");
    }
    resp.body(Body::empty()).unwrap()
}

async fn delete_stream(state: Arc<AppState>, customer_id: String, name: String) -> Response {
    let observed = match state.registry.get(&customer_id, &name).await {
        Ok(Some(desc)) => desc,
        Ok(None) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(error) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &error.to_string(),
            );
        }
    };
    if let Err(response) = require_local_cell(&state, &observed) {
        return response.into_response();
    }
    if !desc_alive(&observed) {
        if observed.fork_children.is_empty()
            && let Err(error) = state
                .registry
                .release_fork_chain(&customer_id, &observed)
                .await
        {
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "fork_reference_unavailable",
                &error.to_string(),
            );
        }
        return dead_stream_response(&observed);
    }
    match state
        .registry
        .mark_deleted(&customer_id, &name, &observed.stream_epoch, &observed.cell)
        .await
    {
        Ok(Some((true, desc))) => {
            state.touch.remove(&desc.storage_hash());
            if desc.fork_children.is_empty()
                && let Err(error) = state.registry.release_fork_chain(&customer_id, &desc).await
            {
                return err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "fork_reference_unavailable",
                    &error.to_string(),
                );
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(Some((false, desc))) => match require_local_cell(&state, &desc) {
            Ok(()) => dead_stream_response(&desc),
            Err(response) => response.into_response(),
        },
        Ok(None) => err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(error) => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &error.to_string(),
        ),
    }
}

// ---- state-protocol touch surface (collapsible GET-per-key model) ----

/// /touch/* authorization: a touch capability token (purpose-bound HKDF of
/// the stream key — observation without decryption) OR the full stream key.
/// The wait route additionally accepts the URL `sig` capability so that CDN
/// cache keys are self-authorizing.
fn touch_authorized(headers: &HeaderMap, state: &AppState, desc: &StreamDesc) -> bool {
    if let Some(expected) = &desc.touch_token_fingerprint {
        if let Some(raw) = headers.get("touch-token").and_then(|v| v.to_str().ok())
            && let Some(bytes) = crate::crypto::unhex(raw.trim())
            && let Ok(token) = <[u8; 32]>::try_from(bytes)
        {
            let actual = crate::crypto::touch_token_fingerprint(&token);
            if bool::from(actual.as_bytes().ct_eq(expected.as_bytes())) {
                return true;
            }
        }
        return matches!(check_key(raw_key(headers, state), desc), KeyCheck::Ok(..));
    }
    false
}

fn pinned_of(desc: &StreamDesc) -> Vec<(String, Vec<String>)> {
    desc.touch_templates
        .iter()
        .map(|t| (t.entity.clone(), t.fields.clone()))
        .collect()
}

async fn touch_entry(
    state: Arc<AppState>,
    customer_id: String,
    stream: String,
    route: String,
    method: Method,
    headers: HeaderMap,
    params: ReadParams,
) -> Response {
    let desc = match state.registry.get(&customer_id, &stream).await {
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(_) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if let Err(response) = require_local_cell(&state, &desc) {
        return response.into_response();
    }
    if desc.profile.as_deref() != Some("state-protocol") {
        return err_resp(StatusCode::NOT_FOUND, "not_found", "touch is not enabled");
    }
    // Touch journals are process-local projections of durable appends. Do
    // not let a fenced-but-alive former owner keep serving a stale journal.
    let _engine = match state.engine_for(&desc.routing_hash()).await {
        Ok(engine) => engine,
        Err(response) => return response,
    };

    // GET /touch/key/{watchKeyHex}?cursor=..&sig=..[&timeout=..]
    // The collapsible wait: one key per URL, journal-global cursors, the
    // `sig` URL capability as auth (so CDN cache keys are self-authorizing).
    if let Some(key_hex) = route.strip_prefix("key/") {
        if method != Method::GET {
            return err_resp(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "GET only",
            );
        }
        let key_hex = key_hex.trim_end_matches('/').to_ascii_lowercase();
        if key_hex.len() != 16 || u64::from_str_radix(&key_hex, 16).is_err() {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_key",
                "watch key must be hex16",
            );
        }
        let sig_ok = match (&params.sig, &desc.touch_sig_key) {
            (Some(sig), Some(stored)) => crate::crypto::unhex(stored)
                .and_then(|k| <[u8; 32]>::try_from(k).ok())
                .map(|k| {
                    let expected = crate::crypto::wait_url_sig(&k, &key_hex);
                    let supplied = sig.trim().to_ascii_lowercase();
                    bool::from(expected.as_bytes().ct_eq(supplied.as_bytes()))
                })
                .unwrap_or(false),
            _ => false,
        };
        if !sig_ok && !touch_authorized(&headers, &state, &desc) {
            return err_resp(
                StatusCode::FORBIDDEN,
                "touch_unauthorized",
                "a valid sig, Touch-Token, or Stream-Encryption-Key is required",
            );
        }
        let journal =
            state
                .touch
                .journal(desc.storage_hash(), desc.routing_hash(), &pinned_of(&desc));
        let cursor = params.cursor.as_deref().unwrap_or("now");
        let timeout = params
            .timeout
            .as_deref()
            .and_then(parse_duration)
            .unwrap_or(Duration::from_secs(25))
            .min(Duration::from_secs(30));
        let key_id = crate::touch_keys::key_id_of(&key_hex);
        let out = journal.wait(cursor, vec![key_id], timeout).await;

        use crate::touch::WaitOutcome;
        let end_off_enc = |end: u64| {
            if end == 0 {
                Offset::START
            } else {
                Offset(Some(end - 1))
            }
            .encode()
        };
        let (body, cache) = match out {
            // Coalescing (identical in-flight URLs collapsed) delivers the
            // origin-load win; caching is only a short straggler window.
            // Measured: long TTLs let desynchronized clients walk a cached
            // hop-chain one generation at a time, so head wakes cache for
            // just 2s and everything else is no-store.
            WaitOutcome::Touched {
                cursor,
                end_offset,
                proven,
                cacheable,
            } => (
                json!({
                    "touched": true,
                    "reason": if proven { "touched" } else { "resync" },
                    "cursor": cursor,
                    "streamEndOffset": end_off_enc(end_offset),
                }),
                if cacheable {
                    "public, max-age=2"
                } else {
                    "no-store"
                },
            ),
            // A touch may still arrive for this (key, cursor): never cache.
            WaitOutcome::Timeout { cursor, end_offset } => (
                json!({
                    "touched": false,
                    "cursor": cursor,
                    "streamEndOffset": end_off_enc(end_offset),
                }),
                "no-store",
            ),
            WaitOutcome::Stale { cursor } => (
                json!({
                    "stale": true,
                    "cursor": cursor,
                    "error": {"code": "stale", "message": "cursor epoch mismatch; rerun and restart from cursor"},
                }),
                "no-store",
            ),
        };
        return (
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, cache),
            ],
            body.to_string(),
        )
            .into_response();
    }

    if !touch_authorized(&headers, &state, &desc) {
        return err_resp(
            StatusCode::FORBIDDEN,
            "touch_unauthorized",
            "a valid Touch-Token or Stream-Encryption-Key is required",
        );
    }
    match (method, route.as_str()) {
        (Method::GET, "meta") => {
            let journal =
                state
                    .touch
                    .journal(desc.storage_hash(), desc.routing_hash(), &pinned_of(&desc));
            (
                [(header::CONTENT_TYPE, "application/json")],
                journal.meta().to_string(),
            )
                .into_response()
        }
        _ => err_resp(StatusCode::NOT_FOUND, "not_found", "unknown touch route"),
    }
}

fn parse_ts_hint(headers: &HeaderMap) -> Option<i64> {
    let raw = headers.get("stream-timestamp")?.to_str().ok()?;
    if let Ok(n) = raw.parse::<i64>() {
        return Some(n / 1_000_000); // unix nanos
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.timestamp_millis())
}

async fn append(
    state: Arc<AppState>,
    customer_id: String,
    name: String,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let desc = match state.registry.get(&customer_id, &name).await {
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(Some(d)) => return dead_stream_response(&d),
        Ok(None) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if let Err(response) = require_local_cell(&state, &desc) {
        return response.into_response();
    }
    let (key, epoch) = match check_key(raw_key(&headers, &state), &desc) {
        KeyCheck::Ok(k, e) => (k, e),
        KeyCheck::Missing => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Stream-Encryption-Key required",
            );
        }
        KeyCheck::Wrong => return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"),
        KeyCheck::BadDescriptor => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "bad descriptor",
            );
        }
    };

    let producer = match parse_producer(&headers) {
        Ok(p) => p,
        Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_producer", &m),
    };
    let close = want_close(&headers);
    if let Err(reason) = state.stream_admission.charge_request(&desc) {
        state
            .admit_shed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return throttled_resp(reason, 1);
    }
    let body = match body_with_quota(body, &state, &customer_id, MAX_BODY_BYTES, Some(&desc)).await
    {
        Ok(b) => b,
        Err(response) => return response,
    };
    let close_only = close && body.is_empty();

    // Content-Type: required on POST with a body; must match the stream's
    // configured media type (case-insensitive; parameters ignored). A
    // close-only POST ignores content type entirely. With producer headers
    // the mismatch is deferred so duplicates still return 204.
    let ct = hdr(&headers, "content-type");
    let mut deferred: Option<crate::shard::DeferredErr> = None;
    if !close_only {
        match &ct {
            None => {
                if producer.is_some() {
                    deferred = Some(crate::shard::DeferredErr::BadBody(
                        "missing Content-Type".into(),
                    ));
                } else {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "missing_content_type",
                        "Content-Type required",
                    );
                }
            }
            Some(c) => {
                if crate::registry::media_type(c) != crate::registry::media_type(&desc.content_type)
                {
                    if producer.is_some() {
                        deferred = Some(crate::shard::DeferredErr::CtMismatch);
                    } else {
                        return err_resp(
                            StatusCode::CONFLICT,
                            "content_type_mismatch",
                            "content type mismatch",
                        );
                    }
                }
            }
        }
    }

    // Body -> entries (batching rules); errors deferred with producers.
    let mut entries: Vec<Bytes> = Vec::new();
    if !close_only && deferred.is_none() {
        if body.is_empty() {
            if producer.is_some() {
                deferred = Some(crate::shard::DeferredErr::BadBody("empty body".into()));
            } else {
                return err_resp(StatusCode::BAD_REQUEST, "empty_body", "empty body");
            }
        } else if desc.is_json() {
            match json_entries(&body, false) {
                Ok(v) => entries = v,
                Err(m) => {
                    if producer.is_some() {
                        deferred = Some(crate::shard::DeferredErr::BadBody(m));
                    } else {
                        return err_resp(StatusCode::BAD_REQUEST, "invalid_json", &m);
                    }
                }
            }
        } else {
            entries = vec![body.clone()];
        }
    }

    let routing_key = hdr(&headers, "stream-key").unwrap_or_default();
    if routing_key.len() > MAX_ROUTING_KEY_BYTES {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_routing_key",
            "Stream-Key exceeds 4096 bytes",
        );
    }
    // Renew only after authentication and body/config validation, but before
    // publishing the append. A failed renewal therefore cannot turn a
    // durable append into an ambiguous retry for clients without producers.
    let desc = match renew_sliding_ttl(&state, &desc).await {
        Ok(desc) => desc,
        Err(response) => return response,
    };
    let seg_ord: Option<u32> = if desc.is_per_key() {
        Some(desc.segment_for(&routing_key))
    } else {
        None
    };
    let hash = match seg_ord {
        Some(o) => desc.segment_hash(o),
        None => desc.storage_hash(),
    };
    let kv = match key_version(&headers) {
        Ok(version) => version,
        Err(message) => return err_resp(StatusCode::BAD_REQUEST, "invalid_key_version", &message),
    };
    let subkey = derive_subkey(&key, &epoch, &routing_key, kv);
    state.keys.put(hash, key, epoch);

    // H1 state-protocol hook (unchanged; uses the incarnation hash).
    let touch = if desc.profile.as_deref() == Some("state-protocol") && !entries.is_empty() {
        let journal = state
            .touch
            .journal(hash, desc.routing_hash(), &pinned_of(&desc));
        let snapshot = journal.snapshot();
        let mut key_ids: Vec<u32> = Vec::new();
        for raw in &entries {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw)
                && let Some(mut ids) = crate::touch::TouchJournal::derive_key_ids(&snapshot, &v)
            {
                key_ids.append(&mut ids);
            }
        }
        key_ids.sort_unstable();
        key_ids.dedup();
        if key_ids.is_empty() {
            None
        } else {
            Some(crate::shard::TouchFeed {
                journal,
                key_ids,
                next_offset: 0,
            })
        }
    } else {
        None
    };

    let bytes = entries.iter().map(|e| e.len()).sum();
    let metric_bytes = bytes as u64;
    let (tx, rx) = oneshot::channel();
    let req = AppendReq {
        customer_id: customer_id.clone(),
        enqueued_at: std::time::Instant::now(),
        hash,
        fair_weight: desc.commit_weight.unwrap_or(1).clamp(1, 100),
        entries,
        routing_key,
        key_version: kv,
        subkey,
        ts_hint_ms: parse_ts_hint(&headers),
        seq: hdr(&headers, "stream-seq"),
        bytes,
        close,
        producer: producer.clone(),
        deferred_error: deferred,
        touch,
        resp: tx,
    };
    let engine = match state.engine_for(&desc.routing_hash()).await {
        Ok(e) => e,
        Err(r) => return r,
    };
    if let Err(error) = engine.try_enqueue(req) {
        return match error {
            EnqueueError::Full => throttled_resp(
                ThrottleReason {
                    scope: "shard",
                    dimension: "queue_depth",
                    limit: engine.queue_limit() as u64,
                    observed: engine.queue_limit().saturating_add(1) as u64,
                },
                1,
            ),
            EnqueueError::ShardMoved => {
                let mut response = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "shard_moving",
                    "shard ownership changed; retry",
                );
                response.headers_mut().insert(
                    header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
                response
            }
        };
    }
    let outcome = match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
        Ok(Ok(o)) => o,
        _ => {
            return err_resp(
                StatusCode::REQUEST_TIMEOUT,
                "append_timeout",
                "append timed out; outcome unknown",
            );
        }
    };

    let tok = |next: u64| match seg_ord {
        Some(o) => crate::offsets::encode_ep(
            o,
            if next == 0 {
                Offset::START
            } else {
                Offset(Some(next - 1))
            },
        ),
        None => tail_token(next),
    };
    match outcome {
        Ok(ack) => {
            if !ack.duplicate && state.should_meter(desc.owner(), &name) {
                state.metrics.append(desc.owner(), &name, metric_bytes);
            }
            let status = if ack.duplicate || close_only || producer.is_none() {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::OK
            };
            let mut r = Response::builder()
                .status(status)
                .header("Stream-Next-Offset", tok(ack.next_offset));
            if let Some((pe, ps)) = ack.producer {
                r = r
                    .header("Producer-Epoch", pe.to_string())
                    .header("Producer-Seq", ps.to_string());
            }
            if ack.closed {
                r = r.header("Stream-Closed", "true");
            }
            r.body(Body::empty()).unwrap()
        }
        Err(AppendErr::SeqConflict { current }) => err_resp(
            StatusCode::CONFLICT,
            "seq_conflict",
            &format!("Stream-Seq must exceed {}", current.unwrap_or_default()),
        ),
        Err(AppendErr::Closed { next_offset }) => {
            let mut r = Response::builder()
                .status(StatusCode::CONFLICT)
                .header("Stream-Closed", "true")
                .header("Stream-Next-Offset", tok(next_offset))
                .header(header::CONTENT_TYPE, "application/json");
            r = r.header(header::CACHE_CONTROL, "no-store");
            r.body(Body::from(
                json!({"error": {"code": "stream_closed", "message": "stream is closed"}})
                    .to_string(),
            ))
            .unwrap()
        }
        Err(AppendErr::ProducerGap { expected, received }) => Response::builder()
            .status(StatusCode::CONFLICT)
            .header("Producer-Expected-Seq", expected.to_string())
            .header("Producer-Received-Seq", received.to_string())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"error": {"code": "producer_seq_gap", "message": "sequence gap"}})
                    .to_string(),
            ))
            .unwrap(),
        Err(AppendErr::ProducerStale { current_epoch }) => Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("Producer-Epoch", current_epoch.to_string())
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({"error": {"code": "producer_stale_epoch", "message": "stale epoch"}})
                    .to_string(),
            ))
            .unwrap(),
        Err(AppendErr::ProducerEpochSeq) => err_resp(
            StatusCode::BAD_REQUEST,
            "producer_epoch_seq",
            "a new epoch must start at seq 0",
        ),
        Err(AppendErr::ShardMoved) => {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_moving",
                "shard ownership changed; retry",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            response
        }
        Err(AppendErr::Overloaded) => {
            let mut response = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "overloaded",
                "active stream capacity exhausted; retry",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            response
        }
        Err(AppendErr::CtMismatch) => err_resp(
            StatusCode::CONFLICT,
            "content_type_mismatch",
            "content type mismatch",
        ),
        Err(AppendErr::BadBody(m)) => err_resp(StatusCode::BAD_REQUEST, "invalid_body", &m),
        Err(AppendErr::Internal(m)) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
    }
}

/// A decrypted record ready for response assembly.
struct PlainRec {
    off: u64,
    ts_ms: i64,
    key_version: u32,
    routing_key: String,
    payload: Bytes,
}

struct ReadOut {
    recs: Vec<PlainRec>,
    last: Option<u64>,
    end: u64,
    completed: bool,
}

/// Merged two-tier read returning plaintext records.
#[allow(clippy::too_many_arguments)]
async fn read_records(
    state: &AppState,
    key: &StreamKey,
    epoch: &[u8; 16],
    handle: &Arc<crate::shard::StreamHandle>,
    engine: &Arc<ShardEngine>,
    scan_from: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> Result<ReadOut, String> {
    // The sub-stream identity (AAD + history-DB path): for total-order
    // streams this is the incarnation hash; for per-key streams, the
    // segment hash. Either way it's the handle's identity.
    let hash = handle.hash;
    let (absorbed, end) = {
        let st = handle.state.lock().unwrap();
        (st.durable.absorbed, st.durable.next)
    };
    let mut out = ReadOut {
        recs: Vec::new(),
        last: None,
        end,
        completed: true,
    };
    let mut budget = max_bytes;

    let mut history_completed = true;
    if scan_from < absorbed && budget > 0 {
        let hist = read_history(
            &state.data_store,
            &hash,
            key,
            scan_from,
            absorbed,
            key_filter,
            budget,
        )
        .await
        .map_err(|e| e.to_string())?;
        history_completed = hist.completed;
        for (off, rec) in hist.records {
            budget = budget.saturating_sub(rec.payload.len());
            out.recs.push(PlainRec {
                off,
                ts_ms: rec.ts,
                key_version: rec.key_version,
                routing_key: rec.routing_key,
                payload: rec.payload,
            });
            out.last = Some(off);
        }
    }
    let shard_from = if scan_from < absorbed {
        if history_completed {
            if absorbed > 0 {
                out.last = Some(out.last.map_or(absorbed - 1, |o| o.max(absorbed - 1)));
            }
            absorbed
        } else {
            out.completed = false;
            return Ok(out);
        }
    } else {
        scan_from
    };
    if budget > 0 && shard_from < end {
        let part = read_frames(engine, handle, shard_from, key_filter, budget)
            .await
            .map_err(|e| e.to_string())?;
        let mut subkeys: HashMap<(String, u32), [u8; 32]> = HashMap::new();
        for raw in part.frames {
            let Some(frame) = decode_frame(&raw) else {
                return Err("bad frame".into());
            };
            let sk = *subkeys
                .entry((frame.header.routing_key.clone(), frame.header.key_version))
                .or_insert_with(|| {
                    derive_subkey(
                        key,
                        epoch,
                        &frame.header.routing_key,
                        frame.header.key_version,
                    )
                });
            let pt = decrypt_frame(&sk, &hash, &frame, &raw)?;
            out.recs.push(PlainRec {
                off: frame.header.offset,
                ts_ms: frame.header.ts_ms,
                key_version: frame.header.key_version,
                routing_key: frame.header.routing_key,
                payload: Bytes::from(pt),
            });
        }
        if let Some(last) = part.last_offset {
            out.last = Some(out.last.map_or(last, |o| o.max(last)));
        }
    }
    let consumed_next = out.last.map(|o| o + 1).unwrap_or(scan_from);
    out.completed = consumed_next >= end;
    Ok(out)
}

fn interval_cursor(req_cursor: Option<&str>) -> String {
    let interval = (now_ms() as u64) / 20_000;
    let req: Option<u64> = req_cursor.and_then(|c| c.parse().ok());
    match req {
        Some(r) if r >= interval => (r + 1).to_string(),
        _ => interval.to_string(),
    }
}

fn read_etag(desc: &StreamDesc, scan_from: u64, end: u64, closed: bool) -> String {
    format!(
        "\"{}-{}-{}-{}\"",
        &desc.stream_epoch[..8],
        scan_from,
        end,
        closed as u8
    )
}

fn encode_frame_response(
    key: &StreamKey,
    epoch: &[u8; 16],
    hash: &StorageHash,
    records: &[PlainRec],
) -> Bytes {
    let mut buf = BytesMut::new();
    let mut subkeys: HashMap<(String, u32), [u8; 32]> = HashMap::new();
    for record in records {
        let subkey = *subkeys
            .entry((record.routing_key.clone(), record.key_version))
            .or_insert_with(|| derive_subkey(key, epoch, &record.routing_key, record.key_version));
        let frame = encrypt_frame(
            &subkey,
            hash,
            &FrameHeader {
                offset: record.off,
                ts_ms: record.ts_ms,
                key_version: record.key_version,
                routing_key: record.routing_key.clone(),
            },
            &record.payload,
        );
        buf.extend_from_slice(&frame);
    }
    buf.freeze()
}

enum StartPos {
    At(u64),
    Now,
}

async fn read(
    state: Arc<AppState>,
    customer_id: String,
    name: String,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
) -> Response {
    let mut desc = match state.registry.get(&customer_id, &name).await {
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(Some(d)) => return dead_stream_response(&d),
        Ok(None) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if let Err(response) = require_local_cell(&state, &desc) {
        return response.into_response();
    }
    // A single-segment per-key stream is the degenerate case: totally
    // ordered, epoch-0 tokens — serve it through the standard path so every
    // semantic (incl. unkeyed live reads) is byte-identical.
    if desc.is_per_key() && desc.segment_count.max(1) > 1 {
        return read_per_key(state, desc, params, headers, head_only).await;
    }
    let hash = if desc.is_per_key() {
        desc.segment_hash(0)
    } else {
        desc.storage_hash()
    };
    let engine = match state.engine_for(&desc.routing_hash()).await {
        Ok(e) => e,
        Err(r) => return r,
    };
    let handle = match engine.stream_handle(hash).await {
        Ok(h) => h,
        Err(e) => return storage_err_resp(e),
    };
    let (mut end, mut closed) = {
        let st = handle.state.lock().unwrap();
        (st.durable.next, st.durable.closed)
    };

    if head_only {
        let mut r = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, desc.content_type.clone())
            .header("Stream-Next-Offset", tail_token(end))
            .header(header::CACHE_CONTROL, "no-store");
        if closed {
            r = r.header("Stream-Closed", "true");
        }
        if let (Some(_), Some(exp)) = (desc.ttl_secs, desc.expires_at_ms) {
            let remaining = ((exp - now_ms()) as f64 / 1000.0).ceil() as i64;
            if remaining > 0 {
                r = r.header("Stream-TTL", remaining.to_string());
            }
        }
        return r.body(Body::empty()).unwrap();
    }

    // Reads require the key (fingerprint auth + history decryption).
    let (key, epoch) = match check_key(raw_key(&headers, &state), &desc) {
        KeyCheck::Ok(k, e) => (k, e),
        KeyCheck::Missing => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Stream-Encryption-Key required",
            );
        }
        KeyCheck::Wrong => return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"),
        KeyCheck::BadDescriptor => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "bad descriptor",
            );
        }
    };
    state.keys.put(hash, key.clone(), epoch);

    // HEAD is metadata-only and explicitly does not renew a sliding TTL.
    // Every authenticated GET, including offset=now and live handshakes, does.
    desc = match renew_sliding_ttl(&state, &desc).await {
        Ok(desc) => desc,
        Err(response) => return response,
    };

    let live = match params.live.as_deref() {
        None => None,
        Some("long-poll") | Some("true") => Some("long-poll"),
        Some("sse") => Some("sse"),
        Some(_) => return err_resp(StatusCode::BAD_REQUEST, "invalid_live", "invalid live mode"),
    };
    if live.is_some() && params.offset.is_none() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "missing_offset",
            "live reads require offset",
        );
    }
    let start = match params.offset.as_deref() {
        None => StartPos::At(0),
        Some("now") => StartPos::Now,
        Some(raw) => match Offset::parse(raw) {
            Ok(o) => StartPos::At(o.scan_from()),
            Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_offset", &m),
        },
    };

    if live == Some("sse") {
        return sse_response(state, desc, key, epoch, engine, handle, start, params).await;
    }

    let scan_from = match start {
        StartPos::Now => {
            // Instant tail snapshot for plain reads; long-poll from `now`
            // falls through with scan_from = current end.
            if live.is_none() {
                let body: Body = if desc.is_json() {
                    Body::from("[]")
                } else {
                    Body::empty()
                };
                let mut r = Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, desc.content_type.clone())
                    .header("Stream-Next-Offset", tail_token(end))
                    .header("Stream-Up-To-Date", "true")
                    .header(header::CACHE_CONTROL, "no-store")
                    .header("Cross-Origin-Resource-Policy", "cross-origin");
                if closed {
                    r = r.header("Stream-Closed", "true");
                }
                return r.body(body).unwrap();
            }
            end
        }
        StartPos::At(p) => p,
    };

    let is_long_poll = live == Some("long-poll");
    if is_long_poll && scan_from >= end {
        if !closed {
            let wait = params
                .timeout
                .as_deref()
                .and_then(parse_duration)
                .unwrap_or(Duration::from_secs(3))
                .min(MAX_LONG_POLL);
            let deadline = tokio::time::Instant::now() + wait;
            loop {
                let notified = handle.notify.notified();
                let (e2, c2) = {
                    let st = handle.state.lock().unwrap();
                    (st.durable.next, st.durable.closed)
                };
                end = e2;
                closed = c2;
                if end > scan_from || closed {
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep_until(deadline) => break,
                }
            }
        }
        if end <= scan_from {
            // Timeout (or closed-at-tail): 204 with resume state. Metered:
            // a tail probe is billable work even when it returns no bytes
            // (run-1 finding: `offset=now` reads were invisible to billing).
            if state.should_meter(desc.owner(), &name) {
                state.metrics.read(desc.owner(), &name, 0);
            }
            let mut r = Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header("Stream-Next-Offset", tail_token(end))
                .header("Stream-Up-To-Date", "true")
                .header("Stream-Cursor", interval_cursor(params.cursor.as_deref()))
                .header(header::CACHE_CONTROL, "no-store");
            if closed {
                r = r.header("Stream-Closed", "true");
            }
            return r.body(Body::empty()).unwrap();
        }
    }

    let frames_format = params.format.as_deref() == Some("frames");
    let out = match read_records(
        &state,
        &key,
        &epoch,
        &handle,
        &engine,
        scan_from,
        params.key.as_deref(),
        MAX_READ_BYTES,
    )
    .await
    {
        Ok(o) => o,
        Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
    };
    let next_token = out
        .last
        .map(|o| Offset(Some(o)).encode())
        .unwrap_or_else(|| match params.offset.as_deref() {
            Some(raw) if raw != "now" => Offset::parse(raw)
                .map(|o| o.encode())
                .unwrap_or_else(|_| tail_token(out.end)),
            _ => tail_token(out.end),
        });
    let up_to_date = out.completed;
    let etag = read_etag(&desc, scan_from, out.end, closed);
    if let Some(inm) = hdr(&headers, "if-none-match")
        && inm == etag
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header("ETag", etag)
            .body(Body::empty())
            .unwrap();
    }

    let body: Bytes = if desc.is_json() && !frames_format {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"[");
        for (i, r) in out.recs.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b",");
            }
            buf.extend_from_slice(&r.payload);
        }
        buf.extend_from_slice(b"]");
        buf.freeze()
    } else if frames_format {
        encode_frame_response(&key, &epoch, &hash, &out.recs)
    } else {
        let mut buf = BytesMut::new();
        for r in &out.recs {
            buf.extend_from_slice(&r.payload);
        }
        buf.freeze()
    };

    if is_long_poll
        && up_to_date
        && let Some(delivered_next) = out.last.map(|offset| offset.saturating_add(1))
        && let Some(freshness) = handle.tail_freshness(delivered_next)
    {
        state.telemetry.record_tail_freshness(freshness);
    }
    if state.should_meter(desc.owner(), &name) {
        state.metrics.read(desc.owner(), &name, body.len() as u64);
    }
    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            if frames_format {
                "application/x-durable-stream-frames".to_string()
            } else {
                desc.content_type.clone()
            },
        )
        .header("Stream-Next-Offset", next_token)
        .header("ETag", etag)
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if up_to_date {
        r = r.header("Stream-Up-To-Date", "true");
        if closed {
            r = r.header("Stream-Closed", "true");
        }
    }
    if is_long_poll {
        r = r
            .header("Stream-Cursor", interval_cursor(params.cursor.as_deref()))
            .header(header::CACHE_CONTROL, "no-store");
    }
    r.body(Body::from(body)).unwrap()
}

// ---- SSE ----

fn sse_data_event(desc: &StreamDesc, payload: &[u8]) -> String {
    let mut ev = String::from("event: data\n");
    let mt = crate::registry::media_type(&desc.content_type);
    if mt == "application/json" {
        ev.push_str("data:[");
        ev.push_str(&String::from_utf8_lossy(payload));
        ev.push_str("]\n\n");
    } else if mt.starts_with("text/") {
        // SSE treats CR, LF, and CRLF as line endings. Normalize CRLF as one
        // boundary so attacker-controlled payloads remain literal data lines
        // without introducing an empty event boundary or duplicating newlines.
        let text = String::from_utf8_lossy(payload);
        let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
        for line in normalized.split('\n') {
            ev.push_str("data:");
            ev.push_str(line);
            ev.push('\n');
        }
        ev.push('\n');
    } else {
        use base64::Engine;
        ev.push_str("data:");
        ev.push_str(&base64::engine::general_purpose::STANDARD.encode(payload));
        ev.push_str("\n\n");
    }
    ev
}

fn sse_control(next: u64, cursor: Option<&str>, up_to_date: bool, closed: bool) -> String {
    let mut fields = vec![format!("\"streamNextOffset\":\"{}\"", tail_token(next))];
    if !closed {
        fields.push(format!("\"streamCursor\":\"{}\"", interval_cursor(cursor)));
    }
    if up_to_date {
        fields.push("\"upToDate\":true".to_string());
    }
    if closed {
        fields.push("\"streamClosed\":true".to_string());
    }
    format!("event: control\ndata:{{{}}}\n\n", fields.join(","))
}

#[allow(clippy::too_many_arguments)]
async fn sse_response(
    state: Arc<AppState>,
    desc: StreamDesc,
    key: StreamKey,
    epoch: [u8; 16],
    engine: Arc<ShardEngine>,
    handle: Arc<crate::shard::StreamHandle>,
    start: StartPos,
    params: ReadParams,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let binary = {
        let mt = crate::registry::media_type(&desc.content_type);
        mt != "application/json" && !mt.starts_with("text/")
    };
    let cursor = params.cursor.clone();
    let key_filter = params.key.clone();

    tokio::spawn(async move {
        let mut pos = match start {
            StartPos::At(p) => p,
            StartPos::Now => handle.state.lock().unwrap().durable.next,
        };
        let from_now = matches!(start, StartPos::Now);
        let mut first = true;
        loop {
            let (end, closed) = {
                let st = handle.state.lock().unwrap();
                (st.durable.next, st.durable.closed)
            };
            let mut sent_any = false;
            if (!first || !from_now) && pos < end {
                match read_records(
                    &state,
                    &key,
                    &epoch,
                    &handle,
                    &engine,
                    pos,
                    key_filter.as_deref(),
                    MAX_READ_BYTES,
                )
                .await
                {
                    Ok(out) => {
                        for (index, r) in out.recs.iter().enumerate() {
                            let ev = sse_data_event(&desc, &r.payload);
                            let next = r.off + 1;
                            let is_last = index + 1 == out.recs.len();
                            let caught_up = is_last && out.completed && next >= end;
                            let ctl = sse_control(
                                next,
                                cursor.as_deref(),
                                caught_up,
                                closed && caught_up,
                            );
                            // Keep the protocol-mandated data/control pair in
                            // one transport chunk. Besides reducing framing
                            // ambiguity, this prevents payload text such as
                            // "event: control" from making a chunk-oriented
                            // client stop before receiving the real control.
                            if tx
                                .send(Ok(Bytes::from(format!("{ev}{ctl}"))))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            if caught_up && let Some(freshness) = handle.tail_freshness(next) {
                                state.telemetry.record_tail_freshness(freshness);
                            }
                            sent_any = true;
                            if closed && caught_up {
                                return;
                            }
                        }
                        if let Some(last) = out.last {
                            pos = last + 1;
                        }
                        if !out.completed {
                            continue; // keep draining before control
                        }
                    }
                    Err(_) => return,
                }
            }
            let at_end = pos >= end;
            if !sent_any && (at_end || first) {
                let ctl = sse_control(pos, cursor.as_deref(), at_end, closed && at_end);
                if tx.send(Ok(Bytes::from(ctl))).await.is_err() {
                    return;
                }
                if closed && at_end {
                    return; // final control sent; close connection
                }
            }
            first = false;
            // Wait for new durable data.
            let notified = handle.notify.notified();
            let cur_end = handle.state.lock().unwrap().durable.next;
            if cur_end > pos {
                continue;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(Duration::from_secs(15)) => {
                    // heartbeat comment keeps proxies happy
                    if tx.send(Ok(Bytes::from(": keep-alive\n\n"))).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if binary {
        r = r.header("Stream-SSE-Data-Encoding", "base64");
    }
    r.body(Body::from_stream(stream)).unwrap()
}

// ---- per-key ordering read surface (PER-KEY-ORDERING.md §4) ----

async fn read_per_key(
    state: Arc<AppState>,
    desc: StreamDesc,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
) -> Response {
    let n = desc.segment_count.max(1);
    let seg_tok = |ord: u32, next: u64| {
        crate::offsets::encode_ep(
            ord,
            if next == 0 {
                Offset::START
            } else {
                Offset(Some(next - 1))
            },
        )
    };
    // All segments of a per-key stream live in the parent stream's shard
    // (routing unit = stream; Pravega-style cross-shard segments deferred).
    let parent_engine = match state.engine_for(&desc.routing_hash()).await {
        Ok(e) => e,
        Err(r) => return r,
    };
    let seg_handle = |ord: u32| {
        let hash = desc.segment_hash(ord);
        (hash, parent_engine.clone())
    };

    if head_only {
        // No single end-of-stream offset exists; report the highest-ordinal
        // segment's tail plus the segment count (spec accommodation #2).
        let (hash, engine) = seg_handle(n - 1);
        let (end, closed) = match engine.stream_handle(hash).await {
            Ok(h) => {
                let st = h.state.lock().unwrap();
                (st.durable.next, st.durable.closed)
            }
            Err(e) => return storage_err_resp(e),
        };
        let mut r = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, desc.content_type.clone())
            .header("Stream-Next-Offset", seg_tok(n - 1, end))
            .header("Stream-Ordering", "per-key")
            .header("Stream-Segment-Count", n.to_string())
            .header(header::CACHE_CONTROL, "no-store");
        if closed {
            r = r.header("Stream-Closed", "true");
        }
        return r.body(Body::empty()).unwrap();
    }

    let (key, epoch) = match check_key(raw_key(&headers, &state), &desc) {
        KeyCheck::Ok(k, e) => (k, e),
        KeyCheck::Missing => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Stream-Encryption-Key required",
            );
        }
        KeyCheck::Wrong => return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"),
        KeyCheck::BadDescriptor => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "bad descriptor",
            );
        }
    };

    let live = match params.live.as_deref() {
        None => None,
        Some("long-poll") | Some("true") => Some("long-poll"),
        Some("sse") => Some("sse"),
        Some(_) => return err_resp(StatusCode::BAD_REQUEST, "invalid_live", "invalid live mode"),
    };
    if live.is_some() && params.offset.is_none() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "missing_offset",
            "live reads require offset",
        );
    }
    // Accommodation #1: whole-stream live tails have no single durable
    // cursor across concurrent segments.
    if live.is_some() && params.key.is_none() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "unsupported_on_per_key",
            "live reads on per-key streams require key=",
        );
    }

    // Resolve start (ordinal, position).
    let parsed = match params.offset.as_deref() {
        None => Some((0u32, 0u64)),
        Some("now") => None, // handled per mode below
        Some(raw) => match crate::offsets::parse_ep(raw) {
            Ok((e, o)) if e < n => Some((e, o.scan_from())),
            Ok(_) => return err_resp(StatusCode::BAD_REQUEST, "invalid_offset", "unknown segment"),
            Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_offset", &m),
        },
    };

    if let Some(rk) = params.key.as_deref() {
        // Keyed read: single-segment chain in v1.
        let ord = desc.segment_for(rk);
        let (hash, engine) = seg_handle(ord);
        state.keys.put(hash, key.clone(), epoch);
        let handle = match engine.stream_handle(hash).await {
            Ok(h) => h,
            Err(e) => return storage_err_resp(e),
        };
        let (mut end, closed) = {
            let st = handle.state.lock().unwrap();
            (st.durable.next, st.durable.closed)
        };
        let scan_from = match parsed {
            None => end, // now
            Some((e, p)) => {
                if e != ord && params.offset.as_deref() != Some("-1") && !(e == 0 && p == 0) {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "invalid_offset",
                        "offset segment does not own this key",
                    );
                }
                p
            }
        };
        if live == Some("long-poll") && scan_from >= end {
            if !closed {
                let wait = params
                    .timeout
                    .as_deref()
                    .and_then(parse_duration)
                    .unwrap_or(Duration::from_secs(3))
                    .min(MAX_LONG_POLL);
                let deadline = tokio::time::Instant::now() + wait;
                loop {
                    let notified = handle.notify.notified();
                    end = handle.state.lock().unwrap().durable.next;
                    if end > scan_from {
                        break;
                    }
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                }
            }
            if end <= scan_from {
                let mut r = Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header("Stream-Next-Offset", seg_tok(ord, end))
                    .header("Stream-Ordering", "per-key")
                    .header("Stream-Up-To-Date", "true")
                    .header("Stream-Cursor", interval_cursor(params.cursor.as_deref()))
                    .header(header::CACHE_CONTROL, "no-store");
                if closed {
                    r = r.header("Stream-Closed", "true");
                }
                return r.body(Body::empty()).unwrap();
            }
        }
        if live == Some("sse") {
            let start = match parsed {
                None => StartPos::Now,
                Some((_, p)) => StartPos::At(p),
            };
            return sse_response(state, desc, key, epoch, engine, handle, start, params).await;
        }
        let out = match read_records(
            &state,
            &key,
            &epoch,
            &handle,
            &engine,
            scan_from,
            Some(rk),
            MAX_READ_BYTES,
        )
        .await
        {
            Ok(o) => o,
            Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
        };
        return per_key_body(
            &desc,
            out,
            ord,
            scan_from,
            closed,
            seg_tok,
            live.is_some(),
            &params,
        );
    }

    // Unkeyed: segment-sequential replay (accommodation: per-segment order).
    let (mut ord, mut pos) = match parsed {
        None => {
            // offset=now: tail of the highest ordinal (HEAD semantics).
            let (hash, engine) = seg_handle(n - 1);
            let end = match engine.stream_handle(hash).await {
                Ok(h) => h.state.lock().unwrap().durable.next,
                Err(e) => return storage_err_resp(e),
            };
            let body: Body = if desc.is_json() {
                Body::from("[]")
            } else {
                Body::empty()
            };
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, desc.content_type.clone())
                .header("Stream-Next-Offset", seg_tok(n - 1, end))
                .header("Stream-Ordering", "per-key")
                .header("Stream-Up-To-Date", "true")
                .header(header::CACHE_CONTROL, "no-store")
                .body(body)
                .unwrap();
        }
        Some(v) => v,
    };
    let mut recs: Vec<PlainRec> = Vec::new();
    let mut budget = MAX_READ_BYTES;
    let mut last_tok: String;
    let mut up_to_date = false;
    let mut closed_at_end = false;
    loop {
        let (hash, engine) = seg_handle(ord);
        state.keys.put(hash, key.clone(), epoch);
        let handle = match engine.stream_handle(hash).await {
            Ok(h) => h,
            Err(e) => return storage_err_resp(e),
        };
        let closed = handle.state.lock().unwrap().durable.closed;
        let out =
            match read_records(&state, &key, &epoch, &handle, &engine, pos, None, budget).await {
                Ok(o) => o,
                Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
            };
        for r in &out.recs {
            budget = budget.saturating_sub(r.payload.len());
        }
        let consumed = out.last.map(|o| o + 1).unwrap_or(pos);
        last_tok = seg_tok(ord, consumed);
        recs.extend(out.recs);
        if !out.completed || budget == 0 {
            break;
        }
        if ord + 1 < n {
            ord += 1;
            pos = 0;
            continue;
        }
        up_to_date = true;
        closed_at_end = closed;
        break;
    }
    let etag = format!(
        "\"{}-pk-{}-{}\"",
        &desc.stream_epoch[..8],
        last_tok,
        up_to_date as u8
    );
    if let Some(inm) = hdr(&headers, "if-none-match")
        && inm == etag
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .header("ETag", etag)
            .body(Body::empty())
            .unwrap();
    }
    let body: Bytes = if desc.is_json() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"[");
        for (i, r) in recs.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b",");
            }
            buf.extend_from_slice(&r.payload);
        }
        buf.extend_from_slice(b"]");
        buf.freeze()
    } else {
        let mut buf = BytesMut::new();
        for r in &recs {
            buf.extend_from_slice(&r.payload);
        }
        buf.freeze()
    };
    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, desc.content_type.clone())
        .header("Stream-Next-Offset", last_tok)
        .header("Stream-Ordering", "per-key")
        .header("ETag", etag)
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if up_to_date {
        r = r.header("Stream-Up-To-Date", "true");
        if closed_at_end {
            r = r.header("Stream-Closed", "true");
        }
    }
    r.body(Body::from(body)).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn per_key_body(
    desc: &StreamDesc,
    out: ReadOut,
    ord: u32,
    scan_from: u64,
    closed: bool,
    seg_tok: impl Fn(u32, u64) -> String,
    is_live: bool,
    params: &ReadParams,
) -> Response {
    let consumed = out.last.map(|o| o + 1).unwrap_or(scan_from);
    let up_to_date = out.completed;
    let etag = format!(
        "\"{}-pk{}-{}-{}\"",
        &desc.stream_epoch[..8],
        ord,
        consumed,
        out.end
    );
    let body: Bytes = if desc.is_json() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"[");
        for (i, r) in out.recs.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b",");
            }
            buf.extend_from_slice(&r.payload);
        }
        buf.extend_from_slice(b"]");
        buf.freeze()
    } else {
        let mut buf = BytesMut::new();
        for r in &out.recs {
            buf.extend_from_slice(&r.payload);
        }
        buf.freeze()
    };
    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, desc.content_type.clone())
        .header("Stream-Next-Offset", seg_tok(ord, consumed))
        .header("Stream-Ordering", "per-key")
        .header("ETag", etag)
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if up_to_date {
        r = r.header("Stream-Up-To-Date", "true");
        if closed {
            r = r.header("Stream-Closed", "true");
        }
    }
    if is_live {
        r = r
            .header("Stream-Cursor", interval_cursor(params.cursor.as_deref()))
            .header(header::CACHE_CONTROL, "no-store");
    }
    r.body(Body::from(body)).unwrap()
}

// ---- queue profile surface (PROFILES.md §7; CF-informed) ----

#[derive(Deserialize, Default)]
struct QueueSettleBody {
    #[serde(default)]
    acks: Vec<QueueTokenRef>,
    #[serde(default)]
    retries: Vec<QueueRetryRef>,
    #[serde(default)]
    extends: Vec<QueueExtendRef>,
}

#[derive(Deserialize)]
struct QueueTokenRef {
    #[serde(rename = "leaseToken")]
    lease_token: String,
}

#[derive(Deserialize)]
struct QueueRetryRef {
    #[serde(rename = "leaseToken")]
    lease_token: String,
    #[serde(default)]
    #[serde(rename = "delayMs")]
    delay_ms: u64,
}

#[derive(Deserialize)]
struct QueueExtendRef {
    #[serde(rename = "leaseToken")]
    lease_token: String,
    #[serde(default = "default_visibility")]
    #[serde(rename = "visibilityMs")]
    visibility_ms: u64,
}

fn default_visibility() -> u64 {
    30_000
}

#[derive(Deserialize, Default)]
struct QueueReceiveBody {
    #[serde(default)]
    #[serde(rename = "batchSize")]
    batch_size: Option<usize>,
    #[serde(default)]
    #[serde(rename = "visibilityMs")]
    visibility_ms: Option<u64>,
    #[serde(default)]
    #[serde(rename = "waitMs")]
    wait_ms: Option<u64>,
}

async fn queue_entry(
    state: Arc<AppState>,
    customer_id: String,
    stream: String,
    route: String,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let desc = match state.registry.get(&customer_id, &stream).await {
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(_) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if let Err(response) = require_local_cell(&state, &desc) {
        return response.into_response();
    }
    if desc.profile.as_deref() != Some("queue") {
        return err_resp(
            StatusCode::NOT_FOUND,
            "not_found",
            "queue profile not enabled",
        );
    }
    let Some((consumer, verb)) = route.split_once('/') else {
        return err_resp(
            StatusCode::NOT_FOUND,
            "not_found",
            "queue route: {consumer}/{verb}",
        );
    };
    if consumer.is_empty() || consumer.len() > 128 || consumer.contains('\u{0}') {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "invalid_consumer",
            "bad consumer name",
        );
    }
    if method != Method::POST {
        return err_resp(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "POST only",
        );
    }
    let (key, epoch) = match check_key(raw_key(&headers, &state), &desc) {
        KeyCheck::Ok(k, e) => (k, e),
        KeyCheck::Missing => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "missing_key",
                "Stream-Encryption-Key required",
            );
        }
        KeyCheck::Wrong => return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"),
        KeyCheck::BadDescriptor => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "bad descriptor",
            );
        }
    };
    let hash = desc.storage_hash();
    state.keys.put(hash, key.clone(), epoch);
    let engine = match state.engine_for(&desc.routing_hash()).await {
        Ok(e) => e,
        Err(r) => return r,
    };
    let handle = match engine.stream_handle(hash).await {
        Ok(h) => h,
        Err(e) => return storage_err_resp(e),
    };
    let max_deliveries = desc.queue_max_deliveries.unwrap_or(5);
    let dlq_subkey = derive_subkey(&key, &epoch, "$dlq", 0);
    let raw = match body_with_quota(body, &state, &customer_id, 1 << 20, None).await {
        Ok(b) => b,
        Err(response) => return response,
    };
    if state.should_meter(desc.owner(), &stream) {
        state.metrics.queue(desc.owner(), &stream);
    }

    match verb {
        "receive" => {
            let req: QueueReceiveBody = if raw.is_empty() {
                QueueReceiveBody::default()
            } else {
                match serde_json::from_slice(&raw) {
                    Ok(r) => r,
                    Err(e) => {
                        return err_resp(
                            StatusCode::BAD_REQUEST,
                            "invalid_request",
                            &e.to_string(),
                        );
                    }
                }
            };
            let max = req.batch_size.unwrap_or(5).clamp(1, 100);
            let visibility = req
                .visibility_ms
                .unwrap_or(30_000)
                .clamp(1_000, 12 * 3600 * 1000);
            let wait = req.wait_ms.unwrap_or(0).min(25_000);
            let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
            loop {
                let out = engine
                    .submit_queue(
                        customer_id.clone(),
                        hash,
                        desc.commit_weight.unwrap_or(1).clamp(1, 100),
                        crate::queue::QueueOp::Receive {
                            consumer: consumer.to_string(),
                            max,
                            visibility_ms: visibility,
                            max_deliveries,
                            dlq_subkey,
                        },
                    )
                    .await;
                let (leased, backlog) = match out {
                    Ok(crate::queue::QueueOut::Received { leased, backlog }) => (leased, backlog),
                    Ok(_) => unreachable!(),
                    Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
                };
                if leased.is_empty() && tokio::time::Instant::now() < deadline {
                    // Long-poll for new messages, then try leasing again.
                    let notified = handle.notify.notified();
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(deadline) => {}
                    }
                    if tokio::time::Instant::now() < deadline {
                        continue;
                    }
                }
                // Fetch + decrypt payloads for the leased offsets.
                let mut messages = Vec::with_capacity(leased.len());
                if !leased.is_empty() {
                    let lo = leased.iter().map(|(o, _, _)| *o).min().unwrap();
                    let hi = leased.iter().map(|(o, _, _)| *o).max().unwrap();
                    let out = match read_records(
                        &state,
                        &key,
                        &epoch,
                        &handle,
                        &engine,
                        lo,
                        None,
                        MAX_READ_BYTES,
                    )
                    .await
                    {
                        Ok(o) => o,
                        Err(m) => {
                            return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m);
                        }
                    };
                    let by_off: HashMap<u64, &PlainRec> =
                        out.recs.iter().map(|r| (r.off, r)).collect();
                    let _ = hi;
                    for (off, lease_gen, attempts) in &leased {
                        let Some(rec) = by_off.get(off) else { continue };
                        let payload: serde_json::Value = if desc.is_json() {
                            serde_json::from_slice(&rec.payload).unwrap_or(serde_json::Value::Null)
                        } else {
                            use base64::Engine;
                            serde_json::Value::String(
                                base64::engine::general_purpose::STANDARD.encode(&rec.payload),
                            )
                        };
                        messages.push(json!({
                            "id": Offset(Some(*off)).encode(),
                            "offset": off,
                            "attempts": attempts,
                            "leaseToken": format!("{off}:{lease_gen}"),
                            "body": payload,
                        }));
                    }
                }
                return (
                    [
                        (header::CONTENT_TYPE, "application/json"),
                        (header::CACHE_CONTROL, "no-store"),
                    ],
                    json!({"messages": messages, "backlog": backlog}).to_string(),
                )
                    .into_response();
            }
        }
        "ack" => {
            let req: QueueSettleBody = match serde_json::from_slice(&raw) {
                Ok(r) => r,
                Err(e) => {
                    return err_resp(StatusCode::BAD_REQUEST, "invalid_request", &e.to_string());
                }
            };
            let parse = |t: &str| crate::queue::parse_token(t);
            let acks = req
                .acks
                .iter()
                .filter_map(|a| parse(&a.lease_token))
                .collect();
            let retries = req
                .retries
                .iter()
                .filter_map(|r| parse(&r.lease_token).map(|(o, g)| (o, g, r.delay_ms)))
                .collect();
            let extends = req
                .extends
                .iter()
                .filter_map(|r| parse(&r.lease_token).map(|(o, g)| (o, g, r.visibility_ms)))
                .collect();
            let out = engine
                .submit_queue(
                    customer_id.clone(),
                    hash,
                    desc.commit_weight.unwrap_or(1).clamp(1, 100),
                    crate::queue::QueueOp::Settle {
                        consumer: consumer.to_string(),
                        acks,
                        retries,
                        extends,
                        max_deliveries,
                        dlq_subkey,
                    },
                )
                .await;
            match out {
                Ok(crate::queue::QueueOut::Settled {
                    acked,
                    retried,
                    extended,
                    dlq,
                    backlog,
                }) => (
                    [
                        (header::CONTENT_TYPE, "application/json"),
                        (header::CACHE_CONTROL, "no-store"),
                    ],
                    json!({
                        "acked": acked, "retried": retried, "extended": extended,
                        "dlq": dlq, "backlog": backlog,
                    })
                    .to_string(),
                )
                    .into_response(),
                Ok(_) => unreachable!(),
                Err(m) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
            }
        }
        _ => err_resp(StatusCode::NOT_FOUND, "not_found", "unknown queue verb"),
    }
}

// ---- internal metrics stream flusher (old-impl pattern: __stream_metrics__) ----

pub async fn metrics_flusher(
    state: Arc<AppState>,
    metrics_key: String,
    instance: String,
    lb_url: String,
    auth: String,
    export_interval: Duration,
) {
    // Billing records go through the ROUTER like any tenant write (run-3
    // finding: local appends to a shared-namespace stream fence-fight the
    // shard's ring owner). One drained, serialized interval remains pending
    // with the same producer sequence until the append is acknowledged.
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(4))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("metrics http client");
    let url = format!("{}/v1/stream/__metrics__", lb_url.trim_end_matches('/'));
    let mut created = false;
    let mut seq = 0u64;
    let process_id = format!(
        "metrics-{}-{:032x}",
        crate::crypto::hex(&crate::crypto::stream_hash(&instance)),
        rand::random::<u128>()
    );
    let mut pending: Option<Vec<u8>> = None;
    let mut tick = tokio::time::interval(export_interval);
    loop {
        tick.tick().await;
        if pending.is_none() {
            let drained = state.metrics.drain();
            if drained.is_empty() {
                continue;
            }
            let record = json!([{
                "ts_ms": now_ms(),
                "instance": instance,
                "process_id": process_id,
                "seq": seq,
                "interval_s": export_interval.as_secs(),
                "streams": drained.streams,
                "dropped_series": drained.dropped_series,
            }]);
            pending = Some(serde_json::to_vec(&record).expect("serialize metrics export"));
        }
        if !created {
            match client
                .put(&url)
                .header("authorization", format!("Bearer {auth}"))
                .header("stream-encryption-key", &metrics_key)
                .header("content-type", "application/json")
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => created = true,
                Ok(r) => {
                    tracing::warn!("metrics stream create via router: {}", r.status());
                    state.metrics.record_export_result(false);
                    continue;
                }
                Err(e) => {
                    tracing::warn!("metrics stream create via router: {e}");
                    state.metrics.record_export_result(false);
                    continue;
                }
            }
        }
        match client
            .post(&url)
            .header("authorization", format!("Bearer {auth}"))
            .header("stream-encryption-key", &metrics_key)
            .header("content-type", "application/json")
            .header("producer-id", &process_id)
            .header("producer-epoch", "0")
            .header("producer-seq", seq.to_string())
            .body(pending.as_ref().expect("pending export").clone())
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                state.metrics.record_export_result(true);
                pending = None;
                seq = seq.saturating_add(1);
            }
            Ok(r) => {
                state.metrics.record_export_result(false);
                tracing::warn!("metrics append via router: {}", r.status());
            }
            Err(e) => {
                state.metrics.record_export_result(false);
                tracing::warn!("metrics append via router: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission_config(
        max_inflight: usize,
        write_bytes_per_second: u64,
        write_burst_bytes: u64,
    ) -> TenantAdmissionConfig {
        TenantAdmissionConfig {
            max_inflight,
            max_live_connections: max_inflight,
            write_bytes_per_second,
            write_burst_bytes,
            append_requests_per_second: 0,
            append_request_burst: 1,
            read_requests_per_second: 0,
            read_request_burst: 1,
            read_bytes_per_second: 0,
            read_burst_bytes: 1,
            queue_receives_per_second: 0,
            queue_receive_burst: 1,
        }
    }

    fn stream_admission_config() -> StreamAdmissionConfig {
        StreamAdmissionConfig {
            append_requests_per_second: 1,
            append_request_burst: 1,
            write_bytes_per_second: 1,
            write_burst_bytes: 4,
            commit_weight: 1,
        }
    }

    fn admission_descriptor(name: &str, limits: ResolvedStreamLimits) -> StreamDesc {
        fresh_desc(
            "customer-a",
            String::new(),
            name,
            &StreamKey([7; 32]),
            "application/octet-stream".to_string(),
            None,
            None,
            None,
            Vec::new(),
            None,
            0,
            None,
            limits,
        )
    }

    #[test]
    fn frame_response_preserves_original_envelope_metadata() {
        let key = StreamKey([9u8; 32]);
        let epoch = [7u8; 16];
        let hash = [9u8; 32];
        let records = vec![
            PlainRec {
                off: 42,
                ts_ms: 1_751_900_123_456,
                key_version: 3,
                routing_key: "chat/a".to_string(),
                payload: Bytes::from_static(b"first"),
            },
            PlainRec {
                off: 43,
                ts_ms: 1_751_900_123_999,
                key_version: 4,
                routing_key: "chat/b".to_string(),
                payload: Bytes::from_static(b"second"),
            },
        ];

        let mut expected = Vec::new();
        for record in &records {
            let subkey = derive_subkey(&key, &epoch, &record.routing_key, record.key_version);
            expected.extend_from_slice(&encrypt_frame(
                &subkey,
                &hash,
                &FrameHeader {
                    offset: record.off,
                    ts_ms: record.ts_ms,
                    key_version: record.key_version,
                    routing_key: record.routing_key.clone(),
                },
                &record.payload,
            ));
        }

        assert_eq!(
            encode_frame_response(&key, &epoch, &hash, &records),
            expected
        );
    }

    #[test]
    fn tenant_admission_is_isolated_and_releases_cardinality() {
        let admission = TenantAdmission::new(admission_config(1, 10, 10));
        let limits = crate::registry::CustomerLimits::default();
        let first = admission.enter("customer-a", &limits, false, 1).unwrap();
        assert_eq!(
            admission.enter("customer-a", &limits, false, 1).err(),
            Some(ThrottleReason {
                scope: "customer",
                dimension: "connections",
                limit: 1,
                observed: 2,
            })
        );
        let other = admission.enter("customer-b", &limits, false, 1).unwrap();
        assert_eq!(admission.inner.lock().unwrap().customers.len(), 2);
        assert!(admission.charge_write("customer-a", 10).is_ok());
        assert_eq!(
            admission.charge_write("customer-a", 1).unwrap_err(),
            ThrottleReason {
                scope: "customer",
                dimension: "write_burst_bytes",
                limit: 10,
                observed: 11,
            }
        );

        drop(first);
        assert!(admission.enter("customer-a", &limits, false, 1).is_ok());
        drop(other);
        assert!(
            admission
                .inner
                .lock()
                .unwrap()
                .customers
                .values()
                .all(|state| state.inflight == 0)
        );

        let unlimited = crate::registry::CustomerLimits {
            max_inflight: Some(0),
            write_bytes_per_second: Some(0),
            write_burst_bytes: Some(1),
            ..Default::default()
        };
        let _guard = admission
            .enter("customer-unlimited", &unlimited, false, 1)
            .unwrap();
        assert!(
            admission
                .charge_write("customer-unlimited", usize::MAX / 2)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn tenant_admission_lives_until_response_body_closes_or_disconnects() {
        let admission = TenantAdmission::new(admission_config(1, 0, 1));
        let limits = crate::registry::CustomerLimits::default();

        let guard = admission.enter("customer-a", &limits, true, 1).unwrap();
        let response = hold_tenant_admission(
            Response::new(Body::from(Bytes::from_static(b"streaming"))),
            guard,
            false,
        );
        assert_eq!(
            admission
                .enter("customer-a", &limits, true, 1)
                .err()
                .expect("second connection must be throttled"),
            ThrottleReason {
                scope: "customer",
                dimension: "connections",
                limit: 1,
                observed: 2,
            }
        );
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
            Bytes::from_static(b"streaming")
        );
        let guard = admission.enter("customer-a", &limits, true, 1).unwrap();

        // Dropping an unconsumed response models a client disconnect. The
        // body-owned guard must release the slot even if it is never polled.
        let response = hold_tenant_admission(Response::new(Body::empty()), guard, false);
        assert!(admission.enter("customer-a", &limits, true, 1).is_err());
        drop(response);
        assert!(admission.enter("customer-a", &limits, true, 1).is_ok());
    }

    #[test]
    fn tenant_request_and_live_connection_dimensions_are_independent() {
        let mut config = admission_config(4, 0, 1);
        config.max_live_connections = 1;
        config.append_requests_per_second = 1;
        config.append_request_burst = 1;
        config.read_requests_per_second = 1;
        config.read_request_burst = 1;
        config.queue_receives_per_second = 1;
        config.queue_receive_burst = 1;
        let admission = TenantAdmission::new(config);
        let limits = crate::registry::CustomerLimits::default();

        let live = admission.enter("customer-a", &limits, true, 1).unwrap();
        assert_eq!(
            admission.enter("customer-a", &limits, true, 1).err(),
            Some(ThrottleReason {
                scope: "customer",
                dimension: "live_connections",
                limit: 1,
                observed: 2,
            })
        );
        // A finite request still fits under the separate all-request ceiling.
        let finite = admission.enter("customer-a", &limits, false, 1).unwrap();
        for (quota, dimension) in [
            (RequestQuota::Append, "append_burst_requests"),
            (RequestQuota::Read, "read_burst_requests"),
            (RequestQuota::QueueReceive, "queue_receive_burst_requests"),
        ] {
            assert!(admission.charge_request("customer-a", quota).is_ok());
            assert_eq!(
                admission.charge_request("customer-a", quota).unwrap_err(),
                ThrottleReason {
                    scope: "customer",
                    dimension,
                    limit: 1,
                    observed: 2,
                }
            );
        }
        drop((live, finite));
        assert_eq!(
            admission
                .inner
                .lock()
                .unwrap()
                .customers
                .get("customer-a")
                .unwrap()
                .live_connections,
            0
        );
    }

    #[test]
    fn tenant_limits_are_shared_across_fresh_fleet_membership() {
        let mut config = admission_config(4, 0, 1);
        config.append_requests_per_second = 4;
        config.append_request_burst = 4;
        let admission = TenantAdmission::new(config);
        let limits = crate::registry::CustomerLimits::default();

        let _first = admission.enter("customer-a", &limits, false, 2).unwrap();
        let _second = admission.enter("customer-a", &limits, false, 2).unwrap();
        assert_eq!(
            admission.enter("customer-a", &limits, false, 2).err(),
            Some(ThrottleReason {
                scope: "customer",
                dimension: "connections",
                limit: 4,
                observed: 6,
            })
        );
        assert!(
            admission
                .charge_request("customer-a", RequestQuota::Append)
                .is_ok()
        );
        assert!(
            admission
                .charge_request("customer-a", RequestQuota::Append)
                .is_ok()
        );
        assert_eq!(
            admission
                .charge_request("customer-a", RequestQuota::Append)
                .unwrap_err(),
            ThrottleReason {
                scope: "customer",
                dimension: "append_burst_requests",
                limit: 4,
                observed: 6,
            }
        );
    }

    #[test]
    fn read_bandwidth_reservations_serialize_without_dropping_frames() {
        let now = std::time::Instant::now();
        let mut bucket = RateBucket::new(10, 10, 10, 1, now);
        assert_eq!(bucket.reserve_delay(10, now), Duration::ZERO);
        let second = bucket.reserve_delay(10, now);
        let third = bucket.reserve_delay(10, now);
        assert!(second >= Duration::from_millis(999));
        assert!(third >= Duration::from_millis(1999));
    }

    #[test]
    fn stream_admission_is_incarnation_scoped_and_strictly_parsed() {
        let admission = StreamAdmission::new(stream_admission_config());
        let limits = ResolvedStreamLimits {
            append_requests_per_second: 1,
            append_request_burst: 1,
            write_bytes_per_second: 1,
            write_burst_bytes: 4,
            commit_weight: 3,
        };
        let first = admission_descriptor("first", limits);
        let second = admission_descriptor("second", limits);

        assert!(admission.charge_request(&first).is_ok());
        assert_eq!(
            admission.charge_request(&first).unwrap_err(),
            ThrottleReason {
                scope: "stream",
                dimension: "append_burst_requests",
                limit: 1,
                observed: 2,
            }
        );
        assert!(admission.charge_request(&second).is_ok());
        assert!(admission.charge_write(&second, 4).is_ok());
        assert_eq!(
            admission.charge_write(&second, 1).unwrap_err(),
            ThrottleReason {
                scope: "stream",
                dimension: "write_burst_bytes",
                limit: 4,
                observed: 5,
            }
        );

        let valid = HeaderMap::from_iter([
            (
                axum::http::HeaderName::from_static("stream-append-request-burst"),
                axum::http::HeaderValue::from_static("9"),
            ),
            (
                axum::http::HeaderName::from_static("stream-commit-weight"),
                axum::http::HeaderValue::from_static("7"),
            ),
        ]);
        let parsed = admission.requested_limits(&valid).unwrap();
        assert_eq!(parsed.append_request_burst, 9);
        assert_eq!(parsed.commit_weight, 7);
        for (name, value) in [
            ("stream-append-request-burst", "0"),
            ("stream-write-burst-bytes", "0"),
            ("stream-commit-weight", "101"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                axum::http::HeaderValue::from_str(value).unwrap(),
            );
            assert!(admission.requested_limits(&headers).is_err());
        }
    }

    #[tokio::test]
    async fn throttled_response_has_stable_machine_readable_contract() {
        let response = throttled_resp(
            ThrottleReason {
                scope: "customer",
                dimension: "streams_count",
                limit: 2,
                observed: 3,
            },
            1,
        );
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({"error": {
                "code": "throttled",
                "scope": "customer",
                "dimension": "streams_count",
                "limit": 2,
                "observed": 3,
                "retry_after_ms": 1000,
            }})
        );
    }

    #[test]
    fn poison_shard_backoff_is_bounded() {
        assert_eq!(quarantine_delay(2), Duration::ZERO);
        assert_eq!(quarantine_delay(3), Duration::from_secs(5));
        assert_eq!(quarantine_delay(20), Duration::from_secs(300));
    }
}
