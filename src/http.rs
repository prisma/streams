//! HTTP surface (spec §3.4/§3.5): keyed appends, merged reads (history +
//! shard tail), ciphertext frames by default, server-side decryption for
//! `format=json`, long-poll tails.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use bytes::{Bytes, BytesMut};
use object_store::ObjectStore;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::oneshot;

use crate::crypto::{
    FrameHeader, StreamKey, decode_frame, decrypt_frame, derive_subkey, encrypt_frame, hex,
};
use crate::history::KeyCache;
use crate::offsets::Offset;
use crate::registry::{Registry, StreamDesc, shard_for_hash};
use crate::shard::{AppendErr, AppendReq, ShardEngine, now_ms, read_frames};

/// Protocol ceiling on a request body. This is the pinned maximum — a
/// deployment may lower its effective limit (see [`max_body_bytes`]) but
/// never raise it above this.
pub(crate) const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Effective request-body ceiling, set once at startup from
/// MAX_REQUEST_BODY_BYTES.
///
/// CHAOS-3 (2026-08-09): this number is not only an input validator — it
/// sizes the absorber's worst-case frame-build reservation
/// ([`crate::history::absorb_worst_frame_transient`]), which every
/// gather holds against the admission shed line. At the pinned 32 MiB
/// ceiling that reservation is 96.2 MiB, or 19% of the 1 GiB posture's
/// 500 MB shed line, held whenever a gather is in flight — measured in
/// Singapore against gathers whose ACTUAL size averaged 6 MB. A
/// deployment that caps bodies at 1 MiB reserves ~3 MiB instead and
/// buys back ~93 MiB of admission headroom.
static MAX_BODY_LIMIT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(MAX_BODY_BYTES);

pub(crate) fn max_body_bytes() -> usize {
    MAX_BODY_LIMIT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Lower the effective body ceiling. Rejects anything above the pinned
/// protocol maximum (the limit may only be tightened) or below a floor
/// that keeps the product surface usable.
pub(crate) fn set_max_body_bytes(v: usize) -> anyhow::Result<()> {
    const FLOOR: usize = 64 * 1024;
    if v > MAX_BODY_BYTES {
        anyhow::bail!(
            "MAX_REQUEST_BODY_BYTES ({v}) exceeds the pinned protocol ceiling \
             ({MAX_BODY_BYTES}); the limit may only be lowered"
        );
    }
    if v < FLOOR {
        anyhow::bail!("MAX_REQUEST_BODY_BYTES ({v}) is below the {FLOOR}-byte floor");
    }
    MAX_BODY_LIMIT.store(v, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

const MAX_READ_BYTES: usize = 8 * 1024 * 1024;

/// Budget for a read that was WOKEN by a long-poll wait — the live-tail
/// case, where response size is latency: materialize + transfer + client
/// parse + the client's rearm gap all scale with it. Catch-up reads keep
/// the full MAX_READ_BYTES for throughput. Env TAIL_MAX_BYTES.
/// Benchmark-only stage timing on live reads (env STREAMS_DEBUG_TIMING=1):
/// woken long-poll responses carry `Streams-Debug-Wait: waited=<0|1>
/// arm_us=<arm->wake> read_us=<wake->records-built>`, splitting the
/// remaining roundtrip-minus-append interval into its server-side stages.
fn debug_timing() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var("STREAMS_DEBUG_TIMING").as_deref() == Ok("1"))
}

fn tail_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("TAIL_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1024 * 1024)
    })
}
const APPEND_TIMEOUT: Duration = Duration::from_secs(10);
// The platform front door kills any request at ~30 s with a 502 (measured
// 30.16 s on Prisma Compute). Every server-side wait must conclude below it
// so clients see clean empty responses instead of gateway errors.
const MAX_LONG_POLL: Duration = Duration::from_secs(25);

/// Everything needed to open a shard log on demand. Shards are opened
/// lazily on first routed request (COMPUTE-SPEC §5.1): opening fences the
/// previous owner, so ownership follows routing with no coordination.
pub struct ShardOpener {
    pub open: Box<
        dyn Fn(String) -> futures_util::future::BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>>
            + Send
            + Sync,
    >,
}

pub struct AppState {
    pub registry: Registry,
    /// Single-flight, cancellation-proof history DbReader service — one
    /// per process/store (history.rs).
    pub shard_prefixes: Vec<String>,
    /// Serving map, shared with the fleet loop and the OpenGate's spawned
    /// open tasks (which insert into it directly — see sharddir.rs).
    pub shards: std::sync::Arc<std::sync::RwLock<HashMap<String, Arc<ShardEngine>>>>,
    /// Single-flight, cancellation-proof shard opens with escalating
    /// holdoff — the eu-central-1 reopen-storm fix (sharddir.rs).
    pub gate: crate::sharddir::OpenGate,
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
    /// Per-stream inflight append cap (0 = off): one hot stream cannot
    /// occupy every admission slot of its shard owner. Scoped 429 +
    /// Retry-After. The counter map is bounded: entries are removed at
    /// zero, and past `STREAM_INFLIGHT_MAX_TRACKED` new streams are
    /// admitted untracked (fail open on the bound, never leak).
    pub admit_max_inflight_per_stream: i64,
    pub stream_inflight: std::sync::Mutex<HashMap<[u8; 16], i64>>,
    pub stream_shed: std::sync::atomic::AtomicU64,
    /// 429s issued because a shard's commit pipeline was blocked (wedge).
    pub wedge_shed: std::sync::atomic::AtomicU64,
    /// R27-1: the instance-wide maintenance latch, ONE per AppState —
    /// the global machine of the two-machine split (the per-shard
    /// machine lives on each engine's `maintenance_shard_shed`).
    pub maint_latch: crate::backpressure::GlobalLatch,
    /// R29: per-state sweep scheduler bookkeeping (custody marks,
    /// quantum cycles, peak gauge) — process statics summed parallel
    /// test rigs into false bound violations.
    pub sweep_sched: crate::billing::SweepSched,
    /// Fleet-coordination store (heartbeats/desired.json) for the operator
    /// dashboard's cell view; None when running standalone.
    pub fleet_store: Option<Arc<dyn object_store::ObjectStore>>,
    /// This instance's name plus the ring's active instance set, updated by
    /// the fleet loop from desired.json + heartbeat liveness (a selected
    /// instance that has gone heartbeat-dark >30 s is dropped until it
    /// revives). Used for the R2 ring-ownership check: never open a shard
    /// the ring assigns elsewhere, even if a stale router sends it.
    /// Empty = check disabled (fleet mode off or bootstrapping).
    pub instance_name: String,
    pub ring_active: std::sync::RwLock<Vec<String>>,
    /// Rebalancer shard-move overrides (fleet/overrides.json, CAS'd):
    /// shard prefix -> instance. Consulted before the rendezvous pick; an
    /// override whose target is not in the active set is ignored.
    pub ring_overrides: std::sync::RwLock<std::collections::HashMap<String, String>>,
    /// Fresh peers' published base URLs (heartbeat `url`, from SELF_URL),
    /// updated by the fleet loop. Segment fan-out — cross-owner lineage
    /// reads and consumer sweeps — addresses owners through this map;
    /// empty in standalone mode or when SELF_URL isn't deployed.
    pub peer_urls: std::sync::RwLock<std::collections::HashMap<String, String>>,
    pub data_store: Arc<dyn ObjectStore>,
    pub keys: Arc<KeyCache>,
    pub touch: Arc<crate::touch::TouchRegistry>,
    /// Conformance/dev accommodation: used when a request carries no
    /// Stream-Encryption-Key header (the upstream conformance suite cannot
    /// send custom headers). Never set in production.
    pub default_key: Option<String>,
    /// Bearer token required on /v1/* when set (pilot authn). This is the
    /// CUSTOMER-facing account token; it never authorizes /v1/internal/*.
    pub auth_token: Option<String>,
    /// The read-delivery meter (docs/OBSERVABILITY-BILLING.md §7): ONE
    /// accumulator, fed only at the public response coordinator.
    pub billing_reads: Arc<crate::billing::ReadUsageAccumulator>,
    /// System encryption key for `_usage` (§8.1). None = the telemetry
    /// pipeline is off (dev default); production billing mode requires
    /// it at startup.
    pub usage_key: Option<String>,
    /// The usage rollup, when THIS instance runs it (§9.1: one fenced
    /// writer per cell). Set once at startup (ROLLUP=1) or by tests.
    pub rollup: std::sync::OnceLock<Arc<crate::rollup::UsageRollup>>,
    /// Durable per-instance spool for sealed read batches (round-21
    /// blocker 3): sealed usage survives crash and ledger outage.
    pub read_spool: std::sync::OnceLock<Arc<crate::billing::ReadSpool>>,
    /// Billing tenant boundary (docs/OBSERVABILITY-BILLING.md §3.2):
    /// explicit account/project identity from the control plane's
    /// deployment config — never inferred from stream names. Persisted
    /// into every descriptor at creation.
    pub account_id: String,
    /// The deployment's tenant (MULTITENANCY transition posture):
    /// until enforce-mode principals carry per-request projects, a
    /// dedicated cell serves exactly ONE project, configured
    /// explicitly at startup (PROJECT_ID env, validated — a missing or
    /// invalid value refuses boot; there is no silent default at the
    /// storage layer). Every layout-4 registry path and identity hash
    /// derives from this value via `sref()`.
    pub tenant: crate::tenant::ProjectId,
    /// Telemetry source coordinates.
    pub cell_id: String,
    pub region: String,
    /// Value of the `Prisma-Streams-Origin` header stamped on every
    /// response: instance name (or version) — proof the response came
    /// from a Streams server rather than the platform edge.
    pub origin_marker: String,
    /// Fleet-internal credential (FLEET_INTERNAL_TOKEN). A SEPARATE trust
    /// boundary from `auth_token`: peer RPCs can fence consumer
    /// generations and read segment state without a stream key, so a
    /// customer bearer must never reach them, and this token must never
    /// authorize a product operation. Required whenever fleet mode is on
    /// (startup refuses otherwise); None = internal routes fail closed.
    pub fleet_internal_token: Option<String>,
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Constant-time comparison for shared-secret tokens: a byte-by-byte
/// `==` on a secret leaks its prefix length through timing.
fn secret_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Customer/account authorization for the PUBLIC surface. Deliberately
/// does not consult the fleet-internal token: the two credentials are
/// separate trust boundaries (round-19 security finding), so an internal
/// token can never perform a product operation.
pub(crate) fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    match &state.auth_token {
        None => true,
        Some(t) => bearer(headers).map(|v| secret_eq(v, t)).unwrap_or(false),
    }
}

/// Authorization for /v1/internal/* ONLY. Fails closed: without a
/// configured fleet-internal token there is no internal surface at all,
/// and the customer bearer is never accepted here even when it matches
/// the account token. Startup refuses to enable fleet mode without this
/// token, so "None" in production means fleet mode is off.
pub(crate) fn fleet_internal_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    match &state.fleet_internal_token {
        None => false,
        Some(t) => bearer(headers).map(|v| secret_eq(v, t)).unwrap_or(false),
    }
}

/// Uniform 401 for internal routes — never distinguishes "fleet mode
/// off" from "wrong token" to an unauthenticated caller.
pub(crate) fn internal_unauthorized() -> Response {
    err_resp(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "fleet-internal credential required",
    )
}

impl AppState {
    /// Project-qualify a canonical stream name under the deployment
    /// tenant. `name` MUST already be canonical (`canonical_name` ran
    /// at the route boundary); the checked construction here is the
    /// invariant that keeps unvalidated bytes out of registry paths
    /// and identity hashes.
    pub fn sref(&self, canonical_name: &str) -> crate::tenant::TenantStreamRef {
        crate::tenant::TenantStreamRef::new(
            self.tenant.clone(),
            crate::tenant::CanonicalStreamName::new(canonical_name)
                .expect("caller passed a canonical stream name"),
        )
    }

    /// Shard engine for `hash`, opening the shard log on first use (which
    /// fences any previous owner). A shard that was just fenced away is
    /// held off for 3 s (anti-flap while the router converges) → 503.
    /// Response-free engine lookup for the unified scaler.
    pub async fn engine_for_scaler(self: &Arc<Self>, hash: &[u8; 16]) -> Option<Arc<ShardEngine>> {
        // R29: the scaler is INTERNAL — it must not stamp external
        // adoption, or its periodic scans would leak sweep-held
        // engines out of the budgeted rotation.
        self.engine_for_inner(hash, false).await.ok()
    }

    /// Internal (non-adopting) resolution: same ownership rules and
    /// on-demand open as engine_for, but never stamps the adoption
    /// sequence — for the tombstone walk and other maintenance.
    pub(crate) async fn engine_for_quiet(
        self: &Arc<Self>,
        hash: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, Response> {
        self.engine_for_inner(hash, false).await
    }

    pub(crate) async fn engine_for(
        self: &Arc<Self>,
        hash: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, Response> {
        self.engine_for_inner(hash, true).await
    }

    async fn engine_for_inner(
        self: &Arc<Self>,
        hash: &[u8; 16],
        external: bool,
    ) -> Result<Arc<ShardEngine>, Response> {
        let prefix = shard_for_hash(&self.shard_prefixes, hash);
        let owner = self.effective_owner(&prefix);
        let not_mine = owner.as_ref().is_some_and(|o| *o != self.instance_name);
        if let Some(e) = {
            let guard = self.shards.read().unwrap();
            let e = guard.get(&prefix).cloned();
            // R29 custody: EXTERNAL resolution stamps the adoption
            // sequence and revokes any sweep custody, INSIDE the read
            // guard — the scheduler's close takes the write lock
            // first, so every resolution that could still hold this
            // engine is visible to the close's re-check.
            if external && let Some(ref e) = e {
                crate::billing::stamp_external(e);
            }
            e
        } {
            // Possession must yield to the ring. An instance that lost
            // a shard on a rendezvous redraw keeps its engine here, and
            // slatedb fencing only fails its next WRITE — with all
            // writes at the new owner, the loser serves reads from a
            // view frozen at the fence point indefinitely (fleet2 leg C:
            // a scan snapshot froze a live segment at 252 of 510
            // records). Yield, close, and redirect like any non-owner.
            if !not_mine {
                return Ok(e);
            }
            let eng = self.shards.write().unwrap().remove(&prefix);
            if let Some(e) = eng {
                e.begin_close();
            }
        }
        // R2/R3: only the ring owner may claim a shard. A stale router can
        // still send us one — answer 409 + Streams-Replay-To so the router
        // corrects itself, instead of fencing the rightful owner.
        if not_mine {
            let owner = owner.expect("not_mine implies owner");
            let mut r = err_resp(
                StatusCode::CONFLICT,
                "not_ring_owner",
                &format!("shard {prefix} belongs to {owner}"),
            );
            if let Ok(v) = axum::http::HeaderValue::from_str(&owner) {
                r.headers_mut().insert("streams-replay-to", v);
            }
            return Err(r);
        }
        // Single-flight open with a bounded wait. A slow WAL replay
        // continues in its own task regardless of what this request does —
        // the caller only ever gets a retryable 503, never the power to
        // abandon or duplicate an open (the eu-central-1 storm).
        let wait = std::time::Duration::from_millis(
            std::env::var("SHARD_OPEN_WAIT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
        );
        match self.gate.get_or_open(&prefix, wait).await {
            crate::sharddir::OpenOutcome::Ready(engine) => {
                // R29: a customer who coalesced into (or raced) an open
                // the sweep started still counts as external adoption.
                if external {
                    crate::billing::stamp_external(&engine);
                }
                Ok(engine)
            }
            crate::sharddir::OpenOutcome::Wait {
                code,
                retry_after_secs,
            } => {
                let mut r = err_resp(
                    StatusCode::SERVICE_UNAVAILABLE,
                    code,
                    "shard not currently serving here; retry",
                );
                if let Ok(v) = axum::http::HeaderValue::from_str(&retry_after_secs.to_string()) {
                    r.headers_mut().insert("retry-after", v);
                }
                Err(r)
            }
            crate::sharddir::OpenOutcome::Failed(e) => Err(err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "shard_open",
                &format!("open shard {prefix}: {e}"),
            )),
        }
    }

    /// Ring ownership for a shard prefix: the rebalancer override if its
    /// target is active, else the rendezvous pick. None when no ring is
    /// configured (single instance) — then everyone may serve everything.
    pub fn effective_owner(&self, prefix: &str) -> Option<String> {
        let active = self.ring_active.read().unwrap().clone();
        if active.is_empty() || self.instance_name.is_empty() {
            return None;
        }
        if let Some(t) = self.ring_overrides.read().unwrap().get(prefix)
            && active.iter().any(|a| a == t)
        {
            return Some(t.clone());
        }
        Some(active[ring_pick(prefix, &active)].clone())
    }

    /// Called when a shard db closes (fenced by a new owner): drop it from
    /// the serving map and start the anti-flap holdoff.
    pub fn shard_closed(self: &Arc<Self>, prefix: &str) {
        // Eviction + holdoff live in the gate; an engine that died young
        // escalates the holdoff (rapid open→die cycles are the storm).
        self.gate.notify_closed(prefix);
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

/// Bound on distinct streams tracked by the per-stream admission map.
const STREAM_INFLIGHT_MAX_TRACKED: usize = 65_536;

#[derive(Debug, PartialEq)]
enum SlotTry {
    /// Limiter off or the map is at its bound: admit without tracking
    /// (fail open on the bound, never leak).
    Untracked,
    Acquired,
    AtCap,
}

fn stream_slot_try(m: &mut HashMap<[u8; 16], i64>, cap: i64, hash: [u8; 16]) -> SlotTry {
    if cap <= 0 {
        return SlotTry::Untracked;
    }
    match m.get_mut(&hash) {
        Some(v) => {
            if *v >= cap {
                return SlotTry::AtCap;
            }
            *v += 1;
            SlotTry::Acquired
        }
        None => {
            if m.len() >= STREAM_INFLIGHT_MAX_TRACKED {
                return SlotTry::Untracked;
            }
            m.insert(hash, 1);
            SlotTry::Acquired
        }
    }
}

/// Entries are removed at zero so the map stays proportional to
/// concurrently-active streams.
fn stream_slot_release(m: &mut HashMap<[u8; 16], i64>, hash: &[u8; 16]) {
    if let Some(v) = m.get_mut(hash) {
        *v -= 1;
        if *v <= 0 {
            m.remove(hash);
        }
    }
}

/// RAII per-stream inflight slot (None = untracked).
struct StreamSlot {
    state: Arc<AppState>,
    hash: [u8; 16],
}
impl Drop for StreamSlot {
    fn drop(&mut self) {
        stream_slot_release(&mut self.state.stream_inflight.lock().unwrap(), &self.hash);
    }
}

/// Acquire a per-stream slot, or a scoped 429 when the stream is at its cap.
#[allow(clippy::result_large_err)] // Err is the ready-to-send 429 Response, same as engine_for
fn acquire_stream_slot(
    state: &Arc<AppState>,
    hash: [u8; 16],
) -> Result<Option<StreamSlot>, Response> {
    let outcome = stream_slot_try(
        &mut state.stream_inflight.lock().unwrap(),
        state.admit_max_inflight_per_stream,
        hash,
    );
    match outcome {
        SlotTry::Untracked => Ok(None),
        SlotTry::Acquired => Ok(Some(StreamSlot {
            state: state.clone(),
            hash,
        })),
        SlotTry::AtCap => {
            state
                .stream_shed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut r = err_resp(
                StatusCode::TOO_MANY_REQUESTS,
                "stream_overloaded",
                "too many concurrent requests for this stream",
            );
            r.headers_mut()
                .insert("retry-after", axum::http::HeaderValue::from_static("1"));
            Err(r)
        }
    }
}

/// Per-engine maintenance state for /v1/debug/load (R25-C).
fn maintenance_shards_json(state: &AppState) -> serde_json::Value {
    let engines: Vec<Arc<ShardEngine>> = state.shards.read().unwrap().values().cloned().collect();
    let now = crate::shard::now_ms();
    let shards_engaged = engines
        .iter()
        .filter(|e| {
            e.maintenance_shard_shed
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .count();
    serde_json::json!({
        "owned_shards": engines.len(),
        // R27-1: the per-shard machine's engaged count, reported next
        // to (not merged with) the instance machine's state.
        "shards_engaged": shards_engaged,
        // R26-7: the exact cumulative frame-byte totals (actual
        // quantities per R26-2 — a mixed group counts both sides), so a
        // campaign can compute the corrected absorption ratio from the
        // field instead of a payload-unit artifact. Committed and
        // retired here are the SAME unit as unabsorbed_frame_bytes:
        // encoded frame bytes.
        "ingest_frame_bytes_total": crate::shard::INGEST_FRAME_BYTES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        "absorbed_frame_bytes_total": crate::shard::ABSORBED_FRAME_BYTES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        "shards": engines.iter().map(|e| {
            let m = e.maintenance_snapshot();
            serde_json::json!({
                "prefix": e.prefix,
                "unabsorbed_frame_bytes": m.unabsorbed_frame_bytes,
                "backlog_started_ms": m.backlog_started_ms,
                "last_progress_ms": m.last_progress_ms,
                "no_progress_secs": m.no_progress_secs(now),
                "shard_shed": e.maintenance_shard_shed
                    .load(std::sync::atomic::Ordering::Relaxed),
            })
        }).collect::<Vec<_>>(),
    })
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
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1"), ("content-type", "application/json")],
            r#"{"error":{"code":"overloaded","message":"instance at admission capacity; retry"}}"#,
        )
            .into_response();
    }
    // R24-B: maintenance backpressure is NO LONGER decided here.
    //
    // A global middleware check runs before descriptor resolution and
    // ownership routing, which produced two real defects: a non-owner
    // could answer 503 for a backlog that belongs to another instance
    // instead of replaying to the owner, and one hot shard's latch shed
    // EVERY append on the process — including unrelated tenants sharing
    // the instance. The decision now happens in the append path, once
    // the stream's shard is known. See product::maintenance_gate.

    // R25-E: the oversized-body 413 (with its bounded drain) and the
    // RSS write-shed both MOVED into append_core, after route-specific
    // authentication and ownership resolution. Running them here — in
    // pre-auth middleware — let an unauthenticated caller force up to
    // 8 MiB of body drain and receive capacity answers (429/503) where
    // the contract requires 401: authenticate before buffering or
    // materially consuming the request body.
    next.run(req).await
}

/// Calibrated-latency endpoint for edge probes: holds the request for
/// ?ms= milliseconds doing no engine work. Lets a probe separate an
/// admitted-concurrency cap (rate = slots/latency) from a rate cap
/// (rate constant regardless of latency).
async fn debug_sleep(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
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

/// GET /v1/segments/{name} (spec §10): the stream's segment map as an
/// observability surface — never a control knob. Implicit maps render
/// as their single live segment.
///
/// Account-authenticated. Physical segmentation is internal to the
/// product surface, and this response names the collection and exposes
/// its key ranges, predecessors, pending transitions and sealed
/// offsets — nothing a caller without the account token should be able
/// to enumerate.
async fn get_segments(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let desc = match state.registry.get(&state.sref(&name)).await {
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
    let seg_json = |s: &crate::segmap::SegmentDesc| {
        serde_json::json!({
            "seg_id": s.seg_id,
            "lo": format!("{:#018x}", s.lo),
            "hi": format!("{:#018x}", s.hi),
            "live": s.is_live(),
            "sealed_next_offset": s.sealed_next_offset,
            "predecessors": s.predecessors,
            "created_ms": s.created_ms,
        })
    };
    let body = match &desc.segments {
        Some(map) => serde_json::json!({
            "version": map.version,
            "pending": map.pending.as_ref().map(|p| p.kind.clone()),
            "segments": map.segments.iter().map(seg_json).collect::<Vec<_>>(),
        }),
        None => serde_json::json!({
            "version": 0,
            "pending": null,
            "segments": [{
                "seg_id": 0,
                "lo": "0x0000000000000000",
                "hi": format!("{:#018x}", crate::segmap::KEYSPACE_END),
                "live": true,
                "sealed_next_offset": null,
                "predecessors": [],
                "created_ms": desc.created_ms,
            }],
        }),
    };
    axum::Json(body).into_response()
}

async fn debug_load(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let now = state.inflight.load(std::sync::atomic::Ordering::Relaxed);
    let peak = state
        .inflight_peak
        .swap(now, std::sync::atomic::Ordering::Relaxed);
    // Cardinality gauges for every stream-indexed structure (static
    // audit: several grew unbounded and invisibly).
    let resident_handles: usize = state
        .shards
        .read()
        .unwrap()
        .values()
        .map(|e| e.resident_streams())
        .sum();
    // Trim maintenance rollup across shards: debt = streams owing
    // physical trims; max_batch is the gate the mature-second-wave
    // stress reads (must stay ≤ TRIM_GLOBAL_BUDGET).
    let (trim_debt, trim_last, trim_max_batch, trim_total) = state
        .shards
        .read()
        .unwrap()
        .values()
        .map(|e| e.trim_stats())
        .fold((0usize, 0u64, 0u64, 0u64), |a, v| {
            (a.0 + v.0, a.1.max(v.1), a.2.max(v.2), a.3 + v.3)
        });
    // Seal-fence rollup (round 12): the map is deliberately unbounded
    // (no safe wall-clock expiry exists over a queue with no residence
    // bound), so its size is surfaced before it could ever matter.
    let (fence_entries, fence_max_gen) = state
        .shards
        .read()
        .unwrap()
        .values()
        .map(|e| e.seal_fence_stats())
        .fold((0usize, 0u64), |a, v| (a.0 + v.0, a.1.max(v.1)));
    // Consumer-fence rollup (round 17): same unbounded-by-design map,
    // same visibility rule.
    let (cfence_entries, cfence_max_gen) = state
        .shards
        .read()
        .unwrap()
        .values()
        .map(|e| e.consumer_fence_stats())
        .fold((0usize, 0u64), |a, v| (a.0 + v.0, a.1.max(v.1)));
    axum::Json(serde_json::json!({
        "inflight_now": now,
        "inflight_peak": peak,
        "rss_mb": crate::fleet::rss_bytes() as f64 / 1048576.0,
        "admit_shed": state.admit_shed.load(std::sync::atomic::Ordering::Relaxed),
        "maintenance_backpressure": state.maint_latch.stats_json(),
        "maintenance_shards": maintenance_shards_json(&state),
        // R26-7: the ORDINARY per-stream limiter's refusals, by code —
        // so a throughput plateau is attributed to the right mechanism.
        "rate_limit_refusals": crate::usage::limit_refusals_json(),
        // R26-9 build identity: the wrapper hashes the binary it
        // actually downloaded and passes the digest in; verify-running
        // compares it to the campaign's upload manifest. "unknown"
        // outside wrapper-managed deployments.
        "binary_sha256": std::env::var("APP_BINARY_SHA256")
            .unwrap_or_else(|_| "unknown".into()),
        // R28: full build/boot identity — the campaign verifier compares
        // ALL of these against its manifest (stale-build platform trap).
        "git_commit": env!("STREAMS_GIT_COMMIT"),
        "build_unix": env!("STREAMS_BUILD_UNIX"),
        "boot_id": crate::billing::boot_id(),
        "compactor_profile": crate::compactor_profile_json(),
        // R29: the KERNEL's high-water mark, not sampled RSS — sampled
        // peaks missed the 5 s kill waves entirely. cgroup v2 first,
        // v1 fallback; null off-Linux.
        "now_unix_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        "cgroup_peak_bytes": std::fs::read_to_string("/sys/fs/cgroup/memory.peak")
            .or_else(|_| std::fs::read_to_string(
                "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"))
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok()),
        "stream_shed": state.stream_shed.load(std::sync::atomic::Ordering::Relaxed),
        "wedge_shed": state.wedge_shed.load(std::sync::atomic::Ordering::Relaxed),
        "streams_tracked": state.stream_inflight.lock().unwrap().len(),
        "absorb_lag_max_secs": crate::usage::absorb_lag_max(),
        "cardinality": {
            "resident_handles": resident_handles,
            "usage_tracked": crate::usage::tracked_streams(),
            "keycache": state.keys.len(),
            "registry_cache": state.registry.cache_len(),
            "seal_fence_entries": fence_entries,
            "seal_fence_max_generation": fence_max_gen,
            "consumer_fence_entries": cfence_entries,
            "consumer_fence_max_generation": cfence_max_gen,
        },
        "trim": {
            "debt_streams": trim_debt,
            "deletes_last_batch": trim_last,
            "deletes_max_batch": trim_max_batch,
            "deletes_total": trim_total,
        },
        // Postings index telemetry (ROUTING-V3 §14): write-side byte
        // ratio (the 8%/2% gates), planner spans (≤ 8), scan-vs-match
        // amplification, and corruption-envelope fallbacks (should
        // never move).
        "postings": {
            "bytes_written": crate::history::POSTINGS_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
            "pages_written": crate::history::POSTINGS_PAGES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
            "runs_written": crate::history::POSTINGS_RUNS_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
            "canonical_bytes_written": crate::history::CANONICAL_BYTES_WRITTEN.load(std::sync::atomic::Ordering::Relaxed),
            "read_spans_max": crate::history::READ_SPANS_MAX.load(std::sync::atomic::Ordering::Relaxed),
            "read_frames_scanned": crate::history::READ_FRAMES_SCANNED.load(std::sync::atomic::Ordering::Relaxed),
            "read_frames_matched": crate::history::READ_FRAMES_MATCHED.load(std::sync::atomic::Ordering::Relaxed),
            "corrupt": crate::history::POSTINGS_CORRUPT.load(std::sync::atomic::Ordering::Relaxed),
            "cache": state
                .shards
                .read()
                .unwrap()
                .values()
                .next()
                .map(|e| e.postings_cache.stats())
                .unwrap_or(serde_json::json!(null)),
        },
        "scaler": crate::scaler3::stats_json(),
        // Cross-layout absorb advances rejected by the committer's
        // layout seal. Nonzero = the absorber's lane classification
        // raced dispatch somewhere; the seal made it harmless, but it
        // should stay rare enough to investigate when it moves.
        "absorb_lane_dropped": state
            .shards
            .read()
            .unwrap()
            .values()
            .map(|e| e.absorb_lane_dropped.load(std::sync::atomic::Ordering::Relaxed))
            .sum::<u64>(),
    }))
    .into_response()
}

/// Object-store client latency snapshot (O14a): per (op, path-class)
/// percentiles over ?window= seconds (default 60), the slow-op ring, and
/// the outbound in-flight gauge. ?swap=1 resets the peak (sampler only —
/// heartbeats read it non-destructively).
async fn debug_store(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let window: u64 = q
        .get("window")
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
        .clamp(1, 300);
    let swap = q.get("swap").map(|v| v == "1").unwrap_or(false);
    let mut snap = crate::store_timing::snapshot(window, swap);
    if let Some(_obj) = snap.as_object_mut() {
        // History DbReader service: hits vs misses shows how much
        // per-request manifest traffic the cache absorbs; stale_reopens
        // is bounded by absorb cadence; coalesced proves single-flight.
    }
    axum::Json(snap).into_response()
}

/// Per-stream usage counters + the active limits. Auth: same bearer as
/// the other debug endpoints (enforced by the middleware layer).
async fn debug_usage(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let l = crate::usage::limits();
    let streams: Vec<serde_json::Value> = crate::usage::snapshot()
        .into_iter()
        .map(|(h, _gen, req, rec, bi, bo, pt, fr)| {
            serde_json::json!({
                "stream": crate::crypto::hex(&h),
                "requests": req,
                "records": rec,
                "bytes_in": bi,
                "bytes_out": bo,
                "plaintext_bytes": pt,
                "frame_bytes": fr,
                "compression_ratio": if fr > 0 { pt as f64 / fr as f64 } else { 0.0 },
                // Counters key by name hash, lag by engine hash; the
                // linked join (usage.rs) is what makes this nonzero.
                "absorb_lag_secs": crate::usage::absorb_lag_for_usage(crate::crypto::RouteHash(h)),
            })
        })
        .collect();
    let (backlog_streams, backlog_max) = crate::usage::absorb_backlog_summary();
    let (eligible, oldest_eligible) = crate::usage::absorb_pending_summary();
    let (overflow_admits, overflow_requests, overflow_records, overflow_bytes) =
        crate::usage::overflow_stats();
    axum::Json(serde_json::json!({
        "limits": {
            "bytes_per_sec": l.bytes_per_sec,
            "requests_per_sec": l.reqs_per_sec,
            "records_per_sec": l.recs_per_sec,
            "burst_secs": l.burst_secs,
        },
        // Aggregate view, immune to the per-stream listing cap: how many
        // engine streams carry absorb lag right now, and the worst one.
        "absorb_backlog": {
            "streams": backlog_streams,
            "max_secs": backlog_max,
            "eligible": eligible,
            "oldest_eligible_secs": oldest_eligible,
        },
        // Past-cap visibility (never fail open): admissions routed
        // through the shared conservative overflow bucket, plus the
        // aggregate counters those streams accrue. `streams` below is
        // capped at MAX_TRACKED entries — use these plus absorb_backlog
        // for population-level truth.
        "tracked_streams": crate::usage::tracked_streams(),
        "overflow": {
            "admits": overflow_admits,
            "requests": overflow_requests,
            "records": overflow_records,
            "bytes_in": overflow_bytes,
        },
        "streams": streams,
    }))
    .into_response()
}

/// Recent operational events (§12.5): the live ring, newest first.
/// Bearer-gated like every debug route; the durable history lives in
/// `_ops_events` and the ops rollup serves timelines.
async fn debug_ops_events(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let recent = crate::ops::recent(128);
    axum::Json(serde_json::json!({
        "events": recent,
        "alerts": crate::ops::open_alerts(),
        "dropped": crate::ops::EVENTS_DROPPED.load(std::sync::atomic::Ordering::Relaxed),
    }))
    .into_response()
}

/// Fleet-internal telemetry append (round-21 blocker 5): the OWNER-side
/// target for system-stream relays. Fleet credential only; reserved
/// names only; creates the stream lazily with the carried system key.
async fn internal_telemetry_append(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !fleet_internal_authorized(&state, &headers) {
        return internal_unauthorized();
    }
    if !crate::billing::is_reserved_stream(&name) {
        return err_resp(
            StatusCode::FORBIDDEN,
            "not_system_stream",
            "telemetry-append accepts only reserved system streams",
        );
    }
    // §16 note: this endpoint is gated to reserved SYSTEM streams and
    // delegates to the deployment-tenant creation path; it moves onto
    // the reserved system project with the Stage-7 system-stream
    // relocation rather than taking the per-request project header.
    let mut hdrs = HeaderMap::new();
    if let Some(k) = headers.get("stream-encryption-key") {
        hdrs.insert("stream-encryption-key", k.clone());
    }
    hdrs.insert(
        "content-type",
        axum::http::HeaderValue::from_static("application/json"),
    );
    let c = create_stream(state.clone(), name.clone(), hdrs.clone(), Bytes::new()).await;
    let cst = c.status().as_u16();
    if !(cst == 200 || cst == 201 || cst == 409) {
        return c;
    }
    append(state, name, hdrs, Body::from(body), None, None, None).await
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(health_axum))
        // R23-5 / R24-E: separate the two questions a platform asks.
        //
        // /livez  — is the process alive? (restart me if not)
        // /readyz — INITIAL data-plane dependencies validated, and this
        //           instance is not in a never-ready failure state.
        //
        // Deliberately narrower than "is storage usable?", which is what
        // this comment used to claim. After one successful shard open,
        // mid-life registry or storage failures do NOT unready the
        // instance — that is intentional fleet behaviour (a store blip
        // must not cascade every instance out of rotation), but it means
        // /readyz is a BOOT readiness signal, not a live dependency
        // probe. The startup canary behind it validates PUT and GET on
        // the ops/shard/data buckets; it does not validate delete
        // permission, LIST or range semantics, conditional puts, or
        // every prefix the fleet and telemetry paths use. Mid-life
        // degradation is surfaced through /v1/debug/store instead.
        .route("/livez", get(|| async { "alive" }))
        .route("/readyz", get(health_axum))
        .route("/operator/billing.json", get(billing_readiness_axum))
        .route("/v1/segments/{*name}", get(get_segments))
        // Fleet-internal segment fan-out target (bearer-gated): a keyed,
        // segment-positioned read served strictly from local ownership.
        // Peers relay here when a lineage crosses instances; the public
        // raw route keeps rejecting ?key= (audit P0 standards isolation).
        .route(
            "/v1/internal/segment-read/{*name}",
            get(internal_segment_read),
        )
        .route(
            "/v1/internal/sweep-segment/{*name}",
            post(crate::product::internal_sweep_segment),
        )
        .route(
            "/v1/internal/queue-cursor/{*name}",
            get(crate::product::internal_queue_cursor),
        )
        .route(
            "/v1/internal/segment-scan/{*name}",
            get(crate::product::internal_segment_scan),
        )
        .route(
            "/v1/internal/telemetry-append/{*name}",
            post(internal_telemetry_append),
        )
        .route("/v1/debug/timings", get(debug_timings))
        .route("/v1/debug/load", get(debug_load))
        .route("/v1/debug/store", get(debug_store))
        .route("/v1/debug/usage", get(debug_usage))
        .route("/v1/debug/ops-events", get(debug_ops_events))
        // Every /v1/debug/* route is account-gated (round-19: the
        // security model claims bearer auth on all of /v1/*, and these
        // MUTATE production state — pausing absorption, occupying
        // request slots, resetting peak gauges — or expose per-stream
        // usage). The unsecured on-call surface is /operator only.
        .route(
            "/v1/debug/absorb-pause",
            post(
                |State(state): State<Arc<AppState>>,
                 headers: HeaderMap,
                 Query(q): Query<std::collections::HashMap<String, String>>| async move {
                    if !authorized(&state, &headers) {
                        return err_resp(
                            StatusCode::UNAUTHORIZED,
                            "unauthorized",
                            "bearer token required",
                        );
                    }
                    let on = q.get("on").map(|v| v == "1").unwrap_or(false);
                    crate::history::absorb_pause_flag()
                        .store(on, std::sync::atomic::Ordering::Relaxed);
                    axum::Json(serde_json::json!({"absorb_paused": on})).into_response()
                },
            ),
        )
        // R27-5: remote crash for field handoff gates. abort() = SIGABRT
        // — no WAL flush, no fencing handoff, no absorber drain; the
        // successor must recover from durable state alone. Enabled only
        // when the deploy sets STREAMS_DEBUG_EXIT=1 (campaign fleets),
        // and auth-gated like every debug route. Platform `versions
        // stop` is too graceful to prove crash recovery.
        .route(
            "/v1/debug/abort",
            post(
                |State(state): State<Arc<AppState>>, headers: HeaderMap| async move {
                    if !authorized(&state, &headers) {
                        return err_resp(
                            StatusCode::UNAUTHORIZED,
                            "unauthorized",
                            "bearer token required",
                        );
                    }
                    if std::env::var("STREAMS_DEBUG_EXIT").as_deref() != Ok("1") {
                        return err_resp(
                            StatusCode::FORBIDDEN,
                            "disabled",
                            "STREAMS_DEBUG_EXIT=1 not set on this deploy",
                        );
                    }
                    tracing::error!("debug abort requested — dying WITHOUT cleanup");
                    // Give the ack + log line a moment, then die hard.
                    tokio::spawn(async {
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                        std::process::abort();
                    });
                    axum::Json(serde_json::json!({"aborting": true})).into_response()
                },
            ),
        )
        .route("/v1/debug/sleep", get(debug_sleep))
        // Injected history-flush slowdown (OOM review acceptance
        // campaign): stalls the REAL gather flush path by ?ms= per
        // flush, with the process-wide reservation held — the
        // mechanism the slow-compactor campaign drives. 0 clears.
        .route(
            "/v1/debug/history-stall",
            post(
                |State(state): State<Arc<AppState>>,
                 headers: HeaderMap,
                 Query(q): Query<std::collections::HashMap<String, String>>| async move {
                    if !authorized(&state, &headers) {
                        return err_resp(
                            StatusCode::UNAUTHORIZED,
                            "unauthorized",
                            "bearer token required",
                        );
                    }
                    let ms: u64 = q.get("ms").and_then(|v| v.parse().ok()).unwrap_or(0);
                    crate::history::HISTORY_FLUSH_STALL_MS
                        .store(ms, std::sync::atomic::Ordering::Relaxed);
                    axum::Json(serde_json::json!({"historyFlushStallMs": ms})).into_response()
                },
            ),
        )
        // OOM-review causal detail: per-partition history L0 posture,
        // the process-wide absorber budget, last-gather phases, and
        // telemetry-plane residency — the exact signals needed to prove
        // (or refute) "history compaction fell behind". Authorized like
        // every other /v1/debug route.
        .route(
            "/v1/debug/absorb",
            get(
                |State(state): State<Arc<AppState>>, headers: HeaderMap| async move {
                    if !authorized(&state, &headers) {
                        return err_resp(
                            StatusCode::UNAUTHORIZED,
                            "unauthorized",
                            "bearer token required",
                        );
                    }
                    let ord = std::sync::atomic::Ordering::Relaxed;
                    let engines: Vec<_> = {
                        let m = state.shards.read().unwrap();
                        m.iter().map(|(p, e)| (p.clone(), e.clone())).collect()
                    };
                    let mut parts = Vec::new();
                    for (prefix, e) in engines {
                        if let Some(part) = e.history_partition_if_open() {
                            let (l0, l0b, runs, mid) = crate::history::history_l0_stats(&part);
                            parts.push(serde_json::json!({
                                "shard": prefix,
                                "l0SstCount": l0,
                                "l0BytesEst": l0b,
                                "compactedRuns": runs,
                                "manifestId": mid,
                            }));
                        }
                    }
                    let spool = state.read_spool.get().map(|sp| {
                        let (rows, bytes) = sp.resident();
                        let (l0, l0b, runs, mid) = sp.l0_stats();
                        serde_json::json!({
                            "pendingRows": rows,
                            "pendingBytes": bytes,
                            "quarantined": sp.quarantined_count(),
                            "l0SstCount": l0,
                            "l0BytesEst": l0b,
                            "compactedRuns": runs,
                            "manifestId": mid,
                        })
                    });
                    let rollup_db = state.rollup.get().map(|ru| {
                        let (l0, l0b, runs, mid) = ru.l0_stats();
                        serde_json::json!({
                            "l0SstCount": l0,
                            "l0BytesEst": l0b,
                            "compactedRuns": runs,
                            "manifestId": mid,
                        })
                    });
                    let budget = crate::history::absorb_budget();
                    axum::Json(serde_json::json!({
                        "historyPartitions": parts,
                        "budget": {
                            "capacityBytes": budget.capacity(),
                            "gatherSlots": budget.gather_slots(),
                            "effectiveGatherConcurrency":
                                crate::history::effective_gather_concurrency(),
                            "perGatherReservationBytes":
                                crate::history::per_gather_reservation_bytes(),
                            "worstFrameTransientBytes":
                                crate::history::absorb_worst_frame_transient(),
                            "injectedFlushStallMs": crate::history::HISTORY_FLUSH_STALL_MS
                                .load(std::sync::atomic::Ordering::Relaxed),
                            "shedLineMb": state.admit_rss_shed_mb,
                        },
                        "absorber": {
                            "reservedBytes": crate::history::absorb_reserved_bytes(),
                            "gathersInflight": crate::history::absorb_gathers_inflight(),
                            "lastReservedBytes": crate::history::GATHER_LAST_RESERVED.load(ord),
                            "lastActualBytes": crate::history::GATHER_LAST_ACTUAL.load(ord),
                            "lastReadMs": crate::history::GATHER_LAST_READ_MS.load(ord),
                            // R25-F: per-gather amplification removed —
                            // the global-delta attribution was
                            // contaminated by concurrent traffic.
                            // Process-wide TRANSFERRED bytes:
                            "storeGetCount": crate::store_timing::GET_COUNT
                                .load(std::sync::atomic::Ordering::Relaxed),
                            "storeGetTransferredBytes": crate::store_timing::GET_BYTES
                                .load(std::sync::atomic::Ordering::Relaxed),
                            "lastWriteMs": crate::history::GATHER_LAST_WRITE_MS.load(ord),
                            "lastFlushMs": crate::history::GATHER_LAST_FLUSH_MS.load(ord),
                            "absorbedBytesTotal": crate::history::ABSORB_BYTES_TOTAL.load(ord),
                            "ingestBytesTotal": crate::history::INGEST_BYTES_TOTAL.load(ord),
                        },
                        "telemetry": {
                            "spool": spool,
                            "rollupDb": rollup_db,
                            "cacheCapacityBytes":
                                crate::billing::TELEMETRY_CACHE_CAPACITY.load(ord),
                            "sweepResidentEngines":
                                crate::billing::sweep_resident_engines(&state),
                        },
                        "config": crate::history::RESOLVED_MEMORY_CONFIG.get(),
                        "process": {
                            "rssMb": state.rss_mb_cached.load(ord),
                            "cgroupCurrentMb": std::fs::read_to_string("/sys/fs/cgroup/memory.current")
                                .ok().and_then(|s| s.trim().parse::<u64>().ok()).map(|v| v / 1048576),
                            "cgroupPeakMb": std::fs::read_to_string("/sys/fs/cgroup/memory.peak")
                                .ok().and_then(|s| s.trim().parse::<u64>().ok()).map(|v| v / 1048576),
                            "oomKillTotal": std::fs::read_to_string("/sys/fs/cgroup/memory.events")
                                .ok().and_then(|s| s.lines().find_map(|l|
                                    l.strip_prefix("oom_kill ").and_then(|v| v.trim().parse::<u64>().ok()))),
                        },
                    }))
                    .into_response()
                },
            ),
        )
        // Operator dashboard: UNSECURED by explicit product decision (on-call
        // must see the cell without credentials). The payload is therefore
        // restricted to operational metadata — never stream names, tenant
        // identifiers, tokens, keys, or signed URLs.
        .route("/operator", get(crate::operator::page))
        .route("/operator/data.json", get(crate::operator::data))
        .route("/operator/runbook", get(crate::operator::runbook))
        .route("/v1/stream/__ds/{*rest}", any(ds_reserved))
        .route(
            "/v1/streams",
            axum::routing::get(product_list_axum).options(product_preflight),
        )
        .route("/v1/streams/{*name}", any(product_entry_axum))
        .route(
            "/v1/projects/{project}/usage",
            axum::routing::get(project_usage_axum).options(product_preflight),
        )
        .route("/v1/stream/{*name}", any(stream_entry))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            track_inflight,
        ))
        // Server-origin marker on EVERY response, including errors
        // (round-19 must-fix 4). A 404 from a real server means "this
        // stream does not exist" — a 404 from the PLATFORM edge (dead
        // or unpublished service) means "this upstream is unavailable",
        // and the SDK retries 429/503 but never 404. A router that
        // cannot tell them apart turns an instance loss into permanent
        // "stream deleted" for applications (the hard-kill campaign:
        // 8,371 semantic 404s in ~30 s). Marked responses are ours;
        // unmarked ones never reached a server.
        .layer(axum::middleware::map_response_with_state(
            state.clone(),
            |State(state): State<Arc<AppState>>, mut resp: Response| async move {
                resp.headers_mut().insert(
                    "x-content-type-options",
                    axum::http::HeaderValue::from_static("nosniff"),
                );
                if let Ok(v) = axum::http::HeaderValue::from_str(&state.origin_marker) {
                    resp.headers_mut().insert("prisma-streams-origin", v);
                }
                resp
            },
        ))
        .with_state(state)
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

pub(crate) fn err_resp(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        json!({"error": {"code": code, "message": message}}).to_string(),
    )
        .into_response()
}

/// Commit-pipeline timing samples per shard: how long db.write took vs how
/// long the group then waited for the durable watermark. Diagnostic only.
async fn debug_timings(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
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
        let flushes = eng.pump_flushes.load(std::sync::atomic::Ordering::Relaxed);
        let barrier_acked = eng
            .pump_barrier_acked
            .load(std::sync::atomic::Ordering::Relaxed);
        shards.insert(
            prefix.clone(),
            json!({
                "groups": samples,
                // requests-per-WAL is the judge of flush scheduling:
                // barrier_acked / flushes, delta'd across two scrapes.
                "pump": {
                    "flushes": flushes,
                    "barrier_acked": barrier_acked,
                    "gathers_applied": eng.pump_gathers.load(std::sync::atomic::Ordering::Relaxed),
                    "gathers_skipped_busy": eng.pump_gathers_skipped_busy.load(std::sync::atomic::Ordering::Relaxed),
                    "gathered_reqs": eng.pump_gathered_reqs.load(std::sync::atomic::Ordering::Relaxed),
                    "flushed_reqs": eng.pump_flushed_reqs.load(std::sync::atomic::Ordering::Relaxed),
                    "flushed_records": eng.pump_flushed_records.load(std::sync::atomic::Ordering::Relaxed),
                    "flushed_bytes": eng.pump_flushed_bytes.load(std::sync::atomic::Ordering::Relaxed),
                    "ack_to_enqueue_sum_us": eng.ack_to_enqueue_sum_us.load(std::sync::atomic::Ordering::Relaxed),
                    "ack_to_enqueue_count": eng.ack_to_enqueue_count.load(std::sync::atomic::Ordering::Relaxed),
                },
                "tail_ring": {
                    "resident_bytes": eng.ring_resident_bytes(),
                    "peak_bytes": eng.ring_peak_bytes.load(std::sync::atomic::Ordering::Relaxed),
                    "published": eng.ring_published.load(std::sync::atomic::Ordering::Relaxed),
                    "hits": eng.ring_hits.load(std::sync::atomic::Ordering::Relaxed),
                    "misses": eng.ring_misses.load(std::sync::atomic::Ordering::Relaxed),
                    "miss_below_floor": eng.ring_miss_below_floor.load(std::sync::atomic::Ordering::Relaxed),
                    "miss_above_ceil": eng.ring_miss_above_ceil.load(std::sync::atomic::Ordering::Relaxed),
                    "miss_empty": eng.ring_miss_empty.load(std::sync::atomic::Ordering::Relaxed),
                    "evicted": eng.ring_evicted.load(std::sync::atomic::Ordering::Relaxed),
                },
            }),
        );
    }
    (
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::Value::Object(shards).to_string(),
    )
        .into_response()
}

#[derive(Deserialize, Default)]
pub struct ReadParams {
    pub(crate) offset: Option<String>,
    pub(crate) format: Option<String>,
    pub(crate) live: Option<String>,
    pub(crate) timeout: Option<String>,
    pub(crate) key: Option<String>,
    // touch wait params
    pub(crate) cursor: Option<String>,
    pub(crate) sig: Option<String>,
    /// Internal page-budget override (product maxBytes). Skipped by
    /// serde so the raw query string can never set it.
    #[serde(skip)]
    pub(crate) max_bytes: Option<usize>,
    /// Read visibility (product `deliver=` param). Skipped by serde so
    /// the raw query string can never set it — the pinned raw surface
    /// is always durable; only product_read installs Applied.
    #[serde(skip)]
    pub(crate) deliver: crate::shard::Deliver,
    /// Set on peer-relayed segment reads (/v1/internal/segment-read):
    /// serve strictly from local ownership — a foreign segment answers
    /// 409 Streams-Replay-To instead of relaying again, so fan-out depth
    /// is exactly one and ownership churn can never build relay cycles.
    #[serde(skip)]
    pub(crate) no_fanout: bool,
    /// Fleet-internal request: NEVER metered (§4.2 — internal relays
    /// return counts; the public coordinator that requested the page
    /// meters exactly once). Serde-skipped so no query string sets it.
    #[serde(skip)]
    pub(crate) internal: bool,
}

/// Reserved Durable Streams control namespace (appendix §2.6): matched
/// before any wildcard stream name, never a customer stream. The pinned
/// baseline's subscription resources mount here when implemented.
async fn ds_reserved() -> Response {
    err_resp(
        StatusCode::NOT_FOUND,
        "reserved",
        "__ds is the reserved Durable Streams control namespace",
    )
}

/// Browser preflight for the catalog route (the wildcard product route
/// answers its own inside product_entry).
async fn product_preflight() -> Response {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("access-control-allow-origin", "*")
        .header("access-control-allow-methods", "GET, OPTIONS")
        .header(
            // `*` does not authorize Authorization: it is a
            // forbidden-wildcard request header, so a bearer request
            // fails preflight unless the name is listed.
            "access-control-allow-headers",
            "authorization, content-type, stream-encryption-key, \
             stream-closed, stream-ttl, stream-forked-from, \
             stream-fork-offset, stream-fork-sub-offset, \
             producer-id, producer-epoch, producer-seq, if-none-match",
        )
        .header("access-control-expose-headers", "*")
        .header("access-control-max-age", "600")
        .body(Body::empty())
        .unwrap()
}

async fn product_list_axum(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
) -> Response {
    crate::product::with_product_cors(
        crate::product::product_list(state, query.unwrap_or_default(), headers).await,
    )
}

/// Health: in BILLING_MODE=required an instance is NOT ready until
/// its billing prerequisites hold (round-22 item 10) — the read spool
/// is open and, on a rollup owner, the rollup DB is open. Both are
/// opened synchronously at startup, so a 503 here means startup-order
/// bugs or a lost OnceLock, and the platform should not route yet.
async fn health_axum(State(state): State<Arc<AppState>>) -> Response {
    // A process that has never opened a shard cannot serve a single
    // append; answering `ok` keeps it in the load balancer forever
    // (CHAOS-2). Report unready so rollouts halt and traffic drains.
    if let Some(reason) = crate::sharddir::unready_reason() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("shard storage unavailable: {reason}"),
        )
            .into_response();
    }
    if crate::billing::billing_required() {
        let spool_ok = state.read_spool.get().is_some();
        let rollup_ok = std::env::var("ROLLUP").map(|v| v != "1").unwrap_or(true)
            || state.rollup.get().is_some();
        if !spool_ok || !rollup_ok {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("billing not ready (spool={spool_ok}, rollup={rollup_ok})"),
            )
                .into_response();
        }
    }
    // R28: identity headers so the campaign verifier can compare the
    // running build against its manifest without auth (body stays "ok"
    // for existing probes).
    (
        [
            ("x-streams-git", env!("STREAMS_GIT_COMMIT")),
            ("x-streams-build-unix", env!("STREAMS_BUILD_UNIX")),
            ("x-streams-boot-id", crate::billing::boot_id()),
        ],
        "ok",
    )
        .into_response()
}

/// GET /operator/billing.json — the billing-readiness surface
/// (round-22 item 10): one JSON answer for "is this fleet's billing
/// pipeline healthy" — ledger reachability (last successful drain),
/// rollup cursor progress, spool corruption, close debt, pending
/// artifacts, and open alerts.
async fn billing_readiness_axum(State(state): State<Arc<AppState>>) -> Response {
    use std::sync::atomic::Ordering;
    let now = crate::shard::now_ms();
    let spool = state.read_spool.get();
    let (spool_open, quarantined, depth) = match spool {
        Some(sp) => (true, sp.quarantined_count(), sp.depth().await as u64),
        None => (false, 0, 0),
    };
    let last_drain = crate::billing::LAST_DRAIN_OK_MS.load(Ordering::Relaxed);
    let last_apply = crate::billing::LAST_ROLLUP_APPLY_MS.load(Ordering::Relaxed);
    let mut rollup_info = serde_json::json!({ "running": false });
    if let Some(r) = state.rollup.get() {
        let pending = r
            .pending_artifacts(1000)
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        let pending_corr = r
            .pending_correction_artifacts(1000)
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        let oldest =
            r.db.get(&b"meta/oldest-unclosed-month"[..])
                .await
                .ok()
                .flatten()
                .map(|v| String::from_utf8_lossy(&v).to_string());
        rollup_info = serde_json::json!({
            "running": true,
            "lastApplyMs": last_apply,
            "lastApplyAgeSecs": if last_apply > 0 { (now - last_apply) / 1000 } else { -1 },
            "oldestUnclosedMonth": oldest,
            "pendingArtifacts": pending,
            "pendingCorrectionArtifacts": pending_corr,
        });
    }
    let ready = !crate::billing::billing_required()
        || (state.usage_key.is_some()
            && spool_open
            && (std::env::var("ROLLUP").map(|v| v != "1").unwrap_or(true)
                || state.rollup.get().is_some()));
    axum::Json(serde_json::json!({
        "mode": std::env::var("BILLING_MODE").unwrap_or_else(|_| "off".into()),
        "ready": ready,
        "usageLedgerConfigured": state.usage_key.is_some(),
        "spool": { "open": spool_open, "depth": depth, "quarantined": quarantined },
        "drain": {
            "lastOkMs": last_drain,
            "lastOkAgeSecs": if last_drain > 0 { (now - last_drain) / 1000 } else { -1 },
        },
        "rollup": rollup_info,
        "artifactContentMismatches":
            crate::billing::ARTIFACT_MISMATCHES.load(Ordering::Relaxed),
        "tombstoneWalkCloseSubmits":
            crate::billing::WALK_CLOSE_SUBMITS.load(Ordering::Relaxed),
        "openAlerts": crate::ops::open_alerts(),
    }))
    .into_response()
}

/// GET /v1/projects/{project}/usage (round-22 doc item D3): project-
/// level usage, bearer-gated like the per-stream endpoint.
async fn project_usage_axum(
    State(state): State<Arc<AppState>>,
    Path(project): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    if !authorized(&state, req.headers()) {
        return crate::product::with_product_cors(crate::product::perr(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
            None,
            false,
        ));
    }
    crate::product::with_product_cors(crate::product::project_usage(state, project, &query).await)
}

/// Prisma product surface (spec Stage 8): everything under /v1/streams/.
async fn product_entry_axum(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    method: Method,
    headers: HeaderMap,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    // Authorize BEFORE buffering. Reading up to MAX_BODY_BYTES first
    // let an unauthenticated caller make the server allocate 32 MiB per
    // request; the gate needs only the path, method, query and headers.
    if let Some(r) = crate::product::product_auth_gate(&state, &name, &method, &query, &headers) {
        return crate::product::with_product_cors(r);
    }
    // System namespace guard (docs/OBSERVABILITY-BILLING.md §8/§15) —
    // same rule as the raw surface: the leading `_` segment belongs to
    // the telemetry planes and no customer credential reaches it. After
    // auth, before body buffering. Note: usage LOOKUP endpoints live on
    // this surface under `{name}/usage`, which is a sub-resource of a
    // CUSTOMER stream — unaffected by this guard.
    if crate::billing::is_reserved_stream(&name) {
        return crate::product::with_product_cors(crate::product::perr(
            StatusCode::FORBIDDEN,
            "reserved_stream",
            "names beginning with '_' are reserved for the system",
            None,
            false,
        ));
    }
    let body = match axum::body::to_bytes(req.into_body(), max_body_bytes()).await {
        Ok(b) => b,
        Err(_) => {
            return crate::product::with_product_cors(crate::product::perr(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds the limit",
                None,
                false,
            ));
        }
    };
    crate::product::with_product_cors(
        crate::product::product_entry(state, name, method, headers, query, body).await,
    )
}

async fn stream_entry(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let st = state.clone();
    let resp = stream_entry_inner(
        State(state),
        Path(name),
        Query(params),
        method,
        headers,
        body,
    )
    .await;
    // Only successful work counts toward the fleet load vector — otherwise
    // routing noise (409 replays, 404s) masquerades as demand and drives
    // the desired count up on garbage.
    if resp.status().is_success() {
        st.fleet_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    resp
}

async fn stream_entry_inner(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    // The `_` namespace is the system's (docs/OBSERVABILITY-BILLING.md
    // §8/§15): `_usage`, `_ops_metrics` and `_ops_events` live there,
    // customer credentials never reach them (their own key + the fleet
    // credential do), and no customer stream can squat a future system
    // name. After auth (an unauthenticated caller learns nothing),
    // before any registry read.
    if crate::billing::is_reserved_stream(&name) {
        return err_resp(
            StatusCode::FORBIDDEN,
            "reserved_stream",
            "names beginning with '_' are reserved for the system",
        );
    }
    match method {
        Method::PUT => {
            let body = match axum::body::to_bytes(body, max_body_bytes()).await {
                Ok(b) => b,
                Err(_) => {
                    return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large");
                }
            };
            let r = create_stream(state.clone(), name.clone(), headers, body).await;
            if r.status() == StatusCode::CREATED
                && let Ok(Some(d)) = state.registry.get(&state.sref(&name)).await
            {
                crate::ops::emit(
                    crate::ops::OpsEvent::new(
                        "stream_created",
                        format!("life/{}/created", d.stream_epoch),
                    )
                    .stream(&d.stream_epoch, &name),
                );
            }
            r
        }
        Method::POST => {
            let r = append(state.clone(), name.clone(), headers, body, None, None, None).await;
            // Operation count only (§4.5) — the BILLED ingest bytes are
            // the committer's, atomic with the records themselves.
            if r.status().is_success()
                && let Ok(Some(desc)) = state.registry.get(&state.sref(&name)).await
            {
                crate::billing::meter_append_request(&state, &desc);
            }
            r
        }
        Method::GET => read(state, name, params, headers, false).await,
        Method::HEAD => read(state, name, params, headers, true).await,
        Method::DELETE => delete_stream(state, name).await,
        Method::OPTIONS => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("access-control-allow-origin", "*")
            .header(
                "access-control-allow-methods",
                "GET, PUT, POST, HEAD, DELETE, OPTIONS",
            )
            .header(
                // `*` does not authorize Authorization: it is a
                // forbidden-wildcard request header, so a bearer request
                // fails preflight unless the name is listed.
                "access-control-allow-headers",
                "authorization, content-type, stream-encryption-key, \
                 stream-closed, stream-ttl, stream-forked-from, \
                 stream-fork-offset, stream-fork-sub-offset, \
                 producer-id, producer-epoch, producer-seq, if-none-match",
            )
            .header("access-control-max-age", "600")
            .body(Body::empty())
            .unwrap(),
        _ => err_resp(
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
            "unsupported method",
        ),
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
pub(crate) enum KeyCheck {
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

pub(crate) fn check_key(raw: Option<&str>, desc: &StreamDesc) -> KeyCheck {
    let Some(raw) = raw else {
        return KeyCheck::Missing;
    };
    let Ok(key) = StreamKey::from_b64(raw) else {
        return KeyCheck::Wrong;
    };
    let Some(epoch) = desc.epoch_bytes() else {
        return KeyCheck::BadDescriptor;
    };
    if key.fingerprint(&epoch) != desc.key_fingerprint {
        return KeyCheck::Wrong;
    }
    KeyCheck::Ok(key, epoch)
}

fn key_version(headers: &HeaderMap) -> u32 {
    headers
        .get("stream-key-version")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

pub(crate) fn desc_alive(desc: &StreamDesc) -> bool {
    !desc.deleted && !desc.soft_deleted && desc.expires_at_ms.map(|e| now_ms() < e).unwrap_or(true)
}

/// Identity of a creation request: a replayed PUT hashes identically,
/// so it JOINS an in-flight initialization instead of observing the
/// descriptor and skipping the work. Deliberately NOT keyed by the
/// encryption key — the key is checked separately, because a request
/// that differs only by key must be refused, not treated as a new one.
pub(crate) fn create_request_hash(
    content_type: &str,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    close: bool,
    body: &[u8],
    fork: Option<&crate::registry::ForkRef>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(content_type.as_bytes());
    h.update([0u8]);
    h.update(ttl_secs.unwrap_or(0).to_le_bytes());
    h.update(expires_at_ms.unwrap_or(0).to_le_bytes());
    h.update([u8::from(close)]);
    h.update((body.len() as u64).to_le_bytes());
    h.update(body);
    if let Some(fr) = fork {
        h.update(fr.source.as_bytes());
        h.update([0u8]);
        // The source INCARNATION is part of the identity. Without it, a
        // retry against a recreated source hashed the same as the
        // original, so it resumed an initialization whose stored
        // forked_from still pointed at the previous epoch — reference
        // installed on incarnation B, child recorded against A, and
        // stitched reads later failing the epoch check.
        h.update(fr.source_epoch.as_bytes());
        h.update([0u8]);
        h.update(fr.fork_offset.to_le_bytes());
        h.update(fr.fork_sub.to_le_bytes());
    }
    hex(&h.finalize()[..16])
}

/// A live descriptor whose initialization has not completed: its
/// content is not durable yet, so readers and appenders get a retryable
/// answer rather than an empty stream.
///
/// Readiness is `init.is_none()` and nothing else. This used to expire
/// with the claim, which answered the wrong question: after 15 seconds
/// an abandoned half-built stream started serving as though it were
/// finished — the original field anomaly, delayed. Whether the claim is
/// stale decides only WHO MAY TAKE OVER the work (see
/// [`init_claim_stale`]); it never makes incomplete content visible.
pub(crate) fn initializing(desc: &StreamDesc) -> bool {
    desc.init.is_some()
}

/// Whether an initialization claim is old enough for another request to
/// take it over. A creator is a single in-process task, so a crash must
/// not wedge the name forever — but taking over means REDOING the work,
/// not declaring it done.
pub(crate) fn init_claim_stale(desc: &StreamDesc) -> bool {
    desc.init
        .as_ref()
        .is_some_and(|i| now_ms() - i.claimed_ms > crate::registry::INIT_CLAIM_MS)
}

fn creating_resp() -> Response {
    let mut r = err_resp(
        StatusCode::SERVICE_UNAVAILABLE,
        "creating",
        "stream is still being created; retry",
    );
    r.headers_mut()
        .insert("retry-after", axum::http::HeaderValue::from_static("1"));
    r
}

/// Not-alive answer per the pinned fork lifecycle: a soft-deleted (or
/// expired-with-live-forks) stream is GONE (410) — its data still
/// backs forks and its name cannot be silently reused — while an
/// ordinary missing/dead stream stays 404.
pub(crate) fn gone_or_missing(desc: Option<&StreamDesc>) -> Response {
    if let Some(d) = desc {
        let expired_with_forks = !d.deleted
            && !d.fork_children.is_empty()
            && d.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false);
        if d.soft_deleted || expired_with_forks {
            return err_resp(
                StatusCode::GONE,
                "gone",
                "stream deleted; live forks remain",
            );
        }
    }
    err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found")
}

/// `Stream-Fork-Offset` parsing: our own opaque tokens, plus the
/// reference-format `<hex16>_<hex16>` literals the conformance suite
/// hardcodes for "zero" and "far beyond".
fn parse_fork_offset(tok: &str) -> Result<u64, String> {
    if let Some((a, b)) = tok.split_once('_') {
        if a.len() == 16
            && b.len() == 16
            && a.chars().all(|c| c.is_ascii_hexdigit())
            && b.chars().all(|c| c.is_ascii_hexdigit())
        {
            let hi = u64::from_str_radix(a, 16).map_err(|e| e.to_string())?;
            let lo = u64::from_str_radix(b, 16).map_err(|e| e.to_string())?;
            return Ok(hi.saturating_add(lo));
        }
        return Err("malformed fork offset".into());
    }
    Offset::parse(tok).map(|o| o.scan_from())
}

/// A fork's ancestor chain, self-first: (descriptor, fork boundary,
/// epoch bytes). boundary = where the entry's OWN records begin.
/// Soft-deleted/expired ancestors still serve (their data backs this
/// fork); a hard-deleted ancestor is an integrity error.
fn fork_chain_of(
    state: &Arc<AppState>,
    desc: &StreamDesc,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Vec<(StreamDesc, u64, [u8; 16])>, String>> + Send>,
> {
    let state_reg = state.clone();
    let desc = desc.clone();
    Box::pin(async move {
        // Bounded, cycle-free, and epoch-checked (audit P0): a stale
        // reference must be an integrity error, never a silent read of
        // a RECREATED source incarnation.
        const MAX_FORK_DEPTH: usize = 64;
        let mut chain: Vec<(StreamDesc, u64, [u8; 16])> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cur = desc;
        loop {
            if chain.len() >= MAX_FORK_DEPTH {
                return Err("fork chain exceeds the maximum depth".into());
            }
            if !seen.insert(format!("{}\u{0}{}", cur.name, cur.stream_epoch)) {
                return Err("fork chain contains a cycle".into());
            }
            let boundary = cur.forked_from.as_ref().map(|f| f.fork_offset).unwrap_or(0);
            let epoch = cur.epoch_bytes().ok_or("bad epoch in fork chain")?;
            let parent = cur
                .forked_from
                .as_ref()
                .map(|f| (f.source.clone(), f.source_epoch.clone()));
            chain.push((cur, boundary, epoch));
            match parent {
                None => break,
                Some((src, want_epoch)) => {
                    let d = match state_reg.registry.get(&state_reg.sref(&src)).await {
                        Ok(Some(d)) if !d.deleted => d,
                        _ => return Err(format!("fork source '{src}' is gone")),
                    };
                    if !want_epoch.is_empty() && d.stream_epoch != want_epoch {
                        return Err(format!(
                            "fork source '{src}' is a different incarnation                              (expected {want_epoch}, found {})",
                            d.stream_epoch
                        ));
                    }
                    cur = d;
                }
            }
        }
        Ok(chain)
    })
}

/// Stitched fork read (pinned DS fork contract): records [from, ...)
/// in the stream's OWN offset numbering, served from the ancestor
/// chain below each fork boundary and from the stream itself at and
/// above its boundary. `end`/`completed` describe the OWN tail.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_stitched(
    state: &Arc<AppState>,
    desc: &StreamDesc,
    key: &StreamKey,
    from: u64,
    max_bytes: usize,
) -> Result<ReadOut, String> {
    let chain = fork_chain_of(state, desc).await?;
    // Every hop must accept the presented key (uniform-key chains; a
    // cross-key fork chain would decrypt garbage, so it is an error).
    for (d, _, _) in &chain {
        if matches!(check_key(Some(&key_b64_of(key)), d), KeyCheck::Wrong) {
            return Err("wrong key for a fork ancestor".into());
        }
    }
    // Own tail state.
    let (own_engine, own_handle) = handle_of(state, &chain[0].0).await?;
    let own_end = own_handle.state.lock().unwrap().durable.next;
    let mut out = ReadOut {
        recs: Vec::new(),
        last: None,
        end: own_end,
        completed: false,
    };
    let mut budget = max_bytes;
    let mut cursor = from;
    for _ in 0..(chain.len() * 4 + 8) {
        if budget == 0 {
            break;
        }
        // Owner of `cursor`: the deepest entry whose boundary <= cursor.
        let Some(idx) = chain.iter().position(|(_, b, _)| *b <= cursor) else {
            return Err("fork chain has no owner for offset".into());
        };
        // Cap: the smallest child boundary above the cursor.
        let cap = chain[..idx]
            .iter()
            .map(|(_, b, _)| *b)
            .min()
            .unwrap_or(u64::MAX);
        let (d, _, epoch) = &chain[idx];
        let (engine, handle) = if idx == 0 {
            (own_engine.clone(), own_handle.clone())
        } else {
            handle_of(state, d).await?
        };
        state.keys.put(handle.hash, key.clone(), *epoch);
        // The DEFAULT key only. `None` here meant "every routing key",
        // so a raw fork of a collection that product clients had
        // written keyed records to replayed all of them through the
        // standards route — the one surface whose contract is that it
        // IS the default-key stream.
        let part = read_merged(
            key,
            epoch,
            &handle,
            &engine,
            cursor,
            Some(""),
            budget,
            crate::shard::Deliver::Durable,
        )
        .await?;
        let mut advanced = false;
        for r in part.recs {
            if r.off >= cap {
                break;
            }
            budget = budget.saturating_sub(r.payload.len());
            cursor = r.off + 1;
            out.last = Some(r.off);
            out.recs.push(r);
            advanced = true;
        }
        if idx == 0 {
            // Own range: read_merged's completion IS the answer.
            out.end = part.end;
            out.completed = part.completed;
            break;
        }
        if part.completed || cursor >= cap {
            // Ancestor drained to the cap (or its whole range): hop to
            // the next owner at the cap.
            cursor = cursor.max(cap);
            if cursor < cap && part.completed {
                // The ancestor's durable end sits below the cap only if
                // records were lost — surface it rather than looping.
                return Err("fork ancestor ended below the fork boundary".into());
            }
            continue;
        }
        if !advanced {
            // Budget too small for one record: honest partial.
            break;
        }
    }
    Ok(out)
}

/// Clear a descriptor's parent-reference debt — fenced to the exact
/// incarnation the debt was OBSERVED on and to the exact debt. The
/// name-only `Registry::update` this replaces could clear a freshly
/// recreated descriptor's genuinely unpaid debt (round 15: the debt
/// flag has the same ABA shape as every other lifecycle decision — a
/// name is not an identity).
async fn clear_parent_debt(
    state: &Arc<AppState>,
    name: &str,
    expect_epoch: &str,
    expect_ref: Option<(&str, &str)>,
) -> Result<(), String> {
    let _ = state
        .registry
        .mutate_incarnation(&state.sref(name), expect_epoch, |x| {
            let debt_matches = match expect_ref {
                Some((src, fid)) => x
                    .forked_from
                    .as_ref()
                    .is_some_and(|f| f.source == src && f.fork_id == fid),
                None => x.forked_from.is_none(),
            };
            if x.parent_ref_pending && debt_matches {
                let mut next = x.clone();
                next.parent_ref_pending = false;
                crate::registry::Mutation::Write(next, ())
            } else {
                // Different debt, different incarnation's business, or
                // already paid: not ours to clear.
                crate::registry::Mutation::Decline(())
            }
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(&state.sref(name));
    Ok(())
}

fn key_b64_of(key: &StreamKey) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(key.0)
}

/// (engine, handle) for a stream's sole segment identity.
async fn handle_of(
    state: &Arc<AppState>,
    desc: &StreamDesc,
) -> Result<(Arc<ShardEngine>, Arc<crate::shard::StreamHandle>), String> {
    let ro = desc.resolve_segment("");
    let engine = state
        .engine_for_scaler(&ro.shard_route)
        .await
        .ok_or("engine unavailable")?;
    let handle = engine
        .stream_handle(ro.identity)
        .await
        .map_err(|e| e.to_string())?;
    Ok((engine, handle))
}

pub(crate) fn rand_epoch() -> [u8; 16] {
    use rand::RngCore;
    let mut e = [0u8; 16];
    rand::rng().fill_bytes(&mut e);
    e
}

/// Sliding idle expiry (protocol Stream-TTL): origin reads and writes
/// reset the window. Lazy — a slide fires only once at least a quarter
/// of the window has elapsed, bounding registry CAS traffic; an active
/// stream only needs SOME slide before expiry.
pub(crate) fn touch_ttl(state: &Arc<AppState>, desc: &StreamDesc) {
    let Some(ttl) = desc.ttl_secs else { return };
    let Some(exp) = desc.expires_at_ms else {
        return;
    };
    let window_ms = (ttl as i64).saturating_mul(1000);
    let now = now_ms();
    if exp.saturating_sub(now) >= window_ms - window_ms / 4 {
        return; // window still fresh
    }
    // One in-flight slide per stream: without this, every request in
    // the window between spawn and CAS completion spawns ANOTHER CAS —
    // a herd against the registry under rapid op sequences.
    fn sliding() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
        static S: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
            std::sync::OnceLock::new();
        S.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
    }
    if !sliding().lock().unwrap().insert(desc.name.clone()) {
        return; // a slide is already in flight
    }
    let target = now + window_ms;
    let state = state.clone();
    let name = desc.name.clone();
    let expect_epoch = desc.stream_epoch.clone();
    tokio::spawn(async move {
        // cas_update is single-shot; a slide can race another descriptor
        // write (e.g. the close path's seal) — retry the benign conflict.
        // Incarnation-fenced: a slide spawned against incarnation A must
        // not extend the expiry of a replacement created under the same
        // name while the task sat on the runtime.
        if let Err(e) = state
            .registry
            .cas_update_incarnation(&state.sref(&name), &expect_epoch, |d| {
                if d.ttl_secs.is_none() {
                    return false;
                }
                match d.expires_at_ms {
                    Some(e) if e < target => {
                        d.expires_at_ms = Some(target);
                        true
                    }
                    _ => false,
                }
            })
            .await
        {
            tracing::warn!(stream = %name, "ttl slide lost: {e}");
        }
        state.registry.invalidate(&state.sref(&name));
        sliding().lock().unwrap().remove(&name);
    });
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
            if id.is_empty() {
                return Err("Producer-Id must not be empty".into());
            }
            // The seal machinery synthesizes producer identities for
            // records a client never coordinates itself. They share the
            // durable producer keyspace, so the wire must not be able to
            // name one: a caller who pre-created `prisma.seal.<op>` at
            // sequence 0 would make a later seal's final append look
            // like a duplicate — the seal would then "complete" without
            // ever writing its record.
            if id.starts_with(crate::shard::INTERNAL_PRODUCER_PREFIX) {
                return Err(format!(
                    "Producer-Id must not begin with '{}' (reserved)",
                    crate::shard::INTERNAL_PRODUCER_PREFIX
                ));
            }
            let epoch = parse_uint_strict(&e).ok_or("invalid Producer-Epoch")?;
            let seq = parse_uint_strict(&s).ok_or("invalid Producer-Seq")?;
            Ok(Some(crate::shard::ProducerReq {
                id,
                epoch,
                seq,
                request_hash: None,
            }))
        }
        _ => Err("Producer-Id, Producer-Epoch and Producer-Seq must be sent together".into()),
    }
}

fn fresh_desc(
    state: &AppState,
    name: &str,
    key: &StreamKey,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
) -> StreamDesc {
    let epoch = rand_epoch();
    StreamDesc {
        name: name.to_string(),
        account_id: Some(state.account_id.clone()),
        project_id: state.tenant.clone(),
        stream_epoch: hex(&epoch),
        seal_gen_counter: 0,
        key_fingerprint: key.fingerprint(&epoch),
        created_ms: now_ms(),
        expires_at_ms: ttl_secs
            .map(|t| now_ms() + (t as i64) * 1000)
            .or(expires_at_ms),
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        content_type,
        ttl_secs,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
}

/// Product-surface descriptor construction (spec Stage 7): no profile,
/// no touch material — those are capability resources, not creation
/// config.
pub(crate) fn fresh_desc_product(
    state: &AppState,
    name: &str,
    key: &StreamKey,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
) -> StreamDesc {
    fresh_desc(state, name, key, content_type, ttl_secs, expires_at_ms)
}

/// R2 ring-ownership check shared by both creation surfaces.
pub(crate) fn ring_owner_check(state: &Arc<AppState>, name: &str) -> Option<Response> {
    let prefix = shard_for_hash(
        &state.shard_prefixes,
        &crate::crypto::RouteHash::for_stream(&state.sref(name)).0,
    );
    if let Some(owner) = state.effective_owner(&prefix)
        && owner != state.instance_name
    {
        let mut r = err_resp(
            StatusCode::CONFLICT,
            "not_ring_owner",
            &format!("shard {prefix} belongs to {owner}"),
        );
        if let Ok(v) = axum::http::HeaderValue::from_str(&owner) {
            r.headers_mut().insert("streams-replay-to", v);
        }
        return Some(r);
    }
    None
}

/// Product DELETE maps to the one collection-delete implementation.
pub(crate) async fn product_delete(state: Arc<AppState>, name: String) -> Response {
    delete_stream(state, name).await
}

pub(crate) async fn create_stream(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // The __ds control namespace is reserved on BOTH surfaces
    // (appendix §2.6); subpaths are caught by explicit routing, the
    // bare name here.
    if name == crate::product::RESERVED_ROOT
        || name.starts_with(&format!("{}/", crate::product::RESERVED_ROOT))
    {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "reserved",
            "__ds is the reserved Durable Streams control namespace",
        );
    }
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
    // Ring ownership is checked HERE, before any registry mutation.
    // engine_for would catch it further down, but by then the descriptor
    // has been written — so a non-owner answered 409 for a stream it had
    // already created, which is both inconsistent and the first thing a
    // new user hits. Answer the same 409 + Streams-Replay-To contract the
    // append and read paths use, and let the client re-issue at the owner.
    {
        let prefix = shard_for_hash(
            &state.shard_prefixes,
            &crate::crypto::RouteHash::for_stream(&state.sref(&name)).0,
        );
        if let Some(owner) = state.effective_owner(&prefix)
            && owner != state.instance_name
        {
            let mut r = err_resp(
                StatusCode::CONFLICT,
                "not_ring_owner",
                &format!("shard {prefix} belongs to {owner}"),
            );
            if let Ok(v) = axum::http::HeaderValue::from_str(&owner) {
                r.headers_mut().insert("streams-replay-to", v);
            }
            return r;
        }
    }
    let ct_hdr_present = hdr(&headers, "content-type").is_some();
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
    let ttl_secs = match &ttl_hdr {
        Some(t) => match parse_ttl_strict(t) {
            Some(v) => Some(v),
            None => return err_resp(StatusCode::BAD_REQUEST, "invalid_ttl", "invalid Stream-TTL"),
        },
        None => None,
    };
    let mut ttl_secs = ttl_secs;
    let expires_at_ms = match &exp_hdr {
        Some(e) => match chrono::DateTime::parse_from_rfc3339(e) {
            Ok(ts) => Some(ts.timestamp_millis()),
            Err(_) => {
                return err_resp(StatusCode::BAD_REQUEST, "invalid_request", "bad Expires-At");
            }
        },
        None => None,
    };
    let close = want_close(&headers);
    // Opt-in per-key ordering (PER-KEY-ORDERING.md §2). Absent => total
    // order, byte-identical semantics to before this feature existed.
    // ROUTING-V3 (docs/ROUTING-V3.md): one routing model. Every stream
    // is key-partitioned internally, per-key ordered, born with one
    // segment, and scaled automatically — ordering, segmentation and
    // scaling are no longer creation-time choices, and the old knobs
    // are rejected loudly rather than silently ignored.
    for h in ["stream-ordering", "stream-segments", "stream-scaling"] {
        if hdr(&headers, h).is_some() {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "unified_routing",
                &format!(
                    "{h} was removed: streams are key-partitioned with \
                     automatic scaling (docs/ROUTING-V3.md)"
                ),
            );
        }
    }

    // ---- Fork creation (pinned DS protocol fork contract) ----------
    // Parsed before descriptor resolution: validation errors must beat
    // creation, and the fork identity participates in the idempotent
    // compare.
    struct ForkCtx {
        source: String,
        source_desc: StreamDesc,
        boundary: u64,
        sub: u64,
        materialize: Option<Bytes>,
    }
    let fork_src_hdr = hdr(&headers, "stream-forked-from");
    let fork_off_hdr = hdr(&headers, "stream-fork-offset");
    let fork_sub_hdr = hdr(&headers, "stream-fork-sub-offset");
    if fork_src_hdr.is_none() && (fork_off_hdr.is_some() || fork_sub_hdr.is_some()) {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "fork_headers",
            "Stream-Fork-Offset/Sub-Offset require Stream-Forked-From",
        );
    }
    let fork_ctx: Option<ForkCtx> = if let Some(src_raw) = &fork_src_hdr {
        let src_name = src_raw
            .strip_prefix("/v1/stream/")
            .unwrap_or(src_raw)
            .trim_matches('/')
            .to_string();
        // Is THIS child already mid-initialization against this source?
        // If so, the source is being retained FOR IT — its reference is
        // already installed — and refusing a retained source would leave
        // the child permanently Initializing over data kept expressly to
        // serve it. Resolve the target's own state before demanding a
        // live source.
        // Ready OR still initializing: either way, if this exact child
        // already holds a reference on the source, the source is being
        // retained for it. Restricting this to initializing children
        // broke idempotence — a completed fork whose response was lost
        // could not be re-PUT once its source was retained, because the
        // soft-delete check fired first.
        let resuming_child = match state.registry.get(&state.sref(&name)).await {
            Ok(Some(c)) if !c.deleted => c
                .forked_from
                .clone()
                .filter(|f| f.source == src_name && !f.fork_id.is_empty()),
            _ => None,
        };
        let src = match state.registry.get(&state.sref(&src_name)).await {
            Ok(Some(d)) if desc_alive(&d) => d,
            // Retained for this very child: same incarnation, and the
            // reference this child installed is still on it.
            Ok(Some(d))
                if !d.deleted
                    && resuming_child.as_ref().is_some_and(|f| {
                        f.source_epoch == d.stream_epoch && d.fork_children.contains(&f.fork_id)
                    }) =>
            {
                d
            }
            Ok(Some(d)) if d.soft_deleted => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_source_gone",
                    "source is deleted (data retained for existing forks only)",
                );
            }
            Ok(_) => {
                return err_resp(StatusCode::NOT_FOUND, "not_found", "fork source not found");
            }
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
        };
        if src
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
        {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "fork_segmented_source",
                "forking a segmented collection is not supported",
            );
        }
        // Content type: inherit when omitted; explicit mismatch is 409
        // BEFORE any reference is taken.
        if ct_hdr_present
            && crate::registry::media_type(&content_type)
                != crate::registry::media_type(&src.content_type)
        {
            return err_resp(
                StatusCode::CONFLICT,
                "fork_content_type_mismatch",
                "fork content type must match the source",
            );
        }
        if !ct_hdr_present {
            content_type = src.content_type.clone();
        }
        if ttl_secs.is_none() && exp_hdr.is_none() {
            ttl_secs = src.ttl_secs; // inherit source TTL
        }
        // Source key must accept the presented key (fork reads decrypt
        // the ancestor's records with it).
        let (src_key, _src_epoch) = match check_key(raw_key(&headers, &state), &src) {
            KeyCheck::Ok(k, e) => (k, e),
            KeyCheck::Wrong => {
                return err_resp(
                    StatusCode::FORBIDDEN,
                    "wrong_key",
                    "key mismatch with source",
                );
            }
            _ => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "missing_key",
                    "Stream-Encryption-Key required",
                );
            }
        };
        let (_, src_handle) = match handle_of(&state, &src).await {
            Ok(v) => v,
            Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
        };
        let src_end = src_handle.state.lock().unwrap().durable.next;
        let base = match &fork_off_hdr {
            None => src_end,
            Some(tok) => match parse_fork_offset(tok) {
                Ok(v) => v,
                Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_fork_offset", &m),
            },
        };
        if base > src_end {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "fork_offset_beyond_end",
                "fork offset beyond the source's length",
            );
        }
        let sub: u64 = match &fork_sub_hdr {
            None => 0,
            Some(v) => match v.trim().parse::<u64>() {
                Ok(n) if v.trim().chars().all(|c| c.is_ascii_digit()) => n,
                _ => {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "invalid_fork_sub_offset",
                        "sub-offset must be a non-negative integer",
                    );
                }
            },
        };
        let mut boundary = base;
        let mut materialize: Option<Bytes> = None;
        if sub > 0 {
            if fork_off_hdr.is_none() {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "fork_headers",
                    "a sub-offset requires an explicit Stream-Fork-Offset",
                );
            }
            if src_end == 0 {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "fork_sub_offset_empty_source",
                    "a sub-offset needs a record to split",
                );
            }
            if src.is_json() {
                // Messages ARE records in this implementation: the
                // sub-offset advances the record boundary.
                boundary = base.saturating_add(sub);
                if boundary > src_end {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "fork_sub_offset_beyond_end",
                        "sub-offset overshoots the source",
                    );
                }
            } else {
                if base >= src_end {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "fork_sub_offset_beyond_end",
                        "no record at the fork offset",
                    );
                }
                // The record being split (the source may itself be a
                // fork — read through its chain).
                let rec = match read_stitched(&state, &src, &src_key, base, 64 << 20).await {
                    Ok(out) => out.recs.into_iter().find(|r| r.off == base),
                    Err(m) => {
                        return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m);
                    }
                };
                let Some(rec) = rec else {
                    return err_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "source record unavailable for sub-offset validation",
                    );
                };
                let len = rec.payload.len() as u64;
                if sub > len {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "fork_sub_offset_beyond_end",
                        "sub-offset overshoots the record",
                    );
                }
                if sub == len {
                    boundary = base + 1; // whole record inherited
                } else {
                    boundary = base; // partial materializes at `base`
                    materialize = Some(rec.payload.slice(..sub as usize));
                }
            }
        }
        Some(ForkCtx {
            source: src_name,
            source_desc: src,
            boundary,
            sub,
            materialize,
        })
    } else {
        None
    };
    let expected_fork_ref = fork_ctx.as_ref().map(|fc| crate::registry::ForkRef {
        source: fc.source.clone(),
        source_epoch: fc.source_desc.stream_epoch.clone(),
        fork_offset: fc.boundary,
        fork_sub: fc.sub,
        // The fork's unique id in the source's child set: this
        // incarnation's epoch, stamped after the descriptor exists.
        fork_id: String::new(),
    });

    // Creation-request identity (audit P0): a replayed PUT hashes
    // identically, so it JOINS an in-flight initialization instead of
    // observing the descriptor and skipping the work.
    let needs_init = !body.is_empty() || close || fork_src_hdr.is_some();
    let create_hash = create_request_hash(
        &content_type,
        ttl_secs,
        expires_at_ms,
        close,
        &body,
        expected_fork_ref.as_ref(),
    );

    // Resolve existing.
    let existing = match state.registry.get(&state.sref(&name)).await {
        Ok(v) => v,
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    // Idempotent-PUT validation against a live descriptor: shared by the
    // alive arm and by a lost recreate race (the winner's incarnation is
    // live, so the loser must observe it under the same rules).
    let validate_live =
        |d: crate::registry::StreamDesc| -> Result<(bool, crate::registry::StreamDesc), Response> {
            let same_ct = crate::registry::media_type(&d.content_type)
                == crate::registry::media_type(&content_type)
                || hdr(&headers, "content-type").is_none();
            // ROUTING-V3: ordering/segmentation are no longer part of
            // user-visible config, so the idempotent-PUT compare ignores
            // the legacy fields — a headerless re-PUT of a pre-v3
            // per-key stream is config-identical, not a conflict.
            let same_fork = match (&d.forked_from, &expected_fork_ref) {
                (None, None) => true,
                (Some(a), Some(b)) => a.same_identity(b),
                _ => false,
            };
            if !same_ct || d.ttl_secs != ttl_secs || !same_fork {
                return Err(err_resp(
                    StatusCode::CONFLICT,
                    "config_mismatch",
                    "stream exists with different config",
                ));
            }
            match check_key(raw_key(&headers, &state), &d) {
                KeyCheck::Ok(..) => {}
                KeyCheck::Wrong => {
                    return Err(err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"));
                }
                _ => {}
            }
            Ok((false, d))
        };
    // An INITIALIZING descriptor (audit P0): its content is not durable
    // yet. The same request resumes the work; a different request is a
    // conflict, not an idempotent hit; a stale claim is taken over.
    let mut resume_init = false;
    if let Some(d) = existing.as_ref()
        && let Some(init) = &d.init
    {
        if !desc_alive(d) {
            // dead-and-initializing: fall through to the recreate arm
        } else if init.request_hash != create_hash {
            if !init_claim_stale(d) {
                return err_resp(
                    StatusCode::CONFLICT,
                    "creating",
                    "stream is being created by a different request",
                );
            }
            return err_resp(
                StatusCode::CONFLICT,
                "config_mismatch",
                "stream exists with different config",
            );
        } else {
            // The resume path skips validate_live, so the key has to
            // be checked HERE. Without it, a caller replaying the
            // same creation body under a DIFFERENT key resumed the
            // initialization and wrote the initial content with that
            // key, while the descriptor kept the original
            // fingerprint — a stream whose configured key cannot
            // decrypt its own first record.
            match check_key(raw_key(&headers, &state), d) {
                KeyCheck::Ok(..) => {}
                _ => {
                    return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch");
                }
            }
            // Belt and braces: the initialization identity itself
            // records which key it was claimed for.
            if !init.key_fingerprint.is_empty() && init.key_fingerprint != d.key_fingerprint {
                return err_resp(
                    StatusCode::FORBIDDEN,
                    "wrong_key",
                    "initialization was claimed under a different key",
                );
            }
            // …and the recorded parentage must still be the one this
            // request is asking for. The resume path skips
            // validate_live, which is where forks are normally
            // compared.
            match (&d.forked_from, &expected_fork_ref) {
                (None, None) => {}
                (Some(a), Some(b)) if a.same_identity(b) => {}
                _ => {
                    return err_resp(
                        StatusCode::CONFLICT,
                        "fork_source_changed",
                        "this initialization was claimed against a different fork source",
                    );
                }
            }
            resume_init = true;
        }
    }

    let (created, mut desc) = match existing {
        // Resume: the SAME creation request found its own in-flight
        // (or abandoned) initialization — redo it idempotently.
        Some(d) if resume_init => (true, d),
        Some(d) if desc_alive(&d) => match validate_live(d) {
            Ok(v) => v,
            Err(r) => return r,
        },
        Some(d)
            if d.soft_deleted
                || (!d.fork_children.is_empty()
                    && !d.deleted
                    && d.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false)) =>
        {
            // The name still backs live forks: blocked, not recreated
            // (pinned fork lifecycle).
            return err_resp(
                StatusCode::CONFLICT,
                "gone",
                "name is soft-deleted; live forks retain its data",
            );
        }
        Some(_) => {
            // Dead incarnation: recreate with a fresh epoch (fresh keyspace).
            // Predicated CAS — one winner; a loser validates against the
            // winner's live descriptor exactly like an idempotent PUT.
            let mut fresh = fresh_desc(
                &state,
                &name,
                &key,
                content_type.clone(),
                ttl_secs,
                expires_at_ms,
            );
            fresh.forked_from = expected_fork_ref.clone();
            let fp = fresh.key_fingerprint.clone();
            fresh.init = needs_init.then(|| crate::registry::InitState {
                request_hash: create_hash.clone(),
                key_fingerprint: fp,
                claimed_ms: now_ms(),
            });
            match state
                .registry
                .recreate(&state.sref(&name), fresh, |d| {
                    !desc_alive(d) && !d.soft_deleted
                })
                .await
            {
                Ok((true, d)) => (true, d),
                Ok((false, winner)) => match validate_live(winner) {
                    Ok(v) => v,
                    Err(r) => return r,
                },
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
            let mut fresh = fresh_desc(
                &state,
                &name,
                &key,
                content_type.clone(),
                ttl_secs,
                expires_at_ms,
            );
            fresh.forked_from = expected_fork_ref.clone();
            let fp = fresh.key_fingerprint.clone();
            fresh.init = needs_init.then(|| crate::registry::InitState {
                request_hash: create_hash.clone(),
                key_fingerprint: fp,
                claimed_ms: now_ms(),
            });
            match state.registry.create(fresh).await {
                Ok((true, d)) => (true, d),
                Ok((false, d)) => {
                    // Raced: treat as idempotent-config path.
                    if crate::registry::media_type(&d.content_type)
                        != crate::registry::media_type(&content_type)
                        || d.ttl_secs != ttl_secs
                    {
                        return err_resp(StatusCode::CONFLICT, "config_mismatch", "conflict");
                    }
                    // The winner may still be INITIALIZING (audit P0):
                    // this replay must JOIN that initialization, not
                    // answer success for content that is not durable
                    // yet. Same request -> resume; different request ->
                    // conflict.
                    // Joining or taking over someone else's
                    // initialization writes THIS request's content under
                    // THIS request's key, so it has to be the right one.
                    if d.init.is_some()
                        && !matches!(check_key(raw_key(&headers, &state), &d), KeyCheck::Ok(..))
                    {
                        return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch");
                    }
                    match d.init.as_ref() {
                        Some(i) if i.request_hash == create_hash => (true, d),
                        Some(_) if !init_claim_stale(&d) => {
                            return err_resp(
                                StatusCode::CONFLICT,
                                "creating",
                                "stream is being created by a different request",
                            );
                        }
                        Some(_) => (true, d), // stale claim: take it over
                        None => (false, d),
                    }
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

    let hash = desc.resolve_segment("").identity;
    let epoch_bytes = desc.epoch_bytes().unwrap_or([0u8; 16]);
    state.keys.put(hash, key.clone(), epoch_bytes);
    // Shard choice keys off the stream NAME hash (COMPUTE-SPEC R1) so the
    // router can compute placement without knowing the stream epoch; the
    // record keyspace keeps using storage/segment hashes.
    let engine = match state
        .engine_for(&crate::crypto::RouteHash::for_stream(&desc.sref()).0)
        .await
    {
        Ok(e) => e,
        Err(r) => return r,
    };

    // Fork post-create (pinned DS fork contract): the tail row must be
    // seeded at the fork boundary BEFORE the first handle load caches
    // next = 0, and the source's reference count records this fork.
    let mut materialize_entry: Option<Bytes> = None;
    if let Some(fc) = &fork_ctx
        && created
    {
        if let Err(e) = engine
            .seed_fork_tail(
                hash,
                crate::crypto::RouteHash::for_stream(&desc.sref()).0,
                fc.boundary,
            )
            .await
        {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
        // Install the reference by unique id (idempotent set
        // insert). A CAS that DECLINES means the source vanished or
        // was tombstoned in the race — the audit found that treated
        // as success, leaving a live fork pointing at a deleted
        // source. The presence check below is the actual proof.
        let fork_id = desc.stream_epoch.clone();
        // Stamp the id into our OWN ForkRef so release can name it.
        if desc
            .forked_from
            .as_ref()
            .is_some_and(|f| f.fork_id.is_empty())
        {
            let fid = fork_id.clone();
            let mut already = false;
            let stamped = match state
                .registry
                .cas_update_incarnation(&state.sref(&name), &desc.stream_epoch, |d| {
                    match d.forked_from.as_mut() {
                        Some(f) if f.fork_id.is_empty() => {
                            f.fork_id = fid.clone();
                            true
                        }
                        Some(f) => {
                            already = f.fork_id == fid;
                            false
                        }
                        None => false,
                    }
                })
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    return err_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &e.to_string(),
                    );
                }
            };
            state.registry.invalidate(&state.sref(&name));
            // A declined CAS here means the child was deleted (or
            // re-forked) underneath us. Installing a source
            // reference for it anyway would pin the source's data
            // for a child that no longer exists.
            if !stamped && !already {
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_target_changed",
                    "the fork target changed while it was being created; retry",
                );
            }
            // Reflect the stamp into the LOCAL snapshot: the
            // readiness give-back names the reference through
            // `desc.forked_from.fork_id`, and a stale empty id
            // silently skipped the release — the source stayed
            // pinned by a child deleted mid-creation (FRK-013).
            if let Some(f) = desc.forked_from.as_mut() {
                f.fork_id = fork_id.clone();
            }
        }
        // The CHILD must still exist to be worth anchoring: a
        // half-made child deleted in the stamp-to-install window
        // must not pin the source at all (FRK-013). This check
        // closes the ordinary path; the residual window between it
        // and the install CAS — including a crash inside it — is
        // repaired by the tombstone's RETAINED debt. The park sits
        // BETWEEN the check and the install so tests can drive
        // exactly that window.
        match state.registry.get(&state.sref(&name)).await {
            Ok(Some(c)) if desc_alive(&c) && c.stream_epoch == desc.stream_epoch => {}
            _ => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_target_changed",
                    "the fork target changed while it was being created; retry",
                );
            }
        }
        #[cfg(test)]
        fork_failpoints::pause_fork_before_source_ref(&name).await;
        match state
            .registry
            .cas_update_retry(&state.sref(&fc.source), |d| {
                // The reference is installed on the incarnation the
                // child actually forked. Between validating the
                // source and getting here it can be recreated,
                // start expiring, begin sealing or begin a split —
                // and a reference installed on the wrong one leaves
                // a child whose data nobody is keeping.
                // Already installed — idempotent, and checked FIRST:
                // a source retained for THIS child is soft-deleted
                // by definition, so demanding liveness here refused
                // the very retry the retention exists to serve.
                if d.fork_children.iter().any(|c| c == &fork_id) {
                    return !d.deleted && d.stream_epoch == fc.source_desc.stream_epoch;
                }
                if d.deleted
                    || d.soft_deleted
                    || d.init.is_some()
                    || d.stream_epoch != fc.source_desc.stream_epoch
                    || d.sealing.is_some()
                    || d.segments.as_ref().is_some_and(|m| {
                        m.pending.is_some() || m.segments.iter().filter(|s| s.is_live()).count() > 1
                    })
                {
                    return false;
                }
                d.fork_children.push(fork_id.clone());
                true
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                // The source moved: recreated, deleted, sealing, or
                // splitting. The child exists but nothing is
                // keeping its data, so refuse rather than hand back
                // a fork with no anchor. Deleting the half-made
                // child is the delete path's job — which is now
                // resumable — so report the conflict and let the
                // caller retry against the current source.
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_source_changed",
                    "the fork source changed while the fork was being created; retry",
                );
            }
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
        }
        state.registry.invalidate(&state.sref(&fc.source));
        #[cfg(test)]
        fork_failpoints::pause_fork_after_source_ref(&name).await;
        // Post-install: the child can have been deleted between the
        // pre-check and the install CAS. Release the reference this
        // request just installed — its tombstone's retained debt
        // covers the crash variant of the same window.
        match state.registry.get(&state.sref(&name)).await {
            Ok(Some(c)) if desc_alive(&c) && c.stream_epoch == desc.stream_epoch => {}
            _ => {
                if let Err(m) =
                    release_fork_ref(&state, &fc.source, &fork_id, &fc.source_desc.stream_epoch)
                        .await
                {
                    tracing::error!(stream = %name, "releasing a dead child's fresh reference: {m}");
                }
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_target_changed",
                    "the fork target changed while it was being created; retry",
                );
            }
        }
        match state.registry.get(&state.sref(&fc.source)).await {
            Ok(Some(sd)) if sd.fork_children.iter().any(|c| c == &fork_id) => {}
            Ok(_) => {
                return err_resp(
                    StatusCode::CONFLICT,
                    "fork_source_gone",
                    "fork source disappeared before the reference was installed",
                );
            }
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
        }
        materialize_entry = fc.materialize.clone();
    }

    // Initial body / close-on-create ride the committer.
    let mut next = {
        match engine.stream_handle(hash).await {
            Ok(h) => h.state.lock().unwrap().durable.next,
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
        }
    };
    let mut closed_now = false;
    // Resume-safety (audit P0): a resumed initialization must not append
    // the initial content twice. Own records begin at `base` (0, or the
    // fork boundary); if the tail already advanced past it, the first
    // attempt's append is durable and this attempt only republishes
    // Ready.
    let own_base = fork_ctx.as_ref().map(|fc| fc.boundary).unwrap_or(0);
    let initial_content_pending = next <= own_base;
    if created
        && initial_content_pending
        && (!body.is_empty() || close || materialize_entry.is_some())
    {
        let mut entries: Vec<Bytes> = if body.is_empty() {
            Vec::new()
        } else if desc.is_json() {
            match json_entries(&body, true) {
                Ok(v) => v,
                Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_json", &m),
            }
        } else {
            vec![body.clone()]
        };
        // The materialized sub-offset partial is the fork's FIRST own
        // record; an initial body follows it in the same command.
        if let Some(m) = materialize_entry {
            entries.insert(0, m);
        }
        let subkey = derive_subkey(&key, &epoch_bytes, "", 0);
        let bytes = entries.iter().map(|e| e.len()).sum();
        #[cfg(test)]
        fork_failpoints::pause_init_before_seed(&name).await;
        let (tx, rx) = oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: crate::crypto::RouteHash::for_stream(&desc.sref()).0,
            entries,
            routing_key: String::new(),
            key_hash: crate::crypto::stream_hash(""),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes,
            close,
            // Exactly-once initial content (audit P0): the append
            // carries a synthetic producer identity derived from the
            // creation-request hash, so concurrent joiners and resumed
            // attempts are deduplicated by the SAME committer machinery
            // that guarantees producer idempotence — rather than by a
            // read-then-check race.
            producer: Some(crate::shard::ProducerReq {
                id: format!("\u{0}init\u{0}{create_hash}"),
                epoch: 1,
                seq: 0,
                request_hash: None,
            }),
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            usage: crate::usage::counters(&crate::crypto::RouteHash::for_stream(&desc.sref()).0),
            seal_gen: None,
            seal_fence_to: None,
            billing: (!crate::billing::is_reserved_stream(&desc.name)).then(|| {
                std::sync::Arc::new(crate::billing::BillingRef {
                    identity: crate::billing::identity_of(&state, &desc),
                    segment_id: 0,
                })
            }),
            resp: tx,
        };
        if engine.try_enqueue(req).is_err() {
            return err_resp(StatusCode::TOO_MANY_REQUESTS, "overloaded", "queue full");
        }
        match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
            Ok(Ok(Ok(ack))) => {
                next = ack.next_offset;
                closed_now = ack.closed || close;
            }
            _ => {
                return err_resp(
                    StatusCode::REQUEST_TIMEOUT,
                    "append_timeout",
                    "initial body timed out",
                );
            }
        }
    } else if !created && close {
        closed_now = true; // preserved on idempotent PUT of a closed stream
    } else if created && !initial_content_pending {
        // Resumed after the content already committed.
        closed_now = close;
    }

    // Publish Ready: every durable initialization step (fork tail seed,
    // source reference, initial content, close-on-create) has landed.
    // Until this CAS, a replay resumes instead of observing a stream
    // whose content never arrived.
    if created && needs_init {
        #[cfg(test)]
        fork_failpoints::pause_create_before_ready(&name).await;
        let published = match state
            .registry
            .cas_update_incarnation(&state.sref(&name), &desc.stream_epoch, |d| {
                // Our OWN claim, on OUR incarnation. Clearing `init`
                // because "something is initializing" let a paused
                // creator publish readiness for a stream that had since
                // been deleted and recreated — announcing the
                // replacement as ready before its own initial records,
                // fork seed or close-on-create had landed.
                match d.init.as_ref() {
                    Some(i) if i.request_hash == create_hash => {}
                    _ => return false,
                }
                d.init = None;
                true
            })
            .await
        {
            Ok(v) => v,
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &format!("publishing stream readiness: {e}"),
                );
            }
        };
        state.registry.invalidate(&state.sref(&name));
        // A declined CAS is NOT readiness. `cas_update` refuses a
        // deleted descriptor, so a delete that won mid-initialization
        // made this return 201 for a stream that no longer exists — and
        // if the work had already installed a fork reference, the source
        // stayed pinned by a child that was never published.
        if !published {
            let now = state.registry.get(&state.sref(&name)).await.ok().flatten();
            let live_and_ready = now.as_ref().is_some_and(|d| {
                desc_alive(d) && d.init.is_none() && d.stream_epoch == desc.stream_epoch
            });
            if !live_and_ready {
                // Compensate: give back the source reference this
                // initialization installed, so the parent is not held by
                // a child that will never exist.
                if let Some(fr) = desc.forked_from.as_ref().filter(|f| !f.fork_id.is_empty())
                    && let Err(m) =
                        release_fork_ref(&state, &fr.source, &fr.fork_id, &fr.source_epoch).await
                {
                    tracing::error!(stream = %name, "releasing an abandoned fork claim: {m}");
                }
                return gone_or_missing(now.as_ref());
            }
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

async fn delete_stream(state: Arc<AppState>, name: String) -> Response {
    let existing = match state.registry.get(&state.sref(&name)).await {
        Ok(v) => v,
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    let Some(d) = existing else {
        return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found");
    };
    if !desc_alive(&d) {
        // A tombstone finishes any outstanding cleanup instead of
        // bouncing — its own debt AND any left on the ancestors by an
        // interrupted cascade. Those references keep whole generations
        // of data alive forever.
        if d.deleted
            && let Err(e) = delete_lifecycle(&state, &name).await
        {
            return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e);
        }
        return gone_or_missing(Some(&d));
    }
    match delete_lifecycle(&state, &name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e),
    }
}

/// Release one fork reference on `src`; cascades hard deletion up the
/// chain when a soft-deleted (or expired) source loses its last fork.
/// Test-only fault injection for the fork cascade. Real functions, real
/// durable writes — the point is to stop BETWEEN them rather than
/// reconstruct the intended post-crash state by hand.
#[cfg(test)]
pub(crate) mod fork_failpoints {
    //! The SEMANTIC failpoint registry (#108, first increment). Every
    //! failpoint is a typed `Fp` variant — enumerable, describable —
    //! backed by ONE state machine keyed by `(Fp, stream name)`:
    //!
    //!   armed    set by a test; a request reaching the site parks (or,
    //!            for flags, acts) while armed FOR ITS NAME.
    //!   held     the one-shot composite (`pause_oneshot`): arrival
    //!            consumes the arm and holds until released, so exactly
    //!            one request enters the window and later arms for the
    //!            same name cannot leak into it.
    //!   arrivals per-(Fp, name) — a test observes ITS request in the
    //!            window by ITS stream name. The old per-failpoint
    //!            global counters were the parallel-flake family: two
    //!            tests watching one counter woke on each other's
    //!            parks.
    //!
    //! Arming and releasing are BOTH per name, and there is
    //! deliberately no "release everything": that is what once let one
    //! test disarm another's failpoint. The narrative helpers below are
    //! one-line sugar over the typed core and double as the site
    //! contract documentation; new failpoints add a variant + a helper
    //! pair and NOTHING else.
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub enum Fp {
        // Flags (the site checks and acts; nothing parks).
        StopAfterTombstone,
        StopBeforeMarkCommitted,
        StopAfterSealIntent,
        // Parks (the site awaits release).
        CreateBeforeReady,
        AppendBeforeEnqueue,
        CloseBeforeEnqueue, // one-shot composite: consume arm, hold
        CloseBeforeMark,
        ProductSealBeforeClaim,
        ProductFinalBeforeAppend,
        ForkBeforeSourceRef,
        InitBeforeSeed,
        ForkAfterSourceRef,
        ReleaseAfterEpochCheck,
        PullBeforeReceive,
        ConsumerSagaBeforeRefresh,
        DeleteBeforeDecision,
    }

    impl Fp {
        pub const ALL: [Fp; 16] = [
            Fp::StopAfterTombstone,
            Fp::StopBeforeMarkCommitted,
            Fp::StopAfterSealIntent,
            Fp::CreateBeforeReady,
            Fp::AppendBeforeEnqueue,
            Fp::CloseBeforeEnqueue,
            Fp::CloseBeforeMark,
            Fp::ProductSealBeforeClaim,
            Fp::ProductFinalBeforeAppend,
            Fp::ForkBeforeSourceRef,
            Fp::InitBeforeSeed,
            Fp::ForkAfterSourceRef,
            Fp::ReleaseAfterEpochCheck,
            Fp::PullBeforeReceive,
            Fp::ConsumerSagaBeforeRefresh,
            Fp::DeleteBeforeDecision,
        ];

        /// The site contract: WHERE the point fires, stated as the
        /// window it opens. This is the enumerable registry the DST
        /// program audits against.
        pub fn site(self) -> &'static str {
            match self {
                Fp::StopAfterTombstone => {
                    "delete cascade: after the named generation is tombstoned \
                     and its debt recorded, before the parent ref releases"
                }
                Fp::StopBeforeMarkCommitted => {
                    "close: after the final append is durable, before \
                     mark_final_committed"
                }
                Fp::StopAfterSealIntent => {
                    "seal: after the seal intent publishes, before the \
                     committer sees it"
                }
                Fp::CreateBeforeReady => {
                    "create: after the fork reference installs, before \
                     readiness publishes"
                }
                Fp::AppendBeforeEnqueue => "append: before the committer enqueue",
                Fp::CloseBeforeEnqueue => {
                    "close: before its enqueue; ONE-SHOT — first arrival \
                     consumes the arm and holds until release"
                }
                Fp::CloseBeforeMark => {
                    "close: between the acknowledged final append and \
                     mark_final_committed"
                }
                Fp::ProductSealBeforeClaim => "product seal: before the claim CAS",
                Fp::ProductFinalBeforeAppend => {
                    "product seal: before the final-bearing append submits"
                }
                Fp::ForkBeforeSourceRef => "fork create: before the source reference installs",
                Fp::InitBeforeSeed => "create: before the tail row seeds",
                Fp::ForkAfterSourceRef => "fork create: after the source reference installs",
                Fp::ReleaseAfterEpochCheck => {
                    "fork-ref release: after the incarnation epoch check, \
                     before the release write"
                }
                Fp::PullBeforeReceive => {
                    "consumer pull: after config load, before the Receive \
                     submits"
                }
                Fp::ConsumerSagaBeforeRefresh => {
                    "consumer deletion saga: before a fan-out round's \
                     descriptor refresh"
                }
                Fp::DeleteBeforeDecision => "stream delete: before the soft-versus-hard decision",
            }
        }
    }

    #[derive(Default)]
    struct FpState {
        armed: bool,
        held: bool,
        arrivals: usize,
    }

    impl Fp {
        fn idx(self) -> usize {
            Fp::ALL.iter().position(|f| *f == self).expect("in ALL")
        }
    }

    /// Per-failpoint count of ARMED-or-HELD entries across all names —
    /// the lock-free fast path. Site checks (`hit`/`pause`) run on
    /// EVERY request in test builds; with nothing armed for a
    /// failpoint they must cost one relaxed load, not a global lock
    /// plus a key allocation (a throughput-gate test caught the
    /// difference within the suite).
    static ACTIVE: [std::sync::atomic::AtomicUsize; 16] = [
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
        std::sync::atomic::AtomicUsize::new(0),
    ];

    fn active(fp: Fp) -> bool {
        ACTIVE[fp.idx()].load(std::sync::atomic::Ordering::Acquire) > 0
    }

    fn reg() -> &'static Mutex<HashMap<(Fp, String), FpState>> {
        static M: std::sync::OnceLock<Mutex<HashMap<(Fp, String), FpState>>> =
            std::sync::OnceLock::new();
        M.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn gate() -> &'static tokio::sync::Notify {
        static N: std::sync::OnceLock<tokio::sync::Notify> = std::sync::OnceLock::new();
        N.get_or_init(tokio::sync::Notify::new)
    }

    pub fn arm(fp: Fp, name: &str) {
        let mut g = reg().lock().unwrap();
        let st = g.entry((fp, name.to_string())).or_default();
        if !st.armed && !st.held {
            ACTIVE[fp.idx()].fetch_add(1, std::sync::atomic::Ordering::Release);
        }
        st.armed = true;
    }

    pub fn release(fp: Fp, name: &str) {
        if let Some(st) = reg().lock().unwrap().get_mut(&(fp, name.to_string())) {
            if st.armed || st.held {
                ACTIVE[fp.idx()].fetch_sub(1, std::sync::atomic::Ordering::Release);
            }
            st.armed = false;
            st.held = false;
        }
        gate().notify_waiters();
    }

    /// Requests of `name` that have ARRIVED at this failpoint since the
    /// process started (monotone; per name, so parallel tests compose
    /// without watching each other's parks).
    pub fn parked(fp: Fp, name: &str) -> usize {
        reg()
            .lock()
            .unwrap()
            .get(&(fp, name.to_string()))
            .map_or(0, |st| st.arrivals)
    }

    fn is_armed(fp: Fp, name: &str) -> bool {
        reg()
            .lock()
            .unwrap()
            .get(&(fp, name.to_string()))
            .is_some_and(|st| st.armed)
    }

    fn is_held(fp: Fp, name: &str) -> bool {
        reg()
            .lock()
            .unwrap()
            .get(&(fp, name.to_string()))
            .is_some_and(|st| st.held)
    }

    /// Flag check: consume-free "is this site sabotaged for name".
    pub(crate) fn hit(fp: Fp, name: &str) -> bool {
        if !active(fp) {
            return false;
        }
        is_armed(fp, name)
    }

    /// Wait until this failpoint is released for `name`, counting the
    /// arrival exactly once so a test can OBSERVE that its request
    /// really is in the window instead of sleeping and hoping.
    pub(crate) async fn pause(fp: Fp, name: &str) {
        if !active(fp) {
            return;
        }
        let mut counted = false;
        loop {
            {
                let mut g = reg().lock().unwrap();
                let Some(st) = g.get_mut(&(fp, name.to_string())) else {
                    return;
                };
                if !st.armed {
                    return;
                }
                if !counted {
                    counted = true;
                    st.arrivals += 1;
                }
            }
            let n = gate().notified();
            if !is_armed(fp, name) {
                return;
            }
            n.await;
        }
    }

    /// One-shot park: the FIRST arrival consumes the arm and holds
    /// until released; later requests for the same name sail through.
    pub(crate) async fn pause_oneshot(fp: Fp, name: &str) {
        if !active(fp) {
            return;
        }
        {
            let mut g = reg().lock().unwrap();
            let Some(st) = g.get_mut(&(fp, name.to_string())) else {
                return;
            };
            if !st.armed {
                return;
            }
            // Armed -> Held is not a deactivation: ACTIVE keeps its
            // count until release() clears the hold.
            st.armed = false;
            st.held = true;
            st.arrivals += 1;
        }
        loop {
            if !is_held(fp, name) {
                return;
            }
            let n = gate().notified();
            if !is_held(fp, name) {
                return;
            }
            n.await;
        }
    }

    // ---- narrative sugar (the site contract, one line each) ----------

    pub fn stop_after_tombstone(name: &str) {
        arm(Fp::StopAfterTombstone, name);
    }
    pub fn stop_after_tombstone_off(name: &str) {
        release(Fp::StopAfterTombstone, name);
    }
    pub(super) fn should_stop_after_tombstone(name: &str) -> bool {
        hit(Fp::StopAfterTombstone, name)
    }
    pub fn stop_before_mark_committed(name: &str) {
        arm(Fp::StopBeforeMarkCommitted, name);
    }
    pub fn stop_before_mark_committed_off(name: &str) {
        release(Fp::StopBeforeMarkCommitted, name);
    }
    pub(super) fn should_stop_before_mark_committed(name: &str) -> bool {
        hit(Fp::StopBeforeMarkCommitted, name)
    }
    pub fn stop_after_seal_intent(name: &str) {
        arm(Fp::StopAfterSealIntent, name);
    }
    pub fn stop_after_seal_intent_off(name: &str) {
        release(Fp::StopAfterSealIntent, name);
    }
    pub(super) fn should_stop_after_seal_intent(name: &str) -> bool {
        hit(Fp::StopAfterSealIntent, name)
    }

    pub fn park_create_before_ready(name: &str) {
        arm(Fp::CreateBeforeReady, name);
    }
    pub fn release_create_before_ready(name: &str) {
        release(Fp::CreateBeforeReady, name);
    }
    pub(super) async fn pause_create_before_ready(name: &str) {
        pause(Fp::CreateBeforeReady, name).await;
    }

    pub fn park_append_before_enqueue(name: &str) {
        arm(Fp::AppendBeforeEnqueue, name);
    }
    pub fn release_append_before_enqueue(name: &str) {
        release(Fp::AppendBeforeEnqueue, name);
    }
    pub(super) async fn pause_append_before_enqueue(name: &str) {
        pause(Fp::AppendBeforeEnqueue, name).await;
    }

    pub fn park_close_before_enqueue(name: &str) {
        arm(Fp::CloseBeforeEnqueue, name);
    }
    pub fn release_close_before_enqueue(name: &str) {
        release(Fp::CloseBeforeEnqueue, name);
    }
    pub(super) async fn pause_close_before_enqueue(name: &str) {
        pause_oneshot(Fp::CloseBeforeEnqueue, name).await;
    }

    pub fn park_close_before_mark(name: &str) {
        arm(Fp::CloseBeforeMark, name);
    }
    pub fn release_close_before_mark(name: &str) {
        release(Fp::CloseBeforeMark, name);
    }
    pub(super) async fn pause_close_before_mark(name: &str) {
        pause(Fp::CloseBeforeMark, name).await;
    }

    pub fn park_product_seal_before_claim(name: &str) {
        arm(Fp::ProductSealBeforeClaim, name);
    }
    pub fn release_product_seal_before_claim(name: &str) {
        release(Fp::ProductSealBeforeClaim, name);
    }
    pub async fn pause_product_seal_before_claim(name: &str) {
        pause(Fp::ProductSealBeforeClaim, name).await;
    }

    pub fn park_product_final_before_append(name: &str) {
        arm(Fp::ProductFinalBeforeAppend, name);
    }
    pub fn release_product_final_before_append(name: &str) {
        release(Fp::ProductFinalBeforeAppend, name);
    }
    pub async fn pause_product_final_before_append(name: &str) {
        pause(Fp::ProductFinalBeforeAppend, name).await;
    }

    pub fn park_fork_before_source_ref(name: &str) {
        arm(Fp::ForkBeforeSourceRef, name);
    }
    pub fn release_fork_before_source_ref(name: &str) {
        release(Fp::ForkBeforeSourceRef, name);
    }
    pub(super) async fn pause_fork_before_source_ref(name: &str) {
        pause(Fp::ForkBeforeSourceRef, name).await;
    }

    pub fn park_init_before_seed(name: &str) {
        arm(Fp::InitBeforeSeed, name);
    }
    pub fn release_init_before_seed(name: &str) {
        release(Fp::InitBeforeSeed, name);
    }
    pub(super) async fn pause_init_before_seed(name: &str) {
        pause(Fp::InitBeforeSeed, name).await;
    }

    pub fn park_fork_after_source_ref(name: &str) {
        arm(Fp::ForkAfterSourceRef, name);
    }
    pub fn release_fork_after_source_ref(name: &str) {
        release(Fp::ForkAfterSourceRef, name);
    }
    pub(super) async fn pause_fork_after_source_ref(name: &str) {
        pause(Fp::ForkAfterSourceRef, name).await;
    }

    pub fn park_release_after_epoch_check(name: &str) {
        arm(Fp::ReleaseAfterEpochCheck, name);
    }
    pub fn release_release_after_epoch_check(name: &str) {
        release(Fp::ReleaseAfterEpochCheck, name);
    }
    pub(super) async fn pause_release_after_epoch_check(name: &str) {
        pause(Fp::ReleaseAfterEpochCheck, name).await;
    }

    pub fn park_pull_before_receive(name: &str) {
        arm(Fp::PullBeforeReceive, name);
    }
    pub fn release_pull_before_receive(name: &str) {
        release(Fp::PullBeforeReceive, name);
    }
    pub(crate) async fn pause_pull_before_receive(name: &str) {
        pause(Fp::PullBeforeReceive, name).await;
    }

    pub fn park_consumer_saga_before_refresh(name: &str) {
        arm(Fp::ConsumerSagaBeforeRefresh, name);
    }
    pub fn release_consumer_saga_before_refresh(name: &str) {
        release(Fp::ConsumerSagaBeforeRefresh, name);
    }
    pub(crate) async fn pause_consumer_saga_before_refresh(name: &str) {
        pause(Fp::ConsumerSagaBeforeRefresh, name).await;
    }

    pub fn park_delete_before_decision(name: &str) {
        arm(Fp::DeleteBeforeDecision, name);
    }
    pub fn release_delete_before_decision(name: &str) {
        release(Fp::DeleteBeforeDecision, name);
    }
    pub(super) async fn pause_delete_before_decision(name: &str) {
        pause(Fp::DeleteBeforeDecision, name).await;
    }
}

/// Release one fork reference. Returns whether the release is
/// CONCLUSIVE: the reference was actually removed, or the source is
/// beyond caring (gone or hard-deleted). An ABSENT reference on a
/// LIVE source is NOT conclusive — the child's creator may still be
/// in flight between its stamp and its install, and clearing the
/// child's debt on that momentary absence is how a crashed creator
/// orphaned a reference forever (FRK-013 crash hole): the install
/// landed after the debt was cleared, and nothing could ever repair
/// it. Callers clear debt only on a conclusive release; retained debt
/// is settled by any later tombstone DELETE, which retries this
/// release and finds the late-installed reference.
#[cfg(test)]
pub(crate) async fn release_fork_ref_for_test(
    state: &Arc<AppState>,
    src: &str,
    fork_id: &str,
    expect_source_epoch: &str,
) -> Result<bool, String> {
    release_fork_ref(state, src, fork_id, expect_source_epoch).await
}

fn release_fork_ref(
    state: &Arc<AppState>,
    src: &str,
    fork_id: &str,
    expect_source_epoch: &str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + Send>> {
    let state = state.clone();
    let src = src.to_string();
    let fork_id = fork_id.to_string();
    let expect_source_epoch = expect_source_epoch.to_string();
    Box::pin(async move {
        // Incarnation fence (round 14): the reference belongs to the
        // source INCARNATION the child forked. A delayed cleanup can
        // run after that name was deleted and recreated; releasing
        // against — or worse, evaluating expiry/soft-delete lifecycle
        // conditions against — the REPLACEMENT would corrupt a stream
        // this fork never touched. A mismatch means the original
        // source is gone: nothing to release, nothing pinned, so the
        // release is conclusive. An empty expectation opts out (the
        // recursive ancestor settle, which already re-reads each hop).
        // ONE descriptor snapshot for the WHOLE operation. Every
        // decision below — the tombstone-debt settle, the reference
        // CAS, the debt clear — binds to THIS snapshot's incarnation.
        // Re-reading the epoch later would re-bind a stale cleanup to
        // whatever incarnation holds the name at that instant (round
        // 15: check A, name recreated as B, mutation fenced to B —
        // legitimately fenced, wrong identity).
        let cur = state
            .registry
            .get(&state.sref(&src))
            .await
            .map_err(|e| e.to_string())?;
        let Some(cur) = cur else {
            // Source gone entirely: nothing to release, ever.
            return Ok(true);
        };
        if !expect_source_epoch.is_empty() && cur.stream_epoch != expect_source_epoch {
            // The incarnation this release was owed to is gone.
            return Ok(true);
        }
        let source_epoch = cur.stream_epoch.clone();
        #[cfg(test)]
        fork_failpoints::pause_release_after_epoch_check(&src).await;
        // Release BY ID: a retried delete removes an id that is already
        // gone (a no-op), where an anonymous decrement would have
        // double-released and freed a still-live fork's data.
        // If the source is ALREADY a tombstone that still owes its own
        // parent, settle that debt first. Otherwise retrying the
        // original delete looks successful while an ancestor stays
        // pinned forever: the CAS below refuses on a deleted descriptor,
        // this function returns Ok, and the caller clears its own flag.
        // Only the hidden intermediate name could repair it, which no
        // ordinary client knows to ask for.
        if cur.deleted {
            if cur.parent_ref_pending
                && let Some(gp) = cur.forked_from.as_ref()
                && release_fork_ref(&state, &gp.source, &gp.fork_id, &gp.source_epoch).await?
            {
                clear_parent_debt(&state, &src, &source_epoch, Some((&gp.source, &gp.fork_id)))
                    .await?;
            }
            // A hard-deleted source holds no live references.
            return Ok(true);
        }
        // Release the reference AND decide the source's fate in one
        // CAS, against the children it has at that instant. Splitting
        // the two let a new fork install itself in between and then be
        // orphaned by an unconditional tombstone.
        //
        // Expressed through the TYPED mutation API: `decide` is pure
        // over an immutable descriptor and RETURNS its verdict, so the
        // stale-flag-across-retries hazard (round 14) is structurally
        // impossible — there are no out-parameters to leak from a lost
        // attempt. The verdict is `(removed_ref, tombstoned)`.
        let outcome = state
            .registry
            .mutate_incarnation(&state.sref(&src), &source_epoch, |x| {
                let before = x.fork_children.len();
                let mut next = x.clone();
                next.fork_children.retain(|c| c != &fork_id);
                let removed = next.fork_children.len() != before;
                let expired = next.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false);
                let should_tombstone = next.fork_children.is_empty()
                    && (next.soft_deleted || expired)
                    && !next.deleted;
                if should_tombstone {
                    next.soft_deleted = false;
                    next.deleted = true;
                    // Round-22 item 7: the closure debt rides the SAME
                    // registry write — the billing clock stops here.
                    next.logical_close_ms = Some(crate::billing::billing_now_ms());
                    next.parent_ref_pending = next.forked_from.is_some();
                }
                if removed || should_tombstone {
                    crate::registry::Mutation::Write(next, (removed, should_tombstone))
                } else {
                    crate::registry::Mutation::Decline((false, false))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        let (removed_ref, tombstoned) = match outcome {
            crate::registry::MutationResult::Applied(v)
            | crate::registry::MutationResult::Declined(v) => v,
            // Source gone or recreated between our snapshot and the
            // mutation: the incarnation this release was owed to no
            // longer exists — CONCLUSIVE, same verdict as the epoch
            // check at the top, so recreated-source debt converges.
            crate::registry::MutationResult::Missing
            | crate::registry::MutationResult::IncarnationChanged => {
                state.registry.invalidate(&state.sref(&src));
                return Ok(true);
            }
        };
        state.registry.invalidate(&state.sref(&src));
        #[cfg(test)]
        if tombstoned && fork_failpoints::should_stop_after_tombstone(&src) {
            // "Crash" here: the tombstone and its debt are durable, the
            // recursive release has not run.
            return Ok(removed_ref);
        }
        if tombstoned {
            // The tombstone we JUST wrote — same incarnation (a
            // tombstone keeps its epoch), so the debt clear stays
            // fenced to it. The grandparent release carries the
            // ForkRef's own recorded source epoch.
            if let Some(after) = state
                .registry
                .get(&state.sref(&src))
                .await
                .map_err(|e| e.to_string())?
                && after.stream_epoch == source_epoch
                && let Some(gf) = after.forked_from.as_ref()
                && release_fork_ref(&state, &gf.source, &gf.fork_id, &gf.source_epoch).await?
            {
                clear_parent_debt(&state, &src, &source_epoch, Some((&gf.source, &gf.fork_id)))
                    .await?;
            }
        }
        Ok(removed_ref)
    })
}

/// The pinned fork delete lifecycle: a stream with live forks
/// SOFT-deletes (data retained, direct access 410, name blocked); a
/// fork's deletion releases its source reference, and a soft-deleted
/// source whose last reference drops cascades to a hard delete —
/// recursively up the chain.
fn delete_lifecycle(
    state: &Arc<AppState>,
    name: &str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
    let state = state.clone();
    let name = name.to_string();
    Box::pin(async move {
        let d = match state
            .registry
            .get(&state.sref(&name))
            .await
            .map_err(|e| e.to_string())?
        {
            Some(d) => d,
            None => return Ok(()),
        };
        let parent = d
            .forked_from
            .as_ref()
            .map(|f| (f.source.clone(), f.fork_id.clone(), f.source_epoch.clone()));
        // Already a tombstone with an unpaid debt: just pay it. This is
        // also how a crashed CASCADE is repaired — an intermediate
        // generation that was tombstoned but never released its own
        // parent is reachable by deleting it again.
        if d.deleted {
            if d.parent_ref_pending
                && let Some((src, fid, sep)) = parent.clone()
            {
                // Clear the debt only on a CONCLUSIVE release: an
                // absent reference on a live source may still be
                // installed by a creator in flight, and this very
                // retry is what repairs that crash later. A source
                // recreated since the fork is conclusive too — the
                // incarnation this debt was owed to is gone.
                if release_fork_ref(&state, &src, &fid, &sep).await? {
                    clear_parent_debt(&state, &name, &d.stream_epoch, Some((&src, &fid))).await?;
                }
            }
            // Then walk UP. A crashed cascade leaves the debt on a
            // hidden intermediate generation, and the only request a
            // client will ever retry is the original delete of the leaf.
            // Repairing only this descriptor left the ancestor pinned
            // and reported success.
            let mut next = d.forked_from.clone();
            for _ in 0..64 {
                let Some(f) = next else { break };
                let Some(anc) = state
                    .registry
                    .get(&state.sref(&f.source))
                    .await
                    .map_err(|e| e.to_string())?
                else {
                    break;
                };
                if !(anc.deleted && anc.parent_ref_pending) {
                    break;
                }
                let conclusive = match anc.forked_from.as_ref() {
                    Some(gp) => {
                        release_fork_ref(&state, &gp.source, &gp.fork_id, &gp.source_epoch).await?
                    }
                    None => true,
                };
                if conclusive {
                    let debt = anc
                        .forked_from
                        .as_ref()
                        .map(|gp| (gp.source.clone(), gp.fork_id.clone()));
                    clear_parent_debt(
                        &state,
                        &f.source,
                        &anc.stream_epoch,
                        debt.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
                    )
                    .await?;
                }
                next = anc.forked_from.clone();
            }
            return Ok(());
        }
        // Soft-versus-hard is decided INSIDE the CAS, against the
        // children the descriptor has at that instant. Deciding it from
        // an earlier read raced fork creation: a concurrent first fork
        // could install its reference between the read and the write,
        // and the unconditional update tombstoned the source anyway —
        // leaving a live fork anchored to a hard-deleted parent.
        //
        // The debt is recorded in the SAME write as the tombstone, so a
        // crash between them is impossible.
        #[cfg(test)]
        fork_failpoints::pause_delete_before_decision(&name).await;
        let mut hard_deleted = false;
        let epoch = d.stream_epoch.clone();
        // Round-22 item 7: ONE logical close instant, decided here,
        // stamped into the tombstone write below and used by every
        // closure submission — however late a retry lands, it accounts
        // to THIS time.
        let close_stamp = crate::billing::billing_now_ms();
        state
            .registry
            .cas_update_incarnation(&state.sref(&name), &epoch, |x| {
                if x.deleted {
                    return false;
                }
                if !x.fork_children.is_empty() {
                    x.soft_deleted = true;
                    hard_deleted = false;
                } else {
                    x.deleted = true;
                    x.logical_close_ms = Some(close_stamp);
                    x.parent_ref_pending = x.forked_from.is_some();
                    hard_deleted = true;
                }
                true
            })
            .await
            .map_err(|e| e.to_string())?;
        state.registry.invalidate(&state.sref(&name));
        if !hard_deleted {
            return Ok(());
        }
        // Ops journal (§12.3): the lifecycle transition, id'd by the
        // incarnation — a retried delete re-emits the same id and the
        // rollup deduplicates.
        crate::ops::emit(
            crate::ops::OpsEvent::new(
                "stream_hard_deleted",
                format!("life/{}/hard_deleted", d.stream_epoch),
            )
            .stream(&d.stream_epoch, &name),
        );
        // Billing closure (§6.2): the hard delete is the terminal
        // storage observation — advance every segment's storage clock
        // to the persisted close stamp, zero its gauge, mark dirty for
        // the ledger. Submission is AWAITED (round-22 item 7): a full
        // committer queue is backpressure, never a silent drop; a
        // submission that still fails is safe because the debt lives
        // on the tombstone and the sweep reconciler retries it.
        {
            let seg_ids: Vec<u32> = d
                .segments
                .as_ref()
                .map(|m| m.segments.iter().map(|sg| sg.seg_id).collect())
                .unwrap_or_else(|| vec![0]);
            for sid in seg_ids {
                let identity = d.dynamic_segment_identity(sid);
                let route = d.segment_route_by_id(sid);
                if let Ok(engine) = state.engine_for(&route).await
                    && let Err(e) = engine.submit_billing_close(identity, close_stamp).await
                {
                    tracing::warn!(
                        "delete {name}: billing close submit failed \
                             (tombstone debt persists; sweep retries): {e}"
                    );
                }
            }
        }
        if let Some((src, fid, sep)) = parent {
            // Released CONCLUSIVELY: the tombstone owes nothing more.
            // This is `update`, not `cas_update`, because the
            // descriptor is already deleted and CAS refuses tombstones
            // by design — which is exactly why the debt has to be
            // recorded ON the tombstone and cleared this way. An
            // absent-on-live-source release keeps the debt: the
            // child's creator may still install the reference, and the
            // next DELETE of this tombstone retries and removes it.
            if release_fork_ref(&state, &src, &fid, &sep).await? {
                clear_parent_debt(&state, &name, &epoch, Some((&src, &fid))).await?;
            }
        }
        Ok(())
    })
}

// ---- state-protocol touch surface (collapsible GET-per-key model) ----

fn parse_ts_hint(headers: &HeaderMap) -> Option<i64> {
    let raw = headers.get("stream-timestamp")?.to_str().ok()?;
    if let Ok(n) = raw.parse::<i64>() {
        return Some(n / 1_000_000); // unix nanos
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.timestamp_millis())
}

/// ROUTING-V3 sealed-segment retry wrapper: post-split streams (a
/// materialized map with successors or an in-flight transition) buffer
/// the body and retry a stream-closed response after refreshing the
/// descriptor and resuming any pending transition — a seal is a few ms
/// of routing indirection, never a client-visible 409. A 409 whose
/// freshly-refreshed map shows the resolved segment LIVE with no
/// pending transition is a genuine user-closed stream and passes
/// through. Pre-split streams (segments: None — the common case) take
/// the core path directly with zero overhead.
pub(crate) async fn append(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Body,
    product_hash: Option<[u8; 16]>,
    product_key: Option<String>,
    // TRUSTED, internal only: this call is the final record a seal
    // intent owes, identified by its operation id. Never derived from a
    // request header — see the `x-seal-final` refusal in append_core.
    seal_auth: Option<SealAuthz>,
) -> Response {
    let wrapped = matches!(
        state.registry.get(&state.sref(&name)).await,
        Ok(Some(d)) if d
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
    );
    if !wrapped {
        return append_core(
            state,
            name,
            headers,
            body,
            product_hash,
            product_key,
            seal_auth,
        )
        .await;
    }
    let body_bytes = match axum::body::to_bytes(body, max_body_bytes()).await {
        Ok(b) => b,
        Err(_) => return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large"),
    };
    for attempt in 0..4u32 {
        let r = append_core(
            state.clone(),
            name.clone(),
            headers.clone(),
            Body::from(body_bytes.clone()),
            product_hash,
            product_key.clone(),
            seal_auth.clone(),
        )
        .await;
        if !(r.status() == StatusCode::CONFLICT && r.headers().contains_key("stream-closed")) {
            return r;
        }
        state.registry.invalidate(&state.sref(&name));
        let Ok(Some(d)) = state.registry.get(&state.sref(&name)).await else {
            return r;
        };
        let rk = product_key.clone().unwrap_or_default();
        let seg = d.resolve_segment(&rk);
        let pending = d.segments.as_ref().is_some_and(|m| m.pending.is_some());
        if pending {
            crate::scaler3::resume(&state, &name).await;
        } else if !seg.sealed {
            // Live segment, no transition: the stream really is closed.
            return r;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10 * (attempt as u64 + 1))).await;
    }
    err_resp(
        StatusCode::SERVICE_UNAVAILABLE,
        "segment_transition",
        "segment map transition did not converge; retry",
    )
}

/// What a refused FINAL append does to the seal intent it belongs to
/// — ONE policy, shared verbatim by the raw and product surfaces
/// (they previously kept separate stringly-typed lists, which drifted:
/// the product list named codes its own translator never produces, so
/// stale-epoch was "retained" in the comment and definitive in fact).
///
/// After round 11 every one of these verdicts is durability-barriered,
/// and after round 8 every claim is generation-fenced — so releasing a
/// definitively-refused generation's intent can never destroy a
/// concurrent exact retry (the retry renewed to a newer generation the
/// release cannot name).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FinalDisposition {
    /// About the moment or the ordering, not the request: the exact
    /// retry can still succeed, so the intent stays. Producer gaps
    /// (the predecessor may already be inside the server) and
    /// epoch-must-start-at-zero (the producer's epoch can advance and
    /// make this sequence meaningful) are ordering; timeouts,
    /// throttles, write failures and ownership moves are the moment.
    AmbiguousOrTransient,
    /// About THIS request, forever: epochs never decrease (stale),
    /// bodies and content types do not change on retry, a reused or
    /// conflicting sequence row is durable, and a segment closed by
    /// another operation stays closed. The uncommitted intent comes
    /// down NOW — retaining it held the collection Sealing behind a
    /// promise that could never be delivered, renewable indefinitely
    /// by the very request that can never deliver it.
    DefinitivelyRejected,
}

pub(crate) fn final_err_disposition(e: &crate::shard::AppendErr) -> FinalDisposition {
    use crate::shard::AppendErr::*;
    match e {
        ProducerGap { .. } | ProducerEpochSeq => FinalDisposition::AmbiguousOrTransient,
        ProducerStale { .. }
        | ProducerSeqReused
        | CtMismatch
        | BadBody(_)
        | SeqConflict { .. }
        | Closed { .. }
        | SealSuperseded => FinalDisposition::DefinitivelyRejected,
        _ => FinalDisposition::AmbiguousOrTransient,
    }
}

/// The same policy over the PRODUCT surface's translated wire codes —
/// the product handler holds a translated Response, not the AppendErr.
/// The names here are the translator's OUTPUT names, asserted by the
/// stale-epoch regression on both surfaces.
pub(crate) fn final_code_disposition(status: StatusCode, code: Option<&str>) -> FinalDisposition {
    if !status.is_client_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
    {
        return FinalDisposition::AmbiguousOrTransient;
    }
    match code {
        // No readable code — an unreadable/absent error body — is
        // UNKNOWN, and unknown keeps the intent (matching
        // take_error_code's own contract). Only a NAMED verdict about
        // the request is definitive; ordering codes stay ambiguous.
        None => FinalDisposition::AmbiguousOrTransient,
        Some("producer_gap") | Some("producer_epoch_must_start_at_zero") => {
            FinalDisposition::AmbiguousOrTransient
        }
        Some(_) => FinalDisposition::DefinitivelyRejected,
    }
}

/// The TRUSTED execution token a product seal's final append carries:
/// the operation, the claim generation, and the incarnation the whole
/// seal was validated against. The append refuses to run unless the
/// CURRENT descriptor still matches all three — an epoch-less token
/// let a seal claimed on incarnation A write its final record into
/// (and physically close a segment of) a same-name, same-key
/// replacement created while the request was in flight.
#[derive(Debug, Clone)]
pub(crate) struct SealAuthz {
    pub op_id: String,
    pub generation: u64,
    pub epoch: String,
}

/// Raise the seal fence on the segment a routing key resolves to, and
/// report whether that segment is closed. The message travels the same
/// queue as appends, so the committer answers it only after deciding
/// every append enqueued before it — the reply is a BARRIER: after a
/// `false`, no append below the fence can ever close the segment; a
/// `true` means the old operation's close already committed.
pub(crate) async fn fence_segment_for_key(
    state: &Arc<AppState>,
    name: &str,
    expect_epoch: &str,
    routing_key: &str,
    fence_to: u64,
) -> Result<bool, String> {
    state.registry.invalidate(&state.sref(name));
    let desc = match state.registry.get(&state.sref(name)).await {
        Ok(Some(d)) if d.stream_epoch == expect_epoch => d,
        Ok(_) => return Err("the collection this seal was issued against no longer exists".into()),
        Err(e) => return Err(e.to_string()),
    };
    let seg = desc.resolve_segment(routing_key);
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let route = desc.segment_route_by_id(seg.seg_id);
    let engine = state
        .engine_for(&route)
        .await
        .map_err(|_| "segment engine unavailable".to_string())?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash: identity,
        route,
        entries: Vec::new(),
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: [0u8; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        close: false,
        seal_gen: None,
        seal_fence_to: Some(fence_to),
        producer: None,
        deferred_error: None,
        sealed_reject_new: None,
        touch: None,
        usage: crate::usage::counters(&route),
        billing: None,
        resp: tx,
    };
    engine
        .try_enqueue(req)
        .map_err(|_| "append queue full; fence not placed".to_string())?;
    match rx.await {
        Ok(Ok(ack)) => Ok(ack.closed),
        Ok(Err(e)) => Err(format!("fence refused: {e:?}")),
        Err(_) => Err("fence dropped".into()),
    }
}

async fn append_core(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Body,
    product_hash: Option<[u8; 16]>,
    product_key: Option<String>,
    seal_auth: Option<SealAuthz>,
) -> Response {
    // Scaled-stream routing (SCALING.md): a parent stream with scaling on
    // never takes appends itself — the routing key maps through the
    // segment map to an internal child stream "<parent>#<seg_id>". The
    // child is sealed (closed) during a split/merge transition; the retry
    // loop refreshes the map and follows the successor, so clients never
    // observe the transition beyond a few ms of latency.
    // (LEGACY path, pre-v3 descriptors only; unified-model streams
    // resolve segments in-process below — docs/ROUTING-V3.md §2.)
    let mut desc = match state.registry.get(&state.sref(&name)).await {
        Ok(Some(d)) if desc_alive(&d) && initializing(&d) => {
            return creating_resp();
        }
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(d) => {
            return gone_or_missing(d.as_ref());
        }
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    // Capacity admission is PER SEGMENT (review blocker 1: after a
    // split, each child must get its own inflight budget — a shared
    // per-stream bucket would cap the pair at one segment's capacity).
    // Contractual accounting (usage counters, admit_append) stays keyed
    // by the stream name below. The slot is acquired after segment
    // resolution, further down.
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

    let mut producer = match parse_producer(&headers) {
        Ok(p) => p,
        Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_producer", &m),
    };
    if let (Some(p), Some(h)) = (producer.as_mut(), product_hash) {
        p.request_hash = Some(h);
    }
    let close = want_close(&headers);
    // R25-E: oversized-body refusal, now AFTER bearer + stream-key auth.
    // Answering mid-upload closes the connection and the edge reports
    // 502 (measured in Singapore: 2 MiB vs a 1 MiB ceiling), so we drain
    // a BOUNDED amount before answering — but only for callers who have
    // already authenticated; an unauthenticated caller got its 401 above
    // without the server reading a byte.
    if let Some(declared) = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        && declared > max_body_bytes()
    {
        const DRAIN_CAP: usize = 8 * 1024 * 1024;
        if declared <= DRAIN_CAP {
            use futures_util::StreamExt;
            let mut stream = body.into_data_stream();
            let mut seen = 0usize;
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(b) => {
                        seen += b.len();
                        if seen > DRAIN_CAP {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }
        return err_resp(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            &format!(
                "request body {} exceeds the {}-byte limit",
                declared,
                max_body_bytes()
            ),
        );
    }
    // R25-E: RSS write-shed, moved from pre-auth middleware. Writes
    // only — reads don't grow memtables, and shedding them would hide
    // the instance from its own operators. The guard considers sampled
    // RSS PLUS reserved absorber bytes so the line moves BEFORE the
    // memory does.
    if state.admit_rss_shed_mb > 0
        && crate::history::memory_pressure_mb(
            state
                .rss_mb_cached
                .load(std::sync::atomic::Ordering::Relaxed),
            crate::history::absorb_reserved_bytes(),
        ) > state.admit_rss_shed_mb
    {
        state
            .admit_shed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let mut r = err_resp(
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "instance memory pressure; retry",
        );
        r.headers_mut()
            .insert("retry-after", axum::http::HeaderValue::from_static("2"));
        return r;
    }
    let body = match axum::body::to_bytes(body, max_body_bytes()).await {
        Ok(b) => b,
        Err(_) => return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large"),
    };
    let close_only = close && body.is_empty();
    // This request's own seal identity: content hash + routing key, the
    // same envelope the intent stores. An exact retry of a crashed raw
    // close therefore recognises ITSELF as the owed final — no private
    // header, no producer opt-in required.
    // The identity of THIS close: the whole semantic request, not just
    // its payload. Two closes with the same body and routing key but
    // different producer coordination are different operations — sharing
    // one id let a request that was refused tear down the intent another
    // one owned, and the promised final record was lost.
    let this_close_op = {
        let hv = |h: &str| hdr(&headers, h).unwrap_or_default();
        // EVERY input that can change what the committer persists or
        // how it rules is part of the identity. The content type is
        // the REQUEST's own header — hashing the descriptor's
        // configured type let a close with the wrong type share the
        // valid close's identity, join its intent, collect the
        // deferred ct-mismatch verdict, and tear down an intent that
        // was never its own. Key version likewise: it is stored in the
        // frame, so two closes differing only there are different
        // operations.
        crate::product::seal_op_id_semantic(
            &create_request_hash(&desc.content_type, None, None, true, &body, None),
            &product_key.clone().unwrap_or_default(),
            &[
                hv("producer-id"),
                hv("producer-epoch"),
                hv("producer-seq"),
                hv("stream-seq"),
                hv("stream-timestamp"),
                hv("content-type"),
                hv("stream-key-version"),
            ],
        )
    };
    // Owed-final authorization: either this request IS the intent's
    // record (computed identity matches) or an internal caller passed
    // the trusted operation id. Nothing a client sends can assert it.
    // A TRUSTED product final proves its whole execution token before
    // anything else: same incarnation, same claim, same generation. It
    // owns a typed transition — if the token no longer matches (the
    // collection was deleted and recreated, or the claim was taken
    // over), the append must not run at all: not write the record, not
    // close a segment, and never fall through into the raw-close claim
    // path on a stranger's descriptor.
    if let Some(auth) = &seal_auth {
        let holds = desc.stream_epoch == auth.epoch
            && desc.sealing.as_ref().is_some_and(|sl| {
                sl.operation_id == auth.op_id && sl.claim_generation == auth.generation
            });
        if !holds {
            return err_resp(
                StatusCode::CONFLICT,
                "seal_superseded",
                "the seal this final record belongs to no longer holds its claim",
            );
        }
    }
    let is_owed_final = desc.sealing.as_ref().is_some_and(|sl| {
        sl.owes_final()
            && (sl.operation_id == this_close_op
                || Some(sl.operation_id.as_str()) == seal_auth.as_ref().map(|a| a.op_id.as_str()))
    });
    // The generation this request's claim-authorized writes will carry.
    // Filled by whichever path holds the claim: the trusted internal
    // seal (its ticket), an owed-final resume (renewed below), or a
    // fresh close (begin_sealing_for_close's install).
    let mut raw_seal_gen: Option<u64> = seal_auth.as_ref().map(|a| a.generation);
    if is_owed_final && seal_auth.is_none() {
        // RESUME of a crashed close: renew the claim before appending.
        // Renewal re-allocates the generation, so the resume can never
        // be fenced out by a takeover reservation that aborted after
        // this operation's original attempt.
        match crate::product::renew_owed_claim(&state, &name, &this_close_op, &desc.stream_epoch)
            .await
        {
            Ok(Some(g)) => raw_seal_gen = Some(g),
            Ok(None) => {
                // The claim moved between the descriptor read and the
                // renewal: whoever holds it now decides. Answer as a
                // conflict rather than write under a claim we lost.
                return err_resp(
                    StatusCode::CONFLICT,
                    "sealed",
                    "the seal this close was resuming has been superseded",
                );
            }
            Err(e) => return err_resp(StatusCode::SERVICE_UNAVAILABLE, "internal", &e),
        }
    }

    // A raw close that carries content and brings no producer of its own
    // gets a SYNTHETIC one, derived from its operation identity. Without
    // it the second crash boundary is unrecoverable: once the records
    // are durable and the segment is closed, an exact retry reaches the
    // committer's closed-stream check (which only forgives an empty
    // close-only) and is refused — so `final_committed` is never
    // written and the collection stays Sealing over records it already
    // holds. With it, the retry is recognised as a duplicate BEFORE the
    // closed check, and can finish the transition.
    let synthetic_producer = close && !body.is_empty() && producer.is_none();
    if synthetic_producer {
        producer = Some(crate::shard::ProducerReq {
            id: format!(
                "{}rawseal.{this_close_op}",
                crate::shard::INTERNAL_PRODUCER_PREFIX
            ),
            epoch: 1,
            seq: 0,
            request_hash: None,
        });
    }

    // Collection lifecycle (audit P0): the DESCRIPTOR is authoritative,
    // so a sealed collection refuses NEW records even when a segment
    // engine has not observed its close yet. Requests whose pinned
    // answer is idempotent success — close-only retries and producer
    // requests, whose duplicate check must still return 204 — are
    // deferred to the committer, which owns that decision and answers
    // 409 with Stream-Next-Offset when they are genuinely new writes.
    // A producer request rides through so the committer can recognise a
    // retry and answer it with its original result — but it carries the
    // refusal with it, and the committer applies it to anything that
    // turns out to be a NEW sequence. Without that, a novel producer
    // write was accepted while the descriptor said Sealing or Sealed.
    // The seal's OWN final record is the one write a Sealing collection
    // still owes. Its identity is COMPUTED from this request — content
    // hash and routing key — and compared with the durable intent.
    //
    // It used to be asserted by an `x-seal-final` request header, which
    // was wrong twice over: any caller could send it (knowing the id was
    // enough to smuggle an arbitrary record into a sealing collection),
    // and no ordinary client sends it, so an exact retry after a crash
    // was rejected as a new write and the collection stayed stuck owing
    // a record nobody could deliver.
    if headers.contains_key("x-seal-final") {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "unknown_field",
            "x-seal-final is not a request header",
        );
    }
    let sealed_reject_new =
        if (desc.sealed || desc.sealing.is_some()) && !close_only && !is_owed_final {
            Some(if desc.sealed {
                crate::shard::SealedReject::Sealed
            } else {
                crate::shard::SealedReject::Sealing
            })
        } else {
            None
        };
    if sealed_reject_new.is_some() && producer.is_none() {
        // The pinned closure contract requires Stream-Next-Offset on
        // the 409, so read the sealed tail before answering.
        let seg0 = desc.resolve_segment("");
        let next = match state.engine_for(&seg0.shard_route).await {
            Ok(e) => match e.stream_handle(seg0.identity).await {
                Ok(h) => h.state.lock().unwrap().durable.next,
                Err(_) => 0,
            },
            Err(_) => 0,
        };
        let mut r = err_resp(StatusCode::CONFLICT, "stream_closed", "stream is closed");
        r.headers_mut().insert(
            "stream-closed",
            axum::http::HeaderValue::from_static("true"),
        );
        if let Ok(v) = axum::http::HeaderValue::from_str(&tail_token(next)) {
            r.headers_mut().insert("stream-next-offset", v);
        }
        return r;
    }

    // A raw close seals the whole COLLECTION, so the intent has to be
    // durable before any physical segment closes. Publishing it
    // afterwards left a window where other routing keys' segments were
    // still writable while this one was already closed, and a failure
    // in between produced a permanently split-brained collection that
    // still answered the close with success.

    // (the raw close intent is published further down, once every
    // deterministic error has been ruled out — see `close_intent`)

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

    let close_carries_content = !entries.is_empty();
    // A body larger than the ingest bucket's CAPACITY can never be
    // admitted — that is a permanent 413, and it must be decided BEFORE
    // the lifecycle intent, or the collection is left sealing forever
    // owing a record the limiter will always refuse.
    if close && close_carries_content && deferred.is_none() {
        // Bytes AND records: a batched close with more records than the
        // record bucket can ever hold is just as permanently refused as
        // an oversized body, and publishing an intent for it stranded
        // the collection at 429 forever.
        if let Some(kind) =
            crate::usage::permanently_unadmittable(body.len() as u64, entries.len() as u64)
        {
            return err_resp(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                &format!("request exceeds the per-stream ingest {kind} capacity"),
            );
        }
    }
    // The raw close publishes its lifecycle intent HERE: after content
    // type, body parsing and every other deterministic refusal, so a
    // request that answers 400 can never leave the collection stuck in
    // Sealing owing a record nobody will write.
    //
    // A close that CARRIES CONTENT is a final-bearing seal, exactly like
    // the product's seal-with-final: the promise is "these records, then
    // closed". Publishing Empty for it meant a crash after the intent
    // let a later close-only finish the seal without them.
    if close && !desc.sealed && !is_owed_final && deferred.is_none() && seal_auth.is_none() {
        let intent = if entries.is_empty() {
            crate::registry::SealIntent::Empty
        } else {
            crate::registry::SealIntent::Final {
                routing_key: product_key.clone().unwrap_or_default(),
                // THE operation id — the same semantic identity the
                // append computes for itself, so a retry recognises its
                // own intent and nothing else can claim it.
                request_hash: this_close_op.clone(),
                final_committed: false,
            }
        };
        match crate::product::begin_sealing_for_close(&state, &name, intent, &desc.stream_epoch)
            .await
        {
            Ok(g) => {
                if let Some(g) = g {
                    raw_seal_gen = Some(g);
                }
            }
            Err(e) => return err_resp(StatusCode::CONFLICT, "sealed", &e),
        }
        #[cfg(test)]
        if fork_failpoints::should_stop_after_seal_intent(&name) {
            // The crash boundary: intent durable, records not written.
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "failpoint",
                "stopped after the seal intent",
            );
        }
    }

    // Per-shard service limits (usage.rs): token buckets over request rate,
    // record rate, and ingest bytes. Reject-whole with the limit named.
    // Admission also picks THE counters object for this request — one Arc
    // carried through both count sites (here and the committer), so a
    // concurrent eviction/promotion can never split one request's
    // accounting across two objects (review round 4).
    let name_hash = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
    let usage_c = if !close_only && deferred.is_none() {
        match crate::usage::admit_append(&name_hash, body.len() as u64, entries.len() as u64) {
            Err(hit) => {
                crate::usage::note_limit_refusal(&hit);
                let l = crate::usage::limits();
                if matches!(hit, crate::usage::LimitHit::Bytes { .. })
                    && body.len() as f64 > l.bytes_per_sec * l.burst_secs
                {
                    // Larger than the bucket's CAPACITY: no retry can
                    // ever admit it — that is 413, not 429.
                    return err_resp(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "payload_too_large",
                        "request exceeds the per-stream ingest capacity",
                    );
                }
                let mut r = err_resp(StatusCode::TOO_MANY_REQUESTS, hit.code(), &hit.message());
                if let Ok(v) = axum::http::HeaderValue::from_str(&format!(
                    "{}",
                    hit.retry_ms().div_ceil(1000).max(1)
                )) {
                    r.headers_mut().insert("retry-after", v);
                }
                return r;
            }
            Ok(c) => {
                c.requests
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                c.records
                    .fetch_add(entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
                c.bytes_in
                    .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
                c
            }
        }
    } else {
        // Close-only / deferred-error requests skip admission; a single
        // resolve here still beats the old two-site double resolve.
        crate::usage::counters(&name_hash)
    };

    // STANDARDS ISOLATION (audit P0): the singular route IS the
    // default-key Durable Stream — one strict sequence before and after
    // any product split. Routing keys belong to the plural product
    // route; the removed extension is rejected, never honored, so the
    // raw sequence can never absorb another key's records.
    if hdr(&headers, "stream-key").is_some() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "unknown_field",
            "Stream-Key was removed: routing keys live on /v1/streams (Prisma-Routing-Key)",
        );
    }
    // The product route passes its routing key as an internal
    // PARAMETER; the raw route has none and is therefore always the
    // default-key sequence.
    let routing_key = product_key.clone().unwrap_or_default();
    // ROUTING-V3 §1: an absent key is the empty/default key, and the
    // sole ordering guarantee is per-routing-key order. Resolution
    // picks the owning segment — the implicit single segment for every
    // stream born under the unified model (splits arrive with the
    // sketch scaler), the ordinal segment for legacy per-key layouts.
    let mut seg = desc.resolve_segment(&routing_key);
    if seg.sealed {
        // Mid-transition (a split/merge sealed this segment): refresh
        // the descriptor once and re-resolve; the successor is in the
        // CAS'd map. Still sealed after a fresh read = the transition
        // is mid-publish — tell the client to retry rather than hang.
        state.registry.invalidate(&state.sref(&name));
        match state.registry.get(&state.sref(&name)).await {
            Ok(Some(d2)) if desc_alive(&d2) => {
                desc = d2;
                seg = desc.resolve_segment(&routing_key);
            }
            _ => {}
        }
        if seg.sealed {
            let mut r = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "segment_transition",
                "segment map transition in progress; retry",
            );
            if let Ok(v) = axum::http::HeaderValue::from_str("1") {
                r.headers_mut().insert("retry-after", v);
            }
            return r;
        }
    }
    let seg = seg;
    let hash = seg.identity;
    // Per-SEGMENT capacity slot (see the note at the removed per-stream
    // acquisition above).
    let _stream_slot = match acquire_stream_slot(&state, seg.identity) {
        Ok(s) => s,
        Err(r) => return r,
    };
    // Predecessor identities for this routing key (nearest-first) —
    // only multi-segment dynamic maps have any.
    let producer_lineage: Vec<[u8; 16]> = match &desc.segments {
        Some(map) if map.segments.len() > 1 => {
            let mut preds: Vec<&crate::segmap::SegmentDesc> = map
                .segments
                .iter()
                .filter(|sg| sg.seg_id != seg.seg_id && sg.contains(seg.point) && !sg.is_live())
                .collect();
            preds.sort_by_key(|sg| std::cmp::Reverse((sg.created_ms, sg.seg_id)));
            preds
                .into_iter()
                .map(|sg| desc.dynamic_segment_identity(sg.seg_id))
                .collect()
        }
        _ => Vec::new(),
    };
    // Unified-scaler sketch feed (spec §5.1): admitted appends only.
    if !close_only && deferred.is_none() {
        let fed: usize = entries.iter().map(|e| e.len()).sum();
        crate::scaler3::note_append(&desc, &seg, fed as u64, entries.len() as u64);
    }
    // Usage counters key by the name hash; the absorber keys lag by this
    // engine hash. Record the alias so /v1/debug/usage can join them.
    crate::usage::link_storage(
        crate::crypto::RouteHash::for_stream(&desc.sref()),
        crate::crypto::SegmentHash(hash),
    );
    let kv = key_version(&headers);
    let subkey = derive_subkey(&key, &epoch, &routing_key, kv);
    state.keys.put(hash, key, epoch);

    // Watch hook (H1 position, H2 delivery): capability-registered by
    // the descriptor's immutable watch definitions, never by a profile.
    let touch = if !desc.watch_definitions.is_empty() && desc.is_json() && !entries.is_empty() {
        // Product watches (spec Stage 2 §3): derive watch keys from the
        // committed JSON records via the immutable definitions; the
        // journal ingests only after durability (H2 hook), preserving
        // the invalidation-after-visibility invariant. One aggregate
        // journal per COLLECTION (storage identity), coarse across
        // segments (§3.7).
        let journal = state.touch.journal(
            desc.storage_hash(),
            crate::crypto::RouteHash::for_stream(&desc.sref()),
            &crate::product::watch_pinned(&desc),
        );
        let mut key_ids: Vec<u32> = Vec::new();
        for raw in &entries {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw) {
                key_ids.extend(crate::product::product_watch_ids(
                    &desc.watch_definitions,
                    &v,
                ));
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
    let _metric_bytes = bytes as u64;
    #[cfg(test)]
    if !close {
        fork_failpoints::pause_append_before_enqueue(&name).await;
    } else {
        fork_failpoints::pause_close_before_enqueue(&name).await;
    }
    let (tx, rx) = oneshot::channel();
    let has_entries = !entries.is_empty();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        // The SEGMENT's physical route, not the parent's: frames, tail
        // state and postings group under the shard that owns them, and
        // for split children that is a different shard than the parent
        // (usage counters stay keyed by the stream name above).
        route: seg.shard_route,
        entries,
        usage: usage_c,
        routing_key,
        key_hash: seg.key_hash.0,
        // Producer state resolves through the key's sealed predecessors
        // after a split (ROUTING-V3 §3.6); single-segment streams carry
        // an empty chain.
        producer_lineage: producer_lineage.clone(),
        key_version: kv,
        subkey,
        ts_hint_ms: parse_ts_hint(&headers),
        seq: hdr(&headers, "stream-seq"),
        bytes,
        close,
        producer: producer.clone(),
        deferred_error: deferred,
        sealed_reject_new,
        touch,
        seal_gen: raw_seal_gen,
        seal_fence_to: None,
        // Reserved system streams bill nothing (§8.4) — without this,
        // every `_usage` emission would dirty `_usage` itself and the
        // drainer would feed back forever. BILLING_METER=off exists for
        // A/B isolation in benchmarks only.
        billing: (!crate::billing::is_reserved_stream(&desc.name)
            && std::env::var("BILLING_METER")
                .map(|v| v != "off")
                .unwrap_or(true))
        .then(|| {
            std::sync::Arc::new(crate::billing::BillingRef {
                identity: crate::billing::identity_of(&state, &desc),
                segment_id: seg.seg_id,
            })
        }),
        resp: tx,
    };
    let engine = match state.engine_for(&seg.shard_route).await {
        Ok(e) => e,
        Err(r) => return r,
    };
    // R25-C: THE maintenance admission point — one, in the shared append
    // core, after `engine_for` resolved ownership. A non-owner already
    // received its Streams-Replay-To above and never reaches this, so a
    // stale local latch cannot answer for someone else's backlog. Both
    // public append surfaces converge here (raw /v1/stream/{*name}
    // including hierarchical names, product append and appendMany, every
    // routing key, split children on their own shard routes), so there
    // is no second copy of the route grammar to drift.
    //
    // Skips: close-only operations carry no entries and must stay
    // admitted (an operator closing a stream is REDUCING future work),
    // and reserved system streams stay admitted because overload
    // recovery must not deadlock on its own system-of-record writes.
    if !close_only && has_entries && !crate::billing::is_reserved_stream(&name) {
        let limits = crate::backpressure::limits();
        if let Some(cause) = crate::backpressure::admit(&engine, &state.maint_latch, &limits) {
            state.maint_latch.note_shed();
            let mut r = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "maintenance_backpressure",
                &format!("{}; retry after maintenance catches up", cause.as_str()),
            );
            r.headers_mut()
                .insert("retry-after", axum::http::HeaderValue::from_static("5"));
            return r;
        }
    }
    // Wedge shed: if the shard's durability pipeline is stalled — either
    // the commit db.write is blocked (unflushed-full) or committed groups
    // have waited on the durable watermark beyond the threshold (WAL flush
    // stalled behind L0-full) — reject with a retryable 429 instead of
    // queueing. Without this, appends hang until the platform front door
    // kills them at ~30 s (8-minute wedge, 2026-07-21; detector missed the
    // stale-durability mode on 2026-07-22 when it watched db.write only).
    // 5 s: healthy durable waits under load peak ~1.5 s; a real wedge
    // climbs to 30 s+, so 5 s discriminates cleanly without false sheds.
    let blocked = engine.wedge_ms();
    if blocked > 5_000 {
        state
            .wedge_shed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut r = err_resp(
            StatusCode::TOO_MANY_REQUESTS,
            "engine_backpressure",
            "commit pipeline blocked (compaction lag); retry",
        );
        r.headers_mut()
            .insert("retry-after", axum::http::HeaderValue::from_static("2"));
        return r;
    }
    if engine.try_enqueue(req).is_err() {
        return err_resp(
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "append queue full",
        );
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

    // Ack-token shape is client-visible: single-segment streams (every
    // unified-model stream until its first split, and all total-order
    // streams) keep the plain token byte-for-byte; legacy per-key
    // layouts keep their epoch-prefixed tokens (epoch 0 when n == 1,
    // exactly as before).
    let segmented = desc.segments.is_some();
    let tok = |next: u64| {
        if segmented {
            crate::offsets::encode_ep(
                seg.seg_id,
                if next == 0 {
                    Offset::START
                } else {
                    Offset(Some(next - 1))
                },
            )
        } else {
            tail_token(next)
        }
    };
    // A definitive committer refusal of a raw close means the promised
    // records can never land, so this operation must take its own
    // uncommitted intent back down — otherwise the collection is left
    // Sealing: ordinary writes refused, and a plain close unable to
    // finish because the intent still owes a record. Only OUR claim, and
    // only while it still owes; 429/408 keep it, because the write may
    // yet succeed on a retry.
    if let Err(e) = &outcome {
        // A gap is NOT terminal: the missing predecessor may already be
        // admitted and staging inside this very commit group, which
        // would make an exact retry succeed. Tearing the intent down on
        // that verdict can drop a final record another request is still
        // completing. Gaps and stale epochs therefore keep the intent
        // and let the client retry exactly; only verdicts about the
        // REQUEST ITSELF — a malformed body, the wrong content type, a
        // sequence reused with different content — are terminal.
        let definitive = final_err_disposition(e) == FinalDisposition::DefinitivelyRejected;
        if close
            && close_carries_content
            && definitive
            && let Some(g) = raw_seal_gen
            && let Err(m) = crate::product::abandon_seal_intent(
                &state,
                &name,
                &this_close_op,
                &desc.stream_epoch,
                g,
            )
            .await
        {
            tracing::error!(stream = %name, "abandoning a refused raw close intent: {m}");
        }
    }
    match outcome {
        Ok(ack) => {
            touch_ttl(&state, &desc); // writes slide the idle window
            // A DUPLICATE that did not close: the producer tuple was
            // spent by an earlier NON-closing operation, so this close
            // can never deliver what its intent promised — the tuple
            // it would deliver under is gone, and every exact retry
            // will meet the same duplicate answer. The claim this
            // request installed comes down NOW (epoch- and
            // generation-fenced, so only our own), or the collection
            // sits Sealing behind an undeliverable promise until a
            // takeover discards it. The response stays the protocol's
            // duplicate answer; the collection stays open.
            if close
                && ack.duplicate
                && !ack.closed
                && let Some(g) = raw_seal_gen
                && let Err(m) = crate::product::abandon_seal_intent(
                    &state,
                    &name,
                    &this_close_op,
                    &desc.stream_epoch,
                    g,
                )
                .await
            {
                tracing::error!(
                    stream = %name,
                    "releasing a non-closing duplicate's seal intent: {m}"
                );
            }
            // The seal's own final record also carries `close`, but that
            // operation finishes the transition itself — it marks the
            // record committed first, which is what lets the seal
            // complete at all. Sealing here would run with no operation
            // id and be refused by its own intent.
            // Who finishes the collection transition:
            //   * the PRODUCT seal completes its own (it marks the
            //     record durable, then seals) — recognised by the
            //     trusted seal_auth parameter;
            //   * a raw close owns whatever intent matches its own
            //     computed identity, including a retry that is resuming
            //     one published before a crash;
            //   * a plain close-only just seals.
            if close && ack.closed && seal_auth.is_none() {
                // Who owns the completion:
                //   * a FRESH close that carried content (its write is
                //     the ack) — it marks and seals;
                //   * a DUPLICATE only when the descriptor says OUR
                //     operation still owes the record (the crashed
                //     close's exact retry). A duplicate whose identity
                //     is NOT the owed one — the protocol's
                //     close-with-different-body retry, deduplicated by
                //     producer sequence against an already-sealed
                //     collection — must answer as the duplicate it is,
                //     not attempt (and fail) somebody else's mark.
                let owns_final = is_owed_final || (close_carries_content && !ack.duplicate);
                #[cfg(test)]
                if owns_final {
                    fork_failpoints::pause_close_before_mark(&name).await;
                }
                #[cfg(test)]
                if owns_final && fork_failpoints::should_stop_before_mark_committed(&name) {
                    return err_resp(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "seal_incomplete",
                        "failpoint: stopped before marking the final durable",
                    );
                }
                if owns_final {
                    let g = raw_seal_gen.unwrap_or_default();
                    if let Err(e) = crate::product::mark_final_committed(
                        &state,
                        &name,
                        &this_close_op,
                        &desc.stream_epoch,
                        g,
                    )
                    .await
                    {
                        // The record is durable and the segment closed,
                        // but the transition could not be recorded as
                        // owning it — the claim moved, or the whole
                        // incarnation did. NEVER continue into run_seal
                        // here: a close issued against a deleted
                        // incarnation would claim and seal the
                        // replacement. The transition (whoever owns it
                        // now) stays resumable.
                        tracing::error!(stream = %name, "marking the close's final durable: {e}");
                        return err_resp(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "seal_incomplete",
                            &format!(
                                "the final record is durable but the seal could not be recorded: {e}; retry the close"
                            ),
                        );
                    }
                }
                let op = owns_final.then(|| this_close_op.clone());
                // An owner drives the transition under ITS generation
                // (the one its marked claim holds). A plain close-only
                // passes None and adopts whatever generation the shared
                // Empty claim holds NOW — concurrent plain closes renew
                // the claim as they join, and a close that pinned its
                // own admission-time generation would fail publication
                // against a sibling's renewal.
                let run_gen = if owns_final { raw_seal_gen } else { None };
                if let Err(e) =
                    crate::product::run_seal(&state, &name, op, &desc.stream_epoch, run_gen).await
                {
                    // The segment is closed but the collection is not
                    // sealed. Answering success is how the two surfaces
                    // end up permanently disagreeing; the transition
                    // stays resumable, so say it failed.
                    tracing::error!(stream = %name, "collection seal after raw close: {e}");
                    return err_resp(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "seal_incomplete",
                        &format!("the collection seal did not complete: {e}; retry the close"),
                    );
                }
            }
            // The synthetic identity is INTERNAL: it must not change
            // what the protocol says on the wire, so the response is
            // shaped as if the caller had sent no producer at all.
            let status = if ack.duplicate || close_only || producer.is_none() || synthetic_producer
            {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::OK
            };
            let mut r = Response::builder()
                .status(status)
                .header("Stream-Next-Offset", tok(ack.next_offset));
            if product_hash.is_some() {
                r = r.header("x-ack-last-offset", ack.last_offset.to_string());
            }
            // Internal: did THIS ack close the stream? A duplicate of an
            // earlier non-closing append also answers 2xx, and the seal
            // must tell those apart before treating the write as its
            // final record. Unconditional — a seal without a caller
            // producer carries no product hash.
            r = r.header("x-ack-closed", if ack.closed { "true" } else { "false" });
            if let Some((pe, ps)) = ack.producer.filter(|_| !synthetic_producer) {
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
        Err(AppendErr::SealSuperseded) => err_resp(
            StatusCode::CONFLICT,
            "seal_superseded",
            "the seal claim authorizing this write was taken over;              retry the close to re-enter the claim",
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
        Err(AppendErr::ProducerSeqReused) => err_resp(
            StatusCode::CONFLICT,
            "producer_sequence_reused",
            "same producer sequence with a different request",
        ),
        Err(AppendErr::CtMismatch) => err_resp(
            StatusCode::CONFLICT,
            "content_type_mismatch",
            "content type mismatch",
        ),
        Err(AppendErr::BadBody(m)) => err_resp(StatusCode::BAD_REQUEST, "invalid_body", &m),
        Err(AppendErr::Internal(m)) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
        Err(AppendErr::Moved) => {
            let mut r = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "shard_moving",
                "shard fenced by a new owner; retry",
            );
            r.headers_mut()
                .insert("retry-after", axum::http::HeaderValue::from_static("1"));
            r
        }
    }
}

/// A decrypted record ready for response assembly.
pub(crate) struct PlainRec {
    pub(crate) off: u64,
    pub(crate) payload: Bytes,
    /// Exact routing-key bytes from the frame header (product scan
    /// surfaces them per record; keyed reads ignore the field).
    pub(crate) rkey: String,
}

pub(crate) struct ReadOut {
    pub(crate) recs: Vec<PlainRec>,
    pub(crate) last: Option<u64>,
    pub(crate) end: u64,
    pub(crate) completed: bool,
}

/// Merged two-tier read returning plaintext records.
async fn read_records(
    _state: &AppState,
    _desc: &StreamDesc,
    key: &StreamKey,
    epoch: &[u8; 16],
    handle: &Arc<crate::shard::StreamHandle>,
    engine: &Arc<ShardEngine>,
    scan_from: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
    deliver: crate::shard::Deliver,
) -> Result<ReadOut, String> {
    read_merged(
        key, epoch, handle, engine, scan_from, key_filter, max_bytes, deliver,
    )
    .await
}

/// Decode raw stream-key-encrypted frames (v2 history or shard tail —
/// byte-identical formats) into plaintext records, charging the byte
/// budget per record.
fn decode_frames_into(
    frames: &[Bytes],
    key: &StreamKey,
    epoch: &[u8; 16],
    hash: &[u8; 16],
    subkeys: &mut HashMap<(String, u32), [u8; 32]>,
    out: &mut ReadOut,
    budget: &mut usize,
) -> Result<(), String> {
    for raw in frames {
        let Some(frame) = decode_frame(raw) else {
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
        let pt = decrypt_frame(&sk, hash, &frame, raw)?;
        *budget = budget.saturating_sub(pt.len());
        out.recs.push(PlainRec {
            off: frame.header.offset,
            payload: Bytes::from(pt),
            rkey: frame.header.routing_key.clone(),
        });
        out.last = Some(
            out.last
                .map_or(frame.header.offset, |o| o.max(frame.header.offset)),
        );
    }
    Ok(())
}

/// The merge itself, free of `AppState` so the simulation harness can call
/// the production reader instead of reimplementing the history/tail split
/// (`src/dst.rs`). A second copy of this boundary logic would be a copy
/// that can drift, and drift here means the oracle stops testing what
/// production does.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_merged(
    key: &StreamKey,
    epoch: &[u8; 16],
    handle: &Arc<crate::shard::StreamHandle>,
    engine: &Arc<ShardEngine>,
    scan_from: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
    deliver: crate::shard::Deliver,
) -> Result<ReadOut, String> {
    // The sub-stream identity (AAD + history-DB path): for total-order
    // streams this is the incarnation hash; for per-key streams, the
    // segment hash. Either way it's the handle's identity.
    let hash = handle.hash;
    let (absorbed, end, mut hist_v2, route) = {
        let st = handle.state.lock().unwrap();
        let end = match deliver {
            crate::shard::Deliver::Durable => st.durable.next,
            // Applied extends visibility to the applied watermark; the
            // history boundary below stays durable-sourced (absorption
            // only ever operates on durable data, so boundary <= end).
            crate::shard::Deliver::Applied => st.applied.next.max(st.durable.next),
        };
        (
            st.durable.absorbed,
            end,
            st.durable.history_v2,
            st.durable.route,
        )
    };
    let mut out = ReadOut {
        recs: Vec::new(),
        last: None,
        end,
        completed: true,
    };
    let mut budget = max_bytes;
    let mut subkeys: HashMap<(String, u32), [u8; 32]> = HashMap::new();

    // The absorbed snapshot above and the tail scan below are a TOCTOU
    // pair: the absorber can advance the boundary AND durably trim the
    // shard log between them, leaving the tail scan a hole at
    // `[cursor, new_boundary)` that this loop would otherwise emit as a
    // "complete" page — permanently skipping records for a paginating
    // client (2026-07-27 boundary-race DST failure). Everything trim can
    // remove is already readable in history (the absorber flushes history
    // before the boundary advances), so on detecting an advance we
    // re-serve the gap from history and re-scan the tail. `boundary` only
    // moves forward and is capped by `end`, so the loop terminates; the
    // bound is paranoia, and falling out of it yields an honest
    // `completed = false` partial page.
    let mut cursor = scan_from; // next offset still needed
    let mut boundary = absorbed; // history serves [_, boundary)
    for _ in 0..16 {
        let hist_upto = boundary.min(end);
        if cursor < hist_upto && budget > 0 {
            if !hist_v2 {
                // The v1 per-stream layout was deleted in the clean
                // switch: an unabsorbed-below-boundary tail without the
                // v2 flag cannot exist in a fresh namespace.
                return Err("unsupported_storage_layout: v1 history".into());
            }
            let completed = {
                // v2: the range lives in the shard's SHARED partition,
                // read through the owner's open Db — no reader open, no
                // checkpoint, no coverage probe (this Db's flush is what
                // advanced the boundary). Frames decode like tail frames.
                // Keyed ranges resolve their postings runs through the
                // engine's decoded slice cache (spec §7).
                let part = engine
                    .history_partition()
                    .await
                    .map_err(|e| e.to_string())?;
                let (frames, scan_last, completed) = match key_filter {
                    Some(rk) => crate::history::read_history2_keyed_cached(
                        &engine.postings_cache,
                        &part,
                        crate::crypto::RouteHash(route),
                        crate::crypto::SegmentHash(hash),
                        rk,
                        cursor,
                        hist_upto,
                        boundary,
                        budget,
                    )
                    .await
                    .map_err(|e| e.to_string())?,
                    None => crate::history::read_history2(
                        &part,
                        crate::crypto::RouteHash(route),
                        crate::crypto::SegmentHash(hash),
                        cursor,
                        hist_upto,
                        None,
                        budget,
                    )
                    .await
                    .map_err(|e| e.to_string())?,
                };
                decode_frames_into(
                    &frames,
                    key,
                    epoch,
                    &hash,
                    &mut subkeys,
                    &mut out,
                    &mut budget,
                )?;
                // consumed_to is first-class (review blocker): a partial
                // keyed page's cursor advances over every range the read
                // PROVED — index-verified match-free stretches and
                // mid-run truncation points — never inferred from the
                // last matching frame alone. Without this, a fat run
                // that planned zero frames re-polled the same position
                // forever.
                if let Some(sl) = scan_last {
                    out.last = Some(out.last.map_or(sl, |o| o.max(sl)));
                }
                completed
            };
            if !completed {
                // Byte-truncated, or (v1) the reader cannot prove coverage
                // of this boundary yet: report the honest partial; the
                // caller re-polls from `last + 1`.
                out.completed = false;
                return Ok(out);
            }
            // Fully scanned with proven coverage: everything below
            // `hist_upto` is consumed even when the range yields no
            // records for this key filter.
            if hist_upto > 0 {
                out.last = Some(out.last.map_or(hist_upto - 1, |o| o.max(hist_upto - 1)));
            }
            cursor = hist_upto;
        }
        if budget == 0 || cursor >= end {
            break;
        }
        let part = read_frames(engine, handle, cursor, key_filter, budget, deliver)
            .await
            .map_err(|e| e.to_string())?;
        // Revalidate the scan against concurrent absorption before
        // trusting it.
        let raced_boundary = if key_filter.is_none() {
            // Unfiltered offsets below the durable frontier are dense, so
            // a missing head IS the trim race (and a dense head rules it
            // out — ring hits and clean scans skip the tracker read).
            let head_gap = match part.frames.first().map(|raw| decode_frame(raw)) {
                Some(Some(f)) => f.header.offset > cursor,
                Some(None) => return Err("bad frame".into()),
                None => cursor < end, // nothing at all in a non-empty range
            };
            if head_gap {
                Some(
                    engine
                        .durable_absorbed(&hash)
                        .await
                        .map_err(|e| e.to_string())?,
                )
            } else {
                None
            }
        } else {
            // A filtered scan cannot distinguish "trimmed" from "did not
            // match", so always ask the remotely-durable tracker.
            let (durable, remote_v2) = engine
                .durable_absorbed(&hash)
                .await
                .map_err(|e| e.to_string())?;
            (durable > cursor).then_some((durable, remote_v2))
        };
        if let Some((durable, remote_v2)) = raced_boundary {
            if durable > boundary {
                // Adopt the remote LAYOUT FLAG with the remote boundary:
                // in the first absorption's flush-to-dispatch window the
                // in-memory snapshot still says v1 while the row that
                // moved the boundary already says v2 — mixing the two
                // refused a perfectly readable v2 range as v1.
                boundary = durable;
                hist_v2 = hist_v2 || remote_v2;
                continue; // the gap is in history now; re-serve from there
            }
            // A hole the boundary does not explain: never emit it as
            // consumed. Drop the tail and report the honest partial.
            out.completed = false;
            return Ok(out);
        }
        decode_frames_into(
            &part.frames,
            key,
            epoch,
            &hash,
            &mut subkeys,
            &mut out,
            &mut budget,
        )?;
        if let Some(last) = part.last_offset {
            out.last = Some(out.last.map_or(last, |o| o.max(last)));
        }
        break;
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

enum StartPos {
    At(u64),
    Now,
}

/// Reads on a FORK (pinned DS fork contract): records below the fork
/// boundary stitch through the ancestor chain; the stream's own tail
/// keeps normal read/long-poll/SSE semantics. Forks are single-segment
/// by construction.
async fn read_fork_inner(
    state: Arc<AppState>,
    desc: StreamDesc,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
) -> Response {
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
    let (_engine, handle) = match handle_of(&state, &desc).await {
        Ok(v) => v,
        Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
    };
    state.keys.put(handle.hash, key.clone(), epoch);
    let (mut end, mut closed) = {
        let st = handle.state.lock().unwrap();
        (st.durable.next, st.durable.closed)
    };
    if head_only {
        if !params.internal {
            crate::billing::meter_read(&state, &desc, 0, 0);
        }
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
    let scan_from = match params.offset.as_deref() {
        None => 0,
        Some("now") => end,
        Some(raw) => match Offset::parse(raw) {
            Ok(o) => o.scan_from(),
            Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_offset", &m),
        },
    };
    if live == Some("sse") {
        let (engine2, _h) = match handle_of(&state, &desc).await {
            Ok(v) => v,
            Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
        };
        return sse_response(
            state,
            desc,
            key,
            epoch,
            engine2,
            handle,
            StartPos::At(scan_from),
            params,
            SseSurface::Raw,
        )
        .await;
    }
    // Long-poll waits only at the OWN tail (inherited data answers
    // immediately).
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
    let out = match read_stitched(&state, &desc, &key, scan_from, MAX_READ_BYTES).await {
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
            _ if !out.completed => Offset::START.encode(),
            _ => tail_token(out.end),
        });
    let up_to_date = out.completed;
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
    let closed_now = handle.state.lock().unwrap().durable.closed;
    // §4.2: fork reads bill to the FORK resource requested — `desc`
    // here is the fork's descriptor even when records were stitched
    // from the ancestor chain.
    if !params.internal {
        let payload: u64 = out.recs.iter().map(|r| r.payload.len() as u64).sum();
        crate::billing::meter_read(&state, &desc, payload, out.recs.len() as u64);
    }
    let mut r = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, desc.content_type.clone())
        .header("Stream-Next-Offset", next_token)
        .header(header::CACHE_CONTROL, "no-store")
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if up_to_date {
        r = r.header("Stream-Up-To-Date", "true");
    }
    if closed_now && up_to_date {
        r = r.header("Stream-Closed", "true");
    }
    r.body(Body::from(body)).unwrap()
}

async fn read(
    state: Arc<AppState>,
    name: String,
    mut params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
) -> Response {
    // STANDARDS ISOLATION (audit P0): the singular route reads exactly
    // the DEFAULT routing key's sequence — never a segment-sequential
    // replay of every key after a product split, and never another
    // key's records before one. `?key=` was a pre-cutover extension and
    // is rejected rather than honored.
    if params.key.as_deref().is_some_and(|k| !k.is_empty()) {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "unknown_field",
            "?key= was removed: keyed reads live on /v1/streams (routingKey)",
        );
    }
    params.key = Some(String::new());
    read_inner(
        state,
        name,
        params,
        headers,
        head_only,
        true,
        SseSurface::Raw,
    )
    .await
}

/// Standard-path closure discriminator: the engine says CLOSED but our
/// cached descriptor never heard of a transition. Refresh once — if the
/// fresh descriptor shows successors or a pending transition, the
/// closure is a split seal and the caller must redispatch instead of
/// reporting it. `false` = redispatch (the fresh descriptor is cached).
async fn genuine_closure(state: &Arc<AppState>, name: &str, may_refresh: bool) -> bool {
    if !may_refresh {
        return true;
    }
    state.registry.invalidate(&state.sref(name));
    match state.registry.get(&state.sref(name)).await {
        Ok(Some(d)) => !d
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some()),
        _ => true,
    }
}

/// `may_refresh`: one stale-descriptor redispatch. A CLOSED engine
/// handle can mean a user close — or a split's seal racing our cached
/// descriptor (the transition seals the parent BEFORE publishing its
/// successors). Only the freshest descriptor tells them apart, and only
/// genuine closure may reach the client: a topology transition may
/// delay a reader, but it must never look like permanent end-of-stream.
pub(crate) async fn read_inner(
    state: Arc<AppState>,
    name: String,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
    may_refresh: bool,
    surface: SseSurface,
) -> Response {
    let desc = match state.registry.get(&state.sref(&name)).await {
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(d) => return gone_or_missing(d.as_ref()),
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if initializing(&desc) {
        return creating_resp();
    }
    if !head_only {
        touch_ttl(&state, &desc); // sliding idle expiry (HEAD never slides)
    }
    if desc.forked_from.is_some() {
        return read_fork_inner(state, desc, params, headers, head_only).await;
    }
    // ROUTING-V3 dynamic maps with successors OR an in-flight transition:
    // lineage-aware reads (spec §3.4/§9). A pending transition routes
    // here even at one segment, because the seal-to-publication gap is
    // exactly where the standard path would mistake the sealed parent
    // for a user-closed stream. Plain single-segment maps fall through —
    // byte-identical to the pre-split contract.
    if desc
        .segments
        .as_ref()
        .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
    {
        return read_v3_lineage_inner(
            state,
            desc,
            params,
            headers,
            head_only,
            may_refresh,
            surface,
        )
        .await;
    }
    // Single-segment streams (every unified-model stream until its
    // first split, all total-order streams, legacy per-key n=1) serve
    // the standard totally-ordered read path — byte-identical to the
    // pre-v3 contract because one segment means one routing key space.
    let hash = desc.resolve_segment("").identity;
    let engine = match state
        .engine_for(&crate::crypto::RouteHash::for_stream(&desc.sref()).0)
        .await
    {
        Ok(e) => e,
        Err(r) => return r,
    };
    let handle = match engine.stream_handle(hash).await {
        Ok(h) => h,
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    let deliver = params.deliver;
    // In Applied mode `end` is the applied watermark (what this mode
    // makes visible); `durable_floor` tracks the durable frontier for
    // cursor clamping — a resume cursor handed to a client must never
    // point past what a crash-restart is guaranteed to still have.
    let (mut end, mut closed, mut durable_floor) = {
        let st = handle.state.lock().unwrap();
        let dur = st.durable.next;
        let end = match deliver {
            crate::shard::Deliver::Durable => dur,
            crate::shard::Deliver::Applied => st.applied.next.max(dur),
        };
        (end, st.durable.closed, dur)
    };

    if head_only {
        if closed && !genuine_closure(&state, &name, may_refresh).await {
            return Box::pin(read_inner(
                state, name, params, headers, head_only, false, surface,
            ))
            .await;
        }
        // §5: HEAD is one read operation with zero data bytes.
        if !params.internal {
            crate::billing::meter_read(&state, &desc, 0, 0);
        }
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
        return sse_response(
            state, desc, key, epoch, engine, handle, start, params, surface,
        )
        .await;
    }

    let scan_from = match start {
        StartPos::Now => {
            // Instant tail snapshot for plain reads; long-poll from `now`
            // falls through with scan_from = current end.
            if live.is_none() {
                if closed && !genuine_closure(&state, &name, may_refresh).await {
                    return Box::pin(read_inner(
                        state, name, params, headers, head_only, false, surface,
                    ))
                    .await;
                }
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
    if deliver == crate::shard::Deliver::Applied && scan_from > end {
        // An Applied-mode cursor can outlive the pre-durability suffix
        // it was minted in (crash + WAL replay): offsets past the tail
        // may be REUSED with different content. Refuse instead of
        // parking at a position that would silently skip the rewritten
        // range; the client resumes from its durable cursor.
        return err_resp(
            StatusCode::CONFLICT,
            "cursor_beyond_tail",
            "cursor is ahead of the stream tail; resume from the durable cursor",
        );
    }

    let is_long_poll = live == Some("long-poll");
    let mut live_wake = false;
    let t_arm = std::time::Instant::now();
    let mut wake_us: u64 = 0;
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
                let applied_notified = handle.applied_notify.notified();
                let (e2, c2, d2) = {
                    let st = handle.state.lock().unwrap();
                    let dur = st.durable.next;
                    let e = match deliver {
                        crate::shard::Deliver::Durable => dur,
                        crate::shard::Deliver::Applied => st.applied.next.max(dur),
                    };
                    (e, st.durable.closed, dur)
                };
                end = e2;
                closed = c2;
                durable_floor = d2;
                if end > scan_from || closed {
                    live_wake = end > scan_from;
                    wake_us = t_arm.elapsed().as_micros() as u64;
                    break;
                }
                if deliver == crate::shard::Deliver::Applied {
                    // Applied waiters ALSO watch the durable notify: a
                    // seal or close publishes there, and applied wakes
                    // fire only for data.
                    tokio::select! {
                        _ = notified => {}
                        _ = applied_notified => {}
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                } else {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                }
            }
        }
        if end <= scan_from {
            if closed && !genuine_closure(&state, &name, may_refresh).await {
                return Box::pin(read_inner(
                    state, name, params, headers, head_only, false, surface,
                ))
                .await;
            }
            // Timeout (or closed-at-tail): 204 with resume state. Metered:
            // a tail probe is billable work even when it returns no bytes
            // (run-1 finding: `offset=now` reads were invisible to billing).
            let mut r = Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header("Stream-Next-Offset", tail_token(end))
                .header("Stream-Up-To-Date", "true")
                .header("Stream-Cursor", interval_cursor(params.cursor.as_deref()))
                .header(header::CACHE_CONTROL, "no-store");
            if deliver == crate::shard::Deliver::Applied {
                r = r.header("Stream-Durable-Offset", tail_token(durable_floor.min(end)));
            }
            if closed {
                r = r.header("Stream-Closed", "true");
            }
            return r.body(Body::empty()).unwrap();
        }
    }

    let frames_format = params.format.as_deref() == Some("frames");
    let t_read = std::time::Instant::now();
    let out = match read_records(
        &state,
        &desc,
        &key,
        &epoch,
        &handle,
        &engine,
        scan_from,
        params.key.as_deref(),
        // Woken live reads carry a fresh commit group, not a backlog:
        // keep the response (and the client's rearm) proportional to it.
        params.max_bytes.unwrap_or(if live_wake {
            tail_max_bytes()
        } else {
            MAX_READ_BYTES
        }),
        deliver,
    )
    .await
    {
        Ok(o) => o,
        Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
    };
    let read_us = t_read.elapsed().as_micros() as u64;
    let next_token = out
        .last
        .map(|o| Offset(Some(o)).encode())
        .unwrap_or_else(|| match params.offset.as_deref() {
            Some(raw) if raw != "now" => Offset::parse(raw)
                .map(|o| o.encode())
                .unwrap_or_else(|_| tail_token(out.end)),
            // No offset given and nothing proven: an INCOMPLETE empty
            // page must hold the cursor at the start, not teleport it to
            // the stream end (review blocker: that fallback silently
            // skipped everything an oversized-run stall failed to serve).
            _ if !out.completed => Offset::START.encode(),
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
        let mut buf = BytesMut::new();
        let mut subkeys: HashMap<String, [u8; 32]> = HashMap::new();
        for r in &out.recs {
            let sk = *subkeys
                .entry(params.key.clone().unwrap_or_default())
                .or_insert_with(|| {
                    derive_subkey(&key, &epoch, params.key.as_deref().unwrap_or(""), 0)
                });
            let frame = encrypt_frame(
                &sk,
                &hash,
                &FrameHeader {
                    offset: r.off,
                    ts_ms: 0,
                    key_version: 0,
                    routing_key: params.key.clone().unwrap_or_default(),
                },
                &r.payload,
            );
            buf.extend_from_slice(&frame);
        }
        buf.freeze()
    } else {
        let mut buf = BytesMut::new();
        for r in &out.recs {
            buf.extend_from_slice(&r.payload);
        }
        buf.freeze()
    };

    // A drained read of a closed handle is the response that would carry
    // Stream-Closed — discriminate a split seal from a user close BEFORE
    // metering, so a redispatched read is billed exactly once.
    if up_to_date && closed && !genuine_closure(&state, &name, may_refresh).await {
        return Box::pin(read_inner(
            state, name, params, headers, head_only, false, surface,
        ))
        .await;
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
    if deliver == crate::shard::Deliver::Applied {
        // Fresh durable floor at response time: the least-stale clamp
        // for the resume cursor, and the marker for which served
        // records are still provisional. Staleness is one-sided — a
        // record marked pending may already be durable, never the
        // reverse.
        let floor_now = handle.state.lock().unwrap().durable.next;
        let next_pos = out.last.map(|o| o + 1).unwrap_or(scan_from);
        r = r.header("Stream-Durable-Offset", tail_token(floor_now.min(next_pos)));
        if let Some(i) = out.recs.iter().position(|rec| rec.off >= floor_now) {
            r = r.header("Stream-Pending-From", i.to_string());
        }
    }
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
    if debug_timing() {
        r = r.header(
            "Streams-Debug-Wait",
            format!(
                "waited={} arm_us={} read_us={}",
                live_wake as u8, wake_us, read_us
            ),
        );
    }
    crate::usage::counters(&crate::crypto::RouteHash::for_stream(&desc.sref()).0)
        .bytes_out
        .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
    // THE read meter (§5/§7): payload bytes only — array brackets,
    // commas, frame encryption and headers are excluded by summing the
    // record payloads, not the wire body. An empty long-poll meters the
    // operation with zero data bytes. Internal relays never meter; the
    // coordinator that requested them does.
    if !params.internal {
        let payload: u64 = out.recs.iter().map(|r| r.payload.len() as u64).sum();
        crate::billing::meter_read(&state, &desc, payload, out.recs.len() as u64);
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
        let text = String::from_utf8_lossy(payload);
        for line in text.split(['\r', '\n']) {
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

/// sse_control with a pre-encoded (epoch) cursor token — the lineage
/// streamer's controls name segments, not scalar offsets.
fn sse_control_tok(next_tok: &str, cursor: Option<&str>, up_to_date: bool, closed: bool) -> String {
    let mut fields = vec![format!("\"streamNextOffset\":\"{next_tok}\"")];
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

/// Which wire contract an SSE connection speaks. The drain/hop/wait
/// machinery is identical; only the CONTROL frames differ — raw
/// connections carry protocol offset tokens, product connections carry
/// signed key cursors (a product control frame must never leak a
/// Stream-Next-Offset token — appendix §13).
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum SseSurface {
    Raw,
    Product,
}

/// Product SSE control frame: signed key cursor + product field names.
fn sse_control_product(cursor_tok: &str, up_to_date: bool, sealed: bool) -> String {
    let mut fields = vec![format!("\"nextCursor\":\"{cursor_tok}\"")];
    if up_to_date {
        fields.push("\"upToDate\":true".to_string());
    }
    if sealed {
        fields.push("\"sealed\":true".to_string());
    }
    format!("event: control\ndata:{{{}}}\n\n", fields.join(","))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn sse_lineage_response(
    state: Arc<AppState>,
    desc: StreamDesc,
    key: StreamKey,
    epoch: [u8; 16],
    lineage: Vec<crate::segmap::SegmentDesc>,
    mut pos: usize,
    mut scan_from: u64,
    rk: String,
    params: ReadParams,
    surface: SseSurface,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let sse_hash = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
    // One subscribe operation; delivered bytes meter per chunk (§5).
    crate::billing::meter_read(&state, &desc, 0, 0);
    let cursor = params.cursor.clone();
    let rk_hash = crate::crypto::stream_hash(&rk);
    let seg_tok = |seg_id: u32, next: u64| {
        crate::offsets::encode_ep(
            seg_id,
            if next == 0 {
                Offset::START
            } else {
                Offset(Some(next - 1))
            },
        )
    };
    tokio::spawn(async move {
        let mut first = true;
        'lineage: loop {
            let sg = &lineage[pos];
            let identity = desc.dynamic_segment_identity(sg.seg_id);
            let Ok(engine) = state
                .engine_for_scaler(&desc.segment_route(sg))
                .await
                .ok_or(())
            else {
                return;
            };
            let Ok(handle) = engine.stream_handle(identity).await else {
                return;
            };
            state.keys.put(identity, key.clone(), epoch);
            let is_last = pos + 1 >= lineage.len();
            loop {
                let (end, closed) = {
                    let st = handle.state.lock().unwrap();
                    (st.durable.next, st.durable.closed)
                };
                if scan_from == u64::MAX {
                    scan_from = end; // offset=now on the live tail
                }
                let seg_end = sg.sealed_next_offset.unwrap_or(end);
                let mut sent_any = false;
                if scan_from < seg_end {
                    match read_records(
                        &state,
                        &desc,
                        &key,
                        &epoch,
                        &handle,
                        &engine,
                        scan_from,
                        Some(&rk),
                        MAX_READ_BYTES,
                        // SSE is durable-only: Applied would need
                        // session-position machinery this streamer does
                        // not have; product_read rejects the combination.
                        crate::shard::Deliver::Durable,
                    )
                    .await
                    {
                        Ok(out) => {
                            // One control per data event; the batch's
                            // last carries the flags (pinned baseline).
                            let pos_after = out
                                .last
                                .map(|l| (l + 1).min(seg_end.max(scan_from)))
                                .unwrap_or(scan_from);
                            let will_end = out.completed && pos_after >= seg_end && is_last;
                            let report_closed = closed
                                && will_end
                                && genuine_closure(&state, &desc.name, true).await;
                            let n = out.recs.len();
                            let bill_id = crate::billing::identity_of(&state, &desc);
                            for (i, r) in out.recs.iter().enumerate() {
                                let ev = sse_data_event(&desc, &r.payload);
                                if tx.send(Ok(Bytes::from(ev))).await.is_err() {
                                    return;
                                }
                                // §4.2: each emitted payload, pre-framing.
                                crate::billing::meter_read_chunk(
                                    &state.billing_reads,
                                    &bill_id,
                                    r.payload.len() as u64,
                                    1,
                                );
                                let last_rec = i + 1 == n && out.completed;
                                let (utd, cls) = if last_rec {
                                    (will_end, report_closed)
                                } else {
                                    (false, false)
                                };
                                let ctl = match surface {
                                    SseSurface::Raw => sse_control_tok(
                                        &seg_tok(sg.seg_id, r.off + 1),
                                        cursor.as_deref(),
                                        utd,
                                        cls,
                                    ),
                                    SseSurface::Product => sse_control_product(
                                        &crate::product_cursor::KeyCursor {
                                            epoch,
                                            key_hash: rk_hash,
                                            seg_id: sg.seg_id,
                                            offset: r.off + 1,
                                        }
                                        .encode(&key),
                                        utd,
                                        cls,
                                    ),
                                };
                                if tx.send(Ok(Bytes::from(ctl))).await.is_err() {
                                    return;
                                }
                                sent_any = true;
                            }
                            if let Some(last) = out.last {
                                scan_from = (last + 1).min(seg_end.max(scan_from));
                            }
                            if !out.completed {
                                continue; // keep draining before control
                            }
                            if report_closed && scan_from >= seg_end {
                                return; // final closed control sent
                            }
                        }
                        Err(_) => return,
                    }
                }
                if scan_from >= seg_end && !is_last {
                    // Sealed predecessor drained: hop to the successor.
                    pos += 1;
                    scan_from = 0;
                    continue 'lineage;
                }
                let at_end = scan_from >= seg_end;
                if (at_end || first) && !sent_any {
                    // Empty drain: one status-only control. A close on
                    // the LAST segment can be a split's seal: genuine
                    // closure sends the final closed control; a
                    // transition ends the connection silently and the
                    // reconnect follows the successors.
                    let report_closed =
                        closed && at_end && genuine_closure(&state, &desc.name, true).await;
                    let ctl = match surface {
                        SseSurface::Raw => sse_control_tok(
                            &seg_tok(sg.seg_id, scan_from),
                            cursor.as_deref(),
                            at_end,
                            report_closed,
                        ),
                        SseSurface::Product => sse_control_product(
                            &crate::product_cursor::KeyCursor {
                                epoch,
                                key_hash: rk_hash,
                                seg_id: sg.seg_id,
                                offset: scan_from,
                            }
                            .encode(&key),
                            at_end,
                            report_closed,
                        ),
                    };
                    if tx.send(Ok(Bytes::from(ctl))).await.is_err() {
                        return;
                    }
                    if closed && at_end {
                        return;
                    }
                } else if closed && at_end && sent_any {
                    if genuine_closure(&state, &desc.name, true).await {
                        return; // final flags rode the last per-data control
                    }
                    return; // transition: silent end, reconnect follows successors
                }
                first = false;
                // Wait for new durable data on the live tail.
                let notified = handle.notify.notified();
                let cur_end = handle.state.lock().unwrap().durable.next;
                if cur_end > scan_from {
                    continue;
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {
                        if tx.send(Ok(Bytes::from(": keep-alive\n\n"))).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    });
    let sse_usage = crate::usage::counters(&sse_hash);
    let stream = futures_util::StreamExt::map(
        tokio_stream::wrappers::ReceiverStream::new(rx),
        move |item| {
            if let Ok(b) = &item {
                sse_usage
                    .bytes_out
                    .fetch_add(b.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            item
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-store")
        .header("Cross-Origin-Resource-Policy", "cross-origin")
        .body(Body::from_stream(stream))
        .unwrap()
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
    surface: SseSurface,
) -> Response {
    let sse_hash = crate::crypto::RouteHash::for_stream(&desc.sref()).0;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    // One subscribe operation; delivered bytes meter per chunk (§5).
    crate::billing::meter_read(&state, &desc, 0, 0);
    let binary = {
        let mt = crate::registry::media_type(&desc.content_type);
        mt != "application/json" && !mt.starts_with("text/")
    };
    let cursor = params.cursor.clone();
    let key_filter = params.key.clone();
    let rk_hash = crate::crypto::stream_hash(key_filter.as_deref().unwrap_or(""));
    let ctl_seg_id = desc
        .resolve_segment(key_filter.as_deref().unwrap_or(""))
        .seg_id;

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
            if pos < end && !from_now || (from_now && !first && pos < end) {
                let read = if desc.forked_from.is_some() {
                    // Fork SSE catch-up: inherited records stitch
                    // through the ancestor chain.
                    read_stitched(&state, &desc, &key, pos, MAX_READ_BYTES).await
                } else {
                    read_records(
                        &state,
                        &desc,
                        &key,
                        &epoch,
                        &handle,
                        &engine,
                        pos,
                        key_filter.as_deref(),
                        MAX_READ_BYTES,
                        // SSE is durable-only (see sse_lineage_response).
                        crate::shard::Deliver::Durable,
                    )
                    .await
                };
                match read {
                    Ok(out) => {
                        // Pinned baseline: every data event pairs with
                        // exactly ONE control naming the position after
                        // it; the batch's LAST control carries the
                        // up-to-date/closed flags (no separate batch
                        // control follows).
                        let pos_after = out.last.map(|l| l + 1).unwrap_or(pos);
                        let will_end = out.completed && pos_after >= end;
                        let report_closed =
                            closed && will_end && genuine_closure(&state, &desc.name, true).await;
                        let n = out.recs.len();
                        let bill_id = crate::billing::identity_of(&state, &desc);
                        for (i, r) in out.recs.iter().enumerate() {
                            let ev = sse_data_event(&desc, &r.payload);
                            if tx.send(Ok(Bytes::from(ev))).await.is_err() {
                                return;
                            }
                            // §4.2: each emitted payload, pre-framing.
                            crate::billing::meter_read_chunk(
                                &state.billing_reads,
                                &bill_id,
                                r.payload.len() as u64,
                                1,
                            );
                            let last_rec = i + 1 == n && out.completed;
                            let (utd, cls) = if last_rec {
                                (will_end, report_closed)
                            } else {
                                (false, false)
                            };
                            let ctl = match surface {
                                SseSurface::Raw => {
                                    sse_control(r.off + 1, cursor.as_deref(), utd, cls)
                                }
                                SseSurface::Product => sse_control_product(
                                    &crate::product_cursor::KeyCursor {
                                        epoch,
                                        key_hash: rk_hash,
                                        seg_id: ctl_seg_id,
                                        offset: r.off + 1,
                                    }
                                    .encode(&key),
                                    utd,
                                    cls,
                                ),
                            };
                            if tx.send(Ok(Bytes::from(ctl))).await.is_err() {
                                return;
                            }
                            sent_any = true;
                        }
                        if let Some(last) = out.last {
                            pos = last + 1;
                        }
                        if !out.completed {
                            continue; // keep draining before control
                        }
                        if closed && pos >= end && report_closed {
                            return; // final closed control sent
                        }
                    }
                    Err(_) => return,
                }
            }
            let at_end = pos >= end;
            if (at_end || first) && !sent_any {
                // Empty drain (connect at tail / offset=now): one
                // status-only control. A close observed mid-SSE can be a
                // split's seal, not a user close: genuine closure sends
                // the final closed control; a transition ends the
                // connection WITHOUT it and the reconnect's fresh
                // dispatch serves the successors.
                let report_closed =
                    closed && at_end && genuine_closure(&state, &desc.name, true).await;
                let ctl = match surface {
                    SseSurface::Raw => sse_control(pos, cursor.as_deref(), at_end, report_closed),
                    SseSurface::Product => sse_control_product(
                        &crate::product_cursor::KeyCursor {
                            epoch,
                            key_hash: rk_hash,
                            seg_id: ctl_seg_id,
                            offset: pos,
                        }
                        .encode(&key),
                        at_end,
                        report_closed,
                    ),
                };
                if tx.send(Ok(Bytes::from(ctl))).await.is_err() {
                    return;
                }
                if closed && at_end {
                    return; // final control sent; close connection
                }
            } else if closed && at_end && sent_any {
                return; // final flags rode the last per-data control
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

    let sse_usage = crate::usage::counters(&sse_hash);
    let stream = futures_util::StreamExt::map(
        tokio_stream::wrappers::ReceiverStream::new(rx),
        move |item| {
            if let Ok(b) = &item {
                sse_usage
                    .bytes_out
                    .fetch_add(b.len() as u64, std::sync::atomic::Ordering::Relaxed);
            }
            item
        },
    );
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

/// Percent-encode a stream name for use as a URL PATH, preserving the
/// hierarchy separator. Product names are hierarchical UTF-8 and may
/// legally contain '?', '#', '%' — interpolating one raw into a relay
/// URL turned the rest of the name into a query, fragment, or invalid
/// escape and addressed the wrong stream (round-19 fleet-contract
/// finding). Every internal relay must route its name through this.
pub(crate) fn encode_stream_name_path(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 8);
    for seg in name.split('/') {
        if !out.is_empty() {
            out.push('/');
        }
        for b in seg.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
    }
    out
}

/// Shared client for fleet-internal peer calls (segment fan-out). One
/// pool, HTTP/1.1, idle timeout under the platform's ~5 s VM-suspend
/// socket kill (same rule as the store client and the pilot LB).
pub(crate) fn peer_client() -> &'static reqwest::Client {
    static C: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .http1_only()
            .pool_idle_timeout(std::time::Duration::from_secs(4))
            .tcp_nodelay(true)
            .build()
            .expect("peer client")
    })
}

/// Resolve a Streams-Replay-To response to a peer base URL. None when
/// the response is not an ownership bounce or the peer is unknown
/// (standalone mode, missing SELF_URL) — callers fall back to returning
/// the original 409, which is today's behavior.
pub(crate) fn replay_peer_url(state: &AppState, r: &Response) -> Option<(String, String)> {
    let owner = r
        .headers()
        .get("streams-replay-to")?
        .to_str()
        .ok()?
        .to_string();
    let url = state.peer_urls.read().unwrap().get(&owner)?.clone();
    Some((owner, url))
}

/// Relay one segment-positioned read to the segment's owner and stream
/// its raw response back verbatim. The peer serves under no_fanout, so
/// depth is exactly one; any relay failure returns None and the caller
/// surfaces the original ownership 409 (retryable via the router).
async fn relay_segment_read(
    state: &Arc<AppState>,
    base: String,
    desc: &StreamDesc,
    seg_id: u32,
    scan_from: u64,
    params: &ReadParams,
    headers: &HeaderMap,
    head_only: bool,
) -> Option<Response> {
    let tok = if scan_from == u64::MAX {
        "now".to_string()
    } else if scan_from == 0 {
        crate::offsets::encode_ep(seg_id, Offset::START)
    } else {
        crate::offsets::encode_ep(seg_id, Offset(Some(scan_from - 1)))
    };
    // No urlencoding dep (supply-chain posture): RFC 3986 unreserved
    // pass-through, everything else percent-encoded.
    fn pct(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }
    let mut q = format!("offset={tok}");
    if let Some(k) = &params.key {
        q.push_str(&format!("&key={}", pct(k)));
    }
    if let Some(l) = &params.live {
        q.push_str(&format!("&live={}", pct(l)));
    }
    if let Some(t) = &params.timeout {
        q.push_str(&format!("&timeout={}", pct(t)));
    }
    if head_only {
        q.push_str("&head=1");
    }
    let mut req = peer_client()
        .get(format!(
            "{base}/v1/internal/segment-read/{}?{q}",
            encode_stream_name_path(&desc.name)
        ))
        .timeout(std::time::Duration::from_secs(40));
    // Incarnation binding: the peer refuses outright if this name now
    // holds a different stream (round-19 ABA).
    let target = crate::product::InternalTarget::of(desc, seg_id)?;
    for (k, v) in target.headers() {
        req = req.header(k, v);
    }
    if let Some(t) = &state.fleet_internal_token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    for h in ["stream-encryption-key", "prisma-encryption-key"] {
        if let Some(v) = headers.get(h) {
            req = req.header(h, v.clone());
        }
    }
    if let Some(mb) = params.max_bytes {
        req = req.header("streams-internal-max-bytes", mb.to_string());
    }
    if params.deliver == crate::shard::Deliver::Applied {
        req = req.header("streams-internal-deliver", "applied");
    }
    match req.send().await {
        Ok(r) => {
            let mut out = Response::builder().status(r.status().as_u16());
            for (k, v) in r.headers() {
                let n = k.as_str();
                if n != "connection" && n != "transfer-encoding" {
                    out = out.header(k, v);
                }
            }
            use futures_util::TryStreamExt;
            Some(
                out.body(axum::body::Body::from_stream(
                    r.bytes_stream().map_err(std::io::Error::other),
                ))
                .unwrap(),
            )
        }
        Err(e) => {
            tracing::warn!("segment fan-out relay to {base} failed: {e}");
            None
        }
    }
}

/// Fleet-internal fan-out target: a keyed, segment-positioned read
/// served strictly from local ownership (no_fanout). Bearer-gated with
/// the fleet's shared token; the internal max-bytes/deliver headers are
/// honored only here so the public raw grammar stays pinned.
async fn internal_segment_read(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(mut params): Query<ReadParams>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    if !fleet_internal_authorized(&state, &headers) {
        return internal_unauthorized();
    }
    params.no_fanout = true;
    params.internal = true;
    // Clamped to the SAME server-side ceiling the public read obeys: an
    // internal budget header must not buy a bigger page than the
    // operation it is relaying on behalf of (round-19 finding).
    params.max_bytes = headers
        .get("streams-internal-max-bytes")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(4096, MAX_READ_BYTES));
    if headers
        .get("streams-internal-deliver")
        .and_then(|v| v.to_str().ok())
        == Some("applied")
    {
        params.deliver = crate::shard::Deliver::Applied;
    }
    let head_only = uri
        .query()
        .map(|q| q.split('&').any(|p| p == "head=1"))
        .unwrap_or(false);
    // ABA GUARD (round-19): bind the relayed read to the sender's
    // incarnation. Without this, a delete/recreate between dispatch and
    // arrival serves the REPLACEMENT stream's records against the
    // original request's cursor. §16: the registry identity comes from
    // the sender's PROJECT header, never the deployment tenant.
    let sref = match crate::product::internal_sref(&headers, &name) {
        Ok(s) => s,
        Err(r) => return r,
    };
    match state.registry.get(&sref).await {
        Ok(Some(desc)) => {
            if let Err(r) = crate::product::verify_internal_target(&desc, &headers) {
                return r;
            }
        }
        Ok(None) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                &e.to_string(),
            );
        }
    }
    read_inner(
        state,
        name,
        params,
        headers,
        head_only,
        true,
        SseSurface::Raw,
    )
    .await
}

/// ROUTING-V3 lineage reads (spec §3.4/§9) for dynamic maps with
/// successors. Contract:
///
/// - `?key=<k>`: ordered read for one routing key. The cursor names a
///   position in ONE segment of the key's lineage
///   (`epoch = segment id`); a drained sealed segment hands the next
///   cursor to the successor containing the key at offset 0. Long-poll
///   waits only on the key's LIVE segment.
/// - no key: deterministic whole-stream replay in segment-id order —
///   every record exactly once, no cross-key ordering.
/// - live without a key: unsupported (one scalar cursor cannot
///   represent concurrent segment progress).
/// - SSE across lineage: not yet wired (single-segment streams keep
///   full SSE); explicit 400 rather than silent misbehavior.
/// `may_refresh`: one stale-descriptor retry. Two shapes demand it —
/// a cursor token naming a segment our cached map does not know yet,
/// and a CLOSED segment handle our map still calls live-and-last (a
/// split sealed it after our descriptor read; stopping there would
/// declare Up-To-Date below the successor's records). A refreshed map
/// that STILL shows live-and-last with no pending transition is a
/// genuinely user-closed stream; one whose pending transition names
/// this segment is mid-split (seal done, successors unpublished) — the
/// SEAL GAP — and the response may carry records and a resume cursor
/// but NEVER Stream-Closed and NEVER a final Stream-Up-To-Date.
async fn read_v3_lineage_inner(
    state: Arc<AppState>,
    desc: StreamDesc,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
    may_refresh: bool,
    surface: SseSurface,
) -> Response {
    let map = desc.segments.clone().expect("dispatch guaranteed a map");
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
        Some("sse") if params.key.is_some() => Some("sse"),
        Some("sse") => {
            // Keyless SSE has the same scalar-cursor impossibility as
            // keyless long-poll on a segmented stream.
            return err_resp(
                StatusCode::BAD_REQUEST,
                "keyless_live",
                "SSE on a segmented stream requires key=",
            );
        }
        Some(_) => return err_resp(StatusCode::BAD_REQUEST, "invalid_live", "invalid live mode"),
    };
    if live.is_some() && params.key.is_none() && map.segments.len() > 1 {
        // Spec §3.4: one scalar cursor cannot represent several
        // concurrently progressing segments. A single-segment map routed
        // here for its PENDING transition keeps keyless live until the
        // successors actually publish — the seal gap must not change the
        // API surface out from under a poller mid-flight.
        return err_resp(
            StatusCode::BAD_REQUEST,
            "keyless_live",
            "live reads on a segmented stream require key=",
        );
    }

    // Lineage: for a keyed read, every segment whose range contains the
    // key point, oldest first; keyless replay walks ALL segments in
    // seg-id order.
    let lineage: Vec<crate::segmap::SegmentDesc> = match params.key.as_deref() {
        Some(rk) => {
            let point = StreamDesc::key_point(rk);
            let mut v: Vec<_> = map
                .segments
                .iter()
                .filter(|sg| sg.contains(point))
                .cloned()
                .collect();
            v.sort_by_key(|sg| (sg.created_ms, sg.seg_id));
            v
        }
        None => {
            let mut v = map.segments.clone();
            v.sort_by_key(|sg| sg.seg_id);
            v
        }
    };
    if lineage.is_empty() {
        return err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "empty lineage",
        );
    }

    // Cursor → (segment position in lineage, offset).
    let (mut pos, mut scan_from) = match params.offset.as_deref() {
        None => (0usize, 0u64),
        Some("now") => {
            // Keyed live tail: the lineage's last (live) segment.
            (lineage.len() - 1, u64::MAX)
        }
        Some(raw) => match crate::offsets::parse_ep(raw) {
            Ok((e, o)) => match lineage.iter().position(|sg| sg.seg_id == e) {
                Some(p) => (p, o.scan_from()),
                None if may_refresh => {
                    // A successor our cached map has not seen yet.
                    state.registry.invalidate(&state.sref(&desc.name));
                    let fresh = match state.registry.get(&state.sref(&desc.name)).await {
                        Ok(Some(d)) if desc_alive(&d) => d,
                        _ => {
                            return err_resp(
                                StatusCode::BAD_REQUEST,
                                "invalid_offset",
                                "offset names a segment outside this lineage",
                            );
                        }
                    };
                    return Box::pin(read_v3_lineage_inner(
                        state, fresh, params, headers, head_only, false, surface,
                    ))
                    .await;
                }
                None => {
                    return err_resp(
                        StatusCode::BAD_REQUEST,
                        "invalid_offset",
                        "offset names a segment outside this lineage",
                    );
                }
            },
            Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_offset", &m),
        },
    };

    if live == Some("sse") {
        // Keyed SSE across lineage (review deferral, now wired): drain
        // every predecessor's matches, then live-follow the key's live
        // segment. A seal observed mid-stream that is NOT a genuine
        // close ends the connection without streamClosed — the
        // reconnect's fresh dispatch serves the successors.
        let rk = params.key.clone().unwrap_or_default();
        return sse_lineage_response(
            state, desc, key, epoch, lineage, pos, scan_from, rk, params, surface,
        );
    }
    // Hop forward over already-drained sealed segments so one request
    // always serves records when any exist ahead.
    let seg_tok = |seg_id: u32, last: Option<u64>| match last {
        None => crate::offsets::encode_ep(seg_id, Offset::START),
        Some(o) => crate::offsets::encode_ep(seg_id, Offset(Some(o))),
    };
    let entry_pos = pos;
    loop {
        let sg = &lineage[pos];
        let identity = desc.dynamic_segment_identity(sg.seg_id);
        // Each segment lives on ITS OWN shard route (split children get
        // real routes — review blocker 1); hard-coding the parent route
        // here read an empty keyspace on the wrong engine for any moved
        // child.
        let engine = match state.engine_for(&desc.segment_route(sg)).await {
            Ok(e) => e,
            Err(r) => {
                // Cross-owner lineage fan-out: this segment lives on a
                // peer. Relay this one segment-positioned page to its
                // owner and stream the raw response back — the caller
                // (raw or product wrapper) treats it exactly like a
                // locally-served page. Depth is one (no_fanout on the
                // peer). Fallback: surface the ownership 409, which
                // routers follow via Streams-Replay-To.
                if !params.no_fanout {
                    let peer = replay_peer_url(&state, &r).map(|(_, base)| base);
                    if let Some(base) = peer
                        && let Some(resp) = relay_segment_read(
                            &state, base, &desc, sg.seg_id, scan_from, &params, &headers, head_only,
                        )
                        .await
                    {
                        return resp;
                    }
                    return r;
                }
                // no_fanout (we ARE the relay target): never relay
                // again. Hop-forward walked over drained local segments
                // into a foreign one — hand the cursor to it with an
                // empty page; the client's next request reaches its
                // owner and the cursor advances every round (no relay
                // cycles under ownership churn). If the REQUESTED
                // segment itself is foreign (ownership moved between
                // the relayer's pick and now), surface the 409.
                if pos > entry_pos {
                    let body: Bytes = if desc.is_json() {
                        Bytes::from_static(b"[]")
                    } else {
                        Bytes::new()
                    };
                    return Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, desc.content_type.clone())
                        .header("Stream-Next-Offset", seg_tok(sg.seg_id, None))
                        .header(header::CACHE_CONTROL, "no-store")
                        .header("Cross-Origin-Resource-Policy", "cross-origin")
                        .body(Body::from(body))
                        .unwrap();
                }
                return r;
            }
        };
        let handle = match engine.stream_handle(identity).await {
            Ok(h) => h,
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
        };
        state.keys.put(identity, key.clone(), epoch);
        let deliver = params.deliver;
        let (durable_next, closed, applied_next) = {
            let st = handle.state.lock().unwrap();
            (
                st.durable.next,
                st.durable.closed,
                st.applied.next.max(st.durable.next),
            )
        };
        let live_and_last = sg.sealed_next_offset.is_none() && pos + 1 >= lineage.len();
        if closed && live_and_last && may_refresh {
            // The engine says CLOSED but our map says live-and-last: a
            // split sealed this segment after our descriptor read. Help
            // the transition along WITHOUT blocking this read (resume is
            // idempotent; a parked or failing publication must not turn
            // a read into a hang), then refresh once — the successor (or
            // a genuine user close, or a still-pending gap) is in the
            // fresh map.
            {
                let st = state.clone();
                let nm = desc.name.clone();
                tokio::spawn(async move {
                    crate::scaler3::resume(&st, &nm).await;
                });
            }
            state.registry.invalidate(&state.sref(&desc.name));
            if let Ok(Some(fresh)) = state.registry.get(&state.sref(&desc.name)).await
                && desc_alive(&fresh)
            {
                return Box::pin(read_v3_lineage_inner(
                    state, fresh, params, headers, head_only, false, surface,
                ))
                .await;
            }
        }
        // The SEAL GAP: this segment is sealed but its successors are
        // not published yet (the map's pending transition names it).
        // Records stay servable; closure and finality do not exist —
        // the reader polls again and finds either the successors or a
        // genuinely closed stream.
        let seal_gap = closed
            && live_and_last
            && map
                .pending
                .as_ref()
                .is_some_and(|p| p.segs.contains(&sg.seg_id));
        let seg_end = sg.sealed_next_offset.unwrap_or(match deliver {
            crate::shard::Deliver::Durable => durable_next,
            crate::shard::Deliver::Applied => applied_next,
        });
        if scan_from == u64::MAX {
            scan_from = seg_end; // offset=now on the live segment
        }
        if deliver == crate::shard::Deliver::Applied && scan_from > seg_end {
            // A stale Applied cursor (minted past a suffix a crash
            // discarded — sealed segments included, since a seal can
            // land below a lost applied tail). See the single-segment
            // guard: refuse rather than skip rewritten offsets.
            return err_resp(
                StatusCode::CONFLICT,
                "cursor_beyond_tail",
                "cursor is ahead of the stream tail; resume from the durable cursor",
            );
        }
        let is_last = pos + 1 >= lineage.len();
        if scan_from >= seg_end && !is_last {
            // Drained sealed segment: hop to the successor at 0.
            pos += 1;
            scan_from = 0;
            continue;
        }

        // Long-poll on the live tail only.
        let mut end = seg_end;
        let mut live_wake = false;
        if live.is_some() && is_last && scan_from >= end && !closed {
            let wait = params
                .timeout
                .as_deref()
                .and_then(parse_duration)
                .unwrap_or(Duration::from_secs(3))
                .min(MAX_LONG_POLL);
            let deadline = tokio::time::Instant::now() + wait;
            loop {
                let notified = handle.notify.notified();
                let applied_notified = handle.applied_notify.notified();
                let (e2, c2) = {
                    let st = handle.state.lock().unwrap();
                    let e = match deliver {
                        crate::shard::Deliver::Durable => st.durable.next,
                        crate::shard::Deliver::Applied => st.applied.next.max(st.durable.next),
                    };
                    (e, st.durable.closed)
                };
                end = e2;
                if end > scan_from || c2 {
                    live_wake = end > scan_from;
                    break;
                }
                if deliver == crate::shard::Deliver::Applied {
                    tokio::select! {
                        _ = notified => {}
                        _ = applied_notified => {}
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                } else {
                    tokio::select! {
                        _ = notified => {}
                        _ = tokio::time::sleep_until(deadline) => break,
                    }
                }
            }
            if end <= scan_from {
                // Timed out empty: 204 with a rearm token. A mid-wait
                // seal (c2) reports nothing here — the next poll's entry
                // logic classifies it (successor, gap, or genuine close).
                let mut r = Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header(
                        "Stream-Next-Offset",
                        seg_tok(sg.seg_id, scan_from.checked_sub(1)),
                    )
                    .header(header::CACHE_CONTROL, "no-store");
                if deliver == crate::shard::Deliver::Applied {
                    let floor_now = handle.state.lock().unwrap().durable.next;
                    r = r.header(
                        "Stream-Durable-Offset",
                        seg_tok(sg.seg_id, floor_now.min(scan_from).checked_sub(1)),
                    );
                }
                if closed && !seal_gap {
                    r = r.header("Stream-Closed", "true");
                }
                return r.body(Body::empty()).unwrap();
            }
        }

        if head_only {
            let mut r = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, desc.content_type.clone())
                .header("Stream-Next-Offset", seg_tok(sg.seg_id, end.checked_sub(1)))
                .header(header::CACHE_CONTROL, "no-store");
            if closed && is_last && !seal_gap {
                r = r.header("Stream-Closed", "true");
            }
            return r.body(Body::empty()).unwrap();
        }

        let out = match read_records(
            &state,
            &desc,
            &key,
            &epoch,
            &handle,
            &engine,
            scan_from,
            params.key.as_deref(),
            params.max_bytes.unwrap_or(if live_wake {
                tail_max_bytes()
            } else {
                MAX_READ_BYTES
            }),
            deliver,
        )
        .await
        {
            Ok(o) => o,
            Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
        };
        // Clamp progression to this SEGMENT's end (sealed_next), never
        // the raw handle end (a sealed identity's tail may sit past the
        // frozen boundary only if re-opened — defensive).
        let consumed_to = out
            .last
            .map(|o| o + 1)
            .unwrap_or(scan_from)
            .min(seg_end.max(scan_from));
        let drained = out.completed && consumed_to >= seg_end;
        let sealed_mid = sg.sealed_next_offset.is_some() && !is_last;
        let next_token = if drained && sealed_mid {
            // Hand the cursor to the successor.
            let succ = &lineage[pos + 1];
            seg_tok(succ.seg_id, None)
        } else {
            seg_tok(sg.seg_id, consumed_to.checked_sub(1))
        };

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
                buf.extend_from_slice(b"\n");
            }
            buf.freeze()
        };
        let up_to_date = drained && (is_last || !sealed_mid);
        let mut r = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, desc.content_type.clone())
            .header("Stream-Next-Offset", next_token)
            .header(header::CACHE_CONTROL, "no-store")
            .header("Cross-Origin-Resource-Policy", "cross-origin");
        if deliver == crate::shard::Deliver::Applied {
            // Fresh durable floor at response time (one-sided staleness:
            // a record marked pending may already be durable, never the
            // reverse). A drained sealed hop is all-durable by
            // construction, so its durable cursor IS the next token.
            let floor_now = handle.state.lock().unwrap().durable.next;
            let durable_tok = if drained && sealed_mid {
                seg_tok(lineage[pos + 1].seg_id, None)
            } else {
                seg_tok(sg.seg_id, consumed_to.min(floor_now).checked_sub(1))
            };
            r = r.header("Stream-Durable-Offset", durable_tok);
            if let Some(i) = out.recs.iter().position(|rec| rec.off >= floor_now) {
                r = r.header("Stream-Pending-From", i.to_string());
            }
        }
        if up_to_date && !seal_gap {
            r = r.header("Stream-Up-To-Date", "true");
        }
        if closed && is_last && drained && !seal_gap {
            r = r.header("Stream-Closed", "true");
        }
        return r.body(Body::from(body)).unwrap();
    }
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

// ---- internal metrics stream flusher (old-impl pattern: __stream_metrics__) ----

#[cfg(test)]
mod tests {

    // Round-19 fleet-contract: hierarchical names with characters that
    // are structural in a URL must survive a relay intact.
    #[test]
    fn stream_names_encode_for_peer_paths() {
        assert_eq!(
            encode_stream_name_path("customers/acme/orders"),
            "customers/acme/orders",
            "the hierarchy separator must survive"
        );
        assert_eq!(encode_stream_name_path("a?b"), "a%3Fb");
        assert_eq!(encode_stream_name_path("a#b"), "a%23b");
        assert_eq!(encode_stream_name_path("a%b"), "a%25b");
        assert_eq!(encode_stream_name_path("a b"), "a%20b");
        // UTF-8 is encoded byte-wise.
        assert_eq!(encode_stream_name_path("é"), "%C3%A9");
    }
    use super::*;

    #[test]
    fn per_stream_slots_cap_and_release() {
        let mut m: HashMap<[u8; 16], i64> = HashMap::new();
        let h = [1u8; 16];
        // cap 0 = limiter off
        assert_eq!(stream_slot_try(&mut m, 0, h), SlotTry::Untracked);
        assert!(m.is_empty());
        // acquire to cap, then reject
        assert_eq!(stream_slot_try(&mut m, 2, h), SlotTry::Acquired);
        assert_eq!(stream_slot_try(&mut m, 2, h), SlotTry::Acquired);
        assert_eq!(stream_slot_try(&mut m, 2, h), SlotTry::AtCap);
        // a different stream is unaffected
        assert_eq!(stream_slot_try(&mut m, 2, [2u8; 16]), SlotTry::Acquired);
        // release frees a slot and empties the entry at zero
        stream_slot_release(&mut m, &h);
        assert_eq!(stream_slot_try(&mut m, 2, h), SlotTry::Acquired);
        stream_slot_release(&mut m, &h);
        stream_slot_release(&mut m, &h);
        assert!(!m.contains_key(&h), "zero-count entry must be removed");
    }
}
