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

const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
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
    pub data_store: Arc<dyn ObjectStore>,
    pub keys: Arc<KeyCache>,
    pub touch: Arc<crate::touch::TouchRegistry>,
    /// Conformance/dev accommodation: used when a request carries no
    /// Stream-Encryption-Key header (the upstream conformance suite cannot
    /// send custom headers). Never set in production.
    pub default_key: Option<String>,
    /// Bearer token required on /v1/* when set (pilot authn).
    pub auth_token: Option<String>,
    pub metrics: Arc<crate::metrics::Metrics>,
}

pub(crate) fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    match &state.auth_token {
        None => true,
        Some(t) => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.strip_prefix("Bearer ").map(|x| x == t).unwrap_or(false))
            .unwrap_or(false),
    }
}

impl AppState {
    /// Shard engine for `hash`, opening the shard log on first use (which
    /// fences any previous owner). A shard that was just fenced away is
    /// held off for 3 s (anti-flap while the router converges) → 503.
    /// Response-free engine lookup for the unified scaler.
    pub async fn engine_for_scaler(self: &Arc<Self>, hash: &[u8; 16]) -> Option<Arc<ShardEngine>> {
        self.engine_for(hash).await.ok()
    }

    pub(crate) async fn engine_for(
        self: &Arc<Self>,
        hash: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, Response> {
        let prefix = shard_for_hash(&self.shard_prefixes, hash);
        if let Some(e) = self.shards.read().unwrap().get(&prefix) {
            return Ok(e.clone());
        }
        // R2/R3: only the ring owner may claim a shard. A stale router can
        // still send us one — answer 409 + Streams-Replay-To so the router
        // corrects itself, instead of fencing the rightful owner.
        if let Some(owner) = self.effective_owner(&prefix) {
            if owner != self.instance_name {
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
            crate::sharddir::OpenOutcome::Ready(engine) => Ok(engine),
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
        if let Some(t) = self.ring_overrides.read().unwrap().get(prefix) {
            if active.iter().any(|a| a == t) {
                return Some(t.clone());
            }
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
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "2"), ("content-type", "application/json")],
            r#"{"error":{"code":"overloaded","message":"instance memory pressure; retry"}}"#,
        )
            .into_response();
    }
    next.run(req).await
}

/// Calibrated-latency endpoint for edge probes: holds the request for
/// ?ms= milliseconds doing no engine work. Lets a probe separate an
/// admitted-concurrency cap (rate = slots/latency) from a rate cap
/// (rate constant regardless of latency).
async fn debug_sleep(
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
    let desc = match state.registry.get(&name).await {
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

async fn debug_load(State(state): State<Arc<AppState>>) -> Response {
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
    axum::Json(serde_json::json!({
        "inflight_now": now,
        "inflight_peak": peak,
        "rss_mb": crate::fleet::rss_bytes() as f64 / 1048576.0,
        "admit_shed": state.admit_shed.load(std::sync::atomic::Ordering::Relaxed),
        "stream_shed": state.stream_shed.load(std::sync::atomic::Ordering::Relaxed),
        "wedge_shed": state.wedge_shed.load(std::sync::atomic::Ordering::Relaxed),
        "streams_tracked": state.stream_inflight.lock().unwrap().len(),
        "absorb_lag_max_secs": crate::usage::absorb_lag_max(),
        "cardinality": {
            "resident_handles": resident_handles,
            "usage_tracked": crate::usage::tracked_streams(),
            "keycache": state.keys.len(),
            "registry_cache": state.registry.cache_len(),
            "metrics": state.metrics.len(),
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
    if let Some(obj) = snap.as_object_mut() {
        // History DbReader service: hits vs misses shows how much
        // per-request manifest traffic the cache absorbs; stale_reopens
        // is bounded by absorb cadence; coalesced proves single-flight.
    }
    axum::Json(snap).into_response()
}

/// Per-stream usage counters + the active limits. Auth: same bearer as
/// the other debug endpoints (enforced by the middleware layer).
async fn debug_usage() -> Response {
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
    let (eligible, oldest_eligible, deferred, deferred_bytes) =
        crate::usage::absorb_pending_summary();
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
        // The interim sparse policy's ledger: streams intentionally held
        // in the shard log (pending < ABSORB_MIN_BYTES_FOR_AGE), NOT lag.
        "deferred_sparse": { "streams": deferred, "bytes": deferred_bytes },
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

/// Billing emitter: every BILLING_INTERVAL_SECS, append one JSON-array
/// record batch to the internal billing stream (BILLING_STREAM, default
/// "_billing") — one record per active stream with the DELTAS since the
/// last emission: requests, records, bytes_in, bytes_out, plus cumulative
/// plaintext_bytes/frame_bytes (stored volume pre-compression and the
/// achieved compression rate are derivable from these). Disabled with a
/// warning when BILLING_STREAM_KEY is unset. The billing stream's own
/// usage is excluded to avoid self-feedback.
/// Server-internal segment seal: enqueue a close-only commit (no key
/// material needed — close writes tail state only) and return the frozen
pub fn spawn_billing(state: Arc<AppState>) {
    let Ok(key) = std::env::var("BILLING_STREAM_KEY") else {
        tracing::warn!("BILLING_STREAM_KEY unset; billing emitter disabled");
        return;
    };
    let name = std::env::var("BILLING_STREAM").unwrap_or_else(|_| "_billing".into());
    let interval = std::env::var("BILLING_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60u64);
    tokio::spawn(async move {
        let self_hash = crate::crypto::stream_hash(&name);
        let mut hdrs = HeaderMap::new();
        if let Ok(v) = axum::http::HeaderValue::from_str(&key) {
            hdrs.insert("stream-encryption-key", v);
        }
        hdrs.insert(
            "content-type",
            axum::http::HeaderValue::from_static("application/json"),
        );
        // Idempotent create (409/conflict is fine on an existing stream).
        let _ = create_stream(state.clone(), name.clone(), hdrs.clone(), Bytes::new()).await;
        // POSTURE (static audit): this emitter is best-effort usage
        // telemetry, not a production billing system of record — that
        // needs a durable outbox/ledger (deltas persisted transactionally
        // with an ack cursor). Known accepted gaps until then: overflow-
        // aggregate traffic (past the tracked-stream cap) has no
        // per-stream attribution and is never emitted here (visible via
        // /v1/debug/usage only), and checkpoints live in process memory —
        // a restart re-bills current cumulative values. What the emitter
        // DOES guarantee: no interval is dropped (checkpoints advance
        // only after the append succeeds), evict-and-return incarnations
        // are told apart by counter generation instead of value
        // regression (which missed regrow-past-checkpoint and
        // under-billed), and checkpoint memory does not grow with every
        // stream ever seen.
        let mut prev: BillingCheckpoints = std::collections::HashMap::new();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            let now_ms = crate::shard::now_ms();
            let mut recs: Vec<serde_json::Value> = Vec::new();
            let mut staged: Vec<([u8; 16], (u64, (u64, u64, u64, u64)))> = Vec::new();
            let mut seen: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();
            for (h, cgen, req, rec, bi, bo, pt, fr) in crate::usage::snapshot() {
                if h == self_hash {
                    continue;
                }
                seen.insert(h);
                let d = billing_delta(&prev, &h, cgen, req, rec, bi, bo);
                staged.push((h, (cgen, (req, rec, bi, bo))));
                if d == (0, 0, 0, 0) {
                    continue;
                }
                recs.push(serde_json::json!({
                    "ts": now_ms,
                    "stream": crate::crypto::hex(&h),
                    "requests": d.0,
                    "records": d.1,
                    "bytes_in": d.2,
                    "bytes_out": d.3,
                    "plaintext_bytes_total": pt,
                    "frame_bytes_total": fr,
                }));
            }
            if recs.is_empty() {
                // Still advance checkpoints for streams whose counters
                // moved without billable deltas, and drop checkpoints for
                // evicted streams (nothing outstanding to lose — their
                // next incarnation carries a fresh generation anyway).
                for (h, cur) in staged {
                    prev.insert(h, cur);
                }
                prev.retain(|h, _| seen.contains(h));
                continue;
            }
            let body = serde_json::to_vec(&recs).unwrap_or_default();
            let resp = append(
                state.clone(),
                name.clone(),
                hdrs.clone(),
                Body::from(body),
                None,
                None,
                None,
            )
            .await;
            if resp.status().is_success() {
                for (h, cur) in staged {
                    prev.insert(h, cur);
                }
                // Checkpoint hygiene: entries for streams no longer in the
                // snapshot are dead weight (their counters object is gone;
                // a returning stream gets a new generation). Only after a
                // SUCCESSFUL emit — an evicted-mid-failure stream keeps
                // nothing outstanding here by construction (its row was in
                // this emit or a previous one).
                prev.retain(|h, _| seen.contains(h));
            } else {
                tracing::warn!(
                    status = %resp.status(),
                    "billing emit failed; interval delta retained for retry"
                );
            }
        }
    });
}

/// stream → (counter generation, cumulative checkpoint) for the emitter.
type BillingCheckpoints = std::collections::HashMap<[u8; 16], (u64, (u64, u64, u64, u64))>;

/// One stream's billable delta against its checkpoint. Same generation
/// and monotonic counters → plain difference. A DIFFERENT generation
/// means the tracked entry was evicted and re-created: bill the fresh
/// cumulative in full — value-regression detection alone missed the
/// evict → return → regrow-past-checkpoint case (old checkpoint 10,
/// new incarnation already at 20 reads as "delta 10" when the truth is
/// 20). In-generation regression cannot happen (counters only grow),
/// but is handled the same way defensively.
fn billing_delta(
    prev: &BillingCheckpoints,
    h: &[u8; 16],
    generation: u64,
    req: u64,
    rec: u64,
    bi: u64,
    bo: u64,
) -> (u64, u64, u64, u64) {
    match prev.get(h) {
        Some((pgen, p))
            if *pgen == generation && req >= p.0 && rec >= p.1 && bi >= p.2 && bo >= p.3 =>
        {
            (req - p.0, rec - p.1, bi - p.2, bo - p.3)
        }
        Some(_) => (req, rec, bi, bo),
        None => (req, rec, bi, bo),
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/segments/{*name}", get(get_segments))
        .route("/v1/debug/timings", get(debug_timings))
        .route("/v1/debug/load", get(debug_load))
        .route("/v1/debug/store", get(debug_store))
        .route("/v1/debug/usage", get(debug_usage))
        .route(
            "/v1/debug/absorb-pause",
            post(
                |Query(q): Query<std::collections::HashMap<String, String>>| async move {
                    let on = q.get("on").map(|v| v == "1").unwrap_or(false);
                    crate::history::absorb_pause_flag()
                        .store(on, std::sync::atomic::Ordering::Relaxed);
                    axum::Json(serde_json::json!({"absorb_paused": on}))
                },
            ),
        )
        .route("/v1/debug/sleep", get(debug_sleep))
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
        .route("/v1/stream/{*name}", any(stream_entry))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            track_inflight,
        ))
        .layer(axum::middleware::map_response(|mut resp: Response| async {
            resp.headers_mut().insert(
                "x-content-type-options",
                axum::http::HeaderValue::from_static("nosniff"),
            );
            resp
        }))
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

fn err_resp(status: StatusCode, code: &str, message: &str) -> Response {
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
    if let Some(r) =
        crate::product::product_auth_gate(&state, &name, &method, &query, &headers)
    {
        return crate::product::with_product_cors(r);
    }
    let body = match axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES).await {
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
    match method {
        Method::PUT => {
            let body = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
                Ok(b) => b,
                Err(_) => {
                    return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large");
                }
            };
            create_stream(state, name, headers, body).await
        }
        Method::POST => append(state, name, headers, body, None, None, None).await,
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
                    let d = match state_reg.registry.get(&src).await {
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
        let part = read_merged(key, epoch, &handle, &engine, cursor, Some(""), budget).await?;
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
            cursor = cursor.max(cap.min(u64::MAX));
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

fn rand_epoch() -> [u8; 16] {
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
    tokio::spawn(async move {
        // cas_update is single-shot; a slide can race another descriptor
        // write (e.g. the close path's seal) — retry the benign conflict.
        if let Err(e) = state
            .registry
            .cas_update_retry(&name, |d| {
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
        state.registry.invalidate(&name);
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
    let _ = state;
    let epoch = rand_epoch();
    StreamDesc {
        name: name.to_string(),
        stream_epoch: hex(&epoch),
        key_fingerprint: key.fingerprint(&epoch),
        created_ms: now_ms(),
        expires_at_ms: ttl_secs
            .map(|t| now_ms() + (t as i64) * 1000)
            .or(expires_at_ms),
        deleted: false,
        soft_deleted: false,
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
    let prefix = shard_for_hash(&state.shard_prefixes, &crate::crypto::stream_hash(name));
    if let Some(owner) = state.effective_owner(&prefix) {
        if owner != state.instance_name {
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
    }
    None
}

/// Product DELETE maps to the one collection-delete implementation.
pub(crate) async fn product_delete(state: Arc<AppState>, name: String) -> Response {
    delete_stream(state, name).await
}

async fn create_stream(
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
        let prefix = shard_for_hash(&state.shard_prefixes, &crate::crypto::stream_hash(&name));
        if let Some(owner) = state.effective_owner(&prefix) {
            if owner != state.instance_name {
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
        let resuming_child = match state.registry.get(&name).await {
            Ok(Some(c)) if !c.deleted => c
                .forked_from
                .clone()
                .filter(|f| f.source == src_name && !f.fork_id.is_empty()),
            _ => None,
        };
        let src = match state.registry.get(&src_name).await {
            Ok(Some(d)) if desc_alive(&d) => d,
            // Retained for this very child: same incarnation, and the
            // reference this child installed is still on it.
            Ok(Some(d))
                if !d.deleted
                    && resuming_child.as_ref().is_some_and(|f| {
                        f.source_epoch == d.stream_epoch
                            && d.fork_children.contains(&f.fork_id)
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
    let existing = match state.registry.get(&name).await {
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
    if let Some(d) = existing.as_ref() {
        if let Some(init) = &d.init {
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
                        return err_resp(
                            StatusCode::FORBIDDEN,
                            "wrong_key",
                            "key mismatch",
                        );
                    }
                }
                // Belt and braces: the initialization identity itself
                // records which key it was claimed for.
                if !init.key_fingerprint.is_empty()
                    && init.key_fingerprint != d.key_fingerprint
                {
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
    }

    let (created, desc) = match existing {
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
                .recreate(&name, fresh, |d| !desc_alive(d) && !d.soft_deleted)
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
        .engine_for(&crate::crypto::stream_hash(&desc.name))
        .await
    {
        Ok(e) => e,
        Err(r) => return r,
    };

    // Fork post-create (pinned DS fork contract): the tail row must be
    // seeded at the fork boundary BEFORE the first handle load caches
    // next = 0, and the source's reference count records this fork.
    let mut materialize_entry: Option<Bytes> = None;
    if let Some(fc) = &fork_ctx {
        if created {
            if let Err(e) = engine
                .seed_fork_tail(hash, crate::crypto::stream_hash(&desc.name), fc.boundary)
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
                    .cas_update_incarnation(&name, &desc.stream_epoch, |d| match d
                        .forked_from
                        .as_mut()
                    {
                        Some(f) if f.fork_id.is_empty() => {
                            f.fork_id = fid.clone();
                            true
                        }
                        Some(f) => {
                            already = f.fork_id == fid;
                            false
                        }
                        None => false,
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
                state.registry.invalidate(&name);
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
            }
            match state
                .registry
                .cas_update_retry(&fc.source, |d| {
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
            state.registry.invalidate(&fc.source);
            match state.registry.get(&fc.source).await {
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
        let (tx, rx) = oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: crate::crypto::stream_hash(&desc.name),
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
            usage: crate::usage::counters(&crate::crypto::stream_hash(&desc.name)),
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
            .cas_update_incarnation(&name, &desc.stream_epoch, |d| {
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
        state.registry.invalidate(&name);
        // A declined CAS is NOT readiness. `cas_update` refuses a
        // deleted descriptor, so a delete that won mid-initialization
        // made this return 201 for a stream that no longer exists — and
        // if the work had already installed a fork reference, the source
        // stayed pinned by a child that was never published.
        if !published {
            let now = state.registry.get(&name).await.ok().flatten();
            let live_and_ready = now
                .as_ref()
                .is_some_and(|d| desc_alive(d) && d.init.is_none() && d.stream_epoch == desc.stream_epoch);
            if !live_and_ready {
                // Compensate: give back the source reference this
                // initialization installed, so the parent is not held by
                // a child that will never exist.
                if let Some(fr) = desc.forked_from.as_ref().filter(|f| !f.fork_id.is_empty()) {
                    if let Err(m) = release_fork_ref(&state, &fr.source, &fr.fork_id).await {
                        tracing::error!(stream = %name, "releasing an abandoned fork claim: {m}");
                    }
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
    let existing = match state.registry.get(&name).await {
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
        if d.deleted {
            if let Err(e) = delete_lifecycle(&state, &name).await {
                return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &e);
            }
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
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Every failpoint holds a SET of stream names, never a single slot.
    // Two things went wrong with one slot: a global flag tripped
    // unrelated tests (an ordinary close-with-content walking into
    // another test's armed abort), and then — once they were scoped by
    // name — a second test arming the SAME failpoint for a DIFFERENT
    // stream silently disarmed the first, so its request sailed through
    // the window it was supposed to be parked in. Arming and releasing
    // are per name, so a parallel suite composes.
    fn armed(which: usize) -> &'static Mutex<Option<HashSet<String>>> {
        static M: [Mutex<Option<HashSet<String>>>; 6] = [
            Mutex::new(None),
            Mutex::new(None),
            Mutex::new(None),
            Mutex::new(None),
            Mutex::new(None),
            Mutex::new(None),
        ];
        &M[which]
    }
    const TOMBSTONE: usize = 0;
    const MARK: usize = 1;
    const READY: usize = 2;
    const APPEND: usize = 3;
    const DELETE: usize = 4;
    const SEAL_INTENT: usize = 5;

    fn set(which: usize, name: Option<&str>) {
        let mut g = armed(which).lock().unwrap();
        match name {
            Some(n) => {
                g.get_or_insert_with(HashSet::new).insert(n.to_string());
            }
            // `None` releases only what this process armed for the
            // caller's own name in practice; tests that need surgical
            // release call `release`.
            None => *g = None,
        }
        drop(g);
        if name.is_none() {
            gate().notify_waiters();
        }
    }

    fn release(which: usize, name: &str) {
        if let Some(s) = armed(which).lock().unwrap().as_mut() {
            s.remove(name);
        }
        gate().notify_waiters();
    }

    fn is_armed(which: usize, name: &str) -> bool {
        armed(which)
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.contains(name))
    }

    fn gate() -> &'static tokio::sync::Notify {
        static N: std::sync::OnceLock<tokio::sync::Notify> = std::sync::OnceLock::new();
        N.get_or_init(tokio::sync::Notify::new)
    }

    /// Wait until this failpoint is released for `name`, counting the
    /// arrival exactly once so a test can OBSERVE that the request
    /// really is in the window instead of sleeping and hoping.
    async fn park(which: usize, name: &str, counter: &'static AtomicUsize) {
        let mut counted = false;
        loop {
            if !is_armed(which, name) {
                return;
            }
            if !counted {
                counted = true;
                counter.fetch_add(1, Ordering::SeqCst);
            }
            let n = gate().notified();
            if !is_armed(which, name) {
                return;
            }
            n.await;
        }
    }

    static PARKED_APPEND: AtomicUsize = AtomicUsize::new(0);
    static PARKED_CREATE: AtomicUsize = AtomicUsize::new(0);
    static PARKED_DELETE: AtomicUsize = AtomicUsize::new(0);

    /// Abort the cascade right after the named generation is tombstoned
    /// and its debt recorded, before the parent reference is released.
    pub fn stop_after_tombstone(name: Option<&str>) {
        set(TOMBSTONE, name);
    }

    /// Drop a raw close on the named stream AFTER its records are
    /// durable and the segment is closed, but before the transition is
    /// marked committed.
    pub fn stop_before_mark_committed(name: Option<&str>) {
        set(MARK, name);
    }

    /// Drop a raw close right after its Sealing intent is durable and
    /// BEFORE the records are appended — the crash boundary an ordinary
    /// retry has to recover from.
    pub fn stop_after_seal_intent(name: Option<&str>) {
        set(SEAL_INTENT, name);
    }

    pub(super) fn should_stop_after_seal_intent(name: &str) -> bool {
        is_armed(SEAL_INTENT, name)
    }

    pub fn stop_after_tombstone_off(name: &str) {
        release(TOMBSTONE, name);
    }

    pub fn stop_before_mark_committed_off(name: &str) {
        release(MARK, name);
    }

    pub fn stop_after_seal_intent_off(name: &str) {
        release(SEAL_INTENT, name);
    }

    pub(super) fn should_stop_after_tombstone(name: &str) -> bool {
        is_armed(TOMBSTONE, name)
    }

    pub(super) fn should_stop_before_mark_committed(name: &str) -> bool {
        is_armed(MARK, name)
    }

    /// Park a creation just before it publishes readiness — after its
    /// fork reference is installed — so a delete can be made to win
    /// that window deterministically.
    pub fn park_create_before_ready(name: Option<&str>) {
        set(READY, name);
    }

    /// Release the creation park for ONE name, leaving other tests'
    /// arms intact.
    pub fn release_create_before_ready(name: &str) {
        release(READY, name);
    }

    pub fn parked_create_count() -> usize {
        PARKED_CREATE.load(Ordering::SeqCst)
    }

    pub(super) async fn pause_create_before_ready(name: &str) {
        park(READY, name, &PARKED_CREATE).await;
    }

    /// Park an ordinary append AFTER admission — its lifecycle verdict
    /// already decided against an OPEN descriptor — and before it is
    /// enqueued. That is the window in which a concurrent close can
    /// publish its seal intent and reach the committer first, so its
    /// producer sequence observes a gap the parked predecessor is about
    /// to fill.
    pub fn park_append_before_enqueue(name: Option<&str>) {
        set(APPEND, name);
    }

    pub fn release_append_before_enqueue(name: &str) {
        release(APPEND, name);
    }

    pub fn parked_append_count() -> usize {
        PARKED_APPEND.load(Ordering::SeqCst)
    }

    pub(super) async fn pause_append_before_enqueue(name: &str) {
        park(APPEND, name, &PARKED_APPEND).await;
    }

    /// Park a delete just before it decides soft-versus-hard, so a
    /// concurrent fork installation can be made to win DETERMINISTICALLY
    /// rather than by racing timers.
    pub fn park_delete_before_decision(name: Option<&str>) {
        set(DELETE, name);
    }

    pub fn release_delete_before_decision(name: &str) {
        release(DELETE, name);
    }

    pub fn parked_delete_count() -> usize {
        PARKED_DELETE.load(Ordering::SeqCst)
    }

    pub(super) async fn pause_delete_before_decision(name: &str) {
        park(DELETE, name, &PARKED_DELETE).await;
    }
}

fn release_fork_ref(
    state: &Arc<AppState>,
    src: &str,
    fork_id: &str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
    let state = state.clone();
    let src = src.to_string();
    let fork_id = fork_id.to_string();
    Box::pin(async move {
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
        if let Some(cur) = state.registry.get(&src).await.map_err(|e| e.to_string())? {
            if cur.deleted && cur.parent_ref_pending {
                if let Some(gp) = cur.forked_from.as_ref() {
                    release_fork_ref(&state, &gp.source, &gp.fork_id).await?;
                }
                state
                    .registry
                    .update(&src, |x| x.parent_ref_pending = false)
                    .await
                    .map_err(|e| e.to_string())?;
                state.registry.invalidate(&src);
                return Ok(());
            }
        }
        // Release the reference AND decide the source's fate in one CAS,
        // against the children it has at that instant. Splitting the two
        // let a new fork install itself in between and then be orphaned
        // by an unconditional tombstone.
        let mut tombstoned = false;
        state
            .registry
            .cas_update_retry(&src, |x| {
                let before = x.fork_children.len();
                x.fork_children.retain(|c| c != &fork_id);
                let removed = x.fork_children.len() != before;
                let expired = x.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false);
                let should_tombstone =
                    x.fork_children.is_empty() && (x.soft_deleted || expired) && !x.deleted;
                if should_tombstone {
                    x.soft_deleted = false;
                    x.deleted = true;
                    x.parent_ref_pending = x.forked_from.is_some();
                    tombstoned = true;
                }
                removed || should_tombstone
            })
            .await
            .map_err(|e| e.to_string())?;
        state.registry.invalidate(&src);
        #[cfg(test)]
        if tombstoned && fork_failpoints::should_stop_after_tombstone(&src) {
            // "Crash" here: the tombstone and its debt are durable, the
            // recursive release has not run.
            return Ok(());
        }
        if tombstoned {
            if let Some(after) = state.registry.get(&src).await.map_err(|e| e.to_string())? {
                if let Some(gf) = after.forked_from.as_ref() {
                    release_fork_ref(&state, &gf.source, &gf.fork_id).await?;
                    state
                        .registry
                        .update(&src, |x| x.parent_ref_pending = false)
                        .await
                        .map_err(|e| e.to_string())?;
                    state.registry.invalidate(&src);
                }
            }
        }
        Ok(())
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
        let d = match state.registry.get(&name).await.map_err(|e| e.to_string())? {
            Some(d) => d,
            None => return Ok(()),
        };
        let parent = d
            .forked_from
            .as_ref()
            .map(|f| (f.source.clone(), f.fork_id.clone()));
        // Already a tombstone with an unpaid debt: just pay it. This is
        // also how a crashed CASCADE is repaired — an intermediate
        // generation that was tombstoned but never released its own
        // parent is reachable by deleting it again.
        if d.deleted {
            if d.parent_ref_pending {
                if let Some((src, fid)) = parent.clone() {
                    release_fork_ref(&state, &src, &fid).await?;
                    state
                        .registry
                        .update(&name, |x| x.parent_ref_pending = false)
                        .await
                        .map_err(|e| e.to_string())?;
                    state.registry.invalidate(&name);
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
                let Some(anc) = state.registry.get(&f.source).await.map_err(|e| e.to_string())?
                else {
                    break;
                };
                if !(anc.deleted && anc.parent_ref_pending) {
                    break;
                }
                if let Some(gp) = anc.forked_from.as_ref() {
                    release_fork_ref(&state, &gp.source, &gp.fork_id).await?;
                }
                state
                    .registry
                    .update(&f.source, |x| x.parent_ref_pending = false)
                    .await
                    .map_err(|e| e.to_string())?;
                state.registry.invalidate(&f.source);
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
        state
            .registry
            .cas_update_incarnation(&name, &epoch, |x| {
                if x.deleted {
                    return false;
                }
                if !x.fork_children.is_empty() {
                    x.soft_deleted = true;
                    hard_deleted = false;
                } else {
                    x.deleted = true;
                    x.parent_ref_pending = x.forked_from.is_some();
                    hard_deleted = true;
                }
                true
            })
            .await
            .map_err(|e| e.to_string())?;
        state.registry.invalidate(&name);
        if !hard_deleted {
            return Ok(());
        }
        if let Some((src, fid)) = parent {
            release_fork_ref(&state, &src, &fid).await?;
            // Released: the tombstone owes nothing more. This is
            // `update`, not `cas_update`, because the descriptor is
            // already deleted and CAS refuses tombstones by design —
            // which is exactly why the debt has to be recorded ON the
            // tombstone and cleared this way.
            state
                .registry
                .update(&name, |x| x.parent_ref_pending = false)
                .await
                .map_err(|e| e.to_string())?;
            state.registry.invalidate(&name);
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
    seal_auth: Option<String>,
) -> Response {
    let wrapped = matches!(
        state.registry.get(&name).await,
        Ok(Some(d)) if d
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
    );
    if !wrapped {
        return append_core(state, name, headers, body, product_hash, product_key, seal_auth).await;
    }
    let body_bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
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
        state.registry.invalidate(&name);
        let Ok(Some(d)) = state.registry.get(&name).await else {
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

async fn append_core(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Body,
    product_hash: Option<[u8; 16]>,
    product_key: Option<String>,
    seal_auth: Option<String>,
) -> Response {
    // Scaled-stream routing (SCALING.md): a parent stream with scaling on
    // never takes appends itself — the routing key maps through the
    // segment map to an internal child stream "<parent>#<seg_id>". The
    // child is sealed (closed) during a split/merge transition; the retry
    // loop refreshes the map and follows the successor, so clients never
    // observe the transition beyond a few ms of latency.
    // (LEGACY path, pre-v3 descriptors only; unified-model streams
    // resolve segments in-process below — docs/ROUTING-V3.md §2.)
    let mut desc = match state.registry.get(&name).await {
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
    let body = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
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
        crate::product::seal_op_id_semantic(
            &create_request_hash(&desc.content_type, None, None, true, &body, None),
            &product_key.clone().unwrap_or_default(),
            &[
                hv("producer-id"),
                hv("producer-epoch"),
                hv("producer-seq"),
                hv("stream-seq"),
                hv("stream-timestamp"),
            ],
        )
    };
    // Owed-final authorization: either this request IS the intent's
    // record (computed identity matches) or an internal caller passed
    // the trusted operation id. Nothing a client sends can assert it.
    let is_owed_final = desc.sealing.as_ref().is_some_and(|sl| {
        sl.owes_final()
            && (sl.operation_id == this_close_op
                || Some(&sl.operation_id) == seal_auth.as_ref())
    });

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
            id: format!("{}rawseal.{this_close_op}", crate::shard::INTERNAL_PRODUCER_PREFIX),
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
    let sealed_reject_new = if (desc.sealed || desc.sealing.is_some())
        && !close_only
        && !is_owed_final
    {
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
    if close && !desc.sealed && !is_owed_final && deferred.is_none() {
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
        if let Err(e) = crate::product::begin_sealing_for_close(&state, &name, intent).await {
            return err_resp(StatusCode::CONFLICT, "sealed", &e);
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
    let name_hash = crate::crypto::stream_hash(&desc.name);
    let usage_c = if !close_only && deferred.is_none() {
        match crate::usage::admit_append(&name_hash, body.len() as u64, entries.len() as u64) {
            Err(hit) => {
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
        state.registry.invalidate(&name);
        match state.registry.get(&name).await {
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
        crate::crypto::RouteHash::of(&desc.name),
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
        let journal = state
            .touch
            .journal(desc.storage_hash(), &crate::product::watch_pinned(&desc));
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
    let metric_bytes = bytes as u64;
    #[cfg(test)]
    if !close {
        fork_failpoints::pause_append_before_enqueue(&name).await;
    }
    let (tx, rx) = oneshot::channel();
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
        resp: tx,
    };
    let engine = match state.engine_for(&seg.shard_route).await {
        Ok(e) => e,
        Err(r) => return r,
    };
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
        let definitive = matches!(
            e,
            AppendErr::ProducerSeqReused
                | AppendErr::CtMismatch
                | AppendErr::BadBody(_)
                | AppendErr::SeqConflict { .. }
        );
        if close && close_carries_content && definitive {
            if let Err(m) =
                crate::product::abandon_seal_intent(&state, &name, &this_close_op).await
            {
                tracing::error!(stream = %name, "abandoning a refused raw close intent: {m}");
            }
        }
    }
    match outcome {
        Ok(ack) => {
            if !ack.duplicate {
                state.metrics.append(&name, metric_bytes);
            }
            touch_ttl(&state, &desc); // writes slide the idle window
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
                let owns_final = is_owed_final || close_carries_content;
                #[cfg(test)]
                if owns_final && fork_failpoints::should_stop_before_mark_committed(&name) {
                    return err_resp(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "seal_incomplete",
                        "failpoint: stopped before marking the final durable",
                    );
                }
                if owns_final {
                    if let Err(e) =
                        crate::product::mark_final_committed(&state, &name, &this_close_op).await
                    {
                        tracing::error!(stream = %name, "marking the close's final durable: {e}");
                    }
                }
                let op = owns_final.then(|| this_close_op.clone());
                if let Err(e) = crate::product::run_seal(&state, &name, op).await {
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
    state: &AppState,
    _desc: &StreamDesc,
    key: &StreamKey,
    epoch: &[u8; 16],
    handle: &Arc<crate::shard::StreamHandle>,
    engine: &Arc<ShardEngine>,
    scan_from: u64,
    key_filter: Option<&str>,
    max_bytes: usize,
) -> Result<ReadOut, String> {
    read_merged(key, epoch, handle, engine, scan_from, key_filter, max_bytes).await
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
) -> Result<ReadOut, String> {
    // The sub-stream identity (AAD + history-DB path): for total-order
    // streams this is the incarnation hash; for per-key streams, the
    // segment hash. Either way it's the handle's identity.
    let hash = handle.hash;
    let (absorbed, end, hist_v2, route) = {
        let st = handle.state.lock().unwrap();
        (
            st.durable.absorbed,
            st.durable.next,
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
        let part = read_frames(engine, handle, cursor, key_filter, budget)
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
            let durable = engine
                .durable_absorbed(&hash)
                .await
                .map_err(|e| e.to_string())?;
            (durable > cursor).then_some(durable)
        };
        if let Some(durable) = raced_boundary {
            if durable > boundary {
                boundary = durable;
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
            state.metrics.read(&desc.name, 0);
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
    state.metrics.read(&desc.name, body.len() as u64);
    let closed_now = handle.state.lock().unwrap().durable.closed;
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
    state.registry.invalidate(name);
    match state.registry.get(name).await {
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
    let desc = match state.registry.get(&name).await {
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
        .engine_for(&crate::crypto::stream_hash(&desc.name))
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
    let (mut end, mut closed) = {
        let st = handle.state.lock().unwrap();
        (st.durable.next, st.durable.closed)
    };

    if head_only {
        if closed && !genuine_closure(&state, &name, may_refresh).await {
            return Box::pin(read_inner(
                state, name, params, headers, head_only, false, surface,
            ))
            .await;
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
                let (e2, c2) = {
                    let st = handle.state.lock().unwrap();
                    (st.durable.next, st.durable.closed)
                };
                end = e2;
                closed = c2;
                if end > scan_from || closed {
                    live_wake = end > scan_from;
                    wake_us = t_arm.elapsed().as_micros() as u64;
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep_until(deadline) => break,
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
            state.metrics.read(&name, 0);
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
    if let Some(inm) = hdr(&headers, "if-none-match") {
        if inm == etag {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("ETag", etag)
                .body(Body::empty())
                .unwrap();
        }
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
    state.metrics.read(&name, body.len() as u64);
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
    if debug_timing() {
        r = r.header(
            "Streams-Debug-Wait",
            format!(
                "waited={} arm_us={} read_us={}",
                live_wake as u8, wake_us, read_us
            ),
        );
    }
    crate::usage::counters(&crate::crypto::stream_hash(&desc.name))
        .bytes_out
        .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
    let sse_hash = crate::crypto::stream_hash(&desc.name);
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
                            for (i, r) in out.recs.iter().enumerate() {
                                let ev = sse_data_event(&desc, &r.payload);
                                if tx.send(Ok(Bytes::from(ev))).await.is_err() {
                                    return;
                                }
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
    let sse_hash = crate::crypto::stream_hash(&desc.name);
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
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
                        for (i, r) in out.recs.iter().enumerate() {
                            let ev = sse_data_event(&desc, &r.payload);
                            if tx.send(Ok(Bytes::from(ev))).await.is_err() {
                                return;
                            }
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
                    state.registry.invalidate(&desc.name);
                    let fresh = match state.registry.get(&desc.name).await {
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
    loop {
        let sg = &lineage[pos];
        let identity = desc.dynamic_segment_identity(sg.seg_id);
        // Each segment lives on ITS OWN shard route (split children get
        // real routes — review blocker 1); hard-coding the parent route
        // here read an empty keyspace on the wrong engine for any moved
        // child.
        let engine = match state.engine_for(&desc.segment_route(sg)).await {
            Ok(e) => e,
            Err(r) => return r,
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
        let (durable_next, closed) = {
            let st = handle.state.lock().unwrap();
            (st.durable.next, st.durable.closed)
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
            state.registry.invalidate(&desc.name);
            if let Ok(Some(fresh)) = state.registry.get(&desc.name).await {
                if desc_alive(&fresh) {
                    return Box::pin(read_v3_lineage_inner(
                        state, fresh, params, headers, head_only, false, surface,
                    ))
                    .await;
                }
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
        let seg_end = sg.sealed_next_offset.unwrap_or(durable_next);
        if scan_from == u64::MAX {
            scan_from = seg_end; // offset=now on the live segment
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
                let (e2, c2) = {
                    let st = handle.state.lock().unwrap();
                    (st.durable.next, st.durable.closed)
                };
                end = e2;
                if end > scan_from || c2 {
                    live_wake = end > scan_from;
                    break;
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep_until(deadline) => break,
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

pub async fn metrics_flusher(
    state: Arc<AppState>,
    metrics_key: String,
    instance: String,
    lb_url: String,
) {
    // Billing records go through the ROUTER like any tenant write (run-3
    // finding: local appends to a shared-namespace stream fence-fight the
    // shard's ring owner). Lossy by design: failures log and drop.
    let auth = state.auth_token.clone().unwrap_or_default();
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(4))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("metrics http client");
    let url = format!("{}/v1/stream/__metrics__", lb_url.trim_end_matches('/'));
    let mut created = false;
    let mut seq = 0u64;
    let mut tick = tokio::time::interval(Duration::from_secs(15));
    loop {
        tick.tick().await;
        let drained = state.metrics.drain();
        if drained.is_empty() {
            continue;
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
                    continue;
                }
                Err(e) => {
                    tracing::warn!("metrics stream create via router: {e}");
                    continue;
                }
            }
        }
        seq += 1;
        let record = json!([{
            "ts_ms": now_ms(),
            "instance": instance,
            "seq": seq,
            "interval_s": 15,
            "streams": drained,
        }]);
        match client
            .post(&url)
            .header("authorization", format!("Bearer {auth}"))
            .header("stream-encryption-key", &metrics_key)
            .header("content-type", "application/json")
            .json(&record)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => tracing::warn!("metrics append via router: {}", r.status()),
            Err(e) => tracing::warn!("metrics append via router: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
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

    /// Review round 4: value-regression reset detection under-billed the
    /// evict → return → regrow case. Generation ids close it.
    #[test]
    fn billing_deltas_follow_counter_generations() {
        let h = [7u8; 16];
        let mut prev: BillingCheckpoints = HashMap::new();

        // First sighting: bill the full cumulative.
        assert_eq!(
            billing_delta(&prev, &h, 1, 10, 10, 100, 0),
            (10, 10, 100, 0)
        );
        prev.insert(h, (1, (10, 10, 100, 0)));

        // Same generation, counters grew: plain difference.
        assert_eq!(billing_delta(&prev, &h, 1, 15, 12, 130, 5), (5, 2, 30, 5));
        prev.insert(h, (1, (15, 12, 130, 5)));

        // Evicted at 15 requests, returned, and REGREW PAST the old
        // checkpoint before the next tick: cumulative 20 under a new
        // generation. Value comparison alone would emit 5 — the truth
        // for the new incarnation is all 20.
        assert_eq!(
            billing_delta(&prev, &h, 2, 20, 20, 200, 0),
            (20, 20, 200, 0)
        );

        // Same generation with a (defensively handled) regression also
        // re-bills the cumulative rather than underflowing.
        assert_eq!(billing_delta(&prev, &h, 1, 3, 1, 10, 0), (3, 1, 10, 0));
    }
}
