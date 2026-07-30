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
use crate::history::{KeyCache, read_history};
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
    pub hist_readers: Arc<crate::history::HistReaders>,
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

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
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

    async fn engine_for(self: &Arc<Self>, hash: &[u8; 16]) -> Result<Arc<ShardEngine>, Response> {
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
async fn get_segments(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> Response {
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
        obj.insert("history_readers".into(), state.hist_readers.stats_json());
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
/// next_offset. None when the stream/engine is unavailable.
async fn internal_close(state: Arc<AppState>, name: String) -> Option<u64> {
    let desc = state.registry.get(&name).await.ok().flatten()?;
    // Mirror the append path exactly: records are keyed by storage_hash,
    // but the ENGINE is selected by stream_hash(name) (mismatching them
    // closed a phantom keyspace on another shard - e2e run 4, next=0).
    let hash = desc.storage_hash();
    let engine = state
        .engine_for(&crate::crypto::stream_hash(&desc.name))
        .await
        .ok()?;
    let (tx, rx) = oneshot::channel();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: crate::crypto::stream_hash(&desc.name),
        entries: vec![],
        usage: crate::usage::counters(&crate::crypto::stream_hash(&desc.name)),
        routing_key: String::new(),
        key_hash: crate::crypto::stream_hash(""),
        producer_lineage: Vec::new(),
        key_version: 0,
        subkey: [0u8; 32],
        ts_hint_ms: None,
        seq: None,
        bytes: 0,
        close: true,
        producer: None,
        deferred_error: None,
        touch: None,
        resp: tx,
    };
    engine.try_enqueue(req).ok()?;
    match rx.await {
        Ok(Ok(ack)) => Some(ack.next_offset),
        Ok(Err(crate::shard::AppendErr::Closed { next_offset })) => Some(next_offset),
        _ => None,
    }
}

/// The scaler loop (SCALING.md §2): evaluates every scaled stream this
/// instance has routed for, on SCALE_EVAL_SECS cadence. CAS on the
/// segment map arbitrates between instances.
pub fn spawn_scaler(state: Arc<AppState>) {
    tokio::spawn(async move {
        let secs = crate::scaler::policy().eval_secs;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
            let store = state.registry.store();
            for parent in crate::scaler::scaled_streams() {
                let mut ewmas = {
                    crate::scaler::ewma_state()
                        .lock()
                        .unwrap()
                        .remove(&parent)
                        .unwrap_or_default()
                };
                let st = state.clone();
                let owner_st = state.clone();
                let outcome = crate::scaler::evaluate_stream(
                    &store,
                    &parent,
                    &mut ewmas,
                    |seg_name: String| {
                        let st = st.clone();
                        async move { internal_close(st, seg_name).await }
                    },
                    // Act only on segments whose shard this instance
                    // serves — its counters are authoritative for exactly
                    // those, and it can seal them locally. POSSESSION is
                    // the truth, not the ring: engine_for grandfathers a
                    // shard it opened before the ring said otherwise
                    // (fencing arbitrates real conflicts), and a
                    // ring-only check here leaves such shards evaluated
                    // by NOBODY (p5: no split for a whole pass).
                    move |seg_name: &str| {
                        let hash = crate::crypto::stream_hash(seg_name);
                        let prefix = shard_for_hash(&owner_st.shard_prefixes, &hash);
                        if owner_st.shards.read().unwrap().contains_key(&prefix) {
                            return true;
                        }
                        owner_st
                            .effective_owner(&prefix)
                            .map(|o| o == owner_st.instance_name)
                            .unwrap_or(true)
                    },
                )
                .await;
                if let Some(desc) = outcome {
                    tracing::info!("scaler: {desc}");
                }
                crate::scaler::ewma_state()
                    .lock()
                    .unwrap()
                    .insert(parent, ewmas);
            }
        }
    });
}

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
            let resp = append(state.clone(), name.clone(), hdrs.clone(), Body::from(body)).await;
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
        .route("/v1/streams", get(list_streams))
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
        .route(
            "/v1/debug/scaler",
            get(|| async { axum::Json(crate::scaler::debug_snapshot()) }),
        )
        .route("/v1/debug/sleep", get(debug_sleep))
        // Operator dashboard: UNSECURED by explicit product decision (on-call
        // must see the cell without credentials). The payload is therefore
        // restricted to operational metadata — never stream names, tenant
        // identifiers, tokens, keys, or signed URLs.
        .route("/operator", get(crate::operator::page))
        .route("/operator/data.json", get(crate::operator::data))
        .route("/operator/runbook", get(crate::operator::runbook))
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

async fn list_streams(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    match state.registry.list(1000).await {
        Ok(streams) => {
            let body: Vec<_> = streams
                .iter()
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
    }
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
    // Queue subresources: /v1/stream/<name>/queue/{consumer}/{receive,ack,extend}
    if let Some((stream, route)) = name.split_once("/queue/") {
        return queue_entry(
            state,
            stream.to_string(),
            route.to_string(),
            method,
            headers,
            body,
        )
        .await;
    }
    // Segment map (SCALING.md §5): GET /v1/stream/<name>/segments returns
    // the map + lineage for SDKs and tooling. Requires the stream key
    // (same proof-of-authorization as reads).
    if let Some(stream) = name.strip_suffix("/segments") {
        if method != Method::GET {
            return err_resp(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "GET only",
            );
        }
        let desc = match state.registry.get(stream).await {
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
        match check_key(raw_key(&headers, &state), &desc) {
            KeyCheck::Ok(..) => {}
            KeyCheck::Missing => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "missing_key",
                    "Stream-Encryption-Key required",
                );
            }
            _ => return err_resp(StatusCode::FORBIDDEN, "wrong_key", "key mismatch"),
        }
        if !desc.scaling {
            return err_resp(
                StatusCode::CONFLICT,
                "not_scaled",
                "stream does not have scaling enabled",
            );
        }
        let hash = crate::crypto::stream_hash(&desc.name);
        let store = state.registry.store();
        let map = crate::scaler::load_map(&store, &hash).await;
        return axum::Json(serde_json::json!({
            "stream": desc.name,
            "version": map.version,
            "segments": map.segments,
        }))
        .into_response();
    }
    // Touch subresources: /v1/stream/<name>/touch/{meta,key/<hex>}
    if let Some((stream, route)) = name.split_once("/touch/") {
        return touch_entry(
            state,
            stream.to_string(),
            route.to_string(),
            method,
            headers,
            params,
        )
        .await;
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
        Method::POST => append(state, name, headers, body).await,
        Method::GET => read(state, name, params, headers, false).await,
        Method::HEAD => read(state, name, params, headers, true).await,
        Method::DELETE => delete_stream(state, name).await,
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

fn parse_expiry(headers: &HeaderMap) -> Result<Option<i64>, String> {
    let ttl = headers.get("stream-ttl").and_then(|v| v.to_str().ok());
    let expires = headers
        .get("stream-expires-at")
        .and_then(|v| v.to_str().ok());
    match (ttl, expires) {
        (Some(_), Some(_)) => Err("at most one of Stream-TTL and Stream-Expires-At".into()),
        (Some(t), None) => {
            let d = parse_duration(t).ok_or_else(|| format!("invalid Stream-TTL: {t}"))?;
            Ok(Some(now_ms() + d.as_millis() as i64))
        }
        (None, Some(e)) => chrono::DateTime::parse_from_rfc3339(e)
            .map(|ts| Some(ts.timestamp_millis()))
            .map_err(|_| format!("invalid Stream-Expires-At: {e}")),
        (None, None) => Ok(None),
    }
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

fn desc_alive(desc: &StreamDesc) -> bool {
    !desc.deleted && desc.expires_at_ms.map(|e| now_ms() < e).unwrap_or(true)
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
            if id.is_empty() {
                return Err("Producer-Id must not be empty".into());
            }
            let epoch = parse_uint_strict(&e).ok_or("invalid Producer-Epoch")?;
            let seq = parse_uint_strict(&s).ok_or("invalid Producer-Seq")?;
            Ok(Some(crate::shard::ProducerReq { id, epoch, seq }))
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
    profile: Option<String>,
    touch_templates: Vec<crate::registry::PinnedTemplate>,
) -> StreamDesc {
    let _ = state;
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
        // ROUTING-V3: every stream is key-partitioned with the implicit
        // single-segment map; ordering/segmentation/scaling are no
        // longer creation-time choices. The legacy fields stay zeroed
        // (they exist only to parse pre-v3 descriptors).
        ordering: None,
        segment_count: 0,
        queue_max_deliveries: None,
        touch_token_fingerprint: tt_fpr,
        touch_templates,
        touch_sig_key: sig_key,
        scaling: false,
        segments: None,
    }
}

async fn create_stream(
    state: Arc<AppState>,
    name: String,
    headers: HeaderMap,
    body: Bytes,
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
    let content_type =
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
    let profile = hdr(&headers, "stream-profile");
    if let Some(p) = &profile {
        if !matches!(p.as_str(), "generic" | "state-protocol" | "queue") {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "invalid_profile",
                "unsupported profile",
            );
        }
    }
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
    }

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
            if !same_ct || d.ttl_secs != ttl_secs {
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
    let (created, desc) = match existing {
        Some(d) if desc_alive(&d) => match validate_live(d) {
            Ok(v) => v,
            Err(r) => return r,
        },
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
                profile.clone(),
                touch_templates.clone(),
            );
            fresh.queue_max_deliveries =
                hdr(&headers, "stream-queue-max-deliveries").and_then(|v| v.parse().ok());
            match state
                .registry
                .recreate(&name, fresh, |d| !desc_alive(d))
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
                profile.clone(),
                touch_templates.clone(),
            );
            fresh.queue_max_deliveries =
                hdr(&headers, "stream-queue-max-deliveries").and_then(|v| v.parse().ok());
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
    if created && (!body.is_empty() || close) {
        let entries: Vec<Bytes> = if body.is_empty() {
            Vec::new()
        } else if desc.is_json() {
            match json_entries(&body, true) {
                Ok(v) => v,
                Err(m) => return err_resp(StatusCode::BAD_REQUEST, "invalid_json", &m),
            }
        } else {
            vec![body.clone()]
        };
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
            producer: None,
            deferred_error: None,
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
                closed_now = ack.closed;
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
    }

    // Scaled stream: persist the initial single-segment map at creation so
    // segment ages (cooldowns) are real. Idempotent: Create-mode CAS loses
    // harmlessly if the map already exists.
    if created && desc.scaling {
        let m = crate::segmap::SegmentMap::initial("", crate::shard::now_ms());
        let _ = crate::segmap::save(
            &state.registry.store(),
            &crate::crypto::stream_hash(&desc.name),
            &m,
            None,
        )
        .await;
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
    match state.registry.update(&name, |d| d.deleted = true).await {
        Ok(Some(_)) => StatusCode::NO_CONTENT.into_response(),
        Ok(None) => err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            &e.to_string(),
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
        if let Some(raw) = headers.get("touch-token").and_then(|v| v.to_str().ok()) {
            if let Some(bytes) = crate::crypto::unhex(raw.trim()) {
                if let Ok(token) = <[u8; 32]>::try_from(bytes) {
                    if &crate::crypto::touch_token_fingerprint(&token) == expected {
                        return true;
                    }
                }
            }
        }
        matches!(check_key(raw_key(headers, state), desc), KeyCheck::Ok(..))
    } else {
        !matches!(check_key(raw_key(headers, state), desc), KeyCheck::Wrong)
    }
}

fn pinned_of(desc: &StreamDesc) -> Vec<(String, Vec<String>)> {
    desc.touch_templates
        .iter()
        .map(|t| (t.entity.clone(), t.fields.clone()))
        .collect()
}

async fn touch_entry(
    state: Arc<AppState>,
    stream: String,
    route: String,
    method: Method,
    headers: HeaderMap,
    params: ReadParams,
) -> Response {
    let desc = match state.registry.get(&stream).await {
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
    if desc.profile.as_deref() != Some("state-protocol") {
        return err_resp(StatusCode::NOT_FOUND, "not_found", "touch is not enabled");
    }

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
                    crate::crypto::wait_url_sig(&k, &key_hex) == sig.trim().to_ascii_lowercase()
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
        let journal = state.touch.journal(desc.storage_hash(), &pinned_of(&desc));
        let cursor = params.cursor.as_deref().unwrap_or("now");
        let timeout = params
            .timeout
            .as_deref()
            .and_then(parse_duration)
            .unwrap_or(MAX_LONG_POLL)
            .min(MAX_LONG_POLL);
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
            let journal = state.touch.journal(desc.storage_hash(), &pinned_of(&desc));
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

/// ROUTING-V3 sealed-segment retry wrapper: post-split streams (a
/// materialized map with successors or an in-flight transition) buffer
/// the body and retry a stream-closed response after refreshing the
/// descriptor and resuming any pending transition — a seal is a few ms
/// of routing indirection, never a client-visible 409. A 409 whose
/// freshly-refreshed map shows the resolved segment LIVE with no
/// pending transition is a genuine user-closed stream and passes
/// through. Pre-split streams (segments: None — the common case) take
/// the core path directly with zero overhead.
async fn append(state: Arc<AppState>, name: String, headers: HeaderMap, body: Body) -> Response {
    let wrapped = matches!(
        state.registry.get(&name).await,
        Ok(Some(d)) if d
            .segments
            .as_ref()
            .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
    );
    if !wrapped {
        return append_core(state, name, headers, body).await;
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
        )
        .await;
        if !(r.status() == StatusCode::CONFLICT && r.headers().contains_key("stream-closed")) {
            return r;
        }
        state.registry.invalidate(&name);
        let Ok(Some(d)) = state.registry.get(&name).await else {
            return r;
        };
        let rk = hdr(&headers, "stream-key").unwrap_or_default();
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
        Ok(Some(d)) if desc_alive(&d) => d,
        Ok(other) => {
            // Lazy child creation: "<parent>#<n>" appears when the scaler
            // opens a segment; the first append (which carries the stream
            // key) creates it inheriting the parent's config.
            if let Some((parent, _)) = name.split_once('#') {
                if let Ok(Some(pd)) = state.registry.get(parent).await {
                    if pd.scaling && desc_alive(&pd) && other.is_none() {
                        let mut ch = headers.clone();
                        if let Ok(v) = axum::http::HeaderValue::from_str(&pd.content_type) {
                            ch.insert("content-type", v);
                        }
                        let r = create_stream(state.clone(), name.clone(), ch, Bytes::new()).await;
                        if r.status().is_success() || r.status() == StatusCode::CONFLICT {
                            return Box::pin(append(state, name, headers, body)).await;
                        }
                        return r;
                    }
                }
            }
            return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found");
        }
        Err(e) => {
            return err_resp(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                &e.to_string(),
            );
        }
    };
    if desc.scaling && !name.contains('#') {
        let rk = hdr(&headers, "stream-key").unwrap_or_default();
        if rk.is_empty() {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "routing_key_required",
                "scaled streams require a Stream-Key routing key on appends",
            );
        }
        let store = state.registry.store();
        let body_bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
            Ok(b) => b,
            Err(_) => {
                return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large");
            }
        };
        for attempt in 0..4u32 {
            let target = crate::scaler::route(&store, &name, &rk).await;
            let r = Box::pin(append(
                state.clone(),
                target.clone(),
                headers.clone(),
                Body::from(body_bytes.clone()),
            ))
            .await;
            // Sealed child mid-transition: refresh the map and follow.
            if r.status() == StatusCode::CONFLICT && r.headers().contains_key("stream-closed") {
                crate::scaler::invalidate(&crate::crypto::stream_hash(&name));
                if attempt >= 1 {
                    // Still routed to a sealed child after a fresh map
                    // read: a scaler died between seal and map-save.
                    // Re-seal (idempotent, returns the frozen offset) and
                    // publish the missing transition ourselves.
                    if let Some((_, sid)) = target.rsplit_once('#') {
                        if let (Ok(seg_id), Some(next)) = (
                            sid.parse::<u32>(),
                            internal_close(state.clone(), target.clone()).await,
                        ) {
                            crate::scaler::resume_split(&store, &name, seg_id, next).await;
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25 * (attempt as u64 + 1)))
                    .await;
                continue;
            }
            return r;
        }
        return err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "segment_transition",
            "segment map transition did not converge; retry",
        );
    }
    let _stream_slot = match acquire_stream_slot(&state, crate::crypto::stream_hash(&desc.name)) {
        Ok(s) => s,
        Err(r) => return r,
    };
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
    let body = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large"),
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

    let routing_key = hdr(&headers, "stream-key").unwrap_or_default();
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

    // H1 state-protocol hook (unchanged; uses the incarnation hash).
    let touch = if desc.profile.as_deref() == Some("state-protocol") && !entries.is_empty() {
        let journal = state.touch.journal(hash, &pinned_of(&desc));
        let snapshot = journal.snapshot();
        let mut key_ids: Vec<u32> = Vec::new();
        for raw in &entries {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw) {
                if let Some(mut ids) = crate::touch::TouchJournal::derive_key_ids(&snapshot, &v) {
                    key_ids.append(&mut ids);
                }
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
        enqueued_at: std::time::Instant::now(),
        hash,
        route: name_hash,
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
    let segmented = desc.is_per_key() || desc.segments.is_some();
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
    match outcome {
        Ok(ack) => {
            if !ack.duplicate {
                state.metrics.append(&name, metric_bytes);
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
    read_merged(
        &state.hist_readers,
        key,
        epoch,
        handle,
        engine,
        scan_from,
        key_filter,
        max_bytes,
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
    hist: &Arc<crate::history::HistReaders>,
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
            let completed = if hist_v2 {
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
                let (frames, _last, completed) = match key_filter {
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
                completed
            } else {
                let h = read_history(hist, &hash, key, cursor, hist_upto, key_filter, budget)
                    .await
                    .map_err(|e| e.to_string())?;
                for (off, rec) in h.records {
                    budget = budget.saturating_sub(rec.payload.len());
                    out.recs.push(PlainRec {
                        off,
                        payload: rec.payload,
                    });
                    out.last = Some(off);
                }
                h.completed
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

async fn read(
    state: Arc<AppState>,
    name: String,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
) -> Response {
    read_inner(state, name, params, headers, head_only, true).await
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
async fn read_inner(
    state: Arc<AppState>,
    name: String,
    params: ReadParams,
    headers: HeaderMap,
    head_only: bool,
    may_refresh: bool,
) -> Response {
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
    // A single-segment per-key stream is the degenerate case: totally
    // ordered, epoch-0 tokens — serve it through the standard path so every
    // semantic (incl. unkeyed live reads) is byte-identical.
    if desc.is_per_key() && desc.segment_count.max(1) > 1 {
        return read_per_key(state, desc, params, headers, head_only).await;
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
        return read_v3_lineage_inner(state, desc, params, headers, head_only, may_refresh).await;
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
            return Box::pin(read_inner(state, name, params, headers, head_only, false)).await;
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
        return sse_response(state, desc, key, epoch, engine, handle, start, params).await;
    }

    let scan_from = match start {
        StartPos::Now => {
            // Instant tail snapshot for plain reads; long-poll from `now`
            // falls through with scan_from = current end.
            if live.is_none() {
                if closed && !genuine_closure(&state, &name, may_refresh).await {
                    return Box::pin(read_inner(state, name, params, headers, head_only, false))
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
                return Box::pin(read_inner(state, name, params, headers, head_only, false)).await;
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
        if live_wake {
            tail_max_bytes()
        } else {
            MAX_READ_BYTES
        },
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
        return Box::pin(read_inner(state, name, params, headers, head_only, false)).await;
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
    let sse_hash = crate::crypto::stream_hash(&desc.name);
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
            if pos < end && !from_now || (from_now && !first && pos < end) {
                match read_records(
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
                {
                    Ok(out) => {
                        for r in &out.recs {
                            let ev = sse_data_event(&desc, &r.payload);
                            if tx.send(Ok(Bytes::from(ev))).await.is_err() {
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
                    }
                    Err(_) => return,
                }
            }
            let at_end = pos >= end;
            if at_end || sent_any || first {
                // A close observed mid-SSE can be a split's seal, not a
                // user close. Genuine closure sends the final closed
                // control; a transition ends the connection WITHOUT it —
                // the client reconnects and the fresh dispatch serves the
                // successors (or tells it SSE is unsupported on the now-
                // segmented stream).
                let report_closed =
                    closed && at_end && genuine_closure(&state, &desc.name, true).await;
                let ctl = sse_control(pos, cursor.as_deref(), at_end, report_closed);
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
        Some("sse") => {
            return err_resp(
                StatusCode::BAD_REQUEST,
                "unsupported_on_segmented",
                "SSE across segment lineage is not supported yet; use long-poll",
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
                        state, fresh, params, headers, head_only, false,
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

    // Hop forward over already-drained sealed segments so one request
    // always serves records when any exist ahead.
    let seg_tok = |seg_id: u32, last: Option<u64>| match last {
        None => crate::offsets::encode_ep(seg_id, Offset::START),
        Some(o) => crate::offsets::encode_ep(seg_id, Offset(Some(o))),
    };
    loop {
        let sg = &lineage[pos];
        let identity = desc.dynamic_segment_identity(sg.seg_id);
        let engine = match state
            .engine_for(&crate::crypto::stream_hash(&desc.name))
            .await
        {
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
                        state, fresh, params, headers, head_only, false,
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
                .header("Stream-Segment-Map-Version", map.version.to_string())
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
            if live_wake {
                tail_max_bytes()
            } else {
                MAX_READ_BYTES
            },
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
            .header("Stream-Ordering", "per-key")
            .header("Stream-Segment-Map-Version", map.version.to_string())
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
    let parent_engine = match state
        .engine_for(&crate::crypto::stream_hash(&desc.name))
        .await
    {
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
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
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
            Err(e) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    &e.to_string(),
                );
            }
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
        let mut live_wake = false;
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
                        live_wake = true;
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
            &desc,
            &key,
            &epoch,
            &handle,
            &engine,
            scan_from,
            Some(rk),
            // A woken live read returns a fresh commit group, not a
            // backlog: the small budget keeps the response — and the
            // consumer's next-poll rearm — proportional to it.
            if live_wake {
                tail_max_bytes()
            } else {
                MAX_READ_BYTES
            },
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
                Err(e) => {
                    return err_resp(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        &e.to_string(),
                    );
                }
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
    let mut last_tok = None;
    let mut up_to_date = false;
    let mut closed_at_end = false;
    loop {
        let (hash, engine) = seg_handle(ord);
        state.keys.put(hash, key.clone(), epoch);
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
        let closed = handle.state.lock().unwrap().durable.closed;
        let out = match read_records(
            &state, &desc, &key, &epoch, &handle, &engine, pos, None, budget,
        )
        .await
        {
            Ok(o) => o,
            Err(m) => return err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &m),
        };
        for r in &out.recs {
            budget = budget.saturating_sub(r.payload.len());
        }
        let consumed = out.last.map(|o| o + 1).unwrap_or(pos);
        last_tok = Some(seg_tok(ord, consumed));
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
        last_tok.clone().unwrap_or_default(),
        up_to_date as u8
    );
    if let Some(inm) = hdr(&headers, "if-none-match") {
        if inm == etag {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("ETag", etag)
                .body(Body::empty())
                .unwrap();
        }
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
        .header(
            "Stream-Next-Offset",
            last_tok.unwrap_or_else(|| seg_tok(0, 0)),
        )
        .header("Stream-Ordering", "per-key")
        .header("ETag", etag)
        .header("Cross-Origin-Resource-Policy", "cross-origin");
    if up_to_date {
        r = r.header("Stream-Up-To-Date", "true");
        if closed_at_end {
            r = r.header("Stream-Closed", "true");
        }
    }
    crate::usage::counters(&crate::crypto::stream_hash(&desc.name))
        .bytes_out
        .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
    r.body(Body::from(body)).unwrap()
}

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
    crate::usage::counters(&crate::crypto::stream_hash(&desc.name))
        .bytes_out
        .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
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
    stream: String,
    route: String,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let desc = match state.registry.get(&stream).await {
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
    let max_deliveries = desc.queue_max_deliveries.unwrap_or(5);
    let dlq_subkey = derive_subkey(&key, &epoch, "$dlq", 0);
    let raw = match axum::body::to_bytes(body, 1 << 20).await {
        Ok(b) => b,
        Err(_) => return err_resp(StatusCode::BAD_REQUEST, "invalid_request", "bad body"),
    };
    state.metrics.queue(&stream);

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
                        hash,
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
                        &desc,
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
                    hash,
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
