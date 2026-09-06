//! HTTP surface (spec §3.4/§3.5): keyed appends, merged reads (history +
//! shard tail), ciphertext frames by default, server-side decryption for
//! `format=json`, long-poll tails.
use crate::application::append::{AppendCode, AppendFailure, FailureClass, fail};

pub(crate) fn append_failure_status(error: &AppendFailure) -> StatusCode {
    match error.class {
        FailureClass::Invalid
            if matches!(
                error.code,
                AppendCode::BodyTooLarge
                    | AppendCode::TooLarge
                    | AppendCode::PayloadTooLarge
                    | AppendCode::RecordTooLarge
            ) =>
        {
            StatusCode::PAYLOAD_TOO_LARGE
        }
        FailureClass::Invalid => StatusCode::BAD_REQUEST,
        FailureClass::Denied => StatusCode::FORBIDDEN,
        FailureClass::Missing => StatusCode::NOT_FOUND,
        FailureClass::Gone => StatusCode::GONE,
        FailureClass::Conflict => StatusCode::CONFLICT,
        FailureClass::Capacity => StatusCode::TOO_MANY_REQUESTS,
        FailureClass::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        FailureClass::Timeout => StatusCode::REQUEST_TIMEOUT,
        FailureClass::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
fn append_position(seg: u32, next: u64, materialized: bool) -> String {
    if materialized {
        crate::offsets::encode_ep(seg, Offset(next.checked_sub(1)))
    } else {
        tail_token(next)
    }
}
pub(crate) fn render_append(result: crate::application::append::AppendResult) -> Response {
    match result {
        Ok(out) => {
            let status = if out.duplicate || out.appended_records == 0 || out.producer.is_none() {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::OK
            };
            let mut r = Response::builder()
                .status(status)
                .header(
                    "Stream-Next-Offset",
                    append_position(out.seg_id, out.next_offset, out.materialized),
                )
                .header("x-ack-closed", if out.closed { "true" } else { "false" });
            if let Some((epoch, seq)) = out.producer {
                r = r
                    .header("Producer-Epoch", epoch.to_string())
                    .header("Producer-Seq", seq.to_string());
            }
            if out.closed {
                r = r.header("Stream-Closed", "true");
            }
            r.body(Body::empty()).unwrap()
        }
        Err(mut error) => {
            let mut r = err_resp(
                append_failure_status(&error),
                error.code.as_str(),
                &error.message,
            );
            for (name, value) in [
                ("retry-after", error.retry_after.map(|v| v.to_string())),
                ("streams-replay-to", error.owner.take()),
                (
                    "producer-expected-seq",
                    error.expected().map(|v| v.to_string()),
                ),
                (
                    "producer-received-seq",
                    error.received().map(|v| v.to_string()),
                ),
                (
                    "producer-epoch",
                    error.producer_epoch().map(|v| v.to_string()),
                ),
            ] {
                if let Some(value) = value
                    && let Ok(value) = axum::http::HeaderValue::from_str(&value)
                {
                    r.headers_mut().insert(name, value);
                }
            }
            if let Some((seg, next, materialized)) = error.closed_at() {
                r.headers_mut().insert(
                    "stream-closed",
                    axum::http::HeaderValue::from_static("true"),
                );
                r.headers_mut().insert(
                    "stream-next-offset",
                    axum::http::HeaderValue::from_str(&append_position(seg, next, materialized))
                        .unwrap(),
                );
            }
            r
        }
    }
}

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

use crate::crypto::{FrameHeader, StreamKey, derive_subkey, encrypt_frame};
use crate::history::KeyCache;
use crate::offsets::Offset;
use crate::registry::{Registry, StreamDesc};
use crate::shard::{ShardEngine, now_ms};

/// Protocol ceiling on a request body — the wire pin, re-exported from
/// [`crate::protocol_pin`] (PR 3.2.1: the pin moved beside the other
/// protocol constants so configuration validation can reference it
/// without a transport edge).
pub(crate) use crate::protocol_pin::MAX_BODY_BYTES;

const MAX_READ_BYTES: usize = 8 * 1024 * 1024;

/// Budget for a read that was WOKEN by a long-poll wait — the live-tail
/// case, where response size is latency: materialize + transfer + client
/// parse + the client's rearm gap all scale with it. Catch-up reads keep
/// the full MAX_READ_BYTES for throughput. Env TAIL_MAX_BYTES.
/// Benchmark-only stage timing on live reads (env STREAMS_DEBUG_TIMING=1):
/// woken long-poll responses carry `Streams-Debug-Wait: waited=<0|1>
/// arm_us=<arm->wake> read_us=<wake->records-built>`, splitting the
/// remaining roundtrip-minus-append interval into its server-side stages.
pub fn debug_timing(cfg: &crate::config::HttpConfig) -> bool {
    cfg.debug_timing
}

pub fn tail_max_bytes(cfg: &crate::config::HttpConfig) -> usize {
    cfg.tail_max_bytes
}
// The platform front door kills any request at ~30 s with a 502 (measured
// 30.16 s on Prisma Compute). Every server-side wait must conclude below it
// so clients see clean empty responses instead of gateway errors.
const MAX_LONG_POLL: Duration = Duration::from_secs(25);

pub struct AppState {
    /// The one parsed, immutable process configuration (WP-01 PR 3.1).
    /// Owners read their knobs from here; nothing reads the process
    /// environment at runtime.
    pub config: Arc<crate::config::ServerConfig>,
    /// Per-runtime capabilities (WP-15/PR 4): clock, entropy, and this
    /// runtime's identity. Owned here, never process-global.
    pub runtime: crate::runtime::RuntimeCaps,
    /// Trusted wall time for externally exchanged credentials and durable
    /// lifecycle leases. Production shares RuntimeCaps.clock; wire fixtures
    /// explicitly supply the same real-time domain as their JWT/policy feeds.
    pub(crate) protocol_clock: Arc<dyn crate::runtime::Clock>,
    pub(crate) watches: std::sync::OnceLock<Arc<crate::application::watch::WatchService>>,
    pub registry: Arc<Registry>,
    pub(crate) reads: std::sync::OnceLock<Arc<crate::application::read::ReadService>>,
    pub(crate) creations: std::sync::OnceLock<Arc<crate::application::creation::CreationService>>,
    /// WP-02 / PR 6-A: the shard directory OWNS the topology prefixes,
    /// the serving map and the single-flight open gate; resolution
    /// policy lives there, transport-neutral.
    pub shards: crate::shard_directory::ShardDirectory,
    /// WP-02 / PR 6-B: every request-admission gate and counter —
    /// global in-flight, survival bound, RSS shed, per-stream slots, the
    /// live-subscription budget, maintenance backpressure, the fleet
    /// load vector — behind RAII tickets and one snapshot.
    pub admission: crate::admission::AdmissionController,
    /// WP-02 / PR 6-C: peers — the trusted URL table, the outbound
    /// bearer, the inbound static-credential rule, the fleet store.
    pub peer: crate::peer::PeerClient,
    /// PR 6.1-D: this runtime's fleet coordination store (heartbeats,
    /// desired count, overrides and their CAS event outboxes).
    pub fleet: crate::fleet::FleetRepository,
    /// WP-02 / PR 6-C: live feeds — registry, memory budget, ring
    /// allowance, keep-alive cadence.
    pub livefeed: crate::sse::service::LiveFeedService,
    /// WP-02 / PR 6-C: the raw surface's deployment credential.
    pub bearer: crate::deployment_bearer::DeploymentBearer,
    /// WP-02 / PR 6-D: who this deployment is — tenant, account, cell,
    /// region — and the raw adapters' identity source.
    pub deployment: crate::deployment::DeploymentIdentity,
    /// WP-02 / PR 6-E: the usage ledger key, the read accumulator, the
    /// spool and rollup slots, the sweep scheduler's bookkeeping.
    pub billing: crate::billing_service::BillingService,
    /// PR 6.1-C: the rollup consumer's DATABASE (install-once), present
    /// only on instances running the consumer. A store, not a decision.
    pub rollup: crate::rollup::RollupSlot,
    /// WP-02 / PR 6.1-A: a READ-ONLY view of the supervisor every
    /// long-lived loop of this runtime is a child of. The supervisor
    /// itself is owned by the composition root: its tasks capture this
    /// state, so a strong edge back would make a failed start immortal.
    pub tasks: crate::tasks::TaskMonitor,
    /// Round-11.6 field canary ONLY: widen the seal close→publication
    /// gap by this many ms so the seal-herd campaign can observe the
    /// two-step window on a real release binary at fleet scale. Boot
    /// refuses a nonzero value unless STREAMS_CERTIFICATION_MODE=1;
    /// zero = inert (production). Atomic so certification rigs can
    /// arm it on a live state.
    pub cert_sealed_publish_delay_ms: Arc<std::sync::atomic::AtomicU64>,
    /// WP-02 / PR 6-A: ring ownership (this instance's name, the active
    /// set, the rebalancer overrides) behind narrow methods.
    pub ownership: crate::ownership::OwnershipService,
    pub data_store: Arc<dyn ObjectStore>,
    pub keys: Arc<KeyCache>,
    pub touch: Arc<crate::touch::TouchRegistry>,
    /// MULTITENANCY Stage 5: the auth service — inert in Off mode,
    /// observing in Shadow, gating (Stage 5b) in Enforce.
    pub auth: std::sync::Arc<crate::auth::AuthService>,
    /// §17.3 server backstops: bounded per-project admission.
    pub quotas: crate::quota::QuotaRegistry,
    /// Review item 3: deployment key signing catalog cursors (fleets
    /// set it so page walks verify across instances; None = bound but
    /// unsigned on a single instance).
    pub catalog_cursor_key: Option<[u8; 32]>,
    /// Value of the `Prisma-Streams-Origin` header stamped on every
    /// response: instance name (or version) — proof the response came
    /// from a Streams server rather than the platform edge.
    pub origin_marker: String,
}

impl AppState {
    pub(crate) fn append_service(self: &Arc<Self>) -> crate::application::append::AppendService {
        crate::application::append::AppendService {
            usage: self.runtime.usage.clone(),
            registry: self.registry.clone(),
            shards: self.shards.clone(),
            admission: self.admission.clone(),
            quotas: self.quotas.clone(),
            history: self.runtime.history.clone(),
            scaler: self.runtime.scaler.clone(),
            keys: self.keys.clone(),
            watches: self.watch_service(),
            lifecycle: self.lifecycle_service(),
            creation: self.creation_service(),
            auth: self.auth.clone(),
            deployment: self.deployment.clone(),
            admission_config: self.config.admission.clone(),
            meter_enabled: self.config.billing.meter_enabled,
        }
    }
}

impl AppState {
    pub(crate) fn consumer_service(
        self: &Arc<Self>,
    ) -> Arc<crate::application::consumer::ConsumerService> {
        Arc::new(crate::application::consumer::ConsumerService {
            registry: self.registry.clone(),
            shards: self.shards.clone(),
            peer: self.peer.clone(),
            keys: self.keys.clone(),
            append: Arc::new(self.append_service()),
        })
    }

    pub(crate) fn creation_service(
        self: &Arc<Self>,
    ) -> Arc<crate::application::creation::CreationService> {
        self.creations
            .get_or_init(|| {
                Arc::new(crate::application::creation::CreationService {
                    registry: self.registry.clone(),
                    shards: self.shards.clone(),
                    ownership: self.ownership.clone(),
                    reads: self.read_service(),
                    keys: self.keys.clone(),
                    runtime: self.runtime.clone(),
                    deployment: self.deployment.clone(),
                    auth: self.auth.clone(),
                    quotas: self.quotas.clone(),
                    admission: self.admission.clone(),
                })
            })
            .clone()
    }

    pub(crate) fn watch_service(&self) -> Arc<crate::application::watch::WatchService> {
        self.watches
            .get_or_init(|| {
                Arc::new(crate::application::watch::WatchService::new(
                    self.registry.clone(),
                    self.auth.clone(),
                    self.quotas.clone(),
                    self.keys.clone(),
                    self.touch.clone(),
                    self.protocol_clock.clone(),
                ))
            })
            .clone()
    }

    pub(crate) fn lifecycle_service(&self) -> crate::application::lifecycle::LifecycleService {
        crate::application::lifecycle::LifecycleService {
            registry: self.registry.clone(),
            topology: self.topology_service(),
            clock: self.protocol_clock.clone(),
            cert_sealed_publish_delay_ms: self.cert_sealed_publish_delay_ms.clone(),
        }
    }

    pub(crate) fn topology_service(&self) -> crate::application::topology::TopologyService {
        crate::application::topology::TopologyService {
            registry: self.registry.clone(),
            shards: self.shards.clone(),
            peer: self.peer.clone(),
            scaler: self.runtime.scaler.clone(),
            work: self.runtime.request_work.clone(),
        }
    }

    pub(crate) fn read_service(self: &Arc<Self>) -> Arc<crate::application::read::ReadService> {
        self.reads
            .get_or_init(|| {
                Arc::new(crate::application::read::ReadService::new(
                    self.registry.clone(),
                    self.shards.clone(),
                    self.peer.clone(),
                    self.ownership.clone(),
                    self.keys.clone(),
                    Arc::new(self.topology_service()),
                ))
            })
            .clone()
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

/// Customer/account authorization for the PUBLIC surface. Deliberately
/// does not consult the fleet-internal token: the two credentials are
/// separate trust boundaries (round-19 security finding), so an internal
/// token can never perform a product operation.
pub(crate) fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    state.bearer.authorizes(bearer(headers), state.auth.mode)
}

/// SR-5 (Søren decision): the raw Durable Streams surface is
/// INTERNAL-ONLY for shared-cell GA. Off = deployment bearer (local
/// development, conformance). Shadow = deployment bearer REQUIRED.
/// Enforce = workload/fleet credentials only — no deployment-global
/// customer bearer exists on a shared cell.
/// §14.1 (SR2 finding 1): the least-privilege operations an internal
/// principal can hold. A workload JWT authorizes EXACTLY the
/// operations its `operations` claim names — an EMPTY or UNKNOWN list
/// grants nothing, and every internal route demands one exact
/// operation. The static bridge token retains full authority until
/// the platform mints workload identity (its retirement is a GA
/// blocker); a workload token is never a cell-wide credential.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum InternalOperation {
    RawRead,
    RawAppend,
    RawLifecycle,
    SegmentRead,
    SegmentClose,
    SegmentScan,
    QueueCursor,
    ConsumerSweep,
    TelemetryAppend,
}

impl InternalOperation {
    pub(crate) fn claim(self) -> &'static str {
        match self {
            Self::RawRead => "raw-read",
            Self::RawAppend => "raw-append",
            Self::RawLifecycle => "raw-lifecycle",
            Self::SegmentRead => "segment-read",
            Self::SegmentClose => "segment-close",
            Self::SegmentScan => "segment-scan",
            Self::QueueCursor => "queue-cursor",
            Self::ConsumerSweep => "consumer-sweep",
            Self::TelemetryAppend => "telemetry-append",
        }
    }
}

/// Round-4 finding 2: the least facts a RAW-surface LIVE subscription
/// must keep re-proving for as long as it stays open. A workload JWT
/// verified at request time used to be reduced to a boolean — its
/// `expires_at` was discarded, and a connection opened under a
/// short-lived credential kept reading forever after the token died.
#[derive(Clone, Debug)]
pub(crate) struct InternalLease {
    pub subject: Arc<str>,
    pub cell_id: Arc<str>,
    pub operation: InternalOperation,
    pub expires_at: i64,
}

/// What authorized this raw-surface request. The boolean gate is
/// retained for one-shot operations; LIVE requests must observe the
/// `Workload` arm so the subscription cannot outlive its token.
#[derive(Clone, Debug)]
pub(crate) enum RawSurfaceAuth {
    /// Off mode / deployment bearer: local development and conformance
    /// posture, no lease semantics.
    DeploymentBearer,
    /// Enforce-mode static bridge token: the NAMED legacy posture
    /// (refused at boot under the release posture). A permanent shared
    /// secret has no expiry to enforce.
    StaticBridge,
    /// Verified workload JWT scoped to exactly one operation:
    /// SHORT-LIVED — a live subscription on this surface carries its
    /// expiry as a lease.
    Workload(InternalLease),
}

/// Typed raw-surface authorization for ONE operation (§14.3 + §14.1).
/// Same trust boundaries as the boolean form; the workload arm keeps
/// the verified principal's expiry instead of discarding it.
pub(crate) fn raw_surface_authorization(
    state: &AppState,
    headers: &HeaderMap,
    op: InternalOperation,
) -> Option<RawSurfaceAuth> {
    match state.auth.mode {
        crate::auth::AuthMode::Enforce => fleet_operation_authorization(state, headers, op),
        _ => authorized(state, headers).then_some(RawSurfaceAuth::DeploymentBearer),
    }
}

fn fleet_operation_authorization(
    state: &AppState,
    headers: &HeaderMap,
    op: InternalOperation,
) -> Option<RawSurfaceAuth> {
    // SR3-1: exclusive modes at runtime (see fleet_operation_authorized).
    let static_ok = state.peer.inbound_static_ok(bearer(headers));
    if static_ok {
        return Some(RawSurfaceAuth::StaticBridge);
    }
    bearer(headers).and_then(|t| {
        state
            .auth
            .verify_internal(t, crate::shard::now_ms() / 1000)
            .ok()
            .filter(|p| p.operations.iter().any(|o| o == op.claim()))
            .map(|p| {
                RawSurfaceAuth::Workload(InternalLease {
                    subject: p.subject.clone(),
                    cell_id: p.cell_id.clone(),
                    operation: op,
                    expires_at: p.expires_at,
                })
            })
    })
}

/// Raw-surface authorization for ONE operation (§14.3 + §14.1): under
/// enforce the raw surface takes fleet identity scoped to the exact
/// raw operation the request performs; other modes keep the
/// deployment-bearer posture. Boolean view of
/// `raw_surface_authorization` — one-shot operations need only this;
/// LIVE requests must observe the typed `Workload` arm.
pub(crate) fn raw_surface_authorized(
    state: &AppState,
    headers: &HeaderMap,
    op: InternalOperation,
) -> bool {
    raw_surface_authorization(state, headers, op).is_some()
}

/// Short-lived workload JWT (§14.1): aud "prisma-streams-internal",
/// same issuer and JWKS as customer tokens, bound to this cell — and
/// authorized for EXACTLY the operations its claim names.
fn workload_jwt_operation(state: &AppState, headers: &HeaderMap, op: InternalOperation) -> bool {
    if state.auth.mode == crate::auth::AuthMode::Off {
        return false;
    }
    bearer(headers).is_some_and(|t| {
        state
            .auth
            .verify_internal(t, crate::shard::now_ms() / 1000)
            .is_ok_and(|p| p.operations.iter().any(|o| o == op.claim()))
    })
}

/// Authorization for /v1/internal/* and the enforce-mode raw surface,
/// for ONE exact operation. Fails closed: without a configured fleet
/// token the static leg is dead, the customer bearer is never
/// accepted here, and a workload JWT whose operations claim does not
/// name `op` is refused — least privilege is enforced per route, not
/// per audience.
pub(crate) fn fleet_operation_authorized(
    state: &AppState,
    headers: &HeaderMap,
    op: InternalOperation,
) -> bool {
    // SR3-1: the modes are EXCLUSIVE at runtime, not just at boot —
    // with a workload source configured, the static credential is
    // dead even if a legacy FLEET_INTERNAL_TOKEN leaked into the
    // environment. Startup also refuses that coexistence under the
    // release posture; this is the defense-in-depth layer beneath it.
    let static_ok = state.peer.inbound_static_ok(bearer(headers));
    static_ok || workload_jwt_operation(state, headers, op)
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
    /// Internal (non-adopting) resolution: same ownership rules and
    /// on-demand open as engine_for, but never stamps the adoption
    /// sequence — for the tombstone walk and other maintenance.
    #[allow(
        clippy::result_large_err,
        reason = "transport boundary returns Axum wire response directly; application errors stay compact"
    )]
    pub(crate) async fn engine_for_quiet(
        &self,
        hash: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, Response> {
        self.shards
            .resolve(hash, crate::shard_directory::Adoption::Internal)
            .await
            .map_err(resolve_error_response)
    }

    /// Customer-path resolution (stamps external adoption). PR 6-A: a
    /// thin adapter over the shard directory — the policy lives there;
    /// this is the transport's error map, deleted with AppState.
    #[allow(
        clippy::result_large_err,
        reason = "transport boundary returns Axum wire response directly; application errors stay compact"
    )]
    pub(crate) async fn engine_for(&self, hash: &[u8; 16]) -> Result<Arc<ShardEngine>, Response> {
        self.shards
            .resolve(hash, crate::shard_directory::Adoption::External)
            .await
            .map_err(resolve_error_response)
    }
}

/// The ONE transport mapping of a shard-resolution refusal (PR 6-A):
/// not-owner → 409 + Streams-Replay-To (so a stale router corrects
/// itself), opening → retryable 503 + Retry-After, open failure → 500.
pub(crate) fn resolve_error_response(e: crate::shard_directory::ResolveError) -> Response {
    use crate::shard_directory::ResolveError;
    match e {
        ResolveError::NotOwner { prefix, owner } => {
            let mut r = err_resp(
                StatusCode::CONFLICT,
                "not_ring_owner",
                &format!("shard {prefix} belongs to {owner}"),
            );
            if let Ok(v) = axum::http::HeaderValue::from_str(&owner) {
                r.headers_mut().insert("streams-replay-to", v);
            }
            r
        }
        ResolveError::Opening {
            code,
            retry_after_secs,
            ..
        } => {
            let mut r = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                code,
                "shard not currently serving here; retry",
            );
            if let Ok(v) = axum::http::HeaderValue::from_str(&retry_after_secs.to_string()) {
                r.headers_mut().insert("retry-after", v);
            }
            r
        }
        ResolveError::OpenFailed { prefix, error } => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "shard_open",
            &format!("open shard {prefix}: {error}"),
        ),
    }
}

/// Per-engine maintenance state for /v1/debug/load (R25-C).
fn maintenance_shards_json(state: &AppState) -> serde_json::Value {
    let engines: Vec<Arc<ShardEngine>> = state.shards.engines();
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

/// Round-13: buffer a request body while charging each arriving chunk
/// to the project's buffered-body pressure (the queued-byte counter
/// starts too late — the buffering window itself must be accounted).
/// Returns the bytes plus the live guard; the caller drops the guard
/// at the queued-append transfer point so the two charges never
/// overlap. Err(()) = the limit was exceeded (the caller answers 413).
pub(crate) async fn buffer_body_charged(
    body: Body,
    limit: usize,
    adm: Option<std::sync::Arc<crate::quota::ProjectAdmission>>,
) -> Result<(Bytes, Option<crate::quota::BufferedBodyGuard>), ()> {
    use futures_util::StreamExt;
    let mut guard = adm.map(|a| crate::quota::BufferedBodyGuard::reserve(a, 0));
    let mut buf: Vec<u8> = Vec::new();
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(c) = chunk else { return Err(()) };
        if buf.len() + c.len() > limit {
            return Err(());
        }
        if let Some(g) = guard.as_mut() {
            g.grow(c.len() as u64);
        }
        buf.extend_from_slice(&c);
    }
    Ok((Bytes::from(buf), guard))
}

async fn track_inflight(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let ticket = state.admission.enter();
    let cur = ticket.current();
    let _guard = ticket;
    // Round-13 (review): the ORDINARY inflight admission gate moved
    // POST-auth into append_core — running it here answered 429 with
    // capacity information (plus a 25 ms tarpit) to UNAUTHENTICATED
    // callers, letting a noisy or anonymous flood consume tarpit slots
    // and learn capacity posture before the project gate ever ran. The
    // contract: authenticate before tarpit work and capacity answers.
    // Pre-auth keeps ONLY a cheap absolute survival bound (4x the
    // ordinary cap): no tarpit, a generic instant refusal, because a
    // process at 4x its admission cap is defending its sockets, not
    // answering capacity questions.
    let path_is_stream = req.uri().path().starts_with("/v1/stream");
    if state.admission.survival_refused(cur, path_is_stream) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1"), ("content-type", "application/json")],
            r#"{"error":{"code":"overloaded","message":"retry"}}"#,
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
    if !raw_surface_authorized(&state, &headers, InternalOperation::SegmentRead) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let desc = match state
        .registry
        .get(&state.deployment.raw_adapter_sref(&name))
        .await
    {
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

async fn debug_load(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Response {
    let _ = &q;
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let (now, peak) = state.admission.swap_peak();
    let adm = state.admission.snapshot();
    let lf = state.livefeed.snapshot();
    // Cardinality gauges for every stream-indexed structure (static
    // audit: several grew unbounded and invisibly).
    let resident_handles: usize = state
        .shards
        .engines()
        .iter()
        .map(|e| e.resident_streams())
        .sum();
    // Trim maintenance rollup across shards: debt = streams owing
    // physical trims; max_batch is the gate the mature-second-wave
    // stress reads (must stay ≤ TRIM_GLOBAL_BUDGET).
    let (trim_debt, trim_last, trim_max_batch, trim_total) = state
        .shards
        .engines()
        .iter()
        .map(|e| e.trim_stats())
        .fold((0usize, 0u64, 0u64, 0u64), |a, v| {
            (a.0 + v.0, a.1.max(v.1), a.2.max(v.2), a.3 + v.3)
        });
    // Seal-fence rollup (round 12): the map is deliberately unbounded
    // (no safe wall-clock expiry exists over a queue with no residence
    // bound), so its size is surfaced before it could ever matter.
    let (fence_entries, fence_max_gen) = state
        .shards
        .engines()
        .iter()
        .map(|e| e.seal_fence_stats())
        .fold((0usize, 0u64), |a, v| (a.0 + v.0, a.1.max(v.1)));
    // Consumer-fence rollup (round 17): same unbounded-by-design map,
    // same visibility rule.
    let (cfence_entries, cfence_max_gen) = state
        .shards
        .engines()
        .iter()
        .map(|e| e.consumer_fence_stats())
        .fold((0usize, 0u64), |a, v| (a.0 + v.0, a.1.max(v.1)));
    axum::Json(serde_json::json!({
        "inflight_now": now,
        "inflight_peak": peak,
        "rss_mb": crate::fleet::rss_bytes() as f64 / 1048576.0,
        "admit_shed": adm.shed.total,
        "admit_shed_inflight": adm.shed.inflight,
        "admit_shed_rss": adm.shed.rss,
        "admit_shed_survival": adm.shed.survival,
        "project_memory": state.quotas.memory_pressure_json(
            adm.project_memory_pressure_bytes,
            32,
        ),
        "project_pressure_model": crate::quota::pressure_model_json(),
        "sse_connections": adm.sse_connections,
        "sse_max_connections": adm.sse_effective_max,
        "sse_configured_max_connections": adm.sse_configured_max,
        "sse_effective_max_connections": adm.sse_effective_max,
        "lease_terminations": crate::sse::auth::lease_terminations_json(),
        "nofile_soft": NOFILE_SOFT.load(std::sync::atomic::Ordering::Relaxed),
        "nofile_hard": NOFILE_HARD.load(std::sync::atomic::Ordering::Relaxed),
        "open_fds": open_fds(),
        "runtime_tick_age_ms": crate::shard::now_ms() - RUNTIME_LAST_TICK_MS.load(std::sync::atomic::Ordering::Relaxed),
        "runtime_max_tick_gap_ms": RUNTIME_MAX_GAP_MS.load(std::sync::atomic::Ordering::Relaxed),
        "sse_livefeed": {
            "live_feeds": lf.live_feeds,
            "reserved_bytes": lf.reserved_bytes,
            "capacity_rejected": crate::sse::auth::sse_stats::FEED_CAPACITY_REJECTED.load(std::sync::atomic::Ordering::Relaxed),
            "no_progress": crate::sse::auth::sse_stats::FEED_NO_PROGRESS.load(std::sync::atomic::Ordering::Relaxed),
            "source_failed": crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.load(std::sync::atomic::Ordering::Relaxed),
            "oversize_dropped": crate::sse::auth::sse_stats::FEED_OVERSIZE_DROPPED.load(std::sync::atomic::Ordering::Relaxed),
            "uncached_publish": crate::sse::auth::sse_stats::FEED_UNCACHED_PUBLISH.load(std::sync::atomic::Ordering::Relaxed),
            "project_cap_uncached": crate::sse::auth::sse_stats::FEED_PROJECT_CAP_UNCACHED.load(std::sync::atomic::Ordering::Relaxed),
            // Bounded cardinality: rows exist only while a project has
            // live feeds (round-10e per-project observability).
            "project_retention": lf.project_retention.into_iter().map(|(p, reserved, cap_hits)| {
                serde_json::json!({"project": p, "reserved_bytes": reserved, "cap_hits": cap_hits})
            }).collect::<Vec<_>>(),
            "lag_disconnects": crate::sse::auth::sse_stats::FEED_LAG_DISCONNECTS.load(std::sync::atomic::Ordering::Relaxed),
            "topology_disconnects": crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_incarnation": crate::sse::auth::sse_stats::FEED_CUTOFF_INCARNATION.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_wrong_owner": crate::sse::auth::sse_stats::FEED_CUTOFF_WRONG_OWNER.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_incompatible": crate::sse::auth::sse_stats::FEED_CUTOFF_INCOMPATIBLE.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_target_mismatch": crate::sse::auth::sse_stats::FEED_CUTOFF_TARGET_MISMATCH.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_fleet_auth": crate::sse::auth::sse_stats::FEED_CUTOFF_FLEET_AUTH.load(std::sync::atomic::Ordering::Relaxed),
            "cutoff_redirect_loop": crate::sse::auth::sse_stats::FEED_CUTOFF_REDIRECT_LOOP.load(std::sync::atomic::Ordering::Relaxed),
            "catchup_retries": crate::sse::auth::sse_stats::FEED_CATCHUP_RETRIES.load(std::sync::atomic::Ordering::Relaxed),
            "version_bumps": crate::sse::auth::sse_stats::FEED_VERSION_BUMPS.load(std::sync::atomic::Ordering::Relaxed),
        },
        "sse_canary": {
            "below_floor_catchups": crate::sse::auth::sse_stats::BELOW_FLOOR_CATCHUPS.load(std::sync::atomic::Ordering::Relaxed),
            "disconnect_send_timeout": crate::sse::auth::sse_stats::DISCONNECT_SEND_TIMEOUT.load(std::sync::atomic::Ordering::Relaxed),
            "disconnect_client_closed": crate::sse::auth::sse_stats::DISCONNECT_CLIENT_CLOSED.load(std::sync::atomic::Ordering::Relaxed),
            "delivered_records": crate::sse::auth::sse_stats::DELIVERED_RECORDS.load(std::sync::atomic::Ordering::Relaxed),
        },
        "absorb_reserved_bytes_now": state.runtime.history.budget.reserved_bytes(),
        "shed_line_mb": adm.rss_shed_mb,
        "maintenance_backpressure": state.admission.maintenance_stats_json(),
        // #266 field attribution: the wc sampler reads THIS endpoint —
        // the /v1/debug/absorb block alone left L1d7 blind on whether
        // pacing fired at all.
        "gather_last_read_ms": crate::history::GATHER_LAST_READ_MS
            .load(std::sync::atomic::Ordering::Relaxed),
        "gather_last_pace_ms": crate::history::GATHER_LAST_PACE_MS
            .load(std::sync::atomic::Ordering::Relaxed),
        "maintenance_shards": maintenance_shards_json(&state),
        // R26-7: the ORDINARY per-stream limiter's refusals, by code —
        // so a throughput plateau is attributed to the right mechanism.
        "rate_limit_refusals": crate::usage::limit_refusals_json(),
        // R26-9 build identity: the wrapper hashes the binary it
        // actually downloaded and passes the digest in; verify-running
        // compares it to the campaign's upload manifest. "unknown"
        // outside wrapper-managed deployments.
        "binary_sha256": state.config.http.binary_sha256.clone(),
        // R28: full build/boot identity — the campaign verifier compares
        // ALL of these against its manifest (stale-build platform trap).
        "git_commit": env!("STREAMS_GIT_COMMIT"),
        "build_unix": env!("STREAMS_BUILD_UNIX"),
        "boot_id": state.runtime.identity.boot_id.as_str(),
        "compactor_profile": crate::config::profile::compactor_profile_json(&state.config),
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
        "stream_shed": adm.shed.stream,
        "wedge_shed": adm.shed.wedge,
        "streams_tracked": adm.streams_tracked,
        "absorb_lag_max_secs": state.runtime.usage.absorb_lag_max(),
        "cardinality": {
            "resident_handles": resident_handles,
            "usage_tracked": state.runtime.usage.tracked_streams(),
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
                .shards.engines().first()
                .map(|e| e.postings_cache.stats())
                .unwrap_or(serde_json::json!(null)),
        },
        "scaler": state.runtime.scaler.stats_json(),
        // Routing state as THIS instance sees it — the fleet
        // certification harness waits on real override convergence
        // instead of guessing at tick cadence.
        "ring": {
            "active": state.ownership.ring_active(),
            "overrides": state.ownership.overrides().into_iter().collect::<std::collections::BTreeMap<_, _>>(),
        },
        // Cross-layout absorb advances rejected by the committer's
        // layout seal. Nonzero = the absorber's lane classification
        // raced dispatch somewhere; the seal made it harmless, but it
        // should stay rare enough to investigate when it moves.
        // PR 6-F: the supervised long-lived loops and the first critical
        // exit, if any (readiness adopts it in WP-15's remaining slice).
        "tasks": {
            "phase": state.tasks.phase().map(|p| format!("{p:?}")),
            "critical_failure": state.tasks.critical_failure(),
            "loops": state.tasks.snapshot().into_iter().map(|t| serde_json::json!({
                "name": t.name,
                "policy": format!("{:?}", t.policy),
                "state": format!("{:?}", t.state),
            })).collect::<Vec<_>>(),
        },
        "absorb_lane_dropped": state
            .shards.engines().iter()
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
    let mut snap = crate::store_timing::snapshot(window, swap, &state.runtime.store_io);
    if let Some(_obj) = snap.as_object_mut() {
        // History DbReader service: hits vs misses shows how much
        // per-request manifest traffic the cache absorbs; stale_reopens
        // is bounded by absorb cadence; coalesced proves single-flight.
    }
    axum::Json(snap).into_response()
}

/// Shadow-mode observability (MULTITENANCY §7.2): mode, counter
/// deltas, and the age/size of every published snapshot — the numbers
/// the field trial reads to decide the enforce flip.
async fn debug_auth(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let now = crate::shard::now_ms() / 1000;
    let (tracked, inflight) = state.quotas.stats();
    axum::Json(serde_json::json!({
        "shadow": state.auth.shadow_json(),
        "feeds": state.auth.feed_json(now),
        "admission": { "trackedProjects": tracked, "inflight": inflight },
    }))
    .into_response()
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
    let l = state.runtime.usage.limits();
    let streams: Vec<serde_json::Value> = state.runtime.usage.snapshot()
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
                "absorb_lag_secs": state.runtime.usage.absorb_lag_for_usage(crate::crypto::RouteHash(h)),
            })
        })
        .collect();
    let (backlog_streams, backlog_max) = state.runtime.usage.absorb_backlog_summary();
    let (eligible, oldest_eligible) = state.runtime.usage.absorb_pending_summary();
    let (overflow_admits, overflow_requests, overflow_records, overflow_bytes) =
        state.runtime.usage.overflow_stats();
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
        "tracked_streams": state.runtime.usage.tracked_streams(),
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
    let recent = state.runtime.ops.recent(128);
    axum::Json(serde_json::json!({
        "events": recent,
        "alerts": state.runtime.ops.open_alerts(),
        "dropped": state.runtime.ops.dropped(),
    }))
    .into_response()
}

/// Invoice reconciliation (MULTITENANCY Stage 7): recompute one
/// month's per-(account, project) totals from the stream month rows
/// and compare against the served project aggregates. Operator
/// bearer; customer tokens never reach /v1/debug/*.
async fn debug_usage_reconcile(
    State(state): State<Arc<AppState>>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    let mut month: Option<String> = None;
    for pair in query
        .as_deref()
        .unwrap_or("")
        .split('&')
        .filter(|s| !s.is_empty())
    {
        match pair.split_once('=') {
            Some(("month", v)) if !v.is_empty() => month = Some(v.to_string()),
            _ => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "bad_query",
                    "only month=YYYY-MM is accepted",
                );
            }
        }
    }
    let month = month.unwrap_or_else(|| {
        let (y, m) = crate::billing::utc_year_month(crate::billing::billing_now_ms());
        crate::billing::month_str(y, m)
    });
    let Some(rollup) = state.rollup.get() else {
        return err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "rollup_unavailable",
            "this instance does not host the usage rollup",
        );
    };
    match rollup.reconcile_month(&month).await {
        Ok(report) => axum::Json(serde_json::json!(report)).into_response(),
        Err(e) => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "reconcile_failed",
            &e.to_string(),
        ),
    }
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
    if !fleet_operation_authorized(&state, &headers, InternalOperation::TelemetryAppend) {
        return internal_unauthorized();
    }
    if !crate::billing::is_reserved_stream(&name) {
        return err_resp(
            StatusCode::FORBIDDEN,
            "not_system_stream",
            "telemetry-append accepts only reserved system streams",
        );
    }
    // Stage 7 review fix: the relay RECEIVER must address the reserved
    // stream under the SYSTEM project — the same identity the sender
    // (billing::system_append) appended toward and every reader
    // (system_read, rollup_step) reads. Writing under the deployment
    // tenant here put relayed usage/ops/audit batches in a stream
    // nobody reads (route hashes include the project), silently losing
    // every batch relayed across instances.
    let mut hdrs = HeaderMap::new();
    if let Some(k) = headers.get("stream-encryption-key") {
        hdrs.insert("stream-encryption-key", k.clone());
    }
    hdrs.insert(
        "content-type",
        axum::http::HeaderValue::from_static("application/json"),
    );
    let system = crate::tenant::system_project();
    let c = create_stream(
        state.clone(),
        system.clone(),
        name.clone(),
        hdrs.clone(),
        Bytes::new(),
    )
    .await;
    let cst = c.status().as_u16();
    if !(cst == 200 || cst == 201 || cst == 409) {
        return c;
    }
    let sref = system.stream_ref(&name);
    append(state, sref, hdrs, Body::from(body), None, None, None).await
}

/// #269: the one h1 serve loop — production and every test rig serve
/// through THIS function, so the suite exercises the real connection
/// path (axum::serve's default hyper posture measured ~53 KB resident
/// per parked conn; a bounded max_buf_size holds the same fleet at
/// ~44 KB, floor now dominated by task/future/slab overhead).
/// max_buf bounds per-READ chunk size, not request body size.
pub async fn serve_h1(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    max_buf: usize,
    tasks: crate::tasks::TaskSupervisor,
) -> std::io::Result<()> {
    let svc = hyper_util::service::TowerToHyperService::new(app);
    let limits = raise_nofile();
    let (soft, hard) = (
        limits.soft.map_or(0, |n| n.get()),
        limits.hard.map_or(0, |n| n.get()),
    );
    NOFILE_SOFT.store(soft, std::sync::atomic::Ordering::Relaxed);
    NOFILE_HARD.store(hard, std::sync::atomic::Ordering::Relaxed);
    tracing::info!("nofile soft={soft} hard={hard} (raised to hard at boot)");
    spawn_runtime_watchdog(&tasks);
    // PR 6.1-A: the accept loop OWNS its connections. A connection is
    // not request-scoped — keep-alives and live subscriptions outlive
    // any one request — so on cancellation the loop stops accepting,
    // releases the address, then aborts and JOINS every connection: a
    // runtime that has shut down has no socket left open, and a
    // replacement can bind the same address immediately.
    let cancel = tasks.cancellation();
    let mut conns = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((sock, _peer)) => {
                    let svc = svc.clone();
                    conns.spawn(async move {
                        let _ = sock.set_nodelay(true);
                        let io = hyper_util::rt::TokioIo::new(sock);
                        let mut b = hyper::server::conn::http1::Builder::new();
                        b.max_buf_size(max_buf);
                        // Errors here are routine client behavior (resets,
                        // half-closed keep-alives), not server faults.
                        let _ = b.serve_connection(io, svc).await;
                    });
                }
                Err(e) => {
                    // Transient accept errors (EMFILE bursts, aborted
                    // handshakes) must not kill the acceptor.
                    tracing::warn!("accept: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            },
            // Reap finished connections so the set never grows with
            // completed entries.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    drop(listener);
    conns.abort_all();
    while conns.join_next().await.is_some() {}
    Ok(())
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
            "/v1/internal/segment-close/{*name}",
            post(internal_segment_close),
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
        .route("/v1/debug/auth", get(debug_auth))
        .route("/v1/debug/ops-events", get(debug_ops_events))
        .route("/v1/debug/usage-reconcile", get(debug_usage_reconcile))
        // Every /v1/debug/* route is account-gated (round-19: the
        // security model claims bearer auth on all of /v1/*, and these
        // MUTATE production state — pausing absorption, occupying
        // request slots, resetting peak gauges — or expose per-stream
        // usage). SR-5: /operator is bearer-gated like the debug surface.
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
                    state.runtime.history.paused
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
                    if !state.config.http.debug_exit {
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
                    let engines: Vec<_> = state.shards.engines_by_prefix();
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
                    let spool = state.billing.read_spool_stats().map(|sp| {
                        let (rows, bytes) = (sp.pending_rows, sp.pending_bytes);
                        let (l0, l0b, runs, mid) = sp.l0;
                        serde_json::json!({
                            "pendingRows": rows,
                            "pendingBytes": bytes,
                            "quarantined": sp.quarantined,
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
                    let budget = &state.runtime.history.budget;
                    axum::Json(serde_json::json!({
                        "historyPartitions": parts,
                        "budget": {
                            "capacityBytes": budget.capacity(),
                            "gatherSlots": budget.gather_slots(),
                            "effectiveGatherConcurrency":
                                state.runtime.history.effective_gather_concurrency(),
                            "perGatherReservationBytes":
                                state.runtime.history.per_gather_reservation_bytes(),
                            "worstFrameTransientBytes":
                                state.runtime.history.worst_frame_transient,
                            "injectedFlushStallMs": crate::history::HISTORY_FLUSH_STALL_MS
                                .load(std::sync::atomic::Ordering::Relaxed),
                            "shedLineMb": state.admission.rss_shed_mb(),
                        },
                        "absorber": {
                            "reservedBytes": state.runtime.history.budget.reserved_bytes(),
                            "gathersInflight": state.runtime.history.budget.inflight(),
                            "lastReservedBytes": crate::history::GATHER_LAST_RESERVED.load(ord),
                            "lastActualBytes": crate::history::GATHER_LAST_ACTUAL.load(ord),
                            "lastReadMs": crate::history::GATHER_LAST_READ_MS.load(ord),
                            "lastPaceMs": crate::history::GATHER_LAST_PACE_MS.load(ord),
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
                                state.runtime.telemetry.capacity_bytes(),
                            "sweepResidentEngines":
                                crate::billing::sweep_resident_engines(&state),
                        },
                        "config": state.runtime.history.resolved_memory_config.get(),
                        "process": {
                            "rssMb": state.admission.rss_mb(),
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
    let engines: Vec<(String, Arc<ShardEngine>)> = state.shards.engines_by_prefix();
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
    #[allow(dead_code)]
    // Retained in the pinned raw query DTO; touch observation has its own owner.
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
    /// Review V4: the verified principal's authorization lease. Set
    /// only by the product dispatch (serde-skipped: the query string
    /// can never forge it); None on internal/operator surfaces. Live
    /// subscriptions re-validate it on auth-generation change and
    /// terminate at token expiry.
    #[serde(skip)]
    pub(crate) lease: Option<crate::auth::AuthLease>,
    /// Round-4 finding 2: the verified workload principal's expiry
    /// lease on the RAW surface. Serde-skipped like `lease`; set by
    /// the raw GET dispatch from the typed workload authorization so
    /// a live subscription cannot outlive its short-lived JWT.
    #[serde(skip)]
    pub(crate) internal_lease: Option<InternalLease>,
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
    let resp = crate::product::with_product_cors(
        crate::product::product_list(state.clone(), query.unwrap_or_default(), headers).await,
    );
    crate::audit::observe_denial(&state, "/v1/streams", &Method::GET, &resp);
    resp
}

/// Health: in BILLING_MODE=required an instance is NOT ready until
/// its billing prerequisites hold (round-22 item 10) — the read spool
/// is open and, on a rollup owner, the rollup DB is open. Both are
/// opened synchronously at startup, so a 503 here means startup-order
/// bugs or a lost OnceLock, and the platform should not route yet.
async fn health_axum(State(state): State<Arc<AppState>>) -> Response {
    if let Some(reason) = state.tasks.unready_reason() {
        return (StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }
    // Review item 6: in shadow/enforce an instance is NOT ready until
    // every auth feed has published an INITIAL snapshot — routing
    // traffic to a cell that would fail-closed (or shadow-count
    // nothing but noise) helps nobody.
    if state.auth.mode != crate::auth::AuthMode::Off {
        let feeds = state.auth.feed_json(crate::shard::now_ms() / 1000);
        let unpublished = [
            ("jwks", &feeds["jwks"]["ageSecs"]),
            ("policies", &feeds["policies"]["ageSecs"]),
            ("grants", &feeds["grants"]["ageSecs"]),
        ]
        .iter()
        .filter(|(_, age)| age.is_null())
        .map(|(n, _)| *n)
        .collect::<Vec<_>>();
        if !unpublished.is_empty() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("auth feeds not yet published: {}", unpublished.join(", ")),
            )
                .into_response();
        }
    }
    // A process that has never opened a shard cannot serve a single
    // append; answering `ok` keeps it in the load balancer forever
    // (CHAOS-2). Report unready so rollouts halt and traffic drains.
    if let Some(reason) = state.shards.unready_reason() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("shard storage unavailable: {reason}"),
        )
            .into_response();
    }
    if crate::billing::billing_required(&state.config.billing) {
        let spool_ok = state.billing.read_spool_open();
        let rollup_ok =
            state.config.billing.rollup_env.as_deref() != Some("1") || state.rollup.installed();
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
            ("x-streams-boot-id", state.runtime.identity.boot_id.as_str()),
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
async fn billing_readiness_axum(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "operator bearer required",
        );
    }
    use std::sync::atomic::Ordering;
    let now = state.runtime.clock.now().ms();
    let (spool_open, quarantined, depth) = state.billing.read_spool_health().await;
    let progress = state.runtime.telemetry.progress();
    let last_drain = progress.last_drain_ok_ms;
    let last_apply = progress.last_rollup_apply_ms;
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
    let ready = !crate::billing::billing_required(&state.config.billing)
        || (state.billing.usage_key().is_some()
            && spool_open
            && (state.config.billing.rollup_env.as_deref() != Some("1")
                || state.rollup.get().is_some()));
    axum::Json(serde_json::json!({
        "mode": state.config.billing.mode_env.clone().unwrap_or_else(|| "off".into()),
        "ready": ready,
        "usageLedgerConfigured": state.billing.usage_key().is_some(),
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
        "openAlerts": state.runtime.ops.open_alerts(),
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
    let route = req.uri().path().to_string();
    let method = req.method().clone();
    let resp = project_usage_axum_inner(state.clone(), project, req).await;
    crate::audit::observe_denial(&state, &route, &method, &resp);
    resp
}

async fn project_usage_axum_inner(
    state: Arc<AppState>,
    project: String,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    crate::product::shadow_observe_request(
        &state,
        req.uri().path(),
        req.method(),
        &query,
        req.headers(),
    );
    // Stage 5d: ONE verification; the principal's project is the
    // authority the handler compares the path against (the URL tenant
    // is a claim to check, never a router hint). Off/shadow keep the
    // deployment tenant + legacy bearer.
    let mut _admission = None;
    let authority = if state.auth.mode == crate::auth::AuthMode::Enforce {
        match crate::product::enforce_customer(&state, req.headers()) {
            Ok(p) => {
                if let Err(e) = p.require(crate::tenant::Scope::UsageRead) {
                    return crate::product::with_product_cors(crate::audit::tag_project(
                        crate::product::auth_failure_response(&e),
                        &p.project_id,
                    ));
                }
                // SR-3: usage queries occupy the project's admission
                // slot like every other customer request.
                match crate::product::project_admission(&state, Some(&p)) {
                    Ok(g) => _admission = g,
                    Err(r) => return crate::product::with_product_cors(r),
                }
                p.project_id
            }
            Err(r) => return crate::product::with_product_cors(r),
        }
    } else {
        if !authorized(&state, req.headers()) {
            return crate::product::with_product_cors(crate::product::perr(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "bearer token required",
                None,
                false,
            ));
        }
        // mt-lint: allow(state-tenant-read): Off/Shadow single-tenant fallback for the usage route (Stage 5d posture)
        state.deployment.deployment_tenant().clone()
    };
    crate::product::with_product_cors(
        crate::product::project_usage(state, &authority, project, &query).await,
    )
}

/// Prisma product surface (spec Stage 8): everything under /v1/streams/.
/// The outer fn exists so EVERY response — early refusals included —
/// passes the §10.4 denial observer exactly once on its way out.
async fn product_entry_axum(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    method: Method,
    headers: HeaderMap,
    req: axum::extract::Request,
) -> Response {
    let resp =
        product_entry_axum_inner(state.clone(), name.clone(), method.clone(), headers, req).await;
    crate::audit::observe_denial(&state, &name, &method, &resp);
    resp
}

pub(crate) async fn product_entry_axum_inner(
    state: Arc<AppState>,
    name: String,
    method: Method,
    headers: HeaderMap,
    req: axum::extract::Request,
) -> Response {
    let query = req.uri().query().unwrap_or("").to_string();
    // Authorize BEFORE buffering. Reading up to MAX_BODY_BYTES first
    // let an unauthenticated caller make the server allocate 32 MiB per
    // request; the gate needs only the path, method, query and headers.
    crate::product::shadow_observe_request(&state, &name, &method, &query, &headers);
    let authorization =
        match crate::product::product_auth_gate(&state, &name, &method, &query, &headers) {
            Ok(p) => p,
            Err(r) => return crate::product::with_product_cors(r),
        };
    let principal = authorization.principal();
    // §17.3: acquire project admission BEFORE reading the body. Only
    // enforce-mode requests carry a verified project; the guard holds
    // the inflight slot for the handler's lifetime.
    let _quota_guard = match crate::product::project_admission(&state, principal) {
        Ok(g) => g,
        Err(r) => return crate::product::with_product_cors(r),
    };
    // Round-13: the per-project memory-pressure backstop for WRITES on
    // this surface — after ordinary project admission, before body
    // work. Reads and established SSE delivery continue while a
    // project is engaged.
    if method == Method::POST
        && let Some(r) = crate::product::project_memory_gate(&state, principal)
    {
        return crate::product::with_product_cors(r);
    }
    // System namespace guard (docs/OBSERVABILITY-BILLING.md §8/§15) —
    // same rule as the raw surface: the leading `_` segment belongs to
    // the telemetry planes and no customer credential reaches it. After
    // auth, before body buffering. Note: usage LOOKUP endpoints live on
    // this surface under `{name}/usage`, which is a sub-resource of a
    // CUSTOMER stream — unaffected by this guard.
    if crate::billing::is_reserved_stream(&name) {
        let mut r = crate::audit::tag(
            crate::product::perr(
                StatusCode::FORBIDDEN,
                "reserved_stream",
                "names beginning with '_' are reserved for the system",
                None,
                false,
            ),
            "reserved_stream",
        );
        if let Some(p) = principal {
            r = crate::audit::tag_project(r, &p.project_id);
        }
        return crate::product::with_product_cors(r);
    }
    // Only mutations consume a body. GET/HEAD/watch and OPTIONS discard it
    // without polling; an unverified watch claim never acquires body memory.
    let (body, _body_charge) = if method == Method::POST || method == Method::PUT {
        match buffer_body_charged(
            req.into_body(),
            state.config.cli.max_request_body_bytes,
            principal.and_then(|p| state.quotas.pressure_handle(&p.project_id)),
        )
        .await
        {
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
        }
    } else {
        (Bytes::new(), None)
    };
    // Round-13: this surface's queued/committer accounting takes over
    // beyond this point (product_append charges queued bytes) — the
    // buffering charge ends here, no transient double charge.
    drop(_body_charge);
    // §10.4: fill the VERIFIED principal's project into any tagged
    // denial the handlers produced (fill-only-if-absent), so classifier
    // sites never need identity plumbing of their own.
    let proj = principal.map(|p| p.project_id.clone());
    let mut resp =
        crate::product::product_entry(state, name, method, headers, query, body, authorization)
            .await;
    if let Some(p) = proj {
        resp = crate::audit::tag_project(resp, &p);
    }
    crate::product::with_product_cors(resp)
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
        st.admission.note_fleet_op();
    }
    resp
}

async fn stream_entry_inner(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(mut params): Query<ReadParams>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // §14.1: the raw operation is derived from the METHOD — a
    // lifecycle token cannot append, an append token cannot delete.
    let raw_op = match method {
        Method::PUT | Method::DELETE => InternalOperation::RawLifecycle,
        Method::POST => InternalOperation::RawAppend,
        _ => InternalOperation::RawRead,
    };
    // Round-4 finding 2: TYPED authorization — a verified workload JWT
    // keeps its expiry (an InternalLease) instead of being reduced to
    // a boolean the request path discards.
    let Some(raw_authz) = raw_surface_authorization(&state, &headers, raw_op) else {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    };
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
            let body = match axum::body::to_bytes(body, state.config.cli.max_request_body_bytes)
                .await
            {
                Ok(b) => b,
                Err(_) => {
                    return err_resp(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large");
                }
            };
            let r = create_stream(
                state.clone(),
                // mt-lint: allow(state-tenant-read): raw adapter create path (SS14.3 scope)
                state.deployment.deployment_tenant().clone(),
                name.clone(),
                headers,
                body,
            )
            .await;
            if r.status() == StatusCode::CREATED
                && let Ok(Some(d)) = state
                    .registry
                    .get(&state.deployment.raw_adapter_sref(&name))
                    .await
            {
                state.runtime.ops.emit(
                    crate::ops::OpsEvent::new(
                        "stream_created",
                        format!("life/{}/created", d.stream_epoch),
                    )
                    .stream(&d.sref(), &d.stream_epoch),
                );
            }
            r
        }
        Method::POST => {
            let r = append(
                state.clone(),
                state.deployment.raw_adapter_sref(&name),
                headers,
                body,
                None,
                None,
                None,
            )
            .await;
            // Operation count only (§4.5) — the BILLED ingest bytes are
            // the committer's, atomic with the records themselves.
            if r.status().is_success()
                && let Ok(Some(desc)) = state
                    .registry
                    .get(&state.deployment.raw_adapter_sref(&name))
                    .await
            {
                crate::billing::meter_append_request(&state, &desc);
            }
            r
        }
        Method::GET | Method::HEAD => {
            // Round-4 finding 2: a workload-JWT read carries its
            // expiry into the request — a live (SSE) subscription on
            // this surface must terminate no later than token expiry,
            // not outlive it. The static bridge and deployment-bearer
            // postures carry no lease (permanent credentials).
            let head_only = method == Method::HEAD;
            if let RawSurfaceAuth::Workload(l) = raw_authz {
                params.internal_lease = Some(l);
            }
            read(state, name, params, headers, head_only).await
        }
        Method::DELETE => {
            let sref = state.deployment.raw_adapter_sref(&name);
            delete_stream(state, sref).await
        }
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
        .or(state.bearer.default_key())
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

pub(crate) use crate::application::creation::desc_alive;

/// Identity of a creation request: a replayed PUT hashes identically,
/// so it JOINS an in-flight initialization instead of observing the
/// descriptor and skipping the work. Deliberately NOT keyed by the
/// encryption key — the key is checked separately, because a request
/// that differs only by key must be refused, not treated as a new one.
pub(crate) use crate::application::creation::create_request_hash;

/// A live descriptor whose initialization has not completed: its
/// content is not durable yet, so readers and appenders get a retryable
/// answer rather than an empty stream.
///
/// Readiness is `init.is_none()` and nothing else. This used to expire
/// with the claim, which answered the wrong question: after 15 seconds
/// an abandoned half-built stream started serving as though it were
/// finished — the original field anomaly, delayed. Whether the claim is
/// stale decides only WHO MAY TAKE OVER the work (see
/// the creation coordinator); it never makes incomplete content visible.
pub(crate) fn initializing(desc: &crate::registry::PersistedDescriptor) -> bool {
    desc.init.is_some()
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

/// Strict TTL grammar: canonical non-negative decimal only.
fn parse_ttl_strict(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.is_empty() || (b[0] == b'0' && b.len() > 1) || !b.iter().all(|c| c.is_ascii_digit()) {
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

pub(crate) fn tail_token(next: u64) -> String {
    if next == 0 {
        Offset::START
    } else {
        Offset(Some(next - 1))
    }
    .encode()
}

fn parse_producer(headers: &HeaderMap) -> Result<Option<crate::shard::ProducerReq>, String> {
    crate::application::append::parse_producer(
        hdr(headers, "producer-id"),
        hdr(headers, "producer-epoch"),
        hdr(headers, "producer-seq"),
    )
}

/// Product DELETE maps to the one collection-delete implementation.
pub(crate) async fn product_delete(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
) -> Response {
    delete_stream(state, tenant.stream_ref(&name)).await
}

pub(crate) async fn create_stream(
    state: Arc<AppState>,
    project: crate::tenant::ProjectId,
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

    let fork_source = hdr(&headers, "stream-forked-from");
    let fork_offset = hdr(&headers, "stream-fork-offset");
    let fork_sub = hdr(&headers, "stream-fork-sub-offset");
    if fork_source.is_none() && (fork_offset.is_some() || fork_sub.is_some()) {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "fork_headers",
            "Stream-Fork-Offset/Sub-Offset require Stream-Forked-From",
        );
    }
    let offset = match fork_offset.as_deref().map(parse_fork_offset).transpose() {
        Ok(v) => v,
        Err(e) => return err_resp(StatusCode::BAD_REQUEST, "invalid_fork_offset", &e),
    };
    let sub_offset = match fork_sub.as_deref() {
        Some(s) => match s.trim().parse::<u64>() {
            Ok(n) if !s.trim().is_empty() && s.trim().chars().all(|c| c.is_ascii_digit()) => {
                Some(n)
            }
            _ => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    "invalid_fork_sub_offset",
                    "sub-offset must be a non-negative integer",
                );
            }
        },
        None => None,
    };
    let fork = fork_source.map(|source| crate::application::creation::ForkCommand {
        source: project.stream_ref(
            source
                .strip_prefix("/v1/stream/")
                .unwrap_or(&source)
                .trim_matches('/'),
        ),
        offset,
        sub_offset,
    });
    let result = state
        .creation_service()
        .create(crate::application::creation::CreateCommand {
            sref: project.stream_ref(&name),
            key,
            content_type: hdr(&headers, "content-type").map(|_| content_type),
            ttl_secs,
            expires_at_ms,
            close,
            body,
            fork,
        })
        .await;
    let out = match result {
        Ok(v) => v,
        Err(e) => return creation_error_response(e),
    };
    let mut response = Response::builder()
        .status(if out.created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        })
        .header(header::CONTENT_TYPE, out.desc.content_type.clone())
        .header("Stream-Next-Offset", tail_token(out.next));
    if out.created {
        let host = hdr(&headers, "host").unwrap_or_else(|| "localhost".into());
        response = response.header(header::LOCATION, format!("http://{host}/v1/stream/{name}"));
    }
    if out.closed {
        response = response.header("Stream-Closed", "true");
    }
    response.body(Body::empty()).unwrap()
}

pub(crate) fn creation_error_response(
    error: crate::application::creation::CreationError,
) -> Response {
    use crate::application::creation::CreationFailure as F;
    let status = match error.kind {
        F::Invalid => StatusCode::BAD_REQUEST,
        F::Conflict => StatusCode::CONFLICT,
        F::Missing => StatusCode::NOT_FOUND,
        F::Gone => StatusCode::GONE,
        F::WrongKey => StatusCode::FORBIDDEN,
        F::Storage => StatusCode::INTERNAL_SERVER_ERROR,
        F::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        F::Overloaded => StatusCode::TOO_MANY_REQUESTS,
        F::Ambiguous => StatusCode::REQUEST_TIMEOUT,
        F::Opening => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut response = err_resp(status, error.code, &error.message);
    if let Some(owner) = error.owner.and_then(|v| v.parse().ok()) {
        response.headers_mut().insert("streams-replay-to", owner);
    }
    if let Some(retry) = error.retry_after.and_then(|v| v.to_string().parse().ok()) {
        response.headers_mut().insert("retry-after", retry);
    }
    response
}

async fn delete_stream(state: Arc<AppState>, sref: crate::tenant::TenantStreamRef) -> Response {
    match state.creation_service().delete(sref).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => creation_error_response(error),
    }
}
#[cfg(test)]
pub(crate) async fn release_fork_ref_for_test(
    state: &Arc<AppState>,
    source: crate::tenant::TenantStreamRef,
    fork_id: &str,
    epoch: &str,
) -> Result<bool, String> {
    state
        .creation_service()
        .release_fork_ref(source, fork_id, epoch)
        .await
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
    sref: crate::tenant::TenantStreamRef,
    headers: HeaderMap,
    body: Body,
    product_hash: Option<[u8; 16]>,
    product_key: Option<String>,
    seal_auth: Option<SealAuthz>,
) -> Response {
    render_append(
        append_typed(
            state,
            sref,
            headers,
            body,
            product_hash,
            product_key,
            seal_auth,
        )
        .await,
    )
}
pub(crate) async fn append_typed(
    state: Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    headers: HeaderMap,
    body: Body,
    product_hash: Option<[u8; 16]>,
    product_key: Option<String>,
    seal_auth: Option<SealAuthz>,
) -> crate::application::append::AppendResult {
    use crate::application::append::{AppendCommand, AppendKey};
    let service = state.append_service();
    let credential = match raw_key(&headers, &state) {
        None => AppendKey::Missing,
        Some(raw) => match StreamKey::from_b64(raw) {
            Ok(key) => AppendKey::Provided(key),
            Err(_) => AppendKey::Invalid,
        },
    };
    let prepared = service.prepare(&sref, credential).await?;
    let producer = parse_producer(&headers).map_err(|message| {
        AppendFailure::new(FailureClass::Invalid, AppendCode::InvalidProducer, message)
    })?;
    let close = want_close(&headers);
    if let Some(declared) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        && declared > state.config.cli.max_request_body_bytes
    {
        const DRAIN_CAP: usize = 8 * 1024 * 1024;
        if declared <= DRAIN_CAP {
            use futures_util::StreamExt;
            let mut stream = body.into_data_stream();
            let mut seen = 0;
            while let Some(Ok(chunk)) = stream.next().await {
                seen += chunk.len();
                if seen > DRAIN_CAP {
                    break;
                }
            }
        }
        return fail(
            FailureClass::Invalid,
            AppendCode::BodyTooLarge,
            &format!(
                "request body {declared} exceeds the {}-byte limit",
                state.config.cli.max_request_body_bytes
            ),
        );
    }
    service.check_memory().await?;
    let (body, body_charge) = buffer_body_charged(
        body,
        state.config.cli.max_request_body_bytes,
        state.quotas.pressure_handle(sref.project_id()),
    )
    .await
    .map_err(|_| {
        AppendFailure::new(
            FailureClass::Invalid,
            AppendCode::TooLarge,
            "body too large",
        )
    })?;
    if headers.contains_key("x-seal-final") {
        return fail(
            FailureClass::Invalid,
            AppendCode::UnknownField,
            "x-seal-final is not a request header",
        );
    }
    if headers.contains_key("stream-key") {
        return fail(
            FailureClass::Invalid,
            AppendCode::UnknownField,
            "Stream-Key was removed: routing keys live on /v1/streams (Prisma-Routing-Key)",
        );
    }
    let routing_key = product_key.unwrap_or_default();
    let close_identity = {
        let hv = |name: &str| hdr(&headers, name).unwrap_or_default();
        crate::application::lifecycle::seal_op_id_semantic(
            &create_request_hash(
                &prepared.descriptor().content_type,
                None,
                None,
                true,
                &body,
                None,
            ),
            &routing_key,
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
    let command = AppendCommand {
        sref,
        expected_epoch: Some(prepared.descriptor().epoch()),
        key: prepared.key().clone(),
        body,
        producer,
        content_type: hdr(&headers, "content-type"),
        routing_key,
        close,
        seal_auth,
        request_hash: product_hash,
        sequence: hdr(&headers, "stream-seq"),
        ts_hint_ms: parse_ts_hint(&headers),
        key_version: key_version(&headers),
        close_identity: Some(close_identity),
        body_charge,
    };
    service.execute_prepared(prepared, command).await
}

pub(crate) use crate::application::lifecycle::SealAuthz;

#[cfg(test)]
pub(crate) async fn fence_segment_for_key(
    state: &Arc<AppState>,
    sref: &crate::tenant::TenantStreamRef,
    epoch: &str,
    key: &str,
    generation: u64,
) -> Result<bool, crate::application::lifecycle::SealError> {
    crate::application::lifecycle::fence_segment_for_key(
        &state.lifecycle_service(),
        sref,
        epoch,
        key,
        generation,
    )
    .await
}

#[cfg(test)]
pub(crate) use crate::application::read::TEST_ASSERT_KEYED_DENSE;
#[cfg(test)]
pub(crate) use crate::application::read::read_merged;

pub(crate) fn interval_cursor(req_cursor: Option<&str>) -> String {
    interval_cursor_at(now_ms() as u64, req_cursor)
}

/// The explicit-time core of `interval_cursor` (WP-15 seam): a
/// 20-second interval counter; a request cursor at or beyond the
/// current interval echoes `r + 1`, anything else yields the current
/// interval. Pure — tests pin exact outputs for fixed instants.
pub(crate) fn interval_cursor_at(now_ms: u64, req_cursor: Option<&str>) -> String {
    let interval = now_ms / 20_000;
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

pub(crate) enum StartPos {
    At(u64),
    Now,
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
    let sref = state.deployment.raw_adapter_sref(&name);
    read_inner(
        state,
        sref,
        params,
        headers,
        head_only,
        true,
        SseSurface::Raw,
    )
    .await
}

// ---- SSE ----

/// #267: bounded-deadline SSE send. The queue is 4 slots and the
/// correct slow-consumer policy on a durable, cursor-addressable
/// stream is DISCONNECT + resume-from-cursor — never a private replay
/// buffer. Returns false when the subscriber should be dropped
/// (receiver gone or deadline elapsed with the queue still full).
pub(crate) async fn sse_send(tx: &SseTx, b: Bytes) -> bool {
    // Zero billable weight: controls, cursors, terminals, keep-alives.
    sse_send_billed(tx, b, 0, 0).await
}

pub(crate) type SseTx = tokio::sync::mpsc::Sender<crate::sse::auth::SseChunk>;

/// Bounded send carrying the chunk's BILLABLE weight. Metering does
/// NOT happen here: `GatedSseBody` meters at the authoritative yield
/// boundary, so a queued frame the gate discards at a cutoff is never
/// charged (round-9 review).
pub(crate) async fn sse_send_billed(
    tx: &SseTx,
    b: Bytes,
    payload_bytes: u64,
    records: u64,
) -> bool {
    // A billable payload with zero record weight would silently skip
    // the yield-boundary metering (round-10 hardening).
    debug_assert!(payload_bytes == 0 || records > 0);
    let chunk = crate::sse::auth::SseChunk {
        bytes: b,
        payload_bytes,
        records,
    };
    let ok = matches!(
        tokio::time::timeout(Duration::from_secs(10), tx.send(chunk)).await,
        Ok(Ok(()))
    );
    if !ok {
        crate::sse::auth::sse_stats::DISCONNECT_SEND_TIMEOUT
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    ok
}

/// #267: RAII connection slot against the instance SSE budget. Held by
/// the response stream's map closure — dropping the body releases it
/// (PR 6-B: the admission controller's ticket).
pub(crate) type SseSlot = crate::admission::SubscriptionTicket;

/// Acquire an SSE connection slot or produce the typed 503. The gate
/// exists so subscriber memory exhausts SUBSCRIPTION capacity, not the
/// shared RSS line that sheds unrelated appends.
pub(crate) fn sse_acquire(state: &Arc<AppState>) -> Result<SseSlot, Box<Response>> {
    state
        .admission
        .subscribe()
        .map_err(|crate::admission::SubscriptionRefusal| {
            let mut r = err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "subscription_capacity",
                "instance at live-subscription capacity; retry another instance or later",
            );
            r.headers_mut()
                .insert("retry-after", axum::http::HeaderValue::from_static("5"));
            Box::new(r)
        })
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

/// #268: one prepared hub event — the combined data+control text every
/// subscriber of this stream shares (product surface; the signed key
/// cursor is deterministic per stream+offset, so it is shareable).
/// `ctl_at` is the control's cursor offset: mid-batch events name
/// off+1; the batch-LAST event names the batch's scan_next so a
/// resuming client skips trailing non-matching records (#272).
#[allow(clippy::too_many_arguments)]
/// L3a field wedge: the instance's file-descriptor limit (~2k) was
/// the wall at ~1.5k parked SSE + writer + S3 sockets — EMFILE stalls
/// accepts, WAL->S3 flushes and therefore appends, while parked
/// clients see silence. Raise the soft limit to the hard limit at boot
/// and expose headroom so the canary can see it coming.
pub(crate) fn raise_nofile() -> crate::config::validation::DescriptorLimits {
    use crate::config::validation::DescriptorLimits;
    #[cfg(unix)]
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            if lim.rlim_cur < lim.rlim_max {
                let mut want = lim;
                want.rlim_cur = lim.rlim_max;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &want) == 0 {
                    lim = want;
                }
            }
            return DescriptorLimits {
                soft: std::num::NonZeroU64::new(lim.rlim_cur),
                hard: std::num::NonZeroU64::new(lim.rlim_max),
            };
        }
    }
    // PR 6-B: no probe = no ceiling, typed — never a zero sentinel.
    DescriptorLimits::default()
}

pub(crate) static NOFILE_SOFT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) static NOFILE_HARD: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Open descriptors right now (Linux: /proc/self/fd; elsewhere 0).
pub(crate) fn open_fds() -> u64 {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count() as u64)
        .unwrap_or(0)
}

/// Wedge diagnostic: a 1 s ticker records the gap between consecutive
/// ticks; the WATERMARK survives the stall so a post-mortem scrape
/// says whether the runtime's workers were blocked (std lock across an
/// await, CPU starvation) as opposed to a logical deadlock on a
/// healthy runtime. Spawned once from serve_h1.
pub(crate) static RUNTIME_LAST_TICK_MS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);
pub(crate) static RUNTIME_MAX_GAP_MS: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);

pub(crate) fn spawn_runtime_watchdog(tasks: &crate::tasks::TaskSupervisor) {
    let _ = tasks.spawn(
        "runtime-watchdog",
        crate::tasks::Policy::Noncritical,
        move |cancel| async move {
            let mut last = crate::shard::now_ms();
            RUNTIME_LAST_TICK_MS.store(last, std::sync::atomic::Ordering::Relaxed);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                }
                let now = crate::shard::now_ms();
                let gap = now - last;
                last = now;
                RUNTIME_LAST_TICK_MS.store(now, std::sync::atomic::Ordering::Relaxed);
                RUNTIME_MAX_GAP_MS.fetch_max(gap, std::sync::atomic::Ordering::Relaxed);
            }
        },
    );
}

/// The single-segment SSE dispatch: the livefeed session (round 11.8:
/// the only engine) with the typed segmented-at-dispatch retryable.
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
    #[cfg(test)]
    crate::failpoints::pause_sse_before_lease_gate(&desc.name).await;
    let slot = match sse_acquire(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    // LIVE-FEED Stage 3+/5: the transition engine. Product default-key
    // AND keyed lanes ride it on single-segment streams; FORKS join
    // here too through the shared typed read adapter.
    if desc
        .segments
        .as_ref()
        .is_none_or(|m| m.segments.len() <= 1 && m.pending.is_none())
    {
        let rk_filter = params.key.clone();
        let src = Arc::new(crate::sse::source::SingleSource {
            state: state.read_service(),
            rk_filter: rk_filter.clone(),
            desc: desc.clone(),
            key: key.clone(),
            epoch,
            route: desc
                .resolve_segment(params.key.as_deref().unwrap_or(""))
                .shard_route,
            engine: engine.clone(),
            handle: handle.clone(),
        });
        return crate::sse::session::serve(
            state, desc, key, epoch, src, start, params, rk_filter, surface, slot,
        )
        .await;
    }
    // Round-11.8: the legacy direct/hub producers are DELETED — the
    // livefeed arm above serves every single-segment connection. A
    // topology that changed between the caller's dispatch and here
    // (fresh split; pending transition) answers the typed retryable
    // and helps the transition along; the retry re-dispatches through
    // the lineage path.
    let _ = state.read_service().topology.schedule(&desc);
    err_resp(
        StatusCode::SERVICE_UNAVAILABLE,
        "segment_transition",
        "the stream segmented at dispatch; retry",
    )
}

// ---- per-key ordering read surface (PER-KEY-ORDERING.md §4) ----

pub(crate) use crate::peer::{client as peer_client, encode_stream_name_path};

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
    let url = state.peer.url_for(&owner)?;
    Some((owner, url))
}

/// Fleet-internal fan-out target: a keyed, segment-positioned read
/// served strictly from local ownership (no_fanout). Bearer-gated with
/// the fleet's shared token; the internal max-bytes/deliver headers are
/// honored only here so the public raw grammar stays pinned.
/// Follow-up review finding 2: the RECEIVER side of cross-owner
/// collection sealing. A coordinator whose descriptor says a live
/// segment belongs elsewhere relays the close HERE; this instance
/// re-derives every target fact from its own registry (project from
/// the sender header, epoch/identity re-checked against it), requires
/// LOCAL ownership (a wrong owner answers 409 + Streams-Replay-To so
/// the relay can follow one redirect), and submits the same idempotent
/// close primitive collection sealing uses. Idempotent per identity:
/// a retried close answers the segment's frozen next offset.
#[derive(Deserialize)]
struct SegmentCloseParams {
    seg_id: u32,
    seal_gen: Option<u64>,
}
async fn internal_segment_close(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<SegmentCloseParams>,
    headers: HeaderMap,
) -> Response {
    if !fleet_operation_authorized(&state, &headers, InternalOperation::SegmentClose) {
        return internal_unauthorized();
    }
    let sref = match crate::product::internal_sref(&headers, &name) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let desc = match state.registry.get(&sref).await {
        Ok(Some(desc)) if desc_alive(&desc) => desc,
        Ok(_) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                &e.to_string(),
            );
        }
    };
    if let Err(r) = crate::product::verify_internal_target(&desc, &headers) {
        return r;
    }
    // The claimed identity must equal what THIS registry derives.
    let identity = desc.dynamic_segment_identity(params.seg_id);
    let claimed = headers
        .get("streams-internal-identity")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if claimed != crate::crypto::hex(&identity) {
        return err_resp(
            StatusCode::CONFLICT,
            "target_mismatch",
            "segment identity does not match this stream's lineage",
        );
    }
    let Some(route) = desc.segment_route_by_id(params.seg_id) else {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "unknown_segment",
            "segment is not part of this incarnation",
        );
    };
    let engine = match state.engine_for_quiet(&route).await {
        Ok(e) => e,
        Err(resp) => {
            // Not the owner (or unresolvable): answer the typed
            // ownership response verbatim so the coordinator follows
            // exactly one redirect.
            return resp;
        }
    };
    match crate::scaler3::close_segment_on_engine(
        &engine,
        identity,
        &route,
        params.seg_id,
        params.seal_gen,
    )
    .await
    {
        Some(next_offset) => {
            axum::Json(serde_json::json!({ "next_offset": next_offset })).into_response()
        }
        None => err_resp(
            StatusCode::SERVICE_UNAVAILABLE,
            "seal_incomplete",
            "the segment close did not commit; retry",
        ),
    }
}

async fn internal_segment_read(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(mut params): Query<ReadParams>,
    uri: axum::http::Uri,
    headers: HeaderMap,
) -> Response {
    if !fleet_operation_authorized(&state, &headers, InternalOperation::SegmentRead) {
        return internal_unauthorized();
    }
    params.no_fanout = true;
    params.internal = true;
    // Round-4 finding 2: this is a BOUNDED relay page, not a live
    // surface — no parked subscription may hang off an internal page
    // route. Peer relays never send `live`; a caller that does is
    // answered 400 rather than silently downgraded to one page.
    if params.live.is_some() {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "live_unsupported",
            "/v1/internal/segment-read serves bounded pages only; live semantics are not offered",
        );
    }
    // Clamped to the SAME server-side ceiling the public read obeys: an
    // internal budget header must not buy a bigger page than the
    // operation it is relaying on behalf of (round-19 finding).
    params.max_bytes = headers
        .get("streams-internal-max-bytes")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, MAX_READ_BYTES));
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
        sref,
        params,
        headers,
        head_only,
        true,
        SseSurface::Raw,
    )
    .await
}

// ---- internal metrics stream flusher (old-impl pattern: __stream_metrics__) ----

#[cfg(test)]
mod tests {

    /// interval_cursor_at is a pure function of (now, request cursor):
    /// exact outputs for fixed instants, both arms and both fallbacks.
    #[test]
    fn interval_cursor_at_is_exact() {
        let now = 90_000_000 * 20_000; // interval 90_000_000 exactly
        assert_eq!(interval_cursor_at(now, None), "90000000");
        // request cursor at/above the current interval echoes r + 1:
        assert_eq!(interval_cursor_at(now, Some("90000000")), "90000001");
        assert_eq!(interval_cursor_at(now, Some("95000000")), "95000001");
        // long-past request cursor yields the current interval:
        assert_eq!(interval_cursor_at(now, Some("5")), "90000000");
        // unparseable request cursor likewise:
        assert_eq!(interval_cursor_at(now, Some("junk")), "90000000");
        // epoch boundary:
        assert_eq!(interval_cursor_at(0, None), "0");
        assert_eq!(interval_cursor_at(19_999, None), "0");
        assert_eq!(interval_cursor_at(20_000, None), "1");
    }

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
}

#[path = "http/read.rs"]
mod read_adapter;
pub(crate) use read_adapter::{meter_read_outcome, read_inner, read_payload, serve_read_sse};

// Fixture adapters preserve existing scenario calls without making AppState a
// production application dependency.
#[cfg(test)]
pub(crate) fn touch_ttl(state: &Arc<AppState>, desc: &StreamDesc) {
    let _ = state.creation_service().touch_ttl(desc);
}
#[cfg(test)]
impl AppState {
    pub async fn engine_for_scaler(&self, hash: &[u8; 16]) -> Option<Arc<ShardEngine>> {
        self.shards
            .resolve(hash, crate::shard_directory::Adoption::Internal)
            .await
            .ok()
    }
}
