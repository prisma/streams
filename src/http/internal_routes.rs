//! The fleet-internal surface: the operation vocabulary a workload
//! credential is scoped by, and the `/v1/internal/...` route table
//! (plans19 split-producer-lineage §9 D3). Every route here demands one
//! exact [`InternalOperation`] and is served strictly from local
//! ownership; the table mounts inside `super::router`, beneath its
//! in-flight tracking and origin marker layers.
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::AppState;
use crate::shard::{SealFenceReq, ShardEngine};

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
    SealFence,
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
            Self::SealFence => "seal-fence",
            Self::SegmentScan => "segment-scan",
            Self::QueueCursor => "queue-cursor",
            Self::ConsumerSweep => "consumer-sweep",
            Self::TelemetryAppend => "telemetry-append",
        }
    }
}

/// §14.1: the raw surface's operation is derived from the request
/// METHOD, so a lifecycle token cannot append and an append token
/// cannot delete.
pub(super) trait RawOperation {
    fn raw_operation(&self) -> InternalOperation;
}

impl RawOperation for Method {
    fn raw_operation(&self) -> InternalOperation {
        match *self {
            Method::PUT | Method::DELETE => InternalOperation::RawLifecycle,
            Method::POST => InternalOperation::RawAppend,
            _ => InternalOperation::RawRead,
        }
    }
}

/// Every `/v1/internal/...` route. Each handler authorizes its one
/// operation before any registry or engine work, and none relays again
/// (depth one).
pub(super) fn table() -> Router<Arc<AppState>> {
    Router::new()
        // Fleet-internal segment fan-out target (bearer-gated): a keyed,
        // segment-positioned read served strictly from local ownership.
        // Peers relay here when a lineage crosses instances; the public
        // raw route keeps rejecting ?key= (audit P0 standards isolation).
        .route(
            "/v1/internal/segment-read/{*name}",
            get(super::internal_segment_read),
        )
        .route(
            "/v1/internal/segment-close/{*name}",
            post(super::internal_segment_close),
        )
        .route("/v1/internal/seal-fence/{*name}", post(internal_seal_fence))
        .route(
            "/v1/internal/sweep-segment/{*name}",
            post(super::internal_sweep_segment),
        )
        .route(
            "/v1/internal/queue-cursor/{*name}",
            get(super::internal_queue_cursor),
        )
        .route(
            "/v1/internal/segment-scan/{*name}",
            get(super::internal_segment_scan),
        )
        .route(
            "/v1/internal/telemetry-append/{*name}",
            post(super::telemetry_append::internal_telemetry_append),
        )
}

/// `?fence_to=`: the takeover's reserved generation. Required.
#[derive(Deserialize)]
struct SealFenceParams {
    fence_to: u64,
}

/// The owner committer's closed-report for the segment, taken when the
/// fence was staged behind every append and close queued before it.
#[derive(Serialize)]
struct SealFenceReply {
    closed: bool,
}

/// NEXT-WORK §5 F1-a: the RECEIVER of a relayed seal fence. A takeover
/// coordinated on an instance that does not own the old final's segment
/// fences it HERE, at the owner. This instance re-derives every target
/// fact from its own registry (the project from the sender's header, then
/// the incarnation, segment and identity), fences only a generation the
/// registry already reserved, and resolves the engine only as the ring
/// owner: a non-owner answers 409 + Streams-Replay-To and opens nothing.
/// The fence travels the committer queue appends and closes use, and the
/// 200 is sent only after its group is durable, so `closed: false` proves
/// that no append below the fence can close the segment afterwards and
/// `closed: true` that the earlier close committed. Repeats are idempotent
/// (the fence is the maximum). It never relays again.
async fn internal_seal_fence(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<SealFenceParams>,
    headers: HeaderMap,
) -> Response {
    if !super::fleet_operation_authorized(&state, &headers, InternalOperation::SealFence) {
        return super::internal_unauthorized();
    }
    let sref = match super::internal_sref(&headers, &name) {
        Ok(sref) => sref,
        Err(refusal) => return refusal,
    };
    // The coordinator re-read the registry before it reserved, so a cached
    // older incarnation must not answer stale_target for the live one. No
    // liveness gate: the local fence binds the incarnation alone, so a
    // soft-deleted or expired descriptor of it stays fenceable here too.
    state.registry.invalidate(&sref);
    let desc = match state.registry.get(&sref).await {
        Ok(Some(desc)) => desc,
        Ok(None) => return super::err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
        Err(e) => {
            return super::err_resp(
                StatusCode::SERVICE_UNAVAILABLE,
                "temporarily_unavailable",
                &e.to_string(),
            );
        }
    };
    let (seg_id, identity) = match super::verify_internal_target(&desc, &headers) {
        Ok(target) => target,
        Err(refusal) => return refusal,
    };
    // The fence is monotone: one above every reservation would refuse
    // every claim this incarnation could still install.
    if params.fence_to > desc.seal_gen_counter {
        return super::err_resp(
            StatusCode::CONFLICT,
            "fence_unreserved",
            "the fence generation was never reserved for this stream",
        );
    }
    let Some(route) = desc.segment_route_by_id(seg_id) else {
        return super::err_resp(
            StatusCode::BAD_REQUEST,
            "unknown_segment",
            "segment is not part of this incarnation",
        );
    };
    // Not the owner, opening, or failed to open: the directory's answer.
    match state.engine_for_quiet(&route).await {
        Ok(engine) => place_fence(&engine, identity, params.fence_to).await,
        Err(refusal) => refusal,
    }
}

/// Queue the fence and answer from its durable reply. Every outcome but
/// the committer's acknowledgement leaves the fence unconfirmed; a retry
/// raises it to the same maximum.
async fn place_fence(engine: &ShardEngine, identity: [u8; 16], fence_to: u64) -> Response {
    let unconfirmed =
        |why: &str| super::err_resp(StatusCode::SERVICE_UNAVAILABLE, "fence_unconfirmed", why);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let request = SealFenceReq {
        hash: identity,
        generation: fence_to,
        resp: tx,
    };
    if engine.try_seal_fence(request).is_err() {
        return unconfirmed("committer queue full or closed; fence not placed");
    }
    match rx.await {
        Ok(Ok(ack)) => Json(SealFenceReply { closed: ack.closed }).into_response(),
        Ok(Err(e)) => unconfirmed(&format!("fence refused: {e:?}")),
        Err(_) => unconfirmed("fence reply dropped"),
    }
}
