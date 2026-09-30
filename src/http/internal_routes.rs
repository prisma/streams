//! The fleet-internal surface: the operation vocabulary a workload
//! credential is scoped by, and the `/v1/internal/...` route table
//! (plans19 split-producer-lineage §9 D3). Every route here demands one
//! exact [`InternalOperation`] and is served strictly from local
//! ownership; the table mounts inside `super::router`, beneath its
//! in-flight tracking and origin marker layers.
use std::sync::Arc;

use axum::Router;
use axum::http::Method;
use axum::routing::{get, post};

use super::AppState;

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
