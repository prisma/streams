//! A watch subscription of the multiplexed wait: one watched key, observed
//! from the touch journal as `GET {collection}/watches/{w}/keys/{k}`
//! observes it (WIRE-MATRIX §2.18), proved by the request's account and the
//! stream key, never by a capability.
//!
//! A watch from `now` is answered at once with the journal's cursor, so no
//! touch between two requests goes unseen. A watch from a cursor is ready
//! when the journal reports a touch of its key or a resynchronisation. At
//! the request's deadline every watch still waiting is answered with the
//! journal's current cursor, as a per-stream wait's timeout is, so its
//! cursor never falls behind the journal's retained history.
use std::sync::Arc;
use std::time::Duration;

use super::super::watch_failure_response;
use super::AppState;
use super::Refusal;
use super::wire::WatchSpec;
use crate::application::watch::{
    Observation, VerifiedObservation, WatchAccess, WatchCredentials, WatchService,
};
use crate::tenant::TenantStreamRef;

pub(super) struct Watch<'a> {
    verified: VerifiedObservation<'a>,
    cursor: String,
}

impl<'a> Watch<'a> {
    /// Prove the observation as the per-stream wait does for an account:
    /// the principal's project and prefix grant, and the stream key.
    pub(super) async fn prepare(
        service: &WatchService,
        sref: &TenantStreamRef,
        spec: WatchSpec,
        access: WatchAccess<'a>,
    ) -> Result<Self, Refusal> {
        let WatchSpec {
            key,
            watch,
            watch_key,
            cursor,
        } = spec;
        let verified = service
            .authenticate(
                sref,
                watch,
                watch_key,
                WatchCredentials {
                    capability: None,
                    encryption_key: Some(key),
                },
                access,
            )
            .await
            .map_err(|e| Box::new(watch_failure_response(e)))?;
        Ok(Self { verified, cursor })
    }

    /// Whether the watch answers before waiting: a watch from `now`.
    pub(super) fn immediate(&self) -> bool {
        self.cursor == "now"
    }

    /// Observe from the watch's cursor for at most `timeout`.
    pub(super) async fn observe(
        &self,
        service: &WatchService,
        timeout: Duration,
    ) -> Result<Observation, Refusal> {
        service
            .observe(&self.verified, &self.cursor, timeout)
            .await
            .map_err(|e| Box::new(watch_failure_response(e)))
    }
}

/// Whether `observation` answers before the deadline (a timeout waits).
pub(super) fn ready(observation: &Observation) -> bool {
    !matches!(observation, Observation::Timeout { .. })
}

/// The watch service the request's watches observe through.
pub(super) fn service(state: &Arc<AppState>) -> Arc<WatchService> {
    state.watch_service()
}
