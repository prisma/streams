//! A group subscription of the multiplexed wait: a consumer pull on one
//! stream, leased by the same walk as `POST {collection}/consumers/{c}:pull`
//! (WIRE-MATRIX §2.15) with no wait of its own.
//!
//! A group whose consumer is drained on every lineage segment (nothing past
//! its cursor unsettled, read from the segments' queue state) is not ready
//! and costs nothing: no configuration read and no Receive through the
//! committer. Any other group walks: once at once, then again only when a
//! segment's view moved since the walk that leased nothing, or one of the
//! consumer's leases expired, as a waiting pull walks (`pull_wait`). A walk
//! that leases is an answer; its leases start when the answer is built, and
//! a lost answer's leases expire after their visibility window, as a lost
//! pull answer's do.
use std::sync::Arc;

use serde_json::value::RawValue;
use tokio::time::Instant;

use super::super::consumer_failure_response;
use super::AppState;
use super::Refusal;
use super::wire::GroupSpec;
use crate::application::consumer::{
    ConsumerAccess, ConsumerService, LineageWatch, PULL_COVERAGE_BYTES, PullInput, Seen,
};
use crate::tenant::{Scope, TenantStreamRef};

pub(super) struct Group {
    sref: TenantStreamRef,
    consumer: String,
    key: String,
    max: Option<usize>,
    visibility_ms: Option<u64>,
    lineage: LineageWatch,
    /// The views the last walk that leased nothing was taken against, and
    /// the earliest live lease deadline then; `None` before the first walk.
    walked: Option<(Seen, Option<Instant>)>,
    /// The lineage's payload bytes when it was last seen drained: a walk
    /// woken from there reads what was appended since, with framing.
    drained_at: Option<u64>,
}

/// A rendered group answer: the pull answer's fields, its messages served
/// as the pull serialises them.
pub(super) struct GroupAnswer {
    pub(super) backlog: u64,
    pub(super) messages: Box<RawValue>,
}

impl Group {
    /// Authorize the consumer's stream as a pull does (scope, project, name,
    /// key) and watch its lineage. `Err` is the per-subscription answer.
    pub(super) async fn prepare(
        state: &Arc<AppState>,
        access: &ConsumerAccess<'_>,
        sref: TenantStreamRef,
        spec: GroupSpec,
    ) -> Result<Self, Refusal> {
        let GroupSpec {
            key,
            group: consumer,
            max,
            visibility_ms,
        } = spec;
        let service = state.consumer_service();
        let context = service
            .authorize(&sref, consumer.clone(), &key, access, Scope::ConsumersPull)
            .await
            .map_err(refused)?;
        let Some(lineage) = LineageWatch::of(&service, context.descriptor()).await else {
            return Err(Box::new(super::not_owner()));
        };
        let drained_at = lineage.drained(&consumer).then(|| lineage.payload());
        Ok(Self {
            sref,
            consumer,
            key,
            max,
            visibility_ms,
            lineage,
            walked: None,
            drained_at,
        })
    }

    /// Whether a walk may lease now.
    pub(super) fn ready(&self) -> bool {
        if self.lineage.drained(&self.consumer) {
            return false;
        }
        let Some((seen, lease)) = &self.walked else {
            return true;
        };
        if lease.is_some_and(|at| Instant::now() >= at) {
            return true;
        }
        self.lineage
            .seen(&self.consumer)
            .is_none_or(|(now, _)| &now != seen)
    }

    /// Record the lineage drained now (its payload), or not.
    pub(super) fn rearm(&mut self) {
        self.drained_at = self
            .lineage
            .drained(&self.consumer)
            .then(|| self.lineage.payload());
    }

    /// The earliest live lease deadline the last empty walk saw.
    pub(super) fn lease_deadline(&self) -> Option<Instant> {
        self.walked.as_ref().and_then(|(_, lease)| *lease)
    }

    /// Every lineage segment's commit notification, registered now.
    pub(super) fn notified(&self) -> Vec<tokio::sync::futures::Notified<'_>> {
        self.lineage.notified()
    }

    /// What a walk woken now may read: what was appended since the lineage
    /// was seen drained, with framing, or one whole coverage.
    pub(super) fn coverage(&self) -> u64 {
        let whole = PULL_COVERAGE_BYTES as u64;
        self.drained_at.map_or(whole, |at| {
            (self.lineage.payload().saturating_sub(at) + 4096).min(whole)
        })
    }

    /// One walk: `Ok(None)` when it leased nothing (the group re-arms on the
    /// views it was taken against), `Err` the per-subscription failure.
    pub(super) async fn walk(
        &mut self,
        state: &Arc<AppState>,
        access: &ConsumerAccess<'_>,
    ) -> Result<Option<GroupAnswer>, Refusal> {
        let seen = self.lineage.seen(&self.consumer);
        let service: Arc<ConsumerService> = state.consumer_service();
        let context = service
            .authorize(
                &self.sref,
                self.consumer.clone(),
                &self.key,
                access,
                Scope::ConsumersPull,
            )
            .await
            .map_err(refused)?;
        let context = service.active(context).await.map_err(refused)?;
        let input = PullInput {
            max: self.max,
            wait_ms: Some(0),
            visibility_ms: self.visibility_ms,
            park: Default::default(),
        };
        let out = crate::application::consumer::pull(context, input)
            .await
            .map_err(refused)?;
        if out.messages.is_empty() {
            self.walked = seen;
            if self.walked.is_none() {
                // A closed engine or a poisoned state: nothing to watch.
                return Err(Box::new(super::not_owner()));
            }
            return Ok(None);
        }
        self.walked = None;
        crate::billing::meter_pull(
            state,
            &out.descriptor,
            out.payload_bytes,
            out.messages.len() as u64,
        );
        let messages = serde_json::value::to_raw_value(&out.messages)
            .map_err(|e| Box::new(super::internal(&e.to_string())))?;
        Ok(Some(GroupAnswer {
            backlog: out.backlog,
            messages,
        }))
    }
}

/// A consumer failure as the subscription's refusal.
fn refused(failure: crate::application::consumer::ConsumerFailure) -> Refusal {
    Box::new(consumer_failure_response(failure))
}
