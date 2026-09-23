//! Append application owner: authorization, semantic planning and one durable
//! commit submission. Public protocols consume the same typed contract.
mod admission;
mod close;
mod content;
mod contract;
mod route;
mod submit;
use crate::crypto::derive_subkey;
use crate::registry::Registry;
use crate::shard::{AppendReq, now_ms};
pub(crate) use content::stored_records;
pub(crate) use contract::fail;
pub(crate) use contract::{
    AppendCode, AppendCommand, AppendFailure, AppendKey, AppendOutcome, AppendResult,
    AuthorizedAppend, FailureClass, parse_producer, product_request_hash,
};
use std::sync::Arc;
use tokio::sync::oneshot;
const APPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) struct AppendService {
    pub(crate) registry: Arc<Registry>,
    pub(crate) shards: crate::shard_directory::ShardDirectory,
    pub(crate) admission: crate::admission::AdmissionController,
    pub(crate) quotas: crate::quota::QuotaRegistry,
    pub(crate) history: Arc<crate::history::HistoryResources>,
    pub(crate) usage: Arc<crate::usage::UsageService>,
    pub(crate) scaler: Arc<crate::scaler3::Scaler>,
    pub(crate) keys: Arc<crate::history::KeyCache>,
    pub(crate) watches: Arc<crate::application::watch::WatchService>,
    pub(crate) lifecycle: crate::application::lifecycle::LifecycleService,
    pub(crate) creation: Arc<crate::application::creation::CreationService>,
    pub(crate) auth: Arc<crate::auth::AuthService>,
    pub(crate) deployment: crate::deployment::DeploymentIdentity,
    pub(crate) admission_config: crate::config::AdmissionConfig,
    pub(crate) meter_enabled: bool,
}
impl AppendService {
    fn maintenance_limits(&self) -> crate::backpressure::Limits {
        crate::backpressure::Limits::from_config(&self.admission_config)
    }
    pub(crate) async fn prepare(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        key: AppendKey,
    ) -> Result<AuthorizedAppend, AppendFailure> {
        let descriptor = match self.registry.get(sref).await {
            Ok(Some(desc))
                if crate::application::creation::desc_alive(&desc) && desc.init.is_some() =>
            {
                return Err(AppendFailure::new(
                    FailureClass::Unavailable,
                    AppendCode::Creating,
                    "stream is still being created; retry",
                )
                .retry(1));
            }
            Ok(Some(desc)) if crate::application::creation::desc_alive(&desc) => desc,
            Ok(desc) => {
                let gone = desc
                    .as_ref()
                    .is_some_and(|d| crate::application::creation::retained_for_forks(d, now_ms()));
                return Err(AppendFailure::new(
                    if gone {
                        FailureClass::Gone
                    } else {
                        FailureClass::Missing
                    },
                    if gone {
                        AppendCode::Gone
                    } else {
                        AppendCode::NotFound
                    },
                    if gone {
                        "stream deleted; live forks remain"
                    } else {
                        "stream not found"
                    },
                ));
            }
            Err(error) => {
                return Err(AppendFailure::new(
                    FailureClass::Internal,
                    AppendCode::Internal,
                    error.to_string(),
                ));
            }
        };
        let key = match key {
            AppendKey::Provided(key)
                if key.fingerprint(&descriptor.epoch()) == descriptor.key_fingerprint =>
            {
                key
            }
            AppendKey::Missing => {
                return Err(AppendFailure::new(
                    FailureClass::Invalid,
                    AppendCode::MissingKey,
                    "Stream-Encryption-Key required",
                ));
            }
            _ => {
                return Err(AppendFailure::new(
                    FailureClass::Denied,
                    AppendCode::WrongKey,
                    "key mismatch",
                ));
            }
        };
        if self.admission.admit_write_inflight().is_err() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            return Err(AppendFailure::new(
                FailureClass::Capacity,
                AppendCode::Overloaded,
                "instance at admission capacity; retry",
            )
            .retry(1));
        }
        Ok(AuthorizedAppend { descriptor, key })
    }
    pub(crate) async fn check_memory(&self) -> Result<(), AppendFailure> {
        if self
            .admission
            .admit_write_memory(self.history.budget.reserved_bytes())
            .is_err()
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            return Err(AppendFailure::new(
                FailureClass::Capacity,
                AppendCode::Overloaded,
                "instance memory pressure; retry",
            )
            .retry(2));
        }
        Ok(())
    }
    pub(crate) async fn execute(&self, command: AppendCommand) -> AppendResult {
        let prepared = self
            .prepare(&command.sref, AppendKey::Provided(command.key.clone()))
            .await?;
        self.check_memory().await?;
        self.execute_prepared(prepared, command).await
    }
    pub(crate) async fn execute_prepared(
        &self,
        prepared: AuthorizedAppend,
        mut command: AppendCommand,
    ) -> AppendResult {
        if prepared.descriptor.sref() != command.sref || prepared.key.0 != command.key.0 {
            return fail(
                FailureClass::Internal,
                AppendCode::Internal,
                "append preparation mismatch",
            );
        }
        if command.expected_epoch.is_none() {
            command.expected_epoch = Some(prepared.descriptor.epoch());
        }
        // Four attempts; the waits before the second, third and fourth.
        let mut waits = [10, 20, 30]
            .map(std::time::Duration::from_millis)
            .into_iter();
        let mut prepared = prepared;
        loop {
            // `sealed` never resets within an incarnation and freezes the
            // map: a sealed descriptor's route cannot be stale.
            let final_route = prepared.descriptor.sealed;
            let result = execute_once(self, prepared, &mut command).await;
            let closed = result.as_ref().err();
            let Some(attempted) = closed.and_then(|e| e.engine_closed_segment()) else {
                return result;
            };
            if final_route || self.closure_is_current(&command, attempted).await? {
                return result;
            }
            let Some(wait) = waits.next() else {
                break;
            };
            tokio::time::sleep(wait).await;
            let key = AppendKey::Provided(command.key.clone());
            prepared = self.prepare(&command.sref, key).await?;
        }
        fail(
            FailureClass::Unavailable,
            AppendCode::SegmentTransition,
            "segment map transition did not converge; retry",
        )
        .map_err(|e| e.retry(1))
    }

    /// An engine's closed segment stream is the collection's closure only
    /// while the CURRENT map still routes this key to that segment, live.
    /// A descriptor that predates a transition meets the transition's own
    /// seal instead (the write-side twin of the reads' genuine_closure): a
    /// transition may delay a writer, it never looks like finality. So a
    /// refresh that cannot be read proves nothing and answers retryable,
    /// and a pending transition is waited out before the retry.
    async fn closure_is_current(
        &self,
        command: &AppendCommand,
        attempted: u32,
    ) -> Result<bool, AppendFailure> {
        use crate::application::read::TopologyResume;
        let unproven = |error: String| {
            AppendFailure::new(
                FailureClass::Unavailable,
                AppendCode::SegmentTransition,
                error,
            )
            .retry(1)
        };
        self.registry.invalidate(&command.sref);
        let fresh = self.registry.get(&command.sref).await;
        let Some(desc) = fresh.map_err(|error| unproven(error.to_string()))? else {
            // Gone: the retry's prepare answers for a missing collection.
            return Ok(false);
        };
        if command
            .expected_epoch
            .is_some_and(|epoch| epoch != desc.epoch())
        {
            // The closure belongs to a replaced incarnation (reads fence
            // the same refresh): it says nothing about this stream.
            return fail(
                FailureClass::Conflict,
                AppendCode::TargetIncarnationChanged,
                "append target incarnation changed",
            );
        }
        if desc.segments.as_ref().is_some_and(|m| m.pending.is_some()) {
            let ticket = self.lifecycle.topology.schedule(&desc);
            let ticket = ticket.map_err(|error| unproven(error.to_string()))?;
            let waited = ticket.wait().await;
            waited.map_err(|error| unproven(error.to_string()))?;
            return Ok(false);
        }
        let seg = desc.resolve_segment(&command.routing_key);
        Ok(seg.seg_id == attempted && !seg.sealed)
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "execute_once; one append validates, admits, commits and settles in the order the retry contract fixes; splitting it would separate the steps from the retry that orders them"
)]
async fn execute_once(
    state: &AppendService,
    prepared: AuthorizedAppend,
    command: &mut AppendCommand,
) -> AppendResult {
    let mut desc = prepared.descriptor;
    if command
        .expected_epoch
        .is_some_and(|epoch| epoch != desc.epoch())
    {
        return fail(
            FailureClass::Conflict,
            AppendCode::TargetIncarnationChanged,
            "append target incarnation changed",
        );
    }
    let key = prepared.key;
    let epoch = desc.epoch();
    #[cfg(test)]
    let name = desc.sref().name().as_str().to_string();
    let body = command.body.clone();
    let close = command.close;
    let product_hash = command.request_hash;
    let mut producer = command.producer.clone();
    if let (Some(producer), Some(hash)) = (producer.as_mut(), product_hash) {
        producer.request_hash = Some(hash);
    }
    let close_only = close && body.is_empty();
    let mut close_plan = close::prepare_close(state, &desc, command, producer).await?;
    let content = content::parse_content(
        &state.usage,
        &desc,
        command,
        state.admission.record_ceiling(),
        close_plan.producer.is_some(),
    )?;
    state.creation.renew_ttl(&desc).await.map_err(|error| {
        AppendFailure::new(
            FailureClass::Unavailable,
            AppendCode::TtlRenewal,
            error.to_string(),
        )
        .retry(1)
    })?;
    close::install_intent(state, &desc, command, &content, &mut close_plan).await?;
    let content::ContentPlan { entries, deferred } = content;
    let close_carries_content = !entries.is_empty();
    let usage_c = admission::admit_usage(
        &state.usage,
        &desc,
        close_only,
        deferred.is_none(),
        body.len(),
        entries.len(),
    )?;
    let routing_key = command.routing_key.clone();
    let seg = route::resolve_segment(state, &mut desc, &routing_key).await?;
    let hash = seg.identity;
    let _stream_slot = match state.admission.stream_slot(seg.identity) {
        Ok(s) => s,
        Err(_) => {
            return Err(AppendFailure::new(
                FailureClass::Capacity,
                AppendCode::StreamOverloaded,
                "too many concurrent requests for this stream",
            )
            .retry(1));
        }
    };
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
    if !close_only && deferred.is_none() {
        let fed: usize = entries.iter().map(|e| e.len()).sum();
        state
            .scaler
            .note_append(&desc, &seg, fed as u64, entries.len() as u64);
    }
    state.usage.link_storage(
        crate::crypto::RouteHash::for_stream(&desc.sref()),
        crate::crypto::SegmentHash(hash),
    );
    let kv = command.key_version;
    let subkey = derive_subkey(&key, &epoch, &routing_key, kv);
    state.keys.put(hash, key, epoch);

    let touch = state.watches.append_touch(&desc, &entries);

    let bytes = entries.iter().map(|e| e.len()).sum();
    #[cfg(test)]
    if !close {
        crate::failpoints::pause_append_before_enqueue(&name).await;
    } else {
        crate::failpoints::pause_close_before_enqueue(&name).await;
    }
    drop(command.body_charge.take());
    let (tx, rx) = oneshot::channel();
    let appended_records = entries.len();
    let req = AppendReq {
        enqueued_at: std::time::Instant::now(),
        hash,
        route: seg.shard_route,
        entries,
        usage: usage_c,
        routing_key,
        key_hash: seg.key_hash.0,
        producer_lineage,
        key_version: kv,
        subkey,
        ts_hint_ms: command.ts_hint_ms,
        seq: command.sequence.clone(),
        bytes,
        finish: if close {
            crate::shard::AppendFinish::Close
        } else {
            crate::shard::AppendFinish::Open
        },
        producer: close_plan.producer.clone(),
        deferred_error: deferred,
        sealed_reject_new: close_plan.sealed_reject_new,
        touch,
        seal_gen: close_plan.generation,
        billing: (!crate::billing::is_reserved_stream(&desc.name) && state.meter_enabled).then(
            || {
                std::sync::Arc::new(crate::billing::BillingRef {
                    identity: crate::billing::identity_with_capabilities(
                        &state.auth,
                        &state.deployment,
                        &desc,
                        true,
                    ),
                    segment_id: seg.seg_id,
                })
            },
        ),
        resp: tx,
    };
    let outcome = submit::submit(state, &desc, &seg, req, rx).await?;

    close::complete(
        state,
        &desc,
        command,
        &close_plan,
        close_carries_content,
        &outcome,
    )
    .await?;
    match outcome {
        Ok(ack) => Ok(crate::application::append::AppendOutcome {
            seg_id: seg.seg_id,
            materialized: desc.segments.is_some(),
            next_offset: ack.next_offset,
            last_offset: ack.last_offset,
            duplicate: ack.duplicate,
            closed: ack.closed,
            producer: ack.producer.filter(|_| !close_plan.synthetic_producer),
            appended_records,
        }),
        Err(error) => Err(AppendFailure::from_commit(
            seg.seg_id,
            desc.segments.is_some(),
            error,
        )),
    }
}
