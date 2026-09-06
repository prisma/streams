use super::{AppendCode, AppendCommand, AppendFailure, AppendService, FailureClass, fail};
use crate::registry::StreamDesc;
use crate::shard::{AppendErr, ProducerReq, SealedReject};

pub(super) struct ClosePlan {
    pub(super) operation: String,
    pub(super) generation: Option<u64>,
    pub(super) owed_final: bool,
    pub(super) synthetic_producer: bool,
    pub(super) producer: Option<ProducerReq>,
    pub(super) sealed_reject_new: Option<SealedReject>,
}

/// Authenticate a final's execution token and renew only its own persisted claim.
pub(super) async fn prepare_close(
    state: &AppendService,
    desc: &StreamDesc,
    command: &AppendCommand,
    mut producer: Option<ProducerReq>,
) -> Result<ClosePlan, AppendFailure> {
    let body = &command.body;
    let close = command.close;
    let close_only = close && body.is_empty();
    let seal_auth = &command.seal_auth;
    let this_close_op = command.close_identity.clone().unwrap_or_else(|| {
        let producer_fields = producer
            .as_ref()
            .map(|p| [p.id.clone(), p.epoch.to_string(), p.seq.to_string()])
            .unwrap_or_default();
        crate::application::lifecycle::seal_op_id_semantic(
            &crate::application::creation::create_request_hash(
                &desc.content_type,
                None,
                None,
                true,
                body,
                None,
            ),
            &command.routing_key,
            &[
                producer_fields[0].clone(),
                producer_fields[1].clone(),
                producer_fields[2].clone(),
                command.sequence.clone().unwrap_or_default(),
                String::new(),
                command.content_type.clone().unwrap_or_default(),
                String::new(),
            ],
        )
    });
    if let Some(auth) = seal_auth {
        let holds = desc.stream_epoch == auth.epoch
            && desc.sealing.as_ref().is_some_and(|sl| {
                sl.operation_id == auth.op_id && sl.claim_generation == auth.generation
            });
        if !holds {
            return fail(
                FailureClass::Conflict,
                AppendCode::SealSuperseded,
                "the seal this final record belongs to no longer holds its claim",
            );
        }
    }
    let is_owed_final = desc.sealing.as_ref().is_some_and(|sl| {
        sl.owes_final()
            && (sl.operation_id == this_close_op
                || Some(sl.operation_id.as_str()) == seal_auth.as_ref().map(|a| a.op_id.as_str()))
    });
    let mut raw_seal_gen: Option<u64> = seal_auth.as_ref().map(|a| a.generation);
    if is_owed_final && seal_auth.is_none() {
        match crate::application::lifecycle::renew_owed_claim(
            &state.lifecycle,
            &desc.sref(),
            &this_close_op,
            &desc.stream_epoch,
        )
        .await
        {
            Ok(Some(g)) => raw_seal_gen = Some(g),
            Ok(None) => {
                return fail(
                    FailureClass::Conflict,
                    AppendCode::Sealed,
                    "the seal this close was resuming has been superseded",
                );
            }
            Err(e) => {
                return fail(
                    FailureClass::Unavailable,
                    AppendCode::Internal,
                    &e.to_string(),
                );
            }
        }
    }

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
        return Err(closed_tail_failure(state, desc).await);
    }
    Ok(ClosePlan {
        operation: this_close_op,
        generation: raw_seal_gen,
        owed_final: is_owed_final,
        synthetic_producer,
        producer,
        sealed_reject_new,
    })
}

async fn closed_tail_failure(state: &AppendService, desc: &StreamDesc) -> AppendFailure {
    let seg = desc.resolve_segment("");
    let engine = match state
        .shards
        .resolve(&seg.shard_route, crate::shard_directory::Adoption::External)
        .await
    {
        Ok(engine) => engine,
        Err(error) => return AppendFailure::from_resolve(error),
    };
    let handle = match engine.stream_handle(seg.identity).await {
        Ok(handle) => handle,
        Err(error) => {
            return AppendFailure::new(
                FailureClass::Unavailable,
                AppendCode::Internal,
                error.to_string(),
            );
        }
    };
    let next = handle.state.lock().unwrap().durable.next;
    AppendFailure::from_commit(
        seg.seg_id,
        desc.segments.is_some(),
        AppendErr::Closed { next_offset: next },
    )
}

/// Publish intent only after deterministic validation; malformed closes leave no debt.
pub(super) async fn install_intent(
    state: &AppendService,
    desc: &StreamDesc,
    command: &AppendCommand,
    content: &super::content::ContentPlan,
    plan: &mut ClosePlan,
) -> Result<(), AppendFailure> {
    let close = command.close;
    let is_owed_final = plan.owed_final;
    let seal_auth = &command.seal_auth;
    let deferred = &content.deferred;
    let entries = &content.entries;
    let this_close_op = &plan.operation;
    #[cfg(test)]
    let name = desc.sref().name().as_str().to_string();
    if close && !desc.sealed && !is_owed_final && deferred.is_none() && seal_auth.is_none() {
        let intent = if entries.is_empty() {
            crate::registry::SealIntent::Empty
        } else {
            crate::registry::SealIntent::Final {
                routing_key: command.routing_key.clone(),
                request_hash: this_close_op.clone(),
                final_committed: false,
            }
        };
        match crate::application::lifecycle::begin_sealing_for_close(
            &state.lifecycle,
            &desc.sref(),
            intent,
            &desc.stream_epoch,
        )
        .await
        {
            Ok(g) => {
                if let Some(g) = g {
                    plan.generation = Some(g);
                }
            }
            Err(e) => return fail(FailureClass::Conflict, AppendCode::Sealed, &e.to_string()),
        }
        #[cfg(test)]
        if crate::failpoints::should_stop_after_seal_intent(&name) {
            return fail(
                FailureClass::Unavailable,
                AppendCode::Failpoint,
                "stopped after the seal intent",
            );
        }
    }

    Ok(())
}

pub(super) async fn complete(
    state: &AppendService,
    desc: &StreamDesc,
    command: &AppendCommand,
    plan: &ClosePlan,
    carries_content: bool,
    outcome: &Result<crate::shard::AppendAck, AppendErr>,
) -> Result<(), AppendFailure> {
    if outcome.is_ok() {
        state.creation.touch_ttl(desc);
    }
    if command.close && command.seal_auth.is_none() {
        crate::application::lifecycle::complete_raw_close(
            &state.lifecycle,
            desc,
            crate::application::lifecycle::RawClose {
                operation: &plan.operation,
                generation: plan.generation,
                carries_content,
                resumes_owed_final: plan.owed_final,
            },
            outcome,
        )
        .await
        .map_err(|error| {
            AppendFailure::new(
                FailureClass::Unavailable,
                AppendCode::SealIncomplete,
                error.to_string(),
            )
        })?;
    }
    Ok(())
}
