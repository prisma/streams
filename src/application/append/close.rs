use super::{AppendCode, AppendCommand, AppendFailure, AppendService, FailureClass, fail};
use crate::registry::StreamDesc;
use crate::shard::{AppendErr, ProducerReq, SealedReject};

pub(super) struct ClosePlan {
    pub(super) operation: String,
    pub(super) generation: Option<u64>,
    pub(super) owed_final: bool,
    /// This attempt installed the claim it carries, after its own content
    /// was validated. Only such an attempt's definitive refusal proves its
    /// final undeliverable; an exact retry that joined or renewed another
    /// attempt's claim leaves it to that attempt (TLA-003-F5).
    pub(super) installed_claim: bool,
    pub(super) synthetic_producer: bool,
    pub(super) producer: Option<ProducerReq>,
    pub(super) sealed_reject_new: Option<SealedReject>,
}

/// Authenticate a final's execution token and find the owed claim this close
/// resumes. An exact retry joins that claim at the generation it observed;
/// only [`install_intent`], once the retry's content is valid here, renews it.
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
    // Only a close resumes an owed final. The close flag is not part of the
    // operation identity, so a plain append with the final's body and
    // coordination would otherwise pass as its exact retry: it would skip
    // the Sealing refusal, renew the claim and land the record twice.
    let owed_claim = desc.sealing.as_ref().filter(|sl| {
        close
            && sl.owes_final()
            && (sl.operation_id == this_close_op
                || Some(sl.operation_id.as_str()) == seal_auth.as_ref().map(|a| a.op_id.as_str()))
    });
    let is_owed_final = owed_claim.is_some();
    let raw_seal_gen = match seal_auth {
        Some(auth) => Some(auth.generation),
        None => owed_claim.map(|sl| sl.claim_generation),
    };

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
        installed_claim: false,
        synthetic_producer,
        producer,
        sealed_reject_new,
    })
}

#[expect(
    clippy::unwrap_used,
    reason = "closed_tail_failure; the stream-state lock read for a declared closure's tail may be poisoned while it holds a half-advanced durable frontier; recovering it could report a closed length never made durable"
)]
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
    AppendFailure::declared_closed(seg.seg_id, desc.segments.is_some(), next)
}

/// Publish or renew intent only after deterministic validation: malformed
/// closes leave no debt. Validation includes this instance's ingest capacity
/// and record ceiling, which may be below the instance that accepted the
/// claim. An exact retry refused by them, 413 in `parse_content` or with a
/// deferred refusal, therefore leaves the owed claim as it found it (TLA-003-F4);
/// the deferred one still reaches the committer, which acknowledges a
/// committed final as a duplicate so the retry marks and seals it.
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
    if is_owed_final && seal_auth.is_none() && deferred.is_none() {
        plan.generation = Some(renew_owed_final(state, desc, this_close_op).await?);
    }
    if close && !desc.sealed && !is_owed_final && deferred.is_none() && seal_auth.is_none() {
        let carries_final = !entries.is_empty();
        let intent = if carries_final {
            crate::registry::SealIntent::Final {
                routing_key: command.routing_key.clone(),
                request_hash: this_close_op.clone(),
                final_committed: false,
            }
        } else {
            crate::registry::SealIntent::Empty
        };
        match crate::application::lifecycle::begin_sealing_for_close(
            &state.lifecycle,
            &desc.sref(),
            intent,
            &desc.stream_epoch,
        )
        .await
        {
            Ok(Some((g, installed))) => {
                plan.generation = Some(g);
                plan.installed_claim = installed;
                // The claim is now this operation's (installed, taken over or
                // renewed). The admission snapshot may have shown another
                // operation's claim: its Sealing refusal no longer applies, or
                // this final would be refused as Closed and release the claim
                // it just took (TLA-003-F2).
                if carries_final {
                    plan.owed_final = true;
                    plan.sealed_reject_new = None;
                }
            }
            Ok(None) => {}
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

/// Renew an owed-final claim for its own exact retry: fresh lease, fresh
/// generation, so no fence or takeover can stand above the active retry.
async fn renew_owed_final(
    state: &AppendService,
    desc: &StreamDesc,
    operation: &str,
) -> Result<u64, AppendFailure> {
    let renewed = crate::application::lifecycle::renew_owed_claim(
        &state.lifecycle,
        &desc.sref(),
        operation,
        &desc.stream_epoch,
    )
    .await;
    match renewed {
        Ok(Some(generation)) => Ok(generation),
        Ok(None) => fail(
            FailureClass::Conflict,
            AppendCode::Sealed,
            "the seal this close was resuming has been superseded",
        ),
        Err(e) => fail(
            FailureClass::Unavailable,
            AppendCode::Internal,
            &e.to_string(),
        ),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "complete; completion takes the state, descriptor, command, the close plan with whether this attempt installed its claim, the content flag and the outcome as the close resolved them; a request struct would exist only for this signature"
)]
pub(super) async fn complete(
    state: &AppendService,
    desc: &StreamDesc,
    command: &AppendCommand,
    plan: &ClosePlan,
    carries_content: bool,
    outcome: &Result<crate::shard::AppendAck, AppendErr>,
) -> Result<(), AppendFailure> {
    if command.close && command.seal_auth.is_none() {
        crate::application::lifecycle::complete_raw_close(
            &state.lifecycle,
            desc,
            crate::application::lifecycle::RawClose {
                operation: &plan.operation,
                generation: plan.generation,
                carries_content,
                resumes_owed_final: plan.owed_final,
                installed_claim: plan.installed_claim,
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
