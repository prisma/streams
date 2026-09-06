//! The seal coordinator owns claim, generation fence, final-record authority,
//! physical closure and terminal publication. Durable phases are resumable;
//! only a definitive rejection can release an undelivered final intent.
use crate::registry::{Mutation, MutationResult, PersistedDescriptor, Registry};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct LifecycleService {
    pub(crate) registry: Arc<Registry>,
    pub(crate) topology: crate::application::topology::TopologyService,
    pub(crate) clock: Arc<dyn crate::runtime::Clock>,
    pub(crate) cert_sealed_publish_delay_ms: Arc<std::sync::atomic::AtomicU64>,
}
impl LifecycleService {
    fn alive(&self, descriptor: &PersistedDescriptor) -> bool {
        !descriptor.deleted
            && !descriptor.soft_deleted
            && descriptor
                .expires_at_ms
                .is_none_or(|expiry| self.clock.now().ms() < expiry)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SealError {
    Conflict(String),
    Resumable(String),
    Missing,
    ChangedIncarnation,
    AlreadySealed,
    OtherOperation,
    OwedFinal,
    InvalidClaim,
    Storage(String),
}
impl std::fmt::Display for SealError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(message) | Self::Storage(message) => f.write_str(message),
            Self::Resumable(message) => write!(f, "{message}; the seal is resumable"),
            Self::Missing => f.write_str("collection not found"),
            Self::ChangedIncarnation => {
                f.write_str("the collection this seal was issued against no longer exists")
            }
            Self::AlreadySealed => f.write_str("collection is already sealed"),
            Self::OtherOperation => {
                f.write_str("the collection sealed under a different operation")
            }
            Self::OwedFinal => f.write_str("this seal has not committed its final record yet"),
            Self::InvalidClaim => {
                f.write_str("the seal intent this record belongs to is no longer in flight")
            }
        }
    }
}
impl std::error::Error for SealError {}

pub(crate) struct FinalSealRequest<'a> {
    pub(crate) stream: &'a crate::tenant::TenantStreamRef,
    pub(crate) epoch: &'a str,
    pub(crate) operation: &'a str,
    pub(crate) routing_key: &'a str,
}

pub(crate) struct FinalRecordAck {
    pub(crate) closed: bool,
}

pub(crate) struct FinalRecordFailure<E> {
    pub(crate) error: E,
    pub(crate) disposition: FinalDisposition,
}

#[derive(Debug)]
pub(crate) enum SealFinalError<E> {
    Append(E),
    Lifecycle(SealError),
    SequenceReused,
}

/// The final record's entire lifecycle has one authority. Cancellation or an
/// ambiguous append reply leaves its durable promise available to an exact
/// retry. Neither a dropped future nor an HTTP status can release that debt.
pub(crate) async fn seal_final<E, F, Fut>(
    state: &LifecycleService,
    request: FinalSealRequest<'_>,
    append: F,
) -> Result<(), SealFinalError<E>>
where
    F: FnOnce(SealAuthz) -> Fut,
    Fut: std::future::Future<Output = Result<FinalRecordAck, FinalRecordFailure<E>>>,
{
    let intent = crate::registry::SealIntent::Final {
        routing_key: request.routing_key.to_string(),
        request_hash: request.operation.to_string(),
        final_committed: false,
    };
    #[cfg(test)]
    crate::failpoints::pause_product_seal_before_claim(request.stream.name().as_str()).await;
    let claim = enter_sealing(
        state,
        request.stream,
        request.operation,
        intent,
        request.epoch,
    )
    .await
    .map_err(SealFinalError::Lifecycle)?;
    let SealClaim::Active(ticket) = claim else {
        return Ok(());
    };
    let auth = SealAuthz {
        op_id: request.operation.to_string(),
        epoch: ticket.epoch.clone(),
        generation: ticket.generation,
    };
    let release = || {
        abandon_seal_intent(
            state,
            request.stream,
            request.operation,
            &ticket.epoch,
            ticket.generation,
        )
    };
    let ack = match append(auth).await {
        Ok(ack) => ack,
        Err(failure) => {
            if failure.disposition == FinalDisposition::DefinitivelyRejected
                && let Err(error) = release().await
            {
                tracing::error!(stream = %request.stream.name().as_str(), %error, "releasing refused final claim");
            }
            return Err(SealFinalError::Append(failure.error));
        }
    };
    if !ack.closed {
        if let Err(error) = release().await {
            tracing::error!(stream = %request.stream.name().as_str(), %error, "releasing non-closing final claim");
        }
        return Err(SealFinalError::SequenceReused);
    }
    mark_final_committed(
        state,
        request.stream,
        request.operation,
        &ticket.epoch,
        ticket.generation,
    )
    .await
    .map_err(SealFinalError::Lifecycle)?;
    run_seal(
        state,
        request.stream,
        Some(request.operation.to_string()),
        &ticket.epoch,
        Some(ticket.generation),
    )
    .await
    .map_err(SealFinalError::Lifecycle)
}

/// Install a seal intent, classifying every outcome. THE serialization
/// point: it installs only over a descriptor that is open, unclaimed
/// and topologically quiet, so once it wins, phase A cannot start a new
/// transition (phase A refuses sealing) and phase B cannot publish one
/// (phase B refuses sealing).
pub(crate) async fn enter_sealing_cas(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<EnterSeal, SealError> {
    let outcome = state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if !state.alive(current) {
                return Mutation::Decline(EnterSeal::Missing);
            }
            decide_claim(current, op_id, intent, state.clock.now().ms())
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    Ok(match outcome {
        MutationResult::Applied(outcome) | MutationResult::Declined(outcome) => outcome,
        MutationResult::Missing | MutationResult::IncarnationChanged => EnterSeal::Missing,
    })
}

/// Drive [`enter_sealing_cas`] to a decision, resolving topology when it
/// is what stands in the way.
pub(crate) async fn claim_seal(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<EnterSeal, SealError> {
    for _ in 0..6 {
        match enter_sealing_cas(state, sref, op_id, intent, expect_epoch).await? {
            EnterSeal::PendingTopology => {
                // Finish the transition, then race for the intent again.
                crate::application::topology::resume(&state.topology, sref).await;
                state.registry.invalidate(sref);
            }
            EnterSeal::AbandonedClaim {
                old_op,
                old_gen,
                old_intent,
            } => {
                match take_over_abandoned(
                    state,
                    sref,
                    expect_epoch,
                    op_id,
                    intent,
                    &old_op,
                    old_gen,
                    &old_intent,
                )
                .await?
                {
                    Some(outcome) => return Ok(outcome),
                    // The claim moved while we were fencing (renewed,
                    // completed, replaced): whatever it is now decides.
                    None => state.registry.invalidate(sref),
                }
            }
            other => return Ok(other),
        }
    }
    Err(SealError::Resumable(
        "a split or merge kept the collection busy".into(),
    ))
}

/// Take over a lapsed final-bearing claim — the ONLY way one is ever
/// replaced, and the wall clock is never the whole argument. Order:
///
/// 1. RESERVE a generation above the old one (a descriptor CAS that
///    only bumps the allocator; the claim is untouched and must still
///    be exactly the lapsed one we saw).
/// 2. FENCE the old generation through the committer of the segment
///    the old final targeted. The fence is processed in queue order,
///    so its answer proves every append enqueued before it — the old
///    operation's final included, however long it sat — has been
///    decided, and no append below the reservation can commit after.
/// 3. Consult the fence's closed-report. CLOSED means the old final
///    won its race after all: its record is durable and the segment is
///    shut, so the old transition is COMPLETED on its behalf and the
///    caller is told the collection sealed under the other operation.
///    NOT CLOSED means it can never commit now — only then does the
///    new claim replace the old, expecting it unchanged.
///
/// A timestamp decides only when this protocol may START; whether the
/// old operation is really gone is decided by the fence.
#[allow(clippy::too_many_arguments)]
async fn take_over_abandoned(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    old_op: &str,
    old_gen: u64,
    old_intent: &crate::registry::SealIntent,
) -> Result<Option<EnterSeal>, SealError> {
    let reservation = state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if !current.sealing.as_ref().is_some_and(|claim| {
                claim.operation_id == old_op && claim.claim_generation == old_gen
            }) {
                return Mutation::Decline(None);
            }
            let mut next = current.to_persisted();
            next.seal_gen_counter += 1;
            let reserved = next.seal_gen_counter;
            Mutation::Write(next, Some(reserved))
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    let MutationResult::Applied(Some(reserved)) = reservation else {
        return Ok(None);
    };
    // 2/3. Fence the old final's segment and read the verdict.
    let routing_key = match old_intent {
        crate::registry::SealIntent::Final { routing_key, .. } => routing_key.clone(),
        // Only owed finals are ever taken over.
        crate::registry::SealIntent::Empty => String::new(),
    };
    let closed = fence_segment_for_key(state, sref, expect_epoch, &routing_key, reserved).await?;
    if closed {
        // The old operation's close committed: its record is durable
        // and its segment shut. Finish ITS transition — the record must
        // not be stranded behind an unmarked intent — and report the
        // collection sealed under the other operation.
        mark_final_committed(state, sref, old_op, expect_epoch, old_gen).await?;
        // Boxed: completing the old transition re-enters run_seal ->
        // claim_seal, and the compiler needs the cycle broken.
        Box::pin(run_seal(
            state,
            sref,
            Some(old_op.to_string()),
            expect_epoch,
            Some(old_gen),
        ))
        .await?;
        return Ok(Some(EnterSeal::AlreadySealed));
    }
    // 4. Install the new claim over the (still unchanged) old one.
    if install_reserved_claim(
        state,
        sref,
        expect_epoch,
        old_op,
        old_gen,
        op_id,
        intent,
        reserved,
    )
    .await?
    {
        return Ok(Some(EnterSeal::Installed {
            generation: reserved,
        }));
    }
    Ok(None)
}

/// The takeover's installation CAS: replace the (still unchanged)
/// lapsed claim with the new one — and ONLY if the caller's
/// reservation is still the NEWEST allocation. Two takeovers can
/// reserve against the same lapsed claim (the reservation deliberately
/// leaves it in place); both fence, and the segment keeps the higher
/// fence. If the LOWER reservation then installed, the live claim's
/// generation would sit below the fence and every close it issues
/// would be refused: a collection held Sealing by its own recovery
/// protocol. The counter check makes the newest reservation the only
/// installable one; an older one restarts the protocol from the top.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn install_reserved_claim(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    old_op: &str,
    old_gen: u64,
    op_id: &str,
    intent: &crate::registry::SealIntent,
    reserved: u64,
) -> Result<bool, SealError> {
    let outcome = state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if !current.sealing.as_ref().is_some_and(|claim| {
                claim.operation_id == old_op
                    && claim.claim_generation == old_gen
                    && current.seal_gen_counter == reserved
            }) {
                return Mutation::Decline(());
            }
            let mut next = current.to_persisted();
            next.sealing = Some(crate::registry::SealState {
                operation_id: op_id.to_string(),
                intent: intent.clone(),
                claimed_ms: state.clock.now().ms(),
                claim_generation: reserved,
            });
            Mutation::Write(next, ())
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    Ok(matches!(outcome, MutationResult::Applied(())))
}

pub(crate) async fn enter_sealing(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    intent: crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<SealClaim, SealError> {
    // The epoch is the caller's VALIDATED one — never re-fetched here.
    // A second lookup between validation and claim was the ABA window.
    match claim_seal(state, sref, op_id, &intent, expect_epoch).await? {
        EnterSeal::Installed { generation } | EnterSeal::AlreadyOurs { generation } => {
            Ok(SealClaim::Active(SealTicket {
                epoch: expect_epoch.to_string(),
                generation,
            }))
        }
        // Exact replay is an explicit successful claim outcome.
        EnterSeal::AlreadyCompleted => Ok(SealClaim::Completed),
        EnterSeal::AlreadySealed => Err(SealError::AlreadySealed),
        EnterSeal::Conflicting(m) => Err(SealError::Conflict(m)),
        EnterSeal::Missing => Err(SealError::Missing),
        EnterSeal::PendingTopology => {
            Err(SealError::Resumable("a split or merge is in flight".into()))
        }
        EnterSeal::AbandonedClaim { .. } => unreachable!("claim_seal resolves abandoned claims"),
    }
}

/// Publish the Sealing intent for a RAW close, before the physical
/// segment closes. Refuses when another operation still owes a final
/// record — that seal must finish first, or its record would be lost.
pub(crate) async fn begin_sealing_for_close(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    intent: crate::registry::SealIntent,
    expect_epoch: &str,
) -> Result<Option<u64>, SealError> {
    // The intent's request_hash IS the operation id: one identity,
    // computed once by the request that owns it. The epoch is the
    // ADMISSION descriptor's — the close is fenced to the incarnation
    // it was admitted against, like every other lifecycle decision.
    let op = match &intent {
        crate::registry::SealIntent::Empty => String::new(),
        crate::registry::SealIntent::Final { request_hash, .. } => request_hash.clone(),
    };
    match claim_seal(state, sref, &op, &intent, expect_epoch).await? {
        EnterSeal::Installed { generation } | EnterSeal::AlreadyOurs { generation } => {
            Ok(Some(generation))
        }
        EnterSeal::AlreadyCompleted => Ok(None),
        EnterSeal::AlreadySealed => Ok(None), // already terminal; the close is a no-op
        EnterSeal::Missing => Ok(None),
        EnterSeal::Conflicting(m) => Err(SealError::Conflict(m)),
        EnterSeal::PendingTopology => Err(SealError::Resumable(
            "a split or merge is in flight; retry the close".into(),
        )),
        EnterSeal::AbandonedClaim { .. } => unreachable!("claim_seal resolves abandoned claims"),
    }
}

/// Renew an owed-final claim for its OWN exact retry: fresh lease,
/// fresh generation. Returns the new generation, or None when the
/// claim is no longer this operation's to renew.
pub(crate) async fn renew_owed_claim(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    expect_epoch: &str,
) -> Result<Option<u64>, SealError> {
    let outcome = state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if !current
                .sealing
                .as_ref()
                .is_some_and(|claim| claim.operation_id == op_id && claim.owes_final())
            {
                return Mutation::Decline(None);
            }
            let mut next = current.to_persisted();
            next.seal_gen_counter += 1;
            let generation = next.seal_gen_counter;
            let claim = next.sealing.as_mut().expect("claim was observed");
            claim.claim_generation = generation;
            claim.claimed_ms = state.clock.now().ms();
            Mutation::Write(next, Some(generation))
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    Ok(match outcome {
        MutationResult::Applied(generation) => generation,
        MutationResult::Declined(_)
        | MutationResult::Missing
        | MutationResult::IncarnationChanged => None,
    })
}

/// Release an intent this operation owns and has NOT committed. Only
/// ever our own, only ever while it still owes its record — a seal that
/// already wrote its final is finished by `run_seal`, not undone here.
pub(crate) async fn abandon_seal_intent(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    expect_epoch: &str,
    expect_gen: u64,
) -> Result<(), SealError> {
    state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            let releasable = current.sealing.as_ref().is_some_and(|claim| {
                claim.operation_id == op_id
                    && claim.claim_generation == expect_gen
                    && !matches!(
                        claim.intent,
                        crate::registry::SealIntent::Final {
                            final_committed: true,
                            ..
                        }
                    )
            });
            if !releasable {
                return Mutation::Decline(());
            }
            let mut next = current.to_persisted();
            next.sealing = None;
            Mutation::Write(next, ())
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    Ok(())
}

/// Record that a final-bearing seal's record is durable. Must happen
/// before any segment closes: after this the transition can be finished
/// by anyone, and before it, only by the operation that owes the record.
pub(crate) async fn mark_final_committed(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op_id: &str,
    expect_epoch: &str,
    expect_gen: u64,
) -> Result<(), SealError> {
    let outcome = state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if current.sealed && current.seal_op.as_deref() == Some(op_id) {
                return Mutation::Decline(true);
            }
            let Some(claim) = &current.sealing else {
                return Mutation::Decline(false);
            };
            if claim.operation_id != op_id || claim.claim_generation != expect_gen {
                return Mutation::Decline(false);
            }
            if !claim.owes_final() {
                return Mutation::Decline(true);
            }
            let mut next = current.to_persisted();
            if let crate::registry::SealIntent::Final {
                final_committed, ..
            } = &mut next.sealing.as_mut().expect("claim was observed").intent
            {
                *final_committed = true;
            }
            Mutation::Write(next, true)
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    match outcome {
        MutationResult::Applied(true)
        | MutationResult::Declined(true)
        | MutationResult::Missing => Ok(()),
        MutationResult::IncarnationChanged => Err(SealError::ChangedIncarnation),
        _ => Err(SealError::InvalidClaim),
    }
}

/// The collection seal transition (audit P0). Open -> Sealing -> every
/// live segment closed -> Sealed. Idempotent and resumable: any request
/// that observes Sealing finishes the same transition, so a crash
/// between the final append, the segment closes and publication can
/// never leave a descriptor claiming sealed over writable segments.
///
/// `op` names the seal operation when a final record is part of it, so
/// a retry resumes instead of appending a second final record.
pub(crate) async fn run_seal(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op: Option<String>,
    expect_epoch: &str,
    claim_gen: Option<u64>,
) -> Result<(), SealError> {
    let Some(ticket) = prepare_execution(state, sref, op.clone(), expect_epoch, claim_gen).await?
    else {
        return Ok(());
    };
    close_claimed_segments(state, sref, &ticket).await?;
    publish_sealed(state, sref, op, &ticket).await
}

/// Establish the operation and incarnation before any physical close. No
/// request with an owed final receives execution authority here.
async fn prepare_execution(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op: Option<String>,
    expect_epoch: &str,
    claim_gen: Option<u64>,
) -> Result<Option<SealTicket>, SealError> {
    let desc = match state.registry.get(sref).await {
        Ok(Some(d)) if state.alive(&d) => d,
        Ok(_) => return Ok(None),
        Err(e) => return Err(SealError::Storage(e.to_string())),
    };
    // The transition this call drives belongs to ONE incarnation. A
    // name-scoped run_seal re-fetched whatever descriptor owned the
    // name and could claim and seal a replacement created while the
    // caller was in flight.
    if desc.stream_epoch != expect_epoch {
        return Err(SealError::ChangedIncarnation);
    }
    if desc.sealed {
        // Terminal — but only OUR terminal counts as our success. A
        // caller driving a specific operation must not report
        // completion because somebody else's seal got there first.
        if let Some(o) = op.as_deref()
            && !o.is_empty()
            && desc.seal_op.as_deref() != Some(o)
        {
            return Err(SealError::OtherOperation);
        }
        return Ok(None);
    }
    // An OWED final is decided by the claim path below, not by a
    // pre-read: a live claim answers Conflicting (the caller must let
    // that operation finish), and a lapsed one goes through the
    // takeover protocol — which is what makes a plain `:seal` a real
    // recovery tool instead of a permanent 409. The one caller who may
    // proceed while its claim is a Final is the OWNER after its mark
    // (final_committed=true no longer owes); an owner that has not
    // marked cannot get here, because segment closes and publication
    // both refuse an owing claim.
    if let Some(sl) = &desc.sealing
        && sl.owes_final()
        && op.as_deref() == Some(sl.operation_id.as_str())
    {
        return Err(SealError::OwedFinal);
    }
    // A topology transition in flight is resolved BEFORE the seal
    // takes its snapshot of live segments. Otherwise the two interleave:
    // the seal closes what it can see, publishes Sealed, and the
    // transition's phase B then publishes a fresh live child. Phase B
    // now refuses once the lifecycle has moved, and this is the other
    // half — finish the transition first so the snapshot is complete.
    // 1. Claim the transition. This CAS — not a preceding read — is the
    //    serialization point: it installs only over an open, unclaimed,
    //    topologically quiet descriptor, resolving a pending split or
    //    merge first. Installing Sealing over pending work deadlocked
    //    the collection, because phase B then refuses to finish it.
    let op_id = op.clone().unwrap_or_default();
    // The generation every close this call issues will carry. Owners
    // arrive with theirs (their claim is installed and, for a final,
    // already marked); everyone else claims here and uses what the
    // claim allocates.
    let mut our_gen = claim_gen;
    // Resuming a claim that is already ours by identity (a planted
    // recovery, a plain close joining a plain sealing): adopt its
    // standing generation — the segment closes below must carry it.
    if our_gen.is_none()
        && let Some(sl) = &desc.sealing
        && sl.operation_id == op_id
    {
        our_gen = Some(sl.claim_generation);
    }
    if desc.sealing.is_none()
        || desc
            .sealing
            .as_ref()
            .is_some_and(|s| s.operation_id != op_id)
    {
        match claim_seal(
            state,
            sref,
            &op_id,
            &crate::registry::SealIntent::Empty,
            expect_epoch,
        )
        .await?
        {
            EnterSeal::Installed { generation } | EnterSeal::AlreadyOurs { generation } => {
                our_gen = Some(generation);
            }
            EnterSeal::AlreadyCompleted => return Ok(None),
            EnterSeal::AlreadySealed if op_id.is_empty() => return Ok(None),
            EnterSeal::AlreadySealed => return Err(SealError::OtherOperation),
            EnterSeal::Missing => return Ok(None),
            EnterSeal::Conflicting(m) => return Err(SealError::Conflict(m)),
            EnterSeal::PendingTopology => {
                return Err(SealError::Resumable(
                    "a split or merge is in flight and did not settle".into(),
                ));
            }
            EnterSeal::AbandonedClaim { .. } => {
                unreachable!("claim_seal resolves abandoned claims")
            }
        }
    }
    Ok(Some(SealTicket {
        epoch: expect_epoch.to_string(),
        generation: our_gen.ok_or(SealError::InvalidClaim)?,
    }))
}

/// Close the claimed incarnation's complete live set. Partial progress remains
/// resumable and cannot publish a terminal descriptor.
async fn close_claimed_segments(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    ticket: &SealTicket,
) -> Result<(), SealError> {
    let expect_epoch = ticket.epoch.as_str();
    let our_gen = Some(ticket.generation);
    // 2. Close every live segment identity. Idempotent per segment.
    state.registry.invalidate(sref);
    let d = match state.registry.get(sref).await {
        Ok(Some(d)) => d,
        Ok(None) => return Ok(()),
        Err(e) => return Err(SealError::Storage(e.to_string())),
    };
    if d.stream_epoch != expect_epoch {
        return Err(SealError::ChangedIncarnation);
    }
    let live: Vec<u32> = match &d.segments {
        Some(m) => m
            .segments
            .iter()
            .filter(|s| s.is_live())
            .map(|s| s.seg_id)
            .collect(),
        None => vec![0],
    };
    for seg_id in live {
        if crate::application::topology::seal_segment_identity(&state.topology, &d, seg_id, our_gen)
            .await
            .is_none()
        {
            // A segment that would not close leaves the collection in
            // Sealing; the next seal request (or retry) resumes it.
            // (This includes a close refused by a seal fence: the claim
            // generation lapsed, and a retry — which renews it — is the
            // correct way back in.)
            return Err(SealError::Resumable(format!(
                "segment {seg_id} did not close"
            )));
        }
    }
    #[cfg(test)]
    if crate::failpoints::should_stop_before_sealed_publish(sref.name().as_str()) {
        // DST crash boundary: every live segment's close is durable,
        // the collection is NOT yet Sealed — the exact state a crash
        // between step 2 and step 3 leaves. The retry contract under
        // test: a plain :seal observes Sealing with closed segments,
        // renews the claim, re-closes idempotently, and publishes.
        return Err(SealError::Resumable(
            "stopped before the sealed publication".into(),
        ));
    }
    // Round-11.6 field canary: hold the close→publication window open
    // for the configured delay so the seal-herd campaign observes the
    // two-step gap on a real release binary. Boot refuses a nonzero
    // value outside STREAMS_CERTIFICATION_MODE=1; zero is inert.
    {
        let ms = state
            .cert_sealed_publish_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
    }
    Ok(())
}

/// Publication rechecks the generation after physical durability, then proves
/// terminal state against the same incarnation and operation.
async fn publish_sealed(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    op: Option<String>,
    ticket: &SealTicket,
) -> Result<(), SealError> {
    let expect_epoch = ticket.epoch.as_str();
    let our_gen = Some(ticket.generation);
    // 3. Publish SEALED only now — and only if no topology transition
    //    reappeared while the segments were closing.
    state
        .registry
        .mutate_incarnation(sref, expect_epoch, |current| {
            if current.sealed
                || current
                    .segments
                    .as_ref()
                    .is_some_and(|map| map.pending.is_some())
            {
                return Mutation::Decline(());
            }
            let Some(claim) = current.sealing.as_ref() else {
                return Mutation::Decline(());
            };
            if Some(claim.claim_generation) != our_gen || claim.owes_final() {
                return Mutation::Decline(());
            }
            let mut next = current.to_persisted();
            next.sealed = true;
            next.seal_op = Some(claim.operation_id.clone());
            next.sealing = None;
            Mutation::Write(next, ())
        })
        .await
        .map_err(|error| SealError::Storage(error.to_string()))?;
    state.registry.invalidate(sref);
    // Success is PROVEN, never assumed. The CAS above declines when a
    // transition reappeared or another writer moved the state, and
    // returning Ok regardless told clients `{"sealed": true}` about a
    // descriptor that was still Sealing with a split pending.
    let final_state = match state.registry.get(sref).await {
        Ok(Some(d)) => d,
        Ok(None) => return Ok(()), // gone: nothing left to seal
        Err(e) => return Err(SealError::Storage(e.to_string())),
    };
    if !state.alive(&final_state) {
        return Ok(());
    }
    // The proof is about THIS incarnation and THIS operation. A
    // replacement created (and even sealed) under the same name
    // between publication and this read is somebody else's resource;
    // reporting success against it violates the very guarantee the
    // rest of the machine establishes.
    if final_state.stream_epoch != expect_epoch {
        return Err(SealError::ChangedIncarnation);
    }
    if final_state.sealed && final_state.sealing.is_none() {
        if let Some(o) = op.as_deref()
            && !o.is_empty()
            && final_state.seal_op.as_deref() != Some(o)
        {
            return Err(SealError::OtherOperation);
        }
        return Ok(());
    }
    Err(SealError::Resumable(format!(
        "the seal did not reach a terminal state (sealed={}, sealing={}, pending={}); it is resumable",
        final_state.sealed,
        final_state.sealing.is_some(),
        final_state
            .segments
            .as_ref()
            .is_some_and(|m| m.pending.is_some())
    )))
}

/// Raise the seal fence on the segment a routing key resolves to, and
/// report whether that segment is closed. The message travels the same
/// queue as appends, so the committer answers it only after deciding
/// every append enqueued before it — the reply is a BARRIER: after a
/// `false`, no append below the fence can ever close the segment; a
/// `true` means the old operation's close already committed.
pub(crate) async fn fence_segment_for_key(
    state: &LifecycleService,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    routing_key: &str,
    fence_to: u64,
) -> Result<bool, SealError> {
    state.registry.invalidate(sref);
    let desc = match state.registry.get(sref).await {
        Ok(Some(d)) if d.stream_epoch == expect_epoch => d,
        Ok(_) => return Err(SealError::ChangedIncarnation),
        Err(e) => return Err(SealError::Storage(e.to_string())),
    };
    let seg = desc.resolve_segment(routing_key);
    let identity = desc.dynamic_segment_identity(seg.seg_id);
    let route = desc
        .segment_route_by_id(seg.seg_id)
        .ok_or(SealError::InvalidClaim)?;
    let engine = state
        .topology
        .shards
        .resolve(&route, crate::shard_directory::Adoption::Internal)
        .await
        .map_err(|_| SealError::Resumable("segment engine unavailable".into()))?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    engine
        .try_seal_fence(crate::shard::SealFenceReq {
            hash: identity,
            generation: fence_to,
            resp: tx,
        })
        .map_err(|_| SealError::Resumable("append queue full; fence not placed".into()))?;
    match rx.await {
        Ok(Ok(ack)) => Ok(ack.closed),
        Ok(Err(e)) => Err(SealError::Resumable(format!("fence refused: {e:?}"))),
        Err(_) => Err(SealError::Resumable("fence dropped".into())),
    }
}

mod claims;
use claims::decide_claim;
pub(crate) use claims::{
    EnterSeal, FinalDisposition, SealAuthz, SealClaim, SealTicket, final_err_disposition,
    seal_op_id_full, seal_op_id_semantic,
};
