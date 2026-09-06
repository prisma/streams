//! Durably seed the child and anchor it to the exact retained source incarnation.
use super::deletion::release_fork_ref;
use super::raw::CreatePlan;
use super::*;

pub(super) async fn install(
    state: &Arc<CreationService>,
    plan: &CreatePlan,
    engine: &Arc<ShardEngine>,
    mut desc: StreamDesc,
    created: bool,
) -> Result<(StreamDesc, Option<Bytes>), CreationError> {
    let project = &plan.project;
    let name = &plan.name;
    let fork_ctx = &plan.fork_ctx;
    let hash = desc.resolve_segment("").identity;
    // Fork post-create (pinned DS fork contract): the tail row must be
    // seeded at the fork boundary BEFORE the first handle load caches
    // next = 0, and the source's reference count records this fork.
    let mut materialize_entry: Option<Bytes> = None;
    if let Some(fc) = &fork_ctx
        && created
    {
        if let Err(e) = engine
            .seed_fork_tail(
                hash,
                crate::crypto::RouteHash::for_stream(&desc.sref()).0,
                fc.boundary,
            )
            .await
        {
            return Err(CreationError::new(
                CreationFailure::Storage,
                "internal",
                &e.to_string(),
            ));
        }
        // Install the reference by unique id (idempotent set
        // insert). A CAS that DECLINES means the source vanished or
        // was tombstoned in the race — the audit found that treated
        // as success, leaving a live fork pointing at a deleted
        // source. The presence check below is the actual proof.
        let fork_id = desc.stream_epoch.clone();
        // Stamp the id into our OWN ForkRef so release can name it.
        desc = stamp_child(state, plan, desc, &fork_id).await?;
        // The CHILD must still exist to be worth anchoring: a
        // half-made child deleted in the stamp-to-install window
        // must not pin the source at all (FRK-013). This check
        // closes the ordinary path; the residual window between it
        // and the install CAS — including a crash inside it — is
        // repaired by the tombstone's RETAINED debt. The park sits
        // BETWEEN the check and the install so tests can drive
        // exactly that window.
        match state.registry.get(&project.stream_ref(name)).await {
            Ok(Some(c)) if desc_alive(&c) && c.stream_epoch == desc.stream_epoch => {}
            Err(error) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &format!("verifying fork child: {error}"),
                ));
            }
            Ok(_) => {
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "fork_target_changed",
                    "the fork target changed while it was being created; retry",
                ));
            }
        }
        #[cfg(test)]
        crate::failpoints::pause_fork_before_source_ref(name).await;
        match state
            .registry
            .mutate_incarnation(&fc.source_desc.sref(), &fc.source_desc.stream_epoch, |d| {
                // The reference is installed on the incarnation the
                // child actually forked. Between validating the
                // source and getting here it can be recreated,
                // start expiring, begin sealing or begin a split —
                // and a reference installed on the wrong one leaves
                // a child whose data nobody is keeping.
                // Already installed — idempotent, and checked FIRST:
                // a source retained for THIS child is soft-deleted
                // by definition, so demanding liveness here refused
                // the very retry the retention exists to serve.
                if d.fork_children.iter().any(|c| c == &fork_id) {
                    return Mutation::Decline(!d.deleted);
                }
                if d.deleted
                    || d.soft_deleted
                    || d.init.is_some()
                    || d.stream_epoch != fc.source_desc.stream_epoch
                    || d.sealing.is_some()
                    || d.segments.as_ref().is_some_and(|m| {
                        m.pending.is_some() || m.segments.iter().filter(|s| s.is_live()).count() > 1
                    })
                {
                    return Mutation::Decline(false);
                }
                let mut next = d.to_persisted();
                next.fork_children.push(fork_id.clone());
                Mutation::Write(next, true)
            })
            .await
        {
            Ok(MutationResult::Applied(true) | MutationResult::Declined(true)) => {}
            Ok(_) => {
                // The source moved: recreated, deleted, sealing, or
                // splitting. The child exists but nothing is
                // keeping its data, so refuse rather than hand back
                // a fork with no anchor. Deleting the half-made
                // child is the delete path's job — which is now
                // resumable — so report the conflict and let the
                // caller retry against the current source.
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "fork_source_changed",
                    "the fork source changed while the fork was being created; retry",
                ));
            }
            Err(e) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &e.to_string(),
                ));
            }
        }
        state.registry.invalidate(&fc.source_desc.sref());
        #[cfg(test)]
        crate::failpoints::pause_fork_after_source_ref(name).await;
        // Post-install: the child can have been deleted between the
        // pre-check and the install CAS. Release the reference this
        // request just installed — its tombstone's retained debt
        // covers the crash variant of the same window.
        match state.registry.get(&project.stream_ref(name)).await {
            Ok(Some(c)) if desc_alive(&c) && c.stream_epoch == desc.stream_epoch => {}
            Err(error) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &format!("verifying fork child: {error}"),
                ));
            }
            Ok(_) => {
                if let Err(m) = release_fork_ref(
                    state,
                    fc.source_desc.sref(),
                    &fork_id,
                    &fc.source_desc.stream_epoch,
                )
                .await
                {
                    tracing::error!(stream = %name, "releasing a dead child's fresh reference: {m}");
                }
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "fork_target_changed",
                    "the fork target changed while it was being created; retry",
                ));
            }
        }
        match state.registry.get(&fc.source_desc.sref()).await {
            Ok(Some(sd)) if sd.fork_children.iter().any(|c| c == &fork_id) => {}
            Ok(_) => {
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "fork_source_gone",
                    "fork source disappeared before the reference was installed",
                ));
            }
            Err(e) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &e.to_string(),
                ));
            }
        }
        materialize_entry = fc.materialize.clone();
    }

    Ok((desc, materialize_entry))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReferenceStamp {
    Installed,
    AlreadyPresent,
    TargetChanged,
}
fn stamp_fork_reference(current: &StreamDesc, fork_id: &str) -> Mutation<ReferenceStamp> {
    if current.deleted {
        return Mutation::Decline(ReferenceStamp::TargetChanged);
    }
    match current.forked_from.as_ref() {
        Some(reference) if reference.fork_id.is_empty() => {
            let mut next = current.to_persisted();
            next.forked_from
                .as_mut()
                .expect("checked fork reference")
                .fork_id = fork_id.into();
            Mutation::Write(next, ReferenceStamp::Installed)
        }
        Some(reference) if reference.fork_id == fork_id => {
            Mutation::Decline(ReferenceStamp::AlreadyPresent)
        }
        _ => Mutation::Decline(ReferenceStamp::TargetChanged),
    }
}

async fn stamp_child(
    state: &Arc<CreationService>,
    plan: &CreatePlan,
    mut desc: StreamDesc,
    fork_id: &str,
) -> Result<StreamDesc, CreationError> {
    let project = &plan.project;
    let name = &plan.name;
    if desc
        .forked_from
        .as_ref()
        .is_some_and(|f| f.fork_id.is_empty())
    {
        let fid = fork_id.to_owned();
        let stamped = state
            .registry
            .mutate_incarnation(&project.stream_ref(name), &desc.stream_epoch, |current| {
                stamp_fork_reference(current, &fid)
            })
            .await
            .map_err(|error| {
                CreationError::new(CreationFailure::Storage, "internal", &error.to_string())
            })?;
        state.registry.invalidate(&project.stream_ref(name));
        // A declined CAS here means the child was deleted (or
        // re-forked) underneath us. Installing a source
        // reference for it anyway would pin the source's data
        // for a child that no longer exists.
        if !matches!(
            stamped,
            MutationResult::Applied(ReferenceStamp::Installed)
                | MutationResult::Declined(ReferenceStamp::AlreadyPresent)
        ) {
            return Err(CreationError::new(
                CreationFailure::Conflict,
                "fork_target_changed",
                "the fork target changed while it was being created; retry",
            ));
        }
        // Reflect the stamp into the LOCAL snapshot: the
        // readiness give-back names the reference through
        // `desc.forked_from.fork_id`, and a stale empty id
        // silently skipped the release — the source stayed
        // pinned by a child deleted mid-creation (FRK-013).
        let mut stamped_desc = desc.to_persisted();
        if let Some(f) = stamped_desc.forked_from.as_mut() {
            f.fork_id = fork_id.to_owned();
        }
        desc = match StreamDesc::try_from(stamped_desc) {
            Ok(desc) => desc,
            Err(error) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "invalid_descriptor",
                    &error.to_string(),
                ));
            }
        };
    }
    Ok(desc)
}
