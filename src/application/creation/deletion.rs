//! Reference-debt settlement and incarnation-fenced deletion transitions.
use super::*;

impl CreationService {
    pub(crate) async fn delete(
        self: &Arc<Self>,
        sref: crate::tenant::TenantStreamRef,
    ) -> Result<(), CreationError> {
        let existing = self.registry.get(&sref).await.map_err(|e| {
            CreationError::new(CreationFailure::Storage, "internal", &e.to_string())
        })?;
        let Some(desc) = existing else {
            return Err(CreationError::gone(None));
        };
        if !desc_alive(&desc) {
            if desc.deleted {
                delete_lifecycle(self, &sref)
                    .await
                    .map_err(|e| CreationError::new(CreationFailure::Storage, "internal", &e))?;
            }
            return Err(CreationError::gone(Some(&desc)));
        }
        delete_lifecycle(self, &sref)
            .await
            .map_err(|e| CreationError::new(CreationFailure::Storage, "internal", &e))
    }
    #[cfg(test)]
    pub(crate) async fn release_fork_ref(
        self: &Arc<Self>,
        source: crate::tenant::TenantStreamRef,
        fork_id: &str,
        epoch: &str,
    ) -> Result<bool, String> {
        release_fork_ref(self, source, fork_id, epoch).await
    }
}
pub(super) async fn clear_parent_debt(
    state: &Arc<CreationService>,
    sref: &crate::tenant::TenantStreamRef,
    expect_epoch: &str,
    expect_ref: Option<(&str, &str)>,
) -> Result<(), String> {
    let _ = state
        .registry
        .mutate_incarnation(sref, expect_epoch, |x| {
            let debt_matches = match expect_ref {
                Some((src, fid)) => x
                    .forked_from
                    .as_ref()
                    .is_some_and(|f| f.source == src && f.fork_id == fid),
                None => x.forked_from.is_none(),
            };
            if x.parent_ref_pending && debt_matches {
                let mut next = x.to_persisted();
                next.parent_ref_pending = false;
                crate::registry::Mutation::Write(next, ())
            } else {
                // Different debt, different incarnation's business, or
                // already paid: not ours to clear.
                crate::registry::Mutation::Decline(())
            }
        })
        .await
        .map_err(|e| e.to_string())?;
    state.registry.invalidate(sref);
    Ok(())
}
pub(super) fn release_fork_ref(
    state: &Arc<CreationService>,
    src_ref: crate::tenant::TenantStreamRef,
    fork_id: &str,
    expect_source_epoch: &str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + Send>> {
    let state = state.clone();
    let fork_id = fork_id.to_string();
    let expect_source_epoch = expect_source_epoch.to_string();
    Box::pin(async move {
        // Incarnation fence (round 14): the reference belongs to the
        // source INCARNATION the child forked. A delayed cleanup can
        // run after that name was deleted and recreated; releasing
        // against — or worse, evaluating expiry/soft-delete lifecycle
        // conditions against — the REPLACEMENT would corrupt a stream
        // this fork never touched. A mismatch means the original
        // source is gone: nothing to release, nothing pinned, so the
        // release is conclusive. An empty expectation opts out (the
        // recursive ancestor settle, which already re-reads each hop).
        // ONE descriptor snapshot for the WHOLE operation. Every
        // decision below — the tombstone-debt settle, the reference
        // CAS, the debt clear — binds to THIS snapshot's incarnation.
        // Re-reading the epoch later would re-bind a stale cleanup to
        // whatever incarnation holds the name at that instant (round
        // 15: check A, name recreated as B, mutation fenced to B —
        // legitimately fenced, wrong identity).
        let cur = state
            .registry
            .get(&src_ref)
            .await
            .map_err(|e| e.to_string())?;
        let Some(cur) = cur else {
            // Source gone entirely: nothing to release, ever.
            return Ok(true);
        };
        if !expect_source_epoch.is_empty() && cur.stream_epoch != expect_source_epoch {
            // The incarnation this release was owed to is gone.
            return Ok(true);
        }
        let source_epoch = cur.stream_epoch.clone();
        #[cfg(test)]
        crate::failpoints::pause_release_after_epoch_check(src_ref.name().as_str()).await;
        // Release BY ID: a retried delete removes an id that is already
        // gone (a no-op), where an anonymous decrement would have
        // double-released and freed a still-live fork's data.
        // If the source is ALREADY a tombstone that still owes its own
        // parent, settle that debt first. Otherwise retrying the
        // original delete looks successful while an ancestor stays
        // pinned forever: the CAS below refuses on a deleted descriptor,
        // this function returns Ok, and the caller clears its own flag.
        // Only the hidden intermediate name could repair it, which no
        // ordinary client knows to ask for.
        if cur.deleted {
            if cur.parent_ref_pending
                && let Some(gp) = cur.forked_from.as_ref()
                && release_fork_ref(
                    &state,
                    cur.ref_in_project(&gp.source),
                    &gp.fork_id,
                    &gp.source_epoch,
                )
                .await?
            {
                clear_parent_debt(
                    &state,
                    &src_ref,
                    &source_epoch,
                    Some((&gp.source, &gp.fork_id)),
                )
                .await?;
            }
            // A hard-deleted source holds no live references.
            return Ok(true);
        }
        // Release the reference AND decide the source's fate in one
        // CAS, against the children it has at that instant. Splitting
        // the two let a new fork install itself in between and then be
        // orphaned by an unconditional tombstone.
        //
        // Expressed through the TYPED mutation API: `decide` is pure
        // over an immutable descriptor and RETURNS its verdict, so the
        // stale-flag-across-retries hazard (round 14) is structurally
        // impossible — there are no out-parameters to leak from a lost
        // attempt. The verdict is `(removed_ref, tombstoned)`.
        let outcome = state
            .registry
            .mutate_incarnation(&src_ref, &source_epoch, |x| {
                let before = x.fork_children.len();
                let mut next = x.to_persisted();
                next.fork_children.retain(|c| c != &fork_id);
                let removed = next.fork_children.len() != before;
                let expired = next.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false);
                let should_tombstone = next.fork_children.is_empty()
                    && (next.soft_deleted || expired)
                    && !next.deleted;
                if should_tombstone {
                    next.soft_deleted = false;
                    next.deleted = true;
                    // Round-22 item 7: the closure debt rides the SAME
                    // registry write — the billing clock stops here.
                    next.logical_close_ms = Some(crate::billing::billing_now_ms());
                    next.parent_ref_pending = next.forked_from.is_some();
                }
                if removed || should_tombstone {
                    crate::registry::Mutation::Write(next, (removed, should_tombstone))
                } else {
                    crate::registry::Mutation::Decline((false, false))
                }
            })
            .await
            .map_err(|e| e.to_string())?;
        let (removed_ref, tombstoned) = match outcome {
            crate::registry::MutationResult::Applied(v)
            | crate::registry::MutationResult::Declined(v) => v,
            // Source gone or recreated between our snapshot and the
            // mutation: the incarnation this release was owed to no
            // longer exists — CONCLUSIVE, same verdict as the epoch
            // check at the top, so recreated-source debt converges.
            crate::registry::MutationResult::Missing
            | crate::registry::MutationResult::IncarnationChanged => {
                state.registry.invalidate(&src_ref);
                return Ok(true);
            }
        };
        // SR3-2 (round-3 finding 2.3): the fork CASCADE is the other
        // terminal hard delete — a soft-retained source whose last
        // fork reference drops tombstones HERE, not in
        // delete_lifecycle, so the max_streams slot releases here too.
        if tombstoned {
            state.quotas.release_stream(src_ref.project_id());
        }

        state.registry.invalidate(&src_ref);
        #[cfg(test)]
        if tombstoned && crate::failpoints::should_stop_after_tombstone(src_ref.name().as_str()) {
            // "Crash" here: the tombstone and its debt are durable, the
            // recursive release has not run.
            return Ok(removed_ref);
        }
        if tombstoned {
            // The tombstone we JUST wrote — same incarnation (a
            // tombstone keeps its epoch), so the debt clear stays
            // fenced to it. The grandparent release carries the
            // ForkRef's own recorded source epoch.
            if let Some(after) = state
                .registry
                .get(&src_ref)
                .await
                .map_err(|e| e.to_string())?
                && after.stream_epoch == source_epoch
                && let Some(gf) = after.forked_from.as_ref()
                && release_fork_ref(
                    &state,
                    after.ref_in_project(&gf.source),
                    &gf.fork_id,
                    &gf.source_epoch,
                )
                .await?
            {
                clear_parent_debt(
                    &state,
                    &src_ref,
                    &source_epoch,
                    Some((&gf.source, &gf.fork_id)),
                )
                .await?;
            }
        }
        Ok(removed_ref)
    })
}

/// The pinned fork delete lifecycle: a stream with live forks
/// SOFT-deletes (data retained, direct access 410, name blocked); a
/// fork's deletion releases its source reference, and a soft-deleted
/// source whose last reference drops cascades to a hard delete —
/// recursively up the chain.
fn delete_lifecycle(
    state: &Arc<CreationService>,
    sref: &crate::tenant::TenantStreamRef,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
    // Søren review blocker 1: this shared-core lifecycle routine is
    // keyed by the caller's PROJECT-QUALIFIED ref, never a bare name.
    // Reconstructing state.sref(name) here bound every reload and CAS
    // to the DEPLOYMENT tenant, so a project-B delete could tombstone
    // the deployment tenant's same-named stream while B's own survived.
    let state = state.clone();
    let sref = sref.clone();
    let name = sref.name().as_str().to_string();
    Box::pin(async move {
        let d = match state.registry.get(&sref).await.map_err(|e| e.to_string())? {
            Some(d) => d,
            None => return Ok(()),
        };
        let parent = d
            .forked_from
            .as_ref()
            .map(|f| (f.source.clone(), f.fork_id.clone(), f.source_epoch.clone()));
        // Already a tombstone with an unpaid debt: just pay it. This is
        // also how a crashed CASCADE is repaired — an intermediate
        // generation that was tombstoned but never released its own
        // parent is reachable by deleting it again.
        if let crate::registry::Lifecycle::Deleted { parent_ref_pending } = d.lifecycle() {
            if parent_ref_pending && let Some((src, fid, sep)) = parent.clone() {
                // Clear the debt only on a CONCLUSIVE release: an
                // absent reference on a live source may still be
                // installed by a creator in flight, and this very
                // retry is what repairs that crash later. A source
                // recreated since the fork is conclusive too — the
                // incarnation this debt was owed to is gone.
                if release_fork_ref(&state, d.ref_in_project(&src), &fid, &sep).await? {
                    clear_parent_debt(&state, &d.sref(), &d.stream_epoch, Some((&src, &fid)))
                        .await?;
                }
            }
            // Then walk UP. A crashed cascade leaves the debt on a
            // hidden intermediate generation, and the only request a
            // client will ever retry is the original delete of the leaf.
            // Repairing only this descriptor left the ancestor pinned
            // and reported success.
            // Each hop resolves in the project of the descriptor that
            // HOLDS the reference (chain-invariant today, structurally
            // per-hop).
            let mut next = d.forked_from.as_ref().map(|f| d.ref_in_project(&f.source));
            for _ in 0..64 {
                let Some(anc_ref) = next else { break };
                let Some(anc) = state
                    .registry
                    .get(&anc_ref)
                    .await
                    .map_err(|e| e.to_string())?
                else {
                    break;
                };
                if !(anc.deleted && anc.parent_ref_pending) {
                    break;
                }
                let conclusive = match anc.forked_from.as_ref() {
                    Some(gp) => {
                        release_fork_ref(
                            &state,
                            anc.ref_in_project(&gp.source),
                            &gp.fork_id,
                            &gp.source_epoch,
                        )
                        .await?
                    }
                    None => true,
                };
                if conclusive {
                    let debt = anc
                        .forked_from
                        .as_ref()
                        .map(|gp| (gp.source.clone(), gp.fork_id.clone()));
                    clear_parent_debt(
                        &state,
                        &anc_ref,
                        &anc.stream_epoch,
                        debt.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
                    )
                    .await?;
                }
                next = anc
                    .forked_from
                    .as_ref()
                    .map(|g| anc.ref_in_project(&g.source));
            }
            return Ok(());
        }
        // Soft-versus-hard is decided INSIDE the CAS, against the
        // children the descriptor has at that instant. Deciding it from
        // an earlier read raced fork creation: a concurrent first fork
        // could install its reference between the read and the write,
        // and the unconditional update tombstoned the source anyway —
        // leaving a live fork anchored to a hard-deleted parent.
        //
        // The debt is recorded in the SAME write as the tombstone, so a
        // crash between them is impossible.
        #[cfg(test)]
        crate::failpoints::pause_delete_before_decision(&name).await;
        let epoch = d.stream_epoch.clone();
        // Round-22 item 7: ONE logical close instant, decided here,
        // stamped into the tombstone write below and used by every
        // closure submission — however late a retry lands, it accounts
        // to THIS time.
        let close_stamp = crate::billing::billing_now_ms();
        let outcome = state
            .registry
            .mutate_incarnation(&sref, &epoch, |current| {
                delete_transition(current, close_stamp)
            })
            .await
            .map_err(|error| error.to_string())?;
        let hard_deleted = matches!(
            outcome,
            MutationResult::Applied(DeleteTransition::Tombstoned)
        );
        state.registry.invalidate(&sref);
        if !hard_deleted {
            return Ok(());
        }
        // SR2-4/SR3-2: the terminal hard delete frees the project's
        // max_streams slot. No name-syntax check — every layout-4
        // registry entry is a customer stream (segments live inside
        // the descriptor), and a customer may legally name a stream
        // with '#'.
        state.quotas.release_stream(sref.project_id());
        // Ops journal (§12.3): the lifecycle transition, id'd by the
        // incarnation — a retried delete re-emits the same id and the
        // rollup deduplicates.
        state.runtime.ops.emit(
            crate::ops::OpsEvent::new(
                "stream_hard_deleted",
                format!("life/{}/hard_deleted", d.stream_epoch),
            )
            .stream(&d.sref(), &d.stream_epoch),
        );
        // Billing closure (§6.2): the hard delete is the terminal
        // storage observation — advance every segment's storage clock
        // to the persisted close stamp, zero its gauge, mark dirty for
        // the ledger. Submission is AWAITED (round-22 item 7): a full
        // committer queue is backpressure, never a silent drop; a
        // submission that still fails is safe because the debt lives
        // on the tombstone and the sweep reconciler retries it.
        {
            let seg_ids: Vec<u32> = d
                .segments
                .as_ref()
                .map(|m| m.segments.iter().map(|sg| sg.seg_id).collect())
                .unwrap_or_else(|| vec![0]);
            for sid in seg_ids {
                let identity = d.dynamic_segment_identity(sid);
                let route = d
                    .segment_route_by_id(sid)
                    .expect("segment selected from validated topology");
                if let Ok(engine) = state.resolve(&route).await
                    && let Err(e) = engine.submit_billing_close(identity, close_stamp).await
                {
                    tracing::warn!(
                        "delete {name}: billing close submit failed \
                             (tombstone debt persists; sweep retries): {e}"
                    );
                }
            }
        }
        if let Some((src, fid, sep)) = parent {
            // Released CONCLUSIVELY: the tombstone owes nothing more.
            // This is `update`, not `cas_update`, because the
            // descriptor is already deleted and CAS refuses tombstones
            // by design — which is exactly why the debt has to be
            // recorded ON the tombstone and cleared this way. An
            // absent-on-live-source release keeps the debt: the
            // child's creator may still install the reference, and the
            // next DELETE of this tombstone retries and removes it.
            if release_fork_ref(&state, d.ref_in_project(&src), &fid, &sep).await? {
                clear_parent_debt(&state, &d.sref(), &epoch, Some((&src, &fid))).await?;
            }
        }
        Ok(())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteTransition {
    AlreadyDeleted,
    Retained,
    Tombstoned,
}
fn delete_transition(current: &StreamDesc, close_ms: i64) -> Mutation<DeleteTransition> {
    if current.deleted {
        return Mutation::Decline(DeleteTransition::AlreadyDeleted);
    }
    let mut next = current.to_persisted();
    if !current.fork_children.is_empty() {
        next.soft_deleted = true;
        Mutation::Write(next, DeleteTransition::Retained)
    } else {
        next.deleted = true;
        next.logical_close_ms = Some(close_ms);
        next.parent_ref_pending = next.forked_from.is_some();
        Mutation::Write(next, DeleteTransition::Tombstoned)
    }
}
