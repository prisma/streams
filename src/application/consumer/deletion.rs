//! Generation-fenced deletion and bounded resumable segment cleanup.
use super::{
    AuthorizedDeletionContext, CONSUMER_DELETE_REQUEST_STEPS, CONSUMER_DELETE_SEGMENT_CONCURRENCY,
    CONSUMER_DELETE_STEP_BYTES, CONSUMER_DELETE_STEP_ROWS, ConsumerAccess, ConsumerFailure,
    ConsumerService, DeleteOutcome, DeletionDebt, FailureClass, KeyCheck, check_key,
    consumer_config_op, consumer_segments, desc_alive, failure, initializing,
};
use crate::application::consumer_remote::relay_sweep_segment;
use crate::application::read_remote::InternalTarget;
use std::sync::Arc;

pub(crate) async fn delete(
    state: Arc<ConsumerService>,
    sref: crate::tenant::TenantStreamRef,
    cname: String,
    key_b64: String,
    version: ([u8; 16], u64),
    access: ConsumerAccess<'_>,
) -> Result<DeleteOutcome, ConsumerFailure> {
    access.require(&sref, crate::tenant::Scope::ConsumersConfigure)?;
    let tenant = sref.project_id();
    let name = sref.name().as_str().to_string();
    let (expect_epoch, expect_gen) = version;
    let desc = match state.registry.get(&tenant.stream_ref(&name)).await {
        Ok(Some(d)) if desc_alive(&d) => {
            if initializing(&d) {
                return Err(failure(
                    FailureClass::Unavailable,
                    "creating",
                    "stream is still being created; retry",
                    None,
                    true,
                ));
            }
            d
        }
        Ok(_) => {
            // The collection is gone; so is the token's target.
            return Ok(DeleteOutcome::TargetGone);
        }
        Err(e) => {
            return Err(failure(
                FailureClass::Unavailable,
                "unavailable",
                &format!("registry unavailable: {e}"),
                None,
                true,
            ));
        }
    };
    let Some(epoch) = desc.epoch_bytes() else {
        return Err(failure(
            FailureClass::Internal,
            "internal",
            "bad descriptor",
            None,
            true,
        ));
    };
    if expect_epoch != epoch {
        // The stream incarnation the version was minted under no longer
        // exists — the old target died with it. Idempotent success, the
        // CURRENT stream untouched, and deliberately BEFORE the key
        // check (the old client may hold a rotated-away key).
        return Ok(DeleteOutcome::TargetGone);
    }
    // Same incarnation: from here on we may touch live state, so the
    // key must validate.
    if !matches!(check_key(Some(&key_b64), &desc), KeyCheck::Ok(..)) {
        return Err(failure(
            FailureClass::Denied,
            "wrong_key",
            "encryption key mismatch",
            None,
            false,
        ));
    }
    // Collection-wide deletion as a GENERATION-FENCED SAGA (rounds
    // 16-17). Invariant: 204 means the TARGETED INCARNATION's deletion
    // is collection-wide — every segment's dead-generation rows are
    // gone and no write of that generation can land afterwards. Any
    // failure propagates; the retry (same endpoint, same version)
    // resumes from the Deleting state and the durably reduced row set.
    //
    //   0. The request names an INCARNATION, not a name: the required
    //      Prisma-Consumer-Version pins {stream epoch, consumer
    //      generation}. A stale retry whose target no longer exists
    //      gets an idempotent 204 and touches NOTHING (round-17 ABA).
    //   1. Parent record: Active -> Deleting, fenced to the exact
    //      generation. New pull/settle refuse from this instant.
    //   2. Every segment (current AND predecessor — the pull lineage):
    //      install the generation fence, then delete the dead
    //      generations' rows in bounded steps, segments swept
    //      concurrently. Any engine/submit failure -> 503, no 204.
    //   3. Re-read the segment map — REFUSING a changed stream epoch —
    //      and repeat until stable across a fan-out round (a split
    //      racing the saga gets its new children swept too).
    //   4. Parent record: Deleting -> Deleted (a TOMBSTONE, kept so
    //      recreation allocates generation+1 and dead-generation
    //      residue stays inert forever).
    let rec = match consumer_config_op(
        &state,
        &desc,
        crate::queue::QueueOp::ConfigGet {
            consumer: cname.clone(),
        },
    )
    .await
    {
        Ok(crate::queue::QueueOut::Config { rec, .. }) => rec,
        Ok(_) => unreachable!("ConfigGet answers Config"),
        Err(r) => return Err(r),
    };
    let rec = match rec {
        None => {
            // The version claims a generation this server never made
            // (or whose tombstone is gone — impossible pre-GC). With
            // no record at all there is nothing to protect and nothing
            // to do.
            return Ok(DeleteOutcome::TargetGone);
        }
        Some(r) => r,
    };
    if rec.generation > expect_gen {
        // The named generation is already dead and buried (the record
        // has moved on — tombstone or a recreated consumer). The old
        // target is gone; the CURRENT generation is another
        // incarnation's property. Idempotent success, no mutation.
        return Ok(DeleteOutcome::TargetGone);
    }
    if rec.generation < expect_gen {
        // A version newer than the record is impossible from an honest
        // client: refuse without mutating anything.
        return Err(failure(
            FailureClass::Conflict,
            "consumer_version_conflict",
            "the presented consumer version is newer than the server's record",
            None,
            false,
        ));
    }
    if rec.state == crate::queue::ConsumerLifecycle::Deleted {
        // Exactly the targeted generation, already fully deleted.
        return Ok(DeleteOutcome::TargetGone);
    }
    let target = DeletionDebt {
        stream: sref.clone(),
        epoch,
        consumer: cname,
        generation: rec.generation,
    };
    resume_deletion(AuthorizedDeletionContext {
        service: state,
        descriptor: desc,
        target: target.clone(),
        lifecycle: rec.state,
    })
    .await
    .map_err(|mut error| {
        error.deletion_debt = Some(target);
        error
    })
}

async fn resume_deletion(
    context: AuthorizedDeletionContext,
) -> Result<DeleteOutcome, ConsumerFailure> {
    let AuthorizedDeletionContext {
        service: state,
        descriptor: desc,
        target,
        lifecycle: desc_deleting_state,
    } = context;
    let tenant = target.stream.project_id();
    let name = target.stream.name().as_str().to_string();
    let cname = target.consumer;
    let epoch = target.epoch;
    let cgen = target.generation;

    if desc_deleting_state == crate::queue::ConsumerLifecycle::Active
        && let Err(r) = consumer_config_op(
            &state,
            &desc,
            crate::queue::QueueOp::ConfigLifecycle {
                consumer: cname.clone(),
                expect_gen: cgen,
                deleting: true,
            },
        )
        .await
    {
        return Err(r);
    }
    // Fan out until the segment set is stable across a full round.
    // Segments are swept CONCURRENTLY (bounded) and each segment is
    // stepped to completion within this request's step budget.
    let steps_left = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
        CONSUMER_DELETE_REQUEST_STEPS as i64,
    ));
    let mut cur_desc = desc.clone();
    for _round in 0..5 {
        let segs = consumer_segments(&cur_desc);
        // The incarnation this round's sweep is bound to. A relayed
        // step carries it so a peer can refuse the request outright if
        // the name has since been recreated (round-19 ABA).
        let round_epoch = cur_desc.epoch_bytes();
        let sweeps = segs.iter().copied().map(|(seg_id, identity, route, _)| {
            let state = state.clone();
            let cname = cname.clone();
            let name = name.clone();
            let project = cur_desc.project_id.clone();
            let steps_left = steps_left.clone();
            async move {
                let engine = match state.engine_for(&route).await {
                    Ok(e) => e,
                    Err(r) => {
                        // Cross-owner sweep fan-out: run this segment's
                        // DeleteStep loop on its owner. The borrow of r
                        // ends before the await (axum Body is !Sync).
                        let peer = r
                            .owner
                            .as_deref()
                            .and_then(|owner| state.peer.url_for(owner));
                        if let Some(base) = peer {
                            let Some(stream_epoch) = round_epoch else {
                                return Err((
                                    "segment_unavailable",
                                    format!(
                                        "segment {seg_id}: no incarnation to bind the \
                                         relayed sweep to; retry"
                                    ),
                                ));
                            };
                            let t = InternalTarget {
                                project_id: project.clone(),
                                stream_epoch,
                                seg_id,
                                identity,
                            };
                            return relay_sweep_segment(
                                &state,
                                &base,
                                &name,
                                &t,
                                &cname,
                                cgen + 1,
                                &steps_left,
                            )
                            .await;
                        }
                        return Err((
                            "segment_unavailable",
                            format!(
                                "segment {seg_id}'s owner is unavailable; the deletion \
                                 is incomplete — retry"
                            ),
                        ));
                    }
                };
                loop {
                    if steps_left.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) <= 0 {
                        return Err((
                            "segment_cleanup_incomplete",
                            format!(
                                "segment {seg_id} still has rows after this request's \
                                 cleanup budget; progress is durable — retry to resume"
                            ),
                        ));
                    }
                    match engine
                        .submit_queue(
                            identity,
                            crate::queue::QueueOp::ConfigDeleteStep {
                                consumer: cname.clone(),
                                fence_below: cgen + 1,
                                max_rows: CONSUMER_DELETE_STEP_ROWS,
                                max_bytes: CONSUMER_DELETE_STEP_BYTES,
                            },
                        )
                        .await
                    {
                        Ok(crate::queue::QueueOut::DeleteStep { complete: true, .. }) => {
                            return Ok(());
                        }
                        Ok(crate::queue::QueueOut::DeleteStep {
                            complete: false, ..
                        }) => continue,
                        Ok(_) => {
                            return Err((
                                "segment_cleanup_failed",
                                format!(
                                    "segment {seg_id} cleanup answered an unexpected \
                                     outcome; the deletion is incomplete — retry"
                                ),
                            ));
                        }
                        Err(m) => {
                            return Err((
                                "segment_cleanup_failed",
                                format!(
                                    "segment {seg_id} cleanup failed ({m}); the \
                                     deletion is incomplete — retry"
                                ),
                            ));
                        }
                    }
                }
            }
        });
        use futures_util::StreamExt as _;
        let results: Vec<Result<(), (&'static str, String)>> = futures_util::stream::iter(sweeps)
            .buffer_unordered(CONSUMER_DELETE_SEGMENT_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for r in results {
            if let Err((code, msg)) = r {
                return Err(failure(FailureClass::Unavailable, code, &msg, None, true));
            }
        }
        let mut swept_ids: Vec<u32> = segs.iter().map(|(id, ..)| *id).collect();
        swept_ids.sort_unstable();
        #[cfg(test)]
        crate::failpoints::pause_consumer_saga_before_refresh(&name).await;
        // FAIL-CLOSED refresh (round 18). Completion is proven by a
        // SUCCESSFUL post-sweep read of the authoritative map: the
        // segments swept this round must equal the segments visible
        // AFTER the sweep, with no topology transition pending. The
        // previous shape treated a refresh error — or a vanished
        // descriptor — as "keep the cached map", which could let a
        // stale pre-split map look stable for two rounds and publish
        // a false collection-wide 204.
        state.registry.invalidate(&tenant.stream_ref(&name));
        let fresh = match state.registry.get(&tenant.stream_ref(&name)).await {
            Ok(Some(d)) if desc_alive(&d) => d,
            Ok(_) => {
                // The collection is gone mid-saga; so is the target.
                // Nothing to finalize, nothing to touch.
                return Ok(DeleteOutcome::TargetGone);
            }
            Err(e) => {
                return Err(failure(
                    FailureClass::Unavailable,
                    "segment_map_unverified",
                    &format!(
                        "cannot verify the segment map after cleanup ({e}); \
                         the deletion is incomplete — retry"
                    ),
                    None,
                    true,
                ));
            }
        };
        // EPOCH PIN (round-17 P0): the refresh is by NAME, and the
        // name may now belong to a recreated stream. This saga's
        // authority extends only to the incarnation it targeted — a
        // changed epoch means the old stream (and with it the old
        // consumer) is gone. Idempotent success, replacement
        // untouched.
        if fresh.epoch_bytes() != Some(epoch) {
            return Ok(DeleteOutcome::TargetGone);
        }
        let pending = fresh.segments.as_ref().is_some_and(|m| m.pending.is_some());
        let mut fresh_ids: Vec<u32> = consumer_segments(&fresh)
            .iter()
            .map(|(id, ..)| *id)
            .collect();
        fresh_ids.sort_unstable();
        if !pending && fresh_ids == swept_ids {
            // Everything that can hold this consumer's rows was swept
            // AFTER its fence went up, and the authoritative map —
            // read successfully, transition-free — confirms no segment
            // escaped the sweep. (A split landing after this read
            // cannot mint state for a Deleting consumer: pulls consult
            // the parent record first.)
            if let Err(r) = consumer_config_op(
                &state,
                &desc,
                crate::queue::QueueOp::ConfigLifecycle {
                    consumer: cname.clone(),
                    expect_gen: cgen,
                    deleting: false,
                },
            )
            .await
            {
                return Err(r);
            }
            return Ok(DeleteOutcome::Cleaned);
        }
        cur_desc = fresh;
    }
    Err(failure(
        FailureClass::Unavailable,
        "segment_map_unstable",
        "the collection kept splitting during deletion; retry",
        None,
        true,
    ))
}
