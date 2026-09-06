//! Commit initial content before publishing readiness, retaining compensating reference debt.
use super::deletion::release_fork_ref;
use super::raw::CreatePlan;
use super::*;

pub(super) async fn seed(
    state: &Arc<CreationService>,
    plan: &CreatePlan,
    engine: &Arc<ShardEngine>,
    desc: &StreamDesc,
    created: bool,
    materialize_entry: Option<Bytes>,
) -> Result<(u64, bool), CreationError> {
    #[cfg(test)]
    let name = plan.sref.name().as_str();
    let key = &plan.key;
    let close = plan.close;
    let body = &plan.body;
    let fork_ctx = &plan.fork_ctx;
    let create_hash = &plan.create_hash;
    let hash = desc.resolve_segment("").identity;
    let epoch_bytes = desc.epoch();
    // Initial body / close-on-create ride the committer.
    let mut next = {
        match engine.stream_handle(hash).await {
            Ok(h) => h.state.lock().unwrap().durable.next,
            Err(e) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &e.to_string(),
                ));
            }
        }
    };
    let mut closed_now = false;
    // Resume-safety (audit P0): a resumed initialization must not append
    // the initial content twice. Own records begin at `base` (0, or the
    // fork boundary); if the tail already advanced past it, the first
    // attempt's append is durable and this attempt only republishes
    // Ready.
    let own_base = fork_ctx.as_ref().map(|fc| fc.boundary).unwrap_or(0);
    let initial_content_pending = next <= own_base;
    if created
        && initial_content_pending
        && (!body.is_empty() || close || materialize_entry.is_some())
    {
        let mut entries: Vec<Bytes> = if body.is_empty() {
            Vec::new()
        } else if desc.is_json() {
            match json_entries(body, true) {
                Ok(v) => v,
                Err(m) => {
                    return Err(CreationError::new(
                        CreationFailure::Invalid,
                        "invalid_json",
                        &m,
                    ));
                }
            }
        } else {
            vec![body.clone()]
        };
        if let Some(over) = over_record_ceiling(state.admission.record_ceiling(), &entries) {
            return Err(CreationError::new(
                CreationFailure::TooLarge,
                "record_too_large",
                &format!(
                    "record of {over} bytes exceeds the per-record ceiling \
             (MAX_RECORD_PAYLOAD_BYTES)"
                ),
            ));
        }
        // The materialized sub-offset partial is the fork's FIRST own
        // record; an initial body follows it in the same command.
        if let Some(m) = materialize_entry {
            entries.insert(0, m);
        }
        let subkey = derive_subkey(key, &epoch_bytes, "", 0);
        let bytes = entries.iter().map(|e| e.len()).sum();
        #[cfg(test)]
        crate::failpoints::pause_init_before_seed(name).await;
        let (tx, rx) = oneshot::channel();
        let req = AppendReq {
            enqueued_at: std::time::Instant::now(),
            hash,
            route: crate::crypto::RouteHash::for_stream(&desc.sref()).0,
            entries,
            routing_key: String::new(),
            key_hash: crate::crypto::stream_hash(""),
            producer_lineage: Vec::new(),
            key_version: 0,
            subkey,
            ts_hint_ms: None,
            seq: None,
            bytes,
            finish: if close {
                crate::shard::AppendFinish::Close
            } else {
                crate::shard::AppendFinish::Open
            },
            // Exactly-once initial content (audit P0): the append
            // carries a synthetic producer identity derived from the
            // creation-request hash, so concurrent joiners and resumed
            // attempts are deduplicated by the SAME committer machinery
            // that guarantees producer idempotence — rather than by a
            // read-then-check race.
            producer: Some(crate::shard::ProducerReq {
                id: format!("\u{0}init\u{0}{create_hash}"),
                epoch: 1,
                seq: 0,
                request_hash: None,
            }),
            deferred_error: None,
            sealed_reject_new: None,
            touch: None,
            usage: crate::usage::counters(&crate::crypto::RouteHash::for_stream(&desc.sref()).0),
            seal_gen: None,
            billing: (!crate::billing::is_reserved_stream(&desc.name)).then(|| {
                std::sync::Arc::new(crate::billing::BillingRef {
                    identity: crate::billing::identity_with_capabilities(
                        &state.auth,
                        &state.deployment,
                        desc,
                        true,
                    ),
                    segment_id: 0,
                })
            }),
            resp: tx,
        };
        if engine.try_enqueue(req).is_err() {
            return Err(CreationError::new(
                CreationFailure::Overloaded,
                "overloaded",
                "queue full",
            ));
        }
        match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
            Ok(Ok(Ok(ack))) => {
                next = ack.next_offset;
                closed_now = ack.closed || close;
            }
            _ => {
                return Err(CreationError::new(
                    CreationFailure::Ambiguous,
                    "append_timeout",
                    "initial body timed out",
                ));
            }
        }
    } else if !created && close {
        closed_now = true; // preserved on idempotent PUT of a closed stream
    } else if created && !initial_content_pending {
        // Resumed after the content already committed.
        closed_now = close;
    }

    Ok((next, closed_now))
}

pub(super) async fn publish(
    state: &Arc<CreationService>,
    plan: &CreatePlan,
    desc: &StreamDesc,
    created: bool,
) -> Result<(), CreationError> {
    let name = plan.sref.name().as_str();
    let needs_init = plan.needs_init;
    let create_hash = &plan.create_hash;
    // Publish Ready: every durable initialization step (fork tail seed,
    // source reference, initial content, close-on-create) has landed.
    // Until this CAS, a replay resumes instead of observing a stream
    // whose content never arrived.
    if created && needs_init {
        #[cfg(test)]
        crate::failpoints::pause_create_before_ready(name).await;
        let published = match state
            .registry
            .mutate_incarnation(&plan.sref, &desc.stream_epoch, |current| {
                if current.deleted
                    || !current
                        .init
                        .as_ref()
                        .is_some_and(|i| i.request_hash == *create_hash)
                {
                    return Mutation::Decline(false);
                }
                let mut next = current.to_persisted();
                next.init = None;
                Mutation::Write(next, true)
            })
            .await
        {
            Ok(v) => matches!(
                v,
                MutationResult::Applied(true) | MutationResult::Declined(true)
            ),
            Err(e) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &format!("publishing stream readiness: {e}"),
                ));
            }
        };
        state.registry.invalidate(&plan.sref);
        // A declined CAS is NOT readiness. `cas_update` refuses a
        // deleted descriptor, so a delete that won mid-initialization
        // made this return 201 for a stream that no longer exists — and
        // if the work had already installed a fork reference, the source
        // stayed pinned by a child that was never published.
        if !published {
            let now = state.registry.get(&plan.sref).await.map_err(|error| {
                CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &format!("verifying stream readiness: {error}"),
                )
            })?;
            let live_and_ready = now.as_ref().is_some_and(|d| {
                desc_alive(d) && d.init.is_none() && d.stream_epoch == desc.stream_epoch
            });
            if !live_and_ready {
                // Compensate: give back the source reference this
                // initialization installed, so the parent is not held by
                // a child that will never exist.
                if let Some(fr) = desc.forked_from.as_ref().filter(|f| !f.fork_id.is_empty())
                    && let Err(m) = release_fork_ref(
                        state,
                        desc.ref_in_project(&fr.source),
                        &fr.fork_id,
                        &fr.source_epoch,
                    )
                    .await
                {
                    tracing::error!(stream = %name, "releasing an abandoned fork claim: {m}");
                }
                return Err(CreationError::gone(now.as_ref()));
            }
        }
    }

    Ok(())
}
