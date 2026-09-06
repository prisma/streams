//! Compare or claim a creation request against one persisted incarnation.
use super::raw::CreatePlan;
use super::*;

pub(super) async fn resolve(
    state: &Arc<CreationService>,
    plan: &CreatePlan,
) -> Result<(bool, StreamDesc), CreationError> {
    let project = &plan.project;
    let name = &plan.name;
    let key = &plan.key;
    let content_type = plan.content_type.clone();
    let ttl_secs = plan.ttl_secs;
    let expires_at_ms = plan.expires_at_ms;
    let expected_fork_ref = plan.expected_fork_ref.clone();
    let needs_init = plan.needs_init;
    let create_hash = &plan.create_hash;
    // Resolve existing.
    let existing = match state.registry.get(&project.stream_ref(name)).await {
        Ok(v) => v,
        Err(e) => {
            return Err(CreationError::new(
                CreationFailure::Storage,
                "internal",
                &e.to_string(),
            ));
        }
    };
    let resume_init = resume_initialization(plan, existing.as_ref())?;
    let result = match existing {
        // Resume: the SAME creation request found its own in-flight
        // (or abandoned) initialization — redo it idempotently.
        Some(d) if resume_init => (true, d),
        Some(d) if desc_alive(&d) => match validate_live(plan, d) {
            Ok(v) => v,
            Err(r) => return Err(r),
        },
        Some(d)
            if d.soft_deleted
                || (!d.fork_children.is_empty()
                    && !d.deleted
                    && d.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false)) =>
        {
            // The name still backs live forks: blocked, not recreated
            // (pinned fork lifecycle).
            return Err(CreationError::new(
                CreationFailure::Conflict,
                "gone",
                "name is soft-deleted; live forks retain its data",
            ));
        }
        Some(_) => {
            // Dead incarnation: recreate with a fresh epoch (fresh keyspace).
            // Predicated CAS — one winner; a loser validates against the
            // winner's live descriptor exactly like an idempotent PUT.
            let mut fresh = fresh_desc(
                state,
                project,
                name,
                key,
                content_type.clone(),
                ttl_secs,
                expires_at_ms,
            );
            fresh.project_id = project.clone();
            fresh.forked_from = expected_fork_ref.clone();
            let fp = fresh.key_fingerprint.clone();
            fresh.init = needs_init.then(|| crate::registry::InitState {
                request_hash: create_hash.clone(),
                key_fingerprint: fp,
                claimed_ms: now_ms(),
            });
            match state
                .registry
                .recreate(&project.stream_ref(name), fresh, |d| {
                    !desc_alive(d) && !d.soft_deleted
                })
                .await
            {
                Ok((true, d)) => (true, d),
                Ok((false, winner)) => match validate_live(plan, winner) {
                    Ok(v) => v,
                    Err(r) => return Err(r),
                },
                Err(e) => {
                    return Err(CreationError::new(
                        CreationFailure::Storage,
                        "internal",
                        &e.to_string(),
                    ));
                }
            }
        }
        None => {
            let mut fresh = fresh_desc(
                state,
                project,
                name,
                key,
                content_type.clone(),
                ttl_secs,
                expires_at_ms,
            );
            fresh.project_id = project.clone();
            fresh.forked_from = expected_fork_ref.clone();
            let fp = fresh.key_fingerprint.clone();
            fresh.init = needs_init.then(|| crate::registry::InitState {
                request_hash: create_hash.clone(),
                key_fingerprint: fp,
                claimed_ms: now_ms(),
            });
            match state.registry.create(fresh).await {
                Ok((true, d)) => (true, d),
                Ok((false, d)) => {
                    // Raced: treat as idempotent-config path.
                    if crate::registry::media_type(&d.content_type)
                        != crate::registry::media_type(&content_type)
                        || d.ttl_secs != ttl_secs
                    {
                        return Err(CreationError::new(
                            CreationFailure::Conflict,
                            "config_mismatch",
                            "conflict",
                        ));
                    }
                    // The winner may still be INITIALIZING (audit P0):
                    // this replay must JOIN that initialization, not
                    // answer success for content that is not durable
                    // yet. Same request -> resume; different request ->
                    // conflict.
                    // Joining or taking over someone else's
                    // initialization writes THIS request's content under
                    // THIS request's key, so it has to be the right one.
                    if d.init.is_some() && key.fingerprint(&d.epoch()) != d.key_fingerprint {
                        return Err(CreationError::new(
                            CreationFailure::WrongKey,
                            "wrong_key",
                            "key mismatch",
                        ));
                    }
                    match d.init.as_ref() {
                        Some(i) if i.request_hash == *create_hash => (true, d),
                        Some(_) if !init_claim_stale(&d) => {
                            return Err(CreationError::new(
                                CreationFailure::Conflict,
                                "creating",
                                "stream is being created by a different request",
                            ));
                        }
                        Some(_) => (true, d), // stale claim: take it over
                        None => (false, d),
                    }
                }
                Err(e) => {
                    return Err(CreationError::new(
                        CreationFailure::Storage,
                        "internal",
                        &e.to_string(),
                    ));
                }
            }
        }
    };

    Ok(result)
}

fn validate_live(plan: &CreatePlan, d: StreamDesc) -> Result<(bool, StreamDesc), CreationError> {
    let key = &plan.key;
    let content_type = &plan.content_type;
    let ct_hdr_present = plan.ct_hdr_present;
    let ttl_secs = plan.ttl_secs;
    let expected_fork_ref = &plan.expected_fork_ref;

    let same_ct = crate::registry::media_type(&d.content_type)
        == crate::registry::media_type(content_type)
        || !ct_hdr_present;
    // ROUTING-V3: ordering/segmentation are no longer part of
    // user-visible config, so the idempotent-PUT compare ignores
    // the legacy fields — a headerless re-PUT of a pre-v3
    // per-key stream is config-identical, not a conflict.
    let same_fork = match (&d.forked_from, &expected_fork_ref) {
        (None, None) => true,
        (Some(a), Some(b)) => a.same_identity(b),
        _ => false,
    };
    if !same_ct || d.ttl_secs != ttl_secs || !same_fork {
        return Err(CreationError::new(
            CreationFailure::Conflict,
            "config_mismatch",
            "stream exists with different config",
        ));
    }
    if key.fingerprint(&d.epoch()) != d.key_fingerprint {
        return Err(CreationError::new(
            CreationFailure::WrongKey,
            "wrong_key",
            "key mismatch",
        ));
    }
    Ok((false, d))
}

fn resume_initialization(
    plan: &CreatePlan,
    existing: Option<&StreamDesc>,
) -> Result<bool, CreationError> {
    let key = &plan.key;
    let create_hash = &plan.create_hash;
    let expected_fork_ref = &plan.expected_fork_ref;
    // An INITIALIZING descriptor (audit P0): its content is not durable
    // yet. The same request resumes the work; a different request is a
    // conflict, not an idempotent hit; a stale claim is taken over.
    let mut resume_init = false;
    if let Some(d) = existing
        && let crate::registry::Lifecycle::Initializing(init) = d.lifecycle()
    {
        if !desc_alive(d) {
            // dead-and-initializing: fall through to the recreate arm
        } else if init.request_hash != *create_hash {
            if !init_claim_stale(d) {
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "creating",
                    "stream is being created by a different request",
                ));
            }
            return Err(CreationError::new(
                CreationFailure::Conflict,
                "config_mismatch",
                "stream exists with different config",
            ));
        } else {
            // The resume path skips validate_live, so the key has to
            // be checked HERE. Without it, a caller replaying the
            // same creation body under a DIFFERENT key resumed the
            // initialization and wrote the initial content with that
            // key, while the descriptor kept the original
            // fingerprint — a stream whose configured key cannot
            // decrypt its own first record.
            if key.fingerprint(&d.epoch()) != d.key_fingerprint {
                return Err(CreationError::new(
                    CreationFailure::WrongKey,
                    "wrong_key",
                    "key mismatch",
                ));
            }
            // Belt and braces: the initialization identity itself
            // records which key it was claimed for.
            if !init.key_fingerprint.is_empty() && init.key_fingerprint != d.key_fingerprint {
                return Err(CreationError::new(
                    CreationFailure::WrongKey,
                    "wrong_key",
                    "initialization was claimed under a different key",
                ));
            }
            // …and the recorded parentage must still be the one this
            // request is asking for. The resume path skips
            // validate_live, which is where forks are normally
            // compared.
            match (&d.forked_from, &expected_fork_ref) {
                (None, None) => {}
                (Some(a), Some(b)) if a.same_identity(b) => {}
                _ => {
                    return Err(CreationError::new(
                        CreationFailure::Conflict,
                        "fork_source_changed",
                        "this initialization was claimed against a different fork source",
                    ));
                }
            }
            resume_init = true;
        }
    }

    Ok(resume_init)
}
