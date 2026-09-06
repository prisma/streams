//! Creation, fork anchoring and deletion own their persisted initialization/debt transitions.
use crate::crypto::{StreamKey, derive_subkey, hex};
use crate::registry::{Mutation, MutationResult, Registry, StreamDesc};
use crate::shard::{AppendReq, ShardEngine, now_ms};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::oneshot;

const APPEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreationFailure {
    Invalid,
    Conflict,
    Missing,
    Gone,
    WrongKey,
    Storage,
    TooLarge,
    Overloaded,
    Ambiguous,
    Opening,
}
#[derive(Debug)]
pub(crate) struct CreationError {
    pub kind: CreationFailure,
    pub code: &'static str,
    pub message: String,
    pub owner: Option<String>,
    pub retry_after: Option<u64>,
}
impl CreationError {
    fn new(kind: CreationFailure, code: &'static str, message: &str) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            owner: None,
            retry_after: None,
        }
    }
    fn gone(desc: Option<&StreamDesc>) -> Self {
        if desc.is_some_and(|d| {
            d.soft_deleted
                || (!d.deleted
                    && !d.fork_children.is_empty()
                    && d.expires_at_ms.is_some_and(|expires| now_ms() >= expires))
        }) {
            Self::new(
                CreationFailure::Gone,
                "gone",
                "stream deleted; live forks remain",
            )
        } else {
            Self::new(CreationFailure::Missing, "not_found", "stream not found")
        }
    }
}
#[derive(Clone)]
pub(crate) struct ForkCommand {
    pub source: String,
    pub offset: Option<u64>,
    pub sub_offset: Option<u64>,
}
pub(crate) struct CreateCommand {
    pub project: crate::tenant::ProjectId,
    pub name: String,
    pub key: StreamKey,
    pub content_type: Option<String>,
    pub ttl_secs: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub close: bool,
    pub body: Bytes,
    pub fork: Option<ForkCommand>,
}
pub(crate) struct CreateOutcome {
    pub created: bool,
    pub desc: StreamDesc,
    pub next: u64,
    pub closed: bool,
}
#[derive(Clone)]
pub(crate) struct CreationService {
    pub registry: Arc<Registry>,
    pub shards: crate::shard_directory::ShardDirectory,
    pub ownership: crate::ownership::OwnershipService,
    pub reads: Arc<crate::application::read::ReadService>,
    pub keys: Arc<crate::history::KeyCache>,
    pub runtime: crate::runtime::RuntimeCaps,
    pub deployment: crate::deployment::DeploymentIdentity,
    pub auth: Arc<crate::auth::AuthService>,
    pub quotas: crate::quota::QuotaRegistry,
    pub record_ceiling: usize,
    pub sliding: Arc<std::sync::Mutex<std::collections::HashSet<crate::tenant::TenantStreamRef>>>,
}
impl CreationService {
    pub(crate) async fn resolve(
        &self,
        route: &[u8; 16],
    ) -> Result<Arc<ShardEngine>, CreationError> {
        use crate::shard_directory::ResolveError;
        self.shards
            .resolve(route, crate::shard_directory::Adoption::External)
            .await
            .map_err(|e| match e {
                ResolveError::NotOwner { prefix, owner } => CreationError {
                    owner: Some(owner.clone()),
                    ..CreationError::new(
                        CreationFailure::Conflict,
                        "not_ring_owner",
                        &format!("shard {prefix} belongs to {owner}"),
                    )
                },
                ResolveError::Opening {
                    code,
                    retry_after_secs,
                    ..
                } => CreationError {
                    retry_after: Some(retry_after_secs),
                    ..CreationError::new(
                        CreationFailure::Opening,
                        code,
                        "shard not currently serving here; retry",
                    )
                },
                ResolveError::OpenFailed { prefix, error } => CreationError::new(
                    CreationFailure::Storage,
                    "shard_open",
                    &format!("open shard {prefix}: {error}"),
                ),
            })
    }
    pub(crate) async fn create(
        self: &Arc<Self>,
        command: CreateCommand,
    ) -> Result<CreateOutcome, CreationError> {
        let state = self.clone();
        let CreateCommand {
            project,
            name,
            key,
            content_type,
            mut ttl_secs,
            expires_at_ms,
            close,
            body,
            fork,
        } = command;
        let ct_hdr_present = content_type.is_some();
        let mut content_type = content_type.unwrap_or_else(|| "application/octet-stream".into());
        let route = crate::crypto::RouteHash::for_stream(&project.stream_ref(&name));
        let prefix = self.shards.prefix_for(&route.0);
        if let Some(owner) = self.ownership.foreign_owner(&prefix) {
            return Err(CreationError {
                owner: Some(owner.clone()),
                ..CreationError::new(
                    CreationFailure::Conflict,
                    "not_ring_owner",
                    &format!("shard {prefix} belongs to {owner}"),
                )
            });
        }
        // ---- Fork creation (pinned DS protocol fork contract) ----------
        // Parsed before descriptor resolution: validation errors must beat
        // creation, and the fork identity participates in the idempotent
        // compare.
        struct ForkCtx {
            source: String,
            source_desc: StreamDesc,
            boundary: u64,
            sub: u64,
            materialize: Option<Bytes>,
        }
        let fork_src_hdr = fork.as_ref().map(|f| f.source.clone());
        let fork_off_hdr = fork.as_ref().and_then(|f| f.offset);
        let fork_sub_hdr = fork.as_ref().and_then(|f| f.sub_offset);
        if fork_src_hdr.is_none() && (fork_off_hdr.is_some() || fork_sub_hdr.is_some()) {
            return Err(CreationError::new(
                CreationFailure::Invalid,
                "fork_headers",
                "Stream-Fork-Offset/Sub-Offset require Stream-Forked-From",
            ));
        }
        let fork_ctx: Option<ForkCtx> = if let Some(src_raw) = &fork_src_hdr {
            let src_name = src_raw.clone();
            // Is THIS child already mid-initialization against this source?
            // If so, the source is being retained FOR IT — its reference is
            // already installed — and refusing a retained source would leave
            // the child permanently Initializing over data kept expressly to
            // serve it. Resolve the target's own state before demanding a
            // live source.
            // Ready OR still initializing: either way, if this exact child
            // already holds a reference on the source, the source is being
            // retained for it. Restricting this to initializing children
            // broke idempotence — a completed fork whose response was lost
            // could not be re-PUT once its source was retained, because the
            // soft-delete check fired first.
            let resuming_child = match state.registry.get(&project.stream_ref(&name)).await {
                Ok(Some(c)) if !c.deleted => c
                    .forked_from
                    .clone()
                    .filter(|f| f.source == src_name && !f.fork_id.is_empty()),
                _ => None,
            };
            let src = match state.registry.get(&project.stream_ref(&src_name)).await {
                Ok(Some(d)) if desc_alive(&d) => d,
                // Retained for this very child: same incarnation, and the
                // reference this child installed is still on it.
                Ok(Some(d))
                    if !d.deleted
                        && resuming_child.as_ref().is_some_and(|f| {
                            f.source_epoch == d.stream_epoch && d.fork_children.contains(&f.fork_id)
                        }) =>
                {
                    d
                }
                Ok(Some(d)) if d.soft_deleted => {
                    return Err(CreationError::new(
                        CreationFailure::Conflict,
                        "fork_source_gone",
                        "source is deleted (data retained for existing forks only)",
                    ));
                }
                Ok(_) => {
                    return Err(CreationError::new(
                        CreationFailure::Missing,
                        "not_found",
                        "fork source not found",
                    ));
                }
                Err(e) => {
                    return Err(CreationError::new(
                        CreationFailure::Storage,
                        "internal",
                        &e.to_string(),
                    ));
                }
            };
            if src
                .segments
                .as_ref()
                .is_some_and(|m| m.segments.len() > 1 || m.pending.is_some())
            {
                return Err(CreationError::new(
                    CreationFailure::Invalid,
                    "fork_segmented_source",
                    "forking a segmented collection is not supported",
                ));
            }
            // Content type: inherit when omitted; explicit mismatch is 409
            // BEFORE any reference is taken.
            if ct_hdr_present
                && crate::registry::media_type(&content_type)
                    != crate::registry::media_type(&src.content_type)
            {
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "fork_content_type_mismatch",
                    "fork content type must match the source",
                ));
            }
            if !ct_hdr_present {
                content_type = src.content_type.clone();
            }
            if ttl_secs.is_none() && expires_at_ms.is_none() {
                ttl_secs = src.ttl_secs; // inherit source TTL
            }
            // Source key must accept the presented key (fork reads decrypt
            // the ancestor's records with it).
            if key.fingerprint(&src.epoch()) != src.key_fingerprint {
                return Err(CreationError::new(
                    CreationFailure::WrongKey,
                    "wrong_key",
                    "key mismatch with source",
                ));
            }
            let src_key = key.clone();
            let (_, src_handle) = match state.reads.handle_of(&src).await {
                Ok(v) => v,
                Err(m) => return Err(CreationError::new(CreationFailure::Storage, "internal", &m)),
            };
            let src_end = src_handle.state.lock().unwrap().durable.next;
            let base = match &fork_off_hdr {
                None => src_end,
                Some(offset) => *offset,
            };
            if base > src_end {
                return Err(CreationError::new(
                    CreationFailure::Invalid,
                    "fork_offset_beyond_end",
                    "fork offset beyond the source's length",
                ));
            }
            let sub = fork_sub_hdr.unwrap_or(0);
            let mut boundary = base;
            let mut materialize: Option<Bytes> = None;
            if sub > 0 {
                if fork_off_hdr.is_none() {
                    return Err(CreationError::new(
                        CreationFailure::Invalid,
                        "fork_headers",
                        "a sub-offset requires an explicit Stream-Fork-Offset",
                    ));
                }
                if src_end == 0 {
                    return Err(CreationError::new(
                        CreationFailure::Invalid,
                        "fork_sub_offset_empty_source",
                        "a sub-offset needs a record to split",
                    ));
                }
                if src.is_json() {
                    // Messages ARE records in this implementation: the
                    // sub-offset advances the record boundary.
                    boundary = base.saturating_add(sub);
                    if boundary > src_end {
                        return Err(CreationError::new(
                            CreationFailure::Invalid,
                            "fork_sub_offset_beyond_end",
                            "sub-offset overshoots the source",
                        ));
                    }
                } else {
                    if base >= src_end {
                        return Err(CreationError::new(
                            CreationFailure::Invalid,
                            "fork_sub_offset_beyond_end",
                            "no record at the fork offset",
                        ));
                    }
                    // The record being split (the source may itself be a
                    // fork — read through its chain).
                    let rec = match state
                        .reads
                        .read_stitched(&src, &src_key, base, 64 << 20)
                        .await
                    {
                        Ok(out) => out.recs.into_iter().find(|r| r.off == base),
                        Err(m) => {
                            return Err(CreationError::new(
                                CreationFailure::Storage,
                                "internal",
                                &m,
                            ));
                        }
                    };
                    let Some(rec) = rec else {
                        return Err(CreationError::new(
                            CreationFailure::Storage,
                            "internal",
                            "source record unavailable for sub-offset validation",
                        ));
                    };
                    let len = rec.payload.len() as u64;
                    if sub > len {
                        return Err(CreationError::new(
                            CreationFailure::Invalid,
                            "fork_sub_offset_beyond_end",
                            "sub-offset overshoots the record",
                        ));
                    }
                    if sub == len {
                        boundary = base + 1; // whole record inherited
                    } else {
                        boundary = base; // partial materializes at `base`
                        let m = rec.payload.slice(..sub as usize);
                        // Round-10e review: the materialized partial is a
                        // CUSTOMER record this child will persist — it
                        // must satisfy the per-record ceiling HERE, before
                        // any fork lifecycle work (tail seed, source
                        // reference) becomes durable. A source record
                        // created under a different profile or an older,
                        // larger ceiling must not smuggle an over-ring
                        // record past release certification.
                        if let Some(over) =
                            over_record_ceiling(state.record_ceiling, std::slice::from_ref(&m))
                        {
                            return Err(CreationError::new(
                                CreationFailure::TooLarge,
                                "record_too_large",
                                &format!(
                                    "the fork's materialized partial record of {over} bytes \
                                 exceeds the per-record ceiling (MAX_RECORD_PAYLOAD_BYTES)"
                                ),
                            ));
                        }
                        materialize = Some(m);
                    }
                }
            }
            Some(ForkCtx {
                source: src_name,
                source_desc: src,
                boundary,
                sub,
                materialize,
            })
        } else {
            None
        };
        let expected_fork_ref = fork_ctx.as_ref().map(|fc| crate::registry::ForkRef {
            source: fc.source.clone(),
            source_epoch: fc.source_desc.stream_epoch.clone(),
            fork_offset: fc.boundary,
            fork_sub: fc.sub,
            // The fork's unique id in the source's child set: this
            // incarnation's epoch, stamped after the descriptor exists.
            fork_id: String::new(),
        });

        // Creation-request identity (audit P0): a replayed PUT hashes
        // identically, so it JOINS an in-flight initialization instead of
        // observing the descriptor and skipping the work.
        let needs_init = !body.is_empty() || close || fork_src_hdr.is_some();
        let create_hash = create_request_hash(
            &content_type,
            ttl_secs,
            expires_at_ms,
            close,
            &body,
            expected_fork_ref.as_ref(),
        );

        // Resolve existing.
        let existing = match state.registry.get(&project.stream_ref(&name)).await {
            Ok(v) => v,
            Err(e) => {
                return Err(CreationError::new(
                    CreationFailure::Storage,
                    "internal",
                    &e.to_string(),
                ));
            }
        };
        // Idempotent-PUT validation against a live descriptor: shared by the
        // alive arm and by a lost recreate race (the winner's incarnation is
        // live, so the loser must observe it under the same rules).
        let validate_live =
        |d: crate::registry::StreamDesc| -> Result<(bool, crate::registry::StreamDesc), CreationError> {
            let same_ct = crate::registry::media_type(&d.content_type)
                == crate::registry::media_type(&content_type)
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
                return Err(CreationError::new(CreationFailure::WrongKey, "wrong_key", "key mismatch"));
            }
            Ok((false, d))
        };
        // An INITIALIZING descriptor (audit P0): its content is not durable
        // yet. The same request resumes the work; a different request is a
        // conflict, not an idempotent hit; a stale claim is taken over.
        let mut resume_init = false;
        if let Some(d) = existing.as_ref()
            && let Some(init) = &d.init
        {
            if !desc_alive(d) {
                // dead-and-initializing: fall through to the recreate arm
            } else if init.request_hash != create_hash {
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

        let (created, mut desc) = match existing {
            // Resume: the SAME creation request found its own in-flight
            // (or abandoned) initialization — redo it idempotently.
            Some(d) if resume_init => (true, d),
            Some(d) if desc_alive(&d) => match validate_live(d) {
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
                    &state.runtime,
                    &state.deployment,
                    &project,
                    &name,
                    &key,
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
                    .recreate(&project.stream_ref(&name), fresh, |d| {
                        !desc_alive(d) && !d.soft_deleted
                    })
                    .await
                {
                    Ok((true, d)) => (true, d),
                    Ok((false, winner)) => match validate_live(winner) {
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
                    &state.runtime,
                    &state.deployment,
                    &project,
                    &name,
                    &key,
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
                            Some(i) if i.request_hash == create_hash => (true, d),
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

        let hash = desc.resolve_segment("").identity;
        let epoch_bytes = desc.epoch();
        state.keys.put(hash, key.clone(), epoch_bytes);
        // Shard choice keys off the stream NAME hash (COMPUTE-SPEC R1) so the
        // router can compute placement without knowing the stream epoch; the
        // record keyspace keeps using storage/segment hashes.
        let engine = match state
            .resolve(&crate::crypto::RouteHash::for_stream(&desc.sref()).0)
            .await
        {
            Ok(e) => e,
            Err(r) => return Err(r),
        };

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
            if desc
                .forked_from
                .as_ref()
                .is_some_and(|f| f.fork_id.is_empty())
            {
                let fid = fork_id.clone();
                let stamped = state
                    .registry
                    .mutate_incarnation(&project.stream_ref(&name), &desc.stream_epoch, |current| {
                        stamp_fork_reference(current, &fid)
                    })
                    .await
                    .map_err(|error| {
                        CreationError::new(CreationFailure::Storage, "internal", &error.to_string())
                    })?;
                state.registry.invalidate(&project.stream_ref(&name));
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
                    f.fork_id = fork_id.clone();
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
            // The CHILD must still exist to be worth anchoring: a
            // half-made child deleted in the stamp-to-install window
            // must not pin the source at all (FRK-013). This check
            // closes the ordinary path; the residual window between it
            // and the install CAS — including a crash inside it — is
            // repaired by the tombstone's RETAINED debt. The park sits
            // BETWEEN the check and the install so tests can drive
            // exactly that window.
            match state.registry.get(&project.stream_ref(&name)).await {
                Ok(Some(c)) if desc_alive(&c) && c.stream_epoch == desc.stream_epoch => {}
                _ => {
                    return Err(CreationError::new(
                        CreationFailure::Conflict,
                        "fork_target_changed",
                        "the fork target changed while it was being created; retry",
                    ));
                }
            }
            #[cfg(test)]
            crate::failpoints::pause_fork_before_source_ref(&name).await;
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
                            m.pending.is_some()
                                || m.segments.iter().filter(|s| s.is_live()).count() > 1
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
            crate::failpoints::pause_fork_after_source_ref(&name).await;
            // Post-install: the child can have been deleted between the
            // pre-check and the install CAS. Release the reference this
            // request just installed — its tombstone's retained debt
            // covers the crash variant of the same window.
            match state.registry.get(&project.stream_ref(&name)).await {
                Ok(Some(c)) if desc_alive(&c) && c.stream_epoch == desc.stream_epoch => {}
                _ => {
                    if let Err(m) = release_fork_ref(
                        &state,
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
                match json_entries(&body, true) {
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
            if let Some(over) = over_record_ceiling(state.record_ceiling, &entries) {
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
            let subkey = derive_subkey(&key, &epoch_bytes, "", 0);
            let bytes = entries.iter().map(|e| e.len()).sum();
            #[cfg(test)]
            crate::failpoints::pause_init_before_seed(&name).await;
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
                usage: crate::usage::counters(
                    &crate::crypto::RouteHash::for_stream(&desc.sref()).0,
                ),
                seal_gen: None,
                billing: (!crate::billing::is_reserved_stream(&desc.name)).then(|| {
                    std::sync::Arc::new(crate::billing::BillingRef {
                        identity: crate::billing::identity_with_capabilities(
                            &state.auth,
                            &state.deployment,
                            &desc,
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

        // Publish Ready: every durable initialization step (fork tail seed,
        // source reference, initial content, close-on-create) has landed.
        // Until this CAS, a replay resumes instead of observing a stream
        // whose content never arrived.
        if created && needs_init {
            #[cfg(test)]
            crate::failpoints::pause_create_before_ready(&name).await;
            let published = match state
                .registry
                .mutate_incarnation(&project.stream_ref(&name), &desc.stream_epoch, |current| {
                    if current.deleted
                        || !current
                            .init
                            .as_ref()
                            .is_some_and(|i| i.request_hash == create_hash)
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
            state.registry.invalidate(&project.stream_ref(&name));
            // A declined CAS is NOT readiness. `cas_update` refuses a
            // deleted descriptor, so a delete that won mid-initialization
            // made this return 201 for a stream that no longer exists — and
            // if the work had already installed a fork reference, the source
            // stayed pinned by a child that was never published.
            if !published {
                let now = state
                    .registry
                    .get(&project.stream_ref(&name))
                    .await
                    .ok()
                    .flatten();
                let live_and_ready = now.as_ref().is_some_and(|d| {
                    desc_alive(d) && d.init.is_none() && d.stream_epoch == desc.stream_epoch
                });
                if !live_and_ready {
                    // Compensate: give back the source reference this
                    // initialization installed, so the parent is not held by
                    // a child that will never exist.
                    if let Some(fr) = desc.forked_from.as_ref().filter(|f| !f.fork_id.is_empty())
                        && let Err(m) = release_fork_ref(
                            &state,
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

        Ok(CreateOutcome {
            created,
            desc,
            next,
            closed: closed_now,
        })
    }
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
pub(crate) fn desc_alive(desc: &crate::registry::PersistedDescriptor) -> bool {
    !desc.deleted
        && !desc.soft_deleted
        && desc.expires_at_ms.is_none_or(|expires| now_ms() < expires)
}
fn init_claim_stale(desc: &crate::registry::PersistedDescriptor) -> bool {
    desc.init
        .as_ref()
        .is_some_and(|init| now_ms() - init.claimed_ms > crate::registry::INIT_CLAIM_MS)
}
pub(crate) fn create_request_hash(
    content_type: &str,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
    close: bool,
    body: &[u8],
    fork: Option<&crate::registry::ForkRef>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(content_type.as_bytes());
    h.update([0u8]);
    h.update(ttl_secs.unwrap_or(0).to_le_bytes());
    h.update(expires_at_ms.unwrap_or(0).to_le_bytes());
    h.update([u8::from(close)]);
    h.update((body.len() as u64).to_le_bytes());
    h.update(body);
    if let Some(fr) = fork {
        h.update(fr.source.as_bytes());
        h.update([0u8]);
        // The source INCARNATION is part of the identity. Without it, a
        // retry against a recreated source hashed the same as the
        // original, so it resumed an initialization whose stored
        // forked_from still pointed at the previous epoch — reference
        // installed on incarnation B, child recorded against A, and
        // stitched reads later failing the epoch check.
        h.update(fr.source_epoch.as_bytes());
        h.update([0u8]);
        h.update(fr.fork_offset.to_le_bytes());
        h.update(fr.fork_sub.to_le_bytes());
    }
    hex(&h.finalize()[..16])
}
pub(crate) fn fresh_desc(
    runtime: &crate::runtime::RuntimeCaps,
    deployment: &crate::deployment::DeploymentIdentity,
    project: &crate::tenant::ProjectId,
    name: &str,
    key: &StreamKey,
    content_type: String,
    ttl_secs: Option<u64>,
    expires_at_ms: Option<i64>,
) -> crate::registry::PersistedDescriptor {
    let epoch = runtime.epoch();
    crate::registry::PersistedDescriptor {
        name: name.to_string(),
        account_id: Some(deployment.account_id().to_string()),
        project_id: project.clone(),
        stream_epoch: hex(&epoch),
        seal_gen_counter: 0,
        key_fingerprint: key.fingerprint(&epoch),
        created_ms: now_ms(),
        expires_at_ms: ttl_secs
            .map(|t| now_ms() + (t as i64) * 1000)
            .or(expires_at_ms),
        deleted: false,
        soft_deleted: false,
        logical_close_ms: None,
        forked_from: None,
        fork_children: Vec::new(),
        init: None,
        sealing: None,
        seal_op: None,
        content_type,
        ttl_secs,
        segments: None,
        sealed: false,
        watch_definitions: Vec::new(),
        watch_sig_key: None,
        parent_ref_pending: false,
        layout_version: crate::registry::LAYOUT_VERSION,
    }
}

pub(crate) fn json_entries(body: &[u8], allow_empty_array: bool) -> Result<Vec<Bytes>, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "invalid JSON body".to_string())?;
    match v {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() && !allow_empty_array {
                return Err("empty JSON array".to_string());
            }
            Ok(arr
                .iter()
                .map(|e| Bytes::from(serde_json::to_vec(e).expect("json")))
                .collect())
        }
        other => Ok(vec![Bytes::from(serde_json::to_vec(&other).expect("json"))]),
    }
}
pub(crate) fn over_record_ceiling(cap: usize, entries: &[Bytes]) -> Option<usize> {
    if cap == 0 {
        return None;
    }
    entries.iter().map(|e| e.len()).find(|l| *l > cap)
}
async fn clear_parent_debt(
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
fn release_fork_ref(
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
        if d.deleted {
            if d.parent_ref_pending
                && let Some((src, fid, sep)) = parent.clone()
            {
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
        crate::ops::emit(
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

impl CreationService {
    pub(crate) fn touch_ttl(self: &Arc<Self>, desc: &StreamDesc) {
        let Some(ttl) = desc.ttl_secs else { return };
        let Some(exp) = desc.expires_at_ms else {
            return;
        };
        let window_ms = (ttl as i64).saturating_mul(1000);
        let now = now_ms();
        if exp.saturating_sub(now) >= window_ms - window_ms / 4 {
            return; // window still fresh
        }
        // One in-flight slide per stream: without this, every request in
        // the window between spawn and CAS completion spawns ANOTHER CAS —
        // a herd against the registry under rapid op sequences.
        // Søren review: keyed by the PROJECT-QUALIFIED ref — a bare-name
        // set let same-name projects suppress one another's slides, and
        // the spawned CAS below extended (or epoch-fenced into a no-op
        // against) the deployment tenant's descriptor instead of this one.
        let sref = desc.sref();
        if !self.sliding.lock().unwrap().insert(sref.clone()) {
            return; // a slide is already in flight
        }
        let target = now + window_ms;
        let state = self.clone();
        let expect_epoch = desc.stream_epoch.clone();
        let slide = TtlSlide {
            sliding: self.sliding.clone(),
            sref: sref.clone(),
        };
        tokio::spawn(async move {
            let _slide = slide;
            // The registry re-decides each typed mutation after a conditional conflict.
            // Incarnation-fenced: a slide spawned against incarnation A must
            // not extend the expiry of a replacement created under the same
            // name while the task sat on the runtime.
            if let Err(e) = state
                .registry
                .mutate_incarnation(&sref, &expect_epoch, |current| {
                    if current.deleted
                        || current.ttl_secs.is_none()
                        || !current.expires_at_ms.is_some_and(|e| e < target)
                    {
                        return Mutation::Decline(());
                    }
                    let mut next = current.to_persisted();
                    next.expires_at_ms = Some(target);
                    Mutation::Write(next, ())
                })
                .await
            {
                tracing::warn!(
                    project = %sref.project_id().as_str(),
                    stream = %sref.name().as_str(),
                    "ttl slide lost: {e}"
                );
            }
            state.registry.invalidate(&sref);
        });
    }
}

pub(crate) struct ProductCreateConfig {
    pub content_type: String,
    pub ttl_secs: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub watches: Vec<crate::registry::WatchDefinition>,
}
pub(crate) enum ProductCreateError {
    Creation(CreationError),
    Quota(crate::quota::QuotaRefusal),
}
impl From<CreationError> for ProductCreateError {
    fn from(e: CreationError) -> Self {
        Self::Creation(e)
    }
}
impl CreationService {
    pub(crate) async fn create_product(
        self: &Arc<Self>,
        project: &crate::tenant::ProjectId,
        name: String,
        key: StreamKey,
        cfg: ProductCreateConfig,
        quotas: Option<&crate::project_policy::ProjectQuotas>,
    ) -> Result<(bool, StreamDesc), ProductCreateError> {
        let sref = project.stream_ref(&name);
        let prefix = self
            .shards
            .prefix_for(&crate::crypto::RouteHash::for_stream(&sref).0);
        if let Some(owner) = self.ownership.foreign_owner(&prefix) {
            return Err(CreationError {
                owner: Some(owner.clone()),
                ..CreationError::new(
                    CreationFailure::Conflict,
                    "not_ring_owner",
                    &format!("shard {prefix} belongs to {owner}"),
                )
            }
            .into());
        }
        // One fail-closed snapshot determines whether this attempt needs a
        // reservation. The reservation rolls back if an idempotent winner wins.
        let existing = self.registry.get(&sref).await.map_err(|e| {
            CreationError::new(CreationFailure::Storage, "internal", &e.to_string())
        })?;
        let mut reservation = None;
        if let Some(quotas) = quotas.filter(|q| q.max_streams > 0)
            && !existing.as_ref().is_some_and(|d| desc_alive(d))
        {
            let seed = if self.quotas.needs_stream_seed(project) {
                Some(self.count_project_streams(project).await?)
            } else {
                None
            };
            reservation = self
                .quotas
                .reserve_stream(project, quotas, seed)
                .map_err(ProductCreateError::Quota)?;
        }
        let validate_live = |d: StreamDesc| -> Result<StreamDesc, CreationError> {
            if crate::registry::media_type(&d.content_type)
                != crate::registry::media_type(&cfg.content_type)
                || d.ttl_secs != cfg.ttl_secs
                || (cfg.ttl_secs.is_none() && d.expires_at_ms != cfg.expires_at_ms)
                || d.watch_definitions != cfg.watches
            {
                return Err(CreationError::new(
                    CreationFailure::Conflict,
                    "config_mismatch",
                    "stream exists with different immutable configuration",
                ));
            }
            if d.key_fingerprint != key.fingerprint(&d.epoch()) {
                return Err(CreationError::new(
                    CreationFailure::WrongKey,
                    "wrong_key",
                    "encryption key mismatch",
                ));
            }
            Ok(d)
        };
        let build_fresh = || {
            let mut d = fresh_desc(
                &self.runtime,
                &self.deployment,
                project,
                &name,
                &key,
                cfg.content_type.clone(),
                cfg.ttl_secs,
                cfg.expires_at_ms,
            );
            d.watch_definitions = cfg.watches.clone();
            // Only this key-bearing creation attempt can install the verifier.
            if let Some(epoch) = d.epoch_bytes() {
                use base64::Engine;
                let token = crate::crypto::touch_token(&key, &epoch);
                d.watch_sig_key = Some(
                    base64::engine::general_purpose::STANDARD
                        .encode(crate::crypto::wait_sig_key(&token, &epoch)),
                );
            }
            d
        };
        let (created, desc) = match existing {
            Some(d) if desc_alive(&d) => (false, validate_live(d)?),
            Some(_) => {
                let (created, winner) = self
                    .registry
                    .recreate(&sref, build_fresh(), |d| {
                        !desc_alive(d) && !d.soft_deleted && d.fork_children.is_empty()
                    })
                    .await
                    .map_err(|e| {
                        CreationError::new(CreationFailure::Storage, "internal", &e.to_string())
                    })?;
                if created {
                    (true, winner)
                } else {
                    if winner.soft_deleted || !winner.fork_children.is_empty() {
                        return Err(CreationError::new(
                            CreationFailure::Conflict,
                            "gone",
                            "name is retained for live forks",
                        )
                        .into());
                    }
                    (false, validate_live(winner)?)
                }
            }
            None => {
                let (created, d) = self.registry.create(build_fresh()).await.map_err(|e| {
                    CreationError::new(CreationFailure::Storage, "internal", &e.to_string())
                })?;
                if created {
                    (true, d)
                } else {
                    (false, validate_live(d)?)
                }
            }
        };
        if created && let Some(reservation) = reservation.take() {
            reservation.commit();
        }
        Ok((created, desc))
    }
    async fn count_project_streams(
        &self,
        project: &crate::tenant::ProjectId,
    ) -> Result<u64, CreationError> {
        let mut count = 0;
        let mut after = None;
        loop {
            let page = self
                .registry
                .list_page(project, after.as_deref(), 512)
                .await
                .map_err(|_| {
                    CreationError::new(
                        CreationFailure::Opening,
                        "catalog_unavailable",
                        "stream count unavailable; retry",
                    )
                })?;
            count += page
                .streams
                .iter()
                .filter(|d| desc_alive(d) || d.soft_deleted)
                .count() as u64;
            if page.exhausted || page.next_after.is_none() {
                return Ok(count);
            }
            after = page.next_after;
        }
    }
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

struct TtlSlide {
    sliding: Arc<std::sync::Mutex<std::collections::HashSet<crate::tenant::TenantStreamRef>>>,
    sref: crate::tenant::TenantStreamRef,
}
impl Drop for TtlSlide {
    fn drop(&mut self) {
        self.sliding.lock().unwrap().remove(&self.sref);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn r05_cancelled_ttl_attempt_releases_only_its_owned_slot() {
        let project = crate::tenant::ProjectId::new("creation-test").unwrap();
        let source = project.stream_ref("source");
        let other = project.stream_ref("other");
        let sliding = Arc::new(std::sync::Mutex::new(std::collections::HashSet::from([
            source.clone(),
            other.clone(),
        ])));
        let guard = TtlSlide {
            sliding: sliding.clone(),
            sref: source.clone(),
        };
        let (entered, waiting) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = guard;
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        waiting.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            *sliding.lock().unwrap(),
            std::collections::HashSet::from([other])
        );
        assert!(
            sliding.lock().unwrap().insert(source),
            "retry can claim the released slot"
        );
    }
}
