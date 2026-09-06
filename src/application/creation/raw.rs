//! Claim and complete one create request, including durable fork anchoring and initial append.
use super::*;

impl CreationService {
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
            ttl_secs,
            expires_at_ms,
            close,
            body,
            fork,
        } = command;
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
        let ct_hdr_present = content_type.is_some();
        let prepared = fork::prepare(
            &state,
            fork::Preparation {
                project: &project,
                name: &name,
                key: &key,
                content_type,
                ttl_secs,
                expires_at_ms,
                fork: fork.as_ref(),
            },
        )
        .await?;
        let content_type = prepared.content_type;
        let ttl_secs = prepared.ttl_secs;
        let fork_ctx = prepared.context;
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
        let needs_init = !body.is_empty() || close || fork.is_some();
        let create_hash = create_request_hash(
            &content_type,
            ttl_secs,
            expires_at_ms,
            close,
            &body,
            expected_fork_ref.as_ref(),
        );

        let plan = CreatePlan {
            project,
            name,
            key,
            content_type,
            ct_hdr_present,
            ttl_secs,
            expires_at_ms,
            close,
            body,
            fork_ctx,
            expected_fork_ref,
            needs_init,
            create_hash,
        };
        let (created, desc) = super::claim::resolve(&state, &plan).await?;
        let hash = desc.resolve_segment("").identity;
        let epoch_bytes = desc.epoch();
        state.keys.put(hash, plan.key.clone(), epoch_bytes);
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

        let (desc, materialize_entry) =
            super::anchor::install(&state, &plan, &engine, desc, created).await?;
        let (next, closed_now) =
            super::initialization::seed(&state, &plan, &engine, &desc, created, materialize_entry)
                .await?;
        super::initialization::publish(&state, &plan, &desc, created).await?;
        Ok(CreateOutcome {
            created,
            desc,
            next,
            closed: closed_now,
        })
    }
}
pub(super) struct CreatePlan {
    pub project: crate::tenant::ProjectId,
    pub name: String,
    pub key: StreamKey,
    pub content_type: String,
    pub ct_hdr_present: bool,
    pub ttl_secs: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub close: bool,
    pub body: Bytes,
    pub fork_ctx: Option<fork::ForkCtx>,
    pub expected_fork_ref: Option<crate::registry::ForkRef>,
    pub needs_init: bool,
    pub create_hash: String,
}
