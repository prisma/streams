//! Product create comparison and quota reservation, using the same persisted lifecycle.
use super::*;

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
        sref: crate::tenant::TenantStreamRef,
        key: StreamKey,
        cfg: ProductCreateConfig,
        quotas: Option<&crate::project_policy::ProjectQuotas>,
    ) -> Result<(bool, StreamDesc), ProductCreateError> {
        let project = sref.project_id();
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
                self,
                &sref,
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
