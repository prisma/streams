//! The registry's in-process descriptor cache: the TTL'd, ETag-revalidated
//! read path, the bounded insert its writers share and the invalidation
//! every descriptor write publishes through. Moved verbatim out of
//! registry.rs; its read contract is stated in the commit that follows.
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use super::{CachedDesc, Registry, StreamDesc, decode_desc, desc_path};

impl Registry {
    /// Bounded cache insert: the descriptor cache previously grew with
    /// every distinct name ever touched (static-audit memory finding —
    /// creates alone put 100k entries in it). At the cap, expired
    /// entries purge first (TTL is seconds, so this is almost always
    /// enough), then the oldest entry falls out.
    #[expect(
        clippy::unwrap_used,
        reason = "Registry::cache_insert; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(super) fn cache_insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
        const REGISTRY_CACHE_MAX: usize = 65_536;
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= REGISTRY_CACHE_MAX && !cache.contains_key(&sref) {
            let ttl = self.cache_ttl;
            cache.retain(|_, e| e.at.elapsed() < ttl);
            if cache.len() >= REGISTRY_CACHE_MAX
                && let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, e)| e.at)
                    .map(|(n, _)| n.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(sref, entry);
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Registry::cache_len; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Test-only: plant a descriptor in the cache as if this instance
    /// had read it moments ago — the cross-instance stale-descriptor
    /// shape (another instance CAS'd a transition we have not seen).
    #[cfg(test)]
    pub(crate) fn test_poison_cache(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        desc: StreamDesc,
    ) {
        self.cache_insert(
            sref.clone(),
            CachedDesc {
                desc: Some(desc),
                at: Instant::now(),
                etag: None,
            },
        );
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Registry::get; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) async fn get(
        &self,
        sref: &crate::tenant::TenantStreamRef,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        #[cfg(test)]
        if self
            .fail_next_get
            .lock()
            .unwrap()
            .remove(sref.name().as_str())
        {
            return Err(object_store::Error::Generic {
                store: "registry",
                source: "injected registry get failure".into(),
            });
        }
        let revalidate = {
            let cache = self.cache.lock().unwrap();
            match cache.get(sref) {
                Some(e) if e.at.elapsed() < self.cache_ttl => return Ok(e.desc.clone()),
                Some(e) => e.etag.clone().map(|t| (t, e.desc.clone())),
                None => None,
            }
        };
        // TTL expired on a descriptor we hold an ETag for: conditional
        // refresh. Unchanged (the overwhelmingly common case — a
        // descriptor changes only on delete/recreate/config update) comes
        // back 304 and only renews the TTL; a real change pays for a body.
        let opts = |etag: Option<String>| object_store::GetOptions {
            if_none_match: etag,
            ..Default::default()
        };
        let (etag_sent, cached_desc) = match revalidate {
            Some((t, d)) => (Some(t), d),
            None => (None, None),
        };
        let fetched = match self
            .store
            .get_opts(&desc_path(&self.cell, sref), opts(etag_sent.clone()))
            .await
        {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                let raw = r.bytes().await?;
                // Fail CLOSED on a corrupt descriptor: treating it as absent
                // would let a create/recreate path overwrite a live stream's
                // identity (key epoch, incarnation) — worse than an error.
                match decode_desc(&raw, Some(sref)) {
                    Ok(d) => (Some(d), etag),
                    Err(e) => {
                        return Err(object_store::Error::Generic {
                            store: "registry",
                            source: format!("descriptor for {sref}: {e}").into(),
                        });
                    }
                }
            }
            Err(object_store::Error::NotModified { .. }) => (cached_desc, etag_sent),
            Err(object_store::Error::NotFound { .. }) => (None, None),
            Err(e) => return Err(e),
        };
        self.cache_insert(
            sref.clone(),
            CachedDesc {
                desc: fetched.0.clone(),
                at: Instant::now(),
                etag: fetched.1,
            },
        );
        Ok(fetched.0)
    }

    #[expect(
        clippy::unwrap_used,
        reason = "Registry::invalidate; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(crate) fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) {
        self.cache.lock().unwrap().remove(sref);
    }

    /// Force a cached entry past its TTL so tests can exercise the
    /// refresh path without sleeping through the real TTL.
    #[cfg(test)]
    pub(super) fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) {
        if let Some(e) = self.cache.lock().unwrap().get_mut(sref) {
            e.at -= self.cache_ttl + Duration::from_secs(1);
        }
    }
}
