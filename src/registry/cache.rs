//! The registry's in-process descriptor cache.
//!
//! A descriptor is immutable for the life of an incarnation and changes
//! only on create, delete, recreate or a lifecycle/topology transition, so
//! a read is served from here for a short TTL and then revalidated against
//! the object's ETag (a 304 renews the TTL for free; a change pays for a
//! body). Every descriptor write publishes through `invalidate`; `create`
//! fills the cache with what it wrote.
//!
//! The contract that keeps a write's invalidation from being lost: a read
//! that goes to the store first claims the stream's slot (empty, under a
//! fresh generation) if there is none, and its answer is published only if
//! the slot still carries the generation it read when the read returns. An
//! invalidation never removes a slot; it empties the slot under a fresh
//! generation. A read that fetched the pre-write descriptor and returns
//! after the write therefore finds the generation moved and keeps its
//! answer to itself. That one caller still gets the answer: the store
//! served it, so it is a valid read that happened before the write; only
//! its reuse would be wrong. Two reads of one stream that both miss: the
//! first to return publishes, the second skips its publish and returns its
//! own answer. An invalidation of a stream with no slot is a no-op: absence
//! is never a generation, so no read in flight could publish against it.
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use object_store::path::Path as ObjPath;

use super::{CachedDesc, Registry, StreamDesc, decode_desc, desc_path};

/// The descriptor cache previously grew with every distinct name ever
/// touched (static-audit memory finding: creates alone put 100k entries
/// in it). At the cap, slots untouched for a TTL purge first (TTL is
/// seconds, so this is almost always enough), then the least recently
/// touched falls out.
const REGISTRY_CACHE_MAX: usize = 65_536;

pub(super) struct DescriptorCache {
    slots: Mutex<Slots>,
}

struct Slots {
    by_ref: HashMap<crate::tenant::TenantStreamRef, Slot>,
    ttl: Duration,
    /// Slots held at most; a new key beyond it runs the purge in `put`.
    cap: usize,
    /// The generation handed out last. Every fill, every invalidation and
    /// every read's claim takes the next one, so no two slot states (of
    /// any stream, at any time) share a generation.
    last_generation: u64,
}

/// One stream's slot. `entry` is `None` after an invalidation or under a
/// read's claim: the slot survives to carry the generation a read in
/// flight compares against.
struct Slot {
    generation: u64,
    /// When the slot was last filled, emptied, claimed or captured by a
    /// read going to the store. The cap purges only slots untouched for
    /// a TTL (a filled one is expired by then, an empty one is a stale
    /// invalidation or an abandoned claim), so a read in flight always
    /// finds the slot it left, or the write that moved it.
    touched: Instant,
    entry: Option<CachedDesc>,
}

/// What a read found in the cache.
enum Lookup {
    /// Served from the cache; no store traffic.
    Fresh(Option<StreamDesc>),
    /// Go to the store. `seen` is the slot generation to publish against;
    /// `held` is the expired entry's ETag and descriptor for a conditional
    /// refresh.
    Fetch {
        seen: u64,
        held: Option<(String, Option<StreamDesc>)>,
    },
}

/// A body the store served, or its confirmed absence (`desc: None`), with
/// the object's ETag. A fetch answers `None` instead of one of these when
/// the store said 304: the entry the read held is current.
struct Fetched {
    desc: Option<StreamDesc>,
    etag: Option<String>,
}

/// Whether `at` is still within `ttl` of `now`. The window is exclusive:
/// an entry or a slot exactly a TTL old is expired.
fn within(at: Instant, now: Instant, ttl: Duration) -> bool {
    now.duration_since(at) < ttl
}

impl Slots {
    fn stamp(&mut self) -> u64 {
        self.last_generation += 1;
        self.last_generation
    }

    fn generation(&self, sref: &crate::tenant::TenantStreamRef) -> Option<u64> {
        self.by_ref.get(sref).map(|slot| slot.generation)
    }

    /// Bounded: at the cap, slots untouched for a TTL purge first, then
    /// the least recently touched falls out. Either way the slot takes a
    /// fresh generation, which is returned.
    fn put(&mut self, sref: crate::tenant::TenantStreamRef, entry: Option<CachedDesc>) -> u64 {
        if self.by_ref.len() >= self.cap && !self.by_ref.contains_key(&sref) {
            let (ttl, now) = (self.ttl, Instant::now());
            self.by_ref.retain(|_, slot| within(slot.touched, now, ttl));
            if self.by_ref.len() >= self.cap
                && let Some(oldest) = self
                    .by_ref
                    .iter()
                    .min_by_key(|(_, slot)| slot.touched)
                    .map(|(n, _)| n.clone())
            {
                self.by_ref.remove(&oldest);
            }
        }
        let generation = self.stamp();
        self.by_ref.insert(
            sref,
            Slot {
                generation,
                touched: Instant::now(),
                entry,
            },
        );
        generation
    }
}

impl DescriptorCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self::bounded(ttl, REGISTRY_CACHE_MAX)
    }

    fn bounded(ttl: Duration, cap: usize) -> Self {
        Self {
            slots: Mutex::new(Slots {
                by_ref: HashMap::new(),
                ttl,
                cap,
                last_generation: 0,
            }),
        }
    }

    /// The one place the cache decides what a poisoned lock means.
    #[expect(
        clippy::unwrap_used,
        reason = "DescriptorCache::slots; a poisoned descriptor cache may hold a partially filled or invalidated slot; recovering it could serve a stale incarnation as current"
    )]
    fn slots(&self) -> MutexGuard<'_, Slots> {
        self.slots.lock().unwrap()
    }

    /// Slots held, emptied ones included: the cardinality figure.
    fn len(&self) -> usize {
        self.slots().by_ref.len()
    }

    /// A writer's fill (what `create` wrote) published under a fresh
    /// generation, unconditionally.
    fn insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
        self.slots().put(sref, Some(entry));
    }

    /// A write's publication: the slot is emptied under a fresh
    /// generation, never removed, so a read in flight sees the move. A
    /// stream with no slot has no read in flight to inform.
    fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) {
        let mut slots = self.slots();
        if slots.by_ref.contains_key(sref) {
            slots.put(sref.clone(), None);
        }
    }

    fn lookup(&self, sref: &crate::tenant::TenantStreamRef) -> Lookup {
        let mut slots = self.slots();
        let (ttl, now) = (slots.ttl, Instant::now());
        if let Some(slot) = slots.by_ref.get_mut(sref) {
            if let Some(e) = slot.entry.as_ref().filter(|e| within(e.at, now, ttl)) {
                return Lookup::Fresh(e.desc.clone());
            }
            // Captured by a read going to the store: the slot stays out
            // of the cap purge while the read is in flight.
            slot.touched = Instant::now();
            return Lookup::Fetch {
                seen: slot.generation,
                held: slot
                    .entry
                    .as_ref()
                    .and_then(|e| e.etag.clone().map(|t| (t, e.desc.clone()))),
            };
        }
        // No slot: claim one, empty, so that a write or an eviction
        // between now and the publish moves the generation this read
        // compares against. Absence is never a generation.
        Lookup::Fetch {
            seen: slots.put(sref.clone(), None),
            held: None,
        }
    }

    /// Publish a read's answer only if the slot is exactly as the read
    /// found it. A moved generation means a write landed after the store
    /// served this read; the answer must not outlive that write.
    fn settle(&self, sref: &crate::tenant::TenantStreamRef, seen: u64, entry: CachedDesc) {
        let mut slots = self.slots();
        if slots.generation(sref) == Some(seen) {
            slots.put(sref.clone(), Some(entry));
        }
    }

    /// Serve `sref` from the cache, or `fetch` it (handing over the ETag
    /// to revalidate against, if any) and publish the answer under the
    /// contract above. The answer is returned to the caller either way.
    async fn fill<E>(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        fetch: impl AsyncFnOnce(Option<String>) -> Result<Option<Fetched>, E>,
    ) -> Result<Option<StreamDesc>, E> {
        let (seen, held) = match self.lookup(sref) {
            Lookup::Fresh(desc) => return Ok(desc),
            Lookup::Fetch { seen, held } => (seen, held),
        };
        let if_none_match = held.as_ref().map(|(etag, _)| etag.clone());
        let (desc, etag) = match fetch(if_none_match).await? {
            Some(Fetched { desc, etag }) => (desc, etag),
            // 304: the held entry is current. (A 304 to an unconditional
            // GET is a store fault; it publishes an absence, as before.)
            None => held.map_or((None, None), |(etag, desc)| (desc, Some(etag))),
        };
        self.settle(
            sref,
            seen,
            CachedDesc {
                desc: desc.clone(),
                at: Instant::now(),
                etag,
            },
        );
        Ok(desc)
    }

    /// Force a slot past its TTL (its entry's freshness and its
    /// `touched` age alike) so tests can exercise the refresh path and
    /// the cap purge without sleeping through the real TTL.
    #[cfg(test)]
    fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) {
        let mut slots = self.slots();
        let age = slots.ttl + Duration::from_secs(1);
        if let Some(slot) = slots.by_ref.get_mut(sref) {
            slot.touched -= age;
            if let Some(e) = slot.entry.as_mut() {
                e.at -= age;
            }
        }
    }
}

impl Registry {
    /// A writer's fill: `create` publishes what it wrote, with the ETag
    /// the store returned for it.
    pub(super) fn cache_insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
        self.cache.insert(sref, entry);
    }

    pub(crate) fn cache_len(&self) -> usize {
        self.cache.len()
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

    pub(crate) async fn get(
        &self,
        sref: &crate::tenant::TenantStreamRef,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        #[cfg(test)]
        if self.take_fail_next_get(sref) {
            return Err(object_store::Error::Generic {
                store: "registry",
                source: "injected registry get failure".into(),
            });
        }
        self.cache
            .fill(sref, async |if_none_match| {
                let path = desc_path(&self.cell, sref);
                self.fetch_descriptor(&path, sref, if_none_match).await
            })
            .await
    }

    #[cfg(test)]
    fn take_fail_next_get(&self, sref: &crate::tenant::TenantStreamRef) -> bool {
        self.fail_next_get
            .lock()
            .unwrap()
            .remove(sref.name().as_str())
    }

    /// One GET of the descriptor object (conditional when the cache
    /// holds an ETag), decoded fail-closed. `None` is the store's 304.
    async fn fetch_descriptor(
        &self,
        path: &ObjPath,
        sref: &crate::tenant::TenantStreamRef,
        if_none_match: Option<String>,
    ) -> Result<Option<Fetched>, object_store::Error> {
        let options = object_store::GetOptions {
            if_none_match,
            ..Default::default()
        };
        match self.store.get_opts(path, options).await {
            Ok(r) => {
                let etag = r.meta.e_tag.clone();
                let raw = r.bytes().await?;
                // Fail CLOSED on a corrupt descriptor: treating it as absent
                // would let a create/recreate path overwrite a live stream's
                // identity (key epoch, incarnation) — worse than an error.
                match decode_desc(&raw, Some(sref)) {
                    Ok(d) => Ok(Some(Fetched {
                        desc: Some(d),
                        etag,
                    })),
                    Err(e) => Err(object_store::Error::Generic {
                        store: "registry",
                        source: Box::new(CorruptDescriptor(format!("descriptor for {sref}: {e}"))),
                    }),
                }
            }
            Err(object_store::Error::NotModified { .. }) => Ok(None),
            Err(object_store::Error::NotFound { .. }) => Ok(Some(Fetched {
                desc: None,
                etag: None,
            })),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) {
        self.cache.invalidate(sref);
    }

    #[cfg(test)]
    pub(super) fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) {
        self.cache.expire_for_tests(sref);
    }
}

#[cfg(test)]
mod tests;

/// A descriptor that was read but does not decode or validate: corruption,
/// never a transient store failure. A read failing with it is final (a
/// retry reads the same bytes), so callers fail closed; any other registry
/// read failure is the store's, and may pass on retry.
#[derive(Debug)]
pub(crate) struct CorruptDescriptor(String);

impl std::fmt::Display for CorruptDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CorruptDescriptor {}

/// Whether a registry read failed on a corrupt descriptor rather than on
/// the store.
pub(crate) fn is_corrupt_descriptor(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::Generic { source, .. } if source.downcast_ref::<CorruptDescriptor>().is_some()
    )
}
