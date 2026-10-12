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
pub(crate) const REGISTRY_CACHE_MAX: usize = 65_536;

/// The cache's estimate of what it holds (`slot_bytes`), at most: a bound
/// on the estimate, which counts every part of a descriptor that a client
/// or the scaler grows as it is allocated, until the shared-cell capacity
/// run (SC2) measures the bytes really held. Under the count cap alone,
/// descriptors with the most a creation document allocates (64 watches of
/// 16 one-byte pointers, about 37 KB decoded) held about 2.3 GiB, one
/// project's 8,192 streams about 290 MiB; at 64 MiB about 1,800 fit.
/// 65,536 ordinary slots are about 53 MiB of estimate, so on a cell of
/// ordinary descriptors the count cap binds first.
const REGISTRY_CACHE_MAX_BYTES: usize = 64 << 20;

/// What every slot is charged before its descriptor's variable parts: the
/// slot itself, inline (an emptied, claimed or absent slot is as large as
/// a filled one), and its key. A descriptor's fixed strings (its name,
/// project, epoch, fingerprint, content type, signing key and ETag, about
/// 250 bytes for an ordinary one) are not counted: their length is bounded
/// by validation, and leaving them out keeps the count cap binding first
/// on a cell of ordinary descriptors (which then hold about 68 MiB).
const SLOT_BYTES: usize =
    std::mem::size_of::<Slot>() + std::mem::size_of::<crate::tenant::TenantStreamRef>();

pub(super) struct DescriptorCache {
    slots: Mutex<Slots>,
}

struct Slots {
    by_ref: HashMap<crate::tenant::TenantStreamRef, Slot>,
    ttl: Duration,
    /// Slots held at most; a new key beyond it runs the purge in `put`.
    cap: usize,
    /// Bytes held at most (the sum of the slots' `bytes`); a put that
    /// would pass it runs the purge in `put`.
    byte_cap: usize,
    /// The sum of every held slot's `bytes`.
    bytes: usize,
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
    /// What the slot is charged against the byte cap: `slot_bytes` of its
    /// entry, estimated when the slot was filled, emptied or claimed.
    bytes: usize,
    entry: Option<CachedDesc>,
}

/// The estimated bytes a slot holding `entry` holds: `SLOT_BYTES`, and
/// for a descriptor the heap of its variable parts (`descriptor_heap`).
fn slot_bytes(entry: Option<&CachedDesc>) -> usize {
    let desc = entry.and_then(|e| e.desc.as_ref());
    SLOT_BYTES + desc.map_or(0, descriptor_heap)
}

/// The heap block an allocation of `len` bytes takes: mimalloc's blocks
/// are multiples of 8 bytes, the smallest 8; an empty string or vector
/// allocates nothing.
fn block(len: usize) -> usize {
    len.next_multiple_of(8)
}

/// The heap of `strings` as a vector holds it: the vector's buffer and
/// each string's block, by capacity.
fn strings_heap(strings: &[String], capacity: usize) -> usize {
    let each: usize = strings.iter().map(|s| block(s.capacity())).sum();
    block(capacity * std::mem::size_of::<String>()) + each
}

/// The heap a decoded descriptor's variable parts hold, as allocated, not
/// as long: its watch definitions (the vector's buffer, each name, each
/// field vector and pointer), which a creation document bounds at 64 of 16
/// pointers; its fork children's names; and its segment map (the vector's
/// buffer, each segment's shard prefix and lineage), which nothing bounds
/// (#NEW-phase-b-5's residual). One-byte strings are charged their 8-byte
/// blocks and 24-byte headers, so a descriptor of the shortest pointers
/// is charged for what it holds, not for the bytes its document spelled.
pub(super) fn descriptor_heap(desc: &StreamDesc) -> usize {
    let watch = std::mem::size_of::<crate::registry::WatchDefinition>();
    let watches = &desc.watch_definitions;
    let definitions: usize = watches
        .iter()
        .map(|w| block(w.name.capacity()) + strings_heap(&w.fields, w.fields.capacity()))
        .sum();
    let forks = &desc.fork_children;
    let segment = std::mem::size_of::<crate::segmap::SegmentDesc>();
    let segments = desc.segments.as_ref().map_or(0, |map| {
        let lineage: usize = map
            .segments
            .iter()
            .map(|s| {
                let ids = |v: &Vec<u32>| block(v.capacity() * std::mem::size_of::<u32>());
                block(s.shard_prefix.capacity()) + ids(&s.predecessors) + ids(&s.successors)
            })
            .sum();
        block(map.segments.capacity() * segment) + lineage
    });
    block(watches.capacity() * watch)
        + definitions
        + strings_heap(forks, forks.capacity())
        + segments
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

    /// Bounded: when the slot would take the cache past its count cap (a
    /// new key) or its byte cap (a new key, or a held one that grows),
    /// slots untouched for a TTL purge first, then the least recently
    /// touched fall out until it fits. Either way the slot takes a fresh
    /// generation, which is returned.
    fn put(&mut self, sref: crate::tenant::TenantStreamRef, entry: Option<CachedDesc>) -> u64 {
        let bytes = slot_bytes(entry.as_ref());
        // The slot's own bytes are replaced, never a reason to evict it.
        if let Some(held) = self.by_ref.remove(&sref) {
            self.bytes -= held.bytes;
        }
        self.make_room(bytes);
        let generation = self.stamp();
        self.bytes += bytes;
        self.by_ref.insert(
            sref,
            Slot {
                generation,
                touched: Instant::now(),
                bytes,
                entry,
            },
        );
        generation
    }

    /// Whether one more slot of `bytes` would pass the count cap or the
    /// byte cap.
    fn over(&self, bytes: usize) -> bool {
        self.by_ref.len() >= self.cap || self.bytes + bytes > self.byte_cap
    }

    /// Room for one more slot of `bytes`: past either cap, every slot
    /// untouched for a TTL purges, then the least recently touched fall
    /// out until the slot fits (or the cache is empty: a slot larger than
    /// the byte cap is held alone). The slots are ordered by age once, so a
    /// put that evicts many slots (one large descriptor in place of many
    /// small ones) sorts them once under the lock rather than rescanning
    /// every slot for each; a slot touched at the same instant as the last
    /// one that had to go goes with it.
    fn make_room(&mut self, bytes: usize) {
        if !self.over(bytes) {
            return;
        }
        let (ttl, now) = (self.ttl, Instant::now());
        self.evict(|slot| !within(slot.touched, now, ttl));
        let mut ages: Vec<(Instant, usize)> = self
            .by_ref
            .values()
            .map(|slot| (slot.touched, slot.bytes))
            .collect();
        ages.sort_unstable_by_key(|&(touched, _)| touched);
        let (mut count, mut held, mut cutoff) = (self.by_ref.len(), self.bytes, None);
        for (touched, slot_bytes) in ages {
            if count < self.cap && held + bytes <= self.byte_cap {
                break;
            }
            (count, held, cutoff) = (count - 1, held - slot_bytes, Some(touched));
        }
        if let Some(cutoff) = cutoff {
            self.evict(|slot| slot.touched <= cutoff);
        }
    }

    /// Removes every slot `goes` names, giving back its bytes.
    fn evict(&mut self, goes: impl Fn(&Slot) -> bool) {
        let mut freed = 0;
        self.by_ref.retain(|_, slot| {
            let gone = goes(slot);
            if gone {
                freed += slot.bytes;
            }
            !gone
        });
        self.bytes -= freed;
    }
}

impl DescriptorCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self::bounded(ttl, REGISTRY_CACHE_MAX, REGISTRY_CACHE_MAX_BYTES)
    }

    fn bounded(ttl: Duration, cap: usize, byte_cap: usize) -> Self {
        Self {
            slots: Mutex::new(Slots {
                by_ref: HashMap::new(),
                ttl,
                cap,
                byte_cap,
                bytes: 0,
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

    /// The estimated bytes held, the figure the byte cap bounds.
    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.slots().bytes
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

    /// What `sref`'s slot holds, fresh or expired, and whether it is still
    /// within its TTL; `None` for no slot or an emptied one. A peek claims
    /// nothing and leaves the slot's `touched` as it was, so it neither
    /// fills the cache nor moves a slot in the cap's eviction order.
    fn peek(&self, sref: &crate::tenant::TenantStreamRef) -> Option<(Option<StreamDesc>, bool)> {
        let slots = self.slots();
        let entry = slots.by_ref.get(sref)?.entry.as_ref()?;
        Some((
            entry.desc.clone(),
            within(entry.at, Instant::now(), slots.ttl),
        ))
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

    /// The descriptor (or the absence) the cache holds for `sref`, fresh
    /// or expired, with whether it is still fresh; `None` when it holds
    /// nothing. For a caller that has not yet proved anything about the
    /// stream: it neither claims, touches nor fills a slot. On one server a
    /// held entry is current, since every descriptor write empties its slot
    /// here; in a fleet an expired one may be stale.
    pub(crate) fn held_descriptor(
        &self,
        sref: &crate::tenant::TenantStreamRef,
    ) -> Option<(Option<StreamDesc>, bool)> {
        self.cache.peek(sref)
    }

    /// One unconditional GET of `sref`'s descriptor that claims no slot and
    /// publishes nothing: its answer is the caller's alone, so an unproven
    /// caller's reads never fill the cache. `None` is the stream's absence.
    pub(crate) async fn fetch_unpublished(
        &self,
        sref: &crate::tenant::TenantStreamRef,
    ) -> Result<Option<StreamDesc>, object_store::Error> {
        let fetched = self
            .fetch_descriptor(&desc_path(&self.cell, sref), sref, None)
            .await?;
        Ok(fetched.and_then(|fetched| fetched.desc))
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
