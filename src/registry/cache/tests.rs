//! The descriptor cache under a write that lands inside a read's reply: the
//! read answers what the store served it and never publishes it.
#![cfg(test)]
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};

use super::{DescriptorCache, Fetched, within};
use crate::registry::tests::{desc, ts};
use crate::registry::{
    CachedDesc, Mutation, MutationResult, PersistedDescriptor, Registry, SealIntent, SealState,
    StreamDesc,
};

const EPOCH: &str = "00000000000000000000000000000001";

/// A store whose next GET carries a writer in its reply: the object is read,
/// then `writer` runs to completion against the registry (its own reads and
/// CAS pass straight through), and only then does the read's result reach
/// the caller. This is a read whose reply was slower than a write issued
/// after the store served it - the shape `ConflictOnceStore` in
/// `registry/tests.rs` gives to a lost CAS.
struct WriterInReply {
    inner: Arc<dyn ObjectStore>,
    writer: Mutex<Option<BoxFuture<'static, ()>>>,
}

impl WriterInReply {
    fn arm(&self, writer: BoxFuture<'static, ()>) {
        *self.writer.lock().unwrap() = Some(writer);
    }
}

impl std::fmt::Debug for WriterInReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriterInReply")
    }
}
impl std::fmt::Display for WriterInReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WriterInReply")
    }
}

#[async_trait::async_trait]
impl ObjectStore for WriterInReply {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &ObjPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let result = self.inner.get_opts(location, options).await;
        // Taken out of the lock before it is awaited (a guard may not
        // cross an await point).
        let writer = self.writer.lock().unwrap().take();
        if let Some(writer) = writer {
            writer.await;
        }
        result
    }
    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn rig() -> (Arc<Registry>, Arc<WriterInReply>) {
    let store = Arc::new(WriterInReply {
        inner: Arc::new(object_store::memory::InMemory::new()),
        writer: Mutex::new(None),
    });
    let reg = Arc::new(Registry::new(
        store.clone(),
        &crate::tenant::CellId::new("test-cell").unwrap(),
    ));
    (reg, store)
}

/// The writer every scenario races: one incarnation-fenced mutation of
/// `name`, applied through the registry like any lifecycle transition
/// (invalidate, read, CAS, invalidate).
fn mutate(
    reg: Arc<Registry>,
    name: &'static str,
    apply: fn(&mut PersistedDescriptor),
) -> BoxFuture<'static, ()> {
    Box::pin(async move {
        let outcome = reg
            .mutate_incarnation(&ts(name), EPOCH, |d| {
                let mut next = d.to_persisted();
                apply(&mut next);
                Mutation::Write(next, ())
            })
            .await
            .unwrap();
        assert!(
            matches!(outcome, MutationResult::Applied(())),
            "the raced writer did not apply: {outcome:?}"
        );
    })
}

/// Shape A, the field flake's shape: the writer's attempt-start invalidation
/// emptied the slot, so the read fetches the in-flight seal claim; the
/// completed seal lands while the reply is on its way back. The read answers
/// the claim it fetched; the read after it must see the completed seal, not
/// the claim for a whole TTL.
#[tokio::test]
async fn a_read_that_raced_a_write_never_publishes_the_pre_write_descriptor() {
    let (reg, store) = rig();
    let mut claimed = desc("s", EPOCH, false);
    claimed.sealing = Some(SealState {
        operation_id: "op".into(),
        claimed_ms: 1,
        claim_generation: 0,
        intent: SealIntent::Empty,
    });
    reg.create(claimed).await.unwrap();
    reg.invalidate(&ts("s"));
    store.arm(mutate(reg.clone(), "s", |d| {
        d.sealing = None;
        d.sealed = true;
    }));

    let raced = reg.get(&ts("s")).await.unwrap().unwrap();
    assert!(
        raced.sealing.is_some() && !raced.sealed,
        "the raced read answers what the store served it"
    );
    let after = reg.get(&ts("s")).await.unwrap().unwrap();
    assert!(
        after.sealed && after.sealing.is_none(),
        "a read after the completed seal still served the in-flight claim"
    );
}

/// Shape B: an expired entry revalidates with its ETag, the store answers
/// 304, and the write lands while the 304 is on its way back. The renewed
/// entry must not outlive the write it raced.
#[tokio::test]
async fn a_renewed_304_never_outlives_the_write_it_raced() {
    let (reg, store) = rig();
    reg.create(desc("r", EPOCH, false)).await.unwrap();
    reg.expire_for_tests(&ts("r"));
    store.arm(mutate(reg.clone(), "r", |d| d.ttl_secs = Some(60)));

    let raced = reg.get(&ts("r")).await.unwrap().unwrap();
    assert_eq!(
        raced.ttl_secs, None,
        "the 304 confirms the entry the read held"
    );
    let after = reg.get(&ts("r")).await.unwrap().unwrap();
    assert_eq!(
        after.ttl_secs,
        Some(60),
        "a read after the write still served the renewed pre-write entry"
    );
}

/// Shape C: a read of a stream that does not exist yet fetches its absence;
/// the stream is created while the reply is on its way back. The read
/// answers absent; the read after it must see the created stream.
#[tokio::test]
async fn a_read_that_raced_a_create_never_publishes_the_absence() {
    let (reg, store) = rig();
    store.arm(Box::pin({
        let reg = reg.clone();
        async move {
            let (created, _) = reg.create(desc("n", EPOCH, false)).await.unwrap();
            assert!(created, "the raced create did not win");
        }
    }));

    assert!(
        reg.get(&ts("n")).await.unwrap().is_none(),
        "the raced read answers the absence the store served it"
    );
    assert!(
        reg.get(&ts("n")).await.unwrap().is_some(),
        "a read after the create still served the raced absence"
    );
}

/// Shape D: the recreate's loser reads the live incarnation somebody else
/// installed; a transition of that incarnation lands in its reply. The
/// loser used to publish what it read.
#[tokio::test]
async fn a_recreate_loser_never_publishes_the_incarnation_it_lost_to() {
    let (reg, store) = rig();
    reg.create(desc("d", EPOCH, false)).await.unwrap();
    reg.invalidate(&ts("d"));
    store.arm(mutate(reg.clone(), "d", |d| d.ttl_secs = Some(60)));

    let fresh = desc("d", "00000000000000000000000000000002", false);
    let (won, current) = reg.recreate(&ts("d"), fresh, |d| d.deleted).await.unwrap();
    assert!(
        !won && current.ttl_secs.is_none(),
        "the loser observes the incarnation the store served it"
    );
    let after = reg.get(&ts("d")).await.unwrap().unwrap();
    assert_eq!(
        after.ttl_secs,
        Some(60),
        "a read after the write still served the loser's raced fill"
    );
}

/// CONTROL (green before and after): the guard is per stream. A cache-wide
/// generation fails here, and would turn every overlapping write into a
/// billable GET on an unrelated stream.
#[tokio::test]
async fn a_write_to_another_stream_leaves_a_read_free_to_publish() {
    let (reg, store) = rig();
    reg.create(desc("a", EPOCH, false)).await.unwrap();
    reg.create(desc("b", EPOCH, false)).await.unwrap();
    reg.invalidate(&ts("a"));
    assert_eq!(
        reg.cache_len(),
        2,
        "the cardinality figure counts emptied slots"
    );
    store.arm(mutate(reg.clone(), "b", |d| d.ttl_secs = Some(60)));

    assert!(reg.get(&ts("a")).await.unwrap().is_some());
    // Published: the next read is a hit, so a writer armed in the reply
    // never runs and is still armed afterwards.
    store.arm(Box::pin(async {
        panic!("a GET went out: the read of a was not published");
    }));
    assert!(reg.get(&ts("a")).await.unwrap().is_some());
    assert!(
        store.writer.lock().unwrap().is_some(),
        "a GET went out: the read of a was not published"
    );
}

/// The cache alone (exists to kill the mutants of `settle` and `stamp`): a
/// fetch whose slot moved underneath it answers but does not publish; one
/// whose slot held still publishes; a hit never fetches.
#[tokio::test]
async fn fill_publishes_only_against_the_generation_it_read() {
    let cache = DescriptorCache::new(Duration::from_secs(5));
    let sref = ts("u");
    let old = StreamDesc::try_from(desc("u", EPOCH, false)).unwrap();
    let mut newer = desc("u", EPOCH, false);
    newer.ttl_secs = Some(60);
    let newer = StreamDesc::try_from(newer).unwrap();
    let fetches = Cell::new(0);

    let answered = cache
        .fill(&sref, async |if_none_match| {
            assert_eq!(if_none_match, None, "a cold miss sends no ETag");
            fetches.set(fetches.get() + 1);
            cache.invalidate(&sref); // the write lands before the read returns
            Ok::<_, std::convert::Infallible>(Some(Fetched {
                desc: Some(old.clone()),
                etag: Some("e1".into()),
            }))
        })
        .await
        .unwrap();
    assert_eq!(
        answered.map(|d| d.ttl_secs),
        Some(None),
        "the raced answer is returned"
    );

    let served = cache
        .fill(&sref, async |_| {
            fetches.set(fetches.get() + 1);
            Ok::<_, std::convert::Infallible>(Some(Fetched {
                desc: Some(newer.clone()),
                etag: Some("e2".into()),
            }))
        })
        .await
        .unwrap();
    assert_eq!(
        (fetches.get(), served.map(|d| d.ttl_secs)),
        (2, Some(Some(60))),
        "the raced answer was published"
    );

    let hit = cache
        .fill(&sref, async |_| {
            fetches.set(fetches.get() + 1);
            Ok::<_, std::convert::Infallible>(None)
        })
        .await
        .unwrap();
    assert_eq!(
        (fetches.get(), hit.map(|d| d.ttl_secs)),
        (2, Some(Some(60)))
    );
}

/// At the cap, a create's purge must not erase the invalidation a read in
/// flight has to see. The read claims its slot before fetching and the
/// purge keeps every slot touched within a TTL, so the read finds the
/// generation moved and keeps its answer to itself; the next read fetches.
#[tokio::test]
async fn a_cap_purge_never_erases_the_invalidation_a_read_in_flight_must_see() {
    let cache = DescriptorCache::bounded(Duration::from_secs(5), 2);
    let raced = ts("raced");
    let old = StreamDesc::try_from(desc("raced", EPOCH, false)).unwrap();
    let mut newer = desc("raced", EPOCH, false);
    newer.ttl_secs = Some(60);
    let newer = StreamDesc::try_from(newer).unwrap();
    let entry_of = |d: &StreamDesc| CachedDesc {
        desc: Some(d.clone()),
        at: Instant::now(),
        etag: None,
    };
    cache.insert(ts("filler-1"), entry_of(&old));
    cache.insert(ts("filler-2"), entry_of(&old)); // at the cap

    let answered = cache
        .fill(&raced, async |_| {
            cache.invalidate(&raced); // the write lands
            cache.insert(ts("newcomer"), entry_of(&newer)); // a create at the cap: the purge runs
            Ok::<_, std::convert::Infallible>(Some(Fetched {
                desc: Some(old.clone()),
                etag: None,
            }))
        })
        .await
        .unwrap();
    assert_eq!(
        answered.map(|d| d.ttl_secs),
        Some(None),
        "the raced answer is returned"
    );

    let fetched = Cell::new(false);
    cache
        .fill(&raced, async |_| {
            fetched.set(true);
            Ok::<_, std::convert::Infallible>(Some(Fetched {
                desc: Some(newer.clone()),
                etag: None,
            }))
        })
        .await
        .unwrap();
    assert!(
        fetched.get(),
        "the raced answer was published over the purge"
    );
}

/// A filled slot, served from the cache: the fetch closure is never run.
async fn held(cache: &DescriptorCache, sref: &crate::tenant::TenantStreamRef) -> bool {
    let fetched = Cell::new(false);
    let served = cache
        .fill(sref, async |_| {
            fetched.set(true);
            Ok::<_, std::convert::Infallible>(None)
        })
        .await
        .unwrap();
    served.is_some() && !fetched.get()
}

fn filled(name: &str) -> CachedDesc {
    CachedDesc {
        desc: Some(StreamDesc::try_from(desc(name, EPOCH, false)).unwrap()),
        at: Instant::now(),
        etag: Some(format!("etag-{name}")),
    }
}

/// The cap counts every slot, emptied ones included, and it purges only
/// for a key the cache does not hold: below the cap nothing is evicted,
/// and a re-fill of a held key evicts nothing even at the cap.
#[tokio::test]
async fn the_cap_purges_only_for_a_new_key_at_the_cap() {
    let cache = DescriptorCache::bounded(Duration::from_secs(5), 3);
    cache.insert(ts("a"), filled("a"));
    cache.insert(ts("b"), filled("b"));
    cache.invalidate(&ts("b"));
    assert_eq!(cache.len(), 2, "an emptied slot is held");
    cache.expire_for_tests(&ts("a"));
    cache.insert(ts("c"), filled("c"));
    assert_eq!(cache.len(), 3, "below the cap, a stale slot is kept");
    cache.insert(ts("c"), filled("c"));
    assert_eq!(cache.len(), 3, "at the cap, a held key evicts nothing");
}

/// At the cap a new key purges the slots untouched for a TTL first, and
/// keeps every young one; only when that frees nothing does the least
/// recently touched slot fall out.
#[tokio::test]
async fn the_cap_evicts_stale_slots_first_then_the_least_recently_touched() {
    let cache = DescriptorCache::bounded(Duration::from_secs(5), 2);
    cache.insert(ts("old"), filled("old"));
    cache.expire_for_tests(&ts("old"));
    cache.insert(ts("young"), filled("young"));
    cache.insert(ts("new"), filled("new"));
    assert_eq!(cache.len(), 2);
    assert!(
        held(&cache, &ts("young")).await,
        "the young slot survived the purge"
    );
    assert!(held(&cache, &ts("new")).await);

    // All young: the least recently touched goes. Re-filling `new`
    // touches it, so `young` is now the older of the two.
    cache.insert(ts("new"), filled("new"));
    cache.insert(ts("newer"), filled("newer"));
    assert_eq!(cache.len(), 2);
    assert!(
        held(&cache, &ts("new")).await,
        "the recently touched slot survived"
    );
    assert!(held(&cache, &ts("newer")).await);
}

/// A cold miss with no write in flight publishes: the claim it made is
/// the generation it publishes against, so the second read is a hit.
#[tokio::test]
async fn a_cold_miss_with_no_write_in_flight_publishes() {
    let cache = DescriptorCache::new(Duration::from_secs(5));
    let sref = ts("cold");
    let fetches = Cell::new(0);
    let fetch = async |_| {
        fetches.set(fetches.get() + 1);
        Ok::<_, std::convert::Infallible>(Some(Fetched {
            desc: Some(StreamDesc::try_from(desc("cold", EPOCH, false)).unwrap()),
            etag: Some("e1".into()),
        }))
    };
    let first = cache.fill(&sref, fetch).await.unwrap();
    assert!(first.is_some());
    assert!(held(&cache, &sref).await, "the cold miss was not published");
    assert_eq!(fetches.get(), 1);
}

/// At the cap, a read in flight keeps its slot (claimed or captured)
/// through a purge another key triggers: an expired entry's free 304
/// still renews it, and a cold claim still publishes.
#[tokio::test]
async fn a_read_in_flight_keeps_its_slot_through_a_cap_purge() {
    let cache = DescriptorCache::bounded(Duration::from_secs(5), 2);
    cache.insert(ts("expired"), filled("expired"));
    cache.expire_for_tests(&ts("expired"));
    cache.insert(ts("filler"), filled("filler"));
    // The expired entry revalidates; the store answers 304 while a create
    // at the cap runs the purge.
    let renewed = cache
        .fill(&ts("expired"), async |if_none_match| {
            assert_eq!(
                if_none_match.as_deref(),
                Some("etag-expired"),
                "a conditional refresh"
            );
            cache.insert(ts("newcomer"), filled("newcomer"));
            Ok::<_, std::convert::Infallible>(None)
        })
        .await
        .unwrap();
    assert!(renewed.is_some(), "the 304 confirms the held entry");
    assert!(
        held(&cache, &ts("expired")).await,
        "the renewal was lost to the purge"
    );
    assert!(cache.len() <= 2, "the cap holds: {}", cache.len());

    // A cold claim, likewise: the create's purge must not erase it.
    let published = cache
        .fill(&ts("claimed"), async |_| {
            cache.insert(ts("another"), filled("another"));
            Ok::<_, std::convert::Infallible>(Some(Fetched {
                desc: Some(StreamDesc::try_from(desc("claimed", EPOCH, false)).unwrap()),
                etag: None,
            }))
        })
        .await
        .unwrap();
    assert!(published.is_some());
    assert!(
        held(&cache, &ts("claimed")).await,
        "the claim was lost to the purge"
    );
    assert!(cache.len() <= 2, "the cap holds: {}", cache.len());
}

/// An invalidation of a stream with no slot leaves the cache as it was:
/// write-only streams do not grow it, and no read could be in flight
/// against a slot that does not exist.
#[tokio::test]
async fn an_invalidation_of_an_unknown_stream_is_a_no_op() {
    let cache = DescriptorCache::new(Duration::from_secs(5));
    cache.insert(ts("known"), filled("known"));
    cache.invalidate(&ts("unknown"));
    assert_eq!(cache.len(), 1);
    cache.invalidate(&ts("known"));
    assert_eq!(cache.len(), 1, "an emptied slot is held");
    assert!(
        !held(&cache, &ts("known")).await,
        "an emptied slot is a miss"
    );
}

/// The recreate loser's read of the incarnation it lost to must not be
/// published either when the winner was ANOTHER instance: nothing on
/// this process invalidates, so only the loser's own discipline keeps
/// the dead descriptor it cached from being confirmed over the live one.
#[tokio::test]
async fn a_recreate_loser_on_another_instances_win_reads_the_live_incarnation_next() {
    let (reg, store) = rig();
    reg.create(desc("x", EPOCH, true)).await.unwrap();
    let live = desc("x", "00000000000000000000000000000002", false);
    let body = serde_json::to_vec(&StreamDesc::try_from(live).unwrap()).unwrap();
    let path = crate::registry::desc_path(&reg.cell, &ts("x"));
    store
        .inner
        .put(&path, object_store::PutPayload::from(body))
        .await
        .unwrap();

    let fresh = desc("x", "00000000000000000000000000000003", false);
    let (won, current) = reg.recreate(&ts("x"), fresh, |d| d.deleted).await.unwrap();
    assert!(
        !won && !current.deleted,
        "the loser observes the live incarnation"
    );
    let after = reg.get(&ts("x")).await.unwrap().unwrap();
    assert!(
        !after.deleted,
        "the next read still served the cached dead descriptor"
    );
}

/// The TTL window is exclusive at its end: exactly a TTL old is expired.
#[test]
fn the_ttl_window_ends_exactly_at_the_ttl() {
    let ttl = Duration::from_secs(5);
    let at = Instant::now();
    assert!(within(at, at, ttl));
    assert!(within(at, at + ttl - Duration::from_nanos(1), ttl));
    assert!(!within(at, at + ttl, ttl));
    assert!(!within(at, at + ttl + Duration::from_nanos(1), ttl));
}
