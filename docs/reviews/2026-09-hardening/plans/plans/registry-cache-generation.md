# Registry::get lost-invalidate race — implementation plan (second pass)

Read-only investigation against `slate` @ `00ff0e7e` (2026-09-22; the tree is
clean, `src/registry.rs` = 1,665 lines). Nothing was compiled or run. Every
Rust block is near-complete; the compile risks are listed in section 6 with
their fallbacks.

**Verdict on the claim: CONFIRMED first-hand, not stale.** `Registry::get`
drops the cache lock at `:1033`, awaits the store at `:1046-1070`, and inserts
unconditionally at `:1071-1078`; `invalidate` at `:1463-1465` is a bare
`remove`, so a write leaves no trace a late insert could notice. The same
unconditional insert also covers a 304 renewal, a cached absence over a
`create`, and `recreate`'s loser branch (section 1.2).

**Why this pass differs from the previous plan at this path.** That plan (a)
made the "move" commit restructure `create`, `recreate`, `get` and
`test_poison_cache` and relied on fact-removal accounting inside two
`expect_used`-ratcheted functions, which is exactly the trap the task warns
about; (b) chose a three-field write log (`clock`/`written`/`floor`, a 4,096
clear, a `Fetch` ticket) where a single per-slot generation suffices; and (c)
staged its red tests with `Notify` parking + `future::join` + a 10 s timeout.
This pass: commit 1 is a pure verbatim move (no ratcheted function is edited;
only two visibility tokens change), the fix is one monotonic counter and
"invalidate empties a slot instead of removing it", and the red tests run the
concurrent writer *inside the store reply* — no parking, no join, nothing that
can hang.

---

## 1. Mechanism, with evidence

All lines are `src/registry.rs` @ `00ff0e7e`.

### 1.1 The race in `Registry::get`

| Line | What it does |
|---|---|
| `:840-859` `struct Registry` | `cache: Mutex<HashMap<TenantStreamRef, CachedDesc>>` (`:844`), `cache_ttl: Duration` (`:845`, 5 s at `:943`). No generation/version anywhere. |
| `:861-870` `struct CachedDesc` | `desc: Option<StreamDesc>` (an absence is cached too), `at: Instant`, `etag: Option<String>`. |
| `:961-977` `cache_insert` | cap logic, then a plain `cache.insert(sref, entry)` (`:976`). Unconditional. |
| `:1026-1033` `get`, lookup | lock; fresh hit returns; otherwise copies `(etag, desc)` out; **the guard drops at `:1033`**. |
| `:1046-1070` `get`, fetch | `get_opts(.., If-None-Match)` + `r.bytes().await` + `decode_desc`. 8-185 ms on Tigris (`src/dst/fault_store.rs:61`). |
| `:1067` | `Err(NotModified) => (cached_desc, etag_sent)` — a 304 re-inserts the descriptor copied out at `:1030`. |
| `:1068` | `Err(NotFound) => (None, None)` — an absence is inserted. |
| `:1071-1078` `get`, insert | `self.cache_insert(sref.clone(), CachedDesc { desc: fetched.0.clone(), at: Instant::now(), etag: fetched.1 })` — **unconditional, fresh `at`**. |
| `:1463-1465` `invalidate` | `self.cache.lock().unwrap().remove(sref)` — leaves nothing behind. |

Interleaving (R = any reader; W = `mutate_incarnation` `:1293-1374`, which
invalidates before each attempt's read at `:1302` and after its CAS at `:1356`):

```
R   get: lock, miss (W's :1302 emptied the slot), unlock            :1026-1033
R   store GET served -> D0 / E0; reply on its way back               :1046
W   GET D0/E0; PUT D1 (If-Match E0) -> E1; invalidate (remove)       :1303-1357
R   cache_insert(D0, at = now, etag = E0)                            :1071-1078
*   every get on this process answers D0 until at + 5 s              :1029
```

After 5 s the refresh sends `If-None-Match: E0`, the store holds E1, a body
comes back — so one occurrence is bounded by one TTL, but every reader in the
meantime (SSE sources, watches, consumers, reads) consumes D0.

Field evidence: commit `668bc80c` ("A topology transition never reaches a
writer as closure") records the flake — `dst livefeed_merge_continuation_in_place`,
1 run in 7, an append after a completed merge answered
`409 {"error":{"code":"sealed",..}}` — and names this mechanism in its body
("Registry::get can re-insert a descriptor it fetched before a transition
after the transition's invalidate"). That commit made `AppendService` retry
on a stale descriptor; it did not touch the cache.

### 1.2 The same insert, four shapes

| Shape | Late inserter | Installs after the write | Evidence |
|---|---|---|---|
| A | `get`, cold miss | pre-write D0 | `:1071` |
| B | `get`, TTL revalidation answered 304 **before** the write | the pre-write `cached_desc` from `:1030`, TTL renewed | `:1067` → `:1071` |
| C | `get`, NotFound **before** a `create` on this process | `desc: None` over the entry `create` installed at `:1104` — "does not exist" for 5 s after a successful create | `:1068` → `:1071` |
| D | `recreate`, loser branch (`!still_dead`) | the `current` it read at `:1152-1164`, inserted at `:1166-1173` | `:1166` |
| E | `create`, success | its own descriptor at `:1104-1111`; stale only if another writer completes a full GET+PUT inside the PUT reply's latency | `:1104` (residual, section 2.4) |

### 1.3 Every path that inserts or invalidates

- `cache_insert` callers: `:996` `test_poison_cache` (cfg(test)), `:1071` `get`,
  `:1104` `create`, `:1166` `recreate`.
- `invalidate` callers inside the file: `:1115` `create` (AlreadyExists),
  `:1187` `recreate` (CAS won), `:1232` `update` (cfg(test)), `:1302`+`:1356`
  `mutate_incarnation`, `:1388` `cas_update_retry` (cfg(test)), `:1455`
  `cas_update` (cfg(test)).
- Production `registry.invalidate(..)` callers outside the file (20 sites):
  `src/sse/source.rs:828`, `src/scaler3/controller.rs:155`,
  `src/application/read_request.rs:168`, `src/application/lifecycle.rs:205,227,713,809,863`,
  `src/application/append.rs:218`, `src/application/append/route.rs:12`,
  `src/application/creation/initialization.rs:226`,
  `src/application/consumer/deletion.rs:246`,
  `src/application/creation/anchor.rs:132,239`,
  `src/application/creation/deletion.rs:65,192,204,304`,
  `src/application/topology.rs:426,553`. All go through `Registry::invalidate`,
  so one implementation change covers them.
- Direct map access: `:1510` `expire_for_tests` (cfg(test); rewinds `at`
  only), `:984` `cache_len` (`src/http.rs:971`, the `/debug` cardinality row).
- `src/registry/catalog.rs` never touches the cache (checked: only
  `self.store`/`self.cell`). `desc_path` is private, so nothing else writes
  descriptor objects.
- The append-side fix (`src/application/append.rs:218`,
  `src/application/append/route.rs:12`) is `invalidate` + `get`. It is itself
  exposed to the race: a third reader's late insert can land between its
  `invalidate` and its `get`'s insert, so the guard belongs in the cache.

### 1.4 Why the existing hooks cannot stage it, and what can

The red test needs the writer to land **after the store served the read and
before `get` inserts**.

- `FaultStore::hold_class` (`src/dst/fault_store.rs:218-244`, `:415-430`)
  parks **before** `inner.get_opts`. Releasing it lets the read see the
  post-write object — the wrong side of the window.
- `src/failpoints.rs` has no registry site; adding `Fp::RegistryGetBeforeInsert`
  means a `#[cfg(test)] pause(..).await` on the hot read path plus a variant,
  an `ALL` entry and a `site()` arm — a production edit for a test.
- The precedent in this file is `ConflictOnceStore` (`src/registry/tests.rs:507-547`):
  a store decorator that performs the concurrent winner's write **inside**
  the operation it wraps. The same shape for reads: a decorator whose `get_opts`
  performs the inner GET, then **runs the writer to completion**, then returns
  the pre-write reply. That is the race, byte for byte, with no concurrency in
  the test at all (section 3).

---

## 2. Design

### 2.1 Rejected: one cache-wide generation

An `AtomicU64` bumped by every insert/invalidate, captured before the fetch,
compared after: correct and ~10 lines, but a read overlapping **any** local
descriptor write to **any** stream would skip its insert. Creates alone ran at
100k in the memory audit (`:952-954`); every skipped insert is one more
billable GET on the next read of that stream. The object-store cost review
(item 5, `:864-868`) exists to keep exactly those GETs down. The guard must be
per stream.

### 2.2 Recommended: a per-slot generation, and an invalidation that empties the slot

State, all under the cache's one mutex:

```
Slots { by_ref: HashMap<TenantStreamRef, Slot>, ttl, last_generation: u64 }
Slot  { generation: u64, entry: Option<CachedDesc> }
```

- Every mutation of a slot — a writer's insert **or** an invalidation — stores
  `generation = ++last_generation`. Generations are unique across all streams
  and all time, so there is no ABA even after a slot is evicted and re-created.
- `invalidate` does **not** remove the slot: it stores
  `Slot { generation: fresh, entry: None }`. An emptied slot is a miss for
  readers, carries no ETag (so the refetch is unconditional, as today after a
  `remove`), and keeps the generation a read in flight compares against.
- A read that misses captures `seen = slot.generation` (`None` if there is no
  slot), fetches, then publishes **only if** `slot.generation == seen` — check
  and insert under one lock acquisition. Absent → absent publishes; absent →
  any slot, or any moved generation, skips.
- Bounded exactly as today: at 65,536 slots the purge `retain`s live
  unexpired entries (empty slots are the cheapest to lose — a read that then
  finds `None != Some(g)` merely skips its insert), then evicts the oldest.
  Memory per empty slot ≈ key + 8 bytes + niche; bounded by the same cap.

**What `get` returns when it skips:** the value it fetched. The store served it,
so it is a valid read that linearizes before the write; only its *reuse* is
unsafe. No retry, no second lookup — a retry loop under a write storm is a
new failure mode, and callers that need the post-write view already
`invalidate` first (`append.rs:218`). The tests pin this ("the raced read
answers what the store served it").

Per site:

| Site | After the fix |
|---|---|
| `get` | `DescriptorCache::fill` (lookup → fetch → conditional publish) |
| `create` `:1104` | unchanged text: `self.cache_insert(..)` → writer insert, fresh generation (blocks shape C) |
| `recreate` `:1166` | **optional, recommended:** `self.invalidate(sref);` instead of the fetched insert (blocks shape D at one GET on a lost recreate; ratchet accounting in 4.0) |
| `test_poison_cache` `:996` | unchanged text: goes through `cache_insert` |
| `invalidate` `:1463` | empties the slot under a fresh generation |
| `expire_for_tests` `:1509` | rewinds `at` of a filled slot; no-op on an empty one |
| `mutate_incarnation`, `update`, `cas_update*`, all 20 external callers | untouched (they call `invalidate`) |

### 2.3 The two shapes of the same guard

**(S) Smallest guard, token threading.** Keep `Registry::get`'s body; add
`let seen = self.cache.generation(sref)` inside the lookup block and replace the
final `cache_insert` with `self.cache.insert_if_unmoved(sref.clone(), seen, entry)`;
make `invalidate` write an empty slot. ~25 changed lines in `cache.rs` (the file
`get` lives in after commit 1). Cons: the guard is caller discipline (a future
reader path that inserts must remember the token); `get` keeps a 70-line body
under an `unwrap_used` expectation whose fingerprints will bite the next edit;
the cache is not unit-testable without a store.

**(O) Owner-first: the cache owns the read protocol.** `DescriptorCache::fill(sref, fetch)`
does lookup → `fetch(if_none_match).await` → conditional publish; the
generation never leaves the cache; `Registry::get` becomes ~12 lines with **no
expectation** (its only remaining unwrap is a statement-level `#[cfg(test)]`
failpoint, precedent `src/shard.rs:2868-2880`). The poison decision lives on
one 3-line accessor. The cache is testable with a closure alone. Cost: ~40 more
lines than S, one `impl AsyncFnOnce` parameter (stable since 1.85; toolchain
is 1.98.1; first use in this crate — fallback in section 6).

**Recommendation: O.** The file is being split anyway; the pinned skill favours
canonical ownership over caller discipline; and O leaves `Registry::get`
without any ratcheted exception, which is what makes the *next* change to this
path cheap. Both commits below are written for O; S is a strict subset of the
same diff if the owner prefers it.

### 2.4 Residual: `create`'s own insert (shape E)

Between the PUT being served and `create` inserting its descriptor (`:1103-1111`),
another local writer would need a full GET + PUT + invalidate to land — two
round trips inside one reply's latency. Closing it needs a generation captured
before the PUT, i.e. a new call expression inside `create`, which is
`expect_used`-ratcheted: every `path` and `call-site` fact there is
fingerprinted, so it needs a re-decided reason text. Out of scope; say so in
the commit body. (Replacing the insert with `invalidate` would put one GET on
every create; not worth it for this window.)

---

## 3. Red tests

New file `src/registry/cache/tests.rs` (`#![cfg(test)]`, declared by
`#[cfg(test)] mod tests;` at the foot of `cache.rs`). Not under `src/dst`, so
`docs/refactor/test-inventory.json` is untouched (`scripts/test-inventory.py:138`
walks `src/dst` only). `src/registry/tests.rs` is at 957/1,000 lines and gets
exactly one token: `fn ts` (`:186`) → `pub(super) fn ts` so this module can
reuse it (`desc` at `:193` is `pub(super)` already; both are visible to every
descendant of `registry`).

Deliberately avoided, because each costs a ledger row or an exception:
`tokio::spawn` (disallowed + `effect` row), `tokio::join!`/`select!`/`serde_json::json!`
(`macro-dsl` rows — `tokio::join` is inventoried: `docs/quality/owners.json`
has four rows for it), `use super::*` (`unresolved-glob` row), `Notify`/`Semaphore`
parking (a wait that could hang). `#[async_trait::async_trait]` and
`#[tokio::test]` are already used unregistered in `src/registry/tests.rs`.

The writer runs *inside* the store's reply, so the tests are sequential on the
default current-thread runtime: **no wait exists that could hang**; every
failure is an assertion.

```rust
//! The descriptor cache under a write that lands inside a read's reply: the
//! read answers what the store served it and never publishes it.
#![cfg(test)]
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;

use super::{DescriptorCache, Fetched};
use crate::registry::tests::{desc, ts};
use crate::registry::{
    Mutation, MutationResult, PersistedDescriptor, Registry, SealIntent, SealState, StreamDesc,
};

const EPOCH: &str = "00000000000000000000000000000001";

/// A store whose next GET carries a writer in its reply: the object is read,
/// then `writer` runs to completion against the registry (its own reads and
/// CAS pass straight through), and only then does the read's result reach
/// the caller. This is a read whose reply was slower than a write issued
/// after the store served it — the shape `ConflictOnceStore` in
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
    assert_eq!(raced.ttl_secs, None, "the 304 confirms the entry the read held");
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

/// Shape D (only if the `recreate` hunk in 4.2 ships): the loser reads the
/// live incarnation somebody else installed; a transition of that
/// incarnation lands in its reply. The loser used to publish what it read.
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

/// The cache alone (green from birth; exists to kill the mutants of `settle`
/// and `stamp`): a fetch whose slot moved underneath it answers but does not
/// publish; one whose slot held still publishes; a hit never fetches.
#[tokio::test]
async fn fill_publishes_only_against_the_generation_it_read() {
    let cache = DescriptorCache::new(Duration::from_secs(5));
    let sref = ts("u");
    let old = StreamDesc::try_from(desc("u", EPOCH, false)).unwrap();
    let mut newer = desc("u", EPOCH, false);
    newer.ttl_secs = Some(60);
    let newer = StreamDesc::try_from(newer).unwrap();
    let fetches = std::cell::Cell::new(0);

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
    assert_eq!(answered.map(|d| d.ttl_secs), Some(None), "the raced answer is returned");

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
    assert_eq!((fetches.get(), hit.map(|d| d.ttl_secs)), (2, Some(Some(60))));
}
```

### Expected result on the pre-fix tree

Procedure: on top of **commit 1**, add this file **without the last test**
(`fill_publishes_only_against_the_generation_it_read` names
`DescriptorCache`/`Fetched`, which do not exist yet) and the
`#[cfg(test)] mod tests;` line, plus `pub(super) fn ts`; then
`cargo test --locked --lib registry::cache::tests`. The other tests use only
the `Registry` API of commit 1.

| Test | Pre-fix | Exact panic (the last assertion in each) |
|---|---|---|
| `a_read_that_raced_a_write_never_publishes_the_pre_write_descriptor` | **FAIL** | `a read after the completed seal still served the in-flight claim` |
| `a_renewed_304_never_outlives_the_write_it_raced` | **FAIL** | ``assertion `left == right` failed: a read after the write still served the renewed pre-write entry`` / `left: None` / `right: Some(60)` |
| `a_read_that_raced_a_create_never_publishes_the_absence` | **FAIL** | `a read after the create still served the raced absence` |
| `a_recreate_loser_never_publishes_the_incarnation_it_lost_to` | **FAIL** | ``assertion `left == right` failed: a read after the write still served the loser's raced fill`` / `left: None` / `right: Some(60)` |
| `a_write_to_another_stream_leaves_a_read_free_to_publish` | pass | control |

Summary line: `test result: FAILED. 1 passed; 4 failed`. In each red test the
earlier assertions pass on the old code (the raced read answered the pre-write
value; the writer applied); the last one fails from a cache hit with no store
traffic. After commit 2 (with the unit test added): `6 passed; 0 failed`.

Trace of the first test on the old code: `create` fills the cache;
`invalidate` removes; `get` misses, `WriterInReply::get_opts` serves the
claim-bearing body from `InMemory` (a `Bytes` snapshot), takes the armed
writer and runs `mutate_incarnation` (invalidate → GET → PUT → invalidate)
to completion, returns the snapshot; `get` decodes it and `cache_insert`s it
with `at = now`; the second `get` is a hit at `:1029`.

Existing test that guards against over-blocking:
`ttl_refresh_of_unchanged_descriptor_is_a_free_304` (`src/registry/tests.rs:594`)
— after `create`, an expired entry must still revalidate and a 304 must renew
it (slot unchanged → published); after `update` (now an emptied slot) the next
read fetches a body unconditionally and publishes it, then the following
expire/get pair is a 304 again. Its only assertions are on `.deleted`,
`.stream_epoch` and the counters before the update; all hold.

---

## 4. The code change

### 4.0 Line budget and ratcheted scopes

Binding ceiling for `src/registry.rs`: `min(max(1000, legacy 2774), max(1000, base 1665)) = 1665`
(`scripts/quality/source_rules.py:223-230`; base = `merge-base HEAD origin/slate`,
or the push's `before` SHA in CI, `scripts/quality/common.py:20-29`). Once
commit 1 is on `origin/slate` the ceiling becomes its post-move size, so
commit 2 is planned to *shrink* `registry.rs` under either base.

| File | now | after c1 | after c2 | ceiling |
|---|---|---|---|---|
| `src/registry.rs` | 1,665 | **1,519** (−147 moved, +1 `mod cache;`) | **1,516** (−3), or **1,509** with the `recreate` hunk | 1,665 → 1,519 |
| `src/registry/cache.rs` | — | ~165 | ~250 | 1,000 (new file; `source_rules.py:226` and `architecture-gate.py:121`) |
| `src/registry/cache/tests.rs` | — | — | ~265 | 1,000 |
| `src/registry/tests.rs` | 957 | 957 | 957 (one token) | 1,000 |

Commit 1 removes exactly these blocks, each with its trailing blank line
(verified boundaries): `:952-978` (`cache_insert` + doc + expect, 27),
`:979-986` (`cache_len`, 8), `:987-1005` (`test_poison_cache`, 19),
`:1006-1081` (`get`, 76), `:1459-1466` (`invalidate`, 8), `:1506-1514`
(`expire_for_tests`, 9) = **147**. `struct CachedDesc` (`:861-870`) **stays**
(see trap 2).

Exception ratchet (`source_rules.py:116-216`): identity is
`(path, qualified item, kind, attribute text)`; a moved function is a new
identity and is not compared; under `expect_used`/`unwrap_used` every
`call-site` (function-call syntax), every `path` spelling (import aliases
resolved) and every `unwrap`/`expect` method site in the scope is a
fingerprint that may not gain a count.

| Function | Exception | c1 | c2 |
|---|---|---|---|
| `Registry::create` `:1083` | `expect_used` | untouched | untouched |
| `Registry::recreate` `:1134` | `expect_used` | untouched | untouched, **or** the optional hunk in 4.2 (removals only; accounting there) |
| `Registry::mutate_incarnation` `:1285-1292` | `expect_used` + `excessive_nesting` | untouched | untouched |
| `Registry::get` `:1006` | `unwrap_used` | moves verbatim (new identity, still fulfilled) | expectation **deleted**: its only unwrap is the `#[cfg(test)]` failpoint statement, which `allow-unwrap-in-tests` covers (precedent `ShardEngine::load_billing_meta`, `src/shard.rs:2868-2880`, no expectation on it) |
| `Registry::cache_insert` `:957`, `cache_len` `:979`, `invalidate` `:1459` | `unwrap_used` | move verbatim | expectations **deleted** (they become one-line delegates with no unwrap; keeping one would be `unfulfilled_lint_expectations`, which is denied) |
| `Registry::list_page` `:1517` | `unwrap_used` | untouched | untouched |

**Trap 1 — `Duration` in commit 1.** In `cache.rs` only `expire_for_tests`
uses `Duration`; a plain `use std::time::{Duration, Instant};` is an unused
import in the non-test build. Import it as `#[cfg(test)] use std::time::Duration;`.

**Trap 2 — import aliases are resolved before fingerprinting**
(`tools/quality-syntax/src/imports.rs:46-66`). If `CachedDesc` moved and
`registry.rs` gained `use cache::CachedDesc;`, the `CachedDesc` path inside
`create`/`recreate` would fingerprint as `cache::CachedDesc` — a new key,
"accepted exception grew". So `struct CachedDesc` stays in `registry.rs`
(private; the child module names it as `super::CachedDesc` and reads its
private fields — descendants may). Do not "tidy" it into `cache.rs`.

**Trap 3 — `gen` is a reserved identifier in edition 2024.** Fields and
locals are `generation`. `Slots::next` would trip `should_implement_trait`;
it is `stamp`.

**Trap 4 — `large_enum_variant`.** A `Fetched` *enum* with a `StreamDesc`
variant and a unit `Unchanged` variant differs by ~600 bytes and needs an
expectation (`Mutation`, `:300-303`, carries one for the same reason).
`Fetched` is a struct and a 304 is `Option::None`, so no exception exists to
ratchet later.

### 4.1 Commit 1 — "Move the registry's descriptor cache into registry/cache.rs, verbatim"

A pure move, in preparation for the fix. Behaviour and tokens unchanged
except two visibilities: `fn cache_insert` → `pub(super) fn cache_insert`
(called from `create`/`recreate` in the parent) and `fn expire_for_tests` →
`pub(super) fn expire_for_tests` (called from `registry::tests`, a sibling).
Every other moved item is already `pub(crate)` or `#[cfg(test)] pub(crate)`.
The field `cache`, `cache_ttl`, `fail_next_get`, `store`, `cell` and the
private fns `desc_path`/`decode_desc` are visible to the child module as they
are.

New `src/registry/cache.rs`:

```rust
//! The registry's in-process descriptor cache: the TTL'd, ETag-revalidated
//! read path, the bounded insert its writers share and the invalidation
//! every descriptor write publishes through. Moved verbatim out of
//! registry.rs; its read contract is stated in the commit that follows.
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use super::{CachedDesc, Registry, StreamDesc, decode_desc, desc_path};

impl Registry {
    // :952-977 verbatim, with `fn cache_insert` -> `pub(super) fn cache_insert`
    /// Bounded cache insert: ...
    #[expect(
        clippy::unwrap_used,
        reason = "Registry::cache_insert; a poisoned descriptor cache may hold a partially inserted or invalidated descriptor; recovering it could serve a stale incarnation as current"
    )]
    pub(super) fn cache_insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
        ...
    }

    // :979-985 verbatim
    pub(crate) fn cache_len(&self) -> usize { ... }

    // :987-1004 verbatim
    #[cfg(test)]
    pub(crate) fn test_poison_cache(...) { ... }

    // :1006-1080 verbatim
    pub(crate) async fn get(...) -> Result<Option<StreamDesc>, object_store::Error> { ... }

    // :1459-1465 verbatim
    pub(crate) fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) { ... }

    // :1506-1513 verbatim, with `fn expire_for_tests` -> `pub(super) fn expire_for_tests`
    #[cfg(test)]
    pub(super) fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) { ... }
}
```

`get` calls `self.store.get_opts(..)` on `Arc<dyn ObjectStore>`; a trait
object's own methods need no trait import. If the compiler disagrees, add
`use object_store::ObjectStore;`.

`src/registry.rs`: delete the six blocks above; add one line at `:1661`:

```rust
mod cache;
mod catalog;
```

Verify verbatim from the scratchpad:

```sh
S=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad
git show --format= HEAD -- src/registry.rs | grep '^-' | grep -v '^---' | sed 's/^-//' | sort > $S/removed
git show --format= HEAD -- src/registry/cache.rs | grep '^+' | grep -v '^+++' | sed 's/^+//' | sort > $S/added
comm -23 $S/removed $S/added
# expected output: exactly the two lines whose visibility changed
#     fn cache_insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
#     fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) {
git show --format= HEAD -- src/registry.rs | grep '^+' | grep -v '^+++'
# expected: +mod cache;
```

Gate for commit 1: `cargo fmt --all -- --check`,
`cargo clippy --locked --workspace --all-targets -- -D warnings`,
`cargo test --locked --lib registry::` (all existing tests unchanged),
`wc -l src/registry.rs` = 1519, `scripts/quality.sh`, then `scripts/gate.sh`.

Commit message body (house style, cf. `d902a85b`): a pure move; which two
tokens changed and why; `registry.rs` 1,665 → 1,519; "the ledgers move with
the code: nothing — no ledger row is keyed on the moved items (section 5)".

### 4.2 Commit 2 — "The descriptor cache never publishes a read that raced a write"

`src/registry/cache.rs`, complete:

```rust
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
//! that goes to the store first captures the generation of the stream's
//! slot, and its answer is published only if the slot still carries that
//! generation when the read returns. An invalidation never removes a slot;
//! it empties the slot under a fresh generation. A read that fetched the
//! pre-write descriptor and returns after the write therefore finds the
//! generation moved and keeps its answer to itself. That one caller still
//! gets the answer: the store served it, so it is a valid read that
//! happened before the write; only its reuse would be wrong.
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use object_store::path::Path as ObjPath;

use super::{CachedDesc, Registry, StreamDesc, decode_desc, desc_path};

/// The descriptor cache previously grew with every distinct name ever
/// touched (static-audit memory finding — creates alone put 100k entries
/// in it). At the cap, emptied and expired slots purge first (TTL is
/// seconds, so this is almost always enough), then the oldest entry
/// falls out.
const REGISTRY_CACHE_MAX: usize = 65_536;

pub(super) struct DescriptorCache {
    slots: Mutex<Slots>,
}

struct Slots {
    by_ref: HashMap<crate::tenant::TenantStreamRef, Slot>,
    ttl: Duration,
    /// The generation handed out last. Every fill and every invalidation
    /// takes the next one, so no two slot states — of any stream, at any
    /// time — share a generation.
    last_generation: u64,
}

/// One stream's slot. `entry` is `None` after an invalidation: the slot
/// survives to carry the generation a read in flight compares against.
struct Slot {
    generation: u64,
    entry: Option<CachedDesc>,
}

/// What a read found in the cache.
enum Lookup {
    /// Served from the cache; no store traffic.
    Fresh(Option<StreamDesc>),
    /// Go to the store. `seen` is the slot generation to publish against
    /// (`None`: no slot); `held` is the expired entry's ETag and
    /// descriptor for a conditional refresh.
    Fetch {
        seen: Option<u64>,
        held: Option<(String, Option<StreamDesc>)>,
    },
}

/// A body the store served, or its confirmed absence (`desc: None`), with
/// the object's ETag. A fetch answers `None` instead of one of these when
/// the store said 304: the entry the read held is current.
pub(super) struct Fetched {
    desc: Option<StreamDesc>,
    etag: Option<String>,
}

impl Slots {
    fn stamp(&mut self) -> u64 {
        self.last_generation += 1;
        self.last_generation
    }

    fn generation(&self, sref: &crate::tenant::TenantStreamRef) -> Option<u64> {
        self.by_ref.get(sref).map(|slot| slot.generation)
    }

    /// Bounded: at the cap, emptied and expired slots purge first, then
    /// the oldest entry falls out. Either way the slot takes a fresh
    /// generation.
    fn put(&mut self, sref: crate::tenant::TenantStreamRef, entry: Option<CachedDesc>) {
        if self.by_ref.len() >= REGISTRY_CACHE_MAX && !self.by_ref.contains_key(&sref) {
            let ttl = self.ttl;
            self.by_ref
                .retain(|_, slot| slot.entry.as_ref().is_some_and(|e| e.at.elapsed() < ttl));
            if self.by_ref.len() >= REGISTRY_CACHE_MAX
                && let Some(oldest) = self
                    .by_ref
                    .iter()
                    .min_by_key(|(_, slot)| slot.entry.as_ref().map(|e| e.at))
                    .map(|(n, _)| n.clone())
            {
                self.by_ref.remove(&oldest);
            }
        }
        let generation = self.stamp();
        self.by_ref.insert(sref, Slot { generation, entry });
    }
}

impl DescriptorCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            slots: Mutex::new(Slots {
                by_ref: HashMap::new(),
                ttl,
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

    /// A writer's fill — what `create` wrote — published under a fresh
    /// generation, unconditionally.
    fn insert(&self, sref: crate::tenant::TenantStreamRef, entry: CachedDesc) {
        self.slots().put(sref, Some(entry));
    }

    /// A write's publication: the slot is emptied under a fresh
    /// generation, never removed, so a read in flight sees the move.
    fn invalidate(&self, sref: &crate::tenant::TenantStreamRef) {
        self.slots().put(sref.clone(), None);
    }

    fn lookup(&self, sref: &crate::tenant::TenantStreamRef) -> Lookup {
        let slots = self.slots();
        match slots.by_ref.get(sref) {
            Some(Slot { entry: Some(e), .. }) if e.at.elapsed() < slots.ttl => {
                Lookup::Fresh(e.desc.clone())
            }
            Some(Slot { generation, entry }) => Lookup::Fetch {
                seen: Some(*generation),
                held: entry
                    .as_ref()
                    .and_then(|e| e.etag.clone().map(|t| (t, e.desc.clone()))),
            },
            None => Lookup::Fetch {
                seen: None,
                held: None,
            },
        }
    }

    /// Publish a read's answer only if the slot is exactly as the read
    /// found it. A moved generation means a write landed after the store
    /// served this read; the answer must not outlive that write.
    fn settle(
        &self,
        sref: &crate::tenant::TenantStreamRef,
        seen: Option<u64>,
        entry: CachedDesc,
    ) {
        let mut slots = self.slots();
        if slots.generation(sref) == seen {
            slots.put(sref.clone(), Some(entry));
        }
    }

    /// Serve `sref` from the cache, or `fetch` it — handing over the ETag
    /// to revalidate against, if any — and publish the answer under the
    /// contract above. The answer is returned to the caller either way.
    pub(super) async fn fill<E>(
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

    /// Force a cached entry past its TTL so tests can exercise the
    /// refresh path without sleeping through the real TTL.
    #[cfg(test)]
    fn expire_for_tests(&self, sref: &crate::tenant::TenantStreamRef) {
        let mut slots = self.slots();
        let ttl = slots.ttl;
        if let Some(Slot { entry: Some(e), .. }) = slots.by_ref.get_mut(sref) {
            e.at -= ttl + Duration::from_secs(1);
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
        let path = desc_path(&self.cell, sref);
        self.cache
            .fill(sref, async |if_none_match| {
                self.fetch_descriptor(&path, sref, if_none_match).await
            })
            .await
    }

    /// One GET of the descriptor object — conditional when the cache
    /// holds an ETag — decoded fail-closed. `None` is the store's 304.
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
                        source: format!("descriptor for {sref}: {e}").into(),
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
```

Notes on the contract text: one reasoned expectation in the file
(`DescriptorCache::slots`), three `;`-separated parts, no `;` or `"` inside a
part (`source_rules.py:258`). This is not a wrapper to satisfy a lint: it
narrows the poison decision from five copies of one reason to one accessor,
and it keeps `fill`/`insert`/`invalidate`/`settle` free of any fingerprinted
scope, so the next edit here is not an "exception grew" event. If the reviewer
prefers the file's previous style, put the same reason on `len`, `insert`,
`invalidate`, `lookup`, `settle` and `expire_for_tests` instead (six
expectations, each fulfilled by its own `.lock().unwrap()`), and drop `slots()`.

`get` keeps the `#[cfg(test)] if self.fail_next_get.lock().unwrap()..`
statement with no expectation (precedent `src/shard.rs:2868-2880`). If clippy
does flag it, replace the statement with `if self.take_fail_next_get(sref)` and
add beside `get`:

```rust
    #[cfg(test)]
    fn take_fail_next_get(&self, sref: &crate::tenant::TenantStreamRef) -> bool {
        self.fail_next_get
            .lock()
            .unwrap()
            .remove(sref.name().as_str())
    }
```

(function-level `#[cfg(test)]` + unwrap, no expectation — the shape of
`take_fail_next_put`, `:1499-1504`).

`src/registry.rs` hunks (net −3 lines; −10 with the optional hunk):

```rust
// :4  (HashMap is used nowhere else in the file; no child reaches it through `use super::*` — checked)
-use std::collections::HashMap;

// :844-845
-    cache: Mutex<HashMap<crate::tenant::TenantStreamRef, CachedDesc>>,
-    cache_ttl: Duration,
+    cache: cache::DescriptorCache,

// :942-943  (Registry::new carries no expectation; not ratcheted)
-            cache: Mutex::new(HashMap::new()),
-            cache_ttl: Duration::from_secs(5),
+            cache: cache::DescriptorCache::new(Duration::from_secs(5)),

// :1166-1173  OPTIONAL (shape D), inside `recreate` (expect_used-ratcheted):
-                self.cache_insert(
-                    sref.clone(),
-                    CachedDesc {
-                        desc: Some(current.clone()),
-                        at: Instant::now(),
-                        etag: etag.clone(),
-                    },
-                );
+                self.invalidate(sref);
                 return Ok((false, current));
```

`Duration` stays imported (`Registry::new`; `catalog.rs:170` also reaches it
through `use super::*`). `Instant` stays (`create` `:1108`, and `recreate`
unless the hunk ships). `Mutex` stays (`fail_next_*`).

Ratchet accounting for the optional `recreate` hunk (every metric must be
≤ its merge-base value): `scope_lines` −7; `nested_items` 0; `syntax_facts`
−~14; `expect_sites` 1 → 1; `expect_site:ordinary-call` — `Instant::now()` and
`Some(..)` call-sites removed, none added; `expect_site:path` — `sref` count
unchanged (one `sref.clone()` receiver out, one `sref` argument in), `self`
unchanged (one receiver out, one in), `CachedDesc`, `Instant::now` removed,
`Some`/`current`/`etag` decremented. Method-call sites are fingerprinted only
when named `unwrap`/`expect`, so `invalidate` gaining a second call is not a
key. **Run `scripts/quality.sh` before committing; if it prints
`accepted exception grew ... Registry::recreate`, drop the hunk and its test
and record shape D as a follow-up** (the sanctioned route is re-deciding
`recreate`'s reason text, which is the owner's decision).

Also in commit 2: `src/registry/tests.rs:186` `fn ts` → `pub(super) fn ts`;
new `src/registry/cache/tests.rs` (section 3).

Commit message body: the interleaving from 1.1; the four shapes; the four red
messages from section 3 verbatim, "observed from a cache hit with no store
traffic"; why per-slot and not cache-wide (2.1); what `get` returns and why;
the `recreate` change and its one-GET cost (if it ships); the residual shape E
and that it is out of scope; `registry.rs` 1,519 → 1,516 (or 1,509).

### 4.3 Order of work

1. Commit 1; gate; `wc -l`; the verbatim check.
2. Add `cache/tests.rs` without the unit test, `mod tests;`, `pub(super) fn ts`;
   run `cargo test --locked --lib registry::cache::tests`; record the four red
   panics (section 3) in the commit message.
3. Apply the fix and the unit test; run the same command → 6 passed; run
   `cargo test --locked --lib registry::`; run the DST legs that plant or
   invalidate descriptors: `dst_tests::append_application`, `dst_tests::reads_raw`,
   `dst_tests::topology_scaling`, `dst_tests::fork_lifecycle`,
   `dst_tests::consumer_saga`, `dst_tests::seal_convergence`,
   `dst_tests::quota_enforcement`, `dst_tests::lifecycle_incarnation`,
   `dst_tests::lifecycle_creation`, `dst_tests::livefeed_swap`; then
   `scripts/quality.sh` and `scripts/gate.sh`.
4. Before pushing, CI's own plan against the push base:
   `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py`
   — expect `"mutants": false` and `"unregistered_mutation_source_files": []`.
   Claim CI green only from `gh run view`.

---

## 5. Ledger and doc rows

| Ledger | Finding | Action |
|---|---|---|
| `docs/quality/owners.json` | Five registry rows, all for files that do not move: `src/registry/tests.rs` (json macro + glob), `resolution_tests.rs` (glob), `catalog.rs` (glob), `catalog/tests.rs` (proptest). None keyed on the moved items. | **None**, provided `cache.rs` and `cache/tests.rs` keep explicit imports and use no DSL macro, `tokio::join!`, `select!` or spawn. If a reviewer adds `use super::*;`, add `{"category":"unresolved-glob","count":1,"owner":"crate","path":"src/registry/cache.rs","reason":"Registry cache imports its enclosing registry owner; the compiler resolves the parent exports and the cache only fills and invalidates descriptors.","syntax":"compiler-resolved import; syntax cannot infer exports"}` (the `catalog.rs` row is the template). |
| `docs/quality/source-allowances.json` | 0 rows under `src/registry*` (grepped); reasoned `#[expect]`s are reviewed in-source (`source_rules.py:261-264`). | Run `python3 scripts/quality/gate.py --prune` after each commit; expect "0 obsolete". Commit the file only if it changed. |
| `scripts/quality/mutation_owners.py` | `src/registry.rs` has no row and `src/registry` is under none of `CRITICAL_PREFIXES` (`verification_plan.py:22-31`); a new file there is neither selected nor "unregistered". | **None required.** Optional: `owner('registry_cache', 'src/registry/cache.rs', 'registry::cache::')` makes the cache a mutation owner (the six tests kill `settle`'s comparison, `stamp`, `invalidate`'s put and `lookup`'s TTL guard). If added, run `python3 scripts/quality/test_mutation_owners.py` and be ready to disposition survivors in the 65,536-entry eviction arm, which has no test. Recommendation: not in this change. |
| `docs/refactor/architecture-policy.json` | `file:src/registry.rs` limit 2774 (non-binding); new files default to 1,000 lines / 200 per function; no `crate::http`/`crate::product` edges from either new file. | None. |
| `docs/refactor/test-inventory.json`, `test-scenario-map.json` | `src/dst` only. | None. |
| `docs/refactor/review-mechanisms.json` | No registry source or test pinned (grepped). | None. |
| `scripts/mt-audit-baseline.txt` | Scans `src/*.rs` (not `src/registry/`) for `stream_hash(`, bare-name registry calls and `HashMap<String` in `registry.rs`; none of the moved lines match. `HashMap<TenantStreamRef, Slot>` is not name-keyed (`src/mt_lint.rs:83-93`); `cache/tests.rs` is skipped by its `#![cfg(test)]` (`mt_lint.rs:392-398`). | None. `bash scripts/multitenancy-audit.sh` must print `MT_AUDIT_OK`. |
| `docs/refactor/WIRE-MATRIX.md`, contract docs | No wire change. `docs/COST-AB1.md:25` and `docs/ROUTING-V3.md:88` describe ETag revalidation, which is unchanged. No doc states "a read may re-insert over an invalidate". | None. The cache contract is stated in `cache.rs`'s module doc, as `2505b987` put it in code. |
| `docs/quality/policy.json`, `legacy-*.json`, `syntax-fragments.json` | immutable adoption baselines. | Never touched. |

---

## 6. What could go wrong

- **Wire compatibility, persisted formats, fleet skew: none.** In-process
  memory only. No descriptor field, object key, header, status or token
  changes. Older binaries keep the local bug and share no state with fixed
  ones. Cross-instance staleness (another cell instance's write is invisible
  for up to the 5 s TTL) is unchanged and remains the contract that `668bc80c`
  makes the append path tolerate; this fix removes *same-process* staleness
  after a local write.
- **Conformance suite (DS protocol, 332 cases):** sees strictly fewer stale
  answers; no case asserts registry GET counts. Run it anyway.
- **Cost:** an extra GET only when a read overlapped a write to the *same*
  stream (the alternative was serving it stale), or on a lost `recreate` (if
  the hunk ships). The 304 path and hit path are unchanged. The control test
  pins that unrelated writes cost nothing.
- **`cache_len` counts emptied slots.** `/debug`'s `registry_cache`
  cardinality (`src/http.rs:971`) may read higher on a write-heavy cell than
  before, bounded by the same 65,536 cap. Say so in the commit body; the
  campaign docs quoting old numbers are historical.
- **Compile risks (nothing was built):**
  1. `impl AsyncFnOnce(Option<String>) -> Result<Option<Fetched>, E>` in
     argument position with a lending closure, first use in the crate.
     Fallback: `fill<E, F, Fut>(.., fetch: F) where F: FnOnce(Option<String>) -> Fut, Fut: Future<Output = Result<Option<Fetched>, E>>`
     and in `get`: `.fill(sref, |if_none_match| self.fetch_descriptor(&path, sref, if_none_match))`.
  2. `Registry::get`'s future must stay `Send` (axum handlers): the fetch
     future holds `&Registry` (Sync), `&ObjPath`, `&TenantStreamRef`; no
     guard crosses an await (`lookup`/`settle` return before/after it).
  3. The unit test's `async |..|` closures capture `&cache` while
     `cache.fill(&self, ..)` also borrows it — both shared. `fetches` is a
     `Cell`, not a `&mut`.
  4. `crate::registry::tests::{desc, ts}` from `registry::cache::tests`: both
     `pub(super)` in `registry::tests`, i.e. visible in `registry` and its
     descendants. If the compiler disagrees, spell them `super::super::tests::desc`.
  5. `WriterInReply` needs manual `Debug` (a `BoxFuture` is not `Debug`);
     `ObjectStore` requires `Debug + Display`. The `Mutex<Option<BoxFuture>>`
     take happens before the await (comment on it, as `ConflictOnceStore:532-534`).
  6. `if let .. = slots.by_ref.get_mut(sref)` in `expire_for_tests` copies
     `slots.ttl` out first (mutable borrow of `by_ref` vs read of `ttl`).
  7. `matches!(outcome, MutationResult::Applied(()))` needs `MutationResult: Debug`
     for the message — it derives it (`:314`).
- **Reviewer "fixes" that break the gate:** moving `CachedDesc` into
  `cache.rs` and importing it in `registry.rs` (trap 2, `create`/`recreate`
  grow a path fingerprint); keeping `#[expect(clippy::unwrap_used)]` on
  `get`/`cache_len`/`invalidate` "for symmetry" (`unfulfilled_lint_expectations`
  is denied); a fourth `;` or a `"` inside a reason; `use super::*` in either
  new file (owners row); `tokio::join!` in a test (owners row); naming a field
  `gen` (edition-2024 keyword).
- **Poison policy unchanged:** the one lock site still `.unwrap()`s under a
  reasoned expectation; no recovery.
- **Not covered, deliberately:** shape E (2.4); and two *reads* racing each
  other with a *remote* write in between can still leave the older read's
  value published last — that is inside the 5 s cross-instance contract, and
  making reads bump the generation would make concurrent reads of one stream
  evict each other.

---

## Skeptic corrections

Checked read-only against the tree on 2026-09-22. **HEAD moved during this
pass**: it is now `efe12b2e` ("An idle window never wraps a stream's expiry
into the past", also `origin/slate`), one commit past the plan's `00ff0e7e`.
`git diff --quiet 00ff0e7e HEAD -- src/registry.rs src/registry/` is clean and
the worktree has no registry changes, so every `src/registry.rs` line number in
the plan is still exact (1,665 lines; `tests.rs` 957).

### What checks out (so nobody re-litigates it)

- **Removed blocks and counts.** `:952-978` (27), `:979-986` (8), `:987-1005`
  (19), `:1006-1081` (76), `:1459-1466` (8), `:1506-1514` (9) = 147, each
  ending on its trailing blank, leaving exactly one blank between neighbours.
  1,665 − 147 + 1 (`mod cache;`) = 1,519. Ceiling today is
  `min(max(1000, legacy 2774), max(1000, merge-base 1665)) = 1665`
  (`scripts/quality/source_gate.py:32-43`, `source_rules.py:224-228`;
  `docs/quality/legacy-source.json` lines for `src/registry.rs` = 2774;
  `policy.json` has no `adoption_line_additions` row for it).
- **Ratchet semantics.** `exception_growth` (`source_rules.py:203-215`) skips
  any identity absent from the merge base, and identity is
  `(path, qualified, kind, attribute text)`, so the moved expectations are new
  identities. Under `expect_used`, only `call-site` facts (function-call
  syntax, `tools/quality-syntax/src/scan.rs:242-255`) and `path` facts are
  fingerprinted; `method-call-site` facts are fingerprinted only when the
  method is `expect`/`expect_err`. `self.invalidate(sref)` is a method call,
  so the optional `recreate` hunk's accounting in 4.0 is right: `syntax_facts`
  falls, `self`/`sref` path counts are unchanged, nothing grows.
- **No `#![expect]`, impl-level or module-level exception in
  `src/registry.rs`** (all 17 `#[expect(` sites are item-scoped: `:56` struct,
  `:300` enum, the rest on fns), so editing `Registry`'s fields and
  `Registry::new` in commit 2 touches no ratcheted scope.
- **The cfg(test) unwrap without an expectation** is real precedent:
  `src/shard.rs:2867-2880` `load_billing_meta` has
  `#[cfg(test)] if billing_read_faults().lock().unwrap()...` and no covering
  expectation at any level (`grep '#!\[' src/shard.rs` is empty); clippy.toml
  has `allow-unwrap-in-tests = true`.
- **Ledgers.** `scripts/quality/mutation_owners.py` has no `src/registry`
  row (only `quota_registry` and `sse_registry`); `CRITICAL_PREFIXES`
  (`verification_plan.py:22-31`) do not cover `src/registry`;
  `docs/quality/source-allowances.json` has zero `src/registry` rows (its one
  "registry" hit is `src/sse/registry.rs`); `owners.json` registry rows are
  the four the plan lists plus `catalog.rs`'s glob, none keyed on moved items;
  `scripts/test-inventory.py:138` walks `src/dst` only;
  `review-mechanisms.json`'s two "registry" hits are prose. `classify()`
  (`source_rules.py:40-63`) does not inventory `macro-attribute` facts unless
  they are `allow`/`expect`/`path`, so `#[async_trait::async_trait]` and
  `#[tokio::test]` need no row; `matches`/`panic`/`assert*`/`format` are in
  `EXPRESSION_MACROS`.
- **mt-lint / audit.** `mt_lint.rs:392-398` skips a file whose inner attrs
  contain `cfg`+`test`, so `cache/tests.rs` is out of scope; `NAME_KEYED`
  (`:83-93`) matches only `String`/`Arc<str>` keys; `NAME_PARAMS` (`:95`) are
  `name`/`stream_name`/`canonical_name`/`stream` — none of the new fns use
  them (`if_none_match` is fine). `scripts/multitenancy-audit.sh` scans
  `src/*.rs`, `src/config`, `src/dst` and (one pattern) `src/registry.rs`; no
  moved line matches `stream_hash(`, `HashMap<String` or a bare-name
  registry signature, and `src/registry/cache*.rs` is scanned by nothing.
- **The `WriterInReply` staging is sound.** `FaultStore::get_opts`
  (`src/dst/fault_store.rs:415-430`) gates *before* `inner.get_opts`, so it
  cannot stage this window. `object_store 0.14.1`: `ObjectStoreExt::get` is
  `self.get_opts(location, GetOptions::default())` (`lib.rs:1516-1518`), so
  `recreate`/`mutate_incarnation`'s `self.store.get(&path)` does pass through
  the decorator; `InMemory::get_opts` (`memory.rs:239-262`) clones the
  entry's `Bytes` at call time (a snapshot) and runs `check_preconditions`
  (so the 304 in shape B happens); `put_opts` returns `e_tag: Some(..)`.
  The trait method set the decorator must implement is exactly
  `ConflictOnceStore`'s (`src/registry/tests.rs:521-589`), which compiles
  today.
- **APIs the tests name exist with the stated shapes.** `SealState`
  `{operation_id, intent, claimed_ms, claim_generation: u64}` (`:196-223`),
  `SealIntent::Empty` (`:229-232`), `Mutation::Write(PersistedDescriptor, T)`
  (`:304-310`), `MutationResult` derives `Debug` (`:314`), `MutationError`
  derives `Debug` (`:808`), `StreamDesc::to_persisted` (`:451`),
  `StreamDesc: Deref<Target = PersistedDescriptor>` (`:406`) so `.sealing`,
  `.sealed`, `.ttl_secs: Option<u64>` (`:124,140,148`) resolve,
  `desc(name, epoch, deleted) -> PersistedDescriptor` is `pub(super)`
  (`tests.rs:193`), `ts` is private at `tests.rs:186`,
  `CellId::new(&str) -> Result` (used with `.unwrap()` at `tests.rs:41`).
  `validate_descriptor` (`:480-576`) accepts test A's claim
  (`claim_generation 0 <= seal_gen_counter 0`, `sealed == false`, no
  segments) and the writer's `sealed = true, sealing = None`.
- **Import hygiene claims.** `HashMap` appears in `src/registry.rs` only at
  `:4,844,942`; no file under `src/registry/` names `HashMap`, `Instant` or
  `Duration` unqualified except `catalog.rs:170` (`Duration`, kept). `cache_ttl`
  is read only at `:965,1029,1511` — all moved. `cache_len` has one caller
  (`src/http.rs:971`); `test_poison_cache` has four DST callers, all through
  the unchanged signature; `expire_for_tests` is called only from
  `registry/tests.rs:629,660,663`. No DST test asserts a descriptor-class GET
  count (`store.count(StoreOp::Get, ..)` is used only for `Wal`/`Manifest`).
- **The four red traces** (shapes A-D) and the control hold on the current
  code as written; the summary line `1 passed; 4 failed` and the exact panic
  texts are right for Rust 1.98 (`assert_eq!` prints
  ``assertion `left == right` failed: <msg>`` then `left:`/`right:`).

### Correction 1 (correctness): `seen: None` is an ABA hole at the cap

Section 2.2 says "Generations are unique across all streams and all time, so
there is no ABA even after a slot is evicted and re-created." That holds for
`Some(g)`. It does **not** hold for `seen = None`, and the plan's own `put`
makes the hole reachable in exactly the regime the cap exists for:

```
cache at REGISTRY_CACHE_MAX; stream X has no slot
R  lookup(X)            -> seen = None; GET in flight (8-185 ms)
W  invalidate(X)        -> put(X, None): X absent + at cap -> purge, then slot X (g1, None)
W  GET, PUT, invalidate -> slot X (g2, None)
C  create(Y) / any new-key put at cap
                        -> retain(|slot| slot.entry.is_some_and(fresh))  <- drops X (empty)
R  settle(X, None, D0)  -> generation(X) == None == seen -> PUBLISHES D0, stale for a TTL
```

Under a create storm at the cap (the memory audit's 100k creates) step C is
*every* create, so shapes A-after-a-purge and C are not closed there. Two
changes close it exactly, and together they avoid the starvation that the
first alone would introduce (a claim purged by every create at the cap would
make every cold read of that stream a billable GET for the storm's duration):

**(a) A cold miss claims an empty slot before fetching, so `seen` is always a
generation.** `Lookup::Fetch { seen: u64, .. }`, `settle(.., seen: u64, ..)`,
`Slots::put` returns the generation it stamped:

```rust
    fn lookup(&self, sref: &crate::tenant::TenantStreamRef) -> Lookup {
        let mut slots = self.slots();
        let ttl = slots.ttl;
        match slots.by_ref.get(sref) {
            Some(Slot { entry: Some(e), .. }) if e.at.elapsed() < ttl => {
                Lookup::Fresh(e.desc.clone())
            }
            Some(Slot { generation, entry, .. }) => Lookup::Fetch {
                seen: *generation,
                held: entry
                    .as_ref()
                    .and_then(|e| e.etag.clone().map(|t| (t, e.desc.clone()))),
            },
            // No slot: claim one, empty, so that a write or an eviction
            // between now and the publish moves the generation this read
            // compares against. Absence is never a generation.
            None => Lookup::Fetch {
                seen: slots.put(sref.clone(), None),
                held: None,
            },
        }
    }

    fn settle(&self, sref: &crate::tenant::TenantStreamRef, seen: u64, entry: CachedDesc) {
        let mut slots = self.slots();
        if slots.generation(sref) == Some(seen) {
            slots.put(sref.clone(), Some(entry));
        }
    }
```

(If NLL rejects the mutable `put` in the `None` arm of a `match` over
`slots.by_ref.get(sref)` — it should not, no reference escapes the arms —
split it: `if let Some(slot) = slots.by_ref.get(sref) { return ..; }` then
`Lookup::Fetch { seen: slots.put(sref.clone(), None), held: None }`.)

**(b) The cap purge keeps every slot touched within a TTL, empty or not.**
`Slot` gains `touched: Instant` (stamped by every `put`); `retain` and the
oldest-eviction key on it. A filled slot untouched for a TTL is expired
anyway (today's rule); an empty slot untouched for a TTL is a stale
invalidation or an abandoned claim (a deleted stream's, for example) and may
go; a read still in flight after a whole TTL merely skips its publish, which
is the safe side. `CachedDesc.at` stays the freshness clock (it must stay:
`create`/`recreate`/`test_poison_cache` build the literal, and `CachedDesc`
stays in `registry.rs` per trap 2). The cap becomes a field so the unit test
below can reach it without 65,536 inserts:

```rust
struct Slots {
    by_ref: HashMap<crate::tenant::TenantStreamRef, Slot>,
    ttl: Duration,
    /// Slots held at most; a new key beyond it runs the purge in `put`.
    cap: usize,
    last_generation: u64,
}

/// One stream's slot. `entry` is `None` after an invalidation or under a
/// read's claim: the slot survives to carry the generation a read in
/// flight compares against.
struct Slot {
    generation: u64,
    /// When the slot last changed hands: a fill, an invalidation or a
    /// read's claim. The cap purges only slots untouched for a TTL — a
    /// filled one is expired by then, an empty one is a stale
    /// invalidation or an abandoned claim — so a read in flight always
    /// finds the slot it claimed, or the write that moved it.
    touched: Instant,
    entry: Option<CachedDesc>,
}

impl Slots {
    /// Bounded: at the cap, slots untouched for a TTL purge first, then
    /// the least recently touched falls out. Either way the slot takes a
    /// fresh generation, which is returned.
    fn put(&mut self, sref: crate::tenant::TenantStreamRef, entry: Option<CachedDesc>) -> u64 {
        if self.by_ref.len() >= self.cap && !self.by_ref.contains_key(&sref) {
            let ttl = self.ttl;
            self.by_ref.retain(|_, slot| slot.touched.elapsed() < ttl);
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
    // insert / invalidate / expire_for_tests unchanged; they ignore put's return.
}
```

Nesting in `put` is impl → fn → if → if-let, the same depth as today's
`cache_insert` (`:961-977`), which passes `excessive-nesting-threshold = 4`.

**Red test for (a)+(b)**, in `cache/tests.rs` (green from birth is not enough
for a behaviour change; this one is red against the plan's `put`/`lookup`
and green with the two changes above). Needs
`use crate::registry::CachedDesc;` (private to `registry`; a descendant may
name it), `use std::cell::Cell;`, `use std::time::Instant;`:

```rust
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
    assert_eq!(answered.map(|d| d.ttl_secs), Some(None), "the raced answer is returned");

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
    assert!(fetched.get(), "the raced answer was published over the purge");
}
```

Against the plan's version: `seen = None`; `invalidate` creates `(g, None)`;
`insert(newcomer)` at the cap runs `retain(entry.is_some_and(..))` and drops
the empty `raced` slot; `settle` finds `None == None` and publishes `old`;
the second `fill` is a hit and never calls the closure → panics with
`the raced answer was published over the purge`. With (a)+(b): the claim
`(g1, None, touched now)` is evicted only if it is the least recently touched
of two young slots — it is not (`filler-1` is) — `invalidate` moves it to
`g2`, the newcomer's purge keeps both young slots and evicts `filler-2`,
`settle` sees `Some(g2) != Some(g1)` and skips, the second `fill` fetches.

Section 2.2's sentence "Absent → absent publishes; absent → any slot, or any
moved generation, skips" and the module doc's "captures the generation of
the stream's slot" become: "a read that goes to the store first claims the
stream's slot — empty, under a fresh generation — if there is none, and
publishes only if the slot still carries the generation it read." Section
2.2's "at 65,536 slots the purge `retain`s live unexpired entries (empty
slots are the cheapest to lose ...)" is withdrawn: empty slots younger than a
TTL are exactly the ones that must *not* be lost.

### Correction 2 (consistency): reads do bump the generation

Section 6's last bullet says "making reads bump the generation would make
concurrent reads of one stream evict each other", but `settle` publishes
through `put`, which stamps a fresh generation, so a read's publish *does*
move the generation. Consequence: of two reads of one stream that both
missed, the first to return publishes and the second skips (it already paid
its GET; nothing is refetched; the value it returns is still served to its
caller). That is fine — keep `put` in `settle` — but the commit body and the
module doc must say it, and the "two reads racing with a remote write in
between can leave the older read's value published last" sentence is then
false in the other direction: the *first* read back wins, whichever is
older. Rewrite that bullet: "Two reads of one stream that both miss: the
first to return publishes, the second skips its publish and returns its own
answer. A remote write between their fetches is still inside the 5 s
cross-instance contract."

### Correction 3 (red-run procedure): the test file does not compile on commit 1 as written

Section 3's procedure says to add `cache/tests.rs` "without the last test".
The file's header also has `use super::{DescriptorCache, Fetched};` — an
unresolved import on commit 1, which is a hard error, not a warning. For the
red run, also drop that line, `use std::time::Duration;` and `StreamDesc`
from the `crate::registry::{..}` import (the remaining unused-import
warnings are harmless under plain `cargo test`; only clippy runs
`-D warnings`). Restore all three with the fix. Section 4.3 step 2 should say
so, or the recorded "1 passed; 4 failed" is unreachable.

### Correction 4 (visibility): fewer `pub(super)` than the plan grants

`Registry::get` lives in `cache.rs`, so `DescriptorCache::fill` and `Fetched`
need no `pub(super)`; only `DescriptorCache` and `DescriptorCache::new` are
named from `registry.rs`. `cache::tests` is a child and sees private items
(the plan already relies on that for `Fetched`'s private fields). Private is
the smaller surface; `unreachable_pub` is indifferent either way.

### Notes (no code change required)

- **`fn slots()` and "never introduce a wrapper solely to satisfy a lint"
  (RUST-QUALITY.md:55).** Tooling accepts it (one reasoned, three-part,
  item-scoped `#[expect]`, fulfilled by its own `.unwrap()`). Whether it
  reads as consolidation of the poison decision or as a lint wrapper is the
  reviewer's call; the plan's fallback (the reason on each locking fn, no
  accessor) is the same diff minus one fn and passes the same gates.
- **`DescriptorCache::len`.** `clippy::len_without_is_empty` fires only on
  exported items (`is_exported`); a `pub(super)` type's private `len` is
  safe. If a reviewer objects, `slots_held` costs nothing.
- **`Registry::get`'s future stays `Send`** under the corrected `lookup`:
  the guard is still local to `lookup`/`settle`; `fill` awaits with only
  `seen: u64`, `held`, and the closure's future (`&Registry`, `&ObjPath`,
  `&TenantStreamRef`, the store's boxed `Send` future) live.
- **The placeholder at the cap moves today's purge from after the fetch to
  before it** for a cold miss; the cost per cold read is unchanged
  (one `put` at the cap either way). A read that errors leaves its claim as
  an empty slot; the next read of that stream refetches, as today.
- **Commit 1's `dyn ObjectStore` method calls need no trait import**
  (trait-object method resolution includes the principal trait's methods);
  the plan's fallback line is correct if rustc disagrees, and an unneeded
  import would fail `-D warnings` as `unused_imports`, so do not add it
  pre-emptively.
- **Ledger rows for the correction:** none. `cap`, `touched`, `bounded` and
  the new test add no macro DSL, glob, spawn, static or `std::env` read;
  `cache/tests.rs` remains `#![cfg(test)]`; nothing under `src/dst` changes,
  so `docs/refactor/test-inventory.json` stays untouched;
  `docs/refactor/WIRE-MATRIX.md` is unaffected (in-process cache only).
- **Line budgets after the correction:** `cache.rs` ≈ 275 and
  `cache/tests.rs` ≈ 320, both far under the 1,000 ceiling for new files;
  `src/registry.rs` figures in 4.0 are unchanged (1,519 → 1,516 or 1,509).
