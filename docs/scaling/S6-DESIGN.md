> **Repository copy** (push 4, 2026-11-10) of the design document `S6-DESIGN.md`, rev 6, package 6: the merge and lineage GC, as it stood when the code landed. Where it and the code differ, the code is the authority; later decisions are in the edge records (`docs/reviews/2026-09-hardening/edge-changes.md`), the RUNBOOK and the module docs. Paths under `~/.streams-k2/` and the sibling documents it cites (reviews, decisions, run results) are the design work's own files outside this repository.

# S6: merge and lineage GC (package 6), detailed design, rev 6

Status: rev 6 applies ROUTE-HASH-DECISION-2026-11-08.md (Fable 5.1, lead), option (A). In layout 8 every
per-entity key carries the **entity hash** `keyspace::entity_hash(route, identity)` = `route[0..3] ‖
identity[3..16]`. Range and L2 are restated on it (§1); the filter of §3.1 is withdrawn on slate and comes
back with package 4's commit "A row's hash places it on its shard"; the two child tests mint through
`entity_hash` (§9); TLA-019 records `ASM-ROUTE-PREFIX` (§8); and every sentence that routes, ranges,
filters, projects or tags by a hash names the hash and its minter (RULES.md, "Name the hash and its
minter"). Rev 5 is kept as S6-DESIGN-rev5.md. The protocol is unchanged.

Rev 5 takes S4 rev 2's intent, fence and claim rules for the merge (S4-REVIEW.md; S4-DESIGN.md rev 2
§4-§6): one cell-wide intent, never deleted, terminal states as ETag CASes, `'T'` alone rolls forward;
only an opener of a source claims, after fencing, and a merge claim fences both sources; the actor stops
before `'T'` on a failed `owned()` and never after; the intent reserves moves; C4's seeded clone takes
`M`'s open out of the pause; a retired-home GC counter; the formal controls (§7.2, §7.4, §8, §9, §11; the
delta is listed in §15). Rev 4 is kept as S6-DESIGN-rev4.md. Rev 4 applied Fable 5.1's review of rev 3 (S6-REVIEW.md: "proceed after fixes"). Every item of its
minimal fix list is taken (§14 maps each finding to its section), and so are the mediums and lows. Code
may start without another round unless a fix changes the protocol materially; none does (§14). Rev 3 is
kept as S6-DESIGN-rev3.md.

Brief: SCALING-DECISIONS.md "S6"; parent design SCALING-DESIGN.md rev 3 §3.2 (option B); P5-DESIGN.md
§1.4-1.5. Depends on S3a (layout 8, package 4: the `home` codecs) and S4 (topology substrate + split,
package 5: the shared state machine, gate, intent and sentinel helper). Agreements with both are in §12.
The external review of SCALING-DESIGN rev 2 (EXTERNAL-REVIEW-rev2.md, split/merge A-E) is taken in §7.

## 0. What S6 delivers

After a split, two children read objects their parent wrote; after a merge one shard reads objects its two
siblings and their ancestors wrote. S6 makes that sharing safe to reclaim:
- a shard never deletes an object another shard may still read;
- an ancestor object is deleted only after every current shard that inherited it has released it;
- a retired shard's SlateDB files are deleted once no clone borrows its SSTs;
- lineage depth stays bounded for segment and run-index objects alike.
S6 also builds the merge itself, the mirror of S4's split, and replaces the merge route's `501`.

Safety rests on one monotone fact (L3): a reference to an ancestor object only flows downward, from a
holder with a positive count to its children, and a released reference never comes back. Every crash
leaves at worst unreclaimed bytes, never a deleted object a reader needs. The one exception, a source
served between the two retirement writes of a merge, is closed by L8.

## 1. Vocabulary and the invariants

- **Incarnation, `home`.** Every shard incarnation (a prefix at one path) has a `home: u32`, allocated
  once by a CAS on S4's home counter (§2.2) and never reused, an abandoned attempt's included. An object
  `(home, kind, epoch, seq)` lives at `path(home)/<kind dir>/<epoch>/<seq>` and only the incarnation
  `home` writes it.
- **Leaf.** An incarnation listed in the current topology. **Retired**: one that was a leaf and is not
  (split or merged away); its log carries `'T'`, and it is never served again.
- **The three hashes** (ROUTE-HASH-DECISION §1; `crypto.rs:272-282` says why they are distinct types):
  - **route**: `RouteHash::for_stream` (route-v1 ‖ project ‖ name) or `RouteHash::for_child` (a split
    child's salted route, persisted in `SegmentDesc.route_hash`). The registry places a segment by it:
    `Identity::segment_route` → `registry::shard_for_hash`, at most 24 leading bits;
  - **engine identity**: `SegmentHash::for_stream` (storage-v1 ‖ project ‖ name ‖ epoch) or
    `SegmentHash::for_segment`. It is fixed per incarnation and per segment, and it never places anything;
  - **entity hash** (layout 8): `keyspace::entity_hash(route, identity)` = `route[0..3] ‖
    identity[3..16]`, minted only in `registry::identity::Identity` and on the queue's creation path. Every
    per-entity key, `'K'` row, seal-table entry and run-index footer entry carries it. On slate, before
    layout 8, those carry the engine identity, which has **no** relation to the shard's prefix.
- **Range.** A leaf's range is the set of entity hashes (`keyspace::entity_hash`) whose leading bits are
  its prefix (a merged leaf's prefix is its siblings' parent prefix), tested by `keyspace::range::Range::of
  (prefix).contains(entity_hash)`. By G3 (S3a §1.1, discharged by KANI-106) that is exactly the set of
  segments whose route (`RouteHash::for_stream`/`for_child`) `registry::shard_for_hash` places on the
  leaf. Only the entity hash may be tested against a range; an engine identity never.
- **Holder.** A leaf holds `(A, kind, id)` when it has any of:
  - a positive refcount row for it (segment objects);
  - an `'I'` row naming it (run-index objects);
  - a pending release row (`'E'`, §3) not yet logged.
- **Ancestor object.** An object whose home is not the reader's own.

Invariants, each pinned by a test, a DST or a formal control (§8, §9):

- **L1 (no foreign delete).** A leaf DELETEs only objects whose home is its own (the `'G'` path, the
  orphan sweep) and SlateDB files only of its own path. Under an ancestor's path a leaf writes only its own
  lineage objects (`lineage/refs/<self>`, `lineage/rel/<self>/…`). Ancestor objects, lineage objects and
  retired paths are deleted only by the lineage sweeper of a retired home (§4) and the retired-LSM
  deletion (§5).
- **L2 (exact counts per holder, per range).** A leaf's `'R'` row for `(home, id)` counts exactly its
  unreclaimed, unsuperseded chunks **of streams whose entity hash (`keyspace::entity_hash`) is in the
  leaf's range**, i.e. the streams whose segment route the registry places on the leaf. Every reader of an object's table or
  of an inherited run-index object filters by range: the cleaner's `live_of`, the consolidation's
  re-check, the run-index merge's `kept_entries` and the purge's reclaim (§3.1). At a split the children's
  counts sum to the parent's (S4's copy rule computes exactly this); at a merge the merged leaf's count is
  the siblings' sum (§7.2).
- **L3 (downward flow).** A leaf gains a reference to an ancestor object only at birth, from a parent or
  sibling that held it with a positive count at the quiesce. After a leaf has logged a release (§3), neither
  it nor any later descendant holds the object. No other cross-shard object reference exists: a fork (child
  stream) pins the source incarnation through the registry, not its objects (checked on slate; a test pins
  that a fork's reads go through its own shard's directory).
- **L4 (release after the leaf's readers).** A leaf logs a release only under the condition on which it
  would have DELETEd an own object (`'G'` today): `due_ms` passed, and no sealed read of the process that
  started at or before the zeroing still runs. An `'E'` row not yet logged is still held, so batching never
  shortens the hold.
- **L5 (complete holders).** Every leaf wrote, before the CAS that made it a leaf, a reference list under
  every ancestor path it holds objects of (§2.3). A leaf with no list under `A` holds nothing under `A`.
  **Detected at open** (§3.3): a leaf whose rows name an ancestor home without its list under that path
  refuses to serve.
- **L6 (deletion rule).** The sweeper deletes `(A, kind, id)` only if all of:
  - at one topology version `V` that **its own process** has held for at least `LINEAGE_GRACE` (§4), every
    leaf of `V` with a reference list under `A` has the object absent from that list or logged released;
  - `A` is not a leaf of `V`;
  - `A` is not a source of an unresolved intent.
- **L7 (separate protection layers).** No SlateDB event (detach, checkpoint deletion, compaction, manifest
  GC) is an input to §4's deletion rule, and no lineage fact is an input to SlateDB's. §5 reads SlateDB
  state only to delete a retired shard's own SlateDB files.
- **L8 (no source served under an intent; claims after fencing).** An engine named as a source by the
  cell's unresolved intent does not serve (S4 §6.3's open-path fence). Only an opener of a source may claim
  a lapsed intent, after its own open fenced the previous writer; a merge claim fences **both** sources
  before deciding (§7.4). This closes the window between the two retirement writes of a merge, in which a
  lapsed override would otherwise let the second source's home owner reopen it and ack writes that the
  merged leaf's older checkpoint lacks.

L6 is safe given L3-L5 and L8:
- a reader in a current leaf reads only objects its log holds (L2);
- a holder's reads end before its release is logged (L4);
- a retired incarnation's in-flight reads end within `LINEAGE_GRACE` of its retirement, by ASM-HSEG-GRACE:
  the same bound the orphan sweep's age rule and the `'G'` grace rest on;
- no future leaf can gain the object (L3).
LIST results are complete for objects PUT before the LIST begins (ASM-OBJSTORE-LIST: strong
read-after-write listing, which S3 and Tigris both document), named in the manifest's assumptions.

## 2. Rows and objects

### 2.1 Keys that carry `home` (S3a's codecs, agreed with package 4)

`home` is absolute, not an index into a per-shard lineage row. There are three reasons:
- Run-index objects are immutable and both children and then the merged leaf read the same ones; a
  relative index would need a per-writer translation table in every ridx object.
- A contiguous key range per home is what the release scan, the detector (§3.3) and the derived `all`
  (§3.2) need.
- Homes are birth-ordered, which gives the equal-span tie-break its order across homes: the rank is
  `(home, epoch, seq)`, and a copy's home always descends from the original's. Without it a merged leaf,
  whose union clone restarts `writer_epoch` at 0, would lose every tie to the original it copied.

| Row / entry | Layout 8 key | Value |
|---|---|---|
| refcount `'R'` | `FF×16 ‖ 'R' ‖ home u32 BE ‖ epoch ‖ seq` | as today (`live_bytes, live_chunks, created_ms, object_len`) |
| orphan counts `'O'` | `FF×16 ‖ 'O' ‖ home u32 BE ‖ epoch ‖ seq` | as today |
| run-index pointer `'I'` | `home u32 BE` in the key, after the pointer tag | as today |
| directory entry, run-index entry, chunk locator | the object's `home` (varint) beside `(epoch, seq)` | |
| garbage `'G'` | `FF×16 ‖ 'G' ‖ epoch ‖ seq`, no home: own objects only (L1), asserted | as today |
| pending release `'E'` (S6) | `FF×16 ‖ 'E' ‖ home u32 BE ‖ kind u8 ‖ epoch ‖ seq` | `ver, object_len, zero_ms (pin clock), due_ms` |
| lineage-sweep cursor `'C'` (S6) | `FF×16 ‖ 'C' ‖ home u32 BE ‖ kind u8` | the last key listed |
| retired `'T'` (S4) | `FF×16 ‖ 'T'` | `(topology version, successor homes, operation id)` |

Rev 4 drops the `'A'` holdings row (review finding 12). A maintained counter whose error would write the
`all` marker early is replaced by a derivation: "holds nothing under `A`" is "no `'R'‖A`, `'I'‖A` or
`'E'‖A` key", one in-memory prefix scan each. Package 4 is told that `'A'` is unreserved.

S6's tags `'C'` and `'E'` are distinct from S3a §2.4's tags. Layout 8 needs no sentinel-tag ordering
(every sentinel read is one tag's prefix). `'G'` stays own-only with an encoder assert; `'K'` may hold rows of several homes after a split or a merge (S6-c), with no own-only assert.

### 2.2 The topology's part (S4's `topology.json` and home counter)

- **Homes are allocated by a CAS** on S4's home counter (in `topology.json` or a sibling object, S4's
  choice), one per attempt of an operation, recorded in the intent at `Preparing`. A retried attempt
  allocates a new home and so a new path (review finding 5). `homes[h] = {path, prefix, parents: [h…],
  born_version, retired_version?}` is written by the CAS that publishes `h`.
- **Paths** (S4-DESIGN rev 2.1 §3.1): the initial homes are `shards/<prefix>`; every operation-born home,
  a split child's and a merge's alike, is `shards/<prefix>-<home>` (rev 6a: `-`, which needs no URL encoding; `~` broke the clone's external paths, NEXT-WORK §17). That is unique by construction, and an
  abandoned attempt's path is never reused.
- `object_path(home, id)` reads `homes[home].path` from the directory's topology snapshot. A retired home
  stays in the map until §4 marks it gone (~100 B each).
- **Depth** of an ancestor home `A` for a leaf `L` is the shortest parent chain from `L` to `A` in the
  home DAG (own home: 0). Used only by §6.

### 2.3 Lineage objects under an ancestor's path

These live outside `hseg/` and `ridx/`, so the orphan sweep's LIST and its parser never see them.

- **Reference list** `path(A)/lineage/refs/<leaf home>`: immutable (Create), written by the operation that
  creates the leaf (S4's split, §7's merge) before its topology CAS. Body: `ver, count, [(kind, epoch,
  seq)] sorted, crc32c`, 17 B per entry (`kind u8, epoch u64 BE, seq u64 BE`; codec `spill::lineage`). It lists every object under `A` the leaf holds at birth: segment
  objects with a positive count for the leaf (L2: chunks whose entity hash is in its range), and run-index
  objects its `'I'` rows name. Size bound:
  1.6 MB at 100,000 objects.
- **Release log** `path(A)/lineage/rel/<leaf home>/<epoch>-<seq>`: an immutable object the leaf writes on a
  collector tick with the releases it logs under `A` since its last log; same body format, at most
  `RELEASE_LOG_MAX` (4,096) entries. `<epoch>` is the opening's writer epoch and `<seq>` counts from 0 per
  opening, so the name is unique by construction and a fenced writer's late PUT never collides (review
  finding 15).
- **Release snapshot** `path(A)/lineage/rel/<leaf home>/snap-<epoch>-<seq>`: once an hour while releasing,
  the cumulative set of the leaf's releases under `A`. The sweeper reads the newest snapshot and the logs
  named after it, so its GETs per holder are bounded by one hour of logs, not by the ancestor's life.
- **Released-all marker** `path(A)/lineage/rel/<leaf home>/all`: PUT once the leaf holds nothing under `A`
  (the derived test of §2.1); the sweeper then skips the leaf's list and logs.

Why logs and not one marker per object: at the soak's seal rate a busy leaf releases ~800 objects an hour.
Per-object markers cost ~$1.1/month per busy shard at ab50; logs cost at most 120 PUTs/hour while
releasing (≤ $0.16/month).

## 3. In each leaf

### 3.1 The range filter (L2; review finding 2)

The cleaner's `live_of` walks an object's whole chunk table and asks each stream's index whether its chunk
is live. On a child, the inherited run-index objects also list the sibling half's streams, so without a
filter the cleaner decrements chunks the leaf never counted, `stage_decrements` refuses the batch as
`Inexact` (`gc.rs:460-468`), and no shared object is ever rewritten.

**In layout 8** two walks filter, each comparing the leaf's prefix with an entry's **entity hash**. Both
sides are named:
- the left side is `keyspace::range::Range::of(own prefix)`, where the prefix is the leaf's, from the
  topology, and a test rig's label or the root prefix means every hash;
- the right side is the seal-table entry's or run-index footer entry's `hash`, which in layout 8 is
  `keyspace::entity_hash(segment route, engine identity)`, written by the sealer and the fold from the
  `'K'` row's key.

By G3 the filter keeps exactly the streams whose segment route the registry places on the leaf.
- the cleaner's `live_of` (`gc/clean.rs`);
- every `'K'` consumer (K-SPLIT-DECISION-2026-11-09 §2.1). `'K'` is copied to both children at a split and
  unioned at a merge, so a row of another home may name entries outside the range. The open's
  `rebuild_directory`, the fold's `scan_unfolded`, S4's `'R'`/`'O'` recompute and S6's range-filtered counts
  (L2) each read rows through `spill::directory::split::decode_row_in_range(id, raw, range)` (package 5's), so
  every `chunk_off` stays right and only in-range entries are admitted;
- the run-index merge's `merge_steps` (`fold/merge.rs`), which drops inherited runs whose entity hash is
  outside the range before merging, so the leaf's own ridx objects never carry the sibling's half (the
  filter sits in `merge_steps`, not in `fold.rs`'s `kept_entries`, so S3a's file is untouched).

The consolidation re-check and the purge reclaim need no filter. Both walk the entries of one stream that
this shard serves, keyed by that stream's entity hash (its engine identity on slate), never an object's
whole table.

A queue's chunks are all in one leaf, so S3b's trim is filtered trivially. The queue's entity hash is
minted on the queue's creation path, from its route.

**Slate, before layout 8: no filter.** Step 2 (0c782ccd6) compared `Range::of(prefix)` with the engine
identity (`SegmentHash::for_stream`/`for_segment`), which is independent of the route. On a 4-shard cell
each merge dropped about 3/4 of a shard's own run-index entries, and the cleaner's copy would have deleted
the chunks it did not count. It was masked in production by `INITIAL_SHARDS=1`, where the root `Range` is
`ALL`, and found by package 5.
- The two filter lines (`merge.rs:116`, `clean.rs:284`) are removed outright on slate, by package 5's
  hotfix.
- They are not made conditional: no "unsplit" test, flag or switch, under AGENTS §1's one production path.
- On slate no child exists, so nothing needs them yet.
- `Range` stays as the matcher that 4a commit 1b moves into `keyspace::range`.
- The filters return in layout 8, with or right after package 4's commit "A row's hash places it on its
  shard". That commit makes the right-hand side an entity hash, typed `EntityHash` at the filter sites.

**Red tests.**
1. `dst::dst_tests::shard_ownership::every_row_of_a_shard_lies_in_its_range` (package 4's commit;
   ROUTE-HASH-DECISION §5 test 1):
   - setup: a cell with four **bit** prefixes `"00" "01" "10" "11"`, and streams and queues created
     through the product path;
   - on every shard: a seal, a fold, a run-index merge and a cleaner pass;
   - assertions: every stream reads back whole, and every key's, `'K'` row's and footer entry's hash is in
     its shard's `Range`.
2. The two-half object, with hashes minted through `keyspace::entity_hash` from one route in the child's
   range and one out of it, never planted raw. The child's ancestor object is rewritten for exactly the
   child's half, and the child's row decrements by exactly that half.

TLA-019 control `nc-count-foreign-half` → `RefcountExact`.

### 3.2 The release protocol

Today a decrement that zeroes an `'R'` row deletes it and writes a `'G'` row in the same batch, and the
collector DELETEs the object once `due_ms` passed and no pinned read started at or before the zeroing.
S6 splits that on `home`:

1. **Own home:** unchanged (`'G'`, DELETE).
2. **Ancestor home:** the same batch deletes the `'R'` row (or the `'I'` row, for a run-index object) and
   writes `'E' ‖ home ‖ kind ‖ id` with the pin-clock stamp and `due_ms = now + grace`. Two places write
   `'G'` on slate:
   - `stage_decrements` (`gc.rs:478`): the segment paths, which are purge reclaim, consolidation commit and
     cleaner commit;
   - the run-index merge's retirement of its inputs (`fold/merge.rs:140`).
   Both branch on `home` through one helper; a test pins each of the four decrement paths.
3. **Logging:** on the collector tick, the due `'E'` rows (the `'G'` collector's predicate, reused) are
   grouped by ancestor home. Per home, one release log is PUT, then one batch deletes those `'E'` rows.
   When a home has no `'R'`, `'I'` or `'E'` key left, the collector PUTs its `all` marker.
4. **Crash windows** (equal to the `'G'` path's):
   - before the zeroing batch: nothing happened;
   - after the batch, before the log PUT: the `'E'` row survives, and the next tick logs it (the next
     opening restarts the pin clock: `grace` from open);
   - after the PUT, before the `'E'` delete: the next tick PUTs a second log naming the same ids; the
     duplicate is harmless because the sweeper computes `list − ∪logs`.
   A release is never logged before its zeroing is durable. A fenced writer's late log is still true.
5. **Reads** of ancestor objects take the same reader pin as own objects, so L4 needs no new mechanism.

The cleaner needs nothing more to release: its commit decrements through `stage_decrements`, and the
zeroing of an ancestor object becomes an `'E'`. The object-table GET goes to `path(home)`.

### 3.3 The open-time detector (L5; review finding 10)

At open, after the sentinel load, the engine collects the distinct foreign homes in its `'R'`, `'I'` and
`'E'` keys (one in-memory pass) and HEADs `path(A)/lineage/refs/<self>` for each (cached for the
opening). On a miss it refuses to serve (`409`, after a topology refresh, like a `'T'`d engine) and counts
`lineage_list_missing`. A late PUT cannot heal it, since a sweep may already have decided on the missing
list's evidence, so it pages (operator row). TLA-019 control `nc-list-missing-holds-nothing` →
`NoDeleteWhileReferenced`.

## 4. The lineage sweeper (one per retired home)

**Who runs it.** For a retired home `A`, the leaf with the smallest home among the current leaves with a
reference list under `A`; if none has one, the leaf with the smallest home overall. It runs on the orphan
sweep's cadence (hourly). Two sweepers at once (a topology change mid-sweep) are safe: every step is
idempotent, and L6 is evaluated from one snapshot.

**One sweep of `A`:**

1. Take the directory's topology snapshot `V`. Skip unless this process installed `V` at least
   `LINEAGE_GRACE` ago by its own monotone clock (review finding 7); `published_at` is a floor, never the
   measure. Also skip if `A` is a leaf of `V` or a source of the cell's live intent.
2. `holders = { L ∈ leaves(V) : path(A)/lineage/refs/<L> exists }`, from one LIST of `lineage/refs/`. A
   `refs/<h>` whose `h` is neither a leaf of `V` nor named by a live intent belongs to an abandoned attempt
   or a retired leaf whose duties passed to its successors: its list is ignored, and step 5 deletes it.
3. For each holder: if `rel/<L>/all` exists, `held_L = ∅`. Otherwise GET its list and its newest snapshot
   plus the later logs; `held_L = list − released`.
4. LIST `path(A)/hseg/` and `path(A)/ridx/` (paged; the cursor is this leaf's `'C' ‖ A ‖ kind` row) and
   stream the listing against `∪ held_L` (a few MB). DELETE every object outside it. This deletes the
   retired shard's own garbage, orphans and unreferenced objects too: the sweeper subsumes the orphan
   sweep for retired paths.
5. Delete the lists and logs of non-holders (step 2). **Never-leaf child paths** (S4-REVIEW-rev2 N1): a home
   that is neither a leaf of `V` nor named by the cell's live intent and was never published (an abandoned
   split or merge attempt; a stale actor's late clone after the claimant's `RolledBack`) may have files at
   its path. **S4's contract** (S4-DESIGN rev 2.1 §15): `h` is never-leaf when `h < next_home`, `h ∉
   homes` (never published) and `h` is not among the live intent's children; its path is found by one
   delimiter LIST of `shards/` (names ending `-<decimal h>`), and its lists are `refs/<h>` and `refs/<h>-d`
   under every ancestor path (step 2). The sweep reads the path's manifest `external_dbs`, deletes each
   `final_checkpoint_id` on its path (the parent and inherited ancestors), then the path's files.
   **Race guard:** the sweeper reads `next_home`, `homes` and the intent, LISTs `shards/`, then **re-reads
   the intent** and skips any `h` the intent now names. A home allocated before the first read whose actor
   creates the intent and clones before the LIST is then seen in the second read (the intent precedes the
   clone's files; ASM-OBJSTORE-LIST); a home allocated after the first read is `≥ next_home` and excluded. S4's counter
   `topology_never_leaf_paths` counts them until this sweep drains it. DST (S4's crash matrix): a stale
   actor's late clone after `RolledBack` leaves, after the sweep, no checkpoint on the parent beyond its own
   and no files under the dead child path. When both prefixes are empty and §5 has deleted
   `A`'s LSM: DELETE `lineage/` under `A`, and count `A` as gone.

**Why one version suffices.** By L3, the objects under `A` that any future leaf can hold are a subset of
`∪ held_L` at any version. A leaf born after `V` holds only what its parent or siblings held at its birth,
and those are leaves of `V` or descend from them. The grace on `V` covers the retired incarnations'
in-process reads.

**Cost.** Per retired home per hour: 1 LIST of `refs/`; per holder a HEAD, a GET, a LIST and at most one
hour of log GETs; 1 LIST page per 1,000 objects. Under $0.001/month per retired home, even for the 63
retired homes of the 64 → 1 ramp.

## 5. Deleting a retired incarnation's SlateDB files

The retired shard's `manifest/`, `wal/`, `compacted/` and `compactions/` are deleted by the sweeper of `A`.
Never `hseg/`, `ridx/` or `lineage/`, which §4 owns (L7). All of the following must hold:
- `A` has been retired for `RETIRED_GRACE` (1 h, provisional) on the sweeper's own clock;
- `A`'s latest manifest lists no checkpoint. Each clone's `final_checkpoint_id` pins one, and the fork's
  detach GC deletes it once the clone references no SST of `A` (`detach_gc.rs:20-40`). S4's split and
  §7's merge delete their own checkpoints as their last step, and a rolled-back attempt deletes both kinds
  (§7.4);
- no leaf of `V` names `A`'s path in its manifest's `external_dbs` (one manifest GET per descendant leaf).

Order: `compacted/` and `wal/`, then `compactions/`, then `manifest/` last. A crash midway leaves a
manifest naming missing SSTs, which is harmless: a retired shard is never opened, and the next sweep
finishes. DST (L7): a parent's final checkpoint deleted by hand mid-lineage leaves the object listing under
its path unchanged.

## 6. Bounding lineage depth: forced rewrite and forced re-tile

- **Segment objects.** An ancestor object whose home has depth > `LINEAGE_MAX_DEPTH` (4, provisional) is a
  cleaner candidate regardless of `clean_pays` (`forced || clean_pays(…)`). It is copied to the own path
  through the cleaner's commit, for the leaf's range only (§3.1), and the old object's zeroing becomes an
  `'E'`.
- **Run-index objects** (review finding 9). An ancestor ridx object at depth > max is a forced input of
  the run-index merge regardless of `fan_in`. The merge re-tiles its own-range entries into the leaf's
  ridx object and retires the input through the `'E'` branch. Otherwise a steady child would keep its
  ancestors' top-level ridx objects forever, and its cold reads would fan out across every ancestor path.
- **Rate.** The pass budget (4 MiB, 64 GETs per 30 s tick) drains ≈ 11.5 GB a day per leaf. When forced
  candidates exist, the budget for them scales ×4 (≈ 46 GB/day); a forced copy moves 100% live bytes, at
  ≈ $1 per 100 GB in PUTs and transfer. A leaf that inherits more than a day's drain per depth step cannot
  hold depth ≤ 4, so S5's policy caps split/merge frequency by inherited live bytes, and the operator
  counter `lineage_depth_max` pages above max + 1 (review finding 17).
- **Red tests:** a segment object and a ridx object at depth 5 are rewritten or re-tiled; at depth 4 they
  are not.

## 7. Merge

The brief: the override moving `…1` to `…0`'s owner; both sources quiesced; a union clone; `'R'` summed per
`(home, id)`; `'T'` into both; a fresh path; one CAS; the route's `501` replaced. The merge and S4's split
share one state machine, one mutation gate, one intent format with its takeover, the open-time source fence
(L8) and one sentinel helper. S4 (package 5) builds them in `src/topology/`; the merge is their second user.

### 7.1 The mutation gate and its lock order (review A; findings 3, 4)

`CommitOp::Barrier` stops client writes, which all go through the committer: appends, queue sends and
settles, `UsageAck`, the billing close and retained ops, `Seal`, and `Seal(Purge)`. Every writer *outside*
the committer goes behind a per-engine `MaintenanceGate` (an async RwLock), shared around exactly one batch
plus the store PUT that batch names:

| Writer | Site |
|---|---|
| fold | `fold.rs:404`, its ridx PUT `fold.rs:537` |
| run-index merge | `fold/merge.rs:129` |
| consolidation commit | `consolidate.rs:226`, its PUT `:210` |
| cleaner commit | `clean.rs:414`, its copy PUT `:373` |
| collector `'G'` deletes and S6's `'E'` deletes, log PUTs | `gc.rs:697` and new |
| purge reclaim and row deletes | `purge.rs:296`, `:320` |
| orphan-sweep cursor, lineage cursor | `sweep.rs:130` and new |
| **`request_purge`** | `purge.rs:62`; callers: the client DELETE path (`deletion.rs:519`), the billing tombstone walk (`walk.rs:194/211`), the replaced-debt pass (`replaced.rs:151`) |
| **S3b's trim pass** (a gc step since S3b rev 2) | `gc/trim.rs`, its one batch (S3b-REVIEW F7) |
| S3b's lane expiry | committer (barrier) |

This is Fable's writer inventory from slate, complete with `request_purge`, S3b's trim pass (outside the committer since S3b rev 2: gated, S3b-REVIEW F7, pinned by S4's "split during a trim" DST) and S3b's lane expiry (inside the committer: the barrier covers it).

**`request_purge`** (review finding 3) takes the gate with `try_acquire_shared`. On a gated or closed engine
it returns `Busy` at once and does not wait out the pause:
- the DELETE path then leaves the purge to its existing fallback, the tombstone walk (package 6's driver
  design), so the client's DELETE answer does not change;
- the walk and the replaced-debt pass retry on their next pass. After publication they resolve the
  stream's segment route (`RouteHash::for_stream`/`for_child`) through `registry::shard_for_hash` to the
  merged leaf, and the request is idempotent.
One test per caller.

**Lock order** (review finding 4), stated in the gate's module documentation and pinned by a DST:
- **Order:** gate (shared) → `refs` → one batch.
- **Never** hold the gate across a committer op, a store GET loop or a sleep. The purge's committer step
  (`CommitOp::Seal(Purge)`, `purge.rs:204`) runs outside the gate, and each reclaim batch takes it
  afterwards.
- **The topology operation** takes the gate exclusive *before* the barrier, with a deadline. On expiry it
  aborts while still before any `'T'` (a roll back), so a stuck writer costs one aborted attempt, never a
  deadlock.
- **DST:** the purge's committer step is in flight when the quiesce starts; the merge completes.

**The merge and the fleet count** (FLEET-HYSTERESIS-REVIEW.md §8, Fable, 2026-11-09; the lead carries it into
SCALING-DESIGN §4.3's serialisation row). Three timescales meet here:
- the count grows on 30 s of sustained load, at least 30 s after its last publication, and shrinks one step
  per 300 s;
- slot-driven growth is blind for 60 s after a count change;
- a merge runs on 6 h of low load and never within 12 h of its split.

The rules:
1. **A merge waits for the fleet.** It starts only when `fleet/desired.json`'s `computed_at_ms` is at least
   `SHARD_TOPOLOGY_COOLDOWN_SECS` (60 s) old, and no override anywhere is younger than
   `REBALANCE_MOVE_COOLDOWN_SECS`. The check runs where the merge's policy decides to start, before the
   intent is created. A merge that waits is not started, so the wait has no intent to resolve and leaves no
   state.
2. **No fleet scale-in while a topology intent is live.** `fleet::hysteresis::Publisher::decide` (the queue
   session's) refuses a shrink while `topology/intent.json` is non-terminal. It reads the intent once per
   tick, cached by its ETag, as S4 §4.4 does for the rebalancer. The reason is that a shrink stops an
   ordinal that may be the merge's actor (`M`'s opener) or a source's owner, and §7.4's recovery is for
   failures, not for something the cell provokes. **Scale-out is never blocked.**
   **Who lands it:** the first commit that writes a topology intent, S4's split or S6's merge, whichever
   lands first. Before that, the input is always false. The queue session's fleet-hysteresis package leaves
   `Publisher::decide(plan, cur, now)` in `src/fleet/hysteresis.rs`, and the intent's commit does three
   things:
   - it gives `decide` the intent's "non-terminal" as one more input, and refuses a shrink while it holds;
   - it adds the per-tick ETag-cached read, which changes `fleet::start`, whose exception row is frozen, so
     the re-pin is that commit's owner decision;
   - it adds a `decide` unit test beside the merge DST.
3. **The clamp does the rest.** `need ≤ shards` stands. A published merge lowers the shard count, so the
   clamp falls, and the fleet then falls one instance per 300 s. There is no merge-specific fleet rule and
   no pin; `FLEET_MIN` stays the floor.

Tests: a merge that is due while `desired.json` is younger than 60 s, or while an override is younger than
`REBALANCE_MOVE_COOLDOWN_SECS`, does not start, and it starts at the first policy tick after both have
aged. Rule 2's `decide` unit test lands with the first intent writer (above). A DST pins a merge's intent spanning a would-be
shrink tick: the count stays, then falls after `Reclaimed`.

### 7.2 The intent and the states (rev 5: S4 rev 2's intent, fence and claim rules)

The merge uses S4 rev 2's mechanism unchanged (S4-DESIGN.md §4-§6). This supersedes rev 4's per-prefix
intent.

**The intent** is `topology/intent.json`, one per cell (S4 §4.1):
- created only when absent (`PutMode::Create`), or by a CAS over a **terminal** intent (`Reclaimed` or
  `RolledBack`) with a new `op_id`; a live intent answers the merge route `409 topology_busy {reason:
  "intent"}`;
- **never deleted**: `Reclaimed` and `RolledBack` are ETag CASes carrying `lease_epoch`, so a stale actor's
  late write fails its CAS; a DST store wrapper refuses any DELETE of `topology/intent.json`;
- for a merge it records `op: merge`, `op_id`, the sources `[(prefix0, home0), (prefix1, home1)]`,
  `merged_home`, `version_seen`, `reserved_shards: 1` (S4 §4.4: the actor holds one shard more until `M`
  is published), and the lease;
- **`'T'` alone rolls forward:** each source's `'T'` = `(version_seen + 1, merged_home, op_id)`, and
  `path(M)` follows from the home (§2.2), so an opener that finds `'T'` with no live intent finishes from
  `'T'` alone (re-creating the intent only for bookkeeping).

**The lease** is S4 §4.2's: renewed by ETag CAS every `TOPOLOGY_LEASE_MS / 3`; lapsed only by the
claimant's own monotone observation of an unchanged ETag across `lease + skew`. **The lease does not
fence**: only an open fences (the formal model keeps a slow actor stepping after a lapse).

**As a move reservation** (S4 §4.4): while the intent is live, return-home and the rebalancer skip both
source prefixes, `pick_move_target` and count balance treat the actor as holding `reserved_shards` more, and
the merge route refuses any override on either source except the merge's own move of `…1` to the actor,
which it writes itself under the live intent.

**States and durable witnesses** (as S4 §6.1, per source where it says "each"):

| State | Durable witness | In the pause? |
|---|---|---|
| Preparing | the live intent; `M`'s reference lists | no |
| Quiesced | `topo-<op_id>` checkpoint on each source (named, 1 h lifetime) | yes |
| ChildReady | `M`'s manifest initialized with its seed SSTs (C4); fallback: `M`'s `'Y'` in its last seed batch | yes |
| SourcesRetired | `'T'` in each source | yes |
| Published | `topology.json` at `version_seen + 1`, `last_op.op_id = op_id` | yes (ends the pause) |
| Reclaimed / RolledBack | the intent CASed terminal; `topo-` checkpoints deleted | no |

**The steps:**

0. **Preparing** (unpaused).
   - Refuse: a live intent (`409 topology_busy {reason: "intent"}`); an override on either source other
     than the merge's own (`{reason: "override"}`); a source carrying `'T'`; the reservation (`409
     instance_full`).
   - The home CAS (`next_home += 1`), then the intent create or terminal-CAS, with the lease.
   - Write the override moving `…1` to the actor; wait until the actor holds both engines as writer.
   - **The snapshot pass:** a SlateDB snapshot on each source; collection off (S4 §5.1); S4's helper
     `plan` over both, which yields `M`'s rows (§7.3), the sums, and `M`'s reference lists filtered by range
     (§3.1). PUT the lists in parallel.
1. **Quiesced** (the pause starts), on both sources in parallel:
   - the gate exclusive, with S4's deadline (a deadline rolls the attempt back, CAS `RolledBack {reason:
     "gate_deadline"}`);
   - the barrier, answered after its group (S4 §5.2);
   - `flush()`, then `create_checkpoint(All, "topo-<op_id>", 1 h)`.
   No fold and no seal: `'K'` is unioned (§7.3), and unsealed pages stay as rows, carried by the projection
   with their `'W'`/`'M'` rows.
2. **ChildReady:**
   - the delta since each snapshot (S4 §7.2), and the checks: `R_M = R_0 + R_1`, `O_M = O_0 + O_1` per
     object, `N_M = N_0 + N_1` per category and project, or roll back (counters as S4's);
   - a supplementary list `refs/<M>-d` where the delta adds a holding;
   - **C4:** the union clone at `path(M)` from both named checkpoints, each source projected onto its entity
     range, **with the seed** (`CloneBuilder::with_seed(M's rows)`): `M` is initialized together with its
     sentinel rows. There is no open of `M` and no seed batch in the pause.
   - **Fallback** (C4 slipped): the union clone, then S4's maintenance open of `M` (no compactor, no GC), the
     seed batches (`'Y'` last, each flushed), `flush()`, `close()`.
3. **SourcesRetired:** one `'T'` batch into each source and `flush()`, in parallel. The source engines stay
   open, still holding their barrier, until step 4's release. From the first durable `'T'` the actor only
   rolls forward and ignores lease loss.
4. **Published:** the topology CAS on the ETag (`leaves`: the two sources replaced by the parent prefix →
   `M`; `homes[M] = {parents: [home0, home1], born_version}`; both sources' `retired_version`; `last_op`),
   with S4 §6.2's failed-CAS rules; the merge's override of `…1` is removed with it (the prefix is no longer
   a leaf; S6-b). Install the snapshot; release the barrier with `Retired`, re-submitting
   held operations to `M` when this instance owns it (S4's local re-route), else `409` with the version.
   **Only then** close both source engines (`begin_close`, `RetirementReason::Merge`). Closed earlier, a
   source would drain its held operations as `shard_moving` instead of re-routing them. This matches the
   correction to S4-DESIGN step 3.2 (2026-11-09), where S4's commit 11 closes the parent after the
   `Retired` release.
5. **Reclaimed** (unpaused): delete both `topo-<op_id>` checkpoints; CAS the intent `Reclaimed {outcome:
   published}`. The union clone's final checkpoints stay until `M`'s detach GC releases them.

**When the actor stops** (S4 §4.3): before `'T'`, at the first step boundary where `owned()` fails (a
failed renewal, either source's `Db` reporting fenced, or a fence error on either source's write); it then
deletes only what the claimant's GC would delete (`path(M)`, `M`'s lists), removes its override of `…1`
(S6-b), and never writes `'T'`. After
`'T'` it ignores lease loss and finishes; the publish is idempotent on `last_op`. With C4 there is no `M`
writer in the pause; in the fallback a fenced `M` writer ends the actor's run, and the claimant with the
higher `lease_epoch` finishes.

**The pause, re-counted with C4** (S4 §9's measured arm, extended to two sources): flush + `checkpoint(All)`
on both in parallel ≈ 1 + 6.5; delta ≈ 0-1; one union clone with seed ≈ 19 (S4's single-source clone) + 3-4
per extra `external_dbs` entry (the second source and its inherited ancestors: ≈ +4-8); `'T'` on both in
parallel 1; the topology CAS 1. **≈ 32-37 round trips, ≈ 0.8-1.5 s** on the fork's measured round trips,
inside the 2 s hold. The fallback adds the maintenance open (≈ 18-20): ≈ 50-57, ≈ 1.3-2.3 s, which may
exceed the hold; E1 then gets a merge figure. Measured on Tigris before E3's `501` goes.

### 7.3 Rebuilding `M`'s sentinel state (review C)

This is S3a-DESIGN rev 3 §2.8's merge column, tag for tag. Keys of one stream or queue sit in exactly one
source; the exceptions are `'I'` (both may name one ancestor ridx object: equal values, one row) and the
per-object counts, which sum.

"Per hash" and "`deadline ‖ hash`" below mean the entity hash (`keyspace::entity_hash`) in layout 8.
Every rule here is a union of two disjoint key sets, and none tests a range.

| Tag | Row | Rule into `M` |
|---|---|---|
| `'K'` | seal directory, current period, keyed `home ‖ epoch ‖ seq` | **union; a key held by both sources has equal values, asserted, and lands once, as `'I'`** (K-SPLIT-DECISION-2026-11-09 §2.2). Two children of one parent both carry the parent's copied `'K'` rows (S4 copies `'K'` to both children, as `'I'`), so the sources' keys are not disjoint by home. A `'K'` row is immutable (put once under a fresh id, only ever deleted), so equal values are the only legal case; a differing value is corruption, and the merge refuses (the `'I'` assert's counter, no `M` row). No fold in the pause. After the union `M` holds `'K'` rows of several homes, so `'K'` has **no** own-only assert; only `'G'` does. A row of another home may name entries outside `M`'s range, and every `'K'` consumer admits only its range (§3.1). The fold of a log holding `'K'` rows of several homes emits run-index entries carrying each row's home (S4-REVIEW-rev2 S6-c; confirmed by package 4, 2026-11-04). |
| `'R'` | segment refcount per `(home, id)` | sum (range-filtered counts, L2); a zero sum is absent |
| `'I'` | run-index pointer | union; a key in both has equal values (asserted) |
| `'O'` | consolidation orphans per `(home, id)` | sum |
| `'G'` | own garbage | dropped: the sources' own objects become ancestor objects `M` does not hold; §4 deletes them |
| `'H'` | purge head per hash; S3b adds state Trimming (a live queue's trimmed prefix, `reclaimed`) | union (a Trimming row is carried with its `reclaimed`); S3b's per-entity `h16‖'Q'‖'k'` interval rows are not sentinel and ride the projected union with their queue |
| `'L'` | orphan-sweep cursor | dropped |
| `'U'` | usage debt index, per hash | union |
| `'Y'` | layout and home | written fresh with `merged_home`: in C4's seed (atomic with `M`'s initialization), or in the fallback's last seed batch |
| `'Q'` | queue roster, `deadline ‖ hash` | union |
| `'D'` | queue debt index, per hash | union |
| `'W'` | open index: unsealed pages, per hash | union |
| `'M'` | open index: usage awaits the ledger, per hash | union |
| `'N'` | S3b's queue gauges | S3b's rule |
| `'E'` | pending release | dropped: an object a source zeroed is not in `M`'s holdings; the sweeper deletes it one grace after `V` |
| `'C'` | lineage cursor | dropped |
| `'T'` | retired | absent in `M` |

The merge uses S3a's primitives (`keyspace::route` over `SENTINEL_TAGS`, `spill::gc::recompute`'s types).
A merge test enumerates `SENTINEL_TAGS` and fails on a tag without a merge rule.

### 7.4 The source fence, the claim, and recovery (rev 5: S4 rev 2 §6.3-§6.4)

**The open-path fence (L8)** is S4 §6.3's table, run by every open of an engine, after SlateDB's open-time
fence. A merge source is one the intent names in `sources`. A merged leaf `M` is never a source of its own
birth: its open checks its initialization (C4) or `'Y'` (fallback).

**Who claims** (S4 §4.3): only an **opener of a source**, after its own open fenced the previous writer.
The fleet tick never claims; it reports a lapsed intent in the heartbeat. **A merge claim fences both
sources before it decides** (rev 5): the claimant opens the first source (its own open), then opens the
other source too (the other is not serving: the fence keeps it out under a live or lapsed intent), so
both of the actor's writers are fenced; only then does it read `'T'` in both and claim by CAS. Opening one
source would leave the actor able to write `'T'` into the other after a rollback was decided, retiring it
with no published `M` (formal control `nc-claim-fences-one-source`). The claimant opens the source this
instance owns first, then the other(s) by S4's maintenance open (never serving), and closes the ones it does
not own promptly after its CAS (S4-REVIEW-rev2 S6-a: a source whose ring owner was serving normally, because
the actor died before its override moved it, sees one fence and one re-open holdoff; an availability blip,
not a loss, since the barrier never ran there).

| Found at the claim (both sources fenced) | Durable state | The claimant does |
|---|---|---|
| no `'T'` in either; no `topo-` checkpoint | intent, maybe lists | **roll back:** delete `M`'s lists (`refs/<M>`, `-d`) under every ancestor path; **remove the merge's own override of `…1` (CAS on `fleet/overrides.json`) as part of `RolledBack`** (S6-b), so `…1`'s effective owner is its ring pick again within one tick; CAS `RolledBack`; close the source it does not own (its ring owner reopens it), serve the one it owns |
| no `'T'` in either; `topo-` checkpoints and/or `path(M)` | plus maybe `M`'s manifest | **roll back with the takeover GC:** if `path(M)/manifest` exists, delete each `final_checkpoint_id` its `external_dbs` names (sources and inherited ancestors); delete both `topo-<op_id>` checkpoints, `path(M)`'s files and `M`'s lists; remove the merge's override (S6-b); CAS `RolledBack`; as above |
| `'T'` in one or both | `M` complete (C4: initialized; fallback: `'Y'`, since `'T'` follows it) | **roll forward:** write the missing `'T'` (both writers are the claimant's), publish (idempotent on `last_op`), release any hold with `Retired`, then close both, Reclaimed |
| the topology at `T.version` naming `op_id` | published | Reclaimed (idempotent) |

**Recovery by `'T'` alone:** an opener that finds `'T'` with the intent terminal or absent and the snapshot
older than `T.version` claims (creating an intent if absent) and rolls forward from `'T'` (S4 §6.3), after
fencing the other source as above.

**A slow, not dead, actor:** before `'T'`, both its writers are fenced by the claimant's opens, so its
`'T'` flushes fail, its renewal fails, and its rollback deletes only the same objects; its terminal CAS
fails on `lease_epoch`. After `'T'`, both roll forward, and the publish is idempotent. An actor never
re-opens a source after it was fenced (`nc-actor-reopens-after-fence`).

**Races** (one intent per cell, S4 §6.5): a merge against a split or another merge cannot start; a move
of either source is refused while the intent is live; the topology CAS on the ETag tolerates only
interleaved `next_home` bumps.

### 7.5 Correctness of the counts (L2 at a merge)

`M`'s count for every object is what the two siblings would count as one shard:
- the snapshot plus the delta is each source's durable state at the barrier;
- each source counted exactly the chunks whose entity hash is in its range (L2), and the ranges are
  disjoint;
- the union holds exactly their rows in their entity-hash ranges. This rests on ASM-SLATEDB-PROJECTION,
  pinned by the fork test of §12 item 4 and the DSTs' exact-state asserts, and on ASM-ROUTE-PREFIX: an
  entity hash's first 24 bits are its segment route's (KANI-106).
Kani `refcount_merge_sums` pins `R_M = R_0 + R_1` per key, with no zero rows.

## 8. Formal work

- **TLA-019 two-holder arm** (its own config: one reader, `MaxClock` 3; today's baseline unchanged).
  - **State:**
    - an ancestor object held by `C0` and `C1`, each with its own `rc`, `'E'` row, logs and list;
    - a retired holder with an in-flight read ending within the grace;
    - the sweeper over lists and logs at one version, aged on its own clock;
    - the merge summing into `M`;
    - a merged leaf whose epoch is below its ancestors'.
  - **Properties:**
    - `NoDeleteWhileReferenced` over all holders;
    - `RefcountExact` per holder, range-aware;
  - **Assumption `ASM-ROUTE-PREFIX`** (rev 6, recorded in the model's assumptions comment, no state change):
    the model has one abstract hash per stream for both placement and keys. That is true only because a
    row's entity hash (`keyspace::entity_hash`) carries its segment route's 24 leading bits, which KANI-106
    discharges. The comment edit stales TLA-019, which is re-recorded in S3a's window.
    - `ReadsServed`;
    - `GarbageUnreferenced`: an `'E'` only for a zero count;
    - `HolderListed`: a positive count under `A` implies a list under `A`.
  - **Controls:**
    - `nc-sweeper-one-holder`;
    - `nc-release-before-zero`;
    - `nc-no-version-grace`;
    - `nc-merge-drops-sibling`;
    - `nc-list-after-cas`;
    - `nc-rank-ignores-home` → ReadsServed;
    - `nc-list-missing-holds-nothing` → NoDeleteWhileReferenced;
    - `nc-count-foreign-half` → RefcountExact.
  - **Witnesses:** `Witness_AncestorDeleted` (after both releases), `Witness_RetiredLsmDeleted`.
- **TLA-045 merge arm** (with package 5).
  - **The model:** the shared state machine's second operation, with background writers as actors the
    gate blocks, the override's lapse and a home owner's open as actions, the source fence at open, and a
    lease-loss takeover in every state. As S4 rev 2 §12: **the lease does not fence** (`LeaseLapse` and
    `Claim` stay enabled while `SlowActorStep` continues), and **the claim is an action of `Open`**, enabled
    only after `Open` bumped the writer epoch; for a merge, after both sources' `Open`.
  - **Properties:**
    - `SingleOwner`;
    - `KeysPreserved`;
    - `NoGhostLeaf`;
    - `AckedReadable`: every acked record is readable from exactly one leaf after any crash;
    - `OperationCompletes`: liveness under the lock order.
  - **Controls:**
    - `nc-merge-serves-after-T`;
    - `nc-cas-without-T`;
    - `nc-takeover-deletes-live`;
    - `nc-writer-outside-gate`;
    - `nc-serve-sibling-under-intent` → AckedReadable;
    - `nc-claim-without-fence` (the claimant decides without opening the sources) → ChildrenExact /
      ForwardAfterRetire;
    - `nc-claim-fences-one-source` (rev 5: a merge claim opens one source only) → AckedReadable /
      RetiredNeverServes;
    - `nc-delete-successor-intent` (the intent deleted rather than CASed terminal) → ForwardAfterRetire;
    - `nc-actor-reopens-after-fence` → OperationCompletes;
    - `nc-reuse-attempt-path` → KeysPreserved;
    - `nc-gate-across-committer` → OperationCompletes;
    - `nc-sentinel-in-projection` → KeysPreserved.
  - **Witness:** split, merge, split.
- **Kani** (an S6 submodule beside package 5's `src/topology/proofs.rs`, so their receipts stay apart):
  - `refcount_merge_sums`: over all two-row inputs, the merged `'R'` row is the exact sum, the earlier
    creation and the one length; a zero sum is absent; two lengths, or a sum that would overflow, refuse
    the merge. Its controls are wrapping addition and the later creation.
  - **Departure (lead, 2026-11-10).** The two harnesses rev 5 also planned are dropped:
    - `lineage_rule` is covered at model level by TLA-019's LineageGC arm: sweeper-one-holder,
      release-before-zero, list-after-CAS, list-missing-holds-nothing and three more controls, with two
      witnesses;
    - `rank_total_across_homes` restates Rust's tuple order, which KANI-105 already uses for
      `(home, epoch, seq)`.
    `refcount_merge_sums` stays, because refcount arithmetic is where a GC bug becomes data loss, and Kani
    proves the sum and its overflow over every input. Its manifest entry joins the S4/S6 push's other
    formal changes (TLA-045 and so on) in one edit, so CI runs the full formal pass once;
  - the row-range harness extended with "no sentinel key in any sibling's entity range", the top prefix
    included.
- **Re-recorded:** TLA-016/018/019 as staled; KANI-099/105 with S3a's codecs.

## 9. Tests

**Red first, unit:**
- `'R'` sums per `(home, id)` at a merge (exact rows; a zero sum absent);
- the fresh path per attempt (a reborn or retried prefix gets `-<home>`);
- the range filter (§3.1): a two-half object rewritten for exactly the child's half, its two streams'
  hashes minted through `keyspace::entity_hash` from a route inside the child's range and one outside it
  (rev 6: `clean/tests.rs::as_child_of_range_0` and `merge_tests::a_childs_merge_keeps_only_its_own_ranges_streams`
  stop planting raw hashes as routes, and one case of each creates its streams through the registry on a
  cell with bit-prefix shards `"00" "01" "10" "11"`);
- shard ownership end to end: package 4's `shard_ownership` DST (§3.1 red test 1), with S6's merge and
  cleaner passes on every shard;
- the release split on `home` on each of the four decrement paths (an ancestor zeroing writes `'E'`, never
  `'G'`; an own zeroing the reverse);
- the release log's and snapshot's codecs and names (`<epoch>-<seq>` from 0);
- the derived `all` (no `'R'‖A`, `'I'‖A` or `'E'‖A` key → marker; one key → none);
- the sweeper's rule edges:
  - a positive count blocks deletion;
  - an absent list entry allows it, and so does a logged release;
  - a version younger than `LINEAGE_GRACE` on the sweeper's own clock blocks all;
  - a source under an unresolved intent blocks all;
  - a list of a non-leaf home is ignored and deleted;
- the open-time detector (§3.3);
- §5's preconditions (a checkpoint, an `external_dbs` entry, the grace: each blocks);
- the forced rewrite and forced re-tile at depth 5 versus 4;
- the sentinel rebuild per tag and the `SENTINEL_TAGS` enumeration;
- the entity range at the top prefix (fork-level, with §12 item 4);
- the gate per writer (batch and PUT), `request_purge`'s `Busy` per caller, and the lock order;
- the merged leaf at epoch 1 rewriting an epoch-7 ancestor object: the copy wins every tie;
- `live_entries`' "two chunks in one object" check keyed on the full id with `home`.

**DSTs** (new files ≤ 1,000 lines each, `#[path]` lines, owners.json rows):
- `topology_merge.rs`:
  - the crash matrix of §7.4, with exact end states (object listings, rows, topology, intent, checkpoints
    on both sources and on the parent);
  - **crash around retirement and publication** (review), including the override lapsing between the two
    `'T'`s while the un-retired source's home owner tries to open it: it is fenced (L8), and every acked
    record is readable from `M`;
  - **merge during fold, cleaner, trim, purge and settlement** (review), each writer mid-batch at the
    quiesce: its batch lands wholly before the snapshot's delta (and is in `M`) or is refused on a closed
    source, never between. The purge case has its committer step in flight;
  - `request_purge` from the DELETE path during the merge: `Busy`, then purged by the walk after
    publication;
  - a merge racing a split of a sibling;
  - a merge under appends to both: every held append answered once, acked at its offset or `503` and then
    acked on retry;
  - **split, write both, merge repeatedly with old payloads live** (review): five rounds, every record of
    every round read back at its offset after each round; after the last grace the object listing equals
    the live objects;
  - no checkpoint left on either source or the parent after a rollback;
  - every merge DST runs on a store wrapper that refuses DELETE of `topology/intent.json` (rev 5);
  - a lapsed merge claimed by an opener of `…0` while the slow actor still holds `…1`: the claimant fences
    `…1` before deciding, and the actor's late `'T'` into `…1` fails (rev 5);
  - recovery from `'T'` alone with the intent terminal: the opener rolls forward and `M` serves;
  - after a rollback, `effective_owner(…1)` is the ring pick within one tick (the merge's override removed,
    S6-b).
- `lineage_gc.rs`:
  - exact object listings across a cycle (storage equals live bytes after the grace);
  - a reader pinned through the release;
  - the takeover GC after a crashed actor;
  - a retired LSM deleted after the grace and not before;
  - two holders releasing in turn;
  - a hand-deleted parent checkpoint changes no listing (L7);
  - a missing list makes the leaf refuse to serve (§3.3).
- `seal_convergence` run on a child.
- The combined 24 h soak gains a split/merge cycle per day as its lineage leg (S7 owns the measurement).

## 10. Edge, measurements, costs

- **Edge.**
  - E1 (the pause) covers the merge pause and the source fence's `503` at open. If the merge exceeds the
    hold, E1 gets the merge's figure.
  - E3: the merge route goes live, and its `501` line is removed in the same commit.
  - The DELETE path's answer is unchanged (§7.1).
  - The operator counters (§11) are additive under E6.
- **Measurements:**
  - storage over a split → merge cycle;
  - the merge pause on Tigris (round trips and wall time), before E3;
  - the idle cost between a merge and the last release;
  - the down-slope of the ramp (64 → 1);
  - the forced-rewrite drain rate.
- **Costs (estimates):**
  - release logs ≤ $0.16/month per busy leaf per ancestor while releasing;
  - the sweeper < $0.001/month per retired home;
  - a forced cycle ≈ $1 per 100 GB;
  - retired-LSM deletion: one DELETE per file, once.

## 11. Operator counters (additive, under E6)

`seal.gc` gains:
- `lineage_released`, `lineage_logs`, `lineage_pending`;
- `lineage_list_missing` (pages);
- `lineage_depth_max` (pages above max + 1);
- for the sweeper: `lineage_swept`, `lineage_swept_bytes`, `retired_lsm_deleted`;
- **retired-home GC** (rev 5): `topology_retired_homes_pending_gc` (S4's gauge: retired homes whose objects or
  LSM are not yet reclaimed; S6's first sweeper commit drains it and owns it from then), `retired_homes_gone`
  (a counter: homes fully reclaimed, `lineage/` deleted), and `retired_home_oldest_pending_secs` (pages above
  24 h: a retired home the sweeper cannot finish, typically a long-lived stream's chunk the cleaner has not
  yet copied, or a checkpoint that never detaches).

## 12. Agreements, open points, what is unverified

1. **S3a (package 4): agreed.**
   - Absolute `home: u32`, as in §2.1; S3a seeds homes as the initial shards' indexes.
   - Tags `'C'` and `'E'` reserved. Rev 4 unreserves `'A'` (told to package 4).
   - The range filter in `fold.rs`'s `kept_entries` is S6's (confirm with package 4, since S3a touches
     the same function).
2. **S4 (package 5): agreed.**
   - The reference lists before the CAS, the checkpoint deleted last, `homes[]`, the intent reserving
     homes, the shared gate, state machine and takeover, and the sentinel helper.
   - **Rev 4 adds, from the review**, all of them shared and so S4's to build once:
     - the source fence at open (L8);
     - `request_purge` behind the gate with `try_acquire`, and the lock-order rule;
     - a fresh home and path per attempt by the home-counter CAS;
     - both checkpoint kinds in the takeover GC;
     - source engines closed at retirement;
     - one intent CAS outside the pause rather than per state;
     - the snapshot-plus-delta pass in the shared helper;
     - a child's `'R'` defined per range, which S4's copy rule already computes.
   - **Confirmed by package 5** (2026-11-03): all eight, built once in S4 and used by both operations. The
     fence runs in the open path every owner, takeover and override uses. S4-DESIGN.md will tabulate the
     durable witness per state for the split, and will count the split's pause the same way (one checkpoint
     CAS per `external_dbs` entry).
3. **The fork test** of a two-source union of two projected clones of one parent, the top prefix
   included: the first commit, before any merge code.
4. **Unmeasured:** the merge pause on Tigris (§7.2: ≈ 1-2.2 s, possibly over the hold); the delta's size
   in practice; the sweeper's costs.
5. **Constants:** `LINEAGE_GRACE` = `grace` (10 min, ASM-HSEG-GRACE); `RETIRED_GRACE` 1 h,
   `LINEAGE_MAX_DEPTH` 4, `RELEASE_LOG_MAX` 4,096 and the forced budget ×4 stay provisional until S6's cycle
   measurement.
6. **S3b:** its trim runs inside the committer's batch (the barrier covers it, and the `'E'` branch applies)
   and its expiry is a gated background writer, never both for one batch. S3b confirms its `'Q'`, `'D'`
   and `'N'` rules. S3b rev 2 (§7.3-7.4) adds `'H'` state Trimming and per-entity `'k'` intervals: the
   trim's decrements go through `stage_decrements`, so the `'E'` branch covers them; the merge unions
   `'H'` and the projection carries `'k'`; the cleaner's Trimming changes (`live_set`, `still_live`,
   no joining of queue chunks, the queue horizon) are in package 6's `clean.rs`, composed after the range
   filter. Agreed with the queue session (2026-11-03; S3b-DESIGN §15.2, part 2 commit 4): package 6 writes
   the cleaner change to S3b §7.4's text once the `'k'` codec lands (part 2 commit 3) and the queue session
   reviews it. The queue side exposes `intervals(hash)` (`hash`: the queue's entity hash, `keyspace::entity_hash`, the same value its `'k'` rows and the cleaner's table entries carry) (read under `refs` on every `still_live` re-check,
   never cached) and `chunk_horizon(hash, f, l)` (the latest `hard_expires_at` among the chunk's `'d'` rows,
   or `now + 2×retention + failure_retention` for an `sbits`-only chunk), so `clean.rs` never decodes queue
   rows. Red tests include a trim adding an interval between the copy's PUT and the lock (abandoned, exact
   rows and counts).
   **The purge order (S3b-REVIEW F4), package 6's `purge.rs`:** a queue's purge reads its `'k'` intervals
   under `refs` before `delete_stream_rows` and before any decrement (today `reclaim_locked` deletes the
   stream's rows first, `purge.rs:242`, then reads and decrements, `:245-277`); decrements skip chunks below
   `reclaimed` or inside an interval; the `'k'` rows are deleted only in the batch of the last decrement;
   `delete_stream_rows`'s prefix excludes `'Q'` (S3a §2.3). Otherwise a chunk the trim already decremented
   is decremented again, an object shared with another stream reaches zero early, and the neighbour's
   chunk is deleted. Package 6 writes it with the cleaner change (same hand-off, after the `'k'` codec);
   red test as F4 states (a two-stream object, one chunk retired by the trim, the queue deleted: the other
   stream's chunk readable after the purge, exact `'R'` row); QueueReclamation control 8.

## 13. Work plan (one behaviour per commit, red first)

1. The fork test (§12 item 3).
2. The range filter (§3.1). Landed on slate as 0c782ccd6 (CI_GREEN), but it compared the prefix with the
   engine identity. **On slate the filter is withdrawn until layout 8** by package 5's hotfix. It re-lands
   with package 4's commit "A row's hash places it on its shard", with the two child tests rewritten
   through `keyspace::entity_hash` and the `shard_ownership` DST. `Range` itself is kept and moves to
   `keyspace::range` (4a commit 1b).
3. The release split on `home` (`'E'`, logs, snapshots, derived `all`) and the open-time detector, on S3a's
   codecs, with `lineage_gc.rs`'s two-holder case on planted rows. **State 2026-11-08** (local `s6-step3` on
   package 4's unpushed 4a stack, over 0beb59d43):
   - a45d95aab: the child range tests mint through the registry;
   - d9ca4bd97: `'E'` and `stage_unreferenced`, which also closes a homeless-`'G'` collision in the
     merge's retirement;
   - 4fd048f80: per-home release logs and the `all` marker.
   Two pieces are moved:
   - the hourly snapshot moves to step 4, with the sweeper that reads it;
   - the open-time detector (§3.3) moves to S4's `'T'` refusal path. Its trigger, rows of a foreign home,
     exists only after a split, and its refusal ("409 after a topology refresh, like a `'T'`d engine") is
     S4's mechanism.
4. The lineage sweeper and the retired-LSM deletion, with the TLA-019 two-holder arm and the
   `lineage_rule` Kani harness.
5. The forced rewrite and forced re-tile (§6).
6. Merge (§7) on S4's state machine, gate, fence, intent and helper. The commit carries the `'K'` union's
   equal-values assert, with its two cases (K-SPLIT-DECISION §3):
   - both sources hold one copied parent-home `'K'` key with equal values, and `M` holds it once,
     byte-identical;
   - a planted differing value makes the merge refuse, the counter increments, and `M` gets no row.
   The merge also has: the snapshot and delta, the entity
   union, the rebuild table, takeover, the route, `topology_merge.rs`, the TLA-045 arm,
   `refcount_merge_sums` and `rank_total_across_homes`; E1 and E3 amended; the pause measured before the
   `501` goes.
7. The cycle measurement and the constants' final values.

Steps 2-5 land before S4's split exists, behind planted rows, since they are reachable only once a lineage
exists. Step 6 follows S4 in the lock order.

## 14. Review findings → this revision

| Finding | Severity | Where taken |
|---|---|---|
| 1 source served between the two `'T'`s | CRITICAL | L8, §7.4 (fence at open), TLA-045 `nc-serve-sibling-under-intent`, override-lapse DST |
| 2 cleaner counts the sibling's half | HIGH | L2 per range, §3.1 filter in four places, two-half test, `nc-count-foreign-half` |
| 3 `request_purge` outside the gate | HIGH | §7.1 table, `try_acquire` → `Busy`, the walk's fallback, a test per caller |
| 4 gate across a committer op | MEDIUM | §7.1 lock order, exclusive before barrier with a deadline, purge-in-flight DST, `nc-gate-across-committer` |
| 5 attempt reuses home and path | MEDIUM | §2.2 home-counter CAS per attempt, `-<home>` paths, non-leaf lists deleted (§4 step 5), `nc-reuse-attempt-path` |
| 6 fork's final checkpoints leak on rollback | MEDIUM | §7.4 takeover GC from `path(M)/manifest`, DST |
| 7 cross-node clock on V's age; LIST consistency | MEDIUM | §4 step 1 own monotone clock; ASM-OBJSTORE-LIST in §1 |
| 8 pause under-counted | MEDIUM | §7.2 re-count (17-22 trips, 1-2.2 s), snapshot-plus-delta default, intent CAS outside the pause, measure before E3 |
| 9 ancestor ridx never released | MEDIUM | §6 forced re-tile, red test |
| 10 missing list deletes silently | MEDIUM | §3.3 detector, `nc-list-missing-holds-nothing` |
| 11 `'W'`/`'M'` missing | LOW-MED | §7.3 (already in rev 3's last edit), and why sealing is not needed (§7.2 step 1) |
| 12 `'A'` counter error | LOW-MED | `'A'` dropped, `all` derived (§2.1, §3.2) |
| 13 sources' tasks run after release | LOW | §7.2 step 3 closes both engines |
| 14 L7 unwritten | LOW | L7, DST |
| 15 log naming and count | LOW | §2.3 `<epoch>-<seq>` from 0, `RELEASE_LOG_MAX`, hourly snapshots |
| 16 seed batch size | LOW | §7.2 step 2d, 4 MiB chunks, `'Y'` last |
| 17 forced drain bound | LOW | §6 rate, ×4 budget, S5 cap, `lineage_depth_max` |
| 18 forks' cross-shard objects; L1's lineage writes | LOW | L3 and L1 sentences, fork test |
| 19-21 owed tests and controls | DONE | §8 (`nc-rank-ignores-home`, `nc-sentinel-in-projection`, `rank_total_across_homes`), §9 |

Material protocol change? No. The fence at open, the per-attempt home, the snapshot-plus-delta pass and
closing the sources each tighten a step of rev 3's protocol without changing its shape: the states, the
roll-back/roll-forward line, the deletion rule and the release path are unchanged.

## 15. Rev 5 delta (S4 rev 2 and S4-REVIEW, for Fable with S4 rev 2's check)

| # | Change | Where |
|---|---|---|
| 1 | One cell-wide intent `topology/intent.json` replaces rev 4's per-prefix intent; created only when absent or over a terminal one; never deleted; `Reclaimed`/`RolledBack` as ETag CASes with `lease_epoch`; a DST store wrapper refuses its DELETE | §7.2, §9 |
| 2 | `'T'` = `(version_seen + 1, merged_home, op_id)` alone rolls forward; `path(M)` from the home | §7.2, §7.4 |
| 3 | Only an opener of a source claims, after its open fenced the actor; the fleet tick only reports; **a merge claim fences both sources before deciding** (new for two sources; **taken into S4-DESIGN rev 2 §4.3** by package 5: the shared claim path opens every source the intent names, a source this instance does not own by a maintenance open that never serves, 'T' in any → forward, none → back) | L8, §7.4 |
| 4 | The actor stops before `'T'` on a failed `owned()` (renewal, either source fenced, a fence error) and never after; it never re-opens a fenced source | §7.2, §7.4 |
| 5 | The intent as a move reservation: `reserved_shards: 1`, return-home and the rebalancer skip both sources, overrides refused except the merge's own | §7.2 |
| 6 | C4 `CloneBuilder::with_seed`: `M` initialized with its sentinel rows in the clone; no open of `M` and no seed batch in the pause; fallback = S4's maintenance open | §7.2, §7.3 (`'Y'`) |
| 7 | No fold in the pause: `'K'` unioned, a key in both sources with equal values asserted and landed once, as `'I'` (K-SPLIT-DECISION-2026-11-09; the rev 5 "disjoint by home" wording is withdrawn); S3a §2.8's merge column changes for `'K'` (package 4 told) | §7.2, §7.3 |
| 8 | Checkpoints as S4: `topo-<op_id>` per source, `checkpoint(All)`, 1 h lifetime, after `flush()`; deleted at Reclaimed | §7.2 |
| 9 | Pause re-counted with C4: ≈ 32-37 round trips (≈ 0.8-1.5 s); fallback ≈ 50-57 (≈ 1.3-2.3 s) | §7.2 |
| 10 | Retired-home GC counters: `topology_retired_homes_pending_gc` (S4's gauge, owned by S6's sweeper), `retired_homes_gone`, `retired_home_oldest_pending_secs` (pages above 24 h) | §11 |
| 11 | Formal: the lease does not fence; the claim is an action of `Open`; controls `nc-claim-without-fence`, `nc-claim-fences-one-source`, `nc-delete-successor-intent`, `nc-actor-reopens-after-fence` | §8 |

The lineage GC (§3-§6) and step 2 (the range filter) are unaffected by rev 5. Rev 6 changes step 2: see
§3.1 and §13.

**Fable's check of rev 5 (S4-REVIEW-rev2.md, S6 section): PROCEED.** The two-source fence and the pause count
are confirmed. Taken since: S6-a (open order: owned source first, others closed promptly after the CAS),
S6-b (the merge's override removed at `RolledBack` and at publish, with a DST), S6-c (`'K'` reworded: each
source's keys carry its own home; no own-only assert on `'K'`, asked of package 4), and N1 (the never-leaf
child-path sweep is package 6's, §4 step 5, on S4's path contract and counter).
