# F1: a routing key's producer and Stream-Seq lanes must follow it to another engine

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `eda79ccf` (= origin/slate). Read-only
investigation: nothing was edited and no cargo command was run. Every "expected output" below
comes from tracing the code. None of it was observed. The implementer records the observed text
in each commit message.

**Prerequisite.** The split-boundary harness is not on slate yet. It exists as `8a2791b0`, and as
`706430f7` on `worktree-wf_e38ad9fe-ae6-3`. The two differ only in two scenario bodies: 706430f7
turns the two `assert!(split, ...)` into `eprintln!`. The F1 reds are identical in both. The
branch applies cleanly on `eda79ccf`: `git merge-tree --write-tree eda79ccf bc2907f7` exits 0.
This plan assumes one of the two harness commits lands first (decision D8). Line numbers for
`src/dst/tests/split_boundary_outcomes.rs` refer to 8a2791b0.

**One-line summary.** The fix places the key's sealed predecessors when the append is submitted
and the serving engine is known. A predecessor on the serving engine is still read by the
committer, by identity. For a predecessor on another engine, `submit` reads its lane rows on that
engine before enqueue, and they travel in the request. A sealed predecessor's rows are frozen, so
the copy is exact. A lane the serving engine already holds never consults a predecessor.
`CommitTransaction::append` is byte-identical, so none of its four exception contracts moves. No
wire vocabulary changes in commits 1 and 2. Commit 3 is owner-gated and adds a fleet-internal
lane read for predecessors owned by another instance.

---

## 1 Problem (verified on eda79ccf)

### 1.1 The mechanism, quoted

**(a) The lineage is a list of identities. Nothing records where each predecessor's rows live.**
`src/application/append.rs:315-329` (inside `execute_once`):

```rust
    let producer_lineage: Vec<[u8; 16]> = match &desc.segments {
        Some(map) if map.segments.len() > 1 => {
            let mut preds: Vec<&crate::segmap::SegmentDesc> = map
                .segments
                .iter()
                .filter(|sg| sg.seg_id != seg.seg_id && sg.contains(seg.point) && !sg.is_live())
                .collect();
            preds.sort_by_key(|sg| std::cmp::Reverse((sg.created_ms, sg.seg_id)));
            preds
                .into_iter()
                .map(|sg| desc.dynamic_segment_identity(sg.seg_id))
                .collect()
        }
        _ => Vec::new(),
    };
```

**(b) The committer reads every identity in the chain from the serving engine's own database.**
`src/shard.rs:2022-2056`:

```rust
    async fn load_seq_chain(&self, own: &[u8; 16], lineage: &[[u8; 16]], key_hash: &[u8; 16])
        -> Result<Option<String>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(seq_key(identity, key_hash)).await? {
                return decode_seq_row(&v).map(Some);
    ...
    async fn load_producer_chain(&self, own: &[u8; 16], lineage: &[[u8; 16]], key_hash: &[u8; 16], pid: &str)
        -> Result<Option<(u64, u64, u64, [u8; 16])>, slatedb::Error> {
        for identity in std::iter::once(own).chain(lineage.iter()) {
            if let Some(v) = self.db.get(producer_key(identity, key_hash, pid)).await? {
                return decode_producer_row(&v).map(Some);
```

The only caller is the committer, `src/shard/transaction/append.rs:36-39` and `:111-114`:

```rust
                    None => match self
                        .engine
                        .load_producer_chain(&hash, &req.producer_lineage, &req.key_hash, &pr.id)
                        .await
    ...
                    None => match self
                        .engine
                        .load_seq_chain(&hash, &req.producer_lineage, &req.key_hash)
                        .await
```

**(c) Phase B always places the high child on a different engine.** `src/application/topology.rs:494-508`:

```rust
            let parent_prefix = crate::registry::shard_for_hash(&prefixes, &low_route);
            let mut high_route = [0u8; 16];
            for salt in 0u32..16 {
                high_route =
                    crate::crypto::RouteHash::for_child(&stream, child_id, &salt.to_be_bytes()).0;
                if prefixes.len() < 2
                    || crate::registry::shard_for_hash(&prefixes, &high_route) != parent_prefix
                {
                    break;
                }
            }
```

A merge publishes its child on the low parent's route. See `resume_merge`, `:585`:
`let Some(child_route) = d.segment_route_by_id(a_id)`. The other parent's rows therefore stay on
its own engine.

**(d) The append is served by the engine of the child's route.** In `src/registry.rs:761-764`,
`resolve_segment` sets `shard_route = self.segment_route(seg)` and
`identity = self.dynamic_segment_identity(seg.seg_id)`. In
`src/application/append/submit.rs:18-25`,
`state.shards.resolve(&seg.shard_route, Adoption::External)` resolves it.

**(e) With no row, the lane counts as never having committed.** `src/shard/commit_plan.rs:131-136`:

```rust
    } else if request.seq != 0 {
        return reject(AppendErr::ProducerGap {
            expected: 0,
            received: request.seq,
        });
    }
```

For Stream-Seq, `transaction/append.rs:127-129` finds no current value, so any sequence is
accepted.

### 1.2 Causal trace of the two existing reds

The rig has four prefixes (`00/01/10/11`, see `Scenario::start`). Both `ga` and the empty key
satisfy `key_point(..) >= SPLIT_AT`, and the harness asserts this.

1. **Before the split.** `desc.segments` is `None`. `resolve_segment` returns seg 0 with
   identity `storage_hash()` and route `RouteHash::for_stream`, which is prefix P. Lineage `[]`.
   The committer on P accepts the append. It writes `producer_key(storage_hash, kh("ga"),
   "f1-high")`, or `seq_key(storage_hash, kh(""))` = `"0002"`, into **P's** database.
   `accept_append` in `transaction/append.rs:179-182` and `:239-240` are the only writers of
   these rows.
2. **`execute_split(0, SPLIT_AT)`.** Phase A records the intent. The seal is an empty close on P.
   Its ack is a `DurableEffects` ack (`transaction/maintenance.rs:126`), so it arrives only once
   the close is durable. Phase B publishes seg 1 on P's route and seg 2 on `high_route`, which is
   prefix Q ≠ P.
3. **The retry.** `resolve_segment("ga")` returns seg 2 with identity `for_segment(..,2)` and
   route Q. The lineage is `[storage_hash]`. `submit` resolves engine Q. In Q's committer the
   overlay misses, the shared state misses, and `load_producer_chain(seg2_id, [storage_hash])`
   reads **Q's** database for both identities and finds nothing. `decide_producer(current=None,
   seq 0)` accepts, and a second record is committed.
   - `a_producer_retry_after_a_split_is_recognised_on_the_high_childs_engine` (`:800`) fails with
     `the high child accepted a parent-committed producer retry as new: 200 - ra=- (2 copies)`.
     The format is `Answer::show()`, `"{status} {code} ra={ra}{dup}"`, with no error code, no
     retry-after and no duplicate.
   - `a_stream_seq_lane_after_a_split_refuses_a_regression_on_the_high_child` (`:819`) takes the
     same path through `load_seq_chain`. `"0001"` is accepted and the raw answer is 204:
     ```
     assertion `left == right` failed: the high child accepted Stream-Seq 0001 after the parent's 0002: 
       left: 204
      right: 409
     ```
4. **The control.** `product_producer_hash_survives_split` (`producer_protocol.rs:584`) passes
   because `http_rig` has one prefix. Both children share P's database, so the same loop finds
   the row.

### 1.3 Further consequences, verified by reading and not yet run

The mechanism is not limited to exact retries. Every one of these follows from 1.1(b) and (e):

- **A continuing producer breaks permanently.** A producer commits seq 0 on the parent. After
  the split its next append, seq 1, reaches Q with no row and gets `ProducerGap{expected 0,
  received 1}`. That is 409 `producer_gap` on the product surface, 409 `producer_seq_gap` on raw,
  and not retryable. The producer is stuck until it bumps its epoch. Automatic splits happen on
  hot streams, so this hits live producers, not only retries.
- **The request-hash conflict is lost.** Same tuple, different body: this should be 409
  `producer_sequence_reused`, as the one-prefix control shows. It is accepted as new instead.
- **Merge.** Merging seg 1 (P) and seg 2 (Q) puts seg 3 on P. For a key in seg 2's range the
  chain is `[seg2 (Q), seg0 (P)]`. P's database has no seg 2 rows, so the committer falls through
  to the **farther** seg 0 row. The wrong row decides: a Stream-Seq `0003` already accepted on
  seg 2 is accepted again, and a producer lane regresses to its pre-split state.
- **Repeated splits.** Seg 2 splits into seg 3 (Q) and seg 4 (R ≠ Q). The chain is
  `[seg2 (Q), seg0 (P)]`. If R = P, today's code reads seg 0's row locally but never seg 2's, so
  again the farther row decides.
- **Multi-instance.** In a fleet, the ring usually gives P and Q different owners. That is the
  common production case. The rows are then on another process, and no fix inside one engine
  reaches them (commit 3).

### 1.4 Full use-site list (eda79ccf)

| Symbol | Definition | Production uses | Test uses |
|---|---|---|---|
| `AppendReq.producer_lineage` | `shard.rs:785` `pub producer_lineage: Vec<[u8; 16]>` | written: `application/append.rs:315-329,364`; `application/creation/initialization.rs:103` (`Vec::new()`); read: `shard/transaction/append.rs:38,113` | `Vec::new()`/`vec![]`: `dst/runtime.rs:207,282`, `dst/tests/fixture_storage.rs:221,284`, `durability_fences.rs:415,707`, `reads_history.rs:194,301,424`, `history_absorption.rs:239`, `read_subset_retention.rs:259`, `read_page_limits.rs:156`, `history.rs:1528,1610` (cfg(test) modules), `history/controller_tests.rs:86`, `history/worker/lane_isolation_tests.rs:23`, `shard/transaction_tests.rs:62`, `shard/retirement_tests.rs:118`; **non-empty: `dst/tests/producer_protocol.rs:41`** (`producer_lineage: lineage`) |
| `load_seq_chain` | `shard.rs:2022` | `shard/transaction/append.rs:113` | `shard/transaction_tests.rs:526` (`&[]`), `shard/storage_decode_tests.rs:203,209(&[]),213` |
| `load_producer_chain` | `shard.rs:2043` | `shard/transaction/append.rs:38` | `shard/storage_decode_tests.rs:161,167(&[]),178` |
| `producer_key` / `seq_key` | `shard.rs:189` / `:181` | reads `shard.rs:2051` / `:2029`; writes `transaction/append.rs:180` / `:240` | storage/transaction tests |
| `decode_producer_row` / `decode_seq_row` | `shard/lane_rows.rs:15/49` | imported `shard.rs:38`, used `:2052` / `:2030` only | `storage_decode_tests.rs:243,249-252,271,279,289-290` (through `use super::*`) |

The only production `AppendReq` constructors are `application/append.rs:356` and
`application/creation/initialization.rs:96`. A new stream has no predecessors.

### 1.5 The invariant the fix relies on (already true)

A predecessor in `desc.segments` is `!is_live()` only after Phase B's CAS. Phase B runs only
after `seal_segment_identity` returned the frozen offset (`topology.rs:428-430`). That return is
the close's durable ack. After the close is applied, the parent identity can write no lane row:
`decide_producer` answers duplicates without writing, and new work reaches
`if local.fields.closed { ... Closed; return }` (`transaction/append.rs:71-95`) before
`accept_append`. So a sealed predecessor's producer and Stream-Seq rows are **durable and
immutable**. A read taken at any time after publication is exact, including one taken outside
that engine's committer. Absence is immutable too.

### 1.6 What F1 is not

F1 is not the release-hold 500. That 500 is the absorber double retirement (`ca80f9d4`,
`a_rescan_during_an_inflight_advance_never_fails_an_append`) and belongs to
`absorber-double-retirement.md`. F1 never answers 500. It commits duplicates, accepts a Stream-Seq
regression, or refuses a legal next sequence. Closing F1 does not close the hold.

---

## 2 Contract decision

**Contract, restoring ROUTING-V3 §7 as written.** A routing key's producer lane and Stream-Seq
lane are decided by the nearest row among the serving segment's own identity and its sealed
predecessors, nearest first. This holds **wherever that predecessor's rows live**. When the
nearest row cannot be read, the append is refused retryably with nothing committed. It is never
decided as if the row were absent.

**Design chosen: lazy carry at submission.** `submit` already holds the resolved serving engine.
It then places the chain:

1. The request has neither a producer nor a Stream-Seq. The committer never walks the chain, so
   `Vec::new()` is returned with no I/O. Cost: none, and slightly less than today, because the
   predecessor list is no longer built for every append.
2. Every predecessor's route maps to the serving prefix. The result is `Local(identity)` links,
   exactly today's chain, with no I/O. This covers low children, single-prefix cells and streams
   that never split.
3. Some predecessor is foreign. First read the **serving engine's own** rows for the requested
   lanes: one point read per lane, off the committer. If each requested lane has an own row, the
   own row decides and the chain is `Vec::new()`. This is the steady state, and it never touches
   the predecessor's engine.
4. Otherwise, predecessors on the serving prefix stay `Local(identity)`. Each foreign one becomes
   `Carried(LaneRows)`, its rows read raw (undecoded) on its engine. The committer walks own, then
   links, in order. A carried row is decoded by the same decoder as a stored one, so corruption
   stays corruption (`edge-changes.md` #30) and is never read as absence.

Races are safe. An own row written after the pre-check is nearer and wins in the committer. An
own row is never removed while the incarnation lives. Carried rows are immutable (1.5).
`CommitTransaction::append` does not change: its call texts still pass `&req.producer_lineage`,
and only the element type changes.

**Rejected alternatives.**

- *Eager copy at Phase B.* The coordinator would scan the parent's lane rows for the high range
  and write them into the child's engine before publication. The work is O(parent lanes) while
  the parent is sealed and its range answers 503 `segment_transition`, so the unavailability
  window of a hot stream grows. It needs a scan relay and a write relay across instances.
  Merges and repeated splits must flatten chains. Its one advantage is zero cost in the steady
  state for per-request-unique producer ids (D10).
- *Read-through inside the committer.* The committer would await another engine's open, with an
  open wait of up to 10 s, or a peer HTTP call, inside the serial commit loop. That stalls the
  whole shard.
- *Fetch the predecessor on every append (no pre-check).* This costs one relay per producer
  append in fleets, and keeps an idle parent engine re-opening under the sweep.
- *A zero-I/O pre-check through the resident handle's `state` mutex.* This needs a new
  `lock().unwrap()` outside the committer, which is exception growth, or a `lock().ok()` poison
  policy. See D4.

**What changes at the edge.** No status code, error code, header or body shape is added in
commits 1-2. Outcomes change, and each change restores a documented answer. This is still a
client-visible edge change, so it needs owner approval (D1):

| Condition: split or merge child on another engine than a predecessor | Before (eda79ccf) | After |
|---|---|---|
| exact producer retry of a parent-committed append | raw 200 (new commit, second copy); product 200 `duplicate:false,count:1` | raw 204 (duplicate, `Producer-Epoch/Seq` echo); product 200 `duplicate:true,count:0`; one copy |
| producer's next sequence after a parent commit | 409 `producer_seq_gap` / product 409 `producer_gap`, not retryable | 200, committed once |
| same tuple, different body (product) | 200, second copy | 409 `producer_sequence_reused` |
| Stream-Seq at or below the nearest predecessor's | 204 accepted | 409 `seq_conflict` |
| first sight of a lane whose foreign predecessor's rows are unreadable (engine opening, held off, closing, open failed, read error) | decided as if absent | 503 existing vocabulary: gate code with its retry-after (`shard_moving`/`shard_closing`/`shard_opening`), or `segment_transition` retry-after 1; product `temporarily_unavailable`, retryable; nothing committed |

Proposed record for `docs/reviews/2026-09-hardening/edge-changes.md`, for owner approval, graded
**medium**: "A split or merge child on another engine decides producer and Stream-Seq lanes by its
nearest predecessor". Surface both. Endpoints: raw POST `/v1/stream/{name}`; product POST
`/records`, `/records:batch` and the `:seal` final. Before/after as in the table. Retry
semantics: one success becomes a duplicate and another becomes a 409 conflict; a 409 gap becomes
a success; a new retryable 503 appears on first sight only. Pinning tests: section 3.

**Commit 3 (owner-gated)** adds a fleet-internal read, `GET /v1/internal/segment-lanes/{*name}`,
listed in WIRE-MATRIX §3. The public edge is unchanged.

---

## 3 Red tests, pins and non-vacuity controls

### 3.1 Existing reds, to un-ignore in commit 2

Both are in `src/dst/tests/split_boundary_outcomes.rs` (harness commit). The expected red output
before the fix is the one given in 1.2.

- `dst::dst_tests::split_boundary_outcomes::a_producer_retry_after_a_split_is_recognised_on_the_high_childs_engine`
- `dst::dst_tests::split_boundary_outcomes::a_stream_seq_lane_after_a_split_refuses_a_regression_on_the_high_child`

The harness scenarios `appends_while_the_split_intent_is_recorded` (rows `intent-a-high`,
`intent-a-raw`) and `appends_staged_around_the_parent_seal_in_one_group` (rows
`sealgroup-before-high`, `sealgroup-before-raw`) currently print `FINDING F1: the retry left 2
copies`. After the fix those rows print `committed`, with retry `200 - ra=- dup` or `204`, and 1
copy. Commit 2 removes their `known(F1, ..)` entries, so a regression becomes a `VIOLATION` and
fails the scenario.

### 3.2 New reds (commit 1, `#[ignore]` until commit 2)

New file `src/dst/tests/split_lineage_lanes.rs`: explicit imports (no `use super::*`) and no
`serde_json::json!`, so no owners rows beyond the by-path one. The rig is `http_rig_build(mem(),
RigRuntime::first(), HttpRigOptions { prefixes: 00/01/10/11, .. })`, and the collection is created
by raw `PUT /v1/stream/{name}` with JSON content, as in the harness. Small local helpers:

- `produce(key, pid, epoch, seq, marker)`: product POST with `prisma-encryption-key`,
  `prisma-routing-key` and the three producer headers.
- `raw_seq(seq)`: raw POST with `stream-seq`.
- `copies(key, marker)`: `drain_no_closure` count.
- `prefix(seg_id)`: `shards.prefix_for(desc.segment_route_by_id(seg))`.
- `retire_parent()` and `await_reopen()`: the harness's `:503-533` helpers, copied because they
  are private to that module.
- `Answer`: status, code, retry-after, duplicate, body, with the harness's `show()`.

Each test asserts its placement first, for example `prefix(2) != prefix(0)`, so it cannot pass
vacuously on a co-located rig.

| # | Name | Steps | Expected red today (traced) | After fix |
|---|---|---|---|---|
| A1 | `a_producer_continues_its_sequence_on_the_high_childs_engine` | `produce(ga,"p",1,0,"p0")`→200; split(0,SPLIT_AT); `produce(ga,"p",1,1,"p1")` | `the high child refused the producer's next sequence: 409 producer_gap ra=-` (details `{"expected":0,"received":1}`) | 200 `duplicate:false`, `copies(p1)==1` |
| A2 | `a_reused_producer_sequence_conflicts_on_the_high_childs_engine` | `produce(ga,"p",1,0,"x")`→200; split; `produce(ga,"p",1,0,"y")` (different body, so different request hash) | `a reused sequence with a different body must conflict through the parent's row: 200 - ra=-` | 409 `producer_sequence_reused`; `copies(y)==0` |
| B | `a_merged_child_refuses_a_stream_seq_its_high_parent_accepted` | `raw_seq("0002")`→204; split; `raw_seq("0003")`→204 (seg 2); `execute_merge(1,2)`; assert `prefix(3)==prefix(0)`, `prefix(2)!=prefix(0)`; `raw_seq("0003")` | ``assertion `left == right` failed: a merged child accepted Stream-Seq 0003 again`` left 204 right 409 | 409 `seq_conflict` (nearest = seg 2, carried) |
| C | `a_second_split_answers_a_retry_from_the_nearest_producer_row` | `produce(ga,"p",1,0,"e1")`; split(0,SPLIT_AT); `produce(ga,"p",2,0,"e2")`→200 (a new epoch starts at 0, accepted today); `at = key_point("ga")`, assert `at > SPLIT_AT`; `execute_split(2, at)`; assert `resolve_segment("ga").seg_id == 4`, `prefix(4) != prefix(2)`, print whether `prefix(4) == prefix(0)`; retry `produce(ga,"p",2,0,"e2")` | `the retry of epoch 2 seq 0 was not answered from the nearest predecessor: 200 - ra=- (2 copies)` | duplicate, `copies(e2)==1`. A reversed chain would read seg 0's `(1,0)`, accept epoch 2 as new and fail |
| E | `a_first_sight_retry_waits_for_its_retired_parent_and_commits_once` | `produce(ga,"p",1,0,"w")`; split; `retire_parent()` (FleetEviction, holdoff armed); retry; then `await_reopen()`; retry again | `a retry its parent's unreadable lanes cannot decide must be refused retryably with nothing committed: 200 - ra=- (2 copies)` | first retry 503 (product `temporarily_unavailable`, retry-after present), `copies(w)==1`; after reopen, duplicate, `copies(w)==1` |

### 3.3 Pins, green before and after

- **D** `a_lane_the_high_child_holds_never_consults_its_retired_parent` (commit 1, not ignored).
  Split; `produce(ga,"q",1,0)`→200 (first sight: the parent has no row, so the lane commits on
  seg 2 and seg 2 now owns the row); `retire_parent()`; `produce(ga,"q",1,1)` must be 200 with 1
  copy. Today the committer's shared state decides and the parent is never touched. After the
  fix the own-row pre-check decides. This pins the cost property (the steady state never waits
  on a predecessor) and kills the "always carry" mutant (NV2).
- **Unit tests** in `src/shard/storage_decode_tests.rs` (commit 2; the file is `#![cfg(test)]`).
  Both use the existing `lane_engine` and `stored_producer_row` helpers.
  - U1 `r12_a_carried_lane_row_is_decoded_like_a_stored_one`. A carried 40-byte row loads as
    `(7,3,11,[9;16])`. A carried 15-byte producer row is `Err`, and so is a carried `[0xff]`
    Stream-Seq row. A carried `b"parent-seq"` loads as `Some("parent-seq")`.
  - U2 `lineage_links_decide_nearest_first_across_local_and_carried_rows`. Setup: parent row A is
    stored locally, B is carried. `[Carried(B), Local(parent)]`→B; `[Local(parent), Carried(B)]`→A;
    `[Carried(none), Local(parent)]`→A; an own row beats both. The same four checks run for
    Stream-Seq.
- **Existing controls that must stay green:** `product_producer_hash_survives_split`,
  `producer_retries_across_a_split_commit_once`, `stream_seq_resolves_through_predecessors`,
  `producer_lanes_scoped_per_routing_key` (single-engine chains), the `r12_*` storage tests (their
  `&[parent]` becomes `&[LineageLink::Local(parent)]`), `r12_an_undecodable_*` in
  `transaction_tests`, and all nine harness phase scenarios.

### 3.4 Commit 3 reds and pins (two instances; owner-gated)

These tests go in the same new file, which is about 500 lines in total. They use
`http_rig_owner`/`http_rig_owner_at` following `read_application.rs:166-200`. A creates the
collection and commits `produce(ga,"p",1,0,"r")`. A splits. B invalidates its descriptor, runs
`set_ring_active([inst-a, inst-b])` and `set_override(prefix, parent ? inst-a : inst-b)`, and
calls `b.peer.set_peer("inst-a", addr_a)`. The test asserts `parent != child` prefixes.

- **G** `a_producer_retry_reads_its_parents_lanes_on_another_instance`: retry at B. Expected red
  after commit 2, before commit 3: `the high child on another instance accepted a
  parent-committed producer retry as new: 200 - ra=- (2 copies)`. Commit 2 keeps today's
  behaviour for `NotOwner`. After commit 3 the retry is a duplicate with 1 copy (read at B, which
  relays the parent segment to A).
- **H** (pin) `a_lane_held_on_another_instance_needs_no_relay`: at B, `produce(ga,"q",1,0)` then
  `set_peer("inst-a", "http://127.0.0.1:9")` (unroutable), then `produce(ga,"q",1,1)`→200.
- **H2** `the_lane_receiver_requires_the_fleet_credential`: a GET without a bearer answers 401
  `unauthorized` with `WWW-Authenticate`.

### 3.5 Non-vacuity controls (uncommitted; run once, quote in the commit message)

- **NV1.** In `chain()`, force every predecessor to `Local` (today's chain). A1, A2, B, C, E, the
  two harness reds and the two F1 scenario rows fail with the red texts above. D stays green.
- **NV2.** Delete the own-row pre-check, so the chain always carries. D fails:
  `... 503 temporarily_unavailable ra=1 ...` (the parent is held off). Everything else stays green.
- **NV3.** Reverse the link order in `load_producer_chain`/`load_seq_chain` (farthest first). C
  fails with 2 copies, B with 204, and U2 fails.
- **NV4.** Map a foreign `Opening` to `Ok(Local)`. E fails with `200 ... (2 copies)`.
- For the harness file's own non-vacuity (the 500 control, `fail_next_group_for`), see the
  8a2791b0 commit message. It is unchanged.

---

## 4 Edits file by file, in commit order

Ceilings: files over 1,000 lines may not grow. DST files stay at or below 1,000 lines. New
files stay at or below 1,000 lines. clippy runs with `-D warnings`. No `#[expect]` reason is
edited, and no `exception-growth.json` row is added in any commit.

### Commit 1: "F1's other shapes have red regressions: a routing key's lanes read only the serving engine's database" (tests only)

| File | Change | Lines |
|---|---|---|
| `src/dst/tests/split_lineage_lanes.rs` (new) | A1, A2, B, C, E (`#[ignore = "red regression F1: <shape>; un-ignore with the fix"]`) and D (not ignored); helpers as in 3.2 | ~330 of 1,000 |
| `src/dst/dst_tests.rs` | `#[path = "tests/split_lineage_lanes.rs"] mod split_lineage_lanes;`, placed after `split_boundary_outcomes` | 273→276 |
| ledgers | see §6 | |

No production source changes. No exception scope is touched.

### Commit 2: "A routing key's lanes follow it to a predecessor on another engine" (the fix, single instance)

| File | Change | Ceiling / scope |
|---|---|---|
| `src/shard/lineage.rs` (**new**, about 110 lines) | See the code outline after this table. | New file, no exceptions. Explicit imports: `bytes::Bytes`, `super::lane_rows::{decode_producer_row, decode_seq_row}`, `super::{ShardEngine, producer_key, seq_key}`. |
| `src/shard.rs` (3,139, may not grow) | Delete `:2015-2056` (the two loaders and their docs; 42 lines; outside every exception scope, since nothing lies between the `submit_absorbed_batch_v2` item and `maintenance_snapshot`'s own `#[expect]`). Add `mod lineage;` after `mod lane_rows;`. Replace `use lane_rows::{decode_producer_row, decode_seq_row, encode_producer_row};` with `use lane_rows::encode_producer_row;` and add `pub(crate) use lineage::{LaneRows, LineageLink};`. Change the field at `:780-785` from 6 lines to 6 lines: `pub producer_lineage: Vec<LineageLink>,`, documented as "the routing key's sealed predecessors, nearest first: one on this engine by identity, one on another engine by the lane rows read there before enqueue". | 3,139 → ~3,099. `CommitOp`'s `large_enum_variant` expect is not touched; the `AppendReq` size is unchanged, since a `Vec` is a `Vec`. |
| `src/shard/storage_decode_tests.rs` (325) | Add `use super::lane_rows::{decode_producer_row, decode_seq_row};` (explicit, because the glob no longer carries them). Four call sites `&[parent]`→`&[LineageLink::Local(parent)]` (`:161,178,203,213`). Add U1 and U2. | ~325→~400 |
| `src/shard/transaction/append.rs` | **No change.** The call texts `.load_producer_chain(&hash, &req.producer_lineage, &req.key_hash, &pr.id)` and `.load_seq_chain(&hash, &req.producer_lineage, &req.key_hash)` stay byte-identical, so the `too_many_lines`, `let_underscore_must_use`, `unwrap_used` (call and path fingerprints) and `excessive_nesting` contracts of `CommitTransaction::append` all measure identical. | untouched |
| `src/application/append/lineage.rs` (**new**, about 120 lines) | See the code outline after this table. | New, no exceptions. No `Response`/`HeaderMap`/`axum`/`AppState` (architecture gate for `src/application/`). |
| `src/application/append/submit.rs` (94) | `req: AppendReq`→`mut req: AppendReq`. Just before `if engine.try_enqueue(req).is_err()`, insert two lines: `// The key's predecessors are placed once the serving engine is known.` and `req.producer_lineage = super::lineage::chain(state, desc, seg, &engine, &req).await?;`. This runs after maintenance and wedge admission, so a shed request does no lineage I/O. | ~96 lines. `submit` stays under 100 lines; it has no exception. |
| `src/application/append.rs` (423) | `mod lineage;` in the module list (+1). In `execute_once`, delete the `:315-329` block (−15) and change `:364` `producer_lineage,` to `producer_lineage: Vec::new(), // placed by submit::submit` (±0). | `execute_once` `too_many_lines` contract: `scope_lines` −15; `syntax_facts` shrink (the block's paths, method calls and three closures go, and one `Vec::new` call-site arrives); `nested_items` shrink. The function stays over 100 clippy lines (about 160), so the expectation stays fulfilled. |
| `src/dst/tests/producer_protocol.rs` (799) | `:41` `producer_lineage: lineage` becomes `lineage.into_iter().map(crate::shard::LineageLink::Local).collect()`. rustfmt wraps it to 4 lines. `LaneSender::send` is a helper, so no test body changes. | 799→802 |
| `src/dst/tests/split_boundary_outcomes.rs` (838/841) | Delete the two `#[ignore = ...]` lines. Remove `known(F1, ..)` from `appends_while_the_split_intent_is_recorded` (finish with `&[]`) and `appends_staged_around_the_parent_seal_in_one_group`. Delete `const F1` (else `dead_code`). Rewrite the module doc's F1 bullet (comments are not hashed) to say it is fixed and that the two scenarios now fail on its return. | ~832 |
| `src/dst/tests/split_lineage_lanes.rs` | Delete the five `#[ignore]` lines. | ~325 |
| docs | `docs/ROUTING-V3.md` §7, second bullet: add "wherever the predecessor's rows live; a foreign predecessor's rows are read on its engine before enqueue; a lane the serving engine holds never consults a predecessor; unreadable ⇒ retryable 503, nothing committed". `docs/append-transitions.md` step 4: "the request's predecessor chain is left to submission". Step 5: "... applies maintenance/wedge admission, places the key's sealed predecessors (`lineage::chain`) and awaits ...". | |

**`src/shard/lineage.rs`, code outline:**

```rust
pub(crate) enum LineageLink { Local([u8; 16]), Carried(LaneRows) }
pub(crate) struct LaneRows { pub(crate) producer: Option<Bytes>, pub(crate) seq: Option<Bytes> }
impl ShardEngine {
    pub(crate) async fn stored_producer_row(&self, identity: &[u8; 16], key_hash: &[u8; 16], pid: &str)
        -> Result<Option<Bytes>, slatedb::Error> { self.db.get(producer_key(identity, key_hash, pid)).await }
    pub(crate) async fn stored_seq_row(&self, identity: &[u8; 16], key_hash: &[u8; 16])
        -> Result<Option<Bytes>, slatedb::Error> { self.db.get(seq_key(identity, key_hash)).await }
    // moved from shard.rs with its doc; own row first, then links in order:
    pub(super) async fn load_seq_chain(&self, own: &[u8; 16], lineage: &[LineageLink], key_hash: &[u8; 16])
        -> Result<Option<String>, slatedb::Error> {
        if let Some(raw) = self.stored_seq_row(own, key_hash).await? { return decode_seq_row(&raw).map(Some); }
        for link in lineage {
            let stored = match link {
                LineageLink::Local(identity) => self.stored_seq_row(identity, key_hash).await?,
                LineageLink::Carried(rows) => rows.seq.clone(),
            };
            if let Some(raw) = stored { return decode_seq_row(&raw).map(Some); }
        }
        Ok(None)
    }
    pub(super) async fn load_producer_chain(/* same shape, stored_producer_row / rows.producer */) -> ...
}
```

Visibility: `pub(super)` inside `shard::lineage` reaches `shard::transaction::append` and the
shard test modules, as the private methods did. `self.db` is private to `shard` and readable from
its descendants. `LineageLink` has no wildcard arms, and the size difference between its variants
is 16 bytes against 64, far under the `large_enum_variant` threshold.

**`src/application/append/lineage.rs`, code outline:**

```rust
struct Predecessor { seg_id: u32, identity: [u8; 16], route: [u8; 16] }
fn predecessors(desc: &StreamDesc, seg: &SegRoute) -> Vec<Predecessor>   // the moved filter/sort, + segment_route(sg)
pub(super) async fn chain(state: &AppendService, desc: &StreamDesc, seg: &SegRoute,
                          engine: &ShardEngine, req: &AppendReq) -> Result<Vec<LineageLink>, AppendFailure> {
    if req.producer.is_none() && req.seq.is_none() { return Ok(Vec::new()); }
    let serving = state.shards.prefix_for(&seg.shard_route);
    let preds = predecessors(desc, seg);
    let foreign = |p: &Predecessor| state.shards.prefix_for(&p.route) != serving;
    if !preds.iter().any(foreign) { return Ok(preds.into_iter().map(|p| LineageLink::Local(p.identity)).collect()); }
    let own = lanes(engine, &seg.identity, req).await?;
    if (req.producer.is_none() || own.producer.is_some()) && (req.seq.is_none() || own.seq.is_some()) {
        return Ok(Vec::new());   // the serving engine's own rows decide every lane
    }
    let mut links = Vec::with_capacity(preds.len());
    for p in &preds { links.push(if foreign(p) { carried(state, p, req).await? } else { LineageLink::Local(p.identity) }); }
    Ok(links)
}
async fn lanes(engine: &ShardEngine, identity: &[u8; 16], req: &AppendReq) -> Result<LaneRows, AppendFailure>
    // stored_producer_row iff req.producer, stored_seq_row iff req.seq; errors -> unreadable
async fn carried(state: &AppendService, p: &Predecessor, req: &AppendReq) -> Result<LineageLink, AppendFailure> {
    match state.shards.resolve(&p.route, Adoption::External).await {
        Ok(engine) => lanes(&engine, &p.identity, req).await.map(LineageLink::Carried),
        Err(ResolveError::NotOwner { owner, .. }) => { tracing::warn!(seg_id = p.seg_id, %owner,
            "a predecessor's lane rows are on another instance; the append is decided without them");
            Ok(LineageLink::Local(p.identity)) }          // today's behaviour; commit 3 relays
        Err(error @ ResolveError::Opening { .. }) => Err(AppendFailure::from_resolve(error)),
        Err(ResolveError::OpenFailed { error, .. }) => Err(unreadable(&error)),
    }
}
fn unreadable(error: &dyn std::fmt::Display) -> AppendFailure  // Unavailable / SegmentTransition / .retry(1)
```

The `ResolveError` match is explicit, with no `_ =>`. The largest function has 5 arguments,
nesting stays at or below 3, and there are no bool parameters. `External` adoption matches
`submit` and `read_scan`: the read is customer traffic, and it keeps the sweep from closing the
engine mid-read (D-minor).

### Commit 3 (owner-gated by D2/D3): "A predecessor owned by another instance lends its lane rows over the fleet channel"

| File | Change |
|---|---|
| `src/application/append/lineage.rs` | `carried(state, desc, p, req)`: the `NotOwner{owner}` arm calls `relay(state, desc, p, &owner, req)`. It reads `state.lifecycle.topology.peer`, which avoids adding a field to `AppendService` and so avoids growing `http.rs:237-255`. `url_for(owner)` → `InternalTarget::of(desc, p.seg_id)` → GET `{base}/v1/internal/segment-lanes/{encode_stream_name_path(name)}` with the target headers, `streams-internal-key-hash: hex(key_hash)`, and optionally `streams-internal-producer: <id>` and `streams-internal-seq: 1`, via `peer.send(mk)`. A 200 decodes a `serde::Deserialize` struct `{producer: Option<String>, seq: Option<String>}` (base64) into `LaneRows`. Every other answer, including a second redirect, becomes `unreadable(..)`, a 503 that the client retries and that re-resolves ownership. |
| `src/http/internal_routes.rs` (**new**) | The fleet-internal route table moves out of `router()` (`http.rs:1187-1214`, the six `/v1/internal/*` routes) into `pub(super) fn internal_routes() -> Router<Arc<AppState>>`, plus the new route and its receiver `internal_segment_lanes`. The receiver checks `fleet_operation_authorized(.., InternalOperation::SegmentRead)`, reusing the claim so the platform JWT needs no change (D2). Then `internal_sref` → `registry.get` (404/503) → `verify_internal_target` → `(seg_id, identity)`. The segment must be **sealed** in the receiver's descriptor, otherwise 409 `stale_target`. Then `segment_route_by_id` → `engine_for_quiet` (ownership 409 + `Streams-Replay-To` passthrough) → `stored_*_row` → 200 `axum::Json(Serialize struct)`, with no `json!`, so no macro-dsl row is needed. |
| `src/http.rs` (3,153, may not grow) | In `router()`, replace the six internal `.route(..)` calls and their comment (`http.rs:1187-1214`, 28 lines) with `.merge(internal_routes::internal_routes())`, and add `mod internal_routes;`. The file shrinks by about 26 lines. `router()` has no exception. `internal_segment_read`/`internal_segment_close` stay in `http.rs`; the child module reaches these private functions as `super::`. |
| `docs/refactor/WIRE-MATRIX.md` | Add a §3 fleet-internal bullet for `GET /v1/internal/segment-lanes/{*name}`: headers, the sealed-only rule, the 200 body, 404/503/409/401. |

---

## 5 Mutation analysis

Critical prefixes and owners come from `scripts/quality/verification_plan.py:23-31` and
`mutation_owners.py`.

**Commit 1.** Only `src/dst/**` changes, which is not critical, so `"mutants": false`.

**Commit 2.** The planner selects `src/shard.rs` (owner `shard`, `'shard::'`) and
`src/shard/lineage.rs` (critical prefix, **new row required**). `storage_decode_tests.rs` is
`#![cfg(test)]` and classified production-unchanged, so it is omitted. `src/application/append*`
is neither critical nor registered, so it is not selected. It is also not a registered owner
today (D6).

- `src/shard.rs`: the changed lines are deletions, `mod`/`use` lines and the field type. They
  hold no in-diff function-body mutants. The receipt reports the owner as selected with zero
  in-diff mutants.
- `src/shard/lineage.rs`, new row `owner('shard_lineage', 'src/shard/lineage.rs', 'shard::')`,
  the same filter as the sibling `lane_rows`:

| Mutant | Killed by |
|---|---|
| `stored_producer_row` → `Ok(None)` | `r12_undecodable_producer_rows_are_corruption_not_absence`: the parent row read through `Local` becomes `None`, which fails "defers to its predecessor"; the own-row width loop accepts every width |
| `stored_producer_row` → `Ok(Some(Default::default()))` (empty) | r12 `alone`: expected `Ok(None)`, got `Err` (an empty row is undecodable) |
| `stored_seq_row` → `Ok(None)` | `r12_non_utf8_stream_seq_row_is_corruption_not_absence` (expects `"parent-seq"`) |
| `stored_seq_row` → `Ok(Some(empty))` | the same test's `load_seq_chain(&own, &[], ..) == None`: got `Some("")` |
| `load_seq_chain` → `Ok(None)` / `Ok(Some(String::new()))` / `Ok(Some("xyzzy".into()))` | r12 seq test (expects `Some("parent-seq")`, then `None`), U1, U2 |
| `load_producer_chain` → `Ok(None)` / `Ok(Some((0,0,0,[0;16])))` / other tuple literals | r12 producer test (exact `(7,3,11,[9;16])`, then `None`), U1, U2 |
| match-arm deletion | not generated (no wildcard arms) |

No loops can become unbounded, so no TIMEOUT source exists. All kills are unit tests under
`shard::`, which is fast.

**Application file, not selected: kill matrix for review only.** Should the owner register it
(D6), these are the killers:

- `chain`→`Ok(vec![])`: A1, A2, B, C, E and the harness reds.
- Negating the classification `!=`: the same set.
- Pre-check condition →`true`: the reds. →`false`: D.
- `NotOwner` arm: G (commit 3).
- `Opening` arm →`Ok(Local)`: E.

Two mutants would survive. `||`→`&&` in the `!any(foreign)` early return, and the
`req.producer.is_none() && req.seq.is_none()` early return, only save I/O, so no behaviour can
observe them. That is why registration would need either a restructure or accepting them as
dispositions.

**Commit 3.** `src/http/internal_routes.rs` needs a row, `owner('http_internal_routes',
'src/http/internal_routes.rs', 'security_workload:: dst_tests::split_lineage_lanes::
dst_tests::read_application::r06')`. `src/http.rs`'s changed `router()` falls under the existing
`http` owner. `src/http` is in `BUFFER_PREFIXES`, so Miri is selected too.

| Mutant | Killed by |
|---|---|
| `internal_routes()` → `Default::default()` | `read_application::r06` and `security_workload` internal-route tests (404 instead of the routes) |
| receiver → `Default::default()` Response | G: the sender cannot parse an empty 200 and answers 503 |
| auth condition →`false` | H2 |
| identity/sealed checks negated | G (a valid request is refused) |

---

## 6 Ledgers (same commit as the change)

**Commit 1:**
- `docs/quality/owners.json`: by-path-module row `{"category":"by-path-module","count":1,"owner":"crate::split_lineage_lanes","path":"src/dst/dst_tests.rs","reason":"Split-lineage lane scenarios: producer and Stream-Seq lanes of a key whose predecessor lives on another engine (split, merge, repeated split, retired parent), over real HTTP against a four-prefix rig; compiled and executed with DST.","syntax":"path = \"tests/split_lineage_lanes.rs\""}`.
- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write` adds 6 entries, 5 of them with an `#[ignore]` attribute. Review the diff.

**Commit 2:**
- `scripts/quality/mutation_owners.py`: the `shard_lineage` row, placed after `lane_rows`.
- `docs/refactor/test-inventory.json` (`--write`): the attributes of 7 un-ignored tests change. The function hashes of the two harness scenarios change (known lists). The `scenarios` fields change (next item). The body hashes of the five new tests do not change.
- `docs/refactor/test-scenario-map.json`: TOP-010 ("Producer lane through predecessors") gets the harness producer red, A1, A2, C, D and E. TOP-009 ("Stream-Seq through predecessors") gets the harness Stream-Seq red and B. Both stay `mapped: true, coverage: full`. Regenerate `docs/refactor/SCENARIO-MAP.md` with `python3 scripts/scenario-map-report.py`.
- `docs/refactor/review-mechanisms.json`: a mechanism `cross-engine-predecessor-lanes`. Fields: `obligations: ["TOP-009","TOP-010"]`; `owner` "append submission lineage + ShardEngine lane chain"; `mechanism` "split/merge child on another shard prefix; predecessor lane rows carried from the predecessor's engine"; `entered_proof` "prefix(child) != prefix(predecessor) asserted in every scenario"; `oracle` "exact retry duplicate with one copy; next sequence commits; reused sequence 409; Stream-Seq regression 409; nearest predecessor decides; unreadable predecessor 503 with nothing committed"; `configuration {prefixes: 4, worker_threads: 4}`; `limitations` "single instance; cross-instance relay is commit 3"; `execution: requires_final_head_receipt`; `tests` = the two harness reds, A1, A2, B, C, D, E with the `function_sha256` from the inventory. The mechanism tests must not be ignored, and that check passes after un-ignore.
- `docs/reviews/2026-09-hardening/edge-changes.md`: record #53 (text in §2), **only if the owner approves (D1)**. Otherwise the commit waits.
- `docs/refactor/WIRE-MATRIX.md`: in the raw POST "Errors" and the product append errors, one sentence on the new 503 occasion (§2 table, last row). Owner-gated with D1.
- `docs/quality/owners.json`: no row. `lineage.rs` files use explicit imports, and no statics, spawns or DSL macros.

**Commit 3:** `mutation_owners.py` row; WIRE-MATRIX §3 bullet; test-inventory (G, H, H2); TOP-010 += G, H; review-mechanisms: extend the mechanism tests with G, and set limitations to "in-memory store; real fleet pending", with an `external_legs` pending entry if the owner wants a Compute fleet run; edge-changes low-risk fleet-internal record.

---

## 7 Controls (exact commands; expected outputs)

Run from the repo root, one commit at a time. The shell is zsh, so quote globs.

**Commit 1 (red):**
```
cargo test --locked --release --lib dst::dst_tests::split_lineage_lanes -- --include-ignored 2>&1 | tee target/f1-red.log
```
Expected: `a_lane_the_high_child_holds_never_consults_its_retired_parent ... ok`. A1, A2, B, C and E
each `... FAILED`, with the §3.2 texts. The result line reads `test result: FAILED. 1 passed; 5 failed`.

```
cargo test --locked --release --lib dst::dst_tests::split_boundary_outcomes::a_ -- --include-ignored
```
Expected: both F1 reds `FAILED` with the §1.2 texts.

```
cargo test --locked --release --lib dst::dst_tests::split_lineage_lanes
```
Expected: `1 passed; 0 failed; 5 ignored`.

```
python3 scripts/test-inventory.py --check && python3 scripts/scenario-map-report.py --check && python3 scripts/review-evidence.py --check
```

**Commit 2 (green):**
```
scripts/test-leg.sh target/f1/lanes.log --min 6 \
  --exact dst::dst_tests::split_lineage_lanes::a_producer_continues_its_sequence_on_the_high_childs_engine \
  --exact dst::dst_tests::split_lineage_lanes::a_reused_producer_sequence_conflicts_on_the_high_childs_engine \
  --exact dst::dst_tests::split_lineage_lanes::a_merged_child_refuses_a_stream_seq_its_high_parent_accepted \
  --exact dst::dst_tests::split_lineage_lanes::a_second_split_answers_a_retry_from_the_nearest_producer_row \
  --exact dst::dst_tests::split_lineage_lanes::a_first_sight_retry_waits_for_its_retired_parent_and_commits_once \
  --exact dst::dst_tests::split_lineage_lanes::a_lane_the_high_child_holds_never_consults_its_retired_parent \
  -- --locked --release --lib dst::dst_tests::split_lineage_lanes
```
Expected: 6 passed, 0 ignored, and `tests_ran` OK (`--exact` is repeatable and `--min` sets the floor; see `scripts/quality/tests_ran.py:39-45`).

```
cargo test --locked --release --lib dst::dst_tests::split_boundary_outcomes
```
Expected: 11 passed. The `SBO | intent | intent-a-high ... | committed` rows show retry
`200 - ra=- dup` and copies 1. There are no `FINDING F1` rows.

```
cargo test --locked --release --lib shard::storage_decode_tests shard::transaction_tests dst::dst_tests::producer_protocol
```
Expected: all ok, including U1, U2 and every `r12_*` test.

Flake check:
```
for i in $(seq 12); do cargo test --locked --release --lib dst::dst_tests::split_lineage_lanes dst::dst_tests::split_boundary_outcomes || break; done
```
Expected: 12 passes.

NV1-NV4 (§3.5): apply each as an uncommitted patch, run the leg above, and confirm the named tests fail with the quoted text. `git checkout -- .` afterwards.

Ceilings:
```
wc -l src/shard.rs src/http.rs src/dst/tests/split_lineage_lanes.rs src/dst/tests/split_boundary_outcomes.rs src/dst/tests/producer_protocol.rs
```
Expected: `shard.rs` ≤ 3139 (about 3099). The DST files are ≤ 1000.

Lint and ratchet:
```
cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse HEAD~1) python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl
```
Expected: exit 0 from both. The ratchet reports no contract growth, and
`CommitTransaction::append`'s four contracts and `execute_once`'s `too_many_lines` contract are at
or below base.

```
python3 scripts/architecture-gate.py --check; python3 scripts/test-inventory.py --check; python3 scripts/review-evidence.py --check; python3 scripts/scenario-map-report.py --check
```

Mutation leg, run before the push (it is CI's plan):
```
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse HEAD~1) QUALITY_HEAD_SHA=$(git rev-parse HEAD) \
  python3 scripts/quality/verification_plan.py --out target/quality-mutations
```
Expected: `"mutants": true`, `mutation_source_files` = `["src/shard.rs","src/shard/lineage.rs"]`,
`selected_mutation_owners` ⊇ `["shard","shard_lineage"]`, `unregistered_mutation_source_files: []`.

```
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse HEAD~1) QUALITY_HEAD_SHA=$(git rev-parse HEAD) scripts/quality/mutations.sh
```
Expected: every `shard_lineage` mutant caught or unviable; 0 missed; 0 timeout.

Full gate:
```
OUT=target/gate-f1.txt scripts/gate.sh; tail -3 target/gate-f1.txt
```
Expected: `GATEDONE`. The capacity leg owns the machine, so run it when the host is idle (memory:
it fails its ratio at a load average of about 40). Then push and confirm CI with
`gh run view <id>` before calling it green.

**Commit 3:** run the same pattern for G, H and H2 (`dst::dst_tests::split_lineage_lanes`). Also
run `dst::dst_tests::read_application::r06_cross_owner_replay_and_scan_share_typed_pages_and_bill_once`
and `security_workload` (the route table move). `wc -l src/http.rs` must be ≤ 3153 (about 3127).
The mutation plan selects `http` and `http_internal_routes`, and the plan shows `miri: true`.

---

## 8 Out of scope

- **The release-hold 500.** Absorber double retirement, `ca80f9d4`; see `absorber-double-retirement.md`. F1 does not close the hold.
- **F2.** A group the retiring parent wrote is refused 503 `shard_moving` but becomes durable; see `retiring-engine-written-group.md`.
- **F3.** `fail_next_get` during a held publication answers 500 `append_failed` from `prepare()` (skeptic probe).
- **F1c (pre-existing; do not change here).** A duplicate answered from a predecessor row renders the product cursor as `KeyCursor{seg_id: child, offset: min(parent_last+1, child_next)}` (`product.rs:2271-2281`). That mixes the parent's offset into the child's offset space. The raw `Stream-Next-Offset` uses the child tail and is fine. The same happens today on one-prefix rigs, and commit 2 makes it reachable on multi-prefix ones. A fix changes a cursor value on the wire and needs `AppendAck` to carry the lane's segment (D9).
- **Harness nits.** `Row::verdict` accepts "refused but committed" for producer shots. There is no Phase B CAS-failure or four-attempt-exhaustion coverage.
- **Pruning lineage depth**, and the other consumers of lineage: queue/consumer lanes (QUE-009/010) and watches (WAT-005).
- **Materializing a carried row under the child's identity on first sight.** That would make later retries local, but it needs a write in `CommitTransaction::append`, which is exception growth.

---

## 9 Decisions for the owner

1. **D1: the edge change of commit 2.** It restores ROUTING-V3 §7 across engines: four outcome changes plus one new occasion for existing 503 codes (§2 table). Approve it and record it as edge-changes #53 (medium), or hold the commit.
2. **D2: cross-instance transport (commit 3).** F1 stays open whenever the ring gives a predecessor and its child different owners, and that is the common fleet case. Commit 2 alone keeps today's at-least-once behaviour there, logged by `warn!`. Options:
   - (a) Approve `GET /v1/internal/segment-lanes/{*name}` under the existing `segment-read` claim, so the platform needs no change. **Recommended, and ship it with commit 2.**
   - (b) The same route with a new `segment-lanes` op claim. This needs workload JWTs to mint the new op.
   - (c) Accept and document at-least-once across instances at split boundaries.
3. **D3: `http.rs` ceiling for commit 3.** Extract the fleet-internal route table into `src/http/internal_routes.rs`, which is recommended: `http.rs` shrinks and the move needs a new mutation row. Or approve a bounded exception of 4 lines.
4. **D4: steady-state cost.** For producer and Stream-Seq appends on a child with a foreign predecessor, each append does one point read of the serving engine's own row per requested lane, off the committer. That is the price of never touching the predecessor in the steady state. The zero-I/O alternative peeks at the resident handle's `state` mutex, which needs either an approved `unwrap` growth row or a `lock().ok()` poison policy.
5. **D5: refusal class for an unreadable predecessor.** Every case is retryable 503 with nothing committed, including `OpenFailed` (mapped to `segment_transition`, not the serving shard's 500 `shard_open`). This keeps the split boundary free of new 500s. The cost is that a predecessor that stays unopenable makes first-sight appends for its keys retry forever.
6. **D6: mutation registration of `src/application/append/lineage.rs`.** It is not registered, like its siblings. Registering it would need either dispositions for the two cost-only mutants or a restructure (§5).
7. **D7: no Loom model.** The change adds no lock, atomic, channel or retirement state. The carried rows are immutable values, and a read racing a predecessor's retirement fails into a retryable 503, as test E exercises. Confirm, or name the transition the owner wants modelled.
8. **D8: which harness commit lands first.** `8a2791b0` or the amended `706430f7`.
9. **D9: F1c, the duplicate cursor for predecessor-answered duplicates.** Plan it separately, because it changes a wire value.
10. **D10: lazy versus eager carry for clients that use a fresh producer id per request.** Such clients pay a first-sight read on every append on a foreign-lineage child: local in commit 2, one relay in commit 3. Eager copy at Phase B removes that cost, but it lengthens every split's unavailability window with O(parent lanes) of work. The recommendation is lazy.
11. **Minor: predecessor resolution uses `Adoption::External`,** the same as `submit`/`read_scan`, rather than `Internal`. The effect is that a sweep cannot close the predecessor's engine during a first-sight read.

---

## Skeptic corrections (C1..C14)

Everything below was checked read-only against `slate` (HEAD is now `46f668b3`, one commit past `eda79ccf`; it touches `src/auth.rs`, `src/auth/lease.rs` and `docs/refactor/WIRE-MATRIX.md` only) and against `8a2791b0` / `706430f7` / `bc2907f7`. Nothing was run.

**What holds.** Every quote in §1.1 matches the tree: `application/append.rs:315-329`, `shard.rs:2016-2056`, `transaction/append.rs:36-39,111-114`, `topology.rs:494-508`, `registry.rs:760-764`, `submit.rs:18-25`, `commit_plan.rs:131-136`. The causal trace in §1.2 is correct. The two reds and their `Answer::show()` text match `sbo.rs:800-838`. The §1.4 use-site list is complete: `history.rs:1528,1610` sit inside `mod tests` (1055..), and the only production constructors are `append.rs:356` and `initialization.rs:96`. The frozen-row invariant (§1.5) holds, with three supports. `SegmentMap::split`/`merge` (`segmap.rs:324,403`) set `sealed_ms` only inside the Phase B CAS. That CAS runs only after `seal_segment_identity` returns (`topology.rs:428`). The only writers are `accept_append` (`transaction/append.rs:179-182,239-240`), and nothing deletes lane rows. `resolve` returns exactly three `ResolveError` variants (`shard_directory.rs:29-45`). A retired prefix answers `Opening{shard_moving|shard_closing}` (`sharddir.rs:480-497,651-658,917`), so E's first retry really is a 503. SlateDB `get` on a closed DB is `Err` (`check_closed`, slatedb `db.rs:226,578`), not `Ok(None)`. `CommitTransaction::append` stays byte-identical, so its four contracts cannot move. `execute_once` carries only `too_many_lines` (`append.rs:242-245`), so the `Vec::new()` call it gains is not an unwrap fingerprint. The file ceilings hold. **For one instance, the fix closes F1's mechanism.** I found no interleaving that duplicates or regresses a lane once the chain is placed in `submit`. An own row written after the pre-check is caught by the committer's overlay, then shared state, then its own DB read (`transaction/append.rs:29-47,104-126`). Carried rows are immutable.

**C1 (design, contract mismatch): stop at the nearest predecessor that holds the lane, and bound the wait.** The outline carries **every** foreign predecessor (`for p in &preds { ... carried(state, p, req).await? }`), even when a nearer one already holds the row. Two things go wrong:
- An unreadable **farther** predecessor refuses an append that the nearer row decides. Example: after two splits, `ga` is served on R with chain `[seg2 (Q), seg0 (P)]`. Seg 2 holds the row and P is retiring, so today's plan answers 503. That contradicts §2's own rule ("When the **nearest** row cannot be read ...").
- Each `resolve` can wait for `open_wait` (10 s, `shard_directory.rs:268-285`), and the resolves run one after another. The worst case is depth × 10 s before enqueue.

Fix: in `submit`, walk the predecessors nearest-first. Read Local links through the serving engine's `stored_*_row`, which is equally exact because the rows are sealed. Stop when every requested lane has a row, and hand the committer only the prefix of the chain up to that link. Memoize one engine per foreign prefix, and run all resolves under one deadline. Add pin **F**, `a_retry_decided_by_the_nearest_predecessor_ignores_an_unreadable_farther_one`: C's setup, then `retire_parent()`, then a retry at seg 4 must be a duplicate. It is 503 under the current outline and a duplicate after this change.

**C2 (claim wording): "a lane the serving engine already holds never consults a predecessor" is true only for rows on disk.** A lane that a predecessor row decided as a duplicate is inserted into the overlay (`transaction/append.rs:48-50`) and published into the handle's shared state (`transaction/publish.rs:49-54`). No own row is ever written for it. The pre-check reads the DB only, so every later request on that lane is carried again. It fails 503 whenever the predecessor is unavailable, even though the committer's shared state would decide it. Reword §2 and pin D ("a lane with an own **stored** row ..."). Name this residue under D4. The zero-I/O peek is the only thing that removes it.

**C3 (availability coupling not stated): first-sight appends on a foreign-lineage child now depend on the predecessor's engine.** They depend on a peer relay as well once commit 3 lands.
- In `appends_queued_on_a_parent_engine_that_retires` (`sbo.rs:720-742`), the rows `retire-control-high` and `retire-control-raw` are first-sight producer shots on Q. They are fired concurrently with `retire_parent`. After the fix each resolves P, so depending on timing it can be refused 503 (`shard_moving`/`shard_closing`) and commit on the retry. The verdict still passes (`sbo.rs:197-200`). But these rows stop being controls, their doc line "The high child's engine is untouched" becomes false, and their outcome becomes nondeterministic. Make those two shots `.bare()` so they stay controls, or rewrite the doc and say so in §3.1.
- Dead-letter delivery uses a per-message producer id, `dlq:{cname}:{msg_id}` with `routing_key: ""` (`application/consumer/delivery.rs:689-703`). On a DLQ collection that has split, **every** DLQ append is therefore first-sight on the high child. Each one pays the predecessor read, a relay in fleets, and the coupling. Add this to D10 as an internal path, not only for "clients that use a fresh producer id".

**C4 (commit 2 contradicts its own contract and docs in fleets).** The `NotOwner → Ok(Local(identity))` arm reads a foreign identity from the serving DB, which always misses. So the lane is "decided as if the row were absent", which §2 forbids.
- The commit-2 edit to ROUTING-V3 §7 ("wherever the predecessor's rows live") would therefore be false in the common production topology until commit 3 lands. Scope the doc text and the edge record to "on this instance" in commit 2. Alternatively, land commit 3 together with commit 2 (D2a).
- The `tracing::warn!` fires on every first-sight producer or Stream-Seq append, so it floods logs under C3's DLQ and fresh-id traffic. Use a counter plus a rate-limited or `debug!` log.
- F1 is closed in production only by commit 3. Say so in the §2 summary line, not only in D2.

**C5 (new side effect of the predecessor resolve; owner-visible).** `ShardDirectory::resolve(.., External)`:
- opens the predecessor's prefix when it is not resident, and so "fences any previous owner" (`shard_directory.rs:229-236,268-285`);
- retires a resident predecessor engine when the ring has moved it (`:257-266`);
- stamps external adoption and revokes sweep custody (`:246-255`).

Until now only P-bound traffic did any of this. After commit 2, Q-bound traffic does it too. During ring convergence this widens who can open and fence P on this instance. Add it to decision 11 (D-minor), and state that the gateway's routing no longer bounds which instances touch P.

**C6 (red and control texts are not achievable as written).**
- **E**: `copies(w)` reads the high key back through seg 0's records on P, and P is retired and held off. Count copies only after `await_reopen()`. The red text `...: 200 - ra=- (2 copies)` must be asserted after the reopen, with the first retry's `Answer` recorded before it.
- **NV2**: the retry-after is `(until-now).as_secs().max(1)` under a holdoff (`sharddir.rs:651-658`), escalated for a young `FleetEviction`, or 1 for `shard_closing`. Do not quote `ra=1`. Assert `503`, `temporarily_unavailable` and a present `retry-after` instead.
- **D**: the plan calls D the "always carry" kill (NV2). The application file is not mutation-selected, so this is a manual control only. State it that way in §3.3.

**C7 (invalid commands in §7).** `cargo test` takes one positional TESTNAME. These two will fail with "unexpected argument":
- `cargo test --locked --release --lib shard::storage_decode_tests shard::transaction_tests dst::dst_tests::producer_protocol`
- the flake loop's `... dst::dst_tests::split_lineage_lanes dst::dst_tests::split_boundary_outcomes`

Put the extra filters after `--`, e.g. `cargo test --locked --release --lib -- shard::storage_decode_tests shard::transaction_tests dst::dst_tests::producer_protocol`. Cargo-mutants itself does this (`mutation_driver.py:30-37`).

**C8 (ledger and anchor details).**
1. `src/dst/dst_tests.rs` is 276 lines once the harness lands (`8a2791b0` adds 3), so commit 1 takes it to 279, not "273→276".
2. `review-mechanisms.json` test entries use the key `sha256`, not `function_sha256` (see `prior-group-remote-frontier`).
3. The `edge-changes.md` record needs three more edits:
   - the Summary counts table (`:15-20`, medium/both 6→7 or high/both, total 52→53);
   - the Index row;
   - numbering agreed with the parallel F2 plan (`plans19/retiring-engine-written-group.md`), which may also claim #53.
4. The F2 plan anchors `split_boundary_outcomes.rs` to the 841-line `706430f7`/`bc2907f7`, while this plan uses the 838-line `8a2791b0`. Settle D8 jointly and re-anchor, because both plans edit the `known(..)` lists of the same scenarios.
5. Re-anchor on `46f668b3` (WIRE-MATRIX shifted).

**C9 (edge grading and F1c).**
- Record #30 (`edge-changes.md:160-166`) changed how a lane is decided, on both surfaces, and was graded **high**. This change turns 200s into 409 conflicts, 409 gaps into 200s, and adds first-sight 503s on hot streams. Propose **high**, or justify medium explicitly.
- F1c is not neutral either. The duplicate cursor is `KeyCursor{seg_id: child, offset: min(parent_last+1, child_next)}` (`product.rs:2271-2281`). A consumer resuming from it skips child records `[0, min)` for that key, all of which were written after the acknowledged write. Commit 2 makes this answer reachable wherever the high child is on another prefix, and it replaces a second copy. D1 should approve it knowingly or bundle D9. Put the cursor value in the §2 "After" cell.

**C10 (D7 and Loom): accept "no Loom model", because no lock, atomic or retirement state changes.** But the two properties the safety argument rests on are unpinned:
- (a) **Race pin (DST).** Two first-sight appends of the same lane on Q, `produce(ga,"r",1,1)` twice after the parent committed `(1,0)`. Both miss the pre-check and are carried, and both are held behind Q's `test_hold_commit` (the harness pattern, `sbo.rs:570-583`). When released, the answers must be one commit plus one duplicate, with 1 copy.
- (b) **Unit pin in `storage_decode_tests`.** Reading a lane row through `stored_producer_row` on an engine after `begin_close()` + `await_terminated` is `Err`, never `Ok(None)`. That is the "a read racing retirement becomes a 503" property. Today it holds only by SlateDB's `check_closed`.

Name both in D7 as the replacement for a model.

**C11 (commit 3 gates the plan does not mention).**
- The architecture gate forbids the words `Response` and `HeaderMap` anywhere in `src/application/**` (`architecture-gate.py:105-113`). `peer.send` returns `reqwest::Response` (`peer.rs:124-127`), so the relay must never name the type. Follow `read_remote.rs:181` (`let resp = match peer.send(mk).await`) and keep errors as strings.
- `src/http/internal_routes.rs` becomes a new `axum_outside_transport` item in `architecture-report.py:327-331`. It is diagnostic only, but it should be in the commit message.

**C12 (minor factual).** The functions `history_partition_if_open` and `history_partition` (`shard.rs:1978-2014`) lie between `submit_absorbed_batch_v2` and the loaders. They have no exception, so the "outside every exception scope" conclusion stands. The deletion is `:2016-2056` (41 lines) plus the blank at `:2015`. The ROUTING-V3 anchor the code cites is §3.6, while the contract is §7 (`ROUTING-V3.md:258-266`). §7 already promises the state is "seeded into the child's first append", so edit §7 and leave §3.6.

**C13 (boundedness).** Lineage depth grows without bound (pruning is out of scope, §8). The pre-check and the C1 walk are bounded by depth × (one point read), plus one memoized resolve per distinct foreign prefix, under the single deadline from C1. State this in §2. Without C1 the bound is depth × 10 s.

**C14 (mutation claims): correct as far as they go.** `src/application/**` is not a critical prefix (`verification_plan.py:23-31`) and not registered. `storage_decode_tests.rs` is `#![cfg(test)]`, so it is production-unchanged. The new `src/shard/lineage.rs` needs the `shard_lineage` row, or `validate_sources` fails before any discovery (`mutation_driver.py:98-101`). The kill table for `stored_*_row` and `load_*_chain` checks out against `storage_decode_tests.rs:153-215`:
- the `Ok(None)` mutant fails "defers to its predecessor";
- the empty-`Bytes` mutant fails the decode or `Some("")`;
- tuple and string literals fail the exact asserts.

If C1 moves the Local reads into the application walk, the committer's `LineageLink::Local` arm keeps its killers only through U2 and the `r12_*` `Local(parent)` call sites, so keep those.

**Verdict: ready-with-corrections.** For one instance, the fix is causal and closes F1's mechanism. The reds are real today and turn green with the fix. No ratcheted scope grows. Required before implementation:
- C1 (walk nearest-first and stop at the first row; one deadline) and pin F;
- C3 (the harness control rows and the DLQ cost);
- C4 (scope the commit-2 docs and edge record; replace the log flood);
- C6 and C7 (achievable red texts; valid commands);
- C10's two pins.

F1 is not a release-hold item, since it never answers 500 (§1.6). It stays open in fleets until commit 3 (D2).
