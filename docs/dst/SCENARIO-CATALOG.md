# Prisma Streams DST Scenario Catalogue

**Baseline:** `681ea0fe73ca49c74fc10a61846b9dbf7195d443`  
**Purpose:** permanent deterministic regressions for every major failure family encountered during the SlateDB, cost, routing-v3, product-surface, lifecycle, and field-validation programs.

## Status labels

- **Existing:** a production-path regression already exists in the reviewed baseline.
- **Strengthen:** related coverage exists, but the exact boundary or mechanism must be tightened.
- **L1-now:** add immediately to the current focused failpoint suite.
- **L2-sim:** requires the deterministic whole-system simulator.
- **L3-field:** requires real processes/adapters/platform; the logical consequence may also have an L2 model.

Every scenario MUST declare mechanism counters and MUST fail as invalid if the mechanism was not reached.

---

# A. Durability, acknowledgement, and idempotence

### DUR-001 — Ordinary success waits for remote durability
**Status:** Existing  
**Procedure:** stage an append, pause after `db.write(await_durable=false)` and before remote watermark; assert no success reaches the client; release durability and require exact offset.  
**Catches:** acknowledgement from applied state.  
**Invariants:** D1, D5, D8.

### DUR-002 — Same-group producer duplicate waits for the original group
**Status:** L1-now  
**Procedure:** place original and exact duplicate in one commit group; hold or fail `db.write`; neither may succeed before the group is durable, and both fail if the group fails.  
**Catches:** duplicate response from batch-local producer state.  
**Invariants:** D2, D5, P2.

### DUR-003 — Applied-but-not-durable producer duplicate waits
**Status:** Existing  
**Procedure:** apply original group, hold durable dispatch, submit duplicate, require it to remain pending.  
**Catches:** duplicate response from `handle.state.applied`.  
**Invariants:** D2, P2.

### DUR-004 — Same-group idempotent close waits for durability
**Status:** L1-now  
**Procedure:** close-only and exact close retry in one group; fail group write; neither may return success or publish collection sealing.  
**Catches:** close success from batch-local `closed=true`.  
**Invariants:** D3, D5, L13.

### DUR-005 — Applied-but-not-durable close retry waits
**Status:** Strengthen  
**Procedure:** hold remote durability after physical close is applied, issue exact close-only retry, require both responses to wait.  
**Catches:** idempotent close from applied state.  
**Invariants:** D3.

### DUR-006 — Producer sequence reuse rejection is barriered
**Status:** L1-now  
**Procedure:** in one group, request A stages producer sequence N/body X; request B uses N/body Y; fail the group write. B must not have observed definitive reuse based on state that never committed.  
**Catches:** state-dependent rejection from batch-local state.  
**Invariants:** D4, D5, P7.

### DUR-007 — Producer gap with admitted predecessor
**Status:** Existing  
**Procedure:** park sequence N before enqueue/commit, allow N+1 to observe a gap, then commit N and retry N+1; final operation must commit exactly once.  
**Catches:** treating a mutable ordering verdict as terminal.  
**Invariants:** P8, L2.

### DUR-008 — Stream-Seq conflict is barriered
**Status:** L1-now  
**Procedure:** stage a new Stream-Seq in current group; send conflicting request; fail the group; conflict must not escape before establishing state is durable.  
**Catches:** non-linearizable Stream-Seq errors.  
**Invariants:** D4, P5.

### DUR-009 — Lost object-store response is internal latency
**Status:** Existing  
**Procedure:** object write succeeds, response is lost, SlateDB retries; client remains pending and eventually receives one durable result.  
**Catches:** confusing storage ambiguity with public ambiguity.  
**Invariants:** D1.

### DUR-010 — Client deadline creates public ambiguity
**Status:** Existing  
**Procedure:** delay WAL beyond client deadline, let server commit, retry with producer idempotence after healing; require one copy and original offset.  
**Catches:** indefinite-patient test blind spot.  
**Invariants:** D7, P2, P3.

### DUR-011 — Response lost after durable dispatch
**Status:** Existing  
**Procedure:** commit and dispatch success, drop response channel, retry exact operation.  
**Catches:** response loss after successful commit.  
**Invariants:** D6, P2.

### DUR-012 — Handoff during in-flight append
**Status:** Existing  
**Procedure:** park WAL write, open new owner, release old owner; result must be durable success or retryable unknown, never lost acknowledgement.  
**Catches:** fence window.  
**Invariants:** D1, T11.

### DUR-013 — Fenced owner acknowledges nothing
**Status:** Existing  
**Procedure:** prove old engine acknowledges before handoff, fence it, then attempt writes and require zero ghost acknowledgements plus task termination.  
**Catches:** original vacuous I4 test.  
**Invariants:** T11, R1.

### DUR-014 — Acknowledged-set equality, not count equality
**Status:** L2-sim  
**Procedure:** inject one missing acknowledged record and one extra ambiguous duplicate while preserving total count; exact operation-set auditor must fail.  
**Catches:** aggregate reconciliation masking loss.  
**Invariants:** D8, D9.

### DUR-015 — Unknown non-idempotent retry may commit twice
**Status:** Existing oracle control  
**Procedure:** lose response, retry without producer identity, permit two distinct attempts; oracle must not report a false duplicate.  
**Catches:** over-strict oracle.  
**Invariants:** model correctness.

---

# B. Create and initialization lifecycle

### CRT-001 — Descriptor before initial body never becomes Ready
**Status:** Existing  
**Procedure:** create descriptor, crash before initial body; metadata/read/append must return retryable initializing result until recovery completes.  
**Catches:** replay returning 200 for empty half-created stream.  
**Invariants:** C1, C5.

### CRT-002 — Exact create replay joins initialization
**Status:** Existing  
**Procedure:** issue same create twice while first is parked; one initializer runs, both results converge, initial body appears once.  
**Catches:** detached duplicate initializer.  
**Invariants:** C2, C7.

### CRT-003 — Wrong-key replay cannot resume
**Status:** Existing  
**Procedure:** create initializing descriptor with key A; retry exact body/config with key B.  
**Catches:** initial content encrypted under wrong key while fingerprint remains A.  
**Invariants:** C3, S5.

### CRT-004 — Claim age does not imply readiness
**Status:** Existing  
**Procedure:** advance beyond initialization claim timeout without completing work; resource remains initializing, but another worker may reclaim it.  
**Catches:** stale init becoming visible after 15 seconds.  
**Invariants:** C1.

### CRT-005 — Old creator cannot publish Ready after recreate
**Status:** Existing  
**Procedure:** park creator before Ready, delete old epoch, recreate same name, release old creator; new init remains intact.  
**Catches:** create ABA.  
**Invariants:** C4, L14.

### CRT-006 — Concurrent delete after source reference but before Ready
**Status:** Strengthen  
**Procedure:** fork child installs source ref, delete child before Ready, resume creator; creator must not return success and source ref must be removed or debt recorded.  
**Catches:** orphan source reference from declined Ready CAS.  
**Invariants:** C6, F7.

### CRT-007 — Initial content duplicate waits for durability
**Status:** L1-now  
**Procedure:** two create retries share synthetic init identity in one group; fail initial-content write; descriptor must not become Ready.  
**Catches:** duplicate fast-path publishing Ready before durability.  
**Invariants:** C5, C7, D2.

### CRT-008 — Create-and-close crash matrix
**Status:** L1-now  
**Procedure:** crash after descriptor, after body durability, after physical close, and before Ready; exact retry resumes each phase.  
**Catches:** create-and-close partial completion.  
**Invariants:** C5, L13.

### CRT-009 — Initialization takeover is cancellation-proof
**Status:** L2-sim  
**Procedure:** all clients abandon while cache-owned initializer runs; initializer still completes or leaves a recoverable intent.  
**Catches:** detached work whose result cannot populate state.  
**Invariants:** C2, R1.

### CRT-010 — Catalog excludes initializing descriptors
**Status:** Existing/strengthen  
**Procedure:** create many initialized and initializing descriptors across pages; catalog returns only Ready live resources and preserves continuation.  
**Catches:** exposing unusable resources.  
**Invariants:** C8, S8.

### CRT-011 — Deterministic create validation before mutation
**Status:** L1-now  
**Procedure:** invalid key, body, config, expiry, watch definition, or impossible capacity returns error with no descriptor object.  
**Catches:** malformed create leaving lifecycle state.  
**Invariants:** C1, S5.

### CRT-012 — Creation request identity is complete
**Status:** L1-now  
**Procedure:** vary exactly one semantic field—key fingerprint, source epoch, body, content type, expiry, fork boundary—and prove operation identities differ.  
**Catches:** concatenation/omission collisions.  
**Invariants:** P7, C2.

---

# C. Sealing, claim, fence, and close

### SEL-001 — Deterministic validation precedes intent
**Status:** Existing/strengthen  
**Procedure:** invalid routing key, header value, producer syntax, body, content type, byte/record/request capacity; no Sealing state may be published.  
**Catches:** permanent invalid request bricking collection.  
**Invariants:** L1.

### SEL-002 — Final null is a real record
**Status:** Existing  
**Procedure:** seal with JSON `null`; require one null record and sealed terminal state.  
**Catches:** `Option<Value>` conflating absent and null.  
**Invariants:** L2.

### SEL-003 — Semantic operation ID has no concatenation collision
**Status:** Existing  
**Procedure:** `{final:1,key:"23"}` and `{final:12,key:"3"}` produce distinct IDs.  
**Catches:** unframed hash input.  
**Invariants:** P7.

### SEL-004 — Identity includes content type and key version
**Status:** Existing/strengthen  
**Procedure:** same body/key with different request Content-Type or key version cannot join one intent.  
**Catches:** invalid request abandoning valid intent; encryption-version ambiguity.  
**Invariants:** L2, P7.

### SEL-005 — Plain seal cannot finish owed final
**Status:** Existing  
**Procedure:** crash after final intent; issue seal-only; collection remains Sealing until exact final operation resumes or a correctly fenced recovery policy acts.  
**Catches:** dropped final record.  
**Invariants:** L3.

### SEL-006 — New producer sequence rejected during sealing, duplicate allowed
**Status:** Existing  
**Procedure:** while Sealing, exact duplicate resolves; N+1 is rejected after duplicate detection.  
**Catches:** producer bypass of lifecycle check.  
**Invariants:** L4.

### SEL-007 — Raw close publishes intent only after validation
**Status:** Existing  
**Procedure:** malformed raw close-with-content returns 400 and leaves collection Open.  
**Catches:** raw close mutating before validation.  
**Invariants:** L1.

### SEL-008 — Raw close crash after intent, before enqueue
**Status:** Existing  
**Procedure:** public exact retry with no private header resumes, appends once, and seals.  
**Catches:** unresumable raw final.  
**Invariants:** L11.

### SEL-009 — Raw close crash after final durability, before mark
**Status:** Existing  
**Procedure:** exact retry deduplicates before closed check, marks committed, finishes seal.  
**Catches:** 409 forever after record already committed.  
**Invariants:** L11, P2.

### SEL-010 — Raw definitive rejection releases exact uncommitted intent
**Status:** Existing/strengthen  
**Procedure:** producer reuse, stale epoch, bad body, CT mismatch, or Stream-Seq conflict; exact owned intent is removed only after linearizable rejection.  
**Catches:** permanently Sealing collection.  
**Invariants:** L1, D4.

### SEL-011 — Producer gap retains intent
**Status:** Existing  
**Procedure:** predecessor already admitted; gap response must not destroy final promise.  
**Catches:** ordering verdict misclassified as terminal.  
**Invariants:** P8, L2.

### SEL-012 — Non-closing duplicate cannot satisfy final
**Status:** Existing  
**Procedure:** reuse producer tuple from ordinary append for close-with-content; duplicate response has `closed=false`, intent clears, collection remains open.  
**Catches:** stuck Sealing or sealing without final.  
**Invariants:** L12.

### SEL-013 — Internal idempotence namespace is unreachable
**Status:** Existing/strengthen  
**Procedure:** public producer IDs cannot address internal seal rows; direct internal path remains idempotent.  
**Catches:** public producer impersonating internal final.  
**Invariants:** P6.

### SEL-014 — Seal claim bound to validated incarnation
**Status:** Existing  
**Procedure:** pause after key validation before claim, delete/recreate same name/key, release old request; replacement remains open.  
**Catches:** validation-to-claim ABA.  
**Invariants:** L14.

### SEL-015 — Seal final append bound to claim incarnation/generation
**Status:** Existing  
**Procedure:** pause after claim before final append, delete/recreate same name/key, release old request; replacement receives no record and stays writable.  
**Catches:** claim-to-append ABA.  
**Invariants:** L5, L14.

### SEL-016 — Mark/abandon/run-seal bound end to end
**Status:** Existing/strengthen  
**Procedure:** pause after final durability, recreate name, resume old operation; no mark, abandonment, segment close, or terminal proof may affect replacement.  
**Catches:** partial epoch fencing.  
**Invariants:** L5, L14, L15.

### SEL-017 — Seal serializes with pending split
**Status:** Existing  
**Procedure:** pending split exists; seal resolves transition, refetches live segments, then seals; no deadlock.  
**Catches:** Sealing + pending topology stalemate.  
**Invariants:** L13, T5.

### SEL-018 — Phase-B split cannot publish after sealing
**Status:** Existing  
**Procedure:** park before phase B, seal collection, release split; no live child appears under Sealed.  
**Catches:** phase-A-only lifecycle fence.  
**Invariants:** T4, L13.

### SEL-019 — Phase-B merge cannot publish after sealing
**Status:** L1-now  
**Procedure:** same as SEL-018 for merge.  
**Catches:** asymmetric split/merge fencing.  
**Invariants:** T4, L13.

### SEL-020 — Fence raise is immediate
**Status:** Existing  
**Procedure:** after takeover fence is enqueued/processed, a lower-generation append arriving later is rejected before staging.  
**Catches:** stale write after takeover.  
**Invariants:** L6.

### SEL-021 — Fence response waits for current-group durability
**Status:** Strengthen  
**Procedure:** close and fence in same commit group; fail `db.write`; fence must return error, never `closed=true`.  
**Catches:** fence reading local closed state before group write.  
**Invariants:** L7, D5.

### SEL-022 — Fence response waits for prior-group remote durability
**Status:** Strengthen  
**Procedure:** close group applied with `await_durable=false`, pause WAL watermark, process fence-only group; fence remains pending.  
**Catches:** fence reading applied closed state.  
**Invariants:** L7.

### SEL-023 — Lower takeover reservation cannot install
**Status:** Existing  
**Procedure:** reserve generations 2 and 3, fence both, allow generation 2 to attempt install first; only newest installs.  
**Catches:** live claim below current fence.  
**Invariants:** L8.

### SEL-024 — Exact retry renews generation
**Status:** Existing  
**Procedure:** retry same operation after abandoned reservation; new generation exceeds fence and completes.  
**Catches:** active operation inheriting fenced generation.  
**Invariants:** L6, L8.

### SEL-025 — Fence survives handle eviction
**Status:** Existing  
**Procedure:** raise fence, evict/reload StreamHandle, enqueue stale generation, require superseded result.  
**Catches:** safety state stored in cache handle.  
**Invariants:** L9, R3.

### SEL-026 — Fence is not expired while stale requests remain queued
**Status:** L1-now  
**Procedure:** raise fence, keep lower-generation request queued, advance wall time beyond any cleanup threshold, release queue; stale request remains rejected.  
**Catches:** wall-clock fence pruning.  
**Invariants:** L9, L10.

### SEL-027 — Concurrent exact final attempts cannot abandon each other
**Status:** L1-now/L2-sim  
**Procedure:** same final bytes/key but different producer semantics; one definitive failure must not clear another operation’s claim.  
**Catches:** under-specified operation identity / shared abandonment.  
**Invariants:** L2, P7.

### SEL-028 — Terminal proof is exact
**Status:** Existing  
**Procedure:** old operation reaches final proof while another incarnation/operation is terminal; old request must not report success.  
**Catches:** success based on someone else’s sealed state.  
**Invariants:** L15.

---

# D. Fork graph, stitched reads, and deletion

### FRK-001 — Basic fork prefix and suffix
**Status:** Existing  
**Procedure:** source records, fork at boundary, child records; stitched read is exact.  
**Invariants:** F1.

### FRK-002 — Binary sub-offset materialization
**Status:** Existing  
**Procedure:** fork inside a binary record; partial suffix becomes child’s first own record exactly once.  
**Invariants:** F2.

### FRK-003 — Per-hop epoch/decryption validation
**Status:** Existing  
**Procedure:** fork chain with distinct epochs; each hop decrypts using its source epoch; mismatch fails safely.  
**Invariants:** F3.

### FRK-004 — Cycle and depth defense
**Status:** Existing  
**Procedure:** inject cycle and over-depth chain; read refuses without recursion/resource blowup.  
**Invariants:** F4, R2.

### FRK-005 — Raw fork filters to default key
**Status:** Existing  
**Procedure:** source has default and non-empty product keys; raw source/fork returns only default-key sequence.  
**Invariants:** F11, S6.

### FRK-006 — Source soft-delete with child
**Status:** Existing  
**Procedure:** delete source with live child; source returns 410, child remains readable, recreate source name is blocked.  
**Invariants:** F5.

### FRK-007 — Last child triggers source hard delete
**Status:** Existing  
**Procedure:** delete final child, release reference, cascade hard delete.  
**Invariants:** F6.

### FRK-008 — Child init crash after tail seed
**Status:** L1-now  
**Procedure:** crash after fork tail seed before source ref; retry resumes without duplicate materialization.  
**Invariants:** F7.

### FRK-009 — Child init crash after source ref before Ready
**Status:** Existing/strengthen  
**Procedure:** source may become soft-deleted while retaining child; exact retry completes Ready.  
**Invariants:** F7.

### FRK-010 — Source incarnation changes during child init
**Status:** Existing  
**Procedure:** hash includes source epoch; retry against replacement conflicts and abandoned child is cleaned.  
**Invariants:** F3, F7.

### FRK-011 — Concurrent first fork versus source delete
**Status:** Existing but strengthen handshake  
**Procedure:** park delete after current descriptor read/inside CAS decision, install child ref, release delete; source soft-deletes, never hard-deletes.  
**Invariants:** F10.

### FRK-012 — Delete wins before fork reference
**Status:** L1-now  
**Procedure:** force opposite ordering; fork source CAS sees dead source and refuses, target init does not become Ready and no orphan remains.  
**Invariants:** F10.

### FRK-013 — Child deleted before source reference install
**Status:** L1-now  
**Procedure:** park creator after child stamp, delete child, resume creator; no source ref leak, no false create success.  
**Invariants:** C6, F7.

### FRK-014 — Direct deletion debt retry
**Status:** Existing  
**Procedure:** crash after child tombstone before parent ref release; retry same DELETE settles debt.  
**Invariants:** F8.

### FRK-015 — Three-generation cascade crash
**Status:** Existing  
**Procedure:** A←B←C, B soft-deleted; delete C, crash after B tombstone before A release; retry DELETE C settles full chain.  
**Invariants:** F9.

### FRK-016 — Cascade concurrent new child
**Status:** L1-now  
**Procedure:** while last-child release decides whether to hard-delete source, install a new child in both forced orderings.  
**Invariants:** F10.

### FRK-017 — Ready fork idempotent PUT after source soft-delete
**Status:** L1-now  
**Procedure:** response lost after fork Ready; source soft-deleted; exact target PUT returns existing fork rather than source-gone.  
**Invariants:** F5, F7.

### FRK-018 — Fork participant cannot split unexpectedly
**Status:** Existing/strengthen  
**Procedure:** source or child in fork graph becomes hot; scaler refuses or follows explicitly supported lineage design.  
**Invariants:** F12.

### FRK-019 — Expiry with fork references
**Status:** L1-now  
**Procedure:** source expires with children; behaves as soft-deleted until final reference release.  
**Invariants:** F5, F6.

### FRK-020 — Fork object reachability under GC
**Status:** L2-sim  
**Procedure:** compaction/GC during fork chain reads and deletion; no ancestor object is deleted before last reference.  
**Invariants:** F1, H13.

---

# E. Routing, split, merge, and ownership

### TOP-001 — Segment-map partition invariant
**Status:** Existing/property  
**Procedure:** random split/merge operations; ranges remain complete, ordered, non-overlapping.  
**Invariants:** T1.

### TOP-002 — Crash after pending split intent
**Status:** L1-now  
**Procedure:** restart/resume publishes or aborts deterministic transition.  
**Invariants:** T2.

### TOP-003 — Crash after parent seal before child publication
**Status:** Existing family  
**Procedure:** GET/HEAD/long-poll/SSE/scan during gap; no permanent closure; resume transition.  
**Invariants:** T2, T3.

### TOP-004 — Crash after child seed before phase B
**Status:** L1-now  
**Procedure:** repeated resume is idempotent; no duplicate child state.  
**Invariants:** T2.

### TOP-005 — Crash after phase-B publication before parent retirement
**Status:** L2-sim  
**Procedure:** reads and appends follow successors; parent retained until safe GC.  
**Invariants:** T2, H13.

### TOP-006 — Split children use distinct physical routes
**Status:** Existing  
**Procedure:** assert different engines/owners when load is splittable.  
**Invariants:** T6.

### TOP-007 — Post-split capacity increase
**Status:** Existing mechanism gate  
**Procedure:** before/after throughput under controlled object-store model; ≥ target increase on distinct owners.  
**Invariants:** T6.

### TOP-008 — Dominant key refuses split
**Status:** Existing  
**Procedure:** one key dominates; no useless split; `hot_key` surfaced.  
**Invariants:** T7.

### TOP-009 — Stream-Seq through predecessors
**Status:** Existing  
**Procedure:** accept sequence on parent, split, duplicate/conflict and N+1 on child.  
**Invariants:** T9, P5.

### TOP-010 — Producer lane through predecessors
**Status:** Existing  
**Procedure:** exact retry after split deduplicates at original offset.  
**Invariants:** T9, P4.

### TOP-011 — Merge exact lineage
**Status:** Existing  
**Procedure:** cold children merge; key reads/scan/SSE exact across predecessors.  
**Invariants:** T2, T9.

### TOP-012 — Stale scaler decision after recreate
**Status:** Existing  
**Procedure:** heat/decision from epoch A cannot split epoch B.  
**Invariants:** T8, L14.

### TOP-013 — Phase-B CAS uses pending-read epoch
**Status:** Existing/strengthen  
**Procedure:** recreate name between reading pending and phase B; stale transition declines.  
**Invariants:** T8.

### TOP-014 — Stale router replay
**Status:** L2-sim  
**Procedure:** router sends to non-owner, receives replay target, converges within bounded hops; no record loss.  
**Invariants:** T10.

### TOP-015 — Ring preference versus possession
**Status:** L2-sim  
**Procedure:** node possesses shard while ring changes; no second owner claim until possession transition/fence.  
**Invariants:** T11, T12.

### TOP-016 — Two owners race to open
**Status:** Existing/L2  
**Procedure:** both open same prefix; exactly one acknowledges, loser tasks terminate.  
**Invariants:** T11, R1.

### TOP-017 — Split under load with response loss
**Status:** Existing field/focused  
**Procedure:** appends continue through seal/replay; exact per-key counts and order.  
**Invariants:** T2, T10.

### TOP-018 — SSE across split
**Status:** Existing  
**Procedure:** subscriber begins before split, follows lineage, sees each record once and controls remain valid.  
**Invariants:** T3, W5.

### TOP-019 — Snapshot scan across split/merge
**Status:** Existing  
**Procedure:** scan snapshot fixed at start; transitions do not omit or duplicate records.  
**Invariants:** T2, S7.

### TOP-020 — Repeated split/merge does not leak parents/tasks
**Status:** L2-sim  
**Procedure:** oscillating load through many transitions; bounded DBs, tasks, parent objects, and sketches.  
**Invariants:** R1, R2, H13.

---

# F. History, postings, caches, trim, and GC

### HIS-001 — Acknowledged data survives absorption
**Status:** Existing  
**Procedure:** append, absorb, trim, merged read exact.  
**Invariants:** H1, H2.

### HIS-002 — V2 absorption needs no customer key
**Status:** Existing  
**Procedure:** key cache empty/restarted; copy encrypted frames into history.  
**Invariants:** H3.

### HIS-003 — History survives owner handoff
**Status:** Existing  
**Procedure:** absorb on A, fence, read/continue on B.  
**Invariants:** H1, T11.

### HIS-004 — Aggregate gather byte bound
**Status:** Existing  
**Procedure:** many streams/large frames; one batch stays within budget except explicit one-item oversized allowance.  
**Invariants:** H5, R2.

### HIS-005 — Oversized chunk makes progress alone
**Status:** Existing  
**Procedure:** first frame exceeds budget; process alone, no starvation.  
**Invariants:** H5, H9.

### HIS-006 — Budget-deferred streams remain pending
**Status:** Existing  
**Procedure:** disable sweep, force budget skip, next tick absorbs without new signal.  
**Invariants:** H6.

### HIS-007 — Dirty restart discovery without touch
**Status:** Existing  
**Procedure:** append, crash before absorption, new owner, zero customer requests, absorb and clear marker.  
**Invariants:** H7.

### HIS-008 — Large single record restart accounting
**Status:** Existing  
**Procedure:** one large frame under production policy; marker bytes make it eligible.  
**Invariants:** H7, H9.

### HIS-009 — Dirty scan retries after failure
**Status:** Existing  
**Procedure:** first scans fail/truncate, later succeeds, untouched streams converge.  
**Invariants:** H8.

### HIS-010 — Sparse intentional deferral remains visible
**Status:** Existing  
**Procedure:** tiny sparse record stays deferred and counted after restart.  
**Invariants:** H7, H15.

### HIS-011 — Pending summary clears on close/move
**Status:** Existing  
**Procedure:** ownership loss removes old owner summary; new owner alone reports backlog.  
**Invariants:** H15.

### HIS-012 — Global trim budget on mature second wave
**Status:** Existing  
**Procedure:** 1,024 mature streams with old safe prefixes receive new data; no commit builds millions of tombstones, eventual trim converges.  
**Invariants:** H4, H5.

### HIS-013 — Duplicate absorbed update does not advance trim
**Status:** Existing  
**Procedure:** repeat boundary; trim remains based on previous safe boundary.  
**Invariants:** H4.

### HIS-014 — History partition close race
**Status:** Existing/strengthen  
**Procedure:** engine closes while partition OnceCell open is in progress; late DB closes and cannot survive old engine.  
**Invariants:** R1, H13.

### HIS-015 — Empty-root shard history path
**Status:** L1-now  
**Procedure:** one-shard topology with empty prefix; history path normalized and readable.  
**Catches:** `/history2` path drift.  
**Invariants:** H1.

### HIS-016 — Postings storage ratio
**Status:** Existing campaign/property  
**Procedure:** incompressible b1/b10 workload; postings/canonical and total bytes within gates.  
**Invariants:** R8.

### HIS-017 — Sparse postings bounded spans
**Status:** Existing  
**Procedure:** fragmented key; planner emits ≤8 spans and bounded scan bytes.  
**Invariants:** H10.

### HIS-018 — Hard amplification bound
**Status:** L1-now  
**Procedure:** tiny matches separated by 1–64 KiB gaps; planner never exceeds configured amplification.  
**Invariants:** H10.

### HIS-019 — Corrupt postings fallback
**Status:** Existing  
**Procedure:** corrupt/missing page; canonical envelope scan, exact frames, honest incomplete/error.  
**Invariants:** H11.

### HIS-020 — Large keyed record pages through
**Status:** Existing  
**Procedure:** record above read budget advances cursor and returns one oversized item.  
**Invariants:** H9.

### HIS-021 — Long keyed run pages with consumed progress
**Status:** Existing  
**Procedure:** run exceeds scan budget; partial pages advance consumed cursor without loops.  
**Invariants:** H9, H10.

### HIS-022 — Reader cache cold stampede
**Status:** Existing  
**Procedure:** 64 callers, one open, ≥63 coalesced.  
**Invariants:** H12.

### HIS-023 — Reader cache stale stampede
**Status:** Existing  
**Procedure:** one probe, one reopen, all callers correct.  
**Invariants:** H12.

### HIS-024 — All callers cancel, open still lands
**Status:** Existing  
**Procedure:** cancel every waiter; cache-owned open completes; next read hits.  
**Invariants:** H12.

### HIS-025 — Probe error does not evict healthy reader
**Status:** Existing  
**Procedure:** deterministic transform/data error, not transient store retry; retain cached reader.  
**Invariants:** H12.

### HIS-026 — Independent node/store caches
**Status:** Existing  
**Procedure:** same hash/key on two stores; each node reads own data.  
**Invariants:** H12.

### HIS-027 — Process-wide postings cache budget
**Status:** L2-sim/performance  
**Procedure:** many shard engines, one million keys/100 active; cache stays within one process budget and warm active hit rate gate.  
**Invariants:** H12, R2.

### HIS-028 — GC cached inventory cannot suppress work
**Status:** Existing fork/campaign gate  
**Procedure:** boot inventory stale, obsolete object appears, refresh logic finds it before TTL dead zone.  
**Invariants:** H14.

### HIS-029 — Latest-version probe gap fallback
**Status:** L1-now/fork  
**Procedure:** N known, N+1 missing, N+2 present; bounded fallback finds later version or writer invariant proves impossible.  
**Invariants:** H14.

### HIS-030 — GC concurrent delete throughput and partial failure
**Status:** L2-sim/performance  
**Procedure:** 16-way deletes, partial failures, retries; foreground WAL remains within budget and objects converge.  
**Invariants:** H13, H14, R8.

---

# G. Consumer groups and watches

### QUE-001 — FIFO per routing key
**Status:** Existing  
**Procedure:** interleaved keys, concurrent pulls; each key delivered in order.  
**Invariants:** Q1, Q2.

### QUE-002 — Lease generation fences stale ack
**Status:** Existing  
**Procedure:** lease expires and redelivers with new generation; old ack ignored.  
**Invariants:** Q3.

### QUE-003 — Ack durability before settlement response
**Status:** L1-now  
**Procedure:** pause/fail state write; settlement success waits for durable state.  
**Invariants:** D1, Q4.

### QUE-004 — Crash after lease durability before response
**Status:** L1-now  
**Procedure:** client retries pull; message not concurrently leased twice under same group.  
**Invariants:** Q3, Q5.

### QUE-005 — Visibility timeout redelivery
**Status:** Existing  
**Procedure:** no ack, advance time, redeliver with incremented generation/count.  
**Invariants:** Q5.

### QUE-006 — Retry and extend race
**Status:** L1-now  
**Procedure:** stale retry, ack, and extend in forced orders; only matching generation mutates.  
**Invariants:** Q3.

### QUE-007 — DLQ transition atomicity
**Status:** Existing/strengthen  
**Procedure:** crash around poison settle and DLQ append; never lose message or duplicate terminal state.  
**Invariants:** Q6.

### QUE-008 — DLQ target recreation
**Status:** L1-now  
**Procedure:** target deleted/recreated same name; pinned epoch prevents silent delivery to replacement.  
**Invariants:** Q8.

### QUE-009 — Consumer split lineage
**Status:** Existing  
**Procedure:** pull before/after split; no duplicate settlement or missing messages.  
**Invariants:** Q7.

### QUE-010 — Consumer merge lineage
**Status:** L1-now  
**Procedure:** active leases across merge; successor state exact.  
**Invariants:** Q7.

### QUE-011 — Consumer delete with active leases
**Status:** L1-now  
**Procedure:** delete/recreate consumer group; old tokens cannot mutate new incarnation.  
**Invariants:** Q3, L14.

### QUE-012 — Bounded queue-state loading
**Status:** L2-sim/performance  
**Procedure:** many consumers/leases; scans, memory, and object requests remain bounded.  
**Invariants:** R2, R8.

### WAT-001 — Watch only after durable readable append
**Status:** Existing  
**Procedure:** pause remote durability; waiter must not wake; release and require read sees data immediately.  
**Invariants:** W1.

### WAT-002 — Watch resync after missed journal
**Status:** Existing/strengthen  
**Procedure:** evict/overflow journal, request old cursor; explicit resync.  
**Invariants:** W2.

### WAT-003 — Signed capability survives restart
**Status:** Existing  
**Procedure:** new process with no decryption key cache verifies URL from persisted watch verification key.  
**Invariants:** W3.

### WAT-004 — Exact signed route only
**Status:** Existing  
**Procedure:** legal names containing `/watches/` and `/keys/`; no auth bypass except exact route enum.  
**Invariants:** W4, S1.

### WAT-005 — Watch split lineage
**Status:** L1-now  
**Procedure:** watch active across split/merge, matching updates once.  
**Invariants:** W5.

### WAT-006 — Watch definition lifecycle ABA
**Status:** L1-now  
**Procedure:** delete/recreate collection while old watch update is parked; replacement unchanged.  
**Invariants:** L14, W3.

---

# H. Security, API, conformance, and SDK

### SEC-001 — Product bearer matrix
**Status:** Existing  
**Procedure:** every route: absent/wrong/correct token; exact signed watch exception only.  
**Invariants:** S1.

### SEC-002 — Auth before body buffering
**Status:** Existing/static + L3  
**Procedure:** unauthenticated 32 MiB request rejected before body allocation/read.  
**Invariants:** S2, R2.

### SEC-003 — CORS preflight and actual responses
**Status:** Existing/outer-loop  
**Procedure:** explicit Authorization and custom headers accepted; success/error/streaming responses expose required headers.  
**Invariants:** S3.

### SEC-004 — Wrong key cannot mutate lifecycle
**Status:** Existing  
**Procedure:** wrong key on create resume, seal, fork, consumer/DLQ config; no state change.  
**Invariants:** S5.

### SEC-005 — Raw default-key isolation under split
**Status:** Existing  
**Procedure:** product writes non-empty keys and splits; raw append/read/SSE/fork sees only empty key.  
**Invariants:** S6.

### SEC-006 — Cursor binding and tamper resistance
**Status:** Existing/strengthen  
**Procedure:** use cursor with wrong stream epoch, key, map version, or operation type; reject.  
**Invariants:** S7.

### SEC-007 — Catalog dense tombstones and vanished objects
**Status:** Existing/strengthen  
**Procedure:** provider page underfull after dead/vanished entries; continuation still reaches later live streams.  
**Invariants:** S8.

### SEC-008 — Catalog transient GET failure
**Status:** L1-now  
**Procedure:** one descriptor GET fails; page retries/fails honestly, never advances past omitted live resource.  
**Invariants:** S8.

### SEC-009 — Clean-switch rejections
**Status:** Existing  
**Procedure:** legacy ordering/profile/scaling headers, old descriptor versions, old routes rejected.  
**Invariants:** S9.

### SEC-010 — Dual-surface equivalence under keyed load and split
**Status:** Existing/strengthen  
**Procedure:** raw default key and product API share canonical data while product non-empty keys split.  
**Invariants:** S6, S10.

### SEC-011 — Official conformance exact outcome
**Status:** Existing CI  
**Procedure:** pinned suite exact 332/0/6; changed skips/failures fail CI.  
**Invariants:** S10.

### SDK-001 — Pipelined subscribe partial-body safety
**Status:** Existing benchmark test  
**Procedure:** headers+cursor then truncated body; discard speculative response and retry committed cursor.  
**Invariants:** exact read delivery.

### SDK-002 — Abort in-flight long poll and retry backoff
**Status:** Existing/strengthen  
**Procedure:** abort during fetch and during Retry-After sleep; prompt termination.  
**Invariants:** resource bound.

### SDK-003 — Producer operation chain bounded
**Status:** L1/SDK  
**Procedure:** one million routing keys with sparse activity; resolved chain entries evict.  
**Invariants:** R2.

### SDK-004 — Producer-backed seal serialized with appends
**Status:** Existing  
**Procedure:** concurrent append/seal same producer lane; monotonic sequence and exact final.  
**Invariants:** P1, L2.

---

# I. Fleet, ownership, router, and liveness

### FLT-001 — OpenGate cancellation storm
**Status:** Existing single-node  
**Procedure:** impatient callers around slow replay; one open, no detached replay storm.  
**Invariants:** R1, R8.

### FLT-002 — Late open reaped after deadline
**Status:** Existing  
**Procedure:** deadline returns retryable result; late engine closes and never enters serving map.  
**Invariants:** R1, T11.

### FLT-003 — Engine dies young and holdoff escalates
**Status:** Existing  
**Procedure:** repeated open→die cycles; bounded retries and no storm.  
**Invariants:** R2.

### FLT-004 — Ring convergence under staggered cold starts
**Status:** L2-sim  
**Procedure:** nodes join over simulated minutes while traffic continues; reproduce historical 371,900-record loss shape and require exact set.  
**Invariants:** T10–T12.

### FLT-005 — Stale router replay bound
**Status:** L2-sim  
**Procedure:** route cache several versions stale; ≤ configured replay hops, no duplicate dispatch commit.  
**Invariants:** T10.

### FLT-006 — Ownership handoff with unabsorbed data
**Status:** L2-sim  
**Procedure:** crash owner, no customer touch to selected stream; new owner discovers dirty marker and drains.  
**Invariants:** H7, T11.

### FLT-007 — Old owner tasks terminate after movement
**Status:** Existing/L2  
**Procedure:** committer, pump, acker, flush ticker, absorber, history DB all exit/join.  
**Invariants:** R1.

### FLT-008 — Pending summaries do not double count after move
**Status:** Existing/L2  
**Procedure:** old row clears; new owner reports one backlog.  
**Invariants:** H15.

### FLT-009 — Node crash versus pause
**Status:** L2-sim  
**Procedure:** pause retains memory/ownership timeout behavior; crash drops state and requires reconstruction.  
**Invariants:** T11, R1.

### FLT-010 — Fleet desired-state CAS conflict
**Status:** L2-sim  
**Procedure:** several nodes compute desired state; CAS losses converge without write storm.  
**Invariants:** liveness, R8.

### FLT-011 — Scale out/in with shard movement
**Status:** L2-sim/L3-field  
**Procedure:** 1→N→1, traffic throughout, no fence war or phantom owner.  
**Invariants:** T11, T12.

### FLT-012 — Half-fleet loss under peak load
**Status:** L2-sim/L3-field  
**Procedure:** crash half nodes; survivors acquire shards within bound and exact data remains.  
**Invariants:** liveness, T11.

### FLT-013 — Stable fleet request-cost bound
**Status:** L2 performance  
**Procedure:** N instances over long virtual time; heartbeat/list/peer reads remain within O(N) budget.  
**Invariants:** R8.

### FLT-014 — Router and server disagree on map version during split
**Status:** L2-sim  
**Procedure:** stale map route, sealed parent response, retry/resume; no user-visible closure.  
**Invariants:** T2, T10.

### FLT-015 — Heal phase convergence
**Status:** L2-sim  
**Procedure:** fault-heavy multi-node phase, heal one viable core, require all lifecycle/topology/history/debt measures converge.  
**Invariants:** all liveness families.

---

# J. Resource, overload, and cost regressions

### RES-001 — Every engine task joins
**Status:** Existing  
**Procedure:** close/fence with queued work, join handles, panic distinguished from success.  
**Invariants:** R1.

### RES-002 — Handle eviction preserves safety state
**Status:** Existing/strengthen  
**Procedure:** dirty markers, fences, producer state, consumer state survive or remain reconstructible.  
**Invariants:** R3, R4.

### RES-003 — 65,536 tracking overflow is fail-closed
**Status:** Existing campaign/test  
**Procedure:** >65,536 streams; shared conservative bucket and aggregate counters visible.  
**Invariants:** R6.

### RES-004 — Billing checkpoint after successful emit only
**Status:** Existing/strengthen  
**Procedure:** emit fails, later succeeds; full accumulated delta emitted once.  
**Invariants:** R7.

### RES-005 — Counter eviction generation
**Status:** L1-now  
**Procedure:** evict/recreate counter with cumulative value exceeding prior checkpoint; generation prevents undercount.  
**Invariants:** R7.

### RES-006 — Overload shed and recover
**Status:** Existing local/liveness  
**Procedure:** memory/load above line, 429, heal/release, successful writes resume within bound.  
**Invariants:** R5.

### RES-007 — Reject storm protection
**Status:** L2 performance  
**Procedure:** noncompliant client instant-retries 429; tarpit/admission preserves health and control loop.  
**Invariants:** R5.

### RES-008 — Event-loop starvation model
**Status:** L2 CPU model/L3 benchmark  
**Procedure:** inject long CPU jobs on one worker; dedicated runtime preserves ack progress; no timer starvation beyond budget.  
**Invariants:** liveness, R2.

### COST-001 — WAL request amortization
**Status:** Existing performance gate  
**Procedure:** b1/b10/large batch; WAL PUTs and Class A/GiB within budget.  
**Invariants:** R8.

### COST-002 — No LIST steady state
**Status:** Existing campaign/fork  
**Procedure:** 30 virtual minutes; LIST count within gate, WAL listing policy explicit.  
**Invariants:** H14, R8.

### COST-003 — Shared history cardinality slope
**Status:** Existing campaign  
**Procedure:** same bytes across 10, 1k, 100k streams; history Class A nearly flat.  
**Invariants:** R8.

### COST-004 — Routing-key postings do not shift cost to GETs
**Status:** Existing/strengthen  
**Procedure:** covering/postings equivalent workload; total Class A unchanged, spans/amplification bounded, warm cache hit.  
**Invariants:** H10, R8.

### COST-005 — Cache stampede request budget
**Status:** Existing  
**Procedure:** 64 callers cause one reader/open/index load.  
**Invariants:** H12, R8.

### COST-006 — Sparse L0 timer/recovery budget
**Status:** Future implementation gate  
**Procedure:** one append per minute; L0/manifest work follows WAL count/bytes/age budget, not one flush per append.  
**Invariants:** R8.

### COST-007 — Physical attempt ledger
**Status:** L2/L3  
**Procedure:** retries, 304/412, lost response; logical and physical counts reconcile and billable class is correct.  
**Invariants:** R8.

### COST-008 — Read fanout does not multiply origin GETs linearly
**Status:** L2 performance  
**Procedure:** 1, 100, 10k subscribers; ring/cache/coalescing keep Class B within bound.  
**Invariants:** R8.

---

# K. Historical incident-to-scenario mapping

| Historical failure | Required permanent scenarios |
|---|---|
| I4 ghost ledger never asserted | DUR-013 |
| shared RNG fault placement depended on arrival order | current seed-purity test + deterministic scheduler completion criterion |
| store errors looked harmless because caller had no deadline | DUR-010 |
| old engine/absorber zombie task cycles | FLT-007, RES-001 |
| one-block-per-GET history regression | COST-004, COST-005 |
| reader-cache cold/stale stampede | HIS-022–HIS-026 |
| detached shard-open replay storm | FLT-001, FLT-002 |
| 65,536-stream fail-open | RES-003 |
| unbounded history gather | HIS-004, HIS-005 |
| AbsorbedBatch trim explosion | HIS-012 |
| budget-skipped streams removed from pending | HIS-006 |
| dirty restart underestimated large record | HIS-008 |
| pending summary survived movement | HIS-011, FLT-008 |
| logical split without physical capacity | TOP-006, TOP-007 |
| split gap reported permanent closure | TOP-003 |
| oversized keyed read made no progress | HIS-020, HIS-021 |
| Stream-Seq/producer state not split-safe | TOP-009, TOP-010 |
| product signed-watch auth bypass | SEC-001, WAT-004 |
| descriptor-before-content create race | CRT-001, CRT-002, CRT-007 |
| wrong-key initialization resume | CRT-003 |
| phase-B topology under sealed collection | SEL-018, SEL-019 |
| raw close-with-content used empty intent | SEL-007–SEL-009 |
| `final:null` lost | SEL-002 |
| seal operation hash collision | SEL-003, SEL-004 |
| fork source epoch omitted | FRK-010 |
| recursive fork debt hidden behind intermediate name | FRK-015 |
| wall-clock seal takeover without fencing | SEL-020–SEL-026 |
| claim-to-final append ABA | SEL-015 |
| duplicate/idempotent close success before durability | DUR-002–DUR-005 |
| fence state evicted with handle | SEL-025 |
| unsafe time-based fence pruning | SEL-026 |
| state-dependent conflict before durability | DUR-006, DUR-008 |
| stale scaler/TTL mutation after recreate | TOP-012, WAT-006 |
| ring convergence data loss under staggered startup | FLT-004 |
| memory shed used non-decreasing metric | RES-006 plus L3 cgroup gate |
| GC stale inventory/dead zones/serial deletes | HIS-028–HIS-030 |

---

# L. Scenario acceptance template

Every new scenario description in code MUST contain:

```text
Failure class:
Production mechanism:
Deterministic ordering:
Required failpoint entered proof:
Expected model state before release:
Expected model state after release/restart:
Required mechanism counters:
Invariants checked:
Cost/resource budget:
Negative control or canary:
```

A test that manually edits storage to create a supposed post-crash state MUST explain why no production failpoint can create it. For lifecycle state machines, manual state construction is not accepted as the primary proof.

