# Scenario Map Summary (WP-00 deliverable 6)

Machine-readable inventory: `test-scenario-map.json` (same directory).
Catalogue source: `docs/dst/SCENARIO-CATALOG.md` (189 scenarios).
Mapped against commit `685ea035`.

# (b) Summary

**Corpus counts**

- `src/dst/dst_tests.rs`: **368 tests** (349 `#[tokio::test]` + 19 `#[test]`; 36,922 lines). `src/dst.rs` itself: 0 tests (harness/model only). `src/dst/fixtures/`: only TLS fixture PEMs.
- Other `src/` files: **233 tests** across 32 files (largest: `quota.rs` 25, `sse/feed.rs` 19, `auth.rs` 15, `fleet.rs` 14, `history.rs` 13, `tenant.rs` 13, `product.rs` 12, `postings.rs` 11, `registry.rs` 11).
- **Total in `src/`: 601 tests.** External harnesses: `conformance/` (SEC-011), `sdk/smoke.mjs` (SDK-002 leg), `bench/awsbench` (SDK-001), `bench/costab` + `bench/fleet` (campaign/field scenarios).

**Status-label counts (189 scenarios)**

- Existing-family (incl. `Existing/strengthen`, `Existing/L2`, `Existing campaign/*`, `Existing CI`, etc.): **127** (of which pure `Strengthen`: DUR-005, CRT-006, SEL-021, SEL-022)
- L1-now-family (incl. `L1-now/L2-sim`, `L1-now/fork`, `L1/SDK`): **33**
- L2-family (L2-sim / L2 performance / L2/L3 / L2-sim/L3-field): **24**
- Future implementation gate: **1** (COST-006)

**Mapping outcome**

- Scenarios with ≥1 concrete test: **148** of 189.
- Scenarios with no test: **41** — of which 24 are L2-sim/L2-perf/L3/future (expected: need the simulator or fleet), and 17 are in-suite gaps (below).
- All 15 STATUS.md workstream-2 items are implemented with explicit scenario-ID comments or doc references (DUR-002/004/006/008, SEL-019/021/022-partial/026/027, CRT-007-single-creator, FRK-013 ×2, FRK-016-by-construction, QUE-003/004).

**Unmapped Existing/Strengthen scenarios (gaps that matter)**

- `FRK-004` (Existing) — cycle/depth defense; `MAX_FORK_DEPTH` exists untested.
- `TOP-008` (Existing) — dominant-key split refusal + `hot_key` surfacing untested (only detection unit test in `sketch.rs`).
- `TOP-013` (Existing/strengthen) — phase-B CAS pending-read epoch; no recreate-between-read-and-phase-B test.
- `SEC-002` (Existing/static+L3) — no unauthenticated-oversize-body test in code.
- `HIS-022/023/024/025/026` (Existing ×5) — the history-reader-cache mechanism was **removed** (LiveFeed/postings-cache replacement); catalogue entries point at a mechanism that no longer exists. Decide: retarget to LiveFeed/postings equivalents or drop.
- Campaign-only Existings with no in-suite test (by design, but worth noting): `TOP-017`, `HIS-016`, `HIS-028`, `COST-001`, `COST-003`.

**Unmapped L1-now scenarios** (label says "add immediately"): `CRT-008`, `FRK-008`, `FRK-012`, `FRK-017`, `FRK-019`, `HIS-029`, `QUE-006`, `QUE-008`, `QUE-010`, `WAT-005`, `WAT-006`, `SEC-008`, `SDK-003`.

**Partial-coverage mapped scenarios to review when refactoring** (mechanism or legs missing): DUR-005, DUR-007, CRT-007 (two-joiner variant open, #108), CRT-010/011/012, SEL-004 (key-version leg), SEL-020, SEL-022 (dispatch-gate only per STATUS.md), FRK-002/003/016, TOP-002/004/016, HIS-010 (**catalogue stale**: R26-1 deleted the deferred-sparse behavior; tests now assert age-absorption), HIS-014/027, QUE-007, WAT-001, SEC-004/007, SDK-002, COST-002/005.

**Task 5 — mechanism claims / entered-proof**: the suite enforces this structurally. File header (`dst_tests.rs:1-6`) mandates `require()` on coverage counters; 7 tests call `cov.require(&[mech::…])` (lines 284, 522, 654, 770, 904, 1021, 2922). Failpoint entered-proofs via `failpoints::parked(Fp::…, name)`: 58 call sites across the ABA/seal/fork/queue tests (registry in `src/failpoints.rs:42`, enumerable via `failpoint_registry_is_enumerable_and_described:24241`). Group-failure proofs: `group_failures_tripped()`, `appends_enqueued()`, `publish_parked_count()` etc. (~55 uses). Doc-comment "proves/forced/entered-proof" claims are concentrated in the workstream-2 block (14820-16260) and round-4 seal matrix (31766-31929), all backed by counters. Anti-vacuity meta-tests exist: `faults_actually_fire:66`, `lost_response_counter_tracks_applied_behaviour_only:153`, and the oracle negative controls (2404-2488).

# (c) `dst_tests.rs` section structure

Flat file — **no `mod` declarations**; grouping is by dash/`=` comment banners. Approximate ranges (test fn start lines):

- **L1-22** module doc + helpers (`mem`, `skey`)
- **L23 "the fault substrate"** — 34-198: seed purity, fault firing, lost-response semantics, op-class telemetry
- **L209 "scenarios over the real engine"** — 260-844: I1-I3 faults, handoff/dedupe, ambiguity, fenced owner
- **L909 "the tiered read path"** — 985-2306: absorption, gather window/barrier, pump, tail ring, trim
- **L2386 "the oracle itself must be able to fail"** — 2396-2488: oracle negative controls (DUR-015)
- **L2496 "the eu-central-1 reopen storm"** — 2619-3055: storm repro, OpenGate (FLT-001/002/003)
- **L3153 "the metadata-read surface (history reader cache + compactions GC)"** — 3224-5431: name is historical; actually gather pacing/budgets, restart rediscovery, trim budget, postings planner/cache, routing-v3 split/producer (banner predates reader-cache removal)
- **L5545 "seal-gap read semantics"** — 5551-6407: HTTP rig helpers + the 6 seal-gap tests (TOP-003)
- **L6499 "oversized keyed records / long runs"** — 6552-6633 (HIS-020/021)
- **L6945 "physical scaling"** — 7064-7484: split capacity (TOP-006/007), merge (TOP-011), SSE lineage (TOP-018)
- **L7608 "product-surface foundation"** — 7730-10888: spec stages 1-8 (product API, consumers QUE-001/002/005/007/009, watches WAT-001/002/003/004, CORS SEC-003)
- **L10955-13110 (no banner; audit rounds 1-7 cluster)** — 10955-12997: create anomaly (CRT-003/004), seal intent/identity (SEL-002/003/005/006/007), fork (FRK-005/009/010/011/014/015), raw close crashes (SEL-008/009), topology fencing (SEL-017/018)
- **L13113 "Round 8: seal claim is a LEASE"** — 13128-13623 (SEL-020/024/016/004/005)
- **L13787 "Round 9: fence is a DURABILITY barrier"** — 13798-14321 (SEL-022/023/025/014/012/028, TOP-012)
- **L14410 "Round 10: no successful answer before its durability barrier"** — 14422-14708 (DUR-003, SEL-026/015)
- **L14820 "DST expansion, workstream 2 (L1-now)"** — 14832-15707 (DUR-002/006/004, SEL-021, DUR-008, CRT-007, SEL-019/027, FRK-013)
- **L15795 "Round 12: queue joins applied/durable discipline"** — 15806-15938 (SEL-010 leg, QUE-003)
- **L16033 "Round 13: fork-reference saga survives creator crash"** — 16045-16160 (FRK-013 crash, QUE-004)
- **L16261 "Round 14: queue config group-local; fork releases incarnation-fenced"** — 16271-17360 (QUE-011 cluster)
- **L17440-21500 (no banner; MT multi-tenancy stages 4-8 + Søren-review red tests)** — 17447-21385
- **L21507 "ABA: a name outlives its contents"** — 21526-21959 (CRT-005/006, seal/delete/fork ABA)
- **L22042-22970 (no banner; audit-P0 contracts)** — dual-surface corpus (SEC-010), fork contract (FRK-001/006/007), create replay (CRT-001/002), resumable seal, fork epoch checks (FRK-010/018), raw default-key (SEC-005), catalog paging (SEC-007)
- **L23066 "deliver=applied: pre-durability subscribe mode"** — 23105-23456
- **L23568 "round 17: deletion names an incarnation"** — 23577-24049 (QUE-011 legs)
- **L24262 "round 18: the saga proves completion"** — 24272-24522
- **L24610 "round 19: consumer fences survive ownership move"** — 24621
- **L24742 "Telemetry cutover (`_` namespace)"** — 24750-26539 (billing/usage RES-004, maintenance R25/R26)
- **L26589 "R26-5: maintenance gate through PRODUCTION surfaces"** — 26610-26996 (RES-006 legs)
- **L27056 "R27-2: cold shard maintenance debt"** — 27104-27769 (R27-R30 sweep residency)
- **L27847 "SR-6c: D/A/B adversarial fixture"** — 28049-28328
- **L28383 "SR2 second review round red tests"** — 28430-28998
- **L29232-29880 (no banner; declared quotas + feeds)** — 29232-29873
- **L29890/29984/30051 "#270/#271/#272/#274 hub"** — 29995-30527
- **L30691 "Review round 3, Phase 0: long-lived authorization boundary"** — 30929-31397
- **L31468/31637/31695 "round-4 findings"** — 31564-31705
- **L31767 "DST expansion round-4: SEAL LIFECYCLE crash/resume matrix"** — 31780-31929
- **L32060 "cross-owner segment close"** — 32073-32163
- **L32223/32231 "LIVE-FEED Stage 0 + Stage 3 equivalence"** — 32247-32379
- **L32405 "LIVE-FEED local benchmarks (STREAMS_SSE_BENCH=1)"** — 32416-32562 region
- **L32596 "LIVE-FEED follow-up-review red battery"** — 32608-33321
- **L33310 "Stage 6: source swap across splits"** — 33321-33447
- **L33447 "Stage 7B replacement legs"** — 33458-33847
- **L34005-34318 (round-11.x two-instance legs)** — remote predecessor, owner movement, cert delay
- **L35253 "Stage 6 round-4 legs"** — 35280-35454
- **L35536 "Stage 6 round-5 legs"** — 35545-35838
- **L35887 "Stage 7A: connect-time product lineage"** — 35930-36282
- **L36349-end (round-13 review items, no banner)** — 36349-36733: auth-before-admission, memory-pressure backstop, frame-debt restart, CODE-RED cut-resume repro

**Method note / caveats**: mappings are grounded in (1) explicit scenario-ID comments (12 hits, all post-catalogue L1-now work), (2) `docs/dst/STATUS.md`'s authoritative workstream-2 table, (3) doc-comment/name correspondence verified by reading test bodies for every uncertain case (CRT-001 legs, FRK-018 assertions, catalog bodies, watch durability path, gap-test shapes, failpoint usage). `coverage:"partial"` marks name-matched tests missing a catalogue-required leg; `mapped:false` means no test function exists in the current tree. The five HIS-022..026 entries are structurally unmappable because the mechanism they target was deleted from `src/` — flagging rather than silently re-mapping them.