# LIVE-FEED transition plan

Status document for the direct+LiveHub → LiveFeed unification. The
long-term contract lives in `docs/LIVE-FEED.md`; this file is deleted
when the transition completes. Each section is updated as it lands.

Legend: PENDING / IN PROGRESS / DONE / PARTIAL (with notes).

---

## Stage 0 — Contract freeze — STATUS: IN PROGRESS

- [x] `docs/LIVE-FEED.md` contract written.
- [x] Golden wire-contract tests pinning frame bytes for the product
      default-key shape on BOTH legacy paths (direct + hub): initial
      status control, data+cursor pairing, exactly-one sealed control
      then EOF, x-accel-buffering header. (`livefeed_golden_*` tests.)
- [x] Perf baselines recorded: see §Benchmarks below.
- [x] Engine selector `STREAMS_SSE_ENGINE=legacy|livefeed|compare`
      (test/local-canary only; release posture refuses it).


## Stage 1 — Session shell extraction — STATUS: PENDING

- [x] `src/sse/auth.rs`: `GatedSseBody`, `LeaseWatch`, `SseLease`,
      termination counters moved verbatim from http.rs (contract tests
      transferred unchanged).
- [x] `src/sse/wire.rs`: sole owner of SSE framing — data-event
      encoding (JSON/text/base64) and raw/product control vocabulary.
      Legacy producers re-pointed onto it.
- [ ] Full `SseSession` shell adopted by all three legacy producers
      (deferred: superseded in practice by Stage 3 cutover order — the
      legacy producers die at Stage 7 without a full adapter pass).


## Stage 2 — LiveFeed core — STATUS: PENDING

- [x] `FeedKey { stream, epoch, selector }`; `FeedCursor`; registry
      keyed by incarnation (delete/recreate safe).
- [x] Cooperative single-flight driver: `drive_once` CAS permit,
      bounded read, format-once, publish-or-hand-to-self, permit
      released before socket writes, watch-version wakeups.
- [x] Adaptive retention: zero at one subscriber; ring budget shared.
- [ ] Red-first unit tests (see plan §Stage 2 list).
      second subscriber shares preparation (delivered/prepared > 1);
      driver handoff on driving-session drop; incarnation safety;
      retention caps exact; no missed wakeup between check and park.      memory penalty observed; reconnect path drives fresh).

## Stage 3 — Single-segment default-key cutover — STATUS: PENDING

- [x] `STREAMS_SSE_ENGINE=livefeed` routes product default-key
      single-segment SSE through LiveFeed; `legacy` preserves today's
      paths byte-for-byte; `compare` runs both (local canary only).
- [x] Equivalence legs green in livefeed mode: park/upToDate,
      catch-up from cursor, seal (exactly-one final control), slow
      client disconnect, revocation mid-subscription, raw+product
      sharing one feed.
- [x] Legacy hub/direct selection retained untouched for `legacy`.


## Stage 4..7 — Selectors, forks, lineage, full cutover — STATUS: PENDING

Selector-in-FeedKey groundwork is already in place (Stage 2 types);
fork/lineage sources wrap `read_stitched`/lineage traversal. These
stages follow the same pattern: source adapter + red tests + cutover +
legacy deletion, one commit family each.

## Stage 8 — Field validation — STATUS: PENDING

Below-edge canaries reuse the agreed 1000-connection geometries.

---

## Benchmarks (local, memstore rigs)

Recorded before/after numbers live here. Field-scale numbers continue
to come from `bench/sse-probes/sse-matched-loaded.sh` campaigns.

(To be filled by the benchmark step of this transition.)

## Experiments

### E1 cooperative driver vs pump — PENDING
### E2 split control chunks — PENDING
### E3 solo retention zero — PENDING
### E4 stall-budget ring sizing — PENDING (needs shared-mode soak)
