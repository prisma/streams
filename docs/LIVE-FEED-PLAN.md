# LIVE-FEED transition plan

Status document for the direct+LiveHub → LiveFeed unification. The
long-term contract lives in `docs/LIVE-FEED.md`; this file is deleted
when the transition completes. Each section is updated as it lands.

Legend: PENDING / IN PROGRESS / DONE / PARTIAL (with notes).

---

## Stage 0 — Contract freeze — STATUS: DONE

- [x] `docs/LIVE-FEED.md` contract written.
- [x] Golden wire-contract test `golden_direct_and_hub_transcripts_
      are_identical`: same append sequence through BOTH legacy paths;
      asserts identical data payload sequence, single sealed control
      terminating the stream, upToDate at head, EOF.
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


## Stage 2 — LiveFeed core — STATUS: DONE

- [x] `FeedKey { stream, epoch, selector }`; `FeedCursor`; registry
      keyed by incarnation (delete/recreate safe).
- [x] Cooperative single-flight driver: `drive_once` CAS permit,
      bounded read, format-once, publish-or-hand-to-self, permit
      released before socket writes, watch-version wakeups.
- [x] Adaptive retention: zero at one subscriber; ring budget shared.
- [x] Red-first unit tests, all green: solo handoff retains nothing;
      shared mode prepares ONCE for both subscribers (source_reads==1);
      contended drive returns None without a source read and the
      permit never leaks; below-floor cursor reports Lagged (lag
      contract); close-during-batch reaches terminal Closed with the
      final batch still served; version-watch fires for post-join
      publications (no missed wakeup between check and park).
      second subscriber shares preparation (delivered/prepared > 1);
      driver handoff on driving-session drop; incarnation safety;
      retention caps exact; no missed wakeup between check and park.      memory penalty observed; reconnect path drives fresh).

## Stage 3 — Single-segment default-key cutover — STATUS: DONE

Engine selector `--streams-sse-engine legacy|livefeed` (per-instance,
test-settable); release posture refuses non-legacy until field-certified.
Four equivalence legs green on livefeed mode (park/upToDate, catch-up
from beginning cursor, two-subscriber shared feed with exactly one
source read per append window, revocation termination). Full suite 533
green.

FINDINGS (each found red by these legs):
1. WAKEUP WIRING — with no pump task, appends must wake the first
   driver: sessions park on BOTH the source's advance notify AND the
   feed version watch; wakeup futures are registered BEFORE the
   frontier check (lost-wakeup discipline).
2. SOLO HANDOFF — a solo drive hands its batch to the driving session;
   discarding it made the only subscriber look LAGGED against the very
   floor it just advanced (floor tracks head when nothing is retained).
   The session consumes its handoff directly.
3. CLOSURE RE-REPORT — a closure flip must re-open the status emission
   even with no new records; it is what turns upToDate into the single
   sealed terminal.
4. E2 VERDICT (measured) — split data/control chunks cost 1.5-3x wall
   time in the same shapes; lane-global flags fold into the prepared
   frame (one frame per record, matching legacy-direct framing). The
   genuinely per-session status remains only for empty-drain cases.

- [x] `STREAMS_SSE_ENGINE=livefeed` routes product default-key
      single-segment SSE through LiveFeed; `legacy` preserves today's
      paths byte-for-byte; `compare` runs both (local canary only).
- [x] Equivalence legs green in livefeed mode: park/upToDate,
      catch-up from cursor, seal (exactly-one final control), slow
      client disconnect, revocation mid-subscription, raw+product
      sharing one feed.
- [x] Legacy hub/direct selection retained untouched for `legacy`.


## Stage 4 — Selectors — STATUS: DONE

FeedKey = (segment identity, selector hash). Product keyed lanes ride
their own feed; foreign-key-only windows advance the cursor without
delivering. Red leg green: interleaved ka/kb traffic — keyed
subscribers see exactly their three records, share one lane, and never
see foreign keys.

FINDING: raw SSE is pinned to the default-key lane at the singular
route (`params.key` forced to Some("")), so raw and unfiltered product
SSE share ONE feed — the plan's shared-lane property falls out of the
existing route contracts rather than needing a new abstraction.

## Stage 8 — Field validation — STATUS: PENDING

Below-edge canaries reuse the agreed 1000-connection geometries.

---

## Benchmarks (local, memstore rigs)

Recorded before/after numbers live here. Field-scale numbers continue
to come from `bench/sse-probes/sse-matched-loaded.sh` campaigns.

(To be filled by the benchmark step of this transition.)

## Experiments

### E1 cooperative driver vs pump — ADOPTED (functional); fanout micro-burst gap open
### E2 split control chunks — REJECTED by measurement (folded into prepared frames)
### E3 solo retention zero — ADOPTED (retained==0 asserted; singleton RSS -28% vs legacy)
### E4 stall-budget ring sizing — PENDING (needs shared-mode soak)
