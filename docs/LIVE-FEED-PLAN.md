# LIVE-FEED transition plan

Status document for the direct+LiveHub → LiveFeed unification. The
long-term contract lives in `docs/LIVE-FEED.md`; this file is deleted
when the transition completes. Each section is updated as it lands.

---

## Stage 0 — Contract freeze — STATUS: DONE

- `docs/LIVE-FEED.md` contract written.
- Golden wire-contract test `golden_direct_and_hub_transcripts_are_identical`:
  same append sequence through BOTH legacy producers; asserts semantic
  equivalence (identical data payload order, exactly one sealed control,
  upToDate at head, EOF).
- FINDING: legacy framings DIVERGE — direct pairs flagged controls per
  batch record; hub emits bare per-record cursors plus one send-time
  status control (#270/#272 rule). Canonical semantics for LiveFeed =
  hub framing; documented in LIVE-FEED.md.
- Perf baselines recorded (see §Benchmarks).

## Stage 1 — Session shell extraction — STATUS: DONE (core)

- `src/sse/auth.rs`: GatedSseBody / LeaseWatch / SseLease /
  TerminateOnce moved verbatim; contract tests transferred unchanged.
- `src/sse/wire.rs`: sole owner of SSE framing; all producers re-pointed.
- Full adapter pass over the three legacy producers superseded by the
  Stage 3+ cutover order.

## Stage 2 — LiveFeed core — STATUS: DONE

Cooperative single-flight driver (`drive_once`), adaptive retention
(zero solo / bounded ring shared), incarnation-safe registry, watch +
source-notify wakeups. Six red-first unit tests green.

## Stage 3 — Single-segment cutover — STATUS: DONE

`--streams-sse-engine` selector; four equivalence legs green; full
suite green. Session findings banked: wakeup wiring order, solo-handoff
consumption, closure re-report.

## Stage 4 — Selectors — STATUS: DONE

FeedKey = (segment identity, selector hash). Keyed lanes ride their own
feed; foreign-key windows advance without delivering. Red leg green.
FINDING: raw SSE is pinned to the default lane by the singular route,
so raw + unfiltered product SSE share one feed naturally.

## Stage 5 — Forks — STATUS: DONE

Forked SSE rides LiveFeed via `read_stitched` (child-global offsets);
raw scalar vocabulary composed byte-identically to legacy. Red leg
green (p0..p2 stitched + child tail).

## Raw surface — STATUS: DONE

Raw vocabulary leg green through LiveFeed (scalar controls only).

## Stage 6 — Lineage — STATUS: PENDING (attempted; two findings banked)

A first LineageSource (sequential traversal, absolute hop bookkeeping,
chain-walking frontier, watchdog-driven source swap) reached functional
delivery but hung a suite run. BANKED findings:

1. frontier() must WALK the remaining chain (sealed caps + live tail) —
   anchoring on a sealed parent strands sessions at upToDate forever.
2. The swap wakeup handoff is the hard part: parked sessions must
   re-arm on the NEW source's notify WITHOUT jumping cursors over
   interleaved foreign-key virtual slots; the first version hot-spun a
   session task post-teardown. Needs a design pass (per-generation exit
   signal + verified permit drain) before shipping.

Split streams remain fully served meanwhile: dispatch falls through to
the dedicated lineage streamer under both engines.

## Stage 7 — Cutover & deletion — STATUS: PARTIAL

POSTURE (corrected 2026-08-24, per the follow-up review): the DEFAULT
engine is `legacy`; `livefeed` is OPT-IN and REFUSED under the release
posture until Stage 6 lands and the engine is certified. (An earlier
revision of this document claimed livefeed was the production default —
that never matched the shipped binary.)

REMAINING (gated on Stage 6): delete sse_response / sse_hub_response /
sse_lineage_response, DirectGuard, join_direct_or_promote, hub ring
machinery, SSE_HUB_PROMOTE_AT; drop the SSE_HUB_* fallbacks of
SSE_FEED_RING_BYTES/SSE_FEED_TOTAL_BYTES; flip DST rig default and
rewrite the ~16 hub-mechanics tests (obsolete pins die; wire-contract
pins carry over). One focused session once Stage 6 lands.

## Follow-up review remediation — STATUS: DONE (2026-08-24)

The follow-up review's blockers, all fixed with deterministic unit legs
(`sse::feed::tests`, `sse::registry::tests`, `sse::session::tests`):

1. `leave_locked` returned the PRE-decrement count — feeds never left
   the registry. Now post-decrement, with a zero-decrement invariant.
2. The process-global budget was reserved per EXTRA subscriber AND
   again per retained batch (double count), and the allowance leaked.
   Interim fix: one allowance per shared feed. FINAL design (round 2,
   item 3 below): the budget reserves the ACTUAL retained bytes per
   batch, released on eviction/drop.
3. The captured `join_head`/`ver_rx` were returned but ignored — the
   session now uses them, closing the subscribe→serve handoff race.
4. Phase A never emits records at/after `join_head` (they arrive via
   the shared ring in Phase B — no duplicates).
5. Shared→solo (2→1) no longer clears the retained ring out from under
   an unread survivor; solo drives keep the floor put while the ring
   drains; retained bytes release at feed drop.
6. The source wake is REGISTERED at loop top (`Notified::enable()`)
   before any state read — an append between observation and park can
   no longer be missed until the heartbeat.
7. No-progress partial pages and source failures are typed outcomes
   that never bump the feed version — no wake-storm spin.
8. Product cursors name the ACTUAL live segment (`resolve_segment`),
   not hard-coded zero; feed identity is `desc.storage_hash()`.
9. The dormant dedicated-driver code (`spawn_shared_driver` /
   `ensure_shared_driver`) is deleted — cooperative driving is the one
   execution model.
10. `SSE_FEED_RING_BYTES`/`SSE_FEED_TOTAL_BYTES` are real now (legacy
    `SSE_HUB_*` fallback, loud on unparseable); the zero-budget
    contract is singleton-only, documented in LIVE-FEED.md.
11. CI: a dedicated `livefeed` job runs the engine's unit and HTTP
    legs on every push.

## Follow-up review remediation, round 2 — STATUS: DONE (2026-08-24)

The second review's six findings:

1. Shared admission no longer exposes `subscriber_count == 2` before
   its memory posture is valid: the 1→2 check is STATIC (nonzero ring
   AND nonzero global budget) and runs BEFORE the attach under the
   registry lock — a refusal leaves no partial state to roll back.
2. Initial catch-up can no longer be mistaken for live lag: sessions
   track `reached_live`; a ring that overtakes a not-yet-live session
   triggers durable re-catch-up to the current head (counted), never
   a disconnect. Genuine lag after `upToDate` still disconnects.
3. Budget geometry redesigned (model B): the process budget reserves
   the ACTUAL retained bytes per batch (released on eviction/drop)
   instead of one whole ring allowance per shared feed — the default
   16 MiB now fits hundreds of mostly-idle shared feeds instead of 16.
   The budget lives on `AppState` (per-rig isolation in tests), and a
   batch the last subscriber has fully passed is drain-released
   immediately (the budget cannot stay pinned after shared use).
4. Ring-zero is singleton-only at admission; an oversized batch
   (larger than the whole ring) is advanced WITHOUT retention — it can
   never be evicted before anyone consumed it. The driver read is
   capped at 2/3 of the ring so prepared batches fit in the ordinary
   case.
5. Version semantics exact: `Idle`/`NoProgress`/`SourceFailed` never
   bump; the close transition bumps exactly once (single outcome
   fold); sessions drain consecutive retained batches back-to-back
   without driving or parking between them.
6. `read_stitched` reports the CONSUMED boundary (scan progress over
   match-free ranges and drained ancestors), not the last matching
   record — fork lanes no longer rescan foreign-only windows.

Coverage added this round: a REAL abort leg (barrier-gated source,
actual task cancellation), the multi-batch drain leg, exact budget
geometry legs (actual-bytes reservation, exhaustion posture,
eviction release, zero-ring refusal), the deterministic join-head
handoff leg (failpoint between attach and session start), the
ring-wrap re-catch-up leg, the fork foreign-window leg (320 KiB of
foreign records consumed as progress), the binary base64 leg, the
exact bytes_out unit leg, and the exact terminal transcript leg. The
640-KiB window leg is now deterministic (driving paused at a new
failpoint until the whole window is durable). LiveFeed production
counters are exported under `/v1/debug/load.sse_livefeed`.

## Retained-ring rollover — STATUS: DONE (2026-08-24)

One contained memory-accounting patch from the third review:

1. Publication is a REPLACEMENT of reservations, not an independent
   new one: the eviction set is computed first, and
   `FeedMemoryBudget::try_replace` atomically swaps the evicted
   charge for the new batch's charge — a full ring rolls forward at a
   full global cap, and the cap is never exceeded.
2. Every uncached path (oversized batch, or a net reservation the
   budget cannot host) releases and clears the ENTIRE retained ring
   before advancing the floor — nothing unreachable keeps a global
   reservation.
3. The `read_stitched` fork-boundary corruption check moved BEFORE
   the cursor is forced to the cap (it previously compared after
   `cursor = max(cap)`, where it could never fire).

Legs: full-ring roll-forward at a full cap; external exhaustion
clearing the unreachable ring while another feed's retention stays
intact; oversized batch clearing the old ring; a barrier-driven
32-feed concurrent-retention herd (cap holds at every instant,
reserved == actual retained, teardown returns to zero).

The Stage 6 lineage work is unblocked from here.

## Stage 8 — Field validation — STATUS: PENDING

Below-edge canaries reuse the agreed 1000-connection geometries.

---

## Benchmarks (local, memstore rigs; RELEASE build)

Harness: STREAMS_SSE_BENCH=1 cargo test -- bench_sse_ --nocapture

| shape | engine | elapsed_ms | rss_delta_mb | delivered |
|---|---|---:|---:|---:|
| 50x1 x20 rec | legacy direct | 6827 | +13.2 | 1000 |
| same | **livefeed** | **6335** | **+7.8** | 1000 |
| 4x25 x10 rec | legacy hub | 254-268 | +1.6-2.5 | 1000 |
| same | **livefeed** | **330-358** | **+1.1-1.7** | 1000 |

Read-out:
- SINGLETON: livefeed −7% wall / −28% RSS vs legacy direct — the
  protect-the-singleton property holds with NO promotion threshold.
- FANOUT micro-burst: after the contended-park fix (losers fall
  straight to the version park; no yield-spin round) livefeed sits
  ~25% above legacy hub on this tiny burst (330 vs 254ms; RSS LOWER).
  The residual is per-session scheduling overhead of the cooperative
  driver vs a dedicated pump task. OPEN OPTIMIZATION: hybrid driver —
  spawn a dedicated reader task ONLY while subscribers >= 2 (shared
  streams are few by definition; singletons stay task-free).
- Efficiency invariant asserted: exactly ONE source read per append
  window regardless of subscriber count.
- Field-scale numbers come from bench/sse-probes/sse-matched-loaded.sh
  campaigns (Stage 8).

## Experiment verdicts

- E1 cooperative driver: ADOPTED functionally; fanout micro-burst gap
  open (see Benchmarks).
- E2 split control chunks: REJECTED by measurement — folded frames.
- E3 solo retention zero: ADOPTED (retained==0 asserted; RSS −28%).
- E4 stall-budget ring sizing: PENDING shared-mode soak.
