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

DONE: production DEFAULT engine = livefeed; `legacy` remains an
explicit A/B bridge REFUSED under the release posture.

REMAINING (gated on Stage 6): delete sse_response / sse_hub_response /
sse_lineage_response, DirectGuard, join_direct_or_promote, hub ring
machinery, SSE_HUB_PROMOTE_AT; rename SSE_HUB_* → SSE_FEED_*; flip DST
rig default and rewrite the ~16 hub-mechanics tests (obsolete pins die;
wire-contract pins carry over). One focused session once Stage 6 lands.

## Stage 8 — Field validation — STATUS: PENDING

Below-edge canaries reuse the agreed 1000-connection geometries.

---

## Benchmarks (local, memstore rigs; RELEASE build)

Harness: STREAMS_SSE_BENCH=1 cargo test -- bench_sse_ --nocapture

| shape | engine | elapsed_ms | rss_delta_mb | delivered |
|---|---|---:|---:|---:|
| 50x1 x20 rec | legacy direct | 6827 | +13.2 | 1000 |
| same | **livefeed** | **6335** | **+7.8** | 1000 |
| 4x25 x10 rec | legacy hub | 234 | +1.9 | 1000 |
| same | **livefeed** | 333 | **+1.2** | 1000 |

Read-out:
- SINGLETON: livefeed −7% wall / −28% RSS vs legacy direct — the
  protect-the-singleton property holds with NO promotion threshold.
- FANOUT micro-burst: +42% wall on a 100-delivery burst dominated by
  wake/scheduling overheads (double-park cycle); RSS already lower.
  OPEN OPTIMIZATION before deleting legacy: contended drivers park
  instead of yield-spinning; batch status suppression.
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
