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

## Stage 6 — Lineage — STATUS: DONE (2026-08-24)

In-place split survival for the product surface via an atomic source
swap. Design decisions (deliberate simplifications, reviewed against
the Stage-6 brief):

1. **Linearized cursor space, not `{seg_id, offset}`**: a selector lane
   (routing key; the default lane is the `""` lane) has exactly ONE
   live segment at any time, so the lane's segments chain over their
   sealed caps into ONE logical u64 — the feed's existing
   head/floor/ring/budget machinery is untouched. `LineageSource::
   locate()` maps logical offsets back to `(seg_id, segment-local)`
   for the wire. (A `{seg_id, offset}` cursor cannot be total over a
   fan-out without per-segment state; the linearization is total by
   construction.)
2. **Feed-owned source snapshot + generation** (`SourceSnapshot`,
   `source_changed` watch): sessions re-snapshot at every loop turn
   and park on (feed version, source generation, current-source
   notify, auth, client, heartbeat). Raw scalar sessions disconnect on
   any generation change (typed fallback; scalar cursors cannot name
   segments).
3. **Atomic swap under the driver permit**: a closed tail triggers the
   ONE descriptor refresh (`refresh_transition`): epoch mismatch /
   gone / incompatible → `Gone` (disconnect, no terminal); sealed →
   `Closed` (genuine, one terminal); longer compatible lineage →
   install + generation bump + continue driving. Compatibility =
   span-signature prefix (a live span may gain its sealed cap, spans
   may be appended — anything else is NOT a swap). No watchdog
   polling; a pending transition spawns the same resumable resume the
   legacy read path uses.
4. **Bugs the red battery caught**: the closed-branch `Solo` arm
   originally dropped the driving session's records (consume-without-
   deliver — lost data on a singleton swap); the lifecycle outcome
   must be REPEATABLE (every parked session observes it) while the
   version bump happens exactly once.

Red legs (all deterministic): split while parked at upToDate (in-place
continuation + genuine seal after), split during initial catch-up
(failpoint-parked attach), keyed lane across a split (isolation
intact), raw fallback disconnect (no terminal), two sequential splits,
delete/recreate isolation (distinct feed, no cross-incarnation leak),
plus unit legs for the span linearization and signature compatibility.
The legacy lineage streamer remains the connect-time path for
already-split streams; LiveFeed eligibility at connect is unchanged.

### Round-4 hardening (2026-08-24)

The fourth review's four Stage 6 blockers, all with red legs:

1. **Wire-position correctness**: `locate()` returns a typed
   `WirePosition { seg_id, local_after }` and every emitted cursor
   uses the SEGMENT-LOCAL offset — the old code wrote the linearized
   offset into `KeyCursor.offset`, which resume reads as segment-local
   and would have skipped records on reconnect.
2. **Batch/generation binding**: `PreparedRecord` carries its wire
   position bound at preparation time (sessions never re-locate a
   record against a newer source); `PreparedBatch` and the `Solo`
   outcome carry the source generation; raw sessions disconnect BEFORE
   emitting any post-swap batch. `FeedSubscription` captures the
   source generation and receiver atomically with the attach.
3. **Fleet posture (same-owner)**: a lineage that cannot be built
   locally (wrong-owner span, engine unavailable, inconsistent map)
   disconnects-and-reroutes IMMEDIATELY with a typed outcome — never
   a heartbeat-long retry limbo. Cross-owner continuation (remote
   sealed-predecessor reads, local live tail) remains Stage 7+ scope.
4. **Deterministic transition handoff**: a pending transition is
   completed via `scaler3::resume` AWAITED under the driver permit
   (bounded), then re-read immediately — continuation no longer
   depends on the 15-s heartbeat.

New legs: decode-and-resume across a split (cursor names the child
segment with segment-local offset 1; resume has no gap and no
duplicate), two shared subscribers through one swap (exactly-once on
both, both cursors decode), and the held-publication handoff (no
false terminal while held, prompt delivery on release).

### Round-5 hardening (2026-08-24)

1. **Seal-before-refresh**: a sealed descriptor with a compatible
   lineage extension is DRAINED first — genuine closure means "no
   future data after the FULL lineage", never "the installed source
   already has everything" (terminal-before-drain data loss closed).
   The terminal control is located against the CURRENT source at emit
   time.
2. **External adoption**: `LineageSource::build` resolves spans with
   the external `engine_for` (customer adoption stamp) — a maintenance
   sweep can never install custody over an engine serving a LiveFeed
   (custody leg reuses the R29 sweep machinery).
3. **Raw compatibility gate**: a raw session may only join a
   single-segment source at generation 0 — peeked before attach and
   enforced against the atomically captured join generation after; a
   late-attaching raw request gets the typed immediate-disconnect
   fallback.
4. **Resume result**: after any completed `scaler3::resume` await, the
   descriptor is re-read unconditionally — the boolean is not evidence
   the topology did not change (external-winner race closed).
5. **Drain-before-terminal**: the `Closed` outcome drains the retained
   ring BEFORE the terminal control — a session whose drive published
   and closed the lifecycle between another session's `take_visible`
   and its drive could otherwise terminal-close the loser with records
   still in the ring (found by the two-subscriber seal flake; transient
   engine errors during a build are typed `RetryLater`, never a
   feed-wide incarnation cutoff).

Legs: seal-before-refresh with two shared subscribers (successor
records exactly once each, one terminal each, terminal cursor IS
(child, local 2)); raw late attach after a swap (immediate EOF, no
scalar lineage data, no terminal); sweep-custody decline on the
swapped child engine.

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
