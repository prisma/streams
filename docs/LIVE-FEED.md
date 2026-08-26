# LiveFeed — the SSE subscription contract

One subscription engine serves every product-SSE shape and every
concurrency level. There is no direct-versus-hub implementation switch:
the one-subscriber and many-subscriber cases differ only in **retention
and read coordination**, never in protocol code.

This document is the long-term contract. The staged transition
(`docs/LIVE-FEED-PLAN.md`) COMPLETED at round 11.8 and its plan file
is deleted; the transition record is folded into §Transition record
below.

## Request flow

```text
HTTP route
  → parse, authenticate, authorize, resolve initial descriptor
  → construct SubscriptionSpec
  → acquire SSE capacity (slots)
  → get or create LiveFeed (registry, incarnation-safe)
  → run one SseSession against that feed
  → GatedSseBody (authoritative lease gate)
  → client socket
```

## The session

Every connection runs ONE state machine:

- prove authorization (generation-stable initial proof; re-proof on
  generation change or deadline — see `sse::auth`);
- poll the feed for progress;
- emit shared data events plus its OWN control frames;
- decide `upToDate`/`sealed` against the DURABLE frontier at send time;
- park until: feed version change, heartbeat tick, lease deadline,
  own cancellation;
- terminate on: genuine closure (exactly ONE final control, then EOF),
  authorization invalidation, lag disconnect, slow-client timeout.

## Lag policy (contract decision, 2026-08-22)

```text
A subscriber CONNECTING from an old cursor performs durable catch-up.
A subscriber still in its initial handoff that the ring overtakes
performs durable catch-up AGAIN — never a lag disconnect.
A subscriber that HAS reached live (an honest upToDate was emitted)
and LATER falls behind the feed floor is disconnected: the WIRE
receives a RESUMABLE EOF — no error control, no terminal sealed
control — while the typed reason lives SERVER-SIDE (the
FEED_LAG_DISCONNECTS counter and a cursor/floor log line). The client
resumes from its last delivered cursor; the SDK treats every
nonterminal EOF as reconnectable. (Round-9 review wording fix: a
lagging or blocked socket is not a reliable place to deliver one
final explanatory event, so none is promised.)
```

Slow subscribers must not become private historical readers. The stall
budget (how far behind live a subscriber may fall before disconnection)
is governed by the shared retention budget below.

## Retention policy

| Subscribers | Retention |
|---|---|
| 1 | none — the driving session consumes its batch directly |
| 2+ | bounded shared ring (`SSE_FEED_RING_BYTES` per feed, `SSE_FEED_TOTAL_BYTES` process-global) |

There is NO dedicated pump task. When progress is needed, one session
acquires the feed's driver permit, reads at most one bounded source
batch, formats each payload event once, publishes (or hands the batch
to itself when retention is zero), releases the permit BEFORE any
socket write, and wakes the other sessions via the feed version watch.
If the driving session disappears, another session takes over.

The process budget reserves the ACTUAL retained bytes — one exact
reservation per retained batch, released on eviction and at feed drop
— so mostly-idle shared feeds cost nothing and hundreds of shared
feeds fit the default budget. A publication that cannot reserve (or a
batch larger than the whole ring) advances the head WITHOUT
retention; subscribers below the new floor take the lag path above.

`SSE_FEED_TOTAL_BYTES=0` (or `SSE_FEED_RING_BYTES=0`) is the
singleton-only emergency posture: a second subscriber to the same
feed is refused with `503 subscription_capacity` BEFORE it attaches;
every admitted session drives for itself on the same code path.
(`SSE_FEED_RING_BYTES` / `SSE_FEED_TOTAL_BYTES` fall back to the
legacy `SSE_HUB_*` names during the transition.)

## Cursors

Internal: `FeedCursor { segment_id, offset }`. Ordinary single-segment
streams use segment 0 with stream-global offsets; split lineages name
their lineage position. The wire layer converts to raw scalar offsets,
raw epoch/segment tokens, or signed product key cursors.

Feed identity = `(stream ref, stream epoch, selector)` — stable across
splits and topology refreshes; NEVER keyed by the current segment
handle. Raw and product subscribers share the same decrypted data lane;
only their control vocabulary differs.

## Wire semantics (unchanged by this rewrite)

| Concern | Contract |
|---|---|
| Data encoding | JSON arrays; text as `data:` lines; binary base64 (`Stream-SSE-Data-Encoding`) |
| Status controls | decided against the durable frontier at SEND time; `upToDate` only when truly caught up |
| Genuine close | exactly ONE final control carrying `sealed/streamClosed`, then EOF |
| Topology transition | NOT terminal: product subscriptions survive splits IN PLACE via the feed's atomic source swap (Stage 6); raw scalar subscriptions and owner movement remain disconnect-and-resume (typed) |
| Slow client | bounded queue + bounded send deadline → disconnect-on-lag |
| Edge buffering | responses always carry `x-accel-buffering: no` |
| Billing | one subscribe meter at connect + one payload chunk meter per delivered record — unchanged |
| Status framing | CANONICAL = bare per-record cursor controls + standalone status controls decided at send time (hub style). The legacy direct path's flag-on-batch-last pairing is retired with it. |

## Non-goals

Changing wire or cursor token formats; delivery guarantees; edge
multiplexing; cross-project subscriptions/forks; token or policy
semantics; billing redesign; distributed collection sealing.

## Decision log

| Decision | Status | Evidence |
|---|---|---|
| Cooperative driver replaces pump task | ADOPTED (E1) | §Transition record |
| Controls emitted as separate chunks from sessions | REJECTED by measurement (E2: folded frames) | §Transition record |
| Solo retention = zero | ADOPTED (E3; retained==0 asserted, RSS −28%) | §Transition record |
| Ring default derived from stall-budget experiment | SUPERSEDED: 1 MiB pinned by the 1-GiB certification | deploy/profiles/compute-1g.env |

## Default engine + rollback (round 11.7)

`STREAMS_SSE_ENGINE` defaults to **`livefeed`** as of round 11.7. The
certification basis: the in-proc battery + engine matrix (CI:
`livefeed`, `livefeed-matrix`), the REAL three-instance fleet
certification (`bench/fleet/livefeed-cert.sh`, CI:
`livefeed-fleet-cert`), and the field-canary battery
(`bench/canary/livefeed-canary.sh`, tag `livefeed-canary-rc1`) — all
on the release posture with the pinned 1-GiB profile.

**Rollback switch**: `STREAMS_SSE_ENGINE=legacy` (kept selectable
until round 11.8 deletes the legacy engine). Roll back ONLY on
evidence the typed counters corroborate, never on load alone:

- **Delivery exactness regressions** — duplicate or missing records /
  duplicate terminals on a replay a legacy-pinned rerun serves
  correctly (compare `delivered_records` and the billing boundary
  against the reconciled generator ledger).
- **Cutoff storms without ownership movement** — sustained nonzero
  `cutoff_wrong_owner` / `cutoff_incarnation` deltas on
  `/v1/debug/load` while `fleet/overrides.json` and the ring are
  stable.
- **Feed-budget accounting drift** — `reserved_bytes` on an idle cell
  failing to return to zero, or `project_retention` rows growing
  without live subscribers.

Every livefeed failure mode is a NONTERMINAL EOF + server-side typed
reason + cursor resume; clients that reconnect-and-resume observe no
data difference between engines. A rollback therefore needs no data
migration in either direction — the engines share the wire contract,
the cursor vocabulary, and the durable store.


## Transition record (the deleted LIVE-FEED-PLAN.md, round 11.8)

Stages 0–7 (contract freeze, session shell, core, single-segment
cutover, selectors, forks, raw surface, lineage, cutover) and the
follow-up remediation rounds all landed and were certified per commit
by `scripts/gate.sh` + CI. Stage 8 (field validation) closed with the
real-fleet certification (`bench/fleet/livefeed-cert.sh`, CI job
`livefeed-fleet-cert`) and the field-canary battery
(`bench/canary/livefeed-canary.sh`, tag `livefeed-canary-rc1`). Round
11.7 flipped the default; round 11.8 deleted the legacy direct/hub
engines, the `STREAMS_SSE_ENGINE` selector, and the engine matrix — a
CI lint keeps the deleted symbols dead.

Benchmark read-out that survived the transition (local memstore rigs,
release build; harness `STREAMS_SSE_BENCH=1 cargo test -- bench_sse_`):

- SINGLETON (50x1): livefeed −7% wall / −28% RSS vs the legacy direct
  path — protect-the-singleton holds with no promotion threshold.
- FANOUT micro-burst (4x25): livefeed ~25% above the legacy hub on a
  tiny burst (330 vs 254 ms) with LOWER RSS; the residual is the
  cooperative driver's per-session scheduling vs a dedicated pump.
  OPEN OPTIMIZATION (tracked, non-blocking): hybrid driver — spawn a
  dedicated reader task only while subscribers >= 2.
- Efficiency invariant asserted in-suite: exactly ONE source read per
  append window regardless of subscriber count.

The Stage 7B exclusion table (8 engine-neutral contracts) was fully
replaced by livefeed legs in round 9c; zero uncovered exclusions
remained at deletion time. The matrix skip list died with the matrix.
