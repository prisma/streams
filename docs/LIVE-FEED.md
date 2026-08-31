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

## Per-project retention allowance (round-12 decision, 2026-08-31)

The 1-GiB cell profile pins `SSE_FEED_TOTAL_BYTES=64 MiB` and
`SSE_FEED_PROJECT_BYTES=32 MiB` — kept at these values by the
round-12 review decision after the six directed field legs (see
docs/PERF-LIVEFEED.md §4). Name it exactly:

**a per-project, per-instance LiveFeed retention allowance.**

It is NOT durable stream storage, NOT a billing quota, NOT a
project-wide value aggregated across the cell, and NOT a maximum
amount of records a subscriber can receive. The contract:

> Up to 32 MiB of prepared LiveFeed data may be retained for one
> project on one cell instance. When the allowance is exhausted,
> appends still remain durable. The affected shared feed may publish
> without retention, causing subscribers behind its new floor to
> receive a nonterminal EOF. The SDK reconnects using the last
> emitted cursor. Delivery is lossless but latency and reconnect
> frequency are not guaranteed while the project remains over the
> allowance.

The exact byte value stays in operator and Control Plane integration
documentation — it is an instance-profile backstop configured through
the deployment environment, not a permanently fixed public product
limit. The eventual Control Plane shape is a
`ProjectQuotas.max_sse_retained_bytes` bounded above by the cell
profile's 32 MiB maximum (docs/CONTROL-PLANE-INTEGRATION.md §4).

Field evidence (2026-08-30, real Compute + Tigris, zero lost acked
records in every leg): the intended multitenant distribution NEVER
touches the caps (500 projects: 0 cap hits, 0 cuts, 0 reconnects);
concentrated single-project geometries get typed, lossless,
resume-exact churn (0.045% at 100x10, 0.2% at 500x2); the global cap
accounts to the byte (pinned at exactly 64.0 MB, zero drift). Do NOT
raise the project cap: with 64/32 one project can consume at most
half the process retention allowance, and leg 5 shows the memory the
raise would spend is already contended by instance-wide pressure.

**Operational thresholds** (hard limits are the final safety
boundary; alert earlier):

| Signal | Warning | Critical |
|---|---|---|
| Project retained bytes | ≥ 24 MiB | any SUSTAINED `project_cap_uncached` growth |
| Global retained bytes | ≥ 48 MiB | ≥ 58 MiB |
| Subscription health | — | sustained lag-cut/resume rate > 0.1% |

A transient nonzero counter during an intentionally hostile campaign
is acceptable; persistent cap hits under the normal multitenant
distribution are not.

**Known gap (shared-cell GA blocker, next RC after rc.3):**
retention isolation is perfect but instance MEMORY isolation is
absent — a noisy project's write volume can push the whole instance
to the RSS shed line, and admission shed is instance-global (leg 5).
The admission design needs BOTH static subscription pressure
(connections, feeds) and dynamic write pressure (queued append
bytes, write breadth, project-attributable unabsorbed debt); retained
SSE bytes stay governed by the existing exact budgets. Acceptance:
rerun leg 5 — noisy must receive typed project-local shed BEFORE
global RSS shed, victims at zero shed/cuts/reconnects with complete
tails — plus the 30-minute product-distribution leg to prove normal
projects are not rejected. **Status: implemented in round 13 — see
the next section.**

## Per-project memory-pressure admission (round-13 backstop)

Closes the gap above: a per-project estimate of instance-memory
occupancy gates NEW APPENDS project-locally before the noisy project
can push the whole instance to the global RSS shed line.

**Knobs** (the only two; everything else is model-versioned):

| Env | Default | Meaning |
|---|---|---|
| `PROJECT_MEMORY_PRESSURE_BYTES` | `0` (disabled) | engage watermark per project |
| `PROJECT_MEMORY_RELEASE_PCT` | `75` | release when pressure falls below this % of the watermark |

**Pressure model v1** (`PROJECT_PRESSURE_MODEL_VERSION=1`, printed at
boot and served under `debug/load.project_pressure_model`):

```
estimated_project_pressure_bytes =
    live_subs            × 32 KiB
  + live_feeds           × 16 KiB
  + retained_sse_bytes           (exact mirror of the retention ledger)
  + buffered_body_bytes          (exact, chunk-accurate; no ceiling reserve)
  + queued_bytes                 (exact append-queue occupancy)
  + unabsorbed_frame_bytes       (exact durable write debt, see below)
  + dirty_streams        × 64 KiB
```

Weights are per-unit RESIDENT-state estimates calibrated from the
round-12 memory model (26.28 KB/conn, 7.95 KB/feed measured; weights
round up for safety). Retained SSE bytes count as occupancy here but
are never REFUSED by this gate — retention keeps its own exact
budgets; the memory gate throttles new appends only. Every dimension
counts unconditionally, whether or not the corresponding quota is
configured (round-13.3: quotas are refusal lines, not counting
gates).

**Exact write-pressure attribution.** Unabsorbed frame bytes ride a
`StreamPressureBinding` on the stream handle: attribution publishes
inside the shard state lock on append success and retires on absorb.
The binding is seeded from the tail's persisted `unabsorbed_bytes`
at bind time, so durable write debt that survives a restart is
charged from the first append — it never starts from zero when
durable debt already exists.

**Admission order.** The gate runs after ordinary per-project quotas
and before the instance-global RSS gate. Pre-auth requests never see
it (round-13.1: authentication precedes every tarpit/capacity
answer; pre-auth keeps only catastrophic survival accounting).

**Behavior at the watermark.** Hysteresis latch: engage at
`PROJECT_MEMORY_PRESSURE_BYTES`, release below
`PROJECT_MEMORY_RELEASE_PCT`% of it. While engaged, new appends
(product and raw adapter) receive a typed, project-audited refusal —
`429` body error `project_memory_pressure`, `Retry-After: 1` —
lossless by construction (the record was never accepted). Existing
subscriptions, reads, and retention are untouched. One ops event per
engage and per release (never per refusal). A project with any
nonzero pressure dimension is pinned against admission-tracker
eviction.

**Sizing the watermark.** Field calibration (2026-08-31, battery v3)
showed the watermark MUST clear the largest supported single-project
profile: `subs×32K + feeds×16K + 32 MiB retention allowance + dirty
margin`. A 1,000-sub + 100-feed project models ~65 MB — a 48 MiB
watermark throttles it at ordinary load (leg A3), while the
adversarial noisy profile plateaus at ~46 MB, BELOW 48 MiB (leg A6:
gate never engages, instance hits the global RSS line — the exact
failure the backstop must prevent). The watermark is therefore a
deployment-profile decision sized between the largest legitimate
tenant and the noisy plateau; `0` (off) remains the default until a
profile pins it. Pure write-rate cache-fill remains under-attributed
by the resident-state model (leg A4: 600 w/s peaked at 8.1 MB
modeled) — the global RSS gate stays the final safety boundary.
