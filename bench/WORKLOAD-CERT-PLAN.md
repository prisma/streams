# Workload certification plan — single Compute instance (2026-08-19)

**Target workload** (Søren):

| dimension | target |
|---|---|
| W1 resident tenants | 10,000 projects on one cell |
| W2 writes | 1,000 wps aggregate; 100 tenants active per 5s window, rotating over the 10k population |
| W3 subscribers | 1,000 tenants with live subscribers, 1,000 subscribers each → **1,000,000 concurrent subscriptions** |
| W4 shed budget | < 0.1% of offered work refused |

## 1. Arithmetic against measured envelopes (before any field minute)

From mtten-20260819T102840Z (same instance class, product surface,
enforce): capacity ≈ **893–1,185 req/s** at 100 active tenants and
1 KiB singles; byte ceiling **≥ 16 MB/s** payload; sub-capacity p50
**73 ms**; typed shed only, zero errors.

- **W2 is in range but tight**: 1,000 wps ≈ 85–110% of the measured
  100-tenant capacity — before subscriber wakeup cost. Batch or a
  faster write path is headroom if needed; the campaign measures the
  real margin with subscribers attached.
- **W3 fan-out has a hard byte bound**: a write to a stream with
  1,000 subscribers is 1 write + 1,000 deliveries. At 1 KiB, the
  16 MB/s envelope caps deliveries at ~16k/s ⇒ **at most ~16 wps may
  land on fully-subscribed streams**. If all 1,000 wps hit subscribed
  streams, that is 1 GB/s of fan-out — no single instance serves
  that. The workload contract must therefore fix the OVERLAP: my
  proposed default is 10 of the 100 active writers per window are
  subscriber-tenants (10 wps × 1,000 = 10k deliveries/s ≈ 10 MB/s,
  inside the envelope). **Knob for Søren to confirm.**
- **W3 residency is the open question that decides feasibility**: at
  even an optimistic 2 KB resident per parked subscription, 1M subs
  ≈ 2 GB — over the 1-GiB profile before counting sockets and fds
  (1M fds is itself beyond any default ulimit), and the platform
  edge's per-service connection ceiling is unknown and probably the
  first wall. **The plan measures the per-subscriber cost curve and
  the actual ceiling rather than assuming either.** The honest prior:
  1M on ONE 1-GiB instance is unlikely without subscription-state
  slimming and/or an edge fan-out tier; the campaign will produce
  the number that says how many instances (or how much slimming)
  W3 requires.
- **W1 touches known 4,096-entry backstop trackers** (quota admission
  FIFO churn is designed-in, but 10k residents churn constantly) and
  a ~3.6 MB policy/grant feed — cheap to verify, easy to miss.
- **W4 (<0.1%) is currently blocked by an open observation**: today's
  warm instance shed ~30% at HALF capacity (typed admission shed,
  flat p50s) after ~2M appends — suspected RSS-line proximity on the
  survival profile. Root-causing that is the FIRST gate; the 0.1%
  target is unmeasurable until a sub-capacity run sheds ~zero.

## 2. Phases

**P0 — warm-shed root cause (gate for everything).** Restart the fra
rig; rerun the sub-capacity control (500 wps, 1 KiB). Expected: shed
≈ 0. Then re-warm (create 10k streams, run 10 min at capacity) and
rerun; if shed returns, chase the resident-memory line (phys_footprint
vs shed line, handle/cache eviction) until sub-capacity shed < 0.01%.
Deliverable: either "clean posture" or a fix.

**P1 — instrumentation + micro-audit (local, ~half day).**
- Per-subscription resident bytes: park 1k/10k SSE subscriptions on a
  local server, measure RSS delta per subscription + per-wakeup CPU.
- Ceiling audit: every limiter a parked subscriber crosses —
  admission inflight, subscription pool, per-project
  max_live_subscriptions (feed-delivered), fd soft limits, tokio task
  per connection, edge idle-timeout behavior for SSE.
- 10k-project residency: feeds at 10k (mtgen --projects 10000),
  tracker churn cost at 10k live projects, feed parse/refresh time.

**P2 — harness build (~1 day).**
- awsbench subscriber mode: `BENCH_SUBS_N`, `BENCH_SUBS_TENANTS` —
  open N product-surface subscriptions spread over M tenants from the
  co-located gen; count deliveries, disconnects, reconnects; emit
  per-window delivery-lag histograms. One gen instance realistically
  holds 10–50k client connections — higher rungs deploy K gen
  services (the fleet harness already supports multiple services).
- Rotating-writer mode: `BENCH_ROTATE_WINDOW_MS=5000`,
  `BENCH_ROTATE_ACTIVE=100` — deterministic window→active-set
  schedule over the 10k tenant space, offered 1,000 wps total.
- Shed accounting to 0.01% resolution: typed 429/503-origin counts vs
  offered, per window, per stage (already in the stats lines; add an
  offered-ops counter so the denominator is exact, not derived).

**P3 — laddered field campaign (one instance, fra, ~1 day).**
- L1 writes-only: 10k projects resident, 1,000 wps rotating 100/5s,
  30 min. Gate: shed < 0.1%, p50 ≤ ~150 ms, flat RSS.
- L2 subscriber ladder: 1k → 10k → 50k → (100k if the edge allows)
  parked subscribers across 1,000 tenants, sparse writes. Measures:
  RSS/sub, wakeup p99 (ring target: single-digit ms), reconnect
  churn. Produces S_max(1 GiB) and the cost curve.
- L3 combined: L1 writes + S_max subscribers + fan-out at the agreed
  overlap (default 10 wps on subscribed streams ⇒ 10k deliveries/s),
  60 min. Gates: total shed < 0.1%, delivery lag p99 < 250 ms,
  no RSS trend, zero errors.
- L4 (if platform offers a larger memory tier): repeat L2/L3 rungs on
  it to get $/subscriber scaling.

**P4 — verdict + report.** Per-dimension go/no-go for ONE instance;
the measured S_max against the 1M target expressed as "K instances at
S_max each, or these specific slimming items" (candidate list already:
subscription state size, shared tail parking per stream — 1,000 subs
on one stream should park on ONE wakeup source, not 1,000 — and edge
multiplexing). Campaign becomes the repeatable `workload-cert`
harness.

## 3. Contract fixed (Søren, 2026-08-19)

1. Fan-out overlap: **10 wps onto fully-subscribed streams** (10k
   deliveries/s at 1 KiB) — CONFIRMED.
2. Certification mode: **durable-cursor subscribe** — CONFIRMED.
3. Instance tier: **1 GiB is the only tier** — L4 dropped; the verdict
   is expressed as K × 1-GiB instances (or named slimming work).

P0/P1 results (2026-08-19; the earlier same-day bullet claiming
~1.3 KB/sub was a PROBE BUG — it measured idle keep-alive connections;
the product surface takes `:sse` as a COLON VERB, and a plain read at
tail returns `[]` immediately. Corrected below, all on true `:sse`):

- **P0 CLEARED**: sub-capacity shed is 0.000% on a FRESH instance
  (90,980 attempts, 505 rps, p50 75 ms) AND on a WARM one (after a
  10-min capacity burn: 0.000%, p50 76 ms). The one 30%-shed
  observation followed a much heavier 7-stage/2M-append accumulation
  and did not reproduce; suspected mechanism is absorber catch-up
  reservations against the shed line in the post-overload recovery
  window — a watch-item for L1/L3, not an open gate.
- **True parked-SSE cost: ~55–70 KB resident per subscription**
  (5k rung: +276 MB; 10k rung: 780 MB total RSS). Each subscription
  spawns a dedicated tokio task whose async state machine inlines two
  large read branches and captures its own StreamDesc clone, plus a
  64-slot Bytes channel. Consequence: **S_max(1 GiB) ≈ 8–9k parked
  subscriptions today**, and crossing it sheds WRITES (observed
  directly: at ~21k local subs the marker append got 429) — parked
  subscribers spend the same shed-line budget writes need.
  1M subscribers at current cost ≈ 110+ instances.
- **Named slimming items** (the deciding lever, est. 5–15× cheaper):
  (1) Box::pin the two read branches so the parked future is small;
  (2) share one Arc<StreamDesc> per stream instead of a clone per
  subscription; (3) channel 64→8 slots (also caps worst-case queued
  frames per slow subscriber 64 KiB→8 KiB); (4) longer-term: one
  reader task PER STREAM fanning to N subscriber channels — collapses
  per-sub cost to channel+response state (~2–4 KB), putting 1M in the
  4–8 instance range.
- **W1 blocker FOUND AND FIXED — tracker capacity vs the rotation.**
  The 10k-project residency itself is cheap (0.6 s boot-to-live,
  48 MB RSS, 3.8 MB feeds), but the certification rotation demands
  20 first-seen projects/s × 300 s un-evictable recency = 6,000
  tracked admission entries against the old 4,096 cap: a
  cert-pacing churn probe shed 19% with typed TrackerCapacity
  (matching arithmetic exactly). Fixed red-first —
  cert_rotation_over_ten_thousand_tenants_never_hits_tracker_capacity
  pins the shape (red: 1,904/10,000 refused) — by raising
  MAX_TRACKED_PROJECTS to 16,384 (~8 MiB worst case; the cap now
  holds the certified tenant POPULATION). MULTITENANCY tracker
  posture updated.
- **Fan-out latency is a non-issue at the workload's shape**: 1,000
  subscribers on one stream all received an appended marker with
  lag p50 23 ms / p99 33 ms from append start (append ack 32 ms);
  tail-parking is a shared per-stream Notify (no busy-poll), and
  appends into 1,000-parked-sub streams ack in 5–24 ms.

## 4. Superseded: original asks

1. Confirm the **fan-out overlap** default (10 wps onto
   fully-subscribed streams). Any number implies deliveries/s =
   overlap × 1,000; the byte envelope says ≤ ~16 wps at 1 KiB today.
2. Subscriber semantics to certify: durable-cursor SSE subscribe
   (default) vs deliver=applied — I'll run durable unless told
   otherwise.
3. Whether a larger Compute memory tier exists for L4 (1-GiB is the
   only profile we've deployed).

Everything else proceeds without input; P0 and P1 start immediately.
