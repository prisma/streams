# Metrics, telemetry, observability, and billing — system review

**Date:** 2026-08-06 · **Head:** 93066698 · **Status: REVIEW — describes
what exists today, measures it against the billing + operations
requirements, and proposes the missing pieces. Nothing in §6 is
implemented.**

The requirements this review is written against:

1. **Billing** — charge per stream, monthly, on (a) data ingested,
   (b) data read, (c) data stored; all derivable from stored metrics.
2. **Dashboards** — cheap-ish per-stream usage lookup for customers.
3. **Operations** — see how the system performs; see events like stream
   splits and rebalances.

**Verdict in one paragraph.** The building blocks exist and are
individually sound — per-stream counters with hard cardinality bounds,
two interval emitters writing usage records to internal streams, a rich
per-instance operational surface, and durable split lineage in the
registry. But the system cannot bill correctly today: metering has
coverage holes (a split stream's reads bill zero; scans and consumer
pulls bill zero), the billing emitter is self-declared best-effort with
in-memory checkpoints (a restart re-bills full cumulative values),
"data stored" is not derivable at all (only cumulative ingest volume is
counted, in process memory), and nothing provides a cheap per-stream
lookup (a dashboard would have to scan a month of interval records).
Operationally, live state is well covered but **event history is not**:
splits survive in the registry, while rebalances, desired-count changes,
and instance transitions exist only as overwritten single objects plus
process logs. §6 proposes a design that fixes all of this with one
consolidated meter, one durable usage pipeline with monthly per-stream
rollup objects (one GET per dashboard query), a persistent stream-meta
byte gauge for storage billing, and a durable ops event journal.

---

## 1. Inventory: the five telemetry planes

### 1.1 Per-stream usage counters + service limits (`src/usage.rs`)

The hot-path accounting layer, keyed by the stream **name hash**
(`stream_hash(name)` — the shard-routing key, so one entry per stream
regardless of splits):

| counter | incremented | meaning |
|---|---|---|
| `requests` | append admission | append requests |
| `records` | append commit | records committed |
| `bytes_in` | append path | request body bytes |
| `bytes_out` | read response builders | response body bytes served |
| `plaintext_bytes` | commit | pre-compression stored volume (cumulative) |
| `frame_bytes` | commit | post-compress+encrypt bytes written (cumulative) |

Properties that matter for billing:

- **Bounded cardinality, never fail-open** (static-audit P0): at
  65,536 tracked streams, new streams share ONE conservative overflow
  token bucket and ONE aggregate counter set — limited and visible
  (`/v1/debug/usage` `overflow` block), but **with no per-stream
  attribution**, and never emitted per-stream to billing.
- **Idle eviction at cap** (≥600 s idle): counters restart at zero if
  the stream returns; each counters incarnation carries a process-unique
  `generation` so the emitter can tell evict-and-return apart from
  growth (value-regression detection alone under-billed the
  regrow-past-checkpoint case — review round 4).
- **In-memory only.** A process restart zeroes everything. This is the
  root of several downstream caveats.
- The same module owns the **per-stream token buckets**
  (`LIMIT_BYTES_PER_SEC` 5 MB/s, `LIMIT_REQS_PER_SEC` 1000,
  `LIMIT_RECS_PER_SEC` 5000, burst 2 s) whose rejections are named 429s
  — limits and metering deliberately share one admission point.

### 1.2 The billing emitter → `_billing` stream (`http.rs::spawn_billing`)

Every `BILLING_INTERVAL_SECS` (default 60), one JSON-array record is
appended to the internal stream `BILLING_STREAM` (default `_billing`,
encrypted with `BILLING_STREAM_KEY`; the emitter is **disabled with a
warning when the key is unset** — which is how every fleet campaign ran).
Each element:

```json
{"ts": …, "stream": "<name-hash hex>", "requests": Δ, "records": Δ,
 "bytes_in": Δ, "bytes_out": Δ,
 "plaintext_bytes_total": cum, "frame_bytes_total": cum}
```

Guarantees it DOES make: no interval is dropped (checkpoints advance
only after a successful append; failures retain the delta and retry
next interval), incarnations are distinguished by counter generation,
checkpoint memory does not grow with total streams ever seen.

Limits it explicitly declares (code comment, static-audit posture):
**best-effort usage telemetry, not a billing system of record.**
Checkpoints live in process memory, so **a restart re-emits current
cumulative values in full** (over-billing after every deploy/crash —
note all four fleet redeploys this week would each have re-billed every
active stream's lifetime counters); overflow-aggregate traffic is never
emitted; rows carry the name **hash**, not the name.

### 1.3 The metrics flusher → `__metrics__` stream (`src/metrics.rs`, `http.rs::metrics_flusher`)

A second, older, **parallel** interval system, keyed by stream **name**
(string): per-stream `{appends, append_bytes, reads, read_bytes,
queue_ops}`, drained every 15 s into one record on the `__metrics__`
stream, **written through the router like any tenant write** (fence
safety), enabled by `METRICS_KEY` + `METRICS_LB_URL`. Explicitly
**lossy by design** — an append failure logs and drops the interval.
Collection is off unless enabled (else the name-keyed map would grow
with total cardinality forever — static-audit finding).

The overlap with §1.2 is a review finding in itself: two meters, two
keyspaces (hash vs name), two cadences, two loss postures, each
covering a *different subset* of operations (§3.1). One of them should
not survive this review.

### 1.4 Live operational surfaces (pull-based)

- **`/operator` dashboard** (unsecured by product decision; payload
  restricted to operational metadata, never stream names/keys):
  instance load vector, open shards, ring active set, admission/shed
  counters (`admit_shed`, `stream_shed`, `wedge_shed`), RSS vs shed
  line, and per-op-class **store latency** from `store_timing`
  (60 s window; client vs Tigris-internal sniffer durations, so network
  vs server time is separable), plus the fleet heartbeat set and
  `desired.json`. Compiled-in runbook at `/operator/runbook`.
- **`/v1/debug/load`** (bearer-gated): absorb lag/pending rollups,
  admission counters, cardinality/postings stats, `streams_tracked`,
  trim counters, consumer-fence metrics (round 17), and the **scaler
  block**: `segment_splits`, `segment_merges`, `hot_keys` (dominant
  single-key streams), `ineffective_split_avoided`, `sketches`,
  `sketch_evictions`, `untracked_appends`, `segment_map_refreshes`.
- **`/v1/debug/usage`**: every tracked stream's cumulative counters +
  the overflow aggregate. **`/v1/debug/store`**, **`/v1/debug/timings`**:
  store health and latency detail.
- **Fleet heartbeats** (`fleetops/fleet/streams-N.json`, 2 s cadence):
  rps, ack p50, CPU %, inflight now/peak, RSS, WAL PUT p50/p99,
  outbound-op inflight/peak, owned shards, absorb-lag max, wedge max,
  draining, self URL. This is the load vector everything scales on.
- **Router `/stats`** (pilot LB): per-upstream reqs/errs/latency/
  cold-starts/**replays** (ownership-bounce count — nonzero steady
  state means the router's view is stale), desired, heartbeat echo,
  overrides, topology, 15-min per-second history ring.

### 1.5 Events and durable state

| event | durable record | history? |
|---|---|---|
| stream **split** | registry descriptor `segments[]`: seg_id, range, `created_ms`, `predecessors`, sealed boundary | **yes, per stream** (while segments exist; a later merge rewrites the map) — plus `segment_splits` counter + tracing log |
| stream **merge** | descriptor map rewritten | counter + log only |
| **rebalancer move** | `fleet/overrides.json` (CAS) | **latest state only** — overwritten; move/return history exists only in tracing logs |
| **desired count change** | `fleet/desired.json` (CAS): count, epoch, verbatim reason, computed_at | **latest only** — epoch proves changes happened, but N→N+1 history is gone (the fleet campaigns recovered reasons only because an observer polled) |
| instance join/dark/fence | heartbeat freshness; fencing in logs | none |
| stream create/seal/fork/delete | registry descriptor lifecycle | current state; no event log |

The pattern: **live state is durable and CAS-disciplined; event
*history* relies on process logs** (which on Compute are ephemeral) or
on campaign-time observers. That fails the "see when splits/rebalances
occur" requirement for anything after the fact.

---

## 2. Requirement (a)–(c): can we bill from what is stored today?

### 2.1 Data ingested — *mostly yes, with holes*

`bytes_in`/`records` deltas per interval land in `_billing` (and
`append_bytes` in `__metrics__`). Monthly ingest per stream =
sum of interval deltas over the month. Holes: emitter-off-by-default
(key unset), restart re-bill (§1.2), overflow aggregate unattributed,
and rows keyed by hash (join problem for invoices/dashboards, §2.4).

### 2.2 Data read — **no. Coverage holes bill real traffic at zero.**

Precise, verified against the code at head:

| read path | `usage.bytes_out` | `metrics.read` |
|---|---|---|
| standard single-segment read (`read_inner` tail path) | ✓ (http.rs:5773) | ✓ (5726) |
| long-poll empty timeout (billable probe) | — | ✓ 0-byte (5606) |
| fork reads | — | ✓ (5219/5265) |
| **v3 lineage reads — ANY split stream** (`read_v3_lineage_inner`) | **✗** | **✗** |
| SSE streaming | ✓ per chunk (6046/6249) | ✗ |
| **scan pages** (`product_scan`) | **✗** | **✗** |
| **consumer pull** (records delivered via `read_merged` + leases) | **✗** | **✗** (`metrics.queue` has **zero call sites** — `queue_ops` is dead) |
| internal fan-out relays | metered once at the owner (primary returns the relayed body without re-metering) — correct | — |

The dispatch structure explains the biggest hole: `read_inner` sends
any stream with >1 segment (or a pending transition) into
`read_v3_lineage_inner` **before** reaching its metering lines — so the
moment a stream splits, its read traffic disappears from both meters.
Heavy readers of large (therefore split) streams are exactly the
customers this under-bills.

### 2.3 Data stored — **not derivable.**

What exists: cumulative `plaintext_bytes`/`frame_bytes` per stream —
**in process memory**, reset on restart/evict, emitted only as
point-in-time totals piggybacked on billing rows. What "data stored"
billing needs is **GB-months of resident bytes**: a durable per-stream
byte gauge sampled over time, decreasing on stream deletion/idle-expiry
(`idleExpiry` exists and deletes whole streams). Nothing persists
per-stream stored bytes today — not in the descriptor, not in stream
meta, nowhere restart-safe. (Bucket-level totals exist via Tigris, but
have no per-stream attribution and include WAL/registry overhead.)

### 2.4 Cheap per-stream lookup — **no.**

Both `_billing` and `__metrics__` are append-only interval logs keyed
by time, not by stream. "Show stream X's usage this month" means
scanning ~43k interval records (60 s cadence) client-side. `_billing`
rows additionally carry only the name hash, so a dashboard must
maintain its own hash→name mapping. There is no rollup, no per-stream
index, no month boundary.

---

## 3. Operational requirement: how well can we see the system?

**Strong today:** live health. One `/operator` page per instance gives
load, admission/shed, store latency split into network vs
Tigris-internal, fleet view; heartbeats give the same machine-readable
at 2 s; `/v1/debug/load` exposes every internal subsystem's counters
(absorber, trim, postings, scaler, consumer fences); the router adds
the client-experienced view (the run-7 lesson — server-side ack latency
cannot see edge queueing — is institutionalized in the router load
report `routers/router-1.json`).

**Weak today:**

1. **No durable event history** (§1.5) — splits are reconstructable
   per stream from the registry, but "what happened in this cell
   yesterday" (moves, returns, desired changes, fences, wakes) is
   unanswerable without having run an observer.
2. **No retention/aggregation for time-series.** `__metrics__`/
   `_billing` grow forever at interval cadence; `/operator` windows are
   60 s in-memory rings; there is no downsampled long-term view of rps,
   latency, shed, or lag.
3. **Counters are process-lifetime and instance-local.** Cross-fleet
   totals (e.g. splits today across four instances) require polling
   every instance's debug endpoint and summing — and reset invisibly on
   redeploys (this week's campaigns redeployed servers five times).
4. **No alerting hooks.** OPS-RELEASE.md defines SLOs and drills, but
   nothing emits a signal when e.g. shed rises, lag crosses the
   rebalance threshold, or the ring loses an instance; discovery is
   dashboard-polling.

---

## 4. What the two internal streams give us (and their self-interaction)

Using streams as the telemetry transport is a good instinct — durable,
ordered, replayable, already multi-instance-safe (the flusher writes
through the router precisely so a shared stream doesn't fence-fight).
Two footnotes: `_billing` excludes its own usage from emission but
`__metrics__` does not (its router-path appends are metered like tenant
traffic — a small self-feedback term), and both are invisible to
customers but count toward tracked-stream cardinality.

---

## 5. Summary of gaps

**Billing-blocking:**
- B1. Read metering holes: split-stream lineage reads, scans, consumer
  pulls (and `queue_ops` never counted).
- B2. No durable per-stream stored-bytes gauge → "data stored" not
  derivable, restart-fragile ingest totals.
- B3. Emitter is not a system of record: in-memory checkpoints
  (restart = full cumulative re-bill), overflow unattributed, disabled
  unless keyed.
- B4. No per-stream/monthly rollup → no cheap dashboard lookup; rows
  keyed by hash, not name.
- B5. Two overlapping metering systems with different keys, cadences,
  and loss postures.

**Operations:**
- O1. No durable event journal (rebalance/desired/fence/wake history).
- O2. No long-term/aggregated time-series; unbounded interval streams.
- O3. Instance-local, restart-reset counters for fleet-level questions.
- O4. No alerting signal path.

---

## 6. Proposed design (FOR REVIEW — not implemented)

The shape follows what the codebase already believes in: streams as the
durable transport, the bucket as the coordination plane, bounded memory,
and explicit cost postures per object-store operation.

### 6.1 One meter (fixes B1, B5)

Collapse §1.1/§1.3 into the `usage.rs` counters as the single source
(they already sit at the admission point, carry generations, and have
the bounded-cardinality discipline). Add the missing increments, each a
one-line site at an existing choke point:

- `read_v3_lineage_inner` page + head responses → `bytes_out` (+ read
  count), including relayed pages **at the owner only** (already true).
- `product_scan` page bodies → `bytes_out`.
- consumer pull delivered payload bytes → `bytes_out`; pull/settle ops →
  a new `queue_ops` counter (replacing the dead metrics.rs field).
- Retire `metrics.rs` + `__metrics__` (or keep the flusher shell but
  feed it from the same counters — one truth either way). `_billing`
  rows gain `"name"` alongside the hash.

Cost: zero new allocations on hot paths (the Arcs are already in hand
at every listed site).

### 6.2 Durable per-stream storage gauge (fixes B2)

Persist `stored_bytes` (frame bytes) and `plaintext_bytes` into the
stream's **durable meta row** (the committer already rewrites tail meta
on every commit — extend that row; zero extra store operations), and
subtract on trim-below-retention/segment cleanup if/when record-level
retention arrives. Whole-stream deletion/expiry already removes the
stream, ending accrual. On restart/ownership change the gauge recovers
with the tail meta (the same recovery path offsets use), making
storage billing restart-proof and owner-move-proof.

"GB-month" then = the usage pipeline (§6.3) sampling each active
stream's gauge per interval and the rollup integrating
`byte_seconds += stored_bytes × interval`; idle streams (no interval
row) are carried forward by the rollup at their last gauge until a
deletion event closes them.

### 6.3 Usage pipeline with monthly rollups (fixes B3, B4)

Keep the emitter cadence, make the pipeline a system of record:

1. **Emit** (per instance, per interval, as today but from the unified
   meter): interval rows to `_billing`, now including `stored_bytes`
   gauge samples and the stream name. Emitter checkpoint fix: persist
   the per-stream cumulative checkpoint INSIDE the emitted record
   stream itself (each row carries `cum_*` next to `Δ`), so a restarted
   emitter reads its own stream tail (one read at boot) and resumes
   without re-billing — the outbox pattern with the stream as the
   outbox. Overflow aggregate: emit it as one explicit
   `"stream": "_overflow"` row so revenue leakage is at least measured.
2. **Roll up** (one elected instance — e.g. the ring owner of
   `_billing`'s shard — or an external Composer job; either works, the
   reader is decoupled): consume `_billing`, maintain
   `usage/rollup/{YYYY-MM}/{name-hash}.json` objects in the bucket:
   `{name, month, ingest_bytes, ingest_records, read_bytes, requests,
   byte_seconds, updated_ms, cursor}`. Idempotent by cursor; one PUT
   per active stream per rollup tick (suggested 5–15 min — PUT cost
   scales with *active* streams per tick, not total).
3. **Serve**: customer dashboard lookup = **one GET** of
   `usage/rollup/{month}/{hash}.json` (plus one for the prior month at
   boundaries). Month close = the object frozen after the last interval
   lands. Invoicing reads the same objects.

This satisfies "easily derived from metrics we store" (the interval
stream is the ledger, replayable end-to-end) and "cheap-ish lookup"
(O(1) GETs, no scan).

### 6.4 Ops event journal (fixes O1, half of O4)

One internal `_events` stream per cell; typed single-record appends at
the points where transitions COMMIT (all already CAS-guarded, so emit
on CAS success): `split{stream, seg, at, ranges}` (scaler driver),
`merge{…}`, `rebalance_move{shard, from, to, lag}` /
`rebalance_return{shard, home}` (rebalancer), `desired{from, to, epoch,
reason}` (fleet publisher), `instance_dark`/`instance_live` (fleet
loop edge detection), `fence{shard, by}` (engine close on yield),
`stream_expired{name}`. Best-effort with local retry (an event journal
must never block the transition it records); the durable state remains
the source of truth, the journal is the timeline. The operator page
gains a "recent events" panel reading the journal tail — which also
answers "when did splits/rebalances occur" directly.

### 6.5 Long-term series + alerting (O2, O3, O4)

Smallest useful step, consistent with the no-external-dependencies
posture: the same rollup job downsamples heartbeats + shed/lag counters
into `ops/rollup/{YYYY-MM-DD}.json` (per-instance per-5-min vectors),
and evaluates the OPS-RELEASE.md SLO thresholds while it's there,
appending `alert{…}` events to `_events` on breach. Retention: rollups
supersede raw intervals; a janitor trims `_billing`/`_events` below a
retention horizon (90 d suggested) once the covering rollups are
frozen. External Prometheus/OTel export stays optional later — the
JSON surfaces are already scrape-friendly.

### 6.6 Suggested build order

1. §6.1 metering holes (small, pure-win, fixes active under-billing).
2. §6.2 durable gauge (touches committer meta — needs a suite pass +
   DST eyes on the meta row shape).
3. §6.3 pipeline/rollup (new code, no hot-path risk).
4. §6.4 journal (small emit sites at existing CAS successes).
5. §6.5 downsampling/alerts (pure reader).

## 7. Open questions before implementation

1. **Tenant boundary.** Billing per *stream* is assumed here (matches
   "charge for each stream monthly"). If invoices aggregate per
   project/customer, the rollup keys need that dimension — cheapest if
   it's derivable from the stream name prefix convention.
2. **Price dimensions.** Ingest is metered pre-compression
   (`bytes_in`/`plaintext`) and storage post-compression
   (`frame_bytes`) today — bill plaintext or stored bytes? (Both are
   kept; the invoice formula just needs to pick.)
3. **Reads that return nothing** (long-poll probes, `offset=now`
   tails): billable requests, billable bytes only, or free? Run-1
   metered them as 0-byte reads deliberately.
4. **Fan-out relays**: owner-side metering means a cross-owner read
   bills identically to a local one (no double count). Confirm that's
   the intended economics.
5. **`__metrics__` consumers**: does anything downstream read it today
   (Composer jobs, dashboards)? Retirement (§6.1) assumes no.
6. **Retention horizon** for raw interval rows once rollups exist.
