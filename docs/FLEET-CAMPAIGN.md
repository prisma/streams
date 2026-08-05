# Fleet campaign: four instances coming live as traffic scales (#113, Compute leg)

**Date:** 2026-08-05 · **Server binary:** freeze4 = v0.2.0-preview.4 (c26c2bd4) · **Router/generator binary:** pilot `fleet1`
**Project:** streams-fleet-fra (`proj_rq8fxqt8j2s34eerh418o9hg`, eu-central-1 — created region-set per BUCKETS-SINGLE-REGION.md) · **Bucket:** streams-fleet-data (co-located single-region Tigris)

The ask: a 4-instance Prisma Compute fleet behind a load balancer, demonstrated coming
live as generated traffic scales. **Demonstrated.** desired stepped 1→2→3→4 under a
doubling closed-loop ramp; each newly-desired instance was serving its rebalanced shards
within one 11-second observer window of the step; append accounting reconciles to zero
loss across all ownership transitions.

## Topology

| Service | What it runs |
|---|---|
| fleet-s1..s4 | streams server, `INSTANCE_NAME=streams-1..4`, `FLEET_PREFIX=fleetops`, `FLEET_MAX=4`, `INITIAL_SHARDS=4`, `SCALE_OUT_CPU_PCT=30` (util target 0.30), `SCALE_CPU_SUSTAIN_SECS=10`, scale-in 5%/900s, soak WAL posture; `KEEP_AWAKE` only on s1 |
| fleet-lb | pilot `MODE=lb`: rendezvous-hash router over the active ordinal set (identical computation to the servers' ownership check), follows `409 Streams-Replay-To` with one internal re-proxy, pings desired-but-stale ordinals awake via `/health`; routes both the raw (`/v1/stream/…`) and product (`/v1/streams…`) surfaces |
| fleet-gen | pilot `MODE=gen`: closed loop against the LB only. 32 streams `fleet1-0..31`, stream = attempt `n % 32`, concurrency 4→512 doubling every 120 s, 200 B pad, `BATCH=1`, `READ_EVERY=10` (every `n≡9 (mod 10)` attempt is a `?offset=now` read, not an append) |

All four servers share one bucket (`fleetd` data prefix, `fleetops` fleet prefix). Fleet
coordination is entirely bucket-mediated: 2 s heartbeats, CAS'd `fleet/desired.json`,
CAS'd `fleet/overrides.json` (rebalancer moves).

## Timeline (observer: 10 s polls of heartbeats + desired.json)

Verbatim transition rows (`t_secs,desired,live,instances,per_instance_rps,per_instance_cpu,owned_shards`):

```
0,   1,4, …, 0|0|0|0,        0|0|0|0,     streams-1:1|streams-2:0|streams-3:0|streams-4:0
91,  1,4, …, 0|0|0|0,        0|0|0|0,     streams-1:4|streams-2:0|streams-3:0|streams-4:0
556, 1,4, …, 471|0|0|0,      11|0|0|0,    streams-1:4|streams-2:0|streams-3:0|streams-4:0
567, 2,4, …, 311|375|0|0,    11|9|0|0,    streams-1:2|streams-2:2|streams-3:0|streams-4:0
601, 3,4, …, 319|480|0|0,    10|11|0|0,   streams-1:2|streams-2:2|streams-3:0|streams-4:0
613, 3,4, …, (s3 serving),   …,           streams-1:2|streams-2:1|streams-3:1|streams-4:0
715, 4,4, …, 343|436|40|483, 10|10|2|16,  streams-1:1|streams-2:1|streams-3:0|streams-4:2
976, 4,4, …, 508|704|164|616,12|16|8|17,  streams-1:1|streams-2:1|streams-3:1|streams-4:1
1249,4,4, …, 491|676|242|556,13|16|7|15,  streams-1:1|streams-2:1|streams-3:1|streams-4:1
```

- **Baseline:** desired=1, all four shards on streams-1; s2–s4 idle (scale-to-min posture).
- **t+567 (gen conc 64): desired 1→2.** The same 11 s window that first shows desired=2
  already shows 2+2 shard ownership and streams-2 serving 375 rps — wake + rebalance +
  serving inside one observer cadence.
- **t+601 (conc 128): desired 2→3;** streams-3 owned a shard and served by t+613.
- **t+715 (conc 128): desired 3→4.** The publishing instance's own reason string
  (recovered from `fleet/desired.json`, epoch 4):
  `cores_used=0.34 util->2 inflight=140 slots->4 hot_cpu=13% (0) ack_p50=90ms (0) edge_p50=87ms (0) rps=1519 (0) live=4`
  — i.e. the **in-flight/admission-slots signal** demanded 4 while the CPU-utilization
  signal alone wanted 2. The scaler is max-of-signals; CPU never exceeded ~20% at any
  observer instant, and that is the intended behavior, not a misfire.
- **Steady 4-wide:** ~1,965 rps aggregate across four instances (row t+1249), generator
  at conc 512 achieving ~1,978 attempts/s, win p50 254 ms / p99 800 ms (closed-loop at
  saturation through a WAN LB hop), cumulative p50 178 ms.

## Two coordination layers, disagreeing safely

The desired=4 rendezvous redraw gave streams-4 two shards and streams-3 none
(`1|1|0|2` at t+715): four shards over four instances is a coarse draw. The
**rebalancer** then moved one of streams-4's shards to streams-3 via `overrides.json`
(observed `1|1|1|1` at t+976, streams-3 serving 164→247 rps). The **LB does not read
overrides** — it keeps first-picking the pure-rendezvous owner, which answers
`409 Streams-Replay-To: streams-3`, and the LB re-proxies. Evidence: streams-3's LB
first-pick counter stayed frozen at 28,833 requests while its heartbeat showed ~240 rps
— its entire steady-state load arrived via replay. LB upstream error counters: 0/0/0/0.

**Flap finding (pilot-era):** the override is not stable. Return-home
(`REBALANCE_RETURN_SECS=300`) drops an override once the rendezvous home is healthy,
the coarse-granularity imbalance immediately re-appears, and the rebalancer re-moves:
ownership oscillated `1|1|0|2 → 1|1|1|1` at t+976, back at t+1305, re-applied at
t+1442, dropped again post-traffic (final `overrides.json` is empty). With
`INITIAL_SHARDS=4` over 4 instances the imbalance is *permanent*, so override vs
return-home is a tug-of-war that never converges.
**Recommendations:** (1) `INITIAL_SHARDS ≥ 4 × FLEET_MAX` so the rendezvous draw is
statistically even and no standing override is needed; (2) make return-home load-aware
(suppress while the home's shard count exceeds the fleet mean).

Client impact of all transitions combined: **299 client-visible 409s out of ~1.83 M
attempts (0.016%)** — windows where both the first pick and the single replay hop missed
(e.g. mid-move). These are retryable (`not_owner` family; the SDK retries them);
`throttled` (429/503 honoring Retry-After) totaled 49.

## Zero-loss accounting

Controlled stop: final generator snapshot 13:04:02Z — `ok=1,771,502 errs=299
throttled=49 conc=512` — then the gen service was destroyed (version deleted 13:04:43Z).
After quiesce, every stream tail was read through the LB (`Stream-Next-Offset`, 26-char
Crockford codec, rawSeq = record count), twice, minutes apart: **identical both passes**.

- **Σ tails = 1,649,098 records** across fleet1-0..31.
- Tails are exactly bimodal — even streams 57,271, odd streams 45,817, ratio 5:4.0000.
  This is the generator's read mix, not an anomaly: reads occur at `n≡9 (mod 10)`,
  which is always odd, so odd streams spend 1 in 5 of their attempt slots on reads.
- **Attempt-level reconciliation:** max `i` observed in final records = 1,832,641 →
  ~1,832,642 attempts issued; expected records = attempts × 9/10 = 1,649,378; observed
  1,649,098; deficit **280**, against 348 recorded non-success attempts (errs +
  throttled) plus ≤512 in-flight at process death. Nothing unaccounted.
- **Acked-record bound:** acked appends at the last readable instant ≈ ok − reads ≈
  1,594,317; Σ tails exceeds it by 54,781 (post-snapshot traffic). Every acknowledged
  append is readable.
- **Integrity sample (fleet1-0 + fleet1-1, 103,088 records paged end-to-end):** paged
  counts equal decoded tails exactly; **0 duplicates; 0 wrong-residue records** (every
  record satisfies `i ≡ stream (mod 32)` — no cross-stream leakage); `i` interleaves
  non-monotonically as expected from 512 concurrent workers committing out of order.

A caution for future campaigns: pilot-gen `ok` counts successful *reads and* appends.
Expected records = attempts × (1 − 1/READ_EVERY_share) — the naive `Σtails ≥ ok` check
reads as a 122 k "loss" that is entirely the read mix. Reconcile via max-`i` as above.

## Also validated through the LB

- Product surface round trip and catalog through the router (both surfaces routed).
- Consumer versioned-DELETE saga smoke (`scripts/consumer-saga-smoke.sh`) — PASS via
  the LB earlier in this campaign.

## What this campaign does and does not establish

**Validated:** fleet formation on a shared single-region bucket; desired.json scaling
under a real ramp (multi-signal scaler, CAS'd publishes); ordinal wake of sleeping
instances; rendezvous routing with replay correction across two disagreeing
coordination layers; multi-instance zero-loss through repeated ownership transitions;
both API surfaces behind one router.

**Not validated (unchanged from the v0.2.0-preview.4 launch posture):** cross-owner
segment fan-out — no stream splits were provoked on this fleet (split knobs deliberately
left at defaults), so reads spanning segments owned by different instances remain the
open field item.

## Ops

- fleet-gen destroyed 13:04:43Z. fleet-s1..4 + fleet-lb left running (idle instances
  sleep; KEEP_AWAKE only s1 + lb) for possible follow-up (cross-owner fan-out would
  reuse this fleet). Tear down with
  `compute-cli services destroy <svc> --project proj_rq8fxqt8j2s34eerh418o9hg`
  (needs `PRISMA_API_TOKEN` from `$SOAK_HOME/platform-token.txt`; ids in
  `$SOAK_HOME/svc-fleet-*.txt`).
- Retained fra + ewr field services (both freeze4) expire 2026-08-19.
