# Soak 7 — durable-tail ring in the field (5 regions, SIN skipped)

**2026-07-28, ~01:08–01:52 UTC.** Run `soak7-7c3f36009b35`. Identical to
soak6 — same harness, tiers, batch, adaptive gather 6 ms,
`STREAMS_DEBUG_TIMING=1`, `RESOLV_OVERRIDE` on, fresh projects, campaign
RUN_ID guard — with **one change: `TAIL_RING_BYTES=33554432` (32 MiB) on
the servers.** Server binary is soak6's code plus split ring-miss
telemetry (`miss_below_floor` / `miss_above_ceil` / `miss_empty`, added
so a non-collapse would be self-diagnosing); generator binary is
byte-identical to soak6's. Zero records lost in all five regions; eight
408s in us-west-1 were the only hard errors (all committed — see §5).

## 1. The prediction, settled

Soak6 discovered that a woken long-poll read costs one object-store RTT
(an L0 SST fetch from Tigris on a cold block cache) and predicted that
serving woken reads from the ring collapses the wake→records-built stage
to ~0 and roundtrip p50 by 20–30 ms. Both halves held — the second one
understated:

| region | wake→read ms (s6→s7) | rt−append gap ms (s6→s7, tier medians) | rt p50 (s6→s7) |
|---|---|---|---|
| ap-northeast-1 | 22.0 → **2.7** | 34 → **1** | 89 → **74** |
| eu-central-1 | 22.4 → **2.8** | 30 → **2** | 91 → **82** |
| eu-west-3 | 30.0 → **2.7** | 40 → **2** | 122 → **96** |
| us-west-1 | 24.1 → **2.7** | 28 → **3** | 137 → **101** |
| us-east-1 | 55.6 → **2.2** | 88 → **1** | 385 → **328** |

The remaining 2–3 ms is decode/build/dispatch — the object store is out
of the live read path. Roundtrip improved at **every tier of every
region**; per-tier gaps of 28–112 ms fell to 0–10 ms.

The prediction said "roundtrip ≈ append + arm-wait." Reality is better:
**roundtrip = append + ~2 ms.** With the read stage at 2 ms the consumer
re-arms before the *next* commit lands, so the per-poll arm→wake grew to
a full WAL interval (39–54 ms healthy, 182 ms iad) while the per-record
cost of it vanished — the poll now waits parked, records are delivered
at dispatch. Soak6's "arm→wake is irreducible for long-poll" stands, but
it taxes the poll, not the record.

## 2. Ring behavior

| region | published | hits | misses | below_floor | above_ceil | empty | evicted | resident/peak MB |
|---|---|---|---|---|---|---|---|---|
| ap-northeast-1 | 171,176 | 32,048 | 14 | 14 | 0 | 0 | 147,751 | 32.0 / 32.1 |
| eu-central-1 | 180,651 | 32,214 | 12 | 12 | 0 | 0 | 150,602 | 32.0 / 32.1 |
| eu-west-3 | 163,683 | 27,866 | 14 | 14 | 0 | 0 | 141,595 | 32.0 / 32.1 |
| us-west-1 | 154,226 | 27,657 | 12 | 12 | 0 | 0 | 133,187 | 32.0 / 32.1 |
| us-east-1 | 35,199 | 8,492 | 0 | 0 | 0 | 0 | 27,348 | 32.0 / 32.1 |

- **Hit rate ≥ 99.9 % everywhere** — 52 misses across ~128k woken reads,
  all `below_floor` (consumer momentarily behind eviction), **zero**
  `above_ceil` (no mid-dispatch races: publish-before-NOTIFY holds in
  the field), zero `empty`.
- Resident pinned at exactly the 32 MiB budget once warm; peak 32.1 MiB
  (the transient publish-then-evict overshoot, as coded). Eviction
  churned 27k–150k batches without a wobble.
- Live-path L0 fetches collapsed: `get:sst` during the run dropped
  115k→25k (nrt), 112k→24k (fra), 101k→21k (cdg), 77k→23k (sjc),
  35k→16k (iad) — the residue is compactor + history reads.

## 3. What the ring did not change

- `put:wal` p50 was flat-to-better in every region (37→37, 43→41,
  52→49, 63→54, 213→186 ms) — appends never touched the ring.
- Pump/gather posture held: flushes and ack→next-enqueue p50 (3.0–5.9 ms)
  match soak6; busy-skips still ~10–20 % of gather decisions.
- End-of-run WAL stage medians (queue_wait / encode / write /
  durable_wait) all equal or better than soak6.
- Ceilings: nrt 494→490, fra 490→490, cdg 486→492, sjc 402→491 (SJC
  variance swung favorable this run), **iad 132→156 (+18 %)** — the
  faster consumer loop raises closed-loop throughput where latency was
  the binding constraint.

## 4. Costs, honestly

- **RSS shed clips.** `admit_shed`: cdg 4,027, nrt 1,527, fra 482 (iad,
  sjc zero) — transient crossings of the 600 MB shed line at top tiers,
  ~0.1–0.3 % of requests, all retried clean (errs = 0 in those regions).
  Post-run RSS settled at 265–283 MB. The ring's 32 MiB per shard is the
  plausible marginal contributor; soak6 didn't scrape `/v1/debug/load`
  so there is no shed baseline to compare against. Watch this counter on
  any instance sized tighter than these.
- **Mid-tier append medians rose** +8–21 ms at t04–t07 in nrt/fra/iad
  while low tiers stayed flat and top tiers *improved* (nrt t10 97→85,
  iad t10 477→380). The signature matches the gather drift zone, not a
  server regression: `gathered_reqs` fell ~30 % (50.7k→36.6k nrt)
  because consumers re-arm 20–50 ms earlier and the closed-loop herd's
  phase against the 6 ms window shifted. Roundtrip still improved at
  those same tiers, which is the product-visible number. Worth a look if
  a future run shows it growing.

## 5. us-west-1: a stall dissected (and an accounting bug found)

SJC's documented sustained-write variance produced two stall windows in
t04-conc8. The layered defenses all fired, each visible in its own
counter:

1. **Wedge shed**: 10,903 new arrivals 429'd pre-enqueue while
   `wedge_ms > 5 s` (the reopen-storm-era detector, doing its job).
2. **Staged-but-unflushed appends** failed retryable
   (`AppendErr::Moved` → 503) when the pipeline recycled — ~10.9k
   requests, client-retried, never durable.
3. **8 requests** caught `APPEND_TIMEOUT` → 408 "outcome unknown." All
   eight committed: the consumer decoded exactly acked + 80 records.
   That is the ambiguity surface working as specified — timeouts, not
   store faults, are the ambiguous case.

Client experience: 429/503 spin inside t04 (~41k throttles), zero loss,
full recovery by t05, then a 491 rps ceiling — *better* than soak6's
402 on the same region.

**The accounting bug:** the harvest's client-vs-server integrity check
showed server = client + 109,750 records in sjc. The stream itself is
clean — the tail consumer (`bodyFailures=0`) decoded exactly
client-acked + 80. The inflation is `usage` counters incrementing at
**staging** (write-batch put) rather than at durability, so the ~10.9k
staged-then-failed requests of mechanism 2 were counted as if committed.
Under any shed storm the usage-based check will overstate server-side
records; the consumer-decode count is the trustworthy integrity signal.
Fix candidates: count usage at watermark release, or export a separate
`staged_failed` counter so the check can subtract it.

Baseline note: all regions show a small +26–64-request offset
(client-sampled totals lag the final trickle), identical to soak6;
sjc's excess above that matched mechanism 2's count.

## 6. Verdict

The ring's cloud case is proven in the field: **32 MiB of ring turns the
woken read from an object-store RTT into a 2 ms memory read, at ≥99.9 %
hit rate, with zero correctness anomalies across ~18.9 M records.** The
reviewer's "don't enable for latency yet" is answered with evidence —
`TAIL_RING_BYTES=32MiB` belongs next to the gather flag in the staging
env (per-shard budget: size it as budget × shards for RSS headroom;
these instances ran 1 active shard). Suggested rollback triggers, same
spirit as the gather's: ring hit rate < 95 %, `admit_shed` > 1 % of
requests, or rt−append gap > 10 ms sustained.

## 7. Bookkeeping

- Campaign RUN_ID guard exercised end to end again: deploys stamped
  `soak7-7c3f36009b35`, teardown verified against the stamp, probe fleet
  and non-campaign projects untouchable by construction.
- Split-miss telemetry (`miss_below_floor` / `miss_above_ceil` /
  `miss_empty`) ships in this commit; the run needed it for exactly one
  sentence (§2), which is what instrumentation is for.
- Artifact bucket lived inside the us-east-1 campaign project this run,
  so teardown reclaims it with the project.
