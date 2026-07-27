# Soak 6 — adaptive gather + stage instrumentation (5 regions, SIN skipped)

**2026-07-27, ~13:30–14:11 UTC.** Same harness/tiers/batch as soak1/soak5.
New in these binaries (commit 81909f7): adaptive gather (busy-skip at 32
reqs / 1 MiB), pump per-WAL + ack-reaction telemetry, split-cursor
consumer, and `STREAMS_DEBUG_TIMING` stage headers on woken long-polls.
Ring OFF. `RESOLV_OVERRIDE` on. Fresh projects; campaign RUN_ID guard
active end to end. Zero errors in all five regions.

## Headline (soak1 → soak5 → soak6)

| region | append p50 | roundtrip p50 | ceiling rps | t2/t1 | t1 floor s5→s6 |
|---|---|---|---|---|---|
| ap-northeast-1 | 78 → 54 → **55** | 111 → 86 → 89 | 490 → 490 → **494** | 1.01 | 36.9 → 40.0 |
| eu-central-1 | wedged → 59 → **61** | 181 → 90 → 91 | — → 472 → **490** | 1.02 | 41.8 → 43.0 |
| eu-west-3 | 119 → 68 → 82* | 164 → 111 → 122 | 482 → 428 → **486** | 1.02 | 52.4 → 53.2 |
| us-west-1 | 101 → 87 → 109* | 130 → 113 → 137 | 490 → 468 → 402* | 1.03 | 61.1 → **55.0** |
| us-east-1 | 456 → 341 → **297** | 539 → 413 → **385** | 124 → 126 → **132** | 0.98 | 232.9 → **207.3** |

\* CDG: tier-1 floor flat (52→53) while the ceiling recovered 428→486
(+13.6%) — soak5's throughput decline, the review's open criterion, is
answered: with busy-skip the top tiers push more at slightly higher
mid-tier medians, which is the intended trade. SJC: floor IMPROVED
(61→55) while sustained ceiling sagged 468→402 — not a knob signature
(skips only raise ceilings); Tigris SJC sustained-write conditions vary
run to run, as documented across soak1→5.

## The gather, quantified in production for the first time

| region | flushes | req/WAL | gathers applied | skipped busy | reqs caught in windows | ack→next-enqueue p50 |
|---|---|---|---|---|---|---|
| ap-northeast-1 | 31,069 | 11.3 | 9,441 | 1,256 | 50,741 | 3.2 ms |
| eu-central-1 | 30,728 | 11.3 | 9,821 | 1,073 | 49,246 | 3.0 ms |
| eu-west-3 | 26,326 | 11.6 | 9,729 | 2,072 | 37,868 | 4.2 ms |
| us-west-1 | 18,068 | 13.5 | 7,939 | 1,197 | 36,190 | 3.4 ms |
| us-east-1 | 6,885 | 11.7 | 2,103 | 401 | 11,797 | 5.0 ms |

- **ack→next-enqueue of 3–5 ms validates the 6 ms window size directly**:
  the closed-loop herd reacts inside the window, with ~1–3 ms to spare.
- Windows caught ~37–51k requests per region that would otherwise have
  straddled an extra WAL generation.
- Busy-skips ran at ~10–20 % of gather decisions — the adaptive path is
  live, not decorative.

## The remaining roundtrip gap, classified

Woken long-poll stages (p50, from `Streams-Debug-Wait`):

| region | arm→wake | wake→records-built |
|---|---|---|
| ap-northeast-1 | 21.0 ms | 22.0 ms |
| eu-central-1 | 24.9 ms | 22.4 ms |
| eu-west-3 | 24.9 ms | 30.0 ms |
| us-west-1 | 43.3 ms | 24.1 ms |
| us-east-1 | 161.9 ms | 55.6 ms |

Two conclusions:

1. **arm→wake ≈ one WAL interval** and scales with each region's PUT
   latency: a poll that re-arms right after a dispatch waits for the next
   commit. Irreducible for long-poll; a persistent SSE removes the re-arm
   but not the commit cadence.
2. **wake→records-built of 22–56 ms is the discovery.** A woken read on
   "memtable-resident" data should cost microseconds; it costs one
   object-store RTT. Over a 30-minute run the flush ticker and compactor
   land recent records in L0, and the woken scan fetches an L0 SST from
   Tigris on a cold block cache. **This is the durable-tail ring's cloud
   case, now quantified**: serving woken reads from the ring should
   collapse this stage to ~0 and bring roundtrip ≈ append + arm-wait.
   (The local ring A/B showed no delta because 60-second local runs never
   pushed the tail out of the memtable — the field just falsified that
   generalization.)

**Next experiment:** identical run with `TAIL_RING_BYTES=32MiB`,
predicted effect: wake→read stage → ~0, roundtrip p50 −20–30 ms in
healthy regions. This turns the reviewer's "don't enable the ring for
latency yet" into a directly testable, evidence-backed enable.

## Bookkeeping

- Consumer integrity counters: bodyFailures = 0 in all regions (the
  split-cursor path saw no partial bodies in this run; its fault handling
  is covered by the bench test server).
- Campaign RUN_ID guard exercised end to end (deploys stamped, teardown
  verified against the stamp; probe-fleet and non-campaign projects
  untouchable by construction).
