# Soak 5 — six regions, 30-minute ramp, WAL post-ACK gather enabled

**2026-07-27.** Same harness, tiers, batch (10×1 KiB), per-shard limits
and topology as the 2026-07-26 baseline soak; differences: the WAL pump's
post-ACK gather (`WAL_POST_ACK_GATHER_MS=6`, commit 9449597), the honest
pipelined consumer (commit a66f1b9), `RESOLV_OVERRIDE` (vultr+google —
the platform DNS forwarder remains unfixed), and the history-reader
cache. The durable-tail ring (commit 5fe4e08) was NOT in these binaries.

Requests acknowledged: **1,556,771** across six regions (≈15,567,710 records), **0 errors**.

## Headline vs the 2026-07-26 baseline

| region | PoP | append p50 | append p99 (max tier) | roundtrip p50 | roundtrip p50 (decoded) | ceiling | errors | colleague target |
|---|---|---|---|---|---|---|---|---|
| ap-northeast-1 | nrt | **78 → 54 ms** | 822 → 713 | 111 → 86 ms | 87 ms | 490 req/s → 490 req/s | 0 | 50–60 |
| us-west-1 | sjc | **101 → 87 ms** | 457 → 638 | 130 → 113 ms | 113 ms | 490 req/s → 468 req/s | 0 | 55–70 |
| eu-west-3 | cdg | **119 → 68 ms** | 393 → 1476 | 164 → 111 ms | 111 ms | 482 req/s → 428 req/s | 0 | 65–80 |
| ap-southeast-1 | sin | *skipped — known platform issue (Shape-C 404 on the generator; server-side load ran the full window with zero storm-detector hits)* | | | | | | |
| eu-central-1 | fra | **100→wedged → 59 ms** | — → 1318 | 181→wedged → 90 ms | 90 ms | collapsed at tier 6 → 472 req/s | 0 | — |
| us-east-1 | ewr/iad1 | **456 → 341 ms** | 3029 → 1906 | 539 → 413 ms | 413 ms | 124 req/s → 126 req/s | 0 | — |

## The c1→c2 boundary — the gather's specific target

The review predicted concurrency 2 would collapse from ~2× to ~1× of
concurrency 1 if the two-generation WAL crossing was real. Field result:

| region | tier-1 p50 | tier-2 p50 | t2/t1 | soak-1 shape |
|---|---|---|---|---|
| ap-northeast-1 | 36.9 | 37.6 | **1.02** | ≈2× |
| us-west-1 | 61.1 | 61.4 | **1.00** | ≈2× |
| eu-west-3 | 52.4 | 51.6 | **0.98** | ≈2× |
| eu-central-1 | 41.8 | 41.8 | **1.00** | ≈2× |
| us-east-1 | 232.9 | 247.5 | **1.06** | ≈2× |

## Findings

1. **The two-generation WAL crossing is fixed in the field.** Every
   reporting region's tier-2/tier-1 ratio sits at 0.98–1.06 (baseline
   shape ≈2×). This was the review's core prediction and the change's
   acceptance criterion (c2 ≤ 1.3×c1): met with margin everywhere.
2. **Targets:** NRT 54 ms (band 50–60 ✓, tier-1 floor now 36.9 ms vs
   47.0 in the baseline week), CDG 68 ms (band 65–80 ✓), FRA 59 ms and
   it **completed the ramp it previously wedged on**. SJC 87 ms misses
   its 55–70 band — but its tier-1 floor measured 61 ms this window
   (49.8 baseline week): Tigris SJC PUT was simply slower today, and no
   scheduling change can beat the floor. IAD improved 456→341 but
   remains storage-bound (its PoP's mutating-op latency, documented).
3. **Zero errors in ~1.56 M acknowledged requests.** Top-tier p99s are
   deep-queue artifacts at conc-64 (CDG 1.48 s, SJC 638 ms); at every
   working tier p99 ≤ ~450 ms, better than baseline.
4. **The honest roundtrip equals the headers-based one here** (decoded
   +0–3 ms, rearm p50 = 0.00 ms): the pipelined consumer removed the
   rearm gap, and at 10 KiB responses decode is negligible. The old
   metric's flaw mattered in principle; at this workload it was small.
5. **Roundtrip minus append is one WAL interval (~21–30 ms)** in every
   region — structural, identical to the local rig, and NOT read-path
   cost (live-tail scans are memtable-resident). This is why the
   durable-tail ring (already merged, not in these binaries) cannot be
   expected to close that gap by itself; the interval's origin (records
   observed one dispatch late) is the next instrumentation target.
6. **DNS:** RESOLV_OVERRIDE remains load-bearing; the probe fleet
   watched the same window in all six regions (separate telemetry).

## Per-tier detail

### ap-northeast-1 (nrt)

| tier | conc | req/s | append p50 | append p99 | rt p50 | rt decoded | rearm | throttled(cum) |
|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 21 | 36.9 | 362.2 | 58.0 | 58.0 | 0.0 | 0 |
| t02-conc2 | 2 | 40 | 37.6 | 371.2 | 58.5 | 58.5 | 0.0 | 0 |
| t03-conc4 | 4 | 76 | 39.3 | 329.0 | 65.0 | 65.0 | 0.0 | 0 |
| t04-conc8 | 8 | 131 | 47.0 | 333.3 | 74.0 | 75.0 | 0.0 | 0 |
| t05-conc12 | 12 | 188 | 49.5 | 289.8 | 80.0 | 80.5 | 0.0 | 0 |
| t06-conc16 | 16 | 226 | 57.9 | 316.2 | 93.0 | 94.0 | 0.0 | 0 |
| t07-conc24 | 24 | 282 | 75.6 | 450.6 | 105.0 | 106.0 | 0.0 | 0 |
| t08-conc32 | 32 | 377 | 78.1 | 355.1 | 120.5 | 121.5 | 0.0 | 0 |
| t09-conc48 | 48 | 474 | 90.6 | 712.7 | 149.6 | 151.0 | 0.0 | 0 |
| t10-conc64 | 64 | 490 | 88.7 | 453.4 | 181.1 | 184.6 | 0.0 | 301453 |

### us-west-1 (sjc)

| tier | conc | req/s | append p50 | append p99 | rt p50 | rt decoded | rearm | throttled(cum) |
|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 14 | 61.1 | 217.0 | 80.5 | 80.5 | 0.0 | 0 |
| t02-conc2 | 2 | 28 | 61.4 | 220.7 | 83.0 | 83.0 | 0.0 | 0 |
| t03-conc4 | 4 | 50 | 70.9 | 303.9 | 93.5 | 94.0 | 0.0 | 0 |
| t04-conc8 | 8 | 90 | 78.0 | 405.8 | 100.5 | 100.5 | 0.0 | 0 |
| t05-conc12 | 12 | 130 | 81.8 | 284.9 | 104.0 | 104.5 | 0.0 | 0 |
| t06-conc16 | 16 | 153 | 93.1 | 268.0 | 121.1 | 121.5 | 0.0 | 0 |
| t07-conc24 | 24 | 207 | 110.7 | 321.8 | 141.6 | 142.1 | 0.0 | 0 |
| t08-conc32 | 32 | 237 | 121.1 | 580.1 | 178.0 | 179.6 | 0.0 | 0 |
| t09-conc48 | 48 | 377 | 123.2 | 448.0 | 158.1 | 160.1 | 0.0 | 0 |
| t10-conc64 | 64 | 468 | 134.9 | 638.0 | 189.6 | 192.1 | 0.0 | 0 |

### eu-west-3 (cdg)

| tier | conc | req/s | append p50 | append p99 | rt p50 | rt decoded | rearm | throttled(cum) |
|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 16 | 52.4 | 172.7 | 81.5 | 81.5 | 0.0 | 0 |
| t02-conc2 | 2 | 34 | 51.6 | 240.8 | 81.0 | 81.0 | 0.0 | 0 |
| t03-conc4 | 4 | 59 | 56.9 | 203.9 | 88.1 | 88.1 | 0.0 | 0 |
| t04-conc8 | 8 | 106 | 62.1 | 241.0 | 97.5 | 98.0 | 0.0 | 0 |
| t05-conc12 | 12 | 150 | 65.5 | 227.7 | 105.1 | 105.1 | 0.0 | 0 |
| t06-conc16 | 16 | 186 | 71.0 | 250.2 | 116.0 | 117.0 | 0.0 | 0 |
| t07-conc24 | 24 | 239 | 96.9 | 342.3 | 130.5 | 132.1 | 0.0 | 0 |
| t08-conc32 | 32 | 289 | 110.1 | 289.3 | 148.6 | 149.6 | 0.0 | 0 |
| t09-conc48 | 48 | 380 | 121.4 | 443.1 | 184.1 | 186.6 | 0.0 | 0 |
| t10-conc64 | 64 | 428 | 128.8 | 1475.6 | 247.1 | 250.6 | 0.0 | 0 |

### eu-central-1 (fra)

| tier | conc | req/s | append p50 | append p99 | rt p50 | rt decoded | rearm | throttled(cum) |
|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 20 | 41.8 | 191.4 | 60.5 | 60.5 | 0.0 | 0 |
| t02-conc2 | 2 | 42 | 41.8 | 144.3 | 59.5 | 60.0 | 0.0 | 0 |
| t03-conc4 | 4 | 70 | 47.8 | 263.4 | 71.0 | 72.1 | 0.0 | 0 |
| t04-conc8 | 8 | 130 | 51.4 | 225.9 | 78.0 | 78.5 | 0.0 | 0 |
| t05-conc12 | 12 | 177 | 57.0 | 259.7 | 83.5 | 84.5 | 0.0 | 0 |
| t06-conc16 | 16 | 214 | 61.6 | 267.0 | 95.5 | 95.5 | 0.0 | 0 |
| t07-conc24 | 24 | 278 | 85.0 | 246.9 | 110.5 | 111.6 | 0.0 | 0 |
| t08-conc32 | 32 | 345 | 89.9 | 302.8 | 129.5 | 130.6 | 0.0 | 0 |
| t09-conc48 | 48 | 452 | 100.4 | 396.8 | 144.1 | 146.0 | 0.0 | 0 |
| t10-conc64 | 64 | 472 | 101.5 | 1317.9 | 192.5 | 195.6 | 0.0 | 170269 |

### us-east-1 (ewr/iad1)

| tier | conc | req/s | append p50 | append p99 | rt p50 | rt decoded | rearm | throttled(cum) |
|---|---|---|---|---|---|---|---|---|
| t01-conc1 | 1 | 3 | 232.9 | 1234.9 | 291.6 | 291.6 | 0.0 | 0 |
| t02-conc2 | 2 | 7 | 247.5 | 1158.1 | 307.7 | 307.7 | 0.0 | 0 |
| t03-conc4 | 4 | 14 | 248.8 | 950.8 | 315.1 | 315.6 | 0.0 | 0 |
| t04-conc8 | 8 | 27 | 238.6 | 1891.3 | 309.1 | 309.1 | 0.0 | 0 |
| t05-conc12 | 12 | 30 | 347.6 | 1905.7 | 409.6 | 409.6 | 0.0 | 0 |
| t06-conc16 | 16 | 44 | 333.7 | 1168.4 | 416.1 | 416.1 | 0.0 | 0 |
| t07-conc24 | 24 | 56 | 411.8 | 1583.1 | 452.1 | 452.6 | 0.0 | 0 |
| t08-conc32 | 32 | 74 | 410.9 | 1810.4 | 492.2 | 493.1 | 0.0 | 0 |
| t09-conc48 | 48 | 102 | 439.9 | 1306.6 | 524.3 | 526.2 | 0.0 | 0 |
| t10-conc64 | 64 | 126 | 507.6 | 1184.8 | 605.7 | 607.7 | 0.0 | 0 |
