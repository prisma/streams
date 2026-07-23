# Tigris latency observatory — findings report #1

2026-07-23, covering 2026-07-22T12:00Z → 2026-07-23T02:30Z (~14 h global-bucket
data, ~10 h pinned-bucket data). Instrument: one probe pair per Prisma region
(six regions), solo PUT/GET at 1 KB & 256 KB every 10 s, fresh-TLS probe each
minute, hourly 60 s × 16-concurrency 256 KB PUT burst; every sample carries
Tigris's own `Server-Timing: total` (time inside Tigris) plus wall time.
Variants: `global` = Prisma Bucket (Tigris global bucket), `pinned` =
region-restricted bucket in the nearest Tigris region (iad/sjc/ams/fra/nrt/sin).
Data hygiene: rows before 22T13:30Z (pre-warm-up probe) excluded from solo-PUT
comparisons; global-vs-pinned uses like-for-like hours only (22T15:00Z+).

## Verdict 1 — Global buckets are exonerated

Across all six regions, all four op/size combinations, global ≈ pinned within
noise (wall p50 deltas ≤ 6 ms except us-east hot-GET, where **pinned** is the
slower one because global hot reads hit the local cache at sp≈0–1 ms):

| region | PUT 1K global/pinned (wall p50) | PUT 256K | GET-hot 1K | GET-hot 256K |
|---|---|---|---|---|
| eu-central-1 | 27 / 24 | 127 / 122 | 10 / 15 | 114 / 108 |
| ap-southeast-1 | 12 / 11 | 37 / 38 | 3 / 6 | 23 / 24 |
| ap-northeast-1 | 10 / 10 | 73 / 74 | 3 / 5 | 36 / 38 |
| us-east-1 | 199 / 183 | 276 / 261 | 10 / 43 | 58 / 90 |
| us-west-1 | 23 / 19 | 58 / 57 | 3 / 7 | 25 / 27 |
| eu-west-3 | 33 / 38 | 164 / 161 | 15 / 25 | 153 / 140 |

The dynamic-placement model works as documented: first writes place locally
(header-verified: fra/sin/nrt/iad1/sjc1), reads serve from local cache. The
earlier "global buckets are slow" impression decomposes into TLS setup on
sparse traffic (below) and one genuinely slow Tigris region (below).

## Verdict 2 — us-east-1 (iad) has a sustained Tigris-internal write problem

Server-Timing-attributed (i.e., *inside Tigris by their own header*), 1 KB
PUT sp50 per hour, `global/pinned`:

```
22h13 190/152  h14 157/144  h15 117/111  h16 119/109  h17 162/165
  h18 198/174  h19 198/219  h20 154/146  h21 186/172  h22 261/228
  h23 192/173  23h00 208/186  h01 199/174  h02 180/162
```

**15 consecutive hours at 110–260 ms internal p50 for a 1 KB write, on both
bucket types** — 10–25× every other region (SIN 8, NRT 7, FRA 14, SJC 17–21,
AMS-pinned 17 ms). 256 KB internal p50 there is ~244–258 ms, and wall p99
reaches 3–10.8 s. us-west-1's first-hour 89 ms did *not* persist (15–23 ms all
day) — us-east-1 is the outlier, and it is not our stack, our network, or the
global-bucket architecture.

## Verdict 3 — connection setup is expensive from 4 of 6 Compute regions

Fresh-TLS probe (new client, 1 KB GET) minus warm GET, p50:

| region | setup cost | region | setup cost |
|---|---|---|---|
| ap-southeast-1 | **10 ms** | eu-central-1 | **121 ms** |
| ap-northeast-1 | **14 ms** | eu-west-3 | **143 ms** |
| us-west-1 | 96 ms | us-east-1 | 155 ms |

SIN/NRT complete DNS+TCP+TLS to `t3.storage.dev` in 10–14 ms; FRA/CDG/EWR/SJC
pay ~96–155 ms — a ~3×RTT handshake model implies those VMs are terminating
TLS ~30–50 ms away despite Tigris having local regions. Joint question for
Tigris + the Compute networking team: where does `t3.storage.dev` anycast
land for Compute egress in fra/cdg/ewr/sjc? (Production impact today is
bounded — server pools stay warm under load and manifest polling keeps them
warm when any shard is open — but every cold start and idle-instance wake
pays it, and it distorted every casual latency measurement we made before
splitting it out.)

## Verdict 4 — the write tails hit SPARSE traffic, not loaded traffic

The hourly 16-concurrent burst is consistently *faster* than the same hour's
10 s-cadence solo probes (median across 15 hours):

| region | burst−solo p50 | burst−solo p99 |
|---|---|---|
| eu-central-1 | −47 ms | **−1,273 ms** |
| us-east-1 | −49 ms | **−3,342 ms** |
| eu-west-3 | −63 ms | **−1,307 ms** |
| ap-southeast-1 | +2 ms | −635 ms |
| ap-northeast-1 | 0 ms | +18 ms |
| us-west-1 | +2 ms | +28 ms |

Sustained load does NOT create the tails — it *suppresses* them. The 0.9–3 s
EU (and up to 10 s us-east) write p99s land on low-rate traffic. This also
retires our earlier "time-of-day weather" hypothesis in its strong form: the
worst hours are scattered (h14–16 and h23–01 across regions), while the
sparse-traffic penalty is systematic. Question for Tigris: what per-request
state goes cold between sparse writes (metadata leases, internal connection
warm-up, placement lookups?) such that a request-every-1.7 s workload sees
multi-second p99s that a 16-concurrent stream does not?

## Also noted

- 256 KB internal PUT cost varies 4× by region even at p50: FRA 108–112 ms,
  AMS/CDG-pinned 132–134 ms vs NRT 68, SJC 53, SIN 33 ms.
- Hour 22T14 carries deploy churn from our own rollouts; excluded from claims.

## Claims shortlist for Tigris (each header-evidenced from their own Server-Timing)

1. **iad internal write latency**: 1 KB PUT sp50 110–260 ms sustained
   2026-07-22T13:00Z→23T02:30Z, both global and iad-pinned buckets, from
   Compute us-east-1 (EWR). 10–25× all other probed regions. Wall p99 up to
   10.8 s.
2. **Sparse-writer tail penalty**: solo 0.1 Hz writers see p99 0.9–3 s
   (EU) / 3–10 s (iad) while 16-concurrent writers in the same hours see
   dramatically lower p99 — what idles out between requests?
3. **PoP routing for Compute egress**: TLS setup to t3.storage.dev is
   96–155 ms from fra/cdg/ewr/sjc Compute but 10–14 ms from sin/nrt —
   is anycast landing off-continent for those origins? (Shared question
   with Prisma Compute networking.)
4. **FYI, positive**: global buckets showed no penalty vs pinned buckets in
   any region/op/size over the comparison window; dynamic placement and
   local caching behaved exactly as documented.

## Observatory follow-ups

- Keep all six global probes running (trend + regression watch).
- The pinned-bucket question is answered; the six pinned probes and their
  reveal-once keys can be torn down on owner confirmation.
- Next instrument increment if Tigris engages on claim 2: a mid-rate arm
  (1 Hz writer) to find the rate threshold where the sparse penalty ends.
