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

## Methodology

**Topology.** One probe pair per Prisma Compute region — six regions:
eu-central-1 (FRA), eu-west-3 (CDG), us-east-1 (EWR), us-west-1 (SJC),
ap-northeast-1 (NRT), ap-southeast-1 (SIN). Each pair is two always-on
1-vCPU Compute services running the same binary (`bench/probe/`,
tigris-probe v6): one probing the region's **global** bucket (a Prisma
Bucket provisioned via the management API), one probing a **pinned**
bucket (Tigris single-region bucket in the nearest Tigris region:
fra/ams/iad/sjc/nrt/sin). Both write to the same per-region Prisma
Postgres with a `variant` column, so every comparison is same-VM-class,
same-network, same-clock.

**Operation mix.** Every 10 s (solo mode): PUT then hot GET (the object
just written) then cold GET (a fixed anchor object), each at 1 KB and
256 KB — six timed ops per tick, ~360 samples per series per hour. Every
60 s: one fresh-client GET (coldconn mode) that pays DNS+TCP+TLS on
purpose. At the top of each hour: 60 s of 16-concurrent 256 KB PUTs
(burst mode, global variant) to separate load-correlated behavior from
time-correlated behavior. 256 KB was chosen to match our WAL SST shape;
1 KB matches the published small-object benchmarks.

**What a sample measures.** Ops execute over presigned URLs with a plain
HTTP client so response headers are visible. Wall time = request issue to
response body fully read, measured with a monotonic clock inside the VM —
deliberately including the platform egress path, since that is what a
customer workload experiences. Each sample also records Tigris's own
`Server-Timing: total` value (reported here as sp50/sp99 — time inside
Tigris by the provider's own accounting) and the `X-Tigris-Served-From` /
`X-Tigris-Regions` headers, so wall time decomposes into
network-to-PoP + Tigris-internal, and placement claims are
header-verified rather than inferred.

**Connection discipline.** The pooled client uses a 4 s idle timeout
(production parity: the platform silently kills flows idle ≳5 s). Because
that guarantees a dead pool at each 10 s tick, every tick begins with one
untimed warm-up GET; solo samples therefore measure the warm path, and
coldconn is the only mode that measures connection setup. Before this
warm-up existed (probe ≤v4), the first timed op of each tick absorbed the
TLS handshake — see hygiene below.

**Storage and aggregation.** Every sample is one Postgres row
(ts, op, mode, size, variant, wall ms, ok, error text, server_ms,
served_from, regions). Hourly aggregates use `percentile_cont` over
successful ops only; failures are counted separately and never enter the
latency distributions. With ~360 samples per series-hour, an hourly p99 is
approximately the 4th-slowest sample — tail figures are indicative, and
multi-hour persistence (as in the iad finding) is required before we treat
a tail as a claim.

**Data hygiene applied in this report.** Solo PUT rows before
2026-07-22T13:30Z are TLS-contaminated (pre-warm-up probe) and excluded;
pinned collection begins ~14:45Z, so global-vs-pinned uses like-for-like
hours from 15:00Z; hour 14 carries our own deployment churn and is
excluded from claims. Instrument version timeline: v4 13:0x Z (header
capture), v5 ~13:30 Z (warm-up), v6 ~14:45 Z (variant dimension).

**Known limitations.** One instance per region (no instance-to-instance
variance); the 0.1 Hz solo cadence is intentionally sparse — which turned
out to be a finding (Verdict 4) rather than a flaw, but means solo numbers
should not be read as under-load numbers; bursts run only against the
global variant; the observatory measures Tigris via `t3.storage.dev`
anycast from inside Prisma Compute, so network-side findings are about
that combined path, not Tigris's network alone. Source, page code, and the
/data API used for all tables in this report: `bench/probe/` on the
`slate` branch; each region page serves `/data?day=YYYY-MM-DD`.

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
