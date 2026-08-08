# Tigris region census — 2026-08-06

One fresh project + region-local bucket per Prisma Compute region, an
in-region probe (zero-dep Bun, hand-rolled SigV4 so every response
header is captured), 50 samples per op class per region, sequential
with 25 ms gaps. Internal = Tigris's own `Server-Timing total;dur`,
which isolates the vendor from the network path. Run id
census-b120c2eb, 06:53-06:57 UTC; 3,000 measured requests total.

## Tigris-internal ms — p50 / p90

| op | us-east-1 (iad) | us-west-1 (sjc) | eu-central-1 (fra) | eu-west-3 (cdg→fra) | ap-southeast-1 (sin) | ap-northeast-1 (nrt) |
|---|---|---|---|---|---|---|
| put_1k | **46 / 71** | 10 / 13 | 15 / 22 | 15 / 26 | 7 / 9 | 7 / 12 |
| put_256k | **135 / 257** | 53 / 107 | 72 / 132 | 73 / 118 | 23 / 31 | 58 / 145 |
| get_hit_1k | **24 / 32** | 3 / 5 | 7 / 9 | 6 / 10 | 3 / 6 | 3 / 5 |
| head_hit | **26 / 43** | 3 / 5 | 6 / 9 | 6 / 9 | 2 / 6 | 2 / 5 |
| get_miss (404) | **27 / 65** | 3 / 6 | 7 / 10 | 6 / 10 | 4 / 6 | 3 / 5 |
| head_miss (404) | **26 / 52** | 3 / 5 | 6 / 9 | 7 / 10 | 3 / 6 | 3 / 6 |
| list_1 (warm) | **23 / 38** | 3 / 4 | 5 / 8 | 6 / 9 | 2 / 5 | 2 / 5 |
| cas_create_ok | **55 / 107** | 10 / 15 | 13 / 16 | 14 / 19 | 7 / 9 | 8 / 14 |
| cas_conflict (412) | **25 / 39** | 5 / 7 | 7 / 10 | 7 / 9 | 4 / 6 | 4 / 6 |
| delete | **53 / 93** | 13 / 19 | 16 / 20 | 16 / 20 | 10 / 12 | 11 / 13 |

Wall times track internal + a small network delta everywhere (~2-10 ms),
except eu-west-3 (below). Statuses exactly as designed in all regions
(200s, 404s on misses, 412 on CAS conflicts, 204s); zero anomalies in
3,000 requests.

## Findings

1. **us-east-1 (iad) is the one degraded region — inside Tigris.**
   Every op class runs 3-10× the other five regions in Tigris-internal
   time: metadata-cache reads 24-26 ms internal vs 2-7 ms everywhere
   else, misses 27 vs 3-7, warm LISTs 23 vs 2-6, 1 KiB writes 46 vs
   7-15, CAS creates 55 vs 7-14, deletes 53 vs 10-16. The probe ran
   in-region (served-from: iad 150/150, wall-internal gap only
   ~8 ms), so this is not network and not our historical ewr↔iad
   distance story — the iad backend itself is uniformly slow relative
   to the rest of the fleet. Sample request ids per op class are in
   the census results for Tigris tracing.
2. **eu-west-3 has no local Tigris region.** Its bucket serves from
   fra (150/150) — consistent with Tigris's region list (no Paris) —
   so cdg compute pays a flat ~10 ms cross-city tax on every op
   (wall 16 ms vs 6 ms internal on reads). A placement fact, not a
   fault: eu-west-3 streams cells inherit fra storage latency + 10 ms.
3. **ap-southeast-1 (sin) has fully recovered** from the 2026-08-02/03
   degradation episode (idle GETs were 473 ms then) — it is now the
   FASTEST region across the board (reads 2-4 ms internal, 1 KiB
   writes 7 ms, 256 KiB writes 23/31 — the cleanest large-write tail
   in the fleet).
4. **The SRC 404 fix holds in all six regions**: misses are 3-7 ms
   internal everywhere except iad's general slowness — no region shows
   the old ~240 ms existence-check penalty.
5. **served-from labels are correct fleet-wide**: 900/900 labeled
   responses across six regions report exactly the local PoP
   (iad/sjc/fra/fra/sin/nrt), zero geo-exotic values — the
   multi-region confirmation of the header fix that the 2026-08-06
   single-vantage sweep (TIGRIS-404-COST.md §7) could not provide.
6. **256 KiB write tails are the one universal soft spot** (p90 107-257
   depending on region; sin excepted at 31) — relevant to WAL SST
   sizing at load, unchanged from prior observations.

## Consequences for us

- iad-internal slowness compounds our us-east-1 SLO problem (the
  historical "iad1 can't meet 250 ms ack p50" was attributed to
  compute↔storage distance; this shows the storage backend itself
  contributes ~5× baseline on the ack path's PUT+CAS mix). Worth
  reporting to Tigris as its own issue with the request ids.
- eu-west-3 cells should be modeled as fra-storage + 10 ms.
- No other region shows vendor-side degradation; the sin episode is
  closed.

Infrastructure: 6 projects torn down post-run (verified). Method and
probe app in the campaign workspace (census-b120c2eb).

## Follow-up census — 2026-08-08 (census2-1b99d720, 02:54-02:58 UTC)

Rerun after Tigris reported iad1 was "heavily loaded" and they would
fix it. Identical method, fresh projects/buckets, same 3,000-request
battery.

**us-east-1 (iad) is fixed for practical purposes** — every op class
dropped 3-4× and is now in-family with fra:

| op (iad) | 08-06 | 08-08 | fra 08-08 (reference) |
|---|---|---|---|
| put_1k | 46 / 71 | **14 / 18** | 15 / 22 |
| get_hit_1k | 24 / 32 | **5 / 8** | 6 / 8 |
| list_1 | 23 / 38 | **6 / 8** | 7 / 9 |
| cas_create_ok | 55 / 107 | **13 / 16** | 13 / 17 |
| cas_conflict | 25 / 39 | **7 / 10** | 8 / 10 |
| delete | 53 / 93 | **15 / 17** | 17 / 20 |
| put_256k | 135 / 257 | **94 / 181** | 69 / 116 |
| get_miss (404) | 27 / 65 | **14 / 17** | 7 / 9 |
| head_miss (404) | 26 / 52 | **15 / 19** | 7 / 9 |

Residuals worth naming:

1. **iad 404s sit at ~14-15 ms internal vs 3-7 ms in every other
   region** — hits, LISTs, CAS and deletes are all in-family now, but
   the miss path is still ~2-4× peers. Same shape as the (fixed) SRC
   fallback issue, iad-only; minor for us post-poll-stretch, worth a
   mention to Tigris.
2. One 503 on a 256 KiB PUT in iad (1 of 300 large writes this run;
   the first 5xx in 6,000 census requests across both runs).
3. **nrt's 256 KiB writes regressed this run**: 58/145 → 117/311 —
   now the worst large-write tail in the fleet (sin 25/36 same
   minute). Transient or trend, unknown from two points; the p50
   doubling says it is not just a tail event.
4. All other regions statistically unchanged run-over-run (±2 ms on
   small ops) — the method is reproducible enough to trust deltas of
   this size. served-from labels again 900/900 correct.

us-east SLO consequence: with iad internal costs now ≈ fra's, the
historical "us-east cannot meet 250 ms" attribution shifts back to
compute↔storage distance and our pipeline — worth a fresh us-east
streams soak before repeating that claim.

Infrastructure: 6 census2 projects torn down post-run (verified).
