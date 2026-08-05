# Single-region Prisma Buckets — measured impact on Streams (2026-08-05)

The platform switched Prisma Buckets from Tigris GLOBAL buckets to
SINGLE-REGION buckets, intending (a) to eliminate the ~1% metadata
trickle and (b) to make 404s faster — with a known Tigris-side bug
that keeps 404s slow even on single-region buckets (under
investigation by Tigris).

We measured, same binary (`freeze3` = `c1d2aedb`), same fra Compute
service, same burst (300 keyed appends), server-side
`/v1/debug/store` op latencies; plus client-side boto3 probes from a
second vantage (Asia). Buckets: the retained campaign bucket
(pre-change, global, `fly.storage.tigris.dev`) vs two freshly created
buckets (post-change, single-region, `t3.storage.dev`) — one created
plain, one with a `"region":"eu-central-1"` hint in the create call.

## Results (fra service vantage, p50 ms)

| op | global (old) | single-region A (plain) | single-region B (region-hinted) |
|---|---|---|---|
| put:wal (ack path) | **27** | 220 | 212 |
| put:manifest | 25 | 131 | 133 |
| get:other (data hits) | 65 | 114 | 115 |
| get:manifest (mostly freshness probes / 404-shaped) | **268** | **111** | **110** |
| head:wal (probe, 404-shaped) | **293** | 292 | **228** |

Client-side from Asia: global hits ~55–82 ms (nearest-edge serving),
single-region hits ~250–590 ms (distance to the pinned region);
GET-404s ~290–305 ms on BOTH bucket types from Asia.

## What this means

1. **Placement is the whole game, and it is currently wrong for us.**
   Both single-region buckets — including the one created with a
   region hint, which the API accepted silently and ignored — pinned
   ~100 ms RTT away from Frankfurt (US-shaped). Every store op from
   the fra service degraded accordingly; the ack-path `put:wal` went
   27 ms → ~215 ms, which would take fra's append p50 from ~59 ms to
   roughly 250 ms+. **As shipped, the single-region change makes a
   fra-based Streams service dramatically slower.** The fix is
   platform-side: bucket placement must follow (or be selectable to)
   the consuming service's region. Until then, production guidance:
   keep latency-sensitive cells on their existing global buckets.
2. **The 404 improvement is real — on the GET path.** On the global
   bucket, 404-shaped GETs cost a FIXED ~270–300 ms from every vantage
   we have ever measured (fra, Asia; and soak10's idle-probe economics
   were built around exactly this). On single-region buckets the
   GET-404 collapses to ~one RTT to the pinned region (111 ms from
   fra to a US-pinned bucket; would be ~5–15 ms co-located). The old
   fixed penalty is gone from GET.
3. **The residual Tigris bug is on HEAD.** `head:wal` stayed at
   ~230–290 ms on single-region buckets — unchanged from global —
   while GET-404s improved. If Tigris is looking for a
   discriminator: on `t3.storage.dev` single-region buckets, GET on a
   missing key ≈ one region RTT, HEAD on a missing key ≈ the old
   ~290 ms penalty. (SlateDB's opener and WAL probes use HEAD, so
   this still taxes reopen/probe paths.)
4. **The ~1% metadata trickle** is not resolvable in these short
   windows; it needs a long soak on a CO-LOCATED single-region bucket,
   which is blocked on (1). Deferred until placement is fixed.

## Follow-ups

- Platform: region-follows-service placement (or honor the `region`
  create parameter) for single-region buckets. Re-run this A/B when
  available; expected: put:wal ≈ 5–15 ms co-located (vs 27 ms global),
  GET-404 ≈ 5–15 ms, and the poll-stretch posture (soak10) can be
  revisited if idle 404 probes become cheap.
- Tigris: HEAD-on-missing-key latency on single-region buckets
  (`t3.storage.dev`), numbers above.
- Housekeeping: test buckets `streams-sr404-test`
  (`bkt_fjep08jld38d9kqm48h14il5`) and `streams-sr404-fra`
  (`bkt_wty98pjw1c0ekcjwjh8gobkg`) in project `streams-camp75-eu` are
  kept for the re-run; delete with the project teardown. The fra
  retained service was restored to `freeze3` + its original bucket and
  re-verified (saga smoke PASS) after the A/B.
