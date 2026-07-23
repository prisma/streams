# Message to Tigris — findings from a 6-region continuous latency observatory

*Prepared 2026-07-23. All data measured from inside Prisma Compute microVMs.
Companion detail: `bench/tigris-observatory-report-1.md` (methodology + first
14 h); raw per-IP diagnostics available per region at each probe's `/diag`.*

---

Hi Tigris team,

We run Prisma Streams (durability-critical: every append is acknowledged only
after a WAL object PUT lands in Tigris) and Prisma Buckets, on Prisma Compute
microVMs. Since your PUT latency is effectively our ack latency, we run a
continuous observatory: one always-on VM in each of six regions, hitting
`t3.storage.dev` every 10 seconds and logging every sample — wall time, your
`Server-Timing: total` value, and `x-tigris-served-from` — to Postgres.

**Setup (for reproduction/correlation):**

- Cadence per region: every 10 s, PUT + hot GET + cold GET at 1 KB and
  256 KB on a warm connection (untimed warm-up first); every 60 s a
  fresh-connection GET (full DNS+TCP+TLS); hourly a 60 s burst of 16
  concurrent 256 KB PUTs. Presigned requests, reqwest/rustls, HTTP/1.1.
- Each region measures **two bucket variants identically**: a global bucket
  and a bucket pinned to the nearest Tigris region. Result up front: global
  ≈ pinned in every region, op, and size — none of the below is
  global-bucket routing.
- Global buckets: `user-eiuyudn91p0y08dvqwygnekt` (nrt),
  `user-mkxumlxecxujb11zq0e8rbsq` (sin), `user-fdjkgmd8ncgdkcke57enrhkm`
  (fra), `user-c6pctjrlt103tlpbpyhbbtmg` (cdg), `user-pb2mzu8zdmitx0tk9x07km4l`
  (iad), `user-nc7m3z2qimfyzlaoaa7fwqln` (sjc). Pinned:
  `prisma-compute-{ap-northeast-1, ap-southeast-1, eu-central-1, eu-west-3,
  us-east-1, us-west-1}`.
- Running continuously since 2026-07-22 ~13:30 UTC.
- Probe egress IPs (source addresses your edge sees): fra
  `192.248.181.165`, cdg `217.69.3.105`, ewr `140.82.10.127`, sjc
  `149.28.204.69`, sin `139.180.184.222`, nrt `45.76.212.83`.

Two issues, both reproducible right now.

---

## A. One member of the EU DNS answer set for `t3.storage.dev` is ~200 ms away

From both our Frankfurt and Paris VMs, `t3.storage.dev` (and
`<bucket>.t3.storage.dev`) resolves to four A records. Three behave like a
local edge; one does not. Raw TCP connect + TLS handshake, three rounds per
IP, measured 2026-07-23 ~05:35 UTC:

**From eu-central-1 VM (egress 192.248.181.165):**

| resolved IP | TCP connect ms | TLS handshake ms |
|---|---|---|
| 129.159.14.217 | 0.8 / 1.0 / 0.9 | 5.9 / 5.1 / 5.5 |
| 138.3.243.185 | 1.3 / 0.8 / 0.8 | 5.0 / 5.3 / 4.7 |
| 144.24.169.163 | 5.8 / 3.7 / 2.8 | 19.5 / 10.7 / 8.7 |
| **130.61.20.236** | **217 / 247 / 177** | **717 / 721 / 565** |

**From eu-west-3 VM (egress 217.69.3.105):** same four IPs; the three
healthy ones at 9–10 ms TCP / 13–14 ms TLS (Paris→Frankfurt), and again
**130.61.20.236 at 171–278 ms TCP / 525–775 ms TLS**.

An earlier run (~05:31 UTC) caught a different member (144.24.169.163) at
120–135 ms TCP / 292–438 ms TLS from fra — so which member is "far" moves
around. S3 clients don't race addresses: whoever picks the bad record pays
0.5–1 s of connection setup, and *every subsequent request on that
connection* runs cross-continent. Our fresh-connection cost over 14 h,
p50: sin ~10 ms, nrt ~14 ms, sjc ~96 ms, fra ~121 ms, cdg ~143 ms, ewr
~155 ms — the EU/US numbers are this lottery, not distance.

`x-tigris-served-from` over the last 24 h confirms connections landing on
distant edges (counts out of ~48 k requests per region):

- fra VM: 980 requests served from **ord1**, 109 from lhr, 16 from jnb
- sin VM: 1,812 from nrt, **345 from fra**
- ewr VM: **647 from gru**
- cdg VM: 24.2 k fra / 23.7 k ams (both nearby — fine)

Also visible in the same diagnostics: the **first request on a fresh
connection** costs ~70–100 ms even when TCP/TLS to the edge are
single-digit ms — and your own header attributes it internally:
`Server-Timing: total;dur=97` (fra, request id 1784784946379780153),
`dur=102` (cdg, 1784784958326399113), `dur=73` (sjc, 1784784951267671273),
each 2026-07-23 ~05:35 UTC, vs `dur=0–3` for the immediately following
warm requests on the same connection. On iad the same first-request cost
shows as ~90 ms wall with `dur=0` (1784784950004883913). Looks like a
per-connection warm-up somewhere behind the edge (backend connection or
auth/metadata cache?).

**Asks:**

1. Can you check the advertisement/health of **130.61.20.236** (and
   generally the rotation policy of the EU answer set) for the source
   prefixes above? One ~200 ms member in a 4-record set means a ~25 %
   chance per fresh connection of a 0.5–1 s setup from your two EU
   regions.
2. The stray `served_from` values (ord1/gru/fra for EU/US-East/APAC VMs)
   suggest anycast occasionally lands us on another continent. Is that
   expected fallback behavior, and can it be tightened for these prefixes?
3. What is the ~70–100 ms first-request-on-fresh-connection cost
   (`dur=73–102` internal)? If it's a backend/session warm-up, is there
   anything we can send (e.g. a cheap prime request) that avoids paying it
   on a real operation?

## B. us-east-1 (iad) internal processing time on writes is degraded, >26 h and counting

Using only your `Server-Timing: total` values (network excluded by
construction), solo warm-path ops, last 24 h, both variants:

| VM region | 1 KB PUT sp50/sp99 (global) | (pinned) | 256 KB PUT sp50/sp99 (global) | (pinned) |
|---|---|---|---|---|
| nrt | 7 / 19 ms | 7 / 16 | 71 / 439 | 70 / 437 |
| sin | 9 / 29 | 8 / 242 | 34 / 732 | 34 / 1141 |
| fra | 14 / 236 | 14 / 120 | 117 / 1252 | 112 / 1236 |
| cdg | 14 / 33 | 17 / 40 | 126 / 1243 | 135 / 1277 |
| **iad** | **177 / 8027** | **164 / 5492** | **249 / 4813** | **242 / 3450** |
| sjc | 20 / 72 | 17 / 63 | 54 / 148 | 53 / 144 |

iad 1 KB hot-GET p99 is also seconds (1.6 s global / 2.0 s pinned) while
p50 stays 0–34 ms. This started before our observatory did (we've measured
it continuously since 2026-07-22 13:30 UTC; the first 15 hourly buckets
were 110–260 ms p50 — see report #1 — and the last 24 h are *worse*), it
is write-dominated, small-object-dominated, and identical on a global and
a region-pinned bucket, so placement/routing is excluded.

**Asks:**

4. What is elevated on the iad write path for small objects
   (metadata/quorum/placement layer?), and is it tenant- or
   bucket-specific? Bucket names and the exact time series are above /
   attached; we log every sample.
5. We keep `x-amz-request-id` for every diagnostic round and can capture
   them for arbitrarily many slow PUTs on demand — what volume/format is
   most useful for your tracing?

Secondary observation, no ask: 256 KB PUT internal p99 exceeds 1.2 s in
fra/cdg/sin (table above) — consistent with the tail behavior in our
report; if the iad investigation surfaces something systemic for write
tails, these are the other places it shows.

Each probe serves a live 24 h dashboard (`/`), raw JSON (`/data?date=…`),
and the network diagnostics above (`/diag`) — happy to share the six URLs
privately, run additional experiments from any/all regions, or hand over
raw sample dumps. The fleet is already in place.

Thanks!
Søren / Prisma
