# Message to Tigris — latency findings from a 6-region continuous observatory

*Draft; data sections filled from probe v7 diagnostics collected 2026-07-23.*

---

Hi Tigris team,

We run Prisma Streams — a durability-critical streaming layer where every
append is acknowledged only after a WAL object PUT lands in Tigris — plus
Prisma Buckets, both on Prisma Compute microVMs. Because our ack latency is
essentially your PUT latency, we stood up a continuous latency observatory:
one always-on probe VM in each of six Prisma Compute regions, each writing
to Tigris through `t3.storage.dev` every 10 seconds and logging every
sample (wall time, your `Server-Timing: total` value, and
`x-tigris-served-from`) to Postgres.

**Setup, so you can reproduce/correlate:**

- Probe cadence per region: every 10 s a PUT + hot GET + cold GET at 1 KB
  and 256 KB over a warm connection (an untimed warm-up request first);
  every 60 s one fresh-connection GET (full DNS+TCP+TLS); hourly a 60 s
  burst of 16 concurrent 256 KB PUTs. All object ops are presigned
  requests via reqwest (HTTP/1.1, TLS 1.3).
- Two bucket variants per region, measured identically: a **global** bucket
  (the Prisma Buckets default) and a **pinned** bucket restricted to the
  Tigris region nearest the VM. This let us rule global-bucket routing in
  or out.
- Buckets (global): `user-…` per region — we can share exact names
  privately. Buckets (pinned): `prisma-compute-{iad,sjc1,ams,fra,nrt,sin}`.
- Regions and time window: fra, sin, nrt, ewr, sjc, cdg probes running
  continuously since 2026-07-22 ~13:30 UTC (pinned variant since ~14:45
  UTC).

Two findings we'd like your help with. Everything below is measured from
inside the VMs (no laptop/anycast noise), and global-vs-pinned showed **no
difference** in any region — so none of this is global-bucket routing.

---

## A. Connection setup to `t3.storage.dev` is expensive from some regions — PoP routing?

Fresh-connection cost (DNS+TCP+TLS, measured as coldconn-minus-warm), p50
over 14 h:

| VM region | TLS+TCP setup p50 |
|---|---|
| sin | ~10 ms |
| nrt | ~14 ms |
| sjc | ~96 ms |
| fra | ~121 ms |
| cdg | ~143 ms |
| ewr | ~155 ms |

sin/nrt look like a nearby edge (~1–2 RTT). fra/cdg/ewr/sjc pay 100–155 ms
to set up a connection to an endpoint that then serves the actual
operation in ~10–20 ms — which strongly suggests the TCP/TLS termination
point is much farther away than the serving region, or extra RTTs are
being added on the path.

To help you localize it, our probes now expose per-IP diagnostics
(`/diag`): resolved A/AAAA set for `t3.storage.dev` and the bucket host,
raw TCP connect time and TLS handshake time per resolved IP (3 rounds),
our egress IP as your edge sees it, and `fly-request-id` echoes from
authenticated GETs.

**[DIAG-TABLE-A: per-region egress IP, resolved IPs, TCP ms, TLS ms, fly-request-id PoP]**

Asks:

1. From the `fly-request-id` PoP suffixes and our egress IPs below, can you
   check which edge each of these source prefixes is being routed to, and
   whether that's the intended advertisement for these locations?
2. TCP connect alone from fra/cdg/ewr is **[N]** ms — is the L4 hop
   terminating far from the region, or is there an in-path proxy adding
   round trips before TLS completes?
3. Is there a recommended endpoint/addressing mode (regional endpoint,
   different hostname, HTTP/2) that avoids the anycast detour for
   VM-resident clients that know their region?

## B. One region shows persistently elevated Tigris-internal time on small PUTs

Using your own `Server-Timing: total` header (so network is excluded by
construction): 1 KB PUT internal time, p50 per hour, was **110–260 ms in
us-east-1 (iad) for 15+ consecutive hours**, on both the global and the
pinned bucket, while every other region sits at 8–25 ms for the same
operation. Reads from iad are normal; 256 KB PUTs are elevated
proportionally less.

**[TABLE-B: fresh 24 h sp50/sp99 per region, op × size, both variants]**

Asks:

4. What is different about the iad write path for small objects right now
   (metadata/quorum/placement)? Is this a known condition, and is it
   tenant- or bucket-specific?
5. We can hand you `x-amz-request-id` / `fly-request-id` values for slow
   samples on demand (probes keep them) — what's the most useful format
   for you to trace these?

Happy to give you live access to the observatory pages (each region's
probe serves its own 24 h charts + tables and a `/diag` endpoint), or to
run any additional probe you'd find useful — the fleet is already in
place.

Thanks!
Søren / Prisma
