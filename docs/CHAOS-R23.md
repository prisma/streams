# R23 — closing the chaos review

> **Superseded on R23-1 by [CHAOS-R24.md](CHAOS-R24.md).** The hard
> backlog bound described below was built on process-lifetime counters.
> A later review showed that is a process-local heuristic, not a durable
> safety boundary — it could manufacture backlog for records that were
> never committed, hide real backlog across a restart, and misinform both
> sides of an ownership move. R24 rebuilt it on durable, ownership-scoped
> per-shard state. Read R23-1 below as "the mechanism and its intent";
> R24 is the implementation that can be relied on.
>
> Evidence in this document is labelled: **mechanism** (code exists),
> **local proof** (deterministic test), **field proof** (measured on a
> real edge), **open** (not yet established).

Response to the review of `32d4b880` (`docs/CHAOS-CAMPAIGN.md`). The
review's verdict was that the campaign was productive but three findings
were not fully closed, one bug class survived outside the route it was
fixed on, and the central capacity problem was correctly left open.

Its one-sentence requirement drove the work order:

> Prisma Streams must never accept writes indefinitely faster than it can
> either absorb them or safely reject them.

| Review item | Was | Now |
|---|---|---|
| CHAOS-1 truncated gather | Fixed correctly | Closed; stale comment fixed |
| CHAOS-2 invalid storage config | Core fix correct; readiness partial | Readiness hardened (R23-5) |
| CHAOS-3 oversized-frame reservation | Useful, not fully closed | Namespace-pinned + metric corrected (R23-2, R23-3) |
| CHAOS-4 invalid query values | Fixed on one route only | Strict on every public route (R23-4) |
| CHAOS-5 absorption < ingestion | Open, launch-gating | Hard bound shipped; throughput still open |
| Crash durability | Strong | Re-verified on the final binary |

## What the review found that I had got wrong

Three of these were my errors, not merely missing work. Recording them
plainly because the pattern matters more than the individual bugs.

**`effectiveGatherConcurrency` was a telemetry artifact.** The campaign
report presented `gatherConc=2` at a 1 MiB body ceiling as a measured
result. The surface computed `capacity / worst_frame` and ignored the
packing term, but a gather reserves
`min(capacity, max(packing x 3, worst_frame))`. The packing limit is
clamped to `capacity / 3`, so `packing x 3` is the entire capacity and
exactly one gather could ever run. Re-measured on the corrected binary:

```
ceiling 32 MiB   worstFrame 96.2MiB  perGatherReservation 96.2MiB  slots 2  effectiveConc 1
ceiling  1 MiB   worstFrame  3.2MiB  perGatherReservation 64.0MiB  slots 2  effectiveConc 1
```

Lowering the ceiling never raised concurrency anywhere. It lowers the
reservation, which is a different and real benefit.

**Readiness counted attempts, not distinct shards.** The comment claimed
"one poison stream must not evict a healthy instance"; the code counted
global failed attempts, so one bad shard retried three times could evict
a never-used instance. The test passed only because it happened to use
three distinct prefixes — production code enforced nothing.

**The readiness claim was too broad.** I wrote that the check covers
"wrong bucket, bad credentials, unreachable endpoint". It only covers
failures that reach a shard open. I had actually *observed* the gap —
killing the object store after boot left `/health` ok with
`shard_opens.started == 0` — and reported it as a deliberate asymmetry
rather than recognising it as a hole in my own claim.

## R23-1 — the hard backlog bound (the central fix)

`src/backpressure.rs`. High/low hysteresis over
`MAX_UNABSORBED_BYTES_PER_INSTANCE`, `MAX_UNABSORBED_BYTES_PER_SHARD`,
`MAX_ABSORB_LAG_SECS` (`MAX_REPLAY_BYTES` existed in this round and was
deleted in R26-6: it was the same open-engine sum as the instance bound
under a name implying ownership-wide replay projection). Past the high mark, new
appends get a retryable `503 maintenance_backpressure`; admission
reopens below the low mark.

The blast radius is the part that matters. **Only record appends shed** —
`POST .../records`, `:batch`, and the raw route. Reads, consumer `:pull`
and `:settle`, and every control-plane operation stay admitted, because
shedding a consumer stops the drain and shedding the control plane makes
the overload unrecoverable at exactly the moment an operator needs to
delete a stream, move ownership, or run cleanup. `is_append_request()`
is a tested classifier over the real route grammar, not a prefix guess.

`bench/chaos/backpressure-gate.sh`, 8/8 on the 1 GiB posture — **local
proof of the mechanism only**. It exercised the process-local counters
this bound was later rebuilt off, so a green result here proved the
latch and the blast radius, NOT that the backlog figure was trustworthy:

```
ok  appends refused once the backlog bound was passed (503)
ok  refusal is typed maintenance_backpressure
ok  refusal is marked retryable
ok  reads still admitted while shedding (200)
ok  control plane: create still admitted (201)
ok  control plane: delete still admitted (204)
ok  control plane: seal still admitted (200)
ok  admission reopened after the backlog drained
```

## R23-2 — the body ceiling is namespace-immutable

The review's sequence: a namespace holding an unabsorbed 32 MiB record,
restarted with `MAX_REQUEST_BODY_BYTES=1048576`, would reserve 24 MiB
for a record whose real transient approaches 96 MiB — reintroducing the
under-reservation the process-wide budget exists to prevent.

`topology.json` now records `maxRequestBodyBytes` at namespace init. A
mismatch refuses startup and names the stored value. Topologies written
before the field carry `None` and are held to the 32 MiB pin they were
created under. Verified:

```
init at 1 MiB          -> "body ceiling 1048576 bytes (namespace-pinned)"
restart at 32 MiB      -> exit 1, "...this namespace was created with 1048576...
                          Set MAX_REQUEST_BODY_BYTES=1048576, or point
                          PATH_PREFIX at a fresh one."
restart at 1 MiB       -> starts
```

## R23-3 — one shared concurrency accounting

`per_gather_reservation_bytes()` and `effective_gather_concurrency()`
now back the startup log, `/v1/debug/absorb`, and the tests, with
`perGatherReservationBytes` exposed alongside. A test asserts the
reported number against what the budget will actually admit.

## R23-4 — the bug class, not just the route

Same `.and_then(parse).ok()` pattern found on three more public routes,
each collapsing a malformed value into the route default: scan
`maxBytes`, watch `timeoutMs`, catalog `limit`. All three answer 400.
`strict_query()` adds the query-string equivalent of
`deny_unknown_fields` — unknown parameters and duplicated scalars are
refused, because "you believe this works and it does not" and "you
stated two intents" are as wrong as an unparseable value.

Oversized bodies are refused rather than reset: a declared
`Content-Length` above the ceiling gets 413 before the body is read.
Previously the server stopped reading mid-stream and the client saw a
transport error, unable to distinguish refusal from a broken network.

`bench/chaos/hostile-surface.sh` grew 42 → 48 checks. 48/48 locally.

**Scope correction (R24-D):** this section overstated the result.
`strict_query()` was wired to the CATALOG route only. Malformed numeric
values were strict everywhere, but unknown keys and duplicate scalars
stayed silently accepted on records/scan/watch until R24-D. The gate is
54 checks now.

## R23-5 — readiness hardening

Distinct failed prefixes (not attempts) now back the unready verdict. A
synchronous **startup storage canary** closes the class the runtime
signal cannot see: write a probe to the ops, shard and data buckets,
read it back, compare bytes, delete. A write *and* a read, because
credentials that can read but not write are a real silent failure.

```
unreachable store -> exit 1, "startup canary: cannot WRITE to the ops bucket
                     — this process would have booted, answered /health with
                     ok, and failed every append"
healthy store     -> "ops/shard/data buckets readable and writable"
```

`/livez` (process alive) and `/readyz` (storage usable) now split the two
questions; `/health` keeps answering readiness for existing probes. A
never-ready instance **exits** after `UNREADY_EXIT_AFTER_SECS` (default
300) rather than sitting in rotation-limbo: once readiness reports 503
the load balancer stops sending the traffic that would trigger another
open, so it cannot recover on its own even after the store heals.

## R23-6 — measuring the read, before optimizing it — **EXPERIMENTAL**

> Not decision-grade. The counters are process-global deltas snapshotted
> around a gather, so they include concurrent customer reads, registry,
> billing and fleet traffic; the denominator uses batch accounting that
> includes keys and WriteBatch overhead rather than raw frame bytes
> advanced; and GET_BYTES uses object metadata size, so a ranged read
> bills the whole object. Do NOT choose read-ahead, tail-ring, or
> two-stage fetch/build architecture off these numbers. Making the
> accounting operation-local is open work.

The review's sharpest diagnostic point: gather concurrency is the knob
that *compensated* for CHAOS-5, not the deepest cause. The underlying
fact is that reading ~4 MiB takes 21–42 s.

Object-store range reads now count GETs and bytes, and each gather
snapshots the delta across its read phase. `/v1/debug/absorb` exposes
`lastReadGets`, `lastReadFetchedBytes`, `lastReadAmplificationX1000`.
Amplification decides where the fix belongs: high means the read path
(read-ahead, fetch concurrency, L0 overlap, a durable-tail ring); near
1× means latency rather than volume, and the answer is a two-stage
pipeline separating I/O concurrency from the memory-heavy build.

## Field results (R23 build, Singapore)

**Hostile surface, live edge: 48/48.** The rerun the review asked for.
It found two failures on the first pass that local testing could not
produce, both now fixed — see R23-9 below.

**SIGKILL durability on the final binary: 2 rounds, 3,590 acked records,
zero lost, zero duplicated, zero phantom, per-producer FIFO intact.**

**Capacity gate (R23-8), 17 min of steady-state soak so far:**

```
absorbed 353 KB/s   ingest 338 KB/s   absorb/ingest = 1.04
lag 0-365 s         RSS 83-401 MB (500 MB shed line)
health non-200: 0   backpressure engagements: 0
```

Absorption is keeping up at this load, where the pre-R23 measurement was
~136 KB/s against ~310 KB/s. **Two things stop that being a clean
before/after.** The offered load differs (this generator settled around
46-68 rps against 122 earlier), and more importantly this is a FRESH
namespace: history-tier depth drives read cost, so a young LSM reads
faster than the accumulated one the earlier numbers came from. The
honest reading is "no deficit observed at this load on this namespace",
not "the deficit is fixed".

The pause/resume legs and the catch-up verdict were still running when
this was written; `$SOAK_HOME/results/chaos/capacity-gate.jsonl` carries
the full series.

**Read amplification has no field data yet.** The deployed binary
predates the R23-6 instrumentation fix, so `lastReadGets` is 0 in every
sample above. The fix is in the code and tested; it needs one more
deploy to produce numbers.

## R23-9 — the WAN rerun found what local testing could not

Two failures on the first live pass, both real:

**A 2 MiB body returned 502 while 8 MiB and 64 MiB returned 413.** The
large ones only passed because curl negotiates `Expect: 100-continue`
above ~1 MiB, so the refusal lands before the upload starts. Below that,
the client is still streaming when the server answers and closes, and
the edge proxy reports the truncated exchange as 502 — a server error
for a client error, and one that invites an impossible retry. The server
now drains the body (bounded at 8 MiB) before answering. Re-measured:
real 2, 9 and 64 MiB uploads all return 413.

**`cursor=%%%%` returned 200 over the WAN and 400 locally.** An edge
proxy normalizes malformed percent-escapes away, so that check silently
became a no-op in the environment where it mattered. The gate now uses a
malformed cursor that survives proxies.

One check was dropped rather than fixed: declaring a 64 MiB
Content-Length and then sending nothing. That framing is never
completed, the server closes, and the edge reports 502. Asserting 413
there would assert behaviour we do not control and no real client
produces.

## CHAOS-5 remains open

Unchanged from the review's position, and reinforced by it:
`ABSORB_GLOBAL_GATHERS` stays at 1. The 4-slot configuration delivered
~660 KB/s and then OOM-killed the instance with reservations accounted
for exactly and RSS at 362 MB against a 500 MB line — so the per-gather
reservation understates the real transient of a concurrent gather. The
dangerous "set GATHERS=2 on larger instances" advice has been removed
from `deploy/profiles/compute-1g.env`.

What R23-1 changes is the *consequence* of the deficit: unbounded growth
is now bounded, retryable refusal that an operator can repair through.
That is a safety property, not a throughput fix. The capacity gate
(R23-8) is what would license publishing a supported write rate.
