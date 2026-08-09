# Chaos campaign — 2026-08-09, Singapore (ap-southeast-1)

Adversarial session against `streams-slate`: break it any way possible,
fix what breaks, report. Load ran on Prisma Compute in Singapore against
Tigris; the attack surface work ran locally against the same binary on
the 1 GiB posture (`deploy/profiles/compute-1g.env`).

**Four defects found and fixed, two of them P0.** Both P0s were silent:
the system reported itself healthy while being permanently broken. No
data-loss defect was found — durability held under every attack,
including four SIGKILLs.

| # | Severity | Defect | Commit |
|---|---|---|---|
| CHAOS-1 | P0 | Absorber wedge: a truncated gather stranded data in the hot tier forever | `506039dd` |
| CHAOS-2 | P0 | Invalid engine config accepted — and the shipped defaults *were* invalid; `/health` said `ok` while every append 500'd | `3e701c34` |
| CHAOS-3 | P1 | 96.2 MiB of the 500 MB shed line reserved per gather for a record size the deployment may never accept | `c00b19ae` |
| CHAOS-4 | P2 | `maxBytes` / `waitMs` silently defaulted instead of rejecting values they could not parse | `204ac19c` |

---

## CHAOS-1 — the absorber could strand data in the hot tier forever

`absorb_gather_v2` packs frames until it hits the per-stream gather cap.
When a stream's backlog exceeded that cap the gather advanced *part* of
the way and then reported the stream as fully settled, so the tick loop
dropped it from `pending`. Nothing ever put it back: not the 60 s sweep,
not the 10-minute durable rescan, not a fresh append signal, not a
process restart.

Field reproduction — three 32 MiB frames (100.6 MB) into one stream:

```
absorbed 33,554,432 of 100,660,225 bytes … and stopped
```

67 MB parked in the shard log permanently. Each process boot absorbed
exactly one more record (seed → one gather → wedge again). Scaled up
behind a 64 KiB cap, absorption froze at 102,444 of 819,200 bytes.

**No data was lost** — 100,660,225 bytes read back byte-exact before and
after trim pressure. The damage is unbounded hot-tier growth: the shard
log never trims, storage costs climb, and restart replay grows without
limit.

Fix: `GatherOutcome::partial` records streams that advanced but did not
reach their durable end; the tick loop keeps them pending so the next
tick continues immediately. Monotonic drain to 819,552 in ~42 s after
the fix.

The regression test needed care. The first version passed *without* the
fix, because six separate appends emit six signals and each re-arms the
stream, masking the wedge. It only reproduces with **one batched append**
of six 100 KiB entries behind a 64 KiB cap — then it fails with
`absorbed=5 next=6`.

## CHAOS-2 — a data plane that cannot open a shard called itself healthy

SlateDB validates `max_unflushed_bytes > l0_sst_size_bytes` at **engine
open** time, and shard engines open lazily on first use. An invalid pair
therefore passes startup, and the failure only appears per-request.

The shipped defaults were that invalid pair: `L0_SST_SIZE_BYTES` 32 MiB
against `MAX_UNFLUSHED_BYTES` 16 MiB. A bare `streams-slate` with no
environment at all:

```
health: ok          <- forever
create: 201         <- the registry DB has its own valid settings
append: 500         <- forever, every request, for the life of the process
```

The only evidence was one `WARN` per attempt. A load balancer keeps such
an instance in rotation indefinitely.

Three fixes:

1. `validate_engine_settings()` runs over both engine tiers before any
   store opens and refuses to start. Deliberately fail-fast rather than
   clamp: the memory posture is an operator declaration that the
   acceptance campaign verifies knob-for-knob, so silently substituting a
   value would make that verification a lie.
2. `L0_SST_SIZE_BYTES` default 32 MiB → 8 MiB (the field-validated 1 GiB
   posture). `MAX_UNFLUSHED_BYTES` stays 16 MiB — it is the bound that
   stops a byte-flood OOMing a 1 GB instance, so the oversized knob was
   the L0 one.
3. `/health` returns 503 when the process has **never** opened a shard
   while repeatedly failing to. That covers the whole broken-from-boot
   class — invalid config, wrong bucket, bad credentials, unreachable
   endpoint — which all look identical at the edge.

The health condition is narrow on purpose (zero lifetime successes). A
single poison stream must not evict a healthy instance, and a mid-life
store blip must not cascade a running fleet out of rotation. Verified:
killing the object store *after* boot leaves health `ok` with
`shard_opens.started == 0`, because the create failed at the registry
read and never reached an open. That asymmetry is the point.

## CHAOS-3 — the shed line was being spent on bytes nobody allocated

Steady state in Singapore: 16,500 of ~290,000 requests shed (~6%) while
RSS sat at 371–378 MB against a 500 MB line. The missing headroom was
not memory, it was a reservation:

```
reservedBytes   = 100,859,904   (96.2 MiB, constant, == capacity)
lastActualBytes =   6,070,719   (5.8 MiB, the work it was doing)
pressure        = RSS + reserved ≈ 474 MB of the 500 MB line
```

Reserving worst-case before reading is correct — one legal oversized
frame must be able to proceed alone. The defect is that "worst case" came
from `MAX_BODY_BYTES`, a compile-time constant, so every deployment
reserved 96.2 MiB (19% of the line) for a 32 MiB record it may never
accept, with no way to say otherwise.

`MAX_REQUEST_BODY_BYTES` makes the ceiling configurable, lowering-only,
so the pinned protocol maximum and the default are unchanged. Measured on
the release binary:

```
MAX_REQUEST_BODY_BYTES=33554432   worstFrame=96.2MiB capacity=96.2MiB gatherConc=1
MAX_REQUEST_BODY_BYTES=1048576    worstFrame= 3.2MiB capacity=64.0MiB gatherConc=2
```

Two wins: 93 MiB returns as admission headroom, and the budget stops
being floored up to a single worst frame, so effective gather
concurrency goes 1 → 2. The one-gather serialization documented in the
OOM campaign was a consequence of the 32 MiB pin, not a law.

## CHAOS-4 — parameters that were silently defaulted instead of refused

Side by side on the same handler:

```
?deliver=bogus       -> 400 invalid_deliver
?routingKey=<1KiB+>  -> 400 invalid_routing_key
?maxBytes=-5         -> 200, and the response carried the 8 MiB default
?waitMs=abc          -> 200, and the long poll silently became a hot read
```

Both used `.and_then(|v| v.parse().ok())`, turning "I could not
understand this" into "you did not ask". A caller that computes
`maxBytes` wrong asks for a small page and receives up to 8 MiB; a caller
whose `waitMs` fails to parse loses its long poll and spins. Both now
answer 400.

A parseable-but-tiny `maxBytes` still clamps up to the 4 KiB floor — a
budget below one record cannot be honoured and every read must make
progress. That clamp is deliberate and documented, not a defect.

---

## What held up

Everything below was attacked and did not break.

**Durability under SIGKILL.** Four kills against one store (never reset
between rounds, so each round recovers from the previous crash state),
6–8 concurrent writers:

| round | acked | present | lost | duplicated | phantom |
|---|---|---|---|---|---|
| 0 | 1,144 | 1,144 | 0 | 0 | 0 |
| 1 | 1,848 | 1,848 | 0 | 0 | 0 |
| 2 | 1,650 | 1,650 | 0 | 0 | 0 |
| 3 | 1,965 | 1,965 | 0 | 0 | 0 |

6,607 acknowledged records, every one present exactly once, per-producer
FIFO intact. Ambiguous requests (connections killed mid-flight) landed 0
of ~5,000 per round; they were never acked, so either outcome is legal,
and none corrupted the log.

One note on method: the harness's first run reported "records out of
order" and that was the *harness* being wrong. With N concurrent writers
the stream carries arrival order, and nothing promises the writer holding
counter 100 reaches the server before the one holding 101. The real
guarantee is per-producer FIFO, so payloads now carry `(writer, seq)` and
each writer's subsequence is checked alone.

**Hostile HTTP surface** — 42/42 (`bench/chaos/hostile-surface.sh`):
rejected credentials never reach the data plane (401/403); path traversal,
encoded traversal, NUL and newline in names, 4000-byte names, empty
names all 400/404 with no 5xx; reserved system ledgers (`_usage`,
`_ops_metrics`) refuse create/append/read/delete with 403; TRACE and
PATCH 405; malformed creation documents 400 including unknown fields.

**Signed cursors.** A valid cursor replays. A one-character flip,
truncation, extension, and — the one that matters — **reuse against a
different stream** are all rejected with `invalid_cursor`.

**Seal.** Under six concurrent writers across three trials, seal is a
hard boundary: zero appends succeeded after the seal returned, zero
succeeded more than 0.5 s after the first refusal, no 5xx. Re-sealing is
idempotent, and reads still work on a sealed collection.

**Delete.** `DELETE` under load returned 204 with no 5xx; strictly after
it, appends and reads return 404. Recreating the name succeeds and reads
back empty — no resurrection of the prior incarnation. Concurrent
in-flight appends returning 200 during the delete window is the saga
racing legitimately, not a leak.

**Admission limiter.** The retryable/permanent split is correct: a 9 MiB
body gets `429 rate_limited` (retryable — under the 10 MB burst cap),
10 MiB and above get `413 body_too_large` (non-retryable).

---

## Field results

The campaign ran two Singapore builds against real Tigris storage.

| | pre-fix build | post-fix build |
|---|---|---|
| requests ok | 288,926 | 55,771 |
| errors | 298 | 53 |
| throttled | 16,917 | 111 |
| admission shed | 16,500 | 0 |
| RSS | 371–378 MB | 300–357 MB |
| absorb lag | 898–950 s | 183 s baseline |

**This is not a controlled A/B.** The post-fix build ran on a fresh store
with a fresh generator and less accumulated state, and for less wall
time. The shed and RSS differences are consistent with CHAOS-1 (a wedged
absorber grows the hot tier, which drives memory pressure into the shed
line) but the run does not isolate that cause. Treat the table as
directional.

Deliberate fault injections against the post-fix build
(`chaos-inject.sh`: stalled history flush with the reservation held,
absorber fully paused, then both together) drove absorb lag from 183 s to
353 s and RSS from 315 MB to 357 MB while health stayed 200 and shed
stayed 0 — the system degraded and kept serving rather than failing.

## An environment failure worth recording

Mid-campaign the host filled its disk (971 GB volume at 100%, ~500 MB
free). The first sampler died instantly and *silently*: it built each
sample with a Python here-document, and bash could not create the
here-doc temp file, so every sample was lost with only `cannot create
temp file` in a log nobody was reading. The load generator kept running,
so the run looked alive.

`chaos-sample.sh` now passes payloads as argv (no temp files) and checks
free space each tick, aborting loudly under 256 MB. 6.4 GB was reclaimed
from `target/debug/incremental`, which is pure rebuild cache. The volume
is still near-full from unrelated data (248 GB in `~/code`, 25 GB in
`~/Library/Caches`) — that is the operator's to triage, not this
campaign's.

## Harness added

- `bench/chaos/hostile-surface.sh` — 42 expected-status checks over the
  hostile HTTP surface. This class of defect is invisible unless
  something asserts the code, which is exactly how CHAOS-4 hid.
- `bench/chaos/kill9-consistency.py` — SIGKILL crash-consistency gate.
- `$SOAK_HOME/chaos-sample.sh`, `chaos-inject.sh` — field sampler and
  scheduled injection driver.

## Still open

- The post-fix Singapore build has not been run long enough for a
  like-for-like comparison against the pre-fix numbers. A controlled A/B
  (same store age, same generator run length) would settle the CHAOS-1
  attribution.
- Unknown query parameters are silently ignored on the product read
  surface, while the creation document rejects unknown fields
  (`deny_unknown_fields`). That inconsistency is a spec decision, not a
  bug, and is left for the surface owner.
- A body far above the ceiling (64 MiB) is answered with a connection
  reset rather than a 413, because the server stops reading. Clients see
  a transport error instead of a clear refusal.
