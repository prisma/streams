# Chaos campaign — 2026-08-09, Singapore (ap-southeast-1)

Adversarial session against `streams-slate`: break it any way possible,
fix what breaks, report. Load ran on Prisma Compute in Singapore against
Tigris; the attack surface work ran locally against the same binary on
the 1 GiB posture (`deploy/profiles/compute-1g.env`).

**Four defects found and fixed, two of them P0**, plus a fifth finding —
a capacity limit whose root cause is identified but whose obvious remedy
was disproved in the field by an OOM kill, so it stays open.

Both P0s were silent: the system reported itself healthy while being
permanently broken. No data-loss defect was found — durability held
under every attack, including four SIGKILLs.

| # | Severity | Defect | Commit |
|---|---|---|---|
| CHAOS-1 | P0 | Absorber wedge: a truncated gather stranded data in the hot tier forever | `506039dd` |
| CHAOS-2 | P0 | Invalid engine config accepted — and the shipped defaults *were* invalid; `/health` said `ok` while every append 500'd | `3e701c34` |
| CHAOS-3 | P1 | 96.2 MiB of the 500 MB shed line reserved per gather for a record size the deployment may never accept | `c00b19ae` |
| CHAOS-4 | P2 | `maxBytes` / `waitMs` silently defaulted instead of rejecting values they could not parse | `204ac19c` |
| CHAOS-5 | P1 | Absorption runs ~2.3× *below* ingest, so the hot tier grows without bound. Root cause is gather concurrency — but raising it to 4 OOM-killed the instance (exit 137), so **no safe fix is known yet** | open |

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
the release binary with only store settings (no profile), where the
budget defaults apply:

```
MAX_REQUEST_BODY_BYTES=33554432   worstFrame=96.2MiB capacity=96.2MiB gatherConc=1
MAX_REQUEST_BODY_BYTES=1048576    worstFrame= 3.2MiB capacity=64.0MiB gatherConc=2
```

and in Singapore, under the actual 1 GiB profile:

```
MAX_REQUEST_BODY_BYTES=1048576    worstFrame= 3.2MiB capacity=96.2MiB
                                  gatherSlots=1  effectiveConc=1
```

**The concurrency win does not transfer to the profile, and saying it
did would be wrong.** The 1 → 2 above comes from the bare defaults
(64 MiB budget, 2 slots). `compute-1g.env` pins
`ABSORB_GLOBAL_GATHERS=1`, so effective concurrency stays 1 whatever the
frame size — it is now clamped by the configured slot count rather than
by the worst frame.

What the knob delivers under the profile is the reservation itself, and
the number is 24 MiB rather than 3.2 MiB: a gather reserves
`max(gather_cap × build_multiplier, worst_frame)`, so once the worst
frame drops below it the profile's 8 MiB gather cap × 3 becomes the
binding term. Measured `reservedBytes` in Singapore confirms it —
25,165,824 bytes exactly.

So the shed line gets **72 MiB** back (96.2 → 24 MiB), not 93. Raising
concurrency is a separate decision that this makes possible for the
first time: at 24 MiB per gather the 96.2 MiB budget admits four, where
before it admitted exactly one. See CHAOS-5, where that turns out to
matter a great deal.

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

## CHAOS-5 — absorption cannot keep up with ingest at this load

Not a code defect: a capacity limit, measured because the injection
campaign made it visible. After a deliberate three-minute absorber
pause, the backlog never recovered. Three samples 30 s apart on the
post-fix Singapore build under the standard chaos load:

```
absorbed  240,022,292 -> 244,216,816 -> 248,411,340   (~140 KB/s)
ingest    340,174,236 -> 349,780,412 -> 359,709,266   (~325 KB/s)
backlog   100,151,944 -> 105,563,596 -> 111,297,926   (+5.5 MB / 30 s)
```

Ingest is running at roughly **2.3× the absorption rate**, so the hot
tier grows without bound and `absorb_lag_max_secs` climbs monotonically
(183 s at baseline, 717 s by the end of the injection sequence). Each
gather moved ~4 MiB per pass with `lastReadMs` of 21–28 s: the read
phase dominates, and with one gather in flight that caps absorption near
140 KB/s.

This is the same shape as CHAOS-1 in its effect (hot tier grows, storage
cost climbs, restart replay lengthens) but a different cause: CHAOS-1
was a correctness bug that stranded data permanently, this is throughput.

**The obvious hypothesis was tested and refuted.** I expected the
CHAOS-3 reservation to be throttling absorption, so Singapore was
redeployed with `MAX_REQUEST_BODY_BYTES=1048576`. The reservation did
drop as designed — 96.2 MiB → 24 MiB per gather, since the reservation
is `max(gather_cap × build_multiplier, worst_frame)` and the profile's
8 MiB gather cap now dominates — but absorption throughput did not move:

```
                    reservation   absorption   ingest    ratio
32 MiB ceiling        96.2 MiB     ~140 KB/s   ~325 KB/s  2.3x
 1 MiB ceiling          24 MiB     ~132 KB/s   ~298 KB/s  2.3x
```

Same deficit, measured at the same 128-concurrency load. The reservation
costs real shed-line headroom, and reclaiming 72 MiB of it is worth
doing, but it is **not** the absorption bottleneck. The remaining
suspects are the read phase (`lastReadMs` 21–42 s, which dominates every
gather cycle) and the single gather slot.

What CHAOS-3 does buy here is that the concurrency experiment becomes
possible at all: at a 96.2 MiB reservation against a 96.2 MiB budget
only one gather could ever run, so raising `ABSORB_GLOBAL_GATHERS` was
futile. At 24 MiB the same budget admits four.

No data is at risk: everything is durable in the shard log the whole
time, and reads serve correctly from it. The exposure is cost and
recovery time.

### Root cause: gather concurrency — but 4 slots OOMs the instance

Redeployed Singapore with `MAX_REQUEST_BODY_BYTES=1048576` **and**
`ABSORB_GLOBAL_GATHERS=4`, which reports `effectiveConc=4` — a value the
old build could not reach at any setting. Measured at the same
128-concurrency load:

| slots | gathers in flight | absorption | ingest | verdict |
|---|---|---|---|---|
| 1 | 1 | ~136 KB/s | ~310 KB/s | backlog grows 5.5 MB / 30 s |
| 4 | 2–4 | **~660 KB/s** | ~346 KB/s | backlog drains — **then the process died** |

Absorption did improve ~4.9× and the regime did invert from growth to
draining. Then, roughly fifteen minutes in, the instance was **killed by
the OOM killer**:

```
GET /health -> 500
{"error":"binary_exited","binary":"/tmp/streams-slate","exitCode":137}
```

Exit 137 is SIGKILL from the kernel. The 4-slot configuration is
**not survivable on the 1 GiB posture** and must not be recommended.
The throughput numbers above are real — the system genuinely absorbed at
660 KB/s — but it bought that rate with memory it did not have.

This is the sharpest lesson of the campaign, and it is a lesson about
the reservation model, not just about a knob. Four gathers reserve
4 × 24 MiB = 96 MiB, which the admission shed accounts for exactly; RSS
was last observed at 362 MB, well inside the 500 MB line. The process
still died. So the **reservation materially understates the real
transient cost of a concurrent gather** — the accounting that made
`ADMIT_RSS_SHED_MB` trustworthy at one gather does not hold at four.
Until that model is re-derived and re-validated, raising
`ABSORB_GLOBAL_GATHERS` on a 1 GiB instance trades a bounded,
observable backlog for an unbounded, fatal one.

What stands:

- The absorption deficit at 1 slot is real and reproduced three times
  independently (~126–140 KB/s absorbed against ~238–325 KB/s ingested).
- Gather concurrency, not the reservation size, is what gates absorption
  throughput.
- CHAOS-3's knob is what makes concurrency reachable at all — but
  reachable is not the same as safe.
- **The fix for CHAOS-5 is not yet known.** Raising concurrency is
  disqualified on this instance class until the transient-cost model is
  corrected. The remaining candidates are the read path (`lastReadMs`
  21–42 s dominates every cycle), a larger instance, or absorbing on
  dedicated capacity rather than in the serving process.

`deploy/profiles/compute-1g.env` is unchanged, and after this run the
region was redeployed back to the single-slot configuration.

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

**Hostile HTTP surface** — 42/42 locally
(`bench/chaos/hostile-surface.sh`), and 35/42 against Singapore where
the five remaining failures are exactly the CHAOS-4 checks, because the
deployed binary predates that fix. The gate detecting a real defect on a
build that lacks the fix is the demonstration that it works.

The first Singapore run reported **19** failures, all bogus: setup drew
a single 429 under full generator load, so the victim stream was never
created and every dependent check hit 404. Setup is now a precondition
that retries retryable codes and aborts the whole run with a distinct
exit status rather than reporting a cascade. A gate that cries wolf once
is a gate nobody reads.

What it checks: rejected credentials never reach the data plane
(401/403); path traversal, encoded traversal, NUL and newline in names,
4000-byte names and empty names all answer 400/404 with no 5xx; reserved
system ledgers (`_usage`, `_ops_metrics`) refuse create/append/read/
delete with 403; TRACE and PATCH 405; malformed creation documents 400,
including unknown fields.

**Signed cursors.** A valid cursor replays. A one-character flip,
truncation, extension, and — the one that matters — **reuse against a
different stream** are all rejected with `invalid_cursor`.

**Seal.** Under six concurrent writers across three trials, seal is a
hard boundary: zero appends succeeded after the seal returned, zero
succeeded more than 0.5 s after the first refusal, no 5xx. Re-sealing is
idempotent, and reads still work on a sealed collection.

**Create/delete contention.** Twelve threads racing create, delete,
append and read against ONE name for 25 s: 15,726 creates, 465 deletes,
8,450 appends, 14,935 reads — zero 5xx, zero transport failures, and
every response a coherent 200/201/204/404. Afterwards the name is fully
usable: create 200, append 200, and a read returns exactly the post-race
record. (`PUT` is an idempotent upsert — 201 when new, 200 when it
already exists — which is why the race shows 15,726 200s next to 155
201s.)

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
| continuous window | 67 min (247 samples) | ~7 min (41 samples) |
| requests ok | 1,241 → 355,306 | 175 → 31,909 |
| errors | 298 | 87 |
| throttled | 22,895 | — |
| admission shed | 22,065 | 164 |
| RSS | 46 → 426 MB, **peak 596 MB** | 103 → 348 MB |
| absorb lag | 0 → 569 s, peak 973 s | 63 → 514 s |
| absorbed / ingested | 254 / 300 KB/s | 126 / 238 KB/s |
| final backlog | 195.7 MB | growing |

Two things in that table deserve to be called out rather than skimmed.

**RSS peaked at 596 MB against a 500 MB shed line.** The line is not a
ceiling on resident memory — it is the point at which admission starts
shedding, and the process still climbed ~96 MB past it without dying.
That is the same accounting looseness the 4-slot OOM later exposed, and
it was visible here an hour earlier if anyone had been reading.

**The absorption deficit is not new to the fixed build.** Pre-fix
absorbed 254 KB/s against 300 KB/s ingested and finished the window
195.7 MB behind. CHAOS-5 predates every change in this campaign; the
fixes did not cause it and did not cure it.

**This is still not a controlled A/B.** The two windows differ in length
(67 min vs 7 min), store age, and generator run — the post-fix column is
short because a redeploy retired the domain its sampler was pinned to.
Nothing here isolates the effect of any single fix. Treat the table as
two independent observations, not a comparison.

Deliberate fault injections against the post-fix build
(`chaos-inject.sh`). A 2.5 s stalled history flush held for three
minutes drove absorb lag 183 s → 321 s with RSS bounded at 312–333 MB,
health 200 and **zero** sheds throughout. A full three-minute absorber
pause then advanced lag by exactly +30 s per 30 s of wall clock — zero
absorption, as designed — while RSS stayed within 320–364 MB, confirming
the paused backlog accumulates in the shard log rather than in memory.

The final combined-injection phase of that run is **invalid data**: I
redeployed the server mid-campaign to test CHAOS-3, which retired the
domain the injector was sampling, so its last eight samples read
`0 0 0`. The stall and pause phases completed before the redeploy and
stand.

## An environment failure worth recording

Mid-campaign the host filled its disk (971 GB volume at 100%, ~500 MB
free). The first sampler built each sample with a Python here-document,
and bash could not create the here-doc temp file, so those samples were
lost with only `cannot create temp file` in a log nobody was reading.
The load generator kept running, so the run looked alive.

**Correction to an earlier draft of this document, which claimed every
sample was lost.** They were not: the run wrote 2,871 samples and lost
9 to ENOSPC. I inferred total loss from the three error lines at the
head of the output file and did not check the JSONL until the task
finally exited. The mistake mattered, because those samples turned out
to hold the campaign's only continuous pre-fix dataset (below).

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

- **CHAOS-5, and the reservation model underneath it.** Absorption runs
  ~2.3× below ingest at one gather slot; raising the slot count fixes
  the throughput and kills the process. Before anything is changed here,
  the per-gather transient-cost model needs re-deriving — two
  independent observations now show `ADMIT_RSS_SHED_MB` is not a ceiling
  on RSS (596 MB peak against a 500 MB line pre-fix; an OOM kill at
  362 MB observed with four gathers reserved).
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
