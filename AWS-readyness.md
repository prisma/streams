# AWS readiness: what we take from `slate-codex`, and the path from here

Status: 2026-07-21. This document is the disposition of the abandoned
`slate-codex` branch (48 commits, ~44k insertions, HEAD `0c992de`) and the
readiness plan for `slate`. It records what that branch attempted, what our
measurements proved, exactly which properties we carry forward into `slate`
now, what we defer, and the release gate every future change must pass.

## 1. Background and verdict

`slate-codex` was an attempt to raise Prisma Streams to the quality bar of an
AWS-operated managed service in one branch: multi-tenant authn/z (RS256/JWKS +
revocation), durable per-customer and per-stream quotas with hierarchical fair
scheduling, crash-safe online shard split/merge, multi-cell placement and
fenced cross-cell stream moves, checkpoint-pinned incremental backup with
epoch-isolated recovery content and provider-failover drills, synchronous
dual-destination audit plus billing export, OpenMetrics with a 20-alert
catalog, supply-chain gates, and machine-checked release judging. Its own
release gate (`AWS-QUALITY-GATE.md` on that branch) honestly concluded
**NOT READY**, with the performance gate **Red**.

Our final experiment made that Red concrete. In a single-instance A/B on
Prisma Compute FRA (identical env, per-branch buckets created via the
management API, the pilot generator pointed directly at each server, closed
loop conc=128, 32 streams, batch=16, ~3.9 KB/req):

| | `slate` (a402a3b) | `slate-codex` (0c992de) |
|---|---|---|
| steady throughput | 266 req/s median (osc. 131–986) | 3 req/s median |
| client errors (18 min) | 0 | 33,935 |
| outcome | stable, RSS ≤ 587 MB | **OOM-killed at ~735 MB anon RSS, unrecoverable crash loop** |

While alive, codex's request path was *faster* (p50 101 ms vs 111 ms; first
2.5 min at 900–1,050 req/s). It died of memory, not latency: the branch was
engineered against a written "1 GiB cell budget" (its non-negotiable scenario
5), but the platform's real kill line on this instance class is ~750 MB. Once
the first OOM landed, recovery was impossible: each replacement instance
replays 16 shards of accumulated WAL *under* full closed-loop pressure,
crosses the line in ~45 s, and dies; concurrent replacements fence each
other's shards and force repeated replays. 33.9k errors over 18 minutes with
no self-healing.

Decision (owner, 2026-07-21): **abandon `slate-codex`**. It tries to do too
much in one movement. `slate` remains the trunk; we harvest deliberately.

## 2. The two structural lessons

**Lesson 1 — memory is a correctness budget, not a tuning knob.** Every
always-on subsystem codex added ("bounded" queues, caches, accounting maps,
audit batches, capability heartbeats) was individually bounded and collectively
fatal. From now on every feature merged into `slate` must state its worst-case
resident footprint, and the *sum* of steady-state budgets must stay under
**450 MB** (60% of the 750 MB kill line), leaving 300 MB for load spikes,
shard-open WAL replay, and compaction bursts. The saturation benchmark (§5)
is the enforcement mechanism, and its pass condition includes surviving
restart-under-load — the exact scenario that killed codex.

**Lesson 2 — degrade, never die.** The platform's response to memory
overrun is SIGKILL, and the crash loop it triggers is strictly worse than any
brownout. Self-protection must fire *before* the platform does:
`ADMIT_RSS_SHED_MB` now defaults to 600 MB (it was configured at 800 — above
the kill line, i.e. unreachable) and sheds with scoped 429s while the process
is still healthy enough to serve reads and drain WAL.

## 3. What we carry into `slate` now

Chosen by three criteria: proven by test or by live fire on Compute; small
enough to audit; no new always-on memory of consequence. Each item lists its
origin on the codex branch.

### 3.1 Correctness / availability (implemented in this change set)

1. **Absorber lifecycle hardening** (from `0c992de`, the "absorption war"
   fix, live-verified on the warring keyspace: 223 fence-failures/15 s → 0).
   The history absorber now: (a) exits its task promptly when its shard
   engine is fenced/closed, draining pending-byte telemetry, instead of
   surviving as a zombie that retries forever; (b) classifies fence-class
   errors (`detected newer DB client` / `Fenced` / `Closed`) and *drops* the
   claim — the new owner accumulates its own signals; (c) applies exponential
   backoff with power-of-two log suppression to non-fence errors instead of
   hammering every tick.
2. **Fenced-engine prompt release** (from `3e28f6a`). A fenced shard fails
   in-flight and queued work with a retryable error and drops its engine
   (committer/flush tasks stop) instead of retaining it per ownership move.
   Zombie engines were both a memory hold and a fencing-war fuel source.
3. **Registry fail-closed** (from `3e28f6a`). A corrupt stream descriptor is
   an *error*, not "absent" — previously `.ok()` turned corruption into a
   recreate-over-live-stream hazard. Topology parsing never panics; a corrupt
   topology object keeps the last installed topology and surfaces an error.
4. **Recreate/create race discipline** (from `3e28f6a`). Delete/recreate uses
   expected-incarnation CAS (one winner; losers observe), and a create race
   re-checks the winner's key/config before caching.
5. **Long-poll ceiling 25 s** (from `0c992de`). The platform front door kills
   requests at ~30 s with a 502 (measured 30.16 s). All long-poll waits now
   conclude ≤ 25 s so clients see clean empty responses, not 502s.

### 3.2 Resource governance (implemented in this change set)

6. **Per-stream inflight cap** (`ADMIT_MAX_INFLIGHT_PER_STREAM`, default 64,
   bounded counter map). One hot stream can no longer occupy every admission
   slot of its shard owner. Scoped 429 + `Retry-After`. Per-*customer*
   admission is deferred with the identity layer (§4) — without verified
   tenant identity a customer cap is fiction.
7. **RSS shed recalibration** (`ADMIT_RSS_SHED_MB` default **600**, and the
   operating envelope documented in `RUNBOOK.md`): shed must be reachable
   below the platform kill line or it protects nothing.

### 3.3 Operability (implemented in this change set)

8. **Operator dashboard `/operator`** (from `52c5517`, adapted to `slate`'s
   direct heartbeat fan-in instead of codex's aggregator): fleet liveness,
   per-op store latency, admission/shed counters, absorber backlog, runbook
   digest. Unsecured by design (operational metadata only — no stream names,
   tenant identifiers, keys, or signed URLs).
9. **Supply-chain gate** (from `1858b6a`): `deny.toml` + `cargo deny check`
   wired into `scripts/release-gate.sh` next to fmt/clippy/tests. Unknown
   registries, wildcard requirements, unapproved licenses, yanked crates, and
   un-excepted advisories fail the gate.
10. **The saturation benchmark as a release test** (§5): scripts + thresholds
    committed; no change ships on regression.

### 3.4 Already present on `slate` (verified, no port needed)

- Per-op object-store instrumentation (`/v1/debug/store`), timer-starvation
  sentinels, `/v1/debug/timings`, `/v1/debug/load` (O14a work).
- Tokio worker floor of 2 on 1-vCPU instances (O14a root-cause fix).
- Instance-wide inflight admission with tarpitted 429s; RSS shed mechanism.
- Envelope crypto with per-stream keys; hash-first keyspace; fresh-incarnation
  storage isolation on recreate.

## 4. What we defer, and why

Deferred ≠ rejected. These are the codex subsystems we will want on the road
to an AWS-operable service, in rough order, each with the reason it does not
enter `slate` today and the memory-budget condition it must meet when it does.

1. **Tenant identity and authn/z** (JWKS/JWT, scoped tokens, revocation,
   verb/prefix authorization). Prerequisite for everything multi-tenant;
   deferred because the pilot is single-tenant and the codex implementation
   is entangled with its audit pipeline. Budget: verification caches ≤ 16 MB.
2. **Durable per-customer quotas + fair scheduling** (limit documents,
   ceil-shares, hierarchical committer turns). Deferred with identity; the
   accounting cardinality must be redesigned to a hard per-instance byte
   budget (≤ 32 MB) — on codex this layer is a prime OOM suspect.
3. **Backup / recovery points / provider failover.** The most valuable
   deferred block (its emulator drill measured RPO 8.751 s / RTO 519 ms) but
   also the largest: epoch leases, integrity cursors, format-3 content
   isolation. Must arrive as its own service/process, not more resident state
   in the serving instance.
4. **OpenMetrics + alert catalog.** Carry the *shape* (fixed-cardinality
   labels, runbook-linked alerts); implement against slate's telemetry.
   The 20-rule catalog on the codex branch is a direct reference.
5. **Online split/merge.** Codex's design (renewable intents co-located with
   shard data, post-durability fences, one-CAS topology publication, 6×
   split/merge trigger gap) is sound and its CI matrix is worth porting
   wholesale *when shard counts demand it*. Note its fence-GET-per-ACK
   mistake and the `4990f34` fix: liveness checks must never add an
   object-store round trip to the ACK path.
6. **Audit + billing pipelines.** Dual-destination synchronous audit is a
   compliance requirement eventually; deferred as an external sink design —
   codex's in-process INFO-log + batch buffers are part of the footprint
   problem.
7. **Multi-cell placement / cross-cell moves / release judges / 24 h soak
   harness.** Post-single-cell-GA machinery. The release-judge concept
   (immutable release ID binding all evidence) is good process DNA to keep.

## 5. The release gate (run on every substantive change)

The single-instance saturation benchmark that exposed the codex regression is
now the standing gate. Definition:

- **Topology**: one server instance (fresh service or fresh keyspace), one
  generator instance pointed *directly* at it (no LB, no router), same region;
  fresh data bucket provisioned via the management API
  (`POST https://api.prisma.io/v1/buckets {projectId,name}` → bucket +
  `POST /v1/buckets/{id}/keys {role:"read_write"}` → S3 creds).
- **Load**: closed loop, `STREAMS=32 CONC_START=128 CONC_MAX=128 BATCH=16`
  (~3.9 KB/request, 16 records/request), ≥ 16 minutes sampled at 20 s.
- **Instruments**: generator stats (`/` JSON: achievedPerSec, winP50/winP99,
  ok/errs/throttled) + server `/v1/debug/{store,timings,load}`.
- **Pass conditions** (vs the 2026-07-21 `slate` baseline in
  `bench/fra-ab-baseline.md`):
  - zero client errors; zero server restarts (any front-door 404/502 burst is
    a restart);
  - RSS max ≤ 620 MB (baseline 587 + margin), and never OOM;
  - steady median throughput ≥ 80% of baseline (baseline is compaction-noisy;
    131–986 req/s band) **and** p10 > 0 — no stall windows;
  - client winP50 ≤ 130 ms (baseline 111 ms + margin) at equal keyspace age;
  - one kill-under-load: restart the instance mid-run; the replacement must
    return to serving without an OOM or error storm (the codex failure mode).
- **Harness**: `scripts/bench-fra-ab.sh` (deploy arms + gen),
  `scripts/sample-fra-ab.sh` (sampler), `scripts/analyze-fra-ab.py`
  (steady-state table + equal-age buckets). These are the exact scripts used
  for the 2026-07-21 discriminating run.

## 6. Platform operating facts (hard-won; do not relearn)

- Prisma Compute instances are **x86_64**: build `x86_64-unknown-linux-musl`
  via `cargo zigbuild`; verify ELF `e_machine` = 62 before upload. Wrong-arch
  binaries crash-loop as silent zombies.
- The platform front door kills any request at ~30 s with 502 → all
  server-side waits must conclude ≤ 25 s.
- The kernel OOM kill line on the pilot instance class is ~750 MB RSS; the
  shed threshold must sit well below it (default 600 MB).
- Env vars are stored in a project-scoped pool and **snapshotted per version
  at deploy time**: every deploy must restate the service's complete env, and
  services in one project see each other's keys in their snapshot (a gen
  service inherited the servers' `SLATE_S3_*` and downloaded from the wrong
  bucket). Binary-download creds and data creds must use distinct var names
  (`BIN_S3_*` vs `SLATE_S3_*`).
- Download deploy binaries from S3 with instance credentials (chunked, with
  retries); presigned URLs broke under a platform Bun-canary rollout.
- Prisma Buckets (management API) are Tigris under the hood; a bucket created
  for a FRA project serves ~25 ms PUTs from FRA instances at idle. Under
  sustained load we observed put:wal p50 of 40–75 ms and p99 spikes to
  500–750 ms with put:sst p99 in seconds — size WAL durability expectations
  accordingly, and treat "25 ms idle" as the floor, not the plan.
- Object-store LIST is 300–900 ms; keep LIST off every hot path (codex's
  background audit scans showed this clearly).

## 7. Verification plan for this change set

1. `cargo fmt --check`, warning-free `cargo clippy --all-targets`, full test
   suite green (including new tests for fence-drop, backoff, engine-closed
   absorber exit, registry fail-closed parsing, recreate CAS race, per-stream
   admission, long-poll ceiling).
2. `cargo deny check` clean (or documented exceptions in `SECURITY.md`).
3. The §5 benchmark on Compute FRA against the recorded `slate` baseline —
   **no regression accepted**, plus the kill-under-load probe.
4. Results appended to this file's changelog (§8) with the run artifacts.

## 8. Changelog

- 2026-07-21: document created; baseline run recorded (slate a402a3b:
  266 req/s median, winP50 111 ms, 0 errors, RSS ≤ 587 MB; codex 0c992de:
  OOM crash loop). Carry-forward implementation begins.
- 2026-07-21 (evening): first gate run on the carry-forward build was RED —
  4 OOM kills at ~701–725 MB. Attribution: a faster evening substrate let
  the closed loop sustain 1.3–1.6k req/s (the morning baseline was paced to
  ~266 by slower put:wal), and slate's documented ~700 MB envelope + a
  flush-stall memtable pileup crossed the ~750 MB kill line — a
  PRE-EXISTING slate weakness the gate was designed to catch, not specific
  to the carried code. Two fixes: (1) the RSS-shed sampler ran only in
  fleet mode, leaving the shed dead in standalone (admit_shed=0 at every
  kill) — now unconditional; (2) the 1-GB envelope is retightened
  (RUNBOOK §3.3: cache 128 MiB, unflushed 8 MiB/shard, shed 550). The §5
  gate now runs as a PAIRED trial (old binary vs new binary, identical
  tightened env, fresh bucket each) whenever a red run needs attribution.
  The gate's speed-dependence is a feature: it must stay red until the
  envelope survives the substrate's fastest day.
- 2026-07-21 (late): the paired trial sharpened the diagnosis into two
  distinct failure modes with two distinct defenses. (1) OOM: the control
  (old binary, shed structurally dead) died once at saturation; the
  candidate's live shed at 550 held RSS ≤ 537 with zero restarts —
  degrade-don't-die works. (2) **Flush wedge**: halving `MAX_UNFLUSHED_BYTES`
  to 8 MiB doubled the L0 mint rate, compaction lagged, `L0_MAX_SSTS=24`
  engaged, and the flusher blocked — appends hung to the front-door kill
  for ~8 minutes before compaction caught up. Wrong lever: memtable caps
  trade OOM for wedge. Final envelope: unflushed 16 MiB, shared cache
  128 MiB, `L0_MAX_SSTS` 32 (L0 count costs S3 objects, not RAM), shed 550.
  Design note for the roadmap: **engine backpressure must surface as scoped
  429/503 rejection, not an unbounded append hang** — a wedged flusher
  currently strands in-flight appends until the platform kills them at
  30 s; admission should observe flush-pipeline health directly (§4 item
  alongside per-customer quotas).
