# Staging deployment plan — Prisma Streams on Compute + Prisma Buckets

Status: plan, not yet executed. Target: a long-lived, production-shaped
staging cell that internal and invited external testers can use for real
workloads.

## 1. What staging is for (and what it is not)

**Purpose.** Everything to date has been *measured* — benchmarks, ladders,
simulations — with load we generated ourselves against keyspaces we threw
away. Staging exists to expose the system to the thing we cannot
synthesise: **other people's workloads, sustained over weeks**, with data
they care about enough to complain when it misbehaves.

Specifically it must answer:

1. Do the per-stream limits and the auto-scaler behave sensibly against
   traffic shapes we did not design for?
2. Does the operational surface (dashboard, alarms, runbook) let someone
   who is not the author diagnose an incident?
3. What breaks when a keyspace lives for weeks instead of an afternoon —
   storage growth, segment-map growth, GC, cost drift?
4. Is the client contract (SDK, retries, 429s, producer idempotence)
   usable by someone reading only the docs?

**Non-goals.** Staging is not GA. No uptime commitment, no data-durability
guarantee beyond best effort (see §7), no on-call rotation. Testers are
told, in writing, that the keyspace may be reset.

## 2. Topology

One **cell**: a self-contained fleet + bucket set in one region. Cells are
the isolation and blast-radius unit (COMPUTE-SPEC §2).

| component | staging shape | rationale |
|---|---|---|
| region | `ap-southeast-1` (SIN) | our warmest measurement base; the sinmax rig, single-region max-out and P2C validation all ran here |
| server instances | 4 × 1 CPU / 1 GB, `FLEET_MIN=2`, `FLEET_MAX=6` | 4 is the validated cluster size (docs/SCALING.md §10); MIN=2 keeps an HA floor and avoids cold-start routing (§5); MAX=6 caps cost |
| router tier | 2 × `pilot MODE=lb` | **required**, not optional — see §5 |
| shards | `INITIAL_SHARDS=4                   # survival posture (OOM review); fresh namespace required to change` | matches the validated ladder/cluster topology; power of two, set at keyspace creation and immutable |
| prefixes | `PATH_PREFIX=stg1`, `FLEET_PREFIX=stg1-fleet` | a fresh prefix is how we get a clean environment; keep the name versioned so `stg2` can exist alongside |

Client entry point is the **LB domain only**. Server instance domains are
internal; publishing them would leak topology into clients and re-create
the routing problem §5 describes.

**Region choice, revisited after the six-region soak**
([docs/SOAK-REGIONS.md](./SOAK-REGIONS.md)). SIN remains the right pick —
it is where every prior result was measured, and re-measuring staging
against a new baseline would waste the comparison. But the soak showed it
is not the fastest region and not the most local: Tigris served 87 % of
SIN's object-store requests from `sin` and routed 8 % to `nrt` and 5 % to
`fra`. `ap-northeast-1` was 100 % local and had the lowest `put:wal`
latency of the six.

Two consequences for staging:

- Do not read staging latency as the platform's best case. NRT is faster,
  and `us-east-1` is far worse (its `put:wal` p50 is 4–6× SIN's).
- Watch `served_from` on the staging cell. The one region that wedged
  during the soak was the one with heavy out-of-region routing, and the
  failure mode it produced — compaction stall into a WAL read storm —
  is now alarmed (§8).

## 3. Prisma Buckets

Buckets are provisioned through the management API — the same flow
`scripts/bench-fra-ab.sh provision` already uses:

```bash
# bucket
curl -sf -X POST -H "Authorization: Bearer $PRISMA_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"projectId":"'"$STAGING_PROJECT"'","name":"streams-stg1-data"}' \
  https://api.prisma.io/v1/buckets

# read_write key -> {endpoint, bucketName, accessKeyId, secretAccessKey}
curl -sf -X POST -H "Authorization: Bearer $PRISMA_API_TOKEN" \
  -H "Content-Type: application/json" -d '{"role":"read_write","name":"stg1"}' \
  https://api.prisma.io/v1/buckets/$BUCKET_ID/keys
```

**Three buckets, not one.** The server already supports splitting roles
(`--ops-bucket` / `--shard-bucket` / `--data-bucket`):

| bucket | holds | why separate |
|---|---|---|
| `streams-stg1-shard` | shard logs (`shards/<id>/wal|manifest|compacted`) | the hot, ack-critical path; keeping GC listings off the same prefix as history matters (RUNBOOK §3.2) |
| `streams-stg1-data` | history tier + registry | large, cold, read-heavy; different growth curve |
| `streams-stg1-ops` | `fleet/`, `routers/`, topology | tiny, high-frequency metadata; isolating it keeps heartbeat listings cheap |

Binaries live in a **fourth, separate bucket** (`streams-artifacts`) that
is *not* per-cell — it outlives cells and is shared across staging and
bench. Distinct env names per role (`BIN_S3_*` vs `SLATE_S3_*`) — the
env-merge trap in RUNBOOK §7.3 has already bitten us twice.

**Key custody.** Bucket keys are `read_write` and grant full access to
tenant ciphertext. They are never committed. For staging they live in the
platform's env store (set per deploy from a canonical script) plus a
password manager entry for humans. Rotation: create a new key, redeploy,
delete the old — no server restart ordering constraint, since keys are
read at boot only.

## 4. Configuration — the canonical staging environment

Production values, **not** the ladder-pace knobs. The ladder used
`SCALE_RATE_WINDOW_SECS=60 / COOLDOWN=60 / COLD_EVALS=12` to fit test
timeboxes; staging uses the real defaults so we observe real damping.

```
# --- identity / placement
INSTANCE_NAME=streams-<n>          # ordinal; the ring's identity contract
PATH_PREFIX=stg1
FLEET_PREFIX=stg1-fleet
INITIAL_SHARDS=8
FLEET_MIN=2
FLEET_MAX=6

# --- buckets (three roles + artifacts)
SLATE_S3_ENDPOINT / _BUCKET / _REGION=auto / _ACCESS_KEY_ID / _SECRET_ACCESS_KEY
BIN_S3_*                            # artifacts bucket, distinct names
SERVER_BINARY_S3_KEY=bin/streams-stg1-x64

# --- auth
AUTH_TOKEN=<staging bearer>         # gates the API
USAGE_STREAM_KEY=<32B base64url>    # `_usage`/`_ops_*` system-ledger key
BILLING_MODE=required               # staging runs the production gate
ACCOUNT_ID=<acct_...>               # real identities; placeholders refused
PROJECT_ID=<proj_...>
CELL_ID=<cell tag>
ROLLUP=1                            # exactly one instance per cell

# --- memory survival posture (OOM review, 2026-08-07)
# Each knob appears exactly ONCE in this file (env application is
# last-one-wins; a stale duplicate silently restores the pre-review
# posture). The values live in their sections: INITIAL_SHARDS=4 under
# placement; SLATEDB_RT_THREADS=4, ADMIT_RSS_SHED_MB=500 and the
# explicit cache bounds under engine/admission below.
ABSORB_GATHER_MAX_BYTES=8388608     # per-gather packing cap (8 MiB)
ABSORB_GLOBAL_BUDGET_BYTES=67108864 # PROCESS-WIDE gather budget (64 MiB)
ABSORB_GLOBAL_GATHERS=2             # concurrent gathers, process-wide
TELEMETRY_CACHE_BYTES=16777216      # spool+rollup DBs share ONE bounded cache
# ROLLUP placement: on multi-instance cells, ROLLUP=1 goes on a
# designated NON-INGEST instance (it must not compete with history
# compaction on an ingestion VM); a single-instance cell accepts the
# co-location with the bounded caches + RT_THREADS=4 below.

# --- engine (1 GB discipline, RUNBOOK 3.2/3.3)
FLUSH_INTERVAL_MS=25
WAL_GROUP_COMMIT=1
WAL_FLUSH_GAP_MS=10
FRAME_COMPRESS=1                    # removed a ~5-6x NIC amplification on sinmax
L0_SST_SIZE_BYTES=16777216
MAX_UNFLUSHED_BYTES=33554432
L0_MAX_SSTS=64
MANIFEST_POLL_MS=1000
COMPACTOR_POLL_MS=500
COMPACTOR_MAX_CONCURRENT=2
SHARED_CACHE_BYTES=67108864
HISTORY_CACHE_BYTES=33554432        # all cache bounds explicit (OOM review)
POSTINGS_CACHE_BYTES=67108864
SLATEDB_RT_THREADS=4                # OOM review: telemetry flushers must not starve history compaction
ABSORB_BYTES=4194304
ABSORB_AGE_SECS=60
ABSORB_PASS_BYTES=67108864
TRIM_PER_OP=65536
TRIM_GLOBAL_BUDGET=65536            # global per-commit trim-delete cap; TRIM_PER_OP is per-stream within it

# --- admission (ON in production, RUNBOOK 3.6)
ADMIT_MAX_INFLIGHT=512
ADMIT_MAX_INFLIGHT_PER_STREAM=256
ADMIT_RSS_SHED_MB=500               # OOM review: 600 was too close to the ~750 kill line for inter-sample SST spikes; shed = RSS + reserved absorber bytes

# --- limits (per stream segment)
LIMIT_BYTES_PER_SEC=5000000
LIMIT_REQS_PER_SEC=1000
LIMIT_RECS_PER_SEC=5000
LIMIT_BURST_SECS=2

# --- autoscaling: PRODUCTION defaults
SCALE_EVAL_SECS=10
SCALE_RATE_WINDOW_SECS=120
SCALE_HOT_PCT=75
SCALE_COLD_PCT=15
SCALE_HOT_EVALS=2
SCALE_COLD_EVALS=180
SCALE_COOLDOWN_SECS=600
MAX_SEGMENTS_PER_STREAM=64
REBALANCE_LAG_SECS=60
REBALANCE_MOVE_COOLDOWN_SECS=60
REBALANCE_RETURN_SECS=300

# --- fleet scaling
SCALE_OUT_CPU_PCT=75
SCALE_IN_CPU_PCT=50
SCALE_CPU_SUSTAIN_SECS=20
SCALE_LATENCY_MS=250
SCALE_EDGE_SLOTS=140                # recalibrate post-Conduit-fix toward ~250
SCALE_EDGE_LATENCY_MS=1000
SCALE_IN_SECS=60

# --- NOT set in staging
# KEEP_AWAKE      : leave unset so idle instances sleep and stop billing
# SCALE_FAULT_POINT / ABSORB_PAUSE : test-only hooks, must never be set
```

`KEEP_AWAKE` deserves emphasis: it bills continuously. It goes on only for
the duration of a soak and comes off immediately after (RUNBOOK §7.1).

## 5. Blockers — must be closed before any tester touches this

| # | blocker | why it blocks | size |
|---|---|---|---|
| **B1** | **`deploy/` wrapper app is missing from the repo.** `scripts/bench-fra-ab.sh` references `deploy/app-server`, which does not exist; the wrapper has only ever lived in scratch dirs — and one such copy was deleted mid-campaign, which would have failed the Compute deploy at the worst moment | deploys are not reproducible from a clean checkout | S |
| **B2** | **Router tier must be deployed and be the only client entry point.** Compute sleeps idle instances and answers **404 while one wakes**; routing must never be the wake mechanism (RUNBOOK §6). A tester pointed at a server domain will see spurious 404s and drop writes | user-visible data loss on a cold cell | S |
| **B3** | **Ring-convergence gate must be in the deploy script.** Measured: load applied to a still-forming ring lost **371,900 acknowledged records**; the same load on a stable ring was clean (docs/SCALING.md §10). Gate exists only in the ladder harness today | data loss on every deploy/scale event | S |
| **B4** | **`PUT /v1/stream/{name}` answers `409 not_ring_owner` instead of following `Streams-Replay-To`.** The stream *is* created (descriptor written before the ownership check), so it is cosmetic for us but actively confusing for a tester reading a 409 | first thing every new user hits | S |
| **B5** | **Secrets handling.** Tokens/keys currently live in a local scratch directory. Staging has multiple operators and multiple testers | credential sprawl; no rotation story | M |
| **B6** | **Decide and document the durability posture.** Backup/PITR is designed (OPERATIONS.md §2) but **not wired**. Either wire the minimal checkpoint-copy, or state plainly that staging data is re-creatable | testers must not be surprised | M (decide), L (wire) |

Recommendation on B6: **do not wire backup for staging.** State the
posture in the tester agreement instead. Wiring PITR is a production
workstream and doing it badly is worse than not having it; a documented
"this may be reset" is honest and cheap. Revisit before GA.

## 6. Known limitations to disclose to testers

Not blockers, but they shape what testers should expect. All are
documented in docs/SCALING.md §9 "Known v1 limitations":

- **Producer sessions do not survive a segment split.** After a split the
  fresh child expects producer seq 0, so an SDK must resync on
  `producer_seq_gap`. A batch whose outcome was ambiguous *exactly* at the
  seal boundary can commit twice. Ladder pass 2b demonstrated the
  underlying at-least-once shape.
- **Cross-instance merges never happen.** Merging seals both parents and
  seals run through the local engine, so an adjacent cold pair split
  across two instances stays split. Correct, just not compacted.
- **The segment map only grows.** ~200 B per transition; `prune()` exists
  but its trigger is retention semantics. At production cooldowns this is
  ~KB/day — fine for staging, revisit with retention.
- **Time-to-scale.** 1 MB/s → 1 GB/s takes ~1.5–2 h under stock cooldowns
  (doubling staircase). Demand-proportional splitting (docs/SCALING.md
  §11.1) would make it ~10–20 min; not implemented.

## 7. Data lifecycle in staging

- **Retention.** Stream TTL exists in the registry (`ttl_secs`,
  `expires_at_ms`). Staging sets a **default 14-day TTL** on tester
  streams so the keyspace cannot grow without bound and so we exercise
  expiry — which we have never run at length.
- **GC.** WAL objects reaped per `WAL_GC_*`; history SSTs retired by
  compaction. Watch bucket size weekly (§8); unbounded growth is the
  cheapest early signal that something is wrong.
- **Backup.** None (see B6). The tester agreement says so.
- **Reset procedure.** A new `PATH_PREFIX` is a new universe: instant,
  cheap, and how every pilot run isolated itself. Keep `stg1` → `stg2`
  as the reset path rather than deleting objects under a live prefix.

## 8. Observability and alarms

**Feeds already available:** `/operator` dashboard (unsecured by design,
operational metadata only — never stream names, tenants, tokens, keys),
`/operator/data.json`, `/v1/debug/{load,store,usage,timings,scaler}`, LB
`/stats`, and fleet heartbeats.

**Alarms to wire before testers** (thresholds from RUNBOOK §8 baselines
and OPERATIONS.md §5 SLOs):

| alarm | condition | why |
|---|---|---|
| append availability | non-429 5xx > 0.05 % over 5 min | the core SLO |
| durable-ack p99 | > 250 ms sustained 15 min | also the scale-out trigger |
| RSS | any instance > 650 MB | kill line ~750; shed starts at 600 |
| `timer_tokio` p99 | > 100 ms | the O14a blocking-work regression alarm — this is how we caught the SlateDB encode stall |
| absorber lag | > 60 s sustained | the rebalance signal; sustained means the fleet cannot keep up |
| fence events | rate ≫ shard-move rate | routing flap |
| bucket size | week-over-week growth > forecast | GC/retention not working |
| cost | daily spend > budget | `KEEP_AWAKE` left on is the classic cause |
| **WAL read storm** | `/v1/debug/store` → `wal_read_storm.stalled` true | compaction stopped, WAL never trimmed, readers scanning it directly; starves appends until throughput hits zero while writes still land after the client's timeout. Took a region out of the six-region soak ([SOAK-REGIONS.md](./SOAK-REGIONS.md)) |
| out-of-region storage | `served_from` non-local share > 10 % over 15 min | the provider is routing this bucket out of region; every op pays the extra RTT and it correlated with the only wedge we have seen |

**Daily human check** for the first two weeks: dashboard, bucket sizes,
error-code histogram, and the observatory pages for the region (the iad1
excursion we reported to Tigris is exactly the kind of upstream drift that
matters).

## 9. Deploy procedure

Scripted, in-repo, one canonical script per role — never an incremental
deploy (RUNBOOK §7.3):

1. **Gate.** `scripts/release-gate.sh` (fmt, clippy-vs-baseline, unit
   suite incl. the 9 DST scenarios, `cargo deny`), then the single-instance
   saturation benchmark (AWS-readyness.md §5). A red run is a hard stop.
2. **Build + verify.** `cargo zigbuild --release --target
   x86_64-unknown-linux-musl`; assert `e_machine == 0x3e` before upload —
   an aarch64 binary deploys "successfully" and crash-loops into a silent
   zombie.
3. **Upload** to the artifacts bucket; record the git rev alongside so the
   deployed artifact is always attributable (we caught a stale-binary
   mismatch this way during the cluster work).
4. **Roll servers one at a time**, full env restated from the canonical
   script, health-gating each instance (poll `/health` up to ~2 min for
   wake + boot + shard reopen) before the next.
5. **Wait for the ring** — all N live and `ring_active` unchanged for 60 s
   — *before* the LBs are allowed to route (B3).
6. **Roll LBs** the same way.
7. **Smoke:** create → append → read → tail → segments on a scratch
   stream; confirm `/operator` shows N live instances.
8. **Post-deploy watch:** first minute of `/v1/debug/load` per instance;
   a redeploy under live load zombies an instance roughly once per ~20
   deploys, and the heal is another deploy.

## 10. Phased rollout

| phase | duration | content | exit gate |
|---|---|---|---|
| **0 — pre-flight** | ~1 week | close B1–B5; decide B6; wire alarms (§8); write the tester quickstart | release gate green; alarms firing on synthetic breach |
| **1 — empty cell** | 3–5 days | deploy; run the validation battery: 24 h soak at moderate load, a chaos pass (rolling restarts), a scale-out/in cycle, a deliberate rebalance, and a deploy-under-load | zero unexplained 5xx; RSS in envelope; ring stable across a full deploy cycle; every alarm exercised at least once |
| **2 — internal dogfood** | 1–2 weeks | 2–3 internal workloads, ideally including one we did not design for (e.g. the observatory's own PG logging, or a CI event stream) | a non-author diagnoses one injected incident using only dashboard + runbook |
| **3 — invited testers** | 3–4 weeks | 5–10 external streams with per-tenant tokens, published limits and quickstart; weekly review of error histograms and cost | no data-integrity incident; support burden tractable; ≥1 tester integrates from docs alone without help |
| **4 — review** | — | decide GA workstreams from what staging actually surfaced | §12 |

Load in phase 1 should be *shaped like the unknown*, not like our
benchmarks: variable record sizes, bursty arrival, idle streams that wake,
long-lived consumers, and at least one workload that deliberately exceeds
its limits so we see 429 behaviour end to end.

## 11. Rollback and incident handling

- **Bad release:** redeploy the previous artifact (kept in the artifacts
  bucket by git rev). Servers are stateless; the object store is the only
  durable tier, and the format is unchanged across these releases.
- **Bad cell:** roll `PATH_PREFIX` to `stg2` and redeploy. Instant clean
  environment; old prefix retained for forensics.
- **Instance zombie** (platform 404, `versions list` says running, no
  logs): redeploy that service. Known platform mode, ~1 per 20 deploys.
- **Runaway cost:** the usual cause is `KEEP_AWAKE` left set; unset and
  redeploy, and the fleet sleeps.
- **Data-integrity suspicion:** freeze the prefix (stop routing), snapshot
  bucket listings, and reproduce with the DST harness before touching
  anything — this campaign's one data-loss scare was diagnosed by
  disproving the innocent explanations first, and that discipline is worth
  keeping.

## 12. Exit criteria — what staging must show to justify GA work

1. **Four consecutive weeks** with no data-integrity incident (no
   acknowledged record lost, no ordering violation, no undetected
   duplicate) across all tester traffic.
2. **SLOs met** on real traffic: append availability ≥ 99.95 %, durable-ack
   p99 < 250 ms, tail freshness p99 < 500 ms.
3. **Operability proven:** at least two incidents diagnosed from the
   dashboard and runbook by someone who did not write the system.
4. **Cost model validated** against the §6 unit-economics estimate within
   a factor the business accepts.
5. **A prioritised GA backlog** derived from what staging surfaced —
   expected candidates today: backup/PITR (B6), demand-proportional
   splitting, producer-session continuity across splits, and request
   forwarding so SDKs stop carrying topology.

## 13. Open questions for the team

- **Tenancy model for staging.** One bearer token per tester, or a real
  per-tenant identity? OPERATIONS.md §3 designs the latter; staging could
  ship the former and defer. This decides how much of §3 lands now.
- **Region.** SIN is our best-measured region, but if testers are
  EU-centric, FRA has the second-best coverage — and iad1 is currently
  showing 7–17× peer-region write latency, so US-East should be avoided
  until Tigris resolves it.
- **Does staging share the observatory's project** or get its own? Its own
  is cleaner for cost attribution and avoids the env-merge trap.
