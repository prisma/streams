# Stream auto-scaling: Pravega-style segment splits, merges, and host rebalancing

Status: design accepted for implementation (task #53). Validation ladder:
unit tests → 1 GB docker containers (s3lite + real fleet of N server
containers) → 4-instance Prisma Compute cluster. Nothing ships to Compute
until the docker ladder is fully green.

## 1. Model

Terminology maps Pravega concepts onto what the codebase already has:

| Pravega | here | exists today? |
|---|---|---|
| Stream | stream (registry `StreamDesc`) | yes |
| Segment | **segment**: a contiguous routing-key hash range `[lo, hi)` of one stream | partially (`seg_ord` per-key segments are a fixed power-of-two split) |
| Segment store | shard (one SlateDB log); a segment is pinned to exactly one shard | yes (hash-first keyspace) |
| Controller | **scaler**: a per-cell controller loop (leader-elected via CAS on an object) | no (new) |
| Epoch / segment lineage | **segment map v(N)**: successor/predecessor records per split/merge | no (new) |
| Auto-scale policy | per-segment target = the per-shard service limits (5 MB/s, 1 000 req/s, 5 000 rec/s) | limits + telemetry exist |

Key space: `k = xxh3(routing_key) / 2^64 ∈ [0,1)`. A stream's **segment
map** is a total partition of `[0,1)` into segments, each with:

```
SegmentDesc { seg_id (monotonic u32), range [lo, hi), shard_prefix,
              created_ms, predecessors: Vec<seg_id>, sealed_ms: Option }
```

The map lives in the registry (`streams/<hash>/segmap.json`, CAS-updated,
version-numbered). Appends and reads carry the map version; a stale
client/router gets `409 + Streams-Replay-To`-style correction exactly as
ring ownership does today (that machinery is reused verbatim).

Ordering contract (unchanged from PROFILES.md): total order per routing
key. A key's order is preserved across splits because a split **seals**
the parent segment first (no more appends), then opens children; a
reader of key k drains parent offsets before switching to the child
whose range contains k — the successor pointers make the handoff exact.
This is Pravega's epoch transition, minus their multi-reader group
coordination (our readers are per-key or per-segment already).

## 2. Scale-out (split)

Trigger — evaluated by the scaler every 10 s from the usage telemetry
(`/v1/debug/usage` counters aggregated into 2-minute EWMA rates, exactly
Pravega's two-minute-rate style):

- **hot**: EWMA(bytes_in) > 75 % of `LIMIT_BYTES_PER_SEC`, OR
  EWMA(requests) > 75 % of `LIMIT_REQS_PER_SEC`, OR EWMA(records) > 75 %
  of `LIMIT_RECS_PER_SEC`, sustained for ≥ 2 consecutive evaluations.
- Guard: segment count < `MAX_SEGMENTS_PER_STREAM` (default 64) and the
  stream opted in (`Stream-Scaling: auto` at create, or default-on for
  per-key streams; total-order streams CANNOT split — a single total
  order is definitionally one segment, enforced at create).

Mechanics (all steps CAS-guarded, crash-resumable; the scaler is the only
writer of segmap.json):

1. Pick split point: median of the observed per-key byte distribution if
   key stats exist (usage.rs gains a tiny per-segment key-range sketch —
   16-bucket histogram over k), else range midpoint (Pravega defaults to
   even splits too).
2. Write segmap v(N+1): parent marked `sealing`, two children created
   (new seg_ids, shard assignment per §4).
3. Seal parent on its owning shard: a `Sealed` commit op (like `close`)
   — subsequent appends to the parent get `410 segment_sealed` +
   `Streams-Segment-Map: v(N+1)` header; clients re-route to children.
4. Children accept appends immediately (they're empty logs — no data
   movement, ever; history/absorption of the parent continues
   independently until drained, then the parent shard prefix is GC'd).
5. Readers: per-key readers finish the parent range (bounded — it's
   sealed), then follow `successors[k]`.

Scale-in (merge) is the mirror image: two adjacent **cold** segments
(EWMA < 15 % of target for ≥ 10 evaluations ≈ 100 s… production default
much longer, e.g. 30 min) seal both parents and open one child covering
the union range. Merge only when both live on shards whose host is below
50 % aggregate utilization, and never within `SCALE_COOLDOWN` (default
10 min) of the segment's creation — Pravega's anti-flap rules.

## 3. Why this is cheap here

Pravega moves no data on scale events and neither do we: a segment's
records live in the shard log where they were written; splitting only
changes WHERE NEW WRITES GO. The absorber keeps draining parent segments
to the history tier after sealing; a fully-absorbed sealed segment is a
tombstone in the segment map plus history-tier objects.

## 4. Placement & host rebalancing

Two independent loops, deliberately separated (Pravega separates
controller scaling from segment-container balancing the same way):

- **Scaler** (above): decides the segment map. Placement of NEW segments:
  least-loaded shard on the least-loaded host (load = Σ segment EWMA
  bytes / host absorb capacity ~14 MB/s per 1 GB instance, from fleet
  heartbeats).
- **Rebalancer**: moves whole SHARDS between hosts when a host is
  unhealthy: `absorb_lag_max_secs > 60` (the agreed signal) or RSS/CPU
  sustained hot. Shard moves already exist operationally (fencing +
  reopen + Streams-Replay-To); the rebalancer just chooses WHEN, via the
  ring: write a new `ring_active` assignment, the losing host closes the
  shard on fence, the gaining host opens it lazily on first routed
  request. Move budget: one shard per host per minute (churn guard).

Scale-out vs rebalance decision: if a HOST is hot but its segments are
individually under 75 % of per-segment limits → rebalance (spread
shards). If a SEGMENT is hot → split it. Both loops read the same
telemetry; the scaler runs strictly after the rebalancer each tick so a
freshly-moved shard isn't double-treated.

## 5. Client/API surface

- Append/read requests gain optional `Streams-Segment-Map-Version`; the
  server answers `409 stale_segment_map` + the current version when the
  client's map is behind AND the routing outcome would differ (same
  convergence contract as ring ownership).
- `GET /v1/stream/{name}/segments` returns the segment map (+ lineage)
  for SDKs; SDKs cache it and re-fetch on 409/410.
- New error codes: `segment_sealed` (410, includes successor ids),
  `stale_segment_map` (409).
- Billing/usage: counters keyed by (stream, segment) so limits remain
  per SEGMENT — the per-shard limits become per-segment limits, which is
  what "Kinesis-like but more generous" means post-split.

## 6. Failure analysis

- Scaler crash mid-split: segmap is CAS-versioned; every step idempotent
  and resumable from the persisted map state (`sealing` parents are
  re-driven to sealed; children re-announced). Two scalers: CAS loser
  aborts (leader lease object, 30 s TTL, renewed at 10 s).
- Seal race: appends in flight when the seal commits are decided by the
  shard's committer order — before the Sealed op → accepted; after →
  410. The committer is already the single serialization point.
- Reader at split boundary: parent is sealed hence bounded; the
  `next_offset` at seal is recorded in the segmap entry so a reader can
  prove it drained the parent before switching (exactly-once handoff).
- Merge with laggy absorber: merging only changes future writes; parent
  drains are independent. No interaction with absorb-lag beyond the
  placement guard.

## 7. Env / knobs

| env | default | |
|---|---|---|
| `SCALE_EVAL_SECS` | 10 | scaler tick |
| `SCALE_RATE_WINDOW_SECS` | 120 | EWMA window for segment rates |
| `SCALE_HOT_PCT` / `SCALE_COLD_PCT` | 75 / 15 | of per-segment limits |
| `SCALE_HOT_EVALS` / `SCALE_COLD_EVALS` | 2 / 180 | consecutive evals |
| `SCALE_COOLDOWN_SECS` | 600 | min segment age before re-scale |
| `MAX_SEGMENTS_PER_STREAM` | 64 | |
| `REBALANCE_LAG_SECS` | 60 | absorb-lag threshold (sustained 2 fleet ticks) |
| `REBALANCE_MOVE_COOLDOWN_SECS` | 60 | churn guard: min gap between moves per host |
| `SCALE_FAULT_POINT` | unset | test-only: `after_seal` aborts a split in the seal→save window (D4) |
| `ABSORB_PAUSE` | unset | test-only: `1` pauses absorption so lag grows (D3) |

### Crash resumability (implemented)

The only non-atomic transition is split = seal, then CAS map-save. A
scaler that dies between the two leaves a sealed segment that the map
still shows live; routed appends bounce off `stream_closed`. The append
wrapper self-heals: after one failed map refresh it re-seals the child
(idempotent — `Closed` returns the frozen offset) and publishes the
missing transition itself (`scaler::resume_split`, CAS-raced safely).
The crashed transition was never published, so completing it with a
fresh midpoint is correct regardless of what the dead scaler intended.

### Known v1 limitations

- **Merges require co-located pairs.** A merge seals BOTH parents, and
  seals run through the local engine; each instance only evaluates
  segments whose shards it serves. An adjacent cold pair split across
  two instances is never merged (correct, just not compacted). With
  production `SCALE_COLD_EVALS=180` merges are rare slow events; the
  fix, if it earns its keep, is seal-by-owner over instance-to-instance
  HTTP (heartbeats would carry a `self_url`) or explicit child placement
  via the segmap's `shard_prefix` field (schema already carries it —
  placement is hash-random today).
- **Segmap cache refresh is synchronous**: one request per stream per
  2 s TTL pays a ~25–50 ms store GET (measured: p99 +53 ms on a 25 ms
  store, p50 +0.4 ms). Stale-while-revalidate would erase it.
- **The segment map retains sealed lineage and only grows** (~200 B
  JSON per transition; production cooldowns bound growth to ~KB/day,
  but it is unbounded in principle). Mitigations: the append-path TTL
  refresh revalidates by etag (`If-None-Match` → 304), so map SIZE
  never taxes routing — only actual transitions pay a full download;
  `SegmentMap::prune()` exists and removes drained sealed entries, but
  its trigger is retention semantics (an entry must outlive its
  segment's READABLE data, which today is retained indefinitely). Wire
  prune when stream retention lands; revisit encoding (binary/zstd or
  an archive chain for cold lineage) only if transition volume ever
  outpaces pruning.
- **Producer sessions do not survive a split.** Producer idempotence
  state (Producer-Id → epoch/seq) is per SEGMENT: after a split the
  fresh child expects seq 0, so a producer must resync on
  `producer_seq_gap` (`Producer-Expected-Seq` header carries the
  target). Consequence: a batch whose outcome was ambiguous (e.g. 408)
  exactly when its segment sealed can commit on the parent AND, after
  resync, again on the child — the dedup window reopens at the
  boundary. Ladder D2 (pass 2b) demonstrated the underlying failure
  without producer headers at all: two 408-ambiguous batches were
  retried as new content and duplicated per-key sequences. The ladder
  driver is now an idempotent producer (retry same seq until
  unambiguous, resync on gap), which closes everything except the
  seal-coincident window. Production fix candidates: seed the child's
  producer table from the parent at split (map-save already knows the
  sealed offset), or reader-side (producer-id, seq) dedup across the
  lineage.

### Rebalancer (implemented)

Self-initiated: the laggard instance knows its own per-shard absorb lag,
so it initiates the move (no leader). When `absorb_lag_max > 
REBALANCE_LAG_SECS` for 2 fleet ticks and the move cooldown passed, it
CAS-appends `{shard -> coolest fresh peer}` to `fleet/overrides.json`
and drops the shard from its serving map. Everyone mirrors
overrides.json into routing each fleet tick; `effective_owner()` =
override (if target live) else rendezvous. The gaining instance fences
the shard log on first routed request — existing R2/R3 machinery.

## 8. Validation plan

Docker ladder (all on this machine, 1 GB-limited containers to mirror
Compute):

- D1: 3 servers + s3lite + 1 scaler; one per-key stream driven past 75 %
  → observe exactly one split, zero append errors during seal (clients
  retry the 410 within one round-trip), per-key order verified by the
  conformance checker across the boundary.
- D2: drive 4× limit → recursive splits to 4 segments; then idle →
  merges back respecting cooldowns; order verified continuously.
- D3: hot HOST, cool segments → rebalancer moves shards (no splits);
  absorb-lag injected via absorber pause → move fires at 60 s.
- D4: kill the scaler mid-split at every step (fault-point loop) →
  resumability; kill a server mid-move → fencing correctness.
- D5: 30-minute soak at mixed load with chaos (random container
  restarts), RSS ≤ envelope, zero data loss (checker), zero stuck
  segments.

Only after D1–D5 are green twice consecutively: the 4-instance Compute
cluster repeat of D1/D3/D5.

## 9. Validation results (2026-07-24 / 25)

Full run journal: [bench/docker/LADDER-LOG.md](../bench/docker/LADDER-LOG.md).

The docker ladder ran eight passes. Every red rung converted into a
real fix; the harness itself needed as much hardening as the code.

**Product defects the ladder caught (all fixed):**

| defect | rung | consequence if shipped |
|---|---|---|
| zombie Db after a shard move (`begin_close` never closed slatedb) | p3 D2 | GC of a fenced owner deleted live SSTs — **data loss** |
| scaler `owns()` was ring-only; possession is the truth | p5 D1 | grandfathered shards evaluated by nobody — no stream ever split |
| absorb lag re-derived a shard from the WRONG hash (storage vs stream) | p6b D3 | rebalancer had no victim — shard moves never fired |
| in-flight work hung on a move | p1 D3 | one client batch lost per worker at each move |
| absorb-lag gauge froze after fencing | p1 D3 | phantom lag on an instance serving nothing |
| split crash between seal and map-save | design | sealed-but-live segment; appends bounce forever |
| backpressure starves the lag signal (sheds before commit) | p4b D3 | a wedged instance never rebalances |

**Harness defects (results-invalidating, all fixed):** fleet mode was
never enabled (`FLEET_PREFIX` unset) so early passes ran effectively
single-instance; an idle fleet shrank `desired` to 1 and collapsed the
ring (`FLEET_MIN`); the fresh-world preamble wiped the bucket while old
servers were still writing manifests into it (poisoned world); the
emulator OOM'd twice under cumulative load; the D3 stream hunt assumed
a fixed owner and sampled one port during the ownership settling
window; the D3 ownership probe polluted the stream it measured; and the
run monitor piped through `head`, which block-buffers and swallowed
every progress event.

**Rung evidence at its best (pass 7b/8, current build):**

- D1: 1,548,800/1,548,800 at 4,300 rec/s through a live split, zero
  errors, producer sessions resynced across the seal boundary.
- D2: 5.1–5.7 M records at 12–13.5 k rec/s against a 5 k/segment limit
  — recursive splits, then merges converging (map v10, 4 live/12
  sealed), full-lineage order verified.
- D3: `moving shard 010 -> streams-3 (absorb lag 64s)` → **eager open
  2 s later** → `returned 1 shard(s) to rendezvous owners` 62 s later.
  Trigger, handoff, and return-home in one rung.
- D4: injected crash in the seal→save window healed by `resume_split`
  in 527 ms; 1,292,800/1,292,800, zero client errors.
- D5: 30-min soak, 8 chaos restarts, 5,401,600/5,401,600, RSS inside
  the 1 GB envelope.

Production measurement on real Tigris (SIN): history reads (consumer
catch-up) **84 → 3,528 rec/s (42×)** with the scan-readahead fix, and
the 1 s producer→consumer guarantee re-validated on the scaling-era
binary (35/36 windows < 1 s, median p99 256 ms at 735 req/s).

Routing overhead for scaled streams: p50 +0.4 ms, p99 +53 ms (segmap
cache refresh — now etag-revalidated, so map size stays off the append
path).

Gate for the 4-instance Compute cluster: two consecutive fully green
passes. Progress is tracked in the run journal.
