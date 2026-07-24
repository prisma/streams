# Docker ladder runs (SCALING.md §8) — 2026-07-24

Fleet: 3 × `streams-slate` servers, 1 GB / 2 CPU each (mirrors Prisma
Compute), shared s3lite (25 ms/op, 4 GB — the emulator must not OOM
before the servers do), 8 shard prefixes, ring seeded `count=3`.
Ladder-pace policy knobs: `SCALE_RATE_WINDOW_SECS=60`,
`SCALE_COOLDOWN_SECS=60`, `SCALE_COLD_EVALS=12` (production defaults
120 s / 600 s / 180 — same mechanism, test-length windows).

## D1 — one split under load, order across the boundary (GREEN, pass 1)

Stream `d1sg`, 32 keys, 4,300 rec/s target (86 % of the 5,000 rec/s
per-segment limit), 100-record batches, 360 s.

- Driver: **1,548,800/1,548,800 records, 4,299.7 rec/s sustained, zero
  errors, zero retries, zero redirects** — the seal/split transition was
  fully absorbed server-side.
- Scaler: EWMA crossed 75 % → `split seg0 of d1sg at 0x7fff…`
  (`next=627,200` on the earlier identical run d1sf; d1sg split at its
  own boundary). Exactly one split; no runaways.
- Order check: all 32 keys gapless `0..48,399` across seg0 → child;
  every key in exactly one child (9/7-ish hash spread). `TOTAL drained
  1,548,800 / sent 1,548,800 — PASS`.
- Multi-instance reality check: counters/seal on the serving instance,
  segmap CAS in the shared bucket, ring redirects exercised in earlier
  probes (affinity converges; steady state has zero redirects).

## Bonus: full lifecycle merge observed

After d1sg went idle its two children went cold on schedule and, being
co-located on one instance, merged: `scaler: merge seg1+seg2 of d1sg`.
Split → hot traffic → idle → cold → merge, end to end, no operator
input. (Cross-instance pairs do NOT merge in v1 — documented
limitation in SCALING.md.)

## D2 — recursive splits, then merges (first attempt aborted, rerun below)

First attempt (`d2s`, 14 k rec/s): first split fired
(`split seg0 of d2s`, next=515,000), then **s3lite OOM-killed at its
1 GB cgroup limit** — the in-memory emulator was holding D1's 1.55 M
records plus D2's ingest plus heartbeat churn from four zombie
containers of a months-old experiment that OrbStack's post-crash boot
had resurrected (`restart=on-failure`, pointed at the same host port).
Zombies stopped, emulator raised to 4 GB, rerun as `d2t`.

Incidents this session (all environmental, none data-integrity):

| incident | cause | fix |
|---|---|---|
| host disk full (489 MB free) | 22 GB cargo target + 21 GB docker VM | cleaned both (~46 GB freed) |
| OrbStack down | killed by the ENOSPC episode | restarted; zombie fleet from restart policies stopped |
| s3lite OOM (exit 137) | 1 GB limit on an in-memory store holding 2 runs | 4 GB; servers stay at 1 GB |

## D2 rerun (`d2t`) — recursive splits under 2.8× offered load

Drive: 14 k rec/s offered against a 5 k rec/s per-segment limit, 420 s.

- Driver: **5,507,000/5,507,000 records, zero errors**; 10,453 requests
  drew 429 + Retry-After and were retried cleanly (that is the limiter
  working as designed, not failure).
- Accepted-rate staircase (M4): ~**10 k rec/s plateau at 2 segments**
  (2 × 5 k caps, heavy 429s) → split cascade → **~14 k+ rec/s at 4+
  segments** — the offered load fully accepted once enough segments
  exist. Effective driver rate 13.1 k rec/s including the throttled
  phase.
- Splits went recursive past 4 (7+ live segments on the busiest
  instance): catch-up bursts at the bucket boundary kept children above
  the 75 % trip. Behavior is per policy (MAX_SEGMENTS guard far away);
  production's 120 s EWMA window would damp the cascade.
- Merge phase: segments went cold on schedule; map converged to
  **v10, 4 live / 12 sealed** (co-located pairs merged; cross-instance
  pairs stay split — documented v1 limitation).
- Order check across the FULL 16-segment lineage: **5,507,000 /
  5,507,000 drained, all 32 keys gapless — PASS**. D2 GREEN (pass 1).

## Fleet-mode correction (affects all later runs)

`FLEET_PREFIX` was unset in the compose — the ring, heartbeats, and
rebalancer were dormant, so D1/D2 (pass 1) ran effectively
single-instance: their scaler/segmap/order results stand, but ring
enforcement does not apply to them. Fixed (`FLEET_PREFIX=ladder-fleet`)
plus a new `FLEET_MIN` floor (an idle fleet otherwise shrinks desired
to 1 and the ring collapses — FLEET_MIN=3 pins the test ring; in
production it doubles as an HA floor). Pass 2 of the ladder runs with
the true 3-instance ring — redirect counts in driver stats prove it.

## D3 — absorb-lag rebalancer (first run: move fired; two fixes)

streams-2 ran with ABSORB_PAUSE=1; stream f3g owned by it; 2 k rec/s.

- Lag climbed 1 s/s; at **64 s** streams-2 published the move:
  `rebalancer: moving shard 000 -> streams-3 (absorb lag 64s)` and
  streams-3 fenced the log 3 s later (`opening shard log shards/000
  (lazy; fences prior owner)`). Post-move: streams-3 lag 6 s and
  draining, streams-2 serving nothing. d2t replay backlog legitimately
  triggered additional moves from other instances (each initiates for
  its own lag; cooldown is per-instance).
- Driver: 598,400 ok, 295 replay-to redirects followed, 25× 429 — but
  **3,200 records abandoned**: the test driver's ~3.6 s max patience is
  shorter than the ~10 s fencing handoff. Fixed the harness (patient
  backoff, as a production SDK would). The handoff window itself is the
  known 3 s anti-flap holdoff + lazy open, availability dip confined to
  the moved shard.
- Fixed a real gauge leak the run exposed: a fenced-away shard's
  absorb-lag entries froze at their last value (phantom 74 s lag on an
  instance serving nothing). The absorber now clears its pending
  streams' lag entries on exit.
- Added `GET /v1/stream/{name}/segments` (SCALING.md §5) — the checker
  now reads the map from the API instead of guessing at bucket objects.

## D3 rerun (`g3a`, true ring) — GREEN

Move fired again (`101 -> streams-1`), **598,400/598,400 drained, ORDER
CHECK PASS** — zero loss, zero duplicates through a live shard move; 396
replay-to redirects followed. Remaining client-visible cost: requests
already in flight to the losing instance at the drop moment hung to
client timeout (one batch per worker). Fixed in code: the rebalancer now
`begin_close()`es the engine (immediate retryable `Moved` for everything
queued) — validated in pass 2.

## D4 — crash-resume (fault-injected splits) — GREEN

All three servers ran `SCALE_FAULT_POINT=after_seal`: every split
attempt "died" in the seal→map-save window (the only non-atomic step).

- `FAULT INJECTED: crashed after sealing d4s#0 (map not saved)` →
  **527 ms later** the append path healed it: `scaler: resumed crashed
  split of d4s seg0 at 0x7fff…` (re-seal is idempotent, missing
  transition published by the wrapper, CAS-raced safely).
- Driver: **1,292,800/1,292,800 at 4,299.7 rec/s, zero errors, zero
  retries** — the injected crash was invisible to clients (15 internal
  stream-closed refreshes, all absorbed inside the routing wrapper).
- Map converged v2 live=[1,2] sealed=[0]; ORDER CHECK PASS.

## Production measurement: history-read fix on real Tigris (SIN)

A/B on the warm sinmax rig (`max-22`, 1.6 M absorbed records, ~2 KB
each), identical paged reads from offset start:

| binary | first page | steady rate | failure mode |
|---|---|---|---|
| pre-fix | 39.9 s / 8,441 recs | **84 rec/s** | 504 on page 2 (front-door 60 s kill) |
| post-fix | 4.3 s cold, ~2 s after | **3,528 rec/s** (~8 MB/s egress) | none (12 pages clean) |

**42× faster; catch-up on this stream: ~5.3 h → ~7.6 min.** The fix is
`hist_scan_opts()` (2 MB readahead / 2 fetch tasks / block cache) on the
history scan — slatedb's default `ScanOptions` fetches ONE ~200-byte
compressed block per sequential GET. New SIN deploy: version
`cpv_m4es5otj7ukw458fqdmk58bg` (adds the scaling machinery, inert
without `Stream-Scaling: auto`).

## D5 — 30-minute chaos soak — GREEN (pass 1 complete)

3,000 rec/s mixed load, **8 random server restarts** over 30 min.

- Driver: **5,401,600/5,401,600, zero errors** (96 retries, 32 × 429 —
  all absorbed by normal backoff).
- ORDER CHECK PASS: all 32 keys gapless 0..168,799.
- RSS stayed inside the 1 GB envelope on all three servers throughout.
- Zero redirects even under chaos: a restarted container reclaims its
  own name's shards (self-fencing), so client affinity stays valid.

**Ladder pass 1: D1 ✓ D2 ✓ D3 ✓ D4 ✓ D5 ✓.** Fixes landed mid-pass
(fleet mode, FLEET_MIN, begin_close, lag-gauge hygiene, /segments,
patient driver); pass 2 runs the whole ladder again on the final image.
