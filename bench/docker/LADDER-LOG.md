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
