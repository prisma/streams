# Single-instance saturation baseline (the release-gate reference)

Recorded 2026-07-21, slate @ a402a3b, Prisma Compute FRA (eu-central-1),
1-vCPU instance, fresh management-API bucket (Tigris, region-local: 25 ms
PUT at idle), pilot generator DIRECT at the server: 32 streams, closed-loop
concurrency 128, batch 16 (~3.9 KB/request), 16 min sampled at 20 s after a
2 min warmup drop.

| metric | value |
|---|---|
| achieved req/s median (p10 / p90) | 266 (131 / 986) |
| events/s median | ~4,260 |
| client winP50 (median of 20 s windows) | 111 ms |
| client winP99 (median / p90 of windows) | 1.71 s / 1.84 s |
| client errors | 0 |
| server RSS median / max | 460 / 587 MB |
| store put:wal p50 / p99 (median of windows) | 73 / 747 ms |
| store put:sst p99 (median of windows) | 3.4 s |

Notes for comparison runs:

- Throughput oscillates 131–986 req/s with compaction stalls; compare
  medians and the p10 (a p10 of ~0 means stall windows — investigate).
- The store-side put:wal numbers are substrate-dependent (Tigris under
  load); large drift between runs means the platform changed, not the
  binary — compare put:wal medians before attributing latency shifts.
- The same run on slate-codex @ 0c992de OOM-crash-looped (kernel kill at
  ~735 MB anon RSS ~45 s after each restart, 33.9k client errors) — the
  failure mode this gate exists to catch.

Pass thresholds for a candidate build: AWS-readyness.md §5.
