# Runtime Read-Path Follow-Up

Date: 2026-04-01

## Scope

This report covers the first full Linux all-index ingest after the runtime
follow-up changes on top of `PSCIX2`:

- range-read the bundled companion TOC plus only the requested family payload
- byte-budgeted TOC and section caches
- lazy restart-string dictionary lookup and prefix expansion
- lighter `.fts` runtime block union logic and selectivity ordering
- single-pass bundled companion building across all families
- throughput-aware exact-index pause gate
- reader memory-sampler integration and aggregate clone cleanup

Primary run:

- [local-linux-all-runtime-followup-2026-04-01.md](/Users/sorenschmidt/code/streams/experiments/reports/local-linux-all-runtime-followup-2026-04-01.md)

Reference baseline:

- [pscix2-cutover-soak-2026-04-01.md](/Users/sorenschmidt/code/streams/experiments/reports/pscix2-cutover-soak-2026-04-01.md)

## Verification

The runtime changes passed:

- `bun run typecheck`
- `bun run check:result-policy`
- `bun test`

## Result

The new runtime changes materially improved stability and memory efficiency.
This run has already moved past the previous `PSCIX2` stall point and is still
making forward progress.

### Comparison to the PSCIX2-only run

| Checkpoint | PSCIX2-only | Runtime follow-up |
| --- | ---: | ---: |
| 5 min logical size | `0.244 GB` | `0.257 GB` |
| 5 min uploaded segments | `14` | `15` |
| 5 min RSS | `2.18 GiB` | `0.82 GiB` |
| 5 min health | `1741 ms` | `13 ms` |
| 5 min index status | not called out in summary | `15 ms` |
| 5 min FTS probe | `1185 ms` | `804 ms` |
| 5 min column probe | `218 ms` | `311 ms` |
| 5 min exact filter | `4389 ms` | `4189 ms` |
| 5 min browse probe | `13.13 s` | timed out at `20 s` |
| 5 min aggregate probe | `11.09 s` | timed out at `20 s` |

### Mid-run progress

At later checkpoints the new run stayed healthy while exact indexes remained
fully paused:

| Time | logical size | uploaded segments | RSS | health | exact count |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10 min | `0.546 GB` | `32` | `2.31 GiB` | `32 ms` | `0` |
| 15 min | `0.805 GB` | `47` | `2.02 GiB` | `1 ms` | `0` |
| 22 min | `1.124 GB` | `66` | `2.91 GiB` | `<1 ms` | `0` |

### Old failure point cleared

The prior `PSCIX2` soak stalled around:

- `1.145 GB` logical size
- `68` uploaded segments
- rising RSS and no forward ingest progress

The new run has already exceeded that point and is still progressing:

- `1.153 GB` logical size
- `68` uploaded segments
- `/health` still `200` in `0.38 ms`
- `/_index_status` still `200` in `14 ms`
- exact indexes still fully paused

And shortly after that:

- `1.282 GB` logical size
- `76` uploaded segments
- RSS `4.71 GiB`
- `/health` still `200` in `0.31 ms`
- bundled companions fully caught up across all `76` uploaded segments

## Interpretation

The runtime changes fixed the previous failure mode well enough to push the
all-index Linux ingest beyond the earlier stall boundary.

What clearly improved:

- early memory use dropped sharply
- control-plane latency stayed low instead of degrading with RSS
- the run kept ingesting past the old stall point
- exact backfill remained fully paused, so the improvement is not coming from
  exact scheduling alone

What is still not good enough:

- the browse probe still times out at the `20 s` sample limit
- the aggregate probe still times out at the `20 s` sample limit
- RSS still climbs as the `agg` family grows, even though the process remains
  healthy

## Main conclusion

The combination of range-reading only the requested companion section, lazy FTS
dictionary access, and single-pass companion building substantially improved the
all-index ingest workload.

The remaining dominant hotspot is no longer the original bundled-companion
decode/build explosion. It is now the expensive broad-query path, especially
aggregate-heavy reads, plus continued resident growth as the aggregate family
gets larger.
