# Aggregate Pruning Follow-Up

Date: 2026-04-01

## Scope

This report covers the follow-up work after the runtime read-path changes:

- persist primary timestamp min/max bounds for each bundled companion object
- use those local bounds to prune non-overlapping aggregate segments before any
  companion fetch
- add a targeted aggregate query benchmark to verify the reduction in
  `.agg` reads and object-store GETs

Code changes:

- [src/reader.ts](/Users/sorenschmidt/code/streams/src/reader.ts)
- [src/search/companion_manager.ts](/Users/sorenschmidt/code/streams/src/search/companion_manager.ts)
- [src/db/db.ts](/Users/sorenschmidt/code/streams/src/db/db.ts)
- [src/db/schema.ts](/Users/sorenschmidt/code/streams/src/db/schema.ts)
- [src/manifest.ts](/Users/sorenschmidt/code/streams/src/manifest.ts)
- [src/bootstrap.ts](/Users/sorenschmidt/code/streams/src/bootstrap.ts)
- [test/aggregate_http.test.ts](/Users/sorenschmidt/code/streams/test/aggregate_http.test.ts)
- [experiments/bench/aggregate_query_perf.ts](/Users/sorenschmidt/code/streams/experiments/bench/aggregate_query_perf.ts)

Reference baseline:

- [runtime-readpath-followup-2026-04-01.md](/Users/sorenschmidt/code/streams/experiments/reports/runtime-readpath-followup-2026-04-01.md)

New full soak:

- [local-linux-all-aggregate-pruning-2026-04-01.md](/Users/sorenschmidt/code/streams/experiments/reports/local-linux-all-aggregate-pruning-2026-04-01.md)

## Verification

Passed:

- `bun run typecheck`
- `bun run check:result-policy`
- `bun test`

## Targeted Minimal Reproduction

Command:

```bash
bun run experiments/bench/aggregate_query_perf.ts --segments=48 --rows-per-segment=256 --group-cardinality=128
```

Before this change, the aligned aggregate query touched every aggregate
companion in the synthetic dataset:

| Metric | Before |
| --- | ---: |
| elapsedMs | `23.14` |
| aggCalls | `48` |
| colCalls | `0` |
| storeGetsDelta | `96` |
| indexedSegmentsUsed | `48` |

After local timestamp-bound pruning:

| Metric | After |
| --- | ---: |
| elapsedMs | `7.39` |
| aggCalls | `1` |
| colCalls | `0` |
| storeGetsDelta | `2` |
| indexedSegmentsUsed | `1` |

This is the intended shape change:

- `.agg` companion reads dropped from `48` to `1`
- object-store GETs dropped from `96` to `2`
- latency dropped by about `68%`

## Full Ingest Comparison

The new all-index Linux soak ran to the `1.5 GB` logical-size stop target and
finished with:

- `1.670 GB` logical size
- `99` uploaded segments
- exact indexes still fully paused
- bundled companions fully caught up

### Checkpoint comparison vs previous runtime-followup run

| Checkpoint | Previous runtime follow-up | Aggregate pruning follow-up |
| --- | ---: | ---: |
| 5 min logical size | `0.257 GB` | `0.331 GB` |
| 5 min uploaded segments | `15` | `19` |
| 5 min RSS | `0.80 GiB` | `0.73 GiB` |
| 5 min health | `13.0 ms` | `128.7 ms` |
| 5 min aggregate probe | timed out at `20.0 s` | `1.08 s` |
| 10-11 min logical size | `0.546 GB` | `0.696 GB` |
| 10-11 min uploaded segments | `32` | `41` |
| 10-11 min RSS | `2.20 GiB` | `1.72 GiB` |
| 10-11 min aggregate probe | timed out at `20.0 s` | `2.57 s` |
| 16 min logical size | `0.805 GB` | `1.005 GB` |
| 16 min uploaded segments | `47` | `59` |
| 16 min RSS | `1.93 GiB` | `0.94 GiB` |
| 16 min aggregate probe | timed out at `20.0 s` | `1.58 s` |
| 22 min logical size | `1.124 GB` | `1.232 GB` |
| 22 min uploaded segments | `66` | `73` |
| 22 min RSS | `2.78 GiB` | `1.64 GiB` |
| 22 min aggregate probe | timed out at `20.0 s` | `2.09 s` |
| furthest point observed | `1.282 GB`, `76` segments | `1.670 GB`, `99` segments |

### New run checkpoints

| Time | Logical size | Uploaded segments | RSS | Health | Aggregate | FTS | Col |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 min | `0.331 GB` | `19` | `0.73 GiB` | `128.7 ms` | `1.08 s` | `4.25 s` | `1.86 s` |
| 10.5 min | `0.696 GB` | `41` | `1.72 GiB` | `494.5 ms` | `2.57 s` | `2.78 s` | `2.57 s` |
| 16.1 min | `1.005 GB` | `59` | `0.94 GiB` | `88.9 ms` | `1.58 s` | `14.9 ms` | `7.0 ms` |
| 21.6 min | `1.232 GB` | `73` | `1.64 GiB` | `397.9 ms` | `2.09 s` | `439.5 ms` | `6.25 s` |
| 27.1 min | `1.433 GB` | `85` | `1.34 GiB` | `0.3 ms` | `1.54 s` | `56.5 ms` | `44.8 ms` |
| 32.6 min | `1.670 GB` | `99` | `2.45 GiB` | `111.4 ms` | `1.82 s` | `4.26 s` | `1.67 s` |

## Interpretation

The local timestamp-bound pruning change successfully fixed the targeted
aggregate regression:

- aligned rollup queries no longer fetch every `.agg` companion in history
- aggregate latency stays in the `1-3 s` range under the all-index ingest soak
  instead of timing out at the `20 s` probe limit
- the soak now advances materially farther than the previous runtime-followup
  run at the same or lower sampled RSS

The remaining dominant problem is no longer `_aggregate`. The slowest probe is
still the broad browse/default-field search, which continues to time out at
`20 s` throughout the run.

The high-frequency memory sampler also shows that transient process RSS still
spikes well above the 5-minute `ps` snapshots:

- main-process peak RSS: about `4.31 GiB`
- top aggregate-phase RSS: about `3.11 GiB`
- top search-phase RSS: about `4.12 GiB`

So this change materially improved the targeted aggregate path and pushed the
all-index ingest further, but it did not eliminate the broader search-path and
memory-retention issues.
