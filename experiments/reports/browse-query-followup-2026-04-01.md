# Browse Query Follow-Up

Date: 2026-04-01

## Scope

This report covers the browse-query work after aggregate pruning:

- exact keyword fallback to `.fts` for prefix-capable keyword fields
- candidate-aware FTS clause evaluation ordered by estimated selectivity
- targeted browse reproduction for `type:pushevent owner:prisma*`

Files changed:

- [src/search/query.ts](/Users/sorenschmidt/code/streams/src/search/query.ts)
- [src/search/fts_runtime.ts](/Users/sorenschmidt/code/streams/src/search/fts_runtime.ts)
- [src/reader.ts](/Users/sorenschmidt/code/streams/src/reader.ts)
- [experiments/bench/search_browse_perf.ts](/Users/sorenschmidt/code/streams/experiments/bench/search_browse_perf.ts)
- [docs/storage-layout-architecture.md](/Users/sorenschmidt/code/streams/docs/storage-layout-architecture.md)
- [docs/indexing-architecture.md](/Users/sorenschmidt/code/streams/docs/indexing-architecture.md)
- [test/search_http.test.ts](/Users/sorenschmidt/code/streams/test/search_http.test.ts)

## Targeted Reproduction

Benchmark command:

```bash
bun run experiments/bench/search_browse_perf.ts \
  --segments=12 \
  --rows-per-segment=512 \
  --prefix-variants=32 \
  --prefix-every=2 \
  --match-every-segments=1 \
  --tail-nonmatch-docs=128 \
  --iterations=100
```

This dataset forces the newest tail docs to match `owner:prisma*` while failing
`type:pushevent`, so the browse query must do real candidate pruning instead of
winning on the first tail record.

### Baseline

Measured from a detached worktree at commit `1530343`:

- elapsed: `163.79 ms` total / `1.64 ms` avg
- posting blocks: `3200`
- JSON parses during query loop: `8002`
- object-store GET delta: `102`

### Current

Measured on the current working tree:

- elapsed: `127.09 ms` total / `1.27 ms` avg
- posting blocks: `3500`
- JSON parses during query loop: `1602`
- object-store GET delta: `102`

### Interpretation

The runtime now does more companion-side posting work, but it avoids most of
the record JSON decode work:

- average latency improved by about `22.6%`
- query-loop JSON parses improved by about `5.0x`

That is the important win for the reproduced browse path. The planner is now
keeping more of the `type` filter in the `.fts` candidate layer instead of
deferring it to record JSON evaluation.

## Full Linux All-Index Soak

Primary run:

- [local-linux-all-browse-followup-2026-04-01.md](/Users/sorenschmidt/code/streams/experiments/reports/local-linux-all-browse-followup-2026-04-01.md)

Reference baseline:

- [local-linux-all-aggregate-pruning-2026-04-01.md](/Users/sorenschmidt/code/streams/experiments/reports/local-linux-all-aggregate-pruning-2026-04-01.md)

### What improved

Early and mid-run control-plane/query health improved materially:

- 5 min browse probe improved from timeout (`20.01 s`) to `13.08 s`
- 5 min column probe improved from `1855 ms` to `10 ms`
- 5 min FTS probe improved from `4252 ms` to `11 ms`
- 5 min health improved from `129 ms` to `3.3 ms`
- 5 min RSS improved from `0.73 GiB` to `0.50 GiB`

At comparable later checkpoints:

- around 16 min, health stayed about `<1 ms` vs the earlier run's `88.9 ms`
- at 21 min, RSS was about `0.94 GiB` vs the earlier run's `1.64 GiB`
- exact indexes remained fully paused throughout

### What did not improve

The run did not improve end-to-end ingest throughput:

- previous run reached `1.670 GB` / `99` uploaded segments in about `32.6 min`
- this run reached `1.318 GB` / `78` uploaded segments in about `32.0 min`
- this run reached `1.886 GB` / `112` uploaded segments only by about `43.0 min`

Browse latency also remained unstable under sustained mixed load:

- it improved early
- then returned to the `20 s` timeout by about `21 min`

Late-run aggregate/FTS probe latency and RSS also climbed again:

- `agg` reached `7.69 s` at about `26 min`
- `fts` reached `2.50 s` at about `43 min`
- RSS peaked above `3 GiB` in sampled summaries
- sampler snapshots still showed multi-GB retained process footprint late in the run

### Recovery

After stopping ingest:

- logical size stayed flat at `1.887 GB`
- browse still timed out at `20 s`
- column probe recovered quickly to about `60-70 ms`
- FTS probe stayed elevated around `0.88-1.65 s`
- aggregate probe stayed around `1.10-1.15 s`
- health recovered from about `762 ms` immediately after stop to about `22 ms` after one recovery interval

## Conclusion

The browse-path planner work is a real improvement on the targeted reproduction.
It removes a large amount of record JSON decode and reduces early-run browse,
FTS, column, and health latency in the full soak.

It is not sufficient to fix the full mixed ingest workload by itself.
The remaining bottleneck is still broader than the exact-keyword browse planner:

- browse no-hit / broad-hit behavior still degrades under sustained load
- aggregate and FTS probe latency still climb later in the run
- ingest throughput is still not where the earlier run was

The next optimization target should therefore stay on the live read path under
mixed load rather than exact scheduling or aggregate pruning.
