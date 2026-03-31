# Heavy Combo Memory Comparison (2026-03-31)

## Configuration

- Selectors: `exact:ghArchiveId`, `fts:message`, `agg:events`
- Ingest target: `16` uploaded segments
- Stop condition: terminate ingester at `16` uploaded segments, then observe recovery for `2` one-minute samples
- Reports:
  - macOS: [heavy-combo-memory-macos-2026-03-31.md](/Users/sorenschmidt/code/streams/experiments/reports/heavy-combo-memory-macos-2026-03-31.md)
  - Linux: [heavy-combo-memory-linux-2026-03-31.md](/Users/sorenschmidt/code/streams/experiments/reports/heavy-combo-memory-linux-2026-03-31.md)

## Summary

| Metric | macOS direct | Linux container |
| --- | ---: | ---: |
| Peak process RSS | `1,318,486,016` B | `881,344,512` B |
| Peak JS heap used | `296,281,234` B | `201,920,979` B |
| Peak JSC heap size | `258,305,408` B | `202,355,796` B |
| Last process RSS after recovery window | `1,318,305,792` B | `543,322,112` B |
| Last JS heap used after recovery window | `2,697,828` B | `2,311,182` B |
| RSS retained after recovery | `99.99%` of peak | `61.65%` of peak |
| RSS released after recovery | `0.01%` | `38.35%` |

## Functional End State

Both environments reached the same logical end state:

- `16/16` uploaded segments
- bundled companions fully caught up
- `fts:message` fully caught up
- `agg:events` fully caught up
- `exact:ghArchiveId` built to `16` segments

The timing differed slightly:

- On macOS, the exact L0 run had already completed by the time the ingester was stopped.
- On Linux, the companions and search families were caught up first; the exact L0 run completed during the first recovery sample after ingest stopped.

## Phase Behavior

### macOS

- The heaviest phase by JSC heap was `companion`, peaking at `258,305,408` B.
- `exact_l0` was much smaller, peaking at `12,717,270` B of JSC heap.
- `append` and `idle` both reached the full process RSS peak of about `1.32 GB`.
- After ingest stopped, JS heap collapsed to about `2.7 MB`, but process RSS remained essentially unchanged at about `1.318 GB`.

### Linux

- The heaviest phase by JSC heap was also `companion`, peaking at `202,355,796` B.
- `exact_l0` was material on Linux, peaking at `141,479,037` B of JSC heap, but the total process RSS still stayed well below macOS.
- After ingest stopped and the exact run finished, JS heap fell to about `2.3 MB` and process RSS dropped from about `881 MB` to about `543 MB`.

## Comparison

The combined heavy trio is heavier than the isolated single-index runs, but it still does not reproduce the earlier worst-case multi-gigabyte macOS footprint. What it does show clearly is:

1. `fts:message` + `agg:events` + `exact:ghArchiveId` together are enough to create materially higher pressure than any single selector.
2. The bundled companion path is still the dominant heap amplifier in both environments.
3. macOS retains substantially more resident memory after work completes.
4. Linux shows real recovery once ingest stops and exact catch-up completes.

Numerically:

- macOS peak RSS was about `437 MB` higher than Linux.
- macOS peak RSS was about `1.50x` Linux peak RSS.
- Linux released about `338 MB` of RSS during the recovery window.
- macOS released effectively none according to the sampler.

## Conclusion

This experiment strengthens the earlier conclusion:

- The heavy memory behavior is not caused by a single isolated index family.
- The strongest workload-specific amplifier is still bundled companion building.
- The OS/runtime effect matters a lot: Bun on macOS retains resident memory much more aggressively than the same workload on Linux.

If the goal is to reduce real production risk, the next target should be the combined companion/search workload rather than the exact index in isolation. In parallel, Linux remains the better reference environment for deciding whether a change fixes actual allocator retention versus only shifting Bun/macOS behavior.
