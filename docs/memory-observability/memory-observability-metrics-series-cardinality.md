# Memory Observability: Metrics Series Cardinality

This counter tracks the number of unique in-process `Metrics` series keys.

Observability name:
- `tieredstore.mem.leak_candidate.metrics.series`

Where implemented:
- `src/app_core.ts:602` maps this counter.
- `src/metrics.ts:121-123` exposes `Metrics.getMemoryStats()`.

State source lines:
- `src/metrics.ts:87` `series` map declaration.
- `src/metrics.ts:90-97` dynamic series insertion in `record(...)`.
