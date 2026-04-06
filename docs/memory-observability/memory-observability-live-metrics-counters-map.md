# Memory Observability: Live Metrics Counters Map

This counter tracks the number of stream-keyed counters retained by `LiveMetricsV2`.

Observability name:
- `tieredstore.mem.leak_candidate.live_metrics.counter_streams`

Where implemented:
- `src/app_core.ts:599` maps this counter.
- `src/touch/live_metrics.ts:347-349` exposes `LiveMetricsV2.getMemoryStats()`.

State source lines:
- `src/touch/live_metrics.ts:227` `counters` map declaration.
