# Memory Observability: Secondary Index Idle Map

This counter tracks stream cardinality in the secondary-index idle-tracking map.

Observability name:
- `tieredstore.mem.leak_candidate.secondary_index.stream_idle_ticks_streams`

Where implemented:
- `src/app_core.ts:603` maps this counter.
- `src/app.ts:258` publishes runtime count into memory subsystem `counts`.
- `src/index/secondary_indexer.ts:194-215` exposes `streamIdleTickEntries` via `getMemoryStats()`.

State source lines:
- `src/index/secondary_indexer.ts:75` `streamIdleTicks` map declaration.
