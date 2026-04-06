# Memory Observability: Local Mock R2 In-Memory Data

This counter group tracks in-process memory/object retention for local `MockR2Store` usage.

Observability names:
- `tieredstore.mem.leak_candidate.mock_r2.in_memory_bytes`
- `tieredstore.mem.leak_candidate.mock_r2.object_count`

Where implemented:
- `src/app_core.ts:604-605` maps these counters.
- `src/app.ts:212-213` reads `MockR2Store` in-memory stats.
- `src/app.ts:265-266` publishes runtime values into memory subsystem `counts`.

State source lines:
- `src/objectstore/mock_r2.ts:42-63` map storage and default in-memory budget behavior.
- `src/objectstore/mock_r2.ts:245-250` helper accessors (`size()`, `memoryBytes()`).
- `src/server.ts:310-325` local-object-store construction and optional spill/bounds envs.
