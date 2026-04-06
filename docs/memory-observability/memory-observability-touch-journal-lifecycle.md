# Memory Observability: Touch Journal Lifecycle

This counter tracks how many per-stream touch journals are currently resident in process memory.

Observability name:
- `tieredstore.mem.leak_candidate.touch.journals.active_count`

Where implemented:
- `src/app_core.ts:584` maps this counter name.
- `src/touch/manager.ts:213-235` computes `journals` from the manager map.
- `src/touch/manager.ts:869-883` creates journals lazily via `getOrCreateJournal`.

State source lines:
- `src/touch/manager.ts:133-143` manager holds journal map and stream-scoped state maps.
- `src/touch/manager.ts:199-208` journals are only bulk-cleared on manager `stop()`.
