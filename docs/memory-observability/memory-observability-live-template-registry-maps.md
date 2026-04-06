# Memory Observability: Live Template Registry Maps

This counter group tracks in-memory map/set cardinality inside the live-template registry.

Observability names:
- `tieredstore.mem.leak_candidate.live_template.last_seen_entries`
- `tieredstore.mem.leak_candidate.live_template.dirty_last_seen_entries`
- `tieredstore.mem.leak_candidate.live_template.rate_state_streams`

Where implemented:
- `src/app_core.ts:596-598` maps these counters.
- `src/touch/live_templates.ts:114-119` exposes `LiveTemplateRegistry.getMemoryStats()`.

State source lines:
- `src/touch/live_templates.ts:100-104` in-memory containers (`lastSeenMem`, `dirtyLastSeen`, `rate`).
