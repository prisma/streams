# Memory Observability: Mmap Pinned Caches

This counter group tracks the mmap-backed cache entries that become pinned and can prevent full cache-budget eviction.

Observability names:
- `tieredstore.mem.leak_candidate.segment_cache.pinned_entries`
- `tieredstore.mem.leak_candidate.lexicon_file_cache.pinned_entries`
- `tieredstore.mem.leak_candidate.companion_file_cache.pinned_entries`
- `tieredstore.mem.leak_candidate.routing_run_disk_cache.pinned_entries`
- `tieredstore.mem.leak_candidate.exact_run_disk_cache.pinned_entries`

Where implemented:
- `src/app_core.ts:579-583` maps endpoint/metrics counter names to runtime counts.
- `src/app.ts:245-257` publishes pinned-entry runtime counts into memory subsystem `counts`.
- `src/index/indexer.ts:221-240` exposes routing run-disk cache pinned/mapped stats.
- `src/index/secondary_indexer.ts:194-214` exposes exact-index run-disk cache pinned/mapped stats.

Behavior source lines:
- `src/segment/cache.ts:29` pinned key set declaration.
- `src/segment/cache.ts:113-138` pinned keys are added on mapped reads.
- `src/segment/cache.ts:191-207` removals/eviction skip pinned keys.
- `src/index/lexicon_file_cache.ts:43` pinned key set declaration.
- `src/index/lexicon_file_cache.ts:103-117` pinned keys are added on mapped loads.
- `src/search/companion_file_cache.ts:48` pinned key set declaration.
- `src/search/companion_file_cache.ts:116-137` pinned keys are added on mapped loads.
