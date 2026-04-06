# Memory Observability: GC And High-Water Marks

This group answers two operational questions:

- "is forced GC happening and is it helping?"
- "what was the highest recent value, and when did it happen?"

Observability fields and metrics:

- `/_mem.gc`
- `/_details.runtime.memory.gc`
- `/_mem.high_water`
- `/_details.runtime.memory.high_water`
- `process.gc.forced.count`
- `process.gc.reclaimed.bytes`
- `process.gc.last_forced_at_ms`
- `process.heap.snapshot.count`
- `process.heap.snapshot.last_at_ms`
- `process.memory.high_water.bytes`
- `tieredstore.memory.high_water.bytes`
- `tieredstore.sqlite.high_water`

Where implemented:

- `src/memory.ts`
- `src/app_core.ts`

GC fields:

- `forced_gc_count`
- `forced_gc_reclaimed_bytes_total`
- `last_forced_gc_at_ms`
- `last_forced_gc_before_bytes`
- `last_forced_gc_after_bytes`
- `last_forced_gc_reclaimed_bytes`
- `heap_snapshots_written`
- `last_heap_snapshot_at_ms`

High-water fields:

- `high_water.process`
- `high_water.process_breakdown`
- `high_water.sqlite`
- `high_water.runtime_bytes`
- `high_water.runtime_totals`

Each high-water entry stores:

- `value`
- `at`

Meaning:

- a steadily increasing `forced_gc_count` with near-zero reclaimed bytes means
  the process is under pressure but GC is not the fix
- a high `runtime_totals.disk_cache_bytes` peak with low RSS/file-backed memory
  implies local disk occupancy rather than heap pressure
- a high `process_breakdown.unattributed_anon_bytes` peak is the signal to
  inspect native allocators, SQLite, or fragmentation rather than JS maps alone
