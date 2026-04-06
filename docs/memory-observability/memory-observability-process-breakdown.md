# Memory Observability: Process Breakdown And Unattributed RSS

This group explains the process-level attribution that sits above individual
subsystem counters.

Observability fields and metrics:

- `/_mem.process_breakdown`
- `/_details.runtime.memory.process_breakdown`
- `process.memory.rss.anon.bytes`
- `process.memory.rss.file.bytes`
- `process.memory.rss.shmem.bytes`
- `process.memory.js_managed.bytes`
- `process.memory.js_external_non_array_buffers.bytes`
- `process.memory.unattributed.bytes`
- `process.memory.unattributed_anon.bytes`

Where implemented:

- `src/runtime_memory.ts`
  - `parseLinuxStatusRssBreakdown(...)`
  - `readLinuxStatusRssBreakdown(...)`
  - `buildProcessMemoryBreakdown(...)`
- `src/app_core.ts`
  - snapshot composition
  - metrics emission

Platform behavior:

- on Linux, the breakdown uses `/proc/self/status` fields:
  - `RssAnon`
  - `RssFile`
  - `RssShmem`
- on other platforms, the endpoint still reports
  `unattributed_rss_bytes`, but the anon/file/shmem fields are `null`

Interpretation rules:

- `js_managed_bytes` is `heap_used_bytes + external_bytes`
- `mapped_file_bytes` is the tracked file-backed mmap footprint from runtime caches
- `sqlite_runtime_bytes` is the current SQLite allocator usage from
  `sqlite3_status64()` when available
- `unattributed_rss_bytes` is the remaining RSS after subtracting:
  - JS-managed bytes
  - tracked mapped-file bytes
  - tracked SQLite runtime bytes
- `unattributed_anon_bytes` is the remaining Linux anonymous RSS after subtracting:
  - JS-managed bytes
  - tracked SQLite runtime bytes

These unattributed values are intentionally conservative approximations. They
are useful as "what is still unexplained?" signals, not exact ownership maps.
