# Memory Observability: SQLite Runtime

This group adds actual SQLite allocator/runtime counters alongside the existing
configured cache budgets.

Observability fields and metrics:

- `/_mem.sqlite`
- `/_details.runtime.memory.sqlite`
- `runtime_bytes.sqlite_runtime.*`
- `runtime_counts.sqlite_*`
- `tieredstore.sqlite.memory.used.bytes`
- `tieredstore.sqlite.memory.high_water.bytes`
- `tieredstore.sqlite.pagecache.used`
- `tieredstore.sqlite.pagecache.high_water`
- `tieredstore.sqlite.pagecache.overflow.bytes`
- `tieredstore.sqlite.pagecache.overflow.high_water.bytes`
- `tieredstore.sqlite.malloc.count`
- `tieredstore.sqlite.malloc.high_water.count`
- `tieredstore.sqlite.open_connections`
- `tieredstore.sqlite.prepared_statements`

Where implemented:

- `src/sqlite/runtime_stats.ts`
- `src/sqlite/adapter.ts`
- `src/app.ts`
- `src/app_local.ts`
- `src/app_core.ts`

Data source:

- Bun builds use `sqlite3_status64()` via `bun:ffi`
- adapter-level connection and prepared-statement counts come from
  `src/sqlite/adapter.ts`

Current counters:

- `memory_used_bytes`
- `memory_highwater_bytes`
- `pagecache_used_slots`
- `pagecache_used_slots_highwater`
- `pagecache_overflow_bytes`
- `pagecache_overflow_highwater_bytes`
- `malloc_count`
- `malloc_count_highwater`
- `open_connections`
- `prepared_statements`

Prepared-statement semantics:

- most literal SQL now resolves through a per-connection prepared-statement
  cache in `src/sqlite/adapter.ts`
- repeated `db.query(<same sql>)` calls reuse one prepared statement per
  connection instead of preparing a fresh statement each time
- iterator-style WAL scans and WAL GC `DELETE ... RETURNING` sweeps use fresh
  prepared statements per call and finalize them after use, because Bun's
  sqlite iterator path is not safe to reuse across repeated scans/sweeps
- this repository's SQLite policy is "prepared statements only" for runtime
  reads and writes:
  - Bun [`Statement`](https://bun.com/reference/bun/sqlite/Statement)
    instances are the supported unit of repeated SQL execution
  - `db.exec(...)` is for one-shot schema/bootstrap work, not recurring runtime
    queries
  - any statement that is not retained by the adapter's bounded cache must be
    finalized immediately after use
  - Bun documents that `finalize()` frees the statement's resources and calls
    `sqlite3_finalize`; treat that as mandatory lifecycle management, not an
    optional cleanup step
- `prepared_statements` therefore means:
  - cached connection-local statements that remain prepared
  - plus any currently live fresh iterator statements

Operational interpretation:

- if `prepared_statements` climbs once during warmup and then stabilizes, that
  is expected
- if it continues climbing with no corresponding increase in supported query
  surface, the process may still be discovering uncached dynamic SQL or leaking
  statement wrappers
- a healthy app should usually need only a small live working set of prepared
  statements; about a dozen at steady state is the preferred shape
- materially higher counts should be assumed suspicious until one explicit,
  bounded statement cache explains them

Important limitation:

- these are process-global SQLite allocator counters, not per-connection
  `sqlite3_db_status()` counters
- Bun and Node sqlite surfaces do not expose the raw `sqlite3*` handle needed
  for `sqlite3_db_status64()`
- so this observability answers:
  - "is SQLite itself using a lot of memory in this process?"
- but not:
  - "which exact database connection owns those bytes?"

That limitation is intentional and documented rather than hidden. The endpoint
labels this source as `sqlite3_status64` when available and `unavailable`
otherwise.
