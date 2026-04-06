# Memory Observability: Top Stream Contributors

This group adds a small current top-N view for stream-scoped contributors.

Observability fields:

- `/_mem.top_streams`
- `/_details.runtime.top_streams`

Current sections:

- `local_storage_bytes`
- `pending_wal_bytes`
- `touch_journal_filter_bytes`
- `notifier_waiters`

Where implemented:

- `src/app_core.ts`
- `src/touch/manager.ts`
- `src/notifier.ts`

Semantics:

- `local_storage_bytes`
  - current per-stream local retained bytes from:
    - retained WAL bytes
    - segment cache bytes
    - routing/exact/lexicon/companion cache bytes
- `pending_wal_bytes`
  - current per-stream retained WAL tail bytes and rows
- `touch_journal_filter_bytes`
  - current per-stream touch journal bloom/filter footprint
- `notifier_waiters`
  - current per-stream read/details waiter counts

This data is intentionally point-in-time and bounded. It is designed for fast
operator triage, not for historical ranking or time-series analysis. The
metrics stream does not emit these top-N entries because doing so would create
unbounded series cardinality by stream name.
