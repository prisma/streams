# Memory Observability: Stream Notifier Version Maps

This counter group tracks retained stream version maps in `StreamNotifier`.

Observability names:
- `tieredstore.mem.leak_candidate.notifier.latest_seq_streams`
- `tieredstore.mem.leak_candidate.notifier.details_version_streams`

Where implemented:
- `src/app_core.ts:600-601` maps these counters.
- `src/notifier.ts:140-153` exposes notifier memory stats.

State source lines:
- `src/notifier.ts:5-8` map declarations (`waiters`, `latestSeq`, `detailsWaiters`, `detailsVersion`).
