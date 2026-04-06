# Memory Observability: Touch Journal Default Footprint

This counter group tracks the estimated memory footprint from in-memory touch journal Bloom filters.

Observability names:
- `tieredstore.mem.leak_candidate.touch.journals.filter_bytes_total`
- `tieredstore.mem.leak_candidate.touch.journal.default_filter_bytes`

Where implemented:
- `src/app_core.ts:586-587` maps both counter names.
- `src/touch/manager.ts:213-235` sums journal filter bytes across active journals.
- `src/touch/touch_journal.ts:279-281` reports per-journal filter byte size.

Default sizing source lines:
- `src/touch/spec.ts:102-106` formula is `4 * 2^filterPow2` bytes.
- `src/touch/spec.ts:105` default `filterPow2` is `22`.
- `src/touch/manager.ts:875` default journal creation uses `filterPow2 ?? 22`.
