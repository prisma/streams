# Memory Observability: Touch Manager Stream Maps

This counter group tracks stream-keyed maps in `TouchProcessorManager` that can grow with stream churn.

Observability names:
- `tieredstore.mem.leak_candidate.touch.maps.fine_lag_coarse_only_streams`
- `tieredstore.mem.leak_candidate.touch.maps.touch_mode_streams`
- `tieredstore.mem.leak_candidate.touch.maps.fine_token_bucket_streams`
- `tieredstore.mem.leak_candidate.touch.maps.hot_fine_streams`
- `tieredstore.mem.leak_candidate.touch.maps.lag_source_offset_streams`
- `tieredstore.mem.leak_candidate.touch.maps.restricted_template_bucket_streams`
- `tieredstore.mem.leak_candidate.touch.maps.runtime_totals_streams`
- `tieredstore.mem.leak_candidate.touch.maps.zero_row_backlog_streams`

Where implemented:
- `src/app_core.ts:588-595` maps these counters.
- `src/touch/manager.ts:213-235` computes current map sizes.

Map declarations:
- `src/touch/manager.ts:136-143` stream-keyed map fields.
