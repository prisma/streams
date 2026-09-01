# Commit-Order Characterization (WP-00 deliverable 5)

The five commit-order properties the restructuring must preserve, each
mapped to the concrete existing tests that pin it. The mapping is the
characterization: any WP-11 PR that changes group collection, batch
preparation, applied publication or durable dispatch must show these
tests still green, unchanged in meaning.

| Property | Pinning tests | Scenario IDs |
|---|---|---|
| Write failure publishes no applied state | `a_failed_group_write_fails_its_duplicate_too` (src/dst/dst_tests.rs:14825), `a_reuse_verdict_dies_with_its_failed_group` (14921), `a_failed_group_fails_the_close_and_its_retry_together` (15035), `a_failed_queue_write_leaves_no_phantom_leases` (15938), `a_failed_settle_leaves_no_phantom_acks` (16160); production mechanism: `send_group_failure` replaces every provisional result (src/shard.rs:2399-2420) | DUR-002, DUR-004, DUR-006, QUE-003, QUE-004 |
| Applied publication precedes durable response dispatch | `the_gather_window_waits_for_ack_dispatch` (1728) | DUR-001 |
| Same-group / applied-not-durable duplicate and conflict results stay barriered | `a_failed_group_write_fails_its_duplicate_too` (14825), `idempotent_successes_wait_for_durability` (round-10 block, 14422), `a_stream_seq_verdict_is_grounded_in_durable_state` (15309) | DUR-002, DUR-003, DUR-006, DUR-008 |
| Billing and maintenance rows share the intended batch | `billing_meta_is_exact_durable_and_ackable` (25000) — meta row, usage version and dirty marker exact and durable with the append; `telemetry_crash_points_and_cost_gates` (25494), `usage_pipeline_end_to_end_exactly_once` (25167); maintenance dirty marker clears in the same committer batch (comment/assertion at 3959), restart rediscovery `untouched_streams_absorb_after_restart` (3870), `pending_summary_clears_on_shard_close` (4714) | RES-004, INV-BILL-ATOMIC |
| Fence-only responses wait on the correct barrier | `a_fence_waits_for_durability_before_reporting_closed` (round-9 fence-is-a-durability-barrier block, 13798-14321) | SEL-022 (labelled Strengthen — see gap below) |

## Recorded gaps (already tracked by the catalogue)

- **DUR-005 (Strengthen)** — applied-but-not-durable close retry: partial
  coverage only; no dedicated test holds remote durability after a
  physical close is applied and forces the exact close retry to wait.
- **SEL-022 (Strengthen)** — fence-as-barrier: dispatch-gate leg pinned;
  the full fence-only barrier schedule is not driven.
- **SEC-002 (Existing/static+L3)** — no in-suite test sends an
  unauthenticated oversize body and asserts rejection before body read
  (nearest: `inflight_admission_answers_only_after_authentication`,
  36349). The product-side order is pinned statically by the wrapper
  order in `src/http.rs:2287-2376`.

These are catalogue-owned follow-ups (the catalogue's own Strengthen/L1
labels), not regressions introduced by WP-00; they are recorded here so
the WP-11 implementer does not mistake the absence of a leg for
permission to change the behavior.
