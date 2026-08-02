# DST expansion — implementation status

Spec baseline: 681ea0f (the catalogue also names later rounds' classes).

## Workstream 2 (L1-now P0) — landed

| scenario | status | test / mechanism |
|---|---|---|
| DUR-002 same-group duplicate + failed write | landed | `a_failed_group_write_fails_its_duplicate_too` (order-independent: the forbidden outcome is a duplicate verdict grounded in failed staging) |
| DUR-004 idempotent close + failed write | landed (folded) | `a_fence_in_a_failed_group_reports_failure_not_closed` covers the close side; barriered close proven in `idempotent_successes_wait_for_durability` |
| DUR-005 applied-not-durable close retry | landed (R11) | `idempotent_successes_wait_for_durability` |
| DUR-006 reuse rejection barrier | landed (R11 + group-fail variant) | `state_dependent_conflicts_wait_for_durability`, `a_failed_group_write_fails_its_duplicate_too` |
| DUR-008 Stream-Seq conflict barrier | landed | `a_stream_seq_verdict_is_grounded_in_durable_state` (a 409 must have durable ground: the exact probe must still conflict) |
| SEL-019 merge phase-B under sealing | landed | `merge_phase_b_declines_under_sealing` (red: publication under a planted claim) |
| SEL-021 close+fence + failed write | landed | `a_fence_in_a_failed_group_reports_failure_not_closed` (fence answers Err or closed=false, never closed=true; intent survives ambiguity; exact retry recovers) |
| SEL-022 prior-group applied, durability paused | landed (R9) | `a_fence_waits_for_durability_before_reporting_closed` via the dispatch gate |
| SEL-026 fence never expires over a queue | landed (R11) | expiry code REMOVED; `a_fence_outlives_the_maintenance_sweep` |
| SEL-027 concurrent finals, different coordination | landed | `concurrent_finals_with_different_coordination_do_not_share_a_claim` |
| CRT-007 create + failed initial write | landed | `create_replay_recovers_from_a_failed_initial_write` (init retained; exact replay delivers seeds exactly once) |
| FRK-013 child deleted before source ref | landed — FOUND A REAL LEAK | the readiness give-back filtered on the LOCAL snapshot's fork_id, which the stamp CAS never wrote back; the source stayed pinned by a dead child. Fixed (stamp reflected into the snapshot); test's first run was the red run |
| FRK-016 cascade vs new child | covered by construction | `release_fork_ref` removes the ref AND decides the source's fate in ONE CAS (the split form was the historical bug); serialization pinned by `fork_creation_and_source_deletion_serialize` |

## New harness capability

`ShardEngine::fail_next_group_for(identity)` — one-shot deterministic
group-write failure, tripping ONLY for groups that carry a CLIENT
append for the armed identity (maintenance ops — absorber, trim —
touch the same locals and must not consume the arm; the first draft
tripped on those under full-suite parallelism). `group_failures_tripped()`
is the entered-proof. Failures flow through the exact lines a real
write error takes: every promise of the group — acks, duplicates,
idempotent closes, refusals, fence reports — fails together.

New parks: `park_init_before_seed` (arm the failure with the real
descriptor identity, deterministically), `park_fork_before_source_ref`
(the FRK-13 window).

## Next (tracked)

Workstream 1/3+: consolidate all pause mechanisms into the semantic
failpoint registry (arm → await_entered → release); then the
deterministic runtime extraction toward the whole-system simulator and
the remaining catalogue. No further timing-assisted race tests.
