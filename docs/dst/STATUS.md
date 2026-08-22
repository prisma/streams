# DST expansion — implementation status

Spec baseline: 681ea0f (the catalogue also names later rounds' classes).
Revised after the round-13 review: statuses below distinguish
mechanism-forced tests from invariant-style ones honestly.

## Workstream 2 (L1-now P0)

| scenario | status | test / mechanism |
|---|---|---|
| DUR-002 same-group duplicate + failed write | **forced** | `a_failed_group_write_fails_its_duplicate_too` — hold-commit gate + enqueue-counter entered-proof compose ONE group; BOTH requests must receive the group failure |
| DUR-004 same-group idempotent close + failed write | **forced** | `a_failed_group_fails_the_close_and_its_retry_together` — close-only + exact retry in one failed group; nothing publishes sealing |
| DUR-005 applied-not-durable close retry | landed (R11) | `idempotent_successes_wait_for_durability` (dispatch gate) |
| DUR-006 reuse rejection + failed group | **forced** | `a_reuse_verdict_dies_with_its_failed_group` — different bodies, one group, both fail; reuse conflicts only once the original is durable |
| DUR-008 Stream-Seq conflict barrier | invariant | `a_stream_seq_verdict_is_grounded_in_durable_state` (a 409 must have durable ground) + the R11 barrier reverts |
| SEL-019 merge phase-B under sealing | **entered-proof** | `merge_phase_b_declines_under_sealing` — `publish_parked_count` proves phase B reached before the claim is planted |
| SEL-021 close+fence one failed group | **forced** | `a_fence_in_a_failed_group_reports_failure_not_closed` — hold-commit composes [close, fence] into one group; fence answers Err/false, never closed=true |
| SEL-022 prior-group non-durable fence | **partial** | dispatch-gate form only (post-durability response dispatch). A true remote-watermark pause needs the simulator substrate (#108) — relabeled, not claimed |
| SEL-026 fence never expires | landed (R11) | expiry code removed; `a_fence_outlives_the_maintenance_sweep` |
| SEL-027 concurrent finals, different coordination | landed | `concurrent_finals_with_different_coordination_do_not_share_a_claim` |
| CRT-007 create + failed seed write | landed (single-creator) | `create_replay_recovers_from_a_failed_initial_write`; the two-joiner same-group variant remains open (#108 catalogue) |
| FRK-013 child deleted before source ref | **crash-safe** | non-crash: creator pre/post child checks release inline. CRASH: the tombstone RETAINS its debt on an inconclusive release; `a_crashed_creators_late_reference_is_repaired_by_delete_retry` leaves the creator parked forever and repairs via the ordinary DELETE retry |
| FRK-016 cascade vs new child | covered by construction | release+decide in ONE CAS (`release_fork_ref`); `fork_creation_and_source_deletion_serialize` now awaits `parked_delete_count` (no sleeps) |
| QUE-003 phantom lease | **forced** | `a_failed_queue_write_leaves_no_phantom_leases` — the selector covers queue ops, so the pull's group deterministically trips |
| QUE-004 phantom settlement | **forced** | `a_failed_settle_leaves_no_phantom_acks` — failed settle; the SAME tokens settle on retry (acked == 3) |

## Harness capabilities

- `fail_next_group_for(identity)` — one-shot group-write failure that
  flows through `send_group_failure`, the SAME function the real
  write-error arm uses (they cannot diverge). Trips only for groups
  carrying a CLIENT append or queue op for the armed identity.
  `group_failures_tripped()` = entered-proof.
- `test_hold_commit()` — parks the committer after it takes one op,
  before it gathers: release with N ops queued ⇒ ONE group with all
  of them. With `appends_enqueued()` as the queue-arrival proof, group
  composition is deterministic. (Safe under the parallel suite: rigs
  are per-test, so a held gate never delays another test's engine.)
- Parks with entered-proof counters: init-seed, fork pre/post source-
  ref, product seal claim/final, close-enqueue (one-shot), append-
  enqueue, delete-decision, create-ready, scaler publish.

## Round-4 addition: the seal lifecycle crash/resume matrix

The spec's core rule — crash every multi-step operation at each
boundary, then require convergence — applied to the collection-seal
transition (the one that produced an unexplained intermittent 500):

| boundary | mechanism | test |
|---|---|---|
| claim vs benign descriptor-write conflict | `fail_next_put` (registry one-shot put failure = etag-conflict stand-in) | `a_benign_descriptor_conflict_never_fails_a_seal` — the claim RETRIES from a fresh read; root cause of the intermittent, fixed by cas_update_retry on both seal CAS sites |
| after every segment close is durable, before SEALED publication | **forced** flag `StopBeforeSealedPublish` | `a_seal_interrupted_before_publication_resumes_on_retry` — Sealing + closed segments observable, plain retry completes, subscriber sees exactly ONE sealed control then EOF |
| concurrent plain seals | natural race, no injection | `concurrent_plain_seals_serialize_into_one_terminal_transition` — both succeed into ONE terminal transition; single sealed control; appends refused afterwards |
| transient COMMIT-GROUP failure x3 | **forced** `fail_next_group_for(identity)` on the close group | `seal_converges_through_transient_commit_group_failures` — entered-proof per round, retryable resumable failures, bounded-retry convergence to terminal Sealed with subscriber protocol intact |

## Next (tracked, #108)

Semantic failpoint registry (one arm → await_entered → release
surface over all of the above), then the deterministic runtime
extraction: injected time/entropy, scenario DSL, choice traces,
shrinking — the remaining catalogue including SEL-022's true
remote-watermark pause and the CRT-007 two-joiner variant.
