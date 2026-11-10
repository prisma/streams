# Native queue models (group `queue`): TLA-046

A native queue's delivery on its stream's page rows (S3b part 1,
`~/.streams-k2/analysis/tuning-2026-11-03/S3b-DESIGN.md` §4-§6, §11.1). Each
result is a bounded model check of the recorded instance under the recorded
assumptions; the model does not prove that the Rust code refines it.

## TLA-046: `QueueDelivery.tla`

| Model | Code |
|---|---|
| one action = one durable group; `Crash` loses the handlers' replies | `commit_plan.rs` `DurableEffects::native_acks` (ASM-QUEUE-REPLY-DURABLE) |
| `SendImmediate`, `SendScheduled` | `shard/transaction/native_queue/send.rs` (the page, the range row's `skip`/`sbits`, `'a'`) |
| `frontier`, `skip`, `cancel`, `sbits`, `opn`, `sch` | `StateRow.frontier`; `native_queue/range.rs` `RangeRow` |
| `LanePass` (`HeadLive`, mutation point 2) | `native_queue/frontier.rs` `at_frontier` (KANI-110); `claim.rs` `nq_frontier_step`; `sweep.rs` `nq_sweep_lane` |
| `Claim`: the least-keyed due candidate | `claim.rs` `nq_candidates`, `nq_claim_lane` (`LaneOpen`, point 1), `nq_claim_scheduled` (`ScheduledClaimKeepsSbits`, point 4), `nq_claim_retry` |
| `HandOut`, `FailUnreadable`, `UnleaseInternal` (`UnleaseReleases`, `UnleaseCounts`, points 6 and 8) | `application/queues/delivery.rs`; `settle.rs` `SettleKind::Unreadable`, `nq_unlease` |
| `Ack`, `Fail`, `RetryNow`, `Release`, `Extend` (`GenOK`, point 5) | `settle.rs` `nq_settle_one`, `nq_end_lease`, `nq_retry_or_fail` |
| `Cancel` (`CancelMarks`, point 3) | `admin.rs` `nq_cancel_one` |
| `SweepLease`, `SweepRetry`, `ExpireScheduled`, `ExpireFailure`, `Discard` | `sweep.rs`; `deliveries.rs` `nq_end_message`, `nq_fail` |

A body is one byte, so `queued_bytes` and `queued_messages` differ by the
cancelled untaken messages the frontier has not passed (I7 as amended).
The client keeps the newest receipt per offset: a stale receipt's
generation is any older one, and the settle compares it with the current
one alone. Part 1 has no handler-prepared window: the committer reads the
lane's pages itself, so S3b-DESIGN §11.1's `Prepare` and its control 7
arrive with part 2. I9 (PoisonBounded) is checked as its bound, a body that
never opens is leased at most `MaxAttempts + UnleaseLimit` times, not as a
liveness property.

The clock is bounded, so every run ends where only time could move: `Tick`
is enabled below `MaxClock`, so the only deadlock is that end. The checks
are safety only and run with TLC's deadlock check off (`deadlock_check:
false`, as TLA-045 and TLA-048 do). `Tick`'s only guard is `clock <
MaxClock`, so TLC's deadlock check could not detect a stuck delivery in
this model whether on or off; TLA-046 makes no claim that a message is
eventually delivered or a lease eventually ended, and I9 is checked as its
bound.

Configurations: `small` (2 offsets in one range row, one unreadable body,
one crash), `ranges` (the same over two range rows of one), `expanded` (3
offsets over two range rows of two, a shorter clock, no unreadable body or
crash). The design's 6 offsets did not converge: 3 offsets with clock 0..3
passed 18 million distinct states at depth 18 in 9 minutes with the queue
still growing.
