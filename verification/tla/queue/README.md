# Native queue models (group `queue`): TLA-046

A native queue's delivery on its stream's pages (S3b part 1 and part 2
step 1,
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
| `Seal`, `sealed` | `shard/seal/steps.rs` (queue streams sealed like streams), the tail's `absorbed` |
| `Prepare`, `prep`, `Available` | `shard/native_queue_submit/prepare.rs` `prepare_queue_pages` (from the stored frontier); `application/queues/delivery.rs` `prepare_lane`; `queue_maintenance.rs` `sweep_queue` |
| `LaneHead`, `LanePass` at `LaneServes` (point 7) | `lane.rs` `nq_lane_run`: prepared below the boundary, then local, gap-free; `claim.rs` `nq_claim_lane` serves only `page.offset == frontier` |

A body is one byte, so `queued_bytes` and `queued_messages` differ by the
cancelled untaken messages the frontier has not passed (I7 as amended).
The client keeps the newest receipt per offset: a stale receipt's
generation is any older one, and the settle compares it with the current
one alone. Part 2 step 1 deletes no page at resolution (`PageAfterEnd`
keeps it; step 3's trim reclaims pages), seals the stream at any boundary
up to the next send, and lets a handler prepare the sealed pages from the
frontier of its time; the window is reused at any later frontier, so a
stale one is checked too. Control 7 serves a prepared page behind the
frontier; the witness `SealedPageLeased` shows a lease served from a
prepared page. With pages kept, control 4 (a scheduled claim clearing its
bit) now breaks `RangeRowsMatch` rather than `NoMessageLost`. I9 (PoisonBounded) is checked as its bound, a body that
never opens is leased at most `MaxAttempts + UnleaseLimit` times, not as a
liveness property.

The clock is bounded, so every run ends where only time could move: `Tick`
is a bare disjunct of `Next` whose only guard is `clock < MaxClock`, and
the three baselines check that as the invariant `TickEnabled` (`clock <
MaxClock => ENABLED Tick`), so a guard added to `Tick` fails them. A
handler's `Prepare` is enabled in every state, so no state is ever a
deadlock. The checks are safety only and run with TLC's deadlock check
off (`deadlock_check: false`, as TLA-045 and TLA-048 do): TLC's deadlock
check could not detect a stuck delivery in this model whether on or off;
TLA-046 makes no claim that a message is eventually delivered or a lease
eventually ended, and I9 is checked as its bound.

Configurations: `small` (2 offsets in one range row, one unreadable body,
one crash), `ranges` (the same over two range rows of one), `expanded` (3
offsets over two range rows of two, a shorter clock, no unreadable body or
crash). The design's 6 offsets did not converge: 3 offsets with clock 0..3
passed 18 million distinct states at depth 18 in 9 minutes with the queue
still growing.

## TLA-047: `QueueReclamation.tla`

A native queue's sealed chunks given back (S3b part 2 step 3,
S3b-DESIGN §4.3, §5.9, §7.3, §7.4, §11.2).

| Model | Code |
|---|---|
| `Seal` (a new object per seal; `Shared` objects also hold another stream's chunk) | `shard/seal/steps.rs`; `transaction/seal.rs` `stage_seal_hint` |
| `Step`, `Resolve`, `Delete`: the needed set shrinks; `Hint` | the committer's queue operations; `transaction/native_queue/hints.rs` `nq_flush_hints` |
| `TrimStart` (the frontier first), `TrimCheck` (references, `Unreferenced` point 1), `TrimCommit` (`CursorAfter` point 10) | `shard/seal/gc/trim.rs` `trim_pass`, `trim_queue`, `queue_refs_in`, `stage_trim_rows`; `native_queue/intervals.rs` `Released` |
| `PurgeRequest`, `PurgeStep` (`PurgeActsOn` point 3), `PurgeOnTrimming` | `gc/purge.rs` `request_purge` (Trimming → Requested, prefix kept), `purge_due`, `purge_stream_at` |
| `ReclaimRows` (`KAtReclaim` point 8), `ReclaimDecrement` | `gc/purge.rs` `reclaim_locked`: F4's order (package 6's change in step 5) |
| `SendScheduled` (a scheduled message's bit references it; `Unreferenced` point 2 counts open deliveries only) | `trim.rs` `queue_refs_in` (the `'d'` rows and the range rows' `sbits`) |
| `Fold`, `Recount` (`EntryDropped` point 6) | `seal/fold.rs` `kept_entries` (the folds and merges); `topology/sentinel.rs` `child_counts` (the split's seed) |
| `Consolidate` (`QueueChunksMove` point 4) | `seal/consolidate.rs` (`is_queue_stream`: a queue read offers no chunk) |
| `PassCapture`, `PassRewrite`, `ClearDebt` (`ClearByGeneration` point 7) | `shard/queue_maintenance.rs` (`capture_debt`, the rewrite, `NqOp::ClearDebt`); `transaction/native_queue/mod.rs` (`nq_debt`, the clear by generation) |
| `ReadStart`/`ReadEnd`, `Collect`, `Crash` | sealed reads' pins (`gc::pin`); the collector; a lost pass |

One chunk per offset: a queue's chunks tile its offsets and never join, so
the released intervals are sets of offsets. The trim and the purge exclude
each other through `refs`; the committer runs beside both. Point 5,
`FrontierAtCommit`, moves the frontier read after the references.

The index (`idx`) holds an entry for each sealed chunk. The folds drop the
released ones, and a split's seed recounts each object from the entries;
both use one view, `EntryDropped` (`kept_entries`). The view covers the
prefix and the `'k'` intervals in every `'H'` state, because `'k'` outlives
Trimming until the purge's last decrement. A split may run between the
purge's passes. Two controls break the view:
- point 6 ignores `'k'`, so the recount counts a chunk the trim gave back;
- its by-state variant reads `'k'` only while Trimming, so a split between
  the purge's request and its reclaim counts the chunk again.

The code does not apply this view yet. The fold reads `'k'` only while
Trimming, and `child_counts` counts every entry, so a split after a trim
rolls back (its children's counts do not sum to the source's). Package 5's
chain fix adds the prefix, and S3b's `'k'` half follows with its red DST.
This obligation is recorded only once both are in.

Point 4 lets a consolidation move a queue's chunk into a new object and
count the old one down without reading `'k'`. An ack's deleted rows are
debt until a collector pass that captured them has rewritten the log; the
clear keeps what was recorded after the capture. Point 7 clears the whole
debt (DebtClearedOnlyAfterACoveringPass). Control 9 (the cleaner's
`still_live` reading a cached `intervals`) comes with part 2 step 4, where
the cleaner lands.

EveryHintHandled is checked as a safety property, not as liveness: a chunk
that is no longer needed and not released is named by a hint after the
cursor, or by the pass in progress.

Configurations:
- `small`: 3 offsets, object 1 shared, one hint and two chunks a pass, one
  read, one crash; scheduled sends, folds, the recount and the consolidation
  point (`Rich`).
- `debt`: small's bounds without reads or scheduled sends; the debt and the
  collector's passes (`Debt`).
- `expanded`: 4 offsets, object 2 shared, two hints a pass, no crash;
  immediate sends only.

Hint ids are bounded by the `HintBound` constraint.
