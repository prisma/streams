# TLA-018-F3: a stale applied cursor was accepted once the new owner's tail passed it

**Classification:** production defect (model counterexample, reproduced on the
real code). **Fixed** by "A provisional read cursor proves the history it
continues, or answers an explicit resync" (`55881d7`).
**Model checks:** `TLA-018/baseline-applied-unfiltered-expanded` and
`TLA-018/baseline-applied-keyed-expanded` now check `ExactDurablePrefix` in
the shapes where the applied suffix is lost on the ownership move and
rewritten; the former known-defect check `known-defect-stale-applied-cursor`
is folded into the first. `TLA-018/nc-no-continuation-check`
(`verification/tla/history/MC_ReadCompose_nc_no_continuation_check.cfg`)
restores the pre-fix acceptance and must keep violating `ExactDurablePrefix`.
`TLA-018/witness-StaleContinuationResynced` shows the refusal after the
replacement tail has passed the cursor, and
`TLA-018/witness-ContinuedAcrossMove` shows an owner change that lost nothing
continuing without resync.
**Permanent regressions:** `dst::dst_tests::reads_applied_history`
(`src/dst/tests/reads_applied_history.rs`):
`a_stale_unfiltered_continuation_is_refused_after_the_replacement_tail_passes_it`,
`a_stale_keyed_continuation_is_refused_after_the_replacement_tail_passes_it`
and `an_owner_change_that_loses_nothing_keeps_the_continuation`; the
continuation algebra in `application::read_continuation::tests`.

## The defect

Before the fix the only guard on an applied read's start was, in
`ReadService::execute_read`:

```rust
if command.visibility == Deliver::Applied && start > end {
    return Err(ReadFailure::CursorBeyondTail);
}
```

`end` is the current owner's end. A `KIND_KEY_V2` product cursor carried no
owner identity and no durable frontier, so the guard fired only while the new
owner's tail was still below the stale cursor.

## Minimized schedule

The model's counterexample (`deliver=applied`, unfiltered, one ownership move;
`verification/tla/history/evidence/TLA-018_applied_stale_cursor.trace.txt`,
recorded on the pre-fix model, the behaviour `nc-no-continuation-check` now
reproduces):

1. The old owner has record 0 durable and appends record 1, applied only.
2. A page delivers record 0. The session cursor and the durable cursor are 1.
3. The next page starts on the old owner.
4. Ownership moves, and the applied record 1 is lost with the old memtable.
5. The new owner appends a different record 1 and a record 2, and the new
   record 1 becomes durable.
6. The page on the old owner completes from its frozen view and delivers the
   lost record 1 as pending. The session cursor is 2; the durable cursor
   stays 1.
7. The client continues from the session cursor 2. The new owner's end is 3,
   not below it, so the cursor was accepted.
8. The page delivers record 2, and the durable cursor becomes 2. Offset 1 is
   now inside the promised prefix, but the client holds the lost record, not
   the durable one.

On the real code, before the fix, the restarted server answered `200` with
`[{"n":20}]` and a durable cursor of 3 instead of refusing the cursor; the
client never received the durable record `{"n":10}` at offset 1.

## The fix

- A page that ends past the durable frontier returns a provisional
  continuation (`Continuation`, `src/application/read_continuation.rs`): the
  writer history that served it (the shard DB prefix and the SlateDB writer
  epoch the engine claimed at open, `ShardEngine.writer_epoch`), the durable
  recovery offset, the digest start and a keyed, chained digest of the records
  the client observed from the digest start. Product reads carry it as a
  `KIND_KEY_V3` cursor; `Prisma-Durable-Cursor` stays V2.
- `check_entry_start` (`src/application/read_request.rs:693-707`) verifies a
  continuation before the read: the same writer history continues; another
  writer continues only if a re-read of `[from, at)` holds exactly the
  observed records (`verify_continuation`, `:716-751`). Otherwise the read
  answers `409 cursor_beyond_tail` with reason `history_replaced` and the
  durable recovery cursor.
- A V2 token is a durable position. With `deliver=applied`, one beyond the
  durable frontier is refused with `409`.

In the model: `peng`, `pfrom` and `pdig` are the continuation's history,
digest start and digest (`EndPage` follows `Continuation::after_page`);
`StartAllowed` is `check_entry_start`, `ObservedIn` is
`Continuation::observed_in` over the current engine's applied view, and
`RResync` is the refusal and the client's resume from the recovery cursor.

## Replaying the regressions

```bash
cargo test --lib dst::dst_tests::reads_applied_history
cargo test --lib application::read_continuation
```

The two stale-continuation tests hold only the shard DB's WAL `PUT` so that a
provisional record is delivered, then let a second instance fence the owner
and append different records at the same offsets. They cover the product
read (unary, durable mode and SSE), both raw renderings and the relay:
resuming from the recovery cursor delivers the replacement records and the
durable cursor ends at 3. They fail on the unfixed code.

## What stays open

A V2 session cursor minted by a server before this change over a suffix that
was later lost cannot be detected once the durable frontier passes it: it
carries no history, and the fix treats a V2 token at or below the frontier as
a durable position (`stale_continuation_after_replacement` pins this, and
`docs/GUIDE-COMPOSER.md` documents it). Restoring the object store to an
older snapshot can repeat a writer epoch (ASM-HISTORY-FENCED-VIEW). A session
started at `now` resynchronises after any owner change until the frontier
passes its start; the model has no `now` start.
