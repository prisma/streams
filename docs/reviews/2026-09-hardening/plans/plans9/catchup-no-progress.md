# Item 86: an empty catch-up page must wait before it is read again

Tree: `slate` @ `fba7b844` (merge base for the gates: `origin/slate` = `fb18840d`).
Every line number below is HEAD `fba7b844`. Other items may land first, so find
each anchor by its content before editing.

Two commits:

1. **Refactor (no behaviour change).** A near-verbatim move of the two
   "read advanced nothing" arms of `serve`'s catch-up `match` into a new
   `src/sse/session/catch_up.rs`, as a typed verdict `Stall { Cutoff, Failed, NoProgress }`.
   This shrinks `serve`, the scope of the five ratcheted exceptions, so the fix has
   room to land.
2. **Fix.** An empty page counts `FEED_NO_PROGRESS` and waits the same 100 ms a failed
   read waits. The pass then reads the same bound again from a fresh snapshot
   (`continue 'handoff`), and it no longer detours through the live loop's `Lagged`
   arm. The red DST test lands in the same commit.

---

## 1. Problem (verified on the current tree)

### 1.1 The catch-up read and its "advanced nothing" arms

The code is in `src/sse/session.rs`. The reviewer's lines 362-375, 442-446 and 500-516
are stale; the code is now at 352-446 and 481-498.

The pass snapshots its source once, then reads privately below the bound:

```rust
352        'handoff: loop {
...
359            let csrc = feed.current_source();
360            while cursor < catchup_bound {
361                if lease_watch.revoked(&task_state) {
...
368                let read = tokio::select! {
369                    r = csrc.read_batch(cursor, 1024 * 1024) => r,
370                    _ = tx.closed() => return,
371                };
372                match read {
373                    Ok(batch) if batch.scan_to > cursor => {
```

A page that advanced nothing leaves the pass at once, with no wait:

```rust
424                    // This source's spans are exhausted below the bound
425                    // (a swap happened mid-catch-up): the live loop
426                    // re-snapshots and, if the ring moved, re-catches-up
427                    // through the 'handoff path.
428                    Ok(_) => break,
```

A failed read, by contrast, waits 100 ms and then retries on the same snapshot:

```rust
440                        // Source failure mid-catch-up: bounded backoff,
441                        // then retry the SAME bound — never a hot loop
442                        // (finding 6 discipline applies here too).
443                        crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
444                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
445                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
```

### 1.2 Where the `break` goes: the Lagged arm sends it straight back

```rust
481                match feed.take_visible(cursor) {
482                    Take::Lagged { floor } => {
483                        if reached_live {
...
494                        crate::sse::auth::sse_stats::FEED_CATCHUP_RETRIES
495                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
496                        catchup_bound = feed.head();
497                        continue 'handoff;
```

`take_visible` answers `Lagged` whenever the cursor is below the floor
(`src/sse/feed.rs:817-821`):

```rust
817    pub(crate) fn take_visible(&self, cursor: u64) -> Take {
818        let mut st = self.st.lock().unwrap();
819        if cursor < st.floor {
820            return Take::Lagged { floor: st.floor };
```

A feed is born with `floor == head == frontier` (`src/sse/feed.rs:559`, `568-569`:
`let head = src.frontier();` … `head, floor: head,`). A solo drive also keeps
`floor = head`. So the first subscriber of a feed that catches up from an old cursor
is below the floor from its first pass.

### 1.3 Trace of the spin

Setup: a session at `cursor = 0`, a feed with `head = floor = 3` and
`reached_live = false`, and a read at 0 that returns an empty page.

1. `read_batch(0)` returns `Ok(SourceBatch{scan_from:0, scan_to:0, records:[], completed:false})`.
2. The guard at `:373` is false, so the `Ok(_) => break` arm at `:428` runs.
3. In the live loop, `take_visible(0)` returns `Lagged{floor:3}`.
4. `FEED_CATCHUP_RETRIES` goes up by one, `catchup_bound` becomes 3, and the loop runs
   `continue 'handoff`.
5. The source is snapshotted again and the read at 0 runs again, back to step 1.

Nothing in a lap waits. The lap's only `.await` is the read. The rest is
`revoked`, `Box::pin` of two unpolled futures, `source_snapshot`, `Notified::enable`,
`take_visible` and `head`. When the read completes on its first poll (a `FakeSource`
read, or a local page served from memory), the lap never returns `Pending`. The task
then holds a worker thread and bumps `FEED_CATCHUP_RETRIES` once per lap, at a rate
of about 10^5 per second, until the hole clears.

The spin is confirmed. When the cursor is at or above the floor there is no spin: the
live loop either serves the cursor from the ring, or drives at the head, and a failed
drive is bounded by `ReadRetry` (item 33).

### 1.4 What an empty catch-up page actually is

The `break` comment names a cause that cannot occur ("exhausted below the bound (a
swap happened mid-catch-up)"). The reasons:

- `catchup_bound` is always a feed head. It is `join_head`, captured under the
  registry lock, or `feed.head()` at `:496`. `csrc` is snapshotted after it (`:359`).
- A head only advances to a driver read's `scan_to`, and `scan_to` is at most that
  source's frontier (`src/sse/source.rs:87,101-104`, `684-687`).
- `install_source` installs only strict compatible extensions
  (`src/sse/feed.rs:708-727`). An extension's frontier is at least as large as its
  predecessor's, and every frontier is monotone.
- Therefore `csrc.frontier() >= catchup_bound > cursor` at every catch-up read. An
  empty page is always a partial page below the frontier: the read found a hole it
  could not explain yet.

The hole cases in `src/application/read.rs`:

- `:259-262`: "A hole the boundary does not explain: never emit it as consumed. Drop
  the tail and report the honest partial."
- `:216-220`: a history range that cannot prove coverage yet.
- The `for _ in 0..16` bound falling through.
- In `LineageSource`, a partial page inside a sealed span (`src/sse/source.rs:660-663`).

Because `completed` is false there, the same page on a live drive is `DriveOutcome::NoProgress`
(`src/sse/feed.rs:931-942`), which already counts `FEED_NO_PROGRESS` and waits on
`ReadRetry`. Only the catch-up path spins.

### 1.5 Does item 33's `ReadRetry` already cover this path?

**No.** `ReadRetry` (`src/sse/session/read_retry.rs`) is armed in exactly one place,
the live drive's `Some(DriveOutcome::NoProgress | DriveOutcome::SourceFailed) => read_retry.failed()`
(`session.rs:807-809`). It is consumed in one place, the live park's sleep branch
(`session.rs:844`). The catch-up pass calls `csrc.read_batch` directly (`:369`) and
never touches it.

**Should it be reused?** Not in this item:

- `ReadRetry`'s `owed` flag models a park that other wakes (version, generation, source,
  `tx`) may end first, with the next park consuming the retry. The catch-up pass has
  no park, so `failed(); nap()` would run back to back and `owed` would do nothing.
- Reusing it would move the catch-up's 100 ms retry for failed reads to 250 ms doubling
  up to 5 s. That is a policy change outside item 86 (decision D2).
- It would also share the per-cursor doubling state between the two phases.

The pass keeps its own existing 100 ms discipline. `catch_up::RETRY` now owns it for
both stall shapes.

### 1.6 Complete list of use sites (grep on HEAD)

| Symbol | Sites |
| --- | --- |
| private catch-up read | `session.rs:369` (the only production `read_batch` call besides the driver at `feed.rs:905`; test fixture `feed/tests/fixture.rs:86`) |
| `Ok(_) => break` (catch-up no-progress) | `session.rs:428` only |
| `Take::Lagged` | produced by `feed.rs:820`; consumed only at `session.rs:482-498` (tests: `feed/tests.rs:326,810`) |
| `FEED_CATCHUP_RETRIES` | def `sse/auth.rs:54`; the only increment is `session.rs:494`; `/v1/debug/load` `http.rs:917` (`"catchup_retries"`); tests `livefeed_basics.rs:763,792` (assert `>` only), `livefeed_swap.rs:423` (diagnostic only) |
| `FEED_NO_PROGRESS` | def `sse/auth.rs:25`; the only increment is `feed.rs:935` (drive, `!completed` only); `http.rs:899` (`"no_progress"`) |
| `FEED_SOURCE_FAILED` | def `sse/auth.rs:27`; `session.rs:443` (catch-up), `feed.rs:926` (drive read), `feed/drive.rs:177` (refresh); `http.rs:900`; `livefeed_swap.rs:421` (diagnostic) |
| `ReadRetry` | `session.rs:31,346,808,844`; `session/tests.rs:141,161,182` |
| `count_cutoff` | def `session.rs:140`; `:434` (catch-up, moves), `:612`, `:790`, `:821` |

No script, bench tool or doc consumes `no_progress` or `catchup_retries`
(`grep -rn` over `scripts bench docs`: none).

---

## 2. Contract decision

**Typed contract.** Add `sse::session::catch_up::Stall`, a verdict for a catch-up read
that advanced nothing. `catch_up::stalled` owns the counters, logs and waits:

| Read | Verdict | Counted | Wait | What `serve` does next |
| --- | --- | --- | --- | --- |
| `Err` downcasting to `FatalSpanCutoff` | `Cutoff` | `count_cutoff`, `FEED_TOPOLOGY_DISCONNECTS`, info log | none | `return` (disconnect, no terminal), unchanged |
| any other `Err` | `Failed` | `FEED_SOURCE_FAILED` | 100 ms | same snapshot, same bound, unchanged |
| `Ok` (page did not advance) | `NoProgress` | **`FEED_NO_PROGRESS`** plus a debug line (new) | **100 ms** (new) | **`continue 'handoff`**: same bound, fresh snapshot, never the Lagged arm (new) |

`serve` matches `Stall` exhaustively, with no `_` arm.

**Why "same bound, fresh snapshot" and not the reviewer's alternatives.**

- **Keep the old `break`, with the wait added first** (the smallest diff: no `serve`
  change in the fix). This still routes every lap through `Lagged`, so
  `FEED_CATCHUP_RETRIES` keeps counting hole laps as re-catch-ups, up to 10 per second
  per stuck session. Rejected: part of the reported problem would remain.
- **Stay on the same snapshot** (`Stall::NoProgress => {}`, like `Failed`). This relies
  on §1.4's frontier invariant for liveness, with no escape if the invariant were ever
  broken. Rejected.
- **The re-snapshot this plan chose.** It keeps the old arm's intent: a stale snapshot
  can never strand the pass. It is also safe for keyless raw sessions. The bound is
  unchanged, and it was fixed while the pre-swap source was current (`bound <= that
  frontier <= its cap`). An installed extension carries the old span signature as its
  exact prefix, so the re-read range `[cursor, bound)` is the same segment-0 records at
  the same wire positions. No post-swap span is read before the live loop's
  `raw_keyless && snap.generation != join_gen` check.
- **One consequence to note.** A session at or above the floor that meets an empty page
  now re-reads durably after 100 ms, where before it fell through to the ring. This is
  timing only: the cursor is unchanged, so nothing can be skipped or duplicated.

**The reviewer's Change is not buildable as written.**

- Extracting the whole pass into `catch_up(..) -> CatchUp{Reached,Exhausted,NoProgress,Closed,Fatal}`
  needs at least 8 inputs: `feed`/`csrc`, `&mut cursor`, the bound, `&mut lease_watch`,
  `&task_state`, `&tx`, `&ctx`, `&mut need_status`, `&mut last_reported`,
  `&mut reached_live`, and the failpoint name. That breaks the five-argument limit.
  Grouping them in a struct is the context bag that RUST-QUALITY.md §Architecture
  rejects.
- The extraction would also re-indent the RAW-pairing emission (`:374-422`). That puts
  the `at_head` conjunction's roughly 15 operator mutants (`==`, `>=`, `<=`, `&&`,
  `min`) in-diff in a critical file.
- `Exhausted` is unreachable (§1.4).

The verdict-only extraction gets the same shrink of `serve` without these problems.

**Wire.** There is **no wire change**. Status codes, frames, cursors, ordering,
`upToDate`/`sealed` placement, EOF and terminal semantics are all identical; only the
timing of re-reads changes.

**Debug values.** The `/v1/debug/load` JSON shape is unchanged. Two values change
meaning (decision D1):

- `sse_livefeed.no_progress` also counts empty catch-up pages.
- `sse_livefeed.catchup_retries` stops counting hole laps. It now counts only genuine
  ring overtakes, which is what its doc says (`auth.rs:52-53`).

---

## 3. Red tests and pinning tests

### 3.1 Red test through production `serve`: commit 2

The test is `dst::dst_tests::sse_delivery::an_empty_catch_up_page_is_read_again_after_a_bounded_wait`
in `src/dst/tests/sse_delivery.rs`. It calls the helper `catch_up_fault_is_retried`
(added in commit 1, §4 C1.5), passing `("catch-up-empty", |s| &s.empty_pages)`.

What the helper does:

1. Starts `FakeSource::new(3, 8)`, so the feed is born with `head = floor = 3`, and sets
   the fault flag.
2. Runs `serve(.., StartPos::At(0), .., SseSurface::Product, slot)`.
3. Polls for up to 5 s until `src.reads > 0`, then stores `first`.
4. Sleeps 500 ms and computes `reads = src.reads - first` and `elapsed`.
5. Clears the fault, then collects the body until `"upToDate":true`, for at most 5 s.
6. Drops the body, calls `engine_shutdown`, and only then asserts.

Assertions:

- `first > 0`, with the message `"{leg}: the faulted catch-up read never ran"`.
- `u128::from(reads) <= elapsed.as_millis() / 100 + 2`. A re-read follows a whole
  `RETRY`, and tokio sleeps never end early. So the fixed tree gives about 5 reads in
  500 ms against a bound of 7.
- `event: data\ndata:{0,1,2}\n` each appears exactly once.
- `"upToDate":true` is present.

Expected red: run the test on commit 1 (behaviourally identical to `fba7b844` for this
path) with only the commit-2 test added. The trace is §1.3. `empty_pages` makes every
catch-up read return `scan_to == scan_from == 0`. The lap never waits. After the fault
clears, the next lap reads records 0..2, so the liveness assertions pass and the pacing
assertion fails:

```
test dst::dst_tests::sse_delivery::an_empty_catch_up_page_is_read_again_after_a_bounded_wait ... FAILED
---- dst::dst_tests::sse_delivery::an_empty_catch_up_page_is_read_again_after_a_bounded_wait stdout ----
thread 'dst::dst_tests::sse_delivery::an_empty_catch_up_page_is_read_again_after_a_bounded_wait' panicked at src/dst/tests/sse_delivery.rs:<line>:5:
catch-up-empty: the faulted catch-up read ran <N> more times in <T>; a read that advanced nothing must wait before it is read again (at most 7)
```

In that output, `T` is about 500 ms. `<N>` is in the tens to hundreds of thousands,
one read per non-yielding lap. The bound is 7 for any T in [500, 600) ms.

The fault clears before the assertion runs, so the spinning session delivers and the
test cannot hang on red. The spinning task holds only one of four workers, and the test
body runs on the `block_on` thread.

Green (commit 2): about 5 reads, each 100 ms after the previous one, then records 0..2
exactly once and `upToDate`.

### 3.2 Unit red for the verdict: commit 2 flips a commit-1 pin

The test is `sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict`
in `src/sse/session/tests.rs`, run with `#[tokio::test(start_paused = true)]`. It has
three legs; each asserts `(verdict, paused-clock elapsed)`:

- `cutoff`: `Err(anyhow::Error::new(FatalSpanCutoff(SourceCutoff::TargetMismatch)))`
  gives `Stall::Cutoff` and `0ns`.
- `failed`: `Err(anyhow!("injected"))` gives `Stall::Failed` and `100ms`.
- `empty`: `Ok(SourceBatch{scan_from:4, scan_to:4, records: default, completed:false})`
  gives `Stall::NoProgress` with `0ns` in commit 1 and `100ms` in commit 2.

`TargetMismatch` is used because no test asserts `FEED_CUTOFF_TARGET_MISMATCH`, and
`FEED_TOPOLOGY_DISCONNECTS` is read only for a diagnostic. All other counter
assertions in the suite are `>`-only.

Red when the commit-2 test runs against the commit-1 helper:

```
thread 'sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict' panicked at src/sse/session/tests.rs:<line>:9:
assertion `left == right` failed: empty: the wait owed before the next read
  left: 0ns
 right: 100ms
```

### 3.3 Pinning for the commit-1 refactor

New pins, green on HEAD semantics:

- `sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict` (commit-1
  form). It pins the moved `Err` arm: a cutoff returns at once and is counted, a
  failure waits 100 ms.
- `dst::dst_tests::sse_delivery::a_failed_catch_up_read_is_read_again_after_a_bounded_wait`,
  which runs `catch_up_fault_is_retried("catch-up-failed", |s| &s.fail_reads)`.
  Through production `serve`, a failed catch-up read is paced at 100 ms on the same
  snapshot, and records 0..2 arrive once. It passes on HEAD (`Err` sleeps 100 ms) and
  after both commits.

Existing pins, which must stay green:

- `dst_tests::livefeed_ownership::livefeed_reopened_sealed_span_serves_catch_up`: the
  catch-up `Err` retry through `serve`.
- `dst_tests::livefeed_basics::livefeed_ring_wrap_during_initial_handoff_recatchups`:
  a genuine `Lagged` re-catch-up still bumps `FEED_CATCHUP_RETRIES`.
- `dst_tests::livefeed_swap::livefeed_split_during_initial_catchup_delivers_everything`.
- `dst_tests::sse_delivery::{a_failed_live_read_is_retried_without_another_append, an_empty_live_read_is_retried_without_another_append}`.

Compile-level proofs for commit 1:

- `serve` matches `Stall` exhaustively with no `_`, so a future verdict cannot be
  silently dropped.
- `stalled` takes the read by value, so each result is answered exactly once.
- `count_cutoff` stays the single cutoff-counter owner (`super::count_cutoff`).
- The counters and the log line move token-for-token.

---

## 4. Edits, file by file, in commit order

### Budgets

Ceilinged files (current wc -l / merge-base wc -l). **None are touched**, so their
budget is 0 net lines:

| File | Current | Merge base |
| --- | --- | --- |
| `http.rs` | 3,366 | 3,369 |
| `product.rs` | 4,205 | 4,205 |
| `shard.rs` | 3,196 | 3,196 |
| `billing.rs` | 2,201 | 2,201 |
| `history.rs` | 1,713 | 1,713 |
| `auth.rs` | 1,676 | 1,676 |
| `registry.rs` | 1,492 | 1,501 |
| `sse/feed.rs` | 1,170 | 1,195 |
| `fleet.rs` | 1,143 | 1,143 |

Touched files, all at or below 1,000:

| File | Before | After C1 | After C2 |
| --- | --- | --- | --- |
| `src/sse/session.rs` | 911 | 897 | 897 |
| `src/sse/session/catch_up.rs` (new) | n/a | ~50 | ~66 |
| `src/sse/session/tests.rs` | 193 | ~221 | ~222 |
| `src/sse/feed/tests.rs` (merge base 970) | 969 | 974 | 974 |
| `src/dst/tests/sse_delivery.rs` | 696 | ~766 | ~772 |
| `src/sse/auth.rs` | 516 | 516 | 517 |

### Ratcheted scopes touched, with the remedy

**`serve`'s five `#[expect]`s** (`session.rs:161-180`: `too_many_arguments`,
`too_many_lines`, `excessive_nesting`, `let_underscore_must_use`, `disallowed_methods`).
Each one ratchets `scope_lines`, `nested_items` and `syntax_facts` against the merge
base, where `serve` is unchanged from HEAD. Remedy: narrow the scope, which commit 1
does by moving the code out.

- `scope_lines`: 699 → 683 (−23 +7), and commit 2 adds 0.
- `syntax_facts` shrink by about 19.
  - Removed: about 25 facts, from the `Ok`/`Err`/`Some` patterns, `downcast_ref` with
    its turbofish path, `count_cutoff`, two counter paths, two `fetch_add`s, two
    `Ordering` paths, `tracing::info!` (macro, tokens and path), and the
    `sleep`/`Duration::from_millis` calls.
  - Added: 6 facts, namely `catch_up::stalled` (call and path), the `read` argument
    path, and three `Stall::*` patterns.
- `nested_items`: 0 → 0.
- `tokio::select`/`tokio::pin` macro-dsl rows for `crate::serve` (select=3, pin=1)
  are untouched, because the catch-up `select!` stays.

All five expects stay fulfilled, so no `unfulfilled_lint_expectations`:

- `too_many_arguments`: still 10 arguments.
- `too_many_lines`: still about 680 lines.
- `excessive_nesting`: the Solo-drive `for` loop at `:651` is still depth 5 or more.
- `let_underscore_must_use`: `:460`, `:463`.
- `disallowed_methods`: `tokio::spawn` at `:321`, whose `effect` row
  `crate::serve`/`tokio::spawn` is unchanged.

The reason texts are not edited.

**No `unwrap_used`/`expect_used`-scoped exception is touched.** The `SingleSource` and
`LineageSource` impls, `response_from_stream`, and `read_and_publish` are all
untouched. The new file has no exceptions.

**Test files.** The `sse_delivery.rs` expects (`:36-43`, `:171-178`) and the
`feed/tests.rs` expects (`:543`, `:589`, `:694`, `:850`) each sit on one named test
fn. The edits are appended at EOF, or in `FakeSource` (lines 44-150), which lies
outside those scopes.

### Commit 1: "A catch-up read that advanced nothing is answered by one typed verdict, moved verbatim"

**C1.1 `src/sse/session/catch_up.rs` (new).**

```rust
//! What a durable catch-up read that advanced nothing owes its pass.
//!
//! `serve`'s catch-up pass reads privately below a fixed bound; its answer
//! to a read that moved nothing lives here so the pass meets every shape at
//! one exhaustive match, outside the pass's size and nesting exceptions.
use crate::sse::feed::SourceBatch;

/// What the pass does next after a read that advanced nothing.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Stall {
    /// A fatal span cutoff, counted and logged here: the session
    /// disconnects without a terminal control.
    Cutoff,
    /// The read failed and its wait passed: the same snapshot is read
    /// again at the same bound.
    Failed,
    /// The page advanced nothing: the pass hands the session to the live
    /// loop.
    NoProgress,
}

/// One owner for a stalled catch-up read's counters, log and wait.
pub(super) async fn stalled(read: anyhow::Result<SourceBatch>) -> Stall {
    // This source's spans are exhausted below the bound
    // (a swap happened mid-catch-up): the live loop
    // re-snapshots and, if the ring moved, re-catches-up
    // through the 'handoff path.
    let Err(e) = read else {
        return Stall::NoProgress;
    };
    // Round-11.2: fatal span outcomes disconnect
    // with the typed reason (no terminal) instead
    // of retrying forever.
    if let Some(cut) = e.downcast_ref::<crate::sse::source::FatalSpanCutoff>() {
        super::count_cutoff(cut.0);
        crate::sse::auth::sse_stats::FEED_TOPOLOGY_DISCONNECTS
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(reason = ?cut.0, "livefeed catch-up fatal cutoff");
        return Stall::Cutoff;
    }
    // Source failure mid-catch-up: bounded backoff,
    // then retry the SAME bound — never a hot loop
    // (finding 6 discipline applies here too).
    crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Stall::Failed
}
```

Lint notes:

- `Stall` deliberately has no `Default`, so the FnValue mutant is unviable (§5).
- The `read` argument is consumed by the let-else pattern, and it is an `async fn`
  argument, so `needless_pass_by_value` does not apply.
- Every item is `pub(super)`, so there is no `unreachable_pub`.

**C1.2 `src/sse/session.rs`.**

- Lines 30-31 become the four lines below (rustfmt order):
  `mod catch_up;` / `mod read_retry;` / `use catch_up::Stall;` / `use read_retry::ReadRetry;`
- Lines 424-446 (the stale "exhausted" comment, `Ok(_) => break,` and the whole
  `Err(e) => { … }` arm) are replaced with:

  ```rust
                      // A read that advanced nothing: `catch_up::Stall`
                      // names what this pass owes it.
                      read => match catch_up::stalled(read).await {
                          Stall::Cutoff => return,
                          Stall::Failed => {}
                          Stall::NoProgress => break,
                      },
  ```
- The `Ok(batch) if batch.scan_to > cursor => { … }` arm (`:373-423`) stays
  byte-identical, with no re-indent.

**C1.3 `scripts/quality/mutation_owners.py`.** After the `sse_session_read_retry` row
(`:143`), add:

`owner('sse_session_catch_up', 'src/sse/session/catch_up.rs', 'sse::session::tests:: dst_tests::sse_delivery::'),`

**C1.4 `src/sse/feed/tests.rs`** (a `#![cfg(test)]` file). In `FakeSource`, add a read
counter:

- After `pub(crate) max_concurrent_reads: AtomicU64,` add:

  ```rust
      /// Every read, counted at entry: the pacing legs' clock-free witness
      /// that a read which advanced nothing is not re-issued in a loop.
      pub(crate) reads: AtomicU64,
  ```
- In `new`, after `max_concurrent_reads: AtomicU64::new(0),`, add `reads: AtomicU64::new(0),`.
- As the first statement of `read_batch`, add `self.reads.fetch_add(1, Ordering::SeqCst);`.

That is +5 lines, giving 974.

**C1.5 `src/dst/tests/sse_delivery.rs`.** Append at EOF, so no existing line moves and
the scenario-map line anchors are unaffected.

```rust
/// Review item 86: a session catching up from 0 behind a feed whose ring
/// already starts at 3 (the feed captured frontier 3 as head and floor)
/// reads pages that advance nothing - a failed read or an empty partial
/// page - until the fault clears. Nothing re-drives the pass but its own
/// next read, so each re-read must follow a bounded wait; the records
/// behind the fault still arrive exactly once. `FakeSource::reads` is the
/// clock-free witness: a hot loop reads thousands of times per window.
async fn catch_up_fault_is_retried(
    leg: &str,
    fault: fn(&crate::sse::feed::tests::FakeSource) -> &std::sync::atomic::AtomicBool,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let (state, _addr) = http_rig(mem()).await;
    let src = std::sync::Arc::new(crate::sse::feed::tests::FakeSource::new(3, 8));
    fault(&src).store(true, SeqCst);
    let Ok(slot) = crate::http::sse_acquire(&state) else {
        panic!("{leg}: an SSE slot");
    };
    let response = crate::sse::session::serve(
        state.clone(),
        crate::sse::feed::tests::test_desc(leg),
        crate::crypto::StreamKey([7; 32]),
        [3; 16],
        src.clone(),
        crate::http::StartPos::At(0),
        crate::http::ReadParams::default(),
        None,
        crate::http::SseSurface::Product,
        slot,
    )
    .await;
    let mut body = response.into_body().into_data_stream();
    for _ in 0..500 {
        if src.reads.load(SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let first = src.reads.load(SeqCst);
    let window = std::time::Instant::now();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let reads = src.reads.load(SeqCst) - first;
    let elapsed = window.elapsed();
    fault(&src).store(false, SeqCst);
    let text = collect_session(&mut body, 5, |t| t.contains("\"upToDate\":true")).await;
    drop(body);
    engine_shutdown(&state).await;
    assert!(first > 0, "{leg}: the faulted catch-up read never ran");
    // A re-read follows a whole wait, and a sleep never ends early.
    let bound = elapsed.as_millis() / 100 + 2;
    assert!(
        u128::from(reads) <= bound,
        "{leg}: the faulted catch-up read ran {reads} more times in {elapsed:?}; a read that advanced nothing must wait before it is read again (at most {bound})"
    );
    for off in 0..3 {
        let record = format!("event: data\ndata:{off}\n");
        assert_eq!(
            text.matches(&record).count(),
            1,
            "{leg}: record {off} exactly once after the fault cleared:\n{text}"
        );
    }
    assert!(
        text.contains("\"upToDate\":true"),
        "{leg}: the session reaches live after the fault cleared:\n{text}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_catch_up_read_is_read_again_after_a_bounded_wait() {
    catch_up_fault_is_retried("catch-up-failed", |s| &s.fail_reads).await;
}
```

The helper uses `u128::from(reads)` and no `as` casts, so `cast_possible_truncation`
does not fire in test code.

**C1.6 `src/sse/session/tests.rs`.** Append:

```rust
/// Item 86: a catch-up read that advanced nothing is answered at one
/// owner. A fatal cutoff ends the session at once; a failed read waits the
/// bounded retry before the same snapshot is read again.
#[tokio::test(start_paused = true)]
async fn a_stalled_catch_up_read_owes_its_pass_one_verdict() {
    let empty = crate::sse::feed::SourceBatch {
        scan_from: 4,
        scan_to: 4,
        records: crate::application::read::PlainBatch::default(),
        completed: false,
    };
    let cut =
        crate::sse::source::FatalSpanCutoff(crate::sse::feed::SourceCutoff::TargetMismatch);
    for (leg, read, owed, waited) in [
        ("cutoff", Err(anyhow::Error::new(cut)), Stall::Cutoff, 0),
        ("failed", Err(anyhow::anyhow!("injected")), Stall::Failed, 100),
        ("empty", Ok(empty), Stall::NoProgress, 0),
    ] {
        let start = tokio::time::Instant::now();
        assert_eq!(catch_up::stalled(read).await, owed, "{leg}");
        assert_eq!(
            start.elapsed(),
            Duration::from_millis(waited),
            "{leg}: the wait owed before the next read"
        );
    }
}
```

`catch_up` and `Stall` reach the test through the existing `use super::*;`, the same way
`ReadRetry` does.

**C1.7 Ledger:** `python3 scripts/test-inventory.py --write`, which adds one entry for
`a_failed_catch_up_read_is_read_again_after_a_bounded_wait`.

Commit-message red/green block: "pure refactor; pins" (§3.3).

### Commit 2: "An empty catch-up page waits its bounded retry and re-reads from a fresh snapshot instead of spinning through the Lagged arm"

**C2.1 `src/sse/session/catch_up.rs`.**

- Replace the module doc with the version below. It drops the false "exhausted" premise.

  ```rust
  //! What a durable catch-up read that advanced nothing owes its pass.
  //!
  //! `serve`'s catch-up pass reads privately below a bound taken from the
  //! feed head, so the frontier of every snapshot it reads is at or past the
  //! bound: an empty page there is a hole the read could not explain yet,
  //! never the snapshot's end. Nothing re-drives the pass but its own next
  //! read - no version bump, no park - so each read that advanced nothing
  //! waits here before the next. An empty page used to reach the live loop
  //! with no wait, and below the ring's floor its Lagged arm sent it straight
  //! back: a lap with no await but the read, each lap counted as a
  //! re-catch-up (review item 86).
  ```
- Add `use std::time::Duration;` and:

  ```rust
  /// One re-read per 100 ms bounds what a failing or hole-bearing store
  /// costs a session in catch-up. The live phase backs off on its own
  /// (`read_retry`): that retry shortens a park other wakes may end first,
  /// and this pass has no park.
  const RETRY: Duration = Duration::from_millis(100);
  ```
- Replace the `NoProgress` doc with:

  ```rust
      /// The page was empty and its wait passed: the pass reads the same
      /// bound again from a fresh snapshot, so a retired snapshot can never
      /// strand it. The ring did not move the session, so this is no
      /// re-catch-up and never reaches the live loop's Lagged count.
  ```
- Replace the let-else with a match that keeps the page's cursor for the debug line:

  ```rust
      let e = match read {
          Ok(page) => {
              crate::sse::auth::sse_stats::FEED_NO_PROGRESS
                  .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              tracing::debug!(
                  cursor = page.scan_from,
                  "livefeed catch-up page made no progress; re-reading after a bounded wait"
              );
              tokio::time::sleep(RETRY).await;
              return Stall::NoProgress;
          }
          Err(e) => e,
      };
  ```
- The `Failed` tail becomes `tokio::time::sleep(RETRY).await;`, which is the same
  100 ms.

**C2.2 `src/sse/session.rs`.**

- In the new arm, `Stall::NoProgress => break,` becomes `Stall::NoProgress => continue 'handoff,`.
- Rewrite the handoff comment `:348-351` in place (4 lines to 4 lines):

  ```
          // The handoff loop: durable catch-up to `catchup_bound`, then
          // live consumption. Re-entered when the ring overtakes a
          // not-yet-live session (the bound refreshes to the feed head),
          // and after an empty catch-up page (same bound, new snapshot).
  ```
- Net 0 lines, and `serve`'s facts are unchanged against commit 1 (the label is not a
  path).

**C2.3 `src/sse/session/tests.rs`.** Change the `empty` row to
`("empty", Ok(empty), Stall::NoProgress, 100),`. Change the doc's second sentence to
"…a failed read or an empty page waits the bounded retry before the next read."

**C2.4 `src/dst/tests/sse_delivery.rs`.** Append:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_catch_up_page_is_read_again_after_a_bounded_wait() {
    catch_up_fault_is_retried("catch-up-empty", |s| &s.empty_pages).await;
}
```

**C2.5 `src/sse/auth.rs:24`.** The doc line `/// Source reads that returned an empty partial page.` becomes:

```rust
    /// Source reads that advanced nothing: a drive's empty partial page, or
    /// an empty page a catch-up read waits out before reading again.
```

**C2.6 `docs/LIVE-FEED.md` §The session.** After the "park until …" bullet (`:35-38`),
add:

```
- in durable catch-up, re-read a page that advanced nothing (a failed read
  or an empty page) only after a bounded 100 ms wait — the pass has no park
  and nothing else re-drives it; an empty page re-reads from a fresh snapshot;
```

**C2.7 Ledger:** `python3 scripts/test-inventory.py --write`, which adds one entry for
`an_empty_catch_up_page_is_read_again_after_a_bounded_wait`.

The commit message quotes the red outputs from §3.1 and §3.2 and the green timing.
Edge: timing plus two `/v1/debug/load` values (D1).

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

**Commit 1.**

- `sse::session::serve` (`src/sse/session.rs`, owner `sse_session`). The body is edited,
  so the FnValue mutant "replace serve -> axum::response::Response with
  Default::default()" is selected. It is viable, because `http::Response<Body>: Default`.
  Its span runs from the first to the last body statement.
  - Killed by `dst_tests::sse_delivery::a_failed_catch_up_read_is_read_again_after_a_bounded_wait`:
    no task runs, so `first == 0` fails with "the faulted catch-up read never ran",
    after about 5.5 s.
  - Also killed by item 33's `…_live_read_is_retried_without_another_append`: the
    empty body EOFs, so "the session parks at the head" fails.
  - Both tests are inside `sse_session`'s `dst_tests::sse_delivery::` filter.
- No other mutant is in-diff:
  - The inserted arm holds only patterns and a call.
  - `continue`, `break` and `return` are not mutated.
  - No `match` in the diff has a `_` arm, so there are no arm-deletion mutants. (A
    binding catch-all `read =>` is not `Pat::Wild`.)
  - The guard `batch.scan_to > cursor` (`:373`) is neither inserted nor adjacent to the
    deletion, whose neighbours are the `}` on `:423` and `:447`, so its true/false
    mutants are not selected.
- `sse::session::catch_up::stalled` (new file, owner `sse_session_catch_up`). The only
  candidate is the FnValue `Default::default()`, which is unviable because `Stall`
  derives no `Default`. That is a build failure, not a miss. The function has no binary
  or unary operators, no guards and no `_` match; literals and `fetch_add` arguments
  are not mutated. With no viable mutant, the planner reports "selects no mutant"
  explicitly.

**Commit 2.**

- `serve`: the changed arm line puts FnValue in-diff again, killed as above. There are
  no other mutants.
- `stalled`: the `Ok` arm body is new. FnValue is unviable, and there are no operators.
  The `const RETRY` is not mutated.
- `src/sse/auth.rs`: only a doc line on a static changes. `sse_auth` is selected, but no
  mutant spans that line, so it reports zero.

**Test-only files are out of mutation scope.** `src/sse/feed/tests.rs` and
`src/sse/session/tests.rs` begin with `#![cfg(test)]`, so they classify as
`production_unchanged_files`. `src/dst/tests/sse_delivery.rs` is outside the critical
prefixes and unregistered.

**Owner rows:** `+ sse_session_catch_up` (C1.3). No filter changes to `sse_session` or
`sse_session_read_retry`.

**Equivalent mutants:** none, and no guard, boundary, predicate or arithmetic is added.

**Timeout risk:** the slowest killer takes about 5.5 s under the FnValue mutant, well
inside cargo-mutants' baseline-scaled timeout. No test can hang: every collect is
bounded, and the fault clears before any assertion.

---

## 6. Ledgers

| Ledger | Change |
| --- | --- |
| `docs/refactor/test-inventory.json` | `--write` in each commit: +1 DST entry each (C1 failed leg, C2 empty leg); both `mechanisms: []`, `scenarios: []`, like item 33 |
| `scripts/quality/mutation_owners.py` | +1 row (C1.3) |
| `docs/refactor/review-mechanisms.json` | none; no pinned test body changes, and tests are appended at EOF |
| `docs/quality/owners.json` | none; no global static, macro-dsl, glob or by-path added (`catch_up.rs` uses `tracing::` only; `tokio::time::sleep` is a plain call) |
| `docs/quality/source-allowances.json` | none vacated (the `crate::serve` rows `tokio::select`=3, `tokio::pin`=1 and effect `tokio::spawn` are unchanged) |
| `docs/refactor/architecture-policy.json` | none (precedent: `session/read_retry.rs`; `catch_up.rs` imports no transport) |
| `docs/refactor/WIRE-MATRIX.md` | none (no wire change) |
| scenario map / dispositions | none (no rename; appended tests do not move mapped line anchors) |
| `src/dst/tests/README.md` | none (no new DST module) |
| `docs/LIVE-FEED.md` | session bullet (C2.6) |

---

## 7. Controls

Run these after the concurrent mutation and gate runs have finished; they contend for
CPU.

1. `cargo fmt --all -- --check`. Expected: no output, exit 0.
2. Red, on commit 1 plus only C2.4:
   `cargo test --locked --lib an_empty_catch_up_page_is_read_again_after_a_bounded_wait`.
   Expected: `... FAILED` with the §3.1 panic, N much larger than 7.
3. Red for the unit test, on commit 1 plus only C2.3:
   `cargo test --locked --lib sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict`.
   Expected: the §3.2 `left: 0ns right: 100ms` panic.
4. Green legs:
   `scripts/test-leg.sh target/legs/item86.log --exact dst::dst_tests::sse_delivery::a_failed_catch_up_read_is_read_again_after_a_bounded_wait --exact dst::dst_tests::sse_delivery::an_empty_catch_up_page_is_read_again_after_a_bounded_wait --exact sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict -- --locked --lib catch_up`.
   Expected: every named test prints `... ok`, and `tests_ran.py` exits 0.
5. `scripts/test-leg.sh target/legs/sse.log --min 30 -- --locked --release --lib sse::`,
   `scripts/test-leg.sh target/legs/livefeed.log --min 40 -- --locked --release --lib livefeed_`,
   and `cargo test --locked --lib dst_tests::sse_delivery::`. Expected: all ok,
   including `livefeed_ring_wrap_during_initial_handoff_recatchups` and
   `livefeed_reopened_sealed_span_serves_catch_up`.
6. `python3 scripts/test-inventory.py --check`. Also run `--check` for
   `scenario-map-report`, `review-evidence` and `architecture-gate`. Expected: exit 0.
7. `scripts/quality.sh`. Expected: `QUALITY_OK`. `gate.py` must print no
   `accepted exception grew` line (serve's five contracts shrink) and no
   `file growth` line. Clippy with `-D warnings` must be clean, with no
   `unfulfilled_lint_expectations`.
8. Mutation, using CI's push plan:
   `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) scripts/quality/mutations.sh`.
   Expected:
   - `selected-owners.json` includes `sse_session`, `sse_session_catch_up`, `sse_auth`,
     and whatever the eight earlier local commits select.
   - `serve`'s FnValue is CAUGHT.
   - `catch_up::stalled`'s FnValue is UNVIABLE.
   - 0 MISSED and 0 TIMEOUT.
9. After the push, `gh run view` on the pushed SHA. Do not claim CI is green without it.

---

## 8. Out of scope

- **Item 87** (a typed `SourceReadError` instead of the anyhow `downcast_ref::<FatalSpanCutoff>`).
  The downcast moves verbatim into `catch_up::stalled`, and item 87 replaces it there
  and in `feed.rs` `read_and_publish`. This plan makes that edit smaller, because the
  arm now lives outside `serve`'s ratchets.
- **The reviewer's whole-pass extraction.** Rejected in §2. `serve` remains the known
  oversized exception.
- **Moving catch-up stalls onto `ReadRetry`'s doubling backoff** (D2).
- **Racing the catch-up wait against `tx.closed()`.** A closed client can linger up to
  100 ms, which is pre-existing for failed reads.
- **Keyless-raw re-snapshot after `Lagged`.** There is a microsecond window between the
  live loop's generation check and `catchup_bound = feed.head()`. It is pre-existing and
  not widened here: `NoProgress` keeps its bound.
- **Counting only `!completed` empty catch-up pages.** Every such page is partial by
  §1.4, and a `completed` test would add a `!` mutant that only a flaky global-counter
  assertion could kill.

---

## 9. Decisions for Søren

**D1: `/v1/debug/load` counter values.** The JSON shape is unchanged.

- `sse_livefeed.no_progress` (`FEED_NO_PROGRESS`) now also counts empty catch-up pages.
  Today it counts only drive pages.
- `sse_livefeed.catchup_retries` (`FEED_CATCHUP_RETRIES`) stops counting laps of a
  session waiting out a hole. Today that is about 10^5 per second during a spin; after
  the fix it counts only ring overtakes, as its doc says.

Backward-compatible alternative: drop the one `FEED_NO_PROGRESS.fetch_add` from
`catch_up::stalled`. Then `no_progress` stays drive-only, and catch-up holes show only
in the debug line. The `catchup_retries` change is the fix itself. The only variant that
keeps counting hole laps is the rejected "wait, then `break`" (§2), which counts up to
10 per second per stuck session.

**D2 (policy, default no change): catch-up stall cadence.** The plan keeps the existing
fixed 100 ms for both failed and empty catch-up reads.

The alternative is the live phase's `ReadRetry` policy for both: 250 ms doubling to
5 s at the same cursor, bounded by the lease nap. It cuts a persistent hole's cost from
10 reads per second to at most 0.2 per session, but slows recovery after a long fault,
and it changes today's 100 ms cadence for failed reads.

---

## Skeptic corrections (C1..C7)

Verified against the tree at `fba7b844` (read-only: Read/grep/git show/wc; nothing built or run).
Confirmed as written: every quoted anchor (`session.rs:30-31, 140, 161-180, 348-351, 352, 359-373,
424-446, 481-497, 808, 844, 911`; `feed.rs:552-569, 708-727, 817-821, 903-942, 960`;
`source.rs:87, 572-663, 684-697`; `read.rs:189-262`), serve's scope (161..859 = 699 lines, so
699 -> 683), the spin trace in §1.3 (FakeSource's empty/failed paths complete on their first
poll, `tokio::select!` then never returns Pending, and the lap's only other work is sync), and §1.4's
frontier invariant (`st.head` moves only at `feed.rs:960` to a read's `scan_to`; lineage
`logical_start` accumulates from 0 at `source.rs:372-378`). Also buildable as written:
`count_cutoff` reached as `super::`, `FatalSpanCutoff(pub(crate) ..)` constructible
(`source/spans.rs:9`, precedent `source/tests.rs:48`), `Stall`/`catch_up` visible to the unit
test through `use super::*` (precedent `ReadRetry`), `FakeSource` built only by `new`, the exact
`Duration` equality under `start_paused` (precedent `fleet/repository/document_tests.rs:166`; tokio
1.52.3 `TimeSource.start_time = clock.now()`), no `match_same_arms`/`needless_pass_by_value` hit,
`wildcard_enum_match_arm` not enabled in `session.rs` (only `feed.rs:1`, `commit_handoff.rs:1`,
pilot), and no ledger rows beyond the ones §6 names (mt-audit baseline is content-keyed; global
static rows count definitions only; `verification.json`/`architecture-review-baseline.json`
were not touched by item 33's `7c4f8606` for the same `serve` edit shape).

**C1: the merge base moved; §0, §4 budgets and control 8 are stale.** `git rev-parse origin/slate`
is now `fba7b844` = HEAD (the eight commits were pushed); `fb18840d` is no longer the comparison.
Merge-base wc -l is therefore equal to current for every ceilinged file: http.rs 3,366 (not 3,369),
product.rs 4,205, shard.rs 3,196, billing.rs 2,201, history.rs 1,713, auth.rs 1,676, registry.rs
1,492 (not 1,501), sse/feed.rs 1,170 (not 1,195), fleet.rs 1,143; budget 0 each, none touched.
`src/sse/feed/tests.rs` base is 969 (not 970); 974 after C1.4 is still under 1,000. Control 8:
`QUALITY_BEFORE_SHA=$(git rev-parse origin/slate)` now yields `fba7b844`, so `selected-owners.json`
lists only this item's owners (`sse_session`, `sse_session_catch_up`, and `sse_auth` in commit 2),
not "whatever the eight earlier local commits select". Re-check `git rev-parse origin/slate`
at landing time again, because other items may push first.

**C2: §5's "selects no mutant" for `catch_up.rs` is wrong, and so is "baseline-scaled timeout".**
`cargo mutants --list --in-diff` lists the FnValue `replace stalled -> Stall with Default::default()`,
because cargo-mutants finds out that a mutant is unviable only when it builds it. So
`mutation_driver.py:147-152` counts 1 and runs the owner: an unmutated baseline of
`sse::session::tests:: dst_tests::sse_delivery::` (the new red DST legs included) plus one failed
build (UNVIABLE, exit 0). Rewrite §5 to say that; §7.8's "UNVIABLE" line is the correct
statement. Also, the per-mutant timeout is the fixed `--timeout 90`
(`mutation_driver.py:52`), not a baseline-scaled one. Under serve's FnValue, every
`sse_session`-filtered test (`sse::`, `dst_tests::sse_delivery::`, `dst_tests::livefeed_swap::`,
`livefeed_engine_retired`) must still finish inside 90 s. The new legs add about 5.5 s (5 s poll +
0.5 s + an immediate EOF collect), run in parallel, which is the same shape item 33 already
passed. State that as the bound.

**C3: the §1.6 `Take::Lagged` list is incomplete.** It is also matched in
`src/sse/feed/tests/retry.rs:57` and `:126` (`!matches!(feed.take_visible(cursor), Take::Lagged { .. })`).
No edit is needed, since the variant does not change, but the "complete list" claim should include them.

**C4: §2's "timing only" for a session at or above the floor rests on §1.4's transient-hole
premise, so say so.** Before: an empty catch-up page at cursor >= floor fell through (`:428`) to
`take_visible`, and the ring (filled by a driver read that got past the same offsets) served the
records at once. After: the session re-reads durably every 100 ms and never consults the ring until
the durable read explains the hole. For the known hole shapes (`read.rs:216-220` coverage not proven
yet, `:259-262` unexplained absorption race, the `0..16` fall-through, a sealed-span partial page at
`source.rs:655-661`) this is a bounded added latency, because each clears once absorption or the peer
catches up. A hole that never clears would now stall that session at 10 reads/s, where it used to
progress through the ring. Put this sentence in the commit message's Edge line and in D2 ("a
persistent durable hole at or above the floor waits instead of being served from the ring").
Do not add a floor predicate: it would add an in-diff `<`/`>=` mutant in a critical file, and
§1.4 shows the case is unreachable for today's sources.

**C5: the keyless-raw re-snapshot argument (§2, fourth bullet) is sound, but no test pins it; name
that as a residual.** With a post-swap `LineageSource` snapshot, records in `[cursor, bound)`
compose at the same segment-0 positions (`locate` of span 0 with `logical_start` 0). The only wire
difference is the RAW `at_head` fold (`session.rs:389-393`): it reads `csrc.frontier()` and
`csrc.closed()`, which are larger/open on the fresh snapshot, so an `upToDate` that the old
(sealed, capped) snapshot might have folded into the last catch-up control is now left to the
standalone status. That is more honest, not a skip or a duplicate, and the live loop's
`raw_keyless && snap.generation != join_gen` check (`:471-476`) still disconnects before any
post-swap span is read. Record it under §8 as covered by argument only, with the one-line reason, so
a reviewer does not read "never the Lagged arm" as "the keyless check still runs first".

**C6: the C2.1 module doc narrates history.** "An empty page used to reach the live loop … (review
item 86)" is commit-message material. The repo style is that a doc states why the code exists
(owner/invariant). Keep the first three sentences (bound from the feed head, so an empty page is a
hole and never the snapshot's end; nothing but the pass's own next read re-drives it) and move the
"used to … Lagged arm … re-catch-up" sentence into the commit message.

**C7: a minor precision fix in §3.1 and §7.2.** The red run must pin the right tree. "Commit 1 plus
only C2.4" is correct: C1.4's `FakeSource::reads` and C1.5's helper are both needed to compile the
red leg, so the red cannot be taken on bare `fba7b844`. Say this explicitly so the red/green block
in the commit message is not read as "fails on HEAD as-is". The expected red text in §3.1 is right:
the panic is at column 5 of the helper's pacing `assert!`, N is at least in the 10^4 range, and the
bound is 7 for T in [500, 600) ms.

### Verdict: **ready-with-corrections**

The diagnosis, the typed-verdict extraction and the red tests hold up against the source.
Commit 1 is behaviour-neutral: the moved arm is token-for-token, `serve`'s five ratchets shrink
(scope 699 -> 683, syntax facts drop by about 19 net, nested items unchanged) and all five expects
stay fulfilled. Commit 2's red DST leg fails today through the traced Lagged spin, and it is
bounded: the fault clears before the collect, and a spinning worker is not the `block_on` thread.
The unit red is exact under the paused clock. No unbuildable control was found. No ledger is
missing beyond what §6 lists. C1 (stale merge base) and C2 (the unviable mutant is listed and
run; the timeout is a fixed 90 s) must be fixed in the text before execution. C3-C7 are precision
and wording.
