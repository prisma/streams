# Item 33: bounded retry of a session's own failed live read

Tree: `slate` @ `0afa2597`. The plan is read-only: nothing below has been run.
Reviewer text: robustness-maintainability-review.md §33 (lines 1004-1014).

## 1. Problem (checked against the current tree)

**The live drive's failure arms park the session with no retry.** `src/sse/session.rs:836-846`:

```rust
// No-progress page or source failure: feed state
// did NOT change and the version was NOT bumped —
// park rather than spin (finding 6). The next
// durable advance or the heartbeat retries.
...
Some(DriveOutcome::NoProgress | DriveOutcome::Idle) | None => {}
Some(DriveOutcome::SourceFailed) => {
    tracing::warn!("livefeed source read failed; parking until next wake");
}
```

These arms run when `cursor < frontier` (`session.rs:770`), whether the source is open or closed. Control then reaches the park (`session.rs:872-886`), which waits on five things:

- `ver_wait`: a feed version bump. `drive_under_permit` bumps only for `Solo`/`Published` (`feed/drive.rs:132`), and `read_and_publish` returns `SourceFailed`/`NoProgress` without touching head or version (`feed.rs:941-956`).
- `gen_wait`: a source swap.
- `src_wait`: `cur_src.advance_notify()`. For `SingleSource` this is `&self.handle.notify` (`source.rs:120-122`), fired by the next durable advance and by engine close (`shard.rs:1911`). For a lineage with a sealed tail it is `&self.idle_notify`, which nothing fires (`source.rs:721-727`, "park on a notify nothing fires"). This iteration's `src_wait` was registered at the loop top, before the drive. The advance that revealed the unread records therefore fired before registration, and it cannot wake this park.
- `tx.closed()`: the client left.
- `tokio::time::sleep(lease_watch.nap())`: `nap()` returns `Duration::from_secs(3600)` for `SseLease::None` (`sse/auth.rs:293-296`, the raw/PRISMA-key surface). For a customer lease it waits until the lease deadline, clamped to 1..=3600 s; with `POLICY_STALENESS_MAX_SECS = 300` that is up to about 5 min.

The feed's retry task does not cover this case. It is armed only from the closed-at-frontier branch (`session.rs:729`). By its own contract it never reads (`feed/drive.rs:42-47`, "the task re-SETTLES at 250 ms - it never reads"). On an open tail it bumps once and exits (`drive.rs:106-109`: `self.bump_version(); src.closed()`).

**Result.** Durable records behind a transiently failed or empty live read wait for the next append, for the lease nap, or for an ownership or engine-close wake. Two cases never get a next append at all:

- a closed `SingleSource` tail with unread records;
- a lineage whose tail is a sealed span, where the read is remote and a peer 429/503 surfaces as `SourceFailed`.

In both, the session stalls until the lease nap. `docs/refactor/WIRE-MATRIX.md:218` already claims the opposite ("the SSE feed parks and retries on its own cadence"). The Phase-A catch-up path does have the discipline the live path lacks (`session.rs:459-464`: "bounded backoff, then retry the SAME bound", a 100 ms sleep).

**The comments are out of date.** Commit e902d951 (round 11.1) removed the producer heartbeat: "NO producer writes keep-alives any more", and the LiveFeed session's `hb.changed()` arm is gone. These comments still cite it:

- `session.rs:839`: "The next durable advance or the heartbeat retries."
- `session.rs:740-742`: "the resume spawn or the next append/heartbeat wakes us."
- `feed.rs:946-947`: "The session parks; the next durable advance or heartbeat retries."
- `feed.rs:1174`: "The session parks instead of spinning" (NoProgress doc).
- `feed.rs:1181-1182`: "The session parks and retries on the next wake" (SourceFailed doc).
- `session.rs:868-871`: "no per-session timer exists". The fix adds one.
- `docs/LIVE-FEED.md:35`: "park until: feed version change, heartbeat tick, lease deadline".
- `feed/drive.rs:94-96`: "this tick is the only retry of that read". This is false today for open tails, and after the fix it is false for closed tails too.

**What the reviewer's Change gets right, and where this plan differs.**

- "Do not route through `schedule_transition_retry`" is correct, and there is a concrete race behind it. On an open tail, `transition_pending` bumps the version (`drive.rs:107`) before the loop exits and stores `retry_scheduled = false` (`drive.rs:79`). A session woken by that bump can re-drive, fail again and call `schedule_transition_retry` while the flag is still `true`. The CAS then fails (`drive.rs:55-59`), the task exits, and the session is parked with no retry: a flaky version of this same stall. The retry task is also fixed at 250 ms with no backoff.
- The reviewer's "loop-local backoff set in the two arms, cleared on progress, min with `lease_watch.nap()`" is buildable, but inline in `serve` it is not free. `serve` carries five `#[expect]`s ratcheted on `scope_lines` 743 and `syntax_facts` 875 (the values recorded in 4247421f's message). Clearing the backoff "on progress" needs a reset at four progress sites (catch-up, `Take::Batch`, two `Solo` arms). Each reset adds facts and lines under all five exceptions.
- This plan puts the policy in a small owner type, `ReadRetry`, with the same semantics. Backoff escalation is keyed on the cursor: a failure at the same cursor means no progress since the last one. `serve` gains one declaration and one call, and the edit is line-neutral and fact-neutral (§4).
- The reviewer's "HTTP-level test with a one-shot fail_next_read hook" is replaced by a session-level test. The only HTTP-reachable seam is `SingleSource::read_batch`, which sits under an impl-wide `#[expect(clippy::unwrap_used)]` (`source.rs:42-45`). A hook there re-fingerprints that exception, so its reason would have to be re-decided just to add test-only code. The test instead drives the production `serve` (real `AppState`, registry, `LiveFeed`, `GatedSseBody`) with the feed tests' `FakeSource`, whose `fail_reads` and `empty_pages` flags already produce `SourceFailed` and `NoProgress` deterministically.

## 2. Contract decision

- **Typed contract.** `DriveOutcome` is unchanged. `NoProgress` and `SourceFailed` keep the "no state changed, no version bump" rule from finding 6. Only their docs change, to name who retries: the driving session, on its own bounded retry.
- **New session-local owner.** `sse::session::read_retry::ReadRetry`. It is `pub(super)`, not shared, and uses no atomics or locks.
- **Retry policy.** Only a session whose own drive returned `NoProgress` or `SourceFailed` owes itself a retry. The next park is shortened to `min(delay, lease nap)`:
  - the first delay is 250 ms, doubled for each further failure at the same cursor, capped at 5 s;
  - a failure after the cursor has advanced starts again at 250 ms;
  - a contended session (`None`) or an `Idle` one still parks on the version watch alone, so fan-out has no timer herd;
  - the retry never postpones the lease deadline.
- **Wire.** No new frames, codes, fields or ordering. Raw and product framing are byte-identical. Cursors are unchanged.
- **What changes at the edge.** Only timing. Records behind a transiently failed or empty live read now arrive after the first successful retry (250 ms first retry, then up to 5 s between retries) instead of at the next append or the lease nap (up to 1 h unleased, about 5 min leased). A persistently failing source costs at most one read per failing driver session per 5 s.
- **Backward-compatible alternative.** None is needed; the change is backward compatible by construction and has no opt-out.
- **Log line.** The only operational text that changes is the log. The `serve` warn "livefeed source read failed; parking until next wake" becomes false after the fix. It is replaced by one warn per owed retry inside `ReadRetry::nap`, carrying cursor and delay, which covers `NoProgress` too. The rate is bounded by the backoff. `FEED_SOURCE_FAILED` and `FEED_NO_PROGRESS` still tell the two outcomes apart.

## 3. Red tests

### 3a. Behaviour reds (compile on 0afa2597, fail there)

File: `src/dst/tests/sse_delivery.rs`. The module is already in the `sse_session` mutation filters (`dst_tests::sse_delivery::`). It is 510 lines now and about 600 after the change.

1. `dst::dst_tests::sse_delivery::a_failed_live_read_is_retried_without_another_append` (fault: `FakeSource::fail_reads`, giving `DriveOutcome::SourceFailed`)
2. `dst::dst_tests::sse_delivery::an_empty_live_read_is_retried_without_another_append` (fault: `FakeSource::empty_pages`, giving `DriveOutcome::NoProgress`)

Shared helper `live_read_fault_is_retried(leg, fault)`. Every wait in it is bounded:

```rust
/// Collect a session body until `done` holds or `secs` pass; a hung
/// collect would read as a mutation timeout, which is not detection.
async fn collect_session(
    body: &mut axum::body::BodyDataStream,
    secs: u64,
    done: impl Fn(&str) -> bool,
) -> String {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut text = String::new();
    while !done(&text) {
        match tokio::time::timeout_at(deadline, futures_util::StreamExt::next(body)).await {
            Ok(Some(Ok(chunk))) => text.push_str(&String::from_utf8_lossy(&chunk)),
            Ok(Some(Err(_)) | None) | Err(_) => break,
        }
    }
    text
}

/// Item 33: a live read on an OPEN source fails (or returns an empty
/// partial page), the fault clears, and nothing else happens - no
/// append, no notify. A read that changed nothing bumps nothing, so
/// only the session's own bounded retry can re-drive it; the record
/// must still arrive (it used to wait for the next append, or for an
/// unleased session's hour-long lease nap). The FakeSource makes the
/// fault one flag on the source instead of a production hook.
async fn live_read_fault_is_retried(
    leg: &str,
    fault: fn(&crate::sse::feed::tests::FakeSource) -> &std::sync::atomic::AtomicBool,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let (state, _addr) = http_rig(mem()).await;
    let src = std::sync::Arc::new(crate::sse::feed::tests::FakeSource::new(0, 8));
    let slot = crate::http::sse_acquire(&state).ok().expect("an SSE slot");
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
    let head = collect_session(&mut body, 10, |t| t.contains("\"upToDate\":true")).await;
    assert!(head.contains("\"upToDate\":true"), "{leg}: the session parks at the head:\n{head}");
    // One durable record whose first read is faulted.
    fault(&src).store(true, SeqCst);
    src.frontier.store(1, SeqCst);
    src.notify.notify_waiters();
    // The faulted read entered (max_concurrent) and returned (none in flight).
    let ran = || src.max_concurrent_reads.load(SeqCst) >= 1 && src.reads_in_flight.load(SeqCst) == 0;
    for _ in 0..500 {
        if ran() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(ran(), "{leg}: the faulted live read never ran");
    fault(&src).store(false, SeqCst);
    let record = "event: data\ndata:0\n";
    let text = collect_session(&mut body, 5, |t| t.contains(record)).await;
    assert!(
        text.contains(record),
        "{leg}: the record behind a failed live read must arrive without another append:\n{head}{text}"
    );
    drop(body);
    engine_shutdown(&state).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_live_read_is_retried_without_another_append() {
    live_read_fault_is_retried("source-failed", |s| &s.fail_reads).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_live_read_is_retried_without_another_append() {
    live_read_fault_is_retried("no-progress", |s| &s.empty_pages).await;
}
```

**Why the wake cannot be lost.** `src_wait` is registered at the loop top (`session.rs:497-499`) before the upToDate status is sent, so `notify_waiters()` after the head collect always wakes the session.

**Why the "read ran" signal is exact.** The first and only read ever made on this FakeSource is the faulted one:

- `StartPos::At(0)` with `join_head == 0` means no catch-up read;
- a frontier of 0 means no drive at the head.

`FakeSource::read_batch` increments `reads_in_flight`, raises `max_concurrent_reads`, checks the fault and drops its guard, in that order (`feed/tests.rs:108-125`). So `max >= 1 && in_flight == 0` holds exactly when the faulted read has returned.

**Why nothing else can wake the session on the old tree.** `wake_all_sessions` fires only in fleet mode (`fleet.rs:812`). The lease is `SseLease::None`, so the nap is 3600 s. The body's 15 s keep-alive never wakes the producer.

**Fallback if the closure-to-fn-pointer coercion is rejected.** Use two named `fn fail_reads(s: &FakeSource) -> &AtomicBool` helpers.

**Exact expected red on 0afa2597.** Apply only this hunk and run the command in §7.1. Each test takes about 5 s (the second collect deadline), then fails. The line is the final `assert!`.

```
---- dst::dst_tests::sse_delivery::a_failed_live_read_is_retried_without_another_append stdout ----

thread 'dst::dst_tests::sse_delivery::a_failed_live_read_is_retried_without_another_append' panicked at src/dst/tests/sse_delivery.rs:<line>:5:
source-failed: the record behind a failed live read must arrive without another append:
event: control
data:{"nextCursor":"<signed product token>","upToDate":true}


---- dst::dst_tests::sse_delivery::an_empty_live_read_is_retried_without_another_append stdout ----

thread 'dst::dst_tests::sse_delivery::an_empty_live_read_is_retried_without_another_append' panicked at src/dst/tests/sse_delivery.rs:<line>:5:
no-progress: the record behind a failed live read must arrive without another append:
event: control
data:{"nextCursor":"<signed product token>","upToDate":true}

test result: FAILED. 0 passed; 2 failed; ...
```

The body holds only the head status. No `event: data`, no keep-alive (15 s default), no second status. Green run: both pass in about 0.3 s (first retry at 250 ms).

### 3b. Unit pins of the policy (the mutation killers)

File: `src/sse/session/tests.rs` (`#![cfg(test)]`, 132 lines now, about 180 after). Path `sse::session::tests::`, matched by `sse::`. These do not compile on 0afa2597 (`error[E0433]: failed to resolve: use of undeclared type ReadRetry`). They are new-type pins, not the behaviour red. Add `use std::time::Duration;`.

```rust
/// Item 33: a failed live read owes exactly ONE bounded park; a park
/// with nothing owed waits the lease nap alone (the retry is not a
/// heartbeat).
#[test]
fn a_failed_live_read_owes_exactly_one_short_park() {
    let hour = Duration::from_secs(3600);
    let mut retry = ReadRetry::IDLE;
    assert_eq!(retry.nap(0, hour), hour, "no failed read, no retry timer");
    retry.failed();
    assert_eq!(retry.nap(0, hour), Duration::from_millis(250), "a failed read shortens the next park");
    assert_eq!(retry.nap(0, hour), hour, "the owed retry belongs to one park");
}

/// Failures without progress back off to the cap, and the lease
/// deadline still bounds every park.
#[test]
fn failures_at_one_cursor_double_to_the_cap() {
    let hour = Duration::from_secs(3600);
    let mut retry = ReadRetry::IDLE;
    let waits: Vec<u128> = (0..7)
        .map(|_| {
            retry.failed();
            retry.nap(5, hour).as_millis()
        })
        .collect();
    assert_eq!(waits, [250, 500, 1000, 2000, 4000, 5000, 5000]);
    retry.failed();
    assert_eq!(retry.nap(5, Duration::from_secs(1)), Duration::from_secs(1), "a retry never postpones the lease deadline");
}

/// Progress resets the backoff: a failure at a later cursor is a new
/// episode.
#[test]
fn a_failure_after_progress_waits_the_first_delay_again() {
    let hour = Duration::from_secs(3600);
    let mut retry = ReadRetry::IDLE;
    retry.failed();
    assert_eq!(retry.nap(5, hour), Duration::from_millis(250));
    retry.failed();
    assert_eq!(retry.nap(5, hour), Duration::from_millis(500));
    retry.failed();
    assert_eq!(retry.nap(9, hour), Duration::from_millis(250), "a failure after progress starts over");
}
```

## 4. Edits, file by file, in commit order

**Commit count.** No verbatim-move commit is needed: no ceilinged file grows, and `session.rs` stays under 1,000 lines. There is one commit, because a red-only push would turn slate CI red. Demonstrate the red locally first (§7.1).

Suggested title: *A live read that failed or made no progress is re-driven on the session's own bounded backoff, not at the next append*

### Line budgets (`wc -l` @ 0afa2597 → after)

| File | Now | After | Ceiling |
|---|---|---|---|
| `src/sse/feed.rs` | 1,200 | 1,200 (line-neutral comment/doc edits) | 1,200 ceiling |
| `src/sse/session.rs` | 948 | 950 (+`mod`/`use`, outside `serve`) | 1,000 new-file rule |
| `src/sse/feed/drive.rs` | 214 | 214 | |
| `src/sse/session/read_retry.rs` | new | about 55 | |
| `src/sse/session/tests.rs` | 132 | about 180 | |
| `src/dst/tests/sse_delivery.rs` | 510 | about 600 | |

None of the other ceilinged files (http.rs 3,371, product.rs, shard.rs, billing.rs, history.rs, auth.rs 1,676, registry.rs, fleet.rs) is touched.

### Ratcheted exceptions this commit touches, and the remedy

- **`serve`** (`session.rs:154-896`: doc and attributes plus body).
  - Five `#[expect]`s: `too_many_arguments`, `too_many_lines`, `excessive_nesting`, `let_underscore_must_use`, `disallowed_methods`. There is no `unwrap_used`/`expect_used`, so there are no fingerprints, only `scope_lines`, `nested_items` and `syntax_facts`.
  - Remedy: the edit is **line-neutral and fact-neutral**. Arithmetic below; measure it per §7.5.
  - Fallback if the measured `syntax_facts` differs: re-decide all five reasons. Proposed texts (each with exactly two `;` and no `"`):
    - `too_many_lines`: "serve; the session is one subscribe, drive, bounded-retry and stream sequence whose teardown depends on which step admitted it; splitting it would separate the steps from the teardown they order"
    - `excessive_nesting`: "serve; the driver nests the lease, version, generation and read-retry waits and the take verdicts inside the drive loop of the spawned task; flattening them would separate each wake from the verdict it produces"
    - `let_underscore_must_use`: "serve; the changed waits are only registrations on watches the driver already owns beside its own read-retry timer; handled results would only restate the registration"
    - `disallowed_methods`: "serve; the driver task is owned by the subscription it drives, retries its own failed reads and ends with it; a supervised driver would tie a request-scoped task to the runtime supervisor"
    - `too_many_arguments`: "serve; the session takes the state, the surface, the descriptor, key, params, headers and lease parts as the handler authorized them; a request struct would restate the authorization the handler already proved"
- **`LiveFeed::read_and_publish`** (`feed.rs:906-1080`). It has a function-wide `#[expect(too_many_lines, …, expect_used, …)]`, so its expect sites and path/call fingerprints are recorded. Only a `//` comment changes. Comments are not facts and the edit is line-neutral, so every metric and fingerprint is unchanged. No remedy needed.
- **`impl LiveFeed` in `feed/drive.rs`** (impl-wide `#[expect(clippy::unwrap_used)]`, `drive.rs:37-40`). Only the `///` doc of `transition_pending` changes, with 2 lines replaced by 2 lines. Each `///` line is one attribute fact, so the count is unchanged. Attributes are not fingerprinted, and path and call sites are untouched. No remedy needed.
- **`DriveOutcome`** (`feed.rs:1159`): no exception covers it; doc edits only, line-neutral.
- **New code**: `read_retry.rs` and the new tests carry no `#[expect]`. Every function is ≤100 lines, nesting ≤4, ≤5 args, no bool params, and there is no unwrap/expect outside `#[cfg(test)]`.

### 4.1 `src/sse/session/read_retry.rs` (new)

```rust
//! The live phase's retry of a session's own failed read.
//!
//! A live drive whose read failed, or returned an empty partial page,
//! changed nothing a parked session waits on: the feed version is not
//! bumped (finding 6) and the source's advance notification already
//! fired for the records it could not read. Since round 11.1 no producer
//! heartbeat re-drives such a park, so without this bound the records
//! waited for the next append - and a sealed tail has none. Only the
//! session whose OWN drive failed owes itself this timer: a contended
//! session parks on the version watch alone, so a fan-out still has no
//! per-session timer herd.
use std::time::Duration;

pub(super) struct ReadRetry {
    /// The cursor whose read failed last and the wait that followed it:
    /// a failure at the same cursor means no progress since, so the wait
    /// doubles; a failure at a later cursor starts over.
    last: Option<(u64, Duration)>,
    /// A failed read owes the NEXT park one bounded wait. That park
    /// consumes it: any other wake re-drives (and re-arms on failure) or
    /// finds the session at the head, where no read is owed.
    owed: bool,
}

impl ReadRetry {
    /// No failed read owed: the session parks on its wakes alone.
    pub(super) const IDLE: Self = Self { last: None, owed: false };
    const FIRST: Duration = Duration::from_millis(250);
    /// One read per failing session per 5 s bounds what a persistently
    /// failing store or peer costs.
    const CAP: Duration = Duration::from_secs(5);

    pub(super) fn failed(&mut self) {
        self.owed = true;
    }

    /// The park's timer at `cursor`: the lease nap, shortened by the
    /// retry a failed read owes this park; a retry never postpones the
    /// lease deadline.
    pub(super) fn nap(&mut self, cursor: u64, lease_nap: Duration) -> Duration {
        if !std::mem::take(&mut self.owed) {
            return lease_nap;
        }
        let delay = match self.last {
            Some((at, waited)) if at == cursor => waited.saturating_mul(2).min(Self::CAP),
            Some(_) | None => Self::FIRST,
        };
        self.last = Some((cursor, delay));
        tracing::warn!(
            cursor,
            ?delay,
            "livefeed live read failed or made no progress; re-driving after a bounded park"
        );
        delay.min(lease_nap)
    }
}
```

Notes on this file:

- The match has no `_` arm (`Some(_) | None`).
- `saturating_mul` has no overflow path.
- `tracing::` macros are exempt from macro-dsl classification (`source_rules.py:54`).
- There is no glob import, no static and no spawn. No `crate::http` reference means no architecture-policy row.
- The docs name no private item as an intra-doc link, so rustdoc `-D warnings` is clean.

### 4.2 `src/sse/session.rs`

Outside `serve`:

- After `use std::sync::atomic::Ordering;` (line 29), add `mod read_retry;` and `use read_retry::ReadRetry;`. That is +2 lines, 948 → 950.
- The import makes `ReadRetry` visible to `session/tests.rs` through its existing `use super::*`.

Inside `serve`: six edits. The comment edits also fix the three stale "heartbeat" comments and the "no per-session timer" line.

- **S1** (after `let mut transition_retries = 0u32;`, line 349): +2 lines, +1 fact (`ReadRetry::IDLE` path).
  ```rust
          // The bounded retry this session owes its own failed live read.
          let mut read_retry = ReadRetry::IDLE;
  ```
- **S2** (lines 740-742, closed-branch fall-through): 3 → 2 lines, −1 line, 0 facts.
  ```rust
                                      // Fall through to the park: the feed's
                                      // retry task or the next append wakes us.
  ```
- **S3** (line 824): `Some(DriveOutcome::Published) | Some(DriveOutcome::Closed) => continue,` becomes `Some(DriveOutcome::Published | DriveOutcome::Closed) => continue,`. 0 lines, −1 fact (one `Some` path). This is the or-pattern style its neighbour arms already use.
- **S4** (lines 836-846): 11 → 10 lines, −1 line. Facts: old arms 8 (`Some`, `NoProgress`, `Idle` / `Some`, `SourceFailed`, and `warn!` = macro + macro-tokens + path). New arms 8 (`Some`, `Idle` / `Some`, `NoProgress`, `SourceFailed`, and `read_retry.failed()` = method-call + method-call-site + receiver path). `None` in a pattern is `Pat::Ident`, not a path.
  ```rust
                          // Idle means the head already covers the frontier;
                          // None means another driver won and its publication
                          // bumps ver_wait (registered at loop top).
                          Some(DriveOutcome::Idle) | None => {}
                          // A failed or empty read changed nothing and bumped
                          // nothing (finding 6): no wake is owed, so the park
                          // below is bounded by this session's own retry.
                          Some(DriveOutcome::NoProgress | DriveOutcome::SourceFailed) => {
                              read_retry.failed()
                          }
  ```
  The one-line arm would be 108 columns, so rustfmt keeps the block. There is no `match_same_arms` collision.
- **S5** (lines 868-871, park comment): 4 → 4 lines.
  ```rust
                  // Park. Seal-publication convergence is the feed's ONE
                  // retry task (round-11.1): it never reads, and its bump
                  // wakes this park. The only per-session timer is the
                  // bounded retry a failed read of THIS session's drive owes.
  ```
- **S6** (line 881, inside `tokio::select!`): `_ = tokio::time::sleep(lease_watch.nap()) => {` becomes `_ = tokio::time::sleep(read_retry.nap(cursor, lease_watch.nap())) => {`. It is 88 columns. Macro tokens are one `macro-tokens` fact whose value changes and whose count does not. The `select!` count stays 3, matching `source-allowances.json`.

**Net for `serve`: lines +2 −1 −1 = 0 (743 → 743); facts +1 −1 +0 = 0 (875 → 875).**

How the `failed()` arm reaches the park: it lies in the `if cursor < frontier` drive block, so after it control reaches the `cut_off` check and then the park. The owed retry is always consumed by that very park, or dropped by a `return`.

### 4.3 `src/sse/feed.rs` (comments and docs only, 1,200 → 1,200)

- **F1** (lines 945-947, `//` inside `read_and_publish`):
  ```rust
          // No-progress partial page (finding 6): nothing scanned, nothing
          // matched — report it WITHOUT touching head/version. The driving
          // session re-drives it on its own bounded retry (ReadRetry).
  ```
- **F2** (line 1174): `/// The session parks instead of spinning (finding 6).` becomes `/// The driving session retries on a bounded backoff (finding 6).`
- **F3** (lines 1181-1182): the second line becomes `/// driving session retries on a bounded backoff (finding 6).`

### 4.4 `src/sse/feed/drive.rs` (doc of `transition_pending`, 214 → 214)

Lines 95-96 become:

```rust
    /// a session parked behind a failed read of that tail has only this
    /// tick and its driver's own bounded retry to wake it. It is
```

### 4.5 `src/sse/session/tests.rs`

Add `use std::time::Duration;` and the three §3b tests.

### 4.6 `src/dst/tests/sse_delivery.rs`

Append §3a: `collect_session`, `live_read_fault_is_retried` and the two tests. The imports it needs (`http_rig`, `mem`, `engine_shutdown`) are already present. No new DST module is added, so there is no README or `dst_tests.rs` `#[path]` row.

### 4.7 Ledgers and docs (same commit; see §6)

## 5. Mutation-kill analysis

**Selected owners** (planner, in-diff):

| Owner | Source | Filters |
|---|---|---|
| `sse_session` | `src/sse/session.rs` | `sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap:: livefeed_engine_retired` |
| **`sse_session_read_retry`** (new row) | `src/sse/session/read_retry.rs` | `sse::` |
| `sse_feed` | `src/sse/feed.rs` | `sse::` (doc attributes changed) |
| `sse_feed_drive` | `src/sse/feed/drive.rs` | `sse::` (doc attributes changed) |

Not selected:

- `session/tests.rs` is `#![cfg(test)]`, so it is `production_unchanged`.
- `src/dst/**` is outside every critical prefix.

**`read_retry.rs` mutants** (every line is in the diff; cargo-mutants 27.1 genres):

| Mutant | Killed by (assertion) |
|---|---|
| replace `ReadRetry::failed` with `()` | `a_failed_live_read_owes_exactly_one_short_park` ("a failed read shortens the next park": expects 250 ms, gets 3600 s) |
| replace `ReadRetry::nap -> Duration` with `Default::default()` | same test, first assert (expects 3600 s, gets 0) |
| delete `!` in `ReadRetry::nap` | same test, first assert (nothing owed but returns 250 ms) |
| match guard `at == cursor` → `true` | `a_failure_after_progress_waits_the_first_delay_again` (expects 250 ms at cursor 9, gets 1 s) |
| match guard `at == cursor` → `false` | `failures_at_one_cursor_double_to_the_cap` (expects `[250, 500, …]`, gets `[250, 250, …]`) |
| `==` → `!=` in `ReadRetry::nap` | both of the two tests above |

What produces no mutants:

- `Some(_) | None` is not a wildcard, so there is no arm-deletion mutant.
- `saturating_mul`, `min` and `take` are method calls, and cargo-mutants does not mutate those.
- Consts are not mutated.

There are no equivalent mutants. No unit test waits on anything, so there is no timeout risk.

The `nap → ZERO` mutant would hot-loop a real session. But the `sse_session_read_retry` filter (`sse::`) does not select the DST tests (`dst_tests::sse_delivery::` does not contain `sse::`), and the unit test kills that mutant at once.

**`session.rs` mutants.** The in-diff lines are S1-S6 plus `mod`/`use`:

- None of these lines adds a binary or unary operator, a guard or a wildcard arm. Code inside `select!` is macro tokens and is not mutated.
- The body change selects `replace serve -> axum::response::Response with Default::default()`. It is viable, since `http::Response<Body>: Default`.
  - It is killed by the new tests: the empty body fails "the session parks at the head" immediately.
  - It is also killed by the existing `sse_delivery` tests, as in d93b421b and 4247421f.

**`feed.rs` and `drive.rs`.** Only `//` and `///` lines change:

- In `drive.rs` the edit is a doc comment, outside `transition_pending`'s body span, so no mutant is selected.
- `feed.rs` F1 sits inside `read_and_publish`'s body. At most it selects the `DriveOutcome` FnValue replacement, which is unviable because `DriveOutcome` has no `Default`, so it cannot count as a miss.

The expected receipt is 0 missed and 0 timeouts.

**Owner table change** (`scripts/quality/mutation_owners.py`, after the `sse_session` row):

```python
    owner('sse_session_read_retry', 'src/sse/session/read_retry.rs', 'sse::'),
```

**No Loom test (item 48).** `ReadRetry` is task-owned (`&mut`, not `Send`-shared) and adds no lock, atomic or ordering. The change is one more deadline in a park's wake set, not a synchronization primitive. The deterministic session-level DST reds are the liveness proof. The planner still selects the existing Loom leg (the `src/sse` lifecycle prefix); those tests are unaffected.

## 6. Ledgers

- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write`, which adds 2 DST entries (the §3a tests, file `src/dst/tests/sse_delivery.rs`, `scenarios: []`). Unit tests are not inventoried.
- `scripts/quality/mutation_owners.py`: the new `sse_session_read_retry` row (§5).
- `docs/quality/owners.json`: no row.
  - `read_retry.rs` has no glob import, static, spawn, `#[path]` module or DSL macro.
  - `tracing::warn!` is exempt.
  - `session/tests.rs` already has its `unresolved-glob` row.
- `docs/quality/source-allowances.json`: unchanged (`serve` still has `tokio::select` ×3, `tokio::pin` ×1 and `tokio::spawn` ×1).
- `docs/refactor/architecture-policy.json`: no row (the new file does not reference `crate::http`).
- `docs/refactor/review-mechanisms.json`: no pinned test body changes.
- `src/dst/tests/README.md`: no new module.
- `docs/LIVE-FEED.md:35-36` becomes:
  `- park until: feed version change, source advance or swap, lease deadline,`
  `  the bounded retry of its own failed read (250 ms doubling to 5 s), own cancellation;`
- `docs/refactor/WIRE-MATRIX.md:218`: this is not a wire change, but the claim becomes true and should state the bound. Replace `the SSE feed parks and retries on its own cadence` with `the SSE session re-drives the failed read on its own bounded backoff, 250 ms doubling to 5 s`.

## 7. Controls (exact commands; to be run by the implementer)

1. **Red on 0afa2597** (scratch worktree, test hunk only):
   ```
   git worktree add /tmp/streams-red 0afa2597
   git -C /Users/sorenschmidt/code/streams diff 0afa2597 <fix> -- src/dst/tests/sse_delivery.rs | git -C /tmp/streams-red apply
   cd /tmp/streams-red && cargo test --locked --lib -- dst::dst_tests::sse_delivery::a_failed_live_read_is_retried_without_another_append dst::dst_tests::sse_delivery::an_empty_live_read_is_retried_without_another_append
   ```
   Expected: `test result: FAILED. 0 passed; 2 failed`, with the two panics in §3a, about 5 s each.
2. **Green:** run the same command on the fix. Expected: `test result: ok. 2 passed`, each well under 1 s.
3. **Units:** `cargo test --locked --lib -- sse::session::tests`. Expected: `test result: ok. 6 passed` (3 existing plus 3 new).
4. **Owner filters as CI runs them:**
   `cargo test --locked --lib -- sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap:: livefeed_engine_retired dst_tests::livefeed_`. Expected: ok, and the existing `source_read_count` assertions (livefeed_basics 1/2/≤4) are unchanged because no fault occurs in them.
5. **`serve` ratchet** (before at 0afa2597, after at the fix; the rows must be identical):
   ```
   cargo build --locked -p streams-quality-syntax
   python3 - <<'EOF'
   import sys; sys.path.insert(0, 'scripts/quality')
   from common import syntax
   from source_rules import exception_contracts
   for path in ('src/sse/session.rs', 'src/sse/feed.rs', 'src/sse/feed/drive.rs'):
       src = {path: open(path).read()}
       for (p, q, kind, value), m in sorted(exception_contracts(src, syntax(src)).items()):
           print(p, q, value.split('reason')[0].strip()[:60], m['scope_lines'], m['nested_items'], m['syntax_facts'],
                 sum(v for k, v in m.items() if ':' in k))
   EOF
   ```
   Expected: the five `crate::serve` rows read `743 … 875` before and after. The `read_and_publish` and drive-impl rows and their fingerprint totals are unchanged. If not, rebalance or apply the §4 fallback reasons.
6. `cargo fmt --all -- --check` produces no output.
7. `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl; python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl` exits 0: no `file growth`, no `accepted exception grew`, no unfulfilled expectation.
8. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` succeeds.
9. `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check` reports OK (+2 entries). `python3 scripts/architecture-gate.py --check` reports OK.
10. **Plan before push:**
    ```
    python3 scripts/quality/verification_plan.py --out target/quality-plan
    python3 -c "import json;p=json.load(open('target/quality-plan/plan.json'));print(p['mutation_source_files'],p['selected_mutation_owners'],p['unregistered_mutation_source_files'],p['production_unchanged_files'])"
    ```
    Expected:
    - `mutation_source_files` ⊇ `src/sse/session.rs` and `src/sse/session/read_retry.rs`, plus `src/sse/feed.rs` and `src/sse/feed/drive.rs`;
    - owners `sse_session`, `sse_session_read_retry`, `sse_feed`, `sse_feed_drive`;
    - `unregistered` is `[]`;
    - `src/sse/session/tests.rs` is in `production_unchanged_files`.
11. `scripts/quality/mutations.sh` reports 0 missed and 0 timeouts: 6 of 6 caught in `read_retry.rs`, 1 of 1 caught in `session.rs`.
12. `scripts/quality.sh` ends with `QUALITY_OK`. After the push, confirm with `gh run view <run-id>`; never claim CI green without it.

## 8. Out of scope

- **A contended session left without a driver.** Contended (`None`) sessions parked behind a failed read rely on the failing driver's retry. If that session disconnects before retrying, they wait for the next append again: on an open source always, on a closed tail if no transition retry task is armed. This behaviour already exists today; the fix narrows it. Closing it needs a per-feed owner (a separate spawn plus its effect row) or a timer on every contended park (the herd round 11.1 removed). Both are decisions for later.
- **Phase-A catch-up retry** (`session.rs:459-464`). It is a fixed 100 ms sleep, not raced against `tx.closed()`, with no backoff.
- **The closed-at-frontier arm** (`session.rs:721-743`: transition retry task plus the 64-retry cap) is unchanged.
- **`read_and_publish` drops the source error `e` unlogged** (`feed.rs:941-942`). Logging it would add facts under that function's `expect_used` fingerprint, and `feed.rs` is at its 1,200-line ceiling.
- **The `schedule_transition_retry` bump-before-`store(false)` window** for its own closed-tail uses (§1). It is not needed by this fix.
- **An HTTP-reachable read-fault seam in `SingleSource::read_batch`.** It would require re-deciding the impl-wide `unwrap_used` reason.
- `source.rs:813`'s historical "and its heartbeat" wording (round-6 note in `refresh_transition`).

## Skeptic corrections (C1..C9)

Checked against the tree at HEAD = origin/slate = **47799eb3** (read-only; no cargo/scripts run). Claims I verified and found correct are listed at the end so the implementer does not re-check them.

**C1 (blocks control 7 as written): `.ok().expect(..)` fails clippy.** §3a line `crate::http::sse_acquire(&state).ok().expect("an SSE slot")` triggers `clippy::ok_expect`. It is in the style group, and `Cargo.toml:122` enables `all = warn`, so `-D warnings` turns it into an error. `allow-expect-in-tests` (`clippy.toml`) covers `expect_used` only, not `ok_expect`. The lint fires because the error type is Debug: `Box<axum::response::Response>` has a Debug impl, since `axum_core::body::Body` is `#[derive(Debug)]` (axum-core 0.5.6 `src/body.rs:38`). Fix: `let Ok(slot) = crate::http::sse_acquire(&state) else { panic!("an SSE slot") };`, or call `.expect("an SSE slot")` on the `Result` directly, which works because E is Debug. The red and green runs are unaffected, since this is a lint and not a compile error.

**C2: base drift.** HEAD is 47799eb3, two commits past 0afa2597. Those commits (0a8ed40a, 47799eb3) touch only `architecture-policy.json`, `architecture-gate.py`, `quality.sh`, `diagnostics.py` and `review-verification-evidence.md`. `git diff --stat 0afa2597 HEAD` shows no file this plan edits, so every line number and `wc -l` in §4 still holds: session.rs 948, feed.rs 1,200, drive.rs 214, session/tests.rs 132, sse_delivery.rs 510, http.rs 3,371. State 47799eb3 as the base. Note what 47799eb3 adds: the architecture gate now fails an *obsolete* budget exception. `function:src/sse/session.rs::serve` has limit 721 against a baseline of 715 (`architecture-policy.json`), and serve is measured as 718 physical lines (179..896). The net-zero line edit keeps it inside the limit and still needed. Do not let serve shrink to ≤715 in a follow-up without deleting that exception. The red worktree in §7.1 should live under the scratchpad, not `/tmp/streams-red`.

**C3: a missed ratchet scope, harmless here.** `read_and_publish` sits under TWO exceptions, not one. One is its own fn-level `#[expect(too_many_lines, …, expect_used)]` (feed.rs:909-917). The other is the impl-wide `#[expect(clippy::unwrap_used)]` on `impl LiveFeed` (feed.rs:604-607), which fingerprints every `path`, `call-site` and `method-call-site` fact of the whole impl (source_rules.py:159-188). F1 is `//`-only and line-neutral, so neither scope moves. But §4's "Ratcheted exceptions this commit touches" must name both. The §8 note on logging `e` should say it would re-fingerprint both reasons, not one. The §7.5 script already prints every row of feed.rs, so the control catches it.

**C4: the new log is a level regression that the backoff cannot fix.** Today NoProgress logs only at `debug`, and only for *partial* pages (feed.rs:949-954). A *completed* empty page, which also returns `NoProgress` (feed.rs:948 then 956), is not logged at all. §4.1 emits `tracing::warn!` on every owed park, so every NoProgress now warns, including the benign completed-empty frontier race. That is up to 4 warns in the first 2 s per session. Use `tracing::debug!` in `nap` (or `info!`). The typed counters `FEED_SOURCE_FAILED` and `FEED_NO_PROGRESS` remain the operational signal. Do NOT split the level on the outcome or on "reached CAP": a branch that only changes a log level produces guard/`==` mutants that no test can observe (equivalent mutants), which the mutation leg forbids. If a SourceFailed-only warn is wanted, it has to stay in `serve`'s arm, and that breaks the fact-neutral arithmetic of S4 (+3 facts for `warn!`). Keep one unbranched log line.

**C5: the "no timer herd" claim is overstated (§2, §4.1 module doc).** Contention (`None`) happens only while another session holds the permit. A fan-out session that acquires the permit *after* the failed driver released it drives on its own, fails, and arms its own `ReadRetry`. So under a persistently failing source, up to N sessions of a feed can own timers. The permit serializes their reads, and the bound is ≤N reads per 5 s cap, not one per feed. State that bound in the doc and in §2. It is still far below the removed producer-heartbeat herd, so it is not a blocker.

**C6: the contract decision must be listed for Søren (§2 says none is needed).** The change is visible at the fleet-internal edge. A sealed-span peer answering 429/503 (`read_remote.rs:193-199`, `RemoteSpanError::Retryable`, documented at `WIRE-MATRIX.md:218`) is now re-read by each driving session at 250 ms, doubling to 5 s. Today the session parked until the lease nap. The retry does not read the peer's `Retry-After`. List it as decision D1: "a failed or empty live read is re-driven on a per-session 250 ms→5 s backoff; peer Retry-After is not honoured". The backward-compatible alternative is today's behaviour: park until the next append or the lease nap, and fix only the stale comments.

**C7: §7.10's expected plan output may be wrong for feed.rs and drive.rs.** Their edits are `//` comments and `///` docs only. `production_changes.py` normalizes `doc` attributes and comments, so the planner may put `src/sse/feed.rs` and `src/sse/feed/drive.rs` in `production_unchanged_files` instead of `mutation_source_files`. It may keep them only if an item macro in feed.rs makes the comparison return `None`, which is conservative. Either outcome is acceptable, so write the expectation as "in `mutation_source_files` with 0 viable mutants, OR in `production_unchanged_files`". Owners `sse_feed` and `sse_feed_drive` may then be absent from `selected_mutation_owners`. Only `sse_session` and `sse_session_read_retry` are required.

**C8: control 7 cannot run as written on a clean tree.** `cargo clippy … > target/quality/clippy.jsonl` fails if `target/quality/` does not exist. Prefix it with `mkdir -p target/quality`, which is what `scripts/quality.sh:6` does with `$QUALITY_OUT`.

**C9: nits.**
- The single-line S4 arm is 107 columns, not 108. The conclusion stands: rustfmt keeps the block.
- `ReadRetry::failed` has no doc comment. Per the style rule, give it one stating why it exists, for example "a read that changed nothing owes the next park a bounded wait (finding 6)".
- The S2 text "the feed's retry task or the next append wakes us" is accurate: that arm has just called `schedule_transition_retry`.

**Verified as claimed (no correction):**
- Every quoted line and number: session.rs:349, 497-499, 729, 740-742, 770, 824, 836-846, 868-871, 881 and 459-464; feed.rs:941-956, 1174 and 1181-1182; drive.rs:42-47, 79, 94-96 and 106-109; auth.rs:293-296; source.rs:721-727; `POLICY_STALENESS_MAX_SECS = 300`; `wake_all_sessions` only in the fleet tick (fleet.rs:812); the 15 s body keep-alive (config/model.rs:419).
- `serve`'s five expects carry no unwrap/expect lints, so there are no fingerprints. scope_lines 743 is 154..896, unchanged since 4247421f (d93b421b changed only macro tokens in serve).
- Fact arithmetic, checked against the fact kinds in `tools/quality-syntax/src/scan.rs`:
  - S1: +1 path.
  - S3: −1 (a `Some` path).
  - S4: old 8 = 3 + 2 paths + `warn!` (macro + macro-tokens + macro path). New 8 = 2 + 3 paths + method-call + method-call-site + receiver path.
  - S6: macro-tokens count unchanged.
  - Lines: +2 −1 −1 = 0.
- The `tokio::select` macro-dsl allowance counts per owner (source-allowances.json, `crate::serve` ×3) and is unchanged.
- The red trace holds. FakeSource(0, 8) with `StartPos::At(0)` means no catch-up read. The first iteration is at head (status sent, no drive). The notify wakes the park registered at the loop top. The second iteration drives, the read fails or makes no progress, `drive_under_permit` does not bump, and the next park has nothing to wake it (unleased nap 3600 s, keep-alive 15 s > the 5 s collect). The `ran()` signal is exact (feed/tests.rs:108-125). After the fix, the 250 ms retry yields Solo, and the frame is `event: data\ndata:0\n\n` followed by the control.
- `FakeSource` and `test_desc` are reachable: `pub(crate) mod tests` is at feed.rs:1200. `serve`, `sse_acquire`, `StartPos`, `ReadParams: Default`, `SseSurface` and `StreamKey(pub [u8; 32])` are all `pub(crate)`.
- A non-capturing closure coerces to `fn(&FakeSource) -> &AtomicBool`.
- The unit-test arithmetic is right: [250, 500, 1000, 2000, 4000, 5000, 5000], then 1 s under a 1 s lease, 250 again after progress, and exactly one owed park.
- The six `read_retry.rs` mutants are all killed, and there is no equivalent mutant. `Some(_) | None` is not a wildcard arm. The only in-diff mutant in session.rs is serve's FnValue, which is viable and killed.
- The `sse::` filter selects no test that drives `serve`, so the `nap → ZERO` mutant cannot hot-loop a selected test.
- `session/tests.rs` is `#![cfg(test)]`, so it is production-unchanged and needs no owner row. The owner row, test-inventory (+2 DST entries), LIVE-FEED.md:35 and WIRE-MATRIX.md:218 edits are the complete ledger set. No owners.json, source-allowances, architecture-policy, review-mechanisms or README row is needed.
- session.rs 948→950 stays under 1,000. read_retry.rs is not an `sse_core_files` entry and has no `crate::http` edge.

**Verdict: ready-with-corrections.** C1 and C8 must be fixed before control 7 can pass. C4 and C6 change the commit (log level, and a decision line for Søren). C3, C5 and C7 are documentation and expectation fixes.
