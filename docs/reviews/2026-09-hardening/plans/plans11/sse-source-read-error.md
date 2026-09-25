# Item 87: a typed `SourceReadError` replaces the anyhow downcast in the SSE source

Tree: `slate` at `33fbd10e`. `origin/slate` is also `33fbd10e`, so the task text's "one commit ahead of `2f2c3015`" is out of date. The merge base (`git merge-base HEAD origin/slate`) and a push's `before` are both `33fbd10e`. Every line number below was checked against that tree. The reviewer's line numbers are stale: `feed.rs:1083-1099` is now `feed.rs:910-928`, and item 86 moved `session.rs:447-464` to `src/sse/session/catch_up.rs:37-68` (`stalled()`).

Summary. `FeedSourceRead::read_batch` returns `Result<SourceBatch, SourceReadError>`. The type is `enum SourceReadError { Fatal(SourceCutoff), Retryable(anyhow::Error) }`, declared in `feed.rs` next to the trait and next to `SourceCutoff`, and it has no `From<anyhow::Error>`. `FatalSpanCutoff`, its `Display` and `Error` impls, and both `downcast_ref` sites are deleted. The two consumers match on it exhaustively:
- The feed drive's failure verdict moves into `LiveFeed::read_failed` in `drive.rs`, which retires through the existing `retire`.
- The catch-up verdict stays in `catch_up::stalled`.

Both consumers now log the retried cause at debug level. This is the only behaviour change, and it has red tests. The remote-refusal table leaves `sealed_span_page` and becomes an engine-free `remote_span_verdict` in `source/spans.rs`, with a unit test. `sealed_span_page` drops below 100 lines as a result and loses its `too_many_lines` exception. The wire and metrics do not change. The work is two commits: C1 adds a pin that is green on HEAD, and C2 is the change.

---

## 1. Problem (checked against `33fbd10e`)

### 1a. Fatal-vs-retry travels as a runtime downcast

The trait returns anyhow (`src/sse/feed.rs:89`):
```rust
async fn read_batch(&self, from: u64, max_bytes: usize) -> anyhow::Result<SourceBatch>;
```
The fatal case is a marker struct that is smuggled through anyhow (`src/sse/source/spans.rs:5-17`):
```rust
/// Round-11.2: a FATAL span error carried through anyhow — the feed
/// downcasts it and turns the source's lifecycle into the typed
/// cutoff instead of retrying forever.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FatalSpanCutoff(pub(crate) crate::sse::feed::SourceCutoff);
impl std::fmt::Display for FatalSpanCutoff { ... "fatal span cutoff: {:?}" ... }
impl std::error::Error for FatalSpanCutoff {}
```
It is re-exported at `src/sse/source.rs:976`: `pub(crate) use spans::FatalSpanCutoff;`.

Two consumers each recover the verdict with `downcast_ref`. Any error that is not the marker falls through as "retry", and its content is dropped:

- The feed drive, `LiveFeed::read_and_publish` (`src/sse/feed.rs:910-928`):
  ```rust
  let batch = match read {
      Ok(x) => x,
      Err(e) => {
          // Round-11.2: FATAL span outcomes become the typed
          // lifecycle cutoff — never an endless retry.
          if let Some(cut) = e.downcast_ref::<crate::sse::source::FatalSpanCutoff>() {
              let reason = cut.0;
              let mut st = self.st.lock().unwrap();
              st.lifecycle = Lifecycle::Gone(reason);
              st.version += 1;
              let ver = st.version;
              drop(st);
              crate::sse::auth::sse_stats::FEED_VERSION_BUMPS.fetch_add(1, Ordering::Relaxed);
              let _ = self.changed.send(ver);
              return DriveOutcome::IncarnationClosed(reason);
          }
          crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.fetch_add(1, Ordering::Relaxed);
          return DriveOutcome::SourceFailed;
      }
  };
  ```
  `e` is dropped without being logged. The fatal block also repeats `LiveFeed::retire` (`src/sse/feed/drive.rs:196-213`) line for line: it sets the lifecycle, bumps the version, bumps `FEED_VERSION_BUMPS` and sends on the watch. The doc of `drive_under_permit` (`drive.rs:127-131`) already says "A lifecycle transition bumped at the transition itself (`retire`)". The read cutoff is the only Gone transition that does not go through `retire`.
- The catch-up pass, `catch_up::stalled` (`src/sse/session/catch_up.rs:37-68`, moved there by item 86):
  ```rust
  pub(super) async fn stalled(read: anyhow::Result<SourceBatch>) -> Stall {
      let e = match read { Ok(page) => { ... return Stall::NoProgress; } Err(e) => e };
      if let Some(cut) = e.downcast_ref::<crate::sse::source::FatalSpanCutoff>() {
          super::count_cutoff(cut.0); ... return Stall::Cutoff;
      }
      crate::sse::auth::sse_stats::FEED_SOURCE_FAILED
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      tokio::time::sleep(RETRY).await;
      Stall::Failed
  }
  ```
  Here too `e` is dropped without being logged.

Verified: nothing in the tree gives `FEED_SOURCE_FAILED` a cause. It is a bare counter, exposed as `/v1/debug/load` `sse_livefeed.source_failed` (`src/http.rs:879`), and neither bump site above logs anything.

### 1b. Every use site

| Role | Site (HEAD) |
|---|---|
| Trait declaration | `src/sse/feed.rs:89` |
| Implementors | `SingleSource::read_batch` `src/sse/source.rs:48`; `LineageSource::read_batch` `source.rs:570`; test `FakeSource::read_batch` `src/sse/feed/tests.rs:112` |
| Helper returning anyhow into `read_batch` | `LineageSource::sealed_span_page` `source.rs:419-552` (its doc at 409-411 states the anyhow convention) |
| Callers | `feed.rs:905` (inside `tokio::select!`), `src/sse/session.rs:371` (inside `tokio::select!`, which hands `read` to `catch_up::stalled` at :428), test `src/sse/feed/tests/fixture.rs:86` (`.unwrap()`) |
| Fatal producers (`anyhow::Error::new(FatalSpanCutoff(..))`), 6 | `source.rs:52` (Single live tail: `live_tail_cutoff` -> WrongOwner/EngineRetired); `source.rs:594` (Lineage LiveLocal tail, same); `source.rs:526` Unauthorized -> FleetAuth; `:529` TargetGone -> IncarnationChanged; `:532` TargetMismatch -> TargetMismatch; `:544-546` RedirectLoop -> RedirectLoop |
| Retryable producers (plain anyhow), 12 | `source.rs:66` and `:79` (Single read_stitched/read_merged `map_err(anyhow!)`); `:440` stream handle; `:459` segment ReadPlan; `:465` `bail!` engine unavailable; `:480-485` ownership indeterminate; `:516-518` remote Retryable; `:519-521` Transport; `:522-524` InvalidResponse; `:548-550` remote WrongOwner; `:611` Lineage LiveLocal ReadPlan; `:657` `bail!` "lineage span ended below its cap". Test: `feed/tests.rs:117-119` `bail!("injected source failure")` |
| Downcast consumers | `feed.rs:915`; `catch_up.rs:54` |
| Marker type and re-export | `spans.rs:5-17`; `source.rs:976` |
| Tests on the marker | `src/sse/session/tests.rs:206-214` (`Err(anyhow::Error::new(cut))`, `Err(anyhow::anyhow!("injected"))`); `src/sse/source/tests.rs:44-50` `a_fatal_span_cutoff_names_its_reason` (pins the Display text only) |
| Prose naming the marker | `spans.rs:1-2` (module doc); `source.rs:409-411`; `src/product/internal.rs:6-7` ("made it `FatalSpanCutoff(IncarnationChanged)`") |
| `FEED_SOURCE_FAILED` bumps | `feed.rs:926` and `catch_up.rs:64` (both without a cause); `drive.rs:177` (`next_source()` `Err(_)`, not a read; out of scope, §8); exposition `http.rs:879`; DST diagnostic read `src/dst/tests/livefeed_swap.rs:421` |

No other error that reaches either downcast can carry the marker. Every `map_err(|e| anyhow::anyhow!(e))` wraps a non-anyhow application error, and `FatalSpanCutoff` is built only in `source.rs` (grep). The typed rewrite therefore keeps the classification exactly as it is.

### 1c. The reviewer's "no unit test covers the downcast-to-Gone path"

This is half true. The catch-up path gained `sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict` in item 86. The feed-drive path has no unit test, because `FakeSource` cannot produce a fatal read: it has `fail_reads` (retryable) only. Its Gone transitions are covered only through `next_source` and `install_source`, in `feed/tests/retry.rs:220-245`. The remote refusal table inside `sealed_span_page` has no unit test at all. Only the multi-instance DST covers it: `livefeed_owner_movement_one_redirect_and_typed_cutoffs` (RedirectLoop, WrongOwner).

### 1d. Rank 14's new cutoff variant

`SourceCutoff::EngineRetired` has already landed (`d93b421b`). It is produced by `live_tail_cutoff` at both live-tail guards, so there is nothing left to combine: `Fatal(SourceCutoff)` carries it unchanged.

---

## 2. Contract decision

The type is declared in `src/sse/feed.rs` immediately after `SourceCutoff`. The reviewer proposed `source/spans.rs`. `feed.rs` owns the trait, its `Ok` type (`SourceBatch`) and the cutoff reasons, so the error belongs beside them. Declaring it there also removes feed.rs's only dependency on `crate::sse::source`, the downcast path at :915.

```rust
/// A failed source read, typed by what its consumer owes it. No
/// `From<anyhow::Error>` exists: each failure site names its verdict, so
/// a `?` can never turn a cutoff into a retry or a retry into a cutoff.
#[derive(Debug)]
pub(crate) enum SourceReadError {
    /// Sessions disconnect without a terminal control and resume.
    Fatal(SourceCutoff),
    /// The same bound is read again after a bounded backoff.
    Retryable(anyhow::Error),
}
```
- `read_batch` (trait and all three impls) returns `Result<SourceBatch, SourceReadError>`. `sealed_span_page` returns `Result<ReadPage, SourceReadError>`.
- There is deliberately no `From<anyhow::Error>`. Because of that, `anyhow::bail!` and a bare `?` on an anyhow result do not compile inside these functions. The compiler enumerates every failure site, and each one names its variant (the table in 1b).
- Consumers: `LiveFeed::read_failed` (feed drive) and `catch_up::stalled` (catch-up). Both match exhaustively with no wildcard. Adding a variant fails to compile in both, and feed's `#![warn(clippy::wildcard_enum_match_arm)]` also covers `drive.rs`.

**No wire change.** No HTTP status, body, header or cursor changes. The metrics do not change either:
- `FEED_SOURCE_FAILED`, `FEED_TOPOLOGY_DISCONNECTS`, `FEED_CUTOFF_*` and `FEED_VERSION_BUMPS` are bumped at the same verdicts, the same number of times.
- `/metrics` and the `/v1/debug/load` JSON keep the same names and shapes.

The disconnect semantics also stay the same: a cutoff is still a nonterminal EOF and a retry still uses the same backoff. The only new output is diagnostic. There is one debug event per retried read, with an `error` field, in the feed drive and in the catch-up pass. The RedirectLoop `warn!` moves unchanged into `spans.rs`, so its tracing target becomes `streams_slate::sse::source::spans`. Filters on the target prefix `streams_slate::sse::source` still match it. None of this is a documented contract, and `docs/refactor/WIRE-MATRIX.md` and `docs/LIVE-FEED.md` do not mention any of it (grep).

Synchronization: the fatal feed read now goes through `retire`, which takes the state lock twice (lifecycle, then `bump_version`) instead of once. No observer can tell the difference:
- `FeedState::lifecycle` is read only by `tail()`, under the driver permit that `read_and_publish` itself holds, and by `lifecycle_for_test`.
- The watch `send` still happens after both writes, which is the only wake.
- `retire` is already the transition for the other two Gone paths and the Closed path (pinned by `retry_cuts_the_feed_off_on_an_incompatible_successor`: "one bump at the transition itself").

No new primitive or ordering is introduced, so this is not a synchronization change that needs Loom. P1 (§3) pins the observable result.

---

## 3. Tests

### 3a. Red tests (the one behaviour change: the retried cause is logged)

Shared helper, new file `src/sse/test_log.rs` (`#![cfg(test)]`, declared in `src/sse/mod.rs` as `#[cfg(test)] pub(crate) mod test_log;`). It uses explicit imports and no glob (§6):
```rust
//! The `error` field of every tracing event on this thread, for tests of
//! diagnostics that have no other observer: a retried source read's cause
//! reaches operators only through its log.
#![cfg(test)]
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::{Context, SubscriberExt};

/// Captures while alive; the thread's previous subscriber returns on drop.
pub(crate) struct ErrorLog {
    causes: Arc<Mutex<Vec<String>>>,
    _guard: tracing::subscriber::DefaultGuard,
}

impl ErrorLog {
    pub(crate) fn capture() -> Self {
        let causes = Arc::new(Mutex::new(Vec::new()));
        let layer = ErrorFields(Arc::clone(&causes));
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        Self { causes, _guard: guard }
    }

    pub(crate) fn causes(&self) -> Vec<String> {
        self.causes.lock().unwrap().clone()
    }
}

struct ErrorFields(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ErrorFields {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        event.record(&mut ErrorField(&self.0));
    }
}

struct ErrorField<'a>(&'a Mutex<Vec<String>>);

impl tracing::field::Visit for ErrorField<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "error" {
            self.0.lock().unwrap().push(format!("{value:?}"));
        }
    }
}
```
Notes on the helper:
- `tracing-subscriber` is already a normal dependency (`Cargo.toml:30`) and its default features include `registry`.
- `set_default` is thread-local. `#[tokio::test]` is current-thread, so the drive emits on the test thread, and parallel tests cannot leak events in.
- `set_default` rebuilds callsite interest, so a callsite that another thread cached as "never" is re-enabled.
- If `tracing::subscriber::DefaultGuard` does not resolve, use `tracing::dispatcher::DefaultGuard`, which is the same type.

**R1** `sse::feed::tests::read_error::a_retryable_source_read_logs_its_cause_once_per_drive` (new file `src/sse/feed/tests/read_error.rs`, created in C1, extended in C2):
```rust
/// Item 87 (red on 33fbd10e: the retried cause was dropped): a transient
/// failure is counted and retried with its cause logged, once per drive.
#[tokio::test]
async fn a_retryable_source_read_logs_its_cause_once_per_drive() {
    let log = crate::sse::test_log::ErrorLog::capture();
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(4, 8, 1 << 20, &budget);
    feed.subscribe_locked();
    src.fail_reads.store(true, Ordering::Relaxed);
    assert!(matches!(feed.drive_once().await, Some(DriveOutcome::SourceFailed)));
    assert_eq!(log.causes(), ["injected source failure"], "one cause per failed drive");
}
```
Trace on HEAD:
1. `drive_once` acquires the permit. `tail()` sees head 0 < frontier 4, so the tail is `Readable`, and `read_and_publish` is called.
2. `FakeSource::read_batch` hits `anyhow::bail!("injected source failure")`.
3. The `downcast_ref` returns `None`. `FEED_SOURCE_FAILED` is bumped and `SourceFailed` is returned.
4. The drive path records no event with an `error` field (`tail()` and `FakeSource` emit nothing), so the first assert passes and the second fails.

Exact red output:
```
thread 'sse::feed::tests::read_error::a_retryable_source_read_logs_its_cause_once_per_drive' panicked at src/sse/feed/tests/read_error.rs:<L>:5:
assertion `left == right` failed: one cause per failed drive
  left: []
 right: ["injected source failure"]
```
After C2, `read_failed`'s `Retryable(cause)` arm emits `error = %cause`, which is recorded as `"injected source failure"`. The test goes green.

**R2** `sse::session::tests::a_failed_catch_up_read_logs_its_cause` (`src/sse/session/tests.rs`):
```rust
/// Item 87 (red on 33fbd10e: the retried cause was dropped): a failed
/// catch-up read logs its cause before the bounded wait.
#[tokio::test(start_paused = true)]
async fn a_failed_catch_up_read_logs_its_cause() {
    let log = crate::sse::test_log::ErrorLog::capture();
    let failed = crate::sse::feed::SourceReadError::Retryable(anyhow::anyhow!("injected"));
    assert_eq!(catch_up::stalled(Err(failed)).await, Stall::Failed);
    assert_eq!(log.causes(), ["injected"], "the retried catch-up read names its cause");
}
```
The red run on HEAD uses HEAD's signature, `catch_up::stalled(Err(anyhow::anyhow!("injected")))`:
1. `downcast_ref` returns `None`.
2. `FEED_SOURCE_FAILED` is bumped, the 100 ms sleep auto-advances under `start_paused`, and `Stall::Failed` is returned, so the first assert passes.
3. No event is recorded.

Exact red output:
```
thread 'sse::session::tests::a_failed_catch_up_read_logs_its_cause' panicked at src/sse/session/tests.rs:<L>:5:
assertion `left == right` failed: the retried catch-up read names its cause
  left: []
 right: ["injected"]
```

### 3b. Pinning tests (the refactor)

**P1 (C1, green on HEAD)** `sse::feed::tests::read_error::a_fatal_source_read_retires_the_feed_once`. This is the reviewer's "first step". It is not red: HEAD already has this behaviour, reached through the downcast. It closes the unit gap described in 1c and pins the `retire` substitution.
```rust
/// Item 87 pin: a fatal read retires the lifecycle with its typed reason
/// and bumps the version exactly once, at the transition, so parked
/// sessions wake to disconnect; nothing is delivered.
#[tokio::test]
async fn a_fatal_source_read_retires_the_feed_once() {
    let budget = Arc::new(FeedMemoryBudget::new_for_test(1 << 20));
    let (feed, src) = feed_with(4, 8, 1 << 20, &budget);
    let (_cursor, woken, _generation, _swapped) = feed.subscribe_locked();
    *src.cut_reads.lock().unwrap() = Some(SourceCutoff::WrongOwner);
    let v0 = feed.version();
    assert!(matches!(
        feed.drive_once().await,
        Some(DriveOutcome::IncarnationClosed(SourceCutoff::WrongOwner))
    ));
    assert_eq!(feed.lifecycle_for_test(), "Gone");
    assert_eq!(feed.version(), v0 + 1, "the cutoff bumps exactly once");
    assert!(woken.has_changed().unwrap(), "parked sessions wake to disconnect");
    assert_eq!(feed.head(), 0, "a cutoff delivers nothing");
}
```
Green on HEAD. The C1 `FakeSource` knob returns `Err(anyhow::Error::new(crate::sse::source::FatalSpanCutoff(cut)))`. The downcast at `feed.rs:915` then sets Gone and applies one `version += 1` with a send. `drive_under_permit` does not bump for `IncarnationClosed` (drive.rs:132). After C2, the knob returns `Err(SourceReadError::Fatal(cut))` and the path goes `read_failed` -> `retire` -> `bump_version`: same lifecycle, same single bump, same send. The test body is not touched.

**P2 (C2)** `sse::source::tests::remote_span_refusals_split_into_cutoffs_and_retries` (`src/sse/source/tests.rs`). It replaces `a_fatal_span_cutoff_names_its_reason`, which pinned only the deleted `Display` string. It pins the refusal table that used to sit inline in `sealed_span_page`, arm by arm against HEAD `source.rs:516-550`:
```rust
/// Item 87: which remote span refusals end the feed here (typed cutoffs)
/// and which the same bound retries, with the cause it logs.
#[test]
fn remote_span_refusals_split_into_cutoffs_and_retries() {
    use crate::application::read_remote::RemoteSpanError as R;
    let verdict = |refusal| match super::spans::remote_span_verdict(3, refusal) {
        SourceReadError::Fatal(cut) => Ok(cut),
        SourceReadError::Retryable(cause) => Err(cause.to_string()),
    };
    assert_eq!(verdict(R::Unauthorized), Ok(SourceCutoff::FleetAuth));
    assert_eq!(verdict(R::TargetGone), Ok(SourceCutoff::IncarnationChanged));
    assert_eq!(verdict(R::TargetMismatch), Ok(SourceCutoff::TargetMismatch));
    let looped = R::RedirectLoop { first: "a".into(), second: "b".into() };
    assert_eq!(verdict(looped), Ok(SourceCutoff::RedirectLoop));
    let busy = R::Retryable { status: 503, code: None };
    assert_eq!(verdict(busy), Err("remote span 3: retryable 503 None".into()));
    assert_eq!(verdict(R::Transport("reset".into())), Err("remote span 3: transport reset".into()));
    let garbled = R::InvalidResponse("json".into());
    assert_eq!(verdict(garbled), Err("remote span 3: invalid response json".into()));
    let moved = R::WrongOwner { owner: "inst-c".into() };
    assert_eq!(verdict(moved), Err("remote span 3: unresolved owner inst-c".into()));
}
```
(It reaches `SourceReadError` and `SourceCutoff` through `use super::*`, since `source.rs` imports both from `feed`.)

**Existing tests that keep pinning the rest:**
- `sse::feed::tests::source_failure_is_typed_and_recoverable` (feed.rs tests :448): retry changes nothing, bumps nothing, recovers.
- `sse::feed::tests::no_progress_page_never_bumps_the_version`.
- `sse::session::tests::a_stalled_catch_up_read_owes_its_pass_one_verdict`. Only its constructors change, from `Err(anyhow::Error::new(cut))` and `Err(anyhow::anyhow!("injected"))` to `Err(SourceReadError::Fatal(SourceCutoff::TargetMismatch))` and `Err(SourceReadError::Retryable(anyhow::anyhow!("injected")))`. The verdicts and waits are the same.
- Retry and cutoff pins in `sse::feed::tests::retry::*`.
- DST, through the real sources:
  - `livefeed_engine_retired_*` (3): both live-tail Fatal producers, Single and Lineage, plus the typed read cutoff.
  - `livefeed_owner_movement_one_redirect_and_typed_cutoffs`: the remote RedirectLoop becomes `FEED_CUTOFF_REDIRECT_LOOP` through catch-up, and a WrongOwner phase.
  - `livefeed_parked_live_session_is_cut_off_by_engine_close`.
  - `livefeed_swap.rs` split legs.

**Compile-level proofs:**
- There is no `From<anyhow::Error>`, so every former `bail!` and every `?` on anyhow inside `read_batch` or `sealed_span_page` fails to compile until it names a variant. The 18 producer sites are all in the 1b table.
- Both consumer matches are exhaustive and have no wildcard.
- `grep -rn "FatalSpanCutoff\|downcast_ref::<crate::sse" src` is empty.
- `src/sse/feed.rs` no longer mentions `crate::sse::source`.

---

## 4. Edits, file by file, in commit order

Budgets for files over 1,000 lines (merge base `33fbd10e`; none of these may grow by one line):

| File | wc -l | Touched? |
|---|---|---|
| http.rs | 3225 | no |
| product.rs | 4205 | no |
| shard.rs | 3196 | no |
| billing.rs | 2201 | no |
| history.rs | 1713 | no |
| auth.rs | 1676 | no |
| registry.rs | 1492 | no |
| fleet.rs | 1142 | no |
| **sse/feed.rs** | **1170** | **yes: -6, to 1164** |

Other touched files that must stay at or below 1,000:

| File | Before | C1 | C2 |
|---|---|---|---|
| `src/sse/source.rs` | 981 | 981 | ~973 (itemised below) |
| `src/sse/feed/tests.rs` | 974 | 981 | 985 |
| `src/sse/feed/drive.rs` | 214 | 214 | ~238 |
| `src/sse/source/spans.rs` | 48 | 48 | ~75 |
| `src/sse/session/catch_up.rs` | 68 | 68 | ~72 |
| `src/sse/session/tests.rs` | 225 | 225 | ~235 |
| `src/sse/source/tests.rs` | 67 | 67 | ~90 |
| `src/sse/mod.rs` | 12 | 12 | 14 |
| new `src/sse/test_log.rs` | - | - | ~45 |
| new `src/sse/feed/tests/read_error.rs` | - | ~28 | ~40 |

### C1: "A fatal source read retires the feed once: the drive's cutoff path gets its pin" (tests only)

1. `src/sse/feed/tests.rs` (974 to 981):
   - After `pub(crate) fail_reads: AtomicBool,` add a doc line and the field: `/// Some(reason): every read is that fatal cutoff.` then `pub(crate) cut_reads: Mutex<Option<SourceCutoff>>,`.
   - In `new()`, add `cut_reads: Mutex::new(None),`.
   - In `read_batch`, after `let _in_flight = ReadInFlight(self);` and before the `fail_reads` check, add:
     ```rust
     if let Some(cut) = *self.cut_reads.lock().unwrap() {
         return Err(anyhow::Error::new(crate::sse::source::FatalSpanCutoff(cut)));
     }
     ```
     The guard lives only through a non-awaiting `return`, so `await_holding_lock` does not apply.
   - After `mod retry;` add `mod read_error;`.
2. New file `src/sse/feed/tests/read_error.rs`:
   ```rust
   //! A failed source read owes the feed one typed verdict (item 87): a
   //! cutoff retires it once, a transient failure changes nothing.
   #![cfg(test)]
   use super::feed_with;
   use crate::sse::feed::{DriveOutcome, FeedMemoryBudget, SourceCutoff};
   use std::sync::Arc;
   ```
   followed by P1. Imports are explicit because a `use super::*` would be a new unresolved-glob that needs an owners.json row.
- Ratchets: `feed/tests.rs` and `read_error.rs` are `#![cfg(test)]` and contain no exceptions. Nothing is touched.

### C2: "A source read fails as a typed SourceReadError, never an anyhow downcast; a retried read logs its cause"

1. **`src/sse/feed.rs`** (1170 to 1164):
   - After `SourceCutoff` (after :151), insert the `SourceReadError` enum from §2, with a leading blank line: +11.
   - :89 becomes `async fn read_batch(&self, from: u64, max_bytes: usize) -> Result<SourceBatch, SourceReadError>;`. This is exactly 100 columns, so rustfmt keeps it on one line. There is precedent: `src/auth.rs:634` is a 100-column signature. If rustfmt wraps it anyway (+4), the file is 1168, which is still within the ceiling.
   - `read_and_publish`:
     - Replace :912-928 (17 lines) with `Err(e) => return self.read_failed(e),` (-16).
     - Delete `clippy::let_underscore_must_use,` from its `#[expect]` (:896, -1). The only `let _ =` in the fn was :923, so leaving the lint in the list would be an unfulfilled expectation, which is denied.
     - Re-word the reason: delete the words ", handling the watch send". It still has exactly two `;`.
   - Ratcheted scopes:
     - `#[expect(clippy::unwrap_used)] impl LiveFeed` (:652-1055), identity unchanged. The new line adds no new fingerprint key. Its facts are path `Err` (1 before, 1 after), path `self` (2 before in that arm, 1 after), path `e` (1 before, 1 after) and a `read_failed` method call, which is not fingerprinted because it is neither `unwrap` nor `expect`. `unwrap_sites` drops by one (the removed `self.st.lock().unwrap()`). `scope_lines` drops by 16, `syntax_facts` drops, and `nested_items` stays the same. No growth.
     - `read_and_publish` fn-level `#[expect(too_many_lines, cast_possible_truncation, excessive_nesting, wildcard_enum_match_arm, expect_used)]`: the value changes (lint dropped, reason re-worded), so it is a new identity and the growth check does not apply. Its `expect_sites` stay the same (`.expect("eviction set pre-counted")` remains).
     - `too_many_lines` stays fulfilled: clippy counts 116 body lines inside the braces, or 118 including the brace lines that an async body keeps. After the edit that is 116 - 15 + 1 = 102, or 104. Both are over 100. Fallback: if clippy reports it unfulfilled, remove it from the list together with the "splitting it" clause.
     - `excessive_nesting` stays fulfilled: the eviction block, then `for`, then `if`, reaches level 5 counting the impl, and that code is untouched.
     - The enum and the trait are outside every exception scope.
2. **`src/sse/feed/drive.rs`** (214 to ~238):
   - Add `SourceReadError` to the `use super::{..}` list (no change in line count).
   - Append a new, exception-free `impl LiveFeed` block after :214. This follows the precedent of e6550142, "Feed construction sits outside the poisoned-state exception":
   ```rust
   /// A failed read's verdict sits outside the poisoned-state exception
   /// above: it touches feed state only through `retire`.
   impl LiveFeed {
       /// What a failed read owes the feed (item 87). A cutoff retires the
       /// lifecycle with its typed reason, bumping the version once at the
       /// transition like every other retirement, so parked sessions wake to
       /// disconnect; a transient failure changes nothing, and the driving
       /// session retries on its own bounded backoff.
       pub(super) fn read_failed(&self, error: SourceReadError) -> DriveOutcome {
           match error {
               SourceReadError::Fatal(reason) => {
                   self.retire(Lifecycle::Gone(reason));
                   DriveOutcome::IncarnationClosed(reason)
               }
               SourceReadError::Retryable(cause) => {
                   crate::sse::auth::sse_stats::FEED_SOURCE_FAILED.fetch_add(1, Ordering::Relaxed);
                   tracing::debug!(
                       error = %cause,
                       "livefeed source read failed; the driving session retries on its backoff"
                   );
                   DriveOutcome::SourceFailed
               }
           }
       }
   }
   ```
   - `retire` stays private. Same-module access is enough, so no visibility fact is added to the excepted impl.
   - Binding `cause` by value is what keeps `needless_pass_by_value` quiet. A `Copy` binding (`reason`) counts as a borrow for that lint, so a `Retryable(_)` arm would make `error` look unconsumed. This is why the log lands in the same commit as the type.
   - Ratcheted scopes: `#[expect(clippy::unwrap_used)] impl LiveFeed` (drive.rs:37-214) is not touched. The new block and the import line are outside it, so its identity and every metric stay the same.
3. **`src/sse/session/catch_up.rs`** (no exceptions):
   - Import: `use crate::sse::feed::{SourceBatch, SourceReadError};`.
   - `stalled(read: Result<SourceBatch, SourceReadError>) -> Stall`.
   - Keep the `Ok(page)` arm verbatim.
   - The fatal case becomes an arm, `Err(SourceReadError::Fatal(cut)) => { super::count_cutoff(cut); FEED_TOPOLOGY_DISCONNECTS += 1; tracing::info!(reason = ?cut, ...); return Stall::Cutoff; }`. It keeps the Round-11.2 comment and the counters exactly as they are.
   - `Err(SourceReadError::Retryable(cause)) => cause`, then the existing `FEED_SOURCE_FAILED` bump, then a new
     ```rust
     tracing::debug!(
         error = %cause,
         "livefeed catch-up read failed; retrying the same bound after a bounded wait"
     );
     ```
     and then the unchanged sleep and `Stall::Failed`.
   - `session.rs` does not change. `read` passes its type through at :428, and `read_batch` is called inside `tokio::select!`, whose tokens are opaque to the fact scanner.
4. **`src/sse/source/spans.rs`** (48 to ~75):
   - Delete `FatalSpanCutoff` and its `Display` and `Error` impls (:5-17).
   - Module doc: "The lineage's engine-free pieces: the linearization rule and which remote span refusals end a feed here."
   - Add `use crate::application::read_remote::RemoteSpanError;`. `SourceReadError` and `SourceCutoff` arrive through the existing `use super::*`, and the glob row in owners.json stays unchanged.
   - Append the function below. Its arms are HEAD :516-550 moved verbatim in meaning: the messages are byte-identical, with `{}`+`span.seg_id` becoming `{seg_id}`, and the `warn!` is unchanged.
   ```rust
   /// A remote owner's refusal of one sealed-span page, as the verdict the
   /// feed owes it (round-11.2): fleet auth after the forced refresh, a gone
   /// or mismatched target and a second redirect are not fixed by reading
   /// the same bound again, so they are typed cutoffs; everything else is
   /// the owner's transient state, retried on the session's backoff.
   pub(super) fn remote_span_verdict(seg_id: u32, refusal: RemoteSpanError) -> SourceReadError {
       match refusal {
           RemoteSpanError::Unauthorized => SourceReadError::Fatal(SourceCutoff::FleetAuth),
           RemoteSpanError::TargetGone => SourceReadError::Fatal(SourceCutoff::IncarnationChanged),
           RemoteSpanError::TargetMismatch => SourceReadError::Fatal(SourceCutoff::TargetMismatch),
           RemoteSpanError::RedirectLoop { first, second } => {
               tracing::warn!(span = seg_id, %first, %second, "sealed span redirect loop refused");
               SourceReadError::Fatal(SourceCutoff::RedirectLoop)
           }
           RemoteSpanError::Retryable { status, code } => SourceReadError::Retryable(
               anyhow::anyhow!("remote span {seg_id}: retryable {status} {code:?}"),
           ),
           RemoteSpanError::Transport(m) => {
               SourceReadError::Retryable(anyhow::anyhow!("remote span {seg_id}: transport {m}"))
           }
           RemoteSpanError::InvalidResponse(m) => SourceReadError::Retryable(
               anyhow::anyhow!("remote span {seg_id}: invalid response {m}"),
           ),
           RemoteSpanError::WrongOwner { owner } => SourceReadError::Retryable(
               anyhow::anyhow!("remote span {seg_id}: unresolved owner {owner}"),
           ),
       }
   }
   ```
   - Leave the exact layout to rustfmt. `match_same_arms` is clean because every body differs. There is no wildcard.
   - Ratcheted scope `locate_in_spans` (`#[expect(clippy::expect_used)]`, :24-48): its text is unchanged and only shifts position. The identity `(spans.rs, crate::locate_in_spans, function, value)` and all its metrics stay the same.
5. **`src/sse/source.rs`** (981 to ~973):
   - Imports :11-14: add `SourceReadError` to the `super::feed::{..}` list. rustfmt keeps it at 2 lines (the row is 99 columns). 0 lines.
   - **Narrow `impl FeedSourceRead for SingleSource`'s impl-wide `unwrap_used`** (:42-45, removed) into fn-level expects on `frontier` (:108) and `closed` (:112), the only two `.unwrap()` in the impl. +4 lines. Reasons:
     - `"SingleSource::frontier; a poisoned stream state may hold a half-advanced durable frontier; recovering it could serve a length never made durable"`
     - `"SingleSource::closed; a poisoned stream state may hold a half-applied durable close; recovering it could report a close never made durable"`
     - This drops the clause rank 14 appended to absorb `read_batch`/`cut_off` growth ("the live tail pins one engine incarnation ... a stale read could serve a retired engine"), which never described an unwrap.
   - `SingleSource::read_batch`:
     - The signature becomes `Result<SourceBatch, SourceReadError>`. At 101 columns it wraps: +4.
     - :52 becomes `return Err(SourceReadError::Fatal(cut));`.
     - :66 and :79 become `.map_err(|e| SourceReadError::Retryable(anyhow::anyhow!(e)))?` (77 and 73 columns). 0 lines.
   - `sealed_span_page`:
     - Doc :409-411: "Fatal outcomes ride `FatalSpanCutoff`; retryables stay anyhow errors" becomes "A refusal is typed by `remote_span_verdict`; every local failure retries."
     - `#[expect]` (:412-418): delete `clippy::too_many_lines,` (-1) and the words "a split, " from the alternatives.
     - Return type `Result<crate::application::read::ReadPage, SourceReadError>`.
     - Delete :428 `use super::feed::SourceCutoff;` (-1). It is unused now and would be a warning.
     - :440 becomes `.map_err(|e| SourceReadError::Retryable(anyhow::anyhow!("stream handle: {e}")))?;`, which rustfmt turns into a block closure (+2).
     - :459 `.map_err(|e| SourceReadError::Retryable(anyhow::anyhow!(e)));` (0).
     - :465 `bail!` becomes `return Err(SourceReadError::Retryable(anyhow::anyhow!("sealed span engine unavailable: {error:?}")));`, wrapped (+2).
     - :481/:484 wrap the existing `anyhow::anyhow!(..)` in `SourceReadError::Retryable(..)`. The line count is the same, and :480 `.ok_or_else(|| {` is unchanged.
     - :516-550 (35 lines) become `Err(refusal) => Err(remote_span_verdict(span.seg_id, refusal)),` (-34).
   - **Narrow `impl FeedSourceRead for LineageSource`'s impl-wide `#[expect(too_many_lines, unwrap_used)]`** (:563-567, removed) into four fn-level expects (+11):
     - `read_batch`: `too_many_lines`, reason `"LineageSource::read_batch; one batch walks the span chain until the budget or the frontier stops it; splitting the walk would separate it from its budget"`.
     - `frontier` (:696), `closed` (:712) and `logicalize` (:753), the only unwraps: `unwrap_used`, with reasons in the same form as SingleSource's (`logicalize`: "...; recovering it could accept a cursor past what was made durable").
   - `LineageSource::read_batch`:
     - The signature becomes typed and wraps (+4).
     - :594 becomes `return Err(SourceReadError::Fatal(cut));`.
     - :611 becomes `.map_err(|e| SourceReadError::Retryable(anyhow::anyhow!(e)))?` (81 columns).
     - :626 `.await?` is unchanged.
     - :657 becomes `return Err(SourceReadError::Retryable(anyhow::anyhow!("lineage span ended below its cap")));`, wrapped (+2).
   - Delete the :976 re-export (-1). :977 becomes `use spans::{locate_in_spans, remote_span_verdict};`.
   - Total: +4 +4 -1 -1 +2 +2 -34 +11 +4 +2 -1 = **-8, to 973**. Without narrowing it would be 958.
   - Ratcheted scopes in source.rs:
     - (E) SingleSource impl-wide `unwrap_used`: HEAD's identity disappears. The two new fn-level identities are new decisions, so the growth check does not apply. `read_batch` ends up under no unwrap exception.
     - (F) LineageSource impl-wide `too_many_lines, unwrap_used`: same treatment, four new fn-level identities.
       - Clippy's count for `read_batch` is 109 inner lines at HEAD and about 111 after, so it stays over 100 and the fn-level `too_many_lines` is fulfilled. Evidence: under `#[async_trait]` the method keeps its attributes and its original brace span, and HEAD's impl-level expect is fulfilled only by this method, which is the sole one over 100 lines.
       - Fallback, only if clippy misplaces it: keep a single impl-level `#[expect(clippy::too_many_lines, clippy::unwrap_used, reason = ...)]` with a re-decided reason. That is rank 14's remedy, and it costs 0 lines.
     - (G) `sealed_span_page` `#[expect(too_many_arguments, excessive_nesting, unwrap_used)]`: the value changed, so it is a new identity.
       - `too_many_lines` must go. The clippy count is 121 inner lines (123 with the async braces) at HEAD. It becomes 121 - 1 + 2 + 2 - 34 = 90 (or 92). That is under 100, so leaving it would be an unfulfilled expectation.
       - `excessive_nesting` stays fulfilled: `let owner = {`, then `_ => {`, then `.ok_or_else(|| {` reaches level 5 counting the impl, and that code is untouched. The new `.map_err(|e| {..})` block at :440 is another level-5 site under the same expect.
       - `too_many_arguments` (7 inputs) and `unwrap_used` (the `owner_hint` read and write) are unchanged.
     - `LineageSource::build` (`excessive_nesting`), `LineageSource::tail` (`expect_used`), `refresh_transition` and `LineageSpan.identity` (`dead_code`) are not touched: same text, same fingerprints. The import change resolves no path inside them differently.
6. **`src/sse/feed/tests.rs`** (981 to 985):
   - The `FakeSource::read_batch` signature becomes typed and wraps (+4).
   - The knob returns `Err(SourceReadError::Fatal(cut))`.
   - `bail!("injected source failure")` becomes `return Err(SourceReadError::Retryable(anyhow::anyhow!("injected source failure")));` (95 columns, 0 lines).
   - `fixture.rs:86`'s `.unwrap()` needs only `SourceReadError: Debug`, which the derive provides.
7. **`src/sse/feed/tests/read_error.rs`**: add R1 and `use std::sync::atomic::Ordering;`.
8. **`src/sse/test_log.rs`** (new) and **`src/sse/mod.rs`** (+2: `#[cfg(test)]` / `pub(crate) mod test_log;`). Put the declaration last, after `pub(crate) mod wire;`. That way both of the planner's proofs hold: the trailing-`#[cfg(test)]`-suffix byte proof, and the token proof, which erases the item and its visibility (`production_changes.py` `erased()`). `mod.rs` is then production-unchanged and needs no owner row.
9. **`src/sse/session/tests.rs`**: adapt the item-86 test's constructors, adding a fn-local `use crate::sse::feed::{SourceCutoff, SourceReadError};` and dropping the `let cut = FatalSpanCutoff(..)` line. Add R2.
10. **`src/sse/source/tests.rs`**: delete `a_fatal_span_cutoff_names_its_reason` (:44-50 plus the blank line) and add P2.
11. **`src/product/internal.rs:6-7`** (doc only; not a critical path): "made it `FatalSpanCutoff(IncarnationChanged)`" becomes "made it a fatal `IncarnationChanged` cutoff". This keeps the grep for the deleted type empty.

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`, timeout 90 s)

Changed critical sources and their owners (`scripts/quality/mutation_owners.py`, no row changes):

| Source | Owner | Filters |
|---|---|---|
| `src/sse/feed.rs` | `sse_feed` | `sse::` |
| `src/sse/feed/drive.rs` | `sse_feed_drive` | `sse::` |
| `src/sse/session/catch_up.rs` | `sse_session_catch_up` | `sse::session::tests:: dst_tests::sse_delivery::` |
| `src/sse/source.rs` | `sse_source` | `sse:: livefeed_engine_retired` |
| `src/sse/source/spans.rs` | `sse_source_spans` | `sse::` |
| `src/sse/source/tests.rs` | `sse_source_tests` | `sse::`; behind `#[cfg(test)] mod`, so cargo-mutants does not walk it and it yields 0 mutants |

Not mutation sources, because the planner rates them "production unchanged": `feed/tests.rs`, `session/tests.rs` and the new `test_log.rs` and `feed/tests/read_error.rs` (all `#![cfg(test)]`; new files compare `''` with test-only `''`), and `sse/mod.rs` (the only change is an item carrying its own `#[cfg(test)]`). `product/internal.rs` is outside the critical prefixes. No new owner rows are needed.

Functions whose bodies change, and the mutants in the diff:

| Function | FnValue | Operators on inserted lines or next to deletions |
|---|---|---|
| `LiveFeed::read_and_publish` (feed.rs) | `Default::default()`: **unviable** (`DriveOutcome` has no `Default`) | Inserted `Err(e) => return self.read_failed(e),`: none. Deletion neighbours `Ok(x) => x,` and `};`: none. The trait signature and enum have no body. |
| `LiveFeed::read_failed` (drive.rs, new) | unviable (`DriveOutcome`) | none; no wildcard arm, so no arm deletion |
| `catch_up::stalled` | unviable (`Stall` has no `Default`) | none (`fetch_add(1, ..)` has no binary operator) |
| `spans::remote_span_verdict` (new) | unviable (`SourceReadError` has no `Default`) | none; no wildcard arm |
| `SingleSource::read_batch` | `Ok(Default::default())`: unviable (`SourceBatch` has no `Default`) | :52, :66, :79 and their neighbours (:51 `if let Some(cut) = live_tail_cutoff(..)`, `.await`, `} else {`, `};`): none |
| `LineageSource::sealed_span_page` | `Ok(Default::default())`: unviable (`ReadPage` has no `Default`) | neighbours of :427/:428, :440, :459, :465, :481/:484, :516-550 are the signature, `if owned_here(..) {`, `.await`, `Err(error) => {`, `.ok_or_else(|| {`, `})?` and the closing `}` of the `Ok(page)` arm: none. :479's `!=` is not adjacent, because :480 is unchanged. |
| `LineageSource::read_batch` | unviable (`SourceBatch`) | :594/:611 neighbours: none. **:656 `if part.completed && !drained {`** sits right before the rewritten :657. It yields **2 viable mutants**: `&&` to `\|\|`, and deleting `!`. |

Killer for both :656 mutants: `dst::…::livefeed_engine_retired_under_the_same_owner_cuts_a_parked_lineage_session`. It is inside `sse_source`'s `livefeed_engine_retired` filter. Trace:
1. The test builds a lineage: span 0 is sealed with cap 1 and holds `{"h":0}`, and the live child holds `{"h":1}`. It then connects with `?cursor=beginning`, so `join_head` is 2 and catch-up reads from 0.
2. Span 0's local page is bounded `0..1`, so `completed = consumed_next(1) >= read_end(1)` is true (`application/read.rs:271`), cursor becomes 1, and `drained` is true.
3. The original condition, `true && !true`, is false, so the walk hops to the child.
4. Under either mutant (`true || false`, or `true && true`) every read of the lineage returns `Retryable("lineage span ended below its cap")`, and catch-up gives `Stall::Failed` every 100 ms.
5. `hub_sse_collect(&mut sub, 15, ..)` never sees `"h":1`, so `assert!(a0.contains("\"h\":1") && !eof0, "parked at the lineage's live tail…")` fails after about 15 s. That is well inside the 90 s timeout, so the mutant is **caught, not timed out**.

Attribute moves (the narrowing, and lint-list removals) sit outside every mutant span. FnValue spans run from the first to the last body statement, so moving attributes creates no mutants. Expected leg result: 2 caught, the rest unviable, 0 missed, 0 timeout.

---

## 6. Ledgers

- `docs/refactor/test-inventory.json`: **no change**. No `src/dst` test is edited. `scripts/test-inventory.py --check` stays green.
- `docs/refactor/review-mechanisms.json`: no change, because no pinned DST test is touched.
- `docs/quality/owners.json`: **no change**.
  - The new test files use explicit imports, so they add no unresolved-glob. Writing `use super::*` in `read_error.rs` or `test_log.rs` would require a row, so don't.
  - No new statics or macro-dsl: `tracing::` and `anyhow::` macros are exempt, and `format!`, `matches!` and `assert*!` are expression macros.
  - `spans.rs`'s existing glob row still applies.
- `docs/quality/source-allowances.json`: **nothing to prune**. The only sse `macro-dsl` row in `feed.rs` is `crate::LiveFeed::read_and_publish tokio::select 1`, and it stays because the `select!` remains. The reasoned `#[expect]`s need no rows ("reviewed in-source").
- `docs/refactor/architecture-policy.json`: no change.
  - `feed.rs`, `drive.rs` and `source.rs` gain no `crate::http`/`crate::product`/`axum` paths.
  - `\bResponse\b` does not match `InvalidResponse`, and `spans.rs` is not an sse core file in any case.
- `docs/refactor/WIRE-MATRIX.md`, `docs/LIVE-FEED.md`: no change (§2).
- Scenario map and dispositions: no rename. The deleted `a_fatal_span_cutoff_names_its_reason` is not referenced, and `source_failure_is_typed_and_recoverable` (cited by HIS-025) is untouched.
- `src/dst/tests/README.md`: no new DST module.
- `scripts/quality/mutation_owners.py`: no change (§5).

---

## 7. Controls (implementer; Python ≥ 3.11 for the quality scripts)

1. Red demonstration on `33fbd10e`, before C2. Use a scratch tree containing `test_log.rs` plus its mod line, `read_error.rs` with P1 and R1 (C1 knob), and R2 written against HEAD's `stalled(Err(anyhow::anyhow!("injected")))`:
   `cargo test --locked --lib -- sse::feed::tests::read_error sse::session::tests::a_failed_catch_up_read_logs_its_cause`
   Expected: `a_fatal_source_read_retires_the_feed_once ... ok`, and the two log tests FAILED with the exact outputs in §3a. `test result: FAILED. 1 passed; 2 failed`.
2. After C1: `cargo test --locked --lib sse::feed::tests::read_error` gives `1 passed`. `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean.
3. After C2:
   - `cargo fmt --all -- --check`: no output.
   - `cargo clippy --locked --workspace --all-targets -- -D warnings`: clean. In particular there is no `unfulfilled_lint_expectations` on `read_and_publish` (still `too_many_lines`), `sealed_span_page` (without `too_many_lines`) or the new fn-level expects, and no `needless_pass_by_value` on `read_failed`.
   - `cargo test --locked --lib sse::`: all pass. The count is HEAD's + 3 (P1, R1, R2 and P2 added; `a_fatal_span_cutoff_names_its_reason` removed).
   - `cargo test --locked --lib livefeed_`: every livefeed DST passes, including `livefeed_engine_retired_*` (3) and `livefeed_owner_movement_one_redirect_and_typed_cutoffs`.
   - `grep -rn "FatalSpanCutoff\|downcast_ref::<crate::sse" src`: no output. `grep -n "sse::source" src/sse/feed.rs`: no output.
   - `wc -l src/sse/feed.rs src/sse/source.rs src/sse/feed/tests.rs`: 1164 (≤1170), about 973 (≤1000), 985 (≤1000).
   - `python3 scripts/quality/verification_plan.py --out target/quality-plan`:
     - `mutation_source_files` = `["src/sse/feed.rs","src/sse/feed/drive.rs","src/sse/session/catch_up.rs","src/sse/source.rs","src/sse/source/spans.rs","src/sse/source/tests.rs"]`
     - `unregistered_mutation_source_files` = `[]`
     - `production_unchanged_files` contains `src/sse/feed/tests.rs`, `src/sse/feed/tests/read_error.rs`, `src/sse/mod.rs`, `src/sse/session/tests.rs` and `src/sse/test_log.rs`.
   - `bash scripts/quality/mutations.sh`: `sse_source` has 2 caught (source.rs `&&`/`!` at the `part.completed && !drained` line) and every other owner is unviable-only. 0 missed, 0 timeout.
   - `bash scripts/quality.sh`: `QUALITY_OK`. The source ratchet reports no `accepted exception grew`, no `file growth` and no obsolete allowances. `architecture-gate --check`, `test-inventory --check` and `scenario-map-report --check` pass.
4. Push C1 and C2 together, as one push, so the CI push comparison is `33fbd10e` to HEAD, the same as the local merge base.

---

## 8. Out of scope

- `drive.rs:176-179`, `tail()`'s `next_source()` `Err(_)`, also counts `FEED_SOURCE_FAILED` without a cause, and `next_source` still returns `anyhow`. Logging it there would grow drive.rs's impl-wide poisoned-state exception. A typed transition error is a separate item.
- Found while tracing P1: a Gone feed whose source still has records above `head` re-reads on every later drive, because `tail()` checks `Readable` before the lifecycle. After a fatal read cutoff, each subscriber's own drive re-reads the source and re-bumps the version once: N sessions give N reads and N bumps. This is bounded, and unchanged by this plan.
- No reclassification. Remote `WrongOwner`, "ownership indeterminate" and "lineage span ended below its cap" stay Retryable, exactly as on HEAD. Any change there belongs with item 22's pending untyped-409 decision.
- Cause-labelled `FEED_SOURCE_FAILED` counters would change the `/metrics` and `/v1/debug` contract. Not proposed.
- Rank 14's `EngineRetired` has landed (§1d). Item 86's no-progress verdict has landed (`2fb92fb9`, `557e8ba6`).

---

## 9. Decisions for Søren

None. There is no status, body, header, cursor, metric-name or debug-JSON change (§2). The new output is debug-level log events only. The two departures from the reviewer's Change are:
- The enum lives in `feed.rs`, not `spans.rs`, beside the trait and `SourceCutoff`, and this removes feed's dependency on `source`.
- The first step is a green pin, P1, not a red test, because HEAD already has that behaviour.

Both are design choices within the item, not edge or policy changes.

---

## Skeptic corrections (C1..Cn)

Everything below was checked read-only against `33fbd10e`. I did not run cargo or any scripts. `git rev-parse origin/slate` and `git merge-base HEAD origin/slate` both give `33fbd10e`, so the plan's note about the task premise is right.

### What I verified and found correct

**File sizes.** `wc -l` matches the plan: feed.rs 1170, source.rs 981, feed/tests.rs 974, drive.rs 214, spans.rs 48, catch_up.rs 68, session/tests.rs 225, source/tests.rs 67, mod.rs 12. The other ceilinged files are untouched.

**Line arithmetic.**
- The feed.rs trait signature is exactly 100 columns (`printf | wc -c`).
- source.rs comes to -8, giving 973.
- The remote arms span 35 lines (:516-550) and collapse to one.

**too_many_lines counts** (non-blank, non-comment lines, brace lines included because each fn is async):

| Function | HEAD | After | Consequence |
|---|---|---|---|
| `read_and_publish` (903-1054) | 118 | 104 | Expectation stays fulfilled |
| `sealed_span_page` (427-551) | 122 | 91 | Expectation must be removed; the plan removes it |
| `LineageSource::read_batch` (570-689) | 111 | 113 | Expectation stays fulfilled |

**Ratchets** (checked against `scripts/quality/source_rules.py:109-201`):
- `ordinary-call` fingerprints only `call-site` facts, not `method-call-site`. So `self.read_failed(e)` adds no key under feed.rs's impl-wide `unwrap_used`. The removed arm's call-sites (`Lifecycle::Gone(..)`, `drop(st)`) only decrease.
- The fn-level expects on `read_and_publish` and `sealed_span_page` change value. That makes them new identities, so the growth check does not apply to them.
- The narrowing of SingleSource and LineageSource creates new fn-level identities. Reasoned exceptions skip the inventory check (`source_rules.py:262`).
- The new reason strings match `"[^";]+;[^";]+;[^";]+"`.
- The only unwraps in those impls are `source.rs:109, 113, 696, 712, 753`. `unreachable!` is not `clippy::panic`.
- `let _ =` occurs in `read_and_publish` only at :923, so removing `let_underscore_must_use` is right.

**Production-unchanged classification.**
- New `#![cfg(test)]` files compare `''` with `''`: `verification_plan.py:333-338` and `production_changes.py:61`.
- The trailing `#[cfg(test)] pub(crate) mod test_log;` in `sse/mod.rs` passes `trailing_test_prefix`.
- `source/tests.rs` has no `#![cfg(test)]`, so it is a production-changed mutation source. Its owner row `sse_source_tests` exists. cargo-mutants does not walk a `#[cfg(test)]` module, so the owner lists 0 mutants and `mutation_driver.py:143-145` skips it.

**Mutation analysis.**
- These types have no `Default`, so every changed-body FnValue is unviable: `DriveOutcome` (feed.rs:1129), `Stall`, `SourceBatch`, `ReadPage` (application/read.rs:108) and `SourceReadError`.
- Once the error type is not anyhow, cargo-mutants generates no `Err(anyhow!)` mutant.
- The `!=` at :479 and the guard or arm mutants on the `match hinted` wildcard at :472 are not next to the changed lines :481/:484.
- :656 `part.completed && !drained` gives the only 2 viable mutants.
- `livefeed_engine_retired_under_the_same_owner_cuts_a_parked_lineage_session` (livefeed_engine_retired.rs:181) kills them. Either mutant makes every read of the sealed span 0 fail, so the 15 s `hub_sse_collect` misses `"h":1`, and the test fails inside the 90 s timeout. `completed = consumed_next >= read_end` is confirmed at application/read.rs:271.

**Contract.**
- `docs/`, WIRE-MATRIX and LIVE-FEED contain no `anyhow`, downcast, `source_failed` or `sse::source` log-target reference.
- `architecture-gate.py:113` `\b(?:HeaderMap|Response)\b` does not match `InvalidResponse`.
- No source-allowance row goes stale. `read_and_publish tokio::select` stays.

**Tests.**
- R1 and R2 are bounded: `fail_reads` returns immediately, and `start_paused` auto-advances the 100 ms sleep.
- R1's red output traces as stated. No event with an `error` field fires on the drive path at HEAD, and `DisplayValue` records the Display text without quotes.
- P1 is green on HEAD and after C2. It gets one bump through the downcast at HEAD and through `retire` → `bump_version` after, and `drive_under_permit` does not bump `IncarnationClosed` (drive.rs:132).
- `tracing-subscriber` 0.3 default features include `registry`. No dependency enables a `tracing` `max_level_*` feature, and nothing in `src` sets a subscriber that could compete.

### Corrections

**C1. Missing pinning tests and a missing control (§3b, §7.3).** Two DST legs drive `FakeSource.fail_reads` through real sessions, so they exercise both retyped consumers:
- `src/dst/tests/sse_delivery.rs:688-691` `dst_tests::sse_delivery::a_failed_live_read_is_retried_without_another_append` covers the live drive, which becomes `read_failed`'s `Retryable` arm.
- `sse_delivery.rs:766-769` `dst_tests::sse_delivery::a_failed_catch_up_read_is_read_again_after_a_bounded_wait` covers catch-up, which becomes `stalled`'s `Retryable` arm.
- The retyped producer they hit is `bail!` at `src/sse/feed/tests.rs:117-119`, which becomes `Err(SourceReadError::Retryable(..))`.
- Both legs sit inside the `sse_session_catch_up` owner filter `dst_tests::sse_delivery::` (`mutation_owners.py:145`).
- §7.3 runs only `cargo test --locked --lib livefeed_`, which does not select them.

Fix: list both in §3b, and add `cargo test --locked --lib dst_tests::sse_delivery::` to §7.3. Expect all of them to pass, including the `no-progress`/`catch-up-empty` siblings. Their bodies are unchanged, so `test-inventory.json` is unaffected.

**C2. The use-site table misses two `FakeSource` users (§1b).** Besides the feed tests, `FakeSource` is used by:
- `src/dst/tests/sse_delivery.rs:636-718`: `FakeSource::new(0, 8)` and `(3, 8)`, plus the `fail_reads` and `empty_pages` fn pointers.
- `src/sse/registry.rs:217-226`: `FakeSource::new(0, 8)`.

Both build through `FakeSource::new` (feed/tests.rs:77-98), and `git grep "FakeSource {"` finds no struct literal. So C1's new `cut_reads` field compiles once it is initialised in `new()`, as the plan does, and no `src/dst` file is edited. State this in §1b so the claim "no src/dst edit, test-inventory unchanged" rests on the grep.

**C3. Citation fixes; no behaviour impact.**
- §3b "`source_failure_is_typed_and_recoverable` (feed.rs tests :448)" is `src/sse/feed/tests.rs:448`.
- §4.5 "`frontier` (:696), `closed` (:712) and `logicalize` (:753)": those are the unwrap lines. The fns begin at `source.rs:691`, `:704` and `:745`, and each fn-level `#[expect]` goes directly above the `fn` line, not the unwrap.
- §4.1 "clippy counts 116 body lines … 118 incl. braces": 118 is the async-fn count, because `too_many_lines` strips braces only when the body is a `Block`. Both numbers stay above 100 after the edit (104).
- §4.5(G) "121 (123)" is 122 by the same method, and 91 after. Still under 100, so the removal stands.

**C4. Optional, logging fidelity.** `error = %cause` prints only anyhow's outermost Display. Most `Retryable` producers wrap typed errors (`anyhow::anyhow!(e)` at source.rs:66, 79, 459 and 611, and `ResolveError` at :465), and the cause chain of those errors is lost with `{}`. Since the item's whole point is "the retried cause", consider `error = %format_args!("{cause:#}")` at both sites (drive.rs `read_failed` and catch_up.rs). R1 and R2 are unchanged, because their injected errors have no chain. `ErrorField` records a `DisplayValue<Arguments>` as `"injected source failure"` / `"injected"`. Not blocking.

**C5. Optional doc accuracy.** Two docs still say "span":
- `src/sse/session/catch_up.rs:23` (`Stall::Cutoff`: "A fatal span cutoff").
- `SourceReadError::Fatal`'s producers include the two live-tail cutoffs (WrongOwner/EngineRetired, source.rs:52/594).

Say "a fatal read cutoff" in catch_up.rs:23 so the prose matches the new type. That line is inside the file being edited, so it costs no extra file.

### Verdict

**ready-with-corrections.** No unbuildable control, and no ledger is missed. Every step is buildable as written. C1 is the one substantive gap: the verification in §7.3 does not run the two DST legs that exercise both new `Retryable` arms end to end. C2 and C3 are completeness and citation fixes, and C4 and C5 are optional.
