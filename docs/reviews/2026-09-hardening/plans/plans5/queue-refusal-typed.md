# Review chain 46 / 47 / 48 → 23 — typed committer refusals

Tree: `slate` @ `47799eb3` (2026-09-23). `0afa2597..47799eb3` touched only
`scripts/`, `docs/refactor/architecture-policy.json` and one evidence doc, so
every `src/` line number below is also exact at `0afa2597`. Read-only analysis:
nothing was built, run or edited. The reviewer's line numbers were re-found by
content; stale ones are corrected inline.

Commit order: **C1 = item 46, C2 = item 47, C3 = item 48, C4 = item 23**. Each
commit passes every gate on its own (see the §4 budget table). No verbatim-move
commit is needed: the only ceilinged file touched in production is
`src/shard.rs`, and it **shrinks** (3,232 → 3,204 in C3, → 3,203 in C4).
`src/history.rs` gets one visibility keyword (C2, ±0 lines).

---

## 1. Problem (verified on the current tree)

### Item 46 — `EnqueueError::Closed` is answered as a full queue

`src/shard.rs:1837-1844` (`ShardEngine::try_command`) returns two different facts:

```rust
        if self.is_closed() {
            return Err(EnqueueError::Closed);
        }
        self.tx.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => EnqueueError::Full,
            mpsc::error::TrySendError::Closed(_) => EnqueueError::Closed,
        })?;
```

Every caller throws the variant away:

- `src/application/append/submit.rs:79-85` (raw + product append, every close-with-body):
  ```rust
      if engine.try_enqueue(req).is_err() {
          return fail(
              FailureClass::Capacity,
              AppendCode::Overloaded,
              "append queue full",
          );
      }
  ```
  → raw `429 overloaded` / product `429 rate_limited`, no `Retry-After`. The same
  request an instant earlier (`resolve` → `ResolveError::Opening`) or a moment
  later (committer `AppendErr::Moved` → `AppendFailure::from_commit`,
  `contract.rs:277-282`) is `503 shard_moving` + `Retry-After: 1`.
- `src/application/creation/initialization.rs:147-153` (raw PUT with an initial body):
  ```rust
          if engine.try_enqueue(req).is_err() {
              return Err(CreationError::new(
                  CreationFailure::Overloaded,
                  "overloaded",
                  "queue full",
              ));
          }
  ```
- `src/application/lifecycle.rs:881-887` (seal fence): `.map_err(|_| SealError::Resumable("append queue full; fence not placed".into()))?` — wrong text for `Closed`.
- `src/application/topology.rs:112-118`: `if let Err(_req) = engine.try_close(req) { tracing::error!(seg_id, ?route, "seal close never enqueued (committer queue full or closed)"); ... }` — the binding is an `EnqueueError`, not the request; the log guesses.

Verified: real. Impact is status/Retry-After shape only (the SDK retries both
429 and 503, `sdk/src/index.ts:523-527`), but the answer contradicts every
neighbouring closed-engine exit.

### Item 47 — a closed database is reported as an internal error

`slatedb::ErrorKind::Closed(CloseReason::{Clean,Fenced,Panic})` means the engine
lost its store (the acker turns the same fact into `begin_close()`,
`shard.rs:3148-3152`). Three committer sites flatten it into `AppendErr::Internal`:

- `src/shard/transaction/finalize.rs:196` (`CommitTransaction::write`): `Err(error) => self.reject(&error.to_string()),` → `mod.rs:186-188` → `AppendErr::Internal`.
- `src/shard/transaction/mod.rs:152-157` (`stage`, handle load): `Err(error) => { Self::reject_op(op, AppendErr::Internal(error.to_string())); return; }`.
- `src/shard/transaction/prepare.rs:53-84` (`billing_rows` → `Result<_, String>`, line 82 `return Err(error.to_string());`) and its consumer `mod.rs:58-66` (`Self::reject_op(op, AppendErr::Internal(error.clone()))`).

Every other closed-engine exit answers `AppendErr::Moved`: `mod.rs:52-56`
(`run`, engine already closed), `publish.rs:26-31` (retired handoff),
`finalize.rs:58` (retired attachment), `shard.rs:1917-1919` (stranded groups).
`Moved` renders `503 shard_moving` + `Retry-After: 1`; `Internal` renders
`500 internal`. Both are "not definitively rejected" (`contract.rs:285-297`) and
both are `AmbiguousOrTransient` for seals (`lifecycle/claims.rs:222-235`), so only
the wire answer changes. Verified: real (partially — a write fails with
`Closed` only when the store closed/fenced before the acker retired the engine,
a genuine but narrow race).

Note on the reviewer's Change: `billing_rows`' error is an `anyhow::Error`
(`load_billing_meta` returns `anyhow::Result`, `shard.rs:2868-2871`), so
`From<slatedb::Error>` alone cannot serve "all three sites". The buildable form
reads the kind through the `anyhow` chain — exactly the predicate the absorber
already owns, `history.rs:742-748` `absorb_error_is_fence`
(pinned by `r08_fence_disposition_uses_error_kind_through_context`,
`history.rs:1183-1195`).

### Item 48 — the committer drain is hand-copied and never closes its receiver

`src/shard.rs:2531-2575` (`committer_loop`):

```rust
                _ = self.closed() => {
                    // Fail everything still queued — ...
                    while let Ok(op) = rx.try_recv() {
                        match op {
                            CommitOp::Append(r) => {
                                let _ = r.resp.send(Err(AppendErr::Moved));
                            }
                    CommitOp::Close(CloseReq { resp, .. }) | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                        let _ = resp.send(Err(AppendErr::Moved));
                    }
                            CommitOp::Queue { resp, .. } => {
                                let _ = resp.send(Err("shard fenced/moved; retry".into()));
                            }
                            ... (seven reply-less variants) => {}
                        }
                    }
                    return;
                }
```
and the second drain, `shard.rs:2566-2574`:
```rust
            if self.is_closed() {
                transaction::CommitTransaction::reject_op(first, AppendErr::Moved);
                while let Ok(op) = rx.try_recv() {
                    transaction::CommitTransaction::reject_op(op, AppendErr::Moved);
                }
                return;
            }
```
`reject_op` (`src/shard/transaction/mod.rs:110-129`) ends in `_ => {}` — the
reply-less variant list lives only in the inline copy.

Neither drain calls `rx.close()`. Two consequences, both verified:

1. **Abort/panic.** The committer is a required supervised task
   (`shard.rs:1685-1687`, `src/shard/lifecycle.rs:21-37`); the supervisor aborts any worker
   still running `WORKER_GRACE` (5 s) after shutdown starts
   (`src/tasks/shutdown.rs:176-203`: `t.handle.abort()`), and a panic unwinds the
   same way. Dropping the future drops the plain `mpsc::Receiver`, whose own
   `Drop` drops every queued `CommitOp` — every queued reply sender is dropped
   unanswered. An append then waits out `APPEND_TIMEOUT` and answers
   `408 append_timeout … outcome unknown` (`submit.rs:86-93`) although it was
   never staged; a queue op answers `"committer dropped request"` (`shard.rs:2395-2396`).
2. **Late enqueue.** `try_command` checks `is_closed()` and then `try_send`s
   non-atomically; `submit_queue` (`shard.rs:2381-2385`) `send().await`s with no
   check at all. An op that lands between the drain's last `try_recv` and the
   receiver drop is dropped the same way.

### Item 23 — queue ops reply `Result<_, String>`; callers prefix-match

The reply type: `shard.rs:919-923` (`CommitOp::Queue { resp: oneshot::Sender<Result<crate::queue::QueueOut, String>> }`),
`commit_plan.rs:38` (`queue_acks: Vec<ReplyEffect<crate::queue::QueueOut, String>>`),
`transaction/queue/mod.rs:12` (`type QueueReply = oneshot::Sender<Result<QueueOut, String>>;`),
`shard.rs:2376-2397` (`submit_queue(..) -> Result<crate::queue::QueueOut, String>`).

String producers (all verified): `queue/mod.rs:36` (load error), `:100`,
`:109` (`"consumer_fence_unverified: {e}"`), `:126` (`"consumer_generation_fenced: generation {op_gen} was deleted"`),
`:137` (`"consumer_not_found: …"`); `config.rs:16`, `:19` (`"consumer_config_corrupt: …"`),
`:64` (`"consumer generation exhausted"`), `:131` (`"consumer_not_found: no record for lifecycle change"`),
`:139` (`"consumer_generation_conflict: …"`), `:161` (`"consumer_lifecycle_conflict: …"`);
`receive.rs:56`, `settle.rs:55` (`"consumer_generation_fenced: generation {cgen} superseded by {current}"`);
`cleanup.rs:51`, `:59` (fence unverified), `:120` (`"consumer delete aborted: state scan failed: {e}"`);
`commit_plan.rs:67-70` (`AppendErr::Moved => "shard fenced/moved; retry"`, else `format!("{error:?}")`);
`transaction/mod.rs:119-126` (same, plus `Internal(message) => message`);
`shard.rs:2548` (inline `"shard fenced/moved; retry"`); `shard.rs:2385` `"committer gone"`, `:2396` `"committer dropped request"`.

The two `starts_with` ladders — `src/application/consumer/delivery.rs:140-160`
(pull) and `:510-530` (settle):
```rust
                Err(m) if m.starts_with("consumer_not_found") => { … Missing, "consumer_not_found" … }
                Err(m) if m.starts_with("consumer_generation_fenced") => { … Conflict, "consumer_deleted" … }
                Err(m) => {
                    return Err(failure(FailureClass::Internal, "internal", &m, None, true));
                }
```
and the blanket `src/application/consumer.rs:399-403` (`consumer_config_op`, used by
consumer PUT, GET, pull/settle's record load and the DELETE saga's lifecycle CAS):
```rust
        .map_err(|m| failure(FailureClass::Internal, "internal", &m, None, true))
```

So `Moved`, `"committer gone"`, `"committer dropped request"`, fence-unverified
and the lifecycle-CAS conflicts all reach the wire as `500 internal`
(`product.rs:3019-3026`), which the SDK does **not** retry (it retries only
429/503 with `retryable: true`, `sdk/src/index.ts:523-527`). A shard move under
a pull or settle therefore surfaces to SDK users as a thrown 500.
Verified: real.

Buildability of the reviewer's Change: `src/queue.rs` is compiled standalone
into the harness crate `streams-quality-invariants`
(`tools/quality-invariants/src/lib.rs:57-63`, `#[path = "../../../src/queue.rs"] mod queue;`),
which has no `crate::shard`. A `From<AppendErr>` impl **inside `src/queue.rs`
cannot build**. The enum stays dependency-free in `src/queue.rs`; the
conversion lives beside its only user, `DurableEffects::reject`, in
`src/shard/commit_plan.rs`.

---

## 2. Contract decision

### 2.1 Typed contract (internal)

`src/queue.rs`, next to `QueueOut`:

```rust
/// Why the committer refused a queue op. The consumer surface decides its
/// wire answer from the variant alone; only refusals caused by storage carry
/// the storage error, for diagnosis, and no caller reads that text.
#[derive(Debug, Clone)]
pub(crate) enum QueueRefusal {
    /// No Active record of the op's consumer generation exists.
    ConsumerNotFound,
    /// The op's generation was deleted or superseded by a newer one.
    GenerationFenced,
    /// A lifecycle step named a generation the record no longer holds.
    GenerationConflict,
    /// The lifecycle step is not legal from the record's current state.
    LifecycleConflict,
    /// The durable generation fence could not be read or decoded.
    FenceUnverified(String),
    /// Queue or consumer state could not be read, decoded or scanned, the
    /// generation counter is exhausted, or the commit group failed.
    Internal(String),
    /// The engine stopped serving the shard before it answered: fenced,
    /// moved, closing, or its committer stopped. Every queue op is safe to
    /// repeat (leases expire, settle tokens go stale, lifecycle steps are CAS).
    Moved,
}
```

Deliberate deviations from the reviewer's list:
- **No `EngineClosed`.** After C3 the committer's queue is closed exactly when the
  engine retires, so a failed `send` *is* the retirement; a dropped reply means
  the committer itself stopped (panic/abort), i.e. the engine is closing. Both
  are retry-safe for every queue op and map identically; a second variant would
  only be a second name. Folded into `Moved` (documented above).
- **Verdict variants carry no text.** The strings only restated the variant plus
  numbers the caller already holds. This also makes
  `ConsumerGeneration::Fenced { current }` (`commit_plan.rs:203`) a unit variant
  (its field would otherwise become dead code). Storage-caused refusals keep
  their error text (`FenceUnverified`, `Internal`).
- **Never derive `Default`** on `QueueRefusal`, `AppendErr` or `ConsumerGeneration`:
  the mutation analysis in §5 relies on their `FnValue` mutants being unviable.

`AppendErr` gains a storage disposition (C2, `commit_plan.rs`):
`AppendErr::from_storage(&anyhow::Error)` (`Closed` anywhere in the chain →
`Moved`, else `Internal(text)`) and `impl From<slatedb::Error> for AppendErr`
delegating to it; `from_storage` asks the absorber's existing predicate,
`crate::history::absorb_error_is_fence` (made `pub(crate)`), so one predicate
decides "the store closed" for the absorber and the committer.

### 2.2 Wire codes — what changes at the edge

| Decision | Route / trigger | Today | After | Backward-compatible alternative |
|---|---|---|---|---|
| **D1** (46) | raw `POST /v1/stream/{name}` and close, engine retired before enqueue | 429 `overloaded` "append queue full", no `Retry-After` | 503 `shard_moving` "shard fenced by a new owner; retry", `Retry-After: 1` | keep 429 (map `Closed` like `Full`) |
| D1 | product `POST …/records[:batch]`, same | 429 `rate_limited` (retryable) | 503 `temporarily_unavailable` (retryable), `Retry-After: 1` | keep 429 |
| D1 | raw `PUT /v1/stream/{name}` with initial body, same | 429 `overloaded` "queue full" | 503 `shard_moving`, `Retry-After: 1` (`CreationFailure::Opening`) | keep 429 |
| **D2** (47) | any append/close/seal-fence whose group write, handle load or billing-row read fails with `ErrorKind::Closed` | 500 `internal` (raw) / `append_failed` 500 (product) | 503 `shard_moving` / `temporarily_unavailable`, `Retry-After: 1`; queue ops in the group get `Moved` (→ D4) | keep 500 |
| **D3** (48) | op still queued when the committer stops abnormally (panic, abort after the 5 s shutdown grace) | append: 408 `append_timeout` after up to 10 s; queue op: 500 `internal` "committer dropped request" | append: immediate 503 `shard_moving` + `Retry-After: 1`; queue op: `Moved` (→ D4). The op the committer had already *taken* keeps 408 (outcome unknown) | none meaningful — today's answer is a lost reply |
| **D4** (23) | consumer pull, settle, PUT, GET, DELETE when the shard engine retires under the queue op | 500 `internal` (retryable:true, not SDK-retried) | 503 `shard_moving` (retryable:true, SDK auto-retries) | keep 500 `internal` |
| **D5** (23) | pull/settle when the generation-fence row cannot be read/decoded | 500 `internal` | 503 `queue_unavailable` (retryable) — the code pull already uses for its cursor read (`delivery.rs:86-97`) | keep 500 `internal` |
| **D6** (23) | DELETE consumer: lifecycle CAS lost a race (`GenerationConflict`, `LifecycleConflict`) | 500 `internal` | 409 `consumer_lifecycle_conflict` (new code, retryable:true — a re-issued DELETE re-reads and answers 204) | map both to 500 `internal` (no new code) |
| D6 | DELETE consumer: record vanished before the CAS (`ConsumerNotFound` from `ConfigLifecycle`) | 500 `internal` | 404 `consumer_not_found` (unreachable in practice: tombstones persist) | same as above |
| D7 (informational) | 404 `consumer_not_found` / 409 `consumer_deleted` bodies | message = committer string (`"consumer_generation_fenced: generation 3 superseded by 4"`) | fixed text (`"this consumer generation was deleted"`) | carry detail strings (costs +4 reason re-decisions, §4) |

Unchanged: pull/settle 404 `consumer_not_found` and 409 `consumer_deleted`
codes; DELETE's segment sweep (`deletion.rs:404-412`) and the fleet-internal
`sweep-segment` receiver (`consumer.rs:700-707`) keep 503
`segment_cleanup_failed`; 500 `internal` for every other storage refusal.

The consumer mapping (C4, `src/application/consumer.rs`, beside
`ConsumerFailure::ownership`):

| `QueueRefusal` | class → status | code | retryable | message |
|---|---|---|---|---|
| `ConsumerNotFound` | Missing → 404 | `consumer_not_found` | false | "no active record of this consumer generation" |
| `GenerationFenced` | Conflict → 409 | `consumer_deleted` | false | "this consumer generation was deleted" |
| `GenerationConflict`, `LifecycleConflict` | Conflict → 409 | `consumer_lifecycle_conflict` | true | "another lifecycle change raced this one; retry" |
| `FenceUnverified(e)` | Unavailable → 503 | `queue_unavailable` | true | `"consumer fence unverified: {e}"` |
| `Internal(e)` | Internal → 500 | `internal` | true | `e` |
| `Moved` | Unavailable → 503 | `shard_moving` | true | "the shard moved or is closing; retry" |

---

## 3. Red tests

`cargo test` names are lib paths (`cargo test --locked --lib -- --exact <path>`).

### C1 (46)

**R46a** — `src/shard/retirement_tests.rs` (new fn, not pinned):
`shard::retirement_tests::r46_a_retired_engine_refuses_enqueue_as_moving_not_overloaded`
```rust
/// R46: a retired engine's queue refuses as a moving shard, like every other
/// closed-engine exit; only a full queue is back-pressure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r46_a_retired_engine_refuses_enqueue_as_moving_not_overloaded() {
    let fixture = Fixture::new("r46-closed-enqueue").await;
    fixture.engine.begin_close();
    let (request, _reply) = fixture.append();
    let refused = fixture
        .engine
        .try_enqueue(request)
        .expect_err("a retired engine admits nothing");
    assert!(matches!(refused, EnqueueError::Closed), "{refused:?}");
    let moving = crate::application::append::AppendFailure::from_enqueue(refused);
    assert_eq!(moving.code, crate::application::append::AppendCode::ShardMoving);
    assert!(!moving.definitively_rejected());
    let response = crate::http::render_append(Err(moving));
    assert_eq!(response.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let full = crate::http::render_append(Err(
        crate::application::append::AppendFailure::from_enqueue(EnqueueError::Full),
    ));
    assert_eq!(full.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
    assert!(!full.headers().contains_key("retry-after"));
    fixture.finish().await;
}
```
(`retirement_tests.rs` is `#![cfg(test)]`, so `crate::http`/`axum` are allowed —
`assert_moved`, `:184-203`, already does this.)

**R46b** — `src/application/creation.rs` `mod tests` (new fn; the pinned
`r05_…` body is untouched): `application::creation::tests::r46_a_closed_committer_queue_is_a_moving_shard_not_an_overload`
```rust
    #[test]
    fn r46_a_closed_committer_queue_is_a_moving_shard_not_an_overload() {
        use super::{CreationError, CreationFailure};
        use crate::shard::EnqueueError;
        let closed = CreationError::from_enqueue(EnqueueError::Closed);
        assert_eq!(
            (closed.kind, closed.code, closed.retry_after),
            (CreationFailure::Opening, "shard_moving", Some(1))
        );
        let full = CreationError::from_enqueue(EnqueueError::Full);
        assert_eq!(
            (full.kind, full.code, full.retry_after),
            (CreationFailure::Overloaded, "overloaded", None)
        );
    }
```
(No `crate::http` here: `src/application/` is a hard owner even in tests,
`architecture-gate.py:106-116`.)

Expected red (current tree, one compile of the lib tests):
```
error[E0599]: no function or associated item named `from_enqueue` found for struct `AppendFailure` in the current scope
  --> src/shard/retirement_tests.rs
error[E0599]: no function or associated item named `from_enqueue` found for struct `CreationError` in the current scope
  --> src/application/creation.rs
```

### C2 (47)

**R47** — `src/shard/commit_plan.rs` `mod tests`:
`shard::commit_plan::tests::r47_a_closed_database_is_the_engines_retirement_not_an_internal_error`
```rust
    #[test]
    fn r47_a_closed_database_is_the_engines_retirement_not_an_internal_error() {
        for reason in [
            slatedb::CloseReason::Clean,
            slatedb::CloseReason::Fenced,
            slatedb::CloseReason::Panic,
        ] {
            let closed = slatedb::Error::closed("arbitrary wording".into(), reason);
            assert!(matches!(AppendErr::from(closed), AppendErr::Moved), "{reason:?}");
            let wrapped = anyhow::Error::new(slatedb::Error::closed(String::new(), reason))
                .context("billing row read");
            assert!(matches!(AppendErr::from_storage(&wrapped), AppendErr::Moved), "{reason:?}");
        }
        for message in ["Fenced", "Closed error", "detected newer DB client"] {
            let unavailable = slatedb::Error::unavailable(message.into());
            assert!(matches!(AppendErr::from(unavailable), AppendErr::Internal(_)));
            assert!(matches!(
                AppendErr::from_storage(&anyhow::anyhow!(message)),
                AppendErr::Internal(_)
            ));
        }
    }
```
Expected red:
```
error[E0599]: no function or associated item named `from_storage` found for enum `AppendErr` in the current scope
  --> src/shard/commit_plan.rs
```
plus, for `AppendErr::from(closed)`, rustc's "no `From<slatedb::Error>` for
`AppendErr`" error (E0277 `the trait bound AppendErr: From<slatedb::Error> is not
satisfied`, or E0308 `expected AppendErr, found slatedb::Error` — `AppendErr`
has only the blanket reflexive impl, so the exact code depends on inference).
No deterministic behavioural red exists without new test hooks in `shard.rs`
(the acker races the write: it retires the engine as soon as `close_reason`
appears, `shard.rs:3148-3152`); the unit test is the falsifiable regression and
the three call sites are one-token type changes.

### C3 (48) — held-commit test (the synchronization change's required proof)

**R48** — `src/shard/commit_command_tests.rs`:
`shard::commit_command_tests::r48_ops_queued_behind_a_stopped_committer_are_answered_moved`
```rust
/// R48: an op still queued when the committer stops was never staged, so it
/// is definitively unwritten and answered Moved; only the op the committer had
/// already taken keeps the unknown outcome of a dropped reply.
#[expect(
    clippy::let_underscore_must_use,
    reason = "r48_ops_queued_behind_a_stopped_committer_are_answered_moved; the fixture waits out termination and closes the database on the way out; a failed wait or close leaves nothing the assertions depend on"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r48_ops_queued_behind_a_stopped_committer_are_answered_moved() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("r48-drain", store.clone()).build().await.unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r48-drain".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        ShardMaintenance::default(),
    );
    let close = |hash| {
        let (resp, reply) = oneshot::channel();
        engine
            .try_close(CloseReq {
                hash,
                generation: None,
                resp,
            })
            .unwrap();
        reply
    };
    let commit_gate = engine.test_hold_commit().await;
    let taken = close([48; 16]);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engine.tx.capacity() < engine.tx.max_capacity() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the committer takes the first op and parks at the commit gate");
    let queued: Vec<_> = (0..3u8).map(|index| close([index; 16])).collect();
    let stopped = engine.test_abort_task("committer");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !stopped.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the committer stops");
    drop(commit_gate);
    for (index, reply) in queued.into_iter().enumerate() {
        let answer = tokio::time::timeout(std::time::Duration::from_secs(5), reply)
            .await
            .expect("a queued op is answered, not left to its caller's timeout");
        assert!(
            matches!(answer, Ok(Err(AppendErr::Moved))),
            "queued op {index} lost its reply: {answer:?}"
        );
    }
    let taken = tokio::time::timeout(std::time::Duration::from_secs(5), taken)
        .await
        .expect("the taken op resolves");
    assert!(taken.is_err(), "the taken op keeps its unknown outcome: {taken:?}");
    let _ = engine.await_terminated(std::time::Duration::from_secs(5)).await;
    let _ = db.close().await;
}
```
Why deterministic: a fresh engine sends nothing on its own queue
(`TrimTick` needs trim debt, `UsageAck` is caller-driven, no absorber is started
in this fixture); the committer takes the first op, passes its `is_closed`
check and parks on the commit gate (`shard.rs:2580-2583`), so capacity returns
to max only after the take; `test_abort_task` (`shard.rs:3214`) aborts it there.

Expected red (current tree): the plain `Receiver` is dropped with the aborted
future, dropping the three queued `CloseReq`s:
```
thread 'shard::commit_command_tests::r48_ops_queued_behind_a_stopped_committer_are_answered_moved' panicked at src/shard/commit_command_tests.rs:<line>:9:
queued op 0 lost its reply: Err(RecvError(()))
test result: FAILED. 0 passed; 1 failed
```

### C4 (23)

Write **R23a** first, alone — it compiles on the tree before C4 and is the
behavioural red; add the other C4 tests with the code.

**R23a** — `src/dst/tests/consumer_product.rs` (626 → ≈690 lines):
`dst::dst_tests::consumer_product::a_pull_racing_its_engines_retirement_is_retryable`
```rust
/// A pull whose engine retires between its consumer load and its Receive is
/// answered as a moving shard — 503 and retryable, so the SDK retries it
/// through the router — never as an internal error.
#[expect(
    clippy::disallowed_methods,
    reason = "a_pull_racing_its_engines_retirement_is_retryable; the parked pull is released and joined before its answer is checked; it must park between its consumer load and its Receive while its engine retires"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pull_racing_its_engines_retirement_is_retryable() {
    let _serial = gap_lock().lock().await;
    let (state, addr) = http_rig(mem()).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(addr, "PUT", "/v1/streams/qc23m", &key, br#"{"format":{"kind":"json"}}"#).await;
    assert_eq!(st, 201);
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/qc23m/records",
        &[("prisma-encryption-key", PRISMA_KEY), ("prisma-routing-key", "k0")],
        br#"{"n":0}"#,
    )
    .await;
    assert_eq!(st, 200);
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/qc23m/consumers/c1",
        &key,
        br#"{"visibilityTimeoutMs":30000,"maxAttempts":3}"#,
    )
    .await;
    assert_eq!(st, 201);
    crate::failpoints::park_pull_before_receive("qc23m");
    let before = crate::failpoints::parked(crate::failpoints::Fp::PullBeforeReceive, "qc23m");
    let pull = tokio::spawn(async move {
        preq(addr, "POST", "/v1/streams/qc23m/consumers/c1:pull", &key, br#"{"max": 1}"#).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while crate::failpoints::parked(crate::failpoints::Fp::PullBeforeReceive, "qc23m") == before {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the pull parks before its Receive");
    let sref = state.deployment.raw_adapter_sref("qc23m");
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let engine = state.engine_for(&desc.resolve_segment("").shard_route).await.unwrap();
    engine
        .await_workers(std::time::Duration::from_secs(10))
        .await
        .expect("the retired engine's committer stops");
    crate::failpoints::release_pull_before_receive("qc23m");
    let (st, _, body) = tokio::time::timeout(std::time::Duration::from_secs(10), pull)
        .await
        .expect("the pull answers")
        .unwrap();
    assert_eq!(
        st,
        503,
        "a pull racing its engine's retirement must be a retryable 503: {}",
        String::from_utf8_lossy(&body)
    );
    let error = &serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"];
    assert_eq!(error["code"], "shard_moving");
    assert_eq!(error["retryable"], true);
    engine_shutdown(&state).await;
}
```
(`key` is `[(&str, &str); 1]`, `Copy`, so the `async move` copies it. The pull
resolves its engine before the failpoint, `delivery.rs:58` then `:117-118`;
`await_workers`, `shard.rs:1797-1801`, begins close and waits for the committer
to exit, so the Receive meets a stopped committer deterministically.)

Expected red (current tree, and after C1–C3): `submit_queue`'s `send` fails →
`"committer gone"` → ladder fallback → `500 internal`:
```
thread 'dst::dst_tests::consumer_product::a_pull_racing_its_engines_retirement_is_retryable' panicked at src/dst/tests/consumer_product.rs:<line>:5:
assertion `left == right` failed: a pull racing its engine's retirement must be a retryable 503: {"error":{"code":"internal","message":"committer gone","retryable":true}}
  left: 500
 right: 503
```

**R23b** — `src/shard/queue_publication_tests.rs` (129 → ≈185):
`shard::queue_publication_tests::r23_a_stale_generation_is_refused_fenced_by_receive_and_settle`
— **also the only `shard::` test that drives `receive()`/`settle()` to an answer**
(needed for §5):
```rust
/// R23: the committer's generation verdicts are typed. In one group, a Receive
/// and a Settle of a generation older than the one the group already bound are
/// refused GenerationFenced; the current generation's ops are answered.
#[expect(
    clippy::let_underscore_must_use,
    reason = "r23_a_stale_generation_is_refused_fenced_by_receive_and_settle; the fixture closes the database on the way out; a failed close leaves nothing the assertions depend on"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r23_a_stale_generation_is_refused_fenced_by_receive_and_settle() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(Db::builder("r23-generation", store.clone()).build().await.unwrap());
    let (tx, _rx) = mpsc::channel(1);
    let engine = ShardEngine::start(
        "r23-generation".into(),
        db.clone(),
        store,
        ShardConfig::default(),
        tx,
        None,
        Default::default(),
    );
    let hash = [23; 16];
    let receive = |cgen| crate::queue::QueueOp::Receive {
        consumer: "c".into(),
        cgen,
        max: 1,
        visibility_ms: 1_000,
        max_deliveries: 3,
        keys: HashMap::new(),
        covered_to: 0,
    };
    let settle = |cgen| crate::queue::QueueOp::Settle {
        consumer: "c".into(),
        cgen,
        acks: Vec::new(),
        retries: Vec::new(),
        extends: Vec::new(),
        max_deliveries: 3,
    };
    let (mut ops, mut replies) = (Vec::new(), Vec::new());
    for op in [receive(2), receive(1), settle(2), settle(1)] {
        let (resp, reply) = oneshot::channel();
        ops.push(CommitOp::Queue { hash, op, resp });
        replies.push(reply);
    }
    engine.commit_group(ops, &ShardConfig::default()).await;
    let mut answers = Vec::new();
    for reply in replies {
        answers.push(
            tokio::time::timeout(std::time::Duration::from_secs(10), reply)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(matches!(answers[0], Ok(crate::queue::QueueOut::Received { .. })), "{:?}", answers[0]);
    assert!(matches!(answers[1], Err(crate::queue::QueueRefusal::GenerationFenced)), "{:?}", answers[1]);
    assert!(matches!(answers[2], Ok(crate::queue::QueueOut::Settled { .. })), "{:?}", answers[2]);
    assert!(matches!(answers[3], Err(crate::queue::QueueRefusal::GenerationFenced)), "{:?}", answers[3]);
    engine.begin_close();
    let _ = db.close().await;
}
```
One group so the second op sees the overlay's bound generation (`receive.rs:36-52`);
no staged config and no durable fence, so `queue()` reaches `receive`/`settle`.

**R23c** — `src/shard/retirement_tests.rs`:
`shard::retirement_tests::r23_a_queue_op_on_a_retired_engine_is_refused_moved` (the reviewer's first step, made deterministic):
```rust
/// R23: a queue op that reaches a retired engine is refused Moved, the typed
/// verdict the consumer surface answers with a retryable 503.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r23_a_queue_op_on_a_retired_engine_is_refused_moved() {
    let fixture = Fixture::new("r23-queue-retired").await;
    fixture.engine.await_workers(Duration::from_secs(10)).await.unwrap();
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        fixture.engine.submit_queue(HASH, QueueOp::ConfigGet { consumer: "c".into() }),
    )
    .await
    .expect("a retired engine answers at once");
    assert!(matches!(refused, Err(crate::queue::QueueRefusal::Moved)), "{refused:?}");
    fixture.finish().await;
}
```

**R23d** — `src/shard/commit_plan.rs` `mod tests`:
`shard::commit_plan::tests::r23_a_group_failure_reaches_queue_ops_as_a_typed_refusal`
```rust
    #[test]
    fn r23_a_group_failure_reaches_queue_ops_as_a_typed_refusal() {
        use crate::queue::QueueRefusal;
        assert!(matches!(QueueRefusal::from(&AppendErr::Moved), QueueRefusal::Moved));
        assert!(matches!(
            QueueRefusal::from(&AppendErr::Internal("maintenance accounting diverged".into())),
            QueueRefusal::Internal(message) if message == "maintenance accounting diverged"
        ));
        assert!(matches!(QueueRefusal::from(&AppendErr::CtMismatch), QueueRefusal::Internal(_)));
    }
```

**R23e** — `src/application/consumer.rs` new `#[cfg(test)] mod refusal_tests`
(explicit imports, no glob → no owners row):
`application::consumer::refusal_tests::r23_queue_refusals_render_by_variant_never_by_text`
```rust
#[cfg(test)]
mod refusal_tests {
    use super::{ConsumerFailure, FailureClass};
    use crate::queue::QueueRefusal;

    #[test]
    fn r23_queue_refusals_render_by_variant_never_by_text() {
        let unavailable: fn(FailureClass) -> bool = |class| matches!(class, FailureClass::Unavailable);
        let missing: fn(FailureClass) -> bool = |class| matches!(class, FailureClass::Missing);
        let conflict: fn(FailureClass) -> bool = |class| matches!(class, FailureClass::Conflict);
        let internal: fn(FailureClass) -> bool = |class| matches!(class, FailureClass::Internal);
        for (refusal, class, code, retryable) in [
            (QueueRefusal::Moved, unavailable, "shard_moving", true),
            (QueueRefusal::FenceUnverified("store unavailable".into()), unavailable, "queue_unavailable", true),
            (QueueRefusal::ConsumerNotFound, missing, "consumer_not_found", false),
            (QueueRefusal::GenerationFenced, conflict, "consumer_deleted", false),
            (QueueRefusal::GenerationConflict, conflict, "consumer_lifecycle_conflict", true),
            (QueueRefusal::LifecycleConflict, conflict, "consumer_lifecycle_conflict", true),
            (QueueRefusal::Internal("consumer_not_found: misleading text".into()), internal, "internal", true),
        ] {
            let failure = ConsumerFailure::from(refusal);
            assert!(class(failure.class), "{code}: {:?}", failure.class);
            assert_eq!((failure.code, failure.retryable), (code, retryable));
        }
    }
}
```
(Keep the identifiers `Response`, `HeaderMap`, `AppState`, `axum` out of this
file — hard-owner scan, `architecture-gate.py:112-116`.)

**R23f** — strengthen `shard::queue_codec_tests::r08_corrupt_queue_rows_refuse_without_replacing_persisted_state`
(`queue_codec_tests.rs:64-72`, not pinned): replace the `.is_err()` assertion with
```rust
        let refusal = tokio::time::timeout(std::time::Duration::from_secs(2), received)
            .await
            .unwrap()
            .unwrap()
            .expect_err("a corrupt row refuses the op");
        assert!(
            match case {
                3 | 4 => matches!(refusal, queue::QueueRefusal::FenceUnverified(_)),
                _ => matches!(refusal, queue::QueueRefusal::Internal(_)),
            },
            "case {case}: {refusal:?}"
        );
```
(case 3 = Receive over a corrupt fence → `queue()`'s fence read; case 4 =
DeleteStep → `delete_step`'s fence read; 0 = corrupt config → `config_record`;
1, 2, 5 = corrupt cursor/lease/marker → `load_queue`). This pins D5's split.

Expected red for R23b–R23f on the pre-C4 tree:
```
error[E0433]: failed to resolve: could not find `QueueRefusal` in `queue`
error[E0432]: unresolved import `crate::queue::QueueRefusal`
```

Adapted assertions (compile-driven, same commit): `transaction_tests.rs:384`
(`assert!(matches!(conflict, QueueRefusal::GenerationConflict));`, add
`QueueRefusal` to the `use crate::queue::{…}` at line 4, alias at line 10),
`queue_publication_tests.rs:72-86` (`Err(crate::queue::QueueRefusal::Internal(message)) if message.contains("group write failed")`)
and `:115-122` (`Err(crate::queue::QueueRefusal::GenerationConflict)`),
`consumer_atomicity.rs:764-768` (`matches!(m, QueueRefusal::GenerationFenced | QueueRefusal::ConsumerNotFound)`, `{m:?}`),
`consumer_delete.rs:442-445` (`Err(QueueRefusal::Internal(m)) => assert!(m.contains("state scan failed"), …)`, `other => panic!(…{other:?})`),
`consumer_saga.rs:510-514` (`matches!(err, QueueRefusal::GenerationFenced)`, `{err:?}`).
Add `use crate::queue::QueueRefusal;` at module level in the three DST files so
no ratcheted test body grows (all edits sit inside `assert!` token trees).

---

## 4. Edits, file by file, in commit order

### Ceilinged files

| File | now | after | ceiling | touched by |
|---|---|---|---|---|
| `src/shard.rs` | 3,232 | C3 3,204, C4 3,203 | 3,232 | C3, C4 |
| `src/history.rs` | 1,713 | 1,713 | 1,713 | C2 (`fn` → `pub(crate) fn`, line 742) |
| `src/http.rs` 3,371 · `src/product.rs` 4,205 · `src/billing.rs` 2,201 · `src/auth.rs` 1,676 · `src/registry.rs` 1,509 · `src/sse/feed.rs` 1,200 · `src/fleet.rs` 1,143 | — | unchanged | = now | not touched |

Other files stay far below 1,000 (largest after: `lifecycle.rs` 903 ±0,
`consumer.rs` ≈845, `consumer_atomicity.rs` 822, `consumer_product.rs` ≈690).
No new files → no new mutation-owner, `by-path-module` or architecture rows.

### `#[expect]`-ratcheted functions touched (every one)

"shrinks" = scope_lines, nested_items and syntax_facts all ≤ merge base, and no
new call/path fingerprint under an `unwrap_used`/`expect_used` scope.

| Commit | Function (file) | Exceptions | Effect | Remedy |
|---|---|---|---|---|
| C1 | `seed` (`creation/initialization.rs`) | too_many_arguments, too_many_lines, **unwrap_used** | −4 lines, 11→10 facts; new `Err`/`from_enqueue`/`refused` call+path fingerprints | re-decide unwrap_used reason (R-1); others shrink |
| C2 | `CommitTransaction::write` (`finalize.rs`) | cast_possible_truncation | 7→7 facts, ±0 lines | none |
| C2 | `CommitTransaction::run` (`transaction/mod.rs`) | cast_possible_truncation | 8→6 facts | none |
| C2 | `CommitTransaction::stage` (`transaction/mod.rs`) | match_same_arms | 8→6 facts | none |
| C2 | `CommitTransaction::billing_rows` (`prepare.rs`) | excessive_nesting | 5→5 facts, ±0 lines (`String`→`AppendErr` is one path either way) | none |
| C3 | `ShardEngine::committer_loop` (`shard.rs`) | let_underscore_must_use, excessive_nesting | −28 lines net, facts shrink (the inline drain was one `select!` token tree plus the second `while`); no `let _ =` remains | **delete** the let_underscore_must_use expect (would be unfulfilled, a denied lint); excessive_nesting still fulfilled by the gather/pacing loops, shrinks |
| C3 | `CommitTransaction::reject_op` (`transaction/mod.rs`) | let_underscore_must_use | +4 lines, +2 facts (exhaustive list) | re-decide (R-2) |
| C3 | `DurableEffects::reject`, `ShardEngine::start` | — | not touched (`start` keeps `committer_loop(rx, cfg)`; the wrap happens inside the loop) | — |
| C4 | `DurableEffects::reject` (`commit_plan.rs`) | let_underscore_must_use, needless_pass_by_value | −3 lines, 7→3 facts | none (both still fulfilled) |
| C4 | `CommitTransaction::reject_op` | let_underscore_must_use (R-2 identity) | −5 lines, −7 facts vs C3 | none |
| C4 | `CommitOp` enum (`shard.rs`) | large_enum_variant | `String` → `crate::queue::QueueRefusal`: one path each | none |
| C4 | `CommitTransaction::queue` (`queue/mod.rs`) | too_many_lines, excessive_nesting, **unwrap_used** | −10 lines, +2 facts; new fingerprints | re-decide all three (R-3, R-4, R-5) |
| C4 | `CommitTransaction::receive` (`receive.rs`) | too_many_lines, **expect_used**, excessive_nesting | −5 lines, −2 facts; new `Err(..)` fingerprint | re-decide expect_used (R-6); others shrink (still >100 lines: 112→107) |
| C4 | `CommitTransaction::settle` (`settle.rs`) | same | −5 lines, −2 facts (121→116) | re-decide expect_used (R-7) |
| C4 | `CommitTransaction::delete_step` (`cleanup.rs`) | too_many_arguments, too_many_lines, **expect_used**, **unwrap_used**, excessive_nesting | +2 lines, +6 facts | re-decide all five (R-8…R-12) |
| C4 | `CommitTransaction::config_put` (`config.rs`) | too_many_arguments | +1 line, +2 facts (exhaustion refusal) | re-decide (R-13) |
| C4 | `CommitTransaction::config_lifecycle` (`config.rs`) | too_many_arguments | −15 lines, −6 facts | none |
| C4 | `config_record`, `config_get`, `submit_queue`, `decide_consumer_generation`, `consumer_config_op`, `sweep_segment` | — | no exception | — |
| C4 | `pull` (`delivery.rs`) | too_many_lines, excessive_nesting | −20 lines, facts shrink; still >100 lines and still nests >4 (foreign-segment branch, `:58-80`) | none |
| C4 | `settle` (`delivery.rs`) | too_many_lines | −20 lines | none |
| C4 | tests `r03a_…` (transaction_tests), `r03_queue_refusal_…` (queue_publication_tests), `a_receive_after_delete_…`, `consumer_fence_survives_ownership_move` | too_many_lines etc. | edits inside `assert!` tokens; ≤ lines | none |
| C4 | test `r08_corrupt_queue_rows_…` (queue_codec_tests) | let_underscore_must_use | +7 lines, + facts | re-decide (R-14) |

Re-decided reason texts (each keeps `owner; invariant; alternative`, exactly two
`;`, no `"`; none of these identities is in `docs/quality/source-allowances.json`
— its 15 `exception` rows are all `http.rs`/`product.rs`/awsbench — so no `--prune`):

- **R-1** `seed` unwrap_used: `seed; covers exactly one site, the durable frontier read, where a poisoned stream state may hold a half-advanced durable frontier; recovering it could seed a child from a length never made durable`
- **R-2** `reject_op`: `CommitTransaction::reject_op; every command variant that carries a reply is named and answered, and a reply is a oneshot whose send fails only when the requester already went away; a handled result would only restate that nobody waits`
- **R-3** `queue` too_many_lines: `CommitTransaction::queue; queue dispatch resolves the consumer fence once and routes every queue op through the same fence verdict, staging each refusal as a typed QueueRefusal; splitting it would separate the ops from the fence that admits them`
- **R-4** `queue` excessive_nesting: `CommitTransaction::queue; the dispatch nests the durable fence read and its FenceUnverified refusal inside the uncached branch of the fence lookup; flattening it would separate the read from the cache it fills`
- **R-5** `queue` unwrap_used: `CommitTransaction::queue; covers the two consumer-fence cache locks, where a poisoned cache may hold a half-raised generation; recovering it could admit an op the fence already superseded`
- **R-6** `receive` expect_used: `CommitTransaction::receive; covers exactly one site, the queue state that was loaded before dispatch; a second fallible read would add a branch no dispatched op reaches`
- **R-7** `settle` expect_used: `CommitTransaction::settle; covers exactly one site, the queue state that was loaded before dispatch; a second fallible read would add a branch no dispatched op reaches`
- **R-8** `delete_step` too_many_arguments: `CommitTransaction::delete_step; the delete step takes the consumer, its queue, the fence and the batch budget separately as the transaction resolved them and answers with a typed QueueRefusal; a request struct would exist for this single call site`
- **R-9** `delete_step` too_many_lines: `CommitTransaction::delete_step; the delete step fences, scans, deletes and accounts in one transaction and refuses at the step that failed; splitting it would hide which rows each fence covers`
- **R-10** `delete_step` expect_used: `CommitTransaction::delete_step; covers exactly one site, the consumer queue entry checked present just above under the same borrow; a fallible read would add a branch no checked step reaches`
- **R-11** `delete_step` unwrap_used: `CommitTransaction::delete_step; covers the fence-table and handle-state locks and the three reads of the queue state populated in this step, where a poisoned lock may hold a half-applied fence or queue; recovering or failing either could delete rows a fence still protects`
- **R-12** `delete_step` excessive_nesting: `CommitTransaction::delete_step; the delete step nests the budget checks inside each prefix scan and lease walk and the fence refusal inside the fence read; flattening them would separate the checks from the rows they bound`
- **R-13** `config_put`: `CommitTransaction::config_put; a config put takes the overlay, stream, consumer, config and reply as the queue dispatch resolved them and refuses an exhausted generation as a typed QueueRefusal; a request struct would exist only for this signature`
- **R-14** test `r08`: `r08_corrupt_queue_rows_refuse_without_replacing_persisted_state; the fixture closes the database on the way out, after each corrupt row's typed refusal is checked; a failed close leaves nothing the assertions depend on`

Verify every count above with the exception-contract step in §7 before commit;
if any other contract reports growth, re-decide it the same way (never widen a
scope, never add a wrapper to dodge the ratchet).

### C1 — item 46 (`EnqueueError::Closed` is the engine's retirement)

1. `src/application/append/contract.rs` — after `from_resolve` (ends line 228):
   ```rust
       /// A full queue is back-pressure. A closed one means the engine retired
       /// before this request was enqueued, so it is answered exactly as a
       /// commit the new owner fenced, and the router converges.
       pub(crate) fn from_enqueue(error: crate::shard::EnqueueError) -> Self {
           match error {
               crate::shard::EnqueueError::Full => {
                   Self::new(FailureClass::Capacity, AppendCode::Overloaded, "append queue full")
               }
               crate::shard::EnqueueError::Closed => Self::from_commit(0, false, AppendErr::Moved),
           }
       }
   ```
   (`from_commit(_, _, Moved)` ignores segment/materialized, `contract.rs:277-282`.)
2. `src/application/append/submit.rs:79-85` → 
   ```rust
       if let Err(refused) = engine.try_enqueue(req) {
           return Err(AppendFailure::from_enqueue(refused));
       }
   ```
   (94 → 90 lines; `fail`, `FailureClass`, `AppendCode` stay used.)
3. `src/application/creation.rs` `impl CreationError` (after `gone`, line 57):
   ```rust
       /// A full committer queue is back-pressure. A closed one is the engine's
       /// retirement, answered as resolution answers a shard mid-move; the
       /// initial body was never enqueued, so the replayed create resumes it.
       fn from_enqueue(error: crate::shard::EnqueueError) -> Self {
           match error {
               crate::shard::EnqueueError::Full => {
                   Self::new(CreationFailure::Overloaded, "overloaded", "queue full")
               }
               crate::shard::EnqueueError::Closed => Self {
                   retry_after: Some(1),
                   ..Self::new(
                       CreationFailure::Opening,
                       "shard_moving",
                       "shard fenced by a new owner; retry",
                   )
               },
           }
       }
   ```
   plus R46b in `mod tests`.
4. `src/application/creation/initialization.rs:147-153` →
   ```rust
           if let Err(refused) = engine.try_enqueue(req) {
               return Err(CreationError::from_enqueue(refused));
           }
   ```
   and R-1 on `seed`'s unwrap_used (lines 14-17).
5. `src/application/lifecycle.rs:887` → `.map_err(|refused| SealError::Resumable(format!("fence not placed: {refused:?}")))?;` (text only; still `Resumable`).
6. `src/application/topology.rs:112-118` → `if let Err(refused) = engine.try_close(req) { tracing::error!(seg_id, ?route, ?refused, "seal close never enqueued"); return None; }` (log only).
7. `src/shard/retirement_tests.rs` — R46a.
8. `docs/refactor/WIRE-MATRIX.md` §1.1 (line 32): after `429 overloaded (committer queue full)` add `503 shard_moving (retry-after: 1; the shard engine retired before the initial body was enqueued)`; §1.2 (line 41): `503 … shard_moving (retry-after: 1; the shard was fenced, or its engine retired before the append was enqueued)`; §2.7 (line 117) needs no change (by-status 503 `temporarily_unavailable`).

### C2 — item 47 (a closed database is `Moved`)

1. `src/history.rs:742` — `fn absorb_error_is_fence` → `pub(crate) fn absorb_error_is_fence` (±0 lines; body, doc and its pinned test unchanged).
2. `src/shard/commit_plan.rs` — after `EnqueueError` (line 28):
   ```rust
   impl AppendErr {
       /// A closed database — fenced by a new owner, shut down, or stopped by a
       /// failed background task — is this engine's retirement, answered like
       /// every other closed-engine exit. The absorber's predicate reads the
       /// typed kind through any context, so no message can decide it.
       pub(super) fn from_storage(error: &anyhow::Error) -> Self {
           if crate::history::absorb_error_is_fence(error) {
               Self::Moved
           } else {
               Self::Internal(error.to_string())
           }
       }
   }
   impl From<slatedb::Error> for AppendErr {
       fn from(error: slatedb::Error) -> Self {
           Self::from_storage(&anyhow::Error::new(error))
       }
   }
   ```
   (`anyhow::Error::new(e).to_string()` is `e`'s Display, so `Internal` texts
   are byte-identical to today's.) Plus R47 in `mod tests`.
3. `src/shard/transaction/finalize.rs:196` → `Err(error) => self.effects.reject(AppendErr::from(error)),` (keep `reject(&str)` for the accounting/failpoint internals, `:7`, `:22`, `:29`).
4. `src/shard/transaction/mod.rs:155` → `Self::reject_op(op, AppendErr::from(error));`; `:58-66` → `tracing::error!(shard = %engine.prefix, "accounting group read failed: {error:?}");` and `Self::reject_op(op, error.clone());`.
5. `src/shard/transaction/prepare.rs:56` → `) -> Result<HashMap<[u8; 16], Option<crate::billing::SegmentBillingMetaV1>>, AppendErr> {`; `:82` → `return Err(AppendErr::from_storage(&error));`.
6. `docs/refactor/WIRE-MATRIX.md` §1.2: extend the `shard_moving` parenthesis with "or its storage closed under the commit".

### C3 — item 48 (the committer's queue answers whoever stops it)

1. `src/shard/commit_plan.rs` — after `EnqueueError`/`from_storage`:
   ```rust
   /// The committer's receiving end. An op still queued when the committer
   /// stops — its close drain, a panic, or the supervisor's abort after the
   /// shutdown grace — was never staged, so it is definitively unwritten: it is
   /// answered Moved instead of being left to its caller's timeout, and closing
   /// first turns every later enqueue into EnqueueError::Closed. A sender caught
   /// between its permit and its push when this runs keeps the unknown outcome
   /// of a dropped reply; the drain cannot await it from Drop.
   pub(super) struct CommitQueue(pub(super) mpsc::Receiver<CommitOp>);
   impl Drop for CommitQueue {
       fn drop(&mut self) {
           self.0.close();
           while let Ok(op) = self.0.try_recv() {
               transaction::CommitTransaction::reject_op(op, AppendErr::Moved);
           }
       }
   }
   ```
   (No backticks-with-brackets in docs: rustdoc runs with `-D warnings` and
   private items; plain names only.)
2. `src/shard/transaction/mod.rs:106-129` — `reject_op`, exhaustive, R-2:
   ```rust
           match op {
               CommitOp::Append(AppendReq { resp, .. })
               | CommitOp::Close(CloseReq { resp, .. })
               | CommitOp::SealFence(SealFenceReq { resp, .. }) => {
                   let _ = resp.send(Err(error));
               }
               CommitOp::Queue { resp, .. } => { /* unchanged String arm until C4 */ }
               CommitOp::UsageAck { .. }
               | CommitOp::BillingClose { .. }
               | CommitOp::BillingRetained { .. }
               | CommitOp::Absorbed { .. }
               | CommitOp::AbsorbedBatch { .. }
               | CommitOp::TrimTick
               | CommitOp::TrimStep { .. } => {}
           }
   ```
   (all three reply senders are `oneshot::Sender<Result<AppendAck, AppendErr>>`: `shard.rs:824`, `commit_plan.rs:15`, `commit_plan.rs:21`).
3. `src/shard.rs`:
   - line 33-36 `use commit_plan::{BillingAckDecision, CommitQueue, ConsumerGeneration, …}` (rustfmt keeps two inner lines, ±0).
   - delete lines 2523-2526 (the let_underscore_must_use expect).
   - line 2531: `async fn committer_loop(self: Arc<Self>, rx: mpsc::Receiver<CommitOp>, cfg: ShardConfig) {`, then after the two comment lines add
     ```rust
             // Every exit — close, panic or abort — drops the queue, which
             // answers every op still waiting in it.
             let mut rx = CommitQueue(rx);
     ```
   - lines 2536-2560 → `_ = self.closed() => return,`; line 2561 → `got = rx.0.recv() => {`.
   - lines 2566-2574 → keep the `reject_op(first, AppendErr::Moved)` and `return;`, delete the `while` (2570-2572); comment: "honor the flag and fail the op we just took; dropping the queue fails the rest."
   - line 2590 `match rx.try_recv()` → `match rx.0.try_recv()`; line 2617 `rx.recv()` → `rx.0.recv()`. **Edit only these tokens** — do not reflow the neighbouring `while`/`if` lines (their `<`/`&&`/`>=` operators must stay out of the diff).
   - Net −28 lines (3,232 → 3,204). The `tokio::select!` macro-dsl row for `crate::ShardEngine::committer_loop` stays valid (same path/owner/count).
4. `src/shard/commit_command_tests.rs` — R48.
5. `docs/refactor/WIRE-MATRIX.md` §1.2: "an append still queued when its engine's committer stops is answered `503 shard_moving` (`retry-after: 1`), not `408`".

### C4 — item 23 (typed `QueueRefusal`)

1. `src/queue.rs` — `QueueRefusal` after `QueueOut` (line 413), text in §2.1. No functions, no derives beyond `Debug, Clone`.
2. `src/shard/commit_plan.rs`:
   - line 38 `pub queue_acks: Vec<ReplyEffect<crate::queue::QueueOut, crate::queue::QueueRefusal>>,`
   - `reject` lines 67-70 → `let queue_error = crate::queue::QueueRefusal::from(&error);`
   - add (beside `reject`):
     ```rust
     /// A queue op shares its group's fate: a retired engine answers it Moved
     /// and any other group failure is internal. Only these two reach a queue
     /// reply; the remaining variants are per-request append verdicts.
     impl From<&AppendErr> for crate::queue::QueueRefusal {
         fn from(error: &AppendErr) -> Self {
             match error {
                 AppendErr::Moved => Self::Moved,
                 AppendErr::Internal(message) => Self::Internal(message.clone()),
                 AppendErr::SeqConflict { .. }
                 | AppendErr::ProducerSeqReused
                 | AppendErr::Closed { .. }
                 | AppendErr::SealSuperseded
                 | AppendErr::ProducerGap { .. }
                 | AppendErr::ProducerStale { .. }
                 | AppendErr::ProducerEpochSeq
                 | AppendErr::CtMismatch
                 | AppendErr::BadBody(_) => Self::Internal(format!("{error:?}")),
             }
         }
     }
     ```
   - `ConsumerGeneration::Fenced { current: u64 }` (line 203) → `Fenced`, and line 209 → `ConsumerGeneration::Fenced` (the field is no longer read; leaving it would be dead code under `-D warnings`). Line 208's comparison stays byte-identical.
   - R23d in `mod tests`.
3. `src/shard/transaction/mod.rs` `reject_op` Queue arm → `CommitOp::Queue { resp, .. } => { let _ = resp.send(Err(crate::queue::QueueRefusal::from(&error))); }`.
4. `src/shard.rs` line 922 → `resp: oneshot::Sender<Result<crate::queue::QueueOut, crate::queue::QueueRefusal>>,`; `submit_queue` (2376-2397): return type `Result<crate::queue::QueueOut, crate::queue::QueueRefusal>`, both `map_err`s → `crate::queue::QueueRefusal::Moved` (`rx.await.map_err(|_| crate::queue::QueueRefusal::Moved)?` fits one line: 3,204 → 3,203).
5. `src/shard/transaction/queue/mod.rs`: line 12 alias; line 36 `Err(QueueRefusal::Internal(error))`; lines 100 and 109 `Err(QueueRefusal::FenceUnverified(e.to_string()))`; lines 123-128 → `self.effects.queue_acks.push((resp, Err(QueueRefusal::GenerationFenced)));`; lines 134-140 → `…Err(QueueRefusal::ConsumerNotFound)…`; R-3, R-4, R-5. (`load_queue` stays `Result<(), String>`; wrapping here keeps its `unwrap_used` identity untouched.)
6. `src/shard/transaction/queue/config.rs`: `config_record` → `Result<Option<ConsumerRecord>, QueueRefusal>` with `.map_err(|error| QueueRefusal::Internal(error.to_string()))?` and `.map_err(|error| QueueRefusal::Internal(format!("consumer_config_corrupt: {error}")))` (its three callers stay byte-identical); `config_put` line 64 → `Err(QueueRefusal::Internal("consumer generation exhausted".into()))` + R-13; `config_lifecycle` lines 129-132, 136-142, 158-164 → `ConsumerNotFound`, `GenerationConflict`, `LifecycleConflict` one-line pushes.
7. `receive.rs:53-60`, `settle.rs:52-59` → `ConsumerGeneration::Fenced => { self.effects.queue_acks.push((resp, Err(QueueRefusal::GenerationFenced))); return; }`; R-6, R-7.
8. `cleanup.rs:51`, `:59` → `Err(QueueRefusal::FenceUnverified(e.to_string()))`; `:118-121` → `Err(QueueRefusal::Internal(format!("consumer delete aborted: state scan failed: {e}")))`; R-8…R-12.
9. `src/application/consumer.rs`: after `fn authorization` (line 115), the `impl From<crate::queue::QueueRefusal> for ConsumerFailure` implementing §2.2's table (doc: "The consumer surface's answer to a committer refusal, decided by variant; the storage text is only the message."); `consumer_config_op` line 403 → `.map_err(ConsumerFailure::from)`; R23e.
10. `src/application/consumer/delivery.rs:140-160` and `:510-530` → `Err(refusal) => return Err(refusal.into()),` (keep both `unreachable!` arms).
11. `src/application/consumer/deletion.rs:410` → `({m:?})`.
12. Tests: R23a–R23f and the adapted assertions listed at the end of §3.
13. `docs/refactor/WIRE-MATRIX.md` §2.12 (PUT) and §2.13 (GET): add `503 shard_moving (retryable)`; §2.14 (DELETE): add `409 consumer_lifecycle_conflict (retryable; a concurrent lifecycle change — re-issue the DELETE)`, `404 consumer_not_found`, `503 shard_moving`; §2.15 (pull) and §2.16 (settle): add `503 shard_moving (retryable; the segment's engine retired under the request)`, `503 queue_unavailable (retryable; the consumer's cursor or generation fence could not be read)`, and keep `500 internal` for the remaining storage refusals.

---

## 5. Mutation-kill analysis (cargo-mutants 27.1.0, `--in-diff`)

Selection rule: a mutant is selected iff its span contains an inserted line or
the first line after a deleted run; `FnValue` spans cover the whole function.
No commit adds a new match guard, comparison or boolean operator on a changed
line in a critical file (the adjacent `>`/`<`/`&&`/`!` lines in
`decide_consumer_generation`, `committer_loop`'s gather, `config_lifecycle`'s
`if` lines stay byte-identical by construction — see the edit notes).

Owners selected (all already registered; **no `mutation_owners.py` row or filter changes**):

| Commit | Owners (filter) | Test-only files classified `production_unchanged` (`#![cfg(test)]`) |
|---|---|---|
| C1 | none (application paths are not critical) → "no experiment" | `retirement_tests.rs` |
| C2 | `commit_plan`, `transaction_finalize`, `transaction_group`, `transaction_prepare` (`shard::`) | — |
| C3 | `shard`, `commit_plan`, `transaction_group` (`shard::`) | `commit_command_tests.rs` |
| C4 | `queue` (harness-lib, `queue::`), `shard`, `commit_plan`, `transaction_group`, `queue_dispatch`, `queue_config`, `queue_receive`, `queue_settle`, `queue_cleanup`, `queue_codec_tests` (`shard::`) | `transaction_tests.rs`, `retirement_tests.rs`, `queue_publication_tests.rs` |

| Mutant | Viable? | Killed by (all under `shard::` unless noted) |
|---|---|---|
| C2 `AppendErr::from_storage` → `Default::default()` | no (`AppendErr` has no `Default`) | — (R47 is the regression) |
| C2 `<impl From<slatedb::Error> for AppendErr>::from` → `Default` | no | — |
| C2 `CommitTransaction::write` → `()` | yes | `commit_command_tests::r03_close_and_fence_wait_for_write_remote_durability_and_dispatch` (close/fence replies dropped → `.unwrap()` on `RecvError`) |
| C2 `CommitTransaction::run` → `()` / `stage` → `()` | yes | same r03 test; `billing_read_tests::r13_…` |
| C2 `billing_rows` → `Ok(Default::default())` | yes | `billing_read_tests::r13_failed_accounting_reads_preserve_group_and_newer_dirty_version` (`rx.await.unwrap().is_err()` fails) |
| C3 `<impl Drop for CommitQueue>::drop` → `()` | yes | R48 (`queued op 0 lost its reply: Err(RecvError(()))`) |
| C3 `CommitTransaction::reject_op` → `()` | yes | R48 (queued `CloseReq`s dropped) |
| C3 `ShardEngine::committer_loop` → `()` | yes | r03 close/fence test (`try_close(..).unwrap()` → `Closed`, or reply `RecvError`) |
| C4 `DurableEffects::reject` → `()` | yes | `retirement_tests::r17b_retirement_before_duplicate_attachment_cannot_erase_remote_dependency` (`assert_moved(first)` gets `RecvError`) |
| C4 `<impl From<&AppendErr> for QueueRefusal>::from` → `Default` | no | — (R23d) |
| C4 `ShardEngine::submit_queue` → `Ok(Default::default())` | no (`QueueOut` has no `Default`) | — (R23c) |
| C4 `decide_consumer_generation` → `Default` | no | — (R23b pins `Fenced`) |
| C4 `CommitTransaction::queue` → `()` | yes | `queue_codec_tests::r08_…` (every case's reply dropped), R23b |
| C4 `config_record` → `Ok(None)` | yes | r08 case 0 (corrupt config must refuse; `Ok(None)` creates it) |
| C4 `config_record` → `Ok(Some(Default::default()))` | no (`ConsumerRecord` has no `Default`) | — |
| C4 `config_put` → `()` | yes | r08 case 0; `retirement_tests::r17b_late_successful_write_settles_without_publishing_retired_effects` (ConfigPut reply) |
| C4 `config_lifecycle` → `()` | yes | `transaction_tests::r03a_…` (conflict reply `.unwrap()`), `queue_publication_tests::r03_…` |
| C4 `receive` → `()` / `settle` → `()` | yes | **R23b only** — no pre-existing `shard::` test drives Receive/Settle past the fence (r08 case 3 is refused in `queue()` first; `plans/queue-settle-clamp.md:145` records the same gap). Without R23b these two would be MISSED and fail the leg. |
| C4 `delete_step` → `()` | yes | r08 case 4 |
| C4 `src/queue.rs` | no mutants (an enum definition) | harness baseline `cargo test -p streams-quality-invariants queue::` must still pass |
| test files (`queue_codec_tests.rs`, all `#![cfg(test)]` files) | none (`#[cfg(test)] mod` subtrees are skipped) | — |

Every killing test bounds its waits (`tokio::time::timeout` ≤ 10 s, or an
immediate `RecvError`), so no mutant can turn into a TIMEOUT. No equivalent
mutant is introduced: the drain exists once (in `Drop`), so there is no second
drain whose removal would be unobservable — the reason the plan rejects "async
close+recv drain in the loop **plus** a `Drop` backstop".

Synchronization proof (policy row "Synchronization or retirement changes"):
R48 is the held-commit test; the existing held-WAL integration tests
(`retirement_tests::r17b_*`, `task_lifecycle_tests::r17a_*`) must stay green.
Loom cannot instrument tokio's `mpsc` from outside
(`commit_handoff/loom_tests.rs:1-4` states the same boundary), so no Loom test is added.

---

## 6. Ledgers (in the commit that needs them)

- **C1**: `docs/refactor/WIRE-MATRIX.md` (§1.1, §1.2). Nothing else (no DST test, no new file, no spawn).
- **C2**: `docs/refactor/WIRE-MATRIX.md` (§1.2).
- **C3**: `docs/refactor/WIRE-MATRIX.md` (§1.2). `docs/quality/source-allowances.json` macro-dsl row `crate::ShardEngine::committer_loop` / `tokio::select` unchanged (still one `select!` in that function).
- **C4**:
  - `docs/refactor/test-inventory.json` — `python3 scripts/test-inventory.py --write` (new `consumer_product::a_pull_racing_its_engines_retirement_is_retryable`; changed bodies of `consumer_atomicity::a_receive_after_delete_in_the_same_group_is_refused`, `consumer_delete::a_failed_config_scan_aborts_the_delete_untouched`, `consumer_saga::consumer_fence_survives_ownership_move`).
  - `docs/quality/owners.json` — one row:
    `{"category":"effect","count":1,"owner":"crate::a_pull_racing_its_engines_retirement_is_retryable","path":"src/dst/tests/consumer_product.rs","reason":"Parked pull fixture owns the pull handle; it is released and joined before its answer is checked. Concurrent execution is required to retire the engine while the pull is parked.","syntax":"tokio::spawn"}`.
    No `unresolved-glob` (new test modules use explicit imports; `commit_plan.rs`'s `mod tests` already has its row), no `global`, no `macro-dsl` (no `json!`/`select!`/`proptest!` added), no `by-path-module`.
  - `docs/refactor/review-mechanisms.json` — `source_adaptations[7]` (`src/shard/transaction_tests.rs::r03a_mixed_transaction_preserves_every_row_reply_and_publication`): replace `after_sha256` with the new body hash (line 384 changes). Keep `before_commit`/`before_sha256`. No other pinned body changes (`r13_…`, the five `r17b_*` tests and the pinned support functions `assert_moved`, `duplicate_order`, `completion_checkpoint`, `remote_missing` are untouched).
  - `docs/refactor/WIRE-MATRIX.md` §2.12–§2.16.
  - `docs/refactor/architecture-policy.json`, `src/dst/tests/README.md`, `scripts/quality/mutation_owners.py`: no change (no new file; no new DST module; all critical files registered).

---

## 7. Controls

From `/Users/sorenschmidt/code/streams`, per commit, in this order.

**Red first**
- C1: `cargo test --locked --lib -- --exact shard::retirement_tests::r46_a_retired_engine_refuses_enqueue_as_moving_not_overloaded` → the two E0599 errors of §3.
- C2: `cargo test --locked --lib -- --exact shard::commit_plan::tests::r47_a_closed_database_is_the_engines_retirement_not_an_internal_error` → E0599 `from_storage` (+ the `From` error).
- C3: `cargo test --locked --lib -- --exact shard::commit_command_tests::r48_ops_queued_behind_a_stopped_committer_are_answered_moved` → `queued op 0 lost its reply: Err(RecvError(()))`, `0 passed; 1 failed`.
- C4: with only R23a added: `cargo test --locked --lib -- --exact dst::dst_tests::consumer_product::a_pull_racing_its_engines_retirement_is_retryable` → `left: 500 / right: 503`, body `{"error":{"code":"internal","message":"committer gone","retryable":true}}`.

**Green** (each named test via `scripts/test-leg.sh`, so a rename cannot pass as `0 passed`):
- C1: the two R46 tests `... ok`; `cargo test --locked --lib -- dst::dst_tests::lifecycle_creation:: dst::dst_tests::append_application::` all ok.
- C2: R47 ok; `cargo test --locked --lib -- shard:: history::tests::r08_fence_disposition_uses_error_kind_through_context` all ok.
- C3: R48 ok; `cargo test --locked --lib -- shard:: dst::dst_tests::runtime_isolation:: dst::dst_tests::runtime_engine_lifecycle:: dst::dst_tests::runtime_retirement::` all ok (held-WAL/cancellation regressions).
- C4: R23a–R23f ok; `cargo test --locked --lib -- shard:: application::consumer dst::dst_tests::consumer_` all ok; `cargo test --locked -p streams-quality-invariants queue::` ok (the harness compiles the new enum).

**Gates** (each commit): `scripts/quality.sh` → `QUALITY_OK`. Its steps that
this change can trip, with the expected result:
- `cargo fmt --all -- --check` clean.
- `cargo clippy --locked --workspace --all-targets -- -D warnings`: no warning; in particular no `unfulfilled_lint_expectations` (committer_loop's let_underscore expect deleted in C3; every other expect still fulfilled — `receive` 107, `settle` 116, `queue` 108, `pull`/`settle` ≈150 body lines > 100), no `dead_code` (`ConsumerGeneration::Fenced` unit in C4).
- `python3 scripts/quality/gate.py --clippy …` (source ratchet): no `accepted exception grew without a new decision`, no `file growth`, no `unregistered source occurrence` (the owners.json effect row in C4). To see the exception contracts before committing: `python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); import source_gate; print("\n".join(source_gate.check()) or "clean")'`.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` clean (no `[..]` links to private items in the new docs).
- `python3 scripts/architecture-gate.py --check` clean (no `crate::http`/`Response` in `src/application/`; no obsolete budget exception — no touched function has one).
- `python3 scripts/test-inventory.py --check` and `python3 scripts/review-evidence.py --check` clean (C4 ledgers).
- `bash scripts/multitenancy-audit.sh` and the `mt_lint::multitenancy_identity_lint` leg: unchanged (no `name: String` parameter, no `.stream_ref(` call added).

**Mutation leg, exactly as CI plans a push** (C2, C3, C4):
```
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<previous slate head> QUALITY_HEAD_SHA=$(git rev-parse HEAD) \
  python3 scripts/quality/verification_plan.py --out target/quality-mutations
QUALITY_MUTANTS_OUT=target/quality-mutations scripts/quality/mutations.sh
```
Expected `plan.json`: `unregistered_mutation_source_files: []`,
`selected_mutation_owners` as in §5's table; driver: every owner `0 missed,
0 timeout` (C4's `queue` owner: "no executable mutants").

---

## 8. Out of scope (follow-ups, deliberately not in this chain)

- `src/shard/transaction/append.rs:43`, `:118` — producer/seq chain load errors are still `AppendErr::Internal(e.to_string())`; same class as item 47, but `append` carries `unwrap_used` (a further re-decision) — separate change.
- Queue-path storage reads that fail with `ErrorKind::Closed` (`load_queue`, `config_record`, the fence reads) still answer `Internal`/`FenceUnverified`, not `Moved`. Applying `from_storage` there is the queue analogue of 47.
- `creation/initialization.rs:154-166` maps any committer refusal of the initial body (including `Moved`) to `408 append_timeout` ("Ambiguous"); unchanged.
- DELETE saga semantics: a lifecycle CAS race could be answered 204 (`TargetGone`) after a re-read instead of 409; this plan only types the refusal.
- `submit_queue` has no `is_closed()` pre-check (a queue op sent to a closing engine waits for the drain rather than refusing at once); harmless after C3.
- `sweep_local` (`consumer.rs:700-707`) still renders `{other:?}` in its 503 message (a message, not a verdict).
- The nanosecond window in `CommitQueue::drop` (a sender between semaphore permit and list push) keeps today's dropped-reply outcome; closing it needs an async drain, which would duplicate the `Drop` drain and create an equivalent mutant.
- `history.rs::absorb_error_is_fence` now serves the committer too; renaming it to a neutral storage predicate touches the pinned `r08_fence_disposition_uses_error_kind_through_context` body and is left for a dedicated move.
- Item 22 (untyped 409) and item 7 remain Søren's pending decisions; nothing here depends on them.

---

## Appendix — where this plan departs from the reviewer's Change

| Reviewer | Plan | Why |
|---|---|---|
| 23: `QueueRefusal` "with `From<AppendErr>`" in `src/queue.rs` | enum in `src/queue.rs`; `impl From<&AppendErr>` in `src/shard/commit_plan.rs` | `src/queue.rs` is compiled standalone by `streams-quality-invariants` (`tools/quality-invariants/src/lib.rs:62`), which has no `crate::shard` — unbuildable as written |
| 23: separate `EngineClosed` | folded into `Moved` | after C3 a failed send *is* the retirement; both map identically and are retry-safe |
| 23: `Storage` | `Internal(String)` | also carries accounting divergence, failpoint and counter exhaustion — not all storage |
| 47: `From<slatedb::Error>` "used at all three sites" | `From<slatedb::Error>` for two sites + `AppendErr::from_storage(&anyhow::Error)` for `billing_rows` | `load_billing_meta` returns `anyhow::Result`; the kind must be read through the chain (the absorber's predicate) |
| 48: "minimum: `rx.close()` before each drain" | `Drop`-owning `CommitQueue` (the reviewer's full form) | the reviewer's own first step (abort the committer) only passes with the `Drop` owner; a close-then-`try_recv` drain in the loop plus a `Drop` backstop would be an equivalent mutant |
| 46: red test "`try_enqueue == Closed`" | `matches!` | `EnqueueError` derives only `Debug` |
| 23: red "queue op on a retired engine" | awaits the committer's exit first (`await_workers`) and adds the wire-level DST red | without the wait the current tree answers one of three strings nondeterministically |

---

## Skeptic corrections (C1..C12)

Checked against the tree at `e578402d` (local `slate`, one commit ahead of
`origin/slate` = `fba5af56`; the working tree has an unrelated uncommitted
edit to `src/fleet/outbox.rs`). I re-checked every quoted `src/` line in §1–§4
by content. All of them hold: shard.rs 1837-1844, 2376-2397, 2523-2575,
919-923, 1917-1919; submit.rs 79-85; initialization.rs 147-153;
lifecycle.rs 881-887; topology.rs 112-118; finalize.rs 196; transaction/mod.rs
52-66, 110-129, 152-157; prepare.rs 53-84; queue/mod.rs 12, 36, 100, 109,
126, 137; config.rs 16/19/64/131/139/161; receive.rs 53-61; settle.rs 52-60;
cleanup.rs 51/59/120; commit_plan.rs 24-28, 38, 67-70, 203-209;
delivery.rs 140-160, 510-530; consumer.rs 393-403; history.rs 742-748,
1183-1195; sdk/src/index.ts 523-527.

The following also check out:
- The red outputs for R46/R47/R48/R23a are the ones the tree produces. R48:
  dropping the aborted future drops the plain Receiver, so queued replies are
  `Err(RecvError(()))`. R23a: after `await_workers` the committer has
  returned, so `send` fails and the reply is "committer gone", which the
  ladder turns into 500.
- The fact-count arithmetic in §4 matches how `tools/quality-syntax/src/scan.rs`
  counts facts: a call gives call-site + path, a method gives method-call +
  method-call-site, and macro arguments are opaque tokens.
- The `src/queue.rs` harness constraint is real
  (`tools/quality-invariants/src/lib.rs:57-63`).
- Every file that changes under a critical prefix is either registered or
  starts with `#![cfg(test)]` (`commit_command_tests.rs`,
  `queue_publication_tests.rs`).

The findings:

**C1 — BLOCKING (commit C1): both `from_enqueue` functions fail
`clippy -D warnings` on `needless_pass_by_value`.** `EnqueueError` derives only
`Debug` (`src/shard/commit_plan.rs:24-28`). `AppendFailure::from_enqueue(error:
EnqueueError)` and `CreationError::from_enqueue(error: EnqueueError)` only
match unit variants, so the parameter is never moved. The workspace enables
`needless_pass_by_value = "warn"` (`Cargo.toml [workspace.lints.clippy]`), and
the repo already carries three expects for this lint (`commit_plan.rs:63-66`,
`history.rs:866-869`, `http/read.rs:374-377`). So the lint fires, and
`-D warnings` fails C1.
Fix: in C1, change `commit_plan.rs:24` to
`#[derive(Debug, Clone, Copy, PartialEq, Eq)]`. R46a can then use
`assert_eq!(refused, EnqueueError::Closed)`. Consequence: C1 now edits
`src/shard/commit_plan.rs`, a registered owner (`commit_plan`, filter
`shard::`). §5's C1 row changes from "none → no experiment" to
"commit_plan selected, zero mutants (derive attribute only); the driver
reports an explicit empty selection".

**C2 — BLOCKING (commit C2): editing `CommitTransaction::stage` selects a
mutant the repo already records as unbounded.** `src/shard/transaction/mod.rs:155`
is inside `stage`, so any change there selects the `stage → ()` FnValue mutant.
Commit `4f522b0f` exists only to keep `stage` out of mutation diffs. Its
message says "A blank stage hangs every waiting reply instead of failing a
test, so the mutation harness cannot bound that mutant". The same statement
is in the `match_same_arms` reason at `transaction/mod.rs:130-133`. §5's claim
that `run → ()` / `stage → ()` are "killed by r03 / r13" does not help: a
failing test does not end the libtest run while another `shard::` test hangs,
and TIMEOUT counts as a miss (`docs/RUST-QUALITY.md:156`).
- `run → ()` (edited at mod.rs:58-66 for the billing site) drops every reply in
  the same way.
- So does `write → ()` (finalize.rs:196).
- Nobody has edited `run`, `stage` or `write` since 2026-09-11, so no CI run
  has ever shown these mutants bounded.

Fix:
- Take the handle-load site (mod.rs:155) out of C2 and move it to §8 with
  this reason. No restructure keeps `stage` out of the diff, because the
  error is already flattened to a String before `reject_op`.
- Before committing the billing site (mod.rs:58-66 plus prepare.rs) and the
  write site (finalize.rs:196), run `cargo mutants` locally against exactly
  `run` and `write`, using CI's timeout. If either times out, defer that site
  too.
- Narrow D2's trigger text to the sites that survive.

**C3 — BLOCKING before push (commit C3): `committer_loop → ()` is recorded as
not observable within a bounded time, and C3 cannot avoid it.** The message of
commit `32b843f6` ("the fenced call sites inside the committer loop and the
engine close, whose blank-body mutants no bounded test can observe") and the
`needless_pass_by_value` reason at `commit_plan.rs:65` both say this. C3
necessarily edits the body of `committer_loop`. Even a signature-only change
touches the line that holds the body's `{`, because the signature fits on one
rustfmt line.
- R48 and `task_lifecycle_tests::r17a_unexpected_required_engine_role_exit_fences_serving`
  would fail fast under this mutant (the engine closes at birth through
  `RequiredExit`).
- The risk is that some other `shard::` test hangs.
- §7's expected result, "every owner 0 missed, 0 timeout", rests on no
  evidence and contradicts the record.

Fix:
- Make the local CI-plan mutation run for C3 (already in §7) a gate that
  must pass before C4 is written.
- If `committer_loop` times out, add a preparatory commit that bounds the
  hanging `shard::` test (find it from the mutant's test log). Bounding every
  wait is policy-compliant.
- Record the outcome in the C3 commit message.

**C4 — minor (§5, Appendix): the reason for rejecting "loop drain plus Drop
backstop" is wrong.** cargo-mutants 27.1.0 generates no mutants that delete a
statement or a loop. Only a separate drain helper function would add a FnValue
mutant that the backstop makes equivalent. An inline second drain would not.
The chosen design (drain only in Drop) is still sound. Correct the rationale
so it does not become precedent.

**C5 — minor (C3): the `CommitQueue` comment overstates what it covers.** "Every
exit — close, panic or abort — drops the queue" misses two cases:
- an abort before the committer's first poll, because
  `let mut rx = CommitQueue(rx);` runs on first poll;
- a spawn the supervisor rejects while stopping. `EngineTasks::required`
  (`src/shard/lifecycle.rs:17-37`) then drops the closure, which holds the
  raw `Receiver`.

Either state the gap in the comment, or wrap before spawning. The second
option edits `ShardEngine::start`, which carries the 399-line budget
exception in `docs/refactor/architecture-policy.json`, so check its length
if you choose it.

**C6 — minor (§4 line deltas): the "one-line pushes" are three lines under
rustfmt.** There is no `rustfmt.toml`, so `chain_width` defaults to 60.
`self.effects.queue_acks.push((resp, Err(QueueRefusal::ConsumerNotFound)));` has
a 72-character chain and splits into `self.effects` / `.queue_acks` /
`.push(...)`, as `config.rs:62-64` already does. The line counts in §4 for
`receive`, `settle`, `queue`, `config_lifecycle` and `delete_step` are
therefore off by about 2 per site. There is no gate consequence: every
affected contract still shrinks or is re-decided. `receive` ends at about
109 lines and `settle` at about 118, both still above 100, so
`too_many_lines` stays fulfilled. The shard.rs figure 3,203 holds:
`rx.await.map_err(|_| crate::queue::QueueRefusal::Moved)?` is a 56-character
chain, which fits. Do not use the §4 line numbers as acceptance values; take
them from the exception-contract step.

**C7 — ledger (C4): `review-mechanisms.json` `source_adaptations[7]` needs
more than a new hash.** Its `reason` says "Every exact
row/reply/accounting/publication assertion is unchanged". After C4 a reply
assertion in `r03a` changes (the conflict is now typed). Update `reason`, and
extend `finding` with the review item, in the same edit as `after_sha256`.
`scripts/review-evidence.py --check` only compares hashes, so a stale reason
text would pass silently.

**C8 — baseline refresh.** The plan says the tree is at `47799eb3`. HEAD is
now `e578402d`, which rewrote `src/registry.rs` to **1,501** lines, so its
ceiling is 1,501, not 1,509 (the §4 table row is stale). The plan does not
touch that file. Every `src/` line cited by the plan is unchanged. For the
local mutation controls, set `QUALITY_BEFORE_SHA` to the previous commit when
checking commits one at a time, and to `origin/slate` at push time for the
aggregate run. If the chain is pushed as one push, CI evaluates the aggregate
diff: C2's `run`/`stage`/`write` edits are selected even if C3/C4 pass.

**C9 — minor (D3 table).** D3 says a queued queue op answers `Moved (→ D4)`.
Between C3 and C4, `reject_op`'s Queue arm still sends the string
`"shard fenced/moved; retry"`, which the ladder turns into 500 `internal`.
State that D3's queue-op answer only takes effect with C4.

**C10 — minor (§3 end).** "All edits sit inside `assert!` token trees" is not
true for `consumer_delete.rs:442-445`: the match patterns change from
`Err(m)`/`Ok(o)` to `Err(QueueRefusal::Internal(m))`/`other`. There is no
ratchet impact, because that test (`consumer_delete.rs:363-368`) has no
`#[expect]`. Fix the claim so a later reviewer does not rely on it.

**C11 — verified, no change:** the ledgers are otherwise complete.
- `owners.json` needs only the C4 effect row: the gate unions
  `source-allowances.json` and `owners.json`, per `source_gate.py:45-52`.
- `unresolved-glob` rows already exist for `commit_command_tests.rs`,
  `queue_publication_tests.rs` and `commit_plan.rs::tests`.
- `test-inventory.json` covers `src/dst` only, so C4 is the only commit that
  touches it.
- No new DST module and no new file, so no README, `mutation_owners.py` or
  architecture-policy rows are needed.
- The mt-audit fingerprints are content-based, and none of the lines they
  match is moved.
- No documented contract outside `docs/refactor/WIRE-MATRIX.md` names the
  changed codes. `sdk/src` does not enumerate them.

**C12 — verified, no change:** the controls can all be run as written.
`scripts/test-leg.sh`, `scripts/quality/{verification_plan.py,mutations.sh,gate.py}`
exist. The helpers and APIs the tests use also exist:
`AbortHandle::is_finished`, `Sender::max_capacity` (tokio 1.52.3),
`slatedb::Error::{closed,unavailable}` and `CloseReason::{Clean,Fenced,Panic}`
(Copy) in the pinned fork, and the failpoint and fixture helpers.

**Verdict: ready-with-corrections.** C1 is a mechanical fix. C2 cuts scope:
drop the `stage` site, and prove `run`/`write` locally or defer them. C3 adds
a mandatory local mutation gate with a planned fallback. The rest of the plan
(typed contract, red tests, ratchet re-decisions, ledgers) survived
verification.
