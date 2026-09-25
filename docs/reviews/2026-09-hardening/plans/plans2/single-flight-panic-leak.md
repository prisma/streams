# Plan: single-flight owners leak their in-flight marker when the detached loader panics

Repo: /Users/sorenschmidt/code/streams, branch slate @ 71345c03. Review rank 15.
Verified first-hand 2026-09-22 (read-only). **The claim is correct on both owners**; the
reviewer's line numbers have drifted only slightly (all citations below are current).

## 0. Verdict in one paragraph

Both single-flight owners spawn a detached task that is the ONLY thing that clears the
in-flight marker, and neither task has an unwind boundary around the foreign code it runs.
`OpenGate` (src/sharddir.rs:557-640) awaits the opener under `tokio::time::timeout` with no
`catch_unwind`; a panic drops the watch sender, leaves `PrefixGate::inflight = Some(rx)` and
`OPENS_IN_FLIGHT` +1 forever, so every later `get_or_open` coalesces onto the dead channel and
returns `Failed("shard open task vanished")` (line 654) with no strike, holdoff or
`health.failed`, and `ShardDirectory::shutdown` counts the phantom open until its grace
expires (src/shard_directory.rs:388-405). `PostingsCache::spawn_load` (src/postings_cache.rs:714-729)
awaits `load_runs` with no boundary; a panic skips `finish_load` (the only remover of the
`inflight` key, line 743), so every later `runs_for` for that key spins four `Decision::Wait`
turns on the dead channel and falls through to the uncached load (lines 593-596, 654-673),
and `maybe_prefetch` is disabled for the key forever (line 803). There is a real production
route into the OpenGate leak: `on_slatedb_rt` (src/bootstrap/runtime_handoff.rs:28-29) does
`rx.await.expect("slatedb-rt task dropped")`, so a panic inside the SlateDB open task
becomes a panic inside the gate's opener task. Only the reaper (src/sharddir.rs:617-623) has
a `catch_unwind`, and it guards the wrong thing (a late completion, not the first poll).

---

## 1. Mechanism, with file:line evidence

### 1a. OpenGate (src/sharddir.rs)

| Step | Evidence |
| --- | --- |
| The open task is the owner of the outcome and of the marker. | 549-556 comment; 542-543 `let (tx, rx) = watch::channel(None); g.inflight = Some(rx.clone());` |
| The opener is polled with a deadline and **no unwind guard**. | 564-566 `let mut fut = Box::pin((inner.opener)(p.clone(), incarnation)); ... tokio::time::timeout(inner.open_deadline, &mut fut).await;` |
| Every arm that clears the marker is *after* the await. | 569 `publish_open` (clears at 920); 580 `g.inflight = None` (Ok(Err)); 603 `g.inflight = None` (deadline). A panic reaches none of them, and 567 `OPENS_IN_FLIGHT.fetch_sub` is skipped too. |
| Tokio catches the task panic; `tx` is dropped unsent. | 557 `tokio::spawn(async move { ... let _ = tx.send(Some(out)); })` at 639 never runs. |
| Every later caller subscribes to the dead channel. | 511-515 `if let Some(rx) = &g.inflight { OPENS_COALESCED += 1; rx.clone() }` |
| ...and gets the vanished error, forever. | 647-658: `rx.borrow()` is `None`, `rx.changed().await.is_err()` -> `Err("shard open task vanished")` -> 662 `OpenOutcome::Failed`. |
| No strike / holdoff / health. | Those live only in the `Ok(Err(e))` arm 571-584 and the deadline arm 585-607. `wait_retired` (678-704) sees no holdoff and no `closing`, so it does not gate either. |
| Shutdown burns its grace. | 796-799 `opens = gates.filter(inflight.is_some() || reaping).count()`; src/shard_directory.rs:388-405 loops until `opens == 0 && pending == 0` or the deadline, then returns `Err("shutdown ongoing or failed: 1 opens, 0 engines; owners retained")`. |
| The reaper's own `catch_unwind` is misplaced. | 617-623: it wraps `fut` only *after* the deadline fired; the first poll (566) is unguarded. |
| Production route. | src/bootstrap/runtime_handoff.rs:24-30: the SlateDB open runs on `slatedb_runtime().spawn(...)`; if that future panics, `tx` is dropped, and `rx.await.expect("slatedb-rt task dropped")` panics **inside the gate's opener task** (bootstrap.rs:484-492 is called from the opener future). The expect's reason ("the opener task is never cancelled ... so its channel is answered") does not cover a panic. Also on the opener task: `load_or_rebuild_maintenance` (bootstrap.rs:501) and `ShardEngine::start` (514). |

Consequence chain observed: one panicking open => the prefix is unopenable for the life of the
process (500 "shard open task vanished" on every request, readiness stays `ok` because
`health.failed` never runs, `/v1/debug/store` shows `in_flight` stuck at 1, and the deploy's
graceful shutdown times out).

### 1b. PostingsCache (src/postings_cache.rs)

| Step | Evidence |
| --- | --- |
| Marker set by the leader under the lock. | 561-562 `let (tx, rx) = watch::channel(false); g.inflight.insert(key, rx);` (and 806-807 for prefetch) |
| The spawned loader has no unwind guard. | 714-729: `tokio::spawn(async move { ... let res = load_runs(&cache, ...).await; ... cache.finish_load(...); ... let _ = tx.send(true); })` |
| `finish_load` is the only remover of the key. | 732-743 doc "A finished load's only way into the map: clears the single-flight marker" ... `g.inflight.remove(&key);` |
| Waiters spin on the dead channel. | 593-596 `Decision::Wait(mut rx) => { coalesced += 1; let _ = rx.changed().await; }` returns at once with `Err` (sender dropped); post-wake 628-653 finds `ready == false`, `still_inflight == true`; the `for _ in 0..4` loop (524) exhausts and 670-680 does the uncached load. Every read of the key: +3..4 `coalesced`, +1 `index_loads`, nothing published. |
| Prefetch disabled for the key. | 803-805 `if g.inflight.contains_key(&key) { return; }` |
| Panic surface. | `load_runs` (883-934) runs `part.scan_with_options(..).await?` / `iter.next().await?` (SlateDB code on the loader task) and the decoder. NOTE: a panicking *object store* does **not** reach here: SlateDB's block fetches are spawned (`FetchTask::InFlight(tokio::spawn(..))`, slatedb/src/sst_iter.rs:395,424) and a panicked fetch is mapped to `SlateDBError::BackgroundTaskPanic` (sst_iter.rs:1070-1084), which `load_runs` already returns as `Err`. The unguarded surface is a panic on the loader task itself (SlateDB iterator/manifest code, decoder invariants). |
| Reason texts. | The reviewer's "632-635" is not a reason text today (it is `runs_for`'s post-wake block). The three candidate reasons — `runs_for` `let_underscore_must_use` (483-486: "a closed channel means the load already published or gave up"), `spawn_load` `disallowed_methods` (693-696) and `let_underscore_must_use` (697-700) — are all accurate after the fix and need no edit. |

Both files are mutation owners (`scripts/quality/mutation_owners.py:72-73`) with test filters
`postings_cache::` and `sharddir::` respectively. **The OpenGate DST tests live under
`dst_tests::runtime_open_gate::` (src/dst/dst_tests.rs:148-149) and therefore never run against a
`sharddir.rs` mutant** — the killing tests must be pathed `sharddir::…` (see §3).

---

## 2. Design

### Constraints that shape the design (measured, not assumed)

1. **Exception ratchet** (scripts/quality/source_rules.py:116-216). Every `#[expect]` is an
   identity `(path, item, kind, reason-text)`. All identities on one function share the same
   scope metrics: `scope_lines` (item span **includes** its doc comments and attributes:
   tools/quality-syntax/src/scan.rs:127-131 uses `node.span()`), `nested_items`, `syntax_facts`
   (every path, call, method-call, macro, attribute, doc line). Under `unwrap_used` every
   `call-site` (full expression tokens, scan.rs:242-255), every `path` (including a bare local
   identifier) is a keyed counter: a **new spelling = new key = "accepted exception grew"**.
   Shrinking or 1:1 replacement of a path fact is free. Growth of any count on any identity
   fails unless *that* identity's reason text changes.
   - `OpenGate::get_or_open` (sharddir.rs:459-668) carries FIVE identities incl. `unwrap_used`
     => only shrink-only edits are free there.
   - `PostingsCache::spawn_load` (701-730) carries THREE (no `unwrap_used`) => counts only;
     a same-arity callee rename is free, a new line or fact is not.
   - `load_runs` (879-934) carries ONE (`too_many_arguments`) => counts only.
2. **File ceiling** 1,000 physical lines, tests count. sharddir.rs 957, postings_cache.rs 941.
3. **Mutation scope** (cargo-mutants 27.1.0, `--in-diff`): a mutant is selected if any line of
   *its own span* is in the diff (in_diff.rs:127-131). Fn-body replacement mutants span the
   whole function, so touching any line of a function selects its body mutant (unviable when
   the return type has no `Default`); operator mutants elsewhere in the function stay out.
   A *verbatim move* puts every operator of the moved body into scope.
4. **New file under `src/shard*` / `src/postings*` needs an owner row** even when test-only:
   `verification_plan.py:164-165,187` puts every ADDED `.rs` into `live`, added files are never
   `production_unchanged` (production_changes.py:209-212 iterates `before` only), and an
   unregistered critical source fails the driver. `src/sse/feed/tests/*.rs` have rows for this reason.
5. **`use super::*` in a new test module = a new `unresolved-glob` occurrence** that needs a
   `docs/quality/owners.json` row (see the `watchdog_policy_tests.rs` row). Import explicitly instead.
6. **Registry failpoints are the wrong injector here**: `crate::failpoints` is stream-name keyed,
   `Fp::ALL.len()` is pinned at 25 by a DST test (src/dst/tests/consumer_generations.rs:715 =>
   inventory regen), `Fp::site` is ratcheted (`too_many_lines`, failpoints.rs:105-108 => reason
   re-decision), and a one-shot arm under a constant name races parallel tests. The registry's own
   scope note (failpoints.rs:7-13) blesses per-instance injection for per-instance state.
7. `[profile.release]` keeps `panic = "unwind"` (Cargo.toml:72-73), so `catch_unwind` is live in prod.

### 2a. Smallest correct design — RECOMMENDED

**OpenGate: put the unwind boundary on the opener at construction, in `OpenGate::new`** (not
ratcheted), via a new child module `src/sharddir/unwind.rs`:

- `unwind::unwind_proof(opener: OpenFn) -> OpenFn` wraps the opener so that a panic — while the
  closure produces its future *or* while the future is polled — becomes
  `Err(anyhow!("shard open panicked: {payload}"))`. The existing `Ok(Err(e))` arm
  (571-584) then does exactly what a failed open does: `OPENS_FAILED`, `health.failed`,
  `inflight = None`, strike, holdoff, `tx.send`. `OPENS_IN_FLIGHT` is decremented at 567.
- `get_or_open` is touched only to **shrink**: the reaper's now-redundant
  `use futures_util::FutureExt; let result = AssertUnwindSafe(fut).catch_unwind().await; match …`
  (618-623, 6 lines) becomes `let engine = fut.await.ok();` (+1 comment). All five ratchet
  identities shrink; no reason text changes.
- Every `OpenGate` is built through `new` (sharddir.rs:433; shard_directory.rs:178), so every
  opener — production (bootstrap.rs:476-553) and every test opener — is wrapped.

**PostingsCache: put the boundary on the OWNED load only**, via a new child module
`src/postings_cache/owned_load.rs`:

- `owned_load::load_owned(...)` = `load_runs` behind `AssertUnwindSafe(..).catch_unwind()`,
  mapping a panic to `Err(anyhow!("postings load panicked"))`. `spawn_load`'s existing
  `Ok(_) | Err(_) => None` arm (722) then hands `loaded = None` to `finish_load`, which removes
  the marker and returns; `tx.send(true)` wakes the waiters; the leader's post-wake sees
  `still_inflight == false` and loads directly (657-668). No Drop guard: `finish_load` stays the
  one way the marker clears (its doc at 732-736 stays true).
- `spawn_load` changes by exactly one identifier: `load_runs(` -> `load_owned(` on line 719.
  Same arity, one path fact replaced 1:1, line stays 99 columns (rustfmt max 100): ratchet-neutral.
  The direct request-task loads (584, 660, 673) are deliberately **not** wrapped: they hold no
  marker, and their task is their boundary (keeps today's request-path panic semantics unchanged).
- The test injector is a per-instance, one-shot `#[cfg(test)]` hook on `PostingsCache`
  (`panic_next_owned_load`), applied inside the boundary through a `#[cfg(test)]`-shadowed
  `let scan = cache.scripted_panic_before(scan);`. In non-test builds no async block exists
  (avoids `clippy::redundant_async_block`).

Why this is "owner-correct" and not a pass-through: each boundary is a property the owner
establishes once (the gate's opener at construction; the cache's owned load at its one call site),
each has its own falsifiable test, and no state, flag or layer is added to the request path.

### 2b. Owner-first alternative (not recommended now)

- **OpenGate**: extract the whole spawned open task (555-640) into
  `src/sharddir/open_task.rs::spawn_open(inner, prefix, tx)` as a verbatim-move commit, then add
  the boundary there. Pros: `get_or_open` shrinks ~85 lines; the boundary sits at the task.
  Costs: (i) new `#[expect(disallowed_methods)]` needs a registered *effect* owner row in
  docs/quality/owners.json (source_rules.py:251-257) and the legacy row
  `crate::OpenGate::get_or_open tokio::spawn ×2` must be pruned (`gate.py --prune`); (ii) the
  now-unfulfilled `disallowed_methods`/`let_underscore_must_use` expects on `get_or_open` must be
  removed (denied lint if left), `too_many_lines`/`excessive_nesting` re-measured; (iii) the moved
  body brings **all** its operators into `--in-diff` scope (`Instant::now() + holdoff_for(..)`,
  the deadline arm, `saturating_add`) and only `sharddir::`-pathed tests kill them => the owner
  filter would have to grow to `sharddir:: dst_tests::runtime_open_gate::`, which runs the 120-SST
  storm scenarios per mutant under `--timeout 90` (mutation TIMEOUT = CI failure). Do this later
  as its own PR if the owner wants `get_or_open` shrunk; it is not needed for correctness.
- **PostingsCache**: wrap in place inside `spawn_load` (`use futures_util::FutureExt;`,
  `AssertUnwindSafe(load_runs(..)).catch_unwind()`, `Ok(Ok((runs, _enc, load_to, false)))`).
  +3 lines/+7 facts on a function with three identities => all three reasons must be re-decided,
  two of them vacuously. If chosen, texts:
  - too_many_arguments: `"PostingsCache::spawn_load; the loader takes the lookup's typed parts exactly as the read path resolved them and runs the scan behind its own unwind boundary; a request struct would exist for this single call site"`
  - disallowed_methods: `"PostingsCache::spawn_load; the single-flight loader runs the scan behind an unwind boundary, publishes into the cache and notifies waiters, and every waiter bounds its own wait on the watch channel; a supervised handle would hold a task nothing joins"`
  - let_underscore_must_use: `"PostingsCache::spawn_load; every waiter may have left before the load or its panic lands; a send with no receivers has nothing to notify"`
  The hook would still need a home inside the boundary (same `#[cfg(test)]` shadowed let).
- A shared `crate::unwind` helper for both owners (and for `tasks/shutdown.rs::classify`'s payload
  downcast, 217-221) is a reasonable *third* step when a third site appears; two sites do not
  justify it.

---

## 3. Red tests (near-complete Rust)

### 3a. OpenGate — `src/sharddir/unwind.rs`, `#[cfg(test)] mod tests` (path `sharddir::unwind::tests::…`)

Compiles against the CURRENT code (uses only `OpenGate`'s existing API), so it is genuinely red.
Explicit imports (no `use super::*`), no statics, no `tokio::select!`. Engine fixture mirrors
`dst/tests/fixture_storage.rs:53-80` (that helper is `pub(super)` to `dst_tests` and unreachable here).

```rust
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use crate::sharddir::{EngineIncarnation, OpenFn, OpenGate, OpenOutcome};

    /// A real engine over an in-memory store (the fixture in
    /// dst/tests/fixture_storage.rs is private to that subtree).
    async fn open_engine(prefix: &str) -> Arc<crate::shard::ShardEngine> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::Db::builder(prefix, store.clone())
            .build()
            .await
            .expect("open db");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        crate::shard::ShardEngine::start(
            prefix.to_string(),
            Arc::new(db),
            store,
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            maintenance,
        )
    }

    /// Panics on its first call, opens normally after: the panic is the
    /// injected fault, the second call is the recovery under test.
    fn panic_once_opener(calls: Arc<AtomicUsize>) -> OpenFn {
        Box::new(move |prefix: String, _inc: EngineIncarnation| {
            let calls = calls.clone();
            Box::pin(async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("scripted opener panic");
                }
                Ok(open_engine(&prefix).await)
            })
        })
    }

    fn describe(outcome: &OpenOutcome) -> String {
        match outcome {
            OpenOutcome::Ready(_) => "Ready".to_string(),
            OpenOutcome::Wait { code, .. } => format!("Wait({code})"),
            OpenOutcome::Failed(msg) => format!("Failed({msg})"),
        }
    }

    /// A panicking opener is a FAILED open, not a vanished one: the
    /// in-flight marker clears, the strike and holdoff arm, health records
    /// the prefix, no phantom open is pending at shutdown, and the next
    /// attempt consults the opener again and installs its engine.
    #[tokio::test]
    async fn a_panicking_opener_fails_its_open_and_the_next_attempt_installs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let shards = Arc::new(RwLock::new(HashMap::new()));
        let gate = OpenGate::new(
            shards.clone(),
            panic_once_opener(calls.clone()),
            Duration::from_secs(30),
        );

        let first = gate.get_or_open("dst-panic", Duration::from_secs(30)).await;
        let OpenOutcome::Failed(msg) = first else {
            panic!("expected Failed, got {}", describe(&first));
        };
        assert!(
            msg.contains("scripted opener panic"),
            "a panicking opener must fail its open with the panic, got: {msg}"
        );
        let (_started, completed, failed, _coalesced) = gate.instance_counters();
        assert_eq!((completed, failed), (0, 1), "the panic is one failed open");
        assert_eq!(
            gate.shutdown_pending().1,
            0,
            "a phantom open is pending: the in-flight marker outlived its task"
        );
        assert_eq!(
            gate.inner.health.0.lock().unwrap().failed.len(),
            1,
            "health must record the failed prefix"
        );
        {
            let st = gate.inner.st.lock().unwrap();
            let g = st.get("dst-panic").expect("gate state for the prefix");
            assert!(g.inflight.is_none(), "the in-flight marker outlived its task");
            assert_eq!(g.strikes, 1, "a panicked open is a strike");
            assert!(g.holdoff_until.is_some(), "a panicked open arms the holdoff");
        }

        // The holdoff is the ordinary failed-open holdoff...
        match gate.get_or_open("dst-panic", Duration::from_secs(1)).await {
            OpenOutcome::Wait { code, .. } => assert_eq!(code, "shard_moving"),
            other => panic!("expected the failed-open holdoff, got {}", describe(&other)),
        }
        // ...and once it passes, the opener runs again and installs. (The
        // holdoff clock is std::time; the test-only reset stands in for it,
        // exactly as the flap test documents.)
        gate.clear_holdoff("dst-panic");
        let engine = match gate.get_or_open("dst-panic", Duration::from_secs(30)).await {
            OpenOutcome::Ready(engine) => engine,
            other => panic!("expected Ready after the holdoff, got {}", describe(&other)),
        };
        assert!(!engine.is_closed(), "the recovery engine must be live");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "the second attempt consulted the opener");
        assert!(!shards.read().unwrap().is_empty(), "the recovery engine is installed");
        engine.begin_close();
    }
}
```

Visibility check: `OpenGate.inner`, `GateInner.{st,health}`, `PrefixGate.{inflight,strikes,holdoff_until}`
are private to `sharddir` and therefore visible in the child module; `ShardHealth.0` and
`OpenHealth.failed` are `pub(super)` (health.rs:6,13) => visible in `sharddir::unwind`.
`instance_counters`, `clear_holdoff` are `#[cfg(test)] pub(crate)` (sharddir.rs:823,841);
`shutdown_pending` is `pub(crate)` (790).

**Exact failure on current code** (after the injected panic's own
`thread 'tokio-runtime-worker' panicked at … scripted opener panic` line):

```
thread 'sharddir::unwind::tests::a_panicking_opener_fails_its_open_and_the_next_attempt_installs' panicked at src/sharddir/unwind.rs:…:
a panicking opener must fail its open with the panic, got: shard open task vanished
```

If that first assertion were removed, the next failures on current code would be, in order:
`(completed, failed) == (0, 0)`, `shutdown_pending().1 == 1`, `health.failed.len() == 0`,
`inflight.is_some()`, and the holdoff call returning `Failed(shard open task vanished)`.

Bounded: every wait is a `get_or_open(.., 30 s)` on an opener that returns immediately; no sleep.

### 3b. PostingsCache — append to `src/postings_cache/tests.rs` (path `postings_cache::tests::…`)

Reuses that file's `mem_db`, `ids`, `runs_of` helpers (lines 15-47) and its existing
`unresolved-glob` owners row. Needs the test-only hook from §4b to compile; the red run is
"hook present, boundary absent" (see §4d ordering).

```rust
/// A panicking OWNED load is a failed load: its single-flight marker
/// clears, so the next read of the key leads a fresh owned load that
/// publishes, instead of spinning on the dead channel and loading
/// uncached forever (with prefetch disabled for the key).
#[tokio::test]
async fn a_panicking_owned_load_clears_its_single_flight_marker() {
    let part = mem_db("wt/panic").await;
    let cache = PostingsCache::new(POSTINGS_CACHE_BYTES);
    let (_, inc, kh) = ids(9);
    cache.panic_next_owned_load();

    // Cold key: this read leads the owned load, which panics once; the
    // reader wakes on the finished (failed) load and loads directly.
    let _ = runs_of(&cache, &part, 9, 0, 100).await;
    assert_eq!(
        cache.coalesced.load(Ordering::Relaxed),
        0,
        "the reader spun on the dead loader's channel: the marker outlived its task"
    );
    assert!(cache.debug_slice(&inc, &kh).is_none(), "a panicked load publishes nothing");

    // The next read leads a FRESH owned load, which publishes.
    let _ = runs_of(&cache, &part, 9, 0, 100).await;
    assert!(
        cache.debug_slice(&inc, &kh).is_some(),
        "the load after a panicked one must publish"
    );
    assert_eq!(
        cache.index_loads.load(Ordering::Relaxed),
        2,
        "one direct load after the panic, one owned load that published"
    );

    // And the published slice serves every later read.
    let _ = runs_of(&cache, &part, 9, 0, 100).await;
    assert_eq!(cache.hits.load(Ordering::Relaxed), 2);
}
```

Trace on the FIXED code: call 1 `Lead` -> loader panics (hook consumed) -> caught -> `loaded = None`
-> `finish_load` removes the key -> `tx.send(true)` -> leader post-wake `ready=false, still_inflight=false`
-> direct `load_runs` (`index_loads = 1`, `provable_to = 100`) -> `Runs`. call 2 `Lead` -> owned load
(`index_loads = 2`) publishes `(covered_from 0, indexed_to 100)` -> post-wake `ready` -> `Hit` (`hits = 1`).
call 3 `Hit` (`hits = 2`). `coalesced` stays 0.

**Exact failure with the hook present and the boundary absent** (current `spawn_load` behaviour):
call 1's loader dies, the leader wakes with `Err`, `still_inflight == true`, loop turns 2-4 are
`Decision::Wait` => `coalesced == 3`, then the contention path loads directly:

```
assertion `left == right` failed: the reader spun on the dead loader's channel: the marker outlived its task
  left: 3
 right: 0
```

(If that assertion were removed: `debug_slice` after call 2 is `None` — nothing is ever published —
and `hits == 0`.) Bounded: `#[tokio::test]` is current-thread; every `rx.changed()` returns at once.

### 3c. Mutation kills (`cargo mutants --in-diff`, owners `sharddir`, `sharddir_unwind`, `postings_cache`, `postings_cache_owned_load`)

| Mutant | Viable? | Killed by |
| --- | --- | --- |
| `unwind_proof` body -> `Default::default()` | unviable (`Box<dyn Fn>` has no `Default`) | — |
| `panic_text` -> `""` / `"xyzzy"` | viable | 3a: `msg.contains("scripted opener panic")` |
| `OpenGate::new` body, `get_or_open` body | unviable (no `Default` for `OpenGate`/`OpenOutcome`) | — |
| reaper line `let engine = fut.await.ok();` | no operators, no mutants | — |
| `load_owned` body -> `Ok(Default::default())` | unviable (`ValidatedRuns` has no `Default`, validated.rs) | — |
| `spawn_load` body -> `()` (in scope because line 719 changed) | viable | 3b: `debug_slice.is_some()` after call 2 / `coalesced == 0`; also every existing test that expects a load to publish |
| `PostingsCache::new` body | unviable (no `Default`) | — |
| `#[cfg(test)]` hook fns | cargo-mutants skips `cfg(test)` items; if ever mutated to a no-op, 3b's `index_loads == 2` (normal path gives 1) kills it | 3b |

No boundary comparisons are added anywhere (no `<`/`>=` on a clock), so there are no boundary mutants
to extract predicates for.

---

## 4. Code change, per file

### 4a. `src/sharddir.rs` (957 -> 955 lines; ceilinged file, net −2)

1. Declare the child module next to `mod health;` (line 74-75), +2 lines:
   ```rust
   mod health;
   pub(crate) use health::ShardHealth;
   /// The opener's unwind boundary; `new` wraps every opener with it.
   mod unwind;
   ```
2. `OpenGate::new` (433-455, not ratcheted), 0 lines: `opener,` -> `opener: unwind::unwind_proof(opener),`.
3. `get_or_open` reaper (617-623), −4 lines, shrink-only under all five identities:
   ```rust
                               tokio::spawn(async move {
                                   // The opener is unwind-proofed in `new`: `fut` cannot panic.
                                   let engine = fut.await.ok();
                                   {
                                       let mut state = reaper.st.lock().unwrap();
   ```
   (removes `use futures_util::FutureExt;`, the `AssertUnwindSafe(fut).catch_unwind()` line and the
   4-line match). Ratchet arithmetic for `get_or_open`: `scope_lines` 190 -> 186; `syntax_facts` −15
   (`use`, path+call `std::panic::AssertUnwindSafe`, method `catch_unwind`, paths `result`, `Ok`×3,
   `Err`×2, `engine`×2, `Some`, `None`) +2 (`.ok()` method facts); `unwrap_site:*` keys only
   disappear (`ordinary-call:std::panic::AssertUnwindSafe(fut)`, `path:std::panic::AssertUnwindSafe`, …).
   The `disallowed_methods` identity keeps `tokio::spawn` ×2 (legacy effect row unchanged).
   **No reason text changes.**
4. Optional in-place doc touch, 0 lines: nothing needed; the module doc (26-36) already states the
   cancellation-proofing; the unwind boundary is documented in `unwind.rs`.

### 4b. NEW `src/sharddir/unwind.rs` (~115 lines incl. tests; needs owner row, §5)

```rust
//! The opener's unwind boundary: a panicking open is a FAILED open.
//!
//! The open task owns the single-flight outcome for its prefix (see the
//! module doc of `sharddir`). Until this boundary existed, a panic inside
//! the opener killed that task together with its result sender: the
//! in-flight marker outlived it, every later `get_or_open` subscribed to
//! the dead channel and failed with "shard open task vanished" — no
//! strike, no holdoff, no health record — and shutdown counted the phantom
//! open until its grace ran out. The production route is real:
//! `bootstrap::on_slatedb_rt` panics when the SlateDB open task panics,
//! because its handoff channel is answered only by an open that completes.

use std::sync::Arc;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;

use super::{EngineIncarnation, OpenFn};
use crate::shard::ShardEngine;

/// Wrap `opener` so that a panic — while it produces its future or while
/// that future is polled — surfaces as `Err`, which `get_or_open` handles
/// exactly like any failed open: strike, holdoff, health, marker cleared.
pub(super) fn unwind_proof(opener: OpenFn) -> OpenFn {
    let opener: Arc<OpenFn> = Arc::new(opener);
    Box::new(
        move |prefix: String,
              incarnation: EngineIncarnation|
              -> BoxFuture<'static, anyhow::Result<Arc<ShardEngine>>> {
            let opener = opener.clone();
            Box::pin(async move {
                // The closure call runs inside the boundary too: a sync
                // panic while building the future is the same failure.
                let attempt = async move {
                    let open = opener(prefix, incarnation);
                    open.await
                };
                match std::panic::AssertUnwindSafe(attempt).catch_unwind().await {
                    Ok(opened) => opened,
                    Err(payload) => Err(anyhow::anyhow!(
                        "shard open panicked: {}",
                        panic_text(payload.as_ref())
                    )),
                }
            })
        },
    )
}

/// The payload as text: `panic!` literals and formatted messages are the
/// two payload types the standard hook produces.
fn panic_text(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

#[cfg(test)]
mod tests { /* §3a */ }
```

Notes: `opener(prefix, incarnation)` calls through `Arc<Box<dyn Fn>>` by auto-deref; if the
pinned compiler objects, spell it `(**opener)(prefix, incarnation)`. The inner `async move` has a
`let` statement, so `clippy::redundant_async_block` does not fire. No `#[expect]` is needed in the
production part. `payload` is only touched via `downcast_ref`, so there is no `unwrap`/`expect`.
`futures_util` is already a dependency (sharddir.rs:237,618). Line width: rustfmt default 100.

### 4c. `src/postings_cache.rs` (941 -> ~949 lines; ceilinged file, net +8, 51 headroom left)

1. After the `use` block (line 29), +3 lines:
   ```rust
   /// The owned load's unwind boundary and its test-only panic hook.
   mod owned_load;
   use owned_load::load_owned;
   ```
2. `PostingsCache` struct (174-187), +3 lines (not ratcheted; the `dead_code` expect is on
   `PostingsSlice::last_bucket_exclusive`, a different item):
   ```rust
       pub warm_extends: AtomicU64,
       /// Test-only: arms the next OWNED load's scripted panic (see `owned_load`).
       #[cfg(test)]
       panic_next_owned_load: std::sync::atomic::AtomicBool,
   }
   ```
3. `PostingsCache::new` (202-222, not ratcheted), +2 lines:
   ```rust
               warm_extends: AtomicU64::new(0),
               #[cfg(test)]
               panic_next_owned_load: std::sync::atomic::AtomicBool::new(false),
   ```
4. `spawn_load` line 719, **0 lines, ratchet-neutral** (one path fact replaced 1:1; the line is 99
   columns — verify with `cargo fmt --check`; if rustfmt ever wraps it, use a 9-character name such
   as `load_held` instead, because a wrapped line grows `scope_lines` on three identities):
   ```rust
               let res = load_owned(&cache, &part, route, inc, kh, start_bucket, target_offset).await;
   ```
5. `spawn_load` doc, optional, **in place on line 687 only** (doc lines are attribute facts inside
   the ratcheted span; the count must not change): `// ... A failed or corrupt load publishes NOTHING (the`
   -> `// ... A failed, corrupt or panicking load publishes NOTHING (the`.
6. `load_runs` (879-934): untouched. `finish_load`, `runs_for`, `maybe_prefetch`: untouched.

### 4d. NEW `src/postings_cache/owned_load.rs` (~55 lines; needs owner row, §5)

```rust
//! The OWNED load's unwind boundary: a panicking load is a failed load.
//!
//! Only the spawned loader (`PostingsCache::spawn_load`) holds a
//! single-flight marker, so only its scan needs a boundary: `finish_load`
//! must run whatever the scan does, or the marker outlives its task — every
//! later read of the key then spins on the dead channel, loads uncached,
//! and its prefetch stays disabled. A request-task load holds no marker;
//! its task is its boundary and it is not wrapped.

use std::sync::Arc;

use futures_util::FutureExt;
use slatedb::Db;

use super::{PostingsCache, load_runs};
use crate::crypto::{RouteHash, RoutingKeyHash, SegmentHash};
use crate::postings::ValidatedRuns;

#[expect(
    clippy::too_many_arguments,
    reason = "load_owned; the owned load takes the read's resolved parts exactly as the loader received them; a request struct would exist for this single call site"
)]
pub(super) async fn load_owned(
    cache: &Arc<PostingsCache>,
    part: &Arc<Db>,
    route: RouteHash,
    inc: SegmentHash,
    kh: RoutingKeyHash,
    start_bucket: u64,
    target_offset: u64,
) -> anyhow::Result<(ValidatedRuns, u64, u64, bool)> {
    let scan = load_runs(cache, part, route, inc, kh, start_bucket, target_offset);
    #[cfg(test)]
    let scan = cache.scripted_panic_before(scan);
    match std::panic::AssertUnwindSafe(scan).catch_unwind().await {
        Ok(loaded) => loaded,
        Err(_payload) => {
            tracing::warn!("postings load panicked; its waiters load directly");
            Err(anyhow::anyhow!("postings load panicked"))
        }
    }
}

impl PostingsCache {
    /// Test-only panic injection for the OWNED load, armed per cache
    /// instance (a registry failpoint keys by stream name, which a postings
    /// load never sees). One-shot: the arm is consumed before it fires.
    #[cfg(test)]
    pub(crate) fn panic_next_owned_load(&self) {
        self.panic_next_owned_load
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    async fn scripted_panic_before<T>(&self, scan: impl std::future::Future<Output = T>) -> T {
        if self
            .panic_next_owned_load
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            panic!("scripted postings load panic");
        }
        scan.await
    }
}
```

Notes: `load_runs` and the private field are visible to the child module. The `panic!` sits in a
`#[cfg(test)]` fn, which clippy treats as test code (`allow-panic-in-tests = true`, clippy.toml).
`Ordering` is spelled inline so non-test builds carry no unused import. The `Send` bound of the
spawned task holds (`&Arc<_>`, the `load_runs` future and `CatchUnwind<AssertUnwindSafe<_>>` are Send).

### 4e. `src/failpoints.rs` (557 -> 559), module doc only (outside every item; no ratchet)

Lines 7-13 scope note: "one injection family lives OUTSIDE this registry BY DESIGN" -> "two
injection families live OUTSIDE this registry BY DESIGN — the shard group-write failure (…) and the
postings cache's owned-load panic (`postings_cache/owned_load.rs`), armed per cache instance because a
postings load has no stream name to key on."

### 4f. Commit shape

One commit per owner, each self-contained and green at its head; the red evidence goes in the
commit message (test name + the exact failure text from §3):

1. `A panicking opener fails its open like any other` — 4a, 4b, owner row `sharddir_unwind`.
   Red proof: run the 4b test against 71345c03 with only the test module added (it compiles
   against the current API) -> `got: shard open task vanished`.
2. `A panicking owned postings load clears its single-flight marker` — 4c, 4d, 4e, 3b test,
   owner row `postings_cache_owned_load`. Red proof: with `load_owned` reduced to
   `load_runs(..).await` after the hook (boundary absent) -> `coalesced left: 3, right: 0`.

No verbatim-move commit is needed: nothing is moved, `get_or_open` only shrinks.

---

## 5. Ledger / doc rows to update

| Ledger | Change | Why |
| --- | --- | --- |
| `scripts/quality/mutation_owners.py` | add `owner('sharddir_unwind', 'src/sharddir/unwind.rs', 'sharddir::')` after line 131 and `owner('postings_cache_owned_load', 'src/postings_cache/owned_load.rs', 'postings_cache::')` after line 72 | new files under critical prefixes `src/shard*`/`src/postings*` (verification_plan.py:26-31,164-165); unregistered = driver failure. Names must be unique (test_mutation_owners.py:103). |
| `docs/quality/owners.json` | **none** | no new spawn/env/static/`select!`/`json!`/glob import in the new files (explicit imports in test modules on purpose). |
| `docs/quality/legacy-source.json` | **none** (frozen). The legacy effect row `crate::OpenGate::get_or_open tokio::spawn count 2` stays exact. | |
| `docs/refactor/test-inventory.json` | **none** — no `src/dst/tests/*.rs` change. (If a DST-level `ShardDirectory::shutdown(grace)` scenario is added later in runtime_open_gate.rs, run `scripts/test-inventory.py --write`.) | |
| `docs/refactor/review-mechanisms.json` | **none** — no pinned mechanism fixture is touched. | |
| `docs/refactor/WIRE-MATRIX.md` | **none**. Wire shapes are unchanged: a panicked open still maps to `ResolveError::OpenFailed` (shard_directory.rs:295 -> http.rs:596) — only the message changes from "shard open task vanished" to "shard open panicked: …" — and later callers now get the existing 503 `shard_moving` holdoff instead of a 500 forever. No row mentions the vanished text (grep clean). | |
| `docs/LIVE-FEED.md` | **none** (no SSE contract change). | |
| Exception contracts (merge-base computed) | no reason text re-decided; new expects: `load_owned` (`too_many_arguments`, 3-part reason above). Run `scripts/quality/verification_plan.py` against the PR merge base before push, then `scripts/quality.sh`. | |
| `src/failpoints.rs` module doc | scope note (4e) | keeps the injection-family audit surface enumerable. |

`cargo machete`/`deny`: no new dependencies.

---

## 6. What could go wrong

- **Wire compatibility**: none breaking. First caller: 500 with a different message; subsequent
  callers: 503 `shard_moving` + `Retry-After` (already a documented outcome) then success. Panics
  on the request-task postings loads keep today's semantics (not wrapped) — deliberate.
- **Fleet skew**: none; both mechanisms are process-local.
- **Hiding bugs**: the default panic hook still prints the panic and backtrace at panic time before
  unwinding; the text also lands in `health.failed` / the `Failed` outcome / `tracing::warn!`;
  strikes escalate the holdoff to the 60 s cap; a store that panics every open makes the instance
  unready after three distinct prefixes (health.rs:58) and the unready watchdog exits it — the same
  path as any persistent open failure, and strictly better than today's silent wedge.
- **`panic = "abort"`**: not set (Cargo.toml:72-73). If ever set, the boundary becomes inert and the
  process aborts — still not a silent wedge.
- **Ratchet surprises**: (i) if rustfmt wraps the `spawn_load` call line after the rename, three
  identities grow by one line — pick a ≤9-character name; (ii) do not add a doc line to
  `spawn_load` or `load_runs`; (iii) keep `get_or_open` edits shrink-only; (iv) explicit imports in
  the new test modules (a glob needs an owners.json row); (v) no statics/`select!` in tests.
- **Mutation leg**: the sharddir owner runs only `sharddir::` tests — the killing test MUST live in
  `src/sharddir/unwind.rs` (§3a), not in `dst/tests/runtime_open_gate.rs`. Expected viable mutants
  are the two `panic_text` literals and the `spawn_load` no-op body; both are killed (§3c).
- **Flakiness**: 3a opens a real SlateDB engine on `InMemory` under a current-thread runtime, as the
  existing gate tests do; no timing dependence (the holdoff is reset with the existing test-only
  `clear_holdoff`, because the holdoff clock is `std::time::Instant`, which paused tokio time does
  not advance). 3b is single-threaded and every channel wait returns at once. The injected panics
  print to test stderr — expected noise, as in the other panic-injection tests.
- **Process-global counters**: 3a asserts only the gate's instance counters and its own state, so
  it does not need `gate_lock()` serialization with the DST gate tests.
- **`on_slatedb_rt`'s `expect("slatedb-rt task dropped")`** (runtime_handoff.rs:29) remains the
  production route that turns a SlateDB-side panic into an opener panic; after this change the gate
  contains it. Turning that `expect` into an `Err` (and re-deciding or removing its reason) is a
  worthwhile separate follow-up, not part of this fix.
- **Reviewer's "reason text at 632-635"**: no stale reason exists at that site today; the three
  candidate reasons (§1b) stay accurate. State this plainly in the PR rather than editing them.

---

## Skeptic corrections

Checked first-hand against slate @ 71345c03 (read-only; `wc -l`, grep, cargo-mutants 27.1.0
source in the registry, `scripts/quality/*.py`). **Verdict: sound with corrections.** Every
function, field, helper and line number the plan cites exists as stated (sharddir.rs 433, 459-478,
542-543, 557-567, 571-584, 585-607, 617-623, 639, 647-658, 790-801, 823, 841; health.rs 6, 13;
postings_cache.rs 174-187, 202-222, 483-498, 524, 561-562, 593-596, 628-668, 689-730, 741-743,
803-807, 879-934; postings_cache/tests.rs 15-47; shard.rs 1357 `ShardEngine::start` 7 params;
runtime_handoff.rs 28-29; shard_directory.rs 178, 295, 387-405). Both red tests fail on the
pre-fix behaviour for exactly the reason stated and pass after the change (traces re-derived
below where the plan's reasoning was thin). The ratchet arithmetic on `get_or_open` is
shrink-only in every metric the gate computes; the `spawn_load` rename is fact-count neutral
(line 719 is 98 columns today, 99 after; rustfmt `fn_call_width` measures the 58-column argument
list, so it stays on one line). Nothing below changes the design; items 1-3 are factual
corrections to the plan's gate reasoning, 4-8 are small concrete edits, 9-11 are optional.

### 1. The two owner files are NOT ceilinged — the limit is 1,000, not the current length

`scripts/quality/source_rules.py:222-230`: `limit = max(1000, before_lines)` then
`min(limit, max(1000, prior_lines))`. The adoption baseline (`docs/quality/legacy-source.json:221,264`)
records postings_cache.rs at 1,167 and sharddir.rs at 979, the merge base has them at 941 and 957,
so the effective ceiling is **1,000 for both**: 59 and 43 lines of headroom. The plan's "ceilinged
file, net −2 / net +8" bookkeeping is correct but the label is wrong. Consequence: the
implementer has room for the one-line comment above `let engine = fut.await.ok();` and for the
`#[cfg(test)]` field + init without any line-neutrality contortions. Keep the new code in the
child modules anyway (the ratchets on `get_or_open`/`spawn_load`/`load_runs` are the real
constraint, and the headroom is worth keeping).

### 2. Constraint 4's mechanism is wrong (conclusion still right for these two files)

`verification_plan.py:333-336` puts an ADDED file into `before` with `''` as its prior source, and
`production_changes.py:65-66` normalizes any file whose root is `#![cfg(test)]` to `''`. An added
whole-file-`#![cfg(test)]` module therefore compares `'' == ''` and IS `production_unchanged` —
which is exactly why `src/sharddir/watchdog_policy_tests.rs` exists under `src/shard*` with no row
in `mutation_owners.py`. The plan's claim "added files are never production_unchanged" is false.
`src/sharddir/unwind.rs` and `src/postings_cache/owned_load.rs` still need their owner rows
because they carry production items (`unwind_proof`, `panic_text`, `load_owned`). Practical
consequence: if the implementer prefers to keep §3a's ~115 test lines out of `unwind.rs`, a
sibling `src/sharddir/unwind_tests.rs` beginning with `#![cfg(test)]` needs NO owner row and no
owners.json row (explicit imports, no glob) — but then `unwind.rs` must declare
`#[cfg(test)] mod unwind_tests;` and the tests must import `super::super::{...}`. Either layout
is fine; do not add a row for a test-only file.

### 3. §3c misses the match-arm mutant genre — no killer is needed, but keep the patterns as written

cargo-mutants 27.1.0 (`src/mutant.rs:23-35`, `src/visit.rs:645-700`) generates `FnValue`,
`BinaryOperator`, `UnaryOperator`, **`MatchArm` (delete an arm, only when the match has a bare
`_ =>` catch-all)**, `MatchArmGuard` and `StructField` (delete a field from a literal with
`..base`). `--in-diff` selects a mutant when ANY line of its span is changed (`in_diff.rs:117-131`),
and `#[cfg(test)]` items are skipped (`visit.rs:862-866`). The plan's new matches use
`Err(payload)` / `Err(_payload)` tuple-struct patterns, not `_`, so no arm-deletion mutant is
generated. Add this as a stated constraint so a reviewer does not "simplify" to `_ => …`:

- `unwind.rs` `match std::panic::AssertUnwindSafe(attempt).catch_unwind().await { Ok(opened) => opened, Err(payload) => … }` — keep `Err(payload)`.
- `owned_load.rs` `match … { Ok(loaded) => loaded, Err(_payload) => … }` — keep `Err(_payload)`, never `_`.

With that, the viable-mutant list in §3c is complete: `panic_text` → `""`/`"xyzzy"` (killed by
`msg.contains("scripted opener panic")`), `spawn_load` body → `()` (killed by 3b's
`coalesced == 0`: with no loader the leader's `rx.changed()` errors at once and
`still_inflight` stays true, so turns 2-4 are `Wait`; also killed by the existing
`cold_load_claims_only_to_its_absorbed_target`). `unwind_proof`/`OpenGate::new`/`get_or_open`/
`load_owned`/`PostingsCache::new` bodies are unviable (`Box<dyn Fn>`, `OpenGate`, `OpenOutcome`,
`ValidatedRuns`, `PostingsCache` have no `Default`). No operator mutants sit on any changed line.

### 4. `[profile.release]` citation

Cargo.toml:71-72 is `[profile.release] lto = "thin"` — there is no explicit `panic =` key. The
conclusion holds (Cargo's default is `unwind`), and the mutation profile `[profile.quality]
inherits = "dev"` (Cargo.toml:77-80) is also unwind, so `catch_unwind` is live in every profile
the gate runs. Cite it as "no `panic = "abort"` anywhere in Cargo.toml" rather than as a line.

### 5. §4d: gate the whole `impl PostingsCache` block, not each fn

As written, non-test builds compile an empty `impl PostingsCache {}` in `owned_load.rs`. Legal,
but pointless; put `#[cfg(test)]` on the block and drop the two per-fn attributes:

```rust
#[cfg(test)]
impl PostingsCache {
    /// Test-only panic injection for the OWNED load, armed per cache
    /// instance (a registry failpoint keys by stream name, which a postings
    /// load never sees). One-shot: the arm is consumed before it fires.
    pub(crate) fn panic_next_owned_load(&self) {
        self.panic_next_owned_load
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    async fn scripted_panic_before<T>(&self, scan: impl std::future::Future<Output = T>) -> T {
        if self
            .panic_next_owned_load
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            panic!("scripted postings load panic");
        }
        scan.await
    }
}
```

`panic!`/`unwrap` inside a `#[cfg(test)]` item are already accepted by this gate's clippy config
(`allow-panic-in-tests`/`allow-unwrap-in-tests`, clippy.toml) — precedent: `sharddir.rs:822-828`
`clear_holdoff` unwraps under `#[cfg(test)]` with no expect. `cargo-mutants` skips the block
(`attr_is_cfg_test`), so the "if ever mutated" hedge in §3c is moot.

### 6. §3a: use the sibling gate tests' runtime flavour

`src/dst/tests/runtime_open_gate.rs` opens the same real engine under
`#[tokio::test(flavor = "current_thread", start_paused = true)]`. Use the same attribute on
`a_panicking_opener_fails_its_open_and_the_next_attempt_installs`: SlateDB's open and the
engine's tickers then auto-advance instead of running against the wall clock, and the plain
`#[tokio::test]` default flavour is the same current-thread runtime anyway. The std-clock holdoff
is unaffected (that is why `clear_holdoff` is used). One residual wall-clock dependence stays:
the `Wait { code: "shard_moving" }` probe must run within 3 s of the failed open
(`HOLDOFF_BASE`, sharddir.rs:47). The gap is a few assertions, so this is not a flake source in
practice; if it ever is, delete that probe — `g.holdoff_until.is_some()` and `g.strikes == 1`
already prove the holdoff armed and `wait_retired` (678-704) is covered by the flap DST test.

### 7. §3a compiles as written; two notes for the implementer

- `let OpenOutcome::Failed(msg) = first else { panic!("…{}", describe(&first)) };` is accepted by
  the borrow checker (the by-move binding is per-arm; the else path never moves `first`). If the
  pinned toolchain objects, write it as a `match` with an `other =>` arm like the DST tests do.
- `OpenOutcome` has no `Debug` derive (sharddir.rs:396), so the `describe` helper is required,
  not decorative — keep it.
- `open_engine(prefix)` in the test passes `prefix` as both the SlateDB path and the engine
  prefix; that matches `fixture_storage.rs:60-78`. `ShardConfig` derives `Default`
  (shard.rs:575). `crate::history::absorber_channel` (history.rs:1105) and
  `crate::shard::load_or_rebuild_maintenance` (shard.rs:453) are `pub(crate)` async/sync as used.

### 8. §4a step 3 ratchet numbers

`get_or_open`'s item span starts at its doc comment (line 457), so `scope_lines` is 212 → 208,
not 190 → 186. Direction is what the gate checks (`exception_growth` fails only on
`count > before`), so nothing changes. Fact delta re-derived from `tools/quality-syntax/src/scan.rs`
(paths in patterns ARE visited: `visit_pat_tuple_struct` → `visit_path`): removed 16 facts
(import, import-target, path+call-site `std::panic::AssertUnwindSafe`, method-call+site
`catch_unwind`, path `result`, paths `Ok`×3/`Err`×2/`Some`/`None`, path `engine`, path `fut`),
added 3 (method-call+site `ok`, path `fut`). Net −13. Under `unwrap_used` the fingerprint keys
`unwrap_site:path:…:fut` stay at count 2, `…:ordinary-call:…Some(engine)` and
`…AssertUnwindSafe(fut)` drop to 0, no new key appears (`.ok()` is a method-call, which is not
fingerprinted). All five identities on the function only shrink; no reason text changes; the
legacy effect row `crate::OpenGate::get_or_open tokio::spawn count 2`
(docs/quality/source-allowances.json) stays exact. `excessive_nesting`, `too_many_lines`,
`unwrap_used`, `disallowed_methods` and `let_underscore_must_use` all remain FULFILLED after the
shrink (the nesting the lint measures is the reaper's `{ let mut state … }` block and the
`if let Some(engine)` inside the second `tokio::spawn`, both untouched), so no
`unfulfilled_lint_expectations` denial.

### 9. Optional: a second `unwind.rs` test for the synchronous-closure panic

`unwind_proof` deliberately calls `opener(prefix, incarnation)` INSIDE the boundary so a panic
while *building* the future is also a failed open. Nothing exercises that path (3a panics inside
the async body, which is also the real production route via `on_slatedb_rt`'s `expect`), and no
mutant can force it (`unwind_proof`'s only mutant is unviable). If the implementer wants
regression cover for it, add a ~12-line sibling test whose opener does
`if calls.fetch_add(1, SeqCst) == 0 { panic!("scripted sync opener panic") }` BEFORE `Box::pin`,
asserting only `Failed(msg)` with `msg.contains("scripted sync opener panic")` and
`shutdown_pending().1 == 0`. Not required for the gate.

### 10. Optional: 3a can avoid the engine fixture entirely

The recovery assertion only needs proof that the SECOND attempt consulted the opener instead of
the dead channel. An opener that panics on call 1 and returns
`Err(anyhow::anyhow!("scripted second failure"))` on call 2 proves that with
`Failed(msg) where msg.contains("scripted second failure")` and `calls == 2`, and drops the
25-line `open_engine` mirror (no SlateDB, no engine tasks, no `begin_close`). The plan's version
is stronger (proves `publish_open` installs after a panicked attempt); keep it if the file budget
does not matter, otherwise this is the cheaper red/green pair.

### 11. Everything else checks out

- Wire: `OpenOutcome::Failed(msg)` → `ResolveError::OpenFailed` (shard_directory.rs:295) → 500
  `shard_open` with the message in the free-text body (http.rs:596-600); the code and status are
  unchanged, WIRE-MATRIX.md pins neither the "vanished" text nor the new one. No fleet skew (both
  mechanisms are process-local). LIVE-FEED.md untouched.
- `docs/refactor/test-inventory.json`: `scripts/test-inventory.py:138` walks only `src/dst`; no
  regen. `docs/refactor/review-mechanisms.json` pins no file this plan touches.
  `docs/quality/verification.json` is a historical receipt, not a live gate.
- `docs/quality/owners.json` / `source-allowances.json`: nothing new to register — no static,
  spawn, env read, `select!`/`json!`/`proptest!`, glob import or `#[path]` in the new files;
  `tokio::test` attributes are not inventoried; `tracing::warn!`/`anyhow!`/`panic!`/`assert*!`
  are in the expression-macro allowlist. A new reasoned `#[expect(clippy::too_many_arguments)]`
  on `load_owned` is reviewed in-source (`source_rules.py:283-285` skips reasoned exceptions);
  its three-part reason matches the `"[^";]+;[^";]+;[^";]+"` check (apostrophes are fine).
- Only one `PostingsCache` struct literal exists (postings_cache.rs:203), so the `#[cfg(test)]`
  field has no other producer. `OpenFn` is unchanged; every `OpenGate` goes through `new`
  (sharddir.rs:433, shard_directory.rs:178, four DST sites).
- The SlateDB claim in §1b holds against the pinned fork (`sst_iter.rs:395,424` spawn the
  fetches; `:1079` maps a panicked fetch to `BackgroundTaskPanic`), so a panicking object store
  is indeed NOT a red injector for `PostingsCache` and the per-instance hook is justified.
- `postings_cache::` and `sharddir::` are the right test filters (`mutation_driver.py:42-54`
  passes them as `--cargo-test-arg`, substring-matched); the new tests' paths
  `sharddir::unwind::tests::…` and `postings_cache::tests::…` match.
