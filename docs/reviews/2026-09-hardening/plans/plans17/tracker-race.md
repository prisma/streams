# External review §9: `QuotaRegistry::admit` tracker race

Tree: `slate` @ `24c4c77a` (= origin/slate, clean). Read-only plan; nothing below has been run.
Sources: `report/external-claims.json` keys `tracker-race-confirm` and `tracker-race-refute`. Both passes confirm the race. Item 50's plan (`plans7/pressure-binding-eviction.md` §1, line 63) also names this window.

**Where this departs from the computed task.** The task suggests a `lookup_or_track` / `charge` seam. I adopt the reviewers' diagnosis and their inflight-pin fix, but I do not add a seam. Two things I found on the tree drive this:

1. **The seam is not needed for a deterministic red.** `admit` takes the per-project request-bucket mutex after the lookup and before the charge (`quota.rs:594-600`). A test that holds that mutex parks a real `admit` from a second thread exactly inside the window, with nothing left to chance (§3).
2. **The seam cannot be built without new exception decisions.** It would move `self.projects.lock().unwrap()` and `admission.bucket.lock().unwrap()` into two new functions. Each would need its own `#[expect(clippy::unwrap_used, reason = …)]`, and both of `admit`'s expects would have to be deleted or re-owned. The exception identity is `(path, qualified item, kind, attribute text)` (`scripts/quality/source_rules.py:196`), so those would be three new reasoned exceptions. The owner-adopted governance rule forbids that when it can be avoided, and here it can.

## 1. Problem (verified on 24c4c77a)

**The lock scope.** `src/quota.rs:531-586`. The map guard `m` lives only inside the `let admission = { … }` block:

```rust
531        let admission = {
532            let mut m = self.projects.lock().unwrap();
533            match m.get(project) {
534                Some(a) => a.clone(),
535                None => {
...                    (eviction sweep 536-558, new entry 559-581)
582                    m.insert(project.clone(), a.clone());
583                    a
584                }
585            }
586        };
587
588        admission.last_seen_ms.store(now_ms, Ordering::Relaxed);
```

**Recency and inflight are written only after `m` drops.** The recency stamp is line 588. The inflight charge is:

```rust
594        if quotas.requests_per_sec > 0
595            && let Err(retry_after_secs) =
596                admission
597                    .bucket
598                    .lock()
599                    .unwrap()
600                    .take(quotas.requests_per_sec as f64, 1.0, now_ms)
...
605        if quotas.max_inflight_requests > 0 {
...
609            let prev = admission.inflight.fetch_add(1, Ordering::Relaxed);
610            if prev >= quotas.max_inflight_requests {
611                admission.inflight.fetch_sub(1, Ordering::Relaxed);
612                return Err(QuotaRefusal::Concurrency);
613            }
614        } else {
615            admission.inflight.fetch_add(1, Ordering::Relaxed);
616        }
617        Ok(QuotaGuard { admission })
```

**The sweep reads only counters and recency.** It runs only in the untracked-project arm, under the same mutex, and only when `m.len() >= MAX_TRACKED_PROJECTS` (16,384, line 46). `quota.rs:543-551`:

```rust
m.retain(|_, a| {
    a.inflight.load(Ordering::Relaxed) > 0
        || a.live_subs.load(Ordering::Relaxed) > 0
        || a.has_pressure()
        || now_ms - a.last_seen_ms.load(Ordering::Relaxed) < IDLE_EVICT_MS
});
```

This is the only removal from the map. `grep -n "projects\.\|\.retain(\|\.insert(\|\.remove(\|strong_count\|Weak" src/quota.rs` finds just `retain` (543), `insert` (582) and the lock sites 532, 646, 892, 910 and 959. There is no `Weak` and no holder test.

**The interleaving.**
1. Project P is tracked and has been idle for at least `IDLE_EVICT_MS`, with every counter at zero.
2. Request A's `admit(P)` clones P at line 534 and releases `m`.
3. Request B's `admit(Q)`, for a first-seen project Q, finds the map full and runs `retain`. P matches no retain clause, so B evicts it.
4. A resumes. It stamps recency, takes a token from the orphan's bucket, increments the orphan's `inflight` and returns `Ok(QuotaGuard { orphan })`. `QuotaGuard` (476-484) holds only the Arc, and its drop decrements the orphan.

The comment at 540-542 ("Never evict a project with inflight requests … their guards point at the Arc we would orphan") states the intent that this window breaks.

**Recency cannot close the window by itself.** Every caller reads `now_ms` before it takes the lock, and the callers use different clock sources:
- `src/product.rs:773` uses `crate::shard::now_ms()`, which is `SystemTime` (`shard.rs:1314-1319`);
- `src/application/watch.rs:296` uses `self.clock.now().ms()`, the runtime `Clock`. That is `SystemClock::now` in production (`runtime.rs:99-104`) and `ManualClock` in tests.

Wall time can step. A stamp compares only as well as two independent clock reads agree.

**Callers of `admit`.**
- `product.rs:773`, inside `project_admission`, which is reached from `http.rs:1810`, `http.rs:1873` and `product.rs:3925`. It holds the guard for the whole handler.
- `application/watch.rs:296`, for capability carriers; `watch.rs:303` then calls `admit_subscription`.
- Tests only: `shard/retirement_tests.rs:393` and `sse/feed/tests.rs:941`.

**Every use site of the tracker entry, and what an orphaned request gets there.** Every later lookup goes through the map.

| Site | Code | Effect on A's own request |
|---|---|---|
| `product.rs:2195` → `quota.rs:645-652` | `admit_append`: `None => return Err(QuotaRefusal::TrackerCapacity)` | **503 `project_tracker_capacity`** (`product.rs:892-897`) on the same request that admit just admitted. Applies when an append-volume quota is set; the function returns at line 642 when both are 0. |
| `product.rs:792` | `let adm = state.quotas.pressure_handle(&p.project_id)?;` | per-project memory gate skipped |
| `product.rs:815`, `831` → `check_read` / `debit_read` (723, 754) | `tracked(...)` returns None | read-debt check skipped; debit dropped |
| `product.rs:1170`, `watch.rs:303` → `admit_subscription` (769) | `else { return Ok(None) }` | live subscription neither counted nor limited |
| `product.rs:2052` → `charge_queued` (872) | `Ok(None)` | queued bytes neither counted nor limited |
| `application/creation/product.rs:55,62` → `needs_stream_seed` / `reserve_stream` (793, 823) | `false` / `Ok(None)` | `max_streams` not enforced |
| `application/creation/deletion.rs:201,313` → `release_stream` (855) | no-op | none (the next seed recounts) |
| `application/append/submit.rs:30`, `http.rs:1914`, `http.rs:2574`, `sse/session.rs:234` → `pressure_handle` (899-901) | None | no stream binding, body charge or feed binding |
| `http.rs:1034` → `stats()` (958-962); `http.rs:858` → `memory_pressure_json` | map walk | orphan's inflight and pressure invisible |

The next `admit(P)` creates a fresh entry with `inflight = 0` and a full bucket. P's concurrency ceiling and rate budget are then split between the orphan and the new entry.

**Existing coverage does not open the window.** `tracker_capacity_churn_evicts_idle_never_active` and `tracker_evicts_idle_projects_never_active_ones` (`src/quota/tests.rs`) pin entries with guards they already hold. `eviction_cannot_remove_a_project_with_pressure` (`src/quota/pressure_tests.rs:167`) pins with pressure. All three are single-threaded.

## 2. Contract decision

- **Fix form: an inflight pin taken under the map lock, with a back-out on refusal. I chose this over a `last_seen` stamp under the lock.**
  - The stamp is not safe against clock steps. The sweep compares B's `now_ms` with A's stamped `now_ms`. Those are two reads taken before two lock acquisitions, possibly from different clock sources (§1). A forward wall-clock step of `IDLE_EVICT_MS` or more between them, or a caller that sits descheduled for that long, reopens the window. §3's red test models exactly that gap, and it stays red under the stamp-only form (control C3).
  - The pin needs no clock. A's `fetch_add` happens before A unlocks the mutex. Any sweep that locks later sees `inflight ≥ 1`, by mutex happens-before plus RMW coherence, until A's own decrement. Every other thread's decrement follows its own increment in the modification order, so it cannot cancel A's. A sweep that locked earlier ran before A's lookup, so A finds the post-sweep map. `Relaxed` is sufficient because the mutex supplies the ordering.
  - The pin adds no new state. `inflight` is already the sweep's "active" term and already the counter `QuotaGuard` releases.
- **Why the pin cannot sit under the lock together with the rate check.** Taking the bucket lock under the map lock (or holding `m` for the whole of `admit`) would close the window with zero new facts. But `admission.bucket.lock().unwrap()` would then panic while `m` is held. That poisons the map for **every** project, and it breaks the pinned poison-isolation test `quota::poison_tests::poisoned_request_bucket_cannot_refill_and_admit`, which asserts that a neighbour is still admitted. The bucket unwrap must therefore run after `m` drops, and so the pin must survive the unlock.
- **Typed contract: unchanged.**
  - `QuotaRefusal`, every wire code and body, and `WIRE-MATRIX.md:85` are unchanged.
  - Refusal precedence is unchanged: TrackerCapacity, then Rate, then Concurrency.
  - A concurrency-refused request still spends its rate token, as today.
  - Every single-threaded sequence of calls behaves identically. `prev` from the pin equals `prev` from today's post-rate `fetch_add` when nothing runs in between.
- **Behaviour change 1 (the fix).** While a request's `QuotaGuard` lives, `tracked(project)` returns that guard's entry. Three things become true again:
  - `admit_append`'s comment (649-650), "absence means the tracker refused it there";
  - `pressure_handle`'s doc (896-898);
  - MULTITENANCY.md:1408, "tracked ones are untouched".

  In the one interleaving where today's sweep makes room by evicting an entry mid-admission, the first-seen project now gets the existing retryable 503 `project_tracker_capacity`, provided nothing else is evictable. The room it got before was the bug.
- **Behaviour change 2 (inherent to the approved "increment under the lock, back out on refusal" form).** Today's optimistic window runs from `fetch_add` to `fetch_sub`. It now runs from the pin to the refusal: map unlock, recency store, bucket lock and refill. That is a sub-microsecond span.
  - Two kinds of in-window attempt can each hold one extra slot for that span: an attempt about to be rate-refused, and an attempt about to be concurrency-refused.
  - A same-project request racing at its own concurrency ceiling can therefore receive a spurious 429 `project_concurrency_limit`. At 100k rate-refused requests per second on one instance, about 0.01 doomed pins are live on average.
  - The ceiling still holds. Of any set of simultaneously admitted requests, the last to pin saw every earlier one, so the set is at most `max`.
  - Other projects are not affected.
  - `stats().1` and `/v1/debug/load` may include those transient pins.
- **Behaviour change 3 (only in an unreachable state).** If the request bucket is poisoned, the panicking `admit` skips the back-out, so its pin stays. The poisoned entry then never becomes evictable, where today it could be evicted and re-created with a fresh bucket. That re-creation is the "recovery could mint fresh request credit" the `admit` exception reason rejects.
  - Nothing can panic while that mutex is held. `bucket` is locked only at line 598, and `Bucket::take`/`refill` are panic-free arithmetic. So this state can only be reached by a test's artificial poisoning.
  - The poison tests stay green unchanged: the map is not poisoned, and the neighbour is still admitted.
- The eviction policy (the `retain` predicate, `IDLE_EVICT_MS`, `MAX_TRACKED_PROJECTS`) is untouched. Item 50 is separate, and the pin is compatible with its holder predicate: `pin_inflight` returns a clone minted under the lock, so the pinned entry has `strong_count ≥ 2`.

## 3. Red tests, pins and non-vacuity controls

All three tests go in `src/quota/tests.rs` (a `#![cfg(test)]` file).
- Imports: insert two lines after line 5, the `};` of `use super::{…}`. rustfmt keeps `super` first, as in `pressure_tests.rs:3-8`:
  ```rust
  use std::sync::Arc;
  use std::time::{Duration, Instant};
  ```
- Old line 307 becomes 309. The blocks below are appended verbatim, each preceded by one blank line. All three are already in rustfmt shape: chains are ≤ 60 columns and macro arguments ≤ 60, or already split.

### 3a. RED: `quota::tests::a_looked_up_entry_survives_a_sweep_before_its_charge` (lines 311-357)

```rust
/// External review §9: admit looked its entry up under the tracker
/// lock but pinned it (inflight) only after the lock dropped, so a
/// first-seen project's sweep in between evicted the entry the
/// request went on to charge: its append then refused
/// TrackerCapacity, and its other charges skipped enforcement. The
/// request parks on its own rate bucket after the lookup, with a
/// clock that trails the sweeper's by more than the idle horizon (a
/// clock step, or two callers reading now_ms apart), so recency
/// cannot hold the entry; only a pin taken under the lock can.
#[test]
fn a_looked_up_entry_survives_a_sweep_before_its_charge() {
    let r = QuotaRegistry::default();
    let t0: i64 = 1_000_000;
    for i in 0..MAX_TRACKED_PROJECTS {
        r.admit(&pid(&format!("idle_{i}")), &q(0, 0), t0).unwrap();
    }
    let parked = pid("idle_0");
    let held = r.tracked(&parked).unwrap();
    let late = t0 + IDLE_EVICT_MS + 2_000;
    std::thread::scope(|s| {
        // Held until the sweep has run: the request blocks on it
        // after its lookup and before its charge.
        let bucket = held.bucket.lock().unwrap();
        let request = s.spawn(|| r.admit(&parked, &q(1, 0), t0 + 1_000));
        let deadline = Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&held) < 3 {
            assert!(Instant::now() < deadline, "the request never looked up");
            std::thread::yield_now();
        }
        r.admit(&pid("first_seen"), &q(0, 0), late).unwrap();
        assert!(r.tracked(&pid("idle_1")).is_none(), "the sweep ran");
        let live = r.tracked(&parked);
        assert!(
            live.is_some_and(|e| Arc::ptr_eq(&e, &held)),
            "a sweep evicted the entry an admitted request was still charging"
        );
        drop(bucket);
        let guard = request.join().unwrap().unwrap();
        let appends = ProjectQuotas {
            append_bytes_per_sec: 1_000,
            ..Default::default()
        };
        assert!(r.admit_append(&parked, &appends, 1, 1, late).is_ok());
        assert_eq!(r.stats(), (2, 1), "idle_0 and first_seen; one request");
        drop(guard);
    });
}
```

**Why it is deterministic.**
- The handshake: `held` plus the map make `strong_count == 2`, because every fill guard was dropped. The count reaches 3 only when A's lookup clones the entry under `m`. If A still holds `m` at that moment, the test's sweep simply waits for it. A never waits on anything while it holds `m`.
- A then blocks on idle_0's bucket (`requests_per_sec = 1`), which the test holds.
- The bucket guard is a local of the scope closure. Any assertion panic therefore unwinds it, A finishes, and `scope` joins before re-raising. No path can hang, and the deadline bounds the wait loop at 10 s.

**Exact red on 24c4c77a, traced.**
1. A: lock `m`, `Some(a) => a.clone()` (count 3), unlock. It stamps `last_seen = t0+1_000` (line 588), then blocks on line 598.
2. Test: `admit(first_seen, late)` takes the None arm with `len == 16_384` and sweeps. For idle_0: inflight 0 (A has not reached line 615), live_subs 0, no pressure, and `late - (t0+1_000) = IDLE_EVICT_MS + 1_000`, which is not `< IDLE_EVICT_MS`. It is evicted, whether or not A's stamp has landed yet (with the stamp not yet written, the gap is `IDLE_EVICT_MS + 2_000`).
3. The other idle entries have a gap of `IDLE_EVICT_MS + 2_000`, so they are evicted too, and first_seen is admitted.
4. Line 341 passes: idle_1 is gone.
5. Line 343: `r.tracked(&parked)` is None, so the panic fires. Unwinding releases the bucket, A completes on the orphan, and scope joins.

```text
running 1 test
test quota::tests::a_looked_up_entry_survives_a_sweep_before_its_charge ... FAILED

failures:

---- quota::tests::a_looked_up_entry_survives_a_sweep_before_its_charge stdout ----

thread 'quota::tests::a_looked_up_entry_survives_a_sweep_before_its_charge' panicked at src/quota/tests.rs:343:9:
a sweep evicted the entry an admitted request was still charging
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    quota::tests::a_looked_up_entry_survives_a_sweep_before_its_charge

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; <N> filtered out; finished in <t>s
```

**Green trace.**
1. A's `pin_inflight` makes inflight 1 under `m`, so the sweep retains idle_0 and evicts every other entry.
2. `live` is `ptr_eq` to `held`.
3. After the bucket is released, A's `take(1.0, 1.0, t0+1_000)` refills idle_0's `Bucket::full(0, t0)` to 1.0. The take succeeds, and `max_inflight` of 0 skips the concurrency check, so A gets `Ok(guard)`.
4. `admit_append` finds the entry. The bytes bucket is `full(0, t0)` refilled over 302 s and capped at 1,000, so a cost of 1 succeeds.
5. `stats()` is `(2, 1)`: idle_0 and first_seen, with A's slot.

**Built-in non-vacuity controls.**
- The handshake proves A had looked the entry up before the sweep.
- The line 341 assertion proves the sweep ran past the horizon.
- The red on HEAD proves the window is really exercised.

### 3b. Pin: `quota::tests::a_refused_admit_gives_its_pin_back` (lines 359-381)

Green on both trees. It guards the new rate-refusal back-out. The concurrency back-out is already guarded by `concurrency_releases_with_the_guard`.

```rust
/// The lookup's pin is the request's inflight slot: a rate refusal
/// gives it back (a kept slot would refuse the next request on
/// concurrency), and so does a concurrency refusal.
#[test]
fn a_refused_admit_gives_its_pin_back() {
    let r = QuotaRegistry::default();
    let p = pid("proj_pin");
    let quotas = q(1, 1);
    r.admit(&p, &quotas, 1_000).unwrap();
    assert!(matches!(
        r.admit(&p, &quotas, 1_000),
        Err(QuotaRefusal::Rate { .. })
    ));
    let held = r
        .admit(&p, &quotas, 2_000)
        .expect("the rate-refused attempt gave its slot back");
    assert!(matches!(
        r.admit(&p, &quotas, 3_000),
        Err(QuotaRefusal::Concurrency)
    ));
    drop(held);
    assert_eq!(r.stats(), (1, 0), "every refusal gave its slot back");
}
```

Trace on the fixed tree:
1. At 1,000: new entry with `Bucket::full(1)`, prev 0; the token is taken and the request admitted. Its guard drops, so inflight is 0.
2. At 1,000 again: the pin makes inflight 1, the bucket is empty, and the request is rate-refused. The back-out returns inflight to 0.
3. At 2,000: the bucket has refilled 1 token, prev 0, so the request is admitted (`held`).
4. At 3,000: prev 1, the token is taken, and the request is concurrency-refused. The back-out leaves inflight at 1.
5. After `drop(held)`: `(1, 0)`.

**Control C4** (local only, not committed): with the rate back-out line deleted, step 3 sees prev 1 and panics at `src/quota/tests.rs:374:10` with `the rate-refused attempt gave its slot back: Concurrency`.

### 3c. Property: `quota::tests::quality_quota_inflight_is_exactly_the_held_guards` (lines 383-416)

`docs/RUST-QUALITY.md` requires a property with at least 1,024 cases for each affected property when admission changes. The property affected here is inflight accounting. The `quality_` prefix puts it in the `--lib quality_` leg.

```rust
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig {
        cases: 1024,
        ..proptest::prelude::ProptestConfig::default()
    })]

    /// Every admission attempt pins a slot and every refusal gives it
    /// back, whatever the quota mix (0 = unlimited) and however the
    /// clock steps: after each step the project's inflight is exactly
    /// the guards still held.
    #[test]
    fn quality_quota_inflight_is_exactly_the_held_guards(
        steps in proptest::collection::vec(
            (0u64..3, 0u64..3, -1_500i64..1_500, proptest::bool::ANY),
            1..64,
        ),
    ) {
        let r = QuotaRegistry::default();
        let p = pid("prop");
        let mut now = 1_000_000;
        let mut held = Vec::new();
        for (rps, max_inflight, step, release) in steps {
            now += step;
            if release {
                drop(held.pop());
            }
            if let Ok(guard) = r.admit(&p, &q(rps, max_inflight), now) {
                held.push(guard);
            }
            let expected = u64::try_from(held.len()).unwrap();
            proptest::prop_assert_eq!(r.stats(), (1, expected));
        }
    }
}
```

- rustfmt does not reformat `proptest!` bodies (see `queue.rs:552-553`), so this block is hand-shaped.
- The inputs cover zero quotas (unlimited), clock reversal (negative steps) and alternating quota policy. It is green on both trees.
- **Control C4** also fails this property: `prop_assert_eq` gives left `(1, 2)` and right `(1, 1)`, and the case shrinks to two rate-1 steps.
- Tracker-capacity and sweep states stay in 3a. Reaching them would need a 16,384-entry fill per case.

## 4. Edits, file by file (one commit)

Order of work:
1. Apply the `src/quota/tests.rs` hunk and capture the red (C1) and the pins (C2) with `src/quota.rs` still at HEAD.
2. Apply the `src/quota.rs` hunk and capture the green.
3. Add the `owners.json` rows.
4. Commit once; slate commits must each be green.

Suggested subject: *"An admitted request pins its tracker entry before the map lock drops, so no sweep evicts it mid-admission"*, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. The body should say that the review's seam was not needed (§3a) and name the contract notes from §2.

| File | Lines now | Limit | After |
|---|---|---|---|
| `src/quota.rs` | 975 | ≤ 1,000 (crossing is blocked) | 984 |
| `src/quota/tests.rs` | 307 | ≤ 1,000 | 416 |
| `docs/quality/owners.json` | n/a | n/a | +3 rows |
| `http.rs` 3,153 · `product.rs` 4,205 · `shard.rs` 3,139 · `billing.rs` 2,151 · `history.rs` 1,656 · `auth.rs` 1,637 · `registry.rs` 1,452 · `sse/feed.rs` 1,165 · `fleet.rs` 1,142 | unchanged | ceilings | not touched |

### 4.1 `src/quota/tests.rs`

Add the two imports (§3) and append §3a, §3b and §3c. The file has no `#[expect]`/`#[allow]`, so there is no ratcheted scope. Nesting is at most 3 (fn body, then scope closure, then `while`), and each function is under 50 lines.

### 4.2 `src/quota.rs`

**(a) New owner method.** Insert after `has_pressure` (after line 158, inside `impl ProjectAdmission` at 135-252, which carries no exception):

```rust

    /// One admission attempt's inflight slot, taken while the caller
    /// holds the tracker lock. The eviction sweep reads `inflight`
    /// under that same lock, so once a lookup returns, no sweep can
    /// evict its entry, whatever either request's clock says (Relaxed
    /// suffices: the lock orders this increment before the sweep's
    /// read). Returns the entry and the inflight count before this
    /// attempt; `QuotaRegistry::admit` gives the slot back on refusal,
    /// and `QuotaGuard`'s drop releases it otherwise.
    fn pin_inflight(self: &Arc<Self>) -> (Arc<Self>, u64) {
        (Arc::clone(self), self.inflight.fetch_add(1, Ordering::Relaxed))
    }
```

This adds 12 lines.
- `self: &Arc<Self>` is a stable receiver. `m.get(project)` yields `&Arc<_>`, and the new `Arc<_>` in the None arm autorefs.
- The name avoids confusion with the associated function `Arc::pin`.
- It becomes the only `inflight` increment in the crate.

**(b) `QuotaRegistry::admit`.** The doc and both `#[expect]`s are untouched, and so is the whole None arm apart from its tail.

```diff
     ) -> Result<QuotaGuard, QuotaRefusal> {
-        let admission = {
+        // Both arms pin the entry (ProjectAdmission::pin_inflight)
+        // before the map lock drops; every refusal gives the slot back.
+        let (admission, prev) = {
             let mut m = self.projects.lock().unwrap();
             match m.get(project) {
-                Some(a) => a.clone(),
+                Some(a) => a.pin_inflight(),
                 None => {
 ...
                     m.insert(project.clone(), a.clone());
-                    a
+                    a.pin_inflight()
                 }
             }
         };
 ...
                     .take(quotas.requests_per_sec as f64, 1.0, now_ms)
         {
+            admission.inflight.fetch_sub(1, Ordering::Relaxed);
             return Err(QuotaRefusal::Rate { retry_after_secs });
         }
 
-        if quotas.max_inflight_requests > 0 {
-            // Optimistic acquire; back out on overshoot. Relaxed is
-            // fine: this is a backstop counter, not a synchronization
-            // edge.
-            let prev = admission.inflight.fetch_add(1, Ordering::Relaxed);
-            if prev >= quotas.max_inflight_requests {
-                admission.inflight.fetch_sub(1, Ordering::Relaxed);
-                return Err(QuotaRefusal::Concurrency);
-            }
-        } else {
-            admission.inflight.fetch_add(1, Ordering::Relaxed);
-        }
+        // Optimistic, as before: the pin already holds this request's
+        // slot, so it is refused when the slots before it were full.
+        if quotas.max_inflight_requests > 0 && prev >= quotas.max_inflight_requests {
+            admission.inflight.fetch_sub(1, Ordering::Relaxed);
+            return Err(QuotaRefusal::Concurrency);
+        }
         Ok(QuotaGuard { admission })
```

The new `if` is 85 columns, so rustfmt keeps it on one line. Net change: +2, +1, −6, so −3 lines.

**The ratcheted scope touched is only `crate::QuotaRegistry::admit`.** It carries `#[expect(clippy::unwrap_used)]`, which is fingerprinted, and `#[expect(clippy::excessive_nesting)]`, and they share one scope: the `ImplItemFn` span from its doc at line 513 to line 618. Neither reason text changes, and no metric grows. By `source_rules.exception_contracts` (`scripts/quality/source_rules.py:115-194`):

| Metric | Before | After | Why |
|---|---|---|---|
| `scope_lines` | 106 | 103 | −3 lines; the `///` doc is untouched (each doc line is an `attribute` fact plus a `doc` path) |
| `nested_items` | 1 | 1 | no new items |
| `syntax_facts` | N | N−2 | inflight block 18 → 10 (−8); rate back-out +4; None-arm `a` → `a.pin_inflight()` +2; `a.clone()` → `a.pin_inflight()` ±0 |
| `unwrap_sites` and `unwrap_site:*` | 2 | 2 | both unwrap sites are textually identical: `self . projects . lock () . unwrap ()` and `admission . bucket . lock () . unwrap ()` (the local is still named `admission`) |
| `unwrap_site:ordinary-call:*` | set S | S | no `ExprCall` is added or changed; `Err (QuotaRefusal :: Concurrency)`, `Err (QuotaRefusal :: Rate {…})` and `Ok (QuotaGuard { admission })` keep their tokens |
| `unwrap_site:path:` `admission` | 6 | 5 | uses: store, bucket, rate back-out, concurrency back-out, guard literal |
| `unwrap_site:path:` `std::sync::atomic::Ordering::Relaxed` | 7 | 6 | retain ×3, store, two back-outs |
| `unwrap_site:path:` `a` / `prev` / `quotas` / `Err` / `now_ms` | 7 / 1 / 8 / 4 / 8 | same | a swapped method name, not a new receiver; the `let (admission, prev)` tuple pattern has no path facts |

**Why an inline pin was rejected.** Written as `let prev = admission.inflight.fetch_add(..); (admission, prev)` inside the block, the pin puts `admission` at 7 uses (limit 6) and `prev` at 2 (limit 1), so the ratchet fails. A method call adds only unfingerprinted method facts.

**Both expects stay fulfilled.** The two `unwrap`s remain. The nesting is unchanged: impl, then fn body, then the `let (admission, prev) = {` block, then the `None => {` arm, then the `if m.len() >= …` block at level 5, which is over 4.

**Other scopes.** `pin_inflight` sits in an `impl` block with no exception. No other `#[expect]` function changes, and neither do the `macro-dsl` `serde_json::json` rows in `source-allowances.json`, which are keyed by owner function (`memory_gate`, `memory_pressure_json`, `pressure_model_json`).

### 4.3 `docs/quality/owners.json`

Append 3 rows to `occurrences` (the file is not sorted):

```json
{"category": "effect", "count": 1, "owner": "crate::a_looked_up_entry_survives_a_sweep_before_its_charge", "path": "src/quota/tests.rs", "reason": "Tracker-race regression owns one scoped thread; the scope joins it before the test returns, and a panicking assertion unwinds the bucket lock it parks on first; a real second caller of the production admit opens the lookup-to-charge window.", "syntax": "std::thread::scope"},
{"category": "effect", "count": 1, "owner": "crate::a_looked_up_entry_survives_a_sweep_before_its_charge", "path": "src/quota/tests.rs", "reason": "Tracker-race regression yields while it waits, under a ten-second deadline, for the scoped request thread to take its entry; the thread is joined before the test returns.", "syntax": "std::thread::yield_now"},
{"category": "macro-dsl", "count": 1, "owner": "crate::macro(proptest::proptest)", "path": "src/quota/tests.rs", "reason": "Quota admission accounting property; the pinned macro generates 1,024 quota-mix and clock-step sequences against the production QuotaRegistry::admit; all generated cases execute in the quality_ leg.", "syntax": "proptest::proptest"}
```

- The owner strings follow the scanner. Qualified names in a file module start at `crate::`, as in the `crate::contend` row for `src/auth/publication/tests.rs`.
- A top-level `proptest::proptest!` is owned by `crate::macro(proptest::proptest)` (`tools/quality-syntax/src/scan.rs:90-99`).
- `std::thread::*` paths classify as `effect` (`source_rules.py:44-46`).
- Clippy's `disallowed-methods` does not list `std::thread::scope`, `Scope::spawn` or `yield_now`.

## 5. Mutation analysis

- **Owner:** the existing row `owner('quota_registry', 'src/quota.rs', 'quota::')` at `scripts/quality/mutation_owners.py:106`. The driver uses `--profile quality --jobs 1 --timeout 90`.
- `src/quota/tests.rs` is `#![cfg(test)]`, so it is `production_unchanged` and not mutated. It needs no row.
- The `--in-diff` mutants on changed `src/quota.rs` lines follow the operator set confirmed from earlier runs: `>`→`==`,`<`,`>=`; `>=`→`<`; `&&`→`||`. `Arc<Self>` renders as `Arc::new(Default::default())`.
- **Expected run: 8 mutants. 5 caught, 3 unviable, 0 missed, 0 timeout.**

| # | Mutant | Killer(s), all under `quota::` |
|---|---|---|
| M1 | `max_inflight_requests > 0` → `== 0` | max>0 is never refused, so `concurrency_releases_with_the_guard` fails (third admit expected `Concurrency`). max=0 is always refused, so `zero_rate_is_unlimited_and_projects_never_share_buckets` fails (`is_ok()`). |
| M2 | `> 0` → `< 0` (always false on u64) | `concurrency_releases_with_the_guard`; `tracker_capacity_churn_evicts_idle_never_active` ("pinned project was evicted (fresh entry admitted)"); §3b (admit at 3,000 expected `Concurrency`) |
| M3 | `> 0` → `>= 0` | max=0 gives `prev >= 0`, always refused: `rate_bucket_admits_burst_then_refuses_then_refills`, `zero_rate_…`, and every fill loop's `unwrap`/`expect` |
| M4 | `&&` → `\|\|` | same as M3 (max=0 gives `prev >= 0`) |
| M5 | `prev >= max` → `prev < max` | `concurrency_releases_with_the_guard` (first `unwrap()` gets `Err(Concurrency)`); `a_refused_admit_gives_its_pin_back` |
| U1 | `admit` → `Ok(Default::default())` (FnValue; its span overlaps the edits) | Unviable: `QuotaGuard` has no `Default` |
| U2, U3 | `pin_inflight` → `(Arc::new(Default::default()), 0)` / `(…, 1)` | Unviable: `ProjectAdmission` has no `Default` (as with `LineageSource::build` in mutants-r16) |

- The two back-out statements, the `pin_inflight` call sites and the tuple binding carry no operators. cargo-mutants 27.1 does not delete statements. The match has no wildcard arm, so there are no arm-deletion mutants.
- There are no equivalent mutants.
- The back-outs are still covered as behaviour: §3b and §3c for rate, `concurrency_releases_with_the_guard` for concurrency.

**Boundedness under every mutant**, including the nightly full-owner rotation:
- §3a: under M1, M3 and M4, the fill `unwrap()` panics at i = 0, before any thread starts.
- §3a otherwise: the handshake loop is bounded by its 10 s deadline. Unwinding always releases the bucket, and the scope joins. A never waits while holding `m`.
- §3c: at most 1,024 × 63 admits.
- The whole `quota::` set (38 tests) runs far inside 90 s at opt-level 1.

## 6. Ledgers

- `docs/quality/owners.json`: +3 rows (§4.3).
- `docs/quality/source-allowances.json`: unchanged. It is the legacy ceiling and must not grow; new rows belong in `owners.json`.
- `docs/refactor/test-inventory.json`: unchanged. `scripts/test-inventory.py` inventories DST tests only.
- `docs/refactor/review-mechanisms.json`: unchanged. There is no quota pin; its only quota mention is a rig reason.
- `docs/refactor/architecture-policy.json`: unchanged (no new file, no transport import).
- `docs/refactor/WIRE-MATRIX.md`: unchanged. No wire shape or code changes; `project_tracker_capacity` and `project_concurrency_limit` are already at line 85.
- `scripts/quality/mutation_owners.py`: unchanged (no new critical file).
- `docs/quality/policy.json` immutable hashes: these cover the legacy files and `syntax-fragments.json` only, so nothing touched here is pinned.
- `docs/MULTITENANCY.md`: unchanged. Its line 1408 claim becomes true again, and there is no sizing or knob change.

## 7. Controls (exact commands, expected outputs)

Run from `/Users/sorenschmidt/code/streams`. The gate needs Python 3.11 or later:

```bash
export PATH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/pybin:$PATH
```

1. **C1, red:** only the tests hunk applied, `src/quota.rs` at 24c4c77a.
   ```bash
   cargo test --locked --lib quota::tests::a_looked_up_entry_survives_a_sweep_before_its_charge -- --exact
   ```
   Expected: exactly the §3a red block, with the panic at `src/quota/tests.rs:343:9`, then `test result: FAILED. 0 passed; 1 failed`. Repeat 20× (`for i in $(seq 20); do …; done`): red every time.
2. **C2, pins on the old tree:** `… quota::tests::a_refused_admit_gives_its_pin_back -- --exact` and `… quota::tests::quality_quota_inflight_is_exactly_the_held_guards -- --exact`. Expected: `test result: ok. 1 passed` each.
3. **C3, clock-step control** (not committed): on HEAD's `quota.rs`, change only line 534 to `Some(a) => { a.last_seen_ms.store(now_ms, Ordering::Relaxed); a.clone() }`. Run C1: it fails identically at 343:9, which shows the stamp form is not safe against clock steps. Then `git checkout src/quota.rs`.
4. **Green:** apply §4.2. C1 gives `... ok` / `1 passed`; repeat 50× with 0 failures. Both C2 tests pass.
5. **C4, back-out control** (not committed): delete the rate-arm `admission.inflight.fetch_sub(1, Ordering::Relaxed);`.
   - §3b panics at `src/quota/tests.rs:374:10` with `the rate-refused attempt gave its slot back: Concurrency`.
   - §3c fails with a `prop_assert_eq` mismatch: left `(1, 2)`, right `(1, 1)`.
   - Restore the line.
6. `cargo test --locked --lib quota::` should give `test result: ok. 38 passed` (35 before, +3). This includes the unchanged `poison_tests` (5), the churn and eviction tests, and `quota::bucket::tests` (5).
7. `cargo test --locked --release --lib quality_quota` should give 3 passed: 2 bucket properties plus §3c.
8. `cargo fmt --all -- --check` should print nothing. In particular it must not re-wrap §3a or §3b, since that would move lines 343 and 374.
9. `cargo clippy --locked --workspace --all-targets -- -D warnings` should be clean. There must be no `unfulfilled_lint_expectations` on `QuotaRegistry::admit`, no `excessive_nesting` in the new tests, and no `disallowed_methods`.
10. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` should succeed (the `pin_inflight` doc uses code spans only).
11. **Ratchet view** (after `cargo build --locked -p streams-quality-syntax`):
    ```bash
    python3 - <<'EOF'
    import subprocess, sys
    sys.path.insert(0, 'scripts/quality')
    from common import syntax
    import source_rules as r
    def admit(src):
        c = r.exception_contracts({'src/quota.rs': src}, syntax({'src/quota.rs': src}))
        return {k[3].split(',')[0]: v for k, v in c.items() if k[1] == 'crate::QuotaRegistry::admit'}
    old = admit(subprocess.check_output(['git', 'show', '24c4c77a:src/quota.rs'], text=True))
    new = admit(open('src/quota.rs').read())
    for k in old:
        grew = {m: (old[k].get(m, 0), n) for m, n in new[k].items() if n > old[k].get(m, 0)}
        print(k, old[k]['scope_lines'], '->', new[k]['scope_lines'], old[k]['syntax_facts'], '->', new[k]['syntax_facts'], 'grew', grew)
    EOF
    ```
    Expected: two lines, `106 -> 103`, `syntax_facts` down by 2, and `grew {}` on both.
12. `scripts/quality.sh` should exit 0. It must print no `file growth: src/quota.rs`, no `accepted exception grew without a new decision: ('src/quota.rs', 'crate::QuotaRegistry::admit', …`, and no `unregistered source occurrence` for `src/quota/tests.rs`. `python3 scripts/test-inventory.py --check` should be clean.
13. **CI's plan, after committing:**
    ```bash
    QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=24c4c77a QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan
    ```
    Expected `plan.json`:
    - `mutants: true`
    - `mutation_source_files: ["src/quota.rs"]`
    - `selected_mutation_owners: ["quota_registry"]`
    - `unregistered_mutation_source_files: []`
    - `production_unchanged_files` containing `src/quota/tests.rs`
    - `properties_fuzz: true` (quota prefix: corpus replay)
    - `miri: false`
14. **Mutants:**
    ```bash
    QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=24c4c77a QUALITY_BASE_REF=origin/slate QUALITY_MUTANTS_OUT=target/q17-mutants scripts/quality/mutations.sh
    ```
    Expected: the §5 set, with 5 caught, 3 unviable, 0 missed and 0 timeout. Do not edit the tree while the gate runs.
15. The full suite as CI runs it (`cargo test --locked --release`). No DST test races admits at the concurrency ceiling at sub-microsecond granularity: `watch_admission.rs:29` and `watch_observation.rs:609` are separated by 300 ms and end with `stats().1 == 0`.
16. After pushing, confirm each workflow with `gh run list --branch slate` / `gh run view`. Never report green from memory.

## 8. Out of scope

- Item 50, the holder-based `retain` predicate, and the cap and handle-horizon sizing (owner decision 7 is pending). The `retain` closure (543-551), `IDLE_EVICT_MS` and `MAX_TRACKED_PROJECTS` are untouched. The pin stays correct under item 50's predicate.
- Moving the `last_seen` stamp. It stays after the lock; recency no longer protects in-flight admissions. Moving it into `pin_inflight(now_ms)` would grow `admit`'s `now_ms` path fingerprint from 8 to 9.
- **A `lookup_or_track` / `charge` seam.** It is not needed (§3a), and it would need three new reasoned exceptions (see the note at the top).
- **A Loom model.** Loom would need a `cfg(loom)` facade over `quota.rs`'s `std::sync::Mutex` and atomics. That is a new pattern: the existing Loom tests wrap owner-free state machines in `loom::sync::Mutex`, and the policy says a rewritten model does not certify. `verification_plan.py` has no Loom selection for `src/quota`. The deterministic scoped-thread test exercises the real code at the lookup/pin boundary.
- Callers that use `pressure_handle` outside a request guard's lifetime, for example SSE feed binding. That is item 50's domain.
- `admit_append` treating absence as `TrackerCapacity`. That mapping is correct again for requests holding a guard.
- Recording the pin leak on a poisoned bucket (§2, change 3) in the poison tests; the plan pins nothing about it.
- Doc edits in `MULTITENANCY.md` and `bench/WORKLOAD-CERT-PLAN.md`.

## 9. Decisions for the owner

None. The fix is the reviewers' adopted "inflight increment under the lock with back-out on refusal". Its transient-slot consequence (§2, change 2) is inherent to that form and does not change any wire code, precedence or ceiling. The poisoned-bucket consequence (§2, change 3) can only be reached through a test's artificial poisoning.

## Skeptic corrections (C1..C8)

Checked against the tree at 24c4c77a, read-only. These all hold: every quote from `src/quota.rs` (531-617, 540-551, 645-652, 891-901, 958-962), the caller table, the `admit` scope span (513-618, 106 lines), the fingerprint counts before and after (`admission` 6→5, `Ordering::Relaxed` 7→6, `a` 7, `prev` 1, `quotas` 8, `Err` 4, `now_ms` 8, `syntax_facts` N−2, `scope_lines` 106→103), the absence of any exception on `impl ProjectAdmission` (135-252), the trace for §3b and its C4 location `374:10`, the 35→38 and quality_quota 3 counts, the owner-string forms (`crate::<fn>` and `crate::macro(proptest::proptest)`), the classification of `std::thread::*` as `effect`, `tests.rs` being `production_unchanged` (`production_changes.py:66-67`, `test_only_file`), and the mutant set (5 caught, 3 unviable). Killers exist for M1-M5. Nothing touches a reason text and no ratchet metric grows.

**C1 — The tree is not clean. The plan header says "clean"; it is not.** `git status` shows uncommitted `docs/quality/owners.json`, `src/tasks/refusal.rs` and `src/tasks/tests.rs`: someone else's F-G work, whose owners.json hunk rewrites the rows around line 659. Before step 1 of §4:
- run on a clean worktree at 24c4c77a, or wait for that work to land;
- stage only this plan's three owners.json rows (`git add -p`). They must be pretty-printed like the rest of the file (multi-line objects, keys `category, count, owner, path, reason, syntax`), not the one-line form in §4.3.

Controls 12-14 (`scripts/quality.sh`, the plan, mutants) read the working tree. With the tasks WIP present they judge that work too. `plans17/append-413.md:583` also appends to owners.json, so whichever lands second must rebase its rows.

**C2 — rustfmt re-blocks `pin_inflight`, so control 8 fails as written.** A tuple is formatted under the function-call rule (`fn_call_width` = 60). The items `Arc::clone(self), self.inflight.fetch_add(1, Ordering::Relaxed)` are 63 columns, and a 2-item tuple cannot overflow its last item. So `cargo fmt` rewrites it as a 4-line vertical tuple. Either:
- commit it that way (+15 lines, `src/quota.rs` 975→987); or, preferred,
- write `let prev = self.inflight.fetch_add(1, Ordering::Relaxed);` followed by `(Arc::clone(self), prev)` (+13 lines, 985).

Both stay under the 1,000 ceiling. The body sits outside every ratcheted scope. Update the §4 table and "This adds 12 lines" to match.

**C3 — The §3a red is not "exact"; there is a second panic.** The red trace (§3a step 5, and C3's "fails identically"):
1. The main thread panics at `tests.rs:343:9`.
2. Unwinding drops `bucket`, a `MutexGuard` created before the panic, so the idle_0 request bucket is **poisoned**. It is not merely released.
3. Thread A then gets `Err(PoisonError)` at `src/quota.rs:596-600` and panics in its own `.unwrap()`, at about `src/quota.rs:599:22`. It does not "complete on the orphan".
4. Scoped threads inherit the test's output capture, so the failure stdout also contains `thread '<unnamed>' panicked at src/quota.rs:599:22:` / ``called `Result::unwrap()` on an `Err` value: PoisonError { .. }`` after the RUST_BACKTRACE note.
5. `scope` then resumes the main panic, so the result is still `1 failed`.

Fix either way:
- (a) add that block to the expected red and to C3's expectation, and say "A panics on the poisoned bucket" in §3a/§5 and in the owners.json `std::thread::scope` reason; or
- (b) take the ptr_eq verdict into a `bool` before `drop(bucket)`, join, and only then `assert!`. That gives one panic, and A completes on the orphan as described. It moves the lines, so trace `343:9` again.

The test stays bounded either way.

**C4 — The red covers the lookup→charge window under a clock gap, not the reviewers' pre-stamp window. Say so.** The park point (the bucket lock at 598) is *after* the recency stamp at 588. With equal clocks, A's stamp usually lands before the test's sweep, and HEAD would pass nondeterministically. §3a is red on HEAD only because the sweeper's clock leads A's by 301 s.
- The window the reviewers describe (`tracker-race-confirm`: 586→588, A not yet stamped, same clock) has no deterministic red without a seam. The pin closes it structurally, because it is taken under the lock.
- Replace "parks a real admit exactly inside the window" (header item 1) with that precise scope, and record the pre-stamp window as closed by construction, with no red of its own.
- This is acceptable: the pin is one mechanism that closes both windows. But the claim must match the trace.

**C5 — §9 "None" is wrong. There are two owner decisions.**
- **(a) Loom.** `docs/RUST-QUALITY.md:161` requires, for "Synchronization or retirement changes", that "Loom exercises the actual small state-transition implementation through instrumented primitives". Moving the `inflight` increment under the map mutex to close a race is a synchronization change. The program's own decision record treats one this way (`report/decisions-final.json` ITEM-50 option (c): "a synchronization change … that selects Loom").
  - §8 declines Loom on cost. The owner has to accept that deviation, or the plan builds a `cfg(loom)` facade; it cannot be marked out of scope unilaterally.
  - The deterministic scoped-thread test is a reasonable substitute to propose, not a certification.
- **(b) Behaviour change 2 is an edge change the owner has not approved.** The "approved 'increment under the lock, back out on refusal' form" comes from the internal confirm pass (`external-claims.json` `tracker-race-confirm.fix`). It is not an owner approval: the memory record of the adopted review only says item 50 "never evict live ownership / accounting / debt".
  - Under the rule "any OTHER edge change is a decision for the owner", the newly possible 429 `project_concurrency_limit` has to be listed. That 429 goes to a within-rate request whose rate token is already spent.
  - "Sub-microsecond" also understates the window. The pin spans the wait on the **same project's** `bucket` mutex (598), so under same-project contention up to one doomed pin per concurrently executing `admit` call can be live. That is bounded by the executor's worker threads, not by 100 ns.
  - It is still project-scoped, retryable, and only reachable while the project is rate-limited and at its concurrency ceiling. There are no SDK, docs or test consumers of the code (`grep -r project_concurrency_limit` finds only product.rs, quota.rs, WIRE-MATRIX, MULTITENANCY).
  - Present an alternative with no wire change, so the owner can choose: a separate eviction pin. Add an `admitting: AtomicU64` field. An RAII pin increments it under the lock and drops at the end of `admit`, the retain predicate keeps entries with `admitting > 0`, and `inflight` keeps today's post-rate logic.
  - To keep that alternative ratchet-neutral, move the `ProjectAdmission { … }` constructor out of `admit` (a new `AtomicU64::new(0)` call site inside the scope would otherwise grow `unwrap_site:ordinary-call`) and move the retain predicate into a method. Both moves *shrink* `admit`, so no exception grows.
  - Recommendation: the plan's form, with an explicit owner acknowledgement of change 2. It is smaller, and the effect falls only on a project that is already being throttled.

**C6 — The use-site list is incomplete.** §1 "Callers of `admit`" misses `src/dst/tests/quota_enforcement.rs:849`, a DST test (`let _ = state.quotas.admit(&tenant, &q, now_ms())`). It drops its guard at once and is unaffected, but the list claims completeness.

**C7 — Sequencing with item 50 and with the governance plan.**
- **(a) Item 50.** The refute pass's fix is to keep an entry while `strong_count > 1` (`plans7/pressure-binding-eviction.md`). Once that lands, §3a no longer discriminates: the test's own `held` Arc pins idle_0, so it passes with or without the pin.
  - Note that in the plan, so item 50's plan re-checks §3a and does not treat it as guarding the pin.
  - Item 50's plan measured `admit`'s facts and lines on HEAD, so those counts shift by −3 lines and −2 facts after this commit. Its plan must be re-traced.
- **(b) The governance plan (`plans17/ratchet-governance.md`)** is adopted to land first. It changes the exception identity to `(path, qualified, kind, lint)` and the failure text to "accepted exception grew without an approved growth row".
  - Control 12's negative check must match both messages.
  - Control 11's `k[3].split(',')[0]` works under either identity, but it then yields one row per lint (still 2).

**C8 — Minor precision.**
- Behaviour change 3's cause is right, but note that §3a's own red path is what poisons a bucket in tests (C3). The poison tests stay green.
- Relatedly, `docs/RUST-QUALITY.md:25` says admission owners must enable `indexing_slicing` and `arithmetic_side_effects`, and `src/quota.rs` does not. That predates this plan and is out of scope, but should be named in §8 rather than left silent, since this commit edits that owner.

**Verdict: ready-with-corrections.** The diagnosis, the fix mechanics, the ratchet accounting and the mutant analysis all hold against the tree. Before implementing:
- apply C2 (fmt) and C3 (red text) to the controls;
- move C5's two items into §9 as owner decisions;
- do C1's worktree isolation.
