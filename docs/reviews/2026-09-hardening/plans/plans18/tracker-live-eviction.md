# Item 50: tracker eviction never evicts live ownership, accounting or debt, and tracking stays bounded

Tree: `slate`. The task named HEAD `d4d631df`. While this plan was written, local HEAD moved to `d255ad6d` (`1d079e01` + `d255ad6d`). Both commits are docs and `scripts/platform-e2e*.mjs` only (`git diff --stat d4d631df..HEAD -- src scripts/quality docs/quality docs/refactor` is empty). Every line number below was read on this tree and holds for `d4d631df`. `origin/slate` = `1d079e01`.

This plan is read-only: nothing below has been run.

Inputs:
- the prior plan `plans7/pressure-binding-eviction.md` and its Skeptic corrections C1-C7;
- the tracker-race fix `d4d631df` (`src/quota/pin.rs`);
- owner position 50: bounded tracking, but never evict live ownership, accounting or debt.

## 1. Problem (verified on the current tree)

### 1.1 The sweep keeps an entry by counters, not by holders

The only removal from the tracker is the sweep in the untracked-project arm of `QuotaRegistry::admit`, under the map lock (`src/quota.rs:575-586`):

```rust
                    if m.len() >= MAX_TRACKED_PROJECTS {
                        // Review item 5: EVICT idle entries before
                        // refusing — a tracker that filled once must
                        // not refuse project 1,025 until restart.
                        m.retain(|_, a| a.in_use(now_ms));
```

The predicate is `src/quota.rs:151-173`:

```rust
    fn has_pressure(&self) -> bool {
        self.live_feeds.load(Ordering::Relaxed) > 0
            || self.retained_sse_bytes.load(Ordering::Relaxed) > 0
            || self.buffered_body_bytes.load(Ordering::Relaxed) > 0
            || self.queued_bytes.load(Ordering::Relaxed) > 0
            || self.unabsorbed_frame_bytes.load(Ordering::Relaxed) > 0
            || self.dirty_streams.load(Ordering::Relaxed) > 0
    }
    ...
    fn in_use(&self, now_ms: i64) -> bool {
        self.counters.active()
            || self.live_subs.load(Ordering::Relaxed) > 0
            || self.has_pressure()
            || now_ms - self.last_seen_ms.load(Ordering::Relaxed) < IDLE_EVICT_MS
    }
```

`counters.active()` (`src/quota/pin.rs:72-74`) is `self.admitting.load(Ordering::Relaxed) > 0`. Only `ProjectAdmission::pin` raises it (`pin.rs:83-86`, under the lock in `admit`), and `AdmissionPin::drop` lowers it (`pin.rs:97-101`). No other holder is counted.

### 1.2 A stream's pressure binding holds the entry while every counter reads zero

`StreamPressureBinding` owns `admission: Arc<ProjectAdmission>` (`quota.rs:331-334`).

- `bind(adm, 0)` touches no counter: `if seed_unabsorbed > 0 { ... }` (`quota.rs:337-348`).
- `frames_retired` down to zero subtracts the last bytes and the dirty-stream count (`quota.rs:367-390`).

From then on the binding still holds the Arc, but no term of `in_use` is true once the project is idle.

### 1.3 The handle keeps that binding, and never rebinds

`src/shard.rs:665`:

```rust
    pub pressure: std::sync::OnceLock<std::sync::Arc<crate::quota::StreamPressureBinding>>,
```

`src/shard.rs:678-691`:

```rust
    pub(crate) fn bind_pressure(&self, adm: std::sync::Arc<crate::quota::ProjectAdmission>) {
        if self.pressure.get().is_some() {
            return;
        }
        let st = self.state.lock().unwrap();
        let _ = self.pressure.get_or_init(|| { ... StreamPressureBinding::bind(adm, st.applied.unabsorbed_bytes) ... });
```

Every append calls it with the tracker's current entry (`src/application/append/submit.rs:30-34`):

```rust
    if let Some(adm) = state.quotas.pressure_handle(sref.project_id())
        && let Ok(h) = engine.stream_handle(hash).await
    {
        h.bind_pressure(adm);
    }
```

The committer charges whatever binding the handle holds (`src/shard/transaction/publish.rs:45-47`):

```rust
            if let Some(b) = local.handle.pressure.get() {
                b.frames_added(local.frames.added_bytes);
                b.frames_retired(local.frames.retired_bytes);
```

The write gate reads the current map entry (`src/product.rs:792`): `let adm = state.quotas.pressure_handle(&p.project_id)?;` then `adm.memory_gate(..)`. `memory_pressure_json` (`quota.rs:915-957`) walks only the map.

### 1.4 The failure sequence

1. Project P appends once at T. The absorber retires the debt about 300 s later, and every counter returns to 0.
2. P makes no admission for at least 300 s. The tracker is full, and a first-seen project's sweep evicts entry E1.
3. P returns and `admit` inserts E2. The append's `bind_pressure(E2)` returns early, because the resident handle still holds its binding to E1.
4. Every later frame of that stream is charged to E1.
5. P's memory gate and `/v1/debug/load` never see that stream's unabsorbed bytes or its 64 KiB dirty-stream weight.
6. Each append re-touches the handle, so the orphan lasts as long as the stream stays active. With `HANDLE_IDLE_EVICT_SECS=0` it lasts for the life of the process.

### 1.5 How long a binding holds the handle (C1 and C6 re-verified)

A lookup re-touches the handle: `stream_handle` stores `last_touch_ms` on every call (`shard.rs:2324-2326`). After the last append, the handle is touched again:

- by the absorber when the stream comes due. `due_streams` admits a stream once `now.duration_since(p.since) >= cfg.threshold_age` (`src/history.rs:694`). `threshold_age` is `ABSORB_AGE_SECS`, default 300 (`src/config/cli.rs:253-254`, `bootstrap.rs:101`). The touch then comes from `classify_due` (`history/worker.rs:251`) and `plan_reads` (`history/gather.rs:457`);
- by the committer's `TrimStep`, `UsageAck`, `BillingClose` and `BillingRetained` (`shard/transaction/mod.rs:152`).

The flush ticker evicts only handles idle for `HANDLE_IDLE_EVICT_SECS` (default 600, `cli.rs:265-271`) and referenced by the map alone (`shard.rs:2388-2394`). It runs every 5 s, after an awaited memtable flush (`shard.rs:1682-1711`).

For a small last append, the binding therefore lives at least 300 + 600 + 5 = **905 s**, plus absorber, trim and flush latency.

`HANDLE_IDLE_EVICT_SECS=0` means never (`cli.rs:265-266`; `shard.rs:2389` skips time eviction). A handle then leaves only through `HANDLE_MAX_RESIDENT` (65,536 per shard, oldest-first unreferenced, `shard.rs:2396-2412`), a shard close, or an owner move.

**Window today.** P's entry is evictable from T+300 s. The stale binding survives until about T+905 s. So a return anywhere in that window, while the tracker is full, orphans the stream.

### 1.6 Full use-site list of the tracker entry (`Arc<ProjectAdmission>`)

**Lookups. Each clones under the tracker lock.**

| Function | Lines |
|---|---|
| `admit` | `quota.rs:571-591` |
| `admit_append` | `651-659` |
| `tracked` | `897-899` |

`tracked` is called from:
- `check_read` (729), `debit_read` (760) and `admit_subscription` (775);
- `needs_stream_seed` (799), `reserve_stream` (829) and `release_stream` (861);
- `charge_queued` (878) and `pressure_handle` (905-907).

**Map walks. No clone.** `memory_pressure_json` (915-957), `stats` (964-968) and the sweep (579).

**`pressure_handle` callers.**

| Caller | Purpose |
|---|---|
| `application/append/submit.rs:30` | stream binding |
| `http.rs:1914` | product body buffer |
| `http.rs:2574` | raw append body buffer |
| `product.rs:792` | memory gate (transient) |
| `sse/session.rs:234` | feed binding |

The tests are `dst/tests/admission_memory.rs:420,497,524`, `shard/retirement_tests.rs:396`, `sse/feed/tests.rs:947` and the quota tests.

**Holders, and whether they are counted today.**

| Holder (file:line) | Lifetime | Counter it moves | Can the counter be 0 while held? |
|---|---|---|---|
| `AdmissionPin` inside `QuotaGuard` (`pin.rs:79`, `quota.rs:491-499`) | admission, then the handler | `admitting` | no (the `d4d631df` pin) |
| `SubscriptionGuard` (`quota.rs:504-512`) | streaming body | `live_subs` | no |
| `FeedPressureGuard` (`quota.rs:272-287`), held by `LiveFeed.pressure_guard` (`sse/feed.rs:549`) | feed | `live_feeds` | no |
| `ProjectRetention.admission` (`sse/feed.rs:299`, set by `retention.rs:9-11`) | every feed of the project, and see 1.7 | `retained_sse_bytes` | **yes** (empty rings) |
| `BufferedBodyGuard` (`quota.rs:293-320`; `http.rs:656` reserves 0) | request | `buffered_body_bytes` | yes, request-scoped |
| `QueuedBytesGuard` (`quota.rs:456-467`) | append decision | `queued_bytes` | yes when 0 bytes, request-scoped |
| `StreamReservation` (`quota.rs:430-452`) | create decision | `streams.count` (not in `in_use`) | yes, request-scoped |
| `StreamPressureBinding`, in the handle's `OnceLock` (`shard.rs:665`) | handle residency, 1.5 | `unabsorbed_frame_bytes`, `dirty_streams` | **yes: the item-50 orphan** |

The request-scoped rows are covered by their request's `AdmissionPin`, but only when an admitted request exists. Binders without an admission do exist:
- raw-surface workload appends go through `stream_entry_inner` (`http.rs:1986`) and then `append` (`http.rs:2060`). They carry no principal and no `admit`, and use the deployment project's `sref`;
- billing system appends go through `billing/system_append.rs:67` and then `append_typed`.

Both reach `submit.rs:30` and bind to whatever entry that project has.

### 1.7 Sibling holes the same rule must cover

- **ProjectRetention.** A feed created while its project was untracked binds nothing (`sse/session.rs:234`, `if let Some(adm)`). It keeps the project's `ProjectRetention` alive after the bound feed drops. The retention entry's `OnceLock` keeps E1, and a later feed's `bind_admission(E2)` is ignored (`retention.rs:10`). Once E1 is evicted at zero retained bytes, that retention mirrors every later reservation into the orphan.
- **Bucket (volume) debt: debt that is evicted today.**
  - `admit_append` admits an oversized op from a full bucket and "go[es] negative" (`quota.rs:677-683`, `705`).
  - `debit_read` debits post hoc, "negative-capable" (`quota.rs:743-765`).
  - Eviction replaces those buckets with full ones. `IDLE_EVICT_MS`'s doc claims "an idle project lost no accumulated debt worth keeping at this horizon" (`quota.rs:48-51`). That is false for a debt longer than 300 s of refill, for example a 1 MiB read page at `read_bytes_per_sec = 1024` is about 1,024 s.
  - Nothing holds this debt, so no holder rule can keep it (§9 D4).
- **Accounting that is re-derived, not lost.** `streams: Mutex<StreamCount>` re-seeds from the durable catalog on the next limited create (`quota.rs:74-80`, `791-802`). Evicting it loses nothing: the catalog is the truth.

### 1.8 Already closed

Plan7 §1's "second, narrower hole" is the admit window, where the entry was cloned and then charged after unlock. It was fixed by `d4d631df`: the pin is taken under the lock, and the red test is `a_looked_up_entry_survives_a_sweep_before_its_charge`.

## 2. Contract decision

### 2.1 Eviction rule (recommended R1, §9 D1)

An entry is evictable only when **nothing but the tracker holds it**, meaning its `Arc` strong count is 1 under the tracker lock, **and** it has admitted nothing for `IDLE_EVICT_MS`. Every holder charges the Arc it already has, so every live owner, every counter maintained by a holder and every frame debt keeps its entry, by construction.

A count of 1 read under the lock is final:
- no `Weak<ProjectAdmission>` exists;
- every clone is minted either under that lock (`admit`, `admit_append`, `tracked`) or from a holder that is already counted. Examples are `LiveFeed::bind_pressure`'s `adm.clone()` and a binding made from a request's lookup.

Handle eviction already relies on this argument: `shard.rs:2360-2394`, `strong_count(h) > 1`.

**The holder rule subsumes the `d4d631df` counted pin.**
- The admission's lookup clone is minted under the same lock as the pin, so `admitting` adds nothing to the rule.
- Keeping both would leave a redundant term in `in_use`, and state that no production reader uses.
- This plan therefore:
  - retires `admitting`, `pin()`/`unpin()`/`active()` and `AdmissionPin::drop`;
  - keeps `AdmissionPin` as the named hold (its Arc);
  - re-points the Loom model at the rule. It keeps the model's structure and its `CounterWord` transitions.
- `inflight`, `charge`, `discharge`, rate and concurrency are unchanged.

`has_pressure`, the `live_subs` term and the `counters.active()` term are deleted. Each is implied by its holder, so keeping them would leave equivalent `||`→`&&` mutants.

### 2.2 Boundedness

**Recommended: S-B + Z1, §9 D2/D3.**

- The map stays hard-capped by `MAX_TRACKED_PROJECTS`.
- **At saturation.** A first-seen project gets the existing retryable `503 project_tracker_capacity`, whose typed shape is unchanged. The fail-closed choice of "refuse to track, never merge" is kept.
- **The ordinary longest holder** is a resident stream's binding: at least 905 s after a small last append at the defaults (1.5).
- **Cap.** Raise it to 32,768 (S-B), so the documented certified first-seen pacing of 20/s × 905 s = 18,100 entries fits with about 81% headroom. That is the design rule `MAX_TRACKED_PROJECTS` already states: hold the design first-seen rate for the un-evictable horizon.
- **`HANDLE_IDLE_EVICT_SECS=0` under enforce** would hold every appending project's entry until restart, and make the tracker monotone. Enforce refuses it at validation (Z1, commit 2). Off and shadow never call `admit`, so 0 stays legal there (`product.rs:768`: no principal means no admit; `watch.rs:283` admits only under Enforce).

### 2.3 What changes at the edge

No response shape or code changes. What does change:

1. **503 `project_tracker_capacity` incidence.** A full tracker now counts resident-binding holders. The sustained first-seen churn ceiling for projects that append moves as shown below:

   | Cap | Posture | Ceiling (projects/s) | Held at 20/s |
   |---|---|---|---|
   | 16,384 | today, recency only (bindings do not hold) | 16,384/300 = 54.6 | — |
   | 32,768 | S-B, defaults (`ABSORB_AGE_SECS=300`) | 32,768/905 = 36.2 | 18,100 |
   | 32,768 | S-B, STAGING posture (`ABSORB_AGE_SECS=60`, `docs/STAGING.md:160`) | 32,768/665 = 49.3 | 13,300 |

   - Read-only first-seen projects hold nothing, so their ceiling rises to 32,768/300 = 109/s.
   - The certified 10,000-tenant rotation stays tracked outright. Every tenant returns within 500 s, so it holds 10,000 entries: 31% of 32,768.
2. **The fix itself.** A returning project's resident streams are visible to its memory gate again. `project_memory_pressure` can now engage where it was blind.
3. **Operator surface.** `/v1/debug/load` `admission.trackedProjects` (`http.rs:1034-1038`) reads higher, and `memory_pressure_json` shows the previously orphaned debt. Worst-case tracker memory goes from about 8 MiB to about 16 MiB (about 500 B per entry, the existing doc estimate).
4. **Commit 2.** `STREAMS_AUTH_MODE=enforce` with `HANDLE_IDLE_EVICT_SECS=0` no longer boots. No deployment uses it: `git grep HANDLE_IDLE_EVICT` finds only `cli.rs` and `docs/COST-CAMPAIGN-2.md:266`.
5. **Also for the better.** A counter that drifted with no holder, which is a bug, used to pin its entry forever through `has_pressure`. It is now evicted after the idle horizon.

## 3. Red tests, pins and non-vacuity controls

### 3.1 Commit 1 behaviour red

`quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding`. Insert it in `src/quota/pressure_tests.rs` directly after battery 12 (after line 190: one blank line, then the block). All symbols are already imported (`pressure_tests.rs:3-8`).

```rust
/// Battery 12b (review item 50): a resident stream's pressure binding
/// holds its entry even at zero debt. Its handle binds once and never
/// rebinds, so an evicted entry would keep taking that stream's frame
/// debt after the project returns — debt its live entry's memory gate
/// never reads. The sweep runs exactly at the idle horizon, where idle
/// peers are already evictable.
#[test]
fn eviction_cannot_orphan_a_zero_debt_stream_binding() {
    let r = QuotaRegistry::default();
    let old_ms = 1_000;
    for i in 0..MAX_TRACKED_PROJECTS {
        drop(
            r.admit(&pid(&format!("f{i}")), &ProjectQuotas::default(), old_ms)
                .expect("seed every tracker entry"),
        );
    }
    // f7's resident stream, bound with nothing unabsorbed.
    let binding = StreamPressureBinding::bind(r.pressure_handle(&pid("f7")).unwrap(), 0);
    let horizon = old_ms + IDLE_EVICT_MS;
    drop(
        r.admit(&pid("fresh"), &ProjectQuotas::default(), horizon)
            .expect("the sweep evicts idle peers at the horizon"),
    );
    drop(
        r.admit(&pid("f7"), &ProjectQuotas::default(), horizon)
            .expect("f7 is admitted after the sweep"),
    );
    binding.frames_added(65_536);
    let live = r.pressure_handle(&pid("f7")).expect("f7 is tracked");
    assert_eq!(
        live.unabsorbed_frame_bytes_now(),
        65_536,
        "frame debt the bound stream commits must reach f7's live entry"
    );
}
```

Line layout:
- the inserted block runs from line 191 (blank) to 226;
- `assert_eq!` is at **221:5**;
- line 204 is copied from line 169, and every other line is under rustfmt's chain and call widths of 60, so rustfmt keeps this layout;
- the file becomes 256 lines.

**Trace on the current tree.**
- Each of the 16,384 seed admits drops its guard, which unpins it.
- `bind(.., 0)` moves no counter.
- At `horizon`, elapsed is 300,000 and `300000 < 300000` is false. Every term of `in_use` is false, so all entries are evicted.
- `fresh` is inserted, `f7` gets a new entry E2, and `frames_added` charges E1.

Expected red for `cargo test --locked --lib quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding -- --exact`:

```text
---- quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding stdout ----

thread 'quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding' panicked at src/quota/pressure_tests.rs:221:5:
assertion `left == right` failed: frame debt the bound stream commits must reach f7's live entry
  left: 0
 right: 65536
```

`test result: FAILED. 0 passed; 1 failed`.

**Green.** f7's count is map + binding = 2, so it is retained. The other entries have count 1 and are idle, so they are evicted. The re-admit finds E1, and the test reads 65536.

### 3.2 Commit 1: the affected admission property, extended and red

`quota::tests::quality_quota_admission_leaves_exactly_the_held_guards` (1,024 cases, `src/quota/tests.rs:414-449`) is RUST-QUALITY's "affected admission property" (C5). Its probe `r.tracked(&p).unwrap().in_use(idle)` (line 446) is itself a holder under R1, so it must change. It also gains the binding dimension. It stays one `proptest::proptest!` invocation, so the `owners.json` macro row is unchanged.

The edits inside the macro (rustfmt does not reformat `proptest!` bodies):

- **doc:** "... and once idle the entry is in use only while a guard or a stream's pressure binding (review item 50) still holds it." (+1 line)
- **strategy tuple:** `(0u64..3, 0u64..3, -1_500i64..1_500, proptest::bool::ANY, proptest::bool::ANY),` (95 cols)
- **loop head:** `for (rps, max_inflight, step, release, bound) in steps {`
- **after the `if let Ok(guard) ... }` block, insert:**
  ```rust
              let binding = bound.then(|| StreamPressureBinding::bind(r.pressure_handle(&p).unwrap(), 0));
  ```
- **replace line 446 with:**
  ```rust
              let in_use = r.projects.lock().unwrap()[&p].in_use(idle);
              proptest::prop_assert_eq!(in_use, !held.is_empty() || binding.is_some());
  ```

The binding is created fresh per step, so no `unused_assignments`. `pressure_handle(&p)` always succeeds, because the first admit of any mix creates the entry and nothing sweeps a single project.

**Red on the current tree.**
- Step 1 admits and holds `g1`.
- Step 2 releases `g1`. Its admit with rps ≥ 1 in the same second is rate-refused, so its pin drops, and it binds.
- `in_use` is false (no pin, no subscriptions, zero debt, idle), but the property expects true.

Expected shape:

```text
Test failed: assertion failed: `(left == right)`
  left: `false`,
 right: `true` at src/quota/tests.rs:454:13.
minimal failing input: steps = [
    (0, 0, 0, false, false),
    (1, 0, 0, true, true),
]
```

- Line 454 is the replaced line 446, shifted by +2 imports, +3 cert-rotation doc lines, +1 property doc line, +1 `let binding` line and +1 `let in_use` line.
- The shrunk values come from proptest's shrinker. The shape shown is the minimum: the second step must release and be rate-refused, and must bind.

### 3.3 Commit 1 sizing tripwire (green on both trees; red is its control)

`quota::tests::first_seen_appenders_holding_stream_bindings_never_hit_tracker_capacity` is appended at the end of `src/quota/tests.rs`. Imports:
- `use super::{..., StreamPressureBinding};` (rustfmt moves it to its own line, +1 line);
- `use std::collections::VecDeque;` (+1 line).

```rust
/// Review item 50 sizing: a first-seen project that appends once holds
/// its entry through its stream's pressure binding until the handle
/// idles out. The absorber re-touches the handle ABSORB_AGE_SECS after
/// the append and the flush ticker evicts it HANDLE_IDLE_EVICT_SECS
/// after that touch, on a 5 s pass: at least 905 s at the defaults. At
/// the certified first-seen pacing (20 projects/s, none returning) the
/// cap must hold 20/s x that horizon (18,100 entries) through two full
/// sweeps. A 0 horizon never releases a binding and fails here.
#[test]
fn first_seen_appenders_holding_stream_bindings_never_hit_tracker_capacity() {
    let cli = crate::config::CliArgs::deterministic();
    assert!(
        cli.handle_idle_evict_secs > 0,
        "HANDLE_IDLE_EVICT_SECS=0 never releases a stream's binding"
    );
    let horizon_secs = cli.absorb_age_secs + cli.handle_idle_evict_secs + 5;
    let held_ms = i64::try_from(horizon_secs * 1_000).expect("the horizon fits i64");
    let r = QuotaRegistry::default();
    let quotas = ProjectQuotas::default();
    let t0: i64 = 1_000_000;
    let mut resident: VecDeque<(i64, StreamPressureBinding)> = VecDeque::new();
    let mut refused = 0usize;
    for i in 0..48_000i64 {
        let now = t0 + i * 50; // 20 first-seen projects/s, none returning
        while resident.front().is_some_and(|(at, _)| now - at >= held_ms) {
            drop(resident.pop_front()); // the handle idled out; its binding goes
        }
        let project = pid(&format!("first_seen_{i}"));
        match r.admit(&project, &quotas, now) {
            Ok(_request) => {
                let entry = r.pressure_handle(&project).expect("an admitted project is tracked");
                resident.push_back((now, StreamPressureBinding::bind(entry, 0)));
            }
            Err(QuotaRefusal::TrackerCapacity) => refused += 1,
            Err(e) => panic!("unexpected refusal {e:?}"),
        }
    }
    assert_eq!(
        refused, 0,
        "resident stream bindings exhausted the tracker at the certified first-seen pacing"
    );
}
```

rustfmt splits the two long chains, `held_ms` and `entry`. The line numbers of this block matter to nothing.

- Nesting is 3: fn, for, arm block.
- `CliArgs::deterministic()` (`cfg(test)`, `cli.rs:539-551`) equals the scrubbed clap defaults (`config::tests::cli_fixture_matches_scrubbed_parse`), so the test follows the real defaults.

**Arithmetic at 32,768.**
- Bindings are released 18,100 iterations after admission.
- The first sweep comes at i = 32,768: 14,669 are evicted, leaving 18,099.
- The second sweep comes at i = 47,437: 14,669 are evicted (j from 14,669 to 29,337).
- 0 refused.

The model uses the lower bound of 905 s. Absorber, trim and flush latency of up to 32,768/20 − 900 = 738 s extra still fits.

**Non-vacuity control NV1.** Put `MAX_TRACKED_PROJECTS` back to 16,384 with the rule in place:
- i from 16,384 to 18,099 are refused, because nothing is released before i − 18,100 ≥ 0: 1,716 refusals;
- then one-for-one;
- then i from 34,484 to 36,199 are refused again, because the refused i's never bound: 1,716 more.

The exact red is:

```text
assertion `left == right` failed: resident stream bindings exhausted the tracker at the certified first-seen pacing
  left: 3432
 right: 0
```

On the current tree the test is green: bindings hold nothing, so entries go at 300 s.

### 3.4 Commit 1: Loom model re-pointed

`src/quota/pin/loom_tests.rs`: `quality_loom_a_sweep_never_evicts_an_entry_mid_admission` becomes `quality_loom_a_sweep_never_evicts_a_held_entry`. The model is not a red: `retained` does not exist before the fix. Its non-vacuity controls are NV2 and NV4 in §7.

### 3.5 Commit 2 red

`config::validation::validation_tests::config_validation_tests::validation_rejects_a_never_evicting_handle_horizon_under_enforce`. Append it inside `mod config_validation_tests`, before its closing `}` (`src/config/validation_tests.rs:724`).

```rust

    /// Review item 50: under enforce a resident stream's pressure binding
    /// holds its project's admission entry until the handle idles out, so
    /// HANDLE_IDLE_EVICT_SECS=0 (never) would hold every appending project
    /// until restart and a full tracker would refuse every new one. Off
    /// enforce nothing is tracked, and 0 stays a valid memory posture.
    #[test]
    fn validation_rejects_a_never_evicting_handle_horizon_under_enforce() {
        let enforce = |c: &mut CliArgs| {
            c.streams_auth_mode = "enforce".into();
            c.project_id = "proj_real".into();
            c.streams_auth_keys_file = Some("/k".into());
            c.streams_auth_policy_file = Some("/p".into());
            c.streams_auth_grants_file = Some("/g".into());
        };
        validate_with(enforce, &[]).expect("the enforce posture itself is valid");
        rejects(
            |c| {
                enforce(c);
                c.handle_idle_evict_secs = 0;
            },
            &[],
            "HANDLE_IDLE_EVICT_SECS",
        );
        validate_with(|c| c.handle_idle_evict_secs = 0, &[])
            .expect("off enforce, 0 keeps handles resident and tracks nothing");
    }
```

Expected red on commit 1:

```text
thread 'config::validation::validation_tests::config_validation_tests::validation_rejects_a_never_evicting_handle_horizon_under_enforce' panicked at src/config/validation_tests.rs:492:32:
validate() must reject (marker "HANDLE_IDLE_EVICT_SECS")
```

The panic comes from the `panic!` in `rejects` (line 492, column 32). The first `expect` is the control that the enforce fixture is otherwise valid. If it fails instead, the red is invalid, and the fixture must be completed before continuing.

### 3.6 Pins

These existing tests stay unmodified and must stay green:

- `quota::tests::tracker_evicts_idle_projects_never_active_ones`
- `tracker_bound_refuses_new_projects_only`
- `tracker_capacity_churn_evicts_idle_never_active` (comment-only edit, §4)
- `cert_rotation_over_ten_thousand_tenants_never_hits_tracker_capacity` (doc-only edit)
- `a_looked_up_entry_survives_a_sweep_before_its_charge`
- `a_refused_admission_leaves_its_entry_evictable`
- `a_live_subscription_keeps_its_entry_through_a_sweep`
- `quota::pressure_tests::eviction_cannot_remove_a_project_with_pressure`
- the 5 poison tests
- the 2 pressure-counting tests

Each was traced under R1. Every holder they use is a guard's Arc, so each keeps its verdict:
- the tracker-race test's `held` plus the parked lookup give count 3, so it is retained;
- a refused admit drops its pin, so it is evictable at 2,000 + IDLE;
- a subscription guard gives count 2, so it is retained.

## 4. Edits, file by file, in commit order

No ceilinged file is touched. The untouched ceilinged files are:
- `src/shard.rs` 3,139 and `src/http.rs` 3,153;
- `src/product.rs` 4,205 and `src/billing.rs` 2,151;
- `src/history.rs` 1,656, `src/auth.rs` 1,637 and `src/registry.rs` 1,452;
- `src/sse/feed.rs` 1,165 and `src/fleet.rs` 1,142.

`src/quota.rs` is limited to 1,000 (`source_rules.violations`: `min(max(1000, 1520), max(1000, 981))`).

| File | Now | After | Limit |
|---|---|---|---|
| `src/quota.rs` | 981 | 980 | 1,000 |
| `src/quota/pin.rs` | 104 | about 90 | 1,000 |
| `src/quota/pin/loom_tests.rs` | 76 | about 83 | 1,000 |
| `src/quota/pressure_tests.rs` | 220 | 256 | 1,000 |
| `src/quota/tests.rs` | 449 | about 500 | 1,000 |
| `src/config/cli.rs` | 686 | 689 (C1), 689 (C2) | 1,000 |
| `src/config/validation.rs` | 973 | 984 | 1,000 |
| `src/config/validation_tests.rs` | 725 | 749 | 1,000 |

### Commit 1: "A tracker entry anything still holds is never evicted, so a zero-debt stream binding keeps its project's entry"

Capture the red run (§3.1, §3.2) with the test hunks applied, before any production hunk. Then apply:

1. **`src/quota/pressure_tests.rs`**: §3.1. The file is `#![cfg(test)]`.
2. **`src/quota/tests.rs`** (`#![cfg(test)]`):
   - the imports (+2);
   - the cert-rotation doc at lines 21-26, 6 lines rewritten as 9 (C4: say which model each number is):
     > Workload-cert W1xW2 shape (bench/WORKLOAD-CERT-PLAN.md): 10,000 resident tenants, 100 active per 5s window rotating over the whole population = 20 first-seen projects/s sustained. Recency alone holds 20/s x IDLE_EVICT_MS = 6,000 entries; a tenant that appends is also held by its streams' pressure bindings for at least 905 s (first_seen_appenders_* below), and every tenant returns within 500 s, so the whole population stays tracked: 10,000 entries. The cap must hold the certified tenant population, not just its active window.
   - the churn-test comment at lines 92-94 (same line count): "(full burst, forgiving any volume debt it had not waited out, as IDLE_EVICT_MS documents)";
   - the property (§3.2);
   - the tripwire, appended (§3.3).
3. **`src/quota/pin.rs`** (no exception anywhere in the file). The new body; unchanged parts are marked:

   ```rust
   //! An admission's hold on its tracker entry. `admit` looks the entry up
   //! under the tracker lock but charges it (rate, then inflight) only after
   //! that lock drops, since a poisoned bucket must not poison the map. The
   //! lookup clones the entry under the lock, and the clone is the hold: a
   //! sweep keeps every entry anything but the tracker still holds
   //! (`retained`), so it never evicts one an admission, its request or any
   //! other holder has (external review §9, review item 50). Every clone is
   //! minted under that lock or from a holder already counted, so a strong
   //! count of one read under the lock is final: no other ordering is
   //! involved.

   use std::ops::Deref;
   use std::sync::Arc;
   use std::sync::atomic::Ordering;

   use super::{IDLE_EVICT_MS, ProjectAdmission};

   // CounterWord trait + its AtomicU64 impl: UNCHANGED (lines 16-34).

   /// An entry's admitted requests in flight.
   #[derive(Default)]
   pub(crate) struct AdmissionCounters<W = std::sync::atomic::AtomicU64> {
       inflight: W,
   }

   impl<W: CounterWord> AdmissionCounters<W> {
       // charge / discharge / inflight: UNCHANGED text (lines 56-67);
       // pin, unpin and active are deleted.
   }

   /// The tracker's retention rule, read under its lock: an entry stays
   /// while anything but the tracker holds it (`holders` is its strong
   /// count, the tracker's own included) or it admitted something within
   /// `IDLE_EVICT_MS` (`idle_ms` ago). A holder charges the Arc it has, so
   /// evicting a held entry would strand its later charges where no gate
   /// reads them.
   pub(super) fn retained(holders: usize, idle_ms: i64) -> bool {
       holders > 1 || idle_ms < IDLE_EVICT_MS
   }

   /// The hold of one admission, its lookup's clone: dropped with `admit`'s
   /// refusal, or kept by the admitted request's guard.
   pub(super) struct AdmissionPin(Arc<ProjectAdmission>);

   impl ProjectAdmission {
       /// Under the tracker lock only.
       pub(super) fn pin(self: &Arc<Self>) -> AdmissionPin {
           AdmissionPin(Arc::clone(self))
       }
   }

   // impl Deref for AdmissionPin: UNCHANGED. impl Drop for AdmissionPin: DELETED.

   #[cfg(test)]
   mod loom_tests;
   ```
4. **`src/quota/pin/loom_tests.rs`**:
   - header doc: "The tracker's retention rule (`retained`) over Arc holds, on Loom's Arc and atomics, under a Loom mutex standing in for the tracker lock. Two threads, preemption bound 2, 1,000 branches per execution; no duration or permutation cutoff. The map itself and `admit`'s rate bucket are not modelled: `quota::tests` parks a real admit between its lookup and its charge, and `quota::pressure_tests` holds a real stream binding through a real sweep."
   - imports: `use super::{AdmissionCounters, CounterWord, retained};` and `use crate::quota::IDLE_EVICT_MS;`
   - the `CounterWord` impl for loom `AtomicU64` and `type Entry` are unchanged;
   - the model:

   ```rust
   /// An idle entry, as a sweep at the horizon sees it.
   fn evictable(entry: &Entry) -> bool {
       !retained(Arc::strong_count(entry), IDLE_EVICT_MS)
   }

   /// External review §9 and item 50: an admission clones its entry under
   /// the tracker lock and charges it after the lock drops; a holder minted
   /// from that clone (a stream's pressure binding) outlives the admission;
   /// a sweep evicts, under the lock, only an idle entry nothing but the
   /// tracker holds. Whatever the interleaving, an entry anything holds stays
   /// tracked, and it is evictable once its last holder is gone.
   #[test]
   fn quality_loom_a_sweep_never_evicts_a_held_entry() {
       // Builder: max_threads 2, max_branches 1000, preemption_bound Some(2),
       // max_permutations None, max_duration None (UNCHANGED).
       model.check(|| {
           let entry: Entry = Arc::new(AdmissionCounters {
               inflight: AtomicU64::new(0),
           });
           let tracker = Arc::new(Mutex::new(Some(entry)));
           let sweeper = tracker.clone();
           let sweep = loom::thread::spawn(move || {
               let mut tracked = sweeper.lock().unwrap();
               if tracked.as_ref().is_some_and(evictable) {
                   *tracked = None;
               }
           });
           let found = tracker.lock().unwrap().as_ref().map(Arc::clone);
           if let Some(admission) = found {
               admission.charge();
               let binding = Arc::clone(&admission);
               admission.discharge();
               drop(admission);
               sweep.join().unwrap();
               let tracked = tracker.lock().unwrap();
               assert!(
                   tracked.as_ref().is_some_and(|e| Arc::ptr_eq(e, &binding)),
                   "a sweep evicted an entry its holder still had"
               );
               drop(binding);
               assert!(
                   tracked.as_ref().is_some_and(evictable),
                   "an entry nothing holds is left in use"
               );
           } else {
               sweep.join().unwrap();
           }
       });
   }
   ```

   The model runs the actual `retained` and the actual `charge`/`discharge` transitions, with loom's instrumented `Arc` for clone and drop. `in_use` is the one-line adapter `retained(Arc::strong_count(self), idle)` on std's `Arc`, and it is covered by the unit tests and the mutation leg (§5). This matches C7: handle eviction (`shard.rs:2360-2394`) rests on the same count-under-lock argument.
5. **`src/quota.rs`** (981 → 980). No exception-attributed item is touched (see the ratchet paragraph below).
   - **`MAX_TRACKED_PROJECTS`** (lines 32-46): the value becomes `32_768`. The doc goes from 14 to 19 lines (+5):
     > Bounded tracker: beyond this many distinct projects the tracker refuses NEW ones (503, typed `TrackerCapacity`) instead of growing without bound. An entry is evictable only when nothing but the tracker holds it and it has admitted nothing for IDLE_EVICT_MS (`ProjectAdmission::in_use`), so the cap must hold every project a live holder keeps, not just its active window. The longest ordinary holder is a resident stream's pressure binding: the absorber re-touches its handle ABSORB_AGE_SECS after the last append and the flush ticker evicts it HANDLE_IDLE_EVICT_SECS after that touch, on a 5 s pass — at least 905 s at the defaults. The certified first-seen pacing (20 projects/s) therefore holds 20/s x 905 s = 18,100 entries (quota/tests.rs pins that shape); 32,768 holds it with ~80% headroom, and the 10,000-tenant rotation outright, at ~16 MiB worst-case tracker memory (~500 B/entry). (The recency-only 300 s horizon once needed 6,000; the 4,096 cap of the 2026-08-19 churn rung shed 19% of those.) Still a deliberate HARD ceiling: refusing to track (never merging strangers into shared buckets) remains the fail-closed choice; the churn test pins evict-idle-first, never-evict-held, and the typed refusal at true saturation.
   - **`IDLE_EVICT_MS`** (lines 48-51, 4 → 5 lines):
     > A tracked project with no admission attempts for this long, and nothing but the tracker holding its entry (`ProjectAdmission::in_use`), may be evicted under tracker pressure. Its buckets restart full, forgiving any read or oversized-append debt the project had not yet waited out.

     This states today's behaviour truthfully. See §9 D4.
   - **field doc, line 71:** "Admitted requests in flight (holds are the entry's Arc clones)."
   - **delete `has_pressure`** (lines 151-162, including the trailing blank line; −12).
   - **`in_use`** (lines 163-173, 11 → 15 lines):
     ```rust
         /// Under the tracker lock: the entry stays while anything but the
         /// tracker holds it or it admitted something within IDLE_EVICT_MS
         /// (`pin::retained`). Every holder charges the Arc it already has: an
         /// admission or its request, a live subscription, a feed or its
         /// retention mirror, a body, queued-byte or stream-slot charge, and a
         /// resident stream's pressure binding, which holds it even at zero
         /// debt. The handle binds once and never rebinds, so evicting that
         /// entry would send the stream's later frames to an entry no memory
         /// gate reads (review item 50).
         fn in_use(self: &Arc<Self>, now_ms: i64) -> bool {
             pin::retained(
                 Arc::strong_count(self),
                 now_ms - self.last_seen_ms.load(Ordering::Relaxed),
             )
         }
     ```
     The call site `m.retain(|_, a| a.in_use(now_ms));` (line 579) is unchanged. `a: &mut Arc<ProjectAdmission>` auto-derefs to `Arc<_>`, and autoref then gives `&Arc<_>`.
   - **`StreamPressureBinding` doc:** replace the last line (329-330) with 3 lines (+1): "... releases this instance's attribution exactly. Its Arc holds the project's tracker entry while the handle is resident, even at zero debt (review item 50)."
6. **`src/config/cli.rs`**: the `HANDLE_IDLE_EVICT_SECS` doc (lines 265-269, 4 → 7 lines; C3). It is `--help` text only: `cli_surface_is_pinned` pins flag, env and default, not the prose.
   > Evict resident per-stream handles idle at least this long (seconds; 0 = never). Handles reload from the shard DB on next touch; the durable dirty-stream index keeps unabsorbed evictees discoverable. A resident handle's pressure binding also holds its project's admission-tracker entry, so with ABSORB_AGE_SECS this sets how long a project that appended stays tracked (MAX_TRACKED_PROJECTS).
7. **`docs/MULTITENANCY.md:1403-1408`**: rewrite the "Bound project trackers" bullet:
   > *(32,768-project cap since review item 50 — an entry is evictable only when nothing but the tracker holds it and it has been idle 300 s, so a resident stream's pressure binding holds its project until the handle idles out (ABSORB_AGE_SECS + HANDLE_IDLE_EVICT_SECS + the 5 s ticker, at least 905 s at the defaults). At the certified first-seen pacing of 20 projects/s that is 18,100 held entries; the workload-cert round's 16,384 cap was sized for 20/s x 300 s = 6,000 recency-only entries, and the older 4,096 cap shed 19% of those. The 10,000-tenant rotation stays tracked outright. Over the cap, NEW projects get 503 `project_tracker_capacity`, tracked ones are untouched.)*

End the commit message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

**Every ratcheted scope, and how it stays non-growing.** No edit is inside an exception-attributed item, so no contract changes and no reason is touched.

- In `src/quota.rs` the excepted items are:
  - `StreamReservation::drop` (442), and `QuotaRegistry::admit` (556/560, `unwrap_used` + `excessive_nesting`);
  - `admit_append` (632/636), `check_read` (716) and `debit_read` (746);
  - `needs_stream_seed` (794), `reserve_stream` (816) and `release_stream` (856);
  - `tracked` (893), `memory_pressure_json` (911) and `stats` (960).
- Their text is byte-identical after this commit. In `admit` that includes `Some(a) => a.pin(),`, `m.retain(|_, a| a.in_use(now_ms));`, `a.pin()`, `admission.counters.charge()`/`discharge()` and `Ok(QuotaGuard { admission })`. So their `scope_lines`, `syntax_facts` and `unwrap_site*` fingerprints are unchanged: fingerprints key on the qualified item and value, not the line (`source_rules._fingerprint_sites`).
- The `admit` `excessive_nesting` expect still fires at the unchanged `if m.len() >= MAX_TRACKED_PROJECTS {` block.
- `impl ProjectAdmission` (138) has no attribute. Neither does anything in `pin.rs`.
- The test files are `#![cfg(test)]`, and carry no exception.
- `bucket.rs` is untouched. That matters, because `tools/quality-invariants/src/lib.rs:41-47`'s `#[allow(dead_code, reason = ...)]` on `#[path] mod quota_bucket;` measures `bucket.rs` as its module file (§9 D4).

### Commit 2 (D3 = Z1): "Enforce refuses HANDLE_IDLE_EVICT_SECS=0, which would hold every appending project's tracker entry until restart"

1. **`src/config/validation_tests.rs`**: §3.5, captured red on commit 1.
2. **`src/config/validation.rs`** (973 → 984). In `validate_auth_and_keys`, which has no exception and grows from 68 to 79 lines, insert after line 903 (the close of the `if auth_mode.is_some_and(..)` block) and before `let catalog_cursor_key`:
   ```rust
           // Review item 50: under enforce (the only mode that tracks
           // projects) a resident stream's pressure binding holds its
           // project's admission entry until the handle idles out; 0 (never)
           // would hold every appending project until restart, and a full
           // tracker would then refuse every new one.
           if auth_mode == Some(crate::auth::AuthMode::Enforce)
               && self.cli.handle_idle_evict_secs == 0
           {
               f.err(
                   "STREAMS_AUTH_MODE=enforce requires HANDLE_IDLE_EVICT_SECS > 0: a resident \
                    stream handle holds its project's admission entry until it idles out",
               );
           }
   ```
   `AuthMode` derives `PartialEq` (`auth.rs:79`).
3. **`src/config/cli.rs`**: append "Enforce refuses 0." to the last doc line added in commit 1 (0 lines).

Push commits 1 and 2 together, so no pushed revision holds bindings under an accepted `HANDLE_IDLE_EVICT_SECS=0` enforce configuration. No deployment sets it either way.

## 5. Mutation analysis

The owners are the existing rows `quota_registry` (`src/quota.rs`) and `quota_pin` (`src/quota/pin.rs`), both with the filter `quota::` (`scripts/quality/mutation_owners.py:106-107`). No row changes.

`src/config/validation.rs` is neither a critical prefix nor registered, so it is not selected. The operator set is cargo-mutants 27.1's, as seen in the `d4d631df` run: `target/quality-mutations/quota_{registry,pin}/mutants.out/caught.txt`.

| # | Mutant | Killed by |
|---|---|---|
| 1 | `retained -> bool` with `true` | `tracker_evicts_idle_projects_never_active_ones` (`p_new` refused: `.is_ok()` fails); §3.1 (`.expect("the sweep evicts idle peers at the horizon")`); Loom ("an entry nothing holds is left in use"); property |
| 2 | `retained` with `false` | `tracker_bound_refuses_new_projects_only` (recent entries evicted, `p_new` admitted); §3.1 (`left: 0`); Loom ("a sweep evicted an entry its holder still had"); property |
| 3 | `holders > 1` → `== 1` | §3.1: f7 is evicted and the unheld entries are kept, so f7's re-admit is refused (`.expect("f7 is admitted after the sweep")`); `tracker_evicts_idle_...` (`tracked == 2` fails); Loom |
| 4 | `>` → `<` | §3.1 (`left: 0`); churn test ("pinned project was evicted (fresh entry admitted)"); Loom; property |
| 5 | `>` → `>=` | as #1 (always retained) |
| 6 | `\|\|` → `&&` | §3.1 (f7 held but idle is evicted); `tracker_bound_refuses_new_projects_only` (recent unheld entries evicted); churn test; Loom |
| 7 | `idle_ms < IDLE` → `==` | §3.1 (every seed at exactly the horizon is retained, so `fresh` is refused); `tracker_bound_refuses_...`; Loom (idle == horizon, so "left in use") |
| 8 | `<` → `>` | `tracker_bound_refuses_new_projects_only` (idle 0 counts as not recent) |
| 9 | `<` → `<=` | §3.1 (the exact horizon); `a_live_subscription_keeps_its_entry_through_a_sweep` (`p_new` at exactly `IDLE_EVICT_MS`: `.unwrap()` panics); `a_refused_admission_leaves_its_entry_evictable` (2,000 + IDLE); Loom |
| 10 | `in_use -> bool` with `true` | as #1 |
| 11 | `in_use` with `false` | as #2 |
| 12 | `now_ms - last` → `+` | churn test: t0 = 1,000,000, so the sum is never recent; `churn_{cap}` is admitted at t0+1,000 ("expected TrackerCapacity, got admission") |
| 13 | `-` → `/` | §3.1 (301,000 / 1,000 = 301 counts as recent, so `fresh` is refused); `tracker_evicts_idle_...` (entries at 0, so the division by zero panics) |
| 14 | `ProjectAdmission::pin -> AdmissionPin` with `Default::default()` (its span holds the deleted line) | **unviable** (no `Default`), as in `d4d631df` |

Expected: **13 caught, 1 unviable, 0 missed, 0 timeout.**

- There are no equivalent mutants: the subsumed terms are deleted, not kept.
- Deleted code (`pin`/`unpin`/`active`/`AdmissionPin::drop`/`has_pressure`) yields no mutants.
- The unchanged `CounterWord` impls, `charge`/`discharge`/`inflight` and `Deref` are not in the diff.

**Timeouts** (driver `--timeout 90`, `--profile quality`, `--jobs 1`). The worst mutants are the never-evict ones (#1, #5, #10). Under them the tripwire sweeps 32,768 entries on each of 15,232 refused admits: about 499M closure calls, a few seconds at opt-level 1. The churn test adds 2,000 × 32,768 ≈ 66M. `/` panics on the divide rather than hanging. Every loop is bounded and there are no waits. The Loom model is bounded by its builder.

## 6. Ledgers

- `scripts/quality/mutation_owners.py`: unchanged. There are no new files, and `pin.rs`/`quota.rs` are already registered.
- `docs/quality/owners.json`: unchanged.
  - The property is still one `proptest::proptest` invocation (row count 1).
  - The tripwire adds no thread, spawn, macro-DSL or global.
  - The Loom model's `loom::thread::spawn` is not classified as an effect (`source_rules.classify`), as before.
  - The tracker-race rows (`std::thread::scope`, `yield_now`) are untouched.
- `docs/quality/source-allowances.json`: unchanged. The `serde_json::json` rows are keyed by owner function: `memory_gate`, `memory_pressure_json`, `pressure_model_json`, all untouched.
- `docs/quality/exception-growth.json`: no row. No contract grows, and an agent never adds one.
- `docs/refactor/test-inventory.json`: unchanged. It is DST-only, and `python3 scripts/test-inventory.py --check` stays clean.
- `docs/refactor/review-mechanisms.json`: unchanged. No pin names a quota test or file.
- `docs/refactor/architecture-policy.json`: unchanged (no new file).
- `docs/refactor/WIRE-MATRIX.md:85`: unchanged. The 503 `project_tracker_capacity` shape is the same; only its incidence changes (§2.3).
- `docs/MULTITENANCY.md`: updated in commit 1.
- `docs/reviews/2026-09-hardening/README.md` (work package 3 lists item 50 as "plans in progress") and `edge-changes.md`: in a docs-only follow-up once the hashes exist, add:
  - #53, low, product/both, the 503 incidence and memory-gate visibility;
  - #54, low, process, the enforce + `HANDLE_IDLE_EVICT_SECS=0` refusal.

  Use the file's own record format: before/after, retry semantics, pins.
- The `--lib quality_` leg (`--min 15`): same count. The Loom model is renamed and the property changed.

## 7. Controls (exact commands, expected outputs)

Run from `/Users/sorenschmidt/code/streams`.

| # | Step | Command | Expected |
|---|---|---|---|
| 1 | Red, C1 test hunks only | `cargo test --locked --lib quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding -- --exact` | §3.1 panic (`left: 0` / `right: 65536` at `pressure_tests.rs:221:5`); `test result: FAILED. 0 passed; 1 failed` |
| 2 | Red, property | `cargo test --locked --lib quota::tests::quality_quota_admission_leaves_exactly_the_held_guards -- --exact` | FAILED with the §3.2 shape (`left: false`, `right: true`, a two-step minimal input) |
| 3 | Tripwire before the fix | `cargo test --locked --lib quota::tests::first_seen_appenders_holding_stream_bindings_never_hit_tracker_capacity -- --exact` | `ok` |
| 4 | Green, C1 applied | commands 1-3, plus `cargo test --locked --lib quota::pin::loom_tests::quality_loom_a_sweep_never_evicts_a_held_entry -- --exact` | each `test result: ok. 1 passed` |
| 5 | NV1 | put `MAX_TRACKED_PROJECTS` back to `16_384`; run command 3 | `left: 3432` / `right: 0`; revert |
| 6 | NV2 | make `retained` return `idle_ms < IDLE_EVICT_MS` (holder term dropped) | command 1 gives `left: 0`; the Loom model panics "a sweep evicted an entry its holder still had"; `a_looked_up_entry_survives_a_sweep_before_its_charge` fails ("a sweep evicted the entry an admitted request was still charging"); `a_live_subscription_keeps_its_entry_through_a_sweep` fails; revert |
| 7 | NV3 | set `handle_idle_evict_secs: 0` in `CliArgs::deterministic` (local only: it also breaks `cli_fixture_matches_scrubbed_parse`) | command 3 panics "HANDLE_IDLE_EVICT_SECS=0 never releases a stream's binding"; revert |
| 8 | Whole owner scope | `cargo test --locked --lib quota::` | `test result: ok. 42 passed` (40 today: 16 + 11 + 2 + 5 + 1 + 5 `#[test]` in tests, pressure_tests, pressure_counting_tests, poison_tests, pin/loom_tests and bucket; plus the 2 new tests) |
| 9 | C2 red (on C1) | `cargo test --locked --lib config::validation::validation_tests::config_validation_tests::validation_rejects_a_never_evicting_handle_horizon_under_enforce -- --exact` | §3.5 red; then `ok` once C2 is applied. NV4: deleting the new `if` restores the red |
| 10 | fmt, clippy, doc | `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings`; `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` | fmt prints nothing and exits 0. clippy has no findings: no `dead_code` for the deleted `active`/`has_pressure`, no `unfulfilled_lint_expectations` on `admit`, no nesting finding. The doc build succeeds: backticked names only, no `[..]`/`<..>` outside code |
| 11 | Plan | `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py --out target/quality-plan` | see below |
| 12 | Mutants | `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_MUTANTS_OUT=target/q50-mutants scripts/quality/mutations.sh` | §5: 13 caught + 1 unviable across `quota_registry`/`quota_pin`; 0 missed, 0 timeout |
| 13 | Gate | `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) OUT=/tmp/q50-gate.txt scripts/gate.sh` | no `GATEFAIL-*`. Its `scripts/quality.sh` prints no `file growth:`, no `accepted exception grew without an approved growth row:` and no `exception needs owner; invariant; alternative`. `tests_ran.py` passes. The suite's `test result: ok` lines have 2 more quota tests and 1 more config test than before |
| 14 | Quality leg | `scripts/test-leg.sh target/legs/quality.log --min 15 -- --locked --release --lib quality_` | ok (same count) |

Expected `plan.json` for command 11, with C1+C2 on top of `origin/slate`:
- `mutants: true`, `properties_fuzz: true` (quota prefix), `miri: false` (no buffer prefix or tooling path);
- `mutation_source_files: ["src/quota.rs","src/quota/pin.rs"]`;
- `selected_mutation_owners: ["quota_registry","quota_pin"]`;
- `unregistered_mutation_source_files: []`;
- `production_unchanged_files ⊇ ["src/quota/pin/loom_tests.rs","src/quota/pressure_tests.rs","src/quota/tests.rs"]`.

`validation_tests.rs` has no `#![cfg(test)]`, so it is listed only under `changed_rust_files`. The unpushed `d255ad6d` adds docs only.

## 8. Out of scope

- **Rebinding** `StreamHandle.pressure` (the §9 D1 alternative). No edit is made in `shard.rs`, `publish.rs` or `submit.rs`.
- **Exact bucket-debt retention** (§9 D4). It needs growth on the harness contract over `bucket.rs`. This plan only corrects the `IDLE_EVICT_MS` doc to state what eviction forgives today.
- **The `ProjectRetention` linger race** (found while verifying; for a separate item). `release_project_entry` (`sse/feed.rs:386-397`) removes the entry only when `strong_count == 2` during `LiveFeed::drop`, while the dropping feed's own field is still alive. When two feeds of one project drop concurrently, both can see 3 and leave the entry in `by_project`, together with its admission `Arc`.
  - Today that is an orphan hazard, as in 1.7.
  - Under R1 it becomes one held tracker slot until that project's next feed drops last, or a restart.
- **Validating a large `HANDLE_IDLE_EVICT_SECS`.** For example, 3,600 gives a horizon of 3,905 s and a 32,768/3,905 ≈ 8.4/s first-seen ceiling. It is documented in the knob's help text; a bound would be a separate decision.
- **The per-stream usage tracker in `src/usage.rs`** (`MAX_TRACKED` 65,536, overflow counters). It is a separate subsystem with its own best-effort telemetry posture, and is not item 50.
- **`/v1/debug/load` `project_memory_shed_total`.** It sums tracked entries, so it drops when an idle shed history is evicted. This is pre-existing and an observability total only.
- **Historical records** are left as they are: `docs/COST-CAMPAIGN-2.md:266-267` and `bench/WORKLOAD-CERT-PLAN.md:144-156`.
- **A tracker-sweep property with fills.** The extended property (§3.2) plus the Loom model cover the rule without a 32k fill per case.

## 9. Decisions for the owner

### D1. Eviction rule. Recommend R1.

- **R1: holder rule** (this plan). Evict only an entry nothing but the tracker holds (strong count 1 under the lock) that has been idle for 300 s.
  - Exact by construction for every holder, present and future, including bindings, `ProjectRetention` and holders minted without an admission (1.6).
  - No new lock site, no signature change, no excepted scope touched.
  - Cost: it subsumes the counted `admitting` pin landed today in `d4d631df`. This plan retires it and re-points that commit's Loom model at the rule instead of keeping two mechanisms.
- **R2: explicit binding hold.** The binding calls `counters.pin()` in `bind` and `unpin()` in drop, reusing `d4d631df`'s counter.
  - About 2 production lines, and `d4d631df` stays intact.
  - But the hold is taken outside the tracker lock, after `pressure_handle`'s lookup and an `await` (`submit.rs:30-31`). That is sound only while the caller's own request pin holds the entry.
  - Binders with no admission are raw workload appends (`http.rs:1986`/`2060`) and billing system appends (`billing/system_append.rs:67`). They can still attach to an entry evicted in that window, which is exactly item 50's orphan.
  - `ProjectRetention` stays uncounted.
  - Rejected, because it is "never" only for principal requests.
- **R3: rebinding.** The handle follows the live entry, with `OnceLock` replaced by a swappable cell and `current_unabsorbed` transferred under the state lock.
  - Keeps today's occupancy: 54.6/s first-seen at 16,384, and `=0` is harmless.
  - But it evicts live ownership, contrary to position 50.
  - It is a synchronization change in `src/shard.rs`, which is at its 3,139-line ceiling (0-line budget), and in the `unwrap_used`-excepted `bind_pressure`. A new `.lock().unwrap()` site is a new exception, and publish.rs would need Loom on shard code.
  - It leaves orphan charges from eviction until the next submit, and leaves `ProjectRetention` exposed.
  - Rejected.

### D2. Cap sizing. Recommend S-B.

The ceiling applies to first-seen projects that append.

| Option | Cap | Ceiling at defaults (H = 905 s) | Ceiling at STAGING (H = 665 s) | 20/s model | Worst-case memory | Other |
|---|---|---|---|---|---|---|
| S-A | 16,384 | 18.1/s | 24.6/s | refuses 9.5% at steady state at defaults (NV1: 3,432 of 48,000 in the tripwire) | 8 MiB | tripwire rate drops to 18/s (headroom 94 entries) |
| **S-B** | 32,768 | 36.2/s | 49.3/s | about 81% headroom (18,100 held) | 16 MiB | sweep O(C) under the global tracker mutex per first-seen admit at saturation doubles (estimate, not measured) |
| S-C | 65,536 | 72.4/s (above today's 54.6/s) | 98.5/s | ample | 32 MiB | sweep ×4 |
| S-D | keep 16,384 and lower the `HANDLE_IDLE_EVICT_SECS` default 600 → 300 | 27.1/s (H = 605 s) | — | — | — | more tail-row reloads for streams idle 300-600 s (`COST-CAMPAIGN-2` tuned 600); changes a clap default pinned in `config::tests::EXPECTED_CLI_SURFACE` ("a product decision") |

S-B keeps the cap's own stated design rule: hold the certified first-seen rate for the un-evictable horizon.

### D3. `HANDLE_IDLE_EVICT_SECS=0`. Recommend Z1.

- **Z1** (commit 2): enforce refuses it at validation. Off and shadow keep it, because they track nothing. Edge change: that configuration no longer boots, and none is deployed.
- **Z2:** accept it and document the consequence. Under enforce, every appending project's entry is held until handle displacement (`HANDLE_MAX_RESIDENT` 65,536 per shard), a shard move or a restart. After 32,768 such projects, every new project gets 503 until restart, which breaks review item 5's "must not refuse ... until restart".
- **Z3:** reject 0 in every mode. Simpler, but it removes a legal off/shadow memory posture for no tracker benefit.
- **Optional Z4, alongside Z1:** also bound `ABSORB_AGE_SECS + HANDLE_IDLE_EVICT_SECS` under enforce, as a notice or an error, so an operator cannot silently shrink the churn ceiling.

### D4. Bucket (volume) debt. Recommend the exact fix via an owner row, as a follow-up.

Position 50 says "never evict debt", but eviction forgives read and oversized-append bucket debt not yet waited out (1.7), and nothing holds that debt.

- **Exact.** Store the refill rate on the balance so the sweep can tell whether a bucket still owes at `now`:
  - `Bucket` gains `rate` (set by `full`/`refill`) and `owes(now_ms)`;
  - `ProjectAdmission::owes_volume` does `try_lock` over the three volume buckets. A poisoned bucket counts as owing. Under R1, strong count 1 means no other thread can hold a bucket guard;
  - `in_use` becomes `retained(..) || self.owes_volume(now_ms)`.

  This **grows** the harness contract (`tools/quality-invariants/src/lib.rs`, owner of `mod quota_bucket`, scope kind module, lint `dead_code`), because it measures `bucket.rs`: `scope_lines`, `syntax_facts`, `fields` 2→3, and a new `field_site`. It needs an owner-approved `docs/quality/exception-growth.json` row with the values the gate reports on that commit. An agent may not add it.

  Doing it at the debit sites instead would grow the `unwrap_used` contracts of `admit_append`/`debit_read`. An entry would then be held for its debt's duration after its last admission. That is bounded per project by (largest single debit)/rate.
- **Conservative, no growth.** Hold while any volume bucket's `level < 0.0` (`level` is `pub(super)`, readable from `quota.rs`) and idle < a new cap constant. This over-retains paid-off debt until its next refill, still forgives debt beyond the cap, and adds a policy constant.
- **Accept.** Keep forgiving; this plan's doc edit states it.

### D5. Found issue: `ProjectRetention` linger race (§8)

Open a separate item. Under R1 it holds one tracker slot per occurrence, instead of orphaning retention bytes.

## Skeptic corrections (C1..C9)

Checked read-only against `slate` @ `d255ad6d` (= `origin/slate` now; `d4d631df..d255ad6d` touches no `src/`, `scripts/quality`, `docs/quality`, `docs/refactor` or `tools/`). No cargo was run.

**Confirmed as written:**
- the problem trace: `quota.rs:151-173`, `331-348`, `367-390`, `571-591`, `897-907`; `pin.rs:47-101`; `shard.rs:665`, `678-691`, `2324-2326`, `2388-2412`, `1682-1711` (ticker period hard-coded at 5 s, `shard.rs:1682`); `submit.rs:30-34`; `publish.rs:45-47`; `product.rs:768`, `792`; `watch.rs:283-296`; `history.rs:694`; absorber touches at `history/worker.rs:251` and `history/gather.rs:457`. Shadow builds no principal (`product.rs:568-579`), so only enforce tracks;
- every holder of `Arc<ProjectAdmission>` in `src` (`git grep`): the guards in `quota.rs`, `AdmissionPin`, `http.rs:653`, `LiveFeed::bind_pressure` (`sse/feed.rs:678-683`), `ProjectRetention.admission` (`sse/feed.rs:299`, `retention.rs:9-11`), the handle binding. No `Weak`. Every clone is minted under the tracker lock or from an existing holder, so the count-of-one argument holds by value, whatever the load ordering;
- all `wc -l`: quota.rs 981, pin.rs 104, loom_tests.rs 76, pressure_tests.rs 220, tests.rs 449, cli.rs 686, validation.rs 973, validation_tests.rs 725. The ceilinged files are as listed and none is touched;
- the §3.1 red. The sweep at elapsed = 300,000 evicts f7 (every `in_use` term is false), `frames_added` charges E1, and the test fails `left: 0 / right: 65536` at `pressure_tests.rs:221:5`. Green: map + binding = 2;
- the §3.2 property red. A step that is rate-refused and bound leaves `in_use` false. This needs `release`, for example a step-1 bucket made at rps 0 (`Bucket::full(0)`, `bucket.rs:12-17`) and then rps 1 at the same instant;
- the §3.3 tripwire arithmetic (14,669 / 18,099 / i = 47,437) and NV1 = 1,716 + 1,716 = 3,432. It is green today at the 16,384 cap, because 6,000 entries are held;
- the compile points:
  - `m.retain(|_, a| a.in_use(now_ms))` resolves to `self: &Arc<Self>` by autoref after one deref;
  - `m[&p].in_use(idle)` compiles on both trees;
  - the child modules reach the private `retained` / `inflight`;
  - loom 0.7.2 has `Arc::strong_count` and `Arc::ptr_eq` (`sync/arc.rs:122,171`);
  - `AuthMode` is compared with `!=` already (`validation.rs:869`);
  - `Findings::err` takes `impl Into<String>` (`validation.rs:639`);
- all 14 mutants are dispositioned as tabled:
  - #7 and #9 are killed by §3.1's exact-horizon admit;
  - #8 and #6 are killed by `tracker_bound_refuses_new_projects_only`;
  - #12 is killed only by the churn test;
  - #13 is killed by §3.1 and by divide-by-zero in `tracker_evicts_idle_...`;
  - `pin -> Default` is unviable;
  - never-evict mutants cost about 5×10⁸ `retain` closure calls at `--profile quality` (opt-level 1, `Cargo.toml:77-81`), under `--timeout 90` (`mutation_driver.py:52`);
- the ratchet trace. No edit lands in an excepted scope. The `admit` text is byte-identical (`quota.rs:556-624`). `impl ProjectAdmission` (138) and `pin.rs` carry no attribute. The harness mounts only `bucket.rs` (`tools/quality-invariants/src/lib.rs:42-47`). The source-allowance and owners rows for quota are untouched. `validation.rs:857-924` has no exception, and `src/config` is neither a critical prefix nor registered (`verification_plan.py:22-31`; `mutation_owners.py` has only `admission_limits`);
- no deployment sets `HANDLE_IDLE_EVICT_SECS`: `git grep` finds only `cli.rs`, `config/tests.rs:430`, `COST-CAMPAIGN-2.md` and the review record. `--help` prose is not pinned: `cli_surface_is_pinned`, `config/tests.rs:507-534`, checks flag, env and default only.

**C1 (blocking: the commit-2 red command is vacuous).** `rejects` and `validate_with` live in `mod validate_boundary_tests` (`validation_tests.rs:467-725`; helpers at `480`, `489`). `mod config_validation_tests` (`43-465`) has neither.
- The plan inserts the test before line 724/725, which really is `validate_boundary_tests`, so it compiles. But §3.5, §7 row 9 and the expected panic header all name `...::config_validation_tests::...`, and `--exact` with that path runs **0 tests and prints `test result: ok. 0 passed`**. The "red" would pass.
- Use `config::validation::validation_tests::validate_boundary_tests::validation_rejects_a_never_evicting_handle_horizon_under_enforce`. Insert before the module's closing `}` at line **725**.
- Sizes: the test block is 27 lines, so the file goes 725 → **752**. After rustfmt the validation block is 13 lines (the `if` line is 102 columns and splits as shown), so `validation.rs` goes 973 → **986**, and `validate_auth_and_keys` goes 68 → **81** lines. All are within limits.
- Also update that function's doc (`validation.rs:855-856`, "the auth mode, its required files and refresh cadence, and the catalog cursor key"), or the new tracker rule sits under a doc that does not describe it.

**C2 (ledger: the red run writes a regression file).** The property keeps proptest's default persistence (`quota/tests.rs:415-418`, `..ProptestConfig::default()`), so the §3.2 red run writes `proptest-regressions/quota/tests.txt`.
- The repository tracks these files (`proptest-regressions/postings_cache/tests.txt`, not git-ignored). RUST-QUALITY.md:193 says "commit minimized failures".
- Stage that file in commit 1 and list it in §6. It replays green after the fix.
- The expected text in §3.2 / §7 row 2 is also wrong in form. proptest 1.11.0 appends `" at {file}:{line}"` without a column (`proptest-1.11.0/src/sugar.rs:757`). The line reads `... right: \`true\` at src/quota/tests.rs:454`, followed by `minimal failing input: steps = [...]`. The line (454) comes from the `prop_assert_eq!` call site and is right.

**C3 (design risk R1 creates: the `ProjectRetention` linger race becomes a permanent tracker hold).** §8/D5 defer this, but under R1 it stops being a bounded orphan hazard.
- `release_project_entry` removes the row only when `strong_count == 2` (`sse/feed.rs:386-397`). It is called from `LiveFeed::drop` while the dropping feed's own `project_reserved` is still alive (`1068-1078`).
- Two feeds of one project that drop concurrently can both read 3. The row then stays in `by_project` with count 1. Its `admission` `OnceLock` (`feed.rs:299`) keeps E at strong count ≥ 2 **until that project opens and last-closes another feed, or the process restarts**.
- Each occurrence pins one tracker slot indefinitely. That is the "must not refuse ... until restart" class (review item 5, `quota.rs:576-578`) that position 50's "bounded tracking" forbids.
- A fix is not free. `impl FeedMemoryBudget` carries an impl-level `#[expect(clippy::unwrap_used)]` (`sse/feed.rs:331-336`), and `sse/feed.rs` is at 1,165 lines. Any fix grows that contract or needs a new lock site with its own exception, which is growth requiring an owner row.
- So D5 must be sequenced explicitly in §9 as an owner decision **before** commit 1 lands. Either (a) fix the race first, with its growth row proposed with the gate's values, or (b) accept the leak with the consequence stated in `MAX_TRACKED_PROJECTS`'s doc and the edge record.
- As written, the plan ships R1 with a known unbounded-lifetime hold.

**C4 (position 50's "debt" clause is not met; do not document the violation as design).** Commit 1's new `IDLE_EVICT_MS` doc (§4 item 5) says eviction forgives "any read or oversized-append debt the project had not yet waited out". That is true today, but the owner-adopted position says never evict debt.
- Phrase it as a known open gap: "forgives ... — open against review position 50, see D4". Do not phrase it as the contract.
- Do not record item 50 as closed in `docs/reviews/2026-09-hardening/README.md` (WP3, lines 55-58) or `edge-changes.md`. Record "live holders done; bucket debt open (D4)".
- D4's conservative variant really is growth-free:
  - `try_lock()` on the `Mutex<Bucket>` fields from the non-excepted `impl ProjectAdmission`, with no `unwrap`, where a poisoned or blocked bucket counts as owing;
  - `level` is `pub(super)` (`bucket.rs:6`);
  - it needs only a new horizon constant.
- So "no growth possible" is not the reason to defer. The only reason is the owner's choice between that variant and the exact one.

**C5 (a measurement is missing for D2).** Under R1, held entries persist for at least 905 s. At saturation, every refused first-seen `admit` therefore runs a full `retain` under the one tracker mutex (`quota.rs:571-586`) for as long as saturation lasts.
- The same mutex serializes every `tracked()`/`pressure_handle()`: every append (`submit.rs:30`), every SSE subscribe (`sse/session.rs:234`) and every memory gate (`product.rs:792`).
- S-B doubles the cap C. S-C quadruples it.
- §9 D2 says "estimate, not measured". Before the owner picks S-B or S-C, add a release-profile measurement of one all-held `retain` at 32,768 and at 65,536 entries. Any mitigation inside `admit` (for example rate-limiting the sweep) would grow `admit`'s `unwrap_used`/`excessive_nesting` contracts (`quota.rs:556-563`), so the number decides the cap and is not a later tweak.

**C6 (the Loom model overstates what it checks).**
- The model's `charge()`/`discharge()` neither read nor affect `retained`, so "runs the actual `charge`/`discharge` transitions" (§4 item 4) claims coverage the model does not have.
- loom's `strong_count` is an `Inspect` branch that synchronizes SeqCst (`loom-0.7.2/src/rt/arc.rs:144-152`), while std reads the count without that synchronization. The model therefore cannot witness an ordering bug. Soundness rests on the value argument alone: once the count reads 1 under the lock, no clone can follow except through the locked map. Say that in the model header and in `pin.rs`'s module doc.
- Either delete the decorative calls, or retire the now-vestigial `CounterWord` generic, whose only stated purpose (`pin.rs:16-17`) was to model the counted hold. That is the complexity-removed answer RUST-QUALITY.md:197 asks for. Or keep it and say why.
- §3.4 names "NV2 and NV4" as the model's controls, but NV4 (§7 row 9) is the commit-2 validation control. The model's second assertion is controlled only by mutant #1 (`retained -> true`). Name that as its control, or add an NV row.

**C7 (verification hygiene).**
- The working tree carries an unrelated uncommitted edit, `src/dst/tests/security_operations.rs` (+26). `discover_changes` diffs the working tree (`verification_plan.py:44-46`, `git diff ... base --`), so run §7 rows 11-13 on a clean tree or with that file stashed. Otherwise `changed_rust_files` and the gate's growth report include it.
- `origin/slate` is `d255ad6d`, not `1d079e01`. §7's `QUALITY_BEFORE_SHA=$(git rev-parse origin/slate)` is still right.
- `validation_tests.rs` will likely be listed under `production_unchanged_files`, not only `changed_rust_files`: every top-level item has an explicit `#[cfg(test)]` (lines 6, 42, 467). This is harmless.

**C8 (minor citations and docs).**
- The `HANDLE_IDLE_EVICT_SECS` doc is `cli.rs:266-269`; 265 is blank, and `0 = never` is at 266-267.
- `README.md:47` credits `d4d631df` with the tracker-race fix. The docs follow-up must say that R1 replaced its `admitting` counter while keeping its tests (`a_looked_up_entry_...`, `a_refused_admission_...`) and its `owners.json` rows (`crate::a_looked_up_entry_survives_a_sweep_before_its_charge`: scope and yield, unchanged).
- Commit 2 adds a boot refusal. Add a line to the RUNBOOK.md knob table (`:108`) or say explicitly why not.

**C9 (sizing statement).** "At least 905 s" holds for a single last append into a clean stream. `due_streams` keys on `p.since`, the first pending append (`history.rs:694`), so a stream that was already dirty is absorbed earlier and held for at least ~605 s after its last append. `HANDLE_MAX_RESIDENT` displacement (`shard.rs:2396-2412`) can shorten either figure. The tripwire models the first-seen single-append case, which is the right case for the ceiling. The `MAX_TRACKED_PROJECTS` doc should say "at least 905 s for a first-seen single append", not present it as a general lower bound.

**Verdict: ready-with-corrections.**
- C1 and C2 are mechanical but must be applied before any red is captured: C1's red is otherwise vacuous.
- C3 and C4 do not change the code of commit 1. They change what may be claimed and what the owner must decide first: D5 before R1 lands, and D4 before item 50 is recorded as closed.
- C5 is a measurement that the D2 decision needs.
