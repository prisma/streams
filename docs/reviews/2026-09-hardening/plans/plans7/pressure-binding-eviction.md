# Item 50: tracker eviction orphans a zero-debt StreamPressureBinding

Tree: `slate` @ `7c4f8606` (origin/slate = `9b2f53bc`; neither unpushed commit touches `src/quota*` or `docs/MULTITENANCY.md`). This plan is read-only: nothing below has been run.
Reviewer text: robustness-maintainability-review.md §50 (lines 1208-1218).

## 1. Problem (checked against the current tree)

**The eviction predicate reads counters, not holders.** In `src/quota.rs:543-551`, `QuotaRegistry::admit` sweeps the full tracker with:

```rust
m.retain(|_, a| {
    a.inflight.load(Ordering::Relaxed) > 0
        || a.live_subs.load(Ordering::Relaxed) > 0
        // Round-13: outstanding memory pressure
        // pins the entry — eviction would
        // orphan feed/body/frame attribution.
        || a.has_pressure()
        || now_ms - a.last_seen_ms.load(Ordering::Relaxed) < IDLE_EVICT_MS
});
```

`has_pressure` (`quota.rs:148-158`) is a pure counter test:

```rust
fn has_pressure(&self) -> bool {
    self.live_feeds.load(Ordering::Relaxed) > 0
        || self.retained_sse_bytes.load(Ordering::Relaxed) > 0
        || self.buffered_body_bytes.load(Ordering::Relaxed) > 0
        || self.queued_bytes.load(Ordering::Relaxed) > 0
        || self.unabsorbed_frame_bytes.load(Ordering::Relaxed) > 0
        || self.dirty_streams.load(Ordering::Relaxed) > 0
}
```

**A live binding can hold its entry while every counter reads zero.** `StreamPressureBinding` owns an `Arc<ProjectAdmission>` (`quota.rs:316-319`). `bind(adm, 0)` touches no counter (`quota.rs:323-328`: `if seed_unabsorbed > 0 { ... }`). `frames_retired` down to zero subtracts the last bytes and the dirty-stream count (`quota.rs:369-374`). From then on the binding still holds the Arc, but `has_pressure()` is false. Once the project has been idle for `IDLE_EVICT_MS = 300_000` (`quota.rs:52`), the next first-seen project that finds the tracker full evicts the entry.

**The handle keeps the dead entry, because the binding is never rebound.** `src/shard.rs:685-697`:

```rust
pub(crate) fn bind_pressure(&self, adm: std::sync::Arc<crate::quota::ProjectAdmission>) {
    if self.pressure.get().is_some() {
        return;
    }
    let st = self.state.lock().unwrap();
    let _ = self.pressure.get_or_init(|| { ... StreamPressureBinding::bind(adm, st.applied.unabsorbed_bytes) ... });
```

When the project returns, `admit` inserts a fresh entry (`quota.rs:559-582`). `src/application/append/submit.rs:30-34` passes that new entry to `h.bind_pressure(adm)` on every append, and the call returns early. The committer then charges the old binding: `src/shard/transaction/publish.rs:45-48`, `if let Some(b) = local.handle.pressure.get() { b.frames_added(..); b.frames_retired(..); }`.

The write gate reads the new entry. In `src/product.rs:792` it is `let adm = state.quotas.pressure_handle(&p.project_id)?;` followed by `adm.memory_gate(..)`. `memory_pressure_json` (`quota.rs:909-951`) walks only the map. So this stream's unabsorbed bytes and its 64 KiB dirty-stream weight are invisible to the per-project backstop and to `/v1/debug/load`. An absorber stall on that stream is exactly the case the round-13 binding doc (`quota.rs:311-313`) says must never be missed.

**The orphan lasts as long as the stream stays active.** The handle outlives the entry's idle window: `HANDLE_IDLE_EVICT_SECS` defaults to 600 (`src/config/cli.rs:270`, pinned at `src/config/tests.rs:311`), against 300 s of tracker recency. The handle is swept only on the 5 s flush ticker (`shard.rs:1698`, `1726-1728`), and only when `strong_count == 1` and it is idle (`shard.rs:2443-2446`). Each append refreshes it: `stream_handle` stores `last_touch_ms` (`shard.rs:2376-2379`), and submit calls it on every append. So once a project has returned inside the handle's window, the orphan persists until the stream goes quiet for 600 s or the shard moves.

**Preconditions, and why this is real but rare.** All of these must hold:

- the tracker holds 16,384 entries when a new project arrives;
- the project has made no admission for at least 300 s and all its bindings are at zero debt;
- its stream handle is still resident, meaning it was touched less than about 605 s ago;
- the project comes back while that handle is still resident.

This matches the reviewer's "impact low".

**The same predicate has a second, narrower hole.** `admit` releases the map lock at the end of the lookup block (`quota.rs:531-586`). Only then does it store `last_seen_ms` (`quota.rs:588`) and increment `inflight` (`quota.rs:609`/`615`). Between those points the request holds a clone of the entry while every counter reads zero and `last_seen_ms` is still old. A concurrent first-seen admit that sweeps in that window evicts the entry. The admitted request then gets `None => return Err(QuotaRefusal::TrackerCapacity)` from `admit_append` (`quota.rs:649-651`, a 503) and `None` from `pressure_handle`, so it skips the body charge and the memory gate. The same fix closes this: the request's clone is a holder.

**Verdict on the reviewer's Change.**

- *"Retain on `Arc::strong_count(a) > 1 || recent`; delete `has_pressure`."* This is the right predicate. Every legitimate pressure counter is maintained by a holder of the Arc:
  - `QuotaGuard` (`quota.rs:476-484`) for `inflight`;
  - `SubscriptionGuard` (`489-497`) for `live_subs`;
  - `FeedPressureGuard` (`257-272`) for `live_feeds`;
  - `ProjectRetention.admission` (`sse/feed/retention.rs:9-21`, alive while feeds are) for `retained_sse_bytes`;
  - `BufferedBodyGuard` (`278-305`);
  - `QueuedBytesGuard` (`441-452`);
  - `StreamPressureBinding` (`316-404`);
  - `StreamReservation` (`415-437`).

  A holder therefore implies `strong_count >= 2`. `strong_count == 1` observed under the map lock is final: `git grep` finds no `Weak<ProjectAdmission>`, and a new holder can only clone from the map (`admit`, `admit_append`, `tracked`), which takes that lock. The same argument is already used for handle eviction (`shard.rs:2441-2451`). The `inflight`/`live_subs` terms must go as well, not only `has_pressure`. They are implied by the holder test, so keeping them leaves equivalent `||`→`&&` mutants that the mutation leg cannot kill.
- **Written inline, the reviewer's Change breaks the source ratchet.** `admit` carries a function-wide `#[expect(clippy::unwrap_used, reason = "QuotaRegistry::admit; ...")]` (`quota.rs:517-520`). Inline, `Arc::strong_count(a)` would add a new `call-site` fingerprint and a new `path` fingerprint (`std::sync::Arc::strong_count`) under that scope (`scripts/quality/source_rules.py:176-187`, `tools/quality-syntax/src/scan.rs:222-252`). `exception_growth` would then fail with `accepted exception grew without a new decision: ... unwrap_site:ordinary-call:...: 0 -> 1`, and the only way through is re-deciding the reason text. This plan does not do that. Instead the predicate becomes an owner method, `ProjectAdmission::pinned(self: &Arc<Self>, now_ms)`, which replaces `has_pressure` one-for-one outside any exception scope. `admit`'s closure becomes one method call. Method-call facts are not fingerprinted, and `admit`'s `scope_lines` and `syntax_facts` both shrink (§4).
- *"Rewrite the MAX_TRACKED_PROJECTS sizing docs (600s -> 12,000 entries)."* Correct in substance. The precise horizon is 600 s of handle idle plus up to one 5 s ticker pass, which is 605 s and 20/s × 605 s = 12,100 entries. That is still under 16,384.
- *"Pin with an extended cert_rotation test."* The existing test cannot pin this. `cert_rotation_over_ten_thousand_tenants_never_hits_tracker_capacity` (`src/quota/tests.rs:25-46`) rotates a 10,000-project population, and 10,000 is below the 16,384 cap, so it never sweeps. A sibling test is needed, with a first-seen population larger than the cap, where every project holds a real binding for the handle horizon (§3b).

## 2. Contract decision

- **Typed contract.** `QuotaRefusal` is unchanged, and so are all wire codes and bodies. The retention invariant becomes: *an entry is evictable only when nothing but the tracker holds it (strong count 1 under the tracker lock) and it has been idle for at least `IDLE_EVICT_MS`*. The eviction clause of the idle-horizon boundary is unchanged: at exactly 300 s the entry is evictable. The new red test pins that.
- **What changes at the edge (a decision for Søren).** The change is in incidence only. When the tracker is saturated, a *new* project can now receive the existing retryable `503 project_tracker_capacity` in a case where the current tree would instead evict an idle entry whose streams are still resident with zero debt. At the certified rotation, steady-state tracker demand rises from 20/s × 300 s = 6,000 to 20/s × 605 s ≈ 12,100 entries, against the 16,384 cap. Headroom falls from 63% to 26%. The new sizing test pins this. The operator-visible `/v1/debug/load` `admission.trackedProjects` (`src/http.rs:1075-1079`) reads correspondingly higher. No response shape changes.
- **Also removed.** A per-project counter that drifted with no holder was pinned forever by `has_pressure`. With nothing holding it, it is now evicted after the idle horizon. This change is only for the better.
- **Backward-compatible alternative (not recommended).** Keep today's eviction and occupancy, and make the handle follow the entry instead:
  - `StreamHandle.pressure` becomes a swappable cell instead of `OnceLock`;
  - `bind_pressure` re-points a binding whose entry is not `ptr_eq` to the current one, transferring `current_unabsorbed` under the state lock;
  - `publish.rs` reads it under the same lock.

  Costs:
  - it is a synchronization change in `src/shard.rs`, which is under the 3,232 ceiling at 3,197 now, and in `src/shard/transaction/publish.rs`, so Loom is selected (lifecycle prefix) along with the shard mutation owners;
  - it leaves the admit-window race (§1) open;
  - it leaves any future zero-counter holder unprotected.

  A middle option, a `bound_streams` counter checked by `has_pressure`, has the same occupancy effect as the recommended change and fixes less, so it is rejected.
- **Cap stays 16,384 (a decision for Søren).** The sizing test fails first if `HANDLE_IDLE_EVICT_SECS`'s default ever exceeds about 814 s, because 16,384 / 20 = 819 s minus the 5 s ticker. The alternative is to raise the cap now to 32,768, about 16 MiB worst case at about 500 B per entry.

## 3. Red tests

### 3a. Behaviour red (compiles on 7c4f8606, fails there)

**`quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding`**. File `src/quota/pressure_tests.rs`, inserted directly after battery 12 (after line 190). Every symbol it uses is already imported there (line 3-7).

```rust

/// Battery 12b (review item 50): a stream's pressure binding pins its
/// entry even at zero debt. The handle's OnceLock never rebinds, so an
/// evicted entry would keep taking that stream's frame debt after the
/// project returns — debt the live entry's memory_gate never reads.
/// The sweep runs exactly at the idle horizon, where idle peers are
/// already evictable.
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

On the current tree:

- elapsed = 300,000, so `300000 < 300000` is false and every entry is idle;
- f7 has no counters, so all 16,384 entries are evicted, `fresh` is admitted, and `f7` gets a new entry;
- `frames_added` charges the orphan.

**Exact expected red** (`cargo test --locked --lib quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding -- --exact`; the line is 221 if the block above is inserted verbatim after line 190):

```text
---- quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding stdout ----

thread 'quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding' panicked at src/quota/pressure_tests.rs:221:5:
assertion `left == right` failed: frame debt the bound stream commits must reach f7's live entry
  left: 0
 right: 65536
```

On the fixed tree, f7's strong count is 2 (map plus binding), so it is pinned. Only f7 and `fresh` survive, the re-admit finds the bound entry, and the test reads 65536.

### 3b. Sizing guard (passes on both trees; pins the new occupancy, not red by design)

**`quota::tests::first_seen_rotation_holding_stream_bindings_never_hits_tracker_capacity`**. File `src/quota/tests.rs`, added after the cert_rotation test. The import list becomes `use super::{IDLE_EVICT_MS, MAX_TRACKED_PROJECTS, ProjectId, ProjectQuotas, QuotaRefusal, QuotaRegistry, StreamPressureBinding};` (rustfmt reflows it), plus `use std::collections::VecDeque;`.

```rust
/// A first-seen population larger than the cap, where each project
/// binds one stream and the binding holds its entry until the handle
/// idles out (HANDLE_IDLE_EVICT_SECS after the last touch, swept by the
/// 5 s flush ticker): 20 new projects/s for 1,200 s means 20/s x 605 s
/// = 12,100 held entries at steady state, across two full sweeps. A
/// handle horizon past ~814 s leaves nothing evictable at the first
/// sweep and refuses here — the cap must move with it.
#[test]
fn first_seen_rotation_holding_stream_bindings_never_hits_tracker_capacity() {
    let r = QuotaRegistry::default();
    let quotas = ProjectQuotas::default();
    let idle_secs = crate::config::CliArgs::deterministic().handle_idle_evict_secs;
    let held_ms = i64::try_from(idle_secs * 1_000).expect("handle horizon fits i64") + 5_000;
    let t0: i64 = 1_000_000;
    let mut resident: VecDeque<(i64, StreamPressureBinding)> = VecDeque::new();
    let mut refused = 0usize;
    for i in 0..24_000i64 {
        let now = t0 + i * 50; // 20 first-seen projects/s, none return
        while resident.front().is_some_and(|(at, _)| now - at >= held_ms) {
            drop(resident.pop_front()); // the handle idled out: its binding goes
        }
        let project = pid(&format!("first_seen_{i}"));
        match r.admit(&project, &quotas, now) {
            Ok(_g) => {
                let entry = r.pressure_handle(&project).expect("admitted project is tracked");
                resident.push_back((now, StreamPressureBinding::bind(entry, 0)));
            }
            Err(QuotaRefusal::TrackerCapacity) => refused += 1,
            Err(e) => panic!("unexpected refusal {e:?}"),
        }
    }
    assert_eq!(
        refused, 0,
        "resident stream bindings must not exhaust the tracker at cert pacing"
    );
}
```

rustfmt decides the final line breaks in this block. Two chains are longer than 60 characters, `let entry = ...` and `let held_ms = ...`, and it will split them. The §3a block is already in rustfmt shape, so its line 221 holds.

Reasoning:

- The first sweep happens at 819.2 s (16,384 × 50 ms). At that point 12,100 entries are held, so 4,284 are evicted. The second sweep is at about 1,033 s.
- `CliArgs::deterministic()` is proven equal to the scrubbed clap defaults by `config::tests::cli_fixture_matches_scrubbed_parse`, so the test follows the real `HANDLE_IDLE_EVICT_SECS` default.
- Nesting is at most 3, and the function is about 30 lines.
- Also edit the stale sentence in the cert_rotation doc (`tests.rs:22-24`: "With IDLE_EVICT_MS of un-evictable recency, steady-state demand is 20/s x 300s = 6,000 tracked entries"). It becomes: an entry is un-evictable while recent or held (a resident stream binding holds it about 605 s), so steady-state demand is about 12,100 entries. The test body is unchanged.

## 4. Edits, file by file, in commit order

**No verbatim-move commit is needed.** No ceilinged file is touched. Current `wc -l` and budgets:

| File | Lines now | Budget / limit | Touched? |
|---|---|---|---|
| `src/quota.rs` | 975 | ≤ 1,000 (crossing 1,000 is blocked); after: 969 | yes |
| `src/quota/pressure_tests.rs` | 220 | ≤ 1,000; after: 256 | yes |
| `src/quota/tests.rs` | 307 | ≤ 1,000; after: about 345 | yes |
| `src/shard.rs` | 3,197 | 3,232 | no |
| `src/http.rs` | 3,371 | 3,371 | no |
| `src/product.rs` | 4,205 | 4,205 | no |
| `src/billing.rs` / `src/history.rs` / `src/auth.rs` / `src/registry.rs` / `src/sse/feed.rs` / `src/fleet.rs` | 2,201 / 1,713 / 1,676 / 1,501 / 1,200 / 1,143 | unchanged ceilings | no |

**Single commit**, with the red run (§3a) captured against the pre-fix `src/quota.rs` before the fix hunk is applied. Suggested subject: *"A tracker entry anything still holds is never evicted; a fully absorbed stream binding no longer orphans later frame debt"*. End the message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

1. **`src/quota/pressure_tests.rs`**: add §3a. This is a test-only file (`#![cfg(test)]`, so it is `production_unchanged` and needs no owner row).
2. **`src/quota/tests.rs`**: add §3b, extend the import, and fix the cert_rotation doc sentence (test-only file).
3. **`src/quota.rs`** (975 → 969):
   - **`MAX_TRACKED_PROJECTS` doc** (lines 32-45, 14 → 18 lines, +4). Rewrite the sizing paragraph:
     - an entry stays un-evictable while it is recent or held (`ProjectAdmission::pinned`, in code font, not a link);
     - the longest ordinary holder is a resident stream's pressure binding, whose handle idles out `HANDLE_IDLE_EVICT_SECS` (600 s) after its last touch, on the 5 s flush ticker;
     - the rotation therefore holds 20/s × 605 s = 12,100 entries, and `quota/tests.rs` pins that shape;
     - keep the history: the old 4,096 cap shed 19% at 300 s on the 2026-08-19 churn rung;
     - 16,384 holds it with about 26% headroom and the 10k population outright, at about 8 MiB;
     - the hard-ceiling and fail-closed paragraph stays, with "never-evict-active" becoming "never-evict-held".
   - **`IDLE_EVICT_MS` doc** (lines 48-51, 4 → 4). "(and no inflight work)" becomes "and held by nothing but the tracker". The value is unchanged.
   - **Replace `has_pressure`** (lines 148-158, 11 → 12 lines, +1) in the `impl ProjectAdmission` block, which has no exception attribute:

     ```rust
     /// An entry anything still holds is never evicted. Every holder
     /// charges the Arc it already has, and a resident stream's binding
     /// is never rebound (its handle's OnceLock keeps the first entry),
     /// so evicting a held entry sends that holder's later debt to an
     /// entry memory_gate no longer reads, even while every counter
     /// reads zero. A count of one under the tracker lock is final: no
     /// Weak exists, and a new holder can only clone from the map,
     /// under that lock.
     fn pinned(self: &Arc<Self>, now_ms: i64) -> bool {
         Arc::strong_count(self) > 1
             || now_ms - self.last_seen_ms.load(Ordering::Relaxed) < IDLE_EVICT_MS
     }
     ```

     `self: &Arc<Self>` is a stable receiver. From the `retain` closure (`a: &mut Arc<ProjectAdmission>`), auto-deref reaches `Arc<_>` and autoref gives `&Arc<_>`. `needless_arbitrary_self_type` does not apply to `&Arc<Self>`. The function is private, called by `admit`, so there is no dead_code or unreachable_pub.
   - **`QuotaRegistry::admit`** (lines 537-551, 15 → 4 lines, −11). Keep the three "Review item 5: EVICT idle entries before refusing..." comment lines. Delete the "Never evict a project with inflight requests..." comment and the whole multi-line closure, and write:

     ```rust
     m.retain(|_, a| a.pinned(now_ms));
     ```

   **Every `#[expect]`-ratcheted function touched, and the remedy:**

   - `QuotaRegistry::admit` has function-wide `#[expect(clippy::unwrap_used)]` (fingerprinted) and `#[expect(clippy::excessive_nesting)]`. Neither reason changes, and neither scope grows:
     - `scope_lines` falls by 11;
     - `syntax_facts` falls by 13 in the closure. Before: 4 `method-call` + 4 `method-call-site` + 9 `path` (`a`×4, `Ordering::Relaxed`×3, `now_ms`, `IDLE_EVICT_MS`). After: 1 + 1 + 2 (`a`, `now_ms`);
     - no new `unwrap_site:ordinary-call:*` or `unwrap_site:path:*` fingerprint: the `now_ms` path count is unchanged, `a` falls by 3, and `Ordering::Relaxed` and `IDLE_EVICT_MS` disappear;
     - the `unwrap_site` fingerprints are untouched.

     Both expects stay fulfilled:
     - the six `.lock().unwrap()`s are unchanged;
     - `excessive_nesting` fires at the unchanged outer `if m.len() >= MAX_TRACKED_PROJECTS {` block. By the clippy visitor, the nesting there is impl(1) → fn body(2) → `let admission = {`(3) → `None => {`(4) → if-block(5) > 4, and the visitor stops descending there. So removing the closure's braces cannot unfulfil the expect.
   - `ProjectAdmission::pinned` is new and carries no exception. `has_pressure` is deleted and carried none.
   - No other function with an `#[expect]` changes. `StreamPressureBinding`, `bind_pressure` (`shard.rs`), `publish` (`publish.rs`) and `LiveFeed::bind_pressure` are all untouched.
4. **`docs/MULTITENANCY.md:1403-1408`** ("Bound project trackers"). Keep the 6,000 / 4,096 / 19% history. Add that, since review item 50, an entry is also un-evictable while anything holds it, including a resident stream's pressure binding until its handle idles out (`HANDLE_IDLE_EVICT_SECS` = 600 s plus the 5 s ticker). The same rotation therefore holds about 12,100 entries under the 16,384 cap. The file is not hash-pinned (no hit in `docs/quality/*.json`, `docs/refactor/*.json` or `scripts/`).

## 5. Mutation-kill analysis

The owner is the existing row `owner('quota_registry', 'src/quota.rs', 'quota::')` (`scripts/quality/mutation_owners.py:98`), with filter `quota::`. It covers every test below:

- `src/quota/{tests,pressure_tests}.rs` are the `quota::tests::` and `quota::pressure_tests::` modules;
- both are `#![cfg(test)]`, so `production_changes.normalized_source` returns `''` for them (`test_only_file`). They land in `production_unchanged_files` and need no row.

No owner row or filter changes. The `--in-diff` mutants for `src/quota.rs`, following cargo-mutants 27.1's operator set as seen in earlier runs (`<`→`==`,`>`,`<=`; `>`→`==`,`<`,`>=`; `||`→`&&`; `-`→`+`,`/`):

| Mutant (in `ProjectAdmission::pinned` unless noted) | Killed by |
|---|---|
| body → `true` | §3a: `fresh` refused, `.expect("the sweep evicts idle peers at the horizon")` panics. Also `tracker_evicts_idle_projects_never_active_ones` (`assert!(... IDLE_EVICT_MS + 1).is_ok())`) and `eviction_cannot_remove_a_project_with_pressure` (`f8 ... is_none()`). |
| body → `false` | §3a (f7 evicted: `left: 0`), `tracker_bound_refuses_new_projects_only`, `tracker_capacity_churn_evicts_idle_never_active` |
| `strong_count > 1` → `== 1` | §3a: f7 (held) is evicted and every unheld entry is pinned, so f7's re-admit gets TrackerCapacity and `.expect("f7 is admitted after the sweep")` panics. Also `tracker_evicts_idle_projects_never_active_ones` (`tracked == 2` fails). |
| `> 1` → `< 1` | §3a (holder test never true, so f7 is evicted: `left: 0`), `tracker_capacity_churn_evicts_idle_never_active` ("pinned project was evicted (fresh entry admitted)") |
| `> 1` → `>= 1` | §3a (all pinned, so `fresh` is refused), `tracker_evicts_idle_projects_never_active_ones` |
| `\|\|` → `&&` | §3a (f7 is held but idle, so it is evicted: `left: 0`), `tracker_bound_refuses_new_projects_only` (recent unheld entries evicted, so `p_new` is admitted) |
| `now_ms - last` → `+` | `tracker_capacity_churn_evicts_idle_never_active`: t0 = 1,000,000, so the sum is always ≥ 300,000 and nothing is recent. `churn_{16384}` is admitted at t0+1,000, giving "expected TrackerCapacity, got admission". This is the only killer; §3a has the same verdict both ways. |
| `-` → `/` | §3a (301,000 / 1,000 = 301 counts as recent, so everything is pinned and `fresh` is refused); `tracker_evicts_idle_projects_never_active_ones` (last_seen 0, so the division by zero panics) |
| `< IDLE_EVICT_MS` → `==` | §3a (elapsed is exactly the horizon, so everything is pinned and `fresh` is refused); `tracker_bound_refuses_new_projects_only` |
| `<` → `>` | `tracker_bound_refuses_new_projects_only` (elapsed 0 counts as not recent, so `p_new` is admitted); `tracker_evicts_idle_projects_never_active_ones` (at `IDLE_EVICT_MS - 1`) |
| `<` → `<=` | **§3a only**: sweeping at exactly `old_ms + IDLE_EVICT_MS` is the boundary kill. No existing test sweeps at the exact horizon. |
| `QuotaRegistry::admit` → `Ok(Default::default())` (FnValue, span overlaps the edited body) | Unviable: `QuotaGuard` has no `Default`. Unviable is not a miss. |

There are no equivalent mutants: the subsumed `inflight`/`live_subs` terms are deleted, not kept. No new match guard exists. The closure `a.pinned(now_ms)` has no operator.

**Timeouts.** The driver uses `--timeout 90` and `--profile quality` (opt-level 1). The worst never-evict mutants (`true`, `>=`, `/`) make §3b sweep 16,384 entries on each of about 7,600 refused admits, roughly 125M trivial closure calls, about 1 s. That is well inside 90 s. The existing churn test adds about 33M. Every loop in both new tests is bounded; there are no waits.

## 6. Ledgers

- `docs/refactor/test-inventory.json`: unchanged. `scripts/test-inventory.py` inventories DST tests only, and both new tests are unit tests. `--check` must stay clean.
- `docs/quality/owners.json`: unchanged. No global static, by-path module, `use super::*`, `tokio::select!`/`json!`/proptest macro, or spawn is added. The existing quota rows are the `macro-dsl` `serde_json::json` rows in `source-allowances.json`, keyed by owner function (`ProjectAdmission::memory_gate`, `QuotaRegistry::memory_pressure_json`, `pressure_model_json`), not by line. Those functions keep their macro counts; they only shift lines.
- `docs/quality/source-allowances.json` and the diagnostic allowances: unchanged. The dormant `unreachable_pub` legacy rows for `crate::MAX_TRACKED_PROJECTS` and `crate::IDLE_EVICT_MS` in `legacy-diagnostics*.json` do not fire, because the constants are `pub(crate)`. The doc edits do not touch them.
- `docs/refactor/architecture-policy.json`: unchanged (no new file, no `crate::http` reference).
- `docs/refactor/WIRE-MATRIX.md`: unchanged. The `503 project_tracker_capacity` row (line 85) already lists the code, and its shape is unchanged; only its incidence changes (§2).
- `docs/refactor/review-mechanisms.json`: unchanged. No pinned test body is touched; its quota pins are DST files only.
- `src/dst/tests/README.md`: unchanged (no DST module).
- `docs/MULTITENANCY.md`: updated in the same commit (§4.4).
- `bench/WORKLOAD-CERT-PLAN.md:144-156`: left as the dated historical record.

## 7. Controls (exact commands, expected outputs)

Run from `/Users/sorenschmidt/code/streams`.

1. **Red** (test hunks applied, `src/quota.rs` still at 7c4f8606):
   ```
   cargo test --locked --lib quota::pressure_tests::eviction_cannot_orphan_a_zero_debt_stream_binding -- --exact
   ```
   Expected: the §3a panic (`left: 0` / `right: 65536`), then `test result: FAILED. 0 passed; 1 failed`.
2. **Guard on the old tree:**
   ```
   cargo test --locked --lib quota::tests::first_seen_rotation_holding_stream_bindings_never_hits_tracker_capacity -- --exact
   ```
   Expected: `... ok`, `test result: ok. 1 passed`, on both trees.
3. **Green** (fix applied): command 1 gives `... ok` and `test result: ok. 1 passed`.
4. `cargo test --locked --lib quota::` should be all ok, with 2 more passing than before the change. The 30 tests in `src/quota/{tests,pressure_tests,pressure_counting_tests,poison_tests}.rs` become 32, and the 5 `bucket` tests are unchanged. The existing `tracker_evicts_idle_projects_never_active_ones`, `tracker_capacity_churn_evicts_idle_never_active`, `eviction_cannot_remove_a_project_with_pressure`, `tracker_bound_refuses_new_projects_only` and the cert_rotation test stay green unmodified, because every holder they use is an Arc clone.
5. `cargo fmt --all -- --check` should print nothing.
6. `cargo clippy --locked --workspace --all-targets -- -D warnings` should report no warnings. In particular there must be no `unfulfilled_lint_expectations` on `QuotaRegistry::admit`, no `dead_code` for `has_pressure`, and no nesting finding in the new tests.
7. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` should succeed. The new docs contain no `[..]` or `<..>` outside code spans.
8. **Plan:**
   ```
   QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py --out target/quality-plan
   ```
   Expected `plan.json`, scoped to the item-50 diff. Use `QUALITY_BEFORE_SHA=7c4f8606` if the two unpushed commits are not pushed first; otherwise their owners join the plan.
   - `mutants: true`
   - `mutation_source_files: ["src/quota.rs"]`
   - `selected_mutation_owners: ["quota_registry"]`
   - `unregistered_mutation_source_files: []`
   - `production_unchanged_files` ⊇ `["src/quota/pressure_tests.rs","src/quota/tests.rs"]`
   - `properties_fuzz: true` (quota prefix: the saved corpus replay runs; no new property, §8)
   - `loom: false` (`src/quota` is not in `LIFECYCLE_PREFIXES`)
   - `miri: false`
9. **Mutants:**
   ```
   QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<same> QUALITY_MUTANTS_OUT=target/q50-mutants scripts/quality/mutations.sh
   ```
   Expected: the §5 set, with 11 caught in `pinned` plus 1 unviable (`admit` FnValue), and 0 missed, 0 timeout.
10. `scripts/quality.sh` should exit 0. The source gate must print no `file growth: src/quota.rs`, no `accepted exception grew without a new decision: ('src/quota.rs', ...QuotaRegistry::admit...`, and no `exception needs owner; invariant; alternative`. `python3 scripts/test-inventory.py --check` should be clean.
11. **Full suite:** the full `cargo test --locked` leg, as CI runs it. Nothing outside `src/quota` references `MAX_TRACKED_PROJECTS`, `IDLE_EVICT_MS` or `has_pressure` (`git grep`), and no DST scenario approaches 16,384 projects.

## 8. Out of scope

- Rebinding `StreamHandle.pressure` or replacing its `OnceLock` (the §2 alternative), and any `shard.rs`, `publish.rs` or `submit.rs` edit.
- Raising `MAX_TRACKED_PROJECTS` or `HANDLE_IDLE_EVICT_SECS`, or coupling the cap to the configured handle horizon at runtime. The sizing test is the tripwire.
- **A new 1,024-case property.** `properties_fuzz` is selected by the quota prefix, but no existing property covers the tracker sweep: the bucket proptests in `quota/bucket.rs` are unaffected and must stay green. A sweep property would have to fill 16,384 entries per case, which is about 16.8M admits per 1,024 cases. Adding one would also need a test-only cap seam, which is a new design decision.
- **Loom.** No primitive, ordering or lock is added. The predicate reads an Arc refcount under the existing map mutex, and every clone of a map entry is minted under that mutex. `verification_plan.py` does not select Loom for `src/quota`. A Loom model would need a `loom::sync` facade over `std::sync` in `quota.rs`, which is a rewritten model that RUST-QUALITY.md says does not certify.
- A dedicated test for the admit-window race (§1). It is the same predicate, and §3a's zero-counter holder exercises it. Making the interleaving deterministic would need a seam inside `admit`'s `unwrap_used` scope.
- `bench/WORKLOAD-CERT-PLAN.md` history, and the `/v1/debug/load` field names, which are unchanged.

## Skeptic corrections (C1..C7)

Checked against `slate` @ `7c4f8606`, read-only. **Confirmed as written:**

- the problem trace: `quota.rs:543-551`, `151-158`, `316-333`, `369-374`; `shard.rs:685-697`; `submit.rs:30-34`; `publish.rs:45-48`; `product.rs:792`;
- every `wc -l`: quota.rs 975; pressure_tests.rs 220; tests.rs 307; shard.rs 3,197; http.rs 3,371; product.rs 4,205; billing.rs 2,201; history.rs 1,713; auth.rs 1,676; registry.rs 1,501; sse/feed.rs 1,200; fleet.rs 1,143. No ceilinged file is touched;
- the red test really fails today. The sweep at elapsed = 300,000 evicts f7, and `frames_added` charges the orphan. The panic is `left: 0 / right: 65536` at `src/quota/pressure_tests.rs:221:5`: the blank line plus 30 lines after line 190 put `assert_eq!` on line 221. The test passes after the fix, because f7's strong count is map plus binding = 2;
- the `self: &Arc<Self>` receiver resolves from `retain`'s `&mut Arc<_>`;
- the fingerprint argument holds. `_fingerprint_sites` keys on qualified name plus value (`source_rules.py:109-113`). The ordinary-call and path fingerprints (`176-187`) would grow with an inline `Arc::strong_count`. `a.pinned(now_ms)` adds only method-call facts, and `admit`'s `scope_lines` and `syntax_facts` shrink;
- the `excessive_nesting` expect stays fulfilled. The count is impl 1 → fn body 2 → `let admission = {` 3 → `None => {` 4 → `if` block 5, which is over the threshold of 4 (`clippy.toml`). The visitor stops there;
- all 11 `pinned` mutants are killed as tabled. `<`→`<=` is killed only by §3a. `-`→`+` is killed only by the churn test. The `admit` FnValue mutant is unviable, since `QuotaGuard` has no `Default`;
- `test-inventory.py` covers DST only; `mutation_owners.py:98` has `quota_registry` / `quota::`; the planner selects `properties_fuzz` and neither loom nor miri (`verification_plan.py:25-31, 98`);
- the `legacy-diagnostics*.json` and `architecture-review-baseline.json` `src/quota.rs` hashes are historical captures (commits 5bdaf968 and a7e2070f; the current sha is 3dba275e). They are not live pins.

**C1 (blocking): the binding horizon is not 605 s after the last admission, and at defaults it is past the cap.**

`last_touch_ms` is refreshed by *every* `stream_handle` lookup (`shard.rs:2376-2379`), not only by appends. After a project's last append, the absorber re-touches the resident handle when the stream comes due:

- `due_streams` (`history.rs:693-704`) admits a stream once `now - since >= threshold_age`, where `threshold_age = ABSORB_AGE_SECS`, default 300 (`config/cli.rs:581`, `bootstrap.rs:101`);
- `classify_due` then calls `self.shard.stream_handle(hash)` (`history/worker.rs:250`);
- gather `plan_reads` calls it again (`history/gather.rs:375`);
- the committer also re-touches it for TrimStep, UsageAck, BillingClose and BillingRetained (`shard/transaction/mod.rs:140-152`).

The ticker also awaits `flush_with_options` before each eviction pass (`shard.rs:1710-1728`), so the pass is 5 s plus flush latency.

A small appender's binding therefore lives about 300 + 600 + 5 ≈ **905 s** or more after its last append. That is 20/s × 905 s ≈ **18,100 entries, above 16,384**. Consequences:

- §2's "12,100 / 26% headroom / cap stays 16,384" is wrong. Headroom is negative for the first-seen model the plan itself chooses to pin.
- §3b as written (`held_ms = handle_idle_evict_secs*1000 + 5000`) is a false-green tripwire. It passes while production at default config would refuse.
- With the true horizon, the §3b model *refuses on the fixed tree*.
- It also widens §1's orphan window from 300→605 s to 300→905 s, so the bug is more likely than stated.

Fix the plan:

1. Model `held_ms = (absorb_age_secs + handle_idle_evict_secs) * 1000 + 5_000`. Read both from `CliArgs::deterministic()`.
2. Re-decide the cap for Søren:
   - (a) raise `MAX_TRACKED_PROJECTS` to 32,768 (about 16 MiB). §3b then needs 16,384 × 2 / 20 s ≈ 1,640 s before the first sweep, so about 48,000 iterations. Never-evict mutants then cost about 15k × 32k ≈ 500M closure calls, still well under `--timeout 90`, but state it;
   - (b) state that first-seen churn above 16,384 / 905 s ≈ 18/s now gets 503 `project_tracker_capacity`. Today that threshold is about 54/s (16,384 / 300 s);
   - (c) take the §2 rebind alternative instead.
3. Update the `MAX_TRACKED_PROJECTS` doc and the `docs/MULTITENANCY.md:1403-1408` edit with the corrected horizon.

**C2 (blocking): `HANDLE_IDLE_EVICT_SECS=0` means "never", and it would make the tracker monotone.**

The flag documents `0 = never` (`config/cli.rs:265-271`). `evict_idle_handles` skips time eviction when `idle.is_zero()` (`shard.rs:2440`). Handles then leave only through `HANDLE_MAX_RESIDENT` (65,536 per shard, and only oldest-first above the cap), shard close or owner movement.

Under the fix, every project with a resident bound handle stays pinned indefinitely. After 16,384 such projects, every new project gets 503 until restart or shard movement. That breaks the invariant stated at `quota.rs:537-539` ("a tracker that filled once must not refuse project 1,025 until restart").

§3b computes `held_ms = 5_000` at 0 and passes vacuously. Fix the plan:

- §3b must refuse a 0 horizon, with `assert!(idle_secs > 0, ...)` or by treating 0 as unbounded and failing;
- §2 must list the new coupling as part of the contract decision: tracker occupancy is now bounded by handle-residency knobs (`HANDLE_IDLE_EVICT_SECS`, `ABSORB_AGE_SECS`, `HANDLE_MAX_RESIDENT`).

**C3: a documented contract breaks that the plan does not edit.**

`HANDLE_IDLE_EVICT_SECS`'s doc says eviction "only trades a tail-row read for memory" (`src/config/cli.rs:265-269`, 663 lines, uncapped). `docs/COST-CAMPAIGN-2.md:266-267` makes the same claim. After the fix, the knob also sets tracker occupancy and the 503 incidence.

Add a sentence to the cli.rs doc comment. It does not change the clap default, so `config/tests.rs:311` is unaffected. Either note the change in COST-CAMPAIGN-2 or leave that file explicitly as history.

**C4: §2's "certified rotation" number is mislabelled.**

The certified rotation is 10,000 tenants with each tenant returning every 500 s (`quota/tests.rs:19-24`). Each return re-touches the handle well inside 905 s, so under the fix all 10,000 stay pinned permanently: occupancy equals the population, not 12,100. That fits under 16,384.

The 12,100 (really about 18,100) figure is an unbounded first-seen model. Say which model each number describes, in §2, the cert_rotation doc rewrite (`tests.rs:22-24`) and the MULTITENANCY.md sentence.

**C5: the property requirement is dismissed on a wrong premise.**

RUST-QUALITY.md:153 ("Codec, index or admission changes") requires 1,024-case properties for affected admission properties. §8 says a sweep property "would need a test-only cap seam". It would not: the predicate is now an isolated method.

A buildable property draws `(extra_holders ∈ 0..3, elapsed ∈ IDLE_EVICT_MS±2)`. It admits one project in a fresh registry, clones holders via `pressure_handle`, and asserts `pinned == (extra_holders > 0 || elapsed < IDLE_EVICT_MS)`. It needs no fill, no seam, and costs microseconds per case. The child module can call the private method.

Either add it or record explicitly why no "affected property" exists. If added, it needs a `docs/quality/owners.json` `macro-dsl` row (`owner: crate::macro(proptest)`, path `src/quota/pressure_tests.rs`, same reason text as `owners.json:70-74`).

**C6: minor accuracy.**

- The handle horizon is not "600 s plus up to one 5 s ticker pass". The ticker's `flush_with_options(...).await` runs before the eviction pass (`shard.rs:1710-1725`), so the bound is 5 s plus flush latency. Word the docs as "≥".
- §1's precondition "touched less than about 605 s ago" should read "last touched, by any handle user including the absorber, less than `HANDLE_IDLE_EVICT_SECS` + one tick ago".

**C7: the Loom exemption is acceptable but should cite its precedent.**

The fix changes a retirement predicate. RUST-QUALITY.md:154 covers retirement changes. The planner does not select Loom for `src/quota`, and the same refcount-under-map-lock argument already backs handle eviction (`shard.rs:2419-2446`). Say that explicitly in §8 rather than only "no primitive added".

**Verdict: ready-with-corrections.** The code fix, the §3a red test, the ratchet analysis and the mutation table are sound and buildable. Before implementation, C1 and C2 must change the §2 cap/contract decision and the §3b tripwire. As written, the sizing test would be green while the default configuration refuses first-seen churn above about 18/s, and `HANDLE_IDLE_EVICT_SECS=0` would make the tracker monotone.
