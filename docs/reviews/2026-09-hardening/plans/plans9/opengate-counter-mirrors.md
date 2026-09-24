# Item 67: OpenGate owns its open counters, and tests read what the operator reads

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `fba7b844`, merge base `origin/slate` = `fb18840d`.
This plan comes from read-only analysis. Nothing was built or run.

**Summary.** The reviewer's Change is correct and can be built. This plan follows it, with four refinements the gates force:

1. `store_timing::snapshot` gets the directory's counters as a `&serde_json::Value` parameter. It no longer reads a process global. Its two callers pass `state.shards.open_stats()`.
2. That call-site edit in `http.rs` selects two mutants: `debug_store`'s whole-body mutant and the `==` on the line above the edit, `?swap=1`. No test in the `http` owner's filters requests `/v1/debug/store`. One new DST router test kills both. It is also the behavioural red test. Its name is added to the `http` owner row.
3. Two ratcheted `unwrap_used` exceptions grow their path fingerprints, so their reason text must be re-decided: `OpenGate::get_or_open` and `store_timing::snapshot`.
4. The test with a ratcheted `#[expect]` (`a_hung_open_…`) asserts through a small `open_counts` helper. Its scope therefore shrinks.

The change is one commit. There is no wire change.

---

## 1. Problem (verified on HEAD `fba7b844`)

### 1.1 Seven process statics feed the operator JSON (`src/sharddir.rs:63-74`, `:195-205`)

```rust
// Process-global counters for /v1/debug/store: the cloud-run detector for
// this failure mode is "opens_started climbing while the serving map stays
// empty", and it must be visible without logs.
static OPENS_STARTED: AtomicU64 = AtomicU64::new(0);
static OPENS_COMPLETED: AtomicU64 = AtomicU64::new(0);
static OPENS_FAILED: AtomicU64 = AtomicU64::new(0);
static OPENS_COALESCED: AtomicU64 = AtomicU64::new(0);
static OPENS_IN_FLIGHT: AtomicI64 = AtomicI64::new(0);
static OPENS_DEADLINED: AtomicU64 = AtomicU64::new(0);
/// Abandoned opens that eventually completed and were closed by the
/// reaper instead of installed.
static OPENS_REAPED: AtomicU64 = AtomicU64::new(0);
...
pub(crate) fn stats_json() -> serde_json::Value {
    serde_json::json!({
        "started": OPENS_STARTED.load(Ordering::Relaxed),
        ... "in_flight": OPENS_IN_FLIGHT.load(...), "deadlined": ..., "reaped": ...
    })
}
```

The free function `crate::sharddir::stats_json()` has **one reader**: `src/store_timing/observations.rs:318-320`, inside `snapshot(...)`.

```rust
        // Reopen-storm visibility (sharddir.rs): started climbing while
        // completed stays flat = the eu-central-1 wedge shape.
        "shard_opens": crate::sharddir::stats_json(),
```

`snapshot` has two callers: `src/http.rs:1054` (`debug_store`, `GET /v1/debug/store`) and `src/operator.rs:111` (`/operator/data.json`, `local.store`).

### 1.2 A `#[cfg(test)]` mirror of four counters (`src/sharddir.rs:299-312`, `:443-450`)

```rust
    /// Per-INSTANCE mirrors of the global counters, for tests. The
    /// statics feed process metrics and are shared with every other
    /// OpenGate in the binary — a paused-clock gate test asserting on
    /// them raced ordinary http_rig tests opening engines concurrently
    /// (completed bled to 1 in one full-suite run per ~8). Tests
    /// assert on THEIR gate's counters instead.
    #[cfg(test)]
    c_started: AtomicU64,
    #[cfg(test)]
    c_completed: AtomicU64,
    #[cfg(test)]
    c_failed: AtomicU64,
    #[cfg(test)]
    c_coalesced: AtomicU64,
```

The mirror was added by `f6be6a9f` ("OpenGate tests now assert on per-instance counter mirrors…"), and `new()` initialises it at `:443-450`.

### 1.3 Every write site (a complete list)

| Counter | Static write | Mirror write | Location |
|---|---|---|---|
| coalesced | `:512 OPENS_COALESCED.fetch_add` | `:513-514 self.inner.c_coalesced` | `get_or_open`, subscribe branch |
| started | `:544 OPENS_STARTED.fetch_add` | `:546-547 self.inner.c_started` | `get_or_open`, start branch |
| in_flight | `:545 fetch_add`, `:567 fetch_sub` | **none** | `get_or_open` + open task |
| failed | `:572` (Err), `:587` (deadline) | `:573-574`, `:588-589 inner.c_failed` | open task |
| deadlined | `:586` | **none** | open task, deadline arm |
| reaped | `:632` | **none** | reaper task |
| completed | `:945` | `:947-948 inner.c_completed` | `publish_open` |

The reviewer says "writes every counter twice". That holds for 4 of the 7 counters (started, completed, failed, coalesced), at 5 double-write sites. `deadlined`, `reaped` and `in_flight` have no mirror, so **no test can observe them**. That part of the claim holds exactly.

### 1.4 Tests never read the operator metric

- `git grep -n "sharddir::stats_json\|shard_opens" -- src` finds only `observations.rs:320` and a comment at `bootstrap.rs:224`. No test calls `stats_json()`.
- No test requests `/v1/debug/store` at all. `git grep "debug/store" -- src/dst/tests` finds only a doc comment in `fault_substrate.rs:193`. That means the `?swap=1` sampler contract (`http.rs:1034`, "?swap=1 resets the peak") is also unpinned. This matters in §5.
- The counter assertions read the mirror through `instance_counters()` (`sharddir.rs:840-849`). It is called at `dst/tests/runtime_open_gate.rs:267` (storm-free open), `:654` (hung open) and `sharddir/unwind.rs:149` (panicking opener).

### 1.5 `reset_counters_for_tests` and `gate_lock` are vestigial

`sharddir.rs:831-838`:

```rust
    #[cfg(test)]
    pub(crate) fn reset_counters_for_tests() {
        OPENS_STARTED.store(0, Ordering::Relaxed);
        OPENS_COMPLETED.store(0, Ordering::Relaxed);
        OPENS_FAILED.store(0, Ordering::Relaxed);
        OPENS_COALESCED.store(0, Ordering::Relaxed);
        OPENS_IN_FLIGHT.store(0, Ordering::Relaxed);
    }
```

It zeroes five statics (not DEADLINED or REAPED) that no test reads. It is called at `runtime_open_gate.rs:223, 467, 532, 606`.

`runtime_open_gate.rs:196-201`:

```rust
/// OpenGate counters are process-global too; its three counter-asserting
/// tests serialize here for the same reason as the reader-cache tests.
fn gate_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}
```

It is taken at `:211, :466, :530, :604`: four tests, not the three the comment says. Only two of them assert counters (`:267`, `:654`), and both read the per-instance mirror. `gate_lock` came in with `a0a45569`, before the mirror (`f6be6a9f`). Since the mirror landed, the lock protects nothing. Nothing else is shared: each test builds its own `mem()` store, its own gate and its own per-gate `ShardHealth`, on its own paused `current_thread` runtime.

### 1.6 Ledger rows tied to the statics (`docs/quality/source-allowances.json`)

- 7 × `global` `src/sharddir.rs` `crate::OPENS_{COALESCED,COMPLETED,DEADLINED,FAILED,IN_FLIGHT,REAPED,STARTED}` (`AtomicU64` / `AtomicI64`), at lines ~2097-2143.
- `global` `src/dst/tests/runtime_open_gate.rs` `crate::gate_lock::L` `std :: sync :: OnceLock < tokio :: sync :: Mutex < () > >` (~1823-1829).
- `macro-dsl` `src/sharddir.rs` `crate::stats_json` `serde_json::json` (~3209-3215). This row moves with the code (§4).

### 1.7 Adjacent dead code this edit removes (`src/http.rs:1054-1059`)

```rust
    let mut snap = crate::store_timing::snapshot(window, swap, &state.runtime.store_io);
    if let Some(_obj) = snap.as_object_mut() {
        // History DbReader service: hits vs misses shows how much
        // per-request manifest traffic the cache absorbs; stale_reopens
        // is bounded by absorb cadence; coalesced proves single-flight.
    }
```

The block is a no-op. It describes a DbReader insertion that no longer exists. Deleting it is what keeps `http.rs` under its ceiling (§4).

### 1.8 One gate per process in production

`bootstrap::run` runs once per process (`lib.rs:84`: "a second invocation in one process fails loudly"; `RUN_WAS_INVOKED`). It builds exactly one `ShardDirectory` (`bootstrap.rs:567`), and so one `OpenGate`. Per-gate counters therefore equal today's process counters in every deployed binary. Only processes that host several runtimes behave differently, and those are the DST rigs.

---

## 2. Contract decision

**Typed internal contract (one owner, one rendering):**

- `struct OpenCounters` is private to `sharddir.rs` and `#[derive(Default)]`. It holds seven atomics: `started, completed, failed, coalesced: AtomicU64`, `in_flight: AtomicI64`, `deadlined, reaped: AtomicU64`. It is an **unconditional** field `opens` of `GateInner`, written once per event at the sites in §1.3.
- `OpenGate::stats_json(&self) -> serde_json::Value` is the only rendering. It keeps the same `json!` literal, the same seven keys in the same order, and `in_flight` stays signed.
- `ShardDirectory::open_stats(&self) -> serde_json::Value` is the directory's production accessor. The directory already hides its gate (see `unready_reason`, `open_or_wait`).
- `store_timing::snapshot(window_secs, swap_peak, resources, shard_opens: &serde_json::Value)` still composes `/v1/debug/store` in one place. The parameter is a value, not `&ShardDirectory`, so the process-instrumentation module stays independent of the directory. It is a reference because `json!` serialises through `&expr`, so a by-value `Value` would never be consumed and risks `needless_pass_by_value`.
- Tests assert on `stats_json()`. That is the JSON operators read, so it is not a test-only typed view. Each JSON key is read once in the `open_counts` helper (§4.7); nothing is matched by string prefix.

**No wire change.** `/v1/debug/store` → `shard_opens` and `/operator/data.json` → `local.store.shard_opens` keep the same key, the same seven fields, the same types and the same literal order. Production values are also identical, because there is one gate per process (§1.8). The only observable difference is in a process that hosts more than one runtime, which today means the DST rigs. There each runtime's endpoint reports its own directory. That is the correct value, because every other directory-level field on those surfaces (`open_shards`, the serving map) is already per runtime. This is why no WIRE-MATRIX or RUNBOOK change is needed.

Alternatives rejected:

- **Compose in the handlers.** Keep `snapshot`'s signature, and insert `shard_opens` into `debug_store`'s empty block and separately in `operator.rs`. This duplicates the composition across two handlers, and `debug_store`'s whole-body mutant still needs the same router test.
- **Derive `in_flight` from the `inflight` markers.** It would delete a counter, but it puts a lock (and a new `unwrap_used` exception) on the stats read. See §8.

---

## 3. Red tests and pinning tests

### R1 (behavioural red; compiles on HEAD): `dst::dst_tests::runtime_open_gate::debug_store_reports_this_runtimes_shard_opens`

File `src/dst/tests/runtime_open_gate.rs`, appended at the end. It needs these imports: `use super::fixture_http::{http_rig, http_rig_at};`, `use super::fixture_requests::hreq;`, `use super::fixture_runtime::RigRuntime;`. It also uses the `open_counts` helper from §4.7.

```rust
/// The reopen-storm detector on /v1/debug/store belongs to the runtime
/// that serves it. Two runtimes share this process, as every DST rig
/// does: the one that opened nothing must report nothing, and the one
/// that opened its shard reports exactly that open. The sampler's
/// `?swap=1` resets the outbound peak and never the cumulative open
/// counters, and the operator dashboard carries the same object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_store_reports_this_runtimes_shard_opens() {
    // Store ops only raise the process peak (fetch_max); the sampler's
    // swap is the one thing that lowers it, and no real gauge gets here.
    const SENTINEL_PEAK: i64 = 1 << 40;
    crate::store_timing::stats()
        .inflight_peak
        .fetch_max(SENTINEL_PEAK, Ordering::Relaxed);
    let (_opened_state, opened) = http_rig(mem()).await;
    let (_idle_state, idle) = http_rig_at(mem(), RigRuntime::incarnation(1)).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(opened, "PUT", "/v1/stream/opens-x", &ct, b"").await;
    assert!(st == 200 || st == 201, "create: {st}");
    let (st, _, _) = hreq(opened, "POST", "/v1/stream/opens-x", &ct, br#"[{"n":1}]"#).await;
    assert!(st == 200 || st == 204, "append: {st}");
    let json = |body: &[u8]| serde_json::from_slice::<serde_json::Value>(body).unwrap();
    let peak = |store: &serde_json::Value| store["out_inflight_peak"].as_i64().unwrap();

    let (st, _, body) = hreq(idle, "GET", "/v1/debug/store", &[], b"").await;
    assert_eq!(st, 200);
    let untouched = json(&body);
    assert_eq!(
        untouched["shard_opens"]["started"],
        0,
        "the idle runtime opened no shard; another runtime's opens reached its operator surface"
    );
    assert_eq!(open_counts(&untouched["shard_opens"]), [0; 7], "{untouched}");

    let (_, _, body) = hreq(opened, "GET", "/v1/debug/store?swap=1", &[], b"").await;
    let sampled = json(&body);
    let [started, completed, failed, _, in_flight, deadlined, reaped] =
        open_counts(&sampled["shard_opens"]);
    assert_eq!(
        (started, completed, failed, in_flight, deadlined, reaped),
        (1, 1, 0, 0, 0, 0),
        "one shard, one completed open: {sampled}"
    );
    assert!(peak(&sampled) >= SENTINEL_PEAK, "the sampler reads the peak it resets");
    let (_, _, body) = hreq(opened, "GET", "/v1/debug/store", &[], b"").await;
    let after = json(&body);
    assert!(peak(&after) < SENTINEL_PEAK, "?swap=1 must reset the outbound peak: {after}");
    assert_eq!(after["shard_opens"], sampled["shard_opens"], "a swap never resets opens");
    let (st, _, body) = hreq(opened, "GET", "/operator/data.json", &[], b"").await;
    assert_eq!(st, 200);
    assert_eq!(
        json(&body)["local"]["store"]["shard_opens"],
        after["shard_opens"],
        "the operator dashboard carries the same counters"
    );
}
```

(Bind the rig states to named `_x_state` variables, not `_`: `_` drops the `AppState` immediately. This matches `admission_maintenance.rs:700-711`.)

**Expected red on HEAD.** Apply only this test, the helper and the imports (the §7 step R). Trace:

1. Rig A's append resolves prefix `"00"` through `ShardDirectory::resolve` → `OpenGate::get_or_open`, which bumps the process static at `sharddir.rs:544`. The open completes (`:945`).
2. Rig B serves `GET /v1/debug/store` → `debug_store` → `store_timing::snapshot` → `observations.rs:320 crate::sharddir::stats_json()`. That reads `OPENS_STARTED` (`sharddir.rs:197`), which is 1 in an isolated run.
3. The first `assert_eq!` fails:

```
---- dst::dst_tests::runtime_open_gate::debug_store_reports_this_runtimes_shard_opens stdout ----

thread 'dst::dst_tests::runtime_open_gate::debug_store_reports_this_runtimes_shard_opens' panicked at src/dst/tests/runtime_open_gate.rs:<line of that assert_eq!>:5:
assertion `left == right` failed: the idle runtime opened no shard; another runtime's opens reached its operator surface
  left: Number(1)
 right: 0
...
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; <N> filtered out
```

In a full-suite run `left` is `Number(N)` with N ≥ 1: the other tests' opens bleed in, which is the leak `f6be6a9f` recorded. On HEAD the second half of the test (the swap contract and the operator object) passes. That part pins existing behaviour.

**After the fix:** rig B's gate has seen nothing (`[0; 7]`). Rig A reports `started 1, completed 1, failed 0, in_flight 0, deadlined 0, reaped 0`; `coalesced` is not asserted. `?swap=1` returns a peak ≥ the sentinel and resets it, so the next read is below the sentinel. `shard_opens` is unchanged by the swap and equals `local.store.shard_opens`.

### R2 (the reviewer's First step; a compile-level red): `dst::dst_tests::runtime_open_gate::a_hung_open_is_deadlined_and_its_late_engine_reaped`

With `gate_lock`/`reset` removed, the deadline assertion becomes `assert_eq!(open_counts(&gate.stats_json()), [1, 0, 1, 0, 0, 1, 0]);`, which covers deadlined==1, in_flight==0, failed==1 and completed==0. After the reaper loop comes `assert_eq!(open_counts(&gate.stats_json()), [1, 0, 1, 0, 0, 1, 1]);` (reaped==1). On HEAD this does not compile:

```
error[E0599]: no method named `stats_json` found for struct `OpenGate` in the current scope
   --> src/dst/tests/runtime_open_gate.rs:<line>:<col>
```

The reviewer's First step cannot be a runtime red on HEAD. Written against the global `crate::sharddir::stats_json()`, it **passes** in isolation (deadlined 1, reaped 1, in_flight 0) and only flakes in the full suite. R1 is therefore the behavioural red, and R2 is the compile-level red for the per-gate read.

### Pinning tests (must stay green; each re-reads the operator JSON)

- `…::open_gate_survives_impatient_clients_without_a_storm`: started 1, completed 1, failed 0, **in_flight 0 (new)**, coalesced ≥ 10, all through `open_counts(&gate.stats_json())`.
- `…::health_reports_unready_when_no_shard_has_ever_opened` and `…::open_gate_escalates_holdoff_for_engines_that_die_young`: only `gate_lock`/`reset` are removed. Running them unserialised is the proof that the lock was vestigial.
- `sharddir::unwind::tests::a_panicking_opener_fails_its_open_and_the_next_attempt_installs`: `stats_json()["failed"] == 1`, `["completed"] == 0`, **`["in_flight"] == 0` (new: no phantom open)**. This is the mutation killer for `OpenGate::stats_json` (§5).
- `shard_directory::directory_tests::open_failure_is_typed`: `dir.open_stats()["started"] == 1`, `["failed"] == 1`. This is the mutation killer for `ShardDirectory::open_stats`.
- `…::reopen_storm_reproduces_the_eu_central_wedge` and `…::idle_engine_store_traffic_is_bounded_by_the_poll_cadence`: untouched.

### Compile-level proofs

Deleting the 7 statics, the free `stats_json`, `reset_counters_for_tests`, `instance_counters`, the four `c_*` fields and `gate_lock` means any missed reference fails to compile (E0425 / E0599 / E0609 / E0560). `git grep -n "OPENS_\|instance_counters\|reset_counters_for_tests\|gate_lock\|sharddir::stats_json" -- src` must return nothing.

---

## 4. Edits, file by file (one commit, in this order)

Only `src/http.rs` of the edited files is ceilinged (over 1,000 lines). The other ceilinged files are untouched.

| File | Now (`wc -l`) | After | Budget |
|---|---|---|---|
| `src/http.rs` | 3,366 (merge base 3,366) | **3,362** | ceiling 3,366: −4 |
| `src/product.rs` 4,205 · `src/shard.rs` 3,196 · `src/billing.rs` 2,201 · `src/history.rs` 1,713 · `src/auth.rs` 1,676 · `src/registry.rs` 1,492 · `src/sse/feed.rs` 1,170 · `src/fleet.rs` 1,143 | unchanged | — | untouched |
| `src/sharddir.rs` | 958 | ~918 | < 1,000 |
| `src/shard_directory.rs` | 638 | ~647 | < 1,000 |
| `src/store_timing/observations.rs` | 640 | ~643 | < 1,000 |
| `src/operator.rs` | 123 | 124 | < 1,000 |
| `src/dst/tests/runtime_open_gate.rs` (DST) | 700 | ~752 | < 1,000 |
| `src/sharddir/unwind.rs` | 229 | ~230 | < 1,000 |

No verbatim-move commit is needed: no ceilinged file grows.

### 4.1 `src/sharddir.rs`

1. **`:63-74`**: replace the comment and 7 statics with:
   ```rust
   /// The reopen-storm detector /v1/debug/store reports as `shard_opens`:
   /// started climbing while the serving map stays empty is this failure
   /// mode's cloud-run signature, and it must be visible without logs. The
   /// gate that counts is the gate that reports (one per directory, one
   /// directory per runtime), so another runtime in the process can neither
   /// inflate nor hide these opens.
   #[derive(Default)]
   struct OpenCounters {
       started: AtomicU64,
       completed: AtomicU64,
       failed: AtomicU64,
       coalesced: AtomicU64,
       in_flight: AtomicI64,
       deadlined: AtomicU64,
       /// Abandoned opens that eventually completed and were closed by the
       /// reaper instead of installed.
       reaped: AtomicU64,
   }
   ```
2. **`:195-206`**: delete the free `pub(crate) fn stats_json()` and its trailing blank line.
3. **`:299-312`** (`GateInner`): replace the mirror doc and the four `#[cfg(test)] c_*` fields with:
   ```rust
       /// Counted per gate, never in process statics: see `OpenCounters`.
       opens: OpenCounters,
   ```
4. **`:443-450`** (`OpenGate::new`): replace the eight `#[cfg(test)]` initialiser lines with `opens: OpenCounters::default(),`.
5. **`get_or_open`**, one statement per event:
   - `:512-514` → `self.inner.opens.coalesced.fetch_add(1, Ordering::Relaxed);`
   - `:544-547` → `self.inner.opens.started.fetch_add(1, Ordering::Relaxed);` and `self.inner.opens.in_flight.fetch_add(1, Ordering::Relaxed);`
   - `:567` → `inner.opens.in_flight.fetch_sub(1, Ordering::Relaxed);`
   - `:572-574` → `inner.opens.failed.fetch_add(1, Ordering::Relaxed);`
   - `:586-589` → `inner.opens.deadlined.fetch_add(1, Ordering::Relaxed);` and `inner.opens.failed.fetch_add(1, Ordering::Relaxed);`
   - `:632` → `reaper.opens.reaped.fetch_add(1, Ordering::Relaxed);`
6. **`:471-474`**: re-decide `get_or_open`'s `clippy::unwrap_used` reason (see "Ratcheted scopes" below):
   ```
   reason = "OpenGate::get_or_open; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff beside this gate's own open counters; recovering either could serve, reopen or reap the wrong incarnation"
   ```
   It has exactly two `;` and no `"` in the text.
7. **After `shutdown_pending` (`:802`)**: insert only these lines:
   ```rust

       /// This gate's `shard_opens` object: the key set is the operator
       /// contract (RUNBOOK: started ≫ completed is the reopen loop), rendered
       /// once so the dashboard, /v1/debug/store and the tests read one object.
       pub(crate) fn stats_json(&self) -> serde_json::Value {
           let opens = &self.inner.opens;
           serde_json::json!({
               "started": opens.started.load(Ordering::Relaxed),
               "completed": opens.completed.load(Ordering::Relaxed),
               "failed": opens.failed.load(Ordering::Relaxed),
               "coalesced": opens.coalesced.load(Ordering::Relaxed),
               "in_flight": opens.in_flight.load(Ordering::Relaxed),
               "deadlined": opens.deadlined.load(Ordering::Relaxed),
               "reaped": opens.reaped.load(Ordering::Relaxed),
           })
       }
   ```
8. **`:830-849`**: delete `reset_counters_for_tests` and `instance_counters`, with their blank separators.
9. **`publish_open` `:945-948`**: `OPENS_COMPLETED…` → `inner.opens.completed.fetch_add(1, Ordering::Relaxed);`, and delete `:947-948` (the mirror).

Imports at `:40` stay: `AtomicI64` is still used by `OpenCounters`.

### 4.2 `src/shard_directory.rs`

- Insert after `open_or_wait` (`:303-305`), without touching the neighbouring lines:
  ```rust

      /// The directory hides its gate; the operator surfaces need exactly one
      /// read of it, this runtime's reopen-storm counters (`shard_opens`).
      pub(crate) fn open_stats(&self) -> serde_json::Value {
          self.inner.gate.stats_json()
      }
  ```
- `directory_tests::open_failure_is_typed`: after `assert_eq!(calls.load(Ordering::Relaxed), 1);` (`:547`) add:
  ```rust
          let opens = dir.open_stats();
          assert_eq!(opens["started"], 1, "the directory reports its own gate: {opens}");
          assert_eq!(opens["failed"], 1, "{opens}");
  ```

### 4.3 `src/store_timing/observations.rs` (`snapshot`, `:248-322`)

- Doc (`:248-249`): "Snapshot for /v1/debug/store: per (op,class) percentiles over `window_secs`, the slow-op ring, the outbound gauge, and the caller's `shard_opens`. Those counters belong to the runtime's directory, which this process-wide module cannot own."
- Re-decide the reason (`:252`):
  ```
  reason = "process slow-operation ring read beside the caller's shard-open counters; poison may follow an interrupted sample update; recovery would present partial diagnostic state as valid"
  ```
- Signature: add a fourth parameter `shard_opens: &serde_json::Value,`. The function then has 4 parameters, one of them a bool.
- `:320`: `"shard_opens": crate::sharddir::stats_json(),` → `"shard_opens": shard_opens,`. Keep the two-line comment above it.

### 4.4 `src/http.rs` (`debug_store`, `:1054-1059`: 6 lines replaced by 2)

```rust
    let swap = q.get("swap").map(|v| v == "1").unwrap_or(false);      // unchanged (:1053)
    let opens = state.shards.open_stats();
    let snap = crate::store_timing::snapshot(window, swap, &state.runtime.store_io, &opens);
    axum::Json(snap).into_response()
```

The empty `if let Some(_obj)` block and its stale DbReader comment are deleted (§1.7). The result is 3,366 → 3,362 lines. The call line is 92 characters, and its arguments are 45 characters (under rustfmt's 60-character argument limit), so it stays on one line.

### 4.5 `src/operator.rs` (`data`, `:104-112`)

After `let adm = state.admission.snapshot();` add `let shard_opens = state.shards.open_stats();`. The `"store"` entry becomes `"store": crate::store_timing::snapshot(60, false, &state.runtime.store_io, &shard_opens),` (97 characters). It sits inside `json!`, so rustfmt leaves it as written, and the macro count for `crate::data` stays at 2.

### 4.6 `src/sharddir/unwind.rs` (test module only)

`:149-150`:

```rust
        let (_started, completed, failed, _coalesced) = gate.instance_counters();
        assert_eq!((completed, failed), (0, 1), "the panic is one failed open");
```

becomes:

```rust
        let opens = gate.stats_json();
        assert_eq!(opens["failed"], 1, "the panic is one failed open: {opens}");
        assert_eq!(opens["completed"], 0, "{opens}");
        assert_eq!(opens["in_flight"], 0, "a panicked open is not in flight: {opens}");
```

### 4.7 `src/dst/tests/runtime_open_gate.rs`

- Imports (top): add `use super::fixture_http::{http_rig, http_rig_at};`, `use super::fixture_requests::hreq;` and `use super::fixture_runtime::RigRuntime;`.
- `:196-201`: replace `gate_lock` and its doc with:
  ```rust
  /// `shard_opens` in its key order (started, completed, failed, coalesced,
  /// in_flight, deadlined, reaped), as the operator reads it; a missing or
  /// renamed key reads -1 and fails every comparison.
  fn open_counts(opens: &serde_json::Value) -> [i64; 7] {
      ["started", "completed", "failed", "coalesced", "in_flight", "deadlined", "reaped"]
          .map(|key| opens[key].as_i64().unwrap_or(-1))
  }
  ```
- `open_gate_survives…`: delete `:211` (`_serial`) and `:223` (reset). Replace `:267-271` with:
  ```rust
      let [started, completed, failed, coalesced, in_flight, ..] = open_counts(&gate.stats_json());
      assert_eq!(started, 1, "exactly one open may start (got {started})");
      assert_eq!(completed, 1);
      assert_eq!(failed, 0);
      assert_eq!(in_flight, 0, "the one open is no longer in flight");
      assert!(coalesced >= 10, "later callers must join the first open");
  ```
- `health_reports_unready…`: delete `:466-467`.
- `open_gate_escalates…`: delete `:530` and `:532`.
- `a_hung_open…`: delete `:604` and `:606`. Replace `:654-656` with:
  ```rust
      // started, completed, failed, coalesced, in_flight, deadlined, reaped
      assert_eq!(open_counts(&gate.stats_json()), [1, 0, 1, 0, 0, 1, 0]);
  ```
  After `assert!(reaped, "the late engine was never closed by the reaper");` (`:676`) add:
  ```rust
      assert_eq!(open_counts(&gate.stats_json()), [1, 0, 1, 0, 0, 1, 1]);
  ```
  Each argument list is 55 characters, under rustfmt's 60-character argument limit, so each assertion stays on one line. Do not add messages here (see the ratchet table).
- Append the R1 test (§3) at the end of the file.

### 4.8 `scripts/quality/mutation_owners.py:149`

```python
    owner('http', 'src/http.rs', 'http:: livefeed_engine_retired security_workload:: debug_store_reports_this_runtimes_shard_opens'),
```

This follows the existing test-name idiom (`livefeed_engine_retired`). Editing this file marks the change as tooling, so the plan also schedules loom, miri and properties_fuzz. That is expected.

### Ratcheted scopes this edit touches, and the remedy for each

| Scope (`#[expect]`) | Effect of the edit | Remedy |
|---|---|---|
| `OpenGate::get_or_open` `unwrap_used` | Removing the mirrors removes 2 `self` and 2 `inner` path facts. The new writes add `self` ×3, `inner` ×4 and `reaper` ×1. Net fingerprints: `self` 10→11, `inner` 14→16, `reaper` 1→2 → "accepted exception grew". | **Re-decide the reason** (§4.1 item 6). Narrowing would mean moving the reaper or the failure arms out of `get_or_open`, a larger diff with new in-diff mutants. |
| `get_or_open` `too_many_lines`, `disallowed_methods`, `let_underscore_must_use`, `excessive_nesting` | `scope_lines` −8. `syntax_facts` drops by at least 24: each of the four mirror sites loses its `cfg(test)` attribute (attribute fact + `cfg` path) and its 4-fact statement, and each `OPENS_X` → `self/inner/reaper.opens.x` replacement is fact-neutral. `nested_items` unchanged. `too_many_lines` still fires (~180 lines, so it stays fulfilled). | none |
| `publish_open` `unwrap_used` | `inner` 6→6, `Ordering::Relaxed` 2→1, the `OPENS_COMPLETED` key is removed, −2 lines. | none (no growth) |
| `store_timing::snapshot` `unwrap_used` | The new parameter adds 1 line and 1 path fact (`serde_json::Value`: fingerprint 1→2). | **Re-decide the reason** (§4.3). Narrowing would mean moving the slow-ring read out, a larger diff. |
| `a_hung_open…` `let_underscore_must_use` (DST) | Lines: −2 (`_serial`, reset), −3 +2 (deadline assertion with comment), +1 (reaped) = **−2**. Facts: −(4+2+3+3+3 = 15) +(3+3) = **−9**. Each macro is 3 facts: `macro`, `macro-tokens`, and a `path` for `assert_eq`. | none. This is why the assertions use the helper and carry no messages. Four separate `opens["x"]` assertions would add +3 facts each and grow the scope. |
| `open_gate_escalates…` `excessive_nesting` (DST) | −2 lines, −6 facts | none |
| `debug_store`, `operator::data`, `ShardDirectory::open_stats`, the new `OpenGate::stats_json`, the unwind/directory tests, the survive and health tests, R1 | no `#[expect]` in scope | none |

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

`in_diff::affected_lines` marks every inserted line and the **new-file line before each deleted line**. A modified line counts as a delete plus an insert. A function's whole-body mutant spans its body, so any edited body selects it. cargo-mutants skips `#[cfg(test)]`/`#[test]` items and any impl method named `new` (`visit.rs:466`).

Planner selection (`verification_plan.py`):

- Selected: `src/sharddir.rs` (`src/shard` prefix, owner `sharddir`, filter `sharddir::`), `src/shard_directory.rs` (owner `shard_directory`, filter `shard_directory::`), `src/http.rs` (owner `http`).
- `src/sharddir/unwind.rs` is only a `#[cfg(test)]` edit, so it lands in `production_unchanged_files`.
- Not critical: `src/store_timing/observations.rs`, `src/operator.rs` (not the `src/ops` prefix), `src/dst/**`.

| # | Mutant | Why it is in the diff | Outcome / killer |
|---|---|---|---|
| M1 | `src/sharddir.rs: replace OpenGate::stats_json -> serde_json::Value with Default::default()` | new function | **caught**: `sharddir::unwind::tests::a_panicking_opener_fails_its_open_and_the_next_attempt_installs`, because `Null["failed"]` is `Null`, not 1 |
| M2 | `replace OpenGate::get_or_open -> OpenOutcome with Default::default()` | body edited | **unviable**: `OpenOutcome` has no `Default` |
| M3 | `replace publish_open -> OpenResult with Default::default()` | body edited. `OpenResult` is an alias whose last path segment is not `Result`, so cargo-mutants falls back to `Default::default()`. | **unviable**: `Result` has no `Default` |
| M4 | `src/shard_directory.rs: replace ShardDirectory::open_stats -> serde_json::Value with Default::default()` | new function (insertion only) | **caught**: `shard_directory::directory_tests::open_failure_is_typed` (`Null["started"]` ≠ 1) |
| M5 | `src/http.rs: replace debug_store -> Response with Default::default()` | body edited | **caught**: R1. The response is `200` with an empty body, so `from_slice(b"")` panics at the idle rig's first read |
| M6 | `src/http.rs:1053: replace == with != in debug_store` | `:1053` (`let swap …`) is the line before the deleted `:1054` | **caught**: R1. With `!=`, `?swap=1` loads instead of swapping, so the next read is still ≥ `SENTINEL_PEAK`, and `peak(&after) < SENTINEL_PEAK` fails |

No other operator is selected. I checked every "line before a deletion" in `sharddir.rs`:

- `:511` `if let Some(rx) = &g.inflight {`
- `:543` `g.inflight = Some(rx.clone());`
- `:566` `…timeout(inner.open_deadline, &mut fut).await;`
- `:571` `Ok(Err(e)) => {`
- `:585` `Err(_deadline) => {`
- `:631` `if let Some(engine) = engine {`
- `:944` `} else {`
- `:946` `inner.health.succeeded();`
- `:949` `Ok(engine)` (the line after a deletion)
- `:62`, `:194`, `:298` (blank lines or field lines)

None of them has a binary or unary operator, a guard, a `match` with a `_` arm, or a struct literal with a base. The `||` at `:922` is not affected. `OpenGate::new` is skipped because it is named `new`. The reason-text edits sit on attribute lines, outside any function body. In `http.rs` the affected lines are exactly `1053`, `1054'`, `1055'`. The lines after the insertions are not marked.

None of the mutants can time out: every killer fails on its first read. **Owner-row change:** the `http` row gains the filter `debug_store_reports_this_runtimes_shard_opens` (§4.8). No new source files, so no new owner rows.

The mutants are CAUGHT ×4 and UNVIABLE ×2. There are no missed mutants, no timeouts, and no new guard, boundary, predicate or arithmetic.

---

## 6. Ledgers (all in the same commit)

- **`docs/quality/source-allowances.json`**: delete 9 rows. These are the 7 `global` `crate::OPENS_*` rows (`src/sharddir.rs`), `global` `crate::gate_lock::L` (`src/dst/tests/runtime_open_gate.rs`), and `macro-dsl` `crate::stats_json` `serde_json::json` (`src/sharddir.rs`). Otherwise `source_gate.check` reports "obsolete source allowances". Nothing may be added here, because the legacy ceiling is 0 for a new identity.
- **`docs/quality/owners.json`**: add one row. The `json!` moved owner, and a macro-dsl row moves with its code:
  ```json
  {
    "category": "macro-dsl",
    "count": 1,
    "owner": "crate::OpenGate::stats_json",
    "path": "src/sharddir.rs",
    "reason": "Diagnostic JSON serialization owner; the pinned serde_json macro encodes this gate's reviewed shard-open counters; it does not establish domain identity or introduce an unowned effect.",
    "syntax": "serde_json::json"
  }
  ```
  The tests use no `json!`: they index `Value` and use `open_counts`. So no test owner rows are needed.
- **`docs/refactor/test-inventory.json`**: run `python3 scripts/test-inventory.py --write`. It updates 4 `function_sha256` values (`open_gate_survives_impatient_clients_without_a_storm`, `health_reports_unready_when_no_shard_has_ever_opened`, `open_gate_escalates_holdoff_for_engines_that_die_young`, `a_hung_open_is_deadlined_and_its_late_engine_reaped`) and adds 1 row (`debug_store_reports_this_runtimes_shard_opens`, `scenarios: []`). The `configuration` fields are unchanged.
- **`scripts/quality/mutation_owners.py`**: the `http` filter (§4.8).
- **No change needed:**
  - `docs/refactor/review-mechanisms.json`: no pins name these tests.
  - `docs/refactor/test-scenario-map.json` and `scenario-dispositions.json`: no renames. FLT-001/002/003 keep their symbols, and the new test is unmapped.
  - `src/dst/tests/README.md`: no new DST module.
  - `docs/refactor/architecture-policy.json`: no new dependency; `store_timing` drops its dependency on `sharddir`.
  - `docs/refactor/WIRE-MATRIX.md`: no wire change.
  - `docs/quality/legacy-*.json`: immutable baselines, left as they are.
  - `diagnostic-allowances*.json`: still empty.

---

## 7. Controls (run by the executor after the mutation run frees the tree)

| Step | Command | Expected |
|---|---|---|
| R (red, before the fix) | Apply only §4.7's imports, `open_counts` and the R1 test onto HEAD (keep `gate_lock`). Then run `cargo test --locked -p streams-slate --lib -- --exact dst::dst_tests::runtime_open_gate::debug_store_reports_this_runtimes_shard_opens` | the §3 panic (`left: Number(1)` / `right: 0`); `test result: FAILED. 0 passed; 1 failed` |
| R2 | Also apply the `a_hung_open` edit on HEAD, then `cargo test --locked -p streams-slate --lib --no-run` | `error[E0599]: no method named `stats_json` found for struct `OpenGate` in the current scope` |
| G1 | the full edit; the same `--exact` command | `test result: ok. 1 passed` |
| G2 | `cargo test --locked -p streams-slate --lib dst_tests::runtime_open_gate::` | `test result: ok. 7 passed; 0 failed` |
| G3 | `for i in $(seq 1 10); do cargo test --locked -p streams-slate --lib dst_tests::runtime_open_gate:: -q \|\| break; done` | 10 green runs (`gate_lock` removal proven) |
| G4 | `cargo test --locked -p streams-slate --lib sharddir::` and `… shard_directory::directory_tests::` | `0 failed` in both |
| G5 | `cargo test --locked -p streams-slate --lib` | green; the passed count reaches the inventory floor |
| S1 | `git grep -n "OPENS_\|instance_counters\|reset_counters_for_tests\|gate_lock\|sharddir::stats_json" -- src` | no output |
| Q1 | `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings`; `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` | clean. This includes no `unfulfilled_lint_expectations`: `get_or_open`'s `too_many_lines` is still exceeded, and every other expect is untouched |
| L1 | `python3 scripts/test-inventory.py --write` then `--check` | `test-inventory: wrote 505 tests` / `test-inventory: OK (505 tests, 0 ignored)` (current count +1) |
| L2 | `python3 scripts/architecture-gate.py --check` | `architecture-gate: OK (…)`. There is no "accepted exception grew" (the two reasons are re-decided; the others shrink), no "obsolete source allowances", and no "unregistered source occurrence" |
| L3 | `python3 scripts/scenario-map-report.py --check`; `python3 -m unittest discover -s scripts/quality` | `scenario-map: OK — …`; unittest `OK` |
| P1 | `python3 scripts/quality/verification_plan.py --out target/quality-plan` | This commit adds `src/http.rs`, `src/shard_directory.rs` and `src/sharddir.rs` to `mutation_source_files`. `src/sharddir/unwind.rs` is in `production_unchanged_files`. The owners `shard_directory`, `sharddir` and `http` are selected. `loom`, `miri` and `properties_fuzz` are all `true` (tooling). The 8 unpushed commits since `fb18840d` add their own selections to the same plan. |
| M | `cargo mutants --list --in-diff target/quality-plan/pr.diff -f src/sharddir.rs -f src/shard_directory.rs -f src/http.rs`, then `scripts/quality/mutations.sh` | The list is exactly M1-M6 (no mutant for `new`). The run reports M1, M4, M5, M6 caught and M2, M3 unviable, with 0 missed and 0 timeouts |
| F | `scripts/quality.sh` | `QUALITY_OK` |

Commit subject, in the house style: *"A gate's open counters are its own: /v1/debug/store reports its runtime's directory, and the tests read what the operator reads"*. The body names the two re-decided reasons and the `http` owner filter. It ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

---

## 8. Out of scope

- **Deriving `in_flight`** from the `PrefixGate::inflight` markers. That would delete a counter and its `fetch_sub`, but it puts the `st` lock and a new `unwrap_used` exception on the stats read. The reviewer keeps seven counters; this could be a follow-up.
- **Event methods on `OpenCounters`** (`started()` bumping started and in_flight, `deadlined()` bumping deadlined and failed). They are not smaller, and they would not avoid the `get_or_open` re-decision: the reaper's `reaper` path still grows.
- **`StoreStats` (`observations.rs:34-53`) stays process-global.** It is legitimate process instrumentation (`owners.json`: "per-runtime copies would misreport physical egress"), and the `?swap=1` peak is process-wide by design. R1 only pins the swap contract that the call-site edit happens to cover.
- **Stale docs:**
  - `RUNBOOK.md:311` (the `/v1/debug/store` row does not list `shard_opens`).
  - `docs/refactor/WIRE-MATRIX.md:207` (anchor `(1272)`; the route is at `http.rs:1400`).
  - The `debug_store` doc comment (`http.rs:1032-1035`).
- **Item 66**, the history counters. It is a separate plan (`plans9/history-counter-tests.md`) and touches no file here except the shared `test-inventory.json`, which is regenerated by `--write` in whichever order the two land.

## 9. Decisions for Søren

1. **Policy (the quality ratchet): two exception reasons are re-decided, not narrowed.**
   - `OpenGate::get_or_open`'s `clippy::unwrap_used` reason gains "beside this gate's own open counters". The per-gate writes go through `self`, `inner` and `reaper`, which grows those path fingerprints (10→11, 14→16, 1→2).
   - `store_timing::snapshot`'s reason gains "read beside the caller's shard-open counters". The new `&serde_json::Value` parameter grows the `serde_json::Value` fingerprint and adds one scope line.

   Under `docs/RUST-QUALITY.md`, an updated reason is "the explicit reviewed decision". The alternative that keeps both reasons byte-identical is narrowing: move the reaper and failure arms out of `get_or_open`, and the slow-ring read out of `snapshot`. That is a larger refactor that brings new in-diff mutants in `sharddir.rs`. Not recommended.

No wire or edge decision is needed. The `/v1/debug/store` and `/operator/data.json` shapes are byte-identical. Production values are unchanged, because there is one directory per process (§1.8, §2).

---

## Skeptic corrections (C1..C7)

This review was read-only against `fba7b844`. Nothing was built or run. I verified these claims and found them correct:

- **§1.3 write-site table.** Every line matches: `sharddir.rs:512/514, 544/545/547, 567, 572/574, 586/587/589, 632, 945/948`.
- **Use sites are complete.** `git grep` over `src`, `scripts` and `docs`:
  - `OPENS_*` appears only in `sharddir.rs`.
  - `instance_counters` is called only at `runtime_open_gate.rs:267,654` and `sharddir/unwind.rs:149`.
  - `reset_counters_for_tests` is called only at `runtime_open_gate.rs:223,467,532,606`.
  - `gate_lock` is taken only at `:211,466,530,604`.
  - The free `stats_json()` has one reader, inside the `json!` at `observations.rs:320`.
  - `store_timing::snapshot` has exactly two callers: `http.rs:1054` and `operator.rs:111`.
  - Nothing reaches these names through a glob or a `#[path]` includer. `sharddir.rs:875/889` use `super::*`, but neither of them names a counter.
  - The quality-invariants harness and fuzz do not include any touched file.
- **One directory per production process.** `bootstrap.rs:567` is the only non-test `ShardDirectory::new`.
- **`wc -l` matches the table.** `http.rs` is 3,366 (the ceiling); the other ceilinged files are untouched. `sharddir.rs` 958 → about 919, and `runtime_open_gate.rs` 700 → about 754, both under 1,000.
- **`get_or_open` unwrap fingerprint deltas.** I counted `self` 10→11, `inner` 14→16 and `reaper` 1→2, matching the plan. Removing the mirrors cuts at least 24 syntax facts. `publish_open` has no growth.
- **The DST `#[expect]` scopes shrink.** For `a_hung_open` the change is −2 lines and −9 facts, because `assert_eq!` arguments are macro tokens and create no facts. For `open_gate_escalates` it is −2 lines and −6 facts.
- **cargo-mutants 27.1.0.** `visit.rs:466` skips impl methods named `new`. In `fnvalue.rs:77-170` the `OpenResult` alias and `OpenOutcome` fall back to `Default::default()`, so those mutants are unviable. `serde_json::Value` and `Response` also fall back to `Default::default()`, and those mutants compile.
- **R1 traces.** The traces for the red run, the green run and M5/M6 hold:
  - `hreq` + `AuthMode::Off` with no bearer is authorised (`deployment_bearer.rs:28`).
  - The rig opens nothing at build (`fixture_http.rs:365-553`).
  - `completed` is bumped before `tx.send`.
  - `in_flight` is decremented before `publish_open`.
  - No other code swaps `inflight_peak`: `heartbeat_summary` only loads it (`observations.rs:635`).
  - No test reads `out_inflight_peak`.
- **Ledger rows.** The 9 source-allowance rows exist, at `source-allowances.json:1826, 2099-2141, 3212`. The new identity `crate::OpenGate::stats_json` has no legacy ceiling, so it must go in `owners.json`, as the plan says. `review-mechanisms.json` pins none of these tests. The inventory count is 504 now and 505 after.

**C1 — The merge base moved. §0 header and step P1 are stale.** `origin/slate` is now `fba7b844`, which is HEAD: `git rev-parse origin/slate HEAD` gives the same sha. The "8 unpushed commits since `fb18840d`" have been pushed.
- The ceilings are unchanged: `git show origin/slate:src/http.rs | wc -l` = 3,366.
- The exception-growth baseline (`source_gate.py:52-58`, `merge_base()`) is now `fba7b844`, and every ratchet delta in §4 was computed against that tree anyway.
- Fix P1's expectation. `pr.diff` and `mutation_source_files` contain **only this commit** (`src/http.rs`, `src/shard_directory.rs`, `src/sharddir.rs`), with no selections carried over from other commits.

**C2 — §5 describes `in_diff::affected_lines` incompletely.** In cargo-mutants 27.1.0 `src/in_diff.rs:222-235`, the code also marks the **first surviving line after a pure deletion run**, through `prev_removed`, not only the line before. Its own test `affected_lines_from_single_deletion` expects `[i-1, i]`. The plan listed `:949 Ok(engine)` but stated the rule without this case.
- I re-checked every pure-deletion run in this edit, and each line after one is operator-free:
  - after the free `stats_json` (old 195-206): `type OpenResult = …;`
  - after `reset_counters_for_tests`/`instance_counters` (old 830-849): the closing `}` of `impl OpenGate`
  - after `publish_open` 947-948: `Ok(engine)`
  - after the `#[cfg(test)]` lines in `new`: `new` is skipped
- Every replacement in `get_or_open` and `debug_store` is a delete block followed by an insert block, so the line after it is not marked.
- The conclusion (exactly M1-M6) stands. Fix the §5 wording so an executor who reorders edits, for example by deleting the mirrors in a separate hunk from the replacement, re-checks the line after each deletion.

**C3 — The `store_timing::snapshot` fingerprint count is wrong. The remedy is right.** Before the edit, `serde_json::Value` already appears twice as a path fact in the scope: the return type at `observations.rs:258` and the generic argument in `serde_json::Map<String, serde_json::Value>` at `:264`. The new parameter makes it **2→3**, not 1→2. It also adds `scope_lines` +1 and `syntax_facts` +1. The replaced `crate::sharddir::stats_json()` sits inside `json!` tokens, so it was never a path or call-site fact, and removing it changes no counted fact. The re-decided reason is still required. Correct the numbers in the ratchet table and in §9.1.
- Optional, and not required: a narrower alternative would avoid this re-decision. Leave `snapshot`'s signature alone and delete its `shard_opens` entry, which is macro-internal and makes the scope shrink. Then fill the existing `if let Some(obj) = snap.as_object_mut()` block in `debug_store` with `obj.insert("shard_opens".into(), state.shards.open_stats());`, which is 6 lines → 4 in `http.rs`, still under the ceiling. `operator::data` would do the same. The cost is composing the counters in two places. List it as the alternative in §9.1 if Søren prefers narrowing.

**C4 — The key-order claims are false. They are harmless to correctness.** `serde_json` here has no `preserve_order` feature: `Cargo.lock` lists its dependencies as `itoa`, `memchr`, `serde`, `serde_core`, `zmij`, with no `indexmap`. `Value::Object` is therefore a BTreeMap, and `shard_opens` serialises alphabetically (`coalesced, completed, deadlined, failed, in_flight, reaped, started`).
- §2's "same literal order" is moot. Say "the same seven keys and types" instead.
- `open_counts`'s doc comment (§4.7) says "in its key order … as the operator reads it". Reword it to "in the field order `OpenGate::stats_json` lists them". Otherwise a reader will expect the array to mirror the wire order.

**C5 — The red claim for the full suite is overstated.** §3 says that in a full-suite run on HEAD `left` is `Number(N)` with N ≥ 1. It is not guaranteed. `OpenGate::reset_counters_for_tests()` (`sharddir.rs:832-838`, called at `runtime_open_gate.rs:223,467,532,606`) stores 0 into `OPENS_STARTED` and the others from concurrently running gate tests. So on HEAD, R1 can **spuriously pass** in a full-suite run when a reset lands between rig A's open and rig B's read. The red control must stay the isolated `--exact` run (step R already uses it). Its deterministic output is exactly `left: Number(1)` / `right: 0`. Delete the full-suite sentence, or say N ≥ 0 and that the full suite is not a valid red. After the fix, nothing process-wide remains in `shard_opens`, so R1 is deterministic.

**C6 — §9 should note the change in counter scope at the edge.** The shape of `/v1/debug/store` and `/operator/data.json` is identical. What `shard_opens` counts changes, from "this process" to "this runtime's directory". Production values are identical because there is one directory per process (`bootstrap.rs:567`, `lib.rs:84`). Under the item's own rule, an edge-visible metric change is listed for Søren with its backward-compatible alternative. Add one line: "Decision 2 (informational): `shard_opens` is per directory. The backward-compatible alternative is the status quo, where process statics feed the endpoint and cfg(test) mirrors feed the tests. That alternative keeps the double write and the unobservable deadlined/reaped/in_flight." No WIRE-MATRIX or RUNBOOK edit is needed: `RUNBOOK.md:601` ("`shard_opens` shows the loop (started ≫ completed)") stays true.

**C7 — Make one fact in the mutation control explicit.** No test in the `sharddir` owner's filter (`sharddir::`) reaches the R1/R2 DST tests: their path is `dst::dst_tests::runtime_open_gate::…`. So M1 (`OpenGate::stats_json → Default`) must be, and is, killed by the edited `sharddir::unwind::tests::a_panicking_opener_fails_its_open_and_the_next_attempt_installs` (`opens["failed"] == 1` fails on `Null`). If that edit to the unwind test is dropped or weakened, M1 becomes MISSED. The same holds for M4 and `shard_directory::directory_tests::open_failure_is_typed`. Mark both test edits as **mandatory mutation killers, not optional pins**.

No unbuildable controls. Every command in §7 exists. `architecture-gate.py` accepts `--check` and runs `source_gate.check()` (`:193, :216-217`), `test-inventory.py` accepts `--write` and `--check`, and the `-f`/`--file` spelling in step M is accepted. Two ledgers need attention. `docs/quality/source-allowances.json` is covered (9 rows pruned) and `docs/quality/owners.json` is covered (1 macro-dsl row). `test-inventory.json` needs `--write`, which the plan schedules. Nothing further is missed: `review-mechanisms`, the scenario map, `src/dst/tests/README.md`, `architecture-policy` budgets (no exception names `get_or_open`), `WIRE-MATRIX` and `syntax-fragments` all need no change. `verification.json`'s file hashes form a historical receipt that nothing checks.

**Verdict: ready-with-corrections.** C1, C3 and C4 correct facts in the text. C5 corrects a red-test claim; the control itself is sound. C2 and C7 harden the mutation analysis. C6 adds one line to §9. No design change is required.
