# Item 41: environment-overlay typos, the scaler cooldown overflow, TOKIO_WORKERS

Tree: `slate` @ aaf2baa5. Merge base: `origin/slate` = 6ef3bc64. No file this item touches differs between the two, except `docs/quality/owners.json`, where the unpushed sweep-custody rows sit elsewhere in the file and do not conflict.
Source: reviewer item 41 (robustness-maintainability-review.md:1100-1110). I re-read every cited location on the current tree, and every problem below is verified.

Commit order (it differs from the reviewer's numbering on purpose):

| # | Commit | Kind | Can land now? |
|---|---|---|---|
| C1 | Saturating scaler cooldown | bug fix, red-first | yes |
| C2 | TOKIO_WORKERS read through `RuntimeConfig` | pure refactor (typo still falls back) | yes |
| C3 | Unparseable knob values refuse at `validate()` | boot-refusal change | **no. Wait for Søren's decision D1/D2** |

C2 goes before C3 for two reasons. C2 stays a pure refactor. C3 then becomes the only commit that changes behaviour, and it covers all 54 knob names (TOKIO_WORKERS included) in one place.

---

## 1. Problem (verified)

### 1a. Every numeric environment knob silently takes its default on a typo

`src/config/load.rs:11-13`:
```rust
fn env_parse<T: FromStr>(env: &dyn Environment, k: &str) -> Option<T> {
    env.get(k).and_then(|v| v.parse().ok())
}
```
A variable that is set but unparseable turns into `None`, and the call site keeps the default. `ServerConfig::validate()` (`validation.rs:652-712`) never sees raw values, so the configuration boots.

Traced: `ABSORB_GLOBAL_GATHERS=1x` → `env_parse::<usize>` → `"1x".parse()` Err → None → `history.absorb_global_gathers` stays 2 → `validate()` returns Ok. For `POSTINGS_CACHE_BYTES=32MiB` → `postings.cache_bytes` stays 64 MiB → Ok.

**All lax use sites (full list, current line numbers):**

- **`env_parse` direct: 43 call sites covering 42 names.**
  - `overlay_storage`: POOL_IDLE_SECS :41, STORE_MAX_CONCURRENT :44, STORE_BULK_INFLIGHT_MAX_BYTES :47, COMPACT_MAX_SST_SIZE_BYTES :50 (u64) **and** :56 (usize; one name read twice).
  - `overlay_engine`: COMPACTOR_POLL_MS :62, COMPACTOR_MAX_CONCURRENT :65, COMPACT_MAX_SUBCOMPACTIONS :68, COMPACT_MAX_FETCH_TASKS :71, COMPACT_BYTES_TO_FETCH :74, SLATEDB_RT_THREADS :77.
  - `overlay_shard_runtime`: SHARD_OPEN_DEADLINE_MS :83, SHARD_OPEN_WAIT_MS :86, UNREADY_EXIT_AFTER_SECS :89.
  - `overlay_history`: ABSORB_GLOBAL_BUDGET_BYTES :98, ABSORB_GLOBAL_GATHERS :101 (`max(1)`), HISTORY_CACHE_BYTES :104.
  - `overlay_postings`: POSTINGS_CACHE_BYTES :127.
  - `overlay_sse`: SSE_HEARTBEAT_MS :153 (`filter(>0)`).
  - `overlay_http`: TAIL_MAX_BYTES :159 (`filter(>0)`), SSE_H1_MAX_BUF :167, SSE_H1_HEADER_TIMEOUT_MS :170 (`filter(>0)`).
  - `overlay_billing_telemetry_rollup`: OUTBOX_SWEEP_SECS :180, TELEMETRY_DRAIN_SECS :183, METRICS_INTERVAL_SECS :186, MONTH_CLOSE_GRACE_MS :189 (i64), TELEMETRY_CACHE_BYTES :192, SWEEP_DISCOVERY_MAX :195, SWEEP_MAINT_RESIDENT :198, SWEEP_RESIDENT_QUANTUM :204, ALERT_USAGE_OUTBOX_DIRTY :208.
  - `overlay_fleet`: REBALANCE_LAG_SECS :216, REBALANCE_MOVE_COOLDOWN_SECS :219, FLEET_MIN :225 (`max(1)`), REBALANCE_RETURN_SECS :228.
  - `overlay_admission_usage_limits`: MAX_UNABSORBED_BYTES_PER_INSTANCE :269, MAX_UNABSORBED_BYTES_PER_SHARD :272, MAX_ABSORB_LAG_SECS :275, MAINT_BACKPRESSURE_RELEASE_PCT :278 (`min(100)`), LIMIT_BYTES_PER_SEC :281, LIMIT_REQS_PER_SEC :284, LIMIT_RECS_PER_SEC :287, LIMIT_BURST_SECS :290 (these four are f64).
- **`envf` closure, :241. One site covering 8 names.** `let envf = |k: &str, d: f64| env_parse(env, k).unwrap_or(d);` is used by SCALE_EVAL_SECS :243, SCALE_RATE_WINDOW_SECS :246, SCALE_HOT_PCT :249, SCALE_COLD_PCT :252, SCALE_HOT_EVALS :255, SCALE_COLD_EVALS :258, SCALE_COOLDOWN_SECS :261, MAX_SEGMENTS_PER_STREAM :264. Each site guards with `env.get(K).is_some()` and repeats a hard-coded default. All 8 hard-coded defaults equal the `ScaleConfig::default()` values (10, 120, 75/100, 15/100, 2, 180, 600, 64), which I checked against model.rs:494-503.
- **Hand-rolled lax parses:**
  - HISTORY_GC_INTERVAL_SECS / legacy alias HISTORY_GC_MAX_INTERVAL_SECS, :116-119: `.or_else(..).and_then(|v| v.parse::<u64>().ok())`. The alias is consulted only when the current name is unset.
  - SSE_FEED_RING_BYTES, :133-141: `raw.trim().parse().unwrap_or_else(|_| { tracing::warn!(..); 1024 * 1024 })`.
  - SSE_FEED_TOTAL_BYTES, :142-151: the same warn fallback, plus `feed_total_bytes_raw` at :142. That raw copy is re-parsed by `validation.rs:349-358` and refused. **This is the only knob that refuses a typo today**, and its "warn + default" loader branch is dead at boot.
- **TOKIO_WORKERS** (main.rs, 1c): the same lax pattern outside the loader.

Total: **54 names** silently fall back today (SSE_FEED_RING_BYTES also logs a warn). The only accidental net is MEMPROFILE_CERT=compute-1g (`profile.rs`), which catches a typo in its 7 certified measurements, and only when the default differs from the certified value.

Other readers of the same channel, **not** in this class:
- Clap-parsed CLI/env flags (`CliArgs`): clap already refuses these.
- Raw strings SSE_FEED_PROJECT_BYTES, FLEET_PEER_DOMAINS, MEMPROFILE_CERT, STREAMS_CERT_SEALED_PUBLISH_DELAY_MS (the last is strict in `validate_instruments`).
- Closed-vocabulary string compares ABSORB_PAUSE / HISTORY_COMPACTOR / STREAMS_DEBUG_* / BILLING_METER / FLEET_ALLOW_HTTP_PEERS / FRAME_COMPRESS. See §8.

**Deploy grep, done before any refusal was planned.** I grepped every tracked non-doc file for the 55 names (54 + SSE_FEED_TOTAL_BYTES) and classified every literal value.
- In-repo sources: `deploy/profiles/compute-1g.env` (16 knob lines); `bench/livefeed-perf/run-one.sh`; `bench/fleet/{deploy-fleet.sh,local-fanout.sh,livefeed-cert.mjs}`; `bench/canary/livefeed-canary.mjs`; `bench/costab/run-{keyed,mature,soak,split,wide}.sh`; `bench/costab/wedge-liveness.sh`; `bench/docker/{compose.yml,harness/cluster-deploy.sh}`; `bench/soak/{deploy-region,mt-tenants,wc-ladder}.sh`; `bench/sse-probes/sse-1per.sh`; `scripts/platform-e2e.mjs`; `.github/workflows/ci.yml:361-362`.
- Every literal is a plain decimal integer (SCALE_* included; they parse as f64 anyway).
- Every expansion is either guarded, `${X:-<int>}` (FLEET_MIN, REBALANCE_*, MAX_ABSORB_LAG_SECS, FEED_TOTAL_BYTES, H1BUF, SOAK_LIMIT_RECS_PER_SEC, WC_SLATE_RT), or emitted only when non-empty (`wc-ladder.sh:82-90` `[ -n "${WC_*:-}" ] &&`).
- The remaining matches are not settings: the `oom-acceptance.sh:64-70` mapping table, the `field-gate.mjs:17,303` message text, the `d2run.sh:4` comment, the `evaluate-capacity.py:37-38` reader, and the `verify-rc-evidence.py:251` mutation fixture.
- The deploy supervisors (`deploy/app-*/index.ts`) set no numeric knob. No Dockerfile, fly.toml or other `.env` files are tracked.
- **Values that parse only by fallback: none found.**
- **Not greppable:** environment set outside the repo (Compute console variables on live apps), and operator-supplied `WC_*`/`FEED_TOTAL_BYTES`/`H1BUF` values. See D1.

Rust tests that load unparseable values: `numeric_tests.rs:43-47` ("bad", load only) and `config/tests.rs:218` (SSE_FEED_RING_BYTES "garbage", load only). Both stay green because `load` keeps defaults. `validation_tests.rs:708-721` uses "NaN"/"0.1", which parse as f64 and are refused by `admission_limits`. That is unchanged. Grep across `src/` found no other non-integer knob tuple.

### 1b. Unchecked `cooldown_secs * 1000` in the scaler

`src/scaler3.rs:128` (inside `State::prune`, under a fn-wide `#[expect(clippy::unwrap_used)]`):
```rust
now.saturating_sub(*at) < (policy.cooldown_secs * 1000).max(SKETCH_IDLE_MS)
```
`:382-388` (inside `evaluate_state`, the split cooldown):
```rust
if now_ms - cooldowns.get(..).copied().unwrap_or(i64::MIN / 2) < pol.cooldown_secs * 1000
```
`:431-437` (the merge filter): `... .unwrap_or(i64::MIN / 2) >= pol.cooldown_secs * 1000`.

How it is reached: `SCALE_COOLDOWN_SECS=inf` → `load.rs:261` `envf(..) as i64` → i64::MAX. This is pinned as accepted by `numeric_tests.rs:14,32` (`("inf", None, None)` → `i64::MAX`). Any value above 9_223_372_036_854_775 s overflows the same way.
- **Release** (shipped binary and CI `cargo test --release`; `[profile.release]` sets no overflow-checks): it wraps. `i64::MAX*1000 = -1000`, so the split and merge cooldowns switch off: every hot segment re-splits on each evaluation. Prune keeps records only for SKETCH_IDLE_MS.
- **Debug/quality profile**: it panics with `attempt to multiply with overflow` inside the Critical `scaler` task.
- The prune site is reached from `evaluate_state` (:331, :445) and **also from the append path**: `Scaler::note_append` (:239, the amortized sweep every 4096 appends; :271, cap eviction), which `application/append.rs:334` calls. In a debug build the panic fires while `self.state` is locked, which poisons it, so every later `lock().unwrap()` in the scaler panics too.

The fourth consumer, `scaler3/controller.rs:181` `self.cooldown_secs.saturating_mul(1000)`, already saturates, so it is not a use site of the bug.

### 1c. TOKIO_WORKERS is read outside the configuration owner

`src/main.rs:11-14`:
```rust
#[expect(clippy::disallowed_methods, reason = "main; the Tokio worker floor is process configuration read once before any runtime, supervisor or configuration owner exists; routing it through an owner would need the runtime it sizes")]
```
`:44-52`:
```rust
let workers: usize = std::env::var("TOKIO_WORKERS").ok().and_then(|v| v.parse().ok())
    .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)).max(2);
```
This read happens **after** `parsed.validate()` (:32), so the reason text is wrong: the configuration owner already exists at that point. It is the only production `std::env::var` outside `src/config/environment.rs`. The other hits (`DST_DRAIN_TRACE` in dst/runtime.rs, history/gather.rs, history/worker.rs, shard/transaction/maintenance.rs; `MT_CERT_PROJECTS` in dst/tests) are test-only traces.

Ledger rows it holds:
- `docs/quality/source-allowances.json`: `{effect, src/main.rs, crate::main, std::env::var}`.
- `docs/quality/owners.json`: the `crate::main` std::env::var row ("Process entry reads the Tokio worker floor once before the runtime exists").

---

## 2. Contract decision

**C1: the saturating cooldown.** Add one private conversion in `src/scaler3.rs`:
```rust
/// Transitions are timed on the monotonic millisecond clock; a cooldown past
/// i64::MAX ms never elapses rather than wrapping into one that always has.
fn cooldown_ms(policy: &ScalePolicy) -> i64 {
    policy.cooldown_secs.saturating_mul(1000)
}
```
All three sites call it.
- Values with |cooldown_secs| ≤ i64::MAX/1000 behave identically.
- `inf` / absurd values mean "never re-scale". A never-transitioned stream is also held once the span exceeds ~4.6e15 s, because the absent-record sentinel is `i64::MIN/2`.
- `-inf` → i64::MIN (it used to wrap to 0). Both mean "no cooldown". They differ only if the monotonic clock ran backwards.

On the reviewer's "stored once": storing a derived `cooldown_ms` field in `ScaleConfig` would either duplicate state or rewrite `summary.rs` (a lossy `cooldown_secs` projection in the startup log), the controller, and the pinned `numeric_tests` float contract. One conversion function is the smallest buildable equivalent. It lives in the mutation-owned file, so its arithmetic is mutation-tested. The controller keeps its own correct saturating conversion (§8).

**C2: TOKIO_WORKERS.**
- `RuntimeConfig` gains `pub tokio_workers: Option<usize>` (TOKIO_WORKERS, parsed by the loader; `Default` gives None).
- It also gains `pub fn worker_threads(&self, available: Option<NonZeroUsize>) -> usize { self.tokio_workers.unwrap_or_else(|| available.map_or(1, NonZeroUsize::get)).max(2) }`. The Run-13/O14a rationale moves into its doc.
- `main` keeps the OS probe `std::thread::available_parallelism()`. That probe already has its own effect row, and moving it into config would need a new owner row. `main` passes it in: `config.config().runtime.worker_threads(std::thread::available_parallelism().ok())`.
- Behaviour is identical: same channel (`ProcessEnvironment::get` = `std::env::var(k).ok()`), same parse, same fallback, same floor, same point after `validate()`.

**C3: typo refusal. `load` stays infallible; `validate()` refuses.**
- `load.rs` gets `pub(crate) struct UnparsedKnob { knob: &'static str, raw: String, expected: &'static str }`.
  - It derives Clone/Debug/PartialEq/Eq.
  - Its `Display` renders ``{knob}={raw:?} does not parse as {expected}; set a plain number or unset it for the default``. `expected` = `std::any::type_name::<T>()`: `u64`, `usize`, `i64` or `f64`.
- `load.rs` also gets a private sink, `struct Overlay<'e> { env: &'e dyn Environment, unparsed: Vec<UnparsedKnob> }`, with `fn get(&self, k: &str) -> Option<String>`.
- `env_parse<T: FromStr>(env: &mut Overlay<'_>, k: &'static str) -> Option<T>`:
  1. `let raw = env.get(k)?;`
  2. Parse `raw.trim()`.
  3. On Err, push one `UnparsedKnob`, unless that knob is already recorded. This dedupes COMPACT_MAX_SST_SIZE_BYTES, which is read as both u64 and usize.
  4. Return `None`, so every call site keeps its current fallback value.
- `ServerConfig` gains `pub(crate) unparsed_knobs: Vec<UnparsedKnob>`. It is filled only by `load` and is empty in `with_knob_defaults`.
- `validate()` first drains it: `f.errors.extend(self.unparsed_knobs.iter().map(ToString::to_string))`. Any entry therefore produces `ConfigError`, and `main` prints `Error: configuration invalid (N problem(s)):` and exits 1 before any store opens. This is the existing path.
- **The parse rule**:
  - surrounding ASCII whitespace is not a typo, so the value is trimmed. The two SSE budgets and `validate_configured_capacity` already trim.
  - An **empty value refuses**, which matches the current SSE_FEED_TOTAL_BYTES behaviour. See D2.
- Unchanged semantics:
  - the zero-means-default filters (SSE_HEARTBEAT_MS, TAIL_MAX_BYTES, SSE_H1_HEADER_TIMEOUT_MS). Only "unparseable" leaves their docs.
  - the clamps (`max(1)`, `min(100)`)
  - the HISTORY_GC alias precedence
  - the float contract for scaler knobs (inf/NaN/negative still load)
  - SSE_FEED_PROJECT_BYTES (raw; strict under release posture)
- `feed_total_bytes_raw` is deleted. `validate_configured_capacity` takes the typed `feed_total_bytes: u64` and loses its private parse. The default of 16 MiB never exceeds either profile maximum (64 MiB), so checking the value unconditionally is equivalent to checking "only when set".

The reviewer wrote "env_parse returns Result". Taken literally, that forces error plumbing at 44 call sites. The sink gives the same outcome: every set-but-unparseable knob reaches `validate()` Findings, with no call-site churn and no `load` signature change (32 callers).

**Wire / metrics: no change.**
- No HTTP status, body, header, `/metrics` name or `/v1/debug` JSON is touched. `redacted_summary()` (logged once at bootstrap.rs:210, never served) never contained `feed_total_bytes_raw`, and `tokio_workers` is deliberately not added.
- `Scaler::stats_json` is unchanged.
- The only operator-visible text change is the boot stderr line for an unparseable SSE_FEED_TOTAL_BYTES (the old "does not parse as a byte count (…)" wording becomes the generic `UnparsedKnob` wording). `git grep` finds no consumer of either wording outside `src/`.
- The behaviour changes are all boot-time refusals: see §9.

---

## 3. Red tests and pins

### C1 (`src/scaler3.rs` `mod tests`; owner filter `scaler3::`)

Fixtures are added beside `sketch()`:
- `splittable(epoch, now)` = `sketch(epoch, now, true, 1)` plus `dist.note(now, u64::MAX / 4 * 3, [2; 16], 1_000_000_000_000, 1)`. That gives two keys with 0.5 share each (not dominated), load in bins 0 and ≥47, `weighted_median` = `(u64::MAX/64, 0.5)`, and it is hot.
- `mergeable(epoch, now)` = `sketch(epoch, now, false, 1)` with `cold_streak = ScalePolicy::default().hot_evals * 4`.

1. **RED** `scaler3::tests::a_cooldown_beyond_the_millisecond_range_never_elapses`.
   - Setup: policy `{ cooldown_secs: i64::MAX, ..default }`; `now = 2 * SKETCH_IDLE_MS`; `hot` with seg 0 `splittable`; `quiet` with segs 0 and 1 `mergeable`; `last_transition_ms` = 0 for both streams.
   - Assertions: `splits.is_empty()`, `merges.is_empty()`, `last_transition_ms.len() == 2`.
   - Trace on the current tree: `evaluate_state` → `g.prune` (:331) → `last_transition_ms.retain` closure → `policy.cooldown_secs * 1000` overflows.
   - Debug (`cargo test --lib`):
     ```
     thread 'scaler3::tests::a_cooldown_beyond_the_millisecond_range_never_elapses' panicked at src/scaler3.rs:128:40:
     attempt to multiply with overflow
     ```
   - Release (`cargo test --release`, CI): wrap → retention `max(-1000, 600_000)` evicts both records (age 1_200_000) → split fires → `assert!(splits.is_empty(), "{splits:?}")` panics with `[(TenantStreamRef { .. }, "epoch", 0, 288230376151711743)]`.
   - After the fix: prune keeps both records (age < i64::MAX). The split check `1_200_000 < i64::MAX` continues. The merge check `1_200_000 >= i64::MAX` is false. The test passes.
2. **Pin** (mutation killer; green on the current tree) `scaler3::tests::a_cooldown_holds_transitions_until_exactly_its_span_has_elapsed`.
   - Setup: policy `cooldown_secs: 10`. Streams: `fresh` (splittable @1_000, no record), `cooled` (splittable @1_000, record @1_000), `quiet` (2× mergeable @1_000, no record).
   - At 5_000: splits name exactly `[&fresh]`; merges == `[(quiet, "epoch")]`.
   - At 11_000: splits name exactly `[&cooled]` (age 10_000 is not < 10_000). The fresh stream's `hot_streak` was reset to 0, so it does not split again. Merges are empty (quiet's record at 5_000 has age 6_000).
3. **Pin** `scaler3::tests::a_transition_record_expires_exactly_at_its_cooldown`.
   - Setup: policy `cooldown_secs: 700`; one record at 0.
   - `prune(699_999)` keeps it. `prune(700_000)` empties the table. This proves the retention horizon is `max(cooldown_ms, SKETCH_IDLE_MS)` with a strict `<`.

The existing `all_scaler_state_is_bounded_and_idle_state_expires`, `segment_order_…` and `recreated_…` tests are unchanged. The review-pinned `repeated_appends_do_not_scan_other_streams_for_incarnation_cleanup` (review-mechanisms.json:485, function sha) is not edited.

### C2 (pure refactor): pins and compile-level proof

- `config::numeric_tests::tokio_workers_come_from_the_knob_or_the_cores_never_below_two`:
  - unset: `tokio_workers == None`; `worker_threads(Some(8)) == 8`; `worker_threads(Some(1)) == 2`; `worker_threads(None) == 2`
  - `TOKIO_WORKERS=3` → 3 with 8 cores
  - `TOKIO_WORKERS=1` → 2
  - `TOKIO_WORKERS=three` → 8 (today's fallback, still true for `load` after C3)
- `config::tests::default_values_are_pinned` gains `assert_eq!(c.runtime.tokio_workers, None);`.
- Compile-level proof: on the pre-C2 tree the pin does not compile (`no field tokio_workers on type RuntimeConfig`, `no method named worker_threads`). After C2:
  - `rg -n "std::env" src/main.rs` prints nothing.
  - `clippy -D warnings` passes with the `disallowed_methods` expectation deleted. An unfulfilled expectation would be a hard error, which proves no disallowed method remains in `main`.
- Equivalence to main.rs:44-52: `tokio_workers` = `ProcessEnvironment::get` (= `std::env::var(k).ok()`) then `.parse().ok()`, and `available.map_or(1, get)` = `available_parallelism().map(get).unwrap_or(1)`. `.max(2)` is identical.

### C3 (`src/config/validation_tests.rs`, `mod validate_boundary_tests`)

1. **RED** `config::validation::validation_tests::validate_boundary_tests::validation_rejects_every_unparseable_knob_instead_of_its_default`.
   - Env: `ABSORB_GLOBAL_GATHERS=1x`, `POSTINGS_CACHE_BYTES=32MiB` (the reviewer's pair), `COMPACT_MAX_SST_SIZE_BYTES=32M` (dedup), `SCALE_COOLDOWN_SECS=10m` (the float path), `SSE_FEED_RING_BYTES=1 MiB` (the old warn path), `SSE_FEED_TOTAL_BYTES=16 MiB` (the old raw path; single refusal site), `HISTORY_GC_INTERVAL_SECS=10m` (the inline path), `TOKIO_WORKERS=three`.
   - Assertions:
     - `err.to_string().lines().next() == Some("configuration invalid (8 problem(s)):")`
     - each `KNOB="raw"` substring appears exactly once
   - Trace on the tree after C2: every knob except SSE_FEED_TOTAL_BYTES loads its default silently. `validate_configured_capacity` (validation.rs:349-358) refuses only SSE_FEED_TOTAL_BYTES. Expected red:
     ```
     assertion `left == right` failed
       left: Some("configuration invalid (1 problem(s)):")
      right: Some("configuration invalid (8 problem(s)):")
     ```
2. **RED** `…::validate_boundary_tests::a_padded_knob_takes_effect_and_an_empty_one_refuses`.
   - `POSTINGS_CACHE_BYTES=" 33554432 "` validates Ok with `postings.cache_bytes == 33_554_432`.
   - Then `rejects(|_| {}, &[("POSTINGS_CACHE_BYTES", "")], "POSTINGS_CACHE_BYTES=\"\"")`.
   - Red on the current tree: the untrimmed parse fails, so the value defaults. Expected output:
     ```
     assertion `left == right` failed
       left: 67108864
      right: 33554432
     ```
3. Adapted pins, same assertions with typed inputs:
   - `release_capacity_validates_hub_budget_and_fd_ceiling`
   - `hub_budget_maximum_is_profile_specific`
   - `capacity_posture_is_unforgeable`
   - `release_capacity_never_turns_the_sse_gate_off`

   The `configured(release, profile, feed: Option<u64>, cap)` helper maps `None` to `SseConfig::default().feed_total_bytes`. The typo line `configured(false, None, Some("16 MiB"), ..).is_err()` (validation_tests.rs:373-374) is deleted, because red test 1 now covers it.
4. Unchanged and still green, because `load` still keeps defaults:
   - `numeric_tests::scaler_coercions_preserve_the_existing_float_contract` (its comment becomes "load keeps defaults; validate refuses")
   - `config::tests::env_overlay_applies_with_legacy_parse_semantics` (comment on :218 becomes "load keeps the default; validate refuses")
   - `validation_rejects_a_limit_posture_that_can_never_admit`

---

## 4. Edits, file by file, in commit order

Ceilinged files (> 1,000 lines at the merge base; none are touched):

| File | wc -l | Budget |
|---|---|---|
| http.rs | 3,155 | 0 |
| product.rs | 4,205 | 0 |
| shard.rs | 3,186 | 0 |
| billing.rs | 2,157 | 0 |
| history.rs | 1,713 | 0 |
| auth.rs | 1,676 | 0 |
| registry.rs | 1,492 | 0 |
| sse/feed.rs | 1,165 | 0 |
| fleet.rs | 1,142 | 0 |

Touched files and the 1,000-line crossing limit:

| File | Now | After | Commit |
|---|---|---|---|
| scaler3.rs | 852 | ≈945 | C1 |
| config/validation.rs | **973** | ≈964 | C3 (−12 capacity parse, +3 stage) |
| config/validation_tests.rs | 725 | ≈765 | C3 |
| config/model.rs | 508 | ≈515 | C2 +9, C3 −2 |
| config/load.rs | 308 | ≈325 | |
| config/tests.rs | 623 | 624 | |
| config/numeric_tests.rs | 78 | ≈98 | |
| main.rs | 59 | ≈47 | |

### C1: "A cooldown past the millisecond range holds for ever instead of wrapping into none"

`src/scaler3.rs` only.
1. Add `fn cooldown_ms(policy: &ScalePolicy) -> i64` (§2) after the SKETCH consts (:100-102).
2. :128 → `now.saturating_sub(*at) < cooldown_ms(policy).max(SKETCH_IDLE_MS)`.
3. :387 → `< cooldown_ms(pol)`.
4. :436 → `>= cooldown_ms(pol)`.
5. Tests from §3/C1.

Ratcheted scopes touched (`exception_contracts`):
- **`State::prune` `#[expect(clippy::unwrap_used)]`** (:120-123, fn-wide). `cooldown_ms(policy)` adds a call-site fact and a `cooldown_ms` path fact. That gives `syntax_facts +2` and two NEW fingerprint keys (`unwrap_site:ordinary-call:…`, `unwrap_site:path:…`), which fails with "accepted exception grew".
  - Remedy: re-decide the reason. The new text also corrects the wrong noun, because the cap loop walks `last_transition_ms`, not the sketch table: `reason = "State::prune; the cooldown table just exceeded its cap, so at least one entry exists to pick as the victim; a fallible pick would turn the cap into a no-op"`. It has two `;` and no `"`.
  - Narrowing (moving the eviction loop out) would cost more mutants for the same result.
- **`evaluate_state` `#[expect(clippy::too_many_lines)]`** (:320-323): two sites, `syntax_facts +4`. scope_lines and nested_items are unchanged.
  - Remedy: re-decide the reason: `reason = "evaluate_state; one pass ranks every sketched segment against the same policy, limits and cooldown clock; splitting it would hide which rule chose each split or merge"`. The fn stays > 100 lines, so the expectation is still fulfilled.
- Not touched: `start` (`let_underscore_must_use`). This is why the controller is not rerouted (§8). The test-module `segment_order_…` expect is also untouched.
- The new top-level fn falls in no exception scope.

### C2: "The Tokio worker count is a RuntimeConfig knob; main no longer reads the environment"

- `src/config/model.rs`:
  - `RuntimeConfig` gains the `tokio_workers` field, with the doc "TOKIO_WORKERS; None = one per available core".
  - Add `impl RuntimeConfig { pub fn worker_threads(..) }`. Its doc carries the O14a reason: one worker on a 1-vCPU box lets a single blocking poll freeze every future, durable acks included, so the floor is two.
  - It is `pub` because the binary crate calls it through `ServerConfig.runtime`. This matches `EngineConfig::compactor_options`, so `unreachable_pub` stays quiet.
- `src/config/load.rs` `overlay_runtime_certification`: add `self.runtime.tokio_workers = env_parse(env, "TOKIO_WORKERS");`.
- `src/main.rs`:
  - delete the `#[expect(clippy::disallowed_methods, …)]` (:11-14) and the Run-13 comment plus :44-52
  - replace them with `let workers = config.config().runtime.worker_threads(std::thread::available_parallelism().ok());`
  - keep the `tracing::info!` line
- Tests: §3/C2 (`numeric_tests.rs`, one line in `tests.rs`).
- Ratchets: the main.rs exception identity disappears, so there is nothing to compare. No other scope is touched.

### C3: "An environment knob that does not parse refuses the configuration instead of booting as its default"

- `src/config/load.rs`:
  1. Add `UnparsedKnob` + `Display`, `Overlay`, and the new `env_parse` (§2).
  2. `load()` builds `Overlay`, overlays, and moves `unparsed` into `cfg.unparsed_knobs`.
  3. `overlay_env` and all 13 `overlay_*` take `env: &mut Overlay<'_>`. The 43 `env_parse(env, "…")` call sites stay byte-identical.
  4. HISTORY_GC: `let secs = if env.get("HISTORY_GC_INTERVAL_SECS").is_some() { env_parse::<u64>(env, "HISTORY_GC_INTERVAL_SECS") } else { env_parse::<u64>(env, "HISTORY_GC_MAX_INTERVAL_SECS") };`. This preserves the precedence and drops the extra block.
  5. SSE: `if let Some(v) = env_parse(env, "SSE_FEED_RING_BYTES") { … }`, the same for `SSE_FEED_TOTAL_BYTES`. Delete both `tracing::warn!` fallbacks and the `feed_total_bytes_raw` line.
  6. `overlay_scaler`: delete the `envf` closure and its comment. Each knob becomes `if let Some(v) = env_parse::<f64>(env, "SCALE_…") { self.scaler.x = v as T; }` (or `v / 100.0` for the pct knobs, `v` for rate_window).
     - This is required because `envf` would hold `&mut *env` across `env.get(..)`, which is a borrowck error.
     - It is equivalent because every `envf` default equals the struct default (§1a).
- **Ratcheted scope**: the `overlay_scaler` `#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]` (load.rs:233-237, fn-wide; `scope_lines`/`nested_items`/`syntax_facts`).
  - scope_lines drops from 34 to about 31: the closure and comment lines go, and each knob stays 3 lines.
  - syntax_facts drops. Per knob, the old header (`env.get(K).is_some()`, 5 facts) is replaced by `env_parse::<f64>(env, K)` + `Some(v)` (5 facts), and the body drops from 4 facts to 3. The `envf` line (≈9 facts) goes.
  - The signature swaps path `Environment` for path `Overlay` (1→1).
  - No growth. The casts remain, so the expectation stays fulfilled and the reason is unchanged.
- `src/config/model.rs`:
  - `ServerConfig.unparsed_knobs` (+ `Vec::new()` in `with_knob_defaults`), with the doc "set knobs whose value did not parse; validate refuses while any is listed, so a typo never boots as the default"
  - delete `SseConfig::feed_total_bytes_raw` (doc, field, Default line)
  - docs: `feed_ring_bytes`/`feed_total_bytes` "unparseable warns + default" → "an unparseable value refuses at validate"; `heartbeat_ms`, `tail_max_bytes`, `h1_header_timeout` "0/unparseable = default" → "0 = default"; `ScaleConfig` "parsed as f64 by `envf`" → "parsed as f64"
- `src/config/validation.rs`:
  - `validate()` gets the drain line + one comment, as the first stage.
  - `validate_configured_capacity(release_posture, profile, feed_total_bytes: u64, sse_max_connections, notices)` (5 args, one bool): the `if let Some(raw) … match parsed` block becomes `let max = profile_feed_budget_max(profile); if feed_total_bytes > max { if release_posture { return Err(…) } notices.push(…) }`.
  - Its doc drops "must parse exactly (a typo'd byte count must not masquerade as the default)" and points to `UnparsedKnob`.
  - `validate_posture` passes `self.sse.feed_total_bytes`.
- `src/config/validation_tests.rs`: §3/C3.
- `src/config/tests.rs:218` and `numeric_tests.rs:7-8`: comment-only edits.
- No exception scopes in validation.rs, model.rs or the test files.

Every function stays ≤ 100 lines (`validate()` ≈ 62), nesting ≤ 4, ≤ 5 args. There are no `_ =>` arms on domain enums, and no `name: &str` params: the mt_lint name-param rule sees only `k`.

---

## 5. Mutation analysis

Owners: `src/scaler3.rs` → owner `scaler`, filter `scaler3::`. `src/config/*` (except the untouched `admission_limits.rs`), `src/main.rs` and `src/scaler3/controller.rs` are neither registered nor under a critical prefix. **C2 and C3 select no mutants**, and no owner rows change. A push of all three commits also carries the four unpushed commits' owners (sweep_custody, …), which are unaffected.

C1 in-diff lines (a modified line marks itself and the line above):
- the inserted `cooldown_ms` fn
- 121-122 and 321-322 (reason attributes; no mutants)
- 127-128
- 386-387
- 435-436

Line numbers shift by about 7 once the fn is inserted.

| # | Mutant (cargo-mutants 27.1.0) | Viability | Killed by |
|---|---|---|---|
| 1-3 | `replace cooldown_ms -> i64 with 0` / `with 1` / `with -1` | viable | red test 1 (the split fires); pin 2 (`cooled` splits at 5 s) |
| 4 | `replace State::prune with ()` | viable | existing `all_scaler_state_is_bounded_and_idle_state_expires` (8192 ≠ 4096); pin 3 |
| 5 | :128 `replace < with ==` | viable | red test 1 (both records evicted, len 0); pin 3 |
| 6 | :128 `replace < with >` | viable | red test 1; pin 3 |
| 7 | :128 `replace < with <=` | viable | pin 3 (retained at exactly 700_000) |
| 8 | `replace evaluate_state -> Decisions with Default::default()` (Decisions is an alias, so the default replacement) | viable | pin 2 (expects `[fresh]`) |
| 9 | :386 `replace / with %` (`i64::MIN % 2 = 0`) | viable | pin 2 (`fresh`: age 5_000 < 10_000 → held → `[]`) |
| 10 | :386 `replace / with *` | **unviable**: `i64::MIN * 2` trips deny-by-default `arithmetic_overflow` | |
| 11 | :387 `replace < with ==` | viable | pin 2 (5 s yields `[cooled, fresh]`); red test 1 |
| 12 | :387 `replace < with >` | viable | pin 2 (5 s yields `[cooled]`) |
| 13 | :387 `replace < with <=` | viable | pin 2 (11 s: age 10_000 held → `[]`) |
| 14 | :435 `replace / with %` | viable | pin 2 (`quiet`: 5_000 ≥ 10_000 false → no merge) |
| 15 | :435 `replace / with *` | **unviable** (`arithmetic_overflow`) | |
| 16 | :436 `replace >= with <` | viable | pin 2 (no merge at 5 s); red test 1 (a merge appears) |

**rustfmt caveat:** with the shorter `cooldown_ms(policy)`, rustfmt may collapse the `retain(|_, at| { … })` block at :127-129 into a braceless chained form. That form fits in 100 columns only because the multiply is gone. If it does, the deletion marks :126 as well, which adds `:126 replace < with == / > / <=` on the sketch-idle retain. All three are already killed by `scaler3::tests::runtime_scalers_use_owned_monotonic_time`: the first `evaluate()` at age 0 must keep the sketch (kills `==` and `>`), and `advance_monotonic(SKETCH_IDLE_MS)` must empty it (kills `<=`). Apply `cargo fmt` before `--list` and use its count.

Expected: 16 listed (19 if rustfmt reflows), 14 (17) caught, 2 unviable, **0 missed, 0 timeout**.
- No mutated site sits in a loop condition. The cap `while` at :130 is not in the diff.
- Scaler tests are pure State/ManualClock tests (< 1 s).

---

## 6. Ledgers

| Ledger | C1 | C2 | C3 |
|---|---|---|---|
| `docs/quality/source-allowances.json` | none (re-decided reasons are in-source decisions; reasoned exceptions skip the inventory) | **remove** `{category: effect, path: src/main.rs, owner: crate::main, syntax: std::env::var}`. Otherwise the source gate fails with "1 obsolete source allowances; run the quality ratchet with --prune". Hand-delete the row or run `gate.py --prune`. Keep the `std::thread::available_parallelism` and `GLOBAL` rows. | none: no new statics, macros, globs or effects (`std::any::type_name` is not an effect path; `write!` is an expression macro) |
| `docs/quality/owners.json` | none | **remove** the `crate::main` / `std::env::var` effect row ("Process entry reads the Tokio worker floor …") | none |
| test-inventory / review-mechanisms / scenario map / `src/dst/tests/README.md` | none (no `src/dst` test changes; the pinned scaler test sha is unchanged) | none | none |
| `docs/refactor/architecture-policy.json`, `WIRE-MATRIX.md` | none | none | none (no wire change) |
| `mutation_owners.py` | none (no new file under a critical prefix) | none | none |

`docs/refactor/architecture-baseline.json:620-625` (the main.rs `env_reads` entry) is a frozen historical anchor. `architecture-report.py` reports it as RESOLVED, as a warning only. Do not edit it.

Optional doc, C3: add one sentence under RUNBOOK.md §3.3, "every numeric knob must parse as its type; an unparseable or empty value refuses boot, naming the knob".

---

## 7. Controls

Run after the mutation/gate runs finish (no CPU contention). Expected outputs follow each item.

**C1**
1. Tests only, before the fix: `cargo test --lib scaler3::tests::a_cooldown_beyond_the_millisecond_range_never_elapses`. Expect `panicked at src/scaler3.rs:128:40:` / `attempt to multiply with overflow` / `test result: FAILED. 0 passed; 1 failed`.
2. After: `cargo test --lib scaler3::`. Expect `test result: ok. 9 passed; 0 failed` (6 existing + 3 new). Also `cargo test --release --lib scaler3::` gives ok, 9 passed.
3. After `cargo fmt`: `git diff origin/slate -- src/scaler3.rs > $SCRATCH/c1.diff && cargo mutants --list --in-diff $SCRATCH/c1.diff --file src/scaler3.rs --package streams-slate` lists the 16 mutants in §5 (19 if rustfmt reflowed :126-129).
4. `scripts/quality/mutations.sh`. Expect the `scaler` owner "16 mutants tested: 14 caught, 2 unviable" (or 19/17/2) with no MISSED or TIMEOUT lines.
5. `cargo fmt --all -- --check` gives no output. `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean.
6. `scripts/quality.sh` → `quality ratchets: OK`. Before the reason re-decisions, the gate prints exactly two `accepted exception grew without a new decision:` lines: prune (syntax_facts and two new fingerprint keys) and evaluate_state (syntax_facts +4).

**C2**
1. Before C2 the pin does not compile (E0609/E0599). After: `cargo test --lib config::numeric_tests::tokio_workers_come_from_the_knob_or_the_cores_never_below_two config::tests::default_values_are_pinned` → ok, 2 passed.
2. `rg -n "std::env" src/main.rs` → no output. Clippy is clean, which proves the deleted expectation was the only disallowed-method site.
3. `scripts/quality.sh` → OK. Without the source-allowances prune it prints `1 obsolete source allowances; run the quality ratchet with --prune`.
4. Smoke: `TOKIO_WORKERS=3 timeout 5 target/release/streams-slate --s3-endpoint http://127.0.0.1:1 2>&1 | grep "tokio runtime"` → `tokio runtime: 3 worker threads`.

**C3**
1. Tests only, before the change: `cargo test --lib validate_boundary_tests::validation_rejects_every_unparseable_knob_instead_of_its_default`. Expect `left: Some("configuration invalid (1 problem(s)):")` / `right: Some("configuration invalid (8 problem(s)):")`. And `…a_padded_knob_takes_effect_and_an_empty_one_refuses` → `left: 67108864` / `right: 33554432`.
2. After: `cargo test --lib config::` → all ok. `cargo test --release` (full suite) → ok.
3. Binary refusal: `ABSORB_GLOBAL_GATHERS=1x target/release/streams-slate --s3-endpoint http://127.0.0.1:1; echo $?` prints `Error: configuration invalid (1 problem(s)):` / `  - ABSORB_GLOBAL_GATHERS="1x" does not parse as usize; set a plain number or unset it for the default` and exits `1`, before any store I/O.
4. Profile still boots past validation: `env $(grep -v '^#' deploy/profiles/compute-1g.env | xargs) timeout 5 target/release/streams-slate --s3-endpoint http://127.0.0.1:1 2>&1 | grep -c "does not parse"` → `0`.
5. `scripts/quality.sh` → OK, with no `accepted exception grew` for `overlay_scaler`.

---

## 8. Out of scope (follow-ups)

- **Closed-vocabulary string knobs**: ABSORB_PAUSE=="1", HISTORY_COMPACTOR=="off", STREAMS_DEBUG_TIMING/EXIT=="1", BILLING_METER!="off", FLEET_ALLOW_HTTP_PEERS=="1", FRAME_COMPRESS "1"/"true". A typo such as `ABSORB_PAUSE=true` still reads as the default. Same class, but it needs a different rule (an enumerated vocabulary), so it belongs in a separate item.
- **SSE_FEED_PROJECT_BYTES dev-path warn fallback** (`src/sse/feed.rs:344-348`; release posture already refuses). Touching it means the 1,165-line ceiling file, the `sse_feed` mutation owner and the impl-wide `unwrap_used` on `FeedMemoryBudget`.
- **`src/sse/budget.rs:5-10,16-18` docs** still describe the deleted warn-and-default contract after C3. The file is under the critical prefix `src/sse` and has **no** mutation owner row, so editing it needs a deliberate `owner('sse_budget', 'src/sse/budget.rs', 'sse::')` row. That puts it in the nightly rotation, which needs its own killing check. Recommended as a separate chip.
- **The reviewer's "one scaler range stage"**: D3.
- **The controller's duplicate conversion** (`scaler3/controller.rs:181`, already saturating). Routing it through `cooldown_ms` means passing it from `start()`, whose fn-wide `let_underscore_must_use` scope would grow and need a third re-decided reason. There is no correctness gain.
- **`tokio_workers` in `redacted_summary()`**: this is a startup-log shape change, and main already logs the count.
- **Extending the memprofile cert**: the reviewer says not to.
- **Test-only env reads** (`DST_DRAIN_TRACE`, `MT_CERT_PROJECTS`).
- **`architecture-baseline.json`**: the historical anchor.

---

## 9. Decisions for Søren

- **D1: C3 turns a typo into a boot refusal.** Today, 54 numeric knobs silently take their default when the value does not parse. SSE_FEED_RING_BYTES also logs a warn, and SSE_FEED_TOTAL_BYTES already refuses. After C3 an unparseable value exits 1 with `configuration invalid (N problem(s))` on stderr, before any store opens. The 54 are the 42 `env_parse` names, the 8 SCALE_*/MAX_SEGMENTS_PER_STREAM, SSE_FEED_RING_BYTES, HISTORY_GC_INTERVAL_SECS + HISTORY_GC_MAX_INTERVAL_SECS, and TOKIO_WORKERS.
  - Evidence: no in-repo deploy source carries a fallback-only value (§1a).
  - Not verifiable from the repo: environment set on live Compute apps outside the repo, and operator-supplied `WC_*`/`FEED_TOTAL_BYTES`/`H1BUF`.
  - A bad value would crash-loop the first instance of a rollout. That is the fail-loud posture `validate_engine_settings` already chose for CHAOS-2.
  - **Backward-compatible alternative:** land C3 with `ConfigNotice::UnparsedKnob` (a warning notice, boot continues on the default, exactly as today), then flip it to an error after one release of logs shows zero hits.
  - Recommendation: refuse.
- **D2: parse-rule edges.**
  - (a) Surrounding whitespace is trimmed, so `" 33554432 "` now takes effect. Today it silently defaults for 53 of the 54 names; SSE_FEED_RING_BYTES and SSE_FEED_TOTAL_BYTES already trimmed.
  - (b) An **empty** value (`KNOB=`, often an unexpanded `$VAR`) refuses. Today it defaults, except for SSE_FEED_TOTAL_BYTES, which already refuses.
  - Alternative for (b): treat empty as unset (fully backward-compatible).
  - Recommendation: refuse, because an unexpanded variable is exactly the silent-default mistake this item exists to catch.
- **D3: the reviewer's "one scaler range stage" is not in the plan.** It would refuse NaN, ±inf and negative scaler floats, which `scaler_coercions_preserve_the_existing_float_contract` pins as accepted.
  - After C1 the only overflowing use saturates. Nothing else overflows: SCALE_EVAL_SECS=inf gives a far-future `tokio::time::sleep`, and NaN pct/window compare false or clamp at `max(1.0)`.
  - The stage is therefore policy, not a bug fix. The backward-compatible option is to keep the float contract (recommended). The other option is a separate commit that refuses non-finite or negative scaler values and rewrites that pinned test.

No decision is needed for C1 or C2: neither changes wire or refusal behaviour. C1's `SCALE_COOLDOWN_SECS=inf` = "never re-scale" replaces a release-build "always re-scale" and a debug-build panic.

---

## Skeptic corrections (C1..C11)

Re-verified on the tree at aaf2baa5 (read-only; nothing was built or run). The following hold as written: every quoted line; the 43 direct `env_parse` sites and 42 names (load.rs:41-290), the `envf` closure at :241, the lax parses at :116-119 and :133-151; scaler3.rs:128/:387/:436 and the four prune callers (:239, :271, :331, :445); main.rs:11-14 and :44-52; validation.rs:343-378 and :940-946; the reason-regex and `exception_contracts` fingerprinting (source_rules.py:109-201, :258); the source-allowances/owners rows for `crate::main`; the C1 red trace (col 40 = `policy`, debug panic, release wrap to -1000, fixtures split at u64::MAX/64 with share 0.5); the kill traces for mutants 1-16; pin 2's hot_streak/cooldown arithmetic; the C3 red count (1 → 8); the overlay_scaler fact arithmetic (header 5→5, body 4→3, closure removed); the deploy grep (all values are integer literals, guarded, or `${X:-int}`). None of the ceilinged files is touched. My wc -l equals origin/slate for all nine, plus scaler3.rs 852 and validation.rs 973.

**C1: The merge base in the plan is stale.** `origin/slate` is now aaf2baa5, which is HEAD (`git rev-parse` shows both). The four commits are pushed. Two statements are therefore wrong: line 3 ("Merge base: origin/slate = 6ef3bc64 …") and §5 ("A push of all three commits also carries the four unpushed commits' owners"). The mutation diff contains only this item. The ceilings and budgets are unchanged, so no numbers move.

**C2: D3's "Nothing else overflows" is false. A second unchecked scaler multiply lives in the same function.**
- scaler3.rs:421 `e.1 &= sk.cold_streak >= pol.hot_evals * 4;` is a u32 multiply.
- `SCALE_HOT_EVALS=inf` is pinned as accepted and gives `hot_evals = u32::MAX` (numeric_tests.rs:13, :28-29), so it overflows. So does any value ≥ 2^30.
- Debug/quality profile: `panicked at src/scaler3.rs:421:34: attempt to multiply with overflow` inside `evaluate_state`, under `Scaler::evaluate`'s `self.state.lock()`, which poisons it. This is the same failure the plan describes for :128.
- Release: `hot_evals = 1<<30` wraps `*4` to 0. `cold_streak >= 0` is then always true, so a HOT multi-segment stream becomes a merge candidate as soon as its cooldown allows.
- Fold it into C1 (or a C1b with the same shape): `fn merge_patience(policy: &ScalePolicy) -> u32 { policy.hot_evals.saturating_mul(4) }`, whose reason is "merges demand 4x split patience; an absurd patience never elapses instead of wrapping to none".
- Red test `scaler3::tests::merge_patience_beyond_u32_never_merges_a_hot_stream`: policy `hot_evals: 1 << 30`, one stream with two `splittable` HOT segments, `evaluate_state` at the fixture time, `assert!(merges.is_empty(), "{merges:?}")`.
  - Debug: the panic above, at `:421:34`.
  - Release: `merges` = `[(sref, "epoch")]`.
- Mutation fallout: editing :421 also selects :420 (the line before). New mutants:
  - `merge_patience` → 0 and → 1
  - :420 `+= → -=` and `+= → *=`
  - :421 `&= → |=` and `&= → ^=`
  - :421 `>= → <`

  Killers:
  - `|=`, `^=`, `<` and →0 die in the existing `segment_order_cannot_erase_a_hot_key_or_change_decisions` (hot segments; asserts `merges.is_empty()`).
  - `-=` dies in debug/quality with a usize underflow panic.
  - `*=` dies in pin 2 (quiet must merge at 5 s).
  - **→1 survives every listed test.** Add a pin with two cold segments whose `cold_streak` enters at `hot_evals*4 - 2` (7 after the increment) and must not merge.
- The evaluate_state ratchet grows by another +2 syntax_facts, which the re-decided reason already covers. If Søren wants this out of scope, correct D3's text and spawn it as a chip instead. Do not leave the claim standing.

**C3: The expected gate output in §7 C1.6 is wrong.** `exception_growth` (source_rules.py:204-216) emits one line per grown metric, not one per scope. Before the reason re-decisions it prints **4** `accepted exception grew without a new decision:` lines, not "exactly two":
- prune `syntax_facts N → N+2`
- prune `unwrap_site:ordinary-call:<q>:<digest(cooldown_ms\tcooldown_ms (policy))> 0 → 1`
- prune `unwrap_site:path:<q>:<digest(cooldown_ms)> 0 → 1`
- evaluate_state `syntax_facts +4` (+6 with C2)

**C4: Controls C2.4 and C3.4 cannot be built on this host.** Neither `timeout` nor `gtimeout` is installed (`which` finds neither on this macOS box). Two replacements:
- C2.4: `perl -e 'alarm 5; exec @ARGV' target/release/streams-slate --s3-endpoint http://127.0.0.1:1 2>&1 | grep "tokio runtime"`, with `TOKIO_WORKERS=3` in the environment.
- C3.4, better: a standing Rust test in C3, `config::tests::the_shipped_memory_profile_sets_only_parseable_knobs`. It feeds the `KEY=VALUE` lines of `include_str!("../../deploy/profiles/compute-1g.env")` into `MapEnvironment` and asserts `load(..).unparsed_knobs.is_empty()`. That turns D1's deploy evidence into a regression. `include_str` is an expression macro, so no macro-dsl row is needed.

**C5: Two of the 54 names already refuse typos in the binary, through clap.** cli.rs:159 `#[arg(long, env = "COMPACTOR_POLL_MS", …)]` and cli.rs:164 `env = "COMPACTOR_MAX_CONCURRENT"`. `CliArgs::parse()` (main.rs:30) rejects `1x` with a clap error, exit code 2, before `load` runs. They are silent only in `MapEnvironment` tests.
- D1's production count is therefore **52**, and §1a/D1 should say so.
- D2(a) is inconsistent for these two names: clap refuses `" 4 "` while C3 would trim and accept it. Clap's empty-env handling is also not C3's "empty refuses" rule.
- Either state the divergence in D2, or keep these two names out of any whitespace or empty example. The current red tests already avoid them.

**C6: Citation fixes.**
- `ScaleConfig::default()` is model.rs:480-493. Lines 494-503 are `AdmissionConfig`.
- `("inf", None, None)` is numeric_tests.rs:13. Line :14 is `-inf`.
- "The only production `std::env::var` outside environment.rs" overlooks src/bin/pilot.rs:40, a separate workload binary with its own reasoned expect. This is harmless.
- `bench/docker/harness/d3run.sh:4` (a comment) is missing from the list of matches that are not settings.

**C7: Wording in §1a.** The SSE_FEED_TOTAL_BYTES "warn + default" branch is not dead at boot. `tracing_subscriber` initialises at main.rs:16, before `load` at :31, so the warn prints and `validate()` then refuses. Say "redundant", not "dead".

**C8: A documented contract breaks the moment C3 lands.**
- src/sse/budget.rs:5-10 and :16-18 state the warn-and-default contract.
- The plan's reason for deferring is correct. Doc comments are `#[doc]` tokens, so the edit is a production change under the critical prefix `src/sse` with no owner row.
- But the chip must be spawned **in the same session C3 lands**, not "later", or C3 should add `owner('sse_budget', 'src/sse/budget.rs', 'sse::')` together with a killing check for the two FnValue mutants of each getter.
- (docs/LIVE-FEED.md:96-97 SSE_HUB_* fallback text is stale from before this item. It is not in scope.)

**C9: The rustfmt-reflow caveat understates the scope.**
- If :127-129 collapse into `self.last_transition_ms` plus `.retain(|_, at| … cooldown_ms(policy).max(SKETCH_IDLE_MS));` (95 columns, so it fits), `});` is deleted.
- Depending on how cargo-mutants resolves "the line after a deletion", the `while self.last_transition_ms.len() > SKETCH_MAX` condition can enter scope. That adds `> → ==`, `> → <` and `> → >=`.
- All three are killed by `all_scaler_state_is_bounded_and_idle_state_expires` (`len == SKETCH_MAX`).
- `<` on a small table panics at the `.unwrap()` of an empty `min_by_key` instead of looping, so there is no TIMEOUT.
- Change §5's "No mutated site sits in a loop condition" to a conditional statement, and accept 16 / 19 / 22 from `--list`.

**C10: Residual gap to record in D1.** `ProcessEnvironment::get` is `std::env::var(k).ok()` (environment.rs:29). A non-UTF-8 value becomes `None`, which still means "silently default" after C3. It is out of scope, but D1 should not claim that every set-but-unparseable knob refuses.

**C11: Red test C3 #2 depends on the D2(b) decision.** If Søren picks "empty = unset", the second half becomes `validate_with(|_| {}, &[("POSTINGS_CACHE_BYTES", "")]).unwrap().config().postings.cache_bytes == 64 MiB`. State both variants so the test is written after D2 is answered.

Missed ledgers: none beyond C1's stale merge-base note. The C2 prune of the source-allowances row is gate-enforced (source_gate.py:60-62). The owners.json row is not gate-checked for staleness, but it must still go: while it stays, `registered = active | owners` would keep admitting a future `std::env::var` in `main`. test-inventory covers only `src/dst`, and review-mechanisms pins scaler3 by function sha (the pinned test is untouched), so no ledger row changes.

**Verdict: ready-with-corrections.** C1 and C2 can land after C2 (the :421 multiply), C3 (the gate output) and C4 (the controls) are folded in. C3 stays gated on D1 and D2, with C5, C8, C10 and C11 applied to its text.
