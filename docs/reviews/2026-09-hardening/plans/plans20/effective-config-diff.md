# Effective-configuration comparison: rc.4 vs HEAD, per deployed family

Tree: `slate` @ 46f668b3 (= origin/slate). Old release: `v0.2.0-rc.4` = 685ea035 (tag object 92f372f1), an ancestor of HEAD, 734 commits behind.
Status: plan only. Nothing here was edited or built. All numbers below were produced with read-only commands (`git show`, `git grep`, `grep`, `sed`, `wc`, and small Python scans over `git show` output) on 2026-09-24.

Owner-adopted prerequisite (external review, 2026-09-24): no deploy of the hardened binary until an old-vs-new **effective** configuration comparison exists for every deployed configuration family, using the real argv and env.

Summary of the design:
- The HEAD side is exact. A 30-line committed `examples/effective_config.rs` goes through the public facade (`CliArgs` → `ServerConfig::load(…, &ProcessEnvironment)` → `{:#?}` → `validate()`).
- The old side is the first revision that has rc.4's configuration as one value: 6bcaff69 (WP-01 PR 3, 3 commits after rc.4). An **uncommitted** dumper is injected there, and an equivalence gate (E1, E2 mechanical and E3 reviewed) ties it back to rc.4.
- Both dumpers are **non-test** builds.
- Name-level differences are found by behavioural probing, not by regex.
- Boot refusals come from the real rc.4 and HEAD binaries booted against a local s3lite (Leg C).
- No production Rust changes. No edge changes. No ledger rows.

---

## 1. Problem (verified)

### 1.1 Three premises of the request that do not hold on the tree

**(a) rc.4 has no `ServerConfig`, `CliArgs` or `Environment`, so there is nothing at rc.4 for a dumper to load.**
- `git ls-tree v0.2.0-rc.4 src/` has no `src/config/` and no `src/lib.rs`. rc.4 is a binary-only crate.
- Its configuration is a private clap struct, `src/main.rs:72-74`:
  ```rust
  #[derive(Parser, Debug)]
  #[command(name = "streams-slate", about = "Durable Streams server on SlateDB")]
  struct Args {
  ```
  The struct block is 522 lines and has 84 fields, 79 of them with `env = "…"`.
- On top of that come **65 direct `std::env::var` sites** in non-bin, non-DST sources, covering **72 names**. Six of the sites are generic helpers keyed by a parameter:
  - `scaler3.rs:58` `envf`
  - `backpressure.rs:66` `v`
  - `usage.rs:142` `envf`
  - `main.rs:679` `env_usize`
  - `main.rs:685` `env_u64`
  - `main.rs:2428` `genv`
- Many of these reads are inline in long functions, for example `main.rs:619` POOL_IDLE_SECS inside `store_for`, `billing.rs:1467` OUTBOX_SWEEP_SECS, and `fleet.rs:389/393/480/682/880`. They cannot be called from a dumper.
- The config graph first appears at 6bcaff69 (2026-09-01, "WP-01 PR 3: one parsed, immutable configuration graph (AppConfig)"). `git log v0.2.0-rc.4..6bcaff69` = 4104afc3 (WP-00: test-only, plus 1 line of main.rs and 14 lines of shard.rs), cf169936 (PR 2: lib split), 6bcaff69. The PR 3 commit body reads: "AppConfig (13 sub-configs) owns ALL 71 environment knobs … Invariant IDs touched: none — env-read centralization, zero behavior edits."

**(b) A `cfg(test)` dumper misreports the old revision's shipped configuration.**
- rc.4 `src/history.rs:277-284`:
  ```rust
  #[cfg(test)]
  let default_bytes: usize = 4 * 1024 * 1024 * 1024;
  #[cfg(not(test))]
  let default_bytes: usize = 64 * 1024 * 1024;
  #[cfg(test)]
  let default_gathers: usize = 64;
  #[cfg(not(test))]
  let default_gathers: usize = 2;
  ```
- 6bcaff69 `src/config/mod.rs:650-655` carries the same fork into `AppConfig::default()`: `absorb_global_budget_bytes: if cfg!(test) { 4 * 1024 * 1024 * 1024 } else { 64 * 1024 * 1024 }` and `absorb_global_gathers: if cfg!(test) { 64 } else { 2 }`.
- The fork was removed at HEAD only by bc43a2bb ("The absorber budget default is the shipped 64 MiB and two gathers in every build").
- A test-build dumper at either old revision would therefore report 4 GiB and 64 gathers for any family that does not pin these two knobs. Both dumpers must be **non-test** builds (§2.3).

**(c) Compute does pass argv, and the env is more than the script's `--env` list.**
- `deploy/app-server/index.ts:94`: ``await superviseBinary(bin, ["--listen", `0.0.0.0:${port}`]);``
- `supervise.ts:22-23` spawns `[bin, ...argv]` with `env` = the supervisor's `process.env`. That env includes `APP_BINARY_SHA256`, which the supervisor itself sets at `index.ts:86` (`process.env.APP_BINARY_SHA256 = hasher.digest("hex");`).
- `RUNBOOK.md:428-429`: "Compute env vars are **project-scoped and merged**: every deploy snapshots the union of everything ever set in the project."
- `bench/soak/wc-ladder.sh:7-8`: "platform env vars persist across deploys, so omitting it kept an old =0 on the service".
- So the real env of a Compute server is the union of every role's `--env` in the project (server, gen, lb, and older campaigns), plus supervisor and platform variables. A script alone does not give the real env (§9 D2).

### 1.2 HEAD: every configuration input of the shipped binary (full use-site list)

| Input | Site | Notes |
|---|---|---|
| argv + 79 clap env names | `src/main.rs:26` `let cli = streams_slate::CliArgs::parse();` (surface: `src/config/cli.rs`, 84 fields) | the only production `CliArgs` parse |
| 69 loader env names | `src/main.rs:27` `let parsed = streams_slate::ServerConfig::load(cli, &streams_slate::ProcessEnvironment);` → `src/config/load.rs` (`overlay_env` + 13 `overlay_*`) | the **only** production `ServerConfig::load` call. Every other call is test code: `runtime.rs:417,458`; `bootstrap/tests.rs:47`; `config/tests.rs:81,87,100,114,144,356,605,691`; `config/numeric_tests.rs:16,40,60,67,78`; `config/certification_tests.rs:6,30`; `config/validation_tests.rs:16,219,486`; `runtime/telemetry.rs:69` (under `#[cfg(test)]` at :63); `usage/runtime_tests.rs:6`; `dst/tests/admission_maintenance.rs:182`; `dst/tests/fixture_http.rs:334`; `rollup/tests.rs:50`; `store_timing/resources.rs:203` (under `#[cfg(test)]` at :190); `fleet/repository/document_tests.rs:210,220`; `billing/tests.rs:9`; `billing/read_spool/tests.rs:36,143,184,236` |
| process env reader | `src/config/environment.rs:24-31` `ProcessEnvironment::get` = `std::env::var(key).ok()` | the only production `std::env::var`. It is guarded by `clippy.toml` `disallowed-methods` (`std::env::var`, `var_os`, `vars`, `vars_os`). The other `std::env::var` sites are all `#[cfg(test)]` DST_DRAIN_TRACE traces: `shard/transaction/maintenance.rs:159-160`, `history/worker.rs:284-285`, `history/gather.rs:225-231` |
| RUST_LOG | `src/main.rs:12-16` `EnvFilter::try_from_default_env()` | logging only; identical at rc.4 `main.rs:1671-1676` |
| validation | `src/main.rs:28` `parsed.validate()` → `src/config/validation.rs:652-713` | pure: "no environment reads, no stores, no spawns … NO LOGS" (:645-651) |
| worker threads | `src/main.rs:35-36` `worker_threads(std::thread::available_parallelism().ok())` | an OS probe, not part of the graph |
| nofile preflight | `src/bootstrap.rs:183-184` `resolve_effective_capacity(configured_capacity, limits, …)` | the one post-validate pre-I/O refusal; OS-dependent |

Name sets:
- HEAD reads 146 names: 69 in `load.rs` (string-literal scan) plus 79 clap names, with COMPACTOR_MAX_CONCURRENT and PATH_PREFIX read by both. RUST_LOG comes on top.
- The old side (6bcaff69 `ENV_KNOBS` 70 + old clap 79 − 5 overlaps + TOKIO_WORKERS) has 145.
- Static delta: **new-only {SSE_H1_HEADER_TIMEOUT_MS}; dropped: none**. BILLING_MODE, ROLLUP and COMPACTOR_POLL_MS moved from dual readers to clap-only readers.

### 1.3 The only effective-configuration output is hand-curated and omits most of what deploys set

- `src/bootstrap.rs:210`: `tracing::info!(config = %config.redacted_summary(), "effective configuration (redacted)");`
- `src/config/summary.rs:3-6`: "This is an EXPLICIT projection, not a derived serialization of the whole graph: a new field on `ServerConfig` does NOT appear in the summary until someone adds it here deliberately. `cli` is excluded wholesale — it carries key material and tokens."
- What it drops: **all 84 CLI fields**, and 3 of the 71 knob fields (`sse.feed_total_bytes_raw`, `runtime.cert_sealed_publish_delay_ms_raw`, `runtime.tokio_workers`).
- The dropped CLI fields include most of what the families actually set: FLUSH_INTERVAL_MS, WAL_*, L0_*, MAX_UNFLUSHED_BYTES, SHARED_CACHE_BYTES, ADMIT_*, SSE_MAX_CONNECTIONS, MAX_RECORD_PAYLOAD_BYTES, INITIAL_SHARDS, FLEET_*, BILLING_MODE, ROLLUP, STREAMS_AUTH_*, STREAMS_RELEASE_POSTURE and SCALE_*_CPU_*.
- Other `redacted_summary` callers: `config/tests.rs:357`, `config/numeric_tests.rs:91,96` (tests).
- rc.4 has no counterpart at all.

### 1.4 At rc.4, most refusals come after store I/O

rc.4 `async_main` runs in this order:
1. `:1736` `args.store_for(…)?` and `:1782` the canary `put_opts`
2. `:1821-1825` PROJECT_ID (panics)
3. `:1828` auth mode
4. `:1876-1878` STREAMS_CURSOR_KEY
5. `:1895` `validate_fleet_auth`
6. `:1896` record ceiling
7. `:1906` `validate_release_capacity`
8. `:2384-2400` BILLING_MODE=required checks
9. `:2504` `TcpListener::bind`

Only `assert_certified_memprofile()` (`:1679`) and the SWEEP_MAINT_RESIDENT=0 check (`:1683-1693`) run before any I/O. So rc.4's refusal verdict for a family can only be observed by booting it (Leg C).

At HEAD every pure configuration check is inside `validate()`. `AuthService::new`'s cell-id check (`auth.rs:390`) repeats `CellId::new`'s `validate_cell_id` (`tenant.rs:215`). What still runs after `validate()` depends on something other than the configuration value:
- the OS: nofile (`bootstrap.rs:183`)
- the stores: canary (`:233-282`), BILLING_MODE=required spool and rollup opens (`:681`, `:692`)
- persisted namespace state: the stored MAX_REQUEST_BODY_BYTES versus `--max-request-body-bytes` (`:348-356`)
- the process: `RUN_WAS_INVOKED` (`:146-150`)

The HEAD bind is at `bootstrap.rs:696`, after all of it.

### 1.5 Families (sources on HEAD) and how they drifted since rc.4

Compute deployers. Each one sources `deploy/profiles/compute-1g.env` (24 knob lines) as `--env` unless `UNSAFE_LEGACY_MEMORY_PROFILE=1`, and runs under the supervisor argv `--listen 0.0.0.0:$PORT`.

| Family | Source | `--env` names in the whole script (all roles = project-merged set) |
|---|---|---|
| fleet-server-1 / fleet-server-n | `bench/fleet/deploy-fleet.sh:84-136` (i=1 adds KEEP_AWAKE=1 and ROLLUP=1; SELF_URL only when the url file exists) | 69 |
| region-server (+ `SCALE_KNOBS=1` variant) | `bench/soak/deploy-region.sh:138-181` | 57 |
| mt-tenants-off / mt-tenants-enforce | `bench/soak/mt-tenants.sh:129-151,211,218-226` | 61 |
| wc-ladder (+ `WC_DIET=1` variant) | `bench/soak/wc-ladder.sh:65-94,181-212` (RUNGFLAGS last: SSE_MAX_CONNECTIONS=2000 overrides the profile's 1200) | 88 |
| fra-ab-server | `scripts/bench-fra-ab.sh:75-90` | 38 |
| *(excluded)* | `bench/docker/harness/cluster-deploy.sh:2` "HISTORICAL … DO NOT DEPLOY FROM THIS FILE"; `:9-12` refuses without the opt-out | — |

Local release-certification family: `bench/canary/livefeed-canary.mjs:79-126`. It is the only STREAMS_RELEASE_POSTURE=1 family: enforce auth, workload fleet auth, and argv `--listen … --s3-endpoint … --bucket … --flush-interval-ms 1 --wal-flush-gap-ms 2`. It inherits `...process.env` from the invoking shell (`:80`), so it is non-hermetic.

Other local rigs (`bench/costab/*`, `bench/sse-probes/*`, `bench/fleet/local-fanout.sh`, `bench/livefeed-perf/run-one.sh`, `bench/docker/compose.yml`, `scripts/platform-e2e.mjs`, `scripts/mt-noisy-campaign.mjs`, `bench/fleet/livefeed-cert.mjs`) are not deployments (§9 D3).

Static prediction for the five Compute scripts (not a result):
- OLD_ONLY = ∅ and NEW_ONLY = ∅ for names the scripts set.
- NEITHER = only supervisor, other-role or platform names (BIN_S3_*, SERVER_BINARY_S3_KEY, KEEP_AWAKE, RESOLV_OVERRIDE, FEEDS_S3_KEY, TOKENS_S3_KEY, the gen/lb BENCH_*/S3_*/CONC_*/STREAMS/STREAM_KEY/MODE/LB_URL/UPSTREAMS/…).
- All 24 profile names are read by both sides.

Drift since rc.4: `ABSORB_PASS_BYTES=67108864` was removed from 13 scripts, including the Compute ones: deploy-fleet, deploy-region, mt-tenants, wc-ladder and bench-fra-ab. At HEAD it is "accepted but ignored" (99d5c098, `cli.rs:645-656`). It was already inert at rc.4: `AbsorberConfig.{pass_bytes, small_pass_bytes, concurrency}` are set at rc.4 `history.rs:729-731` and `main.rs:2136-2137` and never read (`git grep '\.pass_bytes\|\.small_pass_bytes'` on rc.4 finds no reader). Because of the §1.1(c) merge trap, the value probably still sits in each Compute project's env.

### 1.6 Found in passing (not fixed here)

- COMPACTOR_MAX_CONCURRENT is read twice at HEAD: as clap `cli.compactor_max_concurrent`, and by the loader as `engine.compactor_max_concurrent` (`load.rs:64`).
  - Only `engine.*` has a consumer (`model.rs:100,109`), so `--compactor-max-concurrent` given on argv never reaches the compactor.
  - This is the same class as item 32 (fdc31b07 fixed only `--compactor-poll-ms`).
  - PATH_PREFIX has the documented split: `cli.path_prefix` for stores (`bootstrap.rs:64,233,688,884`), `billing.path_prefix_env` for the read spool (`billing.rs:1132`).
  - Compute families are unaffected because they set these only through env.
- `scripts/bench-fra-ab.sh:84` sets `MANIFEST_POLL_MS=1000`, against `RUNBOOK.md:103` ("deploy scripts must not re-tighten it").
- Existing partial verifiers, none of which is a comparison:
  - `bench/soak/oom-acceptance.sh verify`: 12 memory knobs, read live from `/v1/debug/absorb`.
  - MEMPROFILE_CERT: 7 measurements (`config/profile.rs`).
  - `config::tests::cli_surface_is_pinned`: HEAD names and defaults only.

---

## 2. Contract decision

### 2.1 What changes at the edge: nothing

- The shipped `streams-slate` binary is byte-for-byte unaffected. Examples are separate Cargo targets, and neither `Cargo.toml` nor `Cargo.lock` changes (clap is already `Cargo.toml:18`).
- No wire, status, log or boot behaviour changes.
- There are no edge-change records.

### 2.2 The two sides

- **New (HEAD, exact).** A committed `examples/effective_config.rs` uses only the public facade (`src/lib.rs:76-77`: `CliArgs`, `ServerConfig`, `ProcessEnvironment`, `ConfigError`). It prints the complete `ServerConfig` Debug (derived, so a new field appears automatically), then the `validate()` verdict. At HEAD the graph is complete by construction (§1.2).
- **Old model (6bcaff69, injected, never committed there).** A lib module `effective_config_dump` prints:
  - `crate::bootstrap::Args` Debug
  - `crate::config::AppConfig::load()` Debug
  - `runtime.tokio_workers` from the one reader AppConfig did not own. This is a verbatim transcription of `std::env::var("TOKIO_WORKERS").ok().and_then(|v| v.parse().ok())`, anchor-checked in 6bcaff69 `src/main.rs:45-47` **and** rc.4 `src/main.rs:1699-1701`.

  The injected module is reached through an injected example. The old side has no single validation function, so its verdict is `not-evaluated`; rc.4's refusal truth comes from Leg C.
- **Old boot (rc.4, exact).** Leg C boots the real rc.4 binary.
- **Equivalence gate rc.4 ≡ 6bcaff69 on configuration** (the price of D1):
  - **E1 (mechanical, verified today):** rc.4 `src/main.rs:72` `Args` block (522 lines) and 6bcaff69 `src/bootstrap.rs:23` `Args` block (522 lines) are **identical** once `pub(crate)` is stripped. The clap parser (`parse_bool_flag`) and the defaults constants (`DEFAULT_{MANIFEST,COMPACTOR}_POLL_MS` = 2000 and 2500) are the same.
  - **E2 (mechanical, verified today):** rc.4's production env name set (72) = 6bcaff69 `ENV_KNOBS` (70, `config/mod.rs:746-816`) + {TOKIO_WORKERS (transcribed, above), DST_DRAIN_TRACE (`#[cfg(test)]` only at rc.4 `history.rs:1164-1165`, `:1580-1581`, `shard.rs:3205-3206`, and in the `#[cfg(test)] mod dst`)}. There is nothing in `ENV_KNOBS` that rc.4 did not read.
  - **E3 (reviewed once, owner-signed):** a generated 70-row table showing, per knob, the rc.4 read expression next to the 6bcaff69 overlay expression, with defaults auto-compared by evaluating simple literal arithmetic. This is the one place the design trusts a transcription (PR 3's, which is reviewed and was gated GATEDONE at the time) instead of executing rc.4 code.

### 2.3 Build flavour, isolation, secrets, disk

- **Non-test dev builds**, with release arithmetic: `CARGO_PROFILE_DEV_OVERFLOW_CHECKS=false`, `CARGO_PROFILE_DEV_DEBUG_ASSERTIONS=false`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_INCREMENTAL=0`. `git grep 'debug_assertions\|cfg!(test)'` finds no other config-path fork at rc.4, 6bcaff69 or HEAD beyond §1.1(b). Control K5 pins this.
- Trees come from `git archive <rev> | tar -x` into the scratch work dir. This leaves no repo state, no worktree metadata and no checkout.
- `build.rs` gets `STREAMS_GIT_COMMIT=<rev>` and `SOURCE_DATE_EPOCH=0`. Both revisions' `build.rs` honour these, which is needed without `.git`.
- `RUSTUP_TOOLCHAIN=1.98.1` everywhere. Neither old revision has `rust-toolchain.toml` or `#![deny]`/`[lints]`, so newer-compiler warnings cannot fail them.
- All builds use `--locked`. One `CARGO_TARGET_DIR` inside the work dir is shared by all three trees, so dependencies (same slatedb rev `0717cc1e` in all three lockfiles) build once.
- **Disk:** expected 3-5 GiB total. There are no test builds, so the DST suite is never compiled. The tool refuses to start below 12 GiB free (measured free today: 31 GiB), reports `du -sk` afterwards, and deletes the work dir unless `--keep`.
- **Isolation:** every dumper and boot run is `env -i <family env> <binary> <family argv>`. Control K4 proves ambient variables do not leak in.
- **Secrets:** a family value for a secret-class name must start with `placeholder-`, or equal one of two canned shape-valid constants: `CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=` (32×0x09, the canary's own test key) and `AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=`.
  - Secret-class names: the explicit set SLATE_S3_ACCESS_KEY_ID, SLATE_S3_SECRET_ACCESS_KEY, AUTH_TOKEN, FLEET_INTERNAL_TOKEN, USAGE_STREAM_KEY, STREAMS_CURSOR_KEY, STREAM_KEY, `*_ACCESS_KEY_ID`, `*_SECRET_ACCESS_KEY`, plus any name matching `(SECRET|TOKEN|PASSWORD)` that does not end in `_FILE`.
  - Otherwise the tool refuses before building or running anything (K7).
  - Placeholders are shape-valid for `validate()`: FLEET_INTERNAL_TOKEN ≥ 16 characters and different from AUTH_TOKEN; the cursor and usage keys decode to 32 bytes.
  - Outputs therefore contain placeholders only.

### 2.4 Output model

- Each dumper prints sections headed `@@ <prefix>`, each followed by one pretty-Debug value:
  - new: `@@ root` (the whole `ServerConfig`, which includes `cli`), then `@@ verdict`
  - old: `@@ cli`, `@@ root`, `@@ runtime.tokio_workers`, `@@ verdict`
- The Python flattener turns each value into `path = value` leaves. It drops the root type names, which removes the `Args`/`CliArgs` and `AppConfig`/`ServerConfig` differences.
- Leaf counts: new 155 (84 CLI + 71 knobs), old 156 (84 + 71 + injected `runtime.tokio_workers`).
- `scripts/effective-config/rename-map.json` must account for every path present on one side only:
  - old-only `billing.mode_env` ↔ `cli.billing_mode` (46d4b7df, edge record #49)
  - old-only `billing.rollup_env` ↔ `cli.rollup` (46d4b7df)
  - new-only `http.h1_header_timeout` (old semantics: "no request-head deadline"; 71345c03, edge record #12)

  Any unaccounted path fails the run (K9). This is how renamed and added fields are handled.

### 2.5 Name-level classification by behaviour

For each name a family sets, and on each side, the dumper runs twice more: once with the name **removed**, and once with the value **perturbed** (an `x` appended to strings; a `1` appended to numbers). If either run changes the flattened output or the verdict, the name is honoured on that side. Classes:
- BOTH
- OLD_ONLY (honoured by rc.4's model, ignored by HEAD; a blocker unless accepted)
- NEW_ONLY
- NEITHER (reported with its provenance: supervisor, platform or other-role)
- PROCESS, for RUST_LOG and `MIMALLOC_*`, which are read outside the graph by tracing and the allocator on both sides, as a declared list

No source regex is involved at runtime. The regex scan in §1 missed multi-line `env\n.get("…")` forms on its first attempt, which is the reason for this choice.

### 2.6 Leg C: real boot of both binaries

Per family and per side:
1. Start a fresh in-memory `s3lite --latency-ms 2` (HEAD build; it ignores `Authorization`, `s3lite.rs:6`).
2. Apply the declared substitutions: `--listen` → `127.0.0.1:<free>`; `SLATE_S3_ENDPOINT` / `--s3-endpoint` → the s3lite URL. They are listed in the report.
3. Run `env -i`, polling TCP connect every 250 ms, and classify:
   - **BOOTED**: connected, and still alive 3 s later
   - **REFUSED**: exited before binding (exit code + last 40 stderr lines)
   - **DIED_AFTER_BIND**
   - **TIMEOUT** (120 s): a tool failure, not a verdict
4. Stop with SIGTERM, then SIGKILL after 10 s.
5. Keep the boot logs, which contain placeholders only.

Neither binary needs auth feed or workload files to bind: HEAD reads them in the spawned refresher (`bootstrap.rs:744-756`) and lazily for the token (`:589-600`), and rc.4 does the same (`main.rs:2233`, `:2281`).

### 2.7 What the owner reviews, and what counts as a pass

The tool writes `annotations.template.json`, with one row per reviewable item and `decision: null`. An agent may fill `proposed` (commit or edge record), **never** `decision`.

The owner reviews:
- **R1.** Every value-diff row, including the `defaults` family. Rows identical across families collapse into one row with its list of families.
- **R2.** Every OLD_ONLY and NEW_ONLY row.
- **R3.** Each family's NEITHER list, to confirm none of those names is a misspelled knob.
- **R4.** Each HEAD verdict, and each Leg C verdict on both sides.
- **R5.** The E3 transcription table (one decision).
- **R6.** The drift report: rc.4-era vs HEAD-era `--env` names per script (§1.5).
- **R7.** The Leg C boot-log budget summaries, rc.4 vs HEAD, for the compute-1g families (eyeball only).

**PASS** requires every one of:
- **P1.** E1 and E2 PASS, and E3 `accept`.
- **P2.** K1-K12 all PASS in the same run that produced the evidence.
- **P3.** Every in-scope family has HEAD verdict `accepted` and Leg C HEAD `BOOTED`. A rc.4 non-BOOTED result is flagged for R4 but does not block by itself.
- **P4.** Every R1/R2/R4 row is `accept`: zero `null` and zero `reject`.
- **P5.** `check-families` is clean, and the evidence header records the three revisions, the toolchain, the sha256 of each family file and the sha256 of each dumper and binary.
- **P6.** For every Compute project that will actually receive the HEAD binary, a platform-export family (D2) also meets P3-P4.

---

## 3. Red tests / acceptance checks, pins, non-vacuity controls

### 3.1 Red-first order and exact red outputs

- **C1 red:** `cargo build --locked -p streams-slate --example effective_config` on HEAD before C1. Expected first line: ``error: no example target named `effective_config` ``, followed by cargo's available-example list, which contains `readertest`.
- **C2 red:** `python3 -m unittest discover -s scripts/effective-config -v` with only the test file present. Expected: one import `ERROR` for `test_effective_config` containing `ModuleNotFoundError: No module named 'effective_config'`, and the run ends `FAILED (errors=1)`. This holds on Python 3.9.6 (local) and 3.11 (CI).
- **C3 red:** `python3 scripts/effective-config/effective_config.py boot --work "$W"` before C3. argparse exits 2 with `error: argument command: invalid choice: 'boot'`.

### 3.2 Python unit tests (`scripts/effective-config/test_effective_config.py`)

Pure tests with inline fixtures. No cargo and no network.

| Test | Asserts exactly |
|---|---|
| `FlattenTest.test_nested_struct_option_duration_path` | a fixture `ServerConfig {…}` pretty-Debug gives `cli.initial_shards = Some(16)`, `shard.open_deadline = 180s`, `history.gc_interval = Some(600s)`, `sse.feed_total_bytes_raw = None`, `cli.streams_auth_keys_file = Some("/tmp/feeds/keys.json")`, `scaler.hot_pct = 0.75` |
| `FlattenTest.test_sections_prefix_paths` | `@@ cli` + `Args { listen: "x", }` + `@@ root` + `AppConfig { http: HttpConfig { h1_max_buf: 65536, }, }` + `@@ runtime.tokio_workers` + `None` gives `{cli.listen: "x", http.h1_max_buf: 65536, runtime.tokio_workers: None}` |
| `FlattenTest.test_string_escapes_and_commas` | `"a,\"b\""` stays one leaf |
| `FlattenTest.test_unparsed_line_fails` | raises `ValueError` whose message starts `unparsed Debug line 3:` |
| `FamilyTest.test_include_then_override_last_wins` | `include deploy/profiles/compute-1g.env` then `env SSE_MAX_CONNECTIONS=2000` gives the value `2000`, and `overridden == [("SSE_MAX_CONNECTIONS", "1200", "2000")]` |
| `FamilyTest.test_secret_requires_placeholder` | `env AUTH_TOKEN=abc` raises `FamilyError("AUTH_TOKEN must be a placeholder")` |
| `FamilyTest.test_empty_value_rejected` | `env FOO=` raises `FamilyError("FOO: empty value (the Compute CLI rejects --env KEY=)")` |
| `FamilyTest.test_provenance_kept` | `supervisor APP_BINARY_SHA256=placeholder-sha` has provenance `supervisor` |
| `RenameTest.test_unaccounted_path_fails` | an old-only `billing.foo` raises `unaccounted old-only path billing.foo` |
| `RenameTest.test_declared_pair_compares_values` | old `billing.mode_env = Some("required")` with new `cli.billing_mode = "required"` gives a row of kind `paired`, marked `equal-effective` |
| `ProbeTest.test_classes` | (old changed, new changed) → BOTH / OLD_ONLY / NEW_ONLY / NEITHER; `RUST_LOG` → PROCESS |
| `EquivalenceTest.test_args_block_visibility_normalized` | two fixture blocks that differ only in `pub(crate)` compare equal; a changed `default_value_t` compares unequal |
| `EquivalenceTest.test_env_name_extraction_helpers` | a fixture with `std::env::var("A")`, `envf("B", 1.0)`, `env_usize("C", 4)`, `genv("D", 1)` and, in `backpressure.rs`, `v("E", 1)` gives {A,B,C,D,E}; a fixture with `std::env::var(k)` inside an unlisted helper raises `unresolved generic env helper` |
| `BootTest.test_classify` | exit before connect → REFUSED; connect and alive at +3 s → BOOTED; connect then exit → DIED_AFTER_BIND; neither within the budget → TIMEOUT |

### 3.3 Tool controls (`effective_config.py controls --work "$W"`), with exact expected lines

- **K1** `K1 PASS identity(new): fleet-server-1 vs fleet-server-1: 0 differences`
- **K2** `K2 PASS sensitivity(old): defaults vs defaults+SHARD_OPEN_WAIT_MS=10001: exactly [shard.open_wait_ms: 10000 -> 10001]`, plus the same line with `(new)`
- **K3** `K3 PASS argv(old|new): --flush-interval-ms 99: exactly [cli.flush_interval_ms: 25 -> 99]`
- **K4** `K4 PASS isolation: ambient FLEET_MIN=9 not observed (fleet.fleet_min = 1 on both sides)`. The control exports FLEET_MIN=9 into the tool's own environment before calling the dumpers.
- **K5** `K5 PASS release flavour(old): history.absorb_global_budget_bytes = 67108864, history.absorb_global_gathers = 2`. A test-build dumper would print `4294967296` and `64`; this is the red that justifies §2.3.
- **K6** `K6 PASS refusal(new): defaults+SSE_H1_MAX_BUF=4096 -> refused`. The verdict block is exactly:
  ```
  @@ verdict
  refused
  configuration invalid (1 problem(s)):
    - SSE_H1_MAX_BUF=4096 is below hyper's 8192-byte h1 buffer floor
  ```
- **K7** `K7 PASS secret policy: family with AUTH_TOKEN=not-a-placeholder refused before build/run`. The tool exits 2 with `family k7: AUTH_TOKEN must be a placeholder`.
- **K8** `K8 PASS probe: SHARD_OPEN_WAIT_MS=BOTH SSE_H1_HEADER_TIMEOUT_MS=NEW_ONLY KEEP_AWAKE=NEITHER(supervisor) RUST_LOG=PROCESS`
- **K9** `K9 PASS coverage: old 156 leaves, new 155 leaves; unmatched old {billing.mode_env, billing.rollup_env}, new {http.h1_header_timeout}; all declared in rename-map.json`
- **K10** (Leg C) `K10 PASS boot refusal: defaults+SWEEP_MAINT_RESIDENT=0 -> rc.4 REFUSED exit 1 "Error: SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; set >= 1 or unset (default 2)"; head REFUSED exit 1 "Error: configuration invalid (1 problem(s)):" / "  - SWEEP_MAINT_RESIDENT=0 starves all cold-debt drain; set >= 1 or unset (default 2)"`. Sources: rc.4 `main.rs:1683-1693` and HEAD `validation.rs:721-727` via `main.rs:31`.
- **K11** (Leg C) `K11 PASS agreement: every family's head boot verdict is BOOTED iff its head validate verdict is accepted`
- **K12** (Leg C) `K12 PASS positive: defaults BOOTED on rc.4 and head within 120 s`

**Equivalence** (`effective_config.py equivalence --work "$W"`):
- `E1 PASS: Args identical (522 lines): v0.2.0-rc.4 src/main.rs:72 == 6bcaff69 src/bootstrap.rs:23 (pub(crate) normalized)`
- `E2 PASS: rc.4 production env names (72) == 6bcaff69 ENV_KNOBS (70) + {TOKIO_WORKERS: transcribed, anchors 6bcaff69 src/main.rs:45 + v0.2.0-rc.4 src/main.rs:1699; DST_DRAIN_TRACE: #[cfg(test)] only}; generic helpers resolved: 6/6`
- `E3 wrote equivalence/e3-transcription.md: 70 knobs (<n> defaults equal by evaluation, <m> for review)`

**Pins:** the leaf counts 156 and 155 (K9); the rename-map sets (K9); the E1 line count 522; the E2 counts 72/70/6. A future field or knob changes one of these, and the tool then fails until the map or pin is updated deliberately.

### 3.4 Predicted results (hypotheses the run confirms or refutes, not acceptance)

For every Compute family:
- HEAD verdict `accepted`, and BOOTED on both sides.
- Value rows:
  - `cli.absorb_pass_bytes`, `cli.absorb_concurrency`, `cli.absorb_small_bytes`: `268435456 / 6 / 1048576 -> None`. Inert at both (§1.5).
  - `http.h1_header_timeout: <absent> -> 120s`
  - `billing.mode_env` / `rollup_env` paired with `cli.billing_mode` / `cli.rollup`: equal-effective
- Names: OLD_ONLY = NEW_ONLY = ∅. NEITHER = the supervisor and other-role names listed in §1.5.

The `defaults` family shows the same four path rows (static comparison of `Default` impls: no other knob default changed).

---

## 4. Edits, file by file, in commit order

Nothing under `src/` changes. There are no `#[expect]` scopes, no files over 1,000 lines, no DST files and no mutation-owned paths.

**C1 — "The effective configuration of any argv and environment prints without booting"**
- `examples/effective_config.rs`: **new**, about 30 lines. The ceiling is 1,000 (a new file); `examples/readertest.rs` is the precedent for this directory. Body:
  ```rust
  //! Prints the configuration `streams-slate` would run with for the argv
  //! given here and this process's environment, then whether validation
  //! accepts it. Nothing is opened, spawned or logged. The HEAD half of the
  //! old-vs-new effective-configuration comparison (scripts/effective-config/),
  //! which runs it under `env -i` with a family's placeholder variables.
  use clap::Parser;

  fn main() {
      let argv = std::iter::once(String::from("streams-slate")).chain(std::env::args().skip(1));
      let cli = match streams_slate::CliArgs::try_parse_from(argv) {
          Ok(cli) => cli,
          Err(refusal) => {
              print!("@@ verdict\nargv-refused\n{refusal}");
              return;
          }
      };
      let config = streams_slate::ServerConfig::load(cli, &streams_slate::ProcessEnvironment);
      println!("@@ root\n{config:#?}");
      match config.validate() {
          Ok(_) => println!("@@ verdict\naccepted"),
          Err(refusal) => print!("@@ verdict\nrefused\n{refusal}"),
      }
  }
  ```
- Gate checks:
  - No `unwrap`, `expect` or `panic`.
  - No `std::env::var*`: `std::env::args` is not an effect in `source_rules.py:46`, and env reads go through the owner `ProcessEnvironment`.
  - The only macros are `print!` and `println!`, both in `EXPRESSION_MACROS`.
  - One function of fewer than 20 lines; nesting ≤ 2.
  - No `print!` whose literal ends in `\n`, so `print_with_newline` does not fire.
  - rustfmt-clean.
  - No `docs/quality/*` rows.
- Commit body: no production change, not in the shipped binary, no ledger change.

**C2 — "An old-vs-new effective-configuration comparison for every deployment family"** (Python ≥ 3.9, no third-party modules)
- `scripts/effective-config/effective_config.py` (new, about 450 lines). Subcommands:
  - `build`: archive, inject, build, disk guard
  - `equivalence`: E1, E2, E3
  - `compare`: dump, flatten, rename, probe, diff, report and annotations template
  - `controls`: K1-K9
  - `drift`: rc.4 vs HEAD `--env` names per source script
  - `check-families`: every `--env` name in each family's source script appears in the family file, under its declared variant
- `scripts/effective-config/test_effective_config.py` (new, §3.2).
- `scripts/effective-config/rename-map.json` (new, the 3 entries of §2.4, each with its commit and edge record).
- `scripts/effective-config/old/effective_config_dump.rs.in` (new, about 20 lines). This is the §2.2 old lib module. The `.rs.in` suffix keeps it out of the repo-wide `.rs` source ratchet (`common.py:131-139` walks every tracked `.rs`) and out of rustfmt. It is compiled only inside the 6bcaff69 archive, next to `examples/effective_config.rs.in` (about 5 lines) and a one-line `pub mod effective_config_dump;` appended to that archive's `src/lib.rs`.
- `scripts/effective-config/families/*.family` (new): `defaults`, `fleet-server-1`, `fleet-server-n`, `region-server`, `region-server-scale`, `mt-tenants-off`, `mt-tenants-enforce`, `wc-ladder`, `wc-ladder-diet`, `fra-ab-server`, `livefeed-canary`.
  - Line format: `source <script> "<anchor text>"`, `argv …`, `env K=V`, `include <env file>` (read from the tree, never copied), `supervisor K=V`, `platform K=V`, `role <gen|lb|server>`.
  - Evaluation is in file order and last wins, matching the Compute CLI ("Later --env wins", `wc-ladder.sh:62,76`). Other roles' env comes first and the server's own env last (project merge, §1.1(c)).
- `scripts/quality.sh`: +1 line `python3 -m unittest discover -s scripts/effective-config -v` after `:16` (D6). This is not a `scripts/quality/` path, so `verification_plan.py`'s `tooling` stays false.

**C3 — "The real rc.4 and HEAD binaries boot or refuse every family"** (Leg C, D4)
- `effective_config.py`: + `boot` subcommand and K10-K12 (about 150 lines).
- `test_effective_config.py`: + `BootTest`.

**C4 — evidence (after the D-decisions; produced by running the tool, never hand-edited)**
- `docs/reviews/2026-09-hardening/evidence/effective-config/<date>/`:
  - `report.md`
  - `families/*.json`
  - `equivalence/{e1,e2}.txt`, `equivalence/e3-transcription.md`
  - `controls.txt`, `boot.json`, `boot-logs/`
  - `drift.txt`
  - `annotations.template.json`
- The owner's `annotations.json` is committed by the owner, or on their instruction with their decisions, as its own commit.

---

## 5. Mutation analysis

- `scripts/quality/verification_plan.py` on C1-C4:
  - `changed_rust_files = ["examples/effective_config.rs"]`
  - not under `CRITICAL_PREFIXES`, not in `mutation_owners.py`, so `"mutants": false`
  - `properties_fuzz = miri = false`: `codec/quota/buffers` false; `tooling` false because nothing under `scripts/quality/`, `fuzz/` or `tools/quality-invariants/` changes
- No MISSED or TIMEOUT exposure: no production Rust changes.
- For the tool itself, each control is built to kill a specific defect:

| Defect (mutation) | Killed by |
|---|---|
| flattener drops nested or Option leaves | K2 (`shard.open_wait_ms` is nested), K9 (leaf counts) |
| dumper sees the ambient environment (no `env -i`) | K4 |
| old dumper built as a test target | K5 |
| verdict not captured, or refusal text truncated | K6, K10 |
| secret policy bypassed | K7, `FamilyTest.test_secret_requires_placeholder` |
| probe insensitive (only removal, or only perturbation) | K8 (SSE_H1_HEADER_TIMEOUT_MS unset→set is NEW_ONLY only if the old side really ignores it) |
| unmatched paths silently dropped | K9, `RenameTest.test_unaccounted_path_fails` |
| diff always empty | K2, K3 |
| diff never empty (nondeterminism) | K1 |
| boot classifier treats bind-then-die as BOOTED | `BootTest.test_classify`, K11 |
| boot probe never boots anything | K12 |
| transcription anchor drift | E2 (anchors must match both revisions verbatim) |

---

## 6. Ledgers

- `docs/quality/exception-growth.json`: **no rows** (no `#[expect]`/`#[allow]` added, none edited).
- `docs/quality/source-allowances.json`, `owners.json`, `legacy-source.json`, `syntax-fragments.json`: unchanged. The example has no effect, global or macro-dsl facts, and the `.rs.in` files are not Rust sources to the ratchet.
- `scripts/quality/mutation_owners.py`: unchanged.
- `docs/refactor/test-inventory.json`: unchanged (it ratchets `src/dst` only, `scripts/test-inventory.py:138`).
- `docs/reviews/2026-09-hardening/edge-changes.md`: no record (no edge change).
- Evidence: C4 as above. Annotations are the owner's (D7).

---

## 7. Controls (exact commands, expected outputs)

```sh
W=$TMPDIR/effective-config-work   # scratch, outside the repo
# C1
cargo build --locked -p streams-slate --example effective_config          # compiles
env -i SLATE_S3_ENDPOINT=http://127.0.0.1:1 target/debug/examples/effective_config --listen 0.0.0.0:8080 | tail -2
#   @@ verdict
#   accepted
env -i SLATE_S3_ENDPOINT=http://127.0.0.1:1 SSE_H1_MAX_BUF=4096 target/debug/examples/effective_config | tail -3
#   refused
#   configuration invalid (1 problem(s)):
#     - SSE_H1_MAX_BUF=4096 is below hyper's 8192-byte h1 buffer floor
# C2
python3 -m unittest discover -s scripts/effective-config -v                # Ran <N> tests … OK
python3 scripts/effective-config/effective_config.py check-families        # check-families OK: 11 families
python3 scripts/effective-config/effective_config.py build --work "$W"
#   old-model 6bcaff69 examples/effective_config sha256=…
#   new 46f668b3+ examples/effective_config sha256=…
#   free before 31 GiB (floor 12 GiB); work dir <x> GiB
python3 scripts/effective-config/effective_config.py equivalence --work "$W"   # E1 PASS / E2 PASS / E3 wrote … (§3.3)
python3 scripts/effective-config/effective_config.py controls --work "$W"      # K1…K9 PASS (§3.3)
python3 scripts/effective-config/effective_config.py compare --work "$W" --out "$OUT"
#   one line per family: <family>: new=accepted value-rows=<k> OLD_ONLY=0 NEW_ONLY=0 NEITHER=<n>
python3 scripts/effective-config/effective_config.py drift                 # ABSORB_PASS_BYTES removed: 13 scripts (5 Compute)
# C3
python3 scripts/effective-config/effective_config.py build --work "$W" --binaries   # + rc.4 streams-slate, head streams-slate, head s3lite
python3 scripts/effective-config/effective_config.py boot --work "$W" --out "$OUT"  # <family>: rc.4=BOOTED head=BOOTED; K10-K12 PASS
# Gates (CI plan, then push; never claim green without gh run view)
scripts/quality.sh                                                           # fmt, clippy -D warnings (all targets incl. examples), source gate: OK
python3 scripts/quality/verification_plan.py --out target/quality-plan-ecfg  # "mutants": false, changed_rust_files ["examples/effective_config.rs"]
gh run view <id>                                                             # green
```

---

## 8. Out of scope

- **Consumer semantics.** A value used differently at the same path (for example 07db91a7 cooldown saturation, a342ab6f's floor) is not a configuration difference. Those are the 52 edge-change records.
- **Compute platform validation** (startup, readiness, restart, memory pressure, rollback). This is a separate owner prerequisite, as are post-I/O boot failures on Tigris and real nofile values.
- **Fixing anything the comparison finds.** Each finding gets its own plan, including the §1.6 `--compactor-max-concurrent` and PATH_PREFIX splits and the fra-ab MANIFEST_POLL_MS.
- **Item-41 typo refusal (C3 there),** still awaiting its own decision.
- **Making `redacted_summary` Debug-derived.**
- **Derived and proven values** (effective initial shards, SSE capacity after the clamp, SlateDB `Settings` per DB family, compactor options). See D5.
- **Local bench rigs,** unless chosen under D3.

---

## 9. Decisions for the owner

- **D1 — old-side model.**
  - Recommended: 6bcaff69 AppConfig/Args, gated by E1 and E2 (both mechanical and verified today) and by your E3 sign-off. rc.4 truth for refusals comes from Leg C.
  - Alternative: an rc.4-exact dumper injected into rc.4's `main()` (non-test). It would call rc.4's callable readers (OnceLock getters, `Limits::from_env`, `scaler3::policy`, `usage::limits`, `resolved_compactor_options`, …; roughly half the knobs) and would need **new** transcriptions for the inline half. That costs more and leaves less assurance than reusing PR 3's reviewed transcription.
- **D2 — "real env" source.** Given `RUNBOOK.md:428-429` and `wc-ladder.sh:7-8`, the script-derived families are necessary but not sufficient. Someone with platform access should export each target Compute project's env (names and non-secret values; secrets replaced by placeholders) as an extra `.family`. That covers leftover names such as ABSORB_PASS_BYTES and whatever the platform injects (PORT, …). Who does this, and for which projects?
- **D3 — family scope.**
  - Recommended: the 11 family files in §4 C2 (9 Compute variants, the canary, and `defaults`).
  - Add the local certification rigs (`platform-e2e.mjs`, `livefeed-cert.mjs`, `mt-noisy-campaign.mjs`) or the local bench rigs?
  - `cluster-deploy.sh` stays excluded (it refuses without an opt-out).
- **D4 — Leg C** (real boot of rc.4 and HEAD against s3lite; C3).
  - Recommended: yes. It is the only faithful rc.4 refusal oracle (§1.4), and it cross-checks HEAD `validate()` (K11).
  - Without it, rc.4 refusals rest on field evidence alone.
- **D5 — derived and proven values.**
  - Not included, because the public facade cannot reach `into_bootstrap_parts`, `shard_settings` or `production_settings_families`.
  - Including them needs either a test-build HEAD dumper (acceptable at HEAD, where the config path has no `cfg(test)` fork, but asymmetric with the old side) or new public API.
  - Recommended: no. Leg C boot logs carry the HEAD values for R7.
- **D6 — CI hook.** Add the unittest line to `scripts/quality.sh`, so the tool's pure tests run on every push? Recommended: yes.
- **D7 — pass rule.**
  - Every value, name and verdict row is decided by you.
  - Rows identical across families may be decided once, for their family list.
  - Agents may pre-fill `proposed` only.
  - Accept this rule, including the `defaults` family rows?
- **D8 — E3 reviewer.** You alone, or an independent reviewer agent's pass that you countersign?
- **D9 — evidence location.** `docs/reviews/2026-09-hardening/evidence/effective-config/<date>/`, committed (placeholders only)?
- **D10 — script drift.**
  - Primary comparison: HEAD-era family inputs fed to both binaries, which isolates binary semantics.
  - rc.4-era inputs appear only as the `drift` name report.
  - Accept, or also run the rc.4-era script inputs against rc.4?
- **D11 — found in passing (§1.6).**
  - Open a separate item for `--compactor-max-concurrent` on argv never reaching the compactor (same class as item 32) and for the PATH_PREFIX argv/env split?
  - Is `bench-fra-ab.sh` (MANIFEST_POLL_MS=1000, against RUNBOOK:103) a deployed family?
- **D12 — toolchain for old trees.** Build rc.4 and 6bcaff69 with the pinned 1.98.1 (neither pins one). If either fails to compile, fall back to the toolchain recorded in rc.4's release provenance (`--old-toolchain`), and record it in the evidence.

---

## Skeptic corrections (C1..C12)

Skeptic pass on 2026-09-24 against `slate` @ 46f668b3. Read-only: `git show`, `grep`, `sed`, `wc` and small Python scans. Nothing was built or run.

**Verified as stated** (no change needed):
- **E1.** `git show v0.2.0-rc.4:src/main.rs | sed -n 72,593p` and 6bcaff69 `src/bootstrap.rs:23-544` with `pub(crate) ` stripped are byte-identical: 522 lines, `diff` is empty.
- **E2.** The rc.4 non-bin, non-DST sources have 65 `env::var` sites. Literal names plus the 6 helpers resolve to 72 names, which equal 6bcaff69 `ENV_KNOBS` (70) + {DST_DRAIN_TRACE, TOKIO_WORKERS}. Nothing is in ENV_KNOBS that rc.4 does not read.
- **Field sets.**
  - Old `Args` and HEAD `CliArgs` both have 84 fields with the same names. The only type changes are `absorb_{pass_bytes,concurrency,small_bytes}` → `Option<_>`. Their clap attributes differ only in the three dropped `default_value_t`.
  - 6bcaff69 `AppConfig` and the HEAD `ServerConfig` sub-configs both have 71 knob fields.
  - Old-only fields are exactly `billing.{mode_env,rollup_env}`. New-only fields are exactly `http.h1_header_timeout` (default 120 s, `model.rs:440`) and `runtime.tokio_workers`, which is paired with the injected leaf. So K9's 156/155 and the rename map are correct.
- **Other spot checks, all hold:**
  - The facade at `src/lib.rs:76-77`.
  - `validate(self)` at `validation.rs:652`.
  - `ConfigError` Display with `writeln!` per line (`validation.rs:558-570`).
  - The K6 text (`validation.rs:788-793`, `MIN_H1_MAX_BUF = 8*1024`, `model.rs:473`).
  - The K10 texts: rc.4 `main.rs:1683-1693`; HEAD `validation.rs:721-726` via `main.rs:31` `eprintln!("Error: {e}")`.
  - `index.ts:94` argv and `:86` APP_BINARY_SHA256.
  - `RUNBOOK.md:428-429`, `wc-ladder.sh:7-8`.
  - The 24 profile knob lines.
  - §1.6 COMPACTOR_MAX_CONCURRENT: `cli.compactor_max_concurrent` has no consumer, and only `engine.*` reaches `model.rs:100,109`.
  - The `debug_assertions` and `cfg!(test)` forks: none remain on the config path at HEAD; only 6bcaff69 `config/mod.rs:650-655` has one.
  - The lockfiles pin the same slatedb rev `0717cc1e`. rc.4 `build.rs` honours `STREAMS_GIT_COMMIT` and `SOURCE_DATE_EPOCH`.
  - `verification_plan.py:77` `tooling` is false for `scripts/effective-config/**` and `scripts/quality.sh`.

**C1 (blocking, C1 commit): `print!` fails the source gate. The plan's gate claim is false.**
- `scripts/quality/source_rules.py:16-20` `EXPRESSION_MACROS` contains `println` and `eprintln` but **not `print`**.
- `classify()` (`:55-56`) therefore turns each `print!` in the example into a `macro-dsl` fact.
- `examples/` is in the ratcheted source set: `common.py:131-139` walks every `.rs`, and `examples/readertest.rs:6-9` already carries a reasoned `#[expect]`.
- `source_rules.py:486-494` then fails with `unregistered source occurrence (1): ('macro-dsl', 'examples/effective_config.rs', …, 'print')`, twice. The tree has no `print!` today, so there is no allowance row, and agents may not add one.
- Fix: use only `println!`. For example `println!("@@ verdict\nargv-refused\n{refusal}")` and `println!("@@ verdict\nrefused\n{refusal}")`, with the flattener stripping trailing blank lines.
- With `println!`, K6's and §7's exact verdict blocks gain one trailing empty line: `ConfigError` Display already ends in `\n` (`validation.rs:566`). Pin the expected text after `rstrip()`.
- Delete the "both in `EXPRESSION_MACROS`" and `print_with_newline` bullets in §4 C1.

**C2 (blocking, tool): the shared `CARGO_TARGET_DIR` overwrites artifacts between revisions.**
- The injected old example and the new example are both named `effective_config`, so both land at `<target>/debug/examples/effective_config`. rc.4 and HEAD both land at `<target>/debug/streams-slate`. Whatever was built last wins.
- A collision would make "old" silently equal "new".
- K5 cannot catch this, because bc43a2bb made HEAD's non-test defaults the same 64 MiB / 2 gathers. Only K9 catches it, and only by accident, through the leaf count.
- Fix:
  - After each `cargo build`, `build` must copy each artifact to `$W/bin/<rev>/<name>`, then hash and record the copy. Every later step runs only the copies.
  - Add a control line: `K13 PASS artifact identity: old-model dumper sha != new dumper sha; rc.4 streams-slate sha != head streams-slate sha; old output contains billing.mode_env`.
  - Alternative: one `CARGO_TARGET_DIR` per tree. That costs about 2× disk and loses the dependency sharing.

**C3 (design gap): "accepted" hides validation notices, and the example cannot reach them.**
- `validate()` returns typed advisories in `ValidatedServerConfig.notices` (`validation.rs:583-593`). `ValidatedServerConfig` has no public accessor for them; `into_bootstrap_parts` is `pub(crate)` (`:615`).
- The variants (`notice.rs:15-53`) include `IgnoredAbsorberOptions`, the one signal for the leftover ABSORB_PASS_BYTES that §1.5 predicts. They also include `FleetAuthStaticBridge`, `CoarseInitialShards`, `FeedBudgetAboveReleaseMax`, `MemoryProfileCertified` and the descriptor notices.
- HEAD logs them only at boot (`bootstrap.rs:191-197`).
- Fix:
  - Make Leg C mandatory whenever D4 is accepted.
  - Have `boot` extract every HEAD `WARN`/`INFO` notice line from `bootstrap.rs:191-197`, and rc.4's `memory profile certified` line (rc.4 `main.rs:803`), into per-family reviewed rows (R4). They are no longer "eyeball only" (R7).
  - State in §2.4 that the example's verdict is `accepted` or `refused` without notices.

**C4 (scope claim wrong): the canary is not the only release-posture family.**
- `scripts/platform-e2e.mjs:90-110` also sets `STREAMS_RELEASE_POSTURE: "1"`. It adds enforce auth, workload fleet auth, `CELL_ID`, `USAGE_STREAM_KEY`, `ROLLUP=1` and `MAX_RECORD_PAYLOAD_BYTES`. That is closer to the production shape named in the `promote-rc.sh:188-194` tag text than the canary is.
- No Compute family sets STREAMS_RELEASE_POSTURE (`git grep` finds only the canary, platform-e2e and a profile comment). So the release-posture validation path is exercised only by local families.
- Fix:
  - Correct §1.5 ("the only STREAMS_RELEASE_POSTURE=1 family").
  - Add `platform-e2e` to the recommended D3 set as family 12.
  - Tell the owner in D3 that none of the Compute families is release-shaped.

**C5 (classifier gap): PROCESS-class inputs are incomplete.**
- `src/peer.rs:171-180` builds a `reqwest::Client` (reqwest 0.12, `Cargo.toml:23`) without `.no_proxy()`, and no `no_proxy` exists anywhere in `src/`. reqwest's default honours `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY` (and their lowercase forms) from the process env.
- A platform-exported proxy variable (D2) would therefore be classified NEITHER(platform), and R3 would ask whether it is a misspelled knob. In fact the binary honours it on both sides.
- Fix: add these names to the declared PROCESS list next to RUST_LOG and `MIMALLOC_*`. Also add `SSL_CERT_FILE` and `SSL_CERT_DIR` if `cargo tree -p streams-slate -e normal -i rustls-native-certs` shows it linked into the server. Say in §1.2 that dependency-read env exists outside the graph.

**C6 (classifier and dumper parity): the probe and the old dumper are under-specified.**
- **(a) Old dumper argv handling.**
  - The injected old dumper must use `Args::try_parse_from` and print `@@ verdict\nargv-refused\n…`, like the new one.
  - Otherwise a perturbed boolean or number in an env-bound clap field makes clap `exit(2)` with usage text and no sections.
  - The injected `.rs.in` body is not shown. Show it (about 20 lines) so E-review covers it.
- **(b) "No change" does not mean "not read".**
  - Appending `x` to an inert value can leave the output unchanged. Example: `BILLING_METER=on`, where only `off` matters (6bcaff69 `BillingConfig` doc).
  - Report such names as `NO_EFFECT_AT_VALUE`, not NEITHER.
  - Restrict R3's NEITHER list to names absent from **both** static name sets. §1.2 computes those sets; the tool should emit them as a cross-check, not as the classifier.
- **(c) Map `None` to the clap default in the pair.** The declared pair `billing.mode_env: Option<String>` ↔ `cli.billing_mode: String` (default `"off"`, `cli.rs:411`), and `rollup_env` ↔ `cli.rollup` (default `"0"`, `cli.rs:415`), need an explicit `None ≡ clap default` rule in `rename-map.json`. Otherwise every family that leaves BILLING_MODE unset gives a spurious unequal row.
  - Note that old `cli.billing_mode` also exists (the Args are identical), so it is compared directly. The pair is an extra check of the edge #49 reader split.

**C7 (control overclaim): K5's stated red cannot be reached.**
- `cargo build --example` never compiles the lib with `cfg(test)`. `cfg(test)` applies only to a crate compiled by the `--test` harness.
- So no mutation of this tool that keeps building an example can produce 4 GiB / 64 gathers. The §5 row "old dumper built as a test target → K5" has no reachable mutant.
- Fix:
  - Either demonstrate the red once in the red-first sequence by running the injected module as a lib unit test at 6bcaff69. Expected output: `history.absorb_global_budget_bytes = 4294967296`, `absorb_global_gathers = 64`.
  - Or re-label K5 as a pin of the shipped defaults, not a mutation killer.
- §1.1(b)'s conclusion (use non-test builds) stands.

**C8 (secret policy): D2 exports are not covered.**
- The regex `(SECRET|TOKEN|PASSWORD)` plus the explicit list covers every secret-class name the server reads. The HEAD clap and loader names with key material are exactly SLATE_S3_ACCESS_KEY_ID, SLATE_S3_SECRET_ACCESS_KEY, AUTH_TOKEN, STREAMS_CURSOR_KEY, FLEET_INTERNAL_TOKEN and USAGE_STREAM_KEY.
- It does not cover what a D2 platform export can contain: `*_KEY` / `*_API_KEY` names outside the list, and URL userinfo (`scheme://user:pass@host`).
- The plan also leaves the export step itself undesigned. A raw `env list` written to disk, and redacted afterwards, would write the secret.
- Fix:
  - Add a value rule to K7: refuse any value containing `://[^/@]*:[^/@]*@`.
  - Add a name rule: refuse any `KEY|CREDENTIAL|AUTH` name that is not on a declared non-secret allowlist (for example L0_MAX_SSTS_PER_KEY and `*_S3_KEY` object keys).
  - Specify that the D2 export is piped through the family redactor in memory, and that only the redacted `.family` file is ever written.
- Separately, note that family values from `${X:-default}` expansions (`deploy-region.sh`: `WAL_POST_ACK_GATHER_MS`, `TAIL_RING_BYTES`, `SOAK_LIMIT_RECS_PER_SEC`, `STREAMS_DEBUG_TIMING`; `deploy-fleet.sh`: `FLEET_MIN`, `REBALANCE_*`, `MAX_ABSORB_LAG_SECS`) are script defaults. Only D2 shows what a project actually holds, which is another reason P6 is not optional.

**C9 (cost): time is never estimated, and the disk figure is stale.**
- Disk: `df -g /System/Volumes/Data` shows 68 GiB free now, not 31. The 12 GiB floor is fine.
- Add a time budget:
  - Dependencies build once (rc.4 and HEAD lockfiles differ by 17 packages, 399 vs 416).
  - The crate is compiled three times as dev, debug=0: about 101k lines of `src/` Rust at rc.4 and about 155k at HEAD.
  - Probing: 12 families × up to 88 names × 2 sides × 2 runs is roughly 4k dumper process runs.
  - Leg C: 12 × 2 boots, about 5-10 s each; worst case 12 × 2 × 120 s = 48 min if everything times out.
- The tool should print the wall time per phase, and the plan should state an expected total (order of 30-45 min on this machine) and an abort ceiling.

**C10 (Leg C scope): name what a fresh s3lite cannot show.**
- Persisted-state checks never fire on a fresh in-memory store. These are the stored `MAX_REQUEST_BODY_BYTES` refusal (`bootstrap.rs:346-356`, the same at rc.4 `main.rs:1944-1946`) and the stored topology versus `INITIAL_SHARDS`.
- An in-place upgrade of an rc.4-era namespace is therefore outside Leg C. Add it to §8 explicitly, under the Compute platform-validation prerequisite, so that "HEAD BOOTED" is not read as "boots on the existing projects".
- Also note the platform difference: Leg C runs on darwin, while Compute is x86_64 linux-musl. Descriptor notices and the SSE clamp (`validation.rs:404-458`) will differ.

**C11 (D5, optional improvement): derived values without new API.**
- Both real binaries serve `/v1/debug/load` (rc.4 `http.rs:1605`; HEAD `config/profile.rs:31`, "debug/load + startup log").
- While Leg C holds each family BOOTED, `boot` can GET it, and the absorb debug endpoint that `oom-acceptance.sh verify` already reads, with the placeholder AUTH_TOKEN. The resolved compactor profile and memory budgets can then be diffed symmetrically.
- This closes most of the "full effective config" gap without a test-build dumper or new public API. Recommend it under D5 instead of "no".

**C12 (minor text fixes)**
- **C1 red.** Newer cargo continues the first line with ``in `streams-slate` package`` and a `help: available example targets:` block. Pin the prefix only.
- **CI Python.** CI's Python is ubuntu-latest's `python3` (the workflow pins no version; `.github/workflows/rust-quality.yml:22-44`), not necessarily 3.11. The unittest red is the same on any version ≥ 3.9.
- **Anchors.** 6bcaff69 `Args` is at `bootstrap.rs:25` (`:23` is the derive line). Keep `:23` for E1, since the block starts there, but say so.
- **Defaults family.** `SLATE_S3_ENDPOINT` is a required clap field with no default (`cli.rs:17-18`), so `defaults` means `{SLATE_S3_ENDPOINT=<placeholder>}` plus the argv, not an empty env. State this in the family file, because K2, K4, K6 and K10-K12 all derive from it.

**Verdict: ready with corrections.**
- The design meets the owner's conditions, provided P6 (the D2 platform export) is enforced as written:
  - Real argv and env, including the supervisor argv and the project-merge trap.
  - Every script-deployed Compute family, with platform-e2e added (C4).
  - The full graph, not the redacted summary.
  - Only placeholder secrets are written, provided C8 is applied.
- C1 and C2 must be fixed before any commit: C1 is a certain CI failure, and C2 is a silent false-equal. C3, C5 and C6 must be folded into the tool before its evidence counts.
- No production Rust changes, no `#[expect]` scopes, no files over 1,000 lines, no mutation-owned paths and no ledger rows are involved. That claim holds once C1 removes `print!`.
