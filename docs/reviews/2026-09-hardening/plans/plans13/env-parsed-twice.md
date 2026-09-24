# Item 32: five environment names parsed twice (clap + the environment overlay)

Tree: `slate` @ 6669d3b5. `origin/slate` = aaf2baa5; HEAD carries the unpushed item 90/93/41/89
commits, plus the uncommitted `scripts/quality/mutation_owners.py` owner rows (offsets, segmap,
telemetry_batch). Of the files this item edits, only these differ between HEAD and origin/slate:
`src/config/cli.rs` (+6), `load.rs` (+1), `model.rs` (+16), `config/tests.rs` (+1),
`docs/quality/owners.json`, `docs/refactor/test-inventory.json` and (uncommitted)
`mutation_owners.py`. None of those changes overlaps a line edited here.
**Sequencing:** C2 edits the `http` row of `mutation_owners.py`. Start only after item 89's
owner-row commit has landed, so the two edits cannot interleave in one working tree.

Source: reviewer item 32 (robustness-maintainability-review.md:992-1002). I re-read every cited
location and its neighbours on the current tree and grepped every use site. All four problems are
real. Three of the reviewer's anchors are stale or slightly off; the corrections are in §1.

| # | Commit | Kind | Can land now? |
|---|---|---|---|
| C1 | `--compactor-poll-ms` / `--compactor-max-concurrent` reach the compactor | bug fix, red-first | yes |
| C2 | BILLING_MODE and ROLLUP have one reader: `/health`, `/operator/billing.json` and the drain read the clap value | bug fix, red-first | yes |
| C3 | The read spool nests under the clap-resolved PATH_PREFIX | bug fix, red-first | yes (confirm D-32b) |
| C4 | An unknown BILLING_MODE / ROLLUP word refuses boot | **new boot refusal** | **no. Wait for D-32a (same policy as item 41 D1)** |

C1 is config-only, has no mutation selection and needs no ledgers, so it goes first. C2 carries
the only critical-file edit (`src/http.rs`). C3 is separate because it moves one durable location
for argv-only deployments (D-32b). C4 is written below but is not part of this landing.

---

## 1. Problem (verified)

### 1a. Which five names, and why the two channels agree only for env-only deployments

The names are exactly the intersection of clap's `env = "..."` attributes (`src/config/cli.rs`)
and the overlay's string literals (`src/config/load.rs`). Computed with grep/comm:
`BILLING_MODE, COMPACTOR_MAX_CONCURRENT, COMPACTOR_POLL_MS, PATH_PREFIX, ROLLUP`.

Composition root, `src/main.rs:26-27`:
```rust
    let cli = streams_slate::CliArgs::parse();
    let parsed = streams_slate::ServerConfig::load(cli, &streams_slate::ProcessEnvironment);
```
- Clap reads a variable only when argv does not already carry the flag:
  `clap_builder-4.6.0/src/parser/parser.rs:1417-1423`, `if matcher.contains(&arg.id) { continue; }`.
- `Arg::env` captures `env::var_os(name)` as it is, so an empty string is a value
  (`builder/arg.rs:2205-2213`).
- `ProcessEnvironment::get` is `std::env::var(key).ok()` (`environment.rs:28-30`).

So when the process is configured **only through the environment**, the clap field and the overlay
copy hold the same value for all five names. They diverge only when **argv** supplies the flag (or
when argv and the environment disagree, in which case clap's argv value wins). That is exactly the
reviewer's failure class, and it is why "env layout unchanged" can be pinned (§3 F).

### 1b. BILLING_MODE: boot reads clap; `/health`, `/operator/billing.json` and the drain read an environment copy

The clap field is at `src/config/cli.rs:410-412`:
```rust
    /// "required" makes readiness fail without the usage ledger key.
    #[arg(long, env = "BILLING_MODE", default_value = "off")]
    pub(crate) billing_mode: String,
```
The environment copy is at `src/config/load.rs:176` (the reviewer said :173-176):
```rust
        self.billing.mode_env = env.get("BILLING_MODE");
```
`src/config/model.rs:199-204` says so openly: "`billing_required()` and the debug endpoint read the
ENVIRONMENT today, NOT the clap field ... Scheduled for unification in WP-13."

`src/billing.rs:705-709` (the reviewer said :700-702):
```rust
/// BILLING_MODE=required: production billing — volatile fallbacks are
/// refused and billing infrastructure failures are fatal at startup.
pub(crate) fn billing_required(cfg: &crate::config::BillingConfig) -> bool {
    cfg.mode_env.as_deref() == Some("required")
}
```

**Every use site:**

| Site | Channel | What it decides |
|---|---|---|
| `config/validation.rs:834` `if self.cli.billing_mode != "required" {` | clap | required-mode prerequisites (usage key, non-placeholder ids) refuse boot |
| `bootstrap.rs:680` `if config.cli.billing_mode == "required" {` | clap | synchronous spool open (and rollup open) before bind |
| `http.rs:1670` `if crate::billing::billing_required(&state.config.billing) {` | **env copy** | `/health` billing gate |
| `http.rs:1745` `let ready = !crate::billing::billing_required(&state.config.billing)` | **env copy** | `/operator/billing.json` `ready` |
| `http.rs:1751` `"mode": state.config.billing.mode_env.clone().unwrap_or_else(\|\| "off".into()),` | **env copy** | `/operator/billing.json` `mode` |
| `billing.rs:805` `} else if billing_required(&state.config.billing) {` | **env copy** | drain refuses the memory-only path |
| `config/summary.rs:61` `"mode_env": &self.billing.mode_env,` | env copy | boot log only |
| `config/tests.rs:149` `assert_eq!(c.billing.mode_env, None);` | env copy | default pin |
| `config/validation_tests.rs:547,554` `c.billing_mode = "required".into()` | clap | validation tests |

The reviewer names the report route `/v1/debug/billing`. It is actually `GET /operator/billing.json`
(`http.rs:1185` → `billing_readiness_axum`, `http.rs:1701`). No `/v1/debug` route is involved.

Traced for argv `--billing-mode required` with no environment variable:
1. `validate()` requires USAGE_STREAM_KEY and real ids, so boot fails closed.
2. Bootstrap opens the spool synchronously.
3. `/operator/billing.json` then reports `"mode":"off","ready":true`.
4. `/health` never applies the billing gate.
5. `drain_once` would take the volatile memory path whenever the spool is closed.

The reverse case: env `BILLING_MODE=required` plus argv `--billing-mode off` with no usage key.
- Boot treats the instance as off.
- `/health` requires a spool that never opens (`spawn_telemetry` returns early without a key), so
  the instance is **503 forever**.

### 1c. ROLLUP: bootstrap reads clap; the readiness checks read an environment copy

- Clap field: `cli.rs:414-416` (`#[arg(long, env = "ROLLUP", default_value = "0")] pub(crate) rollup: String`).
- Environment copy: `load.rs:178` `self.billing.rollup_env = env.get("ROLLUP");`, which sets
  `model.rs:210`.

| Site | Channel | What it decides |
|---|---|---|
| `bootstrap.rs:693` `if config.cli.rollup == "1" {` | clap | required mode: rollup DB must open before serving |
| `bootstrap.rs:889` `if config.cli.rollup == "1" {` | clap | spawns the rollup consumer/closer |
| `http.rs:1672-1673` `let rollup_ok = state.config.billing.rollup_env.as_deref() != Some("1") \|\| state.rollup.installed();` | **env copy** | `/health` in required mode |
| `http.rs:1748-1749` `&& (state.config.billing.rollup_env.as_deref() != Some("1") \|\| state.rollup.get().is_some()));` | **env copy** | `/operator/billing.json` `ready` |
| `config/summary.rs:62`, `config/tests.rs:151` | env copy | boot log / default pin |

- With argv `--rollup 1`, readiness never waits for the rollup DB.
- With env `ROLLUP=1` and argv `--rollup 0`, no rollup is ever spawned, yet a required-mode
  `/health` waits for one: 503 forever.

### 1d. PATH_PREFIX: every store and the rollup read clap; the read spool reads the environment copy

- Clap field: `cli.rs:422-425` (`#[arg(long, env = "PATH_PREFIX")] pub(crate) path_prefix: Option<String>`).
- Environment copy: `load.rs:179` `self.billing.path_prefix_env = env.get("PATH_PREFIX");`, which
  sets `model.rs:214`.

`src/billing.rs:1134-1139`, inside `open_read_spool`:
```rust
    let prefix = state
        .config
        .billing
        .path_prefix_env
        .clone()
        .unwrap_or_default();
```

| Site | Channel | Use |
|---|---|---|
| `bootstrap.rs:64` `Ok(match &self.cli.path_prefix { Some(p) => Arc::new(PrefixStore::new(s3, p.as_str())), ...` | clap | wraps ops/shard/**data** stores (`bootstrap.rs:216-218`) |
| `bootstrap.rs:233` canary prefix | clap | startup canary |
| `bootstrap.rs:696`, `:892` `&config.cli.path_prefix.clone().unwrap_or_default()` → `open_rollup` | clap | rollup DB path `format!("{prefix}/{ROLLUP_PATH}")` (`rollup.rs:498-502`) inside the already-prefixed data store |
| `billing.rs:1134-1139` (open_read_spool; callers `bootstrap.rs:688` and `billing/telemetry_loop.rs:35`) | **env copy** | spool path `format!("{prefix}/telemetry/read-spool/{inst}")` (`billing/read_spool.rs:79-83`, the reviewer's anchor), also inside the prefixed data store |
| `config/summary.rs:72`, `config/tests.rs:152`, `model.rs:8` (doc) | env copy | log / pin / doc |

Physical layout today, with `data_store = PrefixStore(p)` whenever clap has a prefix:

| Channel | Spool | Rollup |
|---|---|---|
| env `PATH_PREFIX=p` | `p/p/telemetry/read-spool/<inst>` | `p/p/telemetry/usage-rollup/v2/p0` |
| argv `--path-prefix p` only | `p/telemetry/read-spool/<inst>` | `p/p/telemetry/usage-rollup/v2/p0` |

So the spool's location depends on the channel, as the reviewer says. Every in-repo deployment
that sets a usage key sets PATH_PREFIX through the environment:
- `bench/fleet/deploy-fleet.sh:115`: `--env PATH_PREFIX=...`
- `bench/fleet/local-fanout.sh:54`: `PATH_PREFIX=fand`

`docs/GUIDE-COMPOSER.md:222` passes `--path-prefix composer` on argv but sets no USAGE_STREAM_KEY.
Without that key `spawn_telemetry` returns early, and the spool opens only through that task or
through required mode.

### 1e. COMPACTOR_POLL_MS and COMPACTOR_MAX_CONCURRENT: the clap flags are accepted and ignored

Clap fields, `cli.rs:159-165`:
```rust
    #[arg(long, env = "COMPACTOR_POLL_MS", default_value_t = crate::DEFAULT_COMPACTOR_POLL_MS)]
    pub(crate) compactor_poll_ms: u64,
    ...
    #[arg(long, env = "COMPACTOR_MAX_CONCURRENT", default_value_t = 4)]
    pub(crate) compactor_max_concurrent: usize,
```
Only the hermetic fixture `cli.rs:575-576` ever touches them; nothing reads them. The value that
counts is the overlay, `load.rs:62-67`:
```rust
        if let Some(v) = env_parse(env, "COMPACTOR_POLL_MS") {
            self.engine.compactor_poll_ms = v;
        }
        if let Some(v) = env_parse(env, "COMPACTOR_MAX_CONCURRENT") {
            self.engine.compactor_max_concurrent = v;
        }
```
That sets `EngineConfig` (`model.rs:72-75`), and `EngineConfig::compactor_options()` (`model.rs:94-111`)
hands it to every DB family:
- `bootstrap.rs:444`
- `config/profile.rs:28,37,63,68,73,90` (cert + debug profile)
- `rollup.rs:504`
- `billing/read_spool.rs:84`
- `config/validation.rs:737`
- (`shard.rs:1089` uses `EngineConfig::default()`)

Also `summary.rs:23-24`, `config/tests.rs:116-117` and `certification_tests.rs:34` (which feeds
COMPACTOR_MAX_CONCURRENT through `MapEnvironment`).

Traced for argv `--compactor-poll-ms 500` with no environment variable: `engine.compactor_poll_ms`
stays 2500, and every compactor polls at 2.5 s. `cli.rs:167-172` shows R29 already deleted four
such dead mirrors (the COMPACT_MAX_* knobs). These two survived because their env names are also
read by the overlay.

`config/profile.rs:23-24` still claims the two "mirror the same env vars for --help
discoverability". That is exactly the silent-ignore state R29 called a bug.

---

## 2. Contract decision

**One owner per knob: the clap field.** Clap resolves argv over the variable, and nothing
re-reads the environment:

- **BILLING_MODE:** `CliArgs::billing_required(&self) -> bool { self.billing_mode == "required" }`.
  - It is the only mode predicate for http.rs (×2), billing.rs `drain_once` and validation.rs.
  - `crate::billing::billing_required(&BillingConfig)` and `BillingConfig::mode_env` are deleted.
  - `/operator/billing.json` `mode` echoes `&state.config.cli.billing_mode` (clap default `"off"`).
- **ROLLUP:** `CliArgs::runs_rollup(&self) -> bool { self.rollup == "1" }`. It replaces both
  `rollup_env != Some("1")` tests; `BillingConfig::rollup_env` is deleted.
- **PATH_PREFIX:** `open_read_spool` reads `state.config.cli.path_prefix`, the value `open_rollup`
  and every PrefixStore already get. `BillingConfig::path_prefix_env` is deleted.
- **COMPACTOR_POLL_MS / COMPACTOR_MAX_CONCURRENT:**
  - `ServerConfig::with_knob_defaults(cli)` sets `EngineConfig::{compactor_poll_ms,
    compactor_max_concurrent}` from `cli`.
  - `load.rs:62-67` is deleted.
  - `EngineConfig::compactor_options()` and all of its callers are unchanged.

**Semantics kept exactly.**
- Only the word `required` is required mode, and only `1` owns the rollup. `"Required"`,
  `"true"` and `""` still mean off / not owner, as they do today.
- `bootstrap::run` keeps its three `== "required"` / `== "1"` comparisons (see §4/§8). They read the
  same clap field, so they cannot disagree with the predicates.

**Why not the reviewer's enum yet.**
- Without rejecting unknown words, `BillingMode::{Off, Required}` would have to map every
  unrecognised word to `Off`. The enum would hide that mapping instead of typing it.
- The enum earns its place only with the C4 refusal (D-32a). There, validated configs carry only
  known words.

**Why EngineConfig keeps the two fields instead of reading `cli`.**
- `compactor_options()` is a method on the narrow sub-config, with 12 callers.
- `shard.rs:1089` calls it on `EngineConfig::default()`.
- A one-time copy at construction keeps one parse (clap) and one consumer API.

**Wire:**
- **Environment-only deployments (every in-repo deployment):** no change. All five values are
  byte-identical before and after (§1a); pin F proves it through a real clap parse of a real process
  environment.
- **Argv-configured deployments:** `/health`, `/operator/billing.json` `mode`/`ready`, the drain and
  the compactor now follow the configuration boot already enforced (or, for the compactor, the flag
  given). That is the bug fix.
  - No status code, header, body word or JSON key is added or removed.
  - The 503 body `billing not ready (spool=…, rollup=…)` is unchanged.
  - In production, required mode opens the spool and rollup before `bind` (`bootstrap.rs:680-703`),
    so the new 503 cases are reachable only when those opens are lost later. That is the round-22
    item 10 intent.
- **/metrics and /v1/debug:** nothing changes.
- **Boot log:** the `effective configuration (redacted)` summary loses
  `billing.mode_env/rollup_env/path_prefix_env`. This is a log line: grep finds no consumer in
  docs/, scripts/ or bench/. The summary excludes `cli` wholesale (summary.rs:5-7), so it is not
  replaced.
- **Refusals:** no new refusal rule lands. Two consequences of the flags now taking effect are
  listed as D-32c: `MEMPROFILE_CERT` now judges the argv compactor values.
- **Storage:** argv-only `--path-prefix` with a usage key moves the spool root (D-32b).

---

## 3. Red tests and pins

A red run executes on HEAD **plus only the test-side edits** of the commit:
- the new tests;
- in C2/C3, the fixture option of §4 C2.3, which does not change behaviour: its default is a no-op.

Then the production edits turn it green. Assertion-message formats are Rust 1.8x
`assert_eq!` output.

### C1 red: E, `config::tests::compactor_flags_given_on_argv_reach_the_compactor_options` (src/config/tests.rs)

```rust
/// Item 32: the two compactor flags are clap-owned. A value given only on
/// argv must reach the options every DB family opens with; the overlay
/// used to re-read the environment and drop it.
#[test]
fn compactor_flags_given_on_argv_reach_the_compactor_options() {
    let mut cli = test_cli();
    cli.compactor_poll_ms = 500;
    cli.compactor_max_concurrent = 1;
    let options = ServerConfig::load(cli, &MapEnvironment::empty())
        .engine
        .compactor_options();
    assert_eq!(
        options.poll_interval,
        std::time::Duration::from_millis(500),
        "--compactor-poll-ms on argv must reach the compactor"
    );
    assert_eq!(options.max_concurrent_compactions, 1);
    assert_eq!(options.worker.unwrap_or_default().max_concurrent_compactions, 1);
}
```
The hermetic fixture edit is exactly what clap yields for argv-only flags (field = value, no env).
`cli_surface_is_pinned` pins the flag→field mapping (`("compactor-poll-ms", "COMPACTOR_POLL_MS", "2500")`).

Trace on HEAD:
1. `with_knob_defaults` → `EngineConfig::default()` → 2500.
2. The overlay reads the empty map.
3. `compactor_options().poll_interval` = `Duration::from_millis(2500)`.

Expected red:
```
thread 'config::tests::compactor_flags_given_on_argv_reach_the_compactor_options' panicked at src/config/tests.rs:<L>:5:
assertion `left == right` failed: --compactor-poll-ms on argv must reach the compactor
  left: 2.5s
 right: 500ms
```

### C1 pin (passes before and after): F, `config::tests::clap_owned_names_keep_their_environment_channel` plus its helper

This follows the existing subprocess idiom (`process_environment_smoke_test`, `run_helper_test`
with `env_clear()`).
```rust
/// Subject of `clap_owned_names_keep_their_environment_channel`: inert
/// unless the parent set the marker and the five values under test.
#[test]
fn clap_owned_environment_helper() {
    if ProcessEnvironment.get("STREAMS_CLAP_OWNED_ENV_CHECK").is_none() {
        return;
    }
    let cli =
        CliArgs::try_parse_from(["streams-slate", "--s3-endpoint", "http://127.0.0.1:1"]).unwrap();
    assert_eq!(
        (cli.billing_mode.as_str(), cli.rollup.as_str(), cli.path_prefix.as_deref()),
        ("required", "1", Some("pp"))
    );
    let options = ServerConfig::load(cli, &ProcessEnvironment)
        .engine
        .compactor_options();
    assert_eq!(
        (options.poll_interval, options.max_concurrent_compactions),
        (std::time::Duration::from_millis(700), 2)
    );
}

/// Item 32 pin: an environment-only deployment keeps every value of the
/// five names clap and the overlay both read (BILLING_MODE, ROLLUP,
/// PATH_PREFIX, COMPACTOR_POLL_MS, COMPACTOR_MAX_CONCURRENT): clap reads
/// the process environment whenever argv is silent.
#[test]
fn clap_owned_names_keep_their_environment_channel() {
    let out = run_helper_test(
        "config::tests::clap_owned_environment_helper",
        &[
            ("STREAMS_CLAP_OWNED_ENV_CHECK", "1"),
            ("BILLING_MODE", "required"),
            ("ROLLUP", "1"),
            ("PATH_PREFIX", "pp"),
            ("COMPACTOR_POLL_MS", "700"),
            ("COMPACTOR_MAX_CONCURRENT", "2"),
        ],
    );
    assert!(
        out.status.success(),
        "clap-owned environment parse failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("1 passed"),
        "clap-owned environment helper did not run"
    );
}
```
F asserts only fields and `compactor_options()`, which exist on both trees. So it runs green on
HEAD and after C1-C3. That is the proof of "env layout unchanged":
- On HEAD the engine values come from the overlay.
- After C1 they come from clap, which read the same process environment.
- The three billing fields are what C2/C3 consumers read.

### C1 adaptation (not red): `config::certification_tests::certification_notice_requires_the_complete_profile`

It feeds `("COMPACTOR_MAX_CONCURRENT", "1")` through `MapEnvironment`.
- **Without the adaptation, after C1:** `max_concurrent_compactions=4`, so the test fails at
  `assert!(super::profile::certified_memprofile_errors(&cfg, &mut notices).is_empty())`. That
  failure is itself the proof that the overlay no longer reads the name.
- **Adaptation:** `let mut cli = CliArgs::deterministic(); cli.compactor_max_concurrent = 1;`, then
  drop that pair from the map. The certified profile is unchanged.

### C2 reds: new DST module `src/dst/tests/billing_readiness.rs` (`dst::dst_tests::billing_readiness::`)

Shared helpers (private to the module):
```rust
//! Billing readiness follows the clap-resolved billing selectors (item 32).

use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build, install_rollup};
use super::fixture_requests::hreq;
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;
use serde_json::Value;

/// `/health`'s status and body text.
async fn health(addr: std::net::SocketAddr) -> (u16, String) {
    let (status, _, body) = hreq(addr, "GET", "/health", &[], b"").await;
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// `/operator/billing.json`'s `mode` and `ready`.
async fn billing_report(addr: std::net::SocketAddr) -> (Value, Value) {
    let (status, _, body) = hreq(addr, "GET", "/operator/billing.json", &[], b"").await;
    assert_eq!(status, 200);
    let report: Value = serde_json::from_slice(&body).unwrap();
    (report["mode"].clone(), report["ready"].clone())
}
```
- No `serde_json::json!`, `tokio::select!` or `join!` is used, so there is no macro-dsl row.
- The rig has no auth token, so `authorized` holds: `DeploymentBearer::authorizes` with `None` →
  `mode == Off`.
- Fresh rigs report no shard unreadiness (`sharddir/health.rs:58`, `failed.len() < 3`).
- The rig always has a usage key (`fixture_http.rs:449-450`, `Some(PRISMA_KEY)`).

**A, `argv_billing_mode_required_holds_readiness_until_the_spool_opens`:**
```rust
/// RED (item 32): `--billing-mode required` on argv alone. Boot already
/// enforced it; /health, the readiness report and the drain read a copy
/// of the environment and so reported `off`. With one reader, all three
/// stay closed until the read spool opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn argv_billing_mode_required_holds_readiness_until_the_spool_opens() {
    let options = HttpRigOptions {
        cli: |cli| cli.billing_mode = "required".into(),
        ..Default::default()
    };
    let (state, addr) = http_rig_build(mem(), RigRuntime::first(), options).await.parts();
    assert_eq!(
        health(addr).await,
        (503, "billing not ready (spool=false, rollup=true)".to_string()),
        "argv --billing-mode required must gate /health until the spool opens"
    );
    assert_eq!(billing_report(addr).await, (Value::from("required"), Value::from(false)));
    assert_eq!(
        crate::billing::drain_once(&state).await,
        Err("read spool not open (BILLING_MODE=required refuses the memory-only path)".to_string())
    );
    crate::billing::open_read_spool(&state).await.unwrap();
    assert_eq!(health(addr).await, (200, "ok".to_string()));
    assert_eq!(billing_report(addr).await, (Value::from("required"), Value::from(true)));
    engine_shutdown(&state).await;
}
```
Trace on HEAD (+ fixture option):
1. `fixture_config` applies the edit, so `cli.billing_mode = "required"` and the map is empty.
2. `billing.mode_env = None`, so `billing_required(&billing)` is false.
3. `health_axum` skips the billing block and returns the identity headers with `"ok"`.

Expected red:
```
assertion `left == right` failed: argv --billing-mode required must gate /health until the spool opens
  left: (200, "ok")
 right: (503, "billing not ready (spool=false, rollup=true)")
```
If only the `/health` edit were applied, the red would move to the later asserts:
- the report assert: `left: (String("off"), Bool(true))`;
- the drain assert: `left: Ok(0)` (empty accumulator, no dirty rows, so `envelopes.is_empty()` →
  `Ok(0)`, `billing.rs:1092`).

This locates each consumer's edit.

**B, `argv_rollup_owner_is_unready_until_its_rollup_installs`:**
```rust
/// RED (item 32): `--rollup 1` on argv alone. A required-mode rollup
/// owner is not ready until its rollup database is installed; the
/// environment copy saw no ROLLUP and waived the check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn argv_rollup_owner_is_unready_until_its_rollup_installs() {
    let options = HttpRigOptions {
        cli: |cli| {
            cli.billing_mode = "required".into();
            cli.rollup = "1".into();
        },
        ..Default::default()
    };
    let (state, addr) = http_rig_build(mem(), RigRuntime::first(), options).await.parts();
    crate::billing::open_read_spool(&state).await.unwrap();
    assert_eq!(
        health(addr).await,
        (503, "billing not ready (spool=true, rollup=false)".to_string()),
        "argv --rollup 1 must gate /health until the rollup installs"
    );
    assert_eq!(billing_report(addr).await, (Value::from("required"), Value::from(false)));
    let rollup = crate::rollup::UsageRollup::open(state.data_store.clone(), "", &state.config)
        .await
        .unwrap();
    install_rollup(&state, rollup);
    assert_eq!(health(addr).await, (200, "ok".to_string()));
    assert_eq!(billing_report(addr).await, (Value::from("required"), Value::from(true)));
    engine_shutdown(&state).await;
}
```
Expected red on HEAD (+ fixture option):
```
assertion `left == right` failed: argv --rollup 1 must gate /health until the rollup installs
  left: (200, "ok")
 right: (503, "billing not ready (spool=true, rollup=false)")
```
- On HEAD both channels are copies, so `billing_required` is already false.
- With only the BILLING_MODE edits applied, B still reds with the same `left`:
  `rollup_env = None != Some("1")` gives `rollup_ok = true`.
- So B is what proves the ROLLUP edit is load-bearing.

**C (pin, green before and after), `default_billing_mode_reports_off_and_ready_without_a_spool`:**
```rust
/// Pin: with no --billing-mode anywhere the report shows clap's `off`
/// default and the instance is ready without a spool, as it always was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_billing_mode_reports_off_and_ready_without_a_spool() {
    let (state, addr) = http_rig(mem()).await;
    assert_eq!(health(addr).await, (200, "ok".to_string()));
    assert_eq!(billing_report(addr).await, (Value::from("off"), Value::from(true)));
    engine_shutdown(&state).await;
}
```
C pins the `mode` wire value across its move from `mode_env.unwrap_or("off")` to the clap default.

**C2 pin: G, `config::tests::billing_selectors_keep_their_exact_words`** (new API, so it cannot run on HEAD):
```rust
/// Item 32: the one reading of each selector keeps today's exact words;
/// C4 (D-32a) would refuse the others instead of reading them as off.
#[test]
fn billing_selectors_keep_their_exact_words() {
    let mut cli = test_cli();
    assert!(!cli.billing_required() && !cli.runs_rollup());
    for (mode, rollup) in [("Required", "true"), ("required ", "yes"), ("", "")] {
        cli.billing_mode = mode.into();
        cli.rollup = rollup.into();
        assert!(!cli.billing_required() && !cli.runs_rollup(), "{mode:?} / {rollup:?}");
    }
    cli.billing_mode = "required".into();
    cli.rollup = "1".into();
    assert!(cli.billing_required() && cli.runs_rollup());
}
```

**C2 compile-level proofs.**
- Deleting `BillingConfig::{mode_env, rollup_env}` and `crate::billing::billing_required` makes any
  leftover env-copy reader a compile error.
- `rg -n 'mode_env|rollup_env' src` must print nothing after C2.
- Existing pins for the unchanged clap consumers are
  `validation_rejects_missing_required_billing_identity` (validation.rs:834 moves onto the
  accessor) and `deterministic_default_configuration_is_valid`.

### C3 red: D, `read_spool_nests_under_the_clap_path_prefix` (same module)

```rust
/// RED (item 32): the read spool nests under the clap-resolved
/// PATH_PREFIX inside the data store, as the rollup database does. An
/// environment-configured deployment already had exactly this layout
/// (its copy and clap agree); an argv-only prefix opened it one level up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_spool_nests_under_the_clap_path_prefix() {
    let store = mem();
    let options = HttpRigOptions {
        cli: |cli| cli.path_prefix = Some("pp".into()),
        ..Default::default()
    };
    let (state, _) = http_rig_build(store.clone(), RigRuntime::first(), options).await.parts();
    crate::billing::open_read_spool(&state).await.unwrap();
    assert_eq!(
        spool_roots(&store).await,
        vec!["pp/telemetry/read-spool/solo".to_string()],
        "the read spool must open under the clap PATH_PREFIX"
    );
    engine_shutdown(&state).await;
}

/// Every read-spool database root in the store.
async fn spool_roots(store: &std::sync::Arc<dyn object_store::ObjectStore>) -> Vec<String> {
    use futures_util::StreamExt;
    let mut roots = std::collections::BTreeSet::new();
    let mut listing = store.list(None);
    while let Some(meta) = listing.next().await {
        let location = meta.unwrap().location.to_string();
        if let Some((head, tail)) = location.split_once("read-spool/") {
            let instance = tail.split('/').next().unwrap_or_default();
            roots.insert(format!("{head}read-spool/{instance}"));
        }
    }
    roots.into_iter().collect()
}
```
The rig's `data_store` is the raw `store` (`fixture_http.rs:514`). An unnamed rig's ownership
instance is `""` → `"solo"` (`read_spool.rs:74-78`). A SlateDB `build()` writes its first manifest,
so the root is listed.

Trace on HEAD (+ fixture option): `path_prefix_env = None`, so `prefix = ""` and the path is
`telemetry/read-spool/solo`. Expected red:
```
assertion `left == right` failed: the read spool must open under the clap PATH_PREFIX
  left: ["telemetry/read-spool/solo"]
 right: ["pp/telemetry/read-spool/solo"]
```

### C4 (held) red: `config::validation::validation_tests::validate_boundary_tests::validation_rejects_an_unknown_billing_word`

```rust
#[test]
fn validation_rejects_an_unknown_billing_word() {
    rejects(|c| c.billing_mode = "requierd".into(), &[], "BILLING_MODE");
    rejects(|c| c.rollup = "true".into(), &[], "ROLLUP");
}
```
Expected red on the C3 tree: `validate()` returns Ok, giving
`panicked at ...: validate() must reject (marker "BILLING_MODE")`.

Loom and held-commit tests are not needed: no synchronization, ordering or cancellation behaviour
changes.

---

## 4. Edits, file by file, in commit order

### Budgets (merge base origin/slate; `wc -l` at HEAD = at base unless noted)

| File | HEAD | Budget | After C1 | After C2 | After C3 |
|---|---|---|---|---|---|
| `src/http.rs` | 3,155 | ≤ 3,155 | 3,155 | **3,153** | 3,153 |
| `src/billing.rs` | 2,157 | ≤ 2,157 | 2,157 | **2,151** | **2,148** |
| `src/product.rs` 4,205, `src/shard.rs` 3,186, `src/history.rs` 1,713, `src/auth.rs` 1,676, `src/registry.rs` 1,492, `src/sse/feed.rs` 1,165, `src/fleet.rs` 1,142 | — | not touched | — | — | — |
| `src/bootstrap.rs` | 923 | not touched (see "ratcheted scopes") | — | — | — |
| `src/dst/tests/fixture_http.rs` (DST, 1,000 cap) | 791 | ≤ 1,000 | 791 | ~804 | ~804 |
| `src/dst/tests/billing_readiness.rs` (new, 1,000 cap) | — | ≤ 1,000 | — | ~95 | ~125 |
| `src/dst/dst_tests.rs` | 267 | ≤ 1,000 | 267 | 270 | 270 |
| `src/config/tests.rs` | 624 | ≤ 1,000 | ~680 | ~690 | ~689 |
| `src/config/validation.rs` | 973 | ≤ 1,000 | 973 | 973 | 973 |
| `src/config/cli.rs` / `model.rs` / `load.rs` / `summary.rs` | 669 / 524 / 309 / 110 | ≤ 1,000 | 669 / ~531 / 305 / 110 | ~689 / ~521 / 303 / 108 | ~689 / ~516 / 302 / 107 |

### Ratcheted exception scopes this item touches, and the remedy

- **`billing.rs::drain_once`** carries `#[expect(clippy::too_many_lines)]` and
  `#[expect(clippy::excessive_nesting)]`, which ratchet scope_lines, nested_items and syntax_facts.
  - The edit is line 805, one line for one line.
  - Old facts: call-site `billing_required(...)`, path `billing_required`, path `state` = 3.
  - New facts: method-call `billing_required`, method-call-site, path `state` = 3.
  - No growth, so no remedy is needed. The architecture budget
    `function:src/billing.rs::drain_once` (limit 352, current 349 lines, 774-1122) is unchanged.
- **`fixture_http.rs::http_rig_build`** carries `#[expect(clippy::too_many_lines)]` and
  `#[expect(clippy::let_underscore_must_use)]`.
  - The destructure gains `cli,` (+1 line, 0 facts: a shorthand field pattern has no path).
  - The `fixture_config(...)` call gains an argument: +1 path fact, and rustfmt reflows 1 → 6 lines
    because 102 > 100 cols.
  - Growth is scope_lines +6 and syntax_facts +1 for both expects.
  - **Remedy: re-decide both reasons** (exactly two `;`, no `"`):
    - `too_many_lines`: `"HTTP rig builder; every runtime owner and the scenario's command-line edit are wired in one place so the fixture's dependency order stays visible to scenario authors; pass-through steps would hide which owner a scenario option changed"`
    - `let_underscore_must_use`: `"http_rig_build; the supervisor rejects a spawn only while it is stopping, when the rig is being torn down, whatever command line the scenario configured; a rejected rig task has nothing left to serve"`
- **`bootstrap::run` is deliberately NOT touched.** It carries six reasoned expects: too_many_lines,
  cast_possible_truncation, let_underscore_must_use, expect_used, unwrap_used and
  excessive_nesting.
  - Replacing `config.cli.billing_mode == "required"` (1 path fact) with
    `config.cli.billing_required()` (3 facts) adds +2 syntax_facts per site.
  - Three sites add +6, which fails all six contracts. The remedy would be re-deciding six reasons
    for a spelling change.
  - `run` already reads the clap field (the correct channel), so it keeps its comparisons (§8).
- **No other touched scope carries a reasoned exception.** http.rs `health_axum` (1633) and
  `billing_readiness_axum` (1701) sit under no expect, and http.rs has no inner attribute. Also
  exception-free: validation.rs `validate_billing_prerequisites`, cli.rs `impl CliArgs` (641),
  model.rs `with_knob_defaults`, load.rs `overlay_engine` / `overlay_billing_telemetry_rollup`
  (`overlay_scaler`'s expect is untouched), summary.rs, profile.rs, `fixture_config`, the
  `HttpRigOptions` struct and its Default impl.
- **macro-dsl rows:**
  - `crate::ServerConfig::redacted_summary serde_json::json` count 1 stays 1 (keys are deleted
    inside the one invocation).
  - `crate::billing_readiness_axum serde_json::json` count 3 stays 3.

### C1: "The compactor flags reach the compactor; the overlay stops re-reading their names"

1. **`src/config/model.rs`**
   - `:72-75` field docs. State the owner:
     ```rust
     /// `--compactor-poll-ms` (env COMPACTOR_POLL_MS), default
     /// `crate::DEFAULT_COMPACTOR_POLL_MS`. Clap owns it: `with_knob_defaults`
     /// copies the resolved value so an argv override reaches every DB family.
     pub compactor_poll_ms: u64,
     /// `--compactor-max-concurrent` (env COMPACTOR_MAX_CONCURRENT), default
     /// 4; clap-owned like `compactor_poll_ms`.
     pub compactor_max_concurrent: usize,
     ```
   - `:342-362` `with_knob_defaults`: build the engine before `cli` moves:
     ```rust
     let engine = EngineConfig {
         compactor_poll_ms: cli.compactor_poll_ms,
         compactor_max_concurrent: cli.compactor_max_concurrent,
         ..EngineConfig::default()
     };
     Self { cli, storage: Default::default(), engine, /* rest unchanged */ }
     ```
     The doc becomes: "The no-environment knob posture over `cli` (whose two compactor flags it
     carries)."
   - `impl Default for EngineConfig` stays (it is used by `shard.rs:1089` and by the struct update).
2. **`src/config/load.rs:62-67`**: delete both `if let` blocks. In their place:
   `// COMPACTOR_POLL_MS / COMPACTOR_MAX_CONCURRENT are clap-owned (with_knob_defaults).`
   (−6 +1 lines.)
3. **`src/config/profile.rs:22-24`** doc: "the knobs live in `config::EngineConfig`, parsed once at
   startup: the poll interval and concurrency by clap, the four worker knobs from the environment."
4. **`src/config/certification_tests.rs:28-41`**: the adaptation in §3 (the CLI carries
   `compactor_max_concurrent = 1`).
5. **`src/config/tests.rs`**: add E and F (plus F's helper). `default_values_are_pinned:116-117`
   stays valid, because the deterministic CLI is 2500 / 4.

### C2: "BILLING_MODE and ROLLUP have one reader: health, the billing report and the drain read the clap value"

1. **`src/config/cli.rs`**: append to the non-test `impl CliArgs` (`:641-655`):
   ```rust
   /// BILLING_MODE=required: production billing, where volatile fallbacks
   /// are refused and billing infrastructure failures are fatal at startup.
   /// Clap has already resolved `--billing-mode` over the variable, and no
   /// consumer re-reads the environment, so boot, validation, the drain,
   /// /health and /operator/billing.json agree (item 32). Only the exact
   /// word `required` selects it.
   pub(crate) fn billing_required(&self) -> bool {
       self.billing_mode == "required"
   }

   /// ROLLUP=1: this instance runs the usage rollup consumer and month
   /// closer, so required-mode readiness waits for its rollup database.
   /// Resolved by clap like `billing_required`; only the exact word `1`.
   pub(crate) fn runs_rollup(&self) -> bool {
       self.rollup == "1"
   }
   ```
2. **`src/http.rs`** (−2 lines).
   - `:1670-1673` becomes:
     ```rust
         if state.config.cli.billing_required() {
             let spool_ok = state.billing.read_spool_open();
             let rollup_ok = !state.config.cli.runs_rollup() || state.rollup.installed();
     ```
   - `:1745-1751` becomes:
     ```rust
         let ready = !state.config.cli.billing_required()
             || (state.billing.usage_key().is_some()
                 && spool_open
                 && (!state.config.cli.runs_rollup() || state.rollup.get().is_some()));
         axum::Json(serde_json::json!({
             "mode": &state.config.cli.billing_mode,
     ```
     Both new lines are ≤ 84 cols, so rustfmt keeps them single.
3. **`src/billing.rs`** (−6 lines).
   - Delete `:705-710` (`billing_required`, its doc and the trailing blank line; the doc text moves
     to the accessor).
   - `:805` becomes `} else if state.config.cli.billing_required() {`.
4. **`src/config/validation.rs:834`**: `if !self.cli.billing_required() {`.
5. **`src/config/model.rs`**: delete `mode_env` (`:199-204`) and `rollup_env` (`:207-210`) with their
   docs, and their Default entries (`:452`, `:454`).
6. **`src/config/load.rs`**: delete `:176` and `:178`. Add at the head of
   `overlay_billing_telemetry_rollup`:
   `// BILLING_MODE and ROLLUP are clap-owned: CliArgs::{billing_required, runs_rollup}.`
7. **`src/config/summary.rs`**: delete `:61-62` (`mode_env`, `rollup_env`).
8. **`src/config/tests.rs`**
   - `default_values_are_pinned`: replace `:149` and `:151` with
     `assert!(!c.cli.billing_required());` and `assert!(!c.cli.runs_rollup());`.
   - Add G.
9. **`src/dst/tests/fixture_http.rs`** (fixture option; test-only):
   - `HttpRigOptions` (after `fleet_auth`):
     ```rust
     /// The scenario's own command line over the hermetic fixture: a flag
     /// given on argv, which clap resolves ahead of its variable.
     pub(super) cli: fn(&mut crate::config::CliArgs),
     ```
   - `Default`: `cli: |_| {},`.
   - `fixture_config` gets a first parameter `cli_edit: fn(&mut crate::config::CliArgs)`, applied
     right after `CliArgs::deterministic()` (`cli_edit(&mut cli);`). It has 4 params (≤ 5).
   - `http_rig_build`: destructure `cli,`, then call `fixture_config(cli, max_request_body_bytes,
     instance_name.as_deref(), admission)` (reflowed). Re-decide both expects' reasons as in
     "ratcheted scopes" above.
10. **`src/dst/tests/billing_readiness.rs`** (new): helpers plus A, B and C from §3.
11. **`src/dst/dst_tests.rs`**: after `mod billing_maintenance;` (`:29`), add
    `#[path = "tests/billing_readiness.rs"]` / `mod billing_readiness;` and a blank line.
12. **`scripts/quality/mutation_owners.py`**: extend the `http` row's filters with
    `dst_tests::billing_readiness::` (§5).
13. **Ledgers** of §6 (C2 rows).

### C3: "The read spool nests under the clap-resolved PATH_PREFIX, like the rollup"

1. **`src/billing.rs`** (−5 +2 lines).
   - `:1134-1139` becomes `let prefix = state.config.cli.path_prefix.clone().unwrap_or_default();`.
   - The `open_read_spool` doc gains:
     `/// The spool nests under the clap-resolved PATH_PREFIX inside the data store, as the rollup`
     `/// database does, so argv and environment deployments share one layout.`
2. **`src/config/model.rs`**
   - Delete `path_prefix_env` (`:211-214`) and its Default entry (`:455`).
   - The module doc `:7-9` becomes "(see the two readers of `COMPACT_MAX_SST_SIZE_BYTES` with
     different defaults)".
3. **`src/config/load.rs`**: delete `:179`. Extend the C2 comment to "BILLING_MODE, ROLLUP and
   PATH_PREFIX are clap-owned".
4. **`src/config/summary.rs`**: delete `:72` (`path_prefix_env`).
5. **`src/config/tests.rs`**: delete `:152`.
6. **`src/dst/tests/billing_readiness.rs`**: add D and `spool_roots`.
7. **Ledgers** of §6 (C3 row).

### C4 (held for D-32a): "An unknown BILLING_MODE or ROLLUP word refuses boot"

1. **`validation.rs::validate_billing_prerequisites`**, before the early return:
   - `f.err` "BILLING_MODE={v:?} is neither off nor required" unless `billing_mode ∈ {"off","required"}`.
   - `f.err` "ROLLUP={v:?} is neither 0 nor 1" unless `rollup ∈ {"0","1"}`.
   - The error text carries the env name, so `rejects(.., "BILLING_MODE")` matches.
2. Optionally introduce `enum BillingMode { Off, Required }` parsed here and exposed on
   `ValidatedServerConfig`.
   - Do **not** thread it into `bootstrap::run` (the same +facts problem).
   - The predicates stay: after C4 a validated CLI carries only known words, so they are exact.
3. Add the red test of §3. Also add a `docs/refactor/WIRE-MATRIX.md` / release-note line: a boot
   refusal on an unknown word.

---

## 5. Mutation analysis

**Selection (plan receipt).**
- Only `src/http.rs` is under `CRITICAL_PREFIXES` (`src/http`) and registered (owner `http`).
- These files are not critical and not registered: `src/billing.rs`
  (`'src/billing.rs'.startswith('src/billing/read_spool')` is false), `src/config/*`
  (`admission_limits.rs` is registered but untouched), `src/dst/**` and `src/bootstrap.rs`
  (untouched).
- So C1 and C3 select **no mutants**. C2 selects owner `http` only.
- `src/billing/read_spool.rs` (registered) is not edited.
- The `CliArgs` accessors live in non-critical `config/cli.rs`, so they get no CI mutants. A and B
  kill `billing_required -> true/false` and `runs_rollup -> true/false` anyway.

**In-diff lines in http.rs** (cargo-mutants 27.1.0 `in_diff.rs::affected_lines`: a delete marks the
new line before it, and each insert is marked). With the C2 layout above (new numbering, net −1
after the first hunk):
- health hunk: {1669, 1670, 1671, 1672};
- report hunk: {1743, 1744, 1746, 1747, 1748, 1749}.

| # | Mutant | Line (new) | Killed by (behaviour) |
|---|---|---|---|
| 1 | `replace health_axum -> Response with Default::default()` (body span covers the edit) | 1633-1693 | A: first assert expects 503, and the mutant's `(200, "")`. Also A/B/C `"ok"` bodies |
| 2 | `delete !` in `health_axum` (`!state.config.cli.runs_rollup()`) | 1672 | A step 2 (required, spool open, not owner): mutant `rollup_ok = false \|\| false`, giving 503 ≠ `(200,"ok")`. B step 1: mutant `true`, giving 200 ≠ 503 |
| 3 | `replace \|\| with &&` in `health_axum` | 1672 | A step 2: `!false && false` = false, giving 503 ≠ 200 |
| 4 | `replace billing_readiness_axum -> Response with Default::default()` | 1700-1765 | A/B/C `billing_report`: empty body, so `serde_json::from_slice(..).unwrap()` panics |
| 5 | `delete !` in `billing_readiness_axum` (`!…billing_required()`) | 1744 | A step 1: `true \|\| …`, giving ready true ≠ false. C: `false \|\| (key && false && …)`, giving false ≠ true |
| 6 | `replace && with \|\|` in `billing_readiness_axum` (`&& spool_open`) | 1746 | A step 1: `(true \|\| false) && (true)`, giving true ≠ false |
| 7 | `replace && with \|\|` in `billing_readiness_axum` (the `&& (…rollup…)`) | 1747 | A step 1: `(true && false) \|\| true`, giving true ≠ false |
| 8 | `delete !` in `billing_readiness_axum` (`!…runs_rollup()`) | 1747 | A step 2: `false \|\| false`, giving ready false ≠ true. B step 1: true ≠ false |
| 9 | `replace \|\| with &&` in `billing_readiness_axum` (inner) | 1747 | A step 2: `true && false`, giving false ≠ true |

The unselected `||` on 1745 (`|| (state.billing.usage_key()…`) would also die: A step 2
`false && …` gives false ≠ true. That matters only if rustfmt lays the chain out differently and
the line becomes selected. Nothing is generated inside `json!` arguments (macro bodies are not
mutated). No mutant can hang: every request is answered once.

**Expected:** 9 mutants tested, 9 caught, 0 missed, 0 timeout, 0 unviable. The exact count follows
the final rustfmt layout; recompute from `target/quality/item32-mut` if it differs.

**Owner filter change (C2)** in `scripts/quality/mutation_owners.py`:
```python
    owner('http', 'src/http.rs', 'http:: livefeed_engine_retired security_workload:: debug_store_reports_this_runtimes_shard_opens debug_surface_ dst_tests::billing_readiness::'),
```
- **Why:** no test under the current `http` filters requests `/health` or
  `/operator/billing.json`. The only `/health` tests are in `dst::review_readiness`,
  `runtime_engine_lifecycle` and `runtime_isolation`, none of which match a filter. Without the new
  filter, mutants 1-9 are MISSED.
- **Name choice:** the module is deliberately not named `*http*`, because a substring match on
  `http::` would be accidental ownership. D rides along (4 fast rig tests).
- No planner test pins the `http` row's filters (`test_mutation_owners.py` checks structure only).
- The edit is a `scripts/quality/` change, so the plan also sets `properties_fuzz` (tooling).
  `miri` is set regardless by `src/http` (BUFFER_PREFIXES).

---

## 6. Ledgers (same commit as the change)

**C1:** none.
- `config::` tests are not in the DST inventory.
- No owners, allowances, architecture or review-mechanisms rows change.
- Doc: `profile.rs` comment only.

**C2:**
- **`docs/refactor/test-inventory.json`:** `python3 scripts/test-inventory.py --write` adds 3 rows
  (A, B, C; file `src/dst/tests/billing_readiness.rs`, scenarios `[]`). No existing row moves.
- **`docs/quality/owners.json`:** one row after the `quota_read_volume` row:
  ```json
  {
    "category": "by-path-module",
    "count": 1,
    "owner": "crate::billing_readiness",
    "path": "src/dst/dst_tests.rs",
    "reason": "Billing-readiness scenarios for the clap-resolved billing selectors; real HTTP /health and /operator/billing.json against argv-configured rigs, and the read-spool root under the clap PATH_PREFIX; compiled and executed with DST.",
    "syntax": "path = \"tests/billing_readiness.rs\""
  }
  ```
- **`docs/refactor/review-mechanisms.json` `fixture_changes`:**
  - Update `after_sha256` for `src/dst/tests/fixture_http.rs::default` and
    `::http_rig_build`. Keep `before_commit`/`before_sha256`.
  - Append to their reasons:
    - `default`: " Item 32 adds a no-op command-line edit (`|_| {}`); ordinary rigs keep the
      hermetic fixture CLI."
    - `http_rig_build`: " Item 32 threads the scenario's command-line edit into the hermetic
      configuration factory; no other value or assertion changed."
  - Compute the hashes with:
    `python3 -c "import importlib.util as u,pathlib;s=u.spec_from_file_location('i','scripts/test-inventory.py');m=u.module_from_spec(s);s.loader.exec_module(m);[print(f['name'],f['function_sha256']) for f in m.functions(pathlib.Path('src/dst/tests/fixture_http.rs').read_text(),include_helpers=True) if f['name'] in ('default','http_rig_build')]"`
- **`scripts/quality/mutation_owners.py`:** the `http` row (§5).
- **`docs/refactor/WIRE-MATRIX.md`:**
  - `:242` becomes "`/health` readiness depends on auth mode and the clap-resolved
    `--billing-mode`/`--rollup` (argv over the `BILLING_MODE`/`ROLLUP` variables)".
  - `:202` gains "; `mode` echoes the resolved `--billing-mode` (default `off`)".
- **`src/dst/tests/README.md`:** no change. The "Accounting and admission" row already lists
  `billing_*`.
- **`docs/quality/source-allowances.json`:** nothing vacated (macro-dsl counts unchanged; no
  by-path row for new modules there, since the legacy baseline is frozen).
- **`docs/refactor/architecture-policy.json`:** unchanged (`drain_once` stays 349 ≤ 352).
- **Scenario map / dispositions:** no renames.

**C3:**
- `test-inventory.json`: +1 row (D).
- No other ledger changes: fixture pins were already updated in C2, and D uses no new fixture.

**C4 (held):** WIRE-MATRIX / release note for the refusal. `validation_tests.rs` is not a DST file,
so there is no inventory change.

---

## 7. Controls (run after the in-flight mutation/gate runs finish; the tree is shared)

1. **Red runs.** Each runs on the HEAD of the previous commit plus that commit's test-side edits
   only.
   - `cargo test --locked --lib config::tests::compactor_flags_given_on_argv_reach_the_compactor_options`
     gives the §3 E panic and `test result: FAILED. 0 passed; 1 failed`.
   - `cargo test --locked --lib config::tests::clap_owned_names_keep_their_environment_channel`
     gives `1 passed`. It is a pin, green on HEAD.
   - `cargo test --locked --lib dst_tests::billing_readiness::` (C2 test side) gives
     `2 failed` (A, B with the §3 outputs) and `1 passed` (C).
   - `cargo test --locked --lib dst_tests::billing_readiness::read_spool_nests_under_the_clap_path_prefix`
     (C3 test side) gives the §3 D panic.
2. **Green runs.**
   - The same selectors pass. After C3, `dst_tests::billing_readiness::` gives
     `test result: ok. 4 passed`.
   - `cargo test --locked --lib config::` passes, including `certification_tests`,
     `validation_tests` and G.
   - These also pass: `cargo test --locked --lib dst_tests::billing_`,
     `cargo test --locked --lib debug_surface_`, `cargo test --locked --lib review_readiness`,
     `cargo test --locked --lib runtime_engine_lifecycle`.
3. **Greps after C3.** Each of these must print nothing:
   - `rg -n 'mode_env|rollup_env|path_prefix_env|billing::billing_required' src docs/refactor`
   - `rg -n '"(BILLING_MODE|ROLLUP|PATH_PREFIX|COMPACTOR_POLL_MS|COMPACTOR_MAX_CONCURRENT)"' src/config/load.rs`
   - `wc -l src/http.rs src/billing.rs` gives `3153` / `2148`.
4. **Gates.**
   - `cargo fmt --all -- --check`: no output.
   - `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl`
     then `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl`: exit 0, with no
     `accepted exception grew` line. The two re-decided `http_rig_build` reasons are new
     identities, so they are not compared; `drain_once`'s contracts are unchanged.
   - `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`:
     exit 0 (the new docs use code spans, not links).
   - `python3 scripts/architecture-gate.py --check`: `architecture-gate: OK`, with no file growth.
   - `python3 scripts/test-inventory.py --check`, `python3 scripts/review-evidence.py --check` and
     `python3 scripts/scenario-map-report.py --check`: OK.
   - `python3 -m unittest discover -s scripts/quality`: OK (owner table still valid).
   - `bash scripts/multitenancy-audit.sh` passes. So does
     `scripts/test-leg.sh target/quality/mt-lint.log --exact mt_lint::multitenancy_identity_lint -- --locked --release --lib multitenancy_identity_lint`.
5. **Plan receipt for this item alone**, with `T` = the slate tip before C1:
   - Command: `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$T python3 scripts/quality/verification_plan.py --out target/quality/item32`
   - Expected after C2/C3: `"mutation_source_files": ["src/http.rs"]`,
     `"selected_mutation_owners": ["http"]`, `"mutants": true`, `"miri": true` (src/http),
     `"properties_fuzz": true` (scripts/quality).
   - C1 alone gives `"mutants": false`.
6. **Mutation:**
   `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$T QUALITY_MUTANTS_OUT=target/quality/item32-mut scripts/quality/mutations.sh`
   must report owner `http`: 9 tested, 9 caught, 0 MISSED, 0 TIMEOUT (§5 table). The baseline
   (unmutated) run of the filtered tests must be green.
7. **After push:** check `gh run view` for rust-quality and CI. Do not claim green before that.

---

## 8. Out of scope

- **`bootstrap::run`'s three comparisons** (`:680 == "required"`, `:693`/`:889 == "1"`). They read
  the correct (clap) field. Routing them through the accessors adds +2 syntax_facts per site under
  six reasoned expects.
  - Follow-up: fold them in whenever `run` is next split or its exceptions are re-decided for
    another reason.
- **The double prefix** (`p/p/telemetry/...` for spool and rollup under an env PATH_PREFIX). It is
  the existing on-disk layout of every deployment that sets PATH_PREFIX in the environment. Removing
  it is a data migration, not this item.
- **Item 41 C3 interaction:** COMPACTOR_POLL_MS and COMPACTOR_MAX_CONCURRENT leave `env_parse`.
  - Item 41's C3 list shrinks by two names: its §1a overlay_engine :62/:65 rows go, and the
    production count drops accordingly.
  - Their typo behaviour is already fail-loud today, because clap's u64/usize parser exits 2 before
    `load()`.
  - Any future test that certifies `deploy/profiles/compute-1g.env` through `MapEnvironment` must
    put `COMPACTOR_MAX_CONCURRENT=1` (`compute-1g.env:88`) on the `CliArgs`.
- **The four env-only COMPACT_MAX_* knobs and `BILLING_METER`:** there is no clap flag, so they are
  not duplicates. Unchanged.
- **`docs/MULTITENANCY-MAP.md:430,435`** describe the PATH_PREFIX env re-read. The map is a dated
  2026-08-15 snapshot that cites `src/main.rs` lines that no longer exist, so it is not maintained
  per change.
- **A clap `value_parser = ["off","required"]` / `["0","1"]`:** another route to C4's refusal. It
  would change `--help` and refuse at parse time with exit 2 instead of a typed `validate()`
  finding. That belongs in D-32a if Søren prefers it.
- **Re-adding the three billing selectors to the redacted boot summary** would contradict
  "`cli` excluded wholesale". `/operator/billing.json` `mode` already reports the resolved mode.

---

## 9. Decisions for Søren

- **D-32a: an unknown BILLING_MODE / ROLLUP word refuses boot (C4, held).** This is the same
  policy question as item 41 D1: a typo refusing boot.
  - **Today:** any word other than `required` is silently off, and any ROLLUP other than `1`
    silently means "not the rollup owner". `BILLING_MODE=Required` therefore runs a production cell
    with volatile billing fallbacks and no required-mode readiness gates.
  - **After C4:** `validate()` exits 1 with `configuration invalid (N problem(s))`, naming the
    variable, before any store opens.
  - **Evidence:** every in-repo value is canonical (`BILLING_MODE=required` ×13, `=off` ×1;
    `ROLLUP=1` ×8, `=0` ×1, `$( … && echo 1 || echo 0 )` ×1 across docs/bench/scripts/deploy).
    Values set on live Compute apps outside the repo cannot be checked from here.
  - **Backward-compatible alternative:** a `ConfigNotice` warning. Boot continues with today's
    meaning; flip to an error after one release of logs shows zero hits.
  - **Recommendation:** decide together with item 41 D1 and ship both the same way. Refuse, if D1
    refuses: a mistyped `required` is the most expensive silent default in the config.
- **D-32b: argv-only `--path-prefix` deployments with a usage key move their read spool (C3).**
  - `p/telemetry/read-spool/<inst>` becomes `p/p/telemetry/read-spool/<inst>`, the layout every
    env-configured deployment already has and the one the rollup DB already uses on both channels.
  - Rows still pending at the old root (reads spooled but not yet acknowledged by `_usage`, normally
    drained within `TELEMETRY_DRAIN_SECS`) would be stranded. That means under-billing, and it
    resets that instance's quarantine counter.
  - No in-repo deployment is argv-only with a usage key.
  - **Backward-compatible alternative:** at open, adopt the legacy root when it exists and the new
    one does not (one extra LIST per boot, and a branch that lives for ever).
  - **Recommendation:** land C3 with a release note: "argv `--path-prefix` deployments that set
    USAGE_STREAM_KEY: drain the spool (`/operator/billing.json` `spool.depth == 0`) before
    upgrading".
- **D-32c (acknowledge; no alternative worth keeping): `MEMPROFILE_CERT=compute-1g` now judges the
  argv compactor flags (C1).**
  - An argv `--compactor-max-concurrent 1` with no env value now certifies. It used to refuse,
    because the overlay kept 4.
  - An argv override that contradicts a certified env value (e.g. `--compactor-max-concurrent 8`
    with `COMPACTOR_MAX_CONCURRENT=1`) now refuses boot: the compactor would really run at 8. It
    used to boot at 1 and silently ignore the flag.
  - Keeping the old outcome would mean keeping the bug.

---

## Skeptic corrections (C1..Cn)

Checked on slate @ 6669d3b5, read-only. The following hold up: the five-name intersection (recomputed
with `comm` over the clap `env =` literals and the load.rs literals: exactly BILLING_MODE,
COMPACTOR_MAX_CONCURRENT, COMPACTOR_POLL_MS, PATH_PREFIX, ROLLUP). Every use-site table matches a
repo-wide grep over src/, tools/, fuzz/, examples/, tests/, bench/, deploy/ and docs/ (the only
other hits are in `.claude/worktrees/…`, which is out of tree). The wc -l figures hold (http.rs
3,155, billing.rs 2,157, bootstrap.rs 923, fixture_http.rs 791, dst_tests.rs 267, config/* as
listed). clap_builder is 4.6.0 in Cargo.lock, and `add_env` skips an id already on argv
(parser.rs:1420). The drain_once fact count is equal: 3 → 3. The call-site, path and `state` path
facts become method-call, method-call-site and `state`, and no unwrap/expect lint is in that scope,
so nothing is fingerprinted. http.rs has no inner attribute and no expect over `health_axum` or
`billing_readiness_axum`; the only one nearby is `product_preflight` at :1591. The `http_rig_build`
growth is real: the call line is 97 columns, so adding `cli, ` makes it 102 and the args (64) are
wider than `fn_call_width` 60. That is +6 scope_lines and +1 path fact under two reasoned expects,
so both reasons must be re-decided, and the proposed texts have exactly two `;` and no `"`. The
rustfmt widths of the two new http.rs lines are 83 and 81 columns, so http.rs goes 3,155 → 3,153.
billing.rs goes 2,157 → 2,151 → 2,148. A, B and D all fail on HEAD (plus the fixture option) with
the stated messages. The mutants are killed as tabulated. The owner filter is needed, because
nothing under the current `http` filters requests `/health` or `/operator/billing.json`. The
review-mechanisms pins for `default` and `http_rig_build` are required (`scripts/review-evidence.py`
required_fixtures). bootstrap.rs (owner `bootstrap`, critical, with unwrap/expect-fingerprinted `run`)
is correctly left untouched. No mt-audit baseline row, source-allowance row or architecture budget is
touched: `drain_once` stays within its limit of 352, and `crate::http` reverse edges do not grow.

**C1 (policy, blocks C1 as written): C1 bundles a newly reachable boot refusal.**
`certified_memprofile_errors` (src/config/profile.rs:86-110) measures
`max_concurrent_compactions` and `worker_max_concurrent_compactions`, so after C1 the refusal judges
the argv value. This config boots on HEAD and refuses after C1:
`MEMPROFILE_CERT=compute-1g`, env `COMPACTOR_MAX_CONCURRENT=1`, argv `--compactor-max-concurrent 8`.
D-32c admits this but files it as "acknowledge; no alternative", while the hard rule says a new
refusal stays out of the commits that land now and needs a backward-compatible alternative.
- Fix: split C1.
  - **C1a** lands now: COMPACTOR_POLL_MS only. The poll interval is not a certified measurement
    (profile.rs:86-110), so no refusal changes. E keeps only the `poll_interval` assert.
  - **C1b** is held behind D-32c: COMPACTOR_MAX_CONCURRENT, E's two `max_concurrent` asserts, and
    the certification_tests.rs:28-41 adaptation.
- D-32c must name the backward-compatible alternative. Either keep today's behaviour for
  max_concurrent (argv ignored) until Søren decides, or land C1b together with a startup
  ConfigNotice for one release.
- In-repo exposure is nil: no script, deploy file or doc passes `--compactor-*` on argv (grep over
  bench/, deploy/, docs/, RUNBOOK.md, OPERATIONS.md). The decision is still Søren's.
- F stays a valid pin in both halves.

**C2 (control 5/6 base is wrong for the real push).** CI computes the plan and the in-diff over
`github.event.before` (.github/workflows/rust-quality.yml:19, common.py:20-29 / `verification_comparison`).
If C1-C3 go up with the nine unpushed commits (a4de4f51..6669d3b5) and item 89's owner rows,
selection is the union of everything they touch. That includes the uncommitted offsets, segmap and
telemetry_batch owners plus whatever a342ab6f touches. The "9 tested" expectation covers only owner
`http`.
- Run controls 5/6 with `QUALITY_BEFORE_SHA=<origin/slate at push time>` (currently aaf2baa5), not
  `T`. Confirm every selected owner is 0 MISSED / 0 TIMEOUT before pushing, per the
  "run CI's plan before push" rule.

**C3 (§5 in-diff model is mis-stated; the sets survive).** cargo-mutants 27.1.0 `affected_lines`
(src/in_diff.rs:213-253) marks two lines for every deletion run: the new line before it, and the
next surviving line after it (`prev_removed`). The plan's model covers only the first.
- Here each deletion is immediately followed by an insertion, so the sets {1669-1672} and
  {1743,1744,1746-1749} are still correct.
- If rustfmt or diff pairing turns a deletion into a pure removal, the next context line becomes
  selected. That line would be new 1673, `if !spool_ok || !rollup_ok {`, which adds `|| → &&` and
  two `delete !` mutants. All three are killed by A:
  - step 1 `true && false` gives 200 ≠ 503;
  - deleting `!spool_ok` gives `false || false`, so 200 ≠ 503;
  - at step 2, deleting `!rollup_ok` gives `false || true`, so 503 ≠ 200.
- Correct the model text and add these three rows as "selected only if the layout differs".

**C4 (arithmetic, non-ceilinged).**
- load.rs after C1 is 309 − 6 + 1 = **304**, not 305. The C2 and C3 columns then go 303 → 302.
- config/tests.rs growth is underestimated. E is about 22 lines, F plus its helper about 55 and G
  about 15, so the file lands near 715 after C2, not ~690. It stays under 1,000; fix the table.

**C5 (stale comment C1 makes false).** src/config/tests.rs:89-90 says "a different CLI changes only
the CLI segment — knob defaults are environment-independent". After C1(a) the CLI also feeds
`engine.compactor_poll_ms` (and `compactor_max_concurrent` after C1b).
- Reword it in the same commit to name the clap-owned compactor fields.
- Optionally add `assert_eq!(a.engine, c.engine)`. That still holds for the `--flush-interval-ms`
  CLI, and it pins that only the two compactor flags cross over.

**C6 (D-32b is incomplete).** An argv prefix also moves the spool when argv and the environment
disagree. Example: env `PATH_PREFIX=p` plus argv `--path-prefix q`.
- Today: `data_store = PrefixStore(q)` (bootstrap.rs:63-66) and the spool reads the env copy, so
  the spool is at `q/p/telemetry/read-spool/<inst>`.
- After C3 it is at `q/q/…`.
- Add this case to D-32b's stranded-rows paragraph and to the release note.

**C7 (test teardown, recommended).** A, B and D call `http_rig_build(..).await.parts()`, which drops
the rig's `TaskSupervisor`.
- `/health`'s `state.tasks.unready_reason()` stays None only because the spawned http task holds a
  supervisor clone (fixture_http.rs:534 `serve_tasks`), so the `Weak::upgrade` at tasks.rs:270
  succeeds. This works, and `http_rig_at` does the same, but it is implicit.
- Prefer keeping `let rig = http_rig_build(..).await;` and ending with `rig.shutdown().await`
  (fixture_http.rs:90). That joins the server deterministically and asserts no aborts, instead of
  `engine_shutdown(&state)`.
- No ledger change either way.

**C8 (doc cross-reference, optional).** docs/MULTITENANCY-MAP.md:435 is the row that demands "the
duplicate env read must be unified with the parsed config". C3 satisfies it. No gate reads the map
(scripts/multitenancy-audit.sh fingerprints code, not the map). A one-line "unified by item 32 C3"
note is cheaper than leaving the stale demand, but it is not required.

**Verified with no correction:**
- The reason strings are well-formed.
- The fixture option's closure coerces to `fn(&mut CliArgs)`. All 50 `HttpRigOptions { .. }`
  literals use struct update, and `fixture_config` has a single caller (fixture_http.rs:415).
- `BillingConfig` stays used (runtime.rs:247, billing.rs:1488/1726/1777, ops.rs:775), so there is no
  unused-import fallout from deleting `billing_required`.
- No test feeds BILLING_MODE, ROLLUP, PATH_PREFIX or COMPACTOR_POLL_MS through `MapEnvironment`. Only
  certification_tests.rs:34 feeds COMPACTOR_MAX_CONCURRENT.
- The redacted summary has no consumer in docs/, scripts/ or bench/. `/operator/billing.json` `mode`
  has no consumer in bench/, scripts/ or deploy/.
- The owners.json by-path-module row follows the `crate::quota_read_volume` precedent. The
  test-inventory `--check` has no additions gate. README row `billing_*` already covers the module.

**Verdict: ready-with-corrections.**
- Plan commits C2 and C3 can land as planned once the §5 in-diff model text (correction C3) and the push-base receipt (correction C2) are fixed.
- Plan commit C1 must be split (C1a now, C1b with D-32c; correction C1) or get Søren's explicit ack of D-32c before it lands.
- Plan commit C4 stays held (D-32a).
