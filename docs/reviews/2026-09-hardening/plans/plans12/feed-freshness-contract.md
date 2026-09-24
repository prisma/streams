# Item 93: auth file-feed freshness means "file read now", and the refresher owns the stamp

Tree: `slate` @ `aaf2baa5`. Merge base for the ratchets is `origin/slate` = `6ef3bc64`.
Reviewer text: robustness-maintainability-review.md lines 1724-1734. Every citation below was
re-read on the current tree. Line numbers are current.

## 1 Problem (verified)

### 1.1 Each source stamps its own freshness

The three strict parsers take a clock value and put it into the snapshot:

- `src/auth_feed.rs:109` `pub(crate) fn parse_keys(json: &str, now: i64) -> anyhow::Result<JwksSnapshot> {`
  and at 142-146:
  ```rust
  Ok(JwksSnapshot {
      keys,
      fetched_at_unix: now,
      feed_version: doc.feed_version,
  })
  ```
- `src/auth_feed.rs:149` `parse_policies(json: &str, now: i64)`, with `fetched_at_unix: now,` at 172.
- `src/auth_feed.rs:177` `parse_grants(json: &str, now: i64)`, with `fetched_at_unix: now,` at 227.

Each file source reads the clock itself (`src/auth_feed.rs:239-268`):
```rust
fn unix_now() -> i64 { std::time::SystemTime::now() ... .unwrap_or(0) }
impl KeySource for FileKeySource    { ... parse_keys(&raw, unix_now()) }      // :250
impl PolicySource for FilePolicySource { ... parse_policies(&raw, unix_now()) } // :258
impl GrantSource for FileGrantSource  { ... parse_grants(&raw, unix_now()) }   // :266
```
The refresher publishes whatever freshness the source returned (`src/auth_feed.rs:297-298`,
`Ok(Ok(snapshot)) => match publish(snapshot) {`). `refresh_once` (`:325-335`) passes it
through unchanged (`|s| auth.publish_jwks(s)`, and the same for policies and grants). So each
`Source` implementation decides freshness today. A Control-Plane source with a cache would copy
that pattern. If it stamped its cached copy with `now`, a dead origin would never age. If it
stamped a future time, the fail-closed window would be defeated. Nothing in the types or the
docs stops either.

### 1.2 A static file never ages, and nothing documents it

`spawn_refresher` (`:355-388`) re-runs `refresh_once` every `STREAMS_AUTH_REFRESH_SECS`. The
default is 30, and `src/config/validation.rs:892-901` bounds it at 300/3 = 100. Each pass
re-reads the file and stamps `unix_now()`. `publish_*` accepts an identical replay at the same
`feed_version`, because the generation digest matches (`src/auth/publication.rs:330-336`,
`404-410`, `465-471`; the digests exclude `fetched_at_unix`, see `jwks_digest` :242-260 and the
`format!("{p:?}")` rows). So an unchanged, valid file stays at age of at most one cadence
indefinitely. This is the intended contract: MULTITENANCY.md §7.1 names the fields
`last_jwks_refresh`/`last_policy_refresh`, which are refresh times, not publisher times. It is
nowhere stated as such, though:

- The module doc (`src/auth_feed.rs:4-8`) says only that "`verify_customer` fails CLOSED when a
  snapshot's age exceeds `POLICY_STALENESS_MAX_SECS` — so a dead refresher degrades to
  refusal". It never says that a live refresher over a static file keeps the feed fresh, and it
  gives no way to tell whether the author is alive.
- `src/project_policy.rs:100-102`: "`fetched_at_unix` drives the fail-closed staleness window".
  It does not say who stamps it.
- `src/project_policy.rs:139-143` (trait doc): "The refresher task ... calls `fetch`, then
  publishes the result". Nothing says what `Ok` is allowed to mean for a cached origin.
- `src/config/cli.rs:328-329`: "Operator-authored snapshot files (src/auth_feed.rs wire
  formats). All three are required when STREAMS_AUTH_MODE != off." This is also the
  `--help` text, and it says nothing about freshness.

### 1.3 A gap in the reviewer's monitoring recipe

The reviewer says to "monitor feedVersion via /v1/debug/auth". `AuthService::feed_json`
(`src/auth.rs:876-903`) exposes `feedVersion` for `policies` and `grants` only:
`"jwks": { "keys": jwks.keys.len(), "ageSecs": age(jwks.fetched_at_unix) }`. The keys file's
`feed_version` is also optional (`KeysDoc`, `#[serde(default)] feed_version`, `:45-46`). The doc
must therefore name the policy/grant `feedVersion`. Exposing a key version is a wire change;
see Decision D1.

### 1.4 Full use-site list of the stamp (`fetched_at_unix`)

Producers:
- Parsers: `src/auth_feed.rs:144`, `:172`, `:227`. They are the only producers on a production
  path, and are reached only through the three file sources `:250/:258/:266`.
- Wiring: `src/bootstrap.rs:753-766` constructs the three `File*Source`s. It does not change,
  because constructors and trait signatures are untouched.
- `empty()` constructors (0 = never fetched): `src/auth.rs:330`, `src/project_policy.rs:115`,
  `:133`.
- Direct test publications, which bypass the refresher and keep their explicit stamps. None of
  them go through `refresh_once`, so none are affected: `src/auth.rs` tests (:1020-1594),
  `src/auth/publication/tests.rs` (:137-195), and DST `src/dst/tests/{quota_enforcement,
  security_audit, fixture_auth, security_noninterference, watch_observation, security_modes,
  admission_memory, security_isolation, security_workload, security_freshness,
  security_subscription, billing_attribution}.rs`. The owned-clock staleness proofs in
  `security_freshness.rs:64-94` publish directly with `self.now()`.

Consumers (all unchanged):
- `src/auth.rs:500`, `:513` (`lease_check` PolicyStale/GrantsStale), `:540`, `:542`
  (`lease_deadline`), `:603` (`verify_signature` KeysStale), `:704`, `:736` (`verify_customer`),
  `:864` (`status_and_quotas`), `:889-900` (`feed_json` ageSecs/stale).
- `src/http.rs:1037` (`debug_auth` → `/v1/debug/auth`) and `:1642-1658` (`/readyz` refuses
  while any `ageSecs` is null, i.e. a stamp of 0).
- No script, bench or doc reads `ageSecs`, `feedVersion` or `stalenessMaxSecs`. A grep over
  `scripts/ bench/ docs/` finds none.

Only `src/auth_feed.rs` implements `KeySource`/`PolicySource`/`GrantSource`: the three file
sources and the test fixtures `ParkedKeys`, `EmptyPolicies` and `EmptyGrants` (`:394-414`).
`parse_*` has no caller outside `src/auth_feed.rs`.

## 2 Contract decision

The contract is typed. The refresher owns freshness, and a source has only one decision:
`Ok` or `Err`.

1. **The stamp is taken in one place: `refresh_source`.** `let asked_at = unix_now();` is read
   before the fetch starts. `async_trait` futures are lazy, so the source body has not run yet
   at that point. The stamp is handed to `publish` only in the `Ok(Ok(snapshot))` arm, after an
   answer inside `SOURCE_DEADLINE`. A timeout, an `Err`, or a publication refusal discards it,
   so the previous snapshot keeps ageing. `publish` becomes `impl FnOnce(T, i64) -> Result<(),
   String>`, and each of the three `refresh_once` closures applies the stamp with struct update
   (`JwksSnapshot { fetched_at_unix, ..s }`). Whatever `fetched_at_unix` a source put in its
   snapshot is overwritten.
   - **Why the ask instant and not the answer instant.** The origin's answer is only known to be
     at least as new as the moment the refresher asked. Stamping at receipt could claim up to
     `SOURCE_DEADLINE` (10 s) more freshness than is guaranteed. Stamping at ask time errs
     toward fail-closed, never away from it. For file feeds the two instants differ by the
     read latency: microseconds, almost always the same whole second. The reviewer's literal
     alternative is `publish(snapshot, unix_now())` inside the arm. It is equally buildable,
     one line shorter, and the tests below pass under either.
2. **Parsers do not stamp.** `parse_keys/parse_policies/parse_grants(json)` lose the `now`
   parameter and return `fetched_at_unix: 0`, the never-fetched value that `empty()` and
   `feed_json`'s `age` already use. A parsed snapshot that bypassed the refresher reads as
   unpublished and fails closed: `now - 0 > w` gives PolicyStale/GrantsStale/KeysStale, and
   `/readyz` answers 503 "auth feeds not yet published".
3. **Documented meaning of freshness for a FILE feed:** the time since the last successful read
   and parse, not publisher liveness. An unchanged file stays fresh while it stays readable and
   valid. The author's liveness shows only as the policy and grant `feedVersion` on
   `/v1/debug/auth`. A source answers `Ok` only with what this call obtained from its origin,
   never a cached copy in place of an unreachable origin. The previous snapshot is already
   retained and ageing, and an `Ok` would restart that age.

**No wire change.** The `/v1/debug/auth` JSON keeps the same keys and shape. For file feeds,
`ageSecs` differs only by the file read time (sub-second). No status code changes, and no
metric name or `/metrics` shape changes. The one text change is clap's `--help` for
`--streams-auth-keys-file`, which gains a second paragraph. The `-h` summary is the first
paragraph and does not change. That is not a wire contract. `config::tests::cli_surface_is_pinned`
pins flag, env and default, and none of those change. No `as_of` field is added, as the
reviewer asked.

## 3 Tests

### 3.1 Red test (behaviour change: the refresher, not the source, owns the stamp)

File `src/auth_feed.rs`, `mod tests`. It compiles unchanged on `aaf2baa5` because it uses only
`refresh_once`, `RefreshOutcome`, `AuthService`, the `empty()` constructors and `unix_now`.

```rust
/// Stands in for a cached origin that stamps its own reads: the refresher
/// must not take a source's word for how fresh its answer is.
struct SelfStamped(i64);
#[async_trait::async_trait]
impl KeySource for SelfStamped {
    async fn fetch(&self) -> anyhow::Result<JwksSnapshot> {
        Ok(JwksSnapshot { fetched_at_unix: self.0, ..JwksSnapshot::empty() })
    }
}
// ... the same for PolicySource (PolicySnapshot::empty()) and GrantSource (GrantSnapshot::empty())

#[tokio::test]
async fn the_refresher_not_the_source_stamps_feed_freshness() {
    // An ancient stamp would refuse fresh data; a post-dated one would keep
    // a dead feed authorizing past the fail-closed window.
    for stamp in [1, unix_now() + 86_400] {
        let svc = AuthService::new(crate::auth::AuthMode::Shadow, "issuer".into(), "cell").unwrap();
        let source = SelfStamped(stamp);
        let report = refresh_once(&svc, &source, &source, &source).await;
        assert_eq!([report.keys, report.policies, report.grants], [RefreshOutcome::Published; 3]);
        let feeds = svc.feed_json(unix_now());
        for feed in ["jwks", "policies", "grants"] {
            let age = feeds[feed]["ageSecs"].as_i64().unwrap();
            assert!(
                (0..=60).contains(&age),
                "{feed}: a published fetch is fresh whatever the source stamped ({stamp})"
            );
        }
    }
}
```
The `(0..=60)` bound is the house tolerance used by `security_routes.rs:647`. Both red values
are far outside it.

**Trace on the current tree.** The first iteration uses `stamp = 1`. All three empty snapshots
publish, because each `publish_*` on a fresh service sees `feed_version 0 >= 0`, no
generation history and no entries. So `report` is `[Published; 3]` and that assertion passes.
The published JWKS then keeps `fetched_at_unix = 1`. `feed_json` computes
`age(1) = Some(now - 1)`, about 1.79e9, `(0..=60).contains` is false, and the loop fails on
`jwks` first. Expected red output (deterministic apart from line and column):
```
running 1 test
test auth_feed::tests::the_refresher_not_the_source_stamps_feed_freshness ... FAILED
...
thread 'auth_feed::tests::the_refresher_not_the_source_stamps_feed_freshness' panicked at src/auth_feed.rs:<L>:<C>:
jwks: a published fetch is fresh whatever the source stamped (1)
...
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; <N> filtered out
```
After the fix, both iterations have their stamp replaced by `asked_at`, so every age is 0 or 1.
The post-dated iteration (`now + 86_400`) guards the more dangerous direction. On the old tree
it would give `ageSecs = -86400` and `policies.stale = false` for a day.

### 3.2 Characterization pin (the reviewer's "first step"; passes before and after)

`file_feed_freshness_means_last_successful_read_not_publisher_liveness` in `src/auth_feed.rs`
`mod tests`. It lands in commit 1 on the unchanged tree.

1. Use temp dir `mt-feed-freshness-<pid>`. The name is distinct from the `mt-feed-<pid>` used
   by `refresh_once_publishes_from_files_and_survives_a_broken_one`, since both tests run in
   one process and share a pid. Build `FileKeySource/FilePolicySource/FileGrantSource` over
   `keys.json/policies.json/grants.json`.
2. Publish policies and grants directly at `feed_version 7`, empty, with
   `fetched_at_unix = unix_now() - 250`. This stands for the author last writing generation 7
   about 250 s ago.
3. Write `"{ not json"` to all three files and run `refresh_once`. Assert
   `[report.policies, report.grants] == [Unavailable; 2]` and
   `feeds[f]["ageSecs"] >= 250` for policies and grants: a failed read refreshes nothing.
4. Write `{"feed_version":7,"projects":[]}` and `{"feed_version":7,"credentials":[]}` and run
   `refresh_once`. Assert `[Published; 2]`, `(0..=60).contains(ageSecs)`, and
   `feeds[f]["feedVersion"] == 7`: the same generation read again is fresh, though its author
   published nothing.
5. `std::fs::remove_dir_all(&dir).unwrap()`. This uses `unwrap`, which clippy allows in tests,
   not `let _ =`, so the test needs no new `let_underscore_must_use` exception.

It uses no `serde_json::json!` and no `tokio::` macros (JSON is raw string literals), so it
needs no `macro-dsl` ledger row. On `aaf2baa5`, step 4 publishes `parse_policies(&raw, unix_now())`,
an identical replay at gen 7 with the same digest. After the fix it publishes the
refresher-stamped version. Both pass.

### 3.3 Compile-level proofs and pins for the refactor parts

- `parse_*(json)` has no clock input, so a parser cannot stamp anything but its constant.
  `keys_parse_and_reject` changes `assert_eq!(s.fetched_at_unix, NOW)` to
  `assert_eq!(s.fetched_at_unix, 0, "parsing never stamps freshness; the refresher does")`,
  which pins the constant.
- `publish: impl FnOnce(T, i64)` forces every publish closure to receive the stamp. Dropping
  it means either an unused closure parameter, which `unused_variables` fails under
  `-D warnings`, or a visible `_` rename.
- Existing tests also go red if any closure fails to apply the stamp. Parsed snapshots now
  carry 0, so a dropped stamp would make:
  - `slow_keys_do_not_delay_other_publications_and_pass_is_bounded` fail
    (`!feeds["policies"]["ageSecs"].is_null()`, `:426-427`);
  - `refresh_once_publishes_from_files_and_survives_a_broken_one` fail
    (`f["policies"]["stale"] == false`, `:510`).
- The publication-side pins are unchanged and still hold: a refused snapshot changes neither
  the snapshot nor its age. See `src/auth/publication/tests.rs`,
  `refused_{policy,grant,key_rebinding}_changes_neither_snapshot_history_nor_generation`
  (`Arc::ptr_eq`).

## 4 Edits in commit order

No ceilinged file is touched. Current `wc -l`, with merge-base ceiling in parentheses:
http.rs 3,155 (3,159), product.rs 4,205 (4,205), shard.rs 3,186 (3,196), billing.rs 2,157
(2,201), history.rs 1,713 (1,713), auth.rs 1,676 (1,676, zero budget, so the misplaced
`JwksSnapshot` doc stays out of scope), registry.rs 1,492, sse/feed.rs 1,165, fleet.rs 1,142:
all untouched. Touched files and their limits:

| File | Now | After (est.) | Limit | Note |
|---|---|---|---|---|
| `src/auth_feed.rs` | 617 | ~745 | 1,000 (new-file rule; not oversized) | not a critical prefix, not a mutation owner |
| `src/config/cli.rs` | 663 | 668 | 1,000 | not a critical prefix, not an owner |

`src/project_policy.rs` (152) is **deliberately not edited**. Its crate-level
`#![allow(dead_code, reason = ...)]` (`:16-19`) has no enclosing item, so its exception scope
is the whole file. Any added line grows `scope_lines`, and new doc attributes grow
`syntax_facts`: "accepted exception grew". The trait contract therefore lives in the
`auth_feed.rs` module doc, next to the sentence that already names all three traits.

Ratcheted scopes in the touched files: `spawn_refresher`'s two `#[expect]`s
(`too_many_arguments`, `let_underscore_must_use`, `:347-354`) and the test
`refresh_once_publishes_from_files_and_survives_a_broken_one`'s `#[expect(let_underscore_must_use)]`
(`:455-458`). Neither body changes, so `scope_lines`, `nested_items` and `syntax_facts` stay the
same, and no unwrap/expect fingerprinting scope exists in either file. No remedy is needed.

### Commit 1: "File-feed freshness is pinned as the last successful read, not the author's liveness"
- `src/auth_feed.rs` `mod tests`: add `file_feed_freshness_means_last_successful_read_not_publisher_liveness`
  (§3.2, about 55 lines). No production token changes. The planner would classify the file as
  production-unchanged, and it is not critical in any case.

### Commit 2: "The refresher stamps feed freshness at the fetch it asked for; a source only answers Ok or Err"
Run §3.1 red first (message above). Then, in `src/auth_feed.rs`:
1. Module doc: insert after line 14 ("...nothing on the request path changes then."), about 11 lines:
   > Freshness belongs to the refresher, never to a source. A feed's age counts from the pass
   > that last asked its source and published the answer, so no source can post-date its data
   > or keep alive a feed it did not fetch. For a FILE feed that means "read and parsed then",
   > not "its author is still publishing": an unchanged file stays fresh while it stays
   > readable and valid, and the author's liveness shows only as the policy and grant
   > `feedVersion` on `/v1/debug/auth`. A source therefore answers `Ok` only with what this call
   > obtained from its origin. A cached copy standing in for an unreachable origin would restart
   > the age the fail-closed window measures; an `Err` leaves the previous snapshot ageing toward
   > refusal.
2. Section comment `:107`: add after "Strict parsers (...)" the lines: "A parsed snapshot is
   unstamped (0, the never-fetched age): only the refresher stamps, so one that skipped it
   reads as unpublished and fails closed." (+2 lines)
3. `parse_keys/parse_policies/parse_grants`: drop `, now: i64`, and `fetched_at_unix: now` becomes
   `fetched_at_unix: 0` (`:144/:172/:227`).
4. File sources: `parse_keys(&raw)`, `parse_policies(&raw)`, `parse_grants(&raw)` (`:250/:258/:266`).
   `unix_now` stays in place, keeping the diff minimal. Its production caller is now the refresher.
5. `refresh_source` (`:292-317`): add a doc comment:
   > The one place freshness is stamped: the instant the pass asked, attached only to a snapshot
   > the source returned inside the deadline. Asking time rather than answering time, because the
   > origin's answer is only known to be at least that new.

   The signature becomes `publish: impl FnOnce(T, i64) -> Result<(), String>`. Add
   `let asked_at = unix_now();` before the `match`, and change the arm to
   `Ok(Ok(snapshot)) => match publish(snapshot, asked_at) {`.
6. `refresh_once` (`:325-335`): closures become `|s, fetched_at_unix| auth.publish_jwks(JwksSnapshot { fetched_at_unix, ..s })`,
   and the same with `PolicySnapshot` and `GrantSnapshot`. All three types are already
   imported. The `tokio::join!` count stays at 1, so the `macro-dsl` row for
   `crate::refresh_once` is unchanged.
7. Tests:
   - `EmptyPolicies`/`EmptyGrants` drop the `unix_now()` argument (`:405/:412`).
   - `keys_parse_and_reject`, `policies_parse_strictly` and `grants_parse_strictly` drop `, NOW`
     (9 call sites). Delete `const NOW` (`:524`), which would otherwise be dead in cfg(test).
     Change the `:537` assert to `0` (§3.3).
   - Add the `SelfStamped` fixture and `the_refresher_not_the_source_stamps_feed_freshness` (§3.1).
     They use no `json!`/`select!`/`join!`/`pin!`, so no ledger row is needed. The `json!` counts
     for the three parse tests stay 1/1/2.

In `src/config/cli.rs:328-329`, append a second doc paragraph to `streams_auth_keys_file` (+5):
```rust
/// Operator-authored snapshot files (src/auth_feed.rs wire formats).
/// All three are required when STREAMS_AUTH_MODE != off.
///
/// A feed's age counts from its last successful read, so an unchanged
/// file stays fresh while it stays readable and valid; whether its
/// author still publishes shows only as the policy and grant
/// `feedVersion` on /v1/debug/auth.
```
The prose has no `[..]` or `<..>`, so it is rustdoc-safe. The commit message records the red
output from §3.1.

## 5 Mutation analysis

- **Selection.** Neither `src/auth_feed.rs` nor `src/config/cli.rs` falls under
  `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:21-30`), and neither is in
  `mutation_owners.OWNERS`. `src/bootstrap.rs`, which is critical, is not touched. The plan for
  this item's range therefore has `mutation_source_files: []` and `mutants: false`, and no
  owner row or filter changes. The push that carries this also carries the four existing
  unpushed commits, whose own selection is unaffected.
- What cargo-mutants 27.1.0 would generate in-diff if the file were registered, and the killers:
  - `parse_keys/parse_policies/parse_grants` FnValue (the span covers the `fetched_at_unix: 0`
    line). `Ok(Default::default())` is unviable, because the snapshots have no `Default`.
    `Err(anyhow!("mutated!"))` is killed by `keys_parse_and_reject`, `policies_parse_strictly`
    and `grants_parse_strictly` (`.unwrap()` on a valid document).
  - `File*Source::fetch` FnValue. `Ok(Default::default())` is unviable. `Err(..)` is killed by
    `refresh_once_publishes_from_files_and_survives_a_broken_one` (`jwks.keys == 1`) and by
    the §3.2 pin (`[Published; 2]`).
  - `refresh_source` and `refresh_once` return `RefreshOutcome`/`RefreshReport`, which have no
    `Default`, so their FnValue mutants are unviable. No binary operator, guard or `_` arm is
    added. cargo-mutants deletes struct-literal fields only when the base is
    `..Default::default()`. The base here is `..s`, so no field-deletion mutant is generated.
    If one were, the missing-stamp version is killed by §3.1 (all three feeds) plus the two
    existing tests in §3.3.
- No equivalent mutant needs restructuring.

## 6 Ledgers

None change. The reason for each:
- `docs/refactor/test-inventory.json`: DST tests only (`scripts/test-inventory.py` docstring).
  No `src/dst` file changes.
- `docs/refactor/review-mechanisms.json`: no pinned DST test is touched.
- `docs/quality/owners.json`: no new global static, macro-dsl, glob or by-path occurrence. The
  new tests use only expression macros (`assert*`, `format!`) and attribute macros
  (`#[tokio::test]`, `#[async_trait]`).
- `docs/quality/source-allowances.json`: every existing `src/auth_feed.rs` row keeps its count:
  `refresh_once` join 1, `spawn_refresher` select 2, the parse tests' `json!` 1/1/2,
  `keys_json` 1, `refresh_once_publishes...` 2, `slow_keys...` pin/select 1/1, and the
  `crate::tests` glob 1. Nothing is vacated.
- `docs/refactor/architecture-policy.json`, `WIRE-MATRIX.md` (`/v1/debug/auth` still returns
  "200 JSON shadow/feeds/admission"), scenario map, `src/dst/tests/README.md`: untouched.
- `docs/quality/verification.json` and `docs/refactor/architecture-review-baseline.json` hold
  frozen adoption-time hashes of `src/auth_feed.rs`. They are already stale by design, and no
  gate compares them to the live file.

## 7 Controls (run by the implementer after the mutation run releases the tree)

1. Pin on the old tree, after commit 1:
   `cargo test --locked --lib auth_feed::tests::file_feed_freshness_means_last_successful_read_not_publisher_liveness -- --exact`
   gives `test result: ok. 1 passed; 0 failed`.
2. Red, with only the §3.1 test added on top of commit 1:
   `cargo test --locked --lib auth_feed::tests::the_refresher_not_the_source_stamps_feed_freshness -- --exact`
   gives `FAILED`, with the panic message `jwks: a published fetch is fresh whatever the source stamped (1)`.
3. Green after commit 2: `cargo test --locked --lib auth_feed::` gives
   `test result: ok. 8 passed; 0 failed` (6 existing + 2 new).
4. Neighbours:
   - `cargo test --locked --lib auth::` gives ok (publication/verify pins).
   - `cargo test --locked --lib dst_tests::security_freshness::` and
     `cargo test --locked --lib dst_tests::security_routes::` give ok. These publish directly
     and are unaffected; the run confirms it.
   - `cargo test --locked --lib config::tests::cli_surface_is_pinned -- --exact` and
     `cargo test --locked --lib config::tests::legacy_absorber_help_is_honest_about_the_active_controls -- --exact`
     give ok.
5. Gates:
   - `cargo fmt --all -- --check` produces no output.
   - `cargo clippy --locked --workspace --all-targets -- -D warnings` exits 0.
   - `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
     exits 0.
   - `python3 scripts/architecture-gate.py --check` ends with
     `architecture-gate: OK (... Rust files; fixed baseline a7e2070f3b43)`. This covers file
     growth, the exception ratchet and the source allowances, with no "obsolete source
     allowances".
   - `python3 scripts/test-inventory.py --check` gives OK.
   - `bash scripts/multitenancy-audit.sh` passes.
6. Plan receipt for this item alone:
   `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=aaf2baa5 python3 scripts/quality/verification_plan.py --out target/quality/item93`
   gives `"changed_rust_files": ["src/auth_feed.rs", "src/config/cli.rs"]`,
   `"mutation_source_files": []`, `"mutants": false`, `"loom": false`, `"miri": false`,
   `"properties_fuzz": false`, `"compiler": true`.
7. Optional help check: `cargo run --locked --bin streams-slate -- --help` shows the new
   paragraph under `--streams-auth-keys-file`, and `-h` shows only the first paragraph.
8. Loom and held-commit tests are not required. No synchronization changes: the stamp is a
   plain value computed before publication, and publication's lock and ordering are untouched.

## 8 Out of scope

- A typed `as_of` or publisher-age semantics for a future Control-Plane source. The reviewer
  says not now. It would need a rule on which clock wins, and a post-dated `as_of` is exactly
  the hazard §3.1 closes.
- `feeds.jwks.feedVersion` on `/v1/debug/auth`: see D1.
- The misplaced doc in `src/auth.rs:292-295`. The `JwksSnapshot` paragraph is attached to
  `JwksKey`, so `JwksSnapshot` has no doc. auth.rs is at its 1,676 ceiling. A net-zero move is
  possible but unrelated.
- Registering `src/auth_feed.rs` as a mutation owner. It is an authorization input, but no
  critical prefix covers it today. That would be a separate owner decision with filter
  `auth_feed::`.
- Moving `unix_now` under the refresher section, and MULTITENANCY.md wording. §7.1's
  `last_*_refresh` already matches.

## 9 Decisions for Søren

- **D1 (optional, additive wire change on `/v1/debug/auth`).** Add `"feedVersion": jwks.feed_version`
  to `feeds.jwks` so the key author's liveness can be watched like the policy and grant
  authors'. Keys files may omit `feed_version` (serde default 0), so it would read 0 until
  authors version them. **Backward-compatible default (this plan):** no change. The docs name
  only the policy and grant `feedVersion`, and a static keys file's author liveness stays
  unobservable on `/v1/debug/auth`.

## Skeptic corrections (C1..C6)

Re-verified on `aaf2baa5` (read-only). These hold as the plan states them:
- Every quoted line in §1 (`src/auth_feed.rs:109/142-146/149/172/177/227/239-268/292-317/319-341/347-354/455-458/524/537`,
  `src/auth.rs:292-295/321-333/876-903`, `src/auth/publication.rs:242-260/318-336/382-410/446-471`,
  `src/project_policy.rs:16-19/100-102/139-143`, `src/config/cli.rs:328-329`,
  `src/config/validation.rs:892-901`, `src/http.rs:1037/1642-1658`, `src/bootstrap.rs:753-766`).
- The use-site lists are complete. `git grep` finds no caller of `parse_*`, `refresh_once` or `File*Source`,
  and no other `impl {Key,Policy,Grant}Source`, anywhere outside `src/auth_feed.rs` and `src/bootstrap.rs`,
  including `src/dst`, benches and fuzz. DST reaches the feeds only through direct `publish_*`. No DST rig
  boots with `STREAMS_AUTH_*_FILE`. The only other hits are frozen legacy baselines
  (`docs/quality/legacy-diagnostics*.json` unreachable_pub rows for `crate::parse_*`, whose items are
  `pub(crate)` and emit nothing; `legacy-source.json` lines 605; `verification.json`;
  `architecture-review-baseline.json` lines 503). None of them is compared against the live file in a way
  this edit trips. `source_rules.violations` limit = max(1000, 605) = 1000 against an estimated ~745 lines,
  and `architecture-gate` gives the default 1000.
- The `wc -l` values and merge-base ceilings in §4 are exact. No ceilinged file is touched.
- Ratchets: `auth_feed.rs` has only the two `spawn_refresher` expects and the test expect. Their bodies are
  unchanged, so no metric grows. There is no unwrap/expect-fingerprint scope in either touched file.
  `project_policy.rs`'s `#![allow(dead_code)]` has crate scope (no enclosing item, so the scope is the whole
  file, per `source_rules.exception_contracts`), and leaving that file alone is correct.
- The macro-dsl, glob and effect inventory is unchanged. `#[tokio::test]` and `#[async_trait]` classify as
  None, `format!`/`assert*` are EXPRESSION_MACROS, and `std::env::temp_dir`/`std::process::id` do not
  match the effect prefixes.
- The §3.1 red trace is right. `AuthService::new(Shadow,"issuer","cell")` publishes three empty snapshots:
  feed_version 0 >= 0, no gen history. `feed_json` gives `age(1) = now-1`, and the loop fails on `jwks` first
  with exactly `jwks: a published fetch is fresh whatever the source stamped (1)`. After the fix the age is
  0 or 1. `RefreshOutcome` is `Copy + PartialEq + Debug`, so `[..; 3]` builds. The test is bounded: no parked
  source, and the 10 s deadline in real time is never reached, so there is no TIMEOUT.
- §3.2 passes before and after. An identical gen-7 replay has an equal digest (digests exclude
  `fetched_at_unix`).
- Mutation selection: neither file is under `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:21-30`)
  or in `mutation_owners.OWNERS` (the only config owner is `src/config/admission_limits.rs`), so
  `mutants: false` is right.
- Docs: MULTITENANCY.md §7.1 (`last_*_refresh`, :623-624), :1376 ("feed freshness") and
  bench/WORKLOAD-CERT-PLAN.md:578 ("refresh moves the deadline") all agree with refresher-owned freshness.
  Nothing in scripts/bench/docs reads `ageSecs`/`feedVersion`.

Corrections:

- **C1 (buildability, §3.2 step 3).** `feeds[f]["ageSecs"] >= 250` does not compile, because
  `serde_json::Value` has `PartialEq<i64>` but no `PartialOrd`. Write
  `assert!(feeds[f]["ageSecs"].as_i64().is_some_and(|a| a >= 250), "{f}: a failed read refreshes nothing: {feeds}")`.
  Likewise step 4 should use `.as_i64().is_some_and(|a| (0..=60).contains(&a))`, matching
  `security_routes.rs:646-647`. Step 2's direct `publish_policies/publish_grants` return
  `Result<(), &'static str>` and need `.unwrap()`, which is fine under `allow-unwrap-in-tests = true`
  (clippy.toml).
- **C2 (D1 is under-specified).** `src/auth.rs` is 1,676, equal to its merge-base ceiling: zero budget. D1
  must say that it is net-zero. The `json!` body is not reformatted by rustfmt, so
  `"feedVersion": jwks.feed_version` can go on the existing `auth.rs:889` line. D1 must also name its pin, for
  example extending `security_routes.rs:640-647` to assert `v["feeds"]["jwks"]["feedVersion"]`, which touches
  a DST file and therefore needs the `docs/refactor/test-inventory.json` rewrite and a check that the test is
  not pinned in `review-mechanisms.json`. `WIRE-MATRIX.md:210` ("200 JSON shadow/feeds/admission") needs no
  edit.
- **C3 (help-text claim, §2 "No wire change").** Only `-h` is unchanged. clap derive strips the trailing
  period only from the short/single-paragraph form. Once a second paragraph exists, `--help` renders
  `long_help`, and there the first paragraph keeps "…!= off." with its period. Nothing pins it:
  `cli_surface_is_pinned` covers flag/env/default, and `legacy_absorber_help_is_honest_about_the_active_controls`
  (`src/config/tests.rs:550`) scans only the absorb flags. `cli.rs:119` already has a multi-paragraph arg,
  so the `--help` layout does not change. Reword the claim; no action needed.
- **C4 (controls, §7.5-7.6).** `architecture-gate.py --check` (via `source_gate.check`) and
  `verification_plan.py` shell out to `target/debug/streams-quality-syntax` (`scripts/quality/common.py:121`).
  Run `cargo build --locked -p streams-quality-syntax` first, as `scripts/quality.sh:15` does. Add the
  remaining gates that `quality.sh:37-43` runs: `scenario-map-report.py --check`, `review-evidence.py --check`,
  and `scripts/test-leg.sh … --exact mt_lint::multitenancy_identity_lint`. Simpler: run `bash scripts/quality.sh`
  once. Control 6: set `QUALITY_BEFORE_SHA` to the item's actual parent commit. It is `aaf2baa5` only if no
  other item lands first.
- **C5 (canonical placement, §4 step 4).** After the edit, `unix_now` (`auth_feed.rs:239-244`) has no
  File-source caller left. Its only production caller is `refresh_source`. Leaving it under "File-backed
  sources" misstates ownership, which is the point of this item. Move it verbatim under "// The refresher"
  (`:270-271`) in commit 2. That is intra-file, net zero lines, with no ratchet, owner or mutation effect.
- **C6 (doc precision, §4 module doc and cli.rs paragraph).**
  (a) The stamp is taken when the pass asks, so "age counts from its last successful read" should read
  "…from the refresh pass that last read it successfully", matching the module doc.
  (b) "The author's liveness shows only as … `feedVersion`" overclaims. A live author who rewrites the same
  generation does not bump `feed_version`. Say "whether its author has published a newer generation shows
  only as the policy and grant `feedVersion`". Optionally add that keys files may omit `feed_version`
  (`KeysDoc` :45-46).

Missed ledgers: none. test-inventory, review-mechanisms, owners, source-allowances, architecture-policy,
WIRE-MATRIX, scenario map and the DST README are all correctly untouched, unless D1 is taken (see C2).

**Verdict: ready-with-corrections.** C1 is a compile fix in test code. C2-C6 are wording, control and
placement fixes. The red test, the pin, the ratchet analysis, the mutation selection and the no-wire-change
analysis all hold on the current tree.
