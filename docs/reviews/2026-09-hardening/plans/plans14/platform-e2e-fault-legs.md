# Item 92, step A: the platform-e2e fault legs must fail when no fault happened

Tree: `slate` at HEAD 5d9d517f (origin/slate 77c13d74). `git diff origin/slate HEAD` does not touch
`scripts/platform-e2e.mjs`, `platform-demo/`, `contracts/` or `.github/`, so every line number below is
the same on both. Steps B (a refused counter in place of `sleep(2500)`) and C (a `/readyz` poll at boot) are out of scope.

**Do not start until the running mutation leg has finished.** The controls rebuild `--release`
(thin LTO) and run three cells, which is enough CPU load to time out mutants.

## 0. The harness: how it runs, and which CI job runs it

- File: `scripts/platform-e2e.mjs` (470 lines, plain ESM). Run it from the repo root with
  `node scripts/platform-e2e.mjs`. It runs `cargo build --release --bin streams-slate --bin s3lite`
  itself (line 62). It imports `../sdk/dist/index.js` (line 186), so `sdk/dist` must be built first
  (CI: `cd sdk && npm ci && npm run build`; locally `sdk/dist/index.js` already exists). It spawns the
  platform emulator `platform-demo/src/emulator.mjs --enable-fault-api` (lines 64-74), s3lite, three
  `streams-slate` cells in the full release posture (A/B/C, ports 9702/9704/9706) and the gateway. It
  prints one `ok  ` or `FAIL` line per check. The last line is `PLATFORM_E2E_OK binary=sha256:<hex>
  contract=streams-platform/v1` (exit 0) or `PLATFORM_E2E_FAIL (<n>)` on stderr (exit 1).
- CI: job `platform-e2e` in `.github/workflows/ci.yml:159-172` (`node scripts/platform-e2e.mjs`,
  timeout 30 min). It has no `if:` and no path filter, so it runs on every push to `main`/`slate`, on every PR and
  on the nightly schedule. It is also a required check for RC promotion: `scripts/promote-rc.sh:51`
  `REQUIRED_CHECKS=(rust livefeed livefeed-fleet-cert platform-e2e ...)`. A vacuous leg in this
  battery therefore reaches the RC tag.
- Nothing in `src/`, `src/dst`, `tools/`, `fuzz/`, `tests/`, `conformance/` or `sdk/` references the
  harness or the emulator (`git grep` is empty). `docs/refactor/*.json` and `docs/quality/*.json` pin neither
  file. The `owners.json` "emulator" rows are about the s3lite object emulator, not this one.

## 1. Problem (verified on the current tree)

### 1a. `fault()` discards the emulator's answer (reviewer's claim: REAL)

`scripts/platform-e2e.mjs:314-315`:

```js
const fault = (body) =>
  sfetch(`${emuBase}/admin/faults`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ cell: "cell-b", ...body }) });
```

It returns the `Response` unread. All seven call sites `await` it and drop it:

| line | call | leg's probe | probe state before the fault |
|---|---|---|---|
| 316 | `fault({ kind: "partial-write", feed: "grants" })` | 319-320 tokF `=== 200` | alive |
| 321 | `fault({ kind: "clear" })` | 323-324 tokF `401 \|\| 403` | alive (revocation pending) |
| 325 | `fault({ kind: "generation-regression", feed: "grants" })` | 327-328 tokF `!== 200` | **dead** (revoked at 317, landed per 324) |
| 329 | `fault({ kind: "same-gen-drift", feed: "grants" })` | 331-332 tokF `!== 200` | **dead** |
| 337 | `fault({ kind: "workspace-swap-no-bump", project: "proj-b", toWorkspace: "ws-hostile" })` | 339-340 tokB2 `=== 200` | alive |
| 341 | `fault({ kind: "resurrect-kid" })` | 343 tokBOld `=== 401` | **dead** (retired kid, line 271) |
| 356 | `fault({ kind: "workspace-restore", project: "proj-b" })` | 359-360 fresh token `=== 200` | n/a |

The emulator's refusal paths all return non-200 without writing anything
(`platform-demo/src/emulator.mjs`): `403 {"error":"fault API not enabled"}` (430), `404 unknown cell`
(434), `404 unknown project` (446), `409 nothing to restore` (464), `409 no previous publication to
replay` (478), `409 no current publication` (486), `400 {"error":"unknown fault kind"}` (508-509), `500`
from the catch-all (515-516), and `sfetch` maps a refused connection to status 0 (lines 37-43). Every
200 path applies its fault. So **200 ⇔ applied**.

What a refused injection does to each leg:
- **generation-regression, same-gen-drift, resurrect-kid**: the probe token is already dead, so the
  post-fault probe gives the same answer whether or not anything was injected. The legs print `ok` on a
  400, 404 or 409. This is the reviewer's finding, confirmed.
- **workspace-swap-no-bump**: the probe stays alive either way. `ok` on a 404 is vacuous too, and the
  reviewer did not name it.
- **workspace-restore**: if the swap was never injected, restore answers `409 nothing to restore`, and the
  "fresh token serves" check still passes. Vacuous together with the swap.
- **partial-write, clear**: these check themselves. Without the torn write the revocation lands and
  319 FAILs. Without the clear the grants stay torn and 324 FAILs. The new guard costs them nothing.

### 1b. The emulator's `replayed_gen`/`gen` are never read (REAL)

`emulator.mjs:480` returns `{ fault: "generation-regression", cell, replayed_gen: prev.gen }`, `:491`
returns `{ fault: "same-gen-drift", cell, gen: cur.gen }` and `:501` returns `{ fault: "resurrect-kid", cell, gen }`.
Nothing reads them. Here is `cell-b`'s grants history as the battery drives it (`project()` publishes every
feed on every call; a torn feed `continue`s before `hist` is updated, `emulator.mjs:168-177`):
310 mkCred(F) → G1 (credF active, gv 1); 317 revoke → torn, `hist` unchanged; 321 clear → G3 (credF
revoked, gv 2), so `hist = [G3, G1]`. The regression replays G1 (`replayed_gen` G1). The drift rewrites G3
(`gen` G3). If the emulator ever replayed the *current* generation, for example after a history-depth
bug, the cell would accept it as an identical republication (`src/auth/publication.rs:463-472` refuses a same-generation publication only when its digest differs). tokF would stay dead and the regression leg would still print `ok`. Only the
relation `replayed_gen < gen` exposes that.

### 1c. Found while verifying: the drift leg cannot fail even when the fault IS injected (REAL, not in the review)

The same-gen-drift fault flips status only (`emulator.mjs:488`):
```js
for (const c of doc.credentials ?? []) if (c.status === "revoked") c.status = "active";
```
Revocation bumped credF to grant_version 2, and tokF was minted at gv 1. `src/auth.rs:631-643` checks status and
then EXACT grant-version equality:
```rust
if cred.status != CredentialStatus::Active { return Err(AuthError::CredentialNotActive(cred.status)); }
...
if c.grant_version != cred.grant_version { return Err(AuthError::GrantVersionMismatch); }
```
Suppose a cell ACCEPTED the drifted snapshot. It would then refuse tokF with `GrantVersionMismatch` →
`Refusal::Unverified` → **401** (`src/auth/refusal.rs:59`, `src/product.rs:689-693`), and the
`!== 200` probe prints `ok`. So the leg that claims the §14.5 requirement "same generation with
different digest refused" (`docs/CONTROL-PLANE-INTEGRATION.md:832`) cannot tell acceptance from refusal.
Step A as the reviewer wrote it (prove the fault was injected) does not reach this. The probe also
cannot observe a crash: `!== 200` accepts status 0 from a cell that died reading the injected file. The same
`!== 200` guards the regression leg (327-328).

Today a revoked tokF answers **403** (`CredentialNotActive` → `Refusal::Denied(Credential)`, `src/product.rs:678`).

### 1d. The reviewer's other locations (B/C, not planned here)

- `scripts/platform-e2e.mjs:135-139`: `process.on("exit", kill); await sleep(2500);` plus the three
  boot checks. This is step C.
- `src/auth_feed.rs:292-303`: `RefreshOutcome { Published, Refused, Unavailable, TimedOut }` and
  `RefreshReport`. Refusals are only logged (`warn` at `:321`, `debug` pass summary at `:410-411`) and never counted.
  This is step B.
- "sfetch maps connection errors to status 0 which rfetch never retries" is real: `rfetch` retries only
  `503/429` (line 55). That belongs to step C.

## 2. Contract decision

- There is no product, raw or wire edge change and no Rust change. `/admin/faults` is `x-test-only` (`contracts/streams-platform/v1/management.openapi.yaml:166-178`)
  and its response shapes do not change. The openapi description ("same-gen-drift (same feed_version, changed
  content)") stays accurate and is not edited.
- The battery's contract becomes: a fault leg is proven only when (1) the emulator applied the fault (200),
  (2) the regression replayed a generation strictly below the one the cell holds, and (3) the probe then
  answers exactly as it did before the fault. The drift fault's document changes so that an ACCEPTED drift
  would revive the probe token. That makes the drift leg observable by construction and independent of
  the order of the cell's checks.
- Two commits. Commit 1 is the reviewer's step A. Commit 2 closes §1c. It goes beyond the review's
  step A, but without it the drift leg still cannot fail. It can be dropped separately if the
  orchestrator wants step A exactly as the reviewer wrote it.

## 3. Pinning checks and non-vacuity controls

This is a test-tool change, so there is no Rust red test. The pinned checks are the battery's existing
lines, and they keep their names:
`torn feed file never becomes visible: revocation does NOT land, old snapshot serves`,
`clean republication lands the pending revocation`,
`generation regression refused: revoked grant does not resurrect`,
`same-generation content drift refused: revoked grant still dead`,
`workspace change without ownership bump refused: old-workspace token still serves`,
`retired-kid resurrection refused: old-kid token stays dead`,
`refused snapshot does not clobber good state: current-kid token still serves`,
`restored ownership tuple lands: freshly minted token serves on cell B`.

New checks (commit 1), 8 lines:
`fault partial-write injected`, `fault clear injected`, `fault generation-regression injected`,
`fault same-gen-drift injected`, `fault workspace-swap-no-bump injected`, `fault resurrect-kid injected`,
`fault workspace-restore injected`, `regression replayed a generation below the one drift reuses`.
The executed check count goes from 54 to **62**. There are 56 `check(` call sites; 194/196 and 201/203 are try/catch
alternates.

Output format (line 31): `${ok ? "ok  " : "FAIL"} ${name} ${ok ? "" : extra}`.

Each control below is a temporary edit, reverted and confirmed with `git diff --quiet` before the next step. Exact
commands are in §7.

| id | temporary edit | tree | expected (the named lines only; everything else unchanged) |
|---|---|---|---|
| C1-before | emulator: rename `case "generation-regression":` and `case "same-gen-drift":` (append `-off`) | HEAD | `ok   generation regression refused: …`, `ok   same-generation content drift refused: …`, `PLATFORM_E2E_OK …`: **the vacuity** |
| C1-after | same | +commit 1 | `FAIL fault generation-regression injected status 400 body {"error":"unknown fault kind"}`, `FAIL fault same-gen-drift injected status 400 body {"error":"unknown fault kind"}`, `FAIL regression replayed a generation below the one drift reuses replayed_gen undefined gen undefined`, `PLATFORM_E2E_FAIL (3)`, exit 1 |
| C1b-before | emulator regression replays `hist[…]?.[0]` (the current generation) instead of `?.[1]` | HEAD | all `ok`, `PLATFORM_E2E_OK`: a no-op regression passes today |
| C1b-after | same | +commit 1 | `ok   fault generation-regression injected`, `ok   generation regression refused: …` (an identical replay is accepted and tokF stays 403), `FAIL regression replayed a generation below the one drift reuses replayed_gen <N> gen <N>` (equal N), `PLATFORM_E2E_FAIL (1)` |
| C2-before | `src/auth/publication.rs` `publish_grants` accepts any snapshot when `PE2E_SABOTAGE_ACCEPT_GRANTS` is set; run with it set | +commit 1 | `FAIL generation regression refused: revoked grant does not resurrect ` (tokF 200), `ok   same-generation content drift refused: revoked grant still dead` (**accepted drift unseen**: tokF 401 grant_version_mismatch), `PLATFORM_E2E_FAIL (1)` |
| C2-after | same | +commit 2 | `FAIL generation regression refused: revoked grant does not resurrect status 200 (403 before the fault)`, `FAIL same-generation content drift refused: revoked grant still dead status 200 (403 before the fault)`, `PLATFORM_E2E_FAIL (2)`. "status 200" on the drift line shows the emulator change: with the old drift, an accepted drift gives 401, not 200 |
| C3-before | harness: `cellB.kill("SIGKILL");` right after the regression `fault(...)` | +commit 1 | `ok   generation regression refused: …` and `ok   same-generation content drift refused: …` (status 0 passes `!== 200`), then unrelated cascades (swap/resurrect/restore/quota-B/usage FAIL) |
| C3-after | same | +commit 2 | `FAIL generation regression refused: revoked grant does not resurrect status 0 (403 before the fault)`, `FAIL same-generation content drift refused: revoked grant still dead status 0 (403 before the fault)`, same cascades |
| green | none | +commit 2, sabotage reverted | 62 `ok  ` lines, 0 `FAIL`, `PLATFORM_E2E_OK binary=sha256:<hex> contract=streams-platform/v1`, exit 0. Cell logs contain `grant feed_version regressed` and `same grant generation with different content` (warn from `src/auth_feed.rs:321`), naming the defenses that fired |

## 4. Edits in commit order

No ceilinged file is touched. The 1,000-line growth rule covers `.rs` files only (`docs/RUST-QUALITY.md:63`,
`scripts/quality/source_rules.py:226`). `platform-e2e.mjs` goes from 470 to about 495 and `emulator.mjs` from 533
to about 537. No `#[expect]` scope, owner row or allowance is touched. The C2 sabotage edit to
`src/auth/publication.rs` is never staged.

### Commit 1: "A platform-e2e fault leg fails when the emulator did not apply the fault"

`scripts/platform-e2e.mjs`

1. Replace lines 314-315 with:
```js
// The emulator answers 200 only once the fault is applied. A refused
// request (400 unknown kind, 404 unknown project, 409 nothing to
// replay, status 0 unreachable) leaves the accepted snapshot in place,
// and the leg's probe then reads exactly what a refusing cell serves:
// the leg would pass with the defense never exercised.
const fault = async (body) => {
  const r = await j(await sfetch(`${emuBase}/admin/faults`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ cell: "cell-b", ...body }),
  }));
  check(`fault ${body.kind} injected`, r.status === 200, `status ${r.status} body ${JSON.stringify(r.body)}`);
  return r.body;
};
```
   (`j`, line 44, already turns a non-JSON or status-0 body into `{}`. The seven `await fault(...)` sites keep
   their form.)
2. Line 325: `await fault({ kind: "generation-regression", feed: "grants" });` →
   `const regressed = await fault({ kind: "generation-regression", feed: "grants" });`
3. Line 329: `await fault({ kind: "same-gen-drift", feed: "grants" });` →
```js
const drifted = await fault({ kind: "same-gen-drift", feed: "grants" });
// Drift reuses the generation the cell holds. A regression that
// replayed that generation is an identical republication, which the
// cell accepts by design, so the leg above would pass without the
// feed_version-regression defense running.
check("regression replayed a generation below the one drift reuses",
  drifted.gen > regressed.replayed_gen, `replayed_gen ${regressed.replayed_gen} gen ${drifted.gen}`);
```
   (There is deliberately no `Number.isInteger` guard. A missing field is `undefined`, and `undefined > n` or
   `n > undefined` is false, so the plain comparison already fails. The emulator never returns null.)

`platform-demo/README.md`
4. Line 38: `— 45 checks:` → `— 62 checks:`. It was already stale: HEAD executes 54.
5. Feed-faults bullet (59-62): append the sentence "Each fault leg first requires the emulator to have
   applied the fault, so a refused injection fails the battery instead of passing the leg."

Commit body: the §1a/§1b reasons, then
`Red on the previous tree (emulator generation-regression and same-gen-drift cases renamed): both legs printed ok and the battery printed PLATFORM_E2E_OK; now FAIL fault generation-regression injected status 400 …, PLATFORM_E2E_FAIL (3).`
Add the C1b line the same way, then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

### Commit 2: "The regression and drift probes fail when the cell accepts or dies on the injected file"

`platform-demo/src/emulator.mjs`, case `same-gen-drift` (482-491):
```js
        case "same-gen-drift": {
          // Same feed_version, different content: every revoked grant
          // back to active at the grant_version it was revoked from
          // (revocation bumps it by one). A status flip alone keeps the
          // bumped version, which no token minted before the revocation
          // carries, so a cell that ACCEPTED the drift would still refuse
          // the battery's probe and the leg could not tell.
          const cur = hist[body.feed ?? "grants"]?.[0];
          if (!cur) return json(res, 409, { error: "no current publication" });
          const doc = JSON.parse(cur.body);
          for (const c of doc.credentials ?? [])
            if (c.status === "revoked") Object.assign(c, { status: "active", grant_version: c.grant_version - 1 });
          for (const p of doc.projects ?? []) p.status = "active";
          atomicWrite(join(cell.dir, FEED_FILES[body.feed ?? "grants"]), JSON.stringify(doc));
          return json(res, 200, { fault: "same-gen-drift", cell: cellId, gen: cur.gen });
        }
```
The emulator increments grant_version by exactly one on both revocation paths: `revoke` (`:308`, `cred.grant_version += 1`) and `revokeAllCredentials` (`c.grant_version += 1`). A revoked grant is therefore
always at gv ≥ 2, and gv-1 ≥ 1. The only caller is the battery (`git grep admin/faults`). The bench scripts pass
`--enable-fault-api` for `/admin/mint-workload` only.
The cell still refuses on the feed digest first (`src/auth/publication.rs:466-472`, "same grant generation with different content"),
which is the §14.5 defense.

`scripts/platform-e2e.mjs`: the regression probe (orig 326-328) and the drift probe (orig 330-332) become:
```js
await sleep(2500);
// A refused snapshot leaves the probe answering exactly as it did
// before the fault; "not 200" also passes a cell the file crashed
// (status 0) or one that took the snapshot and refused the token for
// another reason.
const afterRegression = (await readRecords(bBase, "e2e/orders", tokF.body.accessToken)).status;
check("generation regression refused: revoked grant does not resurrect",
  afterRegression === afterClear.status, `status ${afterRegression} (${afterClear.status} before the fault)`);
```
and, without repeating the comment,
```js
await sleep(2500);
const afterDrift = (await readRecords(bBase, "e2e/orders", tokF.body.accessToken)).status;
check("same-generation content drift refused: revoked grant still dead",
  afterDrift === afterClear.status, `status ${afterDrift} (${afterClear.status} before the fault)`);
```
(`afterClear` is the line-323 `Response`. It is in scope and holds tokF's pre-fault refusal, 403 today.)

Commit body: the §1c reasons, then C2 and C3 before/after lines as "Red on the previous tree", then the co-author line.

## 5. Mutation analysis

- There are no `.rs` hunks. `scripts/quality/verification_plan.py:plan` keeps only `.rs` paths (line 74), so for
  these two commits `mutants=false`, `properties_fuzz=false` and `miri=false`. `scripts/quality/test_verification_plan.py:158`
  pins the README-only case. `cargo-mutants --in-diff` has nothing to select. The push still carries the 15
  unpushed Rust commits, whose own mutation leg is the one currently running. These commits add nothing to it.
- JS predicates are not mutation-tested, so each new predicate has a hand "mutant" and the control that kills it:
  - `r.status === 200` → `true`: killed by C1-after.
  - `drifted.gen > regressed.replayed_gen` → `>=`: killed by C1b-after (equal generations).
  - `afterX === afterClear.status` → back to `!== 200`: killed by C3-after (status 0).
  - emulator `grant_version: c.grant_version - 1` removed: C2-after's drift line reads `status 401 (403 before the
    fault)` instead of `status 200`. That still FAILs, because exactness catches it. The two changes overlap on
    purpose: exactness does not depend on how the drift is built, and the gv-1 drift does not depend on
    `verify_customer`'s check order. If `auth.rs:631` and `:641` were swapped, a revoked tokF would answer 401 both before and after an accepted old-style drift.
    The distinct extra text (`200` vs `401`) is the control for the emulator change.
- Rust sabotage (C2) is a local control only and is never committed.

## 6. Ledgers

None needed. Each was checked:
`docs/refactor/test-inventory.json` (no `src/dst` change), `docs/refactor/review-mechanisms.json` (pins
`sdk/scripts/*.test.mjs` only; neither file here is pinned), `docs/quality/owners.json` and
`source-allowances.json` (no Rust), `docs/refactor/architecture-policy.json`, `test-scenario-map.json` and
`scenario-dispositions.json` (battery check names are not mapped; no Rust test renamed or deleted),
`src/dst/tests/README.md` (untouched). The only docs edit is `platform-demo/README.md`, in commit 1.
`docs/CONTROL-PLANE-INTEGRATION.md` §14.5 and the openapi description stay accurate. The drift leg now
actually proves "same generation with different digest refused".

## 7. Controls: exact commands, run after the mutation leg has finished

`R=/Users/sorenschmidt/code/streams`,
`S=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/plans14`.
Every run: `cd $R && env -u RUST_LOG node scripts/platform-e2e.mjs > $S/<id>.log 2>&1; echo "exit=$?"`,
then `grep -E '^(ok  |FAIL) ' $S/<id>.log` and `tail -1 $S/<id>.log`. Check first that ports 9700-9718 are free:
`lsof -iTCP:9700-9718 -sTCP:LISTEN` should print nothing. After every temporary edit is reverted,
`git diff --quiet -- <file> && echo clean` must print `clean`.

Temporary edits (all reversible):
- rename: `perl -pi -e 's/case "(generation-regression|same-gen-drift)": \{/case "$1-off": {/' platform-demo/src/emulator.mjs`.
  Revert with `git checkout -- platform-demo/src/emulator.mjs` (only while that file has no uncommitted work).
- hist0: `perl -pi -e 's/\?\.\[1\];/?.[0];/ if /const prev = hist/' platform-demo/src/emulator.mjs`. Revert the same way.
- sabotage: `perl -0pi -e 's/(pub\(crate\) fn publish_grants\(&self, snapshot: GrantSnapshot\) -> Result<\(\), &.static str> \{\n)/$1        if std::env::var_os("PE2E_SABOTAGE_ACCEPT_GRANTS").is_some() { self.credentials.store(Arc::new(snapshot)); self.published(); return Ok(()); }\n/' src/auth/publication.rs`,
  then check that `git diff --stat src/auth/publication.rs` shows exactly `1 insertion(+)`. Run with `PE2E_SABOTAGE_ACCEPT_GRANTS=1`.
  `cellEnv` spreads `process.env`, so the cells inherit it. Revert with `git checkout -- src/auth/publication.rs`.
- killB: `perl -pi -e 's/^(const regressed = await fault\(\{ kind: "generation-regression", feed: "grants" \}\);)$/$1 cellB.kill("SIGKILL");/' scripts/platform-e2e.mjs`.
  Revert with `git checkout -- scripts/platform-e2e.mjs`, only once that file's commit-2 edits are committed.

Order:
1. `node --check scripts/platform-e2e.mjs && node --check platform-demo/src/emulator.mjs` → no output.
2. C1-before (rename, HEAD) → table row. Revert. C1b-before (hist0, HEAD) → row. Revert.
3. Make the commit-1 edits. `node --check` both. Green run → 62 `ok`, 0 `FAIL`, `PLATFORM_E2E_OK`.
   `git add scripts/platform-e2e.mjs platform-demo/README.md`, then commit.
4. C1-after (rename) → `PLATFORM_E2E_FAIL (3)` plus the three FAIL lines. Revert. C1b-after (hist0) → `PLATFORM_E2E_FAIL (1)`. Revert.
5. Apply the sabotage. C2-before with `PE2E_SABOTAGE_ACCEPT_GRANTS=1` → `PLATFORM_E2E_FAIL (1)`, with the drift line `ok`. This build
   rebuilds release with the sabotage. C3-before (killB, env unset, same binary): both probe lines `ok`. Revert killB.
6. Make the commit-2 edits. `node --check` both. `git add scripts/platform-e2e.mjs platform-demo/src/emulator.mjs`.
   `git diff --cached --name-only` must list exactly those two files, then commit.
7. C2-after (env set) → `PLATFORM_E2E_FAIL (2)`, both lines `status 200 (403 before the fault)`. C3-after (killB,
   env unset) → both lines `status 0 (403 before the fault)`. Revert killB.
8. Revert the sabotage. `git status --short` is empty. Green run, which rebuilds clean → 62 `ok`, 0 `FAIL`,
   `PLATFORM_E2E_OK binary=sha256:<hex> contract=streams-platform/v1`, exit 0.
   `grep -c "grant feed_version regressed" $S/green.log` ≥ 1 and
   `grep -c "same grant generation with different content" $S/green.log` ≥ 1.
9. Before pushing: `git diff --name-only origin/slate..HEAD -- '*.rs'` lists only the 15 prior commits'
   files, and `git show --stat HEAD~1 HEAD` shows no `.rs`. After pushing, confirm with `gh run view` that the
   `platform-e2e` job on the pushed SHA is green. Do not claim it green from a local run.

## 8. Out of scope

- Step B: a per-feed refused counter in `feed_json`, exposed on `/v1/debug/auth`, polled in place of the
  `sleep(2500)`s. Note for B's planner: `/v1/debug/*` mounts behind `require_deployment_bearer`
  (`src/http/debug.rs:24-46`), and the battery's release-posture cells set no bearer.
- Step C: a bounded `/readyz` poll in place of the boot `sleep(2500)` (line 137), and `rfetch` not retrying status 0.
- The `401 || 403` looseness at line 324. It is not vacuous, since the revocation leg fails if nothing lands.
- The unused `freeze` fault kind, fault legs on cells other than B, and the other fixed sleeps.

## 9. Decisions for Søren

None. No product, raw or wire edge changes. The emulator fault surface is `x-test-only`, and its response
shapes are unchanged.

## Skeptic corrections (C1..C5)

Checked against the tree at 5d9d517f. `git diff origin/slate HEAD` touches none of `scripts/platform-e2e.mjs`,
`platform-demo/`, `contracts/`, `.github/`. Confirmed as written: harness location and run command, CI job
`.github/workflows/ci.yml:159-172` (no `if:`, triggers at `:3-11`), `scripts/promote-rc.sh:51`, the seven
`fault(` sites (314-356), every emulator refusal path (425-516), the 200 ⇔ applied claim, the `hist` depth-2
history (`emulator.mjs:180`), 56 `check(` sites and 54 executed (194/196 and 201/203 are alternates, no check
runs in a loop), README "45 checks" at `platform-demo/README.md:38`, 403 for a revoked tokF
(`refusal.rs:38` → `product.rs:678`), 401 for GrantVersionMismatch (`refusal.rs:59`), `verification_plan.py:74`
(`.rs` only) and `test_verification_plan.py:158`. No ledger pins either file. The sabotage perl matches
`publication.rs:446` exactly, and `Arc` is imported at `:7`. The `refused` warn at `auth_feed.rs:321` prints `%why`, and
`main.rs:12-16` defaults to `info,slatedb=warn`, so the step-8 log greps see it. The C1/C1b/C2 expected
outputs and FAIL counts hold on the stated trees.

**C1: the drift leg still cannot tell whether the digest defense is present. Reword §1c, §2, §6 and the commit-2 texts.**
`publish_grants` refuses the drifted snapshot at three independent layers, and each layer alone is enough:
(1) the generation digest, `src/auth/publication.rs:465-472` ("same grant generation with different content");
(2) the per-ID high-water `CredHw::check`, `:173-189`: the old drift hits `:177-179` ("revoked credential reactivated
without a newer grant_version"), and the commit-2 gv-1 drift hits `:174-176` ("grant_version below high-water");
(3) `check_credentials_transition`, `:215-225`. If layer 1 were deleted, the drift leg would stay green before commit 2 and after it.
Commit 2 therefore makes an *accepted* drift observable, which is real and worth keeping. It does not make the leg prove
§14.5 "same generation with different digest refused" (`docs/CONTROL-PLANE-INTEGRATION.md:832`). The gv-1 rewrite also
turns the drift into a per-ID grant_version regression as well, so the fault is less pure than before. Required:
drop the §6 sentence "The drift leg now actually proves 'same generation with different digest refused'". Word
the commit-2 comment and body as "an accepted drift revives the probe, so acceptance is observable". State
that the step-8 log grep shows only that the digest layer runs FIRST in this build, and that CI asserts nothing
about it (cell stderr is only inherited into the job log). Only a drift that per-ID checks cannot see isolates the digest layer:
an entry ADDED under the published generation (the SR3-3 rationale at `publication.rs:76-79`), for example a credential
minted while cell B is under the existing but unused `freeze` fault (`emulator.mjs:472-474`) and injected by the drift.
List it as a follow-up for Søren. It is not part of step A.

**C2: the regression leg has the same layering. Do not claim it proves the feed_version check.** Replaying G_N (credF
active at gv 1) is refused at `publication.rs:449-451`. With that check removed, `CredHw::check` `:174-176` would still
refuse it (v_hw 2 > 1), and so would `check_credentials_transition` `:215-216`. §1b's point stands: only
`replayed_gen < gen` catches an emulator that replays the current generation. The commit-1/commit-2 bodies and the README
sentence must not say the leg "proves the feed_version-regression defense". They may say "proves a regressed snapshot is
refused and the prior snapshot keeps serving". The same applies to the commit-1 inline comment "so the leg above would pass
without the feed_version-regression defense running". Reword it to "without any regressed snapshot reaching the cell".

**C3: C3 control expectations are incomplete. State exact counts.** After `cellB.kill("SIGKILL")`, `sfetch`
returns status 0 on the direct `bBase` probes, and the gateway answers 502 (`platform-demo/src/gateway.mjs:97`), which rfetch
does not retry. On the commit-1 tree the cascade is exactly 7 FAIL lines: swap (339), resurrect `=== 401` (343),
"refused snapshot does not clobber" (344), restore (359), cell-B quota `=== 429` (407), usage rollup (430), and
foreign-usage `=== 404` (434, direct to bBase). The plan's list leaves out 344 and 434. C3-before is therefore `PLATFORM_E2E_FAIL (7)`
with both probe lines `ok`. C3-after is `PLATFORM_E2E_FAIL (9)` with both probe lines `status 0 (403 before the fault)`. All seven
`fault … injected` lines and the relation line stay `ok` because the emulator is alive. The usage loop adds about 22s, which is harmless.

**C4: the reviewer's "asserting replayed_gen/gen" also names the `gen` in resurrect-kid's response (`emulator.mjs:501`),
and the plan does not assert it. Say why in §1b.** That `gen` is `gen.keys + 1` by construction (`:497`). The harness sees no keys
generation to compare it with, and the probe token (`tokBOld`) is dead either way. The leg is covered by the 200 guard,
and by the KidHw retired check alone only if the generation is newer. A stale-generation resurrect would be refused
as a jwks regression (`publication.rs:321-323`) and the leg would still pass. Either accept this with that sentence in the plan,
or out of scope ask for the emulator to return the prior keys generation. Do not silently skip it.

**C5 (minor citation fixes):** the §1b and commit-2 text cite the digest check as `publication.rs:463-472` and `:466-472`. It is
`:465-472` (the `let digest` block starts at 456). `docs/RUST-QUALITY.md:63` holds the File-growth row, and `source_rules.py:226`
is the `limit = max(1000, …)` line, both fine. The openapi `/admin/faults` lists only 200/403 responses
(`management.openapi.yaml:178`) while the emulator also returns 400/404/409/500. That is pre-existing, `x-test-only` and not edited, so no action.

Ledgers: none missed. The diff has no `.rs` files and no `src/dst` change. `review-mechanisms.json` pins only `sdk/scripts/*.test.mjs`. No
scenario map or disposition names a battery check. `docs/refactor/BASELINE.md:73` and `CONTROL-PLANE-INTEGRATION.md:775,876`
describe the harness generically and stay accurate. Nothing in the CI mutation leg selects these commits.

Verdict: **ready-with-corrections**. Commit 1 (step A) is sound as written, apart from the C2 comment wording. Its controls are
buildable and predict the right output. Commit 2 is a correct improvement in observability. Land it only with the C1 rewording, the
C3 counts, and C4 answered in the plan.
