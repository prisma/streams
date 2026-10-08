# Prisma Streams — operating manual for agents

This repository is one product: **Prisma Streams**, a massively multi-tenant
Durable Streams server on SlateDB and Tigris, run for Prisma Compute
customers with AWS-like quality, uptime and safety. This file is the single
authority on *how to work here*; the documents it links hold policy and
detail. Where private notes or older handoffs disagree with this file, this
file wins — verify against the tree, then fix whichever is wrong.

Timings below were measured on the owner's Mac (M1 Ultra, 20 cores, 64 GiB)
on 2026-09-27; CI runs on 4-vCPU `ubuntu-latest`.

## 1. Working agreement (the owner is Søren Bramer Schmidt)

- Commit and push directly on `slate`; no pull requests. One behaviour per
  commit. The harness supplies the `Co-Authored-By` trailer; end every
  message with it.
- **Red first.** A behaviour change starts with a test that fails on the
  unfixed tree; paste the exact failure text into the commit message. Assert
  exact state (ledger bytes, record counts, stored rows), not "no 500".
- **Only the owner decides** — write the options down and ask, never
  self-approve: (1) anything client-visible (an *edge change*, section 9);
  (2) any row in `docs/quality/exception-growth.json`, and any edit, rename or
  re-attachment of an `#[expect]` reason or owner to absorb growth;
  (3) anything that changes what a gate counts, selects or excludes —
  excluding code from a check is a CI bypass; (4) release and deployment
  holds (`docs/reviews/2026-09-hardening/README.md`, "Deployment gates").
- **Standing holds.** Performance, cryptographic, deployment and
  raw-evidence-upload acceptance stay with the owner. Local gates and CI do
  not lift them; never upload raw logs or evidence anywhere.
- **Code quality.** `docs/RUST-QUALITY.md` is the adopted normative policy.
  Its legacy inventories are a migration ceiling, not permission to add
  warnings, unowned effects or complexity; never regenerate or grow an
  adoption baseline in ordinary work. Keep proof-bearing fields private,
  review the whole canonical owner, and add no pass-through abstractions.
- **Product invariants.** The guarantees G1-G10 are in `SPEC.md` §4. Read
  optimisations have one permanent production path, never a runtime, env or
  Cargo switch; experiments live in isolated source revisions
  (`docs/read-experiments/final-disposition.md`). Stored formats have one
  `LAYOUT_VERSION`: no legacy decoders or aliases.
- **Evidence discipline** (each rule exists because breaking it shipped a
  red commit or a false claim):
  - Never say CI is green from memory: `scripts/dev/ci-status.sh <sha>` must
    print `CI_GREEN` for exactly the pushed commit.
  - Read a gate's verdict in its own step before committing; never chain
    `gate | grep | tail && git commit`.
  - Do not edit, commit or check out in a checkout while a gate, the suite
    or a mutation leg runs in it; run long legs in a worktree (section 2).
  - Run at most one heavy leg (gate, suite, mutations, Kani, TLC) per
    machine at a time: the DST tests are real-time and load-sensitive.
  - Read a CI failure (`gh run view <id> --log-failed`) before rerunning,
    and report every rerun.
  - Tokens and credentials are read from files, never pasted into commands,
    commits or reports.

## 2. Environment

- **Shell.** The Bash tool runs **zsh**: quote every glob
  (`rg -n 'x' -g '*.rs'`, `grep -rn x --include='*.rs'`; an unquoted glob
  that matches nothing skips the whole command), use `=` not `==` in `[ ]`,
  quote words starting with `=`. There is no `timeout`/`gtimeout`: use the
  tool's timeout or background mode. `/bin/bash` is 3.2 (scripts: under
  `set -u` write `${a[@]+"${a[@]}"}` for a possibly empty array).
- **Python.** `/usr/bin/python3` is 3.9 and lacks `tomllib`; the tools need
  3.11+. These pick a newer one themselves: the bash entry points
  (`scripts/quality.sh`, `gate.sh`, `release-gate.sh`, `test-leg.sh`,
  `quality/mutations.sh`, `quality/nightly.sh`, `install-quality-tools.sh`,
  via `scripts/lib/python.sh`) and `scripts/dev/*`. Every other
  `python3 scripts/...` call that imports `tomllib` — `quality/formal.py`,
  `quality/gate.py`, `install-formal-tools.py` — needs the prefix
  `. scripts/dev/env.sh &&` (Python 3.12, `target/quality-tools/bin` with
  `kani`, `~/.cargo/bin` on `PATH`; unsets `RUSTUP_TOOLCHAIN`). Commands below
  that need it show it.
- **Pins.** Rust 1.98.1 (`rust-toolchain.toml`); every other tool in
  `quality-tools.toml`, checked by `scripts/quality/config.py`. Install:
  `scripts/install-quality-tools.sh`,
  `. scripts/dev/env.sh && python3 scripts/install-formal-tools.py`.
  `quality.sh` refuses `RUSTFLAGS`, `RUSTC_WRAPPER` (so no sccache),
  `RUSTC`, `CLIPPY_CONF_DIR` and friends. Never set `RUSTUP_TOOLCHAIN`.
  Do not add `.cargo/config.toml`, a linker or profile tweaks: try profile
  experiments only with `CARGO_PROFILE_*` env vars in a separate
  `CARGO_TARGET_DIR`. `Cargo.toml`/`Cargo.lock` edits stale every formal
  receipt (section 6).
- **Worktrees.** `git worktree add --detach <path> HEAD`, then either
  `ln -s <main>/target <path>/target` (only while the main checkout is idle —
  two live checkouts must not build into one target) or an APFS clone
  `mkdir -p <path>/target && cp -c -R target/debug target/release <path>/target/`.
  Keep scratch files and worktrees outside the checkout (or under `target/`
  or a dot-directory): the source ratchet parses every `.rs` in every other
  directory, git-ignored or not.
- **Search.** The root `.ignore` hides immutable or historical bulk from
  `rg` (half of all hit lines for core identifiers, 81% of the output
  bytes); `rg --no-ignore-dot` includes it. Search the root or one directory
  at a time: with several path arguments ripgrep applies it intermittently.
  `git grep` ignores it. `docs/README.md` maps every document and marks the
  hidden ones.

## 3. The verification ladder

Climb only as far as the change needs; each rung repeats nothing below it.

| Rung | Command | Cost | When |
|---|---|---|---|
| Impact | `python3 scripts/dev/impact.py [paths]` | ~1 s | before and after editing: tests to run, owners, ledgers, receipts, ceilings (`-v` per file) |
| Compile | `cargo clippy --locked --workspace --all-targets -- -D warnings` | ~10 s | every edit (`--workspace` also builds the harness crates that include the by-path files, section 5) |
| Focused tests | `cargo test --locked --lib <filters>` (dev profile) | ~10 s rebuild + tests | red/green loop |
| Preflight | `scripts/dev/preflight.sh` | ~1-2 min | before the gate: ratchets, ledgers, receipts, CI selection |
| Commit gate | `scripts/gate.sh` (background) | ~5-8 min | before every commit |
| CI-selected legs | mutations, Miri, corpus, receipts (sections 6-7) | minutes-hours | when preflight/impact selects them |
| CI-only legs | see section 8 | 1-5 min each | when the change touches their surface |
| CI | `git push`, then `scripts/dev/ci-status.sh --wait` (background) | CI 14-17 min; rust-quality 12 min-4 h | every push |

- **Inner loop uses the dev profile.** A one-file edit rebuilds the dev test
  binary in ~10 s against ~142 s for `--release` (thin LTO, no incremental),
  and the whole lib suite (1,440 tests) passes in dev in the same ~110 s.
  Release is for evidence: `scripts/gate.sh`, and the timing/capacity tests.
- **Filters** are libtest substrings of *module paths*; every positional
  after `--` is OR'ed: `cargo test --locked --lib -- segmap:: shard::maintenance_tests::`.
  `--exact` applies to all of them, so do not mix it with substrings. `#[path]`
  modules can rename (`src/http/read.rs` is `http::read_adapter`). A filter
  matching nothing exits 0 with `0 passed`: gate legs prove they ran through
  `scripts/test-leg.sh <log> --exact <full::path> -- <cargo test args>`.
  DST tests are `dst::dst_tests::<file>::<test>`; `src/dst/tests/README.md`
  is an orientation map of their contracts (not gate-checked).
- **`scripts/gate.sh`** = `scripts/quality.sh` + the release lib suite
  (inventory floor) + CI's other targets (the bins' unit tests and
  `tests/pilot_membership.rs`) + the capacity test alone, so it runs every
  Rust test CI's `cargo test --release` does. Never run `quality.sh` and
  then `gate.sh`: the gate already runs it. Output: `target/gate/gate.txt`
  (or `OUT=`/first argument) plus `.suite.log`/`.targets.log`/`.capacity.log`; the terminal
  gets one line per stage and ends `GATEDONE`, or `GATEFAIL-<stage>: see
  <log>` with that log's tail. `quality.sh` alone ends `QUALITY_OK`; a failed
  step prints `QUALITY_FAIL: exit <n> at scripts/quality.sh:<line>: <command>`.
- **Capacity test** `dst::dst_tests::topology_scaling::post_split_throughput_scales`
  asserts a 1.8x ratio and fails under host load; the gate runs it alone.
  Rerun it alone on an idle host before debugging it.
- **Release gate:** `scripts/release-gate.sh` = `quality.sh` + every formal
  receipt fresh + the debug-profile suite over every target + the capacity
  test; `scripts/rc-certify.sh` and `scripts/promote-rc.sh` run it.

## 4. Waiting

- Anything over ~2 minutes runs with `run_in_background: true` (gate,
  quality, mutations, `formal_batch.py rerecord`, e2e, CI waits); the
  completion notification carries the exit code. Never `nohup`, `&` or
  `sleep N; cmd` (refused), and never poll CI in a foreground loop.
- Foreground commands die at 10 minutes (exit 143).
- CI: `scripts/dev/ci-status.sh --wait` in the background, then read its
  verdict. A push to `slate` runs CI, rust-quality and workflow-lint; later
  pushes never cancel earlier ones.

## 5. Ratchets and ledgers

The gates enforce these; plan for them before writing
(`python3 scripts/dev/impact.py` prints what applies to your files).

- **File ceilings.** A `.rs` file over 1,000 lines may not exceed its line
  count at `origin/slate` (physical lines, comments and tests included); any
  other file may grow to 1,000. Several large files (`src/shard.rs`,
  `src/http.rs`, `src/product.rs`, ...) have zero headroom: extract a module
  first. **Functions: 100 lines** (clippy `too_many_lines` under `-D warnings`,
  tests included; five parameters, nesting four); an `#[expect]` over it is a
  new exception contract the ratchet then freezes, so split instead. The
  architecture gate separately budgets 200 lines.
- **Exception contracts.** An item carrying `#[expect(...)]` may not grow:
  its contract measures `scope_lines`, `nested_items`, `syntax_facts`, and an
  `unwrap_used`/`expect_used` contract fingerprints every call and path
  spelling (import aliases resolved — moving a type behind a `use` changes
  them). Method calls are not fingerprinted. Each fingerprint counts on its
  own: removing ten uses of one path does not fund one use of another, a
  call-site fingerprint includes its argument text, and a renamed local is a
  new path (keep the old name, or change a method call instead). Put new
  logic in a new function outside the scope, reached by a method call;
  removing an exception is always allowed. Growth needs an owner-approved
  row: propose one (path, owner, scope, lint, the gate's `after` values as
  metrics, rationale); the owner adds it. A scope that already has a row is
  frozen exactly: the row records its current values, so any change to it,
  shrinking included, fails as `stale exception growth row` (today:
  `bootstrap::run`). Hook new behaviour into something it already calls.
- **By-path includes.** `src/{crypto,postings,tenant,retained_bytes,product_cursor,queue}.rs`,
  `src/quota/bucket.rs`, `src/rollup/{allocation,storage}.rs` and
  `src/application/read_{batch,budget,retention_probe}.rs` are compiled by
  `#[path]` into `tools/quality-invariants` and/or `fuzz` under module-wide
  allows: **any net code growth there is exception growth** (owner decision).
  Put new logic in a new module they do not include.
- **`docs/quality/owners.json`.** New statics, effects (`std::env::var`,
  spawns, `mem::forget`), non-expression macros (incl. `kani::cover!`),
  unresolved globs (`use super::*`) and `#[path]` modules need a reasoned row
  (the file is not sorted; add in place).
- **Mutation owners.** A new or changed non-test `.rs` under a critical prefix
  (`scripts/quality/verification_plan.py`) needs a row in
  `scripts/quality/mutation_owners.py`, whose filters must select real tests;
  otherwise CI refuses the push. Test-only files (inner `#![cfg(test)]`, or
  `proofs.rs`) are exempt.
- **Test ledgers.** After adding, renaming, moving or editing a DST test:
  `python3 scripts/test-inventory.py --write`. A new DST file (≤1,000 lines)
  also needs a `#[path]` line in `src/dst/dst_tests.rs` and a
  `by-path-module` row in `owners.json`. Tests pinned in
  `docs/refactor/review-mechanisms.json` need their `sha256` re-pinned after a
  reviewed edit (`python3 scripts/dev/impact.py --pin-hash <file.rs>::<fn>`
  prints it), then `python3 scripts/review-evidence.py --check`. Tests named
  in `docs/refactor/test-scenario-map.json` must keep their names. Add a new
  DST contract's row to `src/dst/tests/README.md`.
- **Multitenancy audit.** `bash scripts/multitenancy-audit.sh` (always bare).
  A GONE fingerprint (a moved or converted line) is fixed with `--regen` in
  the same commit, diff shown; a NEW fingerprint is converted to
  tenant-qualified types or taken to the owner — never absorbed by `--regen`.
- **Never edit**: `docs/quality/{legacy-diagnostics*,legacy-source,syntax-fragments}.json`,
  the templates under `scripts/read-experiments/followup/`,
  `docs/refactor/architecture-review-baseline.json` (hash-pinned), the review
  skill `.agents/skills/thermo-nuclear-code-quality-review/SKILL.md`.
  `RUNBOOK.md` is compiled into the binary; keep it at the root.

| Gate message | Meaning and fix |
|---|---|
| `accepted exception grew without an approved growth row` | Shrink back (new function outside the scope) or propose a row; never touch the reason. |
| `stale exception growth row` | A scope with an approved row changed (even by shrinking): restore it; only the owner updates the row. |
| `unregistered source occurrence (N): (category, path, owner, syntax)` | Add a reasoned `owners.json` row, or remove the occurrence. |
| `file growth: <path>` / architecture `FAIL` | Over the line or function ceiling: extract. |
| `N obsolete source allowances` | `. scripts/dev/env.sh && python3 scripts/quality/gate.py --clippy <json> --prune`, with the clippy JSON of the run that reported it (`target/quality/clippy.jsonl` from quality.sh, `target/preflight/clippy.jsonl` from preflight) on the current tree. |
| `new test requires inventory` / `missing test` | `python3 scripts/test-inventory.py --write`. |
| `mechanism test changed or missing` | Re-pin the reviewed test's sha256 in `review-mechanisms.json` (`impact.py --pin-hash`). |
| `TESTS_RAN_FAIL` | A filter or `--exact` name ran nothing: fix the name. |
| `FORMAL_FAIL` / `FORMAL_CHECK_FAIL` | Invalid receipt (checks changed): re-record in the same commit. |
| `FORMAL_STALE` | Inputs changed: re-record before release (section 6). |
| `register every changed critical mutation owner` | Add a `mutation_owners.py` row. |
| `MISSED` / `TIMEOUT` (mutants) | Add or sharpen a test that kills that mutant; an equivalent mutant is removed from the code (no allowlist). |

## 6. Formal verification (Kani and TLA+)

- A receipt (`verification/receipts/<ID>.json`) digests its obligation's
  source and model files; **any byte change stales it**, comments included.
  `scripts/quality/formal.py`, `Cargo.toml` and `Cargo.lock` feed every
  receipt; `build.rs` and `rust-toolchain.toml` every Kani receipt. Editing
  `verification/manifest.json`, `quality-tools.toml` or `formal.py` makes
  CI's formal job run every obligation (about 40-50 min on the slowest of six
  shards, bounded by TLA-002).
- Before editing, `python3 scripts/dev/formal_batch.py cost [paths]` shows
  which receipts a change stales and what CI will run; after,
  `. scripts/dev/env.sh && python3 scripts/quality/formal.py check` (0.2 s;
  invalid fails, stale is reported). The release gate requires `check --fresh`.
- Re-record stale receipts once per push, in the background: commit, then
  `python3 scripts/dev/formal_batch.py rerecord` (default `--stale`; or
  `--ids ...`, `--all`), then commit the receipts (or amend). It verifies the
  committed `HEAD` in its own worktree (`target/formal-worktree`, warm between
  runs), runs obligations in parallel longest first, copies back only receipts
  whose inputs still match the live checkout, and refuses when the live inputs
  differ from `HEAD`. A full re-record is ~6 h serial and about 1 h with 6-8
  jobs. `status`, `cost`, `plan` and `preflight` (every control patch still
  applies) are read-only and instant.
- **New Kani obligation:** harness in `src/<module>/proofs.rs` declared by
  exactly `#[cfg(kani)] mod proofs;` in its parent; each harness a private
  `fn` directly under `#[kani::proof]` (plus `#[kani::unwind(N)]`); an
  `owners.json` macro-dsl row per `kani::cover!` owner (count = covers);
  negative controls as `git apply` patches in `verification/kani/controls/`
  naming the assertion they must break (a Rust assert that fires first wins);
  the manifest entry; the roadmap row and status. A new obligation's
  receipt must be in the same commit (the gate fails a claimed result
  without one), so record it on the live tree before the gate, in the
  background: `. scripts/dev/env.sh && python3 scripts/quality/formal.py run
  --id <ID> --out target/formal/<id> --record` (the receipt records
  `dirty_tree: true`, which is accepted; edit none of its inputs while it
  runs). Other obligations your edit stales go through `rerecord` after the
  commit. Host harnesses in a small submodule, never directly in a file many
  TLA models list: each edit to `src/shard.rs` stales six receipts (~150 min
  serial), an edit to its own submodule only its own. Creating the submodule
  costs one `mod` line in the parent, once; for a zero-headroom parent fund
  it by moving the code under proof into the submodule (as KANI-047 did).
  Kani's traps (no `HashMap`/`HashSet`, no `anyhow`, no `format!` errors,
  concrete sizes only) are in `verification/README.md`.

## 7. Mutation testing

`plan.json` from preflight says whether CI runs mutants and for which owners.
Run the leg exactly as CI does, on a committed tree, in a worktree (the
driver lists mutants per owner from the live checkout; edits abort it), in
the background, alone on the machine:

```bash
export QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) \
       QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate
scripts/quality/mutations.sh    # zero MISSED and TIMEOUT; ~1 min per mutant locally
```

CI deals the same leg over the `mutants` job's eight matrix jobs of
360 min (`QUALITY_MUTANT_SHARD=k/8`, cargo-mutants `--shard`,
round-robin): each tests every eighth selected mutant, together each one
once. A push selecting more than about 900 mutants does not fit: split it
into smaller pushes (preflight's `plan.json` lists the selection). Locally
run it whole, or one share with the same variable. The driver stops at the first owner with a
survivor: fix it, then re-run for the owners after it.

Miri and the saved fuzz corpus: `scripts/quality/nightly.sh miri`,
`scripts/quality/nightly.sh corpus` (under a minute).

## 8. Other test layers (CI-only; run when the change touches them)

| Layer | Local command | Touching |
|---|---|---|
| Property/Loom leg | `scripts/test-leg.sh target/legs/quality.log --min 15 -- --locked --release --lib quality_` | codecs, synchronisation |
| Compiler fixtures | `python3 scripts/quality/compiler_fixtures.py --out target/fixtures-$(date +%s)` | proof-bearing types, `clippy.toml` |
| Deploy wrapper | `bun test ./deploy/supervise.test.ts ./deploy/stage-app.test.ts` | `deploy/**` |
| SDK | `cd sdk && npm ci && npx tsc --noEmit -p tsconfig.json && npm run build && npm test` | `sdk/**` |
| Platform e2e | `(cd sdk && npm ci && npm run build) && node scripts/platform-e2e.mjs && node scripts/platform-e2e-negative.mjs` | HTTP, auth, boot, lifecycle, fleet |
| Conformance | `CONFORMANCE.md` (332 pass, 6 skipped) | raw wire surface |
| LiveFeed fleet | `bash bench/fleet/livefeed-cert.sh` | SSE, fleet ownership |
| MT cert | `MT_CERT_PROJECTS=1000 scripts/test-leg.sh target/legs/mt-cert.log --exact dst::dst_tests::security_audit::shared_cell_certification_smoke -- --release --lib shared_cell_certification_smoke -- --nocapture` | tenancy, quotas |

The e2e harnesses import `sdk/dist` (git-ignored, easily stale): build the SDK
first. They use fixed ports (9500, 8090-8093, 9700-9718, 9860-9866): one at a
time. Field campaigns (`bench/`) need platform credentials, are never a gate,
and follow `RUNBOOK.md`: secrets stay outside the repo, tear down every rig
you deploy, and never stop or redeploy the always-on Tigris observatory
probes (RUNBOOK §14; ask the owner which services they are).

## 9. Commits and records

Commit message: a title that is one sentence stating the behaviour ("A usage
streamId is served only for an incarnation of the stream the URL names"),
then the why; the change and its canonical owner; red (exact failure) and
green; controls; ledgers touched (owners.json, inventory, pins, mutation
owners, mt audit); formal receipts re-recorded; `Edge change #N` or none; the
trailer.

**Edge change** (anything a client can observe): a record in
`docs/reviews/2026-09-hardening/edge-changes.md` with surface, endpoint,
condition, before, after, retry semantics, who is affected, pinning tests,
risk grade and reason; update `docs/refactor/WIRE-MATRIX.md`; the owner
ratifies. Read one earlier record with `rg -n '^### #60 ' -A 28 <file>`
rather than the whole 230 KB file.

Owner decisions: `docs/reviews/2026-09-hardening/README.md` ("Owner
decisions"), `docs/quality/exception-audit-2026-09.md`,
`docs/read-experiments/final-disposition.md`, and the PR #19 integration
record `docs/quality/pr19-merge-review.md`. Open work, in priority order:
`docs/reviews/2026-09-hardening/NEXT-WORK.md`.

## 10. Where things are

- Architecture and guarantees: `SPEC.md`. Operations: `RUNBOOK.md`,
  `OPERATIONS.md`. Every document: `docs/README.md`.
- Module map: `python3 scripts/dev/impact.py --codemap`. One crate
  (`streams-slate`, ~172k lines, 51 top-level modules; `src/dst` is tests).
- Quality policy (normative): `docs/RUST-QUALITY.md`; structural reviews use
  the pinned skill `.agents/skills/thermo-nuclear-code-quality-review/SKILL.md`.
- Formal verification: `verification/README.md`; the roadmap
  `docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md` (270 KB — read by
  anchor: `rg -n '^### KANI-047' <file>`).
- Gate inputs, edited only through their tools: `docs/refactor/*.json`,
  `docs/quality/*.json`, `scripts/mt-audit-baseline.txt`,
  `verification/receipts/`.

## 11. Known traps

- `scripts/quality/mutations.sh` passes one `--target-dir` to cargo-mutants:
  never raise its `--jobs` (parallel jobs would share one build directory).
- Every commit, checkout or rebase recompiles the whole crate (`build.rs`
  embeds the git HEAD): run gates before committing, batch commits.
- `cargo deny` fetches the advisory database: needs network, and a new
  advisory can fail an unrelated change.
- A `"sleep N; cmd"` chain, an unquoted glob, `timeout`, `==` in `[ ]` and
  heredocs rewriting source files (use the Edit tool) are the common shell
  failures.
- `pub(super)` on a field under an exception adds a `super` path fact to its
  contract: keep such fields private with accessors. A new file naming
  `crate::http` fails the architecture gate (import `AppState` via `super`).
- Clippy denies `drop()` of a `Copy` value, `result_large_err` for
  `Result<_, Response>`, and nesting deeper than four.
- `slate` is the default branch (2026-09-28), so the nightly legs run for
  it: the noisy-neighbor campaign (03:17 UTC) and rust-quality's full
  formal run, fuzzing and mutation rotation (03:43 UTC). The rotation
  deals the owners into 12 groups sized to the 360-minute job (a full cycle
  is 12 nights), runs every owner of the night's group and fails once at the
  end; a new owner needs its size measured (`python3
  scripts/quality/mutation_driver.py --measure-sizes`, NEXT-WORK "Nightly
  mutation rotation").
