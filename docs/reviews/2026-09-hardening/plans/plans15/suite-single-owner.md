# Item 69: one owner for the Rust suite, and a push ratchet that compares with something

Tree: slate @ c397e9e5 (origin/slate = 5d9d517f, 8 unpushed commits for items 77/76/80/92; none of them
touches `scripts/`, `.github/` or `docs/quality`, checked with `git diff --stat origin/slate HEAD -- .github scripts docs/quality`).
Reviewer text: robustness-maintainability-review.md lines 1436-1446.

Two commits, no Rust production change, no ceilinged file touched, no product/raw edge change.

---

## 1. Problem (verified on the current tree)

### 1a. Four copies of the suite invocation, three of the capacity leg, three of the floor

The review names three copies; there is a fourth (`release-provenance.sh`).

| Site | Exact text | Targets | Profile | `--locked` | Capacity skipped | tests-ran floor |
|---|---|---|---|---|---|---|
| `scripts/gate.sh:17` | `if ! cargo test --locked --release --lib -- --skip post_split_throughput_scales > "$OUT.suite.log" 2>&1; then` | **lib only** | release | yes | yes | yes, `gate.sh:23-24` (`--inventory docs/refactor/test-inventory.json --skipped 1`) |
| `scripts/release-gate.sh:78-80` | `bash scripts/test-leg.sh target/release-gate/suite.log \`<br>`  --inventory docs/refactor/test-inventory.json --skipped 1 \`<br>`  -- --lib -- --skip post_split_throughput_scales` | **lib only** | **dev** | **no** | yes | yes (via `test-leg.sh`) |
| `.github/workflows/ci.yml:104` | `cargo test --release -- --skip post_split_throughput_scales 2>&1 \| tee /tmp/suite.log` | all | release | **no** | yes | yes, `ci.yml:113` |
| `scripts/release-provenance.sh:34-35` (only with `RUN_SUITE=1`) | `cargo test --release 2>&1 \| grep -E '^test result' \| \`<br>`    awk '{p+=$4; f+=$6} END {...}'` | all | release | **no** | **no** (capacity runs inside the parallel suite) | **no** |

Capacity leg copies:

- `scripts/gate.sh:34-35`: `cargo test --locked --release --lib post_split_throughput_scales -- \` / `--exact dst::dst_tests::topology_scaling::post_split_throughput_scales`, verdict at `gate.sh:40-41` (`tests_ran.py ... --exact ...`).
- `scripts/release-gate.sh:83-84`: `bash scripts/test-leg.sh target/release-gate/capacity.log --exact dst::...::post_split_throughput_scales \` / `-- --lib post_split_throughput_scales -- --exact dst::...::post_split_throughput_scales` (dev profile, no `--locked`).
- `.github/workflows/ci.yml:120`: `run: scripts/test-leg.sh target/legs/capacity.log --exact dst::...::post_split_throughput_scales -- --release --lib post_split_throughput_scales -- --exact dst::...` (no `--locked`).

Evidence that the copies have to move together: `docs/review-tests-evidence.md:46-47` "Exact capacity selectors in CI, `scripts/gate.sh`, and `scripts/release-gate.sh` were updated together."

**What `--lib` drops (gate.sh, release-gate.sh).** `cargo test` without target selection runs the lib, the 9 bins (8 declared `[[bin]]`s plus the auto-discovered `src/bin/verify.rs`; `src/bin/{pilot,bench,s3lite}/` have no `main.rs`), `tests/pilot_membership.rs` and the lib doctests (all fenced `text`, 0 tests). Test attributes that only exist outside the lib (`grep -rnE '^\s*#\[(tokio::)?test\b' src/bin tests`): 66, not the reviewer's 62:

```
  7 src/bin/bench/tests.rs            16 src/bin/pilot/generator/tests.rs
 16 src/bin/pilot/benchmark/tests.rs   7 src/bin/pilot/proxy/tests.rs
  1 src/bin/pilot/benchmark/window.rs  6 src/bin/s3lite/operation_tests.rs
  3 src/bin/pilot/client.rs            4 src/bin/s3lite/range_tests.rs
                                       4 src/bin/s3lite/tests.rs
                                       2 tests/pilot_membership.rs
```

They include both pilot Loom models (`quality_loom_completion_and_freeze_use_the_actual_window_transitions` in `src/bin/pilot/benchmark/tests.rs:477`, `quality_loom_final_membership_acquires_all_accounting_and_prevents_late_work` in `tests/pilot_membership.rs:12`), which `docs/RUST-QUALITY.md:146-149` says run "on every change" through "ci.yml's full `cargo test --release`". The last local gate log confirms the lib-only scope: `gate-b11.txt.suite.log` has exactly one `Running unittests src/lib.rs` line and `gate-b11.txt` has two `test result` lines (suite 1261 passed, capacity 1). `AGENTS.md:9` calls `scripts/gate.sh` "the full local commit gate".

**`release-gate.sh` dev profile.** Never deliberate: it has been `cargo test --bin streams-slate` (dev) since 8e8d77bb (2026-07-21) and became `--lib` at the lib/bin split.

**Empty livefeed matrix step.** `release-gate.sh:86-87`:

```sh
echo "== livefeed engine matrix =="
echo "== supply chain =="
```

90526742 ("DELETE the legacy engine") removed the `STREAMS_SSE_ENGINE=livefeed cargo test ...` leg and its commit message says "release-gate's matrix step is gone", but the heading stayed. The header comment (`release-gate.sh:4-10`, "R30 review: this scope statement must match what the script runs") still describes the removed step.

**Stale part of the reviewer's claim.** The review says release-gate.sh runs "without ... summary check". That is **not real any more**. 00ff0e7e (2026-09-22, "A gate leg proves it ran the tests it names") routed both release-gate legs through `scripts/test-leg.sh`, which runs `scripts/quality/tests_ran.py` (the reviewer's "rank-18 checker"). Nothing is planned for it. The dev profile, the missing `--locked` and the lib-only scope are still real.

Callers of the consumers: `release-gate.sh` is called by `scripts/rc-certify.sh:53` and `scripts/promote-rc.sh:88`. `gate.sh` is run by hand (`AGENTS.md:9`, `docs/quality/adoption.md:101`). `release-provenance.sh` is run by hand (`docs/OPS-RELEASE.md:183`, `README.md:140`).

Other occurrences, all non-executable:
- `src/dst/tests/topology_scaling.rs:296` is the definition.
- `src/dst/tests/fixture_failpoints.rs:8-11` is a doc comment naming "release-provenance.sh" as an unskipped run. It goes stale with this change and is fixed below.
- `src/dst/tests/README.md:43-47` stays true.
- `scripts/quality/test_tests_ran.py:7,31` are fixtures.
- Historical docs: `docs/refactor/*.json`, `BASELINE.md`, `review-verification-evidence.md:166`, `codereview1.md`.

`tools/`, `fuzz/` and `bench/` have none (`git ls-files -- '*.sh' '*.yml' '*.yaml' | xargs grep -lE 'cargo +test|test-leg\.sh|post_split_throughput_scales'` lists only ci.yml, rust-quality.yml, gate.sh, quality.sh, release-gate.sh, release-provenance.sh and test-leg.sh). Filtered legs that are not suite copies and stay: ci.yml:89 (mt-lint), :132 (`sse::`), :134 (`livefeed_`), :212 (mt-cert), rust-quality.yml:48 (`quality_`), quality.sh:14 (`-p streams-quality-syntax`), :43-44 (mt-lint).

### 1b. ci.yml `architecture-report` compares HEAD with HEAD on push: real

`.github/workflows/ci.yml:43-48`:

```yaml
      - name: Architecture budgets and owner boundaries (hard gate)
        env:
          QUALITY_BASE_REF: origin/${{ github.base_ref || github.ref_name }}
        run: |
          python3 scripts/architecture-gate.py --self-test
          python3 scripts/architecture-gate.py --check
```

`architecture-gate.py:216-217` calls `source_gate.check()`, which at `source_gate.py:27` does `base = merge_base()`. `scripts/quality/common.py:20-29`:

```python
    target = os.environ.get('QUALITY_BASE_REF') or 'origin/slate'
    before = os.environ.get('QUALITY_BEFORE_SHA', '')
    event = os.environ.get('QUALITY_EVENT_NAME') or os.environ.get('GITHUB_EVENT_NAME', '')
    if event == 'push' and before and set(before) != {'0'}:
        target = before
    return git('merge-base', 'HEAD', target)
```

On a push, the runner sets `GITHUB_EVENT_NAME=push`, but this job has no `QUALITY_BEFORE_SHA`. So `target = origin/slate`. After `actions/checkout@v4` with `fetch-depth: 0`, that ref is the pushed commit or a descendant of it, so `merge-base` returns HEAD. Four comparisons become HEAD against itself:
- `removed source debt returned` (prior allowances are read at HEAD),
- the per-file no-growth ceiling (`prior_lines` are HEAD's own lines),
- `exception_growth`,
- the `.rs` line comparisons.

The absolute legacy ceilings still apply. On the same push, `rust-quality.yml`'s `quality` job runs the same check correctly, because its workflow-level env (lines 16-20) sets `QUALITY_BEFORE_SHA: ${{ github.event.before }}`. So the enforcement gap is zero, but a green `architecture-report` on a push certifies nothing about growth.

`merge_base()` use sites: `scripts/quality/gate.py:37` and `source_gate.py:27`. `test_source_gate.py:44,124` patch it. The source ratchet runs from:
- quality.sh:25 (gate.py) and :37-39 (architecture-gate `--check`), with env from rust-quality.yml;
- release-gate.sh:17 (local, no event);
- ci.yml:48 (the vacuous one).

`verification_comparison()` (common.py:51-104) already fails closed on a push without `QUALITY_BEFORE_SHA` (line 83). Only `merge_base()` is silent.

### Required checks (never weakened)

`scripts/promote-rc.sh:51-53`: `REQUIRED_CHECKS=(rust livefeed livefeed-fleet-cert platform-e2e mt-cert-1000 durable-streams-server-conformance product-field-gate sdk-package actionlint)`.

- `architecture-report`, rust-quality's `quality` and `invariant-tools` are **not** required.
- This plan changes one required job, `rust`, and only makes it stricter (see §2).
- Job ids are unchanged, so any branch-protection names still match.

---

## 2. Contract decision

**Suite contract (owned by the new `scripts/suite.sh <log directory>`):**

1. `scripts/test-leg.sh <dir>/suite.log --inventory docs/refactor/test-inventory.json --skipped 1 -- --locked --release -- --skip post_split_throughput_scales`: every target of the root package, floor = inventory − 1 = 520.
2. Then `scripts/test-leg.sh <dir>/capacity.log --exact dst::dst_tests::topology_scaling::post_split_throughput_scales -- --locked --release --lib post_split_throughput_scales -- --exact <same>`: the capacity test alone.

Both legs run in one call, so no consumer can keep the suite and drop the capacity leg.

Consumers: `scripts/gate.sh`, `scripts/release-gate.sh`, ci.yml `rust` job, and `scripts/release-provenance.sh` (with `RUN_SUITE=1`).

Per consumer versus today:

- **ci.yml `rust`** (required):
  - Same targets.
  - Adds `--locked` on both legs. `Cargo.lock` is committed, and quality.sh already uses `--locked`.
  - The step's `grep ... | grep -qv " 0 failed"` is replaced by `tests_ran.py`, which refuses every result line whose status is not `ok` or whose failed count is not 0, so it is at least as strict.
  - Cargo's exit status still fails the step: `test-leg.sh` runs `set -euo pipefail` around `cargo test | tee`.
  - The F-G hang watcher stays in ci.yml around the call. `hang_dump.sh` uses `sudo gdb`/`apt-get`, so it must not run locally.
  - Stricter or equal on every axis, so no weakening.
- **gate.sh**: adds the 66 bin/integration tests (both pilot Loom models).
- **release-gate.sh**: adds the bin/integration tests and `--locked`; dev becomes release (see decision 1 in §9); the empty matrix heading goes.
- **release-provenance.sh**: the capacity test runs isolated, `--locked` is used, both tests-ran checks apply, and a failing suite now stops the script before it prints counts.

**Ratchet contract:** on a push, `merge_base()` raises `ValueError('push ratchet requires QUALITY_BEFORE_SHA')` when the previous revision is absent. Branch creation (`before` = 40 zeros), pull requests, schedules and local runs are unchanged. ci.yml `architecture-report` gets the same env and fetch step as rust-quality.yml.

**Where the reviewer's Change is wrong or out of scope** (nothing below is done):

- **"Delete the duplicated MT audit/lint and cargo check steps."** Not done.
  - `multitenancy-audit.sh` inside `rust` is the only place a *required* check runs the MT audit; `quality`, which also runs it, is not in REQUIRED_CHECKS. Deleting it weakens a required check.
  - `cargo check --all-targets` is subsumed by the suite build, but removing it is not suite ownership.
- **"Fold architecture-report."** Not done. Deleting the job changes job names, and it is the only place that runs `scripts/review-evidence-checkout.py`.
- **"release-gate.sh calls quality.sh."** Separate item. It changes the release gate's lint contract: fingerprint baseline becomes `-D warnings`, and rustdoc, machete and the python suite are added. See §8.

---

## 3. Red tests and pinning tests

Run the unittests with Python ≥ 3.11: system `python3` is 3.9.6, so use `PATH=$SCRATCH/pybin:$PATH`, where `pybin/python3` is 3.12.14.

### R1: `scripts/quality/test_suite_owner.py::SuiteOwner::test_the_repository_runs_the_suite_from_one_owner` (new file)

Reads every tracked or untracked-unignored `*.sh`/`*.yml`/`*.yaml` file (`git ls-files -z --cached --others --exclude-standard`; `.claude/worktrees/` is excluded by `.git/info/exclude`) and asserts `problems(files) == []`. Three rules:

- (a) the capacity name `post_split_throughput_scales` appears in exactly one of those files, `scripts/suite.sh`;
- (b) exactly one whole-package `cargo test` argument list exists, in `scripts/suite.sh`;
- (c) each consumer mentions `scripts/suite.sh`.

A whole-package list is found after `cargo test`, or after the first `--` of a `test-leg.sh` call. It has no name filter, no target/package selector and no `--no-run` before libtest's `--` or before a shell operator or redirect. Comment lines are skipped, and backslash continuations are joined and reported at their first line.

Traced on c397e9e5, the whole-package lists are:
- ci.yml:104: `['--release']` then `--`.
- release-provenance.sh:34-35: `['--release']` then the redirect `2>&1`, which stops the scan.

Everything else is selected:
- gate.sh:17 and :34 use `--lib`;
- release-gate.sh:78 and :83 (joined) use `--lib`;
- quality.sh:14 uses `-p`; quality.sh:43 uses `--lib`;
- test-leg.sh:25 has the positional `"$@"`;
- test-leg.sh:20 is a usage echo whose next word is the positional `arguments>"`;
- release-provenance.sh:33 is an echo whose next positional is `(this`;
- ci.yml:89, :120, :132, :134, :212 and rust-quality.yml:48 use `--lib`.

The comment at ci.yml:22 is skipped.

**Expected red.** The class sets `maxDiff = None`. `pprint` may wrap the two long elements.

```
FAIL: test_the_repository_runs_the_suite_from_one_owner (test_suite_owner.SuiteOwner.test_the_repository_runs_the_suite_from_one_owner)
AssertionError: Lists differ: [...] != []

First list contains 6 additional elements.
First extra element 0:
"the capacity test is named in ['.github/workflows/ci.yml', 'scripts/gate.sh', 'scripts/release-gate.sh'], not only scripts/suite.sh"
```

The six elements, in order:

1. `the capacity test is named in ['.github/workflows/ci.yml', 'scripts/gate.sh', 'scripts/release-gate.sh'], not only scripts/suite.sh`
2. `a whole-package cargo test runs at ['.github/workflows/ci.yml:104', 'scripts/release-provenance.sh:34'], not only scripts/suite.sh`
3. `.github/workflows/ci.yml does not run scripts/suite.sh`
4. `scripts/gate.sh does not run scripts/suite.sh`
5. `scripts/release-gate.sh does not run scripts/suite.sh`
6. `scripts/release-provenance.sh does not run scripts/suite.sh`

The module then ends with `Ran 4 tests` / `FAILED (failures=1)`, because the three fixture tests below pass on both trees.

### Pinning and fixture controls in the same file (green before and after; they exercise the classifier, not the tree)

- `test_every_spelling_of_a_whole_package_run_is_a_suite`. Each of these gives `suites('x.sh', line) == ['x.sh:1']`:
  - `cargo test --release 2>&1 | tee log`
  - `cargo test --locked --release -- --skip capacity`
  - `cargo test --features telemetry --release 2>&1` (kills dropping value-flag consumption)
  - `cargo test \<newline>  --release` (kills dropping continuation joins)
  - `        run: 'scripts/test-leg.sh log --min 1 -- --locked --release'`
  - `bash scripts/test-leg.sh log --inventory i.json --skipped 1 \<newline>  -- --release -- --skip x`
- `test_selected_legs_and_prose_are_not_suites`. Each of these gives `[]`:
  - `cargo test --release --lib sse::`
  - `cargo test --locked -p streams-quality-syntax`
  - `cargo test --package=streams-quality-syntax` (kills dropping the `=` split)
  - `cargo test "$@" 2>&1 | tee "$log"`
  - `cargo test --target-dir d --lib` (kills dropping the selector check)
  - `cargo test --release --no-run`
  - `  # the suite is cargo test --release` (kills dropping comment skipping)
  - `  echo "usage: $0 <log> -- <cargo test arguments>" >&2`
  - `scripts/test-leg.sh log --exact a::b -- --release --lib b -- --exact a::b`
  - `scripts/test-leg.sh)` (a `test-leg.sh` without `--` is not a call)
- `test_a_second_copy_or_a_bypassing_consumer_is_refused`:
  - A minimal owner plus four consumers that call it gives `[]`.
  - Appending `cargo test --release -- --skip post_split_throughput_scales` to gate.sh gives exactly `["the capacity test is named in ['scripts/gate.sh', 'scripts/suite.sh'], not only scripts/suite.sh", "a whole-package cargo test runs at ['scripts/gate.sh:2', 'scripts/suite.sh:1'], not only scripts/suite.sh"]`.
  - gate.sh = `cargo test --release --lib\n` gives `['scripts/gate.sh does not run scripts/suite.sh']`.
  - An empty owner gives both `... named in [], not only ...` and `... runs at [], not only ...`, so the owner itself must hold the suite. This is the non-vacuity of rule (b).

### R2: `scripts/quality/test_event_comparison.py::EventComparison::test_a_push_ratchet_without_the_previous_revision_fails_closed`

The fixture repo from `setUp` gets `git update-ref refs/remotes/origin/slate HEAD`, which models CI, where `origin/<branch>` is the pushed commit. It then calls `common.merge_base()` under env `{PATH, GITHUB_EVENT_NAME: 'push', QUALITY_BASE_REF: 'origin/slate'}`, cleared as in the existing `resolve` helper, and asserts `ValueError` matching `push ratchet requires QUALITY_BEFORE_SHA`.

Trace on the current tree: `event='push'`, `before=''`, so the `if` at line 27 is false, and `git merge-base HEAD origin/slate` returns `self.second` with no exception. Expected red:

```
FAIL: test_a_push_ratchet_without_the_previous_revision_fails_closed (test_event_comparison.EventComparison.test_a_push_ratchet_without_the_previous_revision_fails_closed)
AssertionError: ValueError not raised
```

The module reports `Ran 8 tests` / `FAILED (failures=1)`.

### Pin in the same file: `test_push_and_local_ratchets_keep_their_bases` (green before and after)

- Push with `QUALITY_BEFORE_SHA=self.first` and `QUALITY_BASE_REF=origin/slate` returns `self.first`. This kills "ignore before".
- `QUALITY_BASE_REF=target` with no event returns `self.first`.
- Push with `QUALITY_BEFORE_SHA='0'*40` and `QUALITY_BASE_REF=target` returns `self.first`. Branch creation keeps the base ref, which kills "raise on zero before" too.

### Behavioural non-vacuity for the suite unification (§7 C9/C10)

- The new suite log has 12 `test result` lines, against 1 today.
- It contains `Running unittests src/bin/pilot.rs` and `Running tests/pilot_membership.rs`, and both Loom test names with `... ok`.
- After `QUALITY_OK`, the gate's `$OUT` has 13 `test result` lines, against 2 in `gate-b11.txt`. The quality.sh section before `QUALITY_OK` holds 2 more in both.

---

## 4. Edits, file by file, in commit order

No file over 1,000 lines is touched: ci.yml has 369 lines, common.py 150, and every other file is under 100.

No `.rs` production scope changes, so there is no `#[expect]` or ratchet scope to re-decide. The only `.rs` edit is a doc comment in `src/dst/tests/fixture_failpoints.rs`, which has 42 lines, keeps the same line count, is not a test function, and is not in any inventory, scenario, mechanism or mutation-owner ledger (checked with grep).

### Commit A: "The suite has one owner, scripts/suite.sh, and every gate runs all of it"

Red first: write A1 on the untouched tree, run it, and see R1 fail with the six elements. Then make the edits.

**A1. New `scripts/quality/test_suite_owner.py`** (about 120 lines; Python 3.9+ syntax is enough):

```python
"""The Rust suite has one owner, scripts/suite.sh (review item 69).

gate.sh, release-gate.sh, ci.yml and release-provenance.sh each carried their
own copy of the suite and capacity invocations, and the copies drifted: the
local gates tested the lib target alone, the release gate the dev profile
without --locked, and one capacity rename had to land in three files at once.
"""
import re
import subprocess
import unittest

from common import ROOT

OWNER = 'scripts/suite.sh'
CONSUMERS = ('.github/workflows/ci.yml', 'scripts/gate.sh', 'scripts/release-gate.sh',
             'scripts/release-provenance.sh')
CAPACITY = 'post_split_throughput_scales'
# A cargo test with none of these and no name filter runs the whole package.
SELECTORS = {'--lib', '--bin', '--bins', '--test', '--tests', '--doc', '--example',
             '--examples', '--bench', '--benches', '-p', '--package', '--no-run'}
VALUED = {'--features', '-F', '--profile', '--target', '--target-dir', '--manifest-path',
          '-j', '--jobs', '--message-format', '--color', '--config', '-Z', '--exclude'}
STOP = re.compile(r'^(?:\|\|?|&&?|;|\d*[<>].*)$')
COMMAND = re.compile(r'\bcargo\s+test\b|\btest-leg\.sh\b')


def lines(text):
    """Shell command lines with their first physical line; comments dropped."""
    start, pending = 1, ''
    for number, line in enumerate(text.splitlines(), 1):
        if not pending:
            if line.lstrip().startswith('#'):
                continue
            start = number
        if line.endswith('\\'):
            pending += line[:-1] + ' '
            continue
        yield start, pending + line
        pending = ''
    if pending:
        yield start, pending


def selects_all(words):
    value = False
    for word in words:
        if word == '--' or STOP.match(word):
            return True
        if value:
            value = False
        elif word.split('=', 1)[0] in SELECTORS:
            return False
        elif word.startswith('-'):
            value = word in VALUED
        else:
            return False
    return True


def suites(path, text):
    found = []
    for number, line in lines(text):
        for match in COMMAND.finditer(line):
            words = line[match.end():].split()
            if match.group().startswith('test-leg'):
                if '--' not in words:
                    continue
                words = words[words.index('--') + 1:]
            if selects_all(words):
                found.append(f'{path}:{number}')
    return found


def problems(files):
    found = []
    named = sorted(path for path, text in files.items() if CAPACITY in text)
    if named != [OWNER]:
        found.append(f'the capacity test is named in {named}, not only {OWNER}')
    runs = [site for path in sorted(files) for site in suites(path, files[path])]
    if [site.rsplit(':', 1)[0] for site in runs] != [OWNER]:
        found.append(f'a whole-package cargo test runs at {runs}, not only {OWNER}')
    found.extend(f'{path} does not run {OWNER}' for path in CONSUMERS
                 if OWNER not in files.get(path, ''))
    return found


def tracked():
    names = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--',
         '*.sh', '*.yml', '*.yaml'], cwd=ROOT, text=True).split('\0')
    return {name: (ROOT / name).read_text() for name in names
            if name and (ROOT / name).is_file()}


class SuiteOwner(unittest.TestCase):
    maxDiff = None
    # test_the_repository_runs_the_suite_from_one_owner: assertEqual(problems(tracked()), [])
    # + the three fixture tests of §3, verbatim inputs/expectations as listed there.
```

Notes:

- Quote stripping is deliberately absent. No fixture distinguishes it: a trailing `'` still leaves a word that is a flag or a positional. Keeping it would be dead code.
- `--no-run` is in `SELECTORS` because a build-only `cargo test` runs nothing.

**A2. New `scripts/suite.sh`**, mode 100755 (`chmod +x` before `git add`; ci.yml calls it directly, as it calls `scripts/test-leg.sh`, which is 100755):

```bash
#!/usr/bin/env bash
# The one copy of the Rust suite (review item 69). scripts/gate.sh,
# scripts/release-gate.sh, ci.yml's rust job and
# scripts/release-provenance.sh each kept their own, and the copies
# drifted: the local gates tested the lib target alone, so the bin and
# integration tests (the pilot Loom models among them) ran only in CI,
# and the release gate tested the dev profile without --locked.
# scripts/quality/test_suite_owner.py refuses a second copy.
#
#   scripts/suite.sh <log directory>
#
# The capacity test runs alone because its throughput ratio is only
# valid while the measurement owns the machine: inside the parallel
# suite, contention lands one-sidedly on the post-split phase (it needs
# two committers' worth of CPU) and only ever understates the ratio;
# round 9 measured 1.73-1.80 in-suite against 1.8x with healthy
# baselines. External host load still depresses it; the test's own
# failure text says how to distinguish. Both legs go through
# test-leg.sh, so a leg that selects nothing fails; the suite's floor is
# the test inventory less the capacity test.
set -euo pipefail
cd "$(dirname "$0")/.."
logs=${1:?usage: scripts/suite.sh <log directory>}
capacity=dst::dst_tests::topology_scaling::post_split_throughput_scales
echo "== suite: every target but the capacity test =="
scripts/test-leg.sh "$logs/suite.log" --inventory docs/refactor/test-inventory.json --skipped 1 \
  -- --locked --release -- --skip post_split_throughput_scales
echo "== capacity mechanism gate (owns the machine) =="
scripts/test-leg.sh "$logs/capacity.log" --exact "$capacity" \
  -- --locked --release --lib post_split_throughput_scales -- --exact "$capacity"
```

The script works on macOS `/bin/bash` 3.2. Both `test-leg.sh` calls pass options, so the `"${expect[@]}"` empty-array-under-`set -u` trap never applies. A relative `<log directory>` resolves against the repo root, as `test-leg.sh` does.

**A3. `scripts/gate.sh`**: 45 lines become about 25. Replace lines 17-44 (the suite, suite-ran, capacity and capacity-ran blocks, including the capacity comment, which moves to suite.sh) with:

```bash
# The suite CI runs, not a narrower local copy (review item 69): every
# target, then the capacity test alone. The legs' full logs stay in
# $OUT.legs; $OUT keeps each binary's result line and the tests-ran
# verdicts.
if ! scripts/suite.sh "$OUT.legs" 2>&1 | grep -E '^(test result|TESTS_RAN)' >> "$OUT"; then
  echo GATEFAIL-suite >> "$OUT"
  exit 1
fi
echo GATEDONE >> "$OUT"
```

With `set -o pipefail`, a failing suite.sh fails the `if` even when grep matched lines (control C11).

Visible effects, all local tooling and parsed by nothing (`grep -rn GATEFAIL` finds only gate.sh):
- The logs move from `$OUT.suite.log`/`$OUT.capacity.log` to `$OUT.legs/suite.log` and `$OUT.legs/capacity.log`.
- `GATEFAIL-suite-ran`, `GATEFAIL-capacity` and `GATEFAIL-capacity-ran` fold into `GATEFAIL-suite`. The `TESTS_RAN_FAIL: <log>` line names the leg.
- `$OUT` now also shows `test result: FAILED` lines.

**A4. `scripts/release-gate.sh`** (POSIX sh): 90 lines become about 81.

- Header lines 4-6 become: `# the Rust suite from scripts/suite.sh (every target, locked, release` / `# profile; the capacity test alone so its measurement owns the machine;` / `# livefeed IS the engine since round 11.8, so the suite is its coverage),` / `# and supply-chain checks. It does NOT run the Durable Streams conformance`. The rest of the scope statement is unchanged.
- Lines 75-87 become:

```sh
echo "== tests =="
bash scripts/suite.sh target/release-gate

echo "== supply chain =="
```

This deletes the `== capacity mechanism gate ==` heading (suite.sh prints it now) and the empty `== livefeed engine matrix ==` heading. The log paths stay `target/release-gate/{suite,capacity}.log`.

**A5. `.github/workflows/ci.yml`, `rust` job** (job id unchanged):

- Line 22 comment: "conformance run inside `cargo test --release` (dst::dst_tests);" becomes "conformance run inside the full suite, scripts/suite.sh (dst::dst_tests);".
- Lines 90-120 (the Test step and the "Capacity mechanism gate (owns the runner)" step) become:

```yaml
      - name: Test, then the capacity mechanism gate alone (product conformance + dual-surface + DST + cost budgets)
        run: |
          # F-G: a lib test binary still alive after 15 minutes is hung
          # (the whole suite takes ~3); dump every thread's stack and
          # kill it so the step fails with the stuck frames, not at the
          # job deadline. The hash keeps pgrep off the watcher itself.
          scripts/quality/hang_dump.sh 'target/release/deps/streams_slate-[0-9a-f]{16}' 900 &
          watcher=$!
          # scripts/suite.sh is the one copy of the suite and its
          # isolated capacity leg; the local gates run the same one.
          scripts/suite.sh target/legs
          kill "$watcher" 2>/dev/null || true
```

- The `set -o pipefail` block and its comment go away: no pipeline is left in the step, and the rationale is embodied in `test-leg.sh`.
- `/tmp/suite.log` becomes `target/legs/suite.log`. Nothing reads either after the step.
- The capacity step name disappears. No doc or script references it (checked with grep).
- The default `bash -e` shell fails the step on a suite.sh failure. The watcher is then left to runner cleanup, as today.

**A6. `scripts/release-provenance.sh`**: lines 32-36 become:

```bash
if [ "${RUN_SUITE:-0}" = "1" ]; then
  echo "--- running scripts/suite.sh (this is slow) ---"
  logs=$(mktemp -d)
  scripts/suite.sh "$logs" > /dev/null
  cat "$logs/suite.log" "$logs/capacity.log" | grep -E '^test result' | \
    awk '{p+=$4; f+=$6} END {print "rust_suite_passed:  " p; print "rust_suite_failed:  " f}'
  rm -rf "$logs"
fi
```

The script already runs `cd` to the toplevel and `set -euo pipefail`. `TESTS_RAN_FAIL` still reaches stderr.

**A7. `src/dst/tests/fixture_failpoints.rs`** doc comment, same line count:

- Line 9: `/// it so any unskipped run (a local cargo test, release-provenance.sh)` becomes `/// it so any unskipped run (a bare local cargo test)`.
- Line 11: `/// ratio; CI skips that test in the parallel suite and runs it alone.` becomes `/// ratio; scripts/suite.sh skips it in the parallel suite and runs it alone.`

The reason is kept: it still says why the lock exists.

**A8. `docs/RUST-QUALITY.md`**:

- Line 13: append "The full suite and that capacity leg have one owner, `scripts/suite.sh`: `scripts/gate.sh`, `scripts/release-gate.sh`, ci.yml's `rust` job and `scripts/release-provenance.sh` call it, and `scripts/quality/test_suite_owner.py` refuses a second copy."
- Lines 148-149: "and ci.yml's full `cargo test --release`, which also carries the two pilot Loom models" becomes "and the full suite (`scripts/suite.sh`, every target, run by ci.yml's `rust` job and `scripts/gate.sh`), which also carries the two pilot Loom models". Rewrap the paragraph lines.

### Commit B: "A push compares the source ratchet with the previous pushed revision, never with itself"

Red first: add R2 and the pin, then run them and see R2 fail with `ValueError not raised`.

**B1. `scripts/quality/test_event_comparison.py`** (79 lines become about 100): add a `ratchet_base(**env)` helper next to `resolve`, with the same env patch but calling `common.merge_base()`, plus R2 and the pin from §3.

**B2. `scripts/quality/common.py:20-29`**:

```python
def merge_base():
    # Source/debt ratchets retain merge-base semantics. Verification selection
    # uses verification_comparison() below because push and schedule events have
    # different meanings from a pull request. On a push origin/<branch> is the
    # pushed commit itself, so only the event's previous revision is a base;
    # without it the ratchet would compare HEAD with HEAD and pass any growth.
    target = os.environ.get('QUALITY_BASE_REF') or 'origin/slate'
    before = os.environ.get('QUALITY_BEFORE_SHA', '')
    event = os.environ.get('QUALITY_EVENT_NAME') or os.environ.get('GITHUB_EVENT_NAME', '')
    if event == 'push':
        if not before:
            raise ValueError('push ratchet requires QUALITY_BEFORE_SHA')
        if set(before) != {'0'}:
            target = before
    return git('merge-base', 'HEAD', target)
```

Other callers stay safe:

- rust-quality.yml sets `QUALITY_BEFORE_SHA: ${{ github.event.before }}` at workflow level, which covers the `quality` job's gate.py and architecture-gate runs.
- Local runs set no event.
- `test_source_gate.py` patches `merge_base`.
- `test_event_comparison.py` clears the env.
- No other test calls `merge_base` unpatched (grep).

**B3. `.github/workflows/ci.yml`, `architecture-report` job** (job id unchanged):

- Add a job-level `env:` between `timeout-minutes: 15` and `steps:`:

```yaml
    # On a push the source ratchet's base is the event's previous revision:
    # origin/<branch> is the pushed commit itself, so without it the ratchet
    # compared HEAD with HEAD (scripts/quality/common.py now refuses that).
    env:
      QUALITY_EVENT_NAME: ${{ github.event_name }}
      QUALITY_BEFORE_SHA: ${{ github.event.before }}
      QUALITY_BASE_REF: origin/${{ github.base_ref || github.ref_name }}
```

- Insert right after the checkout step the fetch step copied verbatim from rust-quality.yml:29-31 (`Fetch exact previous push revision when unreachable`, with the same `if:` and `run:`).
- Delete the step-level `env:` from "Architecture budgets and owner boundaries (hard gate)" (ci.yml:44-45).
- Leave `QUALITY_HEAD_SHA` out: only `verification_comparison()` reads it.
- Keep the checkout step byte-identical. `scripts/review-evidence-checkout.py:22-24` slices the text between `  architecture-report:` and `\n  rust:` and asserts `uses: actions/checkout@v4\n\s+with:\n(?:\s+#.*\n)*\s+fetch-depth: 0`. Neither the job-level `env:` nor the new step after checkout affects that match.

**B4. `docs/RUST-QUALITY.md:59`**: append "On a push the comparison base is the event's previous revision (`QUALITY_BEFORE_SHA`), because `origin/<branch>` is the pushed commit itself; a push without it fails closed."

End both commit messages with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

---

## 5. Mutation analysis

- **cargo-mutants selects nothing.** No `.rs` file under a critical prefix or registered in `mutation_owners.py` changes. The only `.rs` change is a doc comment in `src/dst/tests/fixture_failpoints.rs`: not critical, not registered, and `gap_lock` has no FnValue body change. `verification_plan.py` therefore gives `mutants: false`.
- **Checks triggered by the `scripts/quality/` paths.** They set `tooling=true` (verification_plan.py:77), so `properties_fuzz` and `miri` are selected. `invariant-tools` will replay the saved corpus and run the Miri batch. These are existing obligations; this change adds nothing for them to kill.
- **Python mutants, by construction.** Each branch of the classifier and of `merge_base` has a killing fixture:

| Mutant | Killing test |
|---|---|
| Drop `STOP` | `cargo test --release 2>&1 \| tee log` becomes selected; the suite fixture fails |
| Drop `word == '--'` | `cargo test --locked --release -- --skip capacity` walks to the positional `capacity` and becomes selected; fails |
| Drop the selector branch | `cargo test --target-dir d --lib` becomes a suite; the control fails |
| Drop `split('=')` | `--package=...` becomes a suite; the control fails |
| Drop `VALUED` | `--features telemetry --release` becomes selected; the suite fixture fails |
| Drop comment skipping | the comment control fails |
| Drop continuation joins | both continuation fixtures fail |
| Drop the test-leg branch | the YAML test-leg fixture fails |
| Drop the `'--' not in words` guard | `ValueError` on `scripts/test-leg.sh)` |
| `named != [OWNER]` becomes `>`, `in`, etc. | the empty-owner and two-copy cases |
| `merge_base`: drop the raise | R2 |
| `merge_base`: raise on zero `before` too | the pin's third assertion |
| `merge_base`: ignore `before` | the pin's first assertion |

- **No timing loops.** The Loom models that now also run locally are bounded (`preemption_bound = Some(2)`, `max_branches = 1000`), as in CI.

---

## 6. Ledgers

**None change.** Checked:

- `docs/refactor/test-inventory.json`: no test function or `include_*!` fixture changes, and `fixture_failpoints.rs` is not referenced; `scripts/test-inventory.py --check` stays OK.
- `docs/refactor/review-mechanisms.json`: pins no `scripts/` or `.github/` path (checked the JSON for such strings).
- `docs/quality/owners.json`, `source-allowances.json` and `architecture-policy.json`: Rust-only, and no scope is touched.
- `mutation_owners.py`: the new files are not Rust, and there are no critical prefixes involved.
- Scenario map and dispositions: no renamed or deleted tests.
- `src/dst/tests/README.md:43-47`: still true.
- `docs/quality/verification.json` `gate_tooling_sha256`: a historical receipt verified by nothing (`grep -rn gate_tooling_sha256 scripts .github` is empty). Its `common.py` hash, `bffefff…`, is already stale against the current `664cbb10…`.

**Policy prose changes** in `docs/RUST-QUALITY.md` (A8, B4) land in the same commits. Nothing hashes that file.

---

## 7. Controls

Run C1-C8 at any time; they need no CPU-heavy builds beyond the already-built `streams-quality-syntax`. Run C9, C10 and C12 only after the running mutation leg finishes, because of CPU contention. `SCRATCH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad` and `export PATH=$SCRATCH/pybin:$PATH`.

**Red, on the tree with only the new tests added:**

- **C1:** `python3 -m unittest discover -s scripts/quality -p test_suite_owner.py -v` gives the R1 red above, then `Ran 4 tests` / `FAILED (failures=1)`.
- **C2:** `python3 -m unittest discover -s scripts/quality -p test_event_comparison.py -v` gives `test_a_push_ratchet_without_the_previous_revision_fails_closed ... FAIL` / `AssertionError: ValueError not raised` / `Ran 8 tests` / `FAILED (failures=1)`.
- **C2b** (the vacuity itself, before B2): `GITHUB_EVENT_NAME=push QUALITY_BASE_REF=HEAD python3 -c 'import sys; sys.path.insert(0,"scripts/quality"); import common; print(common.merge_base())'` prints the value of `git rev-parse HEAD` (c397e9e5… plus this item's commits), meaning the ratchet base is HEAD.

**Green, after each commit:**

- **C3:** C1 and C2 give `OK`.
- **C4:** `python3 -m unittest discover -s scripts/quality` gives `OK`.
- **C5:** `bash -n scripts/suite.sh && bash -n scripts/gate.sh && bash -n scripts/release-provenance.sh && sh -n scripts/release-gate.sh` exits 0 silently. `git ls-files -s scripts/suite.sh` starts with `100755`.
- **C6:** `actionlint` (bare) exits 0 with no output.
- **C7** (after commit B): `python3 scripts/review-evidence-checkout.py` ends with `fresh-checkout evidence integration: OK (2 real Git checkouts)`.
- **C8:**
  - `GITHUB_EVENT_NAME=push QUALITY_BASE_REF=HEAD python3 scripts/architecture-gate.py --check; echo $?` gives a traceback ending `ValueError: push ratchet requires QUALITY_BEFORE_SHA` and then `1`.
  - `GITHUB_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/architecture-gate.py --check` ends with `architecture-gate: OK (<n> Rust files; fixed baseline a7e2070f3b43)`.
  - The same env with the C2b one-liner prints `5d9d517f2bafb67b29dc7a1f9d4c87b44c045773`, not HEAD. That is the non-vacuous base.
- **C9:** `scripts/suite.sh /tmp/suite69`. The last lines are `TESTS_RAN_OK: /tmp/suite69/suite.log: floor 520, exact 0` and `TESTS_RAN_OK: /tmp/suite69/capacity.log: floor 1, exact 1`. Then check the suite log:
  - `grep -c '^test result' /tmp/suite69/suite.log` gives `12` (lib, 9 bins, `pilot_membership`, doctests).
  - `grep -cE 'Running (unittests src/bin/pilot.rs|tests/pilot_membership.rs)' /tmp/suite69/suite.log` gives `2`.
  - `grep -cE '(quality_loom_final_membership_acquires_all_accounting_and_prevents_late_work|quality_loom_completion_and_freeze_use_the_actual_window_transitions) \.\.\. ok$' /tmp/suite69/suite.log` gives `2`.
  - Surface any bin test that fails on macOS as a finding; never skip it.
- **C10:** `OUT=/tmp/gate69.txt scripts/gate.sh; tail -3 /tmp/gate69.txt` ends with `TESTS_RAN_OK: /tmp/gate69.txt.legs/capacity.log: floor 1, exact 1` / `GATEDONE`. `sed -n '/^QUALITY_OK$/,$p' /tmp/gate69.txt | grep -c '^test result'` gives `13`. The same command on `$SCRATCH/gate-b11.txt` (the old lib-only gate) gives `2`.
- **C11** (pipeline fails closed): `bash -c 'set -euo pipefail; if ! (echo "test result: ok. 1 passed; 0 failed;"; exit 1) | grep -E "^(test result|TESTS_RAN)" > /dev/null; then echo FAILS_CLOSED; fi'` prints `FAILS_CLOSED`.
- **C12** (optional, a second full suite): `RUN_SDK=0 RUN_SUITE=1 scripts/release-provenance.sh | tail -2` gives `rust_suite_passed:  <n>` / `rust_suite_failed:  0`. `grep -c 'livefeed engine matrix' scripts/release-gate.sh` gives `0`. The full `sh scripts/release-gate.sh` (ends `RELEASE_GATE_LOCAL_OK`) runs at the next RC.
- **C13** (the item's own plan, before push): `QUALITY_BASE_REF=$(git rev-parse HEAD~2) python3 scripts/quality/verification_plan.py --out /tmp/plan69 && python3 -c "import json;p=json.load(open('/tmp/plan69/plan.json'));print(p['mutants'],p['miri'],p['properties_fuzz'],p['changed_rust_files'])"` gives `False True True ['src/dst/tests/fixture_failpoints.rs']`. Against origin/slate the plan also carries the eight unpushed commits' Rust.
- **C14** (after push; never claim green without it): `gh run view` for:
  - `ci`: the `rust` step log shows `TESTS_RAN_OK: target/legs/suite.log: floor 520, exact 0` and `TESTS_RAN_OK: target/legs/capacity.log: floor 1, exact 1`, and `architecture-report` shows `architecture-gate: OK`, now against the previous tip;
  - `rust-quality`: `quality` runs the two new unittest modules; `invariant-tools` runs the corpus and Miri;
  - `workflow-lint`.

---

## 8. Out of scope

- **Deleting ci.yml `rust`'s `Check`, `Multitenancy audit` and `Multitenancy identity lint` steps** (reviewer). The MT audit in `rust` is the only *required* run of it, so deleting it weakens a required check. `cargo check --all-targets` is merely redundant, but that is not suite ownership.
- **Folding `architecture-report` into rust-quality.** It would delete a job name and lose `review-evidence-checkout.py`.
- **`release-gate.sh` calling `quality.sh`** (reviewer). That changes the release gate's lint contract: fingerprint baseline becomes `-D warnings`, and rustdoc, machete and the Python suite are added. It deserves its own item.
- **`merge_base()` on branch creation** (`before` = 40 zeros) still uses `origin/<branch>`, which is HEAD. This only arises when main or slate itself is created, and `verification_comparison` uses the empty tree there. Using the empty tree for the source ratchet would put every file under the new-file rules. Pinned unchanged.
- **`--locked` on ci.yml's filtered legs** (`sse::`, `livefeed_`, mt-lint, mt-cert). These are filtered legs, not the suite.
- **The example command in `docs/review-verification-evidence.md:166`**, and the historical `docs/refactor/BASELINE.md` and `docs/review-tests-evidence.md`.
- **The reviewer's "release-gate.sh without summary check".** Already fixed by 00ff0e7e; nothing is planned.

---

## 9. Decisions for Søren

1. **The release gate's test profile.** `release-gate.sh` (the local half of `promote-rc.sh`/`rc-certify.sh`) is today the only full-suite run with `debug_assertions` and `overflow-checks` on. The dev profile gives it 9 `debug_assert!` sites, among them:
   - `src/sse/feed.rs:507` `budget release underflow`,
   - `src/segmap.rs:353/421` `check_partition()`,
   - `src/postings.rs:409`,
   - `src/http.rs:2742`,

   plus integer overflow panics. The profile was never a stated contract: it has been dev since 8e8d77bb. This plan unifies on `--locked --release`, the profile the required `rust` check certifies, as the review asks, so the release gate stops exercising those assertions.

   Decide whether that coverage should exist deliberately. If yes, it should be a named `debug` leg in `scripts/suite.sh`, run by CI, rather than an accident of the release gate's profile.

   The mutation leg's `quality` profile (dev-derived) keeps debug assertions over owner scopes either way.

2. **REQUIRED_CHECKS observation, no change made.** `architecture-report`, rust-quality `quality` and `invariant-tools` are not in `promote-rc.sh` REQUIRED_CHECKS. An RC is therefore certified without the source/merge-base ratchets, the `-D warnings` Clippy leg or mutation results as required check runs. `docs/RUST-QUALITY.md` makes those merge requirements. Adding them would be a strengthening policy change, left to you.

---

## Skeptic corrections (C1..C12)

Verified read-only on the tree at c397e9e5. The plan's central claims hold:
- All four suite copies and three capacity copies are quoted exactly right (gate.sh:17/34-35/40-41, release-gate.sh:78-84/86-87, ci.yml:104/113/120, release-provenance.sh:33-35).
- The grep of tracked and untracked-unignored `*.sh`/`*.yml`/`*.yaml` files finds only the seven files listed; `git ls-files --others --exclude-standard` returns none today.
- The count of 66 test attributes outside the lib is correct.
- REQUIRED_CHECKS at promote-rc.sh:51-53 is quoted correctly.
- common.py:20-29 and source_gate.py:27 are quoted correctly, and `merge_base` is called only from gate.py:37 and source_gate.py:27. test_source_gate.py:44,124 patch it.
- The review-evidence-checkout.py:21-24 slice/regex is quoted correctly.
- tests_ran.py sums `passed` over every result line and prints OK on stdout and FAIL on stderr. The floor is 521-1 = 520.
- No ceilinged file is touched: ci.yml is 369 lines and common.py 150.
- No `#[expect]` scope is involved: `gap_lock` at fixture_failpoints.rs:21-24 carries no attribute.
- mutants=false holds: src/dst is not a critical prefix or a registered owner.

I traced R1 against every use site. The red list has exactly six elements in the stated order. After the change the only whole-package list is the suite.sh suite leg, and every fixture and control in §3 classifies as claimed. R2 is red with `ValueError not raised`: `event='push'`, `before=''`, and `merge-base HEAD origin/slate` returns `self.second`. The module then has 6+2 = 8 tests. Every red/green trace is bounded.

- **C1 (stale facts in the header and C8).**
  - `origin/slate` is now c397e9e5. `git reflog origin/slate` shows a push at 2026-09-24 07:10, so the eight commits are no longer unpushed.
  - "None of them touches `scripts/`" is false: `git diff --stat 5d9d517f HEAD -- scripts` shows `scripts/platform-e2e.mjs` (034d809d). That file is irrelevant to this item, but the claim must go.
  - C8's third bullet hard-codes `5d9d517f…`. Print and compare `$(git rev-parse origin/slate)` at run time instead.
  - C13's `HEAD~2` is still right.

- **C2 (A3 replacement range).** The replacement block for gate.sh ends with `echo GATEDONE >> "$OUT"`, so it replaces gate.sh:17-**45**, not 17-44. Taken literally, 17-44 leaves the old line 45 in place and writes GATEDONE twice.

- **C3 (release-gate scope statement is still incomplete after A4).** A4 rewrites the header, which is governed by release-gate.sh:9-10 ("this scope statement must match what the script runs"). The header omits two things the script runs:
  - the structural and evidence ratchets (release-gate.sh:14-24: architecture-report/gate, scenario-map, test-inventory, review-evidence, verify-rc-evidence);
  - the multitenancy conversion audit (release-gate.sh:61-62).

  Since A4 edits the statement anyway, name them too. Otherwise the rewritten "must match" line is still false.

- **C4 (rule (c) is satisfied by a comment).** `problems()` checks `OWNER not in files.get(path, '')` on raw text, so a consumer that only *mentions* `scripts/suite.sh` in a comment passes while running its own lib-only copy. Such a copy does not name the capacity test, so rule (a) misses it; it is `--lib`, so rule (b) misses it too.
  - Fix: judge rule (c) over the comment-stripped `lines()`.
  - Add a fixture: `gate.sh = '# scripts/suite.sh\ncargo test --release --lib\n'` must give `['scripts/gate.sh does not run scripts/suite.sh']`.
  - Separately, reword the module docstring and RUST-QUALITY A8 from "refuses a second copy" to what it enforces: the capacity selector lives only in suite.sh, the whole-package run lives only in suite.sh, and each consumer calls suite.sh. A filtered `--lib` leg without the capacity name is by design not a "copy".

- **C5 (fixture texts must be pinned).** The expected output of `test_a_second_copy_or_a_bypassing_consumer_is_refused` (`'scripts/gate.sh:2'`, `'scripts/suite.sh:1'`) depends on exact fixture text, which §3 does not give. Pin these verbatim:
  - owner = `'scripts/test-leg.sh s.log -- --release -- --skip post_split_throughput_scales\n'`;
  - each consumer = `'scripts/suite.sh logs\n'`.

  With these, appending the copy to gate.sh puts it on line 2. A1's placeholder "verbatim inputs/expectations as listed there" is not buildable until the texts exist.

- **C6 (the "not in any ledger" claim for fixture_failpoints.rs is imprecise).** The file does appear in ledgers:
  - `docs/quality/source-allowances.json:1789-1800` (global rows `crate::gap_lock::L` and `crate::sweep_lock::L`, keyed by type syntax);
  - `docs/quality/legacy-source.json:117` (lines: 31) and :2892/:2899;
  - `docs/quality/verification.json:605` (a sha256 that nothing reads: `git grep verification.json -- '*.py' '*.sh' '*.yml'` is empty).

  None of these is keyed on doc text or line position, so A7 is safe only if the file stays exactly 42 lines. State that as the invariant and check it with `wc -l` before committing.

- **C7 (the full suite runs more than lib + 66).** The following bins `#[path]`-include `src/crypto.rs` and `src/tenant.rs`, whose `#[cfg(test)]` modules (crypto.rs:641-694, tenant.rs:791-793) therefore run three more times in bin-crate context:
  - src/bin/keys.rs:8-19;
  - src/bin/livebench.rs:34-45;
  - src/bin/cryptobench.rs:4-15.

  The 12 result lines and the floor of 520 in C9 are unaffected. But §1a's "what `--lib` drops" and the gate-time expectations should say so. The local gate will also be noticeably slower, because it builds 9 bins plus an integration test with release thin LTO (Cargo.toml:72-73). Tell the orchestration harness about the new duration and the new log paths (`$OUT.legs/{suite,capacity}.log` instead of `$OUT.suite.log`/`$OUT.capacity.log`), since harness scripts outside the repo read gate output.

- **C8 (Decision 1 is taken, not asked).** Switching release-gate.sh from dev to `--release` removes the only full-suite run with `debug_assertions` and `overflow-checks` from the RC path (promote-rc.sh:88, rc-certify.sh:53). That covers all 9 `debug_assert!` sites (`git grep -n 'debug_assert' src`: read_decode.rs:94, system_append.rs:126, http.rs:2742, postings.rs:409, segmap.rs:353/421, sse/feed.rs:507/803/1009) plus arithmetic overflow panics.
  - The plan lists this as a decision for Søren but lands it in commit A.
  - Hold the push of commit A until Søren answers. If he wants the coverage, suite.sh gains a named `--locked` dev leg in the same commit, and CI's `rust` job runs it too, so ownership stays single.
  - Never quietly ship the reduction.

- **C9 (A6 hides failure evidence).** `scripts/suite.sh "$logs" > /dev/null` under `set -euo pipefail` sends cargo's compile/test failure output to /dev/null. On failure the script exits before `rm -rf`, and the temp directory path is never printed, so only a one-line `TESTS_RAN_FAIL` (or nothing, on a cargo failure) reaches the operator.
  - Print `logs: $logs` to stderr before the run and send suite.sh stdout to stderr (`>&2`) instead of /dev/null.
  - The provenance lines stay on stdout.

- **C10 (architecture-report timeout rule).** Once the push ratchet is non-vacuous, the `--check` step does real prior-revision work: `git show` for every source plus `syntax(prior_sources)` and `exception_growth` (source_gate.py:44-60). The job's timeout is 15 min (ci.yml:32), and RUST-QUALITY.md:11 requires timeouts of at least twice the observed green maximum. Add to C14: record the push run's `architecture-report` duration with `gh run view` and confirm it is at most 7.5 min. Otherwise raise the timeout in the same commit.

- **C11 (commit B's title overclaims).** Scheduled ci.yml runs (ci.yml:7-11) still resolve `origin/slate` == HEAD, so they still compare HEAD with HEAD; §8 leaves that unchanged on purpose. Change the title from "never with itself" to "a push compares … with the previous pushed revision". State the schedule exception in B4's prose.

- **C12 (minor accuracy).**
  - §1a says `gate-b11.txt` "has two `test result` lines"; the whole file has 4 (`grep -c`). Only the part after `QUALITY_OK` has 2, as C10 correctly uses.
  - The reviewer's "62" versus 66 is confirmed as 66.
  - A4's "90 lines become about 81" is fine.

### Buildability of controls

- C1-C8, C11 and C13 are buildable as written, apart from C8's stale hash (C1 above).
- C9, C10 and C12 need the mutation leg to be idle; no cargo-mutants process is running now (`ps`).

### Ledgers

No missed ledger:
- test-inventory stores `function_sha256` only for test functions, and `gap_lock` is not one;
- review-mechanisms.json, owners.json and architecture-policy.json pin no `scripts/` or `.github/` path;
- mutation_owners.py and verification_plan.py CRITICAL_PREFIXES do not cover src/dst;
- no Python test registry exists.

The fixture_failpoints.rs line-count invariant belongs in the commit checklist (C6).

### Required checks

Only `rust` is touched. With `--locked` and tests_ran it becomes at least as strict, and the job id is unchanged. `architecture-report` is not required but becomes non-vacuous. Nothing is weakened in CI. The one reduction, the release gate's debug-assertion coverage, is local and is C8's decision.

**Verdict: ready-with-corrections.** Apply C2-C5, C9 and C11 to the plan text. Hold the push for C8's decision. Fix C1's stale facts before running C8.
