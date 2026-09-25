# Formal verification

This directory holds the implemented part of
[the formal-verification roadmap](../docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md).
The roadmap is the complete planned inventory. `manifest.json` lists only
obligations that have actually been implemented, with their checks, bounds,
assumptions and status. The claims are narrow: each obligation states what was
checked, over which inputs or model instance, and under which assumptions. None
of it says that Prisma Streams as a whole is verified.

| Path | Contents |
|---|---|
| `manifest.json` | Implemented obligations: owners, checks, expected verdicts, status (roadmap §7.3) |
| `assumptions.md` | Assumption ledger: every dependency or domain assumption a check relies on (roadmap §7.7) |
| `tla/<group>/` | TLA+ models, TLC configurations, negative-control variants, witnesses and each group's mapping README |
| `regressions/<ID>/` | Minimized counterexamples and seeds, with replay instructions and the permanent regression tests |
| `receipts/<ID>.json` | The compact receipt of the last complete, matching run of each obligation (see [Receipts](#receipts)) |
| `fixtures/` | Driver self-test inputs; not models of Prisma Streams |
| `src/**/proofs.rs` | Kani harnesses, colocated with the production owner as `#[cfg(kani)] mod proofs;` |
| `../scripts/quality/formal.py` | The driver: validation, selection, execution, receipts, self-test |

## Setting up the toolchain

The tools are pinned in `quality-tools.toml` (under `[formal]`) and installed by
`scripts/install-formal-tools.py`, all under `target/quality-tools/`. Nothing is
installed globally.

### Prerequisites

- Java 11 or newer, needed by TLC. Check with `java -version`.
- rustup. The repo's `rust-toolchain.toml` pins the build toolchain (1.98.1).
  Kani also installs its own dated nightly (`nightly-2026-08-21`) through
  rustup. That nightly is used only for the proofs; normal builds keep the
  pinned 1.98.1.
- Python 3.11 or newer, because the driver uses `tomllib`.
- Network access for the first install.
- A supported host: macOS on arm64 or x86_64, or Linux on x86_64 or aarch64.

### Install

Run from the repository root:

```bash
python3 scripts/install-formal-tools.py
```

This installs:

- `target/quality-tools/tla2tools.jar`: TLA+ tools 1.7.4 (TLC 2.19), checked
  against its pinned sha256;
- Kani 0.68.0, built from its pinned crates.io release and set up from a
  checksum-verified bundle, with CBMC 6.11.0;
- Kani's nightly compiler, installed through rustup.

Then put the tool binaries on your `PATH` for the session:

```bash
export PATH="$PWD/target/quality-tools/bin:$HOME/.cargo/bin:$PATH"
```

A harness is `#[cfg(kani)]` code, so the production compiler never builds it,
and Kani's nightly is a separate analysis configuration: ordinary builds and
every production gate keep the root `rust-toolchain.toml` pin. `build.rs`
declares the cfg name so that ordinary builds check it.

### Verify the setup

```bash
python3 scripts/quality/formal.py self-test
python3 scripts/quality/formal.py check
```

`check` validates the manifest, the models and configurations, and the
receipts; stale receipts are reported without failing. `check --fresh` also
fails on stale receipts, as the release gate requires.

### Run the obligations

Run one obligation:

```bash
python3 scripts/quality/formal.py run --id TLA-016 --out target/formal/tla-016
```

Run only what a diff affects:

```bash
python3 scripts/quality/formal.py run --changed-from origin/slate --out target/formal/changed
```

To write or refresh `verification/receipts/<ID>.json`, add `--record`. Do that
only on a clean tree that will not change during the run: the driver fails a
run whose inputs change mid-run.

### Practical notes

- Do not set `RUSTUP_TOOLCHAIN` for Kani runs. Kani selects its own nightly,
  and forcing 1.98.1 breaks it.
- Parallel runs are fine if each has its own `--out` directory. The driver
  gives every TLC process its own temporary directory.
- Git worktrees do not share `target/`. In a new worktree, link the tools
  before running:

  ```bash
  mkdir -p target && ln -s /path/to/main/checkout/target/quality-tools target/quality-tools
  ```

- Cost: on a busy 8-core laptop the full set takes about 1.7 hours of Kani and
  3-6 hours of TLC. The slowest single checks are KANI-001 (about 30 minutes)
  and the TLA-016 and TLA-018 baselines (15-30 minutes each).
- CI: the `formal` job in `.github/workflows/rust-quality.yml` installs the
  same pins with the same script and runs the affected obligations across six
  shards.

For more detail on the three enforcement levels and the receipt format, see
the sections below; for each model group, the READMEs under `tla/`; and for
the plan, the roadmap `docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md` §0.

## Commands

```bash
python3 scripts/quality/formal.py check              # manifest, files, harness discovery, config/property agreement, receipt validity
python3 scripts/quality/formal.py check --fresh      # also require every claimed result's receipt to match current inputs
python3 scripts/quality/formal.py select --base origin/slate
python3 scripts/quality/formal.py run --id KANI-039  # one obligation, every role
python3 scripts/quality/formal.py run --changed-from origin/slate --record
python3 scripts/quality/formal.py self-test          # the driver must reject bad runs
```

`run` reconciles each check's expected verdict with the actual one:

- A **baseline** must pass. For TLC that means a complete search with no error
  and no states left on the queue. For Kani, every property must hold and
  every `kani::cover!` must be satisfied.
- A **negative control** must fail on its named property. For TLC, the control
  is an `MC_*` module that extends the unmodified specification and substitutes
  one operator. For Kani, it is a patch under `kani/controls/` that the driver
  applies to a scratch copy of the tree; the patch never touches the working
  tree or production code. The driver records the baseline and mutated hashes
  of every patched file.
- A **witness** is a `Witness_*` invariant, stating that a behaviour is *not*
  reachable. It must be violated on the unmodified model, which shows that the
  behaviour is reachable.
- A **known defect** is a configuration of the unmodified model that still
  reproduces a confirmed, open production defect: it must keep violating its
  named property. Only an obligation with status `counterexample` may carry
  one. When the defect is fixed, the check becomes a baseline and the
  pre-fix behaviour becomes a negative control.

A timeout, a parse or configuration error, a deadlock, an unwinding failure, an
unsatisfied cover, zero discovered harnesses, or a control that fails for a
different reason never passes. `self-test` runs real fixtures of each of those
cases and requires the driver to reject them.

Every check runs in its own process group. Each TLC run has its own metadir
and, inside it, its own `java.io.tmpdir`, where TLC extracts its standard
modules; concurrent runs that shared one once failed with "Could not parse
module TLC". A timeout, an interrupt or a SIGTERM kills the check's whole
process group and removes its scratch directories. `self-test` also runs TLC
fixtures concurrently, and times out and cancels a search, requiring no TLC
process or scratch directory to survive.

## Receipts

A receipt is written only by `run --record`, and only for a complete run of
every check of the obligation in which every check matched. It records, in
schema 2:

- `inputs`: what the verdicts rest on, hashed per input. `files` maps each
  digested file to its sha256: the obligation's source owners, proofs or
  models, configurations and control patches, the driver, `Cargo.lock` and
  `Cargo.toml` (the TLA+ models abstract the pinned SlateDB and object_store
  contracts; Kani compiles against them), and for Kani also `build.rs` and
  `rust-toolchain.toml`. `assumptions` maps each assumption the obligation
  names to the sha256 of its exact `### ASM-...` entry in `assumptions.md`
  (heading to the next heading of level 1 to 3). `manifest_entry` is the
  sha256 of the obligation's own manifest entry, and `pins` holds the
  `[formal]` and `[slatedb]` sections of `quality-tools.toml`.
  `inputs_sha256` is the sha256 of that map.
- `complete: true`, the revision and dirty-tree flag, and the tool versions.
- One entry per manifest check: its role, expected verdict string, actual
  verdict, time, the sha256 of the retained log, and what re-judges the
  verdict: for TLC the violated property (null for a pass) and the state
  counts, for Kani the failed assertion descriptions, the covers and, for a
  control, the baseline and mutated hashes of the patched file.

The driver takes the input snapshot before the first check and again after
each one. If anything changed, the run fails and nothing is recorded, so a
receipt never names inputs that an earlier check did not analyse.

Raw logs stay local, under `--out` (`target/formal/logs/<check id>.log`, with
`/` written as `__`); the receipt carries each log's sha256 so a disputed
result can be checked against the retained log. The repository's hold on
raw-evidence upload applies: neither the driver nor the CI job uploads logs.

### Validity and freshness

`check` judges every receipt against the manifest. A receipt is **invalid**
(`check` always fails) when it is not JSON, has an unsupported schema, names
another obligation or no implemented one, is not of a complete run, records
anything but exactly the manifest's checks (an omission, a duplicate or an
extra), records a role or expected string that differs from the manifest, or
records a verdict the driver would not have accepted: a baseline must have
passed; a negative control, witness or known defect must have violated or
failed on its named property. A passing obligation may not carry a known
defect, and a counterexample's receipt must reproduce at least one.

Which statuses need one: `pass-with-recorded-scope` and `counterexample`
claim a result, so each must have a valid receipt; a missing one is invalid.
`implemented-unchecked`, `incomplete` and `unsupported` claim nothing, so no
receipt is required, but one that exists must still be valid, and its
freshness is not checked.

A valid receipt is **stale** when its input digest differs from the current
inputs; `check` names the inputs that changed. Schema 1 receipts, recorded
before the input map, the log hashes and the assumption and dependency
digest existed, still validate structurally but are always stale.

### Three levels of enforcement

1. **Every commit** (`scripts/quality.sh`, the `quality` CI job): `check`.
   Manifest and receipt validity fail; staleness is informational, because
   re-running the affected models takes hours and belongs to the next level.
2. **Every change** (the `formal` CI job): the self-test, then `run
   --changed-from <base>` executes every check of every obligation the diff
   can affect. That job is the gate for a change's formal evidence.
   `select` treats an edit to the manifest, the pins or the driver as
   affecting everything, an edit to `Cargo.lock` or `Cargo.toml` as
   affecting every obligation, and an edit to `assumptions.md` as affecting
   exactly the obligations naming an entry whose text differs from the base.
3. **Every release** (`scripts/release-gate.sh`, run by
   `scripts/rc-certify.sh`): `check --fresh`. A release needs a current,
   valid receipt for every claimed result, recorded with `run --record`.

## Status values

As in roadmap §2.8: `implemented-unchecked`, `pass-with-recorded-scope`,
`counterexample`, `incomplete`, `unsupported`. `planned` items stay in the
roadmap and never appear in the manifest.

## Adding an obligation

1. Read the roadmap entry and its rules (§2), and resolve the current canonical
   owner.
2. Write the harness or model against the production code, with no copied
   algorithm. Include reachability covers or witnesses.
3. Add at least one negative control that the baseline must survive and the
   control must fail on its named property.
4. Add the manifest entry, and any new assumptions to `assumptions.md`.
5. Run `formal.py run --id <ID> --record`, then commit the receipt with the
   change.
6. Mark the roadmap entry with its status, scope and findings.
