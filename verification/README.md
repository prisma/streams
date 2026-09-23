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
| `receipts/<ID>.json` | The compact receipt of the last complete, matching run of each obligation |
| `fixtures/` | Driver self-test inputs; not models of Prisma Streams |
| `src/**/proofs.rs` | Kani harnesses, colocated with the production owner as `#[cfg(kani)] mod proofs;` |
| `../scripts/quality/formal.py` | The driver: validation, selection, execution, receipts, self-test |

## Tools

Tool versions and checksums are pinned in `quality-tools.toml` under `[formal]`.
To install them:

```bash
python3 scripts/install-formal-tools.py
```

That installs `target/quality-tools/tla2tools.jar` (TLA+ tools 1.7.4, TLC
2.19; Java 11 or newer is required) and Kani 0.68.0 with CBMC 6.11.0. Kani
compiles with its own nightly (`nightly-2026-08-21`), which is a separate
analysis configuration. Ordinary builds and every production gate keep the
root `rust-toolchain.toml` pin. A harness is `#[cfg(kani)]` code, so the
production compiler never builds it. `build.rs` declares the cfg name so that
ordinary builds check it.

## Commands

```bash
python3 scripts/quality/formal.py check              # manifest, files, harness discovery, config/property agreement
python3 scripts/quality/formal.py check --fresh      # also require a receipt for current inputs
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

A timeout, a parse or configuration error, a deadlock, an unwinding failure, an
unsatisfied cover, zero discovered harnesses, or a control that fails for a
different reason never passes. `self-test` runs real fixtures of each of those
cases and requires the driver to reject them.

A **receipt** records the input digest over the obligation's source owners,
proofs or models and controls, its own manifest entry, the driver and the
`[formal]` tool pins. Kani receipts also cover the lockfile, the Cargo
manifest, the build script and the root toolchain.
Changing any of those inputs makes the receipt stale (`check` lists it;
`check --fresh` fails), until `run --record` produces a new receipt.

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
