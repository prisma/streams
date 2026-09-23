# Assumption ledger

Every check in `manifest.json` relies on the assumptions it names here
(roadmap §2.3, §7.7). Each entry records its scope, its origin, what enforces
it or is evidence for it, what would invalidate it, and its standing:
**established** (a production boundary or a cited contract enforces it) or
**unestablished** (the dependent result is a conditional design check).
`scripts/quality/formal.py check` rejects a manifest that names an ID not
defined here.

## Kani execution scope

### ASM-KANI-SCOPE

- **Scope:** every Kani obligation.
- **Statement:** a Kani harness checks one sequential execution of the named
  production functions under the Kani 0.68.0 / CBMC 6.11.0 semantics. It does
  not cover concurrency, panic unwinding or foreign functions. Kani's automatic
  checks (arithmetic overflow, bounds, unwinding) stay enabled. Loop bounds are
  set by `#[kani::unwind]`, and the unwinding assertions confirm they are
  sufficient.
- **Origin:** roadmap §1.3, §2.5; Kani *Rust Feature Support*.
- **Enforcement / evidence:** the driver treats an unwinding failure,
  unsupported construct or unsatisfied cover as incomplete, never as a pass.
- **Invalidation:** a Kani or CBMC upgrade; a harness that starts reaching
  async, atomic, FFI or panic-recovery code.
- **Standing:** established (tool contract).

### ASM-KANI-COMPILER

- **Scope:** every Kani obligation.
- **Statement:** Kani compiles the unchanged crate with its own dated nightly
  (`nightly-2026-08-21`, rustc 1.100.0-nightly), not the production 1.98.1
  compiler. The verified functions are plain integer, array and enum code whose
  semantics do not differ between these compilers. The production build and its
  tests remain separate, required evidence.
- **Origin:** roadmap §2.9, §7.2.
- **Enforcement / evidence:** `quality-tools.toml` `[formal]` pins both. The
  ordinary gates still compile and test the same functions with 1.98.1.
- **Invalidation:** a Kani release pinning another nightly; a harnessed
  function adopting a nightly-sensitive feature.
- **Standing:** established for the current harnesses.

## Dependency contracts used by the TLA+ models

The TLA+ groups add their entries below this line. Each model README maps its
actions to these contracts.
