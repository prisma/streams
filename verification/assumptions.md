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

## Numeric domains

### ASM-OFFSET-DOMAIN

- **Scope:** KANI-001, KANI-002, KANI-003.
- **Statement:** an offset token is `(epoch: u32, rawSeq: u64)`, where the epoch
  is the segment ordinal and `rawSeq` is the scan index `next`. Every value of
  both is admitted. The unused `in_block` word and the two pad bits are written
  as zero. The parser ignores them. Canonical-token (alias) rejection is
  KANI-004, which is still planned.
- **Origin:** `src/offsets.rs`, `docs/PER-KEY-ORDERING.md` §3, `src/segmap.rs`
  (ordinals allocated up to `u32::MAX` with `checked_add` on split).
- **Enforcement / evidence:** the `Offset` field is private and
  `Offset::before` is total. The harnesses quantify over the full domain.
- **Invalidation:** a wider segment ordinal, a nonzero `in_block`, or a change
  to the token layout.
- **Standing:** established.

### ASM-READ-NOW-SENTINEL

- **Scope:** KANI-002 (a tracked domain question, not a verified property).
- **Statement:** the read planner uses scan index `u64::MAX` as its in-band "now"
  sentinel (`ReadCommand::position_in`, `read_request.rs`, `read_remote.rs`). The
  KANI-002 harness proves the codec, not this caller convention.
- **Origin:** source inspection during KANI-002.
- **Enforcement / evidence:** none beyond the unreachability of a real
  `2^64 - 1`-record stream.
- **Invalidation:** a separate `Now` carried through the planner, which removes
  this entry.
- **Standing:** unestablished; owner decision pending. See
  `regressions/KANI-002/README.md`.

## Dependency contracts used by the TLA+ models

The TLA+ groups add their entries below this line. Each model README maps its
actions to these contracts.
