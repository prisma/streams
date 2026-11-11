# Mutation outcome fixtures

Trimmed excerpts of real cargo-mutants 27.1.0 scenario logs, used by
`test_mutation_driver.py` (`VerdictEvidence`) to pin the driver's evidence rule
(gate decision of 2026-11-11, H1). Each keeps the mutant's header and its last
phase, with cargo's progress lines and passing tests dropped. Host paths are
replaced by `/home/dev`, `/work/streams` and `/tmp`.

| File | Mutant | cargo-mutants said | The driver says |
|---|---|---|---|
| `unviable-manifest-error.log` | `src/shard/billing_ops.rs:86:9` at df849d2a0 | unviable | incomplete: the temp copy lost `tools/quality-syntax/src` |
| `caught-manifest-error.log` | `src/shard/billing_ops.rs:33:9` at df849d2a0 | caught | incomplete: same |
| `unviable-parse-error.log` | `src/shard/transaction/maintenance.rs:20:13` | unviable | unviable: a parse error with no `error[E...]` code |
| `caught-not-found-panic.log` | `src/shard/seal/gc.rs:784:9` | caught | caught: its tests panic with an object store `NotFound` |
