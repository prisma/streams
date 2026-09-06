# R11 test ownership remediation

The extraction baseline is commit `b612fbf`, after the R03/R04/R10 fixture
adaptations. The monolithic `src/dst/dst_tests.rs` contained 381 tests; the
recursive DST inventory contained 410, including existing trace-store tests
and the R15/R16/R17 regression modules. None were ignored.

The monolith is now a 206-line integration-module registry. Its tests moved into
57 contract modules and two model modules next to their owners, with seven
small fixture capabilities. The largest generated Rust module is 942 lines.
Two unused helpers (`m`, `skey2`) were deleted. No test assertion, seed,
failpoint schedule, coverage requirement, or scenario ID was deleted.

Evidence is machine-readable:

- `docs/refactor/test-inventory-before.json`: all 410 original obligations.
- `docs/refactor/test-inventory.json`: their current owner paths and unchanged
  function/configuration evidence.
- `docs/refactor/test-relocations.json`: 381 exact old/new test symbols.
- `src/dst/tests/README.md`: contract and fixture ownership map.

Executed checks:

- `python3 scripts/test-inventory.py --compare docs/refactor/test-inventory-before.json --adaptations docs/refactor/test-adaptations.json`:
  all 410 names, attributes, mechanism/configuration lines and scenario
  mappings unchanged. 408 function hashes match directly; one R06 API
  adaptation passes `state.read_service()` to `refresh_transition` without
  changing its inputs/assertions. Its exact old/new hashes are recorded in
  `test-adaptations.json`. A second exact R15 adaptation records commit
  `012ba73`, which strengthens the body-poll regression with valid and expired
  signed-watch cases. Any other change still fails the comparison.
  Relative fixture includes resolve to the same repository files and contents.
- `python3 scripts/test-inventory.py --check`: pass; this is now a CI/local
  gate, including negative controls for missing/changed obligations.
- `python3 scripts/scenario-map-report.py --self-test` and `--check`: pass;
  all 189 scenario IDs remain, with the prior 138 mapped/51 unmapped split.
- `bash scripts/multitenancy-audit.sh`: `MT_AUDIT_OK`; DST discovery is now
  recursive, and only the moved DST fingerprint paths were updated.

Exact capacity selectors in CI, `scripts/gate.sh`, and
`scripts/release-gate.sh` were updated together. Release provenance now counts
the recursive inventory instead of grepping a single source file. The split
does not itself claim that unavailable full-capacity, cloud, or fleet legs
were executed; final program evidence records those results separately.

Rust validation used Rust1.98.0 with locked dependencies offline:
`cargo clippy --offline --lib --tests --message-format=json` compiled successfully.
The rebuilt793-test library binary ran all9 relocated oracle tests and all6
fault-substrate tests:15 passed,0 failed,0 ignored. The latter includes the
pure-seed placement and applied lost-response coverage checks.

The14 existing monolith clippy fingerprints were relocated to14 exact new
module fingerprints, with unused `m`/`skey2` debt removed; no blanket allowance
was added. A newly surfaced helper `while_let_loop` warning was fixed without
changing any test. Current-toolchain warnings in the existing security loop
and unused fault-profile method remain for the final R24 gate audit; they are
not added to this finding's allowance list.
