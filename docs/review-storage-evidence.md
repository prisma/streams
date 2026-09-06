# Storage remediation evidence

Each section distinguishes implementation/regressions from execution and external acceptance.

## R12 — persisted tails and cursors

Existing malformed tails now return storage corruption errors through handle opening, tail reads, durable absorption reads and fork initialization. Only missing tails initialize state. v2/v3 tails retain complete historical optional extensions; partial extensions, unknown flags/versions and inconsistent boundaries fail. Cursor values require exactly eight bytes.

Regressions: `storage_decode_tests::r12_supported_tail_versions_and_extensions`, `r12_cursor_requires_exact_width`, `r12_corrupt_tail_refuses_open_without_overwriting_records`. The integration fixture retains record and corrupt tail bytes exactly and ensures no handle is published.

Initial execution: `cargo test --locked --lib r12_ -- --nocapture` blocked by preinstalled Cargo 1.84.1 lacking edition2024. A current isolated toolchain is being provisioned; final execution results will be appended.
