# Storage remediation evidence

Each section distinguishes implementation/regressions from execution and external acceptance.

## R12 — persisted tails and cursors

Existing malformed tails now return storage corruption errors through handle opening, tail reads, durable absorption reads and fork initialization. Only missing tails initialize state. v2/v3 tails retain complete historical optional extensions; partial extensions, unknown flags/versions and inconsistent boundaries fail. Cursor values require exactly eight bytes.

Regressions: `storage_decode_tests::r12_supported_tail_versions_and_extensions`, `r12_cursor_requires_exact_width`, `r12_corrupt_tail_refuses_open_without_overwriting_records`. The integration fixture retains record and corrupt tail bytes exactly and ensures no handle is published.

Initial execution: `cargo test --locked --lib r12_ -- --nocapture` blocked by preinstalled Cargo 1.84.1 lacking edition2024. A current isolated toolchain is being provisioned; final execution results will be appended.

## R13 — shard billing reads

Accounting reads return `Result<Option<SegmentBillingMetaV1>>`, validating the persisted version, identity, month and decimal byte-time. The committer loads each required row before staging any group effect, then shares that validated snapshot in its overlay. A failed read/decode rejects every response in the group and commits nothing. Drain and sweep callers retain dirty work and log failures.

Regression: `billing_read_tests::r13_failed_accounting_reads_preserve_group_and_newer_dirty_version` injects unavailable/corrupt metadata for usage acknowledgement, logical close and retention, alongside a reply-bearing queue operation. It checks exact metadata, dirty watermark, month-final and record bytes and rejects success. A recovered version-7 acknowledgement preserves dirty version 8.

## R14 — rollup reads and checkpoints

Rollup staging functions now propagate required read/decode errors before committing the page or its ledger/source cursor. Financial point queries and segment scans expose `Result` to callers. Dedupe watermarks require exactly eight bytes. Month-close decoding, account/key validation and oldest-month checkpoints fail explicitly rather than skipping input or inventing a month/segment. Decimal financial fields validate at ingestion and persisted-read boundaries (historical empty decimal zero remains supported).

Regressions: `accounting_failure_tests::r14_required_read_failures_leave_every_row_and_checkpoint_unchanged` fails each source/month/name/project/cursor read and compares the entire stored key/value set, then replays successfully with exact totals. `r14_corrupt_watermarks_rows_and_close_keys_block_progress` checks short/oversized watermarks, malformed rows and invalid segment keys without financial/checkpoint mutation. Existing multi-segment monthly carry tests are retained.

R14 execution: Rust 1.98.1, `cargo test --locked --lib r14_ -- --nocapture`: **2 passed**, 0 failed, 761 filtered (local in-memory pinned SlateDB). This is repository-boundary injection, not a remote-store fault campaign.

## R23 — seek month-close pages

Both close passes now pass an exclusive suffix lower bound to pinned SlateDB `scan_prefix`, which enforces the prefix upper bound. Persisted cursors outside the expected prefix are errors. Row and byte chunk limits remain, with cursor/effects in the same batch. `close_rows_visited()` records iterator work.

Regressions: `close_seek_tests::r23_seek_boundaries_are_exclusive_and_prefix_bounded`; `r23_close_visits_scale_linearly_across_chunk_restart` interrupts after the committed 1,000-row chunk, recreates the rollup owner over persisted state, and closes 1,001/2,002 segments contributing to one monthly row. It requires exactly N+1 visits and exact summed byte-time. It measures iterator visits, not billed cloud GETs.

R23 execution: `cargo test --locked --lib r23_ -- --nocapture`: **2 passed**, 0 failed, 769 filtered, 1.28s execution. Measured 1,002 and 2,003 row visits for 1,001 and 2,002 segments, including owner restart after the committed first chunk.
