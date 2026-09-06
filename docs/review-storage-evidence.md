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

## R01 — cryptographic invocation safety

New v4/v5 frames use AES-256-GCM-SIV, segment-separated HKDF keys and stored OS-random nonces. Retained v2/v3 frames use their unchanged legacy reader. `docs/crypto-frame-v4.md` specifies the exact format, invocation domains, coordinated reader/writer migration, bounds and outstanding external review/deployment inventory.

Actual Rust production-source regressions: `crypto::invocation_tests::r01_rfc8452_aes256_empty_plaintext_vector`, `r01_segments_and_reused_offsets_have_safe_invocation_domains`, `r01_retained_legacy_frames_and_new_versions_read_together`, `r01_frame_golden_header_and_ciphertext`; existing compression and authentication round trips updated for fresh encryption invocations. Direct Rust 1.98.1 test harness importing the unmodified repository `src/crypto.rs` and `src/tenant.rs`, linking lockfile-built dependencies, executed **10 crypto tests passed**. This bypassed unrelated concurrent descriptor-fixture compilation failures and is actual Rust execution, not a cryptographic model. Complete repository crypto tests will be rerun after concurrent integration.

No independent cryptographic approval, live split/merge/fork/fencing campaign or operated-ciphertext inventory was performed; those external acceptance items remain explicit. The fixed-nonce integration vector pins the independently specified header plus RustCrypto ciphertext; the primitive is separately checked against the published RFC vector.

## R03 — typed commands and durability effects

`CommitOp` now distinguishes data append, control-only logical close and seal-fence commands. `AppendReq` no longer contains the ignored seal-fence mode; its final-record behavior is an explicit `AppendFinish`. Production fence/scaler-close submission no longer manufactures payload, encryption keys, or usage counters. Pure `decide_producer`/`seal_authorized` functions retain ordered duplicate/fencing checks. A `DurableEffects` plan owns replies, queue acknowledgements, tail/ring publication, absorber signals, touches and usage byte counters; counters now publish only at durable dispatch. The group still owns one WriteBatch and provisional overlay; failed groups discard all planned effects.

Regressions: `r03_producer_decision_keeps_duplicate_before_close_and_new_epoch_fence`; `r03_failed_group_discards_close_and_fence_effects_together`; `r03_close_and_fence_wait_for_write_remote_durability_and_dispatch`. The last test independently holds pre-write acceptance, an actual WAL object-store PUT, and callback dispatch. It observes an applied close with no remotely durable tail, then a remotely durable tail with no replies, releases dispatch, and reopens to verify the closed tail. It is a true remote-durability mechanism test on the pinned backend with an in-memory fault store, distinct from historical dispatch-only scenarios.

Combined Rust 1.98.1 command `cargo test --locked --lib -- r01_ r03_ r12_ r13_ r14_ r23_ --nocapture`: **15 passed**, 0 failed, 768 filtered, 0.58s execution. The first combined run exposed two test-fixture mistakes (R12 corruption seed was not yet remote-durable; R03 cleanup closed an already closed DB), both corrected before this passing run. No product failure was hidden by weakening the durability assertions.

## R09 — billing/history controller continuation (storage portion)

Billing discovery visits at most four rotating engines, 64 dirty rows per engine and 32 month-final rows per segment. Dirty and history cursors use exclusive storage seeks. Partial final acknowledgements delete exact published keys while retaining the dirty marker until all finals were emitted. Residency probes read at most one row per outbox index. Error/cancellation leaves durable debt discoverable. Optional-mode read batches now have a Drop guard which requeues them if cancellation occurs at any await before ledger acceptance.

History restart discovery reads 256 dirty identities per page and continues subsequent ticks from the stored-in-owner cursor. Pending work is capped at 8,192 streams; excess signals rely on their durable dirty markers. Bounded due selection rotates in hash order so an indefinitely hot prefix cannot starve eligible successors. The redundant whole-resident backlog materialization was removed.

Regressions: `r09_cancelled_optional_read_drain_requeues_owned_batches`, `r09_budget_exhaustion_on_first_engine_still_rotates_every_engine`, `r09_hot_prefix_cannot_starve_other_due_streams`, `r09_discovery_pages_progress_without_exceeding_pending_capacity`, `r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt`. These measure finite discovery/read work and exact retained outbox keys. Cloud request/latency comparisons and noisy-neighbor capacity campaign remain pending.

R09 execution: the integrated Rust 1.98.1 test binary (793 tests compiled) ran `r09_`: **5 passed**, 0 failed, 788 filtered, 0.03s. A preceding compile exhausted disk space in generated incremental artifacts; after the composition owner removed only those artifacts, the rebuilt binary passed. Work counts and pending capacity are bounded; individual storage request latency remains governed by the backend and controller cancellation.

## Implementation commit references

- R12: `78c5940` (remote fixture correction included with R03 tests).
- R13: `d985f00`.
- R14: `814d3a5`.
- R23: `320478d`.
- R01: `031ae5b`.
- R03: `fd1706e`.

All source commits are reviewable changes; the external acceptance conditions above are not marked as passed.
