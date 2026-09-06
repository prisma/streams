# R07 consumer application ownership

`application::consumer` and its focused deletion/delivery modules own source authorization, active generation resolution,
configuration, pull/settle, DLQ handoff, and resumable deletion. Its contexts
retain the exact descriptor epoch, consumer name/generation and an `Arc` to the
explicit registry/shard/peer/key/append capabilities that authorized them.
The owner facade is747 lines, deletion400 and delivery587; none exceeds the
1,000-line module target.
`ConsumerAccess` distinguishes account authority from deployment authority;
capability carriers and preflights cannot enter consumer dispatch.

The service returns typed configuration, delivery, settlement and deletion
outcomes; transport adapters parse JSON/headers and render those outcomes.
`DeletionDebt` carries the pinned project/stream, epoch, consumer and generation
when cleanup cannot finish. The existing durable `Deleting` parent state and
bounded row/byte/segment fan-out remain canonical. Stale version DELETE still
compares the stream epoch before validating the old key, returns an untouched
204 for a replaced target, and never finalizes using an unverified map.

DLQ writes now call `AppendService` with a typed command, the configured target
epoch, canonical product request hash and stable producer identity. Source
settlement follows a durable append outcome and counts the actual generation-
fenced source acknowledgement. Queue cursor/remote-tail storage failures no
longer pretend to be empty state. No new network boundary was introduced.

Executed locally with the isolated Rust1.98 toolchain and locked dependencies:

- `cargo check --offline --lib --tests`: pass.
- `cargo test --offline --lib dst::dst_tests::consumer_ -- --test-threads=2`:
  **27 passed,0 failed,0 ignored** (1.92 seconds).
- New controlled regression
  `r07_failed_dlq_commit_retains_source_lease_and_exact_retry_settles_once`
  fires the target committer's identity-specific failure, proves source lease
  preservation and zero target durability, retries the same lease to one durable
  DLQ record and one source settlement, then proves a stale retry cannot append
  another DLQ record.
- Existing focused legs cover failed source queue writes/settlements, bounded
  split cleanup failures and retry, same-group ordering, generation fences,
  ownership moves, stream recreation with a different key, partial map refresh,
  pending topology, lineage drain, and shared-key DLQ validation.

Watch ownership/admission evidence is recorded with the companion R07 watch
change. Scenario mapping validity does not certify unavailable fleet/capacity
campaigns; those outcomes remain explicit in final review evidence.
