# Application remediation evidence

## R15 — request body admission

`product_auth_gate` now returns an explicit authentication state. A capability carrier is distinct from a verified principal and never supplies project admission or audit attribution. The HTTP adapter only collects bodies for PUT and POST; OPTIONS, GET, HEAD, DELETE and unsupported methods discard bodies without polling. Existing authenticated mutation body limits and buffered-byte accounting remain in place.

Regression: `r15_bodyless_and_unauthorized_requests_never_poll_the_body` drives the real HTTP entry function with a pending body that records each poll and declares an oversized length. It covers preflight, missing bearer on read/write/watch, malformed and expired/forged capability carriers in both header and query forms. Every result must arrive within two seconds with zero body polls and CORS headers.

Executed on Rust 1.98.1: both real-entry security tests pass. The ambient Cargo 1.84.1 is not used; it cannot parse this repository's edition 2024 manifest.

## R16 — watch refusal indistinguishability

Key/query diagnostics now occur only after a capability or encryption key verifies. Missing/corrupt/deleted descriptors and failed verification share the same un-attributed 403 response. Capability project claims remain lookup hints only.

Regression: `r16_unauthorized_watch_shapes_are_independent_of_existence` compares status, all headers and complete structured body across missing/live/initializing/deleted descriptors, three malformed/expired/forged carriers, malformed/short/valid keys and valid/unknown/duplicate/malformed query parameters. All 144 requests also use the R15 never-finishing oversized body sentinel.

Executed both security tests with the Rust 1.98.1 debug test binary: `target/debug/deps/streams_slate-17369d5781ba9293 review_security --nocapture`: 2 passed, 0 failed. Local loopback binding required sandbox escalation; the first sandbox-restricted attempt failed at socket binding before the test request, not in product behavior. Full conformance and production auth-mode security matrix remain part of the final repository gates.

## R08 — conditional registry updates

All existing-descriptor update paths now construct an opaque conditional-update token; a missing or empty ETag is an explicit storage error, including recreation and test-only update paths. No mutation may select `PutMode::Overwrite`. Mutation retries now recognize typed object-store precondition conflicts; invalid data, missing conditional tokens, ordinary I/O failure, and ambiguous completion return immediately. Test-injected conflicts use the same typed precondition error as production.

Regressions: `r08_missing_conditional_token_fails_closed`, `r08_only_precondition_conflicts_are_retried`. The latter proves that changing human text to contain “precondition conflict” does not make an unclassified error retryable. Further application typed-error migration follows with lifecycle ownership.

## R04 — immutable validated descriptors

`PersistedDescriptor` remains the JSON/storage DTO. `TryFrom<PersistedDescriptor> for StreamDesc` is the single serving conversion: StreamDesc exposes immutable fields, a validated fixed-width epoch, and explicit lifecycle variants. Creation, recreation, decode, and registry mutation all validate before caching or writing. Mutations edit an attempt-local DTO and cannot mutate a serving snapshot. Existing DTO golden JSON remains byte-identical, including the synthetic all-fields fixture; invalid lifecycle combinations in that synthetic fixture are now correctly refused for serving.

Topology validation distinguishes absent implicit topology from an empty/invalid explicit map, validates allocators/ranges/references/transition shapes, and accepts sealed terminal coverage and legitimately pruned predecessor IDs. Unknown segment IDs return no route; public/internal callers reject them before engine access. An explicit map cannot fall back to invented implicit segment zero. Split rejects invalid points and exhausted IDs before modifying a parent.

Executed debug tests: registry filter 22 passed (including six SSE registry tests); segmap filter 7 passed; security filter 2 passed after the serving-state conversion. New tests `r04_invalid_descriptors_cannot_reach_storage` prove malformed identity/topology/lifecycle fails before object creation; `r04_valid_transition_and_sealed_predecessor_snapshots` covers pending split, split, merge, and sealed predecessors. Golden descriptor tests: 5 passed. They identified a defaults fixture that intentionally omits layout_version; it now decodes as the persisted DTO, preserving its original purpose independently from serving validation.

## R06 — canonical read executor and topology progress

The actual history/postings/tail merge and frame decoding now live in `application::read`. `ReadPlan` binds a physical segment, epoch, selector, visibility and byte budget; `ReadPage` explicitly carries durable/applied watermarks and consumed progress. `ReadTopology` owns selector ordering and successor continuation, shared by HTTP replay and SSE lineage construction. Empty filtered pages advance by scanned position; durable resume clamps applied suffixes. The original bounded history executor remains intact.

`ReadService` owns only query capabilities and is cached once per runtime. Fork-chain validation, ancestor key checks and stitched execution moved into it. SSE feed/source has no HTTP or product import; owner resolution matches typed shard errors. Incarnation-bound remote pages and canonical path encoding moved to the peer read adapter. No internal response is encoded to call the reader.

Rust 1.98.1 checks: typed read progress tests 2 passed; product read/scan scenarios 6 passed; fork/livefeed/cold-history/remote-predecessor/split/merge scenarios 11 passed; external-completion refresh 1 passed; signed watch/bodyless security 2 passed; applied suffix rollback 1 passed; source cursor mapping 2 passed; internal target binding 6 passed. Logs: `/private/tmp/r06-tests.log`, `/private/tmp/r06-final-tests.log`. The raw and product protocol adapters still own public cursor decoding and rendering; full final conformance and cold/hot workload comparison are recorded in the repository-wide gate evidence.

### R04 fixture follow-up

Four seal coordination/recovery fixtures now allocate `seal_gen_counter` before installing a claim, matching the validated durable descriptor contract instead of inserting an impossible claim generation. The unchanged assertions pass: seal coordination 8, fencing 6, recovery 7, incarnation 4, and convergence 6.

## R02 — typed append application contract

`AppendService` owns authorization-bound append preparation, validation, quota and memory admission, routing, topology retry, producer decisions, shard submission, sealing and TTL/watch side effects through explicit capabilities. `AppendCommand`, `AppendOutcome`, and typed `AppendFailure` replace raw HTTP requests and response parsing between the standards/product/consumer surfaces. The product renderer signs the exact committed or duplicate offsets returned by the owner; missing headers can no longer fabricate offset zero or a successful unsigned cursor. Final-seal retry disposition matches error variants, never display text. A command pins its initial incarnation through refresh/retry, and consumer DLQ commands additionally fence the configured target incarnation.

The old HTTP append implementation and product response translator were removed. Pure producer parsing and product request hashing have a single owner, shared with consumer DLQ delivery. The raw adapter authorizes before polling its body and transfers its buffer guard at shard admission. Product body buffering retains its existing authorized entry guard; the refactor adds no request-body copy.

Validation: two direct application tests prove duplicate acknowledgements retain original positions after later writes and that a same-name recreation cannot accept a stale target command. Two boundary tests prove disposition is independent of display text and producer parsing rejects incomplete/reserved/malformed values. Existing protocol tests pass: producer 8, producer handoff 3, seal coordination 8, fencing 6, recovery 7, incarnation 4, convergence 6, quotas 9, memory admission 5, maintenance admission 9, and security refusal/body-poll 2. Logs: `/private/tmp/r02-integration-tests-final.log` and `/private/tmp/r02-integration-tests.log`.

### R06 completion — replay, long-poll and scan ownership

`ReadCommand` binds the validated descriptor, incarnation, selector, continuation, visibility and byte/wait budgets. `ReadService::execute_read` now owns replay and long-poll topology selection, bounded same-incarnation refresh, seal-gap classification, local/remote resolution and wait registration. `execute_scan` owns frozen durable segment snapshots and snapshot-bounded consumed progress. `ReadOutcome` carries mandatory next/durable positions, pending-record index and explicit data/snapshot/timeout/head/handoff outcomes. Raw and product renderers consume these facts directly; the product read/scan response decoders and cursor-zero fallback were deleted, along with the old single/fork/lineage HTTP execution paths.

Peer replay pages now have a bounded typed wire DTO, mandatory next position, incarnation verification and at most one trusted ownership redirect. Scan and SSE reuse the canonical segment `ReadPlan` through the existing typed span protocol. Internal pages do not meter reads; the public delivery adapter charges the actual payload once. The low-level read phases still use the same cached postings reader and bounded parallel history fetches; extraction adds no per-record serialization or copying on local application boundaries.

New tests prove (1) an empty filtered page advances to exactly the same consumed position in the application, raw and product surfaces, while a peer page missing its next position fails decoding; (2) a refresh never adopts a same-name replacement incarnation; and (3) an actual two-owner replay and scan return each record exactly once and only the public coordinator meters them. Existing read obligations pass: product 6, raw/seal-gap 10, applied/rollback 3, billing matrix 1, history-SSE 11, livefeed ownership 7, source swaps 13, basics 14, tails 10, cold history/cache 4, fork lifecycle 9, SSE delivery 4, and tenant lineage 6. Two old renderer test names remain traceable with their setup adapted to typed errors. Evidence logs: `/private/tmp/r06-final-tests.log`, `/private/tmp/r06-request-tests-b.log`, and `/private/tmp/r06-request-tests.log`. Local before/after performance measurements are recorded separately; these correctness results alone do not claim a latency improvement.

## R09 — cancellation-safe event drains

Ops and audit drain batches now retain ownership until their durable append succeeds. Dropping an active drain, a serialization error, or an append error restores the original events and IDs ahead of newer queued events. Overflow remains bounded and restores a displaced gap marker's full represented loss instead of counting it as one event. Successful append explicitly disarms recovery.

The fleet coordinator already retained its source in the durable CAS outbox until append succeeded. Its canonical append-before-clear operation now takes only the fleet repository, cell identity and append capability, allowing cancellation at both persistence boundaries to be tested without an HTTP fixture. Cancellation during append leaves the original outbox; cancellation during CAS clear cannot lose an event whose append already completed. Retries keep the same deterministic IDs.

Six added regressions exercise entered-pending append sinks for ops/audit, stable ordering and IDs across cancellation/error/retry, overflow gap magnitudes, a held fleet append, and a real ObjectStore CAS clear held after append success. They use isolated queues/repositories. Combined all-target release Clippy and formatting checks passed; execution receipts are recorded with the final controller validation run.

R10 final ownership audit: HTTP body ceilings now come directly from each server's immutable validated config at raw create, raw append preflight/incremental collection, and the product mutation collector. There is no process-wide body-limit installer. `RuntimeCaps.ops` owns the operational queue, recent ring, drop debt, gap sequence, and alert states; `RuntimeCaps.audit` owns denial sequencing, queue and drop debt. Quota pressure, shard/scaler, HTTP lifecycle and creation deletion events all use the originating runtime's journal. Cancellation guards continue to restore batches into that same owner. A fresh security fixture now asserts an empty journal instead of draining unrelated tests' denials.

The new `runtime_journals` scenarios create two live runtimes: draining B before A must write exactly each owner's operational and denial events to its own durable ledger, and B's healthy alert evaluation cannot resolve A's alert. Distinct 64 KiB and 128 KiB body limits must independently govern declared-length raw appends, chunked raw appends, raw create bodies and product appends; each runtime's history reservation must use the same configured ceiling. Focused execution and the final receipt are recorded with the coordinated R10 validation artifacts.
