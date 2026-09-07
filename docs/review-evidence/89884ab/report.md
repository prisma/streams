# Prisma Streams follow-up remediation report

Report date: 7 September 2026.

Implementation owner: Codex, acting on the repository owner's requested review remediation. All seven follow-ups are implemented and READY_FOR_REVIEW. This is an engineering report, not an independent reviewer sign-off or deployment approval.

Source: `89884ab114b22929aa93707361e9e8d52c762bc2` on `slate`; Git tree `dc26e246b25fb6275374d3d56088c165901d19c9`. Follow-up baseline: `43813b12b3d13025ba32435a4c8b761ab752b486`. Eighteen separate commits cover this follow-up; 111 commits cover the original review and follow-up together. Both complete commit lists and the source archive are included.

Final CI and the successful final local gate passed. The local totals are 961 Rust target test executions, 33 SDK tests, and 332 protocol tests with six expected skips. The five paired performance rounds still show regressions in hot filtered reads and many-record replay, including higher tails; these remain an explicit owner decision. The following sections preserve failed attempts and distinguish engineering completion from external acceptance.

## Changes

| Finding | Result | Commits |
| --- | --- | --- |
| R03-A | Replaced the 2,137-line commit function with a cohesive provisional transaction owner. Required billing reads precede staging; focused phases preserve ordered operations, one atomic storage write, and publication through the durable-effects path. Removed the obsolete structural exceptions. | 73be780, 7731c3f |
| R06-A | Bounded plaintext, record count, metadata, scan work, decompression and peer wire bytes. Pages stop before an unreturned record and preserve its exact resume position, including history, filters, forks and remote reads. | 93f77ec, e6d2bac, 61353f0, 599fcd3 |
| R08-A | Validated stored key width/identity, frame extent and offsets before filtering or scan progress. Corrupt ring-cache entries fall back to canonical storage; malformed stored rows return typed errors. | 08165bd, 62a898d |
| R09-A | One runtime-owned worker bounds request maintenance to eight active and 64 queued jobs, with coalescing and deadlines. TTL renewal has explicit retryable refusal before append effects. Persisted topology debt survives interrupted work and is retried by subsequent reads/scaler hints. | a1c104d, 8600166 |
| R17-A | Supervised the actual five engine task futures and retained shutdown/finalization authority across timeouts and cancelled callers. Unexpected task exit fences the engine and affects readiness. Production shutdown joins residents, late opens and global tasks. Failed finalization retains the replacement fence. | 24cfdac, 1bbe41d, 89884ab |
| R21-A | Made each request's credential wait abortable while preserving another caller's shared refresh. Tests cover initial acquisition, refresh, external abort, late rejection and listener cleanup. | d48f597 |
| R24-A | Supplied full CI Git history, retained fail-closed provenance checks and tested real shallow/full fresh checkouts. | 31d499c |
| E03 implementation | Profiled decrypt setup, then reused at most 64 page-local cipher schedules without changing authentication or read bounds. Mixed old/new formats, cache overflow and key/epoch/segment fencing are tested. | 643ca83, 3008416 |
| Ledger | Recorded scoped states and external acceptance boundaries. | 4ec14f4 |

The [original R01–R24 dispositions](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/README.md) and [original remediation ledger](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-resolution.md) remain available at the tested source revision. Accepted original mechanisms were retained. No unresolved scenario was relabeled full and no lint or architecture baseline was mechanically reset.

## Regression evidence and limitations

Supporting logs retain the actual pre-fix SDK cancellation failure, malformed stored-row panic, compressed-page overshoot, detached maintenance/held shutdown observations, and shallow-checkout provenance failure. R03-A is primarily structural; its regression tests compare complete key/value state and every reply/publication for mixed operations, required-read failure and failed writes, with existing real WAL barriers retained. Intermediate focused runs are supporting evidence; final-source receipts are separately identified.

Earlier failed attempts remain in `supporting/`: initial compile errors, sandbox-denied loopback tests, zero-selected filters and fixture corrections are not counted as passing coverage. The R17 held-absorber fixture now waits for worker termination before releasing its storage barrier, then waits for full finalization before a new writer; its debt/lag/reservation assertions remain. The R09 topology fixture uses the actual implicit parent prefix. Original fixture changes are anchored in the source evidence checker. The new R03 test observes touch publication through the journal wait API. No failed run was overwritten as a final receipt.

## External acceptance

- E01: Independent cryptographic review, operated historical-ciphertext inventory and rollout acceptance remain with an independent reviewer and an authorized operations owner, to be appointed by the repository owner. Local tests do not repair historical confidentiality or establish deployed data exposure.
- E02: The accompanying portable bundle provides raw logs, commands, toolchain/configuration details, source identity, all scoped commits, historical failed attempts and a verifier. Its hashes prove internal consistency; they are not an independent execution signature.
- E03: No pre-agreed absolute latency/memory budget or signed waiver was supplied. The product/repository owner must decide acceptance for named workloads using the included comparison; no threshold is invented after measurement.
- E04: Prisma Compute/Tigris deployment, noisy-neighbor, restart/fencing and cost campaigns remain with the authorized platform/release owner. The scenario inventory remains 189: 117 full, 23 partial, two external and 47 unmapped. Local CI does not establish the deployed combinations.

The earlier main CI run 34027002320 at 43813b12 failed its architecture job; eight jobs succeeded and the scheduled noisy campaign was skipped. Its Node 18/Bun/Deno package smokes remain valid historical evidence. New-source CI status must be read separately below.

Final validation at 4ec14f4 exposed two further issues, preserved in `attempt-4ec14f4/`: the compressed-tail fixture could race startup recovery and absorb records into history despite long age thresholds, and the new page-local routing-key map lacked its scoped identity-lint explanation. Commit 61353f0 pauses the fixture’s owned absorber before creating debt without changing any paging oracle; 3008416 explains the map’s fixed stream-key/epoch/segment boundary. New-source receipts follow these corrections.

CI at 3008416 then rejected the updated fixture’s stale inventory hash and its direct bare hash helper. Commit 599fcd3 records only that new test’s revised hash and uses the existing RoutingKeyHash constructor in the seed helper. The multitenancy audit baseline is unchanged. These intermediate CI failures are preserved; they are not reported as final-source passes.

Final fleet validation at 599fcd3 exposed an expected-ownership-fence lifecycle regression. A real new database owner fenced an old owner’s final flush; the pinned backend joined all tasks before returning Closed(Fenced), but the engine retained the replacement fence as if the close were incomplete. Commit 89884ab recognizes only that awaited expected result as completed retirement. A real held-SST/new-owner regression failed before the change; pending closes and other finalizer failures still retain their fence. The failed exact-599fcd3 CI job and interrupted local diagnostic are retained under attempt-599fcd3; neither is a final certificate.


The first full local gate at the final 89884ab revision failed the existing `subscription_survives_when_both_feeds_refresh_before_the_deadline` timing test (883 passed, one failed, capacity filtered). An isolated rerun also failed. The unchanged second full gate passed all 885 library tests, and final CI passed the same test. A separate run of that test in the original a7e2070 measurement binary passed. The cause of the intermittent local failure is not established; these observations do not prove that load caused it or that the baseline fails. The failed full run, failed isolated run and baseline observation are retained in `final/failed-local-gate-1/`, `final/feed-refresh-isolated.log`, and `final/feed-refresh-original-baseline.log`. No assertion, timing bound or source file was changed to obtain the successful final rerun.

## Final-source validation

Every final receipt binds clean source commit `89884ab114b22929aa93707361e9e8d52c762bc2` and tree `dc26e246b25fb6275374d3d56088c165901d19c9`. Local runs used Rust/Cargo 1.98.1, release builds with incremental compilation disabled, Node 22.22.2, npm 10.9.7 and the locked SDK/conformance dependencies. Raw commands, configurations, runner hashes, toolchain versions and output hashes accompany each receipt. CI ran on its separately recorded GitHub runner environment; local and CI executions are not combined into one count.

| Local check | Final result | Bundle evidence |
| --- | --- | --- |
| General library suite | 884 passed, zero failed; capacity selected separately | `final/rust-final.json`, `final/final-gate-full.log` |
| Isolated capacity mechanism | One passed; counted once within the 885 library total | `final/final-gate-capacity.log` |
| Binary targets | 76 passed, zero failed | `final/binary-final.json` |
| Rustdoc | Zero tests selected; command succeeded | `final/binary-final.log` |
| SDK | 33 passed, zero failed/skipped; typecheck, build, auth and watch vectors passed | `final/sdk-final.json` |
| Pinned protocol suite 0.3.6 | 332 passed, zero failed, six reserved subscription API skips; 338 total | `conformance/receipt.json`, `conformance/result.json`, `conformance/suite.log` |
| Source gates | Formatting, actionlint, architecture, scenario map, test inventory, review evidence and multitenancy audit passed; zero new Clippy fingerprints | `final/gate-summary.log`, `final/final-clippy.out` |
| Real fresh checkouts | Depth-one history refused missing provenance; full history passed anchored comparisons | `final/fresh-checkout.log` |
| Portable verifier controls | Positive fixture plus ten negative controls passed | `final/portable-verifier-selftest.log` |

The Rust total is **961 target test executions: 885 library + 76 binary**. Some crypto test modules are compiled into multiple binary targets, so this is not a claim of 961 unique test bodies. The mapped inventory contains 441 tests with zero ignored; it is a different scope from the full executable count. The protocol receipt's generic parser counts passes/failures; its six allowed skips are reconciled separately against the pinned exact JSON result and check script.

The successful local gate ran `scripts/gate.sh` unchanged. The separate binary receipt covers the remaining binary/rustdoc targets. `conformance/run.sh` uses freshly started release binaries, synthetic keys, an isolated local s3lite namespace, 2 ms storage latency, 1 ms flush interval, 2 ms WAL flush gap and a 64 MiB unflushed limit. These are recorded test configurations, not production measurements.

## Final GitHub Actions

Main [run 34050181550](https://github.com/prisma/streams/actions/runs/34050181550), attempt one, completed successfully at the exact source revision above. Separate [workflow lint 34050181553](https://github.com/prisma/streams/actions/runs/34050181553) also succeeded. The main run has nine successful jobs and one scheduled-only skip:

| Job | Result |
| --- | --- |
| [rust](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263389) | success |
| [sdk-package](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263566) | success |
| [durable-streams-server-conformance](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263598) | success |
| [platform-e2e](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263606) | success |
| [architecture-report](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263607) | success |
| [mt-cert-1000](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263613) | success |
| [livefeed-fleet-cert](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263644) | success |
| [product-field-gate](https://github.com/prisma/streams/actions/runs/34050181550/job/101532263687) | success |
| [livefeed](https://github.com/prisma/streams/actions/runs/34050181550/job/101532264022) | success |
| [noisy-campaign](https://github.com/prisma/streams/actions/runs/34050181550/job/101532264210) | skipped |

The architecture job reached `review-evidence.py --check`, passed its source inventory, and ran both real checkout cases with anchored comparisons at 89884ab. The SDK job passed current-Node package checks and four credential-wait cancellation regressions on Node 18, Bun and Deno, as well as their local-server package smokes. It does not establish every SDK race on every runtime against a deployed service. The scheduled noisy campaign skip supplies no campaign execution evidence.

`final/ci-final.json` includes source-bound run/job/step conclusions. Full Rust, SDK, fleet and architecture job logs are included. The zero-byte `ci-all-jobs.log` records an empty aggregate-log download and is not used as evidence. Rust CI echoes some test summaries after execution and runs additional focused checks; do not sum every repeated summary line to reconstruct unique coverage.

## Evidence map and source contracts

All seven findings are owned by the implementation owner named above and remain READY_FOR_REVIEW. The table lists supporting before/after observations; the final execution receipts above are the clean-source certification of the author's test runs.

| Finding | Supporting observations in bundle | Final source contract |
| --- | --- | --- |
| R03-A | `supporting/r03-structure.json`, `r03-green-4.log`, and `r03-existing-durability_.log`: structural comparison, complete persisted state/replies and real durability barriers | [commit-transaction.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/commit-transaction.md) |
| R06-A | `supporting/r06-red-executed.log` → `r06-green-executed.log`; final suite adds the owned-absorber fixture correction. Actual compressed local/HTTP pages, large single records, filters, forks and history are exercised | [read-page-contract.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/read-page-contract.md) |
| R08-A | `supporting/r08-red-executed.log` → `r08-green-executed.log`: actual malformed stored-row panic/corruption rejection and valid fallback coverage | [read-page-contract.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/read-page-contract.md) |
| R09-A | `supporting/r09-red-executed.log` → `r09-green-3.log`: bounded/coalesced request maintenance, retry and cancellation ownership | [request-maintenance.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/request-maintenance.md) |
| R17-A | `supporting/r17-red-executed.log`, `r17-green-6.log`, and `r17-fenced-close-red.log` → `r17-fenced-close-green.log`: actual task exits, held shutdown/finalization and real storage-owner fencing | [engine-lifecycle.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/engine-lifecycle.md) |
| R21-A | `supporting/r21-red.log` → `r21-green.log`: held provider cancellation closes before provider release while another shared-refresh waiter survives | [README.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/README.md) |
| R24-A | `supporting/r24-shallow-red.log` → `r24-fresh-green.log`; final `fresh-checkout.log` and `ci-architecture.log` certify the actual final source and CI | [README.md](https://github.com/prisma/streams/blob/89884ab114b22929aa93707361e9e8d52c762bc2/docs/review-followup/README.md) |

The R03 owner retains one WriteBatch and one durable-effects transfer. Required accounting reads occur before staging; storage application, publication and remote-WAL reply eligibility retain their distinct barriers. The entry point shrank from 2,137 lines to three, with focused owner methods below the structural limits; eliminating parallel ownership and retaining transaction semantics are the substantive changes.

R06 enforces a 1-byte–8-MiB requested plaintext budget, a documented first-record exception up to 32 MiB, 4,096 records, 1 MiB metadata, bounded scanned work and bounded decompression. The peer envelope is derived from the same maximum decoded payload, base64 expansion and metadata, with a required progress watermark. A withheld matching record remains at the exact next resume position. R09 bounds ownership to eight active plus 64 queued items; persisted topology debt retries on later reads/scaler hints, without claiming an autonomous full-catalog retry loop. R17 treats only an awaited expected Closed(Fenced) finalization result as completed retirement; pending closes, unexpected failures and panics retain their fence.

## Performance evidence and decision


Five paired/interleaved rounds (AB/BA/AB/BA/AB), same M2 Air, release profile and counted System allocator. Compression disabled; original baseline writes v2 AES-GCM, final source writes v4 AES-GCM-SIV. These are complete logical results under documented budgets. Process-cold means a newly opened process over a persisted fixture; it is not OS-cache cold. No agreed acceptance budget or waiver is asserted.

| Workload | Samples/version | Baseline p50/p95/p99 µs | Final p50/p95/p99 µs | p50 change | Serial requests/s baseline → final |
| --- | ---: | ---: | ---: | ---: | ---: |
| append / local-perf-product-1024 | 640 | 6534/7284/7918 | 6482/7410/8001 | -0.8% | 154.1 → 154.9 |
| append / local-perf-product-65536 | 640 | 6490/7392/7966 | 6426/7447/8048 | -1.0% | 154.1 → 153.3 |
| append / local-perf-raw-1024 | 640 | 6481/7392/7798 | 6479/7318/7716 | -0.0% | 153.6 → 155.1 |
| append / local-perf-raw-65536 | 640 | 6460/7351/7670 | 6476/7330/7849 | +0.2% | 153.9 → 153.4 |
| history-read / postings-hot | 640 | 407/765/1090 | 461/1358/3430 | +13.3% | 2147.9 → 1575.2 |
| history-read / process-cold | 5 | 6877/—/— | 9703/—/— | +41.1% | 116.2 → 104.1 |
| product-replay / local-perf-product-1024 | 640 | 1428/2490/3075 | 1508/2707/6030 | +5.6% | 643.0 → 555.4 |
| product-replay / local-perf-product-65536 | 640 | 1064/2208/2513 | 967/2411/2686 | -9.1% | 782.2 → 826.7 |

Quantiles use nearest rank over pooled request samples. Throughput is serial request count divided by summed request time, excluding warm-up and fixture setup; it is not saturated throughput. Five cold samples are listed individually in summary.json; no cold p95/p99 is reported.

| Workload | Allocations/request baseline → final | Allocated bytes/request baseline → final | GET attempts baseline → final | GET bytes baseline → final |
| --- | ---: | ---: | ---: | ---: |
| append / local-perf-product-1024 | 778.525 → 760.078 | 316052.083 → 308649.381 | 947 → 940 | 0 → 0 |
| append / local-perf-product-65536 | 565.163 → 544.583 | 1148709.123 → 1078198.981 | 395 → 410 | 26967 → 56003 |
| append / local-perf-raw-1024 | 671.97 → 671.603 | 281357.081 → 272271.466 | 791 → 775 | 0 → 0 |
| append / local-perf-raw-65536 | 559.913 → 571.873 | 1209020.125 → 1221376.441 | 585 → 587 | 52845 → 28760 |
| history-read / postings-hot | 2784.264 → 2787.98 | 583364.383 → 584603.327 | 114 → 92 | 442 → 884 |
| history-read / process-cold | 14744.2 → 15656.4 | 6114711.6 → 6787320.2 | 413 → 447 | 387443 → 390781 |
| product-replay / local-perf-product-1024 | 2774.267 → 2764.856 | 1277484.539 → 1278129.956 | 98 → 146 | 0 → 24148 |
| product-replay / local-perf-product-65536 | 427.253 → 418.416 | 561149.27 → 558385.512 | 66 → 64 | 0 → 0 |

ACTIVE-scoped counters cover allocations/background work during each measured phase. Object counters include attempts, successful range bytes and cache index activity in the raw JSON; they are local fixture operations, not billable cloud requests. RSS below covers each entire test process, including setup and every workload in that process.

| Process group | Baseline maximum RSS bytes (five runs) | Final maximum RSS bytes (five runs) |
| --- | --- | --- |
| append | 89096192, 88473600, 89505792, 101908480, 90226688 | 92585984, 103481344, 91062272, 92700672, 92471296 |
| read | 14843904, 14843904, 14876672, 14942208, 15138816 | 16760832, 16531456, 16760832, 16498688, 16564224 |

baseline: 1280 measured product payload bridge copies / 42,598,400 copied bytes across the five rounds.

current: 0 measured product payload bridge copies / 0 copied bytes across the five rounds.

The pre-optimization profile (source a1c104d) measured actual segment HKDF and cipher initialization at 7.697% and 2.218% of many-record replay request time, and 5.498% and 1.566% of hot filtered query time. Timing hooks add overhead and unmeasured work is not attributed. The resulting cache retains at most 64 expanded schedules per physical segment page (129,024 bytes of schedule structs in this build, excluding map/box overhead). It preserves all authentication and page limits.

Full sources, fixtures, toolchains, binary identities, build logs, raw request samples, settings, page/result assertions, and commands are included. Measurement executables are hashed but omitted from the portable bundle; rebuild from the included measurement source archives with the pinned Cargo.lock and the recorded toolchain. No loopback result establishes deployed Prisma Compute/Tigris latency, memory, cost or tails.


In this campaign, many-record replay median rose 5.6% (1,428 → 1,508 µs) and p99 rose from 3,075 to 6,030 µs. Hot filtered-read median rose 13.3% (407 → 461 µs) and p99 rose from 1,090 to 3,430 µs. Single-record replay median fell 9.1%. Process-cold median rose 41.1%, based on only five samples per revision. These local measurements show remaining regressions; the optimization does not establish performance acceptance. The report includes the full distributions and process RSS rather than interpreting the medians alone.

The prior 43813 handoff's three-run replay/hot-query numbers remain historical observations. The five-round comparison above uses the original a7e2070 baseline and the final 89884ab code with complete-result oracles after the read-budget repair. It establishes the measured combined remediation behavior, not a controlled estimate of the cipher-cache change alone. Profiling established measurable HKDF/cipher setup cost before the optimization; unmeasured costs and differences between campaigns are not attributed to it.

E03 remains PERFORMANCE_DECISION_PENDING until the product/repository owner records a workload-specific budget decision or signed waiver with a revisit trigger. E01 and E04 remain EXTERNAL_ACCEPTANCE_PENDING with the owners stated above. E02's author-side portable delivery and verification are complete when accompanied by the published ZIP and verification transcript; independent acceptance remains with the reviewer.

## Using the delivered bundle

Download `verification-bundle.zip` beside this report and extract it into a new directory. The archive includes the exact source archive and raw commit object, full 111-commit list and scoped 18-commit follow-up list, final receipts/results, raw logs and runners, benchmark sources/raw samples, prior failed attempts and the earlier 43813 handoff. Measurement binaries and dependency caches are omitted; their hashes and reproducible source/build inputs are retained. The final source has no uncommitted changes, and `slate` was pushed at 89884ab.

```sh
python3 verify-bundle.py /path/to/extracted-bundle
python3 verify-bundle-selftest.py
```

The portable verifier checks the full file hash inventory, reconstructs the Git tree from archive bytes and modes, binds it to the raw Git commit, and validates the four final clean-source receipts against raw output, configurations, minimum coverage and source identity. It works without the original Mac paths. The receipt tool in the source archive can additionally verify the original live-checkout environment; portable verification preserves those recorded commands/paths as evidence rather than pretending they are reusable unchanged on another host. See `performance/reproduction.md` for the portable paired-measurement rebuild/run command.

The separate evidence branch adds this report and bundle after the source freeze. Its artifact commit is not claimed as the revision used for the runtime tests: every receipt explicitly names 89884ab and its tree. The accompanying `verification.log` records an actual verification of the extracted delivered ZIP, and `SHA256SUMS` identifies the published ZIP and report. Hash consistency is not an independent execution signature. Reviewer sign-off remains pending.
