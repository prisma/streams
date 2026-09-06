# Follow-up remediation ledger

Review input: prisma-streams-remediation-final-review.md, reviewed source 43813b12b3d13025ba32435a4c8b761ab752b486, original baseline a7e2070f3b4346b3e54d552069ff91c56e900130. The document supplies findings and acceptance criteria; it does not authorize production deployment, historical-data access, an independent audit, or a waiver. Engineering changes below are READY_FOR_REVIEW, not self-certified VERIFIED. Exact final execution and benchmark receipts are delivered separately as a portable repository artifact.

| Follow-up | Change | Implementation commits | State |
| --- | --- | --- | --- |
| R03-A | One provisional transaction owner, focused operation phases, one write and durable-effects transfer; removed structural exceptions. [Contract](commit-transaction.md). | 73be780, 7731c3f | READY_FOR_REVIEW |
| R06-A | Plaintext/metadata/record-count/scan/wire bounds with exact withheld-record progress across local, history, and peer paths. [Contract](read-page-contract.md). | 93f77ec, e6d2bac | READY_FOR_REVIEW |
| R08-A | Checked stored record identity/frame decoding before filtering or progress; corrupt ring entries fall back to storage. | 08165bd, 62a898d | READY_FOR_REVIEW |
| R09-A | Runtime-owned bounded topology/TTL work; explicit renewal refusal/retry and incarnation fencing. [Contract](request-maintenance.md). | a1c104d, 8600166 | READY_FOR_REVIEW |
| R17-A | Actual engine task supervision, retained shutdown authority, storage finalization, retirement fences, and readiness failure policy. [Contract](engine-lifecycle.md). | 24cfdac, 1bbe41d | READY_FOR_REVIEW |
| R21-A | Per-request abortable credential waits retain shared refresh and settlement semantics; four SDK races added to the runtime matrix. | d48f597 | READY_FOR_REVIEW |
| R24-A | Full-history CI checkout, explicit provenance failures, and real shallow/full fresh-checkout integration checks. | 31d499c | READY_FOR_REVIEW; final workflow receipts required |
| E03 implementation | Profiled repeated decrypt setup, then capped page-local schedule reuse. [Measurement scope](read-performance.md). | 643ca83 | Final paired measurement and owner decision required |

Preserve the independent review's original dispositions: R02/R04/R07/R11/R12/R13/R14/R15/R16/R18/R19/R20/R22/R23 were accepted for their inspected scope. R05's core coordinator work was accepted; adjacent request scheduling remained open. R01's new-write code was accepted for inspected scope while independent crypto/history/rollout acceptance remained open. R03/R06/R08/R09/R10/R17/R21/R24 had partial or reopened boundaries addressed here; their final-review decisions remain with the reviewer. None of these changes reclassifies unresolved scenarios or turns prior author assertions into independent receipts.

| Acceptance item | Current boundary | Required decision/owner |
| --- | --- | --- |
| E01 | Local source and pinned-backend crypto/compatibility tests only. No operated-namespace inventory, historical confidentiality claim, independent crypto signature, or deployment. | Independent cryptographic reviewer and authorized operations owner, to be appointed by the repository owner. |
| E02 | Portable report/logs/hashes/source identity and verified clean-source receipts accompany the final handoff. Earlier failures remain separately identified. | Repository reviewer verifies the delivered artifact against its named source revision. |
| E03 | Local complete-result profiling and paired benchmarks; no agreed absolute latency/memory budget was supplied. No waiver or acceptance threshold is invented. | Product/repository owner defines the workload budget and decides acceptance or further work. |
| E04 | Local/CI campaigns establish only their recorded configurations. Prisma Compute/Tigris deployment, noisy-neighbor and cost acceptance remain external. Scenario inventory remains 117 full, 23 partial, 2 external, 47 unmapped (189 total). | Authorized platform/release owner assigns and executes the remaining campaigns and scenario mechanisms. |

The reviewed 43813b12 CI run 34027002320 failed its architecture job because required historical objects were absent. Its successful Node 18/Bun/Deno package smokes remain valid historical evidence for that revision. Newly added cancellation races and the repaired evidence gate require results from the new final revision; the final report records those actual job states rather than relabeling the old run.
