# Read follow-up acceptance ledger — 8 September 2026

## Current implementation decision

The subsequent independent source review of `5bdaf968` accepts O2-A/O4-A, closes the specific O2-B aggregate-copy/growth finding and O3 consolidation, and retains R17-B/V01's earlier scoped dispositions. The repository owner subsequently requires permanent implementation choices with no experiment switches. See [permanent read paths and cache removal](../final-disposition.md).

| Item | Current disposition |
| --- | --- |
| O1–O4 | Permanent unconditional implementation; scoped review acceptance retained, including exact ownership, corruption and retirement regressions. |
| O2-C | Compatibility repair and regression verification tracked in the final disposition report. |
| O5 | Removed after failing the complete screen. The recommendation to keep it default-disabled is superseded by the owner's no-switch instruction. |
| Pairing | The independent reviewer reproduced the prior 120 and current 762 triplets; the disclosed-block arithmetic uncertainty is closed. Held raw samples and binaries are still a separate unverified boundary. |
| Performance / E03 | HOLD. O5 removal does not close the independent replay, allocator/RSS or workload acceptance obligations. |
| Crypto / deployed fleet / raw evidence / merge | Existing holds remain. PR #19 stays draft. |

## Historical experiment evidence

The table below preserves the author dispositions at the timed experiment runtime. Current source decisions above supersede its requests for review and default-disabled status; its measurements are not relabelled as results for the current code.

Author dispositions for runtime `eb5ab8ad7a1459b6c679b72bf342a3af73e0bede`. See the [report](report.md), [screen decision](screen-verdict.json), [source validation](validation.json) and [draft PR #19](https://github.com/prisma/streams/pull/19). This supersedes the earlier read-experiment ledger for the items below; it does not supply independent external acceptance.

| Item | Work completed / evidence | Current disposition |
| --- | --- | --- |
| R17-B retirement | Existing transaction publication owner and retirement regressions remain unchanged from the accepted baseline. Cache invalidation is called immediately after the existing engine terminal handoff. Full existing controls pass. | Prior scoped acceptance retained; no reopening or broader approval. |
| V01 historical feed freshness | Prior fixture correction and source remain unchanged. No new attribution of the historical failures is made. | Prior scoped disposition retained. |
| O1 checked frames / borrowed metadata | Preserved; cache compaction now copies its own checked frame with its last-alias charge, avoiding an extra byte comparison without permitting substituted metadata. | Retain scoped source improvement; no standalone service certificate. |
| O2-A retained plaintext, P1 | Explicit admitted batch owner, typed physical ends, centralized subset compaction, actual frozen/fork/held-result/body regressions. `870534e`. | Author remediation complete; independent source acceptance requested. |
| O2-B compressed fallback | Removed the second aggregate plaintext copy and flattened metadata. Executed 360 allocation cases, disclosing extra small allocations and System peak-live increase alongside mimalloc copy/peak reductions. `870534e`, `c58ddc3`, `4615380`. | Author remediation and measurements complete; allocation/RSS tradeoffs not independently accepted. |
| O3 durable-ring witness | One common private coverage walk, unchanged entry points and negative controls. `59e8529`. | Scoped mechanism retained; requested local consolidation complete. |
| O4-A postings windows, P1 | Validated nonempty ordered/disjoint runs at decode, stitch, extension and cache admission; real stored-corruption counterexamples and canonical result/cursor oracles. `1f4cab1`. | Author remediation complete; independent source acceptance requested. |
| O5 encrypted-span candidate | Reviewable default-disabled source, descriptor/Db/engine proof, bounded complete ciphertext spans, negative-space coverage, synchronized lifecycle, cancellation, coalescing and per-project/last-alias accounting. V1 failed reuse preflight; bounded credit refinement passes its regressions and preflight. `1d6b646`, `eb5ab8a`. | Source/lifetime prototype delivered for review. Hot mechanism and latency gain demonstrated. Production/performance acceptance remains open. |
| Matched performance versus original | Both native allocators; five blocks; hot/cold/rotating/high-cardinality/compressed/mixed/retained-reader/tenant-pressure, append and primary full HTTP replay. | **HOLD.** Hot p50/p99 pass; tenant medians, full replay and resource criteria fail or remain inconclusive. No larger acceptance campaign. |
| Replay investigation | Separate persistent connections and request-correlated scheduled controls; 120,000 scheduled reads plus 4,800 background appends, zero errors/timeouts; wake/admission delays localized within requests. | Investigation delivered with bounded conclusions. No diagnosed production transport/backend/lifecycle cause; no tail acceptance. |
| Pairing and arithmetic | Prior 120 triplets reproduced with explicit historical pairs. New 960 identified block rows, 192 comparisons and 762 reproduced ratio/CI triplets. Minimal executed harness, source/tree/binary mapping and guardrails published. | Author arithmetic verified. Independent verification of held raw samples/binaries remains open. |
| Source and SDK validation | 927 local Rust tests including isolated capacity; no new Clippy fingerprints; static/MT gates; 33 SDK tests, auth/capability vectors, typecheck/build. Exact runtime CI successful. | Author and exact-source CI passes recorded; not an independent security or performance certificate. |
| Raw evidence access | Raw samples, full logs, native binaries and full archives remain local. Only permitted source/harness, receipts and block aggregates published. | Existing upload hold retained; no inferred new permission. |
| E01 cryptographic / operational review | No encryption-format rollback or weakened authentication used to obtain a speed result. | Independent review remains open. |
| E03 workload-specific acceptance | New gains, regressions, uncertainty and memory tradeoffs are disclosed by workload and allocator. | **PERFORMANCE_DECISION_PENDING.** No owner tolerance has been granted. |
| E04 deployed platform / fleet work | This campaign uses local storage and an Apple M2; physical scan counts are distinct from remote object GETs. | No new deployed/fleet performance or operational acceptance. |
| Merge / deployment | PR #19 is open, unmerged and draft. O5 remains disabled by default. | Not approved; neither action performed. |

The next acceptance boundary is review of the corrected source and a workload-specific decision on the remaining performance/resource failures. Passing a hot-cache micro-workload, removing physical scans, or meeting a historical absolute ceiling does not discharge those requirements.
