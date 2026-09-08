# Permanent read paths and removal of the span-cache experiment

The repository owner's 8 September 2026 instruction requires every read optimisation to be permanently enabled or removed. O1–O4 remain the single production implementation. O5 is removed. There is no runtime, environment or Cargo feature switch between experimental read paths.

This decision incorporates the follow-up source review of report `5bdaf9684197ff84bd544fd0fcd69520001ea196` and runtime `eb5ab8ad7a1459b6c679b72bf342a3af73e0bede`. Its recommendation to retain O5 default-disabled is superseded by the owner's explicit instruction to eliminate optional experiments. The review's scoped acceptance of the corrected owners is retained.

| Work | Permanent disposition | Reason |
| --- | --- | --- |
| O1 checked frames / borrowed metadata | Enabled unconditionally. | Canonical admission preserves immutable checked metadata through reads; authentication still runs for each delivered response. |
| O2 admitted plaintext batches | Enabled unconditionally. O2-A and the specific O2-B copy/growth finding are closed by the supplied review. | Whole-owner transfer and exact subset ownership prevent discarded suffix retention. Compressed fallback transfers its independent decoded owner. Allocator and whole-process memory tradeoffs remain disclosed. |
| O3 durable ring coverage | Enabled unconditionally. | One walk owns physical-Db, durable-frontier, density and fallback checks for keyed and unfiltered reads. |
| O4 validated postings windows | Enabled unconditionally. O4-A is closed by the supplied review. | Immutable admission validates nonzero, ordered, disjoint and representable runs before binary search. Malformed derived metadata takes bounded canonical fallback or an error. |
| O5 canonical ciphertext span cache | Removed, including its tests specific to cache mechanics. | The complete screen fails. An optional production implementation without acceptance would retain unneeded ownership and lifecycle complexity. |

The O5 deletion removes its ready/pending entries, coalesced waiters, capture publication, quota/reservation ownership, project invalidation and retirement hooks, checked-frame compaction used only by this cache, descriptor-only read-plan plumbing, and duplicate scoped/unscoped history/planner entry points. `HistoryResources` assigns its full configured history-cache budget to the existing block cache. `EXPERIMENTAL_CANONICAL_SPAN_CACHE` and `HistoryConfig::canonical_span_cache` are no longer read, exposed or supported. Existing decoded-postings and block caches remain normal production mechanisms.

The real HTTP history test for repeated reads, wrong keys and delete/recreate isolation is retained without cache-only assertions. O1–O4's retained-owner, frozen scan, fork, peer, body lifetime, malformed postings, valid-domain differential, ring and retirement regressions remain required. Quality compiler fixtures lose only the two construction/mutation cases for the deleted `CipherSpan` type; all surviving proof-owner checks remain.

## Evidence behind removal

The reviewed five-block screen showed candidate-on/original hot-history p50 ratios of 0.511 (System) and 0.765 (mimalloc), but poor-reuse tenant ratios of 1.386 and 2.175. Against the same candidate with O5 off, tenant ratios were 1.373 and 1.640 while canonical openings only fell from 10,240 to 10,160. Plain-history peak RSS ratios of 1.122 and 1.217 failed the declared 1.10 resource rule. The possible 512-interval working set versus 256 slots explains a mechanism to investigate; it is not a completed attribution of all measured latency.

These are the previously disclosed local measurements. Removing O5 does not establish that the remaining implementation matches the original runtime's latency. O5 excluded unfiltered replay, and persistent-connection replay controls still disclosed approximately 12.9% and 11.8% median regressions. The performance hold therefore remains. No larger campaign, enlarged cache budget or revised tolerance is used to declare acceptance.

The [historical report](followup/report.md), failed preflight, paired aggregates, arithmetic evaluator and source-pinned measurement harness remain reproducibility records. They cannot enable an alternate path in the current service. The O5 harness explicitly rejects a current archive without the removed cache; use its recorded runtime for historical reproduction. Raw samples, logs and binaries retain their upload hold.

## Peer compatibility and validation

O2-C, the absent internal scan-end header, is addressed separately from O5 removal. Current senders must continue to transmit explicit ends; an old sender's omitted end must retain open-ended semantics, while a present invalid end must be rejected. Project, epoch, segment, physical identity and authorisation remain mandatory under their existing contracts.

Validation and the compatibility regression results will be recorded here after execution. PR #19 remains draft. No cryptographic, deployed-fleet, workload-owner, merge or deployment acceptance is inferred from source simplification or test results.
