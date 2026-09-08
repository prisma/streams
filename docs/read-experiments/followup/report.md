# Read follow-up report — 8 September 2026

**Decision: keep PR #19 in draft and retain the performance hold.** The two source blockers are corrected in separate commits, compressed storage no longer incurs the extra aggregate plaintext copy, the ring walk is consolidated, and a reviewable O5 implementation is present behind a default-disabled flag. The matched screen establishes a substantial hot-history gain. It does **not** establish overall parity: tenant-pressure medians, resource guardrails and full HTTP replay remain unacceptable or inconclusive. The larger ten-block × 10,000-read acceptance campaign was not advanced.

These are author implementation and measurement results. They do not grant independent source acceptance, cryptographic approval, workload-specific tolerance for regressions, merge approval or deployment approval. The prior R17-B and V01 scoped dispositions remain unchanged.

## Source changes and deterministic evidence

| Item | Commits | Correction and evidence |
| --- | --- | --- |
| O4-A, P1 | `1f4cab1` | A private validated postings owner establishes nonempty, representable, globally ordered and disjoint runs before binary windows or cache publication. Decode, stitch, extension and installation reject invalid derived metadata through bounded canonical fallback or an honest error. Actual stored overlapping/decreasing-end and zero-count counterexamples fail on the prior source and pass after correction; overflow/key/range controls, valid seams and match-free progress are covered. The existing 40,960 valid-domain differential cases are retained. [Details](O4-A.md). |
| O2-A, P1 | `870534e` | Typed physical exclusive ends reach frozen scans, peers and fork ancestors. `PlainBatch` owns explicit blocks and record ranges; admission transfers whole eligible owners or compacts selected subsets centrally. Exact backing storage and the regression observer follow the last alias across another read and slow body retention. Executed prior-source scan controls retained 14,680,066 bytes for two output bytes, and 29,360,132 for two held results; the corrected values are two and four bytes. The clipped one-byte fork/body control falls from 7,340,033 bytes to one, then zero on final drop. Full-batch eligible body sharing remains. [Details](O2-A.md). |
| O2-B | `870534e`, `c58ddc3`, `4615380` | Compressed fallback keeps its admitted independent owner within the same batch abstraction. Flat record metadata removes per-block metadata-vector growth. The separate 360-case allocation diagnostic covers two shapes, three formats, complete/withheld/bad-auth boundaries and both allocators. [Results](O2-allocation.md). |
| O3 | `59e8529` | Keyed and unfiltered readers share one private durable-ring coverage walk. Existing entry points, negative controls and witness ownership are retained. [Details](O3.md). |
| Pairing | `553e0db`, `5edf65a` | Stable pair IDs, exact source/tree/binary identities, and the executed minimal harness are published. The previous 120 ratio/interval triplets reproduce from their actual block pairs; no earlier CI correction was needed. [Prior pairing audit](pairing.md). |
| O5 | `1d6b646`, `eb5ab8a` | Default-disabled encrypted canonical interval reuse, exact negative-space proof, descriptor/Db/engine binding, current response authentication, synchronized fill/invalidation/retirement, cancellation/coalescing, and last-alias global/project accounting. The second commit corrects experimentally observed fill-credit churn. [Source contract and limits](O5-source.md). |

For 1,024 compressed 1 KiB records, the old aggregate plaintext copy is 1,048,576 bytes; the corrected path copies zero bytes into such an aggregate. This is not allocation-free: System allocation calls rise 11,279 → 12,310 and peak live requested storage rises about 8.6%. Mimalloc observed moved-reallocation bytes fall 11,009,856 → 2,194,944 and peak live requested storage falls about one third. Mixed-format allocation behavior and failure/withholding results are included in the allocation report. Requested Rust allocation counters exclude C/zstd workspaces and fragmentation; those figures are distinct from process RSS.

O2 preserves the existing per-page plaintext budget through exact-size owners and central subset admission; its regression observer measures last-alias release. O5 additionally enforces runtime global/project cache reservations.

O5 reserves from the existing history-cache allowance: 2 MiB global, 256 KiB per project, 64 KiB fixed metadata, 256 ready/pending slots, 32 live project quota slots and at most 64 KiB per fill. It caches complete bounded ciphertext intervals below absorption; every response still performs current authorization, key authentication, decompression and plaintext admission. Invalidated/evicted byte aliases retain their quota until last drop. Future destructive canonical retention must invalidate before deleting data; current canonical history is append-only below absorption. The existing transaction/terminal handoff logic and retirement regressions remain byte-identical to the accepted baseline; the engine close adapter adds one cache-retirement call immediately after the existing handoff.

## Experiment identity and controls

The timed production source is **`eb5ab8ad7a1459b6c679b72bf342a3af73e0bede`**; the original control is **`a7e2070f3b4346b3e54d552069ff91c56e900130`**. Four instrumented native arm64 binaries run System or mimalloc on this Apple M2. Release Rust is 1.98.1. The candidate's disabled/enabled conditions use the same binary. Allocation interception is disabled in latency runs. [Build receipts](screen-build-receipts.json) identify every injected source change and binary hash; these engineering binaries are explicitly distinguished from uninstrumented source gates/CI.

Five matched blocks use 128 measured warm requests per case, reversing condition and allocator order on even blocks. The [guardrails](screen-guardrails.json) were published in `5edf65a` before this campaign. Primary p50 and p99 require an upper paired 95% interval below 1; additional latency limits remain 1.05/1.10 and resource limits 1.10. Inconclusive is not a pass. [Configuration and corpus provenance](screen-configuration.json) record the exact conditions.

The history partition contains 32 projects × 256 records × 1 KiB. Plain, compressed and alternating compressible/incompressible datasets were separately seeded through each source's real durable append/absorber path. The original uses its historical AES-GCM frames; the candidate uses the corrected GCM-SIV frames. No historical ciphertext was copied into the candidate. Completed v1 candidate corpora are reused for v2's read-only refinement, whose append, absorber and encryption sources are unchanged. This larger shared partition differs from the earlier one-project feasibility fixture. Historical System ceilings remain unchanged references, not certification of this partition, another allocator or a fleet.

The v1 preflight failed its mechanism check: 16 warm queries still opened 256 canonical scans. Fixed 64 KiB fill reservations with four concurrent reads displaced the small ready set. V2 sizes each credit from the planned span, reserves control/data together, bypasses transient fill pressure without evicting ready entries, and moves unrelated-entry liveness cleanup off hits. New real-canonical tests cover four concurrent fills, underestimated credit and closing an already-warm Db. The v2 preflight passed on both allocators and all formats; both preflights remain in the published block summaries. They are diagnostic, single-block observations, not acceptance runs.

## Primary results

Latency columns below are **medians of five block percentiles**, in microseconds. Ratios are geometric means of paired block ratios with 20,000 paired bootstrap resamples and seed 20260907; a ratio is not the quotient of the displayed median columns. Percentiles use the executed emitter's `floor((N-1) × p)` index. Intervals describe this small local screen and do not certify tails at fleet scale.

| Allocator | Workload | Original → cache-on p50, µs | p50 ratio [95% CI] | Original → cache-on p99, µs | p99 ratio [95% CI] |
| --- | --- | ---: | --- | ---: | --- |
| system | Hot filtered history | 345 → 184 | 0.511 [0.465, 0.569] | 389 → 202 | 0.475 [0.387, 0.580] |
| system | HTTP replay, 64 × 1 KiB | 945 → 2,187 | 2.046 [1.439, 2.743] | 1,181 → 3,217 | 2.468 [1.717, 3.329] |
| system | HTTP replay, 1 × 64 KiB | 676 → 1,262 | 2.166 [1.423, 3.298] | 773 → 1,599 | 2.524 [1.608, 3.961] |
| mimalloc | Hot filtered history | 224 → 169 | 0.765 [0.737, 0.802] | 247 → 202 | 0.819 [0.739, 0.927] |
| mimalloc | HTTP replay, 64 × 1 KiB | 2,377 → 1,150 | 0.862 [0.475, 1.566] | 3,253 → 3,531 | 0.664 [0.165, 2.307] |
| mimalloc | HTTP replay, 1 × 64 KiB | 837 → 703 | 0.940 [0.637, 1.390] | 2,069 → 1,314 | 0.663 [0.461, 1.018] |

Hot p50 improves 48.9% with System and 23.5% with mimalloc; both hot p50/p99 intervals clear the matched primary criterion. Each allocator's 640 warm plain queries open **10,240 → zero canonical scans**, with 95,136 reserved cache bytes after the hot case. These are physical canonical scan starts, not remote GETs saved. Same-binary cache-on/off p50 ratios are 0.475 [0.436, 0.520] and 0.619 [0.529, 0.699].

The corrected cache-disabled source still misses hot parity: p50 ratios are 1.075 [1.006, 1.139] for System and 1.237 [1.128, 1.425] for mimalloc. Full connection-close replay remains primary. System replay fails its matched criteria; mimalloc replay intervals are inconclusive. Cache reuse deliberately excludes these unfiltered replay reads, so differences between replay on/off conditions are not evidence of canonical cache hits or a diagnosed cache-induced transport defect. The old absolute System references also do not rescue these results: the displayed cache-on many-record p50/p99 exceed 1,428/3,075 µs, and singleton p50 exceeds 1,064 µs.

## Coverage beyond a fixed hot range

| Allocator | Plain history case | Original → cache-on p50, µs | p50 ratio [95% CI] | p99 ratio [95% CI] |
| --- | --- | ---: | --- | --- |
| system | rotating | 272 → 131 | 0.454 [0.396, 0.499] | 0.413 [0.338, 0.499] |
| system | high-cardinality | 60 → 69 | 1.052 [0.957, 1.155] | 0.912 [0.770, 1.215] |
| system | retained-reader | 355 → 173 | 0.433 [0.365, 0.494] | 0.387 [0.302, 0.537] |
| system | tenant-pressure | 391 → 552 | 1.386 [1.125, 1.731] | 0.784 [0.500, 1.154] |
| mimalloc | rotating | 167 → 125 | 0.792 [0.735, 0.901] | 0.759 [0.638, 0.955] |
| mimalloc | high-cardinality | 38 → 57 | 1.542 [1.287, 1.847] | 0.836 [0.624, 1.283] |
| mimalloc | retained-reader | 224 → 168 | 0.850 [0.740, 0.978] | 0.925 [0.693, 1.235] |
| mimalloc | tenant-pressure | 230 → 570 | 2.175 [1.800, 2.531] | 0.444 [0.374, 0.513] |

Rotating starts remain within the same 256-record corpus and reuse its immutable spans; they are not a large rotating-storage working set. Retained-reader cases hold sixteen complete results. High-cardinality queries visit project 31's distinct keys, including legitimate missing keys. Tenant pressure cycles all 32 projects. Its canonical openings barely change (10,240 → 10,160 per allocator), while cache-on/off p50 ratios regress to 1.373 for System and 1.640 for mimalloc. This supports the narrower conclusion that this admission strategy adds cost when that workload cannot reuse its working set. It does not justify increasing the predeclared limits after seeing the data.

| Allocator | Hot dataset | p50 ratio [95% CI] | p99 ratio [95% CI] |
| --- | --- | --- | --- |
| system | compressed | 0.369 [0.361, 0.377] | 0.309 [0.216, 0.377] |
| system | mixed | 0.381 [0.374, 0.388] | 0.323 [0.252, 0.377] |
| mimalloc | compressed | 0.617 [0.614, 0.620] | 0.605 [0.570, 0.648] |
| mimalloc | mixed | 0.631 [0.628, 0.634] | 0.574 [0.549, 0.593] |

The mixed hot selector returns the even, compressible records; high-cardinality reads alternate returned formats. The separate O2 allocation diagnostic measures genuinely mixed delivered pages. All format/case block results, including cold and pressure cases, remain in [identified blocks](screen-identified-blocks.json) and [comparisons](screen-comparisons.json). There are only five process-cold observations per condition: no cold p99 is claimed, and process-cold is not an evicted OS/backend-cache experiment.

All eight cache-on append p50 upper intervals fit the 1.05 guardrail, but append p99 guardrails do not all clear. The corrected candidate records zero copies across 5,120 observed product payload bridges (one observed bridge for every measured product append). History process RSS also fails the resource screen: plain-history cache-on/original peak-RSS ratios are 1.122 [1.015, 1.240] with System and 1.217 [1.115, 1.329] with mimalloc; compressed/mixed ratios are also above the accepted band or inconclusive. Whole-process peak RSS includes the test executable/harness and all six history cases in that process; it is not six independent or per-query memory observations. The largest observed reserved span-cache total is 539,136 bytes; that bound does not certify total process RSS.

GET/HEAD attempts are local object-store API calls. Some paired zero baselines have positive candidate counts, which the unchanged rule leaves unaccepted. For example, mimalloc plain hot totals are original 0, candidate-off 73, candidate-on 165, despite canonical scan elimination. That rules out claiming a corresponding remote-GET saving. The screen does not isolate the cause among backend caching, metadata work, timing and other source changes. The [guardrail evaluation](screen-verdict.json) retains these failures/inconclusive results rather than normalizing them away.

## HTTP replay investigation

The campaign completed **120,000 scheduled reads and 4,800 concurrent background appends**, with zero recorded errors/timeouts or wrong bodies/cursors. It also measured 7,680 primary full-completion replay reads, 15,360 separately labeled serial diagnostic reads, 57,690 history queries and 15,360 append requests. Seed operations and warmups are excluded from those counts. All 150 measured processes succeeded.

A separate serial persistent-connection control reduces absolute timings, but the many-record candidate still has p50 ratios 1.129 [1.086, 1.177] with System and 1.118 [1.089, 1.149] with mimalloc. Therefore a switch to persistent connections does not establish parity or substitute for the original completion workload.

The summarizer joins **135,360 measured client/server request IDs**, in 240 block/case groups, with zero unmatched clients. It calculates client deltas within each request and selects the slowest one percent of that same case; server application/page/decode durations are nested and nonadditive. The following candidate-on many-record values are medians of the five blocks' slowest-one-percent cohort medians, in milliseconds:

| Allocator | Arrival rate | Completion | Wake delay | In-flight semaphore queue | After body complete |
| --- | ---: | ---: | ---: | ---: | ---: |
| System | 500/s | 68.605 | 52.200 | 0.001 | 0.003 |
| System | 4,000/s | 41.721 | 1.294 | 35.618 | 0.002 |
| mimalloc | 500/s | 82.705 | 61.966 | 0.001 | 0.010 |
| mimalloc | 4,000/s | 28.029 | 1.532 | 21.265 | 0.008 |

Within-request wake fractions in those slow cohorts are about 79.5% at 500/s for both allocators; in-flight queue fractions are about 85.2% and 75.2% at 4,000/s. This localizes substantial delay to scheduled wake-up and admission in these diagnostic cases. It does not identify why wake-ups were late, prove the historical primary tail's cause, or diagnose a transport/backend/lifecycle defect. The body-to-completion interval includes client body copying/parsing as well as EOF work; it is not pure network-close time. Diagnostic logging and correlation hooks can perturb measurements. [All correlated aggregates](screen-correlated-stages.json) retain the original, disabled and enabled controls, both sizes and both rates. No overlapping stage percentiles were subtracted.

## Validation, evidence access and remaining acceptance

The final runtime's local source gate passes **926 ordinary library tests plus one isolated capacity test (927 total)**, all-target Clippy with no new fingerprints, formatting, source/provenance static gates and the multitenancy audit. Its twelve O5 tests and the existing authentication, corruption, read-budget and retirement controls pass. SDK validation passes all **33 tests**, authentication/capability vectors, typecheck and build. Exact-runtime [CI 34249267069](https://github.com/prisma/streams/actions/runs/34249267069) and [workflow lint](https://github.com/prisma/streams/actions/runs/34249267023) are successful; all nine main jobs pass and the separately scheduled noisy campaign is skipped. [Validation receipts](validation.json) distinguish this from measurement binaries.

The published arithmetic verifier reconstructs **762 ratio/interval triplets from 960 identified block rows and 192 comparisons** to 1e-12, checking stable pairs against exact source/tree/binary identities. This is an author arithmetic audit over disclosed block summaries; it is not independent verification of the underlying samples. The earlier source failures, the v1 failed preflight, superseded builds and successful runs are preserved locally. The [minimal executable harness](../../../scripts/read-experiments/followup/README.md), [arithmetic verification](screen-verification.json), build receipts, configuration, guardrails and block aggregates are reviewer-accessible. Raw latency/stage samples, complete logs, binaries and full archives remain local under the existing upload hold; no new upload permission is inferred.

The [updated ledger](ledger.md) records each disposition. Source review of the O2/O4 repairs and O5 prototype is still external. O5 remains default-disabled; performance and resource tradeoffs have not been accepted by a product/repository owner. R17-B and V01 retain their earlier scoped dispositions without a new historical causal or cryptographic claim. PR #19 remains open, unmerged and draft. No merge or deployment was performed.
