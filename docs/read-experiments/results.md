# Read optimisation experiments — results

The [8 September follow-up report](followup/report.md) contains the corrected O2/O4 source, production-oriented O5 prototype, and latest matched screens. The historical campaign below is preserved; overall parity remains unaccepted.

**Outcome: parity with the original implementation is not established. Keep the performance hold and the draft PR.** The first four changes remove measurable work, but their combined hot-query median remains slower than the original in both matched allocator comparisons. The encrypted-history cache prototype makes repeated hot queries substantially faster; it is a feasibility result with incomplete production lifecycle and admission integration.

The experimental source is pushed in [draft PR #19](https://github.com/prisma/streams/pull/19). No merge or deployment is part of this result. [Updated experiment ledger](ledger.md) records each scoped decision. The earlier [external acceptance ledger](../review-followup/2026-09-07-ledger.md) retains its independent-review, cryptographic and deployment requirements.

## Identity and execution

The supplied plan inspected `89884ab114b22929aa93707361e9e8d52c762bc2`. This work starts from the corrected retirement/transaction reference **`dac9e908ec7bbf50d9b6479ce27991d198269bfb`**, rather than certifying the acknowledged retirement race. Original comparison: **`a7e2070f3b4346b3e54d552069ff91c56e900130`**. Final runtime source: **`13a43c8662b632be1dadb4110363f88a7f1cf8c1`**. Subsequent report commits are documentation, not the timed runtime revision. Earlier screens identify their own exact source in the raw evidence.

Author execution on 7 September 2026: Apple M2, 24 GiB RAM, eight CPUs, AC power, native arm64 binaries, Rust/Cargo 1.98.1 release, four Tokio workers, seed 107. `uname` reports an x86_64 Rosetta shell; the timed executables were verified as arm64. Local synthetic memory/filesystem ObjectStore fixtures are used. These results do not estimate Prisma Compute/Tigris fleet latency. Both sides retain their legitimate original/current encryption formats: current AES-GCM-SIV is not replaced by the old AES-GCM format. Compression is disabled equally for performance; compressed correctness remains gated.

The final comparisons use the same real System allocator, or the same production mimalloc allocator, on both sides. Allocation interception is disabled in these binaries. Zero allocation-counter fields therefore mean **disabled**, not allocation-free. Object and product-bridge counters remain enabled equally. Detailed tracing and CountedSystem allocation screens are separate. Builds finished before the quiet final campaign; it began after a 60-second cooldown. No builds or analysis ran during that campaign.

The serial fixture performs four append workloads, two complete 64 KiB HTTP replays, a complete 16-record/16 KiB filtered history query, and one process-cold query per block. Replay verifies all 65,536 payload bytes, HTTP status, signed terminal cursor and up-to-date state. History follows every required page and checks payloads, offsets `0,16,…,240`, and final consumed offset 255. Limits and expected results are identical. HTTP timing includes reading and decoding the complete response through connection close; each request opens a new connection. History timing is an application-level complete query, without HTTP transport.

## Final serial screening results

Five alternating paired blocks × 128 warm reads give **640 observations per case/workload**. Entries below are pooled nearest-rank percentiles. Ratios are geometric means of paired block percentile ratios, so they need not equal the ratio of the pooled columns. Confidence intervals use 20,000 paired block bootstrap draws, seed 20260907; intervals are individual, without a family-wise correction. These are exploratory screens with limited p99 precision.

| Allocator | Complete read | Original p50 / p99 µs | O1–O4 p50 / p99 µs | Paired p50 ratio [95% CI] | Paired p99 ratio [95% CI] |
| --- | --- | --- | --- | --- | --- |
| System | Hot filtered query | 368 / 418 | 393 / 582 | 1.062 [1.038, 1.077] | 1.284 [1.052, 1.853] |
| System | Replay 64 × 1 KiB | 1,490 / 2,257 | 1,250 / 2,361 | 0.957 [0.780, 1.087] | 0.851 [0.699, 1.014] |
| System | Replay 1 × 64 KiB | 1,001 / 1,898 | 820 / 1,777 | 0.903 [0.772, 1.022] | 1.039 [0.862, 1.252] |
| mimalloc | Hot filtered query | 243 / 320 | 274 / 301 | 1.147 [1.123, 1.187] | 0.980 [0.767, 1.138] |
| mimalloc | Replay 64 × 1 KiB | 1,193 / 2,480 | 1,316 / 2,165 | 1.135 [0.922, 1.372] | 0.745 [0.555, 0.977] |
| mimalloc | Replay 1 × 64 KiB | 781 / 1,333 | 992 / 1,567 | 1.254 [1.078, 1.458] | 1.284 [1.003, 1.717] |

The paired hot median regression is **6.2% with System** (95% interval +3.8% to +7.7%) and **14.7% with mimalloc** (+12.3% to +18.7%). System replay estimates are noisy. Under mimalloc, singleton replay is slower in both percentile estimates and the median confidence interval is above parity. It would be misleading to report only the favorable System replay medians or the many-record mimalloc tail.

All final System source medians/percentiles happen to fall below the old absolute ceilings (407/1,090; 1,428/3,075; 1,064/2,513 µs). That alone is insufficient: the contemporaneous original improved too. Historical System ceilings are not used to certify a different allocator. The preregistered strong rule additionally requires upper paired confidence bounds below 1 for all six primary comparisons in ten blocks of 10,000 reads per workload/revision/block. **No production candidate cleared the screen, so that larger acceptance campaign was not run.** This is a failed parity screen, not a successful performance certificate.

## What changed and what the mechanisms show

**O1 — checked frames and borrowed metadata.** Commit `142fa1607570b87f07b04c4cd75df12373b7d4b5` retains validated immutable frame metadata through filtering/decryption and borrows routing-key cache lookup keys. It preserves canonical admission and per-frame AEAD. Task-local diagnostics on DAC versus `97640d5` show **32 → 16 structural parses per complete hot query**, with 16 canonical rows, 16 physical canonical scan starts and two key derivations unchanged. Replay parses fall 128 → 64 for 64 records and 2 → 1 for a singleton. Initial global allocation screens show about 33 fewer allocations per hot query averaged over overlapping process activity; this is not request-attributed allocation accounting. Checked metadata itself increases retained frame representation size. [Implementation and tests](O1.md).

**O2 — authenticated page storage.** Commit `88f336f2b8a7c871b9f095b79639c35a7a02cc28` decrypts uncompressed frames in place into exclusively owned batch storage, reuses AAD, and shares eligible contiguous binary response storage. Tentative bytes are not published before authentication/admission. Tests cover stable payload/AAD allocations over 64 frames, failed-auth rollback, withholding and exact progress, immutable reader retention, and existing wire/crypto/resource seams. Compressed and over-limit candidates keep the bounded authenticated fallback. History/stitched pages can still coalesce; this is not universal zero-copy output. No full request-attributed peak-live-buffer or slow-client pressure campaign was completed. [Ownership details](O2.md).

**O3 — durable ring coverage.** Commit `05c0d939d25e8cc425c7a7c6f7fc3bbf05c88503`, strengthened by physical-opening ownership and enabled cross-owner tests, produces a private dense-interval witness. Only proven durable ring coverage skips the stored marker. The continuity replay fixture has ring capacity zero, so it cannot demonstrate O3's latency mechanism. Its marker count remains one per page. A separate System comparison isolates O1 versus O1+O3, with a 4 MiB ring and identical crypto/buffering code on both sides:

| Ring case | O1 p50 / p99 µs | O1 + O3 p50 / p99 µs | Marker calls | Pages per 640 queries |
| --- | --- | --- | --- | --- |
| covered | 630.54 / 1312.96 | 626.54 / 761.21 | 640 → 0 | 640 |
| partial-misses | 162.79 / 410.96 | 6.62 / 8.17 | 40,960 → 0 | 40960 |
| applied-control | 722.25 / 2200.50 | 714.92 / 1628.00 | 640 → 640 | 640 |
| evicted-control | 736.17 / 3669.88 | 709.96 / 923.71 | 640 → 640 | 640 |

Covered and partial-miss cases had 100% fixture coverage; applied and evicted controls had no usable witness. Counts are task-local, and every query checked exact progress and result bytes. The covered median ratio is 0.989 [0.980, 0.994]; the partial-miss ratio is 0.0406 [0.0402, 0.0410]. The latter walks **64 tiny pages per empty query**, an intentionally mechanism-sensitive control, not the primary hot-history workload. Applied/evicted timings also changed despite unchanged marker counts, so their apparent speedups are not attributed to marker removal. Held-marker, invalid-cache, dense-gap, trim and cross-owner correctness controls remain part of source validation. This removes a SlateDB operation, not a demonstrated cloud GET. [Coverage boundary](O3.md).

**O4 — immutable postings windows.** Commit `76d549a13cabc7507dc24fea88586492aaf2aa15` removes both clipping vectors, binary-searches overlapping runs, and feeds the existing planner an iterator. A 40,960-case differential test checks every plan field and conservative byte estimate. At 100,000 runs with a late three-run window, measured construction/planning median is **61,959 → 83 ns**; at 16 runs it is **84 → 42 ns**. These are diagnostic microbenchmarks; destruction is outside the measured slice and the small-window saving cannot explain a large service win. Final commit `13a43c8` releases the cached owner before asynchronous scans. [Window details](O4.md).

Request-correlated diagnostic logs retain slow actual page indices and nested stage durations. Durations overlap and include waits; they are not additive and their p99s are not subtracted. No separate off-CPU/lock-wait attribution or whole-HTTP stage profile was obtained. The diagnostics establish deleted work, not a complete causal explanation of replay tails.

## O5 encrypted batch reuse: promising feasibility result

The profile confirms **16 physical canonical scan starts per complete hot query despite warm postings**. An out-of-tree probe caches checked encrypted canonical spans; exact filtering, AEAD, page admission and cursor construction still execute. Its key binds physical Db opening, route, incarnation and exact canonical bounds. It only admits completely scanned, nonempty spans. This is a synthetic immutable-history experiment, not shipping cache code.

The probe carves 2 MiB from the existing Foyer allowance, reserving 64 KiB for its preallocated map, limiting entries to 256 and individual fills to 64 KiB. It immediately compacts captured backend slices, packs retained ciphertext, conservatively charges metadata/transient copies, and keeps the reservation with the Bytes owner while readers retain it after eviction. Cancellation, retained-reader ownership, wrong-key authentication and oversized bypass tests pass. Cache-on/off use the **same executable**, wrapper and reduced Foyer allowance.

| System case | Hot p50 / p99 µs | Paired p50 vs original | Paired p99 vs original |
| --- | --- | --- | --- |
| original | 368 / 418 | — | — |
| source | 393 / 582 | 1.062 [1.038, 1.077] | 1.284 [1.052, 1.853] |
| off | 403 / 460 | 1.096 [1.086, 1.105] | 1.110 [1.031, 1.195] |
| on | 168 / 188 | 0.457 [0.447, 0.468] | 0.448 [0.428, 0.463] |

For 640 warm queries, cache-on records **10,240 hits and zero physical canonical scan starts**; cache-off records zero hits and **10,240 scan starts**. Both still return two application pages per query. Warm residency is 16 entries and **109,376 reserved bytes including the 65,536-byte map reservation**, inside the 2 MiB carve-out. These counters do not imply 16 remote object GETs saved per query. Process-global background object activity remains visible separately.

Relative to the same binary with caching disabled, the paired hot p50 ratio is **0.417 [0.411, 0.424]**, and p99 is **0.404 [0.383, 0.426]**. Relative to the original, the pooled hot median falls **368 → 168 µs** (54% lower). This is useful evidence to continue O5, but it cannot remove the production hold. The probe lacks retirement/retention invalidation, complete project/descriptor lifecycle integration, per-tenant admission and identical-fill coalescing. It has not passed rotating-range/high-cardinality, multi-tenant cache pressure, lifecycle or sufficient cold-tail campaigns. No mimalloc cache-probe comparison was run. Replay and append behavior are not its target and do not show a general win.

The first cache screen is retained but superseded for memory acceptance: inspection found that an early captured Bytes slice could pin a larger backend allocation. Its disposition was recorded before reading its timing results. The final results above use the corrected immediate-compaction probe.

## Offered load and guardrails

Three alternating blocks × 1,000 reads per shape/rate/version produce **108,000 complete scheduled reads**, with **zero recorded incorrect responses, errors or timeouts**, alongside **4,320 successful background appends**. The client limit is 16 in flight; the deadline is ten seconds from scheduled arrival, including queueing. Each replay measures HTTP full-body completion; each history query measures complete application traversal. Period 2,000 µs is 500 scheduled reads/s; period 250 µs is 4,000/s. Each phase schedules 40 background appends ten milliseconds apart on the same engine. These short runs do not establish sustainable throughput or fleet admission behavior.

| Allocator | Workload (scheduled interval) | Original p50 / p99 µs | O1–O4 p50 / p99 µs | O5-on p50 / p99 µs |
| --- | --- | --- | --- | --- |
| System | history:hot-2000us | 2,445 / 4,645 | 2,594 / 4,009 | 1,998 / 3,128 |
| System | history:hot-250us | 1,817 / 9,479 | 1,765 / 10,002 | 1,593 / 3,108 |
| System | replay:1024-bytes-2000us | 2,770 / 277,133 | 3,094 / 410,903 | 3,227 / 350,367 |
| System | replay:1024-bytes-250us | 26,340 / 171,550 | 31,754 / 85,391 | 25,559 / 46,329 |
| System | replay:65536-bytes-2000us | 2,727 / 221,272 | 2,539 / 232,162 | 2,789 / 255,849 |
| System | replay:65536-bytes-250us | 2,531 / 29,350 | 2,639 / 23,399 | 3,202 / 31,083 |
| mimalloc | history:hot-2000us | 1,769 / 2,943 | 2,374 / 3,802 | not run |
| mimalloc | history:hot-250us | 1,670 / 3,663 | 1,770 / 3,480 | not run |
| mimalloc | replay:1024-bytes-2000us | 3,283 / 369,829 | 3,122 / 346,298 | not run |
| mimalloc | replay:1024-bytes-250us | 8,558 / 26,333 | 19,216 / 26,073 | not run |
| mimalloc | replay:65536-bytes-2000us | 2,799 / 297,218 | 2,861 / 293,833 | not run |
| mimalloc | replay:65536-bytes-250us | 2,609 / 24,261 | 2,824 / 29,011 | not run |

The cache-on history advantage persists under this limited load, particularly at 4,000/s. The source does not meet a general load-parity guard: for example, mimalloc history at 500/s has paired median ratio 1.349 [1.273, 1.419], and many-record replay at 4,000/s has median ratio 2.653 [1.267, 5.428]. Three-block intervals remain exploratory.

Long HTTP tails occur in both old and new source. At 500/s, several runs contain clusters above 100 ms at different arrival indices, not just a single first request; the retained slow-request files identify those samples. The current data does not distinguish connection-close transport, scheduler, backend or other lifecycle delays. These are unresolved measurement/causality limits, not a newly diagnosed production defect. Rate phases run in fixed order (500 then 4,000/s), so cross-rate comparisons also include phase/warmth differences. No failed sample was dropped or relabeled as a fast refusal.

Background absorption, deliberate flush/compaction interference, retained slow HTTP clients and many-tenant/noisy-neighbor **performance** campaigns were not executed here. Existing correctness/capacity CI is not substituted for them. There were no pre-agreed numerical append, cold-memory or tenant-latency tolerances beyond the primary rule; no new margins are chosen after seeing results.

All four append controls were retained (640 samples each):

| Allocator | Append | Original p50 / p99 µs | O1–O4 p50 / p99 µs | Probe off | Probe on |
| --- | --- | --- | --- | --- | --- |
| System | product-1024 | 6,539 / 7,845 | 6,574 / 9,267 | 6,444 / 15,521 | 6,611 / 8,100 |
| System | product-65536 | 6,335 / 7,716 | 6,370 / 10,448 | 6,346 / 7,924 | 6,301 / 13,565 |
| System | raw-1024 | 6,578 / 13,707 | 6,556 / 7,886 | 6,560 / 28,847 | 6,569 / 7,773 |
| System | raw-65536 | 6,480 / 8,679 | 6,524 / 9,942 | 6,478 / 7,545 | 6,470 / 7,676 |
| mimalloc | product-1024 | 6,600 / 10,023 | 6,605 / 7,808 | — | — |
| mimalloc | product-65536 | 6,398 / 10,151 | 6,318 / 7,489 | — | — |
| mimalloc | raw-1024 | 6,559 / 7,948 | 6,563 / 8,158 | — | — |
| mimalloc | raw-65536 | 6,454 / 8,077 | 6,405 / 9,891 | — | — |

Corrected source and cache variants retain **zero measured product/application payload bridge copies** in both serial and load runs. The historical a7 baseline records one bridge copy per measured product append; this is not a new regression in the candidate. Allocation counters in final binaries are disabled. Global object attempts/range bytes and per-process RSS remain in the raw/summary files and include background activity; they are not request-attributed cloud costs.

| Allocator | Case | History process max RSS range, MiB | Append/replay process max RSS range, MiB |
| --- | --- | --- | --- |
| System | off | 15.2–15.7 | 84.0–99.0 |
| System | on | 15.3–15.5 | 84.9–98.2 |
| System | original | 13.4–13.7 | 83.7–96.3 |
| System | source | 15.2–15.6 | 84.9–85.5 |
| mimalloc | original | 19.4–19.8 | 83.2–94.5 |
| mimalloc | source | 21.3–21.4 | 84.2–95.2 |

These are ranges of process peak RSS across blocks, not per-request peak-live allocations. The cache carve-out does not prove total RSS parity under pressure. For example, final System source history processes peak at roughly 15.2–15.6 MiB versus 13.4–13.7 MiB for the original. This is retained as a guardrail observation.

| Allocator | Case | Cold observations | Median µs | Maximum µs (not p99) |
| --- | --- | --- | --- | --- |
| System | off | 5 | 6036 | 23896 |
| System | on | 5 | 6907 | 8368 |
| System | original | 5 | 4073 | 14544 |
| System | source | 5 | 6740 | 11533 |
| mimalloc | original | 5 | 4125 | 9505 |
| mimalloc | source | 5 | 3638 | 8522 |

There are only five process-cold queries per case. The raw legacy emitter/summary includes a mathematical p99 field for them; **the report makes no cold p99 claim** and reports only median and maximum. A sufficient independent cold-tail sample is still required for acceptance.

## Validation and retained attempts

[Exact runtime-source CI succeeded at 13a43c8](https://github.com/prisma/streams/actions/runs/34142747263), as did [workflow lint](https://github.com/prisma/streams/actions/runs/34142747242). CI covers full Rust, provenance/static gates, SDK runtime/package matrices, server conformance, livefeed fleet, platform end-to-end and multitenancy/capacity jobs. The nightly noisy campaign was skipped on the PR and is not claimed as executed. Earlier exact-source CI at 97640d5 and a285bef also succeeded.

Local combined gate at `f4cff1ac8c9444574bc4e101b3b1e503b608c450`: **900 library tests passed**, including the isolated capacity case; architecture/provenance/Clippy/multitenancy gates passed. Subsequent source checks and the enabled cross-owner regression passed; final-source CI covers the later runtime changes. SDK **33 tests**, authentication/capability vectors, typecheck and build passed locally. Final probe reservation/cancellation tests passed in the exact final System probe binary. Authenticated-format, corrupt-row/nonmatching corruption, invalid ring, compression bounds, partial progress, peer/wire, lifecycle and corrected retirement controls remain green. This is author execution and CI evidence, not independent cryptographic or release approval.

Earlier results are retained: O1's first screen did not close the hot gap; the cumulative screen also failed hot median parity. Replay varied materially across screens. A sandbox listener failure before requests, O4 nested-test compile failure, two Clippy issues, an initially vacuous cross-owner test, probe closure compile failure, and the superseded cache ownership variant are all recorded with their fixes. One roughly one-second metadata/hash operation overlapped an earlier cumulative screen; that screen is exploratory and not the final quiet campaign. No historical gate baseline was weakened to pass these changes.

## Decision and reproducibility

O1–O4 are **mechanism-verified experimental changes**, retained on the draft branch for review; full-workload performance acceptance is **inconclusive/failed parity**, not accepted. O5 is **feasibility demonstrated for repeated immutable hot history**, with production acceptance pending. The next performance work should complete O5's lifecycle/admission boundary and exercise cold/rotating/multi-tenant pressure, while separately profiling full HTTP replay under offered load. Only a production-ready candidate that clears those screens should advance to the preregistered larger campaign.

[Machine-readable results](performance.json) include full block distributions, paired intervals, diagnostics, object/memory guardrails and source CI identity. The local portable evidence bundle includes all source snapshots with measurement hooks, copied native binaries, exact base revision/tree identities, build logs, test receipts, original raw samples, scripts, negative controls and unsuccessful attempts. Its SHA-256 inventory is author-verified; source snapshots contain synthetic fixtures. Nothing was uploaded as a raw evidence bundle. Source/report commits are published through the draft PR. The previous task's separate evidence-upload hold remains unchanged.

The bundle's `MANIFEST.json`, `SHA256SUMS` and verification transcript bind its contents. `harness/prepare.py`, `build.sh`, `measure.py`, `run-guards.py`, `summarize.py` and `extra-summary.py` show the commands and measurement logic. Final source preparations apply the common harness, optional probe, matched allocator, then offered-load harness before compilation. All final variants use the same offered-load implementation. Stored source snapshots and binary hashes are authoritative for older attempts; later generator edits must not be assumed to reconstruct an earlier attempt. Use the saved snapshots to rebuild those cases. The ledger keeps external workload acceptance with the repository/product owner.
