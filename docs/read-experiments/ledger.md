# Read experiment ledger — 7 September 2026

Engineering owner: Codex experiment task. Decisions below are author dispositions, not independent reviewer or product-owner acceptance. Final runtime source: `13a43c8662b632be1dadb4110363f88a7f1cf8c1`; [results and limitations](results.md), [machine-readable results](performance.json), [draft PR #19](https://github.com/prisma/streams/pull/19).

| Experiment | Immediate source control → implementation | Mechanism / verification | Performance disposition |
| --- | --- | --- | --- |
| O1 | `dac9e90` → `142fa16` | Checked immutable frames, borrowed routing metadata and cache-hit lookup. Complete hot-query structural parses 32 → 16; AEAD and canonical admission retained. First parent/original screen plus cumulative screen retained. | MECHANISM_VERIFIED. Hot median parity not restored; full-workload acceptance INCONCLUSIVE. Retained for draft review. |
| O3 | `142fa16` → `05c0d93`; final owner corrections `97640d5`, `a285bef` | Covered durable pages skip marker; partial misses prove only consumed interval; applied/evicted/gap/cross-owner controls preserve boundary. Conditional ring test: 640 → 0 marker calls for covered queries, 40,960 → 0 for tiny partial-miss pages. Primary replay fixture has ring disabled. | MECHANISM_VERIFIED. Conditional benefit demonstrated; general replay p99 acceptance INCONCLUSIVE. |
| O4 | `05c0d93` → `76d549a`; nested test repair `b1fc921`; final lifetime correction `13a43c8` | 40,960 differential plans; exact large-window index coverage. Late 100,000-run planning/clipping median 61,959 → 83 ns; 16-run case 84 → 42 ns. Owner released before async scans. Isolated repaired O4 control `7c913db` retained. | MECHANISM_VERIFIED. Algorithmic benefit demonstrated; hot full-query parity not restored. |
| O2 | `76d549a` → `88f336f`; validation adapters `b1fc921`, `f4cff1a` | Private uncompressed batch/AAD ownership; eligible response sharing; authenticated rollback/withholding, exact progress and immutable-reader tests. Compressed fallback stays bounded. | MECHANISM_VERIFIED. Replay results mixed across allocator and load. Full-workload acceptance INCONCLUSIVE; no positive overall parity claim. |
| O5 | Final `13a43c8` plus out-of-tree probe; same-binary disabled control | Warm hot-query scan starts 16 → 0, identical full result, 2 MiB budget carved from Foyer, immediate compaction and retained-owner reservations. Final memory ownership/cancellation tests pass. | FEASIBILITY_DEMONSTRATED for immutable hot history only. Production acceptance REJECTED at current scope: lifecycle/invalidation, tenant admission, fill coalescing and pressure/cold campaigns missing. Probe is outside shipping source. |

The combined source **fails the parity screen**: paired hot median +6.2% with System and +14.7% with mimalloc relative to contemporaneous original runs. The larger ten-block × 10,000-read acceptance campaign was not run because no production candidate cleared screening. Five-block p99 intervals and three-block offered-load intervals remain exploratory. No cold p99 is claimed from five observations.

## Source and evidence controls

- Exact runtime source CI succeeded: [34142747263](https://github.com/prisma/streams/actions/runs/34142747263); workflow lint [34142747242](https://github.com/prisma/streams/actions/runs/34142747242). Report commits do not change the timed revision.
- Local full gate at `f4cff1a`: 900 library tests; static/provenance/Clippy/multitenancy checks passed. Final-source CI covers subsequent ownership/lifetime fixes. SDK 33 tests, auth/capability vectors, typecheck and build passed locally.
- Final quiet screens include all eight continuity workloads with matched System and mimalloc allocators. Full-body/cursor oracles passed. Corrected product bridge copies remain zero.
- 108,000 offered-load reads and 4,320 background appends completed with zero recorded errors/timeouts. Load parity and causal explanation of long HTTP tails are unresolved. Background absorption, deliberate compaction interference and many-tenant performance campaigns are not claimed.
- Task-local diagnostic timing includes nested/overlapping waits; no separate off-CPU causal profile was obtained. Allocation interception is disabled in final timings; RSS is process peak, not per-request peak-live memory.
- Failed builds/listener attempt, initial vacuous test, superseded cache-capture variant, and all earlier screens remain in local portable evidence. SHA-256 verification is author internal-consistency evidence, not an independent signature.

## Existing external acceptance

The [review-of-89884ab ledger](../review-followup/2026-09-07-ledger.md) is preserved with its original scope. R17-B and V01 remain tied to their corrected-source evidence and reviewer dispositions; these performance experiments do not reopen them or silently relabel them independently accepted. E01 cryptographic/operational review and E04 deployed platform campaigns remain external.

**E03 remains PERFORMANCE_DECISION_PENDING.** This task measured optimisations; it did not grant a workload-specific regression tolerance. Product/repository owners must accept named latency/memory tradeoffs or continue optimisation. O5's warm-query gain is not a substitute for that decision. No merge, deployment, raw-bundle upload or prior evidence-upload approval is implied.
