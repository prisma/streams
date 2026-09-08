# Follow-up read experiments

This is the permitted minimal source harness. It does not contain raw latency/stage samples, native binaries, or the complete evidence archive. Those remain under the separate upload hold. Published block summaries identify their original log hashes without implying that reviewers possess or independently verified those raw logs.

The harness produces explicitly instrumented Git archives. `SCREEN-SOURCE.json` records the exact base revision/tree, archive hash and every source modification's before/after hashes. Each build verifies those inputs and copies a native binary with its own receipt/hash. These are engineering screen binaries, not clean-source acceptance binaries. The source gate and CI validate the uninstrumented production candidate separately.

## Build before measuring

Run from a full checkout with the fixed baseline available, Python 3.12+ and the pinned Rust dependencies/toolchain. Choose an external `experiment_dir` and an available shared `CARGO_TARGET_DIR`; builds are sequential and require exclusive use of that target. The scripts refuse to overwrite prepared source directories.

```bash
python3 scripts/read-experiments/followup/prepare.py a7e2070f3b4346b3e54d552069ff91c56e900130 "$experiment_dir/system/control" --allocator system
python3 scripts/read-experiments/followup/prepare.py eb5ab8ad7a1459b6c679b72bf342a3af73e0bede "$experiment_dir/system/candidate" --allocator system
python3 scripts/read-experiments/followup/prepare.py a7e2070f3b4346b3e54d552069ff91c56e900130 "$experiment_dir/mimalloc/control" --allocator mimalloc
python3 scripts/read-experiments/followup/prepare.py eb5ab8ad7a1459b6c679b72bf342a3af73e0bede "$experiment_dir/mimalloc/candidate" --allocator mimalloc
python3 scripts/read-experiments/followup/build.py "$experiment_dir"
```

System and mimalloc use their native global allocators; allocation interception is disabled in latency runs. Zero allocation-counter fields therefore mean disabled, not zero allocations. The allocation diagnostic below owns that separate question. A fixture-only hook uses the production plaintext owner rather than the larger test observer.

## Run and summarize

Stop builds and other agent-initiated heavy work before running. `run.py` seeds independent durable data through each source's actual append/absorber implementation. In particular, the candidate writes current GCM-SIV frames; copying original AES-GCM fixtures into the candidate would test a different cryptographic path and is not a valid substitute.

```bash
python3 scripts/read-experiments/followup/run.py "$experiment_dir" --blocks 1 --reads 16 --tests read_history --campaign preflight
python3 scripts/read-experiments/followup/run.py "$experiment_dir" --blocks 5 --reads 128 --campaign matched-screen
python3 scripts/read-experiments/followup/summarize.py "$experiment_dir/matched-screen"
```

Do not reuse a partially seeded directory after a failed setup. Preserve the failure and move that directory aside before retrying. Completed fixtures may be reused by the subsequent campaign. The reported v2 screen reuses the independently seeded v1 corpora: v2 changes only read-cache credit, lookup and compaction, while append, absorber and encryption sources are unchanged. Its candidate read binary is pinned separately; no control corpus was copied into a candidate. The first canonical query in each process is recorded separately as process-cold; five such observations cannot establish cold p99.

Each allocator has three conditions: original, corrected candidate with cache disabled, and the same candidate binary with cache enabled. Pair IDs bind the workload/format/block; AB/BA order reverses on even blocks. Summaries use those IDs, never array position, for 20,000 paired bootstrap resamples with fixed seed 20260907. Latency percentiles select `sorted[floor((N-1) × p)]`, matching the executed Rust emitter. The published guardrails are fixed before screen execution; an inconclusive interval is not a pass.

The history partition has 32 projects × 256 records, 1 KiB each, with a hot record every 16 offsets. Plain, compressed and alternating compressible/incompressible formats are separate datasets. Hot matches are even/compressible records in the mixed dataset; high-cardinality queries alternate formats. The separate allocation diagnostic includes mixed delivered pages. Retained-reader runs hold sixteen complete results; the tenant case cycles across the 32 projects. The expanded shared partition must be distinguished from the earlier one-project feasibility fixture when interpreting historical absolute ceilings.

The original serial append and complete connection-close HTTP replay workload remains primary. `transport.rs` adds a separately labeled persistent-connection control and scheduled-arrival reads at 500/s and 4,000/s, sixteen in flight, ten-second deadlines from arrival, and forty concurrent background appends per case. Errors, timeouts, exact bodies and exact cursors remain in the oracle.

Request-correlated diagnostics record client wake/admission/connect/write/headers/body/close milestones and optional server application/page/decode durations. The server durations are nested and not additive. Diagnostic logging can perturb those timings. The summarizer joins request IDs and computes within-request client deltas and slowest-one-percent cohorts; it never subtracts overlapping stage percentiles. These results do not replace the primary full-completion timings or diagnose a transport/backend defect by themselves.

The reviewer-accessible arithmetic check is `python3 scripts/read-experiments/followup/verify_published.py docs/read-experiments/followup`. It reconstructs every reported ratio and interval from identified block pairs and checks their source/binary mapping. It does not claim verification of the held raw samples. `evaluate_screen.py` takes the same directory and applies the published guardrails; the reported result is HOLD. The summarizer's additional within-request stage deltas and wake/queue fractions are aggregate diagnostics, with no raw events in the published output.

## Allocation diagnostic

`allocation/prepare_allocation.py`, `build_allocation.py` and `run_allocation.py` reproduce the measured `1f4cab1` control and `c58ddc3` flat-batch candidate under each allocator. Preparation/build follow the same immutable-archive receipt contract. The synchronous thread-local allocation region excludes input/frame construction. It records requested allocations, reallocations and moved realloc bytes, explicit old aggregate copies and peak live requested Rust storage. C/zstd workspace and allocator fragmentation are outside that region; whole-process RSS is separately recorded by `/usr/bin/time -l`.

Each binary executes two 1 MiB plaintext shapes, three formats, three completion/failure boundaries and five measured trials after warm-up. The complete error result's allocation must not be mislabeled a plaintext leak. Exact owner-capacity/last-drop regressions are ordinary source tests, separate from this diagnostic.
