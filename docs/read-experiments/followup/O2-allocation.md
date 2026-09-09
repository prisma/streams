# O2 compressed and mixed allocation diagnostic

These are executed synchronous allocation diagnostics, separate from native service latency acceptance. Control source is `1f4cab1fe7f8f8613837f9ce2584659fdf6fa133`; the final flat-metadata candidate measured here is `c58ddc3e0bd387ed20b416ce16cf32f9e5509f54`. Subsequent O5 work shares the exact-storage primitive and has separately identified source and timing receipts. Do not relabel these allocation observations as measurements of a later source revision.

Four immutable instrumented binaries (two revisions × System/mimalloc) each executed 90 cases: two 1 MiB plaintext shapes (64 × 16 KiB and 1,024 × 1 KiB), plain/compressed/mixed frames, complete/withheld/authentication-failure boundaries, and five measured trials after a discarded warm-up. All 360 cases verified exact payload/count/completion or the expected authentication error with no published plaintext. The complete returned error owns 48 allocator-observed bytes in the bad-auth case; that is an error string, not retained plaintext.

The earlier candidate `870534e` was also measured. Its per-owner metadata vectors created avoidable small allocations; commit `c58ddc3` replaced them with one flat record vector and block ranges. Those earlier results remain local and are not substituted for the final flat-metadata diagnostic below.

## 1,024 × 1 KiB complete pages

Medians over five trials. Values are bytes, not KiB. `Moved` counts the old allocation size only when a realloc returned a different pointer. `Requested` is cumulative requested capacity, including growth requests, not RSS or bytes copied. Explicit aggregate copies are measured separately.

| Allocator / format | Version | Allocation calls | Realloc calls | Requested | Moved | Explicit aggregate copy | Peak live requested |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| System / plain | control | 18 | 9 | 1,214,840 | 26,896 | 0 | 1,165,859 |
| System / plain | candidate | 19 | 9 | 1,231,416 | 26,896 | 0 | 1,182,243 |
| System / compressed | control | 11,279 | 7,175 | 676,031,797 | 1,158,400 | 1,048,576 | 1,232,433 |
| System / compressed | candidate | 12,310 | 7,184 | 139,807,709 | 627,648 | 0 | 1,338,905 |
| System / mixed | control | 6,161 | 4,104 | 606,796,664 | 966,720 | 524,288 | 1,232,484 |
| System / mixed | candidate | 7,704 | 3,601 | 70,670,880 | 867,568 | 0 | 1,340,148 |
| mimalloc / compressed | control | 11,279 | 7,175 | 676,031,797 | 11,009,856 | 1,048,576 | 2,020,824 |
| mimalloc / compressed | candidate | 12,310 | 7,184 | 139,807,709 | 2,194,944 | 0 | 1,339,929 |
| mimalloc / mixed | control | 6,161 | 4,104 | 606,796,664 | 9,977,680 | 524,288 | 2,018,827 |
| mimalloc / mixed | candidate | 7,704 | 3,601 | 70,670,880 | 1,162,768 | 0 | 1,340,148 |

The aggregate plaintext copy is removed. The compressed mimalloc moved-byte median falls about 80.1%, and mixed falls about 88.3%. This is not an allocation-free claim: the candidate retains 1,204,224 requested bytes in the compressed/mixed complete output versus 1,114,136 for the control, and makes more small allocations. System's peak live requested storage rises about 8.6–8.7%; mimalloc's falls about 33.6%. These tradeoffs remain part of performance acceptance.

The task-local allocator region excludes frame encoding and input construction. Counts cover Rust global-allocator requests, not C/zstd workspace or allocator slab/fragmentation overhead. The runner also captures process-wide `/usr/bin/time -l` RSS separately; it is not attributed to one case or mistaken for the exact owner-capacity oracle. Actual frozen-scan and slow-body capacity/release regressions are in [O2-A](O2-A.md).
