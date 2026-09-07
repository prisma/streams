# Local paired performance comparison

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
