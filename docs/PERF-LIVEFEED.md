# LiveFeed performance characterization (round 12)

Controlled three-arm study answering the round-12 charter: the
economic and operational envelope of the LiveFeed engine after the
rewrite. Harness, raw manifests and 10-second series:
`bench/livefeed-perf/` (all runs reconcile deliveries EXACTLY per
subscriber; a non-PASS run is not evidence and none is cited here).

**Arms.** A = legacy direct/LiveHub, B = livefeed, both the SAME
dual-engine binary at `1834b726` (engine chosen by env); C = the
certified `v0.2.0-rc.2` artifact (`1eb36b10…`) at `3a8016e6`.
**Environment.** linux/amd64 (Rosetta), s3lite store at 2 ms, the
server alone in a 1-CPU / 1-GiB cgroup (nofile 32768), the pinned
compute-1g memory knobs, fresh bucket/process per run, serial runs in
`A B B A C C` order per shape. Host ran unrelated workloads; the
alternation + per-cell medians absorb that noise.

## 1. Legacy versus LiveFeed (density sweep, 1,000 subscribers)

| Shape | Verdict |
|---|---|
| `10x100` (product target) | **Parity.** Append p99 −4.8%, delivery p99 0%, CPU/delivery ±0%, idle RSS −10% for livefeed. |
| `1x1000` (max sharing) | Parity (delivery +6%, CPU +1.6%). |
| `1000x1` (breadth) | Dual-commit livefeed dipped (+30% append p99, +12% CPU); **the final RC recovered it fully** (C ≈ A). |
| `500x2`, `100x10` (many shared feeds) | Delivery p99 3–8× worse on livefeed **under the default retention budgets** — see §2; with adequate retention: parity. |

- Memory per subscriber: **43–60 KB across every shape and arm**
  (matches the workload-cert 55–70 KB estimate). Livefeed ≤ legacy.
- CPU per delivered record: ~400–500 µs at fanout 10–100, ~330 µs at
  fanout 1000, ~800–1400 µs at fanout 1–2 (breadth-dominated);
  livefeed ≤ legacy at most shapes (−9% at `500x2`).
- Idle: 1,000 parked subscribers cost no measurable CPU on either
  engine; RSS flat over the 10-minute idle windows; teardown returns
  feeds/reserved bytes to zero every run.

**B vs C regression gate (±5%):** met at the product shapes; at
`1000x1` C is BETTER than B (the 11.8 hardening); the only
above-noise drift is `1x1000` delivery p99 +23% (95→116 ms, two runs
each, absolute values small) — noted, not actioned.

## 2. The one real finding: shared-feed retention depth

At hundreds of SHARED feeds in one project, the per-project retention
backstop (`SSE_FEED_PROJECT_BYTES`, defaulted to global/4 = 4 MiB in
round 10) saturates; every over-cap publication clears that feed's
ring and floors parked followers → typed lag-disconnect + resume
churn. Zero loss (the resume contract holds exactly — one sweep ran
64,028 cut/resume cycles with zero missing records), but delivery p99
inflates 3–8× and reconnect work amplifies CPU. The legacy hub served
the same pressure uncached in place — this is livefeed's one
deliberate contract divergence made visible.

Causal chain proven by intervention at `500x2` (arm C):

| Config | Lag/resume cycles | Mixed delivery p99 |
|---|---|---|
| 16 MiB global / 4 MiB project (pinned) | 64,028 | 354 ms |
| 64 MiB global / 4 MiB project | 64,028 | 352 ms (project cap binds) |
| 64 MiB global / 16 MiB project | 16,742 | 333 ms |
| 64 MiB global / 64 MiB project | **1,556** | **137 ms ≈ legacy 134 ms** |
| `1250x2` deep retention | **0** | **35 ms** |

**Recommendation (E4 closure):** raise the 1-GiB profile to
`SSE_FEED_TOTAL_BYTES=67108864` and `SSE_FEED_PROJECT_BYTES=33554432`
(the backstop stays meaningful for isolation but stops binding at
realistic shared-feed counts). Retention is exactly accounted; the
memory headroom exists (10,000 parked subscribers idle at 307 MB).
Do NOT build a dedicated shared-feed reader: with adequate retention
the realistic shapes are at parity or better.

## 3. The 1-GiB envelope (arm C, production-share write load)

Ladder at ~300 writes/s (the per-instance share of the production
workload), writes confined to 100 streams, subscribers scaling:

| Rung | Result | Idle RSS | Peak RSS |
|---|---|---|---|
| `1000x1` | PASS | 86 MB | 389 MB |
| `2500x1` | PASS | 135 MB | 417 MB |
| `5000x1` | FAIL (peak 498 MB, 440 admission sheds) | 206 MB | 498 MB |
| `25x100` (2,500 subs) | PASS | 111 MB | 284 MB |
| **`100x100` (10,000 subs)** | **PASS** | **307 MB** | **399 MB** (p99 54 ms, zero reconnects) |

- Memory model: **base ≈ 46 MB (post-create) + ~26 KB/connection +
  ~8–10 KB/feed.** The `5000x1` failure is the write-phase absorber
  transient stacking on 206 MB of feed+connection state — the ceiling
  is FEED COUNT (breadth), not connections: 10,000 connections at
  fanout 100 pass with 101 MB of headroom.
- **`S_server_instance`:** ≥ **10,000** at the product fanout
  geometry; **2,500** at the all-solo worst case, on the current
  profile. Safe `SSE_MAX_CONNECTIONS`: **2500** as the
  geometry-agnostic profile cap (up from the 1200 placeholder);
  fanout-dense cells can run 10k+ behind a geometry-aware admission.
- **Fleet sizing for 100k subscriptions** (`K = ceil(100k/(0.7·S))`):
  **15 instances** at the product geometry; 58 at the all-solo worst
  case. The earlier plan's placeholder assumed the public-edge wall;
  these are direct-instance numbers.
- Dominant bottleneck, in order: write-transient RSS at high feed
  breadth (absorber machinery, unchanged by the rewrite); shared-feed
  retention depth (§2, config fix); CPU at ~0.4–0.8 core per 1,000
  deliveries/s.

## 4. Status and remaining work

Executed: controlled A/B density + throughput sweep, final-RC
regression comparison, local 1-GiB capacity ladder + attribution
geometries, and the retention-budget interventions. Remaining (cloud):
the in-VPC Compute ladder to certify `S_server_instance` on real
instances, and the full Tigris combined workload at the highest safe
rung — both should run AFTER the profile change ships, so they
certify the config that will actually operate.
