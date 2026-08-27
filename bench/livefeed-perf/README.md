# LiveFeed performance characterization (round 12)

Three-arm controlled comparison + capacity study for the post-rewrite
SSE engine. Correctness/survivability is certified elsewhere (the 11.4
fleet battery, the 11.6 canary); THIS harness answers the economic
questions: memory per subscriber/feed, CPU per delivery, write impact,
and the 1-GiB instance envelope.

## Arms

| Arm | Commit     | Binary                      | Engine |
|-----|------------|-----------------------------|--------|
| A   | `1834b726` | `arms/streams-dual-1834b726`| `STREAMS_SSE_ENGINE=legacy` (mature direct/LiveHub) |
| B   | `1834b726` | same binary                 | `STREAMS_SSE_ENGINE=livefeed` (pure architecture A/B) |
| C   | `3a8016e6` | `arms/streams-rc-3a8016e6`  | sole engine — THE certified `v0.2.0-rc.2` artifact (sha `1eb36b10…`) |

A-vs-B isolates the architecture on one binary; B-vs-C isolates the
final deletion/hardening/deps. Never compare across unrelated commits.

## Environment

Docker (OrbStack, linux/amd64 via Rosetta): `perf-store` (s3lite,
`--latency-ms 2`), `perf-server` (THE MEASURED CGROUP: `--cpus 1
--memory 1g --ulimit nofile=32768`), `perf-gen` (uncapped loadgen).
Fresh bucket + processes per run; runs strictly serial; arm order
`A B B A C C` per shape.

## Runs

- `run-one.sh <a|b|c> <feeds> <subs_per> <out> [tag]` — one phased run:
  warmup → create → park → idle(10m) → sparse(1/s) → fanout(1000
  deliveries/s @1KiB) → mixed(+200 bg writes/s) → slow-client(1%
  paused) → settle → disconnect → teardown(10m observation).
- `run-density.sh` — campaign 1: 1000 subscribers across
  `1000x1 500x2 100x10 10x100 1x1000`.
- `run-capacity.sh` — campaign 2: the per-instance ladder on arm C
  (worst-case Nx1 geometry first).
- `analyze.py <results>` — medians per shape×arm, the memory-model fit
  (`RSS ≈ base + per_sub·S + per_feed·F`), and the decision gates
  (A→B regressions >10% flagged; B→C drift >5% flagged).

Every run emits an immutable `manifest.json` (per `schema.json`) plus
raw 10-second series (`series.jsonl` client+debug view,
`proc.jsonl` /proc RSS/PSS/CPU/fds). Reconciliation is EXACT per
subscriber (bitmaps); a run without `verdict: PASS` is not evidence.

## Gates (round-12 charter)

Regression flag when medians show >10% worse append/delivery latency
or CPU per delivered record, or materially higher steady/residual RSS
(A vs B). Arm C must stay within ~5% of arm B. Do NOT reopen the
shared-feed reader optimization unless the realistic `10x100` @10k
deliveries/s shape shows real CPU/latency pressure.
