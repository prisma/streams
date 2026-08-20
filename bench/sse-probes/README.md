# SSE probes (#267/#268/#275)

Local measurement harnesses against a release binary + `s3lite-ab`
(bench/costab/bin). All three boot a fresh server on 127.0.0.1:8090
and clean up on exit.

- `sse-slope.sh` — N parked `:sse` subscribers (default 2000), RSS
  slope from /v1/debug/load + `sse_future_bytes` + post-disconnect
  residual. Phase-1 result (2026-08-20): SSE increment ≈8.6 KB/sub.
- `ka-slope.sh` — the CONTROL: N plain idle keep-alive connections,
  no SSE. Isolated the ~53 KB/conn hyper/axum floor (task #269).
- `hub-smoke.sh` — SSE_LIVE_HUB=1 delivery smoke: N subscribers on
  ONE stream (one hub, one pump), 3 appends, 10 sampled subscribers
  must receive all of them through shared prepared bytes;
  `sse_hub_future_bytes` reported (760 B post-#273).

Run: `bash bench/sse-probes/<probe>.sh` (env N=... to resize).
