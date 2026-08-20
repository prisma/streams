# Compute edge buffers streaming responses (breaks SSE) — minimal repro

**TL;DR** — The Compute edge (`cv-*.prisma.build`) applies response
buffering with ~8–16 KB flush granularity to `text/event-stream`
responses. A Server-Sent-Events response that trickles small events
never fills the buffer, so the client receives **nothing — not even
response headers** — while the origin believes it is streaming. The
edge honors **`X-Accel-Buffering: no`** as a per-response opt-out
(verified below), so the fix on the platform side is likely one
config line: disable buffering for streaming content types by
default, or document the header.

Repro is a single dependency-free Bun file, `edge-repro.ts`
(~200 lines): an SSE origin that heartbeats every 5 s + a probe
client. The same app passes everything on localhost.

## Run it

```bash
# 1. deploy the origin (any Compute project)
compute-cli deploy --path . --http-port 8080 --service-name edge-repro

# 2. probe it from anywhere
bun edge-repro.ts probe https://<your-cv-url>

# sanity: same probe against a local server is fully green
bun edge-repro.ts server &
bun edge-repro.ts probe http://127.0.0.1:8080
```

## Observed 2026-08-20 (eu-central-1, service cps_buohwmiho0z4np42te6c1g2x)

**1. SSE requests reach the origin; clients receive nothing.**
Probe (115 connection attempts, then 120 s parked):

```
Phase A  sequential 15 connects: ok=0 fail=15
Phase B  burst 100 connects:     ok=0 timeout=100 error=0
server after connects: {"openSse":0,"acceptedSse":115}   <- origin ACCEPTED all 115,
                                                            edge killed every origin leg,
                                                            no client ever saw headers
Phase D  200 rps POST /echo for 60 s:  ok=5191 err=0     <- plain requests: flawless
  mid-load fresh SSE: status=timeout bytesIn15s=0
  mid-load plain GET /ping: 226ms
```

Same binary on localhost: sequential 15/15, burst 100/100, all 115
receive heartbeats for the full park, origin `openSse` == client
live count, zero zombies.

**2. It is response buffering, not connection handling.**
A streaming response that CLOSES immediately arrives fine; one that
stays open never arrives:

```
GET /sse-once   (stream, closes at once)   -> 200 + full body in 0.6 s
GET /sse        (stream, stays open)       -> 0 bytes after 12 s (no headers)
```

**3. Flush granularity ~8–16 KB, tail always held.**
`/sse-pad?kb=N` sends N KiB of SSE comment padding after hello, then
5 s heartbeats:

```
pad= 4 KB  -> no headers within 10 s
pad=16 KB  -> 200, headers only, 0 body bytes delivered
pad=32 KB  -> 200, 24,576 of ~33 KB delivered   (~8 KB held)
pad=64 KB  -> 200, 56,392 of ~66 KB delivered   (~9 KB held)
pad=128 KB -> 200, 106,496 of ~131 KB delivered (~25 KB held)
```

So an SSE stream is delivered only in ~8–16 KB increments; heartbeats
and small events sit in the buffer forever. Long-parked connections
are eventually reaped by the edge while the client socket stays open
and silent (client-side "zombies").

**4. `X-Accel-Buffering: no` fixes it (per response).**
Same deploy, side by side for 12 s:

```
GET /sse        (with  x-accel-buffering: no) -> 200, hello + heartbeats live at 5 s cadence
GET /sse-pad    (without the header)          -> 0 bytes, no headers
```

## Impact

Any Compute app serving SSE / long-poll / chunked streaming appears
completely broken to its clients unless every response carries the
undocumented opt-out header. Failure mode is nasty: origin logs show
healthy accepted streams; clients hang with no error; under
concurrency it presents as connect timeouts and phantom "zombie"
connections. (We spent two full load-test campaigns attributing this
to handshake rate limiting before isolating it with this repro.)

## Ask

1. Default `proxy_buffering off` (or equivalent) for
   `text/event-stream` — and ideally for explicit
   `Cache-Control: no-transform` responses.
2. Document `X-Accel-Buffering: no` support either way.
3. Clarify whether the ~35–60 s reap of buffered-idle origin legs is
   intended; with buffering off it is moot for heartbeating streams.

## Addendum (2026-08-21): the in-region (hairpin) path ignores the opt-out

`X-Accel-Buffering: no` fixes streaming for EXTERNAL clients only.
A client INSIDE Compute calling the same `cv-*.fra.prisma.build` URL
still gets buffered/starved streams:

```
same server, same minute, header present on every SSE response:
  out-of-region curl (h1):   keep-alives every 15 s  (741 B / 50 s)
  out-of-region curl (h2):   keep-alives every 15 s  (892 B / 50 s)
  in-region Bun client:      200 + 590 B initial burst, then SILENT 56 s+
```

Reproduce with this app's probe mode deployed in-region:

```bash
compute-cli deploy --path . --http-port 8080   --env PROBE_TARGET=<streams-url> --env PROBE_TOKEN=...   --env PROBE_KEY=... --env PROBE_N=16
curl https://<probe-url>/probe-report   # live15s=0, silent conns
```

Impact: any Compute-hosted consumer (service-to-service SSE — the
platform's own core use case) cannot hold a live subscription, even
with the documented-nowhere opt-out header. Ask #4: honor the opt-out
(or unbuffer `text/event-stream`) on the internal/hairpin tier too.
