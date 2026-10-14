# edgeprobe: facts about the Compute edge in front of a single-server cell

The edge arm of the shared-cell certification (public-beta deploy-q5 design
§3.5, triage item B6). One server on the existing Compute generation means
every tenant shares one origin service behind the edge, so the edge's limits
bind the beta: the per-service connection wall (exactly 1,536 in August,
whatever the client IP; SSE and writer sockets count together,
`bench/WORKLOAD-CERT-PLAN.md`), SSE buffering on the in-region hairpin
(PRO-218, `bench/edge-repro/README.md`), the 60 s first-byte deadline and the
idle keep-alive ordering against the binary's 120 s `h1_header_timeout`.
These are **facts, not pass or fail**, for the owner and the Compute team;
then one posture check.

```bash
bun bench/edgeprobe/edgeprobe.ts help
bun test ./bench/edgeprobe            # every mode against a local origin that plays the edge
```

Connections are raw HTTP/1.1 over `node:net`/`node:tls`, one socket each, so
a count is a count of sockets the edge accepted (fetch would pool and
queue). Each mode writes JSON lines (`--out FILE`), each record carrying its
`--vantage` and wall-clock time; `report` merges any number of files. Run
from K >= 3 source IPs (the laptop plus app-gen in two other regions, as
`bench/soak/wc-ladder.sh` does) and one in-region app-gen, the Compute
customer's vantage. Raise the descriptor limit for the wall (`ulimit -n 8192`).
Header files are k2gen's (`Name: value` lines, a project token); nothing from
them is recorded.

Who answered: the binary sets `x-streams-boot-id` on `/health` only, and
its product CORS header (`access-control-expose-headers` naming
`prisma-next-cursor`, `src/product.rs` `with_product_cors`) on every
`/v1/streams` and usage answer; an answer carrying neither is the edge's.

| Design §3.5 | Mode | What the record says |
|---|---|---|
| 1. Idle keep-alive reuse | `idle --gaps 30,60,90,110,125,180,300` | per gap: reused, closed by the peer while idle (and when), a reset, or a status; a non-2xx the origin did not send means the edge reused an upstream connection the origin had closed: a FINDING, since its upstream idle timeout must sit below the binary's 120 s (a config change and an edge record) |
| 2. The wall | `park` (SSE and long-polls, ramped) on some vantages, `fresh` on another, `write` on the original ones | the parked count when a fresh client elsewhere first fails, how it failed (connect timeout, refusal, reset, a platform page), and the writers' failures before and after |
| 3. Unauthenticated exhaustion | `tls-idle` | handshaken connections that never send a request: whether they fill the wall. It refuses to run without `--notice FILE` (JSON: `notified` date not after today, `service` equal to the target host, `vantages` listing this `--vantage`, Compute's non-empty `reply` and its `go_ahead: true`, since triage §4 (13) asks Compute whether to run it at all), without `--measured-wall N`, or above the wall + 10% rounded down (a wall of 1,536 allows 1,689; triage D11); the notice goes into the records |
| 4. SSE buffering | `sse-first` | from an append to its event on a raw SSE subscription, per vantage (in region vs out) |
| 5. The 60 s deadline | `deadline --waits 25,55,65` | per wait: who answered (the origin, or the edge with what) and after how long; the longest the origin actually held a long-poll's first byte. The binary caps every long-poll's wait at 25 s in all, across a split's refresh too (`src/application/read_request.rs` `wait_tail`, `src/http.rs` `MAX_LONG_POLL`, edge change #NEW-r4-rust-2; consumer pulls likewise) and answers a longer wait with `204` at its cap, so no long-poll waits more than 25 s in total and none reaches the edge's 60 s first-byte deadline: the report says so when the origin answered before the wait it was asked. The owner dropped the design's probe of the deadline (§3.5 item 5, waits of 55 and 65 s) on 2026-10-13 (round-3 call 11); the mode stays for its FACT, since the edge's own deadline is still unmeasured |
| 6. SC0b's interplay | `park --via long-poll --count 1000` plus `write` from other IPs, then `report --wall N` | the peak parked plus writer connections, and the margin to the wall |

`posture --wall N --long-polls N --writer-headroom N` prints `PASS` when
`SSE_MAX_CONNECTIONS` (read from `deploy/profiles/compute-1g.env` then
`shared-cell.env`, later wins; `--sse-max` overrides) plus the long-polls
plus the writer headroom stays strictly under the measured wall, else `FAIL`.

Records and reports stay on the machine that ran them; nothing is uploaded.
