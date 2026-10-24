# Native queues — pricing goal and cost benchmark

Status: **benchmark definition, 2026-10-23; first results 2026-10-24 (§9): goal not met yet.** It sets the price goal for
native queues ([QUEUES-DESIGN.md](QUEUES-DESIGN.md)), builds a scenario from
Prisma's own queue traffic, prices that scenario on Cloudflare Queues, and
defines how we measure our cost of running the same scenario. Q0 runs a
storage-only preliminary; the full benchmark runs once Q1-Q5 are built, and
its verdict decides whether the goal is met. The calculator is
`bench/queues-cost/cf_bill.py` over `bench/queues-cost/scenario.json`.

## 1. The goal

- **Price: match Cloudflare Queues.** $0.40 per million logical operations;
  an operation per 64 KB written, read or deleted; one million operations a
  month included. An ordinary message costs three operations (write, read,
  delete), so about $1.20 per million messages processed once.
- **Margin: at least 80% on infrastructure.** Our cost of operating a
  workload must be at most 20% of what Cloudflare charges for it. Engineering
  and support are outside this margin.
- **Where it must hold:** on a shared cell at the reference utilization
  (§5). A dedicated cell is reported, never gated: no small workload pays for
  a cell on its own at this price (§7).

Cloudflare's terms, from its pricing page (updated 2026-04-21): each retry is
a read; a message moved to a dead-letter queue is a write there; a message
that expires unread costs a write and a delete; messages carry about 100 B of
internal metadata toward the 64 KB unit; there is no egress charge. Consumer
Worker invocations and CPU are billed separately on both sides and are
compared separately, not here.

## 2. How we bill, so the comparison is like for like

Recommended for the design's billing decision (D14):

- **The unit is a logical operation on a message, per 64 KB**, never an HTTP
  request: a send is one write per message, each delivery attempt one read,
  an ack or discard one delete, a move to failed nothing extra, an expiry a
  delete. A batch of 1,000 messages is 1,000 operations; batching lowers our
  cost, not the price.
- **Empty claims are free and cause no durable write** (an empty long poll
  every 25 s is about 104,000 calls a month per idle consumer).
- **Delivery bandwidth is included**, as Cloudflare includes it.
- **Storage within retention and the standard failure retention is
  included**; longer failure retention, if offered, is priced separately.
- **The included allowance matches Cloudflare's**: one million operations a
  month.

## 3. The scenario: the control plane's queues for one month

Source: the queue usage review of 2026-10-07, Cloudflare's
`queueMessageOperationsAdaptiveGroups` for the seven UTC days 2026-09-30 to
2026-10-06, 18 production primary queues: 5,307,783 writes and 5,388,062
reads, about 8.8 messages a second on average, 62,904 in the busiest hour.
Scaled to a 30-day month:

| Queue | Messages a month | Average bytes | Shape |
|---|---:|---:|---|
| cp-stripe-usage-records-sync | 12,622,929 | 161 | 14 sweeps a day, 80-minute `deliverAt` spread, producer batches of 100 |
| cp-vercel-usage-sync | 2,657,691 | 154 | 12 runs a day, 2-minute delay |
| mkp-vercel-send-billing-data | 2,655,557 | 423 | one per upstream measurement, single sends |
| cp-offer-limit-enforce | 1,897,461 | 115 | hourly sweeps plus domain events, 10 s delay |
| cp-spend-limit-usage-records-sync | 1,761,664 | 161 | hourly sweep |
| cp-notification-controller | 589,826 | 68 | campaign bursts at 10:00, 10:30, 12:00 (17,618 in the peak hour) |
| cp-spend-limit-enforce | 290,743 | 43 | hourly sweep |
| mkp-vercel-submit-invoice | 149,143 | 181 | month-start burst (82% of a week's writes on one day), 2.83 deliveries per message, 37% dead-lettered |
| ten smaller queues | 122,627 | 43-231 | domain events, single sends |
| **Total** | **22,747,641** | 183 (weighted) | 3.018 operations per message |

Replay parameters (in `scenario.json`): per-queue rates, peak hours, payload
sizes, attempts, retry delays, consumer concurrency, dead-letter rates and
delays as measured or configured; consumers claim and settle in batches of 10
as today. Two values are assumptions, labelled in the file: producer batch
sizes (100 for the sweep producers, which chunk at 100 today; single sends for
domain events) and the split of reads among queues other than invoices (in
proportion to writes, 1.0032 deliveries per message, from the week's totals).
The replay generates the daily cron shapes, one month-start invoice burst,
handler latencies of 50-500 ms, and the measured retry and dead-letter rates.

## 4. The Cloudflare bill

`python3 bench/queues-cost/cf_bill.py`:

| Volume | Messages a month | Operations | Bill | Per million messages |
|---|---:|---:|---:|---:|
| 1× (today) | 22.7 M | 68.6 M | **$27.06** | $1.19 |
| 10× | 227 M | 686 M | $274.17 | $1.21 |
| 100× | 2,275 M | 6,864 M | $2,745.26 | $1.21 |

The Stripe usage queue alone is $15.16 of the $27.06.

## 5. What our cost must be

At an 80% margin our cost may be at most **20% of the bill**:

| Volume | Cost ceiling a month | Per million messages |
|---:|---:|---:|
| 1× | **$5.41** | **$0.238** |
| 10× | $54.83 | $0.241 |
| 100× | $549.05 | $0.241 |

For reference, the colleague's pricing note proposes aiming lower later: at
$0.30 per million operations the 1× ceiling is $4.06, at $0.20 it is $2.71.
The goal here is parity.

## 6. How we measure our cost

**Prices.** The owner's prices of 2026-10-07: Tigris Class A $3.75 per
million, Class B $0.375 per million, storage $0.015 per GiB-month, transfer
into Tigris $0.01 per GB; Compute memory $0.006 per GB-hour, CPU $0.064 per
active vCPU-hour, 1 GiB instances; delivery egress $0.005 per GB. Every result
is also priced at Tigris list prices (Class A $5, Class B $0.50 per million,
storage $0.02 per GiB-month) as a sensitivity.

**Three costs, one gated.**

1. **Marginal cost**: the reference shared cell runs its baseline tenant mix
   with and without the queue scenario; the difference in Class A and B
   requests, bytes into Tigris, stored GiB-months, CPU-seconds, memory and
   delivery egress, priced. This includes everything the queue causes in
   others' work: compaction that rewrites unrelated data is the queue's cost.
2. **Fully loaded cost (gated)**: the marginal cost plus the queue's share of
   the cell's fixed floor ($95.3 a month for the shared cell at the owner's
   prices, F1), the share being the larger of the queue's CPU share and
   memory share of the cell at its planned utilization.
3. **Standalone cost (reported)**: a dedicated cell running only the scenario.

**Reference cell.** The `shared-cell` profile on `compute-1g` instances, with
the baseline tenant mix of the shared-cell certification (Layer B's compliant
tenants) driving the shard's commit pump at 60% of its flush capacity. The
baseline is fixed before the run and identical across arms.

**Runs.**

| Run | Length | Purpose |
|---|---|---|
| A/B at 1× | 48 h each arm; the second 24 h measured, the first includes the month-start invoice burst | the gated number, scaled to 30 days with the burst counted once |
| A/B at 10× and 100× | 2 h each arm at the busiest-hour shape | the cost curve; per-million cost must not rise with volume |
| Idle | 6 h with consumers connected and no messages | fixed idle cost: empty claims, sweeps, roster; must be zero durable writes and no object requests beyond the baseline |
| Standalone at 1× | 24 h | reported |

**Instruments.** Requests and bytes come from the object-store ledger
(locally s3lite's `/_s3lite/stats2`, the method of
[COST-METHODOLOGY.md](COST-METHODOLOGY.md); in the field the operator's
store-request counters). CPU and memory come from the Compute wrapper's
samples. Physical storage is sampled hourly from the bucket. Every run also
checks correctness: every message processed, none lost, retries and
dead-letter counts matching the scenario, and physical collection meeting the
design's deadline; a run that fails a check is void.

**Output.** `costs.json` with the three monthly costs and every term behind
them; `python3 bench/queues-cost/cf_bill.py --measured costs.json` prints the
margin and **goal MET or NOT MET** (fully loaded at 1× ≤ $5.41, and per-million
cost at 10× and 100× ≤ the ceiling), or VOID for a run that failed a check.
The local rig that produces it (replay, instruments, pricing, A/B
orchestration) is `bench/queues-cost/run-ab.sh`; its README says how to run
each row of the table above.

## 7. Estimate before building

Not a measurement: it tells us what the benchmark is likely to say and which
term decides it. Prices as in §6, 30-day month, 1×.

| Term | Basis | Shared cell (marginal) | Standalone |
|---|---|---:|---:|
| Requests | sends 3.1 M (sweeps batched by 100), claims 2.3 M and settles 2.3 M (batches of 10): 7.7 M a month, 3 a second | — | — |
| WAL uploads (Class A) | at most one per commit; on a busy shard queue commits ride uploads that happen anyway | $0-2.90 (0-10% of commits add an upload) | $28.90 (every commit an upload) |
| Bytes into Tigris | about 1.4 KB of WAL per message (page, metadata, index, delivery and clock rows, tombstones), written again at L0 flush and compaction: about 100 GB | $1.00 | $1.00 |
| Compaction of unrelated data | forced collection passes rewrite stable shard data; bounded by the amount trigger | **unknown: Q0 measures** | small |
| Object reads (Class B) | claims read cached blocks; cold reads only for scheduled backlog | ≤ $0.40 | ≤ $0.40 |
| CPU | 268.6 µs per request and 0.073 vCPU-s per payload MB (the fitted server model of the throughput analysis), plus maintenance | $0.10-0.50 | $0.10-0.50 |
| Storage | live backlog under 0.2 GiB (Stripe's 80-minute spread, failed invoices, garbage within the 60-minute deadline) | < $0.01 | < $0.01 |
| Delivery egress | 23 M deliveries × 0.5 KB | $0.06 | $0.06 |
| Share of the cell floor | CPU and memory share at planned utilization (well under 1%) | $0.10-0.50 | $95.30 |
| **Total** | | **$1.3-5.4 + compaction** | **about $126** |

**Reading.** Against the $5.41 ceiling:

- **On a busy shared cell the goal is reachable but not comfortable.** The
  known terms come to $1.3-5.4, and the unknown is the compaction that queue
  collection forces on unrelated data in the same shard. Q0 measures exactly
  that, and it can by itself decide the verdict.
- **Low utilization breaks it.** If queue commits are not coalesced with
  other traffic, write-ahead-log uploads alone cost $28.90, five times the
  ceiling. The levers are placement (put queues on busy shards) and a queue
  commit window (hold queue writes up to a few hundred milliseconds so more of
  them share an upload, at a latency cost).
- **A dedicated cell cannot meet the goal at this volume**: about $126 a month
  against $27 of revenue. Queues are a shared-cell product at Cloudflare
  prices.
- **Volume helps.** The upload and floor terms are fixed per shard, so the
  per-million cost falls as traffic grows; the 10× and 100× runs must confirm
  it.

## 8. When it runs, and who decides

- **Q0 (preliminary).** The storage prototype replays the scenario's send →
  claim → ack → reclaim stream through the shard database beside stable data,
  at 1× and 10×, and reports Class A and B, bytes, compaction rewrite and
  CPU per million messages. If its marginal cost already exceeds $0.238 per
  million at 10×, Q1 does not start and the storage design is revisited.
- **After Q5 (decisive).** The full benchmark above, on the real queue
  service, before any pricing decision or production migration.
- Performance and cost acceptance stays with the owner (AGENTS.md, standing
  holds): the benchmark reports MET or NOT MET; the owner accepts it.

## 9. Results of the first build (2026-10-23/24)

Local costab rig (s3lite at 25 ms per operation, the soak7 server posture,
an awsbench baseline co-tenant), owner prices, 30-day month. Arm A is the
baseline alone; arm B adds the queue replay. Runs were shortened from §6's
plan to fit the day the owner gave the build: no 48-hour 1x run, no
month-start invoice burst, no idle or standalone run.

| Run | Server | Shape | Fully loaded | Marginal | Ceiling | Verdict |
|---|---|---|---:|---:|---:|---|
| q1x | 393f72f9 | 1x day profile, 60 min warm-up, 90 min measured | $72.34 | $59.43 | $5.41 | not met (void by the rig's drain rule; nothing lost) |
| q1x-b | 3b3eb559 | 1x, 30 + 60 min | **$36.54** | **$16.91** | $5.41 | **not met** (valid; 53,656 sent, 0 lost) |
| q10x | 3b3eb559 | 10x busiest-hour profile, 30 + 60 min | **$185.75** | **$119.93** | $54.83 | **not met** (void by in-flight leases at the drain deadline; 0 lost) |

Per million messages: Cloudflare $1.19-1.21; the ceiling $0.24; ours
$1.61 fully loaded and $0.74 marginal at 1x, $0.82 fully loaded and $0.53
marginal at 10x. At 10x we already undercut Cloudflare (a 32% margin) but
miss the 80% goal by about 2.2x on marginal cost.

What the runs showed:

- **Reads dominated the first run.** Queue scans used SlateDB's default
  `cache_blocks: false`, so every claim and sweep fetched every sorted
  run's block from the bucket: 1M extra SST GETs in 90 minutes, $180 a
  month. Caching scan blocks (3b3eb559) cut Class B from $179.89 to $1.72
  at 1x.
- **At 10x the reads are message pages.** Scheduled messages (the Stripe
  queue's 80-minute spread) age out of the memtable and L0 into sorted
  runs, whose data blocks the 64 MiB shared cache does not keep under load:
  about 0.5 SST GET per message, $0.20 per million.
- **Writes are WAL uploads.** Queue commits that the baseline's uploads do
  not cover add about 1.1 PUT/s at 1x and 3.9 PUT/s at 10x: $0.165 per
  million at 10x.
- **Bytes into Tigris** (WAL, L0 flushes, compaction rewrite) are about
  6.4 KB per message at 10x, $0.064 per million; **CPU** $0.084 per
  million.
- **CPU and memory are noisy between arms** (at 1x the baseline-only arm
  used more CPU than the queue arm), so the floor share, which follows them,
  is the least certain term.

Levers, in expected order of effect: keep queue pages hot (a queue-page
cache or a claim that reads a run of adjacent pages in one range read); a
queue commit window so queue writes share WAL uploads; fewer and smaller
rows per message (the state row is JSON rewritten per operation); and
fewer compaction rewrites. The next measurement should repeat q10x with
each lever, and run the full 48-hour 1x pair before any pricing decision.
