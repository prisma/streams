# AWS single-stream/shard comparison: test plan and hypotheses

Status: 2026-07-22, plan approved-pending-credentials. Scope: **single
ordered unit** performance — one Prisma stream vs one Kinesis shard vs one
SQS FIFO message group. Multi-stream/shard comparison is the next phase and
is explicitly out of scope here.

## 1. Competitor selection

**Pick 1 — Amazon Kinesis Data Streams (provisioned mode, 1 shard).** The
canonical AWS durable ordered log: named streams of shards, per-shard total
order, sequence numbers, replay from a position, poll- or push-based
consumers. This is the same product shape as a Prisma stream with `total`
ordering, and its per-shard quotas are the industry reference point:
**1 MB/s or 1,000 records/s write per shard**, **2 MB/s read per shard**
with **5 GetRecords calls/s**; production-measured PutRecords latency
p50 ≈ 35 ms, p95 ≈ 100 ms, p99 ≈ 200 ms; consumer propagation typically
several hundred ms to ~1 s with polling (≈70 ms with enhanced fan-out,
which costs extra per consumer-shard-hour and per GB).

**Pick 2 — Amazon SQS FIFO (high-throughput mode, single message group).**
The AWS ordered-delivery queue: a message group is the ordered unit, which
maps to Prisma Streams' queue profile and per-key ordering (one group ≈ one
routing key ≈ one segment). Quotas: **300 messages/s per message group**;
per-queue high-throughput ceiling in eu-central-1 (Frankfurt) is 9,000
TPS/API action; `SendMessageBatch` carries ≤10 messages; message size
≤256 KB (batch total 256 KB). Latency floor is the best of the three
(tens of ms) since it fronts a purpose-built store with no object-store
durability floor.

**Why not MSK (Kafka)?** A Kafka partition would be the third natural
comparison, but MSK is VPC-only (no public HTTP surface), Kafka-protocol,
and cluster-provisioned (MSK Serverless still caps a partition at ~5 MiB/s
write). It is a different operational weight class — closer to "run your
own cell" than "call an API" — and its harness cost (VPC, client tuning)
would dominate the signal. Revisit in the multi-shard phase if desired.

## 2. What Prisma Streams brings to this fight (prior measurements)

- Single stream, local rig (s3lite at 25 ms): **5,000 req/s, 50,000
  records/s, 50 MB/s sustained** (single-stream ceiling campaign).
- Single instance on Compute FRA vs Tigris (16 shards, 32 streams): bursts
  of 1,500–1,600 req/s × 16 records ≈ **25k records/s**, ≈6 MB/s, ack
  p50 ≈ 100–160 ms under load; ack floor ≈ 90–110 ms (50 ms flush cadence
  + Tigris PUT + platform front door).
- Durability model: ack ⇒ WAL SST durable in object storage. One WAL PUT
  carries an entire commit group — throughput is metered by bytes-per-PUT
  and PUT rate, **not by record count**. This is the structural difference
  the hypotheses lean on.
- A fresh **single-stream** cloud reference run is part of this plan (the
  existing FRA numbers are multi-stream); all three systems get measured
  the same week with the same workload shapes.

## 3. Hypotheses (falsifiable, judged per-row in the results table)

| # | Claim | Mechanism | Falsified if |
|---|---|---|---|
| H1 | Prisma single stream sustains **≥10× Kinesis shard** in records/s at 200 B records (≥10,000 vs 1,000 rec/s) | Kinesis meters records/shard; we meter WAL-PUT bytes — batching collapses record count | Prisma cloud single stream < 10,000 rec/s sustained 10 min |
| H2 | Prisma sustains **≥4× Kinesis shard** in bytes/s at 16 KB records (≥4 MB/s vs 1 MB/s) | same; 50 MB/s proven locally, Tigris PUT path is the only limiter | Prisma < 4 MB/s sustained |
| H3 | **Kinesis and SQS win the ack-latency floor**: Kinesis p50 ≈ 35 ms, SQS p50 ≤ 30 ms, Prisma p50 ≈ 90–120 ms at conc=1 | our ack floor = flush cadence + object-store PUT; theirs is a purpose-built ingest tier | Prisma p50 < 60 ms (we'd be happily wrong) or Kinesis/SQS p50 > 120 ms |
| H4 | At each system's **own 80% ceiling**, ack p99: Kinesis ≤ 300 ms, SQS ≤ 300 ms, Prisma ≤ 1.5 s | their tails are tight until throttling; our tail carries compaction stalls | any system's p99 more than 3× its hypothesized bound |
| H5 | Prisma single ordered stream ≥ **30× an SQS FIFO message group** (≥10,000 vs ~300 msg/s; ~3,000/s if batched sends dodge the per-group meter — the test decides) | FIFO serializes per group at the partition; our order is per-WAL-group, amortized | measured group ceiling > 1/10th of Prisma's |
| H6 | **Tail freshness**: Prisma long-poll p50 ≤ 300 ms producer→consumer; Kinesis polling ≥ 500 ms typical (5 polls/s budget); SQS long-poll ≤ 150 ms | poll-budget arithmetic vs push-ish long-poll | Prisma > 500 ms or Kinesis polling < 300 ms at p50 |
| H7 | **Overload behavior parity**: at 2× ceiling offered for 10 min, all three shed cleanly (throttle/429) while goodput holds ≥ 80% of ceiling; nobody collapses or crash-loops | their control planes are mature; our shed discipline was just proven under the saturation gate | any system's goodput < 60% of its ceiling, or errors that are not clean throttles |
| H8 | **Cost at the ordered-unit ceiling** (list prices, filled in at test time): at a sustained 1,000 rec/s × 200 B ordered workload the three are within ~2× of each other; at 10,000 rec/s ordered, Kinesis needs 10 shards (losing single-unit total order) and SQS needs ~33 groups (same loss) while Prisma still runs one stream on one 1-vCPU instance — the cost AND semantics diverge together | per-shard/per-group metering vs per-instance | pricing math at measured ceilings contradicts the ordering-preserving cost claim |

The headline claim under test: **the single ordered unit in Prisma Streams
is 1–2 orders of magnitude wider than AWS's equivalents, at a latency floor
2–4× worse, for a comparable or lower cost.** AWS should win latency floors
and lose ceilings; if the data shows otherwise in either direction, that is
the finding.

## 4. Test matrix (identical shapes for all three systems)

Workload shapes, run against each system's single ordered unit:

| shape | parameters | duration | measures |
|---|---|---|---|
| A. latency floor | closed loop, conc=1, 1×200 B record/req | 5 min | ack p50/p90/p99 |
| B. record ceiling | conc sweep 2→128 (double every 3 min), batch 16×200 B | ~21 min | achieved rec/s ceiling, latency-vs-load curve, first-throttle point |
| C. byte ceiling | conc=8, 1×16 KB record/req (fits all payload caps) | 10 min | MB/s ceiling |
| D. tail freshness | producer at 50% of measured ceiling; separate consumer: Kinesis GetRecords tight-poll (≤5/s), SQS ReceiveMessage long-poll (20 s), Prisma `live=long-poll` | 10 min | producer-ts→receive p50/p99 |
| E. overload | offered 2× measured ceiling | 10 min | goodput retention, throttle semantics, recovery time after load drops |

Accounting rules: every response classified {ack, throttle, error}; a
throttle is CLEAN (Kinesis `ProvisionedThroughputExceededException`, SQS
`ThrottlingException`/`RequestThrottled`, Prisma 429+Retry-After) and never
counted as an error. Records carry a client timestamp for shape D. Each
run emits the same JSONL schema the FRA gate uses (`achievedPerSec`,
`winP50Ms`, `winP99Ms`, `ok/errs/throttled`) so `scripts/analyze-fra-ab.py`
tooling carries over.

## 5. Harness and topology

- **AWS arms**: one EC2 instance in **eu-central-1** (c7i.large, Amazon
  Linux 2023) running `awsbench` — a small Rust binary (aws-sdk-rust:
  `aws-sdk-kinesis`, `aws-sdk-sqs`) implementing the closed-loop shapes
  above with the pilot generator's stats surface. In-region so network is
  ≤1 ms, same as our FRA gen→server hop. SDK retries DISABLED (we count
  throttles ourselves).
- **Kinesis arm**: provisioned stream, `ShardCount=1`. Also record (not
  benchmark) the on-demand-mode behavior note: per-shard limits are
  identical; on-demand just adds shards, which changes the unit under test.
- **SQS arm**: FIFO queue with high-throughput mode enabled
  (`DeduplicationScope=messageGroup`, `FifoThroughputLimit=perMessageGroupId`),
  content-based dedup off (explicit `MessageDeduplicationId` = sequence
  number so dedup never absorbs our load), one `MessageGroupId`.
  Consumer deletes after receive (shape D) to keep the group unblocked.
- **Prisma arm**: the existing FRA rig (`scripts/bench-fra-ab.sh` env),
  slate5 build, `STREAMS=1`, same shapes via gen env
  (`CONC_START/CONC_MAX/BATCH`, 16 KB variant via `BENCH_RECORD_BYTES` —
  small gen addition). Fresh management-API bucket per run.
- **Timing**: all three arms within the same 2-day window; Prisma arm run
  twice (morning/evening) because Tigris-under-load variance is a known
  factor — report both.

### Fairness caveats (stated up front, revisited in results)

1. Different clouds: each system is measured in its home environment with
   an in-region generator; this compares *products as experienced*, not
   isolated software.
2. Durability semantics differ: Kinesis acks after 3-AZ replication; Prisma
   acks after object-store PUT (Tigris regional durability); SQS FIFO is a
   queue — messages are consumed, not replayed. SQS is compared to the
   queue profile / per-key-ordering story, not to the log story.
3. Payload caps differ (Kinesis 1 MB/record, SQS 256 KB/batch, Prisma 32 MB
   body); shape C's 16 KB fits all.
4. Kinesis enhanced fan-out (push, ~70 ms) exists but bills per
   consumer-shard-hour; we measure the default polling consumer and note
   EFO separately.
5. List-price cost math uses published eu-central-1 prices captured on test
   day (Kinesis shard-hour + PUT payload units, SQS per-million requests,
   Prisma Compute instance + Tigris request/storage pricing).

## 6. Execution runbook (when credentials arrive)

1. `aws sts get-caller-identity` — confirm account + region access.
2. Provision (all eu-central-1): `aws kinesis create-stream --stream-name
   slate-cmp --shard-count 1`; `aws sqs create-queue --queue-name
   slate-cmp.fifo --attributes FifoQueue=true,DeduplicationScope=messageGroup,FifoThroughputLimit=perMessageGroupId`;
   EC2 c7i.large + instance profile with kinesis/sqs minimal policy
   (PutRecord(s)/GetRecords/GetShardIterator/DescribeStream,
   SendMessage(Batch)/ReceiveMessage/DeleteMessage(Batch)/GetQueueAttributes).
3. Build `awsbench` for x86_64-linux (zigbuild, same pipeline as the pilot),
   scp to the instance, run the matrix (one script, JSONL out).
4. Prisma arm: `STREAMS=1` gate-shaped runs on the FRA rig (fresh bucket via
   management API).
5. Analyze with the shared tooling; append results + verdicts per
   hypothesis to this file; tear down (delete stream, queue, terminate
   EC2) — everything here is disposable.

Estimated AWS runtime cost for the whole matrix: ≈1 shard-day of Kinesis
(~$0.4 + PUT units), a few million SQS requests (~$2), ~6 hours of
c7i.large (~$0.9) — trivially small; no quota increases needed.

## 7. Sources

- Kinesis per-shard quotas (1 MB/s | 1,000 rec/s write; 2 MB/s read;
  5 GetRecords/s; 10 MB/10k records per call):
  [AWS docs — Quotas and limits](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html),
  [Kinesis FAQs](https://aws.amazon.com/kinesis/data-streams/faqs/)
- Kinesis production latency (PutRecords p50 ≈35 ms, p95 ≈100 ms,
  p99 ≈200 ms): [brandur.org — A Month of Kinesis in Production](https://brandur.org/kinesis-in-production)
- Kinesis propagation delay and low-latency guidance:
  [AWS docs — Improve low-latency processing](https://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html),
  [AWS blog — reduced propagation delay](https://aws.amazon.com/blogs/aws/amazon-kinesis-update-reduced-prop-delay)
- SQS FIFO per-group 300 msg/s and high-throughput mode:
  [AWS docs — Enabling high throughput for FIFO queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/enable-high-throughput-fifo.html),
  [AWS docs — FIFO throttling troubleshooting](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/troubleshooting-fifo-throttling-issues.html),
  [AWS what's new — 9,000 TPS HT mode](https://aws.amazon.com/about-aws/whats-new/2023/08/amazon-sqs-increased-throughput-quota-fifo-high-throughput-mode/)
