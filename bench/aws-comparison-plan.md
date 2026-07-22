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

---

# RESULTS (2026-07-22, eu-central-1, all arms via awsbench on Prisma Compute FRA)

Setup as planned: Kinesis `slate-cmp` (1 provisioned shard), `slate-cmp.fifo`
(high-throughput mode, one message group), Prisma Streams set up from
scratch — fresh project `streams-awscmp`, fresh management-API bucket,
slate @ 5bd3d1d single instance (gate envelope: cache 128 MiB, unflushed
16 MiB, L0 32, shed 550, per-stream inflight cap 64). One harness binary
(awsbench) drove all three systems from the same Compute instance class;
cross-provider hop Frankfurt→Frankfurt (AWS floors of 7 ms confirm the hop
is small). SDK retries disabled; {ack, throttle, error} accounting.

## Measured matrix

| shape | Kinesis (1 shard) | SQS FIFO (1 group) | Prisma (1 stream) |
|---|---|---|---|
| A floor p50 / p99 | **7.3 / 11.7 ms** | **7.0 / 21.6 ms** | 125.7 / 593.4 ms |
| B record ceiling | 1,197 rec/s (wall ~1k, 480k throttles/2 min) | 3,582 msg/s (batch-10) | 1,944 rec/s @ conc 32 |
| B p50 at own peak | 12.7 ms | 138.5 ms (conc 128) / 25 ms (conc 32) | 185.5 ms |
| C byte ceiling (1×16 KB/req) | 0.92 MB/s (the 1 MB/s wall) | 5.40 MB/s (330 msg/s × 16 KB) | 0.48 MB/s |
| D tail p50 / p99 | 172 / 314 ms (poll ≤5/s) | (rerun below) | 580 / 1,197 ms |
| E overload goodput vs own ceiling | 1,028/1,028 = 100 %, clean throttles | 3,004/3,582 = 84 % | 1,524/1,944 = 78 %, clean 429s |

## Hypothesis verdicts

| # | hypothesis | verdict |
|---|---|---|
| H1 | Prisma ≥10× Kinesis shard rec/s | **FALSIFIED** — 1.6× (1,944 vs 1,197). One stream = one WAL chain; rounds are bounded by object-store RTT, and records/round cannot compensate at conc ≤ the per-stream cap |
| H2 | Prisma ≥4× Kinesis MB/s | **FALSIFIED** — 0.5× (0.48 vs 0.92 MB/s) at the tested 1×16 KB/req shape (batch=1 denies our batching lever, but that was the defined shape) |
| H3 | AWS wins the latency floor 2–4× | **CONFIRMED, understated** — they win ~17× (7 ms vs 126 ms). Replicated-memory ingest tiers vs flush-cadence + object-store PUT |
| H4 | p99 bounds at each system's 80% ceiling | **CONFIRMED** — Kinesis 20 ms, SQS ~55 ms, Prisma ≤ 916 ms; all within hypothesized bounds |
| H5 | Prisma ≥30× an SQS FIFO group | **FALSIFIED** — 0.54× vs the batched group (3,582 msg/s: the 300/s quota meters *transactions*, so batch-10 lifts a group to ~3.5k msg/s); 5.9× vs the unbatched meter (330/s) |
| H6 | Tail: Prisma ≤300 ms, Kinesis ≥500 ms | **FALSIFIED both directions** — Kinesis polling delivered 172 ms p50; Prisma measured 580 ms p50 under concurrent produce load |
| H7 | Overload parity: clean shedding, ≥80% goodput | **CONFIRMED (with notes)** — Kinesis 100 %, SQS 84 %, Prisma 78 % (borderline vs the 80 % bar) with zero hard errors and clean 429/Retry-After. The SQS "errors" in raw data are group throttles: the SDK's opaque Display hid `ThrottlingException` (classifier fixed in awsbench) |
| H8 | Cost within ~2× at 1k rec/s ordered; divergence at 10k | **PARTIALLY CONFIRMED** — list-price estimates at 1,000 rec/s × 200 B sustained: Kinesis ≈ $11/mo shard-hours + ≈$37/mo PUT payload units (2.6 B units × $0.014/M) ≈ **$48/mo**; SQS FIFO ≈ 260 M batch-10 sends + matching receives/deletes ≈ 700 M req/mo × $0.50/M ≈ **$350/mo** (~$130 send-only); Prisma ≈ 1-vCPU instance (~$15) + ~160 M Tigris class-A ops (~$0.36/M ≈ $58) ≈ **$73/mo**. Within ~1.5× of Kinesis, 2–5× under SQS. At 10,000 rec/s ordered: no system serves it in one ordered unit — Kinesis/SQS by quota, Prisma by measured ceiling (~2k) — the divergence claim moves to the multi-stream phase |

## Follow-up runs (same session)

- **Prisma uncapped probe** (per-stream inflight cap 0, conc 128, batch 16):
  **6,396 rec/s median, 8,057 peak, zero errors, zero throttles**, p50
  242 ms / p99 1.27 s over 5 minutes. The campaign's 1,944 rec/s was the
  *default admission cap* (`ADMIT_MAX_INFLIGHT_PER_STREAM=64`), not the
  architecture: records-per-round scales with admitted concurrency, so
  uncapped the same stream carries 128×16 = 2,048 records per ~320 ms WAL
  round. Revised ordered-unit ceilings: **Prisma 6.4k > SQS group 3.6k >
  Kinesis shard 1.2k** — Prisma has the widest ordered unit of the three,
  at 20–30× worse p50 than the AWS ingest tiers. H1's ≥10k bar remains
  unmet (falsified as stated), but the "AWS wins ceilings" reading below is
  config-dependent, not architectural.
- **SQS shape-D rerun** (purged queue): tail p50 measured 71.6 s — a
  *shape-design artifact*, not an SQS property: the closed-loop producer
  (2,305 msg/s) outran the single long-poll consumer's drain rate
  (~400–700 msg/s with receive+delete rounds), so the metric measured queue
  backlog. SQS delivery latency at sub-drain produce rates is its ~7–25 ms
  floor. Recorded as NOT COMPARABLE; a rate-limited-producer D-shape is the
  fix for future runs (applies to any queue-semantics system).

## The honest headline

**The plan's headline claim ("1–2 orders of magnitude wider") is REFUTED —
the honest multiplier is 5.3× vs Kinesis and 1.8× vs SQS, and only with the
per-stream admission cap lifted.** AWS's ingest tiers win the latency game
by 17–30× at every load level; Prisma wins the ordered-unit width game once
configured for it (6.4k rec/s vs 1.2k/3.6k), while the default-config
single stream (cap 64) lands at 1,944 rec/s — between the two AWS products.

What survives, measured: (1) overload discipline is at parity with managed
AWS services — the shed work from the saturation gate holds up in direct
comparison; (2) the Prisma arm ran on a from-scratch rig provisioned in
minutes via the management API; (3) the economics at ~1k rec/s
are within ~1.5× of Kinesis and 2–5× under SQS FIFO (list prices).

Structural takeaway for the roadmap: single-ordered-unit throughput is
bounded by durable-ack RTT — object storage (25–500 ms) cannot match
replicated-memory acks (2–5 ms) no matter the batching, because rounds/s is
the hard bound. Closing the single-unit gap requires WAL pipelining
(multiple in-flight WAL PUTs with ordered ack release) and/or a low-latency
durability tier in front of the object store. The multi-stream phase tests
the claim our architecture is actually built around: aggregate density —
many ordered units per instance — where the same instance already sustained
~25k rec/s across 32 streams during the saturation gate.

## Batch-dimension sweep (2026-07-22, follow-up to the campaign)

Question: how do the three systems react to request batching? Matrix:
batch {1, 10, 100} × record {200 B, 16 KB}, conc=32, 3 min/leg, Prisma
uncapped per-stream, all same-morning (substrate slower than the previous
evening — cross-day absolutes carry drift; within-morning comparisons are
clean). SQS caps batches at 10 entries / 256 KB by API design.

| system, 200 B | batch 1 | batch 10 | batch 100 |
|---|---|---|---|
| Kinesis rec/s (p50) | 1,043 (9.7 ms) | 1,246 (12.9 ms) | 1,096 (22.8 ms) |
| SQS msg/s (p50) | 300 (25.3 ms) | 3,000 (26.6 ms) | — API cap 10 |
| Prisma rec/s (p50) | 70 (461 ms) | 961 (282 ms) | 4,340 (673 ms) |

| system, 16 KB | batch 10 | batch 100 |
|---|---|---|
| Kinesis MB/s | 1.18 (its 1 MB/s wall) | 1.03 |
| SQS MB/s | **49.2** (300 × 160 KB batches) | — API cap |
| Prisma MB/s | 1.38 | 0.33, degraded (see note) |

Findings:

1. **Kinesis is batch-inert.** The shard meters records AND bytes; batching
   only reduces request count (1,043 → 1,246 → 1,096 rec/s; 1.0–1.2 MB/s
   at 16 KB). Per-request latency grows with batch size.
2. **SQS multiplies both dimensions by exactly the batch factor**, because
   the group meters 300 *transactions*/s: 300 → 3,000 msg/s, and 0.6 →
   **49.2 MB/s** — the widest ordered-unit byte pipe measured in this whole
   campaign, by 35×. (The raw files show these legs' rejections as "errors"
   — they are group throttles; this sweep still ran the pre-fix classifier.)
3. **Prisma multiplies records nearly linearly with batch** at small record
   sizes (70 → 961 → 4,340 rec/s at ~constant request rate: rounds are
   RTT-bound, records-per-round is nearly free), matching the architecture
   (one WAL PUT carries the whole commit group).
4. **Prisma's byte path is the weak flank and byte floods are unguarded**:
   16 KB×10 reached only 1.38 MB/s, and 16 KB×100 (1.6 MB bodies, 32 conc ≈
   51 MB admitted instantly) degraded to 0.33 MB/s with server restarts
   (front-door 404/502 in-run): the RSS shed samples every 500 ms and
   admission counts requests, not bytes, so a byte flood lands between
   checks. This is direct evidence for the deferred per-byte admission item
   (AWS-readyness §4; codex's `ADMIT_WRITE_BURST_BYTES` family).

Answers to the standing questions: the campaign's 1,944 rec/s was batch-16
HTTP requests (~121 req/s), not individual appends; individual-append
(batch-1) throughput at conc 32 is ~70 req/s on this substrate — each
request pays a full durable round. With batch 10 → ~1k rec/s; batch 100 →
~4.3k rec/s (and 6.4k was measured the prior evening at batch 16 × conc
128: records-per-ROUND ≈ conc × batch is the real lever, however split).
Byte ceiling improves ~3× with batch 10 (0.48 → 1.38 MB/s) then inverts at
batch 100 for the reasons above.
