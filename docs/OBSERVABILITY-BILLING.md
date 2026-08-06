# Prisma Streams billing, usage, telemetry, and observability

**Status:** APPROVED (2026-08-06) — implementation in progress on `slate`; this document is the authority for the telemetry cutover  
**Target:** pre-launch hard cutover; no compatibility work for `_billing`, `__metrics__`, or their schemas  
**Scope:** per-stream monthly billing, customer usage dashboards, fleet and storage observability, durable operational event history, alerting, and telemetry-pipeline self-monitoring

---

## 0. Executive decision

Prisma Streams will have **four distinct telemetry planes**, each with a single job:

1. **Billing state in the data plane** — exact, durable, per-segment cumulative ingest and retained-storage state, updated in the same shard `WriteBatch` as the records it describes.
2. **The immutable usage ledger** — `_usage`, an internal total-order Durable Stream containing idempotent usage snapshots, read-delivery deltas, and lifecycle observations.
3. **The usage rollup store** — one partitioned SlateDB materialization that maintains current and monthly usage by project and stream; dashboard lookups are point reads, not ledger scans.
4. **Operational telemetry** — `_ops_events` for durable typed events and `_ops_metrics` for low-cardinality numeric series, with a separate operational rollup database and alert evaluator.

The current `_billing` emitter and `__metrics__` flusher are removed. Rate limiting, live debug counters, customer billing, and operational metrics are not treated as the same data structure.

### Guarantees

- **Committed ingest and retained-storage usage are exact and restart-safe.**
- **Read usage is never deliberately dropped or aggregated without stream attribution.** It is durably emitted in idempotent batches. An abrupt process loss may undercount at most one configured active flush interval; the default target is 10 seconds. It never re-bills after restart.
- **A stream name is display metadata, not the billing identity.** Billing is keyed by project and immutable stream incarnation.
- **Internal fan-out, retries, compaction, WAL copies, and telemetry traffic are never customer-billed.**
- **No per-stream object is rewritten every telemetry tick.** Rollup updates are batched through SlateDB.
- **Operational transitions are journaled by deterministic event ID.** CAS-backed transitions use a durable outbox or a reconciled state version, rather than ephemeral process logs alone.

---

## 1. Requirements

### 1.1 Billing

For each customer stream and UTC calendar month, derive:

- committed ingest payload bytes;
- committed ingest record count;
- externally delivered read payload bytes;
- externally delivered record count and operation counts;
- retained canonical storage byte-seconds, convertible to GB-months;
- optional informational dimensions such as requests, queue operations, wire bytes, compression ratio, and logical fork view size.

Invoices and customer dashboards must be derived from durable stored telemetry, not from process-lifetime counters.

### 1.2 Customer lookup

A customer query for one stream and one month must require:

- one authenticated API call;
- one rollup point read in the common case;
- no scan of `_usage`;
- no scan of every interval in the month;
- no external hash-to-name join maintained by the customer.

### 1.3 Operations

Operators must be able to answer both:

- **What is happening now?**
- **What happened yesterday, and why?**

This includes:

- stream split and merge history;
- shard ownership changes and rebalancing;
- desired fleet-count changes and their reasons;
- instance live/dark/replacement events;
- fencing, engine opens/closes, stale routing, and replay rates;
- append/read latency, store latency, queueing, saturation, memory, and background-work lag;
- telemetry, billing-ledger, and rollup health;
- SLO breaches and alert resolution history.

---

## 2. Non-goals and explicit trade-offs

### 2.1 No compatibility layer

This is a clean pre-launch switch:

- delete `__metrics__` and its flusher;
- replace `_billing` with `_usage`;
- use fresh internal namespaces and schema version 1;
- do not dual-write old and new formats;
- do not build historical decoders or migration jobs.

### 2.2 Billing is not physical object-store cost allocation

Customer storage billing uses a stable, attributable definition of retained stream data. It does not attempt to assign transient WAL, compaction, manifest, cache, or replica overhead to individual streams.

Physical COGS remains an operational/cost metric by cell, shard, and object class.

### 2.3 Read metering accuracy

Exact synchronous read metering would require waiting for a durable telemetry write before or during every customer response, adding object-store latency and request cost to reads.

The chosen posture is:

- active read deltas flush every 10 seconds by default, earlier by size/cardinality thresholds;
- graceful shutdown and drain wait for the final usage batch;
- a hard process loss can undercount only the unflushed active interval;
- source epochs and batch sequences prevent overbilling after restart;
- the system exposes estimated unflushed read bytes and maximum possible loss.

If finance later requires zero-loss read metering, that is a separate product/cost decision requiring a synchronous durable read-receipt path.

---

## 3. Identity model

Every billing and customer-usage row carries explicit identity. No identity is inferred from a stream-name prefix.

```rust
struct BillingIdentity {
    account_id: AccountId,
    project_id: ProjectId,
    stream_id: StreamEpoch,   // immutable resource incarnation
    stream_name: String,      // display and lookup metadata
}
```

Additional source identity:

```rust
struct SegmentIdentity {
    stream_id: StreamEpoch,
    segment_id: u32,
    segment_route: RouteHash,
}

struct MeterSource {
    cell_id: String,
    region: String,
    instance_id: String,
    boot_id: Uuid,
}
```

### 3.1 Recreation semantics

Deleting and recreating `orders` creates a new `stream_id`.

- Invoice rows are resource-incarnation rows.
- The dashboard may additionally show a name-level monthly aggregate across incarnations.
- A stale telemetry observation for an old `stream_id` can never mutate the new stream's rollup.

### 3.2 Tenant boundary

The tenant boundary is explicit `account_id` and `project_id`, supplied by the control plane or authenticated token context and persisted in the descriptor.

A stream naming convention is not a tenant boundary.

---

## 4. Billable dimensions

### 4.1 Ingest

**Billable ingest bytes** are the sum of committed customer record payload bytes:

- after batch-envelope parsing;
- before compression and encryption;
- excluding HTTP headers, JSON-array brackets, commas, SSE framing, and internal metadata;
- counted only for non-duplicate records that become durable;
- counted for initial create content and final seal content;
- not counted for rejected, timed-out-but-uncommitted, or producer-duplicate requests.

```text
ingest_payload_bytes
committed_records
```

Keep `request_wire_bytes` as an operational metric, not the billing basis.

### 4.2 Reads

**Billable read bytes** are the sum of customer payload bytes externally delivered:

- product reads;
- raw Durable Streams reads;
- split-lineage pages;
- scans;
- SSE/subscription chunks;
- consumer deliveries, including redelivery;
- fork reads, attributed to the fork resource requested.

They exclude:

- response framing and headers;
- cursor/control JSON;
- Base64/SSE expansion;
- internal peer relays;
- retries between router and owners;
- HEAD responses and empty long-poll responses.

Zero-byte reads have zero data-read charge, while `read_operations` still records the operation.

Internal relays return an internal payload-byte count; the public request coordinator meters exactly once. Internal endpoints never increment customer usage.

### 4.3 Stored data

The billable storage gauge is:

```text
owned_canonical_frame_bytes
```

Definition:

- encrypted canonical record-frame bytes;
- counted once per record;
- independent of whether the record currently lives in shard tail or shared history;
- unchanged by absorption, tail trimming, compaction, WAL deletion, indexing, or cache state;
- decremented only when the record logically ceases to be retained, such as future record retention or final hard deletion.

Do not use total bucket bytes or temporary physical amplification as the customer billing dimension.

### 4.4 Forks

Fork behavior is explicit:

- inherited prefix bytes remain owned and billed to the source incarnation;
- a fork is billed for bytes appended to its own suffix;
- deleting a source while forks retain it keeps the source's storage line active with `retained_by_forks=true` until the shared prefix is no longer retained;
- the fork dashboard additionally exposes `logical_view_bytes` and `shared_base_bytes` for clarity, but these are not both billed.

This avoids double-billing copy-on-write data while preserving cost attribution.

### 4.5 Non-priced dimensions retained for product and operations

```text
append_requests
read_operations
read_records
queue_pull_operations
queue_settle_operations
watch_operations
producer_duplicates
rejected_requests_by_code
request_wire_bytes
response_wire_bytes
compression_ratio
```

These may support future packaging, abuse detection, and support investigations but are not required invoice dimensions in v1.

---

## 5. Metering coverage matrix

| operation | ingest bytes | read bytes | storage gauge | ops count |
|---|---:|---:|---:|---:|
| single append | committed payload | — | + canonical frame bytes | append |
| appendMany | committed payload sum | — | + canonical frame bytes | append |
| duplicate append | 0 | — | 0 | duplicate |
| create with initial content | committed payload | — | + frame bytes | create + append |
| seal with final content | committed payload | — | + frame bytes | seal + append |
| product read page | — | returned payload sum | — | read |
| split-lineage read | — | returned payload sum | — | read |
| raw default-key read | — | returned payload sum | — | read |
| scan page | — | returned payload sum | — | scan |
| SSE/subscription | — | each emitted payload | — | subscribe |
| consumer pull | — | delivered payload sum | — | pull |
| consumer redelivery | — | delivered payload sum again | — | pull/redelivery |
| settle/extend | — | 0 | — | queue op |
| empty long-poll/HEAD | — | 0 | — | read op |
| watch notification | — | 0 in v1 | — | watch op |
| fork creation | 0 | — | 0 owned bytes | fork |
| absorption/trim/compaction | 0 | 0 | unchanged | background ops |
| hard delete | 0 | 0 | closes accrual at deletion time | lifecycle |
| internal owner relay | 0 | 0 | 0 | internal only |
| `_usage`/`_ops_*` traffic | excluded | excluded | excluded | system only |

Every public operation gets a single metering choke point and a focused test. No billing logic is duplicated between raw, product, lineage, or internal relay implementations.

---

## 6. Durable data-plane billing state

### 6.1 Per-segment billing metadata

Every segment tail metadata row contains:

```rust
struct SegmentBillingMetaV1 {
    usage_version: u64,

    ingest_payload_bytes_total: u64,
    ingest_records_total: u64,

    owned_frame_bytes_current: u64,
    storage_accounted_through_ms: i64,

    // Current-month exact accumulators. Rollover code splits elapsed
    // storage time at UTC month boundaries.
    month: MonthId,
    month_ingest_payload_bytes: u64,
    month_ingest_records: u64,
    month_storage_byte_ms: u128,
}
```

On every committed append, in the same shard `WriteBatch`:

1. advance storage byte-time from `storage_accounted_through_ms` to commit time;
2. split at UTC month boundaries when necessary;
3. add committed payload and record counts;
4. add canonical frame bytes to the current storage gauge;
5. increment `usage_version`;
6. update the durable usage-outbox row.

A duplicate does none of these.

### 6.2 Storage clock behavior

- Absorption, tier movement, tail trimming, and compaction do not change the gauge.
- Future logical retention decrements the gauge at the logical expiration time.
- Hard deletion advances the storage clock, sets the gauge to zero, and emits a terminal lifecycle observation.
- Month close can extrapolate an idle gauge to the boundary without requiring a stream write.

### 6.3 Durable usage outbox

Each shard database contains coalescing outbox state:

```text
usage/outbox/<stream-id>/<segment-id>
    → latest SegmentUsageSnapshot

usage/dirty/<stream-id>/<segment-id>
    → latest usage_version not yet acknowledged by `_usage`
```

The outbox update is atomic with the record commit. It adds bytes to the existing shard WAL but no additional object-store request.

A drainer:

1. scans dirty entries;
2. batches observations across local shards;
3. appends them to `_usage` with an idempotent producer sequence;
4. after durable acknowledgement, marks the exact emitted version complete;
5. leaves a newer version dirty if another append raced the drain.

Crash cases:

- emit fails: dirty row remains;
- emit succeeds, clear fails: the same version is re-emitted and deduplicated downstream;
- owner moves: the new owner discovers dirty rows from durable storage;
- process restarts: no in-memory checkpoint is required.

This makes ingest and storage usage exact and replayable.

---

## 7. Read-delivery meter

### 7.1 Metering location

Meter only at the external response coordinator:

- internal owner RPCs return payload-byte and record counts;
- the coordinator increments usage once after accepting the page/chunk for external delivery;
- SSE increments as each chunk is yielded;
- owner relays and router retries do not increment billing.

### 7.2 Accumulator

A dedicated `ReadUsageAccumulator` is separate from admission/rate-limit maps.

It is keyed by `BillingIdentity`, rotates rather than falling back to an unattributed overflow bucket, and flushes on any of:

```text
10 seconds active interval
10,000 stream entries
1 MiB encoded batch
shutdown/drain
```

When the active map reaches its entry or size bound, it is sealed into a batch and a fresh map is installed. No per-stream attribution is discarded.

### 7.3 Source identity and idempotence

Each process boot has:

```text
source = cell / instance / boot-id
sequence = monotonically increasing batch number
```

A read batch carries absolute source-batch identity and deltas:

```json
{
  "v": 1,
  "kind": "read_batch",
  "source": {"cell": "...", "instance": "...", "boot": "..."},
  "seq": 42,
  "from_ms": 1786000000000,
  "to_ms": 1786000010000,
  "rows": [
    {
      "account_id": "...",
      "project_id": "...",
      "stream_id": "...",
      "stream_name": "orders",
      "read_payload_bytes": 123456,
      "read_records": 120,
      "read_operations": 13,
      "queue_operations": 2
    }
  ]
}
```

The producer retries the same sequence after an ambiguous append. The rollup tracks the last processed sequence per source boot and ignores duplicates.

A restart creates a new boot ID; counters restart from zero without re-billing prior batches.

### 7.4 Accuracy contract

- Graceful stops flush all read usage before exit.
- Hard process loss may undercount at most one active flush interval.
- No restart path can overcount an already emitted batch.
- `/operator` exposes current unflushed read bytes and the maximum possible loss window.
- The invoice report states the read-meter interval used during the month.

---

## 8. The `_usage` ledger

### 8.1 Resource

`_usage` is a reserved internal raw Durable Stream per cell:

- total ordered;
- invisible to customer catalog and limits;
- authenticated with an internal telemetry credential;
- encrypted with a mandatory system key;
- production startup fails readiness if the usage ledger or key is unavailable.

It replaces `_billing`. `__metrics__` is removed.

### 8.2 Record types

```text
segment_snapshot   exact durable ingest/storage state
read_batch         externally delivered read deltas
stream_lifecycle   create, soft-delete, hard-delete, expiry, fork retention
usage_correction   explicit post-close correction, never silent mutation
```

Common envelope:

```rust
struct UsageEnvelope<T> {
    schema_version: u16,
    event_id: String,
    event_time_ms: i64,
    emitted_ms: i64,
    cell_id: String,
    payload: T,
}
```

`event_id` is deterministic for segment snapshots and lifecycle observations. Read batches use source boot plus sequence.

### 8.3 Required fields in a segment snapshot

```text
account_id
project_id
stream_id
stream_name
segment_id
segment_route
usage_version
month
ingest_payload_bytes_month
ingest_records_month
owned_frame_bytes_current
storage_byte_ms_month
storage_accounted_through_ms
retained_by_forks
```

### 8.4 Self-metering

All reserved system streams and rollup databases are excluded from customer usage, rate limits, and catalog cardinality.

Their object-store requests and bytes remain visible as system COGS metrics.

---

## 9. Usage rollup service

### 9.1 Storage engine

Do not write one JSON object per active stream every 5–15 minutes.

Use one partitioned SlateDB rollup database:

```text
telemetry/usage-rollup/v1/<partition>
```

The active writer is fenced by SlateDB. Read replicas use `DbReader` and a shared cache.

### 9.2 Keyspace

```text
source/<boot-id>                              → last processed read-batch seq
segment/<project>/<stream-id>/<segment-id>    → latest absolute segment state
month/<YYYY-MM>/<project>/<stream-id>          → per-incarnation monthly rollup
name/<YYYY-MM>/<project>/<name-hash>           → aggregate across incarnations
project/<YYYY-MM>/<project>                    → project aggregate
meta/usage-cursor                              → consumed `_usage` cursor
```

### 9.3 Transactional processing

For each `_usage` page, one rollup `WriteBatch` updates:

- source deduplication state;
- segment state;
- monthly stream rows;
- name/project aggregate rows;
- the `_usage` cursor.

The batch is durable before the consumer advances. A crash replays the page safely.

### 9.4 Storage integration

For each stream rollup, maintain:

```text
last_stored_bytes
last_storage_observed_ms
storage_byte_ms
```

Segment snapshots carry exact current-month storage accumulators from the data plane. The rollup uses the larger, newer `usage_version` only; it never adds two snapshots of the same version.

For the current month, the API returns a provisional byte-time value extrapolated from the last gauge to `now`.

At month close, the closer advances every active gauge to the exact UTC boundary, then marks the month finalized after the grace period.

### 9.5 Month close and late data

- Calendar: UTC.
- Current month: provisional.
- Default close grace: 24 hours.
- Late records before close update the provisional month.
- Late records after finalization create a versioned `usage_correction`; finalized rows are never silently rewritten.
- Invoice generation stores the rollup version and correction set used.

### 9.6 Immutable month artifacts

At finalization, write one immutable object per billable stream incarnation:

```text
usage/monthly/<project>/<stream-id>/<YYYY-MM>.json
```

This is one PUT per active stream per month, not per telemetry tick.

It is the invoice/audit artifact. The live dashboard reads the rollup DB; historical finalized months may be served from the immutable object or cache.

---

## 10. Customer usage API

### 10.1 Endpoints

```http
GET /v1/streams/{name}/usage?month=YYYY-MM
GET /v1/streams/{name}/usage/current
GET /v1/projects/{project}/usage?month=YYYY-MM
```

Authorization uses the account/project bearer context. The customer encryption key is not required because usage is control-plane metadata, not record content.

### 10.2 Response

```json
{
  "projectId": "p_123",
  "streamId": "01J...",
  "streamName": "orders",
  "month": "2026-08",
  "status": "provisional",
  "ingestPayloadBytes": 123456789,
  "ingestRecords": 456789,
  "readPayloadBytes": 987654321,
  "readRecords": 765432,
  "readOperations": 12345,
  "storageByteSeconds": "123456789012345",
  "averageStoredBytes": 4601234,
  "gbMonth": 0.0041,
  "ownedStoredBytesNow": 5200000,
  "logicalViewBytesNow": 8100000,
  "sharedBaseBytesNow": 2900000,
  "updatedAt": "2026-08-06T12:34:56Z",
  "finalizedAt": null,
  "metering": {
    "readFlushIntervalSeconds": 10,
    "possibleReadLossWindowSeconds": 10
  }
}
```

### 10.3 Name recreation

If the same name had multiple incarnations during the month:

- the name endpoint returns an aggregate plus an `incarnations` breakdown;
- invoice line items remain keyed by immutable stream ID;
- a direct resource lookup uses the current stream ID.

### 10.4 Performance target

```text
p50 API latency: <= 25 ms inside the control plane
remote object GETs: <= 1 on a cold rollup block
ledger scans: 0
LIST requests: 0
```

---

## 11. Operational telemetry architecture

Operational telemetry is deliberately separate from customer billing.

### 11.1 Live plane

Retain and normalize:

- `/operator`;
- authenticated `/v1/debug/*`;
- fleet heartbeat state;
- router stats;
- shard, history, postings, queue, memory, and store gauges.

The live plane is for immediate diagnosis, not historical truth.

### 11.2 `_ops_metrics`

Every active instance emits a low-cardinality snapshot every 15 seconds, or earlier on a size threshold.

Use counters and mergeable histograms, not instance p50 values that cannot be aggregated.

Recommended dimensions:

```text
cell
region
instance
role (router/server/rollup)
operation class
store path class
HTTP status class
```

Do not use stream name, routing key, producer ID, or consumer name as time-series labels.

### 11.3 Required metric families

#### Request and customer experience

```text
append/read/scan/pull rate
success/error/throttle rate by code
latency exponential histograms
infrastructure 503 vs semantic 404
ownership replay count and latency
SSE connections and delivered bytes
```

#### Commit and storage pipeline

```text
committer queue depth and wait
records/requests/bytes per group
WAL PUT count, bytes, p50/p99 histogram
encode/write/durable-wait histograms
L0 count and compaction backlog
manifest/CAS conflicts
object-store operation count, bytes, status and latency
served-from PoP distribution
```

#### Routing and fleet

```text
live/desired instance count
owned shards per instance
ownership moves/returns
rebalance duration
router replay rate
override age
URL-map freshness
instance dark/live transitions
cross-owner relay rate, bytes and latency
```

#### History and read path

```text
absorb backlog bytes/streams/age
trim backlog
history reader hits/misses/reopens
postings cache entries/bytes/hit rate
canonical scan amplification
ring hit rate
```

#### Consumers and lifecycle

```text
active leases
stale lease tokens
consumer delete steps/rows/time
consumer fence cardinality
seal fence cardinality
fork cleanup debt
initializing/sealing/deleting resource counts
```

#### Runtime and capacity

```text
CPU
cgroup memory.current
RSS/footprint
memory shed/purge count and duration
event-loop delay histogram
inflight requests
open engines/handles/tasks
```

#### Telemetry pipeline

```text
usage outbox dirty entries and age
_usage append failures and lag
unflushed read bytes
usage rollup cursor lag
month-close lag
ops event outbox lag
ops metrics rollup lag
telemetry bytes and Class A/B request count
```

---

## 12. Durable operational event journal

### 12.1 Resource

`_ops_events` is a reserved, encrypted, total-order internal stream per cell.

Events are typed, versioned, and deduplicated by `event_id`.

### 12.2 Event schema

```rust
struct OpsEvent {
    schema_version: u16,
    event_id: String,
    event_time_ms: i64,
    observed_ms: i64,
    cell_id: String,
    region: String,
    event_type: String,
    severity: Severity,
    instance_id: Option<String>,
    project_id: Option<ProjectId>,
    stream_id: Option<StreamEpoch>,
    stream_name: Option<String>,
    shard: Option<String>,
    segment_id: Option<u32>,
    state_version: Option<u64>,
    cause: Option<String>,
    fields: serde_json::Value,
}
```

Routing keys, record payloads, encryption keys, bearer tokens, producer IDs, and consumer tokens never appear in ops events.

### 12.3 Event types

#### Stream and topology

```text
stream_created
stream_sealing
stream_sealed
stream_soft_deleted
stream_hard_deleted
stream_expired
fork_created
fork_deleted
split_pending
split_committed
merge_pending
merge_committed
hot_key_detected
ineffective_split_avoided
```

#### Ownership and fleet

```text
instance_live
instance_dark
instance_replaced
desired_changed
rebalance_move
rebalance_return
ownership_changed
override_created
override_removed
peer_url_changed
router_upstream_ejected
router_upstream_recovered
```

#### Shard and storage

```text
engine_opened
engine_closed
engine_fenced
WAL_stall_detected
WAL_stall_recovered
reopen_started
reopen_failed
GC_backlog
compaction_backlog
```

#### Billing and telemetry

```text
usage_ledger_unavailable
usage_outbox_lag
rollup_lag
month_finalized
usage_correction
telemetry_gap
alert_opened
alert_resolved
```

### 12.4 Durability model

Operational events never block the product transition they describe, but they are not merely best-effort logs.

For CAS-backed transitions:

- the durable state object contains a small pending-event outbox with deterministic event IDs;
- the CAS that commits the transition also records the event envelope;
- a drainer appends the event to `_ops_events` and then clears the exact outbox entry;
- repeated emission is safe because the event ID is deterministic;
- outbox depth and age are monitored.

For state that is naturally versioned but not descriptor-backed, such as fleet `desired.json` and `overrides.json`, the same pending-event array is part of the CAS object.

For instance live/dark and router observations:

- the detector maintains a durable detector epoch and deterministic transition ID;
- duplicate events are ignored downstream.

An outbox cap protects object size. If the cap is exhausted, the transition still proceeds, a durable `events_dropped` count increases, and a `telemetry_gap` event is emitted once capacity returns. Operations telemetry must not stop the product data plane.

### 12.5 Operator use

The operator UI gains:

- recent events;
- filters by type, stream, shard, instance, and severity;
- topology timeline for a stream;
- ownership/rebalance timeline for a shard;
- links from alerts to the supporting metric window and runbook.

---

## 13. Operational metrics rollup and alerts

### 13.1 Rollup database

A dedicated SlateDB rollup store consumes `_ops_metrics` and `_ops_events`.

Retention tiers:

```text
raw 15-second points:     7 days
1-minute rollups:         30 days
5-minute rollups:         180 days
1-hour rollups:           24 months
ops events hot:           90 days
ops events archived:      13 months
```

Billing retention is separate:

```text
_usage raw ledger:        at least 18 months
finalized monthly usage:  accounting retention policy, default 7 years
```

### 13.2 Alert evaluator

Evaluate every 15 seconds to 1 minute depending on the alert.

Initial alert rules:

```text
append ack p50/p99 SLO breach
customer error-rate breach
infrastructure 404/503 spike
router replay rate sustained above baseline
store/WAL latency breach
WAL or compaction stall
memory shed or OOM pressure
event-loop lag
absorb or trim lag
fleet desired/live mismatch
ownership imbalance or override age
peer URL map stale
usage outbox lag
usage ledger unavailable
rollup lag or month-close failure
telemetry outbox overflow
```

Alerts have a fingerprint, `opened_at`, `last_seen`, and `resolved_at`. Open and resolved transitions are appended to `_ops_events`.

External notification targets such as webhook, email, PagerDuty, Prometheus, or OpenTelemetry exporters can subscribe later. The stored internal record remains the audit trail.

---

## 14. Billing and telemetry pipeline self-observability

The telemetry system must expose its own health as first-class state.

### 14.1 Readiness

In production billing mode, readiness fails when any of these is unavailable beyond its configured grace period:

- system telemetry key;
- `_usage` append path;
- rollup writer lease/fencing;
- usage-rollup database;
- project/account identity source.

Customer data operations continue through brief telemetry outages because the durable shard outbox accumulates ingest/storage state. Read responses may backpressure when the read accumulator cannot rotate safely; they never silently discard attribution.

### 14.2 Lag metrics

```text
oldest dirty usage outbox age
number of dirty segment snapshots
read batches pending
_usage end minus rollup cursor
month-close pending streams
ops event outbox age
_ops_metrics end minus rollup cursor
```

### 14.3 Reconciliation

Run scheduled reconciliation:

- compare latest segment billing versions with rollup segment versions;
- verify all hard-deleted streams have closed storage accrual;
- compare aggregate committed ingest totals with stream/segment totals;
- verify source-batch sequences have no unexplained gaps;
- verify finalized monthly objects match rollup DB rows and invoice exports;
- sample customer dashboard responses against ledger replay.

Invoices are generated only after reconciliation passes or a correction is recorded.

---

## 15. Security, privacy, and access

- `_usage`, `_ops_metrics`, and `_ops_events` use a separate fleet-internal credential and mandatory system encryption keys.
- They are not reachable through the customer catalog or standard stream APIs.
- Customer usage APIs require project/account authorization but not the customer record-encryption key.
- Per-stream usage and names are not exposed on the unsecured operator page.
- Ops metrics are low-cardinality and exclude customer identifiers.
- Ops events may contain stream names only on authenticated operator surfaces; a deployment may store only stream IDs/hashes if policy requires.
- Financial rollups survive customer data deletion according to accounting retention policy.
- Secrets, routing keys, record content, producer identities, lease tokens, and signed watch capabilities are prohibited telemetry fields.

---

## 16. Failure semantics

| failure | expected behavior |
|---|---|
| shard process crashes after append commit before usage emit | durable outbox is drained by restart/new owner; no ingest/storage loss |
| `_usage` append response lost | retry same producer sequence/event IDs; no duplicate billing |
| usage append succeeds, outbox clear fails | snapshot is re-emitted; rollup deduplicates by version |
| rollup crashes after row updates before cursor commit | one atomic rollup batch prevents split state; page replays safely |
| rollup writer split-brain | SlateDB fencing leaves one writer; loser retries as reader/candidate |
| read-meter process crash | at most one active interval undercount; new boot ID prevents re-bill |
| month-close worker crashes | finalized flag is absent; retry resumes from rollup state |
| event append fails | durable event outbox remains; product transition is not rolled back |
| telemetry event outbox overflows | product proceeds; durable gap counter/event; operator alert |
| customer stream deletes/recreates same name | old stream ID closes; new ID starts independent rollup |
| ownership move | segment billing meta and dirty outbox move with shard state |
| internal fan-out retries | no customer usage increment until public coordinator delivery |

---

## 17. Cost posture

The telemetry design must not recreate the per-stream request tax removed by history v2.

### 17.1 Default cadences

```text
read usage batches:       10 s active; early by 10k entries / 1 MiB
segment snapshot drain:   included in the same instance usage batch
ops metrics snapshots:    15 s
usage rollup flush:       1–5 s, batched across rows
ops rollup flush:         5–15 s, batched
monthly JSON artifact:    once per active stream per month
```

### 17.2 Request budgets

Initial acceptance budgets:

```text
_usage appends             <= 360 per active instance-hour
_ops_metrics appends       <= 240 per active instance-hour
per-stream PUTs per tick   = 0
LIST requests steady state = 0
monthly stream artifacts   <= 1 PUT per active stream-month
customer usage lookup      <= 1 cold rollup block GET
```

At public Tigris list prices, a 10-second active usage cadence is roughly $1.30 of Class A request cost per continuously active instance-month before group-commit sharing. This cost is explicit and monitored.

---

## 18. Implementation plan

### Phase 0 — schema and clean cutover

1. Add `account_id`, `project_id`, and immutable stream identity to the descriptor context.
2. Reserve and hide `_usage`, `_ops_metrics`, and `_ops_events`.
3. Define schemas and versioned codecs.
4. Remove `metrics.rs`, `__metrics__`, and the old `_billing` emitter.
5. Start with a fresh telemetry namespace; no compatibility layer.

### Phase 1 — complete and unify metering

1. Implement one central public-response read meter.
2. Cover split reads, forks, scans, SSE, and consumer pulls.
3. Add operation counters without using them as billing truth.
4. Exclude internal relays and system streams.
5. Add the full metering coverage matrix tests.

### Phase 2 — durable ingest/storage state and outbox

1. Extend segment tail metadata with `SegmentBillingMetaV1`.
2. Update it atomically on committed appends and logical deletion/retention.
3. Add durable usage-outbox and dirty rows.
4. Add owner-open dirty discovery and idempotent drain.
5. Verify split, merge, owner move, crash, absorption, and trim behavior.

### Phase 3 — `_usage` and rollup

1. Create the mandatory internal usage stream.
2. Implement the segment-snapshot drainer and read accumulator.
3. Build the usage rollup SlateDB and cursor transaction.
4. Implement current and monthly usage APIs.
5. Implement month close, immutable monthly artifacts, and corrections.
6. Add invoice reconciliation.

### Phase 4 — durable ops events

1. Add deterministic event schemas and IDs.
2. Add outbox fields to registry/fleet CAS state.
3. Emit split, merge, ownership, desired, instance, fence, and lifecycle events.
4. Add recent events and topology timelines to the operator UI.

### Phase 5 — operational metrics and alerts

1. Emit mergeable `_ops_metrics` snapshots.
2. Build operational rollups and retention tiers.
3. Implement initial SLO alerts and alert events.
4. Add optional standard exporters.

### Phase 6 — cleanup and release gates

1. Delete old telemetry code and environment variables.
2. Update runbooks, security inventory, and SLO documentation.
3. Run cost campaigns for telemetry overhead.
4. Run multi-instance failure and reconciliation campaigns.
5. Tag the first telemetry-layout release only after month-close and invoice replay pass.

---

## 19. Test and acceptance plan

### 19.1 Billing correctness

- append and appendMany committed bytes exactly match record payloads;
- duplicates and rejected appends add zero;
- split/merge does not reset or double ingest/storage totals;
- absorption and trim do not alter storage gauge;
- hard deletion closes storage accrual at the correct timestamp;
- fork creation does not double owned storage;
- restart and owner movement do not lose or duplicate segment snapshots;
- monthly boundary split is correct;
- late events create explicit corrections;
- name recreation creates independent usage identities.

### 19.2 Read coverage

- single-segment product and raw reads;
- split-lineage reads;
- fork reads;
- scan pagination;
- SSE chunks and cancellation;
- consumer pull and redelivery;
- empty long poll and HEAD;
- cross-owner fan-out with one logical customer increment;
- infrastructure retries with zero duplicate metering.

### 19.3 Rollup durability

- duplicate `_usage` events;
- out-of-order source batches;
- rollup crash before/after cursor update;
- writer fencing/failover;
- current-month provisional query;
- month close with idle streams;
- finalized artifact and rollup equality;
- correction after close.

### 19.4 Operations

- every split/merge produces one deterministic event;
- rebalancing and desired changes survive process loss;
- instance dark/live and URL replacement timeline;
- outbox append failure and retry;
- event-gap behavior at cap;
- alert open/dedup/resolve;
- histogram aggregation across instances.

### 19.5 Cost gates

- zero per-stream telemetry PUTs per interval;
- no steady-state LIST loops;
- usage and ops append cadence within budget;
- dashboard point read without ledger scan;
- telemetry traffic excluded from customer invoices;
- telemetry COGS visible by cell and month.

### 19.6 DST scenarios

Add deterministic crash points at:

```text
after record+usage-meta write, before usage emit
after usage emit, before outbox clear
after rollup row updates, before cursor advance
at UTC month rollover
before/after lifecycle gauge closure
after CAS transition with event outbox, before event emit
cross-owner read relay before coordinator meters
```

---

## 20. Operational dashboards

### 20.1 Customer dashboard

Per stream:

- current-month ingest/read/storage;
- prior finalized months;
- compression ratio and current owned/logical bytes;
- per-incarnation breakdown for recreated names;
- provisional/finalized/corrected status;
- last update and metering accuracy window.

### 20.2 Fleet operator dashboard

Overview:

- SLO health;
- request/error/latency histograms;
- instance live/desired/ownership distribution;
- store and WAL latency;
- memory and event-loop health;
- absorb/trim/history/postings lag;
- usage/event/rollup pipeline lag;
- open alerts.

Timelines:

- stream topology events;
- shard ownership/rebalance history;
- fleet desired-count history with reasons;
- instance live/dark/replacement history;
- engine fences and reopen failures;
- billing and telemetry incidents.

---

## 21. Decisions made by this design

1. **Tenant boundary:** explicit account/project identity, never stream-name prefixes.
2. **Ingest billing:** committed logical payload bytes before compression/encryption.
3. **Read billing:** externally delivered logical payload bytes; zero-byte reads are free in the data dimension.
4. **Storage billing:** canonical encrypted frame bytes owned by the stream; no WAL/index/compaction amplification.
5. **Fork storage:** shared prefix remains billed to its source; forks pay only for their own suffix.
6. **Fan-out:** internal relay is unmetered; public response coordinator meters once.
7. **Legacy metrics:** `__metrics__`, `metrics.rs`, and old `_billing` are deleted.
8. **Read accuracy:** maximum hard-crash undercount equals the active read-flush interval, default 10 seconds; no overbilling from restart.
9. **Billing retention:** raw usage 18 months; finalized monthly records follow accounting retention, default 7 years.
10. **Ops retention:** 7 days raw, 30 days one-minute, 180 days five-minute, 24 months hourly; events 90 days hot and 13 months archived.
11. **Dashboard storage:** live rollups in SlateDB; one immutable JSON artifact per active stream-month at finalization.
12. **Ops events:** deterministic, durable outbox/reconciliation; not process-log-only.

---

## 22. Final architecture summary

```text
                         CUSTOMER DATA PLANE

 append commit ──────┐
                     │ same shard WriteBatch
                     ▼
          segment billing meta + durable usage outbox
                     │
                     │ batched, idempotent drain
                     ▼
                 _usage ledger  ◀──── read batches from public coordinators
                     │
                     │ exactly-once cursor + SlateDB WriteBatch
                     ▼
               usage rollup DB
                  │          │
        point-read API       monthly immutable artifacts / invoices


                         OPERATIONAL PLANE

 live counters + histograms ───────▶ _ops_metrics ─────▶ ops rollup DB
 CAS transitions + event outboxes ─▶ _ops_events  ─────▶ timelines/alerts

 /operator and /v1/debug remain the live diagnostic surface.
```

The billing ledger is immutable and replayable. The rollup is a derived materialization. Customer lookup is a point read. Operational events are durable and typed. Time-series are mergeable and retained at explicit resolutions. Internal telemetry has a measured request-cost budget and is itself observable.
