# Profiles on the SlateDB architecture

How the profile model from the Bun/TS implementation (`generic`, `evlog`,
`metrics`, `otel-traces`, `state-protocol`) maps onto the new architecture
([README.md](./README.md)), which small semantic changes we should take while
porting, and a proposal for the planned **`queue`** profile.

The profile *model* carries over unchanged: profile = semantics, schema =
structure, stream = durable storage; core paths dispatch through profile
hooks, never `if profile.kind == ...` branches. What changes is the substrate
the hooks act on.

---

## 1. Where profile behavior lives now

The TS implementation gave profiles four attachment surfaces: append
normalization, background index/companion builders over sealed segments,
profile HTTP routes, and profile config in SQLite. The new architecture has
four analogous — but differently shaped — hook points:

| hook point | when it runs | what it has | replaces (TS) |
|---|---|---|---|
| **H1 append hook** | in the HTTP handler, after key check, **before encryption** | plaintext payload + stream key + descriptor | JSON-ingest normalization hooks |
| **H2 commit hook** | on the shard owner when the durable watermark passes a batch | record plaintext (retained in-memory through the commit pipeline; no decrypt needed) | touch processor feed, notifier |
| **H3 absorber hook** | while the absorber has a stream's history DB open, key in hand | decrypted records in bulk + writable history keyspace | segment index builders, companions (`.fts`/`.col`/`.agg`/`.mblk`), rollup managers |
| **H4 query surface** | profile HTTP routes; every request carries the stream key | history keyspaces + shard tail + registry | search/aggregate endpoints, `/touch/*`, OTLP ingest |

Two cross-cutting substitutions do most of the work:

**Companions → history-DB keyspaces.** Every derived structure the TS
implementation stored as per-segment companion objects becomes additional key
ranges inside the *same* per-stream history DB, written atomically with the
absorbed records (one `WriteBatch`), block-encrypted with the stream key and
zstd-compressed like everything else. There is no separate index publication,
no companion catalogs, no plan generations, no index-lag bookkeeping beyond
the absorber cursor: **indexes are exactly as durable, as encrypted, and as
fresh as absorption itself.** Reserved key tags in the history DB:

```
r!<offset>                       records            (exists today)
k!<len><rk><offset>              routing-key index  (exists today)
f!<field>!<hash(value)>!<offset> exact-match secondary index
a!<res>!<window>!<field>[!dims]  rollup cells (merge-operator values)
p!<profile-specific...>          profile-owned state (e.g. mblk successor)
```

**Aggregations → SlateDB merge operators.** Rollups (counts, sums, min/max,
histogram sketches) are written as `merge()` deltas; SlateDB folds them
associatively at read and compaction time. This deletes the entire
"rebuild-rollups-over-segments" machinery: an absorber batch emits one merge
per touched rollup cell and is done. Per-resolution retention (1m/1h/1d
cells) uses SlateDB row-level TTL on the `a!` keys.

**One new semantic to state plainly (applies to every profile):** derived
state is **key-gated**, like absorption. A stream that receives appends but
no further keyed traffic keeps its tail un-indexed until the next keyed
request (nobody could query it without the key either, so this is
unobservable in practice). Search/aggregate answers are complete up to
`absorbed_through` plus a documented tail scan; queries always carry the key,
so the tail can be filtered/aggregated in-line for exact answers.

Profile declaration and config move into the **registry descriptor**
(CAS-updated JSON), replacing SQLite profile metadata. Schema registries
attach to the descriptor the same way; the schema rules (first install on
empty stream, `v → v+1` lenses, routing-key derivation) carry over verbatim
and run in H1.

---

## 2. `generic`

Unchanged; it is what the current implementation already serves. Optional
schema validation and schema-managed routing-key extraction slot into H1.
No proposed semantic changes.

## 3. `evlog`

**Mapping.** Envelope normalization, redaction, and the
`requestId`→`traceId` routing-key default are pure H1 — they run on the
plaintext before encryption, so stored ciphertext already contains the
canonical, redacted envelope. Default search fields and rollups install as
descriptor config; the absorber (H3) writes `f!` entries for the configured
exact-match fields and `a!` merge cells for rollups. `filter=` queries (H4)
prune with `f!` where possible, verify against decrypted records, and scan
the un-absorbed tail in-line.

**Proposed semantic changes.**
1. **Redaction becomes explicit-config-only.** In the TS system, redaction
   before durable append was the primary protection for sensitive context
   keys. Here every payload is encrypted with a customer key, so blanket
   default redaction now silently *destroys data the customer already
   protected*. Proposal: keep the mechanism, drop the built-in default key
   list; redaction rules live in the descriptor and default to empty.
   (Rationale to document: redaction's remaining job is limiting what
   *key-holders* can later see, not hiding data from the platform.)
2. **Normalization is declared server-side.** The envelope is canonicalized
   by the origin before encryption. If a future SDK moves to client-side
   encryption of appends, normalization must move into the SDK; we pin the
   envelope in the SDK contract now (`streams-keys` repo) so that door stays
   open.
3. Search field *values* in `f!` keys are stored as salted hashes
   (`hash(field, value, streamEpoch)`), not plaintext — the index answers
   equality without leaking values into key bytes, closing the V1-adjacent
   concern for secondary indexes at zero query cost (equality is the only
   supported `f!` operation anyway; ranges verify against records).

## 4. `metrics`

**Mapping.** Interval normalization: H1. `seriesKey` routing key: H1 →
the native `k!` index gives per-series scans directly. The `.mblk`
metrics-block family and the `PSCIX2` search companions — both of which
existed to make segment-oriented storage queryable — are **deleted, not
ported**: their entire job is done by `a!` merge-operator rollup cells at
1m/1h/1d resolutions keyed by `(series, window)`, written in the absorber,
TTL'd per resolution, and read by a `/v1/stream/{name}/_metrics/query` H4
endpoint that folds the un-absorbed tail in-line.

**Proposed semantic changes.**
1. **Drop the companion-family concept from the public contract.** The TS
   spec leaks `.mblk`/`.cix` existence into stream details. The new contract
   speaks only of *resolutions* and *retention per resolution* — storage
   layout becomes invisible, which it should have been.
2. **Late/out-of-order intervals get a documented horizon.** Merge cells make
   late data trivially absorbable (another merge), so we can *widen* the TS
   semantics: accept intervals up to a configurable lateness horizon (default
   e.g. 24h) instead of clamping to append time.

## 5. `otel-traces`

**Mapping.** Span normalization, attribute/event/link limits: H1. OTLP
JSON/protobuf decode: H4 routes (`POST /v1/stream/{name}/_otlp/v1/traces`
requires the key header like any append; the global `POST /v1/traces` needs a
stream-selection header — keep it, but per-stream is primary). `traceId`
routing key means **trace assembly is a single contiguous `k!` scan** — the
new architecture does natively what the TS implementation needed its routing
index machinery for. Duration/error rollups: `a!` cells.

**Proposed semantic changes.**
1. **Cross-stream request observability moves out of the core.** The
   `/v1/observe/request` API correlates `evlog` and `otel-traces` streams;
   under per-stream keys, a correlation query needs *both* streams' keys.
   Proposal: keep the pairing declaration (`observability.request` in the
   descriptor), but serve correlation as an H4 endpoint that accepts **two
   key headers**, and mark the fully-server-side observability store as out
   of scope — assembly of summaries/trees/timelines is cheap enough to do
   per-request from two `k!` scans (it was already "a query layer, not a
   store" in the TS design; this makes that literal).
2. Redaction: same change as evlog (explicit config, no default list).

## 6. `state-protocol` (Live / touch) — **implemented and stress-tested**

**Mapping (as built).** This profile owns runtime behavior, not storage:
State Protocol records in, `/touch/*` invalidation out. Touch keys are
derived in the append handler (H1) from the plaintext change records against
a **lock-free template snapshot**, ride the commit pipeline, and reach the
**owner-local, in-memory touch journal** only after the durable watermark
(H2) — an invalidation can never precede read visibility. `/touch/wait`
long-polls resolve through a per-key inverted waiter index (flush cost ∝
touched keys, not waiters), with dead waiters reaped every second.

Derivation deliberately stays on the parallel request path rather than in
the committer: the committer is the shard's serialization point, and the
per-record cost (a handful of xxh3 hashes, bounded by template caps) is
microseconds. Template caps: **256 per stream, 64 per entity** (H4 returns
`429 template_limit` beyond).

**Journal bounds** (fits the 1 GB instance budget): 25 ms buckets capped at
64k unique key IDs (overflow ⇒ wake-everyone — never miss); closed buckets
retained as sorted `u32` vecs under a **global 2M-key budget (~8 MB)**;
evicted generations degrade to conservative resync.

**The wait surface: collapsible GET-per-key (each watch key is a virtual,
cursor-addressed invalidation stream).** The former POST `/touch/wait`
(multi-key bodies, dynamic template activation, `declareTemplates`,
server-side herd jitter) is **removed**; the contract is:

```
GET /v1/stream/{name}/touch/key/{watchKeyHex}?cursor={epoch:gen}&sig={hmac}[&timeout=25s]
→ {touched:true, reason:"touched"|"resync", cursor, streamEndOffset}   (head wake: public, max-age=2)
→ {touched:false, cursor, streamEndOffset}                             (timeout: no-store)
→ {stale:true, cursor, error:{code:"stale"}}                           (epoch mismatch / fence: no-store)
```

- **Cursors are journal-global**, so an entire cohort watching the same key
  converges on byte-identical URLs; a CDN (or any coalescing edge) collapses
  them into **one origin long-poll per (key, cursor)** and fans the response
  out. Multi-key queries issue parallel GETs; the SDK batches.
- **`sig` = HMAC(waitSigKey, watchKey)** where
  `waitSigKey = HKDF(touchToken, epoch, "wait-sig-v1")`: URL possession is
  the observation capability, so cache keys are self-authorizing. Clients
  derive it offline from the touch token
  (`HKDF(streamKey, epoch, "touch-capability-v1")`,
  `streams-keys derive-touch-token`); the registry stores only the sig key
  (observation-forging at worst — never decryption) and the token's
  fingerprint. `/touch/meta` accepts `Touch-Token` or the stream key.
- **Templates are pinned in the stream descriptor**
  (`Stream-Touch-Templates` header at creation, durable, caps enforced
  there): query families are deploy-time configuration. No dynamic
  activation, no heartbeats, no TTL — and nothing to lose on a restart or
  move, which deletes the entire `declareTemplates` recovery dance.
- **Coalescing, not caching, is the mechanism.** Measured: in steady state
  the entire origin-load reduction comes from in-flight collapse (cache hit
  rate ~0). Head wakes cache for only **2 s** (a straggler window); catch-up
  answers **jump to the head** and are `no-store` — both learned from the
  stress test, where long-TTL cached wakes let desynchronized clients walk
  the cached touch chain one generation per hop (735k spurious wakes until
  fixed, 3.7k after).
- Server-side herd jitter is **gone**: a collapsed cohort receives one
  fanned-out response, so origin-side spreading is meaningless. Re-query
  stampede protection moves to the SDK (client jitter) — or disappears when
  clients delta-read via `streamEndOffset` instead of re-querying.

**Move-aware invalidation (as built):**
1. Cursor epoch mismatch ⇒ `stale` — re-run and restart. No seeding, no key
   dependence, and (with pinned templates) no re-declaration.
2. **Fence-close**: a fenced shard closes its journals, waking every hanging
   waiter with `stale` immediately (verified live: ~2 s instead of a full
   timeout).

**Benchmark — old POST fan-out vs collapsible GET through a coalescing edge**
(livebench, 2,000 fine waiters over 50 tenants = 40-waiter cohorts, ~59k WAL
changes at ~2k/s, 25 ms emulated object store; edgesim implements
CDN-style GET coalescing + Cache-Control):

| | old POST /touch/wait | GET /touch/key via edge |
|---|---:|---:|
| origin wait requests | **216,280** (~7,200/s) | **5,852** (~195/s) |
| origin load reduction | 1× | **40.0×** (= cohort size) |
| invalidation latency p50/p99 | 52.4 / 82.1 ms | 53.6 / 83.5 ms |
| missed invalidations | 0 | 0 |

Kill-restart chaos through the edge: 0 missed, self-healing recovery,
p50 50 ms. Adapter rule unchanged: **append only after the database commit
is visible** — the inverse ordering measurably loses invalidations.

---

## 7. `queue` — **implemented** (informed by Cloudflare Queues)

**Status: built and verified end-to-end** ([queue.rs](./src/queue.rs), HTTP
surface in http.rs, exercised by the TypeScript SDK example). What we took
from studying Cloudflare Queues, beyond the original SQS-style proposal:

- **One combined settle endpoint** (`POST /queue/{consumer}/ack` with
  `acks` + `retries` + `extends` arrays): a whole batch's disposition is one
  durable round trip, and per-message `delayMs` on retries gives backoff
  without a DLQ hop (CF's `delay_seconds`).
- **Permissive lease tokens**: stale/duplicate tokens are ignored and
  counted, never errors — retries of the settle call are safe (CF: "Queues
  aims to be permissive when it comes to lease IDs").
- **`backlog` on every response** — CF's `message_backlog_count`, the
  consumer-autoscaling signal for serverless workers.
- **`attempts` on every message**; defaults matched to CF (batch 5,
  visibility 30 s, batch cap 100).
- **Lazy expiry** (no sweeper): an expired lease is simply re-leasable at
  the next receive; a message at `maxDeliveries` settles with a reference
  record appended under routing key `$dlq` — the DLQ is a normal keyed
  read view, browsable and replayable.
- Deliberately *not* copied: CF has no FIFO groups; we keep routing keys as
  the future FIFO-group mechanism (spec'd below, lands with per-key queues).
- **Per-key scaling compatibility**: analyzed and documented in
  PER-KEY-ORDERING.md §7 — compatible by construction; DLQ appends go
  two-stage (local durable marker, async mover) when the combo ships, and
  consumer groups (Pravega-style) become the multi-segment receive surface.

All state transitions run through the shard committer (serialized with
appends, durable at the watermark); consumer state (cursor + leases +
early-ack markers) lives in the shard log under `c`/`l`/`x` tags and is
rebuilt by a prefix scan on first use — crash/move-safe at-least-once by
construction.

### Original proposal (retained for the design rationale)

**Positioning.** SQS-style per-message semantics (lease, ack, redelivery,
DLQ, delay) implemented as **rebuildable consumer state over the immutable
stream** — enqueue is just append, message identity is the offset, and the
full history remains readable through the normal stream API (replay,
audit, backfill: a queue you can also tail). No new storage engine concepts:
consumer state is a small keyspace in the shard log, written through the same
committer, durable at the same watermark, moved by the same fencing.

**Shard-log keyspace additions** (owner-local, atomic with appends):

```
<hash16> c <consumer>                cursor: all offsets below are settled
<hash16> l <consumer> <offset>       lease: {deadline_ms, delivery_count, gen}
<hash16> x <consumer> <offset>       out-of-order ack marker (above cursor)
<hash16> d <visible_at_ms> <offset>  delay index (enqueue with delay)
```

Consumer groups are auto-created strings (serverless: no registration).
Settled messages compact away via cursor advance + marker deletion; the
records themselves remain in the stream subject to normal retention.

**API (H4), long-poll native:**

- `POST .../_queue/{consumer}/receive?max=10&wait=20s&visibility=30s` →
  batch of `{offset, payload, deliveryCount, ackToken}`; holds like a
  long-poll when empty. `ackToken = offset + lease generation`, so a
  redelivered message invalidates stale tokens.
- `POST .../_queue/{consumer}/ack` (batch), `.../nack` (immediate
  redelivery, optional `delay`), `.../extend` (heartbeat a long handler).
- Enqueue = normal append; optional `Queue-Delay: 30s` header populates the
  `d` index; optional `Queue-Dedup-Id` gives a 5-minute dedup window via a
  TTL'd marker key.

**Semantics.**

- **At-least-once**, exactly like every serious serverless queue; offsets are
  natural idempotency keys for consumers.
- **Visibility/redelivery**: an owner-local timer sweeps expired leases (they
  are ordinary keys; a shard move rebuilds the sweep from the keyspace, so
  redelivery survives moves with at-most one visibility-timeout of delay).
- **FIFO groups for free**: with `fifo: true` in the descriptor, the routing
  key is the message group and the `k!`-ordered view delivers one in-flight
  batch per group at a time — SQS FIFO semantics from machinery we already
  have.
- **DLQ**: after `maxDeliveries`, the sweeper *appends* the message reference
  to the same stream under routing key `"$dlq"` and settles the original —
  the DLQ is a routing-key view, browsable with a normal key read, replayable
  by re-enqueueing.
- **Encryption**: payloads stay in the normal envelope (receive carries the
  stream key and returns plaintext, or frames for SDK consumers). Queue
  state keys hold only offsets/deadlines/counters — operational metadata,
  never payload.
- **Serverless fit**: everything is stateless HTTP + long-poll; a Lambda/
  Worker can receive → process → ack in one invocation with zero SDK state;
  `extend` supports long handlers; batch endpoints amortize the durable
  round-trip (acks group-commit like appends — an ack costs the same ~30–60ms
  durability as an append, batched across all consumers on the shard).
- **Limits to document**: per-queue throughput = per-stream ceiling (one
  shard); consumer-state writes share the queue's group commit; no push
  delivery in v1 (webhook subscriptions are a later descriptor feature);
  `receive` ordering across groups is best-effort.

**Why this shape wins here:** every queue primitive lands on a mechanism the
platform already paid for — leases and cursors are just keys behind the
durable watermark, redelivery is a keyspace sweep, FIFO is the routing index,
DLQ is a routing key, delay is one extra index, and crash/move safety is
fencing. The only genuinely new component is the lease sweeper.

---

## 8. Suggested build order

1. **`queue`** — highest product leverage, exercises only shard-log
   machinery (no absorber hooks), and its lease sweeper is reusable for TTL
   enforcement generally.
2. **`evlog`** — H1 normalization + `f!`/`a!` in the absorber; brings the
   flagship logging story over and forces the merge-operator plumbing.
3. **`metrics`** — mostly `a!` cells + one query endpoint once evlog laid
   the plumbing; deletes the largest chunk of TS machinery (`.mblk`).
4. **`otel-traces`** — H1 + OTLP decode + `k!` trace assembly; two-key
   correlation endpoint last.
5. **`state-protocol`** — H2 journal + `/touch/*` with move-aware
   invalidation; schedule alongside the fleet work since its semantics
   interact with shard moves.

Open questions to settle before implementation: rollup cell schema and
sketch choice for histograms (`a!` values); the exact `f!` hash construction
(shared with the SDK for client-side query planning?); whether `queue`
consumer state should count against the stream's logical size for billing;
and the two-key authorization shape for `/v1/observe/request`.
