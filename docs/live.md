# Prisma Streams Live / Touch System

Live is an invalidation system built on Durable Streams.

The system is intentionally simple:

1. Append State Protocol changes into a base stream.
2. Run the real query against your database.
3. Wait on a small keyset with `/touch/wait`.
4. Re-run the query when the wait wakes.

This is invalidation, not incremental query maintenance. Durable Streams tells
you that a query may have become stale. It does not compute the new result set
for you.

## What You Need

To use Live correctly, wire up all four pieces:

- A base Durable Stream containing State Protocol change records.
- A `state-protocol` profile with `touch.enabled=true`.
- Application code that computes the table, template, and fine keys for each
  query.
- A wait loop that carries a cursor forward and re-runs the real query on
  `touched` or `stale`.

Important design choices:

- State Protocol is the only canonical input for touch generation.
- Postgres- or database-specific decoding stays outside Durable Streams.
- Deployment model is single app / single tenant per Durable Streams server
  process.
- Live queries use a single in-memory touch journal. There is no sqlite touch
  storage mode anymore.

## End-to-End Flow

This section is the recommended integration pattern.

### 1) Create the base stream and set the state-protocol profile

The touch APIs only exist when the stream profile is `state-protocol` and
`touch.enabled=true`.

```bash
curl -X PUT http://127.0.0.1:8080/v1/stream/app.wal \
  -H 'content-type: application/json'

curl -X POST http://127.0.0.1:8080/v1/stream/app.wal/_profile \
  -H 'content-type: application/json' \
  -d '{
    "apiVersion": "durable.streams/profile/v1",
    "profile": {
      "kind": "state-protocol",
      "touch": {
        "enabled": true,
        "onMissingBefore": "coarse"
      }
    }
  }'
```

If `touch` is not enabled, all `/touch/*` routes return `404`.

### 2) Append State Protocol change records

Touch generation consumes State Protocol records from the base stream.

```json
{
  "type": "public.todos",
  "key": "1",
  "value": { "id": "1", "tenantId": "t1", "status": "open" },
  "old_value": { "id": "1", "tenantId": "t1", "status": "done" },
  "headers": {
    "operation": "update",
    "txid": "2057",
    "timestamp": "2026-03-23T10:30:00Z"
  }
}
```

Rules:

1. `headers.operation` must be `insert|update|delete`.
2. `type` must be non-empty.
3. `key` must be non-empty.
4. `value` should exist for `insert|update`.
5. `old_value` is optional but strongly recommended for precise invalidation on
   updates.
6. Control messages are ignored for touch derivation.

### 3) Decide whether the query is coarse-only or template-eligible

Every live query should at least know its root entity and table key.

If the query also has a small equality predicate shape, it can use fine waits.
The intended eligible shape is:

- one root entity
- up to 3 equality predicate fields

Examples:

- `SELECT * FROM public.todos WHERE tenantId = $1`
- `SELECT * FROM public.todos WHERE tenantId = $1 AND status = $2`
- `SELECT count(*) FROM public.todos WHERE tenantId = $1 AND userId = $2 AND status = $3`

Queries with joins, ranges, arbitrary expressions, or larger predicate sets can
still use Live, but they should usually wait coarsely on the table key.

For a query-family matrix that maps SQL shapes to exact vs coarse invalidation,
see [live-query-invalidation.md](./live-query-invalidation.md).

### 4) Activate templates before relying on fine invalidation

Fine invalidation depends on active templates. Activation is idempotent, but it
is not retroactive.

That means:

- if a change happened before a template was activated, Live will not backfill a
  fine touch for that earlier change
- if you want fine waits for a query shape, activate the template before
  expecting watch-key wakes

```bash
curl -X POST http://127.0.0.1:8080/v1/stream/app.wal/touch/templates/activate \
  -H 'content-type: application/json' \
  -d '{
    "templates": [
      {
        "entity": "public.todos",
        "fields": [
          { "name": "tenantId", "encoding": "string" },
          { "name": "status", "encoding": "string" }
        ]
      }
    ],
    "inactivityTtlMs": 3600000
  }'
```

Response shape:

- `activated[]`: activated or refreshed templates
- `denied[]`: rate-limit or cap denials
- `limits`: current template caps

Each activated record includes:

- `templateId`
- `state`
- `activeFromTouchOffset`

`activeFromTouchOffset` marks the journal cursor from which that template can be
expected to produce template/watch invalidations.

### 5) Capture a cursor, run the query, compute the wait keyset

The safe subscription pattern is:

1. Activate needed templates.
2. Capture the current cursor with `/touch/meta` or reuse the cursor returned by
   the previous wait.
3. Run the real query against your database.
4. Compute the wait keyset for that query shape.
5. Call `/touch/wait` using the cursor captured before the query.

This avoids a race where the database changes between the query and the wait.
If that happens, the subsequent wait will wake immediately because it still
covers changes since the earlier cursor.

```bash
curl -sS http://127.0.0.1:8080/v1/stream/app.wal/touch/meta
```

The response includes a `cursor` such as:

```json
{
  "cursor": "9f3a8d6c5a7b1234:12346",
  "settled": true,
  "touchMode": "fine",
  "lagSourceOffsets": 0,
  "activeTemplates": 3,
  "activeWaiters": 12
}
```

If you need the strongest practical protection against conservative reruns from
the current flush/lag race, call:

```bash
curl -sS "http://127.0.0.1:8080/v1/stream/app.wal/touch/meta?settle=flush&timeoutMs=30000"
```

That waits until:

- the touch processor has no current lag for the stream
- the current pending journal bucket is flushed

and then returns a `settled: true` cursor.

### 6) Wait, then repeat

Call `/touch/wait` with the cursor and keyset for the query you just ran.

On `touched=true`, re-run the query and continue.

On `touched=false`, issue the next wait using the returned cursor.

On `stale=true`, re-run the query from scratch and restart from the returned
cursor.

## Query Shapes, Keys, and Template IDs

Live does not parse SQL for you. Your application decides which entity and
predicate shape represent the query.

### Terminology

#### Base stream

A Durable Stream that stores State Protocol changes, for example `app.wal`.

#### Entity

The State Protocol `type` string, for example `public.posts`.

#### Touch APIs

For a base stream `<stream>`:

- `GET /v1/stream/<stream>/touch/meta`
- `POST /v1/stream/<stream>/touch/wait`
- `POST /v1/stream/<stream>/touch/templates/activate`

### 64-bit routing keys (`keys`)

The canonical API-level routing keys are 64-bit xxh3 hashes encoded as 16
lowercase hex chars.

- Table key:
  - `tableKey(entity) = XXH3_64("tbl\0" + entity)`
- Template id:
  - `templateId(entity, fieldsSorted) = XXH3_64("tpl\0" + entity + "\0" + join(fieldsSorted, "\0"))`
- Template key:
  - `templateKey(templateId) = XXH3_64("tpl\0" + templateIdBytesBE)`
- Membership key:
  - `membershipKey(templateId, args...) = XXH3_64("mem\0" + templateIdBytesBE + "\0" + arg1 + ... )`
- Projected field key:
  - `projectedFieldKey(templateId, fieldName, args...) = XXH3_64("fld\0" + templateIdBytesBE + "\0" + fieldName + "\0" + arg1 + ... )`
- Watch key:
  - `watchKey(templateId, args...) = XXH3_64("key\0" + templateIdBytesBE + "\0" + arg1 + ... )`

Important:

- template key is not the same value as template id
- field names must be sorted before computing the template id
- watch arguments must be encoded in that same sorted-field order

The reference implementation for these calculations lives in
`src/touch/live_keys.ts`.

### 32-bit key IDs (`keyIds`)

The touch journal uses uint32 key IDs on hot paths.

- If the key string is hex16, the key ID is the low 32 bits of that 64-bit key.
- Otherwise the key ID falls back to `xxh32(key)`.

`/touch/wait` accepts `keys`, `keyIds`, or both.

Use `keyIds` on hot paths if you already have them. Otherwise sending `keys` is
fine and the server will derive the IDs.

### Template field encodings

Supported encodings:

- `string`
- `int64`
- `bool`
- `datetime`
- `bytes`

Field lists are canonicalized by field name sort.

### Example: computing the live subscription for a query

For:

```sql
SELECT id, title
FROM public.todos
WHERE tenantId = 't1' AND status = 'open'
ORDER BY id;
```

The live subscription shape is:

- entity: `public.todos`
- fields(sorted): `["status", "tenantId"]`
- args(sorted order): `["open", "t1"]`

Example in TypeScript using the repo's reference helpers:

```ts
import { encodeTemplateArg, membershipKeyFor, projectedFieldKeyFor, tableKeyFor, templateIdFor, watchKeyFor } from "./src/touch/live_keys";

const entity = "public.todos";
const fields = ["tenantId", "status"].sort();
const templateId = templateIdFor(entity, fields);
const tableKey = tableKeyFor(entity);
const membershipKey = membershipKeyFor(templateId, [
  encodeTemplateArg("open", "string")!,
  encodeTemplateArg("t1", "string")!,
]);
const watchKey = watchKeyFor(templateId, [
  encodeTemplateArg("open", "string")!,
  encodeTemplateArg("t1", "string")!,
]);
const projectedTitleKey = projectedFieldKeyFor(templateId, "title", [
  encodeTemplateArg("open", "string")!,
  encodeTemplateArg("t1", "string")!,
]);
```

For this query:

- use `tableKey` for coarse waits
- use `membershipKey` plus `templateIdsUsed: [templateId]` when the result only
  depends on which rows match the predicate, for example `count(*)`,
  `exists(...)`, or a stable row-key set; singleton lookups such as
  `select id from posts where id=?` are also in this membership-only class
- use `membershipKey` plus one `projectedFieldKey` per projected scalar field,
  plus `templateIdsUsed: [templateId]`, when the result depends only on row
  membership plus those fields, for example this `select id, title ... order by
  id` query or `select author from posts where id=?`
- use `watchKey` plus `templateIdsUsed: [templateId]` when any row-content
  change inside the watched equality tuple should rerun the SQL

## Templates

Templates are runtime state stored in SQLite in the `live_templates` table.

They exist so the server knows which fine-grained predicate shapes are worth
tracking right now.

### Activation

`POST /v1/stream/<stream>/touch/templates/activate`

Properties:

- idempotent
- supports per-call `inactivityTtlMs`
- accepts up to 256 templates per request
- each template must have 1 to 3 fields

### Heartbeat and retirement

`templateIdsUsed` in `/touch/wait` does two jobs:

- updates template `last_seen_at_ms`
- tells the server which template shapes the current query depends on

Background GC retires inactive templates.

Defaults:

- `defaultInactivityTtlMs=3600000`
- `gcIntervalMs=60000`

### Limits

- `maxActiveTemplatesPerEntity=256`
- `maxActiveTemplatesPerStream=2048`
- `activationRateLimitPerMinute=100`

### Inline declaration during wait

`/touch/wait` also supports:

- `declareTemplates`
- `inactivityTtlMs`

This is useful when query shapes are discovered lazily and you do not want a
separate activation round-trip.

Important:

- inline declaration activates templates, but it does not replace
  `templateIdsUsed`
- if the same wait should also heartbeat the template and allow runtime fallback
  behavior, still send `templateIdsUsed`
- activation is still not retroactive

## The Touch Journal

Live uses a per-stream in-memory journal.

Properties:

- touches are not appended as separate sqlite WAL rows
- touches are not uploaded to object storage
- `/v1/stream/<stream>/touch` read endpoints do not exist

Implementation details:

- time-aware bloom filter
- recent exact-key history for small fine waits
- bucketed commit generations
- per-key waiter index for small keysets
- exact waiter index for small literal fine keysets
- broad-waiter scan path for large keysets
- global deadline heap with a single timeout timer

Overflow behavior:

- if a bucket exceeds its unique-key capacity, that bucket is marked overflow
- the journal wakes all waiters for safety
- this may cause extra re-runs, but it must not cause missed invalidations

Cursor semantics:

- cursor format is `<epochHex16>:<generation>`
- epoch mismatch means `stale=true`
- `cursor: "now"` means "start waiting from the journal generation visible at
  request time"

Flush cadence:

- `bucketMs` is the hard upper bound
- effective coalescing is adaptive under load

## `/touch/meta`

`GET /v1/stream/<stream>/touch/meta`

Use this endpoint to:

- seed the first cursor for a wait loop
- optionally wait for a settled cursor with `?settle=flush`
- inspect current runtime mode and lag
- debug hot-interest behavior
- confirm that touch is enabled for a stream

The response is flat rather than nested.

Commonly useful fields:

- `cursor`, `epoch`, `generation`
- `settled`
- `bucketMs`, `coalesceMs`
- `pendingKeys`, `overflowBuckets`, `activeWaiters`
- `activeTemplates`, `touchMode`, `lagSourceOffsets`
- `bucketMaxSourceOffsetSeq`
- `hotFineKeys`, `hotTemplates`
- `fineWaitersActive`, `coarseWaitersActive`, `broadFineWaitersActive`

Monotonic totals are also included for scan, touch, wait, and journal
activity.

Query params:

- `settle`
  - optional
  - `flush`
  - when present, `/touch/meta` waits until `pendingKeys=0` and
    `lagSourceOffsets=0`, or until `timeoutMs` expires
- `timeoutMs`
  - optional
  - range `0..120000`
  - default `30000`

`settled=true` means the returned cursor is not behind the current pending
journal bucket or touch-processor lag at response time.

Important:

- `settle=flush` is the best available practical barrier inside Live itself
- it eliminates the current internal "query saw rows from an unflushed bucket"
  race
- it does not turn Live into full cross-system snapshot coordination

If `/touch/meta` returns `404`, touch is not enabled for that stream.

## `/touch/wait`

`POST /v1/stream/<stream>/touch/wait`

This is the core long-poll endpoint.

### Request fields

- `cursor`
  - required
  - either `<epoch>:<generation>` or `"now"`
- `timeoutMs`
  - optional
  - range `0..120000`
  - defaults to `30000`
- `keys`
  - optional string array
  - max `1024`
- `keyIds`
  - optional uint32 array
  - max `1024`
- `interestMode`
  - optional
  - `fine|coarse`
  - defaults to `fine`
- `exact`
  - optional boolean
  - only valid for small literal fine-key waits
  - asks the server to use the exact small-key path instead of only the lossy
    bloom/uint32 matching path
- `templateIdsUsed`
  - optional string array
  - heartbeats active templates and enables runtime fallback logic
- `declareTemplates`
  - optional inline activation payload
- `inactivityTtlMs`
  - optional TTL used with `declareTemplates`

Validation notes:

- at least one of `keys` or `keyIds` is required
- `templateIdsUsed` alone is not enough
- if you send coarse waits without `templateIdsUsed`, your provided `keys` or
  `keyIds` should already be table-level

### Small exact fine-key lane

When all of these are true:

- `interestMode="fine"`
- `exact=true`
- the effective wait kind stays `fineKey`
- you send literal 64-bit routing `keys`, not only `keyIds`
- the keyset is small (currently up to `16` keys)

the server uses an exact small-key path in addition to the normal journal
machinery:

- active waits are matched by full 64-bit routing key, not only uint32 key id
- recent flushed generations are checked against exact routing keys before the
  bloom-style `maybeTouchedSince*` path

This materially reduces false-positive wakes for expensive small keysets, but it
is still not a generic SQL diff engine:

- it only helps while the query can be represented as a small exact fine keyset
- exact recent coverage is bounded to a short recent window around the current
  cursor range
- overflow, degraded runtime modes, missing before-images, or coarse/template
  fallbacks still lose exactness

### Fine vs coarse waits

Use `interestMode="fine"` when you have:

- one or more fine keys
- the template ids used by the query

Use `interestMode="coarse"` when:

- the query shape is not template-eligible
- you deliberately want table-level invalidation only

In coarse mode, if `templateIdsUsed` is present, the server can derive table
keys from those templates. Otherwise it waits on your provided keyset as-is.

### Example: fine wait

```json
{
  "cursor": "9f3a8d6c5a7b1234:12346",
  "timeoutMs": 30000,
  "keys": ["af15ef62f0e1559a"],
  "templateIdsUsed": ["40697bf6e8a33480"]
}
```

This pattern assumes your change adapter can provide the `old_value` data needed
for precise update invalidation. If before-images are not guaranteed, prefer
coarse waits for correctness.

### Example: coarse wait

```json
{
  "cursor": "9f3a8d6c5a7b1234:12346",
  "timeoutMs": 30000,
  "interestMode": "coarse",
  "keys": ["ff6151d2f51e7ee0"]
}
```

### Server-side effective wait key translation

The server chooses a primary effective wait key kind for each request:

- `fineKey`
- `templateKey`
- `tableKey`

Selection rules:

- `interestMode=coarse` => `tableKey`
- `touchMode=restricted` with `templateIdsUsed` => `templateKey`
- `touchMode=coarseOnly` with `templateIdsUsed` => `tableKey`
- otherwise => `fineKey`

Resilience rule:

- when `interestMode=fine` and `templateIdsUsed` is present, the server also
  registers template-key fallbacks even if the primary wait kind is `fineKey`
- this prevents a long-poll that started in fine mode from starving if the
  runtime degrades to `restricted` before the next re-issue

Treat `effectiveWaitKind` as a diagnostic field. Do not hardcode application
behavior around a specific value.

### Responses

Touched:

```json
{
  "touched": true,
  "cursor": "9f3a8d6c5a7b1234:12346",
  "effectiveWaitKind": "templateKey",
  "bucketMaxSourceOffsetSeq": "568399",
  "flushAtMs": 1739481234567,
  "bucketStartMs": 1739481234468
}
```

Timeout:

```json
{
  "touched": false,
  "cursor": "9f3a8d6c5a7b1234:12346",
  "effectiveWaitKind": "templateKey",
  "bucketMaxSourceOffsetSeq": "568399",
  "flushAtMs": 1739481234567,
  "bucketStartMs": 1739481234468
}
```

Stale:

```json
{
  "stale": true,
  "cursor": "9f3a8d6c5a7b1234:12346",
  "epoch": "9f3a8d6c5a7b1234",
  "generation": 12346,
  "effectiveWaitKind": "tableKey",
  "bucketMaxSourceOffsetSeq": "568399",
  "flushAtMs": 1739481234567,
  "bucketStartMs": 1739481234468,
  "error": {
    "code": "stale",
    "message": "cursor epoch mismatch; rerun/re-subscribe and start from cursor"
  }
}
```

Client rules:

- `touched=true`: re-run the query
- `touched=false`: keep the current result and issue the next wait
- `stale=true`: re-run the query and restart the loop from the returned cursor

## Safe Wait-Loop Pattern

This is the recommended client loop.

```ts
let cursor = (await getTouchMeta()).cursor;

for (;;) {
  const queryResult = await runRealDatabaseQuery();
  const subscription = computeLiveSubscription(queryResult.queryShape);

  const res = await waitForTouch({
    cursor,
    keys: subscription.keys,
    keyIds: subscription.keyIds,
    templateIdsUsed: subscription.templateIdsUsed,
    interestMode: subscription.interestMode,
  });

  cursor = res.cursor ?? cursor;

  if (res.stale) {
    continue;
  }
  if (res.touched) {
    continue;
  }
}
```

Guidance:

- for exact-result-sensitive paths, prefer a settled cursor from
  `/touch/meta?settle=flush`
- for small exact fine keysets, first issue a zero-timeout fine wait on the
  exact `keys` to keep them hot through the query, then capture the settled
  cursor and run the query
- otherwise, capture the cursor before running the query
- compute the keyset for the query you actually ran
- use the returned cursor for the next iteration
- if the query shape changes, activate or declare the new template shape before
  relying on fine waits

Example keyed barrier pattern:

```ts
const subscription = computeLiveSubscription();

await waitForTouch({
  cursor: "now",
  exact: true,
  keys: subscription.keys,
  interestMode: "fine",
  timeoutMs: 0,
});

let cursor = (await getTouchMeta({ settle: "flush" })).cursor;
const queryResult = await runRealDatabaseQuery();
```

## Runtime Modes and Load Behavior

Per stream, runtime `touchMode` is one of:

- `idle`
- `fine`
- `restricted`
- `coarseOnly`

Coarse lane:

- table touches are always emitted per change, coalesced by
  `coarseIntervalMs`

Fine lane:

- controlled by lag guardrails, budgets, and hot-interest filtering
- when degraded, `restricted` emits template keys instead of fine watch keys
- `membershipKey` and `projectedFieldKey` are fine keys like `watchKey`; under
  degradation they also fall back through `templateKey`

Guardrail defaults:

- `lagDegradeFineTouchesAtSourceOffsets=5000`
- `lagRecoverFineTouchesAtSourceOffsets=1000`
- `fineTouchBudgetPerBatch=2000`
- `fineTokensPerSecond=200000`
- `fineBurstTokens=400000`
- `lagReservedFineTouchBudgetPerBatch=200`

Design implication:

- do not assume the runtime will always stay in fine mode
- always send `templateIdsUsed` with fine waits so the runtime can degrade
  without breaking wakeups

## Correctness and Precision

For updates that move rows across partitions, precise fine invalidation needs
before-images.

`touch.onMissingBefore`:

- `coarse` (default): suppress fine invalidation when `old_value` is missing,
  but still emit coarse table touches
- `skipBefore`: best-effort after-only fine invalidation
- `error`: treat missing or insufficient before-images as a state-protocol
  processing error

Guidance:

- treat touches as invalidation hints only
- always re-run the real query after `touched`
- exact projected-field invalidation needs usable before-images on updates; with
  `skipBefore`, projected field touches become after-only best effort
- provide `old_value` for updates whenever possible
- fine waits are only fully correct when the adapter can supply the before
  image fields needed to derive the right fine keys
- if before-images are not guaranteed, prefer coarse waits for correctness
- if strict precision matters, use `onMissingBefore="error"`

## Operational Guidance

Use these defaults unless you have a measured reason not to.

- Keep keysets small. Large keysets become broad waits and are more expensive.
- Prefer `keys` for small exact-result-sensitive fine waits. Use `keyIds` only
  on hot paths where the compact uint32 form matters more than exact small-key
  matching.
- Activate templates opportunistically and keep heartbeating them through
  `templateIdsUsed`.
- Use coarse waits for query shapes that are not clean equality templates.
- Watch `/touch/meta` for `lagSourceOffsets`, `touchMode`, `overflowBuckets`,
  and waiter counts when debugging behavior under load.

Primary diagnostics are in `/touch/meta`.

`live.metrics` (`/v1/stream/live.metrics`) is best-effort and can be noisy under
overload.

## Unsupported and Removed Behavior

The current Live system does not support:

- sqlite touch storage
- `touch.storage`
- `touch.derivedStream`
- `touch.retention`
- `/v1/stream/<stream>/touch` read endpoints
- `/v1/stream/<stream>/touch/pk/...` read endpoints
- `sinceTouchOffset`
- touch companion streams

Use:

- the in-memory journal
- `/touch/meta`
- `/touch/templates/activate`
- `/touch/wait`
- `cursor`

## Config Reference

Example state-protocol profile config:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "state-protocol",
    "touch": {
      "enabled": true,
      "coarseIntervalMs": 100,
      "touchCoalesceWindowMs": 100,
      "lagDegradeFineTouchesAtSourceOffsets": 5000,
      "lagRecoverFineTouchesAtSourceOffsets": 1000,
      "fineTouchBudgetPerBatch": 2000,
      "fineTokensPerSecond": 200000,
      "fineBurstTokens": 400000,
      "lagReservedFineTouchBudgetPerBatch": 200,
      "onMissingBefore": "coarse",
      "memory": {
        "bucketMs": 100,
        "filterPow2": 22,
        "k": 4,
        "pendingMaxKeys": 100000,
        "keyIndexMaxKeys": 32,
        "hotKeyTtlMs": 10000,
        "hotTemplateTtlMs": 10000,
        "hotMaxKeys": 1000000,
        "hotMaxTemplates": 4096
      },
      "templates": {
        "defaultInactivityTtlMs": 3600000,
        "lastSeenPersistIntervalMs": 300000,
        "gcIntervalMs": 60000,
        "maxActiveTemplatesPerEntity": 256,
        "maxActiveTemplatesPerStream": 2048,
        "activationRateLimitPerMinute": 100
      }
    }
  }
}
```

Notes:

- State Protocol is the only supported live/touch input format.
- Cursor staleness is epoch-based.
- The profile is applied by `POST /v1/stream/<stream>/_profile`.

## Postgres Adapter Guidance

Durable Streams stays Postgres-agnostic. Adapters map logical decoding output to
State Protocol.

Recommended baseline for precise live invalidation:

- stable primary key for `key`
- before-images for updates that can move across query partitions
- commit order preserved when appending to the base stream
- transaction id and timestamp when available
