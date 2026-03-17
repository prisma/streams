# LIVE: Live Queries V2 (State Protocol, Dynamic Templates, Memory Touch Journal)

Live Queries V2 adds a DX-first invalidation primitive to Durable Streams.

The intended flow is:

1. Run a query to produce initial results.
2. Wait on a small keyset via `/touch/wait`.
3. Re-run when touched.

This is invalidation, not incremental query maintenance.

## Key decisions

- State Protocol is the only canonical input for touch generation.
- Postgres-specific decoding stays outside Durable Streams (adapter maps to State Protocol).
- Deployment model is single-app/single-tenant per DS server process.
- Default touch storage is memory-only (`interpreter.touch.storage="memory"`).
- Legacy sqlite touch storage still exists (`storage="sqlite"`).

## 1) Terminology

### Base stream

A Durable Stream that stores State Protocol changes (for example `ppg.wal`).

### Touch APIs

For a base stream `<stream>`:

- `GET /v1/stream/<stream>/touch/meta`
- `POST /v1/stream/<stream>/touch/wait`
- `POST /v1/stream/<stream>/touch/templates/activate`

These endpoints are available only when the stream's interpreter has
`touch.enabled=true`. Otherwise the server returns `404` (`touch not enabled`).

In sqlite touch mode only, read endpoints also exist on the derived touch stream (`/touch`, `/touch/pk/...`).

### Entity

The State Protocol `type` string (for example `public.posts`).

## 2) State Protocol input

Touch generation interprets State Protocol change records.

Example:

```json
{
  "type": "public.todos",
  "key": "1",
  "value": { "id": "1", "tenantId": "t1", "status": "open" },
  "oldValue": { "id": "1", "tenantId": "t1", "status": "done" },
  "headers": {
    "operation": "update",
    "txid": "2057",
    "timestamp": "2025-12-23T10:30:00Z"
  }
}
```

Rules:

1. `headers.operation` must be `insert|update|delete`.
2. `type` must be non-empty.
3. `key` must be non-empty.
4. `value` should exist for `insert|update`.
5. `oldValue` is optional but strongly recommended for precise invalidation on updates.
6. Control messages are ignored for touch derivation.

Compatibility: both `oldValue` and `old_value` are accepted.

## 3) Keys and IDs

### 3.1 64-bit routing keys (hex16)

The canonical API-level routing keys are 64-bit xxh3 hashes encoded as 16 lowercase hex chars.

- Table key:
  - `tableKey(entity) = XXH3_64("tbl\0" + entity)`
- Template id:
  - `templateId(entity, fieldsSorted) = XXH3_64("tpl\0" + entity + "\0" + join(fieldsSorted, "\0"))`
- Template key (used in restricted mode):
  - `templateKey(templateId) = XXH3_64("tpl\0" + templateIdBytesBE)`
- Watch key (fine key):
  - `watchKey(templateId, args...) = XXH3_64("key\0" + templateIdBytesBE + "\0" + arg1 + ... )`

Important: template key is not the same value as template id.

### 3.2 32-bit key IDs (`keyIds`)

Memory mode uses uint32 key IDs on hot paths.

- If a key string is hex16, `keyId` is the low 32 bits of that 64-bit key.
- Otherwise, key string falls back to xxh32(key).

`/touch/wait` can accept either `keys` (strings), `keyIds` (uint32), or both.

### 3.3 Template field encodings

Supported encodings:

- `string`
- `int64`
- `bool` (`0|1`)
- `datetime` (normalized RFC3339 UTC)
- `bytes`

Template fields are canonicalized by field name sort.

## 4) Dynamic templates

Templates are runtime state stored in SQLite (`live_templates`).

### Activation

`POST /v1/stream/<stream>/touch/templates/activate`

- Idempotent.
- Supports explicit `inactivityTtlMs`.
- Returns `activated[]`, `denied[]`, and limits.
- `activated[]` includes `templateId`, `state`, `activeFromTouchOffset`.

### Heartbeat and retirement

- `templateIdsUsed` in `/touch/wait` updates `last_seen_at_ms`.
- Background GC retires inactive templates.
- Defaults:
  - `defaultInactivityTtlMs=3600000`
  - `gcIntervalMs=60000`

### Caps

- `maxActiveTemplatesPerEntity=256`
- `maxActiveTemplatesPerStream=2048`
- `activationRateLimitPerMinute=100`

## 5) Touch storage modes

### 5.1 Memory mode (default)

Properties:

- Touches are not appended to sqlite WAL as rows.
- Touches are not uploaded to object store.
- `/v1/stream/<stream>/touch` read endpoints are unavailable (404).

Implementation:

- Per-stream memory journal (`TouchJournal`) with:
  - time-aware bloom filter (`lastSet[pos]=generation`)
  - bucketed commit generations
  - per-key waiter index for small keysets
  - broad-waiter scan path for large keysets
  - global deadline heap + single timeout timer
- Overflow behavior when per-bucket unique key capacity is exceeded:
  - bucket marked overflow
  - wake-all for safety (lossy but no false negatives)

Cursor semantics:

- Cursor format: `<epochHex16>:<generation>`
- Epoch mismatch => `stale=true` (client reruns and re-subscribes)

Flush cadence:

- `bucketMs` is the hard upper bound.
- Effective coalescing (`coalesceMs`) is adaptive in manager:
  - lag `0` with active waiters: 10ms
  - lag `1..5000`: 50ms
  - lag `>5000`: `bucketMs` (default 100ms)

### 5.2 SQLite mode (legacy)

- Touch rows are persisted in derived stream `<stream>.__touch`.
- Retention and stale-cursor behavior are offset/retention based.
- `/touch` and `/touch/pk/...` read endpoints are supported.

## 6) Runtime modes and load behavior

Per stream, runtime `touchMode` is one of:

- `idle`
- `fine`
- `restricted`
- `coarseOnly`

Coarse lane:

- Table touches are always emitted per change (coalesced by `coarseIntervalMs`).

Fine lane:

- Controlled by lag guardrails + budgets + hot-interest filtering.
- In degraded mode, `restricted` emits template keys instead of fine watch keys.

Guardrails and budgets (defaults):

- `lagDegradeFineTouchesAtSourceOffsets=5000`
- `lagRecoverFineTouchesAtSourceOffsets=1000`
- `fineTouchBudgetPerBatch=2000`
- `fineTokensPerSecond=200000`
- `fineBurstTokens=400000`
- `lagReservedFineTouchBudgetPerBatch=200`

## 7) `/touch/meta`

### 7.1 Memory mode response

Current response is flat (not nested), combining journal + runtime counters.

Core fields:

- `mode`, `cursor`, `epoch`, `generation`
- `bucketMs`, `coalesceMs`, `filterSize`, `k`
- `pendingKeys`, `overflowBuckets`, `activeWaiters`
- `bucketMaxSourceOffsetSeq`
- `lastFlushAtMs`, `flushIntervalMsMaxLast10s`, `flushIntervalMsP95Last10s`
- `coarseIntervalMs`, `touchCoalesceWindowMs`, `touchRetentionMs` (null in memory mode)
- `activeTemplates`, `touchMode`, `lagSourceOffsets`
- hot-interest state:
  - `hotFineKeys`, `hotTemplates`
  - `hotFineKeysActive`, `hotFineKeysGrace`
  - `hotTemplatesActive`, `hotTemplatesGrace`
  - `fineWaitersActive`, `coarseWaitersActive`, `broadFineWaitersActive`
- monotonic totals for scan/touch/wait/journal activity:
  - `scanRowsTotal`, `scanBatchesTotal`, `scannedButEmitted0BatchesTotal`
  - `interpretedThroughDeltaTotal`
  - `touchesEmittedTotal`, `touchesTableTotal`, `touchesTemplateTotal`
  - `fineTouchesDroppedDueToBudgetTotal`
  - `fineTouchesSkippedColdTemplateTotal`, `fineTouchesSkippedColdKeyTotal`, `fineTouchesSkippedTemplateBucketTotal`
  - `waitTouchedTotal`, `waitTimeoutTotal`, `waitStaleTotal`
  - `journalFlushesTotal`, `journalNotifyWakeupsTotal`, `journalNotifyWakeMsTotal`, `journalNotifyWakeMsMax`
  - `journalTimeoutsFiredTotal`, `journalTimeoutSweepMsTotal`

### 7.2 SQLite mode response

- `mode="sqlite"`
- `currentTouchOffset`
- `oldestAvailableTouchOffset`
- `coarseIntervalMs`, `touchCoalesceWindowMs`, `touchRetentionMs`
- `activeTemplates`
- retained touch WAL stats

## 8) `/touch/wait`

`POST /v1/stream/<stream>/touch/wait`

### Request (memory mode)

Supported inputs:

- `cursor` (or legacy `sinceTouchOffset`), value is `<epoch>:<generation>` or `"now"`
- `timeoutMs` (0..120000)
- `keys` (string[], max 1024)
- `keyIds` (uint32[], max 1024)
- `interestMode`: `fine|coarse` (defaults to `fine`)
- `templateIdsUsed` (heartbeat + fine hot-interest)
- optional inline declaration:
  - `declareTemplates`
  - `inactivityTtlMs`

Validation notes:

- In memory mode, at least one of `keys` or `keyIds` is required.
- In sqlite mode, `keys` is required.

### Server-side effective wait key translation

For memory mode, DS chooses a primary effective wait key kind per request:

- If `interestMode=coarse` => `effectiveWaitKind="tableKey"`.
  - When `templateIdsUsed` is present, DS resolves entities and waits on derived table key IDs.
  - Without `templateIdsUsed`, DS cannot derive entities and uses caller-provided keys/keyIds.
- Else if `touchMode=restricted` and `templateIdsUsed` present => `templateKey`
- Else if `touchMode=coarseOnly` and `templateIdsUsed` present => `tableKey`
- Else => `fineKey`

Resilience rule (fine waits):

- If `interestMode=fine` and `templateIdsUsed` is present, DS also registers template-key fallback IDs in the same wait.
- This keeps a long-poll started in fine mode from starving if DS degrades to `restricted` before that waiter re-issues.
- Response `effectiveWaitKind` still reports the primary mode-selected kind (`fineKey|templateKey|tableKey`).

Response includes:

- `effectiveWaitKind`: `fineKey|templateKey|tableKey`

### Response (memory mode)

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

### SQLite-mode wait behavior

- Uses `sinceTouchOffset` semantics.
- Returns retention staleness based on `oldestAvailableTouchOffset`.

## 9) Hot-interest tracking details

Memory mode uses active waiter accounting with grace windows.

- Fine waits increment/decrement active counters for keys/templates.
- Coarse waits do not contribute to fine hotness.
- Large keysets (`>64`) are tracked as broad fine waiters (no per-key interest registration).
- On wait completion/abort, active counts are released and grace expiries are set.
- Grace windows default to:
  - `hotKeyTtlMs=10000`
  - `hotTemplateTtlMs=10000`

This is what enables watch-driven fine filtering without deadlocks.

## 10) Correctness and `oldValue`

For updates that move rows across partitions, precise fine invalidation requires before-images.

`interpreter.touch.onMissingBefore`:

- `coarse` (default): skip fine for missing before, coarse still guarantees correctness.
- `skipBefore`: best-effort after-only fine.
- `error`: interpreter batch errors on missing/insufficient before.

Guidance:

- Treat touches as invalidation hints; rerun on touched.
- If strict precision is required, use `onMissingBefore="error"` and guarantee adapter-provided `oldValue`.

## 11) AppKit contract

Backend/compute should:

1. Extract an eligible template shape (root entity, up to 3 equality predicates).
2. Compute table key + fine watch key(s) when eligible.
3. Activate template opportunistically.
4. Run wait loop with:
   - `interestMode="fine"`
   - `templateIdsUsed`
   - `keyIds` preferred on hot paths
5. On `touched`, rerun query.
6. On `stale`, rerun and restart from returned cursor.

Important:

- AppKit should not guess server mode.
- Express fine intent and rely on `effectiveWaitKind` diagnostics.

## 12) Observability

Primary diagnostics: `/touch/meta` monotonic counters.

`live.metrics` stream (`/v1/stream/live.metrics`) is best-effort and may lag/noise under overload.

## 12.1) Resilience hardening

Touch interpreter worker pool includes bounded self-heal for rare stuck-reader states:

- Trigger: repeated zero-row interpreter batches while WAL backlog still exists for that stream.
- Action: schedule a bounded/rate-limited worker-pool restart.
- Safety: this is a liveness mechanism; it may cause extra invalidations but must not cause missed invalidations.

## 13) Config reference

Example interpreter config:

```json
{
  "interpreter": {
    "apiVersion": "durable.streams/stream-interpreter/v1",
    "format": "durable.streams/state-protocol/v1",
    "touch": {
      "enabled": true,
      "storage": "memory",
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

- State Protocol is the only supported interpreter format.
- `derivedStream` and touch retention are relevant to sqlite mode.
- Memory mode has epoch-based staleness; sqlite mode has retention-based stale offsets.

## 14) Postgres adapter guidance

Durable Streams stays Postgres-agnostic; adapters map logical decoding to State Protocol.

Recommended baseline for precision:

- stable PK for `key`
- `REPLICA IDENTITY FULL` on tables where before-images matter
- wal2json flags: include schema/pk/txid/timestamp
