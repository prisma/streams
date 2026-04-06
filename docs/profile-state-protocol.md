# State-Protocol Profile

`state-protocol` is the built-in profile for JSON streams that carry State
Protocol change records.

It owns the live `/touch/*` surface and the touch-processing semantics used for
invalidations.

## Contract

`state-protocol` means:

- stream content type must be `application/json`
- records are interpreted as State Protocol change records
- touch configuration lives in the profile document
- `/touch/*` exists only when `touch.enabled=true`

## Profile Document

Example:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "state-protocol",
    "touch": {
      "enabled": true,
      "onMissingBefore": "coarse"
    }
  }
}
```

The `touch` object configures coalescing, lag degradation, template activation,
and related live invalidation behavior.

## Owned Behavior

`state-protocol` owns:

- profile validation for the touch config
- canonical change derivation for touch processing
- `/touch/meta`
- `/touch/wait`
- `/touch/templates/activate`
- seeding and cleanup of `stream_touch_state`

The core engine only provides the shared plumbing:

- durable stream storage
- worker execution
- journals
- template registry
- notifier integration

## Schema Relationship

Schemas are still optional on `state-protocol` streams.

If present, they validate the JSON payload shape and may still define
routing-key extraction. They do not own:

- touch configuration
- live invalidation behavior
- State Protocol semantics

## WAL Search Schema (Prisma-Style Events)

When `state-protocol` is used for WAL change events like:

```json
{
  "type": "public.posts",
  "key": "42",
  "value": { "id": 42, "title": "Hello" },
  "oldValue": null,
  "headers": {
    "operation": "insert",
    "timestamp": "2026-03-16T12:00:00.000Z"
  }
}
```

install schema `search.fields` explicitly if you need efficient event lookup by
table and row key. The `state-protocol` profile does not auto-install WAL
search fields.

Recommended setup at bootstrap time (before ingesting stream data):

- install this as the first schema while the stream is still empty
- for existing streams, follow the schema evolution rules in
  [`schemas.md`](./schemas.md)

```json
{
  "apiVersion": "durable.streams/schema-registry/v1",
  "schema": {
    "type": "object",
    "required": ["type", "key", "headers"],
    "properties": {
      "type": { "type": "string" },
      "key": { "type": "string" },
      "headers": {
        "type": "object",
        "required": ["timestamp", "operation"],
        "properties": {
          "timestamp": { "type": "string", "format": "date-time" },
          "operation": { "type": "string" }
        }
      }
    },
    "additionalProperties": true
  },
  "search": {
    "primaryTimestampField": "eventTime",
    "aliases": {
      "table": "type",
      "rowKey": "key"
    },
    "fields": {
      "eventTime": {
        "kind": "date",
        "bindings": [{ "version": 1, "jsonPointer": "/headers/timestamp" }],
        "exact": true,
        "column": true,
        "exists": true,
        "sortable": true
      },
      "operation": {
        "kind": "keyword",
        "bindings": [{ "version": 1, "jsonPointer": "/headers/operation" }],
        "exact": true,
        "exists": true
      },
      "type": {
        "kind": "keyword",
        "bindings": [{ "version": 1, "jsonPointer": "/type" }],
        "exact": true,
        "exists": true
      },
      "key": {
        "kind": "keyword",
        "bindings": [{ "version": 1, "jsonPointer": "/key" }],
        "exact": true,
        "exists": true
      }
    }
  }
}
```

With that schema, these query patterns are supported efficiently:

- table + row:
  - `POST /v1/stream/<name>/_search` with `q: "type:\"public.posts\" AND key:\"42\""`
- table only:
  - `POST /v1/stream/<name>/_search` with `q: "type:\"public.posts\""`

Important:

- `GET /v1/stream/<name>?key=...` filters by the stream routing key, not the
  top-level WAL payload field named `key`.
- For multi-table WAL streams, keep row identity lookup in schema search
  (`type` + `key`) unless you intentionally define a dedicated composite routing
  key in the payload.

## Implementation Rule

All State Protocol-specific behavior lives behind the profile definition under
`src/profiles/stateProtocol.ts` and `src/profiles/stateProtocol/`.

The supported extension model is:

- profile-specific behavior is implemented in the profile module
- the registry wires the profile in one place
- core runtime code dispatches through profile hooks

The core engine must not add direct `if (profile.kind === "state-protocol")`
branches for supported behavior.
