# Prisma Streams Schemas And Lenses

Durable Streams supports **per‑stream JSON Schemas** and **schema evolution** via
**lenses**. Schemas and lenses are stored in SQLite as a per‑stream registry.

Profiles and schemas are separate concerns:

- a **profile** defines stream semantics
- a **schema** defines payload shape

See [stream-profiles.md](./stream-profiles.md).

## Registry storage

Each stream has a schema registry stored in SQLite (`schemas` table). The registry
format is:

```json
{
  "apiVersion": "durable.streams/schema-registry/v1",
  "schema": "my-stream-name",
  "currentVersion": 2,
  "routingKey": {"jsonPointer": "/user/id", "required": true},
  "search": {
    "primaryTimestampField": "eventTime",
    "fields": {
      "eventTime": {
        "kind": "date",
        "bindings": [{"version": 1, "jsonPointer": "/eventTime"}],
        "column": true,
        "exists": true,
        "sortable": true
      },
      "service": {
        "kind": "keyword",
        "bindings": [{"version": 1, "jsonPointer": "/service"}],
        "normalizer": "lowercase_v1",
        "exact": true,
        "prefix": true,
        "exists": true
      }
    }
  },
  "boundaries": [
    {"offset": 0, "version": 1},
    {"offset": 150, "version": 2}
  ],
  "schemas": {
    "1": {"...": "json schema v1"},
    "2": {"...": "json schema v2"}
  },
  "lenses": {
    "1": {"...": "lens v1->v2"}
  }
}
```

Notes:

- `boundaries` map stream offsets to schema versions; they are stored as numbers
  and must fit in `Number.MAX_SAFE_INTEGER`.
- `routingKey` is optional. When configured, the server derives routing keys
  from JSON appends using the JSON Pointer.
- `search` is optional. When configured, the server builds schema-owned search
  structures from `search.fields`.
- `search.fields` supports stable logical field IDs, per-version bindings,
  aliases, and capability bits such as `exact`, `prefix`, `column`, `exists`,
  and `sortable`.
- `search.rollups` is optional. When configured, the server builds schema-owned
  `.agg` rollup companions and enables `POST /v1/stream/{name}/_aggregate`.

## HTTP API

- `GET /v1/stream/<name>/_schema` returns the registry.
- `POST /v1/stream/<name>/_schema` updates it.
- `POST /v1/stream/<name>/_schema` is strict: it accepts only the supported
  fields for schema updates, routing-key updates, and search updates.
- Profile-owned live/touch configuration belongs in `/_profile`, not `/_schema`.

One profile-owned exception exists in the current shipped system:

- installing the `evlog` profile auto-installs its canonical schema version `1`
  and default `search` registry, so the default evlog path does not require a
  separate manual `/_schema` call

Accepted POST shapes:

1) Schema install or schema evolution:

```json
{"schema": {"type": "object", "additionalProperties": true}, "lens": { ... }, "routingKey": {"jsonPointer": "/id", "required": true}, "search": {"primaryTimestampField": "eventTime", "fields": {"service": {"kind": "keyword", "bindings": [{"version": 1, "jsonPointer": "/service"}], "exact": true, "prefix": true, "exists": true}, "eventTime": {"kind": "date", "bindings": [{"version": 1, "jsonPointer": "/eventTime"}], "column": true, "exists": true, "sortable": true}}}}
```

2) Routing-key only update:

```json
{"routingKey": {"jsonPointer": "/subject/uri", "required": true}}
```

3) Search-only update:

```json
{"search": {"primaryTimestampField": "eventTime", "fields": {"status": {"kind": "integer", "bindings": [{"version": 1, "jsonPointer": "/status"}], "exact": true, "column": true, "exists": true}}}}
```

4) Search update with rollups:

```json
{"search": {"primaryTimestampField": "eventTime", "fields": {"eventTime": {"kind": "date", "bindings": [{"version": 1, "jsonPointer": "/eventTime"}], "exact": true, "column": true, "exists": true, "sortable": true}, "service": {"kind": "keyword", "bindings": [{"version": 1, "jsonPointer": "/service"}], "exact": true, "prefix": true, "exists": true}, "duration": {"kind": "float", "bindings": [{"version": 1, "jsonPointer": "/duration"}], "exact": true, "column": true, "exists": true, "sortable": true, "aggregatable": true}}, "rollups": {"requests": {"dimensions": ["service"], "intervals": ["1m"], "measures": {"requests": {"kind": "count"}, "latency": {"kind": "summary", "field": "duration", "histogram": "log2_v1"}}}}}}
```

Important rule:

- a search-only update requires an already-installed schema version
- if you are installing the first schema for a stream, install `schema` and
  `search` together in the same `_schema` request

Not supported:

- registry-shaped writes like `{ "schemas": ..., "lenses": ... }`
- routing-key aliases such as `routing_key`, `routingKeyPointer`, or
  `json_pointer`
- legacy `indexes[]`
- profile fields under `_schema`

## Write path (validation)

- When `currentVersion > 0`, **JSON appends are validated** against the current schema.
- External `$ref` is **not** supported.
- If validation fails, the append returns 400.

## Read path (promotion)

- Reads always return events matching the **current schema version**.
- Older events are promoted by applying the lens chain `v -> v+1 -> ... -> currentVersion`.
- Reads do **not** re‑validate JSON against the schema; correctness is enforced at update time and write time.
- `GET /v1/stream/<name>?filter=...` may reference only fields named in
  `search.fields`.
- Exact-equality filter clauses can use the internal exact family to prune
  sealed segments.
- Typed equality/range clauses can use `.col` companions to prune segment-local
  docs.
- `_search` uses the same `search.fields` registry to drive exact, prefix,
  typed, and text queries.
- `_aggregate` uses `search.rollups` to drive object-store-native precomputed
  rollups with raw-scan fallback for correctness.
- Unsealed tail reads still verify from the promoted JSON records.

## Schema update rules

- The **first schema** (`currentVersion: 0 -> 1`) requires an **empty stream**.
- Subsequent updates require a valid lens (`from=N`, `to=N+1`).
- Lens safety is validated with a proof check against the old/new schemas.

## Routing keys

If `routingKey` is configured:

- The server derives routing keys per JSON entry using the JSON Pointer.
- JSON appends must **not** include `Stream-Key` (otherwise 400).

## What Schemas Do Not Define

Schemas do not define:

- whether a stream is `generic`, `queue`, `evlog`, or `state-protocol`
- profile-owned endpoints
- profile-owned endpoints or runtime hooks

Schemas do define payload-owned field extraction, including routing keys and
schema-owned search field declarations and rollups.

Those responsibilities belong to the stream profile layer.
