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

## HTTP API

- `GET /v1/stream/<name>/_schema` returns the registry.
- `POST /v1/stream/<name>/_schema` updates it.
- `POST /v1/stream/<name>/_schema` is strict: it accepts only the supported
  fields for schema updates and routing-key updates.
- Profile-owned live/touch configuration belongs in `/_profile`, not `/_schema`.

Accepted POST shapes:

1) Schema install or schema evolution:

```json
{"schema": {"type": "object", "additionalProperties": true}, "lens": { ... }, "routingKey": {"jsonPointer": "/id", "required": true}}
```

2) Routing-key only update:

```json
{"routingKey": {"jsonPointer": "/subject/uri", "required": true}}
```

Not supported:

- registry-shaped writes like `{ "schemas": ..., "lenses": ... }`
- routing-key aliases such as `routing_key`, `routingKeyPointer`, or
  `json_pointer`
- profile fields under `_schema`

## Write path (validation)

- When `currentVersion > 0`, **JSON appends are validated** against the current schema.
- External `$ref` is **not** supported.
- If validation fails, the append returns 400.

## Read path (promotion)

- Reads always return events matching the **current schema version**.
- Older events are promoted by applying the lens chain `v -> v+1 -> ... -> currentVersion`.
- Reads do **not** re‑validate JSON against the schema; correctness is enforced at update time and write time.

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
- profile-owned projections or indexes

Those responsibilities belong to the stream profile layer.
