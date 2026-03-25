# Generic Profile

`generic` is the baseline stream profile.

It means:

- durable append-only stream semantics with no extra profile-owned behavior
- optional user-managed schema validation
- optional schema-managed routing-key extraction
- no canonical payload envelope imposed by the profile
- no profile-owned subroutes, background processing, or secondary indexes

## When To Use It

Use `generic` when the stream is just a durable log and the system does not
need profile-specific semantics.

Examples:

- application event streams
- ad hoc JSON streams
- binary streams without schema validation
- integration streams where consumers define the meaning externally

## Write Path

`generic` uses the normal stream create/append APIs:

- `PUT /v1/stream/{name}`
- `POST /v1/stream/{name}`

If the payload is JSON and the stream has a schema, the schema validates the
payload and may derive the routing key. Otherwise the profile does not modify
the body.

## Read Path

`generic` uses the normal stream read APIs:

- `GET /v1/stream/{name}`
- `HEAD /v1/stream/{name}`

Any filtering or long-poll behavior comes from the durable stream engine, not
from the profile.

## Schema Relationship

On `generic`, schemas are fully user-managed.

Schemas may define:

- JSON validation
- version boundaries
- lenses
- routing-key extraction

The profile does not constrain schema shape and does not own a canonical
envelope.

## Storage

If no profile is explicitly declared, the stream is treated as `generic`.
Storage may omit an explicit `generic` profile row and still resolve the stream
as `generic` at runtime.
