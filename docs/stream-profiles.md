# Prisma Streams Stream Profiles

This document defines the relationship between a **stream**, a **profile**, and
a **schema**.

## Core Model

- A **stream** is always the durable append-only storage object with Durable
  Streams semantics.
- A **profile** defines stream semantics beyond raw append/read behavior.
- A **schema** defines payload shape.

Short rule:

- profile = semantics
- schema = structure

More concretely:

- the **stream** owns ordered append/read behavior, offsets, and durable
  storage
- the **profile** owns semantic meaning, profile-specific runtime behavior, and
  profile-specific endpoints
- the **schema** owns JSON validation, version boundaries, lenses, and
  routing-key extraction

Profiles do **not** replace streams. They sit on top of the existing durable
stream engine.

## Profile Defaults

Every stream has a profile.

- If a stream explicitly declares a profile, that is the stream's profile.
- If a stream is created without an explicit profile, the server treats it as a
  `generic` stream.

This keeps the public model simple while still allowing storage to omit an
explicit `generic` declaration.

## Built-In Profiles

Current built-ins:

- `evlog`
- `generic`
- `metrics`
- `state-protocol`

## `generic`

`generic` is the baseline meaning of “plain durable stream”.

It means:

- append-only ordered storage
- optional user-managed schema validation
- optional schema-managed routing-key extraction
- no profile-owned canonical payload envelope
- no profile-owned indexes
- no profile-specific query surface

`generic` is intentionally narrow. It is the default profile, not a catch-all
for future features.

## `evlog`

`evlog` is the built-in profile for request-centric wide-event logging.

It means:

- the stream content type must be `application/json`
- JSON appends are normalized into a canonical evlog envelope
- redaction happens before durable append
- installing the profile auto-installs the canonical evlog schema registry and
  default search fields and rollups
- the default routing key is `requestId`, with `traceId` fallback
- reads continue to use the normal stream API

See [profile-evlog.md](./profile-evlog.md) for the detailed contract.

## `metrics`

`metrics` is the built-in profile for canonical metric interval streams.

It means:

- the stream content type must be `application/json`
- JSON appends are normalized into the canonical metrics interval envelope
- installing the profile auto-installs the canonical metrics schema registry,
  search fields, and default rollups
- the canonical routing key is `seriesKey`
- metrics streams enable the `.mblk` metrics-block family in addition to the
  generic search families

See [profile-metrics.md](./profile-metrics.md) and [metrics.md](./metrics.md)
for the detailed contract.

## `state-protocol`

`state-protocol` is the built-in profile for streams that carry State Protocol
change records and expose the live `/touch/*` API surface.

It means:

- the stream content type must be `application/json`
- the stream payload semantics are State Protocol records
- the profile owns touch configuration
- `/touch/*` routes exist only when `touch.enabled=true`

Schemas remain optional on `state-protocol` streams. If present, they validate
the JSON payload shape, but they do not own live/touch behavior.

## Ownership Matrix

Use this rule when deciding where behavior belongs:

- put it in the **stream** if it is fundamental durable storage behavior
- put it in the **profile** if it changes stream semantics or adds
  profile-owned runtime/API behavior
- put it in the **schema** if it describes payload shape or schema evolution

Examples:

- append/read ordering: stream
- `/touch/*` availability: profile
- touch configuration: profile
- metrics canonicalization and `.mblk` enablement: profile
- JSON validation: schema
- version boundaries and lenses: schema
- routing-key extraction: schema

## Profile Responsibilities

A profile may define:

- canonical payload semantics
- schema policy
- field bindings and routing-key defaults
- profile-owned indexes or projections
- profile-specific endpoints

Profiles are the place for semantics. They are not a second schema registry.

## Runtime Wiring

Built-in profiles are implemented as definition modules under `src/profiles/`.

A profile definition owns:

- validation and normalization of its profile document
- parsing of stored profile state
- persistence side effects when the profile is installed or updated
- optional capability hooks for profile-owned runtime behavior

The registry in `src/profiles/index.ts` is the single place that wires built-in
profiles into the system.

This means a new built-in profile should normally require:

- one new file under `src/profiles/`
- one registry entry in `src/profiles/index.ts`

If a profile needs more internal files, put them under a profile-owned
subdirectory such as `src/profiles/stateProtocol/` and keep
`src/profiles/<name>.ts` as the single entrypoint that the rest of the system
uses.

The core engine resolves a profile definition and dispatches through its hooks.
Supported stream-specific behavior must not be added by sprinkling
`if (profile.kind === "...")` checks through request handlers, background
loops, or worker code.

## Schema Responsibilities

Schemas remain responsible for:

- JSON validation on write
- version boundaries
- lens-based promotion on read
- routing-key extraction for schema-managed JSON streams

On `generic`, schemas are optional and user-managed.

What does **not** belong in `/_schema`:

- profile selection
- touch configuration
- State Protocol runtime behavior
- evlog envelope normalization or redaction
- metrics interval normalization and `.mblk` enablement

## Supported API Rules

The supported split is strict:

- `/_profile` chooses the stream profile
- `/_schema` manages schema validation and schema evolution
- `state-protocol` configuration does not live under `/_schema`
- unsupported profile kinds are rejected
- schema update alias fields and registry-shaped compatibility writes are
  rejected

This keeps one supported code path for profile semantics and one supported code
path for schema evolution.

## State-Protocol Decision

State Protocol is a profile, not a schema feature.

Reason:

- it defines stream semantics, not just payload shape
- it introduces profile-specific endpoints (`/touch/*`)
- it owns special runtime behavior through the touch processor and touch route
  hooks

## HTTP Resource

Profiles are managed through a dedicated stream subresource:

- `GET /v1/stream/{name}/_profile`
- `POST /v1/stream/{name}/_profile`

The canonical response shape is:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": { "kind": "generic" }
}
```

The canonical update shape is:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": { "kind": "generic" }
}
```

State Protocol uses the same resource:

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

Evlog uses the same resource:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "evlog",
    "redactKeys": ["sessiontoken"]
  }
}
```

Metrics uses the same resource:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "metrics"
  }
}
```

To switch a stream back to the baseline behavior, set `profile` to
`{ "kind": "generic" }`.

`GET /v1/streams` exposes a summary view:

- `profile`: the stream profile kind

The full typed profile object is returned by `GET /_profile`.

For GUI-oriented stream management, the current per-stream inspection endpoints
are:

- `GET /v1/stream/{name}/_schema`
- `GET /v1/stream/{name}/_profile`
- `GET /v1/stream/{name}/_index_status`
- `GET /v1/stream/{name}/_details`

`/_details` is the combined descriptor endpoint. It returns:

- the current full stream summary, including head/lifecycle fields such as
  `epoch`, `next_offset`, `created_at`, `expires_at`, `sealed_through`,
  `uploaded_through`, and `total_size_bytes`
- the full `/_profile` resource
- the full `/_schema` registry
- the current `/_index_status` payload
- storage accounting split into uploaded object-storage bytes, local retained
  bytes, and bundled companion family bytes
- node-local per-stream object-store request counters, including a per-artifact
  breakdown

That lets a UI inspect and edit streams without inventing its own metadata
cache.

`/_details` and `/_index_status` both support conditional long-polling:

- responses include `ETag`
- send `If-None-Match` with the last seen `ETag`
- add `live=long-poll&timeout=5s` to wait for the next visible change
- the server returns `200` when the descriptor changes, `304` on route-local
  timeout, and `408` if the generic `5s` resolver timeout fires first

## Storage Model

The stream metadata stores the profile metadata.

- `NULL` means “no explicit declaration”; current stream creation stores
  `generic`, but readers still treat `NULL` as `generic`
- `streams.profile` stores the profile kind
- `stream_profiles.profile_json` stores non-generic profile configuration
- if no profile is explicitly declared, the stream is treated as `generic`

This keeps storage simple and avoids inventing a second metadata layer.

## Future Profiles

Additional future profiles should follow the same rules:

- the stream remains the same durable append-only storage object
- the profile defines semantic meaning and profile-owned behavior
- the schema continues to define payload structure

`generic` stays narrow so future profiles can add specialized behavior without
turning the baseline durable stream model into a catch-all abstraction.
