# Prisma Streams

Prisma Streams is a Bun + TypeScript implementation of the Durable Streams HTTP
protocol.

It provides:

- a full server mode with SQLite WAL storage, segmenting, upload, and index
  maintenance
- a trusted local mode for Prisma development workflows
- a stream profile model that cleanly separates durable storage semantics from
  payload structure

The canonical documentation index is [docs/index.md](./docs/index.md). The
dedicated stream profile reference is
[docs/stream-profiles.md](./docs/stream-profiles.md).

## Core Model

The system now has three separate concepts:

- a **stream** is the durable append-only storage object with Durable Streams
  semantics
- a **profile** defines the semantic contract of that stream
- a **schema** defines payload structure

Short rule:

- profile = semantics
- schema = structure

More concretely:

- the **stream** owns ordered append/read behavior, offsets, and durable
  storage
- the **profile** owns semantic behavior, profile-specific endpoints, and
  profile-specific runtime configuration
- the **schema** owns JSON validation, version boundaries, lenses, and
  routing-key extraction

Profiles sit on top of the durable stream engine. They do not replace streams.

Built-in profiles are implemented under `src/profiles/`. The core engine
resolves a profile definition and dispatches through its hooks instead of
branching on profile kinds in request or background processing paths.

Profile-specific behavior must live in a dedicated profile module such as
`src/profiles/stateProtocol.ts`. The supported extension model is:

- implement the profile in its own file or subdirectory under `src/profiles/`
- register it once in `src/profiles/index.ts`
- let core paths call profile hooks instead of adding `if (profile.kind ===
  "...")` checks

## Profile Defaults

Every stream has a profile.

- if you declare a profile, that is the stream's profile
- if you create a stream without declaring a profile, the server treats it as a
  `generic` stream

This means old or unconfigured streams still have a clear meaning: they are
plain `generic` durable streams.

## Built-In Profiles

Current built-ins:

- `generic`
- `state-protocol`

Planned next built-ins:

- `evlog`
- `queue`

### `generic`

`generic` means:

- plain ordered append-only storage
- optional user-managed schema validation
- optional schema-managed routing-key extraction
- no profile-owned payload envelope
- no profile-specific endpoints

`generic` is intentionally narrow. It is the baseline durable stream and the
automatic default when no other profile is declared.

### `state-protocol`

`state-protocol` is the built-in profile for JSON streams that carry State
Protocol change records and expose the live `/touch/*` API surface.

It means:

- the stream content type must be `application/json`
- the payload semantics are State Protocol records
- touch configuration belongs to the profile
- `/touch/*` exists only when `touch.enabled=true`

Schemas remain optional on `state-protocol` streams. If present, they validate
the JSON payload shape, but they do not own live/touch behavior.

State Protocol is a profile, not a schema feature, because it defines stream
semantics and profile-owned endpoints, not just JSON shape.

## Profile Versus Schema

What belongs in a profile:

- semantic meaning of records
- profile-specific runtime config
- profile-specific endpoints
- profile-owned indexes or projections
- future canonical envelopes for specialized stream types

What belongs in a schema:

- JSON validation
- version boundaries
- lens-based read promotion
- routing-key extraction rules

What does **not** belong in `/_schema`:

- profile selection
- touch configuration
- State Protocol runtime behavior

The supported model is strict: `/_profile` manages profile semantics,
`/_schema` manages schema evolution.

## Profile API

Profiles are managed through a dedicated subresource:

- `GET /v1/stream/{name}/_profile`
- `POST /v1/stream/{name}/_profile`

Example default response:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": { "kind": "generic" }
}
```

Explicit `generic` declaration:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": { "kind": "generic" }
}
```

State Protocol profile with touch enabled:

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

To switch a stream back to the baseline behavior, set `profile` to
`{ "kind": "generic" }`.

## Storage Model

The stored profile is kept in stream metadata.

- `streams.profile` stores the profile kind
- `stream_profiles.profile_json` stores non-generic profile configuration
- `NULL` profile metadata means there is no explicit stored entry and the
  stream is treated as `generic`

This keeps storage simple while still letting runtime code assume a profile
always exists.

## Current Durability Model

In full mode today:

- append ACK means the write is durable in local SQLite
- object-store durability happens only after segment upload and manifest
  publication

`--bootstrap-from-r2` rebuilds published stream history and metadata from
manifest, segment, and schema objects in object storage. It does
not restore transient local SQLite state such as the unuploaded WAL tail,
producer dedupe state, or runtime live/template state.

A stream becomes recoverable from object storage after its first manifest is
published.

## Possible Future Durability Modes

Not implemented today:

- an object-store-acked mode that would batch writes and ACK only after
  persistence to R2
- a cluster quorum mode that would ACK only after a durability quorum accepts
  the write

## Current Supported Paths

The supported behavior is:

- use `/_profile` to choose `generic` or `state-protocol`
- use `/_schema` only for schema validation, routing-key config, and schema
  evolution
- use `/touch/*` only on `state-protocol` streams with touch enabled

Legacy compatibility branches are intentionally not part of the supported
surface.

## Start Here

- [docs/index.md](./docs/index.md) for the documentation map
- [docs/overview.md](./docs/overview.md) for product and package overview
- [docs/stream-profiles.md](./docs/stream-profiles.md) for the full stream /
  profile / schema reference
- [docs/durable-streams-spec.md](./docs/durable-streams-spec.md) for the HTTP
  protocol contract
- [docs/live.md](./docs/live.md) for the State Protocol live/touch model
- [docs/schemas.md](./docs/schemas.md) for schema registry and lens behavior
