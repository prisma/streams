# Evlog Profile

This document defines the v1 `evlog` profile design.

The design is based on evlog's core model of one wide event per request,
structured errors with `why` and `fix`, request-scoped context, trace
correlation, and production-safe logging practices.

## Goals

The v1 profile:

- keep evlog data in ordinary durable streams
- store one canonical JSON event per appended record
- avoid unbounded local SQLite indexing
- preserve exact append-only durability semantics
- support request-centric lookup through the existing routing-key path

The v1 profile does not introduce a separate observability storage engine.

## Stream Contract

`evlog` means:

- stream content type must be `application/json`
- the profile must be installed before the stream has appended data
- appended JSON records are normalized into an evlog canonical envelope
- the profile provides a default routing key from `requestId`, with `traceId`
  fallback
- reads continue to use the normal durable stream APIs

## Canonical Envelope

Each stored event should use this stable top-level shape:

- `timestamp`
- `level`
- `service`
- `environment`
- `version`
- `region`
- `requestId`
- `traceId`
- `spanId`
- `method`
- `path`
- `status`
- `duration`
- `message`
- `why`
- `fix`
- `link`
- `sampling`
- `redaction`
- `context`

All non-reserved fields are moved into `context`.

`context` is where wide event data stays extensible without letting the profile
surface become open-ended.

## Write Path

V1 uses the existing stream append APIs:

- `PUT /v1/stream/{name}`
- `POST /v1/stream/{name}`

When the stream profile is `evlog`, the profile's JSON-ingest hook:

1. validates that each JSON record is an object
2. normalizes the event into the canonical envelope
3. applies redaction before durable append
4. suggests a routing key from `requestId` or `traceId`

If the stream also has a schema, the schema validates the canonical evlog
record after normalization.

## Redaction

V1 redaction is profile-owned and happens before durable append.

Built-in behavior:

- sensitive keys are matched case-insensitively
- matching fields are replaced with a redacted marker
- redaction metadata is stored in `redaction`

Default sensitive keys should include:

- `password`
- `token`
- `secret`
- `authorization`
- `cookie`
- `apiKey`

The profile may allow extending this list.

## Reads And Lookup

V1 does not add a local SQLite observability index.

Instead:

- normal reads use `GET /v1/stream/{name}`
- exact request lookup uses the existing routing-key filter with the derived
  `requestId`
- trace fallback still works for streams where `requestId` is absent

This keeps the durable source of truth in the stream and avoids unbounded local
projection tables.

## Secondary Indexing

V1 does not build evlog-specific local SQLite indexes.

Future profile-owned secondary indexing should be:

- asynchronous
- rebuildable from the durable stream
- stored as object-store-native index artifacts

That keeps local SQLite bounded and preserves the existing recovery model.

## Schema Relationship

Schemas remain optional on `evlog` streams.

If present, a schema validates the canonical evlog envelope. The profile still
owns:

- envelope normalization
- redaction
- routing-key defaults
- future evlog-specific query surfaces

## Deferred Work

Not in the v1 cut:

- native `POST /v1/evlog`
- OTLP `/v1/logs`
- async evlog-specific object-store index artifacts
- richer profile-owned query endpoints

Those can be added later through the same profile hook model without changing
the durable stream core.
