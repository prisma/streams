# OpenTelemetry Traces Profile

This document defines the v1 `otel-traces` profile.

`otel-traces` is the built-in profile for storing OpenTelemetry trace spans as
ordinary Prisma Streams records. It keeps trace semantics separate from
`evlog`: an `evlog` stream stores one wide request-centric event, while an
`otel-traces` stream stores one canonical JSON record per span.

## Stream Contract

`otel-traces` means:

- the stream content type must be `application/json`
- the profile must be installed before the stream has appended data
- JSON appends are normalized into the canonical span envelope
- OTLP trace exports can be ingested through profile-owned OTLP endpoints
- redaction and backend-side limits run before durable append
- installing the profile auto-installs canonical schema version `1`
- installing the profile auto-installs default `search` fields and rollups
- the default routing key is `traceId`
- reads continue to use the normal durable stream APIs

The profile does not introduce a mutable local span table. The stream remains
the durable source of truth, and search/aggregate behavior uses the existing
schema-owned companion index system.

## Profile Resource

Install the profile with:

```http
POST /v1/stream/app-traces/_profile
Content-Type: application/json

{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "otel-traces",
    "redactKeys": ["authorization", "cookie", "password", "token", "secret"],
    "requestIdAttributes": ["request.id", "http.request.header.x-request-id"],
    "attributeLimits": {
      "maxAttributeValueBytes": 8192,
      "maxAttributesPerSpan": 256,
      "maxEventsPerSpan": 128,
      "maxLinksPerSpan": 128,
      "maxStatementBytes": 4096
    },
    "store": {
      "rawResourceAttributes": true,
      "rawSpanAttributes": true,
      "rawEvents": true,
      "rawLinks": true
    },
    "dbStatementMode": "drop",
    "otlpLimits": {
      "maxCompressedBytes": 4194304,
      "maxDecodedBytes": 16777216,
      "maxResourceSpansPerRequest": 1024,
      "maxScopeSpansPerRequest": 4096,
      "maxSpansPerRequest": 50000
    },
    "observability": {
      "request": {
        "eventsStream": "app-events"
      }
    }
  }
}
```

Supported `dbStatementMode` values:

- `drop` stores `db.statement` as `null`
- `raw` stores the statement after normal attribute value truncation

There is no `redact_literals` mode in the shipped implementation.

Redaction matches configured keys case-insensitively and also checks dotted
header/metadata suffixes. For example, the built-in `authorization`, `cookie`,
`set-cookie`, and `x-api-key` entries redact attributes such as
`http.request.header.authorization`, `http.request.header.cookie`,
`http.response.header.set-cookie`, `http.request.header.x-api-key`, and
`rpc.request.metadata.authorization`.

## Canonical Span Envelope

Each stored span is normalized to a stable JSON object with:

- identity fields: `traceId`, `spanId`, `parentSpanId`, `identity.spanKey`
- timestamps: `timestamp`, `endTimestamp`, `startUnixNano`, `endUnixNano`,
  `duration`
- span semantics: `name`, `kind`, `status`, `traceState`, `traceFlags`
- resource fields: `service`, `serviceNamespace`, `serviceInstanceId`,
  `environment`, `version`, `region`
- correlation field: `requestId`
- semantic convention groups: `http`, `db`, `rpc`, `messaging`, `error`
- raw retained data: `resource.attributes`, `instrumentationScope.attributes`,
  `attributes`, `events`, `links`
- derived `eventNames` for searchable span event names
- dropped/limited counters and `redaction.keys`

Trace IDs must be 32-character lowercase hex strings and span IDs must be
16-character lowercase hex strings. All-zero trace and span IDs are rejected.

Nanosecond timestamps are preserved as decimal strings. `timestamp`,
`endTimestamp`, and `duration` are derived for search, sort, aggregation, and
UI rendering.

When an already-canonical span record is appended again, top-level canonical
fields such as service, environment, request ID, HTTP fields, error fields,
duration, and `eventNames` are preserved even if the raw attributes or raw
events were not retained in the stored record.

## OTLP Ingestion

Two endpoints accept OTLP trace exports.

Default endpoint:

```http
POST /v1/traces
Content-Type: application/x-protobuf
```

or:

```http
POST /v1/traces
Content-Type: application/json
```

`/v1/traces` writes to `DS_OTLP_TRACES_STREAM`. If
`DS_OTLP_AUTO_CREATE=true` and the stream does not exist, the server creates an
`application/json` stream, installs `otel-traces`, uploads the schema/profile
metadata, publishes a manifest, and then appends accepted spans.

Explicit stream endpoint:

```http
POST /v1/stream/app-traces/_otlp/v1/traces
Content-Type: application/x-protobuf
```

The explicit endpoint requires the target stream to already have the
`otel-traces` profile.

Both endpoints support:

- `application/x-protobuf`
- `application/json`
- `Content-Encoding: gzip`

Malformed payloads and requests that exceed resource-span, scope-span, or span
count limits return `400`. Payloads that exceed compressed or decoded byte
limits return `413`. Unsupported media types or encodings return `415`. A
successful full acceptance returns OTLP success. Partial acceptance returns
HTTP `200` with OTLP `partialSuccess` / `partial_success` information; clients
should not retry rejected spans from that response.

## JSON Appends

Normal JSON appends to an `otel-traces` stream are also normalized by the
profile. This is intended for tests, local tools, and direct integrations that
already have canonical span-shaped JSON.

```http
POST /v1/stream/app-traces
Content-Type: application/json

{
  "traceId": "5b8efff798038103d269b633813fc60c",
  "spanId": "086e83747d0e381e",
  "name": "GET /checkout",
  "kind": "server",
  "startUnixNano": "1772020800000000000",
  "endUnixNano": "1772020800123000000",
  "resource": { "attributes": { "service.name": "checkout" } },
  "attributes": { "request.id": "req_123" }
}
```

The stored record is the canonical span envelope, not the input object.

## Search Defaults

The profile installs schema-owned search fields including:

- exact/prefix: `traceId`, `spanId`, `parentSpanId`, `requestId`, `service`,
  `environment`, `name`, `kind`, `status.code`, HTTP/DB/RPC/messaging fields
- typed columns: `timestamp`, `endTimestamp`, `duration`,
  `http.statusCode`, `error.isError`
- text: `status.message`, `error.message`, `error.stacktrace`,
  `db.statement`, `events.name`

Aliases include:

- `trace` -> `traceId`
- `span` -> `spanId`
- `parent` -> `parentSpanId`
- `req` -> `requestId`
- `svc` -> `service`
- `op` -> `name`
- `route` -> `http.route`
- `method` -> `http.method`
- `status` -> `http.statusCode`
- `error` -> `error.isError`
- `db` -> `db.system`
- `duration_ms` -> `duration`
- `time` / `ts` -> `timestamp`

Default rollups:

- `spans` over `service`, `kind`, and `status.code`
- `http_server` over `service`, `http.method`, `http.route`, and
  `http.statusCode`, filtered to `kind:server`

Each rollup includes a count measure, an `errors` count measure filtered to
`error:true`, and a `duration` summary measure.

## Request Correlation

Applications should copy the evlog request ID into the active root/server span
as `request.id`. The profile also checks these request ID attributes by
default:

- `request.id`
- `http.request_id`
- `http.request.header.x_request_id`
- `http.request.header.x-request-id`
- `http.request.header.x_correlation_id`
- `http.request.header.x-correlation-id`
- `correlation.id`

The cross-stream request view is implemented by
[`request-observability.md`](./request-observability.md), not by merging
`evlog` and spans into one profile.

`observability.request.eventsStream` declares the explicit `evlog` counterpart
for request-observability clients. When it is present, `GET /v1/streams` and
`GET /v1/stream/{name}/_details` expose:

```json
{
  "observability": {
    "request": {
      "events_stream": "app-events",
      "traces_stream": "app-traces"
    }
  }
}
```

Clients must use this descriptor instead of selecting the first `evlog` stream
they find.

## Security And Privacy

Redaction is case-insensitive and happens before durable append. It applies to:

- resource attributes
- span attributes
- instrumentation scope attributes
- span event attributes
- link attributes

Default sensitive keys include `password`, `token`, `secret`, `authorization`,
`cookie`, `apikey`, `api_key`, `set-cookie`, and `x-api-key`.

Use separate streams for data with different retention, access, or tenant
boundaries. Do not rely on UI filters for tenant isolation.
