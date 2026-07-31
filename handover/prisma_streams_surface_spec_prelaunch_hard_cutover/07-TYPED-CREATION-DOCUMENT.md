# Stage 7 — Typed Creation Document

**Goal:** replace a growing set of product-specific creation headers with one idempotent, typed JSON configuration while preserving the raw Durable Streams `PUT` contract.

---

## 0. Pre-launch clean-switch policy

This specification targets a **pre-launch** product. Implementation MUST be a destructive clean switch to the final design described here.

- Existing Prisma Streams development, test, staging, and campaign data is disposable. Existing descriptors, cursors, consumer state, watch state, segment maps, history/index layouts, and product API requests MUST NOT be migrated.
- The new implementation MUST be deployed against a fresh storage namespace, such as a new bucket or `PATH_PREFIX`. It MUST NOT open or mutate a namespace written by the current experimental implementation.
- Do not build compatibility translators, legacy decoders, cutover offsets, dual readers, dual writers, shadow writes, aliases, deprecation windows, sunset headers, or background migration tools.
- Delete obsolete fields, routes, codecs, and branches instead of retaining dormant compatibility code.
- Update server, SDK, documentation, conformance fixtures, deployment configuration, and test data together. Intermediate stage builds are engineering checkpoints, not supported product versions.
- Rollback means restoring the previous binary together with its previous isolated storage namespace. The old binary is not required to read data written by the new binary, and the new binary is not required to read old data.
- The only preserved external contract is conformance to the pinned **Durable Streams protocol** on `/v1/stream/{name}`. That route is a standards surface for Durable Streams clients, not a legacy Prisma Streams update path.

---

## 1. SDK contract

```ts
const stream = await client.createStream("orders", {
  encryptionKey,
  format: { kind: "json" },
  expiry: { idle: "30d" },
  watches: [
    { name: "by-customer", fields: ["/customerId"] },
  ],
})
```

Configuration type:

```ts
interface CreateStreamOptions {
  encryptionKey: string

  format:
    | { kind: "json" }
    | { kind: "bytes"; contentType?: string }

  expiry?:
    | { idle: Duration }
    | { at: string } // RFC 3339

  watches?: WatchDefinition[]
}
```

No fields exist for:

```text
profile
ordering
segments
scaling
queueMaxDeliveries
index mode
live mode
```

Consumer groups are created separately. Producer state is created by use. Segmentation/indexing are automatic.

---

## 2. Product management route

```http
PUT /v1/streams/{name}
Content-Type: application/json
Prisma-Encryption-Key: <base64url key>

{
  "format": { "kind": "json" },
  "expiry": { "idle": "30d" },
  "watches": [
    { "name": "by-customer", "fields": ["/customerId"] }
  ]
}
```

Response:

- `201 Created` for a new collection;
- `200 OK` for identical normalized configuration;
- `409 Conflict` for existing collection with different immutable configuration;
- `400 Bad Request` for invalid configuration;
- `403 Forbidden` for an encryption-key mismatch;
- `429 Too Many Requests` for management-plane limits.

Response body:

```json
{
  "name": "orders",
  "contentType": "application/json",
  "createdAt": "...",
  "sealed": false,
  "expiry": { "idle": "30d" }
}
```

---

## 3. Format mapping

### JSON

```json
{ "kind": "json" }
```

maps to:

```text
Content-Type: application/json
```

and enables standards-conformant JSON message semantics on the default-key standards view.

### Bytes/custom MIME

```json
{
  "kind": "bytes",
  "contentType": "application/x-protobuf"
}
```

maps to the specified MIME type. If omitted:

```text
application/octet-stream
```

The server operates at the byte level for all non-JSON content types.

A separate `text` mode is unnecessary; use `kind: "bytes"` with `text/plain`.

---

## 4. Expiry mapping

### Idle expiry

```json
{ "expiry": { "idle": "30d" } }
```

maps to Durable Streams `Stream-TTL` semantics:

- the window is sliding;
- origin reads and writes reset it;
- `HEAD` does not reset it;
- CDN-served reads do not reach the origin and do not reset it;
- live reads reset it when origin processing begins.

The product documentation must call this **idle expiry**, not retention or fixed time since creation.

### Absolute expiry

```json
{ "expiry": { "at": "2027-01-01T00:00:00Z" } }
```

maps to `Stream-Expires-At`.

The two forms are mutually exclusive.

### Record retention

Per-record retention is not part of this creation schema unless a separate retention specification is implemented. Do not overload collection expiry to mean record-age trimming.

---

## 5. Encryption-key handling

The SDK accepts `encryptionKey` in the creation options for ergonomics, but the JSON document MUST NOT contain the secret.

Wire:

```text
Prisma-Encryption-Key: <key>
```

The server stores only the stream-epoch-bound fingerprint and required derived observation capability material.

The same key is attached to the returned stream handle. Account authorization and record encryption remain separate credentials.

---

## 6. Watch configuration

`watches` is normalized and immutable per stream incarnation.

Normalization:

- watch names compared byte-for-byte after UTF-8 validation;
- names sorted for config hashing;
- field order inside one definition remains significant;
- JSON pointers use canonical escaping;
- duplicate names rejected;
- watches rejected for non-JSON formats.

The watch definitions are stored in the parent descriptor and do not create separate descriptor PUTs.

---

## 7. Idempotent normalization

The server computes:

```text
config_hash = hash(normalized protocol config + normalized watch config)
```

Normalized protocol config includes:

```text
content type
idle TTL or absolute expiry
initial sealed state (product create always open)
```

Equivalent duration spellings normalize to the same integer seconds. Media types normalize case and parameters according to existing protocol comparison rules.

A retry after a lost create response returns `200` when the normalized hash matches.

---

## 8. No initial records in product create

The product route creates configuration only. It does not treat its JSON body as stream content.

Users append explicitly after creation:

```ts
const stream = await client.createStream(...)
await stream.append(...)
```

This makes resource configuration unambiguous and keeps create retries independent from append retries.

The raw Durable Streams standards endpoint permanently retains optional initial content and create-and-close behavior as required by the protocol.

---

## 9. Raw Durable Streams `PUT`

```text
PUT /v1/stream/{name}
```

continues to use protocol headers and optional initial body:

```text
Content-Type
Stream-TTL
Stream-Expires-At
Stream-Closed
Stream-Forked-From
Stream-Fork-Offset
Stream-Fork-Sub-Offset
```

Mapping:

- creates the same parent collection descriptor;
- configures the default-key Durable Stream view;
- creates the initial one-segment automatic map;
- does not require product JSON configuration;
- preserves protocol idempotence and error behavior.

A collection created by raw `PUT` may be opened by the product SDK as the same final-format resource. It has no watches. If watches are required, create the stream through the product route from the start or delete and recreate it before launch; no management update or migration operation is provided.

---

## 10. Forks

The raw standards behavior route supports the pinned Durable Streams fork contract.

A fork created through raw protocol forks the default-key Durable Stream view. Whole-collection, all-routing-key fork semantics are not invented implicitly.

A future product method may be explicit:

```ts
client.forkDefaultStream(source, target, options)
```

or a separate collection-fork specification may define how every key and segment lineage is shared. Stage 7 does not pretend these are the same operation.

---

## 11. Immutable versus mutable resources

Immutable collection config:

```text
content format/type
stream encryption identity
watch definitions
expiry policy for the incarnation
automatic routing model
```

Separate mutable resources:

```text
consumer groups
adapter catalog entries
operator limits/policies outside customer config
```

Changing immutable config requires deleting and recreating the stream under a new incarnation. No generic `PATCH` or in-place migration operation is introduced.

---

## 12. Clean creation-contract switch

The plural Prisma product route accepts only the final typed JSON creation document plus the namespaced encryption-key header.

Remove, rather than translate, legacy product creation inputs:

```text
Content-Type as product configuration
Stream-TTL as product configuration
Stream-Expires-At as product configuration
Stream-Touch-Templates
Stream-Profile
Stream-Ordering
Stream-Segments
Stream-Scaling
```

Rules:

- Conflicting old/new input handling is unnecessary because old product headers are rejected.
- No header-to-JSON translator, descriptor upgrader, config-version bridge, or deprecation window is implemented.
- The singular Durable Streams standards route continues to use protocol-defined headers and `PUT` semantics independently.
- The new server starts with a fresh namespace containing only normalized final descriptors.

## 13. Configuration validation

- stream name follows Stage 8 rules;
- content type is syntactically valid;
- duration is positive and within service maximum;
- absolute expiry is future RFC 3339;
- key is valid 32-byte base64/base64url;
- watch count, names, and JSON pointers are bounded;
- unknown JSON fields are rejected in v1 to catch typos;
- `configVersion` may be added when future evolution requires it.

Initial bounds:

```text
max watch definitions      = 64
max fields/watch           = 16
max config body            = 256 KiB
```

---

## 14. Durable Streams compliance

The product management route is additive. The singular protocol route retains exact `PUT` semantics, including:

- optional content type/default;
- idempotent matching;
- TTL/expiry;
- initial bytes;
- initial closed state;
- fork headers;
- response headers and status codes.

The shared descriptor comparison must satisfy both contracts without making the raw client understand Prisma watches or routing internals.

A raw idempotent `PUT` compares only protocol-defined configuration and initial closure/fork identity. Product-only immutable config that cannot be expressed by raw protocol must not cause a generic raw retry of an already-existing stream to fail unexpectedly. Therefore:

- raw-created streams have an empty product capability config;
- product-created streams expose the same protocol config to raw clients;
- raw `PUT` against a product-created stream compares protocol fields and succeeds when those match, while leaving product watches unchanged.

---

## 15. Correctness invariants

1. Lost create response is safely retryable.
2. The encryption secret never enters descriptor JSON/object storage.
3. Raw and product creation resolve to one stream incarnation, not duplicates.
4. Config normalization is deterministic across languages.
5. Watches are durable before the first append can depend on them.
6. Product create never accidentally stores its config as a record.
7. Raw protocol initial-body/fork/close behavior remains conformant.

---

## 16. Test plan

- normalized config equivalence;
- all duration/media-type edge cases;
- conflicting expiry forms;
- unknown-field rejection;
- wrong-key idempotent retry;
- lost response and concurrent creators;
- product create then raw `PUT`;
- raw `PUT` then product open;
- JSON watch validation;
- create crash after descriptor PUT/before response;
- fork and soft-delete conformance corpus;
- official create/TTL/expiry/closure conformance unchanged.

---

## 17. Exit criteria

```text
new product create headers for profile/order/segments  0
product config accidentally appended as data           0
normalized idempotence corpus                          pass
raw create/fork/TTL/close conformance                   pass
secret keys persisted in descriptor                    0
extra Class A request for initial segment map           0

```
