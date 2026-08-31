# Prisma Streams — Control Plane and Console Integration

**Status:** Proposed integration contract  
**Audience:** Prisma Control Plane, Prisma Auth, Console, gateway, Streams cell, billing, and security teams  
**Depends on:** `docs/MULTITENANCY.md` revision 4  
**Purpose:** Turn the data-plane multitenancy contract into an implementable platform plan, with one reference implementation and one end-to-end test battery.

> This document proposes API paths and component boundaries. The exact public Prisma API naming should be aligned with existing Control Plane conventions before it is made normative.

---

## 1. System boundary

```text
Console / Management API clients
            |
            v
   Prisma Control Plane
   - project ownership and placement
   - StreamsCredential CRUD
   - project status and quotas
   - transfer and deletion sagas
            |
            +------------------------+
            |                        |
            v                        v
       Prisma Auth              Feed projector
       token exchange           JWKS / policies / grants
            |                        |
            v                        v
    short-lived project JWT      assigned Streams cell
            |                        ^
            v                        |
        public gateway -------------+
        verify -> quota -> route

Compute workload identity ---> short-lived fleet JWT ---> cell-to-cell RPC

Streams usage/audit events ---> rollup/billing ---> project and workspace views
```

The Control Plane owns mutable commercial and authorization state. The Streams cell owns project-qualified storage and repeats every authorization check locally. No customer request performs a synchronous Control Plane lookup.

---

## 2. Ownership by component

| Concern | Authoritative owner | Consumers |
|---|---|---|
| Project → workspace ownership | Control Plane | Auth, feed projector, billing, Console |
| Project → cell placement | Control Plane | Gateway, project-policy feed, Console |
| Durable Streams credentials | Control Plane | Prisma Auth, Console, grant feed |
| Customer access-token signing | Prisma Auth | Gateway and cell JWKS verifiers |
| Workload-token signing | Compute identity / Prisma Auth | Streams cells |
| Project policy and quotas | Control Plane | Gateway and assigned cell |
| Immutable stream identity and data | Streams cell | Streams API and billing rollup |
| Usage and denial journals | Streams cell | Billing, security, support tooling |
| Workspace transfer orchestration | Control Plane | Auth, feed projector, billing, cell |
| Project deletion orchestration | Control Plane | Feed projector, cell, billing |

---

## 3. Non-negotiable invariants

1. One customer access token authorizes exactly one `project_id`.
2. Human sessions, Management API tokens, workspace integration tokens, and durable Streams credential secrets are never accepted by a Streams cell.
3. `project_id + stream_name + stream_epoch` is stable data identity.
4. `workspace_id + ownership_version` is mutable authorization and billing identity.
5. A workspace transfer never rewrites registry paths, stream hashes, segment identities, cursors, or records.
6. The gateway verifies a token before reading `project_id`, selecting a cell, or choosing a quota bucket.
7. The cell verifies the token and exact current project/grant versions again.
8. Control Plane feeds are complete full snapshots. Omission means removal, not pagination or temporary absence.
9. Every feed generation is immutable: the same generation must always have the same canonical digest.
10. A retired JWKS `kid` is never reused.
11. Release posture contains no deployment-global customer bearer and no static fleet bearer.

---

## 4. Control Plane data model

### 4.1 `StreamsCredential`

Recommended fields:

```text
id
project_id
workspace_id_at_creation       // audit only
created_by_actor_id
created_by_actor_type

display_name
secret_hash                    // never recoverable plaintext
value_hint                     // e.g. last 6 characters

scopes                         // normalized set
stream_prefixes                // null = all, otherwise normalized set
grant_version
status                         // active | disabled | revoked | expired
expires_at

created_at
updated_at
last_used_at
rotated_at
revoked_at
```

The live project-to-workspace relation is authoritative. `workspace_id_at_creation` must never be used to authorize or attribute current usage.

### 4.2 Project Streams policy

```text
project_id
workspace_id
cell_id
project_policy_version
ownership_version
status
quotas
```

Planned quota field (round-12 decision, 2026-08-31): the per-project
LiveFeed retention allowance becomes policy-driven as

```text
ProjectQuotas {
    max_sse_retained_bytes: u64,   // bounded above by the cell
    ...                            // profile's 32 MiB maximum
}
```

Until then the allowance is the instance-profile backstop
(`SSE_FEED_PROJECT_BYTES` in deploy/profiles/compute-1g.env); the
operator contract and alert thresholds live in docs/LIVE-FEED.md
("Per-project retention allowance").

Supported status values:

```text
active
suspended
transfer_pending
deleting
deleted
```

Supported cell quota fields currently include:

```text
requests_per_sec
append_bytes_per_sec
append_records_per_sec
read_bytes_per_sec
max_inflight_requests
max_live_subscriptions
max_streams
queued_append_bytes
```

A quota value of `0` means that no project-level limit is configured at that layer; cell safety limits still apply.

### 4.3 Version rules

`project_policy_version` increments whenever status, placement, quota, or other policy content changes.

`ownership_version` increments whenever workspace ownership changes.

`grant_version` increments whenever credential status, secret, scopes, prefixes, or expiry changes.

A version pins semantic content. Replaying identical content at the same version is valid. Publishing different content at the same version is invalid.

---

## 5. Management API

The following paths are proposed. They should be adapted to existing Prisma project-resource conventions.

### 5.1 Create a credential

```http
POST /v1/projects/{project_id}/streams/credentials
Content-Type: application/json
```

```json
{
  "displayName": "production ingest",
  "scopes": [
    "streams.records.append",
    "streams.metadata.read"
  ],
  "streamPrefixes": ["events/production"],
  "expiresAt": null
}
```

Authorization uses live Control Plane identity, workspace membership, project ownership, and the independent permission to manage Streams credentials.

Initial role policy:

```text
workspace admin      allow
workspace developer  allow
workspace viewer     deny
workspace accountant deny
```

Successful response:

```json
{
  "credential": {
    "id": "strcred_123",
    "projectId": "proj_456",
    "displayName": "production ingest",
    "scopes": ["streams.records.append", "streams.metadata.read"],
    "streamPrefixes": ["events/production"],
    "grantVersion": 1,
    "status": "active",
    "valueHint": "…n7K2pQ",
    "createdAt": "2026-08-19T05:00:00Z"
  },
  "secret": "prisma_streams_strcred_123.<one-time-secret>"
}
```

The `secret` member is returned exactly once and is never present in list or get responses.

### 5.2 List credentials

```http
GET /v1/projects/{project_id}/streams/credentials
```

Return metadata only. Include status, grant, value hint, created/updated/last-used timestamps, and actor identity. Never return a recoverable secret or secret hash.

### 5.3 Update a grant

```http
PATCH /v1/projects/{project_id}/streams/credentials/{credential_id}
```

Any change to status, scopes, prefixes, or expiry increments `grant_version` transactionally with the change.

### 5.4 Rotate a secret

```http
POST /v1/projects/{project_id}/streams/credentials/{credential_id}/rotate
```

Rotation:

1. generates a new secret;
2. replaces the stored secret hash;
3. increments `grant_version`;
4. returns the new secret once;
5. invalidates every previously minted token after the grant feed reaches the cell.

### 5.5 Revoke or disable

```http
POST /v1/projects/{project_id}/streams/credentials/{credential_id}/revoke
POST /v1/projects/{project_id}/streams/credentials/{credential_id}/disable
```

Both increment `grant_version`. Revocation is terminal unless a later product decision explicitly permits reactivation at a strictly newer version.

---

## 6. Prisma Auth token exchange

### 6.1 Request

```http
POST /v1/token/streams
Authorization: StreamsCredential prisma_streams_strcred_123.<secret>
Content-Type: application/json
```

Optional body:

```json
{
  "requestedScopes": ["streams.records.append"],
  "requestedStreamPrefixes": ["events/production/orders"]
}
```

The requested authority may only narrow the stored credential grant.

### 6.2 Exchange procedure

Prisma Auth must:

1. parse and resolve the credential ID;
2. verify the durable secret using a password-secret-grade hash;
3. load the live credential row;
4. verify `status == active` and expiry;
5. load the current project and workspace owner;
6. verify that the project is active;
7. intersect requested scopes and prefixes with the stored grant;
8. resolve the project’s active `cell_id`;
9. mint a short-lived Streams customer JWT;
10. update `last_used_at` asynchronously and without delaying issuance.

### 6.3 Response

```json
{
  "accessToken": "eyJ…",
  "tokenType": "Bearer",
  "expiresIn": 900,
  "projectId": "proj_456",
  "endpoint": "https://streams.prisma.io"
}
```

Prefer a stable gateway endpoint. Direct cell endpoints should not be part of the customer contract unless required for a private deployment mode.

### 6.4 Required customer claims

```json
{
  "iss": "https://auth.prisma.io",
  "aud": "prisma-streams-data",
  "sub": "service-account-or-user-id",
  "credential_id": "strcred_123",
  "project_id": "proj_456",
  "workspace_id": "ws_789",
  "cell_id": "fra-cell-07",
  "ownership_version": 12,
  "grant_version": 7,
  "scope": "streams.records.append streams.metadata.read",
  "stream_prefixes": ["events/production"],
  "jti": "tok_…",
  "iat": 1786600000,
  "nbf": 1786600000,
  "exp": 1786600900
}
```

Default lifetime should be 10–15 minutes. Maximum lifetime accepted by the cell remains a safety ceiling, not the issuer’s normal lifetime.

### 6.5 SDK integration

```ts
const credential = process.env.PRISMA_STREAMS_CREDENTIAL!;

const client = new StreamsClient({
  url: "https://streams.prisma.io",
  project: "proj_456",
  tokenProvider: async () => {
    const response = await fetch("https://auth.prisma.io/v1/token/streams", {
      method: "POST",
      headers: {
        authorization: `StreamsCredential ${credential}`,
      },
    });
    if (!response.ok) throw new Error(`token exchange failed: ${response.status}`);
    return (await response.json()).accessToken;
  },
});
```

The SDK refreshes once after `401`. It must not refresh on `wrong_cell`; placement is resolved through the gateway or project endpoint discovery.

---

## 7. Project-policy, credential-grant, and JWKS feeds

### 7.1 Recommended production interface

Expose one authenticated internal snapshot endpoint per feed and cell assignment:

```http
GET /internal/streams/cells/{cell_id}/jwks
GET /internal/streams/cells/{cell_id}/project-policies
GET /internal/streams/cells/{cell_id}/credential-grants
```

Recommended response headers:

```text
ETag: "<canonical-sha256>"
Prisma-Streams-Feed-Generation: <u64>
Prisma-Streams-Feed-Digest: sha256:<hex>
Cache-Control: no-store
```

The cell may poll with `If-None-Match`, or a local sidecar may project responses into atomic files consumed by the current file-backed sources.

### 7.2 Full-snapshot contract

For each feed:

1. A snapshot is complete for the assigned cell.
2. It is never a page, partial result, or best-effort query response.
3. Omission means removal.
4. Any semantic change increments the global feed generation.
5. The same generation always has the same canonical digest.
6. Entries are canonically sorted before hashing and publishing.
7. Publication is atomic. A reader sees either the previous complete generation or the next complete generation, never an in-place partial write.
8. Feed generations never regress.
9. Per-project and per-credential versions obey the rules in §4.3.

For file projection, write to a temporary file, flush it, and atomically rename it over the previous file. Never truncate and rewrite the live file.

### 7.3 Policy snapshot shape

```json
{
  "feed_version": 42,
  "projects": [
    {
      "project_id": "proj_456",
      "workspace_id": "ws_789",
      "cell_id": "fra-cell-07",
      "project_policy_version": 9,
      "ownership_version": 3,
      "status": "active",
      "quotas": {
        "requests_per_sec": 1000,
        "append_bytes_per_sec": 10485760,
        "append_records_per_sec": 10000,
        "read_bytes_per_sec": 52428800,
        "max_inflight_requests": 100,
        "max_live_subscriptions": 100,
        "max_streams": 10000,
        "queued_append_bytes": 67108864
      }
    }
  ]
}
```

Only projects assigned to this cell are present.

### 7.4 Credential-grant snapshot shape

```json
{
  "feed_version": 87,
  "credentials": [
    {
      "credential_id": "strcred_123",
      "project_id": "proj_456",
      "grant_version": 7,
      "status": "active",
      "scopes": "streams.records.append streams.metadata.read",
      "stream_prefixes": ["events/production"],
      "expires_at": null
    }
  ]
}
```

`stream_prefixes` absent means all streams. An empty array is invalid and must fail publication.

### 7.5 JWKS snapshot shape and lifecycle

```json
{
  "feed_version": 12,
  "keys": [
    {
      "kid": "streams-rs256-2026-08",
      "alg": "RS256",
      "pem": "-----BEGIN PUBLIC KEY-----…"
    }
  ]
}
```

Rules:

1. New key material always receives a new `kid`.
2. A removed `kid` is permanently retired and is never reused.
3. Rotation publishes an overlap generation containing old and new keys.
4. Prisma Auth begins signing with the new key only after the overlap generation is available to gateways and cells.
5. The old key is removed only after the longest accepted token lifetime plus propagation margin.
6. An unknown `kid` triggers an asynchronous, rate-limited refresh; requests still fail closed until the key arrives.

### 7.6 Refresh and readiness

Recommended operational targets:

```text
policy/grant normal refresh       <= 30 seconds
policy/grant fail-closed window   300 seconds
JWKS normal refresh               <= 5 minutes
unknown-kid refresh start         <= 1 second
```

The cell should expose feed generation, digest, age, last success, last error, and stale state. Platform rollout tooling must wait until the target cell acknowledges the expected generation before completing security-sensitive transitions.

---

## 8. Workload identity for fleet and raw-internal traffic

### 8.1 Token shape

```json
{
  "iss": "https://auth.prisma.io",
  "aud": "prisma-streams-internal",
  "sub": "compute-slot-or-instance-id",
  "cell_id": "fra-cell-07",
  "operations": [
    "segment-read",
    "segment-scan",
    "queue-cursor",
    "consumer-sweep",
    "telemetry-append"
  ],
  "nbf": 1786600000,
  "exp": 1786600300
}
```

Use a short lifetime, normally five minutes or less. Operations are exact; empty and unknown operations grant nothing.

### 8.2 Delivery to the cell

Current supported integration is a platform-rotated token file. Rotation must use atomic replacement and restrictive file permissions. The cell caches the token, rereads before expiry, and refreshes once after a peer `401`.

Release posture requirements:

```text
FLEET_AUTH_MODE=workload
WORKLOAD_TOKEN_FILE=<platform-managed path>
STREAMS_RELEASE_POSTURE=1
FLEET_INTERNAL_TOKEN must be absent
```

A static fleet token is a local or migration posture only and must not coexist with workload mode in a release deployment.

### 8.3 Sensitive mutation capabilities

For sensitive internal mutations, the long-term contract should add a delegated capability bound to:

```text
operation
method
project_id
stream_name
stream_epoch
segment_identity
operation_id
expiry
```

The receiving cell verifies both workload identity and the delegated target capability.

---

## 9. Gateway integration

The public gateway sequence is normative:

```text
verify signature, issuer, audience, time, and token size
→ read verified project_id
→ load current placement and workspace/project quota state
→ assign the project to a stable quota-router partition
→ route to current cell
→ cell repeats full token and policy verification
```

The gateway must not use unverified claims for routing, target URL construction, or quota selection.

A valid token routed to the wrong cell is not an authentication failure. The gateway should reroute internally. If the request reaches the wrong cell directly, preserve the cell’s `421 wrong_cell` response rather than converting it to `401`.

The exact project-wide owner for `max_streams` must be decided explicitly. Recommended options:

1. a durable project counter/reservation in the Streams registry; or
2. one gateway quota partition that serializes creates for a project.

A process-local per-instance count is only a safety backstop, not an exact project-wide quota in a multi-instance cell.

---

## 10. Workspace transfer saga

Recommended state machine:

```text
Active(old owner)
  -> TransferPending
  -> ownership changed + credentials revoked
  -> policy/event acknowledged by cell and billing
  -> Active(new owner)
```

Procedure:

1. Transactionally mark the project `transfer_pending` and stop token exchange.
2. Increment `ownership_version`.
3. Change `workspace_id`.
4. Revoke all existing Streams credentials by default, incrementing their grant versions.
5. Publish the new project-policy and credential-grant generations.
6. Emit durable `ProjectOwnershipChanged` with old/new workspace and version plus `effective_at`.
7. Wait for cell feed acknowledgement and billing-rollup acknowledgement.
8. Mark the project active.
9. Permit the new workspace to create replacement credentials.

Failure recovery must be idempotent at every step. Storage identity never changes.

---

## 11. Project deletion saga

Recommended state machine:

```text
Active -> Deleting -> Deleted
```

Procedure:

1. Stop token exchange and mark the project `deleting`.
2. Revoke all credentials and publish the feed.
3. Wait for authorization cutoff acknowledgement.
4. Ask the assigned cell to run project deletion using authenticated internal identity and an idempotent operation ID.
5. Reconcile stream registry, storage closure, usage, and audit artifacts.
6. Mark the project `deleted` only after cell and billing acknowledgement.
7. Retain the deletion audit record and version tombstone.

The first implementation may retain data according to a defined recovery period, but authorization cutoff must occur at the start of the saga.

---

## 12. Console experience

Add a **Streams** area under each project.

### 12.1 Overview

Show:

```text
status
region / cell placement (support-safe label, not internal URL)
number of streams
current quota plan and observed usage
feed / authorization health when relevant to support
SDK quick start
```

Do not expose fleet tokens, signing keys, internal cell URLs, or customer record contents.

### 12.2 Credentials

Credential table columns:

```text
display name
value hint
status
scopes
stream prefixes
created by / created at
last used at
expires at
```

Actions:

```text
Create
Rotate
Disable / enable if supported
Revoke
```

Creation flow:

1. Enter a display name.
2. Choose a preset or advanced scopes.
3. Optionally restrict stream prefixes.
4. Optionally set expiry.
5. Review effective authority.
6. Create.
7. Display the secret once with copy and download-to-secret-manager guidance.
8. Display a generated SDK snippet using `tokenProvider`.

The secret modal must clearly state that closing it permanently hides the value.

### 12.3 Safety UX

- Warn before granting lifecycle, consumer-configuration, or all-stream authority.
- Normalize and preview component-aware prefixes.
- Do not offer an empty prefix set; it grants nothing and is rejected by the feed parser.
- Require explicit confirmation for revocation and rotation.
- Explain that rotation invalidates existing short-lived tokens after propagation.
- During transfer or deletion, make credential creation unavailable and show the project state.

### 12.4 Audit UX

Expose Control Plane credential-management events without exposing data-plane record payloads:

```text
credential created
credential rotated
scopes/prefixes changed
credential disabled/revoked
project suspended
workspace transfer started/completed
project deletion started/completed
```

Link to data-plane denial and usage summaries only through authorized support or project views.

---

## 13. Rollout plan

### Phase A — Reference integration

- Land this contract and JSON schemas.
- Build the in-repo platform emulator described below.
- Exercise token exchange and full feeds against one real Streams process.
- Make the SDK smoke use the emulator’s token endpoint instead of a hand-minted static token.

### Phase B — Control Plane credential lifecycle

- Implement `StreamsCredential` storage and Management API.
- Implement one-time secret presentation and secret hashing.
- Implement Prisma Auth token exchange.
- Implement real policy/grant/JWKS projection.
- Run shadow mode and compare emulator and production feed output against the same contract tests.

### Phase C — Gateway and two-cell placement

- Verify JWT before routing.
- Implement project quota affinity.
- Run wrong-cell and owner-movement tests through a two-cell reference gateway.

### Phase D — Transfer, deletion, and billing

- Implement transfer and deletion sagas.
- Add ownership-change events and exact byte-time split.
- Run the exact release-binary end-to-end battery.

---

## 14. In-repository platform emulator

Build a small independent reference component in this repository. Its purpose is to make the platform contract executable; it is not a replacement for the production Control Plane.

### 14.1 Recommended implementation

Use TypeScript/Node so the producer side is independent from the Rust consumer and resembles the likely Control Plane integration environment.

```text
contracts/streams-platform/v1/
  keys.schema.json
  project-policies.schema.json
  credential-grants.schema.json
  customer-token-claims.schema.json
  workload-token-claims.schema.json
  management.openapi.yaml

platform-demo/
  package.json
  src/model.ts
  src/secrets.ts
  src/tokens.ts
  src/feed-projector.ts
  src/gateway.ts
  src/server.ts
  public/index.html

scripts/
  platform-e2e.mjs
```

Keep the demo dependency-light. Node’s built-in HTTP and crypto libraries are sufficient for the first version. Persist state to one local JSON database or SQLite if a dependency is acceptable. Persist only hashed durable secrets.

### 14.2 Demo capabilities

The first useful version should provide:

1. workspace and project fixtures;
2. project-to-cell placement;
3. credential create/list/rotate/revoke;
4. one-time durable secret return;
5. `/v1/token/streams` exchange and RS256 signing;
6. JWKS, policy, and grant snapshot endpoints;
7. atomic file projection for the cell’s current file sources;
8. workload JWT minting and atomic token-file rotation;
9. a minimal static Console page;
10. an optional verifying reverse proxy acting as the reference gateway.

### 14.3 Shared contract, independent implementations

Do not share token-minting or feed-serialization code directly with the Rust parser. Share schemas and golden vectors instead. Independent implementations are more likely to expose casing, canonicalization, omission, algorithm, and versioning bugs.

The Rust cell remains strict and authoritative for what it accepts. The demo validates its output against the language-neutral schemas before publication.

### 14.4 End-to-end test topology

```text
platform-demo
  ├─ Management API + Console
  ├─ Prisma Auth emulator
  ├─ feed projector
  ├─ workload-token rotator
  └─ reference gateway
          |
          +--> streams cell A
          +--> streams cell B
```

Tests start real processes and communicate only over public/internal HTTP and feed transport. Avoid direct calls to `AuthService::publish_*` in these tests.

### 14.5 Required emulator-driven tests

Credential lifecycle:

- secret shown once;
- wrong secret refused;
- token exchange succeeds for active credential;
- requested authority is intersected;
- rotation invalidates old grant version;
- revoke prevents exchange and cuts off old tokens after feed publication.

Feed contract:

- partial JSON never becomes visible;
- generation regression refused;
- same generation with different digest refused;
- omission tombstones project and credential;
- same per-ID version with different content refused;
- retired JWKS `kid` cannot return;
- unknown `kid` causes an immediate refresh attempt;
- stale feeds fail closed.

Placement and gateway:

- gateway verifies before routing;
- forged `project_id` cannot select a cell;
- wrong cell reroutes or preserves `421`;
- two projects with the same stream name remain isolated;
- owner movement during split/read/append converges.

Fleet identity:

- release posture refuses any static fleet token;
- operation-scoped workload JWTs are enforced;
- expired token refreshes exactly once;
- sensitive mutation target capability is enforced when implemented.

Quotas:

- rate, concurrency, read, append, and subscription limits;
- `max_streams` across two different cell instances;
- catalog failure during stream-count seed fails closed;
- customer names containing `#` count normally;
- terminal fork-cascade hard delete releases the stream slot;
- queued append bytes release on every success, refusal, cancellation, and shutdown path.

Transfer and deletion:

- no new token during `transfer_pending`;
- old ownership token fails after feed publication;
- storage identity remains unchanged;
- usage changes workspace at the ownership boundary;
- restart after every saga step converges without double billing;
- deletion cuts off authorization before storage cleanup.

---

## 15. CI and release use

Add a `platform-e2e` CI job that:

1. builds the exact release server binary;
2. starts the platform emulator;
3. creates a project and credential through the Management API;
4. obtains a token through the exchange endpoint;
5. creates, appends, reads, rotates, revokes, suspends, and transfers through the gateway;
6. rotates workload identity with no static fallback;
7. verifies billing and audit reconciliation;
8. records the server binary digest and emulator contract version.

The release-candidate promotion step should run in a clean detached worktree, verify all required named CI jobs, and attach the exact binary digest and outstanding external legs to a protected signed tag or release manifest.

---

## 16. Open decisions to resolve before this document becomes normative

1. Exact public Management API paths and error envelope.
2. Credential secret hash and secret-manager integration.
3. Stable public gateway endpoint versus endpoint discovery.
4. HTTP pull, streaming, or sidecar/file projection for production feeds.
5. Durable acknowledgement protocol for policy, ownership, and billing events.
6. Authoritative implementation of project-wide `max_streams` across cell instances.
7. Delegated target-capability issuer and verification format.
8. Operator identity and break-glass role integration.
9. Data-retention behavior during project deletion.
10. Console placement and role vocabulary in the existing Prisma UI.
