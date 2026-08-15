# Prisma Streams — Shared-Cell Multitenancy Implementation Plan

**Status:** FROZEN CONTRACT (Stage 0, committed 2026-08-15; revision 1, 2026-08-15 — see Revision log)
**Author:** Søren Bramer Schmidt (implementation plan, delivered 2026-08-15)
**Target:** Many projects in each Prisma Streams cell
**Migration posture:** Clean layout switch; no mixed-layout operation

> Repo anchors: the current single-tenant layout constant lives in
> `src/registry.rs` (`LAYOUT_VERSION`); stream-name canonicalization in
> `src/product.rs` (`canonical_name`). The conversion inventory for
> Stages 2–4 is maintained in `docs/MULTITENANCY-MAP.md`. The tenancy
> decision record is in `docs/READINESS.md`. Changes to THIS document
> after Stage 0 require an explicit contract-revision commit — it is
> the arbiter when implementation questions arise.

**Revision log**
- r1 (2026-08-15): added the `route-child-v1` domain for split-child
  placement (the conversion map found scaler split-child routes uncovered
  by the original five domains); froze the ProjectId/WorkspaceId/cell-id
  grammar; replaced ambiguous empty-prefix-set semantics with the
  explicit `StreamGrant` type.

---

## 1. Locked architectural decisions

### 1.1 Stable and mutable identity

The stable data identity is:

```text
project_id
+ stream_name
+ stream_epoch
```

The mutable commercial and authorization identity is:

```text
workspace_id
+ project_id
+ ownership_version
```

Consequences:

* `project_id` appears in registry paths, route hashes, storage hashes, segment identities, cursors, consumer state, usage rows, and internal RPC targets.
* `workspace_id` does **not** appear in immutable storage identities.
* A project transfer between workspaces changes authorization and billing attribution without rewriting stream data.
* "Account" is reserved for external login identity and is not used as the Streams tenant type.

### 1.2 One access token authorizes exactly one project

Every Streams access token authorizes exactly one project.

Do not say "a user receives one token per project." Users, CI systems, and service accounts may hold several credentials, but each resulting access token has one `project_id`.

Human sessions, Management API tokens, and general workspace integration tokens are never accepted directly by the Streams data plane.

### 1.3 Fixed project-to-cell placement

A project has one active `cell_id`.

Workspace transfer does not move the project between cells. Project-to-cell migration is a separate future protocol and is not part of the first shared-cell GA.

### 1.4 Same-project references for GA

These remain project-local:

* forks;
* consumer DLQs;
* workflow-style parent/child references;
* watch capabilities;
* catalog cursors;
* stream usage lookups.

Cross-project references are rejected before any lifecycle intent or durable debt is created.

### 1.5 Clean layout generation

Introduce a new layout version and fresh namespaces.

```rust
pub const LAYOUT_VERSION: u32 = 4;
```

Do not add a compatibility reader for the current project-global/name-global layout. Preview data that must survive is exported and reimported with an explicit `project_id`.

---

## 2. Core identity types

Create `src/tenant.rs`.

```rust
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkspaceId(Arc<str>);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ProjectId(Arc<str>);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantStreamRef {
    pub project_id: ProjectId,
    pub name: Arc<str>,
}
```

`WorkspaceId` exists in authorization, policy, audit, and billing. It does not belong in `TenantStreamRef`.

Use validated constructors:

```text
workspace ID: 1–128 bytes
project ID:   1–128 bytes
cell ID:      1–128 bytes
stream name:  existing canonical stream-name rules
```

Frozen grammar (r1) for workspace, project, and cell ids — a strict
ASCII allowlist until a shared Control Plane parser exists, at which
point the shared parser supersedes it (it may only tighten, never
widen, this grammar):

```text
id      = 1*128( ALPHA / DIGIT / "_" / "-" )
```

Unicode whitespace, bidi controls, zero-width characters, and every
byte outside the allowlist are rejected at construction.

### 2.1 Canonical binary encoding

Do not concatenate identities with delimiters.

```rust
fn append_component(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}
```

Domain-separate all hashes:

```text
route-v1
route-child-v1
storage-v1
segment-v1
catalog-cursor-v1
watch-capability-v1
```

Hash inputs:

```text
route hash:
  route-v1 + project_id + stream_name

storage hash:
  storage-v1 + project_id + stream_name + stream_epoch

segment identity:
  segment-v1 + project_id + stream_name + stream_epoch + segment_id

split-child route hash (r1; scaler-minted placement of a child segment):
  route-child-v1 + project_id + stream_name + child_segment_id + salt
```

Workspace transfer must not change any of these values.

---

## 3. Durable Streams credentials

Add a Control Plane model:

```text
StreamsCredential
```

Recommended fields:

```text
id
project_id
workspace_id_at_creation       // audit only, not authoritative ownership
display_name
secret_hash or public_key
scope
stream_prefixes
grant_version
status
expires_at
created_by_user_id
created_at
updated_at
last_used_at
value_hint
```

Suggested statuses:

```text
active
disabled
revoked
expired
```

### 3.1 Credential management

Creation, rotation, listing, and revocation belong in the Management API and Console.

The Control Plane actor must:

1. be authenticated through the existing Control Plane model;
2. have access to the project's current workspace;
3. have permission to manage Streams credentials.

Recommended initial role policy:

```text
workspace admin:      allowed
workspace developer:  allowed
workspace viewer:     denied
workspace accountant: denied
```

This policy should be explicit and independently changeable from Streams data-plane scopes.

### 3.2 Secret presentation

A credential secret is shown once.

Suggested format:

```text
prisma_streams_<credential-id>.<secret>
```

Store only:

* credential metadata;
* a secure secret hash or public key;
* a short value hint.

Do not store recoverable plaintext secrets.

### 3.3 Token exchange

The durable credential is exchanged with Prisma Auth for a short-lived Streams access token.

```http
POST /v1/token/streams
Authorization: StreamsCredential <durable-secret>
```

The exchange service:

1. resolves the credential;
2. verifies its secret;
3. verifies that the credential is active;
4. loads the current project policy;
5. verifies the project's current workspace ownership;
6. mints a 10–15 minute access token;
7. updates `last_used_at` asynchronously.

The Streams cell never receives the durable credential secret.

---

## 4. Versioning model

Use three distinct versions.

### 4.1 `project_policy_version`

Changes whenever project placement, status, quota, or policy data changes.

It is used for:

* cache ordering;
* observability;
* stale-policy detection.

A token does not authorize against an older policy. The cell always applies the newest locally cached quota and status.

### 4.2 `ownership_version`

Changes whenever the project's workspace owner changes.

A request requires:

```text
token.ownership_version
==
cached_project.ownership_version
```

This invalidates all pre-transfer access tokens immediately after the new policy reaches the cell.

### 4.3 `grant_version`

Stored on `StreamsCredential`.

It changes whenever:

* scopes change;
* prefixes change;
* credential status changes;
* credential is rotated or revoked.

A request requires:

```text
token.grant_version
==
cached_credential.grant_version
and
cached_credential.status == active
```

Do not authorize with:

```text
token.policy_version <= cached.policy_version
```

That would allow an older token to retain a permission removed by a newer policy.

### 4.4 Revocation identity

Revocation primarily targets:

```text
credential_id
```

`jti` remains useful for audit and exceptional single-token revocation, but the system should not require publishing every short-lived token issuance.

---

## 5. Access-token contract

Use an OAuth-compatible string `scope` claim.

Illustrative token:

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
  "scope": "streams.records.read streams.records.append streams.metadata.read",
  "stream_prefixes": ["customers/acme"],
  "jti": "tok_...",
  "iat": 1786600000,
  "nbf": 1786600000,
  "exp": 1786600900
}
```

`workspace_id` is checked against the current project policy. It is not used to locate storage.

Recommended constraints:

```text
default token lifetime:   10–15 minutes
maximum token lifetime:   24 hours
clock skew:               30 seconds
maximum token size:       8 KiB
kid required:             yes
algorithm allowlist:      explicit
```

The issuer intersects requested scopes and prefixes with the durable credential grant. The client cannot self-assert broader access.

---

## 6. Scope contract

Avoid broad scopes such as `STREAM_MANAGE` and `CONSUMER_USE`.

Use explicit scopes:

```text
streams.metadata.read
streams.records.read
streams.records.append
streams.create
streams.lifecycle.manage
streams.consumers.pull
streams.consumers.settle
streams.consumers.configure
streams.forks.create
streams.dlq.configure
streams.watches.manage
streams.catalog.read
streams.usage.read
```

### 6.1 Route and method matrix

| Operation                     | Required scope                |
| ----------------------------- | ----------------------------- |
| Read stream metadata          | `streams.metadata.read`       |
| HEAD/read/scan/SSE records    | `streams.records.read`        |
| Append/appendMany             | `streams.records.append`      |
| Create/recreate stream        | `streams.create`              |
| Seal/delete/update lifecycle  | `streams.lifecycle.manage`    |
| Consumer pull                 | `streams.consumers.pull`      |
| Consumer settle               | `streams.consumers.settle`    |
| Create/update/delete consumer | `streams.consumers.configure` |
| Create fork                   | `streams.forks.create`        |
| Configure DLQ                 | `streams.dlq.configure`       |
| Create/delete watch           | `streams.watches.manage`      |
| List project streams          | `streams.catalog.read`        |
| Read stream/project usage     | `streams.usage.read`          |

Fork creation must authorize:

```text
read access to source
+
create access to destination
+
streams.forks.create
```

DLQ configuration must authorize the source consumer and destination stream independently.

**Stage 5c (body-visible scopes) — status.** The request gate sees
only route + method + query, so scopes that depend on the request BODY
are enforced where the body is parsed. The gate now returns the
verified `RequestPrincipal` and the entry threads it to handlers:

* watch creation → `streams.watches.manage` — ENFORCED in
  `product_create` (watch definitions ride the PUT create body;
  `streams.create` alone no longer attaches them);
* DLQ configuration → `streams.dlq.configure` — ENFORCED in
  `product_consumer_put` beside `consumers.configure`;
* fork creation → `streams.forks.create` + read on the source —
  PENDING: forks are created on the raw DS surface, which is not yet
  principal-gated; lands with raw-surface enforcement.

The watch-observation route now returns ONE refusal
(`watch_unauthorized`) for every no-valid-capability shape — missing
stream, deleted stream, forged/expired capability — so the token-free
§15 route is not a stream-existence oracle; existence-revealing
answers (creating, unknown watch) come only after the capability or
key verifies. Stage 5d (review item 1) LANDED: the verified
principal's project selects the tenant-qualified storage identity on
the whole product surface — handlers, the shared append/read cores,
seal machinery (TenantStreamRef-keyed), catalog, usage, descriptors
born into the request's project — and the `principal.project == cell
tenant` bridge is DELETED. Off/shadow requests and §15 capability
carriers address the deployment tenant (single-tenant posture; the
capability wire gains the project in review item 3). Raw Durable
Streams routes still address the deployment tenant behind the
deployment bearer — the raw-surface principal decision is the
recorded follow-up. PROJECT_ID must be explicitly non-default in
shadow/enforce.

### 6.2 Prefix grants

Prefix grants are normalized when the credential is created.

Limits should include:

```text
maximum prefix count
maximum prefix length
canonical component separators
no empty or overlapping redundant prefixes
```

Grant semantics (r1): a credential's stream grant is an explicit type,
never an overloaded empty set —

```rust
enum StreamGrant {
    All,
    Prefixes(Arc<[CanonicalPrefix]>),
}
```

In the access token, an ABSENT `stream_prefixes` claim means
`StreamGrant::All` (every stream in the project, subject to scopes).
An empty `stream_prefixes` array is INVALID and the token is rejected:
"no streams" is expressed by not issuing the credential, and an empty
array is far more likely to be an issuer bug than an intent.

Prefix matching is component-aware:

```text
grant: customers/acme

matches:
  customers/acme
  customers/acme/orders

does not match:
  customers/acme-other
```

---

## 7. Data-plane authentication

Create `src/auth.rs`.

```rust
pub struct RequestPrincipal {
    pub workspace_id: WorkspaceId,
    pub project_id: ProjectId,
    pub cell_id: Arc<str>,
    pub credential_id: Arc<str>,
    pub subject: Arc<str>,
    pub ownership_version: u64,
    pub grant_version: u64,
    pub scopes: ScopeSet,
    pub stream_prefixes: Arc<[CanonicalPrefix]>,
    pub token_id: Arc<str>,
    pub issued_at: i64,
    pub expires_at: i64,
}
```

### 7.1 Local verification only

```rust
pub struct AuthService {
    issuer: String,
    audience: String,
    cell_id: String,
    jwks: ArcSwap<JwksSnapshot>,
    projects: ArcSwap<ProjectPolicySnapshot>,
    credentials: ArcSwap<CredentialGrantSnapshot>,
    last_jwks_refresh: AtomicI64,
    last_policy_refresh: AtomicI64,
}
```

No request performs a synchronous Control Plane lookup.

Background refresh:

```text
JWKS:              every 5 minutes
project policy:    streaming or every 30–60 seconds
credential grants: streaming or every 30–60 seconds
unknown kid:       asynchronous rate-limited refresh
```

A request fails closed when:

* JWT signature or claims are invalid;
* `ownership_version` differs;
* `grant_version` differs;
* credential is inactive;
* project is suspended/deleting/deleted;
* policy or credential data is unavailable beyond the accepted staleness window.

---

## 8. Gateway verification and routing

The gateway must verify the JWT **before** using any token claim for:

* destination-cell selection;
* quota partitioning;
* rate-limit buckets;
* internal target URLs.

Correct gateway sequence:

```text
verify signature and standard claims
→ read verified project_id
→ resolve current project placement
→ apply workspace/project quota
→ route to assigned cell
```

The cell repeats token and policy validation.

### 8.1 Wrong-cell behavior

A valid token sent to the wrong cell is not an authentication failure.

Preferred behavior:

```text
gateway reroutes internally
```

Fallback cell response:

```http
421 Misdirected Request
Prisma-Error-Code: wrong_cell
```

or a retryable `503 wrong_cell`.

Do not return `401`; that would cause clients to refresh a perfectly valid credential.

---

## 9. Request pipeline

The cell request path is:

```text
1. Parse the exact route and method.
2. Authenticate the customer, internal, operator, or watch capability.
3. Authorize scope and canonical prefix.
4. Load current project and credential policy from local memory.
5. Apply workspace/project concurrency backstops.
6. Enforce request body limits.
7. Construct TenantStreamRef(project_id, name).
8. Load the tenant-qualified descriptor.
9. Validate customer-held stream key where required.
10. Resolve routing key and exact physical segment.
11. Resolve ownership; non-owner returns replay.
12. Apply stream, shard, and cell safety limits.
13. Execute.
14. Meter and audit against project/workspace identity.
```

Authentication must occur before:

* body buffering or draining;
* tarpit work;
* object-store access;
* registry lookup;
* existence-revealing responses.

Handlers receive:

```rust
Extension(principal): Extension<RequestPrincipal>
```

and operate on `TenantStreamRef`, not bare names.

---

## 10. Tenant-qualified storage layout

### 10.1 Registry

Use:

```text
registry/v4/projects/
  <project-id-hex>/
  streams/
  <name-hex>.json
```

Registry cache keys become:

```rust
TenantStreamRef
```

Registry APIs accept project-qualified identities:

```rust
get(&TenantStreamRef)
create(StreamDesc)
update(&TenantStreamRef, ...)
mutate_incarnation(&TenantStreamRef, ...)
list_page(&ProjectId, after, limit)
```

After decoding, verify:

```text
descriptor.project_id == registry path project
descriptor.name == registry path name
```

Mismatch means corruption.

### 10.2 Descriptor

Make `project_id` mandatory:

```rust
pub project_id: ProjectId;
```

Do not persist mutable `workspace_id` in the immutable stream descriptor.

Remove all deployment-global tenant fallbacks.

### 10.3 Catalog and cursors

Project catalog scans only:

```text
registry/v4/projects/<project>/streams/
```

Catalog cursors bind:

```text
project_id
last_name
cursor_version
```

A cursor reused in another project returns `invalid_cursor`.

### 10.4 System resources

System streams live outside customer projects:

```text
system/v1/cells/<cell-id>/
```

This includes:

```text
_usage
_ops_events
_ops_metrics
_audit_events
```

Only internal workload identity may construct or access these resources.

---

## 11. Project policy

Create `src/project_policy.rs`.

```rust
pub enum ProjectStatus {
    Active,
    Suspended,
    TransferPending,
    Deleting,
    Deleted,
}

pub struct ProjectPolicy {
    pub project_id: ProjectId,
    pub workspace_id: WorkspaceId,
    pub cell_id: Arc<str>,
    pub project_policy_version: u64,
    pub ownership_version: u64,
    pub status: ProjectStatus,
    pub quotas: ProjectQuotas,
}
```

Credential grants are a separate cache:

```rust
pub struct CredentialGrant {
    pub credential_id: CredentialId,
    pub project_id: ProjectId,
    pub grant_version: u64,
    pub status: CredentialStatus,
    pub scopes: ScopeSet,
    pub prefixes: Arc<[CanonicalPrefix]>,
    pub expires_at: Option<i64>,
}
```

The data plane never infers workspace ownership from credential creation metadata. It uses the current `ProjectPolicy`.

---

## 12. Workspace transfer

Project transfer must not rewrite Streams data.

### 12.1 Transfer protocol

1. Mark project `TransferPending`.
2. Stop issuing new access tokens.
3. Increment `ownership_version`.
4. Change `workspace_id`.
5. Revoke all existing Streams credentials by default.
6. Publish the new project policy to the assigned cell.
7. Durably emit `ProjectOwnershipChanged` with:

   ```text
   project_id
   old_workspace_id
   new_workspace_id
   old_ownership_version
   new_ownership_version
   effective_at
   ```

8. Wait for the cell and usage rollup to acknowledge the policy/event.
9. Mark the project `Active`.
10. Allow the destination workspace to create new credentials.

The project's:

```text
registry paths
route hashes
storage hashes
stream epochs
segment identities
```

remain unchanged.

### 12.2 Billing across transfer

Segment billing metadata stores stable `project_id` and stream identity, not permanent workspace ownership.

Usage events include:

```text
project_id
workspace_id_at_event
ownership_version
event time
```

The usage rollup consumes `ProjectOwnershipChanged` and splits:

* ingest/read usage by event ownership version;
* stored byte-time at `effective_at`.

Monthly project artifacts contain a workspace-attribution breakdown. Workspace invoices aggregate only the portions attributable to that workspace.

Historical charges remain attached to the former workspace. New usage accrues to the new workspace.

---

## 13. Control Plane permissions versus Streams permissions

These are separate decisions.

### Control Plane decision

```text
May this actor create, rotate, or revoke a Streams credential for project P?
```

This uses:

* live user or service identity;
* live workspace membership;
* live project-to-workspace relation;
* Control Plane role.

### Streams decision

```text
What may credential C do inside project P?
```

This uses:

* credential scope;
* prefixes;
* grant version;
* project status;
* project quotas.

Do not encode workspace roles such as `admin` or `developer` into Streams access tokens.

---

## 14. Internal and operator authentication

### 14.1 Fleet identity

Do not use a permanent shared `FLEET_INTERNAL_TOKEN` for GA.

Use short-lived workload JWTs or Compute workload identity:

```json
{
  "aud": "prisma-streams-internal",
  "sub": "slot-or-instance-id",
  "cell_id": "fra-cell-07",
  "deployment_digest": "sha256:...",
  "operations": ["segment-read", "segment-scan"],
  "exp": 1786600300
}
```

For sensitive mutation operations, issue a short-lived delegated capability bound to:

```text
method
project ID
stream name
stream epoch
segment identity
operation ID
expiry
```

### 14.2 Operator identity

Operator/debug access requires:

* human or service identity;
* explicit operator role;
* short expiry;
* complete audit.

Customer tokens cannot access `/v1/debug/*`, and operator credentials cannot read customer record payloads unless a separately audited break-glass role permits it.

---

## 15. Watch capabilities

Treat watch capabilities as bearer credentials.

Preferred browser/client API:

```http
Authorization: Prisma-Watch <capability>
```

Use fetch-based streaming rather than URL credentials where possible.

If EventSource compatibility requires a URL capability, use a short-lived query parameter—not a path component—with:

```text
maximum lifetime: 5 minutes
Cache-Control: no-store
Referrer-Policy: no-referrer
strict log/trace redaction
exact audience and HTTP-method binding
project, stream epoch, watch, and expiry binding
```

A watch capability includes the stable `project_id`; it does not depend on the current workspace.

---

## 16. Internal RPC targets

Every internal operation carries:

```rust
pub struct InternalStreamTarget {
    pub project_id: ProjectId,
    pub stream_name: String,
    pub stream_epoch: String,
    pub segment_id: u32,
    pub segment_identity: [u8; 16],
}
```

The receiver:

1. verifies workload identity;
2. verifies delegated operation capability where required;
3. loads the project-qualified registry object;
4. verifies project, epoch, segment, and segment identity;
5. performs the operation.

Apply this to:

* segment reads;
* scans;
* consumer cursor probes;
* consumer deletion fan-out;
* lifecycle cleanup;
* telemetry operations.

Workspace ID is not part of the physical target.

---

## 17. Quotas and noisy-neighbor isolation

### 17.1 Quota levels

```text
cell safety
workspace quota
project quota
project concurrency
stream quota
shard maintenance
```

### 17.2 Gateway quotas

After verifying the token, consistently assign each project to one quota-router partition.

Enforce:

```text
workspace request rate
workspace ingest/read volume
project request rate
project append bytes and records
project read bytes
queue operations
subscriptions
```

The gateway uses the current project policy, not unverified claims.

### 17.3 Server backstops

Each instance maintains bounded project admission:

```rust
ProjectAdmission {
    inflight_requests,
    buffered_body_bytes,
    queued_append_bytes,
    live_subscriptions,
}
```

Acquire project capacity before reading the request body.

Reject only the offending project:

```text
429 project_rate_limit
429 project_concurrency_limit
503 project_tracker_capacity
```

Never merge unrelated projects into one overflow token bucket.

### 17.4 Fairness

Start with:

* gateway project quotas;
* server project concurrency;
* per-stream limits;
* existing cell safety.

Add weighted deficit round-robin inside shard committers only if noisy-neighbor testing proves that projects within limits can still starve each other.

---

## 18. Implementation stages

### Stage 0 — Freeze the contract

* [x] Commit `docs/MULTITENANCY.md`.
* [x] Lock project-stable/workspace-mutable identity.
* [x] Lock credential and token claims.
* [x] Lock scope matrix.
* [x] Lock transfer semantics.
* [x] Lock same-project-only references.
* [x] Lock layout 4 and clean switch.

**Exit:** no unresolved identity, permission, or transfer decisions.

### Stage 1 — Control Plane credential model

* [ ] Add `StreamsCredential`.
* [ ] Add create/list/rotate/revoke endpoints.
* [ ] Add role authorization.
* [ ] Add one-time secret return.
* [ ] Add token exchange.
* [ ] Add `grant_version`.
* [ ] Add transfer-time credential revocation.

**Exit:** automation can obtain short-lived project tokens without contacting a Streams cell.

### Stage 2 — Local token verification

* [ ] Add `tenant.rs`.
* [ ] Add `auth.rs`.
* [ ] Implement JWKS verification.
* [ ] Add project and credential caches.
* [ ] Implement exact ownership/grant version checks.
* [ ] Add separate customer/internal/operator audiences.
* [ ] Authenticate before body work.

**Exit:** every public request yields a verified project principal without remote lookup.

### Stage 3 — Layout 4 identity switch

* [x] Make descriptor `project_id` mandatory.
* [x] Change registry paths and caches.
* [x] Tenant-qualify route/storage/segment hashes.
* [x] Change catalog prefix and cursors. *(prefix + list APIs done; the
      opaque product-cursor project binding lands with Stage 4's
      handler conversion)*
* [x] Change consumer, producer, watch, and fork identities. *(all key
      through storage/segment hashes — converted transitively; fork
      refs stay project-local names under the same-project rule)*
* [x] Create fresh system namespaces. *(desc_path system/v1/cells root
      + reserved system project; billing's system streams move onto it
      in Stage 7)*
* [x] Add CI grep checks for name-only identity derivation.
      *(scripts/multitenancy-audit.sh, in the release gate)*

**Exit:** two projects can independently own an identically named stream.

### Stage 4 — Public and internal surface conversion

* [x] Convert raw and product handlers to `TenantStreamRef`. *(Stage 3
  switch: every handler resolves through `state.sref()` / registry
  refs; bare-name overloads deleted.)*
* [x] Scope catalog and usage. *(Catalog pages are project-prefixed
  since Stage 3; the project-usage route authorizes against the typed
  `AppState.tenant` — the parallel `project_id: String` field is
  deleted.)*
* [x] Convert internal RPC targets. *(Stage 4a:
  `streams-internal-project` on the wire; receivers rebuild registry
  identity from the header and verify it against the descriptor.)*
* [x] Implement same-project fork/DLQ checks. *(Review item 4: the
  DLQ compound rule is complete — source consumer authorization at the
  gate, `streams.dlq.configure` + the credential's PREFIX grant over
  the DESTINATION at config time. Fork's compound rule waits on the
  raw-surface principal.)* *(Stored references —
  `ForkRef.source`, `dead_letter_stream` — resolve exclusively through
  `StreamDesc::ref_in_project`, so a cross-project reference is
  unrepresentable rather than checked; isolation test drives a
  same-named same-fork-id foreign look-alike.)*
* [x] Replace watch URL secret design. *(Stage 4b: §15 short-lived
  fully-bound capabilities, `Prisma-Watch` / `cap=`; `sig=` retired;
  cross-language vector pinned.)*
* [x] Update SDK. *(Watch capability + project option with 4b;
  tokenProvider — lazy, single-flight, refresh-once-on-401 — plus
  typed WrongCellError (421/header fallback, never a refresh) and
  producer state scoped by project + endpoint; behavior pinned by
  sdk/scripts/auth-behavior.mjs.)*

**Exit:** the complete cross-project isolation matrix passes.
*(Covered at the testable-today layers: identity — same name, two
projects share no path/hash/byte; stored references — fork release +
DLQ binding against same-named foreign look-alikes; internal RPC —
foreign-project target refuses; watch capabilities — forged/expired/
cross-carrier negatives; auth §19 unit matrix. The live end-to-end
matrix under per-request principals lands with Stage 5 wiring and is
certified at Stage 8.)*

### Stage 5 — Policy, transfer, and placement

* [~] Publish project-policy snapshots/deltas. *(Data-plane side done:
  strict file-backed sources + independent-failure refresher publish
  into the arc-swap slots; the Control Plane feed implements the same
  `KeySource`/`PolicySource`/`GrantSource` traits when Stage 1 lands.
  Shadow mode observes every product bearer through the full customer
  pipeline — `/v1/debug/auth` reports counters + feed freshness.
  Enforce refuses boot until the Stage-5b route-scope matrix.)*
* [~] Publish credential-grant snapshots/deltas. *(Same: data-plane
  done via `STREAMS_AUTH_GRANTS_FILE`; platform feed pending.)*
* [x] Implement suspension. *(Data-plane: a non-`Active` `ProjectStatus`
  in the policy feed fails closed at authorization — 403
  `project_not_active` in enforce, counted in shadow. The transfer/
  deletion sagas that DRIVE status changes remain platform-side below.)*
* [ ] Implement transfer protocol.
* [ ] Implement project deletion saga.
* [ ] Implement verified gateway placement.

**Exit:** transfer changes workspace billing and authorization without changing stream storage identity. *(Enforce-mode gate — §6.1 matrix,
§6.2 prefix, §7.1/§8.1 response classes — is live and tested; transfer/
deletion sagas and the Control-Plane feed remain platform-side.)*

### Stage 6 — Quotas

* [ ] Implement gateway quota affinity.
* [ ] Add workspace/project rate limits.
* [x] Add server-side project concurrency. *(6a: src/quota.rs —
  per-project request-rate bucket (429 `project_rate_limit` +
  Retry-After) and inflight ceiling (429 `project_concurrency_limit`),
  acquired in the product wrapper after the enforce gate and BEFORE
  body buffering, quotas read from the CURRENT policy snapshot.)*
* [x] Remove overflow coupling. *(Projects never share a bucket; a
  full tracker refuses to TRACK new projects rather than merging.)*
* [x] Bound project trackers. *(1024-project cap; over it, NEW
  projects get 503 `project_tracker_capacity`, tracked ones are
  untouched.)*
* [x] Run noisy-neighbor campaigns. *(Mechanism level DONE, unblocked
  by Stage 5d: two projects on one enforce cell, a hostile flood
  (tight quotas, 300 flat-out appends) runs CONCURRENTLY with a paced
  compliant load — the flood is mostly refused with project-scoped
  429s, every compliant request succeeds, and compliant worst-case
  latency stays bounded. The at-scale binary campaign with real load
  and latency percentiles remains a field exercise for the release
  push. Bucket/tracker isolation is also pinned at the unit level. Volume dimensions are live:
  append bytes/records metered with exact parsed counts at the append
  site; read bytes debited POST-HOC from the served body size (sized
  bodies; refusal while in debt); SSE subscriptions hold a
  `max_live_subscriptions` slot for the STREAM's lifetime via a guard
  riding the response body. Still open: `queued_append_bytes` at the
  committer, `max_streams` at create (needs the Stage-7 per-project
  stream count).)*

**Exit:** one project cannot materially degrade a compliant neighbor.

### Stage 7 — Billing and audit

* [ ] Remove workspace/account fallbacks from billing identity.
* [ ] Emit ownership-change events.
* [ ] Split stored byte-time at transfer.
* [ ] Authorize usage through the project principal.
* [ ] Add `_audit_events`.
* [ ] Run same-name cross-project invoice tests.
* [ ] Run invoice reconciliation.

**Exit:** every billable unit belongs to exactly one project and one workspace-at-event.

### Stage 8 — Shared-cell certification

* [ ] Create at least 1,000 projects.
* [ ] Reuse stream names across projects.
* [ ] Run 32–64 active noisy projects.
* [ ] Keep hundreds of projects idle.
* [ ] Exercise splits, owner movement, suspension, revocation, and transfer.
* [ ] Reconcile operations and billing.
* [ ] Prove zero cross-project leakage.
* [ ] Complete external security review.

**Exit:** exact release-candidate binary passes the shared-cell GA battery.

---

## 19. Required tests

### Identity

* [ ] Same name in two projects yields different registry paths.
* [ ] Same name yields different route and storage hashes.
* [ ] Same producer ID and sequence are independent.
* [ ] Same consumer name is independent.
* [ ] Same routing key is independent.
* [ ] Workspace transfer changes none of these identities.

### Credential lifecycle

* [ ] Credential secret is shown once.
* [ ] Revoked credential cannot exchange a token.
* [ ] Old `grant_version` token is refused.
* [ ] Old `ownership_version` token is refused.
* [ ] Workspace transfer revokes existing credentials.
* [ ] New workspace can create a replacement credential.
* [ ] Durable credential secret is never accepted by the data plane.

### Authorization

* [ ] Missing/expired/not-yet-valid token.
* [ ] Wrong issuer/audience/cell.
* [ ] Missing scope.
* [ ] Prefix escape attempts.
* [ ] Suspended project.
* [ ] Customer credential against debug/internal routes.
* [ ] Internal identity against public product routes.
* [ ] Operator identity without break-glass access.

### Cross-project isolation

For projects A and B, both containing `orders`:

* [ ] metadata;
* [ ] append;
* [ ] read;
* [ ] scan;
* [ ] SSE;
* [ ] consumers;
* [ ] seal/delete/recreate;
* [ ] catalog;
* [ ] usage;
* [ ] cursors;
* [ ] watches;
* [ ] forks;
* [ ] DLQs;
* [ ] split/merge;
* [ ] internal fan-out;
* [ ] owner handoff.

A request authorized for A must mutate and reveal zero state belonging to B.

### Transfer and billing

* [ ] Route/storage hashes remain unchanged after transfer.
* [ ] Old access token fails immediately after policy publication.
* [ ] Old durable credential cannot mint a new token.
* [ ] Ingest/read events after transfer use the new workspace.
* [ ] Stored byte-time splits exactly at `effective_at`.
* [ ] Old invoices retain pre-transfer charges.
* [ ] Project dashboard includes its full history.
* [ ] Workspace invoice includes only its ownership interval.

### Noisy neighbor

* [ ] Project A saturates append quota; B stays within SLO.
* [ ] A saturates reads; B stays within SLO.
* [ ] A consumes its SSE allowance; B can connect.
* [ ] A reaches tracker capacity; B remains admitted.
* [ ] A triggers shard maintenance pressure; calm projects continue unless a real cell-wide limit engages.

---

## 20. GA acceptance criteria

Shared-cell GA is ready only when:

```text
No deployment-global customer bearer remains.
No deployment-global customer workspace/project identity remains.
Every registry and storage identity is project-qualified.
Workspace transfer requires no data rewrite.
Every token is exact-version checked against project and credential state.
The gateway verifies before routing and quota selection.
Customer, fleet, operator, and watch credentials have separate trust boundaries.
Same-name cross-project isolation passes on every surface.
Project and workspace quotas are active.
One noisy project cannot collapse another project's SLO.
Token rotation, revocation, suspension, and transfer pass.
Billing correctly splits ownership at project transfer.
The exact release binary passes the shared-cell field battery.
An external security review is complete.
```

## 21. Fastest critical path

```text
1. Control Plane StreamsCredential + token exchange
2. Data-plane principal middleware
3. Layout-4 project-qualified registry and hashes
4. Public/internal route conversion
5. Project policy, ownership version, and transfer
6. Gateway and server quota isolation
7. Cross-project security battery
8. Billing transfer attribution
9. Shared-cell field certification
```

Do not block this program on:

* generic Compute autoscaling;
* cross-project forks;
* live cell migration;
* multi-project access tokens;
* the full deterministic simulator;
* `ReadIoMetrics`.

Those can follow GA.
