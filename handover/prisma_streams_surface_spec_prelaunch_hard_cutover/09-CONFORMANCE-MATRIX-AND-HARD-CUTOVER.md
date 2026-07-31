# Appendix — Durable Streams Conformance Matrix and Pre-launch Hard-Cutover Plan

**Purpose:** make protocol preservation executable across all eight stages. This appendix is not a ninth product stage.

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

## 1. Normative baseline

The implementation MUST pin and record:

```text
DURABLE_STREAMS_PROTOCOL_COMMIT
DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION
DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION
```

The authoritative external references are:

- [Durable Streams core concepts](https://durablestreams.com/concepts)
- [Building a Durable Streams server](https://durablestreams.com/building-a-server)
- [Building a Durable Streams client](https://durablestreams.com/building-a-client)
- [Durable Streams JSON mode](https://durablestreams.com/json-mode)
- [Durable Streams fork semantics](https://durablestreams.com/fork)
- the protocol specification and conformance packages at the pinned upstream commit

The pre-launch implementation pins one protocol baseline. A change to that baseline is a separate pull request with a conformance report; support for mixed baselines or data migration between them is out of scope for this clean switch.

---

## 2. Non-negotiable standards boundary

The singular route is the protocol product:

```text
/v1/stream/{name}
```

It exposes one strictly ordered Durable Stream: the collection's default routing-key sequence.

The plural route is the Prisma product:

```text
/v1/streams/{name}
```

It exposes a key-partitioned collection. It is an additive API, not a reinterpretation of a Durable Stream offset sequence.

This is not a user-selectable mode. There is one Prisma implementation and one permanent standards-conformant view over its default routing key.

The following MUST be true:

1. A generic Durable Streams client can use the singular route without Prisma-specific knowledge.
2. A product append without `routingKey` and a raw append target the same canonical default-key sequence.
3. Product key cursors and scan cursors are rejected by the raw endpoint.
4. Raw offsets are rejected by product key/scan APIs unless an explicit conversion operation is specified.
5. Product cross-key scans never emit `Stream-Next-Offset` and never claim global append order.
6. Prisma extensions never occupy or shadow upstream-reserved `__ds` paths.

---

## 3. Operation matrix

| Capability | Raw Durable Streams route | Prisma product route | Shared implementation requirement |
|---|---|---|---|
| Create | `PUT /v1/stream/{name}` with protocol headers/body | `PUT /v1/streams/{name}` with typed JSON config | One descriptor/incarnation; protocol and product config normalized independently |
| Append one | Protocol `POST`; JSON mode preserves upstream flattening rules | `POST /records`; exactly one product record | One committer and durability path |
| Append many | Protocol JSON array flattening where applicable | `POST /records:batch`; explicit atomic batch | One atomic storage batch; no extra object-store request |
| Read history | `GET ?offset=` | `GET /records?routingKey=&cursor=` | Same canonical frames/history tier; distinct cursor codecs |
| Live read | Protocol long-poll/SSE and `Stream-Cursor` | SDK `subscribe()` with transport hidden | Same durable notifications/tail ring; protocol headers retained only on raw route |
| Metadata | `HEAD` with protocol headers and `no-store` | `GET /v1/streams/{name}` typed metadata | One descriptor and lifecycle state |
| Seal/close | Protocol `POST` with `Stream-Closed: true` | `POST /v1/streams/{name}:seal` | One monotonic sealed bit; raw default-key view observes closure |
| Delete | Protocol `DELETE` | Product `DELETE` | One deletion/tombstone/fork-reference implementation |
| TTL/expiry | `Stream-TTL` / `Stream-Expires-At` | Typed `expiry` object | One normalized expiry policy |
| Idempotent producer | Raw `Producer-*` headers | Producer session abstraction | One durable producer-state machine; product state scoped by routing key |
| Raw sequence | `Stream-Seq` retained | Not exposed | Standards route only |
| Fork | Pinned protocol fork semantics for default-key stream | No implicit collection fork | Shared references only where semantics are explicitly defined |
| Upstream subscriptions | Reserved `__ds` routes when in baseline | Product consumer groups on plural route | Shared low-level lease primitives MAY be reused; wire contracts remain separate |
| Cross-key scan | Not available as one Durable Stream read | Explicit snapshot `scan()` | Product-only signed scan cursor; no global-order claim |

---

## 4. Raw protocol behaviors that may not regress

The official server conformance suite is the source of truth. At minimum, each release MUST preserve:

```text
idempotent create and configuration conflict behavior
byte-exact resumption
strictly increasing opaque offsets
Content-Type preservation and append validation
JSON message boundaries and one-level array flattening
empty JSON-array rejection where required
catch-up reads and up-to-date signaling
long-poll behavior, including immediate EOF at a closed tail
SSE framing and control events
HEAD metadata and Cache-Control: no-store
monotonic durable closure
atomic append-and-close
DELETE and soft-delete/fork-reference behavior
TTL and absolute-expiry semantics
ETag/conditional-read behavior
Stream-Cursor behavior used for live request collapsing
Stream-Seq standards behavior
Producer-Id / Producer-Epoch / Producer-Seq validation and duplicate handling
fork semantics in the pinned baseline
reserved subscription routes in the pinned baseline
```

No product-stage simplification may remove these raw standards behaviors.

---

## 5. Header and token namespaces

### Raw protocol

All protocol-defined headers retain their exact spelling and semantics, including:

```text
Content-Type
Stream-Next-Offset
Stream-Up-To-Date
Stream-Closed
Stream-Cursor
Stream-TTL
Stream-Expires-At
Stream-Seq
Producer-Id
Producer-Epoch
Producer-Seq
Stream-Forked-From
Stream-Fork-Offset
Stream-Fork-Sub-Offset
```

### Prisma product

Product-only headers MUST be namespaced and MUST NOT reuse protocol headers for different token classes:

```text
Prisma-Encryption-Key
Prisma-Routing-Key
Prisma-Cursor
Prisma-Next-Cursor
Prisma-Scan-Cursor
Prisma-Next-Scan-Cursor
```

JSON response fields MAY be preferred over headers, but the token classes remain distinct.

All token codecs MUST include an explicit kind/version discriminator and authenticated integrity protection where a client could otherwise alter routing or snapshot state.

---

## 6. Stage-by-stage conformance gates

| Stage | Raw-suite focus | Prisma extension focus | Required standards evidence |
|---|---|---|---|
| 1 — Profiles | Full base suite | Final descriptor and capability composition | No profile fields, translators, or raw header changes |
| 2 — Consumers/watches | Base plus pinned upstream subscriptions | Per-key consumer and watch suites | `__ds` paths unshadowed; no consumer/watch DB per resource |
| 3 — Routing/scaling/postings | Full base suite against a fresh default key | Per-key order, split/merge, postings cost | Standards offsets remain valid inside the new namespace; no cross-key order claim |
| 4 — Append APIs | JSON and append/close suites | Explicit one/batch semantics | Product array-as-value does not alter raw JSON flattening |
| 5 — Producers | Producer and `Stream-Seq` suites | Producer-session retry/split corpus | Raw duplicate/gap/stale statuses and headers match the pinned standard |
| 6 — Read APIs | Catch-up, long-poll, SSE, cache suites | Key read/subscribe/scan | Product cursor never appears as `Stream-Next-Offset` |
| 7 — Creation | Create, TTL, expiry, fork, close suites | Typed config/idempotence | Product-created default key is raw-readable in the fresh namespace |
| 8 — Naming/routes | Full lifecycle/security suite | Product route/error/name corpus | Singular standards route remains permanent; removed product routes are absent |

A stage cannot be declared complete when only the Prisma suite passes.

## 7. Required CI topology

Every pull request touching protocol or storage behavior runs:

```text
job: durable-streams-server-conformance
  target: /v1/stream/{generated-name}
  upstream tests: unmodified, pinned version

job: prisma-product-conformance
  target: /v1/streams/{generated-name}
  tests: product API and SDK

job: dual-surface-equivalence
  tests: default-key operations through both routes produce identical canonical data

job: dst-focused
  tests: crash/fence/retry/segment/consumer/watch scenarios

job: request-cost-budgets
  tests: physical Class A/Class B budgets for changed paths
```

Nightly and release candidates add:

```text
full deterministic seed corpus
100k+ routing-key/cardinality workload
long-running split/merge/GC soak
real HTTP client reset/truncation tests
```

The conformance server MUST start from a fresh namespace per test case. Tests must also cover idempotent retry against an existing namespace.

---

## 8. Dual-surface equivalence corpus

For the default routing key, execute equivalent operations through raw and product APIs in both orders.

### Required cases

1. Product create → raw append → product read.
2. Raw create → product append without routing key → raw read.
3. Raw producer append → product read.
4. Product producer append → raw read.
5. Product seal → raw closed-tail read.
6. Raw close → product metadata and consumer drain.
7. Product delete → raw gone response.
8. Raw delete → product gone response.
9. Raw TTL create → product metadata/expiry.
10. Product idle expiry → raw `HEAD`/read semantics.
11. Product-created JSON stream → raw JSON array flattening.
12. Raw fork of default-key stream → product opening of target without inventing collection-wide fork semantics.

For every case, compare:

```text
canonical record bytes/messages
offsets/cursors at their respective surfaces
sealed/deleted/expiry state
producer state
usage/billing counters
object-store request count
```

---

## 9. Pre-launch hard-cutover order

The eight workstreams are implementation dependencies, not customer-visible versions or a migration sequence. The only deployment is one coordinated destructive pre-launch switch.

1. Implement the final internal representation, final readers/writers, final SDK, and final routes on one branch.
2. Delete old product descriptors, codecs, route handlers, headers, aliases, profile branches, covering-index readers, and legacy fixtures.
3. Build and test against a fresh object-storage namespace.
4. Run official Durable Streams server/client conformance, Prisma product conformance, dual-surface equivalence, DST, request-cost, and wide-cardinality gates.
5. Deploy server and SDK together into a new pre-launch environment with a new bucket or `PATH_PREFIX`.
6. Do not run old and new binaries against the same namespace.
7. After validation, delete the old pre-launch environment and its disposable data.

### Fresh-layout rule

Only the final descriptor, segment, postings, consumer, watch, and cursor formats exist in the new namespace. There are no layout-version bridges, cutover markers, mixed secondary indexes, or old-format readers.

### Rollback rule

Rollback is environment-level only: restore the previous binary and its previous isolated namespace. Never point the previous binary at the new namespace, and never point the new binary at the previous namespace. No N/N-1 rolling or storage compatibility is required before launch.

## 10. Protocol baseline upgrades

A baseline-upgrade pull request MUST contain:

```text
old and new protocol commit/version
upstream changelog summary
new/changed headers and routes
server conformance before/after report
client conformance before/after report
reserved-path collision audit
fresh-layout impact
conformance decision
```

If a new upstream feature conflicts with a Prisma product route, Prisma moves its product route. The raw protocol surface and upstream-reserved path win.

---

## 11. Security and privacy gates

- Encryption keys never enter object storage or logs.
- Routing-key hashes in postings are non-authoritative; canonical frames are verified against exact routing-key bytes.
- Product cursors are authenticated and cannot be edited to cross tenant, stream incarnation, routing key, or snapshot bounds.
- Consumer lease tokens are generation-fenced and unforgeable.
- Watch observation URLs contain only the minimum capability required to observe invalidation.
- Raw protocol authentication remains independent of record-encryption credentials.
- Error bodies never echo keys, tokens, plaintext records, or internal object paths.

---

## 12. Cost gates

No stage may shift simplicity into hidden object-storage cost.

For changed paths, report:

```text
Class A requests / million records
Class A requests / GiB ingested
Class B requests / GiB delivered
bytes stored / canonical payload byte
background Class A / shard-hour
background Class B / shard-hour
memory per active stream/key/consumer/watch
CPU per record read/written
```

Hard invariants:

```text
per-routing-key database/manifest/object namespace      prohibited
per-consumer database/manifest/object namespace         prohibited
per-watch database/manifest/object namespace            prohibited
one canonical payload copy                              required
point GET per posting/record pattern                     prohibited
unbounded in-memory cardinality                          prohibited
periodic LIST per key/consumer/watch                     prohibited
```

---

## 13. Final release gate

The full simplification is releasable when:

```text
official pinned Durable Streams server suite             pass
pinned Durable Streams client conformance                 pass
Prisma product conformance                                pass
dual-surface equivalence                                  pass
removed product routes/fields/codecs absent               pass
fresh-namespace destructive cutover                       pass
DST safety and liveness corpus                            pass
postings storage/read COGS gate                           pass
consumer/watch cost gates                                 pass
wide-cardinality memory gates                             pass
real Compute + Tigris field campaign                      pass
```

The release report lists every raw protocol capability, the pinned baseline, and the final product surface. “Durable Streams compliant” may be claimed only for the singular standards route tested by the official suite; the plural Prisma collection API is documented as a separate product API.
