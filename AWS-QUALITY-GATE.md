# AWS-level managed-service quality gate

Status on 2026-07-18: **NOT READY**. The SlateDB rewrite is a strong pilot,
not yet an AWS-level multi-tenant service. This document is the release gate
for `slate-codex`; a green verdict requires evidence for every mandatory row,
not merely an implementation claim.

## Judgment standard

The service must remain correct and available under hostile tenant traffic,
process and instance loss, ownership races, object-store throttling/outages,
corrupt control-plane objects, deploy skew, and sustained operation at the
documented cell limits. A feature described only in `SPEC.md`,
`COMPUTE-SPEC.md`, or `OPERATIONS.md` does not count as implemented.

| Area | Mandatory exit evidence | Current verdict |
|---|---|---|
| Durability and ordering | crash/fence/timeout fault matrix; linearizable incarnation changes; no ACK before remote durability; contiguous atomic appends; bounded recovery | **Amber.** Incarnation CAS races, hard-restart create-with-body idempotence, durable producer dedupe, real two-writer fencing, split-brain post-durability ACK fencing, stale topology responses, corrupt immutable SST responses, and pre/post-commit timeout/429/5xx/412 faults are automated. Stale manifest/list injection and general measured recovery bounds remain. |
| Tenant isolation and authz | customer-scoped identity in every descriptor and request; locally verified scoped tokens; verb/prefix enforcement; no cross-tenant list/existence oracle; revocation; audit | **Amber.** Customer identity scopes registry, storage, routing, metrics, and requests. A real RS256/JWKS service drill proves identical-name isolation, per-tenant listing/non-disclosure, prefix/verb denial, live token revocation, and rollback-resistant revocation versions. Durable control-plane audit exists. An independent security review remains. |
| Encryption and key custody | independently reviewed envelope; canonical codecs; key zeroization/expiry; no persisted keys; recreate/rotation tests; ciphertext-at-rest inspection | **Amber.** Envelope is implemented. Canonical frame parsing and bounded/zeroized key caching are now tested; independent review and full at-rest tests remain. |
| Resource governance | per-stream and per-customer admission; fair committer scheduling; bounded queues, maps, caches, connections, response sizes, and background work; overload returns scoped 429/503 | **Amber.** Bounded, durable per-customer limit documents override concurrency/write-byte admission and enforce exact live stream-name counts through a cross-instance CAS lease plus authoritative recount. Persistent per-tenant round-robin commit scheduling looks past large requests; registry/key/stream/producer/consumer/touch/metric/audit/limit state is bounded. Remaining dimensions (read rates/bytes, connections, queue receives, per-stream weights), the complete structured 429 contract, and a measured noisy-neighbor gate remain. |
| Horizontal scaling | automatic split/merge with quiesce proof; fleet aggregation; cell placement/isolation; hot-key behavior; no global coordination bottleneck at target scale | **Amber.** Online split has a CAS-created shard-store intent, closed admission, remote-durability barrier, exact projection clones, generation-specific paths, one-CAS topology publish, renewable ownership, crash reconciliation, and a calibrated sustained-byte trigger. Concurrent producers, hard restart under a new identity, automatic refinement, separate role buckets, and a deliberately stale second owner are exercised. Merge, abandoned-generation GC, per-stream hot-share policy, and the global cell placement layer remain. |
| Availability and recovery | readiness distinct from liveness; stale-owner read guard; poison-shard quarantine; backup/PITR copy actor; restore and provider-failover drills with measured RPO/RTO | **Amber.** Readiness includes auth/revocation/audit/backup health; idle owners revalidate writer epoch within five seconds; repeated shard-open failures quarantine. Immutable checksummed snapshots and an empty-target restore are exercised end to end. Injected SST corruption fails without a success or partial plaintext and recovers from the untouched source. Incremental PITR, retention, continuous scrub, provider failover, and measured RPO/RTO remain. |
| Operability and SLOs | RED metrics by tenant/cell/shard; bounded-cardinality telemetry; actionable alerts; audit trail; capacity model; on-call runbooks exercised by game days | **Amber.** Tenant-scoped bounded metrics and immutable durable audit records exist, with audit health in readiness. Alert automation, retention/export, and game-day evidence remain. |
| Verification and release | hermetic unit/integration/property/chaos/soak suites; conformance run in CI; lint/format/security/license gates; canary and rollback automation | **Amber.** Focused tests, warning-free serving/recovery/admin clippy, formatting/check gates, hard-restart, backup/dark-restore, transport/conditional/corruption/stale-response faults, production-JWT tenant isolation/revocation, offline/online/automatic/stale-owner split drills, and the current 338-test upstream suite run in CI. Supply-chain gates, mixed-version canary/rollback, and soak automation remain. |
| Performance and cost | repeatable target-hardware tests for p50/p99/p99.9, recovery, compaction, absorption lag, idle cost, noisy-neighbor isolation, and 24 h+ soak with regression budgets | **Red.** Pilot benchmarks are valuable but are not a repeatable release gate. |

## Non-negotiable release scenarios

1. Acknowledged writes survive kill -9, owner fencing, and full cell restart.
2. Two simultaneous creators/recreators/deleters produce one observable
   stream incarnation and never make an acknowledged write unreachable.
3. Object-store timeouts, 429s, 5xx responses, stale reads, and conditional
   write failures never produce false ACKs, holes, duplicates, or stale-owner
   durable reads.
4. A tenant cannot list, read, mutate, infer, starve, or exhaust resources
   belonging to another tenant, including with a valid token for itself.
5. Every attacker-controlled collection has a documented bound and overload
   response. The process stays below its 1 GiB cell budget at those bounds.
6. Backup restore and provider failover meet the published RPO/RTO in a drill;
   corrupt descriptors, topology, manifests, and SSTs fail closed and alarm.
7. A rolling mixed-version deploy and rollback preserve wire, registry,
   topology, and storage compatibility.
8. A 24-hour multi-tenant noisy-neighbor soak meets the SLO and shows no
   unbounded object count, memory growth, WAL backlog, or absorption lag.

## Findings closed on `slate-codex`

- Registry descriptors and topologies now fail closed on malformed JSON;
  topology shape is validated and routing matches all 128 hash bits.
- Delete/recreate uses an expected-incarnation CAS. Concurrent recreators
  have exactly one winner; a loser observes rather than overwrites it.
- Create races re-check the winner's encryption key and full ordering config
  before caching or using that incarnation.
- Registry and transient stream-key caches are bounded; expired/evicted key
  material is overwritten on drop.
- `format=frames` preserves timestamp, routing key, and key version, restoring
  byte-identical deterministic ciphertext across the hot and history tiers.
- Frame decoding rejects non-canonical unauthenticated suffixes.
- A fenced shard now fails in-flight and queued work promptly and shuts down
  its committer/flush tasks instead of retaining an engine per ownership move.
- Registry, storage, routing, listing, and metrics identities are customer
  scoped; equal stream names in different customers cannot collide.
- Production startup requires asymmetric JWT verification plus a monotonic
  token-id revocation document. Verb/name-prefix authorization is local and
  fail-closed; pilot/no-auth modes are explicit.
- Create-with-body is a durable idempotent transaction across response loss
  and process restart, using a reserved producer identity and canonical body
  hash. The CI smoke kills the process and proves exact retry/no duplication.
- Stream handles, producer caches, queue consumer state, touch journals,
  request metrics, and audit queues have hard bounds. Queue state is staged
  until the SlateDB write succeeds and loaded one consumer at a time.
- Per-customer concurrent-request and streaming write-byte admission returns
  scoped 429 responses, limiting noisy-neighbor memory/WAL pressure.
- Idle shard owners refresh the remote manifest at least every five seconds
  and reject reads after writer-epoch fencing; cached engines cannot bypass a
  changed fleet assignment. Repeated open failures use bounded quarantine.
- Create/delete audit events are synchronously persisted as immutable objects;
  sampled data-plane events use bounded NDJSON batches and affect readiness
  on sink failure.
- CI now enforces formatting, warning-free service clippy, tests, all-target
  checking, and a restart/dedupe/auth smoke against the S3 emulator.
- Recovery snapshots stream large objects, condition reads on the listed
  ETag, checksum every object and the bounded-memory inventory, and publish a
  completion marker last. Readiness is red before the first success. The
  offline restore tool rejects non-empty targets and CI proves a restored
  service returns the original encrypted stream.
- The complete current upstream suite is pinned and hermetic: 332 executed
  tests pass and the package's six optional subscription tests skip.
- The shard keyspace now has the topology routing hash as its first 16 bytes
  and incarnation/segment isolation as the next 16. Exact non-byte-aligned
  projection ranges, one-CAS topology publication, last-known-good topology
  polling/readiness, and an offline two-child SlateDB clone drill are in CI.
- Commit groups use persistent per-tenant round-robin look-ahead, so a run of
  large requests from one customer cannot hide a small request from another;
  FIFO order is preserved within each customer.
- Online split intents and renewable 12-second leases live with shard data,
  so an ops-only outage does not become a global write outage. Admission is
  closed before an ordered barrier drains every prior group through remote
  durability; both children must reopen before one topology CAS can expose
  their generation-specific paths.
- Every durable parent group checks the shard-store intent before releasing
  ACKs. A stale second owner therefore returns a retryable error rather than
  acknowledging data outside the clone snapshot. CI forces that two-owner
  race with separate ops/shard/data buckets and proves the acknowledged
  prefix is exact.
- Split recovery rotates clone generations when a different process takes
  over an expired lease, preventing it from clearing objects an old request
  may still be writing. The forced mid-clone kill drill recovers under a new
  identity inside a 16-second test bound and preserves exact producer data.
- A shard sustained above 60% of the configured, deployment-calibrated byte
  ceiling automatically enters the same split actor. Disabled-by-zero is the
  explicit operator override; the default sustain window is 60 seconds.
- The S3 emulator can serve a prior object version or flip a response bit
  while preserving ETag and length. CI proves a v1 topology response cannot
  regress an installed v2 trie, and that a corrupted compacted SST yields no
  success or partial plaintext before an exact retry from the unchanged
  authoritative object.
- CI generates a fresh RSA key, boots the service against real JWKS and
  revocation URLs, and signs multiple scoped customer tokens. Equal stream
  names retain separate keys and bytes, unique names are absent from the
  other tenant's reads/list, restricted verbs/prefixes fail, revocation is
  observed live, and a lower revocation version cannot un-revoke a token.
- Customer limit documents are hash-keyed, strictly validated, cached for 60
  seconds with bounded cardinality, and fail closed on corruption. They can
  override concurrent requests and streaming write-byte buckets. Stream
  count decisions serialize across processes with a renewable CAS lease and
  count durable by-customer descriptors; CI races eight names through two
  processes against a limit of two, then proves delete/replacement capacity.

## Immediate red-gate queue

1. Add stale manifest/list injection, the remaining account/per-stream quota
   dimensions and structured 429 fields, an independent security review, and
   a measured noisy-neighbor workload.
2. Add crash injection at every split phase, abandoned-generation retention/
   GC, sibling merge, per-stream hot-share enforcement, and an explicit
   storage-format migration/rollback plan.
3. Replace full snapshots with checkpoint-pinned incremental PITR, add bounded
   retention/GC and continuous manifest/SST scrubbing, then measure RPO/RTO in
   a real independent-provider failover drill.
4. Aggregate fleet heartbeats and implement the multi-cell placement/control
   plane with per-cell IAM and tenant placement limits.
5. Add dependency/license/security scanning, mixed-version canary/rollback
   automation, and the 24-hour target-hardware release soak.
