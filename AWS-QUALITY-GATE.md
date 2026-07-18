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
| Durability and ordering | crash/fence/timeout fault matrix; linearizable incarnation changes; no ACK before remote durability; contiguous atomic appends; bounded recovery | **Amber.** Incarnation CAS races, hard-restart create-with-body idempotence, durable producer dedupe, real two-writer fencing, split-brain post-durability ACK fencing, stale topology and manifest-discovery LIST responses, corrupt immutable SST responses, and pre/post-commit timeout/429/5xx/412 faults are automated. General measured recovery bounds remain. |
| Tenant isolation and authz | customer-scoped identity in every descriptor and request; locally verified scoped tokens; verb/prefix enforcement; no cross-tenant list/existence oracle; revocation; audit | **Amber.** Customer identity scopes registry, storage, routing, metrics, and requests. A real RS256/JWKS service drill proves identical-name isolation, per-tenant listing/non-disclosure, prefix/verb denial, live token revocation, and rollback-resistant revocation versions. Durable control-plane audit exists. An independent security review remains. |
| Encryption and key custody | independently reviewed envelope; canonical codecs; key zeroization/expiry; no persisted keys; recreate/rotation tests; ciphertext-at-rest inspection | **Amber.** Canonical frame parsing, incarnation-bound history block encryption, read-old/write-selectable migration, and bounded/zeroized root-key lifetimes are tested. An exact stable-corpus scan proves payload and printable/raw key bytes absent from primary and recovery objects and rejects a deliberate leak. Independent review and a real-corpus inspection remain. |
| Resource governance | per-stream and per-customer admission; fair committer scheduling; bounded queues, maps, caches, connections, response sizes, and background work; overload returns scoped 429/503 | **Green (software gate).** Strict durable customer documents cover append requests/bytes, read requests/bytes, live and total connections, queue receives, and exact live stream-name counts. Immutable stream descriptors add incarnation-scoped append request/byte buckets and bounded commit weights. Request/ingress excess is rejected before shard work; response egress is paced without breaking admitted streams. Response-body guards close the SSE lifetime escape. Fleet members enforce customer ceil-shares from fresh membership. The shard owner schedules equal tenant turns, then weighted stream turns, rotating oversized heads. Black-box CI proves every account/stream dimension, sibling isolation, disconnect release, restart-loaded policy, exact producer continuation, and measured egress pacing. Target-hardware isolation remains in the performance gate. |
| Horizontal scaling | automatic split/merge with quiesce proof; fleet aggregation; cell placement/isolation; hot-key behavior; no global coordination bottleneck at target scale | **Amber.** Online split and sibling merge use renewable shard-store intents, post-durability fences, verified clones, and one-CAS topology publication, with calibrated 60%/10% hot/cold triggers. A renewable epoch-fenced aggregator turns N bounded heartbeats plus router reports into one conditionally published `fleet.json`; servers and the pilot router consume that snapshot, and desired capacity includes the 32-shards-per-instance floor. Per-stream admission plus hierarchical owner scheduling bound hot-stream share. The global cell-placement/IAM layer remains. |
| Availability and recovery | readiness distinct from liveness; stale-owner read guard; poison-shard quarantine; backup recovery-point actor; restore and provider-failover drills with measured RPO/RTO | **Amber.** Readiness includes auth/revocation/audit/absorber/backup/fleet health; idle owners revalidate writer epoch within five seconds; repeated shard-open failures quarantine. Exact history-debt markers are atomic with record and absorbed-frontier commits; replacement owners reconstruct the bounded work set before admission and conservatively migrate unmarked tails, so idle debt survives process movement without a new append. Recovery points pin every initialized shard, active history DB, and external ancestor; explicitly record absent lazy DBs; expose only selected manifest closure/compactions/WAL state; reuse content-addressed blobs; prune expired points/unreferenced blobs; and continuously hash referenced recovery content using a durable provider-independent cursor. Pre-manifest acknowledged WALs are ETag-fenced and copied immediately when each DB cut is observed, so source GC cannot overtake the later inventory walk. A clock-independent renewal/observation lease epoch-orders mutable backup publications; followers require observed liveness plus post-observation relative-age health. Format 3 checksum-rehomes unchanged blobs into epoch-specific paths, making a delayed old-epoch delete physically unable to damage its successor. A second durable cursor logically decodes live shard manifests/SSTs/WALs and checks customer-key history ciphertext against writer-verified immutable digests. Primary failure invalidates snapshot health until a full repaired sweep publishes a new point. History enumeration fails closed above the 100,000-DB cell bound. CI restores adjacent points, deletes an eagerly protected WAL from primary before inventory, proves coordinator takeover, and corrupts live shard plus encrypted-history SSTs. The generic drill checks both providers' required S3 semantics, cuts the primary, measures the exact recovered-record RPO and first-decrypted-read RTO, and proves post-activation writes. Its latest two-process emulator run measured 8.751 s/519 ms; a real independent-provider run remains. |
| Operability and SLOs | RED metrics by tenant/cell/shard; bounded-cardinality telemetry; actionable alerts; audit trail; capacity model; on-call runbooks exercised by game days | **Amber.** Exact bounded per-tenant/stream RED and usage intervals are retry-stably appended to an encrypted service stream; exporter health and cardinality loss alarm without recursively metering that exact principal. Control audit is synchronously immutable on primary plus an independently credentialed mirror; sampled batches retry one stable object; conditionally persisted cursors reconcile the full corpus before readiness; and provider-clock retention deletes primary only after byte verification. An operator-authenticated OpenMetrics endpoint exports fixed-label HTTP RED, active-tail freshness, exact absorber backlog, conservative protected-point age, fencing, component/backup health, WAL/memory, L0 compaction debt, unflushed-WAL recovery debt, and bounded per-open-shard signals. Twenty checked alerts carry severity, blast radius, and runbook; CI validates their schema and drills the SLO signals, stale topology, audit-provider loss, and retry-stable billing recovery without tenant labels. Real independent accounts/consumer reconciliation and a real notification-path game day remain. |
| Verification and release | hermetic unit/integration/property/chaos/soak suites; conformance run in CI; lint/format/security/license gates; canary and rollback automation | **Amber.** Focused tests, warning-free serving/recovery/admin/benchmark clippy, formatting/check gates, hard-restart, backup/dark-restore, transport/conditional/corruption/stale-response faults, production-JWT tenant isolation/revocation, split/merge recovery matrices, automatic elasticity drills, a three-node aggregate-lease failover/corruption drill, a two-process provider cut, and stable primary/recovery at-rest inspection run in CI alongside the upstream suite. Pinned `cargo-deny` gates advisories/yanks, licenses, wildcard/banned crates, and sources; weekly Cargo/Actions updates re-enter the same gate. A checked 24-hour soak judge has an authenticated, secret-free short CI proof; the real target run and mixed-version canary/rollback remain. |
| Performance and cost | repeatable target-hardware tests for p50/p99/p99.9, recovery, compaction, absorption lag, idle cost, noisy-neighbor isolation, and 24 h+ soak with regression budgets | **Red.** The repeatable judge now records exact durable offsets, ACK p50/p99/p99.9, throughput/errors, readiness, RSS growth, absorber drain, L0/WAL debt, fences, and RPO against explicit budgets. No qualifying 24-hour target-hardware artifact, target noisy-neighbor run, or idle-cost evidence exists yet. |

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
- `StreamKey` clones zeroize on drop, while the bounded history-key cache also
  expires entries and clears incarnation bytes. History envelope 2 derives an
  HKDF key from the root key plus the 32-byte tenant/name/incarnation identity
  and authenticates that identity, preventing valid-block relocation between
  streams that reuse a customer key. The dual reader accepts legacy blocks;
  `HISTORY_BLOCK_WRITE_FORMAT` provides the required read-first/flip/rollback
  sequence, with cross-incarnation and legacy-read tests. A black-box CI cell
  writes/absorbs envelope 1, flips to envelope 2 over the same DB, rolls the
  dual reader back to writer 1, then finalizes on writer 2 while proving the
  mixed history remains byte-for-byte readable at every step.
- `streams-at-rest-check` conditionally reads every object between two exact
  bounded inventories and scans chunk boundaries without printing forbidden
  material. CI forces hot, absorbed-history, and recovery content, proves a
  unique payload plus printable/raw root-key bytes absent from both corpora,
  and rejects a deliberate plaintext object.
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
- CI now enforces the locked dependency graph with `cargo-deny`: unknown
  registries/Git sources, wildcard requirements, OpenSSL/native-TLS, unapproved
  licenses, yanked crates, and non-excepted advisories fail. The first run
  upgraded `object_store` 0.14.0→0.14.1 / `quick-xml` 0.40.1→0.41.0 to remove
  two remotely reachable XML CPU/memory DoS advisories, pinned `crc-fast` 1.7.1
  to remove yanked `spin` 0.10.0, and collapsed direct reqwest onto 0.13. Three
  narrow transitive exceptions have reachability/removal rationale in
  `SECURITY.md`; Dependabot checks Cargo and Actions weekly.
- Recovery points hold expiring SlateDB checkpoints across topology rechecks,
  filter live shards, active history DBs, and external DBs to exact
  manifest/compactions/WAL state, and treat lazy uninitialized DBs as
  explicitly absent. History is pinned after the shard cut, so it contains at
  least the absorbed prefix the shard manifest exposes. The cut also lists the
  exact contiguous WAL suffix above the manifest replay watermark. WALs not yet
  named by the pinned manifest are ETag-fenced and copied to immutable recovery
  content immediately per DB, before history discovery or the general object
  walk; source WAL GC is no longer part of their safety proof. A deterministic
  test deletes this protected suffix from primary before inventory and restores
  the acknowledged value through normal SlateDB replay. Exact source ETags
  reuse immutable SHA-256 blobs; every point still has a complete checksummed
  inventory and marker-last publication. Bounded retention deletes points
  before unreferenced blobs, and a rolling reference scrub catches missing or
  corrupt content without conflating scrub health with snapshot health. The
  offline tool supports legacy format 1, shared content format 2, and
  epoch-isolated content format 3. CI restores two adjacent points and observes
  the expected durable append boundary.
- A clock-independent CAS lease selects one backup actor per cell. The holder
  advances a renewal sequence every two seconds; contenders require the exact
  content/version to remain unchanged for six locally monotonic seconds before
  takeover. A first observation is not proof of life: followers require a
  same-owner renewal and health published after that proof, repeating the
  handshake after any stale interval. Relative monotonic ages, rather than
  producer wall time, govern readiness. Lease epoch and monotonic sequences
  conditionally fence source indexes, blob references, scrub state,
  latest-point publication, and health; retention runs only while authority is
  locally and remotely valid. Unit tests use `i64::MAX` legacy expiry and health
  timestamps, and prove pause/republication fencing. CI runs two real processes,
  kills the lease holder, requires an epoch-incremented survivor point, proves
  followers emitted no duplicate completion markers, then rolls the survivor
  back to a higher-epoch format-2 point while retaining both reference
  namespaces.
- Format 3 places blobs and references below an epoch-specific namespace and
  checksum-rehomes unchanged format-2/old-epoch content before publication.
  Retention derives both age and current time from the recovery provider's
  object metadata via a CAS clock probe, ignores producer audit timestamps, and
  never expires the newest completed point.
  Retention persists a GC intent before marker removal and deletes it last;
  fault simulation removes all point metadata mid-GC and proves the next pass
  still collects the last-reference blob and mutable index.
  An explicit write-format switch supports a read-first wave, the flip, and a
  one-version rollback; old binaries do not scan the format-3 reference root.
  The procedure and live-format evolution contract are documented in
  `STORAGE-MIGRATIONS.md`.
- The backup coordinator also advances a bounded primary-integrity cursor.
  SlateDB logically decodes every shard manifest, index, statistics block,
  data block, and live WAL; its WAL inventory includes objects above the
  replay watermark even before `next_wal_sst_id` is remotely persisted.
  Customer-key history is keyed-logically-verified by the absorber before a
  create-only ciphertext digest is admitted to ops. CI changes shard and
  encrypted-history SST bodies without changing length, observes readiness
  fail closed, repairs the bytes, and requires a new recovery point before
  readiness returns.
- `streams-provider-check` verifies conditional create/update fencing, strong
  immediate read/list, ranged GET, multipart assembly, server-side copy, and
  delete visibility under a unique disposable prefix. The generic failover
  harness requires different provider identities/endpoints/credentials, an
  executable primary-cut hook, empty recovery targets, and monotonic RPO/RTO
  budgets. CI runs it against two independent emulator processes, kills the
  primary, recovers the last point-proven sequence while losing a deliberately
  later one, measures 8.751 s RPO and 519 ms RTO, and verifies a new producer
  write. This is not substituted for the mandatory real-provider artifact.
- The complete current upstream suite is pinned and hermetic: 332 executed
  tests pass and the package's six optional subscription tests skip.
- The shard keyspace now has the topology routing hash as its first 16 bytes
  and incarnation/segment isolation as the next 16. Exact non-byte-aligned
  projection ranges, one-CAS topology publication, last-known-good topology
  polling/readiness, and an offline two-child SlateDB clone drill are in CI.
- Per-stream history debt is stored under the same 32-byte prefix and therefore
  follows those projection clones and merge unions. Startup recovery seeks past
  each hash's record range, is capped at 100,000 pending streams, and rejects
  corrupt or over-cap state before the shard admits traffic. CI kills an owner
  below both live absorption thresholds, observes recovered debt on its
  replacement, supplies the lost customer key with a read, and proves history
  drains without a post-restart append.
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
- A takeover atomically records each superseded, never-published operation in
  its intent and materializes durable GC candidates. The scanner retains them
  for 24 hours, then deletes only candidates absent from a freshly re-read
  topology and every active intent, with a 100,000-object run bound. CI
  creates a non-empty abandoned generation, uses zero test retention, proves
  its marker and objects are reclaimed, and rechecks the live sibling's data.
- A release-mode test hook aborts after intent creation, claim, parent
  quiescence, zero-child verification, one-child verification, topology
  publication, and intent release. The CI matrix independently rebuilds each
  state, then requires exact acknowledged data, two valid children, intent
  cleanup, and the next producer write after restart.
- A shard sustained above 60% of the configured, deployment-calibrated byte
  ceiling automatically enters the same split actor. Disabled-by-zero is the
  explicit operator override; the default sustain window is 60 seconds.
- Sibling merge CAS-creates one coordinator intent and atomically occupies
  each child's existing split-intent namespace, making split-vs-merge
  exclusion linearizable. Both child writers stop ACKing behind those durable
  fences, drain through WAL then L0, and become a non-overlapping SlateDB
  manifest union before one topology CAS exposes the parent. A reserved
  17-byte tombstone outside the service key grammar advances WAL replay for
  an otherwise-empty projected child. Released locks are CAS tombstones, not
  deletes that a delayed actor could race with a later split/merge cycle.
- CI proves two complete split→merge cycles preserve exact bytes and producer
  continuation, a stale second writer cannot ACK outside the union snapshot,
  all seven merge transitions recover independently, and a different-identity
  takeover rotates then garbage-collects a non-empty abandoned target. A
  delayed child split that validates before merge publication is forced to
  replace the released lock, observe the changed topology, durably abort its
  generation, and leave readiness green.
- Heartbeats include one bounded `(shard, writer_epoch, appended_bytes)` tuple
  for every shard assigned to that instance, including unopened idle shards.
  The automatic merge coordinator accepts only a unique, under-10-second
  report from each child's current ring owner; missing, duplicated, malformed,
  future-dated, unchanged, counter-regressed, or owner/epoch-changed evidence
  cannot advance the cold clock. The default combined threshold is 10% of the
  same calibrated ceiling used by split, sustained for ten minutes, preserving
  a 6× trigger gap. CI proves hot siblings do not merge, exact data and producer
  state survive an automatic merge and restart, and a remote-owner heartbeat
  outage cannot be mistaken for coldness before fresh monotonic reports resume.
- Each instance still plain-PUTs its own heartbeat, but only the holder of a
  renewable six-second CAS lease reads the bounded set of ≤64 ordinal
  heartbeat objects and the fixed `router-1..32` report set. It publishes `fleet.json` with a
  lease epoch and per-epoch sequence through conditional CAS after rechecking
  ownership. A takeover increments the epoch; a delayed old writer cannot
  overwrite that epoch or reuse a conflicting sequence. Serving instances and
  the pilot router read one aggregate instead of fanning in N objects, and only
  the current aggregator may change `desired.json`. Corrupt/stale aggregate or
  desired state fails readiness while preserving the last installed ring. CI
  kills the actual lease holder, proves bounded takeover/dead-heartbeat expiry
  and reintegration, then corrupts/restores the snapshot and observes readiness.
- The S3 emulator can serve a prior object version or prior LIST result, or
  flip a response bit while preserving ETag and length. CI hard-restarts into
  a stale manifest-discovery LIST that omits the newest immutable manifest,
  proves a v1 topology response cannot regress an installed v2 trie, and
  proves a corrupted compacted SST yields no success or partial plaintext
  before an exact retry from the unchanged authoritative object.
- CI generates a fresh RSA key, boots the service against real JWKS and
  revocation URLs, and signs multiple scoped customer tokens. Equal stream
  names retain separate keys and bytes, unique names are absent from the
  other tenant's reads/list, restricted verbs/prefixes fail, revocation is
  observed live, and a lower revocation version cannot un-revoke a token.
- Customer limit documents are hash-keyed, strictly validated, cached for 60
  seconds with bounded cardinality, and fail closed on corruption. They can
  override concurrent/live connections, append/read request buckets,
  ingress/egress bytes, queue receives, and stream counts. Fleet members use
  ceil-shares of the customer limit from fresh aggregate membership; response
  bytes are paced across finite bodies and SSE. The admission guard is owned
  by the HTTP body until EOF or disconnect, closing the handler-return escape.
  CI exhausts each request bucket before storage, holds and releases an SSE
  slot through a real client disconnect, restarts onto a stricter durable
  policy, and measures a 4097-byte response paced at 1 KiB/s. Stream
  count decisions serialize across processes with a renewable CAS lease and
  count durable by-customer descriptors; CI races eight names through two
  processes against a limit of two, then proves delete/replacement capacity.
- A two-principal production-JWT workload runs eight continuous abusive
  writers against tenant A's durable concurrency/byte limits while tenant B
  performs durable producer writes. CI requires every attacker request to be
  a scoped 429, every victim write and byte to survive, and victim p99 to stay
  below a baseline-relative bound (10× baseline + 250 ms, capped at 2 s).
- Every HTTP response enters a fixed operation/status-class counter and fixed-
  bucket latency histogram. The operator-only OpenMetrics scrape also exposes
  each readiness component, configured recovery-point/scrub health, audit
  drops, overload shedding, resident memory, WAL PUT p50/p99, outbound store
  concurrency, and the bounded open-shard set's durable-wait p99 and append
  counters plus L0 and unflushed-WAL debt. It never emits customer or stream
  names. Twenty checked JSON/YAML-
  compatible Prometheus alerts cover telemetry loss, component/backup failure,
  missing audit/billing dependencies, audit/billing loss, fast/slow append
  error-budget burn, durable/WAL latency, active-tail freshness, absorber
  backlog, protected-point age, fence flapping, compaction/recovery debt,
  memory, and shedding, each with
  severity, cell blast radius, and runbook. Black-box game days require
  operator auth, live append/shard/tail/absorber samples, a finite protected-
  point age, an observed reconfiguration fence, last-known-good service with a
  replayed stale topology, an exact topology-unready metric plus readiness 503,
  and recovery after restoring v2.
- Audit control records are acknowledged only after identical immutable writes
  to the primary ops store and a separately credentialed mirror. Sampled
  batches retain one name/body across side-specific retries. Bounded durable
  cursors reconcile control and batch prefixes on every boot before readiness;
  retention uses provider timestamps and exact stable-read comparison before
  primary deletion. CI cuts either mirror path, proves control 503/readiness,
  stable batch identity, and recovery. Real provider-account custody, lifecycle,
  export, and retention inspection remain deployment evidence.
- Per-tenant/stream billing records now include exact request/error/throttle/
  duration bins as well as operation bytes. The exporter retains one serialized
  interval and idempotent producer sequence until ACK, excludes only its exact
  configured principal/stream from self-metering, and exposes configuration,
  health, retry, and bounded-series-loss signals. CI compares failed/retried
  HTTP bodies and producer headers byte for byte, then proves one encrypted
  interval and no feedback loop. A real downstream consumer and reconciliation
  contract remain deployment evidence.
- Stream descriptors now persist validated append request/byte limits and a
  1..=100 commit weight. Admission state is incarnation-scoped and bounded;
  only fully refilled states can be evicted, preventing cache churn from
  resetting depleted bursts. The committer is hierarchical: equal outer
  tenant turns, then weighted stream turns, with oversized heads rotated past
  the current byte budget. CI proves request/byte stream 429s, sibling
  isolation, immutable idempotent config, invalid-limit rejection, restart
  persistence, and exact producer continuation.
- The release-soak judge refuses sub-24-hour evidence unless explicitly in
  short test mode. It drives an authenticated workload, reconciles every
  stream's durable offset, scrapes every named instance, and checks ACK
  p50/p99/p99.9, errors/throughput, readiness, RSS growth, absorber drain,
  L0/WAL debt, fences, and RPO before writing a secret-free JSON verdict. CI
  proves the harness in short mode; no target-hardware release artifact has
  been produced.

## Immediate red-gate queue

1. Complete an independent security/envelope review and run the now-automated
   stable-corpus ciphertext inspection against a production-shaped real corpus;
   hermetic primary/recovery and deliberate-leak controls are complete.
2. Exercise the documented primary-storage migration contract in a real
   mixed-binary canary before the first post-v2 live-format change. The
   hermetic read-first/v2-flip/dual-reader-rollback state sequence is automated;
   it does not replace a deployed old/new binary wave.
3. Measure RPO/RTO in a real independent-provider failover drill; the primary
   and independent recovery corpus now have bounded continuous integrity
   evidence; recovery points are epoch-isolated, lease-ordered, checkpoint-
   pinned, incremental, and closed over eagerly protected pre-manifest WALs,
   with bounded retention/GC and rolling content verification. The reusable
   conformance/cut/restore/activate harness and hermetic process-cut evidence
   are complete; production provider credentials are not configured here.
4. Implement the multi-cell placement/control plane with per-cell IAM and
   tenant placement limits.
5. Run the real mixed-version canary/rollback and execute the checked 24-hour
   target-hardware release soak. The soak judge and its short authenticated CI
   proof now exist; dependency/advisory/license/source scanning and the
   hermetic history-format migration sequence are automated.
6. Provision the implemented audit mirror and billing exporter in genuinely
   independent accounts, validate a complete legacy
   audit backfill plus provider lifecycle/object-lock policy, and reconcile a
   real downstream billing consumer. Then exercise the deployed collector,
   alert evaluator, notification route, inhibition, and ownership labels in a
   real on-call game day. The bounded scrape, 20-rule catalog with all published
   SLO signals, audit-provider cut, retry-stable billing drill, and stale-
   topology drill are complete.
