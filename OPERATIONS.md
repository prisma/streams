# Operations, Durability Dependencies & Security Spec

Companion to SPEC.md (architecture) and COMPUTE-SPEC.md (fleet/cells).
This document answers the operator-grade questions: what the object-store
dependency actually promises, how data survives provider incidents and
operator error, who a tenant is, and what the on-call runs.

---

## 1. Storage dependency analysis (the sole stateful tier)

The service's durability guarantee (G1: committed-before-ack) reduces to
the object store's durability plus our fencing discipline. What we REQUIRE
of any provider (Tigris today; the S3 API is the portability boundary):

| requirement | why | Tigris today |
|---|---|---|
| ≥ 11-nines object durability, region-internal replication across ≥ 3 failure domains | G1 math | FoundationDB-backed metadata + erasure-coded payload across zones |
| strong read-after-write | SlateDB manifests/WAL replay | yes (strict consistency documented) |
| conditional PUT (If-Match / If-None-Match) | ALL fencing, CAS coordination | yes (verified live, V2) |
| per-prefix request scaling ≥ 3.5k PUT/s + 5.5k GET/s with auto-partitioning | shard-log flush + read load | contract-level commitment required before GA (open O12) |
| object versioning + deny-delete IAM on data prefixes | operator/malicious-delete protection (§2.4) | available; must be enabled per bucket |

**Failure-mode mapping** (extends COMPUTE-SPEC §8):

| provider event | service behavior | tenant-visible |
|---|---|---|
| elevated PUT latency | durable watermark lags → ack latency rises → §12 latency dim scales fleet (more batching), then queue-depth 429s | slower appends, then throttled appends; zero loss |
| regional write outage | all appends 429 within seconds (watermark stalls); reads serve from CDN/cache/disk for cached ranges | write unavailability = provider write unavailability; correctness intact |
| regional read+write outage | full unavailability in region; cross-region cells unaffected; recovery = reopen from manifests, < 1 s/shard staggered | regional outage |
| provider data-loss event | bounded by §2 recovery points: RPO = age of the newest recovered acknowledged record at failure (GA target ≤ 5 min), restore per §2.3 | disaster-recovery event |
| provider CONTROL-PLANE event (global namespace, auth, or API outage) | affects all cells on that provider regardless of region — the true failure domain is the provider, not the region. Activation path: repoint cells at the backup provider's bucket (credentials + endpoints already provisioned for §2), restore from backup replica; RTO = restore drill numbers | disaster-recovery event |

**Registry residence:** the by-name registry is a global-namespace
dataset. On Tigris it lives in the provider's global namespace natively;
on per-region S3-class providers it homes in a designated region with
continuous backup-replication (§2.1) elsewhere. A registry-home outage
degrades ONLY control-plane verbs (create/delete/config): the data plane
keeps serving from cached descriptors (60 s TTL, extended on failure with
alarmed staleness) and existing streams keep working. The L7 portability
claim includes this residence rule.

**SLA math** (append availability, single region):
`A_append ≈ A_provider_write × A_cell_serving`. With provider write
availability 99.95 % and cell serving availability 99.99 % (stateless,
N ≥ 3, restart < 15 s), the design supports a **99.9 % single-region
append SLA** with margin; a 99.99 % SLA requires multi-region active
routing of writes (roadmap; the fencing model permits it per-shard because
ownership is a manifest property, not an instance property).

---

## 2. Backup, recovery points, and corruption recovery

All tenant payloads at rest are ciphertext (§3.7 of SPEC.md); routing headers
and bounded control/audit metadata are intentionally visible. **Backup requires
no tenant keys** — it copies ciphertext and control objects exactly and cannot
decrypt stream payloads without customer-held keys.

**Implementation status (`slate-codex`, 2026-07-18):** the service publishes
discrete, checkpoint-pinned incremental recovery points. Exact source ETags
index immutable SHA-256 content; every point still receives a complete
checksummed inventory before its immutable completion marker. Format 3 isolates
content by fenced backup-coordinator epoch. Retention, restartable GC, rolling
recovery-content scrubbing, live-primary logical scrubbing, dark restore, and
coordinator takeover are implemented and gate readiness when
`REQUIRE_BACKUP=true`. `streams-restore` accepts only a complete point and an
empty offline target.

The service does **not** currently offer arbitrary restore-to-timestamp PITR.
Recovery selects one completed point; the RPO is therefore the age of the
newest acknowledged record in that point at the failure boundary. The default
point cadence is five minutes, so a deployment cannot publish a five-minute
RPO until measured point-completion time is budgeted below the configured
cadence. The real-provider harness measures this exact record boundary and the
time to the first decrypted read, but it has not yet been run with two
independent production providers. Those RPO/RTO numbers remain a GA gate, not
a customer guarantee.

### 2.1 What is backed up

| data | implemented cut | cadence |
|---|---|---|
| shard logs | expiring detached checkpoint plus exact selected manifest/checkpoint/compactions closure and every contiguous WAL above the replay watermark | one cell recovery point; default every 5 min |
| pre-manifest acknowledged WALs | WAL IDs at or above the pinned manifest's `next_wal_sst_id` are ETag-pinned and copied to immutable recovery content immediately as each DB cut is observed, before history enumeration or the general object walk; source GC may then remove them | every point |
| history tier | every initialized DB named by an active incarnation is checkpoint-pinned after the shard cut; lazy DBs are explicitly absent; external clone ancestors are recursively closed | every point |
| registry, topology, fleet, audit, and integrity metadata | exact-Etag incremental object copy in the same checksummed inventory | every point |

The final topology, every checkpoint manifest, and every checkpoint-referenced
WAL are rechecked before publication. History is cut after shards, so it is at
least as new as the absorbed frontier exposed by the shard cut. A missing,
changed, malformed, non-contiguous, or over-bound WAL set aborts the point.
Acknowledged WALs not yet named by a manifest are already immutable in the
recovery provider before the slower fleet-wide inventory starts; they do not
depend on the source WAL-GC grace period.

Unchanged objects reuse content-addressed blobs, while each point retains a
full inventory. Retention uses only the recovery provider's own
`Last-Modified` clock, preserves the newest completed point, removes point
authority before content, and persists a restartable GC intent. Recovery and
primary scrubbers advance bounded provider-independent cursors and fail
readiness closed on missing or corrupt authority.

The target MUST be a second provider/region with independent credentials.
Exact primary endpoint+bucket reuse is rejected, but configuration validation
alone is not blast-radius proof; the provider failover drill below is the
release evidence.

### 2.2 Recovery-point semantics

Restore is cell-wide and point-exact: choose `latest` or an immutable snapshot
ID, validate its completion marker, count/checksum every inventory record,
verify every content digest, and materialize it into empty offline role
targets. Normal SlateDB open replays the captured WALs, including the eagerly
protected pre-manifest suffix. A later record is absent by construction.

There is currently no WAL-by-WAL replication ledger and no replay-to-arbitrary
timestamp API. Product and runbook language MUST call these *recovery points*,
not PITR. The measured RPO is `failure boundary - acknowledgment time of the
newest recovered record`; RTO is `failure boundary - first successful
decrypted read from a service using only the recovery provider`. Deployment
budgets must include both interval and point-completion tails.

### 2.3 Restore drills and scrubbing

- Every release runs dark restore around a known durable append and proves the
  older point excludes it while the newer point includes it. The test also
  forces an acknowledged WAL ahead of the manifest, deletes that WAL from the
  primary after eager protection, and reopens it from recovery content.
- `streams-provider-check` destructively probes a unique disposable prefix for
  conditional create/update fencing, strong immediate GET/LIST, strictly
  ordered exclusive-offset listing, ranges, multipart upload, server-side copy,
  and delete visibility. Both providers
  must pass before a failover drill.
- `scripts/provider-failover-drill.sh` requires distinct provider identities,
  endpoint authorities, and (outside hermetic test mode) access-key IDs. It
  protects a known producer sequence, acknowledges one deliberately later
  sequence, invokes a real provider-cut hook, restores `latest` into an empty
  namespace on the recovery provider, measures RPO/RTO with a monotonic clock,
  and proves a new producer can write after activation. Its JSON artifact is
  the release evidence.
- CI runs the same harness against two independent `s3lite` processes and
  kills the primary process. On 2026-07-18 this recovered sequence 1, lost the
  deliberate sequence 2, measured RPO 8.751 s and RTO 519 ms, and verified a
  post-failover write. This is protocol/harness evidence, **not** independent-
  provider evidence.
- Recovery-content scrubbing hashes referenced immutable blobs. Primary
  scrubbing logically decodes live shard SlateDB authority and compares
  customer-key history ciphertext with keyed-at-write immutable digests.
  Detection fails readiness and snapshot health; repair requires a complete
  primary sweep and a fresh point. Automatic source repair is not implemented
  and must not be claimed.

### 2.4 Deletion protection & GDPR

- Data prefixes: object versioning ON + deny-delete IAM for all service
  principals except the GC role; GC deletes go through a 24 h
  soft-delete (version retained) window. Human deletes: two-person rule
  via break-glass role.
- Tenant deletion (API `DELETE` / retention expiry): registry tombstone
  immediately (reads/writes 404/410), physical erasure of shard-log
  records via compaction filter and of history SSTs via prefix delete
  within 30 days, backup copies expire on the same schedule (backup
  retention ≤ 30 d for deleted streams). Crypto-erasure is additionally
  available to customers instantly by destroying their key.

---

## 3. Tenant identity, authn/z, and key custody

The pilot's single shared bearer token is a stand-in. The GA contract,
kept deliberately verify-local so no remote call sits on the data path:

### 3.1 Principals and credentials

- A **tenant principal** = customer id + key pair issued by the control
  plane (the "key service" — same trust domain as billing, NOT on the
  data path).
- Data-plane credential: **scoped signed token** (JWT/PASETO-class):
  `{customer, allowed stream-name prefixes, verbs (create/append/read/
  queue), expiry ≤ 24 h, token-id}`. Instances verify locally against the
  key service's published JWKS (cached 10 min, background-refreshed).
- Operator credentials use the same signature, issuer, audience, ≤24-hour
  lifetime, and token-id revocation path, with an explicit `operator: true`
  claim. The claim defaults false when absent. Operator automation uses a
  principal distinct from tenant/service subjects and rotates its mode-0600
  token file before expiry. Startup rejects the pilot `AUTH_TOKEN` whenever
  JWKS mode is configured, so it cannot remain as a static production bypass.
- The stream encryption key remains a separate capability (crypto
  custody), carried per request as today; authn (who) and crypto access
  (can decrypt) are independent factors.

### 3.2 Key-service contract (the D20 delegation, made concrete)

| property | requirement |
|---|---|
| interface | issue/rotate/revoke principals; publish JWKS; per-customer limits doc (§12) |
| availability SLO | 99.9 %; **off the request path** — tokens that fail verification are ALWAYS rejected; the cached JWKS only bounds verification staleness (background-refreshed; alarms at 10 min stale, requests keep verifying against cache up to 60 min, after which unverifiable-key tokens are rejected = fail closed) |
| revocation latency | token-id denylist object per cell, polled 60 s ⇒ ≤ 60 s revocation without touching JWKS |
| audit | every issue/rotate/revoke is an audit-stream record |

### 3.3 Key compromise (streamKey leak)

`keyVersion` rotation protects new records only. The compromise runbook is
therefore two-tier and documented to customers:

1. **Contain:** customer rotates keyVersion (new writes safe) and rotates
   the authn token (removes API access — a leaked key without a token
   cannot read through the service; exposure is limited to parties who
   also obtain ciphertext).
2. **Re-encrypt history (optional, priced):** a re-encryption job — a
   dedicated worker granted a short-lived, audited copy of old+new keys —
   rewrites history SSTs under the new key epoch (same mechanics as
   delete/recreate, D21 storage-epoch swap, reader cutover via registry).
   This is the only component besides the request path that ever holds a
   tenant key, and only for the job's lifetime.

### 3.4 Access audit

Per-request audit records (tenant, stream, verb, token-id, result, latency)
are written to the cell ops bucket and an independently credentialed audit
provider. Create/delete records are synchronously persisted as identical
immutable objects on both sides before a successful response is returned;
sampled (1 % by default) data-plane reads/appends use bounded one-second NDJSON
batches that retain one object identity while either side retries. Per-tenant
counters remain full fidelity via the metrics stream. Queue loss, write
failure, corrupt/missing mirror content, or maintenance failure makes readiness
fail, and an unaudited successful control mutation is returned as retryable
503 instead.

Every boot reconciles both primary prefixes through conditionally persisted
bounded cursors before readiness can pass. A primary object is pruned only
after an If-Match stable read and exact mirror comparison; each retention
cutoff is derived from that provider's own object metadata. The default is 30
days primary and 365 days mirror, with a deployment-selectable one-day-to-
seven-year envelope. Provider-native encryption/object lock, independent IAM,
the tenant-visible export surface, and a real-account retention inspection are
deployment obligations and remain GA evidence gates.

### 3.5 At-rest inspection and history-block binding

`StreamKey` values and their request-local clones zeroize on drop; the bounded
history key cache additionally expires entries and clears incarnation bytes.
Keys are accepted only from requests/default development configuration and are
never serialized into registry, audit, integrity, primary, or recovery
objects.

History block envelope 2 derives an HKDF key from the customer key and the
32-byte tenant/name/incarnation storage identity, then authenticates a version
marker plus that identity as AAD. Reusing one customer key for two streams no
longer makes a valid encrypted block relocatable between them. The reader is
legacy-compatible and the write-format flip follows the read-first contract in
`STORAGE-MIGRATIONS.md`.

`streams-at-rest-check` takes a bounded exact ETag inventory, scans every body
with conditional reads for operator-supplied byte patterns, and repeats the
inventory so concurrent mutation cannot yield a false pass. Forbidden bytes
come from a private local file and never appear in its JSON evidence. CI forces
hot-shard, encrypted-history, and recovery-point material, then proves a unique
payload sentinel, its printable root key, and the decoded 32-byte root key are
absent from both stable primary and recovery corpora. A deliberate plaintext
object must fail the same checker. This is first-party verification; an
independent envelope review remains mandatory before GA.

---

## 4. Read fan-out: CDN plan A, mux-tier plan B

Plan A (COMPUTE-SPEC §3.1) assumes CDN request-coalescing of ≥ 30 s
hanging GETs. That is **verification item V5, a GA gate**: measured
per-POP coalescing behavior, hold-time limits, and cache-key correctness
for cursor URLs against the chosen CDN, at ≥ 10k concurrent subscribers.

**Plan B (designed now, deployable without architecture change):** a
**tail-multiplexer tier** of the same 1-CPU/1-GB instances:

- Each mux instance holds ≤ 8,000 client SSE/long-poll connections
  (§1.1 budget) and maintains exactly ONE internal long-poll per
  (stream, mux) upstream to the shard owner.
- Origin load per stream = number of mux instances subscribed to it, not
  number of clients: 1M concurrent tails on one stream ≈ 125 mux
  instances ≈ 125 origin long-polls — the same order the CDN plan yields
  (#POPs), with the same freshness (single-flight wakeups).
- Routers send tail reads to the mux tier when a stream's live-connection
  count (heartbeat load vector) exceeds 1,000; below that, direct to the
  owner. The mux tier is cell-local but is ITS OWN pool: separate desired
  count and guardrails (max 512, connection-dimension driven — 1M tails ≈
  125 muxes exceeds the serving fleet's max=64 by design), and its
  heartbeats live in the same cell prefix (the §10.2 arithmetic gains
  ≤ 512/2 s PUTs — still far under prefix baselines).
- **Catch-up parity:** muxes serve immutable canonical chunks from a local
  LRU (same ETag discipline the CDN would use) and fetch misses
  origin-direct — so replay-heavy streams get plan-A-equivalent behavior,
  just with instance economics instead of CDN economics, until V5 passes.

Failure of a mux instance drops its clients' connections; clients
reconnect through the router (jittered) and land on surviving muxes —
no state is lost because tails are offset-addressed.

---

## 5. Telemetry, SLOs, alarms, runbooks

Heartbeats remain the *scaling* feed. Every instance now exposes a bounded,
operator-authenticated OpenMetrics surface at `/v1/debug/metrics`; the platform
collector attaches trusted region/cell/instance labels and may translate it to
OTel. Thus dashboards and metric alarms do not depend on the service's own
storage. `ops/prometheus-alerts.json` is the checked, actionable rule catalog;
CI exercises a stale-topology page signal and recovery.

Customer/stream billing intervals are appended through the normal router to a
dedicated encrypted stream under a scoped service principal. Each interval
contains exact request count, client-error, throttle, server-error, duration,
and fixed-bucket duration totals. On failure the already serialized interval
and producer id/epoch/sequence remain pending until acknowledged; the exact
configured principal/stream tuple is excluded from metering to prevent a
feedback loop without granting other tenants a name-based exemption. Export
configuration, health, retries, and bounded-series drops are part of the
operator scrape and checked alerts. Billing failure deliberately does not evict
an otherwise healthy serving cell; it retains the pending interval and pages
operators. Audit records use the independent dual-write and retention path in
§3.4.

Hermetic provider-cut and retry-stability drills cover both paths. A real
independent audit account, a billing warehouse/consumer with reconciliation,
provider lifecycle evidence, and end-to-end incident notification are still
deployment gates; the local drills do not establish their blast radius or
custody.

**Service SLOs (per cell):**

| SLO | target | alarm |
|---|---|---|
| append availability (non-429 5xx rate) | 99.95 % monthly | 5 min burn-rate |
| append durable-ack p99 | < 250 ms | 15 min sustained |
| tail freshness (append→tail-visible) p99 | < 500 ms | 15 min |
| absorber lag | < 256 MB/instance | 30 min |
| newest protected recovery-point age (§2.1) | deployment RPO budget | immediate |
| fence events | ≈ shard-move rate | excess = flapping page |
| scrub failures | 0 | immediate page |

The 20-rule checked alert catalog implements scrape loss, component/backup
health, missing audit mirror, audit drops, missing/unhealthy billing export,
billing-series drops, append error-budget burn, durable/WAL latency, active-
tail freshness, exact plaintext absorber backlog, conservative newest-
protected-point age, fencing rate, L0 compaction debt, unflushed-WAL recovery
debt, memory, and overload shedding. The bounded absorber queue fails the shard
and process unready instead of silently dropping maintenance work; its frontier
advances only after remote durability. Exact plaintext debt is written in the
same shard batch as each new record and decremented in the remotely durable
frontier batch. Shard replacement reconstructs the bounded work set before
admission, including conservative migration of pre-marker tails. A replacement
never persists customer keys: recovered work stays visible until the next
key-bearing read or write and then drains without requiring an append. A real
collector/evaluator/notification game day remains a deployment gate.

The target-hardware release gate is `scripts/release-soak.py` (invocation and
credential contract in [RUNBOOK §9.1](./RUNBOOK.md#91-target-hardware-release-soak)).
It requires a 24-hour run by default, scrapes every supplied instance, verifies
the exact durable next offset of every generated stream, and emits one
machine-readable budget verdict. CI exercises only its explicit short mode;
no short/local artifact satisfies the performance release gate.

Every row of COMPUTE-SPEC §8 (failure matrix) plus §12.4 quarantine and
§2.3 restore has a runbook entry; runbooks live next to this spec and are
exercised in game-days. On-call model: per-region rotation, cells are the
isolation unit so pages carry a cell id and a blast-radius statement.

---

## 6. Capacity & unit-economics model

Measured inputs (pilot + benchmarks): ~220-byte single-record appends cost
1 gateway hop + share of one WAL PUT (25 ms window) + share of one L0 PUT +
compaction amplification ≈ **3–4 object-store ops amortized per 1,000
records at healthy batching**, ~90 % zstd compression in history tier.

| driver | model |
|---|---|
| storage | raw bytes × ~0.12 (compression) × provider $/GB-mo; history >30 d quiet → archive tier at 1/5 cost (COMPUTE-SPEC §7) |
| requests | WAL PUTs ≈ shards × 40/s max (25 ms) but load-proportional under batching; per-million-appends object-op cost ≈ (1M / effective batch size) × 2.5 ops |
| compute | instances = max over §4.2 dims; ~6k appends/s/core target, 8k conns/instance |
| capacity planning | reactive autoscaler (§4) for minutes-scale; forecasted floor per cell from 7-day peak × 1.3, pre-provisioned as cell weights in `cells.json`; new-cell lead time is the planning horizon (< 1 h, it's stateless) |

Open until provider contract lands (O12): committed per-prefix rates and
volume pricing; the unit model is parameterized on those two numbers.

---

## 7. Edge read contract (closing the CDN ↔ authn/metering gap)

The CDN path and per-tenant credentials are reconciled by making **read
capabilities, not principals, the unit of edge authorization** — the same
pattern the state-protocol touch profile already uses (HMAC capability
URLs), generalized:

1. **Capability issuance (origin, authenticated, quota'd):** a client
   holding a valid token + stream key calls
   `POST /v1/stream/{name}/read-grant {scope: catchup|tail, cursor?}` and
   receives a **capability URL**: the canonical read URL plus
   `sig = HMAC(read_sig_key, canonical-url ‖ expiry)` where
   `read_sig_key` is derived from the stream key epoch exactly like the
   touch `wait_sig_key`. TTL ≤ 5 min (tails) / ≤ 1 h (immutable catch-up
   chunks). Issuance is the per-tenant-quota'd, audited, billable event.
2. **Cache-key correctness:** the capability URL is IDENTICAL for every
   authorized reader of the same (stream, chunk|cursor-cohort, epoch
   window) — expiry is bucketed (5-min windows) so cohorts share URLs.
   Coalescing and cache hits therefore work unchanged; per-tenant
   credentials never reach the CDN.
3. **Ciphertext-only at the edge (O2, now normative):** CDN-path reads
   serve **frames format (ciphertext)**; clients decrypt with their
   stream key. Plaintext reads (server-side decrypt) are origin-only,
   authenticated per request, and metered inline. Tenant keys never
   transit or rest at the CDN in any form.
4. **Origin validation:** on cache miss the origin validates `sig`
   (statelessly, from the stream epoch) before serving — a leaked URL is
   a ≤ TTL, single-stream, ciphertext-only exposure, revocable by epoch
   bump (D21 recreate semantics).
5. **Metering & audit:** issuance writes the audit record (who, stream,
   scope); CDN access logs (standard log delivery, hourly) reconcile
   `read_bytes` per stream into `__metrics__` for billing and anomaly
   alarms. The unauthenticated stance is explicit: possession of an
   unexpired capability URL IS authorization for that scope — bounded,
   revocable, ciphertext-only.
6. **Renewal without stampedes:** the next expiry-bucket's URL is
   derivable before the current one lapses; SDKs renew at 80 % of TTL
   with jitter, so a cohort's renewal spreads across the bucket rather
   than synchronizing at its edge. Grant issuance is a budgeted line in
   the capacity model: ~3.3k grants/s per 1M live tails per cell ≈ 52/s
   per instance at N=64 — negligible next to the append path, but it is
   also the billing/quota event, so it is metered like one.
7. **GA gating:** V5 must verify coalescing/caching *with these URLs*
   (bucketed expiry, sig in path not query-varied per client). Until
   plan A passes, GA reads ride plan B (§4 mux tier), which terminates
   TLS at instances and verifies tokens natively — full parity, higher
   cost per connection.
