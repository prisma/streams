# Operations, Durability Dependencies & Security Spec

Companion to README.md (architecture) and COMPUTE-SPEC.md (fleet/cells).
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
| provider data-loss event | bounded by §2 backups: RPO = WAL-copy lag (target ≤ 5 min), restore per §2.3 | disaster-recovery event |
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

## 2. Backup, PITR, and corruption recovery

Everything at rest is ciphertext (§3.7 of README), so **backup requires no
tenant keys** — backups are exact object copies, useless without the
customer-held keys.

### 2.1 What is backed up

| data | mechanism | cadence |
|---|---|---|
| shard logs (WAL + SSTs + manifests) | per-shard **checkpoint pin** (SlateDB checkpoint = immutable manifest + referenced SSTs) + async copy of newly referenced objects to the backup target | checkpoint every 5 min; copy lag target ≤ 5 min |
| WAL objects | retained ≥ 24 h past checkpoint (GC floor) AND copied continuously | continuous |
| history tier (absorbed per-stream SSTs) | immutable once written → copy-once on creation; per-stream manifests checkpoint-pinned on every absorber close (point-in-time = pinned manifest + its SSTs, not bucket versioning) | continuous |
| registry + coordination prefixes | bucket versioning + hourly full snapshot (small) | hourly |

Backup GC: when compaction retires a history/shard SST past every pinned
checkpoint that references it, the copy actor deletes the backup copy on
the same schedule + 24 h — backup storage tracks live-referenced bytes,
not all-bytes-ever. The scrubber (§2.3) walks the BACKUP replica's
manifests on the same cadence as the primary.

Backup target: a second bucket in a different provider/region with
independent credentials (blast-radius isolation from a compromised primary
credential). A per-cell **copy actor** (compactor-service sibling) tracks
`backup/<shard>.ledger.json` = replicated-through position; the ledger is
the RPO measurement and its lag is an alarmed SLO.

### 2.2 PITR

Restore-to-T = nearest checkpoint ≤ T + WAL replay forward to T (records
carry commit timestamps; replay stops at the first record > T). Granularity:
per shard or per cell. **RPO:** copy-lag (≤ 5 min target; 0 for
provider-internal incidents since primary objects remain). **RTO:** shard
open from checkpoint ≈ seconds; a full 1,536-shard cell restore staggered
at 64 concurrent opens ≈ 15–30 min, dominated by manifest reads.

### 2.3 Restore drills and scrubbing

- Quarterly game-day: restore a sampled shard set into a dark cell,
  replay, diff tails against production (offsets + GCM tags make
  divergence detection exact). Restore paths that aren't exercised don't
  exist.
- **Annual full provider-failover drill:** activate §1's
  provider-control-plane path end-to-end — repoint a dark cell at the
  backup provider, verify conditional-write/CAS semantics there (all
  fencing rests on them), restore, serve. The published
  provider-failover RTO comes from THIS drill, not the restore drill.
- Continuous **scrubber** (compactor-service sibling): walks manifests,
  verifies every referenced object exists and its checksum matches
  (SlateDB block checksums run always and need no tenant key; AES-GCM tag
  verification DOES require the stream key, so it runs only in
  operator-triggered, customer-supplied-key integrity audits).
  Unreachable/corrupt object ⇒ page + auto-restore that object from
  backup (it is immutable — restore is a copy).

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

Per-request audit records (tenant, stream, verb, bytes, token-id, result)
are appended — through normal routed appends — to a per-cell internal
audit stream (same engine, dogfooding), full-fidelity for control-plane
verbs and create/delete, sampled (1 %) for data-plane reads/appends with
per-tenant counters at full fidelity via the metrics stream. 90-day
retention; tenant-visible export on request.

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

Heartbeats remain the *scaling* feed; operator telemetry is exported
(OTel) from every instance to the platform's metrics pipeline — dashboards
and alarms do not depend on the service's own storage being healthy.
Control-plane audit events and billing counters DUAL-WRITE through the
OTel path as well: an unhealthy cell cannot be relied on to log its own
incident or usage, so the in-cell audit/metrics streams are the queryable
system of record while the export path is the incident-safe copy.

**Service SLOs (per cell):**

| SLO | target | alarm |
|---|---|---|
| append availability (non-429 5xx rate) | 99.95 % monthly | 5 min burn-rate |
| append durable-ack p99 | < 250 ms | 15 min sustained |
| tail freshness (append→tail-visible) p99 | < 500 ms | 15 min |
| absorber lag | < 256 MB/instance | 30 min |
| backup copy lag (§2.1) | < 5 min | immediate |
| fence events | ≈ shard-move rate | excess = flapping page |
| scrub failures | 0 | immediate page |

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
