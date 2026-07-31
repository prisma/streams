# Handover — Prisma Streams product-surface implementation

Branch: `slate` (all work committed and pushed)
Suite at handover: **157/157 green** (`cargo test --release`)
Spec package: `Prisma Streams Unified Product-Surface Specification`
(pre-launch hard cutover edition; unzipped copy lives in the session
scratchpad under `spec/prisma_streams_surface_spec_prelaunch_hard_cutover/`)

---

## 1. What this work is

Implement the final Prisma Streams product surface as a **destructive
pre-launch hard cutover**. Normative rules driving every decision:

- Implement the final server, SDK, routes, descriptors, codecs, cursors,
  postings layout, consumers and watches **directly**.
- Deploy against a **fresh bucket / PATH_PREFIX**; existing dev, staging
  and campaign data is disposable.
- **No** legacy decoders, request translators, route/header aliases, SDK
  shims, dual reads/writes, cutover offsets, backfills, migration jobs,
  deprecation windows, feature flags preserving old semantics, or
  mixed-version support. Delete rather than retain.
- The eight stages are **implementation workstreams on one branch**, not
  independently deployable versions.
- The only preserved external contract is the pinned Durable Streams
  protocol on `/v1/stream/{name}` — a standards surface, not a
  compatibility shim.
- Rollback after new-format writes = delete the fresh namespace and
  redeploy. Old binary never reads new layout, new binary never reads
  old layout.

---

## 2. Completed before this work package (context)

Routing v3 (unified routing keys + compact postings) shipped and its
external review was fully addressed — physical scaling with real child
routes and a ≥1.8× capacity gate, transition-safe reads, oversized-run
progress with end-to-end `consumed_to`, split-safe Stream-Seq and
per-routing-key producer lanes, hard 4× read amplification, one
process-wide postings cache, windowed scaler sketches, merge execution,
keyed SSE across lineage, and a COGS break-even model. See
`docs/ROUTING-V3.md` §11 for measured results and disposition.

---

## 3. Completed in this work package

### Foundation (`4e70510`)
- `StreamDesc` gains `sealed`, `watch_definitions`, `layout_version`.
- **Layout gate**: one decode chokepoint (`registry::decode_desc`)
  refuses any descriptor not at `LAYOUT_VERSION = 3` with
  `unsupported_storage_layout` — the clean switch is enforced by the
  binary, proven by a test that feeds it an old-shape descriptor.
- New `src/product.rs`: plural route `/v1/streams/{name}` with canonical
  hierarchical names (1–512 B, no empty/`.`/`..` segments, `__ds`
  reserved, reserved subresource finals), the stable product error
  schema `{error:{code,message,details,retryable}}`, and hard rejection
  (never translation) of `Stream-Encryption-Key`, `Stream-Key`, `?key=`,
  `?offset=` on the product surface.
- Typed creation v1 (Stage 7 core): `{format, expiry, watches}` +
  `Prisma-Encryption-Key`; normalized idempotence → 201/200/409;
  metadata GET leaks no fingerprints/segments/layout.
- Collection seal v1: durable, monotonic, idempotent, collection-wide;
  the raw default-key view observes it (409 append, `Stream-Closed` at
  the drained tail).
- `src/protocol_pin.rs`: pinned-baseline constants with an `UNPINNED`
  sentinel the release gate must refuse.

### Stage 3 alignment + legacy deletion (`f2aa631`, `7975ca7`)
Deleted, not retained:
- the legacy child-stream auto-scaler (`src/scaler.rs`), ops-bucket
  `segmap.json`, `<parent>#<n>` child registry streams, recursive HTTP
  append routing, lazy child creation, `/v1/debug/scaler`;
- static per-key layouts (`read_per_key`, `ordering`, `segment_count`,
  `is_per_key`/`segment_for`/`segment_hash`, the ordinal resolve arm);
- `StreamDesc.scaling` and every branch on it; the legacy `/segments`
  customer route;
- **the entire v1 history lane**: per-stream encrypted DBs, the covering
  `k!` full-frame index writer, `HistReaders` + DbReader coverage
  probes, `read_history`, the v1 record codec, the `force_v1` hatch, and
  `AppState.hist_readers` plumbing. A zero-route tail with data is now
  counted (`ABSORB_ZERO_ROUTE_DROPPED`), warned and dropped; a
  non-v2 history range errors `unsupported_storage_layout`.

Added: `SegmentDesc.successors` **persisted** atomically with each seal
at split/merge Phase B (derivation only as defensive fallback).

Fourteen v1-machinery DSTs deleted with their subject; suite runtime
43 s → 30 s.

### Stage 4 — append/appendMany + cursor codec (`63457b6`, `4fc7b6d`)
- `src/product_cursor.rs`: three token classes with explicit kind bytes
  (protocol offsets / KEY cursors / SCAN cursors), 16-byte HMAC keyed by
  HKDF(stream encryption key, epoch) — cursors cannot be minted or
  edited without the stream key, and cannot cross stream, tenant,
  routing key or snapshot bounds. Scan cursors embed their snapshot
  (map version, per-segment ends, expiry, ≤16 KiB). Both decoders check
  **class before length** so wrong-endpoint use always reports
  `wrong_cursor_kind`.
- `POST /v1/streams/{name}/records` and `/records:batch`: payload shape
  never changes operation meaning — single append stores exactly one
  message (arrays stay array-valued via the protocol's own `[value]`
  flattening, body slice, no DOM reserialization), batch stores
  element-wise (JSON only, 405 for bytes, empty rejected, 10 k cap,
  atomic and contiguous per key). Both compile to the **one shared
  committer path**; producer headers pass through and a producer 204
  maps to `{duplicate: true}`. Product response
  `{cursor, count, duplicate, sealed}`; errors in the product schema
  with `retryable` + `Retry-After`.

---

## 4. Remaining plan (tracked as session tasks #75–#87)

Goal **#75** closes only when every task below is done *and* the full
battery passes.

| # | Task | Notes |
|---|------|-------|
| 79 | **Stage 6** — read, subscribe, scan | `GET /records` with `Prisma-Next-Cursor`/`Up-To-Date`/`Sealed`; internal `:long-poll` / `:sse` transports over the existing lineage reader; `:scan` snapshot traversal using the ScanCursor codec (already built); remove unkeyed product-read ambiguity; committed-vs-speculative cursor discipline |
| 80 | **Stage 5** — producer sessions | Product `ProducerCheckpoint` gains `last_request_hash` (409 `producer_sequence_reused` on same tuple + different body); raw Stream-Seq keeps a separate standards checkpoint; SDK `ProducerStateStore`, `bumpEpoch`, opt-in auto-claim, error taxonomy |
| 81 | **Stage 2a** — consumer groups | Largest new machinery: PUT/GET/DELETE consumers, `:pull` / `:settle`, per-(consumer, routing key) FIFO with one active lease, opaque message IDs, generation-fenced lease tokens, DLQ (durable before source settle, crash-idempotent), state in the owning shard's commit path — no per-consumer DB/manifest/namespace/LIST |
| 82 | **Stage 2b** — watches | Immutable definitions (already in the descriptor), watch-key derivation, journal ingestion strictly after durability + read visibility, signed observation URLs (observation capability only), edge collapsing, resync on stale cursor |
| 83 | **Stage 1** — remove profiles | Depends on 2a/2b: delete `profile`, `queue_max_deliveries`, touch fields, every `profile ==` branch, `/queue/*` and `/touch/*` routes; touch journal machinery is **reused** by watches through capability registration; `profile_branch_executions == 0` gate |
| 84 | **Stage 7** — typed creation (finish) | Full normalization/`config_hash`, raw-PUT ↔ product-create dual contract (§14), remaining validation bounds |
| 85 | **Stage 8** — naming, lifecycle, routes, SDK | Seal with atomic final append, delete/expiry lifecycle, `GET /v1/streams` catalog listing, final SDK in `sdk/` (`StreamsClient`, `Stream.append/appendMany/producer/read/subscribe/scan/consumer/watch/metadata/seal/delete`), security/redaction |
| 86 | **Appendix** — conformance + CI + dual-surface | Record real values in `src/protocol_pin.rs` + `CONFORMANCE.md`; CI jobs (DS server conformance, product conformance, dual-surface equivalence, DST, request-cost); the 12-case dual-surface corpus |
| 87 | **Final gates** — the completion bar | Pinned DS server (239) + client conformance; product conformance; dual-surface equivalence; removed-surface grep gates; **fresh-namespace destructive cutover test** (new binary refuses an old-layout namespace); DST safety+liveness; postings COGS gates vs recorded baselines; consumer/watch cost gates; wide-cardinality memory gates; real Compute + Tigris field campaign with verified teardown |

Dependencies: 83 blocked by 81+82; 87 blocked by everything; 75 blocked
by all.

---

## 5. Working agreements / gotchas

- **Soak secrets live only in `$SOAK_HOME` (`~/.streams-soak`)** — never
  write tokens or keys into the working tree. Compute token:
  `$HOME/.streams-soak/platform-token.txt` (mode 600).
- Prisma Compute requires **x86_64-musl** binaries; aarch64 deploys
  crash-loop silently.
- Every field run includes **verified teardown**.
- Never weaken I1/coverage assertions to make tests pass.
- **Verify suite results with `grep -E "^test result"`, not a plain
  `grep FAILED`** — a grep-based commit gate let two red tests through
  once (fixed in `4fc7b6d`).
- Campaign rigs: `bench/costab/` (`run-keyed.sh`, `run-split.sh`,
  `keyed-compare.py`); rig binaries at `~/.streams-soak/rig/bin`.
- Harness traps already paid for: HTTP header lookups must be
  case-insensitive; byte-ratio workloads need genuinely incompressible
  payloads; read-ahead sizing must not flood the shared block cache.

---

## 6. Open follow-ups outside this package

- **Warm-scan block refetch** (spawned task chip): both the covering and
  postings read paths refetch data blocks on warm re-reads (~1–2 GETs
  per 1 KiB record, warm ≈ cold) — a shared slatedb scan-vs-block-cache
  issue, ranked below the correctness/scaling work by the reviewer.
- Queue and state-protocol profiles remain **pinned single-segment** by
  explicit posture until their per-segment consumer-state design lands
  (a product decision, not a gap in the scaling machinery).
