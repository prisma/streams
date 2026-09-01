# Wire Characterization Matrix (WP-00 deliverable 3)

Baseline for every refactor PR that moves a handler: each entry pins the current method, route, status, headers, error envelope/codes, CORS, body-vs-auth order and metering identity, with the source reference backing it. Characterized from the handlers at commit `685ea035`. Regenerate by re-reading the code, not by editing this file.

# Wire characterization matrix — prisma/streams (branch `slate`)

## 0. Cross-cutting (every route)

- Router: `router()` at `src/http.rs:1552`. One listener, one axum router (`src/main.rs:2500-2513`, h1 only via `serve_h1`).
- **Every response** (including axum-default 404/405) passes `map_response_with_state` adding `x-content-type-options: nosniff` and `prisma-streams-origin: <instance marker>` (`src/http.rs:1842-1854`). Purpose: distinguish server 404 from platform-edge 404.
- Middleware `track_inflight` (`src/http.rs:885-944`): pre-auth survival shed only — if inflight > 4× `admit_max_inflight` AND path starts with `/v1/stream` (note: matches BOTH `/v1/stream` and `/v1/streams` prefixes): instant `503`, body `{"error":{"code":"overloaded","message":"retry"}}` (JSON), `retry-after: 1`. No tarpit pre-auth.
- **Two error envelopes**:
  - Raw: `err_resp` (`src/http.rs:1879-1886`) → `Content-Type: application/json`, body `{"error":{"code":<c>,"message":<m>}}`. No `Cache-Control`.
  - Product: `perr` (`src/product.rs:36-57`) → `application/json`, `Cache-Control: no-store`, body `{"error":{"code":<c>,"message":<m>,"retryable":<bool>,"details"?:<json>}}`.
- Auth plumbing: raw surface uses typed per-operation credential checks (`raw_surface_authorization`, `src/http.rs:447`); product surface gates on parsed route BEFORE body (`product_auth_gate`, `src/product.rs:915-962`; wrapper order at `src/http.rs:2287-2376`). Enforce-mode error taxonomy via `auth_failure_response` (`src/product.rs:660-708`): 421 `wrong_cell` (+`prisma-error-code: wrong_cell` header), 503 `policy_stale`/`grants_stale`/`keys_stale` (retryable), 403 `project_not_active`/`credential_not_active`/`missing_scope`/`prefix_denied` (exact `code` = `AuthError::kind()`), else 401.

---

## 1. Raw "Durable Streams" surface — `/v1/stream/{*name}` (pinned protocol; `src/protocol_pin.rs`, pinned to `@durable-streams/server-conformance-tests@0.3.6`)

Dispatch: `stream_entry`/`stream_entry_inner` (`src/http.rs:2378-2536`). Operation class derived from METHOD for authz: PUT/DELETE→`raw-lifecycle`, POST→`raw-append`, else `raw-read` (incl. OPTIONS!). Successful responses increment the fleet load vector (`src/http.rs:2399-2402`).

**Common pre-handler behavior (all methods, before any body read):**
- 401 `unauthorized` ("bearer token required") if no deployment bearer (Off/Shadow) or static-bridge/workload JWT with the right `operations` claim (Enforce) — `src/http.rs:2424-2430`.
- 403 `reserved_stream` for names starting with `_` (system namespace) — `src/http.rs:2437-2443`.
- Unknown query params are silently IGNORED (`ReadParams` has no `deny_unknown_fields`; `offset, format, live, timeout, key, cursor, sig` are the only parsed fields, `src/http.rs:1970-2013`). `?sig=` is dead (no reader). `?key=` on GET → 400 `unknown_field` (`src/http.rs:6633-6639`).

### 1.1 `PUT /v1/stream/{name}` — create (+ idempotent re-PUT, fork create, close-on-create)
Handler: `create_stream` (`src/http.rs:3235-4279`). Body read AFTER auth (`to_bytes` at `src/http.rs:2446-2451`, cap `max_body_bytes()` = 32 MiB default, `src/http.rs:32-69`).
- **Request headers**: `Stream-Encryption-Key` (required), `Content-Type`, `Stream-TTL`, `Stream-Expires-At` (RFC3339; mutually exclusive with TTL), `Stream-Closed: true` (close-on-create), `Stream-Forked-From`/`Stream-Fork-Offset`/`Stream-Fork-Sub-Offset` (forks), `Stream-Key-Version`. Rejected: `Stream-Ordering`/`Stream-Segments`/`Stream-Scaling` → 400 `unified_routing`.
- **Success**: `201 Created` (new) or `200 OK` (idempotent). Headers: `Content-Type: <configured media type>`, `Stream-Next-Offset: <tail token>` (`tail_token`, `src/http.rs:3062`), `Location: http://{host}/v1/stream/{name}` on 201 only (`src/http.rs:4273`), `Stream-Closed: true` if closed (`src/http.rs:4275-4277`). Empty body. 201 also emits ops event `stream_created` (`src/http.rs:2461-2471`).
- **Errors** (all `err_resp` envelope): 400 `reserved` / `missing_key` / `invalid_key` / `invalid_request` (TTL+Expires-At, bad Expires-At) / `invalid_ttl` / `unified_routing` / `fork_headers` / `fork_segmented_source` / `invalid_fork_offset` / `fork_offset_beyond_end` / `invalid_fork_sub_offset` / `fork_sub_offset_beyond_end` / `fork_sub_offset_empty_source` / `invalid_json` (initial body on JSON stream); 409 `not_ring_owner` (+`Streams-Replay-To: <owner>` header, `src/http.rs:3284-3292`) / `config_mismatch` / `creating` (different request in flight) / `gone` (name retained for forks) / `fork_content_type_mismatch` / `fork_source_gone` / `fork_source_changed` / `fork_target_changed`; 403 `wrong_key`; 404 `not_found` (fork source); 413 `record_too_large` / `too_large` (body overflow); 429 `overloaded` (committer queue full); 408 `append_timeout` (initial body); 500 `internal`.
- **Body/auth order**: auth → reserved → body. Buffering failure → 413 `too_large`.
- **Metering**: the initial-content append carries a `BillingRef` (ingest bytes billed at commit, identity = `billing::identity_of(state,&desc)`, `src/http.rs:4161-4166`); skipped for `_`-reserved streams. No `meter_append_request` op count for PUT.

### 1.2 `POST /v1/stream/{name}` — append / close
Handler: `append` → `append_core` (`src/http.rs:4726-4795`, `4942-6049`).
- **Request headers**: `Stream-Encryption-Key` (required), `Content-Type` (required with body; must match configured media type → else 409 `content_type_mismatch`), `Stream-Closed: true` (append-and-close / close-only), `Producer-Id`+`Producer-Epoch`+`Producer-Seq` (all-or-none → 400 `invalid_producer`), `Stream-Seq` (total-order sequence → 409 `seq_conflict`), `Stream-Timestamp`, `Stream-Key-Version`. Rejected: `Stream-Key` → 400 `unknown_field` (`src/http.rs:5505-5511`); `x-seal-final` → 400 `unknown_field` (`src/http.rs:5259-5265`).
- **Body**: JSON streams — top-level array = batch (one record per element), any other JSON value = one record; empty array → 400 `invalid_json`-class message via `json_entries(allow_empty=false)` (`src/http.rs:3074-3089`). Non-JSON: whole body = one record. Empty body without close → 400 `empty_body`.
- **Success** (`src/http.rs:5951-5977`): status `204 No Content` for non-producer appends, duplicates, close-only, and synthetic-producer closes; `200 OK` only for non-duplicate producer appends. Headers: `Stream-Next-Offset: <token>` (plain scalar token on single-segment streams; `encode_ep` segment-prefixed token once `desc.segments` exists, `src/http.rs:5777-5791`); `x-ack-closed: true|false` ALWAYS (internal header — stripped only on the product surface); `Producer-Epoch`/`Producer-Seq` on producer acks; `Stream-Closed: true` if this ack closed. Empty body.
- **Errors**: 400 `missing_key` / `invalid_producer` / `missing_content_type` / `empty_body` / `invalid_json` / `invalid_body` / `unknown_field` / `producer_epoch_seq`; 403 `wrong_key`; 403 `producer_stale_epoch` (+`Producer-Epoch: <current>` header); 404 `not_found`; 410 `gone` (soft-deleted / expired-with-forks, `gone_or_missing` `src/http.rs:2676-2690`); 409 `content_type_mismatch` / `stream_closed` (+`Stream-Closed: true`, +`Stream-Next-Offset`, +`Cache-Control: no-store`, `src/http.rs:5989-6001`) / `seq_conflict` / `producer_seq_gap` (+`Producer-Expected-Seq`, +`Producer-Received-Seq`) / `producer_sequence_reused` / `sealed` / `seal_superseded`; 413 `body_too_large` (declared-length refusal after bounded ≤8 MiB drain, `src/http.rs:5047-5079`) / `too_large` (buffering) / `record_too_large` / `payload_too_large` (per-stream bucket capacity); 429 `overloaded` (admission cap, 25 ms tarpit, `retry-after: 1`; queue full; RSS shed, `retry-after: 2`) / `engine_backpressure` (`retry-after: 2`) / `limit_bytes_per_sec` / `limit_requests_per_sec` / `limit_records_per_sec` (+computed `retry-after`, `src/usage.rs:236-274`); 503 `creating` (`retry-after: 1`) / `maintenance_backpressure` (`retry-after: 5`) / `shard_moving` (`retry-after: 1`) / `segment_transition` (`retry-after: 1`) / `seal_incomplete`; 408 `append_timeout`; 500 `internal`.
- **Sealed-segment retry wrapper**: on segmented descriptors, 409+`Stream-Closed` from a mid-transition segment is retried internally up to 4× with descriptor refresh; terminal fallback 503 `segment_transition` (`src/http.rs:4761-4794`).
- **Body/auth order**: bearer auth → stream-key check → admission gates → declared-length 413 → body buffering (`buffer_body_charged`, project pressure-charged) → CT/body validation → seal intent → enqueue. Explicit comments: `src/http.rs:5041-5046` (R25-E), `src/http.rs:2295-2297` (product side).
- **Metering**: success → `meter_append_request` (append_requests+1, `src/http.rs:2487-2491`); billed ingest bytes metered at the committer via `BillingRef` (atomic with the records, `src/http.rs:5679-5688`); skipped for `_`-reserved streams or `BILLING_METER=off`. Per-shard usage token buckets are the 429 source (contract, not billing).

### 1.3 `GET /v1/stream/{name}[?offset=][&live=][&timeout=][&format=frames][&cursor=]` — read
Handler: `read` → `read_inner` (`src/http.rs:6621-7133`), forks → `read_fork_inner` (`6430-6619`), segmented maps → `read_v3_lineage_inner` (`7711-8276`). Query params: `offset` (token | `now` | absent=0), `live=long-poll|sse|true` (`true` ≡ long-poll), `timeout` (`3s` default, max 25 s `MAX_LONG_POLL`), `format=frames`, `cursor` (echoed into `Stream-Cursor` via `interval_cursor`, `src/http.rs:6402`). `?key=` → 400 `unknown_field`.
- **Success plain read**: `200 OK`; body = JSON array (json streams), concatenated payloads (bytes, standard path), or payload-per-line (bytes, LINEAGE path, `src/http.rs:8236-8243`), or encrypted frames with `Content-Type: application/x-durable-stream-frames` when `format=frames` (`src/http.rs:7034-7056`). Headers: `Content-Type: <stream ct>`, `Stream-Next-Offset: <resume token>` (scalar token single-segment; `encode_ep` segment token on lineage), `ETag: "<epoch8>-<from>-<end>-<closed>"` (standard path only, `src/http.rs:6411-6419`), `Cross-Origin-Resource-Policy: cross-origin`, `Stream-Up-To-Date: true` when complete, `Stream-Closed: true` when closed-and-caught-up. Long-poll adds `Stream-Cursor: <20s-interval cursor>` + `Cache-Control: no-store`; fork path sets `Cache-Control: no-store` always.
- **`offset=now` (no live)**: 200, empty/`[]` body, `Stream-Up-To-Date: true`, `Cache-Control: no-store`, CORP (`src/http.rs:6861-6876`).
- **Long-poll timeout / closed-at-tail**: `204 No Content` + `Stream-Next-Offset`, `Stream-Up-To-Date: true`, `Stream-Cursor`, `Cache-Control: no-store`, +`Stream-Closed` if closed (`src/http.rs:6955-6967`; lineage variant `8154-8171`).
- **Conditional**: `If-None-Match` equal to computed ETag → `304 Not Modified` + `ETag` (standard path only, `src/http.rs:7013-7021`).
- **`live=sse`**: 200 SSE stream (see §4).
- **Errors**: 400 `missing_key` / `invalid_live` / `missing_offset` (live without offset) / `invalid_offset` / `unknown_field` (`?key=`) / `keyless_live` (live on multi-segment map without key; lineage SSE without key — `src/http.rs:7743-7764`); 403 `wrong_key`; 404 `not_found`; 410 `gone`; 409 `cursor_beyond_tail` (deliver=applied only — unreachable on public raw route since `deliver` is serde-skipped; reachable via `/v1/internal/segment-read` with `streams-internal-deliver: applied`); 409 `not_ring_owner`+`Streams-Replay-To` / `incompatible_topology` (lineage SSE build); 503 `creating` (`retry-after: 1`) / `segment_transition` / `temporarily_unavailable` / `subscription_capacity` (`retry-after: 5`); 500 `internal`.
- **Seal-gap data-dependence**: mid-split responses may carry records + resume cursor but NEVER `Stream-Closed` / final `Stream-Up-To-Date` (`src/http.rs:7700-7710`, `8268-8273`).
- **Body/auth order**: no body; query parsed by axum extractor before handler.
- **Metering**: `meter_read(desc, payload_bytes, records)` after response assembly, skipped when `params.internal` (`src/http.rs:7128-7131`); empty long-poll/204 still meters one op with 0 bytes (`src/http.rs:6952-6954` comment). **Gap**: `read_v3_lineage_inner` contains NO `meter_read` call — reads of split streams (raw and product keyed reads routed through it) are not billing-metered (verified: `meter_read` appears only at 6466/6604/6781/7130).

### 1.4 `HEAD /v1/stream/{name}` — tail metadata
Same handlers, `head_only=true`. **200** + `Content-Type`, `Stream-Next-Offset`, `Cache-Control: no-store`, `Stream-Closed: true` if closed, `Stream-TTL: <remaining secs>` when TTL+expiry set (`src/http.rs:6772-6797`; fork: `6464-6482`; lineage: `8175-8184`, segment token). Never slides idle TTL. Metered: `meter_read(desc, 0, 0)` — one read op, zero bytes (`src/http.rs:6780-6782`). Errors as GET minus body/offset validation.

### 1.5 `DELETE /v1/stream/{name}`
Handler: `delete_stream` (`src/http.rs:4281-4311`) + `delete_lifecycle` (`4511-4703`). **204 No Content** on success (idempotent on tombstones — a deleted stream with pending fork-ref debt re-runs cleanup and still answers per `gone_or_missing`: 410 `gone` for soft-deleted/expired-with-forks, 404 `not_found` otherwise); 500 `internal`. No request headers required (no key check!). Emits ops event `stream_hard_deleted`; billing closure submitted per segment (storage gauge zeroed). Not op-metered.

### 1.6 `OPTIONS /v1/stream/{name}` — preflight
`src/http.rs:2510-2529`. **NOTE: authorized first** — unlike the product surface, the raw dispatch runs `raw_surface_authorization` BEFORE the method match, so a credential-less preflight gets **401**, not 204. When authorized: `204 No Content`, `access-control-allow-origin: *`, `access-control-allow-methods: GET, PUT, POST, HEAD, DELETE, OPTIONS`, `access-control-allow-headers: authorization, content-type, stream-encryption-key, stream-closed, stream-ttl, stream-forked-from, stream-fork-offset, stream-fork-sub-offset, producer-id, producer-epoch, producer-seq, if-none-match`, `access-control-max-age: 600`. No `access-control-expose-headers` on the raw preflight. Raw data responses carry no CORS headers at all (only CORP on read bodies).

### 1.7 Other methods
405 `method_not_allowed` ("unsupported method", err_resp) — `src/http.rs:2530-2534`.

### 1.8 `/v1/stream/__ds/{*rest}`
`ds_reserved` (`src/http.rs:2018-2024`): any method → 404 `reserved` ("__ds is the reserved Durable Streams control namespace"). Bare `/v1/stream/__ds` (no trailing slash) falls into the wildcard route and hits the `_` system-namespace guard → 403 `reserved_stream`.

---

## 2. Product surface — `/v1/streams` + `/v1/streams/{*name}` (spec Stages 4–8)

Route registration: `src/http.rs:1819-1827`. Wildcard dispatch: `product_entry_axum` → `product_entry_axum_inner` → `product::product_entry` (`src/product.rs:1017-1308`). Verb grammar: `:batch, :long-poll, :sse, :pull, :settle, :seal, :scan` — only these split off the final segment (`strip_verb`, `src/product.rs:440-454`); unknown `:foo` stays part of the name. Route classes: Collection, `…/records`, `…/consumers/{c}`, `…/watches`, `…/watches/{w}`, `…/watches/{w}/keys/{key}`, `…/usage` (+`…/usage/current`) (`classify_route`, `src/product.rs:458-528`; `split_subresource` longest-first, `1325-1353`).

**Wrapper order** (`src/http.rs:2287-2376`): shadow-observe → `product_auth_gate` (route parses FIRST — grammar errors are not auth outcomes; enforce: customer JWT + scope + prefix per `required_scope`, `src/product.rs:584-640`) → project admission (§17.3) → POST-only project memory gate → `_`-namespace 403 `reserved_stream` → **body buffering** (413 `body_too_large`, pressure-charged) → handlers → `tag_project` → `with_product_cors`. Body always read AFTER auth (`src/http.rs:2295-2297` comment).

**Every response** gets `with_product_cors` (`src/product.rs:969-986`): `access-control-allow-origin: *` (if absent), `access-control-expose-headers: content-type, retry-after, prisma-next-cursor, prisma-up-to-date, prisma-sealed, prisma-next-scan-cursor, prisma-scan-complete, prisma-routing-key, prisma-durable-cursor, prisma-pending-from, prisma-consumer-version`, and REMOVES `x-ack-closed`.

**Legacy-input rejection** (`reject_legacy_inputs`, `src/product.rs:125-181`): headers `stream-encryption-key, stream-key, stream-profile, stream-touch-templates, stream-queue-max-deliveries, stream-ordering, stream-segments, stream-scaling, stream-ttl, stream-expires-at` → 400 `unknown_field`; query keys `key`, `offset` → 400 `unknown_field`; `routingKey` on non-GET/HEAD → 400 `unknown_field`.

**Quota refusals** (`quota_refusal_response`, `src/product.rs:846-913`): 429 `stream_limit` (not retryable), 429 `queued_bytes`, 429 `project_rate_limit` (+`retry-after`), 429 `project_concurrency_limit`, 503 `project_tracker_capacity`, 429 `project_memory_pressure` (`retry-after: 1`). `strict_query` rejects unknown/duplicate query keys with 400 `unknown_parameter` / `duplicate_parameter` (`src/product.rs:3629-3665`).

### 2.1 `OPTIONS /v1/streams/{*name}` — preflight
Answered inside `product_entry` before auth (`src/product.rs:1026-1050`): 204, `access-control-allow-origin: *`, `access-control-allow-methods: GET, PUT, POST, DELETE, OPTIONS`, `access-control-allow-headers: authorization, content-type, prisma-encryption-key, prisma-routing-key, producer-id, producer-epoch, producer-seq, if-none-match, prisma-consumer-version`, `access-control-expose-headers: *`, `access-control-max-age: 600`.

### 2.2 `PUT /v1/streams/{name}` — create collection
`product_create` (`src/product.rs:1372-1604`). Scope: `streams.create` (+ `watches.manage` if body carries watches). Body: typed JSON doc `{format:{kind:"json"|"bytes",contentType?}, expiry?:{idle}|{at}, watches?}` (`deny_unknown_fields`; `parse_create_doc`, `src/product.rs:241-394`, all failures 400 `invalid_config`; >256 KiB config body also 400 `invalid_config`). Requires `Prisma-Encryption-Key` (400 `missing_key` / 400 `invalid_key`).
- **Success**: `201 Created` or `200 OK` (idempotent same-config), body = metadata JSON `{name, contentType, createdAt(RFC3339), sealed, expiry?, watches?, epoch?}` — `epoch` only when watches exist — headers `Content-Type: application/json`, `Cache-Control: no-store` (`metadata_response`, `src/product.rs:1640-1677`).
- **Errors**: 400 `invalid_config`/`missing_key`/`invalid_key`; 403 `wrong_key`; 409 `config_mismatch` / `gone` (name retained for live forks); 409 `not_ring_owner` + `Streams-Replay-To` (via `ring_owner_check`, `src/http.rs:3202-3224` — raw err_resp envelope on the product surface!); 429 `stream_limit`; 503 `catalog_unavailable` (retryable); 500 `internal` (retryable).
- **Metering**: none (no append op; no records written — product create writes no initial content). Enforce-mode max_streams reservation (SR2-4) is a quota, not billing.

### 2.3 `GET /v1/streams/{name}` — metadata
`product_metadata` (`src/product.rs:1679-1710`). Scope: `metadata.read`. No encryption key required. **200** + metadata JSON (same shape as create). 503 `creating` (retryable, initializing); 404 `not_found`; 500 `internal`.

### 2.4 `DELETE /v1/streams/{name}`
Maps to the raw `delete_stream` via `crate::http::product_delete` (`src/http.rs:3227-3233`). Scope: `lifecycle.manage`. **204 No Content**; errors: **raw envelope** (404 `not_found`, 410 `gone`, 500 `internal`) — an intentional shared-core reuse, so this is the one product route whose error body lacks `retryable`/`details`.

### 2.5 `POST /v1/streams/{name}:seal` — seal collection
`product_seal` (`src/product.rs:1714-2074`). Scope: `lifecycle.manage`. Body empty → seal-only (`product_seal_only`, `2756-2789`); body `{final: <json|null>, routingKey?}` → atomic seal-with-final (`deny_unknown_fields`; 400 `invalid_body`).
- **Success**: `200 OK`, body `{"sealed": true}`, `application/json`, `Cache-Control: no-store` (both the idempotent re-entry and completed paths, `src/product.rs:1929-1934`, `2063-2068`, `2781-2786`).
- **Errors**: 404 `not_found`; 503 `creating`; 400 `missing_key` / `invalid_routing_key` (>1024 B or non-header-safe) / `invalid_producer`; 403 `wrong_key`; 409 `sealed` (conflicting claim); 409 `producer_sequence_reused` (final didn't close); 409 `sealing` (another live final-bearing claim; via `seal_error_response`, retryable=true); 503 `seal_incomplete` (retryable); 500 `internal` (also marked retryable=true by `seal_error_response`, `src/product.rs:2800-2814`); 413 `payload_too_large` (final record over ingest bucket capacity). Translated append errors from the final record pass through `translate_append_response`.
- **Metering**: the final record's ingest bytes meter at the committer like any append; the op-count meter (`meter_append_request`) does NOT fire (internal `product_append_sealing`, principal=None — "lifecycle work, not customer append volume", `src/product.rs:3096-3099`).

### 2.6 `GET /v1/streams/{name}:scan[?cursor=][&maxBytes=]` — snapshot export
`product_scan` (`src/product.rs:4184-4576`). Scope: `records.read` (deliberately NOT metadata.read). Requires key. Strict query: `cursor`, `maxBytes` (clamp 4096–8 MiB, default 4 MiB; bad value → 400 `invalid_max_bytes`).
- **Success**: `200 OK`, body = JSON array of `{"routingKey":..,"value":..}` (json) or `{"routingKey":..,"valueB64":..}` (bytes); headers `Content-Type: application/json`, `Cache-Control: no-store`, `Prisma-Scan-Complete: true` OR `Prisma-Next-Scan-Cursor: <signed cursor>`.
- **Errors**: 400 `missing_key` / `invalid_cursor` (wrong kind, garbage, unknown segment); 410 `scan_expired` (cursor TTL 6 h, `SCAN_TTL_MS`); 403 `wrong_key`; 404 `not_found`; 503 `creating`; 409 `not_stream_owner`+`Streams-Replay-To` and other translated read errors via `translate_read_error`; 500 `internal`.
- **Metering**: `meter_read` once per page (payload bytes incl. peer-relayed pages, `src/product.rs:4571-4574`).

### 2.7 `POST /v1/streams/{name}/records` — append (single)
`product_append` → `product_append_inner` (`src/product.rs:3128-3394`). Scope: `records.append`. Requires `Prisma-Encryption-Key`; optional `Prisma-Routing-Key` (≤1024 B → 400 `invalid_routing_key`), producer triple. Body: JSON stream → one JSON value (wrapped `[value]`); bytes stream → opaque body (empty → 400 `empty_body`).
- **Success**: always `200 OK` (raw 204/200 both translated), JSON body `{"cursor": <signed KeyCursor>, "count": n, "duplicate": bool, "sealed": bool}`; headers `application/json`, `Cache-Control: no-store` (+internal `x-ack-closed`, stripped at the edge). Duplicate ⇒ `count: 0`, cursor = original commit position (`translate_append_response`, `src/product.rs:3400-3482`).
- **Error translation** (`src/product.rs:3484-3605`): raw code `producer_seq_gap`→409 `producer_gap` (+details `{expected, received}`); `producer_stale_epoch`→403 `stale_producer_epoch` (+details `{currentEpoch}`); `producer_sequence_reused`→409 same name; `producer_epoch_seq`→400 `producer_epoch_must_start_at_zero`; `stream_closed`→409 `sealed`; `maintenance_backpressure`→503 same name (retryable, keeps `retry-after`); `content_type_mismatch`→409 same name; 409+`Streams-Replay-To`→409 `not_stream_owner` (retryable, header preserved); by status: 404 `not_found`, 403 `stale_or_wrong_credentials`, 409+stream-closed `sealed`, 409 `conflict`, 413 `body_too_large`, 429 `rate_limited` (retryable), 503 `temporarily_unavailable` (retryable), else `append_failed`. Plus handler-local: 400 `missing_key` / `invalid_routing_key` / `invalid_body`; 405-class: batch on non-JSON stream → 405 `batch_unsupported_format`; 409 `sealed`/`sealing` via `refuse_if_sealed` (both emit code `sealed`); 503 `creating`; quota 429s.
- **Metering**: success → `meter_append_request` (dispatch choke point, `src/product.rs:1093,1108`) + committer ingest bytes. Project `queued_append_bytes` charged for the handler's lifetime; `admit_append` project quotas enforced.

### 2.8 `POST /v1/streams/{name}/records:batch` — appendMany
Same handler with `batch=true`. JSON streams only → 405 `batch_unsupported_format` otherwise. Body must be a non-empty JSON array (400 `invalid_body` / 400 `empty_batch`), ≤ 10,000 records (400 `batch_too_large`). Same success/error contract as 2.7 with `count` = element count.

### 2.9 `GET /v1/streams/{name}/records[?routingKey=][&cursor=][&maxBytes=][&deliver=][&waitMs=]` — keyed read
`product_read` (`src/product.rs:3832-4155`). Scope: `records.read`. Strict query: `cursor, deliver, maxBytes, routingKey, waitMs`. Requires `Prisma-Encryption-Key`. `cursor` = signed KeyCursor | `beginning` | `now` | absent; bad cursor → 400 `invalid_cursor`. `deliver=durable|applied` (400 `invalid_deliver`); `applied` + SSE → 400 `deliver_sse_unsupported`; `applied` on forks → 400 `deliver_unsupported_fork`. `maxBytes` clamp 4096–8 MiB (400 `invalid_max_bytes`); `waitMs` ms (400 `invalid_wait_ms`).
- **Success**: status from the raw machine (200 with page, or 204 long-poll timeout); headers `Content-Type` (stream ct), `Cache-Control: no-store`, `Prisma-Next-Cursor: <signed>`, `Prisma-Up-To-Date: true`, `Prisma-Sealed: true`, and in applied mode `Prisma-Durable-Cursor: <signed>` + `Prisma-Pending-From: <index>`. NO `Stream-*` headers ever (fresh response built, `src/product.rs:4129-4154`).
- **Errors** (`translate_read_error`, `src/product.rs:3775-3824`): 409+`Streams-Replay-To` → 409 `not_stream_owner` (retryable, header preserved); 404 `not_found`; 403 `wrong_key`; 410 `gone`; 429 `rate_limited`; 400 `invalid_cursor`; 409 `cursor_beyond_tail` (deliver=applied only); 503 `temporarily_unavailable`; else `read_failed` (retryable). `retry-after` forwarded. Plus handler-local 400s above and 503 `creating`.
- **Metering**: raw-core `meter_read` (params.internal=false) + post-hoc project read debit (`check_read_quota`/`debit_read_response`, `src/product.rs:794-827`). SSE variant is governed by subscription slot instead.

### 2.10 `GET /v1/streams/{name}/records:long-poll` — subscribe (long-poll)
Same as 2.9 with `live=long-poll` forced; `waitMs` maps to raw `timeout` (server cap 25 s). 204 timeout answers carry `Prisma-Next-Cursor` + `Prisma-Up-To-Date` (+`Prisma-Sealed`).

### 2.11 `GET /v1/streams/{name}/records:sse` — subscribe (SSE)
Same entry, `live=sse`, surface=Product. Response: `200 OK`, `Content-Type: text/event-stream`, `x-accel-buffering: no`, `Cache-Control: no-cache`, `Cross-Origin-Resource-Policy: cross-origin`, `Stream-SSE-Data-Encoding: base64` for binary streams (`src/sse/session.rs:915-924`). Wire frames (`src/sse/wire.rs`): `event: data` (`data:[<json>]`, text lines, or base64) + product control `event: control\ndata:{"nextCursor":"<signed>",("upToDate":true)?,("sealed":true)?}` — never a raw offset token (appendix §13). Refusals before the stream opens: 503 `subscription_capacity` (instance slot, `retry-after: 5`, `src/http.rs:7195-7214`; or feed budget, `src/sse/session.rs:229-234`); lease failures via `lease_refusal_response` (`src/sse/auth.rs:90-102`): 401 `token_expired`/`credential_*`/`grant_changed`/`ownership_changed`, 403 `project_missing`/`project_not_active`, 503 `policy_stale`/`grants_stale` — **raw err_resp envelope on a product route** (wrapped in CORS only). 409 `not_ring_owner`+`Streams-Replay-To`, 409 `incompatible_topology`, 503 `temporarily_unavailable`/`segment_transition` at connect (`src/http.rs:7920-7951`).
- **Metering**: payload metered at the SSE yield boundary via `meter_read_chunk` (`src/http.rs:7152-7157` comment); subscription occupies the project's §17.2 live-subscription slot for the connection's lifetime (`attach_subscription_guard`, `src/product.rs:832-843`).

### 2.12 `PUT /v1/streams/{name}/consumers/{consumer}` — create/configure consumer
`product_consumer_put` (`src/product.rs:4730-4957`). Scope: `consumers.configure` (+ `dlq.configure` + prefix over target when `deadLetterStream` set). Requires key. Body (optional) `{visibilityTimeoutMs?, maxAttempts?, deadLetterStream?, maxBatchRecords?}` (`deny_unknown_fields`; 400 `invalid_config`); clamps: visibility 1 s–12 h, attempts 1–1000, batch 1–1000. DLQ validation errors: 400 `invalid_config` (bad name / self-reference), 400 `unknown_dead_letter_stream`, 400 `dead_letter_sealed`, 400 `dead_letter_key_mismatch`; 503 `unavailable` (registry).
- **Success**: `201 Created` (new) or `200 OK` (idempotent same config); body = config JSON `{name, visibilityTimeoutMs, maxAttempts, deadLetterStream, maxBatchRecords}`; headers `application/json`, `Cache-Control: no-store`, `Prisma-Consumer-Version: <opaque {epoch,generation} token>`.
- **Errors**: 409 `consumer_deleting` (retryable; response CARRIES the deleting incarnation's `Prisma-Consumer-Version` for saga resume), 409 `consumer_config_conflict` (details = existing config JSON), plus `consumer_ctx` shared: 400 `missing_key`, 403 `wrong_key`, 404 `not_found`, 503 `creating`, 500 `internal`.
- **Metering**: none.

### 2.13 `GET /v1/streams/{name}/consumers/{consumer}`
`product_consumer_get` (`src/product.rs:4959-5002`). Scope: `metadata.read`. **200** + config JSON + `Prisma-Consumer-Version`, `Cache-Control: no-store`. 404 `unknown_consumer` (incl. non-Active states); shared ctx errors. Unmetered.

### 2.14 `DELETE /v1/streams/{name}/consumers/{consumer}`
`product_consumer_delete` (`src/product.rs:5848-6251`). Scope: `consumers.configure`. Requires key + **`Prisma-Consumer-Version` header** (400 `missing_consumer_version` / 400 `invalid_consumer_version`).
- **Success**: `204 No Content` + `Cache-Control: no-store` — also the no-touch answer when the collection is gone, the token's epoch died, the generation is superseded, or already deleted (idempotent ABA discipline).
- **Errors**: 409 `consumer_version_conflict` (token newer than record); 503 `unavailable` / `segment_unavailable` / `segment_cleanup_incomplete` / `segment_cleanup_failed` / `segment_map_unverified` / `segment_map_unstable` (all retryable); 403 `wrong_key` (only after epoch matches); 503 `creating`; 500 `internal`. Cross-owner segments relay via `/v1/internal/sweep-segment`.
- **Metering**: none.

### 2.15 `POST /v1/streams/{name}/consumers/{consumer}:pull` — receive messages
`product_consumer_pull` (`src/product.rs:6455-6725`). Scope: `consumers.pull`. Body (optional) `{max?, waitMs?, visibilityMs?}` (`deny_unknown_fields`; 400 `invalid_body`); clamps: max 1..=`maxBatchRecords`, visibility 1 s–12 h, wait ≤ 25 s (50 ms poll loop).
- **Success**: `200 OK`, body `{"messages": [{"id": <signed MessageId>, "routingKey", "attempts", "leaseToken": <signed LeaseToken>, "value"}], "backlog": n}` (`value` = JSON or base64 string); `application/json`, `Cache-Control: no-store`. Empty poll returns `{"messages": [], "backlog": n}` (still 200).
- **Errors**: 404 `consumer_not_found`; 409 `consumer_deleted` (generation-fenced); 409 `consumer_deleting` (via `load_consumer_record`, not retryable); 409 `not_stream_owner` via translate; 500 `internal`; shared ctx errors.
- **Metering**: `meter_pull` = queue_operations+1 + delivered payload bytes (`src/product.rs:6694`, `src/billing.rs:983-998`).

### 2.16 `POST /v1/streams/{name}/consumers/{consumer}:settle` — ack/retry/extend
`product_consumer_settle` (`src/product.rs:6727-6939`). Scope: `consumers.settle`. Body `{acks?, retries?, extends?}` of `{leaseToken, delayMs?, visibilityMs?}` (400 `invalid_body`). Invalid/foreign tokens counted as `stale`, never errors (spec §2.5).
- **Success**: `200 OK`, body `{"acked","retried","extended","dlq","stale","backlog","dlqBlocked"}`; `application/json`, `Cache-Control: no-store`.
- **Errors**: 404 `consumer_not_found`; 409 `consumer_deleted`; 409 `consumer_deleting`; 500 `internal`; ownership via translate.
- **Metering**: success → `meter_queue_op` (queue_operations+1, zero bytes) via `meter_op_if_ok` (`src/product.rs:1222-1224`).

### 2.17 `GET /v1/streams/{name}/watches` and `GET /v1/streams/{name}/watches/{watch}`
`product_watches_list` (`7040-7084`) / `product_watch_get` (`7086-7130`). Scope: `metadata.read`. No key required. **200**: list → `{"watches": [defs]}`; single → the definition JSON; `application/json`, `Cache-Control: no-store`. Errors: 404 `not_found`, 404 `unknown_watch`, 503 `creating`, 500 `internal`. Unmetered. Non-GET → 405 `method_not_allowed` ("watches are read-only (GET)").

### 2.18 `GET /v1/streams/{name}/watches/{watch}/keys/{keyHex16}[?cursor=][&timeoutMs=][&cap=]` — watch observation (§15 capability)
`product_watch_wait` (`src/product.rs:7157-7479`). **Self-authorizing**: `Authorization: Prisma-Watch <cap>` or `?cap=` (≤5 min expiry, bound to project+name+epoch+watch+key+GET); full `Prisma-Encryption-Key` also accepted. `product_auth_gate` lets this route through without a bearer (`watch_capability_carrier`, `src/product.rs:542-550`). Uniform 403 `watch_unauthorized` for missing/missing-stream/bad-capability (non-oracle; journaled); pre-auth global capability bucket 500/s (SR-3). Enforce mode: capability must still pass fresh policy (503 `policy_stale`, 403 `project_not_active`, project admission + subscription slot held for the wait).
- **Success**: `200 OK` (long-poll; `timeoutMs` ≤ 25 s, default 25 s), body: touched → `{"invalidated": true, "reason": "changed"|"resync", "cursor", "streamCursor"?}`; stale cursor → `{"invalidated": true, "reason": "resync", "cursor"}`; timeout → `{"invalidated": false, "cursor", "streamCursor"?}`. Headers: `application/json`, `Cache-Control: no-store`, `Referrer-Policy: no-referrer`.
- **Errors**: 400 `invalid_watch_key` (not 16 hex); 400 `invalid_timeout_ms`; 404 `unknown_watch` (post-auth only); 503 `creating`; 500 `internal`; strict-query 400s.
- **Metering**: none (no meter call).

### 2.19 `GET /v1/streams/{name}/usage[?month=YYYY-MM][&streamId=]` and `GET /v1/streams/{name}/usage/current`
`product_usage` (`src/product.rs:7621-7788`). Both spellings → `ProductRoute::Usage`. Scope: `usage.read`. Bearer only, NO record key. **200** `json_ok` — note: `json_ok` (`src/product.rs:5018-5024`) sets `Content-Type: application/json` but **no `Cache-Control`** (unlike every other product 200). Body: month row JSON (`projectId, streamId, streamName, month, status: provisional|finalized|corrected, ingestPayloadBytes, readPayloadBytes, storageByteSeconds(string), effective, correctionTotals, correctionList, nameAggregate, incarnations, metering{…}`, etc.). Errors: 400 `invalid_month`; strict-query 400s (`month`, `streamId` only); 404 `not_found`; 503 `usage_unavailable` (retryable); 500 `internal`. Unmetered.

### 2.20 `GET /v1/streams[?limit=][&cursor=][&prefix=]` — catalog (own route)
`product_list` (`src/product.rs:7484-7614`), mounted GET+OPTIONS at `src/http.rs:1820-1822`. Scope: `catalog.read`; enforce-mode prefix grant FILTERS items. Off/shadow: deployment bearer (401 `unauthorized`). **200**: `{"streams": [{name, contentType, sealed, createdAt(ms number)}], "cursor"?: <signed CatalogCursor>}`; `application/json`, `Cache-Control: no-store`. Errors: 400 `invalid_cursor` / `invalid_limit` / strict-query 400s; 500 `internal` (retryable); quota refusals. OPTIONS preflight: 204, `allow-methods: GET, OPTIONS`, `allow-headers: authorization, content-type, stream-encryption-key, stream-closed, stream-ttl, stream-forked-from, stream-fork-offset, stream-fork-sub-offset, producer-id, producer-epoch, producer-seq, if-none-match`, `expose-headers: *`, `max-age: 600` (`product_preflight`, `src/http.rs:2028-2047`). Other methods → axum 405.

### 2.21 `GET /v1/projects/{project}/usage[?month=YYYY-MM]` — project usage (own route)
`project_usage_axum` (`src/http.rs:2204-2269`) → `product::project_usage` (`src/product.rs:7796-7896`). Enforce: customer JWT + `usage.read` + path project must equal principal's project; Off/shadow: deployment bearer. **200** via `json_ok` (no Cache-Control): `{accountId, projectId, month, ingestPayloadBytes, …, correctionTotals, effective{…}}`. Errors: 404 `unknown_project` (foreign/grammar-invalid path — deliberately not 403; journaled); 400 `invalid_month`; 503 `usage_unavailable`; 401 `unauthorized`. OPTIONS = `product_preflight` (GET, OPTIONS). Unmetered.

### 2.22 Product catch-alls
- Unknown subresource → 404 `unknown_route` (`classify_route`, `src/product.rs:521-527`).
- Known resource + wrong method → 405 `method_not_allowed` (per-route message).
- Unknown verb'd collection op (e.g. `POST /v1/streams/{name}` with no verb) → 404 `unknown_route` (`src/product.rs:1300-1306`).
- Bad names → 400 `invalid_name` (`canonical_name`, `src/product.rs:62-111`: 1–512 bytes, no control chars, no empty/`.`/`..` segments, no leading `__ds`, final segment ∉ {records, consumers, watches}, no name that itself parses as a subresource path). `:` is legal in names (only known verbs split).
- Bad consumer name → 400 `invalid_consumer_name`; bad watch URL shapes → 404 `unknown_route`.

---

## 3. Operator / health / debug / internal routes

### Health
- `GET /health`, `GET /readyz` → `health_axum` (`src/http.rs:2066-2124`): **200** body `ok` + headers `x-streams-git`, `x-streams-build-unix`, `x-streams-boot-id`; **503** plain-text when auth feeds unpublished / shard storage unready / billing prerequisites unmet (in `BILLING_MODE=required`). No auth.
- `GET /livez` → 200 `alive` (`src/http.rs:1572`).

### Operator (deployment bearer; SR-5)
`src/operator.rs` — gate failure → 401 `text/plain` "operator bearer required" (`operator.rs:35-50`).
- `GET /operator` → 200 `text/html; charset=utf-8`, `Cache-Control: no-store`, compiled-in page.
- `GET /operator/runbook` → 200 `text/markdown; charset=utf-8`, `Cache-Control: max-age=300`.
- `GET /operator/data.json` → 200 JSON `{ts_ms, local{…load vectors…}, fleet{heartbeats, desired}}`; sections degrade to `null`.
- `GET /operator/billing.json` → `billing_readiness_axum` (`src/http.rs:2131-2200`): 401 `unauthorized` or 200 JSON billing-readiness report.

### Debug (all deployment-bearer gated → 401 `unauthorized`; err_resp envelope)
- `GET /v1/debug/timings` (`1890`) → 200 JSON per-shard commit/pump/ring stats.
- `GET /v1/debug/load` (`1040`) → 200 JSON inflight/shed/SSE/fd/runtime gauges (resets inflight peak).
- `GET /v1/debug/store[?window=][&swap=]` (`1272`) → 200 JSON store latency snapshot.
- `GET /v1/debug/usage` (`1322`) → 200 JSON per-stream usage counters + limits.
- `GET /v1/debug/auth` (`1302`) → 200 JSON shadow/feeds/admission.
- `GET /v1/debug/ops-events` (`1388`) → 200 JSON recent ops ring + alerts.
- `GET /v1/debug/usage-reconcile[?month=]` (`1409`) → 200 JSON report; 400 `bad_query`; 503 `rollup_unavailable`; 500 `reconcile_failed`.
- `POST /v1/debug/absorb-pause?on=1` (`1616-1635`) → 200 JSON `{"absorb_paused": bool}` (mutates).
- `POST /v1/debug/abort` (`1642-1669`) → 200 `{"aborting": true}` then SIGABRT; 403 `disabled` unless `STREAMS_DEBUG_EXIT=1`.
- `GET /v1/debug/sleep?ms=` (`950`) → 200 `ok` after ≤5 s sleep.
- `POST /v1/debug/history-stall?ms=` (`1675-1694`) → 200 JSON (mutates).
- `GET /v1/debug/absorb` (`1700-1810`) → 200 JSON L0/absorber/telemetry posture.

### Fleet-internal (fleet credential or workload JWT with exact op claim; failure → 401 `unauthorized` "fleet-internal credential required", `src/http.rs:544-550`). All require `streams-internal-project` (+ `…-epoch`, `…-seg`, `…-identity`) headers; receiver re-derives and answers 400 `invalid_target` / 409 `stale_target` / 409 `target_mismatch` (`src/product.rs:5076-5174`). Never metered (`params.internal`, §4.2).
- `GET /v1/segments/{*name}` (`get_segments`, `src/http.rs:983-1038`) — op `segment-read`; ALSO the public-ish observability route (Off/Shadow: deployment bearer). 200 JSON segment map `{version, pending, segments:[{seg_id, lo, hi, live, sealed_next_offset, predecessors, created_ms}]}`; 404 `not_found`; 500 `internal`.
- `GET /v1/internal/segment-read/{*name}` (`src/http.rs:7609-7685`) — bounded relay page; raw read vocabulary; honors `streams-internal-max-bytes` (clamped 4096–8 MiB), `streams-internal-deliver: applied`, `?head=1`; 400 `live_unsupported` if `live` sent; serves strictly local (409 + `Streams-Replay-To` on foreign).
- `POST /v1/internal/segment-close/{*name}?seg_id=&seal_gen=` (`7539-7607`) → 200 `{"next_offset": n}`; 404 `not_found`; 503 `temporarily_unavailable` / `seal_incomplete`.
- `POST /v1/internal/sweep-segment/{*name}` (`src/product.rs:5281-5378`) — body `{consumer, segId, fenceBelow, maxSteps}`; 200 `{"complete": bool, "steps": n}`; 400 `invalid_body` / `invalid_target`; 404 `not_found`; 503 `segment_cleanup_failed`; ownership 409 passthrough.
- `GET /v1/internal/queue-cursor/{*name}` (`src/product.rs:5385-5449`) — headers `streams-internal-consumer`, `streams-internal-gen`; 200 `{"cursor": n, "tail": n}`; 400 `invalid_body`; 404.
- `GET /v1/internal/segment-scan/{*name}` (`src/product.rs:5491-5604`) — headers `streams-internal-from`, `streams-internal-max-bytes` (clamped), `stream-encryption-key`; 200 `{"items": [{"off","rk","p"(b64)}], "last", "end", "completed"}`; 400 `invalid_body`; 403 `wrong_key`; 404; 500.
- `POST /v1/internal/telemetry-append/{*name}` (`src/http.rs:1463-1509`) — reserved `_`-streams only (403 `not_system_stream`); create-if-missing under the SYSTEM project then append; response is the raw create/append contract (200/201/204 + `Stream-Next-Offset` etc.).

---

## 4. Notes

**Routes I could NOT fully characterize from handler code alone:**
- None at the routing level — every `.route()` entry in `router()` is accounted for above. Residual depth limits: (a) SSE mid-stream frame sequencing (typed disconnects, keep-alives) lives in `src/sse/session.rs`/`feed.rs` beyond the response-header level I characterized; (b) `Offset`/`encode_ep` token ENCODINGS are in `src/offsets.rs` (opaque by design); (c) signed product cursor formats are in `src/product_cursor.rs` (KeyCursor/ScanCursor/MessageId/LeaseToken/CatalogCursor) — all keyed to (project, stream key, epoch).

**Data-dependent behavior (same route, different wire answer):**
- Raw PUT: 201 vs 200 (new vs idempotent); `Location` only on 201; `Stream-Closed` when close-on-create.
- Raw POST: 204 vs 200 depending on producer presence/duplicate/close (`src/http.rs:5951-5956`); offset token shape changes from scalar to `encode_ep` segment-prefixed once `desc.segments` exists.
- Raw GET: 200 vs 204 (long-poll timeout/closed-at-tail) vs 304 (ETag match, standard path only); body framing differs by content type AND by path (lineage bytes = newline-delimited vs standard bytes = concatenated; `format=frames` = encrypted frames).
- Any read of a stream mid-split ("seal gap") may carry records + resume cursor but never `Stream-Closed`/final `Stream-Up-To-Date`.
- DELETE: 204 vs 404 vs 410 depending on descriptor state (hard-deletable vs missing vs soft-deleted/expired-with-forks).
- Product append on a sealed/sealing collection: 409 `sealed` from `refuse_if_sealed` (handler) vs 409 `sealed` translated from raw `stream_closed` (committer) — same code, different origin.
- `/health` readiness depends on auth mode and `BILLING_MODE`/`ROLLUP` env.

**Intentional dual-surface differences (same operation, different contract):**
- Header vocabulary: raw `Stream-*` vs product `Prisma-*` (`Stream-Next-Offset` ↔ `Prisma-Next-Cursor`, `Stream-Closed` ↔ `Prisma-Sealed`, `Stream-Up-To-Date` ↔ `Prisma-Up-To-Date`, `Stream-Durable-Offset` ↔ `Prisma-Durable-Cursor`, `Stream-Pending-From` ↔ `Prisma-Pending-From`). Product responses never carry `Stream-*` (rebuilt from scratch).
- Error envelope: raw `{"error":{code,message}}` vs product `{"error":{code,message,retryable,details?}}` + `Cache-Control: no-store`. Exceptions where the RAW envelope leaks onto the product surface: `DELETE /v1/streams/{name}` (shared `delete_stream`), `not_ring_owner` from `ring_owner_check` on product create, and SSE lease refusals (`lease_refusal_response`).
- Credentials: raw = `Stream-Encryption-Key` header + bearer/workload JWT; product = `Prisma-Encryption-Key` + `Prisma-Routing-Key` (header, not query) + customer JWT scopes. Raw rejects `?key=`/`Stream-Key`; product rejects `Stream-*` config headers and `?key=`/`?offset=` — clean-switch, never translated.
- Routing keys: raw surface is ALWAYS the default-key sequence (standards isolation, `src/http.rs:6628-6640`, `5500-5511`); product reads take `?routingKey=`, appends take the `Prisma-Routing-Key` header.
- CORS: product = full support (preflight pre-auth 204, ACAO `*` on every response, explicit expose list). Raw = preflight exists but is AUTH-GATED (credential-less OPTIONS gets 401), no expose-headers on preflight, and data responses carry no ACAO (only `Cross-Origin-Resource-Policy: cross-origin` on read bodies/SSE).
- Status vocabulary: raw append success 204/200 → product always 200 + JSON body; raw 409 `stream_closed` → product 409 `sealed`; raw 403 `producer_stale_epoch` → product 403 `stale_producer_epoch`; raw 400 `producer_epoch_seq` → product 400 `producer_epoch_must_start_at_zero`; 409+`Streams-Replay-To` → `not_stream_owner` with the header preserved on BOTH surfaces.
- SSE control frames: raw `{"streamNextOffset","streamCursor","upToDate","streamClosed"}` vs product `{"nextCursor","upToDate","sealed"}` (`src/sse/wire.rs:37-84`).
- Product create is a typed JSON document and writes NO initial content; raw PUT accepts an initial body + close-on-create + fork headers (forks exist ONLY on the raw surface; product create has no fork path, and product refuses `fork_segmented_source` scenarios by construction).

**Metering summary (identity = `billing::identity_of(state, desc)` → (workspace-at-event, project, stream_epoch), `src/billing.rs:873-928`):**
- Reads: raw GET/HEAD + product records GET + scan → `meter_read` (op + payload bytes); SSE → `meter_read_chunk` at yield boundary. **Gap: `read_v3_lineage_inner` (split streams) has no `meter_read` call** — raw + product keyed reads of segmented streams are unmetered in billing (product still quota-debits served bytes via `debit_read_response`).
- Appends: op count via `meter_append_request` on both surfaces (raw POST, product POST records/:batch); billed ingest bytes at the committer via `BillingRef`. PUT-create initial content bills bytes but no op count. Seal final record: bytes yes, op count no.
- Queue: pull → `meter_pull` (queue op + bytes); settle → `meter_queue_op`; consumer PUT/GET/DELETE → unmetered.
- Unmetered: metadata/list/watches/usage/create/delete, all `/v1/internal/*` (by `params.internal`/design — the public coordinator meters once), all `_`-reserved system streams, operator/debug/health.
- Project-level (§17.x quota, not billing): admission slot every product request; read-byte post-hoc debit; queued-append-bytes charge; subscription slots for SSE/watch waits; write memory gate.

**Oddities worth refactor attention:**
- `track_inflight`'s survival-shed prefix test `starts_with("/v1/stream")` matches `/v1/streams` too (both surfaces shed at 4× inflight cap, pre-auth, 503 `overloaded`).
- Raw `?sig=` and `?cursor=` remain in `ReadParams` from the deleted touch surface (`/v1/stream/{name}/touch/key/...` routes are GONE — livebench's touch leg now exercises plain creates/reads; see `src/bin/livebench.rs:113`); `?sig=` has no reader.
- Product route verbs not in the known set stay part of the stream name (`:` is a legal name character) — `PUT /v1/streams/x:sealed` CREATES a stream named `x:sealed`.
- `json_ok` (usage endpoints, internal sweep/cursor/scan) sets no `Cache-Control`, unlike the `no-store` used elsewhere on the product surface.
- Axum-default 404/405 bodies (empty/plain) appear for unmatched paths and for non-GET/OPTIONS on `/v1/streams` and `/v1/projects/{p}/usage` — still origin-marked by the response layer.