# Edge changes in the September 2026 hardening program

The external reviewer asked for a before/after contract record for each of the program's client-visible edge changes, grouped by risk, before giving blanket approval. This document holds those records. Each record was checked against its commit. A record whose commit did not match the recorded text is flagged in its subsection and listed under [Discrepancies](#discrepancies) at the end.

Some records cover changes whose effect on clients is only indirect (fleet-internal, operator, or process behaviour). They are included so that the reviewer can see what was judged not to be client-visible, and why.

Each record lists the surface, endpoint, triggering condition, the behaviour before and after the commit, the change in retry semantics, who is affected, the tests that pin the new behaviour, and the reason for its risk grade. "Check against commit" gives the result of the check, including precision points and effects the original record did not state.

Surface values: **product** is the `/v1/streams` API; **raw** is the `/v1/stream` API; **both** means product and raw; **fleet-internal** covers peer routes and fleet control; **operator-debug** covers `/v1/debug`, `/operator`, `/health`, logs and counters; **process** covers startup, shutdown and configuration.

## Summary

### Counts by risk and surface

| Risk | product | raw | both | fleet-internal | operator-debug | process | Total |
|---|---:|---:|---:|---:|---:|---:|---:|
| high | 3 | 1 | 2 | 0 | 0 | 0 | 6 |
| medium | 5 | 0 | 6 | 0 | 0 | 0 | 11 |
| low | 5 | 2 | 7 | 9 | 10 | 4 | 37 |
| **Total** | **13** | **3** | **15** | **9** | **10** | **4** | **54** |

54 records in total; 51 matched their commit and 1 is flagged. #53 (a security fix) and #54 (a release-hold fix) were recorded by their implementers for owner ratification; they have not been checked against their commits.

### Index

| # | Commit | Change | Surface | Risk | Check |
|---:|---|---|---|---|---|
| 1 | b0a6ba65 | Non-text routing key is refused on both product writers | product | high | matches |
| 2 | 6faf4757 | Watch wait proves shard ownership before it parks | product | high | matches |
| 3 | 1994fc25 | Consumer pull honours its deadline under a blocked dead-letter handoff | product | low | matches |
| 4 | d2e87129 + 3dfb970f | Postings cache no longer over-claims or publishes stale coverage | product | low | matches |
| 5 | 84f80b75 | Billing tombstone walk covers every project in the cell | product | low | matches |
| 6 | c19cc2f0 | Absorber never waits for pool bytes while holding some | operator-debug | low | matches |
| 7 | 5292e3e3 | Feed retry task no longer reads and publishes records | both | low | matches |
| 8 | 668bc80c + 5d60dc63 | Append routed by a pre-transition descriptor is retried on the successor | both | medium | matches |
| 9 | efe12b2e | Stream-TTL and expiry.idle ceiling of 4,294,967,295 s | both | high | matches |
| 10 | 2374751d | Settle retry delay and extend visibility are clamped to 12 h | product | medium | matches |
| 11 | 063a674d | Registry read that raced a write no longer re-installs what it read | both | low | matches |
| 12 | 71345c03 | HTTP/1.1 request-head and idle keep-alive deadline of 120 s | both | medium | matches |
| 13 | 4247421f | Shared raw SSE session gets upToDate after a match-free batch | raw | low | matches |
| 14 | fd810688 | Panicking shard opener fails its open instead of wedging its prefix | both | medium | matches |
| 15 | ac9ab99e | Catch-up through a reopened sealed span is served | product | low | matches |
| 16 | d93b421b | Live SSE tail on a retired engine gets a typed EngineRetired cutoff | both | medium | matches |
| 17 | eb52b970 | Billing-sweep close arms only the 3 s base holdoff | both | low | matches |
| 18 | 4bb51c0d | Unreadable routers/\*.json defers only the desired publication | fleet-internal | low | matches |
| 19 | 527d3d3a | Read-byte quota on :scan | product | medium | matches |
| 20 | f1c9e77e | Read-byte quota on consumer :pull | product | medium | matches |
| 21 | 8dabca7f | Typed create-vs-relay on the telemetry-append receiver | fleet-internal | low | matches |
| 22 | 06fb8d51 | Ops checkpoint read failure and missing page position fail the step | operator-debug | low | matches |
| 23 | 5c0e62d6 | Internal receivers distinguish registry fault from absence | fleet-internal | low | matches |
| 24 | 74391f9c | Relayed read verdict is typed on the fleet page route | product | medium | matches |
| 25 | ee99975d | Graceful stop seals and drains the active read window | process | low | matches |
| 26 | a8c99c23 | Append larger than a fresh bucket is 413; boot refuses unadmittable limits | both | medium | matches |
| 27 | 12ad5134 | Refused fleet event append is reported | fleet-internal | low | matches |
| 28 | f499d590 + bbd23d13 | Raw PUT never replaces a name retained for forks | raw | high | matches |
| 29 | e7c7ad87 | Product create that loses the recreate CAS to a live forked incarnation is idempotent | product | low | matches |
| 30 | 517f4fa1 | Undecodable producer or Stream-Seq lane row refuses the append | both | high | matches |
| 31 | 7c4f8606 | Failed or empty live SSE read is retried on a backoff | both | low | matches |
| 32 | 8f590ec5 | SSE_H1_MAX_BUF below 8,192 is refused at validation | process | low | matches |
| 33 | d148e1a4 | Stream-Seq over 65,535 bytes no longer corrupts the tail row | raw | low | matches |
| 34 | 85017d84+729c52ac | Split or merge the segment map refuses keeps its intent pending | both | medium | matches |
| 35 | 2fb92fb9 | Empty catch-up page is re-read after a bounded wait | both | low | matches |
| 36 | a1cf29f3 | shard_opens counters are reported per runtime | operator-debug | low | matches |
| 37 | 6515a15d | Rebalancer targets only active-ring members; eager open follows effective_owner | fleet-internal | low | matches |
| 38 | 2f2c3015 | Telemetry-append authenticates before reading the body | fleet-internal | low | matches |
| 39 | 33fbd10e | One bearer gate for all of /v1/debug | operator-debug | low | matches |
| 40 | a0185c3b | Panicked connection tasks are logged and counted | operator-debug | low | matches |
| 41 | 714abcc2 | Refused supervised future is dropped after the registration lock | process | low | matches |
| 42 | 6ef3bc64 | Typed SourceReadError for SSE source reads | both | low | matches |
| 43 | aaf2baa5 | Sweep custody handshake uses SeqCst read-modify-writes | fleet-internal | low | matches |
| 44 | 4cb6263c | The refresher, not the source, stamps auth feed freshness | operator-debug | low | matches |
| 45 | 07db91a7 | Scaler cooldown and merge patience saturate instead of wrapping | fleet-internal | low | **flagged** |
| 46 | 1791207a | Live SSE lease ends when its project is placed on another cell | product | medium | matches |
| 47 | a73b7b79 | One freshness boundary for every auth feed reader | operator-debug | low | matches |
| 48 | fdc31b07 | --compactor-poll-ms on argv reaches the compactor | process | low | matches |
| 49 | 46d4b7df | Billing readiness reads the clap-resolved mode | operator-debug | low | matches |
| 50 | e19c80c5 | One corrupt row fails only its stream in a v2 gather lane | operator-debug | low | matches |
| 51 | 5d9d517f | Undecodable pending billing artifact is logged | operator-debug | low | matches |
| 52 | 31fd9096 | Late byte-time beyond a correction fails its rollup page | fleet-internal | low | matches |
| 53 | this record's commit | Usage `?streamId=` is served only for an incarnation of the URL's name | product | high | recorded for ratification |
| 54 | this record's commit | The transition retry's re-preparation answers an unreadable registry as retryable | both | low | recorded for ratification |

## High risk (6)

In each of these changes, a request that used to succeed can now fail permanently. Each risk reason states how narrow the affected inputs are and, where it applies, why the earlier success was incorrect.

### #1 b0a6ba65 — Non-text routing key is refused on both product writers

- **Program item:** bug #2
- **Surface:** product
- **Endpoint:** POST /v1/streams/{name}/records and POST /v1/streams/{name}/records:batch (Prisma-Routing-Key header, both go through product_append_inner); POST /v1/streams/{name}:seal with body {final, routingKey} (product_seal, the final-bearing path). The raw /v1/stream surface is not involved: it has no routing key, and Stream-Key was already refused there.
- **Condition:** The routing key contains a byte outside visible ASCII, space and tab. In practice this means obs-text bytes 0x80-0xFF: a Latin-1 or UTF-8 key such as 'café'. The WHATWG fetch in the TS SDK sends Latin-1 characters as single obs-text bytes and throws client-side on anything above U+00FF, and curl sends UTF-8. The seal routingKey arrives as a UTF-8 JSON string. One predicate, parse_routing_key, now checks both writers once, and the parsed value is the only key used afterwards. An absent header still means the default key "".
- **Before:** Append (single or batch) with a non-text Prisma-Routing-Key: 200 {"cursor","count":1,"duplicate":false,"sealed":false}. The header was read with to_str().ok().unwrap_or(""), so the record and its idempotency/request hash were filed under the default key "". A reader of the named key never saw the record. The length check ran on the already-defaulted "", so an over-long NON-text header also got 200. Seal with routingKey 'café': admission used HeaderValue::from_str, which accepts 0x80-0xFF, so the answer was 200 {"sealed":true} with Cache-Control: no-store. The durable intent, the operation id and the takeover fence all named 'café', but the internal final append read the key back through to_str -> "", so the final record was filed under "" in a collection that is now sealed (immutable). Seal keys containing control characters or DEL were ALREADY 400 invalid_routing_key, because from_str rejects them. The message was 'routing key contains characters that cannot be transmitted'.
- **After:** All three routes answer 400 with Content-Type application/json and Cache-Control: no-store. Body: {"error":{"code":"invalid_routing_key","message":"routing key must be visible ASCII (what the append header carries)","retryable":false}}. Nothing is written. For seal, no lifecycle intent is published: the descriptor has sealing=None and sealed=false. Keys over 1,024 bytes answer 400 invalid_routing_key 'routing key exceeds 1,024 bytes', which is unchanged for text keys. Text keys still round-trip under themselves. WIRE-MATRIX §2.5 and §2.7 state the shared rule.
- **Retry semantics:** 200 (silently misfiled) becomes a permanent 400 with retryable:false. The TS SDK throws a StreamsError and does not retry. For an idempotent producer the refused append commits nothing, so the server consumes no sequence number.
- **Who is affected:** SDK and HTTP clients of the product API that use non-ASCII routing keys, for example customer names with accents via producer.append(..., {routingKey}) or stream.seal({final, routingKey}). Their requests used to 'succeed' while losing key placement; they now fail loudly. Clients using ASCII keys are unaffected.
- **Pinning tests:**
  - an_append_key_that_is_not_header_text_is_refused_not_filed_under_the_default_key (src/dst/tests/product_lifecycle.rs)
  - a_final_key_the_append_header_cannot_carry_is_refused_before_the_intent (src/dst/tests/seal_recovery.rs)
  - routing_key_edges_are_pinned (src/product/tests.rs)
  - a_routing_key_is_admitted_exactly_when_a_header_reads_it_back (src/product/tests.rs, 1,024 cases against http's from_bytes+to_str)
- **Risk reason:** High by the rubric's letter: a request that used to return 200 now fails with a permanent, non-retryable 400. The earlier 200 was a silent misfile, so no correct behaviour is lost, and only keys containing bytes 0x80-0xFF are affected. Still, any production caller with Latin-1 or UTF-8 keys will start seeing errors, and seal callers get a hard refusal where they used to get {"sealed":true}.
- **Check against commit:** The commit matches the record. Two points of precision. (1) 'Before, it was 200' holds only for bytes 0x80-0xFF. On seal, control characters and DEL were already 400 invalid_routing_key (only the message text changed). Such bytes cannot arrive in a parsed append header. (2) 'over-long keys were already 400' holds only for TEXT keys. An over-long non-text append header got 200 before, because the length was checked after the key was defaulted to "".

### #2 6faf4757 — Watch wait proves shard ownership before it parks

- **Program item:** bug #3
- **Surface:** product
- **Endpoint:** GET /v1/streams/{name}/watches/{watch}/keys/{keyHex16}[?cursor=&timeoutMs=&cap=] (product_watch_wait -> WatchService::wait -> watch_failure_response -> http::resolve_error_response)
- **Condition:** The wait passes capability/key authentication, quota admission and the Creating/UnknownWatch checks. Then shards.resolve(stream route, Adoption::External) runs BEFORE keys.put and touch.journal, and it refuses: (a) the instance is not the ring owner of the stream's route shard (multi-instance only: a misrouted wait, or an ex-owner after the ring moved); (b) the route shard is not resident and the bounded single-flight open does not finish in time or is held off; (c) the open fails. (b) and (c) also apply ON THE OWNER, including single-instance deployments, whenever the shard is cold.
- **Before:** The wait never resolved ownership. It parked on a process-local journal that it created on demand. On a non-owner nothing ever touches that journal, so the answer was 200 with application/json, Cache-Control: no-store and Referrer-Policy: no-referrer, body {"invalidated":false,"cursor","streamCursor"} after each timeout (at most 25 s), until routing changed. Invalidations were missed silently. With the SDK default cursor=now this happened from the first call. With an owner-minted cursor the first answer was {"invalidated":true,"reason":"resync"}, after which the client adopted the non-owner's cursor. A cold owner parked without opening its shard.
- **After:** (a) Non-owner: 409 with header Streams-Replay-To: \<owner instance id>. Body {"error":{"code":"not_ring_owner","message":"shard \<prefix> belongs to \<owner>"}}, which is the raw err_resp envelope: Content-Type only, NO retryable field, NO Cache-Control: no-store. No key is cached and no journal is created. A cold non-owner does not open the owner's shard, and so does not fence it. (b) 503 with code shard_opening, shard_moving or shard_closing and a Retry-After: \<secs> header, same envelope. (c) 500 shard_open 'open shard \<prefix>: \<error>', same envelope. On success the owner's engine stays resident and is stamped as external adoption, so the ring moving closes the journal and wakes the waiter. Success bodies are unchanged.
- **Retry semantics:** A 200 that silently never invalidated becomes a 409 replay-to on a non-owner. Routers are required to replay it (COMPUTE-SPEC; the pilot LB follows up to 2 replays). The TS SDK has no Streams-Replay-To handling and treats a 409 without a retryable field as non-retryable, so Watch.wait() and Watch.subscribe() throw StreamsError(409,'not_ring_owner'). A 503 shard\_\* defaults to retryable in the SDK, which retries up to 3 times honouring Retry-After (capped at 5 s). A 500 shard_open is non-retryable in the SDK; before, this case answered 200.
- **Who is affected:** Watch observers: SDK Watch users and holders of capability URLs, such as browsers, calling directly. The 409 reaches them only when they hit a non-owner without a replaying router. The router/LB replays the 409 (one extra hop). Owners, including single-instance cells, can now return 503/500 on a cold shard. Operators: a watch wait now opens and keeps the shard resident and counts as external adoption for the billing sweep's custody.
- **Pinning tests:**
  - watch_wait_on_a_non_owner_replays_to_the_ring_owner (src/dst/tests/watch_observation.rs; 409, not_ring_owner, Streams-Replay-To=inst-a, non-owner shard not opened, owner still delivers 'changed')
  - watch_wait_follows_the_ring_when_the_shard_moves (src/dst/tests/watch_observation.rs; ex-owner 409 + Streams-Replay-To=inst-b, new owner answers resync)
- **Risk reason:** By the rubric: a request that used to return 200 now fails, and the SDK treats the 409 as permanent because it does not follow Streams-Replay-To. That applies to any path without a replaying router. There is also a new 500 shard_open on owners with a failing cold open, which the SDK does not retry. Mitigations: the old 200 was wrong (missed invalidations); spec-compliant routers absorb the 409; the 503 is retried. The refusal envelope also differs from the product read path, which maps NotOwner to 409 not_ring_owner WITH retryable:true, and other resolve errors to 503 temporarily_unavailable retryable:true, both via perr with Cache-Control: no-store.
- **Check against commit:** The commit matches the core claim, but the record is incomplete. (1) The 503 + Retry-After and 500 shard_open refusals are not specific to non-owners. They apply on the ring owner, including single-instance deployments, when the shard is cold, because the wait now performs a full External resolve/open that it never did before. (2) The 500 shard_open outcome is missing from the record, and the 503 codes are shard_opening/shard_moving/shard_closing. (3) The refusal uses the raw err_resp envelope (no retryable, no Cache-Control: no-store) on a product route, which is inconsistent with the product read mapping. WIRE-MATRIX §2.18 does document it. (4) No test pins the 503/500 branches on the watch route.

### #9 efe12b2e — Stream-TTL and expiry.idle ceiling of 4,294,967,295 s

- **Program item:** rank 10
- **Surface:** both
- **Endpoint:** Raw PUT /v1/stream/{name} with Stream-TTL, including idempotent re-PUT and fork create with Stream-Forked-From. Product PUT /v1/streams/{name} with body expiry.idle. Also HEAD /v1/stream/{name} Stream-TTL reporting and the idle-TTL renewal on touch.
- **Condition:** Raw Stream-TTL above 4294967295. Product expiry.idle whose seconds, after the unit multiply, exceed 4294967295 (for example '4294967296' or '49711d'). A fork or TTL renewal of a persisted window that no parser bounded (legacy).
- **Before:** Raw: any canonical decimal that fits a u64 was admitted, and fresh_desc computed expiry = now + (ttl as i64)\*1000 unchecked. The product went through the same fresh_desc; parse_idle_secs only checked the unit multiply and refused zero. By value: from 4294967296 up to about 9.22e15, 201 with a working far-future expiry. u64::MAX, and every value above i64::MAX, sign-wraps: 201 for an already-expired stream (u64::MAX gives now - 1 s). From 9223372036854776 to i64::MAX the multiply overflows. In release (the shipped profile has no overflow-checks) the result wraps: 9223372036854776 gives a past expiry, and other values in that band give arbitrary instants, some past and some far-future. In overflow-checked builds the handler panicked (closed socket, no status). A fork whose inherited window recomputed to a past expiry produced a child born expired, and every attempt got 409 {"code":"fork_target_changed","message":"...; retry"}.
- **After:** Raw returns 400 {"error":{"code":"invalid_ttl","message":"invalid Stream-TTL"}}. Product returns 400 {"error":{"code":"invalid_config","message":"invalid expiry.idle duration","retryable":false}}. Nothing is written. 4294967295 (product '49710d' = 4294944000 s) is still admitted, and HEAD reports it. Stream-TTL: 0 is still admitted on raw; product zero is still 400. Persisted windows saturate in fresh_desc, fork inheritance and renewal: expiry = i64::MAX, meaning 'never'. A fork of a legacy u64::MAX window returns 201 and inherits ttl_secs verbatim. HEAD may report a Stream-TTL above 4294967295 for such windows.
- **Retry semantics:** 201 becomes a permanent 400 for windows above 2^32-1 s. This includes an idempotent re-PUT of a stream created with such a TTL, because the header is parsed before the idempotency compare. A fork that was '409 retry, forever' becomes 201.
- **Who is affected:** Raw clients and SDK users who pass a TTL longer than about 136 years as 'effectively never' (the SDK forwards expiry.idle strings verbatim). Anyone re-PUTting such a legacy stream. Fork creators whose source has a legacy wrapped window.
- **Pinning tests:**
  - src/dst/tests/lifecycle_creation.rs::an_idle_window_past_the_ceiling_is_refused_and_creates_nothing
  - src/dst/tests/lifecycle_creation.rs::the_longest_idle_window_is_admitted_and_reported
  - src/dst/tests/lifecycle_creation.rs::a_fork_inheriting_a_legacy_window_is_born_alive
  - src/http/tests.rs::stream_ttl_refuses_windows_past_the_ceiling
  - src/product/tests.rs::idle_durations_stop_at_the_service_maximum
  - src/application/creation/ttl.rs::tests::the_ceiling_is_the_documented_number_and_is_inclusive
  - src/application/creation/ttl.rs::tests::an_expiry_is_total_over_every_persisted_window
  - src/application/creation/ttl.rs::tests::a_fresh_expiry_opens_its_window_at_the_clock
  - Conformance: 332 passed / 0 failed / 6 skipped (@durable-streams/server-conformance-tests@0.3.6)
- **Risk reason:** By the rubric, a previously successful request now fails permanently. A Stream-TTL between 2^32 and about 9.2e15 s, or an expiry.idle of '49711d' or more, used to get 201 with a correct far-future expiry and now gets 400. So does an idempotent re-PUT of such a stream. In practice this is narrow (windows over about 136 years), conformance is unaffected, and the change fixes real past-expiry, panic and unforkable bands.
- **Check against commit:** This is a refinement, not a contradiction. The overflow band is 9223372036854776 through i64::MAX s, and its release wrap gives arbitrary instants, only some in the past (for example 2e16 s wraps to a far-future instant). Every value above i64::MAX wraps into the past. The fork fix matters only for legacy windows whose recomputed expiry wrapped into the past; windows from 2^32 to about 9.2e15 were forkable before. Not recorded: an idempotent re-PUT of a legacy stream with such a TTL now returns 400, and product unit spellings are refused on the same ceiling (for example '49711d'). The claim that the release profile has no overflow-checks is verified: Cargo.toml [profile.release] sets only lto='thin'.

### #28 f499d590 + bbd23d13 — Raw PUT never replaces a name retained for forks

- **Program item:** item 9
- **Surface:** raw
- **Endpoint:** PUT /v1/stream/{name}, including fork creation via Stream-Forked-From (application/creation/claim.rs::resolve). bbd23d13 routes the gone-vs-missing verdicts of POST /v1/stream/{name} (append) and DELETE through the same predicate, with no behaviour change.
- **Condition:** (a) The descriptor snapshot (registry cache, up to \~5 s old) shows a dead name, but the STORED descriptor is expired with fork_children, because a fork was anchored by another instance or inside the check-then-act window. (b) The snapshot shows a tombstone, but the stored descriptor is soft-deleted: the name was recreated, forked and deleted elsewhere, so the recreate CAS declines a soft-deleted winner.
- **Before:** (a) The recreate CAS predicate `!desc_alive(d) && !d.soft_deleted` accepted, answering 201 Created with a fresh epoch. From then on, every read of the fork failed 'different incarnation', and its inherited records were unreachable for good. (b) The declined winner went to validate_live: 200 (existing stream) when content type, ttl, fork ref and key matched, otherwise 409 config_mismatch or 403 wrong_key. This answered for a name whose appends and deletes answer 410.
- **After:** Both (a) and (b) answer 409 {"error":{"code":"gone","message":"name is soft-deleted; live forks retain its data"}}: raw envelope, no retryable field, no Retry-After. The stored epoch and fork_children are kept and the fork still reads. The CAS is now recreatable(d, now) on the stored descriptor, and a declined winner is either retained (409 gone) or live (idempotent compare). Unchanged: an expired name without children is recreated with 201 and a fresh epoch; a live winner answers 200 / 409 config_mismatch / 403 wrong_key. Append and delete on a retained source stay 410.
- **Retry semantics:** A success (201, or 200) becomes a non-retryable 409 gone. It lasts until the last fork releases the name; after that, a PUT recreates with 201.
- **Who is affected:** Raw-API clients and SDKs that recreate a name which has expired or been deleted and still has live forks, and whose request reaches an instance with a stale cached descriptor. Fork readers benefit: they are no longer stranded.
- **Pinning tests:**
  - src/dst/tests/lifecycle_incarnation.rs::a_raw_recreate_never_replaces_an_expired_source_its_forks_read
  - src/dst/tests/lifecycle_incarnation.rs::a_raw_put_behind_a_stale_tombstone_never_revives_a_deleted_source
  - src/application/creation.rs::tests::a_name_is_live_retained_or_recreatable_at_one_instant
  - src/dst/tests/fork_lifecycle.rs (pre-existing 410 legs; bbd23d13 neutrality)
- **Risk reason:** Graded high by the rubric's letter: a previously successful request (201 or 200) now fails permanently (409 gone). Mitigating factors: this happens only in the stale-snapshot race window (at most one cache TTL after a cross-instance fork or delete). The race-free path already answered 409 gone, as documented in WIRE-MATRIX §1.1. The old 201 permanently broke every fork of the name.
- **Check against commit:** Matches, with imprecisions. (1) For case (b), the old answer was 200 'exists' only when config and key matched; mismatching requests got 409 config_mismatch or 403 wrong_key. (2) Claim 'judged at one instant': in claim.rs, `now` is captured before the snapshot's desc_alive guard, which reads its own clock. A childless descriptor that expires within that same millisecond can reach the CAS, be judged live at `now`, and be answered 200 by validate_live instead of 201 recreate. The window is at most 1 ms and did not exist before. (3) bbd23d13 is verified behaviour-neutral: identical predicate terms.

### #30 517f4fa1 — Undecodable producer or Stream-Seq lane row refuses the append

- **Program item:** item 55
- **Surface:** both
- **Endpoint:** Raw POST /v1/stream/{name} with Producer-Id/Producer-Epoch/Producer-Seq or Stream-Seq. Product POST /v1/streams/{name}/records and /records:batch with the producer triple. Also every shard append that consults these rows, including system and relayed internal appends.
- **Condition:** The lane state is not cached in memory (first consult after open, restart or handoff), and the row the loader lands on exists but does not decode. The loader uses the own segment's row, or the nearest predecessor that has one. A producer row fails to decode unless it is exactly 16, 24 or 40 bytes; a Stream-Seq row fails if it is not UTF-8. Only requests that consult that row (that producer id, or Stream-Seq, on that routing key) are affected.
- **Before:** Rows under 16 bytes read as 'no row': the loader fell through to an older predecessor's row, or reset the lane. Rows of 17–23 bytes decoded as legacy 16-byte, 25–39 as 24-byte, and 41+ as the first 40 bytes. A non-UTF-8 Stream-Seq row read as 'no sequence'. The request was judged against absent or garbage state: usually a success (raw 200 with Producer-Epoch/Producer-Seq headers, or 204 for Stream-Seq-only; product 200 {cursor,count,duplicate:false}) with the idempotence fence dropped, so a duplicate could commit a second copy. Depending on the bytes, it could instead be a spurious 409 producer_seq_gap, a 403 producer_stale_epoch, or a false duplicate.
- **After:** The append fails with AppendErr::Internal. Raw: 500 {"error":{"code":"internal","message":\<slatedb data error text, e.g. 'invalid persisted producer row width' / 'invalid persisted Stream-Seq row encoding'>}}, no Retry-After. Product: 500 {"error":{"code":"append_failed","message":"append failed","retryable":false}}. Nothing is written and no offset is consumed. Every retry fails until the row is repaired; no repair path is provided. 16-, 24- and 40-byte rows decode exactly as before.
- **Retry semantics:** A success (or a producer-verdict error) becomes a permanent 500. On product it is explicitly retryable:false. On raw it is a 500 with no Retry-After, which generic clients may retry forever.
- **Who is affected:** Producers and Stream-Seq writers whose lane row is corrupt. Checked in history: every writer only ever wrote supported widths. a402a3b1 wrote 16 bytes, e032edf0 wrote 24, bf4fe33c onward writes 40. Seq rows are always String::into_bytes. At 517f4fa1 there is a single writer for each row kind. So no legacy data reaches the refusal; only storage corruption or a foreign writer would.
- **Pinning tests:**
  - src/shard/storage_decode_tests.rs::r12_undecodable_producer_rows_are_corruption_not_absence
  - src/shard/storage_decode_tests.rs::r12_non_utf8_stream_seq_row_is_corruption_not_absence
  - src/shard/storage_decode_tests.rs::r12_producer_row_requires_exact_supported_width
  - src/shard/storage_decode_tests.rs::r12_stream_seq_row_requires_utf8
  - src/shard/storage_decode_tests.rs::quality_producer_rows_decode_only_at_a_supported_width (proptest, 1,024 cases)
  - src/shard/storage_decode_tests.rs::quality_stream_seq_rows_decode_exactly_their_utf8 (proptest, 1,024 cases)
  - src/shard/transaction_tests.rs::r12_an_undecodable_producer_row_refuses_the_append_and_writes_nothing
  - src/shard/transaction_tests.rs::r12_an_undecodable_stream_seq_row_refuses_the_append_and_writes_nothing
- **Risk reason:** Graded high by the rubric's letter: a request that previously succeeded (fence silently dropped) now fails permanently, and the lane stays wedged with no repair tool. Practical exposure is corruption only; the history check shows no writer ever produced an unsupported width or non-UTF-8 seq bytes. Test gap: the tests pin AppendErr::Internal at the shard level only; no HTTP-level test pins raw 500 internal or product 500 append_failed for this condition. That mapping comes from the existing AppendErr::Internal → FailureClass::Internal path.
- **Check against commit:** Matches, with one nuance: 'before, it was accepted with the idempotence fence silently dropped' is the typical outcome. Because the old code judged the request against absent or misdecoded state, it could also answer a spurious producer error (409 producer_seq_gap, 403 producer_stale_epoch) or a false-duplicate 204.

### #53 (this record's commit) — Usage `?streamId=` is served only for an incarnation of the URL's name

- **Program item:** security fix found by the adversarial authorization review of item 73 (external review, 2026-09-24). Recorded by its implementer for owner ratification.
- **Surface:** product
- **Endpoint:** GET /v1/streams/{name}/usage[?month=YYYY-MM]&streamId=\<id> and GET /v1/streams/{name}/usage/current?streamId=\<id> (product_usage, which now reads through UsageRollup::usage_row in src/product/usage.rs).
- **Condition:** The streamId is neither the live incarnation of {name} nor listed in the month's name aggregate for {name}, which is the answer's `incarnations`. That covers another stream's id, an incarnation of {name} that contributed nothing in the requested month, and an id that names nothing. It applies in every auth mode. Rollup keys carry the account and project, so the id could only ever select a row inside the request's own project.
- **Before:** The handler replaced the rollup lookup key with the id without checking it, and the gate had prefix-checked only the URL's name. The answer was 200 with that id's month row: ingest, read, queue and append counters, storage byte-seconds, status, corrections and the correction list, next to the URL name's `streamName`, `nameAggregate` and `incarnations`. For the current month, an id with no month row also read that id's persistent segment states into `ownedStoredBytesNow` and the provisional storage. On an enforce rig, a credential holding streams.usage.read with prefix grant ["a"] got 403 prefix_denied on /v1/streams/b/secret/usage/current, but 200 with b/secret's full row on /v1/streams/a/mine/usage/current?streamId=\<b/secret's id>. The id is b/secret's stream epoch: the `streamId` of its own usage answer, and the `epoch` of its metadata when it has watches. An id naming nothing got 200 with a zero row.
- **After:** 404 with Content-Type application/json and Cache-Control: no-store. Body: {"error":{"code":"not_found","message":"streamId is not an incarnation of this stream in the requested month","retryable":false}}. The id's month row and segment states are not read. The live id named explicitly, and a prior incarnation that the month lists, are served exactly as before. A failed read of the name aggregate is 503 usage_unavailable (retryable), like every other rollup read failure on this route. Without streamId nothing changes.
- **Retry semantics:** 200 becomes a permanent 404 with retryable:false. The TS SDK has no usage method, so only direct HTTP callers see it.
- **Who is affected:** Callers passing a streamId outside the name's incarnations for the month. That includes a credential reading another stream's usage through a name it may read, which is the bypass this closes, whether the other stream is outside its prefix grant or not. It also includes a dashboard asking for a prior incarnation of the name in a month that incarnation did not contribute to: it used to get a 200 zero row and now gets 404. Callers using the ids `incarnations` lists are unaffected.
- **Pinning tests:**
  - a_usage_stream_id_is_served_only_for_an_incarnation_of_the_url_name (src/dst/tests/security_usage.rs; red at 26c555dd, where b/secret's id through a/mine's URL answered 200 with b/secret's row; controls for a prior incarnation after delete/recreate on both route spellings and for the live id named explicitly)
- **Risk reason:** High by the rubric's letter: a request that used to return 200 now fails permanently. The earlier 200 was an authorization bypass across names and prefix grants, so no correct behaviour is lost. The round-21 delete/recreate lookup, with an id from `incarnations`, is unchanged. The one legitimate-looking loss is the zero row for an incarnation in a month it did not contribute to.
- **Check against commit:** Not checked. This record was written with the change, by its implementer, for the owner to ratify or reverse the 200 → 404 edge change.

## Medium risk (11)

These changes alter a status, error code or retry behaviour on an error case clients may branch on, or change the semantics of a successful path (redelivery timing, connection lifetime, subscription lifetime, new quota refusals).

### #8 668bc80c + 5d60dc63 — Append routed by a pre-transition descriptor is retried on the successor

- **Program item:** bug #9
- **Surface:** both
- **Endpoint:** Raw POST /v1/stream/{name} (append/close). Product POST /v1/streams/{name}/records and POST /v1/streams/{name}/records:batch, plus the final-record append inside POST /v1/streams/{name}:seal. Also internal consumer DLQ delivery appends (AppendService::execute).
- **Condition:** The engine answers Closed for the segment the append was routed to (an engine closure, not one the descriptor declares), and the descriptor was not sealed. Typical causes: the descriptor was cached (5 s TTL, or re-inserted by a raced registry read) before a split or merge, so the append went to a sealed parent or a merged-away child. Also covered: the registry refresh fails, the stream was deleted, or the name was deleted and recreated. Declared closures (descriptor sealed or sealing, closed_tail_failure) are excluded.
- **Before:** Retries happened only when the PREPARED descriptor was already 'wrapped' (segments.len() > 1 or a pending transition). An unwrapped descriptor (for example one from before the first split) got 1 attempt. Result for a LIVE collection: raw 409 {"error":{"code":"stream_closed","message":"stream is closed"}} with Stream-Closed: true and Stream-Next-Offset; product 409 {"error":{"code":"sealed","message":"collection is sealed","retryable":false}}. A wrapped descriptor was invalidated and re-read with a registry GET. If the GET errored or found nothing, the client got the same 409 (fail-open). If the fresh route was live, the client got the same 409 even when it named a different segment than the one attempted. If a transition was pending, the append waited for it (a ticket error gave 503 segment_transition with Retry-After: 1). Otherwise it slept 10/20/30/40 ms and re-prepared, for 4 attempts. Exhaustion gave raw 503 segment_transition 'segment map transition did not converge; retry' with NO Retry-After, and product 503 temporarily_unavailable with retryable:true and no Retry-After. A recreated name got 409 stream_closed (product: sealed).
- **After:** Every descriptor retries an engine closure: 4 attempts, waiting 10, 20 and 30 ms before attempts 2, 3 and 4, with no sleep after the last. The closure goes to the client only when the prepared descriptor is sealed, or when a fresh registry read (closure_is_current) still routes the key to the SAME seg_id, live, with no pending transition. In both cases the 409 is the same as before. Otherwise: a stale route lands on the successor and returns normal success (raw 204/200, product 200). A refresh read error gives raw 503 segment_transition with Retry-After: 1 (product 503 temporarily_unavailable, retryable:true, Retry-After: 1). A changed epoch (recreated name) gives raw 409 {"code":"target_incarnation_changed","message":"append target incarnation changed"} with no Stream-Closed or Stream-Next-Offset. The product surface gives 409 {"code":"conflict","message":"producer or configuration conflict","retryable":false}. A pending transition is waited out, then retried. A descriptor that no longer exists is retried, and the next prepare returns the missing/gone answer (404 not_found or 410 gone) instead of 409. Exhaustion gives the same 503 codes, now with Retry-After: 1. Declared closures return 409 at once, with no refresh.
- **Retry semantics:** A permanent 409 (retryable:false) on a live collection becomes a success, after up to 60 ms of internal retry. A permanent 409 becomes a retryable 503 with Retry-After: 1 when the refresh cannot be read. The exhaustion 503 gains Retry-After: 1. The recreate case stays a permanent 409, but the code changes: raw stream_closed becomes target_incarnation_changed and loses the Stream-Closed header, and product sealed becomes the generic conflict. Deleted-mid-retry changes from 409 to 404/410. Every engine closure on an unsealed descriptor, including on unsegmented streams, now costs an invalidate plus one registry GET.
- **Who is affected:** SDK/product writers and raw writers on collections that split or merge. Any writer whose request met a closed segment through a stale cached descriptor. Clients that branch on raw stream_closed/Stream-Closed or product sealed (the recreate case now looks different). Internal DLQ delivery.
- **Pinning tests:**
  - src/dst/tests/topology_scaling.rs::an_append_routed_by_a_pre_split_descriptor_lands_on_the_child
  - src/dst/tests/topology_scaling.rs::an_append_routed_by_a_pre_merge_descriptor_lands_on_the_merged_segment
  - src/dst/tests/topology_scaling.rs::an_append_routed_by_a_pre_seal_descriptor_is_still_refused_as_sealed
  - src/dst/tests/append_application.rs::r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final
  - src/dst/tests/append_application.rs::r02_a_sealed_descriptors_closure_costs_no_refresh
  - src/dst/tests/append_application.rs::r02_a_closure_from_a_replaced_incarnation_is_not_the_new_streams
  - NOT pinned: the exhaustion 503 with Retry-After, the 10/20/30 ms waits, and the 4-attempt count. No test drives the loop to exhaustion.
- **Risk reason:** The main effect turns a wrong permanent 409 into a success. It also changes statuses and codes on error cases clients may branch on: the recreate case (stream_closed plus Stream-Closed, or product sealed, becomes target_incarnation_changed or the generic product conflict), refresh failure (409 becomes 503), and delete during retry (409 becomes 404/410). It also changes retry semantics by adding Retry-After: 1 to exhaustion.
- **Check against commit:** The recorded text is accurate, but '409 sealed' is product wording only; raw clients got 409 stream_closed with Stream-Closed: true and Stream-Next-Offset. On product, TargetIncarnationChanged surfaces as the generic 409 'conflict' (retryable:false), not a dedicated code. The Retry-After value is 1 s. The 'Gone' path (409 -> 404/410) and the extra registry GET per engine closure on unsegmented streams are not recorded. 5d60dc63 says the existing tests pin the attempt count, but no test found exercises exhaustion.

### #10 2374751d — Settle retry delay and extend visibility are clamped to 12 h

- **Program item:** rank 24
- **Surface:** product
- **Endpoint:** POST /v1/streams/{name}/consumers/{consumer}:settle, body fields retries[].delayMs and extends[].visibilityMs. The :pull visibilityMs and consumer PUT visibilityTimeoutMs clamps are unchanged; they only moved to the shared owner in src/queue.rs.
- **Condition:** A settle whose retries[].delayMs exceeds 43,200,000, or whose extends[].visibilityMs is below 1,000 (including 0) or above 43,200,000.
- **Before:** The committer received the u64 values unchanged and computed deadline = now + window as i64. By value: from 12 h up to about i64::MAX - now, the window was honoured exactly (for example a 24 h delay or extend). Around 9e18 the lease never expires: the record is never redelivered, never reaches maxAttempts, and never dead-letters, and its routing key stays blocked until an ack or a consumer delete. Near i64::MAX the add overflows: a wrapped deadline in release, and a committer-task panic under overflow checks. Above i64::MAX (for example u64::MAX) the cast sign-wraps to now - 1, and the next pull redelivers at once (attempts: 2). An extend with visibilityMs 0 meant 'visible now'; 1-999 ms was honoured. The settle response was 200 with counts.
- **After:** delayMs is clamped to 0..=43,200,000 (default 1,000 when omitted; 0 still releases at once). Extend visibilityMs is clamped to 1,000..=43,200,000 (default: the consumer's visibilityTimeoutMs). QueueOp windows are u32, so now + window is always defined. The response is unchanged: 200 with the same {acked, retried, extended, dlq, stale, ...} counts. The clamp is silent: no error, and no field reports the applied window. Lease rows already written unbounded by an older binary are not healed.
- **Retry semantics:** Redelivery timing changes. A retry delay or extend above 12 h now redelivers at 12 h, where it used to be honoured exactly. An extend of 0-999 ms now holds for 1 s; to release at once a client must now send retries[{delayMs: 0}]. Absurd values are bounded instead of producing a permanent lease or an immediate redelivery. No status change: settle still never errors on these fields.
- **Who is affected:** Product consumer users: SDK message.retry({delayMs}) and message.extend({visibilityMs}) forward user numbers verbatim. Direct HTTP settle callers.
- **Pinning tests:**
  - src/dst/tests/consumer_dlq.rs::a_retry_asking_for_the_longest_delay_is_not_redelivered_at_once
  - src/dst/tests/consumer_dlq.rs::a_retry_delay_beyond_the_lease_window_is_held_to_twelve_hours
  - src/dst/tests/consumer_dlq.rs::an_extend_asking_for_the_longest_visibility_is_not_redelivered_at_once
  - src/dst/tests/consumer_dlq.rs::an_extended_visibility_is_held_between_one_second_and_twelve_hours
  - src/dst/tests/consumer_dlq.rs::a_pulled_visibility_beyond_the_lease_window_is_held_to_twelve_hours
  - src/queue.rs::tests::lease_windows_are_held_between_their_floor_and_twelve_hours
  - src/queue.rs::tests::quality_lease_windows_follow_the_u64_clamp
- **Risk reason:** No status or code changes, but the queue's redelivery semantics change silently. Legitimate windows above 12 h are shortened, and an extend-0 'release now' becomes a 1 s hold. Clients get no signal that their window was altered.
- **Check against commit:** The recorded text is correct but narrower than the change. Extend visibilityMs 1-999 also becomes 1 s, not only 0. Legitimate windows between 12 h and about 9.2e18 ms, which were honoured exactly before, are now cut to 12 h. The clamp is silent in the response.

### #12 71345c03 — HTTP/1.1 request-head and idle keep-alive deadline of 120 s

- **Program item:** rank 17
- **Surface:** both
- **Endpoint:** Every HTTP/1.1 connection on the server's single listener (bootstrap::run -> http::serve_h1 -> http::serve::h1_builder). That covers raw /v1/stream, product /v1/streams, /health, /v1/debug, operator routes and fleet-internal peer routes alike.
- **Condition:** A connection waits for a request head longer than h1_header_timeout (default 120 s). This covers a fresh socket that sends nothing, a slow or partial head, and an idle keep-alive after a completed response.
- **Before:** hyper's documented 30 s header_read_timeout was silently dropped because no timer was installed, and there was no server-side idle close. Idle keep-alive, headless and partial-head sockets were held indefinitely (a file descriptor plus a task) until the client or edge closed them.
- **After:** hyper closes the connection at the deadline without writing any HTTP response: no 408, just a FIN, or an RST if unread bytes are queued. The timer runs only while waiting for a head and re-arms after each response, so in-flight responses, long-polls (25 s max) and SSE bodies are unaffected. New knob SSE_H1_HEADER_TIMEOUT_MS, in ms: values > 0 apply; 0 or unparseable keep the default, so it can never be disabled. It is reported as http.h1_header_timeout_ms in the 'effective configuration (redacted)' startup log. The bench and livebench client pool_idle_timeout went from 120 s to 60 s.
- **Retry semantics:** The server now closes idle connections. A client or edge that reuses a pooled connection idle for about 120 s or more can race the close. It then sees a connection reset or EOF before any response, or the edge returns 502/503. That failure is transport-level and retryable, but HTTP clients usually do not auto-retry non-idempotent POSTs such as appends. Clients whose pool idle is under 120 s are unaffected: the SDK's fetch on Node/undici defaults to 4 s; peers, pilot and the store client use 4 s; edgesim 90 s; the bench tools now 60 s.
- **Who is affected:** All HTTP/1.1 clients that hold keep-alives idle for more than 120 s. The main one is the platform edge's upstream pool, whose idle timeout is unmeasured per the commit; long-idle raw-client pools are another. Slowloris or half-open sockets. Operators get the new knob and a bound on file-descriptor use.
- **Pinning tests:**
  - src/http/serve.rs::tests::headless_connection_is_closed_at_the_deadline
  - src/http/serve.rs::tests::idle_keep_alive_is_served_inside_and_closed_past_the_deadline
  - src/http/serve.rs::tests::in_flight_response_outlives_the_deadline
  - src/config/tests.rs::default_values_are_pinned (asserts the 120 s default)
  - NOT asserted: src/config/tests.rs::env_overlay_applies_with_legacy_parse_semantics sets SSE_H1_HEADER_TIMEOUT_MS=0 but asserts nothing about h1_header_timeout. No test checks that a positive override is applied, and no SSE-specific test exists.
- **Risk reason:** No status or code change, but every route's connection lifetime changes. If the platform edge's upstream pool idle exceeds 120 s, stale reuse turns into intermittent edge 5xx errors or resets on otherwise-good requests, including appends. The knob can mitigate this, and the commit itself says the edge idle is unmeasured.
- **Check against commit:** The recorded text is accurate. Nuances: the deadline also closes a slowly dribbled partial head (the test pins this), although the commit prose says it 'never runs while a head is parsed'. The commit claims 'Config pins: ... the 0-is-filtered overlay', but the overlay row has no assertion, so the 0 filter is unpinned. SSE is unaffected by mechanism only; no test covers it.

### #14 fd810688 — Panicking shard opener fails its open instead of wedging its prefix

- **Program item:** rank 15
- **Surface:** both
- **Endpoint:** Every raw and product request that resolves a shard prefix through OpenGate::get_or_open. Raw: POST/GET/HEAD/PUT/DELETE /v1/stream/{name} including ?live=sse/long-poll connect. Product: /v1/streams/{name} create, /records append/read/:sse, consumers. Operator: /health readiness. Process: the unready watchdog exit. The PostingsCache half has no wire surface.
- **Condition:** The shard opener panics, while building its future or while polling it. The production route: bootstrap::on_slatedb_rt turns a panic in the SlateDB open task into a panic on the opener task. For the postings half: a panic in the spawned owned postings load.
- **Before:** The open task died with its watch sender and the in-flight marker was never cleared. The first caller and every coalesced waiter got OpenOutcome::Failed("shard open task vanished"). Every later caller for that prefix coalesced onto the dead channel and got the same answer, until process restart: no strike, no holdoff, health not recorded, and shutdown counted a phantom pending open until its grace ran out. Rendered as: raw append/other raw routes 500 {"error":{"code":"shard_open","message":"open shard \<p>: shard open task vanished"}}; raw read/SSE connect 500 {"error":{"code":"shard_open_failed",...}}; product append 500 {"error":{"code":"append_failed","retryable":false}}; product create 500 shard_open retryable:true; product consumers 500 shard_open retryable:true; product read/SSE connect 503 temporarily_unavailable retryable:true (every ResolveError maps there). Postings: every later read of the key spun 4 Wait turns on the dead channel, loaded uncached, and prefetch stayed disabled; results were correct and only slower.
- **After:** unwind_proof maps the panic to Err("shard open panicked: \<payload>"), which takes the normal failed-open arm: strike+1, holdoff armed (holdoff_for(strikes): 6 s after a first strike, then 12/24/48, capped at 60 s), health.failed recorded, marker cleared, waiters answered. The first caller and its coalesced waiters get the same 500 code as before with the message 'shard open panicked: \<payload>'. Callers during the holdoff get 503 with Retry-After = remaining whole seconds (min 1): raw {"error":{"code":"shard_moving","message":"shard not currently serving here; retry"}}; product append 503 temporarily_unavailable retryable:true + Retry-After; product create 503 shard_moving retryable:true + Retry-After; product consumers 503 shard_moving retryable:true; product read unchanged (503 temporarily_unavailable). After the holdoff the next caller re-runs the opener: success serves normally, and a repeat panic gives another 500 and a longer holdoff. /health: panicked prefixes now count toward ShardHealth.failed, so an instance that never opened any shard and has >=3 distinct failed prefixes reports 503 'shard storage unavailable' and the unready watchdog eventually exits it (process). Postings: a panicked owned load is a failed load; the next read leads a fresh owned load and the cache recovers.
- **Retry semantics:** For callers after the first: a non-retryable, permanent 500 (shard_open/shard_open_failed; product append retryable:false) becomes a retryable 503 shard_moving (product: temporarily_unavailable retryable:true) with Retry-After, and the prefix can recover without a restart. The first caller's status and code are unchanged; only the message differs.
- **Who is affected:** SDK and raw clients hitting a prefix whose open panicked; they now see 500-then-503-with-Retry-After instead of 500 forever. Operators: /health can go unready and the watchdog can restart an instance whose opens all panic, which never happened before; shutdown no longer waits on a phantom open. Postings cache: internal latency only.
- **Pinning tests:**
  - src/sharddir/unwind.rs::tests::a_panicking_opener_fails_its_open_and_the_next_attempt_installs
  - src/sharddir/unwind.rs::tests::a_synchronous_opener_panic_is_the_same_failed_open
  - src/postings_cache/tests.rs::a_panicking_owned_load_clears_its_single_flight_marker
- **Risk reason:** The status and code change for an error case clients may branch on: raw 500 shard_open/shard_open_failed becomes 503 shard_moving with Retry-After, and product append retryable:false becomes temporarily_unavailable retryable:true. The change is strictly toward retryability and recovery, and the trigger (an opener panic) is rare. The pinning tests are gate-level (OpenOutcome), not HTTP-level; the 503/500 rendering relies on the shared existing ResolveError mappings. /health and watchdog behaviour also change for a node whose opens all panic.
- **Check against commit:** None against the recorded text. Nuances: (1) the commit message says 'No wire change', but later callers' status changes from 500 to 503; the recorded text states this correctly. (2) The 503 lasts only for the holdoff window, after which the opener is retried; it is not 503 forever. (3) Product read paths already returned 503 temporarily_unavailable, so they see no status change.

### #16 d93b421b — Live SSE tail on a retired engine gets a typed EngineRetired cutoff

- **Program item:** rank 14
- **Surface:** both
- **Endpoint:** GET /v1/stream/{name}?live=sse (raw, SingleSource) and GET /v1/streams/{name}/records:sse (product, SingleSource or LineageSource LiveLocal tail). Operator: GET /v1/debug/load gains sse_livefeed.cutoff_engine_retired.
- **Condition:** A live SSE session (parked at the tail, driving a read, or a joiner below the frozen head) whose feed source pins an engine incarnation that closes while ownership stays on this instance: the acker's close_reason on a fatal store error, a required worker's exit, a sub-tick ownership flap, or an explicit shutdown retirement. Ownership is checked first, so a moved tail is still WrongOwner.
- **Before:** cut_off/read_batch checked ownership only. begin_close woke parked sessions once; each re-checked ownership, found it unchanged, and re-parked on the dead handle's notify. The client kept an open HTTP 200 text/event-stream with keep-alives forever: no data, no control, no EOF. A reconnect resolved the replacement engine but install_source returned AlreadyCurrent for an equal-length signature, so the reconnect joined the dead feed and hung too. A joiner below the frozen head spun on the catch-up retry every 100 ms. In the drive race the dead engine's tail ring could serve a record before the session re-parked forever.
- **After:** live_tail_cutoff(owned, engine) returns SourceCutoff::EngineRetired when the pinned engine is_closed(); both the park guard and the live-tail read_batch guards use it. On the wire this is the existing typed nonterminal disconnect: the chunked body ends (EOF) with no terminal control and no sealed/streamClosed flag, and no record is served from the retired engine. FEED_CUTOFF_ENGINE_RETIRED and FEED_TOPOLOGY_DISCONNECTS increment, and /v1/debug/load sse_livefeed.cutoff_engine_retired is a new additive key. The feed tears down. A resume from the client's cursor may first get 503 while the old incarnation terminates or its holdoff runs: raw shard_closing with Retry-After 1, or shard_moving with Retry-After = remaining holdoff (the close is a Died departure, so it strikes); product 503 temporarily_unavailable retryable:true. It then gets 200 served by a NEW engine incarnation that delivers new appends.
- **Retry semantics:** A hang becomes a disconnect that clients must resume from their cursor. The resume can meet a retryable 503 (shard_closing/shard_moving/temporarily_unavailable) before it succeeds. No terminal signal is emitted, so clients must not treat the EOF as stream closed.
- **Who is affected:** Raw and product SSE subscribers (SDK and raw clients) whose engine retires under the same owner; they now see an EOF and must resume. Operators get a new /v1/debug/load counter; docs/LIVE-FEED.md says its deltas should track engine_closed ops events.
- **Pinning tests:**
  - src/dst/tests/livefeed_engine_retired.rs::livefeed_engine_retired_under_the_same_owner_cuts_a_parked_session
  - src/dst/tests/livefeed_engine_retired.rs::livefeed_engine_retired_under_the_same_owner_cuts_a_parked_lineage_session
  - src/dst/tests/livefeed_engine_retired.rs::livefeed_engine_retired_before_a_drive_is_a_typed_read_cutoff
- **Risk reason:** This changes the retry semantics of an error condition on both public SSE surfaces: a new trigger for mid-stream nonterminal EOF, and the resume can hit a 503 holdoff first. False positives are not possible, because a closed incarnation can never serve again, and the prior behaviour was a silent hang, so no working session is lost. All three pinning tests use the product route; the raw route shares SingleSource but has no dedicated raw-surface test.
- **Check against commit:** None against the recorded text. The lineage (LiveLocal) leg is product-only in practice; the single-segment leg covers both surfaces.

### #19 527d3d3a — Read-byte quota on :scan

- **Program item:** rank 28 step A
- **Surface:** product
- **Endpoint:** GET /v1/streams/{name}:scan[?cursor=&maxBytes=] (product_entry scan arm -> src/product/scan.rs::product_scan). Also re-plumbs the debit for GET /v1/streams/{name}/records and /records:long-poll with no wire change.
- **Condition:** Enforce auth mode (STREAMS_AUTH_MODE=enforce, so a verified principal exists), the project policy sets quotas.read_bytes_per_sec > 0, and the project's read-byte bucket is below zero (in debt from earlier served bytes). Deployment-bearer (Off/Shadow) requests carry no principal and are never checked or debited.
- **Before:** A :scan in read debt was served normally: 200, JSON array of {routingKey,value|valueB64}, Content-Type application/json, Cache-Control no-store, Prisma-Scan-Complete or Prisma-Next-Scan-Cursor. Scan pages (up to 8 MiB) were never debited, so they never pushed the project into debt either. Records GET/long-poll were debited after the fact from the finished Response's exact size hint (debit_read_response).
- **After:** check_read_quota runs at the scan arm before anything else in product_scan: in debt the answer is 429 {"error":{"code":"project_rate_limit","message":"the project's rate quota is exhausted; retry","retryable":true}} with retry-after: \<secs> (>=1), Cache-Control no-store and the project audit tag. Every served scan page debits body.len() (the framed page) through debit_read_bytes. Because the check comes before key, cursor and existence checks, a scan in debt with a missing or wrong key, a bad cursor, or no stream now gets 429 where it used to get 400 missing_key/invalid_cursor, 403 wrong_key or 404 not_found. A refused page is never debited. The records GET debit moves to render_product_read (payload.len()), which should be byte-identical to the old size hint; a 204 long-poll timeout debits 0.
- **Retry semantics:** 200 -> 429 retryable (retryable:true + Retry-After) while the project is in debt. The SDK's req() retries 429 at most 3 times and caps each sleep at 5 s (about 15 s in total). If the debt lasts longer, the scan() async iterator throws StreamsError(429, project_rate_limit, retryable=true). The iterator does not expose the next scan cursor, so an SDK caller cannot resume that export partway and has to start a new scan. A raw-HTTP client can re-issue the same cursor after Retry-After.
- **Who is affected:** Product API and SDK users who run :scan exports on enforce-mode projects with a configured read_bytes_per_sec, when they read faster than their budget. Scan traffic now shares one bucket with records GET and long-poll, so heavy scans can cause 429s on records reads and the other way round. Projects without a read budget (0) and Off/Shadow deployments are unaffected.
- **Pinning tests:**
  - src/dst/tests/quota_read_volume.rs::scan_pages_draw_on_the_read_byte_quota
  - src/dst/tests/quota_enforcement.rs::volume_quotas_meter_appends_and_reads
- **Risk reason:** This adds a new retryable 429 class to a public route, but only under a quota condition: enforce mode, a non-zero read budget and a bucket in debt. The common path is unchanged. It is not permanent (Retry-After is provided, and scan cursors are stateless, so re-issuing one is safe). It is still a real behaviour change for SDK exports: when debt outlasts the SDK's roughly 15 s bounded retry, the iterator ends partway and cannot be resumed. Neither the error-precedence change (429 before 400/403/404 while in debt) nor the debit of peer-relayed scan pages is pinned by a test.
- **Check against commit:** No mismatch. Precision: the check comes before the key, cursor and stream-existence checks (it only follows the principal's scope check), so 400/403/404 scan errors turn into 429 while in debt. The same commit also moves the records GET/long-poll debit from the Response size hint to the framed payload length, with no wire change.

### #20 f1c9e77e — Read-byte quota on consumer :pull

- **Program item:** rank 28 step B
- **Surface:** product
- **Endpoint:** POST /v1/streams/{name}/consumers/{consumer}:pull (src/product/consumer_pull.rs::product_consumer_pull)
- **Condition:** Quota refusal: enforce mode (ConsumerAccess::Account(principal)), quotas.read_bytes_per_sec > 0, and the project's read-byte bucket in debt. Cache-Control change: every 200 pull response in all modes.
- **Before:** A pull in read debt was served normally: 200 {"messages":[...],"backlog":n} (an empty poll is also 200). Content-Type was application/json with NO Cache-Control header (json_ok never set it, although WIRE-MATRIX §2.15 documented no-store). Pulls were billed through meter_pull but never checked against or debited from the read-byte quota.
- **After:** The first statement of the handler is check_read_quota, which runs before consumer_key, authorize (the consumer lookup), activation, body parsing and leasing. In debt the answer is 429 {"error":{"code":"project_rate_limit","message":"the project's rate quota is exhausted; retry","retryable":true}} + retry-after: \<secs>, and no lease is taken. A 200 debits the framed body length, including empty polls of about 27 bytes, and now carries Cache-Control: no-store; content-type and body bytes are unchanged. Error precedence while in debt: 429 replaces 400 (missing or invalid key, invalid_body), 404 consumer_not_found and 409 consumer_deleted/consumer_deleting. The consumers.pull scope is still checked first, at product_entry.
- **Retry semantics:** 200 -> 429 retryable (retryable:true + Retry-After) while in debt. A refused pull leases nothing, so retrying is safe. The SDK's Consumer.pull() retries 429 at most 3 times with sleeps capped at 5 s. After that the `for await (… of consumer)` / messages() iterator throws and ends with status "failed"; the application has to restart its consumer loop. Adding Cache-Control: no-store on 200 does not change retry behaviour.
- **Who is affected:** Consumer-queue workloads (SDK and raw-HTTP product clients) on enforce-mode projects with a configured read_bytes_per_sec. Queue draining now shares one bucket with records GET, long-poll and :scan, so read traffic on any of those routes can stall consumers, and consumer traffic can cause 429s on reads. Browsers and intermediaries now see no-store on pull batches. Off/Shadow deployments (ConsumerAccess::Deployment) are never checked or debited.
- **Pinning tests:**
  - src/dst/tests/quota_read_volume.rs::consumer_pulls_draw_on_the_read_byte_quota
- **Risk reason:** This adds a retryable 429 class to a hot, steady-state route, but only while the project is in read debt. It is not permanent, and a refused pull leases nothing. It has the most exposure in this group: consumer workers previously never throttled by the read quota now can be, and the SDK consumer iterator ends after about 15 s of sustained debt. At documented budget sizes (the example policy uses 50 MiB/s) debt should be rare. Test gaps: the pinning test does not assert Cache-Control: no-store on the 200, does not check that the refused pull leased nothing (backlog and attempts are not inspected), and does not cover the error-precedence change.
- **Check against commit:** No mismatch. Precision: the check comes before key, consumer-existence and body validation, so those 400/404/409 errors turn into 429 while in debt; empty polls are debited too; Cache-Control: no-store is added only to the 200 (error responses from perr already had it); no test pins the no-store header.

### #24 74391f9c — Relayed read verdict is typed on the fleet page route

- **Program item:** item 22
- **Surface:** product
- **Endpoint:** Wire change: GET /v1/internal/segment-read/{\*name} with streams-internal-read-page: 1 (src/http/read.rs::respond_read/typed_refusal; coordinator src/application/read_remote.rs::remote_read_page/peer_refusal). Client-visible via GET /v1/streams/{name}/records?deliver=applied (non-long-poll pages the read service relays to another owner, allow_remote=true). Raw clients cannot set deliver=applied: ReadParams.deliver is serde-skipped.
- **Condition:** A product records read with deliver=applied is coordinated by a non-owner instance and relayed to the span owner, and the owner's read service decides CursorBeyondTail (applied cursor ahead of the applied tail). The internal typed body is also emitted for ChangedIncarnation, Missing and Gone decided by the read service.
- **Before:** Owner page route: every refusal used the public envelope, e.g. 409 {"error":{"code":"cursor_beyond_tail",...}}. The coordinator did not read the body and mapped by status only (404 Missing, 410 Gone, 401 Unauthorized, 409 ChangedIncarnation, 429/503 Retryable). The client therefore received 409 {"error":{"code":"target_mismatch","message":"stream incarnation changed","retryable":false}}. The SDK treats target_mismatch as non-retryable and threw, ending the applied subscription instead of rewinding.
- **After:** Owner page route: for a verdict the read service decided (cursor_beyond_tail / changed_incarnation / missing / gone), the status is unchanged (409/409/404/410) and the body is {"refused":"\<verdict>"}. Every other refusal (ownership 409 + Streams-Replay-To, 503-class, 400/403 for the coordinator's key or cursor, and anything decided before the read service ran: 401, registry 404, 400 invalid_target, 409 stale_target), and every request without the header, keeps the public envelope. The coordinator reads the body first and decodes a typed body as the verdict whatever the status. An untyped answer keeps the old status mapping, including a bare 409 -> ChangedIncarnation, and error.code is never read. Client: 409 {"error":{"code":"cursor_beyond_tail","message":"cursor is ahead of the stream tail; resume from the durable cursor","retryable":false}}, the same answer the local owner path gives. Side effect: the coordinator now reads a non-2xx body before classifying it, so a body that exceeds the wire bound or fails mid-transfer becomes Remote -> 503 temporarily_unavailable where it used to use the status class.
- **Retry semantics:** The public error code changes from target_mismatch to cursor_beyond_tail for this case; status 409 and retryable:false stay the same. SDK: applied subscriptions go from a thrown error to a rewind to Prisma-Durable-Cursor. Rolling-deploy safe both ways: an old owner sends an untyped 409 (still ChangedIncarnation), and an old coordinator ignores the body and maps the unchanged status. stale_target is deliberately still ChangedIncarnation.
- **Who is affected:** Product API and SDK users of deliver=applied records reads that are served across owners (segmented streams whose span owner is another instance). Fleet peers: the page-route refusal body format changes, and both old and new versions tolerate it. Raw clients are unaffected.
- **Pinning tests:**
  - src/dst/tests/read_application.rs::relayed_applied_read_beyond_the_tail_keeps_the_owner_verdict
  - src/dst/tests/read_application.rs::the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope
  - src/application/read_remote_tests.rs::the_page_route_refusal_names_the_owner_verdict
  - src/application/read_remote_tests.rs::a_typed_refusal_is_relayed_and_an_untyped_answer_keeps_its_transport_class
- **Risk reason:** This changes a public product error code, for an error case the SDK explicitly branches on. The change restores the documented contract and turns a failure into a rewind, but by the rubric it is a code change. Tests pin the coordinator verdict (remote_read_page returns CursorBeyondTail), the internal wire body, and the envelope staying on the public route and on requests without the header. Nothing drives an end-to-end product HTTP 409 cursor_beyond_tail through a relay, or the SDK rewind, and the body-read-before-status side effect is not covered.
- **Check against commit:** No mismatch in substance. The wire change is on the fleet-internal page route (recorded as fleet-internal), but its only client-visible effect is a public product error-code change on GET /v1/streams/{name}/records?deliver=applied (target_mismatch -> cursor_beyond_tail), so surface is recorded as product. Long-poll is not affected: remote_read_page is skipped for ReadMode::LongPoll.

### #26 a8c99c23 — Append larger than a fresh bucket is 413; boot refuses unadmittable limits

- **Program item:** item 25
- **Surface:** both
- **Endpoint:** Raw POST /v1/stream/{name}, including append-and-close. Product POST /v1/streams/{name}/records and /records:batch. Also process startup (ServerConfig::validate → main.rs exits 1) and the GET /v1/debug/load limiter counters.
- **Condition:** A content append whose body bytes exceed LIMIT_BYTES_PER_SEC×LIMIT_BURST_SECS (10,000,000 at defaults) or whose record count exceeds LIMIT_RECS_PER_SEC×LIMIT_BURST_SECS (10,000 at defaults), with no deferred producer verdict. Raw has no smaller per-POST record cap, so it is reachable at defaults. The product batch cap is MAX_BATCH_RECORDS=10,000 (400 batch_too_large above that), so the product records case is reachable only when record capacity is below 10,000. Boot: any limit knob that is non-finite, negative, a burst \<= 0, a rate×burst that overflows, or an enabled bucket with rate×burst \< 1.
- **Before:** Records over capacity. Raw: 429 {"error":{"code":"limit_records_per_sec","message":\<limit message>}} with retry-after = ceil((records − tokens)/rate) s, min 1 (1 only when the bucket is full); every retry refused, forever. Product: 429 {"error":{"code":"rate_limited","message":"admission or rate limit","retryable":true}} with retry-after. Decided in admit_usage after renew_ttl, so the refused request still renewed a sliding TTL, and it was counted by note_limit_refusal (/v1/debug/load). Bytes over capacity (non-close): raw 413 payload_too_large 'request exceeds the per-stream ingest capacity', product 413 body_too_large retryable:false. Also decided after renew_ttl, and also counted as a bytes limiter refusal. Close with content already got an early 413 (bytes, records, or a sub-token 'requests' arm). Boot accepted any float: LIMIT_BURST_SECS=0 made every append 429 forever; NaN, negative or infinite values silently disabled a limit; a sub-token bucket (e.g. 0.1 req/s × 2 s) made every append 429 limit_requests_per_sec forever.
- **After:** Both kinds are refused in parse_content before renew_ttl and before install_intent, with no retry-after. Raw: 413 {"error":{"code":"payload_too_large","message":"request exceeds the per-stream ingest records capacity"}} (bytes: '… ingest bytes capacity'). Product: 413 {"error":{"code":"body_too_large","message":"request body exceeds the limit","retryable":false}}. A deferred producer verdict still answers first (e.g. 400 invalid_body). note_limit_refusal is never reached, so /v1/debug/load counts transient 429s only. The admit_usage bytes-413 arm is deleted, and every 429 it answers now names a wait the bucket will honour. Boot: validate() rejects the postures above, prints 'Error: …LIMIT\_…' to stderr and exits with code 1. The rejection applies to all three buckets (bytes, requests, records).
- **Retry semantics:** Records over capacity: a retryable 429 with Retry-After becomes a permanent 413 without Retry-After (product retryable true -> false). The request could never be admitted under either behaviour. Bytes over capacity: status and code unchanged (already a permanent 413); only the decision point, the message text and the debug counter change. Startup: some configurations that used to boot now exit 1.
- **Who is affected:** Raw clients that POST more than 10,000 records at defaults: SDK loops that retried the 429 now get a terminal 413. Product SDK users only on cells whose record capacity is below 10,000. Operators whose env sets a limit knob to NaN, inf, a negative value (previously an ad-hoc 'disable'), burst 0 or a sub-token rate: the process now refuses to start. The repo's bench and soak scripts set only large finite values, so none of them is refused. Dashboards that read the /v1/debug/load limiter counters. TTL streams: an oversized append no longer renews the sliding TTL.
- **Pinning tests:**
  - src/dst/tests/admission_maintenance.rs::over_capacity_record_count_is_a_permanent_413_not_a_429
  - src/dst/tests/admission_maintenance.rs::product_batch_over_record_capacity_is_413_without_retry_after
  - src/dst/tests/admission_maintenance.rs::a_deferred_producer_verdict_outranks_the_capacity_refusal
  - src/usage/runtime_tests.rs::the_runtime_owner_decides_only_the_requests_own_size
  - src/usage/runtime_tests.rs::permanent_refusal_is_exactly_fresh_bucket_refusal (proptest, 1,024 cases)
  - src/config/admission_limits.rs::tests::defaults_validate_and_zero_disables_a_bucket
  - src/config/admission_limits.rs::tests::non_finite_or_negative_limits_are_refused_by_name
  - src/config/admission_limits.rs::tests::an_enabled_bucket_must_hold_one_token
  - src/config/validation_tests.rs::validate_boundary_tests::validation_rejects_a_limit_posture_that_can_never_admit
- **Risk reason:** A public status and code change (429 limit_records_per_sec / rate_limited -> 413 payload_too_large / body_too_large), and retry semantics flip from retryable to permanent. This hits an error case clients may branch on, and on raw it is reachable at the default posture. Not graded high because no such request could ever succeed. The boot refusal is fail-closed but can block an upgrade for an operator with an exotic limit env. Test gaps: no wire test pins the bytes-over-bucket 413 on a non-close append at its new early position, or the 'before TTL renewal' ordering; the /v1/debug/load exclusion is deliberately left unasserted.
- **Check against commit:** Matches, with imprecisions. (1) The old Retry-After was computed (ceil(deficit/rate), min 1) and was 1 s only when the bucket was full, so 'retry-after: 1' is not exact. (2) The boot refusal covers sub-token bytes and records buckets too, not only the request bucket. (3) The raw bytes-413 message text changed ('ingest capacity' -> 'ingest bytes capacity'). (4) Unrecorded side effect: oversized appends used to renew the stream's sliding TTL before being refused, and no longer do.

### #34 85017d84+729c52ac — Split or merge the segment map refuses keeps its intent pending

- **Program item:** item 64
- **Surface:** both
- **Endpoint:** Appends to the parents' key range: POST /v1/stream/{name} (raw) and the product append under /v1/streams/{name}, both via AppendService::execute_prepared → closure_is_current. Reads of that range (read_request.rs seal_gap) also change. Operator: GET /v1/debug/load scaler.segment_splits / segment_merges / segment_map_refreshes, and ops events split_committed / merge_committed (/v1/debug/ops-events and the ops sink).
- **Condition:** A latent state: phase B of a split or merge reaches map.split / map.merge with the intent still pending and the parent engine(s) already physically closed, and the segment map refuses. Cases: IdExhausted, with the allocator at u32::MAX-1; the high child's id overflowing at u32::MAX (now a checked add); a merge whose parent is already sealed (AlreadySealed); or, via 729c52ac, a merge at u32::MAX, which previously panicked in debug and wrapped to 0 in release. The commit says no reachable production path produces this state.
- **Before:** The Err(\_) arm was treated as an idempotent completion: it set map.pending = None, bumped map.version, wrote the map and returned published = true. The parent stayed live in the map over a closed engine. A writer's append met the engine's closure, and closure_is_current found no pending transition and the segment live, so it returned the closure as final. Raw answered 409, code stream_closed, 'stream is closed', with stream-closed: true and stream-next-offset headers and no Retry-After. Product answered 409, code 'sealed', 'collection is sealed', retryable:false. Readers saw closed && !seal_gap, i.e. Stream-Closed / sealed reported as final. SEGMENT_SPLITS or SEGMENT_MERGES and SEGMENT_MAP_REFRESHES were incremented, the scaler retired the parents' sketches, and split_committed / merge_committed ops events were emitted.
- **After:** Every MapError now returns Mutation::Decline(false) and nothing is written. The intent stays pending, the map version is unchanged, resume\* returns false, and one tracing::error! line is logged with the typed MapError ('split stays pending…' / 'merge stays pending…'). SegmentMap::merge allocates the child id with checked_add before sealing either parent, so a refused merge leaves the map untouched. A writer to the range sees the closed engine, then closure_is_current sees the pending intent, schedules a topology ticket and waits (bounded by JOB_TIMEOUT 10 s). It retries up to 4 attempts with 10/20/30 ms waits and ends with 503 and Retry-After: 1. On raw the code is segment_transition, 'segment map transition did not converge; retry' (or the WorkError text). On product it is 503 code temporarily_unavailable, 'retry shortly', retryable:true, Retry-After: 1. Readers see seal_gap = true, so closed and Stream-Closed are not reported and upToDate is false. No counters are incremented and no \*\_committed events are emitted.
- **Retry semantics:** A permanent 409 (stream_closed / product 'sealed', retryable:false, Stream-Closed: true) becomes a retryable 503 (segment_transition / product temporarily_unavailable, retryable:true, Retry-After: 1). Because the refusal (e.g. allocator exhausted) does not clear itself, writers in this state retry indefinitely against a range that stays unavailable until an operator steps in. Before, they were told, wrongly, that the collection was finally closed.
- **Who is affected:** Only in the latent refused state: SDK and raw writers and readers of the parents' key range; operators reading split/merge counters and \*\_committed ops events, which no longer over-count; and the scaler, whose parent sketches are no longer retired early.
- **Pinning tests:**
  - a_split_the_allocator_cannot_number_keeps_its_intent_pending (src/dst/tests/topology_scaling.rs)
  - a_split_with_no_id_for_its_high_child_keeps_its_intent_pending (src/dst/tests/topology_scaling.rs)
  - a_merge_the_segment_map_refuses_keeps_its_intent_pending (src/dst/tests/topology_scaling.rs)
  - an_exhausted_allocator_refuses_before_any_parent_seals (src/segmap.rs tests)
  - r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final (src/dst/tests/append_application.rs; pre-existing pin of the writer-side retryable segment_transition contract, not specific to the refused state)
- **Risk reason:** The status, error code and retry semantics change for an error case clients may branch on: a permanent 409 stream_closed/sealed becomes a retryable 503 with Retry-After. The state is latent (no reachable path per the commit), and the old answer was false finality. But the new state never clears itself, so clients retry indefinitely. Neither form is pinned at the HTTP level for this state.
- **Check against commit:** The recorded text matches both commits but is imprecise in two ways. (1) On the product surface the retryable answer is 503 code 'temporarily_unavailable' (retryable:true, Retry-After: 1), not 'segment_transition'; only raw exposes segment_transition. (2) It omits the reader-side twin: with the intent kept pending, reads compute seal_gap = true and no longer report Stream-Closed/sealed for the parents' range. The new DST tests pin map state (resumed=false, intent kept, version unchanged), not the writer's 503.

### #46 1791207a — Live SSE lease ends when its project is placed on another cell

- **Program item:** item 65
- **Surface:** product
- **Endpoint:** GET /v1/streams/{name}/records:sse (product_read with live=sse; enforce mode only, where a customer principal attaches an AuthLease). This is the only producer of customer leases (product.rs:2821). Also the lease_terminations.project_missing counter.
- **Condition:** A project's policy is republished, with new policy and feed versions that publication accepts, placing it on another cell, while this cell holds a live SSE subscription or is establishing one for that project.
- **Before:** Request verification already refused a foreign placement with 421 wrong_cell (prisma-error-code: wrong_cell, retryable:false). lease_check only asked whether the project was present in the snapshot, so a live subscription's lease kept re-proving Ok and the stream kept delivering records on a cell that no longer serves the project. It also established with 200 text/event-stream if the republish landed between verification and LeaseWatch::new_checked.
- **After:** lease_check asks served_policy (present AND cell_id == this cell) and returns LeaseInvalidReason::ProjectMissing. For a live subscription, the next generation wakeup or per-frame gate ends the SSE body cleanly: EOF, no error frame, queued frames discarded unbilled. lease_terminations.project_missing is counted once. The client's reconnect then gets the unchanged 421 wrong_cell. At establishment, reachable only in the verification-to-establishment race window, the response is 403 with body {"error":{"code":"project_missing","message":"authorization was invalidated before the subscription could be established"}}. There is no retryable field and no Retry-After (err_resp shape). Request verification's answer is unchanged (421 wrong_cell). No new code or metric key.
- **Retry semantics:** A live subscription that previously continued now terminates. Reconnect semantics are unchanged: 421 wrong_cell, meaning re-resolve the project's endpoint. In the establishment race, a request that previously succeeded now gets 403 project_missing, a Denied class that looks permanent, where the request path says 421. That asymmetry is documented in auth/refusal.rs.
- **Who is affected:** Enforce-mode product SDK users holding SSE subscriptions during a project re-placement (cell transfer). Operators see the project_missing termination counter rise during transfers. Watch-capability status (status_and_quotas) keeps the same gap; that was held as a decision.
- **Pinning tests:**
  - auth::tests::a_lease_ends_when_its_project_is_placed_on_another_cell (new, red-first: left Ok(()), right Err(ProjectMissing))
  - sse::auth::tests::lease_refusals_answer_their_class_status (pre-existing; pins ProjectMissing -> 403 with code project_missing)
  - sse::auth::tests::lease_terminations_have_one_slot_per_reason (pre-existing)
- **Risk reason:** This changes the lifetime of successful live product subscriptions: they now end on re-placement. It also introduces a 403 project_missing at establishment (race-only) for a condition the request path answers 421, which clients may branch on as a permanent denial instead of re-resolving. The previous behaviour was incorrect (serving from a cell that no longer owns the project), and reconnect semantics are unchanged. No end-to-end SSE test pins the live termination on re-placement; only the unit lease_check test does.
- **Check against commit:** None in behaviour. Precision: '403 at establishment' is reachable only when the republish lands between request verification (already 421 wrong_cell for a foreign placement) and LeaseWatch::new_checked. The common visible effect is a clean EOF on a live stream. Cosmetic defect: served_policy was inserted between status_and_quotas's doc comment and its fn, so at HEAD (src/auth.rs \~726-741) status_and_quotas's doc comment is attached to served_policy and status_and_quotas has no doc.

## Low risk (37)

None of these changes alters a status, code or header on a path that worked before. Most are internal, operator-facing or timing-only; the rest correct data inside successful responses, or turn a failure (or a hang) into a success.

### #3 1994fc25 — Consumer pull honours its deadline under a blocked dead-letter handoff

- **Program item:** bug #4
- **Surface:** product
- **Endpoint:** POST /v1/streams/{name}/consumers/{consumer}:pull (application::consumer::delivery::pull). The shared dlq_and_settle is also reached from POST /v1/streams/{name}/consumers/{consumer}:settle, where the response fields dlq and dlqBlocked do not change.
- **Condition:** A Receive reports poisoned leases: max-delivery reached on an expired lease. Two cases. (a) The dead-letter handoff settles nothing: the target was deleted or recreated as another incarnation, the target lookup failed, a by_off miss, a definitive append rejection, a transient append error, or engine re-resolution failed (the last one is removed). (b) The same Receive also granted leases to OTHER routing keys.
- **Before:** (a) The branch ran `let _ = dlq_and_settle(..)` and then an unconditional `continue 'outer` that jumped above the only deadline check and sleep. The pull never answered, even with waitMs=0. Every pass cost a read_coverage and a durable cursor put. The HTTP request hung until the client or proxy timed out. (b) The poison branch came before delivery, so leases the Receive had durably granted to other keys were dropped undelivered. Each lost an attempt and a visibility window, and at maxAttempts=1 a record was dead-lettered without ever reaching a client. The response then came from a later Receive.
- **After:** (a) 200 {"messages":[],"backlog":n} by the pull's waitMs deadline (at most 25 s, 50 ms poll loop). The poisoned lease stays retained, so its key stays blocked, and a failed dead-letter append is logged at warn. (b) 200 with the co-granted leases in messages (id, routingKey, attempts, leaseToken, value), delivered whether or not the handoff settled anything. The pull loops back only when the handoff settled more than 0 leases, which is bounded by the poisoned count. Status and error codes are unchanged.
- **Retry semantics:** None at the status/code level. A request that hung forever now returns a 200 empty page at its deadline. The number of attempts consumed per record drops, because leases are no longer dropped.
- **Who is affected:** Product consumer (queue) users, SDK pull loops, where poison and dead-lettering occur. The hang needs a dead-letter target whose handoff cannot settle, for example a DLQ stream that was deleted and recreated. Dropped leases affected any consumer whose Receive mixed poison with healthy keys.
- **Pinning tests:**
  - a_blocked_dead_letter_handoff_neither_wedges_the_pull_nor_drops_other_keys_leases (src/dst/tests/consumer_dlq.rs; 5 s bound; waitMs default and 200)
  - a_settled_dead_letter_handoff_still_delivers_the_same_receives_leases (src/dst/tests/consumer_dlq.rs)
- **Risk reason:** No status, code or body-shape change. One case could not succeed before at all (an infinite hang now answers 200). The other now returns records that used to be silently dropped. Clients see more complete 200 pages and lower attempt counts.
- **Check against commit:** None material. The settle path also changed internally: it passes the held engine instead of re-resolving per record, and it logs transient dead-letter append failures. Its dlq and dlqBlocked counts are unchanged, except that a failed per-record engine re-resolution can no longer skip a settlement.

### #4 d2e87129 + 3dfb970f — Postings cache no longer over-claims or publishes stale coverage

- **Program item:** bug #1
- **Surface:** product
- **Endpoint:** GET /v1/streams/{name}/records?routingKey=... and its live :long-poll/:sse forms. Every product read carries a selector (the default key is ""), so this covers any product read served from the absorbed history partition via read_history2_keyed_cached. Fleet-internal /v1/internal/segment-read relays that carry a key use the same path. No raw-surface impact: raw reads are keyless.
- **Condition:** Default configuration (POSTINGS_CACHE_BYTES 64 MiB). Two triggers. (1) Over-claim: a cold or extension postings load (spawn_load/prefetch) published provable_to as the bucket end (a multiple of 65,536) instead of clamping it to its absorbed target, claiming postings that did not exist yet. (2) Stale publish: spawn_load built its slice from its lead-time snapshot and overwrote a write-through install_chunk that landed mid-load. Either way, a later catch-up read over the hole got a cache Hit proving 'no matches'.
- **Before:** 200 read pages silently omitted matching durable records; the reproduction returned [] where [4,5,6,7] was expected. The page/cursor advanced past those records, so the reader lost them permanently with no error.
- **After:** 200 pages contain every matching durable record. load_runs is the single owner of the coverage claim: .min(target_offset), with runs clipped to that claim. publish_load merges against the entry resident at publish time and never lowers indexed_to_offset or drops resident runs. A byte-capped load returns an honest short provable_to and the reader resumes, which is the existing partial-page contract. Response shape and status are unchanged. Documented trade-off (performance only): a cold load that stops short of a chunk-only entry publishes nothing, and such reads are served by direct loads until they reach the seam.
- **Retry semantics:** None.
- **Who is affected:** Every product reader of records from the history partition (catch-up, replay, live resume), including SDK read()/subscribe() and consumers of peer-relayed reads. Records they used to lose silently are now delivered. Readers that already advanced past a hole before the fix do not get those records back.
- **Pinning tests:**
  - keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records (src/dst/tests/reads_history.rs)
  - cold_load_claims_only_to_its_absorbed_target (src/postings_cache/tests.rs)
  - cold_load_clips_runs_the_store_holds_past_its_target (src/postings_cache/tests.rs)
  - load_publish_joins_an_install_that_landed_mid_load (src/postings_cache/tests.rs)
  - load_publish_never_regresses_an_entry_extended_mid_load (src/postings_cache/tests.rs)
  - quality_a_cold_load_never_hides_a_later_install (src/postings_cache/tests.rs, proptest 1,024 cases)
  - publish_load_merges_into_the_resident_entry_or_publishes_nothing (src/postings_cache/tests.rs)
  - publish_load_joins_a_chunk_only_entry_only_across_a_proven_seam (src/postings_cache/tests.rs)
  - load_publish_evicts_least_recent_others_only_over_budget (src/postings_cache/tests.rs)
  - byte_capped_load_claims_through_its_last_decoded_run (src/postings_cache/tests.rs, added by 3dfb970f to kill the 2 surviving mutants)
  - clipped_to_cuts_at_the_boundary_and_keeps_whole_run_weight (src/postings/validated/tests.rs)
  - quality_clipped_runs_are_exactly_the_offsets_below_the_cut (src/postings/validated/tests.rs, proptest 1,024 cases)
- **Risk reason:** No wire change: same statuses, codes, headers and body shapes. It is a data-completeness fix inside 200 responses, and the only other effect is a documented performance trade-off. 3dfb970f adds tests only.
- **Check against commit:** None. Scope note: because product reads always carry a selector (default ""), the fix covers all product history reads, not only reads with an explicit routingKey.

### #5 84f80b75 — Billing tombstone walk covers every project in the cell

- **Program item:** bug #5
- **Surface:** product
- **Endpoint:** No request/response contract change. This is the background billing::tombstone_walk. The effect shows up in the billing rollup/invoices and in the product usage month rows for terminal incarnations: GET /v1/streams/{name}/usage?month=&streamId=, field storageByteSeconds. Internally, Registry::list_page_raw is replaced by the cell-wide Registry::reconciliation_page; the public list_page/catalog goes through the private project_page.
- **Condition:** A stream in any project OTHER than the deployment tenant becomes terminal while its usage row is CLEAN. In enforce mode that is nearly every customer stream. Terminal means deleted, expired or fork-retained. Typical cases: an idle stream expires by TTL (nothing closes at expiry), or a DELETE's close is lost to a foreign owner, a failed committer submit, or a crash after the tombstone write.
- **Before:** The walk paged only the deployment tenant's catalog prefix, so these rows were never closed. owned_frame_bytes_current stayed above 0, and the rollup carried that storage into every month close indefinitely, surviving deletion. Dirty rows were reconciled correctly in every project.
- **After:** The walk pages registry/v4/projects/ cell-wide: 256 descriptors per pass, terminal descriptors included, cursor = last consumed object key. For each terminal descriptor whose segments this instance owns, it resubmits the close at the persisted logical time. The gauge is zeroed and storage stops accruing. Log lines print the project-qualified ref. The walk fails CLOSED on any key it cannot bind (sref_of_key must round-trip through desc_path), on an undecodable descriptor, or on one over 4 MiB anywhere in the cell: it logs 'tombstone walk paused (registry list)' and does not advance. System-project streams (system/v1/cells/...) are still not walked, which is unchanged.
- **Retry semantics:** None.
- **Who is affected:** Customers of non-deployment projects, whose invoices and usage rows for deleted or expired streams stop carrying storage (billed less; the correction is toward truth). Billing and finance, whose totals change at month close. Operators: one unbindable object under any project now stalls reconciliation for the whole cell, where before only a bad object in the deployment tenant could.
- **Pinning tests:**
  - tombstone_walk_closes_clean_terminal_rows_of_every_project (src/dst/tests/billing_controller.rs)
  - r09_tombstone_walk_advances_one_catalog_page_per_pass (src/dst/tests/billing_controller.rs; pinned test re-pinned: expected cursor is now the descriptor key, progression 256/256/1 unchanged)
  - reconciliation_page_reaches_terminals_of_every_project_in_key_order (src/registry/catalog/tests.rs)
  - reconciliation_page_fails_closed_on_a_key_it_cannot_bind (src/registry/catalog/tests.rs)
  - a_minted_key_binds_exactly_its_own_identity (src/registry/catalog/tests.rs, proptest 1,024)
  - no_damaged_key_binds (src/registry/catalog/tests.rs, proptest 1,024)
- **Risk reason:** No request fails and no status or code changes. The only customer-visible effect is lower (corrected) billed storage for terminal incarnations. The residual risk is operational: the cell-wide walk's fail-closed pause on one bad descriptor key in any project.
- **Check against commit:** The commit matches the record. Additional effects not in the record: (1) the cell-wide fail-closed pause described above; (2) the walk cursor changed from a stream name to an object key (in memory only, so no upgrade migration); (3) the product catalog's key binding is stricter. A non-canonical key under a project prefix, for example uppercase hex, which unhex used to accept, now fails the list page ('non-canonical catalog key') instead of being listed. desc_path never mints such keys; (4) a pinned test body (r09) was re-pinned.

### #6 c19cc2f0 — Absorber never waits for pool bytes while holding some

- **Program item:** bug #8
- **Surface:** operator-debug
- **Endpoint:** No wire change. This is the internal history absorber's v2 gather: AbsorbReservation::grow is replaced by try_grow, and stage_chunk is no longer async. The only direct observable is a new info log line: 'v2 gather {prefix}: pool refused +{short} B at a {batch} B batch; deferring'.
- **Condition:** Two or more shard absorbers in one process AND ABSORB_GLOBAL_GATHERS >= 2. The binary default is 2 per RUNBOOK; the certified deploy/profiles/compute-1g.env sets 1 and is unaffected. The trigger is two concurrent gathers that each outgrow their adaptive reservation on the shared ABSORB_GLOBAL_BUDGET_BYTES pool.
- **Before:** grow() awaited acquire_many with no timeout while keeping its existing grant. Two gathers each held part of the pool and waited forever for the rest (hold-and-wait), keeping both gather slots. Every absorber in the process then parked, and nothing was logged. Absorption stalled process-wide until an engine closed. The only thing clients would eventually see is the existing unabsorbed-backlog/maintenance-backpressure behaviour on the affected shards.
- **After:** try_grow refuses without waiting. The batch is closed, the refused chunk goes to the existing deferred_budget outcome (kept pending), and no further read_wave is issued for that gather. The demand is recorded so the next tick's reserve() asks for the full size up front, and reserve() is now the only wait on the pool, made while holding no bytes. One info log per refused gather. Deferred work lands one tick later. An oversized single frame on an empty batch is covered the same way.
- **Retry semantics:** None.
- **Who is affected:** Operators running binary defaults, or any profile with ABSORB_GLOBAL_GATHERS >= 2 and several shards per process. Indirectly, clients of those processes, who no longer risk the backlog/backpressure that follows an absorber wedge.
- **Pinning tests:**
  - two_gathers_outgrowing_a_shared_pool_defer_instead_of_deadlocking (src/dst/tests/history_gather.rs)
  - a_refused_oversized_chunk_defers_and_sizes_the_next_reservation (src/dst/tests/history_gather.rs)
  - reservation_try_grow_accounts_clamps_and_never_waits (src/history.rs unit test; replaces the deleted reservation_grow_accounts_clamps_and_waits and reservation_grow_cancel_leaves_grant_intact)
  - an_oversized_chunk_gathers_alone (src/dst/tests/history_gather.rs, unchanged, still green)
  - v2_gather_packs_to_the_aggregate_budget (src/dst/tests/history_gather.rs, unchanged, still green)
- **Risk reason:** Internal only, with no wire, status or code change. It removes a process-wide deadlock, a case in which no absorption could succeed. The only behavioural cost is one tick of added latency for chunks the pool refuses.
- **Check against commit:** None. Note for the reviewer: the two old tests that pinned the in-grow wait as intentional cross-shard backpressure were deleted, not adapted. That backpressure now lives only in reserve().

### #7 5292e3e3 — Feed retry task no longer reads and publishes records

- **Program item:** bug #6
- **Surface:** both
- **Endpoint:** Product GET /v1/streams/{name}/records:sse and raw keyed GET /v1/stream/{name}?live=sse (LiveFeed sessions, src/sse/session.rs). Raw keyless SSE sessions are unchanged, because they already disconnect on any source swap (raw_keyless && generation != join_gen).
- **Condition:** The feed's single transition-retry task is armed during a split or merge: a session saw a closed source tail whose successor could not be resolved yet (RetryLater, a refresh error, or the 4-swap budget ran out). On a later 250 ms tick the successor can be installed and already holds records past the feed head. (A) One subscriber is parked at the head and has reached live. (B) After a 2->1 leave, the survivor still has an unread retained shared batch.
- **Before:** The tick called the subscriber drive (drive_once -> drive_under_permit) under the permit. It installed the successor, read its records as a Solo drive, and threw them away. (A) The solo read moved head and floor past the subscriber's cursor. The session's next take_visible returned Lagged, and because it had reached live, it ended the SSE body with no terminal control (a 'lag disconnect below feed floor', counted in FEED_LAG_DISCONNECTS). The client had to reconnect and resume from its cursor; nothing was lost durably. (B) The retained ring held the floor, so no lag fired. The survivor drained the ring and found the head already past the offsets the tick had read. Its next drive was Idle, and later drives sent only newer records, so the offsets the tick read were never sent on that connection. The cursor and nextCursor moved past them: a silent gap, with no disconnect.
- **After:** The tick only settles the tail (Tail enum, no variant that carries records). It installs the successor, or retires the feed as Closed or Gone (incarnation cutoff or incompatible topology), and never reads. When the tail is readable, the tick bumps the feed version after releasing the permit, so parked sessions wake and read those records through their own drive. On a readable but closed tail, the bump repeats on every 250 ms tick until a subscriber reads it. The result: no lag disconnect and no gap, and the session delivers the successor's records in order. Statuses, headers, frame formats, terminal controls and cutoff behaviour are unchanged. Operators see fewer sse lag disconnects and more feed version bumps.
- **Retry semantics:** No wire change. A spurious mid-stream disconnect (a forced client reconnect) and a silent skip of live records are removed. The client's resume contract is unchanged.
- **Who is affected:** SSE subscribers: SDK subscribe on records:sse, and raw keyed live=sse clients. Only collections that split or merge while a single subscriber (A) or a surviving subscriber (B) is attached. Operators watching the /v1/debug/load sse_livefeed counters.
- **Pinning tests:**
  - src/sse/feed/tests/retry.rs::retry_install_leaves_the_successor_read_to_the_singleton
  - src/sse/feed/tests/retry.rs::retry_never_skips_the_survivor_past_a_retained_ring
  - src/sse/feed/tests/retry.rs::retry_tick_on_a_readable_tail_wakes_the_session_and_reads_nothing
  - src/sse/feed/tests/retry.rs::retry_keeps_ticking_while_a_closed_tail_is_still_unread
  - src/sse/feed/tests/retry.rs::retry_keeps_ticking_until_the_transition_settles
  - src/sse/feed/tests/retry.rs::retry_cuts_the_feed_off_on_an_incompatible_successor
  - src/sse/feed/tests/retry.rs::a_swap_storm_is_continued_by_the_next_attempt
  - src/sse/feed/tests/retry.rs::retry_ends_when_the_last_subscriber_leaves
  - src/sse/feed/tests/retry.rs::retry_ends_at_once_when_the_feed_is_cancelled
  - (These are feed-level unit tests only. This commit adds no end-to-end SSE wire test.)
- **Risk reason:** The surface is public, but no status, code, header or frame changes. The change removes a spurious disconnect and a silent delivery gap, and nothing that worked before now fails. The remaining risk is liveness: parked sessions now depend on the tick's version bump to wake. The wake tests cover this.
- **Check against commit:** The recorded text is correct but understates the BEFORE behaviour. Besides the solo lag-cut (scenario A), the commit's scenario B (retry_never_skips_the_survivor_past_a_retained_ring) shows a silent record skip: a survivor with a retained ring received no disconnect, and the records the tick read were never sent. That is a live-delivery gap, not just a reconnect. Also not recorded: the tick now re-bumps the feed version every 250 ms while a closed tail stays unread. This is deliberate and documented.

### #11 063a674d — Registry read that raced a write no longer re-installs what it read

- **Surface:** both
- **Endpoint:** Every route that resolves a descriptor through Registry::get on the process: raw /v1/stream/{name} PUT, POST, GET, HEAD and SSE; product /v1/streams/{name}/... appends, reads, SSE, watches and consumers; internal SSE sources.
- **Condition:** On process P, a registry read misses the cache (or renews with a 304) and its store GET is in flight when a write on P completes and invalidates: a create, delete, recreate, seal, split/merge, or config/TTL update. Also: recreate loses its CAS.
- **Before:** The raced read inserted what it fetched after the write's invalidate, unconditionally. Every reader on P then served the pre-write descriptor, or a cached absence, until the 5 s TTL ran out. Examples: 404 not_found for up to 5 s after a successful 201 create on P; appends routed by the pre-transition map (the 409 sealed of #8); reads, SSE and consumers acting on a stale seal, topology or config. A recreate that lost its CAS cached the incarnation it read.
- **After:** Each stream has a slot with a generation. A raced read returns the store's answer to its own caller only and never publishes it to the cache. The next read pays one store GET and sees the post-write descriptor. A lost recreate invalidates instead of caching, costing one extra GET later. For any given descriptor state, statuses and bodies are unchanged.
- **Retry semantics:** None on the wire. A transient stale window of up to 5 s on the writer's process is removed. Store GETs (billable object-store operations) can increase under write contention.
- **Who is affected:** Clients whose requests land on the process that performed a write during a racing read. Other instances keep their own 5 s TTL staleness, unchanged.
- **Pinning tests:**
  - src/registry/cache/tests.rs::a_read_that_raced_a_write_never_publishes_the_pre_write_descriptor
  - src/registry/cache/tests.rs::a_renewed_304_never_outlives_the_write_it_raced
  - src/registry/cache/tests.rs::a_read_that_raced_a_create_never_publishes_the_absence
  - src/registry/cache/tests.rs::a_recreate_loser_never_publishes_the_incarnation_it_lost_to
  - src/registry/cache/tests.rs::a_write_to_another_stream_leaves_a_read_free_to_publish
  - src/registry/cache/tests.rs::fill_publishes_only_against_the_generation_it_read
  - src/registry/cache/tests.rs::a_cap_purge_never_erases_the_invalidation_a_read_in_flight_must_see
  - src/registry/cache/tests.rs::the_cap_purges_only_for_a_new_key_at_the_cap
  - src/registry/cache/tests.rs::the_cap_evicts_stale_slots_first_then_the_least_recently_touched
  - src/registry/cache/tests.rs::a_cold_miss_with_no_write_in_flight_publishes
  - src/registry/cache/tests.rs::a_read_in_flight_keeps_its_slot_through_a_cap_purge
  - src/registry/cache/tests.rs::an_invalidation_of_an_unknown_stream_is_a_no_op
  - src/registry/cache/tests.rs::a_recreate_loser_on_another_instances_win_reads_the_live_incarnation_next
  - src/registry/cache/tests.rs::the_ttl_window_ends_exactly_at_the_ttl
- **Risk reason:** No contract change. It only shortens a transient stale-answer window on the writing process, and nothing that succeeded before now fails. The cost is a few extra registry GETs, one of them per lost recreate.
- **Check against commit:** None against the recorded text. The commit itself records one residual race as out of scope: create's own post-PUT insert can still race a full GET+PUT+invalidate. Staleness across instances (5 s TTL) is unchanged.

### #13 4247421f — Shared raw SSE session gets upToDate after a match-free batch

- **Program item:** rank 13
- **Surface:** raw
- **Endpoint:** GET /v1/stream/{name}?live=sse (raw SSE; the raw route forces key="" so it always rides the default-lane feed, which it can share with other raw sessions or product GET /v1/streams/{name}/records:sse sessions)
- **Condition:** A raw SSE session whose default-lane feed has >=2 subscribers takes a retained batch (Take::Batch arm in src/sse/session.rs serve) that ends at the durable frontier (head_here) but carries no record at/after this session's cursor: a match-free window (e.g. a product append under another routing key that the default lane scans past) or a cursor already past every record of the batch (start_index == records.len()).
- **Before:** The shared arm cleared the RAW status on head_here alone: need_status=false, last_reported=Some(scan_to), reached_live=true, although no frame was sent. The raw client got NO control frame for the window; its last streamNextOffset stayed behind the scanned frontier until the next record. A solo session (Solo arms guard on last_off.is_some()) in the same situation DID get one standalone control, so wire behaviour depended on subscriber count.
- **After:** need_status stays true unless this session itself sent the at_head record (the clear now rides the paired control of the last record the session sends). At the head the session emits exactly ONE standalone `event: control` with data {"streamNextOffset":"\<token past scan_to>","streamCursor":...,"upToDate":true}, identical to the solo case. After a shared RECORD at the frontier, raw still gets ONE paired control carrying upToDate and no standalone duplicate. Product framing (bare control + standalone status) is unchanged; no status code, header or error change.
- **Retry semantics:** None. Additive control frame on the success path; no error or reconnect behaviour changes.
- **Who is affected:** Raw SSE clients (and SDKs consuming the raw protocol) on streams where the default-lane feed is shared, mainly streams that also receive keyed product appends. They now see an upToDate control with an advanced streamNextOffset where they previously saw nothing.
- **Pinning tests:**
  - src/dst/tests/sse_delivery.rs::shared_raw_subscriber_gets_up_to_date_after_a_match_free_batch
- **Risk reason:** Success-path change only. It adds one standalone upToDate control that solo sessions already received and that the pinned protocol already permits. No status, code, header or retry change. Clients waiting for upToDate are unblocked, and none lose a frame they relied on.
- **Check against commit:** None against the recorded text. docs/refactor/WIRE-MATRIX.md now describes the rule as 'SSE (both surfaces)', but the code change affects only the RawToken surface; product sessions already had need_status=true on this path.

### #15 ac9ab99e — Catch-up through a reopened sealed span is served

- **Program item:** rank 16
- **Surface:** product
- **Endpoint:** GET /v1/streams/{name}/records:sse (product SSE served by a LineageSource, i.e. a split stream). Raw ?live=sse cannot ride a lineage: the raw route is keyless, and session.rs answers a segmented lineage with an immediate empty EOF, then 400 keyless_live on reconnect.
- **Condition:** A split stream whose sealed predecessor span is owned by this instance. A session on the feed has already served a sealed-span page locally (which filled the old (engine, handle) cache). The sealed span's engine then closes and reopens under the SAME owner (storage-fault close, ownership bounce that returns, rig/shutdown retirement). A session on the same feed then catches up through that span.
- **Before:** The cached closed engine was reused for every page, and the cache was cleared only when ownership moved away. Each page failed through the closed flag and the session retried every 100 ms forever. On the wire: HTTP 200 text/event-stream with keep-alives only, no records, no EOF (test red: 15 s of keep-alives). FEED_SOURCE_FAILED rose \~10/s per session. A tokio Mutex held across resolve+read also serialised every session's catch-up through that span.
- **After:** Every local sealed page resolves the directory's resident engine (Adoption::External) and its handle, so the reopened engine serves: the records and then the upToDate control arrive (test: r:0, r:1, r:2 in 0.08 s), no EOF. If the engine is in holdoff or not yet open, the page fails with 'sealed span engine unavailable' and retries at 100 ms until it opens, which is the same as the first-page behaviour before. Catch-ups are no longer serialised by a per-span lock. No status, code or header change.
- **Retry semantics:** None on the wire. The server-side 100 ms catch-up retry now terminates when the engine reopens instead of looping forever.
- **Who is affected:** Product SSE subscribers (SDK) on split streams catching up through a sealed span after a same-owner engine close/reopen. Previously they hung with keep-alives; now they receive data.
- **Pinning tests:**
  - src/dst/tests/livefeed_ownership.rs::livefeed_reopened_sealed_span_serves_catch_up
- **Risk reason:** The case could not succeed before (a silent infinite hang). The fix only makes it succeed. No status, code, header or retry change; removing the mutex is a performance side effect.
- **Check against commit:** None. The recorded text does not name the surface; it is product-only in practice (raw keyless sessions are refused on segmented lineages).

### #17 eb52b970 — Billing-sweep close arms only the 3 s base holdoff

- **Program item:** rank 12
- **Surface:** both
- **Endpoint:** Every raw and product request resolving a cold shard prefix that the billing sweep (billing.rs sweep, RetirementReason::SweepEviction) has just closed: raw /v1/stream/{name} (append/read/SSE connect), product /v1/streams/{name} (create/append/read/:sse/consumers).
- **Condition:** A customer's request arrives for a prefix inside the anti-flap holdoff armed when the billing sweep released its probe engine. Secondary: a RetirementReason::Shutdown retirement, and a sweep release of an engine that lived >= SHORT_LIVED (30 s).
- **Before:** retire_resident ignored the reason, and arm_holdoff_locked treated a sweep close like an engine that died young. Each sweep cycle added a strike, and strikes never decayed because probe engines never outlive 30 s. The holdoff escalated per cycle: 6 s, 12 s, 24 s, 48 s, then 60 s cap. The request got 503 {"error":{"code":"shard_moving"}} with Retry-After up to 60 (product append: 503 temporarily_unavailable retryable:true, same Retry-After; product read: 503 temporarily_unavailable, no Retry-After). Shutdown retirements of young engines also struck. A sweep of a long-lived engine reset strikes to 0.
- **After:** ledger_after(Departure::Retired(SweepEviction|Shutdown)) arms the 3 s HOLDOFF_BASE and leaves the strike count untouched (neither grown nor reset). The same 503 shard_moving (product: temporarily_unavailable retryable:true) is returned, but Retry-After is \<= 3 (remaining whole seconds, min 1) and the 503 window lasts at most 3 s. Died, OwnershipMoved and FleetEviction departures still strike and escalate (3 s base up to 60 s) exactly as before.
- **Retry semantics:** Same status, code and retryable flag. Only the Retry-After magnitude and the window length shrink (up to 60 s becomes \<= 3 s). Side effect: a sweep release no longer resets strikes, so a prefix carrying old strikes keeps them for its next evidence departure.
- **Who is affected:** SDK and raw clients whose first request lands on a cold shard just after a sweep close; they wait \<= 3 s instead of up to 60 s. Fleet/operators: shutdown retirements are no longer strikes.
- **Pinning tests:**
  - src/dst/tests/runtime_retirement.rs::a_sweep_eviction_arms_only_the_base_holdoff
  - src/sharddir/holdoff.rs::tests::a_sweep_eviction_or_shutdown_leaves_the_strike_ledger_alone
  - src/sharddir/holdoff.rs::tests::a_young_engine_that_dies_or_is_moved_away_still_strikes
  - src/sharddir/holdoff.rs::tests::the_ledger_judges_evidence_by_lifetime_and_ignores_releases
- **Risk reason:** No status, code or retryable-flag change. Only the Retry-After value (shorter) and the refusal window (shorter) change, which reduces client-visible 503s. Evidence departures (death, move, fleet eviction) keep the escalating holdoff, which the contrast tests pin.
- **Check against commit:** None against the recorded text. Not in the recorded text but in the commit: Shutdown retirements also stop striking, and a sweep release of a long-lived engine no longer resets strikes to 0 (the ledger is left untouched). Before, the escalated Retry-After started at 6 s on the first cycle (holdoff_for(1)), not 3 s.

### #18 4bb51c0d — Unreadable routers/\*.json defers only the desired publication

- **Program item:** item 11
- **Surface:** fleet-internal
- **Endpoint:** Fleet controller tick (src/fleet.rs start). Input: object-store routers/\*.json via FleetRepository::read_router_reports. Outputs: ring view (set_view), peer table (set_peers), yield of moved shards (retire OwnershipMoved), eager open, return-home CAS, rebalancer fleet/overrides.json, and the fleet desired document (scale decision). No HTTP endpoint.
- **Condition:** read_router_reports returns Err: a routers/\*.json that is not JSON, exceeds the 128 KiB/8 MiB population caps, or does not finish within the 10 s population timeout.
- **Before:** The tick hit `continue` right after the router read, before set_view/set_peers, yield, eager open, return-home, the rebalancer and the desired CAS. Every 2 s tick was abandoned cell-wide for as long as the file persisted. The ring and peer table went stale, moved-away shards were never yielded, and return-home never ran. The log line said 'router snapshot deferred; scale decision deferred'.
- **After:** The read error is only logged ('router snapshot unreadable; desired publication deferred'), and edge_p50 = fresh_edge_p50([]) = 0 for that tick. Every other tick step runs, and only the desired publication is skipped: (publish=false), including bootstrap publication when no desired document exists yet (bootstrap was also deferred before). The latency and CPU sustain timers keep updating through the outage, and below_since is left untouched. A slow router GET still delays the tick by up to the 10 s population timeout, but it no longer skips the tick. Once the file becomes readable, the next tick publishes normally. No HTTP wire change.
- **Retry semantics:** None on any client surface. Indirect effect: routing and ownership stay current during a router-report outage, so clients see correct 409 not_ring_owner/Streams-Replay-To answers and yields instead of stale ownership.
- **Who is affected:** Fleet peers and operators: ownership publication, yields, return-home and rebalancing continue through a bad router report, and scaling decisions pause. Clients benefit only indirectly.
- **Pinning tests:**
  - src/dst/tests/fleet_controller.rs::an_unreadable_router_report_defers_only_the_desired_publication
  - src/fleet/tests.rs::edge_p50_is_the_worst_fresh_router_report
- **Risk reason:** Internal fleet control-plane only. No HTTP status, code or header changes; the only operator-visible text change is the log message. It removes a cell-wide freeze of ownership publication.
- **Check against commit:** None. The recorded 'no wire change' holds; the warn log text changed.

### #21 8dabca7f — Typed create-vs-relay on the telemetry-append receiver

- **Program item:** rank 21
- **Surface:** fleet-internal
- **Endpoint:** POST /v1/internal/telemetry-append/{\*name} (src/http.rs::internal_telemetry_append -> src/billing/system_append.rs::append_local). Sender side: billing::system_append / relay (no HTTP surface of its own).
- **Condition:** (a) The receiver is not the ring owner of the reserved stream's shard, and the stream has no registry descriptor yet (unprimed). (b) The name is `_`-prefixed but not canonical (`__ds`, empty or `//` components, control characters, too long). (c) The Stream-Encryption-Key header is missing (and there is no default key) or cannot be parsed.
- **Before:** The receiver ran create_stream first, then an append. (a) The create was refused with 409 not_ring_owner, which the receiver took to mean "exists"; the following append found no descriptor and answered 404 {"error":{"code":"not_found","message":"stream not found"}} with no Streams-Replay-To. A primed non-owner already answered 409 not_ring_owner + Streams-Replay-To, through the append. (b) `__ds` got 400 `reserved` from create_stream. Other non-canonical names panicked in ProjectId::stream_ref (expect), so the connection task died without an HTTP response. (c) 400 missing_key / 400 invalid_key from create_stream. Sender: an unprimed non-owner's first system append failed locally with "system append X: 404 Not Found" and never relayed.
- **After:** The receiver runs raw_key, then CanonicalStreamName::new, then the typed append_local: append first, create under the SYSTEM project only on AppendCode::NotFound, then append again. (a) 409 {"error":{"code":"not_ring_owner","message":"shard \<p> belongs to \<owner>"}} + Streams-Replay-To: \<owner instance id>, rendered by creation_error_response. (b) 400 invalid_name "not a canonical stream name" for `__ds` and every other non-canonical name. (c) 400 missing_key "Stream-Encryption-Key required" / 400 invalid_key \<parse message>: same codes as before. An owner still answers the raw append contract (200/204 + Stream-Next-Offset etc.). All of these use the err_resp envelope, which has no retryable field. Sender: it relays once, only on an owner-bearing typed refusal; other failures are reported as "system append X: \<typed code: message>". A refused relay is reported as "telemetry relay X: \<status>" and never counted as landed.
- **Retry semantics:** Receiver: 404 -> 409 + Streams-Replay-To for the unprimed non-owner. The sender still follows only one hop and treats every non-2xx relay answer as a failed batch, so the new header is informational and neither side retries differently. Case (b) goes from no response (connection reset) to a permanent 400. Sender: an unprimed non-owner now relays and lands its first batch, where before it failed until the owner created the stream itself.
- **Who is affected:** Fleet peers only: system-stream producers for the usage, ops-metrics/events and audit ledgers. Operators see error strings that name the typed code instead of a bare status. No product or raw client can call this route, which needs the fleet credential or a workload JWT with the telemetry-append op claim.
- **Pinning tests:**
  - src/dst/tests/security_workload.rs::unprimed_system_append_relays_its_first_batch_to_the_owner
  - src/dst/tests/security_workload.rs::telemetry_append_receiver_reports_ownership_not_absence
  - src/dst/tests/security_workload.rs::system_append_without_a_peer_reports_the_typed_refusal
  - src/dst/tests/security_workload.rs::a_rotated_system_key_is_the_appends_refusal_never_a_recreate
  - src/dst/tests/security_workload.rs::a_relay_the_skewed_owner_refuses_is_reported_not_counted_as_landed
- **Risk reason:** Fleet-internal only. Every changed case already failed before: 404, a panic, or 400. The one success change is that an unprimed non-owner now lands its telemetry, which is a fix. Mixed-version rolling deploys are safe because old and new senders both treat any non-2xx relay answer as a failed batch. The missing_key path is not exercised by the new tests. The receiver test covers `__ds` -> invalid_name and a garbage key -> invalid_key.
- **Check against commit:** No mismatch in substance; three precision points. (1) The 404 -> 409 change applies only to an UNPRIMED non-owner; a primed non-owner already answered 409 not_ring_owner + Streams-Replay-To. (2) missing_key and invalid_key are not new: create_stream already answered them with the same codes and messages. (3) invalid_name replaces `reserved` for `__ds`; for other non-canonical names it replaces a handler panic (no HTTP response).

### #22 06fb8d51 — Ops checkpoint read failure and missing page position fail the step

- **Program item:** item 49
- **Surface:** operator-debug
- **Endpoint:** None on the wire. It changes the internal usage-rollup supervisor loop (billing::rollup_step / ops_rollup_step) and billing::system_read, which reads the \_usage and \_ops_metrics system streams through a local read_inner call or a relay GET /v1/internal/segment-read/{name}. system_read has no production caller other than these two steps, and the ops minute tier (ops_m1) is read only by tests.
- **Condition:** (a) Reading the \_ops_metrics checkpoint (meta/ops-cursor) fails transiently, or the stored checkpoint is not UTF-8. (b) A 2xx system read page, local or relayed, has no stream-next-offset or an empty one.
- **Before:** (a) ops_cursor turned the error into None (db.get(..).ok().flatten(), decoded with .ok()), so the step re-read \_ops_metrics from the start of the ledger. That merged every snapshot into the minute tier a second time (samples += 1 is not idempotent) and rewound the checkpoint, silently. (b) The missing header defaulted to "", which the step would have checkpointed, replaying from the start of the ledger on the next step.
- **After:** (a) ops_cursor returns anyhow::Result\<Option\<String>> through the shared read_text repository read. An Err fails ops_rollup_step, and the supervisor logs WARN "ops rollup step: \<e>", counts 0 and tries again on the next \~2 s tick from the durable checkpoint; no rows or checkpoint move. A non-UTF-8 checkpoint now fails every tick until an operator fixes it, and the usage step keeps running on its own. (b) page_position returns Err("system read \<ledger>: page without stream-next-offset"), so the step fails and retries without checkpointing. A durable "" checkpoint written by an older build is still read as the start of the ledger. Two unused header maps are removed.
- **Retry semantics:** Internal only. A silent restart from the ledger start (double-merge) becomes a failed step that is retried from the durable checkpoint. A persistently corrupt checkpoint, or a peer that answers 2xx without stream-next-offset, now stalls that ledger's rollup with a WARN on every tick instead of corrupting it.
- **Who is affected:** Operators: new WARN log lines, and an ops minute tier that can no longer double-count. It could now stall visibly on a corrupt checkpoint. Billing and usage rollup owners: a 2xx page without a position no longer rewinds the usage ledger. No clients or fleet wire contracts change.
- **Pinning tests:**
  - src/dst/tests/billing_controller.rs::an_unreadable_ops_checkpoint_fails_the_step_instead_of_restarting_the_ledger
  - src/rollup/accounting_failure_tests.rs::an_ops_checkpoint_read_failure_is_an_error_never_the_start_of_the_ledger
  - src/billing/tests.rs::a_system_page_without_a_position_is_refused_never_checkpointed_as_empty
- **Risk reason:** There is no wire change, and system_read has no production caller other than the two rollup steps. It swaps silent ledger corruption for a visible, retried failure. The residual liveness risk (a corrupt checkpoint, or a peer answering 2xx without stream-next-offset, stalls that one ledger's rollup until an operator steps in) is operator-visible through the WARN logs.
- **Check against commit:** matches; no notes.

### #23 5c0e62d6 — Internal receivers distinguish registry fault from absence

- **Program item:** item 30
- **Surface:** fleet-internal
- **Endpoint:** POST /v1/internal/sweep-segment/{\*name}, GET /v1/internal/queue-cursor/{\*name}, GET /v1/internal/segment-scan/{\*name} (shared prelude src/product/internal.rs::internal_desc)
- **Condition:** The receiver's registry.get(sref) returns Err, a store or read fault. A positive Ok(None) (no descriptor) is unchanged.
- **Before:** `_ =>` collapsed Ok(None) and Err into 404 {"error":{"code":"not_found","message":"stream","retryable":false}}. The segment-scan sender (read_remote::scan_page_once) maps 404/410 to TargetGone. The sealed-span SSE source (sse/source.rs) turned that into FatalSpanCutoff(IncarnationChanged), which ended the live feed for every subscriber of the span (product and raw SSE) even though the stream still existed.
- **After:** A registry Err answers 503 {"error":{"code":"temporarily_unavailable","message":\<registry error text>,"retryable":true}}. Only Ok(None) answers 404 not_found (retryable:false). There is no liveness gating; verify_internal_target still binds the incarnation afterwards. Sender effects: segment-scan maps 503 to RemoteSpanError::Retryable, so the SSE source parks and retries instead of cutting the feed off. Product :scan relays already rendered both TargetGone and Retryable as 503 temporarily_unavailable, so they are unchanged. The queue-cursor sender (relay_queue_cursor, any non-2xx -> None) and the sweep-segment sender (any non-2xx -> segment_unavailable) do not distinguish the two, so their behaviour is unchanged.
- **Retry semantics:** 404 non-retryable -> 503 retryable, only for a registry read fault. For the sealed-span SSE sender, a terminal feed cutoff becomes a bounded retry. No change for the other two senders or for product :scan.
- **Who is affected:** Fleet peers that relay segment-scan, queue-cursor or sweep-segment. Indirectly, product and raw SSE subscribers of split streams whose sealed span is owned by another instance: they are no longer disconnected by a transient registry fault on that owner. The 503 message includes the registry error string, which is visible to fleet peers only.
- **Pinning tests:**
  - src/dst/tests/read_peer_compatibility.rs::o2c_registry_store_error_on_a_receiver_is_retryable_not_gone
- **Risk reason:** Fleet-internal. The change only moves a failure from terminal to retryable, and senders already mapped 503 to retryable (checked in the parent commit's read_remote.rs), so mixed-version deploys are safe. The test pins the wire answer from all three receivers, then a 200 after the one-shot fault, then that an absent name is still 404. It does not pin end to end that an SSE subscriber survives the blip; that follows from the sender mapping in sse/source.rs.
- **Check against commit:** No mismatch. Precision: of the three senders, only the segment-scan sender (sealed-span SSE source) behaves differently; the queue-cursor and sweep-segment senders treat every non-2xx answer the same way.

### #25 ee99975d — Graceful stop seals and drains the active read window

- **Program item:** item 26
- **Surface:** process
- **Endpoint:** No HTTP route. SIGTERM → TaskSupervisor.shutdown (10 s grace, src/bootstrap.rs:899) cancels the telemetry-drain task (src/billing/telemetry_loop.rs), which writes the durable read spool and the \_usage system ledger. The task is spawned only when USAGE_STREAM_KEY is set.
- **Condition:** A graceful stop while the active read window holds metered read deltas younger than READ_FLUSH_INTERVAL_MS (10 s), or while sealed batches are waiting that the 2 s cadence had not reached yet. The stop can land between ticks or in the middle of a drain round.
- **Before:** Both cancel arms returned TaskResult::Done at once. Nothing sealed the active window: seal_if_aged(0) had no production caller. Sealed batches the drain had not yet reached were never spooled. Each graceful stop could lose up to 10 s of read usage, a billing undercount. The stop did not wait for any drain.
- **After:** On cancel, both arms break and terminal_round runs: seal_aged_reads(0), then one drain_once (spool first, then ledger) under tokio::time::timeout(TELEMETRY_DRAIN_SECS, default 2 s). Outcomes: Ok calls drain_succeeded; Err logs warn 'usage drain at shutdown: …'; a timeout logs warn 'usage drain at shutdown did not finish inside …; the spool keeps what the store accepted'. When a round is cut, the ReadDrain guard requeues its batch. The task returns Done/Finished. A graceful stop can now take up to one extra cadence. No HTTP status, header or body changes. The caveats hold in code: (a) meter_read after the seal goes into a new active window that nothing drains, so it is lost at exit; (b) seal_locked will not seal while sealed.len() >= READ_SEALED_MAX_BATCHES (64), so the active window stays the hard-loss window; (c) one round spools and emits at most 64 batches, and the rest stay in the durable spool.
- **Retry semantics:** None on the wire. Internally, a terminal round cut by the timeout leaves its batch in the spool or the requeue, the same custody as an interrupted ordinary round.
- **Who is affected:** Billing and invoice consumers of \_usage: read usage from the final window is now recorded, so totals are slightly higher and correct. Operators: shutdown can take up to TELEMETRY_DRAIN_SECS longer, and there are two new shutdown warn lines. SDK, raw clients and fleet peers are not affected.
- **Pinning tests:**
  - src/dst/tests/billing_controller.rs::graceful_stop_seals_and_drains_the_active_read_window
  - src/dst/tests/billing_controller.rs::r09_active_telemetry_cancels_entered_storage_and_preserves_debt (modified: grace 300 ms -> 5 s; asserts the stop took at least one cadence and was not aborted)
- **Risk reason:** Internal billing and shutdown behaviour only. No wire change. The stop now delivers usage it used to drop, and it stays bounded by one cadence, so a wedged store still stops cooperatively (R09).
- **Check against commit:** The recorded text matches, with two nuances. (1) The caveat 'sealed queue full (a long ledger outage)': when the read spool is open (always the case in BILLING_MODE=required), drain_once moves sealed batches into the spool every round whatever the ledger's health. So the queue reaches 64 only during a sustained spool/store write failure, or during a ledger outage on a deployment without a spool. (2) 'TELEMETRY_DRAIN_SECS below the 10 s grace' is documented but not validated. A value of 10 or more lets the supervisor abort the task, which then reports aborted rather than Finished, and the requeued batch is lost at exit.

### #27 12ad5134 — Refused fleet event append is reported

- **Program item:** item 57
- **Surface:** fleet-internal
- **Endpoint:** No HTTP change. The telemetry-drain loop calls fleet::drain_fleet_events → outbox::drain_events → billing::system_append to the \_ops_events system stream: a local append, or one relay hop to POST /v1/internal/telemetry-append/\_ops_events on the ring owner. Source: pending_events in the fleet CAS documents Desired and Overrides.
- **Condition:** The \_ops_events append fails (local refusal with no reachable owner, or a failed relay), or serde encoding of the stamped events or of the cleared document fails.
- **Before:** The error was swallowed with `continue`. drain_events returned Ok(n), counting only successes (Ok(0) when everything failed), so the 'fleet event drain' warn could never fire. The Overrides document was still tried in the same tick. The outbox was retained. A failed clear encode used unwrap_or_default(), which would have CAS-written an empty document.
- **After:** drain_events returns Err('fleet event append: …' / 'fleet event encode: …' / 'fleet outbox clear encode: …'), and the telemetry loop logs warn 'fleet event drain: …' on every failing tick (2 s cadence). The CAS outbox is unchanged, and the next tick re-emits the same deterministic ids, which the rollup deduplicates. The clear re-decode propagates with ?. The best-effort CAS clear (`let _ =`) is unchanged. The function returns at the first failing document, so when the Desired append fails, the Overrides document is not attempted in that tick.
- **Retry semantics:** None for clients. The internal retry is unchanged (next tick, same ids).
- **Who is affected:** Operators, who now see a warn line while the fleet outbox is stuck. Consumers of \_ops_events see no change in content, except that a failing Desired document now blocks Overrides events.
- **Pinning tests:**
  - src/fleet/outbox.rs::tests::a_refused_fleet_append_is_reported_and_keeps_the_outbox
  - src/fleet/outbox.rs::tests::an_overrides_outbox_clears_exactly_what_it_appended
  - src/fleet/outbox.rs::tests::cancelled_fleet_clear_follows_append_and_retains_retry_source (pre-existing)
- **Risk reason:** Internal telemetry and observability only. No wire, status or client change. The events are retained and retried as before.
- **Check against commit:** The recorded text matches. Unrecorded nuance: before, a failure on the Desired document fell through to Overrides in the same tick; now the first error returns. If the Desired append fails deterministically (e.g. a poison or oversized body), Overrides events are no longer drained at all, where before they still were. The warn log itself is not asserted; the test pins the Err return.

### #29 e7c7ad87 — Product create that loses the recreate CAS to a live forked incarnation is idempotent

- **Program item:** item 9
- **Surface:** product
- **Endpoint:** PUT /v1/streams/{name} (application/creation/product.rs::create_product)
- **Condition:** The snapshot (cached) shows the name dead (tombstone or expired), but the stored descriptor is a LIVE incarnation with fork_children, because another instance recreated and forked it. The recreate CAS declines.
- **Before:** 409 {"error":{"code":"gone","message":"name is retained for live forks","retryable":false}}. The declined winner was classified by `soft_deleted || !fork_children.is_empty()`, which ignores liveness.
- **After:** validate_live(winner) decides. 200 (idempotent create, existing collection) when content type, ttl, expiresAt (without ttl), watches and key all match. Otherwise 409 config_mismatch 'stream exists with different immutable configuration' or 403 wrong_key 'encryption key mismatch' (retryable:false). Retained winners (soft-deleted, or expired with children) still get 409 gone with the same message. The stored epoch and children are untouched. The quota reservation taken for the dead snapshot is not committed on the idempotent path.
- **Retry semantics:** A permanent 409 gone becomes a success (200), or a different permanent error that the race-free path already answers (409 config_mismatch or 403 wrong_key). Nothing moves toward more failure.
- **Who is affected:** Product SDK users who create or re-PUT a collection while an instance's cache is stale after a cross-instance recreate and fork.
- **Pinning tests:**
  - src/dst/tests/product_lifecycle.rs::a_product_create_that_loses_to_a_live_forked_incarnation_is_idempotent
  - src/dst/tests/fork_lifecycle.rs::fork_lifecycle_is_idempotent_and_epoch_checked (product PUT on a soft-deleted source still 409)
  - src/application/creation.rs::tests::a_name_is_live_retained_or_recreatable_at_one_instant
- **Risk reason:** A request that failed permanently now succeeds, or gets the answer the race-free path gives. Race-window only. No new code or status.
- **Check against commit:** Matches. Unrecorded nuance: the CAS predicate changed from `!desc_alive && !soft_deleted && fork_children.is_empty()` to recreatable(). The two also differ for a stored descriptor that is `deleted && !soft_deleted` with non-empty fork_children: before, the CAS declined and answered 409 gone; now it recreates with 201, as the raw path already did. The lifecycle never writes that state (tombstones are written only for childless names, or when the last child is released), so the commit's claim 'same verdict on every reachable state' holds. The race-window config_mismatch and wrong_key branches have no new test.

### #31 7c4f8606 — Failed or empty live SSE read is retried on a backoff

- **Program item:** item 33
- **Surface:** both
- **Endpoint:** SSE sessions: GET /v1/streams/{name}?live=sse (product, SseSurface::Product) and GET /v1/stream/{name}?live=sse (raw, SseSurface::Raw), both served by sse::session::serve. Indirectly, the fleet-internal peer read GET /v1/internal/segment-scan/{name} (the sealed-span remote page, read_remote::scan_page_once) is re-issued on the new schedule.
- **Condition:** The session's own live-phase drive (it holds the feed's drive permit) returns DriveOutcome::SourceFailed (the source read errored, including a sealed-span peer answering 429/503 → RemoteSpanError::Retryable, a transport error, or indeterminate ownership) or DriveOutcome::NoProgress (an empty partial page). Nothing else then re-drives the session: no append, no version bump, and the source's notify has already fired.
- **Before:** The session parked in tokio::select! on ver_wait/gen_wait/src_wait/cancel and sleep(lease_watch.nap()) alone. The nap is 3600 s for an unleased session (SseLease::None, e.g. keyless raw) and otherwise the lease deadline clamped to 1..=3600 s. The durable record behind the failed read arrived only at the next append's notify or when the nap ended. Exception: when the feed's transition retry task was running for a closed tail, its 250 ms tick (at most 4,800 ticks) already bumped the version. The code logged tracing::warn!("livefeed source read failed; parking until next wake") on SourceFailed. Frames, control events, cursors and status codes are as after.
- **After:** ReadRetry::failed() marks one owed park. That park sleeps min(delay, lease nap). delay starts at 250 ms, doubles while failures repeat at the same cursor (250, 500, 1000, 2000, 4000, then 5000 ms), is capped at 5 s, and resets to 250 ms once the cursor advances. The owed retry covers one park only. An idle or contended session (DriveOutcome::Idle or None) still parks on the version watch and the lease nap alone. A sealed-span peer's 429/503 is re-read on this schedule, and its Retry-After header is never parsed (read_remote maps 429/503 to Retryable{status,code} only). The warn log becomes one tracing::debug! line per owed retry. The FEED_SOURCE_FAILED and FEED_NO_PROGRESS counters are unchanged. SSE frames, event names, cursors, upToDate/sealed flags, ordering, status codes and headers are all unchanged.
- **Retry semantics:** None on the wire: no status, code or retryable flag changes. The server's own internal re-read cadence changes from next-append-or-lease-nap to a per-session backoff (250 ms doubling to 5 s). A peer's 429/503 Retry-After is not honoured, so a shedding peer can be re-asked sooner than it asked for: at most one read per failing session per 5 s once backed off, and the first retries come at 250/500/1000 ms.
- **Who is affected:** SSE subscribers on both surfaces (SDK and raw clients) whose live read failed or was empty now get the record within about 250 ms to 5 s instead of waiting for the next append or up to an hour. Fleet peers that own sealed spans get re-reads from subscribing instances on the backoff, regardless of their Retry-After. Operators lose the warn log line (now debug).
- **Pinning tests:**
  - a_failed_live_read_is_retried_without_another_append (src/dst/tests/sse_delivery.rs)
  - an_empty_live_read_is_retried_without_another_append (src/dst/tests/sse_delivery.rs)
  - a_failed_live_read_owes_exactly_one_short_park (src/sse/session/tests.rs)
  - failures_at_one_cursor_double_to_the_cap (src/sse/session/tests.rs)
  - a_failure_after_progress_waits_the_first_delay_again (src/sse/session/tests.rs)
- **Risk reason:** Timing only. No status, code, header or frame change, and records that previously stalled now arrive sooner. The residual concern is extra fleet-internal load: Retry-After from a 429/503 peer is ignored, bounded by the 5 s cap and the per-feed drive permit.
- **Check against commit:** No mismatch with the recorded text. Nuance: 'instead of the lease nap' is exact for failures on an open source and on sealed spans behind a live tail. For a closed tail while the feed's transition retry task was running, the feed's 250 ms tick (drive.rs, at most 4,800 ticks) already re-woke parked sessions before this change. Also operator-visible: the SourceFailed warn log became debug.

### #32 8f590ec5 — SSE_H1_MAX_BUF below 8,192 is refused at validation

- **Program item:** item 53
- **Surface:** process
- **Endpoint:** Process startup: ServerConfig::validate → validate_topology_and_ceilings (src/config/validation.rs), with main.rs printing the error and exiting. Environment knob SSE_H1_MAX_BUF.
- **Condition:** SSE_H1_MAX_BUF parses to a value below 8,192 (HttpConfig::MIN_H1_MAX_BUF = 8 \* 1024, the same as hyper's private MINIMUM_MAX_BUFFER_SIZE).
- **Before:** validate() accepted the value. bootstrap::run then opened engines, spawned supervised loops, set up telemetry and rollup and built the router. Only after that did serve_h1 → h1_builder call hyper's http1::Builder::max_buf_size, which asserts ('The max_buf_size cannot be smaller than 8192.'). The panic unwound through block_on in main, so the process died with Rust's default panic exit status 101 (there is no panic hook or panic=abort in the release profile) without serving a request, after bootstrap's side effects.
- **After:** validate() collects the error 'SSE_H1_MAX_BUF=\<n> is below hyper's 8192-byte h1 buffer floor' alongside every other problem. main prints 'Error: configuration invalid (N problem(s)):\\n  - …' to stderr and calls std::process::exit(1) before any store open, spawn or bind side effect. Values of 8,192 and above behave exactly as before, and there is still no upper limit.
- **Retry semantics:** None (no request path). A crash-loop exit code changes from 101 (panic) to 1 (configuration error), and it now happens before side effects.
- **Who is affected:** Operators and deploy tooling that set SSE_H1_MAX_BUF below 8 KiB. That setting could never serve before. No client-visible effect.
- **Pinning tests:**
  - validation_rejects_an_h1_buffer_below_hypers_floor (src/config/validation_tests.rs, mod validate_boundary_tests)
  - the_validated_buffer_floor_is_the_one_hyper_asserts (src/http/serve.rs tests)
- **Risk reason:** Process-only, and the affected configurations could not succeed before: they always panicked at serve start. Every working value is unchanged, and the only in-tree setter (bench/sse-probes/sse-1per.sh) defaults to 65,536.
- **Check against commit:** None. The before-state exit code (101) is inferred from Rust's default panic behaviour: serve_h1 is awaited directly in bootstrap::run under main's block_on, and no panic hook or panic=abort is configured.

### #33 d148e1a4 — Stream-Seq over 65,535 bytes no longer corrupts the tail row

- **Program item:** item 53
- **Surface:** raw
- **Endpoint:** POST /v1/stream/{name} (raw append) with a Stream-Seq header. Only the raw path sets AppendCommand.sequence (http.rs: hdr("stream-seq")), and product appends pass sequence: None. The effect showed later on every raw/product read, append or fork of the stream that loads the tail row (stream_handle / tail_fields / seed_fork_tail), and on maintenance ledger rebuild at shard open.
- **Condition:** An append whose Stream-Seq value is longer than 65,535 bytes (u16::MAX) is committed. Reaching this over HTTP needs a request head larger than the h1 read buffer, which by default is SSE_H1_MAX_BUF = 64 KiB. hyper typically refuses such a head as too large before any handler runs, so the path is realistic mainly when SSE_H1_MAX_BUF is raised.
- **Before:** The append itself was acknowledged (200/204 with Stream-Next-Offset), and the lane's 's' row stored the full sequence. encode_tail wrote `seq.len() as u16` (wrapped), followed by the whole text. Once the in-memory handle was evicted or the shard reopened, decode_tail read route, trim_safe_to and unabsorbed_bytes from inside the text, and stored_tail refused the row ('Data error: inconsistent persisted tail'). stream_handle, tail_fields and seed_fork_tail then failed for that stream permanently: raw reads answered 500 internal, and appends and forks also failed. A maintenance ledger rebuild that met the row failed the whole shard open, taking every stream on that shard down.
- **After:** A sequence of 65,536 bytes or more is written to the tail row with seq_len 0 and no copy. The lane 's' row still holds it whole, and load_seq_chain, the only reader of Stream-Seq for enforcement, is unchanged. The tail row decodes, and the stream stays readable and appendable. Rows with a sequence of 65,535 bytes or fewer are byte-identical to before (goldens unchanged). No status, code, header or body changes on the wire.
- **Retry semantics:** None on the wire. A permanent wedge (500 internal on every later load, possibly a shard-wide open failure) is removed. The triggering append was already accepted before and still is.
- **Who is affected:** Raw-API clients sending very large Stream-Seq values, only when the server's h1 buffer admits such a head. Also every other stream on the same shard, which a failed ledger rebuild would have blocked. SDK/product clients cannot set Stream-Seq.
- **Pinning tests:**
  - r53_a_stream_seq_past_the_tail_rows_u16_leaves_the_stream_openable (src/shard/transaction_tests.rs)
  - r53_the_tail_copies_a_stream_seq_only_while_its_length_fits (src/shard/storage_decode_tests.rs)
- **Risk reason:** The triggering case could not succeed before: the stream wedged permanently. The fix removes the wedge without any wire change, rows that fit stay byte-identical, and the path is barely reachable over HTTP with the default 64 KiB h1 buffer.
- **Check against commit:** No mismatch. Caveat on reachability: the commit says 'nothing bounds that header', but with the default SSE_H1_MAX_BUF = 65,536 hyper normally rejects a request head of that size before the append handler runs. The tests drive commit_group directly, not HTTP, so no test pins the HTTP path.

### #35 2fb92fb9 — Empty catch-up page is re-read after a bounded wait

- **Program item:** item 86
- **Surface:** both
- **Endpoint:** SSE sessions in their durable catch-up pass: GET /v1/streams/{name}?live=sse (product) and GET /v1/stream/{name}?live=sse (raw) via sse::session::serve → catch_up::stalled. Operator: GET /v1/debug/load sse_livefeed.no_progress and sse_livefeed.catchup_retries.
- **Condition:** A catch-up read below the catch-up bound returns an empty page (Ok(batch) with scan_to \<= cursor), i.e. a hole the durable read cannot explain yet. Typical case: a feed's first subscriber catching up from an old cursor below the ring's floor, since a feed is born with floor = head.
- **Before:** stalled() returned Stall::NoProgress immediately with no await. The live loop's take_visible returned Lagged (the cursor is below the floor), which incremented FEED_CATCHUP_RETRIES and re-entered catch-up at once. The session hot-spun on a worker thread: the red test saw 1,686,963 reads in 503 ms, about 3.3M/s. catchup_retries rose by about that rate, and no_progress did not count catch-up empty pages. Delivered frames were the same once the hole cleared.
- **After:** An empty catch-up page increments FEED_NO_PROGRESS, logs one debug line, sleeps 100 ms (the same RETRY constant a failed catch-up read already waited) and then hands over to the live loop as before. A session at or above the floor is served by the ring about 100 ms later. One below the floor re-enters catch-up at the current head at most once per wait, so catchup_retries counts a persistent hole at most about 10 times/s per session, and no_progress now includes catch-up empty pages. Frames, ordering (records delivered exactly once), codes and headers are unchanged.
- **Retry semantics:** None on the wire. The internal re-read cadence goes from an unbounded hot loop to one re-read per 100 ms, which can add up to 100 ms of latency per empty catch-up page before a session reaches live.
- **Who is affected:** SSE subscribers on both surfaces catching up across a durable hole: CPU is no longer burned, with at most about 100 ms added latency per empty page. Operators and dashboards reading /v1/debug/load sse_livefeed.no_progress (now also counts catch-up empty pages) and catchup_retries (falls from about millions/s to at most about 10/s per session).
- **Pinning tests:**
  - an_empty_catch_up_page_is_read_again_after_a_bounded_wait (src/dst/tests/sse_delivery.rs)
  - a_failed_catch_up_read_is_read_again_after_a_bounded_wait (src/dst/tests/sse_delivery.rs; shared helper catch_up_fault_is_retried)
  - a_stalled_catch_up_read_owes_its_pass_one_verdict (src/sse/session/tests.rs)
- **Risk reason:** Timing and operator-debug counter semantics only. No status, code, header or frame change, and the added wait of at most 100 ms replaces a CPU-burning spin.
- **Check against commit:** No mismatch. Nuances: (1) catch_up::stalled counts FEED_NO_PROGRESS for any empty catch-up page, including one with completed=true, whereas the live drive counts only !completed. The commit argues a completed-empty page cannot occur below a catch-up bound. (2) No test asserts the /v1/debug/load counter values. The DST tests pin the re-read rate (at most elapsed/100 ms + 2) and exactly-once delivery.

### #36 a1cf29f3 — shard_opens counters are reported per runtime

- **Program item:** item 67
- **Surface:** operator-debug
- **Endpoint:** GET /v1/debug/store (shard_opens object) and GET /operator/data.json (local.store.shard_opens). Also ShardDirectory::open_stats and store_timing::snapshot, which now takes the counters from the caller.
- **Condition:** Any read of shard_opens. The values differ only when more than one ShardDirectory/OpenGate runtime lives in one process, which happens in DST rigs, not in production, where bootstrap builds exactly one directory.
- **Before:** shard_opens {started, completed, failed, coalesced, in_flight, deadlined, reaped} was read from seven process-global statics shared by every OpenGate in the process, so each runtime reported the sum of all runtimes' opens. ?swap=1 reset only the outbound in-flight peak. reset_counters_for_tests existed only under cfg(test), so shard_opens were never reset in production.
- **After:** The same seven keys come from the serving runtime's own gate (OpenCounters, rendered by OpenGate::stats_json via ShardDirectory::open_stats) and are written once per event. /operator/data.json carries the identical object. ?swap=1 still resets only the outbound peak and never the opens. Production values are unchanged, since there is one directory per process. No status or header change.
- **Retry semantics:** None.
- **Who is affected:** Operators and dashboards reading the reopen-storm detector. Values are identical in production. Multi-runtime test processes now see per-runtime counts.
- **Pinning tests:**
  - debug_store_reports_this_runtimes_shard_opens (src/dst/tests/runtime_open_gate.rs)
  - a_hung_open_is_deadlined_and_its_late_engine_reaped (src/dst/tests/runtime_open_gate.rs)
  - open_gate_survives_impatient_clients_without_a_storm (src/dst/tests/runtime_open_gate.rs)
  - open_failure_is_typed (src/shard_directory.rs directory_tests)
  - a_panicking_opener_fails_its_open_and_the_next_attempt_installs (src/sharddir/unwind.rs tests)
- **Risk reason:** Operator/debug surface only, with the same key set and identical production values. No client-facing change.
- **Check against commit:** No mismatch. Note: '?swap=1 never resets them' is not a behaviour change; production never reset shard_opens on swap before either. The commit adds the first test that pins it.

### #37 6515a15d — Rebalancer targets only active-ring members; eager open follows effective_owner

- **Program item:** item 34
- **Surface:** fleet-internal
- **Endpoint:** No HTTP route changes. What changes is the R4 rebalancer's write to the object-store document fleet/overrides.json, and the per-tick eager shard open in fleet::start (src/fleet.rs pick_move_target, and the eager-handoff block). Indirect effect: appends and SSE reads on the affected shard via /v1/streams/... and /v1/stream/... served by the ring owner.
- **Condition:** (a) A lagging instance (absorb_lag_max > REBALANCE_LAG_SECS for 2 ticks, cooldown passed) picks a move target while the coolest healthy fresh heartbeat belongs to an instance outside the tick's active ring, such as a scale-in leftover or a non-ordinal name. (b) fleet/overrides.json holds {shard -> X} where X is not in the active ring.
- **Before:** (a) pick_move_target could pick the non-member. The laggard CAS-appended {shard -> non-member}, dropped the shard from its serving map and took a holdoff strike. effective_owner and the router mirror ignore an override to a non-member, so the move did nothing except evict the shard. (b) The eager handoff opened every entry whose raw e.to == me, so non-member X opened the shard and fenced the real ring owner's slatedb. X then yielded the shard at its next tick (OwnershipMoved retirement) and reopened it after the holdoff, so the owner was fenced about once a minute, indefinitely. Likely client symptom (inferred from the code, not pinned by a test): appends queued or in flight on the fenced owner are rejected as AppendErr::Moved, which is 503 shard_moving 'shard fenced by a new owner; retry' with Retry-After: 1, and SSE readers see WrongOwner-type cutoffs. The ring owner opened such a shard only lazily, at its first routed request.
- **After:** (a) pick_move_target(peers, active, me, threshold) keeps only members of the ring the tick just published. With no healthy member it logs 'no healthy peer; holding shards' and moves nothing. (b) The eager open covers exactly the overridden prefixes where effective_owner(p) == me (effective_owner = the override target if it is in the active ring, otherwise the rendezvous owner). A non-member target never opens the shard. The ring's rendezvous owner of a shard overridden to a non-member opens it at its next tick, fencing any previous holder. return-home and the move reuse the tick's `active` instead of re-reading ring_active(). No status, code or header changes on any route.
- **Retry semantics:** None on the wire. Removes a recurring source of retryable 503 shard_moving (Retry-After: 1) and SSE cutoffs on the ring owner of a shard overridden to a non-member. Adds no new error.
- **Who is affected:** Fleet peers and operators; the rebalancer moves less often when only non-members look healthy. Indirectly, product and raw clients of shards that hit the pre-fix fencing loop, who stop seeing periodic retryable shard_moving and cutoffs.
- **Pinning tests:**
  - an_override_the_ring_ignores_is_never_opened_by_its_target (src/dst/tests/fleet_controller.rs)
  - a_lagging_owner_moves_its_shard_only_to_an_active_member (src/dst/tests/fleet_controller.rs)
  - the_rings_owner_opens_every_overridden_shard_it_is_assigned_at_the_tick (src/dst/tests/fleet_controller.rs)
  - target_must_be_a_member_of_the_active_ring (src/fleet/tests.rs)
  - target_must_be_well_under_the_threshold_not_merely_under_it (src/fleet/tests.rs, extended with the exact-half case)
  - target_is_the_coolest_healthy_peer / target_excludes_self / no_target_when_every_peer_is_also_lagging (src/fleet/tests.rs, signature updated)
- **Risk reason:** Fleet-internal ownership policy only. No request that succeeded before now fails; the change removes a fence-flapping failure mode. Minor corner: an empty published ring now makes effective_owner None, so no eager opens happen, where before entries with e.to == me still opened. That case is unlikely because `active` always has self or falls back to the ordinal set.
- **Check against commit:** matches; no notes.

### #38 2f2c3015 — Telemetry-append authenticates before reading the body

- **Program item:** item 52
- **Surface:** fleet-internal
- **Endpoint:** POST /v1/internal/telemetry-append/{\*name} (src/http/telemetry_append.rs::internal_telemetry_append)
- **Condition:** Any POST to the relay receiver, in particular: an unauthenticated or wrongly authenticated upload; an authenticated relay whose body is over 2 MiB; a body over the configured MAX_REQUEST_BODY_BYTES; a header-level refusal (not_system_stream, missing_key, invalid_name) on a body over 2 MiB.
- **Before:** The handler took `body: Bytes`, so axum buffered the whole body under its implicit DefaultBodyLimit of 2 MiB (the router sets no other limit) before any check. Unauthenticated, body \<= 2 MiB: body fully read, then 401 {"error":{"code":"unauthorized","message":"fleet-internal credential required"}}. Any caller, body > 2 MiB: axum rejection 413 text/plain 'Failed to buffer the request body: length limit exceeded' before auth and header checks, so unauthenticated callers, 403 not_system_stream and 400 missing_key/invalid_name cases all got 413. Authorized relay of 2 MiB to 32 MiB: 413 plain text; the sender reported 'system append {stream}: ...' and requeued the batch every round, so it never landed. Body-read transport error: axum 400 'Failed to buffer the request body'.
- **After:** The handler takes `Body`. The order is: fleet_operation_authorized (fleet credential or workload JWT with op telemetry-append), then 403 not_system_stream, 400 missing_key, 400 invalid_name, and only then axum::body::to_bytes(body, config.cli.max_request_body_bytes) (32 MiB default, namespace-pinned). Unauthenticated at any size: 401 unauthorized with zero body bytes read. Over TCP the unread upload may show up to the client as a connection reset instead of a readable 401, as the test's own comment notes. Header-level refusals likewise come before any read. Authorized, body \<= limit: the append contract (200/204 with Stream-Next-Offset etc., or 409 not_ring_owner with Streams-Replay-To). Over the limit: JSON 413 {"error":{"code":"too_large","message":"body too large"}} through render_append (FailureClass::Invalid + AppendCode::TooLarge, no Retry-After). Any to_bytes error, including a transport error mid-body, is also answered 413 too_large, the same conflation as the raw PUT intake.
- **Retry semantics:** Authorized relays of 2 MiB to 32 MiB: from a 413 that failed every round to 2xx success. Unauthenticated over 2 MiB: 413 becomes 401 (both non-retryable). Over the limit: plain-text 413 becomes JSON 413 too_large. A body-read error changes from 400 to 413. No change for authorized relays \<= 2 MiB.
- **Who is affected:** Fleet peers running billing::system_append relays (audit, usage and ops system streams): large batches now land. Unauthenticated probes of the internal route. Operators: a receiver can hold about 2x the configured limit per concurrent authenticated relay (this read plus append_typed's charged buffer).
- **Pinning tests:**
  - telemetry_append_refuses_an_unauthenticated_body_unread (src/dst/tests/security_workload.rs)
  - telemetry_append_reads_to_the_configured_limit_after_authentication (src/dst/tests/security_workload.rs)
- **Risk reason:** Fleet-internal route only. The only status changes are failure to success (authorized 2 MiB to 32 MiB) or one failure to another (413 to 401 or to typed 4xx). The main new exposure is operational: per-relay memory rises from 2 MiB to about 2x 32 MiB, bounded to fleet-credentialed callers. No test pins that header-level refusals precede the read on a large body, or the read-error-to-413 conflation.
- **Check against commit:** No mismatch. Omitted from the recorded text: (1) header-level refusals (403/400) also moved ahead of the body read, so for bodies over 2 MiB they changed from 413 to their typed codes; (2) to_bytes reports any body-read error as 413 too_large (previously axum's 400); (3) the over-limit 413 changed body format from axum plain text to the JSON too_large envelope.

### #39 33fbd10e — One bearer gate for all of /v1/debug

- **Program item:** item 44
- **Surface:** operator-debug
- **Endpoint:** All of /v1/debug and /v1/debug/{\*}: timings, load, store, usage, auth, ops-events, usage-reconcile, absorb-pause (POST), abort (POST), sleep, history-stall (POST), absorb, plus unrouted paths and the bare prefix. Now mounted as .nest("/v1/debug", debug::gated(...)).
- **Condition:** A deployment bearer is configured, and the request has no Authorization bearer or a wrong one, and targets either a path the debug table does not route or a routed path under the wrong method.
- **Before:** Each of the 12 handlers ran its own copy of the authorized() check after extractors. Routed path, right method, no or wrong token: 401 {"error":{"code":"unauthorized","message":"bearer token required"}}. Unrouted /v1/debug/xyz or the bare /v1/debug: the router's default fallback, 404 with an empty body. Routed path, wrong method: 405 with an Allow header and an empty body. This let an anonymous caller map which debug routes exist. All responses carried prisma-streams-origin and x-content-type-options: nosniff.
- **After:** The require_deployment_bearer middleware layer on the nested router, with its own NOT_FOUND fallback, runs before method routing, extractors and handlers. Without the token, EVERY /v1/debug path (routed, unrouted, bare prefix, wrong method) returns 401 with the same JSON unauthorized envelope and the origin and nosniff headers; an unauthenticated absorb-pause does nothing. With the token, or in Off mode with no bearer configured: handler answers unchanged, unrouted paths are an empty 404, and a wrong method is a 405 with Allow. axum 0.8.9's Router::fallback does not override method-router 405s, so the 405 is preserved.
- **Retry semantics:** None for authorized callers. Unauthenticated wrong-method and unrouted requests change from 404/405 to 401; all are non-retryable.
- **Who is affected:** Operators and tooling that call /v1/debug without a token and branched on 404/405 (none found under scripts/). Authorized operators and Off-mode local development see no change.
- **Pinning tests:**
  - debug_surface_refuses_every_path_without_the_token (src/dst/tests/security_routes.rs)
  - debug_surface_serves_every_handler_with_the_token (src/dst/tests/security_routes.rs)
- **Risk reason:** Operator debug surface only. The status change applies only to requests that already failed (unauthenticated), moving from 404/405 to 401. Authorized answers are unchanged. Gaps in the pins: Off mode behind the new layer is covered only by the existing Off-mode debug tests, not a dedicated test; the Allow header on the authorized 405 is not asserted; the bare /v1/debug with the token is not asserted. A theoretical edge: debug_load dropped its unused Query extractor, so a query string that failed HashMap\<String,String> deserialization (effectively impossible with form-urlencoded) would now get 200 instead of 400.
- **Check against commit:** matches; no notes.

### #40 a0185c3b — Panicked connection tasks are logged and counted

- **Program item:** item 37 step A
- **Surface:** operator-debug
- **Endpoint:** GET /v1/debug/load (new key tasks.connection_panics). The serve_h1 accept loop in src/http/serve.rs covers every route's connections.
- **Condition:** A request handler, or a response body being streamed, panics. Nothing in hyper 1.10, hyper-util, axum or the router catches the panic, so it unwinds the whole connection task. Separately, any GET /v1/debug/load.
- **Before:** The panicking request's client saw a closed socket with no response. The JoinError was discarded (join_next reaped with `Some(_)`, and the shutdown drain used `.is_some()`). The only trace was the default panic hook's unstructured stderr line: no tracing event, no counter. /v1/debug/load's `tasks` object held phase, critical_failure and loops.
- **After:** The client still sees a closed socket with no response. The accept loop's reap() logs tracing::error!("connection task panicked; its request got no response: {error}") and increments TaskSupervisor::record_connection_panic on that runtime's own task record, not a process static. A task cancelled by the shutdown's abort_all is not counted. /v1/debug/load gains the additive key tasks.connection_panics (u64, per runtime; 0 once the supervisor is gone). Readiness and /health are unchanged; step B, poisoned locks feeding readiness, is deferred.
- **Retry semantics:** None. A panicked request fails the same way (connection closed), and client retry behaviour is unchanged.
- **Who is affected:** Operators and dashboards reading /v1/debug/load (additive key) and log pipelines (new ERROR line). Clients are not affected.
- **Pinning tests:**
  - a_panicking_handler_is_counted_once_and_an_aborted_connection_is_not (src/http/serve.rs mod tests)
  - debug_load_reports_typed_limiter_and_frame_totals (src/dst/tests/admission_maintenance.rs, now asserts tasks.connection_panics == 1)
  - panicked_connections_are_counted_on_the_monitor (src/tasks/tests.rs)
- **Risk reason:** Additive debug key and a log line; no status or body change on any client route. Release builds unwind (no panic=abort profile), so the counter is reachable in production. The count is per connection task, not per request: one panic also ends any other keep-alive or pipelined requests on that connection, and a panic inside a streaming (e.g. SSE) body counts as well.
- **Check against commit:** matches; no notes.

### #41 714abcc2 — Refused supervised future is dropped after the registration lock

- **Program item:** F-G
- **Surface:** process
- **Endpoint:** No route. TaskSupervisor::spawn (src/tasks.rs) with the new src/tasks/refusal.rs; affects runtime teardown and process shutdown.
- **Condition:** A tokio runtime that is shutting down (being dropped) refuses a TaskSupervisor::spawn issued while that supervisor is still Running, typically from a destructor that runs during teardown. The refused future's drop then re-enters the same supervisor, e.g. an engine required task's RequiredExit guard calls engine.begin_close, which begins the EngineTasks supervisor's shutdown and takes the same std mutex.
- **Before:** tokio::spawn dropped the refused future inline, under the registration lock. Its RequiredExit drop called begin_close, which tried to take the same lock on the same thread: a self-deadlock. The worker never returned, and the runtime drop, which joins workers with no timeout, hung forever. This hung a DST test in CI twice (the F-G hang), printing nothing. In production, main drops its multi-thread runtime after block_on(run(config)) returns, so leftover tasks whose drops spawn could hang process exit until an external SIGKILL.
- **After:** Each supervised future is wrapped in SetAsideOnRefusal. A drop that happens during the spawn call on this thread is parked in a thread-local slot and returned as `Refused`, and spawn drops it only after releasing the registration lock. Outside a spawn call the wrapper drops its future normally, after the slot borrow ends. The registration protocol and return values are unchanged: Ok(id) with the handle registered even when the runtime refused the task, and Err(ShuttingDown/Stopped) from the phase check. Teardown completes.
- **Retry semantics:** None on the wire. A shutdown or teardown that could hang indefinitely now finishes.
- **Who is affected:** Operators and the platform (a clean exit instead of a hang ending in SIGKILL on shutdown); CI (the DST teardown hang).
- **Pinning tests:**
  - a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor (src/tasks/tests.rs)
- **Risk reason:** Process-internal liveness fix with no client-visible contract change. Residual edge, not client-visible: if tokio::spawn itself panics (called outside any runtime), the thread-local slot is left populated. That path already poisoned the registration mutex before this change.
- **Check against commit:** Code matches. Wording nuance: the commit calls this a 'root-cause candidate' for F-G, reproduced deterministically in a unit rig, while also saying the DST test 'hung CI twice that way'. The recorded text states it as 'the CI hang' without that hedge. The production-shutdown exposure is an inference not stated in the commit; it is plausible only when a supervisor is still Running at runtime drop.

### #42 6ef3bc64 — Typed SourceReadError for SSE source reads

- **Program item:** item 87
- **Surface:** both
- **Endpoint:** SSE live reads on both surfaces: GET /v1/streams/{name}?live=sse (product) and GET /v1/stream/{name}?live=sse (raw), via LiveFeed::read_and_publish and session catch_up::stalled. This includes sealed-span remote reads to fleet peers. The only observable change is operator logs.
- **Condition:** An SSE source read (SingleSource or LineageSource read_batch, or LineageSource::sealed_span_page) fails.
- **Before:** Reads returned anyhow::Error, and both consumers used downcast_ref::\<FatalSpanCutoff> to tell fatal from retryable. Fatal: live_tail_cutoff, and the remote refusals Unauthorized->FleetAuth, TargetGone->IncarnationChanged, TargetMismatch->TargetMismatch, RedirectLoop->RedirectLoop (with a warn log). A fatal read made the feed Gone(reason) with one version bump (FEED_VERSION_BUMPS++) so sessions disconnect without a terminal control; in catch-up it counted the cutoff, bumped FEED_TOPOLOGY_DISCONNECTS and logged at info. Everything else (local read and stream-handle errors, 'sealed span engine unavailable', 'ownership indeterminate', remote Retryable/Transport/InvalidResponse/WrongOwner, 'lineage span ended below its cap') was retryable: FEED_SOURCE_FAILED++, bounded backoff (100 ms in catch-up), with the cause dropped unlogged.
- **After:** The same classification, arm by arm, now carried as SourceReadError::{Fatal(SourceCutoff), Retryable(anyhow)} with no From\<anyhow::Error>. The remote table lives in spans::remote_span_verdict. Fatal in the drive goes through LiveFeed::retire(Lifecycle::Gone(reason)), which gives the same Gone lifecycle and one version bump, though lifecycle and version are now set under two lock acquisitions instead of one critical section. Catch-up fatal behaves as before. Retryable keeps the same counter and backoff, plus a tracing::debug! event carrying error="{cause:#}". Statuses, SSE events, controls, cursors and metrics are unchanged.
- **Retry semantics:** None. The fatal/retryable split is preserved exactly; only debug-level log events are added.
- **Who is affected:** Operators, who get debug logs explaining FEED_SOURCE_FAILED. SDK and raw SSE subscribers are not affected.
- **Pinning tests:**
  - a_retryable_source_read_logs_its_cause_once_per_drive (src/sse/feed/tests/read_error.rs)
  - a_fatal_source_read_retires_the_feed_once (src/sse/feed/tests/read_error.rs, from eb742c42)
  - a_failed_catch_up_read_logs_its_cause (src/sse/session/tests.rs)
  - a_stalled_catch_up_read_owes_its_pass_one_verdict (src/sse/session/tests.rs, updated to typed errors)
  - remote_span_refusals_split_into_cutoffs_and_retries (src/sse/source/tests.rs)
- **Risk reason:** No wire change: every failure site keeps its prior verdict, confirmed site by site across source.rs and spans.rs and at both read_batch callers (feed.rs, session.rs). The added logs are debug-level. The only internal nuance is that retire() sets lifecycle and version non-atomically; a session could briefly see Gone before the version bump, but it disconnects either way.
- **Check against commit:** matches; no notes.

### #43 aaf2baa5 — Sweep custody handshake uses SeqCst read-modify-writes

- **Program item:** item 63 step A
- **Surface:** fleet-internal
- **Endpoint:** None on the wire. It is an in-process handshake in src/billing/sweep_custody.rs (SweepCustody::stamp_external / install / revoke_if) between the billing sweep (billing::mark -> install_custody, close_scheduler_engine) and every external engine resolution (shard_directory.rs:255/284 -> billing::stamp_external, reached from product and raw requests and from the LiveFeed source).
- **Condition:** A customer's first resolution of an engine (stamp_external: writes last_external_seq, then swaps custody to 0) races the billing sweep's custody install (reads last_external_seq, publishes custody, re-reads last_external_seq). The Ready path stamps outside the serving map's guard and the install holds no lock.
- **Before:** Every access was Relaxed and the install published custody with a plain store. This is the store-buffering shape, possible even on x86-TSO: both sides could miss each other, leaving the sweep's custody installed over an engine with external history. A later debt-free sweep's close CAS (revoke_if) then succeeded and retired (closed) an engine a customer had adopted. A LiveFeed source stamps once and keeps the engine, so its engine could be closed underneath it. There is no status code or body; the disruption is whatever an engine closed under live traffic produces.
- **After:** Every write to custody is a SeqCst RMW: the stamp's swap, the install's swap (was a store) and the revoke CAS with SeqCst success. Once both sides return, custody is 0. Either the stamp's swap revokes, or the install's re-check sees the external history and declines, and the engine stays customer-resident. The holds/held hints stay Relaxed. The only cost is one extra locked exchange per external resolution. No status, body, header or metric changes.
- **Retry semantics:** None. No wire contract changes. The change removes a rare race in which a customer-adopted engine could be closed.
- **Who is affected:** Nobody on the wire. It could only help clients (especially LiveFeed/SSE consumers) whose engine the billing sweep had opened and who resolved it concurrently with the install.
- **Pinning tests:**
  - billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody (new, Loom model, red-first: 'install 2 kept custody over an external stamp')
  - billing::sweep_custody::tests::install_holds_exactly_its_own_value
  - billing::sweep_custody::tests::an_earlier_external_stamp_declines_the_install
  - billing::sweep_custody::tests::an_external_stamp_revokes_installed_custody
  - billing::sweep_custody::tests::revoke_releases_only_the_installers_value
- **Risk reason:** This is an internal memory-ordering fix with no wire change. It only removes a failure mode. The close CAS under the directory write guard is not in the Loom model; the commit says the DST revoked-close rig covers it.
- **Check against commit:** None. The commit matches the recorded text. Loom is a dev-dependency and the model runs under plain cargo test (no cfg(loom)).

### #44 4cb6263c — The refresher, not the source, stamps auth feed freshness

- **Program item:** item 93
- **Surface:** operator-debug
- **Endpoint:** No endpoint changes in production. The stamp feeds: /v1/debug/auth feeds.{jwks,policies,grants}.ageSecs and stale; the /health readiness gate ('auth feeds not yet published' when ageSecs is null, i.e. stamp 0); and fail-closed enforcement on /v1/streams/... (503 policy_stale / grants_stale / keys_stale, plus live-SSE lease deadlines) and on JWKS checks for workload JWTs.
- **Condition:** Stamping of AuthService snapshot freshness (fetched_at_unix) for the JWKS, policy and grant feeds. Production has only the three file sources, all driven by auth_feed::refresh_once.
- **Before:** parse_keys/parse_policies/parse_grants took a `now` and each file source passed its own unix_now() after the read. The stamp was the moment the answer was parsed, and a source could post-date or back-date freshness. In production the only sources were the files, which stamped the current time, so no laundering or post-dating actually happened.
- **After:** Parsers return fetched_at_unix = 0, the never-fetched value. refresh_source records asked_at = unix_now() before calling fetch and stamps it only on a snapshot returned inside the 10 s SOURCE_DEADLINE and accepted by publish. A timeout, an error or a refusal leaves the previous snapshot ageing. A snapshot published without going through the refresher reads as unpublished: /health answers 503 and requests fail closed. No production path does this today. Net production effect: a feed's stamp is now the time of the request rather than the time of the answer. That is earlier by the file read duration, so up to about 1 s at second granularity and at most the 10 s deadline. ageSecs can read that much higher, and a stale refusal could fire that much earlier. This is immaterial because the window is 300 s and validation caps the refresh cadence at 100 s (POLICY_STALENESS_MAX_SECS/3).
- **Retry semantics:** None. The existing 503 feed-stale refusals (retryable) are unchanged in code, status and trigger, apart from the at-most-read-duration shift in when the window starts.
- **Who is affected:** Operators reading /v1/debug/auth ageSecs, where the shift is negligible. Any future feed source (the platform Stage 1 source) must publish through the refresher, or it will read as unpublished and fail closed. No SDK or raw client sees a change.
- **Pinning tests:**
  - auth_feed::tests::the_refresher_not_the_source_stamps_feed_freshness (new, red-first; source stamps 1 and now+86400, and both must publish with an age in 0..=60)
  - auth_feed::tests::keys_parse_and_reject (asserts parsing stamps 0)
  - auth_feed::tests::file_feed_freshness_means_last_successful_read_not_publisher_liveness (prior pin, unchanged)
  - auth_feed::tests::refresh_once_publishes_from_files_and_survives_a_broken_one
- **Risk reason:** No wire change. Production sources are files that go through the refresher. The only observable shift is a stamp earlier by the read duration against a 300 s window with at most a 100 s cadence. The fail-closed-on-bypass path has no production caller. I found no other caller of parse\_\* or publish\_\* outside tests.
- **Check against commit:** None material. Precision: the stamp is the time the refresher asked, not the time the answer arrived, so freshness is slightly more conservative than before. 'Fails closed' for a bypassing snapshot also means /health readiness answers 503 'auth feeds not yet published'. No end-to-end test pins that path; only the parse-returns-0 assertion does.

### #45 07db91a7 — Scaler cooldown and merge patience saturate instead of wrapping

> **Flagged:** the commit does not match the recorded text. See [Discrepancies](#discrepancies).

- **Program item:** item 41 C1
- **Surface:** fleet-internal
- **Endpoint:** None. This is the internal autoscaler (src/scaler3.rs evaluate_state and State::prune) under the operator env knobs SCALE_COOLDOWN_SECS and SCALE_HOT_EVALS. Clients see it only indirectly: split/merge churn, for example the typed disconnect on keyless raw SSE after a mid-flight split, with reconnect refused as 400 keyless_live.
- **Condition:** SCALE_COOLDOWN_SECS set to a value past the millisecond range (inf loads as i64::MAX via f64 as i64; anything above about 9.2e15 s overflows \*1000). Or SCALE_HOT_EVALS set so that hot_evals\*4 overflows u32 (at least 2^30; inf loads as u32::MAX).
- **Before:** Cooldown was cooldown_secs\*1000 at three sites (prune retention, split cooldown, merge cooldown). In debug builds this panicked ('attempt to multiply with overflow' at scaler3.rs:142). In release, i64::MAX\*1000 wrapped to -1000, which switched every cooldown off: a hot segment could re-split, and a cold stream merge, on every evaluation. Prune fell back to the idle horizon. Merge patience was hot_evals\*4 and panicked in debug. In release it wrapped: at 2^30 it wrapped to 0, and in general to 4\*(hot_evals mod 2^30). A wrapped-to-small patience made every multi-segment stream, however hot, a merge candidate. With exactly inf (u32::MAX), release wrapped to 4,294,967,292, which is effectively never merge rather than always merge.
- **After:** cooldown_ms() = cooldown_secs.saturating_mul(1000) and merge_patience() = hot_evals.saturating_mul(4) at all four sites. In-range values behave exactly as before. With inf cooldown there are no split or merge transitions at all. That includes a stream's FIRST transition, because a stream with no cooldown record has a sentinel age of now - i64::MIN/2, about 2^62 ms, which is below i64::MAX. Cooldown records are kept until the 4,096-entry cap evicts them, but eviction does not unblock anything under inf. Merge patience saturates at u32::MAX, so a huge hot_evals means effectively never merge. No debug panics.
- **Retry semantics:** None on the wire. Operator semantics of inf flip from 'no cooldown / maximal churn' (release) or crash (debug) to 'scaler transitions disabled'.
- **Who is affected:** Operators who set SCALE_COOLDOWN_SECS to inf or to about 9.2e15 or more, or SCALE_HOT_EVALS to 2^30 or more. Indirectly, subscribers on streams that were re-splitting every evaluation under the wrapped cooldown. Default and in-range deployments are unchanged.
- **Pinning tests:**
  - scaler3::tests::a_cooldown_beyond_the_millisecond_range_never_elapses (new, red in release)
  - scaler3::tests::merge_patience_is_four_split_patiences_and_never_wraps (new, red in release; uses hot_evals = 1\<\<30)
  - scaler3::tests::a_cooldown_holds_transitions_until_exactly_its_span_has_elapsed (green before and after, mutation killer)
  - scaler3::tests::a_transition_record_expires_exactly_at_its_cooldown (green before and after, mutation killer)
  - config::numeric_tests::scaler_coercions_preserve_the_existing_float_contract (pins inf -> i64::MAX / u32::MAX on load)
- **Risk reason:** This is an operator-only knob at non-default, pathological values, with no wire contract. Before, the build either crashed (debug) or did the opposite of what was configured (release). In-range values are unchanged and pinned by the mutation-killer tests.
- **Check against commit:** does not match; see [Discrepancies](#discrepancies).

### #47 a73b7b79 — One freshness boundary for every auth feed reader

- **Program item:** item 65
- **Surface:** operator-debug
- **Endpoint:** GET /v1/debug/auth (feeds.policies.stale and feeds.grants.stale; operator-gated debug table). The same boundary refactor also touches request verification on /v1/streams/... (policy_stale / grants_stale / keys_stale 503), capability status_and_quotas, lease_check and lease_deadline.
- **Condition:** The feed's age is compared against the staleness window. It differs only when the instance window (staleness_max_secs) is not POLICY_STALENESS_MAX_SECS (300), or on i64 overflow of stamp arithmetic.
- **Before:** feed_json computed stale as now - fetched_at > POLICY_STALENESS_MAX_SECS (the constant), while enforcement and the adjacent stalenessMaxSecs field used the instance window. Under a shortened window the operator surface reported stale:false for a feed that verification was refusing. Ten sites mixed unchecked subtraction, saturating subtraction and addition.
- **After:** feed_fresh_until(fetched_at, w) = fetched_at.saturating_add(w) and feed_stale = now > fresh_until. All readers use them: verification (policy, grants, JWKS), status_and_quotas, lease_check, lease_deadline and feed_json. /v1/debug/auth stale now uses the instance window. In production the window is always 300, because set_staleness_max_secs is #[cfg(test)] only, so every answer is identical. The JWKS rewrite is algebraically equal for all i64. The unchecked-to-saturating change differs only on overflow, which is unreachable with real stamps.
- **Retry semantics:** None.
- **Who is affected:** Operators and test rigs that shorten the staleness window (DST fixtures only). No production client or operator sees a different value.
- **Pinning tests:**
  - auth::tests::every_freshness_reader_shares_one_boundary (new, red-first: 'the operator surface must call a refusing feed stale')
- **Risk reason:** Operator/debug surface only, and identical in production because the window cannot be changed outside cfg(test). The enforcement boundaries are algebraically unchanged.
- **Check against commit:** None. The claim that the window is never changed in production is confirmed: staleness_max_secs is initialized from POLICY_STALENESS_MAX_SECS and set_staleness_max_secs is #[cfg(test)].

### #48 fdc31b07 — --compactor-poll-ms on argv reaches the compactor

- **Program item:** item 32
- **Surface:** process
- **Endpoint:** None on the wire. It is the process startup flag --compactor-poll-ms (env COMPACTOR_POLL_MS) feeding EngineConfig.compactor_poll_ms, and through compactor_options()/resolved_compactor_options the poll interval of every DB family: shard, history, rollup and read_spool. The effective value also appears in the startup log 'effective configuration (redacted)' as engine.compactor_poll_ms.
- **Condition:** The operator passes --compactor-poll-ms on argv, with or without COMPACTOR_POLL_MS also in the environment.
- **Before:** clap parsed and validated the flag and env var into CliArgs.compactor_poll_ms, but nothing read it. The environment overlay (env_parse COMPACTOR_POLL_MS) alone set EngineConfig. So an argv-only value was accepted and silently ignored (compactors polled at the 2500 ms default), and when both were set the env value won. A malformed value was already rejected by clap at boot.
- **After:** with_knob_defaults copies clap's resolved value into EngineConfig, and the overlay no longer reads the name. Argv values take effect. Clap precedence applies (argv over env over default 2500), so when both are set, argv now wins over env. Environment-only deployments get the same value, because clap reads the process env when argv is silent. Malformed values are rejected by clap at boot, as before. There is no poll-interval validation, so an argv value such as 0, previously ignored, now reaches SlateDB, as an env value already could. The Environment abstraction (MapEnvironment in test rigs) no longer carries COMPACTOR_POLL_MS. --compactor-max-concurrent still has the bug and is held (decision D-32c, MEMPROFILE_CERT).
- **Retry semantics:** None (no client surface).
- **Who is affected:** Operators who pass --compactor-poll-ms on argv, whose value now actually applies (for example 500 ms raises object-store probe rate about 5x, per the RUNBOOK and TIGRIS-404-COST notes), or who set both argv and env (argv now wins). The repo's deploy scripts and docs use env only (bench/soak/deploy-region.sh deliberately leaves it unset).
- **Pinning tests:**
  - config::tests::compactor_poll_given_on_argv_reaches_the_compactor_options (new, red-first: left 2.5s, right 500ms)
  - config::tests::clap_owned_names_keep_their_environment_channel (subprocess clap parse of a scrubbed env; with helper config::tests::clap_owned_environment_helper; green on both trees)
  - config::tests::load_with_empty_environment_equals_knob_defaults (now also asserts engine equality across CLIs)
- **Risk reason:** Process/config only, with no wire change. Env-only deployments are pinned unchanged. It affects only operators who used the argv flag, which was previously a silent no-op. No test pins argv+env precedence.
- **Check against commit:** None. Addition to the record: when both argv and env are set, precedence flips from env to argv, which the recorded text does not mention.

### #49 46d4b7df — Billing readiness reads the clap-resolved mode

- **Surface:** operator-debug
- **Endpoint:** GET /health (readiness body/status); GET /operator/billing.json (fields `mode`, `ready`; bearer-gated 401 path unchanged); internal billing::drain_once memory-only refusal; process boot log 'effective configuration (redacted)' (billing section)
- **Condition:** Clap's resolved --billing-mode/--rollup differ from the raw BILLING_MODE/ROLLUP environment variables, which happens only when the flags are given on argv (argv wins over env in clap): (a) argv --billing-mode required, env BILLING_MODE unset/other; (b) billing mode required (argv or env) plus argv --rollup 1 with env ROLLUP unset; (c) the reverse: env BILLING_MODE=required (and/or ROLLUP=1) with argv --billing-mode off (and/or --rollup 0). Env-only deployments are unchanged because clap reads the variable when argv is silent. Only the exact words `required` and `1` select; 'Required', 'required ', 'true', 'yes' still read as off.
- **Before:** The gate read the env copies BillingConfig.mode_env == Some("required") and rollup_env == Some("1"). (a) GET /health skipped the billing gate and returned 200 'ok' with x-streams-git/x-streams-build-unix/x-streams-boot-id, even with the spool closed. /operator/billing.json returned 200 {"mode":"off","ready":true,...}. With the spool closed, drain_once took the volatile memory-only emission path. (b) The rollup check was waived, so /health returned 200 and `ready` was true with no rollup installed. (c) /health was gated on the spool, which boot does not open because clap mode is off. The spool is opened only asynchronously by telemetry-outbox-sweep, and only when USAGE_STREAM_KEY is set. Until then /health returned 503 text/plain 'billing not ready (spool=false, rollup=...)'. This was permanent if the spool never opened, or if env ROLLUP=1 while clap rollup!=1. /operator/billing.json returned mode "required" with `ready` possibly false forever, and drain_once refused while the spool was closed. The boot summary's billing object included keys mode_env and rollup_env.
- **After:** The gate reads CliArgs::billing_required() (clap billing_mode == "required") and runs_rollup() (clap rollup == "1"). (a)/(b) GET /health returns 503 text/plain 'billing not ready (spool={bool}, rollup={bool})' until the read spool (and, with rollup 1, the rollup DB) is installed, then 200 'ok' with the same identity headers. /operator/billing.json `mode` = the clap-resolved string (default "off"). `ready` = !required || (usageLedgerConfigured && spool.open && (!runs_rollup || rollup installed)). drain_once with the spool closed returns Err('read spool not open (BILLING_MODE=required refuses the memory-only path)'). (c) There is no gate, so /health returns 200 'ok' and the report shows mode "off", ready true. With the spool closed, drain_once now takes the memory-only path. No new status code, error code, header, body word or JSON key. The boot summary loses billing.mode_env and billing.rollup_env. In the real binary, bootstrap::run (src/bootstrap.rs:680-703) opens the spool and rollup DB synchronously before TcpListener::bind whenever clap mode is required. Failure there is fatal, and both slots are OnceLock (never uninstalled). So the new 503 window in (a)/(b) is not externally observable in production. It shows only in the DST rig, which skips bootstrap.
- **Retry semantics:** Readiness only. (a)/(b): a 503 window is added before the spool/rollup installs, which in production is pre-bind, so the effective change is none. (c): a 503 that could be permanent becomes 200. Internal drain: argv-required with a closed spool goes from Ok (volatile emission) to Err, retried each drain tick; the reverse config goes the other way. No client API retry semantics change.
- **Who is affected:** Platform/orchestrator readiness probes and load balancers that consume /health (only mismatched argv/env configurations); operators and dashboards that read /operator/billing.json `mode`/`ready`; log parsers that consume the boot summary JSON. SDK, product and raw clients are not affected.
- **Pinning tests:**
  - dst::dst_tests::billing_readiness::argv_billing_mode_required_holds_readiness_until_the_spool_opens
  - dst::dst_tests::billing_readiness::argv_rollup_owner_is_unready_until_its_rollup_installs
  - dst::dst_tests::billing_readiness::default_billing_mode_reports_off_and_ready_without_a_spool
  - config::tests::billing_selectors_keep_their_exact_words
  - config::tests::default_values_are_pinned
  - config::tests::clap_owned_names_keep_their_environment_channel (env-only channel; added in preceding commit fdc31b07)
- **Risk reason:** Operator and readiness surface only; no product/raw status, code or body changes. Only deployments that pass the billing flags on argv see a difference. In production the added 503 cannot be observed, because boot opens the spool and rollup before binding. The visible deltas are /operator/billing.json `mode` becoming "required" for argv-configured instances, and the reverse-mismatch instances becoming ready (503 to 200). Gap: no test pins case (c) (env required, argv off, now 200) or the removal of the boot-summary keys.
- **Check against commit:** These are nuances; the code matches the commit. (1) '--rollup 1 on argv' gates /health only when billing mode is required; on its own it changes nothing. (2) 'it was 200 and reported off': /health's body was 'ok'. 'off' was /operator/billing.json `mode`, and only when env BILLING_MODE was unset. (3) The 503-until-ready window is reachable only in the DST rig: bootstrap::run opens both slots before listener bind in required mode, so production /health stays 200 once listening. (4) 'Reverse mismatch 503 forever' was conditional. It was permanent only when the async spool open never succeeded (e.g. USAGE_STREAM_KEY unset) or env ROLLUP=1 while clap rollup!=1; otherwise it cleared once telemetry-outbox-sweep opened the spool. (5) The commit's 'No status code ... changes' means no new codes or words. The status returned for a given configuration does change.

### #50 e19c80c5 — One corrupt row fails only its stream in a v2 gather lane

- **Surface:** operator-debug
- **Endpoint:** Nothing on the wire; this is the background v2 history absorber (history::gather::absorb_gather_v2_with, history::worker::settle_gather). It is observable via the warn log line and GET /v1/debug/load postings.pages_written / runs_written / bytes_written. The indirect effect is on product and raw append admission, through absorb-lag/unabsorbed-bytes maintenance backpressure.
- **Condition:** A v2 shared-partition gather lane (up to V2_LANE_PER_TICK = 1024 streams) contains one stream whose stored WAL row fails canonical admission (RangeReadError::Corrupt: key width, namespace, frame, offset), or whose chunk's postings pages fail self-decode or decode to overlapping runs. Store errors (every slatedb kind, including Data, checksum and fence), partition, handle and flush errors are unchanged and still abort the whole lane.
- **Before:** The first per-stream error aborted the gather. Corrupt mapped to slatedb::Error Data 'stored record corruption: \<kind>'; postings failures became anyhow 'postings page failed self-decode during gather' or 'overlapping postings during gather'. The whole staged WriteBatch was dropped. settle_gather_error backed off every stream in the lane together (failures+1, retry_after = now + tick\*2^min(failures,6), up to 64 ticks, about 320 s) with one warn 'v2 gather failed (N streams): \<error>'. The lane-mates' backlog stayed unabsorbed and the engine's last_progress_ms did not advance. POSTINGS_PAGES_WRITTEN and RUNS_WRITTEN counted the refused chunk's pages, plus earlier lane chunks that were then discarded.
- **After:** The gather returns Ok, with GatherOutcome.failed = [(hash, StreamGatherFailure::{Corrupt(RecordCorruption) | PostingsSelfDecode | PostingsOverlap})]. check_postings verifies the pages before stage_checked, so a refused chunk stages no canonical row and no postings page. Lane-mates are written in the same flush and retire as before (advanced/no_work/partial handling unchanged). The failed stream stays pending, gets back_off alone, and never reaches boundary submit or lane marks. Each failure emits one warn: 'v2 gather left \<8-hex hash prefix> out: \<reason>; failure \<n> backs it off alone'. The refused chunk's pages, runs and bytes are no longer counted. No HTTP status, code, header or body change.
- **Retry semantics:** Internal only: lane-mates no longer inherit the corrupt stream's exponential backoff, while the corrupt stream's own backoff is unchanged (still retried indefinitely at most every 64 ticks). Log alerts keyed on 'v2 gather failed' no longer fire for this cause.
- **Who is affected:** Operators, through the log text and the /v1/debug/load postings counters. Indirectly, product and raw clients on the affected engine benefit: one corrupt row can no longer stall absorption for up to 1,024 lane-mates, so it no longer freezes engine maintenance progress, which could have engaged absorb-lag shedding of appends.
- **Pinning tests:**
  - history::worker::lane_isolation_tests::corrupt_row_backs_off_only_its_stream
  - dst::dst_tests::history_gather::one_corrupt_row_fails_only_its_stream
  - history::gather::postings_refusal_tests::pages_that_fail_their_self_decode_refuse_the_chunk
  - history::gather::postings_refusal_tests::pages_that_overlap_refuse_the_chunk
- **Risk reason:** This is an internal background pipeline with no client contract change. The only operator-visible changes are the text of a warn line and debug counters that no longer count refused chunks. The effect on clients can only be positive: less lane-wide absorb stall.
- **Check against commit:** Minor: 'No wire change' holds for client APIs. However, /v1/debug/load postings.pages_written, runs_written and bytes_written no longer count a refused chunk's pages, and the operator log line changes from 'v2 gather failed (N streams): ...' to a per-stream warn.

### #51 5d9d517f — Undecodable pending billing artifact is logged

- **Surface:** operator-debug
- **Endpoint:** No response change. Affects the rollup pending_artifacts scan (artifact-pending/ prefix in the rollup DB), which runs in the hourly artifact publisher (billing::publish_artifacts, max 64) and on every GET /operator/billing.json (rollup.pendingArtifacts, max 1000). The only output is an error-level log line.
- **Condition:** An artifact-pending/ row whose key does not split into exactly five '/'-separated parts (prefix, month, account, project, stream), including a non-UTF-8 key, or whose body does not deserialize as MonthRow.
- **Before:** The row was skipped silently: not returned, never published, not counted in /operator/billing.json rollup.pendingArtifacts, not logged. It stayed in place (never deleted).
- **After:** The row is still skipped: not returned (list and tuple shape unchanged), never published, not counted, and still left in place. Each scan now emits tracing::error! 'pending monthly artifact stays unpublished' with the key (lossy UTF-8) and the error, which is either 'the key does not name a month, account, project and stream' or the serde error. Scans happen once per hourly publisher pass and once per GET /operator/billing.json request. /operator/billing.json output is byte-identical.
- **Retry semantics:** None.
- **Who is affected:** Operators only, through error-level alerts. Log volume grows with the /operator/billing.json polling rate times the number of bad rows.
- **Pinning tests:**
  - rollup::accounting_failure_tests::an_undecodable_pending_artifact_is_logged_and_stays_pending
- **Risk reason:** The change adds a log line and nothing else: no status, code, body or retry change on any surface.
- **Check against commit:** Minor: 'stays in the outbox' is not new behaviour; such rows were never deleted before either. The only behavioural change is the error log.

### #52 31fd9096 — Late byte-time beyond a correction fails its rollup page

- **Surface:** fleet-internal
- **Endpoint:** Usage rollup apply_page / apply_late_snapshot, the consumer of the internal \_usage ledger on the rollup-owner instance (ROLLUP=1). Indirect effects: GET /v1/streams/{name}/usage[?month=YYYY-MM] (product), GET /operator/billing.json rollup.lastApplyMs/lastApplyAgeSecs, and the correction-artifact outbox (corr-pending/).
- **Condition:** A late SegmentSnapshot into a finalized month whose storage_byte_ms_month, parsed as u128, exceeds the stored segment floor by more than i128::MAX (about 1.7e38). One month of a u64 gauge peaks at about 4.9e28, so only a corrupt or forged ledger envelope can reach this.
- **Before:** d_ms was computed as a u128 saturating_sub. The floors advanced, and a UsageCorrection with storage_byte_ms_delta = \<u128 text> was pushed. apply_page committed (cursor advanced, lastApplyMs updated) the month row with the correction, the name/project AggRows, and a corr-pending/ row. absorb() parsed the delta as i128 with unwrap_or(0), so correction totals silently omitted it. Every later decode_json of that month row failed ('number too large to fit in target type'). As a result, GET /v1/streams/{name}/usage?month=M for that stream returned 503 usage_unavailable (retryable true) permanently, and the next rollup page touching that month row failed and wedged the rollup there.
- **After:** apply_late_snapshot converts with i128::try_from before any floor moves. On overflow, apply_page returns Err('late byte-time difference exceeds a correction'); nothing is staged or committed and the cursor does not advance. rollup_step returns Err, the loop logs warn 'rollup step: late byte-time difference exceeds a correction', and it retries the same page about every 2 s indefinitely. The whole usage rollup (all accounts) halts at that page until an operator removes or repairs the envelope, and /operator/billing.json rollup.lastApplyAgeSecs keeps growing. The month row stays readable: GET /v1/streams/{name}/usage returns 200 with pre-correction data, stale for all streams past the stuck page. Every representable correction's text is unchanged.
- **Retry semantics:** Internal: 'commit, then fail on a later read' becomes 'fail this page and retry it forever'. Client usage reads for the affected stream-month change from a permanent 503 usage_unavailable (retryable) to 200 with stale data. The staleness now applies to every stream, not one row.
- **Who is affected:** Billing operators on the rollup-owner instance. Indirectly, SDK users who read usage endpoints, only in the corrupt-envelope case.
- **Pinning tests:**
  - rollup::accounting_failure_tests::a_late_byte_time_no_correction_can_carry_fails_its_page
  - rollup::accounting_failure_tests::a_late_read_correction_carries_a_written_zero_byte_time (pre-existing representable-path pin, 0ddfebc4)
  - rollup::accounting_failure_tests::an_unwritten_byte_time_keeps_its_empty_wire (pre-existing wire pin, 0ddfebc4)
  - rollup::tests::rollup_applies_deltas_and_closes_months (pre-existing late-snapshot delta "0" pin)
- **Risk reason:** Only a corrupt ledger envelope can trigger this, and that request could not succeed before either: it persisted an unreadable row. No client status or code changes on a normal path. Caveat for the reviewer: the blast radius shifts from one unreadable stream-month to a rollup-wide halt at the offending page (no skip or quarantine), which is visible only as growing lastApplyAgeSecs and a repeating warn.
- **Check against commit:** None in the code. Clarification: after the change the rollup still wedges, now at the offending page itself and with no persisted corruption, and it needs operator action to proceed.

### #54 (this record's commit) — The transition retry's re-preparation answers an unreadable registry as retryable

- **Program item:** release hold (split-boundary outcomes), skeptic finding F3. Recorded by its implementer for owner ratification.
- **Surface:** both
- **Endpoint:** Raw POST /v1/stream/{name} (append/close). Product POST /v1/streams/{name}/records and POST /v1/streams/{name}/records:batch, plus the final-record append inside POST /v1/streams/{name}:seal. Also internal AppendService appends (consumer DLQ delivery, system and telemetry appends).
- **Condition:** An attempt was refused by an engine closure on an unsealed descriptor (the retry loop of record #8). `closure_is_current` then found the route stale, waited out a pending split or merge, or found the descriptor gone, and the loop's re-preparation could not read the registry: a store error, or a descriptor that does not decode. The typical case is an append that waited out a split or merge: the publication invalidated the cached descriptor, so the re-preparation is a real store GET, and that GET fails once.
- **Before:** Raw 500 {"error":{"code":"internal","message":"\<store error>"}} with no Retry-After. Product 500 {"error":{"code":"append_failed","message":"append failed","retryable":false}} with no Retry-After. The request had committed nothing: every attempt was refused as closed, before any write.
- **After:** Raw 503 {"error":{"code":"segment_transition","message":"\<store error>"}} with Retry-After: 1. Product 503 {"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}} with Retry-After: 1. This is the answer record #8 gives the closure check's own read of the same round. Unchanged: an append's first descriptor read (raw 500 internal, product 500 append_failed), the gone path (404 not_found or 410 gone), 503 creating, admission's 429, and the incarnation fence (409 target_incarnation_changed). A descriptor that does not decode answers the same 503 inside the loop, as the closure check already did for the same read; the client's retry then meets it at its first read, which answers 500 internal as before.
- **Retry semantics:** A non-retryable 500 becomes a retryable 503. The TS SDK retries a 503 with retryable:true automatically (up to 3 times), so the append lands. The seal final-record disposition (AmbiguousOrTransient) and DLQ blocking are unchanged, because neither Internal nor Unavailable is a definitive rejection.
- **Who is affected:** Writers to a collection mid split or merge whose descriptor read fails once at the retry's re-preparation, and writers whose descriptor stops decoding in the middle of that retry.
- **Pinning tests:**
  - src/dst/tests/append_application.rs::r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal (red at 7549e28a with (Internal, Internal, None); the retry lands once, not as a duplicate)
  - src/dst/tests/append_application.rs::r02_a_closure_the_refresh_cannot_confirm_is_retryable_not_final (the closure check's read, unchanged)
- **Risk reason:** Low: the answer to a request that committed nothing moves from a non-retryable 500 to the retryable 503 that the same loop already gives the neighbouring read. No status, code or header is new to either surface, and the message text (the store error) is the same.
- **Check against commit:** Not checked. This record was written with the change, by its implementer, for the owner to ratify or reverse the 500 → 503 edge change.

## Discrepancies

One record has verified=false: its commit did not match the recorded text.

### #45 07db91a7 — Scaler cooldown and merge patience saturate instead of wrapping

- **Risk:** low
- **Surface:** fleet-internal
- **Finding:** The recorded text says inf means 'never re-scale'. In the code, inf means never scale at all: under SCALE_COOLDOWN_SECS=inf, a stream that has never transitioned is also blocked (its sentinel age of about 2^62 ms is below cooldown_ms = i64::MAX), so no first split or merge ever happens. This matches how any cooldown of about 4.6e15 s or more already behaved before. No test pins the fresh-stream case; the new test only seeds streams with a transition at 0. Secondary: the commit's merge test doc says SCALE_HOT_EVALS=inf 'used to wrap that patience to 0'. In release, u32::MAX\*4 wraps to 4,294,967,292, which is effectively never merge; inf only panicked in debug. The always-merge wrap happens for values near multiples of 2^30, such as the 1\<\<30 the test uses. The recorded wording 'a huge SCALE_HOT_EVALS' is correct for those values.

For #45, the Before and After entries in the low-risk section already describe the behaviour found in the code: under `SCALE_COOLDOWN_SECS=inf` no split or merge happens at all, including a stream's first. The mismatch is with the item's recorded wording ("never re-scale") and with the commit's merge-test doc comment about `SCALE_HOT_EVALS=inf`.

Every other record matched its commit. Where the check found precision points, or effects the original record did not state, they appear under "Check against commit" in that record's subsection.
