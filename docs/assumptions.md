# Prisma Streams Assumptions

This file records ambiguities in the source documentation and the chosen
behavior for this implementation.
Each assumption must have a corresponding conformance test.

## HTTP / protocol

1) Append to a missing stream returns 404 (no implicit create).
- Rationale: spec defines explicit `PUT /v1/stream/{name}` for creation; no auto-create is documented.
- Test: POST to a new stream returns 404 and a subsequent HEAD returns 404.

2) Expired streams return 404 Not Found (not 410) on read/append/head.
- Rationale: hide existence; spec allows either 410 or 404.
- Test: create a stream with Stream-TTL=1s, wait for expiry, then GET/HEAD/POST returns 404.

3) GET /v1/streams pagination uses numeric offset + limit.
- Behavior: query params `limit` (clamped) and `offset` (row index) are supported; returns streams ordered by name ascending.
- Rationale: existing code path and simplest for SQLite; spec only says listing must be efficient.
- Test: create 3 streams, call `/v1/streams?limit=2&offset=0` returns first 2; `offset=2` returns the 3rd.

4) Offsets reject decimal aliases; only `-1` and 26-char base32 are accepted.
- Rationale: avoid ambiguity and ensure canonical offsets on the wire.
- Test: read with `offset=0` returns 400.

5) Content-Type matching for JSON append is lenient.
- Behavior: treat `application/json` with optional `; charset=...` as JSON mode; otherwise byte mode.
- Test: POST with `Content-Type: application/json; charset=utf-8` and a non-empty array succeeds.

6) JSON append requires a top-level array; empty array is rejected on POST.
- Rationale: empty POSTs are ambiguous; enforce at least one element.
- Test: POST JSON `[]` returns 400. (PUT with `[]` still creates an empty stream.)

7) JSON append with Stream-Key is rejected when routingKey is configured.
- Rationale: spec explicitly disallows Stream-Key in that case.
- Test: configure routingKey, POST JSON with Stream-Key -> 400.

8) format=json on a stream containing non-JSON payloads returns 400.
- Rationale: returning invalid JSON would violate response contract; safest is to reject.
- Test: append raw bytes, then GET with `format=json` returns 400.

9) /pk/<key> path takes precedence over `key=` query param if both provided.
- Rationale: path form is an explicit selector; query param is redundant.
- Test: append two keys, GET `/pk/a?key=b` only returns key `a` entries.

10) Unknown `format` values return 400.
- Rationale: avoid silently returning unexpected data formats.
- Test: GET with `format=xml` returns 400.

## Storage and timestamps

11) Append timestamps are stored with millisecond resolution in SQLite, but segment records use unix nanos (ms * 1e6).
- Rationale: SQLite schema calls for ts_ms; segments require nanos.
- Test: append with Stream-Timestamp, read with `since=` between two appends returns only the later record.

12) Initial stream epoch is 0 and stays constant unless explicitly bumped by future migrations.
- Rationale: simplest default; matches "version 0" conventions.
- Test: append one entry, decode Stream-Next-Offset and assert epoch=0.

13) Stream deletion is a tombstone in SQLite, but stream-owned acceleration state is scrubbed immediately; objects in R2 are not synchronously deleted.
- Behavior: `DELETE /v1/stream/{name}` keeps the tombstoned stream row, removes routing/exact/lexicon/companion state plus per-stream object-store request-accounting counters and local `async_index_actions` rows for that stream in the same local transaction, then clears the same request counters again after the delete-manifest publish so recreating the same stream name starts at zero. Startup reconciliation re-scrubs any stale deleted-stream state left by older builds.
- Rationale: avoids slow object-store deletion on the request path while preventing deleted streams from leaving orphaned async-index state behind.
- Test: DELETE then GET returns 404/410, `/v1/streams` excludes the stream, and all per-stream acceleration tables, request-accounting rows, and local async-index action rows are empty. A restart also clears stale deleted-stream state.

14) Stream list excludes deleted and expired streams.
- Rationale: operationally expected; matches UI expectations.
- Test: create, delete, then /v1/streams does not include the stream.

15) Stream-Seq is lexicographic and strictly increasing when provided; values are opaque strings.
- Rationale: matches conformance tests and allows non-numeric sequencing.
- Test: Stream-Seq "2" then "10" is rejected (lexicographically smaller); "3" succeeds.

16) Missing Content-Type on POST is rejected.
- Rationale: keep the append contract explicit and avoid ambiguity between raw vs JSON.
- Test: append without Content-Type returns 400.
