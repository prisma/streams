# Prisma Streams Recovery And Integrity Runbook

This runbook explains how to operate, recover, and validate Durable Streams while preserving correctness.

It is written for operators and developers running either:

- full mode (`src/server.ts`) with segmentation + object store
- local mode (`src/local/*`) with a single SQLite database and no object store

Use this document with:

- `overview.md` (commands and configuration)
- `architecture.md` (component model)
- `sqlite-schema.md` (storage invariants)
- `local-dev.md` (local server lifecycle)
- `conformance.md` (protocol verification)

## 1. Correctness model

### 1.1 Commit points

Correctness depends on explicit commit points in the write path:

1. Append commit (durability to local SQLite):
- Appends are accepted only after the ingest batch transaction commits.
- This is the point where offsets and WAL rows become durable on local disk.

2. Segment build commit (local materialization):
- Segmenter writes `*.tmp`, fsyncs, then atomic-renames to final segment path.
- Segment metadata is committed in SQLite (`commitSealedSegment`).
- If segment metadata commit fails, the local segment file is removed.

3. Segment upload (remote bytes present, but not yet visible):
- Uploader sends segment bytes to object store and marks the segment uploaded.
- By itself, this is not the visibility commit point for reads from object storage.

4. Manifest commit (remote visibility + GC gate):
- Manifest upload succeeds.
- Then `commitManifest(...)` atomically:
  - updates manifest generation
  - advances `uploaded_through`
  - computes GC bound as:
    - `uploaded_through` when no touch-processing state row exists, or
    - `min(uploaded_through, processed_through)` when touch-processing state exists
  - applies chunked WAL GC up to that bound
- This is the remote durability/visibility commit point.

### 1.2 Core safety invariants

For every stream, these invariants must hold:

- `uploaded_through <= sealed_through <= next_offset`
- segments do not overlap for a stream
- WAL offsets are unique per stream and monotonic
- manifest generation is monotonic (`uploaded_generation <= generation`)

Why this matters:

- Readers can merge historical (segments) and tail (WAL) data without gaps/duplicates.
- GC cannot delete data needed for correctness.
- Crash recovery can resume idempotently.

### 1.3 Read correctness

Read path behavior:

- For sealed/uploaded history, read from segments (local cache or object store ranges).
- For unsealed or not-yet-GC tail, read from SQLite WAL.
- Merge in offset order and return bounded slices (`DS_READ_MAX_*`).

Consequences:

- Reads remain correct while uploader/indexer are behind.
- Index lag affects performance, not correctness.

### 1.4 Sequence and idempotence guarantees

Two independent mechanisms exist:

1. `Stream-Seq` (opaque lexicographic monotonic token)
- If provided, must be strictly greater than existing stream value.
- Otherwise append is rejected (`409 stream_seq` conflict).

2. Producer tuple (`Producer-Id`, `Producer-Epoch`, `Producer-Seq`)
- Detects duplicates and gaps per producer.
- Rejects stale epoch and invalid epoch/seq transitions.
- Allows idempotent replays and explicit gap detection.

### 1.5 Schema/lens correctness

Schema registry rules:

- First schema install requires an empty stream.
- Later schema versions require valid `v -> v+1` lens.
- Writes validate JSON against current schema version.
- Reads promote older events through lens chains to current version.

This preserves immutable storage while returning current-schema payloads.

### 1.6 Touch/live correctness boundaries

Touch APIs exist only when the stream profile is `state-protocol` and
`touch.enabled=true`.

The live system:

- uses in-memory journal cursors (`epoch:generation`)
- supports stale detection (`stale=true`) across restarts and epoch changes
- can degrade to broader wakeups under pressure, but is designed to avoid
  missed invalidations
- does not use persisted touch companion streams or retention-based stale
  offsets

### 1.7 Durability boundaries (important)

Full mode:

- ACKed append is durable in local SQLite immediately.
- ACKed append is durable in object storage only after manifest commit advances `uploaded_through`.
- If the node/disk is lost before that point, latest ACKed writes may not exist in object storage.
- `--bootstrap-from-r2` restores published stream history and metadata from
  object storage.
- `--bootstrap-from-r2` does not restore transient local SQLite state such as
  the unuploaded WAL tail, producer dedupe state, or runtime live/template
  state.
- Deleting a node without a local snapshot is only safe after the streams you
  care about have uploaded through the tail you need and published a manifest
  for that state.

Local mode:

- Single durability domain: local SQLite files only.
- No object-store durability path exists.

## 2. Failure model and expected behavior

### 2.1 Crash points and outcomes

Append crash before commit:
- append is not durable; client should retry

Append crash after commit:
- durable in SQLite

Crash during segment build:
- tmp files are cleaned on startup
- stream can be re-segmented safely

Crash after segment upload but before manifest commit:
- uploaded object may exist, but visibility does not advance
- uploader retries; manifest remains source of truth

Crash during/after manifest commit:
- idempotent resume via SQLite state

### 2.2 One bad stream isolation

Segmenter/uploader keep per-stream failure backoff tracking.
One repeatedly failing stream should not globally stall unrelated streams.

## 3. Operational checks

### 3.1 HTTP checks

Basic liveness:

```bash
curl -sS http://127.0.0.1:8080/health
```

Lightweight counters:

```bash
curl -sS http://127.0.0.1:8080/metrics
```

Stream progress snapshot:

```bash
curl -sS "http://127.0.0.1:8080/v1/streams?limit=100&offset=0"
```

Metrics stream read:

```bash
curl -sS "http://127.0.0.1:8080/v1/stream/__stream_metrics__?offset=-1&format=json"
```

### 3.2 SQLite integrity checks

Set DB path:

```bash
DB_PATH="${DS_DB_PATH:-${DS_ROOT:-./ds-data}/wal.sqlite}"
```

Check global integrity:

```bash
sqlite3 "$DB_PATH" "PRAGMA integrity_check;"
```

Check stream progress invariant violations:

```bash
sqlite3 "$DB_PATH" "
SELECT stream, next_offset, sealed_through, uploaded_through
FROM streams
WHERE uploaded_through > sealed_through
   OR sealed_through > next_offset
LIMIT 20;
"
```

Check stuck segment claims:

```bash
sqlite3 "$DB_PATH" "
SELECT stream, segment_in_progress, updated_at_ms
FROM streams
WHERE segment_in_progress != 0
LIMIT 20;
"
```

Check pending upload backlog:

```bash
sqlite3 "$DB_PATH" "
SELECT stream, COUNT(*) AS pending_segments
FROM segments
WHERE uploaded_at_ms IS NULL
GROUP BY stream
ORDER BY pending_segments DESC
LIMIT 20;
"
```

Check manifest generation monotonicity:

```bash
sqlite3 "$DB_PATH" "
SELECT stream, generation, uploaded_generation
FROM manifests
WHERE uploaded_generation > generation
LIMIT 20;
"
```

### 3.3 Optional deep checks (offline)

Use only when traffic is stopped (expensive):

```bash
sqlite3 "$DB_PATH" "
WITH wal_counts AS (
  SELECT stream, COUNT(*) AS c_rows, COALESCE(SUM(payload_len), 0) AS c_bytes
  FROM wal GROUP BY stream
)
SELECT s.stream, s.wal_rows, COALESCE(w.c_rows,0), s.wal_bytes, COALESCE(w.c_bytes,0)
FROM streams s
LEFT JOIN wal_counts w USING(stream)
WHERE s.wal_rows != COALESCE(w.c_rows,0) OR s.wal_bytes != COALESCE(w.c_bytes,0)
LIMIT 20;
"
```

## 4. Standard runbooks

### 4.1 Planned restart (full mode)

1. Confirm health:

```bash
curl -sS http://127.0.0.1:8080/health
```

2. Optionally check backlog and wait for low pending uploads.
3. Send SIGTERM and wait for process exit.
4. Start with the same `DS_ROOT`/`DS_DB_PATH` and object-store config.
5. Re-check `/health`, `/v1/streams`, and metrics stream.

### 4.2 Planned restart (local mode)

Use local CLI:

```bash
bun run src/local/cli.ts status --name default
bun run src/local/cli.ts stop --name default
bun run src/local/cli.ts start --name default --port 8080
```

This preserves the named SQLite database.

### 4.3 Crash recovery

1. Restart with the same data root.
2. Verify:
- `/health` is `ok: true`
- `segment_in_progress` rows reset to 0
- pending uploads begin draining
3. Validate invariants (Section 3.2).

### 4.4 Object-store outage or high latency

Symptoms:

- pending segments grow
- `uploaded_through` stops advancing
- backlog-related backpressure appears

Actions:

1. Keep server running if local disk budget allows (uploads will retry).
2. Reduce incoming write pressure if needed.
3. Restore object-store availability/credentials/network.
4. Monitor that pending segments drain and `uploaded_through` resumes.

### 4.5 Ingest overload (429 responses)

Primary causes:

- memory guard (`DS_MEMORY_LIMIT_*`)
- ingest queue limits
- local backlog gate (`DS_LOCAL_BACKLOG_MAX_BYTES`)

Actions:

1. Inspect `/metrics` and `__stream_metrics__`.
2. Reduce write load or increase capacity knobs safely.
3. If due to backlog, prioritize object-store recovery and upload drain.

### 4.6 Touch/live issues

If `/touch/*` returns 404:

- confirm the stream profile is `state-protocol` with `touch.enabled=true`

If wait latency spikes:

- inspect `/v1/stream/<name>/touch/meta`
- check processor lag and active waiter counters
- reduce load or tune touch batch/check intervals

### 4.7 Suspected local SQLite corruption

1. Stop writer traffic.
2. Run `PRAGMA integrity_check;`.
3. If corruption is confirmed:
- full mode: rebuild from object storage (`--bootstrap-from-r2`) into clean local state
- local mode: restore from backup snapshot
4. Re-run invariants and conformance smoke tests.

## 5. Disaster recovery paths

### 5.1 Rebuild full-mode state from object storage

Warning: this deletes local SQLite and local/cache directories under `DS_ROOT`.

This restores the published durable state from object storage. It does not
recover data or helper state that only existed in local SQLite at the time the
node was lost.

```bash
bun run src/server.ts --object-store r2 --bootstrap-from-r2
```

Required env vars for R2:

- `DURABLE_STREAMS_R2_BUCKET`
- `DURABLE_STREAMS_R2_ACCOUNT_ID`
- `DURABLE_STREAMS_R2_ACCESS_KEY_ID`
- `DURABLE_STREAMS_R2_SECRET_ACCESS_KEY`

After bootstrap:

1. verify `/health`
2. verify stream list and offsets
3. run read smoke checks on critical streams

### 5.2 Local-mode restore

Local mode has no remote durability source.
Recovery requires filesystem backup/restore of:

- `durable-streams.sqlite`
- `durable-streams.sqlite-wal` (if present)
- `durable-streams.sqlite-shm` (if present)

## 6. Backup strategy

### 6.1 Full mode backups

Recommended:

- treat object store + manifest generation as primary long-term durable history
- snapshot SQLite for faster local recovery and operational continuity
- keep SQLite snapshots if you need recovery of the latest ACKed local tail and
  runtime-local helper state, not just published object-store state

SQLite snapshot rules:

- best: stop process first, then copy DB + sidecar files
- if hot snapshot tooling is used, validate restore path and consistency

### 6.2 Local mode backups

Since local mode is single-node SQLite only, regular backups are required if data matters.

## 7. Post-recovery validation checklist

Run after any significant recovery operation:

1. Health:

```bash
curl -sS http://127.0.0.1:8080/health
```

2. Invariants (Section 3.2 SQL queries) show no violations.
3. Protocol smoke tests:

```bash
bun test test/http_behavior.test.ts
bun test test/local_server.test.ts
```

4. Conformance (as needed):

```bash
bun run test:conformance
bun run test:conformance:local
```

5. For touch-enabled workloads, verify:

```bash
curl -sS http://127.0.0.1:8080/v1/stream/<stream>/touch/meta
```

## 8. What this runbook does not guarantee

- It does not make local mode multi-node durable.
- It does not replace object-store redundancy planning for full mode.
- It does not provide cryptographic end-to-end audit proofs; guarantees are implementation-level transactional and protocol guarantees.

## 9. Quick reference

Common commands:

```bash
# Full mode
bun run src/server.ts --object-store local
bun run src/server.ts --object-store r2

# Local mode
bun run src/local/cli.ts start --name default --port 8080
bun run src/local/cli.ts status --name default
bun run src/local/cli.ts stop --name default
bun run src/local/cli.ts reset --name default
```
