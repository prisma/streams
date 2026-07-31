# Stage 2a implementation plan — consumer groups (task #81)

Working notes for the in-flight increment. Spec:
`handover/prisma_streams_surface_spec_prelaunch_hard_cutover/02-CONSUMERS-AND-WATCHES.md` §2.
Delete this file when #81 closes.

## Substrate (exists, verified)

- `src/queue.rs`: Lease/ConsumerState/QueueState + row codecs
  (`<hash16>'c'<consumer>` cursor, `'l'` lease, `'x'` settled-marker),
  `QueueOp::{Receive, Settle}`.
- Committer lane: `engine.submit_queue(hash, op)` →
  `CommitOp::Queue` handled serially in the commit loop
  (shard.rs ~2250): lazy state load from the three row tags, cursor
  advance over the settled prefix, lease/ack rows in the SAME
  WriteBatch as appends — the spec's "shard-commit-path state, no
  per-consumer DB/LIST" requirement is already satisfied by this lane.
- The old `/queue/*` HTTP surface (http.rs ~4087) shows the
  handler-side call shape; those PROFILE routes die in Stage 1 while
  this lane lives on under the product consumer routes.

## Deltas to implement

1. **queue.rs**
   - `Lease` += `key_hash: [u8; 16]` (row widens 16 → 32 bytes; fresh
     namespace, no compat parse needed beyond len check).
   - `QueueOp::Receive` += `keyed: bool` (product consumers always
     true; the profile passes false until Stage 1 deletes it).
   - New config codec + row: `<parent_hash16>'C'<consumer>` →
     ConsumerConfig { visibility_timeout_ms u32 = 30_000,
     max_attempts u32 = 5, dead_letter_stream Option<String>,
     max_batch_records u16 = 10 } (JSON in the row is fine — tiny,
     read-modify only through the committer lane). Config lives under
     the PARENT identity (collection-scoped), consumer STATE under
     each segment identity.
   - New ops: `ConfigPut { consumer, cfg }` (idempotent: equal
     normalized → 200, new → 201, different → 409),
     `ConfigGet`, `ConfigDelete` (deletes config + all state rows of
     that consumer under this identity).

2. **Committer Receive, keyed mode** (per-key FIFO, spec §2.3): the
   handler pre-reads frame HEADERS for `[cursor, cursor+window)` via
   `read_frames_range` (headers are plaintext — no decryption needed)
   to build `offset → key_hash`; then the lease scan skips any offset
   whose key has an ACTIVE (unexpired) lease at another offset, and
   leases at most ONE offset per key per pull batch. An offset outside
   the pre-read window stops the scan (never lease with unknown key —
   that could jump a blocked key's queue). Poison/cursor logic
   unchanged.

3. **Signed tokens** (product_cursor.rs, same MAC discipline):
   - `KIND_MSG_V1 = 0x31`: MessageId { epoch, key_hash, seg_id,
     offset } — same layout as KeyCursor, distinct kind.
   - `KIND_LEASE_V1 = 0x41`: LeaseToken { msg fields, lease_gen u32,
     deadline_ms i64 }.
   Both MAC'd with the stream-key-derived cursor key; wrong-kind
   rejection tests mirror the cursor tests.

4. **product.rs routes** (spec §2.5):
   - `PUT/GET/DELETE /v1/streams/{name}/consumers/{consumer}` →
     Config ops on the parent identity's engine. Consumer names: one
     path-safe segment, 1–128 bytes (validate like watch names).
   - `POST …/consumers/{consumer}:pull` body
     { max?, waitMs?, visibilityMs? } → keyed Receive on the segment
     engine; response { messages: [{ id, routingKey, attempts,
     leaseToken, value }], backlog }. Payloads: read each leased
     offset via read_merged (offset-filtered), decrypt, pair with the
     lease. waitMs: poll loop on handle.notify like long-poll when
     nothing deliverable.
   - `POST …:settle` body { acks: [{leaseToken}], retries:
     [{leaseToken, delayMs}], extends: [{leaseToken, visibilityMs}] }
     → decode+verify tokens (stale/wrong tokens are counted, never
     errors — spec §2.5), map to Settle op.
   - v1 lands single-segment; the lineage walk (predecessor drained
     per key before successor delivers — spec §2.9) is the follow-up
     commit inside #81 before it closes.

5. **DLQ to a separate stream** (spec §2.8): NOT the committer's job.
   When a retry/poison would exceed max_attempts, the HTTP layer:
   (a) internally appends the DLQ record to `dead_letter_stream` via
   the product append path with producer identity
   (`dlq:<consumer>:<message-id>`, epoch 1, seq 0) — producer
   idempotence makes the transition crash-idempotent; (b) after that
   append is durable, submits the source settle. Crash between (a)
   and (b): the retryer re-runs (a) as a duplicate, then (b). The
   existing in-committer `$dlq` routing-key reference lane stays for
   the profile until Stage 1; product consumers use the real stream.
   DLQ payload per spec §2.8 JSON.

6. **Tests**: per-key FIFO with interleaved keys (a blocked key's
   later records never deliver while leased; other keys flow);
   visibility expiry redelivery; stale-token settle ignored+counted;
   out-of-order acks advance the contiguous cursor; poison → DLQ
   stream append (idempotent under double-settle) → source settled;
   config idempotence 200/201/409; wrong-kind tokens rejected;
   consumer state survives engine restart (lazy reload).

## Order

queue.rs deltas → committer keyed Receive → tokens → routes
(create/pull/settle) → DLQ → tests → lineage walk → tests → commit.
