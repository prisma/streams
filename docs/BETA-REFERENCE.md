# Prisma Streams public beta: limits and answers

This page is for customers of the public beta. It states the limits a
project meets on a shared beta cell, the answers the server gives when a
limit is reached, how to retry, and what the beta does not promise.
How an app exchanges its project credential for access tokens, and the
issuer's and the server's authentication answers, are in
[AUTHENTICATING.md](AUTHENTICATING.md).

Every number here is the value at the time of writing, read from the
deployed profile and the server's code. Each section names the file
where an engineer checks it.

The ceilings below are **provisional**. They come from a throughput
envelope measured on a different shape of server. The envelope is being
measured again on the beta's own shape (one server, one shard), and the
ceilings will be restated from that measurement. Do not size a production
workload on them yet.

## How a beta cell is shared

A beta cell is one server that up to `k = 8` projects can use at their
full ceiling at once. Each shared limit of the server is divided by 8,
and every project gets that share unless the operator set a lower value
for it. More than 8 projects are placed on a cell, because most projects
use far less than their ceiling.

Source: `deploy/profiles/shared-cell.env` (`PROJECT_SHARE_K=8` and the
shared bounds), `src/auth/ceiling.rs` (`CellCeiling::effective`; its test
`a_shared_cell_holds_every_bounded_axis_to_its_share` pins the shares).

## Request headers

Send the stream key, a routing key and a consumer's version under their
`Streams-` names:

| Header | Carries |
|---|---|
| `Streams-Encryption-Key` | the stream's key, on every request that reads or writes a stream |
| `Streams-Routing-Key` | an append's routing key (absent: the default key) |
| `Streams-Consumer-Version` | the consumer version a consumer `DELETE` names |

The server also accepts each under its older `Prisma-` name
(`Prisma-Encryption-Key`, `Prisma-Routing-Key`,
`Prisma-Consumer-Version`), with the same value rules. Do not rely on
those names on a beta cell: the Prisma Compute edge in front of
`*.prisma.build` removes every request header whose name starts with
`Prisma-`, so the cell never sees it and answers `400 missing_key`. A
request that carries both names of one header with different values is
refused `400 conflicting_header`, not retryable; the same value under
both names is accepted. The SDK (`@prisma/streams` 0.3.0-beta.1) sends
the `Streams-` names.

## Response headers

The edge removes `Prisma-*` response headers too, so the server sends each
response header under both names, with the same value: read the
`Streams-` name (edge change #176).

| Header | Carries |
|---|---|
| `Streams-Next-Cursor` | the cursor to read from next (records reads and long-polls) |
| `Streams-Up-To-Date` | `true` when the page reached the stream's tail |
| `Streams-Sealed` | `true` when the stream is sealed |
| `Streams-Durable-Cursor`, `Streams-Pending-From` | a `deliver=applied` read's durable cursor and its first provisional record |
| `Streams-Next-Scan-Cursor`, `Streams-Scan-Complete` | a `:scan` page's next cursor, or `true` on its last page |
| `Streams-Consumer-Version` | a consumer's version (create, get, and the `consumer_deleting` conflict) |
| `Streams-Error-Code` | `wrong_cell` on a refusal that has no body to say so |
| `Streams-Origin` | present on every answer that came from a Streams server (not from the edge) |

Each is also sent as `Prisma-<Name>` (`Prisma-Next-Cursor`, ...;
`Prisma-Streams-Origin` for `Streams-Origin`), which a
client that reaches the server without the edge sees, and a browser may
read both (`Access-Control-Expose-Headers` lists both). The SDK reads the
`Streams-` name first.

Source: `src/header_alias.rs` (`REQUEST_ALIASES`; its tests pin that the
table holds every `Prisma-` request header the product surface reads),
edge change #173.

## Per-project limits

| Limit | Value | What happens at the limit |
|---|---|---|
| Requests per second | 176 at the ceiling; **10** for a project created through self-serve, unless its workspace's allow-list entry names another rate | `429 project_rate_limit`, `Retry-After` |
| Appended bytes per second | 257,500 | `429 project_rate_limit`, `Retry-After` |
| Read bytes per second | 3,750,000 | `429 project_rate_limit`, `Retry-After` |
| Requests in flight at once | 64 | `429 project_concurrency_limit` |
| Live subscriptions (SSE) | 150 | `429 project_concurrency_limit` |
| Streams | 8,192 | `429 stream_limit`, not retryable |
| Appended bytes queued, not yet stored | 64 MiB | `429 queued_bytes` |
| Memory a project's requests hold | 32 MiB | `429 project_memory_pressure`, `Retry-After: 1` |
| Stored bytes | 10 GiB by default; the operator can set more | `429 stored_bytes_limit`, not retryable |

Notes:

- **Bursts.** The request rate is a token bucket that holds the larger
  of one second's worth of requests and 50. A project at 10 requests a
  second that has been quiet for 5 s can send 50 at once, then 10 a
  second; a project at 50 a second or more holds one second's worth. The
  byte rates hold one second's worth. One append larger than a full
  second's byte budget is admitted once from a full bucket; the project
  then waits until the debt is paid. Source: `src/quota.rs`
  (`REQUEST_BURST_FLOOR`, `QuotaRegistry::admit`, `admit_append`,
  `check_read`), `src/quota/bucket.rs` (`Bucket::floored`); edge change
  #162.
- **Creates, deletes, seals and listings cost more.** These requests also
  pay for the storage work they cause, from a second budget that refills
  at the project's request rate. A create weighs 11. A listing weighs 10
  plus one per stream it scans. Every create weighs 11, a project's first
  after a deploy or restart included: the count of its streams the server
  makes then is not charged to it. While that budget is in debt, creates,
  deletes, seals and listings are refused `429 project_rate_limit` with a
  `Retry-After` of the seconds until the debt clears. Appends and reads
  are never refused by it. At 10 requests a second, a project creates one
  stream and then one every 1.1 s, and one listing of 100 streams holds
  the next create or listing back for about 10 s. Concurrent requests are
  all admitted before any is charged, so up to 50 concurrent creates are
  admitted at once and paid for afterwards: at 10 a second they leave the
  next create, delete, seal or listing refused for about 54 s, longer
  than the SDK's replays. Create streams one at a time, or ahead of when
  you need them. Source: edge changes #143 and #162;
  `src/quota/parked.rs`, `src/store_timing.rs`.
- **Empty consumer pulls pay for what they read.** Before it leases, a
  pull reads up to 4 MiB of the stream from the consumer's cursor. A pull
  that leases nothing (every record before it is leased to another pull,
  or blocked behind its key's lease) pays those bytes from the read-byte
  rate, as a pull that delivers pays for its batch. At 3,750,000 bytes a
  second that is about one empty pull a second over 3 MiB of leased
  records. While the project is in debt, its pulls, reads and scans are
  refused `429 project_rate_limit` with a `Retry-After` of the seconds
  until the debt clears, and a pull waiting with `waitMs` does not look
  again until the debt clears: new records reach it later, or it answers
  `{"messages": []}` at its deadline. Pull with a `max` that takes what
  you can process, and settle leases promptly. Source: edge change #175;
  `src/quota/parked.rs`, `src/application/consumer/pull_wait.rs`.
- **Self-serve.** A workspace on the beta's allow-list creates at most 5
  projects a day. It holds as many active projects as its allow-list
  entry allows, or else the cell's workspace cap (1 unless the operator
  set another when the cell was made). A project holds at most 10 live credentials and creates or
  rotates at most 20 a day. Source: `deploy/issuer/projects.ts`
  (`FREE_TIER_REQUESTS_PER_SEC`), `deploy/issuer/limits.ts`,
  `deploy/cell-admin/commands.ts` (`--workspace-cap`, default 1).
- **The stored-bytes figure lags.** It is the project's stored bytes
  after compression, as the usage rollup last counted them: about 10 s
  behind at the default cadence. A project can store past its limit by
  what it writes in that time plus one request. After a delete, appends
  are admitted again within 10-20 s. Reads, creates, deletes, seals and
  consumers are never refused on it, so a project at its limit can always
  delete to get under. Project usage
  (`GET /v1/projects/{project}/usage`) states the figure as
  `ownedStoredBytesNow`. Source: edge change #158;
  `src/quota/stored.rs`, `src/quota/append_charge.rs`.

## Size limits

| Limit | Value | Answer |
|---|---|---|
| Request body | 8 MiB (8,388,608 bytes) on a beta cell | `413 body_too_large`, not retryable |
| One record, as stored | 131,072 bytes | `413 body_too_large`, not retryable |
| Records in one `records:batch` | 10,000 | `400 batch_too_large` |
| A body larger than the stream's own rate bucket | depends on the stream | `413 payload_too_large`, not retryable, with `details` `{dimension, capacity, requested}` |

On a bytes stream the whole body is one record, so a bytes append is at
most 131,072 bytes. A JSON record is measured as stored: the client's
text with the whitespace outside strings removed. The body limit is
stored with the cell when it is created. Source:
`deploy/profiles/shared-cell.env` (`MAX_REQUEST_BODY_BYTES`),
`src/config/cli.rs` (`MAX_RECORD_PAYLOAD_BYTES`),
`src/product/append_body.rs` (`MAX_BATCH_RECORDS`).

## Error answers

Every error on the `/v1/streams` surface has this body:

```json
{"error": {"code": "<code>", "message": "<text>", "retryable": true, "details": {}}}
```

`details` is present only for some codes. `retryable: true` means the
request wrote nothing and can be sent again unchanged, after its
`Retry-After` when one is given. `retryable: false` means sending it
again unchanged will not help, or, for `outcome_unknown`, may store a
second copy. Source: `perr` in `src/product.rs`; the full per-route list
is `docs/refactor/WIRE-MATRIX.md` §0 and §2.

### 413

| Code | When | `retryable` |
|---|---|---|
| `body_too_large` | The body is over 8 MiB, or one record is over 131,072 bytes | false |
| `payload_too_large` | The body is larger than the stream can ever take in one request | false |

### 429

| Code | When | `retryable` | `Retry-After` |
|---|---|---|---|
| `project_rate_limit` | The project's request rate, append or read byte rate, or storage-work budget is spent | true | yes |
| `project_concurrency_limit` | 64 requests in flight, or 150 live subscriptions | true | no |
| `queued_bytes` | 64 MiB of the project's appends wait to be stored | true | no |
| `project_memory_pressure` | The project's requests hold 32 MiB | true | 1 |
| `rate_limited` | The server or the stream is at capacity | true | when the server gives one |
| `stream_limit` | The project holds its maximum number of streams | false | no |
| `stored_bytes_limit` | The project stores its stored-bytes limit; `details` `{dimension, limit, stored}` | false | no |

### 503

| Code | When | `retryable` | `Retry-After` |
|---|---|---|---|
| `overloaded` | The server is far over its load and refuses before reading the request | true | 1 |
| `read_memory_busy` | The server's read memory is full | true | 1 |
| `subscription_capacity` | The server holds its 1,200 live subscriptions | true | 5 |
| `temporarily_unavailable` | Storage could not be read for this request, or the stream's shard is opening (a deploy or a restart) | true | 1 |
| `maintenance_backpressure` | The stream's storage maintenance is behind | true | when given |
| `project_tracker_capacity` | The server tracks too many projects at once | true | no |
| `policy_stale`, `grants_stale`, `keys_stale` | The server's copy of the access policy is too old | true | 1 |
| `outcome_unknown` | A plain append was in flight when its shard moved (a deploy or a restart): it may or may not have been written | **false** | 1 |

A `503` from in front of the server (the platform's edge) has no such
body. Treat it like `outcome_unknown` for an append, and as retryable for
anything else.

## Retrying appends safely

The service promises that an ambiguous append is explicit (guarantee G7,
`SPEC.md` §4): after a timeout, a lost connection, a `408`, a `5xx` other
than a retryable `503`, or `503 outcome_unknown`, an append may or may not
have been stored. Do not resend a plain append blindly. Either:

- append through a **producer** (the `producer-id`, `producer-epoch` and
  `producer-seq` headers; `producer(...)` on a collection in the SDK). The server
  recognises the producer's retry and answers it `duplicate: true` with
  the original position instead of storing it twice; or
- read the stream and check whether the record is there before you send
  it again; or
- resend only if a second copy is acceptable to you.

The SDK does this for you: it replays a `429` or a `503` whose body says
`"retryable": true` on its `Retry-After` (capped at 5 s), at most four
attempts in all; a `503` that carries `Retry-After` (a deploy's or a
restart's `temporarily_unavailable`) it keeps replaying until 15 s have
passed since the first attempt, each wait 1 to 5 s, so a call rides
through a restart of up to about 9 s (edge change #177). It rejects a
plain append whose outcome is unknown with `AppendOutcomeUnknownError`
instead of sending it again. Source: `sdk/README.md` ("In plain words"),
`sdk/src/index.ts`; edge changes #147, #153, #154 and #177.

## What the beta does not give you

- **One server per cell, with no spare.** A deploy, a restart or a crash
  of that server stops every project on the cell until it is back. While
  it starts, requests get the platform's `404` or a `503`. Measured on
  the beta's scratch cell: 7 to 9 s without a successful append or read
  per restart or deploy, `503 temporarily_unavailable` with `Retry-After`
  meanwhile on reads and appends alike (reads since edge change #178),
  which the SDK rides through (above). Since edge change #179 the old
  server gives the cell up for good once the new one has taken it, so the
  two no longer take it from each other in turn (the 7 to 9 s above were
  measured before #179; the new window is not yet measured); the
  old server answers what still reaches it `503 temporarily_unavailable`
  with `Retry-After: 1`, nothing written. Since edge change #180 a request
  in flight when the old server is stopped is answered, not cut off (up to
  10 s; a live subscription or a long-poll is ended then). A plain append
  in flight at the switch can still be answered `503 outcome_unknown`
  (field: up to two per switch, none of them stored); appends through a
  producer never are.
- **No SLA.** There is no availability or latency commitment. Append
  latency includes a write to object storage and waits for the next
  flush, up to 100 ms; it has not been measured again on the beta's
  shape. Source: `docs/OPS-RELEASE.md` ("Ack SLO").
- **No backups.** Data is as durable as the object store that holds it,
  and nothing more, until Prisma decides a backup posture. See
  [BETA-DATA-POSTURE.md](BETA-DATA-POSTURE.md).
- **The cell is kept awake.** It does not sleep when idle, so after a
  quiet spell there is no wake delay (edge change #140). This is how the
  beta runs, not a promise for later.
- **No invoices.** Usage is metered and shown, not charged.
- **No physical erasure on delete yet.** A deleted stream is gone from
  the API at once; its encrypted bytes stay in the bucket until
  reclamation is built. See [BETA-DATA-POSTURE.md](BETA-DATA-POSTURE.md).
- **Limits can change.** The ceilings above are restated after the
  envelope measurement, and the cell's endpoint may change before the
  beta ends.
