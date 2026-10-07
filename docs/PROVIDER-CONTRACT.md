# Object-store provider contract

The formal models TLA-001, TLA-002, TLA-003, TLA-005, TLA-011 and TLA-019
assume the object store's conditional writes behave as ASM-OBJSTORE-CAS
states, and TLA-011 also rests on ASM-SLATEDB-FENCE
(`verification/assumptions.md`). A model assumption does not qualify a
provider. This suite checks those assumptions against the real client, a
real endpoint and a real configuration, and then checks what the registry
and SlateDB do with the answers.

## Where it lives

`src/bootstrap/tests/provider_contract.rs` and its children:

| file | checks |
|---|---|
| `store_cases.rs` | the raw contract: competing creates and updates, stale, fabricated and missing-object preconditions, ETag presence and stability, user metadata, and deletes of an object and of a key that holds nothing |
| `registry_cases.rs` | `Registry::create`, `recreate` and `mutate_incarnation` through the store under test: missing ETags, lost replies, failed dispatches, a changed incarnation, racing registries |
| `slatedb_cases.rs` | SlateDB writer fencing with the server's own settings, written through the configured commit pipeline, and ambiguous WAL PUTs |
| `http_cases.rs` | s3lite only: 5xx answers before and after the emulator applied a PUT, below the S3 client: conditional PUTs are sent once and reach the registry and SlateDB as errors, unconditional PUTs are still retried |
| `faults.rs` | a one-shot fault wrapper above the client: a lost reply (the PUT is applied, the caller gets an error) or a failed dispatch (the PUT never reaches the store), and ETag stripping on reads |
| `s3lite_harness.rs` | the s3lite emulator served in-process on a loopback port, compiled from the binary's own `src/bin/s3lite/emulator.rs`, with an HTTP fault layer in front of it |

Every runner builds its stores the way the server does:
`ServerConfig::store_for` in `src/bootstrap.rs` (the `AmazonS3Builder`
with `S3ConditionalPut::ETagMatch`, the timing connector and store wrapper,
the pool settings and `PATH_PREFIX`, and `src/bootstrap/s3_store.rs`, which
sends conditional PUTs through a client that never retries and deletes each
key with its own `DELETE`; see [Deletes](#deletes)). The registry cases use the ops-bucket
store and the SlateDB cases the shard-bucket store, as in production. SlateDB
opens with `shard_settings`, the settings the server gives shard logs. With
group commit on (`WAL_GROUP_COMMIT=1`, the default), those settings make
SlateDB's own flush timer a 1 s failsafe, and the suite's writer flushes the
WAL explicitly after each write, as the shard's pump does; with it off the
timer flushes. The pump's gap and gather are scheduling inside the shard
engine and are not part of what the suite drives.

## Runners

| runner | when | store |
|---|---|---|
| `in_memory_store_meets_the_provider_contract` | every test build | `object_store::memory::InMemory` (the reference semantics) |
| `s3lite_through_the_production_client_meets_the_provider_contract` | every test build | s3lite over loopback HTTP, through the production client |
| `in_memory_store_meets_the_slatedb_contract_under_each_commit_pipeline` | every test build | `InMemory`, the SlateDB cases with `WAL_GROUP_COMMIT` 0 and then 1 |
| `s3lite_meets_the_slatedb_and_http_contract_under_each_commit_pipeline` | every test build | s3lite through the production client, the SlateDB and HTTP cases with `WAL_GROUP_COMMIT` 0 and then 1 |
| `s3lite_receives_every_delete_as_one_keyed_delete_request` | every test build | s3lite through the production client: the requests a delete sends, read from s3lite's request ledger ([Deletes](#deletes)) |
| `real_provider_meets_the_provider_contract` | `#[ignore]`d; runs only with `STREAMS_PROVIDER_CONTRACT=1` | the provider the environment names |

The first two run the whole suite under the default pipeline. The two
pipeline runners set the switch themselves, so both pipelines are covered
whatever the default is, and each first proves that the pipeline is what
made a write durable: under the pump SlateDB's own timer is set to 600 s for
that one database, so a writer that did not flush would time out. The
real-provider runner follows `WAL_GROUP_COMMIT` from its environment. The
first five run in `scripts/gate.sh` and CI (`cargo test --lib`):

```sh
cargo test --locked --lib provider_contract
```

Run with `--ignored` but without the opt-in, the real-provider test prints
`SKIPPED: ... nothing was qualified` and qualifies nothing.

## Running it against the production provider

The runner reads the server's own environment through the server's own
parser (`CliArgs`, then `ServerConfig::load`), so it qualifies whatever
configuration it is given. It requires the opt-in, an endpoint, an
explicitly named bucket and a prefix. Use a dedicated bucket, or a
dedicated prefix in a bucket no service writes under that prefix. Every
object goes under `PATH_PREFIX/provider-contract/<run id>/`, plus the
registry keys of one run-unique project. The run deletes what it wrote,
best effort, when it passes.

For Tigris (the endpoints and region are the ones in docs/STAGING.md and
docs/BUCKETS-SINGLE-REGION.md):

```sh
STREAMS_PROVIDER_CONTRACT=1 \
SLATE_S3_ENDPOINT=https://t3.storage.dev \
SLATE_S3_REGION=auto \
SLATE_S3_BUCKET=<dedicated bucket> \
PATH_PREFIX=provider-contract-$(date -u +%Y%m%d) \
SLATE_S3_ACCESS_KEY_ID=<key id> \
SLATE_S3_SECRET_ACCESS_KEY=<secret> \
scripts/test-leg.sh target/provider-contract/real.log \
  --exact bootstrap::tests::provider_contract::real_provider_meets_the_provider_contract \
  -- --locked --lib \
  -- --ignored --exact bootstrap::tests::provider_contract::real_provider_meets_the_provider_contract --nocapture
```

Set any storage knob the deployment sets (`POOL_IDLE_SECS`,
`STORE_BULK_INFLIGHT_MAX_BYTES`, ...) the same way; the runner reads them as
the server does. Old global buckets live on `fly.storage.tigris.dev`, and a
global bucket is a different configuration from a single-region one:
qualify each bucket kind, endpoint and client region the fleet uses.

A qualifying log contains `qualifying endpoint ... bucket ... prefix ...`
and `real-provider: provider contract passed; Observations { ... }`, and
`scripts/test-leg.sh` confirms that the named test ran. A log with
`SKIPPED` qualified nothing.

## What each case asserts

ASM-OBJSTORE-CAS (a), conditional create:

- 8 concurrent `PutMode::Create` of one path, in 3 rounds: exactly one is
  written and 7 answer `AlreadyExists`; the stored bytes are the winner's,
  and the PUT and GET ETags agree. A later create is refused and changes
  nothing.
- A create whose reply is lost is reported by `Registry::create` as an
  error, never as success. The caller's retry finds its own descriptor as
  a lost race (`(false, existing)`), not a second creation. A failed
  dispatch writes nothing.

ASM-OBJSTORE-CAS (b), conditional update:

- 4 concurrent `PutMode::Update` with one ETag, in 3 rounds: exactly one
  commits and 3 answer `Precondition`. The stale ETag is then refused
  and changes nothing. Any other answer (on S3 a 409 for concurrent
  `If-Match` writes arrives as `AlreadyExists`) may have committed and
  makes the round inconclusive.
- An update of a missing object answers `Precondition` and creates
  nothing. An ETag the store never issued answers `Precondition`.
- A GET and a HEAD of an existing object carry a non-empty ETag, and
  repeated reads of one version agree. Different contents never share an
  ETag. Every ETag the run sees names one content. Whether re-writing
  identical bytes reproduces an earlier ETag is recorded
  (`etag_repeats_for_identical_content`). That allows an A-B-A update
  only over byte-identical content, which the assumption permits.
- A read without an ETag (injected) makes `mutate_incarnation` return
  `MissingConditionalToken` and `recreate` fail, both before any PUT. The
  registry never falls back to an unconditional write.
- A lost reply to an applied update and a failed dispatch both make
  `mutate_incarnation` return `AmbiguousCompletion` after exactly one
  PUT, and only the first changed the descriptor. `recreate` reports
  both as errors, and a retried recreate declines against the incarnation
  that landed.
- A mutator bound to a replaced incarnation gets `IncarnationChanged`
  and writes nothing.
- 4 registries with separate caches (as separate processes) race
  non-idempotent `mutate_incarnation` calls on one descriptor for 3
  rounds. Every applied value is distinct, and the stored counter equals
  the number applied. On a real provider only, transport errors and
  provider 5xx count as ambiguous and widen that bound by their number.
- 4 registries race `recreate` of one dead incarnation: exactly one
  installs its epoch, and the others observe the winner.

ASM-SLATEDB-FENCE:

- A second `Db` writer on a path reads the first writer's durable data
  and fences it: the first writer's next write fails
  `Closed(Fenced)`, and the second writer does not see it.
- A WAL PUT that failed before dispatch is retried and lands.
- A WAL PUT whose reply is lost is retried by SlateDB and finds its path
  taken. If the store returns user metadata on HEAD
  (`metadata_round_trip`), SlateDB recognises its own put id and reports
  one success; otherwise it reports `Fenced`. Either way the batch is
  durable when the path is reopened. The suite measures the store's
  metadata behaviour first, then asserts the matching outcome.

Deletes:

- A delete of an object succeeds and the object is gone (a HEAD answers
  `NotFound`). A delete of a key that holds nothing succeeds too: SlateDB's
  garbage collector and the repository's sweeps may delete a key that is
  already gone, and take an error from a delete as a failed delete.
- s3lite only: each delete, of one key, of a key that holds nothing and of
  two keys in one delete stream, reaches the emulator as one
  `DELETE /<bucket>/<key>` per key and no `DeleteObjects` POST
  ([Deletes](#deletes)).

Rounds that a transport error makes inconclusive are retried with fresh
names, at most 4 attempts per round. A transport error on InMemory or
s3lite is a failure.

## What passing establishes, and what it does not

A pass of the real-provider runner is qualification evidence for
ASM-OBJSTORE-CAS and the store half of ASM-SLATEDB-FENCE. It holds for
one endpoint, bucket, credential scope, client region, client version
(`object_store` 0.14.1, SlateDB 0717cc1) and server configuration, at the
time of the run. Keep the log with the commit it ran at.

It does not establish:

- The behaviour of another endpoint, bucket kind (global or single-region),
  region or provider, or of this one after a provider change. Re-run it
  after such a change and after a client or SlateDB upgrade.
- Behaviour under load, partitions or provider incidents. The races are
  8 and 4 writers from one process for 3 rounds; the suite samples, and
  it proves nothing.
- Anything about lost replies at the HTTP layer of a real provider. The
  suite injects lost replies above the client on every backend, and at
  the HTTP layer on s3lite only.
- Durability, read-after-write visibility across regions, multipart
  conditional writes (the WAL and descriptors use single PUTs) or cost.
  In particular, which requests a provider bills, `DeleteObjects` included;
  the request shape of a delete is asserted on s3lite only.
- Anything that lifts the performance, cryptographic, deployment or
  evidence-upload holds.

InMemory and s3lite passing only shows that the suite and the repository
code agree with those stores. s3lite stores no user metadata, and both use
counter ETags, so the metadata and A-B-A branches differ from S3-style
providers. Only a real run exercises those branches.

## Deletes

The server deletes each object with its own `DELETE /<bucket>/<key>`
(`with_disable_bulk_delete(true)` in `src/bootstrap/s3_store.rs`).
`object_store` 0.14.1 otherwise sends every delete, even of one key, as a
`DeleteObjects` request: `POST /<bucket>?delete` with an XML list of up to
1,000 keys, a `Content-MD5` and a SHA-256 of the body. Every delete the
server issues is of one key: SlateDB's garbage collector deletes WAL SSTs,
compacted SSTs, manifests and compaction files one key per call (8
concurrent per collector task), and the repository's own deletes
(`src/registry/replaced.rs`, `src/registry/fork_debt.rs`, the boot canary
in `src/bootstrap.rs`) are of one key each. So the switch sends as many
requests as before, each without a body. A delete stream of several keys
(only the suite's own cleanup) sends one `DELETE` per key, at most 20 at a
time, where it sent one POST per 1,000 keys.

Why: Tigris's pricing does not bill a `DELETE` and does not name
`DeleteObjects`. If Tigris bills that POST as the write it is, every object
the collector removes costs a Class A request (the K2 cost harness prices
it both ways, `bench/k2cost/README.md`). The retries are the same: both
requests are retried after a 5xx, 429 or 408, and neither after a
transport timeout. A provider that answers a keyed `DELETE` of a missing
key with 404 would surface `NotFound` to the caller, where `DeleteObjects`
reports such a key as deleted; S3 and s3lite answer 204, and the
real-provider runner asserts the provider's answer (the store case
above).

Not established by the suite: whether Tigris bills `DeleteObjects` at all
(the saving rests on it), and the provider's answer to a missing key's
`DELETE` until the real-provider runner has run against it. What an
operator sees change in `GET /v1/debug/store` is edge change #96
(`docs/reviews/2026-09-hardening/edge-changes.md`).

## Findings

**F1 (fixed): the S3 client turned a committed conditional PUT into a
refusal.** `object_store` 0.14.1 re-sends a failed request inside one
call (`client/retry.rs`). A conditional PUT is re-sent after a 5xx, 429 or
408, after a connection that closed before the whole reply arrived
(`HttpErrorKind::Request`), and, for an update, after a 409
(`retry_on_conflict`, set only for `If-Match`). The retry carries the
original `If-None-Match: *` or `If-Match`. If the first attempt was
applied, the retry is refused, and the caller receives `AlreadyExists` or
`Precondition` for its own committed write. The budget is the client's
`RetryConfig` (default 10 retries within 3 minutes); the `idempotent` and
`retry_on_conflict` flags are crate-private, and no per-request option
turns retries off. So ASM-OBJSTORE-CAS (b) could hold for every HTTP
request at the provider while, at the client API, `Precondition` did not
mean "not committed". On e15ebef, through the production client on
s3lite:

- One `mutate_incarnation` call applied a non-idempotent decision twice
  (the counter moved 1 to 3, and the call reported `Applied(3)`).
- `create` reported `(false, own descriptor)`, a lost race to itself.
- `recreate` reported `(false, own incarnation)`, a decline against its
  own write.

The caller audit found the worst production consequence in stream
creation: a recreate with a body, `close` or fork that "declined" against
its own new incarnation answered success without writing the body and left
the stream initializing. Other callers re-decided harmlessly, reported a
wrong outcome (200 for 201, a skipped quota release, a seal claim reported
as not installed) or skipped a follow-up the reconciler repairs.

The fix is in `src/bootstrap/s3_store.rs`. The server's store holds two
`AmazonS3` clients over one HTTP client (one connection pool, the same
timing connector). Every conditional PUT (`PutMode::Create`,
`PutMode::Update`) and every `CopyMode::Create` goes through the client
with `max_retries: 0`. Reads, lists, deletes, unconditional PUTs and
multipart uploads keep the default retries, because repeating them cannot
turn a success into a refusal. So `AlreadyExists` and `Precondition` are
the provider's answer to the one request that carried the precondition,
and mean "not written" exactly as ASM-OBJSTORE-CAS states. Every other
failure reaches the caller as an ordinary error, which each caller
already treats as possibly committed: the registry answers
`AmbiguousCompletion` (`create` and `recreate` an error), and SlateDB's
retrying store retries the WAL or manifest PUT itself and checks its put
id (F2). `http_cases.rs` asserts the fixed outcomes on s3lite: a 5xx
before or after the PUT landed is an error, never a refusal;
`mutate_incarnation` applies once and reports `AmbiguousCompletion`;
`create` and `recreate` report an error, not a race with themselves; an
unconditional PUT answered 5xx is still retried. Each of these assertions
fails if the conditional client is given the default retries.

The cost is availability, not safety. A transient 5xx or 429 on a
conditional PUT is no longer absorbed: the registry caller answers an
error (usually a retryable 500 or 503), the startup canary and the
topology create fail boot, and the fleet document write converges on the
next tick. SlateDB is unaffected, because it retries. A caller's own retry
after such an error sees its landed write as a lost race or a decline,
exactly as after a lost reply; that is the ambiguity every caller already
had to handle, now reported instead of hidden.

**F2: SlateDB recognises its own landed PUT when metadata round-trips.**
The pinned SlateDB's retrying store attaches a `slatedbputid` metadata
attribute to every conditional PUT. After `AlreadyExists` or
`Precondition`, it HEADs the object and reports success if the id is its
own. ASM-SLATEDB-FENCE says a landed PUT whose reply was lost "is
retried and reported Fenced". That is true only for stores that drop user
metadata (s3lite). On InMemory, and on any provider that returns
`x-amz-meta-*` on HEAD, the same event is one success. Both outcomes are
safe, because the batch is durable once and there is no second success,
but the assumption text describes only one of them. The real-provider
run records which one the provider gives.

**F3: the local backends are not S3-shaped in two respects.** s3lite
keeps no user metadata. Both s3lite and InMemory issue counter ETags, so
identical content never repeats an ETag. S3 ETags for single PUTs are
content hashes and do repeat. The ETag ledger and the A-B-A branch are
exercised meaningfully only by a real provider.

`src/bin/verify.rs` `cas` and `fence` print what they observe but assert
nothing. This suite supersedes them as evidence.
