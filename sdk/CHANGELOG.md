# Changelog

`@prisma/streams` is versioned with the Prisma Streams server: each
release is packed from the release candidate that certified the server of
the same version, and published from that candidate's tarball.

## 0.3.0-beta.1

The first public beta, published under the npm dist-tag `beta`
(`npm install @prisma/streams@beta`). It needs a Prisma Streams server of
the same version (storage layout 5; data written by earlier previews is
not carried over).

### Breaking

- A plain append (`append`, `appendBytes`, `appendMany`) whose answer
  leaves its outcome unknown rejects with the new
  `AppendOutcomeUnknownError` (`code: "outcome_unknown"`,
  `retryable: false`, the server's answer or the transport failure as
  `cause`). That covers a transport failure, which was
  `StreamsTransportError` with `retryable: true`, and a `408` or a `5xx`
  other than `503`, which were the server's `StreamsError`. It also covers
  a `503` whose JSON body does not itself say `"retryable": true`, which
  the SDK used to send again up to three times: the server's
  `outcome_unknown`, its answer of this version for a plain append that a
  deploy or a restart caught in flight (it was `temporarily_unavailable`,
  `retryable: true`, and a replay could store the record twice), and a
  `503` from in front of the server without its body. A `503` the server
  marks retryable wrote nothing, and the SDK still replays it. The SDK does
  not send the append again after such an answer. Code that caught
  `StreamsTransportError` around a plain append no longer catches it
  there; append through a producer for a retry the server deduplicates.
  A producer append whose replay after a transport failure the server
  refuses (a fence by another writer of its producer id, a seal, a quota)
  rejects with `AppendOutcomeUnknownError` as well, the refusal as
  `cause`, and `autoClaim` claims no new epoch for it. Its message names
  the recovery for the refusal (README, "Producer state ownership"): after
  a fence, read the stream and send the value again only if it is absent,
  under an epoch above the fence's (`autoClaim` claims one; `bumpEpoch()`
  can land on the fence's); after a seal, read the stream, which is final;
  after any other refusal, send the same value through the same producer
  before any other value on its routing key, which the server answers
  `duplicate: true` if it was written (through a client for the new
  endpoint after a `421`). A plain append
  answered `2xx` whose answer cannot be read rejects with `StreamsError`
  `unreadable_response` (`retryable: false`): it was written. A producer
  append answered so, which rejected with the runtime's `SyntaxError` or
  `TypeError`, is sent again with the same producer id, epoch, sequence
  and body, as after a transport failure, and resolves with the server's
  answer (`duplicate: true` with the original cursor if the first attempt
  was written; a refused resend is the `AppendOutcomeUnknownError` above).
  Only when its last attempt's answer cannot be read either does it reject,
  with `StreamsError` `unreadable_response` (`retryable: true`), the
  producer keeping its sequence: send the same value through it before any
  other value on that routing key. A token provider's failure is never
  classified as an append's unknown outcome.
- `client.stream()` throws, and `client.createStream()` rejects before any
  request, with `StreamsError` `invalid_encryption_key_format` (`status` 0,
  `retryable: false`, the key not shown) for a key that, the whitespace
  around it trimmed, holds anything but printable ASCII without spaces, or
  that is not a string. Such a key was sent: `fetch` refused a control
  character or a character above U+00FF with a `TypeError`, which quoted
  the whole key for a line break, and the SDK replayed it as a transport
  failure (a plain append rejected with `AppendOutcomeUnknownError`
  although nothing was sent; a subscription retried for ever); the server
  refused any other such key, except on a route that reads no key, such as
  `metadata()`.
- A routing key, a producer id or a bearer token that a header cannot carry
  byte for byte (anything but printable ASCII and tabs, whitespace at
  either end, a routing key over 1,024 characters, an empty producer id or
  token, or one that is not a string) is refused before any request,
  nothing trimmed (README, "Routing keys, producer ids and tokens"): an
  append (`append`, `appendBytes`, `appendMany`, `Producer.append`,
  `Producer.appendMany`) and `seal()` reject with `StreamsError`
  `invalid_routing_key`, `stream.producer(id)` throws `invalid_producer`,
  and `new StreamsClient({ token })` throws, a request whose
  `tokenProvider` returned such a token rejects, and `connect()` rejects
  for such an issuer token, with `invalid_token_format` (each `status` 0,
  `retryable: false`, the value not shown). Such a value was sent: `fetch`
  stripped whitespace at either end without a word, so `"k\n"` was written
  under `"k"` and producer `"p "` shared producer `"p"`'s sequence (a
  record could be answered `duplicate: true` unwritten), and a seal's
  `"k "` made a lane no append could write; `fetch` refused a control
  character inside or a character above U+00FF, which the SDK reported as
  a transport failure (a plain append's as `AppendOutcomeUnknownError`
  although nothing was sent, a producer append's after 7 s of replays);
  the server refused the rest. A `tokenProvider` that returned no token
  failed every later request without being asked again; it is now asked
  again. An empty or absent routing key is still the default lane, and an
  empty `token` still sends no `Authorization` header.

### Added

- `StreamsClient.connect({ credential, issuerUrl })` builds a client from
  one credential exchange: its base URL is the issuer's `endpoint` (which
  must be `https:` or a loopback host unless `allowInsecureEndpoint` is
  set; `url` overrides it) and its project the credential's. It exchanges
  again at 80% of a token's lifetime and after a `401`, follows a new
  `endpoint` from the exchange that names it, and keys producer state on
  the issuer URL, so a move keeps every producer's sequence.
- `prismaStreamsCredential(credential, { issuerUrl })`, the token source
  `connect` is built on: `POST {issuerUrl}/v1/token/streams`, a `429`,
  `503` or transport failure replayed within four attempts of at most
  10 s each, any other refusal rejected as `CredentialExchangeError` with
  the issuer's code. A credential that is not
  `prisma_streams_<id>.<secret>` is refused before any request
  (`invalid_credential_format`).
- A `tokenProvider` may return `{ token, expiresAt }`. The first request
  after 80% of such a token's lifetime (two minutes before expiry when that
  is earlier) starts a refresh beside it: no request waits for a refresh
  while the cached token is valid, a failed refresh is tried again 30 s
  later, and a request after expiry waits for one. No timer is started.
- `generateEncryptionKey()`: a new stream key, 32 bytes from WebCrypto as
  43 characters of unpadded base64url.
- `client.usage({ month })`: the project's metered usage for the current
  or a given UTC month (`streams.usage.read`; a stream-prefix-restricted
  credential is refused `prefix_denied`).
- A request whose `fetch` fails is sent again after 1, 2 and 4 s, within
  the four attempts it shares with the `429`/`503` replay;
  `retry: { transport: false }` turns that off. A producer append is sent
  again with the same producer id, epoch and sequence, which the server
  deduplicates (a replay it refuses rejects with `AppendOutcomeUnknownError`,
  above), before the producer's later operations on its routing key.
  A batch settlement is sent again with the same decisions; a settlement
  the SDK replays (after a transport failure or a retryable `429` or `503`)
  reports an ack its first attempt applied as `stale` (no record is
  acknowledged twice), and applies a retry or an extend that attempt
  applied again, its delay or visibility counted from the replay. Plain
  appends, stream deletion and a subscription's reads
  are not replayed after a transport failure.
- The consumer iterator (`for await`, `messages()`) and
  `watch.subscribe()` reconnect after a transport failure or a retryable
  `429`/`503` (after 1 s, doubling to at most 5 s) instead of ending.
  Aborting a watch subscription ends it.

### Fixed since 0.2.0-rc.4 (2026-08-31)

- `read()` and `subscribe()` send a space in a routing key as `%20`. They
  sent `+`, which the server reads as a plus, so a key with a space read
  the lane of the key with a `+` in its place: an empty lane, or another
  key's records.
- Concurrent `401` answers for one cached credential share one refresh
  (R19, 6a79dfb3).
- A producer's state mutations are serialized per scope (R20, 29daf449).
- The consumer iterator settles recorded decisions on cleanup (R21,
  6573da9c).
- Cancelling a request no longer cancels a credential refresh other
  requests share (R21-A, d48f5970).
- `wrong_cell` is classified before any retry or credential refresh (R22,
  544deee5).
- `PullOptions.max` documents that a pull can lease a routing key's run
  (f64ea934).

### Documentation

- Security notes: one append request's records are compressed together
  before they are encrypted, so keep secrets and attacker-controlled data
  in separate appends when the storage operator is outside your trust
  boundary.
- Consumer batches and per-key order: how to keep a routing key's order
  when a batch holds its run.

### Server and SDK changes a client can observe (edge records)

Every client-visible change since 0.2.0-rc.4 has a numbered record, with
the condition, the answer before and after, the retry semantics and the
tests that pin it, in `docs/reviews/2026-09-hardening/edge-changes.md`
of the Prisma Streams repository; this list is generated from that
file's index, SDK-facing records first, then by surface. A record's
risk grade (high, medium, low) is the hardening program's.

#### The SDK (also described above)

- #135 (low): The `421 wrong_cell` refusal's message has one space between "the" and "project's" (it had 14), byte-identical to the SDK's `WrongCellError` text
- #147 (low): The SDK's plain appends reject a transport failure, a 408 or a 5xx other than 503 with `AppendOutcomeUnknownError` (`outcome_unknown`, `retryable: false`), and the SDK never sends one again after that answer, where they rejected with `StreamsTransportError` (`retryable: true`) or the server's `StreamsError`
- #148 (low): The SDK replays a request's transport failure after 1, 2 and 4 s within the four attempts it shares with the 429/503 replay (producer appends with the same producer id, epoch and sequence, settlement with the same decisions; not plain appends, stream deletion or a subscription's reads), `retry: { transport: false }` turns that off, and the consumer iterator and `watch.subscribe()` reconnect after a transport failure or a retryable 429/503 instead of ending; aborting a watch subscription ends it; a producer append whose replay the server refuses after a lost answer rejects with `AppendOutcomeUnknownError` (`outcome_unknown`, `retryable: false`) and `autoClaim` claims no epoch for it
- #154 (medium): A plain SDK append replays a `503` only when its JSON body says `"retryable": true` with a code other than `outcome_unknown`, and rejects any other `503` with `AppendOutcomeUnknownError` after one request
- #155 (low): The pre-auth survival refusal says `"retryable": true`, so a plain SDK append replays it, as it replays every other refusal that wrote nothing
- #165 (low): A producer append answered `2xx` whose answer the SDK cannot read is sent again as a lost answer, inside the transport replay, and resolves with the server's answer; it rejects with `StreamsError` `unreadable_response`, retryable, only when its last attempt's answer cannot be read either
- #166 (low): The SDK refuses a stream key that, the whitespace around it trimmed, holds anything but printable ASCII without spaces, or that is not a string, with `StreamsError` `invalid_encryption_key_format` before any request and without showing the key
- #167 (low): The SDK refuses, before any request, a routing key, a producer id or a bearer token that a header cannot carry byte for byte, with `StreamsError` `invalid_routing_key`, `invalid_producer` or `invalid_token_format`, without showing it
- #168 (low): The SDK's `read()` and `subscribe()` send a space in a routing key as `%20`, so a key with a space reads the lane its appends were written to

#### Access tokens and the interim token issuer (`connect()`, `prismaStreamsCredential`)

- #103 (medium): A token verifies only under a signing key the keys feed pins to its audience (`aud`, contract r7): the customer issuer's key no longer mints workload tokens for the internal surface, nor a fleet key customer tokens, and one key material signs for one audience (amended)
- #149 (medium): The interim token issuer exchanges a customer credential's secret for a signed access token of its project at `POST /v1/token/streams`, refusing a malformed or unknown credential 401 `invalid_credential`, an inactive credential or project 403, a bad body 400 `invalid_request`, more than its rate limits 429 `rate_limited` with `Retry-After`, and minting nothing (503 `issuer_unavailable`, `Retry-After: 5`) while its copy of the state is over 300 s old or its key is not the cell's
- #151 (medium): The interim token issuer's management API serves a browser signed in to a Prisma workspace on the beta's allow-list: `GET /v1/workspaces/{w}/streams/projects` lists the workspace's Streams projects, `POST` creates one, placed on the cell at once with a first credential whose secret the answer shows once, `GET …/projects/{p}` says whether the cell serves it yet (`ready`, probed with the project's own credential), and `DELETE …/projects/{p}` revokes its credentials and leaves it `deleting` until the issuer's offboarding job has deleted its streams and omitted it (about 2 to 3 minutes later for a small project, longer for one with many streams or behind another project's drain); `GET` and `POST /v1/projects/{p}/streams/credentials` list a project's credentials and create one, its secret shown once, `POST …/credentials/{c}/rotate` gives one a new secret at the next `grant_version`, `POST …/credentials/{c}/revoke` revokes one for good, and `GET …/credentials/{c}` says whether the cell serves its last change (`served`); a call without a valid session is refused 401 `unauthorized`, a change without JSON and the session's CSRF value 403 `csrf_failed`, another workspace 403 `forbidden`, a project create, credential create or rotation of a workspace off the allow-list 403 `workspace_not_allowed` (its reads, revokes and deletes stay open: the reduced mode, #169), a full beta or workspace 409 `beta_at_capacity` / `project_limit`, past its rate limits 429 `rate_limited`, and while the issuer's copy of the state is stale 503 `issuer_unavailable`
- #152 (medium): The interim token issuer signs a browser in with Prisma: `GET /auth/login` sends it to Prisma's authorization server (OAuth 2.0 authorization code with PKCE, scope `workspace:admin`, a 10-minute state cookie), and `GET /auth/callback` exchanges the code, reads `/v1/me` once, drops the access token, and gives a browser whose user signed in to a workspace on the beta's allow-list (or off it while it holds a Streams project: the reduced mode, #169) an 8-hour session of that workspace; a refused sign-in is sent to the page with a code (`denied`, `expired`, `failed`, `not_oauth`, `no_user`, `no_workspace`, `invalid_workspace`, `workspace_not_allowed`, `unavailable`); more than 30 sign-ins a minute are refused 429 `rate_limited`; `GET /auth/session` tells the page who is signed in, whether its workspace is on the allow-list (`allowListed`) and the session's CSRF value (401 `unauthorized` without a session, 503 while the list cannot be read), and `GET /` serves the issuer's one page, static HTML under a hash-only CSP, which shows the workspace's project, endpoint, readiness and credentials, creates, rotates and revokes credentials with each secret shown once, deletes the project once its name is typed, gives the SDK snippet, says who may manage, and offers a workspace off the list only its reads, revoke and delete
- #169 (medium): A Prisma workspace off the beta's allow-list that still holds a Streams project keeps a reduced mode at the interim issuer: its users sign in, see its projects and credentials, revoke a credential and delete a project, and are refused a project create, a credential create and a rotation 403 `workspace_not_allowed`; only those three read the list; sign-in tells whether the workspace holds a project from the issuer's state copy (`unavailable` while it is stale); `GET /auth/session` gains `allowListed` (and 503 while the list cannot be read), and the page shows the reduced mode
- #170 (low): The interim issuer lists a project's credentials in the order they were made, by a creation sequence its state records, where two made in one millisecond were listed in the order of their random ids
- #171 (low): The interim issuer's `ready` and `served` say why they cannot turn true (`no_live_credential`, `project_suspended`, `project_deleting`, `credential_expired`) and probe nothing then, a project is proven ready through any of its live credentials, an expired one passed over, and the page asks every 5 s for a minute and every 30 s after, waits out `Retry-After`, and stops on a reason, a failure or ten minutes, with a Retry

#### The product API (`/v1/streams`, what this SDK calls)

- #1 (high): Non-text routing key is refused on both product writers
- #2 (high): Watch wait proves shard ownership before it parks
- #3 (low): Consumer pull honours its deadline under a blocked dead-letter handoff
- #4 (low): Postings cache no longer over-claims or publishes stale coverage
- #5 (low): Billing tombstone walk covers every project in the cell
- #10 (medium): Settle retry delay and extend visibility are clamped to 12 h
- #15 (low): Catch-up through a reopened sealed span is served
- #19 (medium): Read-byte quota on :scan
- #20 (medium): Read-byte quota on consumer :pull
- #24 (medium): Relayed read verdict is typed on the fleet page route
- #29 (low): Product create that loses the recreate CAS to a live forked incarnation is idempotent
- #46 (medium): Live SSE lease ends when its project is placed on another cell
- #53 (high): Usage `?streamId=` is served only for an incarnation of the URL's name
- #56 (high): Project usage totals need an unrestricted stream grant
- #57 (high): A seal carrying a final record needs records.append as well as lifecycle.manage
- #59 (low): A usage month with a sign is refused as `invalid_month`
- #64 (low): A product handler's own descriptor read that the store fails answers a retryable 503; corruption stays a 500 that is not retryable
- #92 (low): A seal with a final record answers its 200 with Cache-Control: no-store, as a plain seal does
- #95 (low): The settle, the watch definitions and the watch wait answer their 200s with Cache-Control: no-store, as WIRE-MATRIX §2.16 to §2.18 list
- #100 (medium): On a cell shared k ways a project's quota on each shared bound is at most the bound ÷ k, and a 0 or missing quota is exactly that share; installed at boot since #130 (amended)
- #101 (low): A cell never serves a policy that names the system project, its deployment's PROJECT_ID or (k > 1) a workspace equal to its ACCOUNT_ID; its tokens get 421 wrong_cell and /v1/debug/auth counts the drops (amended)
- #104 (low): Under enforce, a watch observation that carries a capability must verify it: a stream key observes only beside a verified token, never in place of a failed capability; the 403 says so (amended)
- #105 (medium): A waiting consumer pull walks its lineage again only when a record became durable, the consumer's queue state moved or one of its leases expires, never every 50 ms
- #108 (medium): A page read reserves its page's memory before it runs, under the instance's read memory and its project's memory line, waits up to 2 s for room, then answers 429 project_memory_pressure or 503 read_memory_busy; /operator/data.json reports the read memory
- #109 (low): A write sees its project's held read bytes and parked waits in its memory pressure (model version 2); /v1/debug/load reports version 2 and parked_weight_bytes
- #110 (medium): A project on a single-server shared cell runs under the checked-in shared-cell profile and the ceilings cell-admin publishes for it (bound ÷ 8 on every bounded axis, an 8 MiB LiveFeed share, a 33,554,432-byte memory line, 8 MiB bodies, the release posture) (amended)
- #111 (low): Design-partner tokens on a shared cell are minted by cell-admin from the cell's one key file, for at most 24 h, one credential per project with a random id
- #112 (medium): cell-admin offboards a project: its credentials are refused, every stream its catalog lists is deleted until a walk after the settle time lists none, then it is omitted and its id is never placed again
- #113 (medium): A woken long-poll takes its page's read reservation again before it reads; one whose page finds no room before its wait ends answers its ordinary timeout (204, cursor held)
- #114 (low): A consumer pull reserves the coverage its walk reads (4 MiB) at admission and takes it again after a wait: 429 project_memory_pressure or 503 read_memory_busy after 2 s, and a woken pull without room answers empty; its body is buffered before its coverage is reserved (amended)
- #115 (medium): A product write's body is refused 429 project_memory_pressure while it is buffered, once it would take its project past its memory line while the project holds other bytes, never its own request's coverage reservation (amended)
- #117 (low): A server that meters usage and does not set TELEMETRY_DRAIN_SECS runs its usage drain every 8 s, not every 2 s: the usage answers trail a write by up to about 10 s and a read by up to about 20 s
- #118 (low): The per-stream usage answer's possibleReadLossWindowSeconds states the read flush interval plus one drain cadence, 18 at the defaults, not the flush interval alone (10)
- #121 (low): One usage drain round takes up to 256 of each shard's dirty rows, not 64: a shard that dirties more than 64 segments within one cadence reaches the usage answers in a quarter of the rounds
- #123 (medium): A consumer pull leases consecutive records of a routing key up to max, each under its own lease token, and the key stays blocked for every other pull while any of them is leased
- #130 (medium): Boot installs the cell ceiling from PROJECT_SHARE_K and the measured envelope: on a cell shared k ways each project is held to bound ÷ k on seven shared bounds, queued append bytes included, and PROJECT_ID is reserved on every cell
- #140 (low): A shared cell is kept awake: its profile sets KEEP_AWAKE=1, so after an idle spell a client neither waits for a wake nor meets the platform's 404 while one runs
- #141 (low): An anonymous capability carrier spends a store lookup only when the cell holds no current descriptor to check its capability against, under budgets of 20 a second per claimed project and 100 per cell, and one naming a project the cell does not serve reads nothing
- #143 (medium): A verified request pays for the registry store work its own task makes from its project's store budget, at the project's request rate a second, once it is served, and the project's creates, deletes, seals and catalog listings are refused 429 project_rate_limit while that budget is in debt
- #144 (low): A creation document over 64 KiB is refused 400 invalid_config, a catalog page reads at most 4 MiB of descriptors, each at most 128 KiB, and a split is declined once its stream's segment map could pass 192 segments
- #145 (low): A catalog listing reserves the page it may hold in its project's read bytes and the instance's read memory, as a page read does, and is refused 429 project_memory_pressure or 503 read_memory_busy when no room comes within 2 s
- #146 (low): A settle that dead-letters poisoned records reads them only under a 4 MiB reservation it takes without waiting in its project's read bytes and the instance's read memory; without room it dead-letters none of them, and the first pull after their leases expire hands them off
- #153 (medium): A plain product append whose group its retiring engine stranded is answered 503 `outcome_unknown`, not retryable, because it may or may not have been written; a producer-keyed append and a seal's final keep the retryable `temporarily_unavailable`
- #158 (medium): A project that stores at least its `max_stored_bytes` quota, by the figure the usage rollup publishes into admission's table, is refused new appends 429 `stored_bytes_limit` (not retryable, no `retry-after`, details `{dimension, limit, stored}`) before anything is charged, and project usage states the figure as `ownedStoredBytesNow`
- #162 (medium): A project's request bucket holds max(requests_per_sec, 50) tokens, so a project below 50 requests a second is admitted 50 requests at once and then its rate, and a concurrent burst of its registry writes, all admitted before any is debited, leaves its store budget up to five times deeper in debt (at 10 a second, 50 creates at once and about 54 s refused after them, where 10 left about 10 s)

#### The product and raw APIs

- #7 (low): Feed retry task no longer reads and publishes records
- #8 (medium): Append routed by a pre-transition descriptor is retried on the successor
- #9 (high): Stream-TTL and expiry.idle ceiling of 4,294,967,295 s
- #11 (low): Registry read that raced a write no longer re-installs what it read
- #12 (medium): HTTP/1.1 request-head and idle keep-alive deadline of 120 s
- #14 (medium): Panicking shard opener fails its open instead of wedging its prefix
- #16 (medium): Live SSE tail on a retired engine gets a typed EngineRetired cutoff
- #17 (low): Billing-sweep close arms only the 3 s base holdoff
- #26 (medium): Append larger than a fresh bucket is 413; boot refuses unadmittable limits
- #30 (high): Undecodable producer or Stream-Seq lane row refuses the append
- #31 (low): Failed or empty live SSE read is retried on a backoff
- #34 (medium): Split or merge the segment map refuses keeps its intent pending
- #35 (low): Empty catch-up page is re-read after a bounded wait
- #42 (low): Typed SourceReadError for SSE source reads
- #54 (low): The transition retry's re-preparation answers an unreadable registry as retryable
- #58 (low): An append's first registry read that fails on the store answers retryable 503; a corrupt descriptor stays 500
- #60 (high): Segment lineage is ordered by allocation, not by wall clock, on every surface
- #65 (medium): Replaced and dead storage stops billing in a fleet, at the instant it ended
- #66 (medium): A storage close that reaches the rollup after its month was finalized corrects that month to the close instant, and no later month bills the closed segment
- #68 (medium): A server that sets no shed line refuses appends above 500 MiB of memory, the line production runs
- #69 (low): A server that sets neither feed budget retains up to 64 MiB of shared-feed data, and one project may hold half of the cell
- #70 (medium): A server that does not set SSE_MAX_CONNECTIONS admits 1,200 live subscriptions
- #71 (high): A server that does not set MAX_RECORD_PAYLOAD_BYTES refuses a record over 131,072 bytes
- #72 (low): A billing close of a row that is already closed through its instant writes nothing
- #73 (low): A storage close that a Db which had already failed refused is settled as closed; the shard reopens instead of answering shard_closing until restart
- #79 (medium): A server that sets neither admission cap sheds appends above 512 requests in flight and admits 256 concurrent appends to one stream
- #80 (low): Fourteen settings that nothing sets are constants: their arguments are refused and their environment names are ignored
- #85 (medium): Ingest is metered for every customer stream: BILLING_METER is not read
- #87 (low): A billing close or retention flag that the tombstone walk or the closure-debt pass enqueues on a shard it opened is applied: the pass keeps the shard until its committer has applied it
- #89 (medium): A takeover coordinated on an instance that does not own the old final's segment fences it at the owner, and every answer but the owner's parsed closed-report leaves the seal resumable
- #91 (low): A billing close or retention flag a shard drops unapplied is refused, logged and counted, and the tombstone walk and the closure-debt pass replay it at the next sweep
- #102 (low): A stale-feed refusal wakes the auth refresher, at most once per 30 s, so a cell that slept serves again after one out-of-cadence pass instead of at its next tick; its 503s carry Retry-After: 1 (amended)
- #106 (low): A request waiting for data is parked, not in flight: waiting long-polls, consumer pulls and watch waits no longer shed writes, trip the survival bound or ask the fleet for servers; /operator/data.json adds local.parked
- #107 (medium): A client that accepts less than 16 KiB of a pending response in 10 s is disconnected (every HTTP/1.1 connection, SSE included); the effective-configuration summary reports the floor
- #116 (low): Deleting a stream retires its incarnation's watch journal: its waiters are answered stale at once, and the journal's flusher ends with the stream; an expired stream's at the tombstone walk's close (amended)
- #119 (medium): While the server runs in a fleet (a published ring of any size) the scaler splits no stream: a hot stream stays one segment on its owner and meets its per-stream limit with 429
- #125 (medium): Usage and retention count stored page bytes: frame_bytes, the storage gauge and the stored-byte totals measure layout 5 pages, after compression
- #126 (high): A binary reads only storage layout 5: a namespace written under layout 4 is refused as unsupported_storage_layout
- #129 (high): A history read refuses pages that do not tile its window: an overlapping or a lost page fails the read as stored corruption
- #131 (medium): The deploy wrapper polls a cell's feed bundle every 15 s and rewrites the feeds on a change, so a publish needs no restart, and deletes them after 120 s of awake time without a successful poll, so a cell that cannot see its bundle (unreachable, or malformed and left published) refuses every request once its feeds go stale
- #133 (low): A watch journal idle for ten minutes, or whose stream expired and was closed by the tombstone walk, is retired: its flusher ends and a returning watcher is answered stale once
- #136 (low): An append whose shard engine closed between its resolve and its enqueue is answered raw 503 `shard_moving` and product 503 `temporarily_unavailable` with `Retry-After: 1`, as a moved shard, not 429 as a full queue
- #137 (low): A request that joins the billing sweep's open of a shard is adopted as the open lands, so it is served by that engine and the sweep cannot close it; one handed an engine retired before it resumed gets the retryable 503 `shard_closing` refusal as its route renders it, never the closed engine
- #161 (low): A long-poll its stream's split wakes and refreshes onto the published child waits there only what its first wait's deadline had left, so no long-poll waits more than 25 s in all, on the raw and product surfaces (a race: before, the refresh could wait a second whole budget, up to 25 + 25 s plus cold resolves)
- #163 (low): The server listens with an accept queue of 1,024 connections, not 128, so a burst of up to 1,024 simultaneous connects is queued whole instead of having its SYNs past the 129th dropped

#### The raw Durable Streams API (`/v1/stream`)

- #13 (low): Shared raw SSE session gets upToDate after a match-free batch
- #28 (high): Raw PUT never replaces a name retained for forks
- #33 (low): Stream-Seq over 65,535 bytes no longer corrupts the tail row
- #55 (medium): A raw close whose seal intent failed transiently answers retryable, not sealed
- #128 (low): format=frames answers uncompressed version 4 frames on every deployment, so no response length depends on how well a record compresses

The other 61 records (15 fleet-internal, 20 operator-debug, 26 process) change
peer routes, the operator surface (`/operator`, `/v1/debug`, `/health`,
logs and counters) or how a server starts, stops and is configured; a
client of this SDK sees none of them except through the records above.
