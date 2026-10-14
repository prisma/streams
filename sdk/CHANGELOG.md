# Changelog

`@prisma/streams` is versioned with the Prisma Streams server: each
release is packed from the release candidate that certified the server of
the same version, and published from that candidate's tarball.

## 0.3.0-beta.1 (unreleased)

The first public beta, published under the npm dist-tag `beta`
(`npm install @prisma/streams@beta`). It needs a Prisma Streams server of
the same version (storage layout 6; data written by earlier previews is
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
  append answered so rejects the same way, where it rejected with the
  runtime's `SyntaxError` or `TypeError`, and the producer keeps its
  sequence: send the same value through it before any other value on that
  routing key, which the server answers `duplicate: true`. A token
  provider's failure is never classified as an append's unknown outcome.

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
