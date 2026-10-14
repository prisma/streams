# @prisma/streams

The Prisma Streams SDK: encrypted, append-only collections with routing
keys, producer sessions, consumer groups and watches. Zero dependencies.

Prisma Streams is in public beta for Prisma Console accounts admitted to
it. The beta is metered, not charged (see "Usage" and "Beta terms and
support").

## Install

```sh
npm install @prisma/streams@beta
```

The beta is published under the npm dist-tag `beta`, so install it by that
tag: a bare `npm install @prisma/streams` is not guaranteed to resolve to
it.

## Quickstart

1. From the **Streams (beta)** link in Prisma Console, sign in on the
   Streams beta page, create a project and a credential. Copy the
   credential now; it is shown once. Keep the whole credential
   (`prisma_streams_<id>.<secret>`) in a server-side secret store as
   `PRISMA_STREAMS_CREDENTIAL`, and never ship it to a browser.
2. Create the stream's encryption key once with
   `await generateEncryptionKey()` and keep it in your secret store as
   `PRISMA_STREAMS_KEY`. Prisma never stores it: if you lose it, the
   stream's data cannot be read by anyone.
3. Connect, create a stream, append and read:

```ts
import { StreamsClient } from "@prisma/streams";

const client = await StreamsClient.connect({
  credential: process.env.PRISMA_STREAMS_CREDENTIAL!,
  issuerUrl: "https://<the Streams beta issuer>", // published with the beta
});
const orders = await client.createStream("orders", {
  encryptionKey: process.env.PRISMA_STREAMS_KEY!,
  format: { kind: "json" },
});

// A plain append that fails in transit, or with a 408 or a 5xx (a 503 only
// when the server does not mark it retryable), is not sent again: it rejects
// with AppendOutcomeUnknownError, and the record may or may not have been
// written.
await orders.append({ id: "o-1", total: 42 }, { routingKey: "customer-7" });

// Exactly once: a producer whose sequence state lives in your durable store
// (see "Producer state ownership"). The server deduplicates its retries.
const producer = orders.producer("checkout-service", { state: myStateStore });
await producer.append({ id: "o-2", total: 9 }, { routingKey: "customer-7" });

// Reads the routing key from its first record, then waits for new ones.
for await (const order of orders.subscribe({ routingKey: "customer-7", from: "beginning" })) {
  console.log(order);
}
```

`createStream` with the same configuration again answers the existing
stream, so the quickstart can run twice. `client.stream(name, {
encryptionKey })` opens a stream without creating it.

## Authenticating

`connect` exchanges the credential once. The client's base URL is the
issuer's `endpoint` and its project is the credential's. An explicit `url`
wins, and the client then never follows the issuer's `endpoint`; an issuer
that names no endpoint needs one (`endpoint_required`). An issuer's
`endpoint` must be `https:` or a loopback host (`localhost`, `*.localhost`,
`127.0.0.1`, `[::1]`), because every request carries the stream key:
anything else is refused before the first request (`insecure_endpoint`)
unless you pass `allowInsecureEndpoint: true`. Tokens are exchanged again
at 80% of their lifetime and after a `401`, as described under "Errors and
retries". When an exchange names a different endpoint (and no `url` was
given), the client moves there: every request made after the exchange
returns goes to it, a request that waited for the exchange (its token had
expired, or the server refused it `401`) included, while a request sent
when a refresh started beside it goes to the old one. An insecure endpoint
is not followed.

To build a client yourself, `prismaStreamsCredential` turns the credential
into a token source (this sketch exchanges twice; `connect` seeds its
token cache from the first exchange):

```ts
import { StreamsClient, prismaStreamsCredential } from "@prisma/streams";

const credential = prismaStreamsCredential(process.env.PRISMA_STREAMS_CREDENTIAL!, { issuerUrl });
const first = await credential.exchange(); // { accessToken, expiresIn, projectId, endpoint }
const client = new StreamsClient({
  url: first.endpoint ?? url,
  project: first.projectId,
  tokenProvider: credential.tokenProvider, // { token, expiresAt }: refreshed before it expires
});
```

Each exchange is `POST {issuerUrl}/v1/token/streams` with
`Authorization: StreamsCredential <credential>`. A `429` or `503` is
replayed on its `Retry-After` and a transport failure after 1, 2 and 4 s,
within four attempts. Any other refusal rejects at once with
`CredentialExchangeError`, whose `code` is the issuer's: `401`
`invalid_credential`, `403` `credential_not_active` or
`project_not_active`, `429` `rate_limited` and `503` `issuer_unavailable`
(both `retryable`), or `exchange_failed` when the answer names no code. A
success without a token is `invalid_exchange_response`. A credential that
is not `prisma_streams_<id>.<secret>` in printable characters without
spaces (a line break pasted into it, say) is refused before any request
with `CredentialExchangeError` `invalid_credential_format` (`status` 0, not
`retryable`); whitespace around it is trimmed. Neither the credential nor
a token is logged or put in an error.

Authentication (the token) belongs to the client; encryption (the
`encryptionKey`) belongs to the stream handle.

## Encryption keys

A stream's records are encrypted under a key you hold. Create one per
stream with `await generateEncryptionKey()` (32 random bytes from
WebCrypto, as 43 characters of unpadded base64url) and keep it in your
secret store. The service never stores it: if you lose it, the stream's
data cannot be read by anyone.

## Errors and retries

In plain words:

- A failure the server or the transport causes is a `StreamsError` with
  the HTTP `status` (0 when no answer arrived), a `code` (the server's, or
  the SDK's own, such as `transport_error`) and a `retryable` flag. An
  abort rejects with the signal's reason, and an error of your
  `tokenProvider` or producer state store propagates unchanged. A `2xx`
  whose body the SDK reads and cannot read (not JSON, or cut off
  mid-body) rejects with the runtime's own `SyntaxError` or `TypeError`,
  except an append's (below) and the credential exchange's, which is
  `invalid_exchange_response` ("Authenticating").
- The SDK replays a `429` or a `503` the server does not mark
  `retryable: false` (a plain append's `503` only when the server marks it
  `retryable: true`), on its `Retry-After`, and a transport failure of a
  request the server answers idempotently, after 1, 2 and 4 s: at most
  four attempts in all, and one more after a `401` refreshes the token.
- A plain append (`append`, `appendBytes`, `appendMany`) that meets a
  transport failure, a `408` or a `5xx` other than `503` is not sent
  again: it rejects with `AppendOutcomeUnknownError` (`retryable: false`),
  because the record may or may not have been written. So does a `503`
  whose JSON body does not itself say `"retryable": true`: the server's
  `outcome_unknown` (a deploy or a restart moved the shard while the
  append was in flight), or a `503` from in front of the server without
  its body. Retry such an append yourself only if a second copy is
  acceptable, or append through a producer, whose retry the server
  deduplicates. A `503` the server marks retryable wrote nothing, and the
  SDK replays it. A plain append answered `2xx` whose answer cannot be
  read rejects with `unreadable_response`: it was written, so do not send
  it again. A producer append answered so rejects with
  `unreadable_response` too, and the producer keeps its sequence: send the
  same value through it before any other value on that routing key, which
  the server answers `duplicate: true` (see "Producer state ownership").
- A producer append whose replay the server refuses (another writer
  fenced its producer id, the collection was sealed, a quota still refused
  it) rejects with `AppendOutcomeUnknownError` too, the refusal as its
  `cause`: the attempt whose answer was lost may have been written. Its
  message names the recovery for that refusal, and "Producer state
  ownership" gives each: read the stream after a fence or a seal, and
  send the same value through the same producer after anything else.
- Consumer, subscription and watch loops reconnect after transport
  failures and retryable `429`/`503` answers until you abort them; any
  other error ends them, and so does a consumer loop's failed settlement.

`WrongCellError` is never retried at the current endpoint, including the
503 fallback form. Resolve the project's new endpoint and create a client
for it; refreshing a valid credential cannot fix routing. Ordinary transient
429/503 responses retry with bounded, abort-aware backoff unless the server
explicitly marks the error permanent; a plain append's 503 retries only when
the server explicitly marks it retryable. Subscriptions use the same failure
classification and retain durable-cursor recovery for applied reads.

Fetch failures are exposed as `StreamsTransportError` with the original
failure in `cause`, except for plain appends (`append`, `appendBytes`,
`appendMany`): a transport failure, a 408, a 5xx other than 503, or a 503
whose body does not say `"retryable": true` rejects with
`AppendOutcomeUnknownError` (`retryable: false`), because the record may or
may not have been written and the SDK does not replay it; use a producer
for a retry the server deduplicates.

A request whose `fetch` fails is sent again up to three times, after 1, 2
and 4 s (abort-aware), within the same four attempts as the 429/503 replay,
and rejects with `StreamsTransportError` once they are spent. A producer
append is sent again with the same producer id, epoch and sequence, so the
server deduplicates a replay of an append that landed (`duplicate: true`; a
replay it refuses before that check rejects with `AppendOutcomeUnknownError`,
see "Producer state ownership"), and the producer's later operations on
that routing key wait for it. A
batch settlement is sent again with the same decisions: an ack is applied
once, a retry or an extend may be applied again (see "Consumer cleanup and
cancellation"). Plain appends and
stream deletion are not sent again after a transport failure and reject at
once; a subscription's request
is not replayed either, because the subscription reconnects itself. `retry: { transport: false }` turns the
replay off, for callers whose `fetch` failures are configuration (a wrong
URL, a blocked origin), not a restart. Consumer (`for await`,
`messages()`), subscription and watch loops reconnect after a transport
failure or a retryable 429/503 until aborted, whatever `retry` says:
consumer and watch loops after 1 s, doubling to at most 5 s and back to
1 s after a success, subscriptions after 1 s. They surface any other error,
and a consumer loop also surfaces a failed settlement. Aborting a watch
subscription ends it, as it ends a subscription. Credential-provider
failures propagate unchanged. Concurrent 401 responses for the same cached
credential share one refresh, and late responses cannot invalidate its
successor.

A `tokenProvider` may return `{ token, expiresAt }` (epoch milliseconds or
a `Date`) instead of a string. The first request made after 80% of such a
token's lifetime, or two minutes before it expires when that is earlier (a
3600 s token at 2880 s, a 600 s token at 480 s), starts a refresh and is
sent with the cached token at once. While that token is valid no request
waits for a refresh, even when the issuer is slow or busy; one refresh
runs at a time, and its token replaces the cached one when it arrives. The
client starts no timer. A failed refresh is not surfaced while the cached
token is valid, and the first request 30 s later tries again; a request
made once the token has expired waits for a refresh, and the provider's
error rejects it. A token the server refuses with `401` is never sent
again: the request waits for a new one. A provider that returns a string
is refreshed only after a `401`, as before.

## Producer state ownership

Use one `Producer` instance as the exclusive owner of each producer scope
(endpoint, project, stream, producer ID, routing key), backed by durable
state in production. Appends, batches, final seals, epoch bumps and automatic
reclaims share that instance's per-key queue. Independent routing keys can
progress concurrently. A state store is not a cross-process lock; coordinate
ownership externally before another instance takes over the same scope.
Prefer a few stable producer IDs, each with its state in a durable store,
to a new ID per process.

A client made by `connect` keys producer state on the issuer URL, so a
change of the service's hostname keeps every producer's sequence; a client
made with `new StreamsClient({ url })` keys it on `url`, as before. A watch
URL minted before a hostname change keeps the old host; mint a new one.

A producer append whose fetch fails is sent again with the same sequence
after 1, 2 and 4 s, before any later operation of its routing key; the
server answers a replay of an append that landed with `duplicate: true` and
stores it once. The seal, the quotas and another writer's fence are
checked before that, so a replay can be refused instead: it then rejects
with `AppendOutcomeUnknownError` (`retryable: false`, the refusal as
`cause`), the producer keeps its epoch and sequence, and `autoClaim` claims
no new epoch for it, which would store the record a second time. Its
message names the recovery, by `cause`:

- Another writer fenced the producer id (`ProducerFencedError`, `403`
  `stale_producer_epoch`): read the stream. Send the value again only if it
  is absent, and only under an epoch above `cause.currentEpoch`: a
  producer with `autoClaim` claims one when its resend is refused (and so
  fences the other writer in turn). `bumpEpoch()` does not: it adds one to
  this producer's own epoch, which can land on the other writer's, where
  the server answers a sequence below that writer's `duplicate: true`
  without writing it. Without `autoClaim`, stop using the producer id.
- The collection is sealed (`409` `sealed`): read the stream; it is final.
  Sending the value again is refused `409` `sealed` before the server's
  duplicate check, so it cannot tell you whether it was written.
- Any other refusal (a `429`, a `503`, a `401`, a `421`, any other `4xx`):
  send the same value on the same routing key through the same producer
  scope before any other value on that key. The server answers
  `duplicate: true` if it was written and writes it once if not. It keeps
  only the last sequence of each routing key and producer id, so a
  different value sent first is refused `409` `producer_sequence_reused`
  (`ProducerSequenceReusedError`) if the lost attempt was written, and is
  written ahead of it if not; appends already queued on that routing key
  are sent first and meet the same answer. A `producer_sequence_reused`
  there tells you the record was written: send the same value (answered
  `duplicate: true`, which moves the sequence on), then the refused one
  again. After a `421`, send it through a client for the project's new
  endpoint (see "Errors and retries"): a client made by `connect` keys the
  producer's state on the issuer URL, so the producer there keeps its
  epoch and sequence; a client made with `new StreamsClient({ url })` keys
  it on `url`, so a producer there starts at epoch 0, sequence 0, which the
  server can answer `duplicate: true` without writing it: carry the state
  over in your store first, saving what it holds for the scope whose
  `endpoint` is the old `url` under the same scope with the new one.

A producer append answered `2xx` whose answer cannot be read (not JSON, or
cut off mid-body) rejects with `StreamsError` `unreadable_response`
(`retryable: false`, the read failure as `cause`). The server answered
`2xx`, so the record was written, but the SDK saves a producer's state
only from an answer it read, so the producer keeps its sequence. Send the
same value on the same routing key through it before any other value on
that key: the server answers it `duplicate: true` and the producer moves
on. A different value sent first is refused `409`
`producer_sequence_reused`.

A fetch failure that outlasts the replay's attempts, or a failed state
save, rejects the operation and leaves later queue operations usable.
The server may already have committed the request. Retry the same payload
to resolve that uncertain outcome before sending different content or
intentionally changing the epoch. A rejected state save may itself
have persisted, depending on the store's contract; recover the store's outcome
before continuing. `bumpEpoch()` intentionally starts a fresh sequence and
cannot determine whether an earlier uncertain append committed.

## Consumer batches and per-key order

A pull (`consumer.pull({ max })`, or each batch of `for await (const message
of consumer)`, which pulls the consumer's `maxBatchRecords`) can hold several
consecutive records of one routing key, a run, each with its own lease. While
any of them is leased, no other pull receives that key. To keep a key's
order, handle its messages in batch order and one at a time: processing one
batch's messages concurrently processes a key's records concurrently, and
acknowledging a key's later message after retrying an earlier one has
processed them out of order, because the retried message returns after them.
A consumer that keeps order stops at a failed message and leaves the key's
later messages of the batch undecided; they come back with the failed one.
A record that has been delivered twice is leased alone, so a failing record
takes at most two attempts from each record of its run: with `maxAttempts`
of 3 or more (the default is 5) its successors are not dead-lettered for
its failure.

## Consumer cleanup and cancellation

`for await (const message of consumer)` submits recorded `ack`, `retry` and
`extend` decisions when a batch finishes, the loop breaks, or the iterator
closes. Unseen and undecided messages are never acknowledged. Each batch's
`settle()` submits once and retains its result, including failure; decisions
cannot change after settlement starts. Failed or uncertain settlement is
surfaced to the caller; leases without a successful acknowledgement remain
subject to server expiry/redelivery. A settle whose `fetch` fails is sent
again with the same decisions after 1, 2 and 4 s, as other requests are, so
on a dead network a cleanup's settlement takes about 7 s before it rejects.
A transport failure can occur after the server applied a settlement, so an
error is not proof of non-application. A settle the SDK replays (after a
transport failure, or a retryable 429 or 503) reports an ack the first
attempt applied as `stale` and never acknowledges a record twice (the server
checks each lease's generation). A retry or an extend the first attempt
applied keeps its lease, so the replay applies it again: it counts as
`retried` or `extended`, and the retry's delay or the extension's
visibility runs again from the replay, so it ends later by the time between
the two attempts, unless the message was delivered again in between (then
it counts as `stale`).

Use `consumer.messages({ signal })` to cancel a parked pull, or call the
iterator's `return()`; both abort the pull immediately. Cleanup still submits
decisions already recorded, using a separate request from the cancelled pull.

JavaScript keeps an exception thrown by a `for await` loop body even if
iterator cleanup also fails. Keep the iterator when both outcomes matter:

```ts
const messages = consumer.messages({ signal });
try {
  for await (const message of messages) {
    await process(message);
    message.ack();
  }
} finally {
  const outcome = await messages.closed;
  if (outcome.status === "failed") reportCleanupFailure(outcome.error);
}
```

`closed` always resolves to `{status:"closed"}` or `{status:"failed",error}`;
normal `next()`/`return()` calls also reject on settlement failure. Passing a
processing exception explicitly with `iterator.throw(error)` combines it and
any settlement failure in an `AggregateError`.

## Watches

A watch turns "did anything change for customer c1?" into one long poll.
The key and its signature are derived locally from the stream key, so
the resulting URL can be handed to an untrusted client: it observes that
one key and carries no credentials. Declare a watch when you create the
stream (`watches: [{ name: "by-customer", fields: ["/customerId"] }]`).

```ts
const w = await orders.watch("by-customer", ["c1"]);
w.url();                       // hand this to a browser
for await (const ev of w.subscribe({ signal })) { /* refetch */ }
```

Values go in the definition's declared field order, and the count must
match. Watch keys are derived, never guessed — see `deriveWatchKey` if
you want the key without the URL.

## Dead-letter queues

`deadLetterStream` must be a different collection under the same
encryption key: dead-letter records are written with the source
collection's key. The link is checked when the consumer is configured.

## Usage

The beta is metered, not charged: the service counts what each project
ingests, reads and stores, and you can read the meter. `await
client.usage()` reads the project's metered usage for the current UTC
month, `client.usage({ month: "2026-09" })` for another (`YYYY-MM`; a
malformed month is `400` `invalid_month`). It needs the client's `project`
(`connect` sets it) and a credential with the `streams.usage.read` scope,
which a credential has by default, and no stream-prefix restriction: a
prefix-restricted credential is refused `403` `prefix_denied`, since the
totals cover every stream of the project. The figures trail an
acknowledged write by about 10 s and a read by about 20 s.

## Limits

The beta's limits (per project and per request) are listed at
`<the beta's limits page, published with the beta>`.

Plan your request rate against the free tier's 10 requests a second per
project. Every request counts one: an append (an `appendMany` of up to
10,000 records is one request), a read, a consumer pull, a settlement, a
subscription's or a watch's long poll, a create. A project quiet for 5 s
can send 50 requests at once, and then 10 a second. An append that is
read as it arrives costs 1, plus 2 for each consumer group (its pull and
its settlement), plus 1 for each subscription waiting on the stream, all
within a few tens of milliseconds. Keep the project's average near 5
requests a second: there one or two consumer groups are rarely if ever
refused (one group stays clear up to about 7 a second; a third group at
5 meets about one refusal an hour, which the SDK replays). 10 a
second is a ceiling, not a working rate: a producer whose appends arrive
at random around an average of 10 a second has some refused even after
the SDK's replays, so batch records with `appendMany` rather than
appending them one at a time. A refusal is `429` `project_rate_limit`
with `Retry-After`; the SDK replays it three times, each wait capped at
5 s, about 15 s in all (see "Errors and retries"). Creates, deletes,
seals and stream listings also pay for the registry work they do, from
a second budget at the same rate (a create costs 11, a listing of 100
streams about 110), and are refused while it is in debt: at 10 a second
a project creates about one stream every 1.1 s. A burst is admitted at
once and paid for afterwards, so 50 concurrent creates leave the
project's next create, delete, seal or listing refused for about 54 s,
longer than the SDK replays; create streams one at a time (the SDK
replays the short waits between them) or ahead of when you need them.

## Beta terms and support

`<the beta's terms and support channel, published with the beta>`

Report SDK problems at https://github.com/prisma/streams/issues.

## Security notes

`<Re-verify this section against storage layout 6 before publishing; its
cryptographic acceptance is the owner's.>`

Records are encrypted with the stream's key before they are stored. From
storage layout 5 on, the records of one append request are also compressed
together before they are encrypted, so a stored page's size shows how well
its request compressed, and the storage provider, anyone who can list the
bucket and the stored-byte usage counters can see that size. If the storage
operator is outside your trust boundary, do not batch secrets together with
attacker-controlled data into one append: a gateway that appends many end
users' events in one request lets one user's chosen fields be compressed
together with another user's secret. Append them separately. Layout 5's
page format and this guidance are in `docs/crypto-page-v6.md` ("Compression
and length") in the Prisma Streams repository, a document that layout adds.

## Runtimes

CI runs this package's end-to-end smoke on **Node 18, Node 22, Node 24,
Bun and Deno** — those are gated, not asserted. Node 18 is the `engines`
floor and Node 24 the current LTS. The code uses only `fetch`,
WebCrypto and web streams, so browsers should work too, but no browser is
currently in the gate; treat browser support as expected rather than
verified.
