# @prisma/streams

The Prisma Streams product SDK: encrypted, append-only collections
with routing keys, producer sessions, consumer groups, and watches.

```ts
import { StreamsClient } from "@prisma/streams";

const client = new StreamsClient({ url, token });
const orders = await client.createStream("orders", {
  encryptionKey,
  format: { kind: "json" },
  watches: [{ name: "by-customer", fields: ["/customerId"] }],
});

await orders.append({ customerId: "c1" }, { routingKey: "c1" });
for await (const rec of orders.subscribe({ routingKey: "c1" })) { /* … */ }
```

## Authenticating

A project credential (`prisma_streams_<id>.<secret>`, shown once when it
is created) is exchanged at the token issuer for access tokens. Keep it in
a server-side secret store and pass it whole; never ship it to a browser.

```ts
import { StreamsClient } from "@prisma/streams";

const client = await StreamsClient.connect({
  credential: process.env.PRISMA_STREAMS_CREDENTIAL!,
  issuerUrl,
});
```

`connect` exchanges the credential once. The client's base URL is the
issuer's `endpoint` and its project is the credential's. An explicit `url`
wins, and the client then never follows the issuer's `endpoint`; an issuer
that names no endpoint needs one (`endpoint_required`). An issuer's
`endpoint` must be `https:` or a loopback host (`localhost`, `*.localhost`,
`127.0.0.1`, `[::1]`), because every request carries the stream key:
anything else is refused before the first request (`insecure_endpoint`)
unless you pass `allowInsecureEndpoint: true`. Tokens are exchanged again
at 80% of their lifetime and after a `401`, as described under "Request
failures and endpoint recovery". When an exchange names a different
endpoint (and no `url` was given), the client moves there, starting with
the request that triggered the exchange; an insecure endpoint is not
followed.

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
success without a token is `invalid_exchange_response`. Neither the
credential nor a token is logged or put in an error.

## Encryption keys

A stream's records are encrypted under a key you hold. Create one per
stream with `await generateEncryptionKey()` (32 random bytes from
WebCrypto, as 43 characters of unpadded base64url) and keep it in your
secret store. The service never stores it: if you lose it, the stream's
data cannot be read by anyone.

## Usage

`await client.usage()` reads the project's metered usage for the current
UTC month, `client.usage({ month: "2026-09" })` for another (`YYYY-MM`; a
malformed month is `400` `invalid_month`). It needs the client's `project`
(`connect` sets it) and a credential with the `streams.usage.read` scope,
which a credential has by default, and no stream-prefix restriction: a
prefix-restricted credential is refused `403` `prefix_denied`, since the
totals cover every stream of the project. The figures trail an
acknowledged write by about 10 s and a read by about 20 s.

## Watches

A watch turns "did anything change for customer c1?" into one long poll.
The key and its signature are derived locally from the stream key, so
the resulting URL can be handed to an untrusted client: it observes that
one key and carries no credentials.

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

## Producer state ownership

Use one `Producer` instance as the exclusive owner of each producer scope
(endpoint, project, stream, producer ID, routing key), backed by durable
state in production. Appends, batches, final seals, epoch bumps and automatic
reclaims share that instance's per-key queue. Independent routing keys can
progress concurrently. A state store is not a cross-process lock; coordinate
ownership externally before another instance takes over the same scope.

A client made by `connect` keys producer state on the issuer URL, so a
change of the service's hostname keeps every producer's sequence; a client
made with `new StreamsClient({ url })` keys it on `url`, as before. A watch
URL minted before a hostname change keeps the old host; mint a new one.

A failed fetch or state save rejects the operation and leaves later queue
operations usable. The server may already have committed the request. Retry
the same payload to resolve that uncertain outcome before sending different
content or intentionally changing the epoch. A rejected state save may itself
have persisted, depending on the store's contract; recover the store's outcome
before continuing. `bumpEpoch()` intentionally starts a fresh sequence and
cannot determine whether an earlier uncertain append committed.

## Security notes

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

## Request failures and endpoint recovery

`WrongCellError` is never retried at the current endpoint, including the
503 fallback form. Resolve the project's new endpoint and create a client
for it; refreshing a valid credential cannot fix routing. Ordinary transient
429/503 responses retry with bounded, abort-aware backoff unless the server
explicitly marks the error permanent. Subscriptions use the same failure
classification and retain durable-cursor recovery for applied reads.

Fetch failures are exposed as `StreamsTransportError` with the original
failure in `cause`, except for plain appends (`append`, `appendBytes`,
`appendMany`): a transport failure, a 408 or a 5xx other than 503 rejects
with `AppendOutcomeUnknownError` (`retryable: false`), because the record
may or may not have been written and the SDK does not replay it; use a
producer for a retry the server deduplicates.

A request whose `fetch` fails is sent again up to three times, after 1, 2
and 4 s (abort-aware), within the same four attempts as the 429/503 replay,
and rejects with `StreamsTransportError` once they are spent. Plain appends,
stream deletion, producer appends and batch settlement are sent once and
reject at once; a subscription's request is not replayed either, because
the subscription reconnects itself. `retry: { transport: false }` turns the
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
a `Date`) instead of a string. Such a token is refreshed at the first
request made after 80% of its lifetime, or two minutes before it expires
when that is earlier (a 3600 s token at 2880 s, a 600 s token at 480 s);
requests made while the refresh runs wait for it and share it, and the
client starts no timer. If the refresh fails while the cached token is
still valid, the request is sent with that token and the failure is not
surfaced; the next request tries again, and once the token has expired
the provider's error rejects the request. A token the server refuses with
`401` is never sent again. A provider that returns a string is refreshed
only after a `401`, as before.

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
subject to server expiry/redelivery. A transport failure can occur after the
server applied a settlement, so an error is not proof of non-application.
A settle the SDK replays (after a retryable 429 or 503) may report
decisions the first attempt applied as `stale`; a replay never applies a
decision twice (the server checks each lease's generation).

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

Authentication (`token`) belongs to the client; encryption
(`encryptionKey`) belongs to the stream handle. Zero dependencies.

**Runtimes.** CI runs this package's end-to-end smoke on **Node 18,
Node 22, Bun and Deno** — those are gated, not asserted. The code uses
only `fetch`, WebCrypto and web streams, so browsers should work too,
but no browser is currently in the gate; treat browser support as
expected rather than verified.
