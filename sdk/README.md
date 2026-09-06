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

A failed fetch or state save rejects the operation and leaves later queue
operations usable. The server may already have committed the request. Retry
the same payload to resolve that uncertain outcome before sending different
content or intentionally changing the epoch. A rejected state save may itself
have persisted, depending on the store's contract; recover the store's outcome
before continuing. `bumpEpoch()` intentionally starts a fresh sequence and
cannot determine whether an earlier uncertain append committed.

## Request failures and endpoint recovery

`WrongCellError` is never retried at the current endpoint, including the
503 fallback form. Resolve the project's new endpoint and create a client
for it; refreshing a valid credential cannot fix routing. Ordinary transient
429/503 responses retry with bounded, abort-aware backoff unless the server
explicitly marks the error permanent. Subscriptions use the same failure
classification and retain durable-cursor recovery for applied reads.

Fetch failures are exposed as `StreamsTransportError` with the original
failure in `cause`. Subscriptions reconnect after these transport failures;
credential-provider failures propagate unchanged. Concurrent 401 responses
for the same cached credential share one refresh, and late responses cannot
invalidate its successor.

## Consumer cleanup and cancellation

`for await (const message of consumer)` submits recorded `ack`, `retry` and
`extend` decisions when a batch finishes, the loop breaks, or the iterator
closes. Unseen and undecided messages are never acknowledged. Each batch's
`settle()` submits once and retains its result, including failure; decisions
cannot change after settlement starts. Failed or uncertain settlement is
surfaced to the caller; leases without a successful acknowledgement remain
subject to server expiry/redelivery. A transport failure can occur after the
server applied a settlement, so an error is not proof of non-application.

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
