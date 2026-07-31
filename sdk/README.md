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

Authentication (`token`) belongs to the client; encryption
(`encryptionKey`) belongs to the stream handle. Zero dependencies;
Node 18+, Bun, Deno, and browsers.
