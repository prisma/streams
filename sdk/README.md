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

Authentication (`token`) belongs to the client; encryption
(`encryptionKey`) belongs to the stream handle. Zero dependencies;
Node 18+, Bun, Deno, and browsers.
