import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient } from "../dist/index.js";

// The Prisma Compute edge in front of *.prisma.build strips every request
// header named Prisma-* (edge change #173), so the SDK sends the server's
// Streams- names: the key, the routing key and the consumer version each
// reach the cell, and no request carries a Prisma- header the edge drops.
const KEY = "A".repeat(43);
const json = (body, headers = {}) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json", ...headers } });

function fixture() {
  const calls = [];
  const client = new StreamsClient({
    url: "https://cell.example",
    fetch: async (url, init) => {
      const path = new URL(String(url)).pathname;
      const headers = Object.fromEntries(Object.entries(init.headers ?? {}).map(([k, v]) => [k.toLowerCase(), v]));
      calls.push({ method: init.method, path, headers });
      if (path.includes("/consumers/")) return json({ name: "workers" }, { "prisma-consumer-version": "v1" });
      if (path.endsWith("/records") && init.method === "POST") {
        return json({ cursor: "c", count: 1, duplicate: false, sealed: false });
      }
      return json({ name: "orders", contentType: "application/json", sealed: false, epoch: "0".repeat(32) });
    },
  });
  return { client, calls };
}

test("every keyed request names the key, routing key and consumer version in the Streams- form", async () => {
  const { client, calls } = fixture();
  const orders = await client.createStream("orders", { encryptionKey: KEY, format: { kind: "json" } });
  await orders.append({ n: 1 }, { routingKey: "c1" });
  await orders.producer("p1", { ephemeral: true }).append({ n: 2 }, { routingKey: "c2" });
  const workers = await orders.consumer("workers");
  await workers.delete();

  const sent = calls.map(({ method, path, headers }) => ({
    method,
    path,
    key: headers["streams-encryption-key"],
    routingKey: headers["streams-routing-key"],
    version: headers["streams-consumer-version"],
    prisma: Object.keys(headers).filter(name => name.startsWith("prisma-")),
  }));
  assert.deepEqual(sent.map(({ method, path }) => `${method} ${path}`), [
    "PUT /v1/streams/orders",
    "POST /v1/streams/orders/records",
    "POST /v1/streams/orders/records",
    "PUT /v1/streams/orders/consumers/workers",
    "DELETE /v1/streams/orders/consumers/workers",
  ]);
  for (const request of sent) {
    assert.equal(request.key, KEY, `${request.method} ${request.path}`);
    assert.deepEqual(request.prisma, [], `${request.method} ${request.path} sends no Prisma- header`);
  }
  assert.deepEqual(sent.map(request => request.routingKey), [undefined, "c1", "c2", undefined, undefined]);
  assert.deepEqual(sent.map(request => request.version), [undefined, undefined, undefined, undefined, "v1"]);
});
