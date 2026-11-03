import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient } from "../dist/index.js";

// A read the SDK sends names its page: 1 MiB unless the caller names one
// (tuning P1 item 2, edge change #240). The server reserves a read's memory
// by its page, so an SDK read never asks for the server's 8 MiB default.
const KEY = "A".repeat(43);
const json = (body, headers = {}) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json", ...headers } });

function fixture(onRead) {
  const reads = [];
  const client = new StreamsClient({
    url: "https://cell.example",
    fetch: async (url, init) => {
      const u = new URL(String(url));
      if (u.pathname.includes("/records")) {
        reads.push(u.searchParams.get("maxBytes"));
        onRead?.();
        return json([], { "streams-next-cursor": "c1", "streams-up-to-date": "true" });
      }
      return json({ name: "orders", contentType: "application/json", sealed: false, epoch: "0".repeat(32) });
    },
  });
  return { client, reads };
}

test("a read names a 1 MiB page unless its caller names one", async () => {
  const { client, reads } = fixture();
  const orders = await client.createStream("orders", { encryptionKey: KEY, format: { kind: "json" } });
  await orders.read();
  await orders.read({ maxBytes: 4096 });
  await orders.read({ from: "beginning" });
  assert.deepEqual(reads, ["1048576", "4096", "1048576"]);
});

test("a subscription's reads name a 1 MiB page unless its caller names one", async () => {
  for (const [options, want] of [[{}, "1048576"], [{ maxBytes: 65536 }, "65536"]]) {
    const abort = new AbortController();
    const { client, reads } = fixture(() => abort.abort());
    const orders = await client.createStream("orders", { encryptionKey: KEY, format: { kind: "json" } });
    try {
      for await (const _ of orders.subscribe({ ...options, signal: abort.signal })) break;
    } catch {
      // the abort ends the subscription
    }
    assert.equal(reads[0], want, JSON.stringify(options));
  }
});
